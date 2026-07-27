//! Per-pool ORDERED transaction sequencer, driven by the leader's block order.
//!
//! The order of transactions is NOT something we reconstruct — ShredStream
//! already delivers `solana_entry::entry::Entry` objects in PoH order (the
//! leader's exact execution order), and we stamp every tx with a monotonic
//! `order_seq` as we read them. So per pool we hold the transactions in exactly
//! the order the leader ran them, keyed by `(slot, order_seq)`.
//!
//! The rule (the operator's, verbatim): when an update arrives carrying a tx
//! signature H — an account-update (which also gives H's resulting reserves) or
//! a transaction-update (which tells us H reverted) — we FIND H in the ordered
//! list and simulate the tx IMMEDIATELY AFTER H, on H's resulting reserves.
//! Never re-simulate H itself. If that successor is a tx we couldn't decode
//! (aggregator/CPI), we WAIT for its own update — we never skip it. A reverted H
//! changed nothing, so its successor is priced on the unchanged reserves.
//!
//! This module is PURE logic (no async/I/O) so the exact rule is unit-tested;
//! the engine enqueues shred txs and the pool-state gRPC task drives the events.

use crate::pumpfun_math::PumpPool;
use crate::shred_stream::PumpIxKind;
use dashmap::DashMap;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How long a tx stays in the ordered list before it's discarded (a few Solana
/// blocks — long enough to bridge shred → update, short enough to stay bounded).
pub const MAX_AGE: Duration = Duration::from_secs(10);

/// One decoded Pump swap leg (a tx may carry several — the sell+buy sandwich).
#[derive(Clone, Debug)]
pub struct Leg {
    pub kind: PumpIxKind,
    pub base_amount: u64,
    pub quote_amount: u64,
    /// The tx's slippage bound (arg1): min_quote_out (sell) / max_quote_in (buy)
    /// / min_base_out (exact-quote-in, boost). Drives the revert verdict.
    pub bound: u64,
}

/// What we know about a queued tx's effect on THIS pool.
#[derive(Clone, Debug)]
pub enum TxKind {
    /// We decoded the pool's swap(s): `token_is_base` gives orientation, `legs`
    /// are the ordered swaps on this pool within the tx.
    Readable { token_is_base: bool, legs: Vec<Leg> },
    /// The pool is touched through a CPI we can't decode (aggregator/router/bot).
    /// We must WAIT for its own update to advance past it.
    Unreadable,
}

/// The other pool this tx also touches, when it's a multi-hop arb (leg 1 on one
/// pool, leg 2 on another). Kept only for the human-readable label and to be
/// ready for the future cross-pool barrier — the barrier itself is NOT active in
/// this test phase (pools don't overlap yet). `on_this_pool_leg` is which leg of
/// the tx executes on THIS queue's pool (1 or 2), so the label can show order.
#[derive(Clone, Debug)]
pub struct MultiHop {
    /// The other DEX pool address this tx also swaps on.
    pub other_pool: Pubkey,
    /// Human DEX name of the other pool, e.g. "Meteora_DAMM_v2".
    pub other_dex: &'static str,
    /// Which leg on THIS pool: 1 = first (execute first), 2 = second (would wait
    /// for the other pool's leg 1 once the barrier is enabled).
    pub on_this_pool_leg: u8,
}

#[derive(Clone, Debug)]
struct QueuedTx {
    sig: Signature,
    slot: u64,
    /// GLOBAL PoH order index (counts every cluster tx). Kept for sort stability
    /// and the `{global_idx}` field of the label (which tx among the batch).
    order_seq: u64,
    /// PER-POOL sequence number (1, 2, 3, ...) — this tx's turn WITHIN this pool's
    /// own queue. Assigned on enqueue. This is the number the operator's naming
    /// scheme uses and the basis of "successor = pool_seq + 1".
    pool_seq: u32,
    kind: TxKind,
    /// Multi-hop info for the label / future barrier (None = single-hop Pump).
    multi: Option<MultiHop>,
    seen: Instant,
    reverted: bool,
}

/// A request to simulate one tx against a known pre-state — the tx AFTER a
/// confirmed one, priced on that confirmed one's resulting reserves.
#[derive(Clone, Debug)]
pub struct ComputeReq {
    pub sig: Signature,
    pub slot: u64,
    pub order_seq: u64,
    /// This tx's per-pool turn number (1, 2, 3, ...).
    pub pool_seq: u32,
    pub token_is_base: bool,
    pub legs: Vec<Leg>,
    /// The exact reserves this tx executes against — the confirmed reserves of
    /// its immediate predecessor.
    pub pre_state: PumpPool,
    /// The confirmed tx whose update triggered this (H). Logged so it's provable
    /// we compute the tx AFTER H, not H itself.
    pub predecessor: Signature,
    /// The operator's human-readable queue label, e.g.
    /// `3_540_4XYE…gFCxs6_pump_fun_AMM-7LzU…F3DG1_Meteora_DAMM_v2`.
    pub label: String,
}

/// The ordered ledger for a single pool.
pub struct PoolSeq {
    /// This pool's address — stored so labels can include it without threading it
    /// through every call.
    pool: Pubkey,
    /// Monotonic per-pool sequence counter. Every enqueued tx on THIS pool gets
    /// the next value (1, 2, 3, ...). Continues across shred batches and slots
    /// (only resets on bot restart); combined with `slot` it also orders across
    /// slots correctly.
    next_pool_seq: u32,
    /// All txs we've seen for this pool, kept sorted by `(slot, order_seq)` —
    /// the leader's block order.
    queue: Vec<QueuedTx>,
    /// The latest confirmed reserves — the ground-truth pool state from the last
    /// SUCCESSFUL account-update we matched to a queued tx. This is the ONLY
    /// source of `pre_state` we ever price a successor on. A reverted tx never
    /// touches this. `None` until the first successful match.
    reserves: Option<PumpPool>,
    /// The `(slot, order_seq, sig)` of the tx that PRODUCED `reserves` — i.e. the
    /// last SUCCESSFUL (account-update-confirmed) tx. This is the true anchor the
    /// reserves belong to. Distinct from `confirmed` below, which can also be
    /// advanced by a revert (which produces no reserves).
    reserves_tx: Option<(u64, u64, Signature)>,
    /// The CONFIRMED FRONTIER: `(slot, order_seq, sig)` of the last confirmed tx
    /// H. The tx we want to simulate is the first one strictly after this. Set by
    /// an account-update (H succeeded, `reserves` are H's result) or a revert
    /// (H changed nothing, `reserves` unchanged). Only advances forward.
    confirmed: Option<(u64, u64, Signature)>,
    /// The `(slot, order_seq)` of the successor we last emitted, so we price each
    /// successor exactly once no matter how many events poke us.
    last_emitted: Option<(u64, u64)>,
}

impl PoolSeq {
    /// Create an empty ledger for a specific pool.
    pub fn new(pool: Pubkey) -> Self {
        Self {
            pool,
            next_pool_seq: 0,
            queue: Vec::with_capacity(64),
            reserves: None,
            reserves_tx: None,
            confirmed: None,
            last_emitted: None,
        }
    }
}

impl PoolSeq {
    /// Record a tx from the shred stream. Assigns this pool's NEXT per-pool
    /// sequence number, keeps the queue sorted by `(slot, order_seq)`, dedupes by
    /// signature, prunes by age. Returns a compute request if this newly-arrived
    /// tx is the awaited successor of the confirmed frontier — the shred can
    /// arrive AFTER the predecessor's update, so enqueue must also be able to
    /// trigger the simulation.
    pub fn enqueue(
        &mut self,
        sig: Signature,
        slot: u64,
        order_seq: u64,
        kind: TxKind,
        multi: Option<MultiHop>,
        now: Instant,
    ) -> Option<ComputeReq> {
        self.prune(now);
        if self.queue.iter().any(|q| q.sig == sig) {
            return None;
        }
        self.next_pool_seq += 1;
        let item = QueuedTx {
            sig,
            slot,
            order_seq,
            pool_seq: self.next_pool_seq,
            kind,
            multi,
            seen: now,
            reverted: false,
        };
        let pos = self
            .queue
            .partition_point(|q| (q.slot, q.order_seq) < (slot, order_seq));
        self.queue.insert(pos, item);
        self.emit_next()
    }

    fn prune(&mut self, now: Instant) {
        self.queue.retain(|q| now.duration_since(q.seen) <= MAX_AGE);
    }

    /// An account-update arrived for `sig` (H) with its resulting `reserves`.
    /// Advance the confirmed frontier to H (if newer), adopt H's reserves, and
    /// simulate the tx AFTER H.
    pub fn on_account_update(
        &mut self,
        sig: Signature,
        reserves: PumpPool,
        now: Instant,
    ) -> Option<ComputeReq> {
        self.prune(now);
        let pos = self.position_of(&sig)?; // H not in our ordered list → no-op
        let key = (self.queue[pos].slot, self.queue[pos].order_seq);
        if self.advance_confirmed(pos) {
            // This account-update is the newest confirmed SUCCESSFUL tx: it
            // carries the true post-swap reserves. Anchor both the reserves and
            // the tx they belong to, so a successor is only ever priced on the
            // state of its genuine predecessor.
            self.reserves = Some(reserves);
            self.reserves_tx = Some((key.0, key.1, self.queue[pos].sig));
        }
        self.emit_next()
    }

    /// A transaction-update arrived for `sig`. A reverted tx changed nothing (no
    /// account-update will come): mark it, advance the frontier past it, and price
    /// the successor on the UNCHANGED reserves. A non-reverted tx-update is
    /// redundant with its account-update — leave the reserves to that.
    ///
    /// CRITICAL (condition 4 — rollback of a wrongly-trusted tx): if we had ALREADY
    /// simulated a successor that was priced as if THIS tx succeeded, and it turns
    /// out THIS tx reverted, our earlier `pre_state`/verdict for that successor was
    /// built on the wrong assumption. We reset `last_emitted` so the successor is
    /// RE-EMITTED and re-priced on the correct (unchanged) reserves — i.e. we roll
    /// the pool state back to the last real account-update before recomputing.
    pub fn on_tx_update(&mut self, sig: Signature, reverted: bool, now: Instant) -> Option<ComputeReq> {
        self.prune(now);
        let pos = self.position_of(&sig)?;
        if reverted {
            let key = (self.queue[pos].slot, self.queue[pos].order_seq);
            let was_new = !self.queue[pos].reverted;
            self.queue[pos].reverted = true;
            // If a successor was already emitted at/after this now-reverted tx,
            // that emission may have been priced assuming this tx changed the
            // pool. Force a re-emit by clearing the de-dup latch when the latch
            // points at or beyond this tx.
            if was_new {
                if let Some(le) = self.last_emitted {
                    if le >= key {
                        self.last_emitted = None;
                    }
                }
            }
            self.advance_confirmed(pos);
        }
        self.emit_next()
    }

    fn position_of(&self, sig: &Signature) -> Option<usize> {
        self.queue.iter().position(|q| q.sig == *sig)
    }

    /// Build the operator's human-readable queue label for a queued tx, e.g.
    ///   single-hop:  `2_500_4XYE…gFCxs6_pump_fun_AMM`
    ///   two-hop L1:  `3_540_4XYE…gFCxs6_pump_fun_AMM-7LzU…F3DG1_Meteora_DAMM_v2`
    ///   two-hop L2:  `5_999_7LzU…F3DG1_Meteora_DAMM_v2-4XYE…gFCxs6_pump_fun_AMM`
    /// Format: `{pool_seq}_{global_idx}_{leg1_pool}_{leg1_dex}[-{leg2_pool}_{leg2_dex}]`.
    /// The first leg shown is whichever executes first; for a tx whose leg on THIS
    /// pool is the second, the other (earlier) pool is shown first.
    fn label_for(&self, q: &QueuedTx) -> String {
        let this = short_pubkey(&self.pool);
        match &q.multi {
            None => format!("{}_{}_{}_pump_fun_AMM", q.pool_seq, q.order_seq, this),
            Some(m) => {
                let other = short_pubkey(&m.other_pool);
                if m.on_this_pool_leg == 1 {
                    // This pool is leg 1 → shown first, other pool second.
                    format!(
                        "{}_{}_{}_pump_fun_AMM-{}_{}",
                        q.pool_seq, q.order_seq, this, other, m.other_dex
                    )
                } else {
                    // This pool is leg 2 → the other pool's leg is first.
                    format!(
                        "{}_{}_{}_{}-{}_pump_fun_AMM",
                        q.pool_seq, q.order_seq, other, m.other_dex, this
                    )
                }
            }
        }
    }

    /// Move the confirmed frontier to `pos` if it is strictly newer. Returns true
    /// if it advanced (so the caller may adopt this tx's reserves).
    fn advance_confirmed(&mut self, pos: usize) -> bool {
        let key = (self.queue[pos].slot, self.queue[pos].order_seq);
        let newer = match self.confirmed {
            Some((s, o, _)) => key > (s, o),
            None => true,
        };
        if newer {
            self.confirmed = Some((key.0, key.1, self.queue[pos].sig));
        }
        newer
    }

    /// Simulate the successor of the confirmed frontier — but ONLY when it is the
    /// genuine, contiguous next tx of the pool's executed sequence, priced on the
    /// reserves that truly belong to its immediate predecessor. Emitted at most
    /// once. Several guards protect correctness (this is where the "predecessor is
    /// a reverted tx from many blocks ago" bug lived):
    ///
    ///  1. `reserves` and `reserves_tx` must exist — we never price on nothing.
    ///  2. The successor must be the tx IMMEDIATELY AFTER the frontier in our
    ///     ordered queue — no unknown tx may sit between them. A hole means we
    ///     might be missing a tx that already moved the pool, so we WAIT.
    ///  3. The reserves we price on must belong to the tx IMMEDIATELY BEFORE the
    ///     successor. If the frontier advanced past a REVERT, the reserves still
    ///     belong to the last successful tx — that is correct ONLY if no
    ///     successful (reserve-changing) tx sits between `reserves_tx` and the
    ///     successor. We verify every tx strictly between them is a known revert.
    ///  4. Unreadable successor → wait for its own update; never skip it.
    fn emit_next(&mut self) -> Option<ComputeReq> {
        let pre = self.reserves?;
        let (rslot, rseq, _rsig) = self.reserves_tx?;
        let (cslot, cseq, _csig) = self.confirmed?;

        // Index of the frontier tx in the queue (it must still be present).
        let cpos = self
            .queue
            .iter()
            .position(|q| (q.slot, q.order_seq) == (cslot, cseq))?;

        // The successor is the NEXT element in the ordered queue after the
        // frontier. `order_seq` is GLOBAL across the whole cluster (it counts
        // every tx in every slot), so consecutive txs on THIS pool are NOT
        // numerically consecutive — there is no "expected order_seq+1" to check.
        // Contiguity is therefore defined structurally: since we forward EVERY
        // watched-pool Pump tx into this queue (condition 1), the next queue
        // element IS the pool's next executed tx, unless its shred simply hasn't
        // arrived yet. We can't distinguish "no tx between" from "shred not yet
        // arrived" purely from the queue, so we lean on the reserves anchor:
        let next = self.queue.get(cpos + 1)?;
        let next_key = (next.slot, next.order_seq);

        // GUARD (reserves belong to the immediate predecessor): the reserves we
        // hold were produced by `reserves_tx`. They are valid pre-state for `next`
        // ONLY if the frontier we're emitting from IS that same reserves_tx — i.e.
        // the confirmed frontier equals the tx whose reserves we hold, OR every tx
        // strictly between reserves_tx and the frontier is a known revert (a revert
        // changed nothing, so the reserves still hold). If a NON-reverted tx sits
        // between the reserves anchor and the frontier, its account-update hasn't
        // arrived, so our reserves are stale for `next` → WAIT.
        let anchor_to_frontier_ok = self
            .queue
            .iter()
            .filter(|q| (q.slot, q.order_seq) > (rslot, rseq) && (q.slot, q.order_seq) <= (cslot, cseq))
            .all(|q| q.reverted);
        if !anchor_to_frontier_ok {
            return None; // an unconfirmed reserve-changing tx precedes the frontier → wait
        }

        // GUARD (frontier is truly adjacent to successor): the confirmed frontier
        // must be the reserves anchor itself, or separated from `next` only by
        // reverts. Equivalent to: every tx strictly between the frontier and
        // `next` is a revert. (With cpos+1 there are none in-queue, but a revert
        // may have advanced the frontier past txs we should skip.)
        let frontier_to_next_ok = self
            .queue
            .iter()
            .filter(|q| (q.slot, q.order_seq) > (cslot, cseq) && (q.slot, q.order_seq) < next_key)
            .all(|q| q.reverted);
        if !frontier_to_next_ok {
            return None;
        }

        // A reverted successor changed nothing; the frontier will move past it via
        // its own tx-update. Nothing to simulate for it here.
        if next.reverted {
            return None;
        }

        // Snapshot everything from the successor up front, so building the label
        // (which borrows &self) never overlaps a live &next borrow.
        let next_pool_seq = next.pool_seq;
        let next_sig = next.sig;
        let next_slot = next.slot;
        let next_order = next.order_seq;
        let next_kind = next.kind.clone();
        let label = {
            let q = self.queue.get(cpos + 1)?;
            self.label_for(q)
        };

        match next_kind {
            TxKind::Unreadable => None, // wait for its own update
            TxKind::Readable { token_is_base, legs } => {
                if self.last_emitted == Some(next_key) {
                    return None; // already priced this successor
                }
                self.last_emitted = Some(next_key);
                // predecessor logged is the tx the RESERVES belong to (the true
                // state anchor), so the [seq] line can never again show a
                // predecessor that is many blocks away or a bare revert.
                let predecessor = self.reserves_tx.map(|(_, _, s)| s).unwrap_or_default();
                Some(ComputeReq {
                    sig: next_sig,
                    slot: next_slot,
                    order_seq: next_order,
                    pool_seq: next_pool_seq,
                    token_is_base,
                    legs,
                    pre_state: pre,
                    predecessor,
                    label,
                })
            }
        }
    }

    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }
}

/// An event forwarded from the gRPC pool-state stream to drive the sequencer.
pub enum SeqEvent {
    Account {
        account: Pubkey,
        sig: Option<Signature>,
        slot: u64,
    },
    Tx {
        sig: Signature,
        reverted: bool,
    },
}

/// Concurrent per-pool sequencer: one `PoolSeq` per Pump pool behind its own lock.
pub struct Sequencer {
    pools: DashMap<Pubkey, Mutex<PoolSeq>>,
}

impl Default for Sequencer {
    fn default() -> Self {
        Self {
            pools: DashMap::with_capacity(64),
        }
    }
}

impl Sequencer {
    #[allow(clippy::too_many_arguments)]
    pub fn enqueue(
        &self,
        pool: Pubkey,
        sig: Signature,
        slot: u64,
        order_seq: u64,
        kind: TxKind,
        multi: Option<MultiHop>,
        now: Instant,
    ) -> Option<ComputeReq> {
        self.pools
            .entry(pool)
            .or_insert_with(|| Mutex::new(PoolSeq::new(pool)))
            .lock()
            .unwrap()
            .enqueue(sig, slot, order_seq, kind, multi, now)
    }

    pub fn on_account_update(
        &self,
        pool: Pubkey,
        sig: Signature,
        reserves: PumpPool,
        now: Instant,
    ) -> Option<ComputeReq> {
        self.pools
            .entry(pool)
            .or_insert_with(|| Mutex::new(PoolSeq::new(pool)))
            .lock()
            .unwrap()
            .on_account_update(sig, reserves, now)
    }

    /// A transaction-update whose pool we don't know up front — try every pool's
    /// list (only a handful). Returns each pool that produced a compute request.
    pub fn on_tx_update_any(
        &self,
        sig: Signature,
        reverted: bool,
        now: Instant,
    ) -> Vec<(Pubkey, ComputeReq)> {
        let mut out = Vec::new();
        for e in self.pools.iter() {
            if let Some(req) = e.value().lock().unwrap().on_tx_update(sig, reverted, now) {
                out.push((*e.key(), req));
            }
        }
        out
    }
}

/// Shorten a pubkey for labels: first 4 … last 6 chars of its base58, matching
/// the operator's `4XYE…gFCxs6` style. Kept compact so a two-hop label stays
/// readable in a log line.
fn short_pubkey(pk: &Pubkey) -> String {
    let s = pk.to_string();
    if s.len() <= 12 {
        return s;
    }
    let head = &s[..4];
    let tail = &s[s.len() - 6..];
    format!("{head}…{tail}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(base: u64, quote: u64) -> PumpPool {
        PumpPool {
            base_reserve: base,
            quote_reserve: quote,
            total_fee_bps: 30,
            lp_fee_bps: 25,
            protocol_fee_bps: 5,
            creator_fee_bps: 0,
        }
    }

    fn sig(n: u8) -> Signature {
        Signature::from([n; 64])
    }

    fn tpool() -> Pubkey {
        Pubkey::new_from_array([7u8; 32])
    }

    fn readable(k: PumpIxKind) -> TxKind {
        TxKind::Readable {
            token_is_base: true,
            legs: vec![Leg {
                kind: k,
                base_amount: 1,
                quote_amount: 0,
                bound: 0,
            }],
        }
    }

    // Bug 1: an update for H must simulate the SUCCESSOR of H, never H itself.
    #[test]
    fn update_for_h_simulates_successor_not_h() {
        let now = Instant::now();
        let mut s = PoolSeq::new(tpool());
        s.enqueue(sig(1), 100, 1, readable(PumpIxKind::Sell), None, now);
        s.enqueue(sig(2), 100, 2, readable(PumpIxKind::Sell), None, now);
        let req = s
            .on_account_update(sig(1), pool(1_000, 2_000), now)
            .expect("successor of H");
        assert_eq!(req.sig, sig(2)); // NOT sig(1)
        assert_eq!(req.predecessor, sig(1));
        assert_eq!(req.pre_state.base_reserve, 1_000);
    }

    // Bug 2: order is by (slot, order_seq), NOT arrival order. Enqueue out of
    // order and confirm the successor is the block-next, not the arrival-next.
    #[test]
    fn orders_by_poh_seq_not_arrival() {
        let now = Instant::now();
        let mut s = PoolSeq::new(tpool());
        // Arrive out of order: seq 3 first, then seq 1, then seq 2.
        s.enqueue(sig(3), 100, 3, readable(PumpIxKind::Sell), None, now);
        s.enqueue(sig(1), 100, 1, readable(PumpIxKind::Sell), None, now);
        s.enqueue(sig(2), 100, 2, readable(PumpIxKind::Sell), None, now);
        // Update for H=seq1 → successor is seq2 (block order), not seq3.
        let req = s
            .on_account_update(sig(1), pool(10, 20), now)
            .expect("successor");
        assert_eq!(req.sig, sig(2));
        assert_eq!(req.order_seq, 2);
    }

    // Unreadable successor → wait for ITS update; then simulate the one after it.
    #[test]
    fn waits_on_unreadable_successor() {
        let now = Instant::now();
        let mut s = PoolSeq::new(tpool());
        s.enqueue(sig(1), 100, 1, readable(PumpIxKind::Sell), None, now);
        s.enqueue(sig(2), 100, 2, TxKind::Unreadable, None, now); // aggregator
        s.enqueue(sig(3), 100, 3, readable(PumpIxKind::Sell), None, now);
        // H=seq1 → successor seq2 is unreadable → wait.
        assert!(s.on_account_update(sig(1), pool(10, 20), now).is_none());
        // seq2 (aggregator) confirms → successor seq3 priced on seq2's reserves.
        let req = s
            .on_account_update(sig(2), pool(11, 19), now)
            .expect("seq3 after aggregator");
        assert_eq!(req.sig, sig(3));
        assert_eq!(req.pre_state.base_reserve, 11);
    }

    // Reverted H changed nothing → successor priced on unchanged reserves.
    #[test]
    fn reverted_successor_on_unchanged_reserves() {
        let now = Instant::now();
        let mut s = PoolSeq::new(tpool());
        s.enqueue(sig(1), 100, 1, readable(PumpIxKind::Sell), None, now);
        s.enqueue(sig(2), 100, 2, readable(PumpIxKind::Sell), None, now); // reverts
        s.enqueue(sig(3), 100, 3, readable(PumpIxKind::Sell), None, now);
        let req = s
            .on_account_update(sig(1), pool(10, 20), now)
            .expect("seq2");
        assert_eq!(req.sig, sig(2));
        // seq2 reverts → mark it, price seq3 on the SAME reserves.
        let req = s.on_tx_update(sig(2), true, now).expect("seq3");
        assert_eq!(req.sig, sig(3));
        assert_eq!(req.pre_state.base_reserve, 10);
    }

    // De-dup: repeated update for the same H does not re-emit the same successor.
    #[test]
    fn does_not_reemit_same_successor() {
        let now = Instant::now();
        let mut s = PoolSeq::new(tpool());
        s.enqueue(sig(1), 100, 1, readable(PumpIxKind::Sell), None, now);
        s.enqueue(sig(2), 100, 2, readable(PumpIxKind::Sell), None, now);
        assert!(s.on_account_update(sig(1), pool(10, 20), now).is_some());
        assert!(s.on_account_update(sig(1), pool(10, 20), now).is_none());
        assert!(s.on_account_update(sig(1), pool(10, 20), now).is_none());
    }

    // An update for a tx NOT in our ordered list is a no-op (can't position it).
    #[test]
    fn unknown_hash_is_noop() {
        let now = Instant::now();
        let mut s = PoolSeq::new(tpool());
        s.enqueue(sig(1), 100, 1, readable(PumpIxKind::Sell), None, now);
        assert!(s.on_account_update(sig(99), pool(10, 20), now).is_none());
    }

    // The successor can be enqueued AFTER its predecessor's update arrives — the
    // shred may lag. Enqueue must then trigger the simulation.
    #[test]
    fn enqueue_after_update_triggers_successor() {
        let now = Instant::now();
        let mut s = PoolSeq::new(tpool());
        // Only H is known; its update arrives → no successor yet.
        s.enqueue(sig(1), 100, 1, readable(PumpIxKind::Sell), None, now);
        assert!(s.on_account_update(sig(1), pool(10, 20), now).is_none());
        // The successor's shred arrives late → enqueue must emit it now.
        let req = s
            .enqueue(sig(2), 100, 2, readable(PumpIxKind::Sell), None, now)
            .expect("successor simulated on enqueue");
        assert_eq!(req.sig, sig(2));
        assert_eq!(req.predecessor, sig(1));
        assert_eq!(req.pre_state.base_reserve, 10);
    }

    // ANCHOR GUARD (the core bug): the reserves we price a successor on must belong
    // to that successor's IMMEDIATE predecessor. If a non-reverted tx sits between
    // the reserves anchor and the frontier (its account-update hasn't arrived), we
    // must WAIT — never price the successor on stale reserves from many txs back.
    // This is the exact "predecessor from blocks ago" scenario from the field log.
    #[test]
    fn waits_when_anchor_is_not_the_immediate_predecessor() {
        let now = Instant::now();
        let mut s = PoolSeq::new(tpool());
        // Three consecutive pool txs. order_seq is global, so use 10/20/30 to
        // reflect that they are NOT numerically adjacent (other pools' txs sit
        // between them in the global counter).
        s.enqueue(sig(1), 100, 10, readable(PumpIxKind::Sell), None, now);
        s.enqueue(sig(2), 100, 20, readable(PumpIxKind::Sell), None, now);
        s.enqueue(sig(3), 100, 30, readable(PumpIxKind::Sell), None, now);
        // Only seq1 confirmed. Frontier=seq1=anchor. Successor seq2 is emitted
        // (correct: seq2's predecessor IS seq1, whose reserves we hold).
        let r2 = s.on_account_update(sig(1), pool(10, 20), now).expect("seq2");
        assert_eq!(r2.sig, sig(2));
        assert_eq!(r2.predecessor, sig(1));
        // seq3 is NOT emitted yet: its predecessor seq2 is unconfirmed, so we have
        // no reserves that belong to seq2. Only after seq2's account-update:
        let r3 = s.on_account_update(sig(2), pool(11, 19), now).expect("seq3");
        assert_eq!(r3.sig, sig(3));
        assert_eq!(r3.predecessor, sig(2)); // anchor is seq2, the true predecessor
        assert_eq!(r3.pre_state.base_reserve, 11); // seq2's reserves, never seq1's
    }

    // The reserves anchor must belong to the successor's immediate predecessor.
    // If a reserve-CHANGING (non-revert) tx sits between the anchor and the
    // successor and we haven't confirmed it, we must wait — never price on stale
    // reserves. (This is the "predecessor many blocks ago" scenario.)
    #[test]
    fn waits_when_unconfirmed_tx_sits_between_anchor_and_successor() {
        let now = Instant::now();
        let mut s = PoolSeq::new(tpool());
        s.enqueue(sig(1), 100, 1, readable(PumpIxKind::Sell), None, now); // anchor after its update
        s.enqueue(sig(2), 100, 2, readable(PumpIxKind::Sell), None, now); // reserve-changing, unconfirmed
        s.enqueue(sig(3), 100, 3, readable(PumpIxKind::Sell), None, now); // successor
        // seq1 confirmed → reserves belong to seq1. seq2 is between seq1 and seq3
        // and is NOT reverted/confirmed → seq3 must WAIT (not price on seq1's state).
        assert!(s.on_account_update(sig(1), pool(10, 20), now).is_some());
        // ^ that emits seq2 (the true contiguous successor of seq1), which is correct.
        // seq3 must not have been emitted yet; only after seq2 confirms:
        let req = s
            .on_account_update(sig(2), pool(11, 19), now)
            .expect("seq3 after seq2 confirmed");
        assert_eq!(req.sig, sig(3));
        assert_eq!(req.pre_state.base_reserve, 11); // seq2's reserves, not seq1's
    }

    // Late-discovered revert must roll back: seq2 was priced OK on seq1's reserves,
    // then a tx-update says seq2 reverted → its successor seq3 must be re-priced on
    // the UNCHANGED (seq1) reserves, and the latch reset so it re-emits.
    #[test]
    fn late_revert_reprices_successor_on_unchanged_reserves() {
        let now = Instant::now();
        let mut s = PoolSeq::new(tpool());
        s.enqueue(sig(1), 100, 1, readable(PumpIxKind::Sell), None, now);
        s.enqueue(sig(2), 100, 2, readable(PumpIxKind::Sell), None, now);
        s.enqueue(sig(3), 100, 3, readable(PumpIxKind::Sell), None, now);
        // seq1 confirmed → emits seq2 on seq1's reserves.
        let r2 = s.on_account_update(sig(1), pool(10, 20), now).expect("seq2");
        assert_eq!(r2.sig, sig(2));
        // seq2 turns out reverted → seq3 must be emitted on the SAME (seq1) reserves.
        let r3 = s.on_tx_update(sig(2), true, now).expect("seq3 after seq2 revert");
        assert_eq!(r3.sig, sig(3));
        assert_eq!(r3.pre_state.base_reserve, 10); // unchanged seq1 reserves
        assert_eq!(r3.predecessor, sig(1)); // anchor is seq1, not the reverted seq2
    }

    // Per-pool sequence numbers are assigned 1,2,3,... in enqueue order, and the
    // emitted ComputeReq carries the successor's pool_seq.
    #[test]
    fn assigns_per_pool_sequence_numbers() {
        let now = Instant::now();
        let mut s = PoolSeq::new(tpool());
        // Global order_seq jumps (120, 500, 900) — the pool_seq must still be 1,2,3.
        s.enqueue(sig(1), 100, 120, readable(PumpIxKind::Sell), None, now);
        s.enqueue(sig(2), 100, 500, readable(PumpIxKind::Sell), None, now);
        s.enqueue(sig(3), 100, 900, readable(PumpIxKind::Sell), None, now);
        let r = s.on_account_update(sig(1), pool(10, 20), now).expect("seq2");
        assert_eq!(r.pool_seq, 2); // second tx on THIS pool
        assert_eq!(r.order_seq, 500); // its global index, for the label
    }

    // Label format matches the operator's scheme for single-hop and both two-hop
    // orientations.
    #[test]
    fn builds_operator_labels() {
        let now = Instant::now();
        let mut s = PoolSeq::new(tpool());
        let met = Pubkey::new_from_array([9u8; 32]);
        // seq1 single-hop Pump, seq2 two-hop with THIS pool as leg 1, seq3 two-hop
        // with THIS pool as leg 2 (Meteora leg first).
        s.enqueue(sig(1), 100, 120, readable(PumpIxKind::Sell), None, now);
        s.enqueue(
            sig(2), 100, 540, readable(PumpIxKind::Sell),
            Some(MultiHop { other_pool: met, other_dex: "Meteora_DAMM_v2", on_this_pool_leg: 1 }),
            now,
        );
        s.enqueue(
            sig(3), 100, 999, readable(PumpIxKind::Sell),
            Some(MultiHop { other_pool: met, other_dex: "Meteora_DAMM_v2", on_this_pool_leg: 2 }),
            now,
        );
        // Drive to emit seq2 (label of the two-hop leg-1 case).
        let r2 = s.on_account_update(sig(1), pool(10, 20), now).expect("seq2");
        assert!(r2.label.starts_with("2_540_"), "got {}", r2.label);
        assert!(r2.label.contains("_pump_fun_AMM-"), "leg1 pump first: {}", r2.label);
        assert!(r2.label.ends_with("_Meteora_DAMM_v2"), "leg2 meteora: {}", r2.label);
        // Emit seq3 (leg-2 case → Meteora shown first).
        let r3 = s.on_account_update(sig(2), pool(11, 19), now).expect("seq3");
        assert!(r3.label.starts_with("3_999_"), "got {}", r3.label);
        assert!(r3.label.contains("_Meteora_DAMM_v2-"), "meteora first: {}", r3.label);
        assert!(r3.label.ends_with("_pump_fun_AMM"), "pump last: {}", r3.label);
    }
}
