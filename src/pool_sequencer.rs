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

#[derive(Clone, Debug)]
struct QueuedTx {
    sig: Signature,
    slot: u64,
    order_seq: u64,
    kind: TxKind,
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
    pub token_is_base: bool,
    pub legs: Vec<Leg>,
    /// The exact reserves this tx executes against — the confirmed reserves of
    /// its immediate predecessor.
    pub pre_state: PumpPool,
    /// The confirmed tx whose update triggered this (H). Logged so it's provable
    /// we compute the tx AFTER H, not H itself.
    pub predecessor: Signature,
}

/// The ordered ledger for a single pool.
pub struct PoolSeq {
    /// All txs we've seen for this pool, kept sorted by `(slot, order_seq)` —
    /// the leader's block order.
    queue: Vec<QueuedTx>,
    /// The latest confirmed reserves (from the last account-update we matched to
    /// a queued tx). `None` until the first match.
    reserves: Option<PumpPool>,
    /// The CONFIRMED FRONTIER: `(slot, order_seq, sig)` of the last confirmed tx
    /// H. The tx we want to simulate is the first one strictly after this. Set by
    /// an account-update (H succeeded, `reserves` are H's result) or a revert
    /// (H changed nothing, `reserves` unchanged). Only advances forward.
    confirmed: Option<(u64, u64, Signature)>,
    /// The `(slot, order_seq)` of the successor we last emitted, so we price each
    /// successor exactly once no matter how many events poke us.
    last_emitted: Option<(u64, u64)>,
}

impl Default for PoolSeq {
    fn default() -> Self {
        Self {
            queue: Vec::with_capacity(64),
            reserves: None,
            confirmed: None,
            last_emitted: None,
        }
    }
}

impl PoolSeq {
    /// Record a tx from the shred stream at its PoH `order_seq`. Kept sorted by
    /// `(slot, order_seq)`; deduped by signature; pruned by age. Returns a
    /// compute request if this newly-arrived tx is the awaited successor of the
    /// confirmed frontier — the shred can arrive AFTER the predecessor's update,
    /// so enqueue must also be able to trigger the simulation.
    pub fn enqueue(
        &mut self,
        sig: Signature,
        slot: u64,
        order_seq: u64,
        kind: TxKind,
        now: Instant,
    ) -> Option<ComputeReq> {
        self.prune(now);
        if self.queue.iter().any(|q| q.sig == sig) {
            return None;
        }
        let item = QueuedTx {
            sig,
            slot,
            order_seq,
            kind,
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
        if self.advance_confirmed(pos) {
            self.reserves = Some(reserves);
        }
        self.emit_next()
    }

    /// A transaction-update arrived for `sig`. A reverted tx changed nothing (no
    /// account-update will come): advance the frontier past it and price the
    /// successor on the UNCHANGED reserves. A non-reverted tx-update is redundant
    /// with its account-update — leave the reserves to that.
    pub fn on_tx_update(&mut self, sig: Signature, reverted: bool, now: Instant) -> Option<ComputeReq> {
        self.prune(now);
        let pos = self.position_of(&sig)?;
        if reverted {
            self.queue[pos].reverted = true;
            self.advance_confirmed(pos);
        }
        self.emit_next()
    }

    fn position_of(&self, sig: &Signature) -> Option<usize> {
        self.queue.iter().position(|q| q.sig == *sig)
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

    /// Simulate the first non-reverted tx strictly after the confirmed frontier,
    /// on the confirmed reserves — exactly once. Unreadable successor → wait.
    fn emit_next(&mut self) -> Option<ComputeReq> {
        let pre = self.reserves?;
        let (cslot, cseq, csig) = self.confirmed?;
        let next = self
            .queue
            .iter()
            .find(|q| (q.slot, q.order_seq) > (cslot, cseq) && !q.reverted)?;
        let next_key = (next.slot, next.order_seq);
        match &next.kind {
            TxKind::Unreadable => None, // wait for its own update
            TxKind::Readable { token_is_base, legs } => {
                if self.last_emitted == Some(next_key) {
                    return None; // already priced this successor
                }
                self.last_emitted = Some(next_key);
                Some(ComputeReq {
                    sig: next.sig,
                    slot: next.slot,
                    order_seq: next.order_seq,
                    token_is_base: *token_is_base,
                    legs: legs.clone(),
                    pre_state: pre,
                    predecessor: csig,
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
        now: Instant,
    ) -> Option<ComputeReq> {
        self.pools
            .entry(pool)
            .or_insert_with(|| Mutex::new(PoolSeq::default()))
            .lock()
            .unwrap()
            .enqueue(sig, slot, order_seq, kind, now)
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
            .or_insert_with(|| Mutex::new(PoolSeq::default()))
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
        let mut s = PoolSeq::default();
        s.enqueue(sig(1), 100, 1, readable(PumpIxKind::Sell), now);
        s.enqueue(sig(2), 100, 2, readable(PumpIxKind::Sell), now);
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
        let mut s = PoolSeq::default();
        // Arrive out of order: seq 3 first, then seq 1, then seq 2.
        s.enqueue(sig(3), 100, 3, readable(PumpIxKind::Sell), now);
        s.enqueue(sig(1), 100, 1, readable(PumpIxKind::Sell), now);
        s.enqueue(sig(2), 100, 2, readable(PumpIxKind::Sell), now);
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
        let mut s = PoolSeq::default();
        s.enqueue(sig(1), 100, 1, readable(PumpIxKind::Sell), now);
        s.enqueue(sig(2), 100, 2, TxKind::Unreadable, now); // aggregator
        s.enqueue(sig(3), 100, 3, readable(PumpIxKind::Sell), now);
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
        let mut s = PoolSeq::default();
        s.enqueue(sig(1), 100, 1, readable(PumpIxKind::Sell), now);
        s.enqueue(sig(2), 100, 2, readable(PumpIxKind::Sell), now); // reverts
        s.enqueue(sig(3), 100, 3, readable(PumpIxKind::Sell), now);
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
        let mut s = PoolSeq::default();
        s.enqueue(sig(1), 100, 1, readable(PumpIxKind::Sell), now);
        s.enqueue(sig(2), 100, 2, readable(PumpIxKind::Sell), now);
        assert!(s.on_account_update(sig(1), pool(10, 20), now).is_some());
        assert!(s.on_account_update(sig(1), pool(10, 20), now).is_none());
        assert!(s.on_account_update(sig(1), pool(10, 20), now).is_none());
    }

    // An update for a tx NOT in our ordered list is a no-op (can't position it).
    #[test]
    fn unknown_hash_is_noop() {
        let now = Instant::now();
        let mut s = PoolSeq::default();
        s.enqueue(sig(1), 100, 1, readable(PumpIxKind::Sell), now);
        assert!(s.on_account_update(sig(99), pool(10, 20), now).is_none());
    }

    // The successor can be enqueued AFTER its predecessor's update arrives — the
    // shred may lag. Enqueue must then trigger the simulation.
    #[test]
    fn enqueue_after_update_triggers_successor() {
        let now = Instant::now();
        let mut s = PoolSeq::default();
        // Only H is known; its update arrives → no successor yet.
        s.enqueue(sig(1), 100, 1, readable(PumpIxKind::Sell), now);
        assert!(s.on_account_update(sig(1), pool(10, 20), now).is_none());
        // The successor's shred arrives late → enqueue must emit it now.
        let req = s
            .enqueue(sig(2), 100, 2, readable(PumpIxKind::Sell), now)
            .expect("successor simulated on enqueue");
        assert_eq!(req.sig, sig(2));
        assert_eq!(req.predecessor, sig(1));
        assert_eq!(req.pre_state.base_reserve, 10);
    }
}
