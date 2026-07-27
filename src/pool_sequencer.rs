//! Per-pool ORDERED transaction sequencer — the confirmation-gated, one-tx-at-a-
//! time simulator described by the operator.
//!
//! The problem the diagnostics proved: the bot priced a transaction on whatever
//! pool state happened to be cached at that instant, which — because Yellowstone
//! account-updates arrive per slot and the shred backlog runs behind — was
//! usually the SAME tx's own post-state, or a tx many positions later. So every
//! simulation started from the wrong reserves.
//!
//! The fix keys off two facts about the gRPC streams:
//!   • every ACCOUNT-update carries `txn_signature` — the exact tx that produced
//!     this pool state (see pool_state), plus the reserves themselves;
//!   • every TRANSACTION-update carries the signature + whether it reverted.
//!
//! So we can reconstruct the pool's true transaction order and advance a state
//! machine strictly one confirmed tx at a time:
//!   1. Shreds ENQUEUE every tx we see for the pool, in order, tagged readable
//!      (we decoded its swap) or unreadable (aggregator/router CPI we can't).
//!   2. When the account-update for the tx at the CONFIRMED FRONTIER arrives, its
//!      reserves become the authoritative pre-state for the NEXT queued tx. If
//!      that next tx is readable we compute it on exactly those reserves; if it
//!      is unreadable we WAIT — never skip it — for its own account-update.
//!   3. A tx that gets a transaction-update but NO account-update reverted: it
//!      left the pool unchanged, so we step over it on the unchanged state.
//! Entries older than `MAX_AGE` (10s ≈ a few blocks) fall off the back.
//!
//! This module is the PURE logic (no async, no I/O) so it can be unit-tested to
//! the operator's exact rules; the engine wires the three events into it.

use crate::pumpfun_math::PumpPool;
use crate::shred_stream::PumpIxKind;
use dashmap::DashMap;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How long a tx stays in the ordered queue before it's discarded (roughly a few
/// Solana blocks — long enough to bridge shred → account-update, short enough to
/// never grow unbounded).
pub const MAX_AGE: Duration = Duration::from_secs(10);

/// One decoded Pump swap leg (a tx may carry several — the sell+buy sandwich).
#[derive(Clone, Debug)]
pub struct Leg {
    pub kind: PumpIxKind,
    pub base_amount: u64,
    pub quote_amount: u64,
    /// The tx's own slippage bound (arg1 of the instruction): min_quote_out for
    /// sell, max_quote_in for buy, min_base_out for the exact-quote-in / boost
    /// sides. Drives the revert verdict.
    pub bound: u64,
}

/// What we know about a queued tx's effect on THIS pool.
#[derive(Clone, Debug)]
pub enum TxKind {
    /// We decoded the pool's swap(s) from the shred: `token_is_base` gives the
    /// orientation, `legs` are the ordered swaps on this pool within the tx.
    Readable { token_is_base: bool, legs: Vec<Leg> },
    /// The pool is touched through a CPI we can't decode (aggregator/router/bot).
    /// We must WAIT for its account-update to learn the resulting reserves.
    Unreadable,
}

#[derive(Clone, Debug)]
struct QueuedTx {
    sig: Signature,
    slot: u64,
    kind: TxKind,
    seen: Instant,
    /// A transaction-update said this tx reverted → it changed nothing.
    reverted: bool,
    /// We already emitted ONE prediction for this tx against the current
    /// confirmed frontier. Prevents the "same tx simulated over and over" loop:
    /// a pending tx is predicted once, then we wait for its OWN account-update to
    /// confirm it (which removes it) before the next tx is predicted.
    predicted: bool,
}

/// The confirmed frontier for a pool: the reserves after the newest account-
/// update we've accepted, tagged with the tx that produced it and its ordering
/// keys `(slot, write_version)`.
#[derive(Clone, Copy, Debug)]
struct Confirmed {
    state: PumpPool,
    sig: Signature,
    slot: u64,
    write_version: u64,
}

/// A request to simulate one tx against a known pre-state — emitted the moment
/// the sequencer can price the next queued tx with confidence.
#[derive(Clone, Debug)]
pub struct ComputeReq {
    pub sig: Signature,
    pub slot: u64,
    pub token_is_base: bool,
    pub legs: Vec<Leg>,
    /// The exact reserves this tx executes against (the confirmed state of its
    /// immediate predecessor).
    pub pre_state: PumpPool,
    /// The tx whose account-update produced `pre_state` — logged so it's provable
    /// that we compute the tx AFTER the confirmed one, on ITS reserves.
    pub predecessor: Option<Signature>,
    /// The `(slot, write_version)` of that confirmed account-update.
    pub confirmed_slot: u64,
    pub confirmed_write_version: u64,
}

/// The ordered ledger for a single pool.
pub struct PoolSeq {
    queue: VecDeque<QueuedTx>,
    /// The confirmed frontier, driven ENTIRELY by account-updates ordered by
    /// `(slot, write_version)` — never by matching shred-queue signatures. `None`
    /// until the first account-update arrives.
    confirmed: Option<Confirmed>,
}

impl Default for PoolSeq {
    fn default() -> Self {
        Self {
            queue: VecDeque::with_capacity(64),
            confirmed: None,
        }
    }
}

impl PoolSeq {
    /// Record a tx we saw on the shred stream, preserving arrival order. Dedupes
    /// by signature (shreds can repeat). Prunes anything older than `MAX_AGE`.
    pub fn enqueue(&mut self, sig: Signature, slot: u64, kind: TxKind, now: Instant) {
        if self.queue.iter().any(|q| q.sig == sig) {
            return;
        }
        self.queue.push_back(QueuedTx {
            sig,
            slot,
            kind,
            seen: now,
            reverted: false,
            predicted: false,
        });
        self.prune(now);
    }

    fn prune(&mut self, now: Instant) {
        while let Some(front) = self.queue.front() {
            if now.duration_since(front.seen) > MAX_AGE {
                self.queue.pop_front();
            } else {
                break;
            }
        }
    }

    /// An account-update arrived: `sig` produced `reserves` at `(slot, wv)`. This
    /// is the ground-truth confirmed state — adopt it ONLY if it is strictly
    /// newer than our current frontier (updates can arrive out of order; a stale
    /// one must not clobber a newer state). Any queued shred tx equal to `sig` is
    /// now confirmed → remove it. Then predict the next pending tx (the one AFTER
    /// this confirmed one) on these fresh reserves. Returns `None` if the front
    /// pending tx is unreadable (wait for its account-update) or already
    /// predicted (wait for its confirmation).
    pub fn on_account_update(
        &mut self,
        sig: Signature,
        reserves: PumpPool,
        slot: u64,
        write_version: u64,
        now: Instant,
    ) -> Option<ComputeReq> {
        if let Some(c) = &self.confirmed {
            if (slot, write_version) <= (c.slot, c.write_version) {
                return None; // stale / duplicate — ignore
            }
        }
        self.confirmed = Some(Confirmed {
            state: reserves,
            sig,
            slot,
            write_version,
        });
        // This tx is now confirmed on-chain — it's no longer a pending
        // prediction. Removing it lets the NEXT queued tx become the front.
        self.queue.retain(|q| q.sig != sig);
        self.prune(now);
        self.next_compute()
    }

    /// A transaction-update arrived for `sig`. A reverted tx changed nothing and
    /// produces no account-update, so drop it from the pending queue and let the
    /// next tx be predicted on the unchanged confirmed state. A non-reverted
    /// tx-update is informational — its account-update drives the frontier.
    pub fn on_tx_update(&mut self, sig: Signature, reverted: bool) -> Option<ComputeReq> {
        if reverted {
            let before = self.queue.len();
            self.queue.retain(|q| q.sig != sig);
            if self.queue.len() != before {
                return self.next_compute();
            }
        }
        None
    }

    /// Predict the FRONT pending tx (earliest we haven't confirmed) on the
    /// confirmed reserves — exactly once. We look ONLY at the front: if it's
    /// unreadable we wait (never skip past it), and if it's already been
    /// predicted we wait for its account-update to confirm-and-remove it. This
    /// preserves order and structurally prevents re-predicting the same tx.
    fn next_compute(&mut self) -> Option<ComputeReq> {
        let c = self.confirmed?;
        let front = self.queue.iter_mut().find(|q| !q.reverted)?;
        match &front.kind {
            TxKind::Unreadable => None, // wait for its own account-update
            TxKind::Readable {
                token_is_base,
                legs,
            } => {
                if front.predicted {
                    return None; // already priced against the frontier; wait
                }
                front.predicted = true;
                Some(ComputeReq {
                    sig: front.sig,
                    slot: front.slot,
                    token_is_base: *token_is_base,
                    legs: legs.clone(),
                    pre_state: c.state,
                    predecessor: Some(c.sig),
                    confirmed_slot: c.slot,
                    confirmed_write_version: c.write_version,
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
    /// An account-update: `account` moved to a new state, produced by `sig`.
    Account {
        account: Pubkey,
        sig: Option<Signature>,
        slot: u64,
    },
    /// A transaction-update: `sig` did or did not revert.
    Tx { sig: Signature, reverted: bool },
}

/// Concurrent per-pool sequencer: one `PoolSeq` per Pump pool, each behind its
/// own lock. Shreds enqueue; the gRPC stream drives the account/tx events.
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
    pub fn enqueue(&self, pool: Pubkey, sig: Signature, slot: u64, kind: TxKind, now: Instant) {
        self.pools
            .entry(pool)
            .or_insert_with(|| Mutex::new(PoolSeq::default()))
            .lock()
            .unwrap()
            .enqueue(sig, slot, kind, now);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn on_account_update(
        &self,
        pool: Pubkey,
        sig: Signature,
        reserves: PumpPool,
        slot: u64,
        write_version: u64,
        now: Instant,
    ) -> Option<ComputeReq> {
        self.pools
            .entry(pool)
            .or_insert_with(|| Mutex::new(PoolSeq::default()))
            .lock()
            .unwrap()
            .on_account_update(sig, reserves, slot, write_version, now)
    }

    /// A transaction-update whose pool we don't know up front — try every pool's
    /// queue (there are only a handful). Returns each pool that produced a new
    /// compute request as a result (a reverted tx being stepped over).
    pub fn on_tx_update_any(&self, sig: Signature, reverted: bool) -> Vec<(Pubkey, ComputeReq)> {
        let mut out = Vec::new();
        for e in self.pools.iter() {
            if let Some(req) = e.value().lock().unwrap().on_tx_update(sig, reverted) {
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

    fn readable(k: PumpIxKind, base: u64, quote: u64) -> TxKind {
        TxKind::Readable {
            token_is_base: true,
            legs: vec![Leg {
                kind: k,
                base_amount: base,
                quote_amount: quote,
                bound: 0,
            }],
        }
    }

    #[test]
    fn computes_next_tx_on_confirmed_predecessor_state() {
        let now = Instant::now();
        let mut s = PoolSeq::default();
        s.enqueue(sig(1), 100, readable(PumpIxKind::Sell, 10, 0), now);
        s.enqueue(sig(2), 100, readable(PumpIxKind::Sell, 20, 0), now);
        // Account-update for tx1 (slot 100, wv 5) → tx1 removed (confirmed), and
        // tx2 is predicted on tx1's resulting reserves.
        let req = s
            .on_account_update(sig(1), pool(1_000, 2_000), 100, 5, now)
            .expect("tx2 should be ready");
        assert_eq!(req.sig, sig(2));
        assert_eq!(req.predecessor, Some(sig(1)));
        assert_eq!(req.pre_state.base_reserve, 1_000);
        assert_eq!(req.confirmed_write_version, 5);
    }

    #[test]
    fn does_not_repredict_same_tx_on_unrelated_updates() {
        // THE regression test for the observed bug: account-updates for txs NOT
        // in our queue must advance the frontier but must NOT re-predict the same
        // pending tx over and over.
        let now = Instant::now();
        let mut s = PoolSeq::default();
        s.enqueue(sig(1), 100, readable(PumpIxKind::Sell, 10, 0), now);
        // An unrelated tx (sig 99, not in queue) confirms → frontier advances,
        // tx1 predicted once.
        let req = s
            .on_account_update(sig(99), pool(1_000, 2_000), 100, 5, now)
            .expect("tx1 predicted once");
        assert_eq!(req.sig, sig(1));
        // More unrelated updates (newer wv) must NOT re-emit tx1.
        assert!(s
            .on_account_update(sig(98), pool(1_010, 1_990), 100, 6, now)
            .is_none());
        assert!(s
            .on_account_update(sig(97), pool(1_020, 1_980), 100, 7, now)
            .is_none());
        // A STALE update (older wv) is ignored entirely.
        assert!(s
            .on_account_update(sig(96), pool(9, 9), 100, 4, now)
            .is_none());
        // Only when tx1's OWN account-update lands is it removed and the next
        // (none here) considered.
        assert!(s
            .on_account_update(sig(1), pool(1_030, 1_970), 100, 8, now)
            .is_none());
        assert_eq!(s.queue_len(), 0);
    }

    #[test]
    fn waits_on_unreadable_front_never_skips() {
        let now = Instant::now();
        let mut s = PoolSeq::default();
        s.enqueue(sig(2), 100, TxKind::Unreadable, now); // aggregator, front
        s.enqueue(sig(3), 100, readable(PumpIxKind::Sell, 30, 0), now);
        // Front is the UNREADABLE tx2 → wait, never jump to tx3.
        assert!(s
            .on_account_update(sig(99), pool(1_000, 2_000), 100, 5, now)
            .is_none());
        // When tx2 (aggregator) confirms it is removed → tx3 becomes front and is
        // priced on tx2's resulting reserves.
        let req = s
            .on_account_update(sig(2), pool(1_100, 1_800), 100, 6, now)
            .expect("tx3 ready after aggregator confirms");
        assert_eq!(req.sig, sig(3));
        assert_eq!(req.pre_state.base_reserve, 1_100);
    }

    #[test]
    fn reverted_tx_is_stepped_over_on_unchanged_state() {
        let now = Instant::now();
        let mut s = PoolSeq::default();
        s.enqueue(sig(2), 100, readable(PumpIxKind::Sell, 20, 0), now); // reverts
        s.enqueue(sig(3), 100, readable(PumpIxKind::Sell, 30, 0), now);
        // Frontier set; tx2 (front) predicted.
        let req = s
            .on_account_update(sig(99), pool(1_000, 2_000), 100, 5, now)
            .expect("tx2 predicted");
        assert_eq!(req.sig, sig(2));
        // tx2 reverts (no account-update) → dropped, tx3 priced on unchanged
        // reserves.
        let req = s.on_tx_update(sig(2), true).expect("tx3 ready after revert");
        assert_eq!(req.sig, sig(3));
        assert_eq!(req.pre_state.base_reserve, 1_000);
    }

    #[test]
    fn old_entries_are_pruned() {
        let now = Instant::now();
        let mut s = PoolSeq::default();
        s.enqueue(sig(1), 100, readable(PumpIxKind::Sell, 10, 0), now);
        let later = now + MAX_AGE + Duration::from_secs(1);
        s.enqueue(sig(2), 101, readable(PumpIxKind::Sell, 20, 0), later);
        // tx1 aged out; only tx2 remains.
        assert_eq!(s.queue_len(), 1);
    }
}
