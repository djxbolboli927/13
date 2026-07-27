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
use solana_sdk::signature::Signature;
use std::collections::VecDeque;
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
    /// An account-update confirmed this tx's resulting pool state (or it was a
    /// confirmed-reverted step we've walked past).
    settled: bool,
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
}

/// The ordered ledger for a single pool.
pub struct PoolSeq {
    queue: VecDeque<QueuedTx>,
    /// Reserves after the last account-update we accepted (the confirmed
    /// frontier). `None` until the first account-update arrives.
    confirmed_state: Option<PumpPool>,
    confirmed_sig: Option<Signature>,
}

impl Default for PoolSeq {
    fn default() -> Self {
        Self {
            queue: VecDeque::with_capacity(64),
            confirmed_state: None,
            confirmed_sig: None,
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
            settled: false,
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

    /// An account-update arrived for this pool: `sig` produced state `reserves`.
    /// This is the authoritative confirmed frontier. Mark the queue up to `sig`
    /// as settled, adopt the reserves, and return the NEXT tx to simulate — but
    /// only if it is readable. An unreadable next tx returns `None` (we wait for
    /// ITS account-update, never skipping it).
    pub fn on_account_update(
        &mut self,
        sig: Signature,
        reserves: PumpPool,
        now: Instant,
    ) -> Option<ComputeReq> {
        self.confirmed_state = Some(reserves);
        self.confirmed_sig = Some(sig);
        // Settle everything up to and including `sig` (if we've queued it).
        if let Some(pos) = self.queue.iter().position(|q| q.sig == sig) {
            for q in self.queue.iter_mut().take(pos + 1) {
                q.settled = true;
            }
        }
        self.prune(now);
        self.next_compute()
    }

    /// A transaction-update arrived for `sig` with `reverted`. A reverted tx
    /// leaves the pool unchanged and will NOT produce an account-update, so we
    /// settle it in place (step over it) and can immediately price the tx after
    /// it on the unchanged confirmed state. A non-reverted tx-update is
    /// informational here (its account-update drives the frontier).
    pub fn on_tx_update(&mut self, sig: Signature, reverted: bool) -> Option<ComputeReq> {
        if let Some(q) = self.queue.iter_mut().find(|q| q.sig == sig) {
            if reverted {
                q.reverted = true;
                q.settled = true;
            }
        }
        if reverted {
            self.next_compute()
        } else {
            None
        }
    }

    /// The first queued tx that is not yet settled and not reverted, priced on
    /// the confirmed frontier — if it is readable.
    fn next_compute(&self) -> Option<ComputeReq> {
        let pre = self.confirmed_state?;
        let next = self
            .queue
            .iter()
            .find(|q| !q.settled && !q.reverted)?;
        match &next.kind {
            TxKind::Readable {
                token_is_base,
                legs,
            } => Some(ComputeReq {
                sig: next.sig,
                slot: next.slot,
                token_is_base: *token_is_base,
                legs: legs.clone(),
                pre_state: pre,
            }),
            // Unreadable: we cannot advance past it by computation — hold until
            // its own account-update lands and moves the frontier.
            TxKind::Unreadable => None,
        }
    }

    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }

    pub fn confirmed_sig(&self) -> Option<Signature> {
        self.confirmed_sig
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
            }],
        }
    }

    #[test]
    fn computes_next_tx_on_confirmed_predecessor_state() {
        let now = Instant::now();
        let mut s = PoolSeq::default();
        s.enqueue(sig(1), 100, readable(PumpIxKind::Sell, 10, 0), now);
        s.enqueue(sig(2), 100, readable(PumpIxKind::Sell, 20, 0), now);
        // Account-update for tx1 → its reserves are the pre-state for tx2.
        let req = s
            .on_account_update(sig(1), pool(1_000, 2_000), now)
            .expect("tx2 should be ready");
        assert_eq!(req.sig, sig(2));
        assert_eq!(req.pre_state.base_reserve, 1_000);
        assert_eq!(req.pre_state.quote_reserve, 2_000);
    }

    #[test]
    fn waits_on_unreadable_next_never_skips() {
        let now = Instant::now();
        let mut s = PoolSeq::default();
        s.enqueue(sig(1), 100, readable(PumpIxKind::Sell, 10, 0), now);
        s.enqueue(sig(2), 100, TxKind::Unreadable, now); // aggregator
        s.enqueue(sig(3), 100, readable(PumpIxKind::Sell, 30, 0), now);
        // After tx1 confirms, the next is the UNREADABLE tx2 → must wait, not
        // jump to tx3.
        assert!(s.on_account_update(sig(1), pool(1_000, 2_000), now).is_none());
        // When tx2 (the aggregator) confirms with its own reserves, tx3 is now
        // priced on tx2's resulting state.
        let req = s
            .on_account_update(sig(2), pool(1_100, 1_800), now)
            .expect("tx3 ready after aggregator confirms");
        assert_eq!(req.sig, sig(3));
        assert_eq!(req.pre_state.base_reserve, 1_100);
    }

    #[test]
    fn reverted_tx_is_stepped_over_on_unchanged_state() {
        let now = Instant::now();
        let mut s = PoolSeq::default();
        s.enqueue(sig(1), 100, readable(PumpIxKind::Sell, 10, 0), now);
        s.enqueue(sig(2), 100, readable(PumpIxKind::Sell, 20, 0), now); // will revert
        s.enqueue(sig(3), 100, readable(PumpIxKind::Sell, 30, 0), now);
        // tx1 confirms → tx2 is the candidate.
        let req = s
            .on_account_update(sig(1), pool(1_000, 2_000), now)
            .expect("tx2 ready");
        assert_eq!(req.sig, sig(2));
        // tx2's transaction-update says REVERT (no account-update will come) →
        // step over it and price tx3 on the SAME unchanged reserves.
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
