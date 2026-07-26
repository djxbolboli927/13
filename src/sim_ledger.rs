//! Phase-1 diagnostic ledger: our MANUAL per-transaction simulation verdicts,
//! reconciled against the ground truth that arrives later on the gRPC
//! transaction-update stream.
//!
//! The whole point of ShredStream is that we see a transaction (already ordered
//! and executed on the leader) BEFORE its result is propagated. In that window
//! the only way to know whether that transaction reverted is to re-run its math
//! on our own local pool state. This ledger records, per competitor transaction
//! that touched one of OUR pools:
//!   • what we computed (output, the tx's own slippage bound, our OK/REVERT
//!     verdict), its instruction TYPE, how many hops (1 = single swap, 2 = a
//!     Pump↔Meteora arb), and which DEX + which POOL(s) — never the token.
//! Then, when the gRPC account-update + transaction-update for the same slot
//! arrive, `reconcile` compares our verdict to the real on-chain `err` and logs
//! the match/mismatch so we can measure exactly where (and by how much) our
//! calculation diverges from reality. It changes NO trading decision — it is a
//! measurement layer only (Phase 1).

use dashmap::DashMap;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::info;

/// One transaction's simulated result, keyed by its signature.
#[derive(Clone)]
pub struct SimRecord {
    pub slot: u64,
    /// 1 = single swap on one of our pools; 2 = touches BOTH a Pump and a
    /// Meteora pool of ours (a circular arb).
    pub hops: u8,
    pub pump_pool: Option<Pubkey>,
    pub meteora_pool: Option<Pubkey>,
    /// Human label of the Pump instruction kind ("buy", "sell",
    /// "buy_exact_quote_in", "boost", or "-" if no Pump leg).
    pub kind: &'static str,
    // Pump leg (0 if absent).
    pub pump_in: u64,
    pub pump_out: u64,
    pub pump_bound: u64,
    pub pump_revert: bool,
    pub pump_present: bool,
    // Meteora leg (0 if absent).
    pub met_in: u64,
    pub met_out: u64,
    pub met_bound: u64,
    pub met_revert: bool,
    pub met_present: bool,
    /// Whole-tx verdict = ANY present leg reverts (a 2-hop arb reverts if either
    /// leg fails its slippage).
    pub tx_revert: bool,
    /// Token-2022 transfer-fee rate read from the token's mint state account
    /// (basis points). Usually 0 for these tokens, but read live so we never
    /// assume — logged so it can be compared against the real tx on solscan.
    pub tfee_bps: u16,
    /// Token-2022 transfer fee (in token base units) taken on the token amount
    /// that moved in this swap, at `tfee_bps`. 0 when the mint has no transfer
    /// fee extension.
    pub tfee_token: u64,
}

impl SimRecord {
    fn dex_label(&self) -> &'static str {
        match (self.pump_present, self.met_present) {
            (true, true) => "Pump+Meteora",
            (true, false) => "Pump",
            (false, true) => "Meteora",
            (false, false) => "-",
        }
    }
    fn pools_str(&self) -> String {
        match (self.pump_pool, self.meteora_pool) {
            (Some(p), Some(m)) => format!("pump={p} meteora={m}"),
            (Some(p), None) => format!("pump={p}"),
            (None, Some(m)) => format!("meteora={m}"),
            (None, None) => "-".to_string(),
        }
    }
}

pub struct SimLedger {
    records: DashMap<Signature, SimRecord>,
    /// Records inserted (transactions we simulated).
    pub recorded: AtomicU64,
    /// Reconcile checks performed (a tx-update matched one of our records).
    pub checks: AtomicU64,
    /// Our verdict agreed with the real on-chain err.
    pub matched: AtomicU64,
    /// Our verdict disagreed (missed revert or false revert).
    pub mismatched: AtomicU64,
    /// tx-updates on our pools for a signature we NEVER simulated (a swap type
    /// / path we don't decode yet — e.g. a pure-Meteora tx, a router CPI).
    pub unsimulated: AtomicU64,
}

impl Default for SimLedger {
    fn default() -> Self {
        Self {
            records: DashMap::with_capacity(4096),
            recorded: AtomicU64::new(0),
            checks: AtomicU64::new(0),
            matched: AtomicU64::new(0),
            mismatched: AtomicU64::new(0),
            unsimulated: AtomicU64::new(0),
        }
    }
}

impl SimLedger {
    /// Record our simulation verdict for a competitor tx and emit the `[sim]`
    /// line. Bounded: if the map grows huge (reconciles not arriving) we stop
    /// inserting rather than leak — the counters still move.
    pub fn record(&self, sig: Signature, rec: SimRecord) {
        info!(
            target: "sim",
            %sig,
            slot = rec.slot,
            hops = rec.hops,
            dex = rec.dex_label(),
            pools = %rec.pools_str(),
            kind = rec.kind,
            pump_in = rec.pump_in,
            pump_out = rec.pump_out,
            pump_bound = rec.pump_bound,
            pump_verdict = if rec.pump_revert { "REVERT" } else { "OK" },
            met_in = rec.met_in,
            met_out = rec.met_out,
            met_bound = rec.met_bound,
            met_verdict = if rec.met_revert { "REVERT" } else { "OK" },
            tfee_bps = rec.tfee_bps,
            tfee_token = rec.tfee_token,
            tx_verdict = if rec.tx_revert { "REVERT" } else { "OK" },
            "sim"
        );
        crate::errlog::log(
            "sim",
            &format!(
                "slot={} sig={sig} hops={} dex={} {} kind={} pump[in={} out={} bound={} verdict={}] \
                 meteora[in={} out={} bound={} verdict={}] token2022_fee[bps={} taken={}] tx_verdict={}",
                rec.slot, rec.hops, rec.dex_label(), rec.pools_str(), rec.kind,
                rec.pump_in, rec.pump_out, rec.pump_bound,
                if rec.pump_revert { "REVERT" } else { "OK" },
                rec.met_in, rec.met_out, rec.met_bound,
                if rec.met_revert { "REVERT" } else { "OK" },
                rec.tfee_bps, rec.tfee_token,
                if rec.tx_revert { "REVERT" } else { "OK" },
            ),
        );
        self.recorded.fetch_add(1, Ordering::Relaxed);
        if self.records.len() < 200_000 {
            self.records.insert(sig, rec);
        }
    }

    /// Ground truth from the gRPC transaction-update stream: does our verdict for
    /// `sig` match the real on-chain outcome? Logs `[reconcile-tx]` and bumps the
    /// match/mismatch counters. `real_revert = meta.err.is_some()`.
    pub fn reconcile(&self, sig: &Signature, slot: u64, real_revert: bool) {
        let Some((_, rec)) = self.records.remove(sig) else {
            // A tx touched our pool that we never simulated (unknown swap type
            // or a path we don't decode). This is itself a diagnostic signal.
            self.unsimulated.fetch_add(1, Ordering::Relaxed);
            return;
        };
        self.checks.fetch_add(1, Ordering::Relaxed);
        let matched = rec.tx_revert == real_revert;
        if matched {
            self.matched.fetch_add(1, Ordering::Relaxed);
        } else {
            self.mismatched.fetch_add(1, Ordering::Relaxed);
        }
        info!(
            target: "reconcile",
            %sig,
            grpc_slot = slot,
            sim_slot = rec.slot,
            hops = rec.hops,
            dex = rec.dex_label(),
            pools = %rec.pools_str(),
            our_verdict = if rec.tx_revert { "REVERT" } else { "OK" },
            real = if real_revert { "REVERT" } else { "OK" },
            r#match = if matched { "Y" } else { "N" },
            "reconcile-tx"
        );
        crate::errlog::log(
            "reconcile",
            &format!(
                "grpc_slot={slot} sim_slot={} sig={sig} hops={} dex={} {} \
                 our_verdict={} real={} match={}",
                rec.slot, rec.hops, rec.dex_label(), rec.pools_str(),
                if rec.tx_revert { "REVERT" } else { "OK" },
                if real_revert { "REVERT" } else { "OK" },
                if matched { "Y" } else { "N" },
            ),
        );
    }

    /// Snapshot the counters for the 30s report.
    pub fn snapshot(&self) -> (u64, u64, u64, u64, u64, usize) {
        (
            self.recorded.load(Ordering::Relaxed),
            self.checks.load(Ordering::Relaxed),
            self.matched.load(Ordering::Relaxed),
            self.mismatched.load(Ordering::Relaxed),
            self.unsimulated.load(Ordering::Relaxed),
            self.records.len(),
        )
    }
}
