//! Narrow live pool-state cache for the ShredStream arb strategy.
//!
//! A dedicated, lightweight Yellowstone gRPC subscription that watches ONLY the
//! handful of accounts we trade — every Meteora pool account plus every Pump.fun
//! vault pair — rather than an owner-wide firehose. Overhead is negligible and
//! it is fully independent of the Metis feed.
//!
//! * Meteora price/liquidity live inside the pool account (`sqrt_price` + `L`),
//!   so watching the pool account alone is enough.
//! * Pump.fun reserves are the SPL-token balances of the two vaults, so we watch
//!   both vault token accounts.

use anyhow::{Context, Result};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterAccounts, SubscribeRequestFilterTransactions, SubscribeRequestPing,
    SubscribeUpdateTransactionInfo,
};

use crate::sim_ledger::SimLedger;

use crate::meteora_math::{MeteoraPool, MAX_SQRT_PRICE, MIN_SQRT_PRICE};
use crate::pumpfun_math::PumpPool;

// Meteora DAMM v2 Pool account field offsets (bytes, discriminator included).
// Validated against a live pool whose account is 1112 bytes (INIT_SPACE 1104 +
// 8 discriminator), which matches this layout. The fee sub-struct size is the
// fragile part if the deployed layout ever drifts.
//
// `pool_fees` is the first field (offset 8); its first member is
// `base_fee.cliff_fee_numerator: u64`, i.e. the flat swap fee numerator
// (denominator 1e9).
const MET_OFF_CLIFF_FEE: usize = 8;
const MET_OFF_LIQUIDITY: usize = 360;
const MET_OFF_SQRT_MIN: usize = 424;
const MET_OFF_SQRT_MAX: usize = 440;
const MET_OFF_SQRT_PRICE: usize = 456;

// ── Full fee decode (base-fee scheduler + dynamic fee) ───────────────────────
// Layout per the deployed cp-amm program (MeteoraAg/damm-v2), cross-checked
// against the offsets above that are already validated on live accounts
// (pool_fees occupies bytes 8..168, token_a_mint at 168):
//
// BaseFeeStruct @8..48 (base_fee_info blob 8..40 + padding u64 @40):
//   time-scheduler modes (0 linear / 1 exponential): cliff_fee_numerator u64
//   @8, mode u8 @16, padding[5], number_of_period u16 @22, period_frequency
//   u64 @24, reduction_factor u64 @32. Modes 2 (rate limiter) and 3/4
//   (market-cap scheduler) lay the blob out DIFFERENTLY — those pools are
//   rejected as untradeable rather than mis-decoded.
// protocol_fee_percent u8 @48, padding u8 @49, referral_fee_percent u8 @50,
// padding[3] @51, compounding_fee_bps u16 @54.
// DynamicFeeStruct @56..152: initialized u8 @56, padding[7] @57,
//   max_volatility_accumulator u32 @64, variable_fee_control u32 @68,
//   bin_step u16 @72, filter_period u16 @74, decay_period u16 @76,
//   reduction_factor u16 @78, last_update_timestamp u64 @80,
//   bin_step_u128 u128 @88, sqrt_price_reference u128 @104,
//   volatility_accumulator u128 @120, volatility_reference u128 @136.
// init_sqrt_price u128 @152..168.
// Pool tail: activation_point u64 @472, activation_type u8 @480 (0=slot,
// 1=unix timestamp), pool_status u8 @481, collect_fee_mode u8 @484,
// fee_version u8 @486.
const MET_OFF_SCHED_MODE: usize = 16;
const MET_OFF_NUM_PERIOD: usize = 22;
const MET_OFF_PERIOD_FREQ: usize = 24;
const MET_OFF_REDUCTION: usize = 32;
const MET_OFF_DYN_INIT: usize = 56;
const MET_OFF_DYN_MAX_VOL_ACC: usize = 64;
const MET_OFF_DYN_VFC: usize = 68;
const MET_OFF_DYN_BIN_STEP: usize = 72;
const MET_OFF_DYN_VOL_ACC: usize = 120;
const MET_OFF_ACTIVATION_POINT: usize = 472;
const MET_OFF_ACTIVATION_TYPE: usize = 480;
const MET_OFF_POOL_STATUS: usize = 481;
const MET_OFF_COLLECT_FEE_MODE: usize = 484;
const MET_OFF_FEE_VERSION: usize = 486;

/// On-chain fee caps (numerator over 1e9): 50% for fee_version 0, 99% for 1.
const MAX_FEE_NUMERATOR_V0: u64 = 500_000_000;
const MAX_FEE_NUMERATOR_V1: u64 = 990_000_000;
const BASIS_POINT_MAX: u64 = 10_000;

// SPL token account: amount is a u64 LE at offset 64.
const SPL_AMOUNT_OFFSET: usize = 64;

#[derive(Clone)]
pub struct PoolStateCache {
    inner: Arc<DashMap<Pubkey, Vec<u8>>>,
    /// Last time each account's data changed (seeded at prefetch, refreshed on
    /// every stream update). Used by the rug monitor's idle-timeout check.
    last_update: Arc<DashMap<Pubkey, std::time::Instant>>,
    /// The SLOT at which each account last changed (from the stream update's
    /// slot). Used by the staleness diagnostic to show how many slots behind the
    /// cached state is versus the newest slot we've seen.
    last_update_slot: Arc<DashMap<Pubkey, u64>>,
    slot: Arc<AtomicU64>,
    /// Total account updates received (for diagnostics).
    updates: Arc<AtomicU64>,
    /// The full set of accounts we currently subscribe to. Adding to this set
    /// and pinging `change` re-sends the (overwriting) SubscribeRequest live.
    accounts: Arc<std::sync::Mutex<Vec<Pubkey>>>,
    change: Arc<tokio::sync::Notify>,
    /// LIVE overlay of pump pools, keyed by the pool's token vault: the pool
    /// state advanced by shred txs that haven't hit the gRPC stream yet, tagged
    /// with the `built_on` slot (the account's last-update slot at rebuild time).
    /// When gRPC catches up (last_update_slot > built_on) the overlay is ignored
    /// and rebuilt from the fresh cache — so we never drift from ground truth.
    /// Keyed by the pool's token vault → `(built_on_grpc_slot, shred_block_slot,
    /// inflight_count, PumpPool)`. In-flight swaps accumulate only WITHIN one
    /// block: a new block (shred_block_slot advances) discards the previous
    /// block's queued swaps and restarts from confirmed state — landed ones show
    /// up as a gRPC account update, unlanded ones are moot. The count = how many
    /// in-flight Pump swaps are folded into THIS block's overlay.
    live_pump: Arc<DashMap<Pubkey, (u64, u64, u32, PumpPool)>>,
    /// LIVE overlay of Meteora pools, keyed by pool account (only `sqrt_price`
    /// advances on a swap; liquidity/fees stay from the decoded cache).
    live_meteora: Arc<DashMap<Pubkey, (u64, MeteoraPool)>>,
    /// Phase-1 diagnostic: our per-tx simulation verdicts, reconciled here
    /// against the gRPC transaction-update stream (fed on the same subscription).
    sim_ledger: Arc<SimLedger>,
    /// The LAST transaction-update (signature + slot) seen for each account we
    /// subscribe to (keyed by the account, e.g. a Pump vault). Together with
    /// `last_update_slot` (the last ACCOUNT-update slot) this lets the sim log
    /// show, at the moment it computes a tx, exactly which confirmed tx and which
    /// confirmed account-state it is building on — so a gap is visible.
    last_tx_sig: Arc<DashMap<Pubkey, (solana_sdk::signature::Signature, u64)>>,
    /// The ACCOUNT-update correlation: for each account, the signature of the
    /// transaction that PRODUCED its current cached state, plus the slot and
    /// write_version (intra-slot order). Yellowstone stamps every account-update
    /// with `txn_signature`, so we know EXACTLY which tx each pool-state change
    /// belongs to — no guessing from amounts. This is the key to knowing whether
    /// the reserves we price a tx on are really that tx's immediate predecessor.
    last_acct_tx:
        Arc<DashMap<Pubkey, (Option<solana_sdk::signature::Signature>, u64, u64)>>,
}

fn read_u128_le(data: &[u8], off: usize) -> Option<u128> {
    data.get(off..off + 16)
        .map(|s| u128::from_le_bytes(s.try_into().unwrap()))
}

fn read_u64_le(data: &[u8], off: usize) -> Option<u64> {
    data.get(off..off + 8)
        .map(|s| u64::from_le_bytes(s.try_into().unwrap()))
}

fn read_u32_le(data: &[u8], off: usize) -> Option<u32> {
    data.get(off..off + 4)
        .map(|s| u32::from_le_bytes(s.try_into().unwrap()))
}

fn read_u16_le(data: &[u8], off: usize) -> Option<u16> {
    data.get(off..off + 2)
        .map(|s| u16::from_le_bytes(s.try_into().unwrap()))
}

/// Current TOTAL swap-fee numerator (denominator 1e9) of a DAMM v2 pool:
/// scheduler-adjusted base fee + volatility-based dynamic fee, capped at 50%,
/// exactly per the on-chain program:
///
/// * base fee scheduler (`fee_time_scheduler.rs`): after
///   `period = min(number_of_period, elapsed / period_frequency)` elapsed
///   periods since activation — Linear: `cliff - period * reduction_factor`;
///   Exponential: `cliff * (1 - reduction_factor/10000)^period`.
/// * dynamic fee (`get_variable_fee`):
///   `ceil((volatility_accumulator * bin_step)^2 * variable_fee_control
///    / 100_000_000_000)`.
///
/// Where the elapsed point can't be resolved (slot-based schedule with no slot
/// yet) we use period 0 — the HIGHEST base fee, so profit is never overstated.
/// The stored volatility_accumulator is likewise used without decay: right
/// after the bursts of activity we trade on it is accurate, and between bursts
/// it only overstates the fee (conservative direction).
/// `None` means the pool is NOT SAFELY TRADEABLE with our model (disabled,
/// compounding curve, unknown scheduler/fee version, or garbage read) — the
/// caller must SKIP the pool, never fall back to a cheaper config fee (that
/// exact fallback is what produced streams of fake-profit sends).
/// Breakdown of a Meteora pool's decoded fee, for the audit log so the operator
/// can hand-verify against a real on-chain swap.
#[derive(Clone, Copy, Debug)]
pub struct MetFeeBreakdown {
    pub cliff_bps: f64,
    pub base_bps: f64,
    pub dyn_stored_bps: f64,
    pub dyn_worst_bps: f64,
    pub total_stored_bps: f64,
    pub total_worst_bps: f64,
    pub dynamic_enabled: bool,
}

fn meteora_total_fee_numerator(data: &[u8], current_slot: u64, worst_case: bool) -> Option<u64> {
    // Pool must be enabled (pool_status 0) — a disabled pool reverts all swaps.
    if data.get(MET_OFF_POOL_STATUS).copied().unwrap_or(1) != 0 {
        return None;
    }
    // Fee cap depends on fee_version; unknown version → untradeable.
    let max_fee = match data.get(MET_OFF_FEE_VERSION).copied().unwrap_or(0) {
        0 => MAX_FEE_NUMERATOR_V0,
        1 => MAX_FEE_NUMERATOR_V1,
        _ => return None,
    };
    let cliff = read_u64_le(data, MET_OFF_CLIFF_FEE)?;
    if cliff == 0 || cliff > max_fee {
        return None; // garbage read or above the legal cap — do not trade
    }

    // ── Base fee via the time/slot scheduler ──
    // Only modes 0 (linear) and 1 (exponential) share this field layout; the
    // rate-limiter (2) and market-cap-scheduler (3/4) blobs are laid out
    // differently and would decode as garbage — reject those pools.
    let mode = data.get(MET_OFF_SCHED_MODE).copied().unwrap_or(0);
    if mode > 1 {
        return None;
    }
    let mut base = cliff;
    let period_freq = read_u64_le(data, MET_OFF_PERIOD_FREQ).unwrap_or(0);
    if period_freq > 0 {
        let num_period = read_u16_le(data, MET_OFF_NUM_PERIOD).unwrap_or(0) as u64;
        let reduction = read_u64_le(data, MET_OFF_REDUCTION).unwrap_or(0);
        let activation_point = read_u64_le(data, MET_OFF_ACTIVATION_POINT).unwrap_or(0);
        let activation_type = data.get(MET_OFF_ACTIVATION_TYPE).copied().unwrap_or(0);
        let current_point = if activation_type == 1 {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0)
        } else {
            current_slot
        };
        // Unresolvable point → period 0 → cliff (max) — never understate the fee.
        let period = if current_point == 0 {
            0
        } else {
            (current_point.saturating_sub(activation_point) / period_freq).min(num_period)
        };
        base = if mode == 1 {
            // Exponential: cliff * (1 - reduction/10000)^period, rounded UP so
            // f64 drift can never round below the on-chain fixed-point fee.
            let r = (reduction.min(BASIS_POINT_MAX) as f64) / BASIS_POINT_MAX as f64;
            ((cliff as f64) * (1.0 - r).powi(period.min(u16::MAX as u64) as i32)).ceil() as u64
        } else {
            // Linear.
            cliff.saturating_sub(reduction.saturating_mul(period))
        };
    }

    // ── Dynamic (volatility) fee ──
    // The on-chain program RECOMPUTES `volatility_accumulator` at swap time
    // (volatility_reference + price-move-in-bins, capped at
    // `max_volatility_accumulator`), so a big trade pays MORE than the stored
    // snapshot implies. Two modes:
    //   worst_case = true  → price at `max_volatility_accumulator` (hard ceiling
    //                        → the fee can never be understated → no fake profit)
    //   worst_case = false → price at the currently-stored `volatility_accumulator`
    let dynamic = meteora_dynamic_fee(data, worst_case, max_fee);
    Some(base.saturating_add(dynamic).min(max_fee))
}

/// Dynamic (volatility) fee numerator for a Meteora pool, from either the stored
/// or the worst-case (max) volatility accumulator.
fn meteora_dynamic_fee(data: &[u8], worst_case: bool, max_fee: u64) -> u64 {
    if data.get(MET_OFF_DYN_INIT).copied().unwrap_or(0) == 0 {
        return 0;
    }
    let vfc = read_u32_le(data, MET_OFF_DYN_VFC).unwrap_or(0) as u128;
    let bin_step = read_u16_le(data, MET_OFF_DYN_BIN_STEP).unwrap_or(0) as u128;
    let vol_acc = if worst_case {
        read_u32_le(data, MET_OFF_DYN_MAX_VOL_ACC).unwrap_or(0) as u128
    } else {
        read_u128_le(data, MET_OFF_DYN_VOL_ACC).unwrap_or(0)
    };
    if vfc == 0 || bin_step == 0 || vol_acc == 0 {
        return 0;
    }
    let vfa = vol_acc.saturating_mul(bin_step);
    let square = vfa.saturating_mul(vfa);
    let v_fee = square.saturating_mul(vfc);
    v_fee
        .saturating_add(99_999_999_999)
        .checked_div(100_000_000_000)
        .unwrap_or(0)
        .min(max_fee as u128) as u64
}

/// Compute the base (scheduler) fee numerator alone, for the audit breakdown.
fn meteora_base_fee_numerator(data: &[u8], current_slot: u64) -> Option<u64> {
    let max_fee = match data.get(MET_OFF_FEE_VERSION).copied().unwrap_or(0) {
        0 => MAX_FEE_NUMERATOR_V0,
        1 => MAX_FEE_NUMERATOR_V1,
        _ => return None,
    };
    let cliff = read_u64_le(data, MET_OFF_CLIFF_FEE)?;
    if cliff == 0 || cliff > max_fee {
        return None;
    }
    let mode = data.get(MET_OFF_SCHED_MODE).copied().unwrap_or(0);
    if mode > 1 {
        return None;
    }
    let mut base = cliff;
    let period_freq = read_u64_le(data, MET_OFF_PERIOD_FREQ).unwrap_or(0);
    if period_freq > 0 {
        let num_period = read_u16_le(data, MET_OFF_NUM_PERIOD).unwrap_or(0) as u64;
        let reduction = read_u64_le(data, MET_OFF_REDUCTION).unwrap_or(0);
        let activation_point = read_u64_le(data, MET_OFF_ACTIVATION_POINT).unwrap_or(0);
        let activation_type = data.get(MET_OFF_ACTIVATION_TYPE).copied().unwrap_or(0);
        let current_point = if activation_type == 1 {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0)
        } else {
            current_slot
        };
        let period = if current_point == 0 {
            0
        } else {
            (current_point.saturating_sub(activation_point) / period_freq).min(num_period)
        };
        base = if mode == 1 {
            let r = (reduction.min(BASIS_POINT_MAX) as f64) / BASIS_POINT_MAX as f64;
            ((cliff as f64) * (1.0 - r).powi(period.min(u16::MAX as u64) as i32)).ceil() as u64
        } else {
            cliff.saturating_sub(reduction.saturating_mul(period))
        };
    }
    Some(base)
}

impl PoolStateCache {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::with_capacity(256)),
            last_update: Arc::new(DashMap::with_capacity(256)),
            last_update_slot: Arc::new(DashMap::with_capacity(256)),
            slot: Arc::new(AtomicU64::new(0)),
            updates: Arc::new(AtomicU64::new(0)),
            accounts: Arc::new(std::sync::Mutex::new(Vec::new())),
            change: Arc::new(tokio::sync::Notify::new()),
            last_tx_sig: Arc::new(DashMap::with_capacity(256)),
            last_acct_tx: Arc::new(DashMap::with_capacity(256)),
            live_pump: Arc::new(DashMap::with_capacity(256)),
            live_meteora: Arc::new(DashMap::with_capacity(256)),
            sim_ledger: Arc::new(SimLedger::default()),
        }
    }

    /// Shared handle to the Phase-1 simulation ledger — the engine records its
    /// per-tx verdicts here; the gRPC transaction-update stream reconciles them.
    pub fn sim_ledger(&self) -> Arc<SimLedger> {
        self.sim_ledger.clone()
    }

    /// Add accounts to the live subscription at runtime. Extends the tracked
    /// set (dedup) and signals the stream task to re-send the full filter — no
    /// reconnect. Also seeds their state once via RPC so they're usable
    /// immediately (Yellowstone only pushes on change).
    pub fn add_accounts(&self, rpc: &RpcClient, new: &[Pubkey]) {
        {
            let mut set = self.accounts.lock().unwrap();
            let before = set.len();
            set.extend_from_slice(new);
            set.sort_unstable();
            set.dedup();
            if set.len() == before {
                return; // nothing new
            }
        }
        self.prefetch(rpc, new);
        self.change.notify_one();
        info!(added = new.len(), "pool-state accounts added to live subscription");
    }

    pub fn slot(&self) -> u64 {
        self.slot.load(Ordering::Relaxed)
    }

    /// Total account updates received since start.
    pub fn updates(&self) -> u64 {
        self.updates.load(Ordering::Relaxed)
    }

    /// Number of distinct accounts currently cached.
    pub fn cache_size(&self) -> usize {
        self.inner.len()
    }

    pub fn has(&self, pk: &Pubkey) -> bool {
        self.inner.contains_key(pk)
    }

    /// RPC-fetch initial state for every account so we have a baseline even for
    /// low-activity pools (Yellowstone only pushes on CHANGE — a rarely-traded
    /// Meteora pool would otherwise never appear). Live updates then keep it
    /// fresh. Runs synchronously at startup.
    pub fn prefetch(&self, rpc: &RpcClient, accounts: &[Pubkey]) {
        let mut ok = 0usize;
        let now = std::time::Instant::now();
        let seed_slot = rpc.get_slot().unwrap_or_else(|_| self.slot.load(Ordering::Relaxed));
        for pk in accounts {
            match rpc.get_account(pk) {
                Ok(acct) => {
                    self.inner.insert(*pk, acct.data);
                    self.last_update.insert(*pk, now);
                    self.last_update_slot.insert(*pk, seed_slot);
                    ok += 1;
                }
                Err(e) => warn!(account = %pk, error = %e, "pool-state prefetch miss"),
            }
        }
        // Seed the slot so slot-based fee schedulers resolve before the first
        // stream update arrives (period 0 = max base fee would over-block).
        if self.slot.load(Ordering::Relaxed) == 0 {
            if let Ok(s) = rpc.get_slot() {
                self.slot.store(s, Ordering::Relaxed);
            }
        }
        info!(requested = accounts.len(), fetched = ok, "pool-state prefetch complete");
    }

    /// How long since `account` last changed (seeded at prefetch). `None` if we
    /// have never seen it. Used to detect abandoned/rugged pools by idleness.
    pub fn last_update_age(&self, account: &Pubkey) -> Option<std::time::Duration> {
        self.last_update.get(account).map(|t| t.elapsed())
    }

    /// The slot at which `account` last changed. `None` if never seen. Used by
    /// the staleness diagnostic (current slot − this = how many slots behind).
    pub fn last_update_slot(&self, account: &Pubkey) -> Option<u64> {
        self.last_update_slot.get(account).map(|v| *v.value())
    }

    /// The last transaction-update (signature + slot) seen touching `account`.
    pub fn last_tx_sig(
        &self,
        account: &Pubkey,
    ) -> Option<(solana_sdk::signature::Signature, u64)> {
        self.last_tx_sig.get(account).map(|v| *v.value())
    }

    /// The transaction (signature + slot + write_version) that produced the
    /// current cached state of `account`, taken from the account-update's
    /// `txn_signature`. This is the exact "which tx does this pool state belong
    /// to" correlation.
    pub fn acct_state_tx(
        &self,
        account: &Pubkey,
    ) -> Option<(Option<solana_sdk::signature::Signature>, u64, u64)> {
        self.last_acct_tx.get(account).map(|v| *v.value())
    }

    /// Decode a Meteora pool account into its pricing slice. The fee is read
    /// straight from the pool's `cliff_fee_numerator`; `fallback_fee_numerator`
    /// (from config) is used only if that read looks implausible.
    ///
    /// Returns `None` if the decoded state is not tradeable — a drained pool
    /// (`liquidity == 0`), a sqrt-price outside the on-chain valid range, or a
    /// mangled/partial read — so the engine never sizes a trade off garbage.
    pub fn meteora_pool(
        &self,
        pool: &Pubkey,
        fallback_fee_numerator: u64,
        worst_case_fee: bool,
    ) -> Option<MeteoraPool> {
        let p = self.meteora_pool_decoded(pool, fallback_fee_numerator, worst_case_fee)?;
        // LIVE overlay: if a shred-advanced sqrt_price exists that is still based
        // on the CURRENT cache slot, use it (keeps us in sync with in-flight txs
        // the gRPC stream hasn't delivered yet). Only sqrt_price is overridden;
        // liquidity/fees come from the fresh decode. Re-validate the range.
        if let Some(e) = self.live_meteora.get(pool) {
            if e.value().0 == self.last_update_slot(pool).unwrap_or(0) {
                let sp = e.value().1.sqrt_price;
                if sp >= p.sqrt_min_price && sp <= p.sqrt_max_price {
                    return Some(MeteoraPool { sqrt_price: sp, ..p });
                }
            }
        }
        Some(p)
    }

    /// Raw decode of a Meteora pool WITHOUT the live shred overlay — the
    /// gRPC-confirmed ground truth. Used as the base for the in-flight overlay
    /// so we never advance a price that is itself already advanced (which would
    /// stack in-flight swaps). Same validity gating as `meteora_pool`.
    fn meteora_pool_decoded(
        &self,
        pool: &Pubkey,
        _fallback_fee_numerator: u64,
        worst_case_fee: bool,
    ) -> Option<MeteoraPool> {
        let entry = self.inner.get(pool)?;
        let data = entry.value();
        // Current TOTAL fee: scheduler-adjusted base fee + volatility-based
        // dynamic fee (can push a "0.25%" pool well past 2%). FAIL CLOSED: if
        // the fee cannot be decoded safely (disabled pool, compounding curve,
        // unknown scheduler/fee version), the pool is untradeable — never fall
        // back to a cheaper config fee (that fallback fabricated profit).
        let fee_numerator =
            meteora_total_fee_numerator(data, self.slot.load(Ordering::Relaxed), worst_case_fee)?;
        // Compounding pools (collect_fee_mode 2) use plain x*y=k on tracked
        // reserves, NOT the sqrt-price curve below — reject them.
        let collect_fee_mode = data.get(MET_OFF_COLLECT_FEE_MODE).copied().unwrap_or(0);
        if collect_fee_mode > 1 {
            return None;
        }
        let p = MeteoraPool {
            liquidity: read_u128_le(data, MET_OFF_LIQUIDITY)?,
            sqrt_min_price: read_u128_le(data, MET_OFF_SQRT_MIN)?,
            sqrt_max_price: read_u128_le(data, MET_OFF_SQRT_MAX)?,
            sqrt_price: read_u128_le(data, MET_OFF_SQRT_PRICE)?,
            fee_numerator,
            collect_fee_mode,
        };
        // Validity gate: reject drained / out-of-range / garbage state.
        if p.liquidity == 0
            || p.sqrt_price < MIN_SQRT_PRICE
            || p.sqrt_price > MAX_SQRT_PRICE
            || p.sqrt_min_price < MIN_SQRT_PRICE
            || p.sqrt_max_price > MAX_SQRT_PRICE
            || p.sqrt_min_price >= p.sqrt_max_price
            || p.sqrt_price < p.sqrt_min_price
            || p.sqrt_price > p.sqrt_max_price
        {
            return None;
        }
        Some(p)
    }

    /// Fee breakdown for a Meteora pool (bps), for the operator audit log.
    /// Returns None if the pool isn't cached / not decodable.
    pub fn meteora_fee_breakdown(&self, pool: &Pubkey) -> Option<MetFeeBreakdown> {
        let entry = self.inner.get(pool)?;
        let data = entry.value();
        let slot = self.slot.load(Ordering::Relaxed);
        let max_fee = match data.get(MET_OFF_FEE_VERSION).copied().unwrap_or(0) {
            0 => MAX_FEE_NUMERATOR_V0,
            1 => MAX_FEE_NUMERATOR_V1,
            _ => return None,
        };
        // numerator (1e9 denom) → bps (1e4 denom): / 1e5.
        let to_bps = |n: u64| n as f64 / 100_000.0;
        let cliff = read_u64_le(data, MET_OFF_CLIFF_FEE).unwrap_or(0);
        let base = meteora_base_fee_numerator(data, slot).unwrap_or(cliff);
        let dyn_stored = meteora_dynamic_fee(data, false, max_fee);
        let dyn_worst = meteora_dynamic_fee(data, true, max_fee);
        Some(MetFeeBreakdown {
            cliff_bps: to_bps(cliff),
            base_bps: to_bps(base),
            dyn_stored_bps: to_bps(dyn_stored),
            dyn_worst_bps: to_bps(dyn_worst),
            total_stored_bps: to_bps((base + dyn_stored).min(max_fee)),
            total_worst_bps: to_bps((base + dyn_worst).min(max_fee)),
            dynamic_enabled: data.get(MET_OFF_DYN_INIT).copied().unwrap_or(0) != 0,
        })
    }

    /// Raw `liquidity` field of a Meteora pool (no validity gating). `Some(0)`
    /// means the pool has been fully drained (rug) — used by the rug monitor,
    /// which must distinguish "drained" from "not yet cached".
    pub fn meteora_raw_liquidity(&self, pool: &Pubkey) -> Option<u128> {
        let entry = self.inner.get(pool)?;
        read_u128_le(entry.value(), MET_OFF_LIQUIDITY)
    }

    /// SPL token amount of a vault account.
    pub fn spl_amount(&self, vault: &Pubkey) -> Option<u64> {
        let entry = self.inner.get(vault)?;
        read_u64_le(entry.value(), SPL_AMOUNT_OFFSET)
    }

    /// Build a Pump.fun pool from its two vaults, normalized to base = token,
    /// quote = WSOL. `token_mint` is used to read the token's REAL supply for
    /// the market-cap fee tier; if the mint account isn't cached yet, supply is
    /// passed as 0 and `PumpPool::new` fails closed to the highest fee tier.
    pub fn pump_pool(
        &self,
        pool: &Pubkey,
        token_vault: &Pubkey,
        wsol_vault: &Pubkey,
        token_mint: &Pubkey,
    ) -> Option<PumpPool> {
        let base = self.spl_amount(token_vault)?;
        let quote = self.spl_amount(wsol_vault)?;
        let supply = self.spl_mint_supply(token_mint).unwrap_or(0) as u128;
        // `is_pump_pool` (canonical, market-cap-tiered fee) iff the pool's
        // `coin_creator` is SET (non-default). Non-canonical pools — every
        // inverted pool among them — charge the flat fee. If the pool account
        // isn't cached yet, assume canonical (the conservative higher-fee path).
        let is_canonical = self
            .pump_coin_creator(pool)
            .map(|c| c != Pubkey::default())
            .unwrap_or(true);
        Some(PumpPool::new(base, quote, supply, is_canonical))
    }

    /// The Pump.fun AMM pool's `coin_creator` (pubkey @ offset 211 of the Pool
    /// account, discriminator included). This drives the `creator_vault` PDA and
    /// its ATA. Pump can SET/rotate `coin_creator` after pool creation (default
    /// zero → the real creator, populated by pump's backend on first trades), so
    /// it must be read LIVE — a snapshot taken at discovery can go stale and the
    /// derived vault accounts then mismatch, reverting the swap. `None` if the
    /// pool account isn't cached (it must be subscribed for this to work).
    pub fn pump_coin_creator(&self, pool: &Pubkey) -> Option<Pubkey> {
        let entry = self.inner.get(pool)?;
        let d = entry.value();
        d.get(211..211 + 32)
            .map(|s| Pubkey::new_from_array(s.try_into().unwrap()))
    }

    // ── Live state: advance pool state by in-flight shred txs ─────────────────
    // The gRPC account stream lags the network by a few slots, and a thin Meteora
    // pool may not update for several blocks. So we advance a LIVE overlay from
    // the swaps we see in shreds. Each overlay entry is tagged with the slot the
    // underlying cache was at when we (re)built it; once gRPC catches up past that
    // slot the overlay is dropped and rebuilt from ground truth — no drift.

    /// Apply an observed Pump swap (from a shred) to the live pump overlay.
    /// `shred_slot` is the shred's slot; if it's not newer than the cached state
    /// the swap is already reflected on-chain and only the base is (re)seeded.
    ///
    /// `kind`, `base_amount` and `quote_amount` are the RAW instruction values
    /// (base/quote in PROGRAM terms; fees are levied on the QUOTE side). On a
    /// normal pool base = token and the stored orientation matches; on an
    /// INVERTED pool (base = WSOL) the same raw math is applied on the flipped
    /// view and flipped back — the base amount is then WSOL lamports and the
    /// fee lands on the token side, exactly like on-chain.
    #[allow(clippy::too_many_arguments)]
    pub fn apply_pump_swap(
        &self,
        pool: &Pubkey,
        token_vault: &Pubkey,
        wsol_vault: &Pubkey,
        token_mint: &Pubkey,
        token_is_base: bool,
        kind: crate::shred_stream::PumpIxKind,
        base_amount: u64,
        quote_amount: u64,
        shred_slot: u64,
    ) {
        use crate::shred_stream::PumpIxKind as K;
        let cur = self.last_update_slot(token_vault).unwrap_or(0);
        // CHECKPOINT MODEL (validator-style): the base is the gRPC-confirmed
        // vault state at slot `cur`, and we ACCUMULATE every in-flight swap with
        // slot > cur on top of it — ACROSS BLOCKS — until gRPC advances (which
        // moves `cur` and rebuilds from the fresh checkpoint). The overlay is
        // valid as long as it was built on the CURRENT confirmed slot; a new
        // block does NOT reset it (the old per-block reset discarded prior
        // blocks' swaps whenever gRPC lagged even one slot, which drifted the
        // reserves by several % on active pools). Only successful swaps reach
        // here — the caller gates on the sim verdict.
        let (base, count) = match self.live_pump.get(token_vault) {
            Some(e) if e.value().0 == cur => (e.value().3, e.value().2),
            _ => match self.pump_pool(pool, token_vault, wsol_vault, token_mint) {
                Some(p) => (p, 0),
                None => return,
            },
        };
        // Advance unless gRPC has moved STRICTLY PAST this shred's slot (then the
        // tx is already reflected in the confirmed snapshot). Equal slot is NOT
        // "already applied": a shred tx executes DURING its slot and is seen
        // before the confirmed account update for that slot lands, so freezing
        // the overlay for the whole current slot under-applies every in-flight
        // swap in it — the exact drift that made successive same-slot legs price
        // on a stale checkpoint.
        let _ = K::Opaque; // (Opaque never reaches here; handled via invalidate)
        let advanced = if shred_slot < cur {
            base // already on-chain / in the confirmed cache
        } else if kind == K::Opaque {
            return; // handled via invalidate_pump, never here
        } else {
            base.after_observed(token_is_base, kind, base_amount, quote_amount)
        };
        self.live_pump
            .insert(*token_vault, (cur, shred_slot, count.saturating_add(1), advanced));
    }

    /// Pump "behind": how many in-flight Pump swaps are accumulated in the
    /// current block's overlay for this vault. 0 when the overlay is stale (the
    /// confirmed gRPC state already caught up) or absent.
    pub fn pump_behind(&self, token_vault: &Pubkey) -> u32 {
        let cur = self.last_update_slot(token_vault).unwrap_or(0);
        match self.live_pump.get(token_vault) {
            Some(e) if e.value().0 == cur => e.value().2,
            _ => 0,
        }
    }

    /// Drop the live overlay for a Pump pool — used when a shred tx touched the
    /// pool through an instruction we can NOT decode (router / private bot CPI):
    /// the pool is about to change by an unknown amount, so the overlay is no
    /// longer trustworthy. Pricing falls back to the gRPC cache and the engine
    /// gates trading until a fresh account update arrives.
    pub fn invalidate_pump(&self, token_vault: &Pubkey) {
        self.live_pump.remove(token_vault);
    }

    /// Pump pool including any live (shred-advanced) state, valid as long as the
    /// overlay was built on the CURRENT confirmed gRPC slot (checkpoint model —
    /// it accumulates every in-flight swap since that checkpoint, across blocks).
    /// Once gRPC advances, the overlay is stale and we fall back to the fresh
    /// confirmed decode. `_shred_slot` is retained for call-site symmetry.
    pub fn pump_pool_live(
        &self,
        pool: &Pubkey,
        token_vault: &Pubkey,
        wsol_vault: &Pubkey,
        token_mint: &Pubkey,
        _shred_slot: u64,
    ) -> Option<PumpPool> {
        let cur = self.last_update_slot(token_vault).unwrap_or(0);
        if let Some(e) = self.live_pump.get(token_vault) {
            if e.value().0 == cur {
                return Some(e.value().3);
            }
        }
        self.pump_pool(pool, token_vault, wsol_vault, token_mint)
    }

    /// Apply an observed Meteora swap (from a shred) to the live meteora overlay.
    /// `a_to_b` is the swap direction; the caller derives it from the arb's Pump
    /// leg and the pool's token side.
    pub fn apply_meteora_swap(
        &self,
        pool: &Pubkey,
        fallback_fee_numerator: u64,
        worst_case_fee: bool,
        amount_in: u64,
        a_to_b: bool,
        shred_slot: u64,
    ) {
        let cur = self.last_update_slot(pool).unwrap_or(0);
        // Base is ALWAYS the gRPC-confirmed decode (never the overlay): the only
        // Meteora activity visible in shreds rides on competitor ARB txs, which
        // overwhelmingly revert. Stacking every observed in-flight swap over-
        // moves sqrt_price — if more than one had actually executed, the account
        // would have updated (cur would advance) and the overlay rebuilt. At
        // most one swap is genuinely pending, so apply exactly one on confirmed.
        let base = match self.meteora_pool_decoded(pool, fallback_fee_numerator, worst_case_fee) {
            Some(p) => p,
            None => return,
        };
        let advanced = if shred_slot <= cur {
            base
        } else {
            base.apply_observed_swap(amount_in, a_to_b)
        };
        self.live_meteora.insert(*pool, (cur, advanced));
    }

    /// SPL mint total supply (u64 @ offset 36 of a Mint account). `None` if the
    /// mint account isn't cached.
    pub fn spl_mint_supply(&self, mint: &Pubkey) -> Option<u64> {
        let entry = self.inner.get(mint)?;
        read_u64_le(entry.value(), 36)
    }

    /// Token-2022 transfer-fee basis points for a token mint, or 0 if none.
    ///
    /// WHY THIS MATTERS: cp-amm (and every SPL-token program) charges the mint's
    /// transfer fee on EVERY transfer of the token. The on-chain DAMM v2 swap
    /// applies `calculate_transfer_fee_excluded_amount` to BOTH the input and the
    /// output (`process_swap_exact_in`), so the pool receives less than we send
    /// and we receive less than the curve output. Our pool math models NEITHER,
    /// so on a fee-bearing token our predicted output is systematically HIGHER
    /// than reality — an over-prediction that grows with size and causes 0x1771
    /// slippage reverts even on "profitable" trades.
    ///
    /// A classic (Tokenkeg) SPL mint is EXACTLY 82 bytes and can never carry this
    /// extension, so any transfer fee lives only in a Token-2022 mint whose data
    /// is longer and holds a TLV extension list after the 82-byte base + 1-byte
    /// account-type tag. We scan that TLV for `TransferFeeConfig` (type 1) and
    /// return the fee ACTUALLY IN EFFECT this epoch — exactly what the on-chain
    /// program charges. A `TransferFeeConfig` stores TWO snapshots
    /// (`older_transfer_fee`, `newer_transfer_fee`); SPL `get_epoch_fee` uses
    /// `newer` once `current_epoch >= newer.epoch`, else `older`. Taking the
    /// larger of the two (the old behaviour) invents a fee no real transfer pays
    /// when a mint LOWERS its fee, mis-pricing every leg. Epoch is derived from
    /// the live slot (mainnet: 432_000 slots/epoch); before the slot is seeded
    /// we fall back to the conservative max().
    pub fn mint_transfer_fee_bps(&self, mint: &Pubkey) -> u16 {
        const SLOTS_PER_EPOCH: u64 = 432_000;
        let current_epoch = self.slot.load(Ordering::Relaxed) / SLOTS_PER_EPOCH;
        let entry = match self.inner.get(mint) {
            Some(e) => e,
            None => return 0,
        };
        let d = entry.value();
        // Base Mint is 82 bytes; extensions require [82]=account_type then TLV.
        if d.len() <= 83 {
            return 0;
        }
        let mut off = 83usize; // first TLV entry (after base 82 + account_type 1)
        while off + 4 <= d.len() {
            let ext_type = u16::from_le_bytes([d[off], d[off + 1]]);
            let ext_len = u16::from_le_bytes([d[off + 2], d[off + 3]]) as usize;
            let data_start = off + 4;
            let data_end = data_start + ext_len;
            if data_end > d.len() {
                break;
            }
            // TransferFeeConfig extension = type 1. Its data layout:
            //   authority(32) + withdraw_authority(32) + withheld_amount(8)
            //   + older_transfer_fee(18) + newer_transfer_fee(18)
            // where TransferFee = epoch(8)+maximum_fee(8)+basis_points(u16,2),
            // so basis_points sits at data offset 88 (older) and 106 (newer).
            if ext_type == 1 && ext_len >= 108 {
                // TransferFee = epoch(u64)@0 + maximum_fee(u64)@8 + bps(u16)@16.
                // From data_start: older.bps@88; newer.epoch@90, newer.bps@106.
                let older_bps = u16::from_le_bytes([d[data_start + 88], d[data_start + 89]]);
                let newer_epoch = u64::from_le_bytes(
                    d[data_start + 90..data_start + 98].try_into().unwrap(),
                );
                let newer_bps = u16::from_le_bytes([d[data_start + 106], d[data_start + 107]]);
                if current_epoch == 0 {
                    // Slot not seeded yet — stay conservative (never understate).
                    return older_bps.max(newer_bps);
                }
                return if current_epoch >= newer_epoch {
                    newer_bps
                } else {
                    older_bps
                };
            }
            if ext_len == 0 {
                break; // malformed / end-of-list guard
            }
            off = data_end;
        }
        0
    }

    /// Spawn the subscription task with reconnect/backoff. `accounts` seeds the
    /// initial set; more can be added later via [`add_accounts`].
    pub fn spawn_subscription(
        &self,
        endpoint: String,
        x_token: String,
        accounts: Vec<Pubkey>,
    ) {
        {
            let mut set = self.accounts.lock().unwrap();
            *set = accounts;
            set.sort_unstable();
            set.dedup();
        }
        let inner = self.inner.clone();
        let last_update = self.last_update.clone();
        let last_update_slot = self.last_update_slot.clone();
        let slot = self.slot.clone();
        let updates = self.updates.clone();
        let acct_set = self.accounts.clone();
        let change = self.change.clone();
        let sim_ledger = self.sim_ledger.clone();
        let last_tx_sig = self.last_tx_sig.clone();
        let last_acct_tx = self.last_acct_tx.clone();
        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                match run_stream(
                    &endpoint, &x_token, &acct_set, &change, &inner, &last_update,
                    &last_update_slot, &slot, &updates, &sim_ledger, &last_tx_sig,
                    &last_acct_tx,
                )
                .await
                {
                    Ok(()) => warn!("pool-state stream ended cleanly, reconnecting"),
                    Err(e) => warn!(error = %e, "pool-state stream error, reconnecting"),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
        });
    }
}

/// Map each token account touched by a transaction to its RAW post-swap balance,
/// read from the transaction-update's `post_token_balances`. `account_index` in
/// each balance indexes the tx's FULL account list — static message keys first,
/// then loaded writable, then loaded readonly addresses (the canonical Solana
/// order) — so we reconstruct that list and resolve each index to a pubkey. This
/// is the network's ground-truth pool state after the tx, used by the reconcile
/// to check our predicted reserves to the lamport.
fn post_token_balances(info: &SubscribeUpdateTransactionInfo) -> HashMap<Pubkey, u64> {
    let mut out = HashMap::new();
    let Some(txn) = info.transaction.as_ref() else {
        return out;
    };
    let Some(meta) = info.meta.as_ref() else {
        return out;
    };
    let mut keys: Vec<Pubkey> = Vec::new();
    if let Some(msg) = txn.message.as_ref() {
        for k in &msg.account_keys {
            keys.push(Pubkey::try_from(k.as_slice()).unwrap_or_default());
        }
    }
    for k in &meta.loaded_writable_addresses {
        keys.push(Pubkey::try_from(k.as_slice()).unwrap_or_default());
    }
    for k in &meta.loaded_readonly_addresses {
        keys.push(Pubkey::try_from(k.as_slice()).unwrap_or_default());
    }
    for tb in &meta.post_token_balances {
        let idx = tb.account_index as usize;
        let Some(pk) = keys.get(idx) else { continue };
        if let Some(amt) = tb.ui_token_amount.as_ref() {
            if let Ok(v) = amt.amount.parse::<u64>() {
                out.insert(*pk, v);
            }
        }
    }
    out
}

fn build_request(accounts: &[Pubkey]) -> SubscribeRequest {
    let acct_strs: Vec<String> = accounts.iter().map(|p| p.to_string()).collect();
    let mut accounts_filter: HashMap<String, SubscribeRequestFilterAccounts> = HashMap::new();
    accounts_filter.insert(
        "arb_pools".to_string(),
        SubscribeRequestFilterAccounts {
            account: acct_strs.clone(),
            owner: vec![],
            filters: vec![],
            nonempty_txn_signature: None,
        },
    );
    // Transaction filter on the SAME accounts: this is the ground truth for the
    // Phase-1 reconcile — every transaction that touches one of our pool/vault
    // accounts, WITH its real `err` (revert or not). Both `failed=None` and
    // `vote=Some(false)` so we get succeeded AND reverted non-vote txs.
    let mut tx_filter: HashMap<String, SubscribeRequestFilterTransactions> = HashMap::new();
    tx_filter.insert(
        "arb_pool_txs".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: None,
            signature: None,
            account_include: acct_strs,
            account_exclude: vec![],
            account_required: vec![],
        },
    );
    SubscribeRequest {
        accounts: accounts_filter,
        transactions: tx_filter,
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    }
}

async fn run_stream(
    endpoint: &str,
    x_token: &str,
    acct_set: &Arc<std::sync::Mutex<Vec<Pubkey>>>,
    change: &Arc<tokio::sync::Notify>,
    cache: &Arc<DashMap<Pubkey, Vec<u8>>>,
    last_update: &Arc<DashMap<Pubkey, std::time::Instant>>,
    last_update_slot: &Arc<DashMap<Pubkey, u64>>,
    slot: &Arc<AtomicU64>,
    updates: &Arc<AtomicU64>,
    sim_ledger: &Arc<SimLedger>,
    last_tx_sig: &Arc<DashMap<Pubkey, (solana_sdk::signature::Signature, u64)>>,
    last_acct_tx: &Arc<
        DashMap<Pubkey, (Option<solana_sdk::signature::Signature>, u64, u64)>,
    >,
) -> Result<()> {
    let mut client = GeyserGrpcClient::build_from_shared(endpoint.to_string())?
        .x_token(Some(x_token.to_string()))?
        .tls_config(yellowstone_grpc_client::ClientTlsConfig::new().with_native_roots())?
        .max_decoding_message_size(64 * 1024 * 1024)
        .connect()
        .await
        .context("pool-state gRPC connect failed")?;

    let request = build_request(&acct_set.lock().unwrap().clone());

    let (mut tx, mut stream) = client
        .subscribe_with_request(Some(request))
        .await
        .context("pool-state gRPC subscribe failed")?;

    info!(
        accounts = acct_set.lock().unwrap().len(),
        "pool-state subscription active"
    );

    loop {
        let msg = tokio::select! {
            biased;
            _ = change.notified() => {
                // Account set changed — re-send the full (overwriting) filter.
                let req = build_request(&acct_set.lock().unwrap().clone());
                tx.send(req).await.context("pool-state re-subscribe send failed")?;
                info!(accounts = acct_set.lock().unwrap().len(), "pool-state re-subscribed");
                continue;
            }
            m = stream.next() => match m {
                Some(m) => m.context("pool-state stream yielded error")?,
                None => return Ok(()),
            },
        };
        match msg.update_oneof {
            Some(UpdateOneof::Account(a)) => {
                slot.store(a.slot, Ordering::Relaxed);
                if let Some(info) = a.account {
                    if let Ok(pk) = Pubkey::try_from(info.pubkey.as_slice()) {
                        // Correlate this state change to the tx that caused it:
                        // Yellowstone stamps every account-update with the
                        // producing tx's signature. Store it (+ slot +
                        // write_version) so we know exactly which tx this pool
                        // state belongs to.
                        let acct_sig = info.txn_signature.as_ref().and_then(|s| {
                            solana_sdk::signature::Signature::try_from(s.as_slice()).ok()
                        });
                        last_acct_tx.insert(pk, (acct_sig, a.slot, info.write_version));
                        cache.insert(pk, info.data);
                        last_update.insert(pk, std::time::Instant::now());
                        last_update_slot.insert(pk, a.slot);
                        updates.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Some(UpdateOneof::Transaction(t)) => {
                // Ground truth for the Phase-1 reconcile: this tx touched one of
                // our pools; `meta.err` says whether it reverted. Match it to our
                // earlier simulation verdict (keyed by signature).
                if let Some(info) = t.transaction {
                    if !info.is_vote {
                        if let Ok(sig) =
                            solana_sdk::signature::Signature::try_from(info.signature.as_slice())
                        {
                            let real_revert =
                                info.meta.as_ref().map(|m| m.err.is_some()).unwrap_or(false);
                            // Real post-swap reserves come from post_token_balances:
                            // map each token account (by its index into the tx's
                            // full account list) to its raw post balance, so the
                            // reconcile can compare OUR predicted reserves to the
                            // network's — catching a wrong amount, not just a wrong
                            // revert verdict.
                            let balances = post_token_balances(&info);
                            // Remember this as the last tx-update for every
                            // account it touched (e.g. our Pump vaults), so the
                            // sim log can show which confirmed tx it built on.
                            for acct in balances.keys() {
                                last_tx_sig.insert(*acct, (sig, t.slot));
                            }
                            sim_ledger.reconcile(&sig, t.slot, real_revert, &balances);
                        }
                    }
                }
            }
            Some(UpdateOneof::Ping(_)) => {
                let _ = tx
                    .send(SubscribeRequest {
                        ping: Some(SubscribeRequestPing { id: 1 }),
                        ..Default::default()
                    })
                    .await;
            }
            _ => {}
        }
    }
}
