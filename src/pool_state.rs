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
    SubscribeRequestFilterAccounts, SubscribeRequestPing,
};

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
    /// PumpPool)`. In-flight swaps accumulate only WITHIN one block: a new block
    /// (shred_block_slot advances) discards the previous block's queued swaps and
    /// restarts from confirmed state — landed ones show up as a gRPC account
    /// update, unlanded ones are moot.
    live_pump: Arc<DashMap<Pubkey, (u64, u64, PumpPool)>>,
    /// LIVE overlay of Meteora pools, keyed by pool account (only `sqrt_price`
    /// advances on a swap; liquidity/fees stay from the decoded cache).
    live_meteora: Arc<DashMap<Pubkey, (u64, MeteoraPool)>>,
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
            live_pump: Arc::new(DashMap::with_capacity(256)),
            live_meteora: Arc::new(DashMap::with_capacity(256)),
        }
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
    /// gRPC-confirmed ground truth. Used as the base for the in-flight overlay so
    /// we never advance a price that is itself already advanced (which would
    /// accumulate in-flight swaps). Same validity gating as `meteora_pool`.
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

    /// Build a Pump.fun pool from its two vaults (base = token, quote = WSOL).
    /// `token_mint` is used to read the token's REAL supply for the market-cap
    /// fee tier; if the mint account isn't cached yet, supply is passed as 0 and
    /// `PumpPool::new` fails closed to the highest fee tier.
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
        // `coin_creator` is SET (non-default). Non-canonical pools charge the flat
        // fee. If the pool account isn't cached yet, assume canonical — the
        // conservative (higher-fee) path, so we never understate the fee.
        let is_canonical = self
            .pump_coin_creator(pool)
            .map(|c| c != Pubkey::default())
            .unwrap_or(true);
        Some(PumpPool::new(base, quote, supply, is_canonical))
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
    pub fn apply_pump_swap(
        &self,
        pool: &Pubkey,
        token_vault: &Pubkey,
        wsol_vault: &Pubkey,
        token_mint: &Pubkey,
        is_buy: bool,
        base_amount: u64,
        shred_slot: u64,
    ) {
        let cur = self.last_update_slot(token_vault).unwrap_or(0);
        // ACCUMULATE in-flight Pump swaps, but only WITHIN THE SAME BLOCK: base is
        // the existing overlay iff it was built on the same confirmed gRPC slot
        // AND the same shred block slot, else the fresh confirmed decode.
        // Successive holder/sniper buys+sells inside one block are summed so the
        // NEXT shred prices against a pool reflecting them; a new block discards
        // the previous block's queue (start fresh). NOTE: the caller only feeds
        // SIMPLE (non-arb) Pump swaps here — multi-hop arb legs are ignored for
        // state prediction (they mostly revert on the Meteora side).
        let base = match self.live_pump.get(token_vault) {
            Some(e) if e.value().0 == cur && e.value().1 == shred_slot => e.value().2,
            _ => match self.pump_pool(pool, token_vault, wsol_vault, token_mint) {
                Some(p) => p,
                None => return,
            },
        };
        let advanced = if shred_slot <= cur {
            base // already on-chain / in the cache
        } else if is_buy {
            base.after_observed_buy(base_amount)
        } else {
            base.after_observed_sell(base_amount)
        };
        self.live_pump.insert(*token_vault, (cur, shred_slot, advanced));
    }

    /// Pump pool including any live (shred-advanced) state, valid only when it is
    /// still built on the current confirmed gRPC slot AND belongs to the block
    /// `shred_slot` we are pricing for; otherwise the fresh cache decode. Passing
    /// a NEW block slot therefore discards the previous block's accumulated queue.
    pub fn pump_pool_live(
        &self,
        pool: &Pubkey,
        token_vault: &Pubkey,
        wsol_vault: &Pubkey,
        token_mint: &Pubkey,
        shred_slot: u64,
    ) -> Option<PumpPool> {
        let cur = self.last_update_slot(token_vault).unwrap_or(0);
        if let Some(e) = self.live_pump.get(token_vault) {
            if e.value().0 == cur && e.value().1 == shred_slot {
                return Some(e.value().2);
            }
        }
        self.pump_pool(pool, token_vault, wsol_vault, token_mint)
    }

    /// Apply an observed Meteora swap (from a shred) to the live meteora overlay.
    /// `a_to_b` is the swap direction; the caller derives it from the arb's Pump
    /// leg and the pool's token side.
    ///
    /// NOT called on the default path: we assume we are first on Meteora, since
    /// the only Meteora activity we can observe rides on competitor ARB txs and
    /// those overwhelmingly revert. Retained for the future "exactly one tx
    /// ahead" scenario model.
    #[allow(dead_code)]
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
        // Base is ALWAYS the gRPC-confirmed decode (no overlay), and we apply only
        // THIS single in-flight swap on top. Accumulating every observed in-flight
        // swap over-moves the price: if more than one had actually executed, the
        // account would have updated (cur would advance) and the overlay would be
        // rebuilt. At most one swap is genuinely pending unconfirmed, so summing
        // them is the thin-pool over-prediction that produced the 0x1771 reverts.
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
    /// return the fee that is ACTUALLY IN EFFECT this epoch — exactly what the
    /// on-chain program charges.
    ///
    /// A `TransferFeeConfig` stores TWO fee snapshots, `older_transfer_fee` and
    /// `newer_transfer_fee`, each tagged with the epoch it takes effect. SPL
    /// Token-2022 `get_epoch_fee` uses `newer` once `current_epoch >= newer.epoch`
    /// and `older` before that. Taking the LARGER of the two (the old behaviour)
    /// is a FICTION: when a mint lowers its fee (e.g. to 0, as MEMIPEDE/FABLE
    /// did) the newer/lower fee is what the chain applies, so `max()` invents a
    /// fee that no real transfer pays and mis-prices every leg. We reproduce the
    /// on-chain epoch selection instead, reading the true current-epoch fee
    /// (which may legitimately be 0). Current epoch is derived from the live slot
    /// (mainnet: 432_000 slots/epoch); if the slot isn't seeded yet we fall back
    /// to the conservative `max()` so we never understate before the first slot.
    pub fn mint_transfer_fee_bps(&self, mint: &Pubkey) -> u16 {
        // Mainnet-beta has a fixed 432_000 slots per epoch and no warmup, so the
        // epoch is simply slot / SLOTS_PER_EPOCH.
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
            // where TransferFee = epoch(u64,8)+maximum_fee(u64,8)+basis_points(u16,2).
            // So, from data_start: older.epoch@72, older.bps@88;
            //                      newer.epoch@90, newer.bps@106.
            if ext_type == 1 && ext_len >= 108 {
                let older_bps = u16::from_le_bytes([d[data_start + 88], d[data_start + 89]]);
                let newer_epoch = u64::from_le_bytes(
                    d[data_start + 90..data_start + 98].try_into().unwrap(),
                );
                let newer_bps = u16::from_le_bytes([d[data_start + 106], d[data_start + 107]]);
                // On-chain `get_epoch_fee`: newer applies once epoch >= newer.epoch.
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
        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                match run_stream(
                    &endpoint, &x_token, &acct_set, &change, &inner, &last_update,
                    &last_update_slot, &slot, &updates,
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

fn build_request(accounts: &[Pubkey]) -> SubscribeRequest {
    let mut accounts_filter: HashMap<String, SubscribeRequestFilterAccounts> = HashMap::new();
    accounts_filter.insert(
        "arb_pools".to_string(),
        SubscribeRequestFilterAccounts {
            account: accounts.iter().map(|p| p.to_string()).collect(),
            owner: vec![],
            filters: vec![],
            nonempty_txn_signature: None,
        },
    );
    SubscribeRequest {
        accounts: accounts_filter,
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
                        cache.insert(pk, info.data);
                        last_update.insert(pk, std::time::Instant::now());
                        last_update_slot.insert(pk, a.slot);
                        updates.fetch_add(1, Ordering::Relaxed);
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
