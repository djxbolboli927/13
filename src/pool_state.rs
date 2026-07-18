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
// BaseFeeStruct @8:  cliff_fee_numerator u64 @8, fee_scheduler_mode u8 @16,
//                    padding[5], number_of_period u16 @22,
//                    period_frequency u64 @24, reduction_factor u64 @32,
//                    padding u64 @40 (ends @56)
// protocol/partner/referral percents + padding @56..64
// DynamicFeeStruct @64: initialized u8 @64, padding[7],
//                    max_volatility_accumulator u32 @72,
//                    variable_fee_control u32 @76, bin_step u16 @80,
//                    filter_period u16 @82, decay_period u16 @84,
//                    reduction_factor u16 @86, last_update_timestamp u64 @88,
//                    bin_step_u128 u128 @96, sqrt_price_reference u128 @112,
//                    volatility_accumulator u128 @128,
//                    volatility_reference u128 @144 (ends @160)
// Pool tail: activation_point u64 @472, activation_type u8 @480 (0=slot,
// 1=unix timestamp).
const MET_OFF_SCHED_MODE: usize = 16;
const MET_OFF_NUM_PERIOD: usize = 22;
const MET_OFF_PERIOD_FREQ: usize = 24;
const MET_OFF_REDUCTION: usize = 32;
const MET_OFF_DYN_INIT: usize = 64;
const MET_OFF_DYN_VFC: usize = 76;
const MET_OFF_DYN_BIN_STEP: usize = 80;
const MET_OFF_DYN_VOL_ACC: usize = 128;
const MET_OFF_ACTIVATION_POINT: usize = 472;
const MET_OFF_ACTIVATION_TYPE: usize = 480;

/// On-chain cap: fees never exceed 50% (numerator over 1e9).
const MAX_FEE_NUMERATOR: u64 = 500_000_000;
const BASIS_POINT_MAX: u64 = 10_000;

// SPL token account: amount is a u64 LE at offset 64.
const SPL_AMOUNT_OFFSET: usize = 64;

#[derive(Clone)]
pub struct PoolStateCache {
    inner: Arc<DashMap<Pubkey, Vec<u8>>>,
    /// Last time each account's data changed (seeded at prefetch, refreshed on
    /// every stream update). Used by the rug monitor's idle-timeout check.
    last_update: Arc<DashMap<Pubkey, std::time::Instant>>,
    slot: Arc<AtomicU64>,
    /// Total account updates received (for diagnostics).
    updates: Arc<AtomicU64>,
    /// The full set of accounts we currently subscribe to. Adding to this set
    /// and pinging `change` re-sends the (overwriting) SubscribeRequest live.
    accounts: Arc<std::sync::Mutex<Vec<Pubkey>>>,
    change: Arc<tokio::sync::Notify>,
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
fn meteora_total_fee_numerator(data: &[u8], current_slot: u64) -> Option<u64> {
    let cliff = read_u64_le(data, MET_OFF_CLIFF_FEE)?;
    if cliff == 0 || cliff > MAX_FEE_NUMERATOR {
        return None; // implausible read — caller falls back to config
    }

    // ── Base fee via the time/slot scheduler ──
    let mut base = cliff;
    let period_freq = read_u64_le(data, MET_OFF_PERIOD_FREQ).unwrap_or(0);
    if period_freq > 0 {
        let num_period = read_u16_le(data, MET_OFF_NUM_PERIOD).unwrap_or(0) as u64;
        let reduction = read_u64_le(data, MET_OFF_REDUCTION).unwrap_or(0);
        let mode = data.get(MET_OFF_SCHED_MODE).copied().unwrap_or(0);
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
            // Exponential: cliff * (1 - reduction/10000)^period.
            let r = (reduction.min(BASIS_POINT_MAX) as f64) / BASIS_POINT_MAX as f64;
            ((cliff as f64) * (1.0 - r).powi(period.min(u16::MAX as u64) as i32)) as u64
        } else {
            // Linear.
            cliff.saturating_sub(reduction.saturating_mul(period))
        };
    }

    // ── Dynamic (volatility) fee ──
    let mut dynamic: u64 = 0;
    if data.get(MET_OFF_DYN_INIT).copied().unwrap_or(0) != 0 {
        let vfc = read_u32_le(data, MET_OFF_DYN_VFC).unwrap_or(0) as u128;
        let bin_step = read_u16_le(data, MET_OFF_DYN_BIN_STEP).unwrap_or(0) as u128;
        let vol_acc = read_u128_le(data, MET_OFF_DYN_VOL_ACC).unwrap_or(0);
        if vfc > 0 && bin_step > 0 && vol_acc > 0 {
            let vfa = vol_acc.saturating_mul(bin_step);
            let square = vfa.saturating_mul(vfa);
            let v_fee = square.saturating_mul(vfc);
            dynamic = v_fee
                .saturating_add(99_999_999_999)
                .checked_div(100_000_000_000)
                .unwrap_or(0)
                .min(MAX_FEE_NUMERATOR as u128) as u64;
        }
    }

    Some(base.saturating_add(dynamic).min(MAX_FEE_NUMERATOR))
}

impl PoolStateCache {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::with_capacity(256)),
            last_update: Arc::new(DashMap::with_capacity(256)),
            slot: Arc::new(AtomicU64::new(0)),
            updates: Arc::new(AtomicU64::new(0)),
            accounts: Arc::new(std::sync::Mutex::new(Vec::new())),
            change: Arc::new(tokio::sync::Notify::new()),
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
        for pk in accounts {
            match rpc.get_account(pk) {
                Ok(acct) => {
                    self.inner.insert(*pk, acct.data);
                    self.last_update.insert(*pk, now);
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

    /// Decode a Meteora pool account into its pricing slice. The fee is read
    /// straight from the pool's `cliff_fee_numerator`; `fallback_fee_numerator`
    /// (from config) is used only if that read looks implausible.
    ///
    /// Returns `None` if the decoded state is not tradeable — a drained pool
    /// (`liquidity == 0`), a sqrt-price outside the on-chain valid range, or a
    /// mangled/partial read — so the engine never sizes a trade off garbage.
    pub fn meteora_pool(&self, pool: &Pubkey, fallback_fee_numerator: u64) -> Option<MeteoraPool> {
        let entry = self.inner.get(pool)?;
        let data = entry.value();
        // Current TOTAL fee: scheduler-adjusted base fee + volatility-based
        // dynamic fee (can push a "0.25%" pool well past 2%). Falls back to the
        // config value only when the on-chain read is implausible.
        let fee_numerator =
            meteora_total_fee_numerator(data, self.slot.load(Ordering::Relaxed))
                .unwrap_or(fallback_fee_numerator);
        let p = MeteoraPool {
            liquidity: read_u128_le(data, MET_OFF_LIQUIDITY)?,
            sqrt_min_price: read_u128_le(data, MET_OFF_SQRT_MIN)?,
            sqrt_max_price: read_u128_le(data, MET_OFF_SQRT_MAX)?,
            sqrt_price: read_u128_le(data, MET_OFF_SQRT_PRICE)?,
            fee_numerator,
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
    pub fn pump_pool(&self, token_vault: &Pubkey, wsol_vault: &Pubkey) -> Option<PumpPool> {
        let base = self.spl_amount(token_vault)?;
        let quote = self.spl_amount(wsol_vault)?;
        Some(PumpPool::new(base, quote))
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
        let slot = self.slot.clone();
        let updates = self.updates.clone();
        let acct_set = self.accounts.clone();
        let change = self.change.clone();
        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                match run_stream(
                    &endpoint, &x_token, &acct_set, &change, &inner, &last_update, &slot, &updates,
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
