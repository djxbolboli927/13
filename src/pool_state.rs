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

use crate::dex_ids::DexKind;
use crate::meteora_math::{MeteoraPool, MAX_SQRT_PRICE, MIN_SQRT_PRICE};
use crate::pool_registry::PoolInfo;
use crate::pumpfun_math::PumpPool;

/// The pricing model for the NON-Pump leg of an arb. Meteora DAMM v2 is
/// concentrated liquidity (`MeteoraPool`); every other supported venue
/// (Meteora Dynamic AMM, Raydium V4, Raydium CPMM) is a plain constant-product
/// AMM priced from its two vault balances (`PumpPool` math with a per-DEX fee).
/// A unified interface lets `assess` treat any counter venue the same way.
#[derive(Clone, Copy)]
pub enum CounterPool {
    Concentrated(MeteoraPool),
    ConstProduct(PumpPool),
}

impl CounterPool {
    /// Raw WSOL-per-token price (quote-raw / base-raw), for direction + gap.
    pub fn token_price_in_sol(&self, token_is_a: bool) -> f64 {
        match self {
            CounterPool::Concentrated(m) => m.token_price_in_sol(token_is_a, 0, 0),
            CounterPool::ConstProduct(p) => {
                if p.base_reserve == 0 {
                    0.0
                } else {
                    p.quote_reserve as f64 / p.base_reserve as f64
                }
            }
        }
    }

    /// Spend `wsol_in` lamports, receive token base. `None` if not tradeable.
    pub fn buy_token_with_wsol(&self, wsol_in: u64, token_is_a: bool) -> Option<u64> {
        match self {
            CounterPool::Concentrated(m) => m.buy_token_with_wsol(wsol_in, token_is_a),
            CounterPool::ConstProduct(p) => {
                let out = p.quote_buy(wsol_in);
                (out > 0).then_some(out)
            }
        }
    }

    /// Spend `token_in` base, receive WSOL lamports. `None` if not tradeable.
    pub fn sell_token_for_wsol(&self, token_in: u64, token_is_a: bool) -> Option<u64> {
        match self {
            CounterPool::Concentrated(m) => m.sell_token_for_wsol(token_in, token_is_a),
            CounterPool::ConstProduct(p) => Some(p.quote_sell(token_in)),
        }
    }

    /// Current WSOL-side reserve (lamports), for the trade-size ceiling.
    pub fn wsol_reserve(&self, token_is_a: bool) -> u64 {
        match self {
            CounterPool::Concentrated(m) => m.wsol_reserve(token_is_a),
            CounterPool::ConstProduct(p) => p.quote_reserve,
        }
    }
}

// Meteora DAMM v2 Pool account field offsets (bytes, discriminator included).
// Validated byte-for-byte against the cp-amm source (PoolFeesStruct = 160B:
// BaseFeeStruct 40B + 3 fee-percent bytes + 5 pad + DynamicFeeStruct 96B +
// 16B padding) and against a live 1112-byte pool account (INIT_SPACE 1104 + 8
// discriminator). The decoded state reproduces on-chain reserves exactly.
//
// `pool_fees` is the first field (offset 8); its first member is
// `base_fee.cliff_fee_numerator: u64`, i.e. the flat swap fee numerator
// (denominator 1e9).
const MET_OFF_CLIFF_FEE: usize = 8;
// DynamicFeeStruct begins at 8 (disc) + 40 (BaseFeeStruct) + 8 (fee percents +
// padding) = 56:
//   initialized u8 @56, pad[7], max_volatility_accumulator u32 @64,
//   variable_fee_control u32 @68, bin_step u16 @72, filter/decay/reduction u16
//   @74/76/78, last_update_timestamp u64 @80, bin_step_u128 @88,
//   sqrt_price_reference @104, volatility_accumulator u128 @120,
//   volatility_reference @136.
const MET_OFF_DYNFEE_INITIALIZED: usize = 56;
const MET_OFF_DYNFEE_VAR_CONTROL: usize = 68;
const MET_OFF_DYNFEE_BIN_STEP: usize = 72;
const MET_OFF_DYNFEE_VOL_ACC: usize = 120;
const MET_OFF_LIQUIDITY: usize = 360;
const MET_OFF_SQRT_MIN: usize = 424;
const MET_OFF_SQRT_MAX: usize = 440;
const MET_OFF_SQRT_PRICE: usize = 456;

/// Max plausible TOTAL swap fee we are willing to trade against (50%, in 1e9
/// units). A pool whose current fee reads above this is skipped outright.
const MAX_TRADEABLE_FEE_NUMERATOR: u64 = 500_000_000;

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

fn read_u8(data: &[u8], off: usize) -> Option<u8> {
    data.get(off).copied()
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
        info!(requested = accounts.len(), fetched = ok, "pool-state prefetch complete");
    }

    /// How long since `account` last changed (seeded at prefetch). `None` if we
    /// have never seen it. Used to detect abandoned/rugged pools by idleness.
    pub fn last_update_age(&self, account: &Pubkey) -> Option<std::time::Duration> {
        self.last_update.get(account).map(|t| t.elapsed())
    }

    /// Decode a Meteora pool account into its pricing slice.
    ///
    /// FEE: the effective swap fee is the base `cliff_fee_numerator` PLUS the
    /// DYNAMIC (volatility) fee — DAMM v2 adds the variable fee on top on-chain,
    /// and omitting it made us over-estimate the output of volatile pools (the
    /// ~2% that turned a modelled edge into a 0x1771 slippage revert). We now
    /// decode the DynamicFeeStruct and add it. If the base fee reads implausibly
    /// (0 or > 50%, e.g. a high anti-sniper scheduled fee), we SKIP the pool
    /// rather than silently substituting a cheap 0.25% — trading it on a wrong
    /// fee guarantees a revert. `fallback_fee_numerator` is unused now (kept for
    /// signature stability).
    ///
    /// Returns `None` if the decoded state is not tradeable — a drained pool
    /// (`liquidity == 0`), a sqrt-price outside the on-chain valid range, an
    /// implausible fee, or a mangled/partial read.
    pub fn meteora_pool(&self, pool: &Pubkey, _fallback_fee_numerator: u64) -> Option<MeteoraPool> {
        let entry = self.inner.get(pool)?;
        let data = entry.value();
        let cliff = read_u64_le(data, MET_OFF_CLIFF_FEE).unwrap_or(0);
        // Base fee must be plausible (0, 50%]; otherwise skip (don't fake it).
        if cliff == 0 || (cliff as u128) > MAX_TRADEABLE_FEE_NUMERATOR as u128 {
            return None;
        }
        // Dynamic (volatility) fee, per cp-amm: with variable_fee_control > 0,
        //   square_vfa_bin = (volatility_accumulator · bin_step)^2
        //   v_fee          = square_vfa_bin · variable_fee_control
        //   scaled_v_fee   = (v_fee + 99_999_999_999) / 100_000_000_000
        // Total fee numerator = cliff + scaled_v_fee (denominator 1e9).
        let var_control = read_u32_le(data, MET_OFF_DYNFEE_VAR_CONTROL).unwrap_or(0) as u128;
        let variable_fee = if read_u8(data, MET_OFF_DYNFEE_INITIALIZED).unwrap_or(0) != 0
            && var_control > 0
        {
            let bin_step = read_u16_le(data, MET_OFF_DYNFEE_BIN_STEP).unwrap_or(0) as u128;
            let vol_acc = read_u128_le(data, MET_OFF_DYNFEE_VOL_ACC).unwrap_or(0);
            let scaled = vol_acc
                .checked_mul(bin_step)
                .and_then(|v| v.checked_pow(2))
                .and_then(|sq| sq.checked_mul(var_control))
                .map(|v_fee| (v_fee + 99_999_999_999) / 100_000_000_000);
            match scaled {
                Some(s) => s,
                // Overflow ⇒ the dynamic fee is enormous ⇒ untradeable, skip.
                None => return None,
            }
        } else {
            0
        };
        let total_fee = cliff as u128 + variable_fee;
        // A total fee above our cap means the pool is currently too expensive to
        // arb (usually a fresh high-volatility/anti-sniper window) — skip it.
        if total_fee > MAX_TRADEABLE_FEE_NUMERATOR as u128 {
            return None;
        }
        let fee_numerator = total_fee as u64;
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

    /// Decode the NON-Pump leg of an arb into a unified `CounterPool`, branching
    /// on the venue. Meteora DAMM v2 uses its concentrated-liquidity account;
    /// every other supported venue is a constant-product AMM priced from its two
    /// vault balances with `cp_fee_bps` (the exact quote still comes from Metis —
    /// this is only the fast pre-check). `None` if not tradeable / not cached.
    pub fn counter_pool(
        &self,
        info: &PoolInfo,
        fallback_fee_numerator: u64,
        cp_fee_bps: u64,
    ) -> Option<CounterPool> {
        match info.kind {
            DexKind::MeteoraDammV2 => self
                .meteora_pool(&info.pool, fallback_fee_numerator)
                .map(CounterPool::Concentrated),
            DexKind::PumpFunAmm => None, // Pump is never the counter leg.
            // Constant-product venues: reserves = the two vault SPL balances.
            DexKind::MeteoraDynamicAmm | DexKind::RaydiumV4 | DexKind::RaydiumCpmm => {
                let base = self.spl_amount(&info.token_vault())?;
                let quote = self.spl_amount(&info.wsol_vault())?;
                if base == 0 || quote == 0 {
                    return None;
                }
                let mut p = PumpPool::new(base, quote);
                p.total_fee_bps = cp_fee_bps;
                p.lp_fee_bps = cp_fee_bps;
                Some(CounterPool::ConstProduct(p))
            }
        }
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
