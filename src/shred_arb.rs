//! Decision + execution engine for the ShredStream / Pump.fun ↔ Meteora arb.
//!
//! Flow per detected Pump.fun swap:
//!   1. Predict the Pump.fun pool reserves AFTER the observed trade (we are
//!      ahead of Metis/chain — this is the edge).
//!   2. Read the current Meteora pool state.
//!   3. Compare the post-trade Pump price to the Meteora price → buy the cheaper
//!      venue, sell the dearer one (direction is dynamic).
//!   4. Ternary-search the input size that maximizes net profit, respecting each
//!      pool's slippage.
//!   5. If net > tip + network fee, force-quote each leg on its venue via Metis,
//!      override the output floor to `input + tip + fee`, build the tx, and send
//!      the bundle through the existing Jito paths.

use solana_client::rpc_client::RpcClient;
use solana_sdk::signature::Keypair;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::alt_cache::AltCache;
use crate::blockhash_cache::BlockhashCache;
use crate::dex_ids::DexKind;
use crate::jito::JitoClient;
use crate::jito_grpc::JitoGrpcClient;
use crate::metis::MetisClient;
use crate::pool_registry::ArbPair;
use crate::pool_state::PoolStateCache;
use crate::pumpfun_math::PumpPool;
use crate::rate_limiter::RateLimiter;
use crate::shred_stream::PumpSwapSignal;
use crate::tokens::WSOL_MINT;
use crate::transaction;

/// Consecutive forced-quote route failures after which a pool is dropped as
/// effectively single-sided / rugged (no route on one of the two venues).
const ROUTE_FAIL_LIMIT: u32 = 5;

/// A raw price gap larger than this is never a real arb — it's a decode
/// artifact or a one-sided dead pool. Real cross-pool gaps are a few percent.
const MAX_PLAUSIBLE_GAP_PCT: f64 = 300.0;

/// Log at most 1 in this many `eval-detail` traces (profitable ones always log).
const EVAL_LOG_SAMPLE: u64 = 50;

/// Tunables sourced from `[shred_arb]` config.
#[derive(Clone)]
pub struct ArbParams {
    pub tip_lamports: u64,
    /// Retained for the legacy Jito path / reference; profit gating now uses
    /// `min_net_profit_lamports`.
    #[allow(dead_code)]
    pub network_fee_lamports: u64,
    pub meteora_fee_bps: u64,
    /// Ignore observed trades whose SOL-side arg is below this (small trades
    /// barely move price).
    pub min_trigger_lamports: u64,
    pub min_amount_lamports: u64,
    pub max_amount_lamports: u64,
    pub cu_limit: u32,
    /// Per-pool cooldown to avoid firing repeatedly on a burst of shreds.
    pub cooldown_ms: u64,
    /// Cap the BUY-leg size so its price impact stays under this fraction
    /// (e.g. 0.01 = 1%). This is what keeps trades tiny on low-liquidity pools.
    pub max_price_impact: f64,
    /// Enter slightly below the computed optimum for slippage headroom
    /// (e.g. 0.03 = 3% smaller).
    pub size_safety_margin: f64,
    /// Reject opportunities whose predicted net profit exceeds this fraction of
    /// the input (e.g. 0.5 = 50%) — always a mispricing on a dead pool.
    pub max_profit_fraction: f64,
    /// Minimum predicted NET profit (lamports, above the network fee) required
    /// before we fetch instructions and send. Production default 5000. This is
    /// the profit GATE; the on-chain minimum output is set separately to exactly
    /// the input (break-even floor) so any trade that clears the gate at predict
    /// time still lands on-chain as long as it doesn't lose money.
    pub min_net_profit_lamports: u64,
    /// Send directly to the network via RPC instead of Jito bundles.
    pub direct_send: bool,
    /// Priority fee (micro-lamports/CU) for direct sends (0 = none).
    pub direct_priority_fee_microlamports: u64,
    /// `maxAccounts` requested per forced leg (controls tx size).
    pub metis_max_accounts: u64,
    /// SetLoadedAccountsDataSizeLimit byte value (0 = don't add).
    pub loaded_accounts_data_limit: u32,
    /// Only react to observed trades ≥ this fraction of the Pump WSOL reserve
    /// (0 = disabled, use the absolute min_trigger only).
    pub min_trigger_reserve_frac: f64,
    /// Metis `dexes=` labels for each venue (configurable).
    pub pump_label: String,
    pub meteora_label: String,
}

pub struct ShredArbEngine {
    pub metis: Arc<MetisClient>,
    pub blockhash_cache: Arc<BlockhashCache>,
    pub trading_keypair: Arc<Keypair>,
    pub rpc_client: Arc<RpcClient>,
    pub alt_cache: AltCache,
    pub jito: Arc<JitoClient>,
    pub jito_grpc: Option<Arc<JitoGrpcClient>>,
    pub jito_limiter: Arc<Mutex<RateLimiter>>,
    pub jito_grpc_limiter: Option<Arc<Mutex<RateLimiter>>>,
    pub user_pubkey: String,
    pub pool_state: PoolStateCache,
    /// Keyed by Pump.fun pool pubkey (what the signal carries). Shared, mutable
    /// registry so pools can be added/removed at runtime.
    pub registry: Arc<dashmap::DashMap<solana_sdk::pubkey::Pubkey, ArbPair>>,
    pub params: ArbParams,
    pub shred_metrics: Arc<crate::shred_stream::ShredMetrics>,
    /// Optional pool manager, used to auto-drop pools that consistently fail to
    /// route on Metis (effectively single-sided / rugged) — closes their ATA too.
    pub manager: Option<Arc<crate::pool_manager::PoolManager>>,
    last_fired: dashmap::DashMap<solana_sdk::pubkey::Pubkey, Instant>,
    /// Consecutive forced-quote route failures per pool. A pool that can't be
    /// routed on both venues repeatedly is dropped (see `ROUTE_FAIL_LIMIT`).
    route_fail: dashmap::DashMap<solana_sdk::pubkey::Pubkey, u32>,
    // ── diagnostics ──
    signals_received: AtomicU64,
    skip_min_trigger: AtomicU64,
    skip_no_pair: AtomicU64,
    skip_cooldown: AtomicU64,
    skip_no_pump_state: AtomicU64,
    skip_no_meteora_state: AtomicU64,
    skip_bad_price: AtomicU64,
    skip_implausible: AtomicU64,
    /// Rolling counter to sample the (very chatty) eval-detail trace.
    eval_log_counter: AtomicU64,
    /// Not profitable because the Meteora swap is INFEASIBLE at any size (range
    /// exhausted / would cross a bound) — a real price gap that can't be
    /// crossed. Distinguishes "no route in the pool" from "gap too small".
    skip_uncrossable: AtomicU64,
    evaluated: AtomicU64,
    profitable: AtomicU64,
    not_profitable: AtomicU64,
    sent: AtomicU64,
    /// Best (max) net lamports the optimizer found in the current window,
    /// including negatives — shows how close we get when nothing is profitable.
    best_net_seen: AtomicI64,
}

/// Which venue we buy on (and therefore which we sell on).
#[derive(Clone, Copy, PartialEq)]
enum BuyOn {
    Pump,
    Meteora,
}

impl ShredArbEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        metis: Arc<MetisClient>,
        blockhash_cache: Arc<BlockhashCache>,
        trading_keypair: Arc<Keypair>,
        rpc_client: Arc<RpcClient>,
        alt_cache: AltCache,
        jito: Arc<JitoClient>,
        jito_grpc: Option<Arc<JitoGrpcClient>>,
        jito_limiter: Arc<Mutex<RateLimiter>>,
        jito_grpc_limiter: Option<Arc<Mutex<RateLimiter>>>,
        user_pubkey: String,
        pool_state: PoolStateCache,
        registry: Arc<dashmap::DashMap<solana_sdk::pubkey::Pubkey, ArbPair>>,
        params: ArbParams,
        shred_metrics: Arc<crate::shred_stream::ShredMetrics>,
        manager: Option<Arc<crate::pool_manager::PoolManager>>,
    ) -> Self {
        Self {
            metis,
            blockhash_cache,
            trading_keypair,
            rpc_client,
            alt_cache,
            jito,
            jito_grpc,
            jito_limiter,
            jito_grpc_limiter,
            user_pubkey,
            pool_state,
            registry,
            params,
            shred_metrics,
            manager,
            last_fired: dashmap::DashMap::new(),
            route_fail: dashmap::DashMap::new(),
            signals_received: AtomicU64::new(0),
            skip_min_trigger: AtomicU64::new(0),
            skip_no_pair: AtomicU64::new(0),
            skip_cooldown: AtomicU64::new(0),
            skip_no_pump_state: AtomicU64::new(0),
            skip_no_meteora_state: AtomicU64::new(0),
            skip_bad_price: AtomicU64::new(0),
            skip_implausible: AtomicU64::new(0),
            eval_log_counter: AtomicU64::new(0),
            skip_uncrossable: AtomicU64::new(0),
            evaluated: AtomicU64::new(0),
            profitable: AtomicU64::new(0),
            not_profitable: AtomicU64::new(0),
            sent: AtomicU64::new(0),
            best_net_seen: AtomicI64::new(i64::MIN),
        }
    }

    /// Consume signals forever.
    pub async fn run(self: Arc<Self>, mut rx: mpsc::Receiver<PumpSwapSignal>) {
        info!(pairs = self.registry.len(), "shred-arb engine running");
        while let Some(sig) = rx.recv().await {
            let me = self.clone();
            tokio::spawn(async move {
                me.handle(sig).await;
            });
        }
    }

    /// Second opportunity source: on a timer, re-assess EVERY tracked pair from
    /// its CURRENT pool state (no shred trigger). ShredStream only fires on a
    /// Pump trade, so a gap that opens from a Meteora-side move (or between Pump
    /// trades) would otherwise be missed until the next Pump swap. This catches
    /// those. Cheap — the calc is microseconds and only profitable pairs hit
    /// Metis.
    pub fn spawn_state_evaluator(self: Arc<Self>, interval_ms: u64) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms.max(50)));
            // Only re-assess a pool whose Meteora pool OR Pump vaults changed in
            // the last `fresh` window — so we don't burn cycles (and flood logs)
            // re-simulating dead pools where nothing has happened for hours.
            let fresh = Duration::from_millis(interval_ms.saturating_mul(4).max(800));
            loop {
                ticker.tick().await;
                let pairs: Vec<(solana_sdk::pubkey::Pubkey, ArbPair)> = self
                    .registry
                    .iter()
                    .map(|e| (*e.key(), e.value().clone()))
                    .collect();
                for (pool, pair) in pairs {
                    // Freshness: did any relevant account update recently?
                    let recent = [
                        pair.meteora.pool,
                        pair.pump.token_vault(),
                        pair.pump.wsol_vault(),
                    ]
                    .iter()
                    .filter_map(|a| self.pool_state.last_update_age(a))
                    .any(|age| age <= fresh);
                    if !recent {
                        continue; // nothing changed → no new opportunity
                    }
                    let pump_now = match self
                        .pool_state
                        .pump_pool(&pair.pump.token_vault(), &pair.pump.wsol_vault())
                    {
                        Some(p) => p,
                        None => continue,
                    };
                    // No prediction — price against the current Pump state.
                    self.assess(&pair, pool, pump_now).await;
                }
            }
        });
    }

    async fn handle(&self, sig: PumpSwapSignal) {
        self.signals_received.fetch_add(1, Ordering::Relaxed);
        if sig.quote_amount < self.params.min_trigger_lamports {
            self.skip_min_trigger.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let pair = match self.registry.get(&sig.pool) {
            Some(p) => p.clone(),
            None => {
                self.skip_no_pair.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };

        // 1) Current Pump reserves → predicted post-trade reserves.
        let pump_now = match self
            .pool_state
            .pump_pool(&pair.pump.token_vault(), &pair.pump.wsol_vault())
        {
            Some(p) => p,
            None => {
                self.skip_no_pump_state.fetch_add(1, Ordering::Relaxed);
                debug!(pool = %sig.pool, "pump reserves not cached yet");
                return;
            }
        };
        // Liquidity-relative trigger: skip trades too small to move THIS pool's
        // price meaningfully (more precise than a flat lamport threshold — a
        // "big" trade on a thin pool is tiny on a deep one).
        if self.params.min_trigger_reserve_frac > 0.0 {
            let thresh =
                (pump_now.quote_reserve as f64 * self.params.min_trigger_reserve_frac) as u64;
            if sig.quote_amount < thresh {
                self.skip_min_trigger.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        // Predicted post-trade Pump reserves — we are ahead of Metis/chain.
        let pump_after = if sig.is_buy {
            pump_now.after_observed_buy(sig.base_amount)
        } else {
            pump_now.after_observed_sell(sig.base_amount)
        };
        self.assess(&pair, sig.pool, pump_after).await;
    }

    /// Shared assessment core: read Meteora, choose direction, size the trade,
    /// and execute if it clears the profit gate. `pump_after` is the Pump state
    /// to price/quote against — predicted post-trade reserves for a shred
    /// trigger, or the current reserves for a pool-state-update trigger.
    async fn assess(&self, pair: &ArbPair, pool: solana_sdk::pubkey::Pubkey, pump_after: PumpPool) {
        // Cooldown per pool — applies only after a fire, so the state evaluator
        // can keep re-checking a not-yet-profitable pool every tick.
        if let Some(prev) = self.last_fired.get(&pool) {
            if prev.elapsed() < Duration::from_millis(self.params.cooldown_ms) {
                self.skip_cooldown.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        self.evaluated.fetch_add(1, Ordering::Relaxed);

        // 2) Meteora state.
        // config fallback fee is in bps; convert to the 1e9-denominated numerator.
        let fallback_fee_numerator = self.params.meteora_fee_bps.saturating_mul(100_000);
        let meteora = match self
            .pool_state
            .meteora_pool(&pair.meteora.pool, fallback_fee_numerator)
        {
            Some(m) => m,
            None => {
                self.skip_no_meteora_state.fetch_add(1, Ordering::Relaxed);
                debug!(pool = %pair.meteora.pool, "meteora state not cached yet");
                return;
            }
        };

        // 3) Direction: compare raw price (WSOL-raw per token-raw) on both.
        let pump_price = if pump_after.base_reserve == 0 {
            self.skip_bad_price.fetch_add(1, Ordering::Relaxed);
            return;
        } else {
            pump_after.quote_reserve as f64 / pump_after.base_reserve as f64
        };
        let met_price = meteora.token_price_in_sol(pair.meteora.token_is_a, 0, 0);
        if met_price <= 0.0 {
            self.skip_bad_price.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let buy_on = if pump_price < met_price {
            BuyOn::Pump // token cheaper on pump
        } else {
            BuyOn::Meteora
        };

        // 4) Optimal size.
        let token_is_a = pair.meteora.token_is_a;
        let eval = |x: u64| -> Option<u64> {
            match buy_on {
                BuyOn::Pump => {
                    let base_out = pump_after.quote_buy(x);
                    if base_out == 0 {
                        return None;
                    }
                    meteora.sell_token_for_wsol(base_out, token_is_a)
                }
                BuyOn::Meteora => {
                    let base_out = meteora.buy_token_with_wsol(x, token_is_a)?;
                    if base_out == 0 {
                        return None;
                    }
                    Some(PumpPool::quote_sell(&pump_after, base_out))
                }
            }
        };

        // Profit GATE: the optimizer maximizes `out - x - required_extra`, where
        // `required_extra` is the minimum profit we insist on (default 5000 =
        // one network base fee). Direct sends pay only that fee, no Jito tip.
        // NOTE: this is only the gate — the on-chain minimum output is set to
        // exactly the input (break-even) at send time, so a trade that clears
        // the gate at predict time still lands as long as it doesn't lose money.
        let required_extra = self.params.min_net_profit_lamports;

        // Ceiling on trade size: never add more WSOL than a fraction of the
        // BUY pool's current WSOL reserve (keeps the swap in a valid range and
        // slippage sane). `max_price_impact` is that fraction (default 1.0).
        // The optimizer then finds the net-maximizing size WITHIN this range —
        // that size already balances the price gap against slippage, which is
        // the real "optimal volume".
        let buy_wsol_reserve = match buy_on {
            BuyOn::Pump => pump_after.quote_reserve,
            BuyOn::Meteora => meteora.wsol_reserve(token_is_a),
        };
        // Dead-pool guards — skip empty/broken pools early (they were flooding
        // the logs and wasting cycles): the BUY side must hold real WSOL depth,
        // and the price gap must be plausible. A real cross-pool gap is small
        // (competitors trade ~0.2%); a 100%+ "gap" is a decode artifact or a
        // one-sided dead pool, never an executable arb.
        let gap_pct = (pump_price - met_price) / met_price * 100.0;
        if buy_wsol_reserve < 5_000 || gap_pct.abs() > MAX_PLAUSIBLE_GAP_PCT {
            self.skip_bad_price.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let liq_ceiling = ((buy_wsol_reserve as f64) * self.params.max_price_impact) as u64;
        let hi = self
            .params
            .max_amount_lamports
            .min(liq_ceiling)
            .max(self.params.min_amount_lamports);

        let (opt_x, opt_net) =
            optimize_size(self.params.min_amount_lamports, hi, required_extra, &eval);

        // Track how close we get, even when nothing is profitable, for tuning.
        self.best_net_seen
            .fetch_max(opt_net.clamp(i64::MIN as i128, i64::MAX as i128) as i64, Ordering::Relaxed);

        // Detailed per-evaluation trace — SAMPLED (1 in EVAL_LOG_SAMPLE) so it
        // doesn't flood the terminal; profitable evaluations always print.
        let log_this = opt_net > 0
            || self.eval_log_counter.fetch_add(1, Ordering::Relaxed) % EVAL_LOG_SAMPLE == 0;
        if log_this {
            let sample = |x: u64| -> Option<i64> {
                eval(x).map(|o| o as i64 - x as i64 - required_extra as i64)
            };
            info!(
                token = %pair.token_mint,
                buy = if buy_on == BuyOn::Pump { "Pump" } else { "Meteora" },
                gap_pct,
                pump_after_price = pump_price,
                met_price,
                met_wsol_depth = meteora.wsol_reserve(token_is_a),
                buy_wsol_reserve,
                hi,
                opt_x,
                opt_net = opt_net as i64,
                net_1k = ?sample(1_000),
                net_10k = ?sample(10_000),
                net_100k = ?sample(100_000),
                "eval-detail"
            );
        }

        if opt_net <= 0 {
            // Diagnose: if even a tiny buy is infeasible, the Meteora side is
            // range-exhausted (a real gap we simply cannot cross) rather than
            // the gap being too small.
            if eval(self.params.min_amount_lamports.max(1_000)).is_none() {
                self.skip_uncrossable.fetch_add(1, Ordering::Relaxed);
            }
            self.not_profitable.fetch_add(1, Ordering::Relaxed);
            return;
        }

        // Enter slightly below the optimum for slippage headroom.
        let best_x = (((opt_x as f64) * (1.0 - self.params.size_safety_margin)) as u64)
            .max(self.params.min_amount_lamports);
        let best_out = match eval(best_x) {
            Some(o) => o,
            None => {
                self.not_profitable.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        let profit_floor = best_x + required_extra;
        if best_out <= profit_floor {
            self.not_profitable.fetch_add(1, Ordering::Relaxed);
            return; // predicted profit below the minimum gate
        }
        let net = best_out - profit_floor;

        // Plausibility guard: a real cross-pool gap is small. A predicted net
        // above `max_profit_fraction` of the input is always a dead-pool
        // mispricing — refuse to send garbage.
        if net as f64 > best_x as f64 * self.params.max_profit_fraction {
            self.skip_implausible.fetch_add(1, Ordering::Relaxed);
            debug!(
                pool = %pool, input = best_x, net, "skip implausible profit (mispriced pool)"
            );
            return;
        }
        self.profitable.fetch_add(1, Ordering::Relaxed);

        let (buy_kind, sell_kind) = match buy_on {
            BuyOn::Pump => (DexKind::PumpFunAmm, DexKind::MeteoraDammV2),
            BuyOn::Meteora => (DexKind::MeteoraDammV2, DexKind::PumpFunAmm),
        };
        info!(
            pool = %pool,
            token = %pair.token_mint,
            buy = ?buy_kind_label(buy_kind),
            input = best_x,
            predicted_out = best_out,
            net_lamports = net,
            "shred-arb opportunity"
        );

        self.last_fired.insert(pool, Instant::now());
        // On-chain minimum output = exactly the input (break-even). The tx then
        // reverts only if the trade would actually lose lamports; any realized
        // price still at/above break-even lands and captures whatever profit
        // exists at execution time.
        let onchain_floor = best_x;
        self.execute(pair, pool, buy_kind, sell_kind, best_x, onchain_floor)
            .await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute(
        &self,
        pair: &ArbPair,
        pool: solana_sdk::pubkey::Pubkey,
        buy_kind: DexKind,
        sell_kind: DexKind,
        amount_in: u64,
        floor: u64,
    ) {
        let token = pair.token_mint.to_string();
        let buy_label = self.label_for(buy_kind);
        let sell_label = self.label_for(sell_kind);

        // Leg 1: forced buy on `buy_kind` (WSOL → token).
        let q1 = match self
            .metis
            .get_quote_forced(WSOL_MINT, &token, amount_in, buy_label, self.params.metis_max_accounts)
            .await
        {
            Ok(q) => q,
            Err(e) => {
                warn!(error = %e, leg = "buy", venue = buy_label, "forced quote failed — re-adding market");
                self.readd_market(&pair, buy_kind).await;
                self.note_route_failure(pool);
                return;
            }
        };
        let token_amt: u64 = match q1.out_amount.parse().ok().filter(|&v| v > 0) {
            Some(v) => v,
            None => return,
        };

        // Leg 2: forced sell on `sell_kind` (token → WSOL).
        let q2 = match self
            .metis
            .get_quote_forced(&token, WSOL_MINT, token_amt, sell_label, self.params.metis_max_accounts)
            .await
        {
            Ok(q) => q,
            Err(e) => {
                warn!(error = %e, leg = "sell", venue = sell_label, "forced quote failed — re-adding market");
                self.readd_market(&pair, sell_kind).await;
                self.note_route_failure(pool);
                return;
            }
        };
        // Both legs routed — this pool is genuinely two-sided; clear any strikes.
        self.note_route_success(pool);

        // Set the on-chain floor to exactly the input (break-even) regardless of
        // what Metis quoted (our data is ahead of Metis).
        let merged = match MetisClient::merge_quotes(&q1, &q2, floor) {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "merge_quotes failed");
                return;
            }
        };

        let swap_ixs = match self
            .metis
            .get_swap_instructions(&self.user_pubkey, &merged)
            .await
        {
            Ok(s) => s,
            Err(_) => {
                warn!("swap_instructions failed for forced arb");
                return;
            }
        };

        let recent_blockhash = self.blockhash_cache.get();
        let keypair = self.trading_keypair.clone();
        let alt = self.alt_cache.clone();
        let rpc = self.rpc_client.clone();
        let cu = self.params.cu_limit;

        // ── Direct-to-RPC send (default): no Jito, no tip, minimal latency ──
        if self.params.direct_send {
            let prio = self.params.direct_priority_fee_microlamports;
            let data_limit = self.params.loaded_accounts_data_limit;
            let tx = match tokio::task::spawn_blocking(move || {
                transaction::build_direct_transaction(
                    &swap_ixs,
                    &keypair,
                    cu,
                    prio,
                    data_limit,
                    recent_blockhash,
                    &alt,
                    &rpc,
                )
            })
            .await
            {
                Ok(Ok(tx)) => tx,
                _ => {
                    warn!("build_direct_transaction failed");
                    return;
                }
            };

            if transaction::account_lock_count(&tx) > 64 {
                warn!("direct arb tx exceeds 64 account locks, dropping");
                return;
            }
            // Pre-check the raw byte size (Solana caps at 1232) so we don't burn
            // an RPC round-trip on a guaranteed "-32602 too large" rejection.
            let raw = transaction::serialized_len(&tx);
            if raw > 1232 {
                warn!(
                    bytes = raw,
                    max_accounts = self.params.metis_max_accounts,
                    "direct arb tx too large ({} > 1232 bytes), dropping — lower metis_max_accounts",
                    raw
                );
                return;
            }

            let rpc2 = self.rpc_client.clone();
            let send_res = tokio::task::spawn_blocking(move || {
                use solana_client::rpc_config::RpcSendTransactionConfig;
                rpc2.send_transaction_with_config(
                    &tx,
                    RpcSendTransactionConfig {
                        skip_preflight: true,
                        max_retries: Some(0),
                        ..Default::default()
                    },
                )
            })
            .await;
            match send_res {
                Ok(Ok(sig)) => {
                    self.sent.fetch_add(1, Ordering::Relaxed);
                    info!(signature = %sig, input = amount_in, "shred-arb tx sent (direct)");
                }
                Ok(Err(e)) => warn!(error = %e, "direct send failed"),
                Err(e) => warn!(error = %e, "direct send task join failed"),
            }
            return;
        }

        // ── Jito bundle path (legacy, direct_send = false) ──
        let use_grpc = if self.jito_limiter.lock().unwrap().try_acquire() {
            false
        } else if self
            .jito_grpc_limiter
            .as_ref()
            .map(|g| g.lock().unwrap().try_acquire())
            .unwrap_or(false)
        {
            true
        } else {
            debug!("jito rate-limited, dropping arb");
            return;
        };

        let tip = self.params.tip_lamports;
        let tx = match tokio::task::spawn_blocking(move || {
            transaction::build_arb_transaction(
                &swap_ixs,
                &keypair,
                tip,
                cu,
                recent_blockhash,
                &alt,
                &rpc,
            )
        })
        .await
        {
            Ok(Ok(tx)) => tx,
            _ => {
                warn!("build_arb_transaction failed");
                return;
            }
        };

        if transaction::account_lock_count(&tx) > 64 {
            warn!("forced arb tx exceeds 64 account locks, dropping");
            return;
        }

        let result = if use_grpc {
            match &self.jito_grpc {
                Some(g) => g.send_bundle(&tx).await,
                None => self.jito.send_bundle(&tx).await,
            }
        } else {
            self.jito.send_bundle(&tx).await
        };

        match result {
            Ok(id) => {
                self.sent.fetch_add(1, Ordering::Relaxed);
                info!(bundle = %id, input = amount_in, "shred-arb bundle sent");
            }
            Err(e) => warn!(error = %e, "shred-arb bundle send failed"),
        }
    }

    /// The configurable Metis `dexes=` label for a venue.
    fn label_for(&self, kind: DexKind) -> &str {
        match kind {
            DexKind::PumpFunAmm => &self.params.pump_label,
            DexKind::MeteoraDammV2 => &self.params.meteora_label,
        }
    }

    /// Re-register the relevant leg's pool with Metis after a "No routes"
    /// failure — the market may not have loaded on the first add, or Metis
    /// restarted. Cheap and idempotent; the route-failure counter still drops
    /// the pool if it keeps failing.
    async fn readd_market(&self, pair: &ArbPair, kind: DexKind) {
        let (info, owner) = match kind {
            DexKind::PumpFunAmm => (&pair.pump, crate::dex_ids::PUMPFUN_AMM_PROGRAM),
            DexKind::MeteoraDammV2 => (&pair.meteora, crate::dex_ids::METEORA_DAMM_V2_PROGRAM),
        };
        let alt = info.alt.map(|a| a.to_string());
        if let Err(e) = self
            .metis
            .add_market(&info.pool.to_string(), owner, alt.as_deref())
            .await
        {
            debug!(pool = %info.pool, error = %e, "re-add market failed");
        }
    }

    /// Record a forced-quote route failure for `pool`. Once a pool accrues
    /// `ROUTE_FAIL_LIMIT` consecutive failures it is effectively single-sided
    /// (or rugged) on Metis, so we drop it (and close its ATA) via the manager.
    fn note_route_failure(&self, pool: solana_sdk::pubkey::Pubkey) {
        let n = {
            let mut e = self.route_fail.entry(pool).or_insert(0);
            *e += 1;
            *e
        };
        if n >= ROUTE_FAIL_LIMIT {
            self.route_fail.remove(&pool);
            warn!(%pool, "dropping pool after repeated Metis route failures (single-sided/rugged)");
            if let Some(mgr) = &self.manager {
                // close_ata inside remove_pair does blocking RPC — offload it.
                let mgr = mgr.clone();
                tokio::task::spawn_blocking(move || mgr.remove_pair(&pool));
            } else {
                self.registry.remove(&pool);
            }
        }
    }

    /// Reset the route-failure strike count for `pool` after a successful route.
    fn note_route_success(&self, pool: solana_sdk::pubkey::Pubkey) {
        if self.route_fail.contains_key(&pool) {
            self.route_fail.remove(&pool);
        }
    }

    pub fn spawn_reporter(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(30));
            let fallback_fee = self.params.meteora_fee_bps.saturating_mul(100_000);
            // Snapshot of window-start counters for per-30s deltas.
            let m = &self.shred_metrics;
            let mut prev_entries = 0u64;
            let mut prev_txns = 0u64;
            let mut prev_pump = 0u64;
            let mut prev_matched = 0u64;
            let mut prev_updates = 0u64;
            let mut prev_eval = 0u64;
            loop {
                ticker.tick().await;
                let entries = m.entries.load(Ordering::Relaxed);
                let txns = m.txns.load(Ordering::Relaxed);
                let pump = m.pump_txns.load(Ordering::Relaxed);
                let matched = m.matched.load(Ordering::Relaxed);
                let updates = self.pool_state.updates();
                let eval = self.evaluated.load(Ordering::Relaxed);

                // Everything below is per-30s-window (delta), plus lifetime totals.
                eprintln!(
                    "\n[shred-arb 30s] watching_pump_pools={} subscribed_cache={} pool_updates={} (Δ)\n\
                     SHRED   : entriesΔ={} txnsΔ={} pumpfun_txnsΔ={} matched_our_poolsΔ={} unresolved={} signals_sent={} signals_dropped={}\n\
                     ENGINE  : signals_recv={} evaluatedΔ={} | skip[min_trig={} no_pair={} cooldown={} no_pump_state={} no_meteora_state={} bad_price={} implausible={}]\n\
                     RESULT  : profitable={} not_profitable={} (uncrossable={}) sent={} best_net_lamports_window={} | pool_slot={}",
                    m.watched_pools.load(Ordering::Relaxed),
                    self.pool_state.cache_size(),
                    updates.saturating_sub(prev_updates),
                    entries.saturating_sub(prev_entries),
                    txns.saturating_sub(prev_txns),
                    pump.saturating_sub(prev_pump),
                    matched.saturating_sub(prev_matched),
                    m.unresolved_pool.load(Ordering::Relaxed),
                    m.signals_sent.load(Ordering::Relaxed),
                    m.signals_dropped.load(Ordering::Relaxed),
                    self.signals_received.load(Ordering::Relaxed),
                    eval.saturating_sub(prev_eval),
                    self.skip_min_trigger.load(Ordering::Relaxed),
                    self.skip_no_pair.load(Ordering::Relaxed),
                    self.skip_cooldown.load(Ordering::Relaxed),
                    self.skip_no_pump_state.load(Ordering::Relaxed),
                    self.skip_no_meteora_state.load(Ordering::Relaxed),
                    self.skip_bad_price.load(Ordering::Relaxed),
                    self.skip_implausible.load(Ordering::Relaxed),
                    self.profitable.load(Ordering::Relaxed),
                    self.not_profitable.load(Ordering::Relaxed),
                    self.skip_uncrossable.load(Ordering::Relaxed),
                    self.sent.load(Ordering::Relaxed),
                    {
                        let v = self.best_net_seen.swap(i64::MIN, Ordering::Relaxed);
                        if v == i64::MIN { 0 } else { v }
                    },
                    self.pool_state.slot(),
                );
                prev_entries = entries;
                prev_txns = txns;
                prev_pump = pump;
                prev_matched = matched;
                prev_updates = updates;
                prev_eval = eval;

                // Decoded-state snapshot for calibration & diagnosis. Always
                // printed so a missing side is visible (compare the prices
                // against a live Metis quote for the same pool/size).
                if self.registry.is_empty() {
                    eprintln!("  SNAPSHOT: no pairs registered yet — waiting for discovery/mix.json");
                }
                for entry in self.registry.iter().take(3) {
                    let pair = entry.value();
                    let met = self
                        .pool_state
                        .meteora_pool(&pair.meteora.pool, fallback_fee);
                    let pump = self
                        .pool_state
                        .pump_pool(&pair.pump.token_vault(), &pair.pump.wsol_vault());
                    match (met, pump) {
                        (Some(m), Some(p)) => {
                            let met_price = m.token_price_in_sol(pair.meteora.token_is_a, 0, 0);
                            let pump_price = if p.base_reserve == 0 {
                                0.0
                            } else {
                                p.quote_reserve as f64 / p.base_reserve as f64
                            };
                            let diff_pct = if met_price > 0.0 {
                                (pump_price - met_price) / met_price * 100.0
                            } else {
                                0.0
                            };
                            eprintln!(
                                "  SNAPSHOT {}: meteora[sqrtP={} L={} fee_num={} price={:.6e}] \
                                 pump[base={} quote={} price={:.6e}] diff={:.3}%",
                                pair.token_mint, m.sqrt_price, m.liquidity, m.fee_numerator,
                                met_price, p.base_reserve, p.quote_reserve, pump_price, diff_pct,
                            );
                        }
                        (met_o, pump_o) => {
                            eprintln!(
                                "  SNAPSHOT {}: meteora_cached={} pump_cached={} \
                                 (meteora_pool={} token_vault={} wsol_vault={})",
                                pair.token_mint,
                                met_o.is_some(),
                                pump_o.is_some(),
                                pair.meteora.pool,
                                pair.pump.token_vault(),
                                pair.pump.wsol_vault(),
                            );
                        }
                    }
                }
            }
        });
    }
}

fn buy_kind_label(k: DexKind) -> &'static str {
    match k {
        DexKind::PumpFunAmm => "PumpFun",
        DexKind::MeteoraDammV2 => "Meteora",
    }
}

/// Find the input that maximizes net profit `eval(x) - x - required_extra`.
///
/// The objective is not guaranteed unimodal (the swap can return `None` for
/// oversized trades that cross a range bound), so a plain ternary search can
/// get stuck. We instead sweep a GEOMETRIC grid across `[lo, hi]` — which is
/// dense at the small sizes these low-liquidity pools actually want — then
/// refine linearly around the best grid point. Returns `(best_x, best_net)`.
fn optimize_size<F: Fn(u64) -> Option<u64>>(
    lo: u64,
    hi: u64,
    required_extra: u64,
    eval: &F,
) -> (u64, i128) {
    let net = |x: u64| -> i128 {
        match eval(x) {
            Some(out) => out as i128 - x as i128 - required_extra as i128,
            None => i128::MIN,
        }
    };
    let lo = lo.max(1);
    if hi <= lo {
        return (lo, net(lo));
    }

    // Geometric sweep.
    const STEPS: usize = 240;
    let lo_f = lo as f64;
    let ratio = (hi as f64 / lo_f).powf(1.0 / STEPS as f64);
    let mut best_x = lo;
    let mut best_net = net(lo);
    let mut xf = lo_f;
    for _ in 0..=STEPS {
        let x = xf as u64;
        let n = net(x);
        if n > best_net {
            best_net = n;
            best_x = x;
        }
        xf *= ratio;
    }

    // Linear refinement around the best grid point.
    let span = (best_x / 10).max(1);
    let a = best_x.saturating_sub(span).max(lo);
    let b = best_x.saturating_add(span).min(hi);
    let step = ((b - a) / 50).max(1);
    let mut x = a;
    while x <= b {
        let n = net(x);
        if n > best_net {
            best_net = n;
            best_x = x;
        }
        x = x.saturating_add(step);
    }
    (best_x, best_net)
}
