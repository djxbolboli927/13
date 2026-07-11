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
use std::collections::HashMap;
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

/// Tunables sourced from `[shred_arb]` config.
#[derive(Clone)]
pub struct ArbParams {
    pub tip_lamports: u64,
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
    /// Keyed by Pump.fun pool pubkey (what the signal carries).
    pub pairs: HashMap<solana_sdk::pubkey::Pubkey, ArbPair>,
    pub params: ArbParams,
    pub shred_metrics: Arc<crate::shred_stream::ShredMetrics>,
    last_fired: dashmap::DashMap<solana_sdk::pubkey::Pubkey, Instant>,
    // ── diagnostics ──
    signals_received: AtomicU64,
    skip_min_trigger: AtomicU64,
    skip_no_pair: AtomicU64,
    skip_cooldown: AtomicU64,
    skip_no_pump_state: AtomicU64,
    skip_no_meteora_state: AtomicU64,
    skip_bad_price: AtomicU64,
    skip_implausible: AtomicU64,
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
        pairs: Vec<ArbPair>,
        params: ArbParams,
        shred_metrics: Arc<crate::shred_stream::ShredMetrics>,
    ) -> Self {
        let map = pairs.into_iter().map(|p| (p.pump.pool, p)).collect();
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
            pairs: map,
            params,
            shred_metrics,
            last_fired: dashmap::DashMap::new(),
            signals_received: AtomicU64::new(0),
            skip_min_trigger: AtomicU64::new(0),
            skip_no_pair: AtomicU64::new(0),
            skip_cooldown: AtomicU64::new(0),
            skip_no_pump_state: AtomicU64::new(0),
            skip_no_meteora_state: AtomicU64::new(0),
            skip_bad_price: AtomicU64::new(0),
            skip_implausible: AtomicU64::new(0),
            evaluated: AtomicU64::new(0),
            profitable: AtomicU64::new(0),
            not_profitable: AtomicU64::new(0),
            sent: AtomicU64::new(0),
            best_net_seen: AtomicI64::new(i64::MIN),
        }
    }

    /// Consume signals forever.
    pub async fn run(self: Arc<Self>, mut rx: mpsc::Receiver<PumpSwapSignal>) {
        info!(pairs = self.pairs.len(), "shred-arb engine running");
        while let Some(sig) = rx.recv().await {
            let me = self.clone();
            tokio::spawn(async move {
                me.handle(sig).await;
            });
        }
    }

    async fn handle(&self, sig: PumpSwapSignal) {
        self.signals_received.fetch_add(1, Ordering::Relaxed);
        if sig.quote_amount < self.params.min_trigger_lamports {
            self.skip_min_trigger.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let pair = match self.pairs.get(&sig.pool) {
            Some(p) => p.clone(),
            None => {
                self.skip_no_pair.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };

        // Cooldown per pool.
        if let Some(prev) = self.last_fired.get(&sig.pool) {
            if prev.elapsed() < Duration::from_millis(self.params.cooldown_ms) {
                self.skip_cooldown.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }

        self.evaluated.fetch_add(1, Ordering::Relaxed);

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
        let pump_after = if sig.is_buy {
            pump_now.after_observed_buy(sig.base_amount)
        } else {
            pump_now.after_observed_sell(sig.base_amount)
        };

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

        let required_extra = self.params.tip_lamports + self.params.network_fee_lamports;

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

        // Detailed per-evaluation trace (only a few fire per 30s) so we can see
        // exactly where the math lands: direction, predicted gap, liquidity, and
        // net at several candidate sizes (None = swap infeasible at that size).
        let pump_now_price = if pump_now.base_reserve > 0 {
            pump_now.quote_reserve as f64 / pump_now.base_reserve as f64
        } else {
            0.0
        };
        let sample = |x: u64| -> Option<i64> {
            eval(x).map(|o| o as i64 - x as i64 - required_extra as i64)
        };
        info!(
            token = %pair.token_mint,
            buy = if buy_on == BuyOn::Pump { "Pump" } else { "Meteora" },
            gap_pct = (pump_price - met_price) / met_price * 100.0,
            pump_now_price,
            pump_after_price = pump_price,
            met_price,
            met_sqrt = meteora.sqrt_price,
            met_liq = meteora.liquidity,
            met_wsol_depth = meteora.wsol_reserve(token_is_a),
            buy_wsol_reserve,
            hi,
            opt_x,
            opt_net = opt_net as i64,
            net_1k = ?sample(1_000),
            net_10k = ?sample(10_000),
            net_100k = ?sample(100_000),
            net_500k = ?sample(500_000),
            "eval-detail"
        );

        if opt_net <= 0 {
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
        let floor = best_x + required_extra;
        if best_out <= floor {
            self.not_profitable.fetch_add(1, Ordering::Relaxed);
            return; // not profitable after fixed costs
        }
        let net = best_out - floor;

        // Plausibility guard: a real cross-pool gap is small. A predicted net
        // above `max_profit_fraction` of the input is always a dead-pool
        // mispricing — refuse to send garbage.
        if net as f64 > best_x as f64 * self.params.max_profit_fraction {
            self.skip_implausible.fetch_add(1, Ordering::Relaxed);
            debug!(
                pool = %sig.pool, input = best_x, net, "skip implausible profit (mispriced pool)"
            );
            return;
        }
        self.profitable.fetch_add(1, Ordering::Relaxed);

        let (buy_kind, sell_kind) = match buy_on {
            BuyOn::Pump => (DexKind::PumpFunAmm, DexKind::MeteoraDammV2),
            BuyOn::Meteora => (DexKind::MeteoraDammV2, DexKind::PumpFunAmm),
        };
        info!(
            pool = %sig.pool,
            token = %pair.token_mint,
            buy = ?buy_kind_label(buy_kind),
            input = best_x,
            predicted_out = best_out,
            net_lamports = net,
            "shred-arb opportunity"
        );

        self.last_fired.insert(sig.pool, Instant::now());
        self.execute(&pair, buy_kind, sell_kind, best_x, floor).await;
    }

    async fn execute(
        &self,
        pair: &ArbPair,
        buy_kind: DexKind,
        sell_kind: DexKind,
        amount_in: u64,
        floor: u64,
    ) {
        let token = pair.token_mint.to_string();

        // Leg 1: forced buy on `buy_kind` (WSOL → token).
        let q1 = match self
            .metis
            .get_quote_forced(WSOL_MINT, &token, amount_in, buy_kind.metis_label())
            .await
        {
            Ok(q) => q,
            Err(e) => {
                warn!(error = %e, "forced buy quote failed");
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
            .get_quote_forced(&token, WSOL_MINT, token_amt, sell_kind.metis_label())
            .await
        {
            Ok(q) => q,
            Err(e) => {
                warn!(error = %e, "forced sell quote failed");
                return;
            }
        };

        // Override the on-chain floor to input + tip + fee regardless of what
        // Metis quoted (our data is ahead of Metis).
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

        // Rate-limit: prefer REST, fall back to gRPC.
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

        let recent_blockhash = self.blockhash_cache.get();
        let keypair = self.trading_keypair.clone();
        let alt = self.alt_cache.clone();
        let rpc = self.rpc_client.clone();
        let tip = self.params.tip_lamports;
        let cu = self.params.cu_limit;

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
                     RESULT  : profitable={} not_profitable={} sent={} best_net_lamports_window={} | pool_slot={}",
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
                if self.pairs.is_empty() {
                    eprintln!("  SNAPSHOT: no pairs loaded from mix.json — nothing to trade!");
                }
                for pair in self.pairs.values().take(3) {
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
