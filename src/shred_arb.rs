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
use std::sync::atomic::{AtomicU64, Ordering};
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
    last_fired: dashmap::DashMap<solana_sdk::pubkey::Pubkey, Instant>,
    pub sent: AtomicU64,
    pub evaluated: AtomicU64,
    pub profitable: AtomicU64,
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
            last_fired: dashmap::DashMap::new(),
            sent: AtomicU64::new(0),
            evaluated: AtomicU64::new(0),
            profitable: AtomicU64::new(0),
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
        if sig.quote_amount < self.params.min_trigger_lamports {
            return;
        }
        let pair = match self.pairs.get(&sig.pool) {
            Some(p) => p.clone(),
            None => return,
        };

        // Cooldown per pool.
        if let Some(prev) = self.last_fired.get(&sig.pool) {
            if prev.elapsed() < Duration::from_millis(self.params.cooldown_ms) {
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
                debug!(pool = %pair.meteora.pool, "meteora state not cached yet");
                return;
            }
        };

        // 3) Direction: compare raw price (WSOL-raw per token-raw) on both.
        let pump_price = if pump_after.base_reserve == 0 {
            return;
        } else {
            pump_after.quote_reserve as f64 / pump_after.base_reserve as f64
        };
        let met_price = meteora.token_price_in_sol(pair.meteora.token_is_a, 0, 0);
        if met_price <= 0.0 {
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
        let (best_x, best_out) = ternary_search_best(
            self.params.min_amount_lamports,
            self.params.max_amount_lamports,
            required_extra,
            &eval,
        );

        let best_out = match best_out {
            Some(o) => o,
            None => return,
        };
        let floor = best_x + required_extra;
        if best_out <= floor {
            return; // not profitable after fixed costs
        }
        self.profitable.fetch_add(1, Ordering::Relaxed);
        let net = best_out - floor;

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
            let mut ticker = tokio::time::interval(Duration::from_secs(10));
            let fallback_fee = self.params.meteora_fee_bps.saturating_mul(100_000);
            loop {
                ticker.tick().await;
                info!(
                    evaluated = self.evaluated.load(Ordering::Relaxed),
                    profitable = self.profitable.load(Ordering::Relaxed),
                    sent = self.sent.load(Ordering::Relaxed),
                    pool_slot = self.pool_state.slot(),
                    "shred-arb stats"
                );
                // Decoded-state snapshot for calibration: compare these prices
                // against a live Metis quote for the same pool/size.
                for pair in self.pairs.values().take(3) {
                    let met = self
                        .pool_state
                        .meteora_pool(&pair.meteora.pool, fallback_fee);
                    let pump = self
                        .pool_state
                        .pump_pool(&pair.pump.token_vault(), &pair.pump.wsol_vault());
                    if let (Some(m), Some(p)) = (met, pump) {
                        let met_price = m.token_price_in_sol(pair.meteora.token_is_a, 0, 0);
                        let pump_price = if p.base_reserve == 0 {
                            0.0
                        } else {
                            p.quote_reserve as f64 / p.base_reserve as f64
                        };
                        info!(
                            token = %pair.token_mint,
                            meteora_sqrt_price = m.sqrt_price,
                            meteora_liquidity = m.liquidity,
                            meteora_fee_num = m.fee_numerator,
                            meteora_price = met_price,
                            pump_base = p.base_reserve,
                            pump_quote = p.quote_reserve,
                            pump_price = pump_price,
                            "pool snapshot"
                        );
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

/// Unimodal ternary search for the input that maximizes `eval(x) - x`.
/// Returns the best `(x, eval(x))`. `required_extra` is only used to bias the
/// objective toward net profit (it is a constant, so it doesn't change the
/// argmax, but keeps the comparison in profit space).
fn ternary_search_best<F: Fn(u64) -> Option<u64>>(
    lo: u64,
    hi: u64,
    required_extra: u64,
    eval: &F,
) -> (u64, Option<u64>) {
    if hi <= lo {
        return (lo, eval(lo));
    }
    // Objective in signed profit space.
    let net = |x: u64| -> i128 {
        match eval(x) {
            Some(out) => out as i128 - x as i128 - required_extra as i128,
            None => i128::MIN,
        }
    };
    let mut a = lo;
    let mut b = hi;
    for _ in 0..64 {
        if b - a < 3 {
            break;
        }
        let m1 = a + (b - a) / 3;
        let m2 = b - (b - a) / 3;
        if net(m1) < net(m2) {
            a = m1;
        } else {
            b = m2;
        }
    }
    // Scan the small remaining window for the exact best.
    let mut best_x = a;
    let mut best_net = net(a);
    let mut x = a;
    while x <= b {
        let n = net(x);
        if n > best_net {
            best_net = n;
            best_x = x;
        }
        x += 1;
    }
    (best_x, eval(best_x))
}
