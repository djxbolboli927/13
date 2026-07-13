use anyhow::Result;
use futures::stream::{self, StreamExt};
use solana_client::rpc_client::RpcClient;
use solana_sdk::signature::Keypair;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::account_cache::AccountCache;
use crate::alt_cache::AltCache;
use crate::blockhash_cache::BlockhashCache;
use crate::config::Config;
use crate::jito::JitoClient;
use crate::jito_grpc::JitoGrpcClient;
use crate::litesvm_sim::SimulatorPool;
use crate::metis::{MetisClient, QuoteResponse, SwapInstructionsResponse};
use crate::metrics::Metrics;
use crate::program_registry::{FORBIDDEN_DEX_LABELS, FORBIDDEN_DEX_PROGRAM_IDS, PMM_PROGRAM_IDS};
use crate::rate_limiter::RateLimiter;
use crate::template_cache::{self, TemplateStore};
use crate::token_metrics::TokenMetrics;
use crate::tokens::WSOL_MINT;
use crate::transaction;

const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;
const JITO_TIP_LAMPORTS: u64 = 1_600;
const NETWORK_FEE_LAMPORTS: u64 = 5_000;

const RATE_RETRY_BACKOFF_MS: u64 = 20;

// ─── Route helpers ───────────────────────────────────────────────────────────

fn route_uses_forbidden_dex(quote: &QuoteResponse) -> bool {
    let arr = match quote.route_plan.as_array() {
        Some(a) => a,
        None => return false,
    };
    for hop in arr {
        let swap_info = match hop.get("swapInfo") {
            Some(s) => s,
            None => continue,
        };
        if let Some(label) = swap_info.get("label").and_then(|v| v.as_str()) {
            for banned in FORBIDDEN_DEX_LABELS {
                if label.eq_ignore_ascii_case(banned)
                    || label.to_ascii_lowercase().contains(&banned.to_ascii_lowercase())
                {
                    return true;
                }
            }
        }
        if let Some(obj) = swap_info.as_object() {
            for v in obj.values() {
                if let Some(s) = v.as_str() {
                    if FORBIDDEN_DEX_PROGRAM_IDS.iter().any(|p| *p == s) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

fn route_uses_pmm(quote: &QuoteResponse) -> bool {
    let arr = match quote.route_plan.as_array() {
        Some(a) => a,
        None => return false,
    };
    for hop in arr {
        if let Some(swap_info) = hop.get("swapInfo").and_then(|s| s.as_object()) {
            for v in swap_info.values() {
                if let Some(s) = v.as_str() {
                    if PMM_PROGRAM_IDS.iter().any(|p| *p == s) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// Returns true if quote1 and quote2 share at least one pool (ammKey).
/// A round-trip through the same pool always loses money (pays fee twice).
fn routes_share_pool(q1: &QuoteResponse, q2: &QuoteResponse) -> bool {
    let keys1: Vec<&str> = q1
        .route_plan
        .as_array()
        .map(|hops| {
            hops.iter()
                .filter_map(|h| h.get("swapInfo")?.get("ammKey")?.as_str())
                .collect()
        })
        .unwrap_or_default();
    if keys1.is_empty() {
        return false;
    }
    let arr2 = match q2.route_plan.as_array() {
        Some(a) => a,
        None => return false,
    };
    for hop in arr2 {
        if let Some(key) = hop.get("swapInfo").and_then(|s| s.get("ammKey")).and_then(|v| v.as_str()) {
            if keys1.contains(&key) {
                return true;
            }
        }
    }
    false
}

fn lookup_cu_limit(hop_count: usize, cu_limits: &[u32]) -> u32 {
    if cu_limits.is_empty() {
        return 200_000;
    }
    let index = hop_count.saturating_sub(2);
    cu_limits[index.min(cu_limits.len() - 1)]
}

// ─── Stage 1 output ──────────────────────────────────────────────────────────

struct QuotePair {
    token_mint: String,
    amount: u64,
    output_wsol: u64,
    net_profit: i64,
    quote1: QuoteResponse,
    quote2: QuoteResponse,
    hop_count: usize,
    only_direct: bool,
    #[allow(dead_code)]
    is_pmm: bool,
}

// ─── LIFO queue item ──────────────────────────────────────────────────────────

struct ReadyInstruction {
    swap_ixs: SwapInstructionsResponse,
    hop_count: usize,
    arrived_at: Instant,
    waited_for_slot: bool,
}

// ─── Shared context ───────────────────────────────────────────────────────────

pub struct CalcCtx {
    pub metis: Arc<MetisClient>,
    pub blockhash_cache: Arc<BlockhashCache>,
    pub trading_keypair: Arc<Keypair>,
    pub rpc_client: Arc<RpcClient>,
    pub alt_cache: AltCache,
    pub jito: Arc<JitoClient>,
    pub jito_grpc: Option<Arc<JitoGrpcClient>>,
    pub jito_limiter: Arc<Mutex<RateLimiter>>,
    pub jito_grpc_limiter: Option<Arc<Mutex<RateLimiter>>>,
    pub cu_limits: Vec<u32>,
    pub user_pubkey: String,
    #[allow(dead_code)]
    pub sim_cache: Option<Arc<AccountCache>>,
    #[allow(dead_code)]
    pub sim_pool: Option<Arc<SimulatorPool>>,
    pub template_store: Arc<TemplateStore>,
}

// ─── Pipeline handle ──────────────────────────────────────────────────────────

pub struct Pipeline {
    lifo: Arc<Mutex<Vec<ReadyInstruction>>>,
    lifo_sem: Arc<tokio::sync::Semaphore>,
}

// ─── Stage 1: Quote scanner ───────────────────────────────────────────────────

/// One quote check for a single route mode (free or direct-only).
/// Each call makes 2 sequential HTTP requests (q1 then q2).
/// Free and direct entries are separate items in all_pairs so they never
/// block each other — a direct-route timeout does not delay the free-route
/// check for the same token.
///
/// Both free (only_direct=false) and direct (only_direct=true) routes are
/// scanned for every token.  Free routes may return multi-hop paths
/// (hop_count > 2) which are also sent to /swap-instructions when profitable.
/// Direct routes should always be 2-hop (1 hop per leg).
async fn quote_check(
    metis: &MetisClient,
    token_mint: &str,
    amount: u64,
    only_direct: bool,
    min_profit_lamports: u64,
    metrics: &Metrics,
    token_metrics: &TokenMetrics,
) -> Option<QuotePair> {
    metrics.metis_req_sent.fetch_add(2, Ordering::Relaxed);
    let ts = token_metrics.get(token_mint);
    if let Some(ts) = ts {
        ts.q_sent.fetch_add(2, Ordering::Relaxed);
    }

    let quote1 = match metis.get_quote(WSOL_MINT, token_mint, amount, only_direct).await {
        Ok(q) => q,
        Err(_) => {
            if let Some(ts) = ts {
                ts.route_fail.fetch_add(1, Ordering::Relaxed);
            }
            return None;
        }
    };

    let token_amount: u64 = match quote1.out_amount.parse::<u64>().ok().filter(|&v| v > 0) {
        Some(v) => v,
        None => {
            if let Some(ts) = ts {
                ts.route_fail.fetch_add(1, Ordering::Relaxed);
            }
            return None;
        }
    };

    let quote2 = match metis.get_quote(token_mint, WSOL_MINT, token_amount, only_direct).await {
        Ok(q) => q,
        Err(_) => {
            if let Some(ts) = ts {
                ts.route_fail.fetch_add(1, Ordering::Relaxed);
            }
            return None;
        }
    };

    let output_wsol: u64 = quote2.out_amount.parse().unwrap_or(0);
    metrics.metis_resp_total.fetch_add(1, Ordering::Relaxed);
    if let Some(ts) = ts {
        ts.route_ok.fetch_add(1, Ordering::Relaxed);
    }

    // min_profit_lamports is GROSS profit at quote stage (before fees).
    let stage1_threshold = amount.saturating_add(min_profit_lamports);
    if output_wsol <= stage1_threshold {
        if let Some(ts) = ts {
            ts.not_profitable.fetch_add(1, Ordering::Relaxed);
        }
        return None;
    }

    if route_uses_forbidden_dex(&quote1) || route_uses_forbidden_dex(&quote2) {
        if let Some(ts) = ts {
            ts.not_profitable.fetch_add(1, Ordering::Relaxed);
        }
        return None;
    }

    // Same-pool guard: round-tripping through the same AMM pool pays the fee
    // twice and always loses. These look profitable only due to Metis's
    // optimistic slippage=0 simulation; they revert on-chain every time.
    if routes_share_pool(&quote1, &quote2) {
        metrics.dropped_same_pool.fetch_add(1, Ordering::Relaxed);
        if let Some(ts) = ts {
            ts.not_profitable.fetch_add(1, Ordering::Relaxed);
        }
        return None;
    }

    if let Some(ts) = ts {
        ts.profitable.fetch_add(1, Ordering::Relaxed);
    }

    let is_pmm = route_uses_pmm(&quote1) || route_uses_pmm(&quote2);
    let hop_count = {
        let n1 = quote1.route_plan.as_array().map(|a| a.len()).unwrap_or(1);
        let n2 = quote2.route_plan.as_array().map(|a| a.len()).unwrap_or(1);
        n1 + n2
    };

    metrics.metis_resp_ok.fetch_add(1, Ordering::Relaxed);

    let on_chain_floor = amount
        .saturating_add(JITO_TIP_LAMPORTS)
        .saturating_add(NETWORK_FEE_LAMPORTS);
    let net_profit = output_wsol as i64 - on_chain_floor as i64;
    Some(QuotePair {
        token_mint: token_mint.to_string(),
        amount,
        output_wsol,
        net_profit,
        quote1,
        quote2,
        hop_count,
        only_direct,
        is_pmm,
    })
}

// ─── Worker pool ──────────────────────────────────────────────────────────────

pub fn spawn_workers(
    ctx: Arc<CalcCtx>,
    metrics: Arc<Metrics>,
    worker_count: usize,
    queue_max_age_ms: u64,
) -> Pipeline {
    let lifo: Arc<Mutex<Vec<ReadyInstruction>>> = Arc::new(Mutex::new(Vec::new()));
    let lifo_sem = Arc::new(tokio::sync::Semaphore::new(0));

    for _ in 0..worker_count {
        let lifo_c = lifo.clone();
        let sem_c = lifo_sem.clone();
        let ctx_c = ctx.clone();
        let met_c = metrics.clone();
        tokio::spawn(async move {
            loop {
                sem_c.acquire().await.unwrap().forget();

                let item = lifo_c.lock().unwrap().pop();
                let mut item = match item {
                    Some(i) => {
                        met_c.queue_depth.fetch_sub(1, Ordering::Relaxed);
                        i
                    }
                    None => continue,
                };

                if item.arrived_at.elapsed().as_millis() as u64 > queue_max_age_ms {
                    met_c.dropped_stale.fetch_add(1, Ordering::Relaxed);
                    met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                let use_grpc = if ctx_c.jito_limiter.lock().unwrap().try_acquire() {
                    false
                } else if ctx_c
                    .jito_grpc_limiter
                    .as_ref()
                    .map(|gl| gl.lock().unwrap().try_acquire())
                    .unwrap_or(false)
                {
                    true
                } else {
                    if !item.waited_for_slot {
                        item.waited_for_slot = true;
                        met_c.rate_requeued.fetch_add(1, Ordering::Relaxed);
                    }
                    lifo_c.lock().unwrap().push(item);
                    met_c.queue_depth.fetch_add(1, Ordering::Relaxed);
                    sem_c.add_permits(1);
                    tokio::time::sleep(std::time::Duration::from_millis(RATE_RETRY_BACKOFF_MS))
                        .await;
                    continue;
                };

                let cu_limit = lookup_cu_limit(item.hop_count, &ctx_c.cu_limits);
                let recent_blockhash = ctx_c.blockhash_cache.get();
                let keypair = ctx_c.trading_keypair.clone();
                let alt = ctx_c.alt_cache.clone();
                let rpc = ctx_c.rpc_client.clone();
                let swap_ixs = item.swap_ixs;

                let tx = match tokio::task::spawn_blocking(move || {
                    transaction::build_arb_transaction(
                        &swap_ixs,
                        &keypair,
                        JITO_TIP_LAMPORTS,
                        cu_limit,
                        recent_blockhash,
                        &alt,
                        &rpc,
                    )
                })
                .await
                {
                    Ok(Ok(tx)) => tx,
                    _ => {
                        met_c.tx_build_failed.fetch_add(1, Ordering::Relaxed);
                        met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };

                // Solana hard limit: a tx may lock at most 64 distinct accounts
                // (static keys + every ALT-loaded account). Multi-hop circular
                // routes routinely exceed this; the block engine rejects them
                // with "too many account locks". Drop them here so we don't
                // waste a Jito rate-limit slot on a guaranteed 400.
                if transaction::account_lock_count(&tx) > 64 {
                    met_c.dropped_account_locks.fetch_add(1, Ordering::Relaxed);
                    met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                match bincode::serialize(&tx) {
                    Ok(bytes) if bytes.len() > 1232 => {
                        met_c.tx_too_large.fetch_add(1, Ordering::Relaxed);
                        met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    Err(_) => {
                        met_c.tx_build_failed.fetch_add(1, Ordering::Relaxed);
                        met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    Ok(_) => {}
                }

                met_c.calc_done.fetch_add(1, Ordering::Relaxed);

                let result = if use_grpc {
                    match &ctx_c.jito_grpc {
                        Some(grpc) => grpc.send_bundle(&tx).await,
                        None => ctx_c.jito.send_bundle(&tx).await,
                    }
                } else {
                    ctx_c.jito.send_bundle(&tx).await
                };

                match result {
                    Ok(_) => {
                        met_c.jito_sent.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        met_c.jito_send_failed.fetch_add(1, Ordering::Relaxed);
                        met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
    }

    Pipeline { lifo, lifo_sem }
}

// ─── Queue helper ─────────────────────────────────────────────────────────────

fn push_to_queue(
    swap_ixs: SwapInstructionsResponse,
    hop_count: usize,
    pipeline: &Pipeline,
    metrics: &Metrics,
) {
    let item = ReadyInstruction {
        swap_ixs,
        hop_count,
        arrived_at: Instant::now(),
        waited_for_slot: false,
    };
    pipeline.lifo.lock().unwrap().push(item);
    metrics.queue_in.fetch_add(1, Ordering::Relaxed);
    metrics.queue_depth.fetch_add(1, Ordering::Relaxed);
    pipeline.lifo_sem.add_permits(1);
}

// ─── Main scan entry ─────────────────────────────────────────────────────────

/// One scan cycle over all (token × amount × route_mode) triples.
///
/// For each profitable 2-hop pair the three-tier flow is:
///
///   Tier 1 — RouteTemplate hit:
///     Serve from RAM using a cached SwapInstructionsResponse.
///     If amounts differ from the template, patch in_amount and
///     quoted_out_amount in the Borsh data at pre-discovered byte offsets.
///     Skips /swap-instructions entirely on a hit.
///
///   Tier 2 — HopTemplate check (metrics only, no composer yet):
///     If all hops in the route have been seen before, record the metric.
///     Still falls through to Metis until a composer is implemented.
///
///   Tier 3 — Metis fallback:
///     Call /swap-instructions as before.
///     On success: save RouteTemplate + record hops (if save_new=true).
pub async fn scan_all_tokens(
    token_mints: &[String],
    config: &Config,
    ctx: &Arc<CalcCtx>,
    pipeline: &Pipeline,
    metrics: &Arc<Metrics>,
    token_metrics: &Arc<TokenMetrics>,
) -> Result<()> {
    let min_lamports = (config.trading.min_amount_sol * LAMPORTS_PER_SOL) as u64;
    let max_lamports = (config.trading.max_amount_sol * LAMPORTS_PER_SOL) as u64;
    let step_lamports = (config.trading.step_sol * LAMPORTS_PER_SOL) as u64;
    let min_profit_lamports = config.trading.min_profit_lamports;

    // Each (amount, token) generates two independent entries: free routes and
    // direct-only routes. They run concurrently so a slow/timing-out direct
    // request never blocks the free-route check for the same token.
    // Note: free routes (only_direct=false) often return multi-hop paths that
    // are filtered later by the hop_count==2 gate, but direct routes that
    // happen to share the same 2-hop structure ARE also included.
    let all_pairs: Vec<(u64, String, bool)> = {
        let mut pairs = Vec::new();
        let mut amount = min_lamports;
        while amount <= max_lamports {
            for token_mint in token_mints {
                pairs.push((amount, token_mint.clone(), false)); // free routes
                pairs.push((amount, token_mint.clone(), true));  // direct routes only
            }
            amount += step_lamports;
        }
        pairs
    };
    let max_concurrent = config
        .performance
        .max_concurrent_quotes
        .max(1)
        .min(all_pairs.len().max(1));

    let metis_ref: &MetisClient = &ctx.metis;
    let met_ref: &Metrics = metrics;
    let tok_met_ref: &TokenMetrics = token_metrics;

    let mut opps = stream::iter(all_pairs)
        .map(move |(amt, tok, direct)| async move {
            quote_check(metis_ref, &tok, amt, direct, min_profit_lamports, met_ref, tok_met_ref)
                .await
        })
        .buffer_unordered(max_concurrent);

    while let Some(result) = opps.next().await {
        let pair = match result {
            Some(p) => p,
            None => continue,
        };

        let on_chain_floor = pair.amount + JITO_TIP_LAMPORTS + NETWORK_FEE_LAMPORTS;
        tracing::debug!(
            token = %pair.token_mint,
            amount = pair.amount,
            output = pair.output_wsol,
            quoted_edge = pair.output_wsol as i64 - pair.amount as i64,
            floor_edge = pair.net_profit,
            "send_candidate"
        );

        // Direct routes (only_direct=true) must be exactly 2-hop.
        // Free routes (only_direct=false) may be multi-hop — all are forwarded.
        if pair.only_direct && pair.hop_count != 2 {
            metrics.dropped_multi_hop.fetch_add(1, Ordering::Relaxed);
            metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
            continue;
        }

        let merged = match MetisClient::merge_quotes(&pair.quote1, &pair.quote2, on_chain_floor) {
            Ok(m) => m,
            Err(_) => {
                metrics.dropped_merge_fail.fetch_add(1, Ordering::Relaxed);
                metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };

        let hop_count = pair.hop_count;
        let amount = pair.amount;
        let sig = template_cache::route_sig(&merged.route_plan);
        let tc = &config.template_cache;
        let context_slot = merged.context_slot;

        // ── Tier 1: RouteTemplate hit ─────────────────────────────────────────
        // RouteTemplate is keyed by route_sig (NO amount). When the same route
        // structure is requested with a different amount, the Borsh instruction
        // data is patched at pre-discovered byte offsets.
        if tc.serve_route {
            if let Some(tmpl) = ctx.template_store.get_route(sig) {
                if let Some(patched) = template_cache::serve_route(&tmpl, amount, on_chain_floor) {
                    metrics.route_template_hit.fetch_add(1, Ordering::Relaxed);
                    metrics.ix_from_ram.fetch_add(1, Ordering::Relaxed);
                    metrics.swap_ix_ok.fetch_add(1, Ordering::Relaxed);
                    ctx.template_store.record_route_hit(sig);
                    push_to_queue(patched, hop_count, pipeline, metrics);
                    continue;
                }
                // Patching failed (no offsets discovered): fall through to Tier-2.
            }
        }

        // ── Tier 2: HopTemplate check + hop-pair secondary route lookup ──────
        // When all hops are known, try to find a RouteTemplate via the
        // hop-pair index. This serves from RAM even when the primary route_sig
        // doesn't match exactly (e.g. an extra unstable field in swapInfo).
        {
            let (all_hit, missing) = ctx.template_store.check_hops(&merged.route_plan);
            if all_hit {
                metrics.hop_template_all_hit.fetch_add(1, Ordering::Relaxed);
                if tc.serve_route {
                    if let Some(tmpl) =
                        ctx.template_store.get_route_for_hops(&merged.route_plan)
                    {
                        if let Some(patched) =
                            template_cache::serve_route(&tmpl, amount, on_chain_floor)
                        {
                            metrics.route_template_hit.fetch_add(1, Ordering::Relaxed);
                            metrics.ix_from_ram.fetch_add(1, Ordering::Relaxed);
                            metrics.swap_ix_ok.fetch_add(1, Ordering::Relaxed);
                            ctx.template_store.record_route_hit(tmpl.route_signature);
                            push_to_queue(patched, hop_count, pipeline, metrics);
                            continue;
                        }
                    }
                }
            } else if missing > 0 {
                metrics.hop_template_missing.fetch_add(missing as u64, Ordering::Relaxed);
            }
        }

        // ── Tier 3: Metis fallback ────────────────────────────────────────────
        if !tc.serve_from_metis {
            metrics.dropped_no_serve.fetch_add(1, Ordering::Relaxed);
            metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
            continue;
        }

        let ctx_c = ctx.clone();
        let met_c = metrics.clone();
        let lifo_c = pipeline.lifo.clone();
        let sem_c = pipeline.lifo_sem.clone();
        let save_new = tc.save_new;

        metrics.metis_req_sent.fetch_add(1, Ordering::Relaxed);

        tokio::spawn(async move {
            let t = std::time::Instant::now();
            let result =
                ctx_c.metis.get_swap_instructions(&ctx_c.user_pubkey, &merged, false).await;
            let fetch_ms = t.elapsed().as_millis() as u64;

            let swap_ixs = match result {
                Ok(s) => s,
                Err(e) => {
                    met_c.swap_ix_failed.fetch_add(1, Ordering::Relaxed);
                    met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                    match e {
                        crate::metis::SwapIxError::Timeout => {
                            met_c.swap_ix_timeout.fetch_add(1, Ordering::Relaxed)
                        }
                        crate::metis::SwapIxError::Http(_) => {
                            met_c.swap_ix_http.fetch_add(1, Ordering::Relaxed)
                        }
                        crate::metis::SwapIxError::Network => {
                            met_c.swap_ix_network.fetch_add(1, Ordering::Relaxed)
                        }
                        crate::metis::SwapIxError::Parse => {
                            met_c.swap_ix_parse.fetch_add(1, Ordering::Relaxed)
                        }
                    };
                    return;
                }
            };

            met_c.metis_fetch_ms_total.fetch_add(fetch_ms, Ordering::Relaxed);
            met_c.metis_fetch_samples.fetch_add(1, Ordering::Relaxed);
            met_c.swap_ix_ok.fetch_add(1, Ordering::Relaxed);
            met_c.ix_from_metis.fetch_add(1, Ordering::Relaxed);

            if save_new {
                // Insert RouteTemplate (amount-independent key, patches amounts
                // for future hits with different amounts). Also indexes by
                // hop-pair for Tier-2 fallback lookups.
                ctx_c.template_store.insert_route(
                    sig,
                    swap_ixs.clone(),
                    amount,
                    on_chain_floor,
                    &merged.route_plan,
                );
                // Record each hop (amount-independent: pool + direction only).
                ctx_c.template_store.record_hops(&merged.route_plan, context_slot);
            }

            let item = ReadyInstruction {
                swap_ixs,
                hop_count,
                arrived_at: std::time::Instant::now(),
                waited_for_slot: false,
            };
            lifo_c.lock().unwrap().push(item);
            met_c.queue_in.fetch_add(1, Ordering::Relaxed);
            met_c.queue_depth.fetch_add(1, Ordering::Relaxed);
            sem_c.add_permits(1);
        });
    }

    Ok(())
}
