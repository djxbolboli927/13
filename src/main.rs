#[allow(dead_code)]
mod account_cache;
mod alt_cache;
mod arbitrage;
mod blockhash_cache;
mod config;
mod dex_accounts;
#[allow(dead_code)]
mod dex_ids;
mod jito;
#[allow(dead_code)]
mod jito_grpc;
#[allow(dead_code)]
mod litesvm_sim;
mod mathutil;
mod metis;
#[allow(dead_code)]
mod meteora_math;
mod metrics;
#[allow(dead_code)]
mod pool_registry;
#[allow(dead_code)]
mod pool_state;
mod program_registry;
#[allow(dead_code)]
mod pumpfun_math;
mod rate_limiter;
mod shred_arb;
#[allow(dead_code)]
mod shred_proxy;
mod shred_stream;
mod template_cache;
mod token_metrics;
mod tokens;
mod transaction;
mod wallet;

use anyhow::Result;
use solana_client::rpc_client::RpcClient;
use solana_sdk::signer::Signer;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use tracing::error;

use alt_cache::AltCache;
use blockhash_cache::BlockhashCache;
use rate_limiter::RateLimiter;

fn main() -> Result<()> {
    let log_filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "error".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            format!("{log_filter},hyper_util=error,hyper=error,reqwest=error,h2=error,tonic=error"),
        ))
        .init();

    let config = config::Config::load("config.toml")?;

    let worker_threads = config.performance.threads.max(1);
    let pinned_cores: Vec<usize> = config.performance.bot_cpu_cores.clone();
    let available_cores = core_affinity::get_core_ids().unwrap_or_default();
    let next_worker = Arc::new(AtomicUsize::new(0));

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(worker_threads).enable_all();
    builder.thread_name("arb-worker");

    if !pinned_cores.is_empty() {
        let cores = pinned_cores.clone();
        let available = available_cores.clone();
        let counter = next_worker.clone();
        builder.on_thread_start(move || {
            let idx = counter.fetch_add(1, Ordering::SeqCst);
            let target = cores[idx % cores.len()];
            if let Some(core_id) = available.iter().find(|c| c.id == target) {
                core_affinity::set_for_current(*core_id);
            }
        });
    }

    let runtime = builder.build()?;
    runtime.block_on(async_main(config))
}

async fn async_main(config: config::Config) -> Result<()> {
    let token_mints = tokens::load_tokens(&config.trading.tokens_file)?;

    let trading_keypair = Arc::new(wallet::read_keypair(&config.jito.trading_keypair)?);

    let rpc_client = Arc::new(RpcClient::new(config.rpc.url.clone()));

    let wsol_mint = solana_sdk::pubkey::Pubkey::from_str_const(tokens::WSOL_MINT);
    let wsol_ata = spl_associated_token_account::get_associated_token_address(
        &trading_keypair.pubkey(),
        &wsol_mint,
    );

    // ── Template cache: load hop templates from disk and start periodic flush ─
    let template_store = template_cache::TemplateStore::new();
    if config.template_cache.save_new || config.template_cache.serve_route {
        let hops_loaded = template_store.load_from_disk();
        let routes_loaded = template_store.load_routes_from_disk();
        eprintln!(
            "[template] loaded {hops_loaded} hop templates and {routes_loaded} route templates from /root/c/cache/"
        );
        template_store.spawn_flush_task(60);
    }

    let metrics = metrics::Metrics::new();
    let token_metrics = token_metrics::TokenMetrics::new(&token_mints);
    // The legacy circular-scan reporters only make sense when that scanner runs.
    // With it disabled they print an all-zero funnel that is easily mistaken for
    // the new strategy's Metis activity — so gate them.
    if config.scanner.enabled {
        metrics.spawn_reporter(config.performance.queue_max_age_ms, template_store.clone());
        token_metrics.spawn_reporter();
    }

    let blockhash_cache = Arc::new(BlockhashCache::new(rpc_client.clone()));

    let tip_pubkeys = transaction::jito_tip_pubkeys();
    let alt_cache = AltCache::new(tip_pubkeys);

    let metis = Arc::new(metis::MetisClient::new(
        &config.metis.url,
        config.performance.quote_timeout_ms,
    ));

    let jito_client = Arc::new(jito::JitoClient::new(&config.jito.urls, &config.jito.uuid));

    let jito_limiter = Arc::new(Mutex::new(
        RateLimiter::new(config.jito.max_bundles_per_second),
    ));

    let (jito_grpc_client, jito_grpc_limiter) = if config.jito_grpc.enabled {
        match jito_grpc::JitoGrpcClient::new(
            &config.jito_grpc.endpoints,
            &config.jito_grpc.auth_keypair,
        )
        .await
        {
            Ok(client) => {
                let limiter = Arc::new(Mutex::new(RateLimiter::new(
                    config.jito_grpc.max_bundles_per_second,
                )));
                (Some(Arc::new(client)), Some(limiter))
            }
            Err(e) => {
                eprintln!("Jito gRPC init failed: {e} — continuing REST-only");
                (None, None)
            }
        }
    } else {
        (None, None)
    };

    let (sim_cache, sim_pool) = if config.simulation.enabled {
        let cache = account_cache::AccountCache::new(rpc_client.clone());

        if let Ok(s) = rpc_client.get_slot() {
            cache.seed_slot(s);
        }

        let dex_pools = dex_accounts::load(&config.simulation.dex_dir);
        let mut live_extra = vec![wsol_ata];
        live_extra.extend_from_slice(&dex_pools.subscribe_accounts);

        cache.spawn_subscription(
            config.yellowstone_grpc.endpoint.clone(),
            config.yellowstone_grpc.x_token.clone(),
            program_registry::all_program_ids(),
            live_extra,
        );

        let mut warm: Vec<solana_sdk::pubkey::Pubkey> = token_mints
            .iter()
            .filter_map(|s| solana_sdk::pubkey::Pubkey::try_from(s.as_str()).ok())
            .collect();
        warm.push(wsol_mint);
        warm.push(wsol_ata);
        warm.push(trading_keypair.pubkey());
        for mint_str in &token_mints {
            if let Ok(mint) = solana_sdk::pubkey::Pubkey::try_from(mint_str.as_str()) {
                let ata = spl_associated_token_account::get_associated_token_address(
                    &trading_keypair.pubkey(),
                    &mint,
                );
                warm.push(ata);
            }
        }
        warm.extend_from_slice(&dex_pools.all_accounts);
        cache.prefetch(&warm);

        let pool = litesvm_sim::SimulatorPool::new(
            config.simulation.workers,
            &config.simulation.so_dir,
            wsol_ata,
            config.simulation.fail_closed,
            cache.stream_slot(),
        )?;
        (Some(Arc::new(cache)), Some(Arc::new(pool)))
    } else {
        (None, None)
    };

    // ── Build shared CalcCtx ─────────────────────────────────────────────────
    let calc_ctx = Arc::new(arbitrage::CalcCtx {
        metis: metis.clone(),
        blockhash_cache: blockhash_cache.clone(),
        trading_keypair: trading_keypair.clone(),
        rpc_client: rpc_client.clone(),
        alt_cache: alt_cache.clone(),
        jito: jito_client.clone(),
        jito_grpc: jito_grpc_client.clone(),
        jito_limiter: jito_limiter.clone(),
        jito_grpc_limiter: jito_grpc_limiter.clone(),
        cu_limits: config.performance.cu_limits.clone(),
        user_pubkey: trading_keypair.pubkey().to_string(),
        sim_cache,
        sim_pool,
        template_store,
    });

    let worker_count = config.performance.calc_workers.max(1);
    let jito_capacity = config.jito.max_bundles_per_second as usize
        + jito_grpc_limiter
            .as_ref()
            .map(|_| config.jito_grpc.max_bundles_per_second as usize)
            .unwrap_or(0);
    let pipeline = arbitrage::spawn_workers(
        calc_ctx.clone(),
        metrics.clone(),
        worker_count,
        config.performance.queue_max_age_ms,
    );

    eprintln!(
        "scanner ready | tokens={} | pairs_per_scan={} | calc_workers={worker_count} | jito_capacity_per_sec={jito_capacity} | quote_concurrency={}",
        token_mints.len(),
        {
            let steps = ((config.trading.max_amount_sol - config.trading.min_amount_sol)
                / config.trading.step_sol) as usize
                + 1;
            steps * token_mints.len() * 2 // ×2: free + direct route per pair
        },
        config.performance.max_concurrent_quotes.max(1),
    );

    // ── ShredStream / Pump.fun ↔ Meteora arbitrage strategy ──────────────────
    if config.shred_arb.enabled {
        if let Err(e) = spawn_shred_arb(
            &config,
            metis.clone(),
            blockhash_cache.clone(),
            trading_keypair.clone(),
            rpc_client.clone(),
            alt_cache.clone(),
            jito_client.clone(),
            jito_grpc_client.clone(),
            jito_limiter.clone(),
            jito_grpc_limiter.clone(),
        ) {
            error!(error = %e, "failed to start shred-arb strategy");
        }
    }

    // ── Legacy circular scanner (gated) ──────────────────────────────────────
    if config.scanner.enabled {
        loop {
            if let Err(e) = arbitrage::scan_all_tokens(
                &token_mints,
                &config,
                &calc_ctx,
                &pipeline,
                &metrics,
                &token_metrics,
            )
            .await
            {
                error!(error = %e, "scan cycle error");
            }
        }
    } else {
        eprintln!("legacy circular scanner disabled (config.scanner.enabled=false)");
        // Keep the process alive so background strategies keep running.
        futures::future::pending::<()>().await;
        Ok(())
    }
}

/// Wire up and launch the ShredStream arbitrage strategy.
#[allow(clippy::too_many_arguments)]
fn spawn_shred_arb(
    config: &config::Config,
    metis: Arc<metis::MetisClient>,
    blockhash_cache: Arc<BlockhashCache>,
    trading_keypair: Arc<solana_sdk::signature::Keypair>,
    rpc_client: Arc<RpcClient>,
    alt_cache: AltCache,
    jito_client: Arc<jito::JitoClient>,
    jito_grpc_client: Option<Arc<jito_grpc::JitoGrpcClient>>,
    jito_limiter: Arc<Mutex<RateLimiter>>,
    jito_grpc_limiter: Option<Arc<Mutex<RateLimiter>>>,
) -> Result<()> {
    use std::collections::{HashMap, HashSet};

    let sa = &config.shred_arb;
    let pairs = pool_registry::load_pairs(&sa.mix_cache_path)?;
    if pairs.is_empty() {
        eprintln!("[shred-arb] no pairs in mix.json — strategy idle");
    }

    // Auto-launch the ShredStream proxy so the operator only starts the bot.
    if sa.proxy_autostart {
        let grpc_port = shred_proxy::parse_grpc_port(&sa.shredstream_endpoint, 9999);
        shred_proxy::spawn_supervised(shred_proxy::ProxyConfig {
            bin: sa.proxy_bin.clone(),
            block_engine_url: sa.block_engine_url.clone(),
            auth_keypair: sa.shred_keypair.clone(),
            desired_regions: sa.desired_regions.clone(),
            dest_ip_ports: sa.proxy_dest_ip_ports.clone(),
            src_bind_port: sa.proxy_src_bind_port,
            grpc_service_port: grpc_port,
            extra_args: sa.proxy_extra_args.clone(),
        });
    } else {
        eprintln!("[shred-arb] proxy_autostart=false — expecting an external shredstream proxy");
    }

    // Accounts to watch live: each Meteora pool + each Pump vault pair.
    let mut accounts: Vec<solana_sdk::pubkey::Pubkey> = Vec::new();
    for p in &pairs {
        accounts.push(p.meteora.pool);
        accounts.push(p.pump.token_vault());
        accounts.push(p.pump.wsol_vault());
    }
    accounts.sort_unstable();
    accounts.dedup();

    // Log the loaded pairs so mix.json parsing is verifiable at a glance.
    for p in &pairs {
        eprintln!(
            "[shred-arb] pair token={} | pump_pool={} vaults=({},{}) | meteora_pool={}",
            p.token_mint, p.pump.pool, p.pump.token_vault(), p.pump.wsol_vault(), p.meteora.pool,
        );
    }

    let pool_state = pool_state::PoolStateCache::new();
    if accounts.is_empty() {
        eprintln!("[shred-arb] WARNING: no accounts to watch (0 pairs) — pool-state stream skipped");
    } else {
        // Seed initial state via RPC so low-activity pools (e.g. a rarely-traded
        // Meteora pool) are present before their first live update.
        pool_state.prefetch(&rpc_client, &accounts);
        pool_state.spawn_subscription(
            sa.pool_state_endpoint.clone(),
            sa.pool_state_x_token.clone(),
            accounts,
        );
    }

    // Preload (unfiltered) ALT contents for each Pump pool so the ShredStream
    // consumer can resolve ALT-provided accounts without a hot-path RPC call.
    let mut alt_map: HashMap<solana_sdk::pubkey::Pubkey, Vec<solana_sdk::pubkey::Pubkey>> =
        HashMap::new();
    let mut target_pools: HashSet<solana_sdk::pubkey::Pubkey> = HashSet::new();
    for p in &pairs {
        target_pools.insert(p.pump.pool);
        if let Some(alt) = p.pump.alt {
            if alt_map.contains_key(&alt) {
                continue;
            }
            match rpc_client.get_account(&alt) {
                Ok(acct) => match transaction::deserialize_alt_addresses(&acct.data) {
                    Ok(addrs) => {
                        alt_map.insert(alt, addrs);
                    }
                    Err(e) => eprintln!("[shred-arb] bad ALT {alt}: {e}"),
                },
                Err(e) => eprintln!("[shred-arb] failed to fetch ALT {alt}: {e}"),
            }
        }
    }

    let lamports = |sol: f64| (sol * 1_000_000_000.0) as u64;
    let cu_limit = config.performance.cu_limits.first().copied().unwrap_or(200_000);
    let params = shred_arb::ArbParams {
        tip_lamports: sa.tip_lamports,
        network_fee_lamports: sa.network_fee_lamports,
        meteora_fee_bps: sa.meteora_fee_bps,
        min_trigger_lamports: lamports(sa.min_trigger_sol),
        min_amount_lamports: lamports(sa.min_amount_sol).max(1),
        max_amount_lamports: lamports(sa.max_amount_sol).max(1),
        cu_limit,
        cooldown_ms: sa.cooldown_ms,
        max_price_impact: sa.max_price_impact_pct / 100.0,
        size_safety_margin: sa.size_safety_margin_pct / 100.0,
        max_profit_fraction: sa.max_profit_fraction_pct / 100.0,
    };

    let (tx, rx) = tokio::sync::mpsc::channel(sa.signal_buffer);
    let consumer = Arc::new(shred_stream::ShredConsumer::new(
        sa.shredstream_endpoint.clone(),
        target_pools,
        alt_map,
    ));
    let shred_metrics = consumer.metrics.clone();
    consumer.spawn(tx);

    let engine = Arc::new(shred_arb::ShredArbEngine::new(
        metis,
        blockhash_cache,
        trading_keypair.clone(),
        rpc_client,
        alt_cache,
        jito_client,
        jito_grpc_client,
        jito_limiter,
        jito_grpc_limiter,
        trading_keypair.pubkey().to_string(),
        pool_state,
        pairs,
        params,
        shred_metrics,
    ));
    engine.clone().spawn_reporter();
    tokio::spawn(engine.run(rx));

    eprintln!("[shred-arb] strategy started");
    Ok(())
}
