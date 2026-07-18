#[allow(dead_code)]
mod account_cache;
mod alt_builder;
mod alt_cache;
mod alt_fetch;
mod alt_registry;
mod arbitrage;
#[allow(dead_code)]
mod ata;
mod blockhash_cache;
mod config;
mod dex_accounts;
#[allow(dead_code)]
mod dex_ids;
#[allow(dead_code)]
mod discovery;
mod errlog;
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
mod pool_manager;
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
#[allow(dead_code)]
mod wallet_miner;

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
    let sa = config.shred_arb.clone();
    // Dedicated CU limit for the 2-hop Pump↔Meteora tx. Competitor arb txs
    // consume ~178k CU, so the legacy 170k default would run out — use a
    // roomier value (configurable).
    // CU limit for the arb tx, taken from [performance].cu_limits (indexed by
    // hop count: index 0 = 2 hops, 1 = 3 hops, …) so the operator tunes it in one
    // place. The 2-hop Pump↔Meteora route needs ~120k; falls back to the
    // [shred_arb].cu_limit only if the array is empty.
    let cu_limit = config
        .performance
        .cu_limits
        .first()
        .copied()
        .unwrap_or(sa.cu_limit);

    // Auto-launch the ShredStream proxy immediately — it does not depend on
    // mix.json, and the entries feed can warm up while we wait for the pools.
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

    let lamports = |sol: f64| (sol * 1_000_000_000.0) as u64;
    let params = shred_arb::ArbParams {
        tip_lamports: sa.tip_lamports,
        network_fee_lamports: sa.network_fee_lamports,
        jito_tip_min_lamports: sa.jito_tip_min_lamports,
        jito_tip_profit_fraction: sa.jito_tip_profit_fraction,
        meteora_fee_bps: sa.meteora_fee_bps,
        min_trigger_lamports: lamports(sa.min_trigger_sol),
        min_amount_lamports: lamports(sa.min_amount_sol).max(1),
        max_amount_lamports: lamports(sa.max_amount_sol).max(1),
        cu_limit,
        cooldown_ms: sa.cooldown_ms,
        max_price_impact: sa.max_price_impact_pct / 100.0,
        size_safety_margin: sa.size_safety_margin_pct / 100.0,
        max_profit_fraction: sa.max_profit_fraction_pct / 100.0,
        min_net_profit_lamports: sa.min_net_profit_lamports,
        direct_send: sa.direct_send,
        direct_priority_fee_microlamports: sa.direct_priority_fee_microlamports,
        metis_max_accounts: sa.metis_max_accounts,
        loaded_accounts_data_limit: sa.direct_loaded_accounts_data_limit,
        min_trigger_reserve_frac: sa.min_trigger_reserve_frac,
        pump_label: sa.metis_pump_label.clone(),
        meteora_label: sa.metis_meteora_label.clone(),
        use_shared_accounts: sa.metis_use_shared_accounts,
        send_dedup_ms: sa.send_dedup_ms,
        status_check_delay_secs: sa.status_check_delay_secs,
        never_close: sa.never_close_pools,
    };

    // Errors-only file log (errors + why-not-sent + why-lost) under /root/g.
    errlog::init(&sa.error_log_dir);

    // ── Build self-test ──────────────────────────────────────────────────────
    // Runs the exact Meteora math on real pool numbers seen in the logs. A fresh
    // binary MUST print wsol_reserve=3762681 and swap_buy_1000=Some(31617697).
    // If it prints u64::MAX / None, you are running a STALE binary (rebuild with
    // `cargo clean && cargo build --release`).
    {
        let t = meteora_math::MeteoraPool {
            sqrt_price: 103_676_349_798_172_274,
            liquidity: 12_349_724_641_806_170_141_067_834_768,
            sqrt_min_price: meteora_math::MIN_SQRT_PRICE,
            sqrt_max_price: meteora_math::MAX_SQRT_PRICE,
            fee_numerator: 1_000_000,
        };
        let r = t.wsol_reserve(true);
        let s = t.swap_exact_in(1000, false).map(|o| o.amount_out);
        eprintln!(
            "[selftest] meteora wsol_reserve={r} (expect 3762681) swap_buy_1000={s:?} (expect Some(31617697))"
        );
    }

    // Load pools and run the strategy in a background task that RETRIES the
    // mix.json read — if Metis hasn't written it yet (or is restarting) the bot
    // waits instead of giving up and parking.
    tokio::spawn(async move {
        use std::collections::{HashMap, HashSet};

        let pairs = loop {
            match pool_registry::load_pairs(&sa.mix_cache_path) {
                Ok(p) if !p.is_empty() => break p,
                Ok(_) => eprintln!(
                    "[shred-arb] mix.json has 0 usable Pump↔Meteora pairs — retrying in 5s"
                ),
                Err(e) => eprintln!(
                    "[shred-arb] cannot read {} ({e}) — retrying in 5s (is Metis running?)",
                    sa.mix_cache_path
                ),
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        };

        for p in &pairs {
            eprintln!(
                "[shred-arb] pair token={} | pump_pool={} vaults=({},{}) | meteora_pool={}",
                p.token_mint, p.pump.pool, p.pump.token_vault(), p.pump.wsol_vault(), p.meteora.pool,
            );
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

        let pool_state = pool_state::PoolStateCache::new();
        pool_state.prefetch(&rpc_client, &accounts);
        pool_state.spawn_subscription(
            sa.pool_state_endpoint.clone(),
            sa.pool_state_x_token.clone(),
            accounts,
        );

        // Preload (unfiltered) ALT contents for each Pump pool so the consumer
        // can resolve ALT-provided accounts without a hot-path RPC call.
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

        let (tx, rx) = tokio::sync::mpsc::channel(sa.signal_buffer);
        let consumer = Arc::new(shred_stream::ShredConsumer::new(
            sa.shredstream_endpoint.clone(),
            target_pools,
            alt_map,
            rpc_client.clone(),
            trading_keypair.pubkey(),
        ));
        let shred_metrics = consumer.metrics.clone();
        consumer.clone().spawn(tx);
        // Self-learning ALT cache: resolve pools hidden behind lookup tables so
        // we stop missing swaps competitors already see.
        consumer.clone().spawn_alt_fetcher();

        // Free ALT fetcher (Jupiter/DFlow/Raptor) — the cheap way to compress the
        // route (no on-chain writes / rent from us).
        let providers: Vec<alt_fetch::Provider> = sa
            .alt_fetch_providers
            .iter()
            .filter_map(|s| {
                let (name, url) = s.split_once('|')?;
                Some(alt_fetch::Provider {
                    name: name.trim().to_string(),
                    base_url: url.trim().to_string(),
                })
            })
            .collect();
        let alt_fetcher = if providers.is_empty() {
            None
        } else {
            Some(alt_fetch::AltFetcher::new(
                providers,
                trading_keypair.pubkey().to_string(),
                sa.metis_pump_label.clone(),
                sa.metis_meteora_label.clone(),
                sa.alt_max_per_pool,
            ))
        };

        // Shared, mutable pool registry (seeded from mix.json; discovery adds
        // more). Each Pump pool maps to EVERY (pump, meteora) pair it belongs
        // to — a token can have several Meteora counter-pools.
        let registry: Arc<dashmap::DashMap<solana_sdk::pubkey::Pubkey, Vec<pool_registry::ArbPair>>> =
            Arc::new(dashmap::DashMap::new());
        for p in pairs {
            // Watch the Meteora counter-pool for in-flight competing txs.
            consumer.add_meteora_target(p.meteora.pool);
            registry.entry(p.pump.pool).or_default().push(p);
        }
        // Pre-fetch ALTs for the startup (mix.json) pools so their first txs fit.
        if let Some(f) = &alt_fetcher {
            for e in registry.iter() {
                for p in e.value() {
                    tokio::spawn(f.clone().fetch_for_pool(p.pump.pool, p.meteora.pool, p.token_mint));
                }
            }
        }

        // ALT selector backed by the GLOBAL library of public tables harvested
        // from shreds (shred_stream.alt_map). For each tx we build, it picks the
        // best-covering tables for that route's real accounts (one per leg, no
        // duplicates) — the proven tx-size fix. No Metis registration needed:
        // compression happens in our own v0 build step.
        let alt_registry = alt_registry::AltRegistry::new(consumer.alt_library());

        // Automatic pool manager: add-market to Metis, extend the gRPC/shred
        // subscriptions, and manage ATAs — all at runtime, no restart. It holds
        // clones so the engine can still own its handles below.
        let pool_manager = Arc::new(pool_manager::PoolManager::new(
            registry.clone(),
            pool_state.clone(),
            consumer.clone(),
            metis.clone(),
            rpc_client.clone(),
            trading_keypair.clone(),
            sa.discovery_min_pump_wsol_lamports,
            alt_fetcher.clone(),
        ));

        // Startup ATA reconciliation: read EVERY token mint referenced in
        // mix.json (no RPC needed to know the mints — they're in the file),
        // skip the always-exist set (SOL/WSOL/USDC/USDT…), batch-check which
        // ATAs exist, and create any that are missing. Runs off the async
        // runtime (blocking RPC) so it doesn't stall the strategy.
        {
            let rpc = rpc_client.clone();
            let kp = trading_keypair.clone();
            let mix_path = sa.mix_cache_path.clone();
            let skip: std::collections::HashSet<solana_sdk::pubkey::Pubkey> = sa
                .always_exist_mints
                .iter()
                .filter_map(|s| solana_sdk::pubkey::Pubkey::try_from(s.as_str()).ok())
                .collect();
            tokio::task::spawn_blocking(move || {
                match pool_registry::load_all_token_mints(&mix_path) {
                    Ok(mints) => {
                        if let Err(e) = ata::reconcile_atas(&rpc, &kp, &mints, &skip) {
                            tracing::warn!(error = %e, "ATA reconcile failed");
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "ATA reconcile: cannot read mix.json"),
                }
            });
        }
        // Teardown pipeline (rug monitor / withdraw-close / sweep) is DISABLED
        // when never_close_pools is set: the bot only ever opens pools+ATAs, and
        // an idle-token sweeper (below) just REPORTS untraded tokens to a file
        // instead of closing anything. Kept behind the flag so the old
        // close-on-drain behaviour is still available if re-enabled.
        if !sa.never_close_pools {
            pool_manager.clone().spawn_rug_monitor(
                std::time::Duration::from_secs(2),
                3,
                std::time::Duration::from_secs(sa.pool_idle_close_secs.max(60)),
            );
            let (rm_tx, mut rm_rx) =
                tokio::sync::mpsc::channel::<solana_sdk::pubkey::Pubkey>(256);
            consumer.set_remove_sender(rm_tx);
            let mgr = pool_manager.clone();
            tokio::spawn(async move {
                while let Some(pool) = rm_rx.recv().await {
                    let mgr = mgr.clone();
                    tokio::task::spawn_blocking(move || mgr.check_and_close(&pool));
                }
            });
            pool_manager
                .clone()
                .spawn_hourly_sweep(std::time::Duration::from_secs(1800));
        }

        // Idle-token sweeper: every `idle_token_check_secs` (default 12h), record
        // which tracked tokens had NO on-chain trade in the window to a file.
        // Never closes anything — reporting only.
        {
            let registry = registry.clone();
            let rpc = rpc_client.clone();
            let path = sa.idle_tokens_path.clone();
            let window = std::time::Duration::from_secs(sa.idle_token_check_secs.max(60));
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(window);
                ticker.tick().await; // skip the immediate first tick
                loop {
                    ticker.tick().await;
                    let pools: Vec<(String, solana_sdk::pubkey::Pubkey)> = registry
                        .iter()
                        .flat_map(|e| {
                            e.value()
                                .iter()
                                .map(|p| (p.token_mint.to_string(), p.meteora.pool))
                                .collect::<Vec<_>>()
                        })
                        .collect();
                    let rpc = rpc.clone();
                    let path = path.clone();
                    let window_secs = window.as_secs() as i64;
                    tokio::task::spawn_blocking(move || {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs() as i64)
                            .unwrap_or(0);
                        let mut idle: Vec<String> = Vec::new();
                        for (token, pool) in pools {
                            // Most recent signature's blockTime on the pool account.
                            let last = rpc
                                .get_signatures_for_address(&pool)
                                .ok()
                                .and_then(|v| v.into_iter().next())
                                .and_then(|s| s.block_time);
                            let traded_recently = matches!(last, Some(t) if now - t < window_secs);
                            if !traded_recently {
                                idle.push(token);
                            }
                        }
                        let body = format!(
                            "# tokens with no on-chain trade in the last {}h (as of unix {})\n{}\n",
                            window_secs / 3600,
                            now,
                            idle.join("\n")
                        );
                        if let Err(e) = std::fs::write(&path, body) {
                            tracing::warn!(error = %e, path = %path, "idle-token report write failed");
                        } else {
                            tracing::info!(idle = idle.len(), path = %path, "idle-token report written");
                        }
                    });
                }
            });
        }

        // Wallet-transaction pool miner: mine competitors' recent txs for hot
        // shared Pump/Meteora pools and add them (bot + Metis) every 30 min.
        if !sa.target_wallets.is_empty() {
            let miner = wallet_miner::WalletMiner::new(
                wallet_miner::WalletMinerConfig {
                    rpc_url: rpc_client.url(),
                    wallets: sa.target_wallets.clone(),
                    interval: std::time::Duration::from_secs(
                        sa.wallet_mine_interval_secs.max(60),
                    ),
                    tx_limit: sa.wallet_mine_tx_limit.max(1),
                    min_pump_wsol_lamports: sa.discovery_min_pump_wsol_lamports,
                    min_meteora_wsol_lamports: sa.discovery_min_meteora_wsol_lamports,
                },
                pool_manager.clone(),
                rpc_client.clone(),
            );
            miner.spawn();
        }

        // Auto-discovery: poll public APIs for new shared Pump/Meteora pools and
        // add them at runtime via the pool manager.
        if sa.discovery_enabled {
            let disc = discovery::Discovery::new(
                discovery::DiscoveryConfig {
                    interval: std::time::Duration::from_secs(
                        sa.discovery_interval_secs.max(1),
                    ),
                    recheck_interval: std::time::Duration::from_secs(
                        sa.discovery_recheck_interval_secs.max(10),
                    ),
                    new_pools_url: sa.discovery_new_pools_url.clone(),
                    token_pairs_url: sa.discovery_token_pairs_url.clone(),
                    seed_urls: sa.discovery_seed_urls.clone(),
                    bootstrap_max: sa.discovery_bootstrap_max,
                    min_h1_volume_usd: sa.discovery_min_h1_volume_usd,
                    min_pump_wsol_lamports: sa.discovery_min_pump_wsol_lamports,
                    min_meteora_wsol_lamports: sa.discovery_min_meteora_wsol_lamports,
                },
                pool_manager.clone(),
                rpc_client.clone(),
            );
            disc.spawn();
        }

        // Self-owned ALT — OFF by default (costs rent). Only spawned when
        // alt_self_build=true; normally we rely on the free provider ALTs above.
        let alt_builder = if sa.alt_self_build {
            Some(alt_builder::AltBuilder::spawn(
                rpc_client.clone(),
                trading_keypair.clone(),
                sa.alt_store_path.clone(),
            ))
        } else {
            None
        };

        let user_pubkey = trading_keypair.pubkey().to_string();
        let engine = Arc::new(shred_arb::ShredArbEngine::new(
            metis,
            blockhash_cache,
            trading_keypair,
            rpc_client,
            alt_cache,
            jito_client,
            jito_grpc_client,
            jito_limiter,
            jito_grpc_limiter,
            user_pubkey,
            pool_state,
            registry,
            params,
            shred_metrics,
            Some(pool_manager),
            alt_builder,
            alt_fetcher,
            alt_registry,
            consumer.meteora_activity(),
        ));
        engine.clone().spawn_reporter();
        // Second opportunity source: re-assess all pairs from current state
        // every 200ms, not only when a Pump shred fires.
        engine.clone().spawn_state_evaluator(200);
        eprintln!("[shred-arb] strategy started");
        engine.run(rx).await;
    });

    Ok(())
}
