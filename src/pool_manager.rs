//! Automatic pool-management pipeline.
//!
//! Ties together every moving part needed to bring a freshly-discovered shared
//! pool (a token that trades on BOTH Pump.fun AMM and Meteora DAMM v2) online
//! WITHOUT a restart, and to tear one down when it rugs:
//!
//! Add pipeline (`add_pair`):
//!   1. Register both pools with Metis via `POST /add-market` so quotes route.
//!   2. Add the Meteora pool + Pump vault pair to the live gRPC pool-state sub.
//!   3. Add the Pump pool (and its ALT contents) to the ShredStream watch set.
//!   4. Create the trading wallet's ATA for the token so we can hold it.
//!   5. Insert the pair into the shared registry the engine reads.
//!
//! Remove pipeline (`remove_pair`):
//!   1. Drop the pair from the registry (engine stops trading it).
//!   2. Remove the Pump pool from the ShredStream watch set.
//!   3. Close the token ATA to reclaim rent.

use anyhow::Result;
use dashmap::DashMap;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

use crate::ata;
use crate::dex_ids::{METEORA_DAMM_V2_PROGRAM, PUMPFUN_AMM_PROGRAM};
use crate::metis::MetisClient;
use crate::pool_registry::{ArbPair, PoolInfo};
use crate::pool_state::PoolStateCache;
use crate::shred_stream::ShredConsumer;
use crate::transaction;

pub struct PoolManager {
    registry: Arc<DashMap<Pubkey, ArbPair>>,
    pool_state: PoolStateCache,
    consumer: Arc<ShredConsumer>,
    metis: Arc<MetisClient>,
    rpc: Arc<RpcClient>,
    keypair: Arc<Keypair>,
    /// Reserved for tuning the drain threshold; closing currently uses a strict
    /// full-drain check (dust floor) so a dip never closes a hot pool.
    #[allow(dead_code)]
    min_pump_wsol: u64,
}

impl PoolManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        registry: Arc<DashMap<Pubkey, ArbPair>>,
        pool_state: PoolStateCache,
        consumer: Arc<ShredConsumer>,
        metis: Arc<MetisClient>,
        rpc: Arc<RpcClient>,
        keypair: Arc<Keypair>,
        min_pump_wsol: u64,
    ) -> Self {
        Self {
            registry,
            pool_state,
            consumer,
            metis,
            rpc,
            keypair,
            min_pump_wsol,
        }
    }

    fn read_u64(data: &[u8], off: usize) -> Option<u64> {
        data.get(off..off + 8)
            .map(|s| u64::from_le_bytes(s.try_into().unwrap()))
    }
    fn read_u128(data: &[u8], off: usize) -> Option<u128> {
        data.get(off..off + 16)
            .map(|s| u128::from_le_bytes(s.try_into().unwrap()))
    }

    /// Confirm via RPC whether a pool is actually dead, and close it only then.
    /// Called on a ShredStream `withdraw` event (which may be a PARTIAL remove
    /// on a still-liquid, hot pool) and by the hourly sweep — so a mere
    /// liquidity dip never closes a good token; only a genuine drain does.
    /// A pool is dead when Meteora `liquidity == 0`, or the Pump WSOL reserve
    /// has fallen below `min_pump_wsol` (untradeable / rugged).
    pub fn check_and_close(&self, pump_pool: &Pubkey) {
        let pair = match self.registry.get(pump_pool) {
            Some(e) => e.value().clone(),
            None => return,
        };
        let met_liq = self
            .rpc
            .get_account(&pair.meteora.pool)
            .ok()
            .and_then(|a| Self::read_u128(&a.data, 360))
            .unwrap_or(0);
        let pump_wsol = self
            .rpc
            .get_account(&pair.pump.wsol_vault())
            .ok()
            .and_then(|a| Self::read_u64(&a.data, 64))
            .unwrap_or(0);
        // Close ONLY on a genuine full drain — Meteora liquidity gone, or the
        // Pump WSOL reserve emptied to dust. A partial remove that leaves the
        // pool liquid keeps it (never close on a mere reduction).
        let dead = met_liq == 0 || pump_wsol < 1_000;
        if dead {
            info!(pump = %pump_pool, met_liq, pump_wsol, "confirmed dead pool — closing");
            self.remove_pair(pump_pool);
        } else {
            info!(pump = %pump_pool, pump_wsol, "withdraw seen but pool still liquid — keeping");
        }
    }

    /// Hourly RPC sweep over every tracked pool: close the ones that are dead
    /// (drained/untradeable) so we don't hold idle ATAs. This is the periodic
    /// "check all ATAs' pools" the operator asked for.
    pub fn spawn_hourly_sweep(self: Arc<Self>, interval: Duration) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let pools: Vec<Pubkey> =
                    self.registry.iter().map(|e| e.value().pump.pool).collect();
                info!(count = pools.len(), "hourly pool sweep starting");
                for pool in pools {
                    let me = self.clone();
                    // Blocking RPC off the async worker; space out to respect budget.
                    let _ = tokio::task::spawn_blocking(move || me.check_and_close(&pool)).await;
                    tokio::time::sleep(Duration::from_millis(150)).await;
                }
            }
        });
    }

    /// Whether the given Pump pool is already tracked.
    pub fn contains(&self, pump_pool: &Pubkey) -> bool {
        self.registry.contains_key(pump_pool)
    }

    /// Register a Metis market for a single pool, logging failures without
    /// aborting (Metis may already know the pool, which returns non-2xx).
    async fn register_metis(&self, pool: &PoolInfo, owner: &str) {
        let alt = pool.alt.map(|a| a.to_string());
        match self
            .metis
            .add_market(&pool.pool.to_string(), owner, alt.as_deref())
            .await
        {
            Ok(()) => info!(pool = %pool.pool, "metis add-market ok"),
            Err(e) => warn!(pool = %pool.pool, error = %e, "metis add-market failed"),
        }
    }

    /// Fetch and decode an ALT's addresses (best-effort).
    fn load_alt(&self, alt: Pubkey) -> Option<(Pubkey, Vec<Pubkey>)> {
        match self.rpc.get_account(&alt) {
            Ok(acct) => match transaction::deserialize_alt_addresses(&acct.data) {
                Ok(addrs) => Some((alt, addrs)),
                Err(e) => {
                    warn!(%alt, error = %e, "bad ALT while adding pool");
                    None
                }
            },
            Err(e) => {
                warn!(%alt, error = %e, "failed to fetch ALT while adding pool");
                None
            }
        }
    }

    /// Bring a newly-discovered shared pool fully online. Idempotent: a pool
    /// already in the registry is skipped.
    pub async fn add_pair(&self, pair: ArbPair) -> Result<()> {
        if self.registry.contains_key(&pair.pump.pool) {
            return Ok(());
        }
        info!(
            token = %pair.token_mint,
            pump = %pair.pump.pool,
            meteora = %pair.meteora.pool,
            "adding shared pool"
        );

        // 1. Metis markets for both legs.
        self.register_metis(&pair.pump, PUMPFUN_AMM_PROGRAM).await;
        self.register_metis(&pair.meteora, METEORA_DAMM_V2_PROGRAM).await;

        // 2. Live pool-state subscription: Meteora pool + Pump vault pair.
        let accounts = [
            pair.meteora.pool,
            pair.pump.token_vault(),
            pair.pump.wsol_vault(),
        ];
        self.pool_state.add_accounts(&self.rpc, &accounts);

        // 3. ShredStream watch set (+ ALT contents for account resolution).
        let alt = pair.pump.alt.and_then(|a| self.load_alt(a));
        self.consumer.add_target(pair.pump.pool, alt);

        // 4. Token ATA so we can hold the asset.
        match ata::ensure_ata(&self.rpc, &self.keypair, &pair.token_mint) {
            Ok(_) => {}
            Err(e) => warn!(token = %pair.token_mint, error = %e, "ensure_ata failed"),
        }

        // 5. Register with the engine.
        self.registry.insert(pair.pump.pool, pair);
        Ok(())
    }

    /// Periodically sweep tracked pools and tear down dead ones. A pool is dead
    /// when EITHER it is fully drained (Meteora `liquidity == 0` / an emptied
    /// Pump vault — an unambiguous total removal, not a mere dip) OR it has been
    /// IDLE: no account update for its Meteora pool for `idle_timeout` (the bot
    /// has been running and simply stopped seeing activity → abandoned/rugged).
    /// A liquidity *reduction* is deliberately NOT a trigger — that closed good
    /// hot pools. Full-drain requires `confirm_ticks` consecutive reads to avoid
    /// a transient garbage read; the idle check is time-based already.
    pub fn spawn_rug_monitor(
        self: Arc<Self>,
        interval: Duration,
        confirm_ticks: u32,
        idle_timeout: Duration,
    ) {
        tokio::spawn(async move {
            let mut strikes: std::collections::HashMap<Pubkey, u32> =
                std::collections::HashMap::new();
            loop {
                tokio::time::sleep(interval).await;
                let mut dead: Vec<Pubkey> = Vec::new();
                for entry in self.registry.iter() {
                    let pair = entry.value();

                    // Idle timeout: no Meteora update for the whole window.
                    if let Some(age) = self.pool_state.last_update_age(&pair.meteora.pool) {
                        if age >= idle_timeout {
                            dead.push(pair.pump.pool);
                            continue;
                        }
                    }

                    // Full drain (confirmed over N sweeps).
                    if self.is_fully_drained(pair) {
                        let c = strikes.entry(pair.pump.pool).or_insert(0);
                        *c += 1;
                        if *c >= confirm_ticks {
                            dead.push(pair.pump.pool);
                        }
                    } else {
                        strikes.remove(&pair.pump.pool);
                    }
                }
                for pool in dead {
                    strikes.remove(&pool);
                    // remove_pair does blocking RPC (close ATA) — offload it so
                    // the monitor loop isn't stalled on a tokio worker thread.
                    let me = self.clone();
                    tokio::task::spawn_blocking(move || me.remove_pair(&pool));
                }
            }
        });
    }

    /// True only on a TOTAL drain: Meteora `liquidity == 0` or a Pump vault
    /// emptied. A missing cache read is "unknown", never drained.
    fn is_fully_drained(&self, pair: &ArbPair) -> bool {
        matches!(self.pool_state.spl_amount(&pair.pump.token_vault()), Some(0))
            || matches!(self.pool_state.spl_amount(&pair.pump.wsol_vault()), Some(0))
            || matches!(
                self.pool_state.meteora_raw_liquidity(&pair.meteora.pool),
                Some(0)
            )
    }

    /// Tear down a rugged/dead pool: stop trading it, unwatch it, reclaim rent.
    pub fn remove_pair(&self, pump_pool: &Pubkey) {
        let Some((_, pair)) = self.registry.remove(pump_pool) else {
            return;
        };
        info!(
            token = %pair.token_mint,
            pump = %pair.pump.pool,
            "removing pool (rug/dead)"
        );
        self.consumer.remove_target(pump_pool);
        if let Err(e) = ata::close_ata(&self.rpc, &self.keypair, &pair.token_mint) {
            warn!(token = %pair.token_mint, error = %e, "close_ata failed");
        }
    }
}
