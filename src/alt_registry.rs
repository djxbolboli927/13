//! Per-pool "best ALT" registry, harvested from competitor transactions.
//!
//! There is no free way to MINT a route-optimized ALT (aggregators only publish
//! them for older pools). The reliable source is the competitors themselves: the
//! Pump.fun swaps we already see in the ShredStream carry `addressTableLookups`,
//! and for a fresh pool those tables contain exactly the pool/vault accounts our
//! own tx needs. So we collect the ALT keys each competitor tx used on one of our
//! pools, look INSIDE each candidate table (fetch + decode), and keep the SINGLE
//! table that covers the most of that pool's route accounts.
//!
//! Why a single table: two tables with the same accounts don't help — every
//! extra ALT referenced in a v0 message costs ~34 fixed bytes (its 32-byte pubkey
//! + 2 index-length bytes), and a table contributing no unique account is pure
//! overhead. One table with maximum coverage is optimal.
//!
//! Once a pool's chosen table covers enough accounts we FINALIZE it: register it
//! with Metis (both legs) and stop hunting for that pool. Until then, every new
//! competitor tx is a chance to find a better (higher-coverage) table.

use dashmap::DashMap;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

use crate::dex_ids::{METEORA_DAMM_V2_PROGRAM, PUMPFUN_AMM_PROGRAM};
use crate::metis::MetisClient;
use crate::pool_registry::ArbPair;
use crate::transaction::deserialize_alt_addresses;

#[derive(Clone, Copy)]
struct Chosen {
    alt: Pubkey,
    coverage: usize,
    finalized: bool,
}

pub struct AltRegistry {
    registry: Arc<DashMap<Pubkey, ArbPair>>,
    metis: Arc<MetisClient>,
    rpc: Arc<RpcClient>,
    /// pump_pool → best table chosen so far.
    chosen: DashMap<Pubkey, Chosen>,
    /// ALT key → its member set (fetched once, cached).
    members: DashMap<Pubkey, Arc<HashSet<Pubkey>>>,
    /// Coverage (route accounts found in the table) at which we finalize.
    min_coverage: usize,
}

impl AltRegistry {
    /// Spawn the background hunter and return (handle, candidate sender). Feed the
    /// sender `(pump_pool, alt_keys)` for every competitor tx that touched a
    /// watched pool and referenced ALTs.
    pub fn spawn(
        registry: Arc<DashMap<Pubkey, ArbPair>>,
        metis: Arc<MetisClient>,
        rpc: Arc<RpcClient>,
        min_coverage: usize,
    ) -> (Arc<Self>, mpsc::Sender<(Pubkey, Vec<Pubkey>)>) {
        let (tx, rx) = mpsc::channel(4096);
        let me = Arc::new(Self {
            registry,
            metis,
            rpc,
            chosen: DashMap::new(),
            members: DashMap::new(),
            min_coverage: min_coverage.max(1),
        });
        me.clone().run(rx);
        (me, tx)
    }

    /// The single chosen ALT for a pool (if any found yet). Read by the engine.
    pub fn best_for(&self, pump_pool: &Pubkey) -> Option<Pubkey> {
        self.chosen.get(pump_pool).map(|c| c.alt)
    }

    fn run(self: Arc<Self>, mut rx: mpsc::Receiver<(Pubkey, Vec<Pubkey>)>) {
        tokio::spawn(async move {
            while let Some((pool, candidates)) = rx.recv().await {
                self.consider(pool, candidates).await;
            }
        });
    }

    /// The route accounts a good table should cover: both pools + their vaults.
    fn needed_accounts(&self, pump_pool: &Pubkey) -> Option<HashSet<Pubkey>> {
        let pair = self.registry.get(pump_pool)?;
        let mut set = HashSet::new();
        for a in [
            pair.pump.pool,
            pair.pump.vault_a,
            pair.pump.vault_b,
            pair.meteora.pool,
            pair.meteora.vault_a,
            pair.meteora.vault_b,
        ] {
            if a != Pubkey::default() {
                set.insert(a);
            }
        }
        Some(set)
    }

    async fn members_of(&self, alt: Pubkey) -> Arc<HashSet<Pubkey>> {
        if let Some(m) = self.members.get(&alt) {
            return m.clone();
        }
        let rpc = self.rpc.clone();
        let set: HashSet<Pubkey> = tokio::task::spawn_blocking(move || {
            rpc.get_account(&alt)
                .ok()
                .and_then(|a| deserialize_alt_addresses(&a.data).ok())
                .map(|v| v.into_iter().collect())
                .unwrap_or_default()
        })
        .await
        .unwrap_or_default();
        let arc = Arc::new(set);
        self.members.insert(alt, arc.clone());
        arc
    }

    async fn consider(&self, pool: Pubkey, candidates: Vec<Pubkey>) {
        // Already finalized → stop hunting for this pool.
        if self.chosen.get(&pool).map(|c| c.finalized).unwrap_or(false) {
            return;
        }
        let needed = match self.needed_accounts(&pool) {
            Some(n) if !n.is_empty() => n,
            _ => return,
        };

        let mut best = self.chosen.get(&pool).map(|c| *c);
        let mut seen: HashSet<Pubkey> = HashSet::new();
        for alt in candidates {
            if !seen.insert(alt) {
                continue; // dedupe within this tx
            }
            let members = self.members_of(alt).await;
            let coverage = needed.iter().filter(|a| members.contains(a)).count();
            if coverage == 0 {
                continue;
            }
            let better = best.map(|b| coverage > b.coverage).unwrap_or(true);
            if better {
                best = Some(Chosen {
                    alt,
                    coverage,
                    finalized: false,
                });
                self.chosen.insert(
                    pool,
                    Chosen {
                        alt,
                        coverage,
                        finalized: false,
                    },
                );
                info!(%pool, %alt, coverage, needed = needed.len(), "alt-registry: better ALT chosen");
            }
        }

        // Finalize once coverage is good enough: register with Metis (both legs)
        // and stop hunting.
        let done = best
            .map(|b| b.coverage >= self.min_coverage.min(needed.len()))
            .unwrap_or(false);
        if let (true, Some(b)) = (done, best) {
            if let Some(pair) = self.registry.get(&pool).map(|e| e.clone()) {
                let alt = b.alt.to_string();
                let _ = self
                    .metis
                    .add_market(&pair.pump.pool.to_string(), PUMPFUN_AMM_PROGRAM, Some(&alt))
                    .await;
                let _ = self
                    .metis
                    .add_market(
                        &pair.meteora.pool.to_string(),
                        METEORA_DAMM_V2_PROGRAM,
                        Some(&alt),
                    )
                    .await;
            }
            self.chosen.insert(
                pool,
                Chosen {
                    alt: b.alt,
                    coverage: b.coverage,
                    finalized: true,
                },
            );
            info!(%pool, alt = %b.alt, coverage = b.coverage, "alt-registry: finalized (registered with Metis, hunt stopped)");
        }
    }
}
