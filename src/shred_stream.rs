//! Jito ShredStream consumer — the fast trigger for the arb strategy.
//!
//! We connect to a local `jito-shredstream-proxy` gRPC surface (the proxy holds
//! the whitelisted keypair, deshreds the UDP stream, and serves reconstructed
//! entries). ShredStream has NO server-side filter, so we receive the whole
//! cluster's entries and filter client-side for swaps that invoke the Pump.fun
//! AMM program on one of OUR target pools, then decode buy/sell.
//!
//! Detected swaps are pre-consensus (unconfirmed, possibly forked/failed) — the
//! decision engine treats them as an early probabilistic signal.

use anyhow::{Context, Result};
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::dex_ids::pumpfun_program;

mod pb {
    tonic::include_proto!("shredstream");
}
use pb::{shredstream_proxy_client::ShredstreamProxyClient, SubscribeEntriesRequest};

// Anchor 8-byte discriminators for PumpSwap instructions.
const DISC_BUY: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
const DISC_SELL: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];
/// `withdraw` (remove liquidity) — the direct rug signal.
const DISC_WITHDRAW: [u8; 8] = [183, 18, 70, 156, 148, 109, 161, 34];

/// A Pump.fun swap observed on ShredStream, before it reaches Metis/chain.
#[derive(Debug, Clone, Copy)]
pub struct PumpSwapSignal {
    pub pool: Pubkey,
    pub is_buy: bool,
    /// `base_amount_out` (buy) or `base_amount_in` (sell) — the token leg.
    pub base_amount: u64,
    /// The SOL-side limit arg (`max_quote_amount_in` / `min_quote_amount_out`),
    /// a cheap proxy for trade size before we price it exactly.
    pub quote_amount: u64,
    /// Signature of the observed on-chain tx this shred carried — recorded so the
    /// fee-audit log can print the exact tx whose fee the bot computed.
    pub sig: solana_sdk::signature::Signature,
}

/// Runtime counters for observability.
#[derive(Default)]
pub struct ShredMetrics {
    pub entries: AtomicU64,
    pub txns: AtomicU64,
    pub pump_txns: AtomicU64,
    pub matched: AtomicU64,
    pub unresolved_pool: AtomicU64,
    pub signals_sent: AtomicU64,
    pub signals_dropped: AtomicU64,
    /// Number of target Pump pools being watched (set once at startup).
    pub watched_pools: AtomicU64,
}

pub struct ShredConsumer {
    endpoint: String,
    /// Pool pubkeys we care about (Pump.fun side of each arb pair). Mutable at
    /// runtime so newly-discovered pools can be added without a restart.
    target_pools: std::sync::RwLock<HashSet<Pubkey>>,
    /// Global library of address-lookup-table contents harvested from every
    /// Pump tx we see (key → member pubkeys). Two uses: resolving ALT-hidden
    /// accounts on the hot path, and as the pool of PUBLIC pre-built tables the
    /// engine picks from to compress its own txs. Shared (Arc) so the engine
    /// reads the same live library.
    alt_map: Arc<std::sync::RwLock<HashMap<Pubkey, Vec<Pubkey>>>>,
    pumpfun: Pubkey,
    /// Optional sink for detected remove-liquidity (`withdraw`) events on a
    /// watched pool — the Pump pool pubkey is sent so the manager can close it.
    remove_tx: std::sync::RwLock<Option<mpsc::Sender<Pubkey>>>,
    /// RPC used by the self-learning ALT fetcher.
    rpc: Arc<solana_client::rpc_client::RpcClient>,
    /// ALT account keys seen on Pump txns that we couldn't resolve yet. A
    /// background task fetches these and folds them into `alt_map`, so that
    /// swaps hiding the pool behind an ALT become resolvable — this is how we
    /// stop missing trades that competitors (who resolve ALTs) already see.
    pending_alts: std::sync::Mutex<HashSet<Pubkey>>,
    /// Optional sink for `(pump_pool, alt_keys)` — the ALTs a competitor tx used
    /// on a watched pool, fed to the AltRegistry so it can pick the best table.
    alt_candidate_tx: std::sync::RwLock<Option<mpsc::Sender<(Pubkey, Vec<Pubkey>)>>>,
    pub metrics: Arc<ShredMetrics>,
}

impl ShredConsumer {
    pub fn new(
        endpoint: String,
        target_pools: HashSet<Pubkey>,
        alt_map: HashMap<Pubkey, Vec<Pubkey>>,
        rpc: Arc<solana_client::rpc_client::RpcClient>,
    ) -> Self {
        let metrics = Arc::new(ShredMetrics::default());
        metrics
            .watched_pools
            .store(target_pools.len() as u64, Ordering::Relaxed);
        Self {
            endpoint,
            target_pools: std::sync::RwLock::new(target_pools),
            alt_map: Arc::new(std::sync::RwLock::new(alt_map)),
            pumpfun: pumpfun_program(),
            remove_tx: std::sync::RwLock::new(None),
            rpc,
            pending_alts: std::sync::Mutex::new(HashSet::new()),
            alt_candidate_tx: std::sync::RwLock::new(None),
            metrics,
        }
    }

    /// Register the AltRegistry sink that receives `(pump_pool, alt_keys)` for
    /// each competitor tx seen on a watched pool.
    #[allow(dead_code)]
    pub fn set_alt_candidate_sender(&self, tx: mpsc::Sender<(Pubkey, Vec<Pubkey>)>) {
        *self.alt_candidate_tx.write().unwrap() = Some(tx);
    }

    /// Share the live global ALT library (key → member pubkeys). The engine
    /// reads this to pick the best public tables to compress its own txs.
    pub fn alt_library(&self) -> Arc<std::sync::RwLock<HashMap<Pubkey, Vec<Pubkey>>>> {
        self.alt_map.clone()
    }

    /// Background task: periodically fetch ALT account contents we don't yet
    /// know (harvested from observed Pump txns) and add them to `alt_map`, so
    /// pool accounts hidden behind those ALTs become resolvable.
    pub fn spawn_alt_fetcher(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                // Drain up to N unknown ALTs (skip ones already known).
                let batch: Vec<Pubkey> = {
                    let mut pending = self.pending_alts.lock().unwrap();
                    if pending.is_empty() {
                        continue;
                    }
                    let known = self.alt_map.read().unwrap();
                    let take: Vec<Pubkey> = pending
                        .iter()
                        .filter(|k| !known.contains_key(k))
                        .copied()
                        .take(25)
                        .collect();
                    for k in &take {
                        pending.remove(k);
                    }
                    take
                };
                if batch.is_empty() {
                    continue;
                }
                let rpc = self.rpc.clone();
                let me = self.clone();
                // Blocking RPC off the async worker.
                let _ = tokio::task::spawn_blocking(move || {
                    let mut added = 0usize;
                    for alt in batch {
                        if let Ok(acct) = rpc.get_account(&alt) {
                            if let Ok(addrs) =
                                crate::transaction::deserialize_alt_addresses(&acct.data)
                            {
                                me.alt_map.write().unwrap().insert(alt, addrs);
                                added += 1;
                            }
                        }
                    }
                    if added > 0 {
                        info!(added, "shred ALT cache learned new lookup tables");
                    }
                })
                .await;
            }
        });
    }

    /// Register a channel that receives the Pump pool pubkey whenever a
    /// remove-liquidity (`withdraw`) instruction is seen on a watched pool.
    pub fn set_remove_sender(&self, tx: mpsc::Sender<Pubkey>) {
        *self.remove_tx.write().unwrap() = Some(tx);
    }

    /// Add a Pump.fun pool to the watch set at runtime (and optionally its ALT
    /// contents for account resolution). Takes effect on the next shred.
    pub fn add_target(&self, pool: Pubkey, alt: Option<(Pubkey, Vec<Pubkey>)>) {
        let mut set = self.target_pools.write().unwrap();
        if set.insert(pool) {
            self.metrics
                .watched_pools
                .store(set.len() as u64, Ordering::Relaxed);
        }
        if let Some((alt_key, addrs)) = alt {
            self.alt_map.write().unwrap().insert(alt_key, addrs);
        }
    }

    /// Remove a pool from the watch set (dead/rugged pool).
    pub fn remove_target(&self, pool: &Pubkey) {
        let mut set = self.target_pools.write().unwrap();
        if set.remove(pool) {
            self.metrics
                .watched_pools
                .store(set.len() as u64, Ordering::Relaxed);
        }
    }

    /// Spawn the consumer; detected swaps are pushed to `tx`. Reconnects with
    /// backoff.
    pub fn spawn(self: Arc<Self>, tx: mpsc::Sender<PumpSwapSignal>) {
        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                match self.run(&tx).await {
                    Ok(()) => warn!("shredstream ended cleanly, reconnecting"),
                    Err(e) => warn!(error = %e, "shredstream error, reconnecting"),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
        });
    }

    async fn run(&self, tx: &mpsc::Sender<PumpSwapSignal>) -> Result<()> {
        let mut client = ShredstreamProxyClient::connect(self.endpoint.clone())
            .await
            .with_context(|| format!("shredstream connect failed: {}", self.endpoint))?;

        let mut stream = client
            .subscribe_entries(SubscribeEntriesRequest {})
            .await
            .context("subscribe_entries failed")?
            .into_inner();

        info!(endpoint = %self.endpoint, "shredstream subscription active");

        while let Some(msg) = stream.message().await? {
            self.metrics.entries.fetch_add(1, Ordering::Relaxed);
            let entries: Vec<solana_entry::entry::Entry> =
                match bincode::deserialize(&msg.entries) {
                    Ok(e) => e,
                    Err(_) => continue, // partial / unrecoverable FEC set
                };
            for entry in &entries {
                for vtx in &entry.transactions {
                    self.metrics.txns.fetch_add(1, Ordering::Relaxed);
                    self.scan_tx(vtx, tx);
                }
            }
        }
        Ok(())
    }

    fn scan_tx(
        &self,
        vtx: &solana_sdk::transaction::VersionedTransaction,
        tx: &mpsc::Sender<PumpSwapSignal>,
    ) {
        let msg = &vtx.message;
        let static_keys = msg.static_account_keys();

        // Quick reject: the invoked program must appear as a static key.
        if !static_keys.iter().any(|k| *k == self.pumpfun) {
            return;
        }
        self.metrics.pump_txns.fetch_add(1, Ordering::Relaxed);

        // Resolve the full ordered account list (static + ALT writable + ALT
        // readonly), filling unknown-ALT slots with a placeholder.
        let full_keys = self.resolve_keys(msg);

        // The ALT keys this tx used — candidates for whichever watched pool it
        // touches (fed to the AltRegistry, which picks the best-coverage table).
        let tx_alts: Vec<Pubkey> = msg
            .address_table_lookups()
            .map(|ls| ls.iter().map(|l| l.account_key).collect())
            .unwrap_or_default();

        for ix in msg.instructions() {
            let program = match full_keys.get(ix.program_id_index as usize) {
                Some(p) => *p,
                None => continue,
            };
            if program != self.pumpfun {
                continue;
            }
            if ix.data.len() < 8 {
                continue;
            }
            let disc: [u8; 8] = ix.data[0..8].try_into().unwrap();
            let is_buy = disc == DISC_BUY;
            let is_sell = disc == DISC_SELL;
            let is_withdraw = disc == DISC_WITHDRAW;
            if !is_buy && !is_sell && !is_withdraw {
                continue;
            }
            // Account index 0 = pool.
            let pool = match ix.accounts.first().and_then(|i| full_keys.get(*i as usize)) {
                Some(p) => *p,
                None => continue,
            };
            if pool == Pubkey::default() {
                self.metrics.unresolved_pool.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            if !self.target_pools.read().unwrap().contains(&pool) {
                continue;
            }

            // Feed this tx's ALTs as candidates for the pool (the AltRegistry
            // picks the best-coverage one and stops once it's good enough).
            if !tx_alts.is_empty() {
                if let Some(s) = self.alt_candidate_tx.read().unwrap().as_ref() {
                    let _ = s.try_send((pool, tx_alts.clone()));
                }
            }

            // Remove-liquidity on a watched pool → signal the manager to close it.
            if is_withdraw {
                if let Some(sender) = self.remove_tx.read().unwrap().as_ref() {
                    let _ = sender.try_send(pool);
                }
                continue;
            }

            if ix.data.len() < 24 {
                continue;
            }
            let base_amount = u64::from_le_bytes(ix.data[8..16].try_into().unwrap());
            let quote_amount = u64::from_le_bytes(ix.data[16..24].try_into().unwrap());

            self.metrics.matched.fetch_add(1, Ordering::Relaxed);
            let signal = PumpSwapSignal {
                pool,
                is_buy,
                base_amount,
                quote_amount,
                sig: vtx.signatures.first().copied().unwrap_or_default(),
            };
            // Non-blocking: if the engine is busy, drop (staleness makes an old
            // signal worthless anyway).
            match tx.try_send(signal) {
                Ok(()) => self.metrics.signals_sent.fetch_add(1, Ordering::Relaxed),
                Err(_) => self.metrics.signals_dropped.fetch_add(1, Ordering::Relaxed),
            };
        }
    }

    fn resolve_keys(&self, msg: &solana_sdk::message::VersionedMessage) -> Vec<Pubkey> {
        let mut full: Vec<Pubkey> = msg.static_account_keys().to_vec();
        if let Some(lookups) = msg.address_table_lookups() {
            let mut writable = Vec::new();
            let mut readonly = Vec::new();
            let mut unknown: Vec<Pubkey> = Vec::new();
            {
                let alt_map = self.alt_map.read().unwrap();
                for lookup in lookups {
                    let alt = alt_map.get(&lookup.account_key);
                    if alt.is_none() {
                        // ALT we don't have — queue it for the fetcher so future
                        // swaps hiding a pool behind it become resolvable.
                        unknown.push(lookup.account_key);
                    }
                    for &i in &lookup.writable_indexes {
                        writable.push(
                            alt.and_then(|a| a.get(i as usize)).copied().unwrap_or_default(),
                        );
                    }
                    for &i in &lookup.readonly_indexes {
                        readonly.push(
                            alt.and_then(|a| a.get(i as usize)).copied().unwrap_or_default(),
                        );
                    }
                }
            }
            if !unknown.is_empty() {
                let mut pending = self.pending_alts.lock().unwrap();
                // Bound memory: never let the backlog grow without limit.
                if pending.len() < 5000 {
                    for k in unknown {
                        pending.insert(k);
                    }
                }
            }
            full.extend(writable);
            full.extend(readonly);
        }
        full
    }
}
