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
}

/// Runtime counters for observability.
#[derive(Default)]
pub struct ShredMetrics {
    pub entries: AtomicU64,
    pub txns: AtomicU64,
    pub pump_txns: AtomicU64,
    pub matched: AtomicU64,
    pub unresolved_pool: AtomicU64,
}

pub struct ShredConsumer {
    endpoint: String,
    /// Pool pubkeys we care about (Pump.fun side of each arb pair).
    target_pools: HashSet<Pubkey>,
    /// Preloaded, UNFILTERED address-lookup-table contents for our pools, so we
    /// can resolve ALT-provided accounts without an RPC call on the hot path.
    alt_map: HashMap<Pubkey, Vec<Pubkey>>,
    pumpfun: Pubkey,
    pub metrics: Arc<ShredMetrics>,
}

impl ShredConsumer {
    pub fn new(
        endpoint: String,
        target_pools: HashSet<Pubkey>,
        alt_map: HashMap<Pubkey, Vec<Pubkey>>,
    ) -> Self {
        Self {
            endpoint,
            target_pools,
            alt_map,
            pumpfun: pumpfun_program(),
            metrics: Arc::new(ShredMetrics::default()),
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

        for ix in msg.instructions() {
            let program = match full_keys.get(ix.program_id_index as usize) {
                Some(p) => *p,
                None => continue,
            };
            if program != self.pumpfun {
                continue;
            }
            if ix.data.len() < 24 {
                continue;
            }
            let disc: [u8; 8] = ix.data[0..8].try_into().unwrap();
            let is_buy = disc == DISC_BUY;
            let is_sell = disc == DISC_SELL;
            if !is_buy && !is_sell {
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
            if !self.target_pools.contains(&pool) {
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
            };
            // Non-blocking: if the engine is busy, drop (staleness makes an old
            // signal worthless anyway).
            let _ = tx.try_send(signal);
        }
    }

    fn resolve_keys(&self, msg: &solana_sdk::message::VersionedMessage) -> Vec<Pubkey> {
        let mut full: Vec<Pubkey> = msg.static_account_keys().to_vec();
        if let Some(lookups) = msg.address_table_lookups() {
            let mut writable = Vec::new();
            let mut readonly = Vec::new();
            for lookup in lookups {
                let alt = self.alt_map.get(&lookup.account_key);
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
            full.extend(writable);
            full.extend(readonly);
        }
        full
    }
}
