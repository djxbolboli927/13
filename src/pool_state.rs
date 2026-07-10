//! Narrow live pool-state cache for the ShredStream arb strategy.
//!
//! A dedicated, lightweight Yellowstone gRPC subscription that watches ONLY the
//! handful of accounts we trade — every Meteora pool account plus every Pump.fun
//! vault pair — rather than an owner-wide firehose. Overhead is negligible and
//! it is fully independent of the Metis feed.
//!
//! * Meteora price/liquidity live inside the pool account (`sqrt_price` + `L`),
//!   so watching the pool account alone is enough.
//! * Pump.fun reserves are the SPL-token balances of the two vaults, so we watch
//!   both vault token accounts.

use anyhow::{Context, Result};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterAccounts, SubscribeRequestPing,
};

use crate::meteora_math::MeteoraPool;
use crate::pumpfun_math::PumpPool;

// Meteora DAMM v2 Pool account field offsets (bytes, discriminator included).
// See meteora_math / research report. Verify against a live account if the
// deployed layout drifts (the fee sub-struct size is the fragile part).
const MET_OFF_LIQUIDITY: usize = 360;
const MET_OFF_SQRT_MIN: usize = 424;
const MET_OFF_SQRT_MAX: usize = 440;
const MET_OFF_SQRT_PRICE: usize = 456;

// SPL token account: amount is a u64 LE at offset 64.
const SPL_AMOUNT_OFFSET: usize = 64;

#[derive(Clone)]
pub struct PoolStateCache {
    inner: Arc<DashMap<Pubkey, Vec<u8>>>,
    slot: Arc<AtomicU64>,
}

fn read_u128_le(data: &[u8], off: usize) -> Option<u128> {
    data.get(off..off + 16)
        .map(|s| u128::from_le_bytes(s.try_into().unwrap()))
}

fn read_u64_le(data: &[u8], off: usize) -> Option<u64> {
    data.get(off..off + 8)
        .map(|s| u64::from_le_bytes(s.try_into().unwrap()))
}

impl PoolStateCache {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::with_capacity(256)),
            slot: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn slot(&self) -> u64 {
        self.slot.load(Ordering::Relaxed)
    }

    pub fn has(&self, pk: &Pubkey) -> bool {
        self.inner.contains_key(pk)
    }

    /// Decode a Meteora pool account into its pricing slice. `fee_bps` is the
    /// effective fee supplied by config (dynamic fee not yet modelled).
    pub fn meteora_pool(&self, pool: &Pubkey, fee_bps: u64) -> Option<MeteoraPool> {
        let entry = self.inner.get(pool)?;
        let data = entry.value();
        Some(MeteoraPool {
            liquidity: read_u128_le(data, MET_OFF_LIQUIDITY)?,
            sqrt_min_price: read_u128_le(data, MET_OFF_SQRT_MIN)?,
            sqrt_max_price: read_u128_le(data, MET_OFF_SQRT_MAX)?,
            sqrt_price: read_u128_le(data, MET_OFF_SQRT_PRICE)?,
            fee_bps,
        })
    }

    /// SPL token amount of a vault account.
    pub fn spl_amount(&self, vault: &Pubkey) -> Option<u64> {
        let entry = self.inner.get(vault)?;
        read_u64_le(entry.value(), SPL_AMOUNT_OFFSET)
    }

    /// Build a Pump.fun pool from its two vaults (base = token, quote = WSOL).
    pub fn pump_pool(&self, token_vault: &Pubkey, wsol_vault: &Pubkey) -> Option<PumpPool> {
        let base = self.spl_amount(token_vault)?;
        let quote = self.spl_amount(wsol_vault)?;
        Some(PumpPool::new(base, quote))
    }

    /// Spawn the subscription task with reconnect/backoff.
    pub fn spawn_subscription(
        &self,
        endpoint: String,
        x_token: String,
        accounts: Vec<Pubkey>,
    ) {
        let inner = self.inner.clone();
        let slot = self.slot.clone();
        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                match run_stream(&endpoint, &x_token, &accounts, &inner, &slot).await {
                    Ok(()) => warn!("pool-state stream ended cleanly, reconnecting"),
                    Err(e) => warn!(error = %e, "pool-state stream error, reconnecting"),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
        });
    }
}

async fn run_stream(
    endpoint: &str,
    x_token: &str,
    accounts: &[Pubkey],
    cache: &Arc<DashMap<Pubkey, Vec<u8>>>,
    slot: &Arc<AtomicU64>,
) -> Result<()> {
    let mut client = GeyserGrpcClient::build_from_shared(endpoint.to_string())?
        .x_token(Some(x_token.to_string()))?
        .tls_config(yellowstone_grpc_client::ClientTlsConfig::new().with_native_roots())?
        .max_decoding_message_size(64 * 1024 * 1024)
        .connect()
        .await
        .context("pool-state gRPC connect failed")?;

    let mut accounts_filter: HashMap<String, SubscribeRequestFilterAccounts> = HashMap::new();
    accounts_filter.insert(
        "arb_pools".to_string(),
        SubscribeRequestFilterAccounts {
            account: accounts.iter().map(|p| p.to_string()).collect(),
            owner: vec![],
            filters: vec![],
            nonempty_txn_signature: None,
        },
    );

    let request = SubscribeRequest {
        accounts: accounts_filter,
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    };

    let (mut tx, mut stream) = client
        .subscribe_with_request(Some(request))
        .await
        .context("pool-state gRPC subscribe failed")?;

    info!(accounts = accounts.len(), "pool-state subscription active");

    while let Some(msg) = stream.next().await {
        let msg = msg.context("pool-state stream yielded error")?;
        match msg.update_oneof {
            Some(UpdateOneof::Account(a)) => {
                slot.store(a.slot, Ordering::Relaxed);
                if let Some(info) = a.account {
                    if let Ok(pk) = Pubkey::try_from(info.pubkey.as_slice()) {
                        cache.insert(pk, info.data);
                    }
                }
            }
            Some(UpdateOneof::Ping(_)) => {
                let _ = tx
                    .send(SubscribeRequest {
                        ping: Some(SubscribeRequestPing { id: 1 }),
                        ..Default::default()
                    })
                    .await;
            }
            _ => {}
        }
    }
    Ok(())
}
