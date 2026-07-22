//! Hot account cache fed by a Yellowstone gRPC subscription.
//!
//! This is the same data Metis consumes. By keeping a parallel copy in our
//! own process we can hand it to LiteSVM for pre-flight simulation without
//! any RPC round-trip on the hot path (a getMultipleAccounts would add
//! 20-50ms and make simulation useless).
//!
//! The cache subscribes once at startup with two filter entries:
//!   1. all DEX program ids from `program_registry::PROGRAMS` (owner filter)
//!      -> every pool account owned by those programs streams in
//!   2. the user's WSOL ATA (specific account filter)
//!      -> so the simulated tx can read / debit it
//!
//! Missing entries (token mints, intermediate ATAs) are fetched lazily from
//! RPC the first time they're needed and then cached forever (their data
//! rarely changes).

use anyhow::{Context, Result};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use solana_account::Account;
use solana_address::Address;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterAccounts, SubscribeRequestPing,
};

/// Shared concurrent cache. Cloning an `AccountCache` is cheap; it's just
/// an Arc-wrapped DashMap plus an Arc-wrapped RpcClient for fallbacks.
#[derive(Clone)]
pub struct AccountCache {
    /// Base full accounts (owner/lamports/executable/data). Seeded once from
    /// RPC; for live-moving pool accounts the `data` field is OVERLAID from
    /// `live` on every read so the sim sees fresh state without any RPC.
    inner: Arc<DashMap<Pubkey, Account>>,
    rpc: Arc<RpcClient>,
    /// Slot the simulator reads to set LiteSVM's Clock.slot — no RPC call.
    /// When `live` is set this shares the pool-state gRPC's slot counter.
    stream_slot: Arc<AtomicU64>,
    /// LIVE raw-bytes source: the shred-arb pool-state cache, already fed by
    /// the `[shred_arb].pool_state_endpoint` gRPC. The sim reads the SAME state
    /// the arb math priced off — no second subscription, no re-fetch.
    live: Option<Arc<crate::pool_state::PoolStateCache>>,
    /// Global RPC rate gate: at most one fetch per `rpc_min_interval` across all
    /// callers/workers, so startup prefetch + lazy fetches never trip the shyft
    /// 429 limit. Holds the timestamp of the last RPC call.
    rpc_gate: Arc<std::sync::Mutex<std::time::Instant>>,
    rpc_min_interval: Duration,
}

impl AccountCache {
    pub fn new(rpc: Arc<RpcClient>) -> Self {
        Self {
            inner: Arc::new(DashMap::with_capacity(4096)),
            rpc,
            stream_slot: Arc::new(AtomicU64::new(0)),
            live: None,
            // 5 RPC calls/sec (200ms apart) — the shyft plan's ceiling.
            rpc_gate: Arc::new(std::sync::Mutex::new(
                std::time::Instant::now() - Duration::from_secs(1),
            )),
            rpc_min_interval: Duration::from_millis(200),
        }
    }

    /// Build a cache that overlays live raw bytes from the shred-arb
    /// `PoolStateCache` and shares its slot counter. This is the ONLY source
    /// of live pool state the simulator uses — no owner-wide Yellowstone
    /// firehose is started.
    pub fn with_live(
        rpc: Arc<RpcClient>,
        live: Arc<crate::pool_state::PoolStateCache>,
        rpc_calls_per_sec: u32,
    ) -> Self {
        let interval = Duration::from_millis(1000 / rpc_calls_per_sec.max(1) as u64);
        Self {
            inner: Arc::new(DashMap::with_capacity(4096)),
            rpc,
            stream_slot: live.slot_handle(),
            live: Some(live),
            rpc_gate: Arc::new(std::sync::Mutex::new(
                std::time::Instant::now() - Duration::from_secs(1),
            )),
            rpc_min_interval: interval,
        }
    }

    /// The latest slot seen from the Yellowstone stream. The sim pool reads
    /// this instead of making its own `get_slot` RPC.
    pub fn stream_slot(&self) -> Arc<AtomicU64> {
        self.stream_slot.clone()
    }

    /// Seed the stream slot with an initial value (from RPC at startup)
    /// so sims have a valid Clock.slot before the first Yellowstone message.
    pub fn seed_slot(&self, slot: u64) {
        self.stream_slot.store(slot, Ordering::Relaxed);
    }

    /// Fast path: read from the hot cache. Returns None if not yet populated.
    ///
    /// When a live `PoolStateCache` is attached, the `data` field is OVERLAID
    /// with the freshest bytes for that account (pool / vault / mint), keeping
    /// the RPC-seeded owner/lamports/executable. This is what makes the sim run
    /// against the exact live state the arb math priced off. If no base account
    /// has been seeded yet, returns `None` so the caller RPC-seeds it once; the
    /// next read then overlays the live data on top.
    #[inline]
    pub fn get(&self, pubkey: &Pubkey) -> Option<Account> {
        let base = self.inner.get(pubkey).map(|v| v.value().clone());
        if let Some(live) = &self.live {
            if let Some(data) = live.account_data(pubkey) {
                if let Some(mut acct) = base {
                    acct.data = data;
                    return Some(acct);
                }
                return None; // need an owner/lamports seed first
            }
        }
        base
    }

    /// Sleep just long enough that RPC calls stay under the configured rate
    /// (default 5/sec). Serialises all callers through one gate.
    fn rate_gate(&self) {
        let mut last = self.rpc_gate.lock().unwrap();
        let elapsed = last.elapsed();
        if elapsed < self.rpc_min_interval {
            std::thread::sleep(self.rpc_min_interval - elapsed);
        }
        *last = std::time::Instant::now();
    }

    /// Slow path used only during startup warm-up and for rarely-changing
    /// accounts (token mints, ALTs, global config) not in the live cache.
    /// Rate-limited so bursty prefetch never trips the RPC 429 limit.
    pub fn get_or_fetch(&self, pubkey: &Pubkey) -> Result<Account> {
        if let Some(a) = self.get(pubkey) {
            return Ok(a);
        }
        self.rate_gate();
        let acct = self
            .rpc
            .get_account(pubkey)
            .with_context(|| format!("RPC fetch of {pubkey} failed"))?;
        let account = Account {
            lamports: acct.lamports,
            data: acct.data,
            owner: Address::from(acct.owner.to_bytes()),
            executable: acct.executable,
            rent_epoch: acct.rent_epoch,
        };
        self.inner.insert(*pubkey, account.clone());
        Ok(account)
    }

    /// Pre-fetch a batch of accounts (used at startup to warm up mints, ATAs,
    /// etc. that won't naturally stream in via the owner filter).
    pub fn prefetch(&self, pubkeys: &[Pubkey]) {
        for pk in pubkeys {
            if let Err(e) = self.get_or_fetch(pk) {
                warn!(pubkey = %pk, error = %e, "prefetch miss");
            }
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Spawn the Yellowstone subscription task. Reconnects with exponential
    /// backoff if the stream drops.
    pub fn spawn_subscription(
        &self,
        endpoint: String,
        x_token: String,
        dex_program_ids: Vec<String>,
        extra_accounts: Vec<Pubkey>,
    ) {
        let cache = self.inner.clone();
        let stream_slot = self.stream_slot.clone();
        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                match run_stream(
                    &endpoint,
                    &x_token,
                    &dex_program_ids,
                    &extra_accounts,
                    &cache,
                    &stream_slot,
                )
                .await
                {
                    Ok(()) => {
                        warn!("gRPC account stream ended cleanly, reconnecting");
                    }
                    Err(e) => {
                        warn!(error = %e, "gRPC account stream error, reconnecting");
                    }
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
    dex_program_ids: &[String],
    extra_accounts: &[Pubkey],
    cache: &Arc<DashMap<Pubkey, Account>>,
    stream_slot: &Arc<AtomicU64>,
) -> Result<()> {
    let mut client = GeyserGrpcClient::build_from_shared(endpoint.to_string())?
        .x_token(Some(x_token.to_string()))?
        .tls_config(yellowstone_grpc_client::ClientTlsConfig::new().with_native_roots())?
        .max_decoding_message_size(64 * 1024 * 1024)
        .connect()
        .await
        .context("gRPC connect failed")?;

    info!(endpoint, "gRPC connected");

    let mut accounts_filter: HashMap<String, SubscribeRequestFilterAccounts> =
        HashMap::new();

    accounts_filter.insert(
        "dex_pools".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: dex_program_ids.to_vec(),
            filters: vec![],
            nonempty_txn_signature: None,
        },
    );

    if !extra_accounts.is_empty() {
        accounts_filter.insert(
            "extras".to_string(),
            SubscribeRequestFilterAccounts {
                account: extra_accounts.iter().map(|p| p.to_string()).collect(),
                owner: vec![],
                filters: vec![],
                nonempty_txn_signature: None,
            },
        );
    }

    let request = SubscribeRequest {
        slots: HashMap::new(),
        accounts: accounts_filter,
        transactions: HashMap::new(),
        transactions_status: HashMap::new(),
        entry: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        commitment: Some(CommitmentLevel::Processed as i32),
        accounts_data_slice: vec![],
        ping: None,
        from_slot: None,
    };

    let (mut tx, mut stream) = client
        .subscribe_with_request(Some(request))
        .await
        .context("gRPC subscribe failed")?;

    info!("gRPC subscription active; waiting for account updates");

    let mut count: u64 = 0;
    while let Some(msg) = stream.next().await {
        let msg = msg.context("stream yielded error")?;
        match msg.update_oneof {
            Some(UpdateOneof::Account(a)) => {
                stream_slot.store(a.slot, Ordering::Relaxed);

                if let Some(info) = a.account {
                    let pk = match Pubkey::try_from(info.pubkey.as_slice()) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };
                    let owner_bytes: [u8; 32] = info.owner.as_slice()
                        .try_into()
                        .unwrap_or([0u8; 32]);
                    let account = Account {
                        lamports: info.lamports,
                        data: info.data,
                        owner: Address::from(owner_bytes),
                        executable: info.executable,
                        rent_epoch: info.rent_epoch,
                    };
                    cache.insert(pk, account);
                    count += 1;
                    if count % 10_000 == 0 {
                        debug!(count, size = cache.len(), "cache growth");
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
