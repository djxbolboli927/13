use solana_client::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::hash::Hash;
use std::sync::{Arc, RwLock};
use tokio::time::{interval, Duration};
use tracing::{debug, warn};

/// Single-writer / many-reader blockhash cache. The hot-path `get()` is
/// a tiny RwLock read (no contention with other readers); only the 300ms
/// refresh task takes the write lock briefly.
pub struct BlockhashCache {
    inner: Arc<RwLock<Hash>>,
}

impl BlockhashCache {
    pub fn new(rpc: Arc<RpcClient>) -> Self {
        // Use CONFIRMED commitment: a confirmed blockhash is voted by the
        // supermajority (canonical, not on a minority fork) yet only ~1-2 slots
        // old, so Jito's leaders always recognise it — this is what fixes the
        // "Blockhash not found" bundle rejections — while staying fresh enough
        // that the ~60s validity window is nearly full when the tx lands.
        let commitment = CommitmentConfig::confirmed();
        let initial = rpc
            .get_latest_blockhash_with_commitment(commitment)
            .map(|(h, _)| h)
            .unwrap_or_default();
        let inner = Arc::new(RwLock::new(initial));
        let shared = inner.clone();

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(300));
            loop {
                ticker.tick().await;
                let rpc_clone = rpc.clone();
                let result = tokio::task::spawn_blocking(move || {
                    rpc_clone
                        .get_latest_blockhash_with_commitment(commitment)
                        .map(|(h, _)| h)
                }).await;

                match result {
                    Ok(Ok(h)) => {
                        *shared.write().unwrap() = h;
                        debug!("blockhash refreshed");
                    }
                    Ok(Err(e)) => warn!(error = %e, "blockhash RPC failed"),
                    Err(e) => warn!(error = %e, "blockhash spawn_blocking panicked"),
                }
            }
        });

        Self { inner }
    }

    #[inline]
    pub fn get(&self) -> Hash {
        *self.inner.read().unwrap()
    }
}
