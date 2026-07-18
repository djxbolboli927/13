use anyhow::{Context, Result};
use base64::Engine;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use solana_sdk::transaction::VersionedTransaction;
use tracing::{debug, info, warn};

/// Jito JSON-RPC client for bundle submission.
/// Sends bundles to MULTIPLE block engine endpoints concurrently.
pub struct JitoClient {
    http: Client,
    bundle_urls: Vec<String>,
}

#[derive(Serialize)]
struct SendBundleRpcRequest {
    jsonrpc: &'static str,
    id: u64,
    method: &'static str,
    params: (Vec<String>, SendBundleConfig),
}

#[derive(Serialize)]
struct SendBundleConfig {
    encoding: &'static str,
}

#[derive(Deserialize, Debug)]
struct RpcResponse {
    result: Option<String>,
    error: Option<RpcError>,
}

#[derive(Deserialize, Debug)]
struct RpcError {
    code: i64,
    message: String,
}

impl JitoClient {
    /// Create a Jito client that sends bundles to multiple endpoints concurrently.
    pub fn new(base_urls: &[String], uuid: &str) -> Self {
        let bundle_urls: Vec<String> = base_urls
            .iter()
            .map(|url| {
                format!(
                    "{}/api/v1/bundles?uuid={}",
                    url.trim_end_matches('/'),
                    uuid
                )
            })
            .collect();

        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(1))
            // 8 regional endpoints × concurrent bundle sends — keep 16
            // idle connections per region so consecutive sends reuse the
            // warm TLS session instead of paying ~30ms handshake cost.
            .pool_max_idle_per_host(16)
            .tcp_nodelay(true)
            .build()
            .expect("failed to build http client");

        info!(
            endpoints = bundle_urls.len(),
            "Jito multi-region client initialized"
        );

        Self { http, bundle_urls }
    }

    /// Send a single-transaction bundle to ALL Jito endpoints concurrently.
    /// Returns the first successful bundle ID.
    ///
    /// Every region's POST runs on its OWN detached task, so returning early on
    /// the first acceptance does NOT cancel the remaining in-flight requests —
    /// the bundle genuinely reaches all 8 regions (previously the early return
    /// dropped the pending futures and only the fastest region ever received it).
    pub async fn send_bundle(&self, tx: &VersionedTransaction) -> Result<String> {
        let tx_bytes = bincode::serialize(tx).context("failed to serialize transaction")?;
        let tx_base64 = base64::engine::general_purpose::STANDARD.encode(&tx_bytes);

        let (res_tx, mut res_rx) =
            tokio::sync::mpsc::channel::<Result<String>>(self.bundle_urls.len().max(1));
        for url in &self.bundle_urls {
            let http = self.http.clone();
            let url = url.clone();
            let b64 = tx_base64.clone();
            let res_tx = res_tx.clone();
            tokio::spawn(async move {
                let r = Self::send_to_endpoint(http, &url, &b64).await;
                let _ = res_tx.send(r).await;
            });
        }
        drop(res_tx);

        let mut last_err = None;
        while let Some(result) = res_rx.recv().await {
            match result {
                Ok(bundle_id) => return Ok(bundle_id), // other regions keep sending
                Err(e) => {
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no Jito endpoints configured")))
    }

    /// Send bundle to a single endpoint.
    async fn send_to_endpoint(http: Client, url: &str, tx_base64: &str) -> Result<String> {
        let request = SendBundleRpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "sendBundle",
            params: (
                vec![tx_base64.to_string()],
                SendBundleConfig { encoding: "base64" },
            ),
        };

        let resp = http
            .post(url)
            .json(&request)
            .send()
            .await
            .with_context(|| format!("Jito sendBundle to {} failed", url))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            warn!(endpoint = url, http_status = %status, body = %body, "Jito HTTP error");
            anyhow::bail!("Jito HTTP error at {}: {} -- {}", url, status, body);
        }

        let rpc_resp: RpcResponse = resp
            .json()
            .await
            .context("failed to parse Jito response")?;

        if let Some(err) = rpc_resp.error {
            warn!(endpoint = url, code = err.code, message = %err.message, "Jito RPC error");
            anyhow::bail!(
                "Jito RPC error at {}: code={}, message={}",
                url,
                err.code,
                err.message
            );
        }

        let bundle_id = rpc_resp
            .result
            .ok_or_else(|| anyhow::anyhow!("Jito returned no result and no error"))?;

        debug!(endpoint = url, bundle_id = %bundle_id, "bundle accepted");
        Ok(bundle_id)
    }
}
