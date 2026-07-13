//! Wallet-transaction pool miner.
//!
//! The API-based discovery (GeckoTerminal/DexScreener) mostly surfaced stale or
//! rugged pools. A far better seed for an arb bot is to watch what the
//! *competitors* actually trade: pull a target wallet's most recent
//! transactions, extract every Pump.fun AMM and Meteora DAMM v2 pool it touched,
//! and add the ones that live on BOTH venues (a shared, tradeable pair) to the
//! bot AND to Metis via the normal `PoolManager` pipeline.
//!
//! This is the Rust port of the operator's `universal_extractor.py`, narrowed to
//! the two venues we arb. Decode indices mirror the script exactly:
//!   * Pump.fun AMM  — pool account = instruction account #0
//!   * Meteora DAMM v2 — pool account = instruction account #1
//! We walk both the top-level instructions and every inner instruction, so a
//! pool routed through Jupiter/aggregators is still found.
//!
//! Runs once at startup and then on a timer (default every 30 min). Only the
//! last ~1000 signatures are scanned: going further back tends to surface pools
//! that have since removed liquidity.

use anyhow::Result;
use serde_json::Value;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

use crate::dex_ids::{
    meteora_program, pumpfun_program, DexKind, METEORA_DAMM_V2_PROGRAM, PUMPFUN_AMM_PROGRAM,
};
use crate::discovery::decode_pool;
use crate::pool_manager::PoolManager;
use crate::pool_registry::{ArbPair, PoolInfo};

pub struct WalletMinerConfig {
    pub rpc_url: String,
    pub wallets: Vec<String>,
    pub interval: Duration,
    pub tx_limit: usize,
    pub min_pump_wsol_lamports: u64,
    pub min_meteora_wsol_lamports: u64,
}

pub struct WalletMiner {
    cfg: WalletMinerConfig,
    manager: Arc<PoolManager>,
    rpc: Arc<RpcClient>,
    http: reqwest::Client,
    /// Pools already fetched+decoded (added or ruled out) so repeat passes skip
    /// the on-chain read.
    seen_pools: HashSet<Pubkey>,
}

impl WalletMiner {
    pub fn new(cfg: WalletMinerConfig, manager: Arc<PoolManager>, rpc: Arc<RpcClient>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default();
        Self {
            cfg,
            manager,
            rpc,
            http,
            seen_pools: HashSet::new(),
        }
    }

    pub fn spawn(mut self) {
        tokio::spawn(async move {
            if self.cfg.wallets.is_empty() {
                return;
            }
            info!(
                wallets = self.cfg.wallets.len(),
                interval_s = self.cfg.interval.as_secs(),
                "wallet miner started"
            );
            loop {
                if let Err(e) = self.run_pass().await {
                    warn!(error = %e, "wallet miner pass failed");
                }
                tokio::time::sleep(self.cfg.interval).await;
            }
        });
    }

    /// One full pass over every configured wallet.
    async fn run_pass(&mut self) -> Result<()> {
        // 1) Collect candidate (pool, kind) across all wallets this pass.
        let mut candidates: HashMap<Pubkey, DexKind> = HashMap::new();
        for wallet in self.cfg.wallets.clone() {
            match self.scan_wallet(&wallet, &mut candidates).await {
                Ok(n) => info!(%wallet, pools_seen = n, "wallet scanned"),
                Err(e) => warn!(%wallet, error = %e, "wallet scan failed"),
            }
        }

        // 2) Decode the NEW candidate pool accounts and bucket by token mint.
        let pump_prog = pumpfun_program();
        let met_prog = meteora_program();
        let mut pumps: HashMap<Pubkey, PoolInfo> = HashMap::new();
        let mut meteoras: HashMap<Pubkey, PoolInfo> = HashMap::new();
        for (pool, _kind) in candidates {
            if self.seen_pools.contains(&pool) || self.manager.contains(&pool) {
                continue;
            }
            self.seen_pools.insert(pool);
            let rpc = self.rpc.clone();
            let acct = match tokio::task::spawn_blocking(move || rpc.get_account(&pool)).await {
                Ok(Ok(a)) => a,
                _ => continue,
            };
            if acct.owner != pump_prog && acct.owner != met_prog {
                continue;
            }
            if let Some(info) = decode_pool(pool, &acct.owner, &acct.data) {
                match info.kind {
                    DexKind::PumpFunAmm => {
                        pumps.insert(info.token_mint, info);
                    }
                    DexKind::MeteoraDammV2 => {
                        meteoras.insert(info.token_mint, info);
                    }
                }
            }
        }

        // 3) Pair by shared token and add those that pass the liquidity gates.
        let mut added = 0usize;
        for (token, pump) in pumps {
            let Some(meteora) = meteoras.get(&token).cloned() else {
                // One-sided so far (only Pump seen this pass). Left un-added; a
                // later pass may see its Meteora counterpart and pair it then.
                continue;
            };
            if !self.passes_liquidity(&pump, &meteora).await {
                continue;
            }
            info!(%token, "wallet miner: adding shared Pump/Meteora pool");
            let pair = ArbPair {
                token_mint: token,
                pump,
                meteora,
            };
            if let Err(e) = self.manager.add_pair(pair).await {
                warn!(%token, error = %e, "wallet miner add_pair failed");
            } else {
                added += 1;
            }
        }
        info!(added, "wallet miner pass complete");
        Ok(())
    }

    /// Liquidity / not-rugged gates (same thresholds as API discovery).
    async fn passes_liquidity(&self, pump: &PoolInfo, meteora: &PoolInfo) -> bool {
        let rpc = self.rpc.clone();
        let pump_wsol_vault = pump.wsol_vault();
        let met_pool = meteora.pool;
        let met_wsol_vault = meteora.wsol_vault();
        let min_pump = self.cfg.min_pump_wsol_lamports;
        let min_met = self.cfg.min_meteora_wsol_lamports;
        tokio::task::spawn_blocking(move || {
            if min_pump > 0 {
                let w = rpc
                    .get_account(&pump_wsol_vault)
                    .ok()
                    .and_then(|a| read_u64(&a.data, 64))
                    .unwrap_or(0);
                if w < min_pump {
                    return false;
                }
            }
            // Meteora liquidity@360 must be non-zero (not rugged).
            let liq = rpc
                .get_account(&met_pool)
                .ok()
                .and_then(|a| read_u128(&a.data, 360))
                .unwrap_or(0);
            if liq == 0 {
                return false;
            }
            if min_met > 0 {
                let w = rpc
                    .get_account(&met_wsol_vault)
                    .ok()
                    .and_then(|a| read_u64(&a.data, 64))
                    .unwrap_or(0);
                if w < min_met {
                    return false;
                }
            }
            true
        })
        .await
        .unwrap_or(false)
    }

    /// Scan one wallet: fetch its recent signatures, then each transaction, and
    /// record every Pump/Meteora pool it touched into `out`. Returns the count
    /// of distinct pools found for this wallet.
    async fn scan_wallet(
        &self,
        wallet: &str,
        out: &mut HashMap<Pubkey, DexKind>,
    ) -> Result<usize> {
        let sigs = self.fetch_signatures(wallet, self.cfg.tx_limit).await?;
        let start = out.len();
        for sig in sigs {
            match self.fetch_tx_pools(&sig).await {
                Ok(pools) => {
                    for (pk, kind) in pools {
                        out.entry(pk).or_insert(kind);
                    }
                }
                Err(_) => continue, // one bad tx never aborts the wallet
            }
        }
        Ok(out.len().saturating_sub(start))
    }

    /// `getSignaturesForAddress` (paged, up to `limit`).
    async fn fetch_signatures(&self, wallet: &str, limit: usize) -> Result<Vec<String>> {
        let mut sigs = Vec::new();
        let mut before: Option<String> = None;
        while sigs.len() < limit {
            let want = (limit - sigs.len()).min(1000);
            let mut opts = serde_json::json!({ "limit": want, "commitment": "confirmed" });
            if let Some(b) = &before {
                opts["before"] = Value::String(b.clone());
            }
            let resp = self
                .rpc_call("getSignaturesForAddress", serde_json::json!([wallet, opts]))
                .await?;
            let arr = match resp.get("result").and_then(|r| r.as_array()) {
                Some(a) if !a.is_empty() => a.clone(),
                _ => break,
            };
            for item in &arr {
                if let Some(s) = item.get("signature").and_then(|s| s.as_str()) {
                    sigs.push(s.to_string());
                }
            }
            before = arr
                .last()
                .and_then(|i| i.get("signature"))
                .and_then(|s| s.as_str())
                .map(|s| s.to_string());
            if arr.len() < want {
                break; // end of history
            }
        }
        sigs.truncate(limit);
        Ok(sigs)
    }

    /// Fetch a transaction (jsonParsed) and extract its Pump/Meteora pools from
    /// both the outer instructions and every inner instruction.
    async fn fetch_tx_pools(&self, sig: &str) -> Result<Vec<(Pubkey, DexKind)>> {
        let resp = self
            .rpc_call(
                "getTransaction",
                serde_json::json!([
                    sig,
                    { "encoding": "jsonParsed", "maxSupportedTransactionVersion": 0, "commitment": "confirmed" }
                ]),
            )
            .await?;
        let tx = match resp.get("result") {
            Some(v) if !v.is_null() => v,
            _ => return Ok(Vec::new()),
        };
        let mut out = Vec::new();
        let mut seen: HashSet<Pubkey> = HashSet::new();

        let mut consider = |prog: &str, accounts: &Value| {
            let (kind, idx) = if prog == PUMPFUN_AMM_PROGRAM {
                (DexKind::PumpFunAmm, 0usize)
            } else if prog == METEORA_DAMM_V2_PROGRAM {
                (DexKind::MeteoraDammV2, 1usize)
            } else {
                return;
            };
            if let Some(arr) = accounts.as_array() {
                if let Some(pk_str) = arr.get(idx).and_then(|a| a.as_str()) {
                    if let Ok(pk) = Pubkey::from_str(pk_str) {
                        if seen.insert(pk) {
                            out.push((pk, kind));
                        }
                    }
                }
            }
        };

        // Outer instructions.
        if let Some(ixs) = tx
            .get("transaction")
            .and_then(|t| t.get("message"))
            .and_then(|m| m.get("instructions"))
            .and_then(|i| i.as_array())
        {
            for ix in ixs {
                if let Some(prog) = ix.get("programId").and_then(|p| p.as_str()) {
                    consider(prog, ix.get("accounts").unwrap_or(&Value::Null));
                }
            }
        }
        // Inner instructions.
        if let Some(groups) = tx
            .get("meta")
            .and_then(|m| m.get("innerInstructions"))
            .and_then(|i| i.as_array())
        {
            for g in groups {
                if let Some(ixs) = g.get("instructions").and_then(|i| i.as_array()) {
                    for ix in ixs {
                        if let Some(prog) = ix.get("programId").and_then(|p| p.as_str()) {
                            consider(prog, ix.get("accounts").unwrap_or(&Value::Null));
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    /// One JSON-RPC POST to the configured RPC endpoint.
    async fn rpc_call(&self, method: &str, params: Value) -> Result<Value> {
        let body = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": method, "params": params
        });
        let resp = self
            .http
            .post(&self.cfg.rpc_url)
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        Ok(resp)
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
