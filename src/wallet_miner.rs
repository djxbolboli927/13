//! Wallet-transaction pool miner.
//!
//! The API-based discovery (GeckoTerminal/DexScreener) mostly surfaced stale or
//! rugged pools. A far better seed for an arb bot is to watch what the
//! *competitors* actually trade: pull a target wallet's most recent
//! transactions, extract every Pump.fun AMM and Meteora DAMM v2 pool it touched,
//! and add the ones that live on BOTH venues (a shared, tradeable pair) to the
//! bot AND to Metis via the normal `PoolManager` pipeline.
//!
//! Crucially we also capture the **Address Lookup Table** each competitor tx
//! used for those pools (mirroring `find_pool_alt` in `universal_extractor.py`):
//! that ALT already contains the pool/vault/authority accounts of the exact
//! route we trade, so registering it with Metis (and folding it into our own tx)
//! compresses those accounts from 32 static bytes to a 1-byte index — the
//! difference between a 2-hop tx fitting under 1232 bytes and not.
//!
//! Decode indices mirror the script exactly:
//!   * Pump.fun AMM  — pool account = instruction account #0
//!   * Meteora DAMM v2 — pool account = instruction account #1
//! We walk both the top-level instructions and every inner instruction.
//!
//! First pass is a full scan of the last ~1000 signatures; every pass after
//! that is INCREMENTAL (only signatures newer than the last one seen), so the
//! repeat cost is tiny and it can run every few minutes.

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
use crate::transaction::deserialize_alt_addresses;

/// Meteora Dynamic Bonding Curve program — the 3rd allowed market.
const METEORA_DBC_PROGRAM: &str = "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN";

/// The exactly-three market programs a token may trade on. Any pool owned by a
/// different program disqualifies the whole token.
fn allowed_market_programs() -> [Pubkey; 3] {
    [
        pumpfun_program(),                              // Pump.fun AMM (pAMMBay…)
        meteora_program(),                              // Meteora DAMM v2 (cpamdp…)
        Pubkey::from_str(METEORA_DBC_PROGRAM).unwrap(), // Meteora DBC (dbcij3…)
    ]
}

pub struct WalletMinerConfig {
    /// One or more RPC endpoints to round-robin across (each rate-gated). The
    /// miner never re-fetches pool state on the hot path — this is background
    /// competitor discovery, kept entirely off the trading RPC.
    pub rpc_urls: Vec<String>,
    pub wallets: Vec<String>,
    pub interval: Duration,
    pub tx_limit: usize,
    pub min_pump_wsol_lamports: u64,
    pub min_meteora_wsol_lamports: u64,
    /// Max JSON-RPC calls/sec PER endpoint (shyft caps at 5).
    pub rpc_calls_per_sec: u32,
    /// DexScreener token-pairs URL (with `{mint}` placeholder) used to enforce
    /// the 3-market restriction. Empty = restriction off (allow all).
    pub token_pairs_url: String,
}

/// One RPC endpoint plus its own rate gate (last-call timestamp). Round-robin
/// across a Vec of these spreads load and keeps each under its 429 ceiling.
struct RpcEndpoint {
    url: String,
    min_interval: Duration,
    gate: tokio::sync::Mutex<std::time::Instant>,
}

impl RpcEndpoint {
    /// Wait until this endpoint is allowed to make its next call, then stamp it.
    async fn acquire(&self) {
        let mut last = self.gate.lock().await;
        let elapsed = last.elapsed();
        if elapsed < self.min_interval {
            tokio::time::sleep(self.min_interval - elapsed).await;
        }
        *last = std::time::Instant::now();
    }
}

pub struct WalletMiner {
    cfg: WalletMinerConfig,
    manager: Arc<PoolManager>,
    rpc: Arc<RpcClient>,
    http: reqwest::Client,
    /// Round-robin pool of rate-gated RPC endpoints for JSON-RPC scanning.
    endpoints: Vec<Arc<RpcEndpoint>>,
    next_endpoint: std::sync::atomic::AtomicUsize,
    /// Pools already fetched+decoded (added or ruled out) so repeat passes skip
    /// the on-chain read.
    seen_pools: HashSet<Pubkey>,
    /// Newest signature already processed per wallet — subsequent passes fetch
    /// only signatures newer than this (incremental, cheap).
    newest_sig: HashMap<String, String>,
    /// Cache of ALT → its address set, so `find_pool_alt` reads each table once.
    alt_members: HashMap<Pubkey, HashSet<Pubkey>>,
}

/// A candidate pool found in a competitor tx: its venue and the ALT keys that tx
/// referenced (one of which almost certainly covers this pool's route accounts).
struct Candidate {
    kind: DexKind,
    alt_keys: Vec<Pubkey>,
}

impl WalletMiner {
    pub fn new(cfg: WalletMinerConfig, manager: Arc<PoolManager>, rpc: Arc<RpcClient>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default();
        let min_interval =
            Duration::from_millis(1000 / cfg.rpc_calls_per_sec.max(1) as u64);
        let endpoints: Vec<Arc<RpcEndpoint>> = cfg
            .rpc_urls
            .iter()
            .filter(|u| !u.trim().is_empty())
            .map(|u| {
                Arc::new(RpcEndpoint {
                    url: u.clone(),
                    min_interval,
                    // Stagger so the first calls don't all fire at once.
                    gate: tokio::sync::Mutex::new(
                        std::time::Instant::now() - Duration::from_secs(1),
                    ),
                })
            })
            .collect();
        Self {
            cfg,
            manager,
            rpc,
            http,
            endpoints,
            next_endpoint: std::sync::atomic::AtomicUsize::new(0),
            seen_pools: HashSet::new(),
            newest_sig: HashMap::new(),
            alt_members: HashMap::new(),
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
            let mut first = true;
            loop {
                if let Err(e) = self.run_pass(first).await {
                    warn!(error = %e, "wallet miner pass failed");
                }
                first = false;
                tokio::time::sleep(self.cfg.interval).await;
            }
        });
    }

    /// One full pass over every configured wallet. `full` = scan the whole
    /// `tx_limit` window (startup); otherwise only signatures newer than the
    /// last one seen.
    async fn run_pass(&mut self, full: bool) -> Result<()> {
        // 1) Collect candidate pools (+ their tx's ALT keys) across all wallets.
        let mut candidates: HashMap<Pubkey, Candidate> = HashMap::new();
        for wallet in self.cfg.wallets.clone() {
            match self.scan_wallet(&wallet, full, &mut candidates).await {
                Ok(n) => info!(%wallet, pools_seen = n, full, "wallet scanned"),
                Err(e) => warn!(%wallet, error = %e, "wallet scan failed"),
            }
        }

        // 2) Decode the NEW candidate pool accounts, attach the covering ALT, and
        //    bucket by token mint.
        let pump_prog = pumpfun_program();
        let met_prog = meteora_program();
        let mut pumps: HashMap<Pubkey, PoolInfo> = HashMap::new();
        let mut meteoras: HashMap<Pubkey, PoolInfo> = HashMap::new();
        for (pool, cand) in candidates {
            if self.seen_pools.contains(&pool) || self.manager.contains_pool(&pool) {
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
            let Some(mut info) = decode_pool(pool, &acct.owner, &acct.data) else {
                continue;
            };
            // Golden ALT: whichever of this tx's tables actually contains the pool
            // (and thus its route accounts). This is what makes our tx fit.
            info.alt = self.find_pool_alt(&pool, &cand.alt_keys).await;
            match cand.kind {
                DexKind::PumpFunAmm => {
                    pumps.insert(info.token_mint, info);
                }
                DexKind::MeteoraDammV2 => {
                    meteoras.insert(info.token_mint, info);
                }
            }
        }

        // 3) Pair by shared token and add those that pass the liquidity gates.
        let mut added = 0usize;
        for (token, pump) in pumps {
            let Some(meteora) = meteoras.get(&token).cloned() else {
                continue; // one-sided this pass; a later pass may pair it
            };
            // 3-market restriction: skip tokens that trade anywhere outside
            // Pump.fun / Meteora (busy multi-market tokens wreck Pump prediction).
            if !self.only_allowed_markets(&token).await {
                continue;
            }
            if !self.passes_liquidity(&pump, &meteora).await {
                continue;
            }
            info!(%token, pump_alt = ?pump.alt, met_alt = ?meteora.alt,
                  "wallet miner: adding shared Pump/Meteora pool");
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

    /// True ONLY if EVERY on-chain market for `mint` is one of the exactly-three
    /// allowed programs: Pump.fun AMM, Meteora DAMM v2, Meteora DBC. Any other
    /// venue (Meteora DLMM, Meteora Pools, Orca, Raydium, …) rejects the whole
    /// token — those multi-market tokens draw the 30-buys-per-block arb bots
    /// whose churn wrecks our Pump state prediction and reverts us.
    ///
    /// The check is by PROGRAM OWNER (authoritative), not the DexScreener
    /// `dexId` string (which lumps all four Meteora products under "meteora").
    /// One `getMultipleAccounts` RPC verifies every pair at once. Rejects HARD
    /// on any doubt (empty/failed lookup returns false) so a foreign market is
    /// never let through — correctness beats coverage here.
    async fn only_allowed_markets(&self, mint: &Pubkey) -> bool {
        if self.cfg.token_pairs_url.is_empty() {
            return true; // restriction explicitly disabled
        }
        let url = self.cfg.token_pairs_url.replace("{mint}", &mint.to_string());
        // FAIL OPEN on any inability to check (empty URL, API/RPC error, no
        // data): we only REJECT when we POSITIVELY see a market owned by a
        // program outside the allowed three. This keeps competitor-wallet
        // mining working like before while still filtering the multi-market
        // tokens we CAN identify.
        let body: Value = match self.http.get(&url).send().await.and_then(|r| r.error_for_status()) {
            Ok(r) => match r.json().await {
                Ok(v) => v,
                Err(_) => return true,
            },
            Err(_) => return true,
        };
        let mut pair_pks: Vec<Pubkey> = Vec::new();
        if let Some(pairs) = body.get("pairs").and_then(|p| p.as_array()) {
            for pair in pairs {
                if let Some(a) = pair.get("pairAddress").and_then(|a| a.as_str()) {
                    if let Ok(pk) = Pubkey::from_str(a) {
                        pair_pks.push(pk);
                    }
                }
            }
        }
        if pair_pks.is_empty() {
            return true; // no market data → allow (fail open)
        }
        let rpc = self.rpc.clone();
        let owners = match tokio::task::spawn_blocking(move || rpc.get_multiple_accounts(&pair_pks)).await {
            Ok(Ok(v)) => v,
            _ => return true, // RPC failed → allow (fail open)
        };
        let allowed = allowed_market_programs();
        for acct in owners.iter().flatten() {
            if !allowed.contains(&acct.owner) {
                info!(%mint, owner = %acct.owner, "token rejected: market outside the 3 allowed programs");
                return false; // POSITIVELY identified a foreign market → reject
            }
        }
        true
    }

    /// Pick the ALT to attach to this pool. Prefer the table that literally
    /// contains the pool pubkey; if the pool itself was passed static in the
    /// competitor tx (its vaults still live in the route ALT), fall back to the
    /// LARGEST referenced table — the one most likely to cover the route
    /// accounts and give us the compression we need.
    async fn find_pool_alt(&mut self, pool: &Pubkey, alt_keys: &[Pubkey]) -> Option<Pubkey> {
        for alt in alt_keys {
            if self.alt_contains(alt, pool).await {
                return Some(*alt);
            }
        }
        // Fallback: largest table among those the tx referenced.
        let mut best: Option<(Pubkey, usize)> = None;
        for alt in alt_keys {
            self.alt_contains(alt, pool).await; // ensures it's cached
            let n = self.alt_members.get(alt).map(|s| s.len()).unwrap_or(0);
            if best.map(|(_, bn)| n > bn).unwrap_or(true) {
                best = Some((*alt, n));
            }
        }
        best.map(|(a, _)| a)
    }

    async fn alt_contains(&mut self, alt: &Pubkey, needle: &Pubkey) -> bool {
        if !self.alt_members.contains_key(alt) {
            let rpc = self.rpc.clone();
            let alt_pk = *alt;
            let members = tokio::task::spawn_blocking(move || {
                rpc.get_account(&alt_pk)
                    .ok()
                    .and_then(|a| deserialize_alt_addresses(&a.data).ok())
                    .map(|v| v.into_iter().collect::<HashSet<Pubkey>>())
                    .unwrap_or_default()
            })
            .await
            .unwrap_or_default();
            self.alt_members.insert(*alt, members);
        }
        self.alt_members
            .get(alt)
            .map(|s| s.contains(needle))
            .unwrap_or(false)
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

    /// Scan one wallet: fetch its recent signatures (full window or only those
    /// newer than the last seen), then each transaction, recording every
    /// Pump/Meteora pool it touched (with the tx's ALT keys) into `out`.
    async fn scan_wallet(
        &mut self,
        wallet: &str,
        full: bool,
        out: &mut HashMap<Pubkey, Candidate>,
    ) -> Result<usize> {
        let until = if full {
            None
        } else {
            self.newest_sig.get(wallet).cloned()
        };
        let limit = if full { self.cfg.tx_limit } else { self.cfg.tx_limit.min(1000) };
        let sigs = self.fetch_signatures(wallet, limit, until.as_deref()).await?;
        // Remember the newest signature for the next incremental pass.
        if let Some(newest) = sigs.first() {
            self.newest_sig.insert(wallet.to_string(), newest.clone());
        }
        let start = out.len();
        for sig in &sigs {
            match self.fetch_tx_pools(sig).await {
                Ok((pools, alt_keys)) => {
                    for (pk, kind) in pools {
                        out.entry(pk).or_insert(Candidate {
                            kind,
                            alt_keys: alt_keys.clone(),
                        });
                    }
                }
                Err(_) => continue, // one bad tx never aborts the wallet
            }
        }
        Ok(out.len().saturating_sub(start))
    }

    /// `getSignaturesForAddress` (newest first, paged). If `until` is set, only
    /// signatures newer than it are returned.
    async fn fetch_signatures(
        &self,
        wallet: &str,
        limit: usize,
        until: Option<&str>,
    ) -> Result<Vec<String>> {
        let mut sigs = Vec::new();
        let mut before: Option<String> = None;
        while sigs.len() < limit {
            let want = (limit - sigs.len()).min(1000);
            let mut opts = serde_json::json!({ "limit": want, "commitment": "confirmed" });
            if let Some(b) = &before {
                opts["before"] = Value::String(b.clone());
            }
            if let Some(u) = until {
                opts["until"] = Value::String(u.to_string());
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
                break;
            }
        }
        sigs.truncate(limit);
        Ok(sigs)
    }

    /// Fetch a transaction (jsonParsed) and extract its Pump/Meteora pools plus
    /// the ALT keys it referenced.
    async fn fetch_tx_pools(&self, sig: &str) -> Result<(Vec<(Pubkey, DexKind)>, Vec<Pubkey>)> {
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
            _ => return Ok((Vec::new(), Vec::new())),
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

        let message = tx
            .get("transaction")
            .and_then(|t| t.get("message"));

        // Outer instructions.
        if let Some(ixs) = message
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

        // ALT keys referenced by the tx (v0 addressTableLookups).
        let mut alt_keys = Vec::new();
        if let Some(lookups) = message
            .and_then(|m| m.get("addressTableLookups"))
            .and_then(|l| l.as_array())
        {
            for l in lookups {
                if let Some(k) = l.get("accountKey").and_then(|k| k.as_str()) {
                    if let Ok(pk) = Pubkey::from_str(k) {
                        alt_keys.push(pk);
                    }
                }
            }
        }
        Ok((out, alt_keys))
    }

    /// One JSON-RPC POST. Picks the next endpoint round-robin and waits on that
    /// endpoint's rate gate first, so no single endpoint exceeds its 429 limit.
    /// On a 429 it retries once on the NEXT endpoint (which has its own gate).
    async fn rpc_call(&self, method: &str, params: Value) -> Result<Value> {
        use std::sync::atomic::Ordering;
        let body = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": method, "params": params
        });
        let n = self.endpoints.len();
        if n == 0 {
            return Err(anyhow::anyhow!("wallet miner has no rpc endpoints configured"));
        }
        let start = self.next_endpoint.fetch_add(1, Ordering::Relaxed);
        // Try each endpoint at most once (handles a transient 429 by rotating).
        let attempts = n.max(1);
        let mut last_err: Option<anyhow::Error> = None;
        for i in 0..attempts {
            let ep = &self.endpoints[(start + i) % n];
            ep.acquire().await;
            match self
                .http
                .post(&ep.url)
                .json(&body)
                .send()
                .await
                .and_then(|r| r.error_for_status())
            {
                Ok(resp) => match resp.json::<Value>().await {
                    Ok(v) => return Ok(v),
                    Err(e) => last_err = Some(e.into()),
                },
                Err(e) => {
                    // 429 (or other) → rotate to the next endpoint and retry.
                    last_err = Some(e.into());
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no rpc endpoints configured")))
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
