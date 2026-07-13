//! Automatic discovery of new shared Pump.fun / Meteora pools.
//!
//! Strategy (API-based, wallet-agnostic — competitors rotate wallets so we do
//! NOT watch them):
//!   1. Poll a "new pools" feed (GeckoTerminal) to learn recently-created
//!      tokens on Solana.
//!   2. For each new token, ask DexScreener for every pair it trades in.
//!   3. If the token has BOTH a Pump.fun AMM pool and a Meteora DAMM v2 pool,
//!      read those pool accounts on-chain, decode them, and hand the pair to
//!      the `PoolManager` which registers it everywhere (Metis, gRPC, shreds,
//!      ATA) at runtime.
//!
//! The on-chain owner of each pool account is the source of truth for which
//! DEX it belongs to — API `dexId` labels are only a pre-filter. This keeps the
//! classifier robust and makes it straightforward to extend to more DEXes
//! later: add another `decode_*` arm keyed on the program owner.
//!
//! Latency-tolerant by design (a few seconds is fine — this is arbitrage, not
//! sniping) and rate-limit-tolerant.

use anyhow::Result;
use serde_json::Value;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::dex_ids::{meteora_program, pumpfun_program, wsol_mint, DexKind};
use crate::pool_manager::PoolManager;
use crate::pool_registry::{ArbPair, PoolInfo};

// ── Pump.fun AMM (PumpSwap) Pool account offsets ─────────────────────────────
// 8 disc | 1 pool_bump | 2 index | 32 creator | 32 base_mint | 32 quote_mint |
// 32 lp_mint | 32 pool_base_token_account | 32 pool_quote_token_account | ...
const PUMP_OFF_BASE_MINT: usize = 43;
const PUMP_OFF_QUOTE_MINT: usize = 75;
const PUMP_OFF_BASE_VAULT: usize = 139;
const PUMP_OFF_QUOTE_VAULT: usize = 171;

// ── Meteora DAMM v2 Pool account offsets ─────────────────────────────────────
// pool_fees occupies 8..168; then token_a_mint, token_b_mint, token_a_vault,
// token_b_vault, whitelisted_vault, partner, liquidity@360 (cross-checked
// against pool_state.rs which reads liquidity at 360).
const MET_OFF_MINT_A: usize = 168;
const MET_OFF_MINT_B: usize = 200;
const MET_OFF_VAULT_A: usize = 232;
const MET_OFF_VAULT_B: usize = 264;

pub struct DiscoveryConfig {
    pub interval: Duration,
    pub new_pools_url: String,
    /// DexScreener token endpoint with a literal `{mint}` placeholder.
    pub token_pairs_url: String,
    /// Feed URLs scanned ONCE at startup to seed hot/top/trending tokens.
    pub seed_urls: Vec<String>,
    /// Max tokens resolved during the startup bootstrap.
    pub bootstrap_max: usize,
}

pub struct Discovery {
    cfg: DiscoveryConfig,
    manager: Arc<PoolManager>,
    rpc: Arc<RpcClient>,
    http: reqwest::Client,
    /// Tokens we have already resolved (added or ruled out) — avoids re-hitting
    /// the APIs and re-decoding on every poll.
    seen_tokens: HashSet<Pubkey>,
}

fn read_pk(data: &[u8], off: usize) -> Option<Pubkey> {
    data.get(off..off + 32)
        .map(|s| Pubkey::new_from_array(s.try_into().unwrap()))
}

/// Extract base+quote token mints from a GeckoTerminal-shaped pools response
/// (`data[].relationships.{base,quote}_token.data.id == "solana_<mint>"`).
/// WSOL is skipped. Shared by the new-pools feed and the startup seed feeds.
fn extract_mints(body: &Value) -> Vec<Pubkey> {
    let mut out = Vec::new();
    if let Some(arr) = body.get("data").and_then(|d| d.as_array()) {
        for item in arr {
            for side in ["base_token", "quote_token"] {
                if let Some(id) = item
                    .get("relationships")
                    .and_then(|r| r.get(side))
                    .and_then(|t| t.get("data"))
                    .and_then(|d| d.get("id"))
                    .and_then(|s| s.as_str())
                {
                    let mint_str = id.rsplit('_').next().unwrap_or(id);
                    if let Ok(pk) = Pubkey::from_str(mint_str) {
                        if pk != wsol_mint() {
                            out.push(pk);
                        }
                    }
                }
            }
        }
    }
    out
}

/// Build a `PoolInfo` from a WSOL-paired pool given its mints and vaults.
fn make_pool_info(
    kind: DexKind,
    pool: Pubkey,
    mint_a: Pubkey,
    mint_b: Pubkey,
    vault_a: Pubkey,
    vault_b: Pubkey,
) -> Option<PoolInfo> {
    let wsol = wsol_mint();
    let (token_mint, token_is_a) = if mint_b == wsol {
        (mint_a, true)
    } else if mint_a == wsol {
        (mint_b, false)
    } else {
        return None; // not a WSOL pair — can't arb against SOL
    };
    Some(PoolInfo {
        kind,
        pool,
        vault_a,
        vault_b,
        mint_a,
        mint_b,
        alt: None, // Metis supplies the ALT in its quotes; shred detection uses
        // the static pool key, so an unknown ALT here is harmless.
        token_mint,
        token_is_a,
    })
}

/// Decode a pool account into `PoolInfo`, dispatching on the program `owner`.
/// Returns `None` for programs we don't (yet) support or malformed data.
fn decode_pool(pool: Pubkey, owner: &Pubkey, data: &[u8]) -> Option<PoolInfo> {
    match DexKind::from_owner(owner)? {
        DexKind::PumpFunAmm => make_pool_info(
            DexKind::PumpFunAmm,
            pool,
            read_pk(data, PUMP_OFF_BASE_MINT)?,
            read_pk(data, PUMP_OFF_QUOTE_MINT)?,
            read_pk(data, PUMP_OFF_BASE_VAULT)?,
            read_pk(data, PUMP_OFF_QUOTE_VAULT)?,
        ),
        DexKind::MeteoraDammV2 => make_pool_info(
            DexKind::MeteoraDammV2,
            pool,
            read_pk(data, MET_OFF_MINT_A)?,
            read_pk(data, MET_OFF_MINT_B)?,
            read_pk(data, MET_OFF_VAULT_A)?,
            read_pk(data, MET_OFF_VAULT_B)?,
        ),
    }
}

impl Discovery {
    pub fn new(
        cfg: DiscoveryConfig,
        manager: Arc<PoolManager>,
        rpc: Arc<RpcClient>,
    ) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .user_agent("shred-arb-discovery/1.0")
            .build()
            .unwrap_or_default();
        Self {
            cfg,
            manager,
            rpc,
            http,
            seen_tokens: HashSet::new(),
        }
    }

    pub fn spawn(mut self) {
        tokio::spawn(async move {
            info!(
                interval_s = self.cfg.interval.as_secs(),
                seed_urls = self.cfg.seed_urls.len(),
                "pool discovery started"
            );
            // One-time seed of hot/top tokens so the strategy starts full.
            self.bootstrap().await;
            loop {
                if let Err(e) = self.tick().await {
                    warn!(error = %e, "discovery tick failed");
                }
                tokio::time::sleep(self.cfg.interval).await;
            }
        });
    }

    async fn tick(&mut self) -> Result<()> {
        let mints = self.fetch_new_token_mints().await?;
        for mint in mints {
            if self.seen_tokens.contains(&mint) {
                continue;
            }
            // Mark seen up-front so a token that fails to resolve isn't retried
            // forever; a genuinely-new counterpart pool will re-appear in the
            // feed if it is created later (different token, still unseen).
            self.seen_tokens.insert(mint);
            if let Err(e) = self.try_resolve_pair(mint).await {
                debug!(token = %mint, error = %e, "no shared pool for token");
            }
        }
        Ok(())
    }

    /// Pull the newest tokens from the "new pools" feed (base token of each).
    async fn fetch_new_token_mints(&self) -> Result<Vec<Pubkey>> {
        let body: Value = self
            .http
            .get(&self.cfg.new_pools_url)
            .send()
            .await?
            .json()
            .await?;
        Ok(extract_mints(&body))
    }

    /// Fetch candidate token mints from a single GeckoTerminal-shaped feed.
    async fn fetch_mints_from(&self, url: &str) -> Result<Vec<Pubkey>> {
        let body: Value = self.http.get(url).send().await?.json().await?;
        Ok(extract_mints(&body))
    }

    /// One-time startup bootstrap: scan every seed URL for hot/top/trending
    /// tokens, then resolve each (adding it if it lives on BOTH venues). Runs
    /// before the normal poll loop so the strategy starts with a full pool set
    /// instead of just the mix.json handful.
    async fn bootstrap(&mut self) {
        let mut candidates: Vec<Pubkey> = Vec::new();
        for url in &self.cfg.seed_urls.clone() {
            match self.fetch_mints_from(url).await {
                Ok(mut m) => candidates.append(&mut m),
                Err(e) => warn!(%url, error = %e, "bootstrap seed fetch failed"),
            }
        }
        candidates.sort_unstable();
        candidates.dedup();
        candidates.truncate(self.cfg.bootstrap_max);
        info!(count = candidates.len(), "bootstrap: resolving seed tokens");

        let mut added = 0usize;
        for mint in candidates {
            if self.seen_tokens.contains(&mint) {
                continue;
            }
            self.seen_tokens.insert(mint);
            match self.try_resolve_pair(mint).await {
                Ok(true) => added += 1,
                Ok(false) => {}
                Err(e) => debug!(token = %mint, error = %e, "bootstrap resolve failed"),
            }
            // Gentle pacing so we stay within API/RPC budgets (arb, not sniper).
            tokio::time::sleep(Duration::from_millis(120)).await;
        }
        info!(added, "bootstrap complete");
    }

    /// Ask DexScreener for candidate Pump/Meteora pool addresses for `mint`.
    async fn fetch_candidate_pools(&self, mint: &Pubkey) -> Result<Vec<Pubkey>> {
        let url = self.cfg.token_pairs_url.replace("{mint}", &mint.to_string());
        let body: Value = self.http.get(&url).send().await?.json().await?;
        let mut out = Vec::new();
        if let Some(pairs) = body.get("pairs").and_then(|p| p.as_array()) {
            for pair in pairs {
                let dex = pair.get("dexId").and_then(|d| d.as_str()).unwrap_or("");
                // Pre-filter to the two venues we support; on-chain owner is the
                // authoritative check afterwards.
                if !(dex.contains("pump") || dex.contains("meteora")) {
                    continue;
                }
                if let Some(addr) = pair.get("pairAddress").and_then(|a| a.as_str()) {
                    if let Ok(pk) = Pubkey::from_str(addr) {
                        out.push(pk);
                    }
                }
            }
        }
        Ok(out)
    }

    /// Resolve a token to a Pump↔Meteora `ArbPair` and add it, if one exists.
    /// Returns `Ok(true)` when a shared pool was found and added.
    async fn try_resolve_pair(&self, mint: Pubkey) -> Result<bool> {
        let candidates = self.fetch_candidate_pools(&mint).await?;
        if candidates.len() < 2 {
            return Ok(false);
        }

        let pump_program = pumpfun_program();
        let met_program = meteora_program();
        let mut pump: Option<PoolInfo> = None;
        let mut meteora: Option<PoolInfo> = None;

        for pool in candidates {
            // Already tracked? Skip the RPC read.
            if self.manager.contains(&pool) {
                continue;
            }
            let acct = match self.rpc.get_account(&pool) {
                Ok(a) => a,
                Err(_) => continue,
            };
            if acct.owner != pump_program && acct.owner != met_program {
                continue;
            }
            if let Some(info) = decode_pool(pool, &acct.owner, &acct.data) {
                if info.token_mint != mint {
                    continue; // paired against a different token / not WSOL
                }
                match info.kind {
                    DexKind::PumpFunAmm => pump = Some(info),
                    DexKind::MeteoraDammV2 => meteora = Some(info),
                }
            }
        }

        if let (Some(pump), Some(meteora)) = (pump, meteora) {
            info!(token = %mint, "discovered shared Pump/Meteora pool");
            let pair = ArbPair {
                token_mint: mint,
                pump,
                meteora,
            };
            self.manager.add_pair(pair).await?;
            return Ok(true);
        }
        Ok(false)
    }
}
