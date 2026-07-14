//! Free ALT acquisition from public routing APIs (Jupiter Lite / DFlow / Raptor).
//!
//! Instead of paying rent to build and extend our own Address Lookup Table, we
//! ask a public aggregator for a swap through the SAME pool we trade. Its
//! response references the aggregator's own ALT(s) — already built and optimized
//! for that route — which we then fold into our tx so the pool/vault accounts
//! compress to 1-byte indexes. Free, and no on-chain writes from us.
//!
//! Flow per pool (done once, at add time):
//!   for each leg label (Pump.fun Amm, Meteora DAMM v2):
//!     for each provider (Jupiter first, then DFlow, then Raptor):
//!       GET  {base}/quote?inputMint=WSOL&outputMint=<token>&dexes=<label>&onlyDirectRoutes=true
//!       if the returned route actually goes through our pool/AMM:
//!         POST {base}/swap-instructions  → collect addressLookupTableAddresses
//!         stop trying providers for this leg
//! The union of both legs' ALTs is cached under the Pump pool key (the registry
//! key) and folded into every tx for that pool.
//!
//! All three providers expose the Jupiter-compatible `/quote` + `/swap-instructions`
//! shape, so one parser serves them; only the base URL differs. Rate limits
//! (Jupiter 1 rps, DFlow 40 rps, Raptor none) are respected by the natural
//! once-per-pool cadence plus the ordered fallback.

use dashmap::DashMap;
use reqwest::Client;
use serde_json::Value;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::tokens::WSOL_MINT;

/// One routing provider: a human name + its Jupiter-compatible base URL.
#[derive(Clone)]
pub struct Provider {
    pub name: String,
    pub base_url: String,
}

pub struct AltFetcher {
    http: Client,
    providers: Vec<Provider>,
    user_pubkey: String,
    pump_label: String,
    meteora_label: String,
    /// Probe size for the quote (lamports of WSOL). Any reasonable size routes
    /// through the same pool; the ALTs are size-independent.
    probe_lamports: u64,
    /// pump_pool → the ALT pubkeys that cover its route.
    cache: DashMap<Pubkey, Vec<Pubkey>>,
}

impl AltFetcher {
    pub fn new(
        providers: Vec<Provider>,
        user_pubkey: String,
        pump_label: String,
        meteora_label: String,
    ) -> Arc<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(8))
            .build()
            .unwrap_or_default();
        Arc::new(Self {
            http,
            providers,
            user_pubkey,
            pump_label,
            meteora_label,
            probe_lamports: 10_000_000, // 0.01 SOL
            cache: DashMap::new(),
        })
    }

    /// ALTs cached for a pool (empty if not fetched yet). Cheap, lock-free read.
    pub fn tables_for(&self, pump_pool: &Pubkey) -> Vec<Pubkey> {
        self.cache
            .get(pump_pool)
            .map(|v| v.clone())
            .unwrap_or_default()
    }

    /// Fetch and cache the ALTs covering both legs of a pool. Idempotent — a pool
    /// already cached is skipped. Safe to call from the pool-add path.
    pub async fn fetch_for_pool(
        self: Arc<Self>,
        pump_pool: Pubkey,
        meteora_pool: Pubkey,
        token_mint: Pubkey,
    ) {
        if self.cache.contains_key(&pump_pool) || self.providers.is_empty() {
            return;
        }
        let token = token_mint.to_string();
        let mut alts: Vec<Pubkey> = Vec::new();

        // Pump leg (route must touch the Pump pool), then Meteora leg.
        for (label, want_pool) in [
            (self.pump_label.clone(), pump_pool),
            (self.meteora_label.clone(), meteora_pool),
        ] {
            if let Some(mut found) = self.fetch_leg(&token, &label, &want_pool).await {
                for a in found.drain(..) {
                    if !alts.contains(&a) {
                        alts.push(a);
                    }
                }
            }
        }

        if alts.is_empty() {
            debug!(%token, pump = %pump_pool, "alt-fetch: no ALTs found from any provider");
        } else {
            info!(%token, pump = %pump_pool, alts = alts.len(), "alt-fetch: cached route ALTs");
        }
        self.cache.insert(pump_pool, alts);
    }

    /// Try each provider for one leg; return the first provider's ALTs whose route
    /// actually goes through `want_pool`.
    async fn fetch_leg(&self, token: &str, label: &str, want_pool: &Pubkey) -> Option<Vec<Pubkey>> {
        let dexes = label.replace(' ', "%20");
        for p in &self.providers {
            match self.try_provider(p, token, &dexes, want_pool).await {
                Ok(Some(alts)) if !alts.is_empty() => return Some(alts),
                Ok(_) => {} // provider had no matching route; try next
                Err(e) => debug!(provider = %p.name, %label, error = %e, "alt-fetch provider failed"),
            }
        }
        None
    }

    async fn try_provider(
        &self,
        p: &Provider,
        token: &str,
        dexes: &str,
        want_pool: &Pubkey,
    ) -> anyhow::Result<Option<Vec<Pubkey>>> {
        // 1) Quote WSOL -> token forced onto this DEX label, direct routes only.
        let quote_url = format!(
            "{}/quote?inputMint={}&outputMint={}&amount={}&slippageBps=50&onlyDirectRoutes=true&dexes={}",
            p.base_url.trim_end_matches('/'),
            WSOL_MINT,
            token,
            self.probe_lamports,
            dexes
        );
        let quote: Value = self.http.get(&quote_url).send().await?.error_for_status()?.json().await?;

        // Confirm the route actually touches our pool (ammKey match).
        let touches = quote
            .get("routePlan")
            .and_then(|r| r.as_array())
            .map(|arr| {
                arr.iter().any(|step| {
                    step.get("swapInfo")
                        .and_then(|s| s.get("ammKey"))
                        .and_then(|k| k.as_str())
                        == Some(&want_pool.to_string())
                })
            })
            .unwrap_or(false);
        if !touches {
            return Ok(None);
        }

        // 2) Swap-instructions for that quote → addressLookupTableAddresses.
        let swap_url = format!("{}/swap-instructions", p.base_url.trim_end_matches('/'));
        let body = serde_json::json!({
            "userPublicKey": self.user_pubkey,
            "quoteResponse": quote,
            "wrapAndUnwrapSol": false,
            "asLegacyTransaction": false,
        });
        let resp: Value = self
            .http
            .post(&swap_url)
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let alts: Vec<Pubkey> = resp
            .get("addressLookupTableAddresses")
            .and_then(|a| a.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|s| s.as_str())
                    .filter_map(|s| Pubkey::from_str(s).ok())
                    .collect()
            })
            .unwrap_or_default();
        if alts.is_empty() {
            warn!(provider = %p.name, "alt-fetch: route matched but no ALTs in swap-instructions");
        }
        Ok(Some(alts))
    }
}
