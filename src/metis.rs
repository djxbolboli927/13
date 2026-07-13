use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Client for the Metis (Jupiter self-hosted) routing engine.
pub struct MetisClient {
    base_url: String,
    http: Client,
}

// ---------- Quote types ----------

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QuoteResponse {
    pub input_mint: String,
    pub in_amount: String,
    pub output_mint: String,
    pub out_amount: String,
    pub other_amount_threshold: String,
    pub swap_mode: String,
    pub price_impact_pct: String,
    pub route_plan: serde_json::Value,
    #[serde(default)]
    pub context_slot: Option<u64>,
    /// Returned by Metis v7.0.5+ when the /quote call uses `instructionVersion=V2`.
    /// Will be `Some("V2")` on an up-to-date server, `None` on older binaries
    /// (in which case the bot silently falls back to the legacy `route` instruction).
    #[serde(default)]
    pub instruction_version: Option<String>,
    /// Catch-all for extra fields returned by Metis.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

// ---------- Swap-instructions types ----------

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SwapInstructionsRequest {
    pub user_public_key: String,
    pub quote_response: serde_json::Value,
    pub wrap_and_unwrap_sol: bool,
    pub use_shared_accounts: bool,
    pub dynamic_compute_unit_limit: bool,
    pub skip_user_accounts_rpc_calls: bool,
    pub as_legacy_transaction: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SwapInstructionsResponse {
    #[serde(default)]
    pub compute_budget_instructions: Vec<InstructionData>,
    #[serde(default)]
    pub setup_instructions: Vec<InstructionData>,
    pub swap_instruction: InstructionData,
    #[serde(default)]
    pub cleanup_instruction: Option<InstructionData>,
    #[serde(default)]
    pub address_lookup_table_addresses: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct InstructionData {
    pub program_id: String,
    pub accounts: Vec<AccountMeta>,
    pub data: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AccountMeta {
    pub pubkey: String,
    pub is_signer: bool,
    pub is_writable: bool,
}

/// Why a /swap-instructions call failed. Lets the caller break down the
/// (often large) swap_ix_fail count by root cause instead of one opaque total.
#[derive(Debug, Clone, Copy)]
pub enum SwapIxError {
    /// Request exceeded the client timeout (quote_timeout_ms). Most common when
    /// Metis is overwhelmed and can't build the circular instruction in time.
    Timeout,
    /// Metis returned a non-2xx status — it could not route/build this quote
    /// (e.g. 400 "no route", 422, 500). A genuine rejection, not a stall.
    Http(#[allow(dead_code)] u16),
    /// Connection-level failure (TCP/TLS reset, pool exhausted, etc.).
    Network,
    /// 2xx received but the body could not be parsed as SwapInstructionsResponse.
    Parse,
}

impl MetisClient {
    pub fn new(base_url: &str, timeout_ms: u64) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            // Each scan cycle fires (max-min)/step × tokens quote pairs
            // concurrently. Keeping 64 idle connections warm avoids
            // ~20-50ms TCP/TLS handshake on cold reuse.
            .pool_max_idle_per_host(64)
            .tcp_nodelay(true)
            .build()
            .expect("failed to build http client");
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http,
        }
    }

    /// Get a quote from Metis.
    ///
    /// Parameters:
    /// - slippageBps=0: zero slippage at quote layer; the real on-chain floor
    ///   is set via `other_amount_threshold` in `merge_quotes`.
    /// - maxAccounts=50: leave room for tip account in final tx
    /// - forJitoBundle=true: excludes Jito-incompatible DEXes
    /// - swapMode=ExactIn: exact input amount
    /// - restrictIntermediateTokens=false: allow all intermediate tokens
    /// - instructionVersion=V2: tells Metis to produce a route_plan that uses
    ///   `bps: 10000` (instead of legacy `percent: 100`). When this QuoteResponse
    ///   is later sent to /swap-instructions, Metis builds a `route_v2` instruction
    ///   which costs fewer compute units and is what competing arb bots use.
    /// `only_direct`: when true, adds `&onlyDirectRoutes=true` (single-hop only).
    pub async fn get_quote(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_lamports: u64,
        only_direct: bool,
    ) -> Result<QuoteResponse> {
        // Free routes: restrictIntermediateTokens=true limits intermediary tokens
        // to highly-liquid ones (SOL, USDC, USDT, etc.) for reliable multi-hop arb.
        // Direct routes: onlyDirectRoutes=true — no intermediate tokens anyway.
        let url = format!(
            "{}/quote?inputMint={}&outputMint={}&amount={}\
             &slippageBps=0\
             &maxAccounts=50\
             &swapMode=ExactIn\
             &forJitoBundle=true\
             &instructionVersion=V2{}",
            self.base_url, input_mint, output_mint, amount_lamports,
            if only_direct {
                "&onlyDirectRoutes=true"
            } else {
                "&restrictIntermediateTokens=true"
            }
        );

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("quote request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("quote failed: {} -- {}", status, body);
        }

        let quote: QuoteResponse = resp.json().await.context("failed to parse quote response")?;
        Ok(quote)
    }

    /// Hot-add a market/pool to the running Metis instance (no restart).
    /// Requires Metis started with `--enable-add-market`. Metis fetches the
    /// pool's accounts itself, so only address + owner (program id) + optional
    /// ALT are needed. Idempotent enough to call for pools already present.
    pub async fn add_market(
        &self,
        address: &str,
        owner: &str,
        address_lookup_table_address: Option<&str>,
    ) -> Result<()> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct AddMarketRequest<'a> {
            address: &'a str,
            owner: &'a str,
            // Metis REQUIRES this field to be present (it 422s "missing field
            // addressLookupTableAddress" otherwise), so always send it — null
            // when we have no ALT.
            address_lookup_table_address: Option<&'a str>,
        }
        let url = format!("{}/add-market", self.base_url);
        let body = AddMarketRequest {
            address,
            owner,
            address_lookup_table_address,
        };
        // Metis may do RPC fetches to load the market, so allow more time than
        // the tight quote timeout.
        let resp = self
            .http
            .post(&url)
            .json(&body)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .context("add-market request failed")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("add-market failed: {} -- {}", status, text);
        }
        Ok(())
    }

    /// Get a quote FORCED onto a single venue via the Metis `dexes=` filter.
    ///
    /// Used by the ShredStream arb strategy: each leg is locked to a specific
    /// exchange (buy on one, sell on the other) so Metis builds exactly the
    /// route we priced ourselves, instead of picking its own path. `dex_label`
    /// is the exact, case-sensitive Metis label (e.g. `"Pump.fun Amm"`,
    /// `"Meteora DAMM v2"`); spaces are encoded as `+`.
    pub async fn get_quote_forced(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_lamports: u64,
        dex_label: &str,
        max_accounts: u64,
    ) -> Result<QuoteResponse> {
        let dexes = dex_label.replace(' ', "+");
        let url = format!(
            "{}/quote?inputMint={}&outputMint={}&amount={}\
             &slippageBps=0\
             &maxAccounts={}\
             &swapMode=ExactIn\
             &forJitoBundle=true\
             &onlyDirectRoutes=true\
             &instructionVersion=V2\
             &dexes={}",
            self.base_url, input_mint, output_mint, amount_lamports, max_accounts, dexes
        );

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("forced quote request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("forced quote failed: {} -- {}", status, body);
        }

        let quote: QuoteResponse = resp
            .json()
            .await
            .context("failed to parse forced quote response")?;
        Ok(quote)
    }

    /// Merge two quotes into a single circular quote via Route Concatenation.
    ///
    /// Takes quote1 (WSOL->Token) and quote2 (Token->WSOL),
    /// concatenates their routePlans, and produces a single combined quote
    /// that represents the full circular path WSOL->Token->WSOL.
    ///
    /// The combined quote is then sent to /swap-instructions to get
    /// a SINGLE route_v2 instruction that handles the entire circular arb.
    ///
    /// `min_acceptable_out` is the minimum output lamports we will tolerate
    /// on-chain. It becomes the `other_amount_threshold` of the merged quote
    /// and is embedded as `slippage_bps` floor in the route_v2 instruction.
    /// Setting it to `amount + tip + base_fee` means the tx reverts ONLY if
    /// the trade would lose lamports -- any price jitter that still leaves us
    /// at break-even or better will land on-chain (even if profit shrinks).
    pub fn merge_quotes(
        quote1: &QuoteResponse,
        quote2: &QuoteResponse,
        min_acceptable_out: u64,
    ) -> Result<QuoteResponse> {
        // Concatenate routePlans: q1.routePlan + q2.routePlan
        let route_plan1 = quote1
            .route_plan
            .as_array()
            .context("quote1 routePlan is not an array")?;
        let route_plan2 = quote2
            .route_plan
            .as_array()
            .context("quote2 routePlan is not an array")?;

        let mut combined_route_plan = route_plan1.clone();
        combined_route_plan.extend(route_plan2.iter().cloned());

        // Build merged quote:
        // - inputMint, inAmount from quote1 (WSOL input)
        // - outputMint from quote2 (WSOL output)
        // - outAmount = min_acceptable_out: Metis copies this directly into the route_v2
        //   instruction's quotedOutAmount field, which with slippage_bps=0 becomes the
        //   on-chain minimum. Setting it to our floor (input + fees) lets the tx land
        //   at break-even rather than requiring Metis's exact optimistic prediction.
        // - otherAmountThreshold = same floor (belt-and-suspenders)
        // - instructionVersion propagates from quote1 (must be "V2" for route_v2)
        Ok(QuoteResponse {
            input_mint: quote1.input_mint.clone(),
            in_amount: quote1.in_amount.clone(),
            output_mint: quote2.output_mint.clone(),
            out_amount: min_acceptable_out.to_string(),
            other_amount_threshold: min_acceptable_out.to_string(),
            swap_mode: quote1.swap_mode.clone(),
            price_impact_pct: "0".to_string(),
            route_plan: serde_json::Value::Array(combined_route_plan),
            context_slot: quote2.context_slot,
            instruction_version: quote1.instruction_version.clone(),
            extra: quote1.extra.clone(),
        })
    }

    /// Get swap instructions for a merged circular quote.
    ///
    /// CRITICAL for circular arbitrage:
    /// - useSharedAccounts=false (shared accounts cause memory conflicts in circular swaps)
    /// - dynamicComputeUnitLimit=false (avoid extra RPC simulation call by Metis -- we set CU manually)
    /// - wrapAndUnwrapSol=false (WSOL ATA must pre-exist)
    /// - asLegacyTransaction=false (v0 for ALT support)
    pub async fn get_swap_instructions(
        &self,
        user_pubkey: &str,
        quote_response: &QuoteResponse,
    ) -> std::result::Result<SwapInstructionsResponse, SwapIxError> {
        let quote_value = match serde_json::to_value(quote_response) {
            Ok(v) => v,
            Err(_) => return Err(SwapIxError::Parse),
        };

        let body = SwapInstructionsRequest {
            user_public_key: user_pubkey.to_string(),
            quote_response: quote_value,
            wrap_and_unwrap_sol: false,
            use_shared_accounts: false,
            dynamic_compute_unit_limit: false,
            skip_user_accounts_rpc_calls: true,
            as_legacy_transaction: false,
        };

        let url = format!("{}/swap-instructions", self.base_url);
        let resp = match self.http.post(&url).json(&body).send().await {
            Ok(r) => r,
            Err(e) => {
                // Distinguish a client-timeout (Metis too slow) from a
                // connection-level failure so the funnel can show which one.
                return Err(if e.is_timeout() {
                    SwapIxError::Timeout
                } else {
                    SwapIxError::Network
                });
            }
        };

        if !resp.status().is_success() {
            return Err(SwapIxError::Http(resp.status().as_u16()));
        }

        match resp.json::<SwapInstructionsResponse>().await {
            Ok(swap_ixs) => Ok(swap_ixs),
            Err(_) => Err(SwapIxError::Parse),
        }
    }
}
