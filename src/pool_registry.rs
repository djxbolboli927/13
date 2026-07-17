//! Pool registry sourced from the Metis cache file (`mix.json`).
//!
//! The bot and Metis are handed the SAME cache file so they operate on an
//! identical pool set. Each entry looks like:
//! ```json
//! { "pubkey": "<pool>", "owner": "<program>",
//!   "params": { "addressLookupTableAddress": "<alt>",
//!               "tokenAccountA": "<vaultA>", "tokenAccountB": "<vaultB>",
//!               "tokenmentA": "<mintA>", "tokenmentB": "<mintB>" } } ```
//!
//! We keep only Pump.fun AMM and Meteora DAMM v2 pools, then pair a Pump pool
//! with a Meteora pool that share the same (non-WSOL) token mint.

use anyhow::{Context, Result};
use serde_json::Value;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;
use tracing::{info, warn};

use crate::dex_ids::{wsol_mint, DexKind};

#[derive(Debug, Clone)]
pub struct PoolInfo {
    pub kind: DexKind,
    pub pool: Pubkey,
    pub vault_a: Pubkey,
    pub vault_b: Pubkey,
    pub mint_a: Pubkey,
    pub mint_b: Pubkey,
    pub alt: Option<Pubkey>,
    /// The non-WSOL token traded in this pool.
    pub token_mint: Pubkey,
    /// True if the token sits on side A (WSOL on side B); false otherwise.
    pub token_is_a: bool,
}

impl PoolInfo {
    /// The vault holding the token reserve.
    pub fn token_vault(&self) -> Pubkey {
        if self.token_is_a {
            self.vault_a
        } else {
            self.vault_b
        }
    }
    /// The vault holding the WSOL reserve.
    pub fn wsol_vault(&self) -> Pubkey {
        if self.token_is_a {
            self.vault_b
        } else {
            self.vault_a
        }
    }
}

/// A tradeable pair: the same token on the Pump.fun pool (`pump`) and one
/// counter venue (`counter`) — historically Meteora DAMM v2, now any enabled
/// venue (Meteora DAMM v2 / Meteora Dynamic AMM / Raydium V4 / Raydium CPMM).
#[derive(Debug, Clone)]
pub struct ArbPair {
    pub token_mint: Pubkey,
    pub pump: PoolInfo,
    pub counter: PoolInfo,
}

fn parse_pk(v: Option<&Value>) -> Option<Pubkey> {
    v.and_then(|x| x.as_str()).and_then(|s| Pubkey::from_str(s).ok())
}

/// Recursively collect every JSON object that carries both `owner` and
/// `params`, regardless of how the top-level file wraps them (array, or an
/// object keyed by section).
fn collect_entries<'a>(v: &'a Value, out: &mut Vec<&'a Value>) {
    match v {
        Value::Array(a) => {
            for item in a {
                collect_entries(item, out);
            }
        }
        Value::Object(map) => {
            if map.contains_key("owner") && map.contains_key("params") {
                out.push(v);
            } else {
                for val in map.values() {
                    collect_entries(val, out);
                }
            }
        }
        _ => {}
    }
}

fn entry_to_pool(entry: &Value) -> Option<PoolInfo> {
    let owner = parse_pk(entry.get("owner"))?;
    let kind = DexKind::from_owner(&owner)?;
    let pool = parse_pk(entry.get("pubkey"))?;
    let params = entry.get("params")?;

    let vault_a = parse_pk(params.get("tokenAccountA"))?;
    let vault_b = parse_pk(params.get("tokenAccountB"))?;
    let mint_a = parse_pk(params.get("tokenmentA"))?;
    let mint_b = parse_pk(params.get("tokenmentB"))?;
    let alt = parse_pk(params.get("addressLookupTableAddress"));

    let wsol = wsol_mint();
    let (token_mint, token_is_a) = if mint_b == wsol {
        (mint_a, true)
    } else if mint_a == wsol {
        (mint_b, false)
    } else {
        // Neither side is WSOL — not a WSOL pair we can arb.
        return None;
    };

    Some(PoolInfo {
        kind,
        pool,
        vault_a,
        vault_b,
        mint_a,
        mint_b,
        alt,
        token_mint,
        token_is_a,
    })
}

/// Collect EVERY non-WSOL token mint referenced by any pool in `mix.json`
/// (both Pump.fun and Meteora entries), deduplicated. Used at startup to make
/// sure the trading wallet has an ATA for each token it might hold — without
/// any RPC call to discover which token a pool trades (the mints are right
/// there in each pool's `params`).
pub fn load_all_token_mints(path: &str) -> Result<Vec<Pubkey>> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("cannot read mix cache file {path}"))?;
    let root: Value =
        serde_json::from_str(&content).with_context(|| format!("invalid JSON in {path}"))?;
    let mut raw = Vec::new();
    collect_entries(&root, &mut raw);

    let wsol = wsol_mint();
    let mut mints = Vec::new();
    for e in raw {
        if let Some(p) = entry_to_pool(e) {
            if p.token_mint != wsol {
                mints.push(p.token_mint);
            }
        }
    }
    mints.sort_unstable();
    mints.dedup();
    Ok(mints)
}

/// Load `mix.json` and return the paired Pump.fun / counter-venue pools. A pump
/// pool is paired with a counter pool sharing the same token, where the counter
/// venue's `DexKind` is in `enabled_counters`. If several counter pools exist
/// for a token, the one from the highest-priority enabled venue wins (Meteora
/// DAMM v2 first, then the order they appear) — multi-pool-per-token is a
/// separate phase.
pub fn load_pairs(
    path: &str,
    enabled_counters: &std::collections::HashSet<DexKind>,
) -> Result<Vec<ArbPair>> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("cannot read mix cache file {path}"))?;
    let root: Value =
        serde_json::from_str(&content).with_context(|| format!("invalid JSON in {path}"))?;

    let mut raw = Vec::new();
    collect_entries(&root, &mut raw);

    let mut pumps: HashMap<Pubkey, PoolInfo> = HashMap::new();
    // token → best counter pool found so far.
    let mut counters: HashMap<Pubkey, PoolInfo> = HashMap::new();
    for e in raw {
        if let Some(p) = entry_to_pool(e) {
            if p.kind == DexKind::PumpFunAmm {
                pumps.insert(p.token_mint, p);
            } else if enabled_counters.contains(&p.kind) {
                // Prefer Meteora DAMM v2 if multiple venues are present.
                match counters.get(&p.token_mint) {
                    Some(existing) if existing.kind == DexKind::MeteoraDammV2 => {}
                    _ => {
                        counters.insert(p.token_mint, p);
                    }
                }
            }
        }
    }

    let mut pairs = Vec::new();
    for (token, pump) in &pumps {
        if let Some(counter) = counters.get(token) {
            pairs.push(ArbPair {
                token_mint: *token,
                pump: pump.clone(),
                counter: counter.clone(),
            });
        }
    }

    info!(
        pump_pools = pumps.len(),
        counter_pools = counters.len(),
        pairs = pairs.len(),
        "mix.json pool registry loaded"
    );
    if pairs.is_empty() {
        warn!("no Pump.fun/counter token pairs found in mix.json — strategy will idle");
    }
    Ok(pairs)
}
