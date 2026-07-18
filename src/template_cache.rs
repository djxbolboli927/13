use crate::metis::SwapInstructionsResponse;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

// ─── Constants ────────────────────────────────────────────────────────────────

const BASE_DIR: &str = "/root/c/cache";
/// Max distinct routes kept live (hot+cold ≤ 2×). For 200 pools ×2 directions
/// that's ~400 routes; 2000 gives plenty of headroom without growing unbounded.
const ROUTE_SEG_CAP: usize = 2_000;

// ─── Route signature ──────────────────────────────────────────────────────────

/// Amount-independent 128-bit structural hash of a route_plan.
/// Two independent FNV-1a passes with different seeds.
/// Hashes ALL swapInfo fields except inAmount / outAmount / feeAmount,
/// including booleans (a_to_b) which the old u64 sig missed.
pub fn route_sig(route_plan: &serde_json::Value) -> u128 {
    let h1 = fnv1a_64(route_plan, 0xcbf2_9ce4_8422_2325_u64);
    let h2 = fnv1a_64(route_plan, 0x517c_c1b7_2722_0a95_u64);
    ((h1 as u128) << 64) | (h2 as u128)
}

fn fnv1a_64(route_plan: &serde_json::Value, seed: u64) -> u64 {
    let mut h = seed;
    macro_rules! feed {
        ($b:expr) => {
            for &byte in ($b as &[u8]) {
                h ^= byte as u64;
                h = h.wrapping_mul(0x0000_0001_0000_01b3);
            }
        };
    }
    if let Some(arr) = route_plan.as_array() {
        for hop in arr {
            if let Some(si) = hop.get("swapInfo").and_then(|s| s.as_object()) {
                let mut keys: Vec<&String> = si.keys().collect();
                keys.sort();
                for k in keys {
                    if matches!(k.as_str(), "inAmount" | "outAmount" | "feeAmount") {
                        continue;
                    }
                    feed!(k.as_bytes());
                    feed!(b"=");
                    if let Some(v) = si.get(k) {
                        match v {
                            serde_json::Value::String(s) => feed!(s.as_bytes()),
                            serde_json::Value::Bool(b) => feed!(if *b { b"T" } else { b"F" }),
                            serde_json::Value::Number(n) => feed!(n.to_string().as_bytes()),
                            _ => {}
                        }
                    }
                    feed!(b";");
                }
            }
        }
    }
    h
}

// ─── DEX kind ─────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum DexKind {
    Whirlpool,
    WhirlpoolSwapV2,
    RaydiumClmm,
    RaydiumV2,
    MeteoraDlmm,
    MeteoraPools,
    AlphaQ,
    Aquifer,
    AldrinV2,
    Other,
}

impl DexKind {
    fn from_label(label: &str) -> Self {
        let l = label.to_lowercase();
        if l.contains("whirlpool") {
            return if l.contains("v2") || l.contains("swap_v2") || l.contains("swapv2") {
                DexKind::WhirlpoolSwapV2
            } else {
                DexKind::Whirlpool
            };
        }
        if l.contains("raydium") && l.contains("clmm") {
            return DexKind::RaydiumClmm;
        }
        if l.contains("raydium") {
            return DexKind::RaydiumV2;
        }
        if l.contains("meteora") && l.contains("dlmm") {
            return DexKind::MeteoraDlmm;
        }
        if l.contains("meteora") {
            return DexKind::MeteoraPools;
        }
        if l.contains("alphaq") || l.contains("alpha_q") {
            return DexKind::AlphaQ;
        }
        if l.contains("aquifer") {
            return DexKind::Aquifer;
        }
        if l.contains("aldrin") {
            return DexKind::AldrinV2;
        }
        DexKind::Other
    }

    fn file_stem(&self) -> &'static str {
        match self {
            DexKind::Whirlpool => "whirlpool",
            DexKind::WhirlpoolSwapV2 => "whirlpool_swap_v2",
            DexKind::RaydiumClmm => "raydium_clmm",
            DexKind::RaydiumV2 => "raydium_v2",
            DexKind::MeteoraDlmm => "meteora_dlmm",
            DexKind::MeteoraPools => "meteora_pools",
            DexKind::AlphaQ => "alphaq",
            DexKind::Aquifer => "aquifer",
            DexKind::AldrinV2 => "aldrin_v2",
            DexKind::Other => "other",
        }
    }
}

// ─── Direction ────────────────────────────────────────────────────────────────

/// Per-DEX directional parameter stored in the HopKey.
/// Amount-independent: two calls on the same pool with the same direction
/// share one template entry regardless of trade size.
#[derive(Clone, Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum DirectionParams {
    None,
    AToB(bool),
    SideBid,
    SideAsk,
}

fn extract_direction(
    dex: &DexKind,
    si: &serde_json::Map<String, serde_json::Value>,
) -> DirectionParams {
    match dex {
        DexKind::Whirlpool | DexKind::WhirlpoolSwapV2 | DexKind::AlphaQ => {
            for field in &["a_to_b", "aToB", "atob", "a2b"] {
                if let Some(serde_json::Value::Bool(b)) = si.get(*field) {
                    return DirectionParams::AToB(*b);
                }
            }
            DirectionParams::None
        }
        DexKind::AldrinV2 => {
            if let Some(serde_json::Value::String(s)) = si.get("side") {
                return match s.to_lowercase().as_str() {
                    "bid" => DirectionParams::SideBid,
                    "ask" => DirectionParams::SideAsk,
                    _ => DirectionParams::None,
                };
            }
            DirectionParams::None
        }
        _ => DirectionParams::None,
    }
}

// ─── HopKey ───────────────────────────────────────────────────────────────────

/// Amount-independent key for a single DEX hop.
/// Two calls with different amounts but same pool/direction share one entry.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct HopKey {
    pub dex: DexKind,
    pub pool_or_amm_key: String,
    pub input_mint: String,
    pub output_mint: String,
    pub direction: DirectionParams,
    pub remaining_accounts_hash: u64,
}

impl HopKey {
    fn from_swap_info(si: &serde_json::Map<String, serde_json::Value>) -> Option<Self> {
        let pool = si.get("ammKey").and_then(|v| v.as_str())?.to_string();
        let input = si.get("inputMint").and_then(|v| v.as_str())?.to_string();
        let output = si.get("outputMint").and_then(|v| v.as_str())?.to_string();
        let label = si.get("label").and_then(|v| v.as_str()).unwrap_or("");
        let dex = DexKind::from_label(label);
        let direction = extract_direction(&dex, si);
        Some(HopKey {
            dex,
            pool_or_amm_key: pool,
            input_mint: input,
            output_mint: output,
            direction,
            remaining_accounts_hash: 0,
        })
    }

    fn template_id(&self) -> String {
        let dir = match &self.direction {
            DirectionParams::None => "none".to_string(),
            DirectionParams::AToB(b) => format!("a2b:{b}"),
            DirectionParams::SideBid => "bid".to_string(),
            DirectionParams::SideAsk => "ask".to_string(),
        };
        format!(
            "{}:{}:{}:{}:{}",
            self.pool_or_amm_key,
            self.input_mint,
            self.output_mint,
            dir,
            self.remaining_accounts_hash,
        )
    }
}

// ─── HopTemplate (persisted to per-DEX JSON files) ────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HopTemplate {
    pub dex: DexKind,
    pub pool_or_amm_key: String,
    pub input_mint: String,
    pub output_mint: String,
    pub direction: DirectionParams,
    pub remaining_accounts_hash: u64,
    pub seen_count: u64,
    pub first_seen_slot: Option<u64>,
    pub last_seen_slot: Option<u64>,
}

// ─── RouteTemplate (in-RAM only, keyed by route_sig with NO amount) ───────────

/// Stores one complete SwapInstructionsResponse per route structure.
///
/// Fast path: `amount_variants` maps each `in_amount` seen to the exact
/// SwapInstructionsResponse that Metis returned for it.  A lookup here is
/// O(1) and never wrong — no Borsh patching required.
///
/// Slow path (new amounts not yet cached): `in_amount_offset` /
/// `quoted_out_offset` allow the amounts to be patched in-place in the base
/// instruction's Borsh data.  Works for route_v2; falls through to Metis on
/// failure.  Once Metis succeeds for an amount it is added to
/// `amount_variants` so every subsequent hit is served from the fast path.
#[derive(Clone, Debug)]
pub struct RouteTemplate {
    #[allow(dead_code)]
    pub route_signature: u128,
    /// Base instruction (first captured). Used as the source for Borsh patching.
    pub swap_ixs: SwapInstructionsResponse,
    /// in_amount stored in the base instruction.
    pub template_in_amount: u64,
    /// quoted_out_amount stored in the base instruction.
    pub template_quoted_out: u64,
    /// Byte offset of in_amount in the decoded swap_instruction.data.
    /// None when the Borsh layout didn't validate on first capture.
    pub in_amount_offset: Option<usize>,
    pub quoted_out_offset: Option<usize>,
    /// Per-amount instruction cache.  Key = in_amount (quoted_out is always
    /// in_amount + 6600 for our arb so we only need one key dimension).
    /// After one successful Metis call for (route, amount), all future calls
    /// with the same amount are served from here — no Metis, no patching.
    pub amount_variants: HashMap<u64, SwapInstructionsResponse>,
    pub seen_count: u64,
    pub hit_count: u64,
}

// ─── Borsh amount patching ────────────────────────────────────────────────────

/// Locate in_amount / quoted_out_amount inside Borsh-encoded route_v2 data.
///
/// Jupiter v6 route_v2 serializes `inAmount` (u64 LE) immediately followed by
/// `quotedOutAmount` (u64 LE).  Rather than assume a fixed distance from the
/// end of the buffer (which breaks if Metis adds trailing fields like
/// positiveSlippageBps), we SEARCH for the unique 16-byte window where
///   data[P..P+8]   == in_amount   (LE)
///   data[P+8..P+16]== quoted_out  (LE)
///
/// That consecutive (inAmount, quotedOutAmount) pair is a strong signature —
/// collisions with account pubkeys or other args are effectively impossible.
/// We require exactly ONE match; zero or multiple matches → None (safe
/// fallback to Metis).  Returns (in_offset, quoted_out_offset).
pub fn discover_offsets(data: &[u8], in_amount: u64, quoted_out: u64) -> Option<(usize, usize)> {
    let in_le = in_amount.to_le_bytes();
    let out_le = quoted_out.to_le_bytes();
    if data.len() < 16 {
        return None;
    }
    // Pass 1: consecutive (inAmount, quotedOutAmount) — strongest signature.
    let mut found: Option<usize> = None;
    let mut count = 0usize;
    for p in 0..=(data.len() - 16) {
        if data[p..p + 8] == in_le && data[p + 8..p + 16] == out_le {
            found = Some(p);
            count += 1;
            if count > 1 {
                return None; // ambiguous — refuse to patch
            }
        }
    }
    if let Some(p) = found {
        return Some((p, p + 8));
    }

    // Pass 2: Metis embedded a quotedOutAmount different from our prediction.
    // Locate inAmount uniquely; quotedOutAmount is the next u64 (IDL order).
    let mut found_in: Option<usize> = None;
    let mut in_count = 0usize;
    for p in 0..=(data.len() - 16) {
        if data[p..p + 8] == in_le {
            found_in = Some(p);
            in_count += 1;
            if in_count > 1 {
                return None; // ambiguous inAmount — refuse to patch
            }
        }
    }
    let p = found_in?;
    Some((p, p + 8))
}

pub fn patch_amounts_b64(
    data_b64: &str,
    in_off: usize,
    out_off: usize,
    new_in: u64,
    new_out: u64,
) -> Option<String> {
    let mut data = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data_b64)
        .ok()?;
    if in_off + 8 > data.len() || out_off + 8 > data.len() {
        return None;
    }
    data[in_off..in_off + 8].copy_from_slice(&new_in.to_le_bytes());
    data[out_off..out_off + 8].copy_from_slice(&new_out.to_le_bytes());
    Some(base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        &data,
    ))
}

/// Serve a RouteTemplate with the requested amounts.
///
/// Lookup order:
///   1. `amount_variants[new_in]` — exact cached instruction, always correct.
///   2. Borsh patch of the base instruction at pre-discovered byte offsets.
///
/// Returns None only when both paths fail (no cached variant AND patching
/// is impossible because offsets were never validated).  The caller must
/// then fall through to Metis.
pub fn serve_route(
    tmpl: &RouteTemplate,
    new_in: u64,
    _new_out: u64,
) -> Option<SwapInstructionsResponse> {
    // Fast path: exact per-amount cached instruction.
    if let Some(v) = tmpl.amount_variants.get(&new_in) {
        return Some(v.clone());
    }
    // Slow path: Borsh patch the base instruction for this (unseen) amount.
    let (in_off, out_off) = (tmpl.in_amount_offset?, tmpl.quoted_out_offset?);
    let new_out = new_in + (tmpl.template_quoted_out - tmpl.template_in_amount);
    let new_data =
        patch_amounts_b64(&tmpl.swap_ixs.swap_instruction.data, in_off, out_off, new_in, new_out)?;
    let mut patched = tmpl.swap_ixs.clone();
    patched.swap_instruction.data = new_data;
    Some(patched)
}

// ─── TemplateStore ────────────────────────────────────────────────────────────

pub struct TemplateStore {
    inner: RwLock<StoreInner>,
}

struct StoreInner {
    routes_hot: HashMap<u128, RouteTemplate>,
    routes_cold: HashMap<u128, RouteTemplate>,
    hops: HashMap<HopKey, HopTemplate>,
    /// Secondary index: "hop1_id|hop2_id" -> route_sig.
    /// Allows Tier-2 lookup when the primary route_sig misses (e.g. an
    /// unstable extra field in swapInfo) but both pools have been seen.
    hop_pair_index: HashMap<String, u128>,
}

impl TemplateStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(StoreInner {
                routes_hot: HashMap::new(),
                routes_cold: HashMap::new(),
                hops: HashMap::new(),
                hop_pair_index: HashMap::new(),
            }),
        })
    }

    // ── Route template ────────────────────────────────────────────────────────

    /// Look up a RouteTemplate. Promotes cold hits to hot.
    pub fn get_route(&self, sig: u128) -> Option<RouteTemplate> {
        {
            let g = self.inner.read().ok()?;
            if let Some(t) = g.routes_hot.get(&sig) {
                return Some(t.clone());
            }
            if !g.routes_cold.contains_key(&sig) {
                return None;
            }
        }
        let mut g = self.inner.write().ok()?;
        if let Some(mut t) = g.routes_cold.remove(&sig) {
            t.hit_count += 1;
            let out = t.clone();
            Self::push_hot_route(&mut g, sig, t);
            return Some(out);
        }
        g.routes_hot.get(&sig).cloned()
    }

    /// Record a hit on an already-served template (increments hit_count).
    pub fn record_route_hit(&self, sig: u128) {
        let Ok(mut g) = self.inner.write() else { return };
        if let Some(t) = g.routes_hot.get_mut(&sig) {
            t.hit_count += 1;
        } else if let Some(t) = g.routes_cold.get_mut(&sig) {
            t.hit_count += 1;
        }
    }

    /// Insert a new RouteTemplate (first time we see this route structure).
    /// `route_plan` is used to build the secondary hop-pair index so Tier-2
    /// lookups work even when the primary route_sig doesn't match exactly.
    /// If the route already exists, only increments seen_count.
    pub fn insert_route(
        &self,
        sig: u128,
        swap_ixs: SwapInstructionsResponse,
        in_amount: u64,
        quoted_out: u64,
        route_plan: &serde_json::Value,
    ) {
        let offsets = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            &swap_ixs.swap_instruction.data,
        )
        .ok()
        .and_then(|d| discover_offsets(&d, in_amount, quoted_out));

        let Ok(mut g) = self.inner.write() else { return };

        if g.routes_hot.contains_key(&sig) {
            let t = g.routes_hot.get_mut(&sig).unwrap();
            t.seen_count += 1;
            // Cache this amount so the next scan for (route, amount) is instant.
            t.amount_variants.entry(in_amount).or_insert(swap_ixs);
            return;
        }
        if g.routes_cold.contains_key(&sig) {
            let t = g.routes_cold.get_mut(&sig).unwrap();
            t.seen_count += 1;
            t.amount_variants.entry(in_amount).or_insert(swap_ixs);
            return;
        }

        let mut amount_variants = HashMap::new();
        amount_variants.insert(in_amount, swap_ixs.clone());

        let tmpl = RouteTemplate {
            route_signature: sig,
            swap_ixs,
            template_in_amount: in_amount,
            template_quoted_out: quoted_out,
            in_amount_offset: offsets.map(|(i, _)| i),
            quoted_out_offset: offsets.map(|(_, o)| o),
            amount_variants,
            seen_count: 1,
            hit_count: 0,
        };
        Self::push_hot_route(&mut g, sig, tmpl);

        // Build secondary hop-pair index for 2-hop routes.
        // Key = "hop1_template_id|hop2_template_id" → route_sig.
        // Allows Tier-2 to find a valid RouteTemplate even when the exact
        // route_sig changes between scans (e.g. an extra Metis field).
        if let Some(arr) = route_plan.as_array() {
            if arr.len() == 2 {
                let id0 = arr[0]
                    .get("swapInfo")
                    .and_then(|s| s.as_object())
                    .and_then(HopKey::from_swap_info)
                    .map(|k| k.template_id());
                let id1 = arr[1]
                    .get("swapInfo")
                    .and_then(|s| s.as_object())
                    .and_then(HopKey::from_swap_info)
                    .map(|k| k.template_id());
                if let (Some(id0), Some(id1)) = (id0, id1) {
                    let pair_key = format!("{id0}|{id1}");
                    g.hop_pair_index.entry(pair_key).or_insert(sig);
                }
            }
        }
    }

    /// Tier-2 lookup: find a RouteTemplate by hop-pair identity rather than
    /// exact route_sig. Returns Some only when a previously saved template
    /// shares the same two DEX pools (in the same order and direction).
    pub fn get_route_for_hops(&self, route_plan: &serde_json::Value) -> Option<RouteTemplate> {
        let arr = route_plan.as_array()?;
        if arr.len() != 2 {
            return None;
        }
        let id0 = arr[0]
            .get("swapInfo")
            .and_then(|s| s.as_object())
            .and_then(HopKey::from_swap_info)
            .map(|k| k.template_id())?;
        let id1 = arr[1]
            .get("swapInfo")
            .and_then(|s| s.as_object())
            .and_then(HopKey::from_swap_info)
            .map(|k| k.template_id())?;
        let pair_key = format!("{id0}|{id1}");
        let sig = {
            let g = self.inner.read().ok()?;
            *g.hop_pair_index.get(&pair_key)?
        };
        self.get_route(sig)
    }

    fn push_hot_route(g: &mut StoreInner, sig: u128, tmpl: RouteTemplate) {
        if g.routes_hot.len() >= ROUTE_SEG_CAP {
            g.routes_cold = std::mem::take(&mut g.routes_hot);
        }
        g.routes_hot.insert(sig, tmpl);
    }

    // ── Hop tracking ──────────────────────────────────────────────────────────

    /// Returns (all_hit, missing_count) for the hops in a merged route_plan.
    pub fn check_hops(&self, route_plan: &serde_json::Value) -> (bool, usize) {
        let Ok(g) = self.inner.read() else { return (false, 0) };
        let mut missing = 0usize;
        if let Some(arr) = route_plan.as_array() {
            for hop in arr {
                let has = hop
                    .get("swapInfo")
                    .and_then(|s| s.as_object())
                    .and_then(|si| HopKey::from_swap_info(si))
                    .map(|k| g.hops.contains_key(&k))
                    .unwrap_or(false);
                if !has {
                    missing += 1;
                }
            }
        }
        (missing == 0, missing)
    }

    /// Record hops from a successful Metis response (amount-independent).
    pub fn record_hops(&self, route_plan: &serde_json::Value, context_slot: Option<u64>) {
        let Ok(mut g) = self.inner.write() else { return };
        if let Some(arr) = route_plan.as_array() {
            for hop in arr {
                if let Some(si) = hop.get("swapInfo").and_then(|s| s.as_object()) {
                    if let Some(key) = HopKey::from_swap_info(si) {
                        let ent = g.hops.entry(key.clone()).or_insert_with(|| HopTemplate {
                            dex: key.dex.clone(),
                            pool_or_amm_key: key.pool_or_amm_key.clone(),
                            input_mint: key.input_mint.clone(),
                            output_mint: key.output_mint.clone(),
                            direction: key.direction.clone(),
                            remaining_accounts_hash: key.remaining_accounts_hash,
                            seen_count: 0,
                            first_seen_slot: context_slot,
                            last_seen_slot: context_slot,
                        });
                        ent.seen_count += 1;
                        ent.last_seen_slot = context_slot;
                    }
                }
            }
        }
    }

    // ── Stats ─────────────────────────────────────────────────────────────────

    pub fn route_count(&self) -> usize {
        self.inner
            .read()
            .map(|g| g.routes_hot.len() + g.routes_cold.len())
            .unwrap_or(0)
    }

    pub fn hop_count(&self) -> usize {
        self.inner.read().map(|g| g.hops.len()).unwrap_or(0)
    }

    /// How many stored routes have Borsh offsets discovered (i.e. can be
    /// served from RAM at ANY amount via patching). If this is far below
    /// route_count, the byte-search in discover_offsets is failing and the
    /// route_v2 layout needs investigation.
    pub fn patchable_route_count(&self) -> usize {
        self.inner
            .read()
            .map(|g| {
                g.routes_hot
                    .values()
                    .chain(g.routes_cold.values())
                    .filter(|t| t.in_amount_offset.is_some())
                    .count()
            })
            .unwrap_or(0)
    }

    // ── Disk persistence ──────────────────────────────────────────────────────

    /// Load HopTemplates from per-DEX JSON files in cache/hops/.
    ///
    /// HopTemplates carry only metadata (pool, mints, direction) — they let
    /// `check_hops` report `hop_all_hit` but cannot, on their own, serve any
    /// instruction. The actual RAM-serve path needs RouteTemplates, which are
    /// loaded separately by `load_routes_from_disk`.
    pub fn load_from_disk(&self) -> usize {
        let hops_dir = format!("{BASE_DIR}/hops");
        let entries = match std::fs::read_dir(&hops_dir) {
            Ok(e) => e,
            Err(_) => return 0,
        };
        let Ok(mut g) = self.inner.write() else { return 0 };
        let mut count = 0usize;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let Ok(data) = std::fs::read_to_string(&path) else { continue };
            let file: DexHopFile = match serde_json::from_str(&data) {
                Ok(f) => f,
                Err(_) => continue,
            };
            for (_, tmpl) in file.templates {
                let key = HopKey {
                    dex: tmpl.dex.clone(),
                    pool_or_amm_key: tmpl.pool_or_amm_key.clone(),
                    input_mint: tmpl.input_mint.clone(),
                    output_mint: tmpl.output_mint.clone(),
                    direction: tmpl.direction.clone(),
                    remaining_accounts_hash: tmpl.remaining_accounts_hash,
                };
                g.hops.entry(key).or_insert(tmpl);
                count += 1;
            }
        }
        count
    }

    /// Load RouteTemplates + the hop-pair index from cache/routes.json.
    ///
    /// This is what makes RAM-serving work across restarts. Without it the
    /// store starts empty every run, so with `serve_from_metis=false` every
    /// opportunity is dropped (drop_no_serve) until Metis re-populates RAM.
    ///
    /// We persist only the BASE instruction + Borsh offsets, not the per-amount
    /// `amount_variants` cache (which would bloat the file). Patchable routes
    /// re-derive any amount from the base on load; the variants cache simply
    /// re-warms at runtime.
    pub fn load_routes_from_disk(&self) -> usize {
        let path = format!("{BASE_DIR}/routes.json");
        let Ok(data) = std::fs::read_to_string(&path) else { return 0 };
        let file: RouteFile = match serde_json::from_str(&data) {
            Ok(f) => f,
            Err(_) => return 0,
        };
        let Ok(mut g) = self.inner.write() else { return 0 };
        let mut count = 0usize;
        for rec in file.routes {
            let Ok(sig) = rec.sig.parse::<u128>() else { continue };
            if g.routes_hot.contains_key(&sig) || g.routes_cold.contains_key(&sig) {
                continue;
            }
            let tmpl = RouteTemplate {
                route_signature: sig,
                swap_ixs: rec.swap_ixs,
                template_in_amount: rec.template_in_amount,
                template_quoted_out: rec.template_quoted_out,
                in_amount_offset: rec.in_amount_offset,
                quoted_out_offset: rec.quoted_out_offset,
                amount_variants: HashMap::new(),
                seen_count: rec.seen_count,
                hit_count: rec.hit_count,
            };
            g.routes_hot.insert(sig, tmpl);
            count += 1;
        }
        for (pair_key, sig_str) in file.hop_pairs {
            if let Ok(sig) = sig_str.parse::<u128>() {
                g.hop_pair_index.entry(pair_key).or_insert(sig);
            }
        }
        count
    }

    fn flush_routes_to_disk(&self) {
        let (routes, hop_pairs): (Vec<RouteRecord>, HashMap<String, String>) = {
            let Ok(g) = self.inner.read() else { return };
            let routes = g
                .routes_hot
                .values()
                .chain(g.routes_cold.values())
                .map(RouteRecord::from_template)
                .collect();
            let hop_pairs = g
                .hop_pair_index
                .iter()
                .map(|(k, v)| (k.clone(), v.to_string()))
                .collect();
            (routes, hop_pairs)
        };

        let _ = std::fs::create_dir_all(BASE_DIR);
        let file = RouteFile { schema_version: 1, routes, hop_pairs };
        // Compact (not pretty) — this file holds full instruction blobs and is
        // rewritten on every flush; keep it small.
        let Ok(json) = serde_json::to_string(&file) else { return };
        let path = format!("{BASE_DIR}/routes.json");
        let tmp = format!("{path}.tmp");
        if std::fs::write(&tmp, json.as_bytes()).is_ok() {
            let _ = std::fs::rename(&tmp, &path);
        }
    }

    fn flush_to_disk(&self) {
        self.flush_routes_to_disk();
        let hops: Vec<(HopKey, HopTemplate)> = {
            let Ok(g) = self.inner.read() else { return };
            g.hops.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };

        // Group by DEX (each DEX → one JSON file)
        let mut by_dex: HashMap<&'static str, (DexKind, HashMap<String, HopTemplate>)> =
            HashMap::new();
        for (key, tmpl) in &hops {
            let stem = key.dex.file_stem();
            let (_, map) =
                by_dex.entry(stem).or_insert_with(|| (key.dex.clone(), HashMap::new()));
            map.insert(key.template_id(), tmpl.clone());
        }

        let hops_dir = format!("{BASE_DIR}/hops");
        let _ = std::fs::create_dir_all(&hops_dir);

        for (stem, (dex, templates)) in by_dex {
            let path = format!("{hops_dir}/{stem}.json");
            let file = DexHopFile { schema_version: 1, dex, templates };
            let Ok(json) = serde_json::to_string_pretty(&file) else { continue };
            let tmp = format!("{path}.tmp");
            if std::fs::write(&tmp, json.as_bytes()).is_ok() {
                let _ = std::fs::rename(&tmp, &path);
            }
        }
    }

    pub fn spawn_flush_task(self: &Arc<Self>, secs: u64) {
        let this = self.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(secs.max(1)));
            interval.tick().await;
            loop {
                interval.tick().await;
                let c = this.clone();
                let _ = tokio::task::spawn_blocking(move || c.flush_to_disk()).await;
            }
        });
    }
}

// ─── Disk format ──────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
struct DexHopFile {
    schema_version: u32,
    dex: DexKind,
    /// Keyed by template_id() so repeated pool/direction never adds a second row.
    templates: HashMap<String, HopTemplate>,
}

/// On-disk form of a RouteTemplate. `route_signature` is stored as a decimal
/// string (JSON has no native u128) and the runtime-only `amount_variants`
/// cache is omitted — patchable routes rebuild any amount from the base.
#[derive(Serialize, Deserialize)]
struct RouteRecord {
    sig: String,
    swap_ixs: SwapInstructionsResponse,
    template_in_amount: u64,
    template_quoted_out: u64,
    in_amount_offset: Option<usize>,
    quoted_out_offset: Option<usize>,
    seen_count: u64,
    hit_count: u64,
}

impl RouteRecord {
    fn from_template(t: &RouteTemplate) -> Self {
        RouteRecord {
            sig: t.route_signature.to_string(),
            swap_ixs: t.swap_ixs.clone(),
            template_in_amount: t.template_in_amount,
            template_quoted_out: t.template_quoted_out,
            in_amount_offset: t.in_amount_offset,
            quoted_out_offset: t.quoted_out_offset,
            seen_count: t.seen_count,
            hit_count: t.hit_count,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct RouteFile {
    schema_version: u32,
    routes: Vec<RouteRecord>,
    /// "hop1_id|hop2_id" -> route_sig (decimal string). Rebuilds the Tier-2
    /// secondary index so hop-pair lookups work on a fresh process.
    hop_pairs: HashMap<String, String>,
}
