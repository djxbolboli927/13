//! Pump.fun AMM (PumpSwap) pricing — constant-product `x*y=k`, Uniswap-v2 style.
//!
//! For a canonical pool `base_mint` is the token and `quote_mint` is WSOL, so
//! `price_in_sol = quote_reserve / base_reserve`. Reserves are NOT stored in the
//! pool account; they are the SPL-token `amount` of the two vaults
//! (`pool_base_token_account`, `pool_quote_token_account`).
//!
//! Fees (all levied on the QUOTE / SOL leg) are DYNAMIC since pump.fun's
//! "Dynamic Fees" update: the total fee is tiered by MARKET CAP, from 1.25%
//! on tiny pools down to 0.30% above ~98,240 SOL market cap. The tier table
//! below is the official schedule (pump-public-docs/docs/fees.png); market cap
//! in lamports for a PumpSwap pool is
//! `quote_reserve * base_mint_supply / base_reserve`, where base_mint_supply is
//! the token's REAL on-chain mint supply (read from the mint account, NOT
//! assumed) — a wrong supply picks the wrong fee tier and fabricates profit.
//! On a `buy` the fees are added ON TOP of the pool-bound input; on a `sell`
//! they are subtracted FROM the gross output. All fee roundings are ceiling,
//! matching the on-chain program (pool-favorable). Integer math throughout.

const BPS_DENOM: u64 = 10_000;

const LAMPORTS_PER_SOL: u128 = 1_000_000_000;

/// PumpSwap fee tiers read from the ACTUAL on-chain `FeeConfig` account at
/// startup, ascending by market-cap threshold **in lamports**:
/// `(mcap_threshold_lamports, total_bps, lp_bps)`. This is the exact schedule
/// the on-chain Fee Program applies (canonical PumpSwap pools). When present it
/// overrides the hardcoded `FEE_TIERS` table below, so the fee auto-tracks any
/// change Pump makes to the tiers — no code change, no guessing.
static ONCHAIN_TIERS: std::sync::OnceLock<Vec<(u128, u64, u64)>> = std::sync::OnceLock::new();

/// Install the on-chain PumpSwap fee tiers (called once at startup after the
/// FeeConfig account is read + validated). Ascending by lamport threshold.
pub fn set_onchain_fee_tiers(tiers: Vec<(u128, u64, u64)>) {
    let _ = ONCHAIN_TIERS.set(tiers);
}

/// The on-chain `flat_fees` (`(total_bps, lp_bps)`) that the Fee Program applies
/// to NON-canonical pools (`is_pump_pool == false`) — those charge a flat fee
/// and IGNORE market cap. Read from the FeeConfig at startup.
static ONCHAIN_FLAT: std::sync::OnceLock<(u64, u64)> = std::sync::OnceLock::new();

/// Install the on-chain flat fee (total_bps, lp_bps) for non-canonical pools.
pub fn set_onchain_flat_fee(total_bps: u64, lp_bps: u64) {
    let _ = ONCHAIN_FLAT.set((total_bps, lp_bps));
}

/// Whether on-chain tiers have been installed (for the audit log).
pub fn onchain_fee_tiers_loaded() -> bool {
    ONCHAIN_TIERS.get().map(|t| !t.is_empty()).unwrap_or(false)
}

/// `calculateFeeTier` over the on-chain tiers (thresholds in lamports, ascending):
/// the highest-threshold tier whose threshold ≤ mcap; if mcap is below the first
/// threshold, the first (highest-fee) tier. Returns `(total_bps, lp_bps)`.
fn fee_from_onchain_tiers(mcap_lamports: u128) -> Option<(u64, u64)> {
    let tiers = ONCHAIN_TIERS.get()?;
    if tiers.is_empty() {
        return None;
    }
    if mcap_lamports < tiers[0].0 {
        return Some((tiers[0].1, tiers[0].2));
    }
    let mut chosen = (tiers[0].1, tiers[0].2);
    for &(thresh, total, lp) in tiers {
        if mcap_lamports >= thresh {
            chosen = (total, lp);
        } else {
            break;
        }
    }
    Some(chosen)
}

/// Official PumpSwap dynamic-fee schedule, DESCENDING by market-cap threshold:
/// `(mcap_threshold_sol, total_fee_bps, lp_fee_bps)`. The row whose threshold
/// the market cap meets first applies. Fractional-bps totals (e.g. 0.525%) are
/// rounded UP so the fee is never understated. LP fee is 20 bps in every tier
/// except the lowest (2 bps).
const FEE_TIERS: &[(u64, u64, u64)] = &[
    (98_240, 30, 20),
    (93_330, 33, 20),
    (88_400, 35, 20),
    (83_500, 38, 20),
    (78_590, 40, 20),
    (73_681, 43, 20),
    (68_770, 45, 20),
    (63_860, 48, 20),
    (58_940, 50, 20),
    (54_030, 53, 20),
    (49_120, 55, 20),
    (44_210, 60, 20),
    (39_300, 65, 20),
    (34_380, 70, 20),
    (29_470, 75, 20),
    (24_560, 80, 20),
    (19_650, 85, 20),
    (14_740, 90, 20),
    (9_820, 95, 20),
    (4_420, 100, 20),
    (3_440, 105, 20),
    (2_460, 110, 20),
    (1_470, 115, 20),
    (420, 120, 20),
    (0, 125, 2),
];

/// `(total_fee_bps, lp_fee_bps)` for a PumpSwap pool given its reserves and the
/// base token's ACTUAL mint supply (base units), per the official market-cap
/// tier schedule: `market_cap = quote_reserve * base_mint_supply / base_reserve`
/// (pump-public-docs `poolMarketCap`).
///
/// `supply_base_units == 0` means we don't yet know the real supply — FAIL
/// CLOSED to the HIGHEST fee tier (lowest market cap), so the fee is never
/// understated. This replaces the old hardcoded 1e15-supply assumption, which
/// on tokens with a different supply/decimals produced the wrong tier → an
/// understated fee → phantom profit → reverts.
pub fn fee_for_reserves(
    base_reserve: u64,
    quote_reserve: u64,
    supply_base_units: u128,
    is_canonical: bool,
) -> (u64, u64) {
    // NON-canonical pools (`is_pump_pool == false`) charge the on-chain FLAT fee
    // and IGNORE market cap. Running them through the market-cap tier schedule
    // over-states the fee (up to 125 bps vs the real ~30 bps), which under-
    // predicts profit and HIDES real opportunities on those pools. Empirically
    // (verified across every observed sell/buy event) `is_pump_pool` is true iff
    // the pool's `coin_creator` is set (non-default); the caller passes that as
    // `is_canonical`.
    if !is_canonical {
        if let Some(&(total, lp)) = ONCHAIN_FLAT.get() {
            return (total, lp);
        }
        // No on-chain flat fee read yet → the known flat schedule: lp 25 +
        // protocol 5 = 30 bps total, lp portion 25.
        return (30, 25);
    }
    let highest = (
        FEE_TIERS[FEE_TIERS.len() - 1].1,
        FEE_TIERS[FEE_TIERS.len() - 1].2,
    );
    if base_reserve == 0 || supply_base_units == 0 {
        return highest; // unknown market cap → highest fee
    }
    let mcap_lamports =
        (quote_reserve as u128).saturating_mul(supply_base_units) / base_reserve as u128;
    // Prefer the ACTUAL on-chain fee tiers (read from FeeConfig at startup).
    if let Some(f) = fee_from_onchain_tiers(mcap_lamports) {
        return f;
    }
    // Fallback: the hardcoded schedule (matches the published fees.png tiers).
    let mcap_sol = (mcap_lamports / LAMPORTS_PER_SOL).min(u64::MAX as u128) as u64;
    for &(thresh, total, lp) in FEE_TIERS {
        if mcap_sol >= thresh {
            return (total, lp);
        }
    }
    (125, 2)
}

/// Read + validate the on-chain PumpSwap `FeeConfig` and install its fee tiers.
/// Best-effort: on any RPC/parse/sanity failure we keep the hardcoded schedule.
///
/// FeeConfig account (Fee Program `pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ`,
/// PDA seeds `["fee_config", pAMMBay…]`) borsh layout after the 8-byte anchor
/// discriminator: `bump u8`, `admin Pubkey(32)`, `flat_fees Fees(3×u64=24)`,
/// `fee_tiers Vec<FeeTier>` (u32 len + N×40), `stable_fee_tiers Vec<FeeTier>`.
/// `FeeTier { market_cap_lamports_threshold u128(16), fees Fees(24) }`;
/// `Fees { lp_fee_bps u64, protocol_fee_bps u64, creator_fee_bps u64 }`.
pub fn load_onchain_fee_tiers(
    rpc: &solana_client::rpc_client::RpcClient,
) -> Option<Vec<(u128, u64, u64)>> {
    use solana_sdk::pubkey::Pubkey;
    let fee_program = Pubkey::from_str_const("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ");
    let pamm = Pubkey::from_str_const("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA");
    let (fee_config, _) =
        Pubkey::find_program_address(&[b"fee_config", pamm.as_ref()], &fee_program);
    let acct = rpc.get_account(&fee_config).ok()?;
    let d = &acct.data;
    // flat_fees `Fees` sits at bytes 41..65 (8 disc + 1 bump + 32 admin):
    //   lp_fee_bps u64 @41, protocol_fee_bps u64 @49, creator_fee_bps u64 @57.
    // Non-canonical pools (is_pump_pool == false) charge exactly this flat fee.
    if let (Some(lp), Some(protocol), Some(creator)) = (
        d.get(41..49).and_then(|s| s.try_into().ok()).map(u64::from_le_bytes),
        d.get(49..57).and_then(|s| s.try_into().ok()).map(u64::from_le_bytes),
        d.get(57..65).and_then(|s| s.try_into().ok()).map(u64::from_le_bytes),
    ) {
        let flat_total = lp.saturating_add(protocol).saturating_add(creator);
        // Sanity: a plausible flat fee (≤10%); else leave the fallback in place.
        if flat_total > 0 && flat_total <= 1_000 && lp <= 1_000 {
            set_onchain_flat_fee(flat_total, lp);
        }
    }
    // 8 disc + 1 bump + 32 admin + 24 flat_fees = 65, then the fee_tiers Vec.
    let mut off = 65usize;
    let len = u32::from_le_bytes(d.get(off..off + 4)?.try_into().ok()?) as usize;
    off += 4;
    if len == 0 || len > 64 {
        return None; // implausible → wrong offset/layout, bail to fallback
    }
    let mut tiers: Vec<(u128, u64, u64)> = Vec::with_capacity(len);
    for _ in 0..len {
        let threshold = u128::from_le_bytes(d.get(off..off + 16)?.try_into().ok()?);
        let lp = u64::from_le_bytes(d.get(off + 16..off + 24)?.try_into().ok()?);
        let protocol = u64::from_le_bytes(d.get(off + 24..off + 32)?.try_into().ok()?);
        let creator = u64::from_le_bytes(d.get(off + 32..off + 40)?.try_into().ok()?);
        off += 40;
        let total = lp.saturating_add(protocol).saturating_add(creator);
        // Sanity: fees never exceed 10% and each component is a plausible bps.
        if total > 1_000 || lp > 1_000 || protocol > 1_000 || creator > 1_000 {
            return None;
        }
        tiers.push((threshold, total, lp));
    }
    // Thresholds must be non-decreasing (ascending schedule).
    if tiers.windows(2).any(|w| w[1].0 < w[0].0) {
        return None;
    }
    Some(tiers)
}

#[inline]
fn ceil_div(a: u128, b: u128) -> u128 {
    if a == 0 {
        0
    } else {
        1 + (a - 1) / b
    }
}

/// Reserves of a Pump.fun AMM pool. `base` = token, `quote` = WSOL (lamports).
///
/// NOTE on orientation: reserves are ALWAYS normalized so `base_reserve` is the
/// meme token and `quote_reserve` is WSOL, regardless of the pool's on-chain
/// `base_mint`/`quote_mint` naming — the caller keys them off the resolved
/// token/WSOL vaults. The Pump.fun AMM levies its fee on the on-chain QUOTE
/// mint. On a CANONICAL pool the quote mint IS WSOL, so the fee hits our
/// `quote_reserve` (WSOL) — `fee_on_wsol_leg = true`. On a FLIPPED / non-canonical
/// pool (`base_mint = WSOL`, `quote_mint = token`) the fee hits the TOKEN leg,
/// i.e. our `base_reserve` — `fee_on_wsol_leg = false`. Getting this side right
/// matters: on the real HOOD flipped tx the token-side fee reproduces the
/// on-chain output to ~1 lamport, vs ~24 lamports if charged on WSOL.
#[derive(Debug, Clone, Copy)]
pub struct PumpPool {
    pub base_reserve: u64,
    pub quote_reserve: u64,
    /// Total fee in bps — dynamic, selected from the market-cap tier schedule.
    pub total_fee_bps: u64,
    /// LP portion of the fee that stays in the pool.
    pub lp_fee_bps: u64,
    /// Which leg the on-chain fee is levied on. `true` = WSOL (`quote_reserve`),
    /// the canonical case. `false` = token (`base_reserve`), the flipped /
    /// non-canonical case. Defaults to `true`; the pool-state layer flips it
    /// after reading the on-chain orientation (`pump_base_is_wsol`).
    pub fee_on_wsol_leg: bool,
}

impl PumpPool {
    /// `supply_base_units` is the base token's real mint supply (0 = unknown →
    /// highest fee tier, fail-closed). `is_canonical` = the pool is a canonical
    /// pump pool (`is_pump_pool == true`, i.e. its `coin_creator` is set); false
    /// selects the flat fee instead of the market-cap tier.
    ///
    /// `fee_on_wsol_leg` defaults to `true` (fee on WSOL). Callers that know the
    /// pool is FLIPPED set the field to `false` after construction.
    pub fn new(
        base_reserve: u64,
        quote_reserve: u64,
        supply_base_units: u128,
        is_canonical: bool,
    ) -> Self {
        let (total_fee_bps, lp_fee_bps) =
            fee_for_reserves(base_reserve, quote_reserve, supply_base_units, is_canonical);
        Self {
            base_reserve,
            quote_reserve,
            total_fee_bps,
            lp_fee_bps,
            fee_on_wsol_leg: true,
        }
    }

    /// Spot price in SOL per whole token, ignoring fees/impact. For logging.
    pub fn spot_price(&self, base_decimals: u32, quote_decimals: u32) -> f64 {
        if self.base_reserve == 0 {
            return 0.0;
        }
        let raw = self.quote_reserve as f64 / self.base_reserve as f64;
        raw * 10f64.powi(base_decimals as i32 - quote_decimals as i32)
    }

    // ── Quoting: our own arbitrage legs ──────────────────────────────────────

    /// BUY: spend `quote_in_budget` lamports of WSOL (fees included), receive
    /// token base. Returns `base_out`.
    pub fn quote_buy(&self, quote_in_budget: u64) -> u64 {
        let b = self.base_reserve as u128;
        let q = self.quote_reserve as u128;
        if quote_in_budget == 0 {
            return 0;
        }
        if self.fee_on_wsol_leg {
            // Canonical: fee is on the WSOL leg, added on top of the pool-bound
            // input — strip it, then the whole net WSOL enters the pool.
            let pool_quote_in = (quote_in_budget as u128) * BPS_DENOM as u128
                / (BPS_DENOM + self.total_fee_bps) as u128;
            if pool_quote_in == 0 {
                return 0;
            }
            // base_out = floor(B * qin / (Q + qin))
            let out = b * pool_quote_in / (q + pool_quote_in);
            out.min(u64::MAX as u128) as u64
        } else {
            // Flipped: fee is on the TOKEN (base) output leg. The full WSOL
            // enters the pool; the fee is deducted from the gross token out.
            let qin = quote_in_budget as u128;
            let gross = b * qin / (q + qin);
            let fee = ceil_div(gross * self.total_fee_bps as u128, BPS_DENOM as u128);
            gross.saturating_sub(fee).min(u64::MAX as u128) as u64
        }
    }

    /// SELL: spend `base_in` token, receive WSOL. Returns net lamports out
    /// (after all fees).
    pub fn quote_sell(&self, base_in: u64) -> u64 {
        let b = self.base_reserve as u128;
        let q = self.quote_reserve as u128;
        let bi = base_in as u128;
        if bi == 0 {
            return 0;
        }
        if self.fee_on_wsol_leg {
            // Canonical: fee on the WSOL output leg.
            let gross = q * bi / (b + bi); // floor
            let fee = ceil_div(gross * self.total_fee_bps as u128, BPS_DENOM as u128);
            gross.saturating_sub(fee).min(u64::MAX as u128) as u64
        } else {
            // Flipped: fee on the TOKEN input leg (on-chain buy: token is the
            // quote, fee added on top of the pool-bound token). Strip the fee
            // from the token input, then swap the net token for WSOL (no fee on
            // the WSOL output). Matches the real HOOD tx to ~1 lamport.
            let pool_bi = bi * BPS_DENOM as u128 / (BPS_DENOM + self.total_fee_bps) as u128;
            if pool_bi == 0 {
                return 0;
            }
            let out = q * pool_bi / (b + pool_bi);
            out.min(u64::MAX as u128) as u64
        }
    }

    // ── Prediction: apply a swap we OBSERVED on ShredStream ──────────────────

    /// Apply an observed `buy` (someone bought `base_amount_out` token with
    /// WSOL) and return the pool AFTER the trade. `base_amount_out` is the
    /// exact-out arg carried by the on-chain `buy` instruction.
    pub fn after_observed_buy(&self, base_amount_out: u64) -> PumpPool {
        let b = self.base_reserve as u128;
        let q = self.quote_reserve as u128;
        let out = (base_amount_out as u128).min(b.saturating_sub(1));
        if out == 0 {
            return *self;
        }
        if self.fee_on_wsol_leg {
            // Canonical: WSOL enters, lp fee stays in the WSOL (quote) reserve.
            // quote that must enter the pool for that base out: ceil(Q*out/(B-out))
            let quote_in = ceil_div(q * out, b - out);
            let lp_fee = ceil_div(quote_in * self.lp_fee_bps as u128, BPS_DENOM as u128);
            let new_base = (b - out) as u64;
            let new_quote = (q + quote_in + lp_fee).min(u64::MAX as u128) as u64;
            PumpPool {
                base_reserve: new_base,
                quote_reserve: new_quote,
                ..*self
            }
        } else {
            // Flipped: on-chain SELL (WSOL in, token out); fee on the token
            // output. `out` is what the buyer RECEIVED (gross − total fee), so
            // the gross token leaving the curve is larger; the lp portion stays
            // in the token reserve, the rest of the token leaves.
            let denom = (BPS_DENOM as u128).saturating_sub(self.total_fee_bps as u128);
            if denom == 0 {
                return *self;
            }
            let gross = (ceil_div(out * BPS_DENOM as u128, denom)).min(b.saturating_sub(1));
            let lp_fee = gross * self.lp_fee_bps as u128 / BPS_DENOM as u128; // stays in token
            // WSOL that entered the pool for that gross token out.
            let wsol_in = ceil_div(q * gross, b - gross);
            let net_token_leaving = gross.saturating_sub(lp_fee);
            let new_base = b.saturating_sub(net_token_leaving) as u64;
            let new_quote = (q + wsol_in).min(u64::MAX as u128) as u64;
            PumpPool {
                base_reserve: new_base,
                quote_reserve: new_quote,
                ..*self
            }
        }
    }

    /// Apply an observed `sell` (someone sold `base_amount_in` token for WSOL)
    /// and return the pool AFTER the trade.
    pub fn after_observed_sell(&self, base_amount_in: u64) -> PumpPool {
        let b = self.base_reserve as u128;
        let q = self.quote_reserve as u128;
        let bi = base_amount_in as u128;
        if bi == 0 {
            return *self;
        }
        if self.fee_on_wsol_leg {
            // Canonical: token enters, WSOL leaves; fee (lp) on the WSOL leg —
            // lp stays in the pool, so quote only drops by (gross − lp_fee).
            let gross = q * bi / (b + bi); // floor
            let lp_fee = ceil_div(gross * self.lp_fee_bps as u128, BPS_DENOM as u128);
            let new_base = (b + bi).min(u64::MAX as u128) as u64;
            let new_quote = q.saturating_sub(gross - lp_fee) as u64;
            PumpPool {
                base_reserve: new_base,
                quote_reserve: new_quote,
                ..*self
            }
        } else {
            // Flipped: on-chain BUY (token in, WSOL out); fee on the token input
            // (added on top). `bi` is the total token spent; strip the fee to
            // get the pool-bound token, the lp portion of which stays in the
            // token reserve. WSOL leaves for the pool-bound token.
            let pool_bi = bi * BPS_DENOM as u128 / (BPS_DENOM + self.total_fee_bps) as u128;
            if pool_bi == 0 {
                return *self;
            }
            let lp_fee = pool_bi * self.lp_fee_bps as u128 / BPS_DENOM as u128; // stays in token
            let wsol_out = q * pool_bi / (b + pool_bi);
            let new_base = (b + pool_bi + lp_fee).min(u64::MAX as u128) as u64;
            let new_quote = q.saturating_sub(wsol_out) as u64;
            PumpPool {
                base_reserve: new_base,
                quote_reserve: new_quote,
                ..*self
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buy_then_sell_loses_to_fees() {
        let p = PumpPool::new(1_000_000_000, 200_000_000_000, 1_000_000_000_000_000, true);
        let base_out = p.quote_buy(1_000_000_000);
        assert!(base_out > 0);
        let back = p.quote_sell(base_out);
        // round-trip on one pool must lose to the double fee
        assert!(back < 1_000_000_000);
    }

    #[test]
    fn observed_buy_raises_price() {
        let p = PumpPool::new(1_000_000_000, 200_000_000_000, 1_000_000_000_000_000, true);
        let before = p.spot_price(6, 9);
        let after = p.after_observed_buy(10_000_000).spot_price(6, 9);
        assert!(after > before);
    }

    #[test]
    fn flipped_pool_fee_on_token_reproduces_real_tx() {
        // Real FLIPPED pool tx (token HOOD, pump pool HZeyjnj8…): on-chain
        // `buy_exact_quote_in` spending 874_763_863 HOOD (the on-chain QUOTE) to
        // receive 269_778 WSOL (the on-chain BASE). From OUR token frame that is a
        // token SELL. Reserves normalized to (base=token, quote=WSOL):
        //   base_reserve  = HOOD vault = poolQuoteTokenReserves = 7_186_972_841_433_516
        //   quote_reserve = WSOL vault = poolBaseTokenReserves  = 2_223_125_529_798
        // Non-canonical → flat 30 bps (lp 25 + protocol 5). On a flipped pool the
        // fee is on the TOKEN leg. The token-side fee must reproduce the on-chain
        // 269_778 WSOL out to within a couple lamports (WSOL-side fee is ~24 off).
        let mut p = PumpPool::new(7_186_972_841_433_516, 2_223_125_529_798, 0, false);
        assert_eq!(p.total_fee_bps, 30, "non-canonical → flat 30 bps");
        p.fee_on_wsol_leg = false; // flipped: fee on token
        let wsol_out = p.quote_sell(874_763_863);
        assert!(
            (wsol_out as i64 - 269_778).abs() <= 3,
            "flipped token-fee sell should reproduce ~269778 WSOL, got {wsol_out}"
        );
    }

    #[test]
    fn flipped_fee_side_beats_wsol_side_on_real_tx() {
        // Same pool: the WSOL-side (canonical) fee model is measurably worse on a
        // flipped pool, proving the fee side matters (the user's point).
        let mut flip = PumpPool::new(7_186_972_841_433_516, 2_223_125_529_798, 0, false);
        flip.fee_on_wsol_leg = false;
        let wrong = PumpPool::new(7_186_972_841_433_516, 2_223_125_529_798, 0, false); // fee_on_wsol_leg = true
        let flipped_err = (flip.quote_sell(874_763_863) as i64 - 269_778).abs();
        let wsol_err = (wrong.quote_sell(874_763_863) as i64 - 269_778).abs();
        assert!(flipped_err < wsol_err, "token-side fee ({flipped_err}) must beat WSOL-side ({wsol_err})");
    }

    #[test]
    fn flipped_observed_swap_roundtrip_sane() {
        // A flipped-pool observed buy then sell should move reserves in the right
        // direction and not panic / overflow.
        let mut p = PumpPool::new(7_186_972_841_433_516, 2_223_125_529_798, 0, false);
        p.fee_on_wsol_leg = false;
        let after_buy = p.after_observed_buy(1_000_000); // token leaves
        assert!(after_buy.base_reserve < p.base_reserve);
        assert!(after_buy.quote_reserve > p.quote_reserve);
        let after_sell = p.after_observed_sell(1_000_000_000); // token enters
        assert!(after_sell.base_reserve > p.base_reserve);
        assert!(after_sell.quote_reserve < p.quote_reserve);
    }
}
