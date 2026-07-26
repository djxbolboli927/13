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
/// `(mcap_threshold_lamports, lp_bps, protocol_bps, creator_bps)` — the three
/// fee components stored SEPARATELY so each can be ceiled individually exactly
/// like the on-chain program (a single combined ceil can be 1-2 lamports off).
static ONCHAIN_TIERS: std::sync::OnceLock<Vec<(u128, u64, u64, u64)>> =
    std::sync::OnceLock::new();

/// Install the on-chain PumpSwap fee tiers (called once at startup after the
/// FeeConfig account is read + validated). Ascending by lamport threshold.
pub fn set_onchain_fee_tiers(tiers: Vec<(u128, u64, u64, u64)>) {
    let _ = ONCHAIN_TIERS.set(tiers);
}

/// Whether on-chain tiers have been installed (for the audit log).
pub fn onchain_fee_tiers_loaded() -> bool {
    ONCHAIN_TIERS.get().map(|t| !t.is_empty()).unwrap_or(false)
}

/// `calculateFeeTier` over the on-chain tiers (thresholds in lamports, ascending):
/// the highest-threshold tier whose threshold ≤ mcap; if mcap is below the first
/// threshold, the first (highest-fee) tier. Returns `(lp_bps, protocol_bps,
/// creator_bps)`.
fn fee_from_onchain_tiers(mcap_lamports: u128) -> Option<(u64, u64, u64)> {
    let tiers = ONCHAIN_TIERS.get()?;
    if tiers.is_empty() {
        return None;
    }
    if mcap_lamports < tiers[0].0 {
        return Some((tiers[0].1, tiers[0].2, tiers[0].3));
    }
    let mut chosen = (tiers[0].1, tiers[0].2, tiers[0].3);
    for &(thresh, lp, protocol, creator) in tiers {
        if mcap_lamports >= thresh {
            chosen = (lp, protocol, creator);
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

/// The on-chain `flat_fees` (`(total_bps, lp_bps)`) that the Fee Program applies
/// to NON-canonical pools (`is_pump_pool == false`) — those charge a flat fee
/// and IGNORE market cap. Read from the FeeConfig at startup. Every INVERTED
/// pool (base = WSOL) is non-canonical, so this covers those too.
static ONCHAIN_FLAT: std::sync::OnceLock<(u64, u64, u64)> = std::sync::OnceLock::new();

/// Install the on-chain flat fee `(lp_bps, protocol_bps, creator_bps)` for
/// non-canonical pools.
pub fn set_onchain_flat_fee(lp_bps: u64, protocol_bps: u64, creator_bps: u64) {
    let _ = ONCHAIN_FLAT.set((lp_bps, protocol_bps, creator_bps));
}

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
) -> (u64, u64, u64) {
    // NON-canonical pools (`is_pump_pool == false`) charge the on-chain FLAT fee
    // and IGNORE market cap. Running them through the market-cap tier schedule
    // over-states the fee (up to 125 bps vs the real ~30 bps), which mis-sizes
    // the trade. Empirically (verified across observed sell/buy events)
    // `is_pump_pool` is true iff the pool's `coin_creator` is set (non-default);
    // the caller passes that as `is_canonical`.
    if !is_canonical {
        if let Some(&(lp, protocol, creator)) = ONCHAIN_FLAT.get() {
            return (lp, protocol, creator);
        }
        // No on-chain flat fee read yet → the observed flat schedule (real
        // WSOL-HOOD / WSOL-GUS sell events): lp 25 + protocol 5 + creator 0.
        return (25, 5, 0);
    }
    // Hardcoded fallback carries only (total, lp); split the rest into protocol
    // (creator 0) — only used if the on-chain FeeConfig read failed.
    let hi = FEE_TIERS[FEE_TIERS.len() - 1];
    let split = |total: u64, lp: u64| -> (u64, u64, u64) { (lp, total.saturating_sub(lp), 0) };
    if base_reserve == 0 || supply_base_units == 0 {
        return split(hi.1, hi.2); // unknown market cap → highest fee
    }
    let mcap_lamports =
        (quote_reserve as u128).saturating_mul(supply_base_units) / base_reserve as u128;
    // Prefer the ACTUAL on-chain fee tiers (read from FeeConfig at startup).
    if let Some(f) = fee_from_onchain_tiers(mcap_lamports) {
        return f;
    }
    let mcap_sol = (mcap_lamports / LAMPORTS_PER_SOL).min(u64::MAX as u128) as u64;
    for &(thresh, total, lp) in FEE_TIERS {
        if mcap_sol >= thresh {
            return split(total, lp);
        }
    }
    (2, 123, 0)
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
) -> Option<Vec<(u128, u64, u64, u64)>> {
    use solana_sdk::pubkey::Pubkey;
    let fee_program = Pubkey::from_str_const("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ");
    let pamm = Pubkey::from_str_const("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA");
    let (fee_config, _) =
        Pubkey::find_program_address(&[b"fee_config", pamm.as_ref()], &fee_program);
    let acct = rpc.get_account(&fee_config).ok()?;
    let d = &acct.data;
    // flat_fees (Fees = lp/protocol/creator u64 bps) sits at [41..65] — the fee
    // charged to NON-CANONICAL pools (all inverted pools among them).
    {
        let rd = |o: usize| -> Option<u64> {
            d.get(o..o + 8).map(|s| u64::from_le_bytes(s.try_into().unwrap()))
        };
        if let (Some(lp), Some(protocol), Some(creator)) = (rd(41), rd(49), rd(57)) {
            let total = lp.saturating_add(protocol).saturating_add(creator);
            if total > 0 && total <= 1_000 && lp <= 1_000 && protocol <= 1_000 && creator <= 1_000 {
                set_onchain_flat_fee(lp, protocol, creator);
            }
        }
    }
    // 8 disc + 1 bump + 32 admin + 24 flat_fees = 65, then the fee_tiers Vec.
    let mut off = 65usize;
    let len = u32::from_le_bytes(d.get(off..off + 4)?.try_into().ok()?) as usize;
    off += 4;
    if len == 0 || len > 64 {
        return None; // implausible → wrong offset/layout, bail to fallback
    }
    let mut tiers: Vec<(u128, u64, u64, u64)> = Vec::with_capacity(len);
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
        tiers.push((threshold, lp, protocol, creator));
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
#[derive(Debug, Clone, Copy)]
pub struct PumpPool {
    pub base_reserve: u64,
    pub quote_reserve: u64,
    /// Total fee in bps = lp + protocol + creator (kept for logging / callers).
    pub total_fee_bps: u64,
    /// LP portion — the ONLY fee that stays in the pool vault.
    pub lp_fee_bps: u64,
    /// Protocol portion — leaves the pool to the protocol fee account.
    pub protocol_fee_bps: u64,
    /// Coin-creator portion — leaves the pool to the creator vault (0 on
    /// non-canonical / inverted pools).
    pub creator_fee_bps: u64,
}

impl PumpPool {
    /// `supply_base_units` is the base token's real mint supply (0 = unknown →
    /// highest fee tier, fail-closed). `is_canonical` = the pool is a canonical
    /// pump pool (`is_pump_pool == true`, i.e. its `coin_creator` is set); false
    /// selects the flat fee instead of the market-cap tier.
    pub fn new(
        base_reserve: u64,
        quote_reserve: u64,
        supply_base_units: u128,
        is_canonical: bool,
    ) -> Self {
        let (lp_fee_bps, protocol_fee_bps, creator_fee_bps) =
            fee_for_reserves(base_reserve, quote_reserve, supply_base_units, is_canonical);
        Self {
            base_reserve,
            quote_reserve,
            total_fee_bps: lp_fee_bps + protocol_fee_bps + creator_fee_bps,
            lp_fee_bps,
            protocol_fee_bps,
            creator_fee_bps,
        }
    }

    /// The three swap fees on `amount`, each ceiled INDIVIDUALLY (128-bit) —
    /// exactly as the on-chain program does: `ceilDiv(amount*bps, 10000)` per
    /// component. Returns `(lp, protocol, creator)`.
    #[inline]
    fn fees_on(&self, amount: u128) -> (u128, u128, u128) {
        (
            ceil_div(amount * self.lp_fee_bps as u128, BPS_DENOM as u128),
            ceil_div(amount * self.protocol_fee_bps as u128, BPS_DENOM as u128),
            ceil_div(amount * self.creator_fee_bps as u128, BPS_DENOM as u128),
        )
    }

    /// RAW-orientation view for INVERTED pools (program base_mint = WSOL):
    /// swaps the reserve roles so the raw on-chain buy/sell math — base-side
    /// exact amounts, fees levied on the QUOTE side — applies verbatim. Flip,
    /// run the raw math, flip back. Fees carry over unchanged.
    pub fn flipped(&self) -> PumpPool {
        PumpPool {
            base_reserve: self.quote_reserve,
            quote_reserve: self.base_reserve,
            ..*self
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

    /// BUY exact-quote-in: spend `spendable` lamports of WSOL (fees included),
    /// receive token base. Reproduces the on-chain `buy_exact_quote_in` math to
    /// the lamport: floor the effective quote, ceil each fee, correct any dust
    /// overshoot, then the curve runs on `effective - 1` (the program's `-1`).
    pub fn quote_buy(&self, spendable: u64) -> u64 {
        self.buy_quote_in_parts(spendable).0
    }

    /// Shared core for `buy_exact_quote_in`: returns
    /// `(base_out, input_amount, lp_fee)` — `input_amount` and `lp_fee` are what
    /// the pool quote vault gains (`+= input_amount + lp_fee`).
    fn buy_quote_in_parts(&self, spendable: u64) -> (u64, u64, u64) {
        let total_bps = self.total_fee_bps as u128;
        let mut eff = (spendable as u128) * BPS_DENOM as u128 / (BPS_DENOM as u128 + total_bps);
        if eff == 0 {
            return (0, 0, 0);
        }
        let (lp, protocol, creator) = self.fees_on(eff);
        // Dust correction: if effective + fees overshoot the spendable, shrink.
        let total_with_fees = eff + lp + protocol + creator;
        if total_with_fees > spendable as u128 {
            eff = eff.saturating_sub(total_with_fees - spendable as u128);
        }
        // The curve runs on `effective - 1` (verbatim from the program port).
        let input_amount = if eff > 0 { eff - 1 } else { 0 };
        if input_amount == 0 {
            return (0, 0, 0);
        }
        let b = self.base_reserve as u128;
        let q = self.quote_reserve as u128;
        let base_out = (b * input_amount / (q + input_amount)).min(b.saturating_sub(1));
        // LP fee retained in the pool is computed on the (post-dust) effective.
        let lp_fee = ceil_div(eff * self.lp_fee_bps as u128, BPS_DENOM as u128);
        (
            base_out.min(u64::MAX as u128) as u64,
            input_amount.min(u64::MAX as u128) as u64,
            lp_fee.min(u64::MAX as u128) as u64,
        )
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
        let gross = q * bi / (b + bi); // floor
        // Each fee component ceiled individually, exactly like on-chain.
        let (lp, protocol, creator) = self.fees_on(gross);
        gross.saturating_sub(lp + protocol + creator)
            .min(u64::MAX as u128) as u64
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
        // quote that must enter the pool for that base out: ceil(Q*out/(B-out)).
        // Only the LP fee is retained in the vault; protocol + creator leave it.
        let quote_in = ceil_div(q * out, b - out);
        let lp_fee = ceil_div(quote_in * self.lp_fee_bps as u128, BPS_DENOM as u128);
        let new_base = (b - out) as u64;
        let new_quote = (q + quote_in + lp_fee).min(u64::MAX as u128) as u64;
        PumpPool {
            base_reserve: new_base,
            quote_reserve: new_quote,
            ..*self
        }
    }

    // ── Phase-1 sim: a competitor tx's OWN leg output, for the revert verdict ─

    /// SIM `buy` (exact base out): the quote lamports the user must pay for
    /// `base_amount_out`, all fees added on top. Revert iff this exceeds the
    /// tx's `max_quote_amount_in`.
    pub fn sim_buy_quote_in(&self, base_amount_out: u64) -> u64 {
        let b = self.base_reserve as u128;
        let q = self.quote_reserve as u128;
        let out = (base_amount_out as u128).min(b.saturating_sub(1));
        if out == 0 {
            return 0;
        }
        // Pool quote-in required for the exact base out, then add each fee
        // (ceiled individually) — the total the user must pay.
        let quote_in = ceil_div(q * out, b - out);
        let (lp, protocol, creator) = self.fees_on(quote_in);
        (quote_in + lp + protocol + creator).min(u64::MAX as u128) as u64
    }

    /// SIM `sell` (exact base in): net quote out. Revert iff below the tx's
    /// `min_quote_amount_out`. (Same as our own quote_sell.)
    pub fn sim_sell_quote_out(&self, base_amount_in: u64) -> u64 {
        self.quote_sell(base_amount_in)
    }

    /// SIM `buy_exact_quote_in` (exact quote in): base out. Revert iff below the
    /// tx's `min_base_amount_out`. (Same as our own quote_buy: strips the fee
    /// off the budget then runs the curve.)
    pub fn sim_buy_quote_in_base_out(&self, quote_in: u64) -> u64 {
        self.quote_buy(quote_in)
    }

    /// SIM `boost_buy_and_burn`: base burned for `quote_amount_in` (no user
    /// fee — the full quote lands in the pool). Revert iff below
    /// `min_base_amount_burned`.
    pub fn sim_boost_base_out(&self, quote_in: u64) -> u64 {
        let b = self.base_reserve as u128;
        let q = self.quote_reserve as u128;
        let qin = quote_in as u128;
        if qin == 0 {
            return 0;
        }
        (b * qin / (q + qin))
            .min(b.saturating_sub(1))
            .min(u64::MAX as u128) as u64
    }

    /// Apply an observed `buy_exact_quote_in` (exact-IN on the QUOTE side:
    /// the user spends `spendable_quote_in` total, fees included, and receives
    /// whatever base that buys). Pool: quote vault gains the pool-bound input
    /// plus the LP fee; base vault pays out the curve amount.
    pub fn after_observed_buy_quote_in(&self, spendable_quote_in: u64) -> PumpPool {
        let (base_out, input_amount, lp_fee) = self.buy_quote_in_parts(spendable_quote_in);
        if base_out == 0 {
            return *self;
        }
        let b = self.base_reserve as u128;
        let q = self.quote_reserve as u128;
        // base vault pays out base_out; quote vault gains the pool input + LP fee.
        PumpPool {
            base_reserve: (b - base_out as u128).min(u64::MAX as u128) as u64,
            quote_reserve: (q + input_amount as u128 + lp_fee as u128).min(u64::MAX as u128) as u64,
            ..*self
        }
    }

    /// Apply an observed `boost_buy_and_burn` (pump's buyback bot): exact
    /// `quote_amount_in` enters the quote vault from the boost vault and the
    /// bought base is BURNED out of the base vault. No user-side fees — the
    /// full quote lands in the pool and the curve output leaves it.
    pub fn after_observed_boost(&self, quote_amount_in: u64) -> PumpPool {
        let qin = quote_amount_in as u128;
        if qin == 0 {
            return *self;
        }
        let b = self.base_reserve as u128;
        let q = self.quote_reserve as u128;
        let out = (b * qin / (q + qin)).min(b.saturating_sub(1));
        PumpPool {
            base_reserve: (b - out) as u64,
            quote_reserve: (q + qin).min(u64::MAX as u128) as u64,
            ..*self
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
        let gross = q * bi / (b + bi); // floor
        let lp_fee = ceil_div(gross * self.lp_fee_bps as u128, BPS_DENOM as u128);
        // LP fee stays in the pool, so quote only drops by (gross - lp_fee).
        let new_base = (b + bi).min(u64::MAX as u128) as u64;
        let new_quote = q.saturating_sub(gross - lp_fee) as u64;
        PumpPool {
            base_reserve: new_base,
            quote_reserve: new_quote,
            ..*self
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
}


#[cfg(test)]
mod gus_event_tests {
    use super::*;
    // Real on-chain WSOL-GUS sell event (base=WSOL, quote=GUS), non-canonical:
    // lp=25, protocol=5, creator=0. base_in=21544, reserves as in the event.
    // gross=54858763, lpFee=137147, protocolFee=27430, userQuoteAmountOut=54694186.
    #[test]
    fn gus_sell_lamport_exact() {
        // flipped orientation already applied by the caller; here we build the
        // RAW pool (base=WSOL reserve, quote=GUS reserve) and sell base_in WSOL.
        let p = PumpPool {
            base_reserve: 56_240_500_100,
            quote_reserve: 143_208_572_392_022,
            total_fee_bps: 30,
            lp_fee_bps: 25,
            protocol_fee_bps: 5,
            creator_fee_bps: 0,
        };
        assert_eq!(p.quote_sell(21_544), 54_694_186);
    }
}
