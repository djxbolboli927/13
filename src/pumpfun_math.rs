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
) -> (u64, u64) {
    let highest = (
        FEE_TIERS[FEE_TIERS.len() - 1].1,
        FEE_TIERS[FEE_TIERS.len() - 1].2,
    );
    if base_reserve == 0 || supply_base_units == 0 {
        return highest; // unknown market cap → highest fee
    }
    let mcap_lamports =
        (quote_reserve as u128).saturating_mul(supply_base_units) / base_reserve as u128;
    let mcap_sol = (mcap_lamports / LAMPORTS_PER_SOL).min(u64::MAX as u128) as u64;
    for &(thresh, total, lp) in FEE_TIERS {
        if mcap_sol >= thresh {
            return (total, lp);
        }
    }
    (125, 2)
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
    /// Total fee in bps — dynamic, selected from the market-cap tier schedule.
    pub total_fee_bps: u64,
    /// LP portion of the fee that stays in the pool.
    pub lp_fee_bps: u64,
}

impl PumpPool {
    /// `supply_base_units` is the base token's real mint supply (0 = unknown →
    /// highest fee tier, fail-closed).
    pub fn new(base_reserve: u64, quote_reserve: u64, supply_base_units: u128) -> Self {
        let (total_fee_bps, lp_fee_bps) =
            fee_for_reserves(base_reserve, quote_reserve, supply_base_units);
        Self {
            base_reserve,
            quote_reserve,
            total_fee_bps,
            lp_fee_bps,
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
        // Strip the fee that is added on top of the pool-bound input.
        let pool_quote_in = (quote_in_budget as u128) * BPS_DENOM as u128
            / (BPS_DENOM + self.total_fee_bps) as u128;
        if pool_quote_in == 0 {
            return 0;
        }
        // base_out = floor(B * qin / (Q + qin))
        let b = self.base_reserve as u128;
        let q = self.quote_reserve as u128;
        let out = b * pool_quote_in / (q + pool_quote_in);
        out.min(u64::MAX as u128) as u64
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
        let fee = ceil_div(gross * self.total_fee_bps as u128, BPS_DENOM as u128);
        gross.saturating_sub(fee).min(u64::MAX as u128) as u64
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
        let p = PumpPool::new(1_000_000_000, 200_000_000_000, 1_000_000_000_000_000);
        let base_out = p.quote_buy(1_000_000_000);
        assert!(base_out > 0);
        let back = p.quote_sell(base_out);
        // round-trip on one pool must lose to the double fee
        assert!(back < 1_000_000_000);
    }

    #[test]
    fn observed_buy_raises_price() {
        let p = PumpPool::new(1_000_000_000, 200_000_000_000, 1_000_000_000_000_000);
        let before = p.spot_price(6, 9);
        let after = p.after_observed_buy(10_000_000).spot_price(6, 9);
        assert!(after > before);
    }
}
