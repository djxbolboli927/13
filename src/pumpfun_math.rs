//! Pump.fun AMM (PumpSwap) pricing — constant-product `x*y=k`, Uniswap-v2 style.
//!
//! For a canonical pool `base_mint` is the token and `quote_mint` is WSOL, so
//! `price_in_sol = quote_reserve / base_reserve`. Reserves are NOT stored in the
//! pool account; they are the SPL-token `amount` of the two vaults
//! (`pool_base_token_account`, `pool_quote_token_account`).
//!
//! Fees (all levied on the QUOTE / SOL leg, current mainnet values):
//!   * LP fee        — 20 bps, stays inside the pool (grows the quote reserve)
//!   * protocol fee  —  5 bps, transferred out
//!   * coin-creator  —  5 bps, transferred out (canonical pools only)
//! On a `buy` the fees are added ON TOP of the pool-bound input; on a `sell`
//! they are subtracted FROM the gross output. All fee roundings are ceiling,
//! matching the on-chain program (pool-favorable). Integer math throughout.

/// LP fee that is retained inside the pool on every swap.
pub const LP_FEE_BPS: u64 = 20;
/// Protocol + coin-creator fees that leave the pool (canonical pool: 5 + 5).
pub const OUT_FEE_BPS: u64 = 10;
/// Total fee charged to the trader.
pub const TOTAL_FEE_BPS: u64 = LP_FEE_BPS + OUT_FEE_BPS;

const BPS_DENOM: u64 = 10_000;

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
    /// Total fee in bps (30 canonical, 25 without a coin creator).
    pub total_fee_bps: u64,
    /// LP portion of the fee that stays in the pool (20 bps).
    pub lp_fee_bps: u64,
}

impl PumpPool {
    pub fn new(base_reserve: u64, quote_reserve: u64) -> Self {
        Self {
            base_reserve,
            quote_reserve,
            total_fee_bps: TOTAL_FEE_BPS,
            lp_fee_bps: LP_FEE_BPS,
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
        let p = PumpPool::new(1_000_000_000, 200_000_000_000);
        let base_out = p.quote_buy(1_000_000_000);
        assert!(base_out > 0);
        let back = p.quote_sell(base_out);
        // round-trip on one pool must lose to the double fee
        assert!(back < 1_000_000_000);
    }

    #[test]
    fn observed_buy_raises_price() {
        let p = PumpPool::new(1_000_000_000, 200_000_000_000);
        let before = p.spot_price(6, 9);
        let after = p.after_observed_buy(10_000_000).spot_price(6, 9);
        assert!(after > before);
    }
}
