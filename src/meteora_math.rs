//! Meteora DAMM v2 (cp-amm) pricing — concentrated liquidity with a SINGLE
//! active liquidity `L` and no tick arrays. Within `[sqrt_min, sqrt_max]` the
//! curve is the standard Uniswap-v3 `sqrt_price`/`liquidity` invariant; a swap
//! moves `sqrt_price` but never changes `L`.
//!
//! Fixed-point: `sqrt_price` is Q64.64 (`√P · 2^64`). Price of token A in token
//! B is `(sqrt_price / 2^64)^2 · 10^(decA − decB)`.
//!
//! IMPORTANT — this is a hand port of the on-chain math (constant-product step
//! + rounding directions), which is the dominant slippage driver. The fee here
//! is modelled as a flat `fee_bps` applied on the input. DAMM v2's *dynamic*
//! (volatility) fee and base-fee schedulers are NOT yet modelled — set
//! `fee_bps` to the pool's effective fee and verify against a live quote before
//! trusting profit near the break-even line.

use crate::mathutil::{mul_div_ceil, mul_div_floor, Q64};

/// Lower bound of a valid sqrt-price (from the on-chain program constants).
pub const MIN_SQRT_PRICE: u128 = 4_295_048_016;
/// Upper bound of a valid sqrt-price.
pub const MAX_SQRT_PRICE: u128 = 79_226_673_521_066_979_257_578_248_091;

/// DAMM v2 fee denominator: fee numerators are in billionths (1e9 = 100%).
pub const FEE_DENOM: u128 = 1_000_000_000;

#[inline]
fn ceil_div(a: u128, b: u128) -> u128 {
    if a == 0 {
        0
    } else {
        1 + (a - 1) / b
    }
}

/// The pricing-relevant slice of a DAMM v2 pool account.
#[derive(Debug, Clone, Copy)]
pub struct MeteoraPool {
    pub sqrt_price: u128,
    pub liquidity: u128,
    pub sqrt_min_price: u128,
    pub sqrt_max_price: u128,
    /// Effective fee numerator (denominator `FEE_DENOM` = 1e9), read from the
    /// pool's `cliff_fee_numerator`. Dynamic (volatility) fee is negligible for
    /// the tiny trades on these low-liquidity pools, so the base fee suffices.
    pub fee_numerator: u64,
}

/// Result of a single-range exact-in swap.
#[derive(Debug, Clone, Copy)]
pub struct SwapOut {
    pub amount_out: u64,
    pub next_sqrt_price: u128,
}

impl MeteoraPool {
    /// Price of token A denominated in token B, adjusted for decimals.
    pub fn price_a_in_b(&self, dec_a: u32, dec_b: u32) -> f64 {
        let s = self.sqrt_price as f64 / Q64 as f64;
        let raw = s * s; // token B (raw) per token A (raw)
        raw * 10f64.powi(dec_a as i32 - dec_b as i32)
    }

    /// Price of the token (in SOL) given which side the token sits on.
    pub fn token_price_in_sol(&self, token_is_a: bool, dec_token: u32, dec_wsol: u32) -> f64 {
        if token_is_a {
            self.price_a_in_b(dec_token, dec_wsol)
        } else {
            let p = self.price_a_in_b(dec_wsol, dec_token);
            if p == 0.0 {
                0.0
            } else {
                1.0 / p
            }
        }
    }

    /// Exact-input swap within the active range.
    ///
    /// `a_to_b == true`  → token A in, price falls (guard against `sqrt_min`).
    /// `a_to_b == false` → token B in, price rises (guard against `sqrt_max`).
    ///
    /// Returns `None` if the trade would cross a range bound (the on-chain
    /// program reverts in that case) or if a fixed-point step overflows.
    pub fn swap_exact_in(&self, amount_in: u64, a_to_b: bool) -> Option<SwapOut> {
        if amount_in == 0 || self.liquidity == 0 {
            return None;
        }
        // Fee on input (pool-favorable ceiling).
        let fee = ceil_div(amount_in as u128 * self.fee_numerator as u128, FEE_DENOM);
        let net_in = (amount_in as u128).saturating_sub(fee);
        if net_in == 0 {
            return None;
        }

        let sqrt = self.sqrt_price;
        let l = self.liquidity;

        if a_to_b {
            // Δa in, price down: √P' = √P·L / (L + Δa·√P/2^64), round up.
            let term = mul_div_floor(net_in, sqrt, Q64)?;
            let denom = l.checked_add(term)?;
            let sqrt_next = mul_div_ceil(sqrt, l, denom)?;
            if sqrt_next < self.sqrt_min_price || sqrt_next >= sqrt {
                return None;
            }
            // out_b = L·(√P − √P') / 2^64, round down.
            let out = mul_div_floor(l, sqrt - sqrt_next, Q64)?;
            Some(SwapOut {
                amount_out: out.min(u64::MAX as u128) as u64,
                next_sqrt_price: sqrt_next,
            })
        } else {
            // Δb in, price up: √P' = √P + Δb·2^64 / L, round down.
            let bump = mul_div_floor(net_in, Q64, l)?;
            let sqrt_next = sqrt.checked_add(bump)?;
            if sqrt_next > self.sqrt_max_price || sqrt_next <= sqrt {
                return None;
            }
            // out_a = L·2^64·(√P'−√P) / (√P·√P'), staged to stay within u128.
            let diff = sqrt_next - sqrt;
            let inner = mul_div_floor(l, diff, sqrt)?;
            let out = mul_div_floor(inner, Q64, sqrt_next)?;
            Some(SwapOut {
                amount_out: out.min(u64::MAX as u128) as u64,
                next_sqrt_price: sqrt_next,
            })
        }
    }

    /// Current WSOL reserve held in the pool, used to bound trade size so we
    /// never try to move a low-liquidity pool by more than a small fraction.
    /// `token_is_a` says which side the token sits on (WSOL is the other side).
    pub fn wsol_reserve(&self, token_is_a: bool) -> u64 {
        let l = self.liquidity;
        let sqrt = self.sqrt_price;
        let out = if token_is_a {
            // WSOL is token B: reserve_B = L·(√P − √P_min) / 2^64.
            mul_div_floor(l, sqrt.saturating_sub(self.sqrt_min_price), Q64)
        } else {
            // WSOL is token A: reserve_A = L·2^64·(√P_max − √P) / (√P·√P_max).
            let diff = self.sqrt_max_price.saturating_sub(sqrt);
            match mul_div_floor(l, diff, sqrt) {
                Some(inner) => mul_div_floor(inner, Q64, self.sqrt_max_price),
                None => None,
            }
        };
        out.unwrap_or(0).min(u64::MAX as u128) as u64
    }

    /// Convenience: buy the token with WSOL. Returns token amount out.
    /// `token_is_a` maps the WSOL-in leg to the correct swap direction.
    pub fn buy_token_with_wsol(&self, wsol_in: u64, token_is_a: bool) -> Option<u64> {
        // WSOL in → token out. If token is A then WSOL is B, so B→A (a_to_b=false).
        let a_to_b = !token_is_a;
        self.swap_exact_in(wsol_in, a_to_b).map(|s| s.amount_out)
    }

    /// Convenience: sell the token for WSOL. Returns lamports out.
    pub fn sell_token_for_wsol(&self, token_in: u64, token_is_a: bool) -> Option<u64> {
        // token in → WSOL out. If token is A then A→B (a_to_b=true).
        let a_to_b = token_is_a;
        self.swap_exact_in(token_in, a_to_b).map(|s| s.amount_out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool() -> MeteoraPool {
        MeteoraPool {
            sqrt_price: 2u128 << 64, // √P = 2 → price(A in B) = 4
            liquidity: 1u128 << 80,
            sqrt_min_price: MIN_SQRT_PRICE,
            sqrt_max_price: MAX_SQRT_PRICE,
            fee_numerator: 2_500_000, // 0.25%
        }
    }

    #[test]
    fn price_matches_sqrt() {
        let p = pool();
        // decimals equal → price = 4.0
        assert!((p.price_a_in_b(0, 0) - 4.0).abs() < 1e-9);
    }

    #[test]
    fn swap_moves_price_correct_direction() {
        let p = pool();
        let up = p.swap_exact_in(1_000_000, false).unwrap();
        assert!(up.next_sqrt_price > p.sqrt_price);
        let down = p.swap_exact_in(1_000_000, true).unwrap();
        assert!(down.next_sqrt_price < p.sqrt_price);
        assert!(up.amount_out > 0 && down.amount_out > 0);
    }
}
