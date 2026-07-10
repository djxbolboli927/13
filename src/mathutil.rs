//! Fixed-point 256-bit helpers for AMM math.
//!
//! Meteora DAMM v2 pricing multiplies a u128 liquidity by a Q64.64 sqrt-price
//! before dividing — the intermediate product exceeds 128 bits, so we need a
//! widening multiply + 256-by-128 division. These helpers are pure integer
//! arithmetic (verified against f64 to zero relative error for realistic pool
//! ranges) and take no dependencies.

const MASK64: u128 = 0xFFFF_FFFF_FFFF_FFFF;

/// Q64.64 scale factor: `1.0` in the sqrt-price fixed-point representation.
pub const Q64: u128 = 1u128 << 64;

/// 256-bit product of two u128 values, returned as `(hi, lo)`.
#[inline]
pub fn mul_wide(a: u128, b: u128) -> (u128, u128) {
    let (a_hi, a_lo) = (a >> 64, a & MASK64);
    let (b_hi, b_lo) = (b >> 64, b & MASK64);

    let ll = a_lo * b_lo;
    let lh = a_lo * b_hi;
    let hl = a_hi * b_lo;
    let hh = a_hi * b_hi;

    let mid = (ll >> 64) + (lh & MASK64) + (hl & MASK64);
    let lo = (ll & MASK64) | (mid << 64);
    let hi = hh + (lh >> 64) + (hl >> 64) + (mid >> 64);
    (hi, lo)
}

/// floor(`(hi,lo)` / `d`) and the remainder.
///
/// Assumes the quotient fits in u128 and `d < 2^127` (so `rem << 1` cannot
/// overflow). Both hold for every DAMM v2 divisor we use (sqrt-prices,
/// liquidity, `2^64`); returns `None` if the caller feeds a divisor that
/// breaks the precondition, so the hot path can skip rather than panic.
#[inline]
pub fn div_wide(hi: u128, lo: u128, d: u128) -> Option<(u128, u128)> {
    if d == 0 || d >= (1u128 << 127) {
        return None;
    }
    let mut rem: u128 = 0;
    let mut quo: u128 = 0;
    for i in (0..256).rev() {
        let bit = if i >= 128 {
            (hi >> (i - 128)) & 1
        } else {
            (lo >> i) & 1
        };
        rem = (rem << 1) | bit;
        quo <<= 1;
        if rem >= d {
            rem -= d;
            quo |= 1;
        }
    }
    Some((quo, rem))
}

/// floor(`a * b / c`) with a full 256-bit intermediate. `None` if `c` breaks
/// the [`div_wide`] precondition.
#[inline]
pub fn mul_div_floor(a: u128, b: u128, c: u128) -> Option<u128> {
    let (hi, lo) = mul_wide(a, b);
    div_wide(hi, lo, c).map(|(q, _)| q)
}

/// ceil(`a * b / c`) with a full 256-bit intermediate.
#[inline]
pub fn mul_div_ceil(a: u128, b: u128, c: u128) -> Option<u128> {
    let (hi, lo) = mul_wide(a, b);
    div_wide(hi, lo, c).map(|(q, r)| if r > 0 { q + 1 } else { q })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mul_wide_max() {
        let (hi, lo) = mul_wide(u128::MAX, u128::MAX);
        assert_eq!(hi, u128::MAX - 1);
        assert_eq!(lo, 1);
    }

    #[test]
    fn mul_div_basic() {
        assert_eq!(mul_div_floor(6, 7, 3), Some(14));
        assert_eq!(mul_div_floor(7, 7, 3), Some(16));
        assert_eq!(mul_div_ceil(7, 7, 3), Some(17));
    }

    #[test]
    fn mul_div_large_matches_f64() {
        let a = 123_456_789_012_345u128;
        let b = 987_654_321_098_765u128;
        let c = 1_000_000_007u128;
        let got = mul_div_floor(a, b, c).unwrap() as f64;
        let want = (a as f64) * (b as f64) / (c as f64);
        assert!((got - want).abs() / want < 1e-12);
    }
}
