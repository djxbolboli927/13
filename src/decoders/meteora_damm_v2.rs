//! Meteora DAMM v2 / cp-amm — program `cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG`.
//!
//! Anchor program; discriminator = `sha256("global:<name>")[..8]`. Verified
//! against the official IDL (github.com/MeteoraAg/damm-v2-sdk, src/idl/cp_amm.json)
//! and program source (github.com/MeteoraAg/damm-v2).
//!
//! Fixed pool authority (account 0 on most ixs): `HLnpSz9h2S4hiLQ43rnSD9XkcUThA7B8hQMKmDaiTLcC`.
//!
//! ─── swap  disc [248,198,158,145,225,117,135,200] ──────────────────────────
//!   accounts: 0 pool_authority(fixed), 1 pool(w), 2 input_token_account,
//!             3 output_token_account, 4 token_a_vault, 5 token_b_vault,
//!             6 token_a_mint, 7 token_b_mint, 8 payer(signer), …
//!   args (SwapParameters): amount_in u64@8, minimum_amount_out u64@16.
//!
//! ─── swap2 disc [65,75,63,76,235,91,91,136] (current) ──────────────────────
//!   accounts: identical order; pool is still account index 1.
//!   args (SwapParameters2): amount_0 u64@8, amount_1 u64@16, swap_mode u8@24.
//!     swap_mode 0 = ExactIn      → amount_0 = exact in,  amount_1 = min out
//!     swap_mode 1 = PartialFill  → amount_0 = in,        amount_1 = min out
//!     swap_mode 2 = ExactOut     → amount_0 = exact OUT, amount_1 = max IN (curve reversed)
//!
//! ─── Swap math note (important) ────────────────────────────────────────────
//! DAMM v2 is NOT raw reserve `x*y=k`. The pool stores `sqrt_price` (Q64.64) and
//! `liquidity`; amount_out is computed concentrated-liquidity style. Fee
//! numerator is per-`FEE_DENOMINATOR = 1_000_000_000` (NOT bps). For our purposes
//! the pool's own account-update carries the resulting reserves, so we do not
//! re-derive the DAMM v2 curve from shreds — we only need to recognise the swap
//! and its pool.
//!
//! ─── Liquidity ixs (rug/holdings signals) ──────────────────────────────────
//!   add_liquidity [181,157,89,67,143,182,52,72]  remove_liquidity [80,85,209,72,24,206,177,108]
//!   remove_all_liquidity [10,51,61,35,112,105,24,85]

/// `swap` (SwapParameters: amount_in@8, minimum_amount_out@16).
pub const DISC_SWAP: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];
/// `swap2` (SwapParameters2: amount_0@8, amount_1@16, swap_mode u8@24).
pub const DISC_SWAP2: [u8; 8] = [65, 75, 63, 76, 235, 91, 91, 136];
