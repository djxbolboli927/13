//! Pump.fun AMM ("PumpSwap") — program `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA`.
//!
//! Anchor program: instruction data = `[8-byte discriminator][borsh args]`, so
//! the first argument starts at byte offset 8; all ints are little-endian.
//! Discriminator = `sha256("global:<snake_case_name>")[..8]`. Verified against
//! the official IDL (github.com/pump-fun/pump-public-docs, idl/pump_amm.json).
//!
//! ─── Trade instructions the bot must never skip ────────────────────────────
//!
//! buy   disc [102,6,61,18,1,218,235,234]
//!   args: base_amount_out u64@8, max_quote_amount_in u64@16, track_volume(OptionBool)@24
//!   accounts: 0 pool, 1 user(signer), 2 global_config, 3 base_mint, 4 quote_mint,
//!             5 user_base_ata, 6 user_quote_ata, 7 pool_base_vault, 8 pool_quote_vault,
//!             9 protocol_fee_recipient, 10 protocol_fee_recipient_ata, …
//!   → first field is BASE, second is the QUOTE bound.
//!
//! sell  disc [51,230,133,164,1,127,131,173]
//!   args: base_amount_in u64@8, min_quote_amount_out u64@16   (NO trailing byte)
//!   accounts: same 0..8 as buy; sell has no volume-accumulator tail.
//!
//! buy_exact_quote_in  disc [198,46,21,82,180,217,232,112]
//!   args: spendable_quote_in u64@8 (QUOTE), min_base_amount_out u64@16 (BASE),
//!         track_volume(OptionBool)@24     → order is (quote, base).
//!
//! boost_buy_and_burn  disc [105,68,6,175,0,7,35,162]
//!   args: quote_amount_in u64@8 (QUOTE), min_base_amount_burned u64@16 (BASE).
//!   accounts: 0 pool, 1 authority(signer), 2 global_config, 3 base_mint,
//!             4 quote_mint, 5 pool_base_vault, 6 pool_quote_vault, …
//!
//! withdraw (remove liquidity, the direct rug signal) disc [183,18,70,156,148,109,161,34]
//!
//! The u64 slippage bound is ALWAYS the field at data offset 16 (max_quote_in on
//! buy, min_quote_out on sell, min_base_out on buy_exact_quote_in / boost).
//!
//! ─── Other instructions (recognised so a pool-touching tx is never mistaken
//!     for "not Pump"; amounts decoded only for the trade ixs above) ──────────
//!   create_pool [233,146,209,142,207,104,64,188]  deposit [242,35,198,137,82,225,242,182]
//! Full admin set is in the IDL; add here as needed.

/// `buy`: exact base OUT of the base vault, quote in + fees.
pub const DISC_BUY: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
/// `sell`: exact base INTO the base vault, quote out − fees.
pub const DISC_SELL: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];
/// `buy_exact_quote_in`: exact QUOTE in (fees included); base out.
pub const DISC_BUY_EXACT_QUOTE_IN: [u8; 8] = [198, 46, 21, 82, 180, 217, 232, 112];
/// `boost_buy_and_burn`: exact QUOTE into the pool; bought base burned out.
pub const DISC_BOOST_BUY_AND_BURN: [u8; 8] = [105, 68, 6, 175, 0, 7, 35, 162];
/// `withdraw` (remove liquidity) — rug signal.
pub const DISC_WITHDRAW: [u8; 8] = [183, 18, 70, 156, 148, 109, 161, 34];
