//! OKX DEX Aggregation Router V2 — `6m2CDdhRgxpH4WjvdzxAYbGxwdGUz5MziiL5jek2kBma`.
//!
//! Anchor-style router; CPIs into the underlying DEX (incl. Pump.fun AMM /
//! Meteora DAMM v2), so a watched pool touched via OKX appears in the flattened
//! account keys and is enqueued as `Unreadable`. Discriminators verified from
//! the 0xjeffro/tx-parser constants (corroborated by okxlabs GitHub):
//!   swap                    [65,75,63,76,235,91,91,136]
//!   commissionSplProxySwap  [96,67,12,151,129,164,18,71]
//!   commissionSolSwap2      [113,132,31,74,99,169,57,146]
//!
//! Note: OKX also ships a separate V1 router repo; if unmatched OKX-shaped txns
//! appear, capture and add that program id here.
//!
//! ─── Observed on-chain (real tx, 2026): `swap_tob` ─────────────────────────
//! Unlike Jupiter, OKX puts the scalar amounts BEFORE the variable-length routes
//! vec, so amount_in / expect_amount_out / slippage sit at FIXED offsets and are
//! readable from the shred without parsing the vec:
//!   SwapArgs { order_id u64@8, amount_in u64@16, expect_amount_out u64@24,
//!              slippage u16@32, routes: Vec<Route>@34, … }
//!   trailing: commission_info u32, platform_fee_rate u16, trim_rate u8.
//!   Route { dex: Dex(enum), weight: u16, index: u8 }  — weight is a per-leg
//!   split in basis points (e.g. 10000 = 100%).
//! Real example: amount_in=5390846552666, expect_amount_out=4270594256,
//!   slippage=1140, routes=[PumpfunammSell2 w10000, PumpfunammBuy w9000,
//!   MeteoraDAMMV2Swap2 w1000]. The Dex enum indices (for direction) are being
//!   pinned from the current IDL before byte-parsing the routes vec.

/// OKX DEX Aggregation Router V2.
pub const PROGRAM: &str = "6m2CDdhRgxpH4WjvdzxAYbGxwdGUz5MziiL5jek2kBma";
