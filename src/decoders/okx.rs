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

/// OKX DEX Aggregation Router V2.
pub const PROGRAM: &str = "6m2CDdhRgxpH4WjvdzxAYbGxwdGUz5MziiL5jek2kBma";
