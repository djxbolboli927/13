//! On-chain program registry & instruction catalogue for the shred decoder.
//!
//! ─── Why this module exists ────────────────────────────────────────────────
//! The bot must NEVER silently jump over a transaction that touches a watched
//! pool. Before this module, `scan_tx` bailed out the moment the Pump.fun AMM
//! program was not a *static* account key — which dropped EVERY aggregator /
//! router transaction (Jupiter, OKX, DFlow, Axiom, …), because those programs
//! CPI into the Pump/Meteora pool and load the AMM program from an Address
//! Lookup Table, not as a static key. Those dropped txs left holes in the
//! ordered per-pool sequence, so the confirmed frontier could never advance and
//! the bot "skipped from tx 500 to tx 800".
//!
//! ─── The hard limit (proven by research) ───────────────────────────────────
//! From RAW shred bytes you CANNOT recover the exact executed amounts of an
//! aggregator swap: those live in transaction *meta* (inner instructions /
//! `SwapEvent`), which does not exist yet at shred time. What you CAN always
//! recover is the POOL account — Solana flattens every account referenced by any
//! instruction OR CPI into the transaction's account-key list. So the robust,
//! router-agnostic rule is:
//!
//!   * native Pump/Meteora swap we can decode  → `Readable` (amounts known)
//!   * any other tx that references a watched pool (router/bot CPI)
//!                                             → `Unreadable` (wait for its
//!                                                account-update to learn the
//!                                                new reserves — never skip it)
//!
//! ─── Layout ────────────────────────────────────────────────────────────────
//! One submodule per program. Each documents that program's instruction set
//! (discriminators + argument/account layout) at the TOP, and exposes the
//! constants the scanner needs BELOW. Add a new file per program as new routers
//! appear (Titan/Axiom/BullX program ids are not public yet — capture live and
//! append here).

pub mod dflow;
pub mod jupiter;
pub mod meteora_damm_v2;
pub mod okx;
pub mod pump_amm;
pub mod route_decode;

use solana_sdk::pubkey::Pubkey;

/// Base58 program ids of KNOWN routers/aggregators that CPI into Pump.fun AMM or
/// Meteora DAMM v2. When any of these appears as a static account key, the shred
/// scanner resolves the full (ALT-expanded) account list and checks it for a
/// watched pool — so router txs are no longer dropped before their pool can be
/// matched. Only publicly-verified program ids belong here; unverified ones
/// (Axiom/BullX/Titan/Trojan) are intentionally omitted rather than guessed.
pub const KNOWN_ROUTER_IDS: &[(&str, &str)] = &[
    ("Jupiter_v6", jupiter::PROGRAM_V6),
    ("Jupiter_v4", jupiter::PROGRAM_V4),
    ("OKX_DEX_Router_v2", okx::PROGRAM),
    ("DFlow_Aggregator_v4", dflow::PROGRAM),
];

/// The router program ids as `Pubkey`s, for building the scanner's fast-lookup
/// set once at startup.
pub fn known_router_pubkeys() -> Vec<Pubkey> {
    KNOWN_ROUTER_IDS
        .iter()
        .map(|(_, id)| Pubkey::from_str_const(id))
        .collect()
}
