//! Jupiter Aggregator — the highest-volume router into Pump.fun AMM / Meteora.
//!
//! Verified against the official Anchor IDL (jup-ag/jupiter-amm-implementation,
//! idls/jupiter_aggregator.json). Discriminator = `sha256("global:<name>")[..8]`.
//!
//! ─── Program ids ───────────────────────────────────────────────────────────
//!   v6 (current): JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4
//!   v4 (legacy):  JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB
//!
//! ─── Top-level routing instructions (v6) — discriminators ──────────────────
//!   route                                   [229,23,203,151,122,227,173,42]
//!   route_with_token_ledger                 [150,86,71,116,167,93,14,104]
//!   shared_accounts_route                   [193,32,155,51,65,214,156,129]
//!   shared_accounts_route_with_token_ledger [230,121,143,80,119,159,106,170]
//!   exact_out_route                         [208,51,239,151,123,43,237,92]
//!   shared_accounts_exact_out_route         [176,209,105,168,154,125,69,62]
//!
//! Args (borsh): route { route_plan: Vec<RoutePlanStep>, in_amount u64,
//! quoted_out_amount u64, slippage_bps u16, platform_fee_bps u8 }. The
//! shared_accounts_* variants prefix a `id: u8`. RoutePlanStep = { swap: Swap
//! (1-byte enum tag + optional payload), percent u8, input_index u8,
//! output_index u8 }.
//!
//! ─── Why we DON'T decode amounts here ──────────────────────────────────────
//! The top-level data holds only the user's overall in_amount / quoted_out
//! (a quote, not the fill) and the route plan. The real per-hop pool + executed
//! amounts live in the inner-instruction `SwapEvent`
//!   SwapEvent { amm, input_mint, input_amount, output_mint, output_amount }
//!   event disc [64,198,205,232,38,8,113,226]
//! which is transaction *meta* and does NOT exist at shred time. So a Jupiter tx
//! on a watched pool is treated as `Unreadable`: we recognise it (via the pool in
//! the flattened account keys) and wait for the pool's account-update.
//!
//! For reference, the route_plan `Swap` enum variants for our venues (stable):
//!   PumpdotfunAmmBuy = 72, PumpdotfunAmmSell = 73, MeteoraDammV2 = 77.

/// Jupiter v6 aggregator program.
pub const PROGRAM_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
/// Jupiter v4 aggregator program (legacy; still seen occasionally).
pub const PROGRAM_V4: &str = "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB";

/// route_plan `Swap` enum tags for our two venues (for future route-plan walks).
pub const SWAP_PUMP_AMM_BUY: u8 = 72;
pub const SWAP_PUMP_AMM_SELL: u8 = 73;
pub const SWAP_METEORA_DAMM_V2: u8 = 77;
