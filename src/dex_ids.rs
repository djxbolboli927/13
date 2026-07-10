//! Program ids and shared constants for the ShredStream arbitrage strategy.
//!
//! The strategy trades between exactly two venues: Pump.fun AMM (constant
//! product) and Meteora DAMM v2 (concentrated liquidity). A big swap detected
//! on Pump.fun via ShredStream opens a transient price gap versus the (slow,
//! low-liquidity) Meteora pool for the same token; we buy the cheaper side and
//! sell the dearer one.

use solana_sdk::pubkey::Pubkey;

/// Pump.fun AMM ("PumpSwap") program.
pub const PUMPFUN_AMM_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";

/// Meteora DAMM v2 (cp-amm) program.
pub const METEORA_DAMM_V2_PROGRAM: &str = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG";

/// Wrapped SOL mint — the quote asset on every pair we trade.
pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Metis (`dexes=`) route-filter labels used to FORCE each leg onto a specific
/// venue. Case- and spacing-sensitive; spaces become `+` in the query string.
pub const METIS_LABEL_PUMPFUN: &str = "Pump.fun Amm";
pub const METIS_LABEL_METEORA: &str = "Meteora DAMM v2";

/// Which venue a pool belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DexKind {
    PumpFunAmm,
    MeteoraDammV2,
}

impl DexKind {
    pub fn from_owner(owner: &Pubkey) -> Option<Self> {
        let s = owner.to_string();
        if s == PUMPFUN_AMM_PROGRAM {
            Some(DexKind::PumpFunAmm)
        } else if s == METEORA_DAMM_V2_PROGRAM {
            Some(DexKind::MeteoraDammV2)
        } else {
            None
        }
    }

    /// The Metis route-filter label used to force a leg onto this venue.
    pub fn metis_label(&self) -> &'static str {
        match self {
            DexKind::PumpFunAmm => METIS_LABEL_PUMPFUN,
            DexKind::MeteoraDammV2 => METIS_LABEL_METEORA,
        }
    }
}

pub fn pumpfun_program() -> Pubkey {
    Pubkey::from_str_const(PUMPFUN_AMM_PROGRAM)
}

pub fn meteora_program() -> Pubkey {
    Pubkey::from_str_const(METEORA_DAMM_V2_PROGRAM)
}

pub fn wsol_mint() -> Pubkey {
    Pubkey::from_str_const(WSOL_MINT)
}
