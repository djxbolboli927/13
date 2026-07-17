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

/// Meteora Dynamic AMM ("Meteora pool" / classic Dynamic AMM) program. Constant
/// product, reserves held in lending vaults.
pub const METEORA_DYNAMIC_AMM_PROGRAM: &str = "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB";

/// Raydium Liquidity Pool V4 (classic AMM). Constant product.
pub const RAYDIUM_V4_PROGRAM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";

/// Raydium CPMM (constant-product market maker).
pub const RAYDIUM_CPMM_PROGRAM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";

/// Wrapped SOL mint — the quote asset on every pair we trade.
pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Metis (`dexes=`) route-filter labels used to FORCE each leg onto a specific
/// venue. Case- and spacing-sensitive; spaces become `+` in the query string.
pub const METIS_LABEL_PUMPFUN: &str = "Pump.fun Amm";
pub const METIS_LABEL_METEORA: &str = "Meteora DAMM v2";
pub const METIS_LABEL_METEORA_DYN: &str = "Meteora";
pub const METIS_LABEL_RAYDIUM_V4: &str = "Raydium";
pub const METIS_LABEL_RAYDIUM_CPMM: &str = "Raydium CP";

/// Which venue a pool belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DexKind {
    PumpFunAmm,
    MeteoraDammV2,
    MeteoraDynamicAmm,
    RaydiumV4,
    RaydiumCpmm,
}

impl DexKind {
    pub fn from_owner(owner: &Pubkey) -> Option<Self> {
        let s = owner.to_string();
        if s == PUMPFUN_AMM_PROGRAM {
            Some(DexKind::PumpFunAmm)
        } else if s == METEORA_DAMM_V2_PROGRAM {
            Some(DexKind::MeteoraDammV2)
        } else if s == METEORA_DYNAMIC_AMM_PROGRAM {
            Some(DexKind::MeteoraDynamicAmm)
        } else if s == RAYDIUM_V4_PROGRAM {
            Some(DexKind::RaydiumV4)
        } else if s == RAYDIUM_CPMM_PROGRAM {
            Some(DexKind::RaydiumCpmm)
        } else {
            None
        }
    }

    /// The Metis route-filter label used to force a leg onto this venue.
    pub fn metis_label(&self) -> &'static str {
        match self {
            DexKind::PumpFunAmm => METIS_LABEL_PUMPFUN,
            DexKind::MeteoraDammV2 => METIS_LABEL_METEORA,
            DexKind::MeteoraDynamicAmm => METIS_LABEL_METEORA_DYN,
            DexKind::RaydiumV4 => METIS_LABEL_RAYDIUM_V4,
            DexKind::RaydiumCpmm => METIS_LABEL_RAYDIUM_CPMM,
        }
    }

    /// The on-chain program id (owner) for this venue.
    pub fn program_str(&self) -> &'static str {
        match self {
            DexKind::PumpFunAmm => PUMPFUN_AMM_PROGRAM,
            DexKind::MeteoraDammV2 => METEORA_DAMM_V2_PROGRAM,
            DexKind::MeteoraDynamicAmm => METEORA_DYNAMIC_AMM_PROGRAM,
            DexKind::RaydiumV4 => RAYDIUM_V4_PROGRAM,
            DexKind::RaydiumCpmm => RAYDIUM_CPMM_PROGRAM,
        }
    }

    /// Short label for logs.
    pub fn short(&self) -> &'static str {
        match self {
            DexKind::PumpFunAmm => "PumpFun",
            DexKind::MeteoraDammV2 => "MeteoraDAMMv2",
            DexKind::MeteoraDynamicAmm => "MeteoraDyn",
            DexKind::RaydiumV4 => "RaydiumV4",
            DexKind::RaydiumCpmm => "RaydiumCPMM",
        }
    }

    /// True for the concentrated-liquidity venue (Meteora DAMM v2). All other
    /// non-Pump venues we support are plain constant-product AMMs.
    pub fn is_concentrated(&self) -> bool {
        matches!(self, DexKind::MeteoraDammV2)
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
