//! Mapping between Metis DEX_PROGRAM_IDS and the .so binaries the user has
//! placed on disk. These .so files are loaded into LiteSVM at startup so the
//! simulator can execute real on-chain bytecode (not a mathematical model).
//!
//! Every entry is (on-chain program id, filename inside `simulation.so_dir`).
//! If a filename is empty or the file is missing, that program is skipped
//! and any tx touching it will bypass local simulation (logged as a miss).
//!
//! PMM DEXes (Tessera, SolFi, ZeroFi) rely on same-slot oracle freshness
//! that local simulation cannot provide. These are listed in
//! `PMM_PROGRAM_IDS` — routes touching them BYPASS simulation (when the
//! simulator is enabled) and go directly to Jito. Their .so files are
//! still loaded so mixed routes (AMM + PMM hops) can attempt simulation,
//! but a PMM-only route skips it.
//!
//! Nothing is blocked — every DEX registered here is eligible for arbitrage.

pub const PROGRAMS: &[(&str, &str)] = &[
    // --- Aggregator (top-level program the swap_instruction targets) ---
    // Metis is a Jupiter fork: its swap_instruction carries Jupiter v6's
    // program_id. Without this loaded as executable, LiteSVM rejects the tx
    // with "Program account JUP6Lkb... is not executable".
    ("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4", "Jupiter_Aggregator_v6.so"),

    // --- AMM / CLMM / orderbook DEXes ---
    ("cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG", "Meteora_DAMM_v2.so"),
    // Pump.fun AMM (PumpSwap) — the buy leg of the shred-arb route. Required
    // so LiteSVM can execute the Pump swap instruction and so the pool + vault
    // accounts stream into the AccountCache via this owner filter (no RPC).
    ("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA", "Pump_AMM.so"),
    ("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK", "Raydium_Concentrated_Liquidity.so"),
    ("MNFSTqtC93rEfYHB6hF82sKdZpUDFWkViLByLd1k1Ms", "Manifest.so"),
    ("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc", "Whirlpools_Program.so"),
    ("BSwp6bEBihVLdqJRKGgzjcGLHkcTuzmSo1TQkHepzH8p", "BonkSwap.so"),
    ("fUSioN9YKKSa3CUC2YUc4tPkHJ5Y6XW1yz8y6F7qWz9", "Fusion_AMM.so"),
    ("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo", "Meteora_DLMM_Program.so"),
    ("DEXYosS6oEGvk8uCDayvwEZz4qEyDJRf9nFgYCaqPMTm", "1Dex_Program.so"),
    ("MERLuDFBMmsHnsBPZw2sDQZHvXFMwp8EdjudcU2HKky", "Mercurial_Stable_Swap.so"),
    ("CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C", "Raydium_CPMM.so"),
    ("9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP", "Meteora_Pools_Program.so"),
    ("24Uqj9JCLxUeoC3hGfh5W3s9FM9uCHDS2SG3LYwBpyTi", "Invariant_Swap.so"),
    ("Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB", "PancakeSwap.so"),
    ("HyaB3W9q6XdA5xwpU4XnSZV94htfmbmqJXZcEbRaJutt", "Meteora_Vault_Program.so"),

    // --- Previously forbidden — now allowed at user's request ---
    ("ALPHAQmeA7bjrVuccPsYPiCvsi428SNwte66Srvs4pHA", "AlphaQ.so"),
    ("AQU1FRd7papthgdrwPTTq5JacJh8YtwEXaBfKU3bTz45", "Aquifer.so"),
    ("HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq", "Byreal_CLMM.so"),
    ("REALQqNEomY6cQGZJUGwywTBD2UmDT32rZcNnfxQ5N2", "REALQq.so"),

    // --- PMM DEXes — bypass simulation when enabled, otherwise go direct ---
    ("TessVdML9pBGgG9yGks7o4HewRaXVAMuoVj4x83GLQH", "Tessera_V.so"),
    ("SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe", "SolFi.so"),
    ("SV2EYYJyRz2YhfXwXnhNAevDEui5Q6yrfyo13WtupPF", "SolFi_V2.so"),
    ("ZERor4xhbUycZ6gb9ntrhqscUcZmAbQDjEAtCf4hbZY", "ZeroFi.so"),

    // --- PMM DEX — LiteSVM simulatable (no oracle staleness check) ---
    // GoonFi V2: uses sysvar_instructions whitelist (passes because we send
    // real Jupiter txs). Token vault accounts lazily RPC-fetched on first sim.
    ("goonuddtQRrWqqn5nFyczVKaie28f3kDkHWkHtURSLE", "goofni_v2.so"),
];

/// PMM (Proprietary Market Maker) program ids. Routes through these DEXes
/// BYPASS local LiteSVM simulation when the simulator is enabled, and are
/// sent directly to Jito where the block builder provides same-slot oracle
/// freshness.
pub const PMM_PROGRAM_IDS: &[&str] = &[
    "TessVdML9pBGgG9yGks7o4HewRaXVAMuoVj4x83GLQH",   // Tessera V
    "SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe",   // SolFi
    "SV2EYYJyRz2YhfXwXnhNAevDEui5Q6yrfyo13WtupPF",   // SolFi V2
    "ZERor4xhbUycZ6gb9ntrhqscUcZmAbQDjEAtCf4hbZY",   // ZeroFi
];

/// Program ids the bot refuses to route through. Currently empty — every
/// DEX in `PROGRAMS` is allowed.
pub const FORBIDDEN_DEX_PROGRAM_IDS: &[&str] = &[];

/// Jupiter/Metis DEX label substrings we ask the server to exclude up-front.
/// Currently empty — every DEX label is allowed.
pub const FORBIDDEN_DEX_LABELS: &[&str] = &[];

/// Program ids that the simulator should subscribe to on Yellowstone gRPC so
/// their pool accounts end up in the hot cache. This is the superset of
/// `PROGRAMS` as strings (unchanged if the user edits one side).
pub fn all_program_ids() -> Vec<String> {
    PROGRAMS.iter().map(|(id, _)| (*id).to_string()).collect()
}
