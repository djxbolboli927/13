//! Local LiteSVM simulation gate — ZERO RPC on the hot path.
//!
//! Uses the vendored LiteSVM source (vendor/litesvm, GitHub master) rather
//! than a crates.io release.  Key capabilities used from LiteSVM 0.11:
//!
//! • `with_mainnet_features()` — activates every Solana feature gate live on
//!   mainnet-beta.  PMM DEXes (Tessera, SolFi, ZeroFi) rely on post-2.0
//!   features; without this they silently mis-execute or revert unexpectedly.
//!
//! • `warp_to_slot(slot)` — atomically advances Clock.slot, Clock.epoch,
//!   SlotHashes, and EpochSchedule to the live Yellowstone slot.  PMM DEXes
//!   check Clock.slot for price-staleness; the old manual sysvar approach
//!   could leave SlotHashes stale, causing oracle checks to mis-fire.
//!
//! • `with_default_programs()` — loads the full SPL + built-in program set
//!   (replaces the removed `with_spl_programs()` from earlier versions).
//!
//! • CPI bug-fixes — 0.9+ resolved return-data propagation across CPI hops,
//!   eliminating false-negative reverts on multi-hop routes.
//!
//! ## Type-conversion boundary
//!
//! Our bot is built on `solana-sdk 2.2` (monolithic SDK, uses
//! `solana-transaction 2.x` internally).  LiteSVM 0.11 uses the newer
//! granular crates (`solana-transaction 3.x`, `solana-address 2.x`).
//! Both share the same on-wire binary format (Solana maintains wire-format
//! compatibility across major releases), so the `to_litesvm_tx` adapter
//! does a cheap bincode round-trip at the simulate() call site.
//! Account state (solana-account 3.2.0) is shared by both and needs no
//! conversion.

use anyhow::{anyhow, Context, Result};
use litesvm::LiteSVM;
use solana_account::ReadableAccount;
use solana_address::Address as LsAddr;
use solana_sdk::{
    address_lookup_table::AddressLookupTableAccount,
    message::VersionedMessage,
    pubkey::Pubkey,
    transaction::VersionedTransaction,
};
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

use crate::account_cache::AccountCache;
use crate::metrics::Metrics;

pub struct SimOutcome {
    pub compute_units: u64,
    pub wsol_after: u64,
}

pub struct Simulator {
    svm: Mutex<LiteSVM>,
    wsol_ata: Pubkey,
    fail_closed: bool,
    /// Live mainnet slot from the Yellowstone gRPC stream (zero RPC).
    current_slot: Arc<AtomicU64>,
}

// ── Type-conversion helpers at the solana-sdk 2.x / LiteSVM 3.x boundary ───

/// Convert a solana-sdk 2.x `Pubkey` to the `solana-address 2.x` `Address`
/// type that LiteSVM 0.11 uses for all account-lookup APIs.
/// Both types are `[u8; 32]` wrappers; the conversion is a byte-level copy.
#[inline]
fn pk_to_addr(pk: Pubkey) -> LsAddr {
    LsAddr::from(pk.to_bytes())
}

/// Translate a `solana-sdk 2.x` `VersionedTransaction` to the
/// `solana-transaction 3.x` type expected by `LiteSVM::simulate_transaction`.
///
/// Wire format is identical across Solana major releases, so a bincode
/// round-trip is a safe zero-semantic-change conversion.  Cost: one heap
/// allocation (~few hundred bytes) per simulation — negligible vs. the sim.
fn to_litesvm_tx(
    tx: &VersionedTransaction,
) -> Result<solana_transaction::versioned::VersionedTransaction> {
    let bytes = bincode::serialize(tx).context("serialize tx for litesvm boundary")?;
    bincode::deserialize(&bytes).context("deserialize tx for litesvm boundary")
}

// ────────────────────────────────────────────────────────────────────────────

impl Simulator {
    pub fn new(
        so_dir: &str,
        wsol_ata: Pubkey,
        fail_closed: bool,
        current_slot: Arc<AtomicU64>,
    ) -> Result<Self> {
        // Build the SVM with the full mainnet feature set.
        //
        // with_mainnet_features(): activates every Solana feature gate live on
        //   mainnet-beta.  PMM DEXes rely on post-2.0 features; without this
        //   they silently mis-execute.
        //
        // with_default_programs(): loads SPL Token, SPL Token-2022, ATA,
        //   System, Compute Budget, and other built-ins.
        //
        // with_sigverify(false): skip ed25519 sig checks — the bot already
        //   signs correctly; skipping saves ~0.5 ms per sim on the hot path.
        //
        // with_blockhash_check(false): we use a cached recent blockhash;
        //   skip the SVM's internal staleness check.
        let mut svm = LiteSVM::new()
            .with_sysvars()
            .with_sigverify(false)
            .with_blockhash_check(false)
            .with_default_programs()
            .with_mainnet_features();

        // warp_to_slot atomically advances Clock.slot, Clock.epoch,
        // SlotHashes, and EpochSchedule — everything PMM oracle staleness
        // checks read.
        let initial_slot = current_slot.load(Ordering::Relaxed);
        svm.warp_to_slot(initial_slot);
        debug!(initial_slot, "sim slot initialised via warp_to_slot");

        // Load DEX program bytecode (.so files) from disk.
        let mut loaded = 0usize;
        let mut missing = 0usize;
        for (pid_str, fname) in crate::program_registry::PROGRAMS {
            if fname.is_empty() {
                continue;
            }
            let path = format!("{}/{}", so_dir.trim_end_matches('/'), fname);
            if !Path::new(&path).exists() {
                warn!(path, "program .so not found, skipping");
                missing += 1;
                continue;
            }
            let pid = Pubkey::try_from(*pid_str)
                .map_err(|e| anyhow!("bad program id {pid_str}: {e:?}"))?;
            match svm.add_program_from_file(pk_to_addr(pid), Path::new(&path)) {
                Ok(()) => {
                    debug!(program = %pid, path, "program loaded");
                    loaded += 1;
                }
                Err(e) => {
                    warn!(program = %pid, path, error = ?e, "program load failed");
                    missing += 1;
                }
            }
        }
        info!(loaded, missing, "LiteSVM 0.11 programs loaded");

        Ok(Self {
            svm: Mutex::new(svm),
            wsol_ata,
            fail_closed,
            current_slot,
        })
    }

    /// Simulate `tx` against the Yellowstone-fed `cache`. Returns
    /// `Ok(SimOutcome)` when the tx succeeds AND leaves at least
    /// `min_acceptable_out` lamports in the user's WSOL ATA. Returns `Err`
    /// for reverts or unprofitable outcomes — caller should drop the bundle.
    ///
    /// ZERO RPC calls. Every account read from the Yellowstone-fed cache.
    /// The live slot is sourced from the same stream via `current_slot`.
    pub fn simulate(
        &self,
        tx: &VersionedTransaction,
        alts: &[AddressLookupTableAccount],
        cache: &AccountCache,
        min_acceptable_out: u64,
        metrics: &Metrics,
    ) -> Result<SimOutcome> {
        let accounts = collect_tx_accounts(tx, alts);

        // Lazy-fetch any accounts missing from the Yellowstone cache.
        // For AMM pools this is a no-op (all accounts already streamed).
        // For PMM oracle accounts not in the owner-filter, this fetches them
        // once via RPC and caches permanently — subsequent sims are zero-RPC.
        // Runs BEFORE the svm mutex so a blocking RPC does not stall workers.
        for pk in &accounts {
            // Skip DEX/aggregator programs — already loaded as executables via
            // add_program_from_file; RPC-fetching their (large) bytecode would
            // waste a rate-limited slot and the injection loop skips them anyway.
            if is_loaded_program(pk) {
                continue;
            }
            if cache.get(pk).is_none() {
                if let Err(e) = cache.get_or_fetch(pk) {
                    debug!(pubkey = %pk, error = %e, "lazy RPC fetch for missing account");
                }
            }
        }

        // Seed the raw ALT table accounts themselves (not part of `accounts`)
        // so the SVM can expand the tx's v0 address lookups below. Static —
        // fetched once, then cached.
        for alt in alts {
            if cache.get(&alt.key).is_none() {
                if let Err(e) = cache.get_or_fetch(&alt.key) {
                    debug!(alt = %alt.key, error = %e, "lazy RPC fetch for ALT account");
                }
            }
        }

        let mut svm = self.svm.lock().unwrap();

        // Advance the SVM clock to the live Yellowstone slot.
        let live_slot = self.current_slot.load(Ordering::Relaxed);
        svm.warp_to_slot(live_slot);

        // Inject ALT raw accounts so the SVM can expand v0 address lookups.
        for alt in alts {
            if let Some(raw) = cache.get(&alt.key) {
                if let Err(e) = svm.set_account(pk_to_addr(alt.key), raw) {
                    warn!(alt = %alt.key, error = ?e, "set_account(ALT) failed");
                }
            }
        }

        // Inject live account state from the Yellowstone cache.
        // Executable accounts (programs) are already loaded via
        // add_program_from_file and must NOT be overwritten here.
        let mut injected = 0usize;
        let mut missing_cnt = 0usize;
        for pk in &accounts {
            match cache.get(pk) {
                Some(acct) => {
                    if acct.executable() {
                        continue;
                    }
                    if let Err(e) = svm.set_account(pk_to_addr(*pk), acct) {
                        warn!(pubkey = %pk, error = ?e, "set_account failed");
                    } else {
                        injected += 1;
                    }
                }
                None => {
                    missing_cnt += 1;
                }
            }
        }
        debug!(injected, missing = missing_cnt, accounts = accounts.len(), "sim prepared");

        let wsol_before = parse_wsol_amount(&svm, self.wsol_ata);

        // Convert solana-sdk 2.x VersionedTransaction → solana-transaction 3.x.
        let litesvm_tx = to_litesvm_tx(tx)?;

        match svm.simulate_transaction(litesvm_tx) {
            Ok(info) => {
                // post_accounts: Vec<(Address, AccountSharedData)>
                let wsol_ata_addr = pk_to_addr(self.wsol_ata);
                let wsol_after = info
                    .post_accounts
                    .iter()
                    .find(|(addr, _)| *addr == wsol_ata_addr)
                    .and_then(|(_, acc)| parse_token_amount(acc.data()))
                    .unwrap_or(wsol_before);

                let cu = info.meta.compute_units_consumed;

                if wsol_after < min_acceptable_out {
                    metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
                    anyhow::bail!(
                        "sim unprofitable: wsol_after={} < min={}",
                        wsol_after,
                        min_acceptable_out
                    );
                }
                Ok(SimOutcome {
                    compute_units: cu,
                    wsol_after,
                })
            }
            Err(meta) => {
                if self.fail_closed {
                    anyhow::bail!(
                        "sim reverted: err={:?} logs={:#?}",
                        meta.err,
                        meta.meta.logs
                    );
                } else {
                    warn!(
                        err = ?meta.err,
                        logs = ?meta.meta.logs,
                        "sim reverted but fail_open=true, allowing send"
                    );
                    Ok(SimOutcome {
                        compute_units: meta.meta.compute_units_consumed,
                        wsol_after: 0,
                    })
                }
            }
        }
    }
}

/// True if `pk` is a DEX/aggregator program already loaded into LiteSVM as an
/// executable — those must not be RPC-fetched or overwritten from the cache.
fn is_loaded_program(pk: &Pubkey) -> bool {
    let s = pk.to_string();
    crate::program_registry::PROGRAMS
        .iter()
        .any(|(id, _)| *id == s)
}

/// Read the SPL Token amount field from raw account data.
/// Offset 64..72 is the `amount` field in the spl_token::state::Account layout.
fn parse_token_amount(data: &[u8]) -> Option<u64> {
    if data.len() < 72 {
        return None;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[64..72]);
    Some(u64::from_le_bytes(buf))
}

fn parse_wsol_amount(svm: &LiteSVM, wsol_ata: Pubkey) -> u64 {
    let addr = pk_to_addr(wsol_ata);
    svm.get_account(&addr)
        .and_then(|a| parse_token_amount(a.data()))
        .unwrap_or(0)
}

/// Collect every unique account key referenced by the transaction,
/// expanding v0 address table lookups using the resolved ALTs.
fn collect_tx_accounts(
    tx: &VersionedTransaction,
    alts: &[AddressLookupTableAccount],
) -> Vec<Pubkey> {
    let mut out: Vec<Pubkey> = tx.message.static_account_keys().to_vec();
    if let VersionedMessage::V0(v0) = &tx.message {
        for lookup in &v0.address_table_lookups {
            let alt = match alts.iter().find(|a| a.key == lookup.account_key) {
                Some(a) => a,
                None => continue,
            };
            for &idx in &lookup.writable_indexes {
                if let Some(addr) = alt.addresses.get(idx as usize) {
                    out.push(*addr);
                }
            }
            for &idx in &lookup.readonly_indexes {
                if let Some(addr) = alt.addresses.get(idx as usize) {
                    out.push(*addr);
                }
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// Pool of independent Simulator instances for concurrent simulation.
/// Each worker holds its own LiteSVM instance (mutex-protected) so multiple
/// profitable opportunities can be simulated in parallel without contention.
pub struct SimulatorPool {
    sims: Vec<Arc<Simulator>>,
    next: AtomicUsize,
}

impl SimulatorPool {
    pub fn new(
        workers: usize,
        so_dir: &str,
        wsol_ata: Pubkey,
        fail_closed: bool,
        current_slot: Arc<AtomicU64>,
    ) -> Result<Self> {
        let workers = workers.max(1);
        let mut sims = Vec::with_capacity(workers);
        for i in 0..workers {
            let sim = Simulator::new(so_dir, wsol_ata, fail_closed, current_slot.clone())
                .with_context(|| format!("failed to build sim worker #{i}"))?;
            sims.push(Arc::new(sim));
            info!(worker = i, "sim worker initialised");
        }
        info!(workers, "SimulatorPool ready (LiteSVM 0.11, mainnet features)");
        Ok(Self {
            sims,
            next: AtomicUsize::new(0),
        })
    }

    #[inline]
    pub fn acquire(&self) -> Arc<Simulator> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.sims.len();
        self.sims[idx].clone()
    }
}

pub fn resolve_alts(
    alt_addresses: &[String],
    alt_cache: &crate::alt_cache::AltCache,
    rpc: &solana_client::rpc_client::RpcClient,
) -> Result<Vec<AddressLookupTableAccount>> {
    let mut out = Vec::with_capacity(alt_addresses.len());
    for s in alt_addresses {
        let pk = Pubkey::try_from(s.as_str())
            .map_err(|e| anyhow!("bad ALT pubkey {s}: {e:?}"))?;
        out.push(
            alt_cache
                .get_or_fetch(&pk, rpc)
                .context("ALT fetch for sim failed")?,
        );
    }
    Ok(out)
}
