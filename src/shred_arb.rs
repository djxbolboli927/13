//! Decision + execution engine for the ShredStream / Pump.fun ↔ Meteora arb.
//!
//! Flow per detected Pump.fun swap:
//!   1. Predict the Pump.fun pool reserves AFTER the observed trade (we are
//!      ahead of Metis/chain — this is the edge).
//!   2. Read the current Meteora pool state.
//!   3. Compare the post-trade Pump price to the Meteora price → buy the cheaper
//!      venue, sell the dearer one (direction is dynamic).
//!   4. Ternary-search the input size that maximizes net profit, respecting each
//!      pool's slippage.
//!   5. If net > tip + network fee, force-quote each leg on its venue via Metis,
//!      override the output floor to `input + tip + fee`, build the tx, and send
//!      the bundle through the existing Jito paths.

use solana_client::rpc_client::RpcClient;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::alt_cache::AltCache;
use crate::blockhash_cache::BlockhashCache;
use crate::dex_ids::DexKind;
use crate::jito::JitoClient;
use crate::jito_grpc::JitoGrpcClient;
use crate::metis::MetisClient;
use crate::pool_registry::ArbPair;
use crate::pool_state::PoolStateCache;
use crate::pumpfun_math::PumpPool;
use crate::rate_limiter::RateLimiter;
use crate::shred_stream::PumpSwapSignal;
use crate::tokens::WSOL_MINT;
use crate::transaction;

/// Fallback for the max consecutive Metis "No routes found" failures before a
/// pair is disabled, used only if config leaves `metis_load_retry_limit` at 0.
const LOAD_RETRY_LIMIT_DEFAULT: u32 = 10;

/// Max public ALTs attached per transaction: one best-coverage table per leg
/// (buy + sell), never duplicated.
const MAX_ALTS_PER_TX: usize = 2;

/// A raw price gap larger than this is never a real arb — it's a decode
/// artifact or a one-sided dead pool. Real cross-pool gaps are a few percent.
const MAX_PLAUSIBLE_GAP_PCT: f64 = 300.0;

/// Log at most 1 in this many `eval-detail` traces (profitable ones always log).
const EVAL_LOG_SAMPLE: u64 = 50;

/// Identity of one tradeable pair: (pump pool, meteora pool). A token can have
/// SEVERAL Meteora pools (and even several Pump pools), so per-pair bookkeeping
/// (cooldown, dedup, failure strikes) must key on the combination, not just the
/// Pump pool.
type PairKey = (solana_sdk::pubkey::Pubkey, solana_sdk::pubkey::Pubkey);

/// One saved whole-route Metis response for a (pair, direction). On reuse only
/// the two amounts inside the route instruction's Borsh data are rewritten
/// (input and on-chain output floor — the floor already carries fee + tip), so
/// no Metis call is needed. Lives in RAM only: the tokens are short-lived and a
/// fresh process may never trade them again.
#[derive(Clone)]
struct CachedRoute {
    swap_ixs: crate::metis::SwapInstructionsResponse,
    /// Byte offsets of in_amount / quoted_out_amount in the decoded
    /// swap_instruction data (discovered once at capture time).
    in_off: usize,
    out_off: usize,
    /// The Pump pool's `coin_creator` at capture time. Pump can rotate it after
    /// creation, which moves the creator_vault accounts baked into `swap_ixs`;
    /// on reuse we re-derive from the LIVE creator and patch any stale vault.
    coin_creator: Option<solana_sdk::pubkey::Pubkey>,
}

/// Tunables sourced from `[shred_arb]` config.
#[derive(Clone)]
pub struct ArbParams {
    /// Legacy fixed tip (superseded by the dynamic jito_tip_* below).
    #[allow(dead_code)]
    pub tip_lamports: u64,
    /// Network base fee (lamports) included in the on-chain output floor.
    pub network_fee_lamports: u64,
    /// Minimum Jito tip (lamports) on top of the profit share.
    pub jito_tip_min_lamports: u64,
    /// Fraction of detected net profit paid to Jito as tip (0.20 = 20%).
    pub jito_tip_profit_fraction: f64,
    pub meteora_fee_bps: u64,
    /// Ignore observed trades whose SOL-side arg is below this (small trades
    /// barely move price).
    pub min_trigger_lamports: u64,
    pub min_amount_lamports: u64,
    pub max_amount_lamports: u64,
    pub cu_limit: u32,
    /// Per-pool cooldown to avoid firing repeatedly on a burst of shreds.
    pub cooldown_ms: u64,
    /// Cap the BUY-leg size so its price impact stays under this fraction
    /// (e.g. 0.01 = 1%). This is what keeps trades tiny on low-liquidity pools.
    pub max_price_impact: f64,
    /// Enter slightly below the computed optimum for slippage headroom
    /// (e.g. 0.03 = 3% smaller).
    pub size_safety_margin: f64,
    /// Reject opportunities whose predicted net profit exceeds this fraction of
    /// the input (e.g. 0.5 = 50%) — always a mispricing on a dead pool.
    pub max_profit_fraction: f64,
    /// Minimum predicted NET profit (lamports, above the network fee) required
    /// before we fetch instructions and send. Production default 5000. This is
    /// the profit GATE; the on-chain minimum output is set separately to exactly
    /// the input (break-even floor) so any trade that clears the gate at predict
    /// time still lands on-chain as long as it doesn't lose money.
    pub min_net_profit_lamports: u64,
    /// Send directly to the network via RPC instead of Jito bundles.
    pub direct_send: bool,
    /// Priority fee (micro-lamports/CU) for direct sends (0 = none).
    pub direct_priority_fee_microlamports: u64,
    /// `maxAccounts` requested per forced leg (controls tx size).
    pub metis_max_accounts: u64,
    /// SetLoadedAccountsDataSizeLimit byte value (0 = don't add).
    pub loaded_accounts_data_limit: u32,
    /// SetComputeUnitPrice priority fee, micro-lamports/CU (0 = don't add). Used
    /// on the Jito path too; toggled from config for A/B testing landing rates.
    pub compute_unit_price_microlamports: u64,
    /// Max consecutive Metis "No routes" failures before a pair is disabled and
    /// stops wasting compute (0 = use the built-in default of 10).
    pub metis_load_retry_limit: u32,
    /// Skip if the Meteora pool state is more than this many slots stale
    /// (0 = off). The proven fix for the stale-sqrt_price over-prediction.
    pub meteora_max_stale_slots: u64,
    /// Only react to observed trades ≥ this fraction of the Pump WSOL reserve
    /// (0 = disabled, use the absolute min_trigger only).
    pub min_trigger_reserve_frac: f64,
    /// Metis `dexes=` labels for each venue (configurable).
    pub pump_label: String,
    pub meteora_label: String,
    /// Use Jupiter shared accounts (compresses the tx to fit 1232 bytes).
    pub use_shared_accounts: bool,
    /// Minimum gap between two SENDS on the same pool (0 = off). Small so several
    /// opportunities in one block can each send.
    pub send_dedup_ms: u64,
    /// Seconds to wait before polling a sent tx's on-chain fate.
    pub status_check_delay_secs: u64,
    /// "Instructions++": serve repeat routes from the in-RAM instruction cache
    /// instead of calling Metis. Off = always fetch from Metis.
    pub instructions_pp: bool,
    /// Worst-case Meteora fee: price the volatility (dynamic) fee at the pool's
    /// `max_volatility_accumulator` ceiling instead of the stored value, so the
    /// fee is never understated (kills phantom profit). Toggle in config.
    pub meteora_fee_worst_case: bool,
    /// Minimum WSOL depth (lamports) a pool must hold on EACH side to be traded.
    /// If either the Pump or Meteora WSOL reserve is below this, the opportunity
    /// is skipped up-front (before the slippage calc). Default 800_000
    /// (0.0008 WSOL).
    pub min_pool_wsol_lamports: u64,
    /// Never tear a pool down (route failures no longer drop/close it). Kept for
    /// config symmetry; teardown is gated in main.rs, so it's not read here.
    #[allow(dead_code)]
    pub never_close: bool,
    /// DIAGNOSTIC: after each send, re-simulate the exact tx via RPC and log our
    /// predicted output vs. the network's real output + raw swap logs.
    pub rpc_sim_compare: bool,
    /// Disable the pre-send preempt recheck. Default false = preempt ACTIVE:
    /// if the Meteora pool's gRPC state ADVANCED after we computed (someone's
    /// tx touched it) the trade is re-priced on the fresh state and dropped if
    /// no longer profitable. true = always send.
    pub disable_preempt: bool,
    /// SIM-ONLY: run the full detect+sim+reconcile pipeline but never send. The
    /// engine logs opportunities and records sim verdicts; `execute()` returns
    /// before touching the network. Default true.
    pub sim_only: bool,
}

pub struct ShredArbEngine {
    pub metis: Arc<MetisClient>,
    pub blockhash_cache: Arc<BlockhashCache>,
    pub trading_keypair: Arc<Keypair>,
    pub rpc_client: Arc<RpcClient>,
    pub alt_cache: AltCache,
    pub jito: Arc<JitoClient>,
    pub jito_grpc: Option<Arc<JitoGrpcClient>>,
    pub jito_limiter: Arc<Mutex<RateLimiter>>,
    pub jito_grpc_limiter: Option<Arc<Mutex<RateLimiter>>>,
    pub user_pubkey: String,
    pub pool_state: PoolStateCache,
    /// Phase-1 diagnostic ledger (shared with the pool-state gRPC task, which
    /// reconciles our verdicts against the transaction-update stream).
    sim_ledger: Arc<crate::sim_ledger::SimLedger>,
    /// Keyed by Pump.fun pool pubkey (what the signal carries). Each entry holds
    /// EVERY pair for that Pump pool — one per Meteora counter-pool, since a
    /// token can grow additional Meteora pools over time. Shared, mutable
    /// registry so pools can be added/removed at runtime.
    pub registry: Arc<dashmap::DashMap<solana_sdk::pubkey::Pubkey, Vec<ArbPair>>>,
    pub params: ArbParams,
    pub shred_metrics: Arc<crate::shred_stream::ShredMetrics>,
    /// Optional pool manager (teardown is now gated off by never_close, so this
    /// is retained for wiring symmetry but not used on the route-failure path).
    #[allow(dead_code)]
    pub manager: Option<Arc<crate::pool_manager::PoolManager>>,
    /// Self-learning owned ALT — harvests accounts from Metis swap instructions
    /// so the full route compresses under 1232 bytes. Off by default (costs rent).
    pub alt_builder: Option<Arc<crate::alt_builder::AltBuilder>>,
    /// Free ALTs fetched from Jupiter/DFlow/Raptor per pool — the cheap way to
    /// compress the route (no on-chain writes from us).
    pub alt_fetcher: Option<Arc<crate::alt_fetch::AltFetcher>>,
    /// Best-ALT-per-pool registry harvested from competitor shreds (primary
    /// source for fresh pools). One max-coverage ALT per pool.
    pub alt_registry: Arc<crate::alt_registry::AltRegistry>,
    /// In-RAM whole-route instruction cache: one Metis swap-instructions
    /// response per (pair, direction), amount-patched on every reuse so hot
    /// opportunities skip the Metis round-trip entirely. RAM-only by design —
    /// the tokens are short-lived, so nothing is persisted across restarts.
    route_cache: dashmap::DashMap<(PairKey, bool), CachedRoute>,
    last_fired: dashmap::DashMap<PairKey, Instant>,
    /// Last time we actually SENT a tx for a pair — de-dupes the spam of
    /// re-firing the same standing gap every 100ms.
    last_sent: dashmap::DashMap<PairKey, Instant>,
    /// Last shred tx signature observed on each Pump pool — printed by the
    /// fee-audit so the operator can pull that exact tx and hand-check the fee.
    last_shred_sig: dashmap::DashMap<solana_sdk::pubkey::Pubkey, solana_sdk::signature::Signature>,
    /// Pump pools touched by an UNDECODABLE in-flight tx (router / private-bot
    /// CPI) → the newest such slot. Until the gRPC cache reaches that slot the
    /// pool's true reserves are unknown, so `assess` holds trading on it.
    dirty_pump: dashmap::DashMap<solana_sdk::pubkey::Pubkey, u64>,
    /// On-chain fate of sent txs.
    sent_stats: Arc<SentStats>,
    /// Consecutive forced-quote route failures per pair. A pair that can't be
    /// routed on both venues repeatedly is dropped (see `ROUTE_FAIL_LIMIT`).
    route_fail: dashmap::DashMap<PairKey, u32>,
    /// Consecutive "No routes found" load failures per pair while we retry
    /// add-market on BOTH legs. After `LOAD_RETRY_LIMIT` the pair is disabled.
    load_fail: dashmap::DashMap<PairKey, u32>,
    /// Pairs disabled after repeated failed Metis loads (only one leg ever
    /// loaded). Skipped in `assess` so they stop producing No-routes spam. Never
    /// closed (per never_close policy) — just not traded.
    disabled: dashmap::DashSet<PairKey>,
    // ── diagnostics ──
    signals_received: AtomicU64,
    skip_min_trigger: AtomicU64,
    skip_no_pair: AtomicU64,
    skip_cooldown: AtomicU64,
    skip_no_pump_state: AtomicU64,
    skip_no_meteora_state: AtomicU64,
    /// Skipped because the Meteora pool's cached state is older than
    /// `meteora_max_stale_slots` — pricing off a stale sqrt_price is exactly what
    /// caused the over-prediction reverts (proven: math + decode are exact).
    skip_stale_meteora: AtomicU64,
    /// Opaque markers received (router/private-bot tx touched a watched pool).
    opaque_signals: AtomicU64,
    /// Competitor ARB txs whose Meteora leg was IGNORED for state prediction
    /// (assume-first — they mostly revert on the Meteora side).
    arb_ignored: AtomicU64,
    /// Shred signals dropped as BURNED at a block boundary (an older-block
    /// opportunity superseded by a fresher one while we were busy).
    skip_stale_signal: AtomicU64,
    /// Assessments skipped because the pool was dirty (undecodable in-flight tx
    /// seen, gRPC cache not yet caught up past its slot).
    skip_dirty_pump: AtomicU64,
    /// Phase-1 sim verdicts SKIPPED because a dark tx preceded them on the pool
    /// and gRPC had not resynced — kept the ordering honest (no wrong verdict).
    sim_paused_dark: AtomicU64,
    /// Competitor txs NOT applied to the overlay because OUR sim said they
    /// revert (a reverting tx moves nothing — applying it corrupts the state).
    sim_skipped_revert: AtomicU64,
    skip_bad_price: AtomicU64,
    /// Skipped because one side's WSOL depth was below min_pool_wsol_lamports
    /// (the both-sides liquidity guard). Split out of `bad_price` so the operator
    /// can tell a thin-pool skip from a genuinely broken price read.
    skip_thin_pool: AtomicU64,
    skip_implausible: AtomicU64,
    /// Rolling counter to sample the (very chatty) eval-detail trace.
    eval_log_counter: AtomicU64,
    /// Not profitable because the Meteora swap is INFEASIBLE at any size (range
    /// exhausted / would cross a bound) — a real price gap that can't be
    /// crossed. Distinguishes "no route in the pool" from "gap too small".
    skip_uncrossable: AtomicU64,
    evaluated: AtomicU64,
    profitable: AtomicU64,
    not_profitable: AtomicU64,
    sent: AtomicU64,
    // ── why a PROFITABLE opportunity did NOT reach the network ──
    /// Suppressed by the per-pool send de-dup window.
    nosend_dedup: AtomicU64,
    /// Opportunities suppressed because we're in sim-only mode (never sent).
    nosend_sim_only: AtomicU64,
    /// A forced leg quote failed on Metis (no route / timeout).
    nosend_quote_fail: AtomicU64,
    /// /swap-instructions failed.
    nosend_swapix_fail: AtomicU64,
    /// Tx couldn't be built / signed.
    nosend_build_fail: AtomicU64,
    /// Tx exceeded 1232 raw bytes after compression.
    nosend_too_large: AtomicU64,
    /// Tx exceeded 64 account locks (ALTs don't help this — too many distinct
    /// accounts). Distinguished from byte-size so we know which wall we hit.
    nosend_too_locks: AtomicU64,
    /// RPC rejected the send (or Jito path rate-limited).
    nosend_send_err: AtomicU64,
    /// Skipped at the last moment because the pool moved during our compute
    /// window (a competitor's trade landed) so the tx would now revert.
    nosend_preempted: AtomicU64,
    /// Route-instruction cache: sends served from RAM (no Metis round-trip)
    /// vs. sends that had to fetch instructions from Metis first.
    route_cache_hits: AtomicU64,
    route_cache_misses: AtomicU64,
    /// Best (max) net lamports the optimizer found in the current window,
    /// including negatives — shows how close we get when nothing is profitable.
    best_net_seen: AtomicI64,
}

/// Which venue we buy on (and therefore which we sell on).
#[derive(Clone, Copy, PartialEq)]
enum BuyOn {
    Pump,
    Meteora,
}

/// Everything needed to re-check, at the very last moment before sending,
/// whether the opportunity is still alive. Between detection and send we spend
/// ~100-200ms fetching Metis quotes; in that window a competitor's trade may
/// land on the Meteora pool and close the gap, which would make our tx revert.
/// The pump side is held at our predicted post-trade reserves (the trade we're
/// front-running is landing); only the Meteora side is re-read live.
struct Recheck {
    buy_on_pump: bool,
    token_is_a: bool,
    /// Pump-side orientation: is the program's base mint the token? (false =
    /// inverted pool → the pump legs run on the flipped view.)
    pump_token_is_base: bool,
    pump_after: PumpPool,
    met_pool: solana_sdk::pubkey::Pubkey,
    fallback_fee: u64,
    amount_in: u64,
    /// Minimum WSOL out we need (input + network fee + tip).
    min_out: u64,
    /// Token-2022 transfer-fee bps of the intermediate token (0 for classic SPL).
    tfee_bps: u16,
    /// The Meteora pool's gRPC `last_update_slot` at the moment we computed.
    /// The preempt gate compares it to the current value: if the pool account
    /// has NOT advanced (no on-chain state change) the tx is sent as-is; only a
    /// genuine change triggers the re-check.
    met_slot_at_calc: u64,
}

/// Per-leg snapshot taken at DECISION time, logged on every send so the
/// operator can compare each leg's input / predicted output / fee / the exact
/// reserves the math used against the on-chain result — and localize a
/// mis-calculation to the Pump leg or the Meteora leg.
#[derive(Clone, Copy)]
struct LegSnapshot {
    /// Venue of this leg ("Pump" / "Meteora").
    venue: &'static str,
    /// Input amount (lamports WSOL for the buy leg, token base units after).
    input: u64,
    /// Predicted output of this leg alone.
    predicted_out: u64,
    /// Fee (bps) the math charged on this leg.
    fee_bps: u64,
    /// Token-side reserve the calc used.
    reserve_token: u64,
    /// WSOL-side reserve the calc used.
    reserve_wsol: u64,
}

/// On-chain fate of the direct-sent transactions (checked a few seconds after
/// send via `getSignatureStatuses`).
#[derive(Default)]
struct SentStats {
    /// Landed and succeeded.
    landed_ok: AtomicU64,
    /// Landed but the transaction reverted (has an on-chain error).
    landed_err: AtomicU64,
    /// Never found on-chain — dropped / never landed.
    dropped: AtomicU64,
    /// Fate check could not be resolved (RPC error every retry) — accounted so
    /// sent == landed_ok + reverted + dropped + unknown always holds.
    unknown: AtomicU64,
}

impl ShredArbEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        metis: Arc<MetisClient>,
        blockhash_cache: Arc<BlockhashCache>,
        trading_keypair: Arc<Keypair>,
        rpc_client: Arc<RpcClient>,
        alt_cache: AltCache,
        jito: Arc<JitoClient>,
        jito_grpc: Option<Arc<JitoGrpcClient>>,
        jito_limiter: Arc<Mutex<RateLimiter>>,
        jito_grpc_limiter: Option<Arc<Mutex<RateLimiter>>>,
        user_pubkey: String,
        pool_state: PoolStateCache,
        registry: Arc<dashmap::DashMap<solana_sdk::pubkey::Pubkey, Vec<ArbPair>>>,
        params: ArbParams,
        shred_metrics: Arc<crate::shred_stream::ShredMetrics>,
        manager: Option<Arc<crate::pool_manager::PoolManager>>,
        alt_builder: Option<Arc<crate::alt_builder::AltBuilder>>,
        alt_fetcher: Option<Arc<crate::alt_fetch::AltFetcher>>,
        alt_registry: Arc<crate::alt_registry::AltRegistry>,
    ) -> Self {
        Self {
            metis,
            blockhash_cache,
            trading_keypair,
            rpc_client,
            alt_cache,
            jito,
            jito_grpc,
            jito_limiter,
            jito_grpc_limiter,
            user_pubkey,
            sim_ledger: pool_state.sim_ledger(),
            pool_state,
            registry,
            params,
            shred_metrics,
            manager,
            alt_builder,
            alt_fetcher,
            alt_registry,
            route_cache: dashmap::DashMap::new(),
            last_fired: dashmap::DashMap::new(),
            last_sent: dashmap::DashMap::new(),
            last_shred_sig: dashmap::DashMap::new(),
            dirty_pump: dashmap::DashMap::new(),
            sent_stats: Arc::new(SentStats::default()),
            route_fail: dashmap::DashMap::new(),
            load_fail: dashmap::DashMap::new(),
            disabled: dashmap::DashSet::new(),
            signals_received: AtomicU64::new(0),
            skip_min_trigger: AtomicU64::new(0),
            skip_no_pair: AtomicU64::new(0),
            skip_cooldown: AtomicU64::new(0),
            skip_no_pump_state: AtomicU64::new(0),
            skip_no_meteora_state: AtomicU64::new(0),
            skip_stale_meteora: AtomicU64::new(0),
            opaque_signals: AtomicU64::new(0),
            arb_ignored: AtomicU64::new(0),
            skip_stale_signal: AtomicU64::new(0),
            skip_dirty_pump: AtomicU64::new(0),
            sim_paused_dark: AtomicU64::new(0),
            sim_skipped_revert: AtomicU64::new(0),
            skip_bad_price: AtomicU64::new(0),
            skip_thin_pool: AtomicU64::new(0),
            skip_implausible: AtomicU64::new(0),
            eval_log_counter: AtomicU64::new(0),
            skip_uncrossable: AtomicU64::new(0),
            evaluated: AtomicU64::new(0),
            profitable: AtomicU64::new(0),
            not_profitable: AtomicU64::new(0),
            sent: AtomicU64::new(0),
            nosend_dedup: AtomicU64::new(0),
            nosend_sim_only: AtomicU64::new(0),
            nosend_quote_fail: AtomicU64::new(0),
            nosend_swapix_fail: AtomicU64::new(0),
            nosend_build_fail: AtomicU64::new(0),
            nosend_too_large: AtomicU64::new(0),
            nosend_too_locks: AtomicU64::new(0),
            nosend_send_err: AtomicU64::new(0),
            nosend_preempted: AtomicU64::new(0),
            route_cache_hits: AtomicU64::new(0),
            route_cache_misses: AtomicU64::new(0),
            best_net_seen: AtomicI64::new(i64::MIN),
        }
    }

    /// Consume signals forever. The LIVE-STATE ADVANCE runs sequentially on
    /// THIS task, in arrival order: a tx carrying sell+buy on the same pool
    /// emits two signals, and applying them on parallel tasks raced on the
    /// overlay's non-atomic read-modify-write — the losing write dropped one
    /// swap's effect. The advance is pure math (microseconds, no await), so
    /// sequential costs nothing. Only the assessment (Metis round-trips, sends)
    /// fans out to parallel tasks, exactly as before.
    pub async fn run(self: Arc<Self>, mut rx: mpsc::Receiver<PumpSwapSignal>) {
        info!(pools = self.registry.len(), "shred-arb engine running");
        // Signals more than this many slots behind the freshest buffered one are
        // BURNED (their opportunity already landed/reverted): drop them instead
        // of plodding through a backlog. 1 = only the current block survives.
        const STALE_SIGNAL_SLOTS: u64 = 1;
        while let Some(first) = rx.recv().await {
            let mut batch = vec![first];
            while let Ok(s) = rx.try_recv() {
                batch.push(s);
            }
            let newest = batch.iter().map(|s| s.slot).max().unwrap_or(0);
            // Apply in arrival order (sequential — no overlay race); remember
            // the LAST assessable outcome per pool: its pump_after already
            // reflects every earlier swap of the batch, so ONE assessment per
            // pool is enough.
            let mut to_assess: std::collections::HashMap<
                solana_sdk::pubkey::Pubkey,
                (Vec<ArbPair>, PumpPool),
            > = std::collections::HashMap::new();
            // Threaded working state for this batch, keyed by the Pump token
            // vault. Successive legs of the SAME transaction (an MEV sell→buy on
            // one pool is the common case) — and successive in-flight txs on the
            // same pool this slot — MUST each price on the previous leg's OUTPUT
            // reserves, not on the shared pre-batch checkpoint. Without this, a
            // tx's second leg was priced on the pre-first-leg pool and produced a
            // 2-3% wrong output while still logging verdict=OK.
            let mut work: std::collections::HashMap<solana_sdk::pubkey::Pubkey, PumpPool> =
                std::collections::HashMap::new();
            for s in batch {
                if newest.saturating_sub(s.slot) > STALE_SIGNAL_SLOTS {
                    self.skip_stale_signal.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                if let Some(out) = self.apply_signal(&s, &mut work) {
                    to_assess.insert(s.pool, out);
                }
            }
            for (_, (pairs, pump_after)) in to_assess {
                for pair in pairs {
                    let me = self.clone();
                    tokio::spawn(async move {
                        me.assess(&pair, pump_after).await;
                    });
                }
            }
        }
    }

    /// Second opportunity source: on a timer, re-assess EVERY tracked pair from
    /// its CURRENT pool state (no shred trigger). ShredStream only fires on a
    /// Pump trade, so a gap that opens from a Meteora-side move (or between Pump
    /// trades) would otherwise be missed until the next Pump swap. This catches
    /// those. Cheap — the calc is microseconds and only profitable pairs hit
    /// Metis.
    pub fn spawn_state_evaluator(self: Arc<Self>, interval_ms: u64) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms.max(50)));
            // Only re-assess a pool whose Meteora pool OR Pump vaults changed in
            // the last `fresh` window — so we don't burn cycles (and flood logs)
            // re-simulating dead pools where nothing has happened for hours.
            let fresh = Duration::from_millis(interval_ms.saturating_mul(4).max(800));
            loop {
                ticker.tick().await;
                let pairs: Vec<ArbPair> = self
                    .registry
                    .iter()
                    .flat_map(|e| e.value().clone())
                    .collect();
                for pair in pairs {
                    // Freshness: did any relevant account update recently?
                    let recent = [
                        pair.meteora.pool,
                        pair.pump.token_vault(),
                        pair.pump.wsol_vault(),
                    ]
                    .iter()
                    .filter_map(|a| self.pool_state.last_update_age(a))
                    .any(|age| age <= fresh);
                    if !recent {
                        continue; // nothing changed → no new opportunity
                    }
                    let pump_now = match self.pool_state.pump_pool(
                        &pair.pump.pool,
                        &pair.pump.token_vault(),
                        &pair.pump.wsol_vault(),
                        &pair.token_mint,
                    ) {
                        Some(p) => p,
                        None => continue,
                    };
                    // No prediction — price against the current Pump state.
                    // Each pair on its own task so a Metis round-trip on one
                    // pool never serializes the sweep.
                    let me = self.clone();
                    tokio::spawn(async move {
                        me.assess(&pair, pump_now).await;
                    });
                }
            }
        });
    }

    /// Synchronous per-signal state advance — called SEQUENTIALLY from `run` so
    /// multiple swaps of one tx (e.g. HOOD-style sell+buy on the same pool)
    /// apply in instruction order with no race. Pure math, no await. Returns
    /// the pairs to assess and the post-trade Pump state, or `None` when there
    /// is nothing to assess (unknown pool, opaque marker, below trigger, …).
    /// Phase-1 diagnostic: compute a competitor tx's OWN leg output(s) and
    /// revert verdict on the PRE-swap state, then record it in the sim ledger
    /// (which reconciles against the gRPC transaction-update). `pump_pre` is the
    /// Pump state the tx executes against (normalized base=token/quote=WSOL).
    /// Returns `Some(tx_revert)` — whether OUR sim says this tx reverts — or
    /// `None` when we could not produce a verdict (dark-paused). The caller uses
    /// it to decide whether to advance the pool overlay: a reverting tx moves
    /// NOTHING, so it must NOT be applied (that was corrupting our state).
    fn record_sim_verdict(
        &self,
        sig: &PumpSwapSignal,
        pairs: &[ArbPair],
        pump_pre: &PumpPool,
        token_is_base: bool,
        econ_buy: bool,
    ) -> Option<bool> {
        use crate::shred_stream::PumpIxKind as K;
        // ── Order guard: WAIT after a dark (undecodable) tx ──────────────────
        // If a router/aggregator/private-bot tx touched this pool and gRPC has
        // NOT yet delivered the post-dark state, our pre-state is missing that
        // tx's effect — so simulating the NEXT tx here would give a wrong verdict
        // and a spurious mismatch. Per the ordering rule, PAUSE this pool's sim
        // until the account-update (checkpoint) arrives; then resume. (The dark
        // marker was set and is cleared on catch-up by the dirty gate.)
        if let Some(d) = self.dirty_pump.get(&sig.pool) {
            let dark_slot = *d.value();
            drop(d);
            let vault = pairs.first().map(|p| p.pump.wsol_vault());
            let caught_up = vault
                .and_then(|v| self.pool_state.last_update_slot(&v))
                .unwrap_or(0)
                >= dark_slot;
            if !caught_up {
                self.sim_paused_dark.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        }
        // Pump leg on the RAW-orientation view (flip for inverted pools so the
        // raw base/quote amounts and fee side match on-chain).
        let raw = if token_is_base {
            *pump_pre
        } else {
            pump_pre.flipped()
        };
        let (kind_label, pump_in, pump_out, pump_bound, pump_revert) = match sig.kind {
            K::Buy => {
                let quote_in = raw.sim_buy_quote_in(sig.base_amount);
                // revert iff required quote in EXCEEDS the user's max.
                ("buy", sig.base_amount, quote_in, sig.pump_slippage, quote_in > sig.pump_slippage)
            }
            K::Sell => {
                let quote_out = raw.sim_sell_quote_out(sig.base_amount);
                ("sell", sig.base_amount, quote_out, sig.pump_slippage, quote_out < sig.pump_slippage)
            }
            K::BuyQuoteIn => {
                let base_out = raw.sim_buy_quote_in_base_out(sig.quote_amount);
                ("buy_exact_quote_in", sig.quote_amount, base_out, sig.pump_slippage, base_out < sig.pump_slippage)
            }
            K::BoostBuyBurn => {
                let base_out = raw.sim_boost_base_out(sig.quote_amount);
                ("boost", sig.quote_amount, base_out, sig.pump_slippage, base_out < sig.pump_slippage)
            }
            K::Opaque => return None,
        };

        // Meteora leg (2-hop arb), on its PRE-swap state. Direction = OPPOSITE of
        // the competitor's economic Pump direction (circular arb).
        let mut met_present = false;
        let (mut met_in, mut met_out, mut met_bound, mut met_revert) = (0u64, 0u64, 0u64, false);
        if let (Some(mpool), Some(amt), Some(bound)) =
            (sig.meteora_pool, sig.meteora_amount_in, sig.meteora_min_out)
        {
            if let Some(pair) = pairs.iter().find(|p| p.meteora.pool == mpool) {
                let fee_num = self.params.meteora_fee_bps.saturating_mul(100_000);
                if let Some(met) = self.pool_state.meteora_pool(
                    &mpool,
                    fee_num,
                    self.params.meteora_fee_worst_case,
                ) {
                    met_present = true;
                    met_in = amt;
                    met_bound = bound;
                    if sig.meteora_exact_out {
                        // Reverse curve — our forward math doesn't apply; log the
                        // leg but don't claim a verdict (kept as OK).
                        met_out = 0;
                        met_revert = false;
                    } else {
                        // competitor pump BUY ⇒ they SELL token into Meteora
                        // (token in ⇒ a_to_b = token_is_a); vice versa.
                        let a_to_b = if econ_buy {
                            pair.meteora.token_is_a
                        } else {
                            !pair.meteora.token_is_a
                        };
                        // Cap to the OUT-side reserve (a pool can't pay out more
                        // than it holds). wsol_reserve(true)=reserve_B (out when
                        // a_to_b), wsol_reserve(false)=reserve_A — so the out-side
                        // reserve is exactly wsol_reserve(a_to_b).
                        let out_reserve = met.wsol_reserve(a_to_b);
                        met_out = met
                            .swap_exact_in(amt, a_to_b)
                            .map(|s| s.amount_out.min(out_reserve))
                            .unwrap_or(0);
                        met_revert = met_out < bound;
                    }
                }
            }
        }

        // ── Token-2022 transfer fee (READ from the mint state; usually 0) ────
        // Read the live transfer-fee rate from the token's mint account and
        // compute the fee taken on the TOKEN (base) amount that moved in this
        // Pump swap, so it can be compared against the real tx on solscan. The
        // token base amount is `base_amount` for buy/sell (exact-base sides) and
        // the computed base_out for the exact-quote-in / boost sides.
        let tfee_bps = match pairs.first() {
            Some(p) => self.pool_state.mint_transfer_fee_bps(&p.token_mint) as u64,
            None => 0,
        };
        let token_amt = match sig.kind {
            K::Buy | K::Sell => sig.base_amount,
            K::BuyQuoteIn | K::BoostBuyBurn => pump_out,
            K::Opaque => 0,
        };
        let tfee_token = if tfee_bps == 0 {
            0
        } else {
            (((token_amt as u128) * tfee_bps as u128 + 9_999) / 10_000) as u64
        };

        let tx_revert = pump_revert || (met_present && met_revert);
        // Predicted post-swap Pump reserves this tx leaves behind — exactly the
        // state we carry to the next tx. A reverted tx moves NOTHING, so it stays
        // at the pre-swap reserves. The reconcile compares this to the real
        // post_token_balances so a wrong AMOUNT is caught even when the revert
        // verdict is right.
        let pred_pool = if tx_revert {
            *pump_pre
        } else {
            pump_pre.after_observed(token_is_base, sig.kind, sig.base_amount, sig.quote_amount)
        };
        let (pump_token_vault, pump_wsol_vault) = match pairs.first() {
            Some(p) => (Some(p.pump.token_vault()), Some(p.pump.wsol_vault())),
            None => (None, None),
        };
        self.sim_ledger.record(
            sig.sig,
            crate::sim_ledger::SimRecord {
                slot: sig.slot,
                hops: sig.hops,
                pump_pool: Some(sig.pool),
                meteora_pool: sig.meteora_pool,
                kind: kind_label,
                pump_in,
                pump_out,
                pump_bound,
                pump_revert,
                pump_present: true,
                met_in,
                met_out,
                met_bound,
                met_revert,
                met_present,
                tx_revert,
                tfee_bps: tfee_bps as u16,
                tfee_token,
                pump_token_vault,
                pump_wsol_vault,
                pred_base: pred_pool.base_reserve,
                pred_quote: pred_pool.quote_reserve,
            },
        );
        Some(tx_revert)
    }

    fn apply_signal(
        &self,
        sig: &PumpSwapSignal,
        work: &mut std::collections::HashMap<solana_sdk::pubkey::Pubkey, PumpPool>,
    ) -> Option<(Vec<ArbPair>, PumpPool)> {
        self.signals_received.fetch_add(1, Ordering::Relaxed);
        let pairs = match self.registry.get(&sig.pool) {
            Some(p) => p.clone(),
            None => {
                self.skip_no_pair.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        let Some(first) = pairs.first() else {
            self.skip_no_pair.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        // Record the observed tx signature for this pool (for the fee-audit log).
        // AFTER the registry hit, so unknown pools can't grow the map unbounded.
        self.last_shred_sig.insert(sig.pool, sig.sig);

        // The Pump vaults are identical across every pair of this pool.
        let token_vault = first.pump.token_vault();
        let wsol_vault = first.pump.wsol_vault();

        // ── Opaque marker: a router / private-bot tx touches OUR pool ────────
        // The swap rides in a CPI we cannot decode statically, so the pool is
        // about to change by an UNKNOWN amount. Drop the live overlay and hold
        // trading (see the dirty gate in `assess`) until the gRPC cache has
        // caught up past this slot. Our own in-flight arb txs also look opaque
        // (they swap through the Metis route program) — never self-gate.
        if sig.kind == crate::shred_stream::PumpIxKind::Opaque {
            if sig.fee_payer == self.trading_keypair.pubkey() {
                return None;
            }
            self.opaque_signals.fetch_add(1, Ordering::Relaxed);
            self.pool_state.invalidate_pump(&token_vault);
            self.dirty_pump
                .entry(sig.pool)
                .and_modify(|s| *s = (*s).max(sig.slot))
                .or_insert(sig.slot);
            return None;
        }

        // ── Orientation (THE inverted-pool fix) ──────────────────────────────
        // `token_is_a` on the Pump side == "the program's BASE mint is the
        // token". On an INVERTED pool (base = WSOL, e.g. WSOL-HOOD) the raw
        // instructions flip economic meaning: `sell` = WSOL in = token BUY,
        // `buy` = WSOL out = token SELL — and `base_amount` is WSOL lamports.
        let token_is_base = first.pump.token_is_a;
        // Economic direction: did this trade BUY the token (WSOL into pool)?
        // Buy / BuyQuoteIn / BoostBuyBurn all push QUOTE in and take BASE out.
        let quote_to_base = sig.kind != crate::shred_stream::PumpIxKind::Sell;
        let econ_buy = if token_is_base {
            quote_to_base
        } else {
            !quote_to_base
        };
        // SOL-side size for the triggers: normal pools → the quote limit arg (a
        // proxy, may be u64::MAX on unlimited-slippage buys); inverted pools →
        // `base_amount` IS the exact WSOL leg.
        let sol_side = if token_is_base {
            sig.quote_amount
        } else {
            sig.base_amount
        };

        // Current Pump reserves. Prefer the batch WORKING state (threads earlier
        // legs of THIS tx / earlier in-flight txs on this pool this batch); fall
        // back to the live overlay (checkpoint + cross-batch shreds) and finally
        // to the confirmed gRPC decode.
        let pump_now = match work.get(&token_vault).copied().or_else(|| {
            self.pool_state.pump_pool_live(
                &first.pump.pool,
                &token_vault,
                &wsol_vault,
                &first.token_mint,
                sig.slot,
            )
        }) {
            Some(p) => p,
            None => {
                self.skip_no_pump_state.fetch_add(1, Ordering::Relaxed);
                debug!(pool = %sig.pool, "pump reserves not cached yet");
                return None;
            }
        };

        // ── Simulate THIS tx on the PRE-swap state: compute its own output +
        // revert verdict, record it for reconcile, and — crucially — decide
        // whether it moves the pool. A tx our sim says REVERTS moves NOTHING, so
        // applying it would corrupt the state the NEXT tx prices against (exactly
        // the "we didn't write the pool state correctly" problem). Only advance
        // the overlay for a tx we believe SUCCEEDS. Dark-paused (None) → hold.
        let verdict = self.record_sim_verdict(sig, &pairs, &pump_now, token_is_base, econ_buy);
        match verdict {
            Some(false) => {
                // Believed to SUCCEED → advance BOTH the batch working state (so
                // the NEXT leg of this tx prices on this leg's output) and the
                // cross-batch live overlay. Threaded on the SAME pre-leg state
                // we just priced, so the two never diverge.
                let advanced = pump_now.after_observed(
                    token_is_base,
                    sig.kind,
                    sig.base_amount,
                    sig.quote_amount,
                );
                work.insert(token_vault, advanced);
                self.pool_state.apply_pump_swap(
                    &first.pump.pool,
                    &token_vault,
                    &wsol_vault,
                    &first.token_mint,
                    token_is_base,
                    sig.kind,
                    sig.base_amount,
                    sig.quote_amount,
                    sig.slot,
                );
            }
            Some(true) => {
                // Believed to REVERT → moves NOTHING. Keep the working state at
                // the pre-leg reserves so a following leg sees the unchanged pool.
                work.insert(token_vault, pump_now);
                self.sim_skipped_revert.fetch_add(1, Ordering::Relaxed);
            }
            None => {
                // Dark-paused → we are waiting for the gRPC checkpoint; do not
                // advance (the overlay was already invalidated for the dark tx).
                work.insert(token_vault, pump_now);
            }
        }
        // Meteora legs of competitor ARB txs are NOT applied to state
        // (assume-first): they overwhelmingly REVERT on the Meteora side, so
        // advancing our sqrt_price by them over-moves the price — that exact
        // over-move was a proven source of over-sized entries. The Pump leg
        // above IS real flow either way (it also reverts with the tx, but the
        // gRPC update corrects us within a block; competitor Pump legs that
        // LAND are exactly what we must reflect). Meteora relies on its own
        // gRPC updates + the freshness gate + the pre-send preempt recheck.
        if sig.meteora_pool.is_some() {
            self.arb_ignored.fetch_add(1, Ordering::Relaxed);
        }

        // ── Triggers gate ONLY the assessment, never the state advance ───────
        if sol_side < self.params.min_trigger_lamports {
            self.skip_min_trigger.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        if self.params.min_trigger_reserve_frac > 0.0 {
            let thresh =
                (pump_now.quote_reserve as f64 * self.params.min_trigger_reserve_frac) as u64;
            if sol_side < thresh {
                self.skip_min_trigger.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        }

        // Predicted post-trade Pump reserves — the batch working state now holds
        // this leg's output (threaded), so price the opportunity against it.
        let pump_after = work.get(&token_vault).copied().unwrap_or(pump_now);
        Some((pairs, pump_after))
    }

    /// Shared assessment core: read Meteora, choose direction, size the trade,
    /// and execute if it clears the profit gate. `pump_after` is the Pump state
    /// to price/quote against — predicted post-trade reserves for a shred
    /// trigger, or the current reserves for a pool-state-update trigger.
    async fn assess(&self, pair: &ArbPair, pump_after: PumpPool) {
        let key: PairKey = (pair.pump.pool, pair.meteora.pool);
        // Skip pairs we disabled after repeated Metis load failures (only one leg
        // ever loaded) — they'd only produce No-routes spam.
        if self.disabled.contains(&key) {
            return;
        }
        // Cooldown per pair — applies only after a fire, so the state evaluator
        // can keep re-checking a not-yet-profitable pair every tick.
        if let Some(prev) = self.last_fired.get(&key) {
            if prev.elapsed() < Duration::from_millis(self.params.cooldown_ms) {
                self.skip_cooldown.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        // ── Dirty-pool gate ──────────────────────────────────────────────────
        // An in-flight tx we could NOT decode (router / private-bot CPI) touched
        // this Pump pool in slot `d` — its reserves are about to change by an
        // unknown amount. Pricing now risks a doomed send; hold until the gRPC
        // cache reaches that slot, then clear the mark.
        if let Some(d) = self.dirty_pump.get(&pair.pump.pool) {
            let seen = *d.value();
            drop(d);
            let caught_up = self
                .pool_state
                .last_update_slot(&pair.pump.wsol_vault())
                .unwrap_or(0)
                >= seen;
            if caught_up {
                self.dirty_pump
                    .remove_if(&pair.pump.pool, |_, s| *s <= seen);
            } else {
                self.skip_dirty_pump.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        self.evaluated.fetch_add(1, Ordering::Relaxed);

        // 2) Meteora state.
        // config fallback fee is in bps; convert to the 1e9-denominated numerator.
        let fallback_fee_numerator = self.params.meteora_fee_bps.saturating_mul(100_000);
        let meteora = match self
            .pool_state
            .meteora_pool(&pair.meteora.pool, fallback_fee_numerator, self.params.meteora_fee_worst_case)
        {
            Some(m) => m,
            None => {
                self.skip_no_meteora_state.fetch_add(1, Ordering::Relaxed);
                debug!(pool = %pair.meteora.pool, "meteora state not cached yet");
                return;
            }
        };

        // ── Meteora freshness gate ───────────────────────────────────────────
        // Both a source audit and a real-swap reconstruction proved the Meteora
        // math + decode are EXACT: given the correct sqrt_price/liquidity we
        // reproduce the on-chain output to the lamport. So the ~3-6% over-
        // prediction that reverts 0x1771 is a STALE sqrt_price — we priced off a
        // snapshot older than the state the tx executes against (the pool-state
        // gRPC feed lags for low-activity Meteora accounts). Refuse to trade a
        // Meteora pool whose cached state is more than `meteora_max_stale_slots`
        // behind the newest slot we've seen. 0 = gate off.
        if self.params.meteora_max_stale_slots > 0 {
            let behind = self
                .pool_state
                .slot()
                .saturating_sub(self.pool_state.last_update_slot(&pair.meteora.pool).unwrap_or(0));
            if behind > self.params.meteora_max_stale_slots {
                self.skip_stale_meteora.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }

        // 3) Direction: compare raw price (WSOL-raw per token-raw) on both.
        let pump_price = if pump_after.base_reserve == 0 {
            self.skip_bad_price.fetch_add(1, Ordering::Relaxed);
            return;
        } else {
            pump_after.quote_reserve as f64 / pump_after.base_reserve as f64
        };
        let met_price = meteora.token_price_in_sol(pair.meteora.token_is_a, 0, 0);
        if met_price <= 0.0 {
            self.skip_bad_price.fetch_add(1, Ordering::Relaxed);
            return;
        }

        // ── Liquidity guard (BOTH sides, BEFORE any slippage calc) ───────────
        // If EITHER pool's WSOL depth is below the floor, skip immediately —
        // don't waste the compute on a pool too thin to trade (its slippage
        // eats any gap and just fabricates profit). Checked up-front on both the
        // Pump WSOL reserve and the Meteora WSOL reserve. Default 800_000
        // lamports = 0.0008 WSOL; a pool that recovers above it trades again.
        let token_is_a = pair.meteora.token_is_a;
        let pump_wsol_depth = pump_after.quote_reserve;
        let met_wsol_depth = meteora.wsol_reserve(token_is_a);
        if pump_wsol_depth < self.params.min_pool_wsol_lamports
            || met_wsol_depth < self.params.min_pool_wsol_lamports
        {
            self.skip_thin_pool.fetch_add(1, Ordering::Relaxed);
            return;
        }

        let buy_on = if pump_price < met_price {
            BuyOn::Pump // token cheaper on pump
        } else {
            BuyOn::Meteora
        };

        // Token-2022 transfer fee (0 for classic SPL). The intermediate token is
        // transferred TWICE — out of the buy pool to us, then from us into the
        // sell pool — and the on-chain program excludes the transfer fee on both
        // legs. Model it, or our predicted output over-states reality and the tx
        // reverts 0x1771. Ceiling the fee (pool-favorable) so we never understate.
        let tfee_bps = self.pool_state.mint_transfer_fee_bps(&pair.token_mint) as u128;
        let apply_tfee = move |amt: u64| -> u64 {
            if tfee_bps == 0 {
                return amt;
            }
            let fee = ((amt as u128) * tfee_bps + 9_999) / 10_000;
            (amt as u128).saturating_sub(fee) as u64
        };

        // Pump legs, orientation-exact. On an INVERTED pool (program base =
        // WSOL) our WSOL-in buy is the program's `sell` (fee on the token OUT
        // side) and our token-in sell is the program's `buy` (fee added on the
        // token IN side) — run the raw math on the flipped view so the fee
        // lands on the same side as on-chain, to the lamport.
        let pump_token_is_base = pair.pump.token_is_a;
        let pump_buy_leg = |p: &PumpPool, wsol_in: u64| -> u64 {
            if pump_token_is_base {
                p.quote_buy(wsol_in)
            } else {
                p.flipped().quote_sell(wsol_in)
            }
        };
        let pump_sell_leg = |p: &PumpPool, token_in: u64| -> u64 {
            if pump_token_is_base {
                p.quote_sell(token_in)
            } else {
                p.flipped().quote_buy(token_in)
            }
        };

        // 4) Optimal size.
        let eval = |x: u64| -> Option<u64> {
            match buy_on {
                BuyOn::Pump => {
                    let base_out = pump_buy_leg(&pump_after, x);
                    if base_out == 0 {
                        return None;
                    }
                    let tok = apply_tfee(apply_tfee(base_out));
                    if tok == 0 {
                        return None;
                    }
                    meteora.sell_token_for_wsol(tok, token_is_a)
                }
                BuyOn::Meteora => {
                    let base_out = meteora.buy_token_with_wsol(x, token_is_a)?;
                    if base_out == 0 {
                        return None;
                    }
                    let tok = apply_tfee(apply_tfee(base_out));
                    if tok == 0 {
                        return None;
                    }
                    Some(pump_sell_leg(&pump_after, tok))
                }
            }
        };

        // Profit GATE: the optimizer maximizes `out - x - required_extra`, where
        // `required_extra` is the minimum profit we insist on (default 5000 =
        // one network base fee). Direct sends pay only that fee, no Jito tip.
        // NOTE: this is only the gate — the on-chain minimum output is set to
        // exactly the input (break-even) at send time, so a trade that clears
        // the gate at predict time still lands as long as it doesn't lose money.
        let required_extra = self.params.min_net_profit_lamports;

        // Ceiling on trade size: never add more WSOL than a fraction of the
        // BUY pool's current WSOL reserve (keeps the swap in a valid range and
        // slippage sane). `max_price_impact` is that fraction (default 1.0).
        // The optimizer then finds the net-maximizing size WITHIN this range —
        // that size already balances the price gap against slippage, which is
        // the real "optimal volume".
        let buy_wsol_reserve = match buy_on {
            BuyOn::Pump => pump_after.quote_reserve,
            BuyOn::Meteora => meteora.wsol_reserve(token_is_a),
        };
        // Dead-pool guards — skip empty/broken pools early (they were flooding
        // the logs and wasting cycles): the BUY side must hold real WSOL depth,
        // and the price gap must be plausible. A real cross-pool gap is small
        // (competitors trade ~0.2%); a 100%+ "gap" is a decode artifact or a
        // one-sided dead pool, never an executable arb.
        let gap_pct = (pump_price - met_price) / met_price * 100.0;
        if buy_wsol_reserve < 5_000 || gap_pct.abs() > MAX_PLAUSIBLE_GAP_PCT {
            self.skip_bad_price.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let liq_ceiling = ((buy_wsol_reserve as f64) * self.params.max_price_impact) as u64;
        let hi = self
            .params
            .max_amount_lamports
            .min(liq_ceiling)
            .max(self.params.min_amount_lamports);

        let (opt_x, opt_net) =
            optimize_size(self.params.min_amount_lamports, hi, required_extra, &eval);

        // Track how close we get, even when nothing is profitable, for tuning.
        self.best_net_seen
            .fetch_max(opt_net.clamp(i64::MIN as i128, i64::MAX as i128) as i64, Ordering::Relaxed);

        // Detailed per-evaluation trace — DEBUG only (RUST_LOG=...=debug) and
        // sampled, so a normal run never prints these negative/eval lines. The
        // actionable, always-printed event is the "shred-arb opportunity" log
        // below (a real send) and the sent-tx fate logs.
        if tracing::enabled!(tracing::Level::DEBUG)
            && self.eval_log_counter.fetch_add(1, Ordering::Relaxed) % EVAL_LOG_SAMPLE == 0
        {
            let sample = |x: u64| -> Option<i64> {
                eval(x).map(|o| o as i64 - x as i64 - required_extra as i64)
            };
            debug!(
                token = %pair.token_mint,
                buy = if buy_on == BuyOn::Pump { "Pump" } else { "Meteora" },
                gap_pct,
                pump_after_price = pump_price,
                met_price,
                met_wsol_depth = meteora.wsol_reserve(token_is_a),
                buy_wsol_reserve,
                hi,
                opt_x,
                opt_net = opt_net as i64,
                net_1k = ?sample(1_000),
                net_10k = ?sample(10_000),
                net_100k = ?sample(100_000),
                "eval-detail"
            );
        }

        if opt_net <= 0 {
            // Diagnose: if even a tiny buy is infeasible, the Meteora side is
            // range-exhausted (a real gap we simply cannot cross) rather than
            // the gap being too small.
            if eval(self.params.min_amount_lamports.max(1_000)).is_none() {
                self.skip_uncrossable.fetch_add(1, Ordering::Relaxed);
            }
            self.not_profitable.fetch_add(1, Ordering::Relaxed);
            return;
        }

        // Enter slightly below the optimum for slippage headroom.
        let best_x = (((opt_x as f64) * (1.0 - self.params.size_safety_margin)) as u64)
            .max(self.params.min_amount_lamports);
        let best_out = match eval(best_x) {
            Some(o) => o,
            None => {
                self.not_profitable.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        let profit_floor = best_x + required_extra;
        if best_out <= profit_floor {
            self.not_profitable.fetch_add(1, Ordering::Relaxed);
            return; // predicted profit below the minimum gate
        }
        let net = best_out - profit_floor;

        // Plausibility guard: a real cross-pool gap is small. A predicted net
        // above `max_profit_fraction` of the input is always a dead-pool
        // mispricing — refuse to send garbage.
        if net as f64 > best_x as f64 * self.params.max_profit_fraction {
            self.skip_implausible.fetch_add(1, Ordering::Relaxed);
            debug!(
                pool = %pair.pump.pool, input = best_x, net, "skip implausible profit (mispriced pool)"
            );
            return;
        }
        self.profitable.fetch_add(1, Ordering::Relaxed);

        let (buy_kind, sell_kind) = match buy_on {
            BuyOn::Pump => (DexKind::PumpFunAmm, DexKind::MeteoraDammV2),
            BuyOn::Meteora => (DexKind::MeteoraDammV2, DexKind::PumpFunAmm),
        };
        // ── Per-leg snapshot (operator-requested diagnostics) ────────────────
        // For EACH leg, at decision time: input, predicted output, the fee the
        // math charged, and the exact token/WSOL reserves it used. Logged with
        // every send so a per-leg over/under-prediction is directly visible
        // against the on-chain result. Meteora token-side reserve is
        // wsol_reserve with the flag inverted (it computes the other side).
        let met_reserve_wsol = meteora.wsol_reserve(token_is_a);
        let met_reserve_token = meteora.wsol_reserve(!token_is_a);
        // Meteora effective fee (numerator over 1e9 → bps).
        let met_fee_bps = meteora.fee_numerator / 100_000;
        let (leg1, leg2) = match buy_on {
            BuyOn::Pump => {
                let t_out = pump_buy_leg(&pump_after, best_x);
                let tok = apply_tfee(apply_tfee(t_out));
                (
                    LegSnapshot {
                        venue: "Pump",
                        input: best_x,
                        predicted_out: t_out,
                        fee_bps: pump_after.total_fee_bps,
                        reserve_token: pump_after.base_reserve,
                        reserve_wsol: pump_after.quote_reserve,
                    },
                    LegSnapshot {
                        venue: "Meteora",
                        input: tok,
                        predicted_out: best_out,
                        fee_bps: met_fee_bps,
                        reserve_token: met_reserve_token,
                        reserve_wsol: met_reserve_wsol,
                    },
                )
            }
            BuyOn::Meteora => {
                let t_out = meteora.buy_token_with_wsol(best_x, token_is_a).unwrap_or(0);
                let tok = apply_tfee(apply_tfee(t_out));
                (
                    LegSnapshot {
                        venue: "Meteora",
                        input: best_x,
                        predicted_out: t_out,
                        fee_bps: met_fee_bps,
                        reserve_token: met_reserve_token,
                        reserve_wsol: met_reserve_wsol,
                    },
                    LegSnapshot {
                        venue: "Pump",
                        input: tok,
                        predicted_out: best_out,
                        fee_bps: pump_after.total_fee_bps,
                        reserve_token: pump_after.base_reserve,
                        reserve_wsol: pump_after.quote_reserve,
                    },
                )
            }
        };
        // ── Staleness diagnostic (per DEX) ───────────────────────────────────
        // For each venue: the slot we last got fresh account data, how many slots
        // behind the newest slot we've seen that is, and the wall-clock age. On a
        // thin Meteora pool that hasn't traded for a couple of blocks, this shows
        // the cached state is many slots old — the root of the over-prediction we
        // proved is NOT a formula bug. Pump vaults update almost every block, so
        // their gap should stay ~0.
        let cur_slot = self.pool_state.slot();
        let fmt_stale = |acct: &solana_sdk::pubkey::Pubkey| -> String {
            let upd = self.pool_state.last_update_slot(acct);
            let age_ms = self
                .pool_state
                .last_update_age(acct)
                .map(|d| d.as_millis())
                .unwrap_or(0);
            match upd {
                Some(s) => format!("slot={s} behind={} age={age_ms}ms", cur_slot.saturating_sub(s)),
                None => "uncached".to_string(),
            }
        };
        let met_stale = fmt_stale(&pair.meteora.pool);
        let pump_base_stale = fmt_stale(&pair.pump.token_vault());
        let pump_quote_stale = fmt_stale(&pair.pump.wsol_vault());
        info!(
            pool = %pair.pump.pool,
            meteora = %pair.meteora.pool,
            token = %pair.token_mint,
            buy = ?buy_kind_label(buy_kind),
            input = best_x,
            predicted_out = best_out,
            net_lamports = net,
            calc_slot = cur_slot,
            pump_behind = self.pool_state.pump_behind(&pair.pump.token_vault()),
            meteora_state = %met_stale,
            pump_token_vault_state = %pump_base_stale,
            pump_wsol_vault_state = %pump_quote_stale,
            leg1_venue = leg1.venue,
            leg1_in = leg1.input,
            leg1_predicted_out = leg1.predicted_out,
            leg1_fee_bps = leg1.fee_bps,
            leg1_reserve_token = leg1.reserve_token,
            leg1_reserve_wsol = leg1.reserve_wsol,
            leg2_venue = leg2.venue,
            leg2_in = leg2.input,
            leg2_predicted_out = leg2.predicted_out,
            leg2_fee_bps = leg2.fee_bps,
            leg2_reserve_token = leg2.reserve_token,
            leg2_reserve_wsol = leg2.reserve_wsol,
            "shred-arb opportunity"
        );

        self.last_fired.insert(key, Instant::now());
        // Jito tip = min tip + a share of the detected profit (e.g. 1000 + 20%).
        // On-chain output floor = input + network fee + tip, so the tx reverts
        // unless it at least covers the fee and the tip (we keep the rest). Only
        // opportunities whose realized profit exceeds fee+tip actually land.
        let tip = self.params.jito_tip_min_lamports
            + (net as f64 * self.params.jito_tip_profit_fraction) as u64;
        let onchain_floor = best_x + self.params.network_fee_lamports + tip;
        let recheck = Recheck {
            buy_on_pump: buy_on == BuyOn::Pump,
            token_is_a,
            pump_token_is_base,
            pump_after,
            met_pool: pair.meteora.pool,
            fallback_fee: fallback_fee_numerator,
            amount_in: best_x,
            min_out: onchain_floor,
            tfee_bps: tfee_bps as u16,
            met_slot_at_calc: self
                .pool_state
                .last_update_slot(&pair.meteora.pool)
                .unwrap_or(0),
        };
        self.execute(
            pair, key, buy_kind, sell_kind, best_x, onchain_floor, best_out, tip, recheck,
            [leg1, leg2],
        )
        .await;
    }

    /// Spawn the off-hot-path RPC sim-compare for a just-sent tx (no-op unless
    /// `rpc_sim_compare` is enabled). Clones only what the task needs.
    fn spawn_sim_compare(
        &self,
        tx: solana_sdk::transaction::VersionedTransaction,
        predicted_out: u64,
        amount_in: u64,
        token: String,
    ) {
        if !self.params.rpc_sim_compare {
            return;
        }
        let wsol_ata = spl_associated_token_account::get_associated_token_address(
            &self.trading_keypair.pubkey(),
            &crate::dex_ids::wsol_mint(),
        );
        let rpc = self.rpc_client.clone();
        tokio::spawn(async move {
            sim_compare(rpc, tx, predicted_out, amount_in, token, wsol_ata).await;
        });
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute(
        &self,
        pair: &ArbPair,
        key: PairKey,
        buy_kind: DexKind,
        sell_kind: DexKind,
        amount_in: u64,
        floor: u64,
        predicted_out: u64,
        tip: u64,
        recheck: Recheck,
        legs: [LegSnapshot; 2],
    ) {
        // SIM-ONLY: we ran the full detect + per-tx sim + reconcile pipeline and
        // logged the opportunity above, but in sim-only mode we NEVER send —
        // this is the switch that stops the network flood while we validate that
        // our simulation matches reality. Sending is an explicit opt-in.
        if self.params.sim_only {
            self.nosend_sim_only.fetch_add(1, Ordering::Relaxed);
            return;
        }

        // De-dupe: don't blast the same pool with identical txs while an earlier
        // one is still unconfirmed. Kept SMALL and configurable (send_dedup_ms,
        // 0 = off) so multiple distinct opportunities in one block can each send.
        if self.params.send_dedup_ms > 0 {
            if let Some(prev) = self.last_sent.get(&key) {
                if prev.elapsed() < Duration::from_millis(self.params.send_dedup_ms) {
                    self.nosend_dedup.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            }
        }

        let token = pair.token_mint.to_string();
        let buy_label = self.label_for(buy_kind);
        let sell_label = self.label_for(sell_kind);

        // ── RAM route cache: reuse the saved whole-route instruction ─────────
        // If we already captured this (pair, direction) route from Metis, just
        // rewrite the input amount and the on-chain output floor (which already
        // includes network fee + Jito tip) inside the instruction data and send
        // — no Metis round-trip at all.
        let cache_key = (key, buy_kind == DexKind::PumpFunAmm);
        let mut from_cache = false;
        let cached_ixs = if !self.params.instructions_pp {
            None
        } else {
            self.route_cache.get(&cache_key).and_then(|c| {
            crate::template_cache::patch_amounts_b64(
                &c.swap_ixs.swap_instruction.data,
                c.in_off,
                c.out_off,
                amount_in,
                floor,
            )
            .map(|data| {
                let mut ixs = c.swap_ixs.clone();
                ixs.swap_instruction.data = data;
                ixs
            })
            })
        };

        let mut swap_ixs = if let Some(ixs) = cached_ixs {
            from_cache = true;
            self.route_cache_hits.fetch_add(1, Ordering::Relaxed);
            ixs
        } else {
            self.route_cache_misses.fetch_add(1, Ordering::Relaxed);

            // Leg 1: forced buy on `buy_kind` (WSOL → token).
            let q1 = match self
                .metis
                .get_quote_forced(WSOL_MINT, &token, amount_in, buy_label, self.params.metis_max_accounts)
                .await
            {
                Ok(q) => q,
                Err(e) => {
                    self.nosend_quote_fail.fetch_add(1, Ordering::Relaxed);
                    self.handle_load_failure(pair, key, "buy", buy_label, &e.to_string()).await;
                    return;
                }
            };
            let token_amt: u64 = match q1.out_amount.parse().ok().filter(|&v| v > 0) {
                Some(v) => v,
                None => {
                    self.nosend_quote_fail.fetch_add(1, Ordering::Relaxed);
                    crate::errlog::log(
                        "not-sent",
                        &format!("token={token} reason=buy-quote-zero-out"),
                    );
                    return;
                }
            };

            // Leg 2: forced sell on `sell_kind` (token → WSOL).
            let q2 = match self
                .metis
                .get_quote_forced(&token, WSOL_MINT, token_amt, sell_label, self.params.metis_max_accounts)
                .await
            {
                Ok(q) => q,
                Err(e) => {
                    self.nosend_quote_fail.fetch_add(1, Ordering::Relaxed);
                    self.handle_load_failure(pair, key, "sell", sell_label, &e.to_string()).await;
                    return;
                }
            };
            // Both legs routed — clear any load-failure strikes.
            self.load_fail.remove(&key);
            self.note_route_success(key);

            // Set the on-chain floor to exactly the input (break-even) regardless of
            // what Metis quoted (our data is ahead of Metis).
            let merged = match MetisClient::merge_quotes(&q1, &q2, floor) {
                Ok(m) => m,
                Err(e) => {
                    self.nosend_build_fail.fetch_add(1, Ordering::Relaxed);
                    warn!(error = %e, "merge_quotes failed");
                    crate::errlog::log("not-sent", &format!("token={token} reason=merge-fail err={e}"));
                    return;
                }
            };

            let swap_ixs = match self
                .metis
                .get_swap_instructions(&self.user_pubkey, &merged, self.params.use_shared_accounts)
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    self.nosend_swapix_fail.fetch_add(1, Ordering::Relaxed);
                    warn!(?e, "swap_instructions failed for forced arb");
                    crate::errlog::log(
                        "not-sent",
                        &format!("token={token} reason=swap-instructions-fail err={e:?}"),
                    );
                    return;
                }
            };

            // ammKey guard: with a FORCED dex label Metis can route through a
            // DIFFERENT pool of that same dex (same venue, different ammKey). We
            // only cache — and later reuse — an instruction that genuinely
            // trades OUR two pools, so a cached entry keyed by (pump, meteora)
            // can never serve a route through some other pool.
            let route_accounts: std::collections::HashSet<solana_sdk::pubkey::Pubkey> =
                harvest_accounts(&swap_ixs).into_iter().collect();
            let ammkeys_match = route_accounts.contains(&pair.pump.pool)
                && route_accounts.contains(&pair.meteora.pool);
            if !ammkeys_match {
                crate::errlog::log(
                    "not-sent",
                    &format!(
                        "token={token} reason=ammkey-mismatch (Metis routed through a \
                         different pool) pump={} meteora={}",
                        pair.pump.pool, pair.meteora.pool
                    ),
                );
                self.nosend_build_fail.fetch_add(1, Ordering::Relaxed);
                return;
            }

            // Capture the whole route for next time: find where the two amounts
            // live inside the instruction's Borsh data (this request used
            // in=amount_in, quoted_out=floor, so we can search for them). If the
            // layout doesn't validate we just keep going through Metis.
            if self.params.instructions_pp {
            if let Ok(raw) = base64::Engine::decode(
                &base64::engine::general_purpose::STANDARD,
                &swap_ixs.swap_instruction.data,
            ) {
                if let Some((in_off, out_off)) =
                    crate::template_cache::discover_offsets(&raw, amount_in, floor)
                {
                    self.route_cache.insert(
                        cache_key,
                        CachedRoute {
                            swap_ixs: swap_ixs.clone(),
                            in_off,
                            out_off,
                            coin_creator: self.pool_state.pump_coin_creator(&pair.pump.pool),
                        },
                    );
                    debug!(token = %token, "route instructions cached in RAM");
                }
            }
            }
            swap_ixs
        };

        // ── Pump coin_creator vault freshness ────────────────────────────────
        // The Pump AMM `coin_creator_vault_ata` / `coin_creator_vault_authority`
        // accounts are PDAs of the pool's `coin_creator`, which pump can set or
        // rotate AFTER creation (default zero → real creator, populated by its
        // backend). A cached route — or a Metis quote built off a stale pool
        // snapshot — carries the OLD vault and the swap reverts. Re-derive from
        // the LIVE `coin_creator` (read from the subscribed pool account) and
        // rewrite any stale vault account in place. No-op when nothing is stale.
        if let Some(current_creator) = self.pool_state.pump_coin_creator(&pair.pump.pool) {
            let cached_creator = if from_cache {
                self.route_cache.get(&cache_key).and_then(|c| c.coin_creator)
            } else {
                None
            };
            let patched = patch_pump_creator_vault(
                &mut swap_ixs.swap_instruction,
                &current_creator,
                cached_creator.as_ref(),
            );
            if patched {
                debug!(
                    token = %token, pool = %pair.pump.pool, creator = %current_creator,
                    "patched stale pump coin_creator vault accounts"
                );
            }
        }

        // Teach our self-learning ALT every account in this route so subsequent
        // txs for this pool compress fully. Cheap: only unseen pubkeys enqueue.
        if let Some(ab) = &self.alt_builder {
            ab.note(harvest_accounts(&swap_ixs));
        }
        let owned_alts = self
            .alt_builder
            .as_ref()
            .map(|ab| ab.tables())
            .unwrap_or_default();

        let recent_blockhash = self.blockhash_cache.get();
        let keypair = self.trading_keypair.clone();
        let alt = self.alt_cache.clone();
        let rpc = self.rpc_client.clone();
        let cu = self.params.cu_limit;
        // Pick the best public ALTs to compress THIS tx: take the REAL account
        // list the Metis instruction carries and select, from the global library
        // harvested off shreds, the best-covering tables — at most one per leg
        // (MAX_ALTS_PER_TX = 2), never the same table twice. Fallback (cold
        // library): a free provider ALT and any ALT recorded on the pool itself.
        let route_set: std::collections::HashSet<solana_sdk::pubkey::Pubkey> =
            harvest_accounts(&swap_ixs).into_iter().collect();
        let mut extra_alts: Vec<solana_sdk::pubkey::Pubkey> =
            self.alt_registry.select(&route_set, MAX_ALTS_PER_TX);
        if extra_alts.is_empty() {
            if let Some(f) = &self.alt_fetcher {
                extra_alts = f.tables_for(&key.0);
            }
            for a in [pair.pump.alt, pair.meteora.alt].into_iter().flatten() {
                if !extra_alts.contains(&a) {
                    extra_alts.push(a);
                }
            }
        }

        // ── Direct-to-RPC send (default): no Jito, no tip, no rate limit ──
        if self.params.direct_send {
            let prio = self.params.direct_priority_fee_microlamports;
            let data_limit = self.params.loaded_accounts_data_limit;
            // Build the tx. If it's over the 1232-byte cap AND we included the
            // optional SetLoadedAccountsDataSizeLimit instruction, rebuild WITHOUT
            // it (saves ~10 bytes) rather than dropping a profitable opportunity.
            let tx = match tokio::task::spawn_blocking(move || {
                let build = |dl: u32| {
                    transaction::build_direct_transaction(
                        &swap_ixs,
                        &keypair,
                        cu,
                        prio,
                        dl,
                        recent_blockhash,
                        &alt,
                        &rpc,
                        &extra_alts,
                        &owned_alts,
                    )
                };
                let tx = build(data_limit)?;
                if transaction::serialized_len(&tx) <= 1232 || data_limit == 0 {
                    return Ok::<_, anyhow::Error>(tx);
                }
                // Too big with the data-size ix — try again without it.
                build(0)
            })
            .await
            {
                Ok(Ok(tx)) => tx,
                _ => {
                    if from_cache {
                        self.route_cache.remove(&cache_key);
                    }
                    self.nosend_build_fail.fetch_add(1, Ordering::Relaxed);
                    warn!("build_direct_transaction failed");
                    crate::errlog::log("not-sent", &format!("token={token} reason=build-fail"));
                    return;
                }
            };

            let locks = transaction::account_lock_count(&tx);
            if locks > 64 {
                if from_cache {
                    self.route_cache.remove(&cache_key);
                }
                self.nosend_too_locks.fetch_add(1, Ordering::Relaxed);
                warn!(locks, "direct arb tx exceeds 64 account locks, dropping");
                crate::errlog::log(
                    "not-sent",
                    &format!("token={token} reason=too-many-account-locks locks={locks}"),
                );
                return;
            }
            // Final size gate (Solana caps at 1232 raw bytes).
            let raw = transaction::serialized_len(&tx);
            if raw > 1232 {
                if from_cache {
                    self.route_cache.remove(&cache_key);
                }
                self.nosend_too_large.fetch_add(1, Ordering::Relaxed);
                let alts_used = match &tx.message {
                    solana_sdk::message::VersionedMessage::V0(m) => m.address_table_lookups.len(),
                    _ => 0,
                };
                warn!(
                    bytes = raw, locks, alts_used,
                    "direct arb tx still too large ({raw} > 1232) after compression — dropping"
                );
                crate::errlog::log(
                    "not-sent",
                    &format!(
                        "token={token} reason=tx-too-large bytes={raw} locks={locks} \
                         alts_used={alts_used} pump_alt={:?} met_alt={:?}",
                        pair.pump.alt, pair.meteora.alt
                    ),
                );
                return;
            }

            // Last-moment freshness gate: if the pool moved against us while we
            // fetched quotes/built the tx (a competitor's trade landed), this tx
            // would revert — skip it instead of sending a doomed tx.
            // Preempt: blocks ONLY when the Meteora gRPC state ADVANCED since
            // we computed AND the re-priced trade is no longer profitable. An
            // unchanged pool always sends. `disable_preempt = true` turns it off.
            if !self.params.disable_preempt && !self.still_profitable(&recheck) {
                self.nosend_preempted.fetch_add(1, Ordering::Relaxed);
                crate::errlog::log(
                    "not-sent",
                    &format!("token={token} reason=preempted (meteora state moved, no longer profitable)"),
                );
                return;
            }

            // Clone the exact tx for the post-send RPC sim-compare diagnostic
            // (only when enabled — otherwise this is a cheap no-op branch).
            let sim_tx = if self.params.rpc_sim_compare {
                Some(tx.clone())
            } else {
                None
            };
            let rpc2 = self.rpc_client.clone();
            let send_res = tokio::task::spawn_blocking(move || {
                use solana_client::rpc_config::RpcSendTransactionConfig;
                rpc2.send_transaction_with_config(
                    &tx,
                    RpcSendTransactionConfig {
                        skip_preflight: true,
                        max_retries: Some(0),
                        ..Default::default()
                    },
                )
            })
            .await;
            match send_res {
                Ok(Ok(sig)) => {
                    self.sent.fetch_add(1, Ordering::Relaxed);
                    self.last_sent.insert(key, Instant::now());
                    // Per-leg decision snapshot next to the tx hash — compare
                    // each leg's predicted numbers (input/output/fee/reserves)
                    // against the on-chain result to localize any calc error.
                    info!(
                        signature = %sig,
                        sent_slot = self.pool_state.slot(),
                        input = amount_in,
                        leg1_venue = legs[0].venue,
                        leg1_in = legs[0].input,
                        leg1_predicted_out = legs[0].predicted_out,
                        leg1_fee_bps = legs[0].fee_bps,
                        leg1_reserve_token = legs[0].reserve_token,
                        leg1_reserve_wsol = legs[0].reserve_wsol,
                        leg2_venue = legs[1].venue,
                        leg2_in = legs[1].input,
                        leg2_predicted_out = legs[1].predicted_out,
                        leg2_fee_bps = legs[1].fee_bps,
                        leg2_reserve_token = legs[1].reserve_token,
                        leg2_reserve_wsol = legs[1].reserve_wsol,
                        "shred-arb tx sent (direct)"
                    );
                    crate::errlog::log(
                        "sent",
                        &format!(
                            "token={token} sig={sig} sent_slot={} input={amount_in} \
                             leg1[{} in={} out={} fee_bps={} reserve_token={} reserve_wsol={}] \
                             leg2[{} in={} out={} fee_bps={} reserve_token={} reserve_wsol={}]",
                            self.pool_state.slot(),
                            legs[0].venue, legs[0].input, legs[0].predicted_out,
                            legs[0].fee_bps, legs[0].reserve_token, legs[0].reserve_wsol,
                            legs[1].venue, legs[1].input, legs[1].predicted_out,
                            legs[1].fee_bps, legs[1].reserve_token, legs[1].reserve_wsol,
                        ),
                    );
                    if let Some(t) = sim_tx {
                        self.spawn_sim_compare(t, predicted_out, amount_in, token.clone());
                    }
                    // Resolve the on-chain fate so EVERY sent tx is accounted for
                    // (landed_ok / reverted / dropped / unknown always sums to
                    // sent). Uses history-searching status lookups + retries so a
                    // tx that landed slightly late isn't miscounted as dropped.
                    let rpc3 = self.rpc_client.clone();
                    let stats = self.sent_stats.clone();
                    let token_l = token.clone();
                    let delay = self.params.status_check_delay_secs;
                    tokio::spawn(async move {
                        resolve_fate(rpc3, stats, sig, token_l, predicted_out, delay).await;
                    });
                }
                Ok(Err(e)) => {
                    self.nosend_send_err.fetch_add(1, Ordering::Relaxed);
                    warn!(error = %e, "direct send failed");
                    crate::errlog::log("not-sent", &format!("token={token} reason=rpc-send-err err={e}"));
                }
                Err(e) => {
                    self.nosend_send_err.fetch_add(1, Ordering::Relaxed);
                    warn!(error = %e, "direct send task join failed");
                    crate::errlog::log("not-sent", &format!("token={token} reason=send-join-err err={e}"));
                }
            }
            return;
        }

        // ── Jito bundle path (direct_send = false): REST 5/s + gRPC 5/s ──
        // Rate-limited: try the REST limiter first, else the gRPC limiter; if both
        // are full this second, drop (rate-limited).
        let use_grpc = if self.jito_limiter.lock().unwrap().try_acquire() {
            false
        } else if self
            .jito_grpc_limiter
            .as_ref()
            .map(|g| g.lock().unwrap().try_acquire())
            .unwrap_or(false)
        {
            true
        } else {
            self.nosend_send_err.fetch_add(1, Ordering::Relaxed);
            crate::errlog::log("not-sent", &format!("token={token} reason=jito-rate-limited"));
            return;
        };

        // `tip` is the dynamic tip computed in assess (min + profit share).
        let data_limit = self.params.loaded_accounts_data_limit;
        let cu_price = self.params.compute_unit_price_microlamports;
        let tx = match tokio::task::spawn_blocking(move || {
            transaction::build_arb_transaction(
                &swap_ixs,
                &keypair,
                tip,
                cu,
                data_limit,
                cu_price,
                recent_blockhash,
                &alt,
                &rpc,
                &extra_alts,
                &owned_alts,
            )
        })
        .await
        {
            Ok(Ok(tx)) => tx,
            _ => {
                if from_cache {
                    self.route_cache.remove(&cache_key);
                }
                self.nosend_build_fail.fetch_add(1, Ordering::Relaxed);
                warn!("build_arb_transaction failed");
                crate::errlog::log("not-sent", &format!("token={token} reason=build-fail-jito"));
                return;
            }
        };

        if transaction::account_lock_count(&tx) > 64 {
            if from_cache {
                self.route_cache.remove(&cache_key);
            }
            self.nosend_too_locks.fetch_add(1, Ordering::Relaxed);
            warn!("forced arb tx exceeds 64 account locks, dropping");
            return;
        }
        let raw = transaction::serialized_len(&tx);
        if raw > 1232 {
            if from_cache {
                self.route_cache.remove(&cache_key);
            }
            self.nosend_too_large.fetch_add(1, Ordering::Relaxed);
            let alts_used = match &tx.message {
                solana_sdk::message::VersionedMessage::V0(m) => m.address_table_lookups.len(),
                _ => 0,
            };
            warn!(bytes = raw, alts_used, "jito arb tx too large — dropping");
            crate::errlog::log(
                "not-sent",
                &format!("token={token} reason=tx-too-large-jito bytes={raw} alts_used={alts_used}"),
            );
            return;
        }

        // Last-moment freshness gate (same as the direct path): skip if the pool
        // moved against us during the compute window (would revert).
        if !self.params.disable_preempt && !self.still_profitable(&recheck) {
            self.nosend_preempted.fetch_add(1, Ordering::Relaxed);
            crate::errlog::log(
                "not-sent",
                &format!("token={token} reason=preempted (meteora state moved, no longer profitable)"),
            );
            return;
        }

        // Signature of the swap tx — used to check whether the bundle actually
        // landed (Jito accepting a bundle ≠ it landing; it may lose the auction
        // or its tx may revert). Without this the Jito path is blind.
        let sig = tx.signatures.first().copied();
        let sim_tx = if self.params.rpc_sim_compare {
            Some(tx.clone())
        } else {
            None
        };
        let result = if use_grpc {
            match &self.jito_grpc {
                Some(g) => g.send_bundle(&tx).await,
                None => self.jito.send_bundle(&tx).await,
            }
        } else {
            self.jito.send_bundle(&tx).await
        };

        match result {
            Ok(id) => {
                self.sent.fetch_add(1, Ordering::Relaxed);
                self.last_sent.insert(key, Instant::now());
                info!(
                    bundle = %id,
                    input = amount_in,
                    tip,
                    sent_slot = self.pool_state.slot(),
                    via = if use_grpc { "grpc" } else { "rest" },
                    leg1_venue = legs[0].venue,
                    leg1_in = legs[0].input,
                    leg1_predicted_out = legs[0].predicted_out,
                    leg1_fee_bps = legs[0].fee_bps,
                    leg1_reserve_token = legs[0].reserve_token,
                    leg1_reserve_wsol = legs[0].reserve_wsol,
                    leg2_venue = legs[1].venue,
                    leg2_in = legs[1].input,
                    leg2_predicted_out = legs[1].predicted_out,
                    leg2_fee_bps = legs[1].fee_bps,
                    leg2_reserve_token = legs[1].reserve_token,
                    leg2_reserve_wsol = legs[1].reserve_wsol,
                    "shred-arb bundle sent"
                );
                crate::errlog::log(
                    "sent",
                    &format!(
                        "token={token} bundle={id} sent_slot={} input={amount_in} \
                         leg1[{} in={} out={} fee_bps={} reserve_token={} reserve_wsol={}] \
                         leg2[{} in={} out={} fee_bps={} reserve_token={} reserve_wsol={}]",
                        self.pool_state.slot(),
                        legs[0].venue, legs[0].input, legs[0].predicted_out,
                        legs[0].fee_bps, legs[0].reserve_token, legs[0].reserve_wsol,
                        legs[1].venue, legs[1].input, legs[1].predicted_out,
                        legs[1].fee_bps, legs[1].reserve_token, legs[1].reserve_wsol,
                    ),
                );
                if let Some(t) = sim_tx {
                    self.spawn_sim_compare(t, predicted_out, amount_in, token.clone());
                }
                // Track on-chain fate so we KNOW: landed_ok / reverted / dropped
                // (auction-lost or never landed). A dropped bundle costs nothing.
                if let Some(sig) = sig {
                    let rpc3 = self.rpc_client.clone();
                    let stats = self.sent_stats.clone();
                    let token_l = token.clone();
                    let delay = self.params.status_check_delay_secs;
                    tokio::spawn(async move {
                        resolve_fate(rpc3, stats, sig, token_l, predicted_out, delay).await;
                    });
                }
            }
            Err(e) => {
                self.nosend_send_err.fetch_add(1, Ordering::Relaxed);
                warn!(error = %e, "shred-arb bundle send failed");
                crate::errlog::log("not-sent", &format!("token={token} reason=jito-send-fail err={e}"));
            }
        }
    }

    /// Final gate: using the LATEST cached Meteora state, would the trade still
    /// clear `min_out`? Returns false if a competitor moved the pool against us
    /// during the compute window (the tx would revert). If we have no fresh
    /// Meteora reading we do NOT block (return true) — better to try than to
    /// stall on a cache gap.
    fn still_profitable(&self, r: &Recheck) -> bool {
        // Only the Meteora pool advancing on gRPC can invalidate the trade (the
        // Pump side is held at our predicted post-trade reserves). If the pool
        // account has NOT changed since we computed, nothing moved → send as-is.
        let cur_slot = self.pool_state.last_update_slot(&r.met_pool).unwrap_or(0);
        if cur_slot <= r.met_slot_at_calc {
            return true;
        }
        // The pool moved — recompute against the fresh state and only block if
        // the trade is genuinely no longer profitable.
        let met = match self.pool_state.meteora_pool(&r.met_pool, r.fallback_fee, self.params.meteora_fee_worst_case) {
            Some(m) => m,
            None => return true,
        };
        // Same transfer-fee model as the assess-time eval: the intermediate token
        // is taxed twice (out of the buy pool, into the sell pool).
        let tfee_bps = r.tfee_bps as u128;
        let apply_tfee = |amt: u64| -> u64 {
            if tfee_bps == 0 {
                return amt;
            }
            let fee = ((amt as u128) * tfee_bps + 9_999) / 10_000;
            (amt as u128).saturating_sub(fee) as u64
        };
        let out = if r.buy_on_pump {
            // Orientation-exact pump buy leg (inverted pool = raw sell, flipped).
            let base = if r.pump_token_is_base {
                r.pump_after.quote_buy(r.amount_in)
            } else {
                r.pump_after.flipped().quote_sell(r.amount_in)
            };
            if base == 0 {
                return false;
            }
            let tok = apply_tfee(apply_tfee(base));
            met.sell_token_for_wsol(tok, r.token_is_a)
        } else {
            match met.buy_token_with_wsol(r.amount_in, r.token_is_a) {
                Some(base) if base > 0 => {
                    let tok = apply_tfee(apply_tfee(base));
                    let o = if r.pump_token_is_base {
                        r.pump_after.quote_sell(tok)
                    } else {
                        r.pump_after.flipped().quote_buy(tok)
                    };
                    Some(o)
                }
                _ => None,
            }
        };
        matches!(out, Some(o) if o >= r.min_out)
    }

    /// The configurable Metis `dexes=` label for a venue.
    fn label_for(&self, kind: DexKind) -> &str {
        match kind {
            DexKind::PumpFunAmm => &self.params.pump_label,
            DexKind::MeteoraDammV2 => &self.params.meteora_label,
        }
    }

    /// A forced-quote "No routes found" means one of the two legs never loaded
    /// into Metis. Re-add BOTH legs (add-market is idempotent) and count the
    /// strike; after `LOAD_RETRY_LIMIT` consecutive failures disable the pool so
    /// it stops spamming No-routes, recording why to the /root/g error file.
    async fn handle_load_failure(
        &self,
        pair: &ArbPair,
        key: PairKey,
        leg: &str,
        venue: &str,
        err: &str,
    ) {
        let n = {
            let mut e = self.load_fail.entry(key).or_insert(0);
            *e += 1;
            *e
        };
        let limit = if self.params.metis_load_retry_limit == 0 {
            LOAD_RETRY_LIMIT_DEFAULT
        } else {
            self.params.metis_load_retry_limit
        };
        // Re-add BOTH markets so a half-loaded pair gets its missing leg.
        self.readd_market(pair, DexKind::PumpFunAmm).await;
        self.readd_market(pair, DexKind::MeteoraDammV2).await;
        if n >= limit {
            self.disabled.insert(key);
            self.load_fail.remove(&key);
            warn!(pool = %key.0, meteora = %key.1, token = %pair.token_mint, attempts = n, "disabling pair — Metis never loaded both legs");
            crate::errlog::log(
                "error",
                &format!(
                    "token={} reason=disabled-after-{}-load-failures leg={leg} venue={venue} \
                     pump_pool={} meteora_pool={} last_err={err}",
                    pair.token_mint, n, pair.pump.pool, pair.meteora.pool
                ),
            );
        } else {
            crate::errlog::log(
                "not-sent",
                &format!(
                    "token={} reason={leg}-quote-fail attempt={n}/{limit} venue={venue} err={err}",
                    pair.token_mint
                ),
            );
        }
    }

    /// Re-register the relevant leg's pool with Metis after a "No routes"
    /// failure — the market may not have loaded on the first add, or Metis
    /// restarted. Cheap and idempotent; the route-failure counter still drops
    /// the pool if it keeps failing.
    async fn readd_market(&self, pair: &ArbPair, kind: DexKind) {
        let (info, owner) = match kind {
            DexKind::PumpFunAmm => (&pair.pump, crate::dex_ids::PUMPFUN_AMM_PROGRAM),
            DexKind::MeteoraDammV2 => (&pair.meteora, crate::dex_ids::METEORA_DAMM_V2_PROGRAM),
        };
        let alt = info.alt.map(|a| a.to_string());
        if let Err(e) = self
            .metis
            .add_market(&info.pool.to_string(), owner, alt.as_deref())
            .await
        {
            debug!(pool = %info.pool, error = %e, "re-add market failed");
        }
    }

    /// Reset the route-failure strike count for a pair after a successful route.
    fn note_route_success(&self, key: PairKey) {
        if self.route_fail.contains_key(&key) {
            self.route_fail.remove(&key);
        }
    }

    /// Periodic fee-audit log: for a sample of tracked pairs, print the decoded
    /// Meteora fee breakdown (base scheduler + dynamic, stored vs worst-case)
    /// and the Pump.fun tiered fee, so the operator can hand-verify the fees
    /// against a real on-chain swap on those pools. Off when `secs == 0`.
    pub fn spawn_fee_audit(self: Arc<Self>, secs: u64) {
        if secs == 0 {
            return;
        }
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(secs.max(10)));
            ticker.tick().await; // skip immediate first tick
            loop {
                ticker.tick().await;
                let pairs: Vec<ArbPair> = self
                    .registry
                    .iter()
                    .flat_map(|e| e.value().clone())
                    .take(200)
                    .collect();
                let mut printed = 0usize;
                for pair in pairs {
                    // Only pairs whose Meteora fee we can currently decode.
                    let Some(fb) = self.pool_state.meteora_fee_breakdown(&pair.meteora.pool)
                    else {
                        continue;
                    };
                    let pump = self.pool_state.pump_pool(
                        &pair.pump.pool,
                        &pair.pump.token_vault(),
                        &pair.pump.wsol_vault(),
                        &pair.token_mint,
                    );
                    let pump_fee_bps = pump.map(|p| p.total_fee_bps).unwrap_or(0);
                    let supply = self.pool_state.spl_mint_supply(&pair.token_mint).unwrap_or(0);
                    // The exact shred tx whose fee this line reflects (empty if we
                    // haven't seen a trade on this Pump pool yet this run).
                    let shred_sig = self
                        .last_shred_sig
                        .get(&pair.pump.pool)
                        .map(|s| s.value().to_string())
                        .unwrap_or_else(|| "none-yet".to_string());
                    eprintln!(
                        "[fee-audit] shred_tx={} | PUMP pool={} fee={}bps supply={} | METEORA pool={} \
                         cliff={:.1}bps base={:.1}bps dyn_now={:.1}bps dyn_worst={:.1}bps \
                         TOTAL now={:.1}bps worst={:.1}bps dyn_enabled={} | meteora_using={}",
                        shred_sig,
                        pair.pump.pool,
                        pump_fee_bps,
                        supply,
                        pair.meteora.pool,
                        fb.cliff_bps,
                        fb.base_bps,
                        fb.dyn_stored_bps,
                        fb.dyn_worst_bps,
                        fb.total_stored_bps,
                        fb.total_worst_bps,
                        fb.dynamic_enabled,
                        if self.params.meteora_fee_worst_case { "WORST" } else { "now" },
                    );
                    printed += 1;
                    if printed >= 3 {
                        break; // cap the log volume per tick (operator asked for 3)
                    }
                }
            }
        });
    }

    pub fn spawn_reporter(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(30));
            loop {
                ticker.tick().await;
                // Concise, transaction-focused status. Only the numbers the
                // operator asked to see: what was profitable, what actually SENT,
                // where the sent ones ended up on-chain, and — crucially — WHY the
                // profitable ones that did NOT send were held back. No shred/eval
                // spam (those go to the /root/g error file / debug logs).
                let ss = &self.sent_stats;
                let landed_ok = ss.landed_ok.load(Ordering::Relaxed);
                let reverted = ss.landed_err.load(Ordering::Relaxed);
                let dropped = ss.dropped.load(Ordering::Relaxed);
                let unknown = ss.unknown.load(Ordering::Relaxed);
                let (
                    sim_recorded,
                    sim_checks,
                    sim_matched,
                    sim_mismatched,
                    sim_value_mismatched,
                    sim_unsimulated,
                    sim_pending,
                ) = self.sim_ledger.snapshot();
                eprintln!(
                    "\n[shred-arb 30s] watching_pools={}\n\
                     ENGINE  : evaluated={} profitable={} not_profitable={} (uncrossable={}) | arb_ignored={} burned_signals={} | skip[min_trig={} no_meteora_state={} stale_met={} bad_price={} thin_pool={} implausible={}] | opaque[seen={} dirty_skip={}]\n\
                     TX      : sent={} | on-chain[ok={} reverted={} dropped={} unknown={}]\n\
                     NOT-SENT: sim_only={} dedup={} quote_fail={} swapix_fail={} build_fail={} too_large={} too_locks={} send_err={} preempted={}\n\
                     SIM     : recorded={} reconcile_checks={} matched={} mismatched={} value_wrong={} unsimulated={} paused_dark={} skipped_revert={} pending={}\n\
                     ROUTE-RAM: hits={} misses={} cached_routes={}",
                    self.shred_metrics.watched_pools.load(Ordering::Relaxed),
                    self.evaluated.load(Ordering::Relaxed),
                    self.profitable.load(Ordering::Relaxed),
                    self.not_profitable.load(Ordering::Relaxed),
                    self.skip_uncrossable.load(Ordering::Relaxed),
                    self.arb_ignored.load(Ordering::Relaxed),
                    self.skip_stale_signal.load(Ordering::Relaxed),
                    self.skip_min_trigger.load(Ordering::Relaxed),
                    self.skip_no_meteora_state.load(Ordering::Relaxed),
                    self.skip_stale_meteora.load(Ordering::Relaxed),
                    self.skip_bad_price.load(Ordering::Relaxed),
                    self.skip_thin_pool.load(Ordering::Relaxed),
                    self.skip_implausible.load(Ordering::Relaxed),
                    self.opaque_signals.load(Ordering::Relaxed),
                    self.skip_dirty_pump.load(Ordering::Relaxed),
                    self.sent.load(Ordering::Relaxed),
                    landed_ok,
                    reverted,
                    dropped,
                    unknown,
                    self.nosend_sim_only.load(Ordering::Relaxed),
                    self.nosend_dedup.load(Ordering::Relaxed),
                    self.nosend_quote_fail.load(Ordering::Relaxed),
                    self.nosend_swapix_fail.load(Ordering::Relaxed),
                    self.nosend_build_fail.load(Ordering::Relaxed),
                    self.nosend_too_large.load(Ordering::Relaxed),
                    self.nosend_too_locks.load(Ordering::Relaxed),
                    self.nosend_send_err.load(Ordering::Relaxed),
                    self.nosend_preempted.load(Ordering::Relaxed),
                    sim_recorded,
                    sim_checks,
                    sim_matched,
                    sim_mismatched,
                    sim_value_mismatched,
                    sim_unsimulated,
                    self.sim_paused_dark.load(Ordering::Relaxed),
                    self.sim_skipped_revert.load(Ordering::Relaxed),
                    sim_pending,
                    self.route_cache_hits.load(Ordering::Relaxed),
                    self.route_cache_misses.load(Ordering::Relaxed),
                    self.route_cache.len(),
                );
            }
        });
    }
}

/// Resolve and account for a sent tx's on-chain fate. Retries a history-aware
/// status lookup so a tx that lands a little late is classified correctly, and
/// every outcome (ok / reverted / dropped / unknown) is counted — so
/// `sent == landed_ok + reverted + dropped + unknown` always holds and no sent
/// tx silently vanishes from the tally.
async fn resolve_fate(
    rpc: Arc<RpcClient>,
    stats: Arc<SentStats>,
    sig: solana_sdk::signature::Signature,
    token: String,
    predicted_out: u64,
    delay_secs: u64,
) {
    tokio::time::sleep(Duration::from_secs(delay_secs.max(1))).await;
    const RETRIES: usize = 5;
    let mut last_err = false;
    for _attempt in 0..RETRIES {
        let r = {
            let rpc = rpc.clone();
            tokio::task::spawn_blocking(move || rpc.get_signature_statuses_with_history(&[sig])).await
        };
        match r {
            Ok(Ok(resp)) => {
                last_err = false;
                match resp.value.into_iter().next().flatten() {
                    Some(st) => {
                        if let Some(err) = st.err {
                            stats.landed_err.fetch_add(1, Ordering::Relaxed);
                            warn!(%sig, error = ?err, "sent tx LANDED but REVERTED");
                            // Pull the REAL on-chain logs so we can see the exact
                            // program error (e.g. 0x1771 slippage) and the real
                            // amounts, next to OUR predicted output. This is the
                            // ground-truth diagnostic — it only works when the tx
                            // actually lands (i.e. direct_send, not a Jito bundle
                            // that gets silently sim-dropped).
                            let real_logs = fetch_tx_logs(&rpc, sig).await;
                            crate::errlog::log(
                                "lost",
                                &format!(
                                    "token={token} sig={sig} fate=reverted predicted_out={predicted_out} err={err:?} logs=[{real_logs}]"
                                ),
                            );
                            warn!(%sig, predicted_out, logs = %real_logs, "REVERT detail (predicted vs real)");
                        } else {
                            stats.landed_ok.fetch_add(1, Ordering::Relaxed);
                            info!(%sig, "sent tx landed OK ✅");
                        }
                        return;
                    }
                    None => { /* not found yet — give it another poll */ }
                }
            }
            _ => last_err = true,
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    // Exhausted retries.
    if last_err {
        stats.unknown.fetch_add(1, Ordering::Relaxed);
        crate::errlog::log("lost", &format!("token={token} sig={sig} fate=unknown-rpc-error"));
    } else {
        stats.dropped.fetch_add(1, Ordering::Relaxed);
        warn!(%sig, "sent tx NOT FOUND on-chain (dropped/never landed)");
        crate::errlog::log("lost", &format!("token={token} sig={sig} fate=dropped-not-found"));
    }
}

/// Fetch a landed transaction's on-chain log messages (the program's own
/// `sol_log` output), joined into one line. Used on the REVERT path to surface
/// the real error and amounts. Best-effort: returns a short marker on any RPC or
/// decode miss so the caller can always log something.
async fn fetch_tx_logs(rpc: &Arc<RpcClient>, sig: solana_sdk::signature::Signature) -> String {
    use solana_client::rpc_config::RpcTransactionConfig;
    use solana_transaction_status::UiTransactionEncoding;
    let rpc = rpc.clone();
    let res = tokio::task::spawn_blocking(move || {
        rpc.get_transaction_with_config(
            &sig,
            RpcTransactionConfig {
                encoding: Some(UiTransactionEncoding::Base64),
                commitment: Some(solana_sdk::commitment_config::CommitmentConfig::confirmed()),
                max_supported_transaction_version: Some(0),
            },
        )
    })
    .await;
    match res {
        Ok(Ok(tx)) => match tx.transaction.meta.and_then(|m| {
            let l: Option<Vec<String>> = m.log_messages.into();
            l
        }) {
            Some(logs) => logs.join(" | "),
            None => "no-log-messages".to_string(),
        },
        Ok(Err(e)) => format!("get-tx-err:{e}"),
        Err(e) => format!("join-err:{e}"),
    }
}

/// DIAGNOSTIC: re-run the EXACT built tx through the RPC `simulateTransaction`
/// against live chain state, then log OUR predicted output next to the network's
/// REAL output — plus the raw program logs so the per-leg Pump/Meteora amounts
/// and fees can be hand-compared. Zero rounding: the WSOL delta is read straight
/// from the post-simulation token-account balance (lamport-exact).
///
/// Runs in a spawned task off the hot path — it adds NO latency to the send.
/// `replace_recent_blockhash = true` + `sig_verify = false` so an old blockhash
/// or the fact we didn't re-sign never causes a spurious failure; the sim uses
/// the node's freshest state.
async fn sim_compare(
    rpc: Arc<RpcClient>,
    tx: solana_sdk::transaction::VersionedTransaction,
    predicted_out: u64,
    amount_in: u64,
    token: String,
    wsol_ata: solana_sdk::pubkey::Pubkey,
) {
    use solana_account_decoder::{UiAccountData, UiAccountEncoding};
    use solana_client::rpc_config::{
        RpcSimulateTransactionAccountsConfig, RpcSimulateTransactionConfig,
    };

    let cfg = RpcSimulateTransactionConfig {
        sig_verify: false,
        replace_recent_blockhash: true,
        commitment: Some(solana_sdk::commitment_config::CommitmentConfig::processed()),
        accounts: Some(RpcSimulateTransactionAccountsConfig {
            encoding: Some(UiAccountEncoding::Base64),
            addresses: vec![wsol_ata.to_string()],
        }),
        ..Default::default()
    };

    // PRE balance of the WSOL ATA (before the tx). The round-trip spends
    // `amount_in` WSOL out of this ATA and returns the realized output to it, so
    //   post = pre − amount_in + realized_out ⇒ realized_out = post − pre + amount_in.
    // Without subtracting `pre` the "real_out" is just the absolute balance and
    // the delta is meaningless.
    let rpc_pre = rpc.clone();
    let pre_ata = wsol_ata;
    let pre_balance: Option<u64> = tokio::task::spawn_blocking(move || {
        rpc_pre.get_account(&pre_ata).ok().and_then(|a| {
            a.data.get(64..72).map(|s| {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(s);
                u64::from_le_bytes(buf)
            })
        })
    })
    .await
    .ok()
    .flatten();

    let rpc2 = rpc.clone();
    let res =
        tokio::task::spawn_blocking(move || rpc2.simulate_transaction_with_config(&tx, cfg)).await;

    let value = match res {
        Ok(Ok(r)) => r.value,
        Ok(Err(e)) => {
            warn!(%token, error = %e, "sim-compare RPC error");
            return;
        }
        Err(e) => {
            warn!(%token, error = %e, "sim-compare join error");
            return;
        }
    };

    // Post-simulation WSOL balance (lamport-exact), then convert to the REALIZED
    // round-trip output via realized_out = post − pre + amount_in.
    let post_balance: Option<u64> = value
        .accounts
        .as_ref()
        .and_then(|v| v.first().cloned().flatten())
        .and_then(|acc| match acc.data {
            UiAccountData::Binary(b64, UiAccountEncoding::Base64) => {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD.decode(b64).ok()
            }
            _ => None,
        })
        .filter(|d| d.len() >= 72)
        .map(|d| {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&d[64..72]);
            u64::from_le_bytes(buf)
        });
    // realized_out = post − pre + amount_in (needs a known pre balance).
    let real_out: Option<u64> = match (post_balance, pre_balance) {
        (Some(post), Some(pre)) => {
            Some((post as i128 - pre as i128 + amount_in as i128).max(0) as u64)
        }
        _ => None,
    };

    let reverted = value.err.is_some();
    let logs = value
        .logs
        .as_ref()
        .map(|l| l.join(" | "))
        .unwrap_or_else(|| "no-logs".to_string());

    // predicted_out is the round-trip WSOL out our math computed for `amount_in`.
    // real_out is the node's WSOL balance AFTER the same tx ran on live state.
    match real_out {
        Some(real) => {
            let delta = real as i64 - predicted_out as i64;
            let delta_pct = if predicted_out > 0 {
                delta as f64 / predicted_out as f64 * 100.0
            } else {
                0.0
            };
            info!(
                %token,
                input = amount_in,
                predicted_out,
                real_out = real,
                delta,
                delta_pct,
                reverted,
                "SIM-COMPARE (our calc vs network)"
            );
            crate::errlog::log(
                "sim-compare",
                &format!(
                    "token={token} input={amount_in} predicted_out={predicted_out} \
                     real_out={real} delta={delta} delta_pct={delta_pct:.4} \
                     reverted={reverted} logs=[{logs}]"
                ),
            );
        }
        None => {
            // No post balance (usually because the sim reverted before the sell
            // leg settled). The err + logs still show WHERE it diverged.
            warn!(
                %token, predicted_out, reverted,
                err = ?value.err,
                "SIM-COMPARE reverted / no post-balance"
            );
            crate::errlog::log(
                "sim-compare",
                &format!(
                    "token={token} input={amount_in} predicted_out={predicted_out} \
                     real_out=NONE reverted={reverted} err={:?} logs=[{logs}]",
                    value.err
                ),
            );
        }
    }
}

/// Collect every account pubkey referenced by a Metis swap-instructions response
/// (all instruction blocks + their program ids). These are exactly the accounts
/// our tx will reference, so feeding them into the owned ALT lets `try_compile`
/// compress the whole route next time.
fn harvest_accounts(
    s: &crate::metis::SwapInstructionsResponse,
) -> Vec<solana_sdk::pubkey::Pubkey> {
    use std::str::FromStr;
    let mut out = Vec::new();
    let mut push = |s: &str| {
        if let Ok(pk) = solana_sdk::pubkey::Pubkey::from_str(s) {
            out.push(pk);
        }
    };
    let mut visit = |ix: &crate::metis::InstructionData| {
        push(&ix.program_id);
        for a in &ix.accounts {
            push(&a.pubkey);
        }
    };
    for ix in &s.compute_budget_instructions {
        visit(ix);
    }
    for ix in &s.setup_instructions {
        visit(ix);
    }
    visit(&s.swap_instruction);
    if let Some(ix) = &s.cleanup_instruction {
        visit(ix);
    }
    out
}

/// Derive the Pump.fun AMM `coin_creator_vault_authority` PDA and its WSOL ATA
/// (`coin_creator_vault_ata`) for a given pool `coin_creator`, exactly as the
/// on-chain program does (IDL `pump_amm.json`):
///   authority = PDA([b"creator_vault", coin_creator], PUMP_AMM_PROGRAM)
///   ata       = ATA(authority, quote_mint=WSOL, SPL Token program)
fn pump_creator_vault(
    coin_creator: &solana_sdk::pubkey::Pubkey,
) -> (solana_sdk::pubkey::Pubkey, solana_sdk::pubkey::Pubkey) {
    let program = crate::dex_ids::pumpfun_program();
    let (authority, _) = solana_sdk::pubkey::Pubkey::find_program_address(
        &[b"creator_vault", coin_creator.as_ref()],
        &program,
    );
    // WSOL is a classic SPL token, so the vault ATA lives under the SPL Token
    // program (Tokenkeg…), which is what the pool passes as `quote_token_program`.
    let spl_token = solana_sdk::pubkey::Pubkey::from_str_const(
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    );
    let ata = spl_associated_token_account::get_associated_token_address_with_program_id(
        &authority,
        &crate::dex_ids::wsol_mint(),
        &spl_token,
    );
    (authority, ata)
}

/// Rewrite the Pump `coin_creator_vault_ata` / `coin_creator_vault_authority`
/// accounts in a (possibly cached or Metis-stale) swap instruction so they match
/// the pool's CURRENT `coin_creator`. Pump can rotate `coin_creator` after pool
/// creation (default zero → real creator), which moves both PDAs; a frozen
/// instruction then carries the OLD vault and the swap reverts. We re-derive the
/// correct pair and swap out any account that equals a KNOWN-stale vault
/// (derived from the default/zero creator and from whatever creator was current
/// when the route was cached). A no-op when nothing is stale.
fn patch_pump_creator_vault(
    ix: &mut crate::metis::InstructionData,
    current_creator: &solana_sdk::pubkey::Pubkey,
    cached_creator: Option<&solana_sdk::pubkey::Pubkey>,
) -> bool {
    let (new_auth, new_ata) = pump_creator_vault(current_creator);
    let new_auth_s = new_auth.to_string();
    let new_ata_s = new_ata.to_string();
    // Stale candidates: the zero/default creator (the pre-population value) and
    // the creator captured when this route was cached, if different.
    let mut stale: Vec<solana_sdk::pubkey::Pubkey> =
        vec![solana_sdk::pubkey::Pubkey::default()];
    if let Some(c) = cached_creator {
        stale.push(*c);
    }
    let mut patched = false;
    for old in stale {
        if old == *current_creator {
            continue;
        }
        let (old_auth, old_ata) = pump_creator_vault(&old);
        let old_auth_s = old_auth.to_string();
        let old_ata_s = old_ata.to_string();
        for a in ix.accounts.iter_mut() {
            if a.pubkey == old_auth_s {
                a.pubkey = new_auth_s.clone();
                patched = true;
            } else if a.pubkey == old_ata_s {
                a.pubkey = new_ata_s.clone();
                patched = true;
            }
        }
    }
    patched
}

fn buy_kind_label(k: DexKind) -> &'static str {
    match k {
        DexKind::PumpFunAmm => "PumpFun",
        DexKind::MeteoraDammV2 => "Meteora",
    }
}

/// Find the input that maximizes net profit `eval(x) - x - required_extra`.
///
/// The objective is not guaranteed unimodal (the swap can return `None` for
/// oversized trades that cross a range bound), so a plain ternary search can
/// get stuck. We instead sweep a GEOMETRIC grid across `[lo, hi]` — which is
/// dense at the small sizes these low-liquidity pools actually want — then
/// refine linearly around the best grid point. Returns `(best_x, best_net)`.
fn optimize_size<F: Fn(u64) -> Option<u64>>(
    lo: u64,
    hi: u64,
    required_extra: u64,
    eval: &F,
) -> (u64, i128) {
    let net = |x: u64| -> i128 {
        match eval(x) {
            Some(out) => out as i128 - x as i128 - required_extra as i128,
            None => i128::MIN,
        }
    };
    let lo = lo.max(1);
    if hi <= lo {
        return (lo, net(lo));
    }

    // Geometric sweep.
    const STEPS: usize = 240;
    let lo_f = lo as f64;
    let ratio = (hi as f64 / lo_f).powf(1.0 / STEPS as f64);
    let mut best_x = lo;
    let mut best_net = net(lo);
    let mut xf = lo_f;
    for _ in 0..=STEPS {
        let x = xf as u64;
        let n = net(x);
        if n > best_net {
            best_net = n;
            best_x = x;
        }
        xf *= ratio;
    }

    // EXACT integer ternary search inside the bracket around the best grid
    // point. The basin around a valid maximum is unimodal (oversized trades
    // read as i128::MIN and push the bracket back toward the valid side), so
    // this converges to the single best lamport — no snapping to a coarse grid.
    let span = (best_x / 10).max(2);
    let mut a = best_x.saturating_sub(span).max(lo);
    let mut b = best_x.saturating_add(span).min(hi);
    while b - a > 2 {
        let m1 = a + (b - a) / 3;
        let m2 = b - (b - a) / 3;
        if net(m1) < net(m2) {
            a = m1 + 1;
        } else {
            b = m2;
        }
    }
    // Final exact scan of the tiny residual window (≤3 points).
    let mut x = a;
    while x <= b {
        let n = net(x);
        if n > best_net {
            best_net = n;
            best_x = x;
        }
        x += 1;
    }
    (best_x, best_net)
}
