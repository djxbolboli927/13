//! Jito ShredStream consumer — the fast trigger for the arb strategy.
//!
//! We connect to a local `jito-shredstream-proxy` gRPC surface (the proxy holds
//! the whitelisted keypair, deshreds the UDP stream, and serves reconstructed
//! entries). ShredStream has NO server-side filter, so we receive the whole
//! cluster's entries and filter client-side for swaps that invoke the Pump.fun
//! AMM program on one of OUR target pools, then decode buy/sell.
//!
//! Detected swaps are pre-consensus (unconfirmed, possibly forked/failed) — the
//! decision engine treats them as an early probabilistic signal.

use anyhow::{Context, Result};
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::dex_ids::pumpfun_program;

mod pb {
    tonic::include_proto!("shredstream");
}
use pb::{shredstream_proxy_client::ShredstreamProxyClient, SubscribeEntriesRequest};

// Anchor 8-byte discriminators — single source of truth in `crate::decoders`,
// where each program's full instruction catalogue is documented.
use crate::decoders::meteora_damm_v2::{DISC_SWAP as DISC_METEORA_SWAP, DISC_SWAP2 as DISC_METEORA_SWAP2};
use crate::decoders::pump_amm::{
    DISC_BOOST_BUY_AND_BURN, DISC_BUY, DISC_BUY_EXACT_QUOTE_IN, DISC_SELL, DISC_WITHDRAW,
};

/// A decoded Meteora leg riding inside a competitor's (arb) transaction.
#[derive(Debug, Clone, Copy)]
pub struct MeteoraLeg {
    pub pool: Pubkey,
    /// Input amount (ExactIn/PartialFill) — for ExactOut this is the desired out.
    pub amount_in: u64,
    /// The user's slippage bound: `minimum_amount_out` (ExactIn/PartialFill) or
    /// `maximum_amount_in` (ExactOut).
    pub min_out: u64,
    /// True for swap2 ExactOut mode — the curve runs in reverse, so our
    /// forward-only verdict math does not apply (we log but skip its verdict).
    pub exact_out: bool,
}

/// Which PumpSwap instruction was observed — RAW program semantics, NOT the
/// economic token direction. On a normal pool base = token, quote = WSOL; on an
/// INVERTED pool (base_mint = WSOL, e.g. "WSOL-HOOD Market") a raw `Sell` is
/// economically a token BUY. The ENGINE resolves the economic direction via the
/// pool's decoded orientation (PoolInfo.token_is_a).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PumpIxKind {
    /// `buy`: exact `base_amount` OUT of the base vault; quote in + fees on top.
    Buy,
    /// `sell`: exact `base_amount` INTO the base vault; quote out − fees.
    Sell,
    /// `buy_exact_quote_in`: exact `quote_amount` IN (fees included); base out.
    BuyQuoteIn,
    /// `boost_buy_and_burn`: exact `quote_amount` into the quote vault (no user
    /// fees); the bought base is burned out of the base vault.
    BoostBuyBurn,
    /// This tx references a WATCHED pool but carries NO decodable top-level
    /// pump swap (router / private-bot CPI — Jupiter, Axiom, unknown programs).
    /// Amounts are meaningless (zero); the pool is about to change by an
    /// UNKNOWN amount.
    Opaque,
}

/// A Pump.fun swap observed on ShredStream, before it reaches Metis/chain.
/// Amounts are RAW base-mint / quote-mint side (see [`PumpIxKind`]).
#[derive(Debug, Clone, Copy)]
pub struct PumpSwapSignal {
    pub pool: Pubkey,
    pub kind: PumpIxKind,
    /// BASE-mint-side arg: `base_amount_out` (Buy), `base_amount_in` (Sell),
    /// `min_base_amount_out` (BuyQuoteIn), `min_base_amount_burned` (Boost).
    pub base_amount: u64,
    /// QUOTE-mint-side arg: limit (Buy/Sell) or EXACT in (BuyQuoteIn/Boost).
    pub quote_amount: u64,
    /// The Pump ix's user slippage bound = the u64 at data offset 16 (always):
    /// `max_quote_amount_in` (Buy), `min_quote_amount_out` (Sell),
    /// `min_base_amount_out` (BuyQuoteIn), `min_base_amount_burned` (Boost).
    /// Used by the Phase-1 sim to decide whether THIS competitor tx reverts.
    pub pump_slippage: u64,
    /// How many of OUR pools this tx touches: 1 (single swap) or 2 (Pump↔Meteora
    /// arb). Diagnostic only.
    pub hops: u8,
    /// Fee payer (static key 0) — lets the engine ignore opaque markers caused
    /// by OUR OWN in-flight transactions.
    pub fee_payer: Pubkey,
    /// Signature of the observed on-chain tx this shred carried — recorded so the
    /// fee-audit log can print the exact tx whose fee the bot computed.
    pub sig: solana_sdk::signature::Signature,
    /// Slot this shred belongs to — used to keep the live pool state in sync
    /// (apply only shreds newer than the last gRPC account update).
    pub slot: u64,
    /// If this same tx also carried a Meteora DAMM v2 swap/swap2 (i.e. it is an
    /// arb that touches a Meteora pool), the Meteora pool and its `amount_in`.
    /// Direction is the OPPOSITE of the Pump leg (a circular arb), resolved by
    /// the engine which knows each pool's token side.
    pub meteora_pool: Option<Pubkey>,
    pub meteora_amount_in: Option<u64>,
    /// The Meteora leg's slippage bound (`minimum_amount_out`, or
    /// `maximum_amount_in` when `meteora_exact_out`). For the Phase-1 verdict.
    pub meteora_min_out: Option<u64>,
    /// swap2 ExactOut — reverse curve; the forward verdict math is skipped.
    pub meteora_exact_out: bool,
    /// The leader's block order of this tx (PoH sequence): a monotonic stamp
    /// assigned as ShredStream entries are read in order. `(slot, order_seq)` is
    /// the exact order pending shred txs must be simulated in.
    pub order_seq: u64,
}

/// Per-block diagnostic: records, for each slot, the WATCHED-pool txs the
/// consumer actually forwarded (in PoH order), and prints a summary every 10
/// completed blocks. This is how the operator verifies the bot READS every
/// watched-pool tx of a block instead of jumping from tx 500 to tx 800 — a
/// gap in the printed `pool_seq`/order means a tx was skipped upstream.
type BlockRow = (u64, solana_sdk::signature::Signature, Pubkey);

#[derive(Default)]
struct BlockTracker {
    /// slot → (global order_seq, sig, pool) of every watched-pool tx seen in it.
    open: std::collections::BTreeMap<u64, Vec<BlockRow>>,
    /// Completed blocks waiting to be printed (flushed in batches of 10).
    done: Vec<(u64, Vec<BlockRow>)>,
}

impl BlockTracker {
    fn record(
        &mut self,
        slot: u64,
        order_seq: u64,
        sig: solana_sdk::signature::Signature,
        pool: Pubkey,
    ) {
        self.open.entry(slot).or_default().push((order_seq, sig, pool));
        // A slot more than 2 behind the newest one we've seen is complete
        // (shreds arrive roughly in slot order) → move it to `done`.
        if let Some(&newest) = self.open.keys().next_back() {
            let cutoff = newest.saturating_sub(2);
            let ready: Vec<u64> = self.open.range(..cutoff).map(|(k, _)| *k).collect();
            for s in ready {
                if let Some(v) = self.open.remove(&s) {
                    self.done.push((s, v));
                }
            }
        }
        // Print once per 10 completed blocks, GROUPED BY POOL so the operator can
        // see, per pool (full address), exactly which txs were read and in what
        // order — and by comparing to the chain, which were missed.
        while self.done.len() >= 10 {
            let batch: Vec<(u64, Vec<BlockRow>)> = self.done.drain(..10).collect();
            let mut out = String::from(
                "[block-read] last 10 blocks — watched-pool txs seen, grouped by pool:",
            );
            for (s, mut rows) in batch {
                rows.sort_by_key(|(o, _, _)| *o); // true leader order
                out.push_str(&format!("\nblok:{}  count={}", s, rows.len()));
                // Distinct pools in first-seen (block) order.
                let mut pools: Vec<Pubkey> = Vec::new();
                for (_, _, p) in &rows {
                    if !pools.contains(p) {
                        pools.push(*p);
                    }
                }
                for pool in pools {
                    out.push_str(&format!("\n  pool {pool}:"));
                    let mut i = 0;
                    for (oseq, sig, p) in &rows {
                        if *p != pool {
                            continue;
                        }
                        i += 1;
                        let h = sig.to_string();
                        let head = &h[..h.len().min(5)];
                        out.push_str(&format!("\n    {i}:{head} (seq {oseq})"));
                    }
                }
            }
            info!("{}", out);
        }
    }
}

/// Runtime counters for observability.
#[derive(Default)]
pub struct ShredMetrics {
    pub entries: AtomicU64,
    pub txns: AtomicU64,
    pub pump_txns: AtomicU64,
    pub matched: AtomicU64,
    pub unresolved_pool: AtomicU64,
    pub signals_sent: AtomicU64,
    pub signals_dropped: AtomicU64,
    /// Pump txs that touched a WATCHED pool with NO decodable top-level swap
    /// (router / private-bot CPI) — sent to the engine as opaque markers.
    pub opaque_matched: AtomicU64,
    /// Number of target Pump pools being watched (set once at startup).
    pub watched_pools: AtomicU64,
    /// Per-tx panics contained by `catch_unwind` (should stay 0; nonzero means a
    /// tx shape is hitting a bug — the consumer survives and keeps processing).
    pub scan_panics: AtomicU64,
}

pub struct ShredConsumer {
    endpoint: String,
    /// Pool pubkeys we care about (Pump.fun side of each arb pair). Mutable at
    /// runtime so newly-discovered pools can be added without a restart.
    target_pools: std::sync::RwLock<HashSet<Pubkey>>,
    /// Meteora DAMM v2 program id — to spot the arb's Meteora leg in the same tx.
    meteora: Pubkey,
    /// Global library of address-lookup-table contents harvested from every
    /// Pump tx we see (key → member pubkeys). Two uses: resolving ALT-hidden
    /// accounts on the hot path, and as the pool of PUBLIC pre-built tables the
    /// engine picks from to compress its own txs. Shared (Arc) so the engine
    /// reads the same live library.
    alt_map: Arc<std::sync::RwLock<HashMap<Pubkey, Vec<Pubkey>>>>,
    pumpfun: Pubkey,
    /// Optional sink for detected remove-liquidity (`withdraw`) events on a
    /// watched pool — the Pump pool pubkey is sent so the manager can close it.
    remove_tx: std::sync::RwLock<Option<mpsc::Sender<Pubkey>>>,
    /// RPC used by the self-learning ALT fetcher.
    rpc: Arc<solana_client::rpc_client::RpcClient>,
    /// ALT account keys seen on Pump txns that we couldn't resolve yet. A
    /// background task fetches these and folds them into `alt_map`, so that
    /// swaps hiding the pool behind an ALT become resolvable — this is how we
    /// stop missing trades that competitors (who resolve ALTs) already see.
    pending_alts: std::sync::Mutex<HashSet<Pubkey>>,
    /// Optional sink for `(pump_pool, alt_keys)` — the ALTs a competitor tx used
    /// on a watched pool, fed to the AltRegistry so it can pick the best table.
    alt_candidate_tx: std::sync::RwLock<Option<mpsc::Sender<(Pubkey, Vec<Pubkey>)>>>,
    /// Monotonic ORDER stamp assigned to every transaction as we read the
    /// ShredStream entries. Entries are PoH-sequenced (the leader's exact
    /// execution order) and we iterate them in order, so this counter IS the
    /// block order of pending transactions — the ground truth for ordering
    /// shred txs that have no account/transaction update yet.
    order_seq: AtomicU64,
    /// Per-block diagnostic tracker (prints every 10 blocks).
    block_log: std::sync::Mutex<BlockTracker>,
    /// Known aggregator/router program ids (Jupiter, OKX, DFlow, …). A tx that
    /// invokes one of these as a STATIC key is let through the cheap pre-filter
    /// even when the Pump program itself is ALT-hidden, so router swaps on a
    /// watched pool are resolved and enqueued instead of being dropped.
    router_programs: HashSet<Pubkey>,
    /// ALT keys known to CONTAIN a watched pool. A private-bot tx (no known
    /// program in its static keys) that references one of these tables is almost
    /// certainly touching our pool, so we let it through the pre-filter, resolve
    /// its accounts, and enqueue it — cheaply, without resolving every cluster tx.
    pool_alts: std::sync::RwLock<HashSet<Pubkey>>,
    /// Unknown top-level programs seen touching a watched pool — queued for the
    /// self-learning IDL fetcher to inspect (bounded).
    pending_programs: std::sync::Mutex<HashSet<Pubkey>>,
    /// Programs whose on-chain IDL we've learned (name → arg layout by disc).
    /// Populated by the IDL learner; routers found this way are also added to
    /// `router_programs` so their future txs pass the pre-filter.
    learned_idls: std::sync::RwLock<HashMap<Pubkey, crate::decoders::self_learn::ProgramIdl>>,
    /// Routers DISCOVERED at runtime by the IDL learner (their swap/route txs
    /// then pass the pre-filter, same as the hard-coded `router_programs`).
    learned_routers: std::sync::RwLock<HashSet<Pubkey>>,
    /// Dedicated RPC clients used ONLY to harvest ALT contents in parallel, kept
    /// off the trading RPC. Empty → the fetcher falls back to `self.rpc`.
    alt_rpcs: std::sync::RwLock<Vec<Arc<solana_client::rpc_client::RpcClient>>>,
    /// When the consumer started — used to bound the broad ALT warm-up window.
    started: std::time::Instant,
    /// Distinct slots seen since the last metrics report — proves whether we are
    /// actually receiving most of the cluster's blocks from the shred proxy (a
    /// 30s window spans ~75 slots at 2.5 slots/s; far fewer means the proxy is
    /// delivering only a fraction of shreds and we can never see those txs).
    slots_seen: std::sync::Mutex<HashSet<u64>>,
    /// Warm-up window: while inside it, harvest EVERY tx's ALTs (build the broad
    /// cache); after it, only resolve_keys learns ALTs (txs on our pools).
    alt_warmup: std::sync::RwLock<Duration>,
    pub metrics: Arc<ShredMetrics>,
}

impl ShredConsumer {
    pub fn new(
        endpoint: String,
        target_pools: HashSet<Pubkey>,
        alt_map: HashMap<Pubkey, Vec<Pubkey>>,
        rpc: Arc<solana_client::rpc_client::RpcClient>,
    ) -> Self {
        let metrics = Arc::new(ShredMetrics::default());
        metrics
            .watched_pools
            .store(target_pools.len() as u64, Ordering::Relaxed);
        // Seed the pool-ALT set from any pre-supplied tables that already hold a
        // watched pool, so private-bot detection works from the first shred.
        let pool_alts: HashSet<Pubkey> = alt_map
            .iter()
            .filter(|(_, members)| members.iter().any(|m| target_pools.contains(m)))
            .map(|(alt, _)| *alt)
            .collect();
        Self {
            endpoint,
            target_pools: std::sync::RwLock::new(target_pools),
            alt_map: Arc::new(std::sync::RwLock::new(alt_map)),
            pumpfun: pumpfun_program(),
            meteora: crate::dex_ids::meteora_program(),
            remove_tx: std::sync::RwLock::new(None),
            rpc,
            pending_alts: std::sync::Mutex::new(HashSet::new()),
            alt_candidate_tx: std::sync::RwLock::new(None),
            order_seq: AtomicU64::new(0),
            block_log: std::sync::Mutex::new(BlockTracker::default()),
            router_programs: crate::decoders::known_router_pubkeys().into_iter().collect(),
            pool_alts: std::sync::RwLock::new(pool_alts),
            pending_programs: std::sync::Mutex::new(HashSet::new()),
            learned_idls: std::sync::RwLock::new(HashMap::new()),
            learned_routers: std::sync::RwLock::new(HashSet::new()),
            alt_rpcs: std::sync::RwLock::new(Vec::new()),
            started: std::time::Instant::now(),
            slots_seen: std::sync::Mutex::new(HashSet::new()),
            alt_warmup: std::sync::RwLock::new(Duration::from_secs(1800)),
            metrics,
        }
    }

    /// Set the broad-harvest warm-up window (from config `alt_warmup_secs`).
    pub fn set_alt_warmup(&self, secs: u64) {
        *self.alt_warmup.write().unwrap() = Duration::from_secs(secs);
    }

    /// Install the dedicated ALT-harvest RPC pool (built from
    /// `config.rpc.alt_rpc_urls`). Called once at startup before the fetcher runs.
    pub fn set_alt_rpcs(&self, rpcs: Vec<Arc<solana_client::rpc_client::RpcClient>>) {
        *self.alt_rpcs.write().unwrap() = rpcs;
    }

    /// Load a persisted ALT cache from disk so learned tables SURVIVE restarts
    /// (otherwise every run re-harvests from scratch — the reason "very few ALTs"
    /// were loaded). Format: one line per table, `<alt_b58> <addr_b58> <addr_b58>…`.
    pub fn load_alt_cache(&self, path: &str) {
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => return, // no cache yet — first run
        };
        let mut loaded = 0usize;
        let mut map = self.alt_map.write().unwrap();
        for line in content.lines() {
            let mut it = line.split_whitespace();
            let alt = match it.next().and_then(|s| s.parse::<Pubkey>().ok()) {
                Some(a) => a,
                None => continue,
            };
            let addrs: Vec<Pubkey> = it.filter_map(|s| s.parse::<Pubkey>().ok()).collect();
            if !addrs.is_empty() {
                map.entry(alt).or_insert(addrs);
                loaded += 1;
            }
        }
        drop(map);
        // Seed pool_alts from the loaded tables that hold a watched pool.
        let tables: Vec<(Pubkey, Vec<Pubkey>)> = self
            .alt_map
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        for (alt, addrs) in tables {
            self.note_alt_members(alt, &addrs);
        }
        info!(loaded, "loaded persisted ALT cache from disk");
    }

    /// Periodically write the ALT cache to disk so it accumulates across runs.
    pub fn spawn_alt_persister(self: Arc<Self>, path: String) {
        tokio::spawn(async move {
            let mut last_len = 0usize;
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                // Refresh pool_alts from the WHOLE cache against the CURRENT watch
                // set. Pools are added dynamically, so an ALT harvested BEFORE its
                // pool was watched would otherwise never enter pool_alts (and its
                // private-bot txs would keep failing the pre-filter). Recomputing
                // closes that staleness gap.
                {
                    let targets = self.target_pools.read().unwrap();
                    if !targets.is_empty() {
                        let map = self.alt_map.read().unwrap();
                        let mut palts = self.pool_alts.write().unwrap();
                        for (alt, addrs) in map.iter() {
                            if addrs.iter().any(|m| targets.contains(m)) {
                                palts.insert(*alt);
                            }
                        }
                    }
                }
                // Snapshot under the read lock, then write outside it.
                let snapshot: Vec<(Pubkey, Vec<Pubkey>)> = {
                    let map = self.alt_map.read().unwrap();
                    if map.len() == last_len {
                        continue; // nothing new since last save
                    }
                    last_len = map.len();
                    map.iter().map(|(k, v)| (*k, v.clone())).collect()
                };
                let mut out = String::with_capacity(snapshot.len() * 64);
                for (alt, addrs) in &snapshot {
                    out.push_str(&alt.to_string());
                    for a in addrs {
                        out.push(' ');
                        out.push_str(&a.to_string());
                    }
                    out.push('\n');
                }
                let tmp = format!("{path}.tmp");
                if std::fs::write(&tmp, &out).and_then(|_| std::fs::rename(&tmp, &path)).is_ok() {
                    info!(tables = snapshot.len(), "persisted ALT cache to disk");
                }
            }
        });
    }

    /// Record that `alt` contains a watched pool, if it does — so private-bot txs
    /// referencing this table pass the pre-filter. Called wherever we learn an ALT.
    fn note_alt_members(&self, alt: Pubkey, members: &[Pubkey]) {
        let touches = {
            let targets = self.target_pools.read().unwrap();
            members.iter().any(|m| targets.contains(m))
        };
        if touches {
            self.pool_alts.write().unwrap().insert(alt);
        }
    }

    /// Register the AltRegistry sink that receives `(pump_pool, alt_keys)` for
    /// each competitor tx seen on a watched pool.
    #[allow(dead_code)]
    pub fn set_alt_candidate_sender(&self, tx: mpsc::Sender<(Pubkey, Vec<Pubkey>)>) {
        *self.alt_candidate_tx.write().unwrap() = Some(tx);
    }

    /// Share the live global ALT library (key → member pubkeys). The engine
    /// reads this to pick the best public tables to compress its own txs.
    pub fn alt_library(&self) -> Arc<std::sync::RwLock<HashMap<Pubkey, Vec<Pubkey>>>> {
        self.alt_map.clone()
    }

    /// Background task: periodically fetch ALT account contents we don't yet
    /// know (harvested from observed Pump txns) and add them to `alt_map`, so
    /// pool accounts hidden behind those ALTs become resolvable.
    /// Periodically log the raw shred-consumer counters, so it's visible whether
    /// entries/txs are arriving, being decoded, matched, and forwarded — the
    /// missing diagnostic when "no data is processed but a counter grows".
    pub fn spawn_metrics_reporter(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                let m = &self.metrics;
                // ALT state: how many tables we know, how many are queued to
                // fetch, and how many are recognised as containing a watched pool
                // (pool_alts drives private-bot detection). This tells us whether
                // the coverage gap is "few ALTs" or "pools not in the ALTs we have".
                let alt_known = self.alt_map.read().unwrap().len();
                let alt_pending = self.pending_alts.lock().unwrap().len();
                let alt_with_pool = self.pool_alts.read().unwrap().len();
                // Distinct slots seen in the last 30s (~75 expected). Far fewer =
                // the proxy is only delivering a fraction of the cluster's shreds.
                let slots_30s = {
                    let mut s = self.slots_seen.lock().unwrap();
                    let n = s.len();
                    s.clear();
                    n
                };
                info!(
                    slots_30s,
                    entries = m.entries.load(Ordering::Relaxed),
                    txns = m.txns.load(Ordering::Relaxed),
                    pump_txns = m.pump_txns.load(Ordering::Relaxed),
                    matched = m.matched.load(Ordering::Relaxed),
                    opaque = m.opaque_matched.load(Ordering::Relaxed),
                    signals_sent = m.signals_sent.load(Ordering::Relaxed),
                    signals_dropped = m.signals_dropped.load(Ordering::Relaxed),
                    unresolved_pool = m.unresolved_pool.load(Ordering::Relaxed),
                    alt_known,
                    alt_pending,
                    alt_with_pool,
                    scan_panics = m.scan_panics.load(Ordering::Relaxed),
                    watched = m.watched_pools.load(Ordering::Relaxed),
                    "[shred-consumer 30s]"
                );
            }
        });
    }

    pub fn spawn_alt_fetcher(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                // The dedicated ALT-harvest RPC pool (falls back to the main rpc).
                let rpcs: Vec<Arc<solana_client::rpc_client::RpcClient>> = {
                    let pool = self.alt_rpcs.read().unwrap();
                    if pool.is_empty() {
                        vec![self.rpc.clone()]
                    } else {
                        pool.clone()
                    }
                };
                // getMultipleAccounts fetches up to 100 tables in ONE request, so
                // each RPC does ~100/call not ~10/s — 100x faster cache fill. Take
                // up to 100 unknown tables per RPC this tick.
                let per_rpc = 100usize;
                let want = per_rpc * rpcs.len();
                let batch: Vec<Pubkey> = {
                    let mut pending = self.pending_alts.lock().unwrap();
                    if pending.is_empty() {
                        continue;
                    }
                    let known = self.alt_map.read().unwrap();
                    let take: Vec<Pubkey> = pending
                        .iter()
                        .filter(|k| !known.contains_key(k))
                        .copied()
                        .take(want)
                        .collect();
                    for k in &take {
                        pending.remove(k);
                    }
                    take
                };
                if batch.is_empty() {
                    continue;
                }
                // One getMultipleAccounts (≤100 keys) per RPC, in parallel.
                let chunk_size = batch.len().div_ceil(rpcs.len()).max(1).min(100);
                let mut handles = Vec::new();
                for (i, chunk) in batch.chunks(chunk_size).enumerate() {
                    let rpc = rpcs[i % rpcs.len()].clone();
                    let me = self.clone();
                    let chunk: Vec<Pubkey> = chunk.to_vec();
                    handles.push(tokio::task::spawn_blocking(move || {
                        let mut added = 0usize;
                        let mut failed: Vec<Pubkey> = Vec::new();
                        match rpc.get_multiple_accounts(&chunk) {
                            Ok(accts) => {
                                for (alt, maybe) in chunk.iter().zip(accts) {
                                    match maybe {
                                        Some(acct) => {
                                            if let Ok(addrs) =
                                                crate::transaction::deserialize_alt_addresses(
                                                    &acct.data,
                                                )
                                            {
                                                me.note_alt_members(*alt, &addrs);
                                                me.alt_map.write().unwrap().insert(*alt, addrs);
                                                added += 1;
                                            }
                                            // else: not an ALT / malformed → drop (never parses)
                                        }
                                        // Account doesn't exist → don't retry.
                                        None => {}
                                    }
                                }
                            }
                            // Whole request failed (429/timeout) → retry the chunk.
                            Err(_) => failed.extend(chunk),
                        }
                        (added, failed)
                    }));
                }
                let mut added = 0usize;
                let mut retry: Vec<Pubkey> = Vec::new();
                for h in handles {
                    if let Ok((n, failed)) = h.await {
                        added += n;
                        retry.extend(failed);
                    }
                }
                // Never drop unprocessed tables: re-queue RPC failures for a later
                // attempt (the queue is ALT-only and unbounded except for safety).
                if !retry.is_empty() {
                    let known = self.alt_map.read().unwrap();
                    let mut pending = self.pending_alts.lock().unwrap();
                    for k in retry {
                        if !known.contains_key(&k) {
                            pending.insert(k);
                        }
                    }
                }
                if added > 0 {
                    info!(added, "shred ALT cache learned new lookup tables");
                }
            }
        });
    }

    /// Register a channel that receives the Pump pool pubkey whenever a
    /// remove-liquidity (`withdraw`) instruction is seen on a watched pool.
    pub fn set_remove_sender(&self, tx: mpsc::Sender<Pubkey>) {
        *self.remove_tx.write().unwrap() = Some(tx);
    }

    /// Add a Pump.fun pool to the watch set at runtime (and optionally its ALT
    /// contents for account resolution). Takes effect on the next shred.
    pub fn add_target(&self, pool: Pubkey, alt: Option<(Pubkey, Vec<Pubkey>)>) {
        // Compute everything that needs the target set, then DROP the write guard
        // before taking any other lock. Calling a method that re-reads
        // target_pools while this write guard is held self-deadlocks the thread —
        // and since the shred consumer also reads target_pools, that froze the
        // whole consumer. Keep this critical section lock-clean.
        let alt_touches_watched;
        {
            let mut set = self.target_pools.write().unwrap();
            if set.insert(pool) {
                self.metrics
                    .watched_pools
                    .store(set.len() as u64, Ordering::Relaxed);
            }
            // Membership check for the ALT uses the guard we ALREADY hold — no
            // re-lock. A freshly-added pool is included via the insert above.
            alt_touches_watched = alt
                .as_ref()
                .map(|(_, addrs)| addrs.iter().any(|m| set.contains(m)))
                .unwrap_or(false);
        } // target_pools write guard dropped here
        if let Some((alt_key, addrs)) = alt {
            if alt_touches_watched {
                self.pool_alts.write().unwrap().insert(alt_key);
            }
            self.alt_map.write().unwrap().insert(alt_key, addrs);
        }
    }

    /// Remove a pool from the watch set (dead/rugged pool).
    pub fn remove_target(&self, pool: &Pubkey) {
        let mut set = self.target_pools.write().unwrap();
        if set.remove(pool) {
            self.metrics
                .watched_pools
                .store(set.len() as u64, Ordering::Relaxed);
        }
    }

    /// Spawn the consumer; detected swaps are pushed to `tx`. Reconnects with
    /// backoff.
    pub fn spawn(self: Arc<Self>, tx: mpsc::Sender<PumpSwapSignal>) {
        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                match self.run(&tx).await {
                    Ok(()) => warn!("shredstream ended cleanly, reconnecting"),
                    Err(e) => warn!(error = %e, "shredstream error, reconnecting"),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
        });
    }

    async fn run(&self, tx: &mpsc::Sender<PumpSwapSignal>) -> Result<()> {
        let mut client = ShredstreamProxyClient::connect(self.endpoint.clone())
            .await
            .with_context(|| format!("shredstream connect failed: {}", self.endpoint))?;

        let mut stream = client
            .subscribe_entries(SubscribeEntriesRequest {})
            .await
            .context("subscribe_entries failed")?
            .into_inner();

        info!(endpoint = %self.endpoint, "shredstream subscription active");

        while let Some(msg) = stream.message().await? {
            self.metrics.entries.fetch_add(1, Ordering::Relaxed);
            let slot = msg.slot;
            self.slots_seen.lock().unwrap().insert(slot);
            let entries: Vec<solana_entry::entry::Entry> =
                match bincode::deserialize(&msg.entries) {
                    Ok(e) => e,
                    Err(_) => continue, // partial / unrecoverable FEC set
                };
            for entry in &entries {
                for vtx in &entry.transactions {
                    self.metrics.txns.fetch_add(1, Ordering::Relaxed);
                    // Stamp EVERY tx (in PoH order) with a monotonic sequence, so
                    // the pending-tx order is the leader's exact block order.
                    let order_seq = self.order_seq.fetch_add(1, Ordering::Relaxed);
                    // Contain any panic to THIS tx: a single malformed tx must
                    // never kill the whole consumer task (which would silently
                    // stop all shred processing while the proxy keeps sending).
                    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        self.scan_tx(slot, order_seq, vtx, tx)
                    }));
                    if r.is_err() {
                        self.metrics.scan_panics.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        Ok(())
    }

    fn scan_tx(
        &self,
        slot: u64,
        order_seq: u64,
        vtx: &solana_sdk::transaction::VersionedTransaction,
        tx: &mpsc::Sender<PumpSwapSignal>,
    ) {
        let msg = &vtx.message;
        let static_keys = msg.static_account_keys();

        // HARVEST FIRST (before the pre-filter drops most txs): during the WARM-UP
        // window, queue every ALT table any tx references that we don't already
        // know, so the pool fetches its contents. This breaks the chicken-and-egg
        // where an ALT-hidden competitor pool tx was dropped before its table
        // could be learned — next time we see that table, the pool resolves.
        // Cheap (reads table keys only). After warm-up we stop the broad sweep and
        // let resolve_keys learn only the ALTs of txs on OUR pools. The queue is
        // never dropped for lack of space (bounded only by a large safety cap).
        if self.started.elapsed() < *self.alt_warmup.read().unwrap() {
            if let Some(lookups) = msg.address_table_lookups() {
                if !lookups.is_empty() {
                    let unknown: Vec<Pubkey> = {
                        let known = self.alt_map.read().unwrap();
                        lookups
                            .iter()
                            .map(|l| l.account_key)
                            .filter(|k| !known.contains_key(k))
                            .collect()
                    };
                    if !unknown.is_empty() {
                        let mut pending = self.pending_alts.lock().unwrap();
                        if pending.len() < 1_000_000 {
                            for k in unknown {
                                pending.insert(k);
                            }
                        }
                    }
                }
            }
        }

        // Cheap pre-filter: process the tx if a program we can act on appears as
        // a STATIC key — either the Pump/Meteora AMM itself (native swap), or a
        // KNOWN router (Jupiter/OKX/DFlow) that CPIs into our pools with the AMM
        // program ALT-hidden. Without the router clause every aggregator tx on a
        // watched pool was dropped here, leaving holes in the ordered sequence.
        let learned_has = |k: &Pubkey| {
            self.learned_routers
                .read()
                .map(|s| s.contains(k))
                .unwrap_or_else(|e| e.into_inner().contains(k)) // survive poison
        };
        let touches_known_program = static_keys.iter().any(|k| {
            *k == self.pumpfun
                || *k == self.meteora
                || self.router_programs.contains(k)
                || learned_has(k)
        });
        // Cheap watched-pool catch: does a WATCHED POOL appear directly as a
        // STATIC key? Aggregators/private bots keep the pool's real state accounts
        // as static keys (only repeated infra accounts go in ALTs), so this is the
        // biggest missed class and it needs no ALT resolution at all.
        let static_has_watched_pool = {
            let targets = self.target_pools.read().unwrap_or_else(|e| e.into_inner());
            static_keys.iter().any(|k| targets.contains(k))
        };
        // Cheap private-bot catch: does this tx reference an ALT we know holds a
        // watched pool? (Set lookup per ALT key — no full resolution.)
        let uses_pool_alt = || {
            let palts = self
                .pool_alts
                .read()
                .unwrap_or_else(|e| e.into_inner()); // survive poison
            if palts.is_empty() {
                return false;
            }
            msg.address_table_lookups()
                .map(|ls| ls.iter().any(|l| palts.contains(&l.account_key)))
                .unwrap_or(false)
        };
        let cheap_pass = touches_known_program || static_has_watched_pool || uses_pool_alt();
        // A LEGACY tx (no ALTs) that failed the cheap static checks cannot possibly
        // reference the Pump/Meteora program or a watched pool (everything is a
        // static key), so drop it without resolving.
        let has_alts = msg
            .address_table_lookups()
            .map(|ls| !ls.is_empty())
            .unwrap_or(false);
        if !cheap_pass && !has_alts {
            return;
        }

        // Resolve the full ordered account list (static + ALT writable + ALT
        // readonly) ONCE — RPC-free, from the cached ALT map. We need it for the
        // definitive filter below and for decoding.
        let full_keys = self.resolve_keys(msg);

        // DEFINITIVE FILTER (operator's rule: the ONLY filters are pool + program).
        // If the cheap static checks didn't already pass, process this v0 tx only
        // when the Pump/Meteora PROGRAM or a WATCHED POOL actually appears in the
        // RESOLVED keys. This catches DIRECT Pump swaps whose program/pool was
        // hidden inside an ALT — the biggest missed class — with no static-key
        // requirement and no ALT flagging needed.
        if !cheap_pass {
            let present = {
                let targets = self.target_pools.read().unwrap_or_else(|e| e.into_inner());
                full_keys.iter().any(|k| {
                    *k == self.pumpfun
                        || *k == self.meteora
                        || (*k != Pubkey::default() && targets.contains(k))
                })
            };
            if !present {
                return;
            }
        }
        self.metrics.pump_txns.fetch_add(1, Ordering::Relaxed);

        // Is there also a Meteora DAMM v2 `swap` in THIS tx? In a circular arb the
        // competitor's Meteora leg rides in the same tx as the Pump leg we detect,
        // so we can advance our cached Meteora price from it — even though we never
        // receive standalone Meteora shreds. Extract (meteora_pool, amount_in);
        // direction is resolved by the engine as the opposite of the Pump leg.
        let meteora = self.find_meteora_swap(msg, &full_keys);

        // The ALT keys this tx used — candidates for whichever watched pool it
        // touches (fed to the AltRegistry, which picks the best-coverage table).
        let tx_alts: Vec<Pubkey> = msg
            .address_table_lookups()
            .map(|ls| ls.iter().map(|l| l.account_key).collect())
            .unwrap_or_default();

        let tx_sig = vtx.signatures.first().copied().unwrap_or_default();
        let fee_payer = static_keys.first().copied().unwrap_or_default();
        // Pools we DECODED a top-level swap for in this tx — used below to spot
        // watched pools this tx touches through an UNDECODABLE path instead.
        let mut decoded_pools: Vec<Pubkey> = Vec::new();

        for ix in msg.instructions() {
            let program = match full_keys.get(ix.program_id_index as usize) {
                Some(p) => *p,
                None => continue,
            };
            if program != self.pumpfun {
                continue;
            }
            if ix.data.len() < 8 {
                continue;
            }
            let disc: [u8; 8] = ix.data[0..8].try_into().unwrap();
            let kind = match disc {
                DISC_BUY => Some(PumpIxKind::Buy),
                DISC_SELL => Some(PumpIxKind::Sell),
                DISC_BUY_EXACT_QUOTE_IN => Some(PumpIxKind::BuyQuoteIn),
                DISC_BOOST_BUY_AND_BURN => Some(PumpIxKind::BoostBuyBurn),
                _ => None,
            };
            let is_withdraw = disc == DISC_WITHDRAW;
            if kind.is_none() && !is_withdraw {
                continue;
            }
            // Account index 0 = pool.
            let pool = match ix.accounts.first().and_then(|i| full_keys.get(*i as usize)) {
                Some(p) => *p,
                None => continue,
            };
            if pool == Pubkey::default() {
                self.metrics.unresolved_pool.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            // Only WATCHED pools are forwarded. Forwarding every Pump swap on the
            // cluster floods the engine channel and puts shred processing minutes
            // behind — so by the time a tx's shred is handled, its account-update
            // (only ~200ms behind) has long passed, and the ordered sequencer can
            // never line them up. We trade only watched pools, so that is all the
            // sequencer needs.
            let watched = self.target_pools.read().unwrap().contains(&pool);

            // Feed this tx's ALTs as candidates for the pool (the AltRegistry
            // picks the best-coverage one and stops once it's good enough).
            if watched && !tx_alts.is_empty() {
                if let Some(s) = self.alt_candidate_tx.read().unwrap().as_ref() {
                    let _ = s.try_send((pool, tx_alts.clone()));
                }
            }

            // Remove-liquidity on a watched pool → signal the manager to close it.
            if is_withdraw {
                if watched {
                    if let Some(sender) = self.remove_tx.read().unwrap().as_ref() {
                        let _ = sender.try_send(pool);
                    }
                }
                continue;
            }

            if ix.data.len() < 24 {
                continue;
            }
            let kind = kind.unwrap(); // withdraw handled above
            let arg0 = u64::from_le_bytes(ix.data[8..16].try_into().unwrap());
            let arg1 = u64::from_le_bytes(ix.data[16..24].try_into().unwrap());
            // Arg order differs per instruction: buy/sell carry (base, quote);
            // buy_exact_quote_in and boost_buy_and_burn carry (quote, base).
            let (base_amount, quote_amount) = match kind {
                PumpIxKind::Buy | PumpIxKind::Sell => (arg0, arg1),
                PumpIxKind::BuyQuoteIn | PumpIxKind::BoostBuyBurn => (arg1, arg0),
                PumpIxKind::Opaque => unreachable!(),
            };

            self.metrics.matched.fetch_add(1, Ordering::Relaxed);
            decoded_pools.push(pool);
            // The Pump slippage bound is ALWAYS the u64 at data offset 16.
            let pump_slippage = arg1;
            let (meteora_pool, meteora_amount_in, meteora_min_out, meteora_exact_out) = match meteora
            {
                Some(m) => (Some(m.pool), Some(m.amount_in), Some(m.min_out), m.exact_out),
                None => (None, None, None, false),
            };
            // Hops: 1 = Pump-only; 2 = also touches a watched Meteora pool.
            let hops = if meteora_pool.is_some() { 2 } else { 1 };
            let signal = PumpSwapSignal {
                pool,
                kind,
                base_amount,
                quote_amount,
                pump_slippage,
                hops,
                fee_payer,
                sig: tx_sig,
                slot,
                meteora_pool,
                meteora_amount_in,
                meteora_min_out,
                meteora_exact_out,
                order_seq,
            };
            // Only forward WATCHED pools — see the firehose note above.
            if watched {
                // Record for the per-block diagnostic BEFORE the try_send drop:
                // we want to prove the tx was READ, regardless of channel pressure.
                self.block_log
                    .lock()
                    .unwrap()
                    .record(slot, order_seq, tx_sig, pool);
                // Non-blocking: if the engine is busy, drop (staleness makes an
                // old signal worthless anyway).
                match tx.try_send(signal) {
                    Ok(()) => self.metrics.signals_sent.fetch_add(1, Ordering::Relaxed),
                    Err(_) => self.metrics.signals_dropped.fetch_add(1, Ordering::Relaxed),
                };
            }
        }

        // ── Aggregator path: a KNOWN router whose first hop is a Pump SELL we
        // can PRICE from the shred (Jupiter route/route_v2, OKX swap). The route
        // input equals the first hop's base_amount_in, so this becomes a normal
        // Readable Sell signal instead of an Unreadable wait. Only the clean,
        // single-Pump-pool shapes decode (see route_decode) — anything ambiguous
        // returns None and falls through to the opaque path below. ────────────
        {
            // A decoded first-hop leg: (kind, base_amount, quote_amount).
            let mut leg: Option<(PumpIxKind, u64, u64)> = None;
            for ix in msg.instructions() {
                let program = match full_keys.get(ix.program_id_index as usize) {
                    Some(p) => *p,
                    None => continue,
                };
                // 1) HARD-CODED routers (Jupiter/OKX): first hop Pump sell.
                if self.router_programs.contains(&program) {
                    if let Some(h) = crate::decoders::route_decode::decode_first_hop(&ix.data) {
                        leg = Some((PumpIxKind::Sell, h.base_amount_in, 0));
                        break;
                    }
                }
                // 2) SELF-LEARNED routers (scalar-first): read amount_in by field
                //    name, take direction from the instruction NAME (…sell…/…buy…).
                //    A generic "swap"/"route" name gives no direction → skip (stays
                //    opaque). Only named buy/sell instructions become Readable.
                if let Some(l) = self.learned_router_leg(&program, &ix.data) {
                    leg = Some(l);
                    break;
                }
            }
            if let Some((kind, base_amount, quote_amount)) = leg {
                // target_pools holds only Pump pools; a single watched pool present
                // IS the pool this leg swaps on.
                let pool = {
                    let targets = self.target_pools.read().unwrap();
                    let mut found: Vec<Pubkey> = full_keys
                        .iter()
                        .copied()
                        .filter(|k| {
                            *k != Pubkey::default()
                                && targets.contains(k)
                                && !decoded_pools.contains(k)
                        })
                        .collect();
                    found.dedup();
                    if found.len() == 1 {
                        Some(found[0])
                    } else {
                        None // 0 or ambiguous → let the opaque path handle it
                    }
                };
                if let Some(pool) = pool {
                    decoded_pools.push(pool);
                    self.metrics.matched.fetch_add(1, Ordering::Relaxed);
                    self.block_log.lock().unwrap().record(slot, order_seq, tx_sig, pool);
                    let signal = PumpSwapSignal {
                        pool,
                        kind,
                        base_amount,
                        quote_amount,
                        // Router enforces slippage at the route level, not per leg
                        // (the inner CPI uses min_out=0), so no per-leg revert bound.
                        pump_slippage: 0,
                        hops: 1,
                        fee_payer,
                        sig: tx_sig,
                        slot,
                        meteora_pool: None,
                        meteora_amount_in: None,
                        meteora_min_out: None,
                        meteora_exact_out: false,
                        order_seq,
                    };
                    match tx.try_send(signal) {
                        Ok(()) => self.metrics.signals_sent.fetch_add(1, Ordering::Relaxed),
                        Err(_) => self.metrics.signals_dropped.fetch_add(1, Ordering::Relaxed),
                    };
                }
            }
        }

        // ── Opaque path: watched pool touched via a router / private bot ──────
        // The tx invokes the Pump/Meteora AMM or a known router (Jupiter/OKX/
        // DFlow) and one of OUR pools appears in its (ALT-resolved) account list,
        // but we decoded NO top-level Pump swap for that pool — the swap rides
        // inside a CPI (Jupiter route, Axiom, unknown on-chain bots) whose exact
        // amounts are not in shred data (they live in tx meta). We cannot price
        // it, so we enqueue it as an Unreadable marker: the sequencer waits for
        // this tx's own account-update to learn the pool's new reserves — it is
        // never skipped, so the ordered sequence keeps no holes.
        // The pool WILL change by an unknown amount, so tell the engine to
        // invalidate its live overlay and hold trading until fresh gRPC state.
        let mut opaque_hit = false;
        {
            let targets = self.target_pools.read().unwrap();
            for key in &full_keys {
                if *key == Pubkey::default() || !targets.contains(key) {
                    continue;
                }
                if decoded_pools.contains(key) {
                    continue; // this pool's swap was decoded above
                }
                opaque_hit = true;
                self.metrics.opaque_matched.fetch_add(1, Ordering::Relaxed);
                // Opaque txs still TOUCH the pool (in PoH order) — record them so
                // the per-block diagnostic counts them and the sequence has no gap.
                self.block_log
                    .lock()
                    .unwrap()
                    .record(slot, order_seq, tx_sig, *key);
                let signal = PumpSwapSignal {
                    pool: *key,
                    kind: PumpIxKind::Opaque,
                    base_amount: 0,
                    quote_amount: 0,
                    pump_slippage: 0,
                    hops: 1,
                    fee_payer,
                    sig: tx_sig,
                    slot,
                    meteora_pool: None,
                    meteora_amount_in: None,
                    meteora_min_out: None,
                    meteora_exact_out: false,
                    order_seq,
                };
                match tx.try_send(signal) {
                    Ok(()) => self.metrics.signals_sent.fetch_add(1, Ordering::Relaxed),
                    Err(_) => self.metrics.signals_dropped.fetch_add(1, Ordering::Relaxed),
                };
            }
        }
        // Self-learning: this tx touched a watched pool but we couldn't decode its
        // swap. Queue any unknown top-level program it invoked for the IDL learner,
        // which may recognise it as a new router and add it to the filter.
        if opaque_hit {
            self.note_unknown_programs(msg, &full_keys);
        }
    }

    /// Try to read a first-hop leg from a SELF-LEARNED router's instruction. Uses
    /// the learned IDL to find the instruction by discriminator, its direction
    /// from the instruction NAME (contains "sell"/"buy"), and its input amount
    /// from a leading scalar arg (`amount_in`/`in_amount`/…). Returns
    /// `(kind, base_amount, quote_amount)` or None (→ stays opaque). A sell's
    /// input is the base token; a buy spends exact quote (`buy_exact_quote_in`).
    fn learned_router_leg(&self, program: &Pubkey, data: &[u8]) -> Option<(PumpIxKind, u64, u64)> {
        if data.len() < 8 {
            return None;
        }
        let disc: [u8; 8] = data[0..8].try_into().ok()?;
        let learned = self.learned_idls.read().unwrap();
        let idl = learned.get(program)?;
        let def = idl.by_disc.get(&disc)?;
        let name = def.name.to_lowercase();
        let scalars = crate::decoders::self_learn::extract_leading_scalars(data, def);
        let amount = crate::decoders::self_learn::amount_in(&scalars)?;
        if name.contains("sell") {
            Some((PumpIxKind::Sell, amount, 0)) // base_amount_in
        } else if name.contains("buy") {
            Some((PumpIxKind::BuyQuoteIn, 0, amount)) // exact quote in
        } else {
            None // generic swap/route: no direction → opaque
        }
    }

    /// Record top-level programs we don't yet recognise (not Pump/Meteora, not a
    /// known/learned router, not common infra) so the background IDL learner can
    /// fetch and classify them. Bounded to keep memory flat.
    fn note_unknown_programs(&self, msg: &solana_sdk::message::VersionedMessage, full_keys: &[Pubkey]) {
        let known_router = |k: &Pubkey| {
            self.router_programs.contains(k) || self.learned_routers.read().unwrap().contains(k)
        };
        for ix in msg.instructions() {
            let program = match full_keys.get(ix.program_id_index as usize) {
                Some(p) => *p,
                None => continue,
            };
            if program == Pubkey::default()
                || program == self.pumpfun
                || program == self.meteora
                || is_infra_program(&program)
                || known_router(&program)
                || self.learned_idls.read().unwrap().contains_key(&program)
            {
                continue;
            }
            let mut pending = self.pending_programs.lock().unwrap();
            if pending.len() < 500 {
                pending.insert(program);
            }
        }
    }

    /// Background task: fetch the on-chain Anchor IDL of unknown programs seen
    /// touching our pools. If a program's IDL exposes a swap/route instruction we
    /// register it as a router (its future txs then pass the pre-filter) and cache
    /// its instruction layout for later amount decoding.
    pub fn spawn_idl_learner(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let batch: Vec<Pubkey> = {
                    let mut pending = self.pending_programs.lock().unwrap();
                    if pending.is_empty() {
                        continue;
                    }
                    let take: Vec<Pubkey> = pending.iter().copied().take(10).collect();
                    for k in &take {
                        pending.remove(k);
                    }
                    take
                };
                let rpc = self.rpc.clone();
                let me = self.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    for program in batch {
                        let idl = match crate::decoders::self_learn::fetch_program_idl(&rpc, &program)
                        {
                            Some(i) => i,
                            None => continue, // no on-chain IDL → stays Unreadable
                        };
                        let is_router = idl.by_disc.values().any(|ix| {
                            let n = ix.name.to_lowercase();
                            n.contains("swap") || n.contains("route")
                        });
                        if is_router {
                            me.learned_routers.write().unwrap().insert(program);
                            info!(%program, ixs = idl.by_disc.len(), "learned new router IDL on-chain");
                        }
                        me.learned_idls.write().unwrap().insert(program, idl);
                    }
                })
                .await;
            }
        });
    }

    /// Find a Meteora DAMM v2 `swap` OR `swap2` instruction in this tx and
    /// decode its pool (account index 1), input amount and slippage bound.
    /// `None` if the tx has no Meteora swap. We take the FIRST one — a
    /// Pump↔Meteora circular arb has exactly one Meteora leg.
    fn find_meteora_swap(
        &self,
        msg: &solana_sdk::message::VersionedMessage,
        full_keys: &[Pubkey],
    ) -> Option<MeteoraLeg> {
        for ix in msg.instructions() {
            let program = full_keys.get(ix.program_id_index as usize)?;
            if *program != self.meteora || ix.data.len() < 8 {
                continue;
            }
            let disc: [u8; 8] = ix.data[0..8].try_into().ok()?;
            // cp-amm swap accounts: [pool_authority, pool, ...]; index 1 = pool.
            let pool = match ix.accounts.get(1).and_then(|i| full_keys.get(*i as usize)) {
                Some(p) if *p != Pubkey::default() => *p,
                _ => continue,
            };
            if disc == DISC_METEORA_SWAP {
                // swap: amount_in u64@8, minimum_amount_out u64@16.
                if ix.data.len() < 24 {
                    continue;
                }
                let amount_in = u64::from_le_bytes(ix.data[8..16].try_into().ok()?);
                let min_out = u64::from_le_bytes(ix.data[16..24].try_into().ok()?);
                return Some(MeteoraLeg { pool, amount_in, min_out, exact_out: false });
            }
            if disc == DISC_METEORA_SWAP2 {
                // swap2: amount_0 u64@8, amount_1 u64@16, swap_mode u8@24.
                if ix.data.len() < 25 {
                    continue;
                }
                let amount_0 = u64::from_le_bytes(ix.data[8..16].try_into().ok()?);
                let amount_1 = u64::from_le_bytes(ix.data[16..24].try_into().ok()?);
                let mode = ix.data[24];
                // 0=ExactIn, 1=PartialFill → (amount_in, min_out) = (a0, a1).
                // 2=ExactOut → (desired_out, max_in) = (a0, a1); reverse curve.
                let exact_out = mode == 2;
                return Some(MeteoraLeg {
                    pool,
                    amount_in: amount_0,
                    min_out: amount_1,
                    exact_out,
                });
            }
        }
        None
    }

    fn resolve_keys(&self, msg: &solana_sdk::message::VersionedMessage) -> Vec<Pubkey> {
        let mut full: Vec<Pubkey> = msg.static_account_keys().to_vec();
        if let Some(lookups) = msg.address_table_lookups() {
            let mut writable = Vec::new();
            let mut readonly = Vec::new();
            let mut unknown: Vec<Pubkey> = Vec::new();
            {
                let alt_map = self.alt_map.read().unwrap();
                for lookup in lookups {
                    let alt = alt_map.get(&lookup.account_key);
                    if alt.is_none() {
                        // ALT we don't have — queue it for the fetcher so future
                        // swaps hiding a pool behind it become resolvable.
                        unknown.push(lookup.account_key);
                    }
                    for &i in &lookup.writable_indexes {
                        writable.push(
                            alt.and_then(|a| a.get(i as usize)).copied().unwrap_or_default(),
                        );
                    }
                    for &i in &lookup.readonly_indexes {
                        readonly.push(
                            alt.and_then(|a| a.get(i as usize)).copied().unwrap_or_default(),
                        );
                    }
                }
            }
            if !unknown.is_empty() {
                let mut pending = self.pending_alts.lock().unwrap();
                // Bound memory: never let the backlog grow without limit.
                if pending.len() < 5000 {
                    for k in unknown {
                        pending.insert(k);
                    }
                }
            }
            full.extend(writable);
            full.extend(readonly);
        }
        full
    }
}

/// Common Solana infrastructure programs that are never routers — skip them when
/// harvesting unknown program ids for IDL learning.
fn is_infra_program(p: &Pubkey) -> bool {
    const INFRA: &[&str] = &[
        "11111111111111111111111111111111",            // System
        "ComputeBudget111111111111111111111111111111", // Compute Budget
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  // SPL Token
        "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",  // Token-2022
        "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL", // Associated Token
        "Sysvar1111111111111111111111111111111111111",  // Sysvar
    ];
    INFRA.contains(&p.to_string().as_str())
}
