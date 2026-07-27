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
#[derive(Default)]
struct BlockTracker {
    /// slot → (global order_seq, sig) of every watched-pool tx seen in it.
    open: std::collections::BTreeMap<u64, Vec<(u64, solana_sdk::signature::Signature)>>,
    /// Completed blocks waiting to be printed (flushed in batches of 10).
    done: Vec<(u64, Vec<(u64, solana_sdk::signature::Signature)>)>,
}

impl BlockTracker {
    fn record(&mut self, slot: u64, order_seq: u64, sig: solana_sdk::signature::Signature) {
        self.open.entry(slot).or_default().push((order_seq, sig));
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
        // Print once per 10 completed blocks.
        while self.done.len() >= 10 {
            let batch: Vec<(u64, Vec<(u64, solana_sdk::signature::Signature)>)> =
                self.done.drain(..10).collect();
            let mut out =
                String::from("[block-read] last 10 blocks — watched-pool txs seen, in PoH order:");
            for (s, mut txs) in batch {
                txs.sort_by_key(|(o, _)| *o); // true leader order, not arrival order
                out.push_str(&format!("\nblok:{}  count={}", s, txs.len()));
                for (i, (oseq, sig)) in txs.iter().enumerate() {
                    let h = sig.to_string();
                    let head = &h[..h.len().min(5)];
                    out.push_str(&format!("\n  {}:{} (seq {})", i + 1, head, oseq));
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
            metrics,
        }
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
    pub fn spawn_alt_fetcher(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                // Drain up to N unknown ALTs (skip ones already known).
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
                        .take(25)
                        .collect();
                    for k in &take {
                        pending.remove(k);
                    }
                    take
                };
                if batch.is_empty() {
                    continue;
                }
                let rpc = self.rpc.clone();
                let me = self.clone();
                // Blocking RPC off the async worker.
                let _ = tokio::task::spawn_blocking(move || {
                    let mut added = 0usize;
                    for alt in batch {
                        if let Ok(acct) = rpc.get_account(&alt) {
                            if let Ok(addrs) =
                                crate::transaction::deserialize_alt_addresses(&acct.data)
                            {
                                me.note_alt_members(alt, &addrs);
                                me.alt_map.write().unwrap().insert(alt, addrs);
                                added += 1;
                            }
                        }
                    }
                    if added > 0 {
                        info!(added, "shred ALT cache learned new lookup tables");
                    }
                })
                .await;
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
        let mut set = self.target_pools.write().unwrap();
        if set.insert(pool) {
            self.metrics
                .watched_pools
                .store(set.len() as u64, Ordering::Relaxed);
        }
        if let Some((alt_key, addrs)) = alt {
            self.note_alt_members(alt_key, &addrs);
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
                    self.scan_tx(slot, order_seq, vtx, tx);
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

        // Cheap pre-filter: process the tx if a program we can act on appears as
        // a STATIC key — either the Pump/Meteora AMM itself (native swap), or a
        // KNOWN router (Jupiter/OKX/DFlow) that CPIs into our pools with the AMM
        // program ALT-hidden. Without the router clause every aggregator tx on a
        // watched pool was dropped here, leaving holes in the ordered sequence.
        let touches_known_program = static_keys.iter().any(|k| {
            *k == self.pumpfun || *k == self.meteora || self.router_programs.contains(k)
        });
        // Cheap private-bot catch: does this tx reference an ALT we know holds a
        // watched pool? (Set lookup per ALT key — no full resolution.)
        let uses_pool_alt = || {
            let palts = self.pool_alts.read().unwrap();
            if palts.is_empty() {
                return false;
            }
            msg.address_table_lookups()
                .map(|ls| ls.iter().any(|l| palts.contains(&l.account_key)))
                .unwrap_or(false)
        };
        if !touches_known_program && !uses_pool_alt() {
            return;
        }
        self.metrics.pump_txns.fetch_add(1, Ordering::Relaxed);

        // Resolve the full ordered account list (static + ALT writable + ALT
        // readonly), filling unknown-ALT slots with a placeholder.
        let full_keys = self.resolve_keys(msg);

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
                    .record(slot, order_seq, tx_sig);
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
            let mut first_hop = None;
            for ix in msg.instructions() {
                let program = match full_keys.get(ix.program_id_index as usize) {
                    Some(p) => *p,
                    None => continue,
                };
                if self.router_programs.contains(&program) {
                    if let Some(h) = crate::decoders::route_decode::decode_first_hop(&ix.data) {
                        first_hop = Some(h);
                        break;
                    }
                }
            }
            if let Some(hop) = first_hop {
                // target_pools holds only Pump pools, and the decoded first hop is
                // a Pump sell, so a single watched pool present IS that sell pool.
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
                    self.block_log.lock().unwrap().record(slot, order_seq, tx_sig);
                    let signal = PumpSwapSignal {
                        pool,
                        kind: PumpIxKind::Sell,
                        base_amount: hop.base_amount_in,
                        quote_amount: 0,
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
        {
            let targets = self.target_pools.read().unwrap();
            for key in &full_keys {
                if *key == Pubkey::default() || !targets.contains(key) {
                    continue;
                }
                if decoded_pools.contains(key) {
                    continue; // this pool's swap was decoded above
                }
                self.metrics.opaque_matched.fetch_add(1, Ordering::Relaxed);
                // Opaque txs still TOUCH the pool (in PoH order) — record them so
                // the per-block diagnostic counts them and the sequence has no gap.
                self.block_log
                    .lock()
                    .unwrap()
                    .record(slot, order_seq, tx_sig);
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
