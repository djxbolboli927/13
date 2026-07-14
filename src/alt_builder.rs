//! Self-owned, self-learning Address Lookup Table.
//!
//! The tx-size problem: Metis's forced 2-hop swap references ~30 accounts, and
//! the ALTs it (or a competitor's wallet) provides only cover SOME of them, so
//! the rest stay static at 32 bytes each and the tx blows past 1232.
//!
//! The decisive fix is to stop depending on anyone else's ALT: we create ONE
//! lookup table owned by our own wallet and continuously EXTEND it with every
//! account we ever see in a Metis swap instruction. Once an account is in our
//! table, `v0::Message::try_compile` compresses it to a 1-byte index. Because a
//! hot pool fires repeatedly, the table learns its full account set within a
//! slot or two and every subsequent tx for it fits — and keeps fitting as we add
//! more hops/venues later (they just contribute more accounts to the same table).
//!
//! Learning is off the hot path: `note()` only pushes unseen pubkeys onto a
//! queue; a background task batches them into `extend_lookup_table` transactions
//! (≤ `EXTEND_BATCH` per tx, one table holds up to 256 addresses; a new table is
//! created when the current one fills). The in-memory `tables()` snapshot is what
//! the tx builder folds in, so it's always consistent with what we've committed.

use dashmap::DashSet;
use solana_client::rpc_client::RpcClient;
use solana_sdk::address_lookup_table::instruction::{create_lookup_table, extend_lookup_table};
use solana_sdk::address_lookup_table::AddressLookupTableAccount;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::transaction::Transaction;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tracing::{info, warn};

/// Max addresses per `extend_lookup_table` (keeps the extend tx itself small).
const EXTEND_BATCH: usize = 20;
/// A single ALT tops out at 256 addresses; leave headroom.
const MAX_PER_TABLE: usize = 250;

pub struct AltBuilder {
    /// Accounts already committed to (or queued for) a table — the hot-path guard.
    seen: DashSet<Pubkey>,
    /// Pending accounts waiting to be committed by the flusher.
    pending: Mutex<VecDeque<Pubkey>>,
    /// Snapshot of our tables (key + current addresses) for the tx builder.
    tables: RwLock<Vec<AddressLookupTableAccount>>,
}

impl AltBuilder {
    /// Create the builder and spawn its background flusher. `store_path` persists
    /// the table pubkey(s) so a restart reuses the same on-chain table(s) instead
    /// of leaking rent on new ones.
    pub fn spawn(rpc: Arc<RpcClient>, keypair: Arc<Keypair>, store_path: String) -> Arc<Self> {
        let me = Arc::new(Self {
            seen: DashSet::new(),
            pending: Mutex::new(VecDeque::new()),
            tables: RwLock::new(Vec::new()),
        });
        let me2 = me.clone();
        tokio::spawn(async move {
            me2.run(rpc, keypair, store_path).await;
        });
        me
    }

    /// Record accounts we might want compressed. Cheap: only unseen pubkeys are
    /// enqueued; everything else is a lock-free set hit.
    pub fn note<I: IntoIterator<Item = Pubkey>>(&self, accounts: I) {
        let mut q = self.pending.lock().unwrap();
        for a in accounts {
            if self.seen.insert(a) {
                q.push_back(a);
            }
        }
    }

    /// Current tables to fold into a tx (their in-memory address lists).
    pub fn tables(&self) -> Vec<AddressLookupTableAccount> {
        self.tables.read().unwrap().clone()
    }

    async fn run(self: Arc<Self>, rpc: Arc<RpcClient>, keypair: Arc<Keypair>, store_path: String) {
        // Load any persisted tables and their on-chain addresses.
        self.load_persisted(&rpc, &store_path).await;
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        loop {
            ticker.tick().await;
            // Drain a batch.
            let batch: Vec<Pubkey> = {
                let mut q = self.pending.lock().unwrap();
                let n = q.len().min(EXTEND_BATCH);
                q.drain(..n).collect()
            };
            if batch.is_empty() {
                continue;
            }
            let me = self.clone();
            let rpc = rpc.clone();
            let kp = keypair.clone();
            let store = store_path.clone();
            let batch2 = batch.clone();
            let ok = tokio::task::spawn_blocking(move || me.commit_batch(&rpc, &kp, &store, &batch2))
                .await
                .unwrap_or(false);
            if !ok {
                // Extend failed — forget so a later note() re-enqueues them.
                for a in &batch {
                    self.seen.remove(a);
                }
            }
        }
    }

    /// Blocking: append `batch` to the current table (creating one first if
    /// needed / rotating when full). Returns true on success.
    fn commit_batch(
        &self,
        rpc: &RpcClient,
        keypair: &Keypair,
        store_path: &str,
        batch: &[Pubkey],
    ) -> bool {
        // Which table has room?
        let table_key = match self.current_open_table(rpc, keypair, store_path) {
            Some(k) => k,
            None => return false,
        };
        let ix = extend_lookup_table(
            table_key,
            keypair.pubkey(),
            Some(keypair.pubkey()),
            batch.to_vec(),
        );
        let bh = match rpc.get_latest_blockhash() {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "alt: blockhash for extend failed");
                return false;
            }
        };
        let tx = Transaction::new_signed_with_payer(
            &[ix],
            Some(&keypair.pubkey()),
            &[keypair],
            bh,
        );
        match rpc.send_and_confirm_transaction(&tx) {
            Ok(_) => {
                // Reflect the new addresses in the in-memory snapshot.
                let mut tables = self.tables.write().unwrap();
                if let Some(t) = tables.iter_mut().find(|t| t.key == table_key) {
                    t.addresses.extend_from_slice(batch);
                }
                info!(table = %table_key, added = batch.len(), "alt extended");
                true
            }
            Err(e) => {
                warn!(error = %e, "alt: extend_lookup_table failed");
                false
            }
        }
    }

    /// Return a table with room for more addresses, creating a fresh one when the
    /// last is full or none exists yet.
    fn current_open_table(
        &self,
        rpc: &RpcClient,
        keypair: &Keypair,
        store_path: &str,
    ) -> Option<Pubkey> {
        {
            let tables = self.tables.read().unwrap();
            if let Some(t) = tables.last() {
                if t.addresses.len() < MAX_PER_TABLE {
                    return Some(t.key);
                }
            }
        }
        // Need a new table.
        let recent_slot = rpc
            .get_slot_with_commitment(CommitmentConfig::finalized())
            .ok()?;
        let (ix, table_key) = create_lookup_table(keypair.pubkey(), keypair.pubkey(), recent_slot);
        let bh = rpc.get_latest_blockhash().ok()?;
        let tx =
            Transaction::new_signed_with_payer(&[ix], Some(&keypair.pubkey()), &[keypair], bh);
        match rpc.send_and_confirm_transaction(&tx) {
            Ok(_) => {
                info!(table = %table_key, "alt created");
                self.tables.write().unwrap().push(AddressLookupTableAccount {
                    key: table_key,
                    addresses: Vec::new(),
                });
                self.persist(store_path);
                Some(table_key)
            }
            Err(e) => {
                warn!(error = %e, "alt: create_lookup_table failed");
                None
            }
        }
    }

    async fn load_persisted(&self, rpc: &Arc<RpcClient>, store_path: &str) {
        let content = match std::fs::read_to_string(store_path) {
            Ok(c) => c,
            Err(_) => return,
        };
        let keys: Vec<Pubkey> = content
            .lines()
            .filter_map(|l| l.trim().parse::<Pubkey>().ok())
            .collect();
        for key in keys {
            let rpc = rpc.clone();
            let fetched = tokio::task::spawn_blocking(move || {
                rpc.get_account(&key).ok().and_then(|acc| {
                    solana_sdk::address_lookup_table::state::AddressLookupTable::deserialize(
                        &acc.data,
                    )
                    .ok()
                    .map(|t| t.addresses.to_vec())
                })
            })
            .await
            .ok()
            .flatten();
            if let Some(addresses) = fetched {
                for a in &addresses {
                    self.seen.insert(*a);
                }
                self.tables
                    .write()
                    .unwrap()
                    .push(AddressLookupTableAccount { key, addresses });
                info!(table = %key, "alt loaded from store");
            }
        }
    }

    fn persist(&self, store_path: &str) {
        let tables = self.tables.read().unwrap();
        let body: String = tables
            .iter()
            .map(|t| t.key.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        if let Some(dir) = std::path::Path::new(store_path).parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        if let Err(e) = std::fs::write(store_path, body) {
            warn!(error = %e, path = store_path, "alt: persist failed");
        }
    }
}
