//! Per-transaction ALT selector, backed by the global library of PUBLIC lookup
//! tables harvested from the network (shred_stream's `alt_map`).
//!
//! On-chain evidence: competitors do NOT mint their own tables for fresh pools.
//! They keep a library of pre-existing public ALTs (built months ago by other
//! wallets) and, for each transaction, reference the 1-3 tables that best cover
//! that route's accounts — different combinations per tx. We do exactly the
//! same: for every tx we build, we look at the REAL account list Metis emitted
//! and greedily pick the fewest public tables that compress the most accounts.
//!
//! Why greedy set-cover with a marginal-gain floor: every extra ALT referenced
//! in a v0 message costs ~34 fixed bytes (its 32-byte pubkey + 2 length bytes),
//! while each account it moves out of the static list saves 31 bytes (32 → 1).
//! So a table is only worth adding if it uniquely covers ≥ 2 still-uncovered
//! accounts (34 − 2·31 < 0). Greedy also dedupes automatically: a table
//! identical to (or subsumed by) one already chosen has 0 marginal gain and is
//! skipped.

use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};

/// A table is only worth attaching if it uniquely covers at least this many of
/// the still-uncovered route accounts (fixed ~34-byte cost vs 31-byte/account
/// saving ⇒ break-even is 2).
const MIN_MARGINAL_COVERAGE: usize = 2;

pub struct AltRegistry {
    /// The live global library shared with the shred consumer: ALT key → members.
    library: Arc<RwLock<HashMap<Pubkey, Vec<Pubkey>>>>,
}

impl AltRegistry {
    pub fn new(library: Arc<RwLock<HashMap<Pubkey, Vec<Pubkey>>>>) -> Arc<Self> {
        Arc::new(Self { library })
    }

    /// Pick up to `max` public ALTs from the library that together compress the
    /// most of `needed` (the real accounts in this tx's Metis instruction).
    /// Greedy by marginal coverage; stops when the best remaining table would
    /// cover fewer than `MIN_MARGINAL_COVERAGE` new accounts (not worth the
    /// per-ALT byte overhead). Returned keys are distinct and never redundant.
    pub fn select(&self, needed: &HashSet<Pubkey>, max: usize) -> Vec<Pubkey> {
        if needed.is_empty() || max == 0 {
            return Vec::new();
        }
        let lib = self.library.read().unwrap();

        // Snapshot each library table's coverage of `needed` (only tables that
        // cover ≥ the floor are even candidates).
        let mut candidates: Vec<(Pubkey, Vec<Pubkey>)> = lib
            .iter()
            .filter_map(|(key, members)| {
                let covered: Vec<Pubkey> = members
                    .iter()
                    .filter(|a| needed.contains(*a))
                    .copied()
                    .collect();
                if covered.len() >= MIN_MARGINAL_COVERAGE {
                    Some((*key, covered))
                } else {
                    None
                }
            })
            .collect();
        drop(lib);

        let mut remaining: HashSet<Pubkey> = needed.clone();
        let mut chosen: Vec<Pubkey> = Vec::with_capacity(max);
        while chosen.len() < max && !candidates.is_empty() {
            // Best candidate by CURRENT marginal gain against `remaining`.
            let mut best_idx: Option<usize> = None;
            let mut best_gain = 0usize;
            for (i, (_, covered)) in candidates.iter().enumerate() {
                let gain = covered.iter().filter(|a| remaining.contains(*a)).count();
                if gain > best_gain {
                    best_gain = gain;
                    best_idx = Some(i);
                }
            }
            // Nothing left worth the ~34-byte cost of another ALT → stop.
            if best_gain < MIN_MARGINAL_COVERAGE {
                break;
            }
            let (key, covered) = candidates.swap_remove(best_idx.unwrap());
            for a in &covered {
                remaining.remove(a);
            }
            chosen.push(key);
        }
        chosen
    }
}
