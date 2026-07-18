//! Per-transaction ALT selector, backed by the global library of PUBLIC lookup
//! tables harvested from the network (shred_stream's `alt_map`).
//!
//! Competitors don't mint tables for fresh pools — they reuse public ALTs seen
//! on-chain and reference, per transaction, the tables that best cover that
//! route. We do the same: for every tx we build, take the REAL account list the
//! Metis instruction carries and greedily pick the best-covering tables from
//! the library (max one per leg, never the same table twice).
//!
//! Greedy set-cover with a marginal-gain floor: an extra ALT costs ~34 fixed
//! bytes while each compressed account saves 31 (32 → 1), so a table is only
//! worth adding if it uniquely covers ≥ 2 still-uncovered accounts. Greedy also
//! dedupes automatically: a table identical to (or subsumed by) one already
//! chosen has zero marginal gain and is skipped — no duplicate ALTs in one tx.

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
    /// Greedy by marginal coverage: the first pick covers the most accounts
    /// (one leg + shared programs), the second covers the most of what REMAINS
    /// (the other leg) — i.e. one best table per leg, and a duplicate/subsumed
    /// table can never be picked twice. Stops when the best remaining table
    /// would cover fewer than `MIN_MARGINAL_COVERAGE` new accounts.
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
