//! DFlow Aggregator v4 — `DF1ow4tspfHX9JwWJsAb9epbkA8hmpSEAtxXy1V27QBH`.
//!
//! Anchor-style aggregator; CPIs into the underlying DEX, so a watched pool
//! touched via DFlow appears in the flattened account keys and is enqueued as
//! `Unreadable`. Source: Solana Explorer + Solana Compass.
//!
//! Caveat (from DFlow docs): integrators are told NOT to hardcode the "Swap
//! Orchestrator" program id because it can rotate / is returned by their API, so
//! auxiliary program ids may exist beyond this stable v4 aggregator. If unmatched
//! DFlow-shaped txns appear, capture and add them here.

/// DFlow Aggregator v4.
pub const PROGRAM: &str = "DF1ow4tspfHX9JwWJsAb9epbkA8hmpSEAtxXy1V27QBH";
