//! Self-learning decoder: fetch a program's Anchor IDL FROM THE CHAIN and build
//! an instruction/argument map automatically, so a NEW aggregator (DFlow, Titan,
//! any future router) is supported without hand-writing a decoder file.
//!
//! ─── How it works ──────────────────────────────────────────────────────────
//! Anchor programs publish their IDL on-chain at a deterministic PDA. The account
//! holds a zlib-compressed JSON IDL. From it we derive, for every instruction:
//!   * the 8-byte discriminator  = sha256("global:<name>")[..8]
//!   * the ordered argument list (name + type)
//! and a generic Borsh walker reads the LEADING fixed-size scalar args by name
//! (`in_amount`, `amount_in`, `slippage_bps`, …). That covers scalar-first
//! layouts (OKX-style) directly; layouts whose first field is a Vec/enum stop at
//! that field (the value isn't at a fixed offset) and the caller falls back to
//! the hand-written decoders or to Unreadable.
//!
//! The map key is ALWAYS `(program_id, discriminator)` — two programs can share a
//! discriminator (e.g. OKX `swap` and Meteora `swap2`), so the discriminator
//! alone is not unique.
//!
//! This module is split into a PURE core (discriminator, JSON→layout, Borsh
//! scalar walker, alias lookup — all unit-tested offline) and a thin on-chain
//! fetch (`fetch_program_idl`) that needs an RPC client.

use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;

/// Anchor instruction discriminator = first 8 bytes of sha256("global:<name>").
/// Solana's `Hash` IS sha256, so we reuse it (no extra sha2 dependency).
pub fn anchor_discriminator(name: &str) -> [u8; 8] {
    let h = solana_sdk::hash::hash(format!("global:{name}").as_bytes());
    let mut out = [0u8; 8];
    out.copy_from_slice(&h.to_bytes()[..8]);
    out
}

/// The Borsh-relevant shape of one IDL argument type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdlType {
    U8,
    U16,
    U32,
    U64,
    U128,
    Bool,
    Pubkey,
    /// A variable-length or composite type (Vec/defined/option/array/string):
    /// we cannot compute the offset of anything after it, so the scalar walk
    /// stops here.
    Variable,
}

impl IdlType {
    fn fixed_size(self) -> Option<usize> {
        match self {
            IdlType::U8 | IdlType::Bool => Some(1),
            IdlType::U16 => Some(2),
            IdlType::U32 => Some(4),
            IdlType::U64 => Some(8),
            IdlType::U128 => Some(16),
            IdlType::Pubkey => Some(32),
            IdlType::Variable => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub ty: IdlType,
}

#[derive(Debug, Clone)]
pub struct IxDef {
    pub name: String,
    pub args: Vec<Field>,
}

/// A learned program: instructions keyed by discriminator.
#[derive(Debug, Clone, Default)]
pub struct ProgramIdl {
    pub by_disc: HashMap<[u8; 8], IxDef>,
}

/// Map an IDL JSON type node to our Borsh shape.
fn map_type(ty: &serde_json::Value) -> IdlType {
    match ty {
        serde_json::Value::String(s) => match s.as_str() {
            "u8" => IdlType::U8,
            "u16" => IdlType::U16,
            "u32" => IdlType::U32,
            "u64" => IdlType::U64,
            "u128" => IdlType::U128,
            "bool" => IdlType::Bool,
            "publicKey" | "pubkey" => IdlType::Pubkey,
            _ => IdlType::Variable, // i*, f*, string, etc. — treat as opaque
        },
        // { "vec": … } / { "defined": … } / { "option": … } / { "array": … }
        _ => IdlType::Variable,
    }
}

/// Parse an Anchor IDL JSON document into a discriminator map.
pub fn parse_idl_json(json: &[u8]) -> Option<ProgramIdl> {
    let root: serde_json::Value = serde_json::from_slice(json).ok()?;
    let ixs = root.get("instructions")?.as_array()?;
    let mut by_disc = HashMap::new();
    for ix in ixs {
        let name = ix.get("name")?.as_str()?.to_string();
        let mut args = Vec::new();
        if let Some(arr) = ix.get("args").and_then(|a| a.as_array()) {
            for a in arr {
                let fname = match a.get("name").and_then(|n| n.as_str()) {
                    Some(n) => n.to_string(),
                    None => continue,
                };
                let ty = a.get("type").map(map_type).unwrap_or(IdlType::Variable);
                args.push(Field { name: fname, ty });
            }
        }
        by_disc.insert(anchor_discriminator(&name), IxDef { name, args });
    }
    Some(ProgramIdl { by_disc })
}

/// Walk the LEADING fixed-size scalar args of an instruction, returning their
/// values by field name. Stops at the first variable/composite field (its
/// successors have no fixed offset). `data` includes the 8-byte discriminator.
pub fn extract_leading_scalars(data: &[u8], ix: &IxDef) -> HashMap<String, u128> {
    let mut out = HashMap::new();
    let mut off = 8usize; // past the discriminator
    for f in &ix.args {
        let size = match f.ty.fixed_size() {
            Some(s) => s,
            None => break, // variable field → cannot address anything after it
        };
        let bytes = match data.get(off..off + size) {
            Some(b) => b,
            None => break,
        };
        match f.ty {
            IdlType::Pubkey => {} // fixed size, but not a scalar value to record
            IdlType::U8 | IdlType::Bool => {
                out.insert(f.name.clone(), bytes[0] as u128);
            }
            IdlType::U16 => {
                out.insert(f.name.clone(), u16::from_le_bytes(bytes.try_into().unwrap()) as u128);
            }
            IdlType::U32 => {
                out.insert(f.name.clone(), u32::from_le_bytes(bytes.try_into().unwrap()) as u128);
            }
            IdlType::U64 => {
                out.insert(f.name.clone(), u64::from_le_bytes(bytes.try_into().unwrap()) as u128);
            }
            IdlType::U128 => {
                out.insert(f.name.clone(), u128::from_le_bytes(bytes.try_into().unwrap()));
            }
            IdlType::Variable => break,
        }
        off += size;
    }
    out
}

/// Field-name aliases the router world uses for the trade INPUT amount.
const AMOUNT_IN_ALIASES: &[&str] = &["in_amount", "amount_in", "amount", "input_amount"];
/// Aliases for the user's slippage / minimum-out bound.
const SLIPPAGE_ALIASES: &[&str] =
    &["slippage_bps", "slippage", "min_out", "minimum_amount_out", "min_return"];

fn first_alias(map: &HashMap<String, u128>, aliases: &[&str]) -> Option<u128> {
    aliases.iter().find_map(|k| map.get(*k).copied())
}

/// The route input amount, by any known alias.
pub fn amount_in(map: &HashMap<String, u128>) -> Option<u64> {
    first_alias(map, AMOUNT_IN_ALIASES).and_then(|v| u64::try_from(v).ok())
}

/// The slippage / min-out bound, by any known alias.
pub fn slippage(map: &HashMap<String, u128>) -> Option<u64> {
    first_alias(map, SLIPPAGE_ALIASES).and_then(|v| u64::try_from(v).ok())
}

/// Derive the on-chain Anchor IDL account address for a program.
pub fn idl_address(program_id: &Pubkey) -> Option<Pubkey> {
    // Anchor: base = PDA(program_id, []), idl = create_with_seed(base, "anchor:idl", program_id).
    let (base, _) = Pubkey::find_program_address(&[], program_id);
    Pubkey::create_with_seed(&base, "anchor:idl", program_id).ok()
}

/// Inflate the zlib-compressed JSON out of an Anchor IDL account's raw data.
/// Layout: [8 disc][32 authority][4 data_len LE][zlib bytes].
pub fn inflate_idl_account(data: &[u8]) -> Option<Vec<u8>> {
    let len = u32::from_le_bytes(data.get(40..44)?.try_into().ok()?) as usize;
    let comp = data.get(44..44 + len)?;
    let mut dec = flate2::read::ZlibDecoder::new(comp);
    let mut out = Vec::new();
    std::io::Read::read_to_end(&mut dec, &mut out).ok()?;
    Some(out)
}

/// Fetch a program's IDL from the chain and parse it. `None` if the program has
/// no on-chain IDL (many don't) — the caller then keeps the tx Unreadable.
pub fn fetch_program_idl(
    rpc: &solana_client::rpc_client::RpcClient,
    program_id: &Pubkey,
) -> Option<ProgramIdl> {
    let addr = idl_address(program_id)?;
    let acct = rpc.get_account(&addr).ok()?;
    let json = inflate_idl_account(&acct.data)?;
    parse_idl_json(&json)
}

#[cfg(test)]
mod tests {
    use super::*;

    // The self-derived Anchor discriminator must match the known on-chain bytes
    // — this also proves solana_sdk::hash is sha256, the whole basis of the
    // self-learner.
    #[test]
    fn discriminator_matches_known() {
        assert_eq!(
            anchor_discriminator("route"),
            [229, 23, 203, 151, 122, 227, 173, 42]
        );
        assert_eq!(
            anchor_discriminator("sell"),
            [51, 230, 133, 164, 1, 127, 131, 173]
        );
        assert_eq!(
            anchor_discriminator("swap"),
            [248, 198, 158, 145, 225, 117, 135, 200]
        );
    }

    // Parse a minimal IDL and extract a scalar-first layout by name.
    #[test]
    fn parse_and_extract_scalar_first() {
        let json = br#"{
            "instructions": [
                { "name": "swap", "args": [
                    { "name": "amount_in", "type": "u64" },
                    { "name": "expect_amount_out", "type": "u64" },
                    { "name": "slippage", "type": "u16" },
                    { "name": "routes", "type": { "vec": { "defined": "Route" } } }
                ]}
            ]
        }"#;
        let idl = parse_idl_json(json).expect("parse");
        let disc = anchor_discriminator("swap");
        let ix = idl.by_disc.get(&disc).expect("swap ix");
        // build data: disc + amount_in + expect + slippage + (routes vec unread)
        let mut data = Vec::new();
        data.extend_from_slice(&disc);
        data.extend_from_slice(&5_390_846_552_666u64.to_le_bytes());
        data.extend_from_slice(&4_270_594_256u64.to_le_bytes());
        data.extend_from_slice(&1140u16.to_le_bytes());
        data.extend_from_slice(&99u32.to_le_bytes()); // start of routes vec — ignored
        let scalars = extract_leading_scalars(&data, ix);
        assert_eq!(amount_in(&scalars), Some(5_390_846_552_666));
        assert_eq!(slippage(&scalars), Some(1140));
    }

    // A Vec-first layout (Jupiter route) yields no leading scalars — the walker
    // stops at the first variable field, so amount_in is not falsely read.
    #[test]
    fn vec_first_layout_stops() {
        let json = br#"{
            "instructions": [
                { "name": "route", "args": [
                    { "name": "route_plan", "type": { "vec": { "defined": "RoutePlanStep" } } },
                    { "name": "in_amount", "type": "u64" }
                ]}
            ]
        }"#;
        let idl = parse_idl_json(json).unwrap();
        let ix = idl.by_disc.get(&anchor_discriminator("route")).unwrap();
        let mut data = Vec::new();
        data.extend_from_slice(&anchor_discriminator("route"));
        data.extend_from_slice(&7u32.to_le_bytes()); // route_plan len
        let scalars = extract_leading_scalars(&data, ix);
        assert!(amount_in(&scalars).is_none()); // never guessed past the Vec
    }

    // Pubkey leading fields are skipped in size but not recorded as values.
    #[test]
    fn pubkey_prefix_is_skipped() {
        let json = br#"{
            "instructions": [
                { "name": "swap", "args": [
                    { "name": "authority", "type": "pubkey" },
                    { "name": "amount_in", "type": "u64" }
                ]}
            ]
        }"#;
        let idl = parse_idl_json(json).unwrap();
        let ix = idl.by_disc.get(&anchor_discriminator("swap")).unwrap();
        let mut data = Vec::new();
        data.extend_from_slice(&anchor_discriminator("swap"));
        data.extend_from_slice(&[7u8; 32]); // authority pubkey
        data.extend_from_slice(&12345u64.to_le_bytes()); // amount_in
        let scalars = extract_leading_scalars(&data, ix);
        assert_eq!(amount_in(&scalars), Some(12345));
    }

    // Round-trip an IDL JSON through zlib to prove inflate_idl_account works.
    #[test]
    fn inflate_round_trip() {
        use std::io::Write;
        let json = br#"{"instructions":[{"name":"swap","args":[]}]}"#;
        let mut enc = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(json).unwrap();
        let comp = enc.finish().unwrap();
        // Build a fake account: 8 disc + 32 authority + 4 len + comp.
        let mut acct = vec![0u8; 8 + 32];
        acct.extend_from_slice(&(comp.len() as u32).to_le_bytes());
        acct.extend_from_slice(&comp);
        let out = inflate_idl_account(&acct).expect("inflate");
        assert_eq!(out, json);
        assert!(parse_idl_json(&out).is_some());
    }
}
