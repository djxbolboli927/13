//! Byte-level decoders for aggregator/router TOP-LEVEL instructions, so a swap
//! routed through Jupiter or OKX on a watched pool is PRICED from the shred
//! instead of only waited on. Verified against the current on-chain IDLs
//! (sevenlabs-hq/carbon, Codama-generated) — see the per-program files for the
//! discriminators and enum indices.
//!
//! What we can safely recover from RAW shred bytes: the route's overall
//! `in_amount` and the FIRST hop's venue + direction. The first hop consumes the
//! whole route input, so when it is a Pump.fun-AMM **sell** on a watched pool we
//! know `base_amount_in == in_amount` exactly (confirmed against real txs:
//! Jupiter route ANSEMOTHY sell in_amount == inner sellEvent.baseAmountIn; OKX
//! swap ANSEMOTHY sell amount_in == inner sellEvent.baseAmountIn).
//!
//! We deliberately decode ONLY the clean, unambiguous first-hop Pump-sell case;
//! anything else (buy first-hop, split routes, our pool as a later hop, exotic
//! variants) returns `None` and the caller falls back to `Unreadable` (wait for
//! the account-update) — never a guessed amount.

/// Jupiter v6 `route` discriminator. args: route_plan Vec, in_amount u64, … .
const JUP_ROUTE: [u8; 8] = [229, 23, 203, 151, 122, 227, 173, 42];
/// Jupiter v6 `route_v2` discriminator (route_plan is the LAST field here).
const JUP_ROUTE_V2: [u8; 8] = [187, 100, 250, 204, 49, 196, 175, 20];
/// `shared_accounts_route` — the most common form today. args: id u8, route_plan
/// Vec, in_amount u64, quoted_out u64, slippage u16, platform_fee u8. Same as
/// `route` but with a 1-byte `id` prefix, so route_plan starts at offset 9.
const JUP_SHARED_ROUTE: [u8; 8] = [193, 32, 155, 51, 65, 214, 156, 129];
/// `*_with_token_ledger` variants carry NO in_amount (input comes from a token
/// ledger set by a prior `set_token_ledger` ix), and `exact_out` variants price
/// the OUTPUT not the input — neither gives us a first-hop base_amount_in from
/// bytes alone, so we recognise them only to leave them Unreadable (wait).
const JUP_ROUTE_WITH_LEDGER: [u8; 8] = [150, 86, 71, 116, 167, 93, 14, 104];
const JUP_SHARED_ROUTE_WITH_LEDGER: [u8; 8] = [230, 121, 143, 80, 119, 159, 106, 170];
const JUP_EXACT_OUT_ROUTE: [u8; 8] = [208, 51, 239, 151, 123, 43, 237, 92];
const JUP_SHARED_EXACT_OUT_ROUTE: [u8; 8] = [176, 209, 105, 168, 154, 125, 69, 62];

/// Jupiter `Swap` enum tags that mean a Pump.fun-AMM SELL (all generations).
const JUP_PUMP_SELL_TAGS: [u8; 3] = [73, 93, 100]; // PumpSwapSell / V2 / V3
/// Jupiter `Swap` enum tags that mean a Pump.fun-AMM BUY (all generations) —
/// used only to REJECT a route that has a second Pump hop (so we never mis-map
/// a first-hop sell amount to a different watched Pump pool later in the route).
const JUP_PUMP_ANY_TAGS: [u8; 6] = [72, 73, 92, 93, 99, 100];
/// Jupiter `Swap` tags for Meteora DAMM v2 (a safe, non-Pump second hop).
const JUP_METEORA_TAGS: [u8; 2] = [77, 108];

/// OKX router `swap` / `proxy_swap` / `swap_tob_v3` discriminators. All wrap a
/// `SwapArgs` whose first field `amount_in` sits at args offset 0.
const OKX_SWAP: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];
const OKX_PROXY_SWAP: [u8; 8] = [19, 44, 130, 148, 72, 56, 44, 238];
const OKX_SWAP_TOB_V3: [u8; 8] = [14, 191, 44, 246, 142, 225, 224, 157];

/// OKX `Dex` enum tags that mean a Pump.fun-AMM SELL.
const OKX_PUMP_SELL_TAGS: [u8; 3] = [35, 74, 85]; // PumpfunammSell / Sell3 / Sell2

fn u64_le(d: &[u8], off: usize) -> Option<u64> {
    d.get(off..off + 8)?.try_into().ok().map(u64::from_le_bytes)
}
fn u32_le(d: &[u8], off: usize) -> Option<u32> {
    d.get(off..off + 4)?.try_into().ok().map(u32::from_le_bytes)
}

/// The one thing we decode from a router tx: the first hop is a Pump sell whose
/// base input equals the route input. `None` = not this clean case → Unreadable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PumpSellFirstHop {
    /// `base_amount_in` for the Pump `sell` on the watched pool.
    pub base_amount_in: u64,
}

/// Walk a v1 `route_plan` Vec<RoutePlanStep> starting at `start` (the u32 len).
/// Every step is `swap(1-byte tag, ZERO payload) + percent u8 + input u8 +
/// output u8 = 4 bytes` for our TRUSTED venues; a tag outside that set may carry
/// a variable payload we can't size, so we bail (→ Unreadable). Returns
/// (first_tag, first_percent, pump_step_count, offset AFTER the vec).
fn walk_v1_route_plan(data: &[u8], start: usize) -> Option<(u8, u8, usize, usize)> {
    let vec_len = u32_le(data, start)? as usize;
    if vec_len == 0 {
        return None;
    }
    let mut off = start + 4;
    let mut first: Option<(u8, u8)> = None;
    let mut pump_steps = 0usize;
    for i in 0..vec_len {
        let tag = *data.get(off)?;
        // Only trust venues we KNOW are zero-payload (Pump + Meteora). Anything
        // else could carry a payload → we can't reach in_amount safely → bail.
        if !JUP_PUMP_ANY_TAGS.contains(&tag) && !JUP_METEORA_TAGS.contains(&tag) {
            return None;
        }
        if JUP_PUMP_ANY_TAGS.contains(&tag) {
            pump_steps += 1;
        }
        let percent = *data.get(off + 1)?;
        data.get(off + 2)?; // input_index
        data.get(off + 3)?; // output_index
        if i == 0 {
            first = Some((tag, percent));
        }
        off += 4;
    }
    let (t, p) = first?;
    Some((t, p, pump_steps, off))
}

/// The v1 first-hop-sell case shared by `route` (route_plan @8) and
/// `shared_accounts_route` (id u8 @8, route_plan @9).
fn jupiter_v1_first_hop(data: &[u8], plan_start: usize) -> Option<PumpSellFirstHop> {
    let (tag, percent, pump_steps, end) = walk_v1_route_plan(data, plan_start)?;
    // First hop must be a 100% Pump sell, and it must be the ONLY Pump pool in
    // the route (else a lone watched address can't be attributed to it).
    if !JUP_PUMP_SELL_TAGS.contains(&tag) || percent != 100 || pump_steps != 1 {
        return None;
    }
    let base_amount_in = u64_le(data, end)?; // in_amount immediately after the vec
    Some(PumpSellFirstHop { base_amount_in })
}

/// Decode a Jupiter v6 routing instruction's first hop (all forms).
pub fn decode_jupiter(data: &[u8]) -> Option<PumpSellFirstHop> {
    let disc: [u8; 8] = data.get(0..8)?.try_into().ok()?;
    if disc == JUP_ROUTE {
        // route: route_plan Vec FIRST (offset 8), then in_amount.
        jupiter_v1_first_hop(data, 8)
    } else if disc == JUP_SHARED_ROUTE {
        // shared_accounts_route: id u8 @8, route_plan @9, then in_amount.
        jupiter_v1_first_hop(data, 9)
    } else if disc == JUP_ROUTE_WITH_LEDGER
        || disc == JUP_SHARED_ROUTE_WITH_LEDGER
        || disc == JUP_EXACT_OUT_ROUTE
        || disc == JUP_SHARED_EXACT_OUT_ROUTE
    {
        // Recognised, but no first-hop base input is recoverable from bytes:
        // token-ledger input comes from a prior ix; exact-out prices the output.
        None
    } else if disc == JUP_ROUTE_V2 {
        // route_v2: scalars FIRST (in_amount @8), route_plan LAST @30(len)/34(items).
        let in_amount = u64_le(data, 8)?;
        let vec_len = u32_le(data, 30)?;
        let tag = *data.get(34)?; // first RoutePlanStepV2.swap tag
        let bps = u16::from_le_bytes(data.get(35..37)?.try_into().ok()?); // .bps (u16)
        let input_index = *data.get(37)?;
        if !JUP_PUMP_SELL_TAGS.contains(&tag) || bps != 10_000 || input_index != 0 {
            return None; // not a clean 100% first-hop sell of the route input
        }
        // Safety: the watched Pump pool we attribute to must be THIS first hop.
        // Accept only single-hop, or the Pump→Meteora circular arb (second hop
        // Meteora, so there is exactly one Pump pool in the route). A route with
        // a SECOND Pump hop is rejected — we could not tell which pool a lone
        // watched address belongs to.
        match vec_len {
            1 => {}
            2 => {
                // second RoutePlanStepV2 begins at 34 + 5 = 39 (zero-payload step).
                let tag2 = *data.get(39)?;
                if !JUP_METEORA_TAGS.contains(&tag2) || JUP_PUMP_ANY_TAGS.contains(&tag2) {
                    return None;
                }
            }
            _ => return None,
        }
        Some(PumpSellFirstHop {
            base_amount_in: in_amount,
        })
    } else {
        None
    }
}

/// Decode an OKX router `swap` / `proxy_swap` / `swap_tob_v3` first hop.
/// SwapArgs { amount_in u64@0, expect_amount_out u64@8, min_return u64@16,
///            amounts Vec<u64>@24, routes Vec<Vec<Route>> }; Route { dexes
///            Vec<Dex>, weights Vec<u8> }. All wrappers put SwapArgs at data+8.
pub fn decode_okx(data: &[u8]) -> Option<PumpSellFirstHop> {
    let disc: [u8; 8] = data.get(0..8)?.try_into().ok()?;
    if disc != OKX_SWAP && disc != OKX_PROXY_SWAP && disc != OKX_SWAP_TOB_V3 {
        return None;
    }
    let base = 8usize; // SwapArgs start
    let amount_in = u64_le(data, base)?;
    // Skip to the first Dex tag: amounts Vec<u64> @ base+24, then routes.
    let amounts_off = base + 24;
    let amounts_len = u32_le(data, amounts_off)? as usize;
    let mut off = amounts_off + 4 + amounts_len.checked_mul(8)?;
    // routes: Vec<Vec<Route>>
    let outer_len = u32_le(data, off)?;
    off += 4;
    let inner_len = u32_le(data, off)?; // Vec<Route>
    off += 4;
    // first Route: dexes Vec<Dex>
    let dexes_len = u32_le(data, off)?;
    off += 4;
    // Safety: only a single-hop, single-dex OKX swap is unambiguous (one Pump
    // pool). A multi-hop OKX route (e.g. sell one token then buy another) has
    // two Pump pools, and we could not tell which a lone watched address is —
    // reject it (falls back to Unreadable / wait for the account-update).
    if outer_len != 1 || inner_len != 1 || dexes_len != 1 {
        return None;
    }
    let first_dex_tag = *data.get(off)?;
    if !OKX_PUMP_SELL_TAGS.contains(&first_dex_tag) {
        return None;
    }
    Some(PumpSellFirstHop {
        base_amount_in: amount_in,
    })
}

/// Try every known router layout for this instruction data.
pub fn decode_first_hop(data: &[u8]) -> Option<PumpSellFirstHop> {
    decode_jupiter(data).or_else(|| decode_okx(data))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Jupiter route (v1), single-hop PumpSwapSellV3, in_amount = 95458148935
    // (the real ANSEMOTHY route example).
    #[test]
    fn jupiter_route_v1_single_hop_sell() {
        let mut d = Vec::new();
        d.extend_from_slice(&JUP_ROUTE);
        d.extend_from_slice(&1u32.to_le_bytes()); // route_plan len = 1
        d.push(100); // swap tag = PumpSwapSellV3
        d.push(100); // percent = 100
        d.push(0); // input_index
        d.push(1); // output_index
        d.extend_from_slice(&95_458_148_935u64.to_le_bytes()); // in_amount
        d.extend_from_slice(&6_482_955u64.to_le_bytes()); // quoted_out
        d.extend_from_slice(&100u16.to_le_bytes()); // slippage_bps
        d.push(0); // platform_fee_bps
        let hop = decode_jupiter(&d).expect("first hop");
        assert_eq!(hop.base_amount_in, 95_458_148_935);
    }

    // Jupiter route_v2, first hop PumpSwapSellV3 bps=10000, in_amount=705385
    // (the real WSOL→HOOD→WSOL arb example).
    #[test]
    fn jupiter_route_v2_first_hop_sell() {
        let mut d = Vec::new();
        d.extend_from_slice(&JUP_ROUTE_V2);
        d.extend_from_slice(&705_385u64.to_le_bytes()); // in_amount @8
        d.extend_from_slice(&728_774u64.to_le_bytes()); // quoted_out @16
        d.extend_from_slice(&0u16.to_le_bytes()); // slippage_bps @24
        d.extend_from_slice(&0u16.to_le_bytes()); // platform_fee_bps @26
        d.extend_from_slice(&0u16.to_le_bytes()); // positive_slippage_bps @28
        d.extend_from_slice(&2u32.to_le_bytes()); // route_plan len = 2 @30
        // step 1: PumpSwapSellV3, bps=10000, in_idx=0, out_idx=1  @34
        d.push(100);
        d.extend_from_slice(&10_000u16.to_le_bytes());
        d.push(0);
        d.push(1);
        // step 2: MeteoraDammV2WithRemainingAccounts, bps=10000, in_idx=1, out_idx=0
        d.push(108);
        d.extend_from_slice(&10_000u16.to_le_bytes());
        d.push(1);
        d.push(0);
        let hop = decode_jupiter(&d).expect("first hop");
        assert_eq!(hop.base_amount_in, 705_385);
    }

    // shared_accounts_route (id u8 prefix), single-hop PumpSwapSellV3.
    #[test]
    fn jupiter_shared_accounts_route_sell() {
        let mut d = Vec::new();
        d.extend_from_slice(&JUP_SHARED_ROUTE);
        d.push(3); // id u8
        d.extend_from_slice(&1u32.to_le_bytes()); // route_plan len = 1
        d.push(100); // PumpSwapSellV3
        d.push(100); // percent
        d.push(0);
        d.push(1);
        d.extend_from_slice(&42_000u64.to_le_bytes()); // in_amount
        d.extend_from_slice(&40_000u64.to_le_bytes());
        d.extend_from_slice(&50u16.to_le_bytes());
        d.push(0);
        assert_eq!(decode_jupiter(&d).unwrap().base_amount_in, 42_000);
    }

    // Multi-hop v1 route: Pump sell then Meteora — exactly one Pump pool → decodes.
    #[test]
    fn jupiter_route_v1_pump_then_meteora() {
        let mut d = Vec::new();
        d.extend_from_slice(&JUP_ROUTE);
        d.extend_from_slice(&2u32.to_le_bytes()); // 2 steps
        d.push(73); // PumpSwapSell
        d.push(100);
        d.push(0);
        d.push(1);
        d.push(77); // MeteoraDammV2
        d.push(100);
        d.push(1);
        d.push(0);
        d.extend_from_slice(&999u64.to_le_bytes()); // in_amount
        d.extend_from_slice(&1u64.to_le_bytes());
        d.extend_from_slice(&0u16.to_le_bytes());
        d.push(0);
        assert_eq!(decode_jupiter(&d).unwrap().base_amount_in, 999);
    }

    // Two Pump hops → ambiguous pool attribution → None.
    #[test]
    fn jupiter_two_pump_hops_is_none() {
        let mut d = Vec::new();
        d.extend_from_slice(&JUP_ROUTE);
        d.extend_from_slice(&2u32.to_le_bytes());
        d.push(73); // PumpSwapSell
        d.push(100);
        d.push(0);
        d.push(1);
        d.push(72); // PumpSwapBuy — second Pump pool
        d.push(100);
        d.push(1);
        d.push(2);
        d.extend_from_slice(&999u64.to_le_bytes());
        d.extend_from_slice(&1u64.to_le_bytes());
        d.extend_from_slice(&0u16.to_le_bytes());
        d.push(0);
        assert!(decode_jupiter(&d).is_none());
    }

    // An unknown (possibly payloaded) venue tag in the route → bail to None.
    #[test]
    fn jupiter_unknown_venue_is_none() {
        let mut d = Vec::new();
        d.extend_from_slice(&JUP_ROUTE);
        d.extend_from_slice(&1u32.to_le_bytes());
        d.push(17); // Whirlpool (carries a payload we don't size) — untrusted
        d.push(100);
        d.push(0);
        d.push(1);
        d.extend_from_slice(&999u64.to_le_bytes());
        assert!(decode_jupiter(&d).is_none());
    }

    // with_token_ledger / exact_out are recognised but yield no amount → None.
    #[test]
    fn jupiter_ledger_and_exact_out_are_none() {
        assert!(decode_jupiter(&JUP_ROUTE_WITH_LEDGER).is_none());
        assert!(decode_jupiter(&JUP_EXACT_OUT_ROUTE).is_none());
    }

    // A buy first-hop (tag 72) must NOT decode — returns None → Unreadable.
    #[test]
    fn jupiter_buy_first_hop_is_none() {
        let mut d = Vec::new();
        d.extend_from_slice(&JUP_ROUTE);
        d.extend_from_slice(&1u32.to_le_bytes());
        d.push(72); // PumpSwapBuy
        d.push(100);
        d.push(0);
        d.push(1);
        d.extend_from_slice(&100u64.to_le_bytes());
        d.extend_from_slice(&100u64.to_le_bytes());
        d.extend_from_slice(&0u16.to_le_bytes());
        d.push(0);
        assert!(decode_jupiter(&d).is_none());
    }

    // OKX swap, SwapArgs with amount_in=5390846552666, first dex PumpfunammSell2(85).
    #[test]
    fn okx_swap_first_hop_sell() {
        let mut d = Vec::new();
        d.extend_from_slice(&OKX_SWAP);
        d.extend_from_slice(&5_390_846_552_666u64.to_le_bytes()); // amount_in @8
        d.extend_from_slice(&4_270_594_256u64.to_le_bytes()); // expect_amount_out @16
        d.extend_from_slice(&1u64.to_le_bytes()); // min_return @24
        d.extend_from_slice(&0u32.to_le_bytes()); // amounts Vec<u64> len = 0 @32
        d.extend_from_slice(&1u32.to_le_bytes()); // routes outer len = 1
        d.extend_from_slice(&1u32.to_le_bytes()); // inner Vec<Route> len = 1
        d.extend_from_slice(&1u32.to_le_bytes()); // dexes Vec<Dex> len = 1
        d.push(85); // PumpfunammSell2
        let hop = decode_okx(&d).expect("first hop");
        assert_eq!(hop.base_amount_in, 5_390_846_552_666);
    }

    // Truncated data never panics, always None.
    #[test]
    fn truncated_is_none() {
        assert!(decode_jupiter(&JUP_ROUTE).is_none());
        assert!(decode_okx(&OKX_SWAP).is_none());
        assert!(decode_first_hop(&[]).is_none());
    }
}
