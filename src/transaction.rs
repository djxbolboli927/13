use anyhow::{Context, Result};
use rand::seq::SliceRandom;
use solana_client::rpc_client::RpcClient;
#[allow(deprecated)]
use solana_sdk::{
    address_lookup_table::AddressLookupTableAccount,
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    message::{v0, VersionedMessage},
    pubkey::Pubkey,
    signature::Keypair,
    signer::Signer,
    system_instruction,
    transaction::VersionedTransaction,
};
use std::str::FromStr;

use crate::alt_cache::AltCache;
use crate::metis::{InstructionData, SwapInstructionsResponse};

/// Jito tip account addresses -- pick one at random for each bundle.
/// Per Jito docs: do NOT use ALTs for tip accounts.
const JITO_TIP_ACCOUNTS: &[&str] = &[
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
];

/// Return Jito tip account pubkeys (used by AltCache to filter them out).
pub fn jito_tip_pubkeys() -> Vec<Pubkey> {
    JITO_TIP_ACCOUNTS
        .iter()
        .filter_map(|a| Pubkey::from_str(a).ok())
        .collect()
}

/// Convert a Metis instruction into a Solana SDK Instruction.
fn to_sdk_instruction(ix: &InstructionData) -> Result<Instruction> {
    let program_id = Pubkey::from_str(&ix.program_id)?;
    let accounts: Vec<AccountMeta> = ix
        .accounts
        .iter()
        .map(|a| {
            let pubkey = Pubkey::from_str(&a.pubkey).expect("invalid pubkey in instruction");
            if a.is_writable {
                AccountMeta::new(pubkey, a.is_signer)
            } else {
                AccountMeta::new_readonly(pubkey, a.is_signer)
            }
        })
        .collect();
    let data = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &ix.data,
    )
    .context("failed to decode instruction data")?;
    Ok(Instruction {
        program_id,
        accounts,
        data,
    })
}

/// Build a versioned transaction with exactly 3 instructions:
///
/// #1 - Compute Budget: SetComputeUnitLimit
/// #2 - Jupiter Aggregator: route_v2 (entire circular arb)
/// #3 - System Program: Transfer (Jito tip, MUST be last)
///
/// Uses AltCache for ALT lookups (0ns on cache hit vs ~5ms RPC call).
/// Uses pre-cached blockhash (passed in, ~100ns read vs ~5ms RPC call).
///
/// The on-chain minimum output (quotedOutAmount in the route_v2 instruction)
/// is controlled by setting out_amount in the merged quote passed to Metis
/// before calling this function — do not patch instruction bytes here.
pub fn build_arb_transaction(
    swap_ixs: &SwapInstructionsResponse,
    payer: &Keypair,
    tip_lamports: u64,
    cu_limit: u32,
    recent_blockhash: Hash,
    alt_cache: &AltCache,
    rpc_client: &RpcClient,
) -> Result<VersionedTransaction> {
    let mut instructions: Vec<Instruction> = Vec::new();

    // #1 -- SetComputeUnitLimit
    let cu_limit_ix = Instruction {
        program_id: Pubkey::from_str("ComputeBudget111111111111111111111111111111")?,
        accounts: vec![],
        data: {
            let mut data = vec![0x02];
            data.extend_from_slice(&cu_limit.to_le_bytes());
            data
        },
    };
    instructions.push(cu_limit_ix);

    // #2 -- Single route_v2 for the entire circular swap
    instructions.push(to_sdk_instruction(&swap_ixs.swap_instruction)?);

    // #3 -- Jito tip (MUST be last, MUST NOT be in ALT)
    let tip_account = {
        let mut rng = rand::thread_rng();
        let addr = JITO_TIP_ACCOUNTS.choose(&mut rng).unwrap();
        Pubkey::from_str(addr)?
    };
    #[allow(deprecated)]
    instructions.push(system_instruction::transfer(
        &payer.pubkey(),
        &tip_account,
        tip_lamports,
    ));

    // Fetch ALTs via cache (instant on hit, RPC on first miss only)
    let mut alt_addresses: Vec<Pubkey> = Vec::new();
    for addr in &swap_ixs.address_lookup_table_addresses {
        let pubkey = Pubkey::from_str(addr)?;
        if !alt_addresses.contains(&pubkey) {
            alt_addresses.push(pubkey);
        }
    }

    let mut address_lookup_tables: Vec<AddressLookupTableAccount> = Vec::new();
    for alt_pubkey in &alt_addresses {
        let alt_account = alt_cache.get_or_fetch(alt_pubkey, rpc_client)?;
        address_lookup_tables.push(alt_account);
    }

    // Build VersionedTransaction v0
    let message = v0::Message::try_compile(
        &payer.pubkey(),
        &instructions,
        &address_lookup_tables,
        recent_blockhash,
    )
    .context("failed to compile v0 message")?;

    let versioned_message = VersionedMessage::V0(message);
    let tx = VersionedTransaction::try_new(versioned_message, &[payer])
        .context("failed to sign versioned transaction")?;

    Ok(tx)
}

/// Build a versioned transaction for a DIRECT-to-RPC send (no Jito tip):
///
/// #1 - Compute Budget: SetComputeUnitLimit
/// #2 - Compute Budget: SetComputeUnitPrice (only if `priority_fee_microlamports > 0`)
/// #3 - Jupiter Aggregator: route_v2 (entire circular arb)
///
/// Identical to [`build_arb_transaction`] but with NO tip transfer — the fee we
/// pay is just the network base fee (+ optional priority fee). Faster and
/// cheaper than a Jito bundle for the freshly-created pools this strategy hits.
pub fn build_direct_transaction(
    swap_ixs: &SwapInstructionsResponse,
    payer: &Keypair,
    cu_limit: u32,
    priority_fee_microlamports: u64,
    loaded_accounts_data_limit: u32,
    recent_blockhash: Hash,
    alt_cache: &AltCache,
    rpc_client: &RpcClient,
) -> Result<VersionedTransaction> {
    let mut instructions: Vec<Instruction> = Vec::new();

    // #1 -- SetComputeUnitLimit
    instructions.push(Instruction {
        program_id: Pubkey::from_str("ComputeBudget111111111111111111111111111111")?,
        accounts: vec![],
        data: {
            let mut data = vec![0x02];
            data.extend_from_slice(&cu_limit.to_le_bytes());
            data
        },
    });

    // #2 -- SetComputeUnitPrice (priority fee), optional.
    if priority_fee_microlamports > 0 {
        instructions.push(Instruction {
            program_id: Pubkey::from_str("ComputeBudget111111111111111111111111111111")?,
            accounts: vec![],
            data: {
                let mut data = vec![0x03];
                data.extend_from_slice(&priority_fee_microlamports.to_le_bytes());
                data
            },
        });
    }

    // #2b -- SetLoadedAccountsDataSizeLimit, optional (0x04, u32 bytes).
    if loaded_accounts_data_limit > 0 {
        instructions.push(Instruction {
            program_id: Pubkey::from_str("ComputeBudget111111111111111111111111111111")?,
            accounts: vec![],
            data: {
                let mut data = vec![0x04];
                data.extend_from_slice(&loaded_accounts_data_limit.to_le_bytes());
                data
            },
        });
    }

    // #3 -- Single route_v2 for the entire circular swap.
    instructions.push(to_sdk_instruction(&swap_ixs.swap_instruction)?);

    // Resolve ALTs via cache.
    let mut alt_addresses: Vec<Pubkey> = Vec::new();
    for addr in &swap_ixs.address_lookup_table_addresses {
        let pubkey = Pubkey::from_str(addr)?;
        if !alt_addresses.contains(&pubkey) {
            alt_addresses.push(pubkey);
        }
    }
    let mut address_lookup_tables: Vec<AddressLookupTableAccount> = Vec::new();
    for alt_pubkey in &alt_addresses {
        address_lookup_tables.push(alt_cache.get_or_fetch(alt_pubkey, rpc_client)?);
    }

    let message = v0::Message::try_compile(
        &payer.pubkey(),
        &instructions,
        &address_lookup_tables,
        recent_blockhash,
    )
    .context("failed to compile v0 message (direct)")?;

    let tx = VersionedTransaction::try_new(VersionedMessage::V0(message), &[payer])
        .context("failed to sign versioned transaction (direct)")?;
    Ok(tx)
}

/// Serialized (raw, wire) byte length of a transaction. Solana rejects any tx
/// whose raw form exceeds 1232 bytes, so we check this before sending to avoid
/// a guaranteed `-32602 too large` RPC error.
pub fn serialized_len(tx: &VersionedTransaction) -> usize {
    bincode::serialize(tx).map(|b| b.len()).unwrap_or(usize::MAX)
}

/// Number of distinct accounts the transaction locks.
///
/// Solana enforces MAX_TX_ACCOUNT_LOCKS = 64: the total of static account
/// keys PLUS every account pulled in through an Address Lookup Table counts
/// toward this limit. ALTs shrink the *serialized size* of a tx but do NOT
/// reduce the lock count, so a multi-hop circular swap with >64 distinct
/// accounts is rejected by the block engine ("too many account locks") no
/// matter how many ALTs it references. We compute this before sending so the
/// guaranteed-reject transactions are dropped locally instead of burning a
/// Jito rate-limit slot.
pub fn account_lock_count(tx: &VersionedTransaction) -> usize {
    match &tx.message {
        VersionedMessage::V0(msg) => {
            let from_alt: usize = msg
                .address_table_lookups
                .iter()
                .map(|l| l.writable_indexes.len() + l.readonly_indexes.len())
                .sum();
            msg.account_keys.len() + from_alt
        }
        VersionedMessage::Legacy(msg) => msg.account_keys.len(),
    }
}

/// Deserialize the addresses stored in an Address Lookup Table account.
pub fn deserialize_alt_addresses(data: &[u8]) -> Result<Vec<Pubkey>> {
    const HEADER_SIZE: usize = 56;
    if data.len() < HEADER_SIZE {
        anyhow::bail!("ALT account data too short: {} bytes", data.len());
    }
    let addresses_data = &data[HEADER_SIZE..];
    if addresses_data.len() % 32 != 0 {
        anyhow::bail!(
            "ALT addresses data has invalid length: {} (not a multiple of 32)",
            addresses_data.len()
        );
    }
    let addresses: Vec<Pubkey> = addresses_data
        .chunks_exact(32)
        .map(|chunk| Pubkey::new_from_array(chunk.try_into().unwrap()))
        .collect();
    Ok(addresses)
}
