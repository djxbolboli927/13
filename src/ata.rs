//! Associated-token-account lifecycle for the auto pool-management pipeline.
//!
//! When a new shared pool is discovered we must be able to hold the token, so
//! we create the trading wallet's ATA for that mint. When a pool rugs we close
//! the ATA to reclaim rent. Both operations are idempotent and tolerant of the
//! account already existing / already being gone.
//!
//! The token program is detected from the mint account's `owner` so this works
//! for both the classic SPL Token program and Token-2022 mints.

use anyhow::{Context, Result};
use solana_client::rpc_client::RpcClient;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::transaction::Transaction;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_associated_token_account::instruction::create_associated_token_account_idempotent;
use std::collections::HashSet;
use tracing::{info, warn};

/// Classic SPL Token program.
const SPL_TOKEN: Pubkey =
    solana_sdk::pubkey::Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");

/// Read the token program that owns `mint` (SPL Token vs Token-2022).
fn token_program_of(rpc: &RpcClient, mint: &Pubkey) -> Pubkey {
    match rpc.get_account(mint) {
        Ok(acct) => acct.owner,
        Err(_) => SPL_TOKEN,
    }
}

/// Derive the trading wallet's ATA for `mint` (program-id aware).
pub fn ata_for(rpc: &RpcClient, wallet: &Pubkey, mint: &Pubkey) -> Pubkey {
    let token_prog = token_program_of(rpc, mint);
    get_associated_token_address_with_program_id(wallet, mint, &token_prog)
}

/// Ensure the trading wallet has an ATA for `mint`; create it if missing.
/// Returns the ATA address. Uses the idempotent create instruction so a
/// concurrent create or a pre-existing account is not an error.
pub fn ensure_ata(rpc: &RpcClient, payer: &Keypair, mint: &Pubkey) -> Result<Pubkey> {
    let token_prog = token_program_of(rpc, mint);
    let ata = get_associated_token_address_with_program_id(&payer.pubkey(), mint, &token_prog);

    // Fast path: already exists.
    if rpc.get_account(&ata).is_ok() {
        return Ok(ata);
    }

    let ix = create_associated_token_account_idempotent(
        &payer.pubkey(),
        &payer.pubkey(),
        mint,
        &token_prog,
    );
    let blockhash = rpc
        .get_latest_blockhash()
        .context("ensure_ata: get_latest_blockhash failed")?;
    let tx = Transaction::new_signed_with_payer(
        &[ix],
        Some(&payer.pubkey()),
        &[payer],
        blockhash,
    );
    match rpc.send_and_confirm_transaction(&tx) {
        Ok(sig) => info!(mint = %mint, %ata, %sig, "created token ATA"),
        Err(e) => {
            // The account may have been created between our check and send; if
            // it now exists, treat as success.
            if rpc.get_account(&ata).is_ok() {
                info!(mint = %mint, %ata, "token ATA already present after send");
            } else {
                return Err(e).context("ensure_ata: create ATA transaction failed");
            }
        }
    }
    Ok(ata)
}

/// Startup reconciliation: for every `mint` (excluding `skip`), make sure the
/// trading wallet has an ATA, creating any that are missing. Existence is
/// checked in batches via `getMultipleAccounts` (up to 100 per RPC call) so we
/// stay well within a 10 req/s budget even for large pool sets. The token
/// program for each mint is read from the mint account's owner (also batched),
/// so Token-2022 mints are handled correctly.
pub fn reconcile_atas(
    rpc: &RpcClient,
    payer: &Keypair,
    mints: &[Pubkey],
    skip: &HashSet<Pubkey>,
) -> Result<()> {
    let targets: Vec<Pubkey> = mints
        .iter()
        .filter(|m| !skip.contains(m))
        .copied()
        .collect();
    if targets.is_empty() {
        info!("ATA reconcile: nothing to check (all mints in always-exist set)");
        return Ok(());
    }

    // 1. Batch-read each mint account to learn its token program (owner).
    let mut token_prog: std::collections::HashMap<Pubkey, Pubkey> =
        std::collections::HashMap::new();
    for chunk in targets.chunks(100) {
        match rpc.get_multiple_accounts(chunk) {
            Ok(accts) => {
                for (pk, acct) in chunk.iter().zip(accts) {
                    let owner = acct.map(|a| a.owner).unwrap_or(SPL_TOKEN);
                    token_prog.insert(*pk, owner);
                }
            }
            Err(e) => {
                warn!(error = %e, "ATA reconcile: mint owner batch read failed; assuming SPL Token");
                for pk in chunk {
                    token_prog.entry(*pk).or_insert(SPL_TOKEN);
                }
            }
        }
    }

    // 2. Derive each ATA.
    let derived: Vec<(Pubkey, Pubkey)> = targets
        .iter()
        .map(|mint| {
            let prog = *token_prog.get(mint).unwrap_or(&SPL_TOKEN);
            (
                *mint,
                get_associated_token_address_with_program_id(&payer.pubkey(), mint, &prog),
            )
        })
        .collect();

    // 3. Batch-check which ATAs already exist; collect the missing mints.
    let mut missing: Vec<Pubkey> = Vec::new();
    for chunk in derived.chunks(100) {
        let atas: Vec<Pubkey> = chunk.iter().map(|(_, ata)| *ata).collect();
        match rpc.get_multiple_accounts(&atas) {
            Ok(accts) => {
                for ((mint, _), acct) in chunk.iter().zip(accts) {
                    if acct.is_none() {
                        missing.push(*mint);
                    }
                }
            }
            Err(e) => {
                // On a batch failure, fall through to per-mint create (idempotent).
                warn!(error = %e, "ATA reconcile: existence batch read failed; will create idempotently");
                for (mint, _) in chunk {
                    missing.push(*mint);
                }
            }
        }
    }

    info!(
        checked = targets.len(),
        missing = missing.len(),
        "ATA reconcile: creating missing token ATAs"
    );

    // 4. Create the missing ATAs (idempotent, one tx each).
    let mut created = 0usize;
    for mint in &missing {
        match ensure_ata(rpc, payer, mint) {
            Ok(_) => created += 1,
            Err(e) => warn!(mint = %mint, error = %e, "ATA reconcile: create failed"),
        }
    }
    info!(created, "ATA reconcile complete");
    Ok(())
}

/// Close the trading wallet's ATA for `mint`, returning rent to the wallet.
/// No-op if the account is already gone. Any residual token balance blocks the
/// close on-chain, so callers should only close ATAs known to be empty.
pub fn close_ata(rpc: &RpcClient, payer: &Keypair, mint: &Pubkey) -> Result<()> {
    let token_prog = token_program_of(rpc, mint);
    let ata = get_associated_token_address_with_program_id(&payer.pubkey(), mint, &token_prog);

    if rpc.get_account(&ata).is_err() {
        return Ok(()); // already gone
    }

    // SPL Token `CloseAccount` = instruction index 9. Accounts:
    //   0. [writable] account to close
    //   1. [writable] destination for reclaimed lamports
    //   2. [signer]   account owner
    let ix = Instruction {
        program_id: token_prog,
        accounts: vec![
            AccountMeta::new(ata, false),
            AccountMeta::new(payer.pubkey(), false),
            AccountMeta::new_readonly(payer.pubkey(), true),
        ],
        data: vec![9],
    };
    let blockhash = rpc
        .get_latest_blockhash()
        .context("close_ata: get_latest_blockhash failed")?;
    let tx = Transaction::new_signed_with_payer(
        &[ix],
        Some(&payer.pubkey()),
        &[payer],
        blockhash,
    );
    match rpc.send_and_confirm_transaction(&tx) {
        Ok(sig) => info!(mint = %mint, %ata, %sig, "closed token ATA"),
        Err(e) => warn!(mint = %mint, %ata, error = %e, "close ATA failed (non-empty?)"),
    }
    Ok(())
}
