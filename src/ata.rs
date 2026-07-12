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
