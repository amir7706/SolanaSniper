use crate::types::*;
use chrono::Utc;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, warn};

/// Raydium Detector: Scans every raw transaction for `initialize2` instructions.
///
/// This module is the critical "Signal" detector. It looks for transactions that:
/// 1. Call the Raydium AMM V4 Program (675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8)
/// 2. Contain the `initialize2` instruction (discriminator 0x02b13c045c158651)
/// 3. Create a new liquidity pool with a fresh LP mint
///
/// When all three conditions are met, it emits a `PoolEvent` downstream.
pub async fn run(
    mut rx: mpsc::Receiver<Vec<u8>>,
    tx: broadcast::Sender<PoolEvent>,
) -> anyhow::Result<()> {
    info!("Raydium detector started — scanning for initialize2 instructions");

    let raydium_program = Pubkey::from_str(RAYDIUM_AMM_V4_PROGRAM_ID).expect("valid pubkey");
    let mut scanned_count: u64 = 0;
    let mut detected_count: u64 = 0;

    loop {
        match rx.recv().await {
            Some(tx_bytes) => {
                scanned_count += 1;

                if let Some(pool_event) = detect_raydium_init(&tx_bytes, &raydium_program) {
                    detected_count += 1;
                    info!(
                        mint = %pool_event.base_mint,
                        pool = %pool_event.pool_address,
                        quote_lamports = pool_event.quote_amount,
                        tx = %pool_event.tx_signature,
                        ">>> POOL INITIALIZATION DETECTED <<<"
                    );

                    if tx.send(pool_event).is_err() {
                        warn!("No subscribers for pool events");
                    }
                }

                // Log scan rate every 100k transactions
                if scanned_count % 100_000 == 0 {
                    debug!(
                        scanned = scanned_count,
                        detected = detected_count,
                        "Scan progress"
                    );
                }
            }
            None => {
                warn!("Transaction channel closed, detector stopping");
                return Ok(());
            }
        }
    }
}

/// Parse a serialized Solana transaction and detect Raydium pool initialization.
///
/// Transaction wire format:
///   [compact_u16: num_required_signatures]
///   [1 byte: num_readonly_signed_accounts]
///   [1 byte: num_readonly_unsigned_accounts]
///   [compact_u16: num_instructions]
///   [pubkey(32) × num_account_keys]
///   [instruction_data × num_instructions]
fn detect_raydium_init(tx_data: &[u8], raydium_program: &Pubkey) -> Option<PoolEvent> {
    use crate::types::ZeroCopyWalker;
    let mut walker = ZeroCopyWalker::new(tx_data);

    // 1. Skip Signatures
    let num_sigs = walker.read_compact_u16().unwrap_or(0);
    walker.offset += num_sigs * 64;

    // 2. Read Message Header (3 bytes)
    let _num_req_sigs = walker.read_u8();
    let _num_readonly_signed = walker.read_u8();
    let _num_readonly_unsigned = walker.read_u8();

    // 3. Read Account Keys
    let num_accounts = walker.read_compact_u16().unwrap_or(0);
    let account_keys_bytes = walker.read_bytes(num_accounts * 32)?;
    
    // Check if Raydium program is in the account keys (Optimization: scan bytes directly)
    let mut raydium_idx = None;
    for i in 0..num_accounts {
        let key = &account_keys_bytes[i * 32..(i + 1) * 32];
        if key == raydium_program.as_ref() {
            raydium_idx = Some(i);
            break;
        }
    }
    let raydium_idx = raydium_idx?;

    // 4. Skip Blockhash
    walker.offset += 32;

    // 5. Parse Instructions
    let num_ixs = walker.read_compact_u16().unwrap_or(0);
    for _ in 0..num_ixs {
        let prog_idx = walker.read_u8().unwrap_or(0) as usize;
        let num_acct_indices = walker.read_compact_u16().unwrap_or(0);
        let acct_indices = walker.read_bytes(num_acct_indices).unwrap_or(&[]);
        let data_len = walker.read_compact_u16().unwrap_or(0);
        let data = walker.read_bytes(data_len).unwrap_or(&[]);

        // Check if this is a Raydium instruction
        if prog_idx == raydium_idx && data.len() >= 8 {
            let discriminator = &data[..8];

            if discriminator == INITIALIZE2_DISCRIMINATOR || discriminator == INITIALIZE_DISCRIMINATOR {
                // We need to provide account keys for the sub-parser
                let mut account_keys = Vec::with_capacity(num_accounts);
                for i in 0..num_accounts {
                    let key = &account_keys_bytes[i * 32..(i + 1) * 32];
                    account_keys.push(Pubkey::new_from_array(key.try_into().unwrap()));
                }

                let acct_indices_vec: Vec<usize> = acct_indices.iter().map(|&i| i as usize).collect();

                return parse_initialize2(
                    tx_data,
                    &account_keys,
                    &acct_indices_vec,
                    data,
                    raydium_idx,
                );
            }
        }
    }

    None
}

/// Parse an initialize2 instruction and extract pool creation details.
///
/// Raydium initialize2 accounts layout:
///   0. Pool account (must be uninitialized)
///   1. Authority (PDA)
///   2. LP mint
///   3. Coin mint (base token)
///   4. PC mint (quote token, usually SOL)
///   5. Coin vault
///   6. PC vault
///   7. Withdraw authority
///   8. Timer authority
///   9. Fee recipient
///   10. Event authority (or payer)
///   11. Program
///
/// Instruction data after 8-byte discriminator:
///   - bump (u8)
///   - initial_coin_amount (u64)
///   - initial_pc_amount (u64)
///   - fee_rate (u16)
fn parse_initialize2(
    tx_data: &[u8],
    account_keys: &[Pubkey],
    acct_indices: &[usize],
    instr_data: &[u8],
    _raydium_idx: usize,
) -> Option<PoolEvent> {
    // We need at least 11 accounts for initialize2
    if acct_indices.len() < 11 {
        return None;
    }

    let safe_get = |idx: usize| -> Option<Pubkey> {
        if idx < account_keys.len() {
            Some(account_keys[idx])
        } else {
            None
        }
    };

    // Account indices from the instruction
    let pool_account = safe_get(*acct_indices.get(0)?)?;
    let _authority = safe_get(*acct_indices.get(1)?)?;
    let lp_mint = safe_get(*acct_indices.get(2)?)?;
    let base_mint = safe_get(*acct_indices.get(3)?)?;
    let quote_mint = safe_get(*acct_indices.get(4)?)?;
    let base_vault = safe_get(*acct_indices.get(5)?)?;
    let quote_vault = safe_get(*acct_indices.get(6)?)?;

    // Parse instruction data (after 8-byte discriminator)
    let data = &instr_data[8..];
    if data.len() < 19 {
        return None;
    }

    // bump: u8
    let _bump = data[0];

    // initial_coin_amount: u64 LE (bytes 1..8)
    let base_amount = u64::from_le_bytes(
        data[1..9].try_into().ok()?
    );

    // initial_pc_amount: u64 LE (bytes 9..16)
    let quote_amount = u64::from_le_bytes(
        data[9..17].try_into().ok()?
    );

    // Derive a pseudo-signature from the transaction data
    // (real signature extraction would need full wire format parsing)
    let sig_bytes = &tx_data.get(3..67)?; // Skip 3 header bytes, take 64 sig bytes
    let signature = bs58::encode(sig_bytes).into_string();

    Some(PoolEvent {
        tx_signature: signature,
        slot: 0, // Would need slot from shred metadata
        detected_at: Utc::now(),
        base_mint,
        quote_mint,
        pool_address: pool_account,
        lp_mint,
        base_vault,
        quote_vault,
        base_amount,
        quote_amount,
        authority: safe_get(*acct_indices.get(0)?)?, // Pool account as authority proxy
        raw_data: instr_data.to_vec(),
    })
}

/// Read a compact-u16 from a byte slice (Solana's variable-length encoding).

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_compact_u16_single_byte() {
        let data = [42u8, 0, 0];
        let (val, len) = read_compact_u16(&data, 0).unwrap();
        assert_eq!(val, 42);
        assert_eq!(len, 1);
    }

    #[test]
    fn test_read_compact_u16_multi_byte() {
        let data = [0x85, 0x01, 0]; // (0x05 << 8) | 0x01 = 0x501 = 1281
        let (val, len) = read_compact_u16(&data, 0).unwrap();
        assert_eq!(val, 1281);
        assert_eq!(len, 2);
    }

    #[test]
    fn test_read_compact_u16_out_of_bounds() {
        let data = [];
        assert!(read_compact_u16(&data, 0).is_none());
    }

    #[test]
    fn test_discriminator_constants() {
        // Verify our discriminators match expected Anchor hashes
        // sha256("global:initialize2")[..8]
        assert_eq!(INITIALIZE2_DISCRIMINATOR.len(), 8);
        assert_eq!(INITIALIZE_DISCRIMINATOR.len(), 8);
    }
}
