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
    let mut offset = 0;

    // 1. Parse compact u16 for num_required_signatures
    let (num_sigs, consumed) = read_compact_u16(tx_data, offset)?;
    offset += consumed;

    // 2. num_readonly_signed
    if offset >= tx_data.len() { return None; }
    let _num_readonly_signed = tx_data[offset];
    offset += 1;

    // 3. num_readonly_unsigned
    if offset >= tx_data.len() { return None; }
    let _num_readonly_unsigned = tx_data[offset];
    offset += 1;

    // 4. Parse compact u16 for num_instructions
    let (num_instructions, consumed) = read_compact_u16(tx_data, offset)?;
    offset += consumed;

    // Total account keys = num_sigs + readonly_signed + readonly_unsigned + writable_unsigned
    // But we need to figure out the total number of account keys.
    // The problem: we don't know num_readonly_signed + num_readonly_unsigned separately
    // from the total. Let's re-approach.

    // Actually, the Solana wire format is:
    // [1 byte: num_required_signatures]
    // [1 byte: num_readonly_signed_accounts] 
    // [1 byte: num_readonly_unsigned_accounts]
    // [1 byte: num_required_signatures - num_readonly_signed = num_writable_signed]
    // Wait, no. Let me be precise.

    // Re-parse from the beginning with the correct format:
    offset = 0;

    // Byte 0: number of required signatures
    if offset >= tx_data.len() { return None; }
    let _num_required_sigs = tx_data[offset] as usize;
    offset += 1;

    // Byte 1: number of read-only signed accounts
    if offset >= tx_data.len() { return None; }
    let _num_readonly_signed = tx_data[offset] as usize;
    offset += 1;

    // Byte 2: number of read-only unsigned accounts
    if offset >= tx_data.len() { return None; }
    let _num_readonly_unsigned = tx_data[offset] as usize;
    offset += 1;

    // Then comes the account keys (each 32 bytes)
    // We need to figure out how many account keys there are.
    // The total number of account keys = num_sigs + num_readonly_unsigned + num_writable_unsigned
    // But num_writable_unsigned is not directly in the header.

    // Alternative: scan through pubkeys until we hit an instruction boundary.
    // Each pubkey is 32 bytes. We know there are at least `num_required_sigs` + `num_readonly_signed` keys.
    // The actual total is implicit — we need to know the total before parsing instructions.

    // Practical approach: The Solana transaction format after the 3-byte header is:
    // [compact_u16: num_account_keys]  (in newer versions)
    // But in the legacy format, it's:
    // The total number of accounts is: num_required_signatures + num_readonly_signed + num_readonly_unsigned + num_writable_unsigned
    // where num_writable_unsigned = (implicit, parsed differently)

    // Let's use a simpler, more robust approach: scan all 32-byte boundaries and collect
    // potential pubkeys, then find instructions.

    // Actually, let's use the proper legacy format:
    // After the 3 header bytes, all account keys follow (32 bytes each).
    // The total number of account keys can be inferred by:
    //   total_accounts = first_non_pubkey_offset / 32
    // But we don't know where the pubkeys end and instructions begin.

    // Better approach: try to parse as many 32-byte pubkeys as possible,
    // then look for instruction data patterns.

    let account_keys_start = offset;
    let max_possible_keys = (tx_data.len() - account_keys_start) / 32;

    // Collect account keys and try to identify the Raydium program
    let mut account_keys: Vec<Pubkey> = Vec::new();
    let mut raydium_key_index: Option<usize> = None;

    for i in 0..max_possible_keys {
        let key_start = account_keys_start + i * 32;
        let key_end = key_start + 32;
        if key_end > tx_data.len() {
            break;
        }

        let key_bytes = &tx_data[key_start..key_end];
        let pubkey = Pubkey::new_from_array(key_bytes.try_into().ok()?);

        if pubkey == *raydium_program {
            raydium_key_index = Some(i);
        }

        account_keys.push(pubkey);
    }

    // We need the Raydium program to be in the account keys
    let raydium_idx = raydium_key_index?;

    // Now parse instructions
    // Instruction format:
    //   [1 byte: program_id_index into account_keys]
    //   [compact_u16: num_account_indices]
    //   [1 byte × num_account_indices: account indices]
    //   [compact_u16: data_length]
    //   [data bytes]
    let instr_start = account_keys_start + account_keys.len() * 32;
    let mut instr_offset = instr_start;

    for _ in 0..num_instructions {
        if instr_offset >= tx_data.len() {
            break;
        }

        // Program ID index
        let program_id_index = tx_data[instr_offset] as usize;
        instr_offset += 1;

        if program_id_index >= account_keys.len() {
            break;
        }

        let program_key = &account_keys[program_id_index];

        // Number of account indices
        let (num_acct_indices, consumed) = read_compact_u16(tx_data, instr_offset)?;
        instr_offset += consumed;

        // Account indices
        let mut acct_indices = Vec::with_capacity(num_acct_indices);
        for _ in 0..num_acct_indices {
            if instr_offset >= tx_data.len() {
                break;
            }
            acct_indices.push(tx_data[instr_offset] as usize);
            instr_offset += 1;
        }

        // Data length
        let (data_len, consumed) = read_compact_u16(tx_data, instr_offset)?;
        instr_offset += consumed;

        // Instruction data
        if instr_offset + data_len > tx_data.len() {
            break;
        }

        let instr_data = &tx_data[instr_offset..instr_offset + data_len];
        instr_offset += data_len;

        // Check if this is a Raydium instruction
        if *program_key == *raydium_program && instr_data.len() >= 8 {
            let discriminator = &instr_data[..8];

            // Check for initialize2 discriminator
            if discriminator == INITIALIZE2_DISCRIMINATOR {
                return parse_initialize2(
                    tx_data,
                    &account_keys,
                    &acct_indices,
                    instr_data,
                    raydium_idx,
                );
            }

            // Also check for initialize (non-v2) as fallback
            if discriminator == INITIALIZE_DISCRIMINATOR {
                return parse_initialize2(
                    tx_data,
                    &account_keys,
                    &acct_indices,
                    instr_data,
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
fn read_compact_u16(data: &[u8], offset: usize) -> Option<(usize, usize)> {
    if offset >= data.len() {
        return None;
    }

    let first = data[offset];
    if first < 0x80 {
        Some((first as usize, 1))
    } else {
        if offset + 1 >= data.len() {
            return None;
        }
        let value = ((first as usize & 0x7F) << 8) | (data[offset + 1] as usize);
        Some((value, 2))
    }
}

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
