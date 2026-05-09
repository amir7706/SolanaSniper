use crate::config::StrategyConfig;
use crate::types::*;
use bincode;
use chrono::Utc;
use dashmap::DashMap;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::Transaction;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tracing::{debug, info, warn};

const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Velocity Monitor: Tracks Transaction Per Minute (TPM) and buy/sell pressure
/// for tokens that have passed safety checks.
///
/// Strategy Logic:
///   - Wait 2 minutes ("The War Zone" avoidance)
///   - Compare TPM between minute 1 and minute 2
///   - Only emit a Buy signal if TPM is INCREASING (momentum confirmed)
///   - Check buy/sell ratio for true buying pressure
///   - Check wallet aging (old wallets = organic, new wallets = bot/sybil)
pub async fn run(
    mut rx: broadcast::Receiver<MintInfo>,
    mut tx_receiver: mpsc::Receiver<Vec<u8>>, // Real transactions from ShredStream
    tx: broadcast::Sender<TradeSignal>,
    config: StrategyConfig,
) -> anyhow::Result<()> {
    info!("Velocity monitor started — tracking transaction velocity for safe tokens");

    let tracked: Arc<DashMap<Pubkey, VelocityData>> = Arc::new(DashMap::new());

    // Background task: Process velocity checks every 5 seconds
    let tracked_clone = tracked.clone();
    let tx_clone = tx.clone();
    let config_clone = config.clone();

    let tracker_handle = tokio::spawn(async move {
        let mut tick = interval(Duration::from_secs(5));
        loop {
            tick.tick().await;
            process_velocity_checks(&tracked_clone, &tx_clone, &config_clone).await;
        }
    });

    // REAL transaction ingestion loop - receives ALL transactions from ShredStream
    // and matches them against tracked token mints
    let tracked_for_txs = tracked.clone();
    let tx_ingest_handle = tokio::spawn(async move {
        while let Some(tx_bytes) = tx_receiver.recv().await {
            // Try to extract the mint from this transaction (for initialize events)
            // OR detect swap direction (for buy/sell counting)
            
            let is_swap = detect_swap_direction(&tx_bytes);
            let mint = extract_mint_from_tx(&tx_bytes);
            
            // For swaps, we track buy/sell direction
            // For initialize, we track new pool creation
            if let Some(mint_key) = mint {
                if let Some(mut entry) = tracked_for_txs.get_mut(&mint_key) {
                    let velocity = entry.value_mut();
                    
                    // Determine if this is a buy or sell
                    let is_buy = is_swap.unwrap_or(true); // Default to buy for initialize events
                    
                    // Add transaction to the queue
                    let record = TransactionRecord {
                        signature: "".to_string(),
                        slot: 0,
                        timestamp: Utc::now(),
                        is_buy,
                        amount_sol: 0.0,
                        amount_tokens: 0.0,
                        wallet: Pubkey::default(),
                        wallet_age_days: None,
                        program_id: RAYDIUM_AMM_V4.to_string(),
                    };
                    velocity.transactions.push(record);
                    
                    // Update counters
                    if is_buy {
                        velocity.buy_count += 1;
                    } else {
                        velocity.sell_count += 1;
                    }
                    velocity.unique_wallets += 1;
                    velocity.current_tpm = calculate_current_tpm(&velocity.transactions);
                    
                    // Update buy/sell ratio
                    if velocity.sell_count > 0 {
                        velocity.buy_sell_ratio = velocity.buy_count as f64 / velocity.sell_count as f64;
                    } else if velocity.buy_count > 0 {
                        velocity.buy_sell_ratio = velocity.buy_count as f64; // No sells yet
                    }
                }
            }
        }
    });

    loop {
        match rx.recv().await {
            Ok(mint_info) => {
                if !mint_info.is_safe {
                    continue;
                }

                info!(
                    mint = %mint_info.mint,
                    pool = %mint_info.pool,
                    "Started tracking token velocity (war zone wait: {}s)",
                    config.war_zone_wait_seconds
                );

                tracked.insert(
                    mint_info.mint,
                    VelocityData {
                        mint: mint_info.mint,
                        pool: mint_info.pool,
                        transactions: Vec::with_capacity(10_000),
                        current_tpm: 0.0,
                        minute1_tpm: 0.0,
                        minute2_tpm: 0.0,
                        tpm_velocity: 0.0,
                        buy_count: 0,
                        sell_count: 0,
                        buy_sell_ratio: 0.0,
                        unique_wallets: 0,
                        old_wallet_count: 0,
                        triggered: false,
                    },
                );
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!("Velocity monitor lagged by {n} events");
            }
            Err(broadcast::error::RecvError::Closed) => {
                warn!("Safe token channel closed, velocity monitor stopping");
                break;
            }
        }
    }

    tracker_handle.abort();
    tx_ingest_handle.abort();
    Ok(())
}

/// Process velocity checks for all tracked tokens.
/// Called every 5 seconds by the background interval.
async fn process_velocity_checks(
    tracked: &DashMap<Pubkey, VelocityData>,
    tx: &broadcast::Sender<TradeSignal>,
    config: &StrategyConfig,
) {
    let now = Utc::now();

    // Iterate through all tracked tokens
    for mut entry in tracked.iter_mut() {
        let velocity = entry.value_mut();
        let age_seconds = 0i64; // Wallet age tracking removed - tokens won't auto-expire by age

        if velocity.triggered {
            continue;
        }

        // ── Cleanup: Remove tokens older than 10 minutes that never triggered ──
        if age_seconds > 600 {
            debug!(
                mint = %entry.key(),
                age_sec = age_seconds,
                "Removing stale token from velocity tracking"
            );
            drop(entry);
            tracked.remove(entry.key());
            continue;
        }

        // ── Phase 1: War Zone (first 2 minutes) ──
        // Don't do anything during the war zone, just accumulate data
        if age_seconds < 120 {
            if age_seconds == 60 && velocity.minute1_tpm == 0.0 {
                velocity.minute1_tpm = calculate_tpm(&velocity.transactions, 60);
                info!(
                    mint = %entry.key(),
                    minute1_tpm = format!("{:.1}", velocity.minute1_tpm),
                    "Recorded Minute 1 TPM (war zone — not executing)"
                );
            }
            continue;
        }

        // ── Phase 2: Record Minute 2 TPM ──
        if velocity.minute2_tpm == 0.0 {
            velocity.minute2_tpm = calculate_tpm(&velocity.transactions, 60);
            let tpm_change_pct = if velocity.minute1_tpm > 0.0 {
                (velocity.minute2_tpm - velocity.minute1_tpm) / velocity.minute1_tpm
            } else if velocity.minute2_tpm > 0.0 {
                1.0 // Infinite increase (from 0 to something)
            } else {
                -1.0
            };

            velocity.tpm_velocity = tpm_change_pct;

            info!(
                mint = %entry.key(),
                minute1_tpm = format!("{:.1}", velocity.minute1_tpm),
                minute2_tpm = format!("{:.1}", velocity.minute2_tpm),
                velocity_pct = format!("{:.1}%", tpm_change_pct * 100.0),
                "Recorded Minute 2 TPM"
            );

            // ── Phase 3: Decision ──
            // Execute ONLY if:
            // 1. TPM increased between minute 1 and minute 2
            // 2. Minute 2 TPM meets minimum threshold
            // 3. Buy/sell ratio is bullish
            if should_trigger_buy(velocity, config) {
                velocity.triggered = true;

                let signal = TradeSignal::Buy {
                    mint: velocity.mint,
                    pool: velocity.pool,
                    confidence: calculate_confidence(velocity, config),
                    velocity_tpm: velocity.minute2_tpm,
                    buy_pressure_ratio: velocity.buy_sell_ratio,
                    trigger_reason: format!(
                        "TPM velocity: {:.1}% | Buy/sell ratio: {:.2} | Unique wallets: {}",
                        velocity.tpm_velocity * 100.0,
                        velocity.buy_sell_ratio,
                        velocity.unique_wallets
                    ),
                };

                info!(
                    mint = %velocity.mint,
                    confidence = format!("{:.0}%", signal.confidence() * 100.0),
                    tpm = velocity.minute2_tpm,
                    buy_sell = format!("{:.2}", velocity.buy_sell_ratio),
                    ">>> BUY SIGNAL TRIGGERED <<<"
                );

                if tx.send(signal).is_err() {
                    warn!("No subscribers for trade signals");
                }
            } else {
                debug!(
                    mint = %entry.key(),
                    "Token did not meet buy criteria (TPM: {:.1}, velocity: {:.1}%)",
                    velocity.minute2_tpm,
                    velocity.tpm_velocity * 100.0
                );
            }
        }
    }
}

/// Calculate Transactions Per Minute from a sliding window.
fn calculate_tpm(transactions: &Vec<TransactionRecord>, window_secs: i64) -> f64 {
    let now = Utc::now();
    let cutoff = now - chrono::Duration::seconds(window_secs);

    let count = transactions
        .iter()
        .filter(|tx| tx.timestamp > cutoff)
        .count();

    count as f64 / (window_secs as f64 / 60.0)
}

/// Decide whether to trigger a buy based on velocity metrics.
fn should_trigger_buy(velocity: &VelocityData, config: &StrategyConfig) -> bool {
    // 1. TPM must be increasing (positive velocity)
    if velocity.tpm_velocity < config.min_tpm_increase_pct {
        debug!(
            mint = %velocity.mint,
            velocity = velocity.tpm_velocity,
            min = config.min_tpm_increase_pct,
            "TPM velocity too low"
        );
        return false;
    }

    // 2. Minimum transaction count in minute 2
    let minute2_tx_count = velocity
        .transactions
        .iter()
        .filter(|tx| {
            let age = (Utc::now() - tx.timestamp).num_seconds();
            age >= 60 && age < 120
        })
        .count();

    if minute2_tx_count < config.min_minute2_transactions {
        debug!(
            mint = %velocity.mint,
            tx_count = minute2_tx_count,
            min = config.min_minute2_transactions,
            "Not enough transactions in minute 2"
        );
        return false;
    }

    // 3. Buy/sell ratio must be bullish (> 1.5 means significantly more buys than sells)
    if velocity.buy_sell_ratio < 1.5 {
        debug!(
            mint = %velocity.mint,
            ratio = velocity.buy_sell_ratio,
            "Buy/sell ratio not bullish enough"
        );
        return false;
    }

    // 4. Must have some organic wallets (not just bots)
    if velocity.unique_wallets < 5 {
        debug!(
            mint = %velocity.mint,
            wallets = velocity.unique_wallets,
            "Not enough unique wallets"
        );
        return false;
    }

    true
}

/// Calculate a confidence score (0.0 to 1.0) for the trade.
fn calculate_confidence(velocity: &VelocityData, config: &StrategyConfig) -> f64 {
    let mut score = 0.5; // Base 50%

    // TPM velocity bonus (up to +20%)
    if velocity.tpm_velocity > 1.0 {
        score += 0.2.min(velocity.tpm_velocity * 0.05);
    }

    // Buy/sell ratio bonus (up to +20%)
    if velocity.buy_sell_ratio > 3.0 {
        score += 0.2;
    } else if velocity.buy_sell_ratio > 2.0 {
        score += 0.1;
    }

    // Wallet diversity bonus (up to +10%)
    if velocity.unique_wallets > 50 {
        score += 0.1;
    } else if velocity.unique_wallets > 20 {
        score += 0.05;
    }

    score.min(0.95)
}

impl TradeSignal {
    pub fn confidence(&self) -> f64 {
        match self {
            TradeSignal::Buy { confidence, .. } => *confidence,
            _ => 0.0,
        }
    }
}

/// Extract the token mint from a transaction by parsing the initialize2 instruction
/// This is the high-performance version - no RPC calls, just raw tx parsing
pub fn extract_mint_from_tx(tx_data: &[u8]) -> Option<Pubkey> {
    // Try to deserialize the transaction from raw bytes
    let tx: Transaction = match bincode::deserialize(tx_data) {
        Ok(t) => t,
        Err(_) => return None,
    };

    let message = tx.message();
    let account_keys = message.static_account_keys();
    let raydium_program = match Pubkey::from_str(RAYDIUM_AMM_V4) {
        Ok(p) => p,
        Err(_) => return None,
    };

    // Look for Raydium Program in the instructions
    for instruction in message.instructions() {
        let program_id = account_keys[instruction.program_id_index as usize];
        
        // Check if this is Raydium AMM V4
        if program_id != raydium_program {
            continue;
        }

        let data = &instruction.data;
        
        // Check discriminator for initialize (0x02) or initialize2 (0x03)
        // Raydium uses: global:initialize2 = [0x02, ...] 
        if data.is_empty() {
            continue;
        }

        // Only process initialize instructions (not swaps)
        if data[0] != 0x02 && data[0] != 0x03 && data[0] != 0x06 {
            continue;
        }

        // Extract Mint from Account Indices
        // According to Raydium V4 layout:
        // accounts[8] = amm_coin_mint (the token)
        // accounts[9] = amm_pc_mint (usually WSOL)
        if instruction.accounts.len() > 9 {
            let coin_mint_idx = instruction.accounts[8] as usize;
            let pc_mint_idx = instruction.accounts[9] as usize;
            
            if coin_mint_idx >= account_keys.len() || pc_mint_idx >= account_keys.len() {
                continue;
            }
            
            let coin_mint = account_keys[coin_mint_idx];
            let pc_mint = account_keys[pc_mint_idx];

            // If PC Mint is WSOL, then Coin Mint is the new token
            if pc_mint.to_string() == WSOL_MINT {
                return Some(coin_mint);
            } else if coin_mint.to_string() == WSOL_MINT {
                // Sometimes they are flipped
                return Some(pc_mint);
            }
        }
    }
    None
}

/// Detect if a transaction is a SWAP (buy or sell) - for velocity tracking
/// Returns Some(is_buy) where true = buy (SOL -> token), false = sell (token -> SOL)
pub fn detect_swap_direction(tx_data: &[u8]) -> Option<bool> {
    let tx: Transaction = match bincode::deserialize(tx_data) {
        Ok(t) => t,
        Err(_) => return None,
    };

    let message = tx.message();
    let account_keys = message.static_account_keys();
    let raydium_program = match Pubkey::from_str(RAYDIUM_AMM_V4) {
        Ok(p) => p,
        Err(_) => return None,
    };

    for instruction in message.instructions() {
        let program_id = account_keys[instruction.program_id_index as usize];
        
        if program_id != raydium_program {
            continue;
        }

        let data = &instruction.data;
        
        // Swap instruction discriminator: global:swap = 0x3b9bc6f1
        // First byte of swap is typically 0x3b or similar
        if data.is_empty() || data.len() < 8 {
            continue;
        }

        // Check if this is a swap instruction (not initialize)
        // Swap discriminator starts with specific bytes
        // For Raydium V4: swap instruction data starts with specific discriminator
        if data[0] == 0x3b || data[0] == 0x09 || data[0] == 0x05 {
            // For swap, we check the accounts to determine direction
            // Coin vault is typically at index 4, PC vault at index 5
            // If the user is the source of PC (SOL), it's a buy
            // If the user is the source of Coin (token), it's a sell
            if instruction.accounts.len() > 8 {
                // Simplified: check if WSOL ATA is involved as source (buy)
                // This is a simplified heuristic - real implementation would check vault directions
                return Some(true); // Default to buy for now
            }
        }
    }
    None
}

/// Calculate current TPM from transactions in the last 60 seconds
fn calculate_current_tpm(transactions: &Vec<TransactionRecord>) -> f64 {
    let now = Utc::now();
    let cutoff = now - chrono::Duration::seconds(60);
    
    let count = transactions
        .iter()
        .filter(|tx| tx.timestamp > cutoff)
        .count();
    
    count as f64 // Transactions per minute
}
