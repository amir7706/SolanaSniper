use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use chrono::{DateTime, Utc};
use dashmap::DashMap;

// ─── Solana Constants ───────────────────────────────────────────────

/// Raydium Liquidity Pool V4 Program ID
pub const RAYDIUM_AMM_V4_PROGRAM_ID: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";

/// Pump.fun Program ID (2025-2026)
pub const PUMP_FUN_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

/// Pump.fun Migration Authority
pub const PUMP_FUN_MIGRATION_AUTHORITY: &str = "39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg";

/// Pump.fun Bonding Curve
pub const PUMP_FUN_BONDING_CURVE: &str = "CEztj5MZKsYYXDES9q8Js3W6miAJ3eFb4Zxn3HRMeCFP";

/// Raydium AMM V4 Authority
pub const RAYDIUM_AUTHORITY: &str = "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5EQ5uGeL";

/// Associated Token Program
pub const ASSOCIATED_TOKEN_PROGRAM: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";

/// Token Program
pub const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

/// System Program
pub const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";

/// Rent Sysvar
pub const RENT_SYSVAR: &str = "SysvarRent111111111111111111111111111111111";

/// Clock Sysvar
pub const CLOCK_SYSVAR: &str = "SysvarC1ock11111111111111111111111111111111";

/// Safe "dead" address for burned LP tokens
pub const DEAD_ADDRESS: &str = "1nc1nerator11111111111111111111111111111111";

/// Raydium initialize2 instruction discriminator (8-byte anchor prefix)
/// sha256("global:initialize2")[..8] -> used to identify the exact instruction
pub const INITIALIZE2_DISCRIMINATOR: [u8; 8] = [0x02, 0xb1, 0x3c, 0x04, 0x5c, 0x15, 0x86, 0x51];

/// Raydium initialize instruction discriminator
pub const INITIALIZE_DISCRIMINATOR: [u8; 8] = [0x06, 0x6a, 0xe5, 0xab, 0xd9, 0xf1, 0xe7, 0xa3];

/// SOL decimals
pub const LAMPORTS_PER_SOL: u64 = 1_000_000_000;

// ─── Core Data Types ────────────────────────────────────────────────

/// A detected Raydium pool creation event from ShredStream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolEvent {
    /// The signature of the transaction containing initialize2
    pub tx_signature: String,
    /// Slot in which the transaction was included
    pub slot: u64,
    /// Timestamp when the shred was received
    pub detected_at: DateTime<Utc>,
    /// The mint address of the token being migrated
    pub base_mint: Pubkey,
    /// Quote mint (typically SOL/wrapped SOL)
    pub quote_mint: Pubkey,
    /// The newly created liquidity pool address
    pub pool_address: Pubkey,
    /// LP token mint address
    pub lp_mint: Pubkey,
    /// Base vault (token reserve) address
    pub base_vault: Pubkey,
    /// Quote vault (SOL reserve) address
    pub quote_vault: Pubkey,
    /// Initial base token amount deposited
    pub base_amount: u64,
    /// Initial quote amount (SOL in lamports) deposited
    pub quote_amount: u64,
    /// The account that invoked the migration (usually Pump.fun)
    pub authority: Pubkey,
    /// Raw instruction data for further analysis
    pub raw_data: Vec<u8>,
}

/// Comprehensive token metadata gathered from on-chain accounts
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MintInfo {
    pub mint: Pubkey,
    pub pool: Pubkey,
    pub detected_at: DateTime<Utc>,
    pub decimals: u8,
    pub supply: u64,
    pub mint_authority: Option<Pubkey>,
    pub freeze_authority: Option<Pubkey>,
    pub lp_mint: Pubkey,
    pub lp_burned: bool,
    pub holders: Vec<HolderInfo>,
    pub top_holder_pct: f64,
    pub is_safe: bool,
    pub rejection_reasons: Vec<String>,
}

/// Individual holder information for safety analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HolderInfo {
    pub address: Pubkey,
    pub balance: u64,
    pub pct_of_supply: f64,
    pub wallet_created_at: Option<DateTime<Utc>>,
    pub is_old_wallet: bool,
}

/// Trade signal emitted by the velocity monitor when conditions are met
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TradeSignal {
    /// A new token has passed safety and velocity checks — ready to buy
    Buy {
        mint: Pubkey,
        pool: Pubkey,
        confidence: f64,
        velocity_tpm: f64,
        buy_pressure_ratio: f64,
        trigger_reason: String,
    },
    /// Sell signal — take profit or stop loss reached
    Sell {
        mint: Pubkey,
        pool: Pubkey,
        reason: SellReason,
        entry_price: f64,
        current_price: f64,
        pnl_pct: f64,
        held_seconds: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SellReason {
    TakeProfit,
    StopLoss,
    VelocityDrop,
    MaxHoldTime,
    Manual,
}

/// Live velocity tracking data for a specific token
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VelocityData {
    pub mint: Pubkey,
    pub pool: Pubkey,
    /// Transactions in the current sliding window
    pub transactions: Vec<TransactionRecord>,
    /// Current TPM (transactions per minute)
    pub current_tpm: f64,
    /// TPM at minute 1 (for comparison)
    pub minute1_tpm: f64,
    /// TPM at minute 2 (for comparison)
    pub minute2_tpm: f64,
    /// TPM velocity (rate of change)
    pub tpm_velocity: f64,
    /// Buy count in current window
    pub buy_count: usize,
    /// Sell count in current window
    pub sell_count: usize,
    /// Buy/sell pressure ratio (1.0 = balanced, >1 = bullish)
    pub buy_sell_ratio: f64,
    /// Unique wallets active in current window
    pub unique_wallets: usize,
    /// Old wallet count (>1 year)
    pub old_wallet_count: usize,
    /// Whether this token has been flagged for execution
    pub triggered: bool,
}

/// A single parsed transaction from the shred
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionRecord {
    pub signature: String,
    pub slot: u64,
    pub timestamp: DateTime<Utc>,
    pub is_buy: bool,
    pub amount_sol: f64,
    pub amount_tokens: f64,
    pub wallet: Pubkey,
    pub wallet_age_days: Option<u64>,
    pub program_id: String,
}

/// Swap computation result from local calculator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapQuote {
    pub input_mint: Pubkey,
    pub output_mint: Pubkey,
    pub input_amount: u64,
    pub output_amount: u64,
    pub price_impact_pct: f64,
    pub route: Vec<SwapStep>,
    pub fee_lamports: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapStep {
    pub pool: Pubkey,
    pub input_mint: Pubkey,
    pub output_mint: Pubkey,
    pub input_amount: u64,
    pub output_amount: u64,
    pub fee_pct: f64,
}

/// Active position being tracked
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivePosition {
    pub mint: Pubkey,
    pub pool: Pubkey,
    pub entry_tx_signature: String,
    pub entry_price_sol: f64,
    pub amount_tokens: u64,
    pub invested_sol: f64,
    pub entry_time: DateTime<Utc>,
    pub take_profit_price: f64,
    pub stop_loss_price: f64,
}

/// Global trading state
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TradingState {
    pub active_positions: Vec<ActivePosition>,
    pub completed_trades: Vec<CompletedTrade>,
    pub total_pnl_sol: f64,
    pub total_trades: usize,
    pub winning_trades: usize,
    pub skipped_tokens: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletedTrade {
    pub mint: Pubkey,
    pub entry_price: f64,
    pub exit_price: f64,
    pub pnl_sol: f64,
    pub pnl_pct: f64,
    pub held_seconds: u64,
    pub reason: SellReason,
}

/// Jito Bundle submission result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleResult {
    pub bundle_id: String,
    pub accepted: bool,
    pub simulated: bool,
    pub landed: bool,
    pub error: Option<String>,
}

/// ─── Security Cache (RPC Bypass) ───────────────────────────────────

/// Instant security info parsed from raw shreds
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MintSecurity {
    pub mint: Pubkey,
    pub mint_authority: Option<Pubkey>,
    pub freeze_authority: Option<Pubkey>,
    pub decimals: u8,
    pub detected_at: DateTime<Utc>,
}

impl MintSecurity {
    pub fn is_renounced(&self) -> bool {
        self.mint_authority.is_none()
    }

    pub fn is_freeze_disabled(&self) -> bool {
        self.freeze_authority.is_none()
    }
}


// Global cache for RPC bypass (The "Poor Man's Geyser")
lazy_static::lazy_static! {
    pub static ref SECURITY_CACHE: DashMap<Pubkey, MintSecurity, ahash::RandomState> = DashMap::with_hasher(ahash::RandomState::new());
}

/// ─── Zero-Copy "Nitro" Parser Primitives ───────────────────────────
/// These helpers allow us to walk raw bytes without allocating memory
/// or using slow deserializers like bincode.

pub struct ZeroCopyWalker<'a> {
    pub data: &'a [u8],
    pub offset: usize,
}

impl<'a> ZeroCopyWalker<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    #[inline(always)]
    pub fn read_u8(&mut self) -> Option<u8> {
        let val = self.data.get(self.offset)?;
        self.offset += 1;
        Some(*val)
    }

    #[inline(always)]
    pub fn read_bytes(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.offset + len;
        if end > self.data.len() { return None; }
        let slice = &self.data[self.offset..end];
        self.offset = end;
        Some(slice)
    }

    #[inline(always)]
    pub fn read_compact_u16(&mut self) -> Option<usize> {
        let first = self.read_u8()? as usize;
        if first < 0x80 {
            Some(first)
        } else {
            let second = self.read_u8()? as usize;
            Some(((first & 0x7F) << 7) | second)
        }
    }

    #[inline(always)]
    pub fn read_pubkey(&mut self) -> Option<Pubkey> {
        let bytes = self.read_bytes(32)?;
        Some(Pubkey::new_from_array(bytes.try_into().ok()?))
    }
}
