use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub rpc: RpcConfig,
    pub jito: JitoConfig,
    pub shredstream: ShredStreamConfig,
    pub trading: TradingConfig,
    pub safety: SafetyConfig,
    pub strategy: StrategyConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RpcConfig {
    /// Solana RPC endpoint (e.g., https://api.mainnet-beta.solana.com)
    pub endpoint: String,
    /// WSS endpoint for confirmed slot subscriptions
    pub ws_endpoint: String,
    /// Helius / QuickNode / Triton RPC for faster confirmation
    pub premium_endpoint: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JitoConfig {
    /// Jito Block Engine URL (Frankfurt: https://frankfurt.mainnet.block-engine.jito.wtf)
    pub block_engine_url: String,
    /// Jito tip accounts for bundle priority fees
    pub tip_accounts: Vec<String>,
    /// Base tip in lamports per bundle
    pub tip_lamports: u64,
    /// Simulation timeout in milliseconds
    pub simulation_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShredStreamConfig {
    /// UDP bind address (typically 0.0.0.0:0 for OS-assigned)
    pub bind_address: String,
    /// Jito ShredStream proxy address (e.g., Frankfurt: 185.186.128.136:1002)
    pub proxy_address: String,
    /// Buffer size for UDP socket
    pub recv_buffer_size: usize,
    /// Number of processing threads
    pub worker_threads: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradingConfig {
    /// Private key for the trading wallet (base58 encoded)
    pub private_key_bs58: String,
    /// Maximum SOL to spend per trade (in SOL, not lamports)
    pub max_sol_per_trade: f64,
    /// Take profit percentage (e.g., 0.15 = 15%)
    pub take_profit_pct: f64,
    /// Stop loss percentage (e.g., 0.05 = 5%)
    pub stop_loss_pct: f64,
    /// Maximum slippage for swaps (e.g., 0.30 = 30%)
    pub max_slippage_pct: f64,
    /// How long to hold before selling (seconds)
    pub max_hold_seconds: u64,
    /// Minimum pool liquidity in SOL to consider a trade
    pub min_pool_liquidity_sol: f64,
    /// Priority fee in lamports to pay to Solana leaders
    pub priority_fee_lamports: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SafetyConfig {
    /// Check that mint authority is renounced (disabled = cannot mint more)
    pub require_mint_authority_disabled: bool,
    /// Check that LP tokens are burned
    pub require_lp_burned: bool,
    /// Maximum percentage a single holder can own (0.15 = 15%)
    pub max_single_holder_pct: f64,
    /// Check top 10 holders combined
    pub max_top10_holders_pct: f64,
    /// Freeze authority must be disabled
    pub require_freeze_authority_disabled: bool,
    /// Minimum number of unique holders
    pub min_unique_holders: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyConfig {
    /// Stage 1: Wait time after pool init before buying (seconds)
    /// "The War Zone" avoidance - wait 2 minutes for the first wave to settle
    pub war_zone_wait_seconds: u64,
    /// Stage 2: Minimum TPM (Transactions Per Minute) increase to confirm momentum
    pub min_tpm_increase_pct: f64,
    /// Stage 2: Minimum number of transactions in minute 2 to qualify
    pub min_minute2_transactions: usize,
    /// Stage 3: Seconds after trigger to execute the buy
    pub execution_delay_ms: u64,
    /// Maximum concurrent positions
    pub max_concurrent_positions: usize,
    /// Cooldown between trades in seconds
    pub trade_cooldown_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    pub log_level: String,
    pub log_file: Option<String>,
    pub json_logs: bool,
}

impl Config {
    pub fn load() -> anyhow::Result<Self> {
        // Load .env file if present
        let _ = dotenvy::dotenv();

        let config_path = std::env::var("CONFIG_PATH")
            .unwrap_or_else(|_| "config.toml".to_string());

        let config_str = std::fs::read_to_string(&config_path)
            .map_err(|e| anyhow::anyhow!("Failed to read config file {}: {}", config_path, e))?;

        let config: Config = toml::from_str(&config_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse config: {}", e))?;

        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.trading.max_sol_per_trade <= 0.0 {
            anyhow::bail!("max_sol_per_trade must be > 0");
        }
        if self.trading.take_profit_pct <= 0.0 || self.trading.take_profit_pct > 10.0 {
            anyhow::bail!("take_profit_pct must be between 0 and 10");
        }
        if self.trading.stop_loss_pct <= 0.0 || self.trading.stop_loss_pct > 1.0 {
            anyhow::bail!("stop_loss_pct must be between 0 and 1");
        }
        if self.trading.max_slippage_pct <= 0.0 || self.trading.max_slippage_pct > 1.0 {
            anyhow::bail!("max_slippage_pct must be between 0 and 1");
        }
        if self.safety.max_single_holder_pct <= 0.0 || self.safety.max_single_holder_pct > 1.0 {
            anyhow::bail!("max_single_holder_pct must be between 0 and 1");
        }
        if self.jito.block_engine_url.is_empty() {
            anyhow::bail!("jito.block_engine_url must be set");
        }
        Ok(())
    }
}
