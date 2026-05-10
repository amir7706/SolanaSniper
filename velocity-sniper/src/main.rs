pub mod config;
pub mod shred_listener;
pub mod raydium_detector;
pub mod safety_filter;
pub mod velocity_monitor;
pub mod swap_calculator;
pub mod bundle_executor;
pub mod strategy;
pub mod types;

use anyhow::Result;
use bs58;
use config::Config;
use solana_sdk::signer::Signer;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;
use types::{MintInfo, PoolEvent, TradeSignal, TradingState};

/// Application state shared across all modules
pub struct AppState {
    pub config: Config,
    pub trading_state: Arc<tokio::sync::RwLock<TradingState>>,
    /// Channel: ShredStream -> Raydium Detector
    pub raw_tx_sender: tokio::sync::mpsc::Sender<Vec<u8>>,
    pub raw_tx_receiver: tokio::sync::mpsc::Receiver<Vec<u8>>,
    /// Channel: ShredStream -> Velocity Monitor (ALL transactions for TPM tracking)
    pub tx_for_velocity_sender: tokio::sync::mpsc::Sender<Vec<u8>>,
    pub tx_for_velocity_receiver: tokio::sync::mpsc::Receiver<Vec<u8>>,
    /// Channel: Raydium Detector -> Safety Filter
    pub pool_event_sender: tokio::sync::broadcast::Sender<PoolEvent>,
    pub pool_event_receiver: tokio::sync::broadcast::Receiver<PoolEvent>,
    /// Channel: Safety Filter -> Velocity Monitor
    pub safe_token_sender: tokio::sync::broadcast::Sender<MintInfo>,
    pub safe_token_receiver: tokio::sync::broadcast::Receiver<MintInfo>,
    /// Channel: Velocity Monitor -> Strategy / Executor
    pub trade_signal_sender: tokio::sync::broadcast::Sender<TradeSignal>,
    pub trade_signal_receiver: tokio::sync::broadcast::Receiver<TradeSignal>,
}

impl AppState {
    pub fn new(config: Config) -> Self {
        let (raw_tx_sender, raw_tx_receiver) = tokio::sync::mpsc::channel::<Vec<u8>>(50_000);
        let (tx_for_velocity_sender, tx_for_velocity_receiver) = tokio::sync::mpsc::channel::<Vec<u8>>(50_000);
        let (pool_event_sender, pool_event_receiver) = tokio::sync::broadcast::channel(10_000);
        let (safe_token_sender, safe_token_receiver) = tokio::sync::broadcast::channel(1_000);
        let (trade_signal_sender, trade_signal_receiver) = tokio::sync::broadcast::channel(1_000);

        Self {
            config,
            trading_state: Arc::new(tokio::sync::RwLock::new(TradingState::default())),
            raw_tx_sender,
            raw_tx_receiver,
            tx_for_velocity_sender,
            tx_for_velocity_receiver,
            pool_event_sender,
            pool_event_receiver,
            safe_token_sender,
            safe_token_receiver,
            trade_signal_sender,
            trade_signal_receiver,
        }
    }
}

fn init_logging(config: &Config) {
    let filter = EnvFilter::try_new(&config.logging.log_level)
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true);

    if config.logging.json_logs {
        subscriber.json().init();
    } else {
        subscriber.init();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load()?;
    init_logging(&config);

    tracing::info!("=== Velocity Sniper v0.1.0 ===");
    tracing::info!("Loading configuration...");
    tracing::info!(
        jito_url = %config.jito.block_engine_url,
        shred_proxy = %config.shredstream.proxy_address,
        "Configuration loaded"
    );

    // Test: Load wallet from private key
    let key_bytes = bs58::decode(&config.trading.private_key_bs58)
        .into_vec()
        .map_err(|e| anyhow::anyhow!("Invalid private key: {}", e))?;
    if key_bytes.len() != 64 {
        anyhow::bail!("Private key must be 64 bytes");
    }
    let keypair = solana_sdk::signature::Keypair::from_bytes(&key_bytes)
        .map_err(|e| anyhow::anyhow!("Invalid keypair: {}", e))?;
    tracing::info!(wallet = %keypair.pubkey(), "Wallet loaded successfully");
    tracing::info!(max_sol_per_trade = config.trading.max_sol_per_trade, "Trading config");

    let state = AppState::new(config.clone());

    // Spawn all pipeline stages concurrently
    // Pass both raw_tx_sender (for detector) and tx_for_velocity_sender (for TPM tracking)
    let shred_handle = tokio::spawn(shred_listener::run(
        state.raw_tx_sender.clone(),
        state.tx_for_velocity_sender.clone(),
        config.shredstream.clone(),
    ));
    let detect_handle = tokio::spawn(raydium_detector::run(
        state.raw_tx_receiver,
        state.pool_event_sender.clone(),
    ));
    let safety_handle = tokio::spawn(safety_filter::run(
        state.pool_event_receiver.resubscribe(),
        state.safe_token_sender.clone(),
        config.safety.clone(),
        config.rpc.clone(),
    ));
    let velocity_handle = tokio::spawn(velocity_monitor::run(
        state.safe_token_receiver.resubscribe(),
        state.tx_for_velocity_receiver,
        state.trade_signal_sender.clone(),
        config.strategy.clone(),
    ));
    let strategy_handle = tokio::spawn(strategy::run(
        state.trade_signal_receiver.resubscribe(),
        state.trade_signal_sender.clone(),
        state.pool_event_receiver.resubscribe(),
        state.safe_token_receiver.resubscribe(),
        state.trading_state.clone(),
        config.strategy.clone(),
        config.trading.clone(),
        config.jito.clone(),
        config.rpc.clone(),
        config.safety.clone(),
    ));

    // Wait for Ctrl+C
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received shutdown signal, gracefully stopping...");
        }
        result = shred_handle => { if let Err(e) = result { tracing::error!("Shred listener error: {e}"); } }
        result = detect_handle => { if let Err(e) = result { tracing::error!("Detector error: {e}"); } }
        result = safety_handle => { if let Err(e) = result { tracing::error!("Safety filter error: {e}"); } }
        result = velocity_handle => { if let Err(e) = result { tracing::error!("Velocity monitor error: {e}"); } }
        result = strategy_handle => { if let Err(e) = result { tracing::error!("Strategy error: {e}"); } }
    }

    tracing::info!("Velocity Sniper shut down.");
    Ok(())
}

/// ─── Thread Isolation (The "Nitro" Rule) ───────────────────────────
/// Pins the current thread to the highest available CPU core to 
/// eliminate context switching and OS "pauses" (Linux only).
pub fn pin_thread_to_last_core(name: &str) {
    #[cfg(target_os = "linux")]
    {
        if let Some(cores) = core_affinity::get_core_ids() {
            if let Some(last_core) = cores.last() {
                if core_affinity::set_for_current(*last_core) {
                    tracing::info!(thread = name, core = ?last_core.id, "🚀 NITRO: Thread locked to physical core");
                }
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        tracing::debug!(thread = name, "Core pinning skipped (Not on Linux)");
    }
}
