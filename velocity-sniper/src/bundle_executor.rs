use crate::config::{JitoConfig, TradingConfig};
use crate::swap_calculator::SwapCalculator;
use crate::types::*;
use anyhow::Result;
use chrono::Utc;
use reqwest::Client;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Signer, Keypair},
    system_instruction::transfer,
    compute_budget::ComputeBudgetInstruction,
    transaction::Transaction,
};
use spl_associated_token_account::instruction::create_associated_token_account;
use spl_token::instruction::sync_native;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{timeout, Duration};
use tracing::{info, warn, debug};

const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

fn load_keypair(private_key_bs58: &str) -> Result<Keypair> {
    let bytes = bs58::decode(private_key_bs58)
        .into_vec()
        .map_err(|e| anyhow::anyhow!("Invalid base58 key: {}", e))?;
    if bytes.len() != 64 {
        anyhow::bail!("Keypair must be 64 bytes");
    }
    let keypair = Keypair::from_bytes(&bytes)
        .map_err(|e| anyhow::anyhow!("Invalid keypair: {}", e))?;
    Ok(keypair)
}

#[derive(Clone)]
pub struct PendingSell {
    pub mint: Pubkey,
    pub pool: Pubkey,
    pub token_amount: u64,
    pub entry_price_sol: f64,
    pub take_profit_price: f64,
    pub stop_loss_price: f64,
    pub entry_time: chrono::DateTime<Utc>,
}

pub struct PoolData {
    pub pool: Pubkey,
    pub authority: Pubkey,
    pub base_vault: Pubkey,
    pub quote_vault: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
}

#[derive(Clone)]
pub struct TradeLatency {
    pub detected_at: Option<Instant>,
    pub buy_sent_at: Option<Instant>,
    pub confirmed_at: Option<Instant>,
    pub exited_at: Option<Instant>,
}

impl TradeLatency {
    pub fn new() -> Self {
        Self {
            detected_at: None,
            buy_sent_at: None,
            confirmed_at: None,
            exited_at: None,
        }
    }
    
    pub fn set_detected(&mut self) {
        self.detected_at = Some(Instant::now());
    }
    
    pub fn set_buy_sent(&mut self) {
        self.buy_sent_at = Some(Instant::now());
    }
    
    pub fn set_confirmed(&mut self) {
        self.confirmed_at = Some(Instant::now());
    }
    
    pub fn set_exited(&mut self) {
        self.exited_at = Some(Instant::now());
    }
    
    pub fn detection_to_buy_ms(&self) -> Option<f64> {
        match (self.detected_at, self.buy_sent_at) {
            (Some(d), Some(b)) => Some(d.elapsed().as_secs_f64() * 1000.0),
            _ => None,
        }
    }
    
    pub fn buy_to_confirm_ms(&self) -> Option<f64> {
        match (self.buy_sent_at, self.confirmed_at) {
            (Some(b), Some(c)) => Some(b.elapsed().as_secs_f64() * 1000.0),
            _ => None,
        }
    }
    
    pub fn total_duration_ms(&self) -> Option<f64> {
        match (self.detected_at, self.exited_at) {
            (Some(d), Some(e)) => Some(d.elapsed().as_secs_f64() * 1000.0),
            _ => None,
        }
    }
}

impl Default for TradeLatency {
    fn default() -> Self { Self::new() }
}

pub struct BundleExecutor {
    http: Client,
    jito_config: JitoConfig,
    trading_config: TradingConfig,
    rpc_url: String,
    keypair: Arc<Keypair>,
    pending_sells: std::sync::Mutex<Vec<PendingSell>>,
    latest_blockhash: std::sync::Mutex<Option<solana_sdk::hash::Hash>>,
    latency: std::sync::Mutex<HashMap<String, TradeLatency>>,
    pub atomic_running: AtomicBool,
    pub atomic_checks: AtomicU64,
}

impl BundleExecutor {
    pub fn new(jito_config: JitoConfig, trading_config: TradingConfig, rpc_url: String) -> Result<Self> {
        let http = Client::builder().timeout(Duration::from_secs(10)).build().expect("Failed HTTP");
        let keypair = load_keypair(&trading_config.private_key_bs58)?;
        Ok(Self { 
            http, 
            jito_config, 
            trading_config, 
            rpc_url, 
            keypair: Arc::new(keypair), 
            pending_sells: std::sync::Mutex::new(Vec::new()), 
            latest_blockhash: std::sync::Mutex::new(None),
            latency: std::sync::Mutex::new(HashMap::new()),
            atomic_running: AtomicBool::new(true),
            atomic_checks: AtomicU64::new(0),
        })
    }

    pub fn payer(&self) -> Pubkey { self.keypair.pubkey() }

    pub fn add_pending_sell(&self, sell: PendingSell) { self.pending_sells.lock().unwrap().push(sell); }
    pub fn get_pending_sells(&self) -> Vec<PendingSell> { self.pending_sells.lock().unwrap().clone() }
    pub fn remove_pending_sell(&self, mint: &Pubkey) -> Option<PendingSell> { 
        let mut s = self.pending_sells.lock().unwrap(); 
        if let Some(p) = s.iter().position(|x| x.mint == *mint) { 
            Some(s.remove(p)) 
        } else { 
            None 
        } 
    }
    
    pub fn track_detection(&self, mint: &str) {
        let mut lat = self.latency.lock().unwrap();
        let entry = lat.entry(mint.to_string()).or_insert_with(TradeLatency::new);
        entry.set_detected();
        debug!(mint = %mint, "Detection timestamp recorded");
    }
    
    pub fn track_buy_sent(&self, mint: &str) {
        let mut lat = self.latency.lock().unwrap();
        if let Some(entry) = lat.get_mut(mint) {
            entry.set_buy_sent();
            if let Some(ms) = entry.detection_to_buy_ms() {
                info!(mint = %mint, latency_ms = format!("{:.2}", ms), "[LATENCY] Detection→Buy: {:.2}ms", ms);
            }
        }
    }
    
    pub fn track_confirmed(&self, mint: &str) {
        let mut lat = self.latency.lock().unwrap();
        if let Some(entry) = lat.get_mut(mint) {
            entry.set_confirmed();
            if let Some(ms) = entry.buy_to_confirm_ms() {
                info!(mint = %mint, confirm_ms = format!("{:.2}", ms), "[LATENCY] Buy→Confirm: {:.2}ms (3 slots)", ms);
            }
        }
    }
    
    pub fn track_exit(&self, mint: &str, reason: &str) {
        let mut lat = self.latency.lock().unwrap();
        if let Some(entry) = lat.get_mut(mint) {
            entry.set_exited();
            if let Some(total_ms) = entry.total_duration_ms() {
                info!(mint = %mint, duration_ms = format!("{:.0}", total_ms), reason = %reason, "[TRADE] Duration: {:.0}ms | Reason: {}", total_ms, reason);
            }
        }
        lat.remove(mint);
    }

    pub async fn get_blockhash_internal(&self) -> Result<solana_sdk::hash::Hash> {
        let body = serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "getLatestBlockhash", "params": [{"commitment": "confirmed"}]});
        let resp = timeout(Duration::from_secs(5), self.http.post(&self.rpc_url).json(&body).send()).await??;
        let json: serde_json::Value = resp.json().await?;
        Ok(json.pointer("/result/value/blockhash")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("No bh"))?
            .parse()?)
    }

    pub fn get_blockhash_sync(&self) -> Result<solana_sdk::hash::Hash> {
        let h = self.latest_blockhash.lock().unwrap();
        if let Some(bh) = *h {
            Ok(bh)
        } else {
            anyhow::bail!("No blockhash available. Call get_blockhash_internal first.")
        }
    }

    pub async fn refresh_blockhash(&self) -> Result<()> {
        let bh = self.get_blockhash_internal().await?;
        let mut h = self.latest_blockhash.lock().unwrap(); 
        *h = Some(bh); 
        Ok(())
    }

    pub async fn execute_buy(&self, mint: &Pubkey, pool: &Pubkey) -> Result<BundleResult> {
        info!(mint=%mint, pool=%pool, "Preparing atomic buy bundle");
        let pool_data = self.fetch_pool_data(pool).await?;
        
        let input_lamports = (self.trading_config.max_sol_per_trade * LAMPORTS_PER_SOL as f64) as u64;
        // Simple slippage calculation for now
        let min_output = 0; // Will be refined with real quotes

        let user = self.keypair.pubkey();
        let token_ata = self.compute_ata(mint, &user);
        let wsol_ata = self.compute_wsol_ata(&user);
        
        let tip = self.select_tip_account();
        let tip_ix = transfer(&user, &tip, self.jito_config.tip_lamports);
        
        let cu_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(200_000);
        let cu_price_ix = ComputeBudgetInstruction::set_compute_unit_price(self.trading_config.priority_fee_lamports);
        
        let create_wsol_ix = create_associated_token_account(&user, &user, &Pubkey::from_str(WSOL_MINT).unwrap(), &spl_token::id());
        let wrap_ix = transfer(&user, &wsol_ata, input_lamports);
        let sync_ix = sync_native(&spl_token::id(), &wsol_ata).unwrap();
        let create_token_ix = create_associated_token_account(&user, &user, mint, &spl_token::id());
        let buy_ix = self.build_buy_instruction(&pool_data, &token_ata, &wsol_ata, input_lamports, min_output);

        let bh = self.get_blockhash_internal().await?;
        let mut txs = Vec::new();
        
        for ixs in vec![
            vec![cu_limit_ix.clone(), cu_price_ix.clone(), tip_ix.clone(), create_wsol_ix, wrap_ix, sync_ix], 
            vec![cu_limit_ix, cu_price_ix, create_token_ix, buy_ix]
        ] {
            let mut tx = Transaction::new_with_payer(&ixs, Some(&user));
            tx.sign(&[self.keypair.as_ref()], bh);
            txs.push(tx.message.serialize());
        }

        let sim = self.simulate_bundle(&txs).await;
        if !sim.unwrap_or(false) { 
            return Ok(BundleResult { 
                bundle_id: String::new(), 
                accepted: false, 
                simulated: true, 
                landed: false, 
                error: Some("Sim failed".into()) 
            }); 
        }
        self.send_bundle(txs).await
    }

    pub async fn execute_sell_fast(&self, mint: &Pubkey, pool: &Pubkey, token_amount: u64, min_sol_output: u64) -> Result<BundleResult> {
        let bh = self.get_blockhash_internal().await?;
        let pool_data = self.fetch_pool_data(pool).await?;
        
        // Dynamic Jito Tip: Scale with expected profit
        // If min_sol_output > entry (profitable), use higher tip
        // If min_sol_output <= entry (stop loss/panic), use minimum tip
        let entry_value = self.trading_config.max_sol_per_trade * LAMPORTS_PER_SOL as f64;
        let expected_profit_pct = ((min_sol_output as f64 - entry_value) / entry_value) * 100.0;
        
        let tip_amount = if expected_profit_pct > 20.0 {
            // Strong profit - use higher tip to land fast
            self.jito_config.tip_lamports
        } else if expected_profit_pct > 0.0 {
            // Small profit - use standard tip
            (self.jito_config.tip_lamports as f64 * 0.7) as u64
        } else {
            // Loss or breakeven - use minimum tip
            10_000
        };
        
        let tip = self.select_tip_account();
        let tip_ix = transfer(&self.keypair.pubkey(), &tip, tip_amount);
        
        let cu_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(200_000);
        let cu_price_ix = ComputeBudgetInstruction::set_compute_unit_price(self.trading_config.priority_fee_lamports);
        
        // Use 20% slippage for sell (loose - get out fast)
        let sell_min_out = (min_sol_output as f64 * 0.80) as u64;
        let sell_ix = self.build_sell_instruction(&pool_data, mint, token_amount, sell_min_out);
        
        let user = self.keypair.pubkey();
        let token_ata = self.compute_ata(mint, &user);
        
        // Add Close Account instruction to reclaim rent (~0.002 SOL / $0.40)
        let close_ix = spl_token::instruction::close_account(&spl_token::id(), &token_ata, &user, &user, &[]).unwrap();
        
        let mut txs = Vec::new();
        // Transaction 1: Tip + Compute + Close Account (reclaim rent)
        let tx1_ixs = vec![cu_limit_ix, cu_price_ix, tip_ix.clone(), close_ix];
        let mut tx1 = Transaction::new_with_payer(&tx1_ixs, Some(&user));
        tx1.sign(&[self.keypair.as_ref()], bh);
        txs.push(tx1.message.serialize());
        
        // Transaction 2: Sell (20% slippage for fast exit)
        let tx2_ixs = vec![sell_ix];
        let mut tx2 = Transaction::new_with_payer(&tx2_ixs, Some(&user));
        tx2.sign(&[self.keypair.as_ref()], bh);
        txs.push(tx2.message.serialize());
        
        self.send_bundle(txs).await
    }

    pub async fn confirm_buy_and_remove(&self, mint: &Pubkey, tx_signature: &str) -> bool {
        // 3-Slot Rule: Check if transaction confirmed (~1.6 seconds / 3 slots)
        // If not confirmed, clear the pending state
        tokio::time::sleep(Duration::from_millis(1600)).await;
        
        let body = serde_json::json!({
            "jsonrpc": "2.0", 
            "id": 1, 
            "method": "getTransaction", 
            "params": [tx_signature, {"commitment": "confirmed"}]
        });
        
        match timeout(Duration::from_secs(3), self.http.post(&self.rpc_url).json(&body).send()).await {
            Ok(Ok(resp)) => {
                match resp.json::<serde_json::Value>().await {
                    Ok(json) => {
                        if json.pointer("/result").is_some() {
                            info!(mint = %mint, tx = %tx_signature, "✅ Buy CONFIRMED on-chain");
                            self.track_confirmed(&mint.to_string());
                            self.remove_pending_sell(mint);
                            return true;
                        } else {
                            warn!(mint = %mint, tx = %tx_signature, "❌ Buy FAILED - not confirmed after 3 slots");
                            self.remove_pending_sell(mint);
                            return false;
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "❌ Failed to parse confirmation response");
                        self.remove_pending_sell(mint);
                        return false;
                    }
                }
            }
            _ => {
                warn!(mint = %mint, tx = %tx_signature, "❌ Buy TIMEOUT - not confirmed after 3 slots");
                self.remove_pending_sell(mint);
                return false;
            }
        }
    }

    fn compute_ata(&self, mint: &Pubkey, owner: &Pubkey) -> Pubkey {
        let (ata, _) = Pubkey::find_program_address(
            &[&owner.to_bytes(), &spl_token::id().to_bytes(), &mint.to_bytes()], 
            &spl_associated_token_account::id()
        );
        ata
    }

    fn compute_wsol_ata(&self, owner: &Pubkey) -> Pubkey { 
        let sol: Pubkey = Pubkey::from_str(WSOL_MINT).unwrap(); 
        let (ata, _) = Pubkey::find_program_address(
            &[&owner.to_bytes(), &spl_token::id().to_bytes(), &sol.to_bytes()], 
            &spl_associated_token_account::id()
        ); 
        ata 
    }

    fn select_tip_account(&self) -> Pubkey {
        use rand::Rng;
        if !self.jito_config.tip_accounts.is_empty() { 
            let mut rng = rand::thread_rng(); 
            let idx = rng.gen_range(0..self.jito_config.tip_accounts.len()); 
            if let Ok(k) = Pubkey::from_str(self.jito_config.tip_accounts.get(idx).unwrap()) { 
                return k; 
            } 
        }
        Pubkey::from_str("Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY").unwrap_or_default()
    }

    async fn fetch_pool_data(&self, pool: &Pubkey) -> Result<PoolData> {
        let body = serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "getAccountInfo", "params": [pool.to_string(), {"encoding": "base64"}]});
        let resp = self.http.post(&self.rpc_url).json(&body).send().await?;
        let json: serde_json::Value = resp.json().await?;
        let data_str = json.pointer("/result/value/data/0").and_then(|v| v.as_str()).ok_or_else(|| anyhow::anyhow!("No data"))?;
        let data = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data_str)?;
        
        if data.len() < 280 { anyhow::bail!("Too short") }
        Ok(PoolData { 
            pool: *pool, 
            authority: Pubkey::new_from_array(data[24..56].try_into()?), 
            base_vault: Pubkey::new_from_array(data[152..184].try_into()?), 
            quote_vault: Pubkey::new_from_array(data[184..216].try_into()?), 
            base_mint: Pubkey::new_from_array(data[216..248].try_into()?), 
            quote_mint: Pubkey::new_from_array(data[248..280].try_into()?) 
        })
    }

    fn build_buy_instruction(&self, pd: &PoolData, dest: &Pubkey, wsol: &Pubkey, amt: u64, min_out: u64) -> Instruction {
        let user = self.keypair.pubkey();
        let data = SwapCalculator::build_swap_instruction_data(amt, min_out);
        Instruction { 
            program_id: Pubkey::from_str(RAYDIUM_AMM_V4_PROGRAM_ID).unwrap(), 
            accounts: vec![
                AccountMeta::new(pd.pool, false), 
                AccountMeta::new_readonly(pd.authority, false), 
                AccountMeta::new_readonly(Pubkey::default(), false), 
                AccountMeta::new_readonly(Pubkey::default(), false), 
                AccountMeta::new(pd.base_vault, false), 
                AccountMeta::new(pd.quote_vault, false), 
                AccountMeta::new_readonly(Pubkey::default(), false), 
                AccountMeta::new(*wsol, false), 
                AccountMeta::new(user, true), 
                AccountMeta::new(*dest, false), 
                AccountMeta::new(user, false), 
                AccountMeta::new_readonly(Pubkey::default(), false), 
                AccountMeta::new_readonly(Pubkey::default(), false), 
                AccountMeta::new(user, true), 
                AccountMeta::new_readonly(spl_token::id(), false)
            ], 
            data 
        }
    }

    fn build_sell_instruction(&self, pd: &PoolData, mint: &Pubkey, amt: u64, min_out: u64) -> Instruction {
        let user = self.keypair.pubkey();
        let token_ata = self.compute_ata(mint, &user);
        let wsol = self.compute_wsol_ata(&user);
        let data = SwapCalculator::build_swap_instruction_data(amt, min_out);
        Instruction { 
            program_id: Pubkey::from_str(RAYDIUM_AMM_V4_PROGRAM_ID).unwrap(), 
            accounts: vec![
                AccountMeta::new(pd.pool, false), 
                AccountMeta::new_readonly(pd.authority, false), 
                AccountMeta::new_readonly(Pubkey::default(), false), 
                AccountMeta::new_readonly(Pubkey::default(), false), 
                AccountMeta::new(pd.base_vault, false), 
                AccountMeta::new(pd.quote_vault, false), 
                AccountMeta::new_readonly(Pubkey::default(), false), 
                AccountMeta::new(token_ata, false), 
                AccountMeta::new(wsol, false), 
                AccountMeta::new(wsol, false), 
                AccountMeta::new(user, false), 
                AccountMeta::new_readonly(Pubkey::default(), false), 
                AccountMeta::new_readonly(Pubkey::default(), false), 
                AccountMeta::new(user, true), 
                AccountMeta::new_readonly(spl_token::id(), false)
            ], 
            data 
        }
    }

    async fn simulate_bundle(&self, txs: &[Vec<u8>]) -> Result<bool> {
        let url = format!("{}/api/v1/bundles/simulate", self.jito_config.block_engine_url);
        let body = serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "simulateBundle", "params": [txs]});
        let resp = timeout(Duration::from_millis(self.jito_config.simulation_timeout_ms), self.http.post(&url).json(&body).send()).await??;
        let json: serde_json::Value = resp.json().await?;
        Ok(json.pointer("/result/value").and_then(|v| v.as_bool()).unwrap_or(false))
    }

    async fn send_bundle(&self, txs: Vec<Vec<u8>>) -> Result<BundleResult> {
        let url = format!("{}/api/v1/bundles", self.jito_config.block_engine_url);
        let body = serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "sendBundle", "params": [txs]});
        let resp = timeout(Duration::from_secs(5), self.http.post(&url).json(&body).send()).await??;
        let json: serde_json::Value = resp.json().await?;
        let bid = json.pointer("/result/value").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let err = json.pointer("/error/message").and_then(|v| v.as_str()).map(String::from);
        Ok(BundleResult { 
            bundle_id: bid.clone(), 
            accepted: bid.len() == 88, 
            simulated: true, 
            landed: false, 
            error: err 
        })
    }
}