use crate::config::{JitoConfig, RpcConfig, TradingConfig};
use crate::swap_calculator::SwapCalculator;
use crate::types::*;
use anyhow::Result;
use base64::Engine;
use chrono::Utc;
use rand::seq::SliceRandom;
use reqwest::Client;
use serde::Serialize;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Signer, Keypair},
    system_instruction::transfer,
    system_program,
    transaction::Transaction,
};
use spl_associated_token_account::instruction::create_associated_token_account;
use spl_token::instruction::sync_native;
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::{timeout, Duration};
use tracing::{debug, info, warn};

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

struct PoolData {
    pool: Pubkey,
    authority: Pubkey,
    base_vault: Pubkey,
    quote_vault: Pubkey,
    base_mint: Pubkey,
    quote_mint: Pubkey,
}

pub struct BundleExecutor {
    http: Client,
    jito_config: JitoConfig,
    rpc_config: RpcConfig,
    trading_config: TradingConfig,
    rpc_url: String,
    keypair: Arc<Keypair>,
    pending_sells: std::sync::Mutex<Vec<PendingSell>>,
    latest_blockhash: std::sync::Mutex<Option<solana_sdk::hash::Hash>>,
}

impl BundleExecutor {
    pub fn new(jito_config: JitoConfig, rpc_config: RpcConfig, trading_config: TradingConfig) -> Result<Self> {
        let rpc_url = rpc_config.premium_endpoint.clone().unwrap_or_else(|| rpc_config.endpoint.clone());
        let http = Client::builder().timeout(Duration::from_secs(10)).build().expect("Failed HTTP");
        let keypair = load_keypair(&trading_config.private_key_bs58)?;
        Ok(Self { http, jito_config, rpc_config, trading_config, rpc_url, keypair: Arc::new(keypair), pending_sells: std::sync::Mutex::new(Vec::new()), latest_blockhash: std::sync::Mutex::new(None) })
    }

    pub fn payer(&self) -> Pubkey { self.keypair.pubkey() }

    pub fn add_pending_sell(&self, sell: PendingSell) { self.pending_sells.lock().unwrap().push(sell); }
    pub fn get_pending_sells(&self) -> Vec<PendingSell> { self.pending_sells.lock().unwrap().clone() }
    pub fn remove_pending_sell(&self, mint: &Pubkey) -> Option<PendingSell> { let mut s = self.pending_sells.lock().unwrap(); if let Some(p) = s.iter().position(|x| x.mint == *mint) { Some(s.remove(p)) } else { None } }

    pub fn refresh_blockhash(&self) -> Result<()> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let bh = rt.block_on(async { self.get_blockhash_internal().await })?;
        let mut h = self.latest_blockhash.lock().unwrap(); *h = Some(bh); Ok(())
    }

    pub fn get_blockhash(&self) -> Result<solana_sdk::hash::Hash> { 
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut h = self.latest_blockhash.lock().unwrap(); 
        if h.is_none() { *h = Some(rt.block_on(async { self.get_blockhash_internal().await })?); } 
        Ok(h.unwrap()) 
    }

    pub async fn execute_buy(&self, mint: &Pubkey, pool: &Pubkey) -> Result<BundleResult> {
        info!(mint=%mint, pool=%pool, "Preparing atomic buy bundle");
        let pool_data = self.fetch_pool_data(pool).await?;
        let calc = SwapCalculator::new(self.http.clone(), self.rpc_url.clone());
        let (quote_reserve, base_reserve) = calc.fetch_pool_reserves(pool).await?;
        let input_lamports = (self.trading_config.max_sol_per_trade * LAMPORTS_PER_SOL as f64) as u64;
        let buy_quote = calc.calculate_swap(quote_reserve, base_reserve, input_lamports, 25);
        let min_output = ((buy_quote.output_amount as f64) * (1.0 - self.trading_config.max_slippage_pct)) as u64;
        
        let user = self.keypair.pubkey();
        let wsol_ata = self.compute_wsol_ata(&user);
        let token_ata = self.compute_ata(mint, &user);
        
        let tip = self.select_tip_account();
        let tip_ix = transfer(&user, &tip, self.jito_config.tip_lamports);
        let create_wsol_ix = create_associated_token_account(&user, &user, &WSOL_MINT.parse().unwrap(), &spl_token::id());
        let wrap_ix = transfer(&user, &wsol_ata, input_lamports);
        let sync_ix = sync_native(&spl_token::id(), &wsol_ata).unwrap();
        let create_token_ix = create_associated_token_account(&user, &user, mint, &spl_token::id());
        let buy_ix = self.build_buy_instruction(&pool_data, &token_ata, &wsol_ata, input_lamports, min_output);

        let bh = self.get_blockhash()?;
        let mut txs = Vec::new();
        for ixs in vec![vec![tip_ix.clone(), create_wsol_ix, wrap_ix, sync_ix], vec![create_token_ix, buy_ix]] {
            let mut tx = Transaction::new_with_payer(&ixs, Some(&user));
            tx.sign(&[self.keypair.as_ref()], bh);
            let mut ser = Vec::new(); tx.message.serialize(&mut ser)?; txs.push(ser);
        }

        let sim = self.simulate_bundle(&txs).await;
        if sim.as_ref().map_err(|e| anyhow::anyhow!("{:?}", e))? == false { return Ok(BundleResult { bundle_id: String::new(), accepted: false, simulated: true, landed: false, error: Some("Sim failed".into()) }); }
        let result = self.send_bundle(txs).await?;
        Ok(result)
    }

    pub async fn execute_sell_fast(&self, mint: &Pubkey, pool: &Pubkey, token_amount: u64, min_sol_output: u64) -> Result<BundleResult> {
        let bh = self.get_blockhash_internal().await?;
        let pool_data = self.fetch_pool_data(pool).await?;
        let tip = self.select_tip_account();
        let tip_ix = transfer(&self.keypair.pubkey(), &tip, self.jito_config.tip_lamports);
        let sell_ix = self.build_sell_instruction(&pool_data, mint, token_amount, min_sol_output);
        let bh2 = self.get_blockhash_internal().await?;
        let user = self.keypair.pubkey();
        let mut txs = Vec::new();
        for ixs in vec![vec![tip_ix.clone()], vec![sell_ix]] {
            let mut tx = Transaction::new_with_payer(&ixs, Some(&user));
            tx.sign(&[self.keypair.as_ref()], bh2);
            let mut ser = Vec::new(); tx.message.serialize(&mut ser)?; txs.push(ser);
        }
        self.remove_pending_sell(mint);
        self.send_bundle(txs).await
    }

    fn compute_ata(&self, mint: &Pubkey, owner: &Pubkey) -> Pubkey {
        let (ata, _) = Pubkey::find_program_address(&[&owner.to_bytes(), &spl_token::id().to_bytes(), &mint.to_bytes()], &spl_associated_token_account::id());
        ata
    }

    fn compute_wsol_ata(&self, owner: &Pubkey) -> Pubkey { let sol: Pubkey = WSOL_MINT.parse().unwrap(); let (ata, _) = Pubkey::find_program_address(&[&owner.to_bytes(), &spl_token::id().to_bytes(), &sol.to_bytes()], &spl_associated_token_account::id()); ata }

    fn select_tip_account(&self) -> Pubkey {
        use rand::Rng;
        if !self.jito_config.tip_accounts.is_empty() { let mut rng = rand::thread_rng(); let idx = rng.gen_range(0..self.jito_config.tip_accounts.len()); if let Ok(k) = Pubkey::from_str(self.jito_config.tip_accounts.get(idx).unwrap()) { return k; } }
        Pubkey::from_str("Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY").unwrap_or_default()
    }

    async fn get_blockhash_internal(&self) -> Result<solana_sdk::hash::Hash> {
        let body = serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "getLatestBlockhash", "params": [{"commitment": "confirmed"}]});
        let resp = timeout(Duration::from_secs(5), self.http.post(&self.rpc_url).json(&body).send()).await??;
        let json: serde_json::Value = resp.json().await?;
        Ok(json.pointer("/result/value/blockhash").and_then(|v| v.as_str()).ok_or_else(|| anyhow::anyhow!("No bh"))?.parse()?)
    }

    async fn fetch_pool_data(&self, pool: &Pubkey) -> Result<PoolData> {
        let body = serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "getAccountInfo", "params": [pool.to_string(), {"encoding": "base64"}]});
        let resp = self.http.post(&self.rpc_url).json(&body).send().await?;
        let json: serde_json::Value = resp.json().await?;
        let data = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, json.pointer("/result/value/data/0").and_then(|v| v.as_str()).ok_or_else(|| anyhow::anyhow!("No data"))?)?;
        if data.len() < 200 { anyhow::bail!("Too short") }
        Ok(PoolData { pool: *pool, authority: Pubkey::new_from_array(data[24..56].try_into()?), base_vault: Pubkey::new_from_array(data[152..184].try_into()?), quote_vault: Pubkey::new_from_array(data[184..216].try_into()?), base_mint: Pubkey::new_from_array(data[216..248].try_into()?), quote_mint: Pubkey::new_from_array(data[248..280].try_into()?) })
    }

    fn build_buy_instruction(&self, pd: &PoolData, dest: &Pubkey, wsol: &Pubkey, amt: u64, min_out: u64) -> Instruction {
        let user = self.keypair.pubkey();
        let data = SwapCalculator::new(Client::new(), self.rpc_url.clone()).build_swap_instruction_data(amt, min_out);
        Instruction { program_id: Pubkey::from_str(RAYDIUM_AMM_V4_PROGRAM_ID).unwrap(), accounts: vec![AccountMeta::new(pd.pool, false), AccountMeta::new_readonly(pd.authority, false), AccountMeta::new_readonly(Pubkey::default(), false), AccountMeta::new_readonly(Pubkey::default(), false), AccountMeta::new(pd.base_vault, false), AccountMeta::new(pd.quote_vault, false), AccountMeta::new_readonly(Pubkey::default(), false), AccountMeta::new(*wsol, false), AccountMeta::new(user, true), AccountMeta::new(*dest, false), AccountMeta::new(user, false), AccountMeta::new_readonly(Pubkey::default(), false), AccountMeta::new_readonly(Pubkey::default(), false), AccountMeta::new(user, true), AccountMeta::new_readonly(spl_token::id(), false)], data }
    }

    fn build_sell_instruction(&self, pd: &PoolData, mint: &Pubkey, amt: u64, min_out: u64) -> Instruction {
        let user = self.keypair.pubkey();
        let token_ata = self.compute_ata(mint, &user);
        let wsol = self.compute_wsol_ata(&user);
        let data = SwapCalculator::new(Client::new(), self.rpc_url.clone()).build_swap_instruction_data(amt, min_out);
        Instruction { program_id: Pubkey::from_str(RAYDIUM_AMM_V4_PROGRAM_ID).unwrap(), accounts: vec![AccountMeta::new(pd.pool, false), AccountMeta::new_readonly(pd.authority, false), AccountMeta::new_readonly(Pubkey::default(), false), AccountMeta::new_readonly(Pubkey::default(), false), AccountMeta::new(pd.base_vault, false), AccountMeta::new(pd.quote_vault, false), AccountMeta::new_readonly(Pubkey::default(), false), AccountMeta::new(token_ata, false), AccountMeta::new(wsol, false), AccountMeta::new(wsol, false), AccountMeta::new(user, false), AccountMeta::new_readonly(Pubkey::default(), false), AccountMeta::new_readonly(Pubkey::default(), false), AccountMeta::new(user, true), AccountMeta::new_readonly(spl_token::id(), false)], data }
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
        Ok(BundleResult { bundle_id: bid.clone(), accepted: bid.len() == 88, simulated: true, landed: false, error: err })
    }
}