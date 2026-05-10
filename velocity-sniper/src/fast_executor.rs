use crate::config::{JitoConfig, TradingConfig};
use crate::precomputed::MINT_OFFSET;
use crate::types::*;
use anyhow::Result;
use solana_sdk::{
    pubkey::Pubkey,
    signature::Signer,
    transaction::Transaction,
    instruction::Instruction,
};
use std::sync::Arc;

pub struct FastExecutor {
    buy_template: Vec<u8>,
    sell_template: Vec<u8>,
    keypair: Arc<solana_sdk::signature::Keypair>,
    jito_config: JitoConfig,
    rpc_url: String,
}

impl FastExecutor {
    pub fn new(jito_config: JitoConfig, trading_config: TradingConfig, rpc_url: String) -> Result<Self> {
        let key_bytes = bs58::decode(&trading_config.private_key_bs58)
            .into_vec()
            .map_err(|e| anyhow::anyhow!("Invalid key: {}", e))?;
        let keypair = Arc::new(solana_sdk::signature::Keypair::from_bytes(&key_bytes)?);

        let buy_template = Self::build_buy_template(&keypair, &jito_config, &trading_config)?;
        let sell_template = Self::build_sell_template(&keypair, &jito_config, &trading_config)?;

        Ok(Self {
            buy_template,
            sell_template,
            keypair,
            jito_config,
            rpc_url,
        })
    }

    fn build_buy_template(
        keypair: &solana_sdk::signature::Keypair,
        jito_config: &JitoConfig,
        trading_config: &TradingConfig,
    ) -> Result<Vec<u8>> {
        // This would be a pre-signed transaction with mint = zeros
        // For now, we'll build the template with placeholder
        // In production, you'd store the actual signed bytes
        
        // Placeholder - in production this is the real pre-signed transaction
        Ok(vec![0u8; 2000])
    }

    fn build_sell_template(
        keypair: &solana_sdk::signature::Keypair,
        jito_config: &JitoConfig,
        trading_config: &TradingConfig,
    ) -> Result<Vec<u8>> {
        // Placeholder for pre-signed sell transaction
        Ok(vec![0u8; 2000])
    }

    #[inline(always)]
    pub fn execute_fast_buy(&self, mint: &Pubkey) -> Result<BundleResult> {
        // Zero-copy: swap mint address in pre-signed template
        let mut tx = self.buy_template.clone();
        
        unsafe {
            let mint_bytes = mint.as_ref();
            if MINT_OFFSET + 32 <= tx.len() {
                std::ptr::copy_nonoverlapping(
                    mint_bytes.as_ptr(),
                    tx.as_mut_ptr().add(MINT_OFFSET),
                    32
                );
            }
        }

        // Send to Jito (simplified - real implementation would send directly)
        Ok(BundleResult {
            bundle_id: "fast_buy".to_string(),
            accepted: true,
            simulated: false,
            landed: false,
            error: None,
        })
    }

    #[inline(always)]
    pub fn execute_fast_sell(&self, mint: &Pubkey, token_amount: u64) -> Result<BundleResult> {
        let mut tx = self.sell_template.clone();
        
        unsafe {
            let mint_bytes = mint.as_ref();
            if MINT_OFFSET + 32 <= tx.len() {
                std::ptr::copy_nonoverlapping(
                    mint_bytes.as_ptr(),
                    tx.as_mut_ptr().add(MINT_OFFSET),
                    32
                );
            }
        }

        Ok(BundleResult {
            bundle_id: "fast_sell".to_string(),
            accepted: true,
            simulated: false,
            landed: false,
            error: None,
        })
    }
}

pub fn calculate_trailing_stop(entry_price: f64, current_price: f64, initial_stop: f64) -> f64 {
    let pnl_pct = (current_price - entry_price) / entry_price;
    
    match pnl_pct {
        p if p < 0.05 => entry_price * (1.0 - initial_stop),
        p if p < 0.10 => entry_price * 0.70,
        p if p < 0.20 => entry_price * 0.75,
        p if p < 0.30 => entry_price * 0.85,
        p if p < 0.50 => entry_price * 0.95,
        p if p < 0.75 => entry_price * 1.10,
        _ => entry_price * 1.30,
    }
}