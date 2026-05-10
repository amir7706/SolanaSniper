use crate::types::*;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;

pub const RUG_DISCRIMINATOR_1: [u8; 8] = [0x0b, 0x11, 0x8b, 0x8f, 0x0d, 0x14, 0x5d, 0x07]; // remove_liquidity
pub const RUG_DISCRIMINATOR_2: [u8; 8] = [0x06, 0x4c, 0x01, 0x4f, 0x08, 0x00, 0x00, 0x00]; // set_authority

pub struct RugDetector {
    pub monitored_devs: HashMap<Pubkey, DevMonitor>,
    pub panic_triggered: bool,
}

pub struct DevMonitor {
    pub first_seen: chrono::DateTime<chrono::Utc>,
    pub pool_address: Pubkey,
    pub lp_mint: Pubkey,
    pub initial_liquidity: u64,
    pub rug_signals: Vec<RugSignal>,
}

#[derive(Debug, Clone)]
pub struct RugSignal {
    pub signal_type: RugSignalType,
    pub detected_at: chrono::DateTime<chrono::Utc>,
    pub tx_data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RugSignalType {
    RemoveLiquidity,
    FreezeAuthority,
    LargeOutflow,
    MintAuthorityEnabled,
}

impl Default for RugDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl RugDetector {
    pub fn new() -> Self {
        Self {
            monitored_devs: HashMap::new(),
            panic_triggered: false,
        }
    }

    pub fn monitor_new_pool(&mut self, dev: Pubkey, pool: Pubkey, lp_mint: Pubkey, initial_liquidity: u64) {
        self.monitored_devs.insert(dev, DevMonitor {
            first_seen: chrono::Utc::now(),
            pool_address: pool,
            lp_mint,
            initial_liquidity,
            rug_signals: Vec::new(),
        });
    }

    #[inline(always)]
    pub fn check_transaction(&mut self, tx_data: &[u8]) -> Option<RugSignal> {
        // Scan for rug instruction patterns
        if Self::contains_pattern(tx_data, &RUG_DISCRIMINATOR_1) {
            return Some(RugSignal {
                signal_type: RugSignalType::RemoveLiquidity,
                detected_at: chrono::Utc::now(),
                tx_data: tx_data.to_vec(),
            });
        }
        
        if Self::contains_pattern(tx_data, &RUG_DISCRIMINATOR_2) {
            return Some(RugSignal {
                signal_type: RugSignalType::FreezeAuthority,
                detected_at: chrono::Utc::now(),
                tx_data: tx_data.to_vec(),
            });
        }
        
        None
    }

    #[inline(always)]
    fn contains_pattern(data: &[u8], pattern: &[u8]) -> bool {
        if data.len() < pattern.len() {
            return false;
        }
        for i in 0..=(data.len() - pattern.len()) {
            if data[i..i + pattern.len()] == *pattern {
                return true;
            }
        }
        false
    }

    pub fn check_dev_for_rug(&mut self, dev: &Pubkey, tx_data: &[u8]) -> bool {
        if self.monitored_devs.contains_key(dev) {
            if let Some(signal) = self.check_transaction(tx_data) {
                if let Some(monitor) = self.monitored_devs.get_mut(dev) {
                    monitor.rug_signals.push(signal);
                }
                return true;
            }
        }
        false
    }

    pub fn should_panic(&self, dev: &Pubkey) -> bool {
        if let Some(monitor) = self.monitored_devs.get(dev) {
            !monitor.rug_signals.is_empty()
        } else {
            false
        }
    }
}

pub struct OpportunityScorer;

impl OpportunityScorer {
    pub fn score_opportunity(
        buy_pressure: f64,
        whale_score: u8,
        dev_trust: u8,
        liquidity: f64,
    ) -> u8 {
        // Weighted scoring: Pressure 40%, Whale 30%, Dev 20%, Liquidity 10%
        let pressure_score = (buy_pressure * 20.0).min(40.0) as u8;
        let whale_s = (whale_score as f64 * 0.3).min(30.0) as u8;
        let dev_s = (dev_trust as f64 * 0.2).min(20.0) as u8;
        let liq_s = if liquidity >= 5000.0 && liquidity <= 20000.0 { 10 } else { 5 };
        
        (pressure_score + whale_s + dev_s + liq_s).min(100)
    }

    pub fn should_swap(current_score: u8, new_score: u8) -> bool {
        // Swap if new opportunity is 1.5x better
        new_score as f64 > current_score as f64 * 1.5
    }
}