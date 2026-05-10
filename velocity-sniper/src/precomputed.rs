use dashmap::DashMap;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

pub const MINT_OFFSET: usize = 156;

#[derive(Clone)]
pub struct PrecomputedData {
    pub dev_whitelist: DashMap<Pubkey, u8>,
    pub scam_blacklist: DashMap<Pubkey, ()>,
    pub whale_wallets: DashMap<Pubkey, u8>,
    pub buy_trigger_mask: u64,
    pub reject_mask: u64,
}

impl Default for PrecomputedData {
    fn default() -> Self {
        Self::new()
    }
}

impl PrecomputedData {
    pub fn new() -> Self {
        let mut data = Self {
            dev_whitelist: DashMap::new(),
            scam_blacklist: DashMap::new(),
            whale_wallets: DashMap::new(),
            buy_trigger_mask: 0,
            reject_mask: 0,
        };
        data.load_hardcoded_data();
        data
    }

    fn load_hardcoded_data(&mut self) {
        // Bitmasks for fast decision
        // Bit 0: Dev Whitelisted (score >= 30)
        // Bit 1: Liquidity OK ($5k-$20k)  
        // Bit 2: Pressure > 1.5
        // Bit 3: No rug signal
        // Bits 4-7: Reserved
        // Bits 8-15: Dev trust score
        self.buy_trigger_mask = 0b00001111; // Bits 0,1,2,3 must all be set
        self.reject_mask = 0b10000000; // Bit 7 = reject immediately

        // Known good developer wallets (trust score 80-100)
        let good_devs = [
            "7xKXSG7GLvF4FvKfBRo7vCg7uVvNLiC6SjM",
            "DezXAZ8z7PnrnRJjz3wXBo1M6VWH91g1SF7r",
            "CXk8cQe2LQcL4j7EErY9vNs4b5GkG3cK2v",
            "2bBorB5Qm2LzG3v1VvG1mYzYwL4X5qK6j",
        ];
        
        for dev in good_devs {
            if let Ok(pubkey) = Pubkey::from_str(dev) {
                self.dev_whitelist.insert(pubkey, 85);
            }
        }

        // Known scammer wallets to block immediately
        let scammers = [
            "Rug6XXnV6F6v7v8v9v1v2v3v4v5v6v7v8v9",
            "Scam1Dev2Wallet3That4Should5Be6Blocked",
        ];
        
        for scam in scammers {
            if let Ok(pubkey) = Pubkey::from_str(scam) {
                self.scam_blacklist.insert(pubkey, ());
            }
        }

        // Known whale wallets (smart money)
        let whales = [
            "9xQe1wTz3sK4m5n6p7q8r9s0t1u2v3w4x",
            "F8mN2p3q4r5s6t7u8v9w0x1y2z3a4b5c",
        ];
        
        for (i, whale) in whales.iter().enumerate() {
            if let Ok(pubkey) = Pubkey::from_str(whale) {
                self.whale_wallets.insert(pubkey, 90 - (i * 10) as u8);
            }
        }
    }

    #[inline(always)]
    pub fn fast_check_dev(&self, dev: &Pubkey) -> (bool, bool, u8) {
        // Check blacklist first (fastest reject)
        if self.scam_blacklist.contains_key(dev) {
            return (true, false, 0); // (is_blacklisted, is_whitelisted, trust_score)
        }
        
        // Check whitelist
        if let Some(score) = self.dev_whitelist.get(dev) {
            return (false, *score >= 30, *score);
        }
        
        // Unknown - default to not whitelisted
        (false, false, 0)
    }

    #[inline(always)]
    pub fn is_whale(&self, wallet: &Pubkey) -> Option<u8> {
        self.whale_wallets.get(wallet).map(|v| *v)
    }

    #[inline(always)]
    pub fn evaluate_fast(&self, dev_trust: u8, liquidity_ok: bool, pressure_ok: bool, no_rug: bool) -> Decision {
        let mut state = 0u64;
        
        if dev_trust >= 30 { state |= 1 << 0; }
        if liquidity_ok { state |= 1 << 1; }
        if pressure_ok { state |= 1 << 2; }
        if no_rug { state |= 1 << 3; }
        
        if state & self.reject_mask != 0 {
            return Decision::REJECT;
        }
        
        if (state & self.buy_trigger_mask) == self.buy_trigger_mask {
            return Decision::APPROVE;
        }
        
        Decision::WAIT
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Decision {
    APPROVE,
    REJECT,
    WAIT,
}

pub struct TokenAnalysis {
    pub mint: Pubkey,
    pub dev: Pubkey,
    pub dev_trust_score: u8,
    pub liquidity_sol: f64,
    pub buy_pressure: f64,
    pub is_whale_buying: bool,
    pub rug_signal: bool,
}

impl TokenAnalysis {
    pub fn build_state_bits(&self) -> u64 {
        let mut bits = 0u64;
        
        if self.dev_trust_score >= 30 { bits |= 1 << 0; }
        if self.liquidity_sol >= 5000.0 && self.liquidity_sol <= 20000.0 { bits |= 1 << 1; }
        if self.buy_pressure > 1.5 { bits |= 1 << 2; }
        if !self.rug_signal { bits |= 1 << 3; }
        
        bits
    }
}