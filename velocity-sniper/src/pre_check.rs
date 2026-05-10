use std::collections::HashSet;
use std::sync::RwLock;
use once_cell::sync::Lazy;
use std::time::Instant;
use tracing::{debug, info, instrument};

pub static TRUSTED_DEVS: Lazy<RwLock<HashSet<String>>> = Lazy::new(|| {
    let mut set = HashSet::new();
    set.insert("7xKXhLw4Y2T5v7mN9rY3pW8jH1kF6gD4vL9mN2pQ3rT5".to_string());
    set.insert("9mKjL3wY5T8nP4mO2rZ6pX7kJ3hF9vL4mN8pQ6rT7".to_string());
    set.insert("3nHjkL2wY6T9mP5nO3rZ8pX7kJ4hF6vL8mN3pQ5rT9".to_string());
    set.insert("5pGnmL3wY7T6mP8nO2rZ5pX9kJ6hF4vL6mN9pQ8rT3".to_string());
    set.insert("8rHjkL4wY2T9mP6nO3rZ7pX5kJ8hF2vL5mN4pQ6rT8".to_string());
    set.insert("2qLnmK3wY5T8mP9nO6rZ4pX7kJ3hF6vL8mN2pQ5rT9".to_string());
    set.insert("4rGhmL2wY6T7mP5nO8rZ3pX9kJ7hF4vL3mN6pQ8rT2".to_string());
    set.insert("6sJkmK4wY9T3mP7nO5rZ8pX4kJ9hF6vL7mN5pQ3rT6".to_string());
    set.insert("1tLnmL3wY4T8mP6nO9rZ5pX7kJ2hF8vL4mN3pQ9rT5".to_string());
    set.insert("9uMnmK2wY7T5mP3nO4rZ6pX8kJ5hF1vL6mN9pQ4rT7".to_string());
    RwLock::new(set)
});

const BITMASK_RAYDIUM_POOL: u8 = 0b001;
const BITMASK_NEW_MINT: u8 = 0b010;
const BITMASK_TRUSTED_DEV: u8 = 0b100;

const PATTERN_RAYDIUM_POOL: [u8; 8] = [
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
];

const PATTERN_NEW_MINT: [u8; 8] = [
    0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
];

#[inline]
fn fast_bitmask_check(data: &[u8]) -> u8 {
    let mut result = 0;
    
    if data.len() >= 8 && data[0] == 0x01 {
        result |= BITMASK_RAYDIUM_POOL;
    }
    
    if data.len() >= 8 && data[0] == 0x02 {
        result |= BITMASK_NEW_MINT;
    }
    
    result
}

pub fn check_program_id(program_id: &str) -> bool {
    let raydium_cp_swap = "CPMD7wV8qBPmsqJ7wZvZf2J3j1N6vE9xK8fL2mN4pQ6rT";
    program_id == raydium_cp_swap
}

pub fn check_instruction_discriminator(data: &[u8]) -> bool {
    if data.len() < 8 {
        return false;
    }
    data[0] == 0x01 || data[0] == 0x02
}

pub fn check_dev_trusted(dev_address: &str) -> bool {
    TRUSTED_DEVS.read().unwrap().contains(dev_address)
}

#[inline]
pub fn gold_filter(data: &[u8], program_id: &str, dev_address: &str) -> bool {
    let bitmask = fast_bitmask_check(data);
    
    let is_raydium = check_program_id(program_id);
    let is_new_mint = check_instruction_discriminator(data);
    let is_trusted = check_dev_trusted(dev_address);
    
    (is_raydium && is_new_mint && is_trusted) || 
    (bitmask == 0b011 && is_raydium && is_new_mint) ||
    (bitmask == 0b101 && is_raydium && is_trusted)
}

pub struct PreCheck {
    total_checked: u64,
    total_passed: u64,
}

impl PreCheck {
    pub fn new() -> Self {
        info!("PreCheck initialized - Gold Filter ready");
        Self {
            total_checked: 0,
            total_passed: 0,
        }
    }

    #[inline]
    pub fn process(&mut self, data: &[u8], program_id: &str, dev_address: &str) -> bool {
        self.total_checked += 1;
        
        let passed = gold_filter(data, program_id, dev_address);
        
        if passed {
            self.total_passed += 1;
            debug!(
                checked = self.total_checked,
                passed = self.total_passed,
                rate = format!("{:.2}%", (self.total_passed as f64 / self.total_checked as f64) * 100.0),
                "Gold Filter PASSED"
            );
        }
        
        passed
    }

    pub fn stats(&self) -> (u64, u64, f64) {
        let rate = if self.total_checked > 0 {
            (self.total_passed as f64 / self.total_checked as f64) * 100.0
        } else {
            0.0
        };
        (self.total_checked, self.total_passed, rate)
    }

    pub fn reset_stats(&mut self) {
        self.total_checked = 0;
        self.total_passed = 0;
    }
}

impl Default for PreCheck {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmask() {
        let data = vec![0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let result = fast_bitmask_check(&data);
        assert_eq!(result, BITMASK_RAYDIUM_POOL);
    }

    #[test]
    fn test_gold_filter() {
        let data = vec![0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let program_id = "CPMD7wV8qBPmsqJ7wZvZf2J3j1N6vE9xK8fL2mN4pQ6rT";
        let dev_address = "7xKXhLw4Y2T5v7mN9rY3pW8jH1kF6gD4vL9mN2pQ3rT5";
        
        assert!(gold_filter(&data, program_id, dev_address));
    }
}