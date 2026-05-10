use chrono::{DateTime, Utc};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

const MAX_GOLD_LIST_SIZE: usize = 20;

#[derive(Debug, Clone)]
pub struct GoldCandidate {
    pub mint: String,
    pub pool: String,
    pub dev_address: String,
    pub added_at: DateTime<Utc>,
    pub peak_price: f64,
    pub current_price: f64,
    pub tpm: u64,
    pub buy_pressure: f64,
    pub score: u8,
}

impl GoldCandidate {
    pub fn new(mint: String, pool: String, dev_address: String) -> Self {
        Self {
            mint,
            pool,
            dev_address,
            added_at: Utc::now(),
            peak_price: 0.0,
            current_price: 0.0,
            tpm: 0,
            buy_pressure: 0.0,
            score: 0,
        }
    }

    pub fn update_price(&mut self, price: f64) {
        self.current_price = price;
        if price > self.peak_price {
            self.peak_price = price;
        }
    }

    pub fn update_metrics(&mut self, tpm: u64, buy_pressure: f64) {
        self.tpm = tpm;
        self.buy_pressure = buy_pressure;
        self.score = self.calculate_score();
    }

    pub fn calculate_score(&self) -> u8 {
        let mut score: u8 = 0;

        if self.tpm >= 80 {
            score += 20;
        } else if self.tpm >= 50 {
            score += 10;
        }

        if self.buy_pressure >= 3.0 {
            score += 20;
        } else if self.buy_pressure >= 2.0 {
            score += 10;
        }

        let price_change = if self.peak_price > 0.0 {
            ((self.current_price - self.peak_price) / self.peak_price).abs()
        } else {
            0.0
        };
        
        if price_change < 0.05 {
            score += 20;
        } else if price_change < 0.10 {
            score += 10;
        }

        let age_seconds = (Utc::now() - self.added_at).num_seconds() as u64;
        if age_seconds < 60 {
            score += 20;
        } else if age_seconds < 120 {
            score += 10;
        }

        if self.dev_address.len() > 20 {
            score += 20;
        }

        score.min(100)
    }

    pub fn should_remove(&self) -> Option<&'static str> {
        if self.tpm < 20 {
            return Some("TPM too low");
        }

        let price_drop = if self.peak_price > 0.0 && self.current_price > 0.0 {
            (self.peak_price - self.current_price) / self.peak_price
        } else {
            0.0
        };

        if price_drop > 0.15 {
            return Some("Price dropped -15% from peak");
        }

        let age_seconds = (Utc::now() - self.added_at).num_seconds() as u64;
        if age_seconds > 300 {
            return Some("Stale after 5 minutes");
        }

        None
    }
}

pub struct GoldList {
    queue: VecDeque<GoldCandidate>,
    map: HashMap<String, usize>,
    total_added: u64,
    total_removed: u64,
}

impl GoldList {
    pub fn new() -> Self {
        info!("GoldList initialized - Max {} candidates", MAX_GOLD_LIST_SIZE);
        Self {
            queue: VecDeque::with_capacity(MAX_GOLD_LIST_SIZE),
            map: HashMap::new(),
            total_added: 0,
            total_removed: 0,
        }
    }

    pub fn add(&mut self, mint: String, pool: String, dev_address: String) -> bool {
        if self.queue.len() >= MAX_GOLD_LIST_SIZE {
            debug!("GoldList full - cannot add more candidates");
            return false;
        }

        if self.map.contains_key(&mint) {
            debug!(mint = %mint, "Candidate already in GoldList");
            return false;
        }

        let candidate = GoldCandidate::new(mint.clone(), pool, dev_address);
        let idx = self.queue.len();
        
        self.queue.push_back(candidate);
        self.map.insert(mint.clone(), idx);
        self.total_added += 1;

        info!(
            mint = %mint,
            size = self.queue.len(),
            "Added to GoldList"
        );

        if self.queue.len() >= MAX_GOLD_LIST_SIZE {
            info!("GoldList FULL - Pre-Check can stop");
        }

        true
    }

    pub fn remove(&mut self, mint: &str) -> Option<GoldCandidate> {
        if let Some(idx) = self.map.remove(mint) {
            if idx < self.queue.len() {
                let candidate = self.queue.remove(idx).unwrap();
                self.total_removed += 1;
                
                let mut new_map = std::collections::HashMap::new();
                for (k, v) in self.map.iter() {
                    if *v > idx {
                        new_map.insert(k.clone(), v - 1);
                    } else {
                        new_map.insert(k.clone(), *v);
                    }
                }
                self.map = new_map;

                info!(
                    mint = %mint,
                    reason = candidate.should_remove().unwrap_or("manual"),
                    remaining = self.queue.len(),
                    "Removed from GoldList"
                );

                return Some(candidate);
            }
        }
        None
    }

    pub fn get(&self, mint: &str) -> Option<&GoldCandidate> {
        self.queue.iter().find(|c| c.mint == mint)
    }

    pub fn get_mut(&mut self, mint: &str) -> Option<&mut GoldCandidate> {
        self.queue.iter_mut().find(|c| c.mint == mint)
    }

    pub fn get_all(&self) -> Vec<&GoldCandidate> {
        self.queue.iter().collect()
    }

    pub fn is_full(&self) -> bool {
        self.queue.len() >= MAX_GOLD_LIST_SIZE
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn get_best_candidate(&self) -> Option<&GoldCandidate> {
        self.queue
            .iter()
            .max_by_key(|c| c.score)
            .filter(|c| c.score >= 80)
    }

    pub fn prune_stale(&mut self) -> Vec<GoldCandidate> {
        let mut removed = Vec::new();
        
        let mints_to_remove: Vec<String> = self.queue
            .iter()
            .filter_map(|c| c.should_remove().map(|_| c.mint.clone()))
            .collect();

        for mint in mints_to_remove {
            if let Some(candidate) = self.remove(&mint) {
                removed.push(candidate);
            }
        }

        if !removed.is_empty() {
            info!(count = removed.len(), "Pruned stale candidates");
        }

        removed
    }

    pub fn stats(&self) -> (usize, u64, u64, u8) {
        let avg_score = if !self.queue.is_empty() {
            self.queue.iter().map(|c| c.score as u64).sum::<u64>() as u8 / self.queue.len() as u8
        } else {
            0
        };
        
        (
            self.queue.len(),
            self.total_added,
            self.total_removed,
            avg_score,
        )
    }

    pub fn clear(&mut self) {
        let count = self.queue.len();
        self.queue.clear();
        self.map.clear();
        info!(count = count, "GoldList cleared");
    }
}

impl Default for GoldList {
    fn default() -> Self {
        Self::new()
    }
}

pub type GoldListState = Arc<RwLock<GoldList>>;

pub fn create_gold_list() -> GoldListState {
    Arc::new(RwLock::new(GoldList::new()))
}