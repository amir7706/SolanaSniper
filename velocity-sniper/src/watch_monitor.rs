use crate::gold_list::{GoldCandidate, GoldListState};
use crate::types::TradeSignal;
use chrono::Utc;
use solana_sdk::pubkey::Pubkey;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tokio::time::Duration;
use tracing::{debug, info, warn};

pub const SCORE_THRESHOLD: u8 = 50; // Lowered for testing
pub const MIN_TPM_FOR_EXECUTION: u64 = 30; // Lowered for testing
pub const MIN_BUY_PRESSURE: f64 = 1.5; // Lowered for testing

const BUY_PRESSURE_HISTORY_SIZE: usize = 5;

#[derive(Clone)]
struct CandidateMetrics {
    pub buy_pressure_history: VecDeque<f64>,
    pub last_tpm: u64,
    pub last_check: std::time::Instant,
}

pub struct WatchMonitor {
    gold_list: GoldListState,
    trade_tx: broadcast::Sender<TradeSignal>,
    is_running: bool,
    checks_performed: u64,
    buy_signals_triggered: u64,
    candidate_metrics: std::sync::Mutex<std::collections::HashMap<String, CandidateMetrics>>,
}

impl WatchMonitor {
    pub fn new(gold_list: GoldListState, trade_tx: broadcast::Sender<TradeSignal>) -> Self {
        info!("WatchMonitor initialized - Multi-factor trigger: Buy Pressure Delta → TPM → Score → Freshness");
        Self {
            gold_list,
            trade_tx,
            is_running: false,
            checks_performed: 0,
            buy_signals_triggered: 0,
            candidate_metrics: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    pub async fn start(&mut self) {
        if self.is_running {
            warn!("WatchMonitor already running");
            return;
        }

        self.is_running = true;
        info!("WatchMonitor started - Priority sort every 100ms");

        loop {
            if !self.is_running {
                break;
            }

            self.check_and_trigger().await;

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub fn stop(&mut self) {
        self.is_running = false;
        info!("WatchMonitor stopped");
    }

    async fn check_and_trigger(&mut self) {
        let best_candidate = {
            let mut gold = self.gold_list.write().await;

            if gold.is_empty() {
                return;
            }

            self.checks_performed += 1;

            gold.prune_stale();

            let candidates = gold.get_all();
            
            if candidates.is_empty() {
                return;
            }

            let mut best: Option<(GoldCandidate, f64)> = None;
            
            for candidate in candidates {
                let score = self.calculate_priority_score(&candidate);
                if candidate.score >= SCORE_THRESHOLD && candidate.tpm >= MIN_TPM_FOR_EXECUTION && candidate.buy_pressure >= MIN_BUY_PRESSURE {
                    let delta = self.calculate_buy_pressure_delta(&candidate.mint);
                    if delta >= 0.0 {
                        if best.is_none() || score > best.as_ref().unwrap().1 {
                            best = Some((candidate.clone(), score));
                        }
                    }
                }
            }
            
            best
        };

        if let Some((best, score)) = best_candidate {
            info!(
                mint = %best.mint,
                priority_score = format!("{:.2}", score),
                buy_pressure = format!("{:.2}", best.buy_pressure),
                tpm = best.tpm,
                score = best.score,
                ">>> EXECUTING BUY (Multi-Factor) <<<"
            );
            
            self.trigger_buy(
                best.mint,
                best.pool,
                best.score,
                best.tpm,
                best.buy_pressure,
                score,
            ).await;
        }

        let (size, _total_added, _total_removed, avg_score) = {
            let gold = self.gold_list.read().await;
            gold.stats()
        };
        debug!(
            gold_list = size,
            avg_score = avg_score,
            checks = self.checks_performed,
            "WatchMonitor check complete"
        );
    }

    fn calculate_priority_score(&self, candidate: &GoldCandidate) -> f64 {
        let mut score = 0.0;

        let buy_pressure_delta = self.calculate_buy_pressure_delta(&candidate.mint);
        score += buy_pressure_delta * 40.0;

        if candidate.tpm >= MIN_TPM_FOR_EXECUTION {
            score += 30.0;
        } else if candidate.tpm >= 50 {
            score += 15.0;
        }

        if candidate.score >= SCORE_THRESHOLD {
            score += 20.0;
        } else if candidate.score >= 60 {
            score += 10.0;
        }

        let age_seconds = (chrono::Utc::now() - candidate.added_at).num_seconds() as f64;
        if age_seconds < 60.0 {
            score += 10.0;
        } else if age_seconds < 120.0 {
            score += 5.0;
        }

        score
    }

    fn calculate_buy_pressure_delta(&self, mint: &str) -> f64 {
        let metrics = self.candidate_metrics.lock().unwrap();
        
        if let Some(cand) = metrics.get(mint) {
            if cand.buy_pressure_history.len() >= 2 {
                let recent: Vec<f64> = cand.buy_pressure_history.iter().rev().take(3).cloned().collect();
                if recent.len() >= 2 {
                    return recent[0] - recent[1];
                }
            }
        }
        0.0
    }

    fn should_execute(&self, candidate: &GoldCandidate) -> bool {
        if candidate.score < SCORE_THRESHOLD {
            return false;
        }

        if candidate.tpm < MIN_TPM_FOR_EXECUTION {
            return false;
        }

        if candidate.buy_pressure < MIN_BUY_PRESSURE {
            return false;
        }

        let delta = self.calculate_buy_pressure_delta(&candidate.mint);
        if delta < 0.0 {
            return false;
        }

        true
    }

    fn update_metrics(&self, mint: &str, buy_pressure: f64, tpm: u64) {
        let mut metrics = self.candidate_metrics.lock().unwrap();
        
        let entry = metrics.entry(mint.to_string()).or_insert_with(|| CandidateMetrics {
            buy_pressure_history: VecDeque::with_capacity(BUY_PRESSURE_HISTORY_SIZE),
            last_tpm: 0,
            last_check: std::time::Instant::now(),
        });

        entry.buy_pressure_history.push_back(buy_pressure);
        if entry.buy_pressure_history.len() > BUY_PRESSURE_HISTORY_SIZE {
            entry.buy_pressure_history.pop_front();
        }
        entry.last_tpm = tpm;
        entry.last_check = std::time::Instant::now();
    }

    async fn trigger_buy(&mut self, mint: String, pool: String, score: u8, tpm: u64, buy_pressure: f64, priority_score: f64) {
        self.buy_signals_triggered += 1;

        self.update_metrics(&mint, buy_pressure, tpm);

        let mint_pubkey = mint.parse::<Pubkey>().unwrap_or_else(|_| Pubkey::new_from_array([0u8; 32]));
        let pool_pubkey = pool.parse::<Pubkey>().unwrap_or_else(|_| Pubkey::new_from_array([0u8; 32]));

        let delta = self.calculate_buy_pressure_delta(&mint);
        
        let signal = TradeSignal::Buy {
            mint: mint_pubkey,
            pool: pool_pubkey,
            confidence: score as f64 / 100.0,
            velocity_tpm: tpm as f64,
            buy_pressure_ratio: buy_pressure,
            trigger_reason: format!("Priority: {:.1} | B/P Δ: {:.2} | TPM: {} | Score: {}", priority_score, delta, tpm, score),
        };

        if let Err(e) = self.trade_tx.send(signal) {
            warn!(error = %e, "Failed to send buy signal");
        } else {
            info!(
                mint = %mint,
                priority = format!("{:.1}", priority_score),
                bp_delta = format!("{:.2}", delta),
                tpm = tpm,
                score = score,
                ">>> 🚀 BUY TRIGGERED (Multi-Factor) <<<"
            );
        }
    }

    pub async fn update_candidate_metrics(&self, mint: &str, tpm: u64, buy_pressure: f64) {
        {
            let mut gold = self.gold_list.write().await;
            if let Some(candidate) = gold.get_mut(mint) {
                candidate.update_metrics(tpm, buy_pressure);
            }
        }
        self.update_metrics(mint, buy_pressure, tpm);
    }

    pub async fn add_candidate(&self, mint: String, pool: String, dev_address: String) -> bool {
        let mut gold = self.gold_list.write().await;
        let added = gold.add(mint, pool, dev_address);
        
        if added {
            info!(gold_size = gold.len(), "Candidate added via WatchMonitor");
        }
        
        added
    }

    pub async fn remove_candidate(&self, mint: &str) {
        let mut gold = self.gold_list.write().await;
        gold.remove(mint);
        
        let mut metrics = self.candidate_metrics.lock().unwrap();
        metrics.remove(mint);
    }

    pub async fn get_watchlist_status(&self) -> WatchListStatus {
        let gold = self.gold_list.read().await;
        let (size, total_added, total_removed, avg_score) = gold.stats();
        
        WatchListStatus {
            is_running: self.is_running,
            total_size: size,
            max_size: 20,
            total_added,
            total_removed,
            avg_score,
            checks_performed: self.checks_performed,
            buy_signals: self.buy_signals_triggered,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WatchListStatus {
    pub is_running: bool,
    pub total_size: usize,
    pub max_size: usize,
    pub total_added: u64,
    pub total_removed: u64,
    pub avg_score: u8,
    pub checks_performed: u64,
    pub buy_signals: u64,
}

pub type WatchMonitorState = Arc<RwLock<WatchMonitor>>;

pub fn create_watch_monitor(
    gold_list: GoldListState,
    trade_tx: broadcast::Sender<TradeSignal>,
) -> WatchMonitorState {
    Arc::new(RwLock::new(WatchMonitor::new(gold_list, trade_tx)))
}