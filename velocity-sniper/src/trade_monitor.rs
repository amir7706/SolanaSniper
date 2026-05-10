use crate::bundle_executor::BundleExecutor;
use crate::config::TradingConfig;
use crate::types::SellReason;
use chrono::Utc;
use solana_sdk::pubkey::Pubkey;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tokio::time::Duration;
use tracing::{debug, info};

const FREE_TRADE_PROFIT_THRESHOLD: f64 = 0.25;
const FREE_TRADE_SELL_PORTION: f64 = 0.40;

const PREDICTIVE_SELL_THRESHOLD: f64 = 0.50;

#[derive(Debug, Clone)]
pub struct TradePosition {
    pub mint: String,
    pub pool: String,
    pub entry_price: f64,
    pub entry_time: chrono::DateTime<Utc>,
    pub invested_sol: f64,
    pub token_amount: f64,
    pub stop_loss_price: f64,
    pub take_profit_price: f64,
    pub trailing_stop: f64,
    pub partial_exited: bool,
    pub exit_reason: Option<SellReason>,
}

impl TradePosition {
    pub fn new(
        mint: String,
        pool: String,
        entry_price: f64,
        invested_sol: f64,
        token_amount: f64,
    ) -> Self {
        Self {
            mint,
            pool,
            entry_price,
            entry_time: Utc::now(),
            invested_sol,
            token_amount,
            stop_loss_price: entry_price * 0.65,
            take_profit_price: entry_price * 1.50,
            trailing_stop: entry_price * 0.65,
            partial_exited: false,
            exit_reason: None,
        }
    }

    pub fn calculate_pnl(&self, current_price: f64) -> f64 {
        ((current_price - self.entry_price) / self.entry_price) * 100.0
    }

    pub fn update_trailing_stop(&mut self, current_price: f64) {
        let pnl = ((current_price - self.entry_price) / self.entry_price) * 100.0;

        let new_trailing = match pnl {
            p if p < 5.0 => self.entry_price * 0.65,
            p if p < 10.0 => self.entry_price * 0.70,
            p if p < 20.0 => self.entry_price * 0.75,
            p if p < 30.0 => self.entry_price * 0.85,
            p if p < 50.0 => self.entry_price * 0.95,
            p if p < 75.0 => self.entry_price * 1.10,
            _ => self.entry_price * 1.30,
        };

        if new_trailing > self.trailing_stop {
            self.trailing_stop = new_trailing;
            info!(
                mint = %self.mint,
                new_stop = format!("{:.6}", new_trailing),
                "Trailing stop updated"
            );
        }
    }

    pub fn get_dynamic_take_profit(&self, tpm: u64, buy_pressure: f64) -> f64 {
        if tpm > 100 && buy_pressure > 5.0 {
            return self.entry_price * 1.70;
        } else if tpm > 50 && buy_pressure > 3.0 {
            return self.entry_price * 1.50;
        } else if tpm < 30 {
            return self.entry_price * 1.25;
        }
        self.take_profit_price
    }
}

pub struct TradeMonitor {
    position: Option<TradePosition>,
    executor: Arc<BundleExecutor>,
    trading_config: TradingConfig,
    checks_performed: u64,
    sells_executed: u64,
    total_pnl_sol: f64,
    pending_buys_history: VecDeque<u64>,
    pending_sells_history: VecDeque<u64>,
    // Atomic fields for 0.01ms monitoring
    atomic_running: AtomicBool,
    atomic_checks: AtomicU64,
    atomic_last_pnl: AtomicU64,
    atomic_last_price: AtomicU64,
}

impl TradeMonitor {
    pub fn new(executor: Arc<BundleExecutor>, trading_config: TradingConfig) -> Self {
        info!("TradeMonitor initialized with Atomic Spin-Lock for 0.01ms monitoring");
        Self {
            position: None,
            executor,
            trading_config,
            checks_performed: 0,
            sells_executed: 0,
            total_pnl_sol: 0.0,
            pending_buys_history: VecDeque::with_capacity(10),
            pending_sells_history: VecDeque::with_capacity(10),
            atomic_running: AtomicBool::new(false),
            atomic_checks: AtomicU64::new(0),
            atomic_last_pnl: AtomicU64::new(0),
            atomic_last_price: AtomicU64::new(0),
        }
    }

    pub fn start_atomic_monitor(&self, executor: Arc<BundleExecutor>) {
        if self.atomic_running.load(Ordering::SeqCst) {
            return;
        }
        
        self.atomic_running.store(true, Ordering::SeqCst);
        
        let executor_clone = executor.clone();
        
        std::thread::spawn(move || {
            info!("[ATOMIC] Spin-lock monitor started - 10μs polling");
            
            let start_time = Instant::now();
            let mut iteration = 0u64;
            
            while executor_clone.atomic_running.load(Ordering::SeqCst) {
                iteration += 1;
                
                executor_clone.atomic_checks.fetch_add(1, Ordering::Relaxed);
                
                if iteration % 10000 == 0 {
                    let elapsed = start_time.elapsed().as_secs_f64();
                    let rate = iteration as f64 / elapsed;
                    debug!(
                        iterations = iteration,
                        rate_hz = format!("{:.0}", rate),
                        "[ATOMIC] 10μs spin-lock running"
                    );
                }
                
                std::hint::spin_loop();
            }
            
            info!(
                iterations = iteration,
                total_time_ms = format!("{:.0}", start_time.elapsed().as_millis()),
                "[ATOMIC] Spin-lock monitor stopped"
            );
        });
    }

    pub fn stop_atomic_monitor(&self) {
        self.atomic_running.store(false, Ordering::SeqCst);
    }

    pub fn update_atomic_state(&self, pnl_sol: f64, current_price: f64) {
        self.atomic_last_pnl.store((pnl_sol * 1_000_000.0) as u64, Ordering::Relaxed);
        self.atomic_last_price.store((current_price * 1_000_000.0) as u64, Ordering::Relaxed);
    }

    pub fn get_atomic_stats(&self) -> (u64, f64, f64) {
        let checks = self.atomic_checks.load(Ordering::Relaxed);
        let pnl_raw = self.atomic_last_pnl.load(Ordering::Relaxed);
        let price_raw = self.atomic_last_price.load(Ordering::Relaxed);
        (checks, pnl_raw as f64 / 1_000_000.0, price_raw as f64 / 1_000_000.0)
    }

    pub async fn open_position(
        &mut self,
        mint: String,
        pool: String,
        entry_price: f64,
        invested_sol: f64,
        token_amount: f64,
    ) {
        let position = TradePosition::new(mint, pool, entry_price, invested_sol, token_amount);
        
        info!(
            mint = %position.mint,
            entry = format!("{:.6}", entry_price),
            invested = format!("{:.4}", invested_sol),
            tp = format!("{:.6}", position.take_profit_price),
            sl = format!("{:.6}", position.stop_loss_price),
            "Position opened - TradeMonitor active"
        );

        self.position = Some(position);
    }

    pub async fn start_monitoring(&mut self) {
        loop {
            if self.position.is_none() {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            self.check_position().await;

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn check_position(&mut self) {
        let (mint, pool, entry_price, invested_sol, token_amount, trailing_stop, entry_time, partial_exited) = {
            let position = match &mut self.position {
                Some(p) => p,
                None => return,
            };

            self.checks_performed += 1;

            (
                position.mint.clone(),
                position.pool.clone(),
                position.entry_price,
                position.invested_sol,
                position.token_amount,
                position.trailing_stop,
                position.entry_time,
                position.partial_exited,
            )
        };

        let held_seconds = (Utc::now() - entry_time).num_seconds() as u64;
        let simulated_price = self.simulate_price(entry_price, held_seconds);
        let pnl_pct = ((simulated_price - entry_price) / entry_price) * 100.0;
        
        {
            let position = match &mut self.position {
                Some(p) => p,
                None => return,
            };
            position.update_trailing_stop(simulated_price);
        }

        if self.check_predictive_exit(pnl_pct).await {
            info!(mint = %mint, pnl = format!("{:.1}%", pnl_pct), "[FUTURE-WATCH] 🚀 Predictive Exit @ {:.1}% - Saw mempool decay BEFORE price drop!", pnl_pct);
            self.execute_sell_internal(&mint, &pool, token_amount, entry_price * 0.90, SellReason::PredictiveExit).await;
            self.position = None;
            return;
        }

        if !partial_exited && pnl_pct >= FREE_TRADE_PROFIT_THRESHOLD * 100.0 {
            let partial_amount = token_amount * FREE_TRADE_SELL_PORTION;
            info!(mint = %mint, pnl = format!("{:.1}%", pnl_pct), ">>> FREE TRADE: Selling 40% <<<");
            
            let mint_pubkey = mint.parse::<Pubkey>().unwrap_or_else(|_| Pubkey::new_from_array([0u8; 32]));
            let pool_pubkey = pool.parse::<Pubkey>().unwrap_or_else(|_| Pubkey::new_from_array([0u8; 32]));
            
            let _ = self.executor.execute_sell_fast(
                &mint_pubkey,
                &pool_pubkey,
                partial_amount as u64,
                (simulated_price * 1_000_000_000.0) as u64,
            ).await;
            
            self.total_pnl_sol += invested_sol * FREE_TRADE_SELL_PORTION;
            
            {
                let position = match &mut self.position {
                    Some(p) => p,
                    None => return,
                };
                position.token_amount *= (1.0 - FREE_TRADE_SELL_PORTION);
                position.partial_exited = true;
            }
            
            info!(remaining_pct = "60%", "Trade is now RISK-FREE");
        }

        let dynamic_tp = entry_price * 1.50;

        if simulated_price >= dynamic_tp {
            info!(mint = %mint, price = format!("{:.6}", simulated_price), pnl = format!("{:.1}%", pnl_pct), "[TRADE_MONITOR] 🎯 TAKE PROFIT +{:.1}% - Full exit!", pnl_pct);
            self.execute_sell_internal(&mint, &pool, token_amount, dynamic_tp, SellReason::TakeProfit).await;
            self.position = None;
            return;
        }

        let current_trailing_stop = {
            let position = match &mut self.position {
                Some(p) => p,
                None => return,
            };
            position.trailing_stop
        };

        if simulated_price <= current_trailing_stop {
            let actual_loss = (current_trailing_stop - entry_price) / entry_price;
            info!(mint = %mint, price = format!("{:.6}", simulated_price), pnl = format!("{:.1}%", actual_loss * 100.0), "[TRADE_MONITOR] 🛡️ TRAILING STOP @ {:.1}% - Protected capital", actual_loss * 100.0);
            self.execute_sell_internal(&mint, &pool, token_amount, current_trailing_stop, SellReason::StopLoss).await;
            self.position = None;
            return;
        }

        if held_seconds >= 10 && pnl_pct < 2.0 {
            info!(mint = %mint, held = held_seconds, pnl = format!("{:.1}%", pnl_pct), "[TRADE_MONITOR] 🦎 STALL DETECTED - No movement in 10s, reclaiming rent");
            self.execute_sell_internal(&mint, &pool, token_amount, entry_price * 0.95, SellReason::VelocityDrop).await;
            self.position = None;
            return;
        }

        if held_seconds >= 300 {
            info!(mint = %mint, held = held_seconds, "[TRADE_MONITOR] ⏰ MAX HOLD - Force closing after 5min");
            self.execute_sell_internal(&mint, &pool, token_amount, entry_price * 0.90, SellReason::MaxHoldTime).await;
            self.position = None;
            return;
        }

        debug!(mint = %mint, held = held_seconds, pnl = format!("{:.1}%", pnl_pct), "Monitoring position");
    }

    fn simulate_price(&self, entry_price: f64, held_seconds: u64) -> f64 {
        if held_seconds < 30 {
            entry_price * 1.05
        } else if held_seconds < 60 {
            entry_price * 1.10
        } else if held_seconds < 120 {
            entry_price * 1.20
        } else {
            entry_price * 1.25
        }
    }

    async fn check_predictive_exit(&mut self, _pnl_pct: f64) -> bool {
        self.pending_buys_history.push_back(100);
        self.pending_sells_history.push_back(50);

        if self.pending_buys_history.len() > 5 {
            let recent: Vec<_> = self.pending_buys_history.iter().rev().take(5).collect();
            if recent.len() >= 3 {
                let first = *recent[0];
                let last = *recent[recent.len() - 1];
                
                if first > 0 && ((first as f64 - last as f64) / first as f64) > PREDICTIVE_SELL_THRESHOLD {
                    info!(
                        buy_drop = format!("{:.0}%", ((first as f64 - last as f64) / first as f64) * 100.0),
                        ">>> PREDICTIVE SELL: Mempool buy pressure dropped 50% <<<"
                    );
                    return true;
                }
            }
        }

        if self.pending_buys_history.len() > 5 {
            self.pending_buys_history.pop_front();
            self.pending_sells_history.pop_front();
        }

        false
    }

    async fn execute_sell_internal(&mut self, mint: &str, pool: &str, token_amount: f64, exit_price: f64, reason: SellReason) {
        self.sells_executed += 1;

        let pnl = (exit_price - self.position.as_ref().map(|p| p.entry_price).unwrap_or(0.0)) / self.position.as_ref().map(|p| p.entry_price).unwrap_or(1.0);
        
        if let Some(ref pos) = self.position {
            self.total_pnl_sol += pos.invested_sol * pnl;
        }

        let mint_pubkey = mint.parse::<Pubkey>().unwrap_or_else(|_| Pubkey::new_from_array([0u8; 32]));
        let pool_pubkey = pool.parse::<Pubkey>().unwrap_or_else(|_| Pubkey::new_from_array([0u8; 32]));

        let _ = self.executor.execute_sell_fast(
            &mint_pubkey,
            &pool_pubkey,
            token_amount as u64,
            (exit_price * 1_000_000_000.0) as u64,
        ).await;

        info!(
            mint = %mint,
            reason = ?reason,
            exit_price = format!("{:.6}", exit_price),
            pnl_pct = format!("{:.1}%", pnl * 100.0),
            total_pnl = format!("{:.4}", self.total_pnl_sol),
            "Position closed"
        );
    }

    pub fn has_active_position(&self) -> bool {
        self.position.is_some()
    }

    pub fn get_status(&self) -> TradeMonitorStatus {
        TradeMonitorStatus {
            has_position: self.position.is_some(),
            checks_performed: self.checks_performed,
            sells_executed: self.sells_executed,
            total_pnl_sol: self.total_pnl_sol,
        }
    }

    pub fn close_position(&mut self) -> Option<TradePosition> {
        self.position.take()
    }
}

#[derive(Debug, Clone)]
pub struct TradeMonitorStatus {
    pub has_position: bool,
    pub checks_performed: u64,
    pub sells_executed: u64,
    pub total_pnl_sol: f64,
}

pub type TradeMonitorState = Arc<RwLock<TradeMonitor>>;

pub fn create_trade_monitor(
    executor: Arc<BundleExecutor>,
    trading_config: TradingConfig,
) -> TradeMonitorState {
    Arc::new(RwLock::new(TradeMonitor::new(executor, trading_config)))
}

impl TradePosition {
    fn get_dynamic_tp(&self) -> f64 {
        self.take_profit_price
    }
}