use crate::types::*;
use serde::{Deserialize, Serialize};
use std::sync::mpsc::Sender;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use chrono::Utc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub trade_id: String,
    pub mint: String,
    pub pool: String,
    pub entry_price: f64,
    pub exit_price: f64,
    pub amount_sol: f64,
    pub profit_sol: f64,
    pub profit_pct: f64,
    pub is_win: bool,
    pub status: TradeStatus,
    pub entry_time: String,
    pub exit_time: String,
    pub held_seconds: u64,
    pub exit_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TradeStatus {
    PENDING,
    OPEN,
    CLOSED,
    FAILED,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingSummary {
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub win_rate_pct: f64,
    pub total_profit_sol: f64,
    pub total_loss_sol: f64,
    pub net_pnl_sol: f64,
    pub avg_profit_winners: f64,
    pub avg_loss_losers: f64,
}

pub struct TradeLogger {
    records: Arc<Mutex<VecDeque<TradeRecord>>>,
    summary: Arc<Mutex<TradingSummary>>,
    trade_counter: Arc<Mutex<u64>>,
}

impl TradeLogger {
    pub fn new() -> Self {
        Self {
            records: Arc::new(Mutex::new(VecDeque::new())),
            summary: Arc::new(Mutex::new(TradingSummary {
                total_trades: 0,
                winning_trades: 0,
                losing_trades: 0,
                win_rate_pct: 0.0,
                total_profit_sol: 0.0,
                total_loss_sol: 0.0,
                net_pnl_sol: 0.0,
                avg_profit_winners: 0.0,
                avg_loss_losers: 0.0,
            })),
            trade_counter: Arc::new(Mutex::new(0)),
        }
    }

    pub fn record_open(&self, mint: &str, pool: &str, amount_sol: f64, entry_price: f64) -> String {
        let mut counter = self.trade_counter.lock().unwrap();
        *counter += 1;
        let trade_id = format!("TRADE_{:04}", *counter);
        
        let record = TradeRecord {
            trade_id: trade_id.clone(),
            mint: mint.to_string(),
            pool: pool.to_string(),
            entry_price,
            exit_price: 0.0,
            amount_sol,
            profit_sol: 0.0,
            profit_pct: 0.0,
            is_win: false,
            status: TradeStatus::OPEN,
            entry_time: Utc::now().to_rfc3339(),
            exit_time: String::new(),
            held_seconds: 0,
            exit_reason: String::new(),
        };
        
        self.records.lock().unwrap().push_back(record);
        trade_id
    }

    pub fn record_close(&self, trade_id: &str, exit_price: f64, reason: &str) {
        let mut records = self.records.lock().unwrap();
        
        if let Some(record) = records.iter_mut().find(|r| r.trade_id == trade_id) {
            record.exit_price = exit_price;
            record.status = TradeStatus::CLOSED;
            record.exit_time = Utc::now().to_rfc3339();
            record.held_seconds = (Utc::now().timestamp() - chrono::DateTime::parse_from_rfc3339(&record.entry_time)
                .map(|dt| dt.timestamp())
                .unwrap_or(0)) as u64;
            record.exit_reason = reason.to_string();
            
            // Calculate profit
            record.profit_sol = (exit_price - record.entry_price) * record.amount_sol;
            record.profit_pct = (exit_price - record.entry_price) / record.entry_price;
            record.is_win = record.profit_sol > 0.0;
            
            // Update summary
            let mut summary = self.summary.lock().unwrap();
            summary.total_trades += 1;
            
            if record.is_win {
                summary.winning_trades += 1;
                summary.total_profit_sol += record.profit_sol;
            } else {
                summary.losing_trades += 1;
                summary.total_loss_sol += record.profit_sol.abs();
            }
            
            summary.net_pnl_sol = summary.total_profit_sol - summary.total_loss_sol;
            
            if summary.winning_trades > 0 {
                summary.avg_profit_winners = summary.total_profit_sol / summary.winning_trades as f64;
            }
            if summary.losing_trades > 0 {
                summary.avg_loss_losers = summary.total_loss_sol / summary.losing_trades as f64;
            }
            
            if summary.total_trades > 0 {
                summary.win_rate_pct = (summary.winning_trades as f64 / summary.total_trades as f64) * 100.0;
            }
        }
    }

    pub fn get_recent_trades(&self, count: usize) -> Vec<TradeRecord> {
        let records = self.records.lock().unwrap();
        records.iter().rev().take(count).cloned().collect()
    }

    pub fn get_summary(&self) -> TradingSummary {
        self.summary.lock().unwrap().clone()
    }

    pub fn get_all_records(&self) -> Vec<TradeRecord> {
        self.records.lock().unwrap().iter().cloned().collect()
    }
}

impl Default for TradeLogger {
    fn default() -> Self {
        Self::new()
    }
}

// Log trade info (non-blocking, for debug display)
pub fn log_trade_info(record: &TradeRecord) -> String {
    format!(
        "📊 {} | {}→{:.4} SOL | {} | {}% | {}",
        record.trade_id,
        record.entry_price,
        record.exit_price,
        if record.is_win { "✅ WIN" } else { "❌ LOSS" },
        format!("{:.1}", record.profit_pct * 100.0),
        record.exit_reason
    )
}

pub fn log_summary(summary: &TradingSummary) -> String {
    format!(
        "📈 SUMMARY: {} trades | {}% win rate | Net: {:.4} SOL | Avg Win: {:.4} | Avg Loss: {:.4}",
        summary.total_trades,
        format!("{:.1}", summary.win_rate_pct),
        summary.net_pnl_sol,
        summary.avg_profit_winners,
        summary.avg_loss_losers
    )
}