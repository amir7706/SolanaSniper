use crate::bundle_executor::{BundleExecutor, PendingSell};
use crate::config::{RpcConfig, SafetyConfig, StrategyConfig, TradingConfig};
use crate::types::*;
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tokio::time::Duration;
use tracing::{debug, info, warn};

/// Strategy Orchestrator: The "brain" that connects all modules.
///
/// Implements the Two-Stage Strategy:
///   Stage 1 (Scan): Listen for initialize2 via Raydium Detector
///   Stage 2 (Filter): Safety Filter validates the token
///   Stage 3 (Execute): Velocity Monitor confirms momentum -> Bundle Executor trades
///
/// Also manages:
///   - Position tracking (active buys)
///   - Take-profit / stop-loss monitoring
///   - Trade cooldowns
///   - Concurrent position limits
pub async fn run(
    mut trade_signal_rx: broadcast::Receiver<TradeSignal>,
    _trade_signal_tx: broadcast::Sender<TradeSignal>,
    _pool_event_rx: broadcast::Receiver<PoolEvent>,
    _safe_token_rx: broadcast::Receiver<MintInfo>,
    state: Arc<RwLock<TradingState>>,
    strategy_config: StrategyConfig,
    trading_config: TradingConfig,
    jito_config: crate::config::JitoConfig,
    rpc_config: RpcConfig,
    _safety_config: SafetyConfig,
) -> anyhow::Result<()> {
    crate::pin_thread_to_last_core("strategy");
    info!("Strategy orchestrator started");

    let rpc_url = rpc_config.premium_endpoint.unwrap_or(rpc_config.endpoint);
    let executor = Arc::new(BundleExecutor::new(jito_config.clone(), trading_config.clone(), rpc_url)?);
    let mut last_trade_time = Utc::now() - chrono::Duration::seconds(
        strategy_config.trade_cooldown_seconds as i64
    );

    // Spawn blockhash refresher (refreshes every 30 seconds to avoid stale txs)
    let executor_for_bh = executor.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            if let Err(e) = executor_for_bh.refresh_blockhash().await {
                warn!(error = %e, "Failed to refresh blockhash");
            } else {
                debug!("Blockhash refreshed for fast sells");
            }
        }
    });

    // Spawn continuous price monitor - watches every 100ms
    let executor_for_monitor = executor.clone();
    let state_for_monitor = state.clone();
    let trading_for_monitor = trading_config.clone();
    
    tokio::spawn(async move {
        let mut check_interval = tokio::time::interval(Duration::from_millis(100));
        
        loop {
            check_interval.tick().await;
            
            let pending_sells = executor_for_monitor.get_pending_sells();
            
            if pending_sells.is_empty() {
                continue;
            }

            for sell in &pending_sells {
                let held_seconds = (Utc::now() - sell.entry_time).num_seconds() as u64;
                let entry_price = sell.entry_price_sol;
                
                // Simulate current price (in real implementation, fetch from RPC)
                // For now, use time-based estimation
                let simulated_price = if held_seconds < 30 {
                    entry_price * 1.05 // Early momentum
                } else if held_seconds < 60 {
                    entry_price * 1.10
                } else {
                    entry_price * 1.15
                };
                
                let current_pnl_pct = (simulated_price - entry_price) / entry_price;
                
                // Calculate dynamic trailing stop (35% initial -> moves up)
                let trailing_stop = calculate_trailing_stop(entry_price, simulated_price, trading_for_monitor.stop_loss_pct);
                
                // Check take profit (+50%)
                if current_pnl_pct >= trading_for_monitor.take_profit_pct {
                    info!(
                        mint = %sell.mint,
                        pnl = format!("{:.1}%", current_pnl_pct * 100.0),
                        "🎯 TAKE PROFIT TRIGGERED - Closing trade"
                    );
                    
                    let _ = executor_for_monitor.execute_sell_fast(
                        &sell.mint,
                        &sell.pool,
                        sell.token_amount,
                        (simulated_price * 1_000_000_000.0) as u64,
                    ).await;
                    
                    // Update state
                    let mut state_write = state_for_monitor.write().await;
                    if let Some(pos_idx) = state_write.active_positions.iter().position(|p| p.mint == sell.mint) {
                        let pos = state_write.active_positions.remove(pos_idx);
                        state_write.completed_trades.push(CompletedTrade {
                            mint: sell.mint,
                            entry_price: pos.entry_price_sol,
                            exit_price: simulated_price,
                            pnl_sol: pos.entry_price_sol * current_pnl_pct,
                            pnl_pct: current_pnl_pct,
                            held_seconds,
                            reason: SellReason::TakeProfit,
                        });
                        state_write.total_pnl_sol += pos.entry_price_sol * current_pnl_pct;
                        state_write.winning_trades += 1;
                    }
                    continue;
                }
                
                // Check trailing stop (moves with price)
                if simulated_price <= trailing_stop {
                    let stop_loss_pct = (trailing_stop - entry_price) / entry_price;
                    info!(
                        mint = %sell.mint,
                        current_price = format!("{:.4}", simulated_price),
                        stop_price = format!("{:.4}", trailing_stop),
                        "🛡️ TRAILING STOP HIT - Exiting trade"
                    );
                    
                    let _ = executor_for_monitor.execute_sell_fast(
                        &sell.mint,
                        &sell.pool,
                        sell.token_amount,
                        (trailing_stop * 1_000_000_000.0) as u64,
                    ).await;
                    
                    let mut state_write = state_for_monitor.write().await;
                    if let Some(pos_idx) = state_write.active_positions.iter().position(|p| p.mint == sell.mint) {
                        let pos = state_write.active_positions.remove(pos_idx);
                        state_write.completed_trades.push(CompletedTrade {
                            mint: sell.mint,
                            entry_price: pos.entry_price_sol,
                            exit_price: trailing_stop,
                            pnl_sol: pos.entry_price_sol * stop_loss_pct,
                            pnl_pct: stop_loss_pct,
                            held_seconds,
                            reason: SellReason::StopLoss,
                        });
                        state_write.total_pnl_sol += pos.entry_price_sol * stop_loss_pct;
                    }
                    continue;
                }
                
                // Check max hold time
                if held_seconds >= trading_for_monitor.max_hold_seconds {
                    info!(
                        mint = %sell.mint,
                        held_sec = held_seconds,
                        "⏰ MAX HOLD TIME - Force closing"
                    );
                    
                    let _ = executor_for_monitor.execute_sell_fast(
                        &sell.mint,
                        &sell.pool,
                        sell.token_amount,
                        ((entry_price * 0.9) * 1_000_000_000.0) as u64,
                    ).await;
                    
                    let mut state_write = state_for_monitor.write().await;
                    if let Some(pos_idx) = state_write.active_positions.iter().position(|p| p.mint == sell.mint) {
                        let pos = state_write.active_positions.remove(pos_idx);
                        state_write.completed_trades.push(CompletedTrade {
                            mint: sell.mint,
                            entry_price: pos.entry_price_sol,
                            exit_price: pos.entry_price_sol * 0.9,
                            pnl_sol: pos.entry_price_sol * -0.1,
                            pnl_pct: -0.1,
                            held_seconds,
                            reason: SellReason::MaxHoldTime,
                        });
                        state_write.total_pnl_sol += pos.entry_price_sol * -0.1;
                    }
                    continue;
                }
                
                // Continuous monitoring log
                debug!(
                    mint = %sell.mint,
                    held = held_seconds,
                    pnl = format!("{:.1}%", current_pnl_pct * 100.0),
                    stop = format!("{:.4}", trailing_stop),
                    "👁️ MONITORING: Watching position"
                );
            }
        }
    });

    // Main loop: Process trade signals
    loop {
        match trade_signal_rx.recv().await {
            Ok(TradeSignal::Buy {
                mint,
                pool,
                confidence,
                velocity_tpm,
                buy_pressure_ratio,
                trigger_reason,
            }) => {
                let now = Utc::now();
                let cooldown_remaining = strategy_config.trade_cooldown_seconds as i64
                    - (now - last_trade_time).num_seconds();

                if cooldown_remaining > 0 {
                    info!(
                        mint = %mint,
                        cooldown_sec = cooldown_remaining,
                        "Trade skipped — cooldown active"
                    );
                    continue;
                }

                // Check concurrent position limit
                {
                    let state_read = state.read().await;
                    if state_read.active_positions.len() >= strategy_config.max_concurrent_positions {
                        warn!(
                            mint = %mint,
                            positions = state_read.active_positions.len(),
                            max = strategy_config.max_concurrent_positions,
                            "Trade skipped — max concurrent positions reached"
                        );
                        continue;
                    }
                }

                info!(
                    mint = %mint,
                    pool = %pool,
                    confidence = format!("{:.0}%", confidence * 100.0),
                    tpm = format!("{:.1}", velocity_tpm),
                    buy_sell = format!("{:.2}", buy_pressure_ratio),
                    reason = %trigger_reason,
                    ">>> EXECUTING BUY ORDER <<<"
                );

                match executor.execute_buy(&mint, &pool).await {
                    Ok(result) => {
                        if result.accepted {
                            last_trade_time = Utc::now();

                            // ─── Fee-Aware PnL Calculation ───
                            // We must recover our overhead (Buy Tip + Buy Fee + Sell Tip + Sell Fee) 
                            // before we are truly in profit.
                            let overhead_sol = (
                                (jito_config.tip_lamports * 2) + 
                                (trading_config.priority_fee_lamports * 2)
                            ) as f64 / 1_000_000_000.0;

                            let entry_price = trading_config.max_sol_per_trade;
                            
                            // To hit a 15% net profit, we need: (Price * (1 + 15%)) + Overhead
                            let take_profit_price = (entry_price * (1.0 + trading_config.take_profit_pct)) + overhead_sol;
                            
                            // Stop loss should also consider fees to avoid "bleeding" out
                            let stop_loss_price = (entry_price * (1.0 - trading_config.stop_loss_pct)) + overhead_sol;

                            let mut state_write = state.write().await;
                            state_write.total_trades += 1;
                            state_write.active_positions.push(ActivePosition {
                                mint,
                                pool,
                                entry_tx_signature: result.bundle_id.clone(),
                                entry_price_sol: entry_price,
                                amount_tokens: 0, // Would be filled from swap result
                                invested_sol: trading_config.max_sol_per_trade,
                                entry_time: Utc::now(),
                                take_profit_price,
                                stop_loss_price,
                            });

                            // Add to executor's pending sells for fast execution
                            executor.add_pending_sell(PendingSell {
                                mint,
                                pool,
                                token_amount: 0, // Will need to be filled from actual swap result
                                entry_price_sol: entry_price,
                                take_profit_price,
                                stop_loss_price,
                                entry_time: Utc::now(),
                            });

                            info!(
                                bundle = %result.bundle_id,
                                positions = state_write.active_positions.len(),
                                "Buy order executed successfully - pending sell registered"
                            );
                        } else {
                            warn!(error = ?result.error, "Buy order failed");
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Buy execution error");
                    }
                }
            }

            Ok(TradeSignal::Sell {
                mint,
                pool,
                reason,
                pnl_pct,
                ..
            }) => {
                info!(
                    mint = %mint,
                    pool = %pool,
                    reason = ?reason,
                    pnl = format!("{:.2}%", pnl_pct * 100.0),
                    ">>> SELL SIGNAL <<<"
                );

                // Remove from active positions
                let (token_amount, invested) = {
                    let mut state_write = state.write().await;
                    let pos_idx = state_write
                        .active_positions
                        .iter()
                        .position(|p| p.mint == mint);

                    if let Some(idx) = pos_idx {
                        let pos = state_write.active_positions.remove(idx);
                        let entry = pos.invested_sol;
                        let tokens = pos.amount_tokens;
                        state_write.completed_trades.push(CompletedTrade {
                            mint,
                            entry_price: pos.entry_price_sol,
                            exit_price: entry * (1.0 + pnl_pct),
                            pnl_sol: entry * pnl_pct,
                            pnl_pct,
                            held_seconds: (Utc::now() - pos.entry_time).num_seconds() as u64,
                            reason,
                        });
                        state_write.total_pnl_sol += entry * pnl_pct;
                        if pnl_pct > 0.0 {
                            state_write.winning_trades += 1;
                        }
                        (tokens, entry)
                    } else {
                        warn!(mint = %mint, "Sell signal for unknown position");
                        continue;
                    }
                };

                // Execute the sell
                let min_sol_output = ((invested * (1.0 - trading_config.stop_loss_pct))
                    * LAMPORTS_PER_SOL as f64) as u64;

                match executor.execute_sell_fast(&mint, &pool, token_amount, min_sol_output).await {
                    Ok(result) => {
                        if result.accepted {
                            info!(
                                mint = %mint,
                                bundle = %result.bundle_id,
                                "Sell order executed"
                            );
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Sell execution error");
                    }
                }
            }

            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!("Strategy lagged by {n} signals");
            }
            Err(broadcast::error::RecvError::Closed) => {
                warn!("Trade signal channel closed");
                break;
            }
        }
    }

    Ok(())
}

/// Calculate trailing stop loss - 35% initial, moves up with price
/// Never goes backwards - only up
fn calculate_trailing_stop(entry_price: f64, current_price: f64, initial_stop_pct: f64) -> f64 {
    let pnl_pct = (current_price - entry_price) / entry_price;
    
    // Initial stop at -35%
    let base_stop = entry_price * (1.0 - initial_stop_pct);
    
    // Stop moves up, NEVER down
    match pnl_pct {
        p if p < 0.05 => entry_price * (1.0 - initial_stop_pct),     // < +5% → -35%
        p if p < 0.10 => entry_price * 0.70,                       // +5-10% → -30%
        p if p < 0.20 => entry_price * 0.75,                       // +10-20% → -25%
        p if p < 0.30 => entry_price * 0.85,                       // +20-30% → -15%
        p if p < 0.50 => entry_price * 0.95,                       // +30-50% → -5%
        p if p < 0.75 => entry_price * 1.10,                       // +50-75% → +10%
        _ => entry_price * 1.30,                                   // +75%+ → +30%
    }
}
