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
            if let Err(e) = executor_for_bh.refresh_blockhash() {
                warn!(error = %e, "Failed to refresh blockhash");
            } else {
                debug!("Blockhash refreshed for fast sells");
            }
        }
    });

    // Spawn price monitor
    let executor_for_price = executor.clone();
    let state_for_price = state.clone();
    let trading_for_price = trading_config.clone();
    
    tokio::spawn(async move {
        let mut check_interval = tokio::time::interval(Duration::from_millis(200));
        
        loop {
            check_interval.tick().await;
            
            // Get all pending sells
            let pending_sells = executor_for_price.get_pending_sells();
            
            if pending_sells.is_empty() {
                continue;
            }

            // Check current prices from pool data
            for sell in &pending_sells {
                let held_seconds = (Utc::now() - sell.entry_time).num_seconds() as u64;
                
                // Skip if not yet past the minimum hold time
                if held_seconds < 10 {
                    continue;
                }

                // Check max hold time - force sell
                if held_seconds >= trading_for_price.max_hold_seconds {
                    info!(
                        mint = %sell.mint,
                        held_sec = held_seconds,
                        "Max hold time reached - executing fast sell"
                    );
                    
                    let _ = executor_for_price.execute_sell_fast(
                        &sell.mint,
                        &sell.pool,
                        sell.token_amount,
                        ((sell.entry_price_sol * (1.0 - trading_for_price.stop_loss_pct)) * 1_000_000_000.0) as u64,
                    ).await;
                    
                    // Update state
                    let mut state_write = state_for_price.write().await;
                    if let Some(pos_idx) = state_write.active_positions.iter().position(|p| p.mint == sell.mint) {
                        let _pos = state_write.active_positions.remove(pos_idx);
                        let pnl_pct = -trading_for_price.stop_loss_pct;
                        state_write.completed_trades.push(CompletedTrade {
                            mint: sell.mint,
                            entry_price: sell.entry_price_sol,
                            exit_price: sell.entry_price_sol * (1.0 - trading_for_price.stop_loss_pct),
                            pnl_sol: sell.entry_price_sol * (-trading_for_price.stop_loss_pct),
                            pnl_pct,
                            held_seconds,
                            reason: SellReason::MaxHoldTime,
                        });
                        state_write.total_pnl_sol += sell.entry_price_sol * (-trading_for_price.stop_loss_pct);
                        if pnl_pct > 0.0 {
                            state_write.winning_trades += 1;
                        }
                    }
                    continue;
                }

                // Fetch current pool price and check TP/SL
                // In production, you'd fetch real price from pool reserves
                // For now, we skip actual price check and rely on time-based exits
                
                debug!(
                    mint = %sell.mint,
                    held_sec = held_seconds,
                    "Price check - waiting for targets"
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
