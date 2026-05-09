# Velocity Sniper — Solana Momentum Trading Bot

A Rust-based, ultra-low-latency Solana trading bot that monitors the Jito ShredStream for Raydium pool initialization events (token migrations from Pump.fun), applies a multi-stage safety and velocity filter, and executes atomic Jito Bundles for risk-free trades.

## Architecture Overview

```
┌─────────────────┐     UDP Raw Shreds
│  Jito Block     │─────────────────────┐
│  Engine         │                     │
└─────────────────┘                     ▼
                              ┌─────────────────┐
                              │ jito-shredstream│
                              │ -proxy (local)  │
                              └────────┬────────┘
                                       │ UDP
                                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    VELOCITY SNIPER (Rust)                           │
│                                                                      │
│  ┌──────────────┐    ┌────────────────┐    ┌────────────────────┐  │
│  │ Shred        │    │ Raydium        │    │ Safety             │  │
│  │ Listener     │───▶│ Detector       │───▶│ Filter             │  │
│  │ (UDP Socket) │    │ (initialize2)  │    │ (Mint/LP/Holder)   │  │
│  └──────────────┘    └────────────────┘    └─────────┬──────────┘  │
│                                                            │         │
│                                                            ▼         │
│                                              ┌────────────────────┐ │
│                                              │ Velocity           │ │
│                                              │ Monitor            │ │
│                                              │ (TPM / Pressure)   │ │
│                                              └─────────┬──────────┘ │
│                                                        │            │
│                              ┌──────────────────────────┤           │
│                              │                          ▼           │
│                              │              ┌────────────────────┐ │
│                              │              │ Strategy           │ │
│                              │              │ Orchestrator      │ │
│                              │              │ (Two-Stage Logic) │ │
│                              │              └─────────┬──────────┘ │
│                              │                        │            │
│                              │                        ▼            │
│                              │              ┌────────────────────┐ │
│                              │              │ Bundle             │ │
│                              │              │ Executor           │ │
│                              │              │ (Jito Atomic)      │ │
│                              │              └─────────┬──────────┘ │
│                              │                        │            │
└──────────────────────────────┼────────────────────────┼────────────┘
                               │                        │
                               ▼                        ▼
                    ┌─────────────────┐      ┌──────────────────┐
                    │ Solana RPC      │      │ Jito Block Engine│
                    │ (Account Data)  │      │ (Bundle Submit)  │
                    └─────────────────┘      └──────────────────┘
```

## The Strategy: "Accelerating Record Sniper"

### Why This Works

**Big companies** (Jump, Wintermute, etc.) trade on established tokens with deep liquidity. They use $400/month gRPC connections and machine learning to predict 5-minute candles.

**Your edge**: You trade where they can't — tokens with $10k–$50k liquidity that just graduated from Pump.fun to Raydium. Nobody with institutional capital can meaningfully trade these, and most retail traders wait 30–60 seconds for DexScreener to update.

### Two-Stage Strategy

| Stage | Action | Condition |
|-------|--------|-----------|
| **Stage 1: Scan** | Listen for `initialize2` on Raydium AMM V4 | ShredStream detects pool creation at the millisecond it happens |
| **Stage 2: Filter** | Wait 120 seconds ("War Zone") | Avoid the bot war in the first 60 seconds |
| **Stage 3: Execute** | Buy if velocity is increasing | TPM must increase 20%+ between minute 1 and minute 2, buy/sell ratio > 1.5 |

### Why Wait 2 Minutes?

The first 60 seconds after pool creation is "The War Zone":
- gRPC bots with sub-50ms latency fight each other
- Price often spikes 500% then crashes
- Developer may dump in the first 30 seconds

By entering at the 2-minute mark, you catch the **retail wave** that lasts 5–10 minutes. Your €7/month VPS is more than enough to beat humans clicking buttons on DexScreener.

### The "90% Win" Safety Filter

Before every trade, the bot verifies:

1. **Mint Authority Disabled**: No one can mint more tokens (supply is fixed)
2. **LP Tokens Burned**: Liquidity is locked forever (nobody can rug)
3. **Holder Distribution**: No single holder owns >15%, top 10 own <40%
4. **Freeze Authority Disabled**: No one can freeze your tokens
5. **Minimum Holders**: At least 20 unique token holders
6. **Jito Simulation**: The trade is simulated before submission — if it would fail, $0 is lost

## Quick Start

### 1. Prerequisites

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Build
cargo build --release

# The binary will be at: target/release/velocity-sniper
```

### 2. Server Setup (Hetzner Frankfurt)

```bash
# Rent a CX22 instance in Helsinki/Frankfurt (€7/month)
# OS: Ubuntu 24.04 LTS

# Increase UDP buffer for ShredStream
sudo sysctl -w net.core.rmem_max=50000000
sudo sysctl -w net.core.rmem_default=50000000

# Make permanent
echo "net.core.rmem_max=50000000" | sudo tee -a /etc/sysctl.conf
echo "net.core.rmem_default=50000000" | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
```

### 3. Jito ShredStream Proxy

The `jito-shredstream-proxy` is a local proxy that receives shreds from the Jito Block Engine and forwards them to your listener.

```bash
# Install jito-shredstream-proxy
# Follow: https://github.com/jito-labs/jito-shredstream-proxy

# Run the proxy (connects to Frankfurt block engine)
jito-shredstream-proxy \
    --block-engine-url https://frankfurt.mainnet.block-engine.jito.wiff \
    --bind-address 127.0.0.1:1002
```

### 4. Configuration

```bash
# Copy the example config
cp config.toml.example config.toml
cp .env.example .env

# Edit config.toml with your settings
# Edit .env with your private key
nano config.toml
```

### 5. Run

```bash
# Development mode
RUST_LOG=info cargo run

# Production mode (optimized)
./target/release/velocity-sniper
```

## Project Structure

```
velocity-sniper/
├── Cargo.toml              # Dependencies and build configuration
├── config.toml             # Runtime configuration
├── .env.example            # Environment variables template
├── src/
│   ├── main.rs             # Entry point, pipeline orchestration, tokio runtime
│   ├── config.rs           # Configuration types and loader
│   ├── types.rs            # Core data types, Solana constants, program IDs
│   ├── shred_listener.rs   # UDP socket for receiving raw shreds from proxy
│   ├── raydium_detector.rs # Scans transactions for initialize2 instructions
│   ├── safety_filter.rs    # Validates tokens (mint, LP, holders) via RPC
│   ├── velocity_monitor.rs # Tracks TPM, buy/sell pressure, wallet aging
│   ├── swap_calculator.rs  # Local AMM math (constant product formula)
│   ├── bundle_executor.rs  # Jito bundle construction, simulation, submission
│   └── strategy.rs         # Two-stage orchestration, position management
└── README.md               # This file
```

## Module Deep Dive

### `shred_listener` — The Eyes
- Binds a high-buffer UDP socket to receive raw shreds from the ShredStream proxy
- Parses Solana shred format (data shreds vs. coding shreds)
- Extracts serialized transaction payloads from shred payloads
- Forwards raw transaction bytes downstream via tokio mpsc channel
- Throughput target: ~500,000 shreds/second on a single VPS core

### `raydium_detector` — The Signal
- Parses every serialized Solana transaction from the wire format
- Scans account keys for the Raydium AMM V4 Program ID (`675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8`)
- Checks instruction data for the `initialize2` discriminator (`0x02b13c045c158651`)
- Extracts: pool address, LP mint, base/quote vaults, initial amounts
- Emits `PoolEvent` when a new Raydium pool is born

### `safety_filter` — The Shield
- Fetches the token's mint account data via Solana RPC
- Verifies mint authority and freeze authority are `None` (renounced)
- Checks LP token mint supply is 0 (all LP tokens burned)
- Fetches top token holders via `getTokenLargestAccounts`
- Calculates holder concentration (single holder %, top-10 combined %)
- Rejects tokens with suspicious distribution patterns

### `velocity_monitor` — The Brain
- Tracks every transaction for a monitored token in a sliding 60-second window
- Calculates TPM (Transactions Per Minute) at minute 1 and minute 2
- Computes buy/sell ratio (buys vs. sells in the window)
- Tracks unique wallet count and "old wallet" count (organic vs. bot)
- Emits `TradeSignal::Buy` only when ALL conditions are met:
  - TPM is INCREASING (velocity > 20%)
  - Minimum 30 transactions in minute 2
  - Buy/sell ratio > 1.5 (significantly more buys than sells)
  - At least 5 unique wallets

### `swap_calculator` — The CPU Edge
- Calculates swap output using the **constant product AMM formula**:
  - `output = y_reserve - (x_reserve * y_reserve) / (x_reserve + dx)`
- Computes price impact percentage
- Calculates fees (Raydium default: 0.25% = 25 bps)
- Computes minimum output for slippage protection
- **Zero API latency** — all math is done locally

### `bundle_executor` — The Trigger
- Constructs Jito Bundles containing: `[TIP transaction] + [BUY transaction]`
- Simulates the bundle via Jito's `/api/v1/bundles/simulate` endpoint
- Submits accepted bundles via `/api/v1/bundles`
- The Jito tip ensures priority inclusion in the next block
- If simulation fails (rug, sandwich, insufficient liquidity), **$0 is lost**

### `strategy` — The Orchestrator
- Manages the full pipeline: Scan → Filter → Wait → Execute
- Tracks active positions (entry price, time, token amount)
- Monitors take-profit (15%) and stop-loss (5%) levels
- Enforces cooldown between trades (60 seconds)
- Limits concurrent positions (max 3)
- Logs portfolio summary: total trades, win rate, PnL

## Key Dependencies

| Crate | Purpose |
|-------|---------|
| `solana-client` / `solana-sdk` | Core blockchain types, transaction building |
| `tokio` | Async runtime, channels, UDP, timers |
| `reqwest` | HTTP client for Jito API and Solana RPC |
| `dashmap` | Concurrent hashmap for velocity tracking |
| `serde` / `serde_json` | JSON serialization |
| `bincode` | Binary serialization for wire format |
| `bs58` | Base58 encoding/decoding (Solana addresses) |
| `socket2` | Advanced UDP socket configuration |
| `tracing` | Structured logging with JSON output |

## Risk Disclaimer

**This is experimental software for educational purposes.**

- Solana trading involves significant risk of capital loss
- Even with safety filters, rug pulls and market manipulation are possible
- Past performance does not guarantee future results
- The "90% win rate" is a theoretical target based on the described strategy
- Always test with small amounts first (<0.1 SOL)
- Never trade money you cannot afford to lose

## Performance Targets

| Metric | Target |
|--------|--------|
| Pool detection latency | <400ms from block production |
| Bundle submission latency | <100ms from signal trigger |
| Total trade execution time | <2 seconds from detection |
| Throughput (shred processing) | >100k shreds/second |
| Memory usage | <200MB RAM |
| CPU usage | <50% on single core (CX22) |

## License

MIT
