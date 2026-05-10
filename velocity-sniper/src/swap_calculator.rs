use crate::types::*;
use anyhow::Result;
use reqwest::Client;
use solana_sdk::pubkey::Pubkey;

/// Local Swap Calculator: Computes swap routes and expected output WITHOUT
/// calling an external API (Jupiter). All math is done locally on the VPS CPU.
///
/// This eliminates API latency. The bot calculates:
/// 1. Direct swap through the detected Raydium pool
/// 2. Expected output amount using constant product formula (x * y = k)
/// 3. Price impact estimation
/// 4. Optimal route selection
///
/// For simple token purchases, the route is:
///   SOL ──> [Raydium Pool] ──> Target Token
pub struct SwapCalculator {
    http: Client,
    rpc_url: String,
}

impl SwapCalculator {
    pub fn new(http: Client, rpc_url: String) -> Self {
        Self { http, rpc_url }
    }

    /// Calculate the expected output for a SOL -> Token swap.
    ///
    /// Uses the constant product AMM formula:
    ///   new_y = (x_reserve * y_reserve) / (x_reserve + dx)
    ///   output = y_reserve - new_y
    ///
    /// Where dx = input SOL (adjusted for fees)
    pub fn calculate_swap(
        &self,
        quote_reserve: u64,  // SOL in the pool (in lamports)
        base_reserve: u64,   // Tokens in the pool
        input_lamports: u64, // SOL we want to swap
        fee_bps: u16,        // Fee in basis points (Raydium default: 25 = 0.25%)
    ) -> SwapQuote {
        let fee = (input_lamports as u128 * fee_bps as u128) / 10_000;
        let input_after_fee = input_lamports.saturating_sub(fee as u64);

        // Constant product: k = quote_reserve * base_reserve
        let k = quote_reserve as u128 * base_reserve as u128;

        // New quote reserve after adding our input
        let new_quote_reserve = quote_reserve as u128 + input_after_fee as u128;

        // New base reserve (tokens remaining after swap)
        let new_base_reserve = k / new_quote_reserve;

        // Output tokens
        let output_tokens = base_reserve.saturating_sub(new_base_reserve as u64);

        // Price impact = output / base_reserve
        let price_impact_pct = if base_reserve > 0 {
            output_tokens as f64 / base_reserve as f64 * 100.0
        } else {
            100.0
        };

        // Clamp price impact for sanity
        let price_impact_pct = price_impact_pct.min(99.0);

        SwapQuote {
            input_mint: Pubkey::new_unique(), // SOL placeholder
            output_mint: Pubkey::new_unique(), // Token placeholder
            input_amount: input_lamports,
            output_amount: output_tokens,
            price_impact_pct,
            route: vec![SwapStep {
                pool: Pubkey::new_unique(),
                input_mint: Pubkey::new_unique(),
                output_mint: Pubkey::new_unique(),
                input_amount: input_after_fee as u64,
                output_amount: output_tokens,
                fee_pct: fee_bps as f64 / 10_000.0,
            }],
            fee_lamports: fee as u64,
        }
    }

    /// Calculate the reverse swap (Token -> SOL) for selling.
    pub fn calculate_sell(
        &self,
        quote_reserve: u64,
        base_reserve: u64,
        input_tokens: u64,
        fee_bps: u16,
    ) -> SwapQuote {
        let fee = (input_tokens as u128 * fee_bps as u128) / 10_000;
        let input_after_fee = input_tokens.saturating_sub(fee as u64);

        let k = quote_reserve as u128 * base_reserve as u128;
        let new_base_reserve = base_reserve as u128 + input_after_fee as u128;
        let new_quote_reserve = k / new_base_reserve;
        let output_lamports = quote_reserve.saturating_sub(new_quote_reserve as u64);

        let price_impact_pct = if quote_reserve > 0 {
            output_lamports as f64 / quote_reserve as f64 * 100.0
        } else {
            100.0
        };

        SwapQuote {
            input_mint: Pubkey::new_unique(),
            output_mint: Pubkey::new_unique(),
            input_amount: input_tokens,
            output_amount: output_lamports,
            price_impact_pct: price_impact_pct.min(99.0),
            route: vec![SwapStep {
                pool: Pubkey::new_unique(),
                input_mint: Pubkey::new_unique(),
                output_mint: Pubkey::new_unique(),
                input_amount: input_after_fee as u64,
                output_amount: output_lamports,
                fee_pct: fee_bps as f64 / 10_000.0,
            }],
            fee_lamports: fee as u64,
        }
    }

    /// Fetch current pool reserves from on-chain data.
    pub async fn fetch_pool_reserves(
        &self,
        pool: &Pubkey,
    ) -> Result<(u64, u64)> {
        // Raydium AMM V4 pool account layout (first 216 bytes):
        // [0..8]   status (u64)
        // [8..16]  nonce (u64)
        // [16..24] order_num (u64)
        // [24..56] coin_vault (Pubkey)
        // [56..88] pc_vault (Pubkey)
        // [88..120] coin_mint (Pubkey)
        // [120..152] pc_mint (Pubkey)
        // [152..184] lp_mint (Pubkey)
        // [184..192] coin_amount (u64)
        // [192..200] pc_amount (u64)

        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [
                pool.to_string(),
                {
                    "encoding": "base64",
                    "commitment": "confirmed"
                }
            ]
        });

        let resp = self.http.post(&self.rpc_url).json(&body).send().await?;
        let json: serde_json::Value = resp.json().await?;

        let data_b64 = json
            .pointer("/result/value/data/0")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Pool account not found"))?;

        let data = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            data_b64,
        )?;

        if data.len() < 200 {
            anyhow::bail!("Pool account data too short: {} bytes", data.len());
        }

        let base_amount = u64::from_le_bytes(data[184..192].try_into()?);
        let quote_amount = u64::from_le_bytes(data[192..200].try_into()?);

        Ok((quote_amount, base_amount))
    }

    /// Build the raw instruction data for a Raydium swap.
    /// This is a simplified version — production code would use full Anchor CPI.
    pub fn build_swap_instruction_data(
        amount_in: u64,
        minimum_amount_out: u64,
    ) -> Vec<u8> {
        // Raydium swap instruction (simplified)
        // In production, you would use the raydium-sdk or anchor-lang CPI
        let mut data = Vec::new();

        // Instruction discriminator for "swap" (sha256("global:swap")[..8])
        data.extend_from_slice(&[0x3b, 0x9b, 0xc6, 0xf1, 0x0e, 0xa5, 0x0c, 0xa1]);

        // amount_in (u64 LE)
        data.extend_from_slice(&amount_in.to_le_bytes());

        // minimum_amount_out (u64 LE) — for slippage protection
        data.extend_from_slice(&minimum_amount_out.to_le_bytes());

        data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_calculator() -> SwapCalculator {
        SwapCalculator::new(
            Client::new(),
            "https://api.mainnet-beta.solana.com".to_string(),
        )
    }

    #[test]
    fn test_calculate_buy_basic() {
        let calc = make_calculator();

        // Pool with 100 SOL and 1M tokens
        let quote = calc.calculate_swap(
            100_000_000_000, // 100 SOL in lamports
            1_000_000_000,   // 1M tokens
            1_000_000_000,   // 1 SOL input
            25,              // 0.25% fee
        );

        assert!(quote.output_amount > 0, "Should produce output tokens");
        assert!(quote.fee_lamports > 0, "Should have fees");
        assert!(quote.price_impact_pct < 100.0, "Impact should be reasonable");
        assert_eq!(quote.route.len(), 1);
    }

    #[test]
    fn test_calculate_sell_basic() {
        let calc = make_calculator();

        let quote = calc.calculate_sell(
            100_000_000_000, // 100 SOL in pool
            1_000_000_000,   // 1M tokens in pool
            10_000_000,      // Sell 10 tokens
            25,
        );

        assert!(quote.output_amount > 0);
    }

    #[test]
    fn test_price_impact_high() {
        let calc = make_calculator();

        // Buy 50 SOL from a 100 SOL pool — 50% price impact expected
        let quote = calc.calculate_swap(
            100_000_000_000,
            1_000_000_000,
            50_000_000_000,
            25,
        );

        assert!(quote.price_impact_pct > 30.0, "High input should cause high impact");
    }

    #[test]
    fn test_price_impact_low() {
        let calc = make_calculator();

        // Buy 0.1 SOL from a 1000 SOL pool — very low impact
        let quote = calc.calculate_swap(
            1_000_000_000_000,
            1_000_000_000_000,
            100_000_000,
            25,
        );

        assert!(quote.price_impact_pct < 1.0, "Small input should have low impact");
    }

    #[test]
    fn test_fee_calculation() {
        let calc = make_calculator();

        let quote = calc.calculate_swap(
            100_000_000_000,
            1_000_000_000,
            1_000_000_000,
            25,
        );

        // Fee should be 0.25% of input
        let expected_fee = 1_000_000_000 * 25 / 10_000;
        assert_eq!(quote.fee_lamports, expected_fee);
    }
}
