use crate::config::ShredStreamConfig;
use crate::types::*;
use anyhow::Result;
use solana_sdk::pubkey::Pubkey;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// The ShredStream listener binds a UDP socket to receive raw shreds from Jito.
///
/// Architecture:
///   Jito Block Engine ──UDP──> jito-shredstream-proxy ──UDP──> [THIS LISTENER]
///
/// Each "shred" is a ~1228-byte packet containing pieces of slots/transactions.
/// We reconstruct them and forward complete transactions downstream.
pub struct ShredListener {
    bind_addr: SocketAddr,
    proxy_addr: SocketAddr,
    recv_buffer: usize,
}

impl ShredListener {
    pub fn new(config: ShredStreamConfig) -> Self {
        let bind_addr: SocketAddr = config
            .bind_address
            .parse()
            .expect("Invalid bind address");
        let proxy_addr: SocketAddr = config
            .proxy_address
            .parse()
            .expect("Invalid proxy address");

        Self {
            bind_addr,
            proxy_addr,
            recv_buffer: config.recv_buffer_size,
        }
    }
}

/// Spawns the UDP listener task that receives shreds and sends raw transaction bytes downstream.
///
/// This runs in its own thread (spawned from tokio) because raw UDP I/O is blocking.
pub async fn run(
    tx_sender: mpsc::Sender<Vec<u8>>,
    tx_for_velocity_sender: mpsc::Sender<Vec<u8>>, // For TPM tracking
    config: ShredStreamConfig,
) -> Result<()> {
    let listener = ShredListener::new(config);
    listener.start(tx_sender, tx_for_velocity_sender).await
}

impl ShredListener {
    async fn start(
        self, 
        tx_sender: mpsc::Sender<Vec<u8>>,
        tx_for_velocity_sender: mpsc::Sender<Vec<u8>>,
    ) -> Result<()> {
        info!(
            bind = %self.bind_addr,
            proxy = %self.proxy_addr,
            buffer_size = self.recv_buffer,
            "Starting ShredStream UDP listener"
        );

        // Use socket2 for advanced options (buffer size, reuse port)
        let socket = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        )?;

        // Set receive buffer size (crucial for high-throughput shred ingestion)
        socket.set_recv_buffer_size(self.recv_buffer)?;
        socket.set_nonblocking(true)?;
        socket.set_reuse_address(true)?;

let std_socket: std::net::UdpSocket = socket.into();
        std::net::UdpSocket::bind(self.bind_addr.clone())?;
        std_socket.connect(self.proxy_addr)?;

        info!("UDP socket bound and connected to Jito ShredStream proxy");

        let udp = tokio::net::UdpSocket::from_std(std_socket)?;
        let mut buf = vec![0u8; 65535]; // Max UDP packet size

        let mut shred_count: u64 = 0;
        let mut tx_count: u64 = 0;
        let mut last_report = tokio::time::Instant::now();

        loop {
            match udp.recv(&mut buf).await {
                Ok(len) => {
                    shred_count += 1;

                    // Parse the shred header and extract any complete transactions
                    let shred_data = &buf[..len];
                    let extracted = Self::parse_shred(shred_data);

                    for tx_bytes in extracted {
                        // Send to Raydium detector (for pool detection)
                        if tx_sender.send(tx_bytes.clone()).await.is_err() {
                            warn!("Channel closed, stopping shred listener");
                            return Ok(());
                        }
                        
                        // Send to Velocity Monitor (for TPM tracking)
                        let _ = tx_for_velocity_sender.send(tx_bytes).await;
                        
                        tx_count += 1;
                    }

                    // Log stats every 5 seconds
                    if last_report.elapsed() >= Duration::from_secs(5) {
                        let elapsed = last_report.elapsed().as_secs_f64();
                        let shreds_per_sec = shred_count as f64 / elapsed;
                        let txs_per_sec = tx_count as f64 / elapsed;
                        info!(
                            shreds_total = shred_count,
                            txs_total = tx_count,
                            shreds_sec = format!("{:.0}", shreds_per_sec),
                            txs_sec = format!("{:.0}", txs_per_sec),
                            "ShredStream stats"
                        );
                        shred_count = 0;
                        tx_count = 0;
                        last_report = tokio::time::Instant::now();
                    }
                }
                Err(e) => {
                    error!(error = %e, "UDP receive error");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }

    /// Parse a raw shred packet and extract serialized transaction bytes.
    ///
    /// Solana shreds contain FEC data, duplicate shreds (for reliability),
    /// and transaction payloads. We skip FEC/padding and extract only
    /// complete transaction payloads.
    fn parse_shred(data: &[u8]) -> Vec<Vec<u8>> {
        let mut transactions = Vec::new();

        // Shred header format:
        // [0..1]   shred_type (1 byte): 0=Data, 1=Coding, 2=LastInSlot
        // [1..5]   slot (4 bytes LE)
        // [5..89]  common header fields (84 bytes)
        // [89..]   payload (varying)

        if data.len() < 89 {
            return transactions;
        }

        let shred_type = data[0];
        let slot = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);

        // Only process data shreds (type 0) and last-in-slot shreds (type 2)
        if shred_type != 0 && shred_type != 2 {
            return transactions;
        }

        // The payload starts after the common header
        // Data shreds contain: [common_header(89)] [proof(64)] [payload...]
        // For our purposes, we scan the payload for transaction boundaries
        let payload_start = 89 + 64; // common header + Merkle proof
        if payload_start >= data.len() {
            return transactions;
        }

        let payload = &data[payload_start..];

        // Solana serializes transactions using a compact-array format.
        // Each transaction in a payload block starts with:
        //   - 1 byte: compact u16 length prefix
        //   - N bytes: serialized transaction data
        // We scan for these boundaries.
        let mut offset = 0;
        while offset < payload.len() {
            // Compact array format: first byte encodes length
            let first_byte = payload[offset];
            let (tx_len, bytes_consumed) = if first_byte < 0x80 {
                // Single byte length
                (first_byte as usize, 1)
            } else {
                // Multi-byte compact encoding (varint)
                let len = ((first_byte & 0x7F) as usize) << 8;
                if offset + 1 >= payload.len() {
                    break;
                }
                let second_byte = payload[offset + 1] as usize;
                (len | second_byte, 2)
            };

            offset += bytes_consumed;

            if tx_len == 0 || tx_len > 1232 || offset + tx_len > payload.len() {
                break;
            }

            let tx_data = payload[offset..offset + tx_len].to_vec();
            transactions.push(tx_data);
            offset += tx_len;
        }

        if !transactions.is_empty() {
            debug!(
                slot = slot,
                shred_type = shred_type,
                tx_count = transactions.len(),
                "Extracted transactions from shred"
            );
        }

        transactions
    }
}

/// Connect the shred stream to the proxy via a separate socket.
/// This "handshake" packet tells the proxy we are ready to receive.
fn send_subscribe_packet(socket: &UdpSocket, proxy_addr: SocketAddr) -> Result<()> {
    // Jito ShredStream proxy uses a simple protocol:
    // Send a 0-byte UDP packet to the proxy to subscribe
    socket.send_to(&[], proxy_addr)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_shred_too_short() {
        let data = vec![0u8; 50];
        let result = ShredListener::parse_shred(&data);
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_shred_coding_type() {
        let mut data = vec![0u8; 200];
        data[0] = 1; // Coding shred
        let result = ShredListener::parse_shred(&data);
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_shred_data_type() {
        let mut data = vec![0u8; 300];
        data[0] = 0; // Data shred
        // Set slot
        data[1] = 0x01;
        data[2] = 0x00;
        data[3] = 0x00;
        data[4] = 0x00;

        let result = ShredListener::parse_shred(&data);
        // Payload starts at 153, we need at least compact-u16 + tx data
        assert!(result.is_empty()); // Empty payload, no txs
    }
}
