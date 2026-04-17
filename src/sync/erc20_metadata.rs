//! ERC20 metadata worker.
//!
//! Resolves `name` / `symbol` / `decimals` for rows that the sync writer has
//! marked as `resolution_status = 'pending'` in `erc20_tokens`. Discovery of
//! new addresses is handled atomically at sync-write time (see
//! `src/sync/writer.rs::upsert_erc20_tokens_from_staging_logs`), so this
//! worker is resolution-only.
//!
//! Every [`RESOLUTION_INTERVAL_SECS`] it picks up pending rows in batches
//! and issues a single Multicall3 (`0xcA11bde05977b3631167028862bE2a173976CA11`,
//! same address on Ethereum Mainnet and Sepolia) `aggregate3` call with:
//! `getBlockNumber()`, `getCurrentBlockTimestamp()`, then
//! `name()`/`symbol()`/`decimals()` per token. The block anchor comes back in
//! the same atomic response so the metadata is known to reflect exactly that
//! chain state.
//!
//! Legacy tokens like MKR/SAI whose `name()`/`symbol()` return `bytes32`
//! instead of `string` are decoded via a UTF-8 trim-null fallback.

use std::time::{Duration, Instant};

use alloy::primitives::{Address, Bytes, address};
use alloy::sol;
use alloy::sol_types::{SolCall, SolValue};
use anyhow::{Result, anyhow};
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use crate::db::Pool;
use crate::metrics;

use super::fetcher::RpcClient;

/// How often the worker runs a resolution cycle when everything is healthy.
pub const RESOLUTION_INTERVAL_SECS: u64 = 60;

/// Ceiling for the exponential-backoff delay applied after consecutive tick
/// failures. Truncated at 10 minutes so a recovered RPC is re-tried reasonably
/// soon even after a prolonged outage.
pub const MAX_BACKOFF_SECS: u64 = 600;

/// Multicall3 is deployed at the canonical address on Ethereum Mainnet,
/// Sepolia, and most EVM chains. See <https://www.multicall3.com/>.
pub const MULTICALL3_ADDRESS: Address = address!("cA11bde05977b3631167028862bE2a173976CA11");

/// How many tokens to resolve per Multicall3 batch.
///
/// Each token contributes 3 sub-calls (name, symbol, decimals), so this is
/// 1500 calls per aggregate3 request — comfortably within public RPC limits.
const RESOLUTION_BATCH_SIZE: usize = 500;

/// Max attempts before giving up on a token. Tokens that repeatedly fail to
/// resolve (e.g., contracts that don't implement the metadata methods) are
/// marked `failed` after this many tries so the worker stops retrying them.
const MAX_RESOLUTION_ATTEMPTS: i32 = 3;

sol! {
    // Minimal IERC20Metadata interface — only the three methods we need.
    interface IERC20Metadata {
        function name() external view returns (string);
        function symbol() external view returns (string);
        function decimals() external view returns (uint8);
    }

    // Multicall3 aggregate3: one batch call, per-sub-call failure tolerance.
    // getBlockNumber / getCurrentBlockTimestamp are views on Multicall3 itself,
    // used to capture the block the RPC answered for atomically with the
    // metadata reads in the batch.
    interface IMulticall3 {
        struct Call3 {
            address target;
            bool allowFailure;
            bytes callData;
        }
        struct Result3 {
            bool success;
            bytes returnData;
        }
        function aggregate3(Call3[] calls) external payable returns (Result3[] memory);
        function getBlockNumber() external view returns (uint256);
        function getCurrentBlockTimestamp() external view returns (uint256);
    }
}

/// Background worker that keeps `erc20_tokens` populated and metadata resolved.
pub struct Erc20MetadataWorker {
    pool: Pool,
    rpc: RpcClient,
    chain_id: u64,
}

impl Erc20MetadataWorker {
    pub fn new(pool: Pool, rpc: RpcClient, chain_id: u64) -> Self {
        Self { pool, rpc, chain_id }
    }

    /// Runs until the shutdown receiver fires. Each iteration drains the
    /// pending queue via batched Multicall3 resolution. On tick failure the
    /// worker applies truncated exponential backoff before the next attempt
    /// (base [`RESOLUTION_INTERVAL_SECS`], capped at [`MAX_BACKOFF_SECS`]).
    /// A successful tick resets the backoff.
    pub async fn run(self, mut shutdown: broadcast::Receiver<()>) {
        info!(
            chain_id = self.chain_id,
            interval_secs = RESOLUTION_INTERVAL_SECS,
            "Starting ERC20 metadata worker"
        );

        let mut consecutive_failures: u32 = 0;
        metrics::set_erc20_consecutive_failures(self.chain_id, 0);

        loop {
            match self.tick().await {
                Ok(()) => {
                    consecutive_failures = 0;
                }
                Err(e) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    let next = compute_backoff(consecutive_failures);
                    error!(
                        chain_id = self.chain_id,
                        error = %e,
                        consecutive_failures,
                        next_retry_secs = next.as_secs(),
                        "ERC20 metadata worker tick failed"
                    );
                }
            }
            metrics::set_erc20_consecutive_failures(self.chain_id, consecutive_failures);

            let delay = compute_backoff(consecutive_failures);
            tokio::select! {
                _ = shutdown.recv() => {
                    info!(chain_id = self.chain_id, "Shutting down ERC20 metadata worker");
                    break;
                }
                _ = tokio::time::sleep(delay) => {}
            }
        }
    }

    async fn tick(&self) -> Result<()> {
        // Snapshot the pending backlog for Prometheus. Cheap — the partial
        // index on resolution_status='pending' keeps this O(pending).
        if let Ok(pending) = self.pending_count().await {
            metrics::set_erc20_tokens_pending(self.chain_id, pending);
        }

        // Keep resolving until the pending queue is drained. Lets a cold
        // start backfill quickly without waiting for future ticks.
        let mut total_resolved = 0_usize;
        loop {
            let resolved = self.run_resolution().await?;
            if resolved == 0 {
                break;
            }
            total_resolved += resolved;
        }
        if total_resolved > 0 {
            info!(
                chain_id = self.chain_id,
                resolved = total_resolved,
                "ERC20 metadata: resolved tokens"
            );
        }
        Ok(())
    }

    /// Returns the current count of rows with `resolution_status = 'pending'`
    /// for this chain. Used to publish the pending-backlog gauge.
    async fn pending_count(&self) -> Result<i64> {
        let conn = self.pool.get().await?;
        let row = conn
            .query_one(
                "SELECT COUNT(*) FROM erc20_tokens WHERE resolution_status = 'pending'",
                &[],
            )
            .await?;
        Ok(row.get::<_, i64>(0))
    }

    // ── Resolution ────────────────────────────────────────────────────────

    /// Picks up a batch of pending tokens, resolves them via Multicall3,
    /// and writes the results back. Returns the number of tokens processed
    /// (may be 0 if nothing is pending).
    async fn run_resolution(&self) -> Result<usize> {
        let pending: Vec<Vec<u8>> = {
            let conn = self.pool.get().await?;
            conn.query(
                r#"
                SELECT address FROM erc20_tokens
                WHERE resolution_status = 'pending'
                  AND resolution_attempts < $1
                ORDER BY first_transfer_at DESC
                LIMIT $2
                "#,
                &[&MAX_RESOLUTION_ATTEMPTS, &(RESOLUTION_BATCH_SIZE as i64)],
            )
            .await?
            .iter()
            .map(|r| r.get::<_, Vec<u8>>(0))
            .collect()
        };

        if pending.is_empty() {
            return Ok(0);
        }

        let addresses: Vec<Address> = pending
            .iter()
            .filter_map(|bytes| {
                if bytes.len() == 20 {
                    Some(Address::from_slice(bytes))
                } else {
                    warn!(
                        len = bytes.len(),
                        "Skipping erc20_tokens row with non-20-byte address"
                    );
                    None
                }
            })
            .collect();

        let batch = match self.multicall_metadata(&addresses).await {
            Ok(r) => r,
            Err(e) => {
                // Transient RPC failure: bump attempts on the whole batch so
                // we don't spin on a dead endpoint, then bail for this tick.
                warn!(
                    chain_id = self.chain_id,
                    error = %e,
                    batch_size = addresses.len(),
                    "Multicall3 batch failed, incrementing attempts"
                );
                metrics::record_erc20_batch_error(self.chain_id);
                self.increment_attempts(&pending).await?;
                return Err(e);
            }
        };

        // Block anchor is shared across all tokens in the batch (single atomic
        // aggregate3 call). Store as signed INT8 / seconds-since-epoch to
        // match sibling columns (first_transfer_at/_block, deployed_at/_block).
        let resolved_block: Option<i64> = batch.block_number.and_then(|n| i64::try_from(n).ok());
        let resolved_at_secs: Option<i64> =
            batch.block_timestamp.and_then(|n| i64::try_from(n).ok());

        let conn = self.pool.get().await?;
        let mut ok_count: u64 = 0;
        let mut partial_count: u64 = 0;
        let mut failed_count: u64 = 0;
        for (addr, result) in addresses.iter().zip(batch.results.iter()) {
            let status = match (result.name.is_some(), result.symbol.is_some(), result.decimals.is_some()) {
                (true, true, true) => {
                    ok_count += 1;
                    "ok"
                }
                (false, false, false) => {
                    failed_count += 1;
                    "failed"
                }
                _ => {
                    partial_count += 1;
                    "partial"
                }
            };
            conn.execute(
                r#"
                UPDATE erc20_tokens SET
                    name = $2,
                    symbol = $3,
                    decimals = $4,
                    resolution_status = $5,
                    resolution_attempts = resolution_attempts + 1,
                    resolved_at = CASE WHEN $6::INT8 IS NULL THEN NULL ELSE to_timestamp($6::INT8) END,
                    resolved_block = $7
                WHERE address = $1
                "#,
                &[
                    &addr.as_slice(),
                    &result.name,
                    &result.symbol,
                    &result.decimals.map(i16::from),
                    &status,
                    &resolved_at_secs,
                    &resolved_block,
                ],
            )
            .await?;
        }

        metrics::record_erc20_metadata_resolved(self.chain_id, "ok", ok_count);
        metrics::record_erc20_metadata_resolved(self.chain_id, "partial", partial_count);
        metrics::record_erc20_metadata_resolved(self.chain_id, "failed", failed_count);

        debug!(
            chain_id = self.chain_id,
            batch = addresses.len(),
            ok = ok_count,
            partial = partial_count,
            failed = failed_count,
            "Resolved Multicall3 batch"
        );
        Ok(addresses.len())
    }

    async fn increment_attempts(&self, addresses: &[Vec<u8>]) -> Result<()> {
        let conn = self.pool.get().await?;
        for addr in addresses {
            conn.execute(
                r#"
                UPDATE erc20_tokens SET resolution_attempts = resolution_attempts + 1
                WHERE address = $1
                "#,
                &[&addr.as_slice()],
            )
            .await?;
        }
        Ok(())
    }

    /// Encodes and sends a Multicall3 `aggregate3` for the batch, returning
    /// per-token decoded results plus the block number the RPC answered for.
    /// Individual sub-call failures are tolerated (allowFailure=true) and
    /// surface as `None` on the corresponding field.
    async fn multicall_metadata(&self, addresses: &[Address]) -> Result<BatchResult> {
        if addresses.is_empty() {
            return Ok(BatchResult::default());
        }

        let name_data = Bytes::from(IERC20Metadata::nameCall {}.abi_encode());
        let symbol_data = Bytes::from(IERC20Metadata::symbolCall {}.abi_encode());
        let decimals_data = Bytes::from(IERC20Metadata::decimalsCall {}.abi_encode());
        let block_number_data = Bytes::from(IMulticall3::getBlockNumberCall {}.abi_encode());
        let block_timestamp_data =
            Bytes::from(IMulticall3::getCurrentBlockTimestampCall {}.abi_encode());

        // First two sub-calls capture the block anchor (number + timestamp);
        // rest are per-token metadata. Block anchor calls target Multicall3
        // itself and are tolerated with allowFailure so metadata still lands
        // if either reverts unexpectedly.
        let mut calls: Vec<IMulticall3::Call3> = Vec::with_capacity(addresses.len() * 3 + 2);
        calls.push(IMulticall3::Call3 {
            target: MULTICALL3_ADDRESS,
            allowFailure: true,
            callData: block_number_data,
        });
        calls.push(IMulticall3::Call3 {
            target: MULTICALL3_ADDRESS,
            allowFailure: true,
            callData: block_timestamp_data,
        });
        for addr in addresses {
            calls.push(IMulticall3::Call3 {
                target: *addr,
                allowFailure: true,
                callData: name_data.clone(),
            });
            calls.push(IMulticall3::Call3 {
                target: *addr,
                allowFailure: true,
                callData: symbol_data.clone(),
            });
            calls.push(IMulticall3::Call3 {
                target: *addr,
                allowFailure: true,
                callData: decimals_data.clone(),
            });
        }

        let calldata = IMulticall3::aggregate3Call { calls }.abi_encode();
        let to_hex = format!("0x{}", hex::encode(MULTICALL3_ADDRESS.as_slice()));
        let data_hex = format!("0x{}", hex::encode(&calldata));

        let rpc_start = Instant::now();
        let raw = self.rpc.eth_call(&to_hex, &data_hex).await?;
        metrics::record_erc20_multicall_duration(self.chain_id, rpc_start.elapsed());

        let decoded = IMulticall3::aggregate3Call::abi_decode_returns(&raw)
            .map_err(|e| anyhow!("Failed to decode Multicall3 aggregate3 response: {e}"))?;

        let expected = addresses.len() * 3 + 2;
        if decoded.len() != expected {
            anyhow::bail!(
                "Multicall3 returned {} results, expected {}",
                decoded.len(),
                expected
            );
        }

        // First two results are the block anchor; tolerate failure (null) so
        // the metadata reads still land even if either sub-call reverts. Both
        // return uint256 which won't fit in u64 in theory, but block numbers
        // and timestamps always do in practice — we narrow via try_into.
        let block_number = decoded[0]
            .success
            .then(|| {
                IMulticall3::getBlockNumberCall::abi_decode_returns(&decoded[0].returnData).ok()
            })
            .flatten()
            .and_then(|u| u64::try_from(u).ok());
        let block_timestamp = decoded[1]
            .success
            .then(|| {
                IMulticall3::getCurrentBlockTimestampCall::abi_decode_returns(
                    &decoded[1].returnData,
                )
                .ok()
            })
            .flatten()
            .and_then(|u| u64::try_from(u).ok());

        let mut out = Vec::with_capacity(addresses.len());
        for chunk in decoded[2..].chunks_exact(3) {
            let name = chunk[0]
                .success
                .then(|| decode_string_or_bytes32(&chunk[0].returnData))
                .flatten();
            let symbol = chunk[1]
                .success
                .then(|| decode_string_or_bytes32(&chunk[1].returnData))
                .flatten();
            let decimals = chunk[2]
                .success
                .then(|| {
                    IERC20Metadata::decimalsCall::abi_decode_returns(&chunk[2].returnData).ok()
                })
                .flatten();
            out.push(MetadataResult { name, symbol, decimals });
        }
        Ok(BatchResult { block_number, block_timestamp, results: out })
    }
}

/// Result of one Multicall3 batch: per-token decoded metadata plus the
/// block anchor (number + timestamp, both as uint256 seconds-since-epoch),
/// reported atomically via Multicall3.getBlockNumber() /
/// getCurrentBlockTimestamp() in the same aggregate3 call.
#[derive(Debug, Clone, Default)]
struct BatchResult {
    block_number: Option<u64>,
    block_timestamp: Option<u64>,
    results: Vec<MetadataResult>,
}

/// Decoded metadata for a single token.
#[derive(Debug, Clone, Default)]
struct MetadataResult {
    name: Option<String>,
    symbol: Option<String>,
    decimals: Option<u8>,
}

/// Decode an ABI-encoded `string` return value, falling back to `bytes32`
/// for legacy tokens like MKR and SAI that predate the IERC20Metadata spec.
/// The happy path delegates to alloy's generated `String::abi_decode`; the
/// fallback is the part that has no alloy equivalent.
fn decode_string_or_bytes32(data: &[u8]) -> Option<String> {
    if data.is_empty() {
        return None;
    }
    // Preferred path: standard ABI-encoded `string`.
    if let Ok(s) = String::abi_decode(data) {
        if !s.is_empty() {
            return Some(s);
        }
    }
    // Legacy path: `bytes32` — trim trailing null bytes, interpret as UTF-8.
    if data.len() == 32 {
        let trimmed_len = data.iter().rposition(|&b| b != 0).map(|i| i + 1).unwrap_or(0);
        if trimmed_len > 0 {
            if let Ok(s) = std::str::from_utf8(&data[..trimmed_len]) {
                return Some(s.to_string());
            }
        }
    }
    None
}

/// Truncated exponential backoff: `RESOLUTION_INTERVAL_SECS * 2^failures`,
/// capped at `MAX_BACKOFF_SECS`. Returns `RESOLUTION_INTERVAL_SECS` when
/// there are no consecutive failures.
fn compute_backoff(consecutive_failures: u32) -> Duration {
    if consecutive_failures == 0 {
        return Duration::from_secs(RESOLUTION_INTERVAL_SECS);
    }
    // Cap the shift to avoid 2^failures overflowing u64 on extended outages.
    // The `min` below truncates long before this matters, but belt-and-braces.
    let shift = consecutive_failures.min(20);
    let multiplier: u64 = 1u64 << shift;
    let secs = RESOLUTION_INTERVAL_SECS
        .saturating_mul(multiplier)
        .min(MAX_BACKOFF_SECS);
    Duration::from_secs(secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes32_fallback_decodes_legacy_name() {
        // "Maker" padded with null bytes — the MKR-style return.
        let mut buf = [0u8; 32];
        buf[..5].copy_from_slice(b"Maker");
        let decoded = decode_string_or_bytes32(&buf);
        assert_eq!(decoded.as_deref(), Some("Maker"));
    }

    #[test]
    fn standard_string_decoding_takes_precedence() {
        // Standard ABI-encoded "USDT".
        let encoded = String::abi_encode(&"USDT".to_string());
        assert_eq!(decode_string_or_bytes32(&encoded).as_deref(), Some("USDT"));
    }

    #[test]
    fn empty_data_returns_none() {
        assert!(decode_string_or_bytes32(&[]).is_none());
    }

    #[test]
    fn all_zero_bytes32_returns_none() {
        assert!(decode_string_or_bytes32(&[0u8; 32]).is_none());
    }

    #[test]
    fn backoff_is_base_interval_when_no_failures() {
        assert_eq!(compute_backoff(0), Duration::from_secs(RESOLUTION_INTERVAL_SECS));
    }

    #[test]
    fn backoff_doubles_per_failure_until_cap() {
        assert_eq!(compute_backoff(1), Duration::from_secs(120));
        assert_eq!(compute_backoff(2), Duration::from_secs(240));
        assert_eq!(compute_backoff(3), Duration::from_secs(480));
    }

    #[test]
    fn backoff_truncates_at_cap() {
        // 60 * 2^4 = 960 → capped to MAX_BACKOFF_SECS (600)
        assert_eq!(compute_backoff(4), Duration::from_secs(MAX_BACKOFF_SECS));
        assert_eq!(compute_backoff(100), Duration::from_secs(MAX_BACKOFF_SECS));
        assert_eq!(compute_backoff(u32::MAX), Duration::from_secs(MAX_BACKOFF_SECS));
    }
}
