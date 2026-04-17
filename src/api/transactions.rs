//! Transactions endpoints.
//!
//! `GET /transactions?chainId=X`                 — newest [`LIST_LIMIT`]
//!                                                 transactions joined with
//!                                                 their receipts.
//! `GET /transactions?chainId=X&block=N`         — all transactions for block
//!                                                 `N`, ordered by index.
//! `GET /transactions?chainId=X&address=0x...`   — newest [`LIST_LIMIT`]
//!                                                 transactions where `address`
//!                                                 is either `from` or `to`.
//!                                                 Combinable with `block=` to
//!                                                 scope to a single block.
//! `GET /transactions?chainId=X&live=true`       — SSE stream: initial
//!                                                 snapshot, then one event
//!                                                 per newly indexed block
//!                                                 containing that block's
//!                                                 transactions. Mirrors the
//!                                                 `/blocks?live=true` framing
//!                                                 (`event: result` / `lagged`
//!                                                 / `error`). Not compatible
//!                                                 with `block=` or `address=`.
//! `GET /transactions/:hash?chainId=X`           — single transaction by
//!                                                 0x-prefixed 32-byte hash,
//!                                                 joined with its receipt.

use std::convert::Infallible;
use std::time::Instant;

use axum::{
    extract::{Path, Query, State},
    response::{
        sse::{Event as SseEvent, KeepAlive, KeepAliveStream},
        IntoResponse, Response, Sse,
    },
    Json,
};
use chrono::{DateTime, Utc};
use futures::Stream;
use serde::{Deserialize, Serialize};

use crate::api::{ApiError, AppState};

/// Fixed page size. Not user-selectable for now; revisit when pagination lands.
const LIST_LIMIT: i64 = 50;

#[derive(Deserialize)]
pub struct TransactionsParams {
    #[serde(alias = "chain_id", rename = "chainId")]
    chain_id: u64,
    #[serde(default)]
    live: bool,
    #[serde(default)]
    block: Option<u64>,
    #[serde(default)]
    address: Option<String>,
}

#[derive(Serialize)]
pub struct Transaction {
    hash: String,
    block_number: i64,
    /// Unix seconds.
    block_timestamp: i64,
    transaction_index: i32,
    from: String,
    to: Option<String>,
    value: String,
    nonce: i64,
    tx_type: i16,
    input: String,
    gas_limit: i64,
    gas_used: Option<i64>,
    effective_gas_price: Option<String>,
    status: Option<i16>,
    contract_address: Option<String>,
}

#[derive(Serialize)]
pub struct TransactionsResponse {
    ok: bool,
    transactions: Vec<Transaction>,
    count: usize,
    query_time_ms: f64,
}

/// `GET /transactions?chainId=X[&block=N][&address=0x...][&live=true]`
pub async fn list_transactions(
    State(state): State<AppState>,
    Query(params): Query<TransactionsParams>,
) -> Response {
    if params.live {
        if params.block.is_some() {
            return ApiError::BadRequest(
                "block filter is not supported with live=true".to_string(),
            )
            .into_response();
        }
        if params.address.is_some() {
            return ApiError::BadRequest(
                "address filter is not supported with live=true".to_string(),
            )
            .into_response();
        }
        handle_live(state, params).await.into_response()
    } else {
        handle_once(state, params).await.into_response()
    }
}

async fn handle_once(
    state: AppState,
    params: TransactionsParams,
) -> Result<Json<TransactionsResponse>, ApiError> {
    let pool = state
        .get_pool(Some(params.chain_id))
        .await
        .ok_or_else(|| ApiError::BadRequest(format!("Unknown chainId: {}", params.chain_id)))?;
    let conn = pool
        .get()
        .await
        .map_err(|e| ApiError::Internal(format!("Pool error: {e}")))?;

    let block_num = params
        .block
        .map(|b| i64::try_from(b).map_err(|_| ApiError::BadRequest(format!("block out of range: {b}"))))
        .transpose()?;
    let address_bytes = params.address.as_deref().map(parse_address).transpose()?;

    let start = Instant::now();
    let rows = match (address_bytes, block_num) {
        (Some(addr), Some(bn)) => conn
            .query(BY_ADDRESS_AND_BLOCK_SQL, &[&addr, &bn, &LIST_LIMIT])
            .await
            .map_err(|e| ApiError::QueryError(e.to_string()))?,
        (Some(addr), None) => conn
            .query(BY_ADDRESS_SQL, &[&addr, &LIST_LIMIT])
            .await
            .map_err(|e| ApiError::QueryError(e.to_string()))?,
        (None, Some(bn)) => conn
            .query(BY_BLOCK_SQL, &[&bn])
            .await
            .map_err(|e| ApiError::QueryError(e.to_string()))?,
        (None, None) => conn
            .query(LATEST_SQL, &[&LIST_LIMIT])
            .await
            .map_err(|e| ApiError::QueryError(e.to_string()))?,
    };

    let transactions: Vec<Transaction> = rows.iter().map(row_to_tx).collect();
    let count = transactions.len();
    let query_time_ms = start.elapsed().as_secs_f64() * 1000.0;

    Ok(Json(TransactionsResponse { ok: true, transactions, count, query_time_ms }))
}

const LATEST_SQL: &str = r#"
    SELECT
        t.hash,
        t.block_num,
        EXTRACT(EPOCH FROM t.block_timestamp)::INT8,
        t.idx,
        t."from",
        t."to",
        t.value,
        t.nonce,
        t.type,
        t.input,
        t.gas_limit,
        r.gas_used,
        r.effective_gas_price,
        r.status,
        r.contract_address
    FROM txs t
    LEFT JOIN receipts r
      ON r.tx_hash = t.hash
     AND r.block_num = t.block_num
    ORDER BY t.block_num DESC, t.idx DESC
    LIMIT $1
"#;

const BY_BLOCK_SQL: &str = r#"
    SELECT
        t.hash,
        t.block_num,
        EXTRACT(EPOCH FROM t.block_timestamp)::INT8,
        t.idx,
        t."from",
        t."to",
        t.value,
        t.nonce,
        t.type,
        t.input,
        t.gas_limit,
        r.gas_used,
        r.effective_gas_price,
        r.status,
        r.contract_address
    FROM txs t
    LEFT JOIN receipts r
      ON r.tx_hash = t.hash
     AND r.block_num = t.block_num
    WHERE t.block_num = $1
    ORDER BY t.idx ASC
"#;

// UNION ALL over the two indexed legs (`idx_txs_from`, `idx_txs_to`) rather
// than `WHERE "from" = $1 OR "to" = $1`, since Postgres typically won't combine
// two separate indexes for an OR on different columns. Each leg is capped at
// $2 so the outer ORDER BY never scans more than 2 * LIMIT rows.
const BY_ADDRESS_SQL: &str = r#"
    SELECT
        t.hash,
        t.block_num,
        EXTRACT(EPOCH FROM t.block_timestamp)::INT8,
        t.idx,
        t."from",
        t."to",
        t.value,
        t.nonce,
        t.type,
        t.input,
        t.gas_limit,
        r.gas_used,
        r.effective_gas_price,
        r.status,
        r.contract_address
    FROM (
        (SELECT * FROM txs WHERE "from" = $1 ORDER BY block_timestamp DESC LIMIT $2)
        UNION ALL
        (SELECT * FROM txs WHERE "to" = $1 ORDER BY block_timestamp DESC LIMIT $2)
    ) t
    LEFT JOIN receipts r
      ON r.tx_hash = t.hash
     AND r.block_num = t.block_num
    ORDER BY t.block_num DESC, t.idx DESC
    LIMIT $2
"#;

const BY_ADDRESS_AND_BLOCK_SQL: &str = r#"
    SELECT
        t.hash,
        t.block_num,
        EXTRACT(EPOCH FROM t.block_timestamp)::INT8,
        t.idx,
        t."from",
        t."to",
        t.value,
        t.nonce,
        t.type,
        t.input,
        t.gas_limit,
        r.gas_used,
        r.effective_gas_price,
        r.status,
        r.contract_address
    FROM (
        (SELECT * FROM txs WHERE "from" = $1 AND block_num = $2 ORDER BY block_timestamp DESC LIMIT $3)
        UNION ALL
        (SELECT * FROM txs WHERE "to" = $1 AND block_num = $2 ORDER BY block_timestamp DESC LIMIT $3)
    ) t
    LEFT JOIN receipts r
      ON r.tx_hash = t.hash
     AND r.block_num = t.block_num
    ORDER BY t.idx ASC
    LIMIT $3
"#;

fn row_to_tx(row: &tokio_postgres::Row) -> Transaction {
    let hash: Vec<u8> = row.get(0);
    let from: Vec<u8> = row.get(4);
    let to: Option<Vec<u8>> = row.get(5);
    let input: Vec<u8> = row.get(9);
    let contract_address: Option<Vec<u8>> = row.get(14);

    Transaction {
        hash: format!("0x{}", hex::encode(&hash)),
        block_number: row.get(1),
        block_timestamp: row.get(2),
        transaction_index: row.get(3),
        from: format!("0x{}", hex::encode(&from)),
        to: to.map(|b| format!("0x{}", hex::encode(&b))),
        value: row.get(6),
        nonce: row.get(7),
        tx_type: row.get(8),
        input: format!("0x{}", hex::encode(&input)),
        gas_limit: row.get(10),
        gas_used: row.get(11),
        effective_gas_price: row.get(12),
        status: row.get(13),
        contract_address: contract_address.map(|b| format!("0x{}", hex::encode(&b))),
    }
}

type SseStream = std::pin::Pin<Box<dyn Stream<Item = Result<SseEvent, Infallible>> + Send>>;

/// Maximum blocks to catch up in a single update (prevents query multiplication).
const MAX_CATCHUP_BLOCKS: u64 = 10;

async fn handle_live(
    state: AppState,
    params: TransactionsParams,
) -> Sse<KeepAliveStream<SseStream>> {
    let chain_id = params.chain_id;

    let pool = state.get_pool(Some(chain_id)).await;
    let mut rx = state.broadcaster.subscribe();

    let stream = async_stream::stream! {
        let Some(pool) = pool else {
            yield Ok(SseEvent::default()
                .event("error")
                .json_data(serde_json::json!({
                    "ok": false,
                    "error": format!("Unknown chainId: {chain_id}")
                }))
                .unwrap());
            return;
        };

        // Initial snapshot: newest N transactions.
        let mut last_seen: i64 = {
            let start = Instant::now();
            let conn = match pool.get().await {
                Ok(c) => c,
                Err(e) => {
                    yield Ok(SseEvent::default()
                        .event("error")
                        .json_data(serde_json::json!({ "ok": false, "error": format!("Pool error: {e}") }))
                        .unwrap());
                    return;
                }
            };
            match conn.query(LATEST_SQL, &[&LIST_LIMIT]).await {
                Ok(rows) => {
                    let transactions: Vec<Transaction> = rows.iter().map(row_to_tx).collect();
                    let latest = transactions.first().map(|t| t.block_number).unwrap_or(0);
                    let count = transactions.len();
                    let query_time_ms = start.elapsed().as_secs_f64() * 1000.0;
                    let resp = TransactionsResponse { ok: true, transactions, count, query_time_ms };
                    yield Ok(SseEvent::default()
                        .event("result")
                        .json_data(resp)
                        .unwrap());
                    latest
                }
                Err(e) => {
                    yield Ok(SseEvent::default()
                        .event("error")
                        .json_data(serde_json::json!({ "ok": false, "error": e.to_string() }))
                        .unwrap());
                    return;
                }
            }
        };

        loop {
            match rx.recv().await {
                Ok(update) => {
                    if update.chain_id != chain_id {
                        continue;
                    }
                    let end = update.block_num as i64;
                    if end <= last_seen {
                        continue;
                    }

                    let start_block = last_seen + 1;
                    let blocks_behind = (end - start_block + 1) as u64;
                    let effective_start = if blocks_behind > MAX_CATCHUP_BLOCKS {
                        yield Ok(SseEvent::default()
                            .event("lagged")
                            .json_data(serde_json::json!({
                                "skipped": blocks_behind - MAX_CATCHUP_BLOCKS,
                                "reason": "catch-up limit exceeded"
                            }))
                            .unwrap());
                        end - (MAX_CATCHUP_BLOCKS as i64) + 1
                    } else {
                        start_block
                    };

                    let conn = match pool.get().await {
                        Ok(c) => c,
                        Err(e) => {
                            yield Ok(SseEvent::default()
                                .event("error")
                                .json_data(serde_json::json!({ "ok": false, "error": format!("Pool error: {e}") }))
                                .unwrap());
                            return;
                        }
                    };

                    for block_num in effective_start..=end {
                        let qstart = Instant::now();
                        match conn.query(BY_BLOCK_SQL, &[&block_num]).await {
                            Ok(rows) => {
                                let transactions: Vec<Transaction> = rows.iter().map(row_to_tx).collect();
                                let count = transactions.len();
                                let resp = TransactionsResponse {
                                    ok: true,
                                    transactions,
                                    count,
                                    query_time_ms: qstart.elapsed().as_secs_f64() * 1000.0,
                                };
                                yield Ok(SseEvent::default()
                                    .event("result")
                                    .json_data(resp)
                                    .unwrap());
                            }
                            Err(e) => {
                                yield Ok(SseEvent::default()
                                    .event("error")
                                    .json_data(serde_json::json!({ "ok": false, "error": e.to_string() }))
                                    .unwrap());
                            }
                        }
                    }
                    last_seen = end;
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    yield Ok(SseEvent::default()
                        .event("lagged")
                        .json_data(serde_json::json!({ "skipped": n }))
                        .unwrap());
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    };

    let stream: SseStream = Box::pin(stream);
    Sse::new(stream).keep_alive(KeepAlive::default())
}

// ---------------------------------------------------------------------------
// Single-transaction detail endpoint: GET /transactions/:hash

#[derive(Serialize)]
pub struct TransactionDetail {
    hash: String,
    block_number: i64,
    /// RFC3339 timestamp.
    block_timestamp: String,
    /// Unix seconds.
    block_timestamp_unix: i64,
    transaction_index: i32,
    from: String,
    to: Option<String>,
    value: String,
    nonce: i64,
    tx_type: i16,
    input: String,
    gas_limit: i64,
    max_fee_per_gas: String,
    max_priority_fee_per_gas: String,
    gas_used: Option<i64>,
    cumulative_gas_used: Option<i64>,
    effective_gas_price: Option<String>,
    status: Option<i16>,
    contract_address: Option<String>,
}

#[derive(Serialize)]
pub struct TransactionResponse {
    ok: bool,
    transaction: TransactionDetail,
    query_time_ms: f64,
}

#[derive(Deserialize)]
pub struct TransactionDetailParams {
    #[serde(alias = "chain_id", rename = "chainId")]
    chain_id: u64,
}

/// `GET /transactions/:hash?chainId=X` — `hash` is a `0x`-prefixed 32-byte transaction hash.
pub async fn get_transaction(
    State(state): State<AppState>,
    Path(hash): Path<String>,
    Query(params): Query<TransactionDetailParams>,
) -> Result<Json<TransactionResponse>, ApiError> {
    let hash_bytes = parse_tx_hash(&hash)?;

    let pool = state
        .get_pool(Some(params.chain_id))
        .await
        .ok_or_else(|| ApiError::BadRequest(format!("Unknown chainId: {}", params.chain_id)))?;

    let conn = pool
        .get()
        .await
        .map_err(|e| ApiError::Internal(format!("Pool error: {e}")))?;

    let start = Instant::now();
    let row_opt = conn
        .query_opt(DETAIL_SQL, &[&hash_bytes])
        .await
        .map_err(|e| ApiError::QueryError(e.to_string()))?;

    let row = row_opt
        .ok_or_else(|| ApiError::NotFound(format!("Transaction not found: {hash}")))?;

    let hash_col: Vec<u8> = row.get(0);
    let block_timestamp: DateTime<Utc> = row.get(2);
    let from_col: Vec<u8> = row.get(4);
    let to_col: Option<Vec<u8>> = row.get(5);
    let input_col: Vec<u8> = row.get(9);
    let contract_address: Option<Vec<u8>> = row.get(16);

    let query_time_ms = start.elapsed().as_secs_f64() * 1000.0;

    Ok(Json(TransactionResponse {
        ok: true,
        transaction: TransactionDetail {
            hash: hex_prefixed(&hash_col),
            block_number: row.get(1),
            block_timestamp: block_timestamp.to_rfc3339(),
            block_timestamp_unix: block_timestamp.timestamp(),
            transaction_index: row.get(3),
            from: hex_prefixed(&from_col),
            to: to_col.as_deref().map(hex_prefixed),
            value: row.get(6),
            nonce: row.get(7),
            tx_type: row.get(8),
            input: hex_prefixed(&input_col),
            gas_limit: row.get(10),
            max_fee_per_gas: row.get(11),
            max_priority_fee_per_gas: row.get(12),
            gas_used: row.get(13),
            cumulative_gas_used: row.get(14),
            effective_gas_price: row.get(15),
            status: row.get(17),
            contract_address: contract_address.as_deref().map(hex_prefixed),
        },
        query_time_ms,
    }))
}

const DETAIL_SQL: &str = r#"
    SELECT
        t.hash,
        t.block_num,
        t.block_timestamp,
        t.idx,
        t."from",
        t."to",
        t.value,
        t.nonce,
        t.type,
        t.input,
        t.gas_limit,
        t.max_fee_per_gas,
        t.max_priority_fee_per_gas,
        r.gas_used,
        r.cumulative_gas_used,
        r.effective_gas_price,
        r.contract_address,
        r.status
    FROM txs t
    LEFT JOIN receipts r
      ON r.tx_hash = t.hash
     AND r.block_num = t.block_num
    WHERE t.hash = $1
    LIMIT 1
"#;

/// Parse an `0x`-prefixed 20-byte Ethereum address into raw bytes for a
/// `BYTEA` comparison against the `txs."from"` / `txs."to"` columns.
/// Case-insensitive; does not perform EIP-55 checksum validation.
fn parse_address(addr: &str) -> Result<Vec<u8>, ApiError> {
    let hex = addr
        .strip_prefix("0x")
        .or_else(|| addr.strip_prefix("0X"))
        .ok_or_else(|| {
            ApiError::BadRequest("address must be 0x-prefixed 20-byte hex".to_string())
        })?;

    if hex.len() != 40 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(ApiError::BadRequest(
            "address must be 0x + 40 hex characters".to_string(),
        ));
    }

    hex::decode(hex).map_err(|e| ApiError::BadRequest(format!("Invalid hex in address: {e}")))
}

fn parse_tx_hash(id: &str) -> Result<Vec<u8>, ApiError> {
    let hex = id
        .strip_prefix("0x")
        .or_else(|| id.strip_prefix("0X"))
        .ok_or_else(|| {
            ApiError::BadRequest(
                "Transaction hash must be 0x-prefixed 32-byte hex".to_string(),
            )
        })?;

    if hex.len() != 64 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(ApiError::BadRequest(
            "Transaction hash must be 0x + 64 hex characters".to_string(),
        ));
    }

    hex::decode(hex)
        .map_err(|e| ApiError::BadRequest(format!("Invalid hex in transaction hash: {e}")))
}

fn hex_prefixed(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tx_hash_lower() {
        let h = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        let bytes = parse_tx_hash(h).unwrap();
        assert_eq!(bytes.len(), 32);
    }

    #[test]
    fn parse_tx_hash_mixed_case() {
        let h = "0xABCDef1234567890ABCDef1234567890ABCDef1234567890ABCDef1234567890";
        assert!(parse_tx_hash(h).is_ok());
    }

    #[test]
    fn parse_tx_hash_rejects_missing_prefix() {
        let h = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        assert!(parse_tx_hash(h).is_err());
    }

    #[test]
    fn parse_tx_hash_rejects_short() {
        assert!(parse_tx_hash("0xdeadbeef").is_err());
    }

    #[test]
    fn parse_tx_hash_rejects_garbage() {
        assert!(parse_tx_hash("0xzzzz").is_err());
        assert!(parse_tx_hash("latest").is_err());
    }

    #[test]
    fn parse_address_lower() {
        let bytes = parse_address("0xdac17f958d2ee523a2206206994597c13d831ec7").unwrap();
        assert_eq!(bytes.len(), 20);
    }

    #[test]
    fn parse_address_mixed_case() {
        let bytes = parse_address("0xdAC17F958D2ee523a2206206994597C13D831ec7").unwrap();
        assert_eq!(bytes.len(), 20);
    }

    #[test]
    fn parse_address_rejects_missing_prefix() {
        assert!(parse_address("dac17f958d2ee523a2206206994597c13d831ec7").is_err());
    }

    #[test]
    fn parse_address_rejects_wrong_length() {
        assert!(parse_address("0xdeadbeef").is_err());
        assert!(parse_address("0xdac17f958d2ee523a2206206994597c13d831ec70000").is_err());
    }

    #[test]
    fn parse_address_rejects_garbage() {
        assert!(parse_address("0xGGGG7f958d2ee523a2206206994597c13d831ec7").is_err());
    }
}
