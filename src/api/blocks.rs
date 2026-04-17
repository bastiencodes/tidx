//! Blocks endpoints.
//!
//! `GET /blocks?chainId=X`                  — newest [`pagination::DEFAULT_LIMIT`] blocks
//!                                            from the `blocks` table. Supports keyset
//!                                            pagination via `limit` (clamped to
//!                                            [`pagination::MAX_LIMIT`]) and an opaque
//!                                            `cursor` returned as `next_cursor`.
//! `GET /blocks?chainId=X&live=true`       — SSE stream: initial snapshot, then one
//!                                            event per newly indexed block. Mirrors
//!                                            the `/query?live=true` framing
//!                                            (`event: result` / `lagged` / `error`).
//!                                            Pagination params are ignored in live
//!                                            mode.
//! `GET /blocks/:identifier?chainId=X`      — single block by decimal number or
//!                                            0x-prefixed 32-byte hash.

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

use crate::api::pagination;
use crate::api::{ApiError, AppState};

use pagination::{DEFAULT_LIMIT, MAX_LIMIT};

/// Keyset cursor for `/blocks` — the number of the last block returned on the
/// previous page. Next page is `num < cursor.n`.
#[derive(Serialize, Deserialize)]
struct BlocksCursor {
    n: i64,
}

#[derive(Deserialize)]
pub struct BlocksParams {
    #[serde(alias = "chain_id", rename = "chainId")]
    chain_id: u64,
    #[serde(default)]
    live: bool,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Serialize, Clone)]
pub struct Block {
    number: i64,
    hash: String,
    /// Unix seconds.
    timestamp: i64,
    transaction_count: i64,
}

#[derive(Serialize)]
pub struct BlocksResponse {
    ok: bool,
    blocks: Vec<Block>,
    count: usize,
    /// Opaque cursor for the next page, or `null` on the last page.
    next_cursor: Option<String>,
    query_time_ms: f64,
}

/// `GET /blocks?chainId=X[&live=true]`
pub async fn list_blocks(
    State(state): State<AppState>,
    Query(params): Query<BlocksParams>,
) -> Response {
    if params.live {
        handle_live(state, params).await.into_response()
    } else {
        handle_once(state, params).await.into_response()
    }
}

async fn handle_once(
    state: AppState,
    params: BlocksParams,
) -> Result<Json<BlocksResponse>, ApiError> {
    let pool = state
        .get_pool(Some(params.chain_id))
        .await
        .ok_or_else(|| ApiError::BadRequest(format!("Unknown chainId: {}", params.chain_id)))?;
    let conn = pool
        .get()
        .await
        .map_err(|e| ApiError::Internal(format!("Pool error: {e}")))?;

    let limit = pagination::clamp_limit(params.limit, DEFAULT_LIMIT, MAX_LIMIT);
    let cursor: Option<BlocksCursor> = params
        .cursor
        .as_deref()
        .map(pagination::decode)
        .transpose()?;

    let start = Instant::now();
    let rows = match cursor {
        Some(c) => conn
            .query(LIST_AFTER_SQL, &[&c.n, &limit])
            .await
            .map_err(|e| ApiError::QueryError(e.to_string()))?,
        None => conn
            .query(LIST_SQL, &[&limit])
            .await
            .map_err(|e| ApiError::QueryError(e.to_string()))?,
    };

    let blocks: Vec<Block> = rows.iter().map(row_to_block).collect();
    let count = blocks.len();
    let next_cursor = next_cursor_for(&blocks, limit);
    let query_time_ms = start.elapsed().as_secs_f64() * 1000.0;

    Ok(Json(BlocksResponse { ok: true, blocks, count, next_cursor, query_time_ms }))
}

const LIST_SQL: &str = r#"
    SELECT
        b.num,
        b.hash,
        b.timestamp_ms / 1000,
        (SELECT COUNT(*) FROM txs t WHERE t.block_num = b.num)
    FROM blocks b
    ORDER BY b.num DESC
    LIMIT $1
"#;

const LIST_AFTER_SQL: &str = r#"
    SELECT
        b.num,
        b.hash,
        b.timestamp_ms / 1000,
        (SELECT COUNT(*) FROM txs t WHERE t.block_num = b.num)
    FROM blocks b
    WHERE b.num < $1
    ORDER BY b.num DESC
    LIMIT $2
"#;

/// `Some(cursor)` only when the page filled — i.e. more rows likely exist.
/// A short page means we've hit the end, so `next_cursor` is `None`.
fn next_cursor_for(blocks: &[Block], limit: i64) -> Option<String> {
    if (blocks.len() as i64) < limit {
        return None;
    }
    blocks
        .last()
        .map(|b| pagination::encode(&BlocksCursor { n: b.number }))
}

fn row_to_block(row: &tokio_postgres::Row) -> Block {
    let hash: Vec<u8> = row.get(1);
    Block {
        number: row.get(0),
        hash: format!("0x{}", hex::encode(&hash)),
        timestamp: row.get(2),
        transaction_count: row.get(3),
    }
}

type SseStream = std::pin::Pin<Box<dyn Stream<Item = Result<SseEvent, Infallible>> + Send>>;

async fn handle_live(
    state: AppState,
    params: BlocksParams,
) -> Sse<KeepAliveStream<SseStream>> {
    let chain_id = params.chain_id;

    // Resolve pool before entering the stream so we can surface an immediate
    // error event if the chain is unknown, matching /query?live=true behaviour.
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

        // Initial snapshot: newest N blocks, same shape as the non-live response.
        let snapshot = {
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
            match conn.query(LIST_SQL, &[&DEFAULT_LIMIT]).await {
                Ok(rows) => {
                    let blocks: Vec<Block> = rows.iter().map(row_to_block).collect();
                    let count = blocks.len();
                    let query_time_ms = start.elapsed().as_secs_f64() * 1000.0;
                    // Live snapshots omit pagination; the stream delivers
                    // subsequent blocks push-style, not via cursor.
                    BlocksResponse { ok: true, blocks, count, next_cursor: None, query_time_ms }
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

        let mut last_seen = snapshot.blocks.first().map(|b| b.number).unwrap_or(0);

        yield Ok(SseEvent::default()
            .event("result")
            .json_data(snapshot)
            .unwrap());

        loop {
            match rx.recv().await {
                Ok(update) => {
                    if update.chain_id != chain_id {
                        continue;
                    }
                    let num = update.block_num as i64;
                    if num <= last_seen {
                        continue;
                    }
                    last_seen = num;

                    let start = Instant::now();
                    let block = Block {
                        number: num,
                        hash: update.block_hash,
                        timestamp: update.timestamp,
                        transaction_count: update.tx_count as i64,
                    };
                    let resp = BlocksResponse {
                        ok: true,
                        blocks: vec![block],
                        count: 1,
                        next_cursor: None,
                        query_time_ms: start.elapsed().as_secs_f64() * 1000.0,
                    };
                    yield Ok(SseEvent::default()
                        .event("result")
                        .json_data(resp)
                        .unwrap());
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
// Single-block detail endpoint: GET /blocks/:identifier
//
// Axum can't register `/blocks/:hash` and `/blocks/:number` as separate routes
// (both match any path segment), so we take one free-form `{identifier}` and
// branch on its shape: 0x-prefixed 64-hex → hash lookup, otherwise decimal →
// number lookup.

#[derive(Serialize)]
pub struct BlockDetail {
    number: i64,
    hash: String,
    parent_hash: String,
    timestamp: String,
    timestamp_ms: i64,
    gas_limit: i64,
    gas_used: i64,
    miner: String,
    extra_data: Option<String>,
    tx_count: i64,
}

#[derive(Serialize)]
pub struct BlockResponse {
    ok: bool,
    block: BlockDetail,
    query_time_ms: f64,
}

#[derive(Deserialize)]
pub struct BlockDetailParams {
    #[serde(alias = "chain_id", rename = "chainId")]
    chain_id: u64,
}

/// `GET /blocks/:identifier?chainId=X` — `identifier` is a decimal block number or a
/// `0x`-prefixed 32-byte block hash.
pub async fn get_block(
    State(state): State<AppState>,
    Path(identifier): Path<String>,
    Query(params): Query<BlockDetailParams>,
) -> Result<Json<BlockResponse>, ApiError> {
    let lookup = parse_identifier(&identifier)?;

    let pool = state
        .get_pool(Some(params.chain_id))
        .await
        .ok_or_else(|| ApiError::BadRequest(format!("Unknown chainId: {}", params.chain_id)))?;

    let conn = pool
        .get()
        .await
        .map_err(|e| ApiError::Internal(format!("Pool error: {e}")))?;

    let start = Instant::now();

    const SELECT_COLS: &str = "num, hash, parent_hash, timestamp, timestamp_ms, \
                               gas_limit, gas_used, miner, extra_data";

    let row_opt = match &lookup {
        BlockLookup::Number(n) => {
            conn.query_opt(
                &format!("SELECT {SELECT_COLS} FROM blocks WHERE num = $1"),
                &[n],
            )
            .await
        }
        BlockLookup::Hash(h) => {
            conn.query_opt(
                &format!("SELECT {SELECT_COLS} FROM blocks WHERE hash = $1"),
                &[h],
            )
            .await
        }
    }
    .map_err(|e| ApiError::QueryError(e.to_string()))?;

    let row = row_opt
        .ok_or_else(|| ApiError::NotFound(format!("Block not found: {identifier}")))?;

    let num: i64 = row.get(0);
    let hash: Vec<u8> = row.get(1);
    let parent_hash: Vec<u8> = row.get(2);
    let timestamp: DateTime<Utc> = row.get(3);
    let timestamp_ms: i64 = row.get(4);
    let gas_limit: i64 = row.get(5);
    let gas_used: i64 = row.get(6);
    let miner: Vec<u8> = row.get(7);
    let extra_data: Option<Vec<u8>> = row.get(8);

    let tx_count: i64 = conn
        .query_one("SELECT COUNT(*) FROM txs WHERE block_num = $1", &[&num])
        .await
        .map_err(|e| ApiError::QueryError(e.to_string()))?
        .get(0);

    let query_time_ms = start.elapsed().as_secs_f64() * 1000.0;

    Ok(Json(BlockResponse {
        ok: true,
        block: BlockDetail {
            number: num,
            hash: hex_prefixed(&hash),
            parent_hash: hex_prefixed(&parent_hash),
            timestamp: timestamp.to_rfc3339(),
            timestamp_ms,
            gas_limit,
            gas_used,
            miner: hex_prefixed(&miner),
            extra_data: extra_data.as_deref().map(hex_prefixed),
            tx_count,
        },
        query_time_ms,
    }))
}

enum BlockLookup {
    Number(i64),
    Hash(Vec<u8>),
}

fn parse_identifier(id: &str) -> Result<BlockLookup, ApiError> {
    if let Some(hex) = id.strip_prefix("0x").or_else(|| id.strip_prefix("0X")) {
        if hex.len() != 64 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(ApiError::BadRequest(
                "Block hash must be 0x + 64 hex characters".to_string(),
            ));
        }
        let bytes = hex::decode(hex)
            .map_err(|e| ApiError::BadRequest(format!("Invalid hex in block hash: {e}")))?;
        return Ok(BlockLookup::Hash(bytes));
    }

    let n: i64 = id.parse().map_err(|_| {
        ApiError::BadRequest(format!(
            "Block identifier must be a non-negative block number or a 0x-prefixed 32-byte hash: {id}"
        ))
    })?;

    if n < 0 {
        return Err(ApiError::BadRequest(
            "Block number must be non-negative".to_string(),
        ));
    }

    Ok(BlockLookup::Number(n))
}

fn hex_prefixed(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_identifier_number() {
        match parse_identifier("12345").unwrap() {
            BlockLookup::Number(n) => assert_eq!(n, 12345),
            _ => panic!("expected number"),
        }
    }

    #[test]
    fn parse_identifier_zero() {
        match parse_identifier("0").unwrap() {
            BlockLookup::Number(n) => assert_eq!(n, 0),
            _ => panic!("expected number"),
        }
    }

    #[test]
    fn parse_identifier_hash_lower() {
        let hash = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        match parse_identifier(hash).unwrap() {
            BlockLookup::Hash(bytes) => assert_eq!(bytes.len(), 32),
            _ => panic!("expected hash"),
        }
    }

    #[test]
    fn parse_identifier_hash_mixed_case() {
        let hash = "0xABCDef1234567890ABCDef1234567890ABCDef1234567890ABCDef1234567890";
        assert!(matches!(
            parse_identifier(hash).unwrap(),
            BlockLookup::Hash(_)
        ));
    }

    #[test]
    fn parse_identifier_rejects_short_hash() {
        assert!(parse_identifier("0xdeadbeef").is_err());
    }

    #[test]
    fn parse_identifier_rejects_negative() {
        assert!(parse_identifier("-1").is_err());
    }

    #[test]
    fn parse_identifier_rejects_garbage() {
        assert!(parse_identifier("latest").is_err());
        assert!(parse_identifier("0xzzzz").is_err());
    }
}
