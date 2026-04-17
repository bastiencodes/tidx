//! Latest-transactions endpoint.
//!
//! `GET /transactions?chainId=X`                 — newest [`LIST_LIMIT`]
//!                                                 transactions joined with
//!                                                 their receipts.
//! `GET /transactions?chainId=X&live=true`       — SSE stream: initial
//!                                                 snapshot, then one event
//!                                                 per newly indexed block
//!                                                 containing that block's
//!                                                 transactions. Mirrors the
//!                                                 `/blocks?live=true` framing
//!                                                 (`event: result` / `lagged`
//!                                                 / `error`).

use std::convert::Infallible;
use std::time::Instant;

use axum::{
    extract::{Query, State},
    response::{
        sse::{Event as SseEvent, KeepAlive, KeepAliveStream},
        IntoResponse, Response, Sse,
    },
    Json,
};
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

/// `GET /transactions?chainId=X[&live=true]`
pub async fn list_transactions(
    State(state): State<AppState>,
    Query(params): Query<TransactionsParams>,
) -> Response {
    if params.live {
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

    let start = Instant::now();
    let rows = conn
        .query(LATEST_SQL, &[&LIST_LIMIT])
        .await
        .map_err(|e| ApiError::QueryError(e.to_string()))?;

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
