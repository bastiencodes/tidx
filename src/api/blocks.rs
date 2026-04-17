//! Latest-blocks endpoint.
//!
//! `GET /blocks?chainId=X`                  — newest [`LIST_LIMIT`] blocks from the
//!                                            `blocks` table.
//! `GET /blocks?chainId=X&live=true`       — SSE stream: initial snapshot, then one
//!                                            event per newly indexed block. Mirrors
//!                                            the `/query?live=true` framing
//!                                            (`event: result` / `lagged` / `error`).

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
const LIST_LIMIT: i64 = 20;

#[derive(Deserialize)]
pub struct BlocksParams {
    #[serde(alias = "chain_id", rename = "chainId")]
    chain_id: u64,
    #[serde(default)]
    live: bool,
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

    let start = Instant::now();
    let rows = conn
        .query(LIST_SQL, &[&LIST_LIMIT])
        .await
        .map_err(|e| ApiError::QueryError(e.to_string()))?;

    let blocks: Vec<Block> = rows.iter().map(row_to_block).collect();
    let count = blocks.len();
    let query_time_ms = start.elapsed().as_secs_f64() * 1000.0;

    Ok(Json(BlocksResponse { ok: true, blocks, count, query_time_ms }))
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
            match conn.query(LIST_SQL, &[&LIST_LIMIT]).await {
                Ok(rows) => {
                    let blocks: Vec<Block> = rows.iter().map(row_to_block).collect();
                    let count = blocks.len();
                    let query_time_ms = start.elapsed().as_secs_f64() * 1000.0;
                    BlocksResponse { ok: true, blocks, count, query_time_ms }
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
