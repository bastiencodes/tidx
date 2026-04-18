//! Transactions endpoints.
//!
//! All list paths accept keyset pagination via `limit` (clamped to
//! [`pagination::MAX_LIMIT`], defaulting to [`pagination::DEFAULT_LIMIT`]) and
//! an opaque `cursor` returned in the response as `next_cursor`. The cursor
//! shape is path-specific but always opaque to clients.
//!
//! `GET /transactions?chainId=X`                 — newest transactions joined
//!                                                 with their receipts.
//! `GET /transactions?chainId=X&block=N`         — transactions for block `N`,
//!                                                 ordered by index.
//! `GET /transactions?chainId=X&address=0x...`   — transactions where `address`
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
//!                                                 with `block=`, `address=`,
//!                                                 or pagination params.
//!                                                 Accepts `&decode=true` and
//!                                                 `&labels=true` to populate
//!                                                 `decoded` / `labels` on each
//!                                                 streamed tx (same semantics
//!                                                 as the non-live path).
//! `GET /transactions/:hash?chainId=X`           — single transaction by
//!                                                 0x-prefixed 32-byte hash,
//!                                                 joined with its receipt.
//!                                                 Accepts `&include_logs=true`
//!                                                 to also fetch emitted logs
//!                                                 ordered by `log_idx`.
//!                                                 When combined with
//!                                                 `&decode=true`, each log's
//!                                                 topic0 is resolved against
//!                                                 the `signatures` cache and
//!                                                 best-effort decoded. When
//!                                                 combined with `&labels=true`,
//!                                                 each log's emitting
//!                                                 `address` is resolved against
//!                                                 the `labels_*` tables.

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
use chrono::{DateTime, TimeZone, Utc};
use futures::Stream;
use serde::{Deserialize, Serialize};

use crate::api::pagination::{self, DEFAULT_LIMIT, MAX_LIMIT};
use crate::api::{ApiError, AppState};

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
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    cursor: Option<String>,
    /// Populate `decoded` on each tx using the `signatures` cache.
    #[serde(default)]
    decode: bool,
    /// Populate `labels` on each tx using the `labels_*` tables.
    #[serde(default)]
    labels: bool,
}

/// Cursor for the LATEST path (`ORDER BY block_num DESC, idx DESC`).
#[derive(Serialize, Deserialize)]
struct LatestCursor {
    b: i64,
    i: i32,
}

/// Cursor for the BY_BLOCK path (`ORDER BY idx ASC`). Block is fixed so
/// only `idx` is needed.
#[derive(Serialize, Deserialize)]
struct BlockCursor {
    i: i32,
}

/// Cursor for the BY_ADDRESS path. We include `ts_ms` so the inner per-leg
/// scan (indexed on `("from", block_timestamp DESC)`) can range-scan the
/// index; `(b, i)` break ties within the same block timestamp.
#[derive(Serialize, Deserialize)]
struct AddressCursor {
    ts_ms: i64,
    b: i64,
    i: i32,
}

#[derive(Serialize)]
pub struct Transaction {
    hash: String,
    block_number: i64,
    /// RFC3339 timestamp.
    block_timestamp: String,
    block_timestamp_ms: i64,
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
    /// Present only when the caller set `?decode=true`. Inner `None` means
    /// selector not in cache or args failed to parse; `Some` is the decoded
    /// `{name, signature, inputs[]}`.
    #[serde(skip_serializing_if = "Option::is_none")]
    decoded: Option<Option<crate::decoder::Decoded>>,
    /// Present only when the caller set `?labels=true`. Keys (`from`, `to`,
    /// `contract_address`) are omitted when no label was found. Values are
    /// arrays because one address commonly carries multiple tags (e.g.
    /// `["uniswap", "dex"]` or `["tornado-cash", "blocked"]`).
    #[serde(skip_serializing_if = "Option::is_none")]
    labels: Option<std::collections::BTreeMap<&'static str, Vec<crate::labels::Label>>>,
    /// Present only when the caller set `?include_logs=true` on the detail
    /// endpoint. Ordered by `log_idx` ascending. Empty vec means the tx
    /// emitted no logs.
    #[serde(skip_serializing_if = "Option::is_none")]
    logs: Option<Vec<Log>>,
}

#[derive(Serialize)]
pub struct Log {
    log_index: i32,
    address: String,
    /// Non-null prefix of (topic0..topic3), hex-encoded. EVM logs have
    /// 0-4 topics; the schema stores them as independent nullable columns.
    topics: Vec<String>,
    data: String,
    /// Present only when the caller set both `?decode=true` and
    /// `?include_logs=true`. Inner `None` means topic0 was not found in
    /// the signatures cache; `Some` is the decoded `{name, signature,
    /// inputs[]}` (may have empty `inputs` if arg decoding failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    decoded: Option<Option<crate::decoder::Decoded>>,
    /// Present only when the caller set both `?labels=true` and
    /// `?include_logs=true`. Empty vec means the log's emitting address
    /// had no label; otherwise one entry per tag (e.g. `["uniswap", "dex"]`).
    #[serde(skip_serializing_if = "Option::is_none")]
    labels: Option<Vec<crate::labels::Label>>,
}

#[derive(Serialize)]
pub struct TransactionsResponse {
    ok: bool,
    transactions: Vec<Transaction>,
    count: usize,
    /// Opaque cursor for the next page, or `null` on the last page.
    next_cursor: Option<String>,
    query_time_ms: f64,
}

/// `GET /transactions?chainId=X[&block=N][&address=0x...][&live=true][&limit=N][&cursor=...]`
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
        if params.cursor.is_some() {
            return ApiError::BadRequest(
                "cursor is not supported with live=true".to_string(),
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
    let limit = pagination::clamp_limit(params.limit, DEFAULT_LIMIT, MAX_LIMIT);

    let start = Instant::now();
    let (rows, variant) = match (address_bytes, block_num) {
        (Some(addr), Some(bn)) => {
            // BY_ADDRESS_AND_BLOCK — outer sort is `idx ASC`, cursor is `{i}`.
            let cursor_i: i32 = params
                .cursor
                .as_deref()
                .map(pagination::decode::<BlockCursor>)
                .transpose()?
                .map_or(-1, |c| c.i);
            let rows = conn
                .query(&by_address_and_block_sql(), &[&addr, &bn, &cursor_i, &limit])
                .await
                .map_err(|e| ApiError::QueryError(e.to_string()))?;
            (rows, CursorVariant::BlockIdx)
        }
        (Some(addr), None) => {
            // BY_ADDRESS — outer sort is `(block_num, idx) DESC`, cursor is
            // `{ts_ms, b, i}` so the inner per-leg scan can seek by index.
            let rows = match params
                .cursor
                .as_deref()
                .map(pagination::decode::<AddressCursor>)
                .transpose()?
            {
                Some(c) => {
                    let ts = Utc
                        .timestamp_millis_opt(c.ts_ms)
                        .single()
                        .ok_or_else(|| {
                            ApiError::BadRequest("invalid cursor: timestamp out of range".into())
                        })?;
                    conn.query(&by_address_after_sql(), &[&addr, &ts, &c.b, &c.i, &limit])
                        .await
                        .map_err(|e| ApiError::QueryError(e.to_string()))?
                }
                None => conn
                    .query(&by_address_sql(), &[&addr, &limit])
                    .await
                    .map_err(|e| ApiError::QueryError(e.to_string()))?,
            };
            (rows, CursorVariant::Address)
        }
        (None, Some(bn)) => {
            // BY_BLOCK — outer sort is `idx ASC`, cursor is `{i}`.
            let cursor_i: i32 = params
                .cursor
                .as_deref()
                .map(pagination::decode::<BlockCursor>)
                .transpose()?
                .map_or(-1, |c| c.i);
            let rows = conn
                .query(&by_block_sql(), &[&bn, &cursor_i, &limit])
                .await
                .map_err(|e| ApiError::QueryError(e.to_string()))?;
            (rows, CursorVariant::BlockIdx)
        }
        (None, None) => {
            // LATEST — outer sort is `(block_num, idx) DESC`, cursor is `{b, i}`.
            let rows = match params
                .cursor
                .as_deref()
                .map(pagination::decode::<LatestCursor>)
                .transpose()?
            {
                Some(c) => conn
                    .query(&latest_after_sql(), &[&c.b, &c.i, &limit])
                    .await
                    .map_err(|e| ApiError::QueryError(e.to_string()))?,
                None => conn
                    .query(&latest_sql(), &[&limit])
                    .await
                    .map_err(|e| ApiError::QueryError(e.to_string()))?,
            };
            (rows, CursorVariant::Latest)
        }
    };

    let mut transactions: Vec<Transaction> = rows.iter().map(row_to_tx).collect();
    if params.decode {
        let raw_inputs: Vec<Vec<u8>> = rows.iter().map(|r| r.get::<_, Vec<u8>>(9)).collect();
        let input_refs: Vec<&[u8]> = raw_inputs.iter().map(|v| v.as_slice()).collect();
        let decoded = crate::decoder::decode_calldata_batch(&conn, &input_refs).await;
        for (tx, d) in transactions.iter_mut().zip(decoded) {
            tx.decoded = Some(d);
        }
    }
    if params.labels {
        attach_labels(&pool, &rows, &mut transactions).await;
    }
    let count = transactions.len();
    let next_cursor = next_cursor_for(&transactions, limit, variant);
    let query_time_ms = start.elapsed().as_secs_f64() * 1000.0;

    Ok(Json(TransactionsResponse { ok: true, transactions, count, next_cursor, query_time_ms }))
}

/// Which cursor shape this query path emits when it fills a page.
#[derive(Clone, Copy)]
enum CursorVariant {
    Latest,
    BlockIdx,
    Address,
}

fn next_cursor_for(txs: &[Transaction], limit: i64, variant: CursorVariant) -> Option<String> {
    if (txs.len() as i64) < limit {
        return None;
    }
    let last = txs.last()?;
    Some(match variant {
        CursorVariant::Latest => pagination::encode(&LatestCursor {
            b: last.block_number,
            i: last.transaction_index,
        }),
        CursorVariant::BlockIdx => pagination::encode(&BlockCursor { i: last.transaction_index }),
        CursorVariant::Address => pagination::encode(&AddressCursor {
            ts_ms: last.block_timestamp_ms,
            b: last.block_number,
            i: last.transaction_index,
        }),
    })
}

/// Column list shared by all transaction queries. Must match `row_to_tx` positions.
const TX_COLS: &str = r#"
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
"#;

fn latest_sql() -> String {
    format!(
        r#"
    SELECT {TX_COLS}
    FROM txs t
    LEFT JOIN receipts r
      ON r.tx_hash = t.hash
     AND r.block_num = t.block_num
    ORDER BY t.block_num DESC, t.idx DESC
    LIMIT $1
"#
    )
}

/// Cursor variant of LATEST: `(block_num, idx) < ($1, $2)` pushes the keyset
/// filter down into `idx_txs_block_num`'s range scan.
fn latest_after_sql() -> String {
    format!(
        r#"
    SELECT {TX_COLS}
    FROM txs t
    LEFT JOIN receipts r
      ON r.tx_hash = t.hash
     AND r.block_num = t.block_num
    WHERE (t.block_num, t.idx) < ($1, $2)
    ORDER BY t.block_num DESC, t.idx DESC
    LIMIT $3
"#
    )
}

/// BY_BLOCK paginated: `idx > $2` handles keyset pagination (cursor defaults
/// to -1 on first page so `idx > -1` admits every row in the block).
fn by_block_sql() -> String {
    format!(
        r#"
    SELECT {TX_COLS}
    FROM txs t
    LEFT JOIN receipts r
      ON r.tx_hash = t.hash
     AND r.block_num = t.block_num
    WHERE t.block_num = $1 AND t.idx > $2
    ORDER BY t.idx ASC
    LIMIT $3
"#
    )
}

/// Unbounded BY_BLOCK used by the live SSE push: each per-block event
/// delivers *all* txs in that block, not a page.
fn by_block_all_sql() -> String {
    format!(
        r#"
    SELECT {TX_COLS}
    FROM txs t
    LEFT JOIN receipts r
      ON r.tx_hash = t.hash
     AND r.block_num = t.block_num
    WHERE t.block_num = $1
    ORDER BY t.idx ASC
"#
    )
}

// UNION ALL over the two indexed legs (`idx_txs_from`, `idx_txs_to`) rather
// than `WHERE "from" = $1 OR "to" = $1`, since Postgres typically won't combine
// two separate indexes for an OR on different columns. Each leg is capped at
// $2 so the outer ORDER BY never scans more than 2 * LIMIT rows.
fn by_address_sql() -> String {
    format!(
        r#"
    SELECT {TX_COLS}
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
"#
    )
}

/// Cursor variant of BY_ADDRESS. Inner predicate
/// `(block_timestamp, block_num, idx) < ...` lets the index scan seek past
/// already-returned rows; tuple comparison handles within-block ties (all
/// rows in a block share block_timestamp).
fn by_address_after_sql() -> String {
    format!(
        r#"
    SELECT {TX_COLS}
    FROM (
        (SELECT * FROM txs
           WHERE "from" = $1
             AND (block_timestamp, block_num, idx) < ($2, $3, $4)
           ORDER BY block_timestamp DESC LIMIT $5)
        UNION ALL
        (SELECT * FROM txs
           WHERE "to" = $1
             AND (block_timestamp, block_num, idx) < ($2, $3, $4)
           ORDER BY block_timestamp DESC LIMIT $5)
    ) t
    LEFT JOIN receipts r
      ON r.tx_hash = t.hash
     AND r.block_num = t.block_num
    ORDER BY t.block_num DESC, t.idx DESC
    LIMIT $5
"#
    )
}

/// BY_ADDRESS_AND_BLOCK: block is fixed, so the inner legs have at most
/// ~O(txs per block per address) rows (tiny). We order by `idx ASC` directly
/// rather than `block_timestamp` (constant within a block) and apply the
/// cursor on `idx`.
fn by_address_and_block_sql() -> String {
    format!(
        r#"
    SELECT {TX_COLS}
    FROM (
        (SELECT * FROM txs
           WHERE "from" = $1 AND block_num = $2 AND idx > $3
           ORDER BY idx ASC LIMIT $4)
        UNION ALL
        (SELECT * FROM txs
           WHERE "to" = $1 AND block_num = $2 AND idx > $3
           ORDER BY idx ASC LIMIT $4)
    ) t
    LEFT JOIN receipts r
      ON r.tx_hash = t.hash
     AND r.block_num = t.block_num
    ORDER BY t.idx ASC
    LIMIT $4
"#
    )
}

fn by_hash_sql() -> String {
    format!(
        r#"
    SELECT {TX_COLS}
    FROM txs t
    LEFT JOIN receipts r
      ON r.tx_hash = t.hash
     AND r.block_num = t.block_num
    WHERE t.hash = $1
    LIMIT 1
"#
    )
}

fn row_to_tx(row: &tokio_postgres::Row) -> Transaction {
    let hash: Vec<u8> = row.get(0);
    let block_timestamp: DateTime<Utc> = row.get(2);
    let from: Vec<u8> = row.get(4);
    let to: Option<Vec<u8>> = row.get(5);
    let input: Vec<u8> = row.get(9);
    let contract_address: Option<Vec<u8>> = row.get(16);

    Transaction {
        hash: hex_prefixed(&hash),
        block_number: row.get(1),
        block_timestamp: block_timestamp.to_rfc3339(),
        block_timestamp_ms: block_timestamp.timestamp_millis(),
        transaction_index: row.get(3),
        from: hex_prefixed(&from),
        to: to.as_deref().map(hex_prefixed),
        value: row.get(6),
        nonce: row.get(7),
        tx_type: row.get(8),
        input: hex_prefixed(&input),
        gas_limit: row.get(10),
        max_fee_per_gas: row.get(11),
        max_priority_fee_per_gas: row.get(12),
        gas_used: row.get(13),
        cumulative_gas_used: row.get(14),
        effective_gas_price: row.get(15),
        contract_address: contract_address.as_deref().map(hex_prefixed),
        status: row.get(17),
        decoded: None,
        labels: None,
        logs: None,
    }
}

/// Collect the unique (from/to/contract_address) addresses across `rows`,
/// call [`crate::labels::lookup_batch`] once, and populate each tx's
/// `labels` field. Rows with no labels get an empty BTreeMap so the
/// `labels: {}` key is still present in the response.
async fn attach_labels(
    pool: &crate::db::Pool,
    rows: &[tokio_postgres::Row],
    txs: &mut [Transaction],
) {
    let mut addrs: Vec<[u8; 20]> = Vec::with_capacity(rows.len() * 3);
    for row in rows {
        let from: Vec<u8> = row.get(4);
        if let Ok(a) = <[u8; 20]>::try_from(from.as_slice()) {
            addrs.push(a);
        }
        let to: Option<Vec<u8>> = row.get(5);
        if let Some(t) = to {
            if let Ok(a) = <[u8; 20]>::try_from(t.as_slice()) {
                addrs.push(a);
            }
        }
        let contract: Option<Vec<u8>> = row.get(16);
        if let Some(c) = contract {
            if let Ok(a) = <[u8; 20]>::try_from(c.as_slice()) {
                addrs.push(a);
            }
        }
    }

    let map = crate::labels::lookup_batch(pool, &addrs).await;

    for (row, tx) in rows.iter().zip(txs.iter_mut()) {
        let mut labels: std::collections::BTreeMap<&'static str, Vec<crate::labels::Label>> =
            std::collections::BTreeMap::new();
        let from: Vec<u8> = row.get(4);
        if let Ok(a) = <[u8; 20]>::try_from(from.as_slice()) {
            if let Some(ls) = map.get(&a) {
                labels.insert("from", ls.clone());
            }
        }
        let to: Option<Vec<u8>> = row.get(5);
        if let Some(t) = to {
            if let Ok(a) = <[u8; 20]>::try_from(t.as_slice()) {
                if let Some(ls) = map.get(&a) {
                    labels.insert("to", ls.clone());
                }
            }
        }
        let contract: Option<Vec<u8>> = row.get(16);
        if let Some(c) = contract {
            if let Ok(a) = <[u8; 20]>::try_from(c.as_slice()) {
                if let Some(ls) = map.get(&a) {
                    labels.insert("contract_address", ls.clone());
                }
            }
        }
        tx.labels = Some(labels);
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
            match conn.query(&latest_sql(), &[&DEFAULT_LIMIT]).await {
                Ok(rows) => {
                    let mut transactions: Vec<Transaction> = rows.iter().map(row_to_tx).collect();
                    if params.decode {
                        let raw_inputs: Vec<Vec<u8>> = rows.iter().map(|r| r.get::<_, Vec<u8>>(9)).collect();
                        let input_refs: Vec<&[u8]> = raw_inputs.iter().map(|v| v.as_slice()).collect();
                        let decoded = crate::decoder::decode_calldata_batch(&conn, &input_refs).await;
                        for (tx, d) in transactions.iter_mut().zip(decoded) {
                            tx.decoded = Some(d);
                        }
                    }
                    if params.labels {
                        attach_labels(&pool, &rows, &mut transactions).await;
                    }
                    let latest = transactions.first().map(|t| t.block_number).unwrap_or(0);
                    let count = transactions.len();
                    let query_time_ms = start.elapsed().as_secs_f64() * 1000.0;
                    // Live snapshots omit pagination; the SSE stream itself
                    // is the forward traversal mechanism.
                    let resp = TransactionsResponse {
                        ok: true, transactions, count, next_cursor: None, query_time_ms,
                    };
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
                        match conn.query(&by_block_all_sql(), &[&block_num]).await {
                            Ok(rows) => {
                                let mut transactions: Vec<Transaction> = rows.iter().map(row_to_tx).collect();
                                if params.decode {
                                    let raw_inputs: Vec<Vec<u8>> = rows.iter().map(|r| r.get::<_, Vec<u8>>(9)).collect();
                                    let input_refs: Vec<&[u8]> = raw_inputs.iter().map(|v| v.as_slice()).collect();
                                    let decoded = crate::decoder::decode_calldata_batch(&conn, &input_refs).await;
                                    for (tx, d) in transactions.iter_mut().zip(decoded) {
                                        tx.decoded = Some(d);
                                    }
                                }
                                if params.labels {
                                    attach_labels(&pool, &rows, &mut transactions).await;
                                }
                                let count = transactions.len();
                                let resp = TransactionsResponse {
                                    ok: true,
                                    transactions,
                                    count,
                                    next_cursor: None,
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
pub struct TransactionResponse {
    ok: bool,
    transaction: Transaction,
    query_time_ms: f64,
}

#[derive(Deserialize)]
pub struct TransactionDetailParams {
    #[serde(alias = "chain_id", rename = "chainId")]
    chain_id: u64,
    /// Populate `decoded` on the tx using the `signatures` cache.
    #[serde(default)]
    decode: bool,
    /// Populate `labels` on the tx using the `labels_*` tables.
    #[serde(default)]
    labels: bool,
    /// Populate `logs` on the tx by fetching from the `logs` table.
    #[serde(default, alias = "includeLogs")]
    include_logs: bool,
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
    let row = conn
        .query_opt(&by_hash_sql(), &[&hash_bytes])
        .await
        .map_err(|e| ApiError::QueryError(e.to_string()))?
        .ok_or_else(|| ApiError::NotFound(format!("Transaction not found: {hash}")))?;

    let query_time_ms = start.elapsed().as_secs_f64() * 1000.0;

    let mut transaction = row_to_tx(&row);
    if params.decode {
        let raw_input: Vec<u8> = row.get(9);
        let decoded = crate::decoder::decode_calldata_batch(&conn, &[raw_input.as_slice()])
            .await
            .into_iter()
            .next()
            .flatten();
        transaction.decoded = Some(decoded);
    }
    if params.labels {
        attach_labels(&pool, std::slice::from_ref(&row), std::slice::from_mut(&mut transaction)).await;
    }
    if params.include_logs {
        let log_rows = conn
            .query(LOGS_BY_TX_HASH_SQL, &[&hash_bytes])
            .await
            .map_err(|e| ApiError::QueryError(e.to_string()))?;
        let mut logs: Vec<Log> = log_rows.iter().map(log_row_to_log).collect();

        if params.decode && !logs.is_empty() {
            let events: Vec<crate::decoder::EventInput> = log_rows
                .iter()
                .map(|r| crate::decoder::EventInput {
                    topics: [
                        r.get::<_, Option<Vec<u8>>>(2),
                        r.get(3),
                        r.get(4),
                        r.get(5),
                    ]
                    .into_iter()
                    .take_while(Option::is_some)
                    .flatten()
                    .collect(),
                    data: r.get(6),
                })
                .collect();
            let decoded = crate::decoder::decode_events_batch(&conn, &events).await;
            for (log, d) in logs.iter_mut().zip(decoded) {
                log.decoded = Some(d);
            }
        }

        if params.labels {
            let log_addrs: Vec<[u8; 20]> = log_rows
                .iter()
                .filter_map(|r| {
                    let a: Vec<u8> = r.get(1);
                    <[u8; 20]>::try_from(a.as_slice()).ok()
                })
                .collect();
            let map = crate::labels::lookup_batch(&pool, &log_addrs).await;
            for (log, row) in logs.iter_mut().zip(log_rows.iter()) {
                let a: Vec<u8> = row.get(1);
                let labels = <[u8; 20]>::try_from(a.as_slice())
                    .ok()
                    .and_then(|k| map.get(&k).cloned())
                    .unwrap_or_default();
                log.labels = Some(labels);
            }
        }

        transaction.logs = Some(logs);
    }

    Ok(Json(TransactionResponse {
        ok: true,
        transaction,
        query_time_ms,
    }))
}

const LOGS_BY_TX_HASH_SQL: &str = r#"
    SELECT log_idx, address, topic0, topic1, topic2, topic3, data
    FROM logs
    WHERE tx_hash = $1
    ORDER BY log_idx ASC
"#;

fn log_row_to_log(row: &tokio_postgres::Row) -> Log {
    let address: Vec<u8> = row.get(1);
    let topic0: Option<Vec<u8>> = row.get(2);
    let topic1: Option<Vec<u8>> = row.get(3);
    let topic2: Option<Vec<u8>> = row.get(4);
    let topic3: Option<Vec<u8>> = row.get(5);
    let data: Vec<u8> = row.get(6);

    let topics = [topic0, topic1, topic2, topic3]
        .into_iter()
        .take_while(Option::is_some)
        .flatten()
        .map(|t| hex_prefixed(&t))
        .collect();

    Log {
        log_index: row.get(0),
        address: hex_prefixed(&address),
        topics,
        data: hex_prefixed(&data),
        decoded: None,
        labels: None,
    }
}

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
