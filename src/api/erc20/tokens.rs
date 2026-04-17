//! ERC20 token list endpoint, backed by the `erc20_tokens` table.
//!
//! The table is populated by `src/sync/erc20_metadata.rs`; this module only
//! reads. Exposes `GET /erc20/tokens` which returns a page of discovered
//! tokens newest-first. Supports keyset pagination via `limit` (clamped to
//! [`pagination::MAX_LIMIT`], default [`pagination::DEFAULT_LIMIT`]) and an
//! opaque `cursor` returned as `next_cursor`.

use std::time::Instant;

use axum::{
    extract::{Query, State},
    Json,
};
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::api::pagination::{self, DEFAULT_LIMIT, MAX_LIMIT};
use crate::api::{ApiError, AppState};

/// Keyset cursor: `first_transfer_at` plus the contract address as a
/// tiebreaker, since multiple tokens can share a block timestamp.
#[derive(Serialize, Deserialize)]
struct TokensCursor {
    ts_ms: i64,
    /// Lowercase hex-encoded 20-byte address, no `0x` prefix.
    a: String,
}

#[derive(Deserialize)]
pub struct TokensParams {
    #[serde(alias = "chain_id", rename = "chainId")]
    chain_id: u64,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Serialize)]
pub struct Erc20Token {
    contract_address: String,
    name: Option<String>,
    symbol: Option<String>,
    decimals: Option<i16>,
    first_transfer_at: String,
    first_transfer_block: i64,
    deployed_at: Option<String>,
    deployed_block: Option<i64>,
    resolution_status: String,
    /// Block timestamp the metadata was read at (via
    /// Multicall3.getCurrentBlockTimestamp()). Null if never resolved or
    /// the block-timestamp sub-call reverted.
    resolved_at: Option<String>,
    /// Block number paired with `resolved_at` (via Multicall3.getBlockNumber()).
    resolved_block: Option<i64>,
}

#[derive(Serialize)]
pub struct Erc20TokensResponse {
    ok: bool,
    tokens: Vec<Erc20Token>,
    count: usize,
    total_count: i64,
    /// Opaque cursor for the next page, or `null` on the last page.
    next_cursor: Option<String>,
    query_time_ms: f64,
}

/// GET /erc20/tokens?chainId=X[&limit=N][&cursor=...]
///
/// Returns a page of discovered ERC20 tokens, newest `first_transfer_at`
/// first.
pub async fn list_tokens(
    State(state): State<AppState>,
    Query(params): Query<TokensParams>,
) -> Result<Json<Erc20TokensResponse>, ApiError> {
    let pool = state
        .get_pool(Some(params.chain_id))
        .await
        .ok_or_else(|| ApiError::BadRequest(format!("Unknown chainId: {}", params.chain_id)))?;
    let conn = pool
        .get()
        .await
        .map_err(|e| ApiError::Internal(format!("Pool error: {e}")))?;

    let limit = pagination::clamp_limit(params.limit, DEFAULT_LIMIT, MAX_LIMIT);
    let cursor: Option<TokensCursor> = params
        .cursor
        .as_deref()
        .map(pagination::decode)
        .transpose()?;

    let start = Instant::now();

    let rows = match cursor {
        Some(c) => {
            let ts = Utc
                .timestamp_millis_opt(c.ts_ms)
                .single()
                .ok_or_else(|| {
                    ApiError::BadRequest("invalid cursor: timestamp out of range".into())
                })?;
            let addr_bytes = hex::decode(&c.a)
                .map_err(|_| ApiError::BadRequest("invalid cursor: bad address hex".into()))?;
            conn.query(LIST_AFTER_SQL, &[&ts, &addr_bytes, &limit])
                .await
                .map_err(|e| ApiError::QueryError(e.to_string()))?
        }
        None => conn
            .query(LIST_SQL, &[&limit])
            .await
            .map_err(|e| ApiError::QueryError(e.to_string()))?,
    };

    // Total count is a small index-backed aggregate; fine to run per request.
    let total_count: i64 = conn
        .query_one("SELECT COUNT(*) FROM erc20_tokens", &[])
        .await
        .map_err(|e| ApiError::QueryError(e.to_string()))?
        .get(0);

    let tokens: Vec<Erc20Token> = rows.iter().map(row_to_token).collect();
    let count = tokens.len();
    let next_cursor = next_cursor_for(&rows, limit);
    let query_time_ms = start.elapsed().as_secs_f64() * 1000.0;

    Ok(Json(Erc20TokensResponse {
        ok: true,
        tokens,
        count,
        total_count,
        next_cursor,
        query_time_ms,
    }))
}

const LIST_SQL: &str = r#"
    SELECT address, name, symbol, decimals,
           first_transfer_at, first_transfer_block,
           deployed_at, deployed_block,
           resolution_status, resolved_at, resolved_block
    FROM erc20_tokens
    ORDER BY first_transfer_at DESC, address DESC
    LIMIT $1
"#;

// Tuple comparison lets the `(first_transfer_at DESC)` index range-scan past
// the cursor position; `address` resolves ties among tokens first seen in
// the same block.
const LIST_AFTER_SQL: &str = r#"
    SELECT address, name, symbol, decimals,
           first_transfer_at, first_transfer_block,
           deployed_at, deployed_block,
           resolution_status, resolved_at, resolved_block
    FROM erc20_tokens
    WHERE (first_transfer_at, address) < ($1, $2)
    ORDER BY first_transfer_at DESC, address DESC
    LIMIT $3
"#;

fn next_cursor_for(rows: &[tokio_postgres::Row], limit: i64) -> Option<String> {
    if (rows.len() as i64) < limit {
        return None;
    }
    let last = rows.last()?;
    let ts: DateTime<Utc> = last.get(4);
    let addr: Vec<u8> = last.get(0);
    Some(pagination::encode(&TokensCursor {
        ts_ms: ts.timestamp_millis(),
        a: hex::encode(&addr),
    }))
}

fn row_to_token(row: &tokio_postgres::Row) -> Erc20Token {
    let address: Vec<u8> = row.get(0);
    let first_transfer_at: DateTime<Utc> = row.get(4);
    let deployed_at: Option<DateTime<Utc>> = row.get(6);
    let resolved_at: Option<DateTime<Utc>> = row.get(9);
    Erc20Token {
        contract_address: format!("0x{}", hex::encode(&address)),
        name: row.get(1),
        symbol: row.get(2),
        decimals: row.get(3),
        first_transfer_at: first_transfer_at.to_rfc3339(),
        first_transfer_block: row.get(5),
        deployed_at: deployed_at.map(|d| d.to_rfc3339()),
        deployed_block: row.get(7),
        resolution_status: row.get(8),
        resolved_at: resolved_at.map(|d| d.to_rfc3339()),
        resolved_block: row.get(10),
    }
}
