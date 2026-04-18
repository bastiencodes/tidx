//! ERC20 token list endpoint, backed by the `erc20_tokens` table.
//!
//! The table is populated by `src/sync/erc20_metadata.rs`; this module only
//! reads. Exposes `GET /erc20/tokens` which returns a page of discovered
//! tokens newest-first. Supports keyset pagination via `limit` (clamped to
//! [`pagination::MAX_LIMIT`], default [`pagination::DEFAULT_LIMIT`]) and an
//! opaque `cursor` returned as `next_cursor`.
//!
//! Each row is enriched with Trust Wallet metadata via a LEFT JOIN on the
//! `tw_assets` table (populated by
//! `src/sync/trustwallet_metadata.rs`). Enrichment is purely additive —
//! on-chain `name`/`symbol`/`decimals` from Multicall3 stay source-of-truth;
//! Trust Wallet adds `logo_url` (computed), `website`, `description`,
//! `explorer`, `tags`, `links`, and `trust_wallet_status`.

use std::time::Instant;

use alloy::primitives::Address;
use axum::{
    extract::{Query, State},
    Json,
};
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::api::pagination::{self, DEFAULT_LIMIT, MAX_LIMIT};
use crate::api::{ApiError, AppState};
use crate::sync::trustwallet_metadata;

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

    // ── Trust Wallet enrichment (null when the token isn't listed) ──────
    /// Canonical `logo.png` URL on trustwallet/assets. Computed from the
    /// chain slug and EIP-55 checksummed address; only emitted when a
    /// `tw_assets` row exists for this token.
    #[serde(skip_serializing_if = "Option::is_none")]
    logo_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    website: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    explorer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tags: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    links: Option<serde_json::Value>,
    /// Trust Wallet's `status` field: `active` | `spam` | `abandoned`.
    /// Sets up future filters like `?exclude_spam=true` without needing
    /// a follow-up API shape change.
    #[serde(skip_serializing_if = "Option::is_none")]
    trust_wallet_status: Option<String>,
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

    let tw_slug = trustwallet_metadata::slug_for(params.chain_id);
    let chain_id_i64 = params.chain_id as i64;

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
            conn.query(LIST_AFTER_SQL, &[&chain_id_i64, &ts, &addr_bytes, &limit])
                .await
                .map_err(|e| ApiError::QueryError(e.to_string()))?
        }
        None => conn
            .query(LIST_SQL, &[&chain_id_i64, &limit])
            .await
            .map_err(|e| ApiError::QueryError(e.to_string()))?,
    };

    // Total count is a small index-backed aggregate; fine to run per request.
    let total_count: i64 = conn
        .query_one("SELECT COUNT(*) FROM erc20_tokens", &[])
        .await
        .map_err(|e| ApiError::QueryError(e.to_string()))?
        .get(0);

    let tokens: Vec<Erc20Token> = rows.iter().map(|r| row_to_token(r, tw_slug)).collect();
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

// LEFT JOINs `tw_assets` by the chain_id bound parameter so the
// join predicate is sargable on the composite PK. `twa.info_sha` doubles
// as the "row exists" sentinel because it's the only NOT NULL TW column
// aside from the key.
const LIST_SQL: &str = r#"
    SELECT t.address, t.name, t.symbol, t.decimals,
           t.first_transfer_at, t.first_transfer_block,
           t.deployed_at, t.deployed_block,
           t.resolution_status, t.resolved_at, t.resolved_block,
           twa.info_sha,
           twa.website, twa.description, twa.explorer,
           twa.status, twa.tags, twa.links
    FROM erc20_tokens t
    LEFT JOIN tw_assets twa
      ON twa.chain_id = $1 AND twa.address = t.address
    ORDER BY t.first_transfer_at DESC, t.address DESC
    LIMIT $2
"#;

// Tuple comparison lets the `(first_transfer_at DESC)` index range-scan past
// the cursor position; `address` resolves ties among tokens first seen in
// the same block.
const LIST_AFTER_SQL: &str = r#"
    SELECT t.address, t.name, t.symbol, t.decimals,
           t.first_transfer_at, t.first_transfer_block,
           t.deployed_at, t.deployed_block,
           t.resolution_status, t.resolved_at, t.resolved_block,
           twa.info_sha,
           twa.website, twa.description, twa.explorer,
           twa.status, twa.tags, twa.links
    FROM erc20_tokens t
    LEFT JOIN tw_assets twa
      ON twa.chain_id = $1 AND twa.address = t.address
    WHERE (t.first_transfer_at, t.address) < ($2, $3)
    ORDER BY t.first_transfer_at DESC, t.address DESC
    LIMIT $4
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

fn row_to_token(row: &tokio_postgres::Row, tw_slug: Option<&str>) -> Erc20Token {
    let address_bytes: Vec<u8> = row.get(0);
    let first_transfer_at: DateTime<Utc> = row.get(4);
    let deployed_at: Option<DateTime<Utc>> = row.get(6);
    let resolved_at: Option<DateTime<Utc>> = row.get(9);

    // `info_sha` being non-NULL is the "Trust Wallet lists this token"
    // signal from the LEFT JOIN. The logo URL is computed here rather
    // than stored because it's deterministic from (slug, EIP-55 addr).
    let info_sha: Option<String> = row.get(11);
    let listed = info_sha.is_some();
    let logo_url = match (listed, tw_slug, address_bytes.len()) {
        (true, Some(slug), 20) => {
            let mut arr = [0u8; 20];
            arr.copy_from_slice(&address_bytes);
            Some(trustwallet_metadata::logo_url(slug, &Address::from(arr)))
        }
        _ => None,
    };

    Erc20Token {
        contract_address: format!("0x{}", hex::encode(&address_bytes)),
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

        logo_url,
        website: row.get(12),
        description: row.get(13),
        explorer: row.get(14),
        trust_wallet_status: row.get(15),
        tags: row.get(16),
        links: row.get(17),
    }
}
