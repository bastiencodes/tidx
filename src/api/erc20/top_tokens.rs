//! Top ERC20 tokens by Transfer event count.
//!
//! Ranking runs on ClickHouse against the raw `logs` table; ERC721 is
//! excluded via `topic3 IS NULL` (ERC721's Transfer indexes `tokenId` as
//! topic3, ERC20's has `value` in `data`). Metadata (name/symbol/decimals)
//! is a second hop to Postgres `erc20_tokens`, since ClickHouse only
//! stores `logs`/`txs`/`receipts`.

use std::time::Instant;

use axum::{
    extract::{Query, State},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::api::pagination::{self, DEFAULT_LIMIT, MAX_LIMIT};
use crate::api::{ApiError, AppState};

/// ERC20 Transfer(address,address,uint256) topic0.
const TRANSFER_SELECTOR: &str = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

#[derive(Deserialize)]
pub struct TopTokensParams {
    #[serde(alias = "chain_id", rename = "chainId")]
    chain_id: u64,
    /// One of "24h", "7d", "30d", "all". Defaults to "24h".
    #[serde(default)]
    window: Option<String>,
    #[serde(default)]
    limit: Option<i64>,
}

#[derive(Serialize)]
pub struct TopToken {
    contract_address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    decimals: Option<i16>,
    transfer_count: i64,
}

#[derive(Serialize)]
pub struct TopTokensResponse {
    ok: bool,
    window: String,
    tokens: Vec<TopToken>,
    count: usize,
    query_time_ms: f64,
}

/// GET /erc20/top-tokens?chainId=X[&window=24h|7d|30d|all][&limit=N]
pub async fn list_top_tokens(
    State(state): State<AppState>,
    Query(params): Query<TopTokensParams>,
) -> Result<Json<TopTokensResponse>, ApiError> {
    let window = params.window.as_deref().unwrap_or("24h").to_string();
    let window_clause = match window.as_str() {
        "24h" => "AND block_timestamp >= now() - INTERVAL 1 DAY",
        "7d" => "AND block_timestamp >= now() - INTERVAL 7 DAY",
        "30d" => "AND block_timestamp >= now() - INTERVAL 30 DAY",
        "all" => "",
        _ => {
            return Err(ApiError::BadRequest(
                "window must be one of: 24h, 7d, 30d, all".to_string(),
            ))
        }
    };

    let limit = pagination::clamp_limit(params.limit, DEFAULT_LIMIT, MAX_LIMIT);

    let ch = state
        .get_clickhouse(Some(params.chain_id))
        .await
        .ok_or_else(|| {
            ApiError::BadRequest(format!(
                "ClickHouse not enabled for chainId: {}",
                params.chain_id
            ))
        })?;

    let start = Instant::now();

    let sql = format!(
        r#"SELECT address, count() AS transfer_count
           FROM logs
           WHERE selector = '{TRANSFER_SELECTOR}'
             AND topic3 IS NULL
             {window_clause}
           GROUP BY address
           ORDER BY transfer_count DESC
           LIMIT {limit}"#
    );

    let result = ch
        .query(&sql, &[])
        .await
        .map_err(|e| ApiError::QueryError(format!("ClickHouse query failed: {e}")))?;

    // (address_hex, count) preserving CH-ordered results.
    let mut ranked: Vec<(String, i64)> = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let addr = row
            .first()
            .and_then(|v| v.as_str())
            .ok_or_else(|| ApiError::QueryError("missing address column".into()))?
            .to_string();
        let count = row
            .get(1)
            .and_then(parse_ch_uint)
            .ok_or_else(|| ApiError::QueryError("missing transfer_count column".into()))?;
        ranked.push((addr, count));
    }

    let metadata = fetch_metadata(&state, params.chain_id, &ranked).await?;

    let tokens: Vec<TopToken> = ranked
        .into_iter()
        .map(|(addr_hex, transfer_count)| {
            let meta = metadata.get(&addr_hex);
            TopToken {
                contract_address: format!("0x{addr_hex}"),
                name: meta.and_then(|m| m.name.clone()),
                symbol: meta.and_then(|m| m.symbol.clone()),
                decimals: meta.and_then(|m| m.decimals),
                transfer_count,
            }
        })
        .collect();

    let count = tokens.len();
    let query_time_ms = start.elapsed().as_secs_f64() * 1000.0;

    Ok(Json(TopTokensResponse {
        ok: true,
        window,
        tokens,
        count,
        query_time_ms,
    }))
}

/// ClickHouse serializes UInt64 as a JSON string by default; fall back to
/// a plain number for safety.
fn parse_ch_uint(v: &serde_json::Value) -> Option<i64> {
    v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_i64())
}

#[derive(Default)]
struct Metadata {
    name: Option<String>,
    symbol: Option<String>,
    decimals: Option<i16>,
}

/// Batch lookup name/symbol/decimals for the ranked addresses. Best-effort:
/// on PG failure we log and return an empty map so CH results still surface.
async fn fetch_metadata(
    state: &AppState,
    chain_id: u64,
    ranked: &[(String, i64)],
) -> Result<std::collections::HashMap<String, Metadata>, ApiError> {
    let mut out = std::collections::HashMap::with_capacity(ranked.len());
    if ranked.is_empty() {
        return Ok(out);
    }

    let pool = state.get_pool(Some(chain_id)).await.ok_or_else(|| {
        ApiError::BadRequest(format!("Unknown chainId: {chain_id}"))
    })?;

    let addrs: Vec<Vec<u8>> = ranked
        .iter()
        .filter_map(|(hex_addr, _)| hex::decode(hex_addr).ok())
        .collect();
    if addrs.is_empty() {
        return Ok(out);
    }

    let conn = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "metadata pool get failed, skipping");
            return Ok(out);
        }
    };

    let byte_refs: Vec<&[u8]> = addrs.iter().map(|a| a.as_slice()).collect();
    let rows = match conn
        .query(
            "SELECT address, name, symbol, decimals
             FROM erc20_tokens
             WHERE address = ANY($1)",
            &[&byte_refs],
        )
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "erc20_tokens metadata query failed");
            return Ok(out);
        }
    };

    for row in rows {
        let addr_bytes: Vec<u8> = row.get(0);
        out.insert(
            hex::encode(&addr_bytes),
            Metadata {
                name: row.get(1),
                symbol: row.get(2),
                decimals: row.get(3),
            },
        );
    }
    Ok(out)
}
