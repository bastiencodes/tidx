use axum::{extract::State, Json};
use serde::Serialize;

use crate::api::{ApiError, AppState};

/// Transfer(address indexed from, address indexed to, uint256 value)
const TRANSFER_SIGNATURE: &str =
    "Transfer(address indexed from, address indexed to, uint256 value)";

/// SQL to fetch ERC20 token addresses with their first seen timestamp.
/// Filters for exactly 3 topics (selector + topic1 + topic2, no topic3)
/// to exclude ERC721 which indexes the third parameter (tokenId).
/// Uses count() OVER() to get total count without a second query.
const ERC20_TOKENS_SQL: &str = r#"SELECT address AS contract_address, MIN(block_timestamp) AS created_at, count() OVER() AS total_count FROM Transfer WHERE topic1 IS NOT NULL AND topic2 IS NOT NULL AND topic3 IS NULL GROUP BY address ORDER BY created_at ASC LIMIT 100"#;

#[derive(Serialize)]
pub struct Erc20Token {
    contract_address: String,
    created_at: String,
}

#[derive(Serialize)]
pub struct Erc20TokensResponse {
    ok: bool,
    tokens: Vec<Erc20Token>,
    count: usize,
    total_count: u64,
    query_time_ms: Option<f64>,
}

/// GET /erc20/tokens — list all ERC20 token addresses
pub async fn list_tokens(
    State(state): State<AppState>,
) -> Result<Json<Erc20TokensResponse>, ApiError> {
    let clickhouse = state
        .get_clickhouse(None)
        .await
        .ok_or_else(|| ApiError::Internal("ClickHouse not configured for default chain".to_string()))?;

    let result = clickhouse
        .query(ERC20_TOKENS_SQL, &[TRANSFER_SIGNATURE])
        .await
        .map_err(|e| ApiError::QueryError(e.to_string()))?;

    let query_time_ms = result.query_time_ms;

    let mut total_count: u64 = 0;

    let tokens: Vec<Erc20Token> = result
        .rows
        .into_iter()
        .filter_map(|row| {
            let contract_address = row.first()?.as_str()?.to_string();
            let created_at = row.get(1)?.as_str()?.to_string();
            total_count = row.get(2).and_then(|v| v.as_u64()).unwrap_or(0);
            Some(Erc20Token {
                contract_address,
                created_at,
            })
        })
        .collect();

    let count = tokens.len();

    Ok(Json(Erc20TokensResponse {
        ok: true,
        tokens,
        count,
        total_count,
        query_time_ms,
    }))
}
