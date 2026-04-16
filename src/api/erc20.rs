use axum::{extract::State, Json};
use serde::Serialize;

use super::{ApiError, AppState};
use crate::service::QueryOptions;

/// Transfer(address indexed from, address indexed to, uint256 value)
const TRANSFER_SIGNATURE: &str =
    "Transfer(address indexed from, address indexed to, uint256 value)";

/// SQL to fetch distinct ERC20 token addresses.
/// Filters for exactly 3 topics (selector + topic1 + topic2, no topic3)
/// to exclude ERC721 which indexes the third parameter (tokenId).
const ERC20_TOKENS_SQL: &str = r#"SELECT DISTINCT address AS token FROM Transfer WHERE topic1 IS NOT NULL AND topic2 IS NOT NULL AND topic3 IS NULL"#;

#[derive(Serialize)]
pub struct Erc20TokensResponse {
    ok: bool,
    tokens: Vec<String>,
    count: usize,
}

/// GET /erc20/tokens — list all ERC20 token addresses
pub async fn list_tokens(
    State(state): State<AppState>,
) -> Result<Json<Erc20TokensResponse>, ApiError> {
    let pool = state
        .get_pool(None)
        .await
        .ok_or_else(|| ApiError::Internal("No default chain configured".to_string()))?;

    let options = QueryOptions {
        timeout_ms: 30_000,
        limit: crate::query::HARD_LIMIT_MAX,
    };

    let result = crate::service::execute_query_postgres(
        &pool,
        ERC20_TOKENS_SQL,
        &[TRANSFER_SIGNATURE],
        &options,
    )
    .await
    .map_err(|e| ApiError::QueryError(e.to_string()))?;

    let tokens: Vec<String> = result
        .rows
        .into_iter()
        .filter_map(|row| {
            row.into_iter()
                .next()
                .and_then(|v| v.as_str().map(String::from))
        })
        .collect();

    let count = tokens.len();

    Ok(Json(Erc20TokensResponse {
        ok: true,
        tokens,
        count,
    }))
}
