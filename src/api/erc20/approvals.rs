use axum::{
    extract::{Query, State},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::api::{ApiError, AppState};
use crate::service::{execute_query_postgres, QueryOptions};

/// Approval(address indexed owner, address indexed spender, uint256 value)
const APPROVAL_SIGNATURE: &str =
    "Approval(address indexed owner, address indexed spender, uint256 value)";

/// Current approvals for a given owner: latest value per (contract, spender).
///
/// Uses DISTINCT ON (address, spender) ORDER BY block_num DESC to get the most
/// recent Approval event per token+spender pair. PostgreSQL's B-tree index on
/// (topic1, address, block_num) makes this efficient at P99 volumes (~65 events).
const CURRENT_APPROVALS_SQL: &str = r#"
SELECT DISTINCT ON (address, "spender")
    address        AS contract_address,
    "owner",
    "spender",
    "value"        AS amount,
    block_timestamp AS updated_at
FROM Approval
WHERE "owner" = '{owner}'
ORDER BY address, "spender", block_num DESC
"#;

#[derive(Deserialize)]
pub struct ApprovalsParams {
    /// Owner address to query (required), e.g. 0xabc...
    owner: String,
}

#[derive(Serialize)]
pub struct Erc20Approval {
    contract_address: String,
    owner: String,
    spender: String,
    amount: String,
    updated_at: String,
}

#[derive(Serialize)]
pub struct Erc20ApprovalsResponse {
    ok: bool,
    approvals: Vec<Erc20Approval>,
    count: usize,
    query_time_ms: Option<f64>,
}

/// GET /erc20/approvals?owner=0x...&chainId=...
///
/// Returns the current (latest) ERC20 approvals for the given owner address.
/// "Current" means the most recent Approval event per (contract, spender) pair,
/// which is the effective allowance at the latest indexed block.
pub async fn get_approvals(
    State(state): State<AppState>,
    Query(params): Query<ApprovalsParams>,
) -> Result<Json<Erc20ApprovalsResponse>, ApiError> {
    let owner = validate_address(&params.owner)?;

    let pool = state
        .get_pool(None)
        .await
        .ok_or_else(|| ApiError::Internal("No default chain configured".to_string()))?;

    let sql = CURRENT_APPROVALS_SQL.replace("{owner}", &owner);
    // Default 5s is too tight for outlier addresses with millions of approvals
    // (~5s on PG). P99 users (~65 approvals) complete in ~2ms regardless.
    let options = QueryOptions {
        timeout_ms: 30_000,
        ..QueryOptions::default()
    };

    let result = execute_query_postgres(&pool, &sql, &[APPROVAL_SIGNATURE], &options)
        .await
        .map_err(|e| ApiError::QueryError(e.to_string()))?;

    let query_time_ms = result.query_time_ms;

    let approvals: Vec<Erc20Approval> = result
        .rows
        .into_iter()
        .filter_map(|row| {
            let contract_address = row.first()?.as_str()?.to_string();
            let owner = row.get(1)?.as_str()?.to_string();
            let spender = row.get(2)?.as_str()?.to_string();
            let amount = row.get(3)?.as_str()?.to_string();
            let updated_at = row.get(4)?.as_str()?.to_string();
            Some(Erc20Approval {
                contract_address,
                owner,
                spender,
                amount,
                updated_at,
            })
        })
        .collect();

    let count = approvals.len();

    Ok(Json(Erc20ApprovalsResponse {
        ok: true,
        approvals,
        count,
        query_time_ms,
    }))
}

/// Validate and normalise an Ethereum address.
/// Accepts "0x" + 40 hex chars (case-insensitive), returns lowercase with "0x" prefix.
fn validate_address(addr: &str) -> Result<String, ApiError> {
    let hex = addr
        .strip_prefix("0x")
        .or_else(|| addr.strip_prefix("0X"))
        .ok_or_else(|| ApiError::BadRequest("Address must start with 0x".to_string()))?;

    if hex.len() != 40 {
        return Err(ApiError::BadRequest(
            "Address must be 42 characters (0x + 40 hex)".to_string(),
        ));
    }

    if !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(ApiError::BadRequest(
            "Address contains invalid hex characters".to_string(),
        ));
    }

    Ok(format!("0x{}", hex.to_lowercase()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_address_valid() {
        let result = validate_address("0xdAC17F958D2ee523a2206206994597C13D831ec7");
        assert_eq!(
            result.unwrap(),
            "0xdac17f958d2ee523a2206206994597c13d831ec7"
        );
    }

    #[test]
    fn test_validate_address_no_prefix() {
        assert!(validate_address("dac17f958d2ee523a2206206994597c13d831ec7").is_err());
    }

    #[test]
    fn test_validate_address_too_short() {
        assert!(validate_address("0xdead").is_err());
    }

    #[test]
    fn test_validate_address_invalid_chars() {
        assert!(validate_address("0xGGGG7F958D2ee523a2206206994597C13D831ec7").is_err());
    }
}
