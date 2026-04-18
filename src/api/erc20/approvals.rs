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

/// Current approvals for a given owner: latest value per (contract, spender),
/// excluding revoked approvals (amount = 0).
///
/// We must filter zero amounts AFTER taking the latest per group — filtering
/// before would surface a stale non-zero approval when the user later revoked it.
///
/// Uses DISTINCT ON (address, spender) ORDER BY block_num DESC to get the most
/// recent Approval event per token+spender pair. PostgreSQL's B-tree index on
/// (topic1, address, block_num) makes this efficient at P99 volumes (~65 events).
const CURRENT_APPROVALS_SQL: &str = r#"
SELECT contract_address, "owner", "spender", amount, updated_at FROM (
    SELECT DISTINCT ON (address, "spender")
        address        AS contract_address,
        "owner",
        "spender",
        "value"        AS amount,
        block_timestamp AS updated_at
    FROM Approval
    WHERE "owner" = '{owner}'
    ORDER BY address, "spender", block_num DESC
) latest
WHERE amount != 0
"#;

#[derive(Deserialize)]
pub struct ApprovalsParams {
    /// Owner address to query (required), e.g. 0xabc...
    owner: String,
    #[serde(alias = "chain_id", rename = "chainId")]
    chain_id: u64,
    /// Populate `labels` on each approval using the `labels_*` tables.
    #[serde(default)]
    labels: bool,
}

#[derive(Serialize)]
pub struct Erc20Approval {
    contract_address: String,
    owner: String,
    spender: String,
    amount: String,
    updated_at: String,
    /// Present only when the caller set `?labels=true`. Keys (`owner`,
    /// `spender`, `contract_address`) are omitted when no label was found.
    #[serde(skip_serializing_if = "Option::is_none")]
    labels: Option<std::collections::BTreeMap<&'static str, crate::labels::Label>>,
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
pub async fn list_approvals(
    State(state): State<AppState>,
    Query(params): Query<ApprovalsParams>,
) -> Result<Json<Erc20ApprovalsResponse>, ApiError> {
    let owner = validate_address(&params.owner)?;

    let pool = state
        .get_pool(Some(params.chain_id))
        .await
        .ok_or_else(|| ApiError::BadRequest(format!("Unknown chainId: {}", params.chain_id)))?;

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

    let mut approvals: Vec<Erc20Approval> = result
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
                labels: None,
            })
        })
        .collect();

    if params.labels {
        attach_approval_labels(&pool, &mut approvals).await;
    }

    let count = approvals.len();

    Ok(Json(Erc20ApprovalsResponse {
        ok: true,
        approvals,
        count,
        query_time_ms,
    }))
}

/// Batch-lookup labels for every `owner`/`spender`/`contract_address` in
/// `approvals` and attach a per-row `labels` map. Best-effort; failures log
/// and drop to empty maps.
async fn attach_approval_labels(pool: &crate::db::Pool, approvals: &mut [Erc20Approval]) {
    let mut addrs: Vec<[u8; 20]> = Vec::with_capacity(approvals.len() * 3);
    for a in approvals.iter() {
        for s in [&a.owner, &a.spender, &a.contract_address] {
            if let Some(b) = crate::labels::parse_address_20(s) {
                addrs.push(b);
            }
        }
    }

    let map = crate::labels::lookup_batch(pool, &addrs).await;

    for a in approvals.iter_mut() {
        let mut labels: std::collections::BTreeMap<&'static str, crate::labels::Label> =
            std::collections::BTreeMap::new();
        for (key, val) in [
            ("owner", &a.owner),
            ("spender", &a.spender),
            ("contract_address", &a.contract_address),
        ] {
            if let Some(b) = crate::labels::parse_address_20(val) {
                if let Some(l) = map.get(&b) {
                    labels.insert(key, l.clone());
                }
            }
        }
        a.labels = Some(labels);
    }
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
