use axum::{
    extract::{Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use std::time::Instant;

use crate::api::pagination::{self, DEFAULT_LIMIT, MAX_LIMIT};
use crate::api::{ApiError, AppState};

/// ERC20 Transfer(address,address,uint256) topic0
const TRANSFER_SELECTOR: &str = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

/// Keyset cursor: the `(block_num, log_idx)` of the last row on the previous
/// page.
#[derive(Serialize, Deserialize)]
struct TransfersCursor {
    b: i64,
    l: i32,
}

#[derive(Deserialize)]
pub struct TransferParams {
    /// Address to get transfers for
    address: String,
    /// Filter direction: "in", "out", or omit for both
    #[serde(default)]
    direction: Option<String>,
    #[serde(alias = "chain_id", rename = "chainId")]
    chain_id: u64,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    cursor: Option<String>,
    /// Populate `labels` on each transfer using the `labels_*` tables.
    #[serde(default)]
    labels: bool,
    /// Populate `metadata` on each transfer by looking up `contract_address`
    /// in `erc20_tokens` (+ `token_list` for `logo_url`).
    #[serde(default)]
    include_metadata: bool,
}

#[derive(Serialize)]
pub struct TransferEntry {
    block_number: serde_json::Value,
    block_timestamp: serde_json::Value,
    tx_hash: serde_json::Value,
    tx_idx: serde_json::Value,
    contract_address: serde_json::Value,
    direction: String,
    from: serde_json::Value,
    to: serde_json::Value,
    value: serde_json::Value,
    /// Present only when the caller set `?labels=true`. Keys (`from`, `to`,
    /// `contract_address`) are omitted when no label was found. Values are
    /// arrays because one address commonly carries multiple tags.
    #[serde(skip_serializing_if = "Option::is_none")]
    labels: Option<std::collections::BTreeMap<&'static str, Vec<crate::labels::Label>>>,
    /// Present only when the caller set `?include_metadata=true`. Fields are
    /// individually omitted when missing. Always set (possibly empty) when
    /// the flag is on so clients can rely on the key existing.
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<TokenMetadata>,
}

#[derive(Serialize, Clone, Default)]
pub struct TokenMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    decimals: Option<i16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    logo_url: Option<String>,
}

#[derive(Serialize)]
pub struct TransfersResponse {
    ok: bool,
    transfers: Vec<TransferEntry>,
    count: usize,
    /// Opaque cursor for the next page, or `null` on the last page.
    next_cursor: Option<String>,
    query_time_ms: f64,
}

/// GET /erc20/transfers?address=0x...&direction=in&chainId=1[&limit=N][&cursor=...]
pub async fn list_transfers(
    State(state): State<AppState>,
    Query(params): Query<TransferParams>,
) -> Result<Json<TransfersResponse>, ApiError> {
    // Validate direction
    if let Some(ref dir) = params.direction {
        if dir != "in" && dir != "out" {
            return Err(ApiError::BadRequest(
                "direction must be 'in' or 'out'".to_string(),
            ));
        }
    }

    // Validate and left-pad address to 32 bytes for topic comparison
    let addr_hex = params.address.strip_prefix("0x").unwrap_or(&params.address);
    if addr_hex.len() != 40 || !addr_hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(ApiError::BadRequest("Invalid address format".to_string()));
    }
    let padded = format!("000000000000000000000000{}", addr_hex.to_lowercase());

    let pool = state.get_pool(Some(params.chain_id)).await.ok_or_else(|| {
        ApiError::BadRequest(format!("Unknown chainId: {}", params.chain_id))
    })?;

    let limit = pagination::clamp_limit(params.limit, DEFAULT_LIMIT, MAX_LIMIT);
    let cursor: Option<TransfersCursor> = params
        .cursor
        .as_deref()
        .map(pagination::decode)
        .transpose()?;
    // Sentinel (MAX, MAX) admits every real row when no cursor is supplied,
    // keeping the query shape identical across first / subsequent pages.
    let (cb, cl) = cursor.as_ref().map_or((i64::MAX, i32::MAX), |c| (c.b, c.l));

    // Build address filter based on direction
    let address_filter = match params.direction.as_deref() {
        Some("in") => format!("AND topic2 = '\\x{padded}'"),
        Some("out") => format!("AND topic1 = '\\x{padded}'"),
        _ => format!("AND (topic1 = '\\x{padded}' OR topic2 = '\\x{padded}')"),
    };

    let sql = format!(
        r#"SELECT
            block_num,
            block_timestamp,
            address,
            abi_address(topic1) AS "from",
            abi_address(topic2) AS "to",
            abi_uint(substring(data FROM 1 FOR 32)) AS value,
            log_idx,
            tx_hash,
            tx_idx
        FROM logs
        WHERE selector = '\x{TRANSFER_SELECTOR}'
            {address_filter}
            AND (block_num, log_idx) < ($1, $2)
        ORDER BY block_num DESC, log_idx DESC
        LIMIT $3"#
    );

    let conn = pool
        .get()
        .await
        .map_err(|e| ApiError::Internal(format!("Pool error: {e}")))?;

    conn.execute("SET statement_timeout = 5000", &[])
        .await
        .map_err(|e| ApiError::Internal(format!("Failed to set timeout: {e}")))?;

    let start = Instant::now();
    let rows = tokio::time::timeout(
        std::time::Duration::from_millis(5100),
        conn.query(&sql, &[&cb, &cl, &limit]),
    )
    .await
    .map_err(|_| ApiError::Timeout)?
    .map_err(|e| {
        if e.to_string().contains("timeout") {
            ApiError::Timeout
        } else {
            ApiError::QueryError(e.to_string())
        }
    })?;

    let query_time_ms = start.elapsed().as_secs_f64() * 1000.0;

    let addr_lower = params.address.to_lowercase();

    let next_cursor = if (rows.len() as i64) < limit {
        None
    } else {
        rows.last().map(|row| {
            let block_num: i64 = row.get(0);
            let log_idx: i32 = row.get(6);
            pagination::encode(&TransfersCursor { b: block_num, l: log_idx })
        })
    };

    let mut transfers: Vec<TransferEntry> = rows
        .iter()
        .map(|row| {
            let from_val = crate::service::format_column_json(row, 3);
            let to_val = crate::service::format_column_json(row, 4);

            // Determine direction based on which field matches the queried address
            let direction = if let Some(ref dir) = params.direction {
                dir.clone()
            } else {
                let to_str = to_val.as_str().unwrap_or_default().to_lowercase();
                if to_str == addr_lower {
                    "in".to_string()
                } else {
                    "out".to_string()
                }
            };

            TransferEntry {
                block_number: crate::service::format_column_json(row, 0),
                block_timestamp: crate::service::format_column_json(row, 1),
                tx_hash: crate::service::format_column_json(row, 7),
                tx_idx: crate::service::format_column_json(row, 8),
                contract_address: crate::service::format_column_json(row, 2),
                from: from_val,
                to: to_val,
                value: crate::service::format_column_json(row, 5),
                direction,
                labels: None,
                metadata: None,
            }
        })
        .collect();

    if params.labels {
        attach_transfer_labels(&pool, &mut transfers).await;
    }

    if params.include_metadata {
        attach_transfer_metadata(&pool, params.chain_id as i64, &mut transfers).await;
    }

    let count = transfers.len();

    Ok(Json(TransfersResponse {
        ok: true,
        transfers,
        count,
        next_cursor,
        query_time_ms,
    }))
}

/// Batch-lookup labels for every `from`/`to`/`contract_address` in `transfers`
/// and attach a per-row `labels` map. Best-effort; failures log and drop to
/// empty maps.
async fn attach_transfer_labels(pool: &crate::db::Pool, transfers: &mut [TransferEntry]) {
    let mut addrs: Vec<[u8; 20]> = Vec::with_capacity(transfers.len() * 3);
    for t in transfers.iter() {
        for s in [&t.from, &t.to, &t.contract_address] {
            if let Some(a) = s.as_str().and_then(crate::labels::parse_address_20) {
                addrs.push(a);
            }
        }
    }

    let map = crate::labels::lookup_batch(pool, &addrs).await;

    for t in transfers.iter_mut() {
        let mut labels: std::collections::BTreeMap<&'static str, Vec<crate::labels::Label>> =
            std::collections::BTreeMap::new();
        for (key, val) in [
            ("from", &t.from),
            ("to", &t.to),
            ("contract_address", &t.contract_address),
        ] {
            if let Some(a) = val.as_str().and_then(crate::labels::parse_address_20) {
                if let Some(ls) = map.get(&a) {
                    labels.insert(key, ls.clone());
                }
            }
        }
        t.labels = Some(labels);
    }
}

/// Batch-lookup ERC20 metadata for every distinct `contract_address` in
/// `transfers` and attach a per-row `metadata` object. Best-effort: any PG
/// failure logs and leaves `metadata` as the default empty struct on each row,
/// so clients can always rely on the key being present when the flag is on.
async fn attach_transfer_metadata(
    pool: &crate::db::Pool,
    chain_id: i64,
    transfers: &mut [TransferEntry],
) {
    // Default every row to an empty object up front — metadata being
    // present (even if empty) is the contract of `?include_metadata=true`.
    for t in transfers.iter_mut() {
        t.metadata = Some(TokenMetadata::default());
    }

    let mut addrs: Vec<[u8; 20]> = transfers
        .iter()
        .filter_map(|t| {
            t.contract_address
                .as_str()
                .and_then(crate::labels::parse_address_20)
        })
        .collect();
    if addrs.is_empty() {
        return;
    }
    addrs.sort_unstable();
    addrs.dedup();

    let conn = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "metadata pool get failed, skipping");
            return;
        }
    };

    let byte_refs: Vec<&[u8]> = addrs.iter().map(|a| a.as_slice()).collect();

    // LEFT JOIN so tokens missing from `token_list` still return their
    // on-chain name/symbol/decimals. `source = 'trust_wallet'` matches the
    // single source populated today (see src/api/erc20/tokens.rs).
    //
    // On-chain is source of truth, but we COALESCE to `token_list` so tokens
    // with pending/failed resolution still surface human-readable metadata.
    let sql = r#"
        SELECT
            t.address,
            COALESCE(t.name, tl.name)         AS name,
            COALESCE(t.symbol, tl.symbol)     AS symbol,
            COALESCE(t.decimals, tl.decimals) AS decimals,
            tl.logo_uri
        FROM erc20_tokens t
        LEFT JOIN token_list tl
          ON tl.source = 'trust_wallet'
         AND tl.chain_id = $1
         AND tl.address = t.address
        WHERE t.address = ANY($2)
    "#;

    let rows = match conn.query(sql, &[&chain_id, &byte_refs]).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "erc20_tokens metadata query failed");
            return;
        }
    };

    let mut map: std::collections::HashMap<[u8; 20], TokenMetadata> =
        std::collections::HashMap::with_capacity(rows.len());
    for row in rows {
        let addr: &[u8] = row.get(0);
        if addr.len() != 20 {
            continue;
        }
        let mut key = [0u8; 20];
        key.copy_from_slice(addr);
        map.insert(
            key,
            TokenMetadata {
                name: row.get(1),
                symbol: row.get(2),
                decimals: row.get(3),
                logo_url: row.get(4),
            },
        );
    }

    for t in transfers.iter_mut() {
        if let Some(a) = t
            .contract_address
            .as_str()
            .and_then(crate::labels::parse_address_20)
        {
            if let Some(m) = map.get(&a) {
                t.metadata = Some(m.clone());
            }
        }
    }
}
