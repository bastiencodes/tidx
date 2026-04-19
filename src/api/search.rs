//! Explorer search endpoint.
//!
//! `GET /search?q=QUERY&chainId=X` classifies `q` and returns matching entities:
//!
//! - decimal digits    → block by number
//! - `0x` + 64 hex     → block by hash OR tx by hash (both are probed)
//! - `0x` + 40 hex     → ERC20 token by address, plus a generic address hit
//! - anything else     → fuzzy `ILIKE '%q%'` against `erc20_tokens.symbol`/`name`
//!
//! Response is `{ ok, query, results, query_time_ms }`. Each result is a
//! discriminated union on `type`: `token`, `block`, `transaction`, `address`.

use std::time::Instant;

use axum::{
    extract::{Query, State},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::api::{ApiError, AppState};

/// Cap on rows returned by the free-text ERC20 branch.
const TOKEN_LIMIT: i64 = 20;

/// Token-list source used to derive `is_verified` (status = 'active') for
/// search-result ordering. Mirrors the constant in `api::erc20::tokens`.
const TW_SOURCE: &str = "trust_wallet";

#[derive(Deserialize)]
pub struct SearchParams {
    q: String,
    #[serde(alias = "chain_id", rename = "chainId")]
    chain_id: u64,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SearchResult {
    Token {
        address: String,
        symbol: Option<String>,
        name: Option<String>,
        /// `true` only when Trust Wallet lists the token with `status = "active"`.
        is_verified: bool,
    },
    Block {
        number: i64,
        hash: String,
    },
    Transaction {
        hash: String,
        block_number: i64,
    },
    Address {
        address: String,
    },
}

#[derive(Serialize)]
pub struct SearchResponse {
    ok: bool,
    query: String,
    results: Vec<SearchResult>,
    query_time_ms: f64,
}

pub async fn search(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Result<Json<SearchResponse>, ApiError> {
    let q = params.q.trim();
    if q.is_empty() {
        return Err(ApiError::BadRequest("q is required".to_string()));
    }

    let pool = state
        .get_pool(Some(params.chain_id))
        .await
        .ok_or_else(|| ApiError::BadRequest(format!("Unknown chainId: {}", params.chain_id)))?;
    let conn = pool
        .get()
        .await
        .map_err(|e| ApiError::Internal(format!("Pool error: {e}")))?;

    let start = Instant::now();
    let mut results: Vec<SearchResult> = Vec::new();

    match classify(q) {
        Classified::BlockNumber(n) => {
            if let Some(row) = conn
                .query_opt("SELECT num, hash FROM blocks WHERE num = $1", &[&n])
                .await
                .map_err(|e| ApiError::QueryError(e.to_string()))?
            {
                let hash: Vec<u8> = row.get(1);
                results.push(SearchResult::Block {
                    number: row.get(0),
                    hash: hex_prefixed(&hash),
                });
            }
        }
        Classified::Hash32(bytes) => {
            if let Some(row) = conn
                .query_opt("SELECT num FROM blocks WHERE hash = $1", &[&bytes])
                .await
                .map_err(|e| ApiError::QueryError(e.to_string()))?
            {
                results.push(SearchResult::Block {
                    number: row.get(0),
                    hash: hex_prefixed(&bytes),
                });
            }
            if let Some(row) = conn
                .query_opt("SELECT block_num FROM txs WHERE hash = $1", &[&bytes])
                .await
                .map_err(|e| ApiError::QueryError(e.to_string()))?
            {
                results.push(SearchResult::Transaction {
                    hash: hex_prefixed(&bytes),
                    block_number: row.get(0),
                });
            }
        }
        Classified::Address(bytes) => {
            let chain_id_i64 = params.chain_id as i64;
            if let Some(row) = conn
                .query_opt(
                    r#"
                    SELECT t.name, t.symbol,
                           COALESCE(tl.status = 'active', false) AS is_verified
                    FROM erc20_tokens t
                    LEFT JOIN token_list tl
                      ON tl.source = $2 AND tl.chain_id = $3 AND tl.address = t.address
                    WHERE t.address = $1
                    "#,
                    &[&bytes, &TW_SOURCE, &chain_id_i64],
                )
                .await
                .map_err(|e| ApiError::QueryError(e.to_string()))?
            {
                results.push(SearchResult::Token {
                    address: hex_prefixed(&bytes),
                    name: row.get(0),
                    symbol: row.get(1),
                    is_verified: row.get(2),
                });
            }
            // Always emit a generic address hit so callers have a navigable link
            // even when the address isn't (yet) a known token.
            results.push(SearchResult::Address {
                address: hex_prefixed(&bytes),
            });
        }
        Classified::Text(text) => {
            // Escape ILIKE wildcards in user input so `100%` doesn't become a free-match.
            let escaped = text
                .replace('\\', "\\\\")
                .replace('%', "\\%")
                .replace('_', "\\_");
            let pattern = format!("%{escaped}%");
            let chain_id_i64 = params.chain_id as i64;
            // LEFT JOIN `token_list` so verified (Trust-Wallet-`active`) tokens
            // sort above unlisted/abandoned/spam hits. The join is sargable on
            // the composite PK and adds one boolean to the sort key ahead of
            // the existing symbol-match / length / recency ordering.
            let rows = conn
                .query(
                    r#"
                    SELECT t.address, t.name, t.symbol,
                           COALESCE(tl.status = 'active', false) AS is_verified
                    FROM erc20_tokens t
                    LEFT JOIN token_list tl
                      ON tl.source = $2 AND tl.chain_id = $3 AND tl.address = t.address
                    WHERE t.symbol ILIKE $1 OR t.name ILIKE $1
                    ORDER BY
                        is_verified DESC,
                        CASE WHEN t.symbol ILIKE $1 THEN 0 ELSE 1 END,
                        length(t.symbol) ASC NULLS LAST,
                        t.first_transfer_at DESC
                    LIMIT $4
                    "#,
                    &[&pattern, &TW_SOURCE, &chain_id_i64, &TOKEN_LIMIT],
                )
                .await
                .map_err(|e| ApiError::QueryError(e.to_string()))?;
            for row in rows {
                let addr: Vec<u8> = row.get(0);
                results.push(SearchResult::Token {
                    address: hex_prefixed(&addr),
                    name: row.get(1),
                    symbol: row.get(2),
                    is_verified: row.get(3),
                });
            }
        }
    }

    let query_time_ms = start.elapsed().as_secs_f64() * 1000.0;

    Ok(Json(SearchResponse {
        ok: true,
        query: q.to_string(),
        results,
        query_time_ms,
    }))
}

enum Classified<'a> {
    BlockNumber(i64),
    Hash32(Vec<u8>),
    Address(Vec<u8>),
    Text(&'a str),
}

fn classify(q: &str) -> Classified<'_> {
    if let Some(hex) = q.strip_prefix("0x").or_else(|| q.strip_prefix("0X")) {
        if hex.len() == 64 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
            if let Ok(b) = hex::decode(hex) {
                return Classified::Hash32(b);
            }
        }
        if hex.len() == 40 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
            if let Ok(b) = hex::decode(hex) {
                return Classified::Address(b);
            }
        }
        // Malformed 0x prefix falls through to text.
    }
    if !q.is_empty() && q.chars().all(|c| c.is_ascii_digit()) {
        if let Ok(n) = q.parse::<i64>() {
            return Classified::BlockNumber(n);
        }
    }
    Classified::Text(q)
}

fn hex_prefixed(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_block_number() {
        assert!(matches!(classify("12345"), Classified::BlockNumber(12345)));
        assert!(matches!(classify("0"), Classified::BlockNumber(0)));
    }

    #[test]
    fn classify_hash32() {
        let h = "0x34c0776f3d11d96d622809960b888954e8e4662a37a3d67b1871a24ce274a591";
        match classify(h) {
            Classified::Hash32(b) => assert_eq!(b.len(), 32),
            _ => panic!("expected Hash32"),
        }
    }

    #[test]
    fn classify_address() {
        let a = "0xd57e8b48886ca96040245b70686a1fc69a5d6fc2";
        match classify(a) {
            Classified::Address(b) => assert_eq!(b.len(), 20),
            _ => panic!("expected Address"),
        }
    }

    #[test]
    fn classify_address_mixed_case() {
        let a = "0xD57E8B48886CA96040245B70686A1FC69A5D6FC2";
        assert!(matches!(classify(a), Classified::Address(_)));
    }

    #[test]
    fn classify_text_fallback() {
        assert!(matches!(classify("usdc"), Classified::Text("usdc")));
        // Bare hex without 0x prefix — treat as text (could be a token symbol).
        assert!(matches!(classify("deadbeef"), Classified::Text("deadbeef")));
        // Malformed 0x length → text.
        assert!(matches!(classify("0xdeadbeef"), Classified::Text("0xdeadbeef")));
    }

    #[test]
    fn classify_text_with_wildcards() {
        // Wildcards must flow through to the handler (which escapes them),
        // not trigger any special path here.
        assert!(matches!(classify("100%"), Classified::Text("100%")));
        assert!(matches!(classify("a_b"), Classified::Text("a_b")));
    }
}
