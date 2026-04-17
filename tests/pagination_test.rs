//! End-to-end pagination tests for the list endpoints.
//!
//! Covers:
//! - `GET /blocks` — newest-first cursor pagination
//! - `GET /transactions` — LATEST path cursor pagination
//! - `GET /transactions?block=N` — BY_BLOCK cursor pagination
//! - `GET /erc20/tokens` — newest-first cursor pagination
//! - cursor rejection (malformed base64 / JSON)
//!
//! The tests rely on `TestDb::new()` seeding (Tempo node required, same as
//! smoke_test). All tests are robust to seed size variance — we only assume
//! "there are at least N rows" and check that consecutive pages don't
//! overlap.

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::connect_info::IntoMakeServiceWithConnectInfo;
use axum::http::{Request, StatusCode};
use axum::Router;
use common::testdb::TestDb;
use serde_json::Value;
use serial_test::serial;
use tower::Service;

use tidx::api;
use tidx::broadcast::Broadcaster;

const CHAIN_ID: u64 = 1;

async fn make_app() -> (
    TestDb,
    impl Service<Request<Body>, Response = axum::response::Response, Error = std::convert::Infallible>,
) {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let mut pools = HashMap::new();
    pools.insert(CHAIN_ID, db.pool.clone());

    let mut svc: IntoMakeServiceWithConnectInfo<Router, SocketAddr> =
        api::router(pools, CHAIN_ID, broadcaster)
            .into_make_service_with_connect_info::<SocketAddr>();
    let app = svc
        .call(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    (db, app)
}

async fn get_json(
    app: &mut (impl Service<
        Request<Body>,
        Response = axum::response::Response,
        Error = std::convert::Infallible,
    > + ?Sized),
    uri: &str,
) -> (StatusCode, Value) {
    let response = app
        .call(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    (status, json)
}

// ---------------------------------------------------------------------------
// GET /blocks pagination
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(db)]
async fn blocks_two_pages_do_not_overlap() {
    let (_db, mut app) = make_app().await;

    let limit = 5;
    let (status, page1) =
        get_json(&mut app, &format!("/blocks?chainId={CHAIN_ID}&limit={limit}")).await;
    assert_eq!(status, StatusCode::OK, "page1: {page1}");
    let blocks1 = page1["blocks"].as_array().unwrap().clone();
    assert_eq!(blocks1.len(), limit as usize, "page1: {page1}");
    let cursor = page1["next_cursor"].as_str().expect("expected cursor");

    let (status, page2) = get_json(
        &mut app,
        &format!("/blocks?chainId={CHAIN_ID}&limit={limit}&cursor={cursor}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "page2: {page2}");
    let blocks2 = page2["blocks"].as_array().unwrap().clone();
    assert!(!blocks2.is_empty(), "expected non-empty page2");

    // No overlap: every page-2 block number must be strictly less than every
    // page-1 block number (newest-first ordering).
    let min_p1 = blocks1
        .iter()
        .map(|b| b["number"].as_i64().unwrap())
        .min()
        .unwrap();
    let max_p2 = blocks2
        .iter()
        .map(|b| b["number"].as_i64().unwrap())
        .max()
        .unwrap();
    assert!(
        max_p2 < min_p1,
        "expected page2 below page1: p1_min={min_p1} p2_max={max_p2}"
    );
}

#[tokio::test]
#[serial(db)]
async fn blocks_rejects_malformed_cursor() {
    let (_db, mut app) = make_app().await;

    let (status, body) = get_json(
        &mut app,
        &format!("/blocks?chainId={CHAIN_ID}&cursor=not-valid-base64!!!"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert!(
        body["error"].as_str().unwrap_or_default().contains("cursor"),
        "body: {body}"
    );
}

#[tokio::test]
#[serial(db)]
async fn blocks_limit_is_clamped() {
    let (_db, mut app) = make_app().await;

    // limit=0 must clamp to 1; limit=99999 must clamp to MAX_LIMIT (500).
    let (status, page) = get_json(
        &mut app,
        &format!("/blocks?chainId={CHAIN_ID}&limit=0"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let count = page["blocks"].as_array().unwrap().len();
    assert!(count <= 1, "expected at most 1 row after clamp, got {count}");
}

// ---------------------------------------------------------------------------
// GET /transactions (LATEST) pagination
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(db)]
async fn transactions_latest_two_pages_do_not_overlap() {
    let (db, mut app) = make_app().await;

    // Skip quickly if seeding produced too few txs for a meaningful test.
    let tx_count = db.tx_count().await;
    if tx_count < 10 {
        eprintln!("skipping: only {tx_count} txs seeded");
        return;
    }

    let limit = 5;
    let (status, page1) = get_json(
        &mut app,
        &format!("/transactions?chainId={CHAIN_ID}&limit={limit}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "page1: {page1}");
    let txs1 = page1["transactions"].as_array().unwrap().clone();
    assert_eq!(txs1.len(), limit as usize);
    let cursor = page1["next_cursor"].as_str().expect("expected cursor");

    let (status, page2) = get_json(
        &mut app,
        &format!("/transactions?chainId={CHAIN_ID}&limit={limit}&cursor={cursor}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "page2: {page2}");
    let txs2 = page2["transactions"].as_array().unwrap().clone();
    assert!(!txs2.is_empty(), "expected non-empty page2");

    // Hashes across pages must be disjoint.
    let p1_hashes: std::collections::HashSet<_> =
        txs1.iter().map(|t| t["hash"].as_str().unwrap().to_owned()).collect();
    for tx in &txs2 {
        let h = tx["hash"].as_str().unwrap();
        assert!(!p1_hashes.contains(h), "page2 contains page1 hash {h}");
    }
}

// ---------------------------------------------------------------------------
// GET /transactions?block=N (BY_BLOCK) pagination
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(db)]
async fn transactions_by_block_pagination_walks_idx_ascending() {
    let (db, mut app) = make_app().await;

    // Find a block with at least 3 txs, otherwise skip.
    let conn = db.pool.get().await.unwrap();
    let row = conn
        .query_opt(
            "SELECT block_num FROM txs GROUP BY block_num HAVING COUNT(*) >= 3 LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    let Some(row) = row else {
        eprintln!("skipping: no block with >=3 txs seeded");
        return;
    };
    let block_num: i64 = row.get(0);

    let limit = 2;
    let (status, page1) = get_json(
        &mut app,
        &format!("/transactions?chainId={CHAIN_ID}&block={block_num}&limit={limit}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "page1: {page1}");
    let txs1 = page1["transactions"].as_array().unwrap().clone();
    assert_eq!(txs1.len(), limit as usize);
    // Idx must be ascending within a block.
    let idx_a = txs1[0]["transaction_index"].as_i64().unwrap();
    let idx_b = txs1[1]["transaction_index"].as_i64().unwrap();
    assert!(idx_a < idx_b, "expected ascending idx, got {idx_a} then {idx_b}");

    let cursor = page1["next_cursor"].as_str().expect("expected cursor");
    let (status, page2) = get_json(
        &mut app,
        &format!(
            "/transactions?chainId={CHAIN_ID}&block={block_num}&limit={limit}&cursor={cursor}"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "page2: {page2}");
    let txs2 = page2["transactions"].as_array().unwrap();
    assert!(!txs2.is_empty(), "expected non-empty page2");
    // First tx on page 2 must have idx strictly greater than last tx on page 1.
    let last_p1_idx = txs1.last().unwrap()["transaction_index"].as_i64().unwrap();
    let first_p2_idx = txs2[0]["transaction_index"].as_i64().unwrap();
    assert!(
        first_p2_idx > last_p1_idx,
        "expected page2[0].idx > page1[-1].idx, got {first_p2_idx} vs {last_p1_idx}"
    );
}

// ---------------------------------------------------------------------------
// GET /erc20/tokens pagination
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(db)]
async fn tokens_pagination_walks_first_transfer_at_desc() {
    let (db, mut app) = make_app().await;

    // erc20_tokens is populated by the discovery worker, which doesn't run in
    // tests. Seed two rows directly so we can exercise cursor walking.
    let conn = db.pool.get().await.unwrap();
    conn.execute("DELETE FROM erc20_tokens", &[]).await.unwrap();
    conn.execute(
        "INSERT INTO erc20_tokens
            (address, first_transfer_block, first_transfer_tx_hash, first_transfer_at, resolution_status)
         VALUES
            ('\\x1111111111111111111111111111111111111111', 1, '\\xaa', '2026-01-01T00:00:00Z', 'pending'),
            ('\\x2222222222222222222222222222222222222222', 2, '\\xbb', '2026-01-02T00:00:00Z', 'pending'),
            ('\\x3333333333333333333333333333333333333333', 3, '\\xcc', '2026-01-03T00:00:00Z', 'pending')",
        &[],
    )
    .await
    .unwrap();

    let (status, page1) = get_json(
        &mut app,
        &format!("/erc20/tokens?chainId={CHAIN_ID}&limit=2"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "page1: {page1}");
    let tokens1 = page1["tokens"].as_array().unwrap();
    assert_eq!(tokens1.len(), 2, "page1: {page1}");
    assert_eq!(
        tokens1[0]["contract_address"].as_str().unwrap(),
        "0x3333333333333333333333333333333333333333"
    );
    assert_eq!(
        tokens1[1]["contract_address"].as_str().unwrap(),
        "0x2222222222222222222222222222222222222222"
    );
    let cursor = page1["next_cursor"].as_str().expect("expected cursor");

    let (status, page2) = get_json(
        &mut app,
        &format!("/erc20/tokens?chainId={CHAIN_ID}&limit=2&cursor={cursor}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "page2: {page2}");
    let tokens2 = page2["tokens"].as_array().unwrap();
    assert_eq!(tokens2.len(), 1, "page2: {page2}");
    assert_eq!(
        tokens2[0]["contract_address"].as_str().unwrap(),
        "0x1111111111111111111111111111111111111111"
    );
    assert!(page2["next_cursor"].is_null(), "page2: {page2}");

    // Cleanup so we don't pollute other tests.
    conn.execute("DELETE FROM erc20_tokens", &[]).await.unwrap();
}
