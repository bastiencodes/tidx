//! Verifies that every per-chain data endpoint requires `?chainId=` and
//! rejects unknown chain ids with 400 Bad Request.
//!
//! Covers the endpoints that were migrated from the implicit default-chain
//! pool (`get_pool(None)`) to an explicit `chainId` query parameter:
//! - GET /blocks/:identifier
//! - GET /transactions/:hash
//! - GET /erc20/tokens
//! - GET /erc20/approvals
//! - GET /erc20/transfers

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::connect_info::IntoMakeServiceWithConnectInfo;
use axum::http::{Request, StatusCode};
use axum::Router;
use common::testdb::TestDb;
use serial_test::serial;
use tower::Service;

use tidx::api;
use tidx::broadcast::Broadcaster;

const CHAIN_ID: u64 = 1;
const UNKNOWN_CHAIN_ID: u64 = 99_999;
const SAMPLE_ADDR: &str = "0x0000000000000000000000000000000000000001";
const SAMPLE_TX_HASH: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000001";

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

async fn call(
    app: &mut (impl Service<
        Request<Body>,
        Response = axum::response::Response,
        Error = std::convert::Infallible,
    > + ?Sized),
    uri: &str,
) -> (StatusCode, serde_json::Value) {
    let response = app
        .call(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value =
        serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    (status, json)
}

// ---------------------------------------------------------------------------
// GET /blocks/:identifier
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(db)]
async fn block_detail_missing_chain_id_is_400() {
    let (_db, mut app) = make_app().await;
    // axum's Query extractor returns a 400 (plain text body) when a required
    // field is absent, hence we only assert the status here.
    let (status, _) = call(&mut app, "/blocks/1").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[serial(db)]
async fn block_detail_unknown_chain_id_is_400() {
    let (_db, mut app) = make_app().await;
    let (status, json) = call(&mut app, &format!("/blocks/1?chainId={UNKNOWN_CHAIN_ID}")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        json["error"]
            .as_str()
            .unwrap()
            .contains(&UNKNOWN_CHAIN_ID.to_string()),
        "body: {json}"
    );
}

#[tokio::test]
#[serial(db)]
async fn block_detail_with_chain_id_succeeds() {
    let (_db, mut app) = make_app().await;
    let (status, json) = call(&mut app, &format!("/blocks/1?chainId={CHAIN_ID}")).await;
    assert_eq!(status, StatusCode::OK, "body: {json}");
    assert_eq!(json["ok"], true);
    assert_eq!(json["block"]["number"], 1);
}

#[tokio::test]
#[serial(db)]
async fn block_detail_accepts_snake_case_alias() {
    let (_db, mut app) = make_app().await;
    let (status, _) = call(&mut app, &format!("/blocks/1?chain_id={CHAIN_ID}")).await;
    assert_eq!(status, StatusCode::OK);
}

// ---------------------------------------------------------------------------
// GET /transactions/:hash
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(db)]
async fn tx_detail_missing_chain_id_is_400() {
    let (_db, mut app) = make_app().await;
    let (status, _) = call(&mut app, &format!("/transactions/{SAMPLE_TX_HASH}")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[serial(db)]
async fn tx_detail_unknown_chain_id_is_400() {
    let (_db, mut app) = make_app().await;
    let (status, _) = call(
        &mut app,
        &format!("/transactions/{SAMPLE_TX_HASH}?chainId={UNKNOWN_CHAIN_ID}"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[serial(db)]
async fn tx_detail_with_chain_id_resolves_pool() {
    let (db, mut app) = make_app().await;
    // Fetch a real tx hash to assert OK; otherwise 404 (still proves chain routed).
    let conn = db.pool.get().await.unwrap();
    let row = conn
        .query_opt("SELECT hash FROM txs LIMIT 1", &[])
        .await
        .unwrap();

    let uri = if let Some(row) = row {
        let hash: Vec<u8> = row.get(0);
        format!(
            "/transactions/0x{}?chainId={CHAIN_ID}",
            hex::encode(&hash)
        )
    } else {
        format!("/transactions/{SAMPLE_TX_HASH}?chainId={CHAIN_ID}")
    };

    let (status, json) = call(&mut app, &uri).await;
    assert!(
        status == StatusCode::OK || status == StatusCode::NOT_FOUND,
        "unexpected status {status}, body: {json}"
    );
}

// ---------------------------------------------------------------------------
// GET /erc20/tokens
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(db)]
async fn tokens_missing_chain_id_is_400() {
    let (_db, mut app) = make_app().await;
    let (status, _) = call(&mut app, "/erc20/tokens").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[serial(db)]
async fn tokens_unknown_chain_id_is_400() {
    let (_db, mut app) = make_app().await;
    let (status, _) = call(
        &mut app,
        &format!("/erc20/tokens?chainId={UNKNOWN_CHAIN_ID}"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[serial(db)]
async fn tokens_with_chain_id_succeeds() {
    let (_db, mut app) = make_app().await;
    let (status, json) = call(&mut app, &format!("/erc20/tokens?chainId={CHAIN_ID}")).await;
    assert_eq!(status, StatusCode::OK, "body: {json}");
    assert_eq!(json["ok"], true);
    assert!(json["tokens"].is_array());
}

// ---------------------------------------------------------------------------
// GET /erc20/approvals
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(db)]
async fn approvals_missing_chain_id_is_400() {
    let (_db, mut app) = make_app().await;
    let (status, json) = call(&mut app, &format!("/erc20/approvals?owner={SAMPLE_ADDR}")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {json}");
}

#[tokio::test]
#[serial(db)]
async fn approvals_unknown_chain_id_is_400() {
    let (_db, mut app) = make_app().await;
    let (status, _) = call(
        &mut app,
        &format!("/erc20/approvals?owner={SAMPLE_ADDR}&chainId={UNKNOWN_CHAIN_ID}"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[serial(db)]
async fn approvals_with_chain_id_succeeds() {
    let (_db, mut app) = make_app().await;
    let (status, json) = call(
        &mut app,
        &format!("/erc20/approvals?owner={SAMPLE_ADDR}&chainId={CHAIN_ID}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {json}");
    assert_eq!(json["ok"], true);
    assert!(json["approvals"].is_array());
}

// ---------------------------------------------------------------------------
// GET /erc20/transfers
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(db)]
async fn transfers_missing_chain_id_is_400() {
    let (_db, mut app) = make_app().await;
    let (status, json) = call(&mut app, &format!("/erc20/transfers?address={SAMPLE_ADDR}")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {json}");
}

#[tokio::test]
#[serial(db)]
async fn transfers_unknown_chain_id_is_400() {
    let (_db, mut app) = make_app().await;
    let (status, _) = call(
        &mut app,
        &format!("/erc20/transfers?address={SAMPLE_ADDR}&chainId={UNKNOWN_CHAIN_ID}"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[serial(db)]
async fn transfers_with_chain_id_succeeds() {
    let (_db, mut app) = make_app().await;
    let (status, json) = call(
        &mut app,
        &format!("/erc20/transfers?address={SAMPLE_ADDR}&chainId={CHAIN_ID}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {json}");
    assert_eq!(json["ok"], true);
    assert!(json["transfers"].is_array());
}
