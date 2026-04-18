mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::connect_info::IntoMakeServiceWithConnectInfo;
use axum::http::{Request, StatusCode};
use axum::Router;
use tower::Service;

use tidx::api::{self, inject_block_filter};
use tidx::broadcast::Broadcaster;
use common::testdb::TestDb;
use serial_test::serial;

fn make_pools(pool: tidx::db::Pool) -> (HashMap<u64, tidx::db::Pool>, u64) {
    let mut pools = HashMap::new();
    let chain_id = 1u64;
    pools.insert(chain_id, pool);
    (pools, chain_id)
}

/// Create a test service that includes ConnectInfo.
async fn make_test_service(
    pools: HashMap<u64, tidx::db::Pool>,
    chain_id: u64,
    broadcaster: Arc<Broadcaster>,
) -> impl Service<Request<Body>, Response = axum::response::Response, Error = std::convert::Infallible>
{
    let mut svc: IntoMakeServiceWithConnectInfo<Router, SocketAddr> = api::router(pools, chain_id, broadcaster)
        .into_make_service_with_connect_info::<SocketAddr>();
    svc.call(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap()
}

#[tokio::test]
#[serial(db)]
async fn test_health_endpoint() {
    let db = TestDb::empty().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    let response = app
        .call(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body[..], b"OK");
}

#[tokio::test]
#[serial(db)]
async fn test_status_endpoint() {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    let response = app
        .call(
            Request::builder()
                .uri("/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["ok"], true);
    assert!(json["version"].is_string());
    assert!(!json["version"].as_str().unwrap().is_empty());
    assert!(json["rev"].is_string());
    assert!(!json["rev"].as_str().unwrap().is_empty());
    assert!(json["chains"].is_array());
}

#[tokio::test]
#[serial(db)]
async fn test_query_select_blocks() {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    let response = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/query?sql=SELECT%20num,%20hash%20FROM%20blocks%20ORDER%20BY%20num%20DESC%20LIMIT%205&chainId=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["ok"], true);
    assert_eq!(json["columns"], serde_json::json!(["num", "hash"]));
    assert!(json["row_count"].as_u64().unwrap() > 0, "expected indexed blocks");
}

#[tokio::test]
#[serial(db)]
async fn test_query_select_txs() {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    let response = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/query?sql=SELECT%20block_num,%20hash,%20%22from%22%20FROM%20txs%20LIMIT%2010&chainId=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["ok"], true);
    assert_eq!(json["columns"], serde_json::json!(["block_num", "hash", "from"]));
}

#[tokio::test]
#[serial(db)]
async fn test_query_select_logs() {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    let response = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/query?sql=SELECT%20block_num,%20address,%20selector%20FROM%20logs%20LIMIT%2010&chainId=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["ok"], true);
    // Logs table may be empty if no contracts emitted events
    let columns = json["columns"].as_array().unwrap();
    if !columns.is_empty() {
        assert_eq!(json["columns"], serde_json::json!(["block_num", "address", "selector"]));
    }
}

#[tokio::test]
#[serial(db)]
async fn test_query_with_signature_cte() {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    // URL encode: spaces=%20, commas=%2C, parens=%28/%29
    let sig = "Transfer(address%20indexed%20from%2Caddress%20indexed%20to%2Cuint256%20value)";
    let uri = format!("/query?sql=SELECT%20*%20FROM%20Transfer%20LIMIT%205&chainId=1&signature={sig}");

    let response = app
        .call(
            Request::builder()
                .method("GET")
                .uri(&uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Signature CTE may fail if logs table is empty - 422 is acceptable
    if status == StatusCode::OK {
        assert_eq!(json["ok"], true);
        let columns = json["columns"].as_array().unwrap();
        if !columns.is_empty() {
            assert!(columns.iter().any(|c| c == "from"), "expected 'from' column");
            assert!(columns.iter().any(|c| c == "to"), "expected 'to' column");
            assert!(columns.iter().any(|c| c == "value"), "expected 'value' column");
        }
    } else {
        // 422 is acceptable if no matching logs exist
        assert!(
            status == StatusCode::UNPROCESSABLE_ENTITY,
            "unexpected status: {}, body: {}",
            status,
            json
        );
    }
}

#[tokio::test]
#[serial(db)]
async fn test_query_rejects_non_select() {
    let db = TestDb::empty().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    let response = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/query?sql=DELETE%20FROM%20blocks&chainId=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["ok"], false);
    assert!(json["error"].as_str().unwrap().contains("SELECT"));
}

#[tokio::test]
#[serial(db)]
async fn test_query_chain_id_param() {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    // Query with explicit chainId (use point lookup to route to Postgres)
    let response = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/query?sql=SELECT%20*%20FROM%20blocks%20WHERE%20num%20%3D%201&chainId=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["ok"], true);
}

#[tokio::test]
#[serial(db)]
async fn test_query_invalid_chain_id() {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    let response = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/query?sql=SELECT%201&chainId=99999")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["ok"], false);
    assert!(json["error"].as_str().unwrap().contains("99999"));
}

#[tokio::test]
#[serial(db)]
async fn test_query_live_returns_sse() {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    let response = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/query?sql=SELECT%20num%20FROM%20blocks%20LIMIT%201&chainId=1&live=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let content_type = response
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap_or(""));
    assert!(
        content_type.unwrap_or("").contains("text/event-stream"),
        "expected SSE content-type, got {content_type:?}"
    );
}

/// Read bytes from a live SSE body until we have a complete frame
/// (`\n\n` terminator) or timeout. Returns the first complete frame's raw text.
async fn read_first_sse_frame(body: Body) -> String {
    use futures::StreamExt;
    let mut stream = body.into_data_stream();
    let mut acc = Vec::new();
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let chunk = tokio::time::timeout(remaining, stream.next())
            .await
            .expect("timeout waiting for first SSE frame")
            .expect("stream ended before first frame")
            .expect("stream error");
        acc.extend_from_slice(&chunk);
        if let Some(end) = acc.windows(2).position(|w| w == b"\n\n") {
            return String::from_utf8_lossy(&acc[..end]).into_owned();
        }
    }
}

/// Parse an SSE frame like `event: result\ndata: {...}` into (event, json).
fn parse_sse_frame(frame: &str) -> (String, serde_json::Value) {
    let mut event = String::new();
    let mut data = String::new();
    for line in frame.lines() {
        if let Some(v) = line.strip_prefix("event:") {
            event = v.trim().to_string();
        } else if let Some(v) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(v.trim_start());
        }
    }
    let json = serde_json::from_str(&data)
        .unwrap_or_else(|e| panic!("invalid JSON in SSE data: {e}: {data}"));
    (event, json)
}

#[tokio::test]
#[serial(db)]
async fn test_transactions_live_decode_populates_decoded_field() {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    // With decode=true, every tx in the initial snapshot must serialize a
    // `decoded` key (value may be null when the selector isn't cached).
    let response = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/transactions?chainId=1&live=true&decode=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let frame = read_first_sse_frame(response.into_body()).await;
    let (event, json) = parse_sse_frame(&frame);
    assert_eq!(event, "result", "frame: {frame}");
    assert_eq!(json["ok"], true);

    let txs = json["transactions"].as_array().expect("transactions array");
    assert!(!txs.is_empty(), "expected seeded txs in initial snapshot");
    for (i, tx) in txs.iter().enumerate() {
        assert!(
            tx.get("decoded").is_some(),
            "tx[{i}] missing `decoded` field with decode=true: {tx}"
        );
    }
}

#[tokio::test]
#[serial(db)]
async fn test_transactions_live_without_decode_omits_field() {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    // Sanity check the inverse: without `decode=true`, `decoded` is omitted
    // (skip_serializing_if on Option::is_none).
    let response = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/transactions?chainId=1&live=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let frame = read_first_sse_frame(response.into_body()).await;
    let (event, json) = parse_sse_frame(&frame);
    assert_eq!(event, "result", "frame: {frame}");

    let txs = json["transactions"].as_array().expect("transactions array");
    assert!(!txs.is_empty(), "expected seeded txs in initial snapshot");
    for (i, tx) in txs.iter().enumerate() {
        assert!(
            tx.get("decoded").is_none(),
            "tx[{i}] should omit `decoded` without decode=true: {tx}"
        );
    }
}

// Unit tests for inject_block_filter (no DB required)

#[test]
fn test_inject_block_filter_blocks_table() {
    let sql = "SELECT num, hash FROM blocks ORDER BY num DESC LIMIT 1";
    let filtered = inject_block_filter(sql, 100).unwrap();
    assert!(filtered.contains("blocks.num = 100"), "got: {filtered}");
    assert!(filtered.contains("ORDER BY"), "should preserve ORDER BY");
}

#[test]
fn test_inject_block_filter_txs_table() {
    let sql = "SELECT * FROM txs ORDER BY block_num DESC LIMIT 10";
    let filtered = inject_block_filter(sql, 200).unwrap();
    assert!(filtered.contains("txs.block_num = 200"), "got: {filtered}");
}

#[test]
fn test_inject_block_filter_logs_table() {
    let sql = "SELECT * FROM logs WHERE address = '0x123' ORDER BY block_num DESC";
    let filtered = inject_block_filter(sql, 300).unwrap();
    assert!(filtered.contains("logs.block_num = 300"), "got: {filtered}");
    assert!(filtered.contains("address = '0x123'"), "should preserve existing WHERE");
}

#[test]
fn test_inject_block_filter_with_existing_where() {
    let sql = "SELECT * FROM txs WHERE gas_used > 21000 ORDER BY block_num DESC";
    let filtered = inject_block_filter(sql, 400).unwrap();
    assert!(filtered.contains("txs.block_num = 400"), "got: {filtered}");
    assert!(filtered.contains("gas_used > 21000"), "should preserve existing condition");
}

#[test]
fn test_inject_block_filter_no_order_by() {
    let sql = "SELECT COUNT(*) FROM blocks LIMIT 1";
    let filtered = inject_block_filter(sql, 500).unwrap();
    assert!(filtered.contains("blocks.num = 500"), "got: {filtered}");
}

#[test]
fn test_inject_block_filter_rejects_union() {
    let sql = "SELECT * FROM txs UNION SELECT * FROM logs";
    assert!(inject_block_filter(sql, 100).is_err());
}

#[test]
fn test_inject_block_filter_rejects_non_select() {
    let sql = "INSERT INTO txs VALUES (1)";
    assert!(inject_block_filter(sql, 100).is_err());
}

#[test]
fn test_inject_block_filter_where_keyword_in_string_literal() {
    let sql = "SELECT * FROM txs WHERE input = 'WHERE clause test'";
    let filtered = inject_block_filter(sql, 100).unwrap();
    assert!(filtered.contains("txs.block_num = 100"), "got: {filtered}");
    assert!(filtered.contains("'WHERE clause test'"), "should preserve string literal");
}
