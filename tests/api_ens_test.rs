//! Integration tests for the `?ens=true` enrichment path on `/transactions`.
//!
//! The live-RPC miss path is not exercised here — per AGENTS.md "Never use
//! mocks", we don't mock RPC responses, and hitting a real mainnet RPC would
//! make tests slow and flaky. Instead, these tests pre-seed `ens_records`
//! with fresh rows (`resolved_at = NOW()`) so the resolver short-circuits to
//! cache and never reaches for the RpcClient. The bogus RPC URL bundled with
//! the test `EnsRuntimeState` is therefore never contacted; if a future
//! change breaks the cache-first invariant, these tests will fail fast with
//! a connection error instead of silently hitting the network.

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::{self, Body};
use axum::extract::connect_info::IntoMakeServiceWithConnectInfo;
use axum::http::{Request, StatusCode};
use axum::Router;
use common::testdb::TestDb;
use serial_test::serial;
use tower::Service;

use tidx::api::{self, ChainClickHouseConfig};
use tidx::broadcast::Broadcaster;
use tidx::ens::{EnsConfig, EnsRuntimeState, ENS_REGISTRY_MAINNET};
use tidx::sync::fetcher::RpcClient;

const TEST_CHAIN_ID: u64 = 1;

/// Build a test service with ENS pre-wired for `chain_id`. The RPC URL is
/// deliberately bogus so any accidental cache-miss path blows up loudly in
/// CI rather than reaching out over the network.
async fn make_test_service_with_ens(
    pool: tidx::db::Pool,
) -> impl Service<Request<Body>, Response = axum::response::Response, Error = std::convert::Infallible>
{
    let mut pools: HashMap<u64, tidx::db::Pool> = HashMap::new();
    pools.insert(TEST_CHAIN_ID, pool);

    let mut ens_state: HashMap<u64, EnsRuntimeState> = HashMap::new();
    ens_state.insert(
        TEST_CHAIN_ID,
        EnsRuntimeState {
            config: EnsConfig {
                enabled: true,
                registry: ENS_REGISTRY_MAINNET,
                stale_after_secs: 86_400,
            },
            // `http://127.0.0.1:1` is guaranteed closed — if anything tries
            // to RPC, it fails immediately with ECONNREFUSED.
            rpc: RpcClient::with_concurrency("http://127.0.0.1:1", 2),
        },
    );

    let router: Router = api::router_with_options(
        pools,
        TEST_CHAIN_ID,
        Arc::new(Broadcaster::new()),
        HashMap::<u64, ChainClickHouseConfig>::new(),
        HashMap::<u64, String>::new(),
        ens_state,
        &Default::default(),
    );
    let mut svc: IntoMakeServiceWithConnectInfo<Router, SocketAddr> =
        router.into_make_service_with_connect_info::<SocketAddr>();
    svc.call(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap()
}

/// Build a test service with ENS *not* configured for this chain — confirms
/// the `?ens=true` param is a silent no-op when `[chains.ens]` is absent.
async fn make_test_service_without_ens(
    pool: tidx::db::Pool,
) -> impl Service<Request<Body>, Response = axum::response::Response, Error = std::convert::Infallible>
{
    let mut pools: HashMap<u64, tidx::db::Pool> = HashMap::new();
    pools.insert(TEST_CHAIN_ID, pool);

    let router: Router = api::router(pools, TEST_CHAIN_ID, Arc::new(Broadcaster::new()));
    let mut svc: IntoMakeServiceWithConnectInfo<Router, SocketAddr> =
        router.into_make_service_with_connect_info::<SocketAddr>();
    svc.call(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap()
}

/// 20-byte test address used for the seeded tx's `from` field. Deterministic
/// so the assertion on the response JSON is stable across runs.
const FROM_ADDR: [u8; 20] = [
    0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00,
    0x11, 0x22, 0x33, 0x44,
];

/// Seed one block + one tx directly (no Tempo node, no sync engine).
/// Keeps the ENS tests self-contained and fast.
async fn seed_one_tx(pool: &tidx::db::Pool) {
    let conn = pool.get().await.unwrap();
    conn.batch_execute(
        "TRUNCATE blocks, txs, logs, receipts, ens_records RESTART IDENTITY CASCADE",
    )
    .await
    .unwrap();

    let block_hash: Vec<u8> = vec![0xbb; 32];
    let parent_hash: Vec<u8> = vec![0xaa; 32];
    let tx_hash: Vec<u8> = vec![0x7a; 32];
    let from = FROM_ADDR.to_vec();
    let to: Vec<u8> = vec![0x99; 20];
    let ts = chrono::Utc::now();

    conn.execute(
        r#"
        INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
        &[
            &1_i64,
            &block_hash,
            &parent_hash,
            &ts,
            &ts.timestamp_millis(),
            &30_000_000_i64,
            &21_000_i64,
            &to.as_slice(),
        ],
    )
    .await
    .unwrap();

    conn.execute(
        r#"
        INSERT INTO txs (
            block_num, block_timestamp, idx, hash, "type", "from", "to",
            value, input, gas_limit, max_fee_per_gas, max_priority_fee_per_gas,
            gas_used, nonce
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        "#,
        &[
            &1_i64,
            &ts,
            &0_i32,
            &tx_hash,
            &0_i16,
            &from.as_slice(),
            &Some(to.as_slice()),
            &"0".to_string(),
            &Vec::<u8>::new(),
            &21_000_i64,
            &"1000000000".to_string(),
            &"100000000".to_string(),
            &Some(21_000_i64),
            &0_i64,
        ],
    )
    .await
    .unwrap();
}

/// Insert a fresh `ens_records` row for `FROM_ADDR`.
async fn seed_fresh_ens_record(pool: &tidx::db::Pool, name: &str, verified: bool) {
    let conn = pool.get().await.unwrap();
    conn.execute(
        r#"
        INSERT INTO ens_records (address, name, verified, resolved_at, resolved_block)
        VALUES ($1, $2, $3, NOW(), 1)
        ON CONFLICT (address) DO UPDATE SET
            name = EXCLUDED.name,
            verified = EXCLUDED.verified,
            resolved_at = EXCLUDED.resolved_at
        "#,
        &[&FROM_ADDR.as_slice(), &name, &verified],
    )
    .await
    .expect("insert ens_records");
}

/// Helper: extract the JSON body of a 200 response.
async fn read_json(resp: axum::response::Response) -> serde_json::Value {
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
#[serial(db)]
async fn ens_enrichment_returns_cached_name_for_from_address() {
    let db = TestDb::empty().await;
    seed_one_tx(&db.pool).await;
    seed_fresh_ens_record(&db.pool, "alice.eth", true).await;
    let mut app = make_test_service_with_ens(db.pool.clone()).await;

    let resp = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/transactions?chainId=1&ens=true&limit=5")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let json = read_json(resp).await;
    assert_eq!(json["ok"], true);

    // `ens` key present on every tx when `?ens=true` is set (may be `{}`).
    let txs = json["transactions"].as_array().expect("transactions array");
    assert_eq!(txs.len(), 1, "seeded exactly one tx");
    let tx = &txs[0];
    assert!(tx.get("ens").is_some(), "missing `ens` field: {tx}");

    // `from` should carry our cached name; `to` was not cached so it must be
    // omitted from the map — the enrichment convention.
    let ens_obj = tx["ens"].as_object().expect("ens is an object");
    let from_entry = ens_obj
        .get("from")
        .expect("from key should be populated for the cached address");
    assert_eq!(from_entry["name"], "alice.eth");
    assert_eq!(from_entry["verified"], true);
    assert!(
        ens_obj.get("to").is_none(),
        "to key should be absent when the address has no cache hit: {ens_obj:?}"
    );
}

#[tokio::test]
#[serial(db)]
async fn ens_enrichment_is_silent_noop_when_chain_has_no_ens_config() {
    let db = TestDb::empty().await;
    seed_one_tx(&db.pool).await;
    // Seed a cache row to prove it's the missing config — not the missing
    // cache row — that suppresses enrichment.
    seed_fresh_ens_record(&db.pool, "should-not-appear.eth", true).await;
    let mut app = make_test_service_without_ens(db.pool.clone()).await;

    let resp = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/transactions?chainId=1&ens=true&limit=5")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let json = read_json(resp).await;
    let txs = json["transactions"].as_array().unwrap();
    for (i, tx) in txs.iter().enumerate() {
        assert!(
            tx.get("ens").is_none(),
            "tx[{i}] must omit `ens` when chain has no [chains.ens] config: {tx}"
        );
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Decoded-input enrichment tests
//
// These cover the path where `?decode=true&ens=true` reaches inside a decoded
// calldata payload (Transfer, approve, etc.) and enriches the address-typed
// arguments in place. Requires seeding the `signatures` table so the decoder
// can resolve the 4-byte selector.

const RECIPIENT_ADDR: [u8; 20] = [
    0xca, 0xfe, 0xba, 0xbe, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
    0x88, 0x99, 0xaa, 0xbb,
];

/// ABI-encode `transfer(address,uint256)` calldata:
///     selector (4) || zero-padded recipient (32) || zero-padded amount (32)
///
/// Selector is computed at test time from the canonical signature keccak
/// so if anyone edits the seed below without re-hashing, the decoder simply
/// won't match and the test will fail loudly rather than silently.
fn encode_transfer_calldata(recipient: [u8; 20], amount: u64) -> Vec<u8> {
    let sig_hash = alloy::primitives::keccak256(b"transfer(address,uint256)");
    let mut out = Vec::with_capacity(4 + 32 + 32);
    out.extend_from_slice(&sig_hash[..4]);
    let mut padded_addr = [0u8; 32];
    padded_addr[12..].copy_from_slice(&recipient);
    out.extend_from_slice(&padded_addr);
    let mut padded_amount = [0u8; 32];
    padded_amount[24..].copy_from_slice(&amount.to_be_bytes());
    out.extend_from_slice(&padded_amount);
    out
}

/// Seed a signatures row for `transfer(address,uint256)`. Needed so the
/// decoder's selector lookup returns the canonical signature.
async fn seed_transfer_signature(pool: &tidx::db::Pool) {
    let sig_hash = alloy::primitives::keccak256(b"transfer(address,uint256)");
    let conn = pool.get().await.unwrap();
    conn.execute(
        r#"
        INSERT INTO signatures (signature_hash_32, signature)
        VALUES ($1, $2)
        ON CONFLICT (signature_hash_32) DO NOTHING
        "#,
        &[&sig_hash.as_slice(), &"transfer(address,uint256)".to_string()],
    )
    .await
    .expect("insert signatures");
}

/// Replace the seeded tx's `input` with valid `transfer(addr, amount)`
/// calldata. The rest of the tx structure from `seed_one_tx` is reused.
async fn seed_one_tx_with_transfer_calldata(pool: &tidx::db::Pool) {
    seed_one_tx(pool).await;
    let conn = pool.get().await.unwrap();
    let calldata = encode_transfer_calldata(RECIPIENT_ADDR, 1_000_000);
    conn.execute(
        r#"UPDATE txs SET input = $1 WHERE idx = 0"#,
        &[&calldata.as_slice()],
    )
    .await
    .expect("update tx input with transfer calldata");
}

/// Insert a fresh `ens_records` row for any specific address. Parallel to
/// `seed_fresh_ens_record` (which is hardcoded to FROM_ADDR).
async fn seed_fresh_ens_record_for(
    pool: &tidx::db::Pool,
    addr: [u8; 20],
    name: &str,
    verified: bool,
) {
    let conn = pool.get().await.unwrap();
    conn.execute(
        r#"
        INSERT INTO ens_records (address, name, verified, resolved_at, resolved_block)
        VALUES ($1, $2, $3, NOW(), 1)
        ON CONFLICT (address) DO UPDATE SET
            name = EXCLUDED.name,
            verified = EXCLUDED.verified,
            resolved_at = EXCLUDED.resolved_at
        "#,
        &[&addr.as_slice(), &name, &verified],
    )
    .await
    .expect("insert ens_records");
}

#[tokio::test]
#[serial(db)]
async fn decoded_input_ens_populated_for_address_typed_input() {
    let db = TestDb::empty().await;
    seed_one_tx_with_transfer_calldata(&db.pool).await;
    seed_transfer_signature(&db.pool).await;
    seed_fresh_ens_record_for(&db.pool, RECIPIENT_ADDR, "alice.eth", true).await;
    let mut app = make_test_service_with_ens(db.pool.clone()).await;

    let resp = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/transactions?chainId=1&ens=true&decode=true&limit=5")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let json = read_json(resp).await;
    let tx = &json["transactions"][0];

    let decoded = &tx["decoded"];
    assert_eq!(
        decoded["signature"], "transfer(address,uint256)",
        "decoder should resolve the seeded selector: {tx}"
    );
    let inputs = decoded["inputs"].as_array().expect("decoded.inputs array");
    assert_eq!(inputs.len(), 2, "transfer has 2 params: {inputs:?}");

    // Input 0 is the recipient address — must be enriched.
    assert_eq!(inputs[0]["type"], "address");
    let ens = inputs[0]
        .get("ens")
        .expect("address input must carry `ens` when cache hit");
    assert_eq!(ens["name"], "alice.eth");
    assert_eq!(ens["verified"], true);

    // Input 1 is uint256 — must NOT carry `ens` (not an address).
    assert_eq!(inputs[1]["type"], "uint256");
    assert!(
        inputs[1].get("ens").is_none(),
        "non-address input must not carry `ens`: {:?}",
        inputs[1]
    );
}

#[tokio::test]
#[serial(db)]
async fn decoded_input_ens_absent_when_ens_not_requested() {
    // Sanity-check the inverse: decoding on, ENS cache populated, but the
    // caller didn't pass ?ens=true. The ens field must be absent from every
    // decoded input (skip_serializing_if = "Option::is_none").
    let db = TestDb::empty().await;
    seed_one_tx_with_transfer_calldata(&db.pool).await;
    seed_transfer_signature(&db.pool).await;
    seed_fresh_ens_record_for(&db.pool, RECIPIENT_ADDR, "alice.eth", true).await;
    let mut app = make_test_service_with_ens(db.pool.clone()).await;

    let resp = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/transactions?chainId=1&decode=true&limit=5")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let json = read_json(resp).await;
    let inputs = json["transactions"][0]["decoded"]["inputs"]
        .as_array()
        .expect("decoded.inputs array");
    for (i, input) in inputs.iter().enumerate() {
        assert!(
            input.get("ens").is_none(),
            "decoded.inputs[{i}] must omit `ens` without ens=true: {input}"
        );
    }
}

#[tokio::test]
#[serial(db)]
async fn ens_field_absent_without_param_even_when_configured() {
    let db = TestDb::empty().await;
    seed_one_tx(&db.pool).await;
    seed_fresh_ens_record(&db.pool, "alice.eth", true).await;
    let mut app = make_test_service_with_ens(db.pool.clone()).await;

    // Inverse of the positive test: ENS is enabled, cache has a hit, but
    // the caller didn't ask for it. `skip_serializing_if = "Option::is_none"`
    // should suppress the field entirely.
    let resp = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/transactions?chainId=1&limit=5")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let json = read_json(resp).await;
    let txs = json["transactions"].as_array().unwrap();
    for (i, tx) in txs.iter().enumerate() {
        assert!(
            tx.get("ens").is_none(),
            "tx[{i}] must omit `ens` without ens=true: {tx}"
        );
    }
}
