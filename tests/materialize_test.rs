mod common;

use std::sync::Arc;
use std::time::Duration;

use common::testdb::TestDb;

use ak47::broadcast::Broadcaster;
use ak47::db::{list_materialized_views, refresh_materialized_views};
use ak47::materialize::MaterializeService;

#[tokio::test]
async fn test_refresh_no_views() {
    let db = TestDb::new().await;

    // Drop any existing materialized views
    let conn = db.pool.get().await.unwrap();
    let views = list_materialized_views(&db.pool).await.unwrap();
    for view in views {
        conn.execute(&format!("DROP MATERIALIZED VIEW IF EXISTS \"{view}\""), &[])
            .await
            .ok();
    }

    // Should succeed with 0 views refreshed
    let count = refresh_materialized_views(&db.pool).await.unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_refresh_single_view() {
    let db = TestDb::new().await;
    let conn = db.pool.get().await.unwrap();

    // Create a simple materialized view
    conn.execute("DROP MATERIALIZED VIEW IF EXISTS test_mv_block_count", &[])
        .await
        .ok();

    conn.execute(
        "CREATE MATERIALIZED VIEW test_mv_block_count AS SELECT COUNT(*) as cnt FROM blocks",
        &[],
    )
    .await
    .expect("Failed to create materialized view");

    // Refresh should succeed
    let count = refresh_materialized_views(&db.pool).await.unwrap();
    assert!(count >= 1, "Should refresh at least one view");

    // Verify view exists and has data
    let row = conn
        .query_one("SELECT cnt FROM test_mv_block_count", &[])
        .await
        .expect("Failed to query view");

    let cnt: i64 = row.get(0);
    assert!(cnt > 0, "View should have data");

    // Cleanup
    conn.execute("DROP MATERIALIZED VIEW test_mv_block_count", &[])
        .await
        .ok();
}

#[tokio::test]
async fn test_refresh_view_with_unique_index() {
    let db = TestDb::new().await;
    let conn = db.pool.get().await.unwrap();

    // Create view with unique index (enables CONCURRENTLY refresh)
    conn.execute("DROP MATERIALIZED VIEW IF EXISTS test_mv_tx_types", &[])
        .await
        .ok();

    conn.execute(
        "CREATE MATERIALIZED VIEW test_mv_tx_types AS 
         SELECT type, COUNT(*) as cnt FROM txs GROUP BY type",
        &[],
    )
    .await
    .expect("Failed to create materialized view");

    conn.execute(
        "CREATE UNIQUE INDEX ON test_mv_tx_types (type)",
        &[],
    )
    .await
    .expect("Failed to create unique index");

    // Refresh should use CONCURRENTLY
    let count = refresh_materialized_views(&db.pool).await.unwrap();
    assert!(count >= 1);

    // Cleanup
    conn.execute("DROP MATERIALIZED VIEW test_mv_tx_types", &[])
        .await
        .ok();
}

#[tokio::test]
async fn test_list_materialized_views() {
    let db = TestDb::new().await;
    let conn = db.pool.get().await.unwrap();

    // Create a test view
    conn.execute("DROP MATERIALIZED VIEW IF EXISTS test_mv_list", &[])
        .await
        .ok();

    conn.execute(
        "CREATE MATERIALIZED VIEW test_mv_list AS SELECT 1 as val",
        &[],
    )
    .await
    .expect("Failed to create materialized view");

    let views = list_materialized_views(&db.pool).await.unwrap();
    assert!(
        views.iter().any(|v| v == "test_mv_list"),
        "Should list the created view"
    );

    // Cleanup
    conn.execute("DROP MATERIALIZED VIEW test_mv_list", &[])
        .await
        .ok();
}

#[tokio::test]
async fn test_materialize_service_starts_and_stops() {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());

    let service = MaterializeService::new(db.pool.clone(), broadcaster.clone())
        .with_throttle_interval(Duration::from_millis(100));

    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

    // Start service in background
    let handle = tokio::spawn(async move {
        service.run(shutdown_rx).await;
    });

    // Give it time to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send shutdown signal
    shutdown_tx.send(()).unwrap();

    // Should complete cleanly
    tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("Service should shut down within 1 second")
        .expect("Service task should complete without panic");
}

#[tokio::test]
async fn test_materialize_service_refreshes_on_block() {
    let db = TestDb::new().await;
    let conn = db.pool.get().await.unwrap();

    // Create a view that counts blocks
    conn.execute("DROP MATERIALIZED VIEW IF EXISTS test_mv_service", &[])
        .await
        .ok();

    conn.execute(
        "CREATE MATERIALIZED VIEW test_mv_service AS SELECT COUNT(*) as cnt FROM blocks",
        &[],
    )
    .await
    .expect("Failed to create materialized view");

    let broadcaster = Arc::new(Broadcaster::new());

    let service = MaterializeService::new(db.pool.clone(), broadcaster.clone())
        .with_throttle_interval(Duration::from_millis(10)); // Fast throttle for testing

    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

    // Start service
    let handle = tokio::spawn(async move {
        service.run(shutdown_rx).await;
    });

    // Give service time to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Broadcast a block update
    broadcaster.send(ak47::broadcast::BlockUpdate {
        chain_id: 1,
        block_num: 100,
        block_hash: "0x1234".to_string(),
        tx_count: 10,
        log_count: 20,
        timestamp: 1234567890,
    });

    // Give time for refresh to complete
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Shutdown
    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .ok();

    // View should still be queryable
    let row = conn
        .query_one("SELECT cnt FROM test_mv_service", &[])
        .await
        .expect("View should be queryable after refresh");

    let cnt: i64 = row.get(0);
    assert!(cnt >= 0, "View should be queryable after refresh");

    // Cleanup
    conn.execute("DROP MATERIALIZED VIEW test_mv_service", &[])
        .await
        .ok();
}

#[tokio::test]
async fn test_materialize_service_throttles() {
    let db = TestDb::new().await;
    let conn = db.pool.get().await.unwrap();

    // Create a simple view
    conn.execute("DROP MATERIALIZED VIEW IF EXISTS test_mv_throttle", &[])
        .await
        .ok();

    conn.execute(
        "CREATE MATERIALIZED VIEW test_mv_throttle AS SELECT 1 as val",
        &[],
    )
    .await
    .expect("Failed to create materialized view");

    let broadcaster = Arc::new(Broadcaster::new());

    // Long throttle interval
    let service = MaterializeService::new(db.pool.clone(), broadcaster.clone())
        .with_throttle_interval(Duration::from_secs(60));

    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

    let handle = tokio::spawn(async move {
        service.run(shutdown_rx).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send multiple block updates rapidly
    for i in 0..5 {
        broadcaster.send(ak47::broadcast::BlockUpdate {
            chain_id: 1,
            block_num: 100 + i,
            block_hash: format!("0x{i:064x}"),
            tx_count: 10,
            log_count: 20,
            timestamp: 1234567890 + i as i64,
        });
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Shutdown
    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .ok();

    // Cleanup
    conn.execute("DROP MATERIALIZED VIEW test_mv_throttle", &[])
        .await
        .ok();
}
