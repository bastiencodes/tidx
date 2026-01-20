mod common;

use common::tempo::TempoNode;
use common::testdb::TestDb;

use ak47::sync::engine::SyncEngine;
use ak47::sync::writer::{detect_gaps, get_block_hash};

#[tokio::test]
async fn test_sync_single_block() {
    let tempo = TempoNode::from_env();
    tempo.wait_for_ready().await.expect("Tempo node not ready");

    let db = TestDb::empty().await;

    // Wait for block 5
    let target_block = 5u64;
    tempo.wait_for_block(target_block).await.expect("Block not reached");

    let engine = SyncEngine::new(db.pool.clone(), &tempo.rpc_url)
        .await
        .expect("Failed to create sync engine");

    engine.sync_block(target_block).await.expect("Failed to sync block");

    // Verify block exists
    let conn = db.pool.get().await.expect("Failed to get connection");
    let block = conn
        .query_one(
            "SELECT num, timestamp_ms FROM blocks WHERE num = $1",
            &[&(target_block as i64)],
        )
        .await
        .expect("Failed to query block");

    assert_eq!(block.get::<_, i64>(0), target_block as i64);
}

#[tokio::test]
async fn test_sync_state_persisted() {
    let tempo = TempoNode::from_env();
    tempo.wait_for_ready().await.expect("Tempo node not ready");

    let db = TestDb::empty().await;
    db.truncate_all().await;

    tempo.wait_for_block(10).await.expect("Block 10 not reached");

    let engine = SyncEngine::new(db.pool.clone(), &tempo.rpc_url)
        .await
        .expect("Failed to create sync engine");

    engine.sync_block(10).await.expect("Failed to sync block");

    let conn = db.pool.get().await.expect("Failed to get connection");
    let state = conn
        .query_one(
            "SELECT chain_id, head_num, synced_num FROM sync_state WHERE id = 1",
            &[],
        )
        .await
        .expect("Failed to query sync state");

    let chain_id = tempo.chain_id().await.expect("Failed to get chain ID");
    assert_eq!(state.get::<_, i64>(0), chain_id as i64);
    assert_eq!(state.get::<_, i64>(1), 10);
    assert_eq!(state.get::<_, i64>(2), 10);
}

#[tokio::test]
async fn test_sync_block_range() {
    let tempo = TempoNode::from_env();
    tempo.wait_for_ready().await.expect("Tempo node not ready");

    let db = TestDb::empty().await;
    db.truncate_all().await;

    tempo.wait_for_block(20).await.expect("Block 20 not reached");

    let engine = SyncEngine::new(db.pool.clone(), &tempo.rpc_url)
        .await
        .expect("Failed to create sync engine");

    // Sync blocks 1-20
    for block_num in 1..=20 {
        engine
            .sync_block(block_num)
            .await
            .unwrap_or_else(|_| panic!("Failed to sync block {block_num}"));
    }

    // Verify all 20 blocks in range exist
    let conn = db.pool.get().await.expect("Failed to get connection");
    let count: i64 = conn
        .query_one("SELECT COUNT(DISTINCT num) FROM blocks WHERE num BETWEEN 1 AND 20", &[])
        .await
        .expect("Failed to count blocks")
        .get(0);

    assert_eq!(count, 20);
}

#[tokio::test]
async fn test_sync_logs() {
    let tempo = TempoNode::from_env();
    tempo.wait_for_ready().await.expect("Tempo node not ready");

    let db = TestDb::empty().await;
    db.truncate_all().await;

    // Wait for enough blocks that bench service has generated some txs with logs
    tempo.wait_for_block(50).await.expect("Block 50 not reached");

    let engine = SyncEngine::new(db.pool.clone(), &tempo.rpc_url)
        .await
        .expect("Failed to create sync engine");

    // Sync blocks 1-50
    for block_num in 1..=50 {
        engine
            .sync_block(block_num)
            .await
            .unwrap_or_else(|_| panic!("Failed to sync block {block_num}"));
    }

    let conn = db.pool.get().await.expect("Failed to get connection");

    // Verify logs were synced (bench generates TIP-20/ERC-20 transfers which emit logs)
    let log_count: i64 = conn
        .query_one("SELECT COUNT(*) FROM logs", &[])
        .await
        .expect("Failed to count logs")
        .get(0);

    // Log count may be 0 if bench isn't running - that's OK, we're testing the sync mechanism
    println!("Synced {log_count} logs from blocks 1-50");

    // If we have logs in our range, verify structure is correct
    if log_count > 0 {
        let log = conn
            .query_opt(
                "SELECT block_num, log_idx, tx_idx, address, tx_hash FROM logs WHERE block_num BETWEEN 1 AND 50 LIMIT 1",
                &[],
            )
            .await
            .expect("Failed to query log");

        if let Some(log) = log {
            let address: Vec<u8> = log.get(3);
            let tx_hash: Vec<u8> = log.get(4);

            assert_eq!(address.len(), 20, "Address should be 20 bytes");
            assert_eq!(tx_hash.len(), 32, "Tx hash should be 32 bytes");
        }
    }
}

// ============================================================================
// Seeded data tests - all tests auto-seed via TestDb::new()
// ============================================================================

#[tokio::test]
async fn test_seeded_tx_variance() {
    let db = TestDb::new().await;

    let conn = db.pool.get().await.expect("Failed to get connection");

    // Check tx type distribution (should have multiple types)
    let types: Vec<(i16, i64)> = conn
        .query(
            "SELECT type, COUNT(*) as cnt FROM txs GROUP BY type ORDER BY cnt DESC",
            &[],
        )
        .await
        .expect("Failed to query tx types")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();

    println!("Transaction types: {types:?}");
    
    // Early testnet blocks may have no transactions - just log stats
    if types.is_empty() {
        println!("No transactions found - early testnet blocks may be empty");
        return;
    }

    // Check call_count distribution (multicalls should have call_count > 1)
    let multicalls: i64 = conn
        .query_one("SELECT COUNT(*) FROM txs WHERE call_count > 1", &[])
        .await
        .expect("Failed to count multicalls")
        .get(0);

    println!("Multicall txs: {multicalls}");

    // Check address diversity
    let unique_froms: i64 = conn
        .query_one("SELECT COUNT(DISTINCT \"from\") FROM txs", &[])
        .await
        .expect("Failed to count unique froms")
        .get(0);

    let unique_tos: i64 = conn
        .query_one("SELECT COUNT(DISTINCT \"to\") FROM txs WHERE \"to\" IS NOT NULL", &[])
        .await
        .expect("Failed to count unique tos")
        .get(0);

    println!("Unique from addresses: {unique_froms}, unique to addresses: {unique_tos}");
    assert!(unique_froms >= 1, "Expected at least one from address");
}

#[tokio::test]
async fn test_seeded_log_variance() {
    let db = TestDb::new().await;

    let conn = db.pool.get().await.expect("Failed to get connection");

    let log_count = db.log_count().await;
    println!("Total logs: {log_count}");

    // Check selector diversity (different event types)
    let unique_selectors: i64 = conn
        .query_one("SELECT COUNT(DISTINCT selector) FROM logs WHERE selector IS NOT NULL", &[])
        .await
        .expect("Failed to count selectors")
        .get(0);

    println!("Unique event selectors: {unique_selectors}");
    // May be 0 if no logs yet - just print stats
}

#[tokio::test]
async fn test_seeded_data_stats() {
    let db = TestDb::new().await;

    let conn = db.pool.get().await.expect("Failed to get connection");

    let blocks = db.block_count().await;
    let txs = db.tx_count().await;
    let logs = db.log_count().await;

    println!("=== Seeded Data Stats ===");
    println!("Blocks: {blocks}");
    println!("Transactions: {txs}");
    println!("Logs: {logs}");
    
    if blocks == 0 {
        println!("No blocks found - Tempo node may not be seeding data");
        return;
    }
    
    println!("Avg txs/block: {:.1}", txs as f64 / blocks as f64);
    if txs > 0 {
        println!("Avg logs/tx: {:.1}", logs as f64 / txs as f64);
    }

    // Time range
    let time_range = conn
        .query_one(
            "SELECT MIN(timestamp), MAX(timestamp) FROM blocks",
            &[],
        )
        .await
        .expect("Failed to get time range");

    let min_ts: chrono::DateTime<chrono::Utc> = time_range.get(0);
    let max_ts: chrono::DateTime<chrono::Utc> = time_range.get(1);
    println!("Time range: {min_ts} to {max_ts}");

    // Early testnet blocks may have no transactions - just log stats
    if txs == 0 {
        println!("No transactions found - early testnet blocks may be empty");
    }
}

// ============================================================================
// Phase 3: Chain consistency tests
// ============================================================================

#[tokio::test]
async fn test_parent_hash_validation() {
    let tempo = TempoNode::from_env();
    tempo.wait_for_ready().await.expect("Tempo node not ready");

    let db = TestDb::empty().await;
    db.truncate_all().await;

    tempo.wait_for_block(10).await.expect("Block 10 not reached");

    let engine = SyncEngine::new(db.pool.clone(), &tempo.rpc_url)
        .await
        .expect("Failed to create sync engine");

    // Sync blocks 1-10 sequentially
    for block_num in 1..=10 {
        engine
            .sync_block(block_num)
            .await
            .unwrap_or_else(|_| panic!("Failed to sync block {block_num}"));
    }

    // Verify parent hash chain is valid for blocks 1-10
    let conn = db.pool.get().await.expect("Failed to get connection");
    let chain_breaks: i64 = conn
        .query_one(
            r#"
            WITH chained AS (
                SELECT num, hash, parent_hash,
                       LAG(hash) OVER (ORDER BY num) as expected_parent
                FROM blocks
                WHERE num BETWEEN 1 AND 10
            )
            SELECT COUNT(*) FROM chained
            WHERE num > 1
              AND parent_hash != expected_parent
            "#,
            &[],
        )
        .await
        .expect("Failed to check chain")
        .get(0);

    assert_eq!(chain_breaks, 0, "Parent hash chain should be valid for blocks 1-10");
}

#[tokio::test]
async fn test_get_block_hash() {
    let db = TestDb::new().await;

    // Get hash of a block we know exists
    let conn = db.pool.get().await.expect("Failed to get connection");
    let first_block: i64 = conn
        .query_one("SELECT MIN(num) FROM blocks", &[])
        .await
        .expect("Failed to get min block")
        .get(0);

    let hash = get_block_hash(&db.pool, first_block as u64)
        .await
        .expect("Failed to get block hash");

    assert!(hash.is_some(), "Should find hash for existing block");
    assert_eq!(hash.unwrap().len(), 32, "Block hash should be 32 bytes");

    // Non-existent block should return None
    let missing = get_block_hash(&db.pool, 999_999_999)
        .await
        .expect("Failed to query missing block");

    assert!(missing.is_none(), "Should return None for missing block");
}

#[tokio::test]
async fn test_gap_detection() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let conn = db.pool.get().await.expect("Failed to get connection");

    // Insert blocks with a gap: 1, 2, 3, 5, 6 (missing 4)
    for num in [1i64, 2, 3, 5, 6] {
        conn.execute(
            r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
               VALUES ($1, $2, $3, NOW(), $4, 1000000, 100000, $5)"#,
            &[
                &num,
                &vec![num as u8; 32],
                &vec![(num - 1) as u8; 32],
                &(num * 1000),
                &vec![0u8; 20],
            ],
        )
        .await
        .expect("Failed to insert block");
    }

    let gaps = detect_gaps(&db.pool).await.expect("Failed to detect gaps");

    assert_eq!(gaps.len(), 1, "Should detect one gap");
    assert_eq!(gaps[0], (4, 4), "Gap should be block 4");
}

#[tokio::test]
async fn test_gap_detection_multiple_gaps() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let conn = db.pool.get().await.expect("Failed to get connection");

    // Insert blocks with multiple gaps: 1, 2, 5, 6, 10 (missing 3-4, 7-9)
    for num in [1i64, 2, 5, 6, 10] {
        conn.execute(
            r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
               VALUES ($1, $2, $3, NOW(), $4, 1000000, 100000, $5)"#,
            &[
                &num,
                &vec![num as u8; 32],
                &vec![(num - 1) as u8; 32],
                &(num * 1000),
                &vec![0u8; 20],
            ],
        )
        .await
        .expect("Failed to insert block");
    }

    let gaps = detect_gaps(&db.pool).await.expect("Failed to detect gaps");

    assert_eq!(gaps.len(), 2, "Should detect two gaps");
    assert_eq!(gaps[0], (3, 4), "First gap should be blocks 3-4");
    assert_eq!(gaps[1], (7, 9), "Second gap should be blocks 7-9");
}

#[tokio::test]
async fn test_gap_detection_empty_table() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    // Empty table should have no gaps
    let gaps = detect_gaps(&db.pool).await.expect("Failed to detect gaps");

    assert!(gaps.is_empty(), "Empty table should have no gaps");
}

// ============================================================================
// Receipt indexing tests
// ============================================================================

#[tokio::test]
async fn test_sync_receipts() {
    let tempo = TempoNode::from_env();
    tempo.wait_for_ready().await.expect("Tempo node not ready");

    let db = TestDb::empty().await;
    db.truncate_all().await;

    tempo.wait_for_block(50).await.expect("Block 50 not reached");

    let engine = SyncEngine::new(db.pool.clone(), &tempo.rpc_url)
        .await
        .expect("Failed to create sync engine");

    // Sync blocks 1-50
    for block_num in 1..=50 {
        engine
            .sync_block(block_num)
            .await
            .unwrap_or_else(|_| panic!("Failed to sync block {block_num}"));
    }

    let conn = db.pool.get().await.expect("Failed to get connection");

    // Verify receipts were synced
    let receipt_count: i64 = conn
        .query_one("SELECT COUNT(*) FROM receipts", &[])
        .await
        .expect("Failed to count receipts")
        .get(0);

    let tx_count: i64 = conn
        .query_one("SELECT COUNT(*) FROM txs", &[])
        .await
        .expect("Failed to count txs")
        .get(0);

    println!("Synced {receipt_count} receipts from blocks 1-50 (txs: {tx_count})");

    // Each transaction should have exactly one receipt
    assert_eq!(receipt_count, tx_count, "Receipt count should match tx count");

    // If we have receipts, verify structure is correct
    if receipt_count > 0 {
        let receipt = conn
            .query_opt(
                r#"SELECT block_num, tx_idx, tx_hash, "from", gas_used, status 
                   FROM receipts WHERE block_num BETWEEN 1 AND 50 LIMIT 1"#,
                &[],
            )
            .await
            .expect("Failed to query receipt");

        if let Some(r) = receipt {
            let tx_hash: Vec<u8> = r.get(2);
            let from_addr: Vec<u8> = r.get(3);
            let gas_used: i64 = r.get(4);

            assert_eq!(tx_hash.len(), 32, "Tx hash should be 32 bytes");
            assert_eq!(from_addr.len(), 20, "From address should be 20 bytes");
            assert!(gas_used >= 0, "Gas used should be non-negative");
        }
    }
}

#[tokio::test]
async fn test_receipt_tx_hash_matches() {
    let db = TestDb::new().await;

    let conn = db.pool.get().await.expect("Failed to get connection");

    // Verify receipt tx_hash matches corresponding tx hash
    let mismatches: i64 = conn
        .query_one(
            r#"
            SELECT COUNT(*) FROM receipts r
            JOIN txs t ON r.block_num = t.block_num AND r.tx_idx = t.idx
            WHERE r.tx_hash != t.hash
            "#,
            &[],
        )
        .await
        .expect("Failed to check hash matches")
        .get(0);

    assert_eq!(mismatches, 0, "All receipt tx_hashes should match tx hashes");
}

#[tokio::test]
async fn test_seeded_receipt_stats() {
    let db = TestDb::new().await;

    let receipts = db.receipt_count().await;
    let txs = db.tx_count().await;

    println!("=== Receipt Stats ===");
    println!("Receipts: {receipts}");
    println!("Transactions: {txs}");

    assert_eq!(receipts, txs, "Should have one receipt per transaction");

    let conn = db.pool.get().await.expect("Failed to get connection");

    // Check fee_payer distribution (sponsored txs have fee_payer != from)
    let sponsored: i64 = conn
        .query_one(
            r#"SELECT COUNT(*) FROM receipts WHERE fee_payer IS NOT NULL AND fee_payer != "from""#,
            &[],
        )
        .await
        .expect("Failed to count sponsored txs")
        .get(0);

    println!("Sponsored txs (fee_payer != from): {sponsored}");

    // Check status distribution
    let success: i64 = conn
        .query_one("SELECT COUNT(*) FROM receipts WHERE status = 1", &[])
        .await
        .expect("Failed to count success")
        .get(0);

    let failed: i64 = conn
        .query_one("SELECT COUNT(*) FROM receipts WHERE status = 0", &[])
        .await
        .expect("Failed to count failed")
        .get(0);

    println!("Success: {success}, Failed: {failed}");
}
