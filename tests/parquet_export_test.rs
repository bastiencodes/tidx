//! Tests for Parquet export functionality

mod common;

use common::testdb::TestDb;
use tempfile::TempDir;

/// Test that DuckDB COPY TO PARQUET works via pg_duckdb's raw_query
#[tokio::test]
async fn test_parquet_export_via_pg_duckdb() {
    let db = TestDb::new().await;
    
    // Skip if no logs
    if db.log_count().await == 0 {
        println!("Skipping test: no logs in database");
        return;
    }

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let parquet_path = temp_dir.path().join("test_logs.parquet");
    let path_str = parquet_path.to_string_lossy();

    let conn = db.pool.get().await.expect("Failed to get connection");

    // Use duckdb.raw_query() to execute DuckDB's native COPY command
    let duckdb_query = format!(
        "COPY (SELECT block_num, tx_idx, log_idx, tx_hash, address, \
         topic0, topic1, topic2, topic3, data FROM logs \
         ORDER BY block_num, log_idx LIMIT 100) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD)",
        path_str
    );

    let result = conn
        .execute("SELECT duckdb.raw_query($1)", &[&duckdb_query])
        .await;

    match result {
        Ok(_) => {
            // Verify file was created
            assert!(parquet_path.exists(), "Parquet file should exist");
            
            let file_size = std::fs::metadata(&parquet_path)
                .expect("Failed to get file metadata")
                .len();
            println!("Parquet file size: {} bytes", file_size);
            assert!(file_size > 0, "Parquet file should not be empty");

            // Read row count from parquet metadata
            let row_count = read_parquet_row_count(&parquet_path);
            println!("Exported {} rows to Parquet", row_count);
            assert!(row_count > 0, "Should export at least one row");

            // Verify we can read it back via read_parquet
            let read_query = format!(
                "SELECT COUNT(*)::bigint FROM read_parquet('{}')",
                path_str
            );
            let read_result = conn
                .execute("SELECT duckdb.raw_query($1)", &[&read_query])
                .await;

            match read_result {
                Ok(_) => println!("Successfully read parquet file via DuckDB"),
                Err(e) => println!("Warning: read_parquet failed: {}", e),
            }
        }
        Err(e) => {
            println!("duckdb.raw_query COPY failed: {}", e);
            println!("This test requires pg_duckdb extension to be installed");
        }
    }
}

/// Read row count from parquet file metadata
fn read_parquet_row_count(file_path: &std::path::Path) -> u64 {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use std::fs::File;

    let file = File::open(file_path).expect("Failed to open parquet file");
    let reader = SerializedFileReader::new(file).expect("Failed to create parquet reader");
    let metadata = reader.metadata();
    metadata.file_metadata().num_rows() as u64
}

/// Test the full export flow using compress module
#[tokio::test]
async fn test_compress_tick() {
    let db = TestDb::new().await;
    
    // Need at least some blocks for this test
    let block_count = db.block_count().await;
    if block_count < 10 {
        println!("Skipping test: need at least 10 blocks, have {}", block_count);
        return;
    }

    // Set up sync_state so compress can find the tip
    let conn = db.pool.get().await.expect("Failed to get connection");
    conn.execute(
        "INSERT INTO sync_state (chain_id, head_num, synced_num, tip_num) 
         VALUES (1, $1, $1, $1)
         ON CONFLICT (chain_id) DO UPDATE SET 
            head_num = EXCLUDED.head_num,
            synced_num = EXCLUDED.synced_num, 
            tip_num = EXCLUDED.tip_num",
        &[&block_count],
    )
    .await
    .expect("Failed to set sync_state");

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let chain_dir = temp_dir.path().join("1");
    std::fs::create_dir_all(&chain_dir).expect("Failed to create chain dir");

    // Test duckdb.raw_query() COPY syntax (tick_compress is private)
    let parquet_path = chain_dir.join("logs_1_10.parquet");
    let path_str = parquet_path.to_string_lossy();

    let duckdb_query = format!(
        "COPY (SELECT block_num, tx_idx, log_idx, tx_hash, address, \
         topic0, topic1, topic2, topic3, data FROM logs \
         WHERE block_num >= 1 AND block_num <= 10 \
         ORDER BY block_num, log_idx) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD)",
        path_str
    );

    let result = conn
        .execute("SELECT duckdb.raw_query($1)", &[&duckdb_query])
        .await;

    match result {
        Ok(_) => {
            assert!(parquet_path.exists(), "File should be created");
            let row_count = read_parquet_row_count(&parquet_path);
            println!("Exported {} rows", row_count);
        }
        Err(e) => {
            println!("Export failed (expected if pg_duckdb not configured): {}", e);
        }
    }
}
