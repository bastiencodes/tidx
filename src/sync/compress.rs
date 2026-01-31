//! Parquet compression/export for old log data
//!
//! This module exports completed block ranges from PostgreSQL to Parquet files
//! for efficient OLAP queries. The architecture:
//!
//! 1. PostgreSQL heap tables store recent data for fast writes
//! 2. This job exports old, contiguous ranges to Parquet files
//! 3. pg_duckdb + read_parquet() queries the archived data
//! 4. Query layer combines PG heap + Parquet sources via UNION ALL

use anyhow::Result;
use std::path::PathBuf;

use tokio::sync::broadcast;
use tracing::{debug, error, info};

use crate::config::ParquetExportConfig;
use crate::db::Pool;

/// Tracks exported Parquet file ranges
#[derive(Debug, Clone)]
pub struct ParquetRange {
    pub chain_id: u64,
    pub start_block: u64,
    pub end_block: u64,
    pub file_path: String,
    pub row_count: u64,
    pub file_size_bytes: u64,
}

use crate::broadcast::BlockUpdate;

/// Run the Parquet export loop in background
pub async fn run_compress_loop(
    pool: Pool,
    chain_id: u64,
    config: ParquetExportConfig,
    mut shutdown: broadcast::Receiver<()>,
    mut block_updates: broadcast::Receiver<BlockUpdate>,
) -> Result<()> {
    if !config.enabled {
        debug!(chain_id = chain_id, "Parquet export disabled");
        return Ok(());
    }

    let data_dir = PathBuf::from(&config.data_dir);

    // Create chain-specific directory
    let chain_dir = data_dir.join(chain_id.to_string());
    if let Err(e) = std::fs::create_dir_all(&chain_dir) {
        error!(error = %e, path = %chain_dir.display(), "Failed to create parquet directory");
        return Err(e.into());
    }

    info!(
        chain_id = chain_id,
        threshold = config.threshold_blocks,
        data_dir = %chain_dir.display(),
        "Starting Parquet export loop"
    );

    // Ensure parquet_ranges table exists
    create_parquet_ranges_table(&pool).await?;

    // Export any existing backlog immediately on startup
    loop {
        match tick_compress(&pool, chain_id, &config, &chain_dir).await {
            Ok(true) => {
                tokio::task::yield_now().await;
            }
            Ok(false) => break,
            Err(e) => {
                error!(error = %e, chain_id = chain_id, "Parquet export tick failed");
                break;
            }
        }
    }

    // Then wait for new blocks
    loop {
        tokio::select! {
            biased;

            _ = shutdown.recv() => {
                info!("Parquet export: shutting down");
                break;
            }

            result = block_updates.recv() => {
                match result {
                    Ok(update) if update.chain_id == chain_id => {
                        // New block synced, try to export
                        loop {
                            match tick_compress(&pool, chain_id, &config, &chain_dir).await {
                                Ok(true) => {
                                    tokio::task::yield_now().await;
                                }
                                Ok(false) => break,
                                Err(e) => {
                                    error!(error = %e, chain_id = chain_id, "Parquet export tick failed");
                                    break;
                                }
                            }
                        }
                    }
                    Ok(_) => {} // Different chain
                    Err(broadcast::error::RecvError::Lagged(_)) => {} // Missed some, will catch up
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    Ok(())
}

/// Create the parquet_ranges tracking table if it doesn't exist
async fn create_parquet_ranges_table(pool: &Pool) -> Result<()> {
    let conn = pool.get().await?;
    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS parquet_ranges (
            id SERIAL PRIMARY KEY,
            chain_id BIGINT NOT NULL,
            start_block BIGINT NOT NULL,
            end_block BIGINT NOT NULL,
            file_path TEXT NOT NULL,
            row_count BIGINT NOT NULL DEFAULT 0,
            file_size_bytes BIGINT NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (chain_id, start_block, end_block)
        )
        "#,
        &[],
    )
    .await?;

    // Index for efficient range lookups
    conn.execute(
        r#"
        CREATE INDEX IF NOT EXISTS idx_parquet_ranges_chain_blocks 
        ON parquet_ranges (chain_id, start_block, end_block)
        "#,
        &[],
    )
    .await?;

    Ok(())
}

/// Check for exportable ranges and export to Parquet
/// Returns true if a batch was exported, false if nothing to export
async fn tick_compress(
    pool: &Pool,
    chain_id: u64,
    config: &ParquetExportConfig,
    data_dir: &PathBuf,
) -> Result<bool> {
    let conn = pool.get().await?;

    // Get current tip (highest synced block)
    let tip_row = conn
        .query_opt(
            "SELECT tip_num FROM sync_state WHERE chain_id = $1",
            &[&(chain_id as i64)],
        )
        .await?;

    let tip_num: i64 = match tip_row {
        Some(row) => row.get(0),
        None => {
            debug!(chain_id = chain_id, "No sync state found, skipping export");
            return Ok(false);
        }
    };

    // Find the highest block already exported
    let last_exported = get_last_exported_block(pool, chain_id).await?;

    // Find contiguous range from last_exported to tip
    let range = find_contiguous_range(pool, chain_id, last_exported, tip_num as u64).await?;

    let (start_block, end_block) = match range {
        Some((s, e)) if e - s + 1 >= config.threshold_blocks => (s, e),
        Some((s, e)) => {
            debug!(
                chain_id = chain_id,
                start = s,
                end = e,
                blocks = e - s + 1,
                threshold = config.threshold_blocks,
                "Range too small for export"
            );
            return Ok(false);
        }
        None => {
            debug!(chain_id = chain_id, "No contiguous range found for export");
            return Ok(false);
        }
    };

    info!(
        chain_id = chain_id,
        start = start_block,
        end = end_block,
        blocks = end_block - start_block + 1,
        "Exporting logs to Parquet"
    );

    // Export to Parquet
    let file_path = data_dir.join(format!("logs_{}_{}.parquet", start_block, end_block));
    let (row_count, file_size) =
        export_logs_to_parquet(pool, start_block, end_block, &file_path).await?;

    // Record the exported range
    record_parquet_range(
        pool,
        chain_id,
        start_block,
        end_block,
        file_path.to_string_lossy().as_ref(),
        row_count,
        file_size,
    )
    .await?;

    info!(
        chain_id = chain_id,
        start = start_block,
        end = end_block,
        row_count = row_count,
        file_size_mb = file_size / 1024 / 1024,
        path = %file_path.display(),
        "Parquet export complete"
    );

    Ok(true)
}

/// Get the highest block number already exported to Parquet
async fn get_last_exported_block(pool: &Pool, chain_id: u64) -> Result<u64> {
    let conn = pool.get().await?;
    let row = conn
        .query_opt(
            "SELECT COALESCE(MAX(end_block), 0) FROM parquet_ranges WHERE chain_id = $1",
            &[&(chain_id as i64)],
        )
        .await?;

    match row {
        Some(r) => Ok(r.get::<_, i64>(0) as u64),
        None => Ok(0),
    }
}

/// Find a contiguous range of blocks from start to cutoff
/// Returns None if there are gaps in the range
async fn find_contiguous_range(
    pool: &Pool,
    _chain_id: u64,
    after_block: u64,
    cutoff: u64,
) -> Result<Option<(u64, u64)>> {
    let conn = pool.get().await?;

    // Start from after_block + 1 (or 1 if no prior exports, since block 0 doesn't exist)
    let start = if after_block == 0 { 1 } else { after_block + 1 };

    if start >= cutoff {
        return Ok(None);
    }

    // Check if we have all blocks in the range
    // Use a CTE to find the first gap
    let gap_row = conn
        .query_opt(
            r#"
            WITH expected AS (
                SELECT generate_series($1::bigint, $2::bigint) AS num
            ),
            existing AS (
                SELECT DISTINCT num FROM blocks 
                WHERE num >= $1 AND num <= $2
            )
            SELECT MIN(e.num) as first_gap
            FROM expected e
            LEFT JOIN existing b ON e.num = b.num
            WHERE b.num IS NULL
            "#,
            &[&(start as i64), &(cutoff as i64)],
        )
        .await?;

    let end_block = match gap_row {
        Some(row) => {
            let first_gap: Option<i64> = row.get(0);
            match first_gap {
                Some(gap) if gap > start as i64 => (gap - 1) as u64, // Range ends before gap
                Some(_) => return Ok(None),                          // Gap at start
                None => cutoff, // No gaps, full range available
            }
        }
        None => cutoff,
    };

    // Verify we actually have the start block
    let has_start = conn
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM blocks WHERE num = $1)",
            &[&(start as i64)],
        )
        .await?;

    if !has_start.get::<_, bool>(0) {
        return Ok(None);
    }

    Ok(Some((start, end_block)))
}

/// Export logs from PostgreSQL to Parquet using DuckDB's COPY (via pg_duckdb)
///
/// pg_duckdb intercepts PostgreSQL's COPY command when the format is 'parquet'
/// and routes it through DuckDB, which can write Parquet files directly.
async fn export_logs_to_parquet(
    pool: &Pool,
    start_block: u64,
    end_block: u64,
    file_path: &PathBuf,
) -> Result<(u64, u64)> {
    let conn = pool.get().await?;
    let path_str = file_path.to_string_lossy();

    // Escape single quotes in path for SQL
    let escaped_path = path_str.replace('\'', "''");

    // Use pg_duckdb's COPY TO syntax with parquet format
    // pg_duckdb intercepts COPY commands with FORMAT 'parquet' and routes them through DuckDB
    // The WITH (...) syntax is PostgreSQL-standard and works with pg_duckdb
    let copy_sql = format!(
        "COPY (SELECT block_num, tx_idx, log_idx, tx_hash, address, \
         topic0, topic1, topic2, topic3, data FROM logs \
         WHERE block_num >= {} AND block_num <= {} \
         ORDER BY block_num, log_idx) TO '{}' WITH (FORMAT 'parquet', COMPRESSION 'zstd')",
        start_block, end_block, escaped_path
    );

    // Try the standard COPY approach first (works when pg_duckdb intercepts it)
    match conn.execute(&copy_sql, &[]).await {
        Ok(_) => {
            debug!(path = %path_str, "COPY TO parquet succeeded");
        }
        Err(e) => {
            // If standard COPY fails (e.g., PostgreSQL doesn't recognize parquet format),
            // fall back to using duckdb.raw_query() with explicit table reference
            debug!(error = %e, "Standard COPY failed, trying raw_query fallback");

            // Use duckdb.raw_query() to execute DuckDB's native COPY command
            // DuckDB inside pg_duckdb can see PostgreSQL tables when accessed this way
            let duckdb_query = format!(
                "COPY (SELECT block_num, tx_idx, log_idx, tx_hash, address, \
                 topic0, topic1, topic2, topic3, data FROM logs \
                 WHERE block_num >= {} AND block_num <= {} \
                 ORDER BY block_num, log_idx) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD)",
                start_block, end_block, escaped_path
            );

            conn.execute("SELECT duckdb.raw_query($1)", &[&duckdb_query])
                .await
                .map_err(|e2| {
                    error!(
                        original_error = %e,
                        raw_query_error = %e2,
                        "Both COPY and raw_query failed for Parquet export"
                    );
                    e2
                })?;
        }
    }

    // Get file size and row count from parquet metadata
    let file_size = std::fs::metadata(file_path)
        .map(|m| m.len())
        .unwrap_or(0);

    // Read row count from parquet file footer (avoids extra COUNT query)
    let row_count = read_parquet_row_count(file_path).unwrap_or(0);

    Ok((row_count, file_size))
}

/// Read row count from parquet file metadata (footer)
fn read_parquet_row_count(file_path: &PathBuf) -> Result<u64> {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use std::fs::File;

    let file = File::open(file_path)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();
    Ok(metadata.file_metadata().num_rows() as u64)
}

/// Record the exported range in the tracking table
async fn record_parquet_range(
    pool: &Pool,
    chain_id: u64,
    start_block: u64,
    end_block: u64,
    file_path: &str,
    row_count: u64,
    file_size: u64,
) -> Result<()> {
    let conn = pool.get().await?;
    conn.execute(
        r#"
        INSERT INTO parquet_ranges (chain_id, start_block, end_block, file_path, row_count, file_size_bytes)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (chain_id, start_block, end_block) DO UPDATE SET
            file_path = EXCLUDED.file_path,
            row_count = EXCLUDED.row_count,
            file_size_bytes = EXCLUDED.file_size_bytes
        "#,
        &[
            &(chain_id as i64),
            &(start_block as i64),
            &(end_block as i64),
            &file_path,
            &(row_count as i64),
            &(file_size as i64),
        ],
    )
    .await?;

    Ok(())
}

/// Get all Parquet ranges for a chain (for query layer)
pub async fn get_parquet_ranges(pool: &Pool, chain_id: u64) -> Result<Vec<ParquetRange>> {
    let conn = pool.get().await?;
    let rows = conn
        .query(
            r#"
            SELECT chain_id, start_block, end_block, file_path, row_count, file_size_bytes
            FROM parquet_ranges
            WHERE chain_id = $1
            ORDER BY start_block
            "#,
            &[&(chain_id as i64)],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|row| ParquetRange {
            chain_id: row.get::<_, i64>(0) as u64,
            start_block: row.get::<_, i64>(1) as u64,
            end_block: row.get::<_, i64>(2) as u64,
            file_path: row.get(3),
            row_count: row.get::<_, i64>(4) as u64,
            file_size_bytes: row.get::<_, i64>(5) as u64,
        })
        .collect())
}

/// Get the maximum block number stored in Parquet (for query routing)
pub async fn get_max_parquet_block(pool: &Pool, chain_id: u64) -> Result<Option<u64>> {
    let conn = pool.get().await?;
    let row = conn
        .query_opt(
            "SELECT MAX(end_block) FROM parquet_ranges WHERE chain_id = $1",
            &[&(chain_id as i64)],
        )
        .await?;

    match row {
        Some(r) => Ok(r.get::<_, Option<i64>>(0).map(|v| v as u64)),
        None => Ok(None),
    }
}
