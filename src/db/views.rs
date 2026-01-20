use anyhow::Result;
use tracing::{debug, info, warn};

use super::Pool;

/// Refresh all user-defined materialized views in the database.
/// Uses CONCURRENTLY when possible (requires unique index on view).
/// Returns the number of views refreshed.
pub async fn refresh_materialized_views(pool: &Pool) -> Result<usize> {
    let conn = pool.get().await?;

    // Find all materialized views (excluding system views)
    let rows = conn
        .query(
            r#"
            SELECT schemaname, matviewname 
            FROM pg_matviews 
            WHERE schemaname = 'public'
            ORDER BY matviewname
            "#,
            &[],
        )
        .await?;

    debug!(view_count = rows.len(), "Found materialized views to refresh");
    
    if rows.is_empty() {
        return Ok(0);
    }

    let mut refreshed = 0;
    for row in &rows {
        let schema: String = row.get(0);
        let name: String = row.get(1);
        let full_name = format!("\"{schema}\".\"{name}\"");

        // Try CONCURRENTLY first (non-blocking), fall back to regular refresh
        let result = conn
            .execute(
                &format!("REFRESH MATERIALIZED VIEW CONCURRENTLY {full_name}"),
                &[],
            )
            .await;

        match result {
            Ok(_) => {
                debug!(view = %name, "Refreshed materialized view (concurrent)");
                refreshed += 1;
            }
            Err(e) => {
                // Use Debug format to get the full error including DB message
                let err_msg = format!("{e:?}");
                debug!(view = %name, error = %err_msg, "CONCURRENTLY failed, trying blocking");
                // CONCURRENTLY requires a unique index - fall back to blocking refresh
                if err_msg.contains("unique") || err_msg.contains("concurrently") {
                    match conn
                        .execute(&format!("REFRESH MATERIALIZED VIEW {full_name}"), &[])
                        .await
                    {
                        Ok(_) => {
                            debug!(view = %name, "Refreshed materialized view (blocking)");
                            refreshed += 1;
                        }
                        Err(e) => {
                            warn!(view = %name, error = %e, "Failed to refresh materialized view");
                        }
                    }
                } else {
                    warn!(view = %name, error = %e, "Failed to refresh materialized view");
                }
            }
        }
    }

    if refreshed > 0 {
        info!(count = refreshed, "Refreshed materialized views");
    }

    Ok(refreshed)
}

/// List all materialized views in the database
pub async fn list_materialized_views(pool: &Pool) -> Result<Vec<String>> {
    let conn = pool.get().await?;

    let rows = conn
        .query(
            "SELECT matviewname FROM pg_matviews WHERE schemaname = 'public' ORDER BY matviewname",
            &[],
        )
        .await?;

    Ok(rows.iter().map(|r| r.get(0)).collect())
}
