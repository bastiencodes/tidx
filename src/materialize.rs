use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::broadcast;
use tracing::{debug, error, info};

use crate::broadcast::Broadcaster;
use crate::db::{refresh_materialized_views, Pool};

/// Background service that refreshes materialized views when new blocks arrive.
/// Throttles refreshes to avoid overwhelming the database.
pub struct MaterializeService {
    pool: Pool,
    broadcaster: Arc<Broadcaster>,
    /// Minimum interval between refreshes
    throttle_interval: Duration,
}

impl MaterializeService {
    pub fn new(pool: Pool, broadcaster: Arc<Broadcaster>) -> Self {
        Self {
            pool,
            broadcaster,
            throttle_interval: Duration::from_secs(5), // Default: refresh at most every 5s
        }
    }

    pub fn with_throttle_interval(mut self, interval: Duration) -> Self {
        self.throttle_interval = interval;
        self
    }

    /// Run the service until shutdown signal received
    pub async fn run(self, mut shutdown: broadcast::Receiver<()>) {
        let mut rx = self.broadcaster.subscribe();
        let mut last_refresh = Instant::now().checked_sub(self.throttle_interval).unwrap(); // Allow immediate first refresh

        info!(
            throttle_secs = self.throttle_interval.as_secs(),
            "Materialized view refresh service started"
        );

        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("Materialized view refresh service shutting down");
                    break;
                }
                result = rx.recv() => {
                    match result {
                        Ok(update) => {
                            let elapsed = last_refresh.elapsed();
                            if elapsed >= self.throttle_interval {
                                debug!(block_num = update.block_num, "Triggering materialized view refresh");
                                
                                match refresh_materialized_views(&self.pool).await {
                                    Ok(count) if count > 0 => {
                                        debug!(count, block_num = update.block_num, "Refreshed views");
                                    }
                                    Ok(_) => {} // No views to refresh
                                    Err(e) => {
                                        error!(error = %e, "Failed to refresh materialized views");
                                    }
                                }
                                
                                last_refresh = Instant::now();
                            } else {
                                debug!(
                                    block_num = update.block_num,
                                    wait_ms = self.throttle_interval.checked_sub(elapsed).unwrap().as_millis(),
                                    "Skipping refresh (throttled)"
                                );
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            debug!(skipped = n, "Lagged behind broadcaster");
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            info!("Broadcaster closed, stopping refresh service");
                            break;
                        }
                    }
                }
            }
        }
    }
}
