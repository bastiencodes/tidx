use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::Args as ClapArgs;
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing::info;

use ak47::api;
use ak47::broadcast::Broadcaster;
use ak47::config::Config;
use ak47::db;

use ak47::sync::engine::SyncEngine;

#[derive(ClapArgs)]
pub struct Args {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    pub config: PathBuf,
}

pub async fn run(args: Args) -> Result<()> {
    let config = Config::load(&args.config)?;

    info!(
        chains = config.chains.len(),
        "Loaded config"
    );

    if config.prometheus.enabled {
        let metrics_addr: SocketAddr =
            format!("{}:{}", config.http.bind, config.prometheus.port).parse()?;
        info!(addr = %metrics_addr, "Starting Prometheus metrics server");
        PrometheusBuilder::new()
            .with_http_listener(metrics_addr)
            .install()?;
    }

    let broadcaster = Arc::new(Broadcaster::new());
    let (shutdown_tx, _shutdown_rx) = tokio::sync::broadcast::channel(1);

    tokio::spawn({
        let shutdown_tx = shutdown_tx.clone();
        async move {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down...");
            let _ = shutdown_tx.send(());
        }
    });

    // Create pools and run migrations for each chain
    let mut chain_pools = Vec::new();
    let mut pools_map = std::collections::HashMap::new();
    let mut default_chain_id = 0u64;

    for chain in &config.chains {
        info!(chain = %chain.name, db = %chain.database_url, "Connecting to database...");
        let pool = db::create_pool(&chain.database_url).await?;
        
        info!(chain = %chain.name, "Running migrations...");
        db::run_migrations(&pool).await?;
        
        if default_chain_id == 0 {
            default_chain_id = chain.chain_id;
        }
        pools_map.insert(chain.chain_id, pool.clone());
        chain_pools.push((chain.clone(), pool));
    }

    // Start HTTP API with all chain pools
    if config.http.enabled {
        if !pools_map.is_empty() {
            let addr: SocketAddr = format!("{}:{}", config.http.bind, config.http.port).parse()?;
            let router = api::router_with_admin_key(
                pools_map,
                default_chain_id,
                broadcaster.clone(),
                config.http.admin_api_key.clone(),
            );

            if config.http.admin_api_key.is_some() {
                info!(addr = %addr, "Starting HTTP API server (admin endpoints enabled)");
            } else {
                info!(addr = %addr, "Starting HTTP API server");
            }

            let listener = tokio::net::TcpListener::bind(addr).await?;
            let mut shutdown_rx_api = shutdown_tx.subscribe();

            tokio::spawn(async move {
                axum::serve(listener, router)
                    .with_graceful_shutdown(async move {
                        let _ = shutdown_rx_api.recv().await;
                    })
                    .await
                    .ok();
            });
        }
    }

    // Spawn a sync engine for each chain
    let mut handles = Vec::new();

    for (chain, pool) in chain_pools {
        let broadcaster = broadcaster.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        let backfill_shutdown_rx = shutdown_tx.subscribe();

        info!(
            chain = %chain.name,
            chain_id = chain.chain_id,
            rpc = %chain.rpc_url,
            backfill = chain.backfill,
            "Starting sync for chain"
        );

        let handle = tokio::spawn(async move {
            let engine = SyncEngine::new(pool.clone(), &chain.rpc_url).await?;

            // Start backfill if enabled and not complete
            if chain.backfill {
                let state = engine.status().await?;
                if !state.backfill_complete() {
                    info!(chain = %chain.name, "Starting background backfill");
                    let backfill_engine = SyncEngine::new(pool.clone(), &chain.rpc_url).await?;

                    let batch_size = chain.batch_size;
                    let compress_pool = pool.clone();
                    tokio::spawn(async move {
                        // Get chain head from RPC (don't rely on state which may not be set yet)
                        let head = match backfill_engine.get_head().await {
                            Ok(h) => h,
                            Err(e) => {
                                tracing::error!(error = %e, "Failed to get chain head for backfill");
                                return;
                            }
                        };

                        let state = backfill_engine.status().await.unwrap_or_default();
                        // Resume from backfill position, or start from head if fresh
                        let start = state.backfill_num.unwrap_or(head.saturating_sub(1));

                        if start == 0 {
                            info!("Backfill already complete");
                            return;
                        }

                        info!(start = start, target = 0, "Backfill starting from head");

                        match backfill_engine
                            .backfill(start, 0, batch_size, backfill_shutdown_rx)
                            .await
                        {
                            Ok(synced) if synced > 0 => {
                                info!(blocks = synced, "Backfill complete, starting compression");
                                if let Err(e) = ak47::db::compress_historical_chunks(&compress_pool).await {
                                    tracing::error!(error = %e, "Compression failed");
                                }
                            }
                            Ok(_) => {}
                            Err(e) => {
                                tracing::error!(error = %e, "Backfill failed");
                            }
                        }
                    });
                }
            }

            // Run forward sync with broadcaster for live updates
            let mut engine = SyncEngine::new(pool, &chain.rpc_url)
                .await?
                .with_broadcaster(broadcaster);
            engine.run(shutdown_rx).await
        });

        handles.push(handle);
    }

    // Wait for first engine to complete (usually due to shutdown)
    for handle in handles {
        if let Err(e) = handle.await? {
            tracing::error!(error = %e, "Sync engine failed");
        }
    }

    Ok(())
}
