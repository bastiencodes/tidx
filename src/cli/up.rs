use anyhow::Result;
use clap::Args as ClapArgs;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing::info;

use ak47::api;
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
        db = %config.database_url,
        "Loaded config"
    );

    if config.metrics_port > 0 {
        let metrics_addr: SocketAddr =
            format!("{}:{}", config.bind, config.metrics_port).parse()?;
        info!(addr = %metrics_addr, "Starting Prometheus metrics server");
        PrometheusBuilder::new()
            .with_http_listener(metrics_addr)
            .install()?;
    }

    info!("Connecting to database...");
    let pool = db::create_pool(&config.database_url).await?;

    info!("Running migrations...");
    db::run_migrations(&pool).await?;

    let (shutdown_tx, _shutdown_rx) = tokio::sync::broadcast::channel(1);

    tokio::spawn({
        let shutdown_tx = shutdown_tx.clone();
        async move {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down...");
            let _ = shutdown_tx.send(());
        }
    });

    if config.port > 0 {
        let addr: SocketAddr = format!("{}:{}", config.bind, config.port).parse()?;
        let router = api::router(pool.clone());

        info!(addr = %addr, "Starting HTTP API server");

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

    // Spawn a sync engine for each chain
    let mut handles = Vec::new();

    for chain in &config.chains {
        let pool = pool.clone();
        let chain = chain.clone();
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
                    tokio::spawn(async move {
                        let state = backfill_engine.status().await.unwrap_or_default();
                        let start = state.backfill_num.unwrap_or(state.synced_num);

                        if let Err(e) = backfill_engine
                            .backfill(start, 0, batch_size, backfill_shutdown_rx)
                            .await
                        {
                            tracing::error!(error = %e, "Backfill failed");
                        }
                    });
                }
            }

            // Run forward sync
            let mut engine = SyncEngine::new(pool, &chain.rpc_url).await?;
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
