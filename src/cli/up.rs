use anyhow::Result;
use clap::Args as ClapArgs;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use tracing::info;

use ak47::api;
use ak47::db;
use ak47::sync::engine::SyncEngine;

#[derive(ClapArgs)]
pub struct Args {
    /// Chain to index (presto, andantino, moderato)
    #[arg(long, env = "AK47_CHAIN")]
    pub chain: String,

    /// Database URL
    #[arg(long, env = "AK47_DATABASE_URL")]
    pub db: String,

    /// HTTP API port (0 to disable)
    #[arg(long, default_value = "8080")]
    pub port: u16,

    /// HTTP API bind address
    #[arg(long, default_value = "0.0.0.0")]
    pub bind: String,

    /// Metrics port (0 to disable)
    #[arg(long, default_value = "9090")]
    pub metrics_port: u16,

    /// Disable automatic backfill to genesis
    #[arg(long)]
    pub no_backfill: bool,
}

pub async fn run(args: Args) -> Result<()> {
    let chain = ak47::config::get_chain(&args.chain).ok_or_else(|| {
        anyhow::anyhow!(
            "Unknown chain '{}'. Available: presto, andantino, moderato",
            args.chain
        )
    })?;

    info!(
        chain = chain.name,
        chain_id = chain.chain_id,
        rpc = chain.rpc_url,
        "Starting ak47"
    );

    if args.metrics_port > 0 {
        let metrics_addr: SocketAddr = format!("{}:{}", args.bind, args.metrics_port).parse()?;
        info!(addr = %metrics_addr, "Starting Prometheus metrics server");
        PrometheusBuilder::new()
            .with_http_listener(metrics_addr)
            .install()?;
    }

    info!("Connecting to database...");
    let pool = db::create_pool(&args.db).await?;

    info!("Running migrations...");
    db::run_migrations(&pool).await?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

    tokio::spawn({
        let shutdown_tx = shutdown_tx.clone();
        async move {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down...");
            let _ = shutdown_tx.send(());
        }
    });

    if args.port > 0 {
        let addr: SocketAddr = format!("{}:{}", args.bind, args.port).parse()?;
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

    let engine = SyncEngine::new(pool.clone(), chain.rpc_url).await?;

    // Start backfill task if needed
    if !args.no_backfill {
        let state = engine.status().await?;
        if !state.backfill_complete() {
            info!("Starting background backfill to genesis");
            let backfill_engine = SyncEngine::new(pool, chain.rpc_url).await?;
            let backfill_shutdown = shutdown_tx.subscribe();

            tokio::spawn(async move {
                let start = if state.backfill_num.is_some() {
                    state.backfill_num.unwrap()
                } else {
                    state.synced_num
                };

                if let Err(e) = backfill_engine
                    .backfill(start, 0, 100, backfill_shutdown)
                    .await
                {
                    tracing::error!(error = %e, "Backfill failed");
                }
            });
        }
    }

    info!("Starting sync engine...");
    let mut engine = SyncEngine::new(engine.pool().clone(), chain.rpc_url).await?;
    engine.run(shutdown_rx).await
}
