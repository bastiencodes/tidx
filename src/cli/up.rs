use anyhow::Result;
use clap::Args as ClapArgs;
use std::net::SocketAddr;
use tracing::info;

use ak47::api;
use ak47::config::Config;
use ak47::db;
use ak47::sync::engine::SyncEngine;

#[derive(ClapArgs)]
pub struct Args {
    /// RPC endpoint URL
    #[arg(long, env = "AK47_RPC_URL")]
    pub rpc: String,

    /// Database URL
    #[arg(long, env = "AK47_DATABASE_URL")]
    pub db: String,

    /// HTTP API port (0 to disable)
    #[arg(long, default_value = "8080")]
    pub port: u16,

    /// HTTP API bind address
    #[arg(long, default_value = "0.0.0.0")]
    pub bind: String,
}

pub async fn run(args: Args) -> Result<()> {
    let config = Config {
        rpc_url: args.rpc,
        database_url: args.db,
    };

    info!("Connecting to database...");
    let pool = db::create_pool(&config.database_url).await?;

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

    info!("Starting sync engine...");
    let mut engine = SyncEngine::new(pool, &config.rpc_url).await?;

    engine.run(shutdown_rx).await
}
