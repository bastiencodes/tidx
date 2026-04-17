use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Args as ClapArgs;
use futures::SinkExt;
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tracing::info;

use tidx::config::Config;
use tidx::db;

const BUCKET_URL: &str = "s3://sourcify-production-parquet-export/v2/signatures/";
const ENDPOINT: &str = "https://storage.googleapis.com";
const DATA_DIR: &str = "/tmp/sourcify-signatures";
const STAGE_CSV: &str = "/tmp/sourcify-signatures/_signatures.csv";

#[derive(ClapArgs)]
pub struct Args {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    pub config: PathBuf,

    /// Limit to one chain by chain_id. Default: seed every chain in the config.
    #[arg(long)]
    pub chain_id: Option<u64>,

    /// Skip the parquet download + CSV conversion; reuse the staged CSV
    /// from a prior run. Useful when iterating across chains.
    #[arg(long)]
    pub skip_prepare: bool,
}

pub async fn run(args: Args) -> Result<()> {
    let config = Config::load(&args.config)?;

    let chains: Vec<_> = match args.chain_id {
        Some(id) => {
            let c = config
                .chains
                .iter()
                .find(|c| c.chain_id == id)
                .ok_or_else(|| anyhow::anyhow!("Chain ID {id} not found in config"))?;
            vec![c.clone()]
        }
        None => {
            let mut selected = Vec::new();
            for c in &config.chains {
                if c.decode {
                    selected.push(c.clone());
                } else {
                    info!(
                        chain = %c.name,
                        chain_id = c.chain_id,
                        "Skipping chain (decode=false)"
                    );
                }
            }
            selected
        }
    };

    if chains.is_empty() {
        anyhow::bail!(
            "No chains to seed. Set `decode = true` on at least one chain in the config, \
             or pass --chain-id to override."
        );
    }

    let csv_path = PathBuf::from(STAGE_CSV);

    if args.skip_prepare {
        anyhow::ensure!(
            csv_path.exists(),
            "--skip-prepare given but {} not found",
            csv_path.display()
        );
    } else {
        sync_parquet().await?;
        convert_to_csv(&csv_path).await?;
    }

    for chain in &chains {
        info!(chain = %chain.name, chain_id = chain.chain_id, "Seeding signatures");
        let pg_url = chain.resolved_pg_url()?;
        let pool = db::create_pool(&pg_url).await?;
        let rows = load_into_db(&pool, &csv_path)
            .await
            .with_context(|| format!("loading signatures into chain {}", chain.name))?;
        info!(chain = %chain.name, rows, "Seeded signatures");
    }

    info!(chains = chains.len(), "All chains seeded");
    Ok(())
}

async fn sync_parquet() -> Result<()> {
    info!(dir = DATA_DIR, "Syncing Sourcify signatures parquet");
    tokio::fs::create_dir_all(DATA_DIR).await?;
    let status = Command::new("aws")
        .args([
            "s3",
            "sync",
            BUCKET_URL,
            DATA_DIR,
            "--endpoint-url",
            ENDPOINT,
            "--no-sign-request",
            "--exclude",
            "_*",
        ])
        .status()
        .await
        .context("running `aws s3 sync` (is the aws cli installed?)")?;
    anyhow::ensure!(status.success(), "aws s3 sync exited with {status}");
    Ok(())
}

async fn convert_to_csv(csv_path: &Path) -> Result<()> {
    info!(out = %csv_path.display(), "Converting parquet → CSV via duckdb");
    // Hex-encode the bytea with '\x' prefix so Postgres accepts it on input.
    // Filter rows with embedded null bytes — Sourcify has a handful and TEXT
    // columns can't store them.
    let sql = format!(
        "COPY (\
             SELECT '\\x' || lower(hex(signature_hash_32)) AS signature_hash_32, \
                    signature \
             FROM read_parquet('{DATA_DIR}/*.parquet') \
             WHERE position(chr(0) in signature) = 0\
         ) TO '{}' (FORMAT 'csv', HEADER false);",
        csv_path.display()
    );
    let status = Command::new("duckdb")
        .args([":memory:", "-c", &sql])
        .status()
        .await
        .context("running `duckdb` (is it installed?)")?;
    anyhow::ensure!(status.success(), "duckdb exited with {status}");
    Ok(())
}

async fn load_into_db(pool: &db::Pool, csv_path: &Path) -> Result<u64> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;

    tx.batch_execute("TRUNCATE signatures").await?;

    let sink = tx
        .copy_in(
            "COPY signatures (signature_hash_32, signature) \
             FROM STDIN WITH (FORMAT csv)",
        )
        .await?;
    let mut sink = Box::pin(sink);

    let mut file = tokio::fs::File::open(csv_path)
        .await
        .with_context(|| format!("opening {}", csv_path.display()))?;
    let mut buf = vec![0u8; 8 * 1024 * 1024];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        sink.send(Bytes::copy_from_slice(&buf[..n])).await?;
    }

    let rows = sink.as_mut().finish().await?;
    tx.commit().await?;
    Ok(rows)
}
