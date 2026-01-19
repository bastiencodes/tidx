use anyhow::Result;
use clap::Args as ClapArgs;
use std::path::PathBuf;

use ak47::config::Config;
use ak47::db;
use ak47::sync::fetcher::RpcClient;
use ak47::sync::writer::load_sync_state;

#[derive(ClapArgs)]
pub struct Args {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    pub config: PathBuf,

    /// Watch mode - continuously update status
    #[arg(long, short)]
    pub watch: bool,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

pub async fn run(args: Args) -> Result<()> {
    let config = Config::load(&args.config)?;
    let pool = db::create_pool(&config.database_url).await?;

    loop {
        if args.watch {
            print!("\x1B[2J\x1B[1;1H");
        }

        println!("AK47 Status");
        println!("═══════════════════════════════════════");

        let state = load_sync_state(&pool).await?.unwrap_or_default();

        for chain in &config.chains {
            let rpc = RpcClient::new(&chain.rpc_url);
            let live_head = rpc.latest_block_number().await.ok();

            println!();
            println!("Chain: {} ({})", chain.name, chain.chain_id);
            println!("───────────────────────────────────────");

            if state.chain_id == chain.chain_id {
                let head = live_head.unwrap_or(state.head_num);
                let lag = head.saturating_sub(state.synced_num);

                println!(
                    "  Head:       {}{}",
                    head,
                    if live_head.is_some() { " (live)" } else { "" }
                );
                println!("  Synced:     {}", state.synced_num);
                println!("  Lag:        {} blocks", lag);

                match state.backfill_num {
                    None => println!("  Backfill:   Not started"),
                    Some(0) => println!("  Backfill:   Complete"),
                    Some(n) => println!("  Backfill:   {} blocks remaining", n),
                }
            } else {
                println!("  Status:     Not syncing (chain_id mismatch in sync_state)");
                if let Some(head) = live_head {
                    println!("  Head:       {} (live)", head);
                }
            }
        }

        if !args.watch {
            break;
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    Ok(())
}
