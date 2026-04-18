//! Seed the per-chain `labels_accounts` and `labels_contracts` tables from
//! the eth-labels dataset (<https://github.com/dawsbot/eth-labels>).
//!
//! Fetches HEAD of the `v1` branch's JSON exports over HTTPS, filters each
//! entry by `chainId`, and TRUNCATE+COPYs into the matching per-chain DB.

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Args as ClapArgs;
use futures::SinkExt;
use serde::Deserialize;
use std::path::PathBuf;
use tracing::{info, warn};

use tidx::config::Config;
use tidx::db;

const ACCOUNTS_URL: &str =
    "https://raw.githubusercontent.com/dawsbot/eth-labels/v1/data/json/accounts.json";
const CONTRACTS_URL: &str =
    "https://raw.githubusercontent.com/dawsbot/eth-labels/v1/data/json/tokens.json";
const SOURCE_TAG: &str = "eth-labels";

#[derive(ClapArgs)]
pub struct Args {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    pub config: PathBuf,

    /// Limit to one chain by chain_id. Default: seed every chain in the config.
    #[arg(long)]
    pub chain_id: Option<u64>,
}

#[derive(Deserialize)]
struct AccountEntry {
    address: String,
    #[serde(rename = "chainId")]
    chain_id: u64,
    label: String,
    #[serde(rename = "nameTag", default)]
    name_tag: Option<String>,
}

#[derive(Deserialize)]
struct ContractEntry {
    address: String,
    #[serde(rename = "chainId")]
    chain_id: u64,
    label: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    website: Option<String>,
    #[serde(default)]
    image: Option<String>,
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
        None => config.chains.clone(),
    };

    if chains.is_empty() {
        anyhow::bail!("No chains configured.");
    }

    info!(url = ACCOUNTS_URL, "Fetching eth-labels accounts.json");
    let accounts: Vec<AccountEntry> = fetch_json(ACCOUNTS_URL).await?;
    info!(count = accounts.len(), "Parsed accounts.json");

    info!(url = CONTRACTS_URL, "Fetching eth-labels tokens.json");
    let contracts: Vec<ContractEntry> = fetch_json(CONTRACTS_URL).await?;
    info!(count = contracts.len(), "Parsed tokens.json");

    for chain in &chains {
        info!(chain = %chain.name, chain_id = chain.chain_id, "Seeding labels");
        let pg_url = chain.resolved_pg_url()?;
        let pool = db::create_pool(&pg_url).await?;

        let (acc_rows, con_rows) = seed_chain(&pool, chain.chain_id, &accounts, &contracts)
            .await
            .with_context(|| format!("seeding labels for chain {}", chain.name))?;

        info!(
            chain = %chain.name,
            accounts = acc_rows,
            contracts = con_rows,
            "Seeded labels"
        );
    }

    info!(chains = chains.len(), "All chains seeded");
    Ok(())
}

async fn fetch_json<T: for<'de> Deserialize<'de>>(url: &str) -> Result<T> {
    let client = reqwest::Client::builder()
        .user_agent(concat!("tidx/", env!("CARGO_PKG_VERSION")))
        .build()?;
    let resp = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    let status = resp.status();
    anyhow::ensure!(status.is_success(), "{url} returned HTTP {status}");
    let bytes = resp.bytes().await?;
    let parsed: T = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing JSON from {url}"))?;
    Ok(parsed)
}

async fn seed_chain(
    pool: &db::Pool,
    chain_id: u64,
    accounts: &[AccountEntry],
    contracts: &[ContractEntry],
) -> Result<(u64, u64)> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;

    tx.batch_execute("TRUNCATE labels_accounts; TRUNCATE labels_contracts").await?;

    // accounts ---------------------------------------------------------------
    let sink = tx
        .copy_in(
            "COPY labels_accounts (address, label, name_tag, source) \
             FROM STDIN WITH (FORMAT csv)",
        )
        .await?;
    let mut sink = Box::pin(sink);

    // Dedup by (address, label): upstream has true duplicates (Case A) with
    // identical (address, label, nameTag) that would violate the composite PK.
    // Distinct labels on the same address (Case B, e.g. `0x-protocol` + `bridge`)
    // are preserved.
    let mut seen: std::collections::HashSet<([u8; 20], String)> =
        std::collections::HashSet::new();
    let mut acc_rows: u64 = 0;
    let mut acc_buf = Vec::with_capacity(64 * 1024);
    for e in accounts.iter().filter(|e| e.chain_id == chain_id) {
        let Some(addr) = parse_address_bytes(&e.address) else {
            warn!(addr = %e.address, "skipping account: invalid address");
            continue;
        };
        if !seen.insert((addr, e.label.clone())) {
            continue;
        }
        let name_tag = e.name_tag.as_deref().filter(|s| !s.is_empty()).unwrap_or(&e.label);
        write_csv_row(
            &mut acc_buf,
            &[
                &hex_prefixed_bytea(&addr),
                &e.label,
                name_tag,
                SOURCE_TAG,
            ],
        );
        acc_rows += 1;
        if acc_buf.len() >= 512 * 1024 {
            sink.send(Bytes::from(std::mem::take(&mut acc_buf))).await?;
        }
    }
    if !acc_buf.is_empty() {
        sink.send(Bytes::from(acc_buf)).await?;
    }
    sink.as_mut().finish().await?;

    // contracts --------------------------------------------------------------
    let sink = tx
        .copy_in(
            "COPY labels_contracts \
             (address, label, name_tag, name, symbol, website, image_url, source) \
             FROM STDIN WITH (FORMAT csv)",
        )
        .await?;
    let mut sink = Box::pin(sink);

    let mut seen: std::collections::HashSet<([u8; 20], String)> =
        std::collections::HashSet::new();
    let mut con_rows: u64 = 0;
    let mut con_buf = Vec::with_capacity(64 * 1024);
    for e in contracts.iter().filter(|e| e.chain_id == chain_id) {
        let Some(addr) = parse_address_bytes(&e.address) else {
            warn!(addr = %e.address, "skipping contract: invalid address");
            continue;
        };
        if !seen.insert((addr, e.label.clone())) {
            continue;
        }
        let name_tag = synth_name_tag(e);
        let image_url = e.image.as_deref().and_then(normalize_image_url);

        write_csv_row(
            &mut con_buf,
            &[
                &hex_prefixed_bytea(&addr),
                &e.label,
                &name_tag,
                e.name.as_deref().unwrap_or(""),
                e.symbol.as_deref().unwrap_or(""),
                e.website.as_deref().unwrap_or(""),
                image_url.as_deref().unwrap_or(""),
                SOURCE_TAG,
            ],
        );
        con_rows += 1;
        if con_buf.len() >= 512 * 1024 {
            sink.send(Bytes::from(std::mem::take(&mut con_buf))).await?;
        }
    }
    if !con_buf.is_empty() {
        sink.send(Bytes::from(con_buf)).await?;
    }
    sink.as_mut().finish().await?;

    tx.commit().await?;
    Ok((acc_rows, con_rows))
}

fn parse_address_bytes(addr: &str) -> Option<[u8; 20]> {
    let hex = addr.strip_prefix("0x").or_else(|| addr.strip_prefix("0X"))?;
    if hex.len() != 40 {
        return None;
    }
    let mut out = [0u8; 20];
    hex::decode_to_slice(hex, &mut out).ok()?;
    Some(out)
}

fn hex_prefixed_bytea(bytes: &[u8]) -> String {
    format!("\\x{}", hex::encode(bytes))
}

/// Synthesize a `name_tag` for a contract row. `name (symbol)` when both
/// exist, else whichever one is set, falling back to the project label.
fn synth_name_tag(e: &ContractEntry) -> String {
    match (e.name.as_deref(), e.symbol.as_deref()) {
        (Some(n), Some(s)) if !n.is_empty() && !s.is_empty() => format!("{n} ({s})"),
        (Some(n), _) if !n.is_empty() => n.to_string(),
        (_, Some(s)) if !s.is_empty() => s.to_string(),
        _ => e.label.clone(),
    }
}

/// Normalize the upstream `image` field to an absolute URL. Relative paths
/// resolve against `https://etherscan.io`. Empty/whitespace returns `None`.
fn normalize_image_url(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    if raw.starts_with("http://") || raw.starts_with("https://") {
        return Some(raw.to_string());
    }
    if raw.starts_with('/') {
        return Some(format!("https://etherscan.io{raw}"));
    }
    Some(raw.to_string())
}

/// Append one CSV record to `out`, quoting every field (simpler and safe
/// against commas/newlines without needing a parser-aware encoder).
fn write_csv_row(out: &mut Vec<u8>, fields: &[&str]) {
    for (i, f) in fields.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.push(b'"');
        for b in f.as_bytes() {
            if *b == b'"' {
                out.extend_from_slice(b"\"\"");
            } else {
                out.push(*b);
            }
        }
        out.push(b'"');
    }
    out.push(b'\n');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_absolute_passes_through() {
        assert_eq!(
            normalize_image_url("https://etherscan.io/token/images/x.png"),
            Some("https://etherscan.io/token/images/x.png".to_string())
        );
    }

    #[test]
    fn normalize_relative_gets_etherscan_prefix() {
        assert_eq!(
            normalize_image_url("/token/images/SynthetixSNX_32.png"),
            Some("https://etherscan.io/token/images/SynthetixSNX_32.png".to_string())
        );
    }

    #[test]
    fn normalize_empty_returns_none() {
        assert_eq!(normalize_image_url(""), None);
        assert_eq!(normalize_image_url("   "), None);
    }

    #[test]
    fn name_tag_prefers_name_and_symbol() {
        let e = ContractEntry {
            address: String::new(),
            chain_id: 1,
            label: "usdc-project".into(),
            name: Some("USD Coin".into()),
            symbol: Some("USDC".into()),
            website: None,
            image: None,
        };
        assert_eq!(synth_name_tag(&e), "USD Coin (USDC)");
    }

    #[test]
    fn name_tag_falls_back_to_label() {
        let e = ContractEntry {
            address: String::new(),
            chain_id: 1,
            label: "mystery".into(),
            name: None,
            symbol: None,
            website: None,
            image: None,
        };
        assert_eq!(synth_name_tag(&e), "mystery");
    }

    #[test]
    fn csv_quoting_handles_embedded_quotes() {
        let mut out = Vec::new();
        write_csv_row(&mut out, &["a\"b", "c,d"]);
        assert_eq!(out, b"\"a\"\"b\",\"c,d\"\n");
    }
}
