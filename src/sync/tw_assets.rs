//! Trust Wallet token-list worker.
//!
//! Mirrors the curated metadata that `trustwallet/assets` publishes for
//! each listed ERC20 (name, website, description, explorer, status, tags,
//! links, …) into the shared `token_list` table (under
//! `source = 'trust_wallet'`) so it can be joined into the `/erc20/tokens`
//! response alongside the on-chain metadata resolved by
//! `erc20_metadata.rs`.
//!
//! # Fetch strategy
//!
//! Two phases, driven by GitHub's Git Trees API to minimise redundant
//! network work:
//!
//! 1. **Tree refresh** — one call to
//!    `api.github.com/repos/trustwallet/assets/git/trees/master?recursive=1`
//!    returns the entire repo tree (~16 MB, un-truncated, un-authenticated)
//!    along with a Git blob SHA per `info.json`. We filter to this chain's
//!    slug and cache the result in-memory as `address → source_sha`.
//!
//! 2. **Selective fetch** — intersect our `erc20_tokens` rows with the
//!    cached tree and, for each match whose stored `source_sha` differs from
//!    the upstream sha (or has no row yet), fetch just that `info.json`
//!    from `raw.githubusercontent.com` and upsert the parsed fields.
//!    Addresses present in our table but absent from the tree are deleted.
//!
//! Both phases run each tick; the tick cadence is configurable per
//! `[metadata]` in `config.toml` and defaults to 24h.
//!
//! # Chain support
//!
//! Only chains listed in [`TW_CHAIN_SLUGS`] run. Unknown chain IDs cause
//! the worker to log once and exit cleanly (cheaper than leaving a ticking
//! no-op).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use alloy::primitives::Address;
use anyhow::{Result, anyhow};
use futures::stream::{self, StreamExt};
use serde::Deserialize;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use crate::db::Pool;

/// Default tick interval when `[metadata]` `tw_tick_secs` isn't set.
/// 24h is a good match for how slowly upstream changes — both the tree
/// structure and individual info.json blobs typically go weeks without
/// meaningful edits.
pub const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

/// Ceiling for the exponential-backoff delay after consecutive tick
/// failures. Truncated at 1 hour so a recovered upstream is retried
/// reasonably soon even after an outage.
pub const MAX_BACKOFF_SECS: u64 = 3_600;

/// Parallelism for raw.githubusercontent.com info.json fetches. Kept low
/// both to stay under the 5k/hr unauth cap comfortably and to avoid
/// hammering a third-party CDN.
const FETCH_CONCURRENCY: usize = 4;

/// HTTP request timeout for both the tree API call and per-token info.json
/// fetches. Tree calls take a few seconds; info.json is sub-second.
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// `source` tag this worker writes to in the shared `token_list` table.
/// Part of the composite PK together with `(chain_id, address)`.
const SOURCE_NAME: &str = "trust_wallet";

/// Chain-id → Trust Wallet blockchain slug. Only chains in this table are
/// enriched; everything else (e.g. sepolia, private testnets) no-ops.
const TW_CHAIN_SLUGS: &[(u64, &str)] = &[
    (1, "ethereum"),
    // Add mappings here as new chains join the indexer. Canonical list:
    // https://github.com/trustwallet/assets/tree/master/blockchains
];

/// Returns the Trust Wallet blockchain slug for `chain_id`, or `None` if
/// Trust Wallet does not publish assets for this chain. Exposed so the API
/// layer can compose canonical logo URLs without duplicating the mapping.
pub fn slug_for(chain_id: u64) -> Option<&'static str> {
    TW_CHAIN_SLUGS
        .iter()
        .find(|(id, _)| *id == chain_id)
        .map(|(_, slug)| *slug)
}

/// Build the raw.githubusercontent.com URL for a listed token's info.json.
/// Addresses must be EIP-55 checksummed — Trust Wallet's folder names use
/// that exact casing and 404s are literal mis-matches.
fn info_json_url(slug: &str, addr: &Address) -> String {
    format!(
        "https://raw.githubusercontent.com/trustwallet/assets/master/blockchains/{}/assets/{}/info.json",
        slug,
        addr.to_checksum(None),
    )
}

/// Build the canonical `logo.png` URL for a Trust-Wallet-listed token.
/// The logo lives at the same assets folder as `info.json`, so this is
/// deterministic from (slug, EIP-55 address) without any DB lookup.
pub fn logo_url(slug: &str, addr: &Address) -> String {
    format!(
        "https://raw.githubusercontent.com/trustwallet/assets/master/blockchains/{}/assets/{}/logo.png",
        slug,
        addr.to_checksum(None),
    )
}

/// Background worker that mirrors Trust Wallet's curated metadata into
/// `token_list`.
pub struct TwAssetsWorker {
    pool: Pool,
    chain_id: u64,
    slug: &'static str,
    http: reqwest::Client,
    /// Baseline tick interval (between successful ticks). Source of truth
    /// for `compute_backoff`.
    tick_interval: Duration,

    /// In-memory cache of `address → source_sha` from the latest tree
    /// refresh. Filtered to this worker's chain slug.
    tree: Option<HashMap<[u8; 20], String>>,
}

impl TwAssetsWorker {
    /// Constructs a worker for `chain_id`. Returns `None` when the chain
    /// has no Trust Wallet slug mapping — avoids spawning a no-op task for
    /// chains Trust Wallet doesn't cover. `tick_interval` defaults to
    /// [`DEFAULT_TICK_INTERVAL`] when `None`.
    pub fn new(pool: Pool, chain_id: u64, tick_interval: Option<Duration>) -> Option<Self> {
        let slug = slug_for(chain_id)?;
        let http = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .user_agent(concat!("tidx/", env!("CARGO_PKG_VERSION")))
            .build()
            .ok()?;
        Some(Self {
            pool,
            chain_id,
            slug,
            http,
            tick_interval: tick_interval.unwrap_or(DEFAULT_TICK_INTERVAL),
            tree: None,
        })
    }

    /// Runs until the shutdown receiver fires. Each iteration refreshes
    /// the cached tree and then does a selective info.json pass.
    pub async fn run(mut self, mut shutdown: broadcast::Receiver<()>) {
        info!(
            chain_id = self.chain_id,
            slug = self.slug,
            tick_secs = self.tick_interval.as_secs(),
            "Starting token_list worker"
        );

        let mut consecutive_failures: u32 = 0;

        loop {
            match self.tick().await {
                Ok(()) => consecutive_failures = 0,
                Err(e) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    let next = compute_backoff(self.tick_interval, consecutive_failures);
                    error!(
                        chain_id = self.chain_id,
                        error = %e,
                        consecutive_failures,
                        next_retry_secs = next.as_secs(),
                        "token_list tick failed"
                    );
                }
            }

            let delay = compute_backoff(self.tick_interval, consecutive_failures);
            tokio::select! {
                _ = shutdown.recv() => {
                    info!(chain_id = self.chain_id, "Shutting down token_list worker");
                    break;
                }
                _ = tokio::time::sleep(delay) => {}
            }
        }
    }

    async fn tick(&mut self) -> Result<()> {
        self.refresh_tree().await?;
        let tree = self
            .tree
            .as_ref()
            .ok_or_else(|| anyhow!("tree cache empty after refresh"))?;

        let ours = self.load_erc20_addresses().await?;
        let stored = self.load_stored_shas().await?;

        // Phase B: upstream rows we need to fetch (new or SHA-changed).
        let to_fetch: Vec<([u8; 20], String)> = ours
            .iter()
            .filter_map(|addr| {
                let upstream = tree.get(addr)?;
                let is_stale = stored
                    .get(addr)
                    .is_none_or(|s| s != upstream);
                is_stale.then(|| (*addr, upstream.clone()))
            })
            .collect();

        // Phase C: rows we previously cached but TW no longer publishes.
        let to_delete: Vec<[u8; 20]> = stored
            .keys()
            .filter(|addr| !tree.contains_key(*addr))
            .copied()
            .collect();

        let fetched = self.fetch_and_upsert(&to_fetch).await;
        let deleted = self.delete_removed(&to_delete).await?;

        info!(
            chain_id = self.chain_id,
            listed = tree.len(),
            matched = ours.iter().filter(|a| tree.contains_key(*a)).count(),
            fetched,
            deleted,
            "token_list tick complete"
        );
        Ok(())
    }

    /// Fetches the current Trust Wallet tree and replaces the in-memory
    /// cache. Failures propagate to the tick and leave the previous cache
    /// intact so a single flaky GitHub call doesn't wipe good data.
    async fn refresh_tree(&mut self) -> Result<()> {
        let url = "https://api.github.com/repos/trustwallet/assets/git/trees/master?recursive=1";
        let start = Instant::now();
        let response = self
            .http
            .get(url)
            .header("Accept", "application/vnd.github+json")
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            return Err(anyhow!("GitHub tree API returned {}", status));
        }
        let body: GitTree = response.json().await?;
        if body.truncated {
            // The repo is close to GitHub's 100k-entry tree limit. If that
            // ever flips, we'd need to paginate with sub-tree fetches —
            // bail loudly rather than silently enriching a partial set.
            return Err(anyhow!("GitHub tree API returned truncated=true"));
        }

        let tree = parse_tree(&body.tree, self.slug);
        let duration_ms = start.elapsed().as_millis();
        info!(
            chain_id = self.chain_id,
            slug = self.slug,
            listed = tree.len(),
            duration_ms,
            "Refreshed token_list tree"
        );
        self.tree = Some(tree);
        Ok(())
    }

    /// Pulls all known ERC20 addresses from `erc20_tokens`. Cheap — the
    /// table is small (tens of thousands of rows at most) and we only
    /// need the primary key.
    async fn load_erc20_addresses(&self) -> Result<Vec<[u8; 20]>> {
        let conn = self.pool.get().await?;
        let rows = conn
            .query("SELECT address FROM erc20_tokens", &[])
            .await?;
        Ok(rows.iter().filter_map(row_to_addr_bytes).collect())
    }

    /// Returns the current `(address → source_sha)` mapping stored in
    /// `token_list` for this chain and source.
    async fn load_stored_shas(&self) -> Result<HashMap<[u8; 20], String>> {
        let conn = self.pool.get().await?;
        let rows = conn
            .query(
                "SELECT address, source_sha FROM token_list \
                 WHERE source = $1 AND chain_id = $2",
                &[&SOURCE_NAME, &(self.chain_id as i64)],
            )
            .await?;
        let mut out = HashMap::with_capacity(rows.len());
        for row in &rows {
            let Some(addr) = row_to_addr_bytes(row) else { continue };
            let sha: String = row.get(1);
            out.insert(addr, sha);
        }
        Ok(out)
    }

    /// Fetches `info.json` for each `(addr, upstream_sha)` with bounded
    /// concurrency and upserts the parsed rows. Returns the number of
    /// successful upserts; individual failures are logged and skipped.
    async fn fetch_and_upsert(&self, to_fetch: &[([u8; 20], String)]) -> usize {
        if to_fetch.is_empty() {
            return 0;
        }
        let chain_id = self.chain_id;
        let slug = self.slug;
        let http = &self.http;
        let pool = &self.pool;

        stream::iter(to_fetch.iter().cloned())
            .map(|(addr_bytes, upstream_sha)| async move {
                let addr = Address::from_slice(&addr_bytes);
                match fetch_info_json(http, slug, &addr).await {
                    Ok(parsed) => match upsert_asset(
                        pool,
                        chain_id,
                        slug,
                        &addr_bytes,
                        &upstream_sha,
                        &parsed,
                    )
                    .await
                    {
                        Ok(()) => Some(()),
                        Err(e) => {
                            warn!(
                                chain_id,
                                address = %addr,
                                error = %e,
                                "Trust Wallet upsert failed"
                            );
                            None
                        }
                    },
                    Err(e) => {
                        debug!(
                            chain_id,
                            address = %addr,
                            error = %e,
                            "Trust Wallet info.json fetch failed"
                        );
                        None
                    }
                }
            })
            .buffer_unordered(FETCH_CONCURRENCY)
            .filter_map(|opt| async move { opt })
            .count()
            .await
    }

    /// Removes stale rows for addresses no longer in the upstream tree
    /// (rare, but keeps the mirror honest).
    async fn delete_removed(&self, addresses: &[[u8; 20]]) -> Result<usize> {
        if addresses.is_empty() {
            return Ok(0);
        }
        let conn = self.pool.get().await?;
        let mut deleted = 0_usize;
        for addr in addresses {
            let affected = conn
                .execute(
                    "DELETE FROM token_list \
                     WHERE source = $1 AND chain_id = $2 AND address = $3",
                    &[&SOURCE_NAME, &(self.chain_id as i64), &addr.as_slice()],
                )
                .await?;
            deleted += affected as usize;
        }
        Ok(deleted)
    }
}

fn row_to_addr_bytes(row: &tokio_postgres::Row) -> Option<[u8; 20]> {
    let bytes: Vec<u8> = row.get(0);
    (bytes.len() == 20).then(|| {
        let mut arr = [0u8; 20];
        arr.copy_from_slice(&bytes);
        arr
    })
}

/// Fetches and parses a single token's `info.json`. Returns `Err` for
/// network errors, non-2xx responses, or malformed JSON.
async fn fetch_info_json(
    http: &reqwest::Client,
    slug: &str,
    addr: &Address,
) -> Result<InfoJson> {
    let url = info_json_url(slug, addr);
    let response = http.get(&url).send().await?;
    let status = response.status();
    if !status.is_success() {
        return Err(anyhow!("info.json {} returned {}", url, status));
    }
    let body = response.text().await?;
    let parsed: InfoJson = serde_json::from_str(&body)
        .map_err(|e| anyhow!("info.json decode failed for {}: {}", url, e))?;
    Ok(parsed)
}

/// Upsert a resolved info.json into `token_list` under
/// `source = 'trust_wallet'`. The composite PK `(source, chain_id, address)`
/// makes this idempotent across re-fetches of the same blob sha (which
/// we already filter out upstream, but the DB is the source of truth).
async fn upsert_asset(
    pool: &Pool,
    chain_id: u64,
    slug: &str,
    address: &[u8; 20],
    source_sha: &str,
    info: &InfoJson,
) -> Result<()> {
    // Compute the spec-compliant logoURI at insert-time. Deterministic
    // from (slug, EIP-55 address) so we don't need DB migrations when
    // Trust Wallet churns individual logos.
    let addr = Address::from_slice(address);
    let logo = logo_url(slug, &addr);

    let conn = pool.get().await?;
    conn.execute(
        r#"
        INSERT INTO token_list (
            source, chain_id, address, source_sha,
            name, symbol, decimals, asset_type,
            logo_uri, website, description, explorer, status,
            tags, links, fetched_at
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7, $8,
            $9, $10, $11, $12, $13,
            $14, $15, NOW()
        )
        ON CONFLICT (source, chain_id, address) DO UPDATE SET
            source_sha  = EXCLUDED.source_sha,
            name        = EXCLUDED.name,
            symbol      = EXCLUDED.symbol,
            decimals    = EXCLUDED.decimals,
            asset_type  = EXCLUDED.asset_type,
            logo_uri    = EXCLUDED.logo_uri,
            website     = EXCLUDED.website,
            description = EXCLUDED.description,
            explorer    = EXCLUDED.explorer,
            status      = EXCLUDED.status,
            tags        = EXCLUDED.tags,
            links       = EXCLUDED.links,
            fetched_at  = EXCLUDED.fetched_at
        "#,
        &[
            &SOURCE_NAME,
            &(chain_id as i64),
            &address.as_slice(),
            &source_sha,
            &info.name,
            &info.symbol,
            &info.decimals,
            &info.asset_type,
            &logo,
            &info.website,
            &info.description,
            &info.explorer,
            &info.status,
            &info.tags,
            &info.links,
        ],
    )
    .await?;
    Ok(())
}

/// Filter a recursive tree response down to this chain's info.json entries
/// and return `address → source_sha`. Any entry whose path segment under
/// `assets/` isn't a valid 20-byte `0x…` address is skipped.
fn parse_tree(entries: &[GitTreeEntry], slug: &str) -> HashMap<[u8; 20], String> {
    let prefix = format!("blockchains/{slug}/assets/");
    let suffix = "/info.json";
    let mut out = HashMap::new();
    for e in entries {
        if e.entry_type != "blob" {
            continue;
        }
        let Some(rest) = e.path.strip_prefix(&prefix) else { continue };
        let Some(addr_str) = rest.strip_suffix(suffix) else { continue };
        let Some(addr_hex) = addr_str.strip_prefix("0x") else { continue };
        let Ok(bytes) = hex::decode(addr_hex) else { continue };
        if bytes.len() != 20 {
            continue;
        }
        let mut arr = [0u8; 20];
        arr.copy_from_slice(&bytes);
        out.insert(arr, e.sha.clone());
    }
    out
}

/// Truncated exponential backoff identical in shape to the ERC20 worker's:
/// `tick_interval * 2^failures`, capped at `MAX_BACKOFF_SECS`.
fn compute_backoff(tick_interval: Duration, consecutive_failures: u32) -> Duration {
    if consecutive_failures == 0 {
        return tick_interval;
    }
    let shift = consecutive_failures.min(20);
    let multiplier: u64 = 1u64 << shift;
    let secs = tick_interval
        .as_secs()
        .saturating_mul(multiplier)
        .min(MAX_BACKOFF_SECS);
    Duration::from_secs(secs)
}

// ── JSON shapes ──────────────────────────────────────────────────────────

/// Subset of the GitHub Git Trees API response we consume.
#[derive(Debug, Deserialize)]
struct GitTree {
    tree: Vec<GitTreeEntry>,
    #[serde(default)]
    truncated: bool,
}

/// One entry in the recursive tree response. We only read blobs; directory
/// entries are discarded during parsing.
#[derive(Debug, Deserialize)]
struct GitTreeEntry {
    path: String,
    sha: String,
    #[serde(rename = "type")]
    entry_type: String,
}

/// Subset of per-asset `info.json` we map to typed columns. Everything is
/// `Option` because the repo is maintained by hand and individual fields
/// can be absent or null. `tags` and `links` stay as raw JSON values so we
/// can pass them through to `JSONB` columns without shape assumptions.
#[derive(Debug, Deserialize)]
struct InfoJson {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    decimals: Option<i16>,
    /// `type` in the JSON; renamed to avoid the Rust keyword + match our
    /// `asset_type` column.
    #[serde(default, rename = "type")]
    asset_type: Option<String>,
    #[serde(default)]
    website: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    explorer: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    tags: Option<serde_json::Value>,
    #[serde(default)]
    links: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slug_map_covers_mainnet_only_for_now() {
        assert_eq!(slug_for(1), Some("ethereum"));
        assert_eq!(slug_for(11_155_111), None); // sepolia
        assert_eq!(slug_for(999_999), None);
    }

    #[test]
    fn info_json_url_is_eip55() {
        // USDC, mixed-case EIP-55.
        let addr: Address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
            .parse()
            .unwrap();
        let url = info_json_url("ethereum", &addr);
        assert_eq!(
            url,
            "https://raw.githubusercontent.com/trustwallet/assets/master/blockchains/ethereum/assets/0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48/info.json"
        );
    }

    #[test]
    fn parse_tree_filters_to_chain_slug_and_blobs() {
        let entries = vec![
            GitTreeEntry {
                path: "blockchains/ethereum/assets/0x0000000000000000000000000000000000000001/info.json"
                    .into(),
                sha: "aaaa".into(),
                entry_type: "blob".into(),
            },
            // Wrong chain — skipped.
            GitTreeEntry {
                path: "blockchains/smartchain/assets/0x0000000000000000000000000000000000000002/info.json"
                    .into(),
                sha: "bbbb".into(),
                entry_type: "blob".into(),
            },
            // Directory — skipped.
            GitTreeEntry {
                path: "blockchains/ethereum/assets/0x0000000000000000000000000000000000000003"
                    .into(),
                sha: "cccc".into(),
                entry_type: "tree".into(),
            },
            // Not info.json — skipped.
            GitTreeEntry {
                path: "blockchains/ethereum/assets/0x0000000000000000000000000000000000000001/logo.png"
                    .into(),
                sha: "dddd".into(),
                entry_type: "blob".into(),
            },
        ];
        let map = parse_tree(&entries, "ethereum");
        assert_eq!(map.len(), 1);
        let mut addr = [0u8; 20];
        addr[19] = 1;
        assert_eq!(map.get(&addr), Some(&"aaaa".to_string()));
    }

    #[test]
    fn info_json_decode_handles_real_payload() {
        // Trimmed fixture matching the shape of trust-wallet's USDC entry.
        let raw = r#"{
            "name": "USD Coin",
            "symbol": "USDC",
            "decimals": 6,
            "type": "ERC20",
            "website": "https://centre.io/usdc",
            "description": "USDC is a fully collateralized stablecoin.",
            "explorer": "https://etherscan.io/token/0xA0b8...",
            "status": "active",
            "id": "0xA0b8...",
            "tags": ["stablecoin"],
            "links": [{"name": "github", "url": "https://github.com/centrehq"}]
        }"#;
        let parsed: InfoJson = serde_json::from_str(raw).unwrap();
        assert_eq!(parsed.name.as_deref(), Some("USD Coin"));
        assert_eq!(parsed.symbol.as_deref(), Some("USDC"));
        assert_eq!(parsed.decimals, Some(6));
        assert_eq!(parsed.asset_type.as_deref(), Some("ERC20"));
        assert_eq!(parsed.status.as_deref(), Some("active"));
        assert!(parsed.tags.is_some());
        assert!(parsed.links.is_some());
    }

    #[test]
    fn info_json_decode_tolerates_missing_fields() {
        let parsed: InfoJson = serde_json::from_str("{}").unwrap();
        assert!(parsed.name.is_none());
        assert!(parsed.decimals.is_none());
        assert!(parsed.tags.is_none());
    }

    #[test]
    fn backoff_starts_at_tick_interval() {
        assert_eq!(compute_backoff(DEFAULT_TICK_INTERVAL, 0), DEFAULT_TICK_INTERVAL);
        let custom = Duration::from_secs(3_600);
        assert_eq!(compute_backoff(custom, 0), custom);
    }

    #[test]
    fn backoff_truncates_at_cap() {
        let capped = compute_backoff(DEFAULT_TICK_INTERVAL, u32::MAX);
        assert_eq!(capped, Duration::from_secs(MAX_BACKOFF_SECS));
    }
}
