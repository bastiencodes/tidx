//! ENS reverse-resolution enrichment.
//!
//! Cache-first batch lookup of ENS primary names (address → name), backed by
//! the `ens_records` table and filled on-demand via Multicall3 against the
//! chain's RPC. Enabled per-chain via `[chains.ens]` and wired into the
//! `?ens=true` enrichment path on `/transactions`, `/erc20/transfers`, etc.
//!
//! # Resolution shape
//!
//! For each uncached (or stale) address we issue four sequential Multicall3
//! batches against this chain's RPC:
//!
//!   A. `ENSRegistry.resolver(reverseNode)` — find each address's reverse
//!      resolver. `reverseNode = namehash("<lowerhex(addr)>.addr.reverse")`.
//!   B. `IResolver.name(reverseNode)` — call on the specific resolver from
//!      step A to read the claimed primary name.
//!   C. `ENSRegistry.resolver(namehash(name))` — forward path: find the
//!      resolver for the claimed name.
//!   D. `IResolver.addr(namehash(name))` — resolve the name forward and
//!      compare against the original address. `verified = (forward == addr)`.
//!
//! Four RPC round-trips per uncached batch is fine because (a) we batch all
//! addresses in one Multicall3 per phase, and (b) the cache hit-rate is high
//! under the 24h TTL — most enrichment calls hit zero of these phases.
//!
//! # Staleness
//!
//! `verified` is a point-in-time assertion as of `resolved_at` and MAY be up
//! to `stale_after_secs` (default 24h) out of date. See `db/ens_records.sql`
//! for the full staleness contract. This module does not expose a live-read
//! override; callers needing authoritative identity must resolve against RPC
//! directly rather than through this cache.
//!
//! # Error policy
//!
//! Any RPC or DB failure during on-demand resolution is swallowed and logged
//! as a warning. The function returns whatever cache hits it had — missing
//! ENS enrichment is always preferable to failing the parent request. Matches
//! the best-effort contract of `crate::labels::lookup_batch`.

use std::collections::HashMap;
use std::time::Instant;

use alloy::primitives::{Address, B256, Bytes, address, keccak256};
use alloy::sol;
use alloy::sol_types::SolCall;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio_postgres::types::Type;
use tracing::{debug, warn};

use crate::db::Pool;
use crate::sync::fetcher::RpcClient;

/// Canonical Multicall3 address — same on mainnet, sepolia, and most EVM
/// chains. Matches [`crate::sync::erc20_metadata::MULTICALL3_ADDRESS`].
const MULTICALL3_ADDRESS: Address = address!("cA11bde05977b3631167028862bE2a173976CA11");

/// Default ENS Registry address on Ethereum mainnet. Same contract has been
/// used since ENS launch; no planned migration at time of writing.
pub const ENS_REGISTRY_MAINNET: Address = address!("00000000000C2E074eC69A0dFb2997BA6C7d2e1e");

/// How long a cached row is trusted before it's re-read from RPC. Default 24h.
pub const DEFAULT_STALE_AFTER_SECS: i64 = 86_400;

/// Per-chain ENS configuration. Constructed from `[chains.ens]` in the TOML
/// config; exposed here so this module has no `crate::config` dependency and
/// stays unit-testable in isolation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnsConfig {
    /// When `false`, `lookup_batch` short-circuits to an empty map regardless
    /// of cache state. Enrichment call sites should also gate on this.
    #[serde(default)]
    pub enabled: bool,

    /// Address of the ENS Registry contract for this chain. Mainnet is the
    /// only supported deployment in v1; configurable so sepolia (or forks)
    /// can be wired in later without a code change.
    #[serde(default = "default_ens_registry")]
    pub registry: Address,

    /// Positive + negative cache TTL in seconds. See module docstring for
    /// staleness semantics.
    #[serde(default = "default_stale_after_secs")]
    pub stale_after_secs: i64,
}

fn default_ens_registry() -> Address {
    ENS_REGISTRY_MAINNET
}

fn default_stale_after_secs() -> i64 {
    DEFAULT_STALE_AFTER_SECS
}

impl Default for EnsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            registry: ENS_REGISTRY_MAINNET,
            stale_after_secs: DEFAULT_STALE_AFTER_SECS,
        }
    }
}

/// Bundles the ENS config + the RPC client used to resolve names for a
/// chain. Held by `AppState` behind a single `SharedEnsState` map —
/// chains without ENS enabled have no entry, so there's no wasted
/// `RpcClient` allocation for chains that won't ever use it.
///
/// The RPC client is constructed with low concurrency (matching the ERC20
/// metadata worker, see `src/cli/up.rs`) so enrichment reads don't
/// contend with the sync engine's block-range fetches.
#[derive(Clone)]
pub struct EnsRuntimeState {
    pub config: EnsConfig,
    pub rpc: RpcClient,
}

/// ENS enrichment payload for one address. Serialized under the top-level
/// `ens` map on `/transactions`, `/erc20/transfers`, etc.
#[derive(Debug, Clone, Serialize)]
pub struct EnsName {
    /// Self-declared primary name from the reverse resolver.
    pub name: String,

    /// `true` iff forward resolution of `name` returned this address at
    /// `resolved_at`. See module docstring for staleness caveats.
    pub verified: bool,

    /// Wall-clock time the resolution ran. Present so clients can judge
    /// staleness themselves.
    pub resolved_at: DateTime<Utc>,
}

// ── Contract interfaces ─────────────────────────────────────────────────────

sol! {
    interface IENSRegistry {
        function resolver(bytes32 node) external view returns (address);
    }

    interface IENSResolver {
        function addr(bytes32 node) external view returns (address);
        function name(bytes32 node) external view returns (string);
    }

    interface IMulticall3 {
        struct Call3 {
            address target;
            bool allowFailure;
            bytes callData;
        }
        struct Result3 {
            bool success;
            bytes returnData;
        }
        function aggregate3(Call3[] calls) external payable returns (Result3[] memory);
        function getBlockNumber() external view returns (uint256);
    }
}

// ── Public API ──────────────────────────────────────────────────────────────

/// Cache-first batch reverse-lookup. For each input address:
///
/// - **Fresh cache hit with a name** → returned in the map.
/// - **Fresh cache hit with NULL name** (negative cache) → omitted from map.
/// - **Miss or stale hit** → resolved via RPC, upserted into `ens_records`,
///   and included in the map iff the resolved name is non-empty.
///
/// Returns a map keyed by the input 20-byte address. Addresses absent from
/// the returned map either have no primary name or failed to resolve
/// (failures are logged, not propagated — see module docstring).
pub async fn lookup_batch(
    pool: &Pool,
    rpc: &RpcClient,
    addresses: &[[u8; 20]],
    config: &EnsConfig,
) -> HashMap<[u8; 20], EnsName> {
    if !config.enabled || addresses.is_empty() {
        return HashMap::new();
    }

    // Dedupe so a batch of 100 txs mentioning the same hot address 50 times
    // doesn't turn into 50 redundant cache lookups + 50 RPC calls.
    let mut uniq: Vec<[u8; 20]> = addresses.to_vec();
    uniq.sort_unstable();
    uniq.dedup();

    let mut out: HashMap<[u8; 20], EnsName> = HashMap::new();
    let mut needs_resolve: Vec<[u8; 20]> = Vec::new();

    // Phase 1: partition on cache state. Fresh hits go straight to the output
    // map; misses and stale hits are queued for RPC resolution.
    match read_cache(pool, &uniq, config.stale_after_secs).await {
        Ok(cache) => {
            for addr in &uniq {
                match cache.get(addr) {
                    Some(CacheEntry::FreshHit(name)) => {
                        out.insert(*addr, name.clone());
                    }
                    Some(CacheEntry::FreshNegative) => {
                        // Known to have no primary name, within TTL — skip.
                    }
                    Some(CacheEntry::Stale) | None => {
                        needs_resolve.push(*addr);
                    }
                }
            }
        }
        Err(e) => {
            warn!(error = %e, "ens_records cache read failed; resolving all addresses");
            needs_resolve = uniq.clone();
        }
    }

    if needs_resolve.is_empty() {
        return out;
    }

    // Phase 2: resolve misses + stale rows via RPC.
    let rpc_start = Instant::now();
    let resolved = match resolve_via_rpc(rpc, &needs_resolve, config.registry).await {
        Ok(r) => r,
        Err(e) => {
            warn!(
                error = %e,
                batch_size = needs_resolve.len(),
                "ENS RPC resolution failed; returning cache-only results"
            );
            return out;
        }
    };
    debug!(
        addresses = needs_resolve.len(),
        elapsed_ms = rpc_start.elapsed().as_millis() as u64,
        "ENS RPC resolution complete"
    );

    // Phase 3: write-through cache + populate the output map for non-empty names.
    if let Err(e) = write_cache(pool, &resolved).await {
        warn!(error = %e, "ens_records cache write failed; returning resolved data anyway");
    }
    for (addr, row) in resolved {
        if let Some(name) = row.name {
            out.insert(addr, EnsName { name, verified: row.verified, resolved_at: row.resolved_at });
        }
    }
    out
}

// ── Cache layer ─────────────────────────────────────────────────────────────

enum CacheEntry {
    /// Fresh row with a non-null name.
    FreshHit(EnsName),
    /// Fresh row with a null name — known to have no primary name.
    FreshNegative,
    /// Row exists but is older than `stale_after_secs`.
    Stale,
}

async fn read_cache(
    pool: &Pool,
    addresses: &[[u8; 20]],
    stale_after_secs: i64,
) -> Result<HashMap<[u8; 20], CacheEntry>> {
    let conn = pool.get().await?;
    let refs: Vec<&[u8]> = addresses.iter().map(|a| a.as_slice()).collect();

    let rows = conn
        .query(
            r#"
            SELECT address, name, verified, resolved_at,
                   (EXTRACT(EPOCH FROM (NOW() - resolved_at))::INT8 > $2::INT8) AS is_stale
            FROM ens_records
            WHERE address = ANY($1)
            "#,
            &[&refs, &stale_after_secs],
        )
        .await?;

    let mut out = HashMap::with_capacity(rows.len());
    for row in rows {
        let addr_bytes: &[u8] = row.get(0);
        if addr_bytes.len() != 20 {
            continue;
        }
        let mut addr = [0u8; 20];
        addr.copy_from_slice(addr_bytes);

        let is_stale: bool = row.get(4);
        let entry = if is_stale {
            CacheEntry::Stale
        } else {
            let name: Option<String> = row.get(1);
            match name {
                None => CacheEntry::FreshNegative,
                Some(n) => CacheEntry::FreshHit(EnsName {
                    name: n,
                    verified: row.get(2),
                    resolved_at: row.get(3),
                }),
            }
        };
        out.insert(addr, entry);
    }
    Ok(out)
}

/// Resolved row ready to be upserted into `ens_records`. Distinct from
/// `EnsName` because this struct also carries the negative-cache case
/// (`name: None`) which we still want to persist.
struct ResolvedRow {
    name: Option<String>,
    verified: bool,
    resolved_at: DateTime<Utc>,
    resolved_block: i64,
}

async fn write_cache(pool: &Pool, rows: &HashMap<[u8; 20], ResolvedRow>) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let conn = pool.get().await?;
    // One row per addr; small batches (<= batch-size of an API request) so
    // individual UPSERTs are fine and keep the SQL simple. Worth revisiting
    // if enrichment batches ever get into the thousands.
    let stmt = conn
        .prepare_typed(
            r#"
            INSERT INTO ens_records (address, name, verified, resolved_at, resolved_block)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (address) DO UPDATE SET
                name = EXCLUDED.name,
                verified = EXCLUDED.verified,
                resolved_at = EXCLUDED.resolved_at,
                resolved_block = EXCLUDED.resolved_block
            "#,
            &[Type::BYTEA, Type::TEXT, Type::BOOL, Type::TIMESTAMPTZ, Type::INT8],
        )
        .await?;
    for (addr, row) in rows {
        conn.execute(
            &stmt,
            &[
                &addr.as_slice(),
                &row.name,
                &row.verified,
                &row.resolved_at,
                &row.resolved_block,
            ],
        )
        .await?;
    }
    Ok(())
}

// ── RPC resolution (4 Multicall3 phases) ───────────────────────────────────

async fn resolve_via_rpc(
    rpc: &RpcClient,
    addresses: &[[u8; 20]],
    registry: Address,
) -> Result<HashMap<[u8; 20], ResolvedRow>> {
    let addrs: Vec<Address> = addresses.iter().map(|a| Address::from_slice(a)).collect();
    let now = Utc::now();

    // Phase A: registry.resolver(reverseNode) for each input address.
    let reverse_nodes: Vec<B256> = addrs.iter().map(|a| reverse_node(*a)).collect();
    let (block_num, reverse_resolvers) = multicall_resolvers(rpc, registry, &reverse_nodes).await?;
    let resolved_block = i64::try_from(block_num.unwrap_or(0)).unwrap_or(0);

    // Phase B: resolver.name(reverseNode) on each non-zero resolver.
    let names = multicall_names(rpc, &reverse_resolvers, &reverse_nodes).await?;

    // Phase C: registry.resolver(namehash(name)) — forward path, only for
    // addresses with a non-empty claimed name.
    let mut forward_nodes: HashMap<[u8; 20], B256> = HashMap::new();
    let mut forward_lookup_nodes: Vec<B256> = Vec::new();
    let mut forward_lookup_addrs: Vec<[u8; 20]> = Vec::new();
    for (addr, name) in addresses.iter().zip(names.iter()) {
        if let Some(n) = name {
            if !n.is_empty() {
                let node = namehash(n);
                forward_nodes.insert(*addr, node);
                forward_lookup_nodes.push(node);
                forward_lookup_addrs.push(*addr);
            }
        }
    }
    let forward_resolvers = if forward_lookup_nodes.is_empty() {
        Vec::new()
    } else {
        multicall_resolvers(rpc, registry, &forward_lookup_nodes)
            .await
            .map(|(_, r)| r)?
    };

    // Phase D: resolver.addr(namehash(name)) on each non-zero forward resolver.
    let forward_addrs = if forward_lookup_nodes.is_empty() {
        Vec::new()
    } else {
        multicall_addrs(rpc, &forward_resolvers, &forward_lookup_nodes).await?
    };

    // Stitch results together. For every input address, decide:
    //   - No reverse resolver OR reverse name empty → negative cache row.
    //   - Forward resolution missing or mismatched → verified = false.
    //   - Forward resolution matches → verified = true.
    let forward_by_addr: HashMap<[u8; 20], Option<Address>> = forward_lookup_addrs
        .iter()
        .zip(forward_addrs.iter())
        .map(|(a, fa)| (*a, *fa))
        .collect();

    let mut out = HashMap::with_capacity(addresses.len());
    for (i, addr) in addresses.iter().enumerate() {
        let name = names.get(i).cloned().flatten().filter(|s| !s.is_empty());
        let verified = match (&name, forward_by_addr.get(addr).copied().flatten()) {
            (Some(_), Some(forward)) => forward.as_slice() == addr.as_slice(),
            _ => false,
        };
        out.insert(
            *addr,
            ResolvedRow {
                name,
                verified,
                resolved_at: now,
                resolved_block,
            },
        );
    }
    Ok(out)
}

/// Multicall3: `registry.resolver(node)` for each node. Also returns the
/// block number reported by Multicall3 itself so we have an atomic anchor
/// for the row we're about to cache.
async fn multicall_resolvers(
    rpc: &RpcClient,
    registry: Address,
    nodes: &[B256],
) -> Result<(Option<u64>, Vec<Option<Address>>)> {
    let mut calls: Vec<IMulticall3::Call3> = Vec::with_capacity(nodes.len() + 1);
    calls.push(IMulticall3::Call3 {
        target: MULTICALL3_ADDRESS,
        allowFailure: true,
        callData: Bytes::from(IMulticall3::getBlockNumberCall {}.abi_encode()),
    });
    for node in nodes {
        calls.push(IMulticall3::Call3 {
            target: registry,
            allowFailure: true,
            callData: Bytes::from(IENSRegistry::resolverCall { node: *node }.abi_encode()),
        });
    }

    let decoded = send_aggregate3(rpc, calls).await?;
    if decoded.len() != nodes.len() + 1 {
        anyhow::bail!(
            "Multicall3 (resolvers) returned {} results, expected {}",
            decoded.len(),
            nodes.len() + 1
        );
    }

    let block_number = decoded[0]
        .success
        .then(|| IMulticall3::getBlockNumberCall::abi_decode_returns(&decoded[0].returnData).ok())
        .flatten()
        .and_then(|u| u64::try_from(u).ok());

    let resolvers = decoded[1..]
        .iter()
        .map(|r| {
            if !r.success {
                return None;
            }
            let resolver = IENSRegistry::resolverCall::abi_decode_returns(&r.returnData).ok()?;
            if resolver == Address::ZERO {
                None
            } else {
                Some(resolver)
            }
        })
        .collect();
    Ok((block_number, resolvers))
}

/// Multicall3: `resolver.name(reverseNode)` — one call per input whose
/// resolver is non-None. Returns a vector aligned with the input: `None`
/// means "no resolver set" or "call failed" or "empty string returned".
async fn multicall_names(
    rpc: &RpcClient,
    resolvers: &[Option<Address>],
    nodes: &[B256],
) -> Result<Vec<Option<String>>> {
    // Compact the input to only the positions with a resolver so we don't
    // waste sub-calls on known misses. Reassemble at the end.
    let mut compact_resolvers: Vec<Address> = Vec::new();
    let mut compact_nodes: Vec<B256> = Vec::new();
    let mut back_index: Vec<Option<usize>> = vec![None; resolvers.len()];
    for (i, r) in resolvers.iter().enumerate() {
        if let Some(res) = r {
            back_index[i] = Some(compact_resolvers.len());
            compact_resolvers.push(*res);
            compact_nodes.push(nodes[i]);
        }
    }
    if compact_resolvers.is_empty() {
        return Ok(vec![None; resolvers.len()]);
    }

    let calls: Vec<IMulticall3::Call3> = compact_resolvers
        .iter()
        .zip(compact_nodes.iter())
        .map(|(target, node)| IMulticall3::Call3 {
            target: *target,
            allowFailure: true,
            callData: Bytes::from(IENSResolver::nameCall { node: *node }.abi_encode()),
        })
        .collect();

    let decoded = send_aggregate3(rpc, calls).await?;
    let mut out = vec![None; resolvers.len()];
    for (i, slot) in back_index.iter().enumerate() {
        let Some(j) = slot else { continue };
        let r = &decoded[*j];
        if !r.success {
            continue;
        }
        if let Ok(s) = IENSResolver::nameCall::abi_decode_returns(&r.returnData) {
            if !s.is_empty() {
                out[i] = Some(s);
            }
        }
    }
    Ok(out)
}

/// Multicall3: `resolver.addr(forwardNode)` — one call per input whose
/// resolver is non-None. Same compact/reassemble pattern as
/// [`multicall_names`].
async fn multicall_addrs(
    rpc: &RpcClient,
    resolvers: &[Option<Address>],
    nodes: &[B256],
) -> Result<Vec<Option<Address>>> {
    let mut compact_resolvers: Vec<Address> = Vec::new();
    let mut compact_nodes: Vec<B256> = Vec::new();
    let mut back_index: Vec<Option<usize>> = vec![None; resolvers.len()];
    for (i, r) in resolvers.iter().enumerate() {
        if let Some(res) = r {
            back_index[i] = Some(compact_resolvers.len());
            compact_resolvers.push(*res);
            compact_nodes.push(nodes[i]);
        }
    }
    if compact_resolvers.is_empty() {
        return Ok(vec![None; resolvers.len()]);
    }

    let calls: Vec<IMulticall3::Call3> = compact_resolvers
        .iter()
        .zip(compact_nodes.iter())
        .map(|(target, node)| IMulticall3::Call3 {
            target: *target,
            allowFailure: true,
            callData: Bytes::from(IENSResolver::addrCall { node: *node }.abi_encode()),
        })
        .collect();

    let decoded = send_aggregate3(rpc, calls).await?;
    let mut out = vec![None; resolvers.len()];
    for (i, slot) in back_index.iter().enumerate() {
        let Some(j) = slot else { continue };
        let r = &decoded[*j];
        if !r.success {
            continue;
        }
        if let Ok(a) = IENSResolver::addrCall::abi_decode_returns(&r.returnData) {
            if a != Address::ZERO {
                out[i] = Some(a);
            }
        }
    }
    Ok(out)
}

/// ABI-encode `aggregate3(calls)`, call Multicall3, decode the result into
/// the per-sub-call `Result3` array.
async fn send_aggregate3(
    rpc: &RpcClient,
    calls: Vec<IMulticall3::Call3>,
) -> Result<Vec<IMulticall3::Result3>> {
    let calldata = IMulticall3::aggregate3Call { calls }.abi_encode();
    let to_hex = format!("0x{}", hex::encode(MULTICALL3_ADDRESS.as_slice()));
    let data_hex = format!("0x{}", hex::encode(&calldata));
    let raw = rpc.eth_call(&to_hex, &data_hex).await?;
    IMulticall3::aggregate3Call::abi_decode_returns(&raw)
        .map_err(|e| anyhow!("Failed to decode Multicall3 aggregate3 response: {e}"))
}

// ── ENS name hashing ────────────────────────────────────────────────────────

/// EIP-137 namehash. Iteratively hashes the labels right-to-left starting
/// from the zero node. Empty name hashes to 0x00…00.
///
/// Labels are lowercased before hashing per the common ENS convention.
/// Proper ENSIP-15 normalization (`ens_normalize`) is intentionally NOT
/// applied in v1 — it's a large dependency and the primary-name path works
/// fine without it for the vast majority of names (which are already in
/// canonical form onchain).
pub fn namehash(name: &str) -> B256 {
    let mut node = B256::ZERO;
    if name.is_empty() {
        return node;
    }
    for label in name.rsplit('.') {
        let label_hash = keccak256(label.to_lowercase().as_bytes());
        let mut buf = [0u8; 64];
        buf[..32].copy_from_slice(node.as_slice());
        buf[32..].copy_from_slice(label_hash.as_slice());
        node = keccak256(buf);
    }
    node
}

/// Reverse-resolution namehash: `namehash("<lowerhex(addr)>.addr.reverse")`.
/// Not to be confused with `namehash` on an arbitrary string — the address
/// must be lowercased hex (no `0x` prefix) per ENSIP-3.
pub fn reverse_node(addr: Address) -> B256 {
    let hex_addr = hex::encode(addr.as_slice());
    namehash(&format!("{}.addr.reverse", hex_addr))
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namehash_empty_is_zero() {
        assert_eq!(namehash(""), B256::ZERO);
    }

    #[test]
    fn namehash_matches_ens_reference_vectors() {
        // Canonical namehash(eth) per EIP-137 test vectors.
        let eth =
            "93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae";
        assert_eq!(hex::encode(namehash("eth")), eth);

        // namehash("foo.eth")
        let foo_eth =
            "de9b09fd7c5f901e23a3f19fecc54828e9c848539801e86591bd9801b019f84f";
        assert_eq!(hex::encode(namehash("foo.eth")), foo_eth);
    }

    #[test]
    fn namehash_lowercases_labels() {
        // ENS requires lowercase; the canonical vectors assume it.
        assert_eq!(namehash("FOO.eth"), namehash("foo.eth"));
        assert_eq!(namehash("Foo.ETH"), namehash("foo.eth"));
    }

    #[test]
    fn reverse_node_has_expected_suffix() {
        // The last two components hashed are "addr" and "reverse", so
        // reverse_node for any address must equal namehash of its
        // "<hex>.addr.reverse" composition. Cheapest way to verify is
        // to compute the same composition manually.
        let addr = address!("0102030405060708090a0b0c0d0e0f1011121314");
        let expected = namehash("0102030405060708090a0b0c0d0e0f1011121314.addr.reverse");
        assert_eq!(reverse_node(addr), expected);
    }

    #[test]
    fn ens_config_default_is_disabled() {
        let c = EnsConfig::default();
        assert!(!c.enabled);
        assert_eq!(c.registry, ENS_REGISTRY_MAINNET);
        assert_eq!(c.stale_after_secs, DEFAULT_STALE_AFTER_SECS);
    }
}
