//! Address label lookups against the `labels_accounts` and `labels_contracts`
//! tables (seeded by `tidx seed-labels`).
//!
//! Chain is implicit from the pool — each chain has its own Postgres DB.
//!
//! eth-labels is a taxonomy: a single address commonly carries multiple tags
//! (e.g. a protocol tag + a compliance flag), so lookups return `Vec<Label>`
//! per address. Contracts rows (richer metadata) are listed before accounts
//! rows; within each source the order is by `label` slug.

use std::collections::HashMap;

use serde::Serialize;
use tokio_postgres::Client;
use tracing::warn;

use crate::db::Pool;

#[derive(Serialize, Debug, Clone)]
pub struct Label {
    pub label: String,
    pub name_tag: String,
}

/// Look up labels for a batch of 20-byte addresses in this chain's DB.
///
/// Returns a map from address → ordered `Vec<Label>`. Addresses with no
/// matches are absent from the map (not present as an empty `Vec`).
///
/// On PG failure, logs a warning and returns an empty map rather than
/// propagating the error — labels are best-effort, not critical.
pub async fn lookup_batch(
    pool: &Pool,
    addresses: &[[u8; 20]],
) -> HashMap<[u8; 20], Vec<Label>> {
    if addresses.is_empty() {
        return HashMap::new();
    }

    let mut uniq: Vec<[u8; 20]> = addresses.to_vec();
    uniq.sort_unstable();
    uniq.dedup();

    let conn = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "labels pool get failed, skipping");
            return HashMap::new();
        }
    };

    let byte_refs: Vec<&[u8]> = uniq.iter().map(|a| a.as_slice()).collect();

    // Contracts first (richer source), accounts second. Inside each table we
    // ORDER BY label so results are deterministic regardless of insertion order.
    let mut out: HashMap<[u8; 20], Vec<Label>> = HashMap::new();
    if let Err(e) = append_from("labels_contracts", &conn, &byte_refs, &mut out).await {
        warn!(error = %e, "labels_contracts query failed");
    }
    if let Err(e) = append_from("labels_accounts", &conn, &byte_refs, &mut out).await {
        warn!(error = %e, "labels_accounts query failed");
    }
    out
}

async fn append_from(
    table: &str,
    conn: &Client,
    byte_refs: &[&[u8]],
    out: &mut HashMap<[u8; 20], Vec<Label>>,
) -> Result<(), tokio_postgres::Error> {
    let sql = format!(
        "SELECT address, label, name_tag FROM {table} \
         WHERE address = ANY($1) ORDER BY address, label"
    );
    let rows = conn.query(&sql, &[&byte_refs]).await?;
    for row in rows {
        let addr: &[u8] = row.get(0);
        if addr.len() != 20 {
            continue;
        }
        let mut key = [0u8; 20];
        key.copy_from_slice(addr);
        out.entry(key).or_default().push(Label {
            label: row.get(1),
            name_tag: row.get(2),
        });
    }
    Ok(())
}

/// Parse a `0x`-prefixed 20-byte hex address into raw bytes. Returns `None`
/// on any shape that isn't exactly `0x` + 40 hex chars.
pub fn parse_address_20(addr: &str) -> Option<[u8; 20]> {
    let hex = addr.strip_prefix("0x").or_else(|| addr.strip_prefix("0X"))?;
    if hex.len() != 40 {
        return None;
    }
    let mut out = [0u8; 20];
    hex::decode_to_slice(hex, &mut out).ok()?;
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_address_round_trips() {
        let raw = "0xdac17f958d2ee523a2206206994597c13d831ec7";
        let bytes = parse_address_20(raw).unwrap();
        assert_eq!(format!("0x{}", hex::encode(bytes)), raw);
    }

    #[test]
    fn parse_address_rejects_junk() {
        assert!(parse_address_20("0xzz").is_none());
        assert!(parse_address_20("dac17f958d2ee523a2206206994597c13d831ec7").is_none());
        assert!(parse_address_20("0xdac17f958d2ee523a2206206994597c13d831ec70000").is_none());
    }
}
