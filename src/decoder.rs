//! Function-calldata and event-log decoding against the `signatures` cache.
//!
//! For calldata: resolves the 4-byte selector (`tx.input[0:4]`) to candidate
//! text signatures via the local Sourcify mirror, then uses `alloy-dyn-abi`
//! to try-decode the argument bytes against each candidate and returns the
//! first that parses cleanly.
//!
//! For event logs: resolves the 32-byte `topic0` to candidate canonical
//! signatures. Sourcify stores the canonical form (no `indexed` markers),
//! so we heuristically mark the first `(topics.len() - 1)` params as
//! indexed and decode topics/data via `alloy::json_abi::Event::decode_log`.
//! If arg decoding fails we fall back to returning just `{ name, signature }`
//! so callers still get the human-readable event name.

use std::collections::HashMap;

use alloy::dyn_abi::{DecodedEvent, DynSolValue, EventExt, JsonAbiExt};
use alloy::json_abi::{Event, EventParam, Function};
use alloy::primitives::{LogData, B256};
use serde::Serialize;
use serde_json::Value as JsonValue;
use tokio_postgres::Client;
use tracing::warn;

#[derive(Serialize, Debug, Clone)]
pub struct Decoded {
    pub name: String,
    pub signature: String,
    pub inputs: Vec<DecodedInput>,
}

#[derive(Serialize, Debug, Clone)]
pub struct DecodedInput {
    #[serde(rename = "type")]
    pub ty: String,
    pub value: JsonValue,
    /// Populated only for `type = "address"` inputs when the caller set
    /// both `?decode=true` and `?ens=true` AND the chain has ENS enabled
    /// AND the address has a resolvable primary name. Absence is
    /// meaningful — we checked, there was nothing to attach.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ens: Option<crate::ens::EnsName>,
    /// Populated only for `type = "address"` inputs when the caller set
    /// both `?decode=true` and `?labels=true` AND the address has one or
    /// more entries in the `labels_*` tables. Empty vec is never emitted
    /// — we skip-serialize when there are no matches rather than send
    /// `"labels": []`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<Vec<crate::labels::Label>>,
}

/// Decode a batch of `tx.input` blobs in one PG roundtrip.
///
/// For each input: `None` if selector unknown, input too short, or no
/// candidate parses; `Some(Decoded)` for the first candidate that does.
///
/// On PG failure, logs a warning and returns all-`None` rather than
/// propagating the error — decoding is best-effort, not critical.
pub async fn decode_calldata_batch(
    client: &Client,
    inputs: &[&[u8]],
) -> Vec<Option<Decoded>> {
    // Collect the unique 4-byte selectors we need candidates for.
    let mut selectors: Vec<[u8; 4]> = Vec::with_capacity(inputs.len());
    for input in inputs {
        if let Some(sel) = selector_of(input) {
            selectors.push(sel);
        }
    }
    selectors.sort_unstable();
    selectors.dedup();

    if selectors.is_empty() {
        return inputs.iter().map(|_| None).collect();
    }

    let candidates = match lookup_selectors(client, &selectors).await {
        Ok(m) => m,
        Err(e) => {
            warn!(error = %e, "signature lookup failed, skipping decode");
            return inputs.iter().map(|_| None).collect();
        }
    };

    inputs
        .iter()
        .map(|input| {
            let sel = selector_of(input)?;
            let args = &input[4..];
            let sigs = candidates.get(&sel)?;
            sigs.iter().find_map(|s| try_decode_function(s, args))
        })
        .collect()
}

fn selector_of(input: &[u8]) -> Option<[u8; 4]> {
    if input.len() < 4 {
        return None;
    }
    Some([input[0], input[1], input[2], input[3]])
}

async fn lookup_selectors(
    client: &Client,
    selectors: &[[u8; 4]],
) -> Result<HashMap<[u8; 4], Vec<String>>, tokio_postgres::Error> {
    let byte_slices: Vec<&[u8]> = selectors.iter().map(|s| s.as_slice()).collect();
    let rows = client
        .query(
            "SELECT signature_hash_4, signature \
             FROM signatures \
             WHERE signature_hash_4 = ANY($1)",
            &[&byte_slices],
        )
        .await?;

    let mut map: HashMap<[u8; 4], Vec<String>> = HashMap::new();
    for row in rows {
        let hash: &[u8] = row.get(0);
        let sig: String = row.get(1);
        if hash.len() == 4 {
            let key = [hash[0], hash[1], hash[2], hash[3]];
            map.entry(key).or_default().push(sig);
        }
    }
    Ok(map)
}

fn try_decode_function(text_sig: &str, args: &[u8]) -> Option<Decoded> {
    let func = Function::parse(text_sig).ok()?;
    let values = func.abi_decode_input(args).ok()?;
    if values.len() != func.inputs.len() {
        return None;
    }
    Some(Decoded {
        name: func.name.clone(),
        signature: func.signature(),
        inputs: values
            .into_iter()
            .zip(func.inputs.iter())
            .map(|(val, param)| DecodedInput {
                ty: param.ty.clone(),
                value: value_to_json(val),
                ens: None,
                labels: None,
            })
            .collect(),
    })
}

/// An event-log view suitable for ABI decoding. `topics[0]` is the event
/// selector (topic0); remaining topics are indexed args. Anonymous events
/// (no topic0) should be skipped by the caller. Owns its bytes — topics
/// and data are small, so owning avoids lifetime gymnastics at the call
/// site.
pub struct EventInput {
    pub topics: Vec<Vec<u8>>,
    pub data: Vec<u8>,
}

/// Decode a batch of event logs. For each event: `None` if `topics` is
/// empty, topic0 unknown in the signatures cache, or all candidate
/// signatures fail to parse as events. `Some(Decoded)` otherwise — with
/// best-effort arg decoding and a `{ name, signature, inputs: [] }`
/// fallback when args can't be decoded.
pub async fn decode_events_batch(
    client: &Client,
    events: &[EventInput],
) -> Vec<Option<Decoded>> {
    // Unique 32-byte topic0 values we need candidates for.
    let mut topic0s: Vec<[u8; 32]> = Vec::with_capacity(events.len());
    for ev in events {
        if let Some(t0) = ev.topic0() {
            topic0s.push(t0);
        }
    }
    topic0s.sort_unstable();
    topic0s.dedup();

    if topic0s.is_empty() {
        return events.iter().map(|_| None).collect();
    }

    let candidates = match lookup_topic0s(client, &topic0s).await {
        Ok(m) => m,
        Err(e) => {
            warn!(error = %e, "signature lookup failed, skipping event decode");
            return events.iter().map(|_| None).collect();
        }
    };

    events
        .iter()
        .map(|ev| {
            let t0 = ev.topic0()?;
            let sigs = candidates.get(&t0)?;
            sigs.iter().find_map(|s| try_decode_event(s, ev))
                .or_else(|| {
                    // No candidate fully decoded — return name+sig only so
                    // callers still see the human-readable event identity.
                    sigs.iter().find_map(|s| event_identity_only(s))
                })
        })
        .collect()
}

impl EventInput {
    fn topic0(&self) -> Option<[u8; 32]> {
        let t = self.topics.first()?;
        <[u8; 32]>::try_from(t.as_slice()).ok()
    }
}

async fn lookup_topic0s(
    client: &Client,
    topics: &[[u8; 32]],
) -> Result<HashMap<[u8; 32], Vec<String>>, tokio_postgres::Error> {
    let byte_slices: Vec<&[u8]> = topics.iter().map(|t| t.as_slice()).collect();
    let rows = client
        .query(
            "SELECT signature_hash_32, signature \
             FROM signatures \
             WHERE signature_hash_32 = ANY($1)",
            &[&byte_slices],
        )
        .await?;

    let mut map: HashMap<[u8; 32], Vec<String>> = HashMap::new();
    for row in rows {
        let hash: &[u8] = row.get(0);
        let sig: String = row.get(1);
        if let Ok(key) = <[u8; 32]>::try_from(hash) {
            map.entry(key).or_default().push(sig);
        }
    }
    Ok(map)
}

/// Try to fully decode `ev` against the canonical signature `text_sig`,
/// using the heuristic that the first `(topics.len() - 1)` params are
/// indexed.
fn try_decode_event(text_sig: &str, ev: &EventInput) -> Option<Decoded> {
    let canon = parse_canonical_event_sig(text_sig)?;
    let indexed_count = ev.topics.len().checked_sub(1)?;
    if indexed_count > canon.params.len() {
        return None;
    }

    let event = build_event_with_indexed(&canon, indexed_count);

    // Build LogData. `topics` needs B256; `data` is the raw bytes.
    let topic_b256: Vec<B256> = ev
        .topics
        .iter()
        .map(|t| B256::try_from(t.as_slice()).ok())
        .collect::<Option<Vec<_>>>()?;
    let log_data = LogData::new_unchecked(topic_b256, ev.data.clone().into());

    let decoded: DecodedEvent = event.decode_log(&log_data).ok()?;

    // `decode_log` returns indexed values and body values separately;
    // re-thread them back into source-declaration order.
    let mut indexed_iter = decoded.indexed.into_iter();
    let mut body_iter = decoded.body.into_iter();
    let inputs = event
        .inputs
        .iter()
        .map(|p| {
            let val = if p.indexed {
                indexed_iter.next()?
            } else {
                body_iter.next()?
            };
            Some(DecodedInput {
                ty: p.ty.clone(),
                value: value_to_json(val),
                ens: None,
                labels: None,
            })
        })
        .collect::<Option<Vec<_>>>()?;

    Some(Decoded {
        name: canon.name,
        signature: text_sig.to_string(),
        inputs,
    })
}

/// Return `Decoded` with only name+signature when arg decoding fails.
fn event_identity_only(text_sig: &str) -> Option<Decoded> {
    let canon = parse_canonical_event_sig(text_sig)?;
    Some(Decoded {
        name: canon.name,
        signature: text_sig.to_string(),
        inputs: Vec::new(),
    })
}

struct CanonicalEvent {
    name: String,
    params: Vec<String>,
}

/// Split `Name(t1,t2,...)` into `(name, [t1,t2,...])`. Only handles
/// top-level commas — nested tuples like `(uint256,uint256)` count as
/// a single param. Returns `None` on malformed input.
fn parse_canonical_event_sig(sig: &str) -> Option<CanonicalEvent> {
    let open = sig.find('(')?;
    if !sig.ends_with(')') {
        return None;
    }
    let name = sig[..open].trim().to_string();
    let args = &sig[open + 1..sig.len() - 1];
    if args.is_empty() {
        return Some(CanonicalEvent { name, params: Vec::new() });
    }
    let mut params = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (i, c) in args.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.checked_sub(1)?,
            ',' if depth == 0 => {
                params.push(args[start..i].trim().to_string());
                start = i + 1;
            }
            _ => {}
        }
    }
    params.push(args[start..].trim().to_string());
    Some(CanonicalEvent { name, params })
}

/// Build an `alloy::json_abi::Event` where the first `indexed_count`
/// params are marked `indexed`. Param names are synthetic (`p0`, `p1`, …).
fn build_event_with_indexed(canon: &CanonicalEvent, indexed_count: usize) -> Event {
    let inputs = canon
        .params
        .iter()
        .enumerate()
        .map(|(i, ty)| EventParam {
            name: format!("p{i}"),
            ty: ty.clone(),
            indexed: i < indexed_count,
            components: Vec::new(),
            internal_type: None,
        })
        .collect();
    Event {
        name: canon.name.clone(),
        inputs,
        anonymous: false,
    }
}

fn value_to_json(v: DynSolValue) -> JsonValue {
    use DynSolValue::*;
    match v {
        Bool(b) => JsonValue::Bool(b),
        Int(i, _) => JsonValue::String(i.to_string()),
        Uint(u, _) => JsonValue::String(u.to_string()),
        FixedBytes(b, size) => {
            JsonValue::String(format!("0x{}", hex::encode(&b.as_slice()[..size])))
        }
        Address(a) => JsonValue::String(format!("0x{}", hex::encode(a.as_slice()))),
        Function(f) => JsonValue::String(format!("0x{}", hex::encode(f.as_slice()))),
        Bytes(b) => JsonValue::String(format!("0x{}", hex::encode(b))),
        String(s) => JsonValue::String(s),
        Array(vals) | FixedArray(vals) | Tuple(vals) => {
            JsonValue::Array(vals.into_iter().map(value_to_json).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex_bytes(s: &str) -> Vec<u8> {
        hex::decode(s.trim_start_matches("0x")).expect("valid hex")
    }

    #[test]
    fn decodes_erc20_transfer() {
        // transfer(address,uint256) to 0xabcdef…01 of 1e18
        let input = hex_bytes(
            "a9059cbb\
             000000000000000000000000abcdef0123456789abcdef0123456789abcdef01\
             0000000000000000000000000000000000000000000000000de0b6b3a7640000",
        );
        let decoded = try_decode_function("transfer(address,uint256)", &input[4..])
            .expect("decode succeeds");
        assert_eq!(decoded.name, "transfer");
        assert_eq!(decoded.signature, "transfer(address,uint256)");
        assert_eq!(decoded.inputs.len(), 2);
        assert_eq!(decoded.inputs[0].ty, "address");
        assert_eq!(
            decoded.inputs[0].value,
            JsonValue::String("0xabcdef0123456789abcdef0123456789abcdef01".to_string())
        );
        assert_eq!(decoded.inputs[1].ty, "uint256");
        assert_eq!(
            decoded.inputs[1].value,
            JsonValue::String("1000000000000000000".to_string())
        );
    }

    #[test]
    fn rejects_wrong_signature_shape() {
        // Real transfer(address,uint256) calldata. A colliding-selector
        // candidate that wants different types must fail to parse.
        let args = hex_bytes(
            "000000000000000000000000abcdef0123456789abcdef0123456789abcdef01\
             0000000000000000000000000000000000000000000000000de0b6b3a7640000",
        );
        // 3 static args → needs 96 bytes of body, we only have 64.
        assert!(try_decode_function("foo(address,uint256,uint256)", &args).is_none());
    }

    #[test]
    fn selector_handles_short_input() {
        assert_eq!(selector_of(&[]), None);
        assert_eq!(selector_of(&[0xa9]), None);
        assert_eq!(selector_of(&[0xa9, 0x05, 0x9c]), None);
        assert_eq!(
            selector_of(&[0xa9, 0x05, 0x9c, 0xbb, 0xff]),
            Some([0xa9, 0x05, 0x9c, 0xbb])
        );
    }

    #[test]
    fn decodes_erc20_transfer_event() {
        // Real ERC-20 Transfer: topic0 = keccak256("Transfer(address,address,uint256)")
        // topic1 = from (indexed), topic2 = to (indexed), data = value.
        let topic0 = hex_bytes("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
        let topic1 = hex_bytes("000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let topic2 = hex_bytes("000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let data = hex_bytes("0000000000000000000000000000000000000000000000000de0b6b3a7640000");

        let ev = EventInput {
            topics: vec![topic0, topic1, topic2],
            data,
        };
        let decoded = try_decode_event("Transfer(address,address,uint256)", &ev)
            .expect("decode succeeds");
        assert_eq!(decoded.name, "Transfer");
        assert_eq!(decoded.inputs.len(), 3);
        assert_eq!(decoded.inputs[0].ty, "address");
        assert_eq!(
            decoded.inputs[0].value,
            JsonValue::String("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string())
        );
        assert_eq!(decoded.inputs[1].ty, "address");
        assert_eq!(
            decoded.inputs[1].value,
            JsonValue::String("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string())
        );
        assert_eq!(decoded.inputs[2].ty, "uint256");
        assert_eq!(
            decoded.inputs[2].value,
            JsonValue::String("1000000000000000000".to_string())
        );
    }

    #[test]
    fn event_identity_fallback_preserves_name() {
        let d = event_identity_only("Transfer(address,address,uint256)")
            .expect("parses");
        assert_eq!(d.name, "Transfer");
        assert_eq!(d.signature, "Transfer(address,address,uint256)");
        assert!(d.inputs.is_empty());
    }

    #[test]
    fn parses_canonical_with_nested_tuples() {
        let canon = parse_canonical_event_sig("Foo(address,(uint256,uint256),bytes)")
            .expect("parses");
        assert_eq!(canon.name, "Foo");
        assert_eq!(canon.params, vec!["address", "(uint256,uint256)", "bytes"]);
    }
}
