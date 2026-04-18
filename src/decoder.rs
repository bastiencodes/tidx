//! Function-calldata decoding against the `signatures` cache.
//!
//! Given raw `tx.input` bytes, resolves the 4-byte selector to candidate
//! text signatures via the local Sourcify mirror, then uses `alloy-dyn-abi`
//! to try-decode the argument bytes against each candidate and returns the
//! first that parses cleanly.

use std::collections::HashMap;

use alloy::dyn_abi::{DynSolValue, JsonAbiExt};
use alloy::json_abi::Function;
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
            })
            .collect(),
    })
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
}
