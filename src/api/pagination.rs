//! Cursor-based (keyset) pagination helpers shared by list endpoints.
//!
//! Cursors are opaque to clients: a base64url-encoded JSON payload whose shape
//! is chosen per-endpoint. The server controls the schema; clients round-trip
//! the string as-is between requests.
//!
//! # Why keyset and not offset
//! All list endpoints sort by an index-aligned, append-only key
//! (`(block_num, idx)` or `first_transfer_at`). Keyset pagination hits the
//! B-tree seek at any depth, and stays correct while new rows land at the
//! head — offset pagination would duplicate rows every page while indexing is
//! live.
//!
//! # Future: multi-chain cursors
//! We intentionally encode a bare key today (no `v`/`chains` envelope). When
//! multi-chain list endpoints arrive, cursors can evolve to carry per-chain
//! sub-cursors without a client-visible break: server decode branches on the
//! JSON shape, accepts both, and emits the new shape when multiple chains are
//! requested.
//!
//! # Limits
//! Default and max limits are per-endpoint (caller passes them in). The server
//! clamps unconditionally to avoid accidental huge page requests.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{de::DeserializeOwned, Serialize};

use crate::api::ApiError;

/// Shared default page size when a caller omits `limit`. Unified across list
/// endpoints so client behaviour is predictable regardless of which list
/// they're hitting.
pub const DEFAULT_LIMIT: i64 = 100;

/// Hard ceiling on any list endpoint's page size. Individual endpoints may
/// pick a lower max.
pub const MAX_LIMIT: i64 = 1000;

/// Clamp a caller-supplied `limit` into `[1, max]`, defaulting to `default`
/// when absent.
pub fn clamp_limit(requested: Option<i64>, default: i64, max: i64) -> i64 {
    requested.unwrap_or(default).clamp(1, max)
}

/// Encode a cursor payload to a base64url-no-pad JSON string.
pub fn encode<T: Serialize>(payload: &T) -> String {
    let json = serde_json::to_vec(payload).expect("cursor payload should always serialize");
    URL_SAFE_NO_PAD.encode(json)
}

/// Decode a base64url JSON cursor into a typed payload. Returns a
/// [`ApiError::BadRequest`] on malformed input so callers can propagate with
/// `?`.
pub fn decode<T: DeserializeOwned>(raw: &str) -> Result<T, ApiError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| ApiError::BadRequest("invalid cursor: not base64url".to_string()))?;
    serde_json::from_slice::<T>(&bytes)
        .map_err(|_| ApiError::BadRequest("invalid cursor: malformed payload".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct SampleCursor {
        b: i64,
        i: i32,
    }

    #[test]
    fn roundtrips() {
        let c = SampleCursor { b: 1_234_567, i: 42 };
        let encoded = encode(&c);
        let decoded: SampleCursor = decode(&encoded).unwrap();
        assert_eq!(c, decoded);
        assert!(
            !encoded.contains('='),
            "URL_SAFE_NO_PAD should not emit padding"
        );
        assert!(!encoded.contains('+') && !encoded.contains('/'));
    }

    #[test]
    fn rejects_non_base64() {
        let err = decode::<SampleCursor>("not base64!!!").unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)));
    }

    #[test]
    fn rejects_malformed_json() {
        let encoded = URL_SAFE_NO_PAD.encode(b"not json");
        let err = decode::<SampleCursor>(&encoded).unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)));
    }

    #[test]
    fn clamp_limit_defaults_when_absent() {
        assert_eq!(clamp_limit(None, 50, 500), 50);
    }

    #[test]
    fn clamp_limit_clamps_high_and_low() {
        assert_eq!(clamp_limit(Some(0), 50, 500), 1);
        assert_eq!(clamp_limit(Some(-3), 50, 500), 1);
        assert_eq!(clamp_limit(Some(9_999), 50, 500), 500);
    }

    #[test]
    fn clamp_limit_passes_valid_through() {
        assert_eq!(clamp_limit(Some(100), 50, 500), 100);
    }
}
