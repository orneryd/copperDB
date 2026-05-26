//! General-purpose utilities for copperdb.
//!
//! Equivalent to Go's `pkg/util` in NornicDB.
//! Provides common helpers used across all other crates.

use serde::de::DeserializeOwned;
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

pub const DEFAULT_MAX_MSGPACK_DECODE_BYTES: i64 = 256 * 1024 * 1024;
pub const MAX_MSGPACK_DECODE_BYTES_ENV_KEY: &str = "COPPERDB_MAX_MSGPACK_DECODE_BYTES";

const FNV_OFFSET_BASIS_64: u64 = 14_695_981_039_346_656_037;
const FNV_PRIME_64: u64 = 1_099_511_628_211;

#[derive(Debug, Error)]
pub enum UtilError {
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("msgpack decode error: {0}")]
    MsgpackDecode(#[from] rmp_serde::decode::Error),
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("invalid msgpack payload size: {0}")]
    InvalidMsgpackPayloadSize(i64),
    #[error("msgpack payload exceeds decode limit ({size} > {limit} bytes)")]
    MsgpackPayloadTooLarge { size: i64, limit: i64 },
}

/// Generate a new random UUID v4.
pub fn new_id() -> String {
    Uuid::new_v4().to_string()
}

/// Convert a string ID to a deterministic positive i64 using FNV-1a 64-bit.
///
/// This mirrors NornicDB's Bolt compatibility boundary: copperDB stores string
/// IDs, while some protocol surfaces need stable integer IDs.
pub fn hash_string_to_i64(s: &str) -> i64 {
    let mut hash = FNV_OFFSET_BASIS_64;
    for byte in s.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME_64);
    }
    (hash & 0x7FFF_FFFF_FFFF_FFFF) as i64
}

pub fn max_msgpack_decode_bytes() -> i64 {
    std::env::var(MAX_MSGPACK_DECODE_BYTES_ENV_KEY)
        .ok()
        .and_then(|raw| raw.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_MSGPACK_DECODE_BYTES)
}

pub fn validate_msgpack_payload_size(size: i64) -> Result<(), UtilError> {
    if size < 0 {
        return Err(UtilError::InvalidMsgpackPayloadSize(size));
    }
    let limit = max_msgpack_decode_bytes();
    if size > limit {
        return Err(UtilError::MsgpackPayloadTooLarge { size, limit });
    }
    Ok(())
}

pub fn decode_msgpack_bytes<T>(data: &[u8]) -> Result<T, UtilError>
where
    T: DeserializeOwned,
{
    validate_msgpack_payload_size(data.len() as i64)?;
    Ok(rmp_serde::from_slice(data)?)
}

/// Flatten a nested map into a dot-separated key structure.
///
/// # Example
/// ```
/// use copperdb_util::flatten_map;
/// let mut map = std::collections::HashMap::new();
/// map.insert("key".to_string(), serde_json::json!({"nested": "value"}));
/// let flat = flatten_map(&map, "");
/// ```
pub fn flatten_map(
    map: &HashMap<String, serde_json::Value>,
    prefix: &str,
) -> HashMap<String, serde_json::Value> {
    let mut result = HashMap::new();
    for (key, value) in map {
        let full_key = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{}.{}", prefix, key)
        };
        match value {
            serde_json::Value::Object(nested) => {
                let nested_map: HashMap<String, serde_json::Value> =
                    nested.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                let flattened = flatten_map(&nested_map, &full_key);
                result.extend(flattened);
            }
            _ => {
                result.insert(full_key, value.clone());
            }
        }
    }
    result
}

/// Merge two JSON objects, with the second overriding the first on conflict.
pub fn merge_json(base: serde_json::Value, overlay: serde_json::Value) -> serde_json::Value {
    match (base, overlay) {
        (serde_json::Value::Object(mut a), serde_json::Value::Object(b)) => {
            for (k, v) in b {
                let entry = a.entry(k).or_insert(serde_json::Value::Null);
                *entry = merge_json(entry.clone(), v);
            }
            serde_json::Value::Object(a)
        }
        (_, overlay) => overlay,
    }
}

/// Truncate a string to at most `max_bytes` UTF-8 bytes, preserving char boundaries.
pub fn truncate_utf8(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut boundary = max_bytes;
    while !s.is_char_boundary(boundary) {
        boundary -= 1;
    }
    &s[..boundary]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_id_is_uuid() {
        let id = new_id();
        assert_eq!(id.len(), 36);
        assert!(id.contains('-'));
    }

    #[test]
    fn hash_string_to_i64_is_deterministic_positive_and_distinct() {
        let first = hash_string_to_i64("user-123");
        let second = hash_string_to_i64("user-123");
        let other = hash_string_to_i64("user-124");
        assert_eq!(first, second);
        assert!(first >= 0);
        assert_ne!(first, other);
    }

    #[test]
    fn msgpack_payload_size_validation_rejects_negative_and_large_values() {
        assert!(validate_msgpack_payload_size(0).is_ok());
        assert!(matches!(
            validate_msgpack_payload_size(-1),
            Err(UtilError::InvalidMsgpackPayloadSize(-1))
        ));
        assert!(matches!(
            validate_msgpack_payload_size(DEFAULT_MAX_MSGPACK_DECODE_BYTES + 1),
            Err(UtilError::MsgpackPayloadTooLarge { .. })
        ));
    }

    #[test]
    fn decode_msgpack_bytes_round_trips_structured_payload() {
        let source = vec!["alpha".to_string(), "beta".to_string()];
        let encoded = rmp_serde::to_vec(&source).unwrap();
        let decoded: Vec<String> = decode_msgpack_bytes(&encoded).unwrap();
        assert_eq!(decoded, source);
    }

    #[test]
    fn test_truncate_utf8() {
        let s = "hello world";
        assert_eq!(truncate_utf8(s, 5), "hello");
        assert_eq!(truncate_utf8(s, 100), s);
    }

    #[test]
    fn test_merge_json() {
        let base = serde_json::json!({"a": 1, "b": 2});
        let overlay = serde_json::json!({"b": 3, "c": 4});
        let merged = merge_json(base, overlay);
        assert_eq!(merged["a"], 1);
        assert_eq!(merged["b"], 3);
        assert_eq!(merged["c"], 4);
    }
}
