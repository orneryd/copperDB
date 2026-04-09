//! General-purpose utilities for magnetDB.
//!
//! Equivalent to Go's `pkg/util` in NornicDB.
//! Provides common helpers used across all other crates.

use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum UtilError {
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("invalid input: {0}")]
    InvalidInput(String),
}

/// Generate a new random UUID v4.
pub fn new_id() -> String {
    Uuid::new_v4().to_string()
}

/// Flatten a nested map into a dot-separated key structure.
///
/// # Example
/// ```
/// use magnetdb_util::flatten_map;
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
pub fn merge_json(
    base: serde_json::Value,
    overlay: serde_json::Value,
) -> serde_json::Value {
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
