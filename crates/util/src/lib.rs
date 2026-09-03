//! General-purpose utilities for copperdb.
//!
//! Equivalent to Go's `pkg/util` in NornicDB.
//! Provides common helpers used across all other crates.

use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
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

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("request cancelled")]
pub struct RequestCancelled;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestCancellationReason {
    Explicit,
    Deadline,
}

impl RequestCancellationReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::Deadline => "deadline",
        }
    }
}

const REQUEST_ACTIVE: u8 = 0;
const REQUEST_CANCELLED_EXPLICITLY: u8 = 1;
const REQUEST_DEADLINE_EXCEEDED: u8 = 2;

#[derive(Debug, Clone, Default)]
pub struct RequestCancellation {
    inner: Arc<AtomicU8>,
}

impl RequestCancellation {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.cancel_with_reason(RequestCancellationReason::Explicit);
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.load(Ordering::Acquire) != REQUEST_ACTIVE
    }

    pub fn cancellation_reason(&self) -> Option<RequestCancellationReason> {
        match self.inner.load(Ordering::Acquire) {
            REQUEST_CANCELLED_EXPLICITLY => Some(RequestCancellationReason::Explicit),
            REQUEST_DEADLINE_EXCEEDED => Some(RequestCancellationReason::Deadline),
            _ => None,
        }
    }

    pub fn check_cancelled(&self) -> Result<(), RequestCancelled> {
        if self.is_cancelled() {
            Err(RequestCancelled)
        } else {
            Ok(())
        }
    }

    fn cancel_with_reason(&self, reason: RequestCancellationReason) {
        let state = match reason {
            RequestCancellationReason::Explicit => REQUEST_CANCELLED_EXPLICITLY,
            RequestCancellationReason::Deadline => REQUEST_DEADLINE_EXCEEDED,
        };
        let _ =
            self.inner
                .compare_exchange(REQUEST_ACTIVE, state, Ordering::AcqRel, Ordering::Acquire);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestContextMetadata {
    pub request_id: String,
    pub deadline_unix_ms: Option<u64>,
    pub language_preferences: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RequestContext {
    request_id: String,
    deadline: Option<SystemTime>,
    cancellation: RequestCancellation,
    parent_cancellation: Option<RequestCancellation>,
    language_preferences: Vec<String>,
}

#[derive(Debug)]
pub struct RequestContextGuard {
    cancellation: RequestCancellation,
}

impl Drop for RequestContextGuard {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

impl RequestContext {
    pub fn root(deadline: Option<SystemTime>) -> (Self, RequestContextGuard) {
        let cancellation = RequestCancellation::new();
        (
            Self {
                request_id: new_id(),
                deadline,
                cancellation: cancellation.clone(),
                parent_cancellation: None,
                language_preferences: Vec::new(),
            },
            RequestContextGuard { cancellation },
        )
    }

    pub fn detached() -> Self {
        Self {
            request_id: new_id(),
            deadline: None,
            cancellation: RequestCancellation::new(),
            parent_cancellation: None,
            language_preferences: Vec::new(),
        }
    }

    pub fn from_metadata(metadata: RequestContextMetadata) -> (Self, RequestContextGuard) {
        let cancellation = RequestCancellation::new();
        (
            Self {
                request_id: metadata.request_id,
                deadline: metadata
                    .deadline_unix_ms
                    .map(|millis| UNIX_EPOCH + Duration::from_millis(millis)),
                cancellation: cancellation.clone(),
                parent_cancellation: None,
                language_preferences: metadata.language_preferences,
            },
            RequestContextGuard { cancellation },
        )
    }

    pub fn child(&self, deadline: Option<SystemTime>) -> (Self, RequestContextGuard) {
        let cancellation = RequestCancellation::new();
        let deadline = match (self.deadline, deadline) {
            (Some(parent), Some(child)) => Some(parent.min(child)),
            (parent, child) => parent.or(child),
        };
        (
            Self {
                request_id: self.request_id.clone(),
                deadline,
                cancellation: cancellation.clone(),
                parent_cancellation: Some(self.cancellation.clone()),
                language_preferences: self.language_preferences.clone(),
            },
            RequestContextGuard { cancellation },
        )
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn with_language_preferences(
        mut self,
        preferences: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.language_preferences = preferences.into_iter().map(Into::into).collect();
        self
    }

    pub fn language_preferences(&self) -> &[String] {
        &self.language_preferences
    }

    pub fn deadline(&self) -> Option<SystemTime> {
        self.deadline
    }

    pub fn metadata(&self) -> RequestContextMetadata {
        RequestContextMetadata {
            request_id: self.request_id.clone(),
            deadline_unix_ms: self.deadline.and_then(system_time_to_unix_ms),
            language_preferences: self.language_preferences.clone(),
        }
    }

    pub fn cancellation(&self) -> &RequestCancellation {
        &self.cancellation
    }

    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub fn cancel_due_to_deadline(&self) {
        self.cancellation
            .cancel_with_reason(RequestCancellationReason::Deadline);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
            || self
                .parent_cancellation
                .as_ref()
                .is_some_and(RequestCancellation::is_cancelled)
    }

    pub fn cancellation_reason(&self) -> Option<RequestCancellationReason> {
        self.cancellation.cancellation_reason()
    }

    pub fn check_active(&self) -> Result<(), RequestCancelled> {
        if let Some(parent) = &self.parent_cancellation {
            parent.check_cancelled()?;
        }
        self.cancellation.check_cancelled()?;
        if self
            .deadline
            .is_some_and(|deadline| SystemTime::now() >= deadline)
        {
            self.cancel_due_to_deadline();
            return Err(RequestCancelled);
        }
        Ok(())
    }
}

fn system_time_to_unix_ms(value: SystemTime) -> Option<u64> {
    value
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
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

    #[test]
    fn request_cancellation_is_shared_across_clones() {
        let cancel = RequestCancellation::new();
        let other = cancel.clone();

        assert!(!cancel.is_cancelled());
        assert!(other.check_cancelled().is_ok());

        cancel.cancel();

        assert!(cancel.is_cancelled());
        assert_eq!(other.check_cancelled(), Err(RequestCancelled));
        assert_eq!(
            other.cancellation_reason(),
            Some(RequestCancellationReason::Explicit)
        );
    }

    #[test]
    fn request_context_preserves_the_first_cancellation_reason() {
        let explicit = RequestContext::detached();
        explicit.cancel();
        explicit.cancel_due_to_deadline();
        assert_eq!(
            explicit.cancellation_reason(),
            Some(RequestCancellationReason::Explicit)
        );

        let (deadline, guard) = RequestContext::root(Some(SystemTime::now()));
        assert_eq!(deadline.check_active(), Err(RequestCancelled));
        assert_eq!(
            deadline.cancellation_reason(),
            Some(RequestCancellationReason::Deadline)
        );
        drop(guard);
        assert_eq!(
            deadline.cancellation_reason(),
            Some(RequestCancellationReason::Deadline)
        );
    }

    #[test]
    fn request_context_guard_cancels_on_drop() {
        let (context, guard) = RequestContext::root(None);
        assert!(context.check_active().is_ok());

        drop(guard);

        assert_eq!(context.check_active(), Err(RequestCancelled));
    }

    #[test]
    fn request_context_round_trips_metadata() {
        let deadline = SystemTime::now() + Duration::from_secs(123);
        let (context, _guard) = RequestContext::root(Some(deadline));
        let metadata = context.metadata();

        let (reconstructed, _reconstructed_guard) = RequestContext::from_metadata(metadata);

        assert_eq!(reconstructed.request_id(), context.request_id());
        assert_eq!(reconstructed.metadata(), context.metadata());
        assert!(reconstructed.check_active().is_ok());
    }
}
