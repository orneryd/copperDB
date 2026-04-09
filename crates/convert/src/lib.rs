//! Data-type conversion utilities.
//!
//! Equivalent to Go's `pkg/convert` in NornicDB.
//! Provides conversions between Cypher types, JSON, MessagePack,
//! and native Rust types used throughout the database engine.

use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConvertError {
    #[error("type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: String, got: String },
    #[error("overflow converting value")]
    Overflow,
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("base64 decode error: {0}")]
    Base64(#[from] base64::DecodeError),
}

/// Represents a property value in the graph database.
/// Maps directly to Cypher's type system.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Map(std::collections::HashMap<String, Value>),
}

impl Value {
    /// Attempt to cast to `i64`.
    pub fn as_int(&self) -> Result<i64, ConvertError> {
        match self {
            Value::Integer(n) => Ok(*n),
            Value::Float(f) => Ok(*f as i64),
            other => Err(ConvertError::TypeMismatch {
                expected: "Integer".into(),
                got: format!("{:?}", other),
            }),
        }
    }

    /// Attempt to cast to `f64`.
    pub fn as_float(&self) -> Result<f64, ConvertError> {
        match self {
            Value::Float(f) => Ok(*f),
            Value::Integer(n) => Ok(*n as f64),
            other => Err(ConvertError::TypeMismatch {
                expected: "Float".into(),
                got: format!("{:?}", other),
            }),
        }
    }

    /// Attempt to cast to `&str`.
    pub fn as_str(&self) -> Result<&str, ConvertError> {
        match self {
            Value::String(s) => Ok(s.as_str()),
            other => Err(ConvertError::TypeMismatch {
                expected: "String".into(),
                got: format!("{:?}", other),
            }),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl From<serde_json::Value> for Value {
    fn from(v: serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Integer(i)
                } else {
                    Value::Float(n.as_f64().unwrap_or(f64::NAN))
                }
            }
            serde_json::Value::String(s) => Value::String(s),
            serde_json::Value::Array(arr) => {
                Value::List(arr.into_iter().map(Value::from).collect())
            }
            serde_json::Value::Object(obj) => {
                Value::Map(obj.into_iter().map(|(k, v)| (k, Value::from(v))).collect())
            }
        }
    }
}

/// Encode bytes as base64 standard (used for binary properties).
pub fn bytes_to_base64(data: &[u8]) -> String {
    STANDARD.encode(data)
}

/// Decode base64-encoded bytes.
pub fn base64_to_bytes(s: &str) -> Result<Vec<u8>, ConvertError> {
    Ok(STANDARD.decode(s)?)
}

/// Pack a `Value` into MessagePack bytes (used in Bolt protocol).
pub fn pack_msgpack(value: &Value) -> Result<Bytes, rmp_serde::encode::Error> {
    let encoded = rmp_serde::to_vec(value)?;
    Ok(Bytes::from(encoded))
}

/// Unpack MessagePack bytes into a `Value`.
pub fn unpack_msgpack(data: &[u8]) -> Result<Value, rmp_serde::decode::Error> {
    rmp_serde::from_slice(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_as_int() {
        assert_eq!(Value::Integer(42).as_int().unwrap(), 42);
        assert_eq!(Value::Float(3.9).as_int().unwrap(), 3);
    }

    #[test]
    fn test_base64_roundtrip() {
        let data = b"hello world";
        let enc = bytes_to_base64(data);
        let dec = base64_to_bytes(&enc).unwrap();
        assert_eq!(dec, data);
    }

    #[test]
    fn test_json_to_value() {
        let json = serde_json::json!({"key": 42, "name": "test"});
        let val = Value::from(json);
        assert!(matches!(val, Value::Map(_)));
    }
}
