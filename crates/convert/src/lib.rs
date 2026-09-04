//! Data-type conversion utilities.
//!
//! Equivalent to Go's `pkg/convert` in NornicDB.
//! Provides conversions between Cypher types, JSON, MessagePack,
//! and native Rust types used throughout the database engine.

use std::collections::BTreeMap;

use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use thiserror::Error;

type ParsedNeo4jHeaderToken = (String, Option<String>, BTreeMap<String, String>);

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
    #[error("invalid Neo4j CSV header: {0}")]
    InvalidNeo4jHeader(String),
    #[error("unsupported Neo4j CSV value type: {0}")]
    UnsupportedNeo4jValueType(String),
    #[error("invalid Neo4j CSV value for {value_type}: {value}")]
    InvalidNeo4jValue { value_type: String, value: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Neo4jHeaderTarget {
    Node,
    Relationship,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Neo4jColumnKind {
    Property,
    Id,
    Label,
    Ignore,
    StartId,
    EndId,
    Type,
    NamedEmbedding,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Neo4jColumn {
    pub name: String,
    pub kind: Neo4jColumnKind,
    pub value_type: String,
    pub id_space: Option<String>,
    pub vector_dimensions: Option<usize>,
    pub options: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Neo4jValueOptions {
    pub array_delimiter: char,
    pub vector_delimiter: char,
    pub empty_strings_as_null: bool,
}

impl Default for Neo4jValueOptions {
    fn default() -> Self {
        Self {
            array_delimiter: ';',
            vector_delimiter: ';',
            empty_strings_as_null: false,
        }
    }
}

pub fn parse_neo4j_header(
    fields: &[String],
    target: Neo4jHeaderTarget,
) -> Result<Vec<Neo4jColumn>, ConvertError> {
    let columns: Vec<_> = fields
        .iter()
        .map(|field| parse_neo4j_column(field, target))
        .collect::<Result<_, _>>()?;

    if target == Neo4jHeaderTarget::Relationship
        && (!columns
            .iter()
            .any(|column| column.kind == Neo4jColumnKind::StartId)
            || !columns
                .iter()
                .any(|column| column.kind == Neo4jColumnKind::EndId))
    {
        return Err(ConvertError::InvalidNeo4jHeader(
            "relationship sources require :START_ID and :END_ID columns".into(),
        ));
    }
    Ok(columns)
}

pub fn parse_neo4j_value(
    raw: &str,
    column: &Neo4jColumn,
    options: Neo4jValueOptions,
) -> Result<Value, ConvertError> {
    if column.kind != Neo4jColumnKind::Property && column.kind != Neo4jColumnKind::NamedEmbedding {
        return Err(ConvertError::InvalidNeo4jHeader(format!(
            "column {} does not contain a property value",
            column.name
        )));
    }
    if raw.is_empty() && options.empty_strings_as_null {
        return Ok(Value::Null);
    }
    if let Some(element_type) = column.value_type.strip_suffix("[]") {
        return raw
            .split(options.array_delimiter)
            .map(|part| parse_neo4j_scalar(part, element_type))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::List);
    }
    if column.value_type == "vector" {
        let values = raw
            .split(options.vector_delimiter)
            .map(|part| parse_neo4j_float(part, "vector"))
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(dimensions) = column.vector_dimensions
            && values.len() != dimensions
        {
            return Err(ConvertError::InvalidNeo4jValue {
                value_type: format!("vector[{dimensions}]"),
                value: raw.to_owned(),
            });
        }
        return Ok(Value::List(values.into_iter().map(Value::Float).collect()));
    }
    parse_neo4j_scalar(raw, &column.value_type)
}

fn parse_neo4j_scalar(raw: &str, value_type: &str) -> Result<Value, ConvertError> {
    match value_type.to_ascii_lowercase().as_str() {
        "" | "string" | "char" | "point" | "date" | "localtime" | "time" | "localdatetime"
        | "datetime" | "duration" => Ok(Value::String(raw.to_owned())),
        "byte" | "short" | "int" | "long" => {
            raw.parse()
                .map(Value::Integer)
                .map_err(|_| ConvertError::InvalidNeo4jValue {
                    value_type: value_type.to_owned(),
                    value: raw.to_owned(),
                })
        }
        "float" | "double" => parse_neo4j_float(raw, value_type).map(Value::Float),
        "boolean" => {
            raw.trim()
                .parse()
                .map(Value::Bool)
                .map_err(|_| ConvertError::InvalidNeo4jValue {
                    value_type: value_type.to_owned(),
                    value: raw.to_owned(),
                })
        }
        other => Err(ConvertError::UnsupportedNeo4jValueType(other.to_owned())),
    }
}

fn parse_neo4j_float(raw: &str, value_type: &str) -> Result<f64, ConvertError> {
    raw.trim()
        .parse()
        .map_err(|_| ConvertError::InvalidNeo4jValue {
            value_type: value_type.to_owned(),
            value: raw.to_owned(),
        })
}

fn parse_neo4j_column(field: &str, target: Neo4jHeaderTarget) -> Result<Neo4jColumn, ConvertError> {
    if field.is_empty() {
        return Ok(Neo4jColumn {
            name: String::new(),
            kind: Neo4jColumnKind::Ignore,
            value_type: String::new(),
            id_space: None,
            vector_dimensions: None,
            options: BTreeMap::new(),
        });
    }

    let (name, token) = field.split_once(':').unwrap_or((field, "string"));
    let (keyword, argument, options) = parse_neo4j_header_token(token)?;
    let keyword_upper = keyword.to_ascii_uppercase();
    let (kind, value_type) = if field.starts_with(':') {
        match keyword_upper.as_str() {
            "ID" => (Neo4jColumnKind::Id, String::new()),
            "LABEL" if target == Neo4jHeaderTarget::Node => (Neo4jColumnKind::Label, String::new()),
            "IGNORE" => (Neo4jColumnKind::Ignore, String::new()),
            "START_ID" if target == Neo4jHeaderTarget::Relationship => {
                (Neo4jColumnKind::StartId, String::new())
            }
            "END_ID" if target == Neo4jHeaderTarget::Relationship => {
                (Neo4jColumnKind::EndId, String::new())
            }
            "TYPE" if target == Neo4jHeaderTarget::Relationship => {
                (Neo4jColumnKind::Type, String::new())
            }
            "EMBEDDING" | "NAMED_EMBEDDING" => (Neo4jColumnKind::NamedEmbedding, "vector".into()),
            _ => {
                return Err(ConvertError::InvalidNeo4jHeader(format!(
                    "unsupported column token: {field}"
                )));
            }
        }
    } else if keyword_upper == "ID" {
        (Neo4jColumnKind::Id, String::new())
    } else {
        (Neo4jColumnKind::Property, keyword.to_ascii_lowercase())
    };

    let vector_dimensions = options
        .get("dimensions")
        .map(|value| {
            value.parse::<usize>().map_err(|_| {
                ConvertError::InvalidNeo4jHeader(format!(
                    "invalid vector dimensions in column: {field}"
                ))
            })
        })
        .transpose()?;

    Ok(Neo4jColumn {
        name: if kind == Neo4jColumnKind::NamedEmbedding {
            argument.clone().unwrap_or_else(|| "default".into())
        } else {
            name.to_owned()
        },
        kind,
        value_type,
        id_space: argument,
        vector_dimensions,
        options,
    })
}

fn parse_neo4j_header_token(token: &str) -> Result<ParsedNeo4jHeaderToken, ConvertError> {
    let (base, options) = if let Some(open) = token.find('{') {
        let close = token.rfind('}').ok_or_else(|| {
            ConvertError::InvalidNeo4jHeader(format!("unterminated options: {token}"))
        })?;
        if close != token.len() - 1 {
            return Err(ConvertError::InvalidNeo4jHeader(format!(
                "unexpected suffix after options: {token}"
            )));
        }
        let mut options = BTreeMap::new();
        for entry in token[open + 1..close]
            .split(',')
            .filter(|entry| !entry.is_empty())
        {
            let (key, value) = entry.split_once(':').ok_or_else(|| {
                ConvertError::InvalidNeo4jHeader(format!("invalid option: {entry}"))
            })?;
            options.insert(key.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
        (&token[..open], options)
    } else {
        (token, BTreeMap::new())
    };
    let (keyword, argument) = if let Some(open) = base.find('(') {
        let close = base.rfind(')').ok_or_else(|| {
            ConvertError::InvalidNeo4jHeader(format!("unterminated argument: {token}"))
        })?;
        if close != base.len() - 1 {
            return Err(ConvertError::InvalidNeo4jHeader(format!(
                "unexpected suffix after argument: {token}"
            )));
        }
        (&base[..open], Some(base[open + 1..close].to_owned()))
    } else {
        (base, None)
    };
    if keyword.is_empty() {
        return Err(ConvertError::InvalidNeo4jHeader(
            "empty column token".into(),
        ));
    }
    Ok((keyword.to_owned(), argument, options))
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
    let mut buffer = BytesMut::with_capacity(encoded.len());
    buffer.put_slice(&encoded);
    Ok(buffer.freeze())
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

    #[test]
    fn parses_typed_node_headers() {
        let fields = vec![
            ":ID(Customer)".into(),
            ":LABEL".into(),
            "name:string".into(),
            "embedding:vector{dimensions:3}".into(),
        ];
        let columns = parse_neo4j_header(&fields, Neo4jHeaderTarget::Node).unwrap();

        assert_eq!(columns[0].kind, Neo4jColumnKind::Id);
        assert_eq!(columns[0].id_space.as_deref(), Some("Customer"));
        assert_eq!(columns[1].kind, Neo4jColumnKind::Label);
        assert_eq!(columns[2].value_type, "string");
        assert_eq!(columns[3].vector_dimensions, Some(3));
    }

    #[test]
    fn validates_relationship_endpoint_headers() {
        let fields = vec![
            ":START_ID(Person)".into(),
            ":END_ID(Person)".into(),
            ":TYPE".into(),
        ];
        let columns = parse_neo4j_header(&fields, Neo4jHeaderTarget::Relationship).unwrap();
        assert_eq!(columns[0].kind, Neo4jColumnKind::StartId);
        assert_eq!(columns[1].kind, Neo4jColumnKind::EndId);
        assert_eq!(columns[2].kind, Neo4jColumnKind::Type);

        let error =
            parse_neo4j_header(&[":START_ID".into()], Neo4jHeaderTarget::Relationship).unwrap_err();
        assert!(matches!(error, ConvertError::InvalidNeo4jHeader(_)));
    }

    #[test]
    fn converts_typed_scalar_array_and_vector_values() {
        let fields = vec![
            "age:long".into(),
            "roles:string[]".into(),
            "embedding:vector{dimensions:3}".into(),
        ];
        let columns = parse_neo4j_header(&fields, Neo4jHeaderTarget::Node).unwrap();

        assert_eq!(
            parse_neo4j_value("42", &columns[0], Neo4jValueOptions::default()).unwrap(),
            Value::Integer(42)
        );
        assert_eq!(
            parse_neo4j_value("admin;writer", &columns[1], Neo4jValueOptions::default()).unwrap(),
            Value::List(vec![
                Value::String("admin".into()),
                Value::String("writer".into())
            ])
        );
        assert_eq!(
            parse_neo4j_value("0.1;0.2;0.3", &columns[2], Neo4jValueOptions::default()).unwrap(),
            Value::List(vec![
                Value::Float(0.1),
                Value::Float(0.2),
                Value::Float(0.3)
            ])
        );
    }

    #[test]
    fn rejects_values_with_incompatible_types_or_vector_dimensions() {
        let fields = vec![
            "embedding:vector{dimensions:2}".into(),
            "enabled:boolean".into(),
        ];
        let columns = parse_neo4j_header(&fields, Neo4jHeaderTarget::Node).unwrap();
        let vector_error =
            parse_neo4j_value("0.1", &columns[0], Neo4jValueOptions::default()).unwrap_err();
        assert!(matches!(
            vector_error,
            ConvertError::InvalidNeo4jValue { .. }
        ));
        let boolean_error =
            parse_neo4j_value("not-bool", &columns[1], Neo4jValueOptions::default()).unwrap_err();
        assert!(matches!(
            boolean_error,
            ConvertError::InvalidNeo4jValue { .. }
        ));
    }
}
