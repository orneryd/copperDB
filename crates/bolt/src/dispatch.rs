//! Bolt message dispatch — decode PackStream structs into Bolt messages
//! and route them through the session state machine.

use crate::BoltError;
use crate::messages::BoltMessage;
use crate::packstream::Value;
use std::collections::HashMap;

/// Decode a PackStream struct into a BoltMessage.
///
/// Bolt protocol structs are identified by their signature byte.
/// Reference: https://7687.org/bolt/bolt-protocol-message-specification-4.html
pub fn decode_message(signature: u8, fields: &[Value]) -> Result<BoltMessage, BoltError> {
    match signature {
        0x01 => decode_hello(fields),
        0x02 => Err(BoltError::ProtocolViolation("unexpected GOODBYE".into())),
        0x0F => decode_reset(),
        0x10 => decode_run(fields),
        0x2F => decode_discard(fields),
        0x3F => decode_pull(fields),
        0x11 => decode_begin(fields),
        0x12 => decode_commit(),
        0x13 => decode_rollback(),
        0x15 => decode_reset(),
        0x66 => decode_route(fields),
        0x6A => decode_logon(fields),
        0x6B => decode_logoff(),
        sig => Err(BoltError::ProtocolViolation(format!(
            "unknown message signature: 0x{sig:02X}"
        ))),
    }
}

fn decode_hello(fields: &[Value]) -> Result<BoltMessage, BoltError> {
    let extra = extract_map(fields.first(), "HELLO.extra")?;
    Ok(BoltMessage::Hello { extra })
}

fn decode_run(fields: &[Value]) -> Result<BoltMessage, BoltError> {
    let query = extract_string(fields.first(), "RUN.query")?;
    let parameters = match fields.get(1) {
        Some(Value::Map(pairs)) => pairs
            .iter()
            .map(|(k, v)| (k.clone(), value_to_json(v)))
            .collect(),
        _ => HashMap::new(),
    };
    let extra = extract_map(fields.get(2), "RUN.extra")?;
    Ok(BoltMessage::Run {
        query,
        parameters,
        extra,
    })
}

fn decode_pull(fields: &[Value]) -> Result<BoltMessage, BoltError> {
    // Bolt 4.x: PULL has two struct fields (n: Integer, qid: Integer)
    // Bolt 5.x: PULL has one struct field (extra: Map with "n" and "qid")
    if fields.len() == 1
        && let Value::Map(pairs) = &fields[0]
    {
        let n = pairs
            .iter()
            .find(|(k, _)| k == "n")
            .map(|(_, v)| match v {
                Value::Integer(n) => *n,
                _ => -1,
            })
            .unwrap_or(-1);
        let qid = pairs
            .iter()
            .find(|(k, _)| k == "qid")
            .map(|(_, v)| match v {
                Value::Integer(q) => *q,
                _ => -1,
            })
            .unwrap_or(-1);
        return Ok(BoltMessage::Pull { n, qid });
    }
    let n = extract_integer(fields.first(), "PULL.n")?;
    let qid = extract_integer(fields.get(1), "PULL.qid")?;
    Ok(BoltMessage::Pull { n, qid })
}

fn decode_discard(fields: &[Value]) -> Result<BoltMessage, BoltError> {
    // Same dual-format as PULL
    if fields.len() == 1
        && let Value::Map(pairs) = &fields[0]
    {
        let n = pairs
            .iter()
            .find(|(k, _)| k == "n")
            .map(|(_, v)| match v {
                Value::Integer(n) => *n,
                _ => -1,
            })
            .unwrap_or(-1);
        let qid = pairs
            .iter()
            .find(|(k, _)| k == "qid")
            .map(|(_, v)| match v {
                Value::Integer(q) => *q,
                _ => -1,
            })
            .unwrap_or(-1);
        return Ok(BoltMessage::Discard { n, qid });
    }
    let n = extract_integer(fields.first(), "DISCARD.n")?;
    let qid = extract_integer(fields.get(1), "DISCARD.qid")?;
    Ok(BoltMessage::Discard { n, qid })
}

fn decode_begin(fields: &[Value]) -> Result<BoltMessage, BoltError> {
    let extra = extract_map(fields.first(), "BEGIN.extra")?;
    Ok(BoltMessage::Begin { extra })
}

fn decode_commit() -> Result<BoltMessage, BoltError> {
    Ok(BoltMessage::Commit)
}

fn decode_rollback() -> Result<BoltMessage, BoltError> {
    Ok(BoltMessage::Rollback)
}

fn decode_reset() -> Result<BoltMessage, BoltError> {
    Ok(BoltMessage::Reset)
}

fn decode_route(fields: &[Value]) -> Result<BoltMessage, BoltError> {
    let routing = extract_map(fields.first(), "ROUTE.routing")?;
    let bookmarks = extract_string_list(fields.get(1), "ROUTE.bookmarks")?;
    let db = extract_optional_string(fields.get(2));
    Ok(BoltMessage::Route {
        routing,
        bookmarks,
        db,
    })
}

fn decode_logon(fields: &[Value]) -> Result<BoltMessage, BoltError> {
    let auth = extract_string_map(fields.first(), "LOGON.auth")?;
    Ok(BoltMessage::Logon { auth })
}

fn decode_logoff() -> Result<BoltMessage, BoltError> {
    Ok(BoltMessage::Logoff)
}

// ── Value extractors ────────────────────────────────────────────────────────

fn extract_map(
    value: Option<&Value>,
    field: &str,
) -> Result<HashMap<String, serde_json::Value>, BoltError> {
    match value {
        Some(Value::Map(pairs)) => Ok(pairs
            .iter()
            .map(|(k, v)| (k.clone(), value_to_json(v)))
            .collect()),
        None => Ok(HashMap::new()),
        _ => Err(BoltError::ProtocolViolation(format!(
            "{field}: expected map"
        ))),
    }
}

fn extract_string_map(
    value: Option<&Value>,
    field: &str,
) -> Result<HashMap<String, String>, BoltError> {
    match value {
        Some(Value::Map(pairs)) => {
            let mut out = HashMap::new();
            for (k, v) in pairs {
                let s = match v {
                    Value::String(s) => s.clone(),
                    _ => {
                        return Err(BoltError::ProtocolViolation(format!(
                            "{field}.{k}: expected string"
                        )));
                    }
                };
                out.insert(k.clone(), s);
            }
            Ok(out)
        }
        None => Ok(HashMap::new()),
        _ => Err(BoltError::ProtocolViolation(format!(
            "{field}: expected map"
        ))),
    }
}

fn extract_string(value: Option<&Value>, field: &str) -> Result<String, BoltError> {
    match value {
        Some(Value::String(s)) => Ok(s.clone()),
        _ => Err(BoltError::ProtocolViolation(format!(
            "{field}: expected string"
        ))),
    }
}

fn extract_integer(value: Option<&Value>, field: &str) -> Result<i64, BoltError> {
    match value {
        Some(Value::Integer(n)) => Ok(*n),
        None => Ok(0),
        _ => Err(BoltError::ProtocolViolation(format!(
            "{field}: expected integer"
        ))),
    }
}

fn extract_optional_string(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    }
}

fn extract_string_list(value: Option<&Value>, field: &str) -> Result<Vec<String>, BoltError> {
    match value {
        Some(Value::List(items)) => {
            let mut out = Vec::new();
            for item in items {
                match item {
                    Value::String(s) => out.push(s.clone()),
                    _ => {
                        return Err(BoltError::ProtocolViolation(format!(
                            "{field}: expected list of strings"
                        )));
                    }
                }
            }
            Ok(out)
        }
        None => Ok(Vec::new()),
        _ => Err(BoltError::ProtocolViolation(format!(
            "{field}: expected list"
        ))),
    }
}

/// Convert a PackStream Value to a serde_json::Value for Cypher parameters.
pub fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Integer(n) => serde_json::Value::Number((*n).into()),
        Value::Float(f) => serde_json::json!(*f),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => serde_json::Value::Array(
            b.iter()
                .map(|&b| serde_json::Value::Number(b.into()))
                .collect(),
        ),
        Value::List(items) => serde_json::Value::Array(items.iter().map(value_to_json).collect()),
        Value::Map(pairs) => {
            let mut map = serde_json::Map::new();
            for (k, v) in pairs {
                map.insert(k.clone(), value_to_json(v));
            }
            serde_json::Value::Object(map)
        }
        Value::DateTime {
            seconds,
            nanoseconds,
            offset_seconds,
        } => crate::packstream::datetime_to_rfc3339(*seconds, *nanoseconds, *offset_seconds)
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
        Value::LocalDateTime {
            seconds,
            nanoseconds,
        } => crate::packstream::datetime_to_rfc3339(*seconds, *nanoseconds, 0)
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
        Value::Struct { fields, .. } => {
            serde_json::Value::Array(fields.iter().map(value_to_json).collect())
        }
    }
}

/// Encode a BoltMessage into PackStream for writing to the wire.
pub fn encode_message(msg: &BoltMessage) -> Vec<u8> {
    use bytes::BytesMut;
    if let BoltMessage::Record { data } = msg {
        return encode_record(data);
    }

    let mut buf = BytesMut::with_capacity(256);
    match msg {
        BoltMessage::Success { metadata } => {
            crate::packstream::encode_struct_header(&mut buf, 1, 0x70);
            encode_metadata_map(&mut buf, metadata);
        }
        BoltMessage::Failure { metadata } => {
            crate::packstream::encode_struct_header(&mut buf, 1, 0x7F);
            encode_metadata_map(&mut buf, metadata);
        }
        BoltMessage::Ignored => {
            crate::packstream::encode_struct_header(&mut buf, 0, 0x7E);
        }
        BoltMessage::Record { .. } => unreachable!("records are encoded above"),
        _ => {
            // Other message types aren't sent from server to client
            crate::packstream::encode_struct_header(&mut buf, 0, 0x7E);
        }
    }
    buf.to_vec()
}

pub fn encode_record(data: &[serde_json::Value]) -> Vec<u8> {
    use bytes::BytesMut;
    let mut buf = BytesMut::with_capacity(256);
    crate::packstream::encode_struct_header(&mut buf, 1, 0x71);
    crate::packstream::encode_list_header(&mut buf, data.len());
    for field in data {
        encode_json_value(&mut buf, field);
    }
    buf.to_vec()
}

fn encode_metadata_map(buf: &mut bytes::BytesMut, metadata: &HashMap<String, serde_json::Value>) {
    crate::packstream::encode_map_header(buf, metadata.len());
    for (k, v) in metadata {
        crate::packstream::encode_string(buf, k);
        encode_json_value(buf, v);
    }
}

fn encode_json_value(buf: &mut bytes::BytesMut, value: &serde_json::Value) {
    match value {
        serde_json::Value::Null => crate::packstream::encode_null(buf),
        serde_json::Value::Bool(b) => crate::packstream::encode_bool(buf, *b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                crate::packstream::encode_int(buf, i);
            } else if let Some(f) = n.as_f64() {
                crate::packstream::encode_float(buf, f);
            } else {
                crate::packstream::encode_null(buf);
            }
        }
        serde_json::Value::String(s) => {
            if !crate::packstream::encode_rfc3339_datetime_if_valid(buf, s) {
                crate::packstream::encode_string(buf, s);
            }
        }
        serde_json::Value::Array(items) => {
            crate::packstream::encode_list_header(buf, items.len());
            for item in items {
                encode_json_value(buf, item);
            }
        }
        serde_json::Value::Object(map) => {
            crate::packstream::encode_map_header(buf, map.len());
            for (k, v) in map {
                crate::packstream::encode_string(buf, k);
                encode_json_value(buf, v);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packstream::Value;

    #[test]
    fn decode_hello_message() {
        let fields = vec![Value::Map(vec![(
            "user_agent".to_string(),
            Value::String("neo4j/4.4".to_string()),
        )])];
        let msg = decode_message(0x01, &fields).unwrap();
        match msg {
            BoltMessage::Hello { extra } => {
                assert_eq!(extra["user_agent"], serde_json::json!("neo4j/4.4"));
            }
            _ => panic!("expected Hello"),
        }
    }

    #[test]
    fn local_datetime_packstream_param_normalizes_to_rfc3339_json() {
        let value = Value::LocalDateTime {
            seconds: 1_780_315_200,
            nanoseconds: 123_000_000,
        };

        assert_eq!(
            value_to_json(&value),
            serde_json::json!("2026-06-01T12:00:00.123Z")
        );
    }

    #[test]
    fn rfc3339_json_string_encodes_as_bolt_datetime() {
        let mut buf = bytes::BytesMut::new();
        encode_json_value(&mut buf, &serde_json::json!("2026-06-01T12:00:00.123Z"));

        let (decoded, consumed) = crate::packstream::decode(&buf).unwrap();

        assert_eq!(consumed, buf.len());
        assert_eq!(
            decoded,
            Value::DateTime {
                seconds: 1_780_315_200,
                nanoseconds: 123_000_000,
                offset_seconds: 0,
            }
        );
    }

    #[test]
    fn decode_run_message() {
        let fields = vec![
            Value::String("RETURN 1".to_string()),
            Value::Map(vec![]),
            Value::Map(vec![("db".to_string(), Value::String("neo4j".to_string()))]),
        ];
        let msg = decode_message(0x10, &fields).unwrap();
        match msg {
            BoltMessage::Run {
                query,
                parameters: _,
                extra,
            } => {
                assert_eq!(query, "RETURN 1");
                assert_eq!(extra["db"], serde_json::json!("neo4j"));
            }
            _ => panic!("expected Run"),
        }
    }

    #[test]
    fn decode_pull_message() {
        let fields = vec![Value::Integer(1000), Value::Integer(0)];
        let msg = decode_message(0x3F, &fields).unwrap();
        match msg {
            BoltMessage::Pull { n, qid } => {
                assert_eq!(n, 1000);
                assert_eq!(qid, 0);
            }
            _ => panic!("expected Pull"),
        }
    }

    #[test]
    fn decode_begin_commit_rollback() {
        assert!(matches!(
            decode_message(0x11, &[Value::Map(vec![])]).unwrap(),
            BoltMessage::Begin { .. }
        ));
        assert!(matches!(
            decode_message(0x12, &[]).unwrap(),
            BoltMessage::Commit
        ));
        assert!(matches!(
            decode_message(0x13, &[]).unwrap(),
            BoltMessage::Rollback
        ));
        assert!(matches!(
            decode_message(0x0F, &[]).unwrap(),
            BoltMessage::Reset
        ));
        assert!(matches!(
            decode_message(0x15, &[]).unwrap(),
            BoltMessage::Reset
        ));
    }

    #[test]
    fn encode_success_message() {
        let metadata = HashMap::from([("server".to_string(), serde_json::json!("copperdb/1.0"))]);
        let bytes = encode_message(&BoltMessage::Success { metadata });
        // Should start with SUCCESS struct header (0xB1 0x70)
        assert_eq!(bytes[0], 0xB1);
        assert_eq!(bytes[1], 0x70);
    }

    #[test]
    fn encode_failure_message() {
        let metadata = HashMap::from([
            (
                "code".to_string(),
                serde_json::json!("Neo.ClientError.General"),
            ),
            (
                "message".to_string(),
                serde_json::json!("something went wrong"),
            ),
        ]);
        let bytes = encode_message(&BoltMessage::Failure { metadata });
        assert_eq!(bytes[0], 0xB1);
        assert_eq!(bytes[1], 0x7F);
    }

    #[test]
    fn encode_ignored_message() {
        let bytes = encode_message(&BoltMessage::Ignored);
        assert_eq!(bytes[0], 0xB0);
        assert_eq!(bytes[1], 0x7E);
    }

    #[test]
    fn encode_record_message() {
        let data = vec![serde_json::json!(42), serde_json::json!("hello")];
        let bytes = encode_message(&BoltMessage::Record { data });
        assert_eq!(bytes[0], 0xB1);
        assert_eq!(bytes[1], 0x71);
    }

    #[test]
    fn value_to_json_conversions() {
        assert_eq!(value_to_json(&Value::Null), serde_json::json!(null));
        assert_eq!(value_to_json(&Value::Bool(true)), serde_json::json!(true));
        assert_eq!(value_to_json(&Value::Integer(42)), serde_json::json!(42));
        assert_eq!(
            value_to_json(&Value::String("hi".to_string())),
            serde_json::json!("hi")
        );
        assert_eq!(
            value_to_json(&Value::List(vec![Value::Integer(1), Value::Integer(2)])),
            serde_json::json!([1, 2])
        );
    }

    #[test]
    fn decode_unknown_signature() {
        let result = decode_message(0xFF, &[]);
        assert!(result.is_err());
    }

    // ── Wire-format round-trip tests ────────────────────────────────────────

    /// Encode a Bolt struct message as it appears on the wire.
    fn encode_wire_struct(signature: u8, fields: &[Value]) -> Vec<u8> {
        let mut buf = bytes::BytesMut::with_capacity(256);
        crate::packstream::encode_struct_header(&mut buf, fields.len(), signature);
        for field in fields {
            encode_value(&mut buf, field);
        }
        buf.to_vec()
    }

    fn encode_value(buf: &mut bytes::BytesMut, value: &Value) {
        match value {
            Value::Null => crate::packstream::encode_null(buf),
            Value::Bool(b) => crate::packstream::encode_bool(buf, *b),
            Value::Integer(n) => crate::packstream::encode_int(buf, *n),
            Value::Float(f) => crate::packstream::encode_float(buf, *f),
            Value::String(s) => crate::packstream::encode_string(buf, s),
            Value::Bytes(b) => crate::packstream::encode_bytes(buf, b),
            Value::List(items) => {
                crate::packstream::encode_list_header(buf, items.len());
                for item in items {
                    encode_value(buf, item);
                }
            }
            Value::Map(pairs) => {
                crate::packstream::encode_map_header(buf, pairs.len());
                for (k, v) in pairs {
                    crate::packstream::encode_string(buf, k);
                    encode_value(buf, v);
                }
            }
            Value::DateTime {
                seconds,
                nanoseconds,
                offset_seconds,
            } => crate::packstream::encode_datetime(buf, *seconds, *nanoseconds, *offset_seconds),
            Value::LocalDateTime {
                seconds,
                nanoseconds,
            } => crate::packstream::encode_local_datetime(buf, *seconds, *nanoseconds),
            Value::Struct { signature, fields } => {
                crate::packstream::encode_struct_header(buf, fields.len(), *signature);
                for f in fields {
                    encode_value(buf, f);
                }
            }
        }
    }

    #[test]
    fn round_trip_hello_wire_format() {
        // Encode HELLO { user_agent: "neo4j/4.4" }
        let fields = vec![Value::Map(vec![(
            "user_agent".to_string(),
            Value::String("neo4j/4.4".to_string()),
        )])];
        let wire_bytes = encode_wire_struct(0x01, &fields);

        // Decode from wire bytes
        let (value, consumed) = crate::packstream::decode(&wire_bytes).unwrap();
        assert_eq!(consumed, wire_bytes.len());

        match &value {
            Value::Struct { signature, fields } => {
                assert_eq!(*signature, 0x01);
                let msg = decode_message(0x01, fields).unwrap();
                match msg {
                    BoltMessage::Hello { extra } => {
                        assert_eq!(
                            extra.get("user_agent").unwrap(),
                            &serde_json::json!("neo4j/4.4")
                        );
                    }
                    _ => panic!("expected Hello"),
                }
            }
            _ => panic!("expected Struct, got {:?}", value),
        }
    }

    #[test]
    fn round_trip_run_wire_format() {
        // Encode RUN "RETURN 1" {} {}
        let fields = vec![
            Value::String("RETURN 1".to_string()),
            Value::Map(vec![]),
            Value::Map(vec![("db".to_string(), Value::String("neo4j".to_string()))]),
        ];
        let wire_bytes = encode_wire_struct(0x10, &fields);

        let (value, consumed) = crate::packstream::decode(&wire_bytes).unwrap();
        assert_eq!(consumed, wire_bytes.len());

        let msg = match &value {
            Value::Struct {
                signature: 0x10,
                fields,
            } => decode_message(0x10, fields).unwrap(),
            _ => panic!("expected RUN struct"),
        };
        match msg {
            BoltMessage::Run { query, extra, .. } => {
                assert_eq!(query, "RETURN 1");
                assert_eq!(extra.get("db").unwrap(), &serde_json::json!("neo4j"));
            }
            _ => panic!("expected Run"),
        }
    }

    #[test]
    fn round_trip_pull_wire_format() {
        // Encode PULL { n: 1000, qid: 0 }
        let fields = vec![Value::Map(vec![
            ("n".to_string(), Value::Integer(1000)),
            ("qid".to_string(), Value::Integer(0)),
        ])];
        let wire_bytes = encode_wire_struct(0x3F, &fields);

        let (value, consumed) = crate::packstream::decode(&wire_bytes).unwrap();
        assert_eq!(consumed, wire_bytes.len());

        let msg = match &value {
            Value::Struct {
                signature: 0x3F,
                fields,
            } => decode_message(0x3F, fields).unwrap(),
            _ => panic!("expected PULL struct"),
        };
        match msg {
            BoltMessage::Pull { n, qid } => {
                assert_eq!(n, 1000);
                assert_eq!(qid, 0);
            }
            _ => panic!("expected Pull"),
        }
    }

    #[test]
    fn round_trip_hello_response_is_valid_packstream() {
        // Simulate what process_message returns for HELLO
        let response_bytes = encode_message(&BoltMessage::Success {
            metadata: HashMap::from([
                ("server".to_string(), serde_json::json!("copperdb/1.0")),
                ("connection_id".to_string(), serde_json::json!("bolt-0")),
            ]),
        });

        // Response must be decodable as PackStream
        let (value, consumed) = crate::packstream::decode(&response_bytes).unwrap();
        assert_eq!(consumed, response_bytes.len());

        // Must be a SUCCESS struct (0xB1 0x70 = struct with 1 field, signature 0x70)
        match &value {
            Value::Struct { signature, fields } => {
                assert_eq!(*signature, 0x70, "expected SUCCESS signature 0x70");
                assert_eq!(
                    fields.len(),
                    1,
                    "SUCCESS should have 1 field (metadata map)"
                );
            }
            _ => panic!("expected Struct, got {:?}", value),
        }
    }

    #[test]
    fn full_hello_run_pull_message_flow() {
        // Simulate the full client message flow:
        // 1. HELLO → should produce SUCCESS
        // 2. RUN → should produce SUCCESS
        // 3. PULL → should produce SUCCESS (stream done)

        // HELLO
        let hello_bytes = encode_wire_struct(
            0x01,
            &[Value::Map(vec![(
                "user_agent".to_string(),
                Value::String("neo4j-test/1.0".to_string()),
            )])],
        );
        let (hello_val, _) = crate::packstream::decode(&hello_bytes).unwrap();
        let hello_msg = match &hello_val {
            Value::Struct {
                signature: 0x01,
                fields,
            } => decode_message(0x01, fields).unwrap(),
            _ => panic!("expected HELLO"),
        };
        assert!(matches!(hello_msg, BoltMessage::Hello { .. }));

        // RUN
        let run_bytes = encode_wire_struct(
            0x10,
            &[
                Value::String("RETURN 1 AS n".to_string()),
                Value::Map(vec![]),
                Value::Map(vec![]),
            ],
        );
        let (run_val, _) = crate::packstream::decode(&run_bytes).unwrap();
        let run_msg = match &run_val {
            Value::Struct {
                signature: 0x10,
                fields,
            } => decode_message(0x10, fields).unwrap(),
            _ => panic!("expected RUN"),
        };
        assert!(matches!(run_msg, BoltMessage::Run { .. }));

        // PULL
        let pull_bytes = encode_wire_struct(
            0x3F,
            &[Value::Map(vec![("n".to_string(), Value::Integer(-1))])],
        );
        let (pull_val, _) = crate::packstream::decode(&pull_bytes).unwrap();
        let pull_msg = match &pull_val {
            Value::Struct {
                signature: 0x3F,
                fields,
            } => decode_message(0x3F, fields).unwrap(),
            _ => panic!("expected PULL"),
        };
        assert!(matches!(pull_msg, BoltMessage::Pull { .. }));
    }
}
