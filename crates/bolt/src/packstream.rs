//! PackStream serialization — Bolt's binary encoding format.
//!
//! PackStream is similar to MessagePack but with specific type tags.
//! Used for all data flowing over Bolt connections.
//!
//! ⚠️ **Must be implemented from scratch.**
//! There is no existing Rust PackStream library.
//! Reference: https://7687.org/packstream/packstream-specification-1.html

use bytes::{BufMut, BytesMut};
use time::{OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339};

const DATETIME_UTC_PATCHED_SIGNATURE: u8 = 0x49;
const DATETIME_LEGACY_SIGNATURE: u8 = 0x46;
const LOCAL_DATETIME_SIGNATURE: u8 = 0x64;

/// PackStream type markers (from the Bolt spec).
pub mod markers {
    pub const TINY_STRING_BASE: u8 = 0x80;
    pub const TINY_LIST_BASE: u8 = 0x90;
    pub const TINY_MAP_BASE: u8 = 0xA0;
    pub const TINY_STRUCT_BASE: u8 = 0xB0;
    pub const NULL: u8 = 0xC0;
    pub const FLOAT_64: u8 = 0xC1;
    pub const FALSE: u8 = 0xC2;
    pub const TRUE: u8 = 0xC3;
    pub const INT_8: u8 = 0xC8;
    pub const INT_16: u8 = 0xC9;
    pub const INT_32: u8 = 0xCA;
    pub const INT_64: u8 = 0xCB;
    pub const BYTES_8: u8 = 0xCC;
    pub const BYTES_16: u8 = 0xCD;
    pub const BYTES_32: u8 = 0xCE;
    pub const STRING_8: u8 = 0xD0;
    pub const STRING_16: u8 = 0xD1;
    pub const STRING_32: u8 = 0xD2;
    pub const LIST_8: u8 = 0xD4;
    pub const LIST_16: u8 = 0xD5;
    pub const LIST_32: u8 = 0xD6;
    pub const MAP_8: u8 = 0xD8;
    pub const MAP_16: u8 = 0xD9;
    pub const MAP_32: u8 = 0xDA;
    pub const STRUCT_8: u8 = 0xDC;
    pub const STRUCT_16: u8 = 0xDD;
}

/// Encode a null value.
pub fn encode_null(buf: &mut BytesMut) {
    buf.put_u8(markers::NULL);
}

/// Encode a boolean.
pub fn encode_bool(buf: &mut BytesMut, value: bool) {
    buf.put_u8(if value { markers::TRUE } else { markers::FALSE });
}

/// Encode an integer (choosing the smallest representation).
pub fn encode_int(buf: &mut BytesMut, value: i64) {
    if (-16..=127).contains(&value) {
        buf.put_i8(value as i8);
    } else if i8::MIN as i64 <= value && value <= i8::MAX as i64 {
        buf.put_u8(markers::INT_8);
        buf.put_i8(value as i8);
    } else if i16::MIN as i64 <= value && value <= i16::MAX as i64 {
        buf.put_u8(markers::INT_16);
        buf.put_i16(value as i16);
    } else if i32::MIN as i64 <= value && value <= i32::MAX as i64 {
        buf.put_u8(markers::INT_32);
        buf.put_i32(value as i32);
    } else {
        buf.put_u8(markers::INT_64);
        buf.put_i64(value);
    }
}

/// Encode a UTF-8 string.
pub fn encode_string(buf: &mut BytesMut, value: &str) {
    let bytes = value.as_bytes();
    let len = bytes.len();
    if len <= 15 {
        buf.put_u8(markers::TINY_STRING_BASE | len as u8);
    } else if len <= 255 {
        buf.put_u8(markers::STRING_8);
        buf.put_u8(len as u8);
    } else if len <= 65535 {
        buf.put_u8(markers::STRING_16);
        buf.put_u16(len as u16);
    } else {
        buf.put_u8(markers::STRING_32);
        buf.put_u32(len as u32);
    }
    buf.put_slice(bytes);
}

/// Encode a float64.
pub fn encode_float(buf: &mut BytesMut, value: f64) {
    buf.put_u8(markers::FLOAT_64);
    buf.put_f64(value);
}

/// Encode a byte array.
pub fn encode_bytes(buf: &mut BytesMut, value: &[u8]) {
    let len = value.len();
    if len <= 255 {
        buf.put_u8(markers::BYTES_8);
        buf.put_u8(len as u8);
    } else if len <= 65535 {
        buf.put_u8(markers::BYTES_16);
        buf.put_u16(len as u16);
    } else {
        buf.put_u8(markers::BYTES_32);
        buf.put_u32(len as u32);
    }
    buf.put_slice(value);
}

/// Encode a list header (then encode each element separately).
pub fn encode_list_header(buf: &mut BytesMut, len: usize) {
    if len <= 15 {
        buf.put_u8(markers::TINY_LIST_BASE | len as u8);
    } else if len <= 255 {
        buf.put_u8(markers::LIST_8);
        buf.put_u8(len as u8);
    } else if len <= 65535 {
        buf.put_u8(markers::LIST_16);
        buf.put_u16(len as u16);
    } else {
        buf.put_u8(markers::LIST_32);
        buf.put_u32(len as u32);
    }
}

/// Encode a map header (then encode key/value pairs separately).
pub fn encode_map_header(buf: &mut BytesMut, len: usize) {
    if len <= 15 {
        buf.put_u8(markers::TINY_MAP_BASE | len as u8);
    } else if len <= 255 {
        buf.put_u8(markers::MAP_8);
        buf.put_u8(len as u8);
    } else if len <= 65535 {
        buf.put_u8(markers::MAP_16);
        buf.put_u16(len as u16);
    } else {
        buf.put_u8(markers::MAP_32);
        buf.put_u32(len as u32);
    }
}

/// Encode a struct header with signature byte.
pub fn encode_struct_header(buf: &mut BytesMut, fields: usize, signature: u8) {
    if fields <= 15 {
        buf.put_u8(markers::TINY_STRUCT_BASE | fields as u8);
    } else if fields <= 255 {
        buf.put_u8(markers::STRUCT_8);
        buf.put_u8(fields as u8);
    } else {
        buf.put_u8(markers::STRUCT_16);
        buf.put_u16(fields as u16);
    }
    buf.put_u8(signature);
}

/// PackStream value — the decoded representation of any PackStream type.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<Value>),
    Map(Vec<(String, Value)>),
    DateTime {
        seconds: i64,
        nanoseconds: i64,
        offset_seconds: i64,
    },
    LocalDateTime {
        seconds: i64,
        nanoseconds: i64,
    },
    Struct {
        signature: u8,
        fields: Vec<Value>,
    },
}

/// Decode a PackStream value from a byte slice. Returns the value and bytes consumed.
pub fn decode(data: &[u8]) -> Result<(Value, usize), crate::BoltError> {
    if data.is_empty() {
        return Err(crate::BoltError::PackStream(
            "unexpected end of data".into(),
        ));
    }
    let marker = data[0];
    match marker {
        markers::NULL => Ok((Value::Null, 1)),
        markers::TRUE => Ok((Value::Bool(true), 1)),
        markers::FALSE => Ok((Value::Bool(false), 1)),
        markers::FLOAT_64 => {
            if data.len() < 9 {
                return Err(crate::BoltError::PackStream("truncated float64".into()));
            }
            let v = f64::from_bits(u64::from_be_bytes(data[1..9].try_into().unwrap()));
            Ok((Value::Float(v), 9))
        }
        markers::INT_8 => {
            if data.len() < 2 {
                return Err(crate::BoltError::PackStream("truncated int8".into()));
            }
            Ok((Value::Integer(data[1] as i8 as i64), 2))
        }
        markers::INT_16 => {
            if data.len() < 3 {
                return Err(crate::BoltError::PackStream("truncated int16".into()));
            }
            let v = i16::from_be_bytes([data[1], data[2]]);
            Ok((Value::Integer(v as i64), 3))
        }
        markers::INT_32 => {
            if data.len() < 5 {
                return Err(crate::BoltError::PackStream("truncated int32".into()));
            }
            let v = i32::from_be_bytes([data[1], data[2], data[3], data[4]]);
            Ok((Value::Integer(v as i64), 5))
        }
        markers::INT_64 => {
            if data.len() < 9 {
                return Err(crate::BoltError::PackStream("truncated int64".into()));
            }
            let v = i64::from_be_bytes(data[1..9].try_into().unwrap());
            Ok((Value::Integer(v), 9))
        }
        markers::BYTES_8 => {
            if data.len() < 2 {
                return Err(crate::BoltError::PackStream(
                    "truncated bytes8 header".into(),
                ));
            }
            let len = data[1] as usize;
            decode_bytes_body(data, 2, len)
        }
        markers::BYTES_16 => {
            if data.len() < 3 {
                return Err(crate::BoltError::PackStream(
                    "truncated bytes16 header".into(),
                ));
            }
            let len = u16::from_be_bytes([data[1], data[2]]) as usize;
            decode_bytes_body(data, 3, len)
        }
        markers::BYTES_32 => {
            if data.len() < 5 {
                return Err(crate::BoltError::PackStream(
                    "truncated bytes32 header".into(),
                ));
            }
            let len = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
            decode_bytes_body(data, 5, len)
        }
        markers::STRING_8 => {
            if data.len() < 2 {
                return Err(crate::BoltError::PackStream(
                    "truncated string8 header".into(),
                ));
            }
            let len = data[1] as usize;
            decode_string_body(data, 2, len)
        }
        markers::STRING_16 => {
            if data.len() < 3 {
                return Err(crate::BoltError::PackStream(
                    "truncated string16 header".into(),
                ));
            }
            let len = u16::from_be_bytes([data[1], data[2]]) as usize;
            decode_string_body(data, 3, len)
        }
        markers::STRING_32 => {
            if data.len() < 5 {
                return Err(crate::BoltError::PackStream(
                    "truncated string32 header".into(),
                ));
            }
            let len = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
            decode_string_body(data, 5, len)
        }
        markers::LIST_8 => {
            if data.len() < 2 {
                return Err(crate::BoltError::PackStream(
                    "truncated list8 header".into(),
                ));
            }
            let count = data[1] as usize;
            decode_list_body(data, 2, count)
        }
        markers::LIST_16 => {
            if data.len() < 3 {
                return Err(crate::BoltError::PackStream(
                    "truncated list16 header".into(),
                ));
            }
            let count = u16::from_be_bytes([data[1], data[2]]) as usize;
            decode_list_body(data, 3, count)
        }
        markers::LIST_32 => {
            if data.len() < 5 {
                return Err(crate::BoltError::PackStream(
                    "truncated list32 header".into(),
                ));
            }
            let count = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
            decode_list_body(data, 5, count)
        }
        markers::MAP_8 => {
            if data.len() < 2 {
                return Err(crate::BoltError::PackStream("truncated map8 header".into()));
            }
            let count = data[1] as usize;
            decode_map_body(data, 2, count)
        }
        markers::MAP_16 => {
            if data.len() < 3 {
                return Err(crate::BoltError::PackStream(
                    "truncated map16 header".into(),
                ));
            }
            let count = u16::from_be_bytes([data[1], data[2]]) as usize;
            decode_map_body(data, 3, count)
        }
        markers::MAP_32 => {
            if data.len() < 5 {
                return Err(crate::BoltError::PackStream(
                    "truncated map32 header".into(),
                ));
            }
            let count = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
            decode_map_body(data, 5, count)
        }
        markers::STRUCT_8 => {
            if data.len() < 3 {
                return Err(crate::BoltError::PackStream(
                    "truncated struct8 header".into(),
                ));
            }
            let fields = data[1] as usize;
            let sig = data[2];
            decode_struct_body(data, 3, fields, sig)
        }
        markers::STRUCT_16 => {
            if data.len() < 4 {
                return Err(crate::BoltError::PackStream(
                    "truncated struct16 header".into(),
                ));
            }
            let fields = u16::from_be_bytes([data[1], data[2]]) as usize;
            let sig = data[3];
            decode_struct_body(data, 4, fields, sig)
        }
        b if b & 0xF0 == markers::TINY_STRING_BASE => {
            let len = (b & 0x0F) as usize;
            decode_string_body(data, 1, len)
        }
        b if b & 0xF0 == markers::TINY_LIST_BASE => {
            let count = (b & 0x0F) as usize;
            decode_list_body(data, 1, count)
        }
        b if b & 0xF0 == markers::TINY_MAP_BASE => {
            let count = (b & 0x0F) as usize;
            decode_map_body(data, 1, count)
        }
        b if b & 0xF0 == markers::TINY_STRUCT_BASE => {
            let fields = (b & 0x0F) as usize;
            if data.len() < 2 {
                return Err(crate::BoltError::PackStream("truncated tiny struct".into()));
            }
            let sig = data[1];
            decode_struct_body(data, 2, fields, sig)
        }
        // Tiny int: 0..=127 encoded as raw u8, and -16..=-1 encoded as 0xF0..=0xFF.
        b @ 0x00..=0x7F => Ok((Value::Integer(b as i64), 1)),
        b @ 0xF0..=0xFF => Ok((Value::Integer(b as i8 as i64), 1)),
        b => Err(crate::BoltError::PackStream(format!(
            "unknown or reserved marker: 0x{b:02X}"
        ))),
    }
}

fn decode_bytes_body(
    data: &[u8],
    offset: usize,
    len: usize,
) -> Result<(Value, usize), crate::BoltError> {
    if data.len() < offset + len {
        return Err(crate::BoltError::PackStream("truncated bytes body".into()));
    }
    Ok((
        Value::Bytes(data[offset..offset + len].to_vec()),
        offset + len,
    ))
}

fn decode_string_body(
    data: &[u8],
    offset: usize,
    len: usize,
) -> Result<(Value, usize), crate::BoltError> {
    if data.len() < offset + len {
        return Err(crate::BoltError::PackStream("truncated string body".into()));
    }
    let s = std::str::from_utf8(&data[offset..offset + len])
        .map_err(|e| crate::BoltError::PackStream(format!("invalid UTF-8: {e}")))?;
    Ok((Value::String(s.to_string()), offset + len))
}

fn decode_list_body(
    data: &[u8],
    offset: usize,
    count: usize,
) -> Result<(Value, usize), crate::BoltError> {
    let mut items = Vec::with_capacity(count);
    let mut pos = offset;
    for _ in 0..count {
        let (val, consumed) = decode(&data[pos..])?;
        items.push(val);
        pos += consumed;
    }
    Ok((Value::List(items), pos))
}

fn decode_map_body(
    data: &[u8],
    offset: usize,
    count: usize,
) -> Result<(Value, usize), crate::BoltError> {
    let mut pairs = Vec::with_capacity(count);
    let mut pos = offset;
    for _ in 0..count {
        let (key_val, key_consumed) = decode(&data[pos..])?;
        pos += key_consumed;
        let key = match key_val {
            Value::String(s) => s,
            _ => {
                return Err(crate::BoltError::PackStream(
                    "map key must be string".into(),
                ));
            }
        };
        let (val, val_consumed) = decode(&data[pos..])?;
        pos += val_consumed;
        pairs.push((key, val));
    }
    Ok((Value::Map(pairs), pos))
}

fn decode_struct_body(
    data: &[u8],
    offset: usize,
    field_count: usize,
    signature: u8,
) -> Result<(Value, usize), crate::BoltError> {
    let mut fields = Vec::with_capacity(field_count);
    let mut pos = offset;
    for _ in 0..field_count {
        let (val, consumed) = decode(&data[pos..])?;
        fields.push(val);
        pos += consumed;
    }
    if let Some(value) = decode_temporal_struct(signature, &fields) {
        return Ok((value, pos));
    }
    Ok((Value::Struct { signature, fields }, pos))
}

fn decode_temporal_struct(signature: u8, fields: &[Value]) -> Option<Value> {
    match signature {
        DATETIME_UTC_PATCHED_SIGNATURE | DATETIME_LEGACY_SIGNATURE if fields.len() >= 3 => {
            Some(Value::DateTime {
                seconds: integer_field(&fields[0])?,
                nanoseconds: integer_field(&fields[1])?,
                offset_seconds: integer_field(&fields[2])?,
            })
        }
        LOCAL_DATETIME_SIGNATURE if fields.len() >= 2 => Some(Value::LocalDateTime {
            seconds: integer_field(&fields[0])?,
            nanoseconds: integer_field(&fields[1])?,
        }),
        _ => None,
    }
}

fn integer_field(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(value) => Some(*value),
        _ => None,
    }
}

/// Encode a `Value` into a `BytesMut` buffer.
pub fn encode_value(buf: &mut BytesMut, value: &Value) {
    match value {
        Value::Null => encode_null(buf),
        Value::Bool(b) => encode_bool(buf, *b),
        Value::Integer(i) => encode_int(buf, *i),
        Value::Float(f) => encode_float(buf, *f),
        Value::Bytes(b) => encode_bytes(buf, b),
        Value::String(s) => encode_string(buf, s),
        Value::List(items) => {
            encode_list_header(buf, items.len());
            for item in items {
                encode_value(buf, item);
            }
        }
        Value::Map(pairs) => {
            encode_map_header(buf, pairs.len());
            for (k, v) in pairs {
                encode_string(buf, k);
                encode_value(buf, v);
            }
        }
        Value::DateTime {
            seconds,
            nanoseconds,
            offset_seconds,
        } => encode_datetime(buf, *seconds, *nanoseconds, *offset_seconds),
        Value::LocalDateTime {
            seconds,
            nanoseconds,
        } => encode_local_datetime(buf, *seconds, *nanoseconds),
        Value::Struct { signature, fields } => {
            encode_struct_header(buf, fields.len(), *signature);
            for f in fields {
                encode_value(buf, f);
            }
        }
    }
}

pub fn encode_datetime(buf: &mut BytesMut, seconds: i64, nanoseconds: i64, offset_seconds: i64) {
    encode_struct_header(buf, 3, DATETIME_UTC_PATCHED_SIGNATURE);
    encode_int(buf, seconds);
    encode_int(buf, nanoseconds);
    encode_int(buf, offset_seconds);
}

pub fn encode_local_datetime(buf: &mut BytesMut, seconds: i64, nanoseconds: i64) {
    encode_struct_header(buf, 2, LOCAL_DATETIME_SIGNATURE);
    encode_int(buf, seconds);
    encode_int(buf, nanoseconds);
}

pub fn encode_rfc3339_datetime_if_valid(buf: &mut BytesMut, value: &str) -> bool {
    let Ok(parsed) = OffsetDateTime::parse(value, &Rfc3339) else {
        return false;
    };
    encode_datetime(
        buf,
        parsed.unix_timestamp(),
        i64::from(parsed.nanosecond()),
        i64::from(parsed.offset().whole_seconds()),
    );
    true
}

pub fn datetime_to_rfc3339(seconds: i64, nanoseconds: i64, offset_seconds: i64) -> Option<String> {
    let offset = UtcOffset::from_whole_seconds(offset_seconds as i32).ok()?;
    let datetime = OffsetDateTime::from_unix_timestamp(seconds)
        .ok()?
        .replace_nanosecond(nanoseconds.try_into().ok()?)
        .ok()?
        .to_offset(offset);
    datetime.format(&Rfc3339).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    fn round_trip(v: &Value) -> Value {
        let mut buf = BytesMut::new();
        encode_value(&mut buf, v);
        let (decoded, consumed) = decode(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        decoded
    }

    #[test]
    fn test_encode_null() {
        let mut buf = BytesMut::new();
        encode_null(&mut buf);
        assert_eq!(buf.as_ref(), &[markers::NULL]);
    }

    #[test]
    fn test_encode_bool_true() {
        let mut buf = BytesMut::new();
        encode_bool(&mut buf, true);
        assert_eq!(buf.as_ref(), &[markers::TRUE]);
    }

    #[test]
    fn test_encode_small_int() {
        let mut buf = BytesMut::new();
        encode_int(&mut buf, 42);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], 42);
    }

    #[test]
    fn test_round_trip_null() {
        assert_eq!(round_trip(&Value::Null), Value::Null);
    }

    #[test]
    fn test_round_trip_bool() {
        assert_eq!(round_trip(&Value::Bool(true)), Value::Bool(true));
        assert_eq!(round_trip(&Value::Bool(false)), Value::Bool(false));
    }

    #[test]
    fn test_round_trip_tiny_int() {
        for i in [-16i64, 0, 42, 127] {
            assert_eq!(round_trip(&Value::Integer(i)), Value::Integer(i));
        }
    }

    #[test]
    fn test_round_trip_int8() {
        assert_eq!(round_trip(&Value::Integer(-100)), Value::Integer(-100));
    }

    #[test]
    fn test_round_trip_int16() {
        assert_eq!(round_trip(&Value::Integer(1000)), Value::Integer(1000));
        assert_eq!(round_trip(&Value::Integer(-1000)), Value::Integer(-1000));
    }

    #[test]
    fn test_round_trip_int32() {
        assert_eq!(
            round_trip(&Value::Integer(100_000)),
            Value::Integer(100_000)
        );
    }

    #[test]
    fn test_round_trip_int64() {
        let large = i64::MAX;
        assert_eq!(round_trip(&Value::Integer(large)), Value::Integer(large));
    }

    #[test]
    fn test_round_trip_float() {
        let v = Value::Float(std::f64::consts::PI);
        let rt = round_trip(&v);
        if let Value::Float(f) = rt {
            assert!((f - std::f64::consts::PI).abs() < 1e-10);
        } else {
            panic!("expected float");
        }
    }

    #[test]
    fn test_round_trip_tiny_string() {
        let v = Value::String("hello".into());
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn test_round_trip_long_string() {
        let s: String = "x".repeat(300);
        let v = Value::String(s);
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn test_round_trip_bytes() {
        let v = Value::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn test_round_trip_tiny_list() {
        let v = Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn test_round_trip_list8() {
        let items: Vec<Value> = (0..20).map(Value::Integer).collect();
        let v = Value::List(items);
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn test_round_trip_tiny_map() {
        let v = Value::Map(vec![
            ("name".into(), Value::String("Alice".into())),
            ("age".into(), Value::Integer(30)),
        ]);
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn test_round_trip_struct() {
        let v = Value::Struct {
            signature: 0x4E, // Node signature
            fields: vec![
                Value::Integer(1),
                Value::List(vec![Value::String("Person".into())]),
                Value::Map(vec![]),
            ],
        };
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn test_decode_local_datetime_structure() {
        let mut buf = BytesMut::new();
        encode_struct_header(&mut buf, 2, LOCAL_DATETIME_SIGNATURE);
        encode_int(&mut buf, 1_780_315_200);
        encode_int(&mut buf, 123_000_000);

        let (decoded, consumed) = decode(&buf).unwrap();

        assert_eq!(consumed, buf.len());
        assert_eq!(
            decoded,
            Value::LocalDateTime {
                seconds: 1_780_315_200,
                nanoseconds: 123_000_000,
            }
        );
    }

    #[test]
    fn test_round_trip_datetime_utc_structure() {
        let v = Value::DateTime {
            seconds: 1_780_315_200,
            nanoseconds: 123_000_000,
            offset_seconds: 0,
        };
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn test_rfc3339_string_encodes_as_datetime_structure() {
        let mut buf = BytesMut::new();
        assert!(encode_rfc3339_datetime_if_valid(
            &mut buf,
            "2026-06-01T12:00:00.123Z"
        ));

        let (decoded, consumed) = decode(&buf).unwrap();

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
    fn test_temporal_values_format_as_rfc3339() {
        assert_eq!(
            datetime_to_rfc3339(1_780_315_200, 123_000_000, 0).unwrap(),
            "2026-06-01T12:00:00.123Z"
        );
    }

    #[test]
    fn test_round_trip_nested() {
        let v = Value::Map(vec![
            (
                "list".into(),
                Value::List(vec![Value::Null, Value::Bool(true), Value::Integer(-5)]),
            ),
            (
                "nested".into(),
                Value::Map(vec![("key".into(), Value::String("val".into()))]),
            ),
        ]);
        assert_eq!(round_trip(&v), v);
    }
}
