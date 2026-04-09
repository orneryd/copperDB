//! PackStream serialization — Bolt's binary encoding format.
//!
//! PackStream is similar to MessagePack but with specific type tags.
//! Used for all data flowing over Bolt connections.
//!
//! ⚠️ **Must be implemented from scratch.**
//! There is no existing Rust PackStream library.
//! Reference: https://7687.org/packstream/packstream-specification-1.html

use bytes::{BufMut, BytesMut};

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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

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
        assert_eq!(buf.len(), 1); // Tiny int: no header byte needed
        assert_eq!(buf[0], 42);
    }
}
