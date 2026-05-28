use super::*;

pub(crate) fn property_index_value_key(value: &serde_json::Value) -> String {
    if let Some(key) = ordered_property_index_value_key(value) {
        return key;
    }

    let bytes = serde_json::to_vec(value).unwrap_or_else(|_| value.to_string().into_bytes());
    format!("j{}", hex::encode(bytes))
}

pub(crate) fn property_index_range_scan_bounds(
    prefix: &str,
    comparison: RangeIndexComparison,
    value: &serde_json::Value,
) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
    let value_key = ordered_property_index_value_key(value)?;
    let prefix_bytes = prefix.as_bytes().to_vec();
    let value_prefix = format!("{}{}/", prefix, value_key).into_bytes();
    let prefix_end = prefix_successor(prefix_bytes.clone());

    match comparison {
        RangeIndexComparison::GreaterThan => {
            Some((prefix_successor(value_prefix)?, prefix_end))
        }
        RangeIndexComparison::GreaterThanOrEqual => Some((value_prefix, prefix_end)),
        RangeIndexComparison::LessThan => Some((prefix_bytes, Some(value_prefix))),
        RangeIndexComparison::LessThanOrEqual => {
            Some((prefix_bytes, Some(prefix_successor(value_prefix)?)))
        }
    }
}

fn ordered_property_index_value_key(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Number(number) => {
            let number = number.as_f64()?;
            Some(format!("n{:016x}", ordered_f64_bits(number)))
        }
        serde_json::Value::String(string) => Some(format!("s{}", hex::encode(string.as_bytes()))),
        _ => None,
    }
}

fn ordered_f64_bits(value: f64) -> u64 {
    let bits = value.to_bits();
    if bits & (1_u64 << 63) != 0 {
        !bits
    } else {
        bits ^ (1_u64 << 63)
    }
}

fn prefix_successor(mut bytes: Vec<u8>) -> Option<Vec<u8>> {
    for index in (0..bytes.len()).rev() {
        if bytes[index] != u8::MAX {
            bytes[index] += 1;
            bytes.truncate(index + 1);
            return Some(bytes);
        }
    }
    None
}