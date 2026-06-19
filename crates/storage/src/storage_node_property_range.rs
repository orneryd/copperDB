use super::*;
use crate::storage_property_index_encoding::property_index_range_scan_bounds;
use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeIndexComparison {
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

impl StorageEngine {
    pub fn get_nodes_by_property_range(
        &self,
        label: &str,
        property: &str,
        comparison: RangeIndexComparison,
        value: &serde_json::Value,
    ) -> Result<Vec<NodeRecord>, StorageError> {
        if !self.has_node_property_index(label, property)? {
            return Ok(Vec::new());
        }

        let prefix = node_property_index_property_prefix(label, property);
        let Some((start, end)) = property_index_range_scan_bounds(&prefix, comparison, value)
        else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        let mut seen = BTreeSet::new();
        let entries = match end {
            Some(end) => self.indexes.range(start..end),
            None => self.indexes.range(start..),
        };
        for entry in entries {
            let (key, _) = entry?;
            let key_str =
                std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
            // Skip tombstoned index entries
            if self.has_index_tombstone(key_str) {
                continue;
            }
            let Some(node_id) = key_str.rsplit('/').next() else {
                continue;
            };
            if !seen.insert(node_id.to_string()) {
                continue;
            }
            let Some(node) = self.get_node_record(node_id)? else {
                continue;
            };
            if node_property_matches_range(&node, property, comparison, value) {
                out.push(node);
            }
        }
        out.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(out)
    }

    pub fn get_nodes_by_properties_range(
        &self,
        label: &str,
        properties: &[String],
        range_property: &str,
        comparison: RangeIndexComparison,
        value: &serde_json::Value,
        exact_values: &HashMap<String, serde_json::Value>,
    ) -> Result<Vec<NodeRecord>, StorageError> {
        let Some(range_property_index) = properties
            .iter()
            .position(|property| property == range_property)
        else {
            return Ok(Vec::new());
        };
        if !self.has_exact_node_property_index(label, properties)? {
            return Ok(Vec::new());
        }

        if properties
            .iter()
            .take(range_property_index)
            .any(|property| !exact_values.contains_key(property))
        {
            return Ok(Vec::new());
        }

        let prefix = node_property_index_range_lookup_prefix(
            label,
            properties,
            range_property_index,
            exact_values,
        )?;
        let Some((start, end)) = property_index_range_scan_bounds(&prefix, comparison, value)
        else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        let mut seen = BTreeSet::new();
        let entries = match end {
            Some(end) => self.indexes.range(start..end),
            None => self.indexes.range(start..),
        };
        for entry in entries {
            let (key, _) = entry?;
            let key_str =
                std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
            // Skip tombstoned index entries
            if self.has_index_tombstone(key_str) {
                continue;
            }
            let Some(node_id) = key_str.rsplit('/').next() else {
                continue;
            };
            if !seen.insert(node_id.to_string()) {
                continue;
            }
            let Some(node) = self.get_node_record(node_id)? else {
                continue;
            };
            if node_property_matches_range(&node, range_property, comparison, value)
                && node_matches_exact_index_suffix(&node, properties, exact_values)
            {
                out.push(node);
            }
        }
        out.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(out)
    }
}

fn node_matches_exact_index_suffix(
    node: &NodeRecord,
    properties: &[String],
    exact_values: &HashMap<String, serde_json::Value>,
) -> bool {
    properties.iter().all(|property| {
        exact_values.get(property).is_none_or(|expected| {
            node.properties
                .get(property)
                .is_some_and(|actual| actual == expected)
        })
    })
}

fn node_property_index_range_lookup_prefix(
    label: &str,
    properties: &[String],
    range_property_index: usize,
    exact_values: &HashMap<String, serde_json::Value>,
) -> Result<String, StorageError> {
    let mut prefix = node_property_index_definition_prefix(label, properties);
    if range_property_index == 0 {
        return Ok(prefix);
    }

    let value_keys = properties
        .iter()
        .take(range_property_index)
        .map(|property| {
            exact_values
                .get(property)
                .map(property_index_value_key)
                .ok_or_else(|| {
                    StorageError::NotFound(format!(
                        "missing exact composite range prefix value for property '{}'",
                        property
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    prefix.push_str(&value_keys.join("/"));
    prefix.push('/');
    Ok(prefix)
}

fn node_property_matches_range(
    node: &NodeRecord,
    property: &str,
    comparison: RangeIndexComparison,
    expected: &serde_json::Value,
) -> bool {
    node.properties
        .get(property)
        .and_then(|actual| compare_index_values(actual, expected))
        .is_some_and(|ordering| match comparison {
            RangeIndexComparison::GreaterThan => ordering == Ordering::Greater,
            RangeIndexComparison::GreaterThanOrEqual => ordering != Ordering::Less,
            RangeIndexComparison::LessThan => ordering == Ordering::Less,
            RangeIndexComparison::LessThanOrEqual => ordering != Ordering::Greater,
        })
}

fn compare_index_values(left: &serde_json::Value, right: &serde_json::Value) -> Option<Ordering> {
    match (left, right) {
        (serde_json::Value::Number(left), serde_json::Value::Number(right)) => {
            let left = left.as_f64()?;
            let right = right.as_f64()?;
            left.partial_cmp(&right)
        }
        (serde_json::Value::String(left), serde_json::Value::String(right)) => {
            Some(left.cmp(right))
        }
        _ => None,
    }
}
