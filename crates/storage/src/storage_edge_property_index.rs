use super::*;
use crate::storage_property_index_encoding::property_index_range_scan_bounds;
use std::cmp::Ordering;

impl StorageEngine {
    pub fn get_edges_by_property(
        &self,
        edge_type: &str,
        property: &str,
        value: &serde_json::Value,
    ) -> Result<Vec<EdgeRecord>, StorageError> {
        if !self.has_relationship_property_index(edge_type, &[property.to_string()])? {
            return Ok(Vec::new());
        }

        self.load_edges_from_index_prefix(&edge_property_index_value_prefix(
            edge_type, property, value,
        ))
    }

    pub fn get_edges_by_properties(
        &self,
        edge_type: &str,
        properties: &[String],
        values: &HashMap<String, serde_json::Value>,
    ) -> Result<Vec<EdgeRecord>, StorageError> {
        if !self.has_relationship_property_index(edge_type, properties)? {
            return Ok(Vec::new());
        }

        let Some(prefix) = edge_property_index_lookup_prefix(edge_type, properties, values) else {
            return Ok(Vec::new());
        };

        self.load_edges_from_index_prefix(&prefix)
    }

    pub fn get_edges_by_property_range(
        &self,
        edge_type: &str,
        property: &str,
        comparison: RangeIndexComparison,
        value: &serde_json::Value,
    ) -> Result<Vec<EdgeRecord>, StorageError> {
        if !self.has_relationship_property_index(edge_type, &[property.to_string()])? {
            return Ok(Vec::new());
        }

        let prefix = edge_property_index_property_prefix(edge_type, property);
        let Some((start, end)) = property_index_range_scan_bounds(&prefix, comparison, value)
        else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        let mut seen = BTreeSet::new();
        let entries = match end {
            Some(end) => self.indexes.fjall_range(start..end),
            None => self.indexes.fjall_range(start..),
        };
        for entry in entries {
            let (key, _) = entry?;
            let key_str =
                std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
            // Skip tombstoned index entries
            if self.has_index_tombstone(key_str) {
                continue;
            }
            let Some(edge_id) = key_str.rsplit('/').next() else {
                continue;
            };
            if !seen.insert(edge_id.to_string()) {
                continue;
            }
            let Some(edge) = self.get_edge_record(edge_id)? else {
                continue;
            };
            if edge_property_matches_range(&edge, property, comparison, value) {
                out.push(edge);
            }
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    pub fn get_edges_by_properties_range(
        &self,
        edge_type: &str,
        properties: &[String],
        range_property: &str,
        comparison: RangeIndexComparison,
        value: &serde_json::Value,
        exact_values: &HashMap<String, serde_json::Value>,
    ) -> Result<Vec<EdgeRecord>, StorageError> {
        let Some(range_property_index) = properties
            .iter()
            .position(|property| property == range_property)
        else {
            return Ok(Vec::new());
        };
        if !self.has_relationship_property_index(edge_type, properties)? {
            return Ok(Vec::new());
        }

        if properties
            .iter()
            .take(range_property_index)
            .any(|property| !exact_values.contains_key(property))
        {
            return Ok(Vec::new());
        }

        let prefix = edge_property_index_range_lookup_prefix(
            edge_type,
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
            Some(end) => self.indexes.fjall_range(start..end),
            None => self.indexes.fjall_range(start..),
        };
        for entry in entries {
            let (key, _) = entry?;
            let key_str =
                std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
            // Skip tombstoned index entries
            if self.has_index_tombstone(key_str) {
                continue;
            }
            let Some(edge_id) = key_str.rsplit('/').next() else {
                continue;
            };
            if !seen.insert(edge_id.to_string()) {
                continue;
            }
            let Some(edge) = self.get_edge_record(edge_id)? else {
                continue;
            };
            if edge_property_matches_range(&edge, range_property, comparison, value)
                && edge_matches_exact_index_suffix(&edge, properties, exact_values)
            {
                out.push(edge);
            }
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    pub(crate) fn rebuild_relationship_property_index(
        &self,
        index: &IndexDefinition,
    ) -> Result<(), StorageError> {
        self.rebuild_relationship_property_index_with_cancellation(
            index,
            &crate::RequestCancellation::new(),
        )
    }

    pub(crate) fn rebuild_relationship_property_index_with_cancellation(
        &self,
        index: &IndexDefinition,
        cancel: &crate::RequestCancellation,
    ) -> Result<(), StorageError> {
        self.delete_relationship_property_index_entries_with_cancellation(index, cancel)?;
        let mut batch = crate::Batch::new();
        let mut pending: usize = 0;
        self.stream_edge_records_with_cancellation(cancel, |edge| {
            if edge.edge_type != index.label {
                return Ok(());
            }
            if let Some(key) = relationship_property_index_key_for_edge(index, &edge) {
                batch.push((key.into_bytes(), Some(Vec::<u8>::new())));
                pending += 1;
                if pending >= 4096 {
                    self.indexes
                        .fjall_apply_batch(&std::mem::take(&mut batch))?;
                    pending = 0;
                }
            }
            Ok(())
        })?;
        if pending > 0 {
            cancel.check_cancelled()?;
            self.indexes.fjall_apply_batch(&batch)?;
        }
        Ok(())
    }

    pub(crate) fn delete_relationship_property_index_entries(
        &self,
        index: &IndexDefinition,
    ) -> Result<(), StorageError> {
        self.delete_relationship_property_index_entries_with_cancellation(
            index,
            &crate::RequestCancellation::new(),
        )
    }

    fn delete_relationship_property_index_entries_with_cancellation(
        &self,
        index: &IndexDefinition,
        cancel: &crate::RequestCancellation,
    ) -> Result<(), StorageError> {
        let prefix = edge_property_index_definition_prefix(&index.label, &index.properties);
        let mut batch = crate::Batch::new();
        let mut pending = 0usize;
        for entry in self.indexes.scan_prefix(prefix.as_bytes()) {
            cancel.check_cancelled()?;
            let (key, _) = entry?;
            batch.push((key.to_vec(), None));
            pending += 1;
            if pending >= 4096 {
                self.indexes
                    .fjall_apply_batch(&std::mem::take(&mut batch))?;
                pending = 0;
            }
        }
        if pending > 0 {
            cancel.check_cancelled()?;
            self.indexes.fjall_apply_batch(&batch)?;
        }
        Ok(())
    }

    fn has_relationship_property_index(
        &self,
        edge_type: &str,
        properties: &[String],
    ) -> Result<bool, StorageError> {
        Ok(self
            .relationship_property_index_definitions()?
            .iter()
            .any(|index| index.label == edge_type && index.properties == properties))
    }

    pub(crate) fn relationship_property_index_definitions(
        &self,
    ) -> Result<Vec<IndexDefinition>, StorageError> {
        Ok(self
            .load_index_definitions()?
            .into_iter()
            .filter(is_relationship_property_index)
            .collect())
    }

    fn load_edges_from_index_prefix(&self, prefix: &str) -> Result<Vec<EdgeRecord>, StorageError> {
        let mut out = Vec::new();
        for entry in self.indexes.scan_prefix(prefix.as_bytes()) {
            let (key, _) = entry?;
            let key_str =
                std::str::from_utf8(key.as_ref()).map_err(|_| StorageError::InvalidUtf8)?;
            // Skip tombstoned index entries
            if self.has_index_tombstone(key_str) {
                continue;
            }
            if let Some(edge_id) = key_str.rsplit('/').next() {
                if let Some(edge) = self.get_edge_record(edge_id)? {
                    out.push(edge);
                }
            }
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }
}

pub(crate) fn is_relationship_property_index(index: &IndexDefinition) -> bool {
    index.entity_type == IndexEntityType::Relationship
        && is_property_backed_index_kind(index.kind)
        && !index.properties.is_empty()
}

fn edge_property_index_property_prefix(edge_type: &str, property: &str) -> String {
    format!(
        "{IDX_EDGE_PROPERTY_PREFIX}/{}/{}/",
        escape_index_component(edge_type),
        escape_index_component(property)
    )
}

fn edge_property_index_value_prefix(
    edge_type: &str,
    property: &str,
    value: &serde_json::Value,
) -> String {
    format!(
        "{}{}/",
        edge_property_index_property_prefix(edge_type, property),
        property_index_value_key(value)
    )
}

fn edge_property_index_key(
    edge_type: &str,
    property: &str,
    value: &serde_json::Value,
    edge_id: &str,
) -> String {
    format!(
        "{}{}",
        edge_property_index_value_prefix(edge_type, property, value),
        edge_id
    )
}

pub(crate) fn edge_property_index_definition_prefix(
    edge_type: &str,
    properties: &[String],
) -> String {
    if properties.len() == 1 {
        return edge_property_index_property_prefix(edge_type, &properties[0]);
    }

    format!(
        "{IDX_EDGE_PROPERTY_PREFIX}/{}/composite/{}/values/",
        escape_index_component(edge_type),
        properties
            .iter()
            .map(|property| escape_index_component(property))
            .collect::<Vec<_>>()
            .join("/")
    )
}

fn edge_property_index_lookup_prefix(
    edge_type: &str,
    properties: &[String],
    values: &HashMap<String, serde_json::Value>,
) -> Option<String> {
    let value_refs = properties
        .iter()
        .map(|property| values.get(property))
        .collect::<Option<Vec<_>>>()?;

    if properties.len() == 1 {
        return Some(edge_property_index_value_prefix(
            edge_type,
            &properties[0],
            value_refs[0],
        ));
    }

    Some(format!(
        "{}{}/",
        edge_property_index_definition_prefix(edge_type, properties),
        value_refs
            .iter()
            .map(|value| property_index_value_key(value))
            .collect::<Vec<_>>()
            .join("/")
    ))
}

pub(crate) fn relationship_property_index_key_for_edge(
    index: &IndexDefinition,
    edge: &EdgeRecord,
) -> Option<String> {
    let value_refs = index
        .properties
        .iter()
        .map(|property| edge.properties.get(property))
        .collect::<Option<Vec<_>>>()?;

    if index.properties.len() == 1 {
        return Some(edge_property_index_key(
            &index.label,
            &index.properties[0],
            value_refs[0],
            &edge.id,
        ));
    }

    Some(format!(
        "{}{}/{}",
        edge_property_index_definition_prefix(&index.label, &index.properties),
        value_refs
            .iter()
            .map(|value| property_index_value_key(value))
            .collect::<Vec<_>>()
            .join("/"),
        edge.id
    ))
}

fn edge_matches_exact_index_suffix(
    edge: &EdgeRecord,
    properties: &[String],
    exact_values: &HashMap<String, serde_json::Value>,
) -> bool {
    properties.iter().all(|property| {
        exact_values.get(property).is_none_or(|expected| {
            edge.properties
                .get(property)
                .is_some_and(|actual| actual == expected)
        })
    })
}

fn edge_property_index_range_lookup_prefix(
    edge_type: &str,
    properties: &[String],
    range_property_index: usize,
    exact_values: &HashMap<String, serde_json::Value>,
) -> Result<String, StorageError> {
    let mut prefix = edge_property_index_definition_prefix(edge_type, properties);
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

fn edge_property_matches_range(
    edge: &EdgeRecord,
    property: &str,
    comparison: RangeIndexComparison,
    expected: &serde_json::Value,
) -> bool {
    edge.properties
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
