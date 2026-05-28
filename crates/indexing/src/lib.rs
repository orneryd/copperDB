//! Graph index catalog management for copperdb.
//!
//! The Layer 4 indexing crate owns the typed create/drop/list contract for
//! durable index definitions while delegating persistence to `copperdb-storage`.
//! This keeps Cypher evaluation, engine wiring, and future index workers on one
//! path for index catalog validation and deterministic listing behavior.

use copperdb_storage::{
    EdgeRecord, IndexDefinition, NodeRecord, RangeIndexComparison, StorageEngine, StorageError,
};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashMap;
use thiserror::Error;

pub use copperdb_storage::{
    IndexDefinition as CatalogIndexDefinition,
    IndexEntityType as CatalogIndexEntityType,
    IndexKind as CatalogIndexKind,
    RangeIndexComparison as CatalogRangeIndexComparison,
};

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("index already exists: {0}")]
    AlreadyExists(String),
    #[error("index not found: {0}")]
    NotFound(String),
    #[error("invalid index definition: {0}")]
    InvalidDefinition(String),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

/// Storage-backed index catalog.
pub struct IndexCatalog<'a> {
    storage: &'a StorageEngine,
}

impl<'a> IndexCatalog<'a> {
    pub fn new(storage: &'a StorageEngine) -> Self {
        Self { storage }
    }

    pub fn create(&self, definition: IndexDefinition) -> Result<(), IndexError> {
        validate_definition(&definition)?;
        if self.get(&definition.name)?.is_some() {
            return Err(IndexError::AlreadyExists(definition.name));
        }
        self.storage.persist_index_definition(&definition)?;
        Ok(())
    }

    pub fn create_if_absent(&self, definition: IndexDefinition) -> Result<bool, IndexError> {
        validate_definition(&definition)?;
        if self.get(&definition.name)?.is_some() {
            return Ok(false);
        }
        self.storage.persist_index_definition(&definition)?;
        Ok(true)
    }

    pub fn get(&self, name: &str) -> Result<Option<IndexDefinition>, IndexError> {
        Ok(self
            .storage
            .load_index_definitions()?
            .into_iter()
            .find(|definition| definition.name == name))
    }

    pub fn list(&self) -> Result<Vec<IndexDefinition>, IndexError> {
        Ok(self.storage.load_index_definitions()?)
    }

    pub fn lookup_nodes(
        &self,
        labels: &[String],
        properties: &HashMap<String, Value>,
    ) -> Result<Vec<NodeRecord>, IndexError> {
        let Some(primary_label) = labels
            .first()
            .map(String::as_str)
            .filter(|label| !label.is_empty())
        else {
            let mut nodes = self.storage.all_node_records()?;
            nodes.retain(|node| node_matches_properties(node, properties));
            return Ok(nodes);
        };

        if let Some(index) = self.preferred_node_index_definition(primary_label, properties)? {
            return if index.properties.len() == 1 {
                Ok(self.storage.get_nodes_by_property(
                    primary_label,
                    &index.properties[0],
                    &properties[&index.properties[0]],
                )?)
            } else {
                Ok(self
                    .storage
                    .get_nodes_by_properties(primary_label, &index.properties, properties)?)
            };
        }

        let mut nodes = self.storage.get_nodes_by_label(primary_label)?;
        nodes.retain(|node| node_matches_properties(node, properties));
        Ok(nodes)
    }

    pub fn lookup_nodes_by_range(
        &self,
        labels: &[String],
        property: &str,
        comparison: RangeIndexComparison,
        value: &Value,
        exact_properties: &HashMap<String, Value>,
    ) -> Result<Vec<NodeRecord>, IndexError> {
        let Some(primary_label) = labels
            .first()
            .map(String::as_str)
            .filter(|label| !label.is_empty())
        else {
            let mut nodes = self.storage.all_node_records()?;
            nodes.retain(|node| {
                node_matches_range_property(node, property, comparison, value)
                    && node_matches_properties(node, exact_properties)
            });
            return Ok(nodes);
        };

        if let Some(index) =
            self.preferred_node_range_index_definition(primary_label, property, exact_properties)?
        {
            return if index.properties.len() == 1 {
                Ok(self.storage.get_nodes_by_property_range(
                    primary_label,
                    property,
                    comparison,
                    value,
                )?)
            } else {
                Ok(self.storage.get_nodes_by_properties_range(
                    primary_label,
                    &index.properties,
                    property,
                    comparison,
                    value,
                    exact_properties,
                )?)
            };
        }

        let mut nodes = self.storage.get_nodes_by_label(primary_label)?;
        nodes.retain(|node| {
            node_matches_range_property(node, property, comparison, value)
                && node_matches_properties(node, exact_properties)
        });
        Ok(nodes)
    }

    pub fn lookup_edges(&self, edge_type: Option<&str>) -> Result<Vec<EdgeRecord>, IndexError> {
        match edge_type {
            Some(edge_type) if !edge_type.is_empty() => Ok(self.storage.get_edges_by_type(edge_type)?),
            _ => Ok(self.storage.all_edges()?),
        }
    }

    pub fn lookup_edges_by_properties(
        &self,
        edge_type: Option<&str>,
        properties: &HashMap<String, Value>,
    ) -> Result<Vec<EdgeRecord>, IndexError> {
        if properties.is_empty() {
            return self.lookup_edges(edge_type);
        }

        let mut edges = match edge_type.filter(|edge_type| !edge_type.is_empty()) {
            Some(edge_type) => {
                if let Some(index) = self.preferred_relationship_index_definition(edge_type, properties)? {
                    if index.properties.len() == 1 {
                        self.storage.get_edges_by_property(
                            edge_type,
                            &index.properties[0],
                            &properties[&index.properties[0]],
                        )?
                    } else {
                        self.storage.get_edges_by_properties(edge_type, &index.properties, properties)?
                    }
                } else {
                    self.storage.get_edges_by_type(edge_type)?
                }
            }
            None => self.storage.all_edges()?,
        };
        edges.retain(|edge| edge_matches_properties(edge, properties));
        Ok(edges)
    }

    pub fn lookup_edges_by_range(
        &self,
        edge_type: Option<&str>,
        property: &str,
        comparison: RangeIndexComparison,
        value: &Value,
        exact_properties: &HashMap<String, Value>,
    ) -> Result<Vec<EdgeRecord>, IndexError> {
        let mut edges = match edge_type.filter(|edge_type| !edge_type.is_empty()) {
            Some(edge_type) => {
                if let Some(index) = self.preferred_relationship_range_index_definition(
                    edge_type,
                    property,
                    exact_properties,
                )? {
                    if index.properties.len() == 1 {
                        self.storage
                            .get_edges_by_property_range(edge_type, property, comparison, value)?
                    } else {
                        self.storage.get_edges_by_properties_range(
                            edge_type,
                            &index.properties,
                            property,
                            comparison,
                            value,
                            exact_properties,
                        )?
                    }
                } else {
                    self.storage.get_edges_by_type(edge_type)?
                }
            }
            None => self.storage.all_edges()?,
        };
        edges.retain(|edge| {
            edge_matches_range_property(edge, property, comparison, value)
                && edge_matches_properties(edge, exact_properties)
        });
        Ok(edges)
    }

    pub fn drop(&self, name: &str) -> Result<(), IndexError> {
        if !self.storage.delete_index_definition(name)? {
            return Err(IndexError::NotFound(name.to_string()));
        }
        Ok(())
    }

    pub fn drop_if_present(&self, name: &str) -> Result<bool, IndexError> {
        Ok(self.storage.delete_index_definition(name)?)
    }

    fn preferred_node_index_definition(
        &self,
        label: &str,
        properties: &HashMap<String, Value>,
    ) -> Result<Option<IndexDefinition>, IndexError> {
        let mut preferred: Option<IndexDefinition> = None;

        for definition in self.list()?.into_iter().filter(|definition| {
            definition.entity_type == CatalogIndexEntityType::Node
                && supports_property_lookup_index_kind(definition.kind)
                && definition.label == label
                && definition
                    .properties
                    .iter()
                    .all(|property| properties.contains_key(property))
        }) {
            let replace = preferred
                .as_ref()
                .map(|current| definition.properties.len() > current.properties.len())
                .unwrap_or(true);
            if replace {
                preferred = Some(definition);
            }
        }

        Ok(preferred)
    }

    fn preferred_relationship_index_definition(
        &self,
        edge_type: &str,
        properties: &HashMap<String, Value>,
    ) -> Result<Option<IndexDefinition>, IndexError> {
        let mut preferred: Option<IndexDefinition> = None;

        for definition in self.list()?.into_iter().filter(|definition| {
            definition.entity_type == CatalogIndexEntityType::Relationship
                && supports_property_lookup_index_kind(definition.kind)
                && definition.label == edge_type
                && definition
                    .properties
                    .iter()
                    .all(|property| properties.contains_key(property))
        }) {
            let replace = preferred
                .as_ref()
                .map(|current| definition.properties.len() > current.properties.len())
                .unwrap_or(true);
            if replace {
                preferred = Some(definition);
            }
        }
        Ok(preferred)
    }

    fn preferred_relationship_range_index_definition(
        &self,
        edge_type: &str,
        property: &str,
        exact_properties: &HashMap<String, Value>,
    ) -> Result<Option<IndexDefinition>, IndexError> {
        let mut preferred: Option<IndexDefinition> = None;

        for definition in self.list()?.into_iter().filter(|definition| {
            definition.entity_type == CatalogIndexEntityType::Relationship
                && supports_ordered_comparison_index_kind(definition.kind)
                && definition.label == edge_type
                && definition.properties.iter().any(|candidate| candidate == property)
                && definition
                    .properties
                    .iter()
                    .take_while(|candidate| *candidate != property)
                    .all(|prefix_property| exact_properties.contains_key(prefix_property))
        }) {
            let replace = preferred
                .as_ref()
                .map(|current| {
                    range_index_preference_tuple(&definition, property, exact_properties)
                        > range_index_preference_tuple(current, property, exact_properties)
                })
                .unwrap_or(true);
            if replace {
                preferred = Some(definition);
            }
        }

        Ok(preferred)
    }

    fn preferred_node_range_index_definition(
        &self,
        label: &str,
        property: &str,
        exact_properties: &HashMap<String, Value>,
    ) -> Result<Option<IndexDefinition>, IndexError> {
        let mut preferred: Option<IndexDefinition> = None;

        for definition in self.list()?.into_iter().filter(|definition| {
            definition.entity_type == CatalogIndexEntityType::Node
                && supports_ordered_comparison_index_kind(definition.kind)
                && definition.label == label
                && definition.properties.iter().any(|candidate| candidate == property)
                && definition
                    .properties
                    .iter()
                    .take_while(|candidate| *candidate != property)
                    .all(|prefix_property| exact_properties.contains_key(prefix_property))
        }) {
            let replace = preferred
                .as_ref()
                .map(|current| {
                    range_index_preference_tuple(&definition, property, exact_properties)
                        > range_index_preference_tuple(current, property, exact_properties)
                })
                .unwrap_or(true);
            if replace {
                preferred = Some(definition);
            }
        }

        Ok(preferred)
    }
}

fn supports_ordered_comparison_index_kind(kind: CatalogIndexKind) -> bool {
    matches!(kind, CatalogIndexKind::Range | CatalogIndexKind::Temporal)
}

fn supports_property_lookup_index_kind(kind: CatalogIndexKind) -> bool {
    matches!(kind, CatalogIndexKind::Range | CatalogIndexKind::Temporal)
}

fn matching_exact_suffix_count(
    definition: &IndexDefinition,
    exact_properties: &HashMap<String, Value>,
) -> usize {
    definition
        .properties
        .iter()
        .skip(1)
        .filter(|property| exact_properties.contains_key(*property))
        .count()
}

fn range_index_preference_tuple(
    definition: &IndexDefinition,
    range_property: &str,
    exact_properties: &HashMap<String, Value>,
) -> (usize, usize, usize) {
    let range_position = definition
        .properties
        .iter()
        .position(|property| property == range_property)
        .unwrap_or(usize::MAX);
    let prefix_matches = definition
        .properties
        .iter()
        .take(range_position)
        .filter(|property| exact_properties.contains_key(*property))
        .count();
    let total_matches = matching_exact_suffix_count(definition, exact_properties);
    (prefix_matches, total_matches, definition.properties.len())
}

fn node_matches_properties(node: &NodeRecord, properties: &HashMap<String, Value>) -> bool {
    properties.iter().all(|(property, expected)| {
        node.properties
            .get(property)
            .is_some_and(|actual| actual == expected)
    })
}

fn edge_matches_properties(edge: &EdgeRecord, properties: &HashMap<String, Value>) -> bool {
    properties.iter().all(|(property, expected)| {
        edge.properties
            .get(property)
            .is_some_and(|actual| actual == expected)
    })
}

fn edge_matches_range_property(
    edge: &EdgeRecord,
    property: &str,
    comparison: RangeIndexComparison,
    expected: &Value,
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

fn node_matches_range_property(
    node: &NodeRecord,
    property: &str,
    comparison: RangeIndexComparison,
    expected: &Value,
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

fn compare_index_values(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => {
            let left = left.as_f64()?;
            let right = right.as_f64()?;
            left.partial_cmp(&right)
        }
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

fn validate_definition(definition: &IndexDefinition) -> Result<(), IndexError> {
    if definition.name.trim().is_empty() {
        return Err(IndexError::InvalidDefinition(
            "name must not be empty".to_string(),
        ));
    }
    if definition.label.trim().is_empty() {
        return Err(IndexError::InvalidDefinition(
            "label must not be empty".to_string(),
        ));
    }
    if definition.properties.is_empty() {
        return Err(IndexError::InvalidDefinition(
            "properties must not be empty".to_string(),
        ));
    }
    if definition
        .properties
        .iter()
        .any(|property| property.trim().is_empty())
    {
        return Err(IndexError::InvalidDefinition(
            "properties must not contain empty names".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    fn sample_definition(name: &str, label: &str, properties: &[&str]) -> IndexDefinition {
        IndexDefinition {
            name: name.to_string(),
            entity_type: CatalogIndexEntityType::Node,
            kind: CatalogIndexKind::Range,
            label: label.to_string(),
            properties: properties.iter().map(|property| property.to_string()).collect(),
        }
    }

    fn open_test_storage(test_name: &str) -> (tempfile::TempDir, StorageEngine) {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join(test_name);
        let storage = StorageEngine::open(&path).unwrap();
        (temp_dir, storage)
    }

    fn store_node(
        storage: &StorageEngine,
        id: &str,
        labels: &[&str],
        properties: &[(&str, Value)],
    ) {
        storage
            .put_node_record(&NodeRecord {
                id: id.to_string(),
                labels: labels.iter().map(|label| (*label).to_string()).collect(),
                properties: properties
                    .iter()
                    .map(|(key, value)| ((*key).to_string(), value.clone()))
                    .collect::<BTreeMap<_, _>>(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
    }

    fn store_edge(storage: &StorageEngine, id: &str, edge_type: &str, start: &str, end: &str) {
        store_edge_with_properties(storage, id, edge_type, start, end, &[]);
    }

    fn store_edge_with_properties(
        storage: &StorageEngine,
        id: &str,
        edge_type: &str,
        start: &str,
        end: &str,
        properties: &[(&str, Value)],
    ) {
        storage
            .put_edge_record(&EdgeRecord {
                id: id.to_string(),
                start_node: start.to_string(),
                end_node: end.to_string(),
                edge_type: edge_type.to_string(),
                properties: properties
                    .iter()
                    .map(|(key, value)| ((*key).to_string(), value.clone()))
                    .collect::<BTreeMap<_, _>>(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
    }

    #[test]
    fn catalog_create_list_get_and_drop_are_deterministic() {
        let (_temp_dir, storage) = open_test_storage("catalog-deterministic");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(sample_definition("person_name_idx", "Person", &["name"]))
            .unwrap();
        catalog
            .create(sample_definition("company_domain_idx", "Company", &["domain"]))
            .unwrap();

        let listed = catalog.list().unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].name, "company_domain_idx");
        assert_eq!(listed[0].label, "Company");
        assert_eq!(listed[0].properties, vec!["domain"]);
        assert_eq!(listed[1].name, "person_name_idx");
        assert_eq!(listed[1].label, "Person");
        assert_eq!(listed[1].properties, vec!["name"]);

        let found = catalog.get("person_name_idx").unwrap().unwrap();
        assert_eq!(found.entity_type, CatalogIndexEntityType::Node);
        assert_eq!(found.kind, CatalogIndexKind::Range);
        assert_eq!(found.label, "Person");
        assert_eq!(found.properties, vec!["name"]);

        catalog.drop("company_domain_idx").unwrap();
        let remaining = catalog.list().unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].name, "person_name_idx");
    }

    #[test]
    fn catalog_if_exists_variants_are_idempotent() {
        let (_temp_dir, storage) = open_test_storage("catalog-idempotent");
        let catalog = IndexCatalog::new(&storage);
        let definition = sample_definition("person_email_idx", "Person", &["email"]);

        assert!(catalog.create_if_absent(definition.clone()).unwrap());
        assert!(!catalog.create_if_absent(definition).unwrap());
        assert_eq!(catalog.list().unwrap().len(), 1);

        assert!(catalog.drop_if_present("person_email_idx").unwrap());
        assert!(!catalog.drop_if_present("person_email_idx").unwrap());
        assert!(catalog.list().unwrap().is_empty());
    }

    #[test]
    fn catalog_rejects_invalid_definitions_before_persisting() {
        let (_temp_dir, storage) = open_test_storage("catalog-invalid");
        let catalog = IndexCatalog::new(&storage);
        let error = catalog
            .create(sample_definition("", "Person", &["email"]))
            .unwrap_err();

        assert!(matches!(error, IndexError::InvalidDefinition(_)));
        assert!(catalog.list().unwrap().is_empty());
    }

    #[test]
    fn catalog_persists_definitions_across_reopen() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("index-catalog");

        {
            let storage = StorageEngine::open(&path).unwrap();
            let catalog = IndexCatalog::new(&storage);
            catalog
                .create(sample_definition("person_email_idx", "Person", &["email"]))
                .unwrap();
            catalog
                .create(sample_definition("person_name_idx", "Person", &["name"]))
                .unwrap();
        }

        let reopened = StorageEngine::open(&path).unwrap();
        let catalog = IndexCatalog::new(&reopened);
        let listed = catalog.list().unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0], sample_definition("person_email_idx", "Person", &["email"]));
        assert_eq!(listed[1], sample_definition("person_name_idx", "Person", &["name"]));
    }

    #[test]
    fn lookup_nodes_prefers_indexed_property_and_preserves_deterministic_order() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-indexed-property");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(sample_definition("person_name_idx", "Person", &["name"]))
            .unwrap();

        store_node(
            &storage,
            "person:2",
            &["Person"],
            &[("name", json!("Bob")), ("age", json!(30))],
        );
        store_node(
            &storage,
            "person:1",
            &["Person", "Employee"],
            &[("name", json!("Alice")), ("age", json!(40))],
        );
        store_node(
            &storage,
            "device:1",
            &["Device"],
            &[("name", json!("Alice"))],
        );

        let properties = HashMap::from([(String::from("name"), json!("Alice"))]);
        let nodes = catalog
            .lookup_nodes(&[String::from("Person")], &properties)
            .unwrap();

        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, "person:1");
        assert_eq!(nodes[0].labels, vec!["Person", "Employee"]);
        assert_eq!(nodes[0].properties.get("age"), Some(&json!(40)));
    }

    #[test]
    fn lookup_nodes_uses_most_specific_index_definition_available() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-composite-property");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(sample_definition("person_email_idx", "Person", &["email"]))
            .unwrap();
        catalog
            .create(sample_definition(
                "person_email_country_idx",
                "Person",
                &["email", "country"],
            ))
            .unwrap();

        store_node(
            &storage,
            "person:2",
            &["Person"],
            &[("email", json!("alice@example.com")), ("country", json!("CA"))],
        );
        store_node(
            &storage,
            "person:1",
            &["Person"],
            &[("email", json!("alice@example.com")), ("country", json!("US"))],
        );

        let properties = HashMap::from([
            (String::from("email"), json!("alice@example.com")),
            (String::from("country"), json!("US")),
        ]);
        let nodes = catalog
            .lookup_nodes(&[String::from("Person")], &properties)
            .unwrap();

        assert_eq!(nodes.iter().map(|node| node.id.as_str()).collect::<Vec<_>>(), vec!["person:1"]);
    }

    #[test]
    fn lookup_nodes_falls_back_to_label_and_full_scan_without_matching_index() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-fallback");
        let catalog = IndexCatalog::new(&storage);

        store_node(
            &storage,
            "person:2",
            &["Person"],
            &[("name", json!("Bob")), ("team", json!("ops"))],
        );
        store_node(
            &storage,
            "person:1",
            &["Person"],
            &[("name", json!("Alice")), ("team", json!("ops"))],
        );
        store_node(
            &storage,
            "person:3",
            &["Person"],
            &[("name", json!("Eve")), ("team", json!("sales"))],
        );
        store_node(&storage, "robot:1", &["Robot"], &[("name", json!("R2"))]);

        let label_properties = HashMap::from([(String::from("team"), json!("ops"))]);
        let label_nodes = catalog
            .lookup_nodes(&[String::from("Person")], &label_properties)
            .unwrap();
        assert_eq!(
            label_nodes
                .iter()
                .map(|node| node.id.as_str())
                .collect::<Vec<_>>(),
            vec!["person:1", "person:2"]
        );

        let all_nodes = catalog.lookup_nodes(&[], &HashMap::new()).unwrap();
        assert_eq!(
            all_nodes
                .iter()
                .map(|node| node.id.as_str())
                .collect::<Vec<_>>(),
            vec!["person:1", "person:2", "person:3", "robot:1"]
        );

        let filtered_all_nodes = catalog
            .lookup_nodes(&[], &HashMap::from([(String::from("team"), json!("ops"))]))
            .unwrap();
        assert_eq!(
            filtered_all_nodes
                .iter()
                .map(|node| node.id.as_str())
                .collect::<Vec<_>>(),
            vec!["person:1", "person:2"]
        );
    }

    #[test]
    fn lookup_nodes_ignores_metadata_only_index_kinds_for_exact_property_lookup() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-ignore-metadata-only-node-index");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(IndexDefinition {
                name: "person_name_fulltext_idx".to_string(),
                entity_type: CatalogIndexEntityType::Node,
                kind: CatalogIndexKind::FullText,
                label: "Person".to_string(),
                properties: vec!["name".to_string()],
            })
            .unwrap();

        store_node(
            &storage,
            "person:1",
            &["Person"],
            &[("name", json!("Alice")), ("team", json!("ops"))],
        );
        store_node(
            &storage,
            "person:2",
            &["Person"],
            &[("name", json!("Bob")), ("team", json!("ops"))],
        );

        let nodes = catalog
            .lookup_nodes(
                &[String::from("Person")],
                &HashMap::from([(String::from("name"), json!("Alice"))]),
            )
            .unwrap();

        assert_eq!(
            nodes.iter().map(|node| node.id.as_str()).collect::<Vec<_>>(),
            vec!["person:1"]
        );
    }

    #[test]
    fn lookup_nodes_by_range_uses_range_index_and_preserves_deterministic_order() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-range-property");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(sample_definition("person_age_idx", "Person", &["age"]))
            .unwrap();

        store_node(&storage, "person:2", &["Person"], &[("age", json!(35))]);
        store_node(&storage, "person:1", &["Person"], &[("age", json!(29))]);
        store_node(&storage, "person:3", &["Person"], &[("age", json!(41))]);
        store_node(&storage, "device:1", &["Device"], &[("age", json!(99))]);

        let nodes = catalog
            .lookup_nodes_by_range(
                &[String::from("Person")],
                "age",
                CatalogRangeIndexComparison::GreaterThan,
                &json!(30),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(
            nodes.iter().map(|node| node.id.as_str()).collect::<Vec<_>>(),
            vec!["person:2", "person:3"]
        );
    }

    #[test]
    fn lookup_edges_filters_by_type_and_preserves_deterministic_order() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-edges-by-type");
        let catalog = IndexCatalog::new(&storage);

        store_edge(&storage, "rel:2", "KNOWS", "person:2", "person:3");
        store_edge(&storage, "rel:1", "KNOWS", "person:1", "person:2");
        store_edge(&storage, "rel:3", "LIKES", "person:1", "movie:1");

        let edges = catalog.lookup_edges(Some("KNOWS")).unwrap();
        assert_eq!(edges.iter().map(|edge| edge.id.as_str()).collect::<Vec<_>>(), vec!["rel:1", "rel:2"]);
        assert!(edges.iter().all(|edge| edge.edge_type == "KNOWS"));
    }

    #[test]
    fn lookup_edges_by_properties_uses_relationship_index_and_filters_residual_properties() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-edges-by-property");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(IndexDefinition {
                name: "knows_weight_idx".to_string(),
                entity_type: CatalogIndexEntityType::Relationship,
                kind: CatalogIndexKind::Range,
                label: "KNOWS".to_string(),
                properties: vec!["weight".to_string()],
            })
            .unwrap();

        store_edge_with_properties(
            &storage,
            "rel:2",
            "KNOWS",
            "person:2",
            "person:3",
            &[("weight", json!(1.5)), ("years", json!(3))],
        );
        store_edge_with_properties(
            &storage,
            "rel:1",
            "KNOWS",
            "person:1",
            "person:2",
            &[("weight", json!(1.5)), ("years", json!(5))],
        );
        store_edge_with_properties(
            &storage,
            "rel:3",
            "LIKES",
            "person:1",
            "movie:1",
            &[("weight", json!(1.5)), ("years", json!(5))],
        );

        let edges = catalog
            .lookup_edges_by_properties(
                Some("KNOWS"),
                &HashMap::from([
                    (String::from("weight"), json!(1.5)),
                    (String::from("years"), json!(5)),
                ]),
            )
            .unwrap();
        assert_eq!(edges.iter().map(|edge| edge.id.as_str()).collect::<Vec<_>>(), vec!["rel:1"]);
    }

    #[test]
    fn lookup_edges_by_properties_prefers_most_specific_relationship_index_definition() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-edges-by-composite-property");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(IndexDefinition {
                name: "knows_weight_idx".to_string(),
                entity_type: CatalogIndexEntityType::Relationship,
                kind: CatalogIndexKind::Range,
                label: "KNOWS".to_string(),
                properties: vec!["weight".to_string()],
            })
            .unwrap();
        catalog
            .create(IndexDefinition {
                name: "knows_weight_years_idx".to_string(),
                entity_type: CatalogIndexEntityType::Relationship,
                kind: CatalogIndexKind::Range,
                label: "KNOWS".to_string(),
                properties: vec!["weight".to_string(), "years".to_string()],
            })
            .unwrap();

        store_edge_with_properties(
            &storage,
            "rel:2",
            "KNOWS",
            "person:2",
            "person:3",
            &[("weight", json!(1.5)), ("years", json!(3))],
        );
        store_edge_with_properties(
            &storage,
            "rel:1",
            "KNOWS",
            "person:1",
            "person:2",
            &[("weight", json!(1.5)), ("years", json!(5))],
        );

        let edges = catalog
            .lookup_edges_by_properties(
                Some("KNOWS"),
                &HashMap::from([
                    (String::from("weight"), json!(1.5)),
                    (String::from("years"), json!(5)),
                ]),
            )
            .unwrap();
        assert_eq!(edges.iter().map(|edge| edge.id.as_str()).collect::<Vec<_>>(), vec!["rel:1"]);
    }

    #[test]
    fn lookup_edges_falls_back_to_all_edges_when_type_is_absent() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-all-edges");
        let catalog = IndexCatalog::new(&storage);

        store_edge(&storage, "rel:3", "LIKES", "person:1", "movie:1");
        store_edge(&storage, "rel:1", "KNOWS", "person:1", "person:2");
        store_edge(&storage, "rel:2", "KNOWS", "person:2", "person:3");

        let edges = catalog.lookup_edges(None).unwrap();
        assert_eq!(edges.iter().map(|edge| edge.id.as_str()).collect::<Vec<_>>(), vec!["rel:1", "rel:2", "rel:3"]);
    }

    #[test]
    fn lookup_edges_by_properties_ignores_metadata_only_index_kinds_for_exact_property_lookup() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-ignore-metadata-only-edge-index");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(IndexDefinition {
                name: "knows_note_vector_idx".to_string(),
                entity_type: CatalogIndexEntityType::Relationship,
                kind: CatalogIndexKind::Vector,
                label: "KNOWS".to_string(),
                properties: vec!["note".to_string()],
            })
            .unwrap();

        store_edge_with_properties(
            &storage,
            "rel:1",
            "KNOWS",
            "person:1",
            "person:2",
            &[("note", json!("met at summit")), ("years", json!(5))],
        );
        store_edge_with_properties(
            &storage,
            "rel:2",
            "KNOWS",
            "person:2",
            "person:3",
            &[("note", json!("teammates")), ("years", json!(3))],
        );

        let edges = catalog
            .lookup_edges_by_properties(
                Some("KNOWS"),
                &HashMap::from([(String::from("note"), json!("met at summit"))]),
            )
            .unwrap();

        assert_eq!(
            edges.iter().map(|edge| edge.id.as_str()).collect::<Vec<_>>(),
            vec!["rel:1"]
        );
    }

    #[test]
    fn lookup_edges_by_range_uses_relationship_range_index_and_filters_values() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-edges-by-range");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(IndexDefinition {
                name: "knows_weight_idx".to_string(),
                entity_type: CatalogIndexEntityType::Relationship,
                kind: CatalogIndexKind::Range,
                label: "KNOWS".to_string(),
                properties: vec!["weight".to_string()],
            })
            .unwrap();

        store_edge_with_properties(
            &storage,
            "rel:2",
            "KNOWS",
            "person:2",
            "person:3",
            &[("weight", json!(1.5))],
        );
        store_edge_with_properties(
            &storage,
            "rel:1",
            "KNOWS",
            "person:1",
            "person:2",
            &[("weight", json!(0.5))],
        );
        store_edge_with_properties(
            &storage,
            "rel:3",
            "LIKES",
            "person:1",
            "movie:1",
            &[("weight", json!(9.0))],
        );

        let edges = catalog
            .lookup_edges_by_range(
                Some("KNOWS"),
                "weight",
                CatalogRangeIndexComparison::GreaterThan,
                &json!(1.0),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(edges.iter().map(|edge| edge.id.as_str()).collect::<Vec<_>>(), vec!["rel:2"]);
    }

    #[test]
    fn lookup_nodes_by_range_uses_composite_range_index_when_exact_suffix_is_available() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-nodes-by-composite-range");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(sample_definition("person_age_idx", "Person", &["age"]))
            .unwrap();
        catalog
            .create(sample_definition("person_age_team_idx", "Person", &["age", "team"]))
            .unwrap();

        store_node(
            &storage,
            "person:2",
            &["Person"],
            &[("age", json!(35)), ("team", json!("ops"))],
        );
        store_node(
            &storage,
            "person:1",
            &["Person"],
            &[("age", json!(29)), ("team", json!("ops"))],
        );
        store_node(
            &storage,
            "person:3",
            &["Person"],
            &[("age", json!(41)), ("team", json!("sales"))],
        );
        store_node(
            &storage,
            "person:4",
            &["Person"],
            &[("age", json!(43)), ("team", json!("ops"))],
        );

        let nodes = catalog
            .lookup_nodes_by_range(
                &[String::from("Person")],
                "age",
                CatalogRangeIndexComparison::GreaterThan,
                &json!(30),
                &HashMap::from([(String::from("team"), json!("ops"))]),
            )
            .unwrap();

        assert_eq!(
            nodes.iter().map(|node| node.id.as_str()).collect::<Vec<_>>(),
            vec!["person:2", "person:4"]
        );
    }

    #[test]
    fn lookup_nodes_by_range_uses_composite_range_index_without_exact_suffix_filters() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-nodes-by-composite-range-no-suffix");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(sample_definition("person_age_team_idx", "Person", &["age", "team"]))
            .unwrap();

        store_node(&storage, "person:2", &["Person"], &[("age", json!(35)), ("team", json!("ops"))]);
        store_node(&storage, "person:1", &["Person"], &[("age", json!(29)), ("team", json!("ops"))]);
        store_node(&storage, "person:3", &["Person"], &[("age", json!(41)), ("team", json!("sales"))]);

        let nodes = catalog
            .lookup_nodes_by_range(
                &[String::from("Person")],
                "age",
                CatalogRangeIndexComparison::GreaterThan,
                &json!(30),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(
            nodes.iter().map(|node| node.id.as_str()).collect::<Vec<_>>(),
            vec!["person:2", "person:3"]
        );
    }

    #[test]
    fn lookup_nodes_by_range_uses_non_leading_composite_range_index_when_exact_prefix_is_available() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-nodes-by-non-leading-composite-range");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(sample_definition("person_team_age_idx", "Person", &["team", "age"]))
            .unwrap();

        store_node(&storage, "person:2", &["Person"], &[("team", json!("ops")), ("age", json!(35))]);
        store_node(&storage, "person:1", &["Person"], &[("team", json!("ops")), ("age", json!(29))]);
        store_node(&storage, "person:3", &["Person"], &[("team", json!("sales")), ("age", json!(41))]);
        store_node(&storage, "person:4", &["Person"], &[("team", json!("ops")), ("age", json!(43))]);

        let nodes = catalog
            .lookup_nodes_by_range(
                &[String::from("Person")],
                "age",
                CatalogRangeIndexComparison::GreaterThan,
                &json!(30),
                &HashMap::from([(String::from("team"), json!("ops"))]),
            )
            .unwrap();

        assert_eq!(
            nodes.iter().map(|node| node.id.as_str()).collect::<Vec<_>>(),
            vec!["person:2", "person:4"]
        );
    }

    #[test]
    fn lookup_nodes_by_range_uses_temporal_index_and_preserves_deterministic_order() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-nodes-by-temporal-range");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(IndexDefinition {
                name: "person_seen_at_idx".to_string(),
                entity_type: CatalogIndexEntityType::Node,
                kind: CatalogIndexKind::Temporal,
                label: "Person".to_string(),
                properties: vec!["seenAt".to_string()],
            })
            .unwrap();

        store_node(&storage, "person:2", &["Person"], &[("seenAt", json!("2024-06-01T00:00:00Z"))]);
        store_node(&storage, "person:1", &["Person"], &[("seenAt", json!("2024-01-01T00:00:00Z"))]);
        store_node(&storage, "person:3", &["Person"], &[("seenAt", json!("2025-01-01T00:00:00Z"))]);

        let nodes = catalog
            .lookup_nodes_by_range(
                &[String::from("Person")],
                "seenAt",
                CatalogRangeIndexComparison::GreaterThan,
                &json!("2024-03-01T00:00:00Z"),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(
            nodes.iter().map(|node| node.id.as_str()).collect::<Vec<_>>(),
            vec!["person:2", "person:3"]
        );
    }

    #[test]
    fn lookup_nodes_by_range_uses_composite_temporal_index_with_exact_suffix() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-nodes-by-composite-temporal-range");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(IndexDefinition {
                name: "person_seen_at_team_idx".to_string(),
                entity_type: CatalogIndexEntityType::Node,
                kind: CatalogIndexKind::Temporal,
                label: "Person".to_string(),
                properties: vec!["seenAt".to_string(), "team".to_string()],
            })
            .unwrap();

        store_node(
            &storage,
            "person:2",
            &["Person"],
            &[("seenAt", json!("2024-06-01T00:00:00Z")), ("team", json!("ops"))],
        );
        store_node(
            &storage,
            "person:1",
            &["Person"],
            &[("seenAt", json!("2024-01-01T00:00:00Z")), ("team", json!("ops"))],
        );
        store_node(
            &storage,
            "person:3",
            &["Person"],
            &[("seenAt", json!("2025-01-01T00:00:00Z")), ("team", json!("sales"))],
        );
        store_node(
            &storage,
            "person:4",
            &["Person"],
            &[("seenAt", json!("2025-03-01T00:00:00Z")), ("team", json!("ops"))],
        );

        let nodes = catalog
            .lookup_nodes_by_range(
                &[String::from("Person")],
                "seenAt",
                CatalogRangeIndexComparison::GreaterThan,
                &json!("2024-03-01T00:00:00Z"),
                &HashMap::from([(String::from("team"), json!("ops"))]),
            )
            .unwrap();

        assert_eq!(
            nodes.iter().map(|node| node.id.as_str()).collect::<Vec<_>>(),
            vec!["person:2", "person:4"]
        );
    }

    #[test]
    fn lookup_nodes_by_range_uses_non_leading_composite_temporal_index_when_exact_prefix_is_available() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-nodes-by-non-leading-composite-temporal-range");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(IndexDefinition {
                name: "person_team_seen_at_idx".to_string(),
                entity_type: CatalogIndexEntityType::Node,
                kind: CatalogIndexKind::Temporal,
                label: "Person".to_string(),
                properties: vec!["team".to_string(), "seenAt".to_string()],
            })
            .unwrap();

        store_node(
            &storage,
            "person:2",
            &["Person"],
            &[("team", json!("ops")), ("seenAt", json!("2024-06-01T00:00:00Z"))],
        );
        store_node(
            &storage,
            "person:1",
            &["Person"],
            &[("team", json!("ops")), ("seenAt", json!("2024-01-01T00:00:00Z"))],
        );
        store_node(
            &storage,
            "person:3",
            &["Person"],
            &[("team", json!("sales")), ("seenAt", json!("2025-01-01T00:00:00Z"))],
        );
        store_node(
            &storage,
            "person:4",
            &["Person"],
            &[("team", json!("ops")), ("seenAt", json!("2025-03-01T00:00:00Z"))],
        );

        let nodes = catalog
            .lookup_nodes_by_range(
                &[String::from("Person")],
                "seenAt",
                CatalogRangeIndexComparison::GreaterThan,
                &json!("2024-03-01T00:00:00Z"),
                &HashMap::from([(String::from("team"), json!("ops"))]),
            )
            .unwrap();

        assert_eq!(
            nodes.iter().map(|node| node.id.as_str()).collect::<Vec<_>>(),
            vec!["person:2", "person:4"]
        );
    }

    #[test]
    fn lookup_edges_by_range_uses_composite_relationship_range_index_when_exact_suffix_is_available() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-edges-by-composite-range");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(IndexDefinition {
                name: "knows_weight_years_idx".to_string(),
                entity_type: CatalogIndexEntityType::Relationship,
                kind: CatalogIndexKind::Range,
                label: "KNOWS".to_string(),
                properties: vec!["weight".to_string(), "years".to_string()],
            })
            .unwrap();

        store_edge_with_properties(
            &storage,
            "rel:2",
            "KNOWS",
            "person:2",
            "person:3",
            &[("weight", json!(1.5)), ("years", json!(3))],
        );
        store_edge_with_properties(
            &storage,
            "rel:1",
            "KNOWS",
            "person:1",
            "person:2",
            &[("weight", json!(2.5)), ("years", json!(5))],
        );
        store_edge_with_properties(
            &storage,
            "rel:3",
            "KNOWS",
            "person:3",
            "person:4",
            &[("weight", json!(3.0)), ("years", json!(5))],
        );

        let edges = catalog
            .lookup_edges_by_range(
                Some("KNOWS"),
                "weight",
                CatalogRangeIndexComparison::GreaterThan,
                &json!(2.0),
                &HashMap::from([(String::from("years"), json!(5))]),
            )
            .unwrap();

        assert_eq!(
            edges.iter().map(|edge| edge.id.as_str()).collect::<Vec<_>>(),
            vec!["rel:1", "rel:3"]
        );
    }

    #[test]
    fn lookup_edges_by_range_uses_composite_relationship_range_index_without_exact_suffix_filters() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-edges-by-composite-range-no-suffix");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(IndexDefinition {
                name: "knows_weight_years_idx".to_string(),
                entity_type: CatalogIndexEntityType::Relationship,
                kind: CatalogIndexKind::Range,
                label: "KNOWS".to_string(),
                properties: vec!["weight".to_string(), "years".to_string()],
            })
            .unwrap();

        store_edge_with_properties(
            &storage,
            "rel:2",
            "KNOWS",
            "person:2",
            "person:3",
            &[("weight", json!(1.5)), ("years", json!(3))],
        );
        store_edge_with_properties(
            &storage,
            "rel:1",
            "KNOWS",
            "person:1",
            "person:2",
            &[("weight", json!(2.5)), ("years", json!(5))],
        );
        store_edge_with_properties(
            &storage,
            "rel:3",
            "KNOWS",
            "person:3",
            "person:4",
            &[("weight", json!(3.0)), ("years", json!(5))],
        );

        let edges = catalog
            .lookup_edges_by_range(
                Some("KNOWS"),
                "weight",
                CatalogRangeIndexComparison::GreaterThan,
                &json!(2.0),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(
            edges.iter().map(|edge| edge.id.as_str()).collect::<Vec<_>>(),
            vec!["rel:1", "rel:3"]
        );
    }

    #[test]
    fn lookup_edges_by_range_uses_non_leading_composite_relationship_range_index_when_exact_prefix_is_available() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-edges-by-non-leading-composite-range");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(IndexDefinition {
                name: "knows_years_weight_idx".to_string(),
                entity_type: CatalogIndexEntityType::Relationship,
                kind: CatalogIndexKind::Range,
                label: "KNOWS".to_string(),
                properties: vec!["years".to_string(), "weight".to_string()],
            })
            .unwrap();

        store_edge_with_properties(
            &storage,
            "rel:2",
            "KNOWS",
            "person:2",
            "person:3",
            &[("years", json!(5)), ("weight", json!(2.5))],
        );
        store_edge_with_properties(
            &storage,
            "rel:1",
            "KNOWS",
            "person:1",
            "person:2",
            &[("years", json!(5)), ("weight", json!(1.5))],
        );
        store_edge_with_properties(
            &storage,
            "rel:3",
            "KNOWS",
            "person:3",
            "person:4",
            &[("years", json!(2)), ("weight", json!(7.0))],
        );
        store_edge_with_properties(
            &storage,
            "rel:4",
            "KNOWS",
            "person:4",
            "person:5",
            &[("years", json!(5)), ("weight", json!(3.0))],
        );

        let edges = catalog
            .lookup_edges_by_range(
                Some("KNOWS"),
                "weight",
                CatalogRangeIndexComparison::GreaterThan,
                &json!(2.0),
                &HashMap::from([(String::from("years"), json!(5))]),
            )
            .unwrap();

        assert_eq!(
            edges.iter().map(|edge| edge.id.as_str()).collect::<Vec<_>>(),
            vec!["rel:2", "rel:4"]
        );
    }

    #[test]
    fn lookup_edges_by_range_uses_temporal_relationship_index_and_filters_values() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-edges-by-temporal-range");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(IndexDefinition {
                name: "knows_seen_at_idx".to_string(),
                entity_type: CatalogIndexEntityType::Relationship,
                kind: CatalogIndexKind::Temporal,
                label: "KNOWS".to_string(),
                properties: vec!["seenAt".to_string()],
            })
            .unwrap();

        store_edge_with_properties(
            &storage,
            "rel:2",
            "KNOWS",
            "person:2",
            "person:3",
            &[("seenAt", json!("2024-06-01T00:00:00Z"))],
        );
        store_edge_with_properties(
            &storage,
            "rel:1",
            "KNOWS",
            "person:1",
            "person:2",
            &[("seenAt", json!("2024-01-01T00:00:00Z"))],
        );
        store_edge_with_properties(
            &storage,
            "rel:3",
            "KNOWS",
            "person:3",
            "person:4",
            &[("seenAt", json!("2025-01-01T00:00:00Z"))],
        );

        let edges = catalog
            .lookup_edges_by_range(
                Some("KNOWS"),
                "seenAt",
                CatalogRangeIndexComparison::GreaterThan,
                &json!("2024-03-01T00:00:00Z"),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(
            edges.iter().map(|edge| edge.id.as_str()).collect::<Vec<_>>(),
            vec!["rel:2", "rel:3"]
        );
    }

    #[test]
    fn lookup_edges_by_range_uses_composite_temporal_relationship_index_with_exact_suffix() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-edges-by-composite-temporal-range");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(IndexDefinition {
                name: "knows_seen_at_years_idx".to_string(),
                entity_type: CatalogIndexEntityType::Relationship,
                kind: CatalogIndexKind::Temporal,
                label: "KNOWS".to_string(),
                properties: vec!["seenAt".to_string(), "years".to_string()],
            })
            .unwrap();

        store_edge_with_properties(
            &storage,
            "rel:2",
            "KNOWS",
            "person:2",
            "person:3",
            &[("seenAt", json!("2024-06-01T00:00:00Z")), ("years", json!(5))],
        );
        store_edge_with_properties(
            &storage,
            "rel:1",
            "KNOWS",
            "person:1",
            "person:2",
            &[("seenAt", json!("2024-01-01T00:00:00Z")), ("years", json!(5))],
        );
        store_edge_with_properties(
            &storage,
            "rel:3",
            "KNOWS",
            "person:3",
            "person:4",
            &[("seenAt", json!("2025-01-01T00:00:00Z")), ("years", json!(2))],
        );
        store_edge_with_properties(
            &storage,
            "rel:4",
            "KNOWS",
            "person:4",
            "person:5",
            &[("seenAt", json!("2025-03-01T00:00:00Z")), ("years", json!(5))],
        );

        let edges = catalog
            .lookup_edges_by_range(
                Some("KNOWS"),
                "seenAt",
                CatalogRangeIndexComparison::GreaterThan,
                &json!("2024-03-01T00:00:00Z"),
                &HashMap::from([(String::from("years"), json!(5))]),
            )
            .unwrap();

        assert_eq!(
            edges.iter().map(|edge| edge.id.as_str()).collect::<Vec<_>>(),
            vec!["rel:2", "rel:4"]
        );
    }

    #[test]
    fn lookup_edges_by_range_uses_non_leading_composite_temporal_relationship_index_when_exact_prefix_is_available() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-edges-by-non-leading-composite-temporal-range");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(IndexDefinition {
                name: "knows_years_seen_at_idx".to_string(),
                entity_type: CatalogIndexEntityType::Relationship,
                kind: CatalogIndexKind::Temporal,
                label: "KNOWS".to_string(),
                properties: vec!["years".to_string(), "seenAt".to_string()],
            })
            .unwrap();

        store_edge_with_properties(
            &storage,
            "rel:2",
            "KNOWS",
            "person:2",
            "person:3",
            &[("years", json!(5)), ("seenAt", json!("2024-06-01T00:00:00Z"))],
        );
        store_edge_with_properties(
            &storage,
            "rel:1",
            "KNOWS",
            "person:1",
            "person:2",
            &[("years", json!(5)), ("seenAt", json!("2024-01-01T00:00:00Z"))],
        );
        store_edge_with_properties(
            &storage,
            "rel:3",
            "KNOWS",
            "person:3",
            "person:4",
            &[("years", json!(2)), ("seenAt", json!("2025-01-01T00:00:00Z"))],
        );
        store_edge_with_properties(
            &storage,
            "rel:4",
            "KNOWS",
            "person:4",
            "person:5",
            &[("years", json!(5)), ("seenAt", json!("2025-03-01T00:00:00Z"))],
        );

        let edges = catalog
            .lookup_edges_by_range(
                Some("KNOWS"),
                "seenAt",
                CatalogRangeIndexComparison::GreaterThan,
                &json!("2024-03-01T00:00:00Z"),
                &HashMap::from([(String::from("years"), json!(5))]),
            )
            .unwrap();

        assert_eq!(
            edges.iter().map(|edge| edge.id.as_str()).collect::<Vec<_>>(),
            vec!["rel:2", "rel:4"]
        );
    }

    #[test]
    fn preferred_node_range_index_definition_prefers_more_exact_prefix_for_non_leading_range() {
        let (_temp_dir, storage) = open_test_storage("catalog-preferred-node-non-leading-range-prefix");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(sample_definition("person_team_age_level_idx", "Person", &["team", "age", "level"]))
            .unwrap();
        catalog
            .create(sample_definition(
                "person_division_team_age_idx",
                "Person",
                &["division", "team", "age"],
            ))
            .unwrap();

        let preferred = catalog
            .preferred_node_range_index_definition(
                "Person",
                "age",
                &HashMap::from([
                    (String::from("division"), json!("platform")),
                    (String::from("team"), json!("ops")),
                    (String::from("level"), json!("senior")),
                ]),
            )
            .unwrap()
            .expect("expected preferred range definition");

        assert_eq!(preferred.name, "person_division_team_age_idx");
    }

    #[test]
    fn preferred_node_range_index_definition_prefers_more_exact_fields_when_prefix_is_equal() {
        let (_temp_dir, storage) = open_test_storage("catalog-preferred-node-non-leading-range-specificity");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(sample_definition("person_team_age_idx", "Person", &["team", "age"]))
            .unwrap();
        catalog
            .create(sample_definition("person_team_age_level_idx", "Person", &["team", "age", "level"]))
            .unwrap();

        let preferred = catalog
            .preferred_node_range_index_definition(
                "Person",
                "age",
                &HashMap::from([
                    (String::from("team"), json!("ops")),
                    (String::from("level"), json!("senior")),
                ]),
            )
            .unwrap()
            .expect("expected preferred range definition");

        assert_eq!(preferred.name, "person_team_age_level_idx");
    }

    #[test]
    fn preferred_relationship_range_index_definition_prefers_more_exact_prefix_for_non_leading_range() {
        let (_temp_dir, storage) = open_test_storage("catalog-preferred-edge-non-leading-range-prefix");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(IndexDefinition {
                name: "knows_years_weight_level_idx".to_string(),
                entity_type: CatalogIndexEntityType::Relationship,
                kind: CatalogIndexKind::Range,
                label: "KNOWS".to_string(),
                properties: vec!["years".to_string(), "weight".to_string(), "level".to_string()],
            })
            .unwrap();
        catalog
            .create(IndexDefinition {
                name: "knows_since_years_weight_idx".to_string(),
                entity_type: CatalogIndexEntityType::Relationship,
                kind: CatalogIndexKind::Range,
                label: "KNOWS".to_string(),
                properties: vec!["since".to_string(), "years".to_string(), "weight".to_string()],
            })
            .unwrap();

        let preferred = catalog
            .preferred_relationship_range_index_definition(
                "KNOWS",
                "weight",
                &HashMap::from([
                    (String::from("since"), json!(2020)),
                    (String::from("years"), json!(5)),
                    (String::from("level"), json!("strong")),
                ]),
            )
            .unwrap()
            .expect("expected preferred relationship range definition");

        assert_eq!(preferred.name, "knows_since_years_weight_idx");
    }

    #[test]
    fn preferred_relationship_range_index_definition_prefers_more_exact_fields_when_prefix_is_equal() {
        let (_temp_dir, storage) = open_test_storage("catalog-preferred-edge-non-leading-range-specificity");
        let catalog = IndexCatalog::new(&storage);

        catalog
            .create(IndexDefinition {
                name: "knows_years_weight_idx".to_string(),
                entity_type: CatalogIndexEntityType::Relationship,
                kind: CatalogIndexKind::Range,
                label: "KNOWS".to_string(),
                properties: vec!["years".to_string(), "weight".to_string()],
            })
            .unwrap();
        catalog
            .create(IndexDefinition {
                name: "knows_years_weight_level_idx".to_string(),
                entity_type: CatalogIndexEntityType::Relationship,
                kind: CatalogIndexKind::Range,
                label: "KNOWS".to_string(),
                properties: vec!["years".to_string(), "weight".to_string(), "level".to_string()],
            })
            .unwrap();

        let preferred = catalog
            .preferred_relationship_range_index_definition(
                "KNOWS",
                "weight",
                &HashMap::from([
                    (String::from("years"), json!(5)),
                    (String::from("level"), json!("strong")),
                ]),
            )
            .unwrap()
            .expect("expected preferred relationship range definition");

        assert_eq!(preferred.name, "knows_years_weight_level_idx");
    }
}
