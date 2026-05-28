//! Graph index catalog management for copperdb.
//!
//! The Layer 4 indexing crate owns the typed create/drop/list contract for
//! durable index definitions while delegating persistence to `copperdb-storage`.
//! This keeps Cypher evaluation, engine wiring, and future index workers on one
//! path for index catalog validation and deterministic listing behavior.

use copperdb_storage::{EdgeRecord, IndexDefinition, NodeRecord, StorageEngine, StorageError};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use thiserror::Error;

pub use copperdb_storage::{IndexDefinition as CatalogIndexDefinition, IndexEntityType as CatalogIndexEntityType};

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
            return Ok(self.storage.all_node_records()?);
        };

        if let Some((property, value)) = self.preferred_indexed_property(primary_label, properties)?
        {
            return Ok(self
                .storage
                .get_nodes_by_property(primary_label, property, value)?);
        }

        Ok(self.storage.get_nodes_by_label(primary_label)?)
    }

    pub fn lookup_edges(&self, edge_type: Option<&str>) -> Result<Vec<EdgeRecord>, IndexError> {
        match edge_type {
            Some(edge_type) if !edge_type.is_empty() => Ok(self.storage.get_edges_by_type(edge_type)?),
            _ => Ok(self.storage.all_edges()?),
        }
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

    fn preferred_indexed_property<'b>(
        &self,
        label: &str,
        properties: &'b HashMap<String, Value>,
    ) -> Result<Option<(&'b str, &'b Value)>, IndexError> {
        let indexed_properties: BTreeSet<String> = self
            .list()?
            .into_iter()
            .filter(|definition| {
                definition.entity_type == CatalogIndexEntityType::Node
                    && definition.label == label
                    && definition.properties.len() == 1
            })
            .map(|definition| definition.properties[0].clone())
            .collect();

        let mut property_names = properties.keys().collect::<Vec<_>>();
        property_names.sort();

        for property_name in property_names {
            if indexed_properties.contains(property_name.as_str()) {
                return Ok(Some((property_name.as_str(), &properties[property_name])));
            }
        }

        Ok(None)
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
        storage
            .put_edge_record(&EdgeRecord {
                id: id.to_string(),
                start_node: start.to_string(),
                end_node: end.to_string(),
                edge_type: edge_type.to_string(),
                properties: BTreeMap::new(),
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
            vec!["person:1", "person:2", "robot:1"]
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
    fn lookup_edges_falls_back_to_all_edges_when_type_is_absent() {
        let (_temp_dir, storage) = open_test_storage("catalog-lookup-all-edges");
        let catalog = IndexCatalog::new(&storage);

        store_edge(&storage, "rel:3", "LIKES", "person:1", "movie:1");
        store_edge(&storage, "rel:1", "KNOWS", "person:1", "person:2");
        store_edge(&storage, "rel:2", "KNOWS", "person:2", "person:3");

        let edges = catalog.lookup_edges(None).unwrap();
        assert_eq!(edges.iter().map(|edge| edge.id.as_str()).collect::<Vec<_>>(), vec!["rel:1", "rel:2", "rel:3"]);
    }
}
