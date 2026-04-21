//! Graph index management for copperdb.
//!
//! Equivalent to Go's `pkg/indexing` in NornicDB.
//! Manages B-tree property indexes, composite indexes, and full-text indexes
//! that accelerate `MATCH` clause lookups.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("index already exists: {0}")]
    AlreadyExists(String),
    #[error("index not found: {0}")]
    NotFound(String),
    #[error("index build error: {0}")]
    BuildError(String),
}

/// Index type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexType {
    /// B-tree property index for equality/range lookups.
    BTree,
    /// Full-text inverted index (powered by Tantivy).
    FullText,
    /// Vector/HNSW index for similarity search.
    Vector { dimensions: usize },
    /// Composite index over multiple properties.
    Composite,
}

/// Index definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub name: String,
    pub label: String,
    pub properties: Vec<String>,
    pub index_type: IndexType,
    pub unique: bool,
}

/// Index registry.
#[derive(Default)]
pub struct IndexRegistry {
    indexes: std::collections::HashMap<String, IndexDefinition>,
}

impl IndexRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create(&mut self, def: IndexDefinition) -> Result<(), IndexError> {
        if self.indexes.contains_key(&def.name) {
            return Err(IndexError::AlreadyExists(def.name.clone()));
        }
        self.indexes.insert(def.name.clone(), def);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<&IndexDefinition> {
        self.indexes.get(name)
    }

    pub fn drop(&mut self, name: &str) -> Result<(), IndexError> {
        self.indexes.remove(name).ok_or_else(|| IndexError::NotFound(name.to_owned()))?;
        Ok(())
    }

    pub fn list(&self) -> Vec<&IndexDefinition> {
        self.indexes.values().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_get_index() {
        let mut registry = IndexRegistry::new();
        registry.create(IndexDefinition {
            name: "person_name".into(),
            label: "Person".into(),
            properties: vec!["name".into()],
            index_type: IndexType::BTree,
            unique: false,
        }).unwrap();
        assert!(registry.get("person_name").is_some());
    }

    #[test]
    fn test_duplicate_index_error() {
        let mut registry = IndexRegistry::new();
        let def = IndexDefinition {
            name: "idx".into(),
            label: "Person".into(),
            properties: vec!["id".into()],
            index_type: IndexType::BTree,
            unique: true,
        };
        registry.create(def.clone()).unwrap();
        assert!(registry.create(def).is_err());
    }
}
