//! Multi-database management for copperdb.
//!
//! Equivalent to Go's `pkg/multidb` in NornicDB.
//! Manages multiple isolated database instances within a single copperdb
//! server, with per-database auth, storage, and index spaces.

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use thiserror::Error;

use copperdb_config::{
    allowed_per_database_config_keys, resolve_per_database_config, validate_per_database_overrides,
    Config as GlobalConfig, EffectiveDatabaseConfig, PerDatabaseConfigKey,
};
use copperdb_storage::{NodeRecord, StorageEngine};

const DATABASE_LABEL: &str = "DatabaseCatalogEntry";
const DATABASE_PAYLOAD_PROPERTY: &str = "payload";
const DATABASE_CONFIG_LABEL: &str = "DatabaseConfigOverrideEntry";
const DATABASE_CONFIG_PAYLOAD_PROPERTY: &str = "overrides";

#[derive(Debug, Error)]
pub enum MultiDbError {
    #[error("database already exists: {0}")]
    AlreadyExists(String),
    #[error("database not found: {0}")]
    NotFound(String),
    #[error("cannot drop system database")]
    CannotDropSystem,
    #[error("storage error: {0}")]
    Storage(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("invalid database config override: {0}")]
    InvalidConfig(String),
}

impl From<copperdb_storage::StorageError> for MultiDbError {
    fn from(error: copperdb_storage::StorageError) -> Self {
        MultiDbError::Storage(error.to_string())
    }
}

impl From<serde_json::Error> for MultiDbError {
    fn from(error: serde_json::Error) -> Self {
        MultiDbError::Serialization(error.to_string())
    }
}

/// Database metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Database {
    pub name: String,
    pub storage_path: String,
    pub status: DatabaseStatus,
    pub created_at: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DatabaseStatus {
    Online,
    Offline,
    Deleted,
}

/// System-level database manager.
pub struct DatabaseManager {
    databases: DashMap<String, Database>,
    config_overrides: DashMap<String, BTreeMap<String, String>>,
    catalog_path: Option<PathBuf>,
}

impl Default for DatabaseManager {
    fn default() -> Self {
        Self {
            databases: DashMap::new(),
            config_overrides: DashMap::new(),
            catalog_path: None,
        }
    }
}

impl DatabaseManager {
    pub fn new() -> Self {
        let manager = Self::default();
        manager.seed_builtin_databases();
        manager
    }

    pub fn open(catalog_path: impl AsRef<Path>) -> Result<Self, MultiDbError> {
        let catalog_path = catalog_path.as_ref().to_path_buf();
        let storage = StorageEngine::open(&catalog_path)?;
        let catalog = storage.for_namespace("multidb");
        let manager = Self {
            databases: DashMap::new(),
            config_overrides: DashMap::new(),
            catalog_path: Some(catalog_path),
        };
        for node in catalog.get_nodes_by_label(DATABASE_LABEL)? {
            let database = database_from_node(&node)?;
            manager.databases.insert(database.name.clone(), database);
        }
        for node in catalog.get_nodes_by_label(DATABASE_CONFIG_LABEL)? {
            let (name, overrides) = database_config_from_node(&node)?;
            manager.config_overrides.insert(name, overrides);
        }
        manager.seed_builtin_databases();
        // Persist using the already-open storage instead of opening a new one.
        manager.persist_all_with(&storage)?;
        drop(storage);
        Ok(manager)
    }

    fn seed_builtin_databases(&self) {
        self.databases
            .entry("system".into())
            .or_insert_with(|| Database {
                name: "system".into(),
                storage_path: "./data/system".into(),
                status: DatabaseStatus::Online,
                created_at: 0,
            });
        self.databases
            .entry("default".into())
            .or_insert_with(|| Database {
                name: "default".into(),
                storage_path: "./data/default".into(),
                status: DatabaseStatus::Online,
                created_at: 0,
            });
    }

    pub fn create(
        &self,
        name: impl Into<String>,
        storage_path: impl Into<String>,
    ) -> Result<(), MultiDbError> {
        let name = name.into();
        if self.databases.contains_key(&name) {
            return Err(MultiDbError::AlreadyExists(name));
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let database = Database {
            name: name.clone(),
            storage_path: storage_path.into(),
            status: DatabaseStatus::Online,
            created_at: now,
        };
        self.persist_database(&database)?;
        self.databases.insert(name, database);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<Database> {
        self.databases.get(name).map(|d| d.clone())
    }

    pub fn drop(&self, name: &str) -> Result<(), MultiDbError> {
        if name == "system" {
            return Err(MultiDbError::CannotDropSystem);
        }
        self.databases
            .remove(name)
            .ok_or_else(|| MultiDbError::NotFound(name.to_owned()))?;
        self.delete_database_record(name)?;
        Ok(())
    }

    pub fn list(&self) -> Vec<Database> {
        self.databases.iter().map(|e| e.value().clone()).collect()
    }

    pub fn allowed_config_keys(&self) -> &'static [PerDatabaseConfigKey] {
        allowed_per_database_config_keys()
    }

    pub fn get_config_overrides(&self, name: &str) -> BTreeMap<String, String> {
        self.config_overrides
            .get(name)
            .map(|entry| entry.clone())
            .unwrap_or_default()
    }

    pub fn set_config_overrides(
        &self,
        name: &str,
        overrides: BTreeMap<String, String>,
    ) -> Result<(), MultiDbError> {
        if !self.databases.contains_key(name) {
            return Err(MultiDbError::NotFound(name.to_owned()));
        }
        validate_per_database_overrides(&overrides)
            .map_err(|error| MultiDbError::InvalidConfig(error.to_string()))?;
        self.persist_database_config(name, &overrides)?;
        if overrides.is_empty() {
            self.config_overrides.remove(name);
        } else {
            self.config_overrides.insert(name.to_owned(), overrides);
        }
        Ok(())
    }

    pub fn effective_config(
        &self,
        name: &str,
        global: &GlobalConfig,
    ) -> Result<EffectiveDatabaseConfig, MultiDbError> {
        if !self.databases.contains_key(name) {
            return Err(MultiDbError::NotFound(name.to_owned()));
        }
        resolve_per_database_config(global, &self.get_config_overrides(name))
            .map_err(|error| MultiDbError::InvalidConfig(error.to_string()))
    }

    fn persist_all_with(&self, storage: &StorageEngine) -> Result<(), MultiDbError> {
        for database in self.list() {
            self.persist_database_with(storage, &database)?;
        }
        Ok(())
    }

    fn persist_database(&self, database: &Database) -> Result<(), MultiDbError> {
        let Some(path) = &self.catalog_path else {
            return Ok(());
        };
        let storage = StorageEngine::open(path)?;
        self.persist_database_with(&storage, database)?;
        Ok(())
    }

    fn persist_database_with(&self, storage: &StorageEngine, database: &Database) -> Result<(), MultiDbError> {
        storage
            .for_namespace("multidb")
            .put_node_record(&database_to_node(database)?)?;
        Ok(())
    }

    fn persist_database_config(
        &self,
        name: &str,
        overrides: &BTreeMap<String, String>,
    ) -> Result<(), MultiDbError> {
        let Some(path) = &self.catalog_path else {
            return Ok(());
        };
        let storage = StorageEngine::open(path)?;
        let catalog = storage.for_namespace("multidb");
        if overrides.is_empty() {
            match catalog.delete_node_record(&database_config_node_id(name)) {
                Ok(()) => {}
                Err(copperdb_storage::StorageError::NotFound(_)) => {}
                Err(error) => return Err(error.into()),
            }
        } else {
            catalog.put_node_record(&database_config_to_node(name, overrides)?)?;
        }
        Ok(())
    }

    fn delete_database_record(&self, name: &str) -> Result<(), MultiDbError> {
        let Some(path) = &self.catalog_path else {
            return Ok(());
        };
        let storage = StorageEngine::open(path)?;
        let catalog = storage.for_namespace("multidb");
        catalog.delete_node_record(&database_node_id(name))?;
        match catalog.delete_node_record(&database_config_node_id(name)) {
            Ok(()) => {}
            Err(copperdb_storage::StorageError::NotFound(_)) => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
}

fn database_node_id(name: &str) -> String {
    format!("database:{name}")
}

fn database_config_node_id(name: &str) -> String {
    format!("database-config:{name}")
}

fn database_to_node(database: &Database) -> Result<NodeRecord, MultiDbError> {
    let mut properties = BTreeMap::new();
    properties.insert(
        "name".into(),
        serde_json::Value::String(database.name.clone()),
    );
    properties.insert(
        DATABASE_PAYLOAD_PROPERTY.into(),
        serde_json::to_value(database)?,
    );
    Ok(NodeRecord {
        id: database_node_id(&database.name),
        labels: vec![DATABASE_LABEL.into()],
        properties,
        named_embeddings: BTreeMap::new(),
        chunk_embeddings: Vec::new(),
        embed_meta: Default::default(),
        created_at_unix_ms: now_unix_ms(),
        updated_at_unix_ms: now_unix_ms(),
    })
}

fn database_from_node(node: &NodeRecord) -> Result<Database, MultiDbError> {
    let payload = node
        .properties
        .get(DATABASE_PAYLOAD_PROPERTY)
        .cloned()
        .ok_or_else(|| MultiDbError::Serialization("missing payload".into()))?;
    Ok(serde_json::from_value(payload)?)
}

fn database_config_to_node(
    name: &str,
    overrides: &BTreeMap<String, String>,
) -> Result<NodeRecord, MultiDbError> {
    let mut properties = BTreeMap::new();
    properties.insert("name".into(), serde_json::Value::String(name.to_owned()));
    properties.insert(
        DATABASE_CONFIG_PAYLOAD_PROPERTY.into(),
        serde_json::to_value(overrides)?,
    );
    Ok(NodeRecord {
        id: database_config_node_id(name),
        labels: vec![DATABASE_CONFIG_LABEL.into()],
        properties,
        named_embeddings: BTreeMap::new(),
        chunk_embeddings: Vec::new(),
        embed_meta: Default::default(),
        created_at_unix_ms: now_unix_ms(),
        updated_at_unix_ms: now_unix_ms(),
    })
}

fn database_config_from_node(
    node: &NodeRecord,
) -> Result<(String, BTreeMap<String, String>), MultiDbError> {
    let name = node
        .properties
        .get("name")
        .and_then(|value| value.as_str())
        .ok_or_else(|| MultiDbError::Serialization("missing name".into()))?
        .to_owned();
    let payload = node
        .properties
        .get(DATABASE_CONFIG_PAYLOAD_PROPERTY)
        .cloned()
        .ok_or_else(|| MultiDbError::Serialization("missing overrides".into()))?;
    let overrides = serde_json::from_value(payload)?;
    Ok((name, overrides))
}

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_get_database() {
        let manager = DatabaseManager::new();
        manager.create("mydb", "./data/mydb").unwrap();
        let db = manager.get("mydb").unwrap();
        assert_eq!(db.status, DatabaseStatus::Online);
    }

    #[test]
    fn test_cannot_drop_system() {
        let manager = DatabaseManager::new();
        assert!(matches!(
            manager.drop("system"),
            Err(MultiDbError::CannotDropSystem)
        ));
    }

    #[test]
    fn catalog_persists_created_and_dropped_databases() {
        let dir = tempfile::tempdir().unwrap();
        let catalog_path = dir.path().join("catalog");

        let manager = DatabaseManager::open(&catalog_path).unwrap();
        manager.create("clinic", "./data/clinic").unwrap();
        manager.create("analytics", "./data/analytics").unwrap();
        manager.drop("analytics").unwrap();
        drop(manager);

        let reloaded = DatabaseManager::open(&catalog_path).unwrap();
        assert_eq!(
            reloaded.get("clinic").unwrap().storage_path,
            "./data/clinic"
        );
        assert!(reloaded.get("analytics").is_none());
        assert!(reloaded.get("system").is_some());
        assert!(reloaded.get("default").is_some());
    }

    #[test]
    fn config_overrides_persist_and_reload() {
        let dir = tempfile::tempdir().unwrap();
        let catalog_path = dir.path().join("catalog");

        let manager = DatabaseManager::open(&catalog_path).unwrap();
        manager.create("clinic", "./data/clinic").unwrap();
        manager
            .set_config_overrides(
                "clinic",
                BTreeMap::from([
                    ("COPPERDB_SEARCH_BM25_ENABLED".into(), "true".into()),
                    ("COPPERDB_SEARCH_VECTOR_ENABLED".into(), "false".into()),
                ]),
            )
            .unwrap();
        drop(manager);

        let reloaded = DatabaseManager::open(&catalog_path).unwrap();
        let overrides = reloaded.get_config_overrides("clinic");
        assert_eq!(
            overrides.get("COPPERDB_SEARCH_BM25_ENABLED").unwrap(),
            "true"
        );
        assert_eq!(reloaded.allowed_config_keys().len(), 13);
    }

    #[test]
    fn effective_config_uses_global_defaults_and_cli_precedence() {
        let manager = DatabaseManager::new();
        manager.create("clinic", "./data/clinic").unwrap();
        manager
            .set_config_overrides(
                "clinic",
                BTreeMap::from([("COPPERDB_SEARCH_VECTOR_ENABLED".into(), "true".into())]),
            )
            .unwrap();

        let mut global = GlobalConfig::default();
        global
            .cli_overrides
            .insert("COPPERDB_SEARCH_VECTOR_ENABLED".into(), "false".into());

        let effective = manager.effective_config("clinic", &global).unwrap();
        assert!(!effective.vector_enabled);
    }

    #[test]
    fn rejecting_unknown_config_override_keeps_store_clean() {
        let manager = DatabaseManager::new();
        manager.create("clinic", "./data/clinic").unwrap();

        let error = manager
            .set_config_overrides(
                "clinic",
                BTreeMap::from([("COPPERDB_UNKNOWN".into(), "true".into())]),
            )
            .unwrap_err();

        assert!(matches!(error, MultiDbError::InvalidConfig(_)));
        assert!(manager.get_config_overrides("clinic").is_empty());
    }
}
