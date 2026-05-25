//! Multi-database management for copperdb.
//!
//! Equivalent to Go's `pkg/multidb` in NornicDB.
//! Manages multiple isolated database instances within a single copperdb
//! server, with per-database auth, storage, and index spaces.

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MultiDbError {
    #[error("database already exists: {0}")]
    AlreadyExists(String),
    #[error("database not found: {0}")]
    NotFound(String),
    #[error("cannot drop system database")]
    CannotDropSystem,
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
#[derive(Default)]
pub struct DatabaseManager {
    databases: DashMap<String, Database>,
}

impl DatabaseManager {
    pub fn new() -> Self {
        let manager = Self::default();
        // Create the system database (always exists)
        manager.databases.insert(
            "system".into(),
            Database {
                name: "system".into(),
                storage_path: "./data/system".into(),
                status: DatabaseStatus::Online,
                created_at: 0,
            },
        );
        // Create the default database
        manager.databases.insert(
            "default".into(),
            Database {
                name: "default".into(),
                storage_path: "./data/default".into(),
                status: DatabaseStatus::Online,
                created_at: 0,
            },
        );
        manager
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
        self.databases.insert(
            name.clone(),
            Database {
                name,
                storage_path: storage_path.into(),
                status: DatabaseStatus::Online,
                created_at: now,
            },
        );
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
        Ok(())
    }

    pub fn list(&self) -> Vec<Database> {
        self.databases.iter().map(|e| e.value().clone()).collect()
    }
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
}
