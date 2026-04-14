//! Transaction session management for magnetDB.
//!
//! Provides ACID transaction handling with begin, commit, rollback, and
//! pending-write buffering.

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use thiserror::Error;
use uuid::Uuid;

// ─── Errors ──────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum TxError {
    #[error("transaction already committed")]
    AlreadyCommitted,
    #[error("transaction already rolled back")]
    AlreadyRolledBack,
    #[error("transaction conflict")]
    Conflict,
    #[error("transaction not found: {0}")]
    NotFound(String),
    #[error("transaction timed out")]
    TimedOut,
    #[error("transaction not active")]
    NotActive,
}

// ─── Enums ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    Active,
    Committed,
    RolledBack,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionMode {
    Read,
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BookmarkMode {
    None,
    Required,
    Optional,
}

/// Isolation level (kept for backward compatibility).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

// ─── TxOperation ─────────────────────────────────────────────────────────────

/// A single pending storage operation buffered by a transaction.
#[derive(Debug, Clone)]
pub enum TxOperation {
    Put { tree: String, key: Vec<u8>, value: Vec<u8> },
    Delete { tree: String, key: Vec<u8> },
}

// ─── Transaction ─────────────────────────────────────────────────────────────

/// An active database transaction.
#[derive(Debug)]
pub struct Transaction {
    pub id: Uuid,
    pub state: TransactionState,
    pub mode: TransactionMode,
    pub created_at: Instant,
    pub timeout: Duration,
    pub bookmarks: Vec<String>,
    pub database: Option<String>,
    pub(crate) pending: Vec<TxOperation>,
    // Legacy fields
    pub isolation: IsolationLevel,
}

impl Transaction {
    pub fn new(config: &SessionConfig) -> Self {
        Self {
            id: Uuid::new_v4(),
            state: TransactionState::Active,
            mode: config.mode,
            created_at: Instant::now(),
            timeout: config.timeout,
            bookmarks: config.bookmarks.clone(),
            database: config.database.clone(),
            pending: Vec::new(),
            isolation: IsolationLevel::ReadCommitted,
        }
    }

    /// Legacy constructor used in tests.
    pub fn begin(database: impl Into<String>, isolation: IsolationLevel) -> Self {
        let db_str = database.into();
        Self {
            id: Uuid::new_v4(),
            state: TransactionState::Active,
            mode: TransactionMode::Write,
            created_at: Instant::now(),
            timeout: Duration::from_secs(30),
            bookmarks: Vec::new(),
            database: Some(db_str),
            pending: Vec::new(),
            isolation,
        }
    }

    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active && !self.is_expired()
    }

    pub fn is_expired(&self) -> bool {
        self.created_at.elapsed() > self.timeout
    }

    pub fn commit(&mut self) -> Result<(), TxError> {
        if self.is_expired() {
            self.state = TransactionState::Failed;
            return Err(TxError::TimedOut);
        }
        match self.state {
            TransactionState::Active => {
                self.state = TransactionState::Committed;
                Ok(())
            }
            TransactionState::Committed => Err(TxError::AlreadyCommitted),
            TransactionState::RolledBack => Err(TxError::AlreadyRolledBack),
            TransactionState::Failed => Err(TxError::NotActive),
        }
    }

    pub fn rollback(&mut self) -> Result<(), TxError> {
        match self.state {
            TransactionState::Active | TransactionState::Failed => {
                self.state = TransactionState::RolledBack;
                self.pending.clear();
                Ok(())
            }
            TransactionState::Committed => Err(TxError::AlreadyCommitted),
            TransactionState::RolledBack => Err(TxError::AlreadyRolledBack),
        }
    }

    pub fn add_operation(&mut self, op: TxOperation) -> Result<(), TxError> {
        if !self.is_active() {
            return Err(TxError::NotActive);
        }
        if self.mode == TransactionMode::Read {
            return Err(TxError::NotActive); // write on read tx
        }
        self.pending.push(op);
        Ok(())
    }

    pub fn take_pending(&mut self) -> Vec<TxOperation> {
        std::mem::take(&mut self.pending)
    }
}

// ─── TxState (legacy alias) ──────────────────────────────────────────────────

/// Legacy alias kept for backward compat.
pub type TxState = TransactionState;

// ─── SessionConfig ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub mode: TransactionMode,
    pub database: Option<String>,
    pub fetch_size: usize,
    pub timeout: Duration,
    pub bookmarks: Vec<String>,
    pub bookmark_mode: BookmarkMode,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            mode: TransactionMode::Write,
            database: None,
            fetch_size: 1000,
            timeout: Duration::from_secs(30),
            bookmarks: Vec::new(),
            bookmark_mode: BookmarkMode::None,
        }
    }
}

// ─── TransactionManager ──────────────────────────────────────────────────────

/// Manages all active transactions.
pub struct TransactionManager {
    active: DashMap<Uuid, Transaction>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            active: DashMap::new(),
        }
    }

    /// Begin a new transaction, returning its ID.
    pub fn begin(&self, config: &SessionConfig) -> Result<Uuid, TxError> {
        let tx = Transaction::new(config);
        let id = tx.id;
        self.active.insert(id, tx);
        Ok(id)
    }

    /// Commit the transaction with the given ID.
    /// Removes the transaction from `active` on success.
    pub fn commit(&self, id: Uuid) -> Result<(), TxError> {
        let (_, mut tx) = self.active
            .remove(&id)
            .ok_or_else(|| TxError::NotFound(id.to_string()))?;
        match tx.commit() {
            Ok(()) => Ok(()),
            Err(err) => {
                self.active.insert(id, tx);
                Err(err)
            }
        }
    }

    /// Rollback the transaction with the given ID.
    /// Removes the transaction from `active` on success.
    pub fn rollback(&self, id: Uuid) -> Result<(), TxError> {
        let (_, mut tx) = self.active
            .remove(&id)
            .ok_or_else(|| TxError::NotFound(id.to_string()))?;
        match tx.rollback() {
            Ok(()) => Ok(()),
            Err(err) => {
                self.active.insert(id, tx);
                Err(err)
            }
        }
    }

    /// Check if a transaction is active.
    pub fn is_active(&self, id: &Uuid) -> bool {
        self.active.get(id).map(|tx| tx.is_active()).unwrap_or(false)
    }

    /// Add an operation to a transaction's pending list.
    pub fn add_operation(&self, id: &Uuid, op: TxOperation) -> Result<(), TxError> {
        let mut entry = self.active
            .get_mut(id)
            .ok_or_else(|| TxError::NotFound(id.to_string()))?;
        entry.add_operation(op)
    }

    /// Get read-only access to a transaction.
    pub fn get(&self, id: &Uuid) -> Option<dashmap::mapref::one::Ref<'_, Uuid, Transaction>> {
        self.active.get(id)
    }

    /// Get mutable access to a transaction.
    pub fn get_mut(&self, id: &Uuid) -> Option<dashmap::mapref::one::RefMut<'_, Uuid, Transaction>> {
        self.active.get_mut(id)
    }

    /// Remove expired transactions.
    pub fn cleanup_expired(&self) {
        self.active.retain(|_, tx| !tx.is_expired());
    }

    /// Remove a transaction after it's been fully processed.
    pub fn remove(&self, id: &Uuid) {
        self.active.remove(id);
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_lifecycle() {
        let mut tx = Transaction::begin("testdb", IsolationLevel::ReadCommitted);
        assert!(tx.is_active());
        tx.commit().unwrap();
        assert!(!tx.is_active());
        assert!(tx.commit().is_err());
    }

    #[test]
    fn test_rollback() {
        let mut tx = Transaction::begin("testdb", IsolationLevel::Serializable);
        tx.rollback().unwrap();
        assert!(!tx.is_active());
        assert!(tx.rollback().is_err());
    }

    #[test]
    fn test_transaction_manager_begin_commit() {
        let mgr = TransactionManager::new();
        let config = SessionConfig::default();
        let id = mgr.begin(&config).unwrap();
        assert!(mgr.is_active(&id));
        mgr.commit(id).unwrap();
        assert!(!mgr.is_active(&id));
    }

    #[test]
    fn test_transaction_manager_rollback() {
        let mgr = TransactionManager::new();
        let config = SessionConfig::default();
        let id = mgr.begin(&config).unwrap();
        mgr.rollback(id).unwrap();
        assert!(!mgr.is_active(&id));
    }

    #[test]
    fn test_transaction_manager_not_found() {
        let mgr = TransactionManager::new();
        let id = Uuid::new_v4();
        assert!(matches!(mgr.commit(id), Err(TxError::NotFound(_))));
    }

    #[test]
    fn test_add_operation() {
        let mgr = TransactionManager::new();
        let config = SessionConfig::default();
        let id = mgr.begin(&config).unwrap();
        mgr.add_operation(&id, TxOperation::Put {
            tree: "nodes".to_string(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        }).unwrap();
        let tx = mgr.get(&id).unwrap();
        assert_eq!(tx.pending.len(), 1);
    }

    #[test]
    fn test_cleanup_expired() {
        let mgr = TransactionManager::new();
        let mut config = SessionConfig::default();
        config.timeout = Duration::from_millis(1);
        let id = mgr.begin(&config).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        mgr.cleanup_expired();
        assert!(mgr.get(&id).is_none());
    }

    #[test]
    fn test_session_config_defaults() {
        let config = SessionConfig::default();
        assert_eq!(config.mode, TransactionMode::Write);
        assert_eq!(config.fetch_size, 1000);
        assert_eq!(config.bookmark_mode, BookmarkMode::None);
    }
}

