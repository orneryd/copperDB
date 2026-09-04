//! Transaction session management for copperdb.
//!
//! Provides ACID transaction handling with begin, commit, rollback, and
//! pending-write buffering.

use copperdb_errors::{TransientTransactionCode, map_transient_transaction_error};
use copperdb_topology::{DistributedTransactionClock, LogicalTransactionId, TransactionTimeOracle};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use uuid::Uuid;

// ─── Errors ──────────────────────────────────────────────────────────────────

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TxError {
    #[error("no active transaction")]
    NoActiveTransaction,
    #[error("transaction already active")]
    TransactionActive,
    #[error("transaction already closed")]
    TransactionClosed,
    #[error("transaction rolled back")]
    TransactionRolledBack,
    #[error("transaction is read-only")]
    TransactionReadOnly,
    #[error("transaction timed out")]
    TimedOut,
    #[error("transaction conflict")]
    Conflict,
    #[error("invalid bookmark: {0}")]
    InvalidBookmark(String),
    #[error("terminal transaction error: {0}")]
    Terminal(String),
}

pub fn classify_retryable_error(
    err: &(dyn std::error::Error + 'static),
) -> Option<TransientTransactionCode> {
    if let Some(tx_error) = err.downcast_ref::<TxError>() {
        return match tx_error {
            TxError::Conflict => Some(TransientTransactionCode::Outdated),
            TxError::TimedOut => Some(TransientTransactionCode::Outdated),
            _ => None,
        };
    }

    map_transient_transaction_error(err)
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
    Put {
        tree: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        tree: String,
        key: Vec<u8>,
    },
}

fn parse_bookmark(value: &str) -> Result<LogicalTransactionId, TxError> {
    let trimmed = value.trim();
    let mut parts = trimmed.split(':');
    let Some(epoch_hex) = parts.next() else {
        return Err(TxError::InvalidBookmark(trimmed.to_string()));
    };
    let Some(counter_hex) = parts.next() else {
        return Err(TxError::InvalidBookmark(trimmed.to_string()));
    };
    let Some(node_hex) = parts.next() else {
        return Err(TxError::InvalidBookmark(trimmed.to_string()));
    };
    if parts.next().is_some() {
        return Err(TxError::InvalidBookmark(trimmed.to_string()));
    }

    let epoch = u64::from_str_radix(epoch_hex, 16)
        .map_err(|_| TxError::InvalidBookmark(trimmed.to_string()))?;
    let counter = u64::from_str_radix(counter_hex, 16)
        .map_err(|_| TxError::InvalidBookmark(trimmed.to_string()))?;
    let node_ordinal = u32::from_str_radix(node_hex, 16)
        .map_err(|_| TxError::InvalidBookmark(trimmed.to_string()))?;

    Ok(LogicalTransactionId::new(epoch, counter, node_ordinal))
}

fn normalize_bookmarks(bookmarks: &[String], mode: BookmarkMode) -> Result<Vec<String>, TxError> {
    if mode == BookmarkMode::None {
        return Ok(Vec::new());
    }

    let mut normalized = Vec::new();
    for bookmark in bookmarks {
        match parse_bookmark(bookmark) {
            Ok(parsed) => normalized.push(parsed.stable_id()),
            Err(_) if mode == BookmarkMode::Optional => {}
            Err(err) => return Err(err),
        }
    }

    Ok(normalized)
}

fn resolve_bookmark_fence(
    bookmarks: &[String],
    mode: BookmarkMode,
) -> Result<Option<LogicalTransactionId>, TxError> {
    if mode == BookmarkMode::None {
        return Ok(None);
    }

    let mut fence: Option<LogicalTransactionId> = None;
    for bookmark in bookmarks {
        match parse_bookmark(bookmark) {
            Ok(parsed) => {
                fence = Some(fence.map(|current| current.max(parsed)).unwrap_or(parsed));
            }
            Err(_) if mode == BookmarkMode::Optional => {}
            Err(err) => return Err(err),
        }
    }

    Ok(fence)
}

pub fn resolve_read_fence_from_bookmarks(
    bookmarks: &[String],
) -> Result<Option<LogicalTransactionId>, TxError> {
    resolve_bookmark_fence(bookmarks, BookmarkMode::Required)
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
    pub begin_timestamp: LogicalTransactionId,
    pub commit_timestamp: Option<LogicalTransactionId>,
    pub(crate) pending: Vec<TxOperation>,
    pub isolation: IsolationLevel,
}

impl Transaction {
    pub fn new(config: &SessionConfig) -> Self {
        Self::new_with_timestamp(config, LogicalTransactionId::ZERO)
    }

    pub fn new_with_timestamp(
        config: &SessionConfig,
        begin_timestamp: LogicalTransactionId,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            state: TransactionState::Active,
            mode: config.mode,
            created_at: Instant::now(),
            timeout: config.timeout,
            bookmarks: config.bookmarks.clone(),
            database: config.database.clone(),
            begin_timestamp,
            commit_timestamp: None,
            pending: Vec::new(),
            isolation: IsolationLevel::ReadCommitted,
        }
    }

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
            begin_timestamp: LogicalTransactionId::ZERO,
            commit_timestamp: None,
            pending: Vec::new(),
            isolation,
        }
    }

    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active && !self.is_expired()
    }

    pub fn read_fence(&self) -> LogicalTransactionId {
        self.begin_timestamp
    }

    pub fn is_expired(&self) -> bool {
        self.created_at.elapsed() > self.timeout
    }

    pub fn commit(&mut self) -> Result<(), TxError> {
        self.commit_at(LogicalTransactionId::ZERO)
    }

    pub fn commit_at(&mut self, commit_timestamp: LogicalTransactionId) -> Result<(), TxError> {
        if self.is_expired() {
            self.state = TransactionState::Failed;
            self.pending.clear();
            return Err(TxError::TimedOut);
        }
        match self.state {
            TransactionState::Active => {
                self.state = TransactionState::Committed;
                self.commit_timestamp = Some(commit_timestamp);
                Ok(())
            }
            TransactionState::Committed | TransactionState::Failed => {
                Err(TxError::TransactionClosed)
            }
            TransactionState::RolledBack => Err(TxError::TransactionRolledBack),
        }
    }

    pub fn rollback(&mut self) -> Result<(), TxError> {
        match self.state {
            TransactionState::Active | TransactionState::Failed => {
                self.state = TransactionState::RolledBack;
                self.pending.clear();
                Ok(())
            }
            TransactionState::RolledBack => Err(TxError::TransactionRolledBack),
            TransactionState::Committed => Err(TxError::TransactionClosed),
        }
    }

    pub fn add_operation(&mut self, op: TxOperation) -> Result<(), TxError> {
        self.ensure_writable()?;
        if self.mode == TransactionMode::Read {
            return Err(TxError::TransactionReadOnly);
        }
        self.pending.push(op);
        Ok(())
    }

    fn ensure_writable(&self) -> Result<(), TxError> {
        if self.is_expired() {
            return Err(TxError::TimedOut);
        }
        match self.state {
            TransactionState::Active => Ok(()),
            TransactionState::Committed | TransactionState::Failed => {
                Err(TxError::TransactionClosed)
            }
            TransactionState::RolledBack => Err(TxError::TransactionRolledBack),
        }
    }

    pub fn take_pending(&mut self) -> Vec<TxOperation> {
        std::mem::take(&mut self.pending)
    }
}

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
    time_oracle: Arc<dyn TransactionTimeOracle>,
}

#[derive(Debug, Clone)]
pub struct Session {
    pub id: Uuid,
    pub transaction_id: Uuid,
    pub database: Option<String>,
    pub bookmarks: Vec<String>,
    pub read_fence: LogicalTransactionId,
    pub owner: Option<String>,
    pub expires_at: Instant,
    terminal_error: Option<TxError>,
}

impl Session {
    pub fn terminal_error(&self) -> Option<&TxError> {
        self.terminal_error.as_ref()
    }

    pub fn is_expired(&self) -> bool {
        Instant::now() > self.expires_at
    }

    pub fn read_fence(&self) -> LogicalTransactionId {
        self.read_fence
    }
}

pub struct SessionManager {
    sessions: DashMap<Uuid, Session>,
    transactions: Arc<TransactionManager>,
    ttl: Duration,
}

impl SessionManager {
    pub fn new(ttl: Duration) -> Self {
        Self::with_transactions(ttl, Arc::new(TransactionManager::new()))
    }

    pub fn with_transactions(ttl: Duration, transactions: Arc<TransactionManager>) -> Self {
        Self {
            sessions: DashMap::new(),
            transactions,
            ttl: if ttl.is_zero() {
                Duration::from_secs(30)
            } else {
                ttl
            },
        }
    }

    pub fn transactions(&self) -> &Arc<TransactionManager> {
        &self.transactions
    }

    pub fn open(&self, config: &SessionConfig) -> Result<Uuid, TxError> {
        self.open_for_owner(config, None)
    }

    pub fn open_for_owner(
        &self,
        config: &SessionConfig,
        owner: Option<&str>,
    ) -> Result<Uuid, TxError> {
        let transaction_id = self.transactions.begin(config)?;
        let transaction = self
            .transactions
            .get(&transaction_id)
            .ok_or(TxError::NoActiveTransaction)?;
        let session_id = Uuid::new_v4();
        let owner = owner.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });
        let session = Session {
            id: session_id,
            transaction_id,
            database: config.database.clone(),
            bookmarks: transaction.bookmarks.clone(),
            read_fence: transaction.read_fence(),
            owner,
            expires_at: Instant::now() + self.ttl,
            terminal_error: None,
        };
        self.sessions.insert(session_id, session);
        Ok(session_id)
    }

    pub fn get(&self, session_id: &Uuid) -> Option<dashmap::mapref::one::Ref<'_, Uuid, Session>> {
        self.get_for_owner(session_id, None)
    }

    pub fn get_for_owner(
        &self,
        session_id: &Uuid,
        owner: Option<&str>,
    ) -> Option<dashmap::mapref::one::Ref<'_, Uuid, Session>> {
        let session = self.sessions.get(session_id)?;
        match (
            &session.owner,
            owner.map(str::trim).filter(|value| !value.is_empty()),
        ) {
            (Some(bound_owner), Some(request_owner)) if bound_owner == request_owner => {
                Some(session)
            }
            (Some(_), _) => None,
            (None, _) => Some(session),
        }
    }

    pub fn touch(&self, session_id: &Uuid) -> Result<(), TxError> {
        let mut session = self
            .sessions
            .get_mut(session_id)
            .ok_or(TxError::NoActiveTransaction)?;
        session.expires_at = Instant::now() + self.ttl;
        Ok(())
    }

    pub fn record_terminal_error(
        &self,
        session_id: &Uuid,
        err: TxError,
    ) -> Result<TxError, TxError> {
        let mut session = self
            .sessions
            .get_mut(session_id)
            .ok_or(TxError::NoActiveTransaction)?;
        if let Some(existing) = &session.terminal_error {
            return Ok(existing.clone());
        }
        session.terminal_error = Some(err.clone());
        Ok(err)
    }

    pub fn commit_and_delete(&self, session_id: Uuid) -> Result<(), TxError> {
        self.commit_and_get_bookmark(session_id).map(|_| ())
    }

    pub fn read_fence(&self, session_id: &Uuid) -> Result<LogicalTransactionId, TxError> {
        let session = self.get(session_id).ok_or(TxError::NoActiveTransaction)?;
        Ok(session.read_fence())
    }

    pub fn commit_and_get_bookmark(&self, session_id: Uuid) -> Result<String, TxError> {
        let (_, session) = self
            .sessions
            .remove(&session_id)
            .ok_or(TxError::NoActiveTransaction)?;
        if let Some(err) = session.terminal_error {
            self.transactions.remove(&session.transaction_id);
            return Err(err);
        }
        self.transactions
            .commit_with_bookmark(session.transaction_id)
    }

    pub fn rollback_and_delete(&self, session_id: Uuid) -> Result<(), TxError> {
        let (_, session) = self
            .sessions
            .remove(&session_id)
            .ok_or(TxError::NoActiveTransaction)?;
        if session.terminal_error.is_some() {
            self.transactions.remove(&session.transaction_id);
            return Ok(());
        }
        self.transactions.rollback(session.transaction_id)
    }

    pub fn delete(&self, session_id: &Uuid) {
        if let Some((_, session)) = self.sessions.remove(session_id) {
            self.transactions.remove(&session.transaction_id);
        }
    }

    pub fn cleanup_expired(&self) {
        let expired: Vec<(Uuid, Uuid)> = self
            .sessions
            .iter()
            .filter_map(|entry| {
                if entry.is_expired() {
                    Some((*entry.key(), entry.transaction_id))
                } else {
                    None
                }
            })
            .collect();

        for (session_id, transaction_id) in expired {
            self.sessions.remove(&session_id);
            self.transactions.remove(&transaction_id);
        }
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

impl TransactionManager {
    pub fn new() -> Self {
        Self::with_clock(Arc::new(DistributedTransactionClock::new(0)))
    }

    pub fn with_clock(clock: Arc<DistributedTransactionClock>) -> Self {
        let time_oracle: Arc<dyn TransactionTimeOracle> = clock;
        Self::with_time_oracle(time_oracle)
    }

    pub fn with_time_oracle(time_oracle: Arc<dyn TransactionTimeOracle>) -> Self {
        Self {
            active: DashMap::new(),
            time_oracle,
        }
    }

    pub fn time_oracle(&self) -> &Arc<dyn TransactionTimeOracle> {
        &self.time_oracle
    }

    fn issue_begin_timestamp(
        &self,
        config: &SessionConfig,
    ) -> Result<LogicalTransactionId, TxError> {
        match resolve_bookmark_fence(&config.bookmarks, config.bookmark_mode)? {
            Some(fence) => Ok(self.time_oracle.observe(fence)),
            None => Ok(self.time_oracle.issue()),
        }
    }

    /// Begin a new transaction, returning its ID.
    pub fn begin(&self, config: &SessionConfig) -> Result<Uuid, TxError> {
        let begin_timestamp = self.issue_begin_timestamp(config)?;
        let mut tx = Transaction::new_with_timestamp(config, begin_timestamp);
        tx.bookmarks = normalize_bookmarks(&config.bookmarks, config.bookmark_mode)?;
        let id = tx.id;
        self.active.insert(id, tx);
        self.record_active_transactions();
        Ok(id)
    }

    pub fn read_fence(&self, id: &Uuid) -> Result<LogicalTransactionId, TxError> {
        let tx = self.get(id).ok_or(TxError::NoActiveTransaction)?;
        Ok(tx.read_fence())
    }

    pub fn commit_with_bookmark(&self, id: Uuid) -> Result<String, TxError> {
        let (_, mut tx) = self
            .active
            .remove(&id)
            .ok_or(TxError::NoActiveTransaction)?;
        let commit_timestamp = self.time_oracle.issue();
        let result = match tx.commit_at(commit_timestamp) {
            Ok(()) => Ok(commit_timestamp.stable_id()),
            Err(err) => {
                if tx.is_active() {
                    self.active.insert(id, tx);
                }
                Err(err)
            }
        };
        self.record_active_transactions();
        result
    }

    /// Commit the transaction with the given ID.
    /// Removes the transaction from `active` on success or terminal failure.
    /// Re-inserts only when the error is transient (i.e. the transaction
    /// remains Active so the caller can retry).
    pub fn commit(&self, id: Uuid) -> Result<(), TxError> {
        self.commit_with_bookmark(id).map(|_| ())
    }

    /// Rollback the transaction with the given ID.
    /// Removes the transaction from `active` on success or terminal failure.
    pub fn rollback(&self, id: Uuid) -> Result<(), TxError> {
        let (_, mut tx) = self
            .active
            .remove(&id)
            .ok_or(TxError::NoActiveTransaction)?;
        let result = match tx.rollback() {
            Ok(()) => Ok(()),
            Err(err) => {
                // Same policy: only keep the transaction around if it's still active.
                if tx.is_active() {
                    self.active.insert(id, tx);
                }
                Err(err)
            }
        };
        self.record_active_transactions();
        result
    }

    /// Check if a transaction is active.
    pub fn is_active(&self, id: &Uuid) -> bool {
        self.active
            .get(id)
            .map(|tx| tx.is_active())
            .unwrap_or(false)
    }

    /// Add an operation to a transaction's pending list.
    pub fn add_operation(&self, id: &Uuid, op: TxOperation) -> Result<(), TxError> {
        let mut entry = self
            .active
            .get_mut(id)
            .ok_or(TxError::NoActiveTransaction)?;
        entry.add_operation(op)
    }

    /// Get read-only access to a transaction.
    pub fn get(&self, id: &Uuid) -> Option<dashmap::mapref::one::Ref<'_, Uuid, Transaction>> {
        self.active.get(id)
    }

    /// Get mutable access to a transaction.
    pub fn get_mut(
        &self,
        id: &Uuid,
    ) -> Option<dashmap::mapref::one::RefMut<'_, Uuid, Transaction>> {
        self.active.get_mut(id)
    }

    /// Remove expired transactions.
    pub fn cleanup_expired(&self) {
        self.active.retain(|_, tx| !tx.is_expired());
        self.record_active_transactions();
    }

    /// Remove a transaction after it's been fully processed.
    pub fn remove(&self, id: &Uuid) {
        self.active.remove(id);
        self.record_active_transactions();
    }

    fn record_active_transactions(&self) {
        if let Some(telemetry) = copperdb_otel::global_telemetry() {
            let _ = telemetry.set_gauge(
                "nornicdb_cypher_active_transactions",
                &[],
                self.active.len() as f64,
            );
        }
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
        assert!(matches!(tx.commit(), Err(TxError::TransactionClosed)));
    }

    #[test]
    fn test_rollback() {
        let mut tx = Transaction::begin("testdb", IsolationLevel::Serializable);
        tx.rollback().unwrap();
        assert!(!tx.is_active());
        assert!(matches!(tx.rollback(), Err(TxError::TransactionRolledBack)));
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
    fn transaction_manager_assigns_logical_begin_and_commit_timestamps() {
        let clock = Arc::new(DistributedTransactionClock::with_epoch(11, 5));
        let mgr = TransactionManager::with_clock(Arc::clone(&clock));
        let config = SessionConfig::default();
        let id = mgr.begin(&config).unwrap();

        let begin_timestamp = mgr.get(&id).unwrap().begin_timestamp;
        assert_eq!(begin_timestamp.epoch, 5);
        assert_eq!(begin_timestamp.node_ordinal, 11);

        mgr.commit(id).unwrap();
        let next = clock.issue();
        assert_eq!(next.counter, begin_timestamp.counter + 2);
    }

    #[test]
    fn transaction_manager_begin_observes_required_bookmark_fence() {
        let clock = Arc::new(DistributedTransactionClock::with_epoch(11, 5));
        let mgr = TransactionManager::with_clock(clock);
        let fence = LogicalTransactionId::new(7, 41, 9);
        let config = SessionConfig {
            bookmarks: vec![fence.stable_id()],
            bookmark_mode: BookmarkMode::Required,
            ..SessionConfig::default()
        };

        let id = mgr.begin(&config).unwrap();
        let tx = mgr.get(&id).unwrap();

        assert_eq!(tx.bookmarks, vec![fence.stable_id()]);
        assert!(tx.begin_timestamp > fence);
        assert_eq!(tx.begin_timestamp.epoch, fence.epoch);
        assert_eq!(tx.begin_timestamp.counter, fence.counter + 1);
        assert_eq!(mgr.read_fence(&id).unwrap(), tx.begin_timestamp);
    }

    #[test]
    fn transaction_manager_begin_required_rejects_invalid_bookmark() {
        let mgr = TransactionManager::new();
        let config = SessionConfig {
            bookmarks: vec!["not-a-bookmark".to_string()],
            bookmark_mode: BookmarkMode::Required,
            ..SessionConfig::default()
        };

        let err = mgr.begin(&config).unwrap_err();
        assert!(matches!(err, TxError::InvalidBookmark(_)));
    }

    #[test]
    fn transaction_manager_begin_optional_ignores_invalid_bookmark() {
        let mgr = TransactionManager::new();
        let config = SessionConfig {
            bookmarks: vec!["not-a-bookmark".to_string()],
            bookmark_mode: BookmarkMode::Optional,
            ..SessionConfig::default()
        };

        let id = mgr.begin(&config).unwrap();
        let tx = mgr.get(&id).unwrap();

        assert!(tx.is_active());
        assert!(tx.bookmarks.is_empty());
    }

    #[test]
    fn transaction_manager_commit_with_bookmark_returns_commit_timestamp() {
        let clock = Arc::new(DistributedTransactionClock::with_epoch(11, 5));
        let mgr = TransactionManager::with_clock(Arc::clone(&clock));
        let id = mgr.begin(&SessionConfig::default()).unwrap();
        let begin_timestamp = mgr.get(&id).unwrap().begin_timestamp;

        let bookmark = mgr.commit_with_bookmark(id).unwrap();
        let commit_timestamp = parse_bookmark(&bookmark).unwrap();

        assert!(commit_timestamp > begin_timestamp);
        let next = clock.issue();
        assert_eq!(next.counter, commit_timestamp.counter + 1);
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
        assert!(matches!(mgr.commit(id), Err(TxError::NoActiveTransaction)));
    }

    #[test]
    fn test_add_operation() {
        let mgr = TransactionManager::new();
        let config = SessionConfig::default();
        let id = mgr.begin(&config).unwrap();
        mgr.add_operation(
            &id,
            TxOperation::Put {
                tree: "nodes".to_string(),
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
        )
        .unwrap();
        let tx = mgr.get(&id).unwrap();
        assert_eq!(tx.pending.len(), 1);
    }

    #[test]
    fn test_cleanup_expired() {
        let mgr = TransactionManager::new();
        let config = SessionConfig {
            timeout: Duration::from_millis(1),
            ..SessionConfig::default()
        };
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

    #[test]
    fn test_error_messages_match_nornicdb_transaction_contract() {
        assert_eq!(
            TxError::NoActiveTransaction.to_string(),
            "no active transaction"
        );
        assert_eq!(
            TxError::TransactionActive.to_string(),
            "transaction already active"
        );
        assert_eq!(
            TxError::TransactionClosed.to_string(),
            "transaction already closed"
        );
        assert_eq!(
            TxError::TransactionRolledBack.to_string(),
            "transaction rolled back"
        );
        assert_eq!(
            TxError::TransactionReadOnly.to_string(),
            "transaction is read-only"
        );
    }

    #[test]
    fn transaction_errors_use_shared_retry_classification() {
        assert_eq!(
            classify_retryable_error(&TxError::Conflict),
            Some(TransientTransactionCode::Outdated)
        );
        assert_eq!(
            classify_retryable_error(&TxError::TimedOut),
            Some(TransientTransactionCode::Outdated)
        );
        assert_eq!(
            classify_retryable_error(&TxError::TransactionReadOnly),
            None
        );
        assert_eq!(
            classify_retryable_error(&copperdb_errors::CopperDbError::TransactionDeadlock),
            Some(TransientTransactionCode::DeadlockDetected)
        );
    }

    #[test]
    fn test_write_after_rollback_returns_transaction_rolled_back() {
        let mut tx = Transaction::begin("testdb", IsolationLevel::ReadCommitted);
        tx.rollback().unwrap();
        let err = tx
            .add_operation(TxOperation::Delete {
                tree: "nodes".to_string(),
                key: b"k".to_vec(),
            })
            .unwrap_err();
        assert!(matches!(err, TxError::TransactionRolledBack));
    }

    #[test]
    fn test_write_after_commit_returns_transaction_closed() {
        let mut tx = Transaction::begin("testdb", IsolationLevel::ReadCommitted);
        tx.commit().unwrap();
        let err = tx
            .add_operation(TxOperation::Put {
                tree: "nodes".to_string(),
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            })
            .unwrap_err();
        assert!(matches!(err, TxError::TransactionClosed));
    }

    #[test]
    fn test_write_on_read_transaction_returns_read_only_error() {
        let config = SessionConfig {
            mode: TransactionMode::Read,
            ..SessionConfig::default()
        };
        let mut tx = Transaction::new(&config);
        let err = tx
            .add_operation(TxOperation::Put {
                tree: "nodes".to_string(),
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            })
            .unwrap_err();
        assert!(matches!(err, TxError::TransactionReadOnly));
    }

    #[test]
    fn session_manager_opens_owner_bound_sessions() {
        let manager = SessionManager::new(Duration::from_secs(30));
        let config = SessionConfig {
            database: Some("copper".to_string()),
            ..SessionConfig::default()
        };

        let session_id = manager
            .open_for_owner(&config, Some("  user:alice  "))
            .unwrap();

        assert!(
            manager
                .get_for_owner(&session_id, Some("user:alice"))
                .is_some()
        );
        assert!(
            manager
                .get_for_owner(&session_id, Some("user:bob"))
                .is_none()
        );
        assert!(manager.get_for_owner(&session_id, None).is_none());
        assert_eq!(
            manager
                .get_for_owner(&session_id, Some("user:alice"))
                .unwrap()
                .database,
            Some("copper".to_string())
        );
    }

    #[test]
    fn session_manager_persists_normalized_bookmarks_and_read_fence() {
        let clock = Arc::new(DistributedTransactionClock::with_epoch(11, 5));
        let transactions = Arc::new(TransactionManager::with_clock(clock));
        let manager =
            SessionManager::with_transactions(Duration::from_secs(30), Arc::clone(&transactions));
        let bookmark = LogicalTransactionId::new(7, 41, 9).stable_id();
        let config = SessionConfig {
            database: Some("copper".to_string()),
            bookmarks: vec![format!("  {bookmark}  ")],
            bookmark_mode: BookmarkMode::Required,
            ..SessionConfig::default()
        };

        let session_id = manager.open(&config).unwrap();
        let session = manager.get(&session_id).unwrap();
        let transaction = transactions.get(&session.transaction_id).unwrap();

        assert_eq!(session.bookmarks, vec![bookmark]);
        assert_eq!(session.read_fence(), transaction.begin_timestamp);
        assert_eq!(
            manager.read_fence(&session_id).unwrap(),
            transaction.begin_timestamp
        );
    }

    #[test]
    fn session_manager_commit_deletes_session_and_transaction() {
        let transactions = Arc::new(TransactionManager::new());
        let manager =
            SessionManager::with_transactions(Duration::from_secs(30), Arc::clone(&transactions));
        let session_id = manager.open(&SessionConfig::default()).unwrap();
        let transaction_id = manager.get(&session_id).unwrap().transaction_id;

        manager.commit_and_delete(session_id).unwrap();

        assert!(manager.get(&session_id).is_none());
        assert!(!transactions.is_active(&transaction_id));
    }

    #[test]
    fn session_manager_commit_and_get_bookmark_returns_commit_bookmark() {
        let transactions = Arc::new(TransactionManager::new());
        let manager =
            SessionManager::with_transactions(Duration::from_secs(30), Arc::clone(&transactions));
        let session_id = manager.open(&SessionConfig::default()).unwrap();
        let transaction_id = manager.get(&session_id).unwrap().transaction_id;

        let bookmark = manager.commit_and_get_bookmark(session_id).unwrap();

        assert!(parse_bookmark(&bookmark).is_ok());
        assert!(manager.get(&session_id).is_none());
        assert!(!transactions.is_active(&transaction_id));
    }

    #[test]
    fn session_manager_rollback_after_terminal_error_succeeds() {
        let transactions = Arc::new(TransactionManager::new());
        let manager =
            SessionManager::with_transactions(Duration::from_secs(30), Arc::clone(&transactions));
        let session_id = manager.open(&SessionConfig::default()).unwrap();
        let transaction_id = manager.get(&session_id).unwrap().transaction_id;

        let terminal = manager
            .record_terminal_error(
                &session_id,
                TxError::Terminal("snapshot cancelled".to_string()),
            )
            .unwrap();
        assert_eq!(
            terminal,
            TxError::Terminal("snapshot cancelled".to_string())
        );

        manager.rollback_and_delete(session_id).unwrap();

        assert!(manager.get(&session_id).is_none());
        assert!(!transactions.is_active(&transaction_id));
    }

    #[test]
    fn session_manager_commit_replays_terminal_error_and_deletes() {
        let manager = SessionManager::new(Duration::from_secs(30));
        let session_id = manager.open(&SessionConfig::default()).unwrap();
        manager
            .record_terminal_error(&session_id, TxError::Terminal("hard expired".to_string()))
            .unwrap();

        let err = manager.commit_and_delete(session_id).unwrap_err();

        assert_eq!(err, TxError::Terminal("hard expired".to_string()));
        assert!(manager.get(&session_id).is_none());
    }

    #[test]
    fn session_manager_cleanup_expired_removes_transaction() {
        let transactions = Arc::new(TransactionManager::new());
        let manager =
            SessionManager::with_transactions(Duration::from_millis(1), Arc::clone(&transactions));
        let session_id = manager.open(&SessionConfig::default()).unwrap();
        let transaction_id = manager.get(&session_id).unwrap().transaction_id;

        std::thread::sleep(Duration::from_millis(5));
        manager.cleanup_expired();

        assert!(manager.get(&session_id).is_none());
        assert!(!transactions.is_active(&transaction_id));
    }
}
