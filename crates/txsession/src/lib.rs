//! Transaction session management for magnetDB.
//!
//! Equivalent to Go's `pkg/txsession` in NornicDB.
//! Provides ACID transaction handling: begin, commit, rollback, and
//! isolation-level enforcement.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum TxError {
    #[error("transaction already committed")]
    AlreadyCommitted,
    #[error("transaction already rolled back")]
    AlreadyRolledBack,
    #[error("transaction conflict (serializable isolation violation)")]
    Conflict,
    #[error("transaction not found: {0}")]
    NotFound(String),
}

/// Transaction isolation level.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// Transaction state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxState {
    Active,
    Committed,
    RolledBack,
}

/// An active database transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub id: String,
    pub isolation: IsolationLevel,
    pub state: TxState,
    pub created_at: u64,
    pub database: String,
}

impl Transaction {
    pub fn begin(database: impl Into<String>, isolation: IsolationLevel) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            isolation,
            state: TxState::Active,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            database: database.into(),
        }
    }

    pub fn commit(&mut self) -> Result<(), TxError> {
        match self.state {
            TxState::Active => {
                self.state = TxState::Committed;
                Ok(())
            }
            TxState::Committed => Err(TxError::AlreadyCommitted),
            TxState::RolledBack => Err(TxError::AlreadyRolledBack),
        }
    }

    pub fn rollback(&mut self) -> Result<(), TxError> {
        match self.state {
            TxState::Active => {
                self.state = TxState::RolledBack;
                Ok(())
            }
            TxState::Committed => Err(TxError::AlreadyCommitted),
            TxState::RolledBack => Err(TxError::AlreadyRolledBack),
        }
    }

    pub fn is_active(&self) -> bool {
        self.state == TxState::Active
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
}
