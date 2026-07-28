//! Shared error classification contracts.
//!
//! This ports NornicDB's Neo4j-compatible transient transaction mapping into
//! Rust. The crate is intentionally small so storage, query execution, Bolt,
//! HTTP, and transaction sessions can share retry semantics without depending on
//! protocol implementations.

use copperdb_storage::StorageError;
use thiserror::Error;

pub const TRANSIENT_DEADLOCK_DETECTED: &str = "Neo.TransientError.Transaction.DeadlockDetected";
pub const TRANSIENT_OUTDATED: &str = "Neo.TransientError.Transaction.Outdated";

#[derive(Debug, Error)]
pub enum CopperDbError {
    #[error("transaction deadlock")]
    TransactionDeadlock,
    #[error("transaction conflict")]
    TransactionConflict,
    #[error("mvcc resource pressure")]
    MvccResourcePressure,
    #[error("mvcc snapshot graceful cancel")]
    MvccSnapshotGracefulCancel,
    #[error("mvcc snapshot hard expired")]
    MvccSnapshotHardExpired,
    #[error("merge commit-time unique conflict: {0}")]
    MergeCommitTimeUniqueConflict(#[source] StorageError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransientTransactionCode {
    DeadlockDetected,
    Outdated,
}

impl TransientTransactionCode {
    pub fn as_neo4j_code(self) -> &'static str {
        match self {
            Self::DeadlockDetected => TRANSIENT_DEADLOCK_DETECTED,
            Self::Outdated => TRANSIENT_OUTDATED,
        }
    }
}

pub fn map_transient_transaction_error(
    err: &(dyn std::error::Error + 'static),
) -> Option<TransientTransactionCode> {
    let mut current = Some(err);
    while let Some(error) = current {
        if let Some(copperdb_error) = error.downcast_ref::<CopperDbError>() {
            return match copperdb_error {
                CopperDbError::TransactionDeadlock => {
                    Some(TransientTransactionCode::DeadlockDetected)
                }
                CopperDbError::TransactionConflict
                | CopperDbError::MvccResourcePressure
                | CopperDbError::MvccSnapshotGracefulCancel
                | CopperDbError::MvccSnapshotHardExpired
                | CopperDbError::MergeCommitTimeUniqueConflict(_) => {
                    Some(TransientTransactionCode::Outdated)
                }
                CopperDbError::Storage(storage_error) => map_storage_error(storage_error),
            };
        }
        if let Some(storage_error) = error.downcast_ref::<StorageError>() {
            return map_storage_error(storage_error);
        }
        current = error.source();
    }
    None
}

pub fn mark_merge_commit_time_unique_conflict(err: StorageError) -> CopperDbError {
    match err {
        unique @ StorageError::UniqueConstraintViolation { .. } => {
            CopperDbError::MergeCommitTimeUniqueConflict(unique)
        }
        other => CopperDbError::Storage(other),
    }
}

pub fn is_merge_commit_time_unique_conflict(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(err);
    while let Some(error) = current {
        if matches!(
            error.downcast_ref::<CopperDbError>(),
            Some(CopperDbError::MergeCommitTimeUniqueConflict(_))
        ) {
            return true;
        }
        current = error.source();
    }
    false
}

fn map_storage_error(err: &StorageError) -> Option<TransientTransactionCode> {
    match err {
        StorageError::TransactionConflict { .. } => Some(TransientTransactionCode::Outdated),
        StorageError::UniqueConstraintViolation { .. } => None,
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Error)]
    #[error("outer: {0}")]
    struct Outer(#[source] CopperDbError);

    fn unique_violation() -> StorageError {
        StorageError::UniqueConstraintViolation {
            label: "TerraformResource".into(),
            property: "uid".into(),
            value: "X".into(),
        }
    }

    #[test]
    fn maps_known_transient_transaction_errors() {
        assert_eq!(
            map_transient_transaction_error(&CopperDbError::TransactionDeadlock),
            Some(TransientTransactionCode::DeadlockDetected)
        );
        assert_eq!(
            map_transient_transaction_error(&Outer(CopperDbError::TransactionConflict)),
            Some(TransientTransactionCode::Outdated)
        );
        assert_eq!(
            map_transient_transaction_error(&CopperDbError::MvccSnapshotHardExpired),
            Some(TransientTransactionCode::Outdated)
        );
        assert_eq!(
            TransientTransactionCode::Outdated.as_neo4j_code(),
            TRANSIENT_OUTDATED
        );
    }

    #[test]
    fn maps_storage_snapshot_conflicts_as_outdated() {
        let err = StorageError::TransactionConflict {
            logical_key: "edge:e1".to_string(),
            snapshot_version: 4,
            current_version: 5,
        };

        assert_eq!(
            map_transient_transaction_error(&err),
            Some(TransientTransactionCode::Outdated)
        );
    }

    #[test]
    fn marks_only_retry_safe_merge_unique_conflicts() {
        let marked = mark_merge_commit_time_unique_conflict(unique_violation());
        assert!(is_merge_commit_time_unique_conflict(&marked));
        assert_eq!(
            map_transient_transaction_error(&marked),
            Some(TransientTransactionCode::Outdated)
        );

        let ordinary = CopperDbError::Storage(unique_violation());
        assert!(!is_merge_commit_time_unique_conflict(&ordinary));
        assert_eq!(map_transient_transaction_error(&ordinary), None);
    }
}
