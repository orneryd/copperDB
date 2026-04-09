//! Cypher query evaluator for magnetDB.
//!
//! Equivalent to Go's `pkg/eval` in NornicDB.
//! Executes Cypher ASTs from `magnetdb-cypher` against the storage engine,
//! applying filtering, projection, and aggregation.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum EvalError {
    #[error("execution error: {0}")]
    ExecutionError(String),
    #[error("type error: {0}")]
    TypeError(String),
    #[error("storage error: {0}")]
    StorageError(String),
    #[error("plan not available — parser not yet implemented")]
    ParserNotImplemented,
}

/// An evaluated result row.
pub type Row = std::collections::HashMap<String, serde_json::Value>;

/// The query executor. Takes a parsed AST and returns result rows.
///
/// ⚠️ **Not yet implemented.** Requires the Cypher parser (`magnetdb-cypher`)
/// to be complete before the evaluator can be wired up.
pub struct Executor {
    // storage: Arc<StorageEngine>
}

impl Executor {
    pub fn new() -> Self {
        Self {}
    }

    /// Execute a Cypher query string and return result rows.
    pub fn execute(
        &self,
        _query: &str,
        _params: std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Row>, EvalError> {
        Err(EvalError::ParserNotImplemented)
    }
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_returns_not_implemented() {
        let exec = Executor::new();
        let result = exec.execute("MATCH (n) RETURN n", Default::default());
        assert!(matches!(result, Err(EvalError::ParserNotImplemented)));
    }
}
