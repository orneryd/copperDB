//! Result set filtering and predicate evaluation.
//!
//! Equivalent to Go's `pkg/filter` in NornicDB.
//! Applies WHERE clause predicates and result projections to query output rows.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum FilterError {
    #[error("predicate evaluation error: {0}")]
    PredicateError(String),
}

/// Represents a predicate that can be applied to a result row.
pub trait Predicate: Send + Sync {
    fn evaluate(&self, row: &std::collections::HashMap<String, serde_json::Value>) -> Result<bool, FilterError>;
}

/// Filter a list of rows using a predicate.
pub fn filter_rows<P: Predicate>(
    rows: Vec<std::collections::HashMap<String, serde_json::Value>>,
    predicate: &P,
) -> Result<Vec<std::collections::HashMap<String, serde_json::Value>>, FilterError> {
    rows.into_iter()
        .filter_map(|row| match predicate.evaluate(&row) {
            Ok(true) => Some(Ok(row)),
            Ok(false) => None,
            Err(e) => Some(Err(e)),
        })
        .collect()
}

/// A predicate that checks for key equality.
pub struct EqPredicate {
    pub key: String,
    pub value: serde_json::Value,
}

impl Predicate for EqPredicate {
    fn evaluate(&self, row: &std::collections::HashMap<String, serde_json::Value>) -> Result<bool, FilterError> {
        Ok(row.get(&self.key).map(|v| v == &self.value).unwrap_or(false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_eq_predicate() {
        let pred = EqPredicate {
            key: "name".to_string(),
            value: serde_json::json!("Alice"),
        };
        let mut row = HashMap::new();
        row.insert("name".to_string(), serde_json::json!("Alice"));
        assert!(pred.evaluate(&row).unwrap());
        let mut row2 = HashMap::new();
        row2.insert("name".to_string(), serde_json::json!("Bob"));
        assert!(!pred.evaluate(&row2).unwrap());
    }
}
