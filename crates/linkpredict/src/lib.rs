//! Graph link prediction algorithms for copperdb.
//!
//! Equivalent to Go's `pkg/linkpredict` in NornicDB.
//! Implements classic graph-based link prediction heuristics:
//! - Common Neighbors
//! - Jaccard Coefficient
//! - Adamic-Adar Index
//! - Preferential Attachment

use thiserror::Error;
use std::collections::HashSet;

#[derive(Debug, Error)]
pub enum LinkPredictError {
    #[error("node not found: {0}")]
    NodeNotFound(String),
}

/// Compute Common Neighbors score between two node neighbor sets.
pub fn common_neighbors(a: &HashSet<String>, b: &HashSet<String>) -> usize {
    a.intersection(b).count()
}

/// Compute Jaccard Coefficient.
pub fn jaccard(a: &HashSet<String>, b: &HashSet<String>) -> f64 {
    let union = a.union(b).count();
    if union == 0 { return 0.0; }
    a.intersection(b).count() as f64 / union as f64
}

/// Compute Preferential Attachment score.
pub fn preferential_attachment(a: &HashSet<String>, b: &HashSet<String>) -> usize {
    a.len() * b.len()
}

/// Compute Adamic-Adar score given a degree map for common neighbors.
pub fn adamic_adar(
    a: &HashSet<String>,
    b: &HashSet<String>,
    degree: &std::collections::HashMap<String, usize>,
) -> f64 {
    a.intersection(b)
        .filter_map(|n| degree.get(n))
        .filter(|&&d| d > 1)
        .map(|&d| 1.0 / (d as f64).ln())
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_common_neighbors() {
        let a: HashSet<String> = vec!["c", "d", "e"].into_iter().map(String::from).collect();
        let b: HashSet<String> = vec!["c", "d", "f"].into_iter().map(String::from).collect();
        assert_eq!(common_neighbors(&a, &b), 2);
    }

    #[test]
    fn test_jaccard() {
        let a: HashSet<String> = vec!["a", "b", "c"].into_iter().map(String::from).collect();
        let b: HashSet<String> = vec!["b", "c", "d"].into_iter().map(String::from).collect();
        // intersection = {b,c} = 2, union = {a,b,c,d} = 4
        assert!((jaccard(&a, &b) - 0.5).abs() < 1e-9);
    }
}
