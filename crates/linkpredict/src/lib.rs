//! Deterministic graph link prediction for CopperDB.

mod graph;
mod hybrid;
mod topology;

pub use graph::{AdjacencyStream, GraphBuildConfig, GraphBuildStats, GraphSnapshot};
pub use hybrid::{HybridConfig, HybridPrediction, HybridScorer, SemanticScorer, TopologyAlgorithm};
pub use topology::{
    adamic_adar, common_neighbors, jaccard, preferential_attachment, resource_allocation,
    Prediction,
};

use thiserror::Error;

/// Computes cosine similarity with the same high-precision accumulation as NornicDB.
pub fn cosine_similarity(left: &[f32], right: &[f32]) -> f64 {
    if left.len() != right.len() || left.is_empty() {
        return 0.0;
    }

    let (mut dot_product, mut left_norm, mut right_norm) = (0.0, 0.0, 0.0);
    for (&left_value, &right_value) in left.iter().zip(right) {
        dot_product += (left_value * right_value) as f64;
        left_norm += (left_value * left_value) as f64;
        right_norm += (right_value * right_value) as f64;
    }
    if left_norm == 0.0 || right_norm == 0.0 {
        return 0.0;
    }

    dot_product / (left_norm.sqrt() * right_norm.sqrt())
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LinkPredictError {
    #[error("node not found: {0}")]
    NodeNotFound(String),
    #[error("adjacency stream failed: {0}")]
    Adjacency(String),
    #[error("request cancelled")]
    RequestCancelled,
}

#[cfg(test)]
mod tests {
    use super::cosine_similarity;

    #[test]
    fn cosine_similarity_matches_upstream_edge_contracts() {
        let vector = [1.0, 2.0, 3.0];
        assert!((cosine_similarity(&vector, &vector) - 1.0).abs() < 1e-12);
        assert_eq!(cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]), 0.0);
        assert!((cosine_similarity(&vector, &[-1.0, -2.0, -3.0]) + 1.0).abs() < 1e-12);
        assert_eq!(cosine_similarity(&[1.0], &[1.0, 2.0]), 0.0);
        assert_eq!(cosine_similarity(&[], &[]), 0.0);
        assert_eq!(cosine_similarity(&vector, &[0.0, 0.0, 0.0]), 0.0);
    }
}
