//! Embedding utility functions for copperdb.
//!
//! Equivalent to Go's `pkg/embeddingutil` in NornicDB.
//! Helpers for normalizing, comparing, and batching embeddings.

pub use copperdb_math::{cosine_similarity, dot, l2_norm, normalize, MathError};

/// Batch-normalize a collection of embedding vectors in place.
pub fn normalize_batch(embeddings: &mut [Vec<f32>]) -> Result<(), MathError> {
    for v in embeddings.iter_mut() {
        normalize(v)?;
    }
    Ok(())
}

/// Find the index of the most similar vector in `candidates` to `query`.
pub fn nearest(query: &[f32], candidates: &[Vec<f32>]) -> Option<usize> {
    candidates
        .iter()
        .enumerate()
        .map(|(i, c)| (i, cosine_similarity(query, c).unwrap_or(f32::NEG_INFINITY)))
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(i, _)| i)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nearest() {
        let query = vec![1.0f32, 0.0, 0.0];
        let candidates = vec![vec![0.0f32, 1.0, 0.0], vec![1.0f32, 0.0, 0.0]];
        assert_eq!(nearest(&query, &candidates), Some(1));
    }
}
