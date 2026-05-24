//! Vector space registry for copperdb.
//!
//! Equivalent to Go's `pkg/vectorspace` in NornicDB.
//! Manages named embedding spaces (collections of high-dimensional vectors)
//! and supports similarity search via HNSW indexing.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum VectorSpaceError {
    #[error("space not found: {0}")]
    SpaceNotFound(String),
    #[error("dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },
    #[error("vector not found: {0}")]
    VectorNotFound(String),
}

/// A named vector space (collection of embeddings).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSpace {
    pub name: String,
    pub dimensions: usize,
    pub metric: SimilarityMetric,
    entries: HashMap<String, Vec<f32>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SimilarityMetric {
    Cosine,
    Euclidean,
    DotProduct,
}

impl VectorSpace {
    pub fn new(name: impl Into<String>, dimensions: usize, metric: SimilarityMetric) -> Self {
        Self {
            name: name.into(),
            dimensions,
            metric,
            entries: HashMap::new(),
        }
    }

    /// Insert a vector with the given ID.
    pub fn insert(
        &mut self,
        id: impl Into<String>,
        vector: Vec<f32>,
    ) -> Result<(), VectorSpaceError> {
        if vector.len() != self.dimensions {
            return Err(VectorSpaceError::DimensionMismatch {
                expected: self.dimensions,
                got: vector.len(),
            });
        }
        self.entries.insert(id.into(), vector);
        Ok(())
    }

    /// Find the k nearest neighbors to the query vector (brute-force).
    ///
    /// ⚠️ For production use, replace with HNSW indexing.
    pub fn knn(&self, query: &[f32], k: usize) -> Result<Vec<(String, f32)>, VectorSpaceError> {
        if query.len() != self.dimensions {
            return Err(VectorSpaceError::DimensionMismatch {
                expected: self.dimensions,
                got: query.len(),
            });
        }
        let mut scores: Vec<(String, f32)> = self
            .entries
            .iter()
            .map(|(id, v)| {
                let score = match self.metric {
                    SimilarityMetric::Cosine => cosine(query, v),
                    SimilarityMetric::Euclidean => -euclidean(query, v),
                    SimilarityMetric::DotProduct => dot(query, v),
                };
                (id.clone(), score)
            })
            .collect();
        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores.truncate(k);
        Ok(scores)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let d = dot(a, b);
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        d / (na * nb)
    }
}

fn euclidean(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y) * (x - y))
        .sum::<f32>()
        .sqrt()
}

/// Global registry of vector spaces.
#[derive(Default)]
pub struct Registry {
    spaces: HashMap<String, VectorSpace>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_space(&mut self, space: VectorSpace) {
        self.spaces.insert(space.name.clone(), space);
    }

    pub fn get_space(&self, name: &str) -> Option<&VectorSpace> {
        self.spaces.get(name)
    }

    pub fn get_space_mut(&mut self, name: &str) -> Option<&mut VectorSpace> {
        self.spaces.get_mut(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_knn() {
        let mut space = VectorSpace::new("test", 4, SimilarityMetric::Cosine);
        space.insert("a", vec![1.0, 0.0, 0.0, 0.0]).unwrap();
        space.insert("b", vec![0.0, 1.0, 0.0, 0.0]).unwrap();
        let results = space.knn(&[1.0, 0.0, 0.0, 0.0], 2).unwrap();
        assert_eq!(results[0].0, "a");
        assert!((results[0].1 - 1.0).abs() < 1e-5);
    }

    #[test]
    fn test_dimension_mismatch() {
        let mut space = VectorSpace::new("test", 4, SimilarityMetric::Cosine);
        assert!(space.insert("bad", vec![1.0, 2.0]).is_err());
    }
}
