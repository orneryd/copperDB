//! Mathematical utilities for magnetDB.
//!
//! Equivalent to Go's `pkg/math` in NornicDB.
//! Provides vector math used in SIMD acceleration, embeddings, and
//! link-prediction scoring.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum MathError {
    #[error("dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },
    #[error("empty vector")]
    EmptyVector,
    #[error("numerical overflow or NaN")]
    Overflow,
}

/// Compute the dot product of two equal-length slices.
pub fn dot(a: &[f32], b: &[f32]) -> Result<f32, MathError> {
    if a.len() != b.len() {
        return Err(MathError::DimensionMismatch {
            expected: a.len(),
            got: b.len(),
        });
    }
    Ok(a.iter().zip(b.iter()).map(|(x, y)| x * y).sum())
}

/// Compute the L2 (Euclidean) norm of a vector.
pub fn l2_norm(v: &[f32]) -> f32 {
    v.iter().map(|x| x * x).sum::<f32>().sqrt()
}

/// Normalize a vector to unit length (in-place).
pub fn normalize(v: &mut [f32]) -> Result<(), MathError> {
    let norm = l2_norm(v);
    if norm == 0.0 {
        return Err(MathError::EmptyVector);
    }
    for x in v.iter_mut() {
        *x /= norm;
    }
    Ok(())
}

/// Compute cosine similarity between two vectors.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> Result<f32, MathError> {
    let d = dot(a, b)?;
    let norm_a = l2_norm(a);
    let norm_b = l2_norm(b);
    if norm_a == 0.0 || norm_b == 0.0 {
        return Err(MathError::EmptyVector);
    }
    Ok(d / (norm_a * norm_b))
}

/// Compute the Euclidean distance between two vectors.
pub fn euclidean_distance(a: &[f32], b: &[f32]) -> Result<f32, MathError> {
    if a.len() != b.len() {
        return Err(MathError::DimensionMismatch {
            expected: a.len(),
            got: b.len(),
        });
    }
    let sq_sum: f32 = a.iter().zip(b.iter()).map(|(x, y)| (x - y) * (x - y)).sum();
    Ok(sq_sum.sqrt())
}

/// Arithmetic mean of a slice.
pub fn mean(v: &[f32]) -> Result<f32, MathError> {
    if v.is_empty() {
        return Err(MathError::EmptyVector);
    }
    Ok(v.iter().sum::<f32>() / v.len() as f32)
}

/// Softmax over a slice (numerically stable).
pub fn softmax(v: &[f32]) -> Vec<f32> {
    let max = v.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
    let exps: Vec<f32> = v.iter().map(|x| (x - max).exp()).collect();
    let sum: f32 = exps.iter().sum();
    exps.iter().map(|x| x / sum).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dot_product() {
        let a = [1.0f32, 2.0, 3.0];
        let b = [4.0f32, 5.0, 6.0];
        assert!((dot(&a, &b).unwrap() - 32.0).abs() < 1e-6);
    }

    #[test]
    fn test_l2_norm() {
        let v = [3.0f32, 4.0];
        assert!((l2_norm(&v) - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_parallel() {
        let a = [1.0f32, 0.0, 0.0];
        let b = [1.0f32, 0.0, 0.0];
        assert!((cosine_similarity(&a, &b).unwrap() - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_softmax_sums_to_one() {
        let v = [1.0f32, 2.0, 3.0];
        let s = softmax(&v);
        assert!((s.iter().sum::<f32>() - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_dimension_mismatch() {
        assert!(matches!(
            dot(&[1.0f32], &[1.0f32, 2.0f32]),
            Err(MathError::DimensionMismatch { .. })
        ));
    }
}
