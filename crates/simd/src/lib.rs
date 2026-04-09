//! SIMD-accelerated vector operations for magnetDB.
//!
//! Equivalent to Go's `pkg/simd` in NornicDB.
//! NornicDB uses custom C++ NEON (ARM) and x86 AVX2 implementations via
//! `github.com/ebitengine/purego` (CGo-free FFI).
//!
//! This crate provides:
//! - Dot product (cosine similarity inner loop)
//! - L2 distance computation
//! - Vector normalization
//!
//! Uses `wide` for stable cross-platform SIMD (x86 SSE2/AVX2 and ARM NEON
//! via portable SIMD abstractions).

use thiserror::Error;
use wide::f32x8;

#[derive(Debug, Error)]
pub enum SimdError {
    #[error("vector dimension mismatch: {a} vs {b}")]
    DimensionMismatch { a: usize, b: usize },
}

/// Compute dot product using SIMD f32x8 lanes.
///
/// Falls back to scalar for trailing elements when length is not a multiple of 8.
pub fn dot_f32(a: &[f32], b: &[f32]) -> Result<f32, SimdError> {
    if a.len() != b.len() {
        return Err(SimdError::DimensionMismatch {
            a: a.len(),
            b: b.len(),
        });
    }

    let chunks = a.len() / 8;
    let remainder = a.len() % 8;
    let mut acc = f32x8::ZERO;

    for i in 0..chunks {
        let base = i * 8;
        let va = f32x8::new([a[base], a[base+1], a[base+2], a[base+3], a[base+4], a[base+5], a[base+6], a[base+7]]);
        let vb = f32x8::new([b[base], b[base+1], b[base+2], b[base+3], b[base+4], b[base+5], b[base+6], b[base+7]]);
        acc += va * vb;
    }

    let simd_sum: f32 = acc.to_array().iter().sum();

    // Scalar remainder
    let scalar_sum: f32 = a[a.len() - remainder..]
        .iter()
        .zip(b[b.len() - remainder..].iter())
        .map(|(x, y)| x * y)
        .sum();

    Ok(simd_sum + scalar_sum)
}

/// Compute squared L2 distance using SIMD.
pub fn l2_distance_sq_f32(a: &[f32], b: &[f32]) -> Result<f32, SimdError> {
    if a.len() != b.len() {
        return Err(SimdError::DimensionMismatch {
            a: a.len(),
            b: b.len(),
        });
    }

    let chunks = a.len() / 8;
    let remainder = a.len() % 8;
    let mut acc = f32x8::ZERO;

    for i in 0..chunks {
        let base = i * 8;
        let va = f32x8::new([a[base], a[base+1], a[base+2], a[base+3], a[base+4], a[base+5], a[base+6], a[base+7]]);
        let vb = f32x8::new([b[base], b[base+1], b[base+2], b[base+3], b[base+4], b[base+5], b[base+6], b[base+7]]);
        let diff = va - vb;
        acc += diff * diff;
    }

    let simd_sum: f32 = acc.to_array().iter().sum();
    let scalar_sum: f32 = a[a.len() - remainder..]
        .iter()
        .zip(b[b.len() - remainder..].iter())
        .map(|(x, y)| (x - y) * (x - y))
        .sum();

    Ok(simd_sum + scalar_sum)
}

/// L2 distance.
pub fn l2_distance_f32(a: &[f32], b: &[f32]) -> Result<f32, SimdError> {
    Ok(l2_distance_sq_f32(a, b)?.sqrt())
}

/// Cosine similarity.
pub fn cosine_similarity_f32(a: &[f32], b: &[f32]) -> Result<f32, SimdError> {
    let dot = dot_f32(a, b)?;
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        return Ok(0.0);
    }
    Ok(dot / (norm_a * norm_b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dot_product() {
        let a = vec![1.0f32; 16];
        let b = vec![1.0f32; 16];
        assert!((dot_f32(&a, &b).unwrap() - 16.0).abs() < 1e-5);
    }

    #[test]
    fn test_l2_distance_identical_vectors() {
        let a = vec![1.0f32; 8];
        let b = vec![1.0f32; 8];
        assert!((l2_distance_f32(&a, &b).unwrap() - 0.0).abs() < 1e-5);
    }

    #[test]
    fn test_cosine_similarity_orthogonal() {
        let a = vec![1.0f32, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        let b = vec![0.0f32, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        assert!((cosine_similarity_f32(&a, &b).unwrap() - 0.0).abs() < 1e-5);
    }

    #[test]
    fn test_dimension_mismatch() {
        assert!(dot_f32(&[1.0f32], &[1.0f32, 2.0f32]).is_err());
    }
}
