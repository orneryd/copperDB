//! SIMD-accelerated vector operations for copperdb.
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

use std::sync::OnceLock;

use thiserror::Error;
use wide::f32x8;

type DotF32Kernel = fn(&[f32], &[f32]) -> f32;

static DOT_F32_KERNEL: OnceLock<DotF32Kernel> = OnceLock::new();

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

    Ok(DOT_F32_KERNEL.get_or_init(select_dot_f32_kernel)(a, b))
}

fn select_dot_f32_kernel() -> DotF32Kernel {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
        return |a, b| {
            // SAFETY: this kernel is selected only after detecting AVX2 and FMA support.
            unsafe { dot_f32_avx2_fma(a, b) }
        };
    }
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    if is_x86_feature_detected!("avx2") {
        return |a, b| {
            // SAFETY: this kernel is selected only after detecting AVX2 support.
            unsafe { dot_f32_avx2(a, b) }
        };
    }

    dot_f32_portable
}

fn dot_f32_portable(a: &[f32], b: &[f32]) -> f32 {
    let chunks = a.len() / 8;
    let remainder = a.len() % 8;
    let mut acc = f32x8::ZERO;

    for i in 0..chunks {
        let base = i * 8;
        let va = f32x8::new([
            a[base],
            a[base + 1],
            a[base + 2],
            a[base + 3],
            a[base + 4],
            a[base + 5],
            a[base + 6],
            a[base + 7],
        ]);
        let vb = f32x8::new([
            b[base],
            b[base + 1],
            b[base + 2],
            b[base + 3],
            b[base + 4],
            b[base + 5],
            b[base + 6],
            b[base + 7],
        ]);
        acc += va * vb;
    }

    let simd_sum: f32 = acc.to_array().iter().sum();

    // Scalar remainder
    let scalar_sum: f32 = a[a.len() - remainder..]
        .iter()
        .zip(b[b.len() - remainder..].iter())
        .map(|(x, y)| x * y)
        .sum();

    simd_sum + scalar_sum
}

#[cfg(target_arch = "x86")]
use std::arch::x86::{
    __m256, _mm_add_ps, _mm_add_ss, _mm_cvtss_f32, _mm_movehdup_ps, _mm_movehl_ps, _mm256_add_ps,
    _mm256_castps256_ps128, _mm256_extractf128_ps, _mm256_fmadd_ps, _mm256_loadu_ps, _mm256_mul_ps,
    _mm256_setzero_ps,
};
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::{
    __m256, _mm_add_ps, _mm_add_ss, _mm_cvtss_f32, _mm_movehdup_ps, _mm_movehl_ps, _mm256_add_ps,
    _mm256_castps256_ps128, _mm256_extractf128_ps, _mm256_fmadd_ps, _mm256_loadu_ps, _mm256_mul_ps,
    _mm256_setzero_ps,
};

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx")]
unsafe fn horizontal_sum_f32x8(value: __m256) -> f32 {
    let low = _mm256_castps256_ps128(value);
    let high = _mm256_extractf128_ps(value, 1);
    let sum = _mm_add_ps(low, high);
    let pairs = _mm_add_ps(sum, _mm_movehdup_ps(sum));
    _mm_cvtss_f32(_mm_add_ss(pairs, _mm_movehl_ps(pairs, pairs)))
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2,fma")]
unsafe fn dot_f32_avx2_fma(a: &[f32], b: &[f32]) -> f32 {
    unsafe {
        let unrolled_len = a.len() / 32 * 32;
        let vectorized_len = a.len() / 8 * 8;
        let mut acc0 = _mm256_setzero_ps();
        let mut acc1 = _mm256_setzero_ps();
        let mut acc2 = _mm256_setzero_ps();
        let mut acc3 = _mm256_setzero_ps();
        let mut index = 0;
        while index < unrolled_len {
            let left = _mm256_loadu_ps(a.as_ptr().add(index));
            let right = _mm256_loadu_ps(b.as_ptr().add(index));
            acc0 = _mm256_fmadd_ps(left, right, acc0);
            let left = _mm256_loadu_ps(a.as_ptr().add(index + 8));
            let right = _mm256_loadu_ps(b.as_ptr().add(index + 8));
            acc1 = _mm256_fmadd_ps(left, right, acc1);
            let left = _mm256_loadu_ps(a.as_ptr().add(index + 16));
            let right = _mm256_loadu_ps(b.as_ptr().add(index + 16));
            acc2 = _mm256_fmadd_ps(left, right, acc2);
            let left = _mm256_loadu_ps(a.as_ptr().add(index + 24));
            let right = _mm256_loadu_ps(b.as_ptr().add(index + 24));
            acc3 = _mm256_fmadd_ps(left, right, acc3);
            index += 32;
        }
        acc0 = _mm256_add_ps(acc0, acc1);
        acc2 = _mm256_add_ps(acc2, acc3);
        acc0 = _mm256_add_ps(acc0, acc2);
        while index < vectorized_len {
            let left = _mm256_loadu_ps(a.as_ptr().add(index));
            let right = _mm256_loadu_ps(b.as_ptr().add(index));
            acc0 = _mm256_fmadd_ps(left, right, acc0);
            index += 8;
        }

        horizontal_sum_f32x8(acc0)
            + a[vectorized_len..]
                .iter()
                .zip(&b[vectorized_len..])
                .map(|(left, right)| left * right)
                .sum::<f32>()
    }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn dot_f32_avx2(a: &[f32], b: &[f32]) -> f32 {
    unsafe {
        let unrolled_len = a.len() / 32 * 32;
        let vectorized_len = a.len() / 8 * 8;
        let mut acc0 = _mm256_setzero_ps();
        let mut acc1 = _mm256_setzero_ps();
        let mut acc2 = _mm256_setzero_ps();
        let mut acc3 = _mm256_setzero_ps();
        let mut index = 0;
        while index < unrolled_len {
            let left = _mm256_loadu_ps(a.as_ptr().add(index));
            let right = _mm256_loadu_ps(b.as_ptr().add(index));
            acc0 = _mm256_add_ps(acc0, _mm256_mul_ps(left, right));
            let left = _mm256_loadu_ps(a.as_ptr().add(index + 8));
            let right = _mm256_loadu_ps(b.as_ptr().add(index + 8));
            acc1 = _mm256_add_ps(acc1, _mm256_mul_ps(left, right));
            let left = _mm256_loadu_ps(a.as_ptr().add(index + 16));
            let right = _mm256_loadu_ps(b.as_ptr().add(index + 16));
            acc2 = _mm256_add_ps(acc2, _mm256_mul_ps(left, right));
            let left = _mm256_loadu_ps(a.as_ptr().add(index + 24));
            let right = _mm256_loadu_ps(b.as_ptr().add(index + 24));
            acc3 = _mm256_add_ps(acc3, _mm256_mul_ps(left, right));
            index += 32;
        }
        acc0 = _mm256_add_ps(acc0, acc1);
        acc2 = _mm256_add_ps(acc2, acc3);
        acc0 = _mm256_add_ps(acc0, acc2);
        while index < vectorized_len {
            let left = _mm256_loadu_ps(a.as_ptr().add(index));
            let right = _mm256_loadu_ps(b.as_ptr().add(index));
            acc0 = _mm256_add_ps(acc0, _mm256_mul_ps(left, right));
            index += 8;
        }

        horizontal_sum_f32x8(acc0)
            + a[vectorized_len..]
                .iter()
                .zip(&b[vectorized_len..])
                .map(|(left, right)| left * right)
                .sum::<f32>()
    }
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
        let va = f32x8::new([
            a[base],
            a[base + 1],
            a[base + 2],
            a[base + 3],
            a[base + 4],
            a[base + 5],
            a[base + 6],
            a[base + 7],
        ]);
        let vb = f32x8::new([
            b[base],
            b[base + 1],
            b[base + 2],
            b[base + 3],
            b[base + 4],
            b[base + 5],
            b[base + 6],
            b[base + 7],
        ]);
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
    fn test_dot_product_with_scalar_tail() {
        let a = (0..19).map(|value| value as f32 * 0.25).collect::<Vec<_>>();
        let b = (0..19)
            .map(|value| (value as f32 - 7.0) * 0.5)
            .collect::<Vec<_>>();
        let expected = a
            .iter()
            .zip(&b)
            .map(|(left, right)| left * right)
            .sum::<f32>();
        assert!((dot_f32(&a, &b).unwrap() - expected).abs() < 1e-4);
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
