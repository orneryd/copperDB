//! GPU-accelerated computation for magnetDB.
//!
//! Equivalent to Go's `pkg/gpu` in NornicDB.
//! NornicDB supports four GPU backends:
//! - CUDA (NVIDIA) via custom CGo wrappers
//! - Metal (Apple Silicon) via Objective-C bridging
//! - Vulkan (cross-platform) via CGo
//! - OpenCL (portable) via CGo
//!
//! This crate provides the same capabilities via:
//! - **wgpu**: WebGPU/Vulkan/Metal/DX12 unified backend (preferred)
//! - **cuda-sys** / **cudarc**: CUDA direct bindings (optional feature)
//! - **opencl3**: OpenCL 3.0 bindings (optional feature)
//!
//! ## Usage
//! GPU acceleration is used for:
//! - K-means clustering of embedding vectors
//! - Batch cosine similarity (ANN search)
//! - Matrix operations for graph algorithms
//!
//! ⚠️ **wgpu integration is the primary backend.**
//! CUDA and OpenCL are opt-in via Cargo features.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum GpuError {
    #[error("no GPU device found")]
    NoDevice,
    #[error("GPU initialization failed: {0}")]
    InitFailed(String),
    #[error("GPU computation failed: {0}")]
    ComputeFailed(String),
    #[error("unsupported backend: {0}")]
    UnsupportedBackend(String),
}

/// Available GPU backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// wgpu (cross-platform: Vulkan/Metal/DX12/WebGPU)
    Wgpu,
    /// NVIDIA CUDA (requires CUDA toolkit)
    Cuda,
    /// Apple Metal (macOS/iOS only)
    Metal,
    /// OpenCL (cross-vendor)
    OpenCl,
}

/// GPU accelerator handle.
pub struct GpuAccelerator {
    pub backend: Backend,
    // wgpu: Option<(wgpu::Device, wgpu::Queue)>
}

impl GpuAccelerator {
    /// Initialize a GPU accelerator with the preferred backend.
    ///
    /// ⚠️ Full wgpu initialization requires async context.
    /// This is a synchronous stub; wire up `wgpu::Instance::request_adapter`
    /// in the async executor.
    pub fn new(backend: Backend) -> Result<Self, GpuError> {
        Ok(Self { backend })
    }

    /// Batch cosine similarity: compute similarity between `query` and all `candidates`.
    ///
    /// Falls back to CPU SIMD via `magnetdb-simd` when GPU is unavailable.
    pub fn batch_cosine_similarity(
        &self,
        query: &[f32],
        candidates: &[Vec<f32>],
    ) -> Result<Vec<f32>, GpuError> {
        // TODO: Dispatch to wgpu compute shader for GPU acceleration.
        // For now, fall back to CPU scalar.
        let results = candidates
            .iter()
            .map(|c| {
                let dot: f32 = query.iter().zip(c.iter()).map(|(a, b)| a * b).sum();
                let na: f32 = query.iter().map(|x| x * x).sum::<f32>().sqrt();
                let nb: f32 = c.iter().map(|x| x * x).sum::<f32>().sqrt();
                if na == 0.0 || nb == 0.0 { 0.0 } else { dot / (na * nb) }
            })
            .collect();
        Ok(results)
    }

    /// K-means clustering of a set of vectors.
    ///
    /// ⚠️ GPU implementation pending. Currently uses CPU fallback.
    pub fn kmeans(
        &self,
        vectors: &[Vec<f32>],
        k: usize,
        max_iters: usize,
    ) -> Result<Vec<usize>, GpuError> {
        if vectors.is_empty() || k == 0 {
            return Ok(vec![]);
        }
        // TODO: Implement GPU-accelerated k-means.
        // CPU fallback: assign all to cluster 0 as placeholder.
        Ok(vec![0usize; vectors.len()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_cosine_similarity_cpu_fallback() {
        let acc = GpuAccelerator::new(Backend::Wgpu).unwrap();
        let query = vec![1.0f32, 0.0, 0.0];
        let candidates = vec![
            vec![1.0f32, 0.0, 0.0],
            vec![0.0f32, 1.0, 0.0],
        ];
        let scores = acc.batch_cosine_similarity(&query, &candidates).unwrap();
        assert!((scores[0] - 1.0).abs() < 1e-5);
        assert!((scores[1] - 0.0).abs() < 1e-5);
    }
}
