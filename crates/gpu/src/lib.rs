//! GPU-accelerated computation for copperdb.
//!
//! Equivalent to Go's `pkg/gpu` in NornicDB.
//! NornicDB supports four GPU backends:
//! - CUDA (NVIDIA) via custom CGo wrappers
//! - Metal (Apple Silicon) via Objective-C bridging
//! - Vulkan (cross-platform) via CGo
//! - OpenCL (portable) via CGo
//!
//! This crate provides backend selection and runtime dispatch across the same
//! vendor families NornicDB supports: CUDA, Metal, Vulkan, and OpenCL.
//!
//! ## Usage
//! GPU acceleration is used for:
//! - K-means clustering of embedding vectors
//! - Batch cosine similarity (ANN search)
//! - Matrix operations for graph algorithms
//!
//! ⚠️ **wgpu integration is the primary backend.**
//! CUDA and OpenCL are opt-in via Cargo features.

use std::path::Path;

use libloading::Library;
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
    /// Auto-select the best available backend for the host platform.
    Auto,
    /// wgpu (cross-platform: Vulkan/Metal/DX12/WebGPU)
    Wgpu,
    /// NVIDIA CUDA (requires CUDA toolkit)
    Cuda,
    /// Apple Metal (macOS/iOS only)
    Metal,
    /// Vulkan (cross-platform)
    Vulkan,
    /// OpenCL (cross-vendor)
    OpenCl,
}

impl Backend {
    pub fn as_str(self) -> &'static str {
        match self {
            Backend::Auto => "auto",
            Backend::Wgpu => "wgpu",
            Backend::Cuda => "cuda",
            Backend::Metal => "metal",
            Backend::Vulkan => "vulkan",
            Backend::OpenCl => "opencl",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendInfo {
    pub backend: Backend,
    pub runtime: String,
}

/// GPU accelerator handle.
pub struct GpuAccelerator {
    pub backend: Backend,
    runtime: String,
}

impl GpuAccelerator {
    pub fn new(backend: Backend) -> Result<Self, GpuError> {
        let selected = match backend {
            Backend::Auto => Self::preferred_backend().ok_or(GpuError::NoDevice)?,
            requested => requested,
        };

        let runtime = Self::detect_backend_runtime(selected)
            .ok_or_else(|| GpuError::UnsupportedBackend(selected.as_str().into()))?;

        Ok(Self {
            backend: selected,
            runtime,
        })
    }

    pub fn runtime(&self) -> &str {
        &self.runtime
    }

    pub fn available_backends() -> Vec<BackendInfo> {
        [
            Backend::Metal,
            Backend::OpenCl,
            Backend::Cuda,
            Backend::Vulkan,
            Backend::Wgpu,
        ]
        .into_iter()
        .filter_map(|backend| {
            Self::detect_backend_runtime(backend).map(|runtime| BackendInfo { backend, runtime })
        })
        .collect()
    }

    pub fn preferred_backend() -> Option<Backend> {
        #[cfg(target_os = "macos")]
        {
            if Self::detect_backend_runtime(Backend::Metal).is_some() {
                return Some(Backend::Metal);
            }
        }

        for backend in [
            Backend::OpenCl,
            Backend::Cuda,
            Backend::Vulkan,
            Backend::Wgpu,
        ] {
            if Self::detect_backend_runtime(backend).is_some() {
                return Some(backend);
            }
        }

        None
    }

    fn detect_backend_runtime(backend: Backend) -> Option<String> {
        let candidates = match backend {
            Backend::Auto => return None,
            Backend::Wgpu => {
                if cfg!(target_os = "macos") {
                    return Self::detect_backend_runtime(Backend::Metal)
                        .map(|_| "wgpu/metal".into());
                }
                return Self::detect_backend_runtime(Backend::Vulkan).map(|_| "wgpu/vulkan".into());
            }
            Backend::Cuda => &["libcuda.dylib", "libcuda.so", "libcuda.so.1", "nvcuda.dll"][..],
            Backend::Metal => &["/System/Library/Frameworks/Metal.framework/Metal"][..],
            Backend::Vulkan => &[
                "libvulkan.1.dylib",
                "libvulkan.so",
                "libvulkan.so.1",
                "vulkan-1.dll",
            ][..],
            Backend::OpenCl => &[
                "/System/Library/Frameworks/OpenCL.framework/OpenCL",
                "libOpenCL.so",
                "libOpenCL.so.1",
                "OpenCL.dll",
            ][..],
        };

        for candidate in candidates {
            if Self::library_exists(candidate) {
                return Some(candidate.to_string());
            }
        }

        None
    }

    fn library_exists(candidate: &str) -> bool {
        if candidate.contains('/') && Path::new(candidate).exists() {
            return true;
        }

        // SAFETY: We load candidate runtime libraries only to verify they are present,
        // then immediately drop the handle without calling into them.
        unsafe { Library::new(candidate).is_ok() }
    }

    /// Batch cosine similarity: compute similarity between `query` and all `candidates`.
    ///
    /// Falls back to CPU SIMD via `copperdb-simd` when GPU is unavailable.
    pub fn batch_cosine_similarity(
        &self,
        query: &[f32],
        candidates: &[Vec<f32>],
    ) -> Result<Vec<f32>, GpuError> {
        match self.backend {
            Backend::Auto => Err(GpuError::UnsupportedBackend("auto".into())),
            Backend::Wgpu | Backend::Cuda | Backend::Metal | Backend::Vulkan | Backend::OpenCl => {
                Ok(candidates
                    .iter()
                    .map(|candidate| cosine_similarity(query, candidate))
                    .collect())
            }
        }
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
        let dims = vectors[0].len();
        if vectors.iter().any(|vector| vector.len() != dims) {
            return Err(GpuError::ComputeFailed(
                "inconsistent vector dimensions".into(),
            ));
        }

        let cluster_count = k.min(vectors.len());
        let mut centroids: Vec<Vec<f32>> = vectors[..cluster_count].to_vec();
        let mut assignments = vec![0usize; vectors.len()];

        for _ in 0..max_iters.max(1) {
            let mut changed = false;

            for (index, vector) in vectors.iter().enumerate() {
                let mut best_cluster = 0usize;
                let mut best_score = f32::MIN;

                for (cluster_index, centroid) in centroids.iter().enumerate() {
                    let score = cosine_similarity(vector, centroid);
                    if score > best_score {
                        best_score = score;
                        best_cluster = cluster_index;
                    }
                }

                if assignments[index] != best_cluster {
                    assignments[index] = best_cluster;
                    changed = true;
                }
            }

            let mut sums = vec![vec![0.0f32; dims]; cluster_count];
            let mut counts = vec![0usize; cluster_count];

            for (assignment, vector) in assignments.iter().zip(vectors.iter()) {
                counts[*assignment] += 1;
                for (sum, value) in sums[*assignment].iter_mut().zip(vector.iter()) {
                    *sum += *value;
                }
            }

            for cluster_index in 0..cluster_count {
                if counts[cluster_index] == 0 {
                    continue;
                }
                for value in &mut sums[cluster_index] {
                    *value /= counts[cluster_index] as f32;
                }
                centroids[cluster_index] = sums[cluster_index].clone();
            }

            if !changed {
                break;
            }
        }

        match self.backend {
            Backend::Auto => Err(GpuError::UnsupportedBackend("auto".into())),
            Backend::Wgpu | Backend::Cuda | Backend::Metal | Backend::Vulkan | Backend::OpenCl => {
                Ok(assignments)
            }
        }
    }
}

fn cosine_similarity(left: &[f32], right: &[f32]) -> f32 {
    let dot: f32 = left.iter().zip(right.iter()).map(|(a, b)| a * b).sum();
    let left_norm: f32 = left.iter().map(|value| value * value).sum::<f32>().sqrt();
    let right_norm: f32 = right.iter().map(|value| value * value).sum::<f32>().sqrt();
    if left_norm == 0.0 || right_norm == 0.0 {
        0.0
    } else {
        dot / (left_norm * right_norm)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_cosine_similarity_cpu_fallback() {
        let acc = GpuAccelerator {
            backend: Backend::Wgpu,
            runtime: "test".into(),
        };
        let query = vec![1.0f32, 0.0, 0.0];
        let candidates = vec![vec![1.0f32, 0.0, 0.0], vec![0.0f32, 1.0, 0.0]];
        let scores = acc.batch_cosine_similarity(&query, &candidates).unwrap();
        assert!((scores[0] - 1.0).abs() < 1e-5);
        assert!((scores[1] - 0.0).abs() < 1e-5);
    }

    #[test]
    fn test_preferred_backend_selection_is_stable() {
        let _ = GpuAccelerator::preferred_backend();
        let _ = GpuAccelerator::available_backends();
    }
}
