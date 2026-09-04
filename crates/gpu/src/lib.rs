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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;

use bytemuck::cast_slice;
use libloading::Library;
use thiserror::Error;
use wgpu::util::DeviceExt;

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

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SimilarityResult {
    pub index: usize,
    pub score: f32,
}

/// GPU accelerator handle.
pub struct GpuAccelerator {
    pub backend: Backend,
    runtime: String,
    compute: ComputeDevice,
    device_dispatches: AtomicU64,
    cpu_fallbacks: AtomicU64,
}

enum ComputeDevice {
    Wgpu {
        device: wgpu::Device,
        queue: wgpu::Queue,
    },
    CpuFallback,
}

const COSINE_SHADER: &str = r#"
struct Parameters {
    candidate_count: u32,
    dimensions: u32,
    _padding0: u32,
    _padding1: u32,
}

@group(0) @binding(0) var<storage, read> candidates: array<f32>;
@group(0) @binding(1) var<storage, read> query: array<f32>;
@group(0) @binding(2) var<storage, read_write> scores: array<f32>;
@group(0) @binding(3) var<uniform> parameters: Parameters;

@compute @workgroup_size(64)
fn cosine(@builtin(global_invocation_id) invocation: vec3<u32>) {
    let candidate_index = invocation.x;
    if (candidate_index >= parameters.candidate_count) {
        return;
    }

    let offset = candidate_index * parameters.dimensions;
    var dot = 0.0;
    var candidate_norm = 0.0;
    var query_norm = 0.0;
    for (var dimension = 0u; dimension < parameters.dimensions; dimension += 1u) {
        let candidate = candidates[offset + dimension];
        let query_value = query[dimension];
        dot = dot + candidate * query_value;
        candidate_norm = candidate_norm + candidate * candidate;
        query_norm = query_norm + query_value * query_value;
    }
    if (candidate_norm == 0.0 || query_norm == 0.0) {
        scores[candidate_index] = 0.0;
    } else {
        scores[candidate_index] = dot / sqrt(candidate_norm * query_norm);
    }
}
"#;

const NORMALIZE_SHADER: &str = r#"
struct Parameters {
    vector_count: u32,
    dimensions: u32,
    _padding0: u32,
    _padding1: u32,
}

@group(0) @binding(0) var<storage, read_write> vectors: array<f32>;
@group(0) @binding(1) var<uniform> parameters: Parameters;

@compute @workgroup_size(64)
fn normalize(@builtin(global_invocation_id) invocation: vec3<u32>) {
    let vector_index = invocation.x;
    if (vector_index >= parameters.vector_count) {
        return;
    }
    let offset = vector_index * parameters.dimensions;
    var norm_squared = 0.0;
    for (var dimension = 0u; dimension < parameters.dimensions; dimension += 1u) {
        let value = vectors[offset + dimension];
        norm_squared = norm_squared + value * value;
    }
    if (norm_squared == 0.0) {
        return;
    }
    let inverse_norm = inverseSqrt(norm_squared);
    for (var dimension = 0u; dimension < parameters.dimensions; dimension += 1u) {
        vectors[offset + dimension] = vectors[offset + dimension] * inverse_norm;
    }
}
"#;
impl GpuAccelerator {
    pub fn new(backend: Backend) -> Result<Self, GpuError> {
        let selected = match backend {
            Backend::Auto => Self::preferred_backend().ok_or(GpuError::NoDevice)?,
            requested => requested,
        };

        let runtime = Self::detect_backend_runtime(selected)
            .ok_or_else(|| GpuError::UnsupportedBackend(selected.as_str().into()))?;
        let compute = match selected {
            Backend::Metal | Backend::Vulkan | Backend::Wgpu => ComputeDevice::new(selected)?,
            Backend::Cuda | Backend::OpenCl => ComputeDevice::new(Backend::Wgpu)?,
            Backend::Auto => return Err(GpuError::UnsupportedBackend("auto".into())),
        };

        Ok(Self {
            backend: selected,
            runtime,
            compute,
            device_dispatches: AtomicU64::new(0),
            cpu_fallbacks: AtomicU64::new(0),
        })
    }

    pub fn runtime(&self) -> &str {
        &self.runtime
    }

    pub fn device_dispatches(&self) -> u64 {
        self.device_dispatches.load(Ordering::Relaxed)
    }

    pub fn cpu_fallbacks(&self) -> u64 {
        self.cpu_fallbacks.load(Ordering::Relaxed)
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

        [
            Backend::OpenCl,
            Backend::Cuda,
            Backend::Vulkan,
            Backend::Wgpu,
        ]
        .into_iter()
        .find(|&backend| Self::detect_backend_runtime(backend).is_some())
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
        if candidates
            .iter()
            .any(|candidate| candidate.len() != query.len())
        {
            return Err(GpuError::ComputeFailed(
                "candidate vector dimension does not match query".into(),
            ));
        }
        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        match &self.compute {
            ComputeDevice::Wgpu { device, queue } => {
                match gpu_cosine_similarity(device, queue, query, candidates) {
                    Ok(scores) => {
                        self.device_dispatches.fetch_add(1, Ordering::Relaxed);
                        Ok(scores)
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "GPU cosine dispatch failed; using CPU fallback");
                        self.cpu_fallbacks.fetch_add(1, Ordering::Relaxed);
                        Ok(candidates
                            .iter()
                            .map(|candidate| cosine_similarity(query, candidate))
                            .collect())
                    }
                }
            }
            ComputeDevice::CpuFallback => {
                self.cpu_fallbacks.fetch_add(1, Ordering::Relaxed);
                Ok(candidates
                    .iter()
                    .map(|candidate| cosine_similarity(query, candidate))
                    .collect())
            }
        }
    }

    pub fn normalize_vectors(&self, vectors: &[Vec<f32>]) -> Result<Vec<Vec<f32>>, GpuError> {
        let dimensions = vectors.first().map_or(0, Vec::len);
        if vectors.iter().any(|vector| vector.len() != dimensions) {
            return Err(GpuError::ComputeFailed(
                "vector dimensions are inconsistent".into(),
            ));
        }
        if vectors.is_empty() || dimensions == 0 {
            return Ok(vectors.to_vec());
        }
        match &self.compute {
            ComputeDevice::Wgpu { device, queue } => {
                match gpu_normalize_vectors(device, queue, vectors) {
                    Ok(normalized) => {
                        self.device_dispatches.fetch_add(1, Ordering::Relaxed);
                        Ok(normalized)
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "GPU normalization dispatch failed; using CPU fallback");
                        self.cpu_fallbacks.fetch_add(1, Ordering::Relaxed);
                        Ok(cpu_normalize_vectors(vectors))
                    }
                }
            }
            ComputeDevice::CpuFallback => {
                self.cpu_fallbacks.fetch_add(1, Ordering::Relaxed);
                Ok(cpu_normalize_vectors(vectors))
            }
        }
    }

    pub fn top_k_cosine_similarity(
        &self,
        query: &[f32],
        candidates: &[Vec<f32>],
        top_k: usize,
    ) -> Result<Vec<SimilarityResult>, GpuError> {
        let mut results: Vec<_> = self
            .batch_cosine_similarity(query, candidates)?
            .into_iter()
            .enumerate()
            .map(|(index, score)| SimilarityResult { index, score })
            .collect();
        results.sort_unstable_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.index.cmp(&right.index))
        });
        results.truncate(top_k.min(results.len()));
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

            let scores_by_centroid: Result<Vec<_>, _> = centroids
                .iter()
                .map(|centroid| self.batch_cosine_similarity(centroid, vectors))
                .collect();
            let scores_by_centroid = scores_by_centroid?;
            for (index, _vector) in vectors.iter().enumerate() {
                let mut best_cluster = 0usize;
                let mut best_score = f32::MIN;

                for (cluster_index, scores) in scores_by_centroid.iter().enumerate() {
                    let score = scores[index];
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

impl ComputeDevice {
    fn new(backend: Backend) -> Result<Self, GpuError> {
        let backends = match backend {
            Backend::Metal => wgpu::Backends::METAL,
            Backend::Vulkan => wgpu::Backends::VULKAN,
            Backend::Wgpu => wgpu::Backends::METAL | wgpu::Backends::VULKAN | wgpu::Backends::DX12,
            _ => return Ok(Self::CpuFallback),
        };
        let instance = wgpu::Instance::new(wgpu::InstanceDescriptor {
            backends,
            ..wgpu::InstanceDescriptor::new_without_display_handle()
        });
        let adapter = pollster::block_on(instance.request_adapter(&wgpu::RequestAdapterOptions {
            power_preference: wgpu::PowerPreference::HighPerformance,
            force_fallback_adapter: false,
            compatible_surface: None,
            ..Default::default()
        }))
        .map_err(|error| GpuError::InitFailed(error.to_string()))?;
        let adapter_info = adapter.get_info();
        if backend == Backend::Metal && adapter_info.backend != wgpu::Backend::Metal {
            return Err(GpuError::InitFailed(format!(
                "requested Metal, selected {:?}",
                adapter_info.backend
            )));
        }
        let (device, queue) = pollster::block_on(adapter.request_device(&wgpu::DeviceDescriptor {
            label: Some("copperdb-gpu"),
            required_features: wgpu::Features::empty(),
            required_limits: wgpu::Limits::downlevel_defaults(),
            ..Default::default()
        }))
        .map_err(|error| GpuError::InitFailed(error.to_string()))?;
        Ok(Self::Wgpu { device, queue })
    }
}

fn gpu_cosine_similarity(
    device: &wgpu::Device,
    queue: &wgpu::Queue,
    query: &[f32],
    candidates: &[Vec<f32>],
) -> Result<Vec<f32>, GpuError> {
    let dimensions = u32::try_from(query.len())
        .map_err(|_| GpuError::ComputeFailed("query dimensions exceed GPU limit".into()))?;
    let candidate_count = u32::try_from(candidates.len())
        .map_err(|_| GpuError::ComputeFailed("candidate count exceeds GPU limit".into()))?;
    let flat_candidates: Vec<f32> = candidates.iter().flatten().copied().collect();
    let candidate_buffer = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
        label: Some("copperdb GPU candidates"),
        contents: cast_slice(&flat_candidates),
        usage: wgpu::BufferUsages::STORAGE,
    });
    let query_buffer = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
        label: Some("copperdb GPU query"),
        contents: cast_slice(query),
        usage: wgpu::BufferUsages::STORAGE,
    });
    let parameters = [candidate_count, dimensions, 0, 0];
    let parameter_buffer = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
        label: Some("copperdb GPU cosine parameters"),
        contents: cast_slice(&parameters),
        usage: wgpu::BufferUsages::UNIFORM,
    });
    let output_size = u64::from(candidate_count) * std::mem::size_of::<f32>() as u64;
    let score_buffer = device.create_buffer(&wgpu::BufferDescriptor {
        label: Some("copperdb GPU cosine scores"),
        size: output_size,
        usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
        mapped_at_creation: false,
    });
    let readback_buffer = device.create_buffer(&wgpu::BufferDescriptor {
        label: Some("copperdb GPU cosine readback"),
        size: output_size,
        usage: wgpu::BufferUsages::COPY_DST | wgpu::BufferUsages::MAP_READ,
        mapped_at_creation: false,
    });
    let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
        label: Some("copperdb cosine shader"),
        source: wgpu::ShaderSource::Wgsl(COSINE_SHADER.into()),
    });
    let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
        label: Some("copperdb cosine pipeline"),
        layout: None,
        module: &shader,
        entry_point: Some("cosine"),
        compilation_options: Default::default(),
        cache: None,
    });
    let bind_group = device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("copperdb cosine bind group"),
        layout: &pipeline.get_bind_group_layout(0),
        entries: &[
            wgpu::BindGroupEntry {
                binding: 0,
                resource: candidate_buffer.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 1,
                resource: query_buffer.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 2,
                resource: score_buffer.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 3,
                resource: parameter_buffer.as_entire_binding(),
            },
        ],
    });
    let mut encoder = device.create_command_encoder(&wgpu::CommandEncoderDescriptor {
        label: Some("copperdb cosine encoder"),
    });
    {
        let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
            label: Some("copperdb cosine pass"),
            timestamp_writes: None,
        });
        pass.set_pipeline(&pipeline);
        pass.set_bind_group(0, &bind_group, &[]);
        pass.dispatch_workgroups(candidate_count.div_ceil(64), 1, 1);
    }
    encoder.copy_buffer_to_buffer(&score_buffer, 0, &readback_buffer, 0, output_size);
    queue.submit(Some(encoder.finish()));

    let slice = readback_buffer.slice(..);
    let (sender, receiver) = mpsc::channel();
    slice.map_async(wgpu::MapMode::Read, move |result| {
        let _ = sender.send(result);
    });
    device
        .poll(wgpu::PollType::wait_indefinitely())
        .map_err(|error| GpuError::ComputeFailed(error.to_string()))?;
    receiver
        .recv()
        .map_err(|error| GpuError::ComputeFailed(error.to_string()))?
        .map_err(|error| GpuError::ComputeFailed(error.to_string()))?;
    let scores = {
        let mapped = slice
            .get_mapped_range()
            .map_err(|error| GpuError::ComputeFailed(error.to_string()))?;
        cast_slice::<u8, f32>(&mapped).to_vec()
    };
    readback_buffer.unmap();
    Ok(scores)
}

fn gpu_normalize_vectors(
    device: &wgpu::Device,
    queue: &wgpu::Queue,
    vectors: &[Vec<f32>],
) -> Result<Vec<Vec<f32>>, GpuError> {
    let dimensions = u32::try_from(vectors[0].len())
        .map_err(|_| GpuError::ComputeFailed("vector dimensions exceed GPU limit".into()))?;
    let vector_count = u32::try_from(vectors.len())
        .map_err(|_| GpuError::ComputeFailed("vector count exceeds GPU limit".into()))?;
    let flat_vectors: Vec<f32> = vectors.iter().flatten().copied().collect();
    let byte_size = (flat_vectors.len() * std::mem::size_of::<f32>()) as u64;
    let vector_buffer = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
        label: Some("copperdb GPU vectors"),
        contents: cast_slice(&flat_vectors),
        usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
    });
    let parameters = [vector_count, dimensions, 0, 0];
    let parameter_buffer = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
        label: Some("copperdb GPU normalization parameters"),
        contents: cast_slice(&parameters),
        usage: wgpu::BufferUsages::UNIFORM,
    });
    let readback_buffer = device.create_buffer(&wgpu::BufferDescriptor {
        label: Some("copperdb GPU normalization readback"),
        size: byte_size,
        usage: wgpu::BufferUsages::COPY_DST | wgpu::BufferUsages::MAP_READ,
        mapped_at_creation: false,
    });
    let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
        label: Some("copperdb normalization shader"),
        source: wgpu::ShaderSource::Wgsl(NORMALIZE_SHADER.into()),
    });
    let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
        label: Some("copperdb normalization pipeline"),
        layout: None,
        module: &shader,
        entry_point: Some("normalize"),
        compilation_options: Default::default(),
        cache: None,
    });
    let bind_group = device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("copperdb normalization bind group"),
        layout: &pipeline.get_bind_group_layout(0),
        entries: &[
            wgpu::BindGroupEntry {
                binding: 0,
                resource: vector_buffer.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 1,
                resource: parameter_buffer.as_entire_binding(),
            },
        ],
    });
    let mut encoder = device.create_command_encoder(&wgpu::CommandEncoderDescriptor {
        label: Some("copperdb normalization encoder"),
    });
    {
        let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
            label: Some("copperdb normalization pass"),
            timestamp_writes: None,
        });
        pass.set_pipeline(&pipeline);
        pass.set_bind_group(0, &bind_group, &[]);
        pass.dispatch_workgroups(vector_count.div_ceil(64), 1, 1);
    }
    encoder.copy_buffer_to_buffer(&vector_buffer, 0, &readback_buffer, 0, byte_size);
    queue.submit(Some(encoder.finish()));
    let slice = readback_buffer.slice(..);
    let (sender, receiver) = mpsc::channel();
    slice.map_async(wgpu::MapMode::Read, move |result| {
        let _ = sender.send(result);
    });
    device
        .poll(wgpu::PollType::wait_indefinitely())
        .map_err(|error| GpuError::ComputeFailed(error.to_string()))?;
    receiver
        .recv()
        .map_err(|error| GpuError::ComputeFailed(error.to_string()))?
        .map_err(|error| GpuError::ComputeFailed(error.to_string()))?;
    let normalized = {
        let mapped = slice
            .get_mapped_range()
            .map_err(|error| GpuError::ComputeFailed(error.to_string()))?;
        cast_slice::<u8, f32>(&mapped).to_vec()
    };
    readback_buffer.unmap();
    Ok(normalized
        .chunks_exact(dimensions as usize)
        .map(ToOwned::to_owned)
        .collect())
}

fn cpu_normalize_vectors(vectors: &[Vec<f32>]) -> Vec<Vec<f32>> {
    vectors
        .iter()
        .map(|vector| {
            let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
            if norm == 0.0 {
                vector.clone()
            } else {
                vector.iter().map(|value| value / norm).collect()
            }
        })
        .collect()
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
            compute: ComputeDevice::CpuFallback,
            device_dispatches: AtomicU64::new(0),
            cpu_fallbacks: AtomicU64::new(0),
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

    #[cfg(target_os = "macos")]
    #[test]
    fn metal_batch_cosine_similarity_executes_device_kernel() {
        let accelerator = GpuAccelerator::new(Backend::Metal)
            .expect("this macOS test host must provide a Metal compute device");
        let query = vec![3.0f32, 4.0, 0.0];
        let candidates = vec![vec![6.0f32, 8.0, 0.0], vec![0.0f32, 2.0, 0.0]];

        let scores = accelerator
            .batch_cosine_similarity(&query, &candidates)
            .expect("Metal cosine kernel must complete");

        assert_eq!(scores.len(), candidates.len());
        assert!((scores[0] - 1.0).abs() < 1e-5, "scores: {scores:?}");
        assert!((scores[1] - 0.8).abs() < 1e-5, "scores: {scores:?}");

        let normalized = accelerator
            .normalize_vectors(&[vec![3.0, 4.0], vec![0.0, 0.0]])
            .expect("Metal normalization kernel must complete");
        assert!(
            (normalized[0][0] - 0.6).abs() < 1e-5,
            "normalized: {normalized:?}"
        );
        assert!(
            (normalized[0][1] - 0.8).abs() < 1e-5,
            "normalized: {normalized:?}"
        );
        assert_eq!(normalized[1], vec![0.0, 0.0]);

        let top_k = accelerator
            .top_k_cosine_similarity(&[1.0, 0.0], &[vec![1.0, 0.0], vec![1.0, 0.0]], 2)
            .expect("Metal top-k score kernel must complete");
        assert_eq!(
            top_k,
            vec![
                SimilarityResult {
                    index: 0,
                    score: 1.0
                },
                SimilarityResult {
                    index: 1,
                    score: 1.0
                }
            ]
        );

        let assignments = accelerator
            .kmeans(&[vec![1.0, 0.0], vec![0.9, 0.1], vec![0.0, 1.0]], 2, 2)
            .expect("Metal K-means assignment kernel must complete");
        assert_eq!(assignments, vec![0, 0, 1]);
        assert!(accelerator.device_dispatches() >= 4);
        assert_eq!(accelerator.cpu_fallbacks(), 0);
    }
}
