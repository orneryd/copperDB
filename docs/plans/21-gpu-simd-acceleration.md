# 21: GPU And SIMD Acceleration

Status: in progress. Priority: P2. Owners: `math`, `simd`, `gpu`, `vectorspace`, `search`.

## Objective

Add measured SIMD and real GPU acceleration behind one authoritative CPU-visible vector contract, enabling a backend only when end-to-end crossover measurements justify it.

## Current Evidence

`simd` has real `wide::f32x8` kernels, but zero/error semantics differ from `math`. `gpu` now executes portable wgpu compute shaders for batch cosine similarity and normalization; K-means uses those device scores for assignment while retaining deterministic CPU centroid reduction and ordering. On this macOS host the Metal path is verified end to end. Vector search must first become true maintained HNSW under item 10, and Plan 20 must close upstream fixed-stride vector and architecture-specific tolerance changes.

## Contract

Define dimensions, empty/zero vectors, NaN/Inf, normalization, accumulation precision, ordering/ties, tolerances, cancellation, and fallback once in scalar reference code. SIMD/GPU results must preserve ranking and errors within declared tolerance.

## Phases

1. Unify scalar semantics and property tests across `math`, `simd`, exact fallback, HNSW distance, and reranking.
2. Add runtime-dispatched scalar/SIMD single and flattened batch kernels; benchmark crossover on x86-64 and ARM64.
3. Implement one real `wgpu` compute backend with buffer pooling, bounded allocation, device-loss handling, cancellation boundaries, and transparent CPU fallback.
4. Add transfer-inclusive GPU batch scoring; add k-means only if measured benefit warrants it.
5. Consider backend-specific CUDA/Metal only after portable GPU behavior is stable. Distributed GPU scheduling remains excluded.

## Backend Matrix

| Requested backend | Execution path | Status |
| --- | --- | --- |
| `metal` | wgpu Metal compute shader | Verified on macOS for scoring, normalization, stable top-k, and K-means assignment. |
| `vulkan` | wgpu Vulkan compute shader | Compiles; requires verification on a Vulkan-capable host. |
| `cuda` | CUDA runtime detection plus wgpu adapter selection | Compiles; executes through Vulkan or DX12 where that NVIDIA driver is available. Native CUDA kernels remain future work. |
| `opencl` | OpenCL runtime detection plus wgpu adapter selection | Compiles; executes through Vulkan or DX12 where that driver is available. Native OpenCL kernels remain future work. |
| `wgpu` | Metal, Vulkan, or DX12 selected by wgpu | Compiles; Metal verification complete. |

## Validation And Benchmark Matrix

Property tests cover odd sizes, zero/subnormal/NaN/Inf/large values, deterministic top-k, architecture-specific tolerances from Plan 20, and backend failure. Benchmark dimensions 32-4096 and batches 1-100k, including allocation/transfer, concurrency, peak host/device memory, throughput, and p99. Enable only above measured conservative crossover thresholds.

## Definition Of Done

Paths labeled GPU execute verified device kernels, disabling acceleration gives equivalent behavior, fallback is transparent and observable, and each enabled path demonstrates end-to-end benefit on documented hardware. Record the supported target/backend/feature matrix, runtime library requirements, CPU feature floors, and experimental exclusions as the authoritative publication input for Plan 23.