# 20: GPU And SIMD Acceleration

Status: planned. Priority: P2. Owners: `math`, `simd`, `gpu`, `vectorspace`, `search`.

## Objective

Add measured SIMD and real GPU acceleration behind one authoritative CPU-visible vector contract, enabling a backend only when end-to-end crossover measurements justify it.

## Current Evidence

`simd` has real `wide::f32x8` kernels, but zero/error semantics differ from `math`. `gpu` detects libraries/backends while its scoring/k-means implementations still execute CPU loops. Vector search must first become true maintained HNSW under item 10.

## Contract

Define dimensions, empty/zero vectors, NaN/Inf, normalization, accumulation precision, ordering/ties, tolerances, cancellation, and fallback once in scalar reference code. SIMD/GPU results must preserve ranking and errors within declared tolerance.

## Phases

1. Unify scalar semantics and property tests across `math`, `simd`, exact fallback, HNSW distance, and reranking.
2. Add runtime-dispatched scalar/SIMD single and flattened batch kernels; benchmark crossover on x86-64 and ARM64.
3. Implement one real `wgpu` compute backend with buffer pooling, bounded allocation, device-loss handling, cancellation boundaries, and transparent CPU fallback.
4. Add transfer-inclusive GPU batch scoring; add k-means only if measured benefit warrants it.
5. Consider backend-specific CUDA/Metal only after portable GPU behavior is stable. Distributed GPU scheduling remains excluded.

## Validation And Benchmark Matrix

Property tests cover odd sizes, zero/subnormal/NaN/Inf/large values, deterministic top-k, and backend failure. Benchmark dimensions 32-4096 and batches 1-100k, including allocation/transfer, concurrency, peak host/device memory, throughput, and p99. Enable only above measured conservative crossover thresholds.

## Definition Of Done

Paths labeled GPU execute verified device kernels, disabling acceleration gives equivalent behavior, fallback is transparent and observable, and each enabled path demonstrates end-to-end benefit on documented hardware.