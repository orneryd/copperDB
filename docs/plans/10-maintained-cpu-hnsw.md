# 10: Maintained CPU HNSW

Status: planned. Priority: P1. Owners: `vectorspace`, `search`, `storage`, `engine`, `eval`.

## Objective

Replace full-vector scans mislabeled as HNSW with an engine-owned, maintained, persistent CPU HNSW service and explicit exact fallback policy.

## Current Evidence

`VectorSpace::knn` scores and sorts every vector. Vector procedures scan storage records. Engine semantic/hybrid routes are unimplemented. NornicDB references include HNSW, vector file store, search services, readiness, passive-write, exact-candidate, and shutdown fixes listed in the consolidated audit.

## Runtime Contract

Each schema-declared vector index owns dimensions, metric, generation, readiness, actual strategy, and an `AnnIndex` implementation. Reads never trigger builds or strategy changes. Committed storage events maintain inserts/updates/tombstones. Exact CPU fallback is explicit and observable, never reported as HNSW. Dimension mismatch is an error.

## Persistence

Store normalized vectors in a versioned append-only file with ID-to-offset metadata. Persist HNSW graph metadata separately with magic, format version, dimensions, metric, config, checksum, and source generation. Artifacts are rebuildable derived state and must not block graph storage open when incompatible.

## Phases

1. True in-memory HNSW traversal with deterministic seeds and exact-oracle tests.
2. Mutation maintenance, tombstones, update semantics, and periodic rebuild thresholds.
3. Per-database/index service registry, readiness, cancellation, and procedure routing without record scans.
4. Vector/graph persistence, reopen, corruption detection, compaction, and rebuild orchestration.
5. Recall/latency tuning, file-backed exact candidate supplementation, and observability.

## Tests And Benchmarks

Test CRUD, dimensions, zero vectors, concurrency, deterministic ties, cancellation, recall, no query-triggered warming, restart/corruption/version mismatch, tombstones, shutdown, and node/relationship procedures. Benchmark 1k to 1m vectors at 128/384/1,024 dimensions; report recall@k, p50/p99, QPS, visited nodes, memory/index bytes, build/update/load time.

Initial gate: recall@10 at least 0.95 on seeded representative sets and a clear latency advantage by 10k vectors without unbounded tombstones.

## Definition Of Done

Owned vector queries execute graph traversal rather than all-record scans, survive restart, follow committed mutations, expose honest readiness/fallback, meet recall/latency gates, and stop cleanly.