# 10: Maintained CPU HNSW

Status: complete. Priority: P1. Owners: `vectorspace`, `search`, `storage`, `engine`, `eval`.

## Objective

Replace full-vector scans mislabeled as HNSW with an engine-owned, maintained, persistent CPU HNSW service and explicit exact fallback policy.

## Current Evidence

`VectorSpace::knn` is the explicit exact cosine oracle and bounded fallback. Vector procedures use the engine-owned maintained index service rather than scanning storage records. Engine semantic/hybrid routes remain outside this plan. NornicDB references include HNSW, vector file store, search services, readiness, passive-write, exact-candidate, and shutdown fixes listed in the consolidated audit.

## Progress

- Complete: `vectorspace` reports its bounded full-scan implementation as `ExactCosine` rather than HNSW, with deterministic ID tie-breaking. This remains the explicit exact oracle/fallback baseline.
- Complete: `vectorspace::HnswIndex` owns deterministic in-memory graph construction and traversal. Its seeded test compares a query result with the exact oracle and verifies that graph queries visit fewer candidates than the corpus; engine lifecycle, mutations, and persistence are wired through the registry and vector index manager described below.
- Complete: the in-memory index now supports tombstone deletion, deterministic upsert rebuilding, mutation-triggered graph compaction after a bounded tombstone threshold, and explicit below-threshold compaction. Query reads do not rebuild or mutate the graph.
- Complete: `vectorspace::HnswRegistry` provides a thread-safe named-index service with explicit HNSW strategy, readiness, mutation generation, and explicit compaction. Empty-index queries preserve that generation, proving that reads do not trigger warming.
- Complete: storage event registrations fan out to every listener rather than replacing a previous listener. Engine-owned vector maintenance consumes node and relationship mutation callbacks without blocking other post-commit consumers.
- Complete: `CopperDb` now builds declared node vector indexes with explicit dimensions during startup, updates them from post-commit node events, and routes `db.index.vector.queryNodes` through registry-owned candidate lookup plus ID hydration. Cosine uses HNSW traversal; Euclidean uses an explicit exact strategy and is never reported as HNSW. The evaluator no longer performs an all-record fallback scan. Broader lifecycle work remains open.
- Complete: `HnswRegistry` writes and reloads a single greenfield format-1, checksummed artifact containing normalized active vectors, index configuration, strategy, generation, and committed storage revision. Loads reject corrupt or incompatible artifacts and deterministically rebuild HNSW topology. `CopperDb` stores the sidecar beside durable storage, persists it after all post-commit maintenance callbacks and vector DDL changes, restores it only when its source revision and declared index schema match, and otherwise rebuilds from graph records.
- Complete: declared relationship vector indexes now share the engine-owned registry lifecycle: startup/reopen builds, committed edge create/update/delete callbacks, sidecar persistence, and `db.index.vector.queryRelationships` candidate lookup with ID hydration. Relationship queries no longer scan stored edges; Phase 5 tuning/observability remains open.
- Complete: vector index status now exposes tombstone count and an explicitly scoped owned-buffer memory estimate, and `CopperDb::compact_vector_index` explicitly rebuilds a named HNSW index below the automatic threshold before synchronously refreshing its sidecar artifact. Query reads remain non-mutating. Persistent cosine indexes mirror normalized vectors into per-index greenfield append-only files, rebuilt from graph records on startup and vector DDL and maintained through committed node/relationship mutations. Engine-owned vector procedures expand HNSW results to a bounded `4 * k` candidate set and deterministically exact-rerank those IDs against the file store; standalone evaluators retain direct registry behavior.
- Complete: exact cosine vectors are normalized once at mutation time, queries are normalized once per request, scoring uses the runtime-dispatched SIMD dot-product kernel, and a bounded top-k heap avoids cloning and sorting the full corpus. Deterministic score/ID ordering, replacement, zero vectors, non-unit inputs, and `k = 0` are covered by regression tests.

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

Progress: `cargo bench -p copperdb-vectorspace --bench hnsw` calibrates deterministic recall@10, average visited nodes, raw vector bytes, an owned-buffer graph-memory estimate, and Windows process working-set before/after/delta before measuring HNSW build, update/rebuild, artifact load/rebuild, HNSW query, HNSW plus file-backed exact reranking, and exact-cosine full-scan query latency at 128/384/1,024 dimensions. All reported Rust measurements use Cargo's optimized `bench` profile; the build must report `Finished bench profile [optimized]` and any directly invoked scale-gate executable must come from `target/release/deps`, never `target/debug`. It defaults to 10,000 vectors; set `COPPERDB_HNSW_BENCH_VECTORS` to 1,000 through 1,000,000 for a scale sweep and `COPPERDB_HNSW_BENCH_DIMENSIONS` to a comma-separated dimension list for a targeted run. Set `COPPERDB_HNSW_BENCH_SCALE_GATE=1` for bounded one-shot build/update/save/load measurements and fixed-corpus query latency/QPS instead of repeated production-scale rebuilds under Criterion.

Correlated exact-cosine results on an Intel i9-9900KF with Rust/Cargo 1.95.0 and Go 1.25.4 compare the same 10,000 deterministic vectors and 16 queries against NornicDB `2c7dbe9e`. CopperDB's optimized `bench` profile averages 0.893 ms at 128 dimensions, 1.841 ms at 384 dimensions, and 3.540 ms at 1,024 dimensions. NornicDB averages 2.524 ms, 3.842 ms, and 5.215 ms respectively, so CopperDB is 2.83x, 2.09x, and 1.47x faster on the correlated exact scan.

Correlated HNSW results use the same machine, vectors, queries, `k = 10`, accurate CPU preset, and disabled GPU build. At 10,000 vectors CopperDB builds in 2.973/6.248/16.132 seconds at 128/384/1,024 dimensions versus NornicDB's 3.781/7.617/17.484 seconds. CopperDB HNSW query averages are 0.406/0.897/1.607 ms versus NornicDB's 0.928/0.804/1.924 ms; recall is 1.000/1.000/0.994 versus 1.000/1.000/0.988. At 100,000 vectors and 128 dimensions, CopperDB builds in 97.404 seconds, queries in 0.865 ms, exact-scans in 14.161 ms, and reaches 0.950 recall; NornicDB builds in 126.464 seconds, queries in 1.446 ms, exact-scans in 34.560 ms, and reaches 0.944 recall. CopperDB therefore passes the production-scale recall gate while building 1.30x faster and querying HNSW 1.67x faster.

Initial gate: recall@10 at least 0.95 on seeded representative sets and a clear latency advantage by 10k vectors without unbounded tombstones.

## Definition Of Done

Owned vector queries execute graph traversal rather than all-record scans, survive restart, follow committed mutations, expose honest readiness/fallback, meet recall/latency gates, and stop cleanly.