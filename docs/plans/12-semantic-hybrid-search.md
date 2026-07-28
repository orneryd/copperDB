# 12: Semantic And Hybrid Search

Status: planned. Priority: P1. Owners: `search`, `vectorspace`, `engine`, `server`, `knowledgepolicy`.

## Objective

Implement one engine-owned semantic/hybrid search API using maintained lexical and vector indexes, deterministic RRF, policy filtering, hydration, pagination, cancellation, and stage diagnostics.

## Request And Response Contract

Requests identify database, mode (`lexical`, `semantic`, `hybrid`), text or vector, index/labels/fields, limit, offset, minimum score, RRF constant/weights, and caller policy context. Responses identify actual strategies, readiness, candidate/filtered counts, partial/degraded state, stable hits, and stage timings.

## Execution Order

1. Validate per-database feature gates and index readiness.
2. Produce bounded lexical and vector candidate batches independently.
3. Convert to one hit envelope and merge with deterministic RRF: score descending, stable ID tie-break.
4. Apply compliance/knowledge-policy/decay suppression before pagination.
5. Hydrate only the retained bounded window and return diagnostics.

## Phases

1. Introduce shared index/service traits with status, generation, bytes, cancellation, and actual implementation kind.
2. Implement local semantic batches over item 10 and optional item 11 query embedding.
3. Implement hybrid composition using existing RRF primitives and strict duplicate handling.
4. Replace the server's BM25-only branch with the engine API; maintain HTTP/embedded parity.
5. Add parsed/query result caches keyed by data/index generation and stage-level observability.

## Tests And Performance

Test deterministic ties, dimensions, min score, fallback labels, disabled gates, declared indexes when automatic work is off, duplicate fusion, suppression before pagination, cancellation, restart/rebuild, and HTTP parity. Benchmark each stage, candidate oversampling, recall, QPS, hydration counts, and cache behavior.

## Definition Of Done

Semantic and hybrid paths no longer return unimplemented errors, use maintained indexes without unbounded hydration, produce deterministic policy-safe results, and expose actual strategy/readiness at engine and HTTP surfaces.