# copperDB / NornicDB Consolidated Parity Plan

Date: 2026-07-28

Status: authoritative implementation audit and roadmap.

This document is the single source of truth for copperDB's implementation status, NornicDB parity work, upstream synchronization, and execution order. Older parity checklists, package plans, architecture proposals, and status tables remain useful historical or design references, but they do not override this plan.

Detailed execution plans for all numbered audit items are indexed in [plans/README.md](plans/README.md).

## Audit Baseline

| Repository | Branch | Audited commit | Date |
| --- | --- | --- | --- |
| copperDB | `main` | `9e37203d6525b82a62332389a4268e1338278de0` | 2026-06-30 |
| NornicDB | `main` | `36f2e532ab4839e237691866b978249ee149fe0f` | 2026-07-27 |
| Previous NornicDB plan anchor | `main` history | `fd37a21e9694c5739b6afe2e6d78a4225b55c981` | superseded |
| Previous Cypher wave anchor | `main` history | `f1fb4beb` | superseded |

NornicDB is 236 commits beyond the oldest recorded audit anchor. The source inventory at this baseline is 50 copperDB workspace packages, 109 Rust source files, and 1,674 upstream Go files under `pkg` and `cmd`. File counts indicate audit scale, not implementation completeness.

The audit combined:

- current source inspection in both local repositories;
- NornicDB history and changed-file review from the recorded anchors to current `main`;
- targeted package audits for storage, Cypher/eval, Bolt, server, search/vector, embedding, configuration, security, and distributed scaffolding;
- direct inspection of known stubs, fallback paths, and completion claims;
- focused and workspace Rust tests used by the audit agents.

This is a broad source-level audit, not a claim that every upstream file has already received a final Rust disposition. File-level disposition is an explicit completion gate below.

## Product Boundary

copperDB currently supports single-node execution only. Distributed placement, replication, fabric federation, remote shard execution, consensus transaction time, and multi-region operation remain deferred architecture. Existing distributed crates contain meaningful contracts and experiments, but they are not a supported runtime.

The parity objective is behavioral compatibility with NornicDB where it improves the single-node product, implemented in Rust-native ways that preserve or improve safety and performance. Parity does not require copying Go package internals or retaining upstream implementation constraints.

## Next Steps: Audit Items To Implement First

The first work must close verified audit findings. New feature expansion should not bypass these items.

### P0: Correctness And Security

1. **Complete: enable authentication by default and define one override contract.**
   - Upstream: commits `20215f13` and `1837c8c7`; `pkg/config/config.go`, `cmd/nornicdb/main.go`.
   - Copper targets: [crates/config/src/lib.rs](../crates/config/src/lib.rs), [crates/server/src/lib.rs](../crates/server/src/lib.rs), [crates/copperdb/src/main.rs](../crates/copperdb/src/main.rs), and [crates/engine/src/lib.rs](../crates/engine/src/lib.rs).
   - Delivered behavior: authentication defaults on; `auth.enabled` and `COPPERDB_AUTH_ENABLED` participate in normal precedence; `--no-auth` is the only command-line bypass; HTTP, Bolt, and internal services consume the same resolved setting.
   - Validated: config, executable startup, Bolt, server auth, engine, and full workspace test suites.

2. **Replace Bolt's placeholder authentication and transaction acknowledgements.**
   - Upstream: `pkg/bolt/server.go` and transaction/write tests.
   - Copper target: [crates/bolt/src/server.rs](../crates/bolt/src/server.rs), plus shared auth, engine, errors, and txsession crates.
   - Current gap: `LOGON` does not validate credentials, `RUN` is not reliably auth-gated, and `BEGIN`/`COMMIT`/`ROLLBACK` can acknowledge operations without owning a real transaction. The server adapter also supplies administrative roles instead of propagating the authenticated principal.
   - Required behavior: validate `HELLO`/`LOGON`, carry principal and per-database roles, reject unauthenticated `RUN`, bind explicit transactions to their connection/session, implement rollback and commit semantics, support `PULL`/`DISCARD` pagination, and map transaction errors to Neo4j-compatible codes.
   - Exit tests: Bolt driver authentication, denied role access, commit visibility, rollback invisibility, disconnect cleanup, pagination, bookmark flow, and retryable error mapping.

3. **Complete: classify post-snapshot edge updates as retryable conflicts.**
   - Upstream: commit `36f2e532`; `pkg/storage/badger_transaction.go`, `pkg/storage/badger_transaction_edge_update_snapshot_conflict_test.go`, and the Bolt E2E regression.
   - Copper targets: [crates/storage/src/lib.rs](../crates/storage/src/lib.rs), [crates/storage/src/mvcc.rs](../crates/storage/src/mvcc.rs), [crates/errors/src/lib.rs](../crates/errors/src/lib.rs), [crates/txsession/src/lib.rs](../crates/txsession/src/lib.rs), and Bolt error encoding.
   - Delivered behavior: edge conflict validation and commit share one serialized storage boundary; a post-snapshot edge change returns the typed `StorageError::TransactionConflict`, which maps to `Neo.TransientError.Transaction.Outdated`. Snapshot-visible deleted edges remain `NotFound`, while identical logical updates converge as timestamp-insensitive no-ops.
   - Validated: storage stale/fresh/deleted/no-op and non-conflicting controls, an explicit Cypher `MERGE ... ON MATCH SET` race through the production executor, and a live TCP Bolt `FAILURE` response carrying `Neo.TransientError.Transaction.Outdated`.

4. **Complete: port NornicDB's Lucene-classic full-text query behavior.**
   - Upstream: commit `e090df01`; `pkg/cypher/fulltext_query.go`, `fulltext_query_test.go`, and `call_fulltext_parser_test.go`.
   - Copper targets: [crates/search/src/lib.rs](../crates/search/src/lib.rs), [crates/eval/src/eval_engine_policy.rs](../crates/eval/src/eval_engine_policy.rs), storage full-text indexes, and procedure tests.
   - Delivered behavior: one typed parser/evaluator supports Boolean groups, required/prohibited clauses, field scopes, phrases/proximity, fuzzy terms, ranges, boosts, wildcard variants including leading wildcards, regex, match-all/presence, empty input, and Lucene escapes for node and relationship procedures. Candidate planning uses maintained node and relationship postings with bounded cancellable vocabulary expansion; full-text result ties are ordered by score then entity ID.
   - Validated: the shared upstream parser/evaluator truth-table mirror, node and relationship procedure regressions for options, parameters, cancellation, match-all/presence, pure-negative queries, deterministic ordering, schema-cache invalidation, and Criterion benchmarks for the five required query classes.

5. **Close the July Cypher correctness regression set.**
   - Upstream families: OPTIONAL MATCH function projection and aggregate identity (`e4b84afe`, `883065cd`), connected-node delete guard and relationship existence/scoping (`4f35ea92`, `8775bf1c`), relationship rebinding and IN-list indexed traversal (`98f6b4c1`, `2959060f`), colon-containing property keys (`ce1973e6`), and explicit-transaction UNWIND mutation statistics (`b46ceb1f`, `389fb2e6`).
   - Copper targets: [crates/cypher/src](../crates/cypher/src), [crates/eval/src](../crates/eval/src), [crates/engine/src](../crates/engine/src), and Bolt transaction tests.
   - Required behavior: port each upstream regression or record a precise equivalent/non-applicable disposition. A broad existing feature is not sufficient evidence for a specific edge case.
   - Exit tests: a checked manifest maps every changed upstream test in the selected range to a Copper test, implementation issue, or justified non-applicable entry.

6. **Fix the bounded embedding cache capacity path.**
   - Copper target: [crates/embed/src/cached.rs](../crates/embed/src/cached.rs).
   - Current gap: the eviction loop can remain at capacity because eviction does not remove or reuse an entry, allowing a cache miss after capacity to hang.
   - Required behavior: eviction makes immediate capacity available, preserves bounded memory, and remains safe under concurrent misses.
   - Exit tests: capacity-one replacement, repeated churn, concurrent same-key requests, and bounded entry count.

### P1: Single-Node Runtime Parity And Performance

7. **Complete: make unique-constraint synchronization key-granular.**
   - Upstream: commit `6362a50` and unique-lock concurrency tests.
   - Copper target: storage schema/unique-value synchronization.
   - Delivered behavior: canonical node and endpoint-scoped relationship keys cover composite values, namespace isolation, updates, deletes, `UNIQUE`, `NODE KEY`, and `RELATIONSHIP KEY`. Per-entity and per-value registries retire only after their final holder/waiter releases; no full-map cleanup or evaluator duplicate scan remains for storage-owned collision classes.
   - Validated: concurrent same-key winner/disjoint-key success, composite and typed values, update/delete release, node/relationship direct and transactional writes, namespace isolation, `MERGE ... ON CREATE SET`, and registry retirement-race regressions. Durable whole-batch serialization is provided by completed item 8.

8. **Finish durable transaction semantics, MVCC history, and WAL integration.**
   - Progress: `StorageEngine::batch_write` now commits primary records and maintained derived state through one cross-keyspace Fjall batch, with MVCC mirrors and callbacks applied only after storage commit. Transaction-local writes, persistent history, and WAL authority remain open.
   - Define whether historical versions survive restart and make the implementation match that contract.
   - Ensure structured storage mutations participate in one atomic transaction/WAL boundary.
   - Complete snapshot anomaly, namespace pinning, WAL repair, corruption reporting, snapshot install, and lifecycle orchestration tests.

9. **Complete: implement the offline administrative import/export subsystem.**
   - Upstream: `pkg/adminimport/importer.go`, `neo4j_csv.go`, and tests.
   - Copper targets: [crates/convert/src/lib.rs](../crates/convert/src/lib.rs), storage batching, indexing, and executable commands.
   - Delivered behavior: bounded `BufRead` pipelines, chunked fjall staging batches, cancellation, constrained compressed/zip input, Neo4j typed headers and ID spaces, duplicate/bad-row policy, schema/index application, deterministic reports, atomic promotion, and Neo4j-compatible CSV export.
   - Validated: deterministic import/export round trips, schema rollback, cancellation cleanup, relationship tolerance, archive safety, chunk/compression workloads, index-build timing, and cancellation-latency benchmarks.

10. **Complete: replace vector full scans with a real maintained HNSW path.**
   - Delivered behavior: engine-owned CPU HNSW uses dense normalized vectors, maintained mutation hooks, tombstones/compaction, cancellable ANN traversal, lifecycle status, checksummed greenfield persistence, direct topology restoration, and file-backed exact reranking. Exact cosine fallback normalizes once, uses SIMD scoring, and retains only bounded top-k results.
   - Preserved NornicDB behavior for passive reads/writes, explicit exact fallback, no query-triggered warming, file-backed exact candidates, and shutdown cancellation (`f065645b`, `214729d2`, `72876f17`, `31ce0546`, `ec0de01a`, `a10fe13a`, `53b4234b`, `b90574ef`, `2c27ec5f`).
   - Validated: deterministic lifecycle/restart/cancellation tests, production-profile 10k and 100k Criterion workloads, recall and resource gates, and correlated same-machine comparisons against current NornicDB.

11. **Complete: assemble the per-database embedding lifecycle.**
   - Delivered behavior: per-database configuration resolves into an engine-owned runtime with bounded workers, persistent pending work, retries/backoff, cache integration, startup/lazy loading, typed embedding state, readiness/status, explicit re-embedding/cancellation, and lifecycle-governed shutdown.
   - Dynamic-library symbol failures return typed errors, and backend status reports the actual CPU/GPU fallback path.
   - Validated: runtime lifecycle, queue drain/retry, cache, shutdown, status, and re-embedding tests.

12. **In progress: finish semantic and hybrid search.**
   - Delivered so far: local semantic search uses compatible declared maintained node vector indexes with bounded cancellable queries, minimum-score filtering, stable-ID deduplication, and deterministic ranking. Local hybrid search independently produces bounded BM25 and vector batches and combines them through shared deterministic RRF duplicate fusion.
   - Current gap: HTTP remains BM25-only; request index/label selection, optional query embedding, policy/decay suppression before pagination, diagnostics, caches, and production benchmarks remain incomplete.
    - Route BM25 and HNSW candidates through deterministic RRF, policy/decay filtering, hydration, pagination, and stage-level observability.
    - Keep automatic search/index/embedding work disabled by default per database. Schema-declared indexes still load, rebuild, and maintain regardless of that automatic-work gate.

13. **Replace synthetic operational status.**
    - Copper target: [crates/server/src/lib.rs](../crates/server/src/lib.rs).
    - Populate uptime, request/error counts, active connections, graph counts, storage bytes, embedding queue/worker state, index readiness, and search readiness from owning components.

14. **Complete cancellation propagation for local work.**
    - Promote the existing storage cancellation handle into an ingress-rooted request context.
    - Thread it through HTTP, Bolt, engine, eval traversal, scans, index rebuild, BM25/vector loops, embedding, and result materialization.
    - Cancellation is cooperative stop, not automatic rollback after a commit decision.

15. **Complete: make built-in function and procedure dispatch plugin-ready.**
   - Preserve the immutable registry boundary used by item 17's package loader and separate APOC/Heimdall packages.
    - Unknown procedure behavior remains deterministic and names the missing procedure.

### P2: Protocol And Operational Breadth

16. Expand OpenTelemetry/OpenMetrics exporters, runtime tracing, redaction, and live metric producers.
17. Implement versioned plugin packages and startup loading; verify the architecture with separate representative APOC and Heimdall packages before mechanically expanding either suite.
18. Implement MCP transport and production tools only after search/vector and auth are stable.
19. Expand Heimdall inference, link prediction, and reranking behavior after item 17 proves the package/action lifecycle and the benchmark-ready single-node core is stable.
20. Add GPU/SIMD acceleration only behind behaviorally identical CPU contracts and measured crossover thresholds.
21. Complete GraphQL traversal, pagination, mutation, error, database-selection, and auth behavior after item 17.

## Current Implementation Audit

Status meanings:

- **Operational baseline:** real implementation with focused consumers/tests, but not full NornicDB parity.
- **Partial:** meaningful code exists, with important behavior or runtime wiring missing.
- **Deferred:** intentionally outside the supported single-node runtime.

| Area | Current status | Verified strengths | Remaining parity boundary |
| --- | --- | --- | --- |
| Foundation: util, buildinfo, envutil, errors, lifecycle | Operational baseline | Shared helpers, versioning, config parsing, error vocabulary, supervision primitives | Broader consumer and operational defaults testing |
| Config and multidb | Operational baseline | Durable catalog, per-database overrides, effective resolution | Full key surface, auth precedence, warming/runtime consumers, CLI override model |
| Auth, audit, compliance, encryption, KMS | Operational baseline | Durable identities/policies/audit and encrypted storage paths | Default-on startup, Bolt identity, protocol-wide enforcement, rotation/cache performance |
| Storage records and indexes | Operational baseline | fjall records, namespace APIs, adjacency, property/range/temporal/fulltext maintenance, embedding queue | Atomic transaction boundary, event notifier, import/export, richer schema enforcement |
| Async storage | Operational baseline | Write-behind overlay, adaptive flush, cancellation, pending indexes, count race protection | Richer lifecycle/events and embedding worker composition |
| MVCC and WAL | Partial | Visible-at reads, reader leases, pruning/debt controls, WAL segments/replay/compaction baseline | Durable history contract, snapshot conflicts, repair/install/orchestration, namespace transactions |
| Cypher parser/eval | Operational baseline | Handwritten parser, broad expressions/clauses, path traversal, procedures, DDL, routed shapes, many upstream regressions | July regression closure, fulltext grammar, transaction-owned mutations, remaining optimized shapes |
| Bolt | Partial | Framing, PackStream, WebSocket/TCP, RUN and record streaming, datetime compatibility | Real auth, roles, transactions, pagination, bookmarks, summaries, error codes |
| HTTP/server/engine | Operational baseline | Engine execution, durable audit/compliance, admin routes, BM25 endpoint | Unified request context, true status, search/embedding lifecycle, identity propagation |
| Search/fulltext | Partial | Maintained token index, BM25 baseline, deterministic RRF types | Lucene-classic grammar, persistence lifecycle, semantic/hybrid execution, decay filtering |
| Vector/embedding/local LLM | Partial | Typed embedding fields, pending queue, GGUF/llama.cpp loading and cache components | Cache fix, worker composition, true HNSW, durable vector store, readiness/fallback correctness |
| Cache and pool | Operational baseline | Bounded reusable structures and query/result caching | Verify hot-path complexity and broader eval adoption |
| Temporal/decay/knowledge policy | Operational baseline | Policy persistence, scoring, ON ACCESS buffering, access metadata | Adaptive/Kalman behavior, computed mutations, search-time integration |
| Convert/admin import | Operational baseline | Typed Neo4j conversion, bounded staged import, schema/index rebuild, deterministic streaming export, CLI, and performance workloads | Broader Neo4j administrative format coverage outside the audited full-import/CSV-export contract |
| Observability | Partial | Metric catalog, validation, redaction/readiness foundations | Exporters, live ownership, protocol/search/storage instrumentation |
| GraphQL | Deferred to item 21 | Storage-backed baseline operations | Resume full schema/resolver/auth/pagination behavior after plugin package verification |
| MCP, Heimdall, inference, linkpredict | Partial/deferred | Contracts or narrow local algorithms exist | Production runtime intentionally after single-node core |
| GPU and SIMD | Deferred | Configuration and utility foundations | Measured vector/graph acceleration with CPU parity |
| Replication, fabric, topology, nornicgrpc, qdrantgrpc | Deferred | Placement, merge, transport, and topology contracts are substantial | No supported distributed runtime until deferred completion gates pass |

## Upstream Fix Ledger

This ledger prevents recent NornicDB fixes from disappearing into broad package labels.

| Upstream commit/family | Behavior | Copper disposition |
| --- | --- | --- |
| `36f2e532` | Retryable post-snapshot edge update conflict | P0, complete; storage, Cypher, and live Bolt regressions |
| `20215f13`, `1837c8c7` | Auth defaults on and explicit precedence | P0, complete |
| `e090df01` | Lucene-classic fulltext grammar | P0, complete; shared truth-table mirror, public node/relationship regressions, maintained postings, and Criterion benchmarks |
| `e4b84afe`, `883065cd` | Generalized OPTIONAL MATCH projection/aggregate behavior | P0, partial; exact tests required |
| `4f35ea92`, `8775bf1c` | Relationship existence/scoping and connected-node delete guard | P0, partial; guard proof missing |
| `b46ceb1f`, `389fb2e6` | Explicit-transaction UNWIND mutations and summary counters | P0/P1, blocked by real Bolt transactions |
| `98f6b4c1`, `2959060f` | Multi-MATCH relationship rebinding and IN-list index seeding | P0/P1, exact regressions missing |
| `ce1973e6` | Preserve property keys containing `:` | P1, unproven |
| `6362a50` | Remove unique-lock false sharing | P1, complete; key-granular node/relationship ownership, namespace isolation, and retirement-safe registries |
| `f065645b`, `214729d2`, `72876f17` | Vector readiness differs from serviceability; no query warming | P1, missing runtime |
| `31ce0546`, `ec0de01a`, `a10fe13a` | Passive vector behavior and opt-in CPU fallback | P1, missing strategy gate |
| `53b4234b` | Stop background vector work after shutdown | P1, pending runtime |
| `b90574ef`, `2c27ec5f` | Exact file-backed vector candidates supplement ANN | P1, pending vector store |
| `36e64ba9` | Prefix counts synchronize with async flush | Implemented in Rust; retain race test |
| `7809f54c`, `94732dd4`, `0e667ef5` | Namespace filtering and deterministic qualified-key splitting | Implemented for current Rust model |
| `c31669ba`, `4a6eca5f` | Chained MATCH/WITH and post-WITH predicates | Implemented; retain regressions |
| `2dd36705`, `95f6e14d` | LocalDateTime and Bolt 4.x datetime compatibility | Implemented; retain round trips |
| `20215f13` APOC file hardening | Import/export path containment and allowlists | APOC deferred; apply policy when any file procedure is introduced |

## Rust-Native Performance Contract

Parity is the behavioral floor, not an instruction to reproduce Go internals.

1. Use fjall batches and ordered keyspaces instead of emulating Badger APIs.
2. Keep current records and historical MVCC bodies in separate hot/cold paths.
3. Use maintained adjacency and property indexes; no whole-graph traversal for indexed shapes.
4. Use key-granular or sharded synchronization; avoid global locks and write-path full-map scans.
5. Bound all queues, caches, batches, fan-out, and background work.
6. Stream import/export and result production; do not materialize unbounded datasets.
7. Prefer zero-copy/borrowed decoding where ownership permits, but never expose storage buffers past their validity.
8. Use pooled rows and buffers only where benchmarks prove lower allocation cost.
9. Keep CPU behavior authoritative. SIMD/GPU paths must share result contracts and deterministic fallback.
10. Add Criterion or workload benchmarks for every performance-motivated divergence, with correctness tests run first.

The benchmark comparison must control dataset, schema, query results, cold/warm state, durability mode, concurrency, hardware, and process lifecycle. No benchmark-only code paths are allowed.

## Upstream Synchronization Process

NornicDB `main` is a moving compatibility target. Synchronization is a recurring engineering process, not an occasional rewrite.

1. Record the last audited NornicDB commit in this document and a machine-readable ledger to be added under `docs/parity/`.
2. Before each parity work cycle, update the local NornicDB `main` and inspect `last_audited..origin/main`.
3. Classify every changed upstream file as behavior, bug fix, performance, test-only, generated, deferred distributed, or non-applicable.
4. Map behavior and bug fixes to a Copper owner crate and test before implementation.
5. Port tests as behavior specifications; adapt implementation to Rust architecture.
6. Run the narrow crate tests, then dependent crate tests, then the workspace suite.
7. Benchmark only after correctness is green.
8. Advance the audited commit only when every changed file has a recorded disposition.

Recommended cadence:

- weekly change triage while upstream is active;
- a parity checkpoint for every NornicDB release tag;
- no more than one upstream release of unclassified behavior drift;
- a full package sweep before any copperDB parity or benchmark release claim.

## File-Level Audit Workstreams

The 1,674 upstream Go files must eventually receive one of four dispositions: implemented, merged into an equivalent Rust behavior, intentionally different with equivalent contract, or deferred/non-applicable with reason.

Execute workstreams in this order:

1. `config`, `auth`, `bolt`, `errors`, `txsession`, `storage` correctness and security.
2. `cypher`, `eval`, `filter`, and `indexing` parser/execution regression closure.
3. `adminimport`, `convert`, storage batching, and schema/index build.
4. `search`, `vectorspace`, `embed`, `embeddingutil`, `localllm`, and engine lifecycle.
5. `server`, observability, GraphQL, and protocol composition.
6. Deferred AI/governance packages after the benchmark-ready core.
7. Distributed packages only after the single-node completion gate.

Generated files should be dispositioned as generated outputs tied to their source schema, not audited line by line as handwritten behavior.

## Single-Node Completion Gate

copperDB may claim a benchmark-ready NornicDB parity milestone only when:

- all P0 findings are closed with tests;
- the selected upstream range has complete file-level disposition;
- Bolt authentication and explicit transactions use the same engine/auth contracts as HTTP;
- the Northwind import path is bounded, public, repeatable, and produces equivalent data;
- the agreed Northwind Cypher corpus returns equivalent rows, types, errors, and mutation statistics;
- schema-declared indexes rebuild and maintain correctly across restart;
- full-text and vector procedures use maintained indexes with explicit readiness states;
- MVCC/WAL durability behavior is documented and restart-tested;
- automatic indexing/search/embedding remains off by default while declared indexes remain active;
- audit, compliance, and per-database authorization apply at every supported ingress;
- the full Rust workspace suite is green with no ignored parity regressions;
- apples-to-apples cold/warm benchmarks record configuration and result equivalence.

## Deferred Distributed Gate

Distributed work remains deferred until the single-node gate passes. When resumed, the retained direction is:

- Cassandra/Dynamo-style quorum placement for data replication;
- a separate consensus-backed transaction-time oracle for authoritative distributed SI/RYOW timestamps;
- topology-owned placement plans;
- transport-neutral request envelopes with auth, trace, deadline, cancellation lineage, and read fences;
- fragment/shard execution through engine-owned APIs;
- deterministic merge and explicit partial/failure semantics;
- production gRPC transport behavior matching in-memory contract tests.

No distributed crate should be marked supported until quorum failures, restart recovery, auth/TLS, cancellation, snapshot fences, repair, and multi-node integration tests pass.

## Documentation Consolidation

This document supersedes the following documents for current priority, completion status, and next steps:

- [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md)
- [PARITY.md](PARITY.md)
- [plans/old/nornic-main-parity-checklist.md](plans/old/nornic-main-parity-checklist.md)
- [plans/old/nornicdb-dependency-graph.md](plans/old/nornicdb-dependency-graph.md)
- [plans/old/nornicdb-full-sweep-audit-2026-05-28.md](plans/old/nornicdb-full-sweep-audit-2026-05-28.md)
- [plans/old/storage-v0-implementation-plan.md](plans/old/storage-v0-implementation-plan.md)
- [plans/old/neo4j-parity-contract.md](plans/old/neo4j-parity-contract.md)

The following remain detailed deferred design references and are summarized, not replaced, by this plan:

- [plans/old/distributed-execution-architecture.md](plans/old/distributed-execution-architecture.md)
- [plans/old/federated-ai-fabric-architecture.md](plans/old/federated-ai-fabric-architecture.md)
- [plans/old/request-cancellation-propagation.md](plans/old/request-cancellation-propagation.md)
- [COPPERDB_STRATEGIC_ROADMAP.md](COPPERDB_STRATEGIC_ROADMAP.md)

When status changes, update this document first. Historical plans should not accumulate new completion claims.