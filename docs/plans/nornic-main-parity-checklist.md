# NornicDB Main Parity Checklist (Storage-Centric)

## Upstream reference
- Repository: `https://github.com/orneryd/NornicDB`
- Branch: `main`
- Latest inspected commit: `fd37a21e9694c5739b6afe2e6d78a4225b55c981`

## Major storage architecture/features detected upstream

### Core engine and API surface
- [ ] Engine interface parity (node/edge CRUD, traversal, schema, bulk ops, stats)
- [ ] Optional extension interfaces parity (prefix stats, adjacent edges, namespace schema provider)
- [ ] Streaming APIs parity (stream nodes/edges/chunks)
- [ ] Prefix delete / namespace deletion parity

### MVCC and lifecycle
- [x] MVCC version/head model parity (baseline in `crates/storage::MvccStore`)
- [x] Snapshot-visible read selectors parity (snapshot timestamp reads in `MvccStore::read`)
- [ ] MVCC pruning/rebuild lifecycle parity
- [x] Snapshot reader registry parity (tracked snapshot leases now pin the oldest active reader and pruning respects that floor)
- [x] Lifecycle debt/scheduling controller baseline parity (lifecycle status now reports active readers, retained-version debt, and safe prune-now behavior)

### WAL and durability
- [x] WAL engine wrapper parity (baseline in `crates/storage::WAL`)
- [x] WAL segmenting, diagnostics, degraded mode baseline parity (segment stats, corruption/degraded signaling, and persisted compaction-truncation baseline)
- [x] Snapshot + compaction orchestration baseline parity (WAL can now compact/truncate through a persisted sequence boundary while preserving replay order and next sequence across reopen)
- [x] Atomic record/batch replay parity (baseline batch append/replay in `WAL::append_batch/replay_after`)

### Async engine
- [ ] Async write-behind cache parity
- [ ] Flush hold/flush result parity
- [ ] Async cache index maintenance parity
- [ ] Async count/read-path consistency parity

### Schema and constraints
- [x] Constraint contracts (unique/existence/node-key/type/relationship) parity (baseline types in `ConstraintType`)
- [x] Constraint validation and namespaced validation parity (baseline unique/exists/node-key checks in `SchemaManager`)
- [x] Schema persistence/index catalog parity (baseline persistence in `StorageEngine::persist_constraint/load_constraints`)
- [x] Knowledge policy schema hooks parity (storage now persists decay bindings, promotion-policy targets, typed `ON ACCESS` mutations, and per-entity access metadata)

### Indexing and query support
- [ ] Label/edge/property/range/temporal index parity
- [ ] Deindex enqueue/worker/cleanup parity
- [ ] Prefix/namespace stats parity
- [ ] Embedding pending-index parity

### Transaction model
- [ ] Badger transaction behavior parity (atomic commit/rollback semantics)
- [x] Conflict handling/message parity (baseline NornicDB-compatible error messages in `crates/txsession`)
- [ ] Namespace-pin transaction semantics parity

## Detailed parity status (implemented vs stubbed)

### Implemented baselines
- [x] `crates/txsession`: transaction lifecycle/error surface updated to include NornicDB messages (`no active transaction`, `transaction already closed`, `transaction rolled back`) with error-path tests.
- [x] `crates/storage`: MVCC snapshot isolation primitives (`MvccStore`, `MvccSnapshot`, head encode/decode) with pruning + error-path tests.
- [x] `crates/storage`: WAL primitives (`WAL`, `WALEntry`, `WALSegment`) including batch append/replay, checksum verification, degraded-mode signaling, and close/error-path tests.
- [x] `crates/storage`: schema primitives (`SchemaManager`, `Constraint`) including unique/existence/node-key validation and persistent catalog round-trip tests.
- [x] `crates/storage`: knowledge policy metadata hooks for decay bindings, promotion-policy target catalogs, typed `ON ACCESS` metadata mutations, and separate per-entity access metadata persistence with deterministic target guards.
- [x] `crates/knowledgepolicy` + `crates/eval`: shared access-flusher buffering/flush-on-success semantics, compiled promotion `WHEN` predicates, and score-time promotion multiplier/floor/cap application against persisted plus buffered access metadata for node and edge visibility.
- [x] `crates/eval`: `CALL nornicdb.knowledgepolicy.resolve(...)` now exposes local target resolution, matched promotion profile/predicate inspection, score inputs, final score, and suppression state with focused deterministic regressions for entity-backed and dry-run label-backed resolution.
- [x] `crates/replication`: replica storage/transport now exposes a read-only graph access-metadata seam so routed score parity can consume remote access counters/timestamps from storage instead of guessing from payload bytes.
- [x] `crates/replication` + `crates/engine`: distributed graph node materialization now carries `_created_at_unix_ms` and `_updated_at_unix_ms` through replica payloads, with a focused engine regression proving those anchors survive routed path materialization.
- [x] `crates/nornicgrpc`: build-time protobuf generation no longer depends on a machine-level `protoc`; the crate now uses a vendored compiler so Windows compile checks can run in-place.
- [x] `crates/engine` + `crates/eval`: routed distributed path queries now reuse the shared knowledge-policy scorer for remote node candidate resolution and edge traversal, with deterministic stale-node and stale-edge regressions.
- [x] `crates/replication`: replica commands now include a dedicated knowledge-policy access-metadata upsert primitive, validated through the storage-backed replica adapter.
- [x] `crates/engine` + `crates/replication`: distributed reads now flush remote `ON ACCESS` metadata updates through the replicated access-metadata command, with deterministic engine regressions for node and edge metadata persistence.
- [x] `crates/eval`: generic relationship `MATCH` execution now supports fixed-length linear multi-hop chains with deterministic path-variable assertions for `nodes(p)`, `relationships(p)`, and `length(p)`.
- [x] `crates/cypher` + `crates/eval`: comma-separated pattern segments now retain AST boundaries so disconnected relationship `MATCH` groups and comma-separated relationship `CREATE` groups execute without cross-segment misbinding.
- [x] `crates/eval`: the generalized linear relationship matcher now also covers mixed fixed/variable-length chains and multi-edge `shortestPath(...)` execution with deterministic path assertions.
- [x] `crates/cypher` + `crates/eval`: routed pipeline execution now accepts `OPTIONAL MATCH` after `WITH` and preserves null bindings instead of falling back out of the pipeline route.
- [x] `crates/cypher` + `crates/eval`: routed pipeline execution now accepts `DELETE` after `WITH`, and eval’s shared delete path correctly distinguishes node vs relationship bindings when applying deletions.
- [x] `crates/cypher` + `crates/eval`: routed pipeline execution now accepts `SET` after `WITH`, and eval’s shared set path correctly persists relationship property updates instead of only rewriting node bindings.
- [x] `crates/cypher` + `crates/eval`: full `REMOVE` support now exists across parser, eval, and routed pipeline execution for relationship property removal and node label removal.

### Still stubbed / not yet parity-complete
- [ ] `crates/storage`: full MVCC rebuild/orchestration parity with upstream beyond the reader-aware prune-now + lifecycle-status baseline.
- [ ] `crates/storage`: fuller WAL durability parity beyond the current persisted compaction/truncation baseline (e.g. richer snapshot install/orchestration and diagnostics wiring).
- [ ] `crates/storage`: WAL snapshot compaction/truncation orchestration parity with upstream.
- [ ] `crates/storage`: full schema contract bundles/type constraints/relationship cardinality enforcement parity with upstream.
- [ ] `crates/replication`, `crates/search`, `crates/fabric`: integration wiring to consume the new MVCC/WAL/schema baselines.
- [x] `crates/engine` + `crates/replication`: distributed knowledge-policy parity for routed/special-path reads now includes shared scorer reuse plus honest remote `ON ACCESS` persistence through coordinator writes.

### Data model and serialization
- [ ] Node/Edge model parity (metadata, embeddings, named embeddings)
- [ ] Property codec + serializer detection parity
- [ ] Neo4j import/export compatibility parity
- [ ] Storage event notifier parity

### Multi-database and routing
- [ ] Namespaced engine parity
- [ ] Composite engine/routing parity
- [ ] Remote engine adapter parity

### Migration policy in Copper
- [x] Explicitly target **storage layout version 0 only** in Copper
- [x] Do not add legacy migration arms or upgrade paths in Copper storage v0 baseline

## Distributed search mesh + hyperscaler baseline requirements
- [ ] Cluster search peer registry in storage metadata
- [ ] Placement metadata for shard/tenant routing
- [ ] Health/heartbeat tracking for mesh nodes
- [ ] Hyperscaler profile metadata (AWS/Azure/GCP/local)
- [ ] Contract hooks for search crate + fabric/replication integration

## Copper crates needing updates/stubs due storage parity
- [ ] `crates/storage` — storage v0 baseline, layout contract, metadata registries, deterministic tests
- [ ] `crates/search` — distributed mesh routing, shard fan-out, peer selection integration
- [ ] `crates/fabric` — cluster routing + mesh topology propagation
- [ ] `crates/replication` — WAL/MVCC parity integration points
- [ ] `crates/multidb` — namespace-aware storage APIs
- [ ] `crates/eval` — richer storage APIs (label/edge/index paths)
- [ ] `crates/temporal` — temporal lookup and pruning hooks
- [ ] `crates/indexing` — index catalog/property index maintenance hooks
- [ ] `crates/txsession` — transaction lifecycle integration
- [ ] `crates/server` + `crates/graphql` — API surfacing for mesh/storage controls

## Test parity expectations
- [ ] Deterministic test fixtures
- [ ] Deep assertions for index/state transitions
- [ ] Coverage target for storage crate: >= 90%
- [ ] Regression tests for flush/count/async/WAL edge cases
