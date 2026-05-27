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
- [ ] Knowledge policy schema hooks parity

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

### Still stubbed / not yet parity-complete
- [ ] `crates/storage`: full MVCC rebuild/orchestration parity with upstream beyond the reader-aware prune-now + lifecycle-status baseline.
- [ ] `crates/storage`: fuller WAL durability parity beyond the current persisted compaction/truncation baseline (e.g. richer snapshot install/orchestration and diagnostics wiring).
- [ ] `crates/storage`: WAL snapshot compaction/truncation orchestration parity with upstream.
- [ ] `crates/storage`: full schema contract bundles/type constraints/relationship cardinality enforcement parity with upstream.
- [ ] `crates/replication`, `crates/search`, `crates/fabric`: integration wiring to consume the new MVCC/WAL/schema baselines.

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
