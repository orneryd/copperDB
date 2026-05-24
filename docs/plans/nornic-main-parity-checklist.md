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
- [ ] MVCC version/head model parity
- [ ] Snapshot-visible read selectors parity
- [ ] MVCC pruning/rebuild lifecycle parity
- [ ] Snapshot reader registry parity
- [ ] Lifecycle debt/scheduling controller parity

### WAL and durability
- [ ] WAL engine wrapper parity
- [ ] WAL segmenting, diagnostics, degraded mode parity
- [ ] Snapshot + compaction orchestration parity
- [ ] Atomic record/batch replay parity

### Async engine
- [ ] Async write-behind cache parity
- [ ] Flush hold/flush result parity
- [ ] Async cache index maintenance parity
- [ ] Async count/read-path consistency parity

### Schema and constraints
- [ ] Constraint contracts (unique/existence/node-key/type/relationship) parity
- [ ] Constraint validation and namespaced validation parity
- [ ] Schema persistence/index catalog parity
- [ ] Knowledge policy schema hooks parity

### Indexing and query support
- [ ] Label/edge/property/range/temporal index parity
- [ ] Deindex enqueue/worker/cleanup parity
- [ ] Prefix/namespace stats parity
- [ ] Embedding pending-index parity

### Transaction model
- [ ] Badger transaction behavior parity (atomic commit/rollback semantics)
- [ ] Conflict handling/message parity
- [ ] Namespace-pin transaction semantics parity

### Data model and serialization
- [ ] Node/Edge model parity (metadata, embeddings, named embeddings)
- [ ] Property codec + serializer detection parity
- [ ] Neo4j import/export compatibility parity
- [ ] Storage event notifier parity

### Multi-database and routing
- [ ] Namespaced engine parity
- [ ] Composite engine/routing parity
- [ ] Remote engine adapter parity

### Migration policy in copper
- [x] Explicitly target **storage layout version 0 only** in copper
- [x] Do not add legacy migration arms or upgrade paths in copper storage v0 baseline

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
