# NornicDB Architecture Dependency Graph

Date: 2026-05-26

This graph is the package-porting order for copperDB. It follows NornicDB's package architecture while keeping Rust dependency cycles out of the workspace. Each layer may depend on earlier layers; later layers should not be required by earlier layers.

A checked package means the Rust package has a single-path implementation, full durable persistence for package-owned state, is threaded into its immediate consumers, and has focused contract tests. Unchecked packages may exist as scaffolds or partial ports but are not complete enough to call done.

## Layer 0: Shared Contracts And Process Foundation

These packages define vocabulary, errors, startup/shutdown behavior, and observability contracts. They must be implemented first because every other layer consumes them.

- [x] `util` -> shared helpers, deterministic ID hashing, bounded MessagePack decode.
- [x] `buildinfo` -> build/version metadata, product version, display version, server announcement.
- [x] `envutil` -> environment parsing helpers for strings, numbers, loose/strict booleans, durations.
- [x] `config` -> config files, env, CLI/default precedence and listener resolution.
- [x] `errors` -> Neo4j-compatible transient error codes, retry classification, conflict sentinels.
- [x] `otel` (`observability` in NornicDB) -> metrics catalog, runtime config, endpoint precedence, readiness checks, resource identity, redaction, mandatory fields, recovery.
- [x] `lifecycle` -> component supervisor, first-error cancellation, reverse-order shutdown, fresh shutdown budget.
- [x] `topology` -> hyperscaler placement, latency-aware distributed search fan-out, high-availability write planning contracts, and syscall-free distributed transaction ordering.

Current Rust status: all Layer 0 packages have focused implementations, active consumer wiring, and tests. `buildinfo` is consumed by startup logging and HTTP discovery/health/status surfaces. `envutil` is consumed by config env parsing and server auth/security defaults. `config` is consumed by binary startup for file/env/CLI listener precedence. `errors` is consumed by `txsession` retry classification. `lifecycle` supervises HTTP and Bolt startup, cancellation, and reverse-order shutdown in the binary process path.
`topology` also owns the distributed transaction timestamp contract: logical IDs are `(epoch, counter, node_ordinal)`, allocated by atomics without wall-clock syscalls, batch-reservable for multi-core writers, and mergeable from peer observations for distributed transaction ordering.

## Layer 1: Security, Identity, And Compliance

These packages depend on layer 0 and gate every externally reachable surface.

- [x] `kms` -> provider-backed data keys, local KEK wrapping, metadata, audit signing, provider factory.
- [x] `encryption` -> versioned envelopes, provider-backed `EnvelopeEncryptor`, DEK cache, rewrap rotation surface.
- [x] `auth` -> JWT/RBAC identity, persistent users, roles, allowlists, privileges, entitlements, token cache.
- [x] `audit` -> durable security and data-access event trail, append-only storage, hash-chain verification.
- [x] `security` -> token/header/URL validation, SSRF and injection defenses, server ingress enforcement.
- [x] `compliance` -> durable governance policies, access controls, retention markers, audit-backed HIPAA/SOC2 evidence.

Required direction: API surfaces call into this layer; this layer must not depend on HTTP/Bolt/GraphQL/MCP implementations.

Current Rust status: all Layer 1 packages are checked. `security` remains checked because protocol-neutral request validation is threaded into the HTTP server and owns no durable state. `kms` and `encryption` are checked because `StorageEngine::open_encrypted` now uses a provider-backed `EnvelopeEncryptor` to encrypt/decrypt node and edge record bytes, persists encryption metadata, rejects plaintext opens for encrypted stores, enforces key URI metadata, and is reachable from `CopperDb::open` through encrypted storage configuration. `auth` is checked because the HTTP server now bootstraps a durable storage-backed `Authenticator`, seeds/reloads the configured admin user, issues tokens through the persisted identity model, accepts bearer or cookie tokens, and enforces role database privileges before protected status/query/database routes. `audit` is checked because `CopperDb` now constructs a storage-backed `AuditLog`, records successful and failed embedded Cypher query operations as durable data-access events, exposes the log for verification, and has focused engine tests proving hash-chain verification and event persistence. `compliance` is checked because `CopperDb` now constructs a storage-backed `ComplianceManager`, enforces durable label/property policies against parsed queries using caller roles, exports HIPAA/SOC2 evidence from the durable audit trail, and the HTTP server passes authenticated roles into engine execution.

## Layer 2: Storage, Transactions, And Metadata

These packages own durable graph state and storage-adjacent state.

- [x] `storage` -> graph records, metadata catalogs, MVCC, WAL, schema, indexes, namespace primitives.
- [x] `cache` -> query/result/write-through caches.
- [x] `pool` -> reusable query-execution resource pooling.
- [x] `txsession` -> transaction/session lifecycle and conflict semantics.
- [x] `retention` -> retention policies, legal holds, erasure request model and sweeper hooks.
- [x] `multidb` -> logical database catalog and namespace routing.

Distributed foundation hooks in this layer:

- storage metadata persists topology-native hyperscaler profiles, mesh peers, placements, search policies, and HA write policy inputs.
- storage rebuilds a validated `TopologyRegistry` from durable metadata.
- transaction errors should route through `errors`.

Current Rust status: all Layer 2 packages are checked. `storage` is complete for its Layer 2 contract. It owns durable graph records, metadata catalogs, schema constraints and index definitions, label/property/edge indexes, namespace discovery, topology-native distributed metadata, encrypted record storage with manifest enforcement, MVCC snapshot/head helpers, and a persistent WAL with replay, checksum validation, partial-write/error contracts, segment stats, and reopen sequence continuity. It is consumed by engine/eval plus audit/auth/compliance policy surfaces. `cache` is complete for its Layer 2 contract. It owns no source-of-truth durable state; its package-owned state is bounded, reloadable acceleration state. The crate provides query-plan LRU caching, parameter-sensitive query-result caching with non-deterministic query rejection, write-through cache wrappers that update memory only after backing writes succeed, enable/disable controls, TTL expiration, explicit invalidation, eviction stats, and focused concurrency/contract tests. `pool` is complete for its Layer 2 contract. It owns no durable source-of-truth state; it provides reusable execution scratch pools for result row slices, evaluator binding rows, pooled nodes, byte buffers, string builders, maps, string slices, value slices, bounded retention, oversized-object rejection, disabled-mode behavior, clearing-on-return, and concurrency, and `eval` now consumes pooled binding-row vectors in MATCH/OPTIONAL MATCH/WHERE/DELETE/UNWIND row-processing paths. `txsession` is complete for package-owned transaction/session state: logical begin/commit timestamps from `topology`, pending write buffers, read-only enforcement, terminal state errors, explicit owner-bound sessions, TTL refresh/cleanup, terminal-error replay, and commit/rollback deletion. Its active sessions are runtime coordination state, not durable graph source-of-truth. `retention` is complete for its Layer 2 contract: policies, legal holds, and erasure requests persist through storage-backed records; active legal holds block erasure creation and retention deletion; the sweep path supports dry-run, batch limits, expiry checks, deletion, held-node reporting, and is wired into the server retention sweep endpoint. `multidb` is complete for its Layer 2 contract: the logical database catalog persists through storage-backed records, seeds system/default entries durably, create/open/drop paths consume the catalog, server startup opens the durable catalog, and engine opening uses registered database storage paths for namespace routing.

## Layer 3: Distributed Execution Foundation

These packages turn topology into cluster behavior. In this phase, seams must exist before full execution is enabled.

- [x] `replication` -> Cassandra-like coordinator writes/reads, replica fan-out transport, consistency-level quorum enforcement, repair seams.
- [x] `fabric` -> cluster routing, topology propagation, placement-aware execution.
- [x] `search` -> distributed search mesh planning, shard fan-out, peer selection.
- [x] `qdrantgrpc` -> external vector-store client and remote vector index offload.
- [x] `nornicgrpc` -> gRPC transport for internal/remote execution.

Current Rust status:

- `replication` consumes `topology` through `HighAvailabilityWritePlanner` and the coordinator-based `DynamoQuorum` contract documented in [docs/plans/distributed-execution-architecture.md](distributed-execution-architecture.md). It now has Cassandra-style coordinator write/read execution, in-memory replica transport, storage-backed replica adapter coverage, quorum failure behavior, failed-replica outputs, durable hinted handoff/read-repair queue records for post-quorum repair follow-up, a scheduled repair worker for background replay, and transport graph-read hooks that let higher layers traverse remote peer-backed graph partitions.
- `fabric` consumes `topology` through `FabricTopology` and exposes search, consistency-aware write, and consistency-aware read plans.
- `search` consumes `topology` through `DistributedSearchRouter` and now has a distributed executor seam for fan-out, failed-node tracking, and deterministic merge ordering.
- `qdrantgrpc` consumes `DistributedSearchPlan` to build vector-search request targets for external vector-store offload and now has a production Qdrant HTTP search client plus a distributed executor that fans out through the remote client seam, tracks failed targets, and merges hits deterministically.
- `nornicgrpc` consumes `DistributedWritePlan`, `DistributedReadPlan`, and `DistributedSearchPlan` to build internal remote execution envelopes without inventing alternate routing rules, and now exposes generated tonic/prost replica service bindings, a generated-client adapter, a generated-server adapter, and a `ReplicaTransport` adapter that maps coordinator write/read calls to target-addressed remote client requests.
- `engine` now loads durable topology from storage, exposes distributed read/write plan helpers, builds Cassandra coordinators from caller-provided replica transports, roots durable repair queues beside the database path or an explicit repair-queue path, replays repair batches through replica transports, builds scheduled repair workers, exposes explicit distributed Cypher execution that routes mutations through `DynamoQuorum` before local evaluation, and now has a real mesh-backed distributed BFS helper that traverses multiple peer storages and reconstructs node/edge paths for outgoing, incoming, and undirected traversal, with focused coverage for shortest-path selection, disconnected no-path results, and read-quorum failure. That traversal surface now also has an engine-level query-style materialization wrapper that returns `path`, `nodes(path)`, `relationships(path)`, and `length(path)` rows from the reconstructed mesh path while full Cypher path-variable syntax remains a Layer 4 parser/eval gap.
- `server` now lets HTTP Cypher and Neo4j transaction commit requests opt into distributed Cypher execution with `COPPERDB_DISTRIBUTED_CYPHER` or `x-copperdb-distributed`, using topology-derived placement and quorum consistency while keeping protocol handlers thin.
- `storage` persists and reloads the same topology contracts.
- Layer 3 is checked: package-owned durable state is persisted, runtime-only transport/executor state is explicit, immediate consumers compile, and focused distributed contract tests cover topology planning, coordinator quorum behavior, repair replay, protocol opt-in, nornic remote transport, and Qdrant vector-search offload.

## Enterprise Scaling Design

The hyperscaler, distributed search, and HA write layers are first-class architecture, not add-ons. The clean path is:

- `topology` owns all placement vocabulary: hyperscaler profile, mesh peer, health, capacity, observed RTT, placement, search routing policy, and write mode.
- `storage` persists topology-native records only and reconstructs a validated registry for boot and control-plane updates.
- `search` receives a `DistributedSearchPlan` from topology. The planner chooses candidates by same-region locality, observed latency, inflight load, capacity weight, health, fan-out cap, and hedge deadline.
- `fabric` routes by `PlacementKey`, never by protocol-specific IDs.
- `replication` receives `DistributedWritePlan` and `DistributedReadPlan` values from topology and enforces Cassandra-like coordinator semantics for `DynamoQuorum` writes and consistency-level reads.
- `txsession` receives begin/commit timestamps from topology's distributed logical transaction clock; storage/MVCC and replication must compare logical transaction IDs, not syscall timestamps or NTP-corrected wall time.

Latency strategy:

- prefer same-region peers when the request region is known;
- include degraded peers only for reads, never write leadership;
- score peers as observed RTT plus cross-region penalty plus load divided by capacity weight;
- execute fan-out with bounded parallelism from the plan;
- use hedged requests after the plan hedge deadline to cut p99 latency;
- keep transport pluggable so the fastest compatible Rust stack can be selected per protocol surface.

## Layer 4: Graph Query And Index Semantics

These packages define the language, graph algorithms, query evaluation, and index/search semantics.

- [ ] `cypher` -> parser, AST, Neo4j/Cypher compatibility grammar.
- [ ] `filter` -> predicate evaluation.
- [ ] `indexing` -> label/property/range/temporal/vector index catalog and maintenance.
- [ ] `eval` -> query execution engine over storage, indexes, tx, and policy hooks.
- [ ] `math`, `simd` -> numeric primitives.
- [ ] `embeddingutil`, `textchunk`, `embed`, `localllm`, `inference`, `vectorspace` -> embedding/vector/LLM stack.
- [ ] `decay`, `temporal`, `knowledgepolicy` -> time, memory decay, promotion/scoring, ON ACCESS policy runtime. `knowledgepolicy` is missing in Rust.
- [ ] `linkpredict` -> graph prediction algorithms over indexes/storage.

Required direction: `eval` may depend on query/storage/index/policy layers, but storage must not depend on eval.

Current Rust status: Layer 4 is in progress. `cypher`, `filter`, and `eval` now share a single parsed expression path for Cypher list and map literals: the parser accepts bracketed list expressions and `{key: expression}` map expressions, the predicate/expression evaluator resolves them into JSON arrays/objects, and `UNWIND [1, 2, 3] AS value RETURN value` plus map-import-style `UNWIND [{name: 'Ada'}] AS row RETURN row.name` execute through the normal evaluator row pipeline. The latest NornicDB parser bugfixes for `IN`, `NOT IN`, and regex `=~` predicates are represented in the Rust AST/evaluator. Pattern property maps in `MATCH`/`CREATE`/`MERGE` now also parse as expressions rather than literals only, so shapes like `MATCH (p:Product {productID: prodRef.productID})`, `UNWIND [1,2] AS id CREATE (:Order {orderID: id})`, and row-aware `MERGE` keys can be represented and evaluated against the current binding row instead of stopping at parser acceptance alone. `cypher` now also matches NornicDB's query-shape detector more closely by recognizing edge-property aggregation shapes (`RETURN p.name, avg(r.rating)`, strict mixed-property rejection, and return-shape validation) and by carrying dedicated compound-shape and pipeline-probe matchers with upstream-style capture/reject-reason coverage. `engine` and `eval` now consume those routes for real optimized execution slices: mutual-relationship, incoming/outgoing count aggregation, typed and untyped edge-property aggregation, the current compound create/delete relationship fast paths, and a dedicated pipeline executor for parser-approved `MATCH`/`CREATE`/`WITH`/`UNWIND`/`RETURN` clause chains all execute without falling through to the generic evaluator. That pipeline path now threads binding rows itself, reuses bound nodes across chained `CREATE` clauses, constrains subsequent relationship `MATCH` clauses to already-bound endpoints, executes the seeded `UNWIND ... MATCH (p {productID: prodRef.productID}) ... CREATE` shape end-to-end, and is now covered through both routed execution and direct executor invocation; relationship `OPTIONAL MATCH` now also supports relationship-pattern hit and miss cases with null-row preservation instead of hard-failing. The generic evaluator’s `CREATE` and `MERGE` clauses also resolve those expression-valued pattern properties per current row instead of evaluating them once globally. `WITH ... LIMIT` is also parsed on the handwritten parser path so benchmark-style compound and pipeline routes stay inside the normal engine/compliance flow. `MATCH p = ...` and `CREATE p = ...` path variables now parse on the handwritten path, and normal evaluation now binds/query-projects path values for node-only patterns and relationship traversals, including `RETURN p`, `nodes(p)`, `relationships(p)`, and `length(p)` over single-hop and variable-length BFS-backed matches plus relationship `CREATE` paths. `MATCH p = shortestPath((...)-[:TYPE*]->(...))` now also parses on the handwritten path and executes through the BFS substrate as a single shortest-path result rather than returning every reachable path. On the distributed engine path, `execute_distributed_as(...)` now routes six path-query slices through remote peer graph reads: shortest-path query shapes for both `_id`-anchored and label/property-selected endpoints, node-only path-variable `MATCH`, direct single-hop path-variable `MATCH` with literal edge-property filters, non-shortest variable-length path-variable `MATCH` with literal selectors and literal edge filters, standalone `OPTIONAL MATCH p = ... RETURN ...` path queries for the same direct-path subset, and `MATCH (...) OPTIONAL MATCH p = ... RETURN ...` when the leading `MATCH` clauses are a prefix of simple node selectors or relationship matches, including exact single-hop, variable-length start/end bindings, and routed `WHERE` filters over the bound prefix rows. Those routes materialize query-visible `p`, `nodes(p)`, `relationships(p)`, and `length(p)` results from remote peer storage, and `distributed_bfs_query_as(...)` reuses the same full path-object contract. Distributed variable-length path routing now mirrors the local evaluator’s BFS visitation rule by deduplicating on `(node_id, depth)` rather than attempting to enumerate every possible path permutation, and distributed optional path projection now matches local null semantics: `p` is `null`, `length(p)` is `null`, and `nodes(p)` / `relationships(p)` become empty arrays on misses, including the leading-`MATCH` route when the optional path depends on the bound leading node variable or follows mixed simple node, relationship, and `WHERE`-filtered prefixes. Distributed endpoint selection now prefers direct `_id` fetches when available and otherwise uses peer property lookups with label-scan fallback before selecting candidate nodes. The distributed edge-read helper also now tolerates partial replica failures in undirected (`Both`) fetches instead of aborting the whole path materialization on the first missing peer. The NornicDB shortest-path/BFS optimization has been pulled into the storage/query foundation by maintaining durable start-node and end-node edge adjacency indexes, including type-filtered lookups, and `eval` now executes single-hop plus variable-length outgoing, incoming, and undirected relationship `MATCH` through adjacency-backed BFS, including larger-chain consistency scenarios beyond the earlier small-graph coverage. These packages remain unchecked until broader Neo4j/Cypher compatibility, broader distributed row-aware routed reads beyond `MATCH`/`WHERE` prefix shapes, broader pipeline-shape parity, broader compound-query execution parity, index maintenance, and query execution contracts are complete.

## Layer 5: Core Engine Composition

This is the embedded database facade.

- [ ] NornicDB: `pkg/nornicdb`.
- [ ] copperDB: `crates/engine` / package `copperdb-engine`.

The engine should compose storage, transactions, cache, eval, auth context, audit/compliance hooks, replication/fabric routing, retention checks, knowledge policy runtime, and telemetry. Today it composes storage, parser, eval, transaction manager handle, and query cache only.

## Layer 6: Protocols And User-Facing APIs

These packages should be thin protocol adapters over the engine/query pipeline.

- [ ] `server` -> HTTP/REST/UI surface.
- [ ] `bolt` -> Neo4j Bolt state machine and PackStream transport.
- [ ] `graphql` -> GraphQL schema and resolvers.
- [ ] `mcp` -> Model Context Protocol tools and transport.
- [ ] `heimdall` -> governance/admin/security control plane.
- [ ] `convert` -> import/export/conversion tools.
- [ ] `copperdb` -> executable binary and component assembly.

Required direction: protocol packages should not implement storage/query semantics directly. They should authenticate, decode protocol input, call the engine/query pipeline, encode responses, and emit telemetry.

## Implementation Walk Order

1. Finish layer 0 contracts and keep `topology` as the single distributed contract path.
2. Complete layer 1 durable security packages before calling protocol surfaces complete: `audit`, `security`, then `compliance`.
3. Expand layer 2 storage and transaction metadata only when the package being implemented owns durable state there.
4. Keep layer 3 distributed execution as real planning and persistence contracts first; do not check a package until its state is durable and its immediate consumers compile against it.
5. Port layer 4 package behavior one package at a time, starting with `cypher`, `filter`, `indexing`, `eval`, then `knowledgepolicy`.
6. Thread layer 5 engine through auth, audit, compliance, retention, replication/fabric, search/index, and telemetry.
7. Complete layer 6 protocol adapters after the central engine pipeline is stable.

## Foundational Distributed Contracts

The following contracts are intentionally first-class even while distributed execution remains disabled:

- Hyperscaler profiles: provider, region, zones, tier, metadata.
- Mesh peers: node id, address, region/zone, capabilities, health, heartbeat, observed RTT, inflight load, capacity weight.
- Placements: tenant/database/shard, primary, replicas, search nodes, hyperscaler profile.
- Distributed search plans: placement plus latency-ranked healthy search fan-out, bounded parallelism, and hedge deadline.
- Distributed write plans: placement plus mode, consistency level, coordinator, replicas, and required acknowledgements.
- Distributed read plans: placement plus consistency level, coordinator, replicas, and required responses.
- Distributed transaction IDs: epoch, logical counter, node ordinal; no wall-clock dependency on the write hot path.

These contracts are implemented in `copperdb-topology`, persisted by `storage` where they are durable topology metadata, and consumed by `fabric`, `search`, `replication`, and `txsession`.
