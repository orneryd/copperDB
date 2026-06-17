# NornicDB Architecture Dependency Graph

Date: 2026-05-26

This graph is the package-porting order for copperDB. It follows NornicDB's package architecture while keeping Rust dependency cycles out of the workspace. Each layer may depend on earlier layers; later layers should not be required by earlier layers.

Documentation status note: copperDB's supported runtime architecture is currently single-node only. Any distributed, fabric, replication, remote search, or cross-node transaction material in this file is parity backlog and future architecture guidance, not a shipped/runtime guarantee.

Audit note: the full local on-disk package comparison against NornicDB is tracked in [nornicdb-full-sweep-audit-2026-05-28.md](nornicdb-full-sweep-audit-2026-05-28.md), including the full agent findings register. That audit calls out architecture/performance drift that should guide future implementation order without automatically changing the checked status of this dependency graph.

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


Distributed foundation hooks in this layer:
- storage rebuilds a validated `TopologyRegistry` from durable metadata.
- transaction errors should route through `errors`.

Current Rust status: all Layer 2 packages are checked. `storage` is complete for its Layer 2 contract. It owns durable graph records, metadata catalogs, schema constraints and index definitions, dedicated namespace-scoped schema snapshots, maintained namespace prefix and label-cardinality stats, a bounded namespaced wrapper baseline, callback-based node/edge/chunk streaming, prefix deletion with namespace metadata cleanup, label/property/edge indexes, namespace discovery, topology-native distributed metadata, encrypted record storage with manifest enforcement, MVCC snapshot/head helpers, a Nornic-style head-plus-archive MVCC split that keeps current reads pinned to per-key heads while historical versions stay off the hot index path, retained-floor anchored pruning with focused churn/ghost-chain regressions, a bounded lifecycle pause/resume/schedule/debt-inspection control surface, typed snapshot-visible node or edge plus label or edge-type reads that use current-only indexes plus historical directories instead of collapsing to current-state-only answers, real `StorageEngine` ownership of that `MvccStore` path so opens and structured writes bootstrap and mirror into MVCC automatically, a guarded `rebuild_mvcc_from_current_state()` control that can clear and replay MVCC state from persisted current rows when no active readers are registered, `NamespacedStorageEngine` delegation of the visible-at and lifecycle controls through the real storage surface, the narrower `NamespacedMvccStore` wrapper for direct MVCC-store callers, and a persistent WAL with replay, checksum validation, partial-write/error contracts, segment stats, and reopen sequence continuity. It is consumed by engine/eval plus audit/auth/compliance policy surfaces, and focused engine regressions now prove the snapshot-visible storage path is reachable through `db.storage()`. `cache` is complete for its Layer 2 contract. It owns no source-of-truth durable state; its package-owned state is bounded, reloadable acceleration state. The crate provides query-plan LRU caching, parameter-sensitive query-result caching with non-deterministic query rejection, write-through cache wrappers that update memory only after backing writes succeed, enable/disable controls, TTL expiration, explicit invalidation, eviction stats, and focused concurrency/contract tests. `pool` is complete for its Layer 2 contract. It owns no durable source-of-truth state; it provides reusable execution scratch pools for result row slices, evaluator binding rows, pooled nodes, byte buffers, string builders, maps, string slices, value slices, bounded retention, oversized-object rejection, disabled-mode behavior, clearing-on-return, and concurrency, and `eval` now consumes pooled binding-row vectors in MATCH/OPTIONAL MATCH/WHERE/DELETE/UNWIND row-processing paths. `txsession` is complete for package-owned transaction/session state: logical begin/commit timestamps from `topology`, pending write buffers, read-only enforcement, terminal state errors, explicit owner-bound sessions, TTL refresh/cleanup, terminal-error replay, and commit/rollback deletion. Its active sessions are runtime coordination state, not durable graph source-of-truth. `retention` is complete for its Layer 2 contract: policies, legal holds, and erasure requests persist through storage-backed records; active legal holds block erasure creation and retention deletion; the sweep path supports dry-run, batch limits, expiry checks, deletion, held-node reporting, and is wired into the server retention sweep endpoint. `multidb` is complete for its Layer 2 contract: the logical database catalog persists through storage-backed records, now uses the namespaced storage wrapper for its durable catalog rows, seeds system/default entries durably, create/open/drop paths consume the catalog, server startup opens the durable catalog, and engine opening uses registered database storage paths for namespace routing.

## Layer 3: Distributed Execution Foundation

These packages are deferred roadmap contracts for future cluster behavior. copperDB does not currently expose a supported distributed runtime, even where partial scaffolding or parity experiments exist in the workspace.

- [ ] `replication` -> future Cassandra-like coordinator writes/reads, replica fan-out transport, consistency-level quorum enforcement, repair seams.
- [ ] `fabric` -> future cluster routing, topology propagation, placement-aware execution.
- [ ] `search` -> future distributed search mesh planning, shard fan-out, peer selection.
- [ ] `qdrantgrpc` -> future external vector-store client and remote vector index offload.
- [ ] `nornicgrpc` -> future gRPC transport for internal/remote execution.

Current Rust status:

Layer 3 remains deferred. Some crates contain topology vocabulary, transport scaffolding, parity experiments, or partial remote execution code paths, but none of that should be documented as a supported runtime. The active architecture and product guarantee remain single-node.

## Enterprise Scaling Design

This section is forward-looking only and is intentionally deferred until after the single-node architecture is complete.

The hyperscaler, distributed search, and HA write layers are first-class architecture, not add-ons. The clean path is:

- `topology` owns all placement vocabulary: hyperscaler profile, mesh peer, health, capacity, observed RTT, placement, search routing policy, and write mode.
- `storage` persists topology-native records only and reconstructs a validated registry for boot and control-plane updates.
- `search` receives a `DistributedSearchPlan` from topology. The planner chooses candidates by same-region locality, observed latency, inflight load, capacity weight, health, fan-out cap, and hedge deadline.
- `fabric` routes by `PlacementKey`, never by protocol-specific IDs.
- `replication` receives `DistributedWritePlan` and `DistributedReadPlan` values from topology and enforces Cassandra-like coordinator semantics for `DynamoQuorum` writes and consistency-level reads.
- `txsession` receives begin/commit timestamps through topology's transaction-time oracle seam; the local distributed logical transaction clock remains the default allocator and merge helper today, but the target distributed SI/RYOW path is a consensus-backed oracle while storage/MVCC and replication continue to avoid syscall timestamps or NTP-corrected wall time.

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
- [ ] `decay`, `temporal`, `knowledgepolicy` -> time, memory decay, promotion/scoring, ON ACCESS policy runtime.
- [ ] `linkpredict` -> graph prediction algorithms over indexes/storage.

Required direction: `eval` may depend on query/storage/index/policy layers, but storage must not depend on eval.

Current Rust status: Layer 4 is in progress. `cypher`, `filter`, and `eval` now share a single parsed expression path for Cypher list and map literals; pattern property maps in `MATCH`/`CREATE`/`MERGE` also evaluate as expressions against the current row, so literal, row-aware, and `UNWIND`-driven shapes execute through the same evaluator path instead of stopping at parser acceptance. The parser and handwritten validator now also retain comma-separated pattern segment boundaries in the AST, which lets `eval` execute disconnected relationship-pattern `MATCH` groups and comma-separated relationship `CREATE` groups without cross-segment misbinding. For future distributed consistency, the retained architecture direction is explicit: keep Dynamo-quorum replication for data placement writes and reads, but move authoritative distributed transaction times and read fences onto a separate consensus-backed oracle so MVCC snapshot isolation and RYOW do not depend on purely local clock allocation.

`cypher` now matches NornicDB’s query-shape detector more closely for edge-property aggregation, compound-shape probing, pipeline probing, and the narrow simple `MATCH ... LIMIT` detector, while `engine` and `eval` consume those routes for real optimized slices: single-node label-backed `MATCH (n:Label) RETURN n LIMIT k`, mutual-relationship, incoming/outgoing count aggregation, typed and untyped edge-property aggregation, current compound create/delete relationship fast paths, and parser-approved pipeline chains. That routed pipeline path now threads binding rows itself, reuses bound nodes across chained `CREATE` clauses, constrains later relationship `MATCH` clauses to already-bound endpoints, supports routed `OPTIONAL MATCH` with correct null preservation, supports routed `MERGE`, `DELETE`, `SET`, and `REMOVE` after `WITH` or `UNWIND` without dropping out of the pipeline route, and covers both the seeded `UNWIND ... MATCH ... CREATE` seeder shape and a basic `UNWIND ... MERGE ... RETURN` batch-upsert shape through routed execution. Eval now also owns per-engine hot-path trace state for these narrow routes, with deterministic assertions on the simple `MATCH ... LIMIT`, current compound create/delete relationship fast path, node `MERGE` schema-lookup versus scan-fallback behavior, seeded `UNWIND ... MATCH ... CREATE`, and basic `UNWIND ... MERGE ... RETURN` trace flags. Eval’s shared mutation paths now distinguish node and relationship bindings for `DELETE`, persist relationship property updates for `SET`, and remove relationship properties plus node labels correctly for `REMOVE`.

The next cypher plan anchor is the upstream `pkg/cypher` wave from `2503ffca..f1fb4beb`. That wave adds 152 changed cypher files, including parser-helper coverage, CALL-tail parsing and execution, fulltext third-argument signature enforcement, vector-cosine query-shape coverage, Graphiti scenarios, broad bug regressions, and a large schema-contract expansion. The Rust work order should therefore stay iterative: parser-owned tests into `crates/cypher` first, row-pipeline and procedure behavior into `crates/eval` second, vector-cosine and Graphiti scenarios into consuming runtime crates third, and schema-query-shape parity fourth. Do not call the cypher slice complete until every changed upstream test file in that wave is mirrored, merged, or explicitly dispositioned.

Path semantics are materially broader than before: `MATCH p = ...` and `CREATE p = ...` path variables parse on the handwritten path, normal evaluation now binds `p`, `nodes(p)`, `relationships(p)`, and `length(p)` for node-only patterns and relationship traversals, and the generalized linear relationship matcher now covers fixed-length multi-hop chains, variable-length edges at any position in a linear chain, and multi-edge `shortestPath(...)` patterns. `MATCH p = shortestPath((...)-[:TYPE*]->(...))` executes as a single shortest-path result rather than enumerating every reachable path.

The Rust `knowledgepolicy` port is no longer missing: Copper persists decay bindings, promotion policies, and per-entity access metadata; promotion `WHEN` clauses compile into parsed expressions up front; `knowledgepolicy` owns the shared `ON ACCESS` flusher/runtime boundary; and eval applies promotion multiplier/floor/cap math against persisted plus buffered access metadata for node and edge visibility. `CALL nornicdb.knowledgepolicy.resolve(...)` exposes the same local scoring and target-resolution path as a deterministic inspection surface.

Retained future-state distributed scaffolding currently present in the workspace includes `execute_distributed_as(...)` routes for path-query slices through remote peer graph reads for shortest-path queries, node-only path-variable `MATCH`, direct single-hop path-variable `MATCH`, non-shortest variable-length path-variable `MATCH`, standalone `OPTIONAL MATCH p = ... RETURN ...`, and routed path reads behind a bounded leading-clause subset. Those routes materialize query-visible path objects from remote peer storage, reuse the same node/edge knowledge-policy scoring as local reads for visibility suppression, persist successful remote `ON ACCESS` mutations through the replicated access-metadata write primitive, prefer direct `_id` fetches before property/label fallback during endpoint selection, and tolerate partial replica failures for undirected edge reads. This remains future-state scaffolding only and does not change the current single-node support guarantee.

These packages remain unchecked until broader Neo4j/Cypher compatibility, broader single-node pipeline-shape parity beyond the currently routed clause mix, broader compound-query execution parity, index maintenance, query execution contracts, and any future distributed row-aware routed reads are complete.

Audit delta: `indexing` parity now needs to be tracked in two lanes. Property-backed exact, RANGE, and TEMPORAL indexes are actively moving through storage/indexing/eval parity. `FULLTEXT` and `VECTOR` are still not at NornicDB runtime parity, but copperDB now has a first maintained local fulltext runtime baseline: storage persists FULLTEXT catalog rows and maintains inverted-token entries for node properties, and `copperdb-engine` consumes that storage-backed local fulltext path under the per-database search gate. Cypher schema DDL remains the authoritative source of per-database index definitions: when a database declares property, RANGE, TEMPORAL, FULLTEXT, or VECTOR indexes, those declared indexes should still load, rebuild, and maintain for that database. Automatic search/index/embedding work must default disabled in copperDB and only refer to extra implicit/background indexing beyond schema DDL, while an operator CLI flag remains the hard global kill switch for all indexing work.

Index maintenance is incrementally broader now: storage rebuilds, updates, and drops maintained node-property index state for both single-property and composite node index definitions, maintains single-property and composite relationship-property indexes keyed by relationship type, and the indexing catalog prefers the most specific matching node or relationship index definition during lookup instead of treating richer definitions as catalog-only metadata.

Index catalog metadata is also closer to current Cypher DDL expectations now: `IndexDefinition` carries an explicit index kind, generic `CREATE INDEX` currently materializes as `RANGE`, explicit `CREATE RANGE INDEX` plus explicit `CREATE TEMPORAL|FULLTEXT|VECTOR INDEX` now persist their typed catalog rows, and `SHOW INDEXES` exposes kind instead of collapsing every property-backed definition into an untyped bucket. The query surface can also filter stored index metadata through `SHOW RANGE INDEXES`, `SHOW TEMPORAL INDEXES`, `SHOW FULLTEXT INDEXES`, and `SHOW VECTOR INDEXES` without inventing alternate catalog rows. Those typed DDL forms now also follow the same duplicate-name, `IF NOT EXISTS`, drop-by-name, and `IF EXISTS` contract as the older generic/RANGE path. That ordered-comparison path is no longer RANGE-only either: current Cypher DDL now accepts both node `FOR (n:Label) ON (n.prop)` and relationship `FOR ()-[r:TYPE]-() ON (r.prop)` RANGE or TEMPORAL index targets, and storage/indexing can use maintained single-property and composite node or relationship RANGE and TEMPORAL indexes to narrow simple `<op>` comparisons before eval applies the normal predicate filter. Single-property ordered keys are now encoded in an order-preserving storage form for strings and numbers, so those node and relationship comparison reads run as bounded sled range scans instead of scanning an entire property prefix and filtering every indexed value in memory. Maintained composite ordered-comparison indexes now participate in the same current-state path when the compared property is either the leading indexed property or a later indexed property whose preceding indexed fields are all constrained by exact predicates; exact suffix predicates are no longer required for the scan itself, but when they are present the catalog prefers the composite definition with the most matching exact fields and storage filters those exact properties deterministically. `FULLTEXT` and `VECTOR` definitions are catalog-visible DDL metadata only at this stage; storage persists them without rebuilding or maintaining property-backed lookup state, and exact-match and ordered-comparison lookup selection intentionally exclude those kinds until they have real maintained runtime paths.

Search/vector audit status: NornicDB's `pkg/search` includes BM25/fulltext indexes, HNSW, IVFPQ, vector file storage, index persistence/versioning, decay filtering, reranking, hybrid cluster routing, GPU acceleration, and search observability. copperDB's current search layer is not yet at that runtime parity, but the engine now has a first maintained storage-backed local fulltext runtime plus engine-native local fabric ranked-search batch and hydration helpers that can feed the internal transport seam without rebuilding query-time indexes. The next search/index slices should prioritize improving that local baseline toward CPU BM25/fulltext parity, then wiring the real engine-backed local ranked-search handler through `nornicgrpc`/server runtime assembly before adding CPU vector runtime, vector file storage, and local in-memory embedding execution. GPU acceleration, reranking, and the broader inference lifecycle are deferred out of the first runnable MVP.

Layer 4 delta: `knowledgepolicy` now owns the shared `ON ACCESS` flusher/runtime boundary rather than eval-local buffering, promotion `WHEN` clauses compile into parsed expressions up front, eval now applies promotion multiplier/floor/cap math against persisted plus buffered access metadata when deciding node and edge visibility, and `CALL nornicdb.knowledgepolicy.resolve(...)` exposes the same local scoring/target-resolution path as a tested inspection surface.

Retained Layer 4 distributed-scaffolding status: routed/special-path remote-read experiments reuse the same node/edge scoring path as local evaluation for visibility suppression, and successful remote reads persist `ON ACCESS` mutations through the replicated access-metadata write primitive with coordinator semantics. This is audit context for future distributed work, not a current supported runtime claim.

## Layer 5: Core Engine Composition

This is the embedded database facade.

- [ ] NornicDB: `pkg/nornicdb`.
- [ ] copperDB: `crates/engine` / package `copperdb-engine`.

The engine should compose storage, transactions, cache, eval, auth context, audit/compliance hooks, replication/fabric routing, retention checks, knowledge policy runtime, and telemetry. Today it composes storage, parser, eval, transaction manager handle, and query cache only.

Audit delta: NornicDB has a per-database config resolver for search/vector/embedding controls and warming behavior. copperDB now has the first baseline of that model: `copperdb-config` owns the allowed-key registry plus effective-config resolver, `copperdb-multidb` persists durable per-DB overrides, `copperdb-engine` validates ranked-search requests against the resolved per-DB search settings, consumes a first maintained storage-backed local fulltext runtime, and now exposes engine-native local fabric ranked-search batch plus hydration helpers for the internal transport seam, `copperdb-server` exposes read/write admin endpoints plus an effective-config view while enforcing the ranked-search gate on the user-facing admin route and building local engine-backed replica/ranked-search/hydration/graph-read gRPC services, and the `copperdb` binary can start that tonic service behind config/CLI gRPC listener settings while threading the first TLS settings through the same resolved runtime config and relying on unified-auth validation rather than a separate shared gRPC token setting. Deterministic engine/server regressions now cover fulltext, vector/hybrid, CLI-override precedence, the first local fulltext runtime path, the new local fabric ranked-search seam, internal replica admin-JWT enforcement with no-auth bypass, caller-forwarded ranked-search authorization, caller-forwarded distributed graph-read transport and route coverage, and a tonic TLS handshake path. The remaining Layer 5 work is broader engine/runtime consumption of those resolved settings for search/embedding warming and activation, plus composing the consensus-backed transaction-time oracle and bookmark/read-fence flow needed for distributed SI/RYOW; defaults for automatic work remain disabled and opt-in per database. For clarity, that disabled automatic mode must not suppress rebuild/maintenance of indexes already declared through Cypher DDL for a database; it only suppresses extra implicit/background indexing beyond declared schema, while the CLI override remains the hard global kill switch.

MVP scope note: the first runnable MVP should follow NornicDB's local embedding backend directly instead of introducing `mistral.rs`: embed llama.cpp with the same local GGUF and multi-model loading shape, preserve env-driven passthrough for llama.cpp controls such as context type, pooling, attention type, and flash-attn, and pin to the same current llama.cpp revision NornicDB uses (`b9410` at present). Heimdall governance, reranking, GPU acceleration, MCP, GraphQL, APOC, and distributed execution are deferred until after the single-node Northwind benchmark core is proven.

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

Audit delta: protocol parity remains broader than thin transport shape. The full sweep found missing or incomplete NornicDB surfaces for MCP tools (`store`, `recall`, `discover`, `link`, `task`, `tasks`), Heimdall governance/LLM quality-control workflows, GraphQL engine-backed resolvers, Bolt auth/role propagation, and streaming import/export conversion utilities. copperDB already has per-database config admin routes plus effective-config views on the server surface; the remaining MVP protocol priorities are Bolt auth/role propagation, streaming conversion, and plugin-ready builtin function/procedure registration, while MCP, GraphQL, Heimdall, and APOC compatibility remain deferred.

## Implementation Walk Order

1. Finish layer 0 contracts and keep `topology` as the single distributed contract path.
2. Complete layer 1 durable security packages before calling protocol surfaces complete: `audit`, `security`, then `compliance`.
3. Expand layer 2 storage and transaction metadata only when the package being implemented owns durable state there.
4. Keep layer 3 distributed execution as real planning and persistence contracts first; do not check a package until its state is durable and its immediate consumers compile against it.
5. Port layer 4 package behavior one package at a time, starting with `cypher`, `filter`, `indexing`, `eval`, then `knowledgepolicy`.
6. Thread layer 5 engine through auth, audit, compliance, retention, replication/fabric, search/index, and telemetry.
7. Complete layer 6 protocol adapters after the central engine pipeline is stable.

## Foundational Distributed Contracts

The following contracts are recorded as future distributed vocabulary even while distributed execution remains disabled:

- Hyperscaler profiles: provider, region, zones, tier, metadata.
- Mesh peers: node id, address, region/zone, capabilities, health, heartbeat, observed RTT, inflight load, capacity weight.
- Placements: tenant/database/shard, primary, replicas, search nodes, hyperscaler profile.
- Distributed search plans: placement plus latency-ranked healthy search fan-out, bounded parallelism, and hedge deadline.
- Distributed write plans: placement plus mode, consistency level, coordinator, replicas, and required acknowledgements.
- Distributed read plans: placement plus consistency level, coordinator, replicas, and required responses.
- Distributed transaction times: authoritative begin/commit/read-fence values come from the transaction-time oracle seam; the current local fallback format is `epoch, logical counter, node ordinal`, with no wall-clock dependency on the write hot path.

These contracts are implemented in `copperdb-topology`, persisted by `storage` where they are durable topology metadata, and consumed by `fabric`, `search`, `replication`, and `txsession`.
