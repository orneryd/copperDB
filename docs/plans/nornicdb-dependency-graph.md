# NornicDB Architecture Dependency Graph

Date: 2026-05-26

This graph is the package-porting order for copperDB. It follows NornicDB's package architecture while keeping Rust dependency cycles out of the workspace. Each layer may depend on earlier layers; later layers should not be required by earlier layers.

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

- `replication` consumes `topology` through `HighAvailabilityWritePlanner` and the coordinator-based `DynamoQuorum` contract documented in [docs/plans/distributed-execution-architecture.md](distributed-execution-architecture.md). It now has Cassandra-style coordinator write/read execution, in-memory replica transport, storage-backed replica adapter coverage, quorum failure behavior, failed-replica outputs, durable hinted handoff/read-repair queue records for post-quorum repair follow-up, a scheduled repair worker for background replay, transport graph-read hooks that let higher layers traverse remote peer-backed graph partitions, a read-only remote knowledge-policy access-metadata seam, and timestamp-rich remote node payloads so routed scoring has the same anchor inputs as local reads. The replication contract remains Dynamo-quorum-based even as transaction-time allocation moves toward a separate consensus-backed oracle for distributed SI/RYOW.
- `fabric` consumes `topology` through `FabricTopology` and exposes search, consistency-aware write, consistency-aware read plans, federated database shard-map planning, targeted/scatter fabric read scopes for label, relationship type, collection, shard, default-shard, global-id, and all-shard reads, plus deterministic row, aggregate, and path-set merging for scatter-gather outputs.
- `search` consumes `topology` through `DistributedSearchRouter` and now has a distributed executor seam for fan-out, failed-node tracking, deterministic merge ordering, in-memory ranked batch collection, transport-backed home-shard hydration collection, RRF ranked batch outcomes for fabric lexical/vector/graph/temporal candidates, planned-shard execution accounting, and post-merge hydration/policy filtering helpers.
- Bookmark-derived fabric read fences now thread through that ranked-search seam as well: the server-side fabric ranked-search admin route can resolve bookmark strings into a `LogicalTransactionId` fence and pass it through ranked batch fanout plus home-shard hydration fanout, even though broader session-owned propagation and remote fence enforcement are still open.
- `qdrantgrpc` consumes `DistributedSearchPlan` to build vector-search request targets for external vector-store offload and now has a production Qdrant HTTP search client plus a distributed executor that fans out through the remote client seam, tracks failed targets, and merges hits deterministically.
- `nornicgrpc` consumes `DistributedWritePlan`, `DistributedReadPlan`, and `DistributedSearchPlan` to build internal remote execution envelopes without inventing alternate routing rules, and now exposes generated tonic/prost replica service bindings, a generated-client adapter, a generated-server adapter, a `ReplicaTransport` adapter that maps coordinator write/read calls to target-addressed remote client requests, ranked-search plus hydration gRPC request/response messages and transport adapters that map planned search fanout and home-shard read plans to target-addressed client/server requests, distinct auth modes for internal versus data-path RPCs, internal replica apply/read enforcement through the unified auth core with admin JWT validation when security is enabled and no-auth bypass when it is disabled, and caller-token forwarding on ranked-search, hydration, and distributed graph-read RPCs so remote nodes reapply the existing per-database auth model. The tonic transport baseline is mTLS-capable through config-driven server cert/key, client CA, client cert/key, trust CA, and domain wiring for listener and client paths with focused handshake coverage, startup certificate validity-window and cert-or-key consistency checks for configured gRPC identities, and a vendored-`protoc` build path so the crate compiles on Windows without a machine-level protobuf install.
- `engine` now loads durable topology from storage, exposes distributed read/write plan helpers, builds Cassandra coordinators from caller-provided replica transports, roots durable repair queues beside the database path or an explicit repair-queue path, replays repair batches through replica transports, builds scheduled repair workers, exposes explicit distributed Cypher execution that routes mutations through `DynamoQuorum` before local evaluation, exposes the first embedded fabric database control-plane facade for durable shard-map registration/listing/loading plus per-shard read/search planning, targeted fabric read planning, deterministic row/aggregate/path-set merging, composed RRF ranked search execution over planned fabric shards, transport-backed ranked batch collection with responded/failed node reporting, transport-backed home-shard hydration planning from merged hit ids, and post-merge ranked search hydration/policy filtering, and now has a real mesh-backed distributed BFS helper that traverses multiple peer storages and reconstructs node/edge paths for outgoing, incoming, and undirected traversal, with focused coverage for shortest-path selection, disconnected no-path results, and read-quorum failure. That traversal surface now also has an engine-level query-style materialization wrapper that returns `path`, `nodes(path)`, `relationships(path)`, and `length(path)` rows from the reconstructed mesh path while full Cypher path-variable syntax remains a Layer 4 parser/eval gap.
- `server` now keeps protocol handlers thin when HTTP Cypher and Neo4j transaction commit requests opt into distributed Cypher execution with `COPPERDB_DISTRIBUTED_CYPHER` or `x-copperdb-distributed`. The server-owned write path now builds a real outbound tonic replica transport from topology peers and generates a short-lived admin cluster JWT when security is enabled. The read path now also builds the real graph-read tonic transport for remote node, edge, label/property, and access-metadata RPCs, forwarding the original caller bearer token so the remote node reapplies the existing per-database read gate while clustered access-metadata side effects continue to use the internal admin-authenticated replica channel instead of fabricating in-memory peers. Those server-owned distributed read paths now derive an effective request fence from a real `txsession` transaction seeded by request bookmarks before forwarding remote graph reads or fabric ranked-search plus hydration fanout, and the local gRPC data-path handlers now observe that forwarded fence through a short-lived serving-node `txsession` read transaction before serving current-state graph, ranked-search, or hydration reads. Full snapshot-materialized remote MVCC enforcement is still open because the storage/replication graph-read seam remains current-state only. It also exposes authenticated fabric admin routes for shard-map registration/listing, scoped read/search plan inspection, and transport-backed ranked fabric search execution over the internal gRPC ranked-search and hydration RPCs, with list results filtered by the caller's durable per-database access instead of the process default database. The server crate now also builds engine-backed gRPC replica services for local replica apply/read, ranked search, hydration, and graph-read handling, and the `copperdb` binary can start that tonic service behind config/CLI gRPC listener settings. The retention admin routes now use the same durable auth gate, allowing read-only principals to inspect retention status/policies while reserving policy, hold, erasure, and sweep mutations for writers.
- `storage` persists and reloads the same topology contracts, and now also persists durable fabric database shard maps used by the federated AI fabric plan.
- Layer 3 is checked: package-owned durable state is persisted, runtime-only transport/executor state is explicit, immediate consumers compile, and focused distributed contract tests cover topology planning, coordinator quorum behavior, repair replay, protocol opt-in, nornic remote transport, and Qdrant vector-search offload.

## Enterprise Scaling Design

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

Current Rust status: Layer 4 is in progress. `cypher`, `filter`, and `eval` now share a single parsed expression path for Cypher list and map literals; pattern property maps in `MATCH`/`CREATE`/`MERGE` also evaluate as expressions against the current row, so literal, row-aware, and `UNWIND`-driven shapes execute through the same evaluator path instead of stopping at parser acceptance. The parser and handwritten validator now also retain comma-separated pattern segment boundaries in the AST, which lets `eval` execute disconnected relationship-pattern `MATCH` groups and comma-separated relationship `CREATE` groups without cross-segment misbinding. For distributed consistency, the architecture direction is now explicit: keep Dynamo-quorum replication for data placement writes and reads, but move authoritative distributed transaction times and read fences onto a separate consensus-backed oracle so MVCC snapshot isolation and RYOW do not depend on purely local clock allocation.

`cypher` now matches NornicDB’s query-shape detector more closely for edge-property aggregation, compound-shape probing, pipeline probing, and the narrow simple `MATCH ... LIMIT` detector, while `engine` and `eval` consume those routes for real optimized slices: single-node label-backed `MATCH (n:Label) RETURN n LIMIT k`, mutual-relationship, incoming/outgoing count aggregation, typed and untyped edge-property aggregation, current compound create/delete relationship fast paths, and parser-approved pipeline chains. That routed pipeline path now threads binding rows itself, reuses bound nodes across chained `CREATE` clauses, constrains later relationship `MATCH` clauses to already-bound endpoints, supports routed `OPTIONAL MATCH` with correct null preservation, supports routed `MERGE`, `DELETE`, `SET`, and `REMOVE` after `WITH` or `UNWIND` without dropping out of the pipeline route, and covers both the seeded `UNWIND ... MATCH ... CREATE` seeder shape and a basic `UNWIND ... MERGE ... RETURN` batch-upsert shape through routed execution. Eval now also owns per-engine hot-path trace state for these narrow routes, with deterministic assertions on the simple `MATCH ... LIMIT`, current compound create/delete relationship fast path, node `MERGE` schema-lookup versus scan-fallback behavior, seeded `UNWIND ... MATCH ... CREATE`, and basic `UNWIND ... MERGE ... RETURN` trace flags. Eval’s shared mutation paths now distinguish node and relationship bindings for `DELETE`, persist relationship property updates for `SET`, and remove relationship properties plus node labels correctly for `REMOVE`.

Path semantics are materially broader than before: `MATCH p = ...` and `CREATE p = ...` path variables parse on the handwritten path, normal evaluation now binds `p`, `nodes(p)`, `relationships(p)`, and `length(p)` for node-only patterns and relationship traversals, and the generalized linear relationship matcher now covers fixed-length multi-hop chains, variable-length edges at any position in a linear chain, and multi-edge `shortestPath(...)` patterns. `MATCH p = shortestPath((...)-[:TYPE*]->(...))` executes as a single shortest-path result rather than enumerating every reachable path.

The Rust `knowledgepolicy` port is no longer missing: Copper persists decay bindings, promotion policies, and per-entity access metadata; promotion `WHEN` clauses compile into parsed expressions up front; `knowledgepolicy` owns the shared `ON ACCESS` flusher/runtime boundary; and eval applies promotion multiplier/floor/cap math against persisted plus buffered access metadata for node and edge visibility. `CALL nornicdb.knowledgepolicy.resolve(...)` exposes the same local scoring and target-resolution path as a deterministic inspection surface.

On the distributed engine path, `execute_distributed_as(...)` routes path-query slices through remote peer graph reads for shortest-path queries, node-only path-variable `MATCH`, direct single-hop path-variable `MATCH`, non-shortest variable-length path-variable `MATCH`, standalone `OPTIONAL MATCH p = ... RETURN ...`, and routed path reads behind a bounded leading-clause subset. Those routes now materialize query-visible path objects from remote peer storage, reuse the same node/edge knowledge-policy scoring as local reads for visibility suppression, persist successful remote `ON ACCESS` mutations through the replicated access-metadata write primitive, prefer direct `_id` fetches before property/label fallback during endpoint selection, and tolerate partial replica failures for undirected edge reads. Distributed variable-length traversal also mirrors the local BFS visitation rule by deduplicating on `(node_id, depth)`, and distributed optional path projection matches local null semantics on misses.

These packages remain unchecked until broader Neo4j/Cypher compatibility, broader distributed row-aware routed reads beyond the current routed prefix subset, broader pipeline-shape parity beyond the currently routed clause mix, broader compound-query execution parity, index maintenance, and query execution contracts are complete.

Audit delta: `indexing` parity now needs to be tracked in two lanes. Property-backed exact, RANGE, and TEMPORAL indexes are actively moving through storage/indexing/eval parity. `FULLTEXT` and `VECTOR` are still not at NornicDB runtime parity, but copperDB now has a first maintained local fulltext runtime baseline: storage persists FULLTEXT catalog rows and maintains inverted-token entries for node properties, and `copperdb-engine` consumes that storage-backed local fulltext path under the per-database search gate. Cypher schema DDL remains the authoritative source of per-database index definitions: when a database declares property, RANGE, TEMPORAL, FULLTEXT, or VECTOR indexes, those declared indexes should still load, rebuild, and maintain for that database. Automatic search/index/embedding work must default disabled in copperDB and only refer to extra implicit/background indexing beyond schema DDL, while an operator CLI flag remains the hard global kill switch for all indexing work.

Index maintenance is incrementally broader now: storage rebuilds, updates, and drops maintained node-property index state for both single-property and composite node index definitions, maintains single-property and composite relationship-property indexes keyed by relationship type, and the indexing catalog prefers the most specific matching node or relationship index definition during lookup instead of treating richer definitions as catalog-only metadata.

Index catalog metadata is also closer to current Cypher DDL expectations now: `IndexDefinition` carries an explicit index kind, generic `CREATE INDEX` currently materializes as `RANGE`, explicit `CREATE RANGE INDEX` plus explicit `CREATE TEMPORAL|FULLTEXT|VECTOR INDEX` now persist their typed catalog rows, and `SHOW INDEXES` exposes kind instead of collapsing every property-backed definition into an untyped bucket. The query surface can also filter stored index metadata through `SHOW RANGE INDEXES`, `SHOW TEMPORAL INDEXES`, `SHOW FULLTEXT INDEXES`, and `SHOW VECTOR INDEXES` without inventing alternate catalog rows. Those typed DDL forms now also follow the same duplicate-name, `IF NOT EXISTS`, drop-by-name, and `IF EXISTS` contract as the older generic/RANGE path. That ordered-comparison path is no longer RANGE-only either: current Cypher DDL now accepts both node `FOR (n:Label) ON (n.prop)` and relationship `FOR ()-[r:TYPE]-() ON (r.prop)` RANGE or TEMPORAL index targets, and storage/indexing can use maintained single-property and composite node or relationship RANGE and TEMPORAL indexes to narrow simple `<op>` comparisons before eval applies the normal predicate filter. Single-property ordered keys are now encoded in an order-preserving storage form for strings and numbers, so those node and relationship comparison reads run as bounded sled range scans instead of scanning an entire property prefix and filtering every indexed value in memory. Maintained composite ordered-comparison indexes now participate in the same current-state path when the compared property is either the leading indexed property or a later indexed property whose preceding indexed fields are all constrained by exact predicates; exact suffix predicates are no longer required for the scan itself, but when they are present the catalog prefers the composite definition with the most matching exact fields and storage filters those exact properties deterministically. `FULLTEXT` and `VECTOR` definitions are catalog-visible DDL metadata only at this stage; storage persists them without rebuilding or maintaining property-backed lookup state, and exact-match and ordered-comparison lookup selection intentionally exclude those kinds until they have real maintained runtime paths.

Search/vector audit status: NornicDB's `pkg/search` includes BM25/fulltext indexes, HNSW, IVFPQ, vector file storage, index persistence/versioning, decay filtering, reranking, hybrid cluster routing, GPU acceleration, and search observability. copperDB's current search layer is not yet at that runtime parity, but the engine now has a first maintained storage-backed local fulltext runtime plus engine-native local fabric ranked-search batch and hydration helpers that can feed the internal transport seam without rebuilding query-time indexes. The next search/index slices should prioritize improving that local baseline toward CPU BM25/fulltext parity, then wiring the real engine-backed local ranked-search handler through `nornicgrpc`/server runtime assembly before adding CPU vector runtime, vector file storage, and local in-memory embedding execution. GPU acceleration, reranking, and the broader inference lifecycle are deferred out of the first runnable MVP.

Layer 4 delta: `knowledgepolicy` now owns the shared `ON ACCESS` flusher/runtime boundary rather than eval-local buffering, promotion `WHEN` clauses compile into parsed expressions up front, eval now applies promotion multiplier/floor/cap math against persisted plus buffered access metadata when deciding node and edge visibility, and `CALL nornicdb.knowledgepolicy.resolve(...)` exposes the same local scoring/target-resolution path as a tested inspection surface.

Current Layer 4 distributed status: routed/special-path distributed reads now reuse the same node/edge scoring path as local evaluation for visibility suppression, and successful remote reads now persist `ON ACCESS` mutations through the replicated access-metadata write primitive with coordinator semantics. Deterministic engine regressions now cover stale remote node suppression, stale remote edge suppression, node access-metadata persistence, and edge access-metadata persistence.

## Layer 5: Core Engine Composition

This is the embedded database facade.

- [ ] NornicDB: `pkg/nornicdb`.
- [ ] copperDB: `crates/engine` / package `copperdb-engine`.

The engine should compose storage, transactions, cache, eval, auth context, audit/compliance hooks, replication/fabric routing, retention checks, knowledge policy runtime, and telemetry. Today it composes storage, parser, eval, transaction manager handle, and query cache only.

Audit delta: NornicDB has a per-database config resolver for search/vector/embedding controls and warming behavior. copperDB now has the first baseline of that model: `copperdb-config` owns the allowed-key registry plus effective-config resolver, `copperdb-multidb` persists durable per-DB overrides, `copperdb-engine` validates ranked-search requests against the resolved per-DB search settings, consumes a first maintained storage-backed local fulltext runtime, and now exposes engine-native local fabric ranked-search batch plus hydration helpers for the internal transport seam, `copperdb-server` exposes read/write admin endpoints plus an effective-config view while enforcing the ranked-search gate on the user-facing admin route and building local engine-backed replica/ranked-search/hydration/graph-read gRPC services, and the `copperdb` binary can start that tonic service behind config/CLI gRPC listener settings while threading the first TLS settings through the same resolved runtime config and relying on unified-auth validation rather than a separate shared gRPC token setting. Deterministic engine/server regressions now cover fulltext, vector/hybrid, CLI-override precedence, the first local fulltext runtime path, the new local fabric ranked-search seam, internal replica admin-JWT enforcement with no-auth bypass, caller-forwarded ranked-search authorization, caller-forwarded distributed graph-read transport and route coverage, and a tonic TLS handshake path. The remaining Layer 5 work is broader engine/runtime consumption of those resolved settings for search/embedding warming and activation, plus composing the consensus-backed transaction-time oracle and bookmark/read-fence flow needed for distributed SI/RYOW; defaults for automatic work remain disabled and opt-in per database. For clarity, that disabled automatic mode must not suppress rebuild/maintenance of indexes already declared through Cypher DDL for a database; it only suppresses extra implicit/background indexing beyond declared schema, while the CLI override remains the hard global kill switch.

MVP scope note: the first runnable MVP should use an in-process embedding backend if feasible, preferring `mistral.rs` over a llama.cpp-based local backend while preserving NornicDB's local in-memory embedding goal. Heimdall governance, reranking, GPU acceleration, MCP, GraphQL, and APOC are deferred until after the core distributed engine works.

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

The following contracts are intentionally first-class even while distributed execution remains disabled:

- Hyperscaler profiles: provider, region, zones, tier, metadata.
- Mesh peers: node id, address, region/zone, capabilities, health, heartbeat, observed RTT, inflight load, capacity weight.
- Placements: tenant/database/shard, primary, replicas, search nodes, hyperscaler profile.
- Distributed search plans: placement plus latency-ranked healthy search fan-out, bounded parallelism, and hedge deadline.
- Distributed write plans: placement plus mode, consistency level, coordinator, replicas, and required acknowledgements.
- Distributed read plans: placement plus consistency level, coordinator, replicas, and required responses.
- Distributed transaction times: authoritative begin/commit/read-fence values come from the transaction-time oracle seam; the current local fallback format is `epoch, logical counter, node ordinal`, with no wall-clock dependency on the write hot path.

These contracts are implemented in `copperdb-topology`, persisted by `storage` where they are durable topology metadata, and consumed by `fabric`, `search`, `replication`, and `txsession`.
