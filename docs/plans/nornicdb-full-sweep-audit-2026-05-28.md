# NornicDB Full Sweep Audit

Date: 2026-05-28

Scope: local on-disk comparison between `copperDB` and `NornicDB`, using the current hierarchy plan plus the actual package implementations in `c:\Users\timot\Documents\GitHub\copperDB` and `c:\Users\timot\Documents\GitHub\NornicDB`.

This is an audit and documentation artifact only. It records architecture and performance drift that is not yet fully captured in the layer plans. It does not mark additional packages complete.

## Executive Findings

1. copperDB's explicit-index direction is correct, but the docs need to preserve the target policy: Cypher schema DDL remains authoritative per database, so declared indexes should still load, rebuild, and maintain for that database, while automatic index/search/embedding work means only extra implicit/background indexing beyond declared schema and must default off unless enabled through explicit per-database configuration. NornicDB already has per-DB search/vector/embedding controls in `pkg/config/dbconfig`; copperDB now has a first per-DB resolver/override baseline, but broader runtime consumers and warming behavior still need to be threaded through the engine.
2. Layer 2 storage is the largest structural drift. NornicDB has a substantial async write-behind engine, WAL repair/diagnostics, MVCC extension interfaces, streaming/prefix APIs, and richer schema/index lifecycle. copperDB has focused baselines but should not be documented as storage-complete for those operational surfaces.
3. Layer 3 distributed packages are checked in the current dependency graph, but the audit found important production gaps versus NornicDB: multi-region replication, transport security, chaos testing, fragment-based fabric execution, distributed transaction context, remote fragment execution, and full gRPC caller auth/TLS parity.
4. Distributed transaction time is also a first-class architecture gap. The current Rust path allocates logical transaction IDs locally and merges observed remote values, which is adequate for ordering and repair, but it is not the intended end state for distributed MVCC snapshot isolation plus read-your-own-writes. The target architecture is hybrid: keep Dynamo-style quorum replication for data durability and fan-out, but add a consensus-backed transaction-time oracle, with Paxos v2 as the target distributed implementation for authoritative begin/commit/read-fence allocation.
5. Layer 4 query/index/search remains the most performance-sensitive gap. copperDB has strong progress on Cypher execution and range/temporal index semantics, but NornicDB has production BM25, HNSW/IVFPQ/vector file store, decay-filtered search, hot-path tracing, SIMD/math acceleration, reranking, and embedding cache/backends that copperDB has not ported. For the first runnable MVP, GPU acceleration and reranking/inference lifecycle work are deferred; CPU search/vector runtime and in-memory embeddings remain the core target.
6. Layer 5 and Layer 6 still need the central engine/config/protocol composition pass: per-database config, search/embedding warming, Bolt role propagation, conversion/import-export streaming, plugin-ready builtin function/procedure registration, and bookmark/read-fence flow for the hybrid transaction-time model are MVP-relevant. MCP tools, GraphQL resolvers, and Heimdall governance workflows remain recorded for parity but are deferred until after the core distributed engine is working.

## Target Policy: Auto Indexing And Search Defaults

copperDB should keep user-defined indexes as the default operational mode. Any automatic index, search-index, embedding, auto-link, or auto-TLP behavior must be opt-in and scoped per logical database.

Concrete target:

- Global defaults for automatic work: `false` or disabled.
- Cypher schema DDL is still authoritative per database: if a database defines property, RANGE, TEMPORAL, FULLTEXT, or VECTOR indexes in schema, those declared indexes should still load, rebuild, and maintain for that database.
- Per-database overrides: durable and queryable through the multidb/config plane.
- CLI overrides: highest precedence emergency kill switch, matching NornicDB's resolver pattern and acting as a hard global stop for all indexing work.
- Warming modes: `startup` or `lazy`, but only meaningful after the relevant per-DB feature is enabled.
- Explicit `CREATE INDEX` DDL remains the default path for property/range/temporal indexes, and explicit `CREATE FULLTEXT INDEX` / `CREATE VECTOR INDEX` DDL is likewise the default path for search/vector index definitions.
- Disabled automatic work must prevent duplicate or surprise implicit/background indexing only; it must not suppress rebuild or maintenance of schema-declared indexes.
- `FULLTEXT` and `VECTOR` catalog rows still do not have NornicDB-level storage/search backends. copperDB now has a first maintained local fulltext baseline: storage keeps inverted-token entries for node FULLTEXT indexes and the engine consumes that path locally, but broader persisted search/index lifecycle parity and vector runtime remain open.

NornicDB reference files:

- `pkg/config/dbconfig/keys.go` exposes per-DB keys including `NORNICDB_SEARCH_BM25_ENABLED`, `NORNICDB_SEARCH_BM25_WARMING`, `NORNICDB_SEARCH_VECTOR_ENABLED`, `NORNICDB_SEARCH_VECTOR_WARMING`, embedding options, HNSW/IVF-HNSW options, auto-links, auto-TLP, embed worker, and MVCC lifecycle interval.
- `pkg/config/dbconfig/resolver.go` defines precedence: built-in defaults, global config/env/YAML, per-DB overrides, then CLI overrides.
- `cmd/nornicdb/main.go` wires CLI flags for embedding enablement and BM25/vector warming.

copperDB current drift:

- `crates/config/src/lib.rs` has global storage/server/bolt/auth/replication/encryption/vectorspace/GPU config only.
- `crates/multidb/src/lib.rs` now carries durable per-DB override maps and exposes effective-config resolution through the shared config layer, but the key surface is still intentionally narrow.
- A first Rust baseline now exists for the NornicDB-style per-DB config path: `copperdb-config` owns the allowed-key registry and effective resolver, `copperdb-multidb` persists per-DB override maps durably, `copperdb-engine` validates ranked-search requests against the resolved per-DB search settings, and `copperdb-server` exposes per-DB config read/write plus effective-config admin endpoints while enforcing that ranked-search gate on the admin route. Engine-side search/embedding warming and broader runtime consumption are still open.

## MVP Scope Clarification

The full audit is a parity register, not the first-runnable MVP scope. For the first point where copperDB can be turned on and tried end to end, the MVP should focus on the core distributed engine, durable storage/query/index behavior, per-database config, CPU search/vector runtime, and embedding execution that can run in process.

Deferred until after the core distributed engine works:

- MCP tool surface and transport.
- GraphQL schema/resolver surface.
- Heimdall governance, Bifrost review workflows, auto-link/auto-TLP approval flow, reranking, and the broader inference lifecycle.
- GPU acceleration, including CUDA/Metal vector scoring and GPU clustering.
- APOC compatibility.

Still MVP-relevant preparation work:

- Builtin function/procedure registration should be plugin-ready so APOC-style extensions can be added later without rewriting the call dispatch architecture.
- Embedding runtime should track NornicDB's llama.cpp local GGUF path rather than introducing `mistral.rs`: preserve the ability to load multiple local model domains, pass through env-driven llama.cpp controls such as context type, pooling, attention type, and flash-attn, and pin to the same current llama.cpp revision NornicDB uses (`b9410` at present).
- Automatic index/search/embedding behavior remains disabled by default and per-DB opt-in only, but schema-declared indexes must still reload/rebuild per database and the CLI override remains a hard global kill switch.

## Layer 0 Audit: Shared Contracts And Config

Implemented baselines remain credible for `util`, `buildinfo`, `envutil`, `config`, `errors`, `otel`, `lifecycle`, and `topology`, but the following drift should stay visible:

- `config`: NornicDB has a broader operational configuration surface: WAL sync mode, WAL auto-compaction, strict durability, WAL retention, encryption enablement/provider, password policy, feature flags, search/vector warming, embedding toggles, and per-DB override resolution. copperDB currently has simpler global config and should not be documented as having NornicDB-level runtime tunability.
- `topology`: copperDB intentionally promotes topology to a Layer 0 Rust contract. NornicDB has more topology behavior embedded around inference/search/fabric paths. This is an intentional architecture deviation, but docs should continue explaining it as a cycle-avoidance and distributed-contract choice.
- Feature flags: NornicDB has many env/config-controlled features for Kalman, edge provenance, cooldown, evidence buffering, per-node config, WAL, parser selection, auto-TLP, and GPU clustering. copperDB should add a feature-flag posture before porting more experimental behavior.

Documentation actions:

- Add a config-defaults section before marking config parity broader than startup/listener/auth basics.
- Track per-DB override resolver as a Layer 5 dependency because engine/server must consume effective database config.

## Layer 1 Audit: Security, Identity, Compliance

The checked Layer 1 status remains directionally valid, but the audit found missed details:

- `encryption`: NornicDB separates envelope encryption, DEK cache, and rotation concerns across dedicated files. copperDB has provider-backed envelopes and encrypted storage opens, but docs should still track DEK cache hit behavior and rotation/rekey performance as remaining audit items unless verified by focused tests.
- `auth`: copperDB persists users/roles and HTTP auth, but protocol-wide propagation is incomplete until Bolt, GraphQL, distributed gRPC, and remote shard calls carry the caller identity and per-database role entitlements.
- `audit`/`compliance`: copperDB has durable audit/compliance baselines. NornicDB's broader evidence, policy, and protocol coverage should remain a protocol-composition task rather than being treated as fully covered by embedded query execution alone.

Documentation actions:

- Keep Layer 1 checked for package-owned durable state, but list protocol propagation and per-DB entitlement enforcement under Layers 5-6.
- Track encryption rotation/DEK-cache parity as a follow-up if it is not already covered by tests.

## Layer 2 Audit: Storage, Transactions, Metadata

Layer 2 is the largest source of architecture/performance drift.

### Storage

NornicDB reference areas:

- `pkg/storage/async_engine.go` plus async-engine tests for write-behind, flush intervals, update/delete races, embedding counts, event callbacks, prefix streaming, and label/edge index consistency.
- `pkg/storage/wal*.go` for WAL segmenting, repair, corruption diagnostics, degraded mode, atomic records, recovery chunking, compaction, and durability tests.
- `pkg/storage/types.go` optional extension interfaces for prefix stats, adjacent edges, MVCC visibility, temporal lookup, namespace label stats, and streaming APIs.

copperDB current drift:

- Async write-behind engine is missing. copperDB mutations are effectively synchronous against storage and do not expose NornicDB's flush hold/result model.
- MVCC now matches one important upstream architectural constraint more closely: `MvccStore` keeps current-head state separate from archived historical versions so hot-path reads stay pinned to the current head, old versions stay off the current index path, retained-floor anchoring prevents sparse post-prune ghost-chain reads, and a bounded lifecycle-control surface exists for pause/resume/schedule/debt inspection. The in-memory baseline also now has typed snapshot-visible label and edge-type reads plus a bounded namespaced wrapper that delegates those lifecycle controls through the namespace seam. Broader snapshot-visible indexed parity, temporal point-in-time lookup, and automated pruning/rebuild orchestration remain incomplete.
- WAL exists as a focused baseline, but NornicDB's repair/diagnostic/segment lifecycle is much broader.
- Prefix/namespace stats, namespace label-cardinality, adjacent-edge fast APIs, namespace-scoped schema snapshots, and callback-based node/edge/chunk streaming now have copperDB storage baselines. Deeper MVCC/temporal optional interfaces remain open.
- Prefix delete/namespace deletion now has a storage baseline through structured prefix deletion with index, stats, and namespace-schema cleanup. Async streaming context cancellation and cache-merge behavior remain open under AsyncEngine parity.
- A bounded namespaced storage wrapper now exists as `crates/storage::NamespacedStorageEngine`, built on those prefix/stat/schema helpers for namespace-local CRUD, counts, schema, and streaming. Broader namespaced parity still remains open where upstream layers also compose async, MVCC-visible, composite, and remote wrappers.

### Cache And Pool

- `cache`: copperDB's query/result/write-through cache is useful, but NornicDB's LRU implementation has O(1) move-to-front behavior through linked-list style structures. copperDB should watch for O(n) eviction/update paths as cache sizes grow.
- `pool`: copperDB's pooled row/buffer structures are aligned with the performance direction; no major new drift was found beyond ensuring eval uses them in all hot row paths.

### txsession, retention, multidb

- `txsession`: broadly aligned on session states, buffered operations, and logical transaction IDs.
- `txsession` now has the right first seam for the hybrid direction: the local transaction clock can be abstracted behind a transaction-time oracle, but the distributed consensus-backed implementation and bookmark/read-fence semantics are still missing.
- `retention`: broadly aligned on policies, legal holds, and erasure request state.
- `multidb`: durable logical database catalog and the first per-DB config store/resolver baseline now exist, and the catalog itself now consumes the namespace-aware storage wrapper instead of hand-encoding the `multidb:` prefix. NornicDB's broader per-DB key surface and downstream runtime consumers are still missing, so this remains a high-priority cross-layer drift item.

Documentation actions:

- Keep existing storage async/MVCC/WAL TODOs, but expand them with explicit async-engine flush semantics, streaming APIs, prefix stats, adjacent-edge APIs, and per-DB config dependency.
- Add per-DB config to both Layer 2 `multidb` and Layer 5 engine composition as a required dependency for search/index/embedding runtime control.

## Layer 3 Audit: Distributed Execution Foundation

The current plan marks Layer 3 checked, but the full sweep found production-readiness drift versus NornicDB that should be documented as remaining architecture work.

### replication

NornicDB reference areas include `pkg/replication/multi_region.go`, `raft.go`, `transport_security.go`, `chaos_test.go`, and `peer_metrics_gc.go`.

Drift:

- Multi-region replication and failover promotion are not ported as a first-class copperDB path.
- Transport security for inter-node replication is still incomplete: copperDB now has an mTLS-capable tonic baseline with server cert/key, client-auth CA, client cert/key, trust CA, and domain wiring plus startup validation for required cert or key combinations, active certificate windows, and cert-or-key consistency, but stronger certificate lifecycle handling, cipher/version policy, and richer token/HMAC auth parity are still not documented as implemented.
- Chaos testing for cross-region latency, jitter, packet loss, partitions, and corruption is missing.
- Peer metrics garbage collection is missing, which risks unbounded observability cardinality under node churn.

### fabric

NornicDB reference areas include `pkg/fabric/executor.go`, `fragment.go`, `remote_executor.go`, `transaction.go`, and `plan_cache.go`.

Drift:

- copperDB has fabric planning/merge helpers, but not a full fragment-tree executor with local/remote fragment dispatch.
- Distributed transaction context for per-shard subtransactions is missing.
- Remote fragment execution with auth forwarding and result streaming is missing.
- Fabric query plan caching is missing.
- Distributed transaction-time fencing remains open. The current architecture still relies on local logical allocation for transaction order, while the target SI/RYOW path is a separate consensus-backed transaction-time oracle layered beside Dynamo quorum replication.

### search, qdrantgrpc, nornicgrpc

NornicDB reference areas include `pkg/search/ann_profile.go`, `hybrid_cluster_routing.go`, `hnsw_index.go`, `ivfpq_index.go`, `vector_file_store.go`, `decay_filter.go`, `fulltext_index_v2_persist.go`, `pkg/qdrantgrpc/*_service.go`, and `pkg/nornicgrpc/search_service.go`.

Drift:

- Distributed search planning exists in copperDB, but local/distributed search runtime is far behind NornicDB's BM25/HNSW/IVFPQ/hybrid routing stack.
- Search index persistence with format versioning is missing.
- Vector file store and sparse embedding storage are missing.
- Decay-filtered search is missing.
- Qdrant client support exists, but full Qdrant collection/points/snapshot service parity is missing.
- nornic gRPC has generated transport/adapters, and copperDB now has an engine-owned local replica apply/read handler plus local fabric ranked-search batch and hydration seams that can back the server side, with the `copperdb` binary now able to start that tonic service behind config/CLI gRPC listener settings. Internal replica apply/read RPCs now authenticate through the same unified auth core as UI and ingress by requiring an admin JWT when security is enabled and bypassing auth under `--no-auth`, while the ranked-search, hydration, and distributed graph-read data paths forward the original caller bearer token so the receiving node reapplies the existing per-database read gate. The tonic transport baseline is mTLS-capable for listener and client paths through config-driven server cert/key, trust CA, client cert/key, client-auth CA, and domain settings plus focused handshake coverage and startup certificate validity-window and cert-or-key consistency checks, but stronger secret and certificate distribution or rotation, broader cluster-client token generation or rotation handling, write-path caller identity forwarding, and broader end-to-end server-side distributed execution parity should remain open. The Neo4j `tx/commit` distributed write path now builds the real outbound replica transport instead of fabricating in-memory peers, that same Neo4j-compatible routed surface now covers distributed graph-read execution without hanging under the test runtime, and the read path builds the real graph-read gRPC transport with caller-auth forwarding while clustered access-metadata side effects continue through the internal replica channel.

Documentation actions:

- Keep Layer 3 checked only if the definition is "distributed planning seams and focused contract tests". Add a clear note that NornicDB production distributed execution parity remains open for multi-region, transport security, fabric fragment execution, and distributed search runtime.

## Layer 4 Audit: Query, Index, Search, AI Runtime

Layer 4 remains the active area with the most performance-sensitive drift.

### cypher, filter, eval, indexing

Current copperDB progress is real: expression parsing, relationship/path semantics, routed pipeline slices, knowledge-policy scoring, and range/temporal index semantics are now documented and tested. Remaining drift:

- NornicDB has broader hot-path query routing and trace coverage for simple `MATCH ... LIMIT`, UNWIND/MERGE batches, call-tail traversal, compound mutation chains, and pipeline branch shapes. copperDB now has narrow routed slices for single-node `MATCH ... LIMIT`, the current compound create/delete relationship fast path, the seeded `UNWIND ... MATCH ... CREATE` pipeline shape, and basic `UNWIND ... MERGE ... RETURN` pipeline upserts, and node `MERGE` now also records deterministic schema-lookup versus scan-fallback trace behavior. Trace assertions cover those current routed slices plus the merge lookup/fallback branch, but hot-path parity should remain open until broader query-shape routing and trace tests cover the remaining performance paths.
- `FULLTEXT` and `VECTOR` DDL/catalog lifecycle exists in copperDB. A first maintained local fulltext runtime now exists through storage-maintained inverted-token entries plus engine-side local query execution, but vector runtime paths and broader search lifecycle parity are still absent. The next parity step is to deepen that maintained runtime rather than adding more DDL.
- Composite range/temporal selection is now strong, but broader Neo4j index provider semantics, index options, analyzers, and vector index configuration are not ported.

### search

NornicDB has production search packages for BM25 fulltext, query-plan caching, fulltext persistence, HNSW, IVFPQ, GPU/Metal candidate generation, hybrid lexical/vector routing, decay filters, reranking, vector file store, and observability. copperDB search currently provides distributed/RRF data structures and simple in-memory fulltext helpers only. For MVP, GPU/Metal candidate generation and reranking are deferred; the nearer target is CPU BM25/fulltext, CPU vector runtime, vector file store, persistence/versioning, decay filtering, and observability.

Priority drift:

- BM25/fulltext maintained indexes and query surface.
- Vector index runtime with strategy selection: brute-force for small sets, HNSW for larger sets, compressed IVFPQ for very large sets.
- Durable vector file store and search index persistence with version checks.
- Decay/knowledge-policy filtering during search.
- Reranking and MMR/local-LLM hooks are parity items, but deferred out of the first runnable MVP.
- Search observability with stage-level latency metrics.

### temporal, decay, knowledgepolicy

- NornicDB integrates temporal/decay with Kalman-style access velocity and adaptive multipliers. copperDB has knowledge-policy scoring and access metadata, but adaptive decay/temporal Kalman integration remains incomplete.
- NornicDB's ON ACCESS runtime supports computed mutations and Kalman-aware state updates. copperDB has a shared flusher boundary and local scoring, but arbitrary computed overflow properties and Kalman mutation semantics should remain open until proven.

### embedding/vector/LLM stack

- `embed`: NornicDB has cached embedders, backend reporting, remote/local providers, crash recovery, and chunking behavior. copperDB embed remains much thinner. For MVP, follow NornicDB's llama.cpp-based local embedding backend instead of introducing `mistral.rs`, including parity for multiple local model domains, env-driven context tuning, and the same pinned llama.cpp revision (`b9410` at present). Storage parity should also stop treating embedding metadata as user properties: the target model is dedicated typed node fields mirroring NornicDB's managed embedding state.
- `vectorspace`: copperDB has vector primitives/config, but not NornicDB's full search lifecycle integration.
- `math`/`simd`: copperDB crates are scaffolds; NornicDB has platform-specific SIMD/Metal acceleration for vector scoring. GPU acceleration is deferred until late-stage parity.
- `localllm`/`inference`: copperDB should track backend lifecycle, crash isolation, and model/provider config before claiming parity. The broader inference lifecycle is deferred from MVP except for the embedding backend needed to run locally in memory.
- `linkpredict`: NornicDB auto-link/auto-TLP behavior is opt-in and should remain per-DB disabled by default in copperDB until governance controls exist.

Documentation actions:

- Split `indexing` parity into two lanes: property/range/temporal indexes, and search/vector indexes. The former is actively nearing parity; the latter is still mostly open.
- Add explicit Layer 4 search runtime TODOs rather than hiding them under generic "index parity".

## Layer 5 Audit: Core Engine Composition

NornicDB `pkg/nornicdb` composes storage, search, embeddings, per-DB config, warming gates, vector recall, and distributed features more deeply than copperDB currently does.

Drift:

- Per-database search flags resolver is no longer missing from copperDB engine composition: ranked-search gates, the first local fulltext runtime path, and engine-native local replica/ranked-search/hydration gRPC seams now consume the resolved per-database settings, with the binary able to start the local tonic service through config/CLI listener settings and thread the first TLS settings through the same resolved runtime config while internal replica auth is handled through the unified auth core instead of a separate shared-token setting. Broader search/vector/embedding runtime consumers are still missing.
- Transaction-time oracle composition is missing: begin/commit timestamps and session read fences still need an engine-visible distributed implementation so MVCC snapshot isolation and RYOW do not depend on purely local logical allocation.
- Search index warmup strategy is missing: `startup` versus `lazy` should be a per-DB effective config value.
- Embedding enablement/warming/cache/model/dimensions are not engine-composed per database.
- Auto-index/search/embedding work should default disabled in copperDB, with explicit per-DB opt-in for extra implicit/background work and a CLI kill switch that acts as the hard global stop for all indexing.
- Engine composition still needs to thread auth, audit, compliance, retention, replication/fabric, search/index, knowledge-policy, and telemetry consistently through all query/protocol entrypoints.

Documentation actions:

- Add `dbconfig` as a Layer 5 composition dependency even if implemented as a crate under config/multidb later.
- Add deterministic engine tests for effective per-DB config resolution before enabling any automatic search/index work.

## Layer 6 Audit: Protocols And User-Facing APIs

### server

- NornicDB has admin API surfaces for per-DB config keys/effective config and search/vector control. copperDB now has per-database config admin routes plus effective-config views; remaining server-surface parity is the broader search/vector control plane and other distributed status or repair endpoints backed by engine APIs and auth gates.
- Distributed/fabric/search/repair status routes should remain open until backed by engine APIs and auth gates.

### bolt

- copperDB Bolt remains incomplete relative to NornicDB's authentication and role propagation expectations. Bolt `HELLO` auth, transaction state, per-DB routing, and caller roles must feed the same engine path as HTTP.

### graphql

- copperDB GraphQL is still a stub schema/resolver layer. NornicDB has schema/resolver structure. Needed work includes node/edge traversal resolvers, mutations, pagination, subscriptions/streaming where applicable, auth propagation, and engine-backed execution. GraphQL is deferred out of the first runnable MVP and should follow the core distributed engine.

### mcp

- NornicDB exposes real MCP tools: `store`, `recall`, `discover`, `link`, `task`, and `tasks`. copperDB MCP currently has protocol/tool scaffolding only. MCP is deferred out of the first runnable MVP; `discover` should not be implemented until search/vector runtime is available.

### heimdall

- NornicDB Heimdall includes governance/quality-control workflows around LLM-backed suggestion review, scheduler, plugin points, and RBAC filtering. copperDB currently has rate-limiter/anomaly scaffolding only. Heimdall, reranking, and broad inference lifecycle work are deferred until after the core distributed engine works; this also keeps auto-TLP/auto-link disabled by default.

### plugins and APOC

- APOC compatibility is out of MVP scope. However, builtin function and procedure registration should be designed with plugin hooks so APOC-style extensions can be added later without rewiring Cypher call dispatch.

### convert and executable assembly

- `convert`: copperDB has value conversion basics, but streaming import/export, format detection, batch conversions, and validation rules remain open.
- executable assembly: copperDB has startup composition, but no per-DB config CLI override model equivalent to NornicDB's `CLIOverrides` plus `PerDBOverrides` path.

## Priority Backlog From Audit

1. Add the hybrid transaction-time architecture: keep Dynamo quorum for data durability, but introduce a consensus-backed transaction-time oracle for authoritative begin/commit/read-fence allocation, with Paxos v2 as the target distributed implementation.
2. Thread bookmark/read-fence semantics through txsession, engine, replication, and fabric so distributed MVCC snapshot isolation and RYOW have an enforceable contract.
3. Add per-database config store/resolver with allowed keys and precedence: defaults, global config/env/YAML, per-DB stored overrides, CLI overrides. Default automatic search/index/embedding work to disabled for copperDB unless a database opts in.
4. Add search/index warming lifecycle docs and implementation hooks: BM25, vector, embedding; `startup` and `lazy`; deterministic state transitions.
5. Split Layer 4 index work into property/range/temporal parity versus fulltext/vector runtime parity.
6. Port or consciously defer NornicDB's storage async engine: write-behind cache, flush hold/result, async count/read consistency, callback/event deadlock tests.
7. Expand MVCC/WAL documentation with snapshot-visible indexes, temporal point-in-time lookup, lifecycle pruning, WAL repair, and corruption diagnostics.
8. Add distributed production-hardening TODOs: multi-region replication, transport security, chaos tests, peer metrics GC, fragment executor, remote fragment execution, distributed transaction context.
9. Add MVP search runtime TODOs: BM25 fulltext, CPU vector runtime, HNSW/IVFPQ strategy support, vector file store, search persistence/versioning, decay filter, and observability. Defer GPU acceleration and rerank/MMR/local-LLM lifecycle until late-stage parity.
10. Add protocol/runtime TODOs: Bolt auth/role propagation, plugin-ready builtin function/procedure registration, and streaming import/export conversion utilities. copperDB already has server per-DB config routes plus effective-config views; MCP tools, Heimdall governance, GraphQL resolvers, and APOC compatibility remain deferred until after the core distributed engine works.

## Audit Notes

- This audit intentionally does not change package completion status by itself. It records drift that should guide the next implementation slices.
- Some copperDB architecture choices are deliberate deviations from NornicDB, especially promoting topology to Layer 0 and keeping Rust dependency cycles out of the workspace. Those deviations are acceptable as long as docs explain the reason and the consumer contracts remain equivalent.
- When NornicDB defaults conflict with copperDB target policy, copperDB should prefer explicit per-DB opt-in for automatic work. This is especially important for automatic search/index/embedding features that can create memory pressure or surprise operators.
- MVP scope deliberately excludes MCP, GraphQL, Heimdall, APOC, GPU acceleration, reranking, and broad inference lifecycle work. These remain in the register as post-core-distributed-engine parity items.

## Full Agent Findings Register

This register preserves every concrete item returned by the layer audit agents. Items may duplicate the synthesized audit above; they are repeated here so lower-priority or follow-up findings are not lost before plan review.

### Agent A: Layers 0-2

Source report: architecture layer parity audit for `util/buildinfo/envutil/config/errors/otel/lifecycle/topology`, `kms/encryption/auth/audit/security/compliance`, and `storage/cache/pool/txsession/retention/multidb`.

Layer 0 package structure findings:

- `otel` versus `observability`: copperDB has a standalone `otel` crate while NornicDB has a standalone `observability` package. The audit marked this as a naming and feature-catalog comparison item, not an immediate correctness bug.
- `topology`: copperDB has a separate Layer 0 `topology` crate, while NornicDB embeds topology behavior in `linkpredict` and `inference`. This is an intentional architecture deviation in copperDB, but docs must continue explaining why topology is foundational in the Rust graph.

Layer 0 config/default findings from NornicDB `pkg/config/config.go`:

- `WALSyncMode` default is `batch`, described by the audit as fsync every 100ms and a throughput/safety tradeoff.
- `WALAutoCompactionEnabled` default is `true`, with snapshots/truncation reducing WAL growth.
- `StrictDurability` default is `false`; enabling it means fsync-per-write and sync writes, with an expected 2-5x write penalty.
- `WALRetentionMaxSegments` default is `0`, meaning unlimited retention.
- `EncryptionEnabled` default is `false`.
- `MinPasswordLength` default is `8`.
- copperDB docs do not yet clearly preserve storage WAL sync mode, auto-compaction defaults, encryption defaults/provider selection, password-policy defaults, or feature-flag defaults.

Feature flag findings from NornicDB `pkg/config/feature_flags.go`:

- `NORNICDB_KALMAN_ENABLED` defaults disabled; copperDB has no visible equivalent feature flag.
- `NORNICDB_EDGE_PROVENANCE_ENABLED` defaults enabled; copperDB has no visible equivalent feature flag.
- `NORNICDB_COOLDOWN_ENABLED` defaults enabled; copperDB has no visible equivalent feature flag.
- `NORNICDB_EVIDENCE_BUFFERING_ENABLED` defaults enabled; copperDB has no visible equivalent feature flag.
- `NORNICDB_PER_NODE_CONFIG_ENABLED` defaults enabled; copperDB has no visible equivalent feature flag.
- `NORNICDB_WAL_ENABLED` defaults enabled; copperDB has no visible equivalent feature flag.
- `NORNICDB_PARSER` selects `nornic` fast mode or `antlr` strict mode; copperDB has no equivalent runtime parser-selection flag.
- `NORNICDB_AUTO_TLP_ENABLED` defaults disabled; copperDB should keep automatic link prediction disabled by default unless explicitly enabled per database.
- `NORNICDB_GPU_CLUSTERING_ENABLED` defaults disabled; copperDB has no equivalent runtime flag.
- Recommendation from the agent: document a feature-flag strategy where safety features may default enabled, but experimental automatic behavior defaults disabled.

Layer 1 security/auth/audit/compliance findings:

- `encryption`: NornicDB splits envelope encryption, DEK cache, and rotation across files such as `dek_cache.go`, `envelope.go`, and `rotation.go`; copperDB has a more bundled Rust implementation. DEK cache hit behavior and rotation/rekey performance should remain explicit audit/test items.
- `audit` and `compliance`: both codebases have packages, but the agent called out NornicDB's explicit per-feature tests as stronger evidence of coverage. copperDB should preserve audit/compliance test coverage as a parity criterion, especially where protocol entrypoints are involved.

Layer 2 storage findings:

- NornicDB storage has roughly 160+ Go files compared with a much smaller copperDB Rust storage surface; the agent treated this as the largest structural mismatch.
- `async_engine.go`: NornicDB has async write-behind, flush intervals, pending-write tracking, label index inversion, callbacks, in-flight versus persisted state, and flush result/hold behavior. copperDB does not yet expose an equivalent async write cache layer.
- MVCC maturity: NornicDB has `badger_mvcc*` files, MVCC pruning, rebuild, and temporal visibility; copperDB has foundational `MvccSnapshot`, `MvccVersion`, and `MvccHead` concepts but lacks the broader extension surface.
- WAL maturity: NornicDB has segmenting, compaction, repair, corruption diagnostics, degraded mode, atomic records, recovery chunking, and durability tests. copperDB currently has a narrower WAL/storage contract.
- Index maintenance: NornicDB maintains property indexes in storage with rebuild/update/deindex paths; copperDB has storage/indexing contracts and index-definition persistence, but the full storage-maintenance breadth should remain visible.
- Async cache consistency: NornicDB's async engine tracks in-flight versus committed state; copperDB's cache crate is a read/query/write-through acceleration layer and not a storage write-behind consistency model.
- Underlying KV layout differs: NornicDB uses Badger keyspaces for nodes, edges, constraints, indexes, and metadata; copperDB uses sled trees for metadata, nodes, edges, and indexes. This is acceptable as an implementation-language/storage choice but should not hide missing operational behavior.

Layer 2 auto-indexing/index-default findings:

- The agent inferred NornicDB auto-creates label/index structures around MATCH/WHERE and maintains composite property indexes on update/delete, but this was noted as inferred from implementation patterns and should be verified before being promoted to a hard requirement.
- copperDB currently has explicit index DDL, persisted definitions, lookup-path preference, and ordered range encoding; the agent found no evidence of automatic index creation.
- Documentation should state the copperDB target clearly: no surprise automatic property/search/vector indexes by default; explicit DDL is the default, declared indexes still reload/rebuild per database from schema, and any automatic index/search/embedding behavior beyond schema DDL must be disabled/off until a database opts in.

Layer 2 MVCC extension-interface findings from NornicDB `pkg/storage/types.go`:

- `MVCCVisibilityEngine`: snapshot-visible reads for audit/rollback are not fully ported.
- `MVCCIndexedVisibilityEngine`: historical label/type/topology query acceleration is not fully ported.
- `TemporalLookupEngine`: temporal point-in-time lookup is not fully ported.
- `TemporalMaintenanceEngine`: temporal history pruning is not fully ported.
- `PrefixStatsEngine`: fast per-namespace node/edge counts now have a maintained storage baseline for namespace prefixes.
- `AdjacentEdgesEngine`: dual-direction adjacency now has a storage-owned baseline consumed by local eval traversal.
- `NamespaceLabelStatsProvider`: per-namespace label cardinality now has a maintained storage baseline.
- `NamespaceSchemaProvider`: namespace-scoped schema snapshots now have a dedicated storage keyspace; this corrects the earlier planning assumption that the global schema catalog could stand in for isolated per-database schema.

Layer 2 indexing architecture findings:

- NornicDB `pkg/indexing` is primarily text tokenization/BM25 support (`config.go`, searchable properties, extraction, tokenization).
- copperDB `crates/indexing` owns an index catalog and query planning/lookup selection.
- This is a structural split: NornicDB keeps catalog/index maintenance closer to storage/Cypher search paths, while copperDB centralizes property index catalog logic. The agent judged copperDB's unified model potentially useful for future vector backends, but the difference must be documented.

Layer 2 cache findings:

- Both codebases have query-cache concepts with TTL.
- NornicDB `pkg/cache/query_cache.go` uses `container/list` plus a hash map for O(1) LRU movement.
- copperDB `crates/cache/src/lib.rs` uses `VecDeque` plus a hash map; the agent flagged possible O(n) movement/retain costs as cache sizes grow.

Layer 2 txsession findings:

- NornicDB and copperDB both have transaction state machines with modes, states, operation buffering, and distributed/logical IDs.
- The agent marked this as good alignment and not a major gap.

Layer 2 retention/compliance findings:

- Both implement data lifecycle policies, legal holds, and erasure request tracking.
- The agent marked retention as structurally aligned, while still requiring tests and protocol integration to support compliance claims.

Layer 2 concrete missed parity rank order from the agent:

1. `storage`: async write-behind engine is complete in NornicDB and missing in copperDB; critical write-latency and flush-batching gap.
2. `storage`: MVCC snapshot-visible indexing is represented by NornicDB extension interfaces and missing in copperDB; affects historical graph audit and point-in-time restore.
3. `storage`: distributed MVCC pruning/background maintenance is missing; version history can grow without cleanup controls.
4. `storage`: WAL segment repair and diagnostics are broader in NornicDB; copperDB needs explicit repair/corruption recovery seams.
5. `config`: feature flag system is missing in copperDB; experimental and safety behavior cannot be toggled operationally.
6. `indexing`: automatic index creation behavior is unclear/inferred; copperDB should document explicit DDL defaults and disabled automatic work.
7. `storage`: `PrefixStatsEngine` has a maintained namespace-prefix counter baseline; broader streaming and async count consistency remain open under async-engine parity.
8. `storage`: `AdjacentEdgesEngine` has a storage-owned adjacent-edge baseline; broader remote/distributed traversal optimizations remain open where transport still exposes split directional calls.
9. `encryption`: DEK caching and rotation behavior should be verified as explicit parity coverage.
10. `config`: WAL auto-compaction defaults are not clearly documented in copperDB.

### Agent B: Layer 3

Source report: architecture layer audit for `replication`, `fabric`, `search`, `qdrantgrpc`, `nornicgrpc`, and distributed engine/server hooks.

Replication findings:

1. Multi-region replication architecture is missing as a first-class copperDB path. NornicDB has local Raft cluster per region, async cross-region WAL streaming, primary/failover promotion, and cross-region connection pooling/streaming coordination.
2. Replication transport security is still incomplete in copperDB. NornicDB `transport_security.go` includes TLS config building, cert/key loading, CA validation, TLS 1.2/1.3 policy, cipher parsing, mTLS client verification, and `AuthSecret` token-based auth with time-skew tolerance; copperDB now has an mTLS-capable tonic baseline plus startup validation for required cert or key combinations, active certificate windows, and cert-or-key consistency, but not full parity.
3. Chaos testing infrastructure is missing. NornicDB `chaos_test.go` has chaos configs for cross-region latency/jitter, partition latency, packet loss, corruption, and local/cross-region/global scenarios.
4. Peer metrics garbage collection is missing. NornicDB `peer_metrics_gc.go` removes stale peer metric entries after topology changes to avoid unbounded metric cardinality.

Fabric findings:

1. Full `FabricExecutor` fragment-based routing is missing. NornicDB traverses fragment trees (`Init`, `Leaf`, `Exec`, `Apply`, `Union`), dispatches local/remote fragments, manages distributed transaction context, and supports bounded correlated APPLY clauses.
2. Fabric query plan caching is missing. NornicDB `plan_cache.go` caches plans by query/options with LRU and TTL.
3. Remote fragment execution is missing. NornicDB `remote_executor.go` sends fragments to remote shard coordinators, forwards OIDC tokens, streams results, and propagates errors.
4. Distributed transaction context is missing. NornicDB `transaction.go` manages per-shard subtransaction state, distributed commit/rollback boundaries, timeouts, and cleanup.

Layer 3 search findings:

1. ANN profile and hybrid cluster routing are missing. NornicDB `ann_profile.go` and `hybrid_cluster_routing.go` build vector-statistics profiles and select relevant clusters by semantic similarity plus lexical term overlap.
2. Advanced vector strategies are missing. NornicDB has IVFPQ, HNSW, GPU/Metal acceleration, index-selection heuristics, and benchmarks.
3. Search index persistence with format versioning is missing. NornicDB `fulltext_index_v2_persist.go` and `vector_file_store.go` reject mismatched versions, rebuild safely, and debounce persistence.
4. Decay filter integration is missing. NornicDB `decay_filter.go` applies knowledge-policy decay during search ranking.
5. Vector file store is missing. NornicDB uses durable append-only vector storage, sparse embedding handling, and corruption recovery.

Qdrant gRPC findings:

1. Full Qdrant service implementation is missing. NornicDB has collection, points, snapshot, discovery, and health service coverage; copperDB has much thinner client support.
2. Qdrant collections API is missing: create, list, update, delete, config persistence, and dynamic index lifecycle management.
3. Qdrant points extended API is missing: scroll cursors, batch operations, conditional updates, and efficient bulk indexing.
4. Qdrant snapshots service is missing: create/recover point-in-time backups for vector index disaster recovery.

NornicDB gRPC findings:

1. Search service implementation is missing. NornicDB receives distributed `SearchQuery` calls over gRPC, executes local shard search, returns ranked batches, and handles auth forwarding.
2. gRPC authentication/authorization is still incomplete. NornicDB has auth tests and OIDC token forwarding; copperDB now distinguishes internal replica RPCs from client-facing data-path RPCs by validating admin JWTs through the unified auth core for replica apply/read while forwarding caller tokens and reapplying per-database read enforcement on ranked-search, hydration, and distributed graph-read RPCs, but it still must extend caller identity and entitlement forwarding across the remaining remote operations and broaden cluster-client token handling before remote execution is fully safe.

Distributed engine/server hook finding:

- Server-side distributed routes are missing or unclear. The agent called out fabric shard-map admin, distributed search execution, repair queue inspection, and cross-region replication status as route families that should remain open until backed by engine APIs and auth gates.

Layer 3 summary table from the agent:

- Multi-region replication: missing in copperDB, complete in NornicDB; blocks geo-distributed failover.
- Transport security (TLS/mTLS): an mTLS-capable tonic baseline now exists in copperDB, and startup now rejects inactive gRPC certificate bundles and mismatched configured cert or key pairs before bind, while broader certificate-policy, rotation, and TLS-version or cipher parity remain incomplete relative to NornicDB; this still blocks compliant inter-node deployment.
- Chaos testing infrastructure: missing in copperDB, complete in NornicDB; weakens failure-mode confidence.
- Peer metrics GC: missing in copperDB, complete in NornicDB; risks metric-cardinality growth.
- Fabric fragment routing: data structures only in copperDB, complete in NornicDB; multi-shard queries are non-functional without it.
- Query plan caching: missing in copperDB, complete in NornicDB; repeated distributed queries replan.
- Remote fragment executor: missing in copperDB, complete in NornicDB; cross-shard execution impossible.
- Distributed transaction context: missing in copperDB, complete in NornicDB; multi-shard mutation failures unrecoverable.
- ANN profiling and hybrid routing: missing in copperDB, complete in NornicDB; large search stays O(n).
- Advanced vector indexing (IVFPQ, HNSW GPU): missing in copperDB, complete in NornicDB.
- Index persistence with versioning: missing in copperDB, complete in NornicDB.
- Decay filter integration: missing in copperDB, complete in NornicDB.
- Vector file store: missing in copperDB, complete in NornicDB.
- Qdrant collections API: missing in copperDB, complete in NornicDB.
- Qdrant points extended API: missing in copperDB, complete in NornicDB.
- Qdrant snapshots: missing in copperDB, complete in NornicDB.
- gRPC search service: missing in copperDB, complete in NornicDB.
- gRPC authentication and transport: internal replica apply/read now validate admin JWTs through the unified auth core when security is enabled and bypass auth under `--no-auth`, ranked-search and hydration forward caller bearer tokens and reapply per-database read auth on the remote node, and config startup now rejects inactive gRPC certificate bundles plus mismatched configured cert or key pairs, while broader caller-forwarded auth, cluster-client token generation and rotation handling, entitlement parity, and fuller secret and certificate lifecycle handling remain incomplete relative to NornicDB.

### Agent C: Layer 4 Query/Index/Search

Source report: layer 4 audit for `cypher`, `filter`, `indexing`, `eval`, `search` integration, `temporal`, `decay`, `knowledgepolicy`, `math`, `simd`, `embeddingutil`, `textchunk`, `embed`, `localllm`, `inference`, `vectorspace`, and `linkpredict`.

Search package findings:

- BM25 fulltext runtime is missing: copperDB has search result/scaffold types; NornicDB has `FulltextIndexV2`, query-plan cache, IDF computation, and posting lists.
- HNSW vector index is missing: NornicDB has graph-based ANN with M/efConstruction/efSearch config and cache-friendly neighbor storage.
- IVF-PQ compression is missing: NornicDB has product quantization, 1-128 variable segments, training sample bounds, and nprobe adaptation.
- GPU acceleration is missing: NornicDB has CUDA/Metal paths for exact scoring and GPU k-means clustering.
- Decay filtering in search is missing: NornicDB applies decay multipliers during ranking.
- Index strategy selection is missing: NornicDB selects brute-force below 5k vectors, HNSW for mid-size sets, and compressed IVFPQ above very large thresholds.
- Reranking is missing: NornicDB has local-LLM reranking, MMR diversity, and Kalman cross-validation hooks.
- Result/query caching is missing or much thinner: NornicDB has version-stamped BM25 query-plan cache and per-query LRU cache with TTL.
- Hybrid RRF merging is incomplete: copperDB has `RrfSearchOutcome` structures, while NornicDB has full RRF with configurable K, per-source weighting, and deterministic multi-shard merge.
- Search observability is missing: NornicDB tracks latency per stage, caches bound latency observers, and emits spans.

Search performance implications from the agent:

- BM25 query cost can blow up from indexed posting-list lookup to broad scans when a maintained inverted index is missing.
- Vector index build/search cost scales poorly without HNSW/IVFPQ strategy switching.
- Decay visibility leaks occur if stale/suppressed entities are not filtered during search ranking.

Knowledgepolicy runtime findings:

- ON ACCESS mutation flusher is incomplete. NornicDB has a standalone flusher applying mutations per namespace and entity type, emitting metrics and evaluating expressions.
- Kalman filter execution in ON ACCESS is missing or incomplete. NornicDB `ProcessKalmanMutation()` updates state from mutation input.
- Access metadata visibility suppression timing differs: the agent noted NornicDB applies suppression at scorer time before ranking, while copperDB has eval-time/local scoring pieces.
- Overflow property persistence for ON ACCESS computed properties is missing or incomplete.
- Multi-tenant namespace isolation in policy/scorer resolution is missing or incomplete.

Temporal and decay integration findings:

- Adaptive decay multiplier is missing. NornicDB `DecayModifier` uses Kalman velocity to adjust decay multipliers, including slower decay for frequently accessed entities and faster decay for declining entities.
- Kalman velocity is not fully integrated into copperDB decay calculations.
- Daily/periodic pattern detection is missing.
- Cold-storage/archive candidate decisions from decay/access patterns are missing.

Query optimization and hot-path findings:

- Query shape detection/routing is incomplete. NornicDB routes UNWIND/MERGE/MATCH patterns to optimized paths.
- Simple `MATCH ... LIMIT` fast path now exists for the narrow single-node `MATCH (n:Label) RETURN n LIMIT k` shape by routing through label-backed early-stop retrieval, but broader trace-backed parity and richer `MATCH ... LIMIT` shapes remain open.
- Compound query fast path is missing or incomplete for mutation chains and relationship link paths.
- Call-tail traversal fast path is missing or incomplete for bounded traversal inside procedure/call execution.
- UNWIND batch fast path is still incomplete overall; copperDB now routes the basic `UNWIND ... MERGE ... RETURN` pipeline upsert shape in addition to the existing seeded `UNWIND ... MATCH ... CREATE` slice, but broader batch mutation shapes and trace parity remain open.
- Pipeline composite routing is missing or incomplete; NornicDB can route compatible clause chains through a single evaluator rather than materializing between every clause.

Vector/embedding lifecycle findings:

- Cached embedder is missing or incomplete: NornicDB has LRU/TTL caching, hit/miss metrics, and wrapper behavior.
- Backend reporting is missing: NornicDB exposes backend labels such as CPU/CUDA/Metal/Vulkan for observability.
- Crash recovery is missing: NornicDB has FFI/local GGUF panic/segfault recovery and graceful degradation.
- GPU memory management is missing or incomplete for embedding batches.
- Ollama/remote backend fallback/reporting semantics are missing or incomplete.
- Standard `ChunkText`/token-aware chunking interface is missing or incomplete.
- Embedding recomputation for suppressed entities should be avoided once decay/search filtering exists.

SIMD/math findings:

- copperDB `simd` and `math` crates are stubs or very thin compared with NornicDB.
- NornicDB has ARM64 NEON acceleration.
- NornicDB has Apple Metal GPU dispatch for batch scoring.
- This affects vector dot/cosine throughput on ARM64 and macOS.

Indexing catalog and preference findings:

- Both systems prefer more specific/longer matching index definitions in broad terms.
- Exact-suffix preference logic for composite indexes remains a drift/follow-up item: for queries with exact suffix predicates after a range predicate, NornicDB behavior/documentation prefers the composite with more matching exact fields and storage-side deterministic filtering.
- copperDB has improved range/temporal semantics, but this exact-suffix composite preference should stay explicitly tested.

Index runtime type semantics findings:

- `FULLTEXT` and `VECTOR` index kinds must not route through property/range lookup paths.
- NornicDB routes `FULLTEXT` through BM25 procedures and `VECTOR` through vector nearest-neighbor procedures.
- copperDB currently persists typed catalog rows and intentionally keeps `FULLTEXT`/`VECTOR` metadata-only until maintained runtime paths exist.
- Documentation should state that `FULLTEXT`/`VECTOR` are not inferred from ordinary `WHERE` equality/range clauses and should route through explicit procedures once implemented.

Auto-indexing behavior finding:

- The agent found defaults unspecified in the docs. copperDB should state explicit index creation is required by default, property writes do not auto-create indexes, schema-declared indexes still reload/rebuild per database, and any automatic index/search/vector/embedding behavior beyond schema DDL must be opt-in per database.

Inference/local LLM findings:

- `localllm` has TODO/stub behavior for loading `libllama`/local models.
- `inference` has TODO/stub behavior for real model loading.
- NornicDB has integration tests and crash recovery around local inference.

Vector persistence/file store findings:

- copperDB's vector persistence story is not articulated at NornicDB parity.
- NornicDB `vector_file_store.go` persists vectors with a format-version header, rejects mismatches, triggers rebuilds, and supports multiple strategies over the same vector set.

Link prediction findings:

- copperDB has basic stateless heuristics such as common neighbors, Jaccard, and Adamic-Adar.
- NornicDB integrates link prediction with topology and distributed/cross-shard context.

Layer 4 summary table from the agent:

- Search execution: copperDB stubs only; NornicDB has production BM25/HNSW/IVF-PQ/GPU; critical.
- Knowledgepolicy runtime: copperDB types/local pieces only; NornicDB has full flusher and Kalman integration; critical.
- Temporal decay integration: copperDB separate packages; NornicDB integrated `DecayModifier`; high.
- Query optimization hot paths: copperDB types/pieces only; NornicDB has routed paths and metrics; high.
- Vector/embedding cache: missing in copperDB; NornicDB has LRU/TTL/crash recovery; high.
- SIMD platforms: copperDB stubs; NornicDB ARM64 NEON and Metal; medium.
- Index preference composites: copperDB basic length-based preference; NornicDB exact-suffix aware behavior should be preserved/tested; medium.
- LocalLLM/inference: copperDB stubs/TODOs; NornicDB integration and crash handling; medium.
- Vector file store: unclear/missing in copperDB; NornicDB versioned multi-strategy store; medium.
- Linkpredict topology: copperDB heuristics; NornicDB distributed-aware topology; low.

### Agent D: Layers 5-6

Source report: audit for embedded engine composition, server, Bolt, GraphQL, MCP, Heimdall, convert, executable assembly, and config.

Per-database configuration findings:

- NornicDB has a comprehensive per-DB config system; copperDB lacks it.
- NornicDB keys include `NORNICDB_SEARCH_BM25_ENABLED`, `NORNICDB_SEARCH_BM25_WARMING`, `NORNICDB_SEARCH_VECTOR_ENABLED`, `NORNICDB_SEARCH_VECTOR_WARMING`, plus embedding, HNSW, IVF-HNSW, K-means, auto-links, auto-TLP, and MVCC lifecycle keys.
- copperDB `DatabaseManager` tracks name/path/status only; no per-DB feature toggles.
- copperDB `DatabaseConfig` is global/top-level and lacks per-DB search warming, BM25/vector enablement, MVCC, and embedding parameters.
- Missing pieces: `dbconfig.Store`, `dbconfig.Resolver`, durable per-DB runtime config, admin routes such as `GET /admin/databases/{name}/config` and `POST /admin/databases/{name}/config/{key}`.

Search warming findings:

- NornicDB has CLI flags for `search-bm25-warming` and `search-vector-warming` with `startup|lazy` values.
- copperDB has no equivalent flags, env vars, or per-DB warmup control.
- copperDB startup/search warmup behavior is implicit and undocumented.
- Documentation must adapt the agent's wording to the user target: copperDB automatic search/index work should default disabled/off, and warming modes only apply after a database enables the relevant feature.

MCP findings:

- NornicDB exposes `store`, `recall`, `discover`, `link`, `task`, and `tasks` MCP tools.
- copperDB has MCP request/response/tool scaffolding and error variants but no actual handlers for those tools.
- Missing pieces: tool registry, database-scoped executor wiring, semantic search integration for `discover`, task model/persistence, and MCP protocol routing through HTTP/Bolt adapters.

Heimdall findings:

- NornicDB Heimdall includes LLM-backed quality control (`bifrost.go`), suggestion review/approval routes, pluggable LLM generators, RBAC context filtering, scheduler, and plugin system.
- copperDB Heimdall currently has rate limiting/anomaly scaffolding only.
- Missing pieces: Bifrost quality-control workflow, anomaly rules engine, approval queue, plugin registry/lifecycle, and OpenAI/Ollama/local/custom LLM adapters.

Executable assembly/config precedence findings:

- NornicDB precedence is explicit: CLI flags, env vars, YAML config, built-in defaults, plus `CLIOverrides` and `PerDBOverrides` maps for staged resolution.
- copperDB CLI captures top-level flags and `load_with_precedence` handles global config only.
- Missing pieces: per-DB CLI override capture, tested `dbconfig.Resolver` precedence, and a syntax/model for per-DB CLI override keys.

Embedding lifecycle findings:

- NornicDB has `embedding-enabled` default false and `embedding-warming startup|lazy`, plus per-DB embedding dimensions, model, cache size, and GPU layers.
- copperDB has vector space config and embed scaffolding but no full embedding service lifecycle or warmup control.
- Missing pieces: `--embedding-enabled`, `--embedding-warming`, per-DB embedding dimensions/model/cache overrides, and background embedding worker lifecycle.

Authentication/RBAC protocol findings:

- NornicDB propagates auth through Bolt `HELLO`, auth verification, and role-filtering expectations.
- copperDB has HTTP auth scaffolding and startup seeding, but Bolt integration, distributed query role propagation, and per-DB roles remain unclear/incomplete.
- Missing pieces: Cypher result post-filtering by role/policy, Bolt role propagation into distributed execution, and per-database role assignments.

GraphQL findings:

- NornicDB has gqlgen config, schema directory, resolvers, and generated interfaces.
- copperDB GraphQL currently has stub `QueryRoot`, stub `MutationRoot`, and an empty async-graphql schema path.
- Missing pieces: node/edge traversal resolvers, create/update/delete mutations, engine-backed schema stitching, subscriptions/streaming if applicable, auth propagation, and relay-style pagination.

Convert/import-export findings:

- NornicDB convert has numeric casting/validation, slice/vector operations, and tests.
- copperDB convert has a Cypher-style `Value` enum, scalar conversions, and base64 helpers.
- Missing pieces: batch conversions, format detection for CSV/JSON/Parquet, streaming converters for large datasets, and validation rules such as enum/constraint checking.

Layers 5-6 summary table from the agent:

- Per-DB config toggles: missing in copperDB; NornicDB has 40+ keys plus resolver; critical/high effort.
- Search warming strategy: undocumented/missing in copperDB; NornicDB has CLI and per-DB controls; high/medium effort.
- MCP tools: copperDB stub types only; NornicDB has six production tools; high/high effort.
- Heimdall governance: copperDB rate limiter only; NornicDB has quality control and LLM workflows; medium/very high effort.
- Config precedence: copperDB global/implicit; NornicDB explicit and tested; medium/medium effort.
- Embedding warming: missing flags/lifecycle in copperDB; NornicDB full lifecycle; medium/medium effort.
- Auth in protocols: copperDB scaffolding; NornicDB fully wired; low/medium effort.
- GraphQL resolvers: copperDB stubs; NornicDB partial implementation; low/medium effort.
- Convert utilities: copperDB basic; NornicDB extended; low/low effort.