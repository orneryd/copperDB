# copperDB / NornicDB Parity Audit

Date: 2026-05-26

This audit compares the local Rust workspace at `~/src/copperDB` against the local Go upstream at `~/src/NornicDB`. NornicDB is the source of truth for package boundaries and runtime behavior. copperDB is a Rust conversion of that architecture.

Upstream reference noted in existing docs: NornicDB `main` commit `fd37a21e9694c5739b6afe2e6d78a4225b55c981`.

## Executive Summary

copperDB has a broad crate skeleton that mirrors most NornicDB packages, and several central paths are functional enough for smoke tests: startup configuration, HTTP health/status/auth endpoints, simple Cypher parsing/evaluation, basic storage, retention CRUD, and a partial observability catalog. However, the implementation is not parity-complete. Most crates are thin baselines compared with their Go counterparts, several upstream packages are missing entirely, and multiple declared crates are not actually threaded into the running server.

Highest risk gaps:

1. Bolt is only a TCP listener plus handshake and metric counters. It does not dispatch Bolt messages into auth, transactions, Cypher execution, or result streaming.
2. Cypher/eval supports a useful subset, but relationship MATCH/OPTIONAL MATCH is explicitly unsupported and the upstream grammar/evaluator surface is far larger.
3. Storage has baseline sled, MVCC, WAL, schema, and catalog primitives, but is missing the upstream lifecycle, async write-behind, reader registry, pruning/rebuild controller, full index maintenance, and namespace transaction semantics.
4. `knowledgepolicy`, `lifecycle`, and `errors` have no Rust crate equivalents yet. `observability` appears intentionally renamed to `otel`, but only part of the observability implementation has been ported.
5. Several crates are present but effectively stubs or local-only models: `graphql`, `mcp`, `qdrantgrpc`, `nornicgrpc`, `inference`, `localllm`, and parts of `gpu`.
6. The running `copperdb` binary wires HTTP, Bolt, config, and otel, but it does not start or coordinate many subsystem crates through a lifecycle supervisor.

## Rename / Branding Audit

The old core engine path and package naming still contained magnet-era naming:

- Old: `crates/magnetdb`
- Old package: `copperdb-copperdb`
- Old Rust crate import: `copperdb_copperdb`
- Old public error type: `MagnetError`
- Old non-idiomatic public types: `copperdb`, `copperdbError`

As part of this audit, those source-level references were renamed:

- New path: `crates/engine`
- New package: `copperdb-engine`
- New Rust crate import: `copperdb_engine`
- New public engine type: `CopperDb`
- New public error types: `CopperDbError`, `CopperDbServerError`

Exact source checks for `magnetrdb`, `magnetdb`, `copperdb-copperdb`, `copperdb_copperdb`, `MagnetError`, `copperdbError`, and `copperdb::` are clean in `Cargo.toml`, `crates/**`, `README.md`, and `docs/**` after the rename.

Note: Git reports this as delete/add (`crates/magnetdb` deleted, `crates/engine` added). That is expected for a directory rename in the current tooling.

## Package Inventory

NornicDB packages found under `pkg/`:

- `audit`, `auth`, `bolt`, `buildinfo`, `cache`, `compliance`, `config`, `convert`, `cypher`, `embed`, `embeddingutil`, `encryption`, `envutil`, `errors`, `eval`, `fabric`, `filter`, `gpu`, `graphql`, `heimdall`, `indexing`, `inference`, `kms`, `knowledgepolicy`, `lifecycle`, `linkpredict`, `localllm`, `math`, `mcp`, `multidb`, `nornicdb`, `nornicgrpc`, `observability`, `pool`, `qdrantgrpc`, `replication`, `retention`, `search`, `security`, `server`, `simd`, `storage`, `temporal`, `textchunk`, `txsession`, `util`, `vectorspace`.

copperDB crates found under `crates/`:

- `audit`, `auth`, `bolt`, `buildinfo`, `cache`, `compliance`, `config`, `convert`, `copperdb`, `cypher`, `decay`, `embed`, `embeddingutil`, `encryption`, `engine`, `envutil`, `eval`, `fabric`, `filter`, `gpu`, `graphql`, `heimdall`, `indexing`, `inference`, `kms`, `linkpredict`, `localllm`, `math`, `mcp`, `multidb`, `nornicgrpc`, `otel`, `pool`, `qdrantgrpc`, `replication`, `retention`, `search`, `security`, `server`, `simd`, `storage`, `temporal`, `textchunk`, `txsession`, `util`, `vectorspace`.

## Missing Or Renamed Packages

| NornicDB package | copperDB status | Audit finding |
| --- | --- | --- |
| `pkg/nornicdb` | `crates/engine` | Present as core embedded engine after rename from `crates/magnetdb`. It wires parser, eval, storage, tx manager, and query cache for embedded use, but not all subsystems. |
| `pkg/observability` | `crates/otel` | Renamed, not missing. Partial port: metric catalog and in-memory validation exist; listener/provider/registry/openmetrics/logging/redaction/pprof/resource/recovery surfaces are not parity-complete. |
| `pkg/errors` | missing | No Rust crate for transient transaction error mapping, retryable Neo4j wire codes, conflict sentinels, or merge commit-time unique conflict tagging. |
| `pkg/knowledgepolicy` | missing | No dedicated crate for decay/promotion policy resolution, access accumulation, ON ACCESS runtime, Kalman policy integration, binding builder, scorer, or storage integration. Partial schema-like shapes exist in `cypher`, `eval`, `storage`, `decay`, and `otel`, but not the package behavior. |
| `pkg/lifecycle` | missing | No supervisor/component abstraction for coordinated startup, first-error cancellation, signal handling, 30s fresh-context shutdown, or reverse drain order. Startup is hand-wired in the binary. |
| `pkg/decay` | `crates/decay` | Rust has a decay crate; this does not replace the missing `knowledgepolicy` package. |

## Size And Surface Comparison

These are rough source metrics from local files, useful only as a scale signal:

| Area | NornicDB Go surface | copperDB Rust surface | Interpretation |
| --- | ---: | ---: | --- |
| `cypher` | 363 files, about 162k LOC | 5 files, about 3.7k LOC | Large parser/evaluator parity gap. Rust is a hand-rolled subset. |
| `storage` | 238 files, about 77.6k LOC | 1 file, about 1.7k LOC | Baseline storage exists, but most lifecycle/index/async/MVCC/WAL orchestration is missing. |
| `bolt` | 49 files, about 16.6k LOC | 4 files, about 713 LOC | Rust has handshake/listener/PackStream pieces, not a full server state machine. |
| `server` | 43 files, about 18.7k LOC | 1 file, about 970 LOC | Basic REST/UI endpoints exist; management and integration surface is much smaller. |
| `observability`/`otel` | 96 files, about 9.5k LOC | 2 files, about 467 LOC | Catalog port exists; provider/export/listener/logging/redaction surfaces missing. |
| `knowledgepolicy` | 38 files, about 7.7k LOC | no crate | Missing behavior. |
| `lifecycle` | 9 files, about 355 LOC | no crate | Missing supervisor contract. |
| `graphql` | 19 files, about 19.3k LOC | 1 file, about 38 LOC | Placeholder schema, not wired to storage. |
| `qdrantgrpc` | 15 files, about 7.2k LOC | 1 file, about 5 LOC | Stub only. |
| `mcp` | 13 files, about 5.7k LOC | 1 file, about 243 LOC | Tool list exists; tool calls return a stub response. |

## Runtime Wiring Status

### Running binary (`crates/copperdb`)

The binary currently wires:

- Config loading from CLI/env/config file/defaults.
- HTTP router from `copperdb-server`.
- Bolt listener from `copperdb-bolt`.
- In-memory telemetry from `copperdb-otel`.
- Static UI serving when a UI dist exists.

The binary does not wire:

- A lifecycle supervisor equivalent to NornicDB `pkg/lifecycle`.
- Replication startup, leadership, cluster membership, or transport binding.
- Fabric routing into the query path.
- GraphQL endpoint into the HTTP router.
- MCP server transport into the HTTP/router or standalone listener.
- gRPC server listener.
- Qdrant client connection.
- Local LLM/model loading.
- KMS provider lifecycle.
- Search indexing workers or distributed search mesh.
- Audit/compliance/security enforcement as middleware across all surfaces.

### HTTP server (`crates/server`)

Wired:

- UI/static route handling.
- Health/status endpoints.
- Basic auth token issue/logout/me.
- Database metadata CRUD through `multidb`.
- Neo4j-style transaction commit endpoint backed by the engine.
- `/db/data/cypher` query endpoint backed by the engine.
- Retention policy/hold/erasure CRUD backed by in-memory `retention` manager.
- A small set of telemetry counters/histograms.

Not wired or incomplete:

- GraphQL schema is not mounted.
- MCP is not mounted.
- gRPC is not started.
- Retention sweep endpoint returns a placeholder and does not delete graph data.
- Database manager is in-memory; persistence/routing/namespaces are incomplete.
- Status counts for nodes/edges are static placeholders.
- Auth is development-mode username/password/JWT; upstream OAuth/RBAC/session coverage is not complete.

### Embedded engine (`crates/engine`)

Wired:

- `StorageEngine`.
- `Parser`.
- `EvalEngine`.
- `TransactionManager` handle.
- Query cache.
- Flush guard pattern around implicit queries.

Not wired:

- Auth/security checks per query.
- Audit logging.
- Compliance checks.
- Replication command routing for writes.
- Fabric database routing.
- Search/index worker integration beyond storage catalog calls.
- Vector/embedding pipeline on writes.
- Retention enforcement during query execution.
- Knowledge policy scoring/access mutation runtime.
- Observability spans/metrics beyond HTTP-level callers.

## Crate Findings

### `bolt`

Status: high priority incomplete.

What exists:

- TCP listener.
- Bolt magic preamble validation.
- Forced Bolt 4.4 version response.
- Message type enum.
- Metrics for connection/message/session counters.

Missing:

- Proper version negotiation from offered client versions.
- Chunked message framing.
- PackStream decode to `BoltMessage` in the connection loop.
- HELLO/LOGON/LOGOFF auth flow.
- RUN/PULL/DISCARD/BEGIN/COMMIT/ROLLBACK dispatch.
- Transaction/session state machine.
- Result records and metadata encoding.
- Error mapping to Neo4j-compatible codes.
- Integration with `auth`, `engine`, `txsession`, `errors`, and `otel` beyond coarse metrics.

### `cypher` and `eval`

Status: useful subset, not parity.

What exists:

- Hand-rolled parser for a subset of Cypher.
- Clauses for MATCH, OPTIONAL MATCH, CREATE, MERGE, RETURN, SET, DELETE, WITH, UNWIND, schema DDL, decay profile/promotion profile/promotion policy shapes.
- Basic evaluator support for node create/match/where/return, simple merge caching, DDL persistence paths, decay/promotion schema rows.

Missing:

- Full upstream grammar and generated parser parity.
- Relationship MATCH and OPTIONAL MATCH execution; Rust explicitly returns `relationship patterns in MATCH are not yet supported`.
- Full path patterns, variable-length paths, shortest path, rich expression/function support, planner behavior, and Neo4j compatibility surface.
- Full knowledge policy semantics behind parsed decay/promotion declarations.
- Full transaction, conflict, retry, and index selection semantics.

### `storage`

Status: baseline engine with important pieces, not parity.

What exists:

- sled-backed key/value storage.
- Node/edge put/get/delete/scan helpers.
- MVCC version/head model and snapshot-visible reads.
- WAL primitives with append/batch/replay/checksum/degraded signaling.
- Schema manager with unique/existence/node-key checks and persistence.
- Storage layout version 0 guard.
- Flush guard placeholder.

Missing:

- Full NornicDB engine interface parity for traversal, bulk ops, streaming, stats, prefix stats, adjacent edges, and namespace deletion.
- MVCC reader registry, pruning/rebuild scheduler, debt controller, and resource-pressure behavior.
- WAL segment diagnostics, degraded mode orchestration, truncation/compaction, and snapshot coordination.
- Async write-behind cache parity and flush hold/results.
- Label/edge/property/range/temporal indexes with deindex workers and cleanup.
- Namespace-pin transaction semantics.
- Storage event notifier.
- Property codec/serializer detection parity.
- Knowledge policy hooks.

### `otel` as `observability`

Status: renamed partial port.

What exists:

- Metric and enum catalog copied from upstream observability.
- Label validation.
- In-memory counter/gauge/histogram samples.
- Basic operation classifier for Cypher metrics.

Missing:

- OpenMetrics listener/exporter.
- Provider and registry implementation.
- Structured logging and logger lifecycle.
- Redaction, PII filtering, baggage filtering, span redaction.
- pprof/debug endpoints.
- Resource and Kubernetes detection.
- Recovery helpers.
- Mandatory field enforcement across live metrics.
- Integration across most crates. The binary currently calls `mock_unimplemented_catalog_metrics`, which indicates many metrics are placeholders.

### `graphql`

Status: placeholder.

What exists:

- Minimal async-graphql schema with `node` query and `create_node` mutation.

Missing:

- Storage/engine wiring. Both handlers have TODOs and return synthetic data.
- HTTP route mounting in `server`.
- Generated schema parity with upstream gqlgen API.
- Auth, database selection, errors, and subscription behavior.

### `mcp`

Status: protocol skeleton only.

What exists:

- JSON-RPC request/response types.
- Tool registry with `run_cypher` and `find_similar`.
- `initialize`, `tools/list`, `tools/call` dispatch shape.

Missing:

- Transport listener/WebSocket/stdio integration.
- Real tool execution. `tools/call` currently returns `stub response`.
- Engine/search/vector/auth/database wiring.

### `qdrantgrpc`

Status: stub.

What exists:

- Error enum and comment recommending `qdrant-client`.

Missing:

- Actual dependency, client, connection management, collection/index operations, vector upsert/query/delete, error mapping, and server wiring.

### `nornicgrpc`

Status: data shapes only.

What exists:

- Config and response/request structs.
- A `GrpcServer` handle with accessors.

Missing:

- Protobuf definitions/build script.
- tonic service implementation.
- Listener startup.
- Engine/auth/database/streaming wiring.

### `inference` and `localllm`

Status: stubs.

What exists:

- Inference config and simple normalization/text echo behavior.
- GGUF config and missing-model validation.

Missing:

- Real model loading.
- llama.cpp FFI.
- ONNX/OpenAI/custom backend implementations.
- Embedding/textchunk/search/vector integration.

### `search`, `vectorspace`, `indexing`, `linkpredict`

Status: local algorithms, not threaded into full storage/query lifecycle.

What exists:

- In-memory inverted index in `search`.
- Vector utilities/registry surface.
- Basic index catalog interactions.
- Link prediction algorithm placeholder/small surface.

Missing:

- Tantivy-backed production full-text index despite dependency declaration.
- Distributed search mesh metadata and routing.
- Storage event driven index maintenance.
- Query planner/evaluator usage of indexes.
- Qdrant/offloaded vector integration.
- Fabric/replication integration for search topology.

### `fabric` and `replication`

Status: standalone primitives.

What exists:

- Fabric router can choose primary/readable nodes from an in-memory list.
- Replication has command/log/snapshot structs, memory storage, storage adapter, and quorum-ish primitives.

Missing:

- Runtime startup in binary.
- Cluster membership persistence.
- Raft-grade election/log replication parity.
- Integration with storage WAL/MVCC lifecycle.
- Query/write path routing through replication.
- Fabric routing in HTTP/Bolt/engine execution.

### `multidb`

Status: metadata manager only.

What exists:

- In-memory database registry with create/get/drop/list.
- System/default database entries.

Missing:

- Namespace-aware storage APIs.
- Composite engine routing.
- Remote engine adapters.
- Persistent database catalog.
- Integration with fabric/replication and auth scopes.

### `auth`, `security`, `audit`, `compliance`, `kms`, `encryption`

Status: partial subsystem crates.

What exists:

- JWT/token basics and dev auth path.
- Encryption/KMS wrappers at a small surface.
- Audit/compliance/security data structures and helper functions.

Missing:

- Full upstream OAuth/provider/RBAC/session behavior.
- Enforcement across HTTP, Bolt, GraphQL, MCP, gRPC, and engine execution.
- Audit event persistence and policy integration.
- KMS providers beyond the currently sketched implementations.

### `retention`

Status: CRUD surface, not enforcement.

What exists:

- Retention policies, defaults, legal holds, erasure requests.
- HTTP admin endpoints wired to an in-memory manager.

Missing:

- Storage-backed persistence.
- Sweep implementation that actually scans and deletes graph data.
- Legal hold enforcement across all deletion paths.
- Audit/compliance integration.

### `gpu`, `simd`, `math`

Status: utilities only.

What exists:

- SIMD/math helpers.
- GPU backend enum/config and CPU fallback-style pieces.

Missing:

- Actual GPU compute backend parity with upstream CUDA/Metal/Vulkan/OpenCL surfaces.
- Integration into embedding/vector/query/search hot paths.

## Threading Gaps By Workflow

### Startup and shutdown

NornicDB uses package-level lifecycle semantics for components. copperDB currently starts HTTP and Bolt tasks directly and joins them. There is no unified component graph, signal-aware supervisor, reverse shutdown ordering, or fresh shutdown timeout context.

Needed:

- Add `crates/lifecycle` equivalent.
- Model HTTP, Bolt, GraphQL, MCP, gRPC, replication, telemetry exporter, search workers, retention sweeper, and background storage tasks as components.
- Use forward startup order and reverse drain order.
- Surface joined errors.

### Query execution over HTTP

HTTP Cypher currently opens an engine and executes simple queries. It does not consistently flow through auth scopes, audit, compliance, fabric, replication, retention, search indexes, vector/embedding hooks, knowledge policy scoring, or full telemetry.

Needed:

- A request execution pipeline that accepts database/session/auth context.
- Centralized error mapping, especially transient transaction errors.
- Query planner/evaluator hooks for index/search/vector/knowledge policy.
- Write routing through replication when enabled.

### Query execution over Bolt

Bolt does not yet execute queries. The listener accepts TCP connections and reads bytes, but ignores message semantics.

Needed:

- Implement PackStream framing/decoding/encoding.
- Implement Bolt state machine and authentication.
- Connect RUN/PULL and transactions to `engine`, `txsession`, `auth`, `errors`, and `otel`.

### Multi-database and fabric routing

`multidb` and `fabric` exist but are not a coherent query routing layer.

Needed:

- Persist database catalog.
- Bind logical database to storage namespace/path.
- Route local vs remote execution through fabric.
- Apply auth and replication per database.

### Observability

`otel` validates catalog names but is not an OpenTelemetry/OpenMetrics runtime.

Needed:

- Replace mock catalog seeding with live metrics producers.
- Add exporter/listener/provider/registry surfaces.
- Add redaction and mandatory label enforcement for all emitted metrics/logs/spans.

## Recommended Implementation Order

1. Core naming and crate topology: complete the `engine` rename, ensure all manifests and imports use `copperdb-engine`, and keep `crates/copperdb` as the binary crate.
2. Add missing `errors` crate: port transient transaction error mapping and wire it into HTTP/Bolt/tx/session errors.
3. Add missing `lifecycle` crate: component trait, supervisor, signal handling, reverse shutdown, and 30s fresh shutdown context.
4. Bolt parity: complete PackStream framing and message dispatch to auth, engine, txsession, and error mapping.
5. Storage parity: expand MVCC reader registry/pruning, WAL snapshot/compaction, async write-behind, index maintenance, namespace APIs, and storage event notifier.
6. Knowledge policy parity: add `knowledgepolicy` crate and wire parsed decay/promotion schemas to storage/eval/otel.
7. Query pipeline threading: centralize auth, audit, compliance, fabric, replication, retention, telemetry, and engine execution.
8. Observability runtime: turn `otel` from catalog/mock telemetry into provider/exporter/listener/logging/redaction runtime.
9. API surfaces: mount and wire GraphQL, MCP, gRPC, and Qdrant client behavior.
10. Search/vector/index integration: connect storage events and query planner paths to search/index/vector crates.

## Verification Performed

- Compared local package inventories under `~/src/NornicDB/pkg` and `~/src/copperDB/crates`.
- Collected rough per-package file/function/LOC markers to identify skeletons vs substantial ports.
- Searched Rust source for TODO/stub/unimplemented markers.
- Audited internal crate dependencies and runtime startup wiring.
- Renamed the old magnet-era engine crate path/package/API names.
- Ran targeted validation: `cargo check -p copperdb-engine -p copperdb-server -p copperdb`.

Validation result:

- Targeted check passed.
- Cargo reported two existing warnings in `crates/server/src/lib.rs`: an unnecessary `mut` on the router and an unused `state` parameter in `host_for_request`.
- Full `cargo check --workspace` was started earlier but cancelled by the user before completion, so full-workspace validation is not claimed here.
