# NornicDB Architecture Dependency Graph

Date: 2026-05-26

This graph is the package-porting order for copperDB. It follows NornicDB's package architecture while keeping Rust dependency cycles out of the workspace. Each layer may depend on earlier layers; later layers should not be required by earlier layers.

A checked package means the Rust package has a single-path implementation, is threaded into its immediate consumers, and has focused contract tests. Unchecked packages may exist as scaffolds or partial ports but are not complete enough to call done.

## Layer 0: Shared Contracts And Process Foundation

These packages define vocabulary, errors, startup/shutdown behavior, and observability contracts. They must be implemented first because every other layer consumes them.

- [ ] `util` -> shared helpers.
- [ ] `buildinfo` -> build/version metadata.
- [ ] `envutil` -> environment parsing helpers.
- [ ] `config` -> config files, env, CLI/default precedence.
- [x] `errors` -> Neo4j-compatible transient error codes, retry classification, conflict sentinels.
- [ ] `otel` (`observability` in NornicDB) -> metrics catalog, providers, exporters, redaction, logging, recovery.
- [x] `lifecycle` -> component supervisor, first-error cancellation, reverse-order shutdown, fresh shutdown budget.
- [x] `topology` -> hyperscaler placement, latency-aware distributed search fan-out, and high-availability write planning contracts.

Current Rust status: `errors`, `lifecycle`, and `topology` have focused implementations and tests. `topology` is the single contract path for enterprise placement, search routing, and HA write planning.

## Layer 1: Security, Identity, And Compliance

These packages depend on layer 0 and gate every externally reachable surface.

- [ ] `encryption` -> envelope encryption primitives.
- [ ] `kms` -> provider-backed key wrapping.
- [ ] `auth` -> JWT/OAuth/RBAC/session identity.
- [ ] `security` -> hardening and auth-adjacent enforcement.
- [ ] `audit` -> durable security and data-access event trail.
- [ ] `compliance` -> policy enforcement and regulated-data controls.

Required direction: API surfaces call into this layer; this layer must not depend on HTTP/Bolt/GraphQL/MCP implementations.

## Layer 2: Storage, Transactions, And Metadata

These packages own durable graph state and storage-adjacent state.

- [ ] `storage` -> graph records, metadata catalogs, MVCC, WAL, schema, indexes, namespace primitives.
- [ ] `cache` -> query/result/write-through caches.
- [ ] `pool` -> connection/resource pooling.
- [ ] `txsession` -> transaction/session lifecycle and conflict semantics.
- [ ] `retention` -> retention policies, legal holds, erasure request model and sweeper hooks.
- [ ] `multidb` -> logical database catalog and namespace routing.

Distributed foundation hooks in this layer:

- storage metadata persists topology-native hyperscaler profiles, mesh peers, placements, search policies, and HA write policy inputs.
- storage rebuilds a validated `TopologyRegistry` from durable metadata.
- transaction errors should route through `errors`.

## Layer 3: Distributed Execution Foundation

These packages turn topology into cluster behavior. In this phase, seams must exist before full execution is enabled.

- [ ] `replication` -> HA write planning, log replication, leader election, snapshots, write quorum.
- [ ] `fabric` -> cluster routing, topology propagation, placement-aware execution.
- [ ] `search` -> distributed search mesh planning, shard fan-out, peer selection.
- [ ] `qdrantgrpc` -> external vector-store client and remote vector index offload.
- [ ] `nornicgrpc` -> gRPC transport for internal/remote execution.

Current Rust status:

- `replication` consumes `topology` through `HighAvailabilityWritePlanner`.
- `fabric` consumes `topology` through `FabricTopology`.
- `search` consumes `topology` through `DistributedSearchRouter`.
- `storage` persists and reloads the same topology contracts.
- Network execution, remote fan-out transport, and HA write enforcement remain open work before these packages can be checked.

## Enterprise Scaling Design

The hyperscaler, distributed search, and HA write layers are first-class architecture, not add-ons. The clean path is:

- `topology` owns all placement vocabulary: hyperscaler profile, mesh peer, health, capacity, observed RTT, placement, search routing policy, and write mode.
- `storage` persists topology-native records only and reconstructs a validated registry for boot and control-plane updates.
- `search` receives a `DistributedSearchPlan` from topology. The planner chooses candidates by same-region locality, observed latency, inflight load, capacity weight, health, fan-out cap, and hedge deadline.
- `fabric` routes by `PlacementKey`, never by protocol-specific IDs.
- `replication` receives a `DistributedWritePlan` from topology and enforces required acknowledgements for `LeaderLease`, `Quorum`, and `RaftLog` modes.

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

1. Finish layer 0 contracts: `errors`, `lifecycle`, and `otel` runtime, building on the new `topology` crate.
2. Expand layer 2 storage metadata for topology placements and HA write/search policies.
3. Keep layer 3 as planning seams first: distributed search fan-out and HA write plans must exist before network execution.
4. Port layer 4 package behavior one package at a time, starting with `cypher`, `filter`, `indexing`, `eval`, then `knowledgepolicy`.
5. Thread layer 5 engine through auth, audit, compliance, retention, replication/fabric, search/index, and telemetry.
6. Complete layer 6 protocol adapters after the central engine pipeline is stable.

## Foundational Distributed Contracts

The following contracts are intentionally first-class even while distributed execution remains disabled:

- Hyperscaler profiles: provider, region, zones, tier, metadata.
- Mesh peers: node id, address, region/zone, capabilities, health, heartbeat, observed RTT, inflight load, capacity weight.
- Placements: tenant/database/shard, primary, replicas, search nodes, hyperscaler profile.
- Distributed search plans: placement plus latency-ranked healthy search fan-out, bounded parallelism, and hedge deadline.
- Distributed write plans: placement plus mode, leader, replicas, required acknowledgements.

These contracts are implemented in `copperdb-topology`, persisted by `storage`, and consumed by `fabric`, `search`, and `replication`.
