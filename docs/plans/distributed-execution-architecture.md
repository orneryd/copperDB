# Distributed Execution Architecture

Date: 2026-05-26

This document is the implementation contract for copperDB distributed writes, reads, and search. The target is Cassandra-like distributed coordination rather than a single Raft leader path. Any Raft-style machinery that remains in the Rust workspace is transitional until it is either adapted behind this contract or removed.

This document defines one replicated placement at a time. The higher-level federated multi-shard AI fabric plan is defined in [federated-ai-fabric-architecture.md](federated-ai-fabric-architecture.md).

## Goals

- Any healthy coordinator-capable node can accept a client write, read, or search request.
- Placement is resolved through `copperdb-topology` using `PlacementKey { tenant, database, shard }`.
- Writes are sent to all healthy storage replicas for the placement and succeed once the requested consistency level is met.
- Reads are sent to enough healthy storage replicas to satisfy the requested consistency level and leave room for read repair.
- Search is planned separately from graph storage reads: search fan-out targets search-capable nodes, merges shard results, and uses hedged requests for p99 control.
- Distributed transaction IDs use topology logical clocks, not wall-clock time, for ordering across cores and peers.
- High availability and hyperscaler/distributed transport seams are foundational, but external multi-region transports can remain pluggable while local/in-memory contract tests prove the behavior.

## Core Vocabulary

- `TopologyRegistry`: validated registry of peers, placements, health, search policy inputs, and write/read plan inputs.
- `MeshPeer`: node identity, address, region/zone, capabilities, health, observed latency, load, and capacity.
- `PlacementRecord`: tenant/database/shard ownership, replica nodes, search nodes, and minimum durability requirements.
- `ConsistencyLevel`: `One`, `Quorum`, `All`, and `LocalQuorum`.
- `DistributedWriteMode::DynamoQuorum`: coordinator-based multi-replica writes with Cassandra/Dynamo-style acknowledgement rules.
- `DistributedWritePlan`: coordinator, target replicas, consistency level, and required acknowledgements.
- `DistributedReadPlan`: coordinator, target replicas, consistency level, and required responses.
- `DistributedSearchPlan`: latency-ranked search fan-out, bounded parallelism, and hedge deadline.

## Distributed Write Flow

```mermaid
sequenceDiagram
    participant Client
    participant Coordinator
    participant Topology
    participant Clock
    participant ReplicaA
    participant ReplicaB
    participant ReplicaC

    Client->>Coordinator: write(placement, mutation, consistency)
    Coordinator->>Topology: plan_write_with_consistency(placement, DynamoQuorum, consistency)
    Topology-->>Coordinator: coordinator, replicas, required_acks
    Coordinator->>Clock: issue logical transaction id
    par fan out
        Coordinator->>ReplicaA: apply mutation(tx_id)
        Coordinator->>ReplicaB: apply mutation(tx_id)
        Coordinator->>ReplicaC: apply mutation(tx_id)
    end
    ReplicaA-->>Coordinator: ack
    ReplicaB-->>Coordinator: ack
    Coordinator-->>Client: success after required_acks
    ReplicaC-->>Coordinator: late ack or hinted handoff candidate
```

Write execution rules:

- The coordinator is selected by topology from healthy coordinator-capable placement participants, preferring request locality and lower observed RTT.
- The coordinator does not need to be the primary node.
- The coordinator fans the mutation to every healthy storage/write participant in the placement.
- `One` requires one successful replica acknowledgement.
- `Quorum` requires `floor(replica_count / 2) + 1` acknowledgements.
- `All` requires every healthy planned replica acknowledgement.
- `LocalQuorum` requires `floor(local_replica_count / 2) + 1` acknowledgements in the request region.
- A write that cannot satisfy the required acknowledgement count fails with `NoQuorum` and must not be reported as committed.
- Late replica responses may still be accepted as durability progress, but they cannot change a client-visible failure into success after the coordinator has returned.
- Future remote transports should preserve the same plan and acknowledgement contract; only the transport implementation should change.

## Distributed Read Flow

```mermaid
sequenceDiagram
    participant Client
    participant Coordinator
    participant Topology
    participant ReplicaA
    participant ReplicaB
    participant ReplicaC
    participant Repair

    Client->>Coordinator: read(placement, key, consistency)
    Coordinator->>Topology: plan_read(placement, consistency, region)
    Topology-->>Coordinator: coordinator, replicas, required_responses
    par fan out
        Coordinator->>ReplicaA: read key
        Coordinator->>ReplicaB: read key
        Coordinator->>ReplicaC: optional hedge/read repair probe
    end
    ReplicaA-->>Coordinator: value @ tx_id
    ReplicaB-->>Coordinator: value @ tx_id
    Coordinator->>Coordinator: choose latest logical tx_id
    Coordinator-->>Client: value after required_responses
    Coordinator->>Repair: enqueue stale replica repair if versions differ
```

Read execution rules:

- Reads use storage-capable placement participants, sorted by locality, observed RTT, and node id for deterministic planning.
- The coordinator waits for enough successful responses to meet the requested consistency level.
- When multiple values are returned, the value with the highest topology logical transaction ID wins.
- Missing, stale, or older-version responses should enqueue read repair once versioned storage values are wired through the engine.
- A read can return `None` only after the requested consistency level has responded and no newer value is found.
- Degraded peers may serve reads when topology policy allows it; unreachable peers do not count toward consistency.

## Distributed Search Flow

```mermaid
sequenceDiagram
    participant Client
    participant Coordinator
    participant Topology
    participant SearchA
    participant SearchB
    participant SearchC
    participant Merger

    Client->>Coordinator: search(placement, query, policy)
    Coordinator->>Topology: plan_search_with_policy(placement, policy)
    Topology-->>Coordinator: fanout, parallelism, hedge_after
    par bounded fan-out
        Coordinator->>SearchA: search shard/query
        Coordinator->>SearchB: search shard/query
    end
    Coordinator->>SearchC: hedged request if hedge_after expires
    SearchA-->>Merger: scored hits
    SearchB-->>Merger: scored hits
    SearchC-->>Merger: late or hedge hits
    Merger-->>Coordinator: merged ranked page
    Coordinator-->>Client: ranked results
```

Search execution rules:

- Search planning is independent from write/read replica planning because search nodes may be specialized index nodes.
- Search fan-out is bounded by placement `search_fanout` and request policy `max_fanout`.
- Topology ranks candidates by observed RTT plus cross-region penalty plus load divided by capacity weight.
- Same-region search nodes are preferred when `request_region` is known and policy requests locality.
- Hedged requests are issued after the plan hedge deadline to reduce p99 latency.
- Result merging must be deterministic for equal scores, using stable document ids as tie breakers.
- Vector offload through `qdrantgrpc` and internal remote execution through `nornicgrpc` must consume the same `DistributedSearchPlan` rather than inventing separate routing rules.

## Package Responsibilities

- `topology`: owns all placement, peer, consistency, read/write/search planning, and logical transaction ordering vocabulary.
- `storage`: persists topology metadata and graph/index records with version information needed for read repair and last-write-wins conflict resolution.
- `replication`: owns coordinator write/read execution, replica transport abstraction, acknowledgement counting, quorum failure behavior, failed-replica outputs, and durable hinted handoff/read-repair queue records.
- `search`: owns local index execution, distributed search routing over `DistributedSearchPlan`, fan-out transport seams, failure tracking, and deterministic result merging.
- `fabric`: exposes placement-aware routing and control-plane-friendly topology views without implementing storage semantics.
- `qdrantgrpc`: executes vector-search subplans against external vector stores when a search plan targets remote vector infrastructure; request envelopes must be derived from `DistributedSearchPlan`.
- `nornicgrpc`: executes internal remote read/write/search RPCs while preserving topology plans and consistency contracts; request envelopes must be derived from `DistributedWritePlan`, `DistributedReadPlan`, or `DistributedSearchPlan`.
- `engine`: loads durable topology from storage, exposes distributed read/write planning, builds Cassandra coordinators with durable repair queues, replays repair batches through replica transports, builds scheduled repair workers, and routes Cypher through an explicit distributed execution API; protocol crates must not reimplement distributed semantics.
- `server`: selects the distributed Cypher path for HTTP and Neo4j transaction requests when `COPPERDB_DISTRIBUTED_CYPHER` or `x-copperdb-distributed` is enabled, then delegates to the engine API with topology-derived placement and consistency. The server-owned write path now builds a real outbound tonic replica transport from topology peers and generates a short-lived admin cluster JWT when security is enabled. The read path now builds the real graph-read tonic transport, forwarding the original caller bearer token so the receiving node reapplies the existing per-database read gate while clustered access-metadata side effects continue through the internal admin-authenticated replica channel rather than fabricating in-memory replicas.
- `nornicgrpc`: provides generated tonic/prost replica service bindings, remote execution envelopes, a generated-client adapter, a generated-server adapter, and a replica transport adapter that turns replication writes/reads into target-addressed remote client requests.
- `qdrantgrpc`: turns distributed search plans into vector-search requests and provides a production Qdrant HTTP search client plus a distributed executor that fans out to Qdrant targets, records target failures, and merges hits deterministically.

## Failure And Recovery Semantics

- Coordinator failure before client response is retried by the client or upper engine layer using idempotent mutation identity.
- Replica failure before required acknowledgements returns `NoQuorum`.
- Replica failure after required acknowledgements becomes asynchronous repair/hinted handoff work persisted in the replication repair queue.
- Peer health changes must flow through topology and alter future plans without changing the consistency contract.
- Cross-region failures should prefer local quorum when requested and avoid global stalls for locality-scoped operations.
- Transaction ordering must compare topology logical transaction IDs: `(epoch, counter, node_ordinal)`.

## Implementation Order

1. Finish topology consistency-level read/write plans and tests.
2. Finish replication coordinator writes and reads using an in-memory replica transport, storage-backed adapter tests, and durable post-quorum repair records.
3. Update `fabric` to expose read/write/search planning for the same Cassandra-like contract.
4. Update `search` docs/tests around distributed fan-out and deterministic merge contracts.
5. Wire engine request paths to the coordinator/fabric surfaces while keeping protocol adapters thin. The engine now exposes topology-backed planning, coordinator construction, repair replay, scheduled repair workers, and explicit distributed Cypher execution; HTTP and Neo4j transaction handlers can opt into that mode through server configuration or request headers. The server layer now has a real outbound replica transport for write-side routed execution and a real graph-read gRPC transport for read-side routed execution, with caller-auth forwarding on remote graph reads and internal admin-authenticated replica writes for distributed read side effects.
6. Add `nornicgrpc` and `qdrantgrpc` transports as execution backends for the same plans.

Current status: complete for the Layer 3 foundation. Remaining future work belongs to protocol hardening and deeper engine integration, not to the foundational distributed execution contracts.

## Completion Bar

Layer 3 packages can only be checked when:

- The package follows this document as the single distributed execution path.
- Package-owned state is durable or explicitly runtime-only.
- Immediate consumers compile against the package contract.
- Focused tests prove success, quorum failure, topology health behavior, and restart/persistence behavior where applicable.