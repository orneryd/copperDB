# Request Cancellation Propagation

> Detailed cancellation design reference. Current implementation status and sequencing are defined in [../../COPPERDB_NORNICDB_PARITY_PLAN.md](../../COPPERDB_NORNICDB_PARITY_PLAN.md).

Date: 2026-05-29

Status: local cancellation is in scope; distributed propagation is deferred.

copperDB's current supported architecture is single-node execution. This document defines the planned Rust pattern for request-scoped cancellation across copperDB ingress and local execution today, and records a future extension for distributed mesh fan-out if and when distributed work resumes. It is architecture guidance only; no implementation is implied by this document.

## Problem

Any user-visible request can disappear before work finishes: an HTTP client can disconnect, a Bolt transaction can time out, a gRPC caller can cancel, or an internal distributed subrequest can lose its parent. copperDB must not keep burning CPU on expensive loops, scans, graph traversals, search fan-out, hydration, repair-adjacent follow-up, or remote shard work after nobody is waiting for the result.

NornicDB models this with Go `context.Context`. Rust has no single standard-library equivalent, so copperDB should use explicit cooperative cancellation:

- async request code uses a request-scoped cancellation token;
- synchronous or CPU-heavy loops receive a cheap cancellation handle and check it periodically;
- deadlines and timeouts are part of the same request context;
- distributed transports propagate both passive deadlines and active cancellation identity across the mesh.

## Core Pattern

Introduce one request context vocabulary that can cross crate boundaries without each layer inventing its own cancellation API.

Conceptual shape:

```rust
pub struct RequestContext {
    pub request_id: RequestId,
    pub parent_request_id: Option<RequestId>,
    pub deadline: Option<std::time::Instant>,
    pub cancel: RequestCancellation,
    pub trace: TraceContext,
    pub auth: CallerAuthContext,
}
```

The concrete type can live in a small shared crate once implemented. The important contract is the behavior:

- `cancel()` marks the local request tree as cancelled.
- `is_cancelled()` is cheap enough for hot loops.
- `check_cancelled()` returns a normal request-cancelled error, not an internal failure.
- `check_deadline()` returns the same cancellation class when the deadline has passed.
- child contexts inherit parent cancellation and may narrow, but never extend, the parent deadline.
- dropping or timing out an ingress request cancels the root context and all local children.

Recommended implementation building blocks:

- use `tokio_util::sync::CancellationToken` for async tasks and child-token propagation;
- expose a sync-friendly wrapper for storage/eval/search loops so they do not need to be async;
- use `Arc<AtomicBool>` or the token's nonblocking check path for CPU-bound loops;
- use `tokio::time::timeout` only as the outer deadline trigger, not as the only cancellation mechanism;
- use task abort only as a cleanup aid for spawned futures, not as the primary model for stopping deep work.

## Ingress Ownership

Every ingress creates a root request context before it calls engine code:

- HTTP handlers cancel the root context when the response future is dropped, the route times out, or the server deadline fires.
- Bolt handlers cancel the root context when the client connection closes, the transaction timeout fires, or the statement is explicitly interrupted.
- gRPC handlers derive the root context from the inbound gRPC deadline/cancellation signal and cancel it when tonic observes request cancellation.
- internal background jobs that expose admin-triggered work also create a root context with an explicit deadline or shutdown token.

Protocol crates stay thin: they own ingress cancellation detection, then pass the request context to `engine`. They must not implement storage, replication, search, or fabric-specific cancellation policy themselves.

## Local Cooperative Checkpoints

Long-running local work must receive a request context or cancellation handle and check it at bounded intervals. Required checkpoint locations include:

- storage tree scans, prefix scans, streaming callbacks, MVCC prune/rebuild loops, and index rebuild loops;
- eval graph expansion, path search, pattern matching, projection/materialization loops, and batch `UNWIND` work;
- search ranking, candidate collection, BM25/vector loops, rerank loops, hydration, and policy filtering;
- replication quorum fan-out, response collection, read repair candidate loops, and hinted handoff replay where request-scoped;
- fabric scatter/gather row merging, path-set merging, shortest-path traversal, and shard hydration;
- outbound transport retry, hedging, and response merge loops.

Hot loops should not perform expensive checks on every tiny operation. A normal pattern is to check every fixed budget, such as every 64, 128, or 256 items, and also before each blocking or remote call. Anything that may block on I/O should race the work against cancellation in async code.

## Mesh Propagation

This section is deferred future work. The current product/runtime guarantee stops at single-node ingress and local execution.

Distributed work requires both passive and active propagation.

Passive propagation is mandatory for every remote envelope:

- `request_id`: stable id for the root request or distributed operation;
- `parent_request_id`: optional parent span/request id for fan-out lineage;
- `deadline`: absolute deadline or remaining timeout, never later than the parent;
- `trace_context`: existing tracing/span correlation;
- caller auth/compliance context;
- read fence/bookmark context where relevant.

Active cancellation is also required for efficient CPU cleanup across the mesh:

- each node keeps a bounded in-flight request registry keyed by `request_id` or remote child id;
- when a coordinator cancels a parent request, it sends best-effort cancel messages to all outstanding remote children;
- remote nodes cancel the registered child token and let local cooperative checkpoints unwind work;
- cancellation messages are idempotent and may arrive before, during, or after the normal response;
- registries expire entries by deadline so lost cancel messages cannot leak state.

The best approach is to use both mechanisms. Deadlines guarantee eventual stop even if a cancel message is lost. Active cancel reduces wasted CPU immediately when HTTP/Bolt/gRPC ingress disconnects, when a timeout fires, when a quorum has already been satisfied, or when a hedge loses the race.

## Transport Rules

Only the local ingress parts are current-scope guidance. The copperDB-to-copperDB transport material below is deferred until distributed execution is intentionally resumed.

For copperDB-to-copperDB gRPC:

- carry gRPC deadline metadata where available;
- carry copperDB request/cancellation ids in typed protobuf fields or stable metadata;
- add a small internal cancel RPC or control message that targets in-flight request ids;
- server-side handlers register the child context before starting work and unregister it on completion;
- client-side fan-out cancels children when the parent is cancelled, after quorum success when extra responses are no longer needed, and after a hedged request loses.

For HTTP and Bolt ingress:

- translate disconnect, timeout, and explicit interrupt into root context cancellation;
- do not rely on the protocol connection state being visible in deeper layers;
- pass the context through engine APIs and into every distributed fan-out path.

For external systems such as Qdrant:

- pass timeout/deadline when the remote API supports it;
- cancel/drop the local client future on parent cancellation;
- record that active remote compute cancellation may be best-effort if the external API lacks a cancellation endpoint.

## Semantics

Cancellation means "stop doing work for this request as soon as the current execution state allows." It is not a single outcome rule. The effect depends on whether the cancelled operation is a read/search, an uncommitted transaction, an in-flight distributed commit, or already past its commit point.

Read/search/traversal semantics:

- Cancelled read/search/traversal work should stop local loops, cancel remote children, drop partial result assembly, and return a request-cancelled class of error.
- Hedged losers and surplus fan-out after enough successful results have been collected should be cancelled immediately.
- Late remote read/search responses after cancellation are ignored except for metrics/tracing cleanup.

Transactional write semantics:

- If cancellation arrives before the transaction has reached a durable commit decision, copperDB should abort the transaction when the transaction protocol supports abort. This includes rolling back local pending writes, releasing locks/leases/intents, and sending best-effort abort/cancel messages to remote participants.
- If cancellation arrives while a distributed commit decision is in progress, the coordinator must resolve the transaction outcome before reporting a final state to any surviving caller/session owner. The outcome may be committed, aborted, or ambiguous if the coordinator cannot determine the commit decision.
- If a write has already reached the commit/quorum boundary, cancellation must not claim rollback. Post-commit cancellation can still stop surplus fan-out, hinted-handoff-adjacent work, repair-adjacent work, hydration, response serialization, and client-result delivery.
- If the client disconnects before learning the outcome, the system still has to finish or recover the commit/abort decision according to the transaction protocol. The client-visible state may be "unknown" even when the cluster later resolves the transaction.
- Late remote write responses after local cancellation are ignored for client response purposes, but may still be consumed for safe durability progress, commit-decision recovery, hints, repair bookkeeping, or participant cleanup allowed by the distributed execution contract.

Non-transactional Dynamo/quorum write semantics:

- For single-placement Dynamo/quorum writes without a prepare/commit transaction record, cancellation before the acknowledgement threshold should stop remaining local work and cancel outstanding replica sends when possible.
- Once the requested consistency level has acknowledged success, cancellation cannot undo the write. Any missing replicas converge through the normal hinted handoff/read repair/repair path.
- If cancellation races with acknowledgements and the coordinator cannot prove whether the acknowledgement threshold was reached, the client-facing result should be classified as cancelled/unknown rather than reported as a clean rollback.

Error classification:

- `RequestCancelled`: work stopped before a known commit decision or for read/search work with no durable mutation outcome.
- `TransactionAborted`: an explicit transaction abort decision was reached and any pending writes were discarded.
- `CommitUnknown` or `OutcomeUnknown`: the coordinator cannot determine whether a commit decision was durably reached.
- `CommittedButClientGone`: optional observability classification for work that committed after the caller disconnected or stopped waiting.
- `NoQuorum` remains distinct from cancellation: it means the required acknowledgement count was not met.

## Existing Database Patterns

The industry pattern is not "cancel equals rollback" or "cancel never rolls back." Systems split cancellation by execution state:

- PostgreSQL query cancellation is best-effort. If effective, the current command terminates early with an error. If cancellation arrives after the backend has finished, it may have no effect. Disconnect rolls back an open transaction, but a non-transactional write may finish and commit before the server notices the disconnect.
- CockroachDB treats explicit `ROLLBACK` as transaction abort, has retryable/aborted transaction states before commit, and has an ambiguous-error class when the client cannot assume commit or failure. That is the important distributed lesson: once commit uncertainty exists, the client must not infer rollback from timeout or cancellation.
- Cassandra/Dynamo-style writes are acknowledgement-threshold based. Writes are sent to all replicas, the consistency level controls how many replies the coordinator waits for, and timed-out or unavailable replicas may be repaired later by hints/repair. Once the chosen consistency level is acknowledged, cancellation cannot retract the write; before that point, cancellation can stop waiting and cancel remaining work, but may leave an unknown race if acknowledgements are in flight.
- MongoDB has explicit distributed transaction abort/commit semantics. Transactions either commit and become visible or abort and discard changes. Separately, operations can be killed across shards; for writes in sessions, cancellation targets the session/operation. Commit result uncertainty is handled as a separate condition from ordinary operation cancellation.

For copperDB, the target model is therefore:

- reads/search/traversals: cancel aggressively and return request-cancelled;
- uncommitted transactions: cancel should request abort and cleanup;
- commit in progress: resolve committed/aborted/unknown, never guess;
- post-commit/quorum success: stop extra work, but do not claim rollback;
- Dynamo/quorum writes without a transaction record: cancellation can stop surplus work but cannot reliably undo writes that may already be durably acknowledged.

## Implementation Order

1. Define the shared request context and request-cancelled error class.
2. Wire root contexts at HTTP, Bolt, and gRPC ingress without changing business logic.
3. Thread context through `engine` APIs into local eval, search, fabric, replication, and storage calls.
4. Add cooperative checkpoints to expensive local loops, beginning with streaming, scans, traversal, search merge/hydration, and MVCC/index maintenance.
5. Add request ids, deadlines, and parent lineage to internal gRPC/fabric/search/replication envelopes.
6. Add a remote cancellation registry and best-effort internal cancel RPC/control path.
7. Cancel hedged losers and post-quorum surplus work as soon as the coordinator has enough successful results.
8. Add deterministic tests for ingress timeout, local loop cancellation, remote fan-out cancellation, hedged loser cancellation, and lost-cancel deadline expiry.

## Non-Goals

- Do not treat cancellation as a substitute for a transaction protocol. Cancellation may request abort before commit, but only the transaction/replication protocol decides whether the final outcome is aborted, committed, or unknown.
- Do not make protocol crates own distributed cancellation policy beyond detecting ingress cancellation and forwarding the context.
- Do not depend only on dropping futures or aborting tasks; CPU loops must cooperate explicitly.
- Do not let child requests extend parent deadlines.