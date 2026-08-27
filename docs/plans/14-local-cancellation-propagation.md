# 14: Local Request Cancellation Propagation

Status: in progress. Priority: P1. Owners: `util`, `server`, `bolt`, `engine`, `eval`, `storage`, `indexing`, `search`, `embed`, `nornicgrpc`.

## Objective

Root cancellation and deadlines at every supported ingress, preserve a typed cancellation error through the stack, and stop expensive local work at bounded checkpoints.

## Existing Foundation

`RequestContext`/cancellation handles, Bolt rooting, selected eval traversal checks, and cancellable storage/index APIs already exist. Missing pieces include HTTP-wide rooting, context-free internal paths, search/embedding/materialization loops, and end-to-end error preservation.

## Phases

1. Add Axum middleware creating request ID, root cancellation token, narrowed deadline, trace/auth context, and drop guard in request extensions.
2. Make context-aware engine APIs canonical; retain detached wrappers only for explicit embedded/test calls.
3. Add checks before blocking calls and every bounded work budget in scans, traversal, projection, BM25/vector scoring, hydration, index rebuild, and embedding workers.
4. Preserve `RequestCancelled` without string conversion and map it to HTTP, Bolt, and tonic cancellation semantics.
5. Instrument cancellation by protocol, stage, and reason with bounded labels.

## Semantics

Reads/searches drop partial assembly. Uncommitted transactions request rollback. Cancellation racing with commit reports committed, aborted, or unknown according to the transaction decision; it never invents rollback. Blocking model calls are bounded but not forcibly aborted.

## Tests

Cancel before work, mid-loop, during rebuild/hydration, on disconnect/deadline, and before/during/after commit. Assert bounded latency, cleanup, no partial result, stable IDs, and correct error class.

## Progress

- Complete: Axum middleware creates one root request context for every application HTTP request, retains its cancellation guard through handler execution, and exposes the context through request extensions. Search, transactional Cypher, and Fabric ranked-search paths consume that ingress context instead of creating independent roots. Deterministic middleware tests verify the context is active in the handler and cancelled when the request future completes or is dropped.
- Complete: HTTP ingress contexts carry NornicDB-compatible route deadlines for status, search, and transaction requests. Timeout expiration returns the upstream `503` message, cancels the shared context, and honors positive `COPPERDB_HTTP_TX_TIMEOUT` or upstream-compatible `NORNICDB_HTTP_TX_TIMEOUT` duration overrides with a five-minute transaction default.
- Pending: complete context-aware internal APIs, bounded cooperative checkpoints, typed protocol error mapping, and cancellation telemetry.

## Definition Of Done

All supported ingress creates a root context, expensive local loops cooperate within a documented bound, typed cancellation survives to clients, and write outcomes follow transaction state.