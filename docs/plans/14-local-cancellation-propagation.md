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
- Complete: MCP HTTP dispatch passes the ingress request context through both built-in tools. Cypher uses the canonical context-aware engine execution path, full-text search delegates to the storage cancellation-aware primitive, and pre-cancelled requests preserve `CopperDbError::RequestCancelled` before local work begins. Detached MCP dispatch remains available only as an explicit embedded/test wrapper.
- Complete: GraphQL attaches the HTTP request context to async-graphql execution and requires it in every resolver. Point reads and mutations check before storage work, node listing uses cancellable streaming, storage cancellation remains typed, and cancelled mutations are rejected before writing. Detached schema execution remains an explicit embedded/test wrapper.
- Complete: distributed ranked-search and hydration collectors recheck cancellation after every awaited transport response, discard response assembly when cancellation wins the race, and check hydration materialization every 256 items. Cancel-on-return transport regressions verify no partial collection escapes as success.
- Complete: storage cancellation remains typed when converted through eval and engine errors, including direct engine storage adapters. Focused regressions also verify ordinary storage and eval failures retain their existing generic classifications.
- Complete: remote ranked-search and hydration transports preserve tonic `Cancelled` as typed search cancellation instead of flattening it into a transport string. Adapter regressions verify both cancellation paths and unchanged classification of ordinary gRPC transport failures.
- Complete: distributed ranked-search and hydration collectors propagate typed remote cancellation immediately instead of classifying the cancelled child as a failed node. Mixed-success regressions verify earlier shard results are discarded when a later remote child reports cancellation.
- Complete: HTTP transaction execution preserves typed engine cancellation through the server helper and maps it to the same upstream-compatible `503` transaction-timeout response used by route deadlines. Bolt adapters retain their existing string contract at the explicit protocol boundary.
- Pending: complete context-aware internal APIs, bounded cooperative checkpoints, typed protocol error mapping, and cancellation telemetry.

## Definition Of Done

All supported ingress creates a root context, expensive local loops cooperate within a documented bound, typed cancellation survives to clients, and write outcomes follow transaction state.