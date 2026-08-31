# 14: Local Request Cancellation Propagation

Status: complete. Priority: P1. Owners: `util`, `server`, `bolt`, `engine`, `eval`, `storage`, `indexing`, `search`, `embed`, `nornicgrpc`.

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
- Complete: context-aware Bolt RUN execution preserves typed cancellation through its executor contract. Unsolicited cancellation maps to NornicDB's default `Neo.ClientError.Statement.SyntaxError` query failure with `request cancelled`, while ordinary execution strings retain `ExecutionFailed` and active explicit transactions are rolled back.
- Complete: query embedding uses the ingress request context and checks immediately before and after the blocking provider call. Pre-cancelled requests never invoke the provider, results produced after cancellation are discarded as typed `RequestCancelled`, and hybrid HTTP search does not swallow cancellation as a BM25 fallback.
- Complete: index definition persistence checks cancellation before schema mutation and publishes metadata only after rebuild completion and a final check. Relationship property and full-text rebuilds stream edges with a one-record cancellation bound instead of materializing the full relationship set, and relationship-property cleanup is cancellable and batched.
- Complete: indexing catalog cancellation remains typed through eval instead of being flattened into a generic execution error. Context-aware `CREATE INDEX` now returns `RequestCancelled` across the storage-indexing-eval boundary without publishing a definition or advancing schema generation, while ordinary index errors retain their existing classification.
- Complete: eval row projection checks the installed request context before every return item and wildcard field copy, bounding final result materialization while preserving context-free direct projection for explicit embedded callers.
- Complete: vector index finalization streams existing nodes and relationships with one-record cancellation bounds. Failure or cancellation removes the registry entry, options, and catalog definition so partially built indexes cannot become visible.
- Complete: eval pattern-comprehension assembly, UNWIND expansion and optimized UNWIND mutation batches, and buffered access-metadata flushing check the installed request context per item. A direct optimized-batch regression verifies typed cancellation before any graph mutation.
- Complete: Bolt races active RUN execution against TCP and WebSocket input. Disconnect, close, and upstream RESET signature `0x0F` cancel the shared context; RESET then recovers the session in protocol order. Positive client `tx_timeout` takes precedence over the optional CopperDB/NornicDB server fallback, whose default remains disabled.
- Complete: inbound tonic handlers own request guards through blocking execution and preserve typed engine cancellation as tonic `Cancelled`. HTTP, Bolt, gRPC, GraphQL, and MCP cancellation are counted by bounded protocol, ingress/execution stage, and explicit/deadline reason labels; the first observed cancellation cause wins.

## Definition Of Done

All supported ingress creates a root context, expensive local loops cooperate within a documented bound, typed cancellation survives to clients, and write outcomes follow transaction state.