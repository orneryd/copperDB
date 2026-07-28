# 08: Durable Transactions, MVCC History, And WAL

Status: in progress. Priority: P1. Owners: `storage`, `txsession`, `engine`, `eval`, `bolt`.

## Objective

Create one atomic, durable transaction boundary covering primary records, indexes, counters, embedding queues, schema effects, and persistent MVCC history.

## Current Evidence

`MvccStore` persists its logical heads, archived versions, and index candidates in metadata and restores them on open; a legacy database without that metadata receives a one-time current-state bootstrap. Persistent `StorageEngine` opens now own `copperdb.wal.rmp`, distinct from Fjall's internal WAL: each non-empty `BatchWriter` commit writes one checksummed, versioned transaction frame before its Fjall batch, and that batch atomically persists the matching applied sequence alongside primary records, indexes, counters, embedding metadata, and MVCC state. Ordinary structured node/edge single and bulk APIs now share this boundary rather than mutating Fjall and MVCC sequentially; live MVCC publication preserves active reader leases while startup/rebuild restoration clears them. On open, frames past the marker replay through the normal batch/index/MVCC path without producing a second frame, then atomically advance the marker; an interrupted `copperdb.wal.tmp` replacement is discarded while the prior authoritative WAL remains intact. An explicit repair surface can discard a corrupt WAL only when Fjall's applied marker covers every WAL sequence, and refuses any repair that could discard unapplied intent. `txsession::Transaction::commit_at` still changes state without applying typed graph operations.

## Contract Decisions

- Historical MVCC versions survive restart.
- Target isolation is snapshot isolation with read-your-writes and lost-update prevention; write skew is documented unless later strengthened.
- A commit receives one logical version and one configured durability outcome.
- Callbacks/events publish only after durable commit.
- Recovery is idempotent and never exposes a partial transaction.

## Architecture

Add `StorageTransaction` with namespace pin, begin snapshot, typed operations, read/write versions, and unique keys. Commit validates conflicts, acquires ordered key locks, allocates a version, constructs one fjall batch for all primary and derived state, appends a checksummed transaction WAL frame, applies atomically, persists applied sequence, then publishes events. Dedicated MVCC head/archive keyspaces are authoritative; memory is cache.

## Phases

1. Complete: replace fake batch atomicity with one Fjall database batch and one logical MVCC version. Existing closure-failure, mixed node/edge, index-removal, and shared-version regression coverage proves no staged mutation is published before the single commit. Storage I/O failure injection remains part of the later durable/WAL work.
2. In progress: persist MVCC heads/archives and prove visible-at reads across reopen. Transactional and ordinary structured node/edge primary records, indexes, counters, embedding metadata, and the prepared MVCC state now stage in one Fjall batch; the live MVCC cache publishes only after that batch succeeds while retaining active reader leases, and restart regressions prove mixed node/edge transactions retain one visibility boundary. The former MVCC-bypassing bulk edge loader has been removed; its callers now use durable edge batches. Raw byte-key APIs remain a separate replication/metadata substrate rather than graph mutation APIs.
3. In progress: `StorageTransaction` now pins an MVCC snapshot, overlays node/edge reads plus label/type/full/adjacency scans, commits primary and derived state plus its prepared MVCC history through one locally serialized Fjall batch, discards rollback writes, and rejects post-snapshot key updates with a typed conflict. The engine exposes an owned context from `Arc<StorageEngine>`, and the Bolt executor retains it from `BEGIN` through terminal `COMMIT`/`ROLLBACK`. Bolt `RUN` now routes connected or disconnected `CREATE`, node and bound-endpoint relationship `MERGE`, `MATCH` and `OPTIONAL MATCH` over connected or disconnected linear fixed and variable-length relationship patterns, `WHERE`, `WITH`, `UNWIND`, `FOREACH` with `SET` updates, property/map/label `SET`, `REMOVE`, `DELETE`, `DETACH DELETE`, and `RETURN` through that owned overlay. End-to-end regressions prove read-your-writes, private node/relationship creation, disconnected pattern staging/matching, optional null preservation and staged relationship visibility, transaction-local one-hop, fixed-chain, bounded variable-length, mixed fixed/variable traversal, `WHERE` filtering, `WITH` row pipelines, `UNWIND` writes, and `FOREACH` updates, private updates, staged property/label removal, staged detach deletion, staged node/relationship merge matching, commit publication, and rollback discard. Extend the transaction-aware evaluator to indexes, schema, and policy metadata before claiming general explicit-transaction support.
4. In progress: WAL persists checksummed, versioned transaction frames as one replay unit and can replay complete frames after an applied sequence marker. Persistent storage owns `copperdb.wal.rmp`, separate from Fjall's internal WAL; each non-empty transactional or ordinary structured node/edge batch, including bulk edge writes, appends its frame before the Fjall commit, that commit atomically advances the applied marker with primary/derived/MVCC state, and open idempotently replays node/edge frames beyond the marker through the ordinary batch path. Async storage flushes now drain their coalesced node/edge queue through that same single batch, producing one WAL frame rather than one frame per record. Startup also removes an interrupted `copperdb.wal.tmp` replacement without using it as a replay source. `NoSync` preserves buffered write behavior and `Immediate` fsyncs the sidecar replacement and Fjall batch before acknowledging a commit. The server maps `storage.sync_writes` to `Immediate` for plaintext and encrypted graph engine storage. Live WAL compaction is marker-safe: `compact_applied_wal` removes only frames already durable in Fjall and leaves later frames for recovery. `inspect_wal` reports healthy, checksum-corrupt, and malformed sidecars without modifying them; normal open remains fail-closed. A guarded repair entrypoint clears checksum-corrupt WAL history only when every entry is already applied, otherwise returning a typed loss-prevention error. Add timed batch sync and explicit operator repair orchestration.
5. Add snapshot install/compaction, reader-aware pruning scheduler, namespace controls, and lifecycle status.

## Test Matrix

Atomic failure, rollback, repeatable ID/index/adjacency scans, node/edge conflicts, endpoint deletion, namespace pinning, restart history, WAL torn tail/checksum/replay, snapshot install, active-reader prune, encryption, and callback ordering. Run storage first, then txsession/engine/Bolt, then workspace tests.

## Benchmarks And Migration

Benchmark 1/100/1,000-op commits under immediate/batch/no-sync modes; disjoint/hot writers; snapshot overhead; replay; MVCC churn/prune. Record p99, commits/s, fsyncs, bytes/op, replay rows/s, and retained bytes/version.

Bump layout/WAL versions. Provide an explicit one-time current-state bootstrap that creates initial heads without fabricating historical versions; reject unknown formats.

## Definition Of Done

One durable boundary covers every derived structure, rollback is invisible, history survives restart, recovery is idempotent, corruption yields typed degraded/repair states, and Bolt acknowledges only after configured durability.