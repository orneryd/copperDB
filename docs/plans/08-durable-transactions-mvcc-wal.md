# 08: Durable Transactions, MVCC History, And WAL

Status: in progress. Priority: P1. Owners: `storage`, `txsession`, `engine`, `eval`, `bolt`.

## Objective

Create one atomic, durable transaction boundary covering primary records, indexes, counters, embedding queues, schema effects, and persistent MVCC history.

## Current Evidence

`MvccStore` persists its logical heads, archived versions, and index candidates in metadata and restores them on open; a legacy database without that metadata receives a one-time current-state bootstrap. `WAL` is a standalone primitive rather than the authority for all structured mutations. `BatchWriter::commit` now stages primary records, indexes, counters, embedding metadata, and schema-derived state in one Fjall database batch; it publishes in-memory MVCC changes and callbacks only after the batch succeeds. `txsession::Transaction::commit_at` still changes state without applying typed graph operations.

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
2. In progress: persist MVCC heads/archives and prove visible-at reads across reopen. The persisted state currently follows structured primary-record commits rather than participating in their same Fjall batch; fold it into the typed transaction/WAL boundary before calling this durable commit semantics.
3. In progress: `StorageTransaction` now pins an MVCC snapshot, provides node/edge read-your-writes through a private overlay, commits through the atomic batch writer, discards rollback writes, and rejects post-snapshot key updates with a typed conflict. Connect txsession, eval, engine, and Bolt to this typed transaction instead of their current direct storage execution path.
4. Define WAL frame/version/sync modes, replay markers, torn-tail handling, corruption diagnostics, and repair.
5. Add snapshot install/compaction, reader-aware pruning scheduler, namespace controls, and lifecycle status.

## Test Matrix

Atomic failure, rollback, repeatable ID/index/adjacency scans, node/edge conflicts, endpoint deletion, namespace pinning, restart history, WAL torn tail/checksum/replay, snapshot install, active-reader prune, encryption, and callback ordering. Run storage first, then txsession/engine/Bolt, then workspace tests.

## Benchmarks And Migration

Benchmark 1/100/1,000-op commits under immediate/batch/no-sync modes; disjoint/hot writers; snapshot overhead; replay; MVCC churn/prune. Record p99, commits/s, fsyncs, bytes/op, replay rows/s, and retained bytes/version.

Bump layout/WAL versions. Provide an explicit one-time current-state bootstrap that creates initial heads without fabricating historical versions; reject unknown formats.

## Definition Of Done

One durable boundary covers every derived structure, rollback is invisible, history survives restart, recovery is idempotent, corruption yields typed degraded/repair states, and Bolt acknowledges only after configured durability.