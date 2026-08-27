# 13: Truthful Operational Status

Status: in progress. Priority: P1. Owners: `server`, `engine`, `storage`, `bolt`, `otel`, embedding/search runtimes.

## Objective

Replace literal zero/false status fields with cheap immutable snapshots from the components that own process, protocol, database, storage, embedding, and search state.

## Data Ownership

- Server: process start time, HTTP request/error/active counters.
- Bolt: active connections, sessions, transactions, and failures.
- Database/engine: open state, node/edge counts, transaction state, audit/compliance readiness.
- Storage: durable/estimated bytes, WAL/MVCC lifecycle, pending embedding count.
- Embedding/search: readiness, generation, queue/workers, index bytes, strategy, last sanitized error.

## Phases

1. Define versioned `ServerStatusSnapshot` and `DatabaseRuntimeStatus` DTOs with `unknown/degraded` states rather than ambiguous zeros.
2. Instrument HTTP and Bolt ownership with RAII active counters and monotonic process timing.
3. Add cheap snapshot methods to each owner. Cache expensive byte calculations with sample timestamps; never scan records or recurse directories per request.
4. Replace status handlers while preserving compatible fields where meaningful. Add readiness/liveness separation.
5. Add status collection latency and stale-snapshot observability.

## Tests

Values change after requests and graph mutations; active counts return to zero; failed components report degraded; embedding/search states match items 11/12; status remains available during partial failure and responsive under load; sensitive paths/errors are sanitized.

## Progress

- Complete: storage maintains global node and edge cardinalities through atomic mutation deltas. Legacy databases seed missing counters from a one-time keyspace count on their first mutation; status reads never scan records.
- Complete: `/status` returns a versioned, timestamped snapshot with monotonic process uptime, maintained graph cardinalities from the open default engine, database state, and explicit `unknown`/`null` HTTP counters instead of synthetic zeros.
- Complete: `/db/{database}` preserves its established fields while reporting maintained node/edge counts, real storage bytes, search readiness, and embedding runtime state plus pending work. Unowned managed-embedding bytes are explicitly unknown.
- Complete: the UI status contract accepts unknown counters, cardinalities, and embedding bytes and renders unavailable database values as `Unknown` rather than zero or a runtime error.
- Complete: a dedicated unauthenticated telemetry listener serves NornicDB-compatible probes: bodyless `/livez` and map-backed `/readyz`; readiness reports informational storage availability without blocking the probe response.
- Complete: `/status` receives the Bolt listener's RAII-backed active connection, session, explicit transaction, and failure snapshot when Bolt is enabled, and reports explicit unknown values when it is not running.
- Complete: `/status` includes the open database's storage-owned MVCC lifecycle snapshot without scanning graph records; unopened engines report `null` rather than invented lifecycle values.
- Complete: `/status` remains available with an unopened engine and under concurrent request load; unavailable storage snapshots are explicit `unknown`/`null` values rather than handler failures.
- Complete: `/status` reads a bounded storage MVCC snapshot that uses atomics and active-reader bookkeeping only. Scan-derived retention and prune-debt diagnostics remain on the detailed lifecycle endpoint and are explicit `null` in the frequent status snapshot.
- Complete: `/db/{database}` caches Fjall disk-space measurements for five seconds and reports the sample timestamp and age. Successful flushes invalidate the storage-owned sample for refresh on the next poll without adding work to the durability boundary, while the exact synchronous storage size API remains available for non-polling callers.
- Validated: storage regressions cover namespace/global count mutation and pre-counter migration initialization; the focused server regression creates one node and one edge and verifies database and server snapshots report `1/1`, disabled-but-pending embedding work, timestamps, schema versions, and unknown counters.
- Remaining: add broader engine lifecycle snapshots.

## Definition Of Done

No synthetic operational value remains, snapshots have clear owners/timestamps, readiness reflects real dependencies, and status collection is bounded and tested under concurrent load.