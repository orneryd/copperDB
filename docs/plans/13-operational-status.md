# 13: Truthful Operational Status

Status: planned. Priority: P1. Owners: `server`, `engine`, `storage`, `bolt`, `otel`, embedding/search runtimes.

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

## Definition Of Done

No synthetic operational value remains, snapshots have clear owners/timestamps, readiness reflects real dependencies, and status collection is bounded and tested under concurrent load.