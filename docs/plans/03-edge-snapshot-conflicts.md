# 03: Retryable Edge Snapshot Conflicts

Status: planned. Priority: P0. Owners: `storage`, `txsession`, `eval`, `engine`, `errors`, `bolt`.

## Objective

Prevent a transaction from silently overwriting an edge changed after its snapshot and expose the conflict as a retryable Neo4j transaction error.

## References

- Copper: `MvccStore` head/snapshot methods in `crates/storage/src/mvcc.rs`, structured edge writes in `crates/storage/src/lib.rs`, transient mapping in `crates/errors`, tx classification in `txsession`, and Bolt failure encoding.
- Upstream: NornicDB commit `36f2e532`, storage edge snapshot conflict, Cypher merge retry, and Bolt E2E tests.

## Design

Every transactional edge read/update records the observed logical version. Commit compares that expected version to the latest live head within the same serialized commit boundary as the write. Outcomes are: unchanged head -> commit; changed live head -> `Outdated`; tombstone/missing edge -> not found; identical no-op update -> avoid mutation where semantics permit.

## Phases

1. Add a typed `StorageConflict::Outdated { entity, expected, actual }` and map it through storage, engine, HTTP, and Bolt without string matching.
2. Add expected-version metadata to typed transaction edge operations.
3. Implement direct-key latest-head validation under the per-edge or commit lock used by item 8. Check-and-write must be atomic.
4. Route relationship `SET`, `REMOVE`, and `MERGE ... SET` through the conditional update primitive.
5. Add metrics for conflicts and successful retries, excluding entity IDs from labels.

## Tests

- Two snapshots update the same edge; first wins and second returns `Outdated`.
- Fresh retry succeeds.
- Different edges and read-only transactions do not conflict.
- Deleted edge returns not found rather than stale overwrite.
- Identical-value no-op behavior is explicit.
- Bolt receives `Neo.TransientError.Transaction.Outdated`.

Run focused storage, eval, errors, engine, and Bolt conflict tests.

## Performance And Risks

Latest-head validation must be direct-key, not an edge scan. A validation followed by an unlocked write is still incorrect. Avoid a graph-global lock; use the ordered commit/key locking contract from items 7 and 8.

## Definition Of Done

All edge mutation paths carry expected versions, commit validation is atomic, retry classification is preserved end to end, and the upstream conflict scenarios plus non-conflicting controls pass.