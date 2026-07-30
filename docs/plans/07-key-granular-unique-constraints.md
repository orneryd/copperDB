# 07: Key-Granular Unique Constraints

Status: complete. Priority: P1. Owners: `storage`, `eval`. Integrates with item 8.

## Objective

Remove global unique-constraint serialization and label scans while preserving exact node, relationship, composite, update, delete, and namespace semantics.

## Current Anchors

`SchemaManager` stores all unique values behind one `RwLock<BTreeMap<...>>` and cleans stale values with `retain`; eval separately scans matching entities during constraint checks. Upstream reference is NornicDB commit `6362a50` and `unique_lock_*_concurrency_test.go`.

## Design

Define a canonical `UniqueValueKey` containing namespace, entity kind, label/type, ordered property names, and type-preserving encoded values. A reference-counted lock registry provides one lock per exact key. Multi-key operations deduplicate and acquire keys in stable order. A maintained unique-value index maps key to entity ID and is rebuildable derived state.

The protected transaction window is: acquire old/new keys -> validate committed owner -> atomically mutate records/indexes -> release. Error paths cannot leak reservations.

## Phases

1. Canonical encoding and key ordering, including composite values and numeric edge cases.
2. Lock registry with holder/waiter-safe retirement and deadlock-free multi-key acquisition.
3. Maintained unique index and reverse entity-to-key entries; eliminate full-map cleanup.
4. Route node/relationship create, update, delete, node-key, relationship-key, and namespace wrappers through one storage validator.
5. Move final validation into item 8's atomic commit and remove eval label/type scans.

## Tests And Benchmarks

Test disjoint concurrency, same-key single winner, overlapping key-set deadlock resistance, retirement races, typed values, composite keys, old-key release, delete/recreate, relationships, and namespace isolation. Benchmark 1/2/8/32 writers over disjoint, 10% collision, and hot-key workloads; report throughput, p99 wait, conflicts, and registry high-water.

## Risks

Canonical float/NaN handling, stale reservations, and inconsistent composite semantics can corrupt uniqueness. Persist constraints, not lock state; rebuild derived unique indexes on open/schema creation.

## Definition Of Done

No global constraint lock, full-map cleanup, or label scan remains in the commit path; unrelated keys progress concurrently and every collision produces the shared typed constraint error.

## Progress

- Complete: `SchemaManager` now tracks canonical label/property/JSON-value ownership behind per-node and per-value locks. Updates release only their prior keys; the prior full-map stale-value `retain` pass is removed.
- Complete: direct storage regressions prove one winner for concurrent equal values, successful disjoint concurrent values, old-value release, and string-versus-number key separation.
- Complete: canonical keys now preserve ordered composite property/value pairs; direct regressions cover composite UNIQUE isolation, NODE KEY required fields, collisions, and old-key reuse.
- Complete: `StorageEngine` rebuilds keyed node-constraint state from persisted constraints and validates direct node create, update, delete, and every structured `BatchWriter` node mutation at the shared durable batch-commit boundary.
- Complete: direct storage regressions prove persisted unique constraints reject duplicates and release ownership after update or delete without evaluator involvement.
- Complete: explicit `StorageTransaction` commits validate staged node writes under the batch-commit lock against unchanged persisted constraints; failed commits rebuild in-memory ownership from durable records.
- Complete: structured batches with constraint DDL build and validate the effective constraint catalog against the final staged node set before durability, then install the replacement keyed manager only after a successful commit.
- Complete: relationship `UNIQUE` and `RELATIONSHIP KEY` ownership is endpoint-scoped in the shared storage validator; direct edge writes reject same-endpoint collisions and release keys on update/delete while allowing equal values on different endpoints.
- Complete: namespace-scoped constraint catalogs are loaded into the shared manager and keyed by record namespace; tenant constraints enforce only matching prefixed records, survive global DDL manager replacement, and reject invalid namespace DDL without persisting metadata.
- Complete: evaluator duplicate scans for node `UNIQUE`/`NODE KEY` and relationship `UNIQUE`/`RELATIONSHIP KEY` now defer to the shared storage validator; evaluator-side required-property checks remain for compatible diagnostics, while type/domain/temporal validation remains intentionally separate.
- Complete: `MERGE ... ON CREATE SET` defers new node and relationship persistence until final properties are available, preserving valid create-set flows while applying shared storage constraints at the durable write.
- Complete: storage-backed node `EXISTS` constraints treat JSON `null` as absent, matching evaluator behavior and Neo4j `IS NOT NULL` semantics for direct writes.
- Complete: keyed entity-lock and unique-value registries track active holders/waiters and retire idle entries only after the final participant releases; regressions cover update/delete cleanup and retirement races.
- Complete: the durable batch boundary remains locally serialized by Plan 08 for Fjall/MVCC/WAL atomicity, while unique ownership, collision validation, and registry lifecycle are key-granular within that boundary.