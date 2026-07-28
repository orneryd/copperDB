# 07: Key-Granular Unique Constraints

Status: planned. Priority: P1. Owners: `storage`, `eval`. Integrates with item 8.

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