# 05: July Cypher Regression Closure

Status: planned. Priority: P0. Owners: `cypher`, `eval`, `engine`, `indexing`, `bolt`.

## Objective

Disposition and port every selected July NornicDB Cypher regression instead of inferring parity from broad feature support.

## Upstream Families

- OPTIONAL MATCH projection/aggregate identity: `e4b84afe`, `883065cd`.
- Relationship existence/scoping and connected-node delete guard: `4f35ea92`, `8775bf1c`.
- Multi-MATCH relationship rebinding and indexed IN-list traversal: `98f6b4c1`, `2959060f`.
- Colon-containing property keys: `ce1973e6`.
- Explicit-transaction UNWIND mutation statistics: `b46ceb1f`, `389fb2e6`.

## Deliverable: Disposition Manifest

Create `docs/parity/nornicdb-cypher-2026-07.csv` with upstream commit, file, test, behavior, Copper owner, Copper test, disposition, and notes. Allowed dispositions are `implemented`, `merged-equivalent`, and `non-applicable` with a precise reason. Open work is not a completed disposition.

## Phases

1. Inventory all changed tests in the selected commit families and establish failing Copper regressions before implementation.
2. Port parser/property-map cases, preserving `:` in quoted keys without weakening label/map parsing.
3. Port OPTIONAL MATCH function projection, null extension, aggregate identity, relationship variables, existence predicates, REMOVE scope, and connected-node delete rejection.
4. Port relationship binding/rebinding and indexed `IN` traversal. Add trace counters proving no all-node/label scan.
5. After items 2/3/8, port explicit-transaction UNWIND commit/rollback and aggregate Bolt summary counters.
6. Update the manifest and run the workspace suite.

## Correctness And Performance Gates

- Exact rows, ordering, values/types, errors, and mutation statistics match.
- OPTIONAL MATCH preserves each left row and correct empty aggregate identities.
- Connected node deletion requires `DETACH DELETE`.
- Conflicting relationship variable rebinding is rejected or yields no row per upstream semantics.
- Indexed `IN` work scales with list size plus adjacency, not graph cardinality.
- Rollback leaves no mutation and no committed summary count.

Run Cypher, eval upstream regressions, engine, Bolt, and workspace tests.

## Definition Of Done

Every selected upstream test has a checked manifest disposition and executable Copper evidence; no ignored parity regression remains.