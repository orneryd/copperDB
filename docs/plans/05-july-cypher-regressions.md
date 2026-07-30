# 05: July Cypher Regression Closure

Status: complete. Priority: P0. Owners: `cypher`, `eval`, `engine`, `indexing`, `bolt`.

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

## Progress

- Complete: `ce1973e6` quoted map keys with `:` now normalize at the map-key parser boundary, so `SET n.my_property = {'key:key': 'value'}` persists the key as `key:key` without quote characters. Parser and evaluator regressions mirror the upstream case.
- Complete: `4f35ea92` plain `DELETE` now preflights the whole clause and rejects connected nodes unless every adjacent relationship is also explicitly deleted. The rejected statement leaves the graph unchanged, while `DELETE node, relationship` remains valid.
- Complete: `883065cd` trailing `OPTIONAL MATCH` retains unmatched left rows and prior relationship bindings, so projections such as `type(rel)` remain correct while optional properties are `null`.
- Complete: `883065cd` implicit grouping after trailing `OPTIONAL MATCH` counts every matched optional relationship per retained target.
- Complete: `2959060f` repeated relationship variables across chained `MATCH` clauses retain the original edge binding; rows that would overwrite that binding are excluded.
- Complete: `98f6b4c1` indexed `IN` predicates on relationship-pattern start nodes seed traversal from the property index, with trace-backed no-label-scan evidence and a delete-shape correctness regression.
- Complete: `98f6b4c1` relationships introduced in a later `MATCH` remain available to relationship predicates, aggregation, projection, and deletion.
- Complete: `98f6b4c1` chained `MATCH` clauses retain multiple independently bound relationship variables.
- Complete: `8775bf1c` `REMOVE` of a relationship property is scoped to that relationship and leaves endpoint properties untouched.
- Complete: `8775bf1c` typed undirected relationship predicates accept anonymous endpoints and match only the requested relationship type.
- Complete: `8775bf1c` `OPTIONAL MATCH` projects bound relationship and node variables with their type and properties intact.
- Complete: `e4b84afe` disconnected single-node `OPTIONAL MATCH` joins independent matches and null-fills only new variables when unmatched.
- Complete: `e4b84afe` disconnected relationship `OPTIONAL MATCH` independently joins relationship, endpoint, and prior row bindings.
- Complete: `e4b84afe` node-only `OPTIONAL MATCH` preserves an existing binding when the optional label or properties do not match.
- Complete: `e4b84afe` multi-hop `OPTIONAL MATCH` binds every relationship-chain variable in the optional clause.
- Complete: `e4b84afe` aggregate-containing return expressions evaluate correctly after an `OPTIONAL MATCH` traversal.
- Complete: `e4b84afe` `stdev` and `stdevp` implement Neo4j-compatible sample/population standard deviation identities after optional traversal.
- Complete: `b46ceb1f` explicit transactions delete `UNWIND`-matched relationships exactly once and correctly expose commit versus rollback visibility.
- Complete: `b46ceb1f` an empty explicit-transaction `UNWIND` relationship delete is a no-op with zero deletion statistics.
- Complete: `b46ceb1f` `UNWIND` mutation statistics aggregate per input row before Bolt result-summary serialization.
- Complete: `b46ceb1f` Bolt terminal summaries serialize relationship-deletion counters alongside other mutation counters.
- Complete: `4f35ea92` plain `DELETE` remains valid for orphan nodes while connected-node preflight stays enforced.
- Complete: `4f35ea92` `DETACH DELETE` removes a connected node and its incident relationship while preserving peer nodes.
- Complete: `4f35ea92` a multi-row plain `DELETE` rejects atomically when any selected node retains a relationship.
- Complete: `4f35ea92` parameterized `WHERE` selection still enforces the connected-node plain-`DELETE` guard.
- Complete: `4f35ea92` explicit transactions reject a plain connected-node `DELETE` before staging mutation, and rollback preserves committed graph state.
- Complete: `4f35ea92` plain `DELETE` of both connected endpoints without naming their surviving edge is rejected without mutation.
- Complete: `883065cd` a `WHERE` attached to a trailing `OPTIONAL MATCH` filters optional candidates while preserving a null-extended left row.
- Complete: `883065cd` trailing optional traversal preserves relationship-property projections from the preceding `MATCH`.
- Complete: `883065cd` a second trailing `OPTIONAL MATCH` can consume a binding created by the first and project its relationship endpoint.
- Complete: `883065cd` `ORDER BY` and `LIMIT` apply to rows after trailing optional traversal.
- Complete: `883065cd` function projection and aggregate evaluation after trailing optional traversal have exact executable evidence alongside the null-extension, relationship-property, chained-binding, `WHERE`, and order/limit variants.
- Complete: `389fb2e6` explicit transactions preserve `UNWIND MATCH WITH` rows through SET, REMOVE, and DETACH DELETE while aggregating mutation counters.
- Complete: `98f6b4c1` indexed IN-list relationship traversal returns the exact ordered source/target pairs as well as avoiding a label scan.
- Complete: `98f6b4c1` compound IN-list extraction is covered by the parameterized relationship DELETE regression; Go-private relationship-binding adapter tests are non-applicable to Copper's JSON-row evaluator.

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