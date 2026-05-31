# Neo4j/NornicDB Parity Contract (Current Slice)

Scope note: this file describes local Cypher/parser/storage parity only. copperDB's supported runtime architecture remains single-node, and distributed execution is intentionally out of scope here.

## Cypher DDL surface (implemented in this slice)

- `CREATE CONSTRAINT <name> [IF NOT EXISTS] FOR (n:<Label>) REQUIRE n.<property> IS UNIQUE`
- `CREATE CONSTRAINT <name> [IF NOT EXISTS] FOR (n:<Label>) REQUIRE n.<property> IS NOT NULL`
- `DROP CONSTRAINT <name> [IF EXISTS]`
- `SHOW CONSTRAINTS`
- `CREATE INDEX <name> [IF NOT EXISTS] FOR (n:<Label>) ON (n.<property>[, n.<property>...])`
- `DROP INDEX <name> [IF EXISTS]`
- `SHOW INDEXES`

## Cypher DDL parser error contract

- Variable mismatch between `FOR (...)` and `REQUIRE/ON` is rejected.
- Unsupported `SHOW` and `DROP` targets are rejected with deterministic parse errors.
- Missing required DDL keywords (`FOR`, `REQUIRE`, `ON`) are rejected.

## Evaluator/storage DDL execution contract

- Creating an existing constraint/index returns an error unless `IF NOT EXISTS` is present.
- Dropping a missing constraint/index returns an error unless `IF EXISTS` is present.
- `SHOW CONSTRAINTS` and `SHOW INDEXES` return deterministic metadata row sets.

## Pending parity areas (out of scope for this slice)

- Bolt state machine parity and Bolt-over-WebSocket parity.
- Full configuration knob/fallback parity matrix.
- Full Cypher coverage gate enforcement in CI.
