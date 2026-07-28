# 09: Offline Administrative Import And Export

Status: planned. Priority: P1. Owners: new `adminimport` crate and admin binary, plus `convert`, `storage`, `indexing`.

## Objective

Provide bounded-memory, offline Neo4j-compatible import/export with staging, deterministic reports, cancellation, schema application, and index construction.

## Scope

Create `copperdb-adminimport` for pipeline behavior and `copperdb-admin` for `database import full` and `database export neo4j-csv`. Keep scalar/header conversions in `convert`. Inputs include plain, gzip, and zip CSV; outputs include deterministic node/relationship CSV and schema metadata.

## Safety Contract

Preflight every source before writing. Require exclusive offline access. Import into a staging directory and atomically promote only after data, constraints, and indexes succeed. Reject archive traversal, zip bombs, oversized fields, non-empty targets, and unsafe output paths. Cancellation removes staging state.

## Phases

1. CLI/options, typed errors, source preflight, report schema, and exit codes.
2. Neo4j typed headers, delimiters, quoting, multiline records, and compressed readers using `BufRead`.
3. Chunked node pass and a temporary fjall ID map; relationship pass with endpoint validation and duplicate/bad-row policies.
4. Apply schema and build derived property/fulltext/vector indexes using item 8's batch/event contracts.
5. Streaming deterministic export and import/export round-trip validation.

## Tests

Header types, custom delimiters, gzip/zip, anonymous/composite IDs, duplicates across chunks, missing endpoints, tolerance thresholds, cancellation, namespace isolation, non-empty rejection, schema failure rollback, deterministic reports, and full round trip.

## Performance

Benchmark 1m nodes/5m relationships with scalar- and vector-heavy records, several chunk sizes, and compression formats. RSS must scale with chunk plus bounded ID-map cache, not dataset size. Report rows/s, MB/s, bytes written, index time, and cancellation latency.

## Definition Of Done

No partial database becomes visible, sources are fully preflighted, memory is bounded, reports and exit codes are deterministic, cancellation cleans up, and a schema/vector/relationship dataset round-trips without semantic loss.