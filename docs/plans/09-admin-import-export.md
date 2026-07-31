# 09: Offline Administrative Import And Export

Status: in progress. Priority: P1. Owners: new `adminimport` crate and admin binary, plus `convert`, `storage`, `indexing`.

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

## Progress

- Complete: added `copperdb-adminimport` with typed options/errors, deterministic source metadata, cancellation checks, source/target safety validation, typed exit codes, and atomic deterministic JSON reports. `copperdb-admin database import full` now exposes the implemented offline import path without coupling it to the running server executable.
- Complete: Neo4j typed header and scalar/array/vector conversion now lives in `convert`; plain CSV, gzip CSV, and constrained single-entry zip sources validate headers before import work starts.
- Complete: bounded node and relationship records decode in configured chunks and import through storage-owned sibling staging. Successful chunks use `StorageEngine::put_node_records_batch` and `StorageEngine::put_edge_records_batch`; the staged database is the durable cross-chunk ID map for duplicate and relationship endpoint validation. Failed decoding, cancellation, duplicate IDs, or missing endpoints leave no promoted target.
- Complete: `copperdb-adminimport` now exports deterministic Neo4j CSV through ordered storage streams. It infers only sorted property and embedding headers in a first pass, writes nodes and relationships in a second pass, rejects values the current importer cannot round-trip without ambiguity, and atomically publishes a new output directory. `copperdb-admin database export neo4j-csv` exposes the export path offline; supported typed properties, vectors, named embeddings, labels, and relationships have round-trip coverage.
- Complete: export writes a deterministic `copperdb-schema.json` sidecar from the storage-owned constraint/index catalogs and index options. `database import full --schema <path>` parses it before staging, applies constraints after staged data validation, and rebuilds declared indexes through the cancellation-aware storage API before promotion. Schema or index failure leaves no target database visible.
- Complete: importer now supports deterministic anonymous nodes, composite `:ID` values, and named Neo4j ID spaces. Composite or space-qualified identifiers use one stable staging key for both node records and relationship endpoints, while ordinary single-column IDs remain unchanged for compatibility.
- Complete: relationship imports now support an explicit skip policy and bounded bad-row tolerance through `database import full --skip-bad-relationships` and `--bad-relationship-tolerance`. Skipped endpoint/type failures are counted deterministically; CSV decoding, duplicate IDs, and storage failures remain fatal.

## Tests

Header types, custom delimiters, gzip/zip, anonymous/composite IDs, duplicates across chunks, missing endpoints, tolerance thresholds, cancellation, namespace isolation, non-empty rejection, schema failure rollback, deterministic reports, and full round trip.

## Performance

Benchmark 1m nodes/5m relationships with scalar- and vector-heavy records, several chunk sizes, and compression formats. RSS must scale with chunk plus bounded ID-map cache, not dataset size. Report rows/s, MB/s, bytes written, index time, and cancellation latency.

## Definition Of Done

No partial database becomes visible, sources are fully preflighted, memory is bounded, reports and exit codes are deterministic, cancellation cleans up, and a schema/vector/relationship dataset round-trips without semantic loss.