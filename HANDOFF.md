I’ll profile the exact slow relationship-link query path against the current code, compare it with NornicDB’s batch relationship fast paths, then patch the Rust hot path and lock it down with focused performance/semantic tests.

First I’m gathering the live state around CopperDB’s routed `UNWIND MATCH MATCH MERGE relationship SET` execution and NornicDB’s corresponding fast path, because the log points to that exact query taking 40-90 seconds per batch.

Searched for regex `execute_pipeline_create_clause|execute_merge_clause|match_relationship_pattern|put_edge_record|execute_unwind|HYPERLANE|relationship|edge` (`**/copperDB/crates/eval/src/eval_engine.rs`), 160 results

Searched for regex `put_edge_record|put_edge_records|index_edge|edge_property_index|BatchOp::PutEdge|get_edge_record|find_edges|edges_by` (`**/copperDB/crates/storage/src/lib.rs`), 34 results

Searched for regex `UNWIND|MERGE|Relationship|relationship|executeUnwind|batch|fast` (`**/NornicDB/pkg/cypher/**`), 160 results

Read [](file:///c%3A/Users/timot/Documents/GitHub/NornicDB/AGENTS.md#1-1), lines 1 to 200

CopperDB is currently going through the generic routed pipeline for that relationship batch, while storage has only single-edge write APIs. I’m narrowing in on the exact NornicDB relationship batch implementation and the CopperDB merge/write helpers now.

Searched for regex `executeUnwindMerge|UnwindMerge|Batch|batch|fastpath|fast path` (`**/NornicDB/pkg/cypher/*.go`), 200 results

Searched for regex `CreateRelationship|UpdateRelationship|Batch|WAL|relationship|edge|Fast|bulk` (`**/NornicDB/pkg/storage/*.go`), 200 results

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/eval/src/eval_engine.rs#2325-2325), lines 2325 to 2675

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/storage/src/lib.rs#2090-2090), lines 2090 to 2305

The key Nornic fast path is unwind_multi_match_create.go: it prefetches MATCH-side nodes by distinct parameter values and then writes relationships in a tight batch. CopperDB is currently doing row-by-row MATCH scans and row-by-row relationship MERGE writes, which matches the multi-minute browser log.

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/eval/src/eval_engine.rs#3000-3000), lines 3000 to 3185

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/storage/src/lib.rs#3230-3230), lines 3230 to 3325

Read [](file:///c%3A/Users/timot/Documents/GitHub/NornicDB/pkg/cypher/unwind_multi_match_create.go#1-1), lines 1 to 280

Read [](file:///c%3A/Users/timot/Documents/GitHub/NornicDB/pkg/cypher/unwind_multi_match_create.go#280-280), lines 280 to 520

The first fix target is clear: add a CopperDB fast path for `UNWIND $rows AS row MATCH (a:Label {key: row.x}) MATCH (b:Label {key: row.y}) MERGE (a)-[r:TYPE]->(b) SET r.p = row.p`. I’m checking the current MERGE implementation so the fast path keeps the same semantics but avoids the generic row-stream machinery.

Searched for regex `fn execute_merge_clause|fn execute_set_clause|fn persist_edge|edge_record_from|check_edge_constraints|relationships_created|properties_set` (`**/copperDB/crates/eval/src/eval_engine.rs`), 13 results

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/eval/src/eval_engine.rs#3650-3650), lines 3650 to 3845

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/eval/src/eval_engine.rs#4100-4100), lines 4100 to 4255

Searched for regex `struct QueryStats|relationships_created|edges_created|properties_set|HotPathTrace` (`**/copperDB/crates/eval/src/lib.rs`), 6 results

I’m going to add storage-level bulk edge writes and a direct edge lookup by `(start, type, end)`, then use those in the new relationship seed fast path. That gives us both halves NornicDB relies on: pre-batched lookup and bulk mutation.

Before editing, I’m reading the rest of `execute_merge_clause` and the edge index key helpers so the fast path’s idempotency and index maintenance line up with the existing engine.

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/eval/src/eval_engine.rs#3845-3845), lines 3845 to 3995

Searched for regex `fn edge_type_index_key|fn edge_start_index_key|fn edge_end_index_key|fn edge_start_type_index_prefix|fn edge_end_type_index_prefix|struct EdgeRecord` (`**/copperDB/crates/storage/src/lib.rs`), 6 results

Searched for regex `edge_type_index_key|edge_start_index_key|edge_end_index_key|edge_start_type_index_prefix|edge_end_type_index_prefix` (`**/copperDB/crates/storage/src/**`), 19 results

The generic relationship MERGE currently scans every edge of the type for each row, which explains the compounding slowdown as hyperlanes grow. The fast path will use adjacency indexes and a per-batch edge cache instead.

I also found the likely superlinear root cause: each `MERGE (a)-[:HYPERLANE]->(b)` calls `get_edges_by_type("HYPERLANE")` and scans it for every row. That gets slower every batch as the relationship count grows.

Searched for files matching `**/copperDB/crates/**/hot_path_trace*`, 1 match

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/eval/src/eval_engine.rs#3995-3995), lines 3995 to 4075

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/storage/src/lib.rs#3528-3528), lines 3528 to 3580

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/storage/src/lib.rs#960-960), lines 960 to 985

I’ve got enough to patch now: storage gets batch edge persistence plus `find_edge_between`, and eval gets a structural fast path for the demo’s relationship batch shape before generic pipeline routing.

I’m also adding a trace bit for this path, so tests can prove we’re on the optimized route rather than just asserting a timer.

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/cypher/src/hot_path_trace.rs#1-1), lines 1 to 140

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/cypher/src/hot_path_trace.rs#140-140), lines 140 to 240

Now I’m editing three focused areas: the trace flag, storage edge batch primitives, and the eval fast-path detector/executor.

Generating patch (39 lines) in [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/cypher/src/hot_path_trace.rs)


```

```
The trace flag is added. Next I’m adding the storage helpers that avoid type-wide scans for MERGE and collapse final edge writes into one batch-style call.

Generating patch (70 lines) in [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/storage/src/lib.rs)


```

```
Storage now has the two missing primitives. I’m checking the Cypher AST shapes for `MATCH` and `SET` so the detector can be structural instead of string-matching the demo query.

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/cypher/src/ast.rs#1-1), lines 1 to 155

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/cypher/src/ast.rs#155-155), lines 155 to 250

Searched for regex `fn evaluate_pattern_properties|enum SetItem|PropertyAssignment|SetItem::` (`**/copperDB/crates/eval/src/eval_engine.rs`), 12 results

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/eval/src/eval_engine.rs#4800-4800), lines 4800 to 4925

The existing node batch detector gives us a good template. I’m extending that pattern with a relationship plan rather than touching the demo code or falling back to HTTP.

Searched for regex `pub struct NodePattern|pub struct EdgePattern|pub enum SetItem|pub struct PropertyEntry|pub enum Expression` (`**/copperDB/crates/cypher/src/ast.rs`), 5 results

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/cypher/src/ast.rs#388-388), lines 388 to 575

Searched for regex `fn lookup_matching_node_props|fn matching_node_props_with_where|fn node_id\(|fn node_matches_pattern|fn node_record_from_props|fn evaluate_pattern_properties` (`**/copperDB/crates/eval/src/eval_engine.rs`), 3 results

For node endpoint resolution I’m going one step beyond per-row lookups: the fast path will build per-batch maps for each `(label, property)` used by the MATCH clauses, so `Star.starId` is resolved once into an in-memory lookup table for the whole relationship batch.

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/eval/src/eval_engine.rs#4460-4460), lines 4460 to 4525

Searched for regex `fn node_id|fn relationship_id|fn edge_id|fn node_record_from_props|fn edge_value` (`**/copperDB/crates/eval/src/eval_engine.rs`), no results

Searched for regex `fn check_relationship_constraints|ConstraintType::Relationship|RelationshipKey` (`**/copperDB/crates/eval/src/eval_engine.rs`), 5 results

Searched for regex `pub fn get_nodes_by_property|has_node_property_index|node_property_index_definitions` (`**/copperDB/crates/storage/src/lib.rs`), 9 results

Endpoint matching will honor visibility and existing lookup semantics by using `lookup_matching_node_props`, but cache those results per distinct row value and per label/property. That keeps correctness while removing repeated scans.

I’m grabbing the helper definitions for relationship constraints and property/index lookup now; after that the patch should be mechanical.

Searched for regex `fn node_id|fn node_record_to_props|fn edge_record|fn now_unix_ms|fn evaluate_pattern_properties` (`**/copperDB/crates/eval/src/**`), 6 results

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/eval/src/eval_engine.rs#3290-3290), lines 3290 to 3375

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/storage/src/lib.rs#1998-1998), lines 1998 to 2025

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/eval/src/eval_engine_tail.rs#1-1), lines 1 to 80

I’m going to reuse the existing edge conversion/persistence semantics but batch the final records, preserving timestamps for updates and create timestamps for new relationships.

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/eval/src/eval_engine.rs#4890-4890), lines 4890 to 4925

The detector will cover the exact browser link query and a trailing `RETURN count(r) AS ...` variant for tests/benchmarks. It will deliberately refuse ambiguous shapes and fall back to the generic executor.

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/eval/src/eval_engine.rs#4925-4925), lines 4925 to 4955

I’m inserting the relationship fast path alongside the existing node batch path so it catches both direct execution and routed pipeline execution.

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/eval/src/eval_engine.rs#330-330), lines 330 to 355

I’m also replacing the generic relationship MERGE type-wide scan with `find_edge_between`; that gives a baseline improvement even if a query shape falls off the specialized batch path.

Generating patch (28 lines) in [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/eval/src/eval_engine.rs)


```

```
The callers are routed; now I’m adding the actual relationship batch executor and its structural plan detector.

Generating patch (165 lines) in [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/eval/src/eval_engine.rs)

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/eval/src/eval_engine.rs#2645-2645), lines 2645 to 2710

The insert point was slightly different after the latest edits, so I’m applying the fast-path method immediately after the node batch method.

Generating patch (162 lines) in [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/eval/src/eval_engine.rs)


```

```
The executor body is in. I’m adding the plan structs and detector helpers at the bottom of the eval module, near the existing UNWIND batch detector.

Generating patch (212 lines) in [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/eval/src/eval_engine.rs)


```

```
The structural detector and executor are in. I’m running formatting and a quick compile to catch any type drift before adding the focused tests.

Ran terminal command: Push-Location copperDB; cargo fmt; cargo check -p copperdb-eval; Pop-Location

The new fast path compiles. Now I’m adding tests that cover: hot-path trace engagement, idempotent rerun, duplicate edge rows with last-row distance, and a timing guard for the browser-sized relationship batch.

I’m placing the relationship batch tests next to the existing UNWIND fast-path eval tests so they exercise the same API surface the Bolt executor eventually calls.

Generating patch (121 lines) in [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/eval/src/tests.rs)


```

```
The tests are in. I’m running the focused eval tests first, with `--nocapture` so we can see whether the timing guard stays comfortably below the slow browser numbers.

Ran terminal command: Push-Location copperDB; cargo fmt; cargo test -p copperdb-eval --lib test_unwind_match_merge_relationship_set_batch_idempotent_last_row_wins -- --nocapture; cargo test -p copperdb-eval --lib test_unwind_match_merge_relationship_set_batch_browser_sized_performance -- --nocapture; Pop-Location

The new 400-row relationship batch test completes in 0.48s instead of tens of seconds. I’m adding server/Bolt-level coverage for the exact demo link query so this is proven through the same executor path the browser uses.

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/server/src/tests.rs#4215-4215), lines 4215 to 4267

I found the existing Bolt executor 400-star seed test. I’m extending coverage with a sibling relationship-link test that uses the exact query from your log.

Generating patch (75 lines) in [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/server/src/tests.rs)


```

```
The Bolt-level relationship test is in. I’m running it with the existing star seed and demo persistence tests to make sure the full path still behaves.

Ran terminal command: Push-Location copperDB; cargo fmt; cargo test -p copperdb-server --lib appstate_bolt_executor_links_demo_sized_hyperlane_batch -- --nocapture; cargo test -p copperdb-server --lib appstate_bolt_executor_seeds_demo_sized_star_batch -- --nocapture; cargo test -p copperdb-server --lib demo_e2e_seed_query_and_persistence -- --nocapture; Pop-Location

The new Bolt relationship path is correct, but the 400-row server test took 5.6s, so we’re not done. The remaining cost is now storage-side edge indexing/writes, not the Cypher row-stream scan; I’m profiling that layer next and tightening the edge batch write the same way we did for nodes.

Searched for regex `fn index_edge_property_indexes|fn unindex_edge_property_indexes|relationship_property|edge_property_index_definitions|IndexEntityType::Relationship` (`**/copperDB/crates/storage/src/lib.rs`), 7 results

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/storage/src/lib.rs#3308-3308), lines 3308 to 3368

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/storage/src/lib.rs#2075-2075), lines 2075 to 2145

Searched for regex `put_edge_records_batch|put_node_records_batch` (`**/copperDB/crates/storage/src/**`), 2 results

`put_edge_records_batch` was still just looping the expensive single-edge path. I’m opening the relationship property index module so the batch writer can cache index definitions and avoid repeated catalog scans.

Searched for regex `index_edge_property_indexes|unindex_edge_property_indexes|relationship_property_index|edge_property_index_key|IndexEntityType::Relationship` (`**/copperDB/crates/storage/src/**`), 42 results

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/storage/src/lib.rs#3725-3725), lines 3725 to 3775

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/storage/src/lib.rs#3350-3350), lines 3350 to 3385

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/storage/src/storage_edge_property_index.rs#1-1), lines 1 to 220

The edge batch path is repeatedly loading relationship property index definitions. I’m making the relationship index helper reusable and then caching those definitions inside `put_edge_records_batch`.

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/storage/src/storage_edge_property_index.rs#217-217), lines 217 to 365

Read [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/storage/src/storage_edge_property_index.rs#365-365), lines 365 to 390

Generating patch (14 lines) in [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/storage/src/storage_edge_property_index.rs)


```

```
The helper visibility is adjusted. I’m patching the edge batch writer to cache relationship property index definitions and avoid the per-edge helper that reloads them.

Generating patch (57 lines) in [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/storage/src/lib.rs)


```

```


Searched for regex `fn namespace_from_str|fn namespace_from_prefix|namespace_edge_count_key` (`**/copperDB/crates/storage/src/lib.rs`), 6 results

The edge batch still updates the stats counter once per edge. I’m batching that counter update too, which matters in the server path because it hits sled metadata on every relationship create.

Generating patch (37 lines) in [](file:///c%3A/Users/timot/Documents/GitHub/copperDB/crates/storage/src/lib.rs)


```

```
The storage batch is now closer to NornicDB’s “bulk call” model: cached relationship index definitions and one metadata delta per namespace. I’m rerunning the Bolt link test to see the remaining cost.

Ran terminal command: Push-Location copperDB; cargo fmt; cargo test -p copperdb-server --lib appstate_bolt_executor_links_demo_sized_hyperlane_batch -- --nocapture; Pop-Location

The Bolt relationship batch test is now green; full test including database setup completes in 2.54s. I’m running the focused eval/storage/cypher checks next, because we touched all three layers.

Ran terminal command: Push-Location copperDB; cargo test -p copperdb-cypher --lib hot_path_trace -- --nocapture; cargo test -p copperdb-storage --lib relationship_property_index_rebuilds_and_tracks_mutations composite_relationship_property_index_rebuilds_and_tracks_mutations -- --nocapture; cargo test -p copperdb-eval --lib test_unwind_match_merge_relationship_set_batch_idempotent_last_row_wins -- --nocapture; cargo test -p copperdb-eval --lib test_unwind_match_merge_relationship_set_batch_browser_sized_performance -- --nocapture; Pop-Location

The cypher trace and eval performance checks passed; I reused invalid cargo syntax for two storage filters in one command, so I’m rerunning those storage index tests separately.

Ran terminal command: Push-Location copperDB; cargo test -p copperdb-storage --lib relationship_property_index_rebuilds_and_tracks_mutations -- --nocapture; cargo test -p copperdb-storage --lib composite_relationship_property_index_rebuilds_and_tracks_mutations -- --nocapture; Pop-Location

Focused storage index coverage is green too. I’m doing the final validation pass now across the demo server tests, Bolt protocol tests, and a full `cargo check -p copperdb`.

Ran terminal command: Push-Location copperDB; cargo test -p copperdb-server --lib appstate_bolt_executor_links_demo_sized_hyperlane_batch -- --nocapture; cargo test -p copperdb-server --lib appstate_bolt_executor_seeds_demo_sized_star_batch -- --nocapture; cargo test -p copperdb-server --lib demo_e2e_seed_query_and_persistence -- --nocapture; cargo test -p copperdb-bolt --lib; cargo check -p copperdb; Pop-Location

Final validation is green. I’m checking the diff summary now so I can give you the exact files touched and measured results.

Ran terminal command: Push-Location copperDB; git diff --stat; git status --short; Pop-Location

I’m recording the relationship-ingest lesson in repo memory too: the expensive bug was the type-wide edge scan inside MERGE plus non-batched edge metadata/index writes.

Inserted into memory file [](file:///c%3A/Users/timot/AppData/Roaming/Code/User/workspaceStorage/847b45c1dc3ec3e847386ae0aaf9e43d/GitHub.copilot-chat/memory-tool/memories/repo/copperdb.md)

Fixed the slow relationship linking path.

Root cause was twofold: CopperDB’s generic relationship `MERGE` was doing `get_edges_by_type("HYPERLANE")` and scanning all relationships for every row, and the edge “batch” write path was still repeating relationship-index definition loads and metadata counter writes per edge. That explains the worsening 40-90s batches in your log as the relationship count grew.

What I changed:
- Added direct edge lookup and optimized bulk edge writes in lib.rs.
- Exposed relationship property index helpers for cached batch indexing in storage_edge_property_index.rs.
- Added a structural NornicDB-style fast path for:
  `UNWIND $rows AS row MATCH ... MATCH ... MERGE (a)-[r:TYPE]->(b) SET r.prop = row.prop`
  in eval_engine.rs.
- Replaced the generic relationship `MERGE` type-wide scan with `find_edge_between`, so fallback paths are faster too.
- Added a hot-path trace flag in hot_path_trace.rs.
- Added eval and Bolt/server regressions in tests.rs and tests.rs.

Performance profile from the focused tests:
- Eval 400-row relationship batch: `0.38s`.
- Bolt/AppState 400-row exact hyperlane link query, including test DB setup and verification: `2.49s`.
- Existing 400-row star seed path remains green at `0.82s`.
- Demo persistence e2e remains green at `1.56s`.

Validation run:
- `cargo test -p copperdb-server --lib appstate_bolt_executor_links_demo_sized_hyperlane_batch -- --nocapture`
- `cargo test -p copperdb-server --lib appstate_bolt_executor_seeds_demo_sized_star_batch -- --nocapture`
- `cargo test -p copperdb-server --lib demo_e2e_seed_query_and_persistence -- --nocapture`
- `cargo test -p copperdb-bolt --lib`
- `cargo check -p copperdb`

All passed. There are still pre-existing dirty files in the repo from the broader parity work, but the changes for this request are concentrated in cypher trace, eval, storage, and server tests.

Made changes.