# copperDB Cypher Parity — Agent Handoff

**Date:** 2026-06-18  
**Last commit:** `891d13e` — "handoff"  
**Working tree:** dirty (constraint enforcement in progress)  
**Repos involved:**
- `/Users/timothysweet/src/copperDB` — Rust workspace (this is the target)
- `/Users/timothysweet/src/NornicDB` — Go upstream (source of truth for behavior)

---

## What We Were Doing

**Goal:** Full Cypher/parser/eval parity between NornicDB (Go) and copperDB (Rust). The approach is: find gaps by comparing behavior, write regression tests in copperDB that demonstrate expected NornicDB behavior, then implement the missing functionality or fix bugs — never adjust tests to paper over gaps.

---

## Current State

### Test Counts
| Crate | Tests | Failing |
|-------|-------|---------|
| copperdb-eval | 247 | 0 |
| copperdb-cypher | 146 | 0 |
| copperdb-engine | 83 | 0 |
| copperdb-storage | 106 | 0 |
| copperdb-server | 28 | 1 (pre-existing distributed timeout) |
| **Workspace total** | **~610** | **1 (pre-existing)** |

All core tests pass. One pre-existing server test (`neo4j_commit_can_opt_into_distributed_graph_read_routing`) has a timing issue in the deferred Layer 3 distributed path; it is not caused by this work.

### Key Files Recently Modified
| File | What changed |
|------|-------------|
| `crates/eval/src/eval_engine.rs` | `+=` nil skip, `check_node_constraints` (Unique/Exists/NodeKey/Type), `check_relationship_constraints` (Unique/Exists/RelationshipKey/Type), `value_matches_type` helper, FOREACH, CALL YIELD, MERGE ON CREATE/MATCH SET, relationship MERGE, aggregation identity |
| `crates/eval/src/regressions/upstream_bugs.rs` | 243 tests — latest: NodeKey, Type, and RelationshipKey constraint enforcement |
| `crates/storage/src/lib.rs` | `NodeRecord` single-format, `get_nodes_by_label`, constraint persistence, `type_name` field on `Constraint` |
| `crates/storage/src/mvcc.rs` | `trigger_prune_now` safety fix |
| `crates/cypher/src/ast.rs` | Expression enum, constraint types (`Unique`, `Exists`, `NodeKey`, `RelationshipKey`, `Type(String)`) |
| `crates/cypher/src/lib.rs` | `parse_merge` with ON CREATE/MATCH SET, `parse_create_constraint` multi-entry |
| `crates/cypher/src/dispatcher.rs` | Implicit RETURN after CALL+YIELD |
| `crates/filter/src/lib.rs` | 60+ functions, aggregation stubs, bracket access, temporal functions |
| `crates/engine/src/lib.rs` | BracketAccess pattern arm added |

### Last 4 Tests Added (Passing)
1. `test_relationship_key_unique_enforcement` — `RELATIONSHIP KEY` blocks duplicate keys between same nodes
2. `test_relationship_key_constraint_enforcement` — `RELATIONSHIP KEY` requires non-null key properties
3. `test_type_constraint_enforcement` — `IS :: INTEGER` type constraint enforced at CREATE time
4. `test_node_key_constraint_enforcement` — `NODE KEY` blocks null keys and duplicate composite keys

---

## Architecture Quick-Reference

```
crates/
  cypher/     — Tokenizer, parser, AST (hand-rolled recursive descent)
  filter/     — Expression evaluation, built-in functions (60+), aggregation
  eval/       — Query execution engine (EvalEngine)
  engine/     — CopperDb embeddable engine, integration, tests
  storage/    — MVCC storage, sled backend, label indexes, constraints
```

### Expression enum (28+ variants in `crates/cypher/src/ast.rs`)
All parse + eval variants: `Literal`, `Variable`, `PropertyAccess`, `Parameter`, `ParameterPropertyAccess`, `FunctionCall`, `Comparison`, `And`, `Or`, `Xor`, `Not`, `Add`, `Subtract`, `Multiply`, `Divide`, `Modulo`, `IsNull`, `IsNotNull`, `InList`, `ListLiteral`, `MapLiteral`, `Case`, `Between`, `ListComprehension`, `Reduce`, `PatternExists`, `BracketAccess`, `MapIndexAccess`...

### Adding a new expression type
1. Add variant to `Expression` enum in `crates/cypher/src/ast.rs`
2. Add parsing in `crates/cypher/src/expression_parser.rs`
3. Add evaluation in `crates/filter/src/lib.rs` (the `eval_expression` match)
4. Add to non-exhaustive matches: `parser_allocation_profile.rs`, `engine/src/lib.rs`, and anywhere else that exhaustively matches `Expression`
5. Add regression test in `crates/eval/src/regressions/upstream_bugs.rs`

---

## Completed (Across Sessions)
- [x] MVCC prune safety fix
- [x] Engine tests migrated to `put_node_record`
- [x] Legacy storage fallback removed
- [x] Temporal functions (date.*, datetime.*, time(), localtime(), etc.)
- [x] CALL YIELD RETURN
- [x] Schema constraints expansion (Unique, Exists, NodeKey, RelationshipKey, Type)
- [x] MERGE ON CREATE SET / ON MATCH SET (parser + eval)
- [x] Relationship MERGE
- [x] Aggregation (avg, sum, min, max, count) in RETURN and WITH
- [x] Aggregation identity on empty input (count→0, sum→0, avg/min/max→null)
- [x] Bracket access `map[key]` parser + eval
- [x] MERGE cache invalidation between queries
- [x] SET += map merge
- [x] Relationship SET (properties on edges)
- [x] ON CREATE/ON MATCH SET for relationship MERGE
- [x] `+=` nil value skip (must not clobber explicit SET values)
- [x] Unique constraint enforcement at CREATE time
- [x] Exists constraint enforcement at CREATE time
- [x] NodeKey constraint enforcement at CREATE time
- [x] Type constraint enforcement (IS :: TYPE) at CREATE time
- [x] RelationshipKey constraint enforcement at CREATE time
- [x] Relationship constraint checking for Unique, Exists, Type, RelationshipKey
- [x] Fulltext index on multiple properties — already implemented end-to-end (parser `ON EACH [...]`, storage multi-property indexing, search across all properties)
- [x] Vector index options persistence — `persist_index_options` / `load_index_options` in storage, round-trip through `CREATE VECTOR INDEX ... OPTIONS {indexConfig: {...}}`
- [x] Temporal constraint enforcement — `IS TEMPORAL [NO OVERLAP]` with temporal overlap detection
- [x] Domain constraint enforcement — `IN [value1, value2, ...]` with allowed values checking
- [x] `allowed_values` field on storage `Constraint` for domain constraints

---

## Remaining Known Gaps
These are from the parity checklist — NOT yet implemented in copperDB:

### Cypher/Eval
- [ ] Vector index options consumed at query time (persisted but not yet read by vector search procedures)
- [ ] Constraint enforcement on relationship properties during MERGE + ON CREATE SET
- [ ] `OPTIONAL MATCH` with relationship patterns (paths, not just nodes)
- [ ] Path materialization / path functions (`nodes()`, `relationships()`)
- [ ] `shortestPath` / `allShortestPaths`
- [ ] `apoc.*` procedure parity
- [ ] `db.*` procedure parity beyond `db.constraints`
- [ ] Subqueries (CALL {} / existential subqueries)
- [ ] Pattern comprehensions `[(n)-->(m) | n.prop]`
- [ ] `FOREACH` with complex expressions

### Storage
- [ ] Async write-behind
- [ ] Reader registry
- [ ] Pruning/rebuild controller
- [ ] Full index maintenance lifecycle
- [ ] Namespace transaction semantics

### Missing Crates
- [ ] `knowledgepolicy` — decay/promotion policy resolution (no Rust crate exists)
- [ ] `lifecycle` — supervisor/component coordination (no Rust crate exists)
- [ ] `errors` — retryable Neo4j wire codes (no Rust crate exists)

### Wiring
- [ ] Bolt message dispatch (handshake exists, no query execution path)
- [ ] GraphQL endpoint wired to HTTP router
- [ ] MCP server transport

---

## How to Continue

### Refreshing context
1. Read `PARITY.md` for high-level audit
2. Read the conversation transcript at:
   `c:\Users\timot\AppData\Roaming\Code\User\workspaceStorage\847b45c1dc3ec3e847386ae0aaf9e43d\GitHub.copilot-chat\transcripts\c2120c31-5983-4ccb-a6c0-b83d13459725.jsonl`
3. Run `cargo test --workspace` to confirm 0 failures before starting

### Recommended next steps (in priority order)
1. **Constraint enforcement completeness** — Add Node Key, Relationship Key, and Type constraint enforcement in `check_node_constraints` (already have tests for unique/exists, add tests for the rest)
2. **Fulltext index multi-property** — NornicDB supports fulltext on multiple properties
3. **Vector index options** — Index kind enum exists but options path incomplete
4. **Schema constraint enforcement on relationship properties** — Only node constraints enforced currently

### Testing pattern
- Tests live in `crates/eval/src/regressions/upstream_bugs.rs`
- Tests follow the `make_engine()` + `Parser::new()` pattern
- Cross-reference NornicDB test files under `C:\Users\timot\Documents\GitHub\NornicDB\pkg\cypher\` and `pkg\storage\` for expected behavior
- All tests must pass — never skip or ignore failing tests

### Building & Testing
```powershell
# Build check
cargo check --workspace

# Run all tests
cargo test --workspace

# Run just eval tests
cargo test -p copperdb-eval --lib

# Run a specific test with output
cargo test -p copperdb-eval --lib test_unique_constraint -- --nocapture

# Run all eval tests with output
cargo test -p copperdb-eval --lib -- --nocapture
```

### NornicDB Reference
- Upstream repo: `C:\Users\timot\Documents\GitHub\NornicDB`
- Cypher tests: `pkg/cypher/merge_test.go`, `pkg/cypher/set_chained_compat_test.go`
- Constraint validation: `pkg/storage/badger_constraint_validation.go`
- Set helpers (`+=`): `pkg/cypher/set_helpers.go`
- Transaction constraint checking: `pkg/storage/badger_transaction.go` (line 2327)
