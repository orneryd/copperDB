# copperDB Cypher Parity — Agent Handoff

**Date:** 2026-06-18  
**Last commit:** `bfe84fc` — "continuing cypher parity work"  
**Working tree:** clean (all committed)  
**Repos involved:**
- `C:\Users\timot\Documents\GitHub\copperDB` — Rust workspace (this is the target)
- `C:\Users\timot\Documents\GitHub\NornicDB` — Go upstream (source of truth for behavior)

---

## What We Were Doing

**Goal:** Full Cypher/parser/eval parity between NornicDB (Go) and copperDB (Rust). The approach is: find gaps by comparing behavior, write regression tests in copperDB that demonstrate expected NornicDB behavior, then implement the missing functionality or fix bugs — never adjust tests to paper over gaps.

---

## Current State

### Test Counts
| Crate | Tests | Failing |
|-------|-------|---------|
| copperdb-eval | 239 | 0 |
| copperdb-cypher | 146 | 0 |
| copperdb-engine | 83 | 0 |
| copperdb-storage | 106 | 0 |
| **Workspace total** | **~571** | **0** |

All tests pass across the entire workspace.

### Key Files Recently Modified
| File | What changed |
|------|-------------|
| `crates/eval/src/eval_engine.rs` | `+=` nil skip, `check_node_constraints`, FOREACH, CALL YIELD, MERGE ON CREATE/MATCH SET, relationship MERGE, aggregation identity |
| `crates/eval/src/regressions/upstream_bugs.rs` | 239 tests — latest: constraint enforcement, nil key map merge |
| `crates/eval/src/lib.rs` | `#[derive(Debug)]` on `EvalResult`, `node_has_all_labels`, aggregation helpers |
| `crates/cypher/src/ast.rs` | Expression enum: `BracketAccess`, `Reduce`, `ListComprehension`, `Between`, `CASE`, constraint types |
| `crates/cypher/src/expression_parser.rs` | Full recursive descent, bracket access postfix |
| `crates/cypher/src/lib.rs` | `parse_merge` with ON CREATE/MATCH SET, `parse_set_item_with_terminators`, `parse_create_constraint` multi-entry |
| `crates/cypher/src/dispatcher.rs` | Implicit RETURN after CALL+YIELD |
| `crates/filter/src/lib.rs` | 60+ functions, aggregation stubs, bracket access, temporal functions |
| `crates/storage/src/lib.rs` | `NodeRecord` single-format, `get_nodes_by_label`, constraint persistence |
| `crates/storage/src/mvcc.rs` | `trigger_prune_now` safety fix |
| `crates/engine/src/lib.rs` | BracketAccess pattern arm added |
| `crates/cypher/tests/parser_allocation_profile.rs` | BracketAccess pattern arm added |

### Last 3 Tests Added (Passing)
1. `test_merge_on_create_map_merge_nil_keys` — `+=` must not clobber explicit `ON CREATE SET` values with null from map
2. `test_unique_constraint_enforcement` — `CREATE CONSTRAINT ... IS UNIQUE` enforced at CREATE time
3. `test_exists_constraint_enforcement` — `CREATE CONSTRAINT ... IS NOT NULL` enforced at CREATE time

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

## Completed (This Session)
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

---

## Remaining Known Gaps
These are from the parity checklist — NOT yet implemented in copperDB:

### Cypher/Eval
- [ ] Fulltext index on multiple properties
- [ ] Vector index options
- [ ] Node Key constraint enforcement (parsed, not enforced)
- [ ] Relationship Key constraint enforcement (parsed, not enforced)
- [ ] Type constraint enforcement (IS :: TYPE — parsed, not enforced)
- [ ] Temporal constraint enforcement
- [ ] Domain constraint enforcement
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
