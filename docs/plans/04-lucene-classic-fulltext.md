# 04: Lucene-Classic Full-Text Query Behavior

Status: complete. Priority: P0. Owners: `search`, `storage`, `eval`, `engine`.

## Objective

Implement one field-aware Lucene-classic query parser/evaluator for node and relationship full-text procedures while retaining maintained indexes and deterministic BM25 ranking.

## References

- Copper: `SearchIndex` in `crates/search/src/lib.rs`, storage full-text lookup/tokenization, and procedure dispatch in `crates/eval/src/eval_engine_policy.rs`.
- Upstream: commit `e090df01`; `pkg/cypher/fulltext_query.go`, parser tests, `call_fulltext.go`, and procedure tests.

## Query Contract

Support Boolean `AND/OR/NOT` and symbolic forms, nested groups, required/prohibited prefixes, field scopes and field groups, phrases and proximity, fuzzy terms, inclusive/exclusive ranges, boosts, `?`/`*` wildcards including leading wildcards, regex, presence, match-all, and Lucene escaping. Malformed input returns a typed deterministic query error.

## Architecture

Add a tokenizer, recursive-descent parser, and immutable query AST in `copperdb-search`. Evaluation receives a field-aware document and an index vocabulary/posting interface. AST nodes expose primary scoring terms so exact terms can seed candidates and reuse BM25 term statistics. Both procedure paths and engine search call the same API.

## Phases

1. Tokenizer, escape decoder, AST, precedence parser, and parser-only tests.
2. Exact terms, fields, Boolean occurrence, groups, phrases, and presence.
3. Proximity, fuzzy, ranges, boosts, wildcard, and regex evaluation.
4. Candidate planning over maintained postings; bounded vocabulary expansion and hydration.
5. Node/relationship procedure integration, parsed-query cache keyed by query plus index schema generation, and observability.

## Progress

- Complete: `copperdb-search::lucene` now provides a typed AST and deterministic parser errors for Boolean precedence, grouping, required/prohibited clauses, fields and field groups, phrases/proximity, boosts, fuzzy terms, ranges, wildcards, regex, presence, and escapes.
- Complete: its field-aware evaluator implements exact terms, Boolean occurrence, phrases/proximity, presence, wildcard, fuzzy, range, regex, and boost semantics. Parser and evaluator tests include the Graphiti-style `group_id:"ft_repro" AND (CloudTrail)` shape.
- Complete: `db.index.fulltext.queryRelationships` uses the shared parser/evaluator over declared indexed properties, deduplicates overlapping catalog hits by relationship ID, and seeds candidates only from maintained relationship full-text postings.
- Complete: node full-text candidate planning uses declared maintained postings. Exact term/phrase queries use direct postings; wildcard, fuzzy, regex, range, presence, and proximity queries expand only from a deterministic vocabulary scan capped at 2,048 terms and 16,384 posting entries. A truncated scan fails explicitly rather than returning incomplete results.
- Complete: pure-negative Lucene queries such as `NOT beta` now seed candidates from the same bounded complete vocabulary and are filtered by the shared evaluator, so node and relationship procedures retain Neo4j/NornicDB prohibited-clause behavior without graph scans.
- Complete: storage vocabulary scans have explicit truncation and cancellation behavior for nodes and relationships. Relationship postings are rebuilt on index creation and maintained atomically across edge inserts, updates, and deletes; relationship wildcard queries use bounded vocabulary expansion rather than graph scans. Node procedure regressions cover field Boolean/phrase queries plus wildcard, fuzzy, regex, range, and proximity queries.
- Complete: request-context cancellation propagates through `execute_with_context` into vocabulary expansion, posting scans, candidate hydration, document evaluation, ranking boundaries, and result materialization for both entity kinds.
- Complete: parsed full-text queries are cached in the evaluator by query text plus the storage index-schema generation. Direct and atomic index DDL advance that generation, so the cache discards stale entries before reuse.
- Complete: the server records catalog-backed full-text procedure request outcome, index-stage duration, and returned candidate count without query-text or index-name labels.
- Complete: Criterion benchmarks exercise exact terms, nested Boolean queries, leading wildcards, regex, and fuzzy expansion over deterministic 256- and 2,048-term vocabularies in `crates/search/benches/lucene_fulltext.rs`.
- Complete: public node procedure coverage verifies escaped Lucene query parameters and deterministic descending-score, ascending-ID ordering for equal scores; relationship ranking uses the same ordering.
- Complete: `mirrors_nornicdb_lucene_parser_and_evaluator_matrix` ports the upstream parser/evaluator truth table across Boolean forms, field scopes/groups, phrases/proximity, wildcard variants, range boundaries, regex, fuzzy distances, escapes, unknown fields, boosts, nested groups, and pure-negative queries. Node and relationship procedure regressions cover the corresponding public call paths, options, parameter handling, match-all, field presence, and empty input.

## Tests And Benchmarks

Mirror every upstream parser table case and malformed input. Add field isolation, deterministic score/ID ties, escaped Cypher parameters, Graphiti queries, and node/relationship parity. Benchmark exact terms, nested Boolean queries, leading wildcard, regex, and fuzzy queries over controlled vocabulary sizes.

Expansion limits, deadline, and cancellation must prevent pathological queries. No grammar branch may silently degrade to bag-of-words.

## Definition Of Done

The mirrored upstream suite passes, both entity kinds share one parser/evaluator, BM25 ordering remains stable, expensive expansion is bounded and cancellable, and no whole-graph scan occurs when a compatible index exists.