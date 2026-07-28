# 04: Lucene-Classic Full-Text Query Behavior

Status: planned. Priority: P0. Owners: `search`, `storage`, `eval`, `engine`.

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

## Tests And Benchmarks

Mirror every upstream parser table case and malformed input. Add field isolation, deterministic score/ID ties, escaped Cypher parameters, Graphiti queries, and node/relationship parity. Benchmark exact terms, nested Boolean queries, leading wildcard, regex, and fuzzy queries over controlled vocabulary sizes.

Expansion limits, deadline, and cancellation must prevent pathological queries. No grammar branch may silently degrade to bag-of-words.

## Definition Of Done

The mirrored upstream suite passes, both entity kinds share one parser/evaluator, BM25 ordering remains stable, expensive expansion is bounded and cancellable, and no whole-graph scan occurs when a compatible index exists.