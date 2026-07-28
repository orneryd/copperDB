# Audit Implementation Plans

Date: 2026-07-28

These plans expand the numbered findings in [../COPPERDB_NORNICDB_PARITY_PLAN.md](../COPPERDB_NORNICDB_PARITY_PLAN.md). The consolidated audit owns priority and status; these files own implementation detail. Documents under [old](old) are historical references only.

## Execution Rules

- Implement in numeric order unless a plan explicitly states it can proceed independently.
- Port upstream tests as behavioral specifications, not upstream internals.
- Keep automatic indexing, search, embedding, inference, and graph mutation disabled by default.
- Run focused tests after each phase, dependent-crate tests before completion, and workspace tests before changing audit status.
- Add benchmarks only after correctness tests pass. Performance work must retain the same public behavior and error contract.
- Update the owning plan and consolidated audit in the same change that completes an item.

## Plans

| Item | Priority | Plan | Prerequisites |
| ---: | --- | --- | --- |
| 1 | P0 | [Authentication defaults and precedence](01-auth-defaults-and-precedence.md) | None |
| 2 | P0 | [Bolt authentication and transactions](02-bolt-auth-and-transactions.md) | 1; item 8 for durable commit |
| 3 | P0 | [Edge snapshot conflicts](03-edge-snapshot-conflicts.md) | Transaction foundation from 2/8 |
| 4 | P0 | [Lucene-classic full-text queries](04-lucene-classic-fulltext.md) | None |
| 5 | P0 | [July Cypher regression closure](05-july-cypher-regressions.md) | 2/3 for transaction cases |
| 6 | P0 | [Embedding cache capacity](06-embedding-cache-capacity.md) | None |
| 7 | P1 | [Key-granular unique constraints](07-key-granular-unique-constraints.md) | Integrates with 8 |
| 8 | P1 | [Durable transactions, MVCC, and WAL](08-durable-transactions-mvcc-wal.md) | 7 lock contract |
| 9 | P1 | [Administrative import/export](09-admin-import-export.md) | 8 batch boundary |
| 10 | P1 | [Maintained CPU HNSW](10-maintained-cpu-hnsw.md) | Storage events from 8 |
| 11 | P1 | [Per-database embedding lifecycle](11-per-database-embedding-lifecycle.md) | 6; preferably 10 |
| 12 | P1 | [Semantic and hybrid search](12-semantic-hybrid-search.md) | 4, 10, 11 |
| 13 | P1 | [Operational status](13-operational-status.md) | 11/12 status contracts |
| 14 | P1 | [Local cancellation propagation](14-local-cancellation-propagation.md) | None; consumed by 9-13 |
| 15 | P1 | [Plugin-ready dispatch](15-plugin-ready-dispatch.md) | None |
| 16 | P2 | [Observability exporters and tracing](16-observability-exporters-tracing.md) | 13 ownership contracts |
| 17 | P2 | [GraphQL completeness](17-graphql-completeness.md) | 1, 2/8, 12, 14 |
| 18 | P2 | [MCP transport and tools](18-mcp-transport-tools.md) | 1, 8, 12, 14, 15 |
| 19 | P2 | [Heimdall, inference, link prediction, and reranking](19-ai-governance-runtime.md) | 11, 12, 16 |
| 20 | P2 | [GPU and SIMD acceleration](20-gpu-simd-acceleration.md) | 10, 12 and CPU benchmarks |

## Status Convention

Each plan starts as `planned`. Change it to `in progress` only when implementation begins and to `complete` only when every definition-of-done item and validation command passes. Record blocked phases and upstream commit changes directly in the plan.