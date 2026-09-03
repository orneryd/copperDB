# 22: GraphQL Completeness

Status: deferred. Priority: P2. Owners: `graphql`, `server`, `engine`, `auth`.

## Sequencing

GraphQL feature work remains deferred until Plan 20 closes the latest upstream database settings, authorization, limits, localization, headless routing, and error contracts. This avoids expanding a protocol adapter before its shared engine and boundary behavior is stable.

## Objective

Make GraphQL a thin authenticated, database-scoped adapter over the same engine transaction, policy, audit, cancellation, search, and error contracts used by HTTP and Bolt.

## Current Defects

GraphQL is directly storage-backed, bypassing engine auth/compliance/audit/transactions. Server construction can use a separate temporary storage. Authorization targets the default database and read permission even for mutations.

## Scope

Stable node/relationship models, directional traversal, adjacency-backed neighbors, cursor pagination, transactional CRUD/merge, schema/stats introspection, and search after item 12. Subscriptions wait for a bounded post-commit event broker. Distributed GraphQL is excluded.

## Phases

1. Replace direct storage context with a protocol-neutral engine service carrying principal, roles, database, request context, and read/write intent.
2. Mount explicit database selection, preferably `/db/{database}/graphql`, and reject conflicting selectors.
3. Implement node/relationship reads, traversal, deterministic bounded cursors, batching/data loaders, and typed errors.
4. Implement transactional mutations and explicit atomic versus best-effort bulk behavior.
5. Add maintained search/similar, introspection policy, depth/complexity/alias/response limits, and optional subscriptions after an event broker exists.

## Tests And Performance

Database isolation, read/write/admin denial, rollback, constraints, traversal directions/types, cursor stability, N+1 prevention, malformed scalars, localized stable error extensions, audit, cancellation, and HTTP auth. Benchmark paginated scans, one/two-hop traversal, mutation batches, and complexity rejection.

## Definition Of Done

GraphQL sees identical data/permissions to other protocols, performs no direct storage mutation or unbounded scan, enforces operation-specific authorization, and passes transaction/policy/audit tests.