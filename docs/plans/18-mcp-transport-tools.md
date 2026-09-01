# 18: MCP Transport And Production Tools

Status: in progress. Priority: P2. Owners: `mcp`, `server`, `engine`, `auth`, `search`.

## Objective

Implement secure, cancellable MCP transport and durable database-scoped `store`, `recall`, `discover`, `link`, `task`, and `tasks` tools after auth, transactions, and search are stable.

## Current Defects

Dispatch is synchronous/ad hoc, `run_cypher` ignores parameters, `find_similar` is a misleading fulltext call, and ingress applies read authorization to every tool. Arbitrary Cypher can therefore attempt writes through a read-gated tool.

## Phases

1. Add async typed tool handlers, strict JSON Schema validation, protocol negotiation, cancellation, bounded output, and structured tool errors using item 17's package/action descriptors and item 15's immutable dispatch principles.
2. Harden HTTP JSON-RPC with limits, timeouts, content types, notifications/batches, sessions, and per-tool/database authorization. Add stdio as a separate local mode; WebSocket is not required.
3. Implement transactional `store`, `recall`, and `link` with identifier/metadata validation and audit.
4. Implement durable `task`/`tasks`, valid status transitions, cycle-free dependencies, deletion rules, stable sort, and pagination.
5. Implement `discover` over item 12 with depth capped at 3, feature gates, policy-filtered hydration, and documented lexical fallback. Remove or explicitly classify unrestricted `run_cypher` as non-production/admin-only.

## Progress

- The JSON-RPC dispatch boundary validates version `2.0`, negotiates the supported MCP protocol version `2024-11-05`, and reports protocol failures with stable codes and structured data.
- Tool schemas are compiled when registered. Invalid schemas are rejected atomically, and `tools/call` arguments are validated before execution against closed built-in schemas and bounded numeric inputs.
- Tool listing advertises the upstream-compatible immutable registry capability with `tools.listChanged: false`.
- Async typed handlers, bounded output, hardened transports, per-tool authorization, and production tools remain pending.

## Tests And Security

Protocol conformance, malformed parameters, notifications, cancellation, oversized input/output, auth by tool/database, write denial, task cycles, conflicts, fallback, audit, and concurrent sessions. Never broaden caller visibility or permit arbitrary labels/relationship injection.

## Definition Of Done

All six tools are durable, schema-valid, database-scoped, cancellable, audited, privilege-safe, and bounded over HTTP and stdio.