# 18: MCP Transport And Production Tools

Status: in progress. Priority: P2. Owners: `mcp`, `server`, `engine`, `auth`, `search`.

## Objective

Implement secure, cancellable MCP transport and durable database-scoped `store`, `recall`, `discover`, `link`, `task`, and `tasks` tools after auth, transactions, and search are stable.

## Current Defects

`find_similar` remains a misleading fulltext call. The six durable production tools and stdio transport are not implemented.

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
- Every serialized tool result is bounded to 1 MiB. Oversized output is replaced rather than truncated with a valid MCP `isError` result containing deterministic size metadata.
- Tool access metadata now drives HTTP database authorization. Read tools require read access, future write tools require write access, and the unrestricted compatibility-only `run_cypher` tool requires admin access.
- HTTP execution preserves authenticated caller roles, forwards declared Cypher parameters, avoids opening databases for protocol-only methods, and awaits native async dispatch. Blocking graph operations are isolated inside their owning handlers.
- The HTTP route enforces the upstream-compatible 10 MiB request ceiling, accepts JSON and `+json` media types, rejects unsupported media with `415`, rejects oversized bodies with `413`, and maps malformed JSON and request shapes to JSON-RPC `-32700` and `-32600` errors.
- HTTP response negotiation accepts JSON-compatible media ranges with quality and specificity precedence, rejects incompatible `Accept` headers with `406`, and applies the upstream-compatible 30-second MCP request deadline through request-context cancellation.
- Tool calls accept the upstream-compatible `arguments.database` selector and unadvertised `arguments.db` alias, remove both before strict schema validation, fall back to the configured default, and authorize the selected database before opening its engine.
- JSON-RPC notifications omit `id`, while explicit null IDs remain requests. Notifications execute through the same authorization and tool path, recognize `notifications/initialized`, and return `202 Accepted` with no response body even when dispatch reports a protocol or tool error.
- JSON-RPC batches are capped at 32 entries, execute sequentially, preserve response order, isolate malformed and unauthorized entries, omit notification entries, reject invalid cardinality with `-32600`, and return an empty `202` response when every entry is a notification.
- Successful single initialization responses issue cryptographically random `Mcp-Session-Id` values from a 4,096-entry, sliding 30-minute store. Presented sessions are validated and refreshed, malformed or unknown IDs are rejected, `DELETE /mcp` terminates sessions, and sessionless protocol `2024-11-05` clients remain compatible.
- MCP requests without an `Origin` header remain compatible with non-browser clients. Browser-originated POST and DELETE requests require exactly one HTTP(S) Origin matching exactly one direct Host authority; malformed, cross-host, and ambiguous headers fail with `403` before session or protocol processing.
- Tool registration atomically binds each immutable descriptor to its compiled schema, access requirement, and typed async handler. Discovery, authorization, validation, and execution therefore resolve through one entry, while execution contexts expose cancellation, the selected graph engine, and authenticated roles.
- Stdio transport and production tools remain pending.

## Tests And Security

Protocol conformance, malformed parameters, notifications, cancellation, oversized input/output, auth by tool/database, write denial, task cycles, conflicts, fallback, audit, and concurrent sessions. Never broaden caller visibility or permit arbitrary labels/relationship injection.

## Definition Of Done

All six tools are durable, schema-valid, database-scoped, cancellable, audited, privilege-safe, and bounded over HTTP and stdio.