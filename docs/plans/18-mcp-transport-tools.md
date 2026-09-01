# 18: MCP Transport And Production Tools

Status: in progress. Priority: P2. Owners: `mcp`, `server`, `engine`, `auth`, `search`.

## Objective

Implement secure, cancellable MCP transport and durable database-scoped `store`, `recall`, `discover`, `link`, `task`, and `tasks` tools after auth, transactions, and search are stable.

## Current Defects

`find_similar` remains a compatibility-only fulltext call. The durable `link`, `task`, and `tasks` tools are not implemented.

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
- The exclusive `--mcp-stdio` subprocess mode implements the protocol's newline-delimited UTF-8 framing without starting network listeners. It keeps stdout JSON-only, sends no response for notifications, supports bounded batches, enforces the 10 MiB request ceiling without unbounded reads, recovers at the next message after malformed or oversized input, honors per-call database selection, and applies a 30-second cancellation deadline. Local process-launch permission is its admin authority boundary.
- The production `store` tool persists an upstream-compatible labeled knowledge node in one owned storage transaction, applies label/type/default precedence, generates bounded titles, flattens metadata while removing caller-supplied vector fields, returns the durable element ID, and records a chained `DATA_CREATE` audit event. HTTP authorization requires database write access before opening the selected engine, and stdio retains its local admin boundary.
- The production `recall` tool retrieves policy-visible nodes by durable ID or upstream-compatible type, all-tags, RFC3339 `since`, and bounded-limit filters. ID lookup takes precedence over filters, large embedding properties are removed from LLM-facing results, and HTTP callers require database read access.
- The production `discover` tool performs policy-filtered hybrid semantic search with normalized keyword fallback, preserves cosine similarity separately from RRF ordering, chunks long queries into bounded embedding batches, hydrates sanitized results, and optionally traverses related context with bounded depth and fan-out. Calls require database read access and emit chained `DATA_READ` audit evidence.
- Three production tools remain pending.

## Tests And Security

Protocol conformance, malformed parameters, notifications, cancellation, oversized input/output, auth by tool/database, write denial, task cycles, conflicts, fallback, audit, and concurrent sessions. Never broaden caller visibility or permit arbitrary labels/relationship injection.

## Definition Of Done

All six tools are durable, schema-valid, database-scoped, cancellable, audited, privilege-safe, and bounded over HTTP and stdio.