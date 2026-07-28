# 15: Plugin-Ready Function And Procedure Dispatch

Status: planned. Priority: P1. Owners: `filter`, `eval`, `engine`.

## Objective

Replace hard-coded function/procedure matches and separately maintained discovery lists with immutable registries that support built-ins now and injected extensions later.

## Contract

Descriptors contain canonical case-normalized name, aliases, signature, description/category, read/write/admin mode, and handler. Registration rejects canonical/alias collisions deterministically. Handlers receive evaluated arguments and a restricted context with row/params, capabilities, caller roles, database, and request context.

## Phases

1. Add `FunctionRegistry` and move all scalar built-ins without behavior changes.
2. Add `ProcedureRegistry` and move current procedures into static registrations.
3. Generate `dbms.functions`/`dbms.procedures` discovery from descriptors; enforce dispatch/discovery consistency.
4. Allow engine construction to inject registrars and enforce mode/capability/auth requirements.
5. Add panic isolation, cancellation, and stable extension errors. Dynamic-library ABI and APOC remain deferred.

## Tests

Behavior snapshots before/after migration, case-insensitive and alias lookup, duplicate rejection, metadata consistency, mode/role checks, extension error/panic isolation, cancellation, and unknown-name errors containing the original requested name.

## Performance

Build immutable maps at engine startup. Dispatch should be O(1) with no per-row registry allocation. Benchmark hot scalar functions against the previous match baseline.

## Definition Of Done

Every advertised built-in dispatches, every public handler is advertised unless intentionally hidden, extensions can register without editing core matches, and auth/cancellation/error contracts are enforced centrally.