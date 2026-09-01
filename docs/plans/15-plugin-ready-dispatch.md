# 15: Plugin-Ready Function And Procedure Dispatch

Status: complete. Priority: P1. Owners: `filter`, `eval`, `engine`.

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

## Completion Notes

- Added immutable startup-built scalar and procedure registries with case-insensitive canonical names, aliases, deterministic collision rejection, descriptors, and O(1) lookup.
- Migrated built-in dispatch and `dbms.functions`/`dbms.procedures` discovery to registry descriptors while preserving CopperDB's existing advertised rows and behavior snapshots.
- Added constructor-level scalar and procedure registrar injection through `EvalEngine` and `CopperDb`, including restricted row/parameter, capability, role, database, and request contexts.
- Enforced extension capability and role requirements, panic isolation, cancellation before and after handlers, stable extension errors, consistent procedure columns, and unknown-name errors containing the requested spelling.
- Enforced procedure `READ`, `WRITE`, and `DBMS` modes at HTTP and authenticated Bolt authorization boundaries. Unknown extension calls remain conservatively write-classified.
- Added a Criterion dispatch benchmark. On the completion run, canonical immutable-registry lookup measured approximately 13.7 ns versus 30.1 ns for the previous lowercase-plus-match shape; mixed-case lookup measured approximately 50.8 ns and allocates only for normalization.
- Validated 27 filter tests, 364 eval tests, 118 engine tests, and 100 server tests. Workspace Clippy passed for all targets and features with warnings denied; touched-file rustfmt and `git diff --check` passed.
- Package loading, APOC compatibility, and representative Heimdall verification continue in [item 17](17-plugin-packages-apoc-verification.md). Item 17 begins with statically linked package factories rather than exposing Rust's unstable dynamic-library ABI.