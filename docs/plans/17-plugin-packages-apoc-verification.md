# 17: Plugin Packages And APOC Verification

Status: complete. Priority: P2. Owners: `eval`, `engine`, `server`, `copperdb`, `heimdall`, `plugin`, and `apoc` crates.

## Implementation Progress

- Complete: versioned package descriptors and factories, typed requested/granted capabilities, host compatibility, deterministic dependency sorting, transactional collisions, immutable registries, and package-attributed function/procedure discovery.
- Complete: disabled-by-default static package allowlist, required/optional failure policy, package configuration, bounded panic-safe initialize/start/stop/shutdown hooks, health/status, reverse drain, process-wide startup resolution, and package-aware engine construction for every logical database.
- Complete: separate `copperdb-apoc` package with the eight representative scalar functions and mirrored NornicDB query-contract tests.
- Complete: database-scoped read-only graph host service and bounded `apoc.path.subgraphNodes` with direction/type filters, label filters, deterministic BFS order, authorization, cancellation, and mirrored NornicDB semantics.
- Complete: separate Heimdall watcher package loaded through the production catalog with exact lifecycle, MCP-compatible `heimdall_watcher_query`, database-scoped read authorization, cancellation, bounded timestamped FIFO event hooks, newest-event drop, panic/timeout isolation, and package metrics.
- Complete: local `apoc.load.json` through a default-denied rooted import-file host service with explicit `FileImport` grants, canonical symlink containment, file-URI validation, cancellation, byte/row bounds, ordered root-array expansion, and strict trailing-data rejection.
- Complete: offline package configuration-schema compilation and instance validation before factory creation, closed-by-default package settings, stable package-attributed errors, and APOC's declared `file_access_root` contract.
- Complete: loaded APOC dispatch through embedded execution, the HTTP transaction endpoint, and a real Bolt 4.4 TCP `HELLO`/`RUN`/`PULL` exchange with deterministic result assertions.
- Complete: Criterion coverage for cold package lifecycle, canonical/mixed-case registry dispatch, representative APOC scalar execution, bounded traversal, rooted JSON loading, and event enqueue/saturation. Initial quick-mode baselines measured approximately `152 us` cold startup, `33 us` scalar query execution, `184 us` per 1,000-row JSON load, `47 us` per bounded 65-node traversal, `1.11 us` enqueue plus dispatcher turn, and `50 ns` saturated ingress rejection.
- Complete: default-denied transactional graph-write host service and bounded representative `apoc.import.json` with explicit `QueryWrite` and `FileImport` grants, atomic implicit batches, caller-owned explicit transaction staging, rollback/commit visibility, pre-write validation, exact result columns, and real mutation statistics.
- Complete: default-denied remote `apoc.load.json` through an explicit `Network` grant and normalized exact/wildcard host allowlist, with URL credential/fragment rejection, DNS resolution pinned to validated public addresses, private/loopback/link-local/multicast rejection, proxies and redirects disabled, caller-bounded deadlines, HTTP status checks, bounded streaming, and deterministic adversarial tests.

## Objective

Turn item 15's injected function/procedure registrars into a production package system that loads independently owned plugins at startup. Prove the contract with a separate `copperdb-apoc` package and a representative Heimdall package whose registration, discovery, authorization, lifecycle, cancellation, errors, and behavior match NornicDB. Do not port the full APOC suite in this item; make later function and procedure ports mechanical additions to the package.

GraphQL completeness is deferred to item 21 and must not begin until this package boundary is complete.

## Upstream Contract

Use NornicDB's current implementations and tests as behavioral specifications:

- `apoc/apoc.go`, `apoc/registry`, `apoc/plugin`, `apoc/storage`, and `apoc/plugin-src/apoc` define APOC package registration and storage-facing execution.
- `pkg/nornicdb/plugins.go`, `pkg/nornicdb/db.go`, and `pkg/nornicdb/apoc_storage_adapter.go` define startup discovery, package initialization, partial-failure behavior, and database services.
- `pkg/cypher/apoc_*.go`, `functions.go`, and `procedure_registry*.go` define callable behavior and discovery metadata.
- `pkg/heimdall/plugin.go`, `types.go`, `handler.go`, `scheduler.go`, `rbac_context.go`, and `plugins/heimdall` define action discovery, lifecycle, hooks, bounded events, database routing, and RBAC.

CopperDB should preserve item 15's stricter deterministic collisions, panic isolation, typed errors, and immutable dispatch. It must not reproduce upstream's global mutable registries, unsafe partial registration, or unversioned Go `.so` assumptions.

## Missing Architecture Audit

Item 15 proves in-process registrar injection but does not yet provide:

- package identity, version, provider, dependencies, compatibility version, or ownership of descriptors;
- configured startup discovery, enable/disable policy, deterministic dependency order, or rollback when package initialization fails;
- initialize/start/stop/shutdown lifecycle, health/status, reverse-order drain, or package failure isolation;
- typed package capabilities or database-scoped services for queries, writes, schema, admin, files, network, metrics, audit, and events;
- package configuration validation or secret-safe configuration access;
- complete typed parameter/return/default metadata, `SCHEMA`/`ADMIN` modes, system-database eligibility, examples, deprecation, and package attribution;
- package discovery procedures and proof that discovery, authorization, and dispatch use one descriptor source;
- a separate APOC crate or representative Heimdall package loaded through the same runtime;
- default-deny rooted file and allowlisted network services for APOC I/O procedures;
- a stable dynamic extension ABI, package trust policy, reload, or unload.

## Package And Loader Contract

Add a `copperdb-plugin` crate owning a versioned Rust package API. A package descriptor contains a stable package ID, semantic version, provider, compatibility version, dependencies, configuration schema, requested capabilities, and function/procedure/action descriptors. A package factory creates an isolated instance from a restricted host context.

The executable loads configured package factories before database engines become available, validates the complete dependency graph and descriptor set transactionally, then initializes and starts packages in deterministic dependency order. One disabled or failed optional package must not prevent unrelated packages or the database from starting. A required package failure must fail startup with the package ID and stable error code. Shutdown stops packages in reverse order within lifecycle bounds.

The first implementation uses statically linked package factories from separate crates. Do not expose Rust trait objects through a dynamic-library ABI. Runtime-installed plugins require a separately versioned WASM component or subprocess protocol, signature/trust policy, resource limits, and compatibility tests; that ABI is a follow-up after this package contract is proven.

Configuration defaults every non-core package off. Support an explicit package allowlist and per-package configuration. Unknown package IDs, duplicate IDs, dependency cycles, incompatible host versions, descriptor collisions, and ungranted capabilities fail deterministically before any registry becomes visible.

## Host Services And Security

Replace free-form capability strings at the package boundary with typed grants for query read, query write, schema, DBMS admin, file import, file export, outbound network, metrics, audit, events, and model invocation. Function, procedure, action, and hook calls receive the authenticated principal, roles, selected database, transaction intent, request context, package logger/metrics/audit handles, and only the granted services.

Every handler and lifecycle hook is panic-isolated, cancellable, deadline-bounded, output-validated, and attributed to its package. Package errors have stable public codes and must not expose paths, credentials, query text, or configuration secrets.

File and network access are denied by default. Representative I/O uses rooted file handles with canonical containment and symlink defenses, bounded streaming parsers, independent import/export grants, explicit remote-host allowlists, DNS resolution pinned to validated addresses, redirects and proxies disabled, private/loopback/link-local/multicast rejection, response byte limits, deadlines, and audit events. No package receives an unrestricted filesystem path or HTTP client.

## Representative APOC Package

Create `copperdb-apoc` as a separate workspace crate that exports one package factory and owns all `apoc.*` descriptors. Port a deliberately small cross-section from current NornicDB:

- pure scalar functions: `apoc.create.uuid`, `apoc.text.join`, `apoc.coll.flatten`, `apoc.coll.toSet`, `apoc.map.merge`, `apoc.convert.toJson`, `apoc.convert.fromJsonMap`, and `apoc.meta.type`;
- one graph read procedure: `apoc.path.subgraphNodes` with bounded traversal, direction/type filtering, deterministic order, authorization, and cancellation;
- one guarded I/O procedure: `apoc.load.json` using the host's bounded file/network service rather than opening paths or URLs directly;
- one write procedure: `apoc.import.json`, limited initially to a bounded representative node/relationship fixture, to prove guarded input, transaction ownership, write authorization, rollback, audit, and mutation statistics without beginning the full import suite.

Match NornicDB names, aliases, null behavior, coercion, defaults, result columns, ordering, errors, and `dbms.functions`/`dbms.procedures` metadata. Keep reusable conversion and collection helpers inside `copperdb-apoc` so subsequent APOC ports only add descriptors, handlers, and mirrored tests.

The full APOC function/procedure catalog, bulk import/export, refactor, atomic, neighbors, and graph algorithm breadth remain follow-up mechanical ports after this verification set passes.

## Representative Heimdall Package

Adapt the existing `heimdall` crate to export a package through the same loader. Implement a representative watcher with initialize/start/stop/shutdown, health/status, one read-only action with MCP-compatible JSON input schema, one bounded database-event hook, and package metrics. Verify database-scoped RBAC, cancellation, queue saturation, panic isolation, and reverse shutdown.

This item proves the package and action/hook architecture only. Inference providers, reranking, suggestions, autonomous mutation, and governance workflows remain in item 19 and disabled by default.

## Phases

1. Add package descriptors, factories, host compatibility, dependency sorting, transactional registration, package-attributed discovery, and stable loader errors.
2. Add typed capability grants, restricted database services, package configuration, health/status, lifecycle supervision, panic isolation, cancellation, deadlines, and reverse shutdown.
3. Build `copperdb-apoc` and port the representative pure scalar set with mirrored upstream behavior/discovery tests.
4. Add `apoc.path.subgraphNodes` and the bounded representative `apoc.import.json` contract through database-scoped transactional host services.
5. Add secure `apoc.load.json` and `apoc.import.json` file and remote modes with default-deny policy and adversarial containment/network tests.
6. Load the representative Heimdall package through the same runtime and verify action schema, RBAC, bounded hooks/events, metrics, health, and shutdown.
7. Exercise loaded package calls through embedded engine, HTTP transaction endpoint, and Bolt, then benchmark startup, dispatch, traversal, I/O streaming, and hook overhead.

## Tests And Performance

Mirror the relevant cases from NornicDB's `plugins_test.go`, `plugin_e2e_test.go`, `plugin_unified_e2e_test.go`, Cypher plugin/APOC tests, Heimdall plugin/handler tests, and watcher tests. Cover disabled-by-default behavior, deterministic load order, dependency cycles, incompatible versions, duplicate descriptors, all-or-nothing registration, optional versus required failures, discovery equality, mode/role/capability denial, database isolation, transaction rollback, cancellation, timeout, panic recovery, malformed outputs, bounded queues, and reverse shutdown.

I/O tests must include traversal, symlink escape, file URI authority/query/fragment rejection, redirect and proxy rejection, DNS rebinding resistance, private-address rejection, host allowlists, oversized and slow responses, streaming memory bounds, and redacted audit/errors.

Criterion benchmarks cover cold package startup, canonical and mixed-case dispatch, representative scalar calls, bounded traversal by visited-node count, streamed JSON by bytes, and Heimdall hook enqueue/saturation. Package indirection must preserve item 15's O(1) dispatch and add no per-row registry allocation.

## Definition Of Done

Two independently owned packages, `copperdb-apoc` and Heimdall, load through one configured lifecycle and register without core dispatch edits. The representative APOC surface matches current NornicDB through embedded, HTTP, and Bolt execution. Discovery, authorization, and dispatch derive from the same package descriptors; failures cannot expose partial registries or stop unrelated optional packages; I/O is default-deny and bounded; hooks and shutdown are bounded; and workspace tests, warning-denied Clippy, and focused benchmarks pass.