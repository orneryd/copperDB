# 20: Upstream Sync, September 2026

Status: planned. Priority: P1/P2. Owners: `config`, `multidb`, `storage`, `search`, `vectorspace`, `cypher`, `eval`, `engine`, `server`, `bolt`, `auth`, `security`, `inference`, `admin`, `otel`.

## Objective

Port every applicable behavior and regression introduced on NornicDB `main` after the Plan 19 baseline, preserving CopperDB's stricter Rust safety, deterministic behavior, bounded work, and default-off automatic mutation policy.

GPU/SIMD acceleration and GraphQL feature completeness remain deferred to Plans 21 and 22. Their shared or prerequisite behavior affected by this range remains in Plan 20, and neither later plan begins until every Plan 20 requirement is complete.

Nothing in the audited range is excluded merely because the upstream implementation, test, generator, build, or release asset is written in Go. Each change must be evaluated for an equivalent Rust, UI, CI, packaging, documentation, dependency, or operational obligation. If applying an upstream change touches a CopperDB area whose prerequisite implementation is missing or partial, this plan must complete that prerequisite and its tests rather than marking the delta deferred. Only unrelated product surfaces with no CopperDB analogue may receive a no-code disposition, supported by repository evidence and an explicit future trigger.

## Audit Baseline

- Upstream range: `d9b76ae82334e6b23b847156eb81931781546b85..21b998cb27e9a555f5f83ecd6ad9ab830178d541`.
- Scope: 44 commits, 619 changed files, 54,292 additions, and 5,893 deletions.
- Audit method: inspect production diffs, regressions, benchmarks, generated assets, dependencies, build/release changes, and follow-up fixes; map each change to its Rust, UI, CI, packaging, documentation, dependency, or operational analogue before deciding its action.
- Completion requires a checked file-level disposition for all 619 changed upstream files and all 44 commits. A commit or file may be marked port required, already covered with test evidence, superseded by a stricter behaviorally equivalent Rust implementation, generated from an audited source, or no-code with a concrete justification. Labels such as Go-only, test-only, documentation-only, generated, dependency-only, or deferred are not by themselves sufficient justification.

## Phase 0: Auditable Change Ledger

Before implementation, add a machine-readable ledger under `docs/parity/` containing every changed upstream path and commit in the baseline range. Each entry records:

- upstream commit and path, change category, externally observable contract, and relevant upstream tests;
- CopperDB owner, target files, current implementation evidence, required action, and focused validation;
- prerequisite gaps exposed in the affected area, including older upstream contracts that must be completed to make the new change correct;
- source schema and generator for generated artifacts;
- for a no-code disposition, the repository evidence proving CopperDB has no analogous runtime/build/release surface and the concrete condition that would invalidate that decision.

CI must validate ledger completeness against `git diff --name-only` for the recorded range. Plan 20 cannot complete while any entry is unclassified, justified only by implementation language, or assigned to a future plan despite being required by an affected current surface.

## Phase 1: Localization And Stable Diagnostics

Upstream commits: `0c744e16`, `33981a3b`, `e3cc9f88`, `4e93fd8b`, `4d748cf1`, `e12c3796`, `c6fa8012`, `04943b38`, `96bdce44`, `2cc50d89`, `506d5331`, `7b2360a9`, `575ae88a`, `471a6c66`, `0d5f4ed0`, `a2982e4d`, `c26eb3c5`.

Port the protocol-neutral localization contract:

- Normalize BCP 47 and POSIX locale identifiers with precedence `COPPERDB_LANGUAGE`, configured language, OS locale, then `en-US`; preserve upstream `auto` behavior.
- Support ordered request-context preferences, embedded `en-US`, `es-ES`, and `en-XA`, plural forms, source-English fallback, and bounded missing-pack/catalog warnings.
- Preserve stable locale-independent error codes, causes, telemetry event IDs, and structured fields while localizing presentation at HTTP, CLI/admin, Bolt, MCP, Cypher, storage, auth, security, inference, search, and server boundaries.
- Add deterministic catalog and procedure-metadata generation, manifest/inventory generation, duplicate/missing ID, placeholder, plural-form, and pseudo-locale validation. Regeneration must leave the worktree clean.
- Propagate ordered preferences through `Accept-Language`, Bolt metadata, request context, and gRPC metadata. Every currently supported boundary must be localized; no untranslated fallback may escape when a supported catalog entry exists.
- Add catalogs and contract tests for distributed gRPC/Qdrant/replication adapters even where their production runtime remains unsupported. Runtime deferral does not defer shared message, negotiation, or stable-error contracts.
- Complete partial protocol behavior needed to reach or preserve newly localized errors. In particular, Bolt localization includes the state-machine transitions and failure paths that emit those messages rather than adding unreachable catalog entries.

Upstream anchors: `pkg/localization`, `scripts/localization_catalog`, `scripts/localization_inventory`, package-specific `messages_*`, `localized_errors`, and `log_events` files. CopperDB owners: `crates/errors`, `crates/otel`, protocol crates, and a new protocol-neutral localization crate.

## Phase 2: Typed Database Settings And Live Application

Upstream commits: `81cb38fa`, `412b562e`, `67fe6e2c`, `da1aca14`, `7505f8cb`.

- Build one canonical typed setting registry with type, category, scope, default, valid values, zero semantics, redaction, dynamic/restart classification, and optional hot applicator.
- Normalize accepted environment alternatives before persistence; reject unknown keys and deprecated aliases; preserve defaults, process configuration, explicit process/CLI values, and canonical database override precedence.
- Implement `SHOW SETTINGS` selection plus admin configured/effective values, secret redaction, and `pendingRestart`.
- Apply dynamic cache, search, embedding, and reranker changes atomically. Keep restart-required storage mode, transaction memory, vector storage, index budget, and related settings isolated until reopen.
- Resolve query result cache capacity and TTL per database and prove that one database cannot alter another database's runtime.

Upstream anchors: `pkg/config/dbconfig/{keys,resolver,settings,store}.go`, Cypher `SHOW SETTINGS`, and `pkg/server/server_dbconfig.go`. CopperDB owners: `crates/config`, `crates/multidb`, `crates/cache`, `crates/engine`, `crates/cypher`, and `crates/server`.

## Phase 3: Database Isolation, Capacity, Security, And Startup

Upstream commits: `81cb38fa`, `1d7287ff`, `67fe6e2c`, `4b66f84f`, `21b998cb`, and `2feb8dc4`.

- Enforce request-selected database RBAC and canonical per-database execution limits.
- Implement atomic Bolt database permit acquire, release, and rebind. A failed rebind must retain the previous binding and permit.
- Reconcile persisted storage-byte totals before quota enforcement at startup.
- Route inference-created relationships through the same quota-enforcing database storage boundary and propagate capacity failures without partial suggestion/audit state.
- Apply security validation in every environment: auth-enabled startup rejects empty credentials, wildcard CORS, public plaintext HTTP/gRPC, and public Bolt without required TLS. Explicit no-auth remains allowed and emits a stable warning event.
- Permit default credentials only when bootstrapping a missing administrator; never overwrite a changed durable password.
- Add an exact restart regression proving default bootstrap credentials never overwrite a changed durable password.
- Mark session cookies `Secure` only for direct TLS or `X-Forwarded-Proto: https` received through an explicitly trusted proxy configuration. Ignore untrusted forwarded headers.
- In headless mode remove browser UI and GraphQL Playground GET routes while retaining discovery, health, GraphQL POST API, and admin APIs. Implement this routing correction now; Plan 22 still owns GraphQL feature completeness.
- Complete any partial Bolt admission, authentication, state-machine, or database-binding behavior required by the newly affected limits and localization paths, including rollback failure: the connection becomes defunct and no buffered result is flushed.

## Phase 4: Streaming Snapshots, WAL, And Recovery

Upstream commits: `81cb38fa`, `67fe6e2c`, `7505f8cb`, with recovery integration in `e5c1615b`.

- Add bounded framed node/edge snapshots with magic/version, sequence metadata, configurable record limits, CRC/footer validation, cancellation, atomic replacement, and directory synchronization.
- Reject storage backends that cannot provide streaming iteration rather than silently buffering an unbounded graph.
- Never materialize the whole graph before encoding; replace the current collecting snapshot path with bounded streaming iteration before claiming snapshot parity.
- Write a recoverable snapshot before automatic WAL truncation.
- Stream snapshot records and ordered WAL entries into recovery destinations in bounded chunks.
- Preserve legacy snapshot compatibility and WAL-only recovery.

Validation covers large node/edge round trips, corruption, cancellation, visitor errors, unsupported backends, legacy input, WAL-only and snapshot-plus-WAL recovery, compaction ordering, fsync, interruption, and reopen.

## Phase 5: Search And Vector Runtime

Upstream commits: BM25 `34b97e08`, `90921403`, `df576353`, `22a27165`; parallel RRF `ce7f8b56`; adaptive overfetch `0d4336a7`, `87f18d06`; fixed-stride vectors `16b6c8fb`; retrieval policy `e5c1615b`.

- Preserve exact-term BM25 by default and make prefix expansion explicit. Match Unicode canonical equivalence and language-neutral tokenization for Latin, Greek, Cyrillic, Arabic, Devanagari, and CJK text.
- Keep optimized and exhaustive BM25 scores identical, including deterministic score/ID ties, bounded sparse/dense scratch selection, concurrent cold queries, and configured property projection.
- Run BM25 and vector retrieval concurrently, always joining both branches before returning errors or cancellation. Preserve IDs, scores, metrics, method labels, filtering, RRF, MMR, reranking, and fallback behavior.
- Cancellation returns the cancellation error and no partial results, including when one retrieval branch has already completed.
- Apply adaptive widening after decay/type/property filtering and chunk-to-node deduplication. Grow geometrically to source exhaustion or configured cap and expose retry/raw-candidate metrics.
- Port fixed-stride file-backed vectors: ordinal-only vector payload, separate IDs/metadata, discarded unsaved tails, reclaiming compaction, and non-global-lock add/get/sync paths.
- Apply the upstream architecture-safe `1e-6` persisted-vector tolerance in correctness tests now; Plan 21 owns acceleration, not this storage compatibility contract.
- Implement deterministic `db.retrieve` and `db.rretrieve` policy controls for camel/snake-case `rrfK`, branch weights, minimum RRF score, fallback, candidate targets, adaptive bounds, and scalar/list property filters. Invalid values retain documented defaults; filtering occurs before top-k.

Criterion gates compare BM25 sparse/dense paths, fixed-stride random reads and compaction, adaptive underfill, and parallel hybrid crossover against both current CopperDB and upstream benchmarks.

## Phase 6: Cypher, Embedding, And Retrieval Regressions

Upstream commits: `0d4336a7` and `e5c1615b`.

- Distinguish `count(*)`, `count(variable)`, `count(property)`, and `count(DISTINCT variable)`; require a self-loop when relationship endpoint variables repeat.
- Keep optimized `UNWIND ... MATCH elementId(...) ... CREATE` on the batch path with exact mutation statistics.
- Treat zero embedding workers as a disabled pool and negative/invalid values as the documented fallback of one worker.
- Port end-to-end `db.retrieve` and `db.rretrieve` policy parsing, filtering, defaults, and result-shape regressions alongside Phase 5's runtime work.

LiteLLM Heimdall, rerank-provider restoration, Bolt transaction lifecycle, and the August binding/`WITH ... WHERE` fixes predate this audit baseline. Retain their existing CopperDB coverage and plans, but do not count them as work discovered in this range.

## Phase 7: Operations, Dependencies, And Deployment Handoff

- Audit every dependency change in `c6008985` against Cargo and the UI dependency graph, including OpenTelemetry, tonic/prost, cryptography, YAML/configuration, KMS, storage, React/router/PostCSS, and `@ornery/ui-grid`. Upgrade applicable equivalents, port migration work, run advisory checks, and test behavior; a different ecosystem is not a reason to skip risk analysis.
- Apply `e520625a`: update Browserslist to `4.28.8` or a newer patched compatible release, regenerate the lockfile, and build/test the UI. This supersedes the intermediate `f9e67c2c` version and also owns the Phase 5 architecture-safe persisted-vector tolerance.
- Reconcile benchmark configuration, environment precedence, corpus counts, and reproducibility guidance from `16203d3a` and `16596bc6` with CopperDB tooling. Publish results only after apples-to-apples CopperDB measurements; rename-only upstream benchmark symbols require no mechanical port.
- Use `18097f09` as an acceptance inventory and update CopperDB status/changelog claims only when the corresponding runtime behavior passes its completion gate.
- Audit test repairs in `17dee036` as behavioral evidence. Port cancellation-with-no-results and Bolt rollback/defunct-connection regressions; classify remaining expectation-only edits individually in the ledger.
- Define local dependency/advisory and generated-artifact drift commands in Plan 20. Plan 23 ports them into pinned, least-privilege GitHub Actions after Plan 21 finalizes the build matrix.
- Record every deployment and container obligation from `81cb38fa`, `c6008985`, `286c6840`, `f9e67c2c`, and `e520625a` in the Plan 20 ledger with Plan 23 as the implementation owner. The audit currently finds no Dockerfile, Compose file, or workflow under `.github/workflows`; this is not a no-code disposition. Plan 23 must port the full upstream delivery system after Plan 21 and revalidate the upstream asset inventory.
- The deletion in `23a6e856` requires no product code only if the ledger verifies CopperDB has no tracked equivalent agent instruction. Preserve any generally applicable localization rules in this plan rather than importing upstream tool-specific state.

## Validation

Each phase must first port the upstream regression tests as deterministic Rust tests, then pass affected dependent crates. Before completion run:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test --workspace --benches --no-run
npm --prefix ui ci
npm --prefix ui run build
```

Also run Rust and UI dependency advisory checks, localization/catalog generator clean-tree checks, ledger coverage validation, headless route tests, trusted/untrusted proxy cookie tests, and durable bootstrap restart tests. Performance-motivated divergence is allowed only with identical public behavior and recorded apples-to-apples benchmark evidence. GraphQL feature expansion remains deferred to Plan 22, but all shared routing, localization, error, authorization, settings, and limit behavior affected by this range is mandatory in Plan 20. Plan 23 owns container and release-policy validation after Plan 21.

## Definition Of Done

Every upstream file and commit in the baseline range has a checked, evidence-backed disposition; every affected CopperDB runtime area has the prerequisite implementation needed to exercise the new contract; every production, test, generated, dependency, UI, and documentation obligation is completed and validated; deployment/build/release obligations are exhaustively transferred to Plan 23 with exact asset ownership because they depend on Plan 21's final target matrix; each true no-code decision proves the absence of an analogous owned surface and names its reopening trigger; security, storage recovery, and database-isolation behavior fail closed; performance changes meet measured Rust gates; active documentation reflects only verified runtime behavior; and the final implementation is re-audited against upstream `main` before Plan 21 begins.