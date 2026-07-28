# 01: Authentication Defaults And Precedence

Status: complete. Priority: P0. Owners: `config`, `copperdb`, `server`, `engine`, `bolt`, `nornicgrpc`.

## Objective

Make authentication enabled by default and derive one authoritative runtime decision from defaults, file, environment, CLI, and the explicit `--no-auth` startup override.

## Evidence And References

- Copper: `ConfigOverrides`, `AuthConfig`, `load_with_precedence_from`, and `apply_overrides` in `crates/config/src/lib.rs`; `Cli` and `resolve_startup_config` in `crates/copperdb/src/main.rs`; `AuthState` in `crates/server/src/lib.rs`; `DatabaseConfig::auth_enabled` in `crates/engine/src/lib.rs`.
- Original defect: server and engine independently defaulted authentication to false instead of consuming resolved runtime configuration.
- Upstream: NornicDB commits `1837c8c7` and `20215f13`, `pkg/config/config.go`, config tests, defaults consumer tests, and `cmd/nornicdb/main.go`.

## Required Contract

Precedence is `built-in defaults < config file < environment < --no-auth`. `auth.enabled` defaults true. `--no-auth` is the sole command-line bypass and disables all supported ingress consistently. Omitted and explicitly false values must remain distinguishable during deserialization.

## Implementation Phases

1. Completed: `AuthConfig.enabled` defaults true. Canonical configuration names are `auth.enabled` and `COPPERDB_AUTH_ENABLED`.
2. Completed: `--no-auth` applies after normal resolution and logs an explicit authentication-disabled warning; normal startup logs the resolved Boolean without secrets.
3. Completed: `AuthState`, HTTP, gRPC, engine construction, and Bolt session construction consume the resolved Boolean. Bolt credential validation and role propagation remain owned by Plan 02.
4. Completed: enabled startup validates non-empty JWT key material; `--no-auth` is visible and intentional.
5. Completed: [README.md](../../README.md) documents the default, precedence, canonical environment variable, and explicit no-auth migration path.

## Test Matrix

- Omitted setting defaults enabled.
- File false and env true follow precedence.
- Explicit file false remains false when no higher layer exists.
- `--no-auth` overrides every lower source.
- HTTP, Bolt, and internal service behavior agree for the same config.
- Invalid auth-enabled startup fails clearly; secrets never appear in output.

Focused validation passed: `cargo test -p copperdb-config`, `cargo test -p copperdb-bolt --lib`, `cargo test -p copperdb-server auth`, `cargo test -p copperdb`, and `cargo test -p copperdb-engine`.

An isolated workspace run reached the server crate after the embedding-cache capacity repair. It remains red on two unrelated server tests: browser discovery sees embedded UI fallback instead of the test's expected JSON, and distributed graph-read routing reports an unavailable quorum. Neither failure exercises this plan's authentication contract.

## Risks And Rollout

Default-on can lock out unattended development deployments. Provide an actionable startup error and explicit local `--no-auth`, but no silent fallback. Resolve authenticators once per runtime, not per request.

## Definition Of Done

- One resolved boolean reaches every ingress and engine configuration.
- Protected routes reject anonymous requests by default.
- Only `--no-auth` bypasses all authentication.
- Precedence and migration behavior are documented and regression-tested.

Completion record (2026-07-28): all definition-of-done criteria are satisfied by the focused configuration, executable, Bolt, server-auth, and engine suites. The unrelated workspace failures noted above remain outside this plan. Plan 02 extends Bolt from this gate to credential validation, authenticated principals, roles, and transaction semantics.