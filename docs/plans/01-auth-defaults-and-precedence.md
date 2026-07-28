# 01: Authentication Defaults And Precedence

Status: planned. Priority: P0. Owners: `config`, `copperdb`, `server`, `engine`, `bolt`, `nornicgrpc`.

## Objective

Make authentication enabled by default and derive one authoritative runtime decision from defaults, file, environment, CLI, and the explicit `--no-auth` startup override.

## Evidence And References

- Copper: `ConfigOverrides`, `AuthConfig`, `load_with_precedence_from`, and `apply_overrides` in `crates/config/src/lib.rs`; `Cli` and `resolve_startup_config` in `crates/copperdb/src/main.rs`; `AuthState` in `crates/server/src/lib.rs`; `DatabaseConfig::auth_enabled` in `crates/engine/src/lib.rs`.
- Current defect: server auth separately reads `COPPERDB_SECURITY_ENABLED` with a false default, while engine auth also defaults false.
- Upstream: NornicDB commits `1837c8c7` and `20215f13`, `pkg/config/config.go`, config tests, defaults consumer tests, and `cmd/nornicdb/main.go`.

## Required Contract

Precedence is `built-in defaults < config file < environment < ordinary CLI overrides < --no-auth`. `auth.enabled` defaults true. `--no-auth` is the sole hard bypass and disables all supported ingress consistently. Omitted and explicitly false values must remain distinguishable during deserialization.

## Implementation Phases

1. Add `AuthConfig.enabled: bool` with default true and `ConfigOverrides.auth_enabled: Option<bool>`. Add canonical config/env names and a warning-only compatibility alias for `COPPERDB_SECURITY_ENABLED` if retained.
2. Add `--no-auth` to the executable. Apply it after normal config resolution and expose the final value in startup diagnostics without secrets.
3. Remove direct environment reads from `AuthState`. Construct server, engine, Bolt, and gRPC auth adapters from the resolved config.
4. Validate startup: auth-enabled mode requires usable bootstrap credentials/key material; no-auth mode must be explicit and visibly logged.
5. Document migration for deployments that previously relied on anonymous-by-default startup.

## Test Matrix

- Omitted setting defaults enabled.
- File false, env true, and ordinary CLI values follow precedence.
- Explicit file false remains false when no higher layer exists.
- `--no-auth` overrides every lower source.
- HTTP, Bolt, and internal service behavior agree for the same config.
- Invalid auth-enabled startup fails clearly; secrets never appear in output.

Run `cargo test -p copperdb-config`, `cargo test -p copperdb-server auth`, and executable startup tests.

## Risks And Rollout

Default-on can lock out unattended development deployments. Provide an actionable startup error and explicit local `--no-auth`, but no silent fallback. Resolve authenticators once per runtime, not per request.

## Definition Of Done

- One resolved boolean reaches every ingress and engine configuration.
- Protected routes reject anonymous requests by default.
- Only `--no-auth` bypasses all authentication.
- Precedence and migration behavior are documented and regression-tested.