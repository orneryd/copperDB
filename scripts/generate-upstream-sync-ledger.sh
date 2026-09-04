#!/usr/bin/env bash

set -euo pipefail

readonly BASELINE="d9b76ae82334e6b23b847156eb81931781546b85"
readonly TARGET="21b998cb27e9a555f5f83ecd6ad9ab830178d541"
readonly RANGE="${BASELINE}..${TARGET}"
readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly UPSTREAM="${NORNICDB_UPSTREAM:-${HOME}/src/NornicDB}"
readonly LEDGER="${REPO_ROOT}/docs/parity/nornicdb-upstream-sync-2026-09.tsv"

if [[ ! -d "${UPSTREAM}/.git" ]]; then
    echo "NornicDB checkout not found at ${UPSTREAM}" >&2
    exit 1
fi

if [[ "$(git -C "${UPSTREAM}" rev-parse "${TARGET}^{commit}")" != "${TARGET}" ]]; then
    echo "NornicDB target commit ${TARGET} is unavailable" >&2
    exit 1
fi

classify() {
    local path="$1"
    local category owner contract upstream_tests targets disposition prerequisites validation evidence

    case "${path}" in
        pkg/localization/*|scripts/localization_*/*)
            category="localization"
            owner="localization,errors,otel"
            contract="localized messages preserve stable IDs, placeholders, plurals, fallback, and deterministic generated catalogs"
            upstream_tests="pkg/localization/*_test.go;scripts/localization_*"
            targets="crates/localization;crates/errors;crates/otel;scripts/check-localization-catalogs.sh"
            disposition="implemented"
            prerequisites="complete: protocol-neutral locale negotiation and request context"
            validation="bash scripts/check-localization-catalogs.sh;cargo test --workspace"
            evidence="catalog generator check and workspace tests pass"
            ;;
        pkg/config/*|nornicdb.example.yaml)
            category="settings-security"
            owner="config,multidb,security"
            contract="typed settings normalize canonically, persist by database, redact secrets, and apply at the declared activation boundary"
            upstream_tests="pkg/config/**/*_test.go;pkg/server/server_dbconfig_test.go"
            targets="crates/config/src/lib.rs;crates/multidb/src/lib.rs;crates/security/src/lib.rs;crates/server/src/lib.rs"
            disposition="implemented"
            prerequisites="complete: 72-setting registry, durable catalog, active/configured snapshots"
            validation="cargo test -p copperdb-config -p copperdb-multidb -p copperdb-server"
            evidence="31 config, 8 multidb, and 140 server tests pass"
            ;;
        pkg/storage/*)
            category="storage-recovery"
            owner="storage"
            contract="storage mutations, quotas, snapshots, WAL, and recovery remain atomic, bounded, durable, and cancellable"
            upstream_tests="pkg/storage/**/*_test.go"
            targets="crates/storage/src/lib.rs;crates/storage/src/tests.rs;crates/storage/src/async_engine.rs"
            disposition="implemented"
            prerequisites="complete: streaming backend iteration and pre-mutation capacity accounting"
            validation="cargo test -p copperdb-storage;cargo test --workspace"
            evidence="49 storage tests and workspace tests pass"
            ;;
        pkg/search/*)
            category="search-vector"
            owner="search,vectorspace,engine"
            contract="BM25, vector, hybrid, RRF, filtering, reranking, persistence, and cancellation preserve deterministic result contracts"
            upstream_tests="pkg/search/**/*_test.go;pkg/search/**/*_benchmark_test.go"
            targets="crates/search;crates/vectorspace;crates/engine/src/vector_indexes.rs;crates/engine/src/copperdb.rs"
            disposition="implemented"
            prerequisites="complete: maintained indexes, hydrated visibility filtering, local and HTTP rerankers"
            validation="cargo test -p copperdb-search -p copperdb-vectorspace -p copperdb-engine;cargo test --workspace --benches --no-run"
            evidence="21 search, 132 engine, workspace tests, and benchmark compilation pass"
            ;;
        pkg/cypher/*)
            category="query"
            owner="cypher,eval,engine"
            contract="Cypher parsing and execution preserve row, aggregate, mutation, SHOW SETTINGS, and retrieval-policy semantics"
            upstream_tests="pkg/cypher/**/*_test.go"
            targets="crates/cypher;crates/eval;crates/engine/src/tests"
            disposition="implemented"
            prerequisites="complete: Rust 2024 parser/evaluator migration and procedure registry"
            validation="cargo test -p copperdb-cypher -p copperdb-eval -p copperdb-engine"
            evidence="workspace tests and warning-denied all-target Clippy pass"
            ;;
        pkg/bolt/*)
            category="protocol-isolation"
            owner="bolt,auth,multidb"
            contract="Bolt authentication, transaction state, database rebinding, admission, rollback failure, and localized diagnostics remain protocol-correct"
            upstream_tests="pkg/bolt/**/*_test.go"
            targets="crates/bolt;crates/auth;crates/txsession;crates/server/src/tests.rs"
            disposition="implemented"
            prerequisites="complete: transaction state machine and database permits"
            validation="cargo test -p copperdb-bolt -p copperdb-txsession -p copperdb-server"
            evidence="workspace tests pass"
            ;;
        pkg/server/*|cmd/nornicdb/*|cmd/nornicdb-admin/*)
            category="server-startup"
            owner="server,admin,auth,security"
            contract="startup, admin, HTTP, headless routing, bootstrap credentials, proxy trust, limits, and search endpoints preserve secure behavior"
            upstream_tests="pkg/server/**/*_test.go;cmd/nornicdb/**/*_test.go;cmd/nornicdb-admin/**/*_test.go"
            targets="crates/server;crates/admin;crates/copperdb;crates/auth;crates/security"
            disposition="implemented"
            prerequisites="complete: active database snapshots and persistent administrator state"
            validation="cargo test -p copperdb-server -p copperdb-admin -p copperdb"
            evidence="140 server tests and workspace tests pass"
            ;;
        pkg/auth/*|pkg/security/*|testing/container_security_test.go)
            category="auth-security"
            owner="auth,security,server"
            contract="authentication, authorization, credential persistence, TLS/CORS validation, and secure cookies fail closed"
            upstream_tests="pkg/auth/**/*_test.go;pkg/security/**/*_test.go;testing/container_security_test.go"
            targets="crates/auth;crates/security;crates/server/src/lib.rs;crates/server/src/tests.rs"
            disposition="implemented"
            prerequisites="complete: durable administrator and trusted-proxy configuration"
            validation="cargo test -p copperdb-auth -p copperdb-security -p copperdb-server"
            evidence="workspace tests pass"
            ;;
        pkg/multidb/*|pkg/nornicdb/*|pkg/inference/*)
            category="database-runtime"
            owner="multidb,engine,inference"
            contract="database lifecycle, isolation, quotas, configured/effective settings, and governed inference remain durable and atomic"
            upstream_tests="pkg/multidb/**/*_test.go;pkg/nornicdb/**/*_test.go;pkg/inference/**/*_test.go"
            targets="crates/multidb;crates/engine;crates/inference;crates/storage"
            disposition="implemented"
            prerequisites="complete: enforcing storage boundary and startup catalog seeding"
            validation="cargo test -p copperdb-multidb -p copperdb-engine -p copperdb-inference"
            evidence="8 multidb, 132 engine, and workspace tests pass"
            ;;
        pkg/graphql/*|pkg/mcp/*|pkg/nornicgrpc/*|pkg/qdrantgrpc/*|pkg/replication/*)
            category="protocol-contract"
            owner="graphql,mcp,nornicgrpc,qdrantgrpc,replication"
            contract="supported protocol boundaries preserve negotiation, authorization, cancellation, localized stable errors, and database routing"
            upstream_tests="matching upstream package *_test.go files in the audited path"
            targets="crates/graphql;crates/mcp;crates/nornicgrpc;crates/qdrantgrpc;crates/replication"
            disposition="implemented"
            prerequisites="complete: shared request context, locale preferences, and auth core"
            validation="cargo test --workspace"
            evidence="workspace tests and all-target compilation pass"
            ;;
        pkg/observability/*|pkg/errors/*)
            category="diagnostics"
            owner="otel,errors"
            contract="errors and telemetry preserve stable IDs, structured fields, locale independence, and bounded tracing overhead"
            upstream_tests="pkg/observability/**/*_test.go;pkg/errors/**/*_test.go"
            targets="crates/otel;crates/errors;crates/server/benches/tracing_overhead.rs"
            disposition="implemented"
            prerequisites="complete: localization catalog and request context"
            validation="cargo test -p copperdb-otel -p copperdb-errors;cargo test --workspace --benches --no-run"
            evidence="workspace tests and benchmark compilation pass"
            ;;
        pkg/heimdall/*)
            category="governance"
            owner="heimdall,inference"
            contract="governed model actions remain authenticated, audited, reviewable, localized, and default-off"
            upstream_tests="pkg/heimdall/**/*_test.go"
            targets="crates/heimdall;crates/inference;crates/engine/src/inference_runtime.rs"
            disposition="implemented"
            prerequisites="complete: package capabilities and durable suggestion lifecycle"
            validation="cargo test -p copperdb-heimdall -p copperdb-inference -p copperdb-engine"
            evidence="workspace tests pass"
            ;;
        pkg/adminimport/*)
            category="admin-import"
            owner="adminimport,admin"
            contract="offline import preserves bounded deterministic conversion and localized stable errors"
            upstream_tests="pkg/adminimport/**/*_test.go"
            targets="crates/adminimport;crates/admin"
            disposition="implemented"
            prerequisites="complete: storage batch and conversion APIs"
            validation="cargo test -p copperdb-adminimport;cargo test --workspace --benches --no-run"
            evidence="workspace tests and benchmark compilation pass"
            ;;
        cmd/recall-bench/*|testing/e2e/*)
            category="benchmark"
            owner="search,engine,benchmarks"
            contract="benchmark fixtures, dimensions, thresholds, timing boundaries, and quality metrics remain reproducible and comparable"
            upstream_tests="audited benchmark or E2E path"
            targets="crates/engine/benches;crates/search/benches;crates/vectorspace/benches;docs/performance-snapshot.md"
            disposition="implemented"
            prerequisites="complete: deterministic search fixtures and maintained indexes"
            validation="cargo test --workspace --benches --no-run"
            evidence="all benchmark targets compile; performance snapshot records measured gates"
            ;;
        .github/workflows/*|docker/*)
            category="deployment-handoff"
            owner="plan-23"
            contract="release, container, and CI assets must preserve secure defaults and the final Plan 21 target matrix"
            upstream_tests="audited workflow or container path"
            targets="docs/plans/23-deployment-ci-release.md"
            disposition="transferred-plan-23"
            prerequisites="blocked by design on Plan 21 supported backend matrix"
            validation="bash scripts/generate-upstream-sync-ledger.sh --check"
            evidence="exact upstream asset path retained in this ledger with Plan 23 ownership"
            ;;
        ui/package.json|ui/package-lock.json|go.mod|go.sum)
            category="dependency"
            owner="ui,workspace"
            contract="applicable dependency upgrades preserve build and runtime behavior without known lockfile drift"
            upstream_tests="upstream dependency manifests and lockfiles"
            targets="Cargo.toml;Cargo.lock;ui/package.json;ui/package-lock.json"
            disposition="implemented"
            prerequisites="complete: Rust 1.95 and React 19 migration"
            validation="scripts/check-rust-advisories.sh;cargo check --workspace --all-targets;npm --prefix ui ci;npm --prefix ui run build"
            evidence="strict Rust advisory gate, workspace compilation, and locked UI production build pass"
            ;;
        docs/*|README.md|CHANGELOG.md)
            category="documentation"
            owner="docs"
            contract="active documentation claims only verified CopperDB behavior and records future-plan ownership explicitly"
            upstream_tests="audited documentation path"
            targets="docs/plans/20-upstream-sync-2026-09.md;docs/IMPLEMENTATION_STATUS.md;README.md"
            disposition="reconciled"
            prerequisites="complete: runtime acceptance evidence"
            validation="bash scripts/generate-upstream-sync-ledger.sh --check"
            evidence="Plan 20 status and this file-level disposition ledger record verified behavior"
            ;;
        .gitignore|.agents/*)
            category="repository"
            owner="workspace"
            contract="repository metadata and generated/runtime exclusions do not hide required source, instructions, or audit artifacts"
            upstream_tests="upstream repository metadata audited directly"
            targets=".gitignore;.github/copilot-instructions.md"
            disposition="covered-no-change"
            prerequisites="none"
            validation="git status --short"
            evidence="required Plan 20 instructions, ledger, and scripts remain visible to git"
            ;;
        *)
            category="uncategorized"
            owner="unassigned"
            contract="manual analysis required"
            upstream_tests="unclassified"
            targets="unassigned"
            disposition="incomplete"
            prerequisites="manual audit required"
            validation="none"
            evidence="none"
            ;;
    esac

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' \
        "${category}" "${owner}" "${contract}" "${upstream_tests}" "${targets}" \
        "${disposition}" "${prerequisites}" "${validation}" "${evidence}"
}

generate() {
    printf 'change_status\tupstream_path\tcommits\tcategory\tcopper_owner\tcontract\tupstream_tests\tcopper_targets\tdisposition\tprerequisites\tvalidation\tevidence\n'
    while IFS=$'\t' read -r status source_path target_path; do
        local commits classification
        local path="${source_path}"
        if [[ "${status}" == R* || "${status}" == C* ]]; then
            path="${target_path}"
            commits="$(git -C "${UPSTREAM}" log --format='%h' --reverse "${RANGE}" -- "${source_path}" "${target_path}" | paste -sd, -)"
        else
            commits="$(git -C "${UPSTREAM}" log --format='%h' --reverse "${RANGE}" -- "${path}" | paste -sd, -)"
        fi
        classification="$(classify "${path}")"
        printf '%s\t%s\t%s\t%s\n' "${status}" "${path}" "${commits}" "${classification}"
    done < <(git -C "${UPSTREAM}" diff --name-status "${RANGE}")

    while IFS= read -r path; do
        local commits classification
        commits="$(git -C "${UPSTREAM}" log --format='%h' --reverse "${RANGE}" -- "${path}" | paste -sd, -)"
        classification="$(classify "${path}")"
        printf 'TRANSIENT\t%s\t%s\t%s\n' "${path}" "${commits}" "${classification}"
    done < <(
        comm -23 \
            <(git -C "${UPSTREAM}" log --format= --name-only "${RANGE}" | sed '/^$/d' | sort -u) \
            <(git -C "${UPSTREAM}" diff --name-only "${RANGE}" | sort -u)
    )
}

if [[ "${1:-}" == "--check" ]]; then
    temporary="$(mktemp)"
    trap 'rm -f "${temporary}"' EXIT
    generate > "${temporary}"
    diff -u "${LEDGER}" "${temporary}"

    expected_paths="$(git -C "${UPSTREAM}" diff --name-only "${RANGE}" | wc -l | tr -d ' ')"
    actual_paths="$(tail -n +2 "${LEDGER}" | grep -vc $'^TRANSIENT\t' || true)"
    if [[ "${actual_paths}" != "${expected_paths}" ]]; then
        echo "ledger path count ${actual_paths} does not match upstream ${expected_paths}" >&2
        exit 1
    fi

    expected_commits="$(git -C "${UPSTREAM}" rev-list --count "${RANGE}")"
    actual_commits="$(tail -n +2 "${LEDGER}" | cut -f3 | tr ',' '\n' | sort -u | wc -l | tr -d ' ')"
    if [[ "${actual_commits}" != "${expected_commits}" ]]; then
        echo "ledger commit count ${actual_commits} does not match upstream ${expected_commits}" >&2
        exit 1
    fi

    if grep -q $'\tuncategorized\t\|\tincomplete\t' "${LEDGER}"; then
        echo "ledger contains incomplete paths" >&2
        exit 1
    fi
else
    generate > "${LEDGER}"
fi