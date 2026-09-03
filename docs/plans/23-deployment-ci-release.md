# 23: Deployment, CI, And Release Distribution

Status: planned. Priority: P1. Owners: `release`, `security`, `server`, `gpu`, `embed`, `localllm`, `buildinfo`, `ui`, `docs`.

## Sequencing

Begin after Plan 21 establishes the supported scalar, SIMD, and GPU backend matrix. Plan 20 must already have completed its runtime, security, localization, settings, dependency, and generated-artifact contracts. Plan 22 is not a prerequisite: Plan 23 may execute immediately after Plan 21, and release descriptions must continue to identify GraphQL completeness as deferred until Plan 22 finishes.

This plan is the explicit implementation owner for deployment, CI, installer, packaging, and release-publishing obligations identified by Plan 20. That handoff exists because final workflows and artifacts must exercise the build targets produced by Plan 21; it is sequencing, not a no-code disposition.

## Objective

Port NornicDB's complete GitHub Actions, Docker/Compose, macOS product, installer, documentation deployment, and release publication structure to CopperDB. Preserve its target-specific behavior and memory-safe test splitting while replacing all NornicDB branding, defaults, paths, identifiers, environment variables, repositories, artifacts, and release metadata with verified CopperDB equivalents.

Do not mechanically copy insecure or Go-specific implementation details. Reproduce their contract using pinned Rust toolchains, deterministic Cargo builds, least-privilege jobs, signed artifacts, SBOMs, provenance, and target-native validation.

Audit baseline: NornicDB `main` at `21b998cb27e9a555f5f83ecd6ad9ab830178d541`. Before implementation, refresh upstream and disposition every deployment, workflow, Docker, Compose, macOS, package-manager, and publishing change since that commit.

## Upstream Asset Inventory

Port and track every current upstream owner file:

- Workflows: `.github/workflows/{ci,cd,cd-llama-cpu,cd-llama-cuda,docs-pages,release-macos}.yml`.
- Repository automation: `.github/dependabot.yml` and `.github/CODEOWNERS`.
- Containers: root `docker-compose.yml`, `docker-compose.amd64.yml`, `docker-compose.arm64.yml`; all 11 `docker/Dockerfile.*` variants; Docker-local CUDA, Vulkan, ARM64 Metal, and ARM64 Metal Heimdall Compose files; entrypoint, fallback wrapper, and documentation.
- macOS: the entire `macos/` tree, including Swift menu-bar application, Apple ML embedding server, file indexer, settings and first-run UI, tests, Swift package manifest/lockfile, icons, LaunchAgents, configuration, installer lifecycle scripts, package/disk-image builder, and user/release documentation.
- Release helpers: `scripts/build-homebrew-artifacts.sh`, `scripts/build-llama.sh`, `scripts/build-llama-cuda.ps1`, `scripts/build-llama-windows.sh`, `scripts/hydrate-llama-cpu-libs.sh`, the UI build/embed flow, `mkdocs.yml`, `requirements-docs.txt`, and version/build metadata sources.

Maintain a checked upstream-to-CopperDB asset ledger. No workflow, script, image, Compose service, installer resource, generated asset, or publishing behavior may disappear because it is Go, Swift, shell, YAML, or documentation.

## Phase 1: Complete GitHub Actions Port

Create pinned, least-privilege CopperDB counterparts:

1. `ci.yml`: Rust 1.95, Cargo dependency/build caching, Node 20, `npm ci`, UI production build before Rust embedding, formatting, warning-denied Clippy, sharded tests, merged coverage, generated-file drift, dependency advisories, and release-script linting.
2. `cd.yml`: immutable release metadata, general and accelerated image matrices, strict tag sanitization, `vX.Y.Z` validation, versioned tags, conditional `latest`, registry variables instead of personal namespaces, and publication only from the checked source SHA.
3. `cd-llama-cpu.yml`: multi-architecture `copperdb-llama-cpu-libs`, with the llama revision read from one authoritative tracked source.
4. `cd-llama-cuda.yml`: amd64 CUDA prerequisite image, enabled only after Plan 21's CUDA/runtime gate passes.
5. `docs-pages.yml`: strict documentation build, Pages artifact/deploy jobs, job-scoped permissions, and branch-aware concurrency cancellation.
6. `release-macos.yml`: signed/notarized pkg and dmg variants, arm64 and Intel archives, checksums, GitHub Release publication, and CopperDB Homebrew tap dispatch.

Port Dependabot coverage for Cargo, npm, GitHub Actions, and Docker. Add CODEOWNERS for release-critical paths. Pin every third-party action to an immutable commit SHA, grant permissions per job, use protected release environments, prevent untrusted-fork secrets from reaching builds, and key publication concurrency by tag.

## Phase 2: Deterministic Test Sharding And OOM Control

Do not run the full workspace test suite as one memory-unbounded CI process. Define named, independently scheduled groups:

- `foundation`: utility, configuration, auth/security, observability, and small support crates;
- `query`: Cypher, filter, eval, and indexing;
- `storage`: storage, txsession, multidb, retention, WAL, snapshot, and recovery;
- `protocol`: server, Bolt, GraphQL, MCP, nornicgrpc, and qdrantgrpc;
- `search-ai`: search, vectorspace, embedding, inference, Heimdall, local LLM, and link prediction;
- `distributed`: topology, replication, and fabric;
- dedicated storage and server heavy-test partitions plus deterministic catch-all partitions.

Use `cargo-nextest` partitions or prebuilt test binaries with a checked manifest, `CARGO_BUILD_JOBS=2` by default in constrained runners, bounded test threads, per-partition timeouts, fail-fast disabled across independent jobs, and explicit retry policy only for proven environmental flakes. Catch-all partitions must ensure newly added tests cannot silently miss CI.

Collect coverage per immutable partition, merge profiles once, and fail the build on test or coverage-generation failure. Add `.config/nextest.toml` and scripts that can reproduce every CI partition locally. Record peak RSS and duration so shard boundaries are evidence-based and revised before approaching runner limits.

## Phase 3: Docker Build And Runtime Matrix

Port all upstream image roles with CopperDB names and verified backend claims:

- CPU: amd64 CPU, multi-arch CPU+BGE, model-bundled, and headless variants.
- Accelerated: amd64 CUDA, CUDA+BGE, CUDA+Heimdall, amd64 Vulkan, Vulkan+BGE, Vulkan+Heimdall, and headless variants.
- ARM64: CPU and any Plan 21-proven accelerated variants; do not label Linux ARM64 as Metal-capable unless an actual supported backend and runtime test prove that claim.
- Prerequisites: multi-arch llama CPU libraries, amd64 llama CUDA libraries, and a Windows CUDA library build retained as experimental until a Windows runtime job validates it.

Create `docker/`, `.dockerignore`, root and backend-specific Compose files, entrypoint/fallback scripts, and operator documentation. Replace `nornicdb-*` with `copperdb-*`, `/app/nornicdb` with `/app/copperdb`, and `NORNICDB_*` with canonical `COPPERDB_*`; preserve old environment names only where Plan 20 explicitly defines compatibility aliases. Keep `/data` as the portable volume.

Build the UI before compiling server assets. Add a real compile-time headless feature that omits UI payloads without placeholder files while retaining health, discovery, admin APIs, and GraphQL POST behavior. Copy and verify all target-specific llama and accelerator shared-library dependencies. Use repository/registry variables rather than personal account names.

## Phase 4: Container Security And Supply Chain

- Run as a fixed non-root UID/GID with only `/data`, model cache, and required temporary paths writable.
- Keep authentication enabled by default. Require explicit `COPPERDB_NO_AUTH=true` for no-auth mode and emit Plan 20's stable warning event.
- Set read-only root filesystems where supported, drop all capabilities, enable no-new-privileges, bound memory/PIDs, and provide secrets-file examples rather than plaintext credentials.
- Pin base images by digest; verify downloaded models and llama sources by checksum.
- Generate CycloneDX or SPDX SBOMs for binaries and images, publish BuildKit provenance, sign image digests with keyless Cosign/OIDC, attach GitHub attestations, and fail configured critical-vulnerability scans.
- Validate `/health`, ports 7474/7687/9091 when enabled, restart persistence, auth defaults, headless APIs, configuration passthrough, accelerated execution, device loss, and transparent CPU fallback.

## Phase 5: Complete macOS Product Port

Copy the entire upstream macOS implementation as the behavioral starting point, then rebrand and adapt every source, asset, identifier, default, test, script, and document:

- `NornicDB.app` becomes `CopperDB.app`; executable names become `CopperDB`/`copperdb`.
- `com.nornicdb.*` becomes a publisher-approved `com.copperdb.*` bundle and LaunchAgent namespace.
- `~/.nornicdb`, `/usr/local/var/nornicdb`, and `/usr/local/share/nornicdb` become CopperDB paths with tested migration and upgrade preservation.
- `NORNICDB_*` becomes canonical `COPPERDB_*`, retaining only explicitly supported compatibility aliases.
- Product names, icons, menu labels, help links, logs, process matching, package identifiers, artifact names, file associations, and accessibility labels must contain no accidental NornicDB branding.

Port the Swift menu-bar application, Apple ML embedding service, file indexer/browser, settings UI, first-run flow, Package.swift/Package.resolved, unit/integration tests, app/icon assets, LaunchAgents, default configuration, install/preinstall/postinstall/uninstall scripts, and all macOS documentation.

Generated configuration must use the canonical Plan 20 settings registry. Generate credentials and secrets through Keychain; never install `admin/password`. Do not overwrite user configuration or data on upgrade. Do not start the server until first-run authentication, encryption, storage, and model choices are complete. Uninstall must distinguish binaries/services from retained user data and require explicit confirmation for destructive removal.

## Phase 6: macOS Packaging, Signing, And Notarization

Port the installer builder with lite/full pkg and dmg variants and the Homebrew archive builder for both Apple Silicon and Intel. Produce:

- `CopperDB-<version>-<arch>-lite.pkg` and `CopperDB-<version>-<arch>-full.pkg`;
- matching dmg images;
- `copperdb-darwin-arm64.tar.gz` and `copperdb-darwin-amd64.tar.gz`;
- one sorted `SHA256SUMS` covering every published asset.

Codesign the server binary, bundled libraries, plugins, helper tools, and app with hardened runtime. Sign packages with Developer ID Installer; submit pkg, dmg, and Homebrew binaries with `notarytool`; staple tickets where supported; verify with `codesign`, `pkgutil`, `spctl`, and `stapler`. Keep certificates, App Store Connect credentials, Keychain material, and Homebrew tokens confined to protected environments and ephemeral keychains.

Test lite/full content manifests, fresh install, upgrade, uninstall with retained data, first run, LaunchAgent load/unload, Intel execution, Apple Silicon execution, Gatekeeper offline verification, and a clean macOS VM with no developer tools installed.

## Phase 7: Version, Artifact, And Publishing Contract

Use the root `Cargo.toml` workspace package version as release authority. Extend `crates/buildinfo` through build-script/environment inputs for source commit, UTC build time, target triple, profile, feature/backend set, and dirty-state rejection. CI must not mutate tracked version files.

Require release-tag/version equality, immutable source checkout, `SOURCE_DATE_EPOCH`, deterministic archive ordering/ownership/timestamps, and reproducible checksum generation. Publish `latest` only for an automatic stable SemVer tag. Every binary, image manifest, pkg, dmg, archive, SBOM, checksum, signature, attestation, and release note must identify the same source SHA and version.

Create or update the GitHub Release only after all required target jobs succeed. Upload assets without overwriting a conflicting digest. Dispatch the CopperDB Homebrew tap with tag, repository, asset URLs, and checksums. A failed signing, notarization, scan, attestation, checksum, or downstream dispatch step must leave the release visibly incomplete and must never promote `latest`.

Remove release-facing remnants such as `nornicdb-ui`, `nornicdb_token`, NornicDB artifact names, and obsolete Go embed files under `ui/`. Retain protocol or metric names only where compatibility requires them and document those exceptions in the asset ledger.

## Phase 8: Linux And Windows Release Targets

Publish tested Rust binaries for Linux amd64/arm64 and Windows amd64 with matching checksums, SBOMs, signatures, provenance, startup smoke tests, and CPU feature floors. Validate GNU/musl or document the chosen libc contract; validate Windows runtime DLL discovery and service-free foreground startup.

Upstream does not currently implement deb, rpm, MSI, winget, Chocolatey, or native Linux/Windows release workflows. Do not claim parity for those package managers. Record each as an unimplemented distribution surface with an owner and trigger for a future plan. Keep Windows llama/CUDA output experimental until a Windows GPU runner proves load, inference, fallback, and shutdown behavior.

## Validation Matrix

Required local and CI gates include:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test --workspace --benches --no-run
npm --prefix ui ci
npm --prefix ui run build
```

Also require cargo-nextest shard-manifest coverage, merged coverage validation, `cargo audit` or equivalent advisory policy, npm audit policy, actionlint, ShellCheck, Swift tests, Dockerfile/Compose linting, generated-file clean-tree checks, image scans, SBOM/provenance/signature verification, and release dry runs that cannot publish.

The target matrix must test Linux amd64/arm64 CPU, macOS arm64/Intel CPU, Windows amd64 CPU, and every Plan 21 backend on hardware that actually supports it. Cross-compilation proves compilation only; publication requires target-native startup and smoke tests. Record peak memory for each test shard and image build.

## Definition Of Done

- All six upstream workflows, repository automation files, 11 Dockerfiles, seven Compose definitions, macOS files, installer scripts, and release helpers have checked CopperDB dispositions and implemented counterparts where CopperDB owns the surface.
- CI deterministically covers every test without a monolithic OOM-prone job; catch-all shards prove new tests cannot disappear; coverage is merged and blocking.
- Every published image runs non-root, persists data across restart, passes health/auth/headless tests, and reports only a backend verified on that target.
- CopperDB macOS arm64 and Intel artifacts pass signing, notarization, clean-machine installation, first-run setup, upgrade preservation, uninstall, and checksum verification with complete CopperDB branding.
- Linux, Windows, macOS, container, Homebrew, documentation, and GitHub Release assets share one source/version contract and have verified checksums, SBOMs, signatures, and provenance.
- A final audit against then-current NornicDB `main` finds no unclassified deployment, CI, packaging, installer, or publishing asset.