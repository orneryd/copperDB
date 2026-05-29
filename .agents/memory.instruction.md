---
applyTo: '**'
---

# Coding Preferences
- Keep parity-tracking docs current as transport and auth slices land.
- Prefer focused deterministic cargo tests scoped to the touched crate or behavior.
- Keep automatic indexing or search work disabled by default; schema-declared indexes remain authoritative per database.

# Project Architecture
- Remove anything considered legacy. it is vestigial since this is a new project that is not released yet. Any plan changes to architecture are considered refactor to clean architecture that are not backwards compatible until a 1.0 release
- copperDB is a Rust Cargo workspace mirroring NornicDB ../NornicDB package structure under `crates/`.
- Distributed transport currently uses `crates/nornicgrpc` with tonic, unified-auth admin JWT validation for internal replica apply/read when security is enabled, `--no-auth` bypass for those internal RPCs, and caller-token forwarding on ranked-search, hydration, and distributed graph-read RPCs.
- Config and multidb own per-database effective config resolution; engine and server consume that for ranked-search gating.

# Solutions Repository
- Internal gRPC auth is split by method: replica apply/read validate admin JWTs from the unified auth core via `authorization`, while ranked-search, hydration, and distributed graph-read RPCs use forwarded caller auth in `x-copperdb-caller-authorization` so remote nodes can reapply per-database auth without changing protobuf schemas.
- No transport component should auto-load a legacy shared gRPC token from config or env; replica/internal auth is validator-based on the server side, and outbound callers must attach bearer credentials explicitly when that runtime path is implemented.
- Server-owned Neo4j-compatible `tx/commit` writes now build a real outbound tonic replica transport from topology peers and mint a short-lived admin cluster JWT when security is enabled; routed read execution now builds the real graph-read gRPC transport and forwards the caller token while clustered access-metadata side effects continue through the admin-authenticated replica channel. Do not fabricate remote peers with `InMemoryReplicaTransport` and `MemoryStorage` in runtime code.
- Server protocol handlers must not drive distributed tonic work through a blocking bridge on the request runtime thread. For current-thread Tokio test contexts, offload Neo4j `tx/commit` distributed execution onto `spawn_blocking` and let the blocking worker run its own short-lived runtime for the routed transport call.
- For server-level distributed graph-read regressions, prefer real tonic peers backed by `LocalEngineReplicaHandler` over bespoke stubs. If the test is only about routing and not auth forwarding, disable peer auth explicitly; otherwise supply a real forwarded caller token.
- Startup gRPC cert hardening currently includes certificate validity-window rejection plus cert-or-key pair consistency validation in config before listener bind.