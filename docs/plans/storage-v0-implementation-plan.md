# Storage V0 Implementation Plan (Copper)

## Objective
Implement a Rust-native storage architecture baseline aligned to current NornicDB storage concepts while locking copper storage to layout version **0** and excluding migration baggage.

## Phase 1 (initial baseline in this change)
- [x] Add layout manifest with fixed version `0`
- [x] Enforce layout version checks at open-time
- [x] Add deterministic record models (`NodeRecord`, `EdgeRecord`) with stable serialization
- [x] Maintain secondary indexes for labels and edge types
- [x] Add namespace-aware counts/listing based on ID prefixes
- [x] Add metadata registries for distributed search mesh + hyperscaler profiles
- [x] Preserve compatibility API used by existing crates (`put_node/get_node/put_edge/get_edge/...`)

## Phase 2 (next)
- [ ] Add transaction API model parity
- [ ] Add WAL wrapper + durability primitives
- [ ] Add async write-behind engine and flush reconciliation
- [ ] Add schema/constraint manager parity
- [ ] Add streaming and prefix-delete behaviors

## Phase 3 (next)
- [ ] Add MVCC visibility/head model and lifecycle controls
- [ ] Add temporal indexing and maintenance APIs
- [ ] Add embedding/index rebuild workflows
- [ ] Wire mesh-aware query routing into `search`, `fabric`, and `replication`

## Codebase update notes/stubs
- `crates/search`: consume `storage` mesh peer/profile registry for distributed query fan-out.
- `crates/fabric`: publish cluster topology updates to storage mesh metadata.
- `crates/replication`: align WAL integration points and MVCC sequencing.
- `crates/multidb`: use namespace count/list APIs and prefix deletion hooks.
- `crates/server`/`graphql`: expose admin endpoints for mesh peer lifecycle.

## Validation notes
- Keep tests deterministic: sorted outputs, fixed timestamps where needed.
- Prefer deep state assertions: raw record + index + metadata checks per operation.
