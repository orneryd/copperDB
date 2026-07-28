# Storage V0 Implementation Plan (Copper)

> Historical storage phase plan. Current storage findings and next steps are consolidated in [../../COPPERDB_NORNICDB_PARITY_PLAN.md](../../COPPERDB_NORNICDB_PARITY_PLAN.md).

## Objective
Implement a Rust-native storage architecture baseline aligned to current NornicDB storage concepts while locking Copper storage to layout version **0** and excluding migration baggage.

Status note: the supported runtime architecture is single-node only. Any topology or mesh metadata mentioned here is durable vocabulary/backlog for future work, not a statement that distributed execution is currently supported.

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
- [x] Add WAL wrapper + durability primitives
- [ ] Add async write-behind engine and flush reconciliation
- [x] Add schema/constraint manager parity
- [x] Add streaming and prefix-delete behaviors

## Phase 3 (next)
- [ ] Add MVCC visibility/head model and lifecycle controls
- [ ] Add temporal indexing and maintenance APIs
- [ ] Add embedding/index rebuild workflows
- [ ] After single-node GA, evaluate whether to wire any future distributed query routing into `search`, `fabric`, and `replication`

## Codebase update notes/stubs
- `crates/search`: any future distributed fan-out must be treated as deferred work after the single-node architecture is complete.
- `crates/fabric`: any future topology publication remains deferred.
- `crates/replication`: any future distributed integration remains deferred; current storage work should stay single-node-owned.
- `crates/multidb`: use namespace count/list APIs and prefix deletion hooks.
- `crates/server`/`graphql`: do not expose mesh-peer lifecycle as current capability; any such admin surface is future work only.

## Validation notes
- Keep tests deterministic: sorted outputs, fixed timestamps where needed.
- Prefer deep state assertions: raw record + index + metadata checks per operation.
