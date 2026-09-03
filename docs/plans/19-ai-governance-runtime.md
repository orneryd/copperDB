# 19: Heimdall, Inference, Link Prediction, And Reranking

Status: complete. Priority: P2. Owners: `heimdall`, `inference`, `linkpredict`, `search`, `engine`.

## Objective

Add deterministic read-only reranking and governed link/inference suggestions before any opt-in automatic graph mutation.

Item 17 owns the package loader and representative Heimdall action/hook verification. This item consumes that proven lifecycle and expands Heimdall behavior; it does not define a second plugin system.

## Product Safety Boundary

Defaults remain disabled. Reranking may reorder visible candidates but cannot add hidden entities. Link prediction and inference produce durable suggestions with evidence/provenance; they do not mutate the graph automatically until review, audit, policy, and kill-switch controls are complete.

## Phases

1. **Complete:** add a read-only `Reranker` after policy-filtered hydration: identity, deterministic MMR, and optional local scoring with bounded candidates/documents, timeout/error fallback, preserved original scores/ranks, and strict visible-membership enforcement.
2. **Complete:** build bounded, cancellable graph snapshots from storage node streams and maintained outgoing adjacency indexes; implement normalized Common Neighbors, Jaccard, Adamic-Adar, Preferential Attachment, Resource Allocation, and deterministic single/ensemble topology plus semantic candidate scoring without self/existing edges.
3. **Complete:** add bounded, cancellable similarity-on-store, co-access, temporal proximity, and transitive signals with configured canonical embedding identity and maintained vector-index search.
4. **Complete:** persist aggregate-window evidence, cooldown, reproducible provenance, model/policy versions, Heimdall review, and authenticated human decisions. Decisions and materialization are audited, idempotent, race-safe, and reject stale evidence.
5. **Complete:** add immutable provider registries, bounded lossless dispatch, timeout/retry/restart recovery, notifications, configurable Heimdall injection with fail-closed default, and atomic materialization behind per-database opt-in plus overriding global kill switches.

## Tests And Benchmarks

Deterministic ties, thresholds, no self/existing edge, evidence accumulation, cooldown, duplicate approval, provider failure/timeout, prompt-injection resistance, RBAC-filtered context, cancellation, restart recovery, and no default mutation. Benchmark graph build/update, candidate generation by degree, reranking sizes, and queue saturation.

Phase 1 validation covers identity/pass-through, deterministic MMR ties and diversity, local scoring bounds, provider failure/timeout, cancellation, redacted context, unknown/duplicate provider IDs, omission fallback, and score/rank provenance. Criterion measures identity and MMR at 10, 50, and 100 visible candidates.

Phase 2 validation covers directed/undirected snapshots, node/edge bounds, cancellation, unknown endpoints, production storage adjacency streaming, all five upstream-normalized topology formulas, deterministic ties and top-K, no self/existing edges, semantic blending, thresholds, and five-algorithm ensembles. Criterion measures streamed 1k/10k-node graph builds and candidate generation at degree 4/16/64.

Phases 3-5 validation covers bounded signal inputs, canonical result identity, aggregate expiry, durable recovery, provider timeout/failure/membership, parent cancellation, malicious relationship overrides, authenticated distinct model/human gates, global/default-off switches, transactional hash-chain audit rollback, concurrent decision/materialization convergence, maintained-index production callbacks, configured provider approval, and no graph mutation before every gate passes.

## Definition Of Done

Every suggestion records reproducible inputs/provenance, reranking cannot broaden membership, default operation never mutates graph state, and optional materialization passes review/policy/audit gates.