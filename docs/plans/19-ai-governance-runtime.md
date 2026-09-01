# 19: Heimdall, Inference, Link Prediction, And Reranking

Status: planned. Priority: P2. Owners: `heimdall`, `inference`, `linkpredict`, `search`, `engine`.

## Objective

Add deterministic read-only reranking and governed link/inference suggestions before any opt-in automatic graph mutation.

Item 17 owns the package loader and representative Heimdall action/hook verification. This item consumes that proven lifecycle and expands Heimdall behavior; it does not define a second plugin system.

## Product Safety Boundary

Defaults remain disabled. Reranking may reorder visible candidates but cannot add hidden entities. Link prediction and inference produce durable suggestions with evidence/provenance; they do not mutate the graph automatically until review, audit, policy, and kill-switch controls are complete.

## Phases

1. Add a read-only `Reranker` after policy-filtered hydration: identity, MMR, then optional local-LLM scoring with timeout/error fallback and preserved original scores.
2. Build graph snapshots/streams from adjacency APIs and implement resource allocation plus topology/semantic candidate scoring without self/existing edges.
3. Add bounded inference signals: similarity-on-store, co-access, temporal proximity, and transitive candidates with canonical embedding identity.
4. Persist suggestions, evidence, cooldown, provenance, model/version, policy, and Heimdall review state. Approval/rejection is authenticated, audited, idempotent, and race-safe.
5. Add bounded scheduler/provider registries and notifications. Automatic materialization is last, per-database opt-in, and globally killable.

## Tests And Benchmarks

Deterministic ties, thresholds, no self/existing edge, evidence accumulation, cooldown, duplicate approval, provider failure/timeout, prompt-injection resistance, RBAC-filtered context, cancellation, restart recovery, and no default mutation. Benchmark graph build/update, candidate generation by degree, reranking sizes, and queue saturation.

## Definition Of Done

Every suggestion records reproducible inputs/provenance, reranking cannot broaden membership, default operation never mutates graph state, and optional materialization passes review/policy/audit gates.