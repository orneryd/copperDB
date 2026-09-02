# 19: Heimdall, Inference, Link Prediction, And Reranking

Status: in progress (Phase 1 complete). Priority: P2. Owners: `heimdall`, `inference`, `linkpredict`, `search`, `engine`.

## Objective

Add deterministic read-only reranking and governed link/inference suggestions before any opt-in automatic graph mutation.

Item 17 owns the package loader and representative Heimdall action/hook verification. This item consumes that proven lifecycle and expands Heimdall behavior; it does not define a second plugin system.

## Product Safety Boundary

Defaults remain disabled. Reranking may reorder visible candidates but cannot add hidden entities. Link prediction and inference produce durable suggestions with evidence/provenance; they do not mutate the graph automatically until review, audit, policy, and kill-switch controls are complete.

## Phases

1. **Complete:** add a read-only `Reranker` after policy-filtered hydration: identity, deterministic MMR, and optional local scoring with bounded candidates/documents, timeout/error fallback, preserved original scores/ranks, and strict visible-membership enforcement.
2. Build graph snapshots/streams from adjacency APIs and implement resource allocation plus topology/semantic candidate scoring without self/existing edges.
3. Add bounded inference signals: similarity-on-store, co-access, temporal proximity, and transitive candidates with canonical embedding identity.
4. Persist suggestions, evidence, cooldown, provenance, model/version, policy, and Heimdall review state. Approval/rejection is authenticated, audited, idempotent, and race-safe.
5. Add bounded scheduler/provider registries and notifications. Automatic materialization is last, per-database opt-in, and globally killable.

## Tests And Benchmarks

Deterministic ties, thresholds, no self/existing edge, evidence accumulation, cooldown, duplicate approval, provider failure/timeout, prompt-injection resistance, RBAC-filtered context, cancellation, restart recovery, and no default mutation. Benchmark graph build/update, candidate generation by degree, reranking sizes, and queue saturation.

Phase 1 validation covers identity/pass-through, deterministic MMR ties and diversity, local scoring bounds, provider failure/timeout, cancellation, redacted context, unknown/duplicate provider IDs, omission fallback, and score/rank provenance. Criterion measures identity and MMR at 10, 50, and 100 visible candidates.

## Definition Of Done

Every suggestion records reproducible inputs/provenance, reranking cannot broaden membership, default operation never mutates graph state, and optional materialization passes review/policy/audit gates.