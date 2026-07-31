# 11: Per-Database Embedding Lifecycle

Status: planned. Priority: P1. Owners: `config`, `multidb`, `storage`, `embed`, `localllm`, `engine`, `lifecycle`.

## Objective

Compose existing GGUF, cache, typed embedding, and pending-queue pieces into bounded per-database runtimes with truthful readiness and restart recovery.

## Runtime Contract

Each database reports `Disabled`, `Cold`, `Warming`, `Ready`, `Degraded`, `Failed`, or `Stopping`, plus provider/model/dimensions, actual backend, worker counts, pending/completed/failed totals, queue age, and last sanitized error. Automatic embedding remains disabled by default.

## Configuration

Resolve enabled, `startup|lazy` warming, model, dimensions, cache capacity, worker count, batch size, GPU layers, retry/backoff, and queue limits through effective per-database config. CLI remains a global kill switch for automatic work.

## Progress

- Complete: local GGUF required-symbol resolution now returns a typed loader error instead of panicking. Provider stats report the loader's CPU/GPU outcome and the latest embedding activity timestamp; warmup observes that shared timestamp rather than a startup snapshot.
- Complete: engine-owned `EmbeddingRuntime` is created per database and is `Disabled` by default with zero workers. It reports `Disabled`, `Cold`, `Ready`, or `Degraded` status and provides an explicit one-item drain path for injected providers. Successful inference runs outside storage locks, writes typed managed embeddings durably, and clears the pending entry; failures retain pending work for recovery.
- Complete: `local_gguf` is the explicitly supported configured provider. It is loaded at runtime through the shared `Embedder` interface and status reports the actual backend. Unsupported providers, missing model paths, and loader/model failures enter `Failed` without a substitute provider or worker.
- Complete: effective per-database `COPPERDB_EMBEDDING_WORKERS` is bounded to at least one and defaults to one only after embedding is explicitly enabled. Ready runtimes start that many claim-aware workers; disabled and failed runtimes start none. Each worker preserves pending work until its typed write succeeds, retries failed work after a short pause, and joins during runtime teardown.
- Complete: per-database retry policy is configurable through `COPPERDB_EMBEDDING_MAX_ATTEMPTS` and `COPPERDB_EMBEDDING_RETRY_BACKOFF_MS`. Failure attempts are durable across restarts. Exhausted work moves to a durable dead-letter record, is excluded from automatic workers, appears in runtime status, and can only be retried through explicit pending-queue re-enqueueing.
- Complete: `COPPERDB_EMBEDDING_SHUTDOWN_TIMEOUT_MS` bounds runtime teardown. Workers observe shutdown cooperatively between calls; completed workers join, while workers still inside an uninterruptible model call detach after the deadline so database teardown does not block indefinitely.
- Complete: explicit `request_node_reembedding` queues a durable forced managed re-embedding operation. It preserves external named vectors, clears only CopperDB-managed chunk embeddings, survives queue refresh, and clears the force marker only after a successful typed embedding write.
- Complete: `COPPERDB_EMBEDDING_WARMING=startup|lazy` controls provider loading. Startup retains eager model initialization; lazy databases report `Cold`, start workers without loading a model, and transition through `Warming` only when a drain first needs the provider. Provider-load failures transition to `Failed` with a sanitized error.
- Complete: `COPPERDB_EMBEDDING_WARMUP_INTERVAL_MS` is an opt-in, per-database periodic warmup interval for the local GGUF provider. Its default of zero disables periodic work; configured loops honor the requested cadence, skip recently used models, and terminate when the provider closes.
- Complete: `cancel_node_embedding` removes only an unclaimed durable pending request and its forced re-embedding marker. It returns false for absent or worker-claimed work, preserving in-flight inference; later source updates or explicit re-embedding may queue new work.
- Complete: storage compares labels and properties, the canonical embedding input, on committed node writes. Changed sources clear only managed chunk embeddings and queue forced re-embedding without disturbing external named vectors. After an enabled runtime has successfully loaded its provider, it performs one generation reconciliation pass that requeues managed embeddings whose durable configured model generation or nonzero requested dimensions differ.
- Open: queue age, cache-ratio, batch-latency, and model-load-duration metrics.

## Phases

1. Complete item 6; convert dynamic symbol failures to typed errors; report actual CPU/GPU fallback and current activity timestamps.
2. Add engine-owned `EmbeddingRuntime` per database. Bounded workers claim durable queue entries, build canonical input, perform blocking inference outside storage locks, and persist typed embedding updates.
3. Implement startup/lazy model loading and index warming, retry/backoff/dead-letter behavior, cancellation, and bounded shutdown.
4. Connect committed node events and explicit re-embedding controls; handle model/dimension generation changes idempotently.

## Tests And Metrics

Test disabled default, two-database isolation, queue recovery, bounded concurrency, retries, model mismatch, CPU fallback, cancellation, shutdown, and restart idempotence. Record queue depth/age, throughput, latency, cache ratio, retries, active workers, model-load duration, and actual backend.

## Risks

llama calls are blocking and cannot be force-aborted; use bounded blocking workers plus cooperative checks between calls. Never hold storage locks during inference or log model inputs by default.

## Definition Of Done

Enabled databases recover and drain only their own queues, disabled databases start no workers, restart is idempotent, shutdown is bounded, and status reflects actual provider/backend/readiness.