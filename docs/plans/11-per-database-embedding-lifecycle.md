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
- Open: provider construction, bounded background workers, full readiness states, retry/backoff/dead-letter handling, cancellation, shutdown, and event-driven re-embedding.

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