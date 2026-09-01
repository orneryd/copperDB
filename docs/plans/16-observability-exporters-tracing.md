# 16: Observability Exporters And Runtime Tracing

Status: complete. Priority: P2. Owners: `otel`, `copperdb`, `server`, and instrumented runtime crates.

## Objective

Turn the existing catalog/in-memory telemetry foundation into production OpenMetrics and OTLP runtime observability with real ownership, bounded overhead, redaction, propagation, readiness, and ordered shutdown.

## Completion Evidence

`copperdb-otel` now provides noop, test, and production providers; deterministic Prometheus/OpenMetrics encoding; fixed-memory histograms; OTLP gRPC and HTTP/protobuf tracing; parent-aware sampling; W3C propagation; redaction; bounded labels; and idempotent bounded shutdown. The private telemetry listener owns `/metrics`, `/livez`, `/readyz`, and `/version`. HTTP, Bolt, engine, storage, search, embedding, transaction, and lifecycle boundaries own live instruments and spans. Production no longer seeds synthetic catalog values; unsupported families remain absent.

## Scope And Non-Goals

Implement single-node HTTP/Bolt/engine/storage/search/embedding/lifecycle observability. Distributed peer/cardinality metrics remain deferred. Telemetry must not expose query text, record IDs, user IDs, tokens, raw paths, model inputs, or unbounded labels.

## Phases

1. Add provider abstraction with noop/test and production implementations; typed instruments retain catalog validation.
2. Add private-by-default OpenMetrics listener and `/metrics`, `/livez`, `/readyz`, `/version` with negotiation, limits, and timeouts.
3. Add OTLP traces over gRPC/HTTP, bounded initialization, parent-aware sampling, W3C propagation, batching/drop self-metrics, and flush-on-shutdown.
4. Instrument ownership boundaries: ingress, parse/execute, transactions/conflicts, storage, fulltext/vector stages, embedding queue/model, and lifecycle.
5. Remove production zero-seeding. Unsupported metrics remain absent rather than fabricated.

## Tests And Benchmarks

Golden OpenMetrics, negotiation, disabled behavior, bind failure, collector outage/noop fallback, sampling, propagation, redaction, baggage allowlist, cardinality rejection, and bounded shutdown. Benchmark instrument recording and request throughput with tracing off and sampled; target documented default overhead under 2% where practical.

Criterion measured validated counter recording at approximately `712 ns`, a disabled request span at approximately `616 ps`, and an always-sampled request span at approximately `878 ns`. The end-to-end `/health` router measured approximately `6.10 us` with tracing off and `7.40 us` with the opt-in 1% sampler. The under-2% request target is therefore met by the default configuration because tracing is disabled by default, but not by this deliberately minimal handler when tracing is enabled; the benchmark remains a regression and optimization guardrail.

Final validation passed `104` server tests, `213` storage tests, `119` engine tests, `73` Bolt tests, `25` transaction tests, `21` telemetry tests, and `4` executable tests. `cargo clippy --workspace --all-targets --all-features -- -D warnings` and `git diff --check` also passed.

## Definition Of Done

Advertised metrics have real owners, collector failure does not prevent startup, secrets never escape, propagation works across supported local protocols, and shutdown flushes within a bound.