# 16: Observability Exporters And Runtime Tracing

Status: planned. Priority: P2. Owners: `otel`, `copperdb`, `server`, and instrumented runtime crates.

## Objective

Turn the existing catalog/in-memory telemetry foundation into production OpenMetrics and OTLP runtime observability with real ownership, bounded overhead, redaction, propagation, readiness, and ordered shutdown.

## Current Evidence

`copperdb-otel` has configuration, metric catalogs, validation, health, redaction, and recovery helpers. The executable uses a formatting subscriber and seeds synthetic zero metrics. There is no OpenTelemetry SDK/OTLP exporter or OpenMetrics encoder, and most runtime areas do not own live instruments.

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

## Definition Of Done

Advertised metrics have real owners, collector failure does not prevent startup, secrets never escape, propagation works across supported local protocols, and shutdown flushes within a bound.