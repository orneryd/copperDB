use std::hint::black_box;

use copperdb_otel::Telemetry;
use criterion::{Criterion, criterion_group, criterion_main};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use tracing::info_span;
use tracing_subscriber::layer::SubscriberExt as _;

fn benchmark_recording(c: &mut Criterion) {
    let telemetry = Telemetry::new();
    c.bench_function("telemetry_counter_record", |b| {
        b.iter(|| {
            telemetry
                .record_counter(
                    "nornicdb_http_requests_total",
                    &[
                        ("method", "GET"),
                        ("path_template", "/health"),
                        ("status_class", "2xx"),
                    ],
                )
                .unwrap();
        });
    });
}

fn benchmark_request_span(c: &mut Criterion) {
    c.bench_function("request_span_tracing_off", |b| {
        b.iter(|| {
            info_span!(
                "http.request",
                http.request.method = "GET",
                http.route = "/health"
            )
            .in_scope(|| black_box(()));
        });
    });

    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .build();
    let tracer = provider.tracer("copperdb-observability-benchmark");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    tracing::subscriber::with_default(subscriber, || {
        c.bench_function("request_span_always_sampled", |b| {
            b.iter(|| {
                info_span!(
                    "http.request",
                    http.request.method = "GET",
                    http.route = "/health"
                )
                .in_scope(|| black_box(()));
            });
        });
    });
    provider.shutdown().unwrap();
}

criterion_group!(benches, benchmark_recording, benchmark_request_span);
criterion_main!(benches);
