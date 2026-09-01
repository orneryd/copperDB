use std::sync::Arc;

use axum::{body::Body, http::Request};
use copperdb_server::{build_router, AppState};
use criterion::{criterion_group, criterion_main, Criterion};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use tower::ServiceExt as _;
use tracing_subscriber::layer::SubscriberExt as _;

fn benchmark_health_request(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let router = build_router(Arc::new(AppState::default()));

    c.bench_function("health_request_tracing_off", |b| {
        b.iter(|| {
            runtime.block_on(
                router
                    .clone()
                    .oneshot(Request::get("/health").body(Body::empty()).unwrap()),
            )
        });
    });

    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            0.01,
        ))))
        .build();
    let tracer = provider.tracer("copperdb-server-benchmark");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    tracing::subscriber::with_default(subscriber, || {
        c.bench_function("health_request_default_sampling", |b| {
            b.iter(|| {
                runtime.block_on(
                    router
                        .clone()
                        .oneshot(Request::get("/health").body(Body::empty()).unwrap()),
                )
            });
        });
    });
    provider.shutdown().unwrap();
}

criterion_group!(benches, benchmark_health_request);
criterion_main!(benches);
