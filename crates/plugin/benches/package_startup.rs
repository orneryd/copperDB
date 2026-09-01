use std::sync::Arc;
use std::time::Duration;

use copperdb_plugin::{
    DatabaseEvent, DatabaseEventRuntime, DatabaseEventType, PackageDefinition, PackageDescriptor,
    PackageFactory, PackageRuntime, PackageSpec, StaticPackageFactory, EVENT_INGRESS_CAPACITY,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use semver::Version;

const PACKAGE_ID: &str = "benchmark.static";

fn benchmark_package_startup(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime must build");
    let definition = PackageDefinition::new(PackageDescriptor::new(
        PACKAGE_ID,
        Version::new(1, 0, 0),
        "copperdb benchmarks",
    ));
    let factory = Arc::new(StaticPackageFactory::new(definition)) as Arc<dyn PackageFactory>;

    criterion.bench_function("package_cold_start/static", |bencher| {
        bencher.iter(|| {
            runtime.block_on(async {
                let package_runtime = PackageRuntime::start(
                    [Arc::clone(black_box(&factory))],
                    [PackageSpec::new(PACKAGE_ID)],
                    Duration::from_secs(1),
                )
                .await
                .expect("benchmark package must start");
                package_runtime
                    .shutdown()
                    .await
                    .expect("benchmark package must stop");
            });
        });
    });
}

fn benchmark_event_enqueue(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime must build");
    let events = {
        let _guard = runtime.enter();
        DatabaseEventRuntime::start(&[], Duration::from_secs(1))
    };

    criterion.bench_function("package_event/enqueue", |bencher| {
        bencher.iter(|| {
            assert!(events.emit(black_box(DatabaseEvent::new(
                DatabaseEventType::QueryExecuted,
            ))));
            runtime.block_on(tokio::task::yield_now());
        });
    });

    runtime.block_on(events.shutdown());
}

fn benchmark_event_saturation(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime must build");
    let events = {
        let _guard = runtime.enter();
        DatabaseEventRuntime::start(&[], Duration::from_secs(1))
    };
    for _ in 0..EVENT_INGRESS_CAPACITY {
        assert!(events.emit(DatabaseEvent::new(DatabaseEventType::QueryExecuted)));
    }

    criterion.bench_function("package_event/reject_saturated_ingress", |bencher| {
        bencher.iter(|| {
            assert!(!events.emit(black_box(DatabaseEvent::new(
                DatabaseEventType::QueryExecuted,
            ))));
        });
    });
    assert!(events.ingress_dropped() > 0);

    runtime.block_on(events.shutdown());
}

criterion_group!(
    benches,
    benchmark_package_startup,
    benchmark_event_enqueue,
    benchmark_event_saturation
);
criterion_main!(benches);
