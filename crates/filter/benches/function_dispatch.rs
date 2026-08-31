use copperdb_filter::FunctionRegistry;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn legacy_match_lookup(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "abs"
            | "avg"
            | "coalesce"
            | "count"
            | "datetime"
            | "elementid"
            | "id"
            | "size"
            | "substring"
            | "tolower"
            | "toupper"
    )
}

fn benchmark_function_dispatch(criterion: &mut Criterion) {
    let registry = FunctionRegistry::builtins();
    let mut group = criterion.benchmark_group("function_dispatch");

    group.bench_function("legacy_match_canonical", |bencher| {
        bencher.iter(|| legacy_match_lookup(black_box("toupper")));
    });
    group.bench_function("registry_canonical", |bencher| {
        bencher.iter(|| registry.get(black_box("toupper")).is_some());
    });
    group.bench_function("legacy_match_mixed_case", |bencher| {
        bencher.iter(|| legacy_match_lookup(black_box("toUpper")));
    });
    group.bench_function("registry_mixed_case", |bencher| {
        bencher.iter(|| registry.get(black_box("toUpper")).is_some());
    });

    group.finish();
}

criterion_group!(benches, benchmark_function_dispatch);
criterion_main!(benches);
