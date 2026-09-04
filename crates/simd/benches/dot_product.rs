use copperdb_simd::dot_f32;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

const DIMENSIONS: [usize; 8] = [128, 256, 384, 512, 768, 1_024, 1_536, 3_072];

fn bench_dot_product(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("dot_product");
    for dimensions in DIMENSIONS {
        let left = (0..dimensions)
            .map(|index| index as f32 * 0.001)
            .collect::<Vec<_>>();
        let right = (0..dimensions)
            .map(|index| (dimensions - index) as f32 * 0.001)
            .collect::<Vec<_>>();
        group.throughput(Throughput::Bytes(
            (dimensions * 2 * std::mem::size_of::<f32>()) as u64,
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(dimensions),
            &dimensions,
            |bench, _| {
                bench.iter(|| {
                    black_box(
                        dot_f32(black_box(&left), black_box(&right))
                            .expect("benchmark vectors have equal dimensions"),
                    );
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_dot_product);
criterion_main!(benches);
