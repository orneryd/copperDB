use std::env;

use copperdb_vectorspace::{HnswConfig, HnswIndex, VectorSpace};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

const DEFAULT_VECTOR_COUNT: usize = 10_000;
const K: usize = 10;
const DIMENSIONS: [usize; 3] = [128, 384, 1_024];

struct Workload {
    dimensions: usize,
    vector_count: usize,
    hnsw: HnswIndex,
    exact: VectorSpace,
    queries: Vec<Vec<f32>>,
}

impl Workload {
    fn new(dimensions: usize, vector_count: usize) -> Self {
        let mut hnsw = HnswIndex::new(dimensions, HnswConfig::default())
            .expect("benchmark HNSW configuration must be valid");
        let mut exact = VectorSpace::new("exact", dimensions);
        for position in 0..vector_count {
            let id = format!("vector-{position:08}");
            let vector = deterministic_vector(position, dimensions);
            hnsw.insert(id.clone(), vector.clone())
                .expect("benchmark vector dimensions must match");
            exact
                .insert(id, vector)
                .expect("benchmark vector dimensions must match");
        }
        let queries = (0..16)
            .map(|position| deterministic_vector(position * 97 + 11, dimensions))
            .collect();
        Self {
            dimensions,
            vector_count,
            hnsw,
            exact,
            queries,
        }
    }

    fn print_calibration(&self) {
        let mut recall_sum = 0.0_f64;
        let mut visited_sum = 0_usize;
        for query in &self.queries {
            let (approximate, stats) = self
                .hnsw
                .knn(query, K)
                .expect("benchmark query dimensions must match");
            let exact = self
                .exact
                .knn(query, K)
                .expect("benchmark query dimensions must match");
            let exact_ids = exact
                .into_iter()
                .map(|(id, _)| id)
                .collect::<std::collections::BTreeSet<_>>();
            let matches = approximate
                .iter()
                .filter(|(id, _)| exact_ids.contains(id))
                .count();
            recall_sum += matches as f64 / K as f64;
            visited_sum += stats.visited_nodes;
        }
        let query_count = self.queries.len() as f64;
        let vector_bytes = self.vector_count * self.dimensions * std::mem::size_of::<f32>();
        eprintln!(
            "hnsw calibration: vectors={}, dimensions={}, recall@{K}={:.4}, average_visited_nodes={:.1}, vector_bytes={vector_bytes}",
            self.vector_count,
            self.dimensions,
            recall_sum / query_count,
            visited_sum as f64 / query_count,
        );
    }
}

fn configured_vector_count() -> usize {
    env::var("COPPERDB_HNSW_BENCH_VECTORS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_VECTOR_COUNT)
}

fn configured_dimensions() -> Vec<usize> {
    env::var("COPPERDB_HNSW_BENCH_DIMENSIONS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|dimension| dimension.trim().parse().ok())
                .filter(|dimension| *dimension > 0)
                .collect()
        })
        .filter(|dimensions: &Vec<usize>| !dimensions.is_empty())
        .unwrap_or_else(|| DIMENSIONS.to_vec())
}

fn deterministic_vector(seed: usize, dimensions: usize) -> Vec<f32> {
    let mut state = (seed as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    (0..dimensions)
        .map(|_| {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            let bits = state.wrapping_mul(0x2545_f491_4f6c_dd1d);
            (bits as u32 as f32 / u32::MAX as f32) * 2.0 - 1.0
        })
        .collect()
}

fn bench_hnsw(criterion: &mut Criterion) {
    let vector_count = configured_vector_count();
    for dimensions in configured_dimensions() {
        let workload = Workload::new(dimensions, vector_count);
        workload.print_calibration();

        let mut hnsw_group = criterion.benchmark_group("hnsw_query");
        hnsw_group.throughput(Throughput::Elements(1));
        hnsw_group.bench_function(
            BenchmarkId::new("vectors", format!("{vector_count}-d{dimensions}")),
            |bench| {
                let mut query_index = 0;
                bench.iter(|| {
                    let query = &workload.queries[query_index % workload.queries.len()];
                    query_index += 1;
                    black_box(
                        workload
                            .hnsw
                            .knn(black_box(query), K)
                            .expect("benchmark query dimensions must match"),
                    );
                });
            },
        );
        hnsw_group.finish();

        let mut exact_group = criterion.benchmark_group("exact_cosine_query");
        exact_group.throughput(Throughput::Elements(1));
        exact_group.bench_function(
            BenchmarkId::new("vectors", format!("{vector_count}-d{dimensions}")),
            |bench| {
                let mut query_index = 0;
                bench.iter(|| {
                    let query = &workload.queries[query_index % workload.queries.len()];
                    query_index += 1;
                    black_box(
                        workload
                            .exact
                            .knn(black_box(query), K)
                            .expect("benchmark query dimensions must match"),
                    );
                });
            },
        );
        exact_group.finish();
    }
}

criterion_group!(benches, bench_hnsw);
criterion_main!(benches);
