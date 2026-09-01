use std::{env, path::PathBuf, time::Instant};

use copperdb_vectorspace::{HnswConfig, HnswIndex, HnswRegistry, VectorFileStore, VectorSpace};
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use tempfile::TempDir;

const DEFAULT_VECTOR_COUNT: usize = 10_000;
const K: usize = 10;
const RERANK_CANDIDATE_MULTIPLIER: usize = 4;
const PIPELINE_RERANK_CANDIDATE_MULTIPLIER: usize = 20;
const DIMENSIONS: [usize; 3] = [128, 384, 1_024];
const NORNICDB_BENCH_EF_CONSTRUCTION: usize = 200;
const NORNICDB_BENCH_EF_SEARCH: usize = 100;
const CANONICAL_HNSW_SEED: u64 = 0x6a09_e667_f3bc_c909;
const CANONICAL_HNSW_MULTIPLIER: u64 = 6_364_136_223_846_793_005;
const CANONICAL_HNSW_INCREMENT: u64 = 1_442_695_040_888_963_407;

struct Workload {
    dimensions: usize,
    vector_count: usize,
    vectors: Vec<(String, Vec<f32>)>,
    hnsw: HnswIndex,
    exact: VectorSpace,
    query: Vec<f32>,
    _artifact_directory: TempDir,
    artifact_path: PathBuf,
    exact_store: VectorFileStore,
}

impl Workload {
    fn new(dimensions: usize, vector_count: usize) -> Self {
        let hnsw_config = configured_hnsw_config();
        let mut fixture = CanonicalHnswFixture::new(CANONICAL_HNSW_SEED);
        let vectors = (0..vector_count)
            .map(|position| (canonical_hnsw_id(position), fixture.vector(dimensions)))
            .collect::<Vec<_>>();
        let mut hnsw = HnswIndex::new(dimensions, hnsw_config)
            .expect("benchmark HNSW configuration must be valid");
        let mut exact = VectorSpace::new("exact", dimensions);
        for (id, vector) in &vectors {
            hnsw.insert(id.clone(), vector.clone())
                .expect("benchmark vector dimensions must match");
            exact
                .insert(id.clone(), vector.clone())
                .expect("benchmark vector dimensions must match");
        }
        let query = fixture.vector(dimensions);
        let artifact_directory =
            tempfile::tempdir().expect("benchmark artifact directory must exist");
        let artifact_path = artifact_directory.path().join("registry.artifact");
        let mut exact_store = VectorFileStore::open(
            artifact_directory.path().join("exact-vectors.bin"),
            dimensions,
        )
        .expect("benchmark vector file store must be created");
        let registry = HnswRegistry::new();
        registry
            .create_index("benchmark", dimensions, hnsw_config)
            .expect("benchmark HNSW configuration must be valid");
        for (id, vector) in &vectors {
            registry
                .upsert("benchmark", id.clone(), vector.clone())
                .expect("benchmark vector dimensions must match");
        }
        exact_store
            .upsert_batch(vectors.iter().cloned())
            .expect("benchmark vector dimensions must match");
        registry
            .save_artifact(&artifact_path)
            .expect("benchmark artifact must be written");
        Self {
            dimensions,
            vector_count,
            vectors,
            hnsw,
            exact,
            query,
            _artifact_directory: artifact_directory,
            artifact_path,
            exact_store,
        }
    }

    fn print_calibration(&self) {
        let mut recall_sum = 0.0_f64;
        let mut rerank_recall_sum = 0.0_f64;
        let mut visited_sum = 0_usize;
        let (approximate, stats) = self
            .hnsw
            .knn(&self.query, K)
            .expect("benchmark query dimensions must match");
        let exact = self
            .exact
            .knn(&self.query, K)
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
        let (candidates, _) = self
            .hnsw
            .knn(
                &self.query,
                K.saturating_mul(PIPELINE_RERANK_CANDIDATE_MULTIPLIER),
            )
            .expect("benchmark query dimensions must match");
        let reranked = self
            .exact_store
            .score_candidates(&self.query, candidates.iter().map(|(id, _)| id.as_str()), K)
            .expect("benchmark candidates must be readable");
        rerank_recall_sum += reranked
            .iter()
            .filter(|(id, _)| exact_ids.contains(id))
            .count() as f64
            / K as f64;
        visited_sum += stats.visited_nodes;
        let query_count = 1.0;
        let vector_bytes = self.vector_count * self.dimensions * std::mem::size_of::<f32>();
        let estimated_memory_bytes = self.hnsw.estimated_memory_bytes();
        eprintln!(
            "hnsw calibration: vectors={}, dimensions={}, recall@{K}={:.4}, pipeline_rerank_recall@{K}={:.4}, average_visited_nodes={:.1}, vector_bytes={vector_bytes}, estimated_memory_bytes={estimated_memory_bytes}",
            self.vector_count,
            self.dimensions,
            recall_sum / query_count,
            rerank_recall_sum / query_count,
            visited_sum as f64 / query_count,
        );
    }

    fn build_hnsw(&self) -> HnswIndex {
        let mut index = HnswIndex::new(self.dimensions, configured_hnsw_config())
            .expect("benchmark HNSW configuration must be valid");
        for (id, vector) in &self.vectors {
            index
                .insert(id.clone(), vector.clone())
                .expect("benchmark vector dimensions must match");
        }
        index
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

fn configured_scale_gate() -> bool {
    env::var("COPPERDB_HNSW_BENCH_SCALE_GATE").is_ok_and(|value| value == "1")
}

fn configured_hnsw_config() -> HnswConfig {
    let mut config = HnswConfig {
        m: 16,
        ef_construction: NORNICDB_BENCH_EF_CONSTRUCTION,
        ef_search: NORNICDB_BENCH_EF_SEARCH,
    };
    if let Some(value) = env::var("COPPERDB_HNSW_BENCH_EF_CONSTRUCTION")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
    {
        config.ef_construction = value;
    }
    if let Some(value) = env::var("COPPERDB_HNSW_BENCH_EF_SEARCH")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
    {
        config.ef_search = value;
    }
    config
}

struct CanonicalHnswFixture {
    state: u64,
}

impl CanonicalHnswFixture {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn vector(&mut self, dimensions: usize) -> Vec<f32> {
        (0..dimensions).map(|_| self.next_f32()).collect()
    }

    fn next_f32(&mut self) -> f32 {
        self.state = self
            .state
            .wrapping_mul(CANONICAL_HNSW_MULTIPLIER)
            .wrapping_add(CANONICAL_HNSW_INCREMENT);
        let mantissa = (self.state >> 40) as u32;
        mantissa as f32 / (1_u32 << 24) as f32
    }
}

fn canonical_hnsw_id(position: usize) -> String {
    char::from_u32(position as u32)
        .expect("benchmark vector position must be a valid Unicode code point")
        .to_string()
}

#[cfg(windows)]
fn process_working_set_bytes() -> Option<usize> {
    use windows_sys::Win32::System::{
        ProcessStatus::{K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS},
        Threading::GetCurrentProcess,
    };

    let mut counters = PROCESS_MEMORY_COUNTERS {
        cb: std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
        PageFaultCount: 0,
        PeakWorkingSetSize: 0,
        WorkingSetSize: 0,
        QuotaPeakPagedPoolUsage: 0,
        QuotaPagedPoolUsage: 0,
        QuotaPeakNonPagedPoolUsage: 0,
        QuotaNonPagedPoolUsage: 0,
        PagefileUsage: 0,
        PeakPagefileUsage: 0,
    };
    // SAFETY: GetCurrentProcess returns a valid pseudo-handle for this process,
    // and counters points to a correctly sized writable structure.
    let succeeded = unsafe {
        K32GetProcessMemoryInfo(
            GetCurrentProcess(),
            &mut counters,
            std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
        )
    };
    (succeeded != 0).then_some(counters.WorkingSetSize)
}

#[cfg(not(windows))]
fn process_working_set_bytes() -> Option<usize> {
    None
}

fn run_scale_gate(dimensions: usize, vector_count: usize) {
    let mut fixture = CanonicalHnswFixture::new(CANONICAL_HNSW_SEED);
    let vectors = (0..vector_count)
        .map(|position| (canonical_hnsw_id(position), fixture.vector(dimensions)))
        .collect::<Vec<_>>();
    let query = fixture.vector(dimensions);
    let mut exact = VectorSpace::new("exact", dimensions);
    let artifact_directory = tempfile::tempdir().expect("benchmark artifact directory must exist");
    let artifact_path = artifact_directory.path().join("registry.artifact");
    let mut exact_store = VectorFileStore::open(
        artifact_directory.path().join("exact-vectors.bin"),
        dimensions,
    )
    .expect("benchmark vector file store must be created");
    for (id, vector) in &vectors {
        exact
            .insert(id.clone(), vector.clone())
            .expect("benchmark vector dimensions must match");
    }
    exact_store
        .upsert_batch(vectors.iter().cloned())
        .expect("benchmark vector dimensions must match");

    let registry = HnswRegistry::new();
    registry
        .create_index("benchmark", dimensions, configured_hnsw_config())
        .expect("benchmark HNSW configuration must be valid");
    let working_set_before = process_working_set_bytes();
    let build_started = Instant::now();
    for (id, vector) in &vectors {
        registry
            .upsert("benchmark", id.clone(), vector.clone())
            .expect("benchmark vector dimensions must match");
    }
    let build_elapsed = build_started.elapsed();
    let working_set_after = process_working_set_bytes();

    let save_started = Instant::now();
    registry
        .save_artifact(&artifact_path)
        .expect("benchmark artifact must be written");
    let save_elapsed = save_started.elapsed();
    let load_started = Instant::now();
    let loaded = HnswRegistry::load_artifact(&artifact_path).expect("benchmark artifact must load");
    let load_elapsed = load_started.elapsed();

    let mut recall_sum = 0.0_f64;
    let mut rerank_recall_sum = 0.0_f64;
    let mut visited_sum = 0_usize;
    let mut hnsw_elapsed = std::time::Duration::ZERO;
    let mut rerank_elapsed = std::time::Duration::ZERO;
    let mut exact_elapsed = std::time::Duration::ZERO;
    let query_started = Instant::now();
    let (approximate, stats) = registry
        .knn("benchmark", &query, K)
        .expect("benchmark query dimensions must match");
    hnsw_elapsed += query_started.elapsed();

    let rerank_started = Instant::now();
    let (candidates, _) = registry
        .knn(
            "benchmark",
            &query,
            K.saturating_mul(RERANK_CANDIDATE_MULTIPLIER),
        )
        .expect("benchmark query dimensions must match");
    let reranked = exact_store
        .score_candidates(&query, candidates.iter().map(|(id, _)| id.as_str()), K)
        .expect("benchmark candidates must be readable");
    rerank_elapsed += rerank_started.elapsed();

    let exact_started = Instant::now();
    let exact_matches = exact
        .knn(&query, K)
        .expect("benchmark query dimensions must match");
    exact_elapsed += exact_started.elapsed();
    let exact_ids = exact_matches
        .into_iter()
        .map(|(id, _)| id)
        .collect::<std::collections::BTreeSet<_>>();
    recall_sum += approximate
        .iter()
        .filter(|(id, _)| exact_ids.contains(id))
        .count() as f64
        / K as f64;
    rerank_recall_sum += reranked
        .iter()
        .filter(|(id, _)| exact_ids.contains(id))
        .count() as f64
        / K as f64;
    visited_sum += stats.visited_nodes;

    let updated_id = &vectors[vector_count / 2].0;
    let updated_vector =
        CanonicalHnswFixture::new(CANONICAL_HNSW_SEED ^ vector_count as u64).vector(dimensions);
    let update_started = Instant::now();
    loaded
        .upsert("benchmark", updated_id.clone(), updated_vector)
        .expect("benchmark vector dimensions must match");
    let update_elapsed = update_started.elapsed();

    let query_count = 1.0;
    let average_seconds = |elapsed: std::time::Duration| elapsed.as_secs_f64() / query_count;
    let status = registry
        .status("benchmark")
        .expect("benchmark index must exist");
    let artifact_bytes = std::fs::metadata(&artifact_path)
        .expect("benchmark artifact metadata must be readable")
        .len();
    eprintln!(
        "hnsw scale gate: vectors={vector_count}, dimensions={dimensions}, recall@{K}={:.4}, rerank_recall@{K}={:.4}, average_visited_nodes={:.1}, build_seconds={:.6}, update_seconds={:.6}, artifact_save_seconds={:.6}, artifact_load_seconds={:.6}, artifact_bytes={artifact_bytes}, estimated_memory_bytes={}, hnsw_average_seconds={:.9}, hnsw_qps={:.1}, rerank_average_seconds={:.9}, rerank_qps={:.1}, exact_average_seconds={:.9}, exact_qps={:.1}",
        recall_sum / query_count,
        rerank_recall_sum / query_count,
        visited_sum as f64 / query_count,
        build_elapsed.as_secs_f64(),
        update_elapsed.as_secs_f64(),
        save_elapsed.as_secs_f64(),
        load_elapsed.as_secs_f64(),
        status.estimated_memory_bytes,
        average_seconds(hnsw_elapsed),
        1.0 / average_seconds(hnsw_elapsed),
        average_seconds(rerank_elapsed),
        1.0 / average_seconds(rerank_elapsed),
        average_seconds(exact_elapsed),
        1.0 / average_seconds(exact_elapsed),
    );
    if let (Some(before), Some(after)) = (working_set_before, working_set_after) {
        eprintln!(
            "hnsw process memory: working_set_before_bytes={before}, working_set_after_bytes={after}, working_set_delta_bytes={}",
            after.saturating_sub(before),
        );
    }
}

fn bench_hnsw(criterion: &mut Criterion) {
    let vector_count = configured_vector_count();
    if configured_scale_gate() {
        for dimensions in configured_dimensions() {
            run_scale_gate(dimensions, vector_count);
        }
        return;
    }
    for dimensions in configured_dimensions() {
        let working_set_before = process_working_set_bytes();
        let workload = Workload::new(dimensions, vector_count);
        let working_set_after = process_working_set_bytes();
        if let (Some(before), Some(after)) = (working_set_before, working_set_after) {
            eprintln!(
                "hnsw process memory: working_set_before_bytes={before}, working_set_after_bytes={after}, working_set_delta_bytes={}",
                after.saturating_sub(before),
            );
        }
        let benchmark_id = || BenchmarkId::new("vectors", format!("{vector_count}-d{dimensions}"));

        let mut build_group = criterion.benchmark_group("hnsw_build");
        build_group.throughput(Throughput::Elements(vector_count as u64));
        build_group.bench_function(benchmark_id(), |bench| {
            bench.iter(|| black_box(workload.build_hnsw()));
        });
        build_group.finish();

        let updated_id = workload.vectors[vector_count / 2].0.clone();
        let updated_vector =
            CanonicalHnswFixture::new(CANONICAL_HNSW_SEED ^ vector_count as u64).vector(dimensions);
        let mut update_group = criterion.benchmark_group("hnsw_update");
        update_group.throughput(Throughput::Elements(1));
        update_group.bench_function(benchmark_id(), |bench| {
            bench.iter_batched(
                || workload.hnsw.clone(),
                |mut index| {
                    index
                        .upsert(updated_id.clone(), updated_vector.clone())
                        .expect("benchmark vector dimensions must match");
                    black_box(());
                },
                BatchSize::LargeInput,
            );
        });
        update_group.finish();

        let mut load_group = criterion.benchmark_group("hnsw_artifact_load");
        load_group.throughput(Throughput::Elements(vector_count as u64));
        load_group.bench_function(benchmark_id(), |bench| {
            bench.iter(|| {
                black_box(
                    HnswRegistry::load_artifact(black_box(&workload.artifact_path))
                        .expect("benchmark artifact must load"),
                );
            });
        });
        load_group.finish();

        let mut hnsw_group = criterion.benchmark_group("hnsw_query");
        hnsw_group.throughput(Throughput::Elements(1));
        hnsw_group.bench_function(benchmark_id(), |bench| {
            bench.iter(|| {
                black_box(
                    workload
                        .hnsw
                        .knn(black_box(&workload.query), K)
                        .expect("benchmark query dimensions must match"),
                );
            });
        });
        hnsw_group.finish();

        workload.print_calibration();

        let mut rerank_group = criterion.benchmark_group("hnsw_file_rerank_query");
        rerank_group.throughput(Throughput::Elements(1));
        rerank_group.bench_function(benchmark_id(), |bench| {
            bench.iter(|| {
                let (candidates, _) = workload
                    .hnsw
                    .knn(
                        black_box(&workload.query),
                        K.saturating_mul(RERANK_CANDIDATE_MULTIPLIER),
                    )
                    .expect("benchmark query dimensions must match");
                black_box(
                    workload
                        .exact_store
                        .score_candidates(
                            &workload.query,
                            candidates.iter().map(|(id, _)| id.as_str()),
                            K,
                        )
                        .expect("benchmark candidates must be readable"),
                );
            });
        });
        rerank_group.finish();

        let mut rerank_pipeline_group =
            criterion.benchmark_group("hnsw_file_rerank_pipeline_query");
        rerank_pipeline_group.throughput(Throughput::Elements(1));
        rerank_pipeline_group.bench_function(benchmark_id(), |bench| {
            bench.iter(|| {
                let (candidates, _) = workload
                    .hnsw
                    .knn(
                        black_box(&workload.query),
                        K.saturating_mul(PIPELINE_RERANK_CANDIDATE_MULTIPLIER),
                    )
                    .expect("benchmark query dimensions must match");
                black_box(
                    workload
                        .exact_store
                        .score_candidates(
                            &workload.query,
                            candidates.iter().map(|(id, _)| id.as_str()),
                            K,
                        )
                        .expect("benchmark candidates must be readable"),
                );
            });
        });
        rerank_pipeline_group.finish();

        let mut exact_group = criterion.benchmark_group("exact_cosine_query");
        exact_group.throughput(Throughput::Elements(1));
        exact_group.bench_function(benchmark_id(), |bench| {
            bench.iter(|| {
                black_box(
                    workload
                        .exact
                        .knn(black_box(&workload.query), K)
                        .expect("benchmark query dimensions must match"),
                );
            });
        });
        exact_group.finish();
    }
}

criterion_group!(benches, bench_hnsw);
criterion_main!(benches);
