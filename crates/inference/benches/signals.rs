use std::sync::Arc;

use copperdb_inference::{
    ExistingEdge, InferenceError, SignalConfig, SignalEngine, SimilarityResult, SimilaritySearch,
};
use copperdb_util::RequestContext;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

struct Search;

impl SimilaritySearch for Search {
    fn search(
        &self,
        _request_context: &RequestContext,
        embedding: &[f32],
        limit: usize,
    ) -> Result<Vec<SimilarityResult>, InferenceError> {
        Ok((0..limit)
            .map(|index| SimilarityResult {
                id: format!("node-{index}-chunk-{}", embedding[0] as usize),
                score: 0.99 - index as f64 / 1_000.0,
            })
            .collect())
    }
}

fn bench_store_signals(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("inference_store_signals");
    let engine =
        SignalEngine::new(SignalConfig::default()).with_similarity_search(Arc::new(Search));
    for chunk_count in [1_usize, 8, 64] {
        let embeddings = (0..chunk_count)
            .map(|index| vec![index as f32, 1.0, 2.0])
            .collect::<Vec<_>>();
        group.bench_with_input(
            BenchmarkId::new("chunks", chunk_count),
            &embeddings,
            |bench, embeddings| {
                bench.iter(|| {
                    engine
                        .on_store(&RequestContext::detached(), "source", black_box(embeddings))
                        .unwrap()
                });
            },
        );
    }
    group.finish();
}

fn bench_transitive_signals(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("inference_transitive_signals");
    let engine = SignalEngine::new(SignalConfig::default());
    for degree in [4_usize, 16, 64] {
        let mut edges = Vec::new();
        for middle in 0..degree {
            edges.push(ExistingEdge {
                source_id: "source".into(),
                target_id: format!("middle-{middle}"),
                confidence: 0.9,
            });
            edges.push(ExistingEdge {
                source_id: format!("middle-{middle}"),
                target_id: format!("target-{middle}"),
                confidence: 0.9,
            });
        }
        group.bench_with_input(
            BenchmarkId::new("degree", degree),
            &edges,
            |bench, edges| {
                bench.iter(|| {
                    engine
                        .transitive(&RequestContext::detached(), black_box(edges))
                        .unwrap()
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_store_signals, bench_transitive_signals);
criterion_main!(benches);
