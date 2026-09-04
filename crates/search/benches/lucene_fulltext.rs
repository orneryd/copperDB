use std::sync::Arc;

use copperdb_search::lucene::{FulltextDocument, evaluate_fulltext_query, parse_fulltext_query};
use copperdb_search::{
    CandidateEmbeddingSource, IdentityReranker, MmrReranker, RerankCandidate, Reranker, SearchError,
};
use copperdb_util::RequestContext;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

const VOCABULARY_SIZES: [usize; 2] = [256, 2_048];
const NORNICDB_FULLTEXT_PARSING_QUERIES: [&str; 4] = [
    "simple query",
    "\"exact phrase\"",
    "word1 \"phrase one\" word2 \"phrase two\"",
    "complex AND query OR \"multiple phrases\" NOT excluded",
];

fn vocabulary(size: usize) -> Vec<String> {
    let mut terms = (0..size)
        .map(|index| format!("token{index:04}"))
        .collect::<Vec<_>>();
    terms.extend(["alpha", "beta", "gamma", "prefixtrail", "cloudtrail"].map(str::to_owned));
    terms
}

fn document(terms: &[String]) -> FulltextDocument {
    FulltextDocument::from_fields([
        ("title".into(), "alpha beta prefixtrail cloudtrail".into()),
        ("body".into(), terms.join(" ")),
    ])
}

fn bench_lucene_fulltext(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("lucene_fulltext");
    group.bench_function("nornicdb_fulltext_query_parsing", |bench| {
        bench.iter(|| {
            for input in NORNICDB_FULLTEXT_PARSING_QUERIES {
                black_box(
                    parse_fulltext_query(input).expect("NornicDB benchmark query must parse"),
                );
            }
        });
    });
    for vocabulary_size in VOCABULARY_SIZES {
        let vocabulary = vocabulary(vocabulary_size);
        let document = document(&vocabulary);
        group.throughput(Throughput::Elements(vocabulary.len() as u64));

        for (name, input) in [
            ("exact_terms", "alpha AND beta"),
            (
                "nested_boolean",
                "(alpha AND beta) OR (gamma AND NOT token0001)",
            ),
            ("leading_wildcard", "*trail"),
            ("regex", "/token0[0-9]{3}/"),
            ("fuzzy", "cloudtrail~2"),
        ] {
            let query = parse_fulltext_query(input).expect("benchmark query must parse");
            group.bench_with_input(
                BenchmarkId::new(name, vocabulary_size),
                &vocabulary,
                |bench, vocabulary| {
                    bench.iter(|| {
                        let candidate_terms = query
                            .expand_candidate_terms(black_box(vocabulary))
                            .expect("candidate expansion must succeed");
                        let score = evaluate_fulltext_query(&query, &document)
                            .expect("evaluation must succeed");
                        black_box((candidate_terms, score));
                    });
                },
            );
        }
    }
    group.finish();
}

struct BenchmarkEmbeddings;

impl CandidateEmbeddingSource for BenchmarkEmbeddings {
    fn embedding(
        &self,
        _request_context: &RequestContext,
        candidate_id: &str,
    ) -> Result<Option<Vec<f32>>, SearchError> {
        let index = candidate_id
            .strip_prefix("candidate-")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or_default();
        let angle = index as f32 * 0.1;
        Ok(Some(vec![angle.cos(), angle.sin(), 0.5, 0.25]))
    }
}

fn bench_reranking_sizes(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("reranking_sizes");
    let request_context = RequestContext::detached();
    let identity = IdentityReranker;
    let mmr = MmrReranker::new(Arc::new(BenchmarkEmbeddings), 0.7);
    for size in [10_usize, 50, 100] {
        let candidates = (0..size)
            .map(|index| RerankCandidate {
                id: format!("candidate-{index}"),
                content: format!("document content {index}"),
                score: 1.0 - index as f64 / size as f64,
            })
            .collect::<Vec<_>>();
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("identity", size),
            &candidates,
            |bench, candidates| {
                bench.iter(|| {
                    black_box(
                        identity
                            .rerank(&request_context, "query", black_box(candidates))
                            .unwrap(),
                    )
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("mmr", size),
            &candidates,
            |bench, candidates| {
                bench.iter(|| {
                    black_box(
                        mmr.rerank(&request_context, "query", black_box(candidates))
                            .unwrap(),
                    )
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_lucene_fulltext, bench_reranking_sizes);
criterion_main!(benches);
