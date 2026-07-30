use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use copperdb_search::lucene::{
    evaluate_fulltext_query, parse_fulltext_query, FulltextDocument,
};

const VOCABULARY_SIZES: [usize; 2] = [256, 2_048];

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
    for vocabulary_size in VOCABULARY_SIZES {
        let vocabulary = vocabulary(vocabulary_size);
        let document = document(&vocabulary);
        group.throughput(Throughput::Elements(vocabulary.len() as u64));

        for (name, input) in [
            ("exact_terms", "alpha AND beta"),
            ("nested_boolean", "(alpha AND beta) OR (gamma AND NOT token0001)"),
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

criterion_group!(benches, bench_lucene_fulltext);
criterion_main!(benches);