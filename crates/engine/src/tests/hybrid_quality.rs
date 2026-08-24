use super::*;
use copperdb_storage::{IndexDefinition, IndexEntityType, IndexKind, NodeRecord, StorageEngine};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::Arc;

const FIXTURE_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../docs/plans/fixtures/12-hybrid-quality-v1.json"
));

#[derive(Deserialize)]
struct QualityFixture {
    version: u32,
    dimensions: usize,
    limit: usize,
    runs: usize,
    minimum_ndcg_at_k: f64,
    documents: Vec<QualityDocument>,
    queries: Vec<QualityQuery>,
}

#[derive(Deserialize)]
struct QualityDocument {
    id: String,
    title: String,
    content: String,
    vector: Vec<f32>,
}

#[derive(Deserialize)]
struct QualityQuery {
    name: String,
    text: String,
    vector: Vec<f32>,
    relevance: BTreeMap<String, u32>,
}

fn ndcg_at_k(ids: &[String], relevance: &BTreeMap<String, u32>, k: usize) -> f64 {
    let dcg = ids
        .iter()
        .take(k)
        .enumerate()
        .map(|(position, id)| {
            let gain = relevance.get(id).copied().unwrap_or_default();
            (2_f64.powi(gain as i32) - 1.0) / (position as f64 + 2.0).log2()
        })
        .sum::<f64>();
    let mut ideal_gains = relevance.values().copied().collect::<Vec<_>>();
    ideal_gains.sort_unstable_by(|left, right| right.cmp(left));
    let ideal_dcg = ideal_gains
        .into_iter()
        .take(k)
        .enumerate()
        .map(|(position, gain)| (2_f64.powi(gain as i32) - 1.0) / (position as f64 + 2.0).log2())
        .sum::<f64>();
    if ideal_dcg == 0.0 {
        0.0
    } else {
        dcg / ideal_dcg
    }
}

#[test]
fn hybrid_search_meets_shared_graded_quality_and_stability_floor() {
    let fixture: QualityFixture =
        serde_json::from_str(FIXTURE_JSON).expect("hybrid quality fixture must be valid JSON");
    assert_eq!(fixture.version, 1);
    assert!(fixture.runs > 1);

    let storage = StorageEngine::open_memory().expect("quality fixture storage must open");
    storage
        .persist_index_definition(&IndexDefinition {
            name: "quality_text".into(),
            entity_type: IndexEntityType::Node,
            label: "Document".into(),
            properties: vec!["title".into(), "content".into()],
            kind: IndexKind::FullText,
        })
        .unwrap();
    storage
        .persist_index_definition(&IndexDefinition {
            name: "quality_vector".into(),
            entity_type: IndexEntityType::Node,
            label: "Document".into(),
            properties: vec!["embedding".into()],
            kind: IndexKind::Vector,
        })
        .unwrap();
    storage
        .persist_index_options(
            "quality_vector",
            &std::collections::HashMap::from([(
                "indexConfig".into(),
                serde_json::json!({
                    "vector.dimensions": fixture.dimensions,
                    "vector.similarity_function": "cosine"
                }),
            )]),
        )
        .unwrap();
    for document in &fixture.documents {
        assert_eq!(document.vector.len(), fixture.dimensions);
        storage
            .put_node_record(&NodeRecord {
                id: document.id.clone(),
                labels: vec!["Document".into()],
                properties: BTreeMap::from([
                    (
                        "title".into(),
                        serde_json::Value::String(document.title.clone()),
                    ),
                    (
                        "content".into(),
                        serde_json::Value::String(document.content.clone()),
                    ),
                ]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: vec![document.vector.clone()],
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
    }

    let mut config = DatabaseConfig::default();
    config.runtime_config.bm25_enabled = true;
    config.runtime_config.vector_enabled = true;
    let db = CopperDb::from_storage(Arc::new(storage), config).unwrap();
    db.set_ranked_search_cache_enabled(false);
    let placement = PlacementKey::default_for_database("copper");
    let mut ndcg_sum = 0.0;

    for query in &fixture.queries {
        assert_eq!(query.vector.len(), fixture.dimensions);
        let search_query = SearchQuery::Hybrid {
            text: query.text.clone(),
            vector: query.vector.clone(),
            k: fixture.limit,
        };
        let mut expected_ids = None;
        let mut expected_counts = None;
        let mut query_ndcg = 0.0;
        for _ in 0..fixture.runs {
            let outcome = db
                .search_fabric_ranked_outcome_locally_scoped_with_context_and_roles_and_indexes(
                    &copperdb_util::RequestContext::detached(),
                    &placement,
                    &search_query,
                    &["Document".into()],
                    &BTreeMap::new(),
                    &["admin".into()],
                    &[],
                )
                .unwrap();
            let ids = outcome
                .results
                .iter()
                .map(|hit| hit.global_id.local_id.clone())
                .collect::<Vec<_>>();
            let counts = (outcome.input_hits, outcome.fused_hits, outcome.output_hits);
            if let Some(expected_ids) = &expected_ids {
                assert_eq!(&ids, expected_ids, "{} ranked IDs changed", query.name);
                assert_eq!(
                    Some(counts),
                    expected_counts,
                    "{} candidate counts changed",
                    query.name
                );
            } else {
                query_ndcg = ndcg_at_k(&ids, &query.relevance, fixture.limit);
                expected_ids = Some(ids);
                expected_counts = Some(counts);
            }
        }
        assert!(
            query_ndcg >= fixture.minimum_ndcg_at_k,
            "{} NDCG@{} was {:.4}, below {:.4}",
            query.name,
            fixture.limit,
            query_ndcg,
            fixture.minimum_ndcg_at_k
        );
        eprintln!(
            "hybrid quality: query={}, ndcg@{}={:.4}, ids={:?}, counts={:?}",
            query.name,
            fixture.limit,
            query_ndcg,
            expected_ids.expect("quality run must produce ranked IDs"),
            expected_counts.expect("quality run must produce candidate counts")
        );
        ndcg_sum += query_ndcg;
    }

    let mean_ndcg = ndcg_sum / fixture.queries.len() as f64;
    assert!(
        mean_ndcg >= fixture.minimum_ndcg_at_k,
        "mean NDCG@{} was {:.4}, below {:.4}",
        fixture.limit,
        mean_ndcg,
        fixture.minimum_ndcg_at_k
    );
    eprintln!(
        "hybrid quality: mean_ndcg@{}={mean_ndcg:.4}, minimum={:.4}, runs={}",
        fixture.limit, fixture.minimum_ndcg_at_k, fixture.runs
    );
}
