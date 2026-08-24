use std::collections::BTreeMap;

use copperdb_engine::{CopperDb, DatabaseConfig};
use copperdb_search::{
    merge_rrf_search_batches, merge_rrf_search_hits, RrfConfig, RrfSearchBatch, RrfSearchHit,
    SearchQuery,
};
use copperdb_storage::{IndexDefinition, IndexEntityType, IndexKind, NodeRecord, StorageEngine};
use copperdb_topology::{FabricGlobalId, PlacementKey};
use copperdb_util::RequestContext;
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use std::sync::Arc;
use tempfile::TempDir;

const NODE_COUNT: usize = 9_000;
const DIMENSIONS: usize = 64;
const LIMIT: usize = 20;
const HIGH_LIMIT: usize = 100;
const QUERY: &str = "where are my prescriptions?";

#[derive(Clone, Copy)]
enum StorageMode {
    Fjall,
    Memory,
}

struct Workload {
    db: CopperDb,
    placement: PlacementKey,
    hybrid_query: SearchQuery,
    high_limit_hybrid_query: SearchQuery,
    filtered_hybrid_filters: BTreeMap<String, Vec<String>>,
    request_context: RequestContext,
    lexical_query: SearchQuery,
    semantic_query: SearchQuery,
    lexical_batch: RrfSearchBatch,
    semantic_batch: RrfSearchBatch,
    _data_directory: Option<TempDir>,
}

impl Workload {
    fn new(storage_mode: StorageMode) -> Self {
        let data_directory = match storage_mode {
            StorageMode::Fjall => {
                Some(tempfile::tempdir().expect("benchmark data directory must exist"))
            }
            StorageMode::Memory => None,
        };
        let storage = match &data_directory {
            Some(directory) => StorageEngine::open(directory.path().join("db"))
                .expect("durable benchmark storage must open"),
            None => StorageEngine::open_memory().expect("memory benchmark storage must open"),
        };
        storage
            .persist_index_definition(&IndexDefinition {
                name: "document_title".into(),
                entity_type: IndexEntityType::Node,
                label: "Document".into(),
                properties: vec!["title".into(), "content".into()],
                kind: IndexKind::FullText,
            })
            .expect("fulltext index definition must persist");
        storage
            .persist_index_definition(&IndexDefinition {
                name: "document_embedding".into(),
                entity_type: IndexEntityType::Node,
                label: "Document".into(),
                properties: vec!["embedding".into()],
                kind: IndexKind::Vector,
            })
            .expect("vector index definition must persist");
        storage
            .persist_index_options(
                "document_embedding",
                &std::collections::HashMap::from([(
                    "indexConfig".into(),
                    serde_json::json!({
                        "vector.dimensions": DIMENSIONS,
                        "vector.similarity_function": "cosine"
                    }),
                )]),
            )
            .expect("vector index options must persist");
        for position in 0..NODE_COUNT {
            storage
                .put_node_record(&NodeRecord {
                    id: format!("n-{position}"),
                    labels: vec!["Document".into()],
                    properties: BTreeMap::from([
                        (
                            "title".into(),
                            serde_json::Value::String(format!("Prescription document {position}")),
                        ),
                        (
                            "content".into(),
                            serde_json::Value::String(
                                "where are my prescriptions and refill history".into(),
                            ),
                        ),
                        (
                            "cohort".into(),
                            serde_json::Value::String(
                                if position % 2 == 0 { "keep" } else { "discard" }.into(),
                            ),
                        ),
                    ]),
                    named_embeddings: BTreeMap::new(),
                    chunk_embeddings: vec![profile_vector(position)],
                    embed_meta: Default::default(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })
                .expect("benchmark node must persist");
        }
        let mut config = DatabaseConfig {
            ..Default::default()
        };
        config.runtime_config.bm25_enabled = true;
        config.runtime_config.vector_enabled = true;
        let db = match data_directory.as_ref() {
            Some(directory) => {
                drop(storage);
                config.data_dir = directory.path().join("db").to_string_lossy().into_owned();
                CopperDb::open(config).expect("durable benchmark engine must open")
            }
            None => CopperDb::from_storage(Arc::new(storage), config)
                .expect("memory benchmark engine must open"),
        };
        let placement = PlacementKey::default_for_database("copper");
        let hybrid_query = SearchQuery::Hybrid {
            text: QUERY.into(),
            vector: profile_query_vector(),
            k: LIMIT,
        };
        let high_limit_hybrid_query = SearchQuery::Hybrid {
            text: QUERY.into(),
            vector: profile_query_vector(),
            k: HIGH_LIMIT,
        };
        let filtered_hybrid_filters = BTreeMap::from([("cohort".into(), vec!["keep".into()])]);
        let request_context = RequestContext::detached();
        let lexical_query = SearchQuery::FullText {
            query: QUERY.into(),
            fields: Vec::new(),
            limit: LIMIT * 2,
        };
        let semantic_query = SearchQuery::Semantic {
            vector: profile_query_vector(),
            k: LIMIT,
            min_score: 0.5,
        };
        db.set_ranked_search_cache_enabled(false);
        let warmup = db
            .search_fabric_ranked_batch_locally(&placement, &hybrid_query)
            .expect("benchmark warmup search must succeed");
        assert_eq!(warmup.source, "hybrid");
        assert!(warmup.hits.iter().any(|hit| hit.source == "hybrid"));
        assert!(!warmup.hits.is_empty());
        let filtered_warmup = db
            .search_fabric_ranked_outcome_locally_scoped_with_context_and_roles_and_indexes(
                &request_context,
                &placement,
                &hybrid_query,
                &[],
                &filtered_hybrid_filters,
                &["admin".into()],
                &[],
            )
            .expect("benchmark filtered hybrid warmup search must succeed");
        assert_eq!(filtered_warmup.output_hits, LIMIT);
        assert!(filtered_warmup.filtered_hits > 0);
        assert!(filtered_warmup.results.iter().all(|hit| {
            hit.global_id
                .local_id
                .strip_prefix("n-")
                .and_then(|position| position.parse::<usize>().ok())
                .is_some_and(|position| position % 2 == 0)
        }));
        let lexical_batch = db
            .search_fabric_ranked_batch_locally(&placement, &lexical_query)
            .expect("benchmark lexical warmup search must succeed");
        let semantic_batch = db
            .search_fabric_ranked_batch_locally(&placement, &semantic_query)
            .expect("benchmark semantic warmup search must succeed");
        let fused_candidate_count = merge_rrf_search_batches(
            vec![lexical_batch.clone(), semantic_batch.clone()],
            RrfConfig::new(60.0, usize::MAX).with_min_score(0.01),
        )
        .fused_hits;

        eprintln!(
            "search profile RRF contract: lexical_candidates={}, semantic_candidates={}, fused_candidates={}, returned_results={}",
            lexical_batch.hits.len(),
            semantic_batch.hits.len(),
            fused_candidate_count,
            warmup.hits.len(),
        );

        eprintln!(
            "search profile RRF hybrid: nodes={NODE_COUNT}, dimensions={DIMENSIONS}, limit={LIMIT}, response_cache=disabled, storage={}"
            , db.storage_engine().backend_name()
        );
        Self {
            db,
            placement,
            hybrid_query,
            high_limit_hybrid_query,
            filtered_hybrid_filters,
            request_context,
            lexical_query,
            semantic_query,
            lexical_batch,
            semantic_batch,
            _data_directory: data_directory,
        }
    }
}

fn profile_vector(position: usize) -> Vec<f32> {
    let mut vector = vec![0.0; DIMENSIONS];
    vector[position % DIMENSIONS] = 1.0;
    vector[(position + 7) % DIMENSIONS] = 0.25;
    vector[(position + 13) % DIMENSIONS] = 0.15;
    vector
}

fn profile_query_vector() -> Vec<f32> {
    let mut vector = vec![0.0; DIMENSIONS];
    vector[0] = 1.0;
    vector[7] = 0.25;
    vector[13] = 0.15;
    vector
}

fn nornicdb_rrf_fixture_id(position: usize, offset: usize) -> String {
    let prefix = char::from_u32('a' as u32 + ((position + offset) % 26) as u32)
        .expect("benchmark ID prefix must be a Unicode scalar value");
    let suffix = char::from_u32(position as u32)
        .expect("benchmark ID suffix must be a Unicode scalar value");
    format!("{prefix}{suffix}")
}

fn nornicdb_rrf_batches() -> Vec<RrfSearchBatch> {
    let shard = PlacementKey::new("default", "copper", "primary");
    let semantic_hits = (0..50)
        .map(|position| RrfSearchHit {
            global_id: FabricGlobalId::new(
                shard.clone(),
                "node",
                nornicdb_rrf_fixture_id(position, 0),
            ),
            rank: position + 1,
            score: (50 - position) as f32 / 50.0,
            source: "semantic".into(),
            shard: shard.clone(),
            label: "Node".into(),
            snippet: None,
        })
        .collect();
    let lexical_hits = (0..50)
        .map(|position| RrfSearchHit {
            global_id: FabricGlobalId::new(
                shard.clone(),
                "node",
                nornicdb_rrf_fixture_id(position, 10),
            ),
            rank: position + 1,
            score: (50 - position) as f32,
            source: "lexical".into(),
            shard: shard.clone(),
            label: "Node".into(),
            snippet: None,
        })
        .collect();
    vec![
        RrfSearchBatch {
            shard: shard.clone(),
            source: "semantic".into(),
            hits: semantic_hits,
            filtered_hits: 0,
        },
        RrfSearchBatch {
            shard,
            source: "lexical".into(),
            hits: lexical_hits,
            filtered_hits: 0,
        },
    ]
}

fn bench_semantic_hybrid(criterion: &mut Criterion) {
    let workload = Workload::new(StorageMode::Fjall);
    let benchmark_id = BenchmarkId::new("rrf_hybrid", format!("{NODE_COUNT}-d{DIMENSIONS}"));
    let mut group = criterion.benchmark_group("search_profile");
    group.throughput(Throughput::Elements(LIMIT as u64));
    group.bench_with_input(benchmark_id, &workload, |bench, workload| {
        bench.iter(|| {
            black_box(
                workload
                    .db
                    .search_fabric_ranked_batch_locally(
                        &workload.placement,
                        black_box(&workload.hybrid_query),
                    )
                    .expect("benchmark hybrid search must succeed"),
            );
        });
    });
    group.bench_with_input(
        BenchmarkId::new(
            "rrf_hybrid_high_limit",
            format!("{NODE_COUNT}-d{DIMENSIONS}-k{HIGH_LIMIT}"),
        ),
        &workload,
        |bench, workload| {
            bench.iter(|| {
                black_box(
                    workload
                        .db
                        .search_fabric_ranked_batch_locally(
                            &workload.placement,
                            black_box(&workload.high_limit_hybrid_query),
                        )
                        .expect("benchmark high-limit hybrid search must succeed"),
                );
            });
        },
    );
    group.bench_with_input(
        BenchmarkId::new(
            "rrf_hybrid_filtered_50pct",
            format!("{NODE_COUNT}-d{DIMENSIONS}-k{LIMIT}"),
        ),
        &workload,
        |bench, workload| {
            bench.iter(|| {
                black_box(
                    workload
                        .db
                        .search_fabric_ranked_outcome_locally_scoped_with_context_and_roles_and_indexes(
                            &workload.request_context,
                            &workload.placement,
                            black_box(&workload.hybrid_query),
                            &[],
                            black_box(&workload.filtered_hybrid_filters),
                            &["admin".into()],
                            &[],
                        )
                        .expect("benchmark filtered hybrid search must succeed"),
                );
            });
        },
    );
    group.bench_with_input(
        BenchmarkId::new("rrf_hybrid_reopen", format!("{NODE_COUNT}-d{DIMENSIONS}")),
        &workload,
        |bench, workload| {
            bench.iter(|| {
                black_box(
                    workload
                        .db
                        .search_fabric_ranked_batch_locally(
                            &workload.placement,
                            black_box(&workload.hybrid_query),
                        )
                        .expect("benchmark hybrid search must succeed"),
                );
            });
        },
    );
    workload.db.set_ranked_search_cache_enabled(true);
    workload
        .db
        .search_fabric_ranked_batch_locally(&workload.placement, &workload.hybrid_query)
        .expect("benchmark cache warmup search must succeed");
    group.bench_with_input(
        BenchmarkId::new(
            "rrf_hybrid_cache_hit",
            format!("{NODE_COUNT}-d{DIMENSIONS}"),
        ),
        &workload,
        |bench, workload| {
            bench.iter(|| {
                black_box(
                    workload
                        .db
                        .search_fabric_ranked_batch_locally(
                            &workload.placement,
                            black_box(&workload.hybrid_query),
                        )
                        .expect("benchmark cached hybrid search must succeed"),
                );
            });
        },
    );
    workload.db.set_ranked_search_cache_enabled(false);
    group.bench_with_input(
        BenchmarkId::new("lexical_branch", format!("{NODE_COUNT}-d{DIMENSIONS}")),
        &workload,
        |bench, workload| {
            bench.iter(|| {
                black_box(
                    workload
                        .db
                        .search_fabric_ranked_batch_locally(
                            &workload.placement,
                            black_box(&workload.lexical_query),
                        )
                        .expect("benchmark lexical search must succeed"),
                );
            });
        },
    );
    group.bench_with_input(
        BenchmarkId::new("semantic_branch", format!("{NODE_COUNT}-d{DIMENSIONS}")),
        &workload,
        |bench, workload| {
            bench.iter(|| {
                black_box(
                    workload
                        .db
                        .search_fabric_ranked_batch_locally(
                            &workload.placement,
                            black_box(&workload.semantic_query),
                        )
                        .expect("benchmark semantic search must succeed"),
                );
            });
        },
    );
    group.bench_with_input(
        BenchmarkId::new("storage_fulltext", format!("{NODE_COUNT}-d{DIMENSIONS}")),
        &workload,
        |bench, workload| {
            bench.iter(|| {
                black_box(
                    workload
                        .db
                        .storage_engine()
                        .search_fulltext_nodes_by_properties(
                            "Document",
                            &["title".into(), "content".into()],
                            QUERY,
                            LIMIT * 2,
                        )
                        .expect("benchmark storage fulltext search must succeed"),
                );
            });
        },
    );
    group.bench_with_input(
        BenchmarkId::new("policy_catalog_load", format!("{NODE_COUNT}-d{DIMENSIONS}")),
        &workload,
        |bench, workload| {
            bench.iter(|| {
                let storage = workload.db.storage_engine();
                black_box((
                    storage
                        .load_decay_profile_schemas()
                        .expect("benchmark decay profiles must load"),
                    storage
                        .load_decay_profile_binding_schemas()
                        .expect("benchmark decay bindings must load"),
                    storage
                        .load_promotion_profile_schemas()
                        .expect("benchmark promotion profiles must load"),
                    storage
                        .load_promotion_policy_schemas()
                        .expect("benchmark promotion policies must load"),
                ));
            });
        },
    );
    group.bench_with_input(
        BenchmarkId::new("index_schema_load", format!("{NODE_COUNT}-d{DIMENSIONS}")),
        &workload,
        |bench, workload| {
            bench.iter(|| {
                black_box(
                    workload
                        .db
                        .storage_engine()
                        .load_index_definitions()
                        .expect("benchmark index definitions must load"),
                );
            });
        },
    );
    group.bench_with_input(
        BenchmarkId::new("policy_metadata_40", format!("{NODE_COUNT}-d{DIMENSIONS}")),
        &workload,
        |bench, workload| {
            bench.iter(|| {
                let storage = workload.db.storage_engine();
                for position in 0..LIMIT * 2 {
                    black_box(
                        storage
                            .get_knowledge_policy_access_metadata(&format!("n-{position}"))
                            .expect("benchmark access metadata must load"),
                    );
                }
            });
        },
    );
    group.bench_with_input(
        BenchmarkId::new("rrf_fusion", format!("{NODE_COUNT}-d{DIMENSIONS}")),
        &workload,
        |bench, workload| {
            bench.iter_batched(
                || {
                    vec![
                        workload.lexical_batch.clone(),
                        workload.semantic_batch.clone(),
                    ]
                },
                |batches| {
                    black_box(merge_rrf_search_batches(
                        batches,
                        RrfConfig::new(60.0, LIMIT),
                    ))
                },
                BatchSize::SmallInput,
            );
        },
    );
    let nornicdb_rrf_hits = nornicdb_rrf_batches()
        .into_iter()
        .map(|batch| batch.hits)
        .collect::<Vec<_>>();
    let nornicdb_rrf_config = RrfConfig::new(60.0, usize::MAX).with_min_score(0.01);
    let nornicdb_rrf_results = merge_rrf_search_hits(nornicdb_rrf_hits.clone(), nornicdb_rrf_config);
    assert_eq!(nornicdb_rrf_results.len(), 80);
    group.bench_function("rrf_fusion_nornicdb_50x2", |bench| {
        bench.iter_batched(
            || nornicdb_rrf_hits.clone(),
            |hits| {
                black_box(merge_rrf_search_hits(hits, nornicdb_rrf_config));
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();

    let memory_workload = Workload::new(StorageMode::Memory);
    let mut memory_group = criterion.benchmark_group("search_profile_memory");
    memory_group.throughput(Throughput::Elements(LIMIT as u64));
    memory_group.bench_with_input(
        BenchmarkId::new("rrf_hybrid", format!("{NODE_COUNT}-d{DIMENSIONS}")),
        &memory_workload,
        |bench, workload| {
            bench.iter(|| {
                black_box(
                    workload
                        .db
                        .search_fabric_ranked_batch_locally(
                            &workload.placement,
                            black_box(&workload.hybrid_query),
                        )
                        .expect("memory benchmark hybrid search must succeed"),
                );
            });
        },
    );
    memory_group.finish();
}

criterion_group!(benches, bench_semantic_hybrid);
criterion_main!(benches);
