use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use copperdb_engine::CopperDb;
use copperdb_eval::{EvalEngine, EvalResult};
use copperdb_storage::{
    EdgeRecord, IndexDefinition, IndexEntityType, IndexKind, NodeRecord, StorageEngine,
};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_json::Value;

const NODE_COUNT: usize = 1_000;
const EDGE_COUNT: usize = NODE_COUNT - 1;
const COUNT_ALL_RELATIONSHIPS: &str = "MATCH ()-[r]->() RETURN count(r) as count";
const OPTIONAL_MATCH_COUNT: &str =
    "MATCH (n) OPTIONAL MATCH (n)-[:KNOWS]->(m) RETURN count(m) as count";
const TWO_HOP_MATCH_COUNT: &str =
    "MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN count(c) as count";
const SHORTEST_PATH_HOPS: &str = "MATCH (start:Star {starId: 's0'}), (end:Star {starId: 's999'}) MATCH p = shortestPath((start)-[:HYPERLANE*]->(end)) RETURN length(p) AS hops";

fn seed_traversal_fixture(storage: &StorageEngine) {
    for index in 0..NODE_COUNT {
        storage
            .put_node_record(&NodeRecord {
                id: format!("n{index}"),
                labels: Vec::new(),
                properties: BTreeMap::new(),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .expect("benchmark node must persist");
    }
    for index in 0..EDGE_COUNT {
        storage
            .put_edge_record(&EdgeRecord {
                id: format!("e{index}"),
                start_node: format!("n{index}"),
                end_node: format!("n{}", index + 1),
                edge_type: "KNOWS".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .expect("benchmark edge must persist");
    }
}

fn seed_shortest_path_fixture(storage: &StorageEngine) {
    storage
        .persist_index_definition(&IndexDefinition {
            name: "star_id".into(),
            entity_type: IndexEntityType::Node,
            label: "Star".into(),
            properties: vec!["starId".into()],
            kind: IndexKind::Range,
        })
        .expect("benchmark range index must persist");
    let nodes = (0..NODE_COUNT)
        .map(|index| NodeRecord {
            id: format!("s{index}"),
            labels: vec!["Star".into()],
            properties: BTreeMap::from([("starId".into(), Value::String(format!("s{index}")))]),
            named_embeddings: BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .collect::<Vec<_>>();
    storage
        .put_node_records_batch(&nodes)
        .expect("benchmark nodes must persist");
    let edges = (0..EDGE_COUNT)
        .map(|index| EdgeRecord {
            id: format!("hyperlane:{index}"),
            start_node: format!("s{index}"),
            end_node: format!("s{}", index + 1),
            edge_type: "HYPERLANE".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .collect::<Vec<_>>();
    storage
        .put_edge_records_batch(&edges)
        .expect("benchmark edges must persist");
}

fn assert_count_all_result(result: &EvalResult) {
    assert_eq!(result.columns, vec!["count".to_owned()]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0].get("count"),
        Some(&Value::from(EDGE_COUNT as u64))
    );
}

fn assert_two_hop_count_result(result: &EvalResult) {
    assert_eq!(result.columns, vec!["count".to_owned()]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0].get("count"),
        Some(&Value::from((EDGE_COUNT - 1) as u64))
    );
}

fn assert_shortest_path_result(result: &EvalResult) {
    assert_eq!(result.columns, vec!["hops".to_owned()]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0].get("hops"),
        Some(&Value::from(EDGE_COUNT as u64))
    );
}

fn traversal_fixture(memory_backed: bool) -> CopperDb {
    let database = if memory_backed {
        CopperDb::open_memory().expect("memory benchmark database must open")
    } else {
        CopperDb::open_temporary().expect("durable benchmark database must open")
    };
    seed_traversal_fixture(database.storage());
    database
}

fn bench_durable_count_all_relationships(criterion: &mut Criterion) {
    let database = traversal_fixture(false);
    let result = database
        .execute(COUNT_ALL_RELATIONSHIPS, Default::default())
        .expect("benchmark traversal warmup must succeed");
    assert_eq!(result.rows.len(), 1);

    let mut group = criterion.benchmark_group("traversal_durable");
    group.throughput(Throughput::Elements(EDGE_COUNT as u64));
    group.bench_function(
        BenchmarkId::new(
            "count_all_relationships",
            format!("{NODE_COUNT}-nodes-fjall"),
        ),
        |bench| {
            bench.iter(|| {
                black_box(
                    database
                        .execute(black_box(COUNT_ALL_RELATIONSHIPS), Default::default())
                        .expect("benchmark traversal must succeed"),
                );
            });
        },
    );
    group.finish();
}

fn bench_memory_count_all_relationships(criterion: &mut Criterion) {
    let database = traversal_fixture(true);
    let result = database
        .execute(COUNT_ALL_RELATIONSHIPS, Default::default())
        .expect("benchmark traversal warmup must succeed");
    assert_eq!(result.rows.len(), 1);

    let mut group = criterion.benchmark_group("traversal_memory");
    group.throughput(Throughput::Elements(EDGE_COUNT as u64));
    group.bench_function(
        BenchmarkId::new(
            "count_all_relationships",
            format!("{NODE_COUNT}-nodes-memory"),
        ),
        |bench| {
            bench.iter(|| {
                black_box(
                    database
                        .execute(black_box(COUNT_ALL_RELATIONSHIPS), Default::default())
                        .expect("benchmark traversal must succeed"),
                );
            });
        },
    );
    group.finish();
}

fn bench_memory_storage_executor_count_all_relationships(criterion: &mut Criterion) {
    let storage =
        Arc::new(StorageEngine::open_memory().expect("memory benchmark storage must open"));
    seed_traversal_fixture(&storage);
    let evaluator = EvalEngine::new(Arc::clone(&storage));
    let result = evaluator
        .execute_cypher(COUNT_ALL_RELATIONSHIPS, &HashMap::new())
        .expect("benchmark traversal warmup must succeed");
    assert_count_all_result(&result);

    let mut group = criterion.benchmark_group("traversal_memory_storage_executor");
    group.throughput(Throughput::Elements(EDGE_COUNT as u64));
    group.bench_function(
        BenchmarkId::new(
            "count_all_relationships",
            format!("{NODE_COUNT}-nodes-memory"),
        ),
        |bench| {
            bench.iter(|| {
                black_box(
                    evaluator
                        .execute_cypher(black_box(COUNT_ALL_RELATIONSHIPS), &HashMap::new())
                        .expect("benchmark traversal must succeed"),
                );
            });
        },
    );
    group.finish();
}

fn bench_memory_storage_executor_optional_match_count(criterion: &mut Criterion) {
    let storage =
        Arc::new(StorageEngine::open_memory().expect("memory benchmark storage must open"));
    seed_traversal_fixture(&storage);
    let evaluator = EvalEngine::new(Arc::clone(&storage));
    let result = evaluator
        .execute_cypher(OPTIONAL_MATCH_COUNT, &HashMap::new())
        .expect("benchmark optional traversal warmup must succeed");
    assert_count_all_result(&result);

    let mut group = criterion.benchmark_group("traversal_memory_storage_executor");
    group.throughput(Throughput::Elements(NODE_COUNT as u64));
    group.bench_function(
        BenchmarkId::new(
            "optional_match_count",
            format!("{NODE_COUNT}-nodes-memory"),
        ),
        |bench| {
            bench.iter(|| {
                black_box(
                    evaluator
                        .execute_cypher(black_box(OPTIONAL_MATCH_COUNT), &HashMap::new())
                        .expect("benchmark optional traversal must succeed"),
                );
            });
        },
    );
    group.finish();
}

fn bench_memory_storage_executor_two_hop_match_count(criterion: &mut Criterion) {
    let storage =
        Arc::new(StorageEngine::open_memory().expect("memory benchmark storage must open"));
    seed_traversal_fixture(&storage);
    let evaluator = EvalEngine::new(Arc::clone(&storage));
    let result = evaluator
        .execute_cypher(TWO_HOP_MATCH_COUNT, &HashMap::new())
        .expect("benchmark two-hop traversal warmup must succeed");
    assert_two_hop_count_result(&result);

    let mut group = criterion.benchmark_group("traversal_memory_storage_executor");
    group.throughput(Throughput::Elements((EDGE_COUNT - 1) as u64));
    group.bench_function(
        BenchmarkId::new(
            "two_hop_match_count",
            format!("{NODE_COUNT}-nodes-memory"),
        ),
        |bench| {
            bench.iter(|| {
                black_box(
                    evaluator
                        .execute_cypher(black_box(TWO_HOP_MATCH_COUNT), &HashMap::new())
                        .expect("benchmark two-hop traversal must succeed"),
                );
            });
        },
    );
    group.finish();
}

fn bench_memory_storage_executor_shortest_path(criterion: &mut Criterion) {
    let storage =
        Arc::new(StorageEngine::open_memory().expect("memory benchmark storage must open"));
    seed_shortest_path_fixture(&storage);
    let evaluator = EvalEngine::new(Arc::clone(&storage));
    let result = evaluator
        .execute_cypher(SHORTEST_PATH_HOPS, &HashMap::new())
        .expect("benchmark shortest-path warmup must succeed");
    assert_shortest_path_result(&result);

    let mut group = criterion.benchmark_group("traversal_memory_storage_executor");
    group.throughput(Throughput::Elements(EDGE_COUNT as u64));
    group.bench_function(
        BenchmarkId::new("shortest_path", format!("{NODE_COUNT}-nodes-memory")),
        |bench| {
            bench.iter(|| {
                black_box(
                    evaluator
                        .execute_cypher(black_box(SHORTEST_PATH_HOPS), &HashMap::new())
                        .expect("benchmark shortest-path traversal must succeed"),
                );
            });
        },
    );
    group.finish();
}

criterion_group!(
    benches,
    bench_durable_count_all_relationships,
    bench_memory_count_all_relationships,
    bench_memory_storage_executor_count_all_relationships,
    bench_memory_storage_executor_optional_match_count,
    bench_memory_storage_executor_two_hop_match_count,
    bench_memory_storage_executor_shortest_path
);
criterion_main!(benches);
