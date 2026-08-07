use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use copperdb_cypher::Parser;
use copperdb_engine::CopperDb;
use copperdb_eval::EvalEngine;
use copperdb_storage::{EdgeRecord, NodeRecord, StorageEngine};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

const NODE_COUNT: usize = 1_000;
const EDGE_COUNT: usize = NODE_COUNT - 1;
const COUNT_ALL_RELATIONSHIPS: &str = "MATCH ()-[r]->() RETURN count(r) as count";

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
    let parser = Parser::new();
    let warmup = parser
        .parse(COUNT_ALL_RELATIONSHIPS)
        .expect("benchmark query must parse");
    evaluator
        .execute(&warmup, &HashMap::new())
        .expect("benchmark traversal warmup must succeed");

    let mut group = criterion.benchmark_group("traversal_memory_storage_executor");
    group.throughput(Throughput::Elements(EDGE_COUNT as u64));
    group.bench_function(
        BenchmarkId::new(
            "count_all_relationships",
            format!("{NODE_COUNT}-nodes-memory"),
        ),
        |bench| {
            bench.iter(|| {
                let query = Parser::new()
                    .parse(black_box(COUNT_ALL_RELATIONSHIPS))
                    .expect("benchmark query must parse");
                black_box(
                    evaluator
                        .execute(&query, &HashMap::new())
                        .expect("benchmark traversal must succeed"),
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
    bench_memory_storage_executor_count_all_relationships
);
criterion_main!(benches);
