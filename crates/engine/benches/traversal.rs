use std::collections::BTreeMap;

use copperdb_engine::CopperDb;
use copperdb_storage::{EdgeRecord, NodeRecord};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

const NODE_COUNT: usize = 1_000;
const EDGE_COUNT: usize = NODE_COUNT - 1;
const COUNT_ALL_RELATIONSHIPS: &str = "MATCH ()-[r]->() RETURN count(r) AS count";

fn traversal_fixture() -> CopperDb {
    let database = CopperDb::open_temporary().expect("benchmark database must open");
    for index in 0..NODE_COUNT {
        database
            .storage()
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
        database
            .storage()
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
    database
}

fn bench_count_all_relationships(criterion: &mut Criterion) {
    let database = traversal_fixture();
    let result = database
        .execute(COUNT_ALL_RELATIONSHIPS, Default::default())
        .expect("benchmark traversal warmup must succeed");
    assert_eq!(result.rows.len(), 1);

    let mut group = criterion.benchmark_group("traversal");
    group.throughput(Throughput::Elements(EDGE_COUNT as u64));
    group.bench_function(
        BenchmarkId::new("count_all_relationships", format!("{NODE_COUNT}-nodes")),
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

criterion_group!(benches, bench_count_all_relationships);
criterion_main!(benches);
