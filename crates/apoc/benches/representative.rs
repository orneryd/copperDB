use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use copperdb_engine::{CopperDb, DatabaseConfig};
use copperdb_plugin::resolve_packages;
use copperdb_storage::{EdgeRecord, NodeRecord, StorageEngine};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use serde_json::{json, Value};

const FIXTURE_SIZE: usize = 1_000;
const TRAVERSAL_NODE_COUNT: usize = 65;
const SCALAR_QUERY: &str = "RETURN apoc.text.join(['alpha', 'beta', 'gamma'], '/') AS value";
const JSON_QUERY: &str = "CALL apoc.load.json('payload.json') YIELD value RETURN value";
const TRAVERSAL_QUERY: &str =
    "CALL apoc.path.subgraphNodes($start, {maxLevel: 64}) YIELD node RETURN node";

fn database(config: DatabaseConfig) -> CopperDb {
    let packages =
        resolve_packages([copperdb_apoc::package()]).expect("benchmark APOC package must resolve");
    CopperDb::from_storage_with_packages(
        Arc::new(StorageEngine::open_memory().expect("benchmark storage must open")),
        config,
        &packages,
    )
    .expect("benchmark database must open")
}

fn benchmark_scalar(criterion: &mut Criterion) {
    let database = database(DatabaseConfig::default());
    let result = database
        .execute(SCALAR_QUERY, HashMap::new())
        .expect("scalar warmup must succeed");
    assert_eq!(result.rows[0]["value"], json!("alpha/beta/gamma"));

    criterion.bench_function("apoc_scalar/text_join", |bencher| {
        bencher.iter(|| {
            black_box(
                database
                    .execute(black_box(SCALAR_QUERY), HashMap::new())
                    .expect("scalar benchmark must succeed"),
            );
        });
    });
}

fn benchmark_json_load(criterion: &mut Criterion) {
    let root = tempfile::tempdir().expect("benchmark import root must open");
    let payload = Value::Array(
        (0..FIXTURE_SIZE)
            .map(|index| json!({"id": index, "active": index % 2 == 0}))
            .collect(),
    );
    std::fs::write(
        root.path().join("payload.json"),
        serde_json::to_vec(&payload).expect("benchmark JSON must encode"),
    )
    .expect("benchmark JSON must persist");
    let database = database(DatabaseConfig {
        package_import_file_root: Some(root.path().to_string_lossy().into_owned()),
        ..DatabaseConfig::default()
    });
    let result = database
        .execute(JSON_QUERY, HashMap::new())
        .expect("JSON warmup must succeed");
    assert_eq!(result.rows.len(), FIXTURE_SIZE);

    let mut group = criterion.benchmark_group("apoc_json_load");
    group.throughput(Throughput::Elements(FIXTURE_SIZE as u64));
    group.bench_function("root_array_1000_rows", |bencher| {
        bencher.iter(|| {
            black_box(
                database
                    .execute(black_box(JSON_QUERY), HashMap::new())
                    .expect("JSON benchmark must succeed"),
            );
        });
    });
    group.finish();
}

fn benchmark_subgraph_nodes(criterion: &mut Criterion) {
    let database = database(DatabaseConfig::default());
    let nodes = (0..TRAVERSAL_NODE_COUNT)
        .map(|index| NodeRecord {
            id: format!("node-{index}"),
            labels: vec!["Benchmark".into()],
            properties: BTreeMap::new(),
            named_embeddings: BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .collect::<Vec<_>>();
    database
        .storage()
        .put_node_records_batch(&nodes)
        .expect("benchmark nodes must persist");
    let edges = (0..TRAVERSAL_NODE_COUNT - 1)
        .map(|index| EdgeRecord {
            id: format!("edge-{index}"),
            start_node: format!("node-{index}"),
            end_node: format!("node-{}", index + 1),
            edge_type: "KNOWS".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .collect::<Vec<_>>();
    database
        .storage()
        .put_edge_records_batch(&edges)
        .expect("benchmark edges must persist");
    let params = HashMap::from([("start".into(), json!({"_id": "node-0"}))]);
    let result = database
        .execute(TRAVERSAL_QUERY, params.clone())
        .expect("traversal warmup must succeed");
    assert_eq!(result.rows.len(), TRAVERSAL_NODE_COUNT);

    let mut group = criterion.benchmark_group("apoc_subgraph_nodes");
    group.throughput(Throughput::Elements(TRAVERSAL_NODE_COUNT as u64));
    group.bench_function("linear_65_nodes", |bencher| {
        bencher.iter(|| {
            black_box(
                database
                    .execute(black_box(TRAVERSAL_QUERY), params.clone())
                    .expect("traversal benchmark must succeed"),
            );
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    benchmark_scalar,
    benchmark_json_load,
    benchmark_subgraph_nodes
);
criterion_main!(benches);
