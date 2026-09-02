use std::{
    env,
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

use copperdb_adminimport::{import_offline, ImportOptions};
use copperdb_storage::{
    IndexDefinition, IndexEntityType, IndexKind, NodeEmbeddingMetadata, NodeRecord, StorageEngine,
};
use copperdb_util::RequestCancellation;
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use flate2::{write::GzEncoder, Compression};
use tempfile::TempDir;
use zip::{write::SimpleFileOptions, ZipWriter};

const DEFAULT_NODE_COUNT: usize = 10_000;
const DEFAULT_RELATIONSHIP_COUNT: usize = 50_000;
const CHUNK_SIZES: [usize; 3] = [1_000, 10_000, 100_000];

#[derive(Clone, Copy)]
enum CompressionFormat {
    Plain,
    Gzip,
    Zip,
}

impl CompressionFormat {
    const ALL: [Self; 3] = [Self::Plain, Self::Gzip, Self::Zip];

    const fn name(self) -> &'static str {
        match self {
            Self::Plain => "csv",
            Self::Gzip => "gzip",
            Self::Zip => "zip",
        }
    }

    const fn extension(self) -> &'static str {
        match self {
            Self::Plain => "csv",
            Self::Gzip => "csv.gz",
            Self::Zip => "zip",
        }
    }
}

struct ImportWorkload {
    directory: TempDir,
    nodes: PathBuf,
    relationships: PathBuf,
    source_bytes: u64,
    rows: u64,
}

impl ImportWorkload {
    fn new(format: CompressionFormat, node_count: usize, relationship_count: usize) -> Self {
        let directory = tempfile::tempdir().expect("benchmark data directory must be created");
        let nodes = directory
            .path()
            .join(format!("nodes.{}", format.extension()));
        let relationships = directory
            .path()
            .join(format!("relationships.{}", format.extension()));
        write_nodes(&nodes, format, node_count);
        write_relationships(&relationships, format, node_count, relationship_count);
        let source_bytes = fs::metadata(&nodes)
            .expect("node benchmark source must exist")
            .len()
            + fs::metadata(&relationships)
                .expect("relationship benchmark source must exist")
                .len();
        Self {
            directory,
            nodes,
            relationships,
            source_bytes,
            rows: (node_count + relationship_count) as u64,
        }
    }

    fn options(&self, chunk_size: usize) -> ImportOptions {
        ImportOptions {
            database_name: "benchmark".into(),
            node_sources: vec![self.nodes.clone()],
            relationship_sources: vec![self.relationships.clone()],
            schema_file: None,
            data_directory: self.directory.path().to_path_buf(),
            report_file: None,
            delimiter: b',',
            quote: b'"',
            array_delimiter: ';',
            vector_delimiter: ';',
            empty_strings_as_null: false,
            bad_relationship_tolerance: 0,
            skip_bad_relationships: false,
            chunk_size,
        }
    }
}

fn configured_count(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn write_nodes(path: &Path, format: CompressionFormat, node_count: usize) {
    write_source(path, format, "nodes.csv", |writer| {
        writeln!(
            writer,
            ":ID,:LABEL,name:string,score:double,embedding:vector{{dimensions:4}}"
        )?;
        for node_id in 0..node_count {
            writeln!(
                writer,
                "node-{node_id},Document,document {node_id},{},0.1;0.2;0.3;0.4",
                node_id % 100
            )?;
        }
        Ok(())
    });
}

fn write_relationships(
    path: &Path,
    format: CompressionFormat,
    node_count: usize,
    relationship_count: usize,
) {
    write_source(path, format, "relationships.csv", |writer| {
        writeln!(writer, ":START_ID,:END_ID,:TYPE,weight:double")?;
        for relationship_id in 0..relationship_count {
            let start = relationship_id % node_count;
            let end = (relationship_id + 1) % node_count;
            writeln!(
                writer,
                "node-{start},node-{end},LINKS,{}",
                relationship_id % 100
            )?;
        }
        Ok(())
    });
}

fn write_source<F>(path: &Path, format: CompressionFormat, entry_name: &str, write: F)
where
    F: FnOnce(&mut dyn Write) -> std::io::Result<()>,
{
    match format {
        CompressionFormat::Plain => {
            let file = File::create(path).expect("benchmark source must be created");
            let mut writer = BufWriter::new(file);
            write(&mut writer).expect("benchmark source must be written");
            writer.flush().expect("benchmark source must be flushed");
        }
        CompressionFormat::Gzip => {
            let file = File::create(path).expect("benchmark source must be created");
            let mut writer = GzEncoder::new(BufWriter::new(file), Compression::default());
            write(&mut writer).expect("benchmark source must be written");
            writer.finish().expect("benchmark source must be finalized");
        }
        CompressionFormat::Zip => {
            let file = File::create(path).expect("benchmark source must be created");
            let mut writer = ZipWriter::new(BufWriter::new(file));
            writer
                .start_file(entry_name, SimpleFileOptions::default())
                .expect("zip entry must be started");
            write(&mut writer).expect("benchmark source must be written");
            writer.finish().expect("benchmark source must be finalized");
        }
    }
}

fn import_once(workload: &ImportWorkload, chunk_size: usize, target: PathBuf) {
    let report = import_offline(
        &target,
        &workload.options(chunk_size),
        &RequestCancellation::new(),
    )
    .expect("benchmark import must succeed");
    assert_eq!(
        report.nodes_imported,
        workload.rows - report.relationships_imported
    );
    assert_eq!(
        report.relationships_imported + report.nodes_imported,
        workload.rows
    );
    fs::remove_dir_all(&target).expect("benchmark target must be removed");
    black_box(report);
}

fn target_bytes_written(workload: &ImportWorkload, chunk_size: usize) -> u64 {
    let target = workload.directory.path().join("target-calibration");
    let report = import_offline(
        &target,
        &workload.options(chunk_size),
        &RequestCancellation::new(),
    )
    .expect("benchmark calibration import must succeed");
    assert_eq!(
        report.relationships_imported + report.nodes_imported,
        workload.rows
    );
    let bytes_written = directory_size(&target);
    fs::remove_dir_all(&target).expect("benchmark calibration target must be removed");
    bytes_written
}

fn directory_size(path: &Path) -> u64 {
    fs::read_dir(path)
        .expect("benchmark target must be readable")
        .map(|entry| entry.expect("target entry must be readable").path())
        .map(|path| {
            let metadata = fs::metadata(&path).expect("target metadata must be readable");
            if metadata.is_dir() {
                directory_size(&path)
            } else {
                metadata.len()
            }
        })
        .sum()
}

fn prepare_index_workload(node_count: usize) -> StorageEngine {
    let engine = StorageEngine::open_temporary().expect("benchmark storage must open");
    let records = (0..node_count)
        .map(|node_id| NodeRecord {
            id: format!("node-{node_id}"),
            labels: vec!["Document".into()],
            properties: std::collections::BTreeMap::from([(
                "score".into(),
                serde_json::json!(node_id % 100),
            )]),
            named_embeddings: std::collections::BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: NodeEmbeddingMetadata::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .collect::<Vec<_>>();
    engine
        .put_node_records_batch(&records)
        .expect("benchmark nodes must be stored");
    engine
}

fn bench_schema_index_build(criterion: &mut Criterion) {
    let node_count = configured_count("COPPERDB_ADMINIMPORT_BENCH_NODES", DEFAULT_NODE_COUNT);
    let index = IndexDefinition {
        name: "document_score".into(),
        entity_type: IndexEntityType::Node,
        label: "Document".into(),
        properties: vec!["score".into()],
        kind: IndexKind::Range,
    };
    let mut group = criterion.benchmark_group("offline_import_index_build");
    group.sample_size(10);
    group.throughput(Throughput::Elements(node_count as u64));
    group.bench_function("range", |bench| {
        bench.iter_batched(
            || prepare_index_workload(node_count),
            |engine| {
                engine
                    .persist_index_definition_with_cancellation(
                        black_box(&index),
                        &RequestCancellation::new(),
                    )
                    .expect("benchmark index must build");
                black_box(engine);
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

fn bench_cancellation_latency(criterion: &mut Criterion) {
    let workload = ImportWorkload::new(CompressionFormat::Plain, DEFAULT_NODE_COUNT, 0);
    let sequence = AtomicUsize::new(0);
    let mut group = criterion.benchmark_group("offline_import_cancellation_latency");
    group.sample_size(10);
    group.bench_function("pre_cancelled", |bench| {
        bench.iter_batched(
            || {
                let target = workload.directory.path().join(format!(
                    "cancelled-target-{}",
                    sequence.fetch_add(1, Ordering::Relaxed)
                ));
                let cancellation = RequestCancellation::new();
                cancellation.cancel();
                (target, cancellation)
            },
            |(target, cancellation)| {
                let error = import_offline(
                    &target,
                    black_box(&workload.options(DEFAULT_NODE_COUNT)),
                    &cancellation,
                )
                .expect_err("pre-cancelled benchmark import must fail");
                assert!(matches!(
                    error,
                    copperdb_adminimport::AdminImportError::Cancelled
                ));
                assert!(!target.exists());
                black_box(error);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_offline_import(criterion: &mut Criterion) {
    let node_count = configured_count("COPPERDB_ADMINIMPORT_BENCH_NODES", DEFAULT_NODE_COUNT);
    let relationship_count = configured_count(
        "COPPERDB_ADMINIMPORT_BENCH_RELATIONSHIPS",
        DEFAULT_RELATIONSHIP_COUNT,
    );
    let sequence = AtomicUsize::new(0);

    for format in CompressionFormat::ALL {
        let workload = ImportWorkload::new(format, node_count, relationship_count);
        for chunk_size in CHUNK_SIZES {
            let output_bytes = target_bytes_written(&workload, chunk_size);
            let mut row_group = criterion.benchmark_group("offline_import_rows");
            row_group.sample_size(10);
            row_group.throughput(Throughput::Elements(workload.rows));
            row_group.bench_function(
                BenchmarkId::new(format.name(), format!("chunk-{chunk_size}")),
                |bench| {
                    bench.iter_batched(
                        || {
                            workload.directory.path().join(format!(
                                "target-{}",
                                sequence.fetch_add(1, Ordering::Relaxed)
                            ))
                        },
                        |target| import_once(&workload, chunk_size, target),
                        BatchSize::SmallInput,
                    );
                },
            );
            row_group.finish();

            let mut byte_group = criterion.benchmark_group("offline_import_input_bytes");
            byte_group.sample_size(10);
            byte_group.throughput(Throughput::Bytes(workload.source_bytes));
            byte_group.bench_function(
                BenchmarkId::new(format.name(), format!("chunk-{chunk_size}")),
                |bench| {
                    bench.iter_batched(
                        || {
                            workload.directory.path().join(format!(
                                "target-{}",
                                sequence.fetch_add(1, Ordering::Relaxed)
                            ))
                        },
                        |target| import_once(&workload, chunk_size, target),
                        BatchSize::SmallInput,
                    );
                },
            );
            byte_group.finish();

            let mut output_group = criterion.benchmark_group("offline_import_output_bytes");
            output_group.sample_size(10);
            output_group.throughput(Throughput::Bytes(output_bytes));
            output_group.bench_function(
                BenchmarkId::new(format.name(), format!("chunk-{chunk_size}")),
                |bench| {
                    bench.iter_batched(
                        || {
                            workload.directory.path().join(format!(
                                "target-{}",
                                sequence.fetch_add(1, Ordering::Relaxed)
                            ))
                        },
                        |target| import_once(&workload, chunk_size, target),
                        BatchSize::SmallInput,
                    );
                },
            );
            output_group.finish();
        }
    }
}

criterion_group!(
    benches,
    bench_offline_import,
    bench_schema_index_build,
    bench_cancellation_latency
);
criterion_main!(benches);
