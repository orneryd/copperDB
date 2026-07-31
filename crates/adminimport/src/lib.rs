//! Offline administrative import and export pipeline primitives.
//!
//! This crate owns source validation and deterministic reporting. Storage
//! staging and CSV record ingestion are added in later Plan 09 phases.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{BufReader, Read},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use copperdb_convert::{
    parse_neo4j_header, parse_neo4j_value, Neo4jColumn, Neo4jColumnKind, Neo4jHeaderTarget,
    Neo4jValueOptions, Value,
};
use copperdb_storage::{
    EdgeRecord, NodeEmbeddingMetadata, NodeRecord, StorageEngine, StorageError,
};
use copperdb_util::RequestCancellation;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const EXIT_OK: i32 = 0;
pub const EXIT_CSV: i32 = 2;
pub const EXIT_UNSUPPORTED: i32 = 6;
pub const MAX_ZIP_ENTRY_BYTES: u64 = 256 * 1024 * 1024;
pub const NEO4J_CSV_SCHEMA_FILE: &str = "copperdb-schema.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportOptions {
    pub database_name: String,
    pub node_sources: Vec<PathBuf>,
    pub relationship_sources: Vec<PathBuf>,
    pub schema_file: Option<PathBuf>,
    pub data_directory: PathBuf,
    pub report_file: Option<PathBuf>,
    pub delimiter: u8,
    pub quote: u8,
    pub array_delimiter: char,
    pub vector_delimiter: char,
    pub empty_strings_as_null: bool,
    pub bad_relationship_tolerance: usize,
    pub skip_bad_relationships: bool,
    pub chunk_size: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreflightReport {
    pub database_name: String,
    pub node_sources: Vec<SourceMetadata>,
    pub relationship_sources: Vec<SourceMetadata>,
    pub status: PreflightStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeImportReport {
    pub database_name: String,
    pub nodes_imported: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportReport {
    pub database_name: String,
    pub nodes_imported: u64,
    pub relationships_imported: u64,
    pub bad_relationships: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Neo4jCsvExportOptions {
    pub output_directory: PathBuf,
    pub delimiter: u8,
    pub quote: u8,
    pub array_delimiter: char,
    pub vector_delimiter: char,
}

impl Neo4jCsvExportOptions {
    pub fn new(output_directory: impl Into<PathBuf>) -> Self {
        Self {
            output_directory: output_directory.into(),
            delimiter: b',',
            quote: b'"',
            array_delimiter: ';',
            vector_delimiter: ';',
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Neo4jCsvExportReport {
    pub nodes_exported: u64,
    pub relationships_exported: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaMetadata {
    pub constraints: Vec<copperdb_storage::Constraint>,
    pub indexes: Vec<copperdb_storage::IndexDefinition>,
    pub index_options: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreflightStatus {
    Ready,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceMetadata {
    pub path: PathBuf,
    pub format: SourceFormat,
    pub bytes: u64,
    pub header: Vec<Neo4jColumn>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceFormat {
    Csv,
    GzipCsv,
    Zip,
}

#[derive(Debug, Error)]
pub enum AdminImportError {
    #[error("database name is required")]
    MissingDatabaseName,
    #[error("database name contains an unsupported character: {0}")]
    UnsafeDatabaseName(String),
    #[error("at least one node source is required")]
    MissingNodeSource,
    #[error("{kind} source does not exist: {path}")]
    MissingSource { kind: &'static str, path: PathBuf },
    #[error("{kind} source is not a regular file: {path}")]
    SourceNotFile { kind: &'static str, path: PathBuf },
    #[error("{kind} source is empty: {path}")]
    EmptySource { kind: &'static str, path: PathBuf },
    #[error("unsupported source format: {path}")]
    UnsupportedSourceFormat { path: PathBuf },
    #[error("source is listed more than once: {path}")]
    DuplicateSource { path: PathBuf },
    #[error("report path must be contained in the data directory: {path}")]
    UnsafeReportPath { path: PathBuf },
    #[error("import preflight was cancelled")]
    Cancelled,
    #[error("failed to inspect source {path}: {source}")]
    InspectSource {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read CSV header from {path}: {source}")]
    ReadCsvHeader {
        path: PathBuf,
        #[source]
        source: csv::Error,
    },
    #[error("invalid CSV header in {path}: {source}")]
    InvalidHeader {
        path: PathBuf,
        #[source]
        source: copperdb_convert::ConvertError,
    },
    #[error("invalid zip source {path}: {source}")]
    InvalidZip {
        path: PathBuf,
        #[source]
        source: zip::result::ZipError,
    },
    #[error("zip source must contain exactly one CSV file: {path}")]
    AmbiguousZipSource { path: PathBuf },
    #[error("zip source contains an unsafe entry path: {path}")]
    UnsafeZipEntry { path: PathBuf },
    #[error("zip source expands beyond the {limit} byte limit: {path}")]
    OversizedZipEntry { path: PathBuf, limit: u64 },
    #[error("import chunk size must be greater than zero")]
    InvalidChunkSize,
    #[error("relationship sources require the relationship import phase")]
    RelationshipImportNotImplemented,
    #[error("duplicate node ID during import: {0}")]
    DuplicateNodeId(String),
    #[error("duplicate relationship ID during import: {0}")]
    DuplicateRelationshipId(String),
    #[error("relationship CSV row {row} is missing a {endpoint} ID: {path}")]
    MissingRelationshipEndpoint {
        path: PathBuf,
        row: u64,
        endpoint: &'static str,
    },
    #[error("relationship CSV must have exactly one {column} column: {path}")]
    InvalidRelationshipColumns { path: PathBuf, column: &'static str },
    #[error("relationship CSV row {row} has an unknown {endpoint} node {id}: {path}")]
    MissingRelationshipNode {
        path: PathBuf,
        row: u64,
        endpoint: &'static str,
        id: String,
    },
    #[error("relationship CSV row {row} is missing a type: {path}")]
    MissingRelationshipType { path: PathBuf, row: u64 },
    #[error("invalid relationship value in column {column} at row {row}: {path}: {source}")]
    InvalidRelationshipValue {
        path: PathBuf,
        row: u64,
        column: String,
        #[source]
        source: copperdb_convert::ConvertError,
    },
    #[error("storage import error: {0}")]
    Storage(#[from] StorageError),
    #[error("failed to write import report {path}: {source}")]
    WriteReport {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to serialize import report: {0}")]
    SerializeReport(#[from] serde_json::Error),
    #[error("failed to read CSV row {row} from {path}: {source}")]
    ReadCsvRow {
        path: PathBuf,
        row: u64,
        #[source]
        source: csv::Error,
    },
    #[error("CSV row {row} has {actual} columns but header has {expected}: {path}")]
    ColumnCountMismatch {
        path: PathBuf,
        row: u64,
        expected: usize,
        actual: usize,
    },
    #[error("node CSV row {row} is missing an ID: {path}")]
    MissingNodeId { path: PathBuf, row: u64 },
    #[error("node CSV row {row} has more than one ID: {path}")]
    MultipleNodeIds { path: PathBuf, row: u64 },
    #[error("invalid node value in column {column} at row {row}: {path}: {source}")]
    InvalidNodeValue {
        path: PathBuf,
        row: u64,
        column: String,
        #[source]
        source: copperdb_convert::ConvertError,
    },
    #[error("export output directory already exists: {path}")]
    ExportOutputExists { path: PathBuf },
    #[error("failed to prepare Neo4j CSV export directory {path}: {source}")]
    PrepareExportOutput {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write Neo4j CSV export file {path}: {source}")]
    WriteExportCsv {
        path: PathBuf,
        #[source]
        source: csv::Error,
    },
    #[error("Neo4j CSV export cannot represent {value_type} in property {property}")]
    UnsupportedExportValue {
        property: String,
        value_type: &'static str,
    },
    #[error("Neo4j CSV export found incompatible values in property {property}")]
    IncompatibleExportValues { property: String },
    #[error("Neo4j CSV export found a sparse named embedding: {name}")]
    SparseNamedEmbedding { name: String },
    #[error("schema file does not exist: {path}")]
    MissingSchemaFile { path: PathBuf },
    #[error("schema file is not a regular file: {path}")]
    SchemaFileNotFile { path: PathBuf },
    #[error("failed to read schema metadata {path}: {source}")]
    ReadSchemaMetadata {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid schema metadata {path}: {source}")]
    InvalidSchemaMetadata {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to write schema metadata {path}: {source}")]
    WriteSchemaMetadata {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

impl AdminImportError {
    pub const fn exit_code(&self) -> i32 {
        match self {
            Self::MissingDatabaseName
            | Self::UnsafeDatabaseName(_)
            | Self::MissingNodeSource
            | Self::UnsupportedSourceFormat { .. }
            | Self::UnsafeReportPath { .. }
            | Self::InvalidChunkSize
            | Self::RelationshipImportNotImplemented
            | Self::Storage(_)
            | Self::WriteReport { .. }
            | Self::SerializeReport(_)
            | Self::ExportOutputExists { .. }
            | Self::PrepareExportOutput { .. }
            | Self::WriteExportCsv { .. }
            | Self::UnsupportedExportValue { .. }
            | Self::IncompatibleExportValues { .. }
            | Self::SparseNamedEmbedding { .. }
            | Self::MissingSchemaFile { .. }
            | Self::SchemaFileNotFile { .. }
            | Self::ReadSchemaMetadata { .. }
            | Self::InvalidSchemaMetadata { .. }
            | Self::WriteSchemaMetadata { .. } => EXIT_UNSUPPORTED,
            Self::Cancelled => EXIT_UNSUPPORTED,
            Self::MissingSource { .. }
            | Self::SourceNotFile { .. }
            | Self::EmptySource { .. }
            | Self::DuplicateSource { .. }
            | Self::InspectSource { .. }
            | Self::ReadCsvHeader { .. }
            | Self::InvalidHeader { .. }
            | Self::InvalidZip { .. }
            | Self::AmbiguousZipSource { .. }
            | Self::UnsafeZipEntry { .. }
            | Self::OversizedZipEntry { .. }
            | Self::ReadCsvRow { .. }
            | Self::ColumnCountMismatch { .. }
            | Self::MissingNodeId { .. }
            | Self::MultipleNodeIds { .. }
            | Self::DuplicateNodeId(_)
            | Self::InvalidNodeValue { .. }
            | Self::DuplicateRelationshipId(_)
            | Self::MissingRelationshipEndpoint { .. }
            | Self::InvalidRelationshipColumns { .. }
            | Self::MissingRelationshipNode { .. }
            | Self::MissingRelationshipType { .. }
            | Self::InvalidRelationshipValue { .. } => EXIT_CSV,
        }
    }
}

/// Write a deterministic JSON report when `report_file` is configured.
///
/// The report is written through a sibling temporary file and then atomically
/// replaced, so callers never observe partially serialized output.
pub fn write_import_report(
    options: &ImportOptions,
    report: &ImportReport,
) -> Result<(), AdminImportError> {
    let Some(report_file) = options.report_file.as_deref() else {
        return Ok(());
    };
    let data_directory = canonical_directory(&options.data_directory)?;
    validate_report_path(Some(report_file), &data_directory)?;
    let report_path = if report_file.is_absolute() {
        report_file.to_path_buf()
    } else {
        data_directory.join(report_file)
    };
    let parent = report_path.parent().unwrap_or(&data_directory);
    fs::create_dir_all(parent).map_err(|source| AdminImportError::WriteReport {
        path: report_path.clone(),
        source,
    })?;
    let parent = parent
        .canonicalize()
        .map_err(|source| AdminImportError::WriteReport {
            path: report_path.clone(),
            source,
        })?;
    if !parent.starts_with(&data_directory) {
        return Err(AdminImportError::UnsafeReportPath {
            path: report_file.to_path_buf(),
        });
    }
    let report_path =
        parent.join(
            report_path
                .file_name()
                .ok_or_else(|| AdminImportError::UnsafeReportPath {
                    path: report_file.to_path_buf(),
                })?,
        );
    let bytes = serde_json::to_vec_pretty(report)?;
    let mut temporary = tempfile::NamedTempFile::new_in(&parent).map_err(|source| {
        AdminImportError::WriteReport {
            path: report_path.clone(),
            source,
        }
    })?;
    use std::io::Write;
    temporary
        .write_all(&bytes)
        .and_then(|()| temporary.flush())
        .map_err(|source| AdminImportError::WriteReport {
            path: report_path.clone(),
            source,
        })?;
    temporary
        .persist(&report_path)
        .map_err(|error| AdminImportError::WriteReport {
            path: report_path,
            source: error.error,
        })?;
    Ok(())
}

/// Export graph records as deterministic Neo4j CSV files without loading the
/// graph into memory. The output directory is only made visible after both
/// streams have completed successfully.
pub fn export_neo4j_csv(
    engine: &StorageEngine,
    options: &Neo4jCsvExportOptions,
    cancellation: &RequestCancellation,
) -> Result<Neo4jCsvExportReport, AdminImportError> {
    check_cancelled(cancellation)?;
    if options.output_directory.exists() {
        return Err(AdminImportError::ExportOutputExists {
            path: options.output_directory.clone(),
        });
    }

    let parent = options
        .output_directory
        .parent()
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|source| AdminImportError::PrepareExportOutput {
        path: parent.to_path_buf(),
        source,
    })?;
    let parent = parent
        .canonicalize()
        .map_err(|source| AdminImportError::PrepareExportOutput {
            path: parent.to_path_buf(),
            source,
        })?;
    let output_name = options
        .output_directory
        .file_name()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| AdminImportError::PrepareExportOutput {
            path: options.output_directory.clone(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "export output directory requires a final path component",
            ),
        })?;
    let output_directory = parent.join(output_name);
    if output_directory.exists() {
        return Err(AdminImportError::ExportOutputExists {
            path: output_directory,
        });
    }

    let staging = tempfile::Builder::new()
        .prefix(".copperdb-export-")
        .tempdir_in(&parent)
        .map_err(|source| AdminImportError::PrepareExportOutput {
            path: parent.clone(),
            source,
        })?;
    let node_columns = infer_node_export_columns(engine, cancellation)?;
    let edge_columns = infer_edge_export_columns(engine, cancellation)?;
    let nodes_exported =
        write_node_export(engine, staging.path(), &node_columns, options, cancellation)?;
    let relationships_exported = if edge_columns.record_count == 0 {
        0
    } else {
        write_edge_export(engine, staging.path(), &edge_columns, options, cancellation)?
    };
    write_schema_metadata(engine, staging.path())?;
    check_cancelled(cancellation)?;
    fs::rename(staging.path(), &output_directory).map_err(|source| {
        AdminImportError::PrepareExportOutput {
            path: output_directory,
            source,
        }
    })?;

    Ok(Neo4jCsvExportReport {
        nodes_exported,
        relationships_exported,
    })
}

fn write_schema_metadata(
    engine: &StorageEngine,
    output_directory: &Path,
) -> Result<(), AdminImportError> {
    let indexes = engine.load_index_definitions()?;
    let mut index_options = BTreeMap::new();
    for index in &indexes {
        if let Some(options) = engine.load_index_options(&index.name)? {
            index_options.insert(index.name.clone(), options.into_iter().collect());
        }
    }
    let metadata = SchemaMetadata {
        constraints: engine.load_constraints()?,
        indexes,
        index_options,
    };
    let path = output_directory.join(NEO4J_CSV_SCHEMA_FILE);
    let mut bytes = serde_json::to_vec_pretty(&metadata)?;
    bytes.push(b'\n');
    fs::write(&path, bytes).map_err(|source| AdminImportError::WriteSchemaMetadata { path, source })
}

fn read_schema_metadata(path: &Path) -> Result<SchemaMetadata, AdminImportError> {
    let metadata = path.metadata().map_err(|source| match source.kind() {
        std::io::ErrorKind::NotFound => AdminImportError::MissingSchemaFile {
            path: path.to_path_buf(),
        },
        _ => AdminImportError::ReadSchemaMetadata {
            path: path.to_path_buf(),
            source,
        },
    })?;
    if !metadata.is_file() {
        return Err(AdminImportError::SchemaFileNotFile {
            path: path.to_path_buf(),
        });
    }
    let bytes = fs::read(path).map_err(|source| AdminImportError::ReadSchemaMetadata {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_slice(&bytes).map_err(|source| AdminImportError::InvalidSchemaMetadata {
        path: path.to_path_buf(),
        source,
    })
}

fn apply_schema_metadata(
    engine: &StorageEngine,
    metadata: &SchemaMetadata,
    cancellation: &RequestCancellation,
) -> Result<(), AdminImportError> {
    for constraint in &metadata.constraints {
        check_cancelled(cancellation)?;
        engine.persist_constraint(constraint)?;
    }
    for index in &metadata.indexes {
        check_cancelled(cancellation)?;
        if let Some(options) = metadata.index_options.get(&index.name) {
            engine.persist_index_options(&index.name, &options.clone().into_iter().collect())?;
        }
        engine.persist_index_definition_with_cancellation(index, cancellation)?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScalarType {
    String,
    Boolean,
    Long,
    Double,
}

impl ScalarType {
    const fn neo4j_name(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Boolean => "boolean",
            Self::Long => "long",
            Self::Double => "double",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExportValueShape {
    Scalar(ScalarType),
    Array(ScalarType),
    Vector(usize),
}

#[derive(Debug, Clone)]
struct ExportColumn {
    name: String,
    shape: ExportValueShape,
    named_embedding: bool,
}

impl ExportColumn {
    fn header(&self) -> String {
        if self.named_embedding {
            let ExportValueShape::Vector(dimensions) = self.shape else {
                unreachable!("named embeddings are always vectors")
            };
            return format!(":EMBEDDING({}){{dimensions:{dimensions}}}", self.name);
        }
        match self.shape {
            ExportValueShape::Scalar(value_type) => {
                format!("{}:{}", self.name, value_type.neo4j_name())
            }
            ExportValueShape::Array(value_type) => {
                format!("{}:{}[]", self.name, value_type.neo4j_name())
            }
            ExportValueShape::Vector(dimensions) => {
                format!("{}:vector{{dimensions:{dimensions}}}", self.name)
            }
        }
    }
}

#[derive(Debug, Default)]
struct ExportSchema {
    columns: Vec<ExportColumn>,
    record_count: u64,
}

fn infer_node_export_columns(
    engine: &StorageEngine,
    cancellation: &RequestCancellation,
) -> Result<ExportSchema, AdminImportError> {
    let mut properties = BTreeMap::new();
    let mut embeddings = BTreeMap::new();
    let mut record_count = 0;
    let mut failure = None;
    let result = engine.stream_node_records_with_cancellation(cancellation, |node| {
        record_count += 1;
        for (name, value) in &node.properties {
            if let Err(error) = merge_export_property(&mut properties, name, value) {
                failure = Some(error);
                return Err(StorageError::IterationStopped);
            }
        }
        for (name, vector) in &node.named_embeddings {
            let dimensions = vector.len();
            match embeddings.get(name) {
                Some(existing) if *existing != dimensions => {
                    failure = Some(AdminImportError::IncompatibleExportValues {
                        property: format!(":EMBEDDING({name})"),
                    });
                    return Err(StorageError::IterationStopped);
                }
                Some(_) => {}
                None => {
                    embeddings.insert(name.clone(), dimensions);
                }
            }
        }
        Ok(())
    });
    if let Some(error) = failure {
        return Err(error);
    }
    result?;

    let mut columns = properties
        .into_iter()
        .map(|(name, shape)| ExportColumn {
            name,
            shape,
            named_embedding: false,
        })
        .collect::<Vec<_>>();
    for (name, dimensions) in embeddings {
        columns.push(ExportColumn {
            name,
            shape: ExportValueShape::Vector(dimensions),
            named_embedding: true,
        });
    }
    for column in &columns {
        if column.named_embedding {
            let name = &column.name;
            let mut present = 0;
            engine.stream_node_records_with_cancellation(cancellation, |node| {
                if node.named_embeddings.contains_key(name) {
                    present += 1;
                }
                Ok(())
            })?;
            if present != record_count {
                return Err(AdminImportError::SparseNamedEmbedding { name: name.clone() });
            }
        }
    }
    columns.sort_by_key(ExportColumn::header);
    Ok(ExportSchema {
        columns,
        record_count,
    })
}

fn infer_edge_export_columns(
    engine: &StorageEngine,
    cancellation: &RequestCancellation,
) -> Result<ExportSchema, AdminImportError> {
    let mut properties = BTreeMap::new();
    let mut record_count = 0;
    let mut failure = None;
    let result = engine.stream_edge_records_with_cancellation(cancellation, |edge| {
        record_count += 1;
        for (name, value) in &edge.properties {
            if let Err(error) = merge_export_property(&mut properties, name, value) {
                failure = Some(error);
                return Err(StorageError::IterationStopped);
            }
        }
        Ok(())
    });
    if let Some(error) = failure {
        return Err(error);
    }
    result?;
    let mut columns = properties
        .into_iter()
        .map(|(name, shape)| ExportColumn {
            name,
            shape,
            named_embedding: false,
        })
        .collect::<Vec<_>>();
    columns.sort_by_key(ExportColumn::header);
    Ok(ExportSchema {
        columns,
        record_count,
    })
}

fn merge_export_property(
    properties: &mut BTreeMap<String, ExportValueShape>,
    name: &str,
    value: &serde_json::Value,
) -> Result<(), AdminImportError> {
    let shape = export_value_shape(value, name)?;
    match properties.get_mut(name) {
        None => {
            properties.insert(name.to_owned(), shape);
            Ok(())
        }
        Some(existing) => merge_export_shape(existing, shape, name),
    }
}

fn merge_export_shape(
    existing: &mut ExportValueShape,
    next: ExportValueShape,
    property: &str,
) -> Result<(), AdminImportError> {
    if *existing == next {
        Ok(())
    } else {
        Err(AdminImportError::IncompatibleExportValues {
            property: property.to_owned(),
        })
    }
}

fn export_value_shape(
    value: &serde_json::Value,
    property: &str,
) -> Result<ExportValueShape, AdminImportError> {
    let scalar = |value: &serde_json::Value| match value {
        serde_json::Value::String(_) => Ok(ScalarType::String),
        serde_json::Value::Bool(_) => Ok(ScalarType::Boolean),
        serde_json::Value::Number(number) if number.as_i64().is_some() => Ok(ScalarType::Long),
        serde_json::Value::Number(number)
            if number
                .as_u64()
                .is_some_and(|number| number <= i64::MAX as u64) =>
        {
            Ok(ScalarType::Long)
        }
        serde_json::Value::Number(number) if number.as_f64().is_some() => Ok(ScalarType::Double),
        serde_json::Value::Null => Err(AdminImportError::UnsupportedExportValue {
            property: property.to_owned(),
            value_type: "null",
        }),
        serde_json::Value::Array(_) => Err(AdminImportError::UnsupportedExportValue {
            property: property.to_owned(),
            value_type: "nested array",
        }),
        serde_json::Value::Object(_) => Err(AdminImportError::UnsupportedExportValue {
            property: property.to_owned(),
            value_type: "object",
        }),
        serde_json::Value::Number(_) => Err(AdminImportError::UnsupportedExportValue {
            property: property.to_owned(),
            value_type: "number outside Neo4j long/double range",
        }),
    };
    match value {
        serde_json::Value::Null => Err(AdminImportError::UnsupportedExportValue {
            property: property.to_owned(),
            value_type: "null",
        }),
        serde_json::Value::Array(values) if values.is_empty() => {
            Err(AdminImportError::UnsupportedExportValue {
                property: property.to_owned(),
                value_type: "empty array",
            })
        }
        serde_json::Value::Array(values) => {
            let scalar_type = scalar(&values[0])?;
            for value in &values[1..] {
                if scalar(value)? != scalar_type {
                    return Err(AdminImportError::IncompatibleExportValues {
                        property: property.to_owned(),
                    });
                }
            }
            if scalar_type == ScalarType::Double {
                Ok(ExportValueShape::Vector(values.len()))
            } else {
                Ok(ExportValueShape::Array(scalar_type))
            }
        }
        value => Ok(ExportValueShape::Scalar(scalar(value)?)),
    }
}

fn write_node_export(
    engine: &StorageEngine,
    output_directory: &Path,
    schema: &ExportSchema,
    options: &Neo4jCsvExportOptions,
    cancellation: &RequestCancellation,
) -> Result<u64, AdminImportError> {
    let path = output_directory.join("nodes.csv");
    let mut writer = csv::WriterBuilder::new()
        .delimiter(options.delimiter)
        .quote(options.quote)
        .from_path(&path)
        .map_err(|source| AdminImportError::WriteExportCsv {
            path: path.clone(),
            source,
        })?;
    let mut header = vec![":ID".to_owned(), ":LABEL".to_owned()];
    header.extend(schema.columns.iter().map(ExportColumn::header));
    writer
        .write_record(&header)
        .map_err(|source| AdminImportError::WriteExportCsv {
            path: path.clone(),
            source,
        })?;
    let mut failure = None;
    let streamed = engine.stream_node_records_with_cancellation(cancellation, |node| {
        let mut row = vec![
            node.id,
            node.labels.join(&options.array_delimiter.to_string()),
        ];
        for column in &schema.columns {
            let value = if column.named_embedding {
                node.named_embeddings
                    .get(&column.name)
                    .map(|vector| format_vector(vector, options.vector_delimiter))
                    .ok_or_else(|| AdminImportError::SparseNamedEmbedding {
                        name: column.name.clone(),
                    })
            } else {
                format_export_value(
                    node.properties.get(&column.name),
                    &column.shape,
                    options,
                    &column.name,
                )
            };
            match value {
                Ok(value) => row.push(value),
                Err(error) => {
                    failure = Some(error);
                    return Err(StorageError::IterationStopped);
                }
            }
        }
        if let Err(source) = writer.write_record(&row) {
            failure = Some(AdminImportError::WriteExportCsv {
                path: path.clone(),
                source,
            });
            return Err(StorageError::IterationStopped);
        }
        Ok(())
    });
    if let Some(error) = failure {
        return Err(error);
    }
    let streamed = streamed?;
    writer
        .flush()
        .map_err(|source| AdminImportError::WriteExportCsv {
            path,
            source: source.into(),
        })?;
    Ok(streamed)
}

fn write_edge_export(
    engine: &StorageEngine,
    output_directory: &Path,
    schema: &ExportSchema,
    options: &Neo4jCsvExportOptions,
    cancellation: &RequestCancellation,
) -> Result<u64, AdminImportError> {
    let path = output_directory.join("relationships.csv");
    let mut writer = csv::WriterBuilder::new()
        .delimiter(options.delimiter)
        .quote(options.quote)
        .from_path(&path)
        .map_err(|source| AdminImportError::WriteExportCsv {
            path: path.clone(),
            source,
        })?;
    let mut header = vec![
        ":ID".to_owned(),
        ":START_ID".to_owned(),
        ":END_ID".to_owned(),
        ":TYPE".to_owned(),
    ];
    header.extend(schema.columns.iter().map(ExportColumn::header));
    writer
        .write_record(&header)
        .map_err(|source| AdminImportError::WriteExportCsv {
            path: path.clone(),
            source,
        })?;
    let mut failure = None;
    let streamed = engine.stream_edge_records_with_cancellation(cancellation, |edge| {
        let mut row = vec![edge.id, edge.start_node, edge.end_node, edge.edge_type];
        for column in &schema.columns {
            match format_export_value(
                edge.properties.get(&column.name),
                &column.shape,
                options,
                &column.name,
            ) {
                Ok(value) => row.push(value),
                Err(error) => {
                    failure = Some(error);
                    return Err(StorageError::IterationStopped);
                }
            }
        }
        if let Err(source) = writer.write_record(&row) {
            failure = Some(AdminImportError::WriteExportCsv {
                path: path.clone(),
                source,
            });
            return Err(StorageError::IterationStopped);
        }
        Ok(())
    });
    if let Some(error) = failure {
        return Err(error);
    }
    let streamed = streamed?;
    writer
        .flush()
        .map_err(|source| AdminImportError::WriteExportCsv {
            path,
            source: source.into(),
        })?;
    Ok(streamed)
}

fn format_export_value(
    value: Option<&serde_json::Value>,
    shape: &ExportValueShape,
    options: &Neo4jCsvExportOptions,
    property: &str,
) -> Result<String, AdminImportError> {
    let Some(value) = value else {
        return Ok(String::new());
    };
    if value.is_null() {
        return Err(AdminImportError::UnsupportedExportValue {
            property: property.to_owned(),
            value_type: "null",
        });
    }
    match shape {
        ExportValueShape::Scalar(_) => format_export_scalar(value, property),
        ExportValueShape::Array(_) => {
            let serde_json::Value::Array(values) = value else {
                return Err(AdminImportError::IncompatibleExportValues {
                    property: property.to_owned(),
                });
            };
            values
                .iter()
                .map(|value| format_export_scalar(value, property))
                .collect::<Result<Vec<_>, _>>()
                .map(|values| values.join(&options.array_delimiter.to_string()))
        }
        ExportValueShape::Vector(_) => {
            let serde_json::Value::Array(values) = value else {
                return Err(AdminImportError::IncompatibleExportValues {
                    property: property.to_owned(),
                });
            };
            values
                .iter()
                .map(|value| format_export_scalar(value, property))
                .collect::<Result<Vec<_>, _>>()
                .map(|values| values.join(&options.vector_delimiter.to_string()))
        }
    }
}

fn format_export_scalar(
    value: &serde_json::Value,
    property: &str,
) -> Result<String, AdminImportError> {
    match value {
        serde_json::Value::String(value) => Ok(value.clone()),
        serde_json::Value::Bool(value) => Ok(value.to_string()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        _ => Err(AdminImportError::IncompatibleExportValues {
            property: property.to_owned(),
        }),
    }
}

fn format_vector(vector: &[f32], delimiter: char) -> String {
    vector
        .iter()
        .map(f32::to_string)
        .collect::<Vec<_>>()
        .join(&delimiter.to_string())
}

/// Import nodes and relationships into a sibling staging database, promoting
/// the completed graph only after every source passes validation.
pub fn import_offline(
    target: impl AsRef<Path>,
    options: &ImportOptions,
    cancellation: &RequestCancellation,
) -> Result<ImportReport, AdminImportError> {
    let schema_metadata = options
        .schema_file
        .as_deref()
        .map(read_schema_metadata)
        .transpose()?;
    preflight_import(options, cancellation)?;
    let staging = StorageEngine::start_offline_staging(target)?;
    let nodes_imported = stream_node_record_batches(options, cancellation, |records| {
        write_node_batch(staging.engine(), records)
    })?;
    let (relationships_imported, bad_relationships) =
        stream_relationship_record_batches(options, cancellation, staging.engine(), |records| {
            write_relationship_batch(staging.engine(), records)
        })?;
    if let Some(schema_metadata) = schema_metadata.as_ref() {
        apply_schema_metadata(staging.engine(), schema_metadata, cancellation)?;
    }
    check_cancelled(cancellation)?;
    staging.promote()?;
    Ok(ImportReport {
        database_name: options.database_name.clone(),
        nodes_imported,
        relationships_imported,
        bad_relationships,
    })
}

/// Import node-only sources into a sibling staging database and promote it on success.
///
/// This is deliberately unavailable for relationship sources until endpoint
/// validation and the temporary ID map are implemented. Any decoding or
/// storage failure drops the staging directory without exposing target data.
pub fn import_nodes_offline(
    target: impl AsRef<Path>,
    options: &ImportOptions,
    cancellation: &RequestCancellation,
) -> Result<NodeImportReport, AdminImportError> {
    if !options.relationship_sources.is_empty() {
        return Err(AdminImportError::RelationshipImportNotImplemented);
    }
    preflight_import(options, cancellation)?;
    let staging = StorageEngine::start_offline_staging(target)?;
    let nodes_imported = stream_node_record_batches(options, cancellation, |records| {
        write_node_batch(staging.engine(), records)
    })?;
    check_cancelled(cancellation)?;
    staging.promote()?;
    Ok(NodeImportReport {
        database_name: options.database_name.clone(),
        nodes_imported,
    })
}

fn write_node_batch(
    engine: &StorageEngine,
    records: &[NodeRecord],
) -> Result<(), AdminImportError> {
    let mut chunk_ids = BTreeSet::new();
    for record in records {
        if !chunk_ids.insert(record.id.clone()) || engine.get_node_record(&record.id)?.is_some() {
            return Err(AdminImportError::DuplicateNodeId(record.id.clone()));
        }
    }
    engine.put_node_records_batch(records)?;
    Ok(())
}

fn write_relationship_batch(
    engine: &StorageEngine,
    records: &[EdgeRecord],
) -> Result<(), AdminImportError> {
    let mut chunk_ids = BTreeSet::new();
    for record in records {
        if !chunk_ids.insert(record.id.clone()) || engine.get_edge_record(&record.id)?.is_some() {
            return Err(AdminImportError::DuplicateRelationshipId(record.id.clone()));
        }
    }
    engine.put_edge_records_batch(records)?;
    Ok(())
}

/// Stream node records in bounded chunks after successful source preflight.
///
/// The caller owns the destination. This keeps pre-staging decoding separate
/// from the later atomic promotion boundary, so a parse failure never exposes
/// a partially imported live database.
pub fn stream_node_record_batches<F>(
    options: &ImportOptions,
    cancellation: &RequestCancellation,
    mut visit: F,
) -> Result<u64, AdminImportError>
where
    F: FnMut(&[NodeRecord]) -> Result<(), AdminImportError>,
{
    if options.chunk_size == 0 {
        return Err(AdminImportError::InvalidChunkSize);
    }
    let report = preflight_import(options, cancellation)?;
    let mut records = Vec::with_capacity(options.chunk_size);
    let mut streamed = 0;
    let mut anonymous_node_sequence = 0_u64;
    for source in &report.node_sources {
        with_source_reader(&source.path, source.format, |reader| {
            let mut csv_reader = csv::ReaderBuilder::new()
                .has_headers(false)
                .delimiter(options.delimiter)
                .quote(options.quote)
                .from_reader(reader);
            let header = csv_reader
                .records()
                .next()
                .ok_or_else(|| AdminImportError::EmptySource {
                    kind: "CSV",
                    path: source.path.clone(),
                })?
                .map_err(|source_error| AdminImportError::ReadCsvHeader {
                    path: source.path.clone(),
                    source: source_error,
                })?;
            let fields = header.iter().map(str::to_owned).collect::<Vec<_>>();
            let columns =
                parse_neo4j_header(&fields, Neo4jHeaderTarget::Node).map_err(|header_error| {
                    AdminImportError::InvalidHeader {
                        path: source.path.clone(),
                        source: header_error,
                    }
                })?;
            for (index, row) in csv_reader.records().enumerate() {
                check_cancelled(cancellation)?;
                let row_number = index as u64 + 2;
                let row = row.map_err(|source_error| AdminImportError::ReadCsvRow {
                    path: source.path.clone(),
                    row: row_number,
                    source: source_error,
                })?;
                if row.len() != columns.len() {
                    return Err(AdminImportError::ColumnCountMismatch {
                        path: source.path.clone(),
                        row: row_number,
                        expected: columns.len(),
                        actual: row.len(),
                    });
                }
                records.push(node_record_from_row(
                    row.iter().collect::<Vec<_>>().as_slice(),
                    &columns,
                    options,
                    &source.path,
                    row_number,
                    anonymous_node_sequence,
                )?);
                if !columns
                    .iter()
                    .any(|column| column.kind == Neo4jColumnKind::Id)
                {
                    anonymous_node_sequence += 1;
                }
                if records.len() == options.chunk_size {
                    visit(&records)?;
                    streamed += records.len() as u64;
                    records.clear();
                }
            }
            Ok(())
        })?;
    }
    if !records.is_empty() {
        check_cancelled(cancellation)?;
        visit(&records)?;
        streamed += records.len() as u64;
    }
    Ok(streamed)
}

fn stream_relationship_record_batches<F>(
    options: &ImportOptions,
    cancellation: &RequestCancellation,
    nodes: &StorageEngine,
    mut visit: F,
) -> Result<(u64, u64), AdminImportError>
where
    F: FnMut(&[EdgeRecord]) -> Result<(), AdminImportError>,
{
    if options.chunk_size == 0 {
        return Err(AdminImportError::InvalidChunkSize);
    }
    let report = preflight_import(options, cancellation)?;
    let mut records = Vec::with_capacity(options.chunk_size);
    let mut streamed = 0;
    let mut bad_relationships = 0;
    let mut generated_id = 0_u64;
    for source in &report.relationship_sources {
        with_source_reader(&source.path, source.format, |reader| {
            let mut csv_reader = csv::ReaderBuilder::new()
                .has_headers(false)
                .delimiter(options.delimiter)
                .quote(options.quote)
                .from_reader(reader);
            let header = csv_reader
                .records()
                .next()
                .ok_or_else(|| AdminImportError::EmptySource {
                    kind: "CSV",
                    path: source.path.clone(),
                })?
                .map_err(|source_error| AdminImportError::ReadCsvHeader {
                    path: source.path.clone(),
                    source: source_error,
                })?;
            let fields = header.iter().map(str::to_owned).collect::<Vec<_>>();
            let columns = parse_neo4j_header(&fields, Neo4jHeaderTarget::Relationship).map_err(
                |header_error| AdminImportError::InvalidHeader {
                    path: source.path.clone(),
                    source: header_error,
                },
            )?;
            validate_relationship_columns(&columns, &source.path)?;
            for (index, row) in csv_reader.records().enumerate() {
                check_cancelled(cancellation)?;
                let row_number = index as u64 + 2;
                let row = row.map_err(|source_error| AdminImportError::ReadCsvRow {
                    path: source.path.clone(),
                    row: row_number,
                    source: source_error,
                })?;
                if row.len() != columns.len() {
                    return Err(AdminImportError::ColumnCountMismatch {
                        path: source.path.clone(),
                        row: row_number,
                        expected: columns.len(),
                        actual: row.len(),
                    });
                }
                let row_generated_id = generated_id;
                generated_id += 1;
                let edge = edge_record_from_row(
                    row.iter().collect::<Vec<_>>().as_slice(),
                    &columns,
                    options,
                    nodes,
                    &source.path,
                    row_number,
                    row_generated_id,
                );
                let edge = match edge {
                    Ok(edge) => edge,
                    Err(error) if is_bad_relationship_error(&error) => {
                        bad_relationships += 1;
                        if options.skip_bad_relationships
                            || (options.bad_relationship_tolerance > 0
                                && bad_relationships <= options.bad_relationship_tolerance as u64)
                        {
                            continue;
                        }
                        return Err(error);
                    }
                    Err(error) => return Err(error),
                };
                records.push(edge);
                if records.len() == options.chunk_size {
                    visit(&records)?;
                    streamed += records.len() as u64;
                    records.clear();
                }
            }
            Ok(())
        })?;
    }
    if !records.is_empty() {
        check_cancelled(cancellation)?;
        visit(&records)?;
        streamed += records.len() as u64;
    }
    Ok((streamed, bad_relationships))
}

fn is_bad_relationship_error(error: &AdminImportError) -> bool {
    matches!(
        error,
        AdminImportError::MissingRelationshipEndpoint { .. }
            | AdminImportError::MissingRelationshipNode { .. }
            | AdminImportError::MissingRelationshipType { .. }
    )
}

fn validate_relationship_columns(
    columns: &[Neo4jColumn],
    path: &Path,
) -> Result<(), AdminImportError> {
    for (kind, name) in [
        (Neo4jColumnKind::StartId, ":START_ID"),
        (Neo4jColumnKind::EndId, ":END_ID"),
    ] {
        if columns.iter().filter(|column| column.kind == kind).count() == 0 {
            return Err(AdminImportError::InvalidRelationshipColumns {
                path: path.to_path_buf(),
                column: name,
            });
        }
    }
    if columns
        .iter()
        .filter(|column| column.kind == Neo4jColumnKind::Id)
        .count()
        > 1
    {
        return Err(AdminImportError::InvalidRelationshipColumns {
            path: path.to_path_buf(),
            column: ":ID",
        });
    }
    Ok(())
}

fn edge_record_from_row(
    row: &[&str],
    columns: &[Neo4jColumn],
    options: &ImportOptions,
    nodes: &StorageEngine,
    path: &Path,
    row_number: u64,
    generated_id: u64,
) -> Result<EdgeRecord, AdminImportError> {
    let mut id = Vec::new();
    let mut start_node = Vec::new();
    let mut end_node = Vec::new();
    let mut edge_type = None;
    let mut properties = BTreeMap::new();
    let value_options = Neo4jValueOptions {
        array_delimiter: options.array_delimiter,
        vector_delimiter: options.vector_delimiter,
        empty_strings_as_null: options.empty_strings_as_null,
    };
    for (value, column) in row.iter().zip(columns) {
        match column.kind {
            Neo4jColumnKind::Id => id.push((column.id_space.as_deref(), *value)),
            Neo4jColumnKind::StartId => start_node.push((column.id_space.as_deref(), *value)),
            Neo4jColumnKind::EndId => end_node.push((column.id_space.as_deref(), *value)),
            Neo4jColumnKind::Type => edge_type = Some((*value).to_owned()),
            Neo4jColumnKind::Property => {
                let parsed = parse_neo4j_value(value, column, value_options).map_err(|source| {
                    AdminImportError::InvalidRelationshipValue {
                        path: path.to_path_buf(),
                        row: row_number,
                        column: column.name.clone(),
                        source,
                    }
                })?;
                properties.insert(column.name.clone(), value_to_json(parsed));
            }
            Neo4jColumnKind::Ignore => {}
            Neo4jColumnKind::Label | Neo4jColumnKind::NamedEmbedding => {
                unreachable!("relationship headers cannot include node-only columns")
            }
        }
    }
    let start_node = import_identifier(&start_node, "start", path, row_number)?;
    let end_node = import_identifier(&end_node, "end", path, row_number)?;
    if nodes.get_node_record(&start_node)?.is_none() {
        return Err(AdminImportError::MissingRelationshipNode {
            path: path.to_path_buf(),
            row: row_number,
            endpoint: "start",
            id: start_node,
        });
    }
    if nodes.get_node_record(&end_node)?.is_none() {
        return Err(AdminImportError::MissingRelationshipNode {
            path: path.to_path_buf(),
            row: row_number,
            endpoint: "end",
            id: end_node,
        });
    }
    let edge_type = edge_type.filter(|value| !value.is_empty()).ok_or_else(|| {
        AdminImportError::MissingRelationshipType {
            path: path.to_path_buf(),
            row: row_number,
        }
    })?;
    let id = if id.is_empty() {
        format!("rel_{generated_id}")
    } else {
        import_identifier(&id, "relationship", path, row_number)?
    };
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    Ok(EdgeRecord {
        id,
        start_node,
        end_node,
        edge_type,
        properties,
        created_at_unix_ms: timestamp,
        updated_at_unix_ms: timestamp,
    })
}

fn import_identifier(
    components: &[(Option<&str>, &str)],
    name: &'static str,
    path: &Path,
    row: u64,
) -> Result<String, AdminImportError> {
    if components.is_empty() || components.iter().any(|(_, value)| value.is_empty()) {
        return Err(AdminImportError::MissingRelationshipEndpoint {
            path: path.to_path_buf(),
            row,
            endpoint: name,
        });
    }
    if components.len() == 1 && components[0].0.is_none() {
        return Ok(components[0].1.to_owned());
    }
    serde_json::to_string(
        &components
            .iter()
            .map(|(space, value)| (space.unwrap_or_default(), *value))
            .collect::<Vec<_>>(),
    )
    .map(|value| format!("__copperdb_import_id__{value}"))
    .map_err(|_| AdminImportError::MissingRelationshipEndpoint {
        path: path.to_path_buf(),
        row,
        endpoint: name,
    })
}

fn node_record_from_row(
    row: &[&str],
    columns: &[Neo4jColumn],
    options: &ImportOptions,
    path: &Path,
    row_number: u64,
    anonymous_node_sequence: u64,
) -> Result<NodeRecord, AdminImportError> {
    let mut id = Vec::new();
    let mut labels = Vec::new();
    let mut properties = BTreeMap::new();
    let mut named_embeddings = BTreeMap::new();
    let value_options = Neo4jValueOptions {
        array_delimiter: options.array_delimiter,
        vector_delimiter: options.vector_delimiter,
        empty_strings_as_null: options.empty_strings_as_null,
    };
    for (value, column) in row.iter().zip(columns) {
        match column.kind {
            Neo4jColumnKind::Id => {
                id.push((column.id_space.as_deref(), *value));
                if !column.name.is_empty() {
                    properties.insert(column.name.clone(), serde_json::json!(*value));
                }
            }
            Neo4jColumnKind::Label => labels.extend(
                value
                    .split(options.array_delimiter)
                    .filter(|label| !label.is_empty())
                    .map(str::to_owned),
            ),
            Neo4jColumnKind::Property => {
                let parsed = parse_neo4j_value(value, column, value_options).map_err(|source| {
                    AdminImportError::InvalidNodeValue {
                        path: path.to_path_buf(),
                        row: row_number,
                        column: column.name.clone(),
                        source,
                    }
                })?;
                properties.insert(column.name.clone(), value_to_json(parsed));
            }
            Neo4jColumnKind::NamedEmbedding => {
                let parsed = parse_neo4j_value(value, column, value_options).map_err(|source| {
                    AdminImportError::InvalidNodeValue {
                        path: path.to_path_buf(),
                        row: row_number,
                        column: column.name.clone(),
                        source,
                    }
                })?;
                named_embeddings.insert(
                    column.name.clone(),
                    value_to_embedding(parsed, column, path, row_number)?,
                );
            }
            Neo4jColumnKind::Ignore => {}
            Neo4jColumnKind::StartId | Neo4jColumnKind::EndId | Neo4jColumnKind::Type => {
                unreachable!("node headers cannot include relationship columns")
            }
        }
    }
    let id = if id.is_empty() {
        format!("_anon_{anonymous_node_sequence}")
    } else {
        import_identifier(&id, "node", path, row_number).map_err(|_| {
            AdminImportError::MissingNodeId {
                path: path.to_path_buf(),
                row: row_number,
            }
        })?
    };
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    Ok(NodeRecord {
        id,
        labels,
        properties,
        named_embeddings,
        chunk_embeddings: Vec::new(),
        embed_meta: NodeEmbeddingMetadata::default(),
        created_at_unix_ms: timestamp,
        updated_at_unix_ms: timestamp,
    })
}

fn value_to_json(value: Value) -> serde_json::Value {
    serde_json::to_value(value).expect("Neo4j CSV values are serializable")
}

fn value_to_embedding(
    value: Value,
    column: &Neo4jColumn,
    path: &Path,
    row: u64,
) -> Result<Vec<f32>, AdminImportError> {
    let Value::List(values) = value else {
        unreachable!("vector conversion always returns a list")
    };
    values
        .into_iter()
        .map(|value| match value {
            Value::Float(value) => Ok(value as f32),
            _ => Err(AdminImportError::InvalidNodeValue {
                path: path.to_path_buf(),
                row,
                column: column.name.clone(),
                source: copperdb_convert::ConvertError::InvalidNeo4jValue {
                    value_type: "vector".into(),
                    value: "non-float vector component".into(),
                },
            }),
        })
        .collect()
}

pub fn preflight_import(
    options: &ImportOptions,
    cancellation: &RequestCancellation,
) -> Result<PreflightReport, AdminImportError> {
    check_cancelled(cancellation)?;
    validate_database_name(&options.database_name)?;
    if options.node_sources.is_empty() {
        return Err(AdminImportError::MissingNodeSource);
    }

    let data_directory = canonical_directory(&options.data_directory)?;
    validate_report_path(options.report_file.as_deref(), &data_directory)?;

    let mut seen = BTreeSet::new();
    let node_sources = preflight_sources(
        &options.node_sources,
        "node",
        Neo4jHeaderTarget::Node,
        options.delimiter,
        options.quote,
        &mut seen,
        cancellation,
    )?;
    let relationship_sources = preflight_sources(
        &options.relationship_sources,
        "relationship",
        Neo4jHeaderTarget::Relationship,
        options.delimiter,
        options.quote,
        &mut seen,
        cancellation,
    )?;

    Ok(PreflightReport {
        database_name: options.database_name.clone(),
        node_sources,
        relationship_sources,
        status: PreflightStatus::Ready,
    })
}

fn validate_database_name(database_name: &str) -> Result<(), AdminImportError> {
    if database_name.is_empty() {
        return Err(AdminImportError::MissingDatabaseName);
    }
    if database_name
        .chars()
        .any(|character| !(character.is_ascii_alphanumeric() || matches!(character, '_' | '-')))
    {
        return Err(AdminImportError::UnsafeDatabaseName(
            database_name.to_owned(),
        ));
    }
    Ok(())
}

fn canonical_directory(path: &Path) -> Result<PathBuf, AdminImportError> {
    let metadata = path
        .metadata()
        .map_err(|source| AdminImportError::InspectSource {
            path: path.to_path_buf(),
            source,
        })?;
    if !metadata.is_dir() {
        return Err(AdminImportError::SourceNotFile {
            kind: "data directory",
            path: path.to_path_buf(),
        });
    }
    path.canonicalize()
        .map_err(|source| AdminImportError::InspectSource {
            path: path.to_path_buf(),
            source,
        })
}

fn validate_report_path(
    report_file: Option<&Path>,
    data_directory: &Path,
) -> Result<(), AdminImportError> {
    let Some(report_file) = report_file else {
        return Ok(());
    };
    let report_path = if report_file.is_absolute() {
        report_file.to_path_buf()
    } else {
        data_directory.join(report_file)
    };
    if !report_path.starts_with(data_directory) {
        return Err(AdminImportError::UnsafeReportPath {
            path: report_file.to_path_buf(),
        });
    }
    Ok(())
}

fn preflight_sources(
    paths: &[PathBuf],
    kind: &'static str,
    target: Neo4jHeaderTarget,
    delimiter: u8,
    quote: u8,
    seen: &mut BTreeSet<PathBuf>,
    cancellation: &RequestCancellation,
) -> Result<Vec<SourceMetadata>, AdminImportError> {
    paths
        .iter()
        .map(|path| {
            check_cancelled(cancellation)?;
            let metadata = path.metadata().map_err(|source| match source.kind() {
                std::io::ErrorKind::NotFound => AdminImportError::MissingSource {
                    kind,
                    path: path.clone(),
                },
                _ => AdminImportError::InspectSource {
                    path: path.clone(),
                    source,
                },
            })?;
            if !metadata.is_file() {
                return Err(AdminImportError::SourceNotFile {
                    kind,
                    path: path.clone(),
                });
            }
            if metadata.len() == 0 {
                return Err(AdminImportError::EmptySource {
                    kind,
                    path: path.clone(),
                });
            }
            File::open(path).map_err(|source| AdminImportError::InspectSource {
                path: path.clone(),
                source,
            })?;
            let path = path
                .canonicalize()
                .map_err(|source| AdminImportError::InspectSource {
                    path: path.clone(),
                    source,
                })?;
            if !seen.insert(path.clone()) {
                return Err(AdminImportError::DuplicateSource { path });
            }
            let format = source_format(&path)?;
            let header = read_header(&path, format, target, delimiter, quote)?;
            Ok(SourceMetadata {
                path,
                format,
                bytes: metadata.len(),
                header,
            })
        })
        .collect()
}

fn source_format(path: &Path) -> Result<SourceFormat, AdminImportError> {
    let path_text = path.to_string_lossy().to_ascii_lowercase();
    if path_text.ends_with(".csv") {
        Ok(SourceFormat::Csv)
    } else if path_text.ends_with(".csv.gz") || path_text.ends_with(".gz") {
        Ok(SourceFormat::GzipCsv)
    } else if path_text.ends_with(".zip") {
        Ok(SourceFormat::Zip)
    } else {
        Err(AdminImportError::UnsupportedSourceFormat {
            path: path.to_path_buf(),
        })
    }
}

fn check_cancelled(cancellation: &RequestCancellation) -> Result<(), AdminImportError> {
    cancellation
        .check_cancelled()
        .map_err(|_| AdminImportError::Cancelled)
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use super::*;
    use tempfile::tempdir;

    fn options(data_directory: &Path, source: PathBuf) -> ImportOptions {
        ImportOptions {
            database_name: "northwind".into(),
            node_sources: vec![source],
            relationship_sources: Vec::new(),
            schema_file: None,
            data_directory: data_directory.to_path_buf(),
            report_file: Some(PathBuf::from("reports/import.json")),
            delimiter: b',',
            quote: b'"',
            array_delimiter: ';',
            vector_delimiter: ';',
            empty_strings_as_null: false,
            bad_relationship_tolerance: 0,
            skip_bad_relationships: false,
            chunk_size: 2,
        }
    }

    #[test]
    fn preflight_reports_canonical_csv_sources_deterministically() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("nodes.csv");
        fs::write(&source, ":ID,name\n1,Ada\n").unwrap();
        let report = preflight_import(
            &options(directory.path(), source.clone()),
            &RequestCancellation::new(),
        )
        .unwrap();

        assert_eq!(report.database_name, "northwind");
        assert_eq!(report.status, PreflightStatus::Ready);
        assert_eq!(report.node_sources.len(), 1);
        assert_eq!(report.node_sources[0].path, source.canonicalize().unwrap());
        assert_eq!(report.node_sources[0].format, SourceFormat::Csv);
        assert_eq!(report.node_sources[0].bytes, 15);
        assert_eq!(
            report.node_sources[0].header[0].kind,
            copperdb_convert::Neo4jColumnKind::Id
        );
    }

    #[test]
    fn preflight_rejects_duplicate_sources_across_node_and_relationship_inputs() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("records.csv");
        fs::write(&source, ":ID\n1\n").unwrap();
        let mut options = options(directory.path(), source.clone());
        options.relationship_sources.push(source);

        let error = preflight_import(&options, &RequestCancellation::new()).unwrap_err();
        assert!(matches!(error, AdminImportError::DuplicateSource { .. }));
        assert_eq!(error.exit_code(), EXIT_CSV);
    }

    #[test]
    fn preflight_rejects_unsafe_inputs_and_cancellation() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("nodes.txt");
        fs::write(&source, "not csv").unwrap();
        let mut options = options(directory.path(), source);
        options.database_name = "../northwind".into();
        let error = preflight_import(&options, &RequestCancellation::new()).unwrap_err();
        assert!(matches!(error, AdminImportError::UnsafeDatabaseName(_)));

        options.database_name = "northwind".into();
        let cancellation = RequestCancellation::new();
        cancellation.cancel();
        let error = preflight_import(&options, &cancellation).unwrap_err();
        assert!(matches!(error, AdminImportError::Cancelled));
    }

    #[test]
    fn preflight_reads_custom_delimiter_headers_and_rejects_bad_relationship_headers() {
        let directory = tempdir().unwrap();
        let nodes = directory.path().join("nodes.csv");
        fs::write(&nodes, ":ID;name:string\n1;Ada\n").unwrap();
        let mut options = options(directory.path(), nodes);
        options.delimiter = b';';
        let report = preflight_import(&options, &RequestCancellation::new()).unwrap();
        assert_eq!(report.node_sources[0].header.len(), 2);

        let relationships = directory.path().join("relationships.csv");
        fs::write(&relationships, ":START_ID,:TYPE\n1,KNOWS\n").unwrap();
        options.relationship_sources.push(relationships);
        let error = preflight_import(&options, &RequestCancellation::new()).unwrap_err();
        assert!(matches!(error, AdminImportError::InvalidHeader { .. }));
    }

    #[test]
    fn preflight_reads_gzip_csv_headers() {
        use flate2::{write::GzEncoder, Compression};
        use std::io::Write;

        let directory = tempdir().unwrap();
        let source = directory.path().join("nodes.csv.gz");
        let file = File::create(&source).unwrap();
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder.write_all(b":ID,name:string\n1,Ada\n").unwrap();
        encoder.finish().unwrap();

        let report = preflight_import(
            &options(directory.path(), source),
            &RequestCancellation::new(),
        )
        .unwrap();
        assert_eq!(report.node_sources[0].format, SourceFormat::GzipCsv);
        assert_eq!(report.node_sources[0].header.len(), 2);
    }

    #[test]
    fn preflight_reads_single_safe_zip_csv_header() {
        use std::io::Write;
        use zip::{write::SimpleFileOptions, ZipWriter};

        let directory = tempdir().unwrap();
        let source = directory.path().join("nodes.zip");
        let file = File::create(&source).unwrap();
        let mut archive = ZipWriter::new(file);
        archive
            .start_file("nodes.csv", SimpleFileOptions::default())
            .unwrap();
        archive.write_all(b":ID,name:string\n1,Ada\n").unwrap();
        archive.finish().unwrap();

        let report = preflight_import(
            &options(directory.path(), source),
            &RequestCancellation::new(),
        )
        .unwrap();
        assert_eq!(report.node_sources[0].format, SourceFormat::Zip);
        assert_eq!(report.node_sources[0].header.len(), 2);
    }

    #[test]
    fn streams_typed_node_records_in_bounded_chunks() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("nodes.csv");
        fs::write(
            &source,
            ":ID,:LABEL,age:long,roles:string[],:EMBEDDING(default){dimensions:2}\n1,Person;Author,42,admin;writer,0.1;0.2\n2,Person,7,reader,0.3;0.4\n3,Robot,3,observer,0.5;0.6\n",
        )
        .unwrap();
        let mut chunks = Vec::new();
        let streamed = stream_node_record_batches(
            &options(directory.path(), source),
            &RequestCancellation::new(),
            |records| {
                chunks.push(records.to_vec());
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(streamed, 3);
        assert_eq!(chunks.iter().map(Vec::len).collect::<Vec<_>>(), vec![2, 1]);
        assert_eq!(chunks[0][0].id, "1");
        assert_eq!(chunks[0][0].labels, vec!["Person", "Author"]);
        assert_eq!(chunks[0][0].properties["age"], serde_json::json!(42));
        assert_eq!(
            chunks[0][0].properties["roles"],
            serde_json::json!(["admin", "writer"])
        );
        assert_eq!(chunks[0][0].named_embeddings["default"], vec![0.1, 0.2]);
    }

    #[test]
    fn stream_assigns_anonymous_ids_and_rejects_zero_chunk_size() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("nodes.csv");
        fs::write(&source, "name:string\nAda\n").unwrap();
        let import_options = options(directory.path(), source);
        let mut records = Vec::new();
        stream_node_record_batches(&import_options, &RequestCancellation::new(), |chunk| {
            records.extend_from_slice(chunk);
            Ok(())
        })
        .unwrap();
        assert_eq!(records[0].id, "_anon_0");

        let source = directory.path().join("valid.csv");
        fs::write(&source, ":ID\n1\n").unwrap();
        let mut import_options = options(directory.path(), source);
        import_options.chunk_size = 0;
        let error =
            stream_node_record_batches(&import_options, &RequestCancellation::new(), |_| Ok(()))
                .unwrap_err();
        assert!(matches!(error, AdminImportError::InvalidChunkSize));
    }

    #[test]
    fn imports_node_chunks_through_staging_without_exposing_partial_target() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("nodes.csv");
        fs::write(
            &source,
            ":ID,:LABEL,name:string\n1,Person,Ada\n2,Person,Lin\n",
        )
        .unwrap();
        let target = directory.path().join("target");

        let report = import_nodes_offline(
            &target,
            &options(directory.path(), source),
            &RequestCancellation::new(),
        )
        .unwrap();
        assert_eq!(report.nodes_imported, 2);
        let storage = StorageEngine::open(&target).unwrap();
        assert_eq!(
            storage.get_node_record("1").unwrap().unwrap().properties["name"],
            serde_json::json!("Ada")
        );
        assert_eq!(
            storage.get_node_record("2").unwrap().unwrap().labels,
            vec!["Person"]
        );
    }

    #[test]
    fn node_only_import_rejects_relationship_sources_before_creating_target() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("nodes.csv");
        let relationships = directory.path().join("relationships.csv");
        fs::write(&source, ":ID\n1\n").unwrap();
        fs::write(&relationships, ":START_ID,:END_ID\n1,1\n").unwrap();
        let target = directory.path().join("target");
        let mut options = options(directory.path(), source);
        options.relationship_sources.push(relationships);

        let error =
            import_nodes_offline(&target, &options, &RequestCancellation::new()).unwrap_err();
        assert!(matches!(
            error,
            AdminImportError::RelationshipImportNotImplemented
        ));
        assert!(!target.exists());
    }

    #[test]
    fn staged_node_import_rejects_duplicate_ids_across_chunks_without_promotion() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("nodes.csv");
        fs::write(&source, ":ID,name\n1,Ada\n2,Lin\n1,Duplicate\n").unwrap();
        let target = directory.path().join("target");

        let error = import_nodes_offline(
            &target,
            &options(directory.path(), source),
            &RequestCancellation::new(),
        )
        .unwrap_err();
        assert!(matches!(error, AdminImportError::DuplicateNodeId(id) if id == "1"));
        assert!(!target.exists());
    }

    #[test]
    fn imports_relationship_batches_after_staged_node_resolution() {
        let directory = tempdir().unwrap();
        let nodes = directory.path().join("nodes.csv");
        let relationships = directory.path().join("relationships.csv");
        fs::write(&nodes, ":ID,name:string\n1,Ada\n2,Lin\n3,Sam\n").unwrap();
        fs::write(
            &relationships,
            ":START_ID,:END_ID,:TYPE,weight:double\n1,2,KNOWS,0.9\n2,3,WORKS_WITH,0.5\n",
        )
        .unwrap();
        let target = directory.path().join("target");
        let mut import_options = options(directory.path(), nodes);
        import_options.relationship_sources.push(relationships);

        let report = import_offline(&target, &import_options, &RequestCancellation::new()).unwrap();
        assert_eq!(report.nodes_imported, 3);
        assert_eq!(report.relationships_imported, 2);
        let storage = StorageEngine::open(&target).unwrap();
        let first = storage.get_edge_record("rel_0").unwrap().unwrap();
        assert_eq!(first.start_node, "1");
        assert_eq!(first.end_node, "2");
        assert_eq!(first.edge_type, "KNOWS");
        assert_eq!(first.properties["weight"], serde_json::json!(0.9));
        assert_eq!(
            storage.get_edge_record("rel_1").unwrap().unwrap().edge_type,
            "WORKS_WITH"
        );
    }

    #[test]
    fn imports_composite_space_qualified_ids_and_resolves_relationships() {
        let directory = tempdir().unwrap();
        let nodes = directory.path().join("nodes.csv");
        let relationships = directory.path().join("relationships.csv");
        fs::write(
            &nodes,
            ":ID(Person),:ID(Region),name:string\n1,east,Ada\n1,west,Lin\n",
        )
        .unwrap();
        fs::write(
            &relationships,
            ":START_ID(Person),:START_ID(Region),:END_ID(Person),:END_ID(Region),:TYPE\n1,east,1,west,KNOWS\n",
        )
        .unwrap();
        let target = directory.path().join("target");
        let mut import_options = options(directory.path(), nodes);
        import_options.relationship_sources.push(relationships);

        let report = import_offline(&target, &import_options, &RequestCancellation::new()).unwrap();
        assert_eq!(report.nodes_imported, 2);
        assert_eq!(report.relationships_imported, 1);
        let storage = StorageEngine::open(&target).unwrap();
        let edge = storage.get_edge_record("rel_0").unwrap().unwrap();
        assert_ne!(edge.start_node, edge.end_node);
        assert!(edge.start_node.starts_with("__copperdb_import_id__["));
        assert!(storage.get_node_record(&edge.start_node).unwrap().is_some());
        assert!(storage.get_node_record(&edge.end_node).unwrap().is_some());
    }

    #[test]
    fn failed_relationship_endpoint_validation_does_not_promote_target() {
        let directory = tempdir().unwrap();
        let nodes = directory.path().join("nodes.csv");
        let relationships = directory.path().join("relationships.csv");
        fs::write(&nodes, ":ID\n1\n").unwrap();
        fs::write(&relationships, ":START_ID,:END_ID,:TYPE\n1,missing,KNOWS\n").unwrap();
        let target = directory.path().join("target");
        let mut import_options = options(directory.path(), nodes);
        import_options.relationship_sources.push(relationships);

        let error =
            import_offline(&target, &import_options, &RequestCancellation::new()).unwrap_err();
        assert!(matches!(
            error,
            AdminImportError::MissingRelationshipNode { endpoint: "end", id, .. } if id == "missing"
        ));
        assert!(!target.exists());
    }

    #[test]
    fn full_import_preflights_relationship_sources_before_creating_staging() {
        let directory = tempdir().unwrap();
        let nodes = directory.path().join("nodes.csv");
        let relationships = directory.path().join("relationships.csv");
        let staging_parent_file = directory.path().join("not-a-directory");
        fs::write(&nodes, ":ID\n1\n").unwrap();
        fs::write(&relationships, ":START_ID,:TYPE\n1,KNOWS\n").unwrap();
        fs::write(&staging_parent_file, "not a directory").unwrap();
        let mut import_options = options(directory.path(), nodes);
        import_options.relationship_sources.push(relationships);

        let error = import_offline(
            staging_parent_file.join("target"),
            &import_options,
            &RequestCancellation::new(),
        )
        .unwrap_err();

        assert!(matches!(error, AdminImportError::InvalidHeader { .. }));
    }

    #[test]
    fn staged_import_rejects_duplicate_relationship_ids_across_chunks() {
        let directory = tempdir().unwrap();
        let nodes = directory.path().join("nodes.csv");
        let relationships = directory.path().join("relationships.csv");
        fs::write(&nodes, ":ID\n1\n2\n").unwrap();
        fs::write(
            &relationships,
            ":ID,:START_ID,:END_ID,:TYPE\ne1,1,2,KNOWS\ne2,2,1,KNOWS\ne1,1,2,KNOWS\n",
        )
        .unwrap();
        let target = directory.path().join("target");
        let mut import_options = options(directory.path(), nodes);
        import_options.relationship_sources.push(relationships);

        let error =
            import_offline(&target, &import_options, &RequestCancellation::new()).unwrap_err();
        assert!(matches!(error, AdminImportError::DuplicateRelationshipId(id) if id == "e1"));
        assert!(!target.exists());
    }

    #[test]
    fn relationship_tolerance_skips_bad_rows_and_counts_them() {
        let directory = tempdir().unwrap();
        let nodes = directory.path().join("nodes.csv");
        let relationships = directory.path().join("relationships.csv");
        fs::write(&nodes, ":ID\n1\n2\n").unwrap();
        fs::write(
            &relationships,
            ":START_ID,:END_ID,:TYPE\n1,2,KNOWS\n1,missing,KNOWS\n2,1,KNOWS\n",
        )
        .unwrap();
        let target = directory.path().join("target");
        let mut import_options = options(directory.path(), nodes);
        import_options.relationship_sources.push(relationships);
        import_options.bad_relationship_tolerance = 1;

        let report = import_offline(&target, &import_options, &RequestCancellation::new()).unwrap();
        assert_eq!(report.relationships_imported, 2);
        assert_eq!(report.bad_relationships, 1);
        let storage = StorageEngine::open(&target).unwrap();
        assert!(storage.get_edge_record("rel_0").unwrap().is_some());
        assert!(storage.get_edge_record("rel_2").unwrap().is_some());
    }

    #[test]
    fn schema_validation_failure_does_not_promote_target() {
        let directory = tempdir().unwrap();
        let nodes = directory.path().join("nodes.csv");
        fs::write(
            &nodes,
            ":ID,:LABEL,email:string\nn1,Person,shared@example.com\nn2,Person,shared@example.com\n",
        )
        .unwrap();
        let schema = directory.path().join(NEO4J_CSV_SCHEMA_FILE);
        fs::write(
            &schema,
            serde_json::to_vec_pretty(&SchemaMetadata {
                constraints: vec![copperdb_storage::Constraint {
                    name: "person_email_unique".into(),
                    constraint_type: copperdb_storage::ConstraintType::Unique,
                    entity_type: copperdb_storage::ConstraintEntityType::Node,
                    label: "Person".into(),
                    properties: vec!["email".into()],
                    type_name: None,
                    allowed_values: Vec::new(),
                }],
                indexes: Vec::new(),
                index_options: BTreeMap::new(),
            })
            .unwrap(),
        )
        .unwrap();
        let target = directory.path().join("target");
        let mut import_options = options(directory.path(), nodes);
        import_options.schema_file = Some(schema);

        let error =
            import_offline(&target, &import_options, &RequestCancellation::new()).unwrap_err();
        assert!(matches!(
            error,
            AdminImportError::Storage(StorageError::UniqueConstraintViolation { .. })
        ));
        assert!(!target.exists());
    }

    #[test]
    fn writes_deterministic_import_report_inside_data_directory() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("nodes.csv");
        fs::write(&source, ":ID\n1\n").unwrap();
        let import_options = options(directory.path(), source);
        let report = ImportReport {
            database_name: "northwind".into(),
            nodes_imported: 2,
            relationships_imported: 1,
            bad_relationships: 0,
        };

        write_import_report(&import_options, &report).unwrap();
        let path = directory.path().join("reports/import.json");
        assert_eq!(
            fs::read_to_string(path).unwrap(),
            "{\n  \"database_name\": \"northwind\",\n  \"nodes_imported\": 2,\n  \"relationships_imported\": 1,\n  \"bad_relationships\": 0\n}"
        );
    }

    #[test]
    fn exports_deterministic_neo4j_csv_and_round_trips_supported_records() {
        let directory = tempdir().unwrap();
        let engine = StorageEngine::open_temporary().unwrap();
        engine
            .put_node_record(&NodeRecord {
                id: "n2".into(),
                labels: vec!["Person".into()],
                properties: BTreeMap::from([
                    ("age".into(), serde_json::json!(42)),
                    ("name".into(), serde_json::json!("Lin")),
                    ("roles".into(), serde_json::json!(["reader", "writer"])),
                    ("scores".into(), serde_json::json!([0.3, 0.4])),
                ]),
                named_embeddings: BTreeMap::from([("default".into(), vec![0.3, 0.4])]),
                chunk_embeddings: Vec::new(),
                embed_meta: NodeEmbeddingMetadata::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        engine
            .put_node_record(&NodeRecord {
                id: "n1".into(),
                labels: vec!["Author".into(), "Person".into()],
                properties: BTreeMap::from([
                    ("age".into(), serde_json::json!(7)),
                    ("name".into(), serde_json::json!("Ada")),
                    ("roles".into(), serde_json::json!(["admin"])),
                    ("scores".into(), serde_json::json!([0.1, 0.2])),
                ]),
                named_embeddings: BTreeMap::from([("default".into(), vec![0.1, 0.2])]),
                chunk_embeddings: Vec::new(),
                embed_meta: NodeEmbeddingMetadata::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        engine
            .put_edge_record(&EdgeRecord {
                id: "e1".into(),
                start_node: "n1".into(),
                end_node: "n2".into(),
                edge_type: "KNOWS".into(),
                properties: BTreeMap::from([("weight".into(), serde_json::json!(0.9))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        engine
            .persist_constraint(&copperdb_storage::Constraint {
                name: "person_name_exists".into(),
                constraint_type: copperdb_storage::ConstraintType::Exists,
                entity_type: copperdb_storage::ConstraintEntityType::Node,
                label: "Person".into(),
                properties: vec!["name".into()],
                type_name: None,
                allowed_values: Vec::new(),
            })
            .unwrap();
        engine
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "person_age".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Person".into(),
                properties: vec!["age".into()],
                kind: copperdb_storage::IndexKind::Range,
            })
            .unwrap();

        let output = directory.path().join("export");
        let report = export_neo4j_csv(
            &engine,
            &Neo4jCsvExportOptions::new(&output),
            &RequestCancellation::new(),
        )
        .unwrap();
        assert_eq!(report.nodes_exported, 2);
        assert_eq!(report.relationships_exported, 1);
        assert_eq!(
            fs::read_to_string(output.join("nodes.csv")).unwrap(),
            ":ID,:LABEL,:EMBEDDING(default){dimensions:2},age:long,name:string,roles:string[],scores:vector{dimensions:2}\nn1,Author;Person,0.1;0.2,7,Ada,admin,0.1;0.2\nn2,Person,0.3;0.4,42,Lin,reader;writer,0.3;0.4\n"
        );
        assert_eq!(
            fs::read_to_string(output.join("relationships.csv")).unwrap(),
            ":ID,:START_ID,:END_ID,:TYPE,weight:double\ne1,n1,n2,KNOWS,0.9\n"
        );
        assert!(output.join(NEO4J_CSV_SCHEMA_FILE).exists());

        let target = directory.path().join("round-trip");
        let mut import_options = options(directory.path(), output.join("nodes.csv"));
        import_options
            .relationship_sources
            .push(output.join("relationships.csv"));
        import_options.schema_file = Some(output.join(NEO4J_CSV_SCHEMA_FILE));
        import_options.report_file = None;
        import_offline(&target, &import_options, &RequestCancellation::new()).unwrap();
        let imported = StorageEngine::open(&target).unwrap();
        assert_eq!(
            imported.get_node_record("n1").unwrap().unwrap().properties,
            engine.get_node_record("n1").unwrap().unwrap().properties
        );
        assert_eq!(
            imported
                .get_node_record("n2")
                .unwrap()
                .unwrap()
                .named_embeddings["default"],
            vec![0.3, 0.4]
        );
        assert_eq!(
            imported.get_edge_record("e1").unwrap().unwrap().properties,
            engine.get_edge_record("e1").unwrap().unwrap().properties
        );
        assert_eq!(
            imported.load_constraints().unwrap(),
            engine.load_constraints().unwrap()
        );
        assert_eq!(
            imported.load_index_definitions().unwrap(),
            engine.load_index_definitions().unwrap()
        );
    }

    #[test]
    fn export_rejects_existing_output_and_cancellation_without_creating_output() {
        let directory = tempdir().unwrap();
        let engine = StorageEngine::open_temporary().unwrap();
        let output = directory.path().join("export");
        fs::create_dir(&output).unwrap();

        let error = export_neo4j_csv(
            &engine,
            &Neo4jCsvExportOptions::new(&output),
            &RequestCancellation::new(),
        )
        .unwrap_err();
        assert!(matches!(error, AdminImportError::ExportOutputExists { .. }));

        let cancelled_output = directory.path().join("cancelled-export");
        let cancellation = RequestCancellation::new();
        cancellation.cancel();
        let error = export_neo4j_csv(
            &engine,
            &Neo4jCsvExportOptions::new(&cancelled_output),
            &cancellation,
        )
        .unwrap_err();
        assert!(matches!(error, AdminImportError::Cancelled));
        assert!(!cancelled_output.exists());
    }
}

fn read_header(
    path: &Path,
    format: SourceFormat,
    target: Neo4jHeaderTarget,
    delimiter: u8,
    quote: u8,
) -> Result<Vec<Neo4jColumn>, AdminImportError> {
    with_source_reader(path, format, |reader| {
        read_csv_header(path, reader, target, delimiter, quote)
    })
}

fn with_source_reader<T>(
    path: &Path,
    format: SourceFormat,
    read: impl FnOnce(&mut dyn Read) -> Result<T, AdminImportError>,
) -> Result<T, AdminImportError> {
    if format != SourceFormat::Zip {
        let file = File::open(path).map_err(|source| AdminImportError::InspectSource {
            path: path.to_path_buf(),
            source,
        })?;
        let mut reader: Box<dyn Read> = match format {
            SourceFormat::Csv => Box::new(file),
            SourceFormat::GzipCsv => Box::new(flate2::read::GzDecoder::new(file)),
            SourceFormat::Zip => unreachable!(),
        };
        return read(reader.as_mut());
    }
    let file = File::open(path).map_err(|source| AdminImportError::InspectSource {
        path: path.to_path_buf(),
        source,
    })?;
    let mut archive =
        zip::ZipArchive::new(file).map_err(|source| AdminImportError::InvalidZip {
            path: path.to_path_buf(),
            source,
        })?;
    if archive.len() != 1 {
        return Err(AdminImportError::AmbiguousZipSource {
            path: path.to_path_buf(),
        });
    }
    let entry = archive
        .by_index(0)
        .map_err(|source| AdminImportError::InvalidZip {
            path: path.to_path_buf(),
            source,
        })?;
    if !is_safe_zip_entry_name(entry.name()) || entry.is_dir() {
        return Err(AdminImportError::UnsafeZipEntry {
            path: path.to_path_buf(),
        });
    }
    if entry.size() > MAX_ZIP_ENTRY_BYTES {
        return Err(AdminImportError::OversizedZipEntry {
            path: path.to_path_buf(),
            limit: MAX_ZIP_ENTRY_BYTES,
        });
    }
    let mut entry = entry;
    read(&mut entry)
}

fn is_safe_zip_entry_name(name: &str) -> bool {
    let path = Path::new(name);
    !path.is_absolute()
        && path.components().all(|component| {
            matches!(
                component,
                std::path::Component::Normal(_) | std::path::Component::CurDir
            )
        })
}

fn read_csv_header<R: Read + ?Sized>(
    path: &Path,
    reader: &mut R,
    target: Neo4jHeaderTarget,
    delimiter: u8,
    quote: u8,
) -> Result<Vec<Neo4jColumn>, AdminImportError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .delimiter(delimiter)
        .quote(quote)
        .from_reader(BufReader::new(reader));
    let header = reader
        .records()
        .next()
        .ok_or_else(|| AdminImportError::EmptySource {
            kind: "CSV",
            path: path.to_path_buf(),
        })?
        .map_err(|source| AdminImportError::ReadCsvHeader {
            path: path.to_path_buf(),
            source,
        })?;
    let fields = header.iter().map(str::to_owned).collect::<Vec<_>>();
    parse_neo4j_header(&fields, target).map_err(|source| AdminImportError::InvalidHeader {
        path: path.to_path_buf(),
        source,
    })
}
