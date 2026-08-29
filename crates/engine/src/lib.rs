//! copperdb core engine.
//!
//! This is the primary entry point crate that integrates all subsystems
//! into a unified graph database engine. It is the Rust equivalent of
//! NornicDB's `pkg/nornicdb` package.
//!
//! # Architecture
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                        copperdb                              │
//! │                                                             │
//! │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌───────────┐  │
//! │  │  server  │  │   bolt   │  │  graphql │  │    mcp    │  │
//! │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └─────┬─────┘  │
//! │       └─────────────┴─────────────┴──────────────┘        │
//! │                           ↓                                │
//! │              ┌────────────────────────┐                    │
//! │              │      auth / security    │                    │
//! │              └────────────┬───────────┘                    │
//! │                           ↓                                │
//! │  ┌────────────────────────────────────────────────────┐    │
//! │  │                     eval                            │    │
//! │  │   ┌──────────┐  ┌──────────┐  ┌──────────────────┐│    │
//! │  │   │  cypher  │  │  filter  │  │     indexing     ││    │
//! │  │   └──────────┘  └──────────┘  └──────────────────┘│    │
//! │  └────────────────────────┬───────────────────────────┘    │
//! │                           ↓                                │
//! │  ┌────────────────────────────────────────────────────┐    │
//! │  │                    storage                          │    │
//! │  │      ┌──────────┐  ┌──────────┐  ┌──────────────┐ │    │
//! │  │      │ temporal │  │   decay  │  │ replication  │ │    │
//! │  │      └──────────┘  └──────────┘  └──────────────┘ │    │
//! │  └────────────────────────────────────────────────────┘    │
//! │                                                             │
//! │  ┌──────────────────┐  ┌──────────────┐  ┌─────────────┐  │
//! │  │   vectorspace    │  │     embed    │  │     gpu     │  │
//! │  └──────────────────┘  └──────────────┘  └─────────────┘  │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use crate::embedding_runtime::EmbeddingRuntime;
pub use crate::embedding_runtime::{
    EmbeddingOperationalStatus, EmbeddingRuntimeState, EmbeddingRuntimeStatus,
};
use crate::vector_indexes::VectorIndexManager;
use copperdb_audit::{AuditConfig, AuditLog, Event, EventType};
use copperdb_cache::{is_cacheable_read_query, QueryCache, QueryResultCache};
use copperdb_compliance::{ComplianceManager, ComplianceReporter};
use copperdb_cypher::{
    can_execute_as_pipeline, detect_query_pattern, match_compound_query_shape, Clause,
    EdgeDirection, Expression, LiteralValue, Parser, Pattern, QueryType, ReturnItem, SetItem,
    WhereClause, WithClause,
};
use copperdb_eval::{EvalEngine, QueryStats};
use copperdb_fabric::{
    merge_fabric_aggregates, merge_fabric_paths, merge_fabric_rows, FabricAggregateOptions,
    FabricMergedPaths, FabricMergedRows, FabricPathBatch, FabricPathMergeOptions, FabricReadPlan,
    FabricReadRequest, FabricRowBatch, FabricRowMergeOptions, FabricTopology,
};
use copperdb_filter::{eval_expression, eval_predicate};
use copperdb_kms::{new_provider, ProviderFactoryConfig};
use copperdb_replication::{
    CassandraCoordinator, Command, DistributedReadOutcome, DistributedWriteOutcome,
    DurableRepairQueue, RepairReplayReport, RepairWorkerConfig, ReplicaTransport, ReplicationError,
    ScheduledRepairWorker,
};
use copperdb_search::{
    collect_fabric_hydration_records_with_context,
    collect_planned_fabric_ranked_batches_with_context, execute_planned_fabric_ranked_search,
    hydrate_rrf_search_outcome, merge_rrf_search_batches, FabricHydrationRequest,
    FabricRankedSearchExecution, HydrationTransport, RankedSearchTransport, RrfConfig,
    RrfHydratedSearchOutcome, RrfHydrationRecord, RrfSearchBatch, RrfSearchHit, RrfSearchOutcome,
    RrfSearchPolicy, SearchQuery, SearchResult,
};
use copperdb_storage::{
    EdgeRecord, IndexEntityType, IndexKind, KnowledgePolicyAccessMetadata, NodeRecord,
    StorageEngine, StorageTransaction,
};
use copperdb_topology::{
    ConsistencyLevel, DistributedReadPlan, DistributedSearchPlan, DistributedWriteMode,
    DistributedWritePlan, FabricDatabase, FabricGlobalId, LogicalTransactionId, PlacementKey,
    TopologyRegistry,
};
use copperdb_txsession::{SessionConfig, TransactionManager, TxError};
use copperdb_util::RequestCancelled;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;

// ─── Errors ──────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum CopperDbError {
    #[error("storage error: {0}")]
    Storage(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("eval error: {0}")]
    Eval(String),
    #[error("initialization error: {0}")]
    Init(String),
    #[error("audit error: {0}")]
    Audit(String),
    #[error("compliance error: {0}")]
    Compliance(String),
    #[error("replication error: {0}")]
    Replication(String),
    #[error("configuration error: {0}")]
    Config(String),
    #[error(transparent)]
    RequestCancelled(#[from] RequestCancelled),
}

impl From<copperdb_storage::StorageError> for CopperDbError {
    fn from(e: copperdb_storage::StorageError) -> Self {
        match e {
            copperdb_storage::StorageError::RequestCancelled(cancelled) => {
                CopperDbError::RequestCancelled(cancelled)
            }
            error => CopperDbError::Storage(error.to_string()),
        }
    }
}

impl From<copperdb_cypher::CypherError> for CopperDbError {
    fn from(e: copperdb_cypher::CypherError) -> Self {
        CopperDbError::Parse(e.to_string())
    }
}

impl From<copperdb_eval::EvalError> for CopperDbError {
    fn from(e: copperdb_eval::EvalError) -> Self {
        match e {
            copperdb_eval::EvalError::RequestCancelled(cancelled) => {
                CopperDbError::RequestCancelled(cancelled)
            }
            error => CopperDbError::Eval(error.to_string()),
        }
    }
}

impl From<TxError> for CopperDbError {
    fn from(e: TxError) -> Self {
        CopperDbError::Init(e.to_string())
    }
}

impl From<copperdb_audit::AuditError> for CopperDbError {
    fn from(e: copperdb_audit::AuditError) -> Self {
        CopperDbError::Audit(e.to_string())
    }
}

impl From<copperdb_compliance::ComplianceError> for CopperDbError {
    fn from(e: copperdb_compliance::ComplianceError) -> Self {
        CopperDbError::Compliance(e.to_string())
    }
}

fn compare_json(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Number(n1), Value::Number(n2)) => {
            let f1 = n1.as_f64().unwrap_or(f64::NAN);
            let f2 = n2.as_f64().unwrap_or(f64::NAN);
            f1.partial_cmp(&f2).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::String(s1), Value::String(s2)) => s1.cmp(s2),
        _ => std::cmp::Ordering::Equal,
    }
}

fn sort_rows_by_with_order(
    rows: &mut Vec<HashMap<String, Value>>,
    with_clause: &WithClause,
    params: &HashMap<String, Value>,
) -> Result<(), CopperDbError> {
    let mut rows_with_keys: Vec<(HashMap<String, Value>, Vec<Value>)> = rows
        .drain(..)
        .map(|row| {
            let keys = with_clause
                .order_by
                .iter()
                .map(|item| {
                    eval_expression(&item.expression, &row, params)
                        .map_err(|err| CopperDbError::Eval(err.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok((row, keys))
        })
        .collect::<Result<Vec<_>, CopperDbError>>()?;

    rows_with_keys.sort_by(|(_, keys_a), (_, keys_b)| {
        for (idx, item) in with_clause.order_by.iter().enumerate() {
            let ord = compare_json(&keys_a[idx], &keys_b[idx]);
            if ord != std::cmp::Ordering::Equal {
                return if item.descending { ord.reverse() } else { ord };
            }
        }
        std::cmp::Ordering::Equal
    });

    *rows = rows_with_keys.into_iter().map(|(row, _)| row).collect();
    Ok(())
}

fn apply_with_window(rows: &mut Vec<HashMap<String, Value>>, with_clause: &WithClause) {
    let skip_val = with_clause.skip.as_ref().and_then(|e| match e {
        Expression::Literal(LiteralValue::Integer(i)) => Some(*i),
        Expression::Parameter(_name) => {
            // Engine path doesn't have params context; resolve from expression only
            None
        }
        _ => None,
    });
    let limit_val = with_clause.limit.as_ref().and_then(|e| match e {
        Expression::Literal(LiteralValue::Integer(i)) => Some(*i),
        _ => None,
    });
    if let Some(skip) = skip_val {
        *rows = rows.drain(..).skip(skip.max(0) as usize).collect();
    }
    if let Some(limit) = limit_val {
        rows.truncate(limit.max(0) as usize);
    }
}

impl From<ReplicationError> for CopperDbError {
    fn from(e: ReplicationError) -> Self {
        CopperDbError::Replication(e.to_string())
    }
}

// ─── Legacy error (kept for existing tests) ──────────────────────────────────

#[derive(Debug, Error)]
pub enum CopperDbServerError {
    #[error("storage error: {0}")]
    Storage(String),
    #[error("auth error: {0}")]
    Auth(String),
    #[error("query error: {0}")]
    Query(String),
    #[error("initialization error: {0}")]
    Init(String),
}

// ─── Config ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub data_dir: String,
    pub max_connections: usize,
    pub default_database: String,
    pub auth_enabled: bool,
    pub log_queries: bool,
    /// Fsync the CopperDB WAL and Fjall batch before acknowledging writes.
    pub sync_writes: bool,
    pub runtime_config: copperdb_config::EffectiveDatabaseConfig,
    pub storage_encryption_master_key: Option<Vec<u8>>,
    pub storage_encryption_key_uri: String,
    pub distributed_repair_queue_dir: Option<String>,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        let runtime_config = copperdb_config::resolve_per_database_config(
            &copperdb_config::Config::default(),
            &BTreeMap::new(),
        )
        .expect("default per-database config should resolve");
        Self {
            data_dir: "data".to_string(),
            max_connections: 100,
            default_database: "copperdb".to_string(),
            auth_enabled: true,
            log_queries: false,
            sync_writes: false,
            runtime_config,
            storage_encryption_master_key: None,
            storage_encryption_key_uri: "kms://local/storage".into(),
            distributed_repair_queue_dir: None,
        }
    }
}

// ─── QueryResult / QueryStats ─────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<HashMap<String, Value>>,
    pub stats: ResultStats,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchOperationalStatus {
    pub ready: bool,
    pub building: bool,
    pub initialized: bool,
    pub strategy: &'static str,
    pub phase: &'static str,
    pub processed_nodes: u64,
    pub total_nodes: u64,
    pub rate_nodes_per_sec: f64,
    pub eta_seconds: i64,
    pub bm25_enabled: bool,
    pub vector_enabled: bool,
    pub lazy_trigger_needed: bool,
}

#[derive(Debug)]
pub struct DistributedQueryResult {
    pub result: QueryResult,
    pub write_outcome: Option<DistributedWriteOutcome>,
    pub read_outcome: Option<DistributedReadOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedPath {
    pub node_ids: Vec<String>,
    pub edge_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DistributedBfsResult {
    pub plan: DistributedReadPlan,
    pub responded_by: Vec<String>,
    pub failed_replicas: Vec<String>,
    pub path: Option<DistributedPath>,
}

type DistributedAccessWrites = BTreeMap<String, KnowledgePolicyAccessMetadata>;

#[derive(Debug, Default, Clone)]
pub struct ResultStats {
    pub nodes_created: usize,
    pub nodes_deleted: usize,
    pub relationships_created: usize,
    pub relationships_deleted: usize,
    pub properties_set: usize,
    pub execution_time_ms: u64,
}

impl From<QueryStats> for ResultStats {
    fn from(s: QueryStats) -> Self {
        Self {
            nodes_created: s.nodes_created,
            nodes_deleted: s.nodes_deleted,
            relationships_created: s.relationships_created,
            relationships_deleted: s.relationships_deleted,
            properties_set: s.properties_set,
            execution_time_ms: 0,
        }
    }
}

// ─── copperdb (embedded sync engine) ─────────────────────────────────────────

/// The primary embedded database engine.
pub struct CopperDb {
    config: DatabaseConfig,
    storage: Arc<StorageEngine>,
    vector_indexes: Arc<VectorIndexManager>,
    embedding_runtime: Arc<EmbeddingRuntime>,
    eval: EvalEngine,
    tx_manager: Arc<TransactionManager>,
    query_cache: Arc<QueryCache<copperdb_cypher::Query>>,
    cypher_result_cache: Arc<QueryResultCache<QueryResult>>,
    ranked_search_cache: Arc<QueryCache<RrfSearchBatch>>,
    audit_log: Arc<AuditLog>,
    compliance: Arc<ComplianceManager>,
}

mod copperdb;
mod embedding_runtime;
mod vector_indexes;

// ─── Legacy copperdb (full-server async variant) ──────────────────────────────

/// Full-server async variant that integrates all subsystems.
pub struct CopperDbServer {
    pub config: copperdb_config::Config,
}

impl CopperDbServer {
    /// Initialize and start the database engine.
    pub async fn start(config: copperdb_config::Config) -> Result<Self, CopperDbServerError> {
        tracing::info!("Starting copperdb v{}", env!("CARGO_PKG_VERSION"));
        Ok(Self { config })
    }

    /// Gracefully shut down all subsystems.
    pub async fn shutdown(&self) -> Result<(), CopperDbServerError> {
        tracing::info!("Shutting down copperdb");
        Ok(())
    }
}

fn collect_compliance_terms(
    query: &copperdb_cypher::Query,
    labels: &mut Vec<String>,
    properties: &mut Vec<String>,
) {
    for clause in &query.clauses {
        match clause {
            Clause::Match(clause) | Clause::OptionalMatch(clause) => {
                collect_pattern_terms(&clause.pattern, labels, properties)
            }
            Clause::Create(clause) => collect_pattern_terms(&clause.pattern, labels, properties),
            Clause::Merge(clause) => collect_pattern_terms(&clause.pattern, labels, properties),
            Clause::Where(clause) => collect_expression_properties(&clause.expression, properties),
            Clause::Set(clause) => {
                for item in &clause.items {
                    match item {
                        SetItem::Property {
                            property,
                            value,
                            variable: _,
                        } => {
                            properties.push(property.clone());
                            collect_expression_properties(value, properties);
                        }
                        SetItem::MapAssignment { value, variable: _ } => {
                            collect_expression_properties(value, properties);
                        }
                        SetItem::MapMerge { value, variable: _ } => {
                            collect_expression_properties(value, properties);
                        }
                        SetItem::Label { label, variable: _ } => {
                            properties.push(label.clone());
                        }
                        SetItem::DynamicLabel {
                            expression,
                            variable: _,
                        } => {
                            collect_expression_properties(expression, properties);
                        }
                    }
                }
            }
            Clause::Return(clause) => {
                for item in &clause.items {
                    collect_expression_properties(&item.expression, properties);
                }
                for item in &clause.order_by {
                    collect_expression_properties(&item.expression, properties);
                }
            }
            Clause::With(clause) => {
                for item in &clause.items {
                    collect_expression_properties(&item.expression, properties);
                }
                for item in &clause.order_by {
                    collect_expression_properties(&item.expression, properties);
                }
                if let Some(where_clause) = &clause.where_clause {
                    collect_expression_properties(&where_clause.expression, properties);
                }
            }
            _ => {}
        }
    }
}

fn collect_pattern_terms(
    pattern: &Pattern,
    labels: &mut Vec<String>,
    properties: &mut Vec<String>,
) {
    for node in &pattern.nodes {
        labels.extend(node.labels.iter().cloned());
        properties.extend(node.properties.iter().map(|entry| entry.key.clone()));
    }
    for edge in &pattern.edges {
        properties.extend(edge.properties.iter().map(|entry| entry.key.clone()));
    }
}

fn collect_expression_properties(expression: &Expression, properties: &mut Vec<String>) {
    match expression {
        Expression::PropertyAccess { property, .. } => properties.push(property.clone()),
        Expression::Comparison { operands, .. }
        | Expression::InList { operands, .. }
        | Expression::And(operands)
        | Expression::Or(operands)
        | Expression::Xor(operands) => {
            collect_expression_properties(&operands.left, properties);
            collect_expression_properties(&operands.right, properties);
        }
        Expression::FunctionCall { args, .. } => {
            for arg in args {
                collect_expression_properties(arg, properties);
            }
        }
        Expression::ListLiteral(items) => {
            for item in items {
                collect_expression_properties(item, properties);
            }
        }
        Expression::ListComprehension(comp) => {
            collect_expression_properties(&comp.list, properties);
            if let Some(ref pred) = comp.predicate {
                collect_expression_properties(pred, properties);
            }
            collect_expression_properties(&comp.expression, properties);
        }
        Expression::PatternComprehension(comp) => {
            // Recurse into the projection expression
            collect_expression_properties(&comp.expression, properties);
            if let Some(ref pred) = comp.predicate {
                collect_expression_properties(pred, properties);
            }
        }
        Expression::Reduce(reduce) => {
            collect_expression_properties(&reduce.initial, properties);
            collect_expression_properties(&reduce.list, properties);
            collect_expression_properties(&reduce.expression, properties);
        }
        Expression::MapLiteral(entries) => {
            for entry in entries {
                collect_expression_properties(&entry.value, properties);
            }
        }
        Expression::Not(inner) | Expression::IsNull(inner) | Expression::IsNotNull(inner) => {
            collect_expression_properties(inner, properties);
        }
        Expression::Add(operands)
        | Expression::Subtract(operands)
        | Expression::Multiply(operands)
        | Expression::Divide(operands)
        | Expression::Modulo(operands) => {
            collect_expression_properties(&operands.left, properties);
            collect_expression_properties(&operands.right, properties);
        }
        Expression::Between {
            expression,
            lower,
            upper,
        } => {
            collect_expression_properties(expression, properties);
            collect_expression_properties(lower, properties);
            collect_expression_properties(upper, properties);
        }
        Expression::Literal(_)
        | Expression::Parameter(_)
        | Expression::ParameterPropertyAccess { .. }
        | Expression::Variable(_)
        | Expression::PatternExists { .. }
        | Expression::BracketAccess { .. } => {}
        Expression::Case(case) => {
            if let Some(ref expr) = case.expression {
                collect_expression_properties(expr, properties);
            }
            for alt in &case.alternatives {
                collect_expression_properties(&alt.condition, properties);
                collect_expression_properties(&alt.result, properties);
            }
            if let Some(ref default) = case.default {
                collect_expression_properties(default, properties);
            }
        }
    }
}

#[cfg(test)]
mod tests;
