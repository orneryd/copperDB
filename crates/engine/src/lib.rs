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

use copperdb_audit::{AuditConfig, AuditLog, Event, EventType};
use copperdb_cache::QueryCache;
use copperdb_compliance::{ComplianceManager, ComplianceReporter};
use copperdb_cypher::{
    can_execute_as_pipeline, detect_query_pattern, match_compound_query_shape, Clause,
    EdgeDirection, Expression, Parser, Pattern, QueryType, ReturnItem, WhereClause, WithClause,
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
    collect_fabric_hydration_records, collect_planned_fabric_ranked_batches,
    execute_planned_fabric_ranked_search, hydrate_rrf_search_outcome, merge_rrf_search_batches,
    FabricHydrationRequest, FabricRankedSearchExecution, HydrationTransport, RankedSearchTransport,
    RrfConfig, RrfHydratedSearchOutcome, RrfHydrationRecord, RrfSearchBatch, RrfSearchHit,
    RrfSearchOutcome, RrfSearchPolicy, SearchQuery, SearchResult,
};
use copperdb_storage::{
    IndexEntityType, IndexKind, KnowledgePolicyAccessMetadata, NodeRecord, StorageEngine,
};
use copperdb_topology::{
    ConsistencyLevel, DistributedReadPlan, DistributedSearchPlan, DistributedWriteMode,
    DistributedWritePlan, FabricDatabase, FabricGlobalId, PlacementKey, TopologyRegistry,
};
use copperdb_txsession::TransactionManager;
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
}

impl From<copperdb_storage::StorageError> for CopperDbError {
    fn from(e: copperdb_storage::StorageError) -> Self {
        CopperDbError::Storage(e.to_string())
    }
}

impl From<copperdb_cypher::CypherError> for CopperDbError {
    fn from(e: copperdb_cypher::CypherError) -> Self {
        CopperDbError::Parse(e.to_string())
    }
}

impl From<copperdb_eval::EvalError> for CopperDbError {
    fn from(e: copperdb_eval::EvalError) -> Self {
        CopperDbError::Eval(e.to_string())
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
            auth_enabled: false,
            log_queries: false,
            runtime_config,
            storage_encryption_master_key: None,
            storage_encryption_key_uri: "kms://local/storage".into(),
            distributed_repair_queue_dir: None,
        }
    }
}

// ─── QueryResult / QueryStats ─────────────────────────────────────────────────

#[derive(Debug, Default)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<HashMap<String, Value>>,
    pub stats: ResultStats,
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
    eval: EvalEngine,
    tx_manager: Arc<TransactionManager>,
    query_cache: Arc<QueryCache<copperdb_cypher::Query>>,
    audit_log: Arc<AuditLog>,
    compliance: Arc<ComplianceManager>,
}

mod copperdb;


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
                    properties.push(item.property.clone());
                    collect_expression_properties(&item.value, properties);
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
        | Expression::Or(operands) => {
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
        Expression::MapLiteral(entries) => {
            for entry in entries {
                collect_expression_properties(&entry.value, properties);
            }
        }
        Expression::Not(inner) | Expression::IsNull(inner) | Expression::IsNotNull(inner) => {
            collect_expression_properties(inner, properties);
        }
        Expression::Literal(_) | Expression::Parameter(_) | Expression::Variable(_) => {}
    }
}


#[cfg(test)]
mod tests;
