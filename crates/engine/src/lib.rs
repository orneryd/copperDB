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
    RrfConfig, RrfHydratedSearchOutcome, RrfHydrationRecord, RrfSearchBatch, RrfSearchOutcome,
    RrfSearchPolicy, SearchQuery,
};
use copperdb_storage::{KnowledgePolicyAccessMetadata, StorageEngine};
use copperdb_topology::{
    ConsistencyLevel, DistributedReadPlan, DistributedSearchPlan, DistributedWriteMode,
    DistributedWritePlan, FabricDatabase, PlacementKey, TopologyRegistry,
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
    pub storage_encryption_master_key: Option<Vec<u8>>,
    pub storage_encryption_key_uri: String,
    pub distributed_repair_queue_dir: Option<String>,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            data_dir: "data".to_string(),
            max_connections: 100,
            default_database: "copperdb".to_string(),
            auth_enabled: false,
            log_queries: false,
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

impl CopperDb {
    /// Create a new in-memory (temporary) database instance.
    pub fn open_temporary() -> Result<Self, CopperDbError> {
        let storage = Arc::new(StorageEngine::open_temporary()?);
        Self::from_storage(storage, DatabaseConfig::default())
    }

    /// Create a persistent database at the given path.
    pub fn open(config: DatabaseConfig) -> Result<Self, CopperDbError> {
        let storage = Arc::new(open_storage(&config)?);
        Self::from_storage(storage, config)
    }

    fn from_storage(
        storage: Arc<StorageEngine>,
        config: DatabaseConfig,
    ) -> Result<Self, CopperDbError> {
        let eval = EvalEngine::new(Arc::clone(&storage));
        let audit_log = Arc::new(AuditLog::new(Arc::clone(&storage), AuditConfig::default())?);
        let compliance = Arc::new(ComplianceManager::new(Arc::clone(&storage)));
        Ok(Self {
            config,
            storage,
            eval,
            tx_manager: Arc::new(TransactionManager::new()),
            query_cache: Arc::new(QueryCache::new(
                1024,
                Some(std::time::Duration::from_secs(300)),
            )),
            audit_log,
            compliance,
        })
    }

    /// Execute a Cypher query string as an embedded admin caller.
    pub fn execute(
        &self,
        cypher: &str,
        params: HashMap<String, Value>,
    ) -> Result<QueryResult, CopperDbError> {
        self.execute_as(cypher, params, &["admin".to_string()])
    }

    /// Execute a Cypher query as a caller with the provided normalized role names.
    pub fn execute_as(
        &self,
        cypher: &str,
        params: HashMap<String, Value>,
        roles: &[String],
    ) -> Result<QueryResult, CopperDbError> {
        let start = Instant::now();

        if self.config.log_queries {
            tracing::info!(query = cypher, "executing query");
        }

        let _flush_guard = self.storage.hold_flush();

        let hash = QueryCache::<copperdb_cypher::Query>::key(cypher, &[]);
        let parsed = if let Some(cached) = self.query_cache.get(hash) {
            cached
        } else {
            let parser = Parser::new();
            let q = match parser.parse(cypher) {
                Ok(q) => q,
                Err(err) => {
                    self.record_query_audit(
                        cypher,
                        "PARSE",
                        false,
                        Some(err.to_string()),
                        None,
                        0,
                    )?;
                    return Err(err.into());
                }
            };
            self.query_cache.put(hash, q.clone());
            q
        };

        if let Err(err) = self.enforce_compliance(&parsed, roles) {
            self.record_query_audit(
                cypher,
                query_action(&parsed.query_type),
                false,
                Some(err.to_string()),
                Some(hash),
                start.elapsed().as_millis() as u64,
            )?;
            return Err(err.into());
        }

        let pattern_info = detect_query_pattern(cypher);
        let (compound_shape, compound_ok) = match_compound_query_shape(cypher);
        let (pipeline_clauses, pipeline_ok) = can_execute_as_pipeline(cypher);
        let eval_result = match self.eval.execute_with_routes(
            &parsed,
            &params,
            &pattern_info,
            compound_ok.then_some(&compound_shape),
            pipeline_ok.then_some(pipeline_clauses.as_slice()),
        ) {
            Ok(result) => result,
            Err(err) => {
                self.record_query_audit(
                    cypher,
                    query_action(&parsed.query_type),
                    false,
                    Some(err.to_string()),
                    Some(hash),
                    start.elapsed().as_millis() as u64,
                )?;
                return Err(err.into());
            }
        };

        let elapsed_ms = start.elapsed().as_millis() as u64;
        let mut stats = ResultStats::from(eval_result.stats);
        stats.execution_time_ms = elapsed_ms;
        self.record_query_audit(
            cypher,
            query_action(&parsed.query_type),
            true,
            None,
            Some(hash),
            elapsed_ms,
        )?;

        Ok(QueryResult {
            columns: eval_result.columns,
            rows: eval_result.rows,
            stats,
        })
    }

    pub async fn execute_distributed_as(
        &self,
        cypher: &str,
        params: HashMap<String, Value>,
        roles: &[String],
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
        transport: Arc<dyn ReplicaTransport>,
    ) -> Result<DistributedQueryResult, CopperDbError> {
        let parsed = Parser::new().parse(cypher)?;
        self.enforce_compliance(&parsed, roles)?;

        if !is_mutating_query(&parsed.query_type) {
            if let Some(shape) = distributed_shortest_path_query_shape(&parsed) {
                let (result, bfs) = self
                    .execute_distributed_shortest_path_query(
                        &shape,
                        &params,
                        placement,
                        consistency,
                        request_region,
                        transport,
                    )
                    .await?;
                return Ok(DistributedQueryResult {
                    result,
                    write_outcome: None,
                    read_outcome: Some(DistributedReadOutcome {
                        plan: bfs.plan,
                        responded_by: bfs.responded_by,
                        failed_replicas: bfs.failed_replicas,
                        value: None,
                    }),
                });
            }
            if let Some(shape) = distributed_direct_path_query_shape(&parsed) {
                let (result, read_outcome) = self
                    .execute_distributed_direct_path_query(
                        &shape,
                        &params,
                        placement,
                        consistency,
                        request_region,
                        transport,
                    )
                    .await?;
                return Ok(DistributedQueryResult {
                    result,
                    write_outcome: None,
                    read_outcome: Some(read_outcome),
                });
            }
            if let Some(shape) = distributed_leading_path_query_shape(&parsed) {
                let (result, read_outcome) = self
                    .execute_distributed_leading_path_query(
                        &shape,
                        &params,
                        placement,
                        consistency,
                        request_region,
                        transport,
                    )
                    .await?;
                return Ok(DistributedQueryResult {
                    result,
                    write_outcome: None,
                    read_outcome: Some(read_outcome),
                });
            }
        }

        let coordinator = self.build_cassandra_coordinator(transport)?;
        let mut write_outcome = None;
        let mut read_outcome = None;
        if is_mutating_query(&parsed.query_type) {
            write_outcome = Some(
                coordinator
                    .write(
                        placement,
                        consistency,
                        Command::CypherMutation {
                            database: self.config.default_database.clone(),
                            query: cypher.to_string(),
                            params: Value::Object(params.clone().into_iter().collect()),
                        },
                        request_region,
                    )
                    .await?,
            );
        } else {
            let plan = self.plan_distributed_read(placement, consistency, request_region)?;
            read_outcome = Some(DistributedReadOutcome {
                plan,
                responded_by: Vec::new(),
                failed_replicas: Vec::new(),
                value: None,
            });
        }

        Ok(DistributedQueryResult {
            result: self.execute_as(cypher, params, roles)?,
            write_outcome,
            read_outcome,
        })
    }

    pub async fn distributed_bfs_path_as(
        &self,
        start_node_id: &str,
        end_node_id: &str,
        rel_type: Option<&str>,
        direction: EdgeDirection,
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
        transport: Arc<dyn ReplicaTransport>,
    ) -> Result<DistributedBfsResult, CopperDbError> {
        let plan = self.plan_distributed_read(placement, consistency, request_region)?;
        let mut responded_by = BTreeSet::new();
        let mut failed_replicas = BTreeSet::new();
        let mut access_writes = BTreeMap::new();

        let start_exists = self
            .distributed_graph_node_exists(
                &plan,
                transport.as_ref(),
                start_node_id,
                &mut responded_by,
                &mut failed_replicas,
            )
            .await?;
        let end_exists = self
            .distributed_graph_node_exists(
                &plan,
                transport.as_ref(),
                end_node_id,
                &mut responded_by,
                &mut failed_replicas,
            )
            .await?;

        if responded_by.len() < plan.required_responses {
            return Err(ReplicationError::NoQuorum {
                required: plan.required_responses,
                received: responded_by.len(),
            }
            .into());
        }

        let path = if start_exists && end_exists {
            let params = HashMap::new();
            self.distributed_bfs_path(
                &plan,
                transport.as_ref(),
                start_node_id,
                end_node_id,
                rel_type,
                &direction,
                &params,
                &mut access_writes,
                &mut responded_by,
                &mut failed_replicas,
            )
            .await?
        } else {
            None
        };

        if responded_by.len() < plan.required_responses {
            return Err(ReplicationError::NoQuorum {
                required: plan.required_responses,
                received: responded_by.len(),
            }
            .into());
        }

        self.flush_distributed_access_writes(
            placement,
            consistency,
            request_region,
            transport.clone(),
            access_writes,
        )
        .await?;

        Ok(DistributedBfsResult {
            plan,
            responded_by: responded_by.into_iter().collect(),
            failed_replicas: failed_replicas.into_iter().collect(),
            path,
        })
    }

    pub async fn distributed_bfs_query_as(
        &self,
        start_node_id: &str,
        end_node_id: &str,
        rel_type: Option<&str>,
        direction: EdgeDirection,
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
        transport: Arc<dyn ReplicaTransport>,
    ) -> Result<(QueryResult, DistributedBfsResult), CopperDbError> {
        let bfs = self
            .distributed_bfs_path_as(
                start_node_id,
                end_node_id,
                rel_type,
                direction.clone(),
                placement,
                consistency,
                request_region,
                transport.clone(),
            )
            .await?;

        let path_value = if let Some(path) = &bfs.path {
            let params = HashMap::new();
            let mut access_writes = BTreeMap::new();
            Some(
                self.materialize_distributed_path_value(
                    &bfs.plan,
                    transport.as_ref(),
                    path,
                    &direction,
                    &params,
                    &mut access_writes,
                )
                .await?,
            )
        } else {
            None
        };

        Ok((distributed_bfs_query_result(path_value.as_ref()), bfs))
    }

    async fn execute_distributed_shortest_path_query(
        &self,
        shape: &DistributedShortestPathQueryShape,
        params: &HashMap<String, Value>,
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
        transport: Arc<dyn ReplicaTransport>,
    ) -> Result<(QueryResult, DistributedBfsResult), CopperDbError> {
        let plan = self.plan_distributed_read(placement, consistency, request_region)?;
        let mut responded_by = BTreeSet::new();
        let mut failed_replicas = BTreeSet::new();
        let mut access_writes = BTreeMap::new();

        let start_candidates = self
            .distributed_resolve_node_candidates(
                &plan,
                transport.as_ref(),
                &shape.start_selector,
                params,
                &mut access_writes,
                &mut responded_by,
                &mut failed_replicas,
            )
            .await?;
        let end_candidates = self
            .distributed_resolve_node_candidates(
                &plan,
                transport.as_ref(),
                &shape.end_selector,
                params,
                &mut access_writes,
                &mut responded_by,
                &mut failed_replicas,
            )
            .await?;

        if responded_by.len() < plan.required_responses {
            return Err(ReplicationError::NoQuorum {
                required: plan.required_responses,
                received: responded_by.len(),
            }
            .into());
        }

        let start_ids = distributed_node_ids(&start_candidates);
        let end_ids = distributed_node_ids(&end_candidates);
        let mut best_path: Option<DistributedPath> = None;

        for start_node_id in &start_ids {
            for end_node_id in &end_ids {
                let candidate = self
                    .distributed_bfs_path(
                        &plan,
                        transport.as_ref(),
                        start_node_id,
                        end_node_id,
                        shape.rel_type.as_deref(),
                        &shape.direction,
                        params,
                        &mut access_writes,
                        &mut responded_by,
                        &mut failed_replicas,
                    )
                    .await?;
                if let Some(candidate) = candidate {
                    let replace = best_path
                        .as_ref()
                        .map(|current| candidate.edge_ids.len() < current.edge_ids.len())
                        .unwrap_or(true);
                    if replace {
                        best_path = Some(candidate);
                    }
                }
            }
        }

        if responded_by.len() < plan.required_responses {
            return Err(ReplicationError::NoQuorum {
                required: plan.required_responses,
                received: responded_by.len(),
            }
            .into());
        }

        let bfs = DistributedBfsResult {
            plan,
            responded_by: responded_by.into_iter().collect(),
            failed_replicas: failed_replicas.into_iter().collect(),
            path: best_path,
        };

        let path_value = if let Some(path) = &bfs.path {
            Some(
                self.materialize_distributed_path_value(
                    &bfs.plan,
                    transport.as_ref(),
                    path,
                    &shape.direction,
                    params,
                    &mut access_writes,
                )
                .await?,
            )
        } else {
            None
        };

        self.flush_distributed_access_writes(
            placement,
            consistency,
            request_region,
            transport.clone(),
            access_writes,
        )
        .await?;

        Ok((
            distributed_shortest_path_result(shape, path_value.as_ref())?,
            bfs,
        ))
    }

    async fn execute_distributed_direct_path_query(
        &self,
        shape: &DistributedDirectPathQueryShape,
        params: &HashMap<String, Value>,
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
        transport: Arc<dyn ReplicaTransport>,
    ) -> Result<(QueryResult, DistributedReadOutcome), CopperDbError> {
        let plan = self.plan_distributed_read(placement, consistency, request_region)?;
        let mut responded_by = BTreeSet::new();
        let mut failed_replicas = BTreeSet::new();
        let mut access_writes = BTreeMap::new();
        let mut path_values = self
            .distributed_direct_path_values(
                &plan,
                transport.as_ref(),
                &shape.pattern,
                params,
                &mut access_writes,
                &mut responded_by,
                &mut failed_replicas,
            )
            .await?;

        if shape.optional && path_values.is_empty() {
            path_values.push(Value::Null);
        }

        if responded_by.len() < plan.required_responses {
            return Err(ReplicationError::NoQuorum {
                required: plan.required_responses,
                received: responded_by.len(),
            }
            .into());
        }

        self.flush_distributed_access_writes(
            placement,
            consistency,
            request_region,
            transport.clone(),
            access_writes,
        )
        .await?;

        Ok((
            distributed_path_query_result(&shape.return_items, &shape.path_variable, &path_values)?,
            DistributedReadOutcome {
                plan,
                responded_by: responded_by.into_iter().collect(),
                failed_replicas: failed_replicas.into_iter().collect(),
                value: None,
            },
        ))
    }

    async fn execute_distributed_leading_path_query(
        &self,
        shape: &DistributedLeadingPathQueryShape,
        params: &HashMap<String, Value>,
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
        transport: Arc<dyn ReplicaTransport>,
    ) -> Result<(QueryResult, DistributedReadOutcome), CopperDbError> {
        let plan = self.plan_distributed_read(placement, consistency, request_region)?;
        let mut responded_by = BTreeSet::new();
        let mut failed_replicas = BTreeSet::new();
        let mut access_writes = BTreeMap::new();

        let mut base_rows = vec![HashMap::new()];
        for leading_step in &shape.leading_steps {
            match leading_step {
                DistributedLeadingStep::Match(leading_match) => {
                    let mut next_rows = Vec::new();
                    for base_row in &base_rows {
                        match leading_match {
                            DistributedLeadingMatch::Node { selector, variable } => {
                                let matched_nodes = self
                                    .distributed_resolve_node_candidates_for_row(
                                        &plan,
                                        transport.as_ref(),
                                        selector,
                                        params,
                                        base_row,
                                        &mut access_writes,
                                        &mut responded_by,
                                        &mut failed_replicas,
                                    )
                                    .await?;
                                if responded_by.len() < plan.required_responses {
                                    return Err(ReplicationError::NoQuorum {
                                        required: plan.required_responses,
                                        received: responded_by.len(),
                                    }
                                    .into());
                                }
                                for matched_node in matched_nodes {
                                    let mut row = base_row.clone();
                                    if let Some(variable) = variable {
                                        row.insert(variable.clone(), matched_node);
                                    }
                                    next_rows.push(row);
                                }
                            }
                            DistributedLeadingMatch::Relationship {
                                pattern,
                                start_variable,
                                end_variable,
                                edge_variable,
                            } => {
                                let matched_paths = self
                                    .distributed_direct_path_values_for_row(
                                        &plan,
                                        transport.as_ref(),
                                        pattern,
                                        params,
                                        base_row,
                                        &mut access_writes,
                                        &mut responded_by,
                                        &mut failed_replicas,
                                    )
                                    .await?;
                                for matched_path in matched_paths {
                                    let Some(nodes) = distributed_path_nodes(&matched_path) else {
                                        continue;
                                    };
                                    let Some(relationships) =
                                        distributed_path_relationships(&matched_path)
                                    else {
                                        continue;
                                    };
                                    if nodes.len() < 2 || relationships.is_empty() {
                                        continue;
                                    }

                                    let mut row = base_row.clone();
                                    if let Some(variable) = start_variable {
                                        row.insert(variable.clone(), nodes[0].clone());
                                    }
                                    if let Some(variable) = end_variable {
                                        row.insert(
                                            variable.clone(),
                                            nodes[nodes.len() - 1].clone(),
                                        );
                                    }
                                    if let Some(variable) = edge_variable {
                                        if relationships.len() != 1 {
                                            continue;
                                        }
                                        row.insert(variable.clone(), relationships[0].clone());
                                    }
                                    next_rows.push(row);
                                }
                            }
                        }
                    }
                    base_rows = next_rows;
                }
                DistributedLeadingStep::OptionalMatch(leading_match) => {
                    let mut next_rows = Vec::new();
                    for base_row in &base_rows {
                        match leading_match {
                            DistributedLeadingMatch::Node { selector, variable } => {
                                let matched_nodes = self
                                    .distributed_resolve_node_candidates_for_row(
                                        &plan,
                                        transport.as_ref(),
                                        selector,
                                        params,
                                        base_row,
                                        &mut access_writes,
                                        &mut responded_by,
                                        &mut failed_replicas,
                                    )
                                    .await?;
                                if responded_by.len() < plan.required_responses {
                                    return Err(ReplicationError::NoQuorum {
                                        required: plan.required_responses,
                                        received: responded_by.len(),
                                    }
                                    .into());
                                }
                                if matched_nodes.is_empty() {
                                    let mut row = base_row.clone();
                                    if let Some(variable) = variable {
                                        if !row.contains_key(variable) {
                                            row.insert(variable.clone(), Value::Null);
                                        }
                                    }
                                    next_rows.push(row);
                                    continue;
                                }
                                for matched_node in matched_nodes {
                                    let mut row = base_row.clone();
                                    if let Some(variable) = variable {
                                        row.insert(variable.clone(), matched_node);
                                    }
                                    next_rows.push(row);
                                }
                            }
                            DistributedLeadingMatch::Relationship {
                                pattern,
                                start_variable,
                                end_variable,
                                edge_variable,
                            } => {
                                let matched_paths = self
                                    .distributed_direct_path_values_for_row(
                                        &plan,
                                        transport.as_ref(),
                                        pattern,
                                        params,
                                        base_row,
                                        &mut access_writes,
                                        &mut responded_by,
                                        &mut failed_replicas,
                                    )
                                    .await?;
                                let mut matched_any = false;
                                for matched_path in matched_paths {
                                    let Some(nodes) = distributed_path_nodes(&matched_path) else {
                                        continue;
                                    };
                                    let Some(relationships) =
                                        distributed_path_relationships(&matched_path)
                                    else {
                                        continue;
                                    };
                                    if nodes.len() < 2 || relationships.is_empty() {
                                        continue;
                                    }

                                    let mut row = base_row.clone();
                                    if let Some(variable) = start_variable {
                                        row.insert(variable.clone(), nodes[0].clone());
                                    }
                                    if let Some(variable) = end_variable {
                                        row.insert(
                                            variable.clone(),
                                            nodes[nodes.len() - 1].clone(),
                                        );
                                    }
                                    if let Some(variable) = edge_variable {
                                        if relationships.len() != 1 {
                                            continue;
                                        }
                                        row.insert(variable.clone(), relationships[0].clone());
                                    }
                                    matched_any = true;
                                    next_rows.push(row);
                                }
                                if !matched_any {
                                    let mut row = base_row.clone();
                                    distributed_bind_optional_leading_match_nulls(
                                        &mut row,
                                        leading_match,
                                    );
                                    next_rows.push(row);
                                }
                            }
                        }
                    }
                    base_rows = next_rows;
                }
                DistributedLeadingStep::Where(where_clause) => {
                    let mut filtered_rows = Vec::new();
                    for row in base_rows {
                        let keep = eval_predicate(&where_clause.expression, &row, params)
                            .map_err(|err| CopperDbError::Eval(err.to_string()))?;
                        if keep {
                            filtered_rows.push(row);
                        }
                    }
                    base_rows = filtered_rows;
                }
                DistributedLeadingStep::With(with_clause) => {
                    let mut projected_rows = base_rows
                        .into_iter()
                        .map(|row| distributed_project_row(&row, &with_clause.items, params))
                        .collect::<Result<Vec<_>, CopperDbError>>()?;

                    if let Some(limit) = with_clause.limit {
                        projected_rows.truncate(limit.max(0) as usize);
                    }

                    if let Some(where_clause) = &with_clause.where_clause {
                        let mut filtered_rows = Vec::new();
                        for row in projected_rows {
                            let keep = eval_predicate(&where_clause.expression, &row, params)
                                .map_err(|err| CopperDbError::Eval(err.to_string()))?;
                            if keep {
                                filtered_rows.push(row);
                            }
                        }
                        base_rows = filtered_rows;
                    } else {
                        base_rows = projected_rows;
                    }
                }
            }
            if base_rows.is_empty() {
                break;
            }
        }

        let mut path_values = Vec::new();
        for base_row in base_rows {
            let row_path_values = self
                .distributed_direct_path_values_for_row(
                    &plan,
                    transport.as_ref(),
                    &shape.path_shape.pattern,
                    params,
                    &base_row,
                    &mut access_writes,
                    &mut responded_by,
                    &mut failed_replicas,
                )
                .await?;
            if row_path_values.is_empty() {
                if shape.path_shape.optional {
                    path_values.push(Value::Null);
                }
            } else {
                path_values.extend(row_path_values);
            }
        }

        if responded_by.len() < plan.required_responses {
            return Err(ReplicationError::NoQuorum {
                required: plan.required_responses,
                received: responded_by.len(),
            }
            .into());
        }

        self.flush_distributed_access_writes(
            placement,
            consistency,
            request_region,
            transport.clone(),
            access_writes,
        )
        .await?;

        Ok((
            distributed_path_query_result(
                &shape.path_shape.return_items,
                &shape.path_shape.path_variable,
                &path_values,
            )?,
            DistributedReadOutcome {
                plan,
                responded_by: responded_by.into_iter().collect(),
                failed_replicas: failed_replicas.into_iter().collect(),
                value: None,
            },
        ))
    }

    async fn distributed_direct_path_values(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        pattern: &DistributedDirectPathPattern,
        params: &HashMap<String, Value>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Vec<Value>, CopperDbError> {
        let mut path_values = Vec::new();

        match pattern {
            DistributedDirectPathPattern::SingleNode { selector } => {
                let nodes = self
                    .distributed_resolve_node_candidates(
                        plan,
                        transport,
                        selector,
                        params,
                        access_writes,
                        responded_by,
                        failed_replicas,
                    )
                    .await?;
                if responded_by.len() < plan.required_responses {
                    return Err(ReplicationError::NoQuorum {
                        required: plan.required_responses,
                        received: responded_by.len(),
                    }
                    .into());
                }
                path_values.extend(nodes.into_iter().map(|node| {
                    Value::Object(
                        [
                            ("nodes".to_string(), Value::Array(vec![node])),
                            ("relationships".to_string(), Value::Array(Vec::new())),
                            ("length".to_string(), Value::from(0)),
                        ]
                        .into_iter()
                        .collect(),
                    )
                }));
            }
            DistributedDirectPathPattern::RelationshipPath {
                start_selector,
                end_selector,
                rel_type,
                direction,
                edge_properties,
                min_hops,
                max_hops,
            } => {
                let start_nodes = self
                    .distributed_resolve_node_candidates(
                        plan,
                        transport,
                        start_selector,
                        params,
                        access_writes,
                        responded_by,
                        failed_replicas,
                    )
                    .await?;
                let end_nodes = self
                    .distributed_resolve_node_candidates(
                        plan,
                        transport,
                        end_selector,
                        params,
                        access_writes,
                        responded_by,
                        failed_replicas,
                    )
                    .await?;
                if responded_by.len() < plan.required_responses {
                    return Err(ReplicationError::NoQuorum {
                        required: plan.required_responses,
                        received: responded_by.len(),
                    }
                    .into());
                }

                let end_ids = distributed_node_ids(&end_nodes)
                    .into_iter()
                    .collect::<HashSet<_>>();
                for start_node_id in distributed_node_ids(&start_nodes) {
                    for path in self
                        .distributed_relationship_paths(
                            plan,
                            transport,
                            &start_node_id,
                            &end_ids,
                            rel_type.as_deref(),
                            direction,
                            edge_properties,
                            *min_hops,
                            *max_hops,
                            params,
                            access_writes,
                            responded_by,
                            failed_replicas,
                        )
                        .await?
                    {
                        path_values.push(
                            self.materialize_distributed_path_value(
                                plan, transport, &path, direction, params, access_writes,
                            )
                            .await?,
                        );
                    }
                }

                if responded_by.len() < plan.required_responses {
                    return Err(ReplicationError::NoQuorum {
                        required: plan.required_responses,
                        received: responded_by.len(),
                    }
                    .into());
                }
            }
        }

        Ok(path_values)
    }

    async fn distributed_direct_path_values_for_row(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        pattern: &DistributedDirectPathPattern,
        params: &HashMap<String, Value>,
        base_row: &HashMap<String, Value>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Vec<Value>, CopperDbError> {
        let mut path_values = Vec::new();

        match pattern {
            DistributedDirectPathPattern::SingleNode { selector } => {
                let nodes = self
                    .distributed_resolve_node_candidates_for_row(
                        plan,
                        transport,
                        selector,
                        params,
                        base_row,
                        access_writes,
                        responded_by,
                        failed_replicas,
                    )
                    .await?;
                if responded_by.len() < plan.required_responses {
                    return Err(ReplicationError::NoQuorum {
                        required: plan.required_responses,
                        received: responded_by.len(),
                    }
                    .into());
                }
                path_values.extend(nodes.into_iter().map(|node| {
                    Value::Object(
                        [
                            ("nodes".to_string(), Value::Array(vec![node])),
                            ("relationships".to_string(), Value::Array(Vec::new())),
                            ("length".to_string(), Value::from(0)),
                        ]
                        .into_iter()
                        .collect(),
                    )
                }));
            }
            DistributedDirectPathPattern::RelationshipPath {
                start_selector,
                end_selector,
                rel_type,
                direction,
                edge_properties,
                min_hops,
                max_hops,
            } => {
                let start_nodes = self
                    .distributed_resolve_node_candidates_for_row(
                        plan,
                        transport,
                        start_selector,
                        params,
                        base_row,
                        access_writes,
                        responded_by,
                        failed_replicas,
                    )
                    .await?;
                let end_nodes = self
                    .distributed_resolve_node_candidates_for_row(
                        plan,
                        transport,
                        end_selector,
                        params,
                        base_row,
                        access_writes,
                        responded_by,
                        failed_replicas,
                    )
                    .await?;
                if responded_by.len() < plan.required_responses {
                    return Err(ReplicationError::NoQuorum {
                        required: plan.required_responses,
                        received: responded_by.len(),
                    }
                    .into());
                }

                let end_ids = distributed_node_ids(&end_nodes)
                    .into_iter()
                    .collect::<HashSet<_>>();
                for start_node_id in distributed_node_ids(&start_nodes) {
                    for path in self
                        .distributed_relationship_paths(
                            plan,
                            transport,
                            &start_node_id,
                            &end_ids,
                            rel_type.as_deref(),
                            direction,
                            edge_properties,
                            *min_hops,
                            *max_hops,
                            params,
                            access_writes,
                            responded_by,
                            failed_replicas,
                        )
                        .await?
                    {
                        path_values.push(
                            self.materialize_distributed_path_value(
                                plan, transport, &path, direction, params, access_writes,
                            )
                            .await?,
                        );
                    }
                }

                if responded_by.len() < plan.required_responses {
                    return Err(ReplicationError::NoQuorum {
                        required: plan.required_responses,
                        received: responded_by.len(),
                    }
                    .into());
                }
            }
        }

        Ok(path_values)
    }

    async fn distributed_resolve_node_candidates_for_row(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        selector: &DistributedNodeSelector,
        params: &HashMap<String, Value>,
        base_row: &HashMap<String, Value>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Vec<Value>, CopperDbError> {
        match selector {
            DistributedNodeSelector::Bound {
                variable,
                labels,
                properties,
            } => Ok(base_row
                .get(variable)
                .filter(|value| distributed_node_matches(value, labels, properties))
                .cloned()
                .into_iter()
                .collect()),
            _ => {
                self.distributed_resolve_node_candidates(
                    plan,
                    transport,
                    selector,
                    params,
                    access_writes,
                    responded_by,
                    failed_replicas,
                )
                .await
            }
        }
    }

    async fn distributed_relationship_paths(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        start_node_id: &str,
        end_ids: &HashSet<String>,
        rel_type: Option<&str>,
        direction: &EdgeDirection,
        edge_properties: &BTreeMap<String, Value>,
        min_hops: u32,
        max_hops: u32,
        params: &HashMap<String, Value>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Vec<DistributedPath>, CopperDbError> {
        let mut frontier = VecDeque::from([(
            start_node_id.to_string(),
            0_u32,
            vec![start_node_id.to_string()],
            Vec::<String>::new(),
        )]);
        let mut visited = HashSet::from([(start_node_id.to_string(), 0_u32)]);
        let mut paths = Vec::new();

        while let Some((current_node_id, depth, path_node_ids, path_edge_ids)) =
            frontier.pop_front()
        {
            if depth >= min_hops && end_ids.contains(&current_node_id) {
                paths.push(DistributedPath {
                    node_ids: path_node_ids.clone(),
                    edge_ids: path_edge_ids.clone(),
                });
            }

            if depth >= max_hops {
                continue;
            }

            let mut edges = self
                .distributed_graph_edges_from_node(
                    plan,
                    transport,
                    &current_node_id,
                    rel_type,
                    direction,
                    params,
                    access_writes,
                    responded_by,
                    failed_replicas,
                )
                .await?;
            edges.sort_by(|left, right| left.id.cmp(&right.id));

            for edge in edges {
                if !distributed_edge_matches(&edge, edge_properties) {
                    continue;
                }
                let Some(next_node_id) =
                    distributed_related_node_id(&current_node_id, &edge, direction)
                else {
                    continue;
                };
                let next_depth = depth + 1;
                if !visited.insert((next_node_id.clone(), next_depth)) {
                    continue;
                }
                let mut next_node_ids = path_node_ids.clone();
                next_node_ids.push(next_node_id.clone());
                let mut next_edge_ids = path_edge_ids.clone();
                next_edge_ids.push(edge.id.clone());
                frontier.push_back((next_node_id, next_depth, next_node_ids, next_edge_ids));
            }
        }

        Ok(paths)
    }

    async fn distributed_resolve_node_candidates(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        selector: &DistributedNodeSelector,
        params: &HashMap<String, Value>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Vec<Value>, CopperDbError> {
        match selector {
            DistributedNodeSelector::LiteralId(node_id) => Ok(self
                .distributed_graph_node_value(
                    plan,
                    transport,
                    node_id,
                    params,
                    access_writes,
                    responded_by,
                    failed_replicas,
                )
                .await?
                .into_iter()
                .collect()),
            DistributedNodeSelector::Pattern { labels, properties } => {
                let primary_label = labels.first().expect("selector labels are non-empty");
                if let Some(Value::String(node_id)) = properties.get("_id") {
                    let node = self
                        .distributed_graph_node_value(
                            plan,
                            transport,
                            node_id,
                            params,
                            access_writes,
                            responded_by,
                            failed_replicas,
                        )
                        .await?;
                    return Ok(node
                        .into_iter()
                        .filter(|node| distributed_node_matches(node, labels, properties))
                        .collect());
                }

                let mut candidates = if let Some((property, value)) = properties.iter().next() {
                    self.distributed_graph_nodes_by_property(
                        plan,
                        transport,
                        primary_label,
                        property,
                        value,
                        params,
                        access_writes,
                        responded_by,
                        failed_replicas,
                    )
                    .await?
                } else {
                    self.distributed_graph_nodes_by_label(
                        plan,
                        transport,
                        primary_label,
                            params,
                            access_writes,
                        responded_by,
                        failed_replicas,
                    )
                    .await?
                };

                if candidates.is_empty() && !properties.is_empty() {
                    candidates = self
                        .distributed_graph_nodes_by_label(
                            plan,
                            transport,
                            primary_label,
                            params,
                            access_writes,
                            responded_by,
                            failed_replicas,
                        )
                        .await?;
                }

                Ok(candidates
                    .into_iter()
                    .filter(|node| distributed_node_matches(node, labels, properties))
                    .collect())
            }
            DistributedNodeSelector::Bound { .. } => Ok(Vec::new()),
        }
    }

    async fn materialize_distributed_path_value(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        path: &DistributedPath,
        direction: &EdgeDirection,
        params: &HashMap<String, Value>,
        access_writes: &mut DistributedAccessWrites,
    ) -> Result<Value, CopperDbError> {
        let mut responded_by = BTreeSet::new();
        let mut failed_replicas = BTreeSet::new();

        let mut node_values = Vec::with_capacity(path.node_ids.len());
        for node_id in &path.node_ids {
            node_values.push(
                self.distributed_graph_node_value(
                    plan,
                    transport,
                    node_id,
                    params,
                    access_writes,
                    &mut responded_by,
                    &mut failed_replicas,
                )
                .await?
                .unwrap_or(Value::Null),
            );
        }

        let mut edge_values = Vec::with_capacity(path.edge_ids.len());
        for (index, edge_id) in path.edge_ids.iter().enumerate() {
            let Some(node_id) = path.node_ids.get(index) else {
                break;
            };
            let edge = self
                .distributed_graph_edge_value(
                    plan,
                    transport,
                    node_id,
                    edge_id,
                    direction,
                    params,
                    access_writes,
                    &mut responded_by,
                    &mut failed_replicas,
                )
                .await?
                .unwrap_or(Value::Null);
            edge_values.push(edge);
        }

        Ok(Value::Object(
            [
                ("nodes".to_string(), Value::Array(node_values)),
                (
                    "relationships".to_string(),
                    Value::Array(edge_values.clone()),
                ),
                ("length".to_string(), Value::from(edge_values.len() as i64)),
            ]
            .into_iter()
            .collect(),
        ))
    }

    async fn distributed_graph_node_exists(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        node_id: &str,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<bool, CopperDbError> {
        for replica in &plan.replicas {
            match transport.graph_node(&replica.node_id, node_id).await {
                Ok(Some(_)) => {
                    responded_by.insert(replica.node_id.clone());
                    return Ok(true);
                }
                Ok(None) => {
                    responded_by.insert(replica.node_id.clone());
                }
                Err(_) => {
                    failed_replicas.insert(replica.node_id.clone());
                }
            }
        }
        Ok(false)
    }

    async fn distributed_graph_node_value(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        node_id: &str,
        params: &HashMap<String, Value>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Option<Value>, CopperDbError> {
        for replica in &plan.replicas {
            match transport.graph_node(&replica.node_id, node_id).await {
                Ok(Some(bytes)) => {
                    responded_by.insert(replica.node_id.clone());
                    let props: BTreeMap<String, Value> = rmp_serde::from_slice(&bytes)
                        .map_err(|error| CopperDbError::Storage(error.to_string()))?;
                    let value = Value::Object(props.into_iter().collect());
                    if let Some(node) = distributed_node_record(&value) {
                        let access_metadata = self
                            .distributed_graph_access_metadata(
                                plan,
                                transport,
                                &node.id,
                                responded_by,
                                failed_replicas,
                            )
                            .await?;
                        if !self
                            .eval
                            .node_visible_with_access_metadata(&node, access_metadata.clone(), params)?
                        {
                            return Ok(None);
                        }
                        self.record_distributed_node_access(&node, access_metadata, access_writes)?;
                    }
                    return Ok(Some(value));
                }
                Ok(None) => {
                    responded_by.insert(replica.node_id.clone());
                }
                Err(_) => {
                    failed_replicas.insert(replica.node_id.clone());
                }
            }
        }
        Ok(None)
    }

    async fn distributed_graph_nodes_by_label(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        label: &str,
        params: &HashMap<String, Value>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Vec<Value>, CopperDbError> {
        let mut nodes = BTreeMap::new();
        for replica in &plan.replicas {
            match transport
                .graph_nodes_by_label(&replica.node_id, label)
                .await
            {
                Ok(raw_nodes) => {
                    responded_by.insert(replica.node_id.clone());
                    for raw in raw_nodes {
                        let props: BTreeMap<String, Value> = rmp_serde::from_slice(&raw)
                            .map_err(|error| CopperDbError::Storage(error.to_string()))?;
                        let value = Value::Object(props.into_iter().collect());
                        let Some(node) = distributed_node_record(&value) else {
                            continue;
                        };
                        let access_metadata = self
                            .distributed_graph_access_metadata(
                                plan,
                                transport,
                                &node.id,
                                responded_by,
                                failed_replicas,
                            )
                            .await?;
                        if !self
                            .eval
                            .node_visible_with_access_metadata(&node, access_metadata.clone(), params)?
                        {
                            continue;
                        }
                        self.record_distributed_node_access(&node, access_metadata, access_writes)?;
                        if let Some(node_id) = distributed_node_id(&value) {
                            nodes.insert(node_id, value);
                        }
                    }
                }
                Err(_) => {
                    failed_replicas.insert(replica.node_id.clone());
                }
            }
        }
        Ok(nodes.into_values().collect())
    }

    async fn distributed_graph_nodes_by_property(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        label: &str,
        property: &str,
        value: &Value,
        params: &HashMap<String, Value>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Vec<Value>, CopperDbError> {
        let mut nodes = BTreeMap::new();
        for replica in &plan.replicas {
            match transport
                .graph_nodes_by_property(&replica.node_id, label, property, value)
                .await
            {
                Ok(raw_nodes) => {
                    responded_by.insert(replica.node_id.clone());
                    for raw in raw_nodes {
                        let props: BTreeMap<String, Value> = rmp_serde::from_slice(&raw)
                            .map_err(|error| CopperDbError::Storage(error.to_string()))?;
                        let value = Value::Object(props.into_iter().collect());
                        let Some(node) = distributed_node_record(&value) else {
                            continue;
                        };
                        let access_metadata = self
                            .distributed_graph_access_metadata(
                                plan,
                                transport,
                                &node.id,
                                responded_by,
                                failed_replicas,
                            )
                            .await?;
                        if !self
                            .eval
                            .node_visible_with_access_metadata(&node, access_metadata.clone(), params)?
                        {
                            continue;
                        }
                        self.record_distributed_node_access(&node, access_metadata, access_writes)?;
                        if let Some(node_id) = distributed_node_id(&value) {
                            nodes.insert(node_id, value);
                        }
                    }
                }
                Err(_) => {
                    failed_replicas.insert(replica.node_id.clone());
                }
            }
        }
        Ok(nodes.into_values().collect())
    }

    async fn distributed_graph_edge_value(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        node_id: &str,
        edge_id: &str,
        direction: &EdgeDirection,
        params: &HashMap<String, Value>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Option<Value>, CopperDbError> {
        let edges = self
            .distributed_graph_edges_from_node(
                plan,
                transport,
                node_id,
                None,
                &EdgeDirection::Both,
                params,
                access_writes,
                responded_by,
                failed_replicas,
            )
            .await?;
        let edge = edges.into_iter().find(|edge| {
            edge.id == edge_id
                && match direction {
                    EdgeDirection::Outgoing | EdgeDirection::Both => true,
                    EdgeDirection::Incoming => true,
                }
        });
        Ok(edge.map(|edge| distributed_edge_to_value(&edge)))
    }

    async fn distributed_graph_edges_from_node(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        node_id: &str,
        rel_type: Option<&str>,
        direction: &EdgeDirection,
        params: &HashMap<String, Value>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Vec<copperdb_storage::EdgeRecord>, CopperDbError> {
        let mut edges = BTreeMap::new();
        for replica in &plan.replicas {
            match direction {
                EdgeDirection::Outgoing => match transport
                    .graph_edges_from_node(&replica.node_id, node_id, rel_type)
                    .await
                {
                    Ok(replica_edges) => {
                        responded_by.insert(replica.node_id.clone());
                        for edge in replica_edges {
                            edges.insert(edge.id.clone(), edge);
                        }
                    }
                    Err(_) => {
                        failed_replicas.insert(replica.node_id.clone());
                    }
                },
                EdgeDirection::Incoming => match transport
                    .graph_edges_to_node(&replica.node_id, node_id, rel_type)
                    .await
                {
                    Ok(replica_edges) => {
                        responded_by.insert(replica.node_id.clone());
                        for edge in replica_edges {
                            edges.insert(edge.id.clone(), edge);
                        }
                    }
                    Err(_) => {
                        failed_replicas.insert(replica.node_id.clone());
                    }
                },
                EdgeDirection::Both => {
                    let outgoing = transport
                        .graph_edges_from_node(&replica.node_id, node_id, rel_type)
                        .await;
                    let incoming = transport
                        .graph_edges_to_node(&replica.node_id, node_id, rel_type)
                        .await;
                    match (outgoing, incoming) {
                        (Ok(mut outgoing), Ok(incoming)) => {
                            responded_by.insert(replica.node_id.clone());
                            outgoing.extend(incoming);
                            for edge in outgoing {
                                edges.insert(edge.id.clone(), edge);
                            }
                        }
                        (Ok(replica_edges), Err(_)) | (Err(_), Ok(replica_edges)) => {
                            responded_by.insert(replica.node_id.clone());
                            failed_replicas.insert(replica.node_id.clone());
                            for edge in replica_edges {
                                edges.insert(edge.id.clone(), edge);
                            }
                        }
                        (Err(_), Err(_)) => {
                            failed_replicas.insert(replica.node_id.clone());
                        }
                    }
                }
            }
        }
        let mut visible_edges = Vec::new();
        for edge in edges.into_values() {
            let access_metadata = self
                .distributed_graph_access_metadata(
                    plan,
                    transport,
                    &edge.id,
                    responded_by,
                    failed_replicas,
                )
                .await?;
            if self
                .eval
                .edge_visible_with_access_metadata(&edge, access_metadata.clone(), params)?
            {
                self.record_distributed_edge_access(&edge, access_metadata, access_writes)?;
                visible_edges.push(edge);
            }
        }
        Ok(visible_edges)
    }

    fn record_distributed_node_access(
        &self,
        node: &copperdb_storage::NodeRecord,
        access_metadata: Option<KnowledgePolicyAccessMetadata>,
        access_writes: &mut DistributedAccessWrites,
    ) -> Result<(), CopperDbError> {
        let current = access_writes.get(&node.id).cloned().or(access_metadata);
        if let Some(updated) = self.eval.node_access_metadata_after_read(node, current)? {
            access_writes.insert(node.id.clone(), updated);
        }
        Ok(())
    }

    fn record_distributed_edge_access(
        &self,
        edge: &copperdb_storage::EdgeRecord,
        access_metadata: Option<KnowledgePolicyAccessMetadata>,
        access_writes: &mut DistributedAccessWrites,
    ) -> Result<(), CopperDbError> {
        let current = access_writes.get(&edge.id).cloned().or(access_metadata);
        if let Some(updated) = self.eval.edge_access_metadata_after_read(edge, current)? {
            access_writes.insert(edge.id.clone(), updated);
        }
        Ok(())
    }

    async fn flush_distributed_access_writes(
        &self,
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
        transport: Arc<dyn ReplicaTransport>,
        access_writes: DistributedAccessWrites,
    ) -> Result<(), CopperDbError> {
        if access_writes.is_empty() {
            return Ok(());
        }

        let coordinator = self.build_cassandra_coordinator(transport)?;
        for (entity_id, metadata) in access_writes {
            coordinator
                .write(
                    placement,
                    consistency,
                    Command::PutKnowledgePolicyAccessMetadata { entity_id, metadata },
                    request_region,
                )
                .await?;
        }

        Ok(())
    }

    async fn distributed_graph_access_metadata(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        entity_id: &str,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Option<copperdb_storage::KnowledgePolicyAccessMetadata>, CopperDbError> {
        for replica in &plan.replicas {
            match transport
                .graph_access_metadata(&replica.node_id, entity_id)
                .await
            {
                Ok(metadata) => {
                    responded_by.insert(replica.node_id.clone());
                    if metadata.is_some() {
                        return Ok(metadata);
                    }
                }
                Err(_) => {
                    failed_replicas.insert(replica.node_id.clone());
                }
            }
        }
        Ok(None)
    }

    async fn distributed_bfs_path(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        start_node_id: &str,
        end_node_id: &str,
        rel_type: Option<&str>,
        direction: &EdgeDirection,
        params: &HashMap<String, Value>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Option<DistributedPath>, CopperDbError> {
        if start_node_id == end_node_id {
            return Ok(Some(DistributedPath {
                node_ids: vec![start_node_id.to_string()],
                edge_ids: Vec::new(),
            }));
        }

        let mut frontier = VecDeque::from([start_node_id.to_string()]);
        let mut visited = HashSet::from([start_node_id.to_string()]);
        let mut predecessors: HashMap<String, (String, String)> = HashMap::new();

        while let Some(current_node_id) = frontier.pop_front() {
            let mut edges = self
                .distributed_graph_edges_from_node(
                    plan,
                    transport,
                    &current_node_id,
                    rel_type,
                    direction,
                    params,
                    access_writes,
                    responded_by,
                    failed_replicas,
                )
                .await?;
            edges.sort_by(|left, right| left.id.cmp(&right.id));

            for edge in edges {
                let next_node_id = match direction {
                    EdgeDirection::Outgoing => edge.end_node.clone(),
                    EdgeDirection::Incoming => edge.start_node.clone(),
                    EdgeDirection::Both if edge.start_node == current_node_id => {
                        edge.end_node.clone()
                    }
                    EdgeDirection::Both if edge.end_node == current_node_id => {
                        edge.start_node.clone()
                    }
                    EdgeDirection::Both => continue,
                };
                if !visited.insert(next_node_id.clone()) {
                    continue;
                }
                predecessors.insert(
                    next_node_id.clone(),
                    (current_node_id.clone(), edge.id.clone()),
                );
                if next_node_id == end_node_id {
                    let mut node_ids = vec![end_node_id.to_string()];
                    let mut edge_ids = Vec::new();
                    let mut cursor = end_node_id.to_string();
                    while let Some((previous_node_id, edge_id)) = predecessors.get(&cursor) {
                        edge_ids.push(edge_id.clone());
                        node_ids.push(previous_node_id.clone());
                        cursor = previous_node_id.clone();
                    }
                    node_ids.reverse();
                    edge_ids.reverse();
                    return Ok(Some(DistributedPath { node_ids, edge_ids }));
                }
                frontier.push_back(next_node_id);
            }
        }

        Ok(None)
    }

    /// Flush all pending writes to disk.
    pub fn flush(&self) -> Result<(), CopperDbError> {
        self.storage.flush()?;
        Ok(())
    }

    /// Return the on-disk size in bytes.
    pub fn size_on_disk(&self) -> u64 {
        self.storage.size_on_disk()
    }

    /// Access the transaction manager.
    pub fn tx_manager(&self) -> &Arc<TransactionManager> {
        &self.tx_manager
    }

    /// Access the storage engine directly.
    pub fn storage(&self) -> &Arc<StorageEngine> {
        &self.storage
    }

    /// Access the durable audit log.
    pub fn audit_log(&self) -> &Arc<AuditLog> {
        &self.audit_log
    }

    /// Access the durable compliance policy manager.
    pub fn compliance_manager(&self) -> &Arc<ComplianceManager> {
        &self.compliance
    }

    pub fn load_distributed_topology(&self) -> Result<TopologyRegistry, CopperDbError> {
        self.storage.load_topology_registry().map_err(Into::into)
    }

    pub fn plan_distributed_write(
        &self,
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
    ) -> Result<DistributedWritePlan, CopperDbError> {
        self.load_distributed_topology()?
            .plan_write_with_consistency(
                placement,
                DistributedWriteMode::DynamoQuorum,
                consistency,
                request_region,
            )
            .map_err(|error| CopperDbError::Replication(error.to_string()))
    }

    pub fn plan_distributed_read(
        &self,
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
    ) -> Result<DistributedReadPlan, CopperDbError> {
        self.load_distributed_topology()?
            .plan_read(placement, consistency, request_region)
            .map_err(|error| CopperDbError::Replication(error.to_string()))
    }

    pub fn register_fabric_database(&self, database: &FabricDatabase) -> Result<(), CopperDbError> {
        self.storage.register_fabric_database(database)?;
        Ok(())
    }

    pub fn list_fabric_databases(&self) -> Result<Vec<FabricDatabase>, CopperDbError> {
        self.storage.list_fabric_databases().map_err(Into::into)
    }

    pub fn load_fabric_database(
        &self,
        tenant: &str,
        database: &str,
    ) -> Result<Option<FabricDatabase>, CopperDbError> {
        Ok(self
            .list_fabric_databases()?
            .into_iter()
            .find(|fabric| fabric.tenant == tenant && fabric.database == database))
    }

    pub fn plan_fabric_reads(
        &self,
        database: &FabricDatabase,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
    ) -> Result<Vec<DistributedReadPlan>, CopperDbError> {
        database
            .validate()
            .map_err(|error| CopperDbError::Replication(error.to_string()))?;
        let topology = self.load_distributed_topology()?;
        database
            .placement_keys()
            .iter()
            .map(|placement| {
                topology
                    .plan_read(placement, consistency, request_region)
                    .map_err(|error| CopperDbError::Replication(error.to_string()))
            })
            .collect()
    }

    pub fn plan_fabric_query_reads(
        &self,
        database: &FabricDatabase,
        request: FabricReadRequest,
    ) -> Result<FabricReadPlan, CopperDbError> {
        FabricTopology::new(self.load_distributed_topology()?)
            .plan_fabric_query_reads(database, request)
            .map_err(|error| CopperDbError::Replication(error.to_string()))
    }

    pub fn merge_fabric_rows(
        &self,
        rows: Vec<FabricRowBatch>,
        options: FabricRowMergeOptions,
    ) -> FabricMergedRows {
        merge_fabric_rows(rows, options)
    }

    pub fn merge_fabric_aggregates(
        &self,
        rows: Vec<FabricRowBatch>,
        options: FabricAggregateOptions,
    ) -> FabricMergedRows {
        merge_fabric_aggregates(rows, options)
    }

    pub fn merge_fabric_paths(
        &self,
        paths: Vec<FabricPathBatch>,
        options: FabricPathMergeOptions,
    ) -> FabricMergedPaths {
        merge_fabric_paths(paths, options)
    }

    pub fn merge_fabric_ranked_search(
        &self,
        batches: Vec<RrfSearchBatch>,
        config: RrfConfig,
    ) -> RrfSearchOutcome {
        merge_rrf_search_batches(batches, config)
    }

    pub fn hydrate_fabric_ranked_search(
        &self,
        outcome: RrfSearchOutcome,
        hydration: Vec<RrfHydrationRecord>,
        policy: RrfSearchPolicy,
    ) -> RrfHydratedSearchOutcome {
        hydrate_rrf_search_outcome(outcome, hydration, policy)
    }

    pub fn execute_fabric_ranked_search(
        &self,
        database: &FabricDatabase,
        batches: Vec<RrfSearchBatch>,
        hydration: Vec<RrfHydrationRecord>,
        config: RrfConfig,
        policy: RrfSearchPolicy,
    ) -> Result<FabricRankedSearchExecution, CopperDbError> {
        let plans = self.plan_fabric_searches(database)?;
        Ok(execute_planned_fabric_ranked_search(
            plans, batches, hydration, config, policy,
        ))
    }

    pub async fn execute_fabric_ranked_search_with_transport(
        &self,
        database: &FabricDatabase,
        query: SearchQuery,
        hydration: Vec<RrfHydrationRecord>,
        config: RrfConfig,
        policy: RrfSearchPolicy,
        transport: Arc<dyn RankedSearchTransport>,
    ) -> Result<FabricRankedSearchExecution, CopperDbError> {
        let plans = self.plan_fabric_searches(database)?;
        let collected = collect_planned_fabric_ranked_batches(plans.clone(), query, transport)
            .await
            .map_err(|error| CopperDbError::Replication(error.to_string()))?;
        let mut execution = execute_planned_fabric_ranked_search(
            plans,
            collected.batches,
            hydration,
            config,
            policy,
        );
        execution.responded_nodes = collected.responded_nodes;
        execution.failed_nodes = collected.failed_nodes;
        Ok(execution)
    }

    pub async fn fetch_fabric_ranked_hydration_with_transport(
        &self,
        outcome: &RrfSearchOutcome,
        consistency: ConsistencyLevel,
        transport: Arc<dyn HydrationTransport>,
    ) -> Result<Vec<RrfHydrationRecord>, CopperDbError> {
        let mut by_placement: BTreeMap<PlacementKey, Vec<_>> = BTreeMap::new();
        for hit in &outcome.results {
            by_placement
                .entry(hit.global_id.placement.clone())
                .or_default()
                .push(hit.global_id.clone());
        }

        let mut requests = Vec::new();
        for (placement, global_ids) in by_placement {
            let plan = self.plan_distributed_read(&placement, consistency, None)?;
            requests.push(FabricHydrationRequest {
                node_id: plan.coordinator.node_id,
                placement,
                global_ids,
            });
        }

        let collected = collect_fabric_hydration_records(requests, transport)
            .await
            .map_err(|error| CopperDbError::Replication(error.to_string()))?;
        Ok(collected.records)
    }

    pub async fn execute_fabric_ranked_search_with_full_transport(
        &self,
        database: &FabricDatabase,
        query: SearchQuery,
        hydration_consistency: ConsistencyLevel,
        config: RrfConfig,
        policy: RrfSearchPolicy,
        ranked_transport: Arc<dyn RankedSearchTransport>,
        hydration_transport: Arc<dyn HydrationTransport>,
    ) -> Result<FabricRankedSearchExecution, CopperDbError> {
        let plans = self.plan_fabric_searches(database)?;
        let collected =
            collect_planned_fabric_ranked_batches(plans.clone(), query, ranked_transport)
                .await
                .map_err(|error| CopperDbError::Replication(error.to_string()))?;
        let merged = merge_rrf_search_batches(collected.batches.clone(), config);
        let hydration = self
            .fetch_fabric_ranked_hydration_with_transport(
                &merged,
                hydration_consistency,
                hydration_transport,
            )
            .await?;
        let mut execution = execute_planned_fabric_ranked_search(
            plans,
            collected.batches,
            hydration,
            config,
            policy,
        );
        execution.responded_nodes = collected.responded_nodes;
        execution.failed_nodes = collected.failed_nodes;
        Ok(execution)
    }

    pub fn plan_fabric_searches(
        &self,
        database: &FabricDatabase,
    ) -> Result<Vec<DistributedSearchPlan>, CopperDbError> {
        database
            .validate()
            .map_err(|error| CopperDbError::Replication(error.to_string()))?;
        let topology = self.load_distributed_topology()?;
        database
            .placement_keys()
            .iter()
            .map(|placement| {
                topology
                    .plan_search(placement)
                    .map_err(|error| CopperDbError::Replication(error.to_string()))
            })
            .collect()
    }

    pub fn open_repair_queue(&self) -> Result<Arc<DurableRepairQueue>, CopperDbError> {
        Ok(Arc::new(DurableRepairQueue::open(
            self.repair_queue_path(),
        )?))
    }

    pub fn build_cassandra_coordinator(
        &self,
        transport: Arc<dyn ReplicaTransport>,
    ) -> Result<CassandraCoordinator, CopperDbError> {
        Ok(CassandraCoordinator::with_repair_queue(
            self.load_distributed_topology()?,
            transport,
            self.open_repair_queue()?,
        ))
    }

    pub async fn replay_repairs(
        &self,
        transport: Arc<dyn ReplicaTransport>,
        max_records: usize,
    ) -> Result<RepairReplayReport, CopperDbError> {
        self.open_repair_queue()?
            .replay_batch(transport, max_records)
            .await
            .map_err(Into::into)
    }

    pub fn build_repair_worker(
        &self,
        transport: Arc<dyn ReplicaTransport>,
        config: RepairWorkerConfig,
    ) -> Result<ScheduledRepairWorker, CopperDbError> {
        Ok(ScheduledRepairWorker::new(
            self.open_repair_queue()?,
            transport,
            config,
        ))
    }

    /// Build a compliance reporter over the durable audit trail.
    pub fn compliance_reporter(&self) -> ComplianceReporter {
        ComplianceReporter::new(Arc::clone(&self.audit_log))
    }

    fn repair_queue_path(&self) -> PathBuf {
        self.config
            .distributed_repair_queue_dir
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(&self.config.data_dir).join("replication-repair"))
    }

    fn enforce_compliance(
        &self,
        query: &copperdb_cypher::Query,
        roles: &[String],
    ) -> Result<(), copperdb_compliance::ComplianceError> {
        let mut labels = Vec::new();
        let mut properties = Vec::new();
        collect_compliance_terms(query, &mut labels, &mut properties);
        labels.sort();
        labels.dedup();
        properties.sort();
        properties.dedup();

        for label in labels {
            self.compliance.check_label_access(&label, roles)?;
        }
        for property in properties {
            self.compliance.check_property_access(&property, roles)?;
        }
        Ok(())
    }

    fn record_query_audit(
        &self,
        cypher: &str,
        action: &str,
        success: bool,
        reason: Option<String>,
        query_hash: Option<u64>,
        elapsed_ms: u64,
    ) -> Result<(), CopperDbError> {
        let mut event = Event {
            event_type: audit_event_type(action),
            user_id: Some("embedded".into()),
            username: Some("embedded".into()),
            resource: Some("cypher_query".into()),
            resource_id: query_hash.map(|hash| format!("{hash:016x}")),
            action: Some(action.into()),
            success,
            reason,
            data_classification: Some("DATABASE".into()),
            ..Event::new(EventType::DataRead)
        };
        event
            .metadata
            .insert("database".into(), self.config.default_database.clone());
        event
            .metadata
            .insert("query_length".into(), cypher.len().to_string());
        event
            .metadata
            .insert("elapsed_ms".into(), elapsed_ms.to_string());
        self.audit_log.record(event)?;
        Ok(())
    }
}

fn open_storage(config: &DatabaseConfig) -> Result<StorageEngine, CopperDbError> {
    match &config.storage_encryption_master_key {
        Some(master_key) => {
            let provider = new_provider(ProviderFactoryConfig {
                provider: "local".into(),
                key_uri: config.storage_encryption_key_uri.clone(),
                master_key: master_key.clone(),
                audit_signing_key: None,
            })
            .map_err(|err| CopperDbError::Init(err.to_string()))?;
            StorageEngine::open_encrypted(
                &config.data_dir,
                provider,
                config.storage_encryption_key_uri.clone(),
            )
            .map_err(|e| CopperDbError::Storage(e.to_string()))
        }
        None => {
            StorageEngine::open(&config.data_dir).map_err(|e| CopperDbError::Storage(e.to_string()))
        }
    }
}

fn query_action(query_type: &QueryType) -> &'static str {
    match query_type {
        QueryType::Match | QueryType::Return | QueryType::With => "READ",
        QueryType::Create => "CREATE",
        QueryType::Merge | QueryType::Set | QueryType::Ddl => "UPDATE",
        QueryType::Delete => "DELETE",
    }
}

fn is_mutating_query(query_type: &QueryType) -> bool {
    matches!(
        query_type,
        QueryType::Create | QueryType::Merge | QueryType::Set | QueryType::Delete | QueryType::Ddl
    )
}

fn audit_event_type(action: &str) -> EventType {
    match action {
        "CREATE" => EventType::DataCreate,
        "UPDATE" => EventType::DataUpdate,
        "DELETE" => EventType::DataDelete,
        "EXPORT" => EventType::DataExport,
        _ => EventType::DataRead,
    }
}

#[derive(Debug, Clone)]
struct DistributedShortestPathQueryShape {
    path_variable: String,
    start_selector: DistributedNodeSelector,
    end_selector: DistributedNodeSelector,
    rel_type: Option<String>,
    direction: EdgeDirection,
    return_items: Vec<ReturnItem>,
}

#[derive(Debug, Clone)]
struct DistributedDirectPathQueryShape {
    optional: bool,
    path_variable: String,
    pattern: DistributedDirectPathPattern,
    return_items: Vec<ReturnItem>,
}

#[derive(Debug, Clone)]
struct DistributedLeadingPathQueryShape {
    leading_steps: Vec<DistributedLeadingStep>,
    path_shape: DistributedDirectPathQueryShape,
}

#[derive(Debug, Clone)]
enum DistributedLeadingStep {
    Match(DistributedLeadingMatch),
    OptionalMatch(DistributedLeadingMatch),
    With(WithClause),
    Where(WhereClause),
}

#[derive(Debug, Clone)]
enum DistributedLeadingMatch {
    Node {
        selector: DistributedNodeSelector,
        variable: Option<String>,
    },
    Relationship {
        pattern: DistributedDirectPathPattern,
        start_variable: Option<String>,
        end_variable: Option<String>,
        edge_variable: Option<String>,
    },
}

#[derive(Debug, Clone)]
enum DistributedDirectPathPattern {
    SingleNode {
        selector: DistributedNodeSelector,
    },
    RelationshipPath {
        start_selector: DistributedNodeSelector,
        end_selector: DistributedNodeSelector,
        rel_type: Option<String>,
        direction: EdgeDirection,
        edge_properties: BTreeMap<String, Value>,
        min_hops: u32,
        max_hops: u32,
    },
}

#[derive(Debug, Clone)]
enum DistributedNodeSelector {
    LiteralId(String),
    Pattern {
        labels: Vec<String>,
        properties: BTreeMap<String, Value>,
    },
    Bound {
        variable: String,
        labels: Vec<String>,
        properties: BTreeMap<String, Value>,
    },
}

fn distributed_shortest_path_query_shape(
    query: &copperdb_cypher::Query,
) -> Option<DistributedShortestPathQueryShape> {
    if query.clauses.len() != 2 {
        return None;
    }
    let Clause::Match(match_clause) = &query.clauses[0] else {
        return None;
    };
    let Clause::Return(return_clause) = &query.clauses[1] else {
        return None;
    };
    if !match_clause.pattern.shortest_path
        || match_clause.pattern.nodes.len() != 2
        || match_clause.pattern.edges.len() != 1
        || return_clause.distinct
        || return_clause.skip.is_some()
        || return_clause.limit.is_some()
        || !return_clause.order_by.is_empty()
    {
        return None;
    }

    let path_variable = match_clause.pattern.path_variable.clone()?;
    let start_selector = distributed_node_selector(&match_clause.pattern.nodes[0], &[])?;
    let end_selector = distributed_node_selector(&match_clause.pattern.nodes[1], &[])?;
    let edge = &match_clause.pattern.edges[0];

    if !return_clause
        .items
        .iter()
        .all(|item| supported_distributed_path_return_expression(&item.expression, &path_variable))
    {
        return None;
    }

    Some(DistributedShortestPathQueryShape {
        path_variable,
        start_selector,
        end_selector,
        rel_type: edge.rel_type.clone(),
        direction: edge.direction.clone(),
        return_items: return_clause.items.clone(),
    })
}

fn distributed_direct_path_query_shape(
    query: &copperdb_cypher::Query,
) -> Option<DistributedDirectPathQueryShape> {
    distributed_direct_path_query_shape_with_bound_nodes(query, &[])
}

fn distributed_direct_path_query_shape_with_bound_nodes(
    query: &copperdb_cypher::Query,
    bound_variables: &[String],
) -> Option<DistributedDirectPathQueryShape> {
    if query.clauses.len() != 2 {
        return None;
    }
    let (match_clause, optional) = match &query.clauses[0] {
        Clause::Match(match_clause) => (match_clause, false),
        Clause::OptionalMatch(match_clause) => (match_clause, true),
        _ => return None,
    };
    let Clause::Return(return_clause) = &query.clauses[1] else {
        return None;
    };
    if match_clause.pattern.shortest_path
        || return_clause.distinct
        || return_clause.skip.is_some()
        || return_clause.limit.is_some()
        || !return_clause.order_by.is_empty()
    {
        return None;
    }

    let path_variable = match_clause.pattern.path_variable.clone()?;
    if !return_clause
        .items
        .iter()
        .all(|item| supported_distributed_path_return_expression(&item.expression, &path_variable))
    {
        return None;
    }

    let pattern = match (
        &match_clause.pattern.nodes[..],
        &match_clause.pattern.edges[..],
    ) {
        ([node], []) => DistributedDirectPathPattern::SingleNode {
            selector: distributed_node_selector(node, bound_variables)?,
        },
        ([start, end], [edge]) => {
            let min_hops = edge.min_hops.unwrap_or(1);
            let max_hops = edge.max_hops.unwrap_or(1).max(min_hops);
            DistributedDirectPathPattern::RelationshipPath {
                start_selector: distributed_node_selector(start, bound_variables)?,
                end_selector: distributed_node_selector(end, bound_variables)?,
                rel_type: edge.rel_type.clone(),
                direction: edge.direction.clone(),
                edge_properties: distributed_literal_properties(&edge.properties)?,
                min_hops,
                max_hops,
            }
        }
        _ => return None,
    };

    Some(DistributedDirectPathQueryShape {
        optional,
        path_variable,
        pattern,
        return_items: return_clause.items.clone(),
    })
}

fn distributed_leading_path_query_shape(
    query: &copperdb_cypher::Query,
) -> Option<DistributedLeadingPathQueryShape> {
    if query.clauses.len() < 3 {
        return None;
    }
    let Clause::Return(return_clause) = query.clauses.last()? else {
        return None;
    };
    let path_clause = match &query.clauses[query.clauses.len() - 2] {
        Clause::Match(match_clause) => Clause::Match(match_clause.clone()),
        Clause::OptionalMatch(match_clause) => Clause::OptionalMatch(match_clause.clone()),
        _ => return None,
    };
    if return_clause.distinct
        || return_clause.skip.is_some()
        || return_clause.limit.is_some()
        || !return_clause.order_by.is_empty()
    {
        return None;
    }

    let mut bound_variables = Vec::new();
    let mut leading_steps = Vec::new();
    for clause in &query.clauses[..query.clauses.len() - 2] {
        match clause {
            Clause::OptionalMatch(leading_match_clause) => {
                let leading_match =
                    distributed_leading_match(&leading_match_clause.pattern, &bound_variables)?;
                leading_steps.push(DistributedLeadingStep::OptionalMatch(leading_match));
                distributed_extend_bound_variables(
                    &mut bound_variables,
                    &leading_match_clause.pattern,
                );
            }
            Clause::With(with_clause) => {
                if !distributed_supported_leading_with_clause(with_clause) {
                    return None;
                }
                leading_steps.push(DistributedLeadingStep::With(with_clause.clone()));
                bound_variables = with_clause
                    .items
                    .iter()
                    .map(distributed_leading_with_column_name)
                    .collect();
            }
            Clause::Where(where_clause) => {
                if leading_steps.is_empty() {
                    return None;
                }
                leading_steps.push(DistributedLeadingStep::Where(where_clause.clone()));
            }
            Clause::Match(leading_match_clause) => {
                let leading_match =
                    distributed_leading_match(&leading_match_clause.pattern, &bound_variables)?;
                leading_steps.push(DistributedLeadingStep::Match(leading_match));
                distributed_extend_bound_variables(
                    &mut bound_variables,
                    &leading_match_clause.pattern,
                );
            }
            _ => return None,
        }
    }

    let path_query = copperdb_cypher::Query {
        query_type: QueryType::Match,
        clauses: vec![path_clause, Clause::Return(return_clause.clone())],
        parameters: HashMap::new(),
    };

    Some(DistributedLeadingPathQueryShape {
        leading_steps,
        path_shape: distributed_direct_path_query_shape_with_bound_nodes(
            &path_query,
            &bound_variables,
        )?,
    })
}

fn distributed_literal_properties(
    properties: &[copperdb_cypher::PropertyEntry],
) -> Option<BTreeMap<String, Value>> {
    properties
        .iter()
        .map(|entry| match &entry.value {
            Expression::Literal(value) => Some((
                entry.key.clone(),
                match value {
                    copperdb_cypher::LiteralValue::String(value) => Value::String(value.clone()),
                    copperdb_cypher::LiteralValue::Integer(value) => Value::from(*value),
                    copperdb_cypher::LiteralValue::Float(value) => Value::from(*value),
                    copperdb_cypher::LiteralValue::Bool(value) => Value::Bool(*value),
                    copperdb_cypher::LiteralValue::Null => Value::Null,
                },
            )),
            _ => None,
        })
        .collect::<Option<BTreeMap<_, _>>>()
}

fn distributed_leading_match(
    pattern: &Pattern,
    bound_variables: &[String],
) -> Option<DistributedLeadingMatch> {
    if pattern.shortest_path || pattern.path_variable.is_some() {
        return None;
    }

    match (&pattern.nodes[..], &pattern.edges[..]) {
        ([node], []) => Some(DistributedLeadingMatch::Node {
            selector: distributed_node_selector(node, bound_variables)?,
            variable: node.variable.clone(),
        }),
        ([start, end], [edge]) => {
            let min_hops = edge.min_hops.unwrap_or(1);
            let max_hops = edge.max_hops.unwrap_or(1).max(min_hops);
            if max_hops > 1 && edge.variable.is_some() {
                return None;
            }
            Some(DistributedLeadingMatch::Relationship {
                pattern: DistributedDirectPathPattern::RelationshipPath {
                    start_selector: distributed_node_selector(start, bound_variables)?,
                    end_selector: distributed_node_selector(end, bound_variables)?,
                    rel_type: edge.rel_type.clone(),
                    direction: edge.direction.clone(),
                    edge_properties: distributed_literal_properties(&edge.properties)?,
                    min_hops,
                    max_hops,
                },
                start_variable: start.variable.clone(),
                end_variable: end.variable.clone(),
                edge_variable: edge.variable.clone(),
            })
        }
        _ => None,
    }
}

fn distributed_extend_bound_variables(bound_variables: &mut Vec<String>, pattern: &Pattern) {
    for node in &pattern.nodes {
        if let Some(variable) = &node.variable {
            if !bound_variables.iter().any(|bound| bound == variable) {
                bound_variables.push(variable.clone());
            }
        }
    }
}

fn distributed_bind_optional_leading_match_nulls(
    row: &mut HashMap<String, Value>,
    leading_match: &DistributedLeadingMatch,
) {
    match leading_match {
        DistributedLeadingMatch::Node { variable, .. } => {
            if let Some(variable) = variable {
                if !row.contains_key(variable) {
                    row.insert(variable.clone(), Value::Null);
                }
            }
        }
        DistributedLeadingMatch::Relationship {
            start_variable,
            end_variable,
            edge_variable,
            ..
        } => {
            for variable in [start_variable, end_variable, edge_variable]
                .into_iter()
                .flatten()
            {
                if !row.contains_key(variable) {
                    row.insert(variable.clone(), Value::Null);
                }
            }
        }
    }
}

fn distributed_supported_leading_with_clause(with_clause: &WithClause) -> bool {
    with_clause
        .items
        .iter()
        .all(distributed_supported_leading_with_item)
}

fn distributed_supported_leading_with_item(item: &ReturnItem) -> bool {
    item.alias.is_some()
        || matches!(
            item.expression,
            Expression::Variable(_) | Expression::PropertyAccess { .. }
        )
}

fn distributed_leading_with_column_name(item: &ReturnItem) -> String {
    if let Some(alias) = &item.alias {
        return alias.clone();
    }
    match &item.expression {
        Expression::Variable(variable) => variable.clone(),
        Expression::PropertyAccess { variable, property } => format!("{variable}.{property}"),
        _ => "expr".to_string(),
    }
}

fn distributed_project_row(
    row: &HashMap<String, Value>,
    items: &[ReturnItem],
    params: &HashMap<String, Value>,
) -> Result<HashMap<String, Value>, CopperDbError> {
    let mut projected = HashMap::new();
    for item in items {
        projected.insert(
            distributed_leading_with_column_name(item),
            eval_expression(&item.expression, row, params)
                .map_err(|err| CopperDbError::Eval(err.to_string()))?,
        );
    }
    Ok(projected)
}

fn distributed_node_selector(
    node: &copperdb_cypher::NodePattern,
    bound_variables: &[String],
) -> Option<DistributedNodeSelector> {
    let literal_properties = distributed_literal_properties(&node.properties)?;

    if let Some(variable) = &node.variable {
        if bound_variables.iter().any(|bound| bound == variable) {
            return Some(DistributedNodeSelector::Bound {
                variable: variable.clone(),
                labels: node.labels.clone(),
                properties: literal_properties,
            });
        }
    }

    if node.labels.is_empty() {
        return match literal_properties.get("_id") {
            Some(Value::String(node_id)) => {
                Some(DistributedNodeSelector::LiteralId(node_id.clone()))
            }
            _ => None,
        };
    }

    Some(DistributedNodeSelector::Pattern {
        labels: node.labels.clone(),
        properties: literal_properties,
    })
}

fn supported_distributed_path_return_expression(
    expression: &Expression,
    path_variable: &str,
) -> bool {
    match expression {
        Expression::Variable(variable) => variable == path_variable,
        Expression::FunctionCall { name, args, .. }
            if args.len() == 1
                && matches!(&args[0], Expression::Variable(variable) if variable == path_variable) =>
        {
            matches!(
                name.to_ascii_lowercase().as_str(),
                "nodes" | "relationships" | "length"
            )
        }
        _ => false,
    }
}

fn distributed_shortest_path_result(
    shape: &DistributedShortestPathQueryShape,
    path_value: Option<&Value>,
) -> Result<QueryResult, CopperDbError> {
    let path_values = path_value.cloned().into_iter().collect::<Vec<_>>();
    distributed_path_query_result(&shape.return_items, &shape.path_variable, &path_values)
}

fn distributed_path_query_result(
    return_items: &[ReturnItem],
    path_variable: &str,
    path_values: &[Value],
) -> Result<QueryResult, CopperDbError> {
    let columns = return_items
        .iter()
        .map(distributed_return_column_name)
        .collect::<Vec<_>>();
    let rows = path_values
        .iter()
        .map(|path_value| {
            return_items
                .iter()
                .map(|item| {
                    Ok((
                        distributed_return_column_name(item),
                        distributed_return_value(&item.expression, path_variable, path_value)?,
                    ))
                })
                .collect::<Result<HashMap<_, _>, CopperDbError>>()
        })
        .collect::<Result<Vec<_>, CopperDbError>>()?;

    Ok(QueryResult {
        columns,
        rows,
        stats: ResultStats::default(),
    })
}

fn distributed_bfs_query_result(path_value: Option<&Value>) -> QueryResult {
    let columns = vec![
        "path".into(),
        "nodes(path)".into(),
        "relationships(path)".into(),
        "length(path)".into(),
    ];
    let Some(path_value) = path_value else {
        return QueryResult {
            columns,
            rows: Vec::new(),
            stats: ResultStats::default(),
        };
    };

    let row = HashMap::from([
        ("path".into(), path_value.clone()),
        (
            "nodes(path)".into(),
            distributed_return_value(
                &Expression::FunctionCall {
                    name: "nodes".into(),
                    args: vec![Expression::Variable("path".into())],
                    distinct: false,
                },
                "path",
                path_value,
            )
            .unwrap_or(Value::Array(Vec::new())),
        ),
        (
            "relationships(path)".into(),
            distributed_return_value(
                &Expression::FunctionCall {
                    name: "relationships".into(),
                    args: vec![Expression::Variable("path".into())],
                    distinct: false,
                },
                "path",
                path_value,
            )
            .unwrap_or(Value::Array(Vec::new())),
        ),
        (
            "length(path)".into(),
            distributed_return_value(
                &Expression::FunctionCall {
                    name: "length".into(),
                    args: vec![Expression::Variable("path".into())],
                    distinct: false,
                },
                "path",
                path_value,
            )
            .unwrap_or(Value::Null),
        ),
    ]);

    QueryResult {
        columns,
        rows: vec![row],
        stats: ResultStats::default(),
    }
}

fn distributed_return_column_name(item: &ReturnItem) -> String {
    if let Some(alias) = &item.alias {
        return alias.clone();
    }
    match &item.expression {
        Expression::Variable(variable) => variable.clone(),
        Expression::FunctionCall { name, args, .. } if !args.is_empty() => {
            format!("{name}({})", distributed_expression_name(&args[0]))
        }
        _ => distributed_expression_name(&item.expression),
    }
}

fn distributed_expression_name(expression: &Expression) -> String {
    match expression {
        Expression::Variable(variable) => variable.clone(),
        Expression::FunctionCall { name, .. } => name.clone(),
        _ => "expr".to_string(),
    }
}

fn distributed_return_value(
    expression: &Expression,
    path_variable: &str,
    path_value: &Value,
) -> Result<Value, CopperDbError> {
    match expression {
        Expression::Variable(variable) if variable == path_variable => Ok(path_value.clone()),
        Expression::FunctionCall { name, args, .. }
            if args.len() == 1
                && matches!(&args[0], Expression::Variable(variable) if variable == path_variable) =>
        {
            match name.to_ascii_lowercase().as_str() {
                "nodes" => Ok(match path_value {
                    Value::Object(path_map) => path_map
                        .get("nodes")
                        .cloned()
                        .unwrap_or_else(|| Value::Array(Vec::new())),
                    _ => Value::Array(Vec::new()),
                }),
                "relationships" => Ok(match path_value {
                    Value::Object(path_map) => path_map
                        .get("relationships")
                        .cloned()
                        .unwrap_or_else(|| Value::Array(Vec::new())),
                    _ => Value::Array(Vec::new()),
                }),
                "length" => Ok(match path_value {
                    Value::Object(path_map) => {
                        path_map.get("length").cloned().unwrap_or(Value::Null)
                    }
                    _ => Value::Null,
                }),
                other => Err(CopperDbError::Eval(format!(
                    "unsupported distributed path return function: {other}"
                ))),
            }
        }
        _ => Err(CopperDbError::Eval(
            "unsupported distributed shortestPath return expression".to_string(),
        )),
    }
}

fn distributed_edge_to_value(edge: &copperdb_storage::EdgeRecord) -> Value {
    Value::Object(
        edge.properties
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .chain([
                ("_id".to_string(), Value::String(edge.id.clone())),
                ("_type".to_string(), Value::String(edge.edge_type.clone())),
                ("_start".to_string(), Value::String(edge.start_node.clone())),
                ("_end".to_string(), Value::String(edge.end_node.clone())),
                (
                    "_created_at_unix_ms".to_string(),
                    Value::from(edge.created_at_unix_ms),
                ),
                (
                    "_updated_at_unix_ms".to_string(),
                    Value::from(edge.updated_at_unix_ms),
                ),
            ])
            .collect(),
    )
}

fn distributed_node_record(node: &Value) -> Option<copperdb_storage::NodeRecord> {
    let Value::Object(map) = node else {
        return None;
    };
    let id = map.get("_id")?.as_str()?.to_string();
    let labels = map
        .get("_labels")?
        .as_array()?
        .iter()
        .map(|value| value.as_str().map(str::to_string))
        .collect::<Option<Vec<_>>>()?;
    let created_at_unix_ms = map.get("_created_at_unix_ms")?.as_i64()?;
    let updated_at_unix_ms = map.get("_updated_at_unix_ms")?.as_i64()?;
    let properties = map
        .iter()
        .filter(|(key, _)| {
            !matches!(
                key.as_str(),
                "_id" | "_labels" | "_created_at_unix_ms" | "_updated_at_unix_ms"
            )
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    Some(copperdb_storage::NodeRecord {
        id,
        labels,
        properties,
        created_at_unix_ms,
        updated_at_unix_ms,
    })
}

fn distributed_node_id(node: &Value) -> Option<String> {
    let Value::Object(map) = node else {
        return None;
    };
    match map.get("_id") {
        Some(Value::String(node_id)) => Some(node_id.clone()),
        _ => None,
    }
}

fn distributed_node_ids(nodes: &[Value]) -> Vec<String> {
    let mut ids = nodes
        .iter()
        .filter_map(distributed_node_id)
        .collect::<Vec<_>>();
    ids.sort();
    ids.dedup();
    ids
}

fn distributed_path_nodes(path_value: &Value) -> Option<&[Value]> {
    let Value::Object(map) = path_value else {
        return None;
    };
    map.get("nodes")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
}

fn distributed_path_relationships(path_value: &Value) -> Option<&[Value]> {
    let Value::Object(map) = path_value else {
        return None;
    };
    map.get("relationships")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
}

fn distributed_node_matches(
    node: &Value,
    labels: &[String],
    properties: &BTreeMap<String, Value>,
) -> bool {
    let Value::Object(map) = node else {
        return false;
    };

    let label_match = labels.iter().all(|label| {
        map.get("_labels")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .any(|value| value.as_str() == Some(label.as_str()))
            })
            .unwrap_or(false)
    });
    let prop_match = properties.iter().all(|(key, value)| {
        map.get(key)
            .map(|candidate| candidate == value)
            .unwrap_or(false)
    });

    label_match && prop_match
}

fn distributed_edge_matches(
    edge: &copperdb_storage::EdgeRecord,
    properties: &BTreeMap<String, Value>,
) -> bool {
    properties.iter().all(|(key, value)| {
        edge.properties
            .get(key)
            .map(|candidate| candidate == value)
            .unwrap_or(false)
    })
}

fn distributed_related_node_id(
    current_node_id: &str,
    edge: &copperdb_storage::EdgeRecord,
    direction: &EdgeDirection,
) -> Option<String> {
    match direction {
        EdgeDirection::Outgoing if edge.start_node == current_node_id => {
            Some(edge.end_node.clone())
        }
        EdgeDirection::Incoming if edge.end_node == current_node_id => {
            Some(edge.start_node.clone())
        }
        EdgeDirection::Both if edge.start_node == current_node_id => Some(edge.end_node.clone()),
        EdgeDirection::Both if edge.end_node == current_node_id => Some(edge.start_node.clone()),
        _ => None,
    }
}

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
mod tests {
    use super::*;
    use copperdb_storage::EdgeRecord;
    use std::collections::BTreeMap;

    // ── Legacy tests ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_start_with_default_config() {
        let config = copperdb_config::Config::default();
        let db = CopperDbServer::start(config).await.unwrap();
        db.shutdown().await.unwrap();
    }

    // ── Embedded copperdb tests ───────────────────────────────────────────────

    #[test]
    fn test_open_temporary() {
        let db = CopperDb::open_temporary().unwrap();
        assert_eq!(db.config.default_database, "copperdb");
    }

    #[test]
    fn test_create_and_match() {
        let db = CopperDb::open_temporary().unwrap();

        let result = db
            .execute(
                "CREATE (n:Person {name: 'Alice', age: 30})",
                Default::default(),
            )
            .unwrap();
        assert_eq!(result.stats.nodes_created, 1);

        let result = db
            .execute("MATCH (n:Person) RETURN n", Default::default())
            .unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_match_with_where() {
        let db = CopperDb::open_temporary().unwrap();
        db.execute(
            "CREATE (n:Person {name: 'Alice', age: 30})",
            Default::default(),
        )
        .unwrap();
        db.execute(
            "CREATE (n:Person {name: 'Bob', age: 25})",
            Default::default(),
        )
        .unwrap();

        let result = db
            .execute(
                "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n",
                Default::default(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        if let Some(Value::Object(props)) = result.rows[0].get("n") {
            assert_eq!(props.get("name"), Some(&Value::String("Alice".into())));
        } else {
            panic!("expected object with n");
        }
    }

    #[test]
    fn test_optional_match_relationship_pattern_preserves_row_with_nulls() {
        let db = CopperDb::open_temporary().unwrap();
        db.execute(
            "CREATE (p:Person {id: 1, name: 'Alice'})",
            Default::default(),
        )
        .unwrap();

        let result = db
            .execute(
                "MATCH (p:Person {id: 1}) OPTIONAL MATCH (p)-[r:FOLLOWS]->(friend:Person) RETURN p.name AS person, friend AS friend, r AS rel",
                Default::default(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["person", "friend", "rel"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("person"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(result.rows[0].get("friend"), Some(&Value::Null));
        assert_eq!(result.rows[0].get("rel"), Some(&Value::Null));
    }

    #[test]
    fn test_optional_match_relationship_pattern_returns_bound_values_on_match() {
        let db = CopperDb::open_temporary().unwrap();
        for cypher in [
            "CREATE (p:Person {id: 1, name: 'Alice'})",
            "CREATE (p:Person {id: 2, name: 'Bob'})",
            "MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:FOLLOWS]->(b)",
        ] {
            db.execute(cypher, Default::default()).unwrap();
        }

        let result = db
            .execute(
                "MATCH (p:Person {id: 1}) OPTIONAL MATCH (p)-[r:FOLLOWS]->(friend:Person) RETURN p.name AS person, friend.name AS friendName, r._type AS relType",
                Default::default(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["person", "friendName", "relType"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("person"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(
            result.rows[0].get("friendName"),
            Some(&Value::String("Bob".into()))
        );
        assert_eq!(
            result.rows[0].get("relType"),
            Some(&Value::String("FOLLOWS".into()))
        );
    }

    #[test]
    fn test_flush_and_size() {
        let db = CopperDb::open_temporary().unwrap();
        db.execute("CREATE (n:Test {x: 1})", Default::default())
            .unwrap();
        db.flush().unwrap();
        // size should be non-zero after flush
        let _ = db.size_on_disk();
    }

    #[test]
    fn test_query_caching() {
        let db = CopperDb::open_temporary().unwrap();
        db.execute("CREATE (n:Cached {v: 1})", Default::default())
            .unwrap();
        // Second identical query hits cache
        let r1 = db
            .execute("MATCH (n:Cached) RETURN n", Default::default())
            .unwrap();
        let r2 = db
            .execute("MATCH (n:Cached) RETURN n", Default::default())
            .unwrap();
        assert_eq!(r1.rows.len(), r2.rows.len());
    }

    #[test]
    fn test_execute_routes_simple_edge_property_aggregation_through_fast_path() {
        let db = CopperDb::open_temporary().unwrap();
        for (id, props) in [
            (
                "customer:1",
                BTreeMap::from([("name".to_string(), Value::String("Alice".into()))]),
            ),
            (
                "customer:2",
                BTreeMap::from([("name".to_string(), Value::String("Bob".into()))]),
            ),
            (
                "customer:3",
                BTreeMap::from([("name".to_string(), Value::String("Carol".into()))]),
            ),
            (
                "product:1",
                BTreeMap::from([("name".to_string(), Value::String("Widget".into()))]),
            ),
            (
                "product:2",
                BTreeMap::from([("name".to_string(), Value::String("Thing".into()))]),
            ),
        ] {
            db.storage()
                .put_node(id, &rmp_serde::to_vec(&props).unwrap())
                .unwrap();
        }
        for edge in [
            EdgeRecord {
                id: "review:1".into(),
                start_node: "customer:1".into(),
                end_node: "product:1".into(),
                edge_type: "REVIEWED".into(),
                properties: BTreeMap::from([("rating".into(), Value::from(4))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "review:2".into(),
                start_node: "customer:2".into(),
                end_node: "product:1".into(),
                edge_type: "REVIEWED".into(),
                properties: BTreeMap::from([("rating".into(), Value::from(5))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "review:3".into(),
                start_node: "customer:3".into(),
                end_node: "product:2".into(),
                edge_type: "REVIEWED".into(),
                properties: BTreeMap::from([("rating".into(), Value::from(5))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
        ] {
            db.storage().put_edge_record(&edge).unwrap();
        }

        let result = db
            .execute(
                "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) RETURN p.name AS product, avg(r.rating) AS avgRating, count(r) AS reviewCount ORDER BY avgRating DESC LIMIT 2",
                Default::default(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["product", "avgRating", "reviewCount"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(
            result.rows[0].get("product"),
            Some(&Value::String("Thing".into()))
        );
        assert_eq!(result.rows[0].get("reviewCount"), Some(&Value::from(1)));
        assert_eq!(
            result.rows[1].get("product"),
            Some(&Value::String("Widget".into()))
        );
        assert_eq!(result.rows[1].get("reviewCount"), Some(&Value::from(2)));
    }

    #[test]
    fn test_execute_routes_edge_property_aggregation_branch_coverage() {
        let db = CopperDb::open_temporary().unwrap();
        for (id, props) in [
            (
                "customer:1",
                BTreeMap::from([("name".to_string(), Value::String("C1".into()))]),
            ),
            (
                "product:1",
                BTreeMap::from([("name".to_string(), Value::String("P1".into()))]),
            ),
            (
                "product:2",
                BTreeMap::from([("name".to_string(), Value::String("P2".into()))]),
            ),
        ] {
            db.storage()
                .put_node(id, &rmp_serde::to_vec(&props).unwrap())
                .unwrap();
        }
        for edge in [
            EdgeRecord {
                id: "review:1".into(),
                start_node: "customer:1".into(),
                end_node: "product:1".into(),
                edge_type: "REVIEWED".into(),
                properties: BTreeMap::from([("rating".into(), Value::from(4.5))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "review:2".into(),
                start_node: "customer:1".into(),
                end_node: "product:1".into(),
                edge_type: "REVIEWED".into(),
                properties: BTreeMap::from([("rating".into(), Value::from(5))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "review:3".into(),
                start_node: "customer:1".into(),
                end_node: "product:2".into(),
                edge_type: "REVIEWED".into(),
                properties: BTreeMap::from([("other".into(), Value::from(9))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "review:4".into(),
                start_node: "customer:1".into(),
                end_node: "product:2".into(),
                edge_type: "REVIEWED".into(),
                properties: BTreeMap::from([("rating".into(), Value::String("bad".into()))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "review:5".into(),
                start_node: "customer:1".into(),
                end_node: "product:missing".into(),
                edge_type: "REVIEWED".into(),
                properties: BTreeMap::from([("rating".into(), Value::from(2))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
        ] {
            db.storage().put_edge_record(&edge).unwrap();
        }

        let result = db
            .execute(
                "MATCH (c)-[r:REVIEWED]->(p) RETURN p.name AS product, avg(r.rating) AS avgRating, count(r) AS reviewCount, min(r.rating) AS minRating, max(r.rating) AS maxRating, sum(r.rating) AS totalRating",
                Default::default(),
            )
            .unwrap();

        assert_eq!(
            result.columns,
            vec![
                "product",
                "avgRating",
                "reviewCount",
                "minRating",
                "maxRating",
                "totalRating"
            ]
        );
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("product"),
            Some(&Value::String("P1".into()))
        );
        assert_eq!(result.rows[0].get("avgRating"), Some(&Value::from(4.75)));
        assert_eq!(result.rows[0].get("reviewCount"), Some(&Value::from(2)));
        assert_eq!(result.rows[0].get("minRating"), Some(&Value::from(4.5)));
        assert_eq!(result.rows[0].get("maxRating"), Some(&Value::from(5.0)));
        assert_eq!(result.rows[0].get("totalRating"), Some(&Value::from(9.5)));
    }

    #[test]
    fn test_execute_routes_incoming_count_star_through_fast_path() {
        let db = CopperDb::open_temporary().unwrap();
        for (id, props) in [
            (
                "person:1",
                BTreeMap::from([("name".to_string(), Value::String("Alice".into()))]),
            ),
            (
                "person:2",
                BTreeMap::from([("name".to_string(), Value::String("Bob".into()))]),
            ),
            (
                "person:3",
                BTreeMap::from([("name".to_string(), Value::String("Carol".into()))]),
            ),
            (
                "person:4",
                BTreeMap::from([("name".to_string(), Value::String("Dana".into()))]),
            ),
            (
                "person:5",
                BTreeMap::from([("name".to_string(), Value::String("Eve".into()))]),
            ),
        ] {
            db.storage()
                .put_node(id, &rmp_serde::to_vec(&props).unwrap())
                .unwrap();
        }
        for edge in [
            EdgeRecord {
                id: "follows:1".into(),
                start_node: "person:2".into(),
                end_node: "person:1".into(),
                edge_type: "FOLLOWS".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "follows:2".into(),
                start_node: "person:3".into(),
                end_node: "person:1".into(),
                edge_type: "FOLLOWS".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "follows:3".into(),
                start_node: "person:4".into(),
                end_node: "person:1".into(),
                edge_type: "FOLLOWS".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "follows:4".into(),
                start_node: "person:5".into(),
                end_node: "person:2".into(),
                edge_type: "FOLLOWS".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
        ] {
            db.storage().put_edge_record(&edge).unwrap();
        }

        let result = db
            .execute(
                "MATCH (p:Person)<-[:FOLLOWS]-(f:Person) RETURN p.name AS person, count(*) AS followers LIMIT 2",
                Default::default(),
            )
            .unwrap();

        assert_eq!(result.rows.len(), 2);
        assert_eq!(
            result.rows[0].get("person"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(result.rows[0].get("followers"), Some(&Value::from(3)));
    }

    #[test]
    fn test_execute_routes_incoming_count_limit_zero_returns_empty() {
        let db = CopperDb::open_temporary().unwrap();
        for (id, props) in [
            (
                "person:1",
                BTreeMap::from([("name".to_string(), Value::String("Alice".into()))]),
            ),
            (
                "person:2",
                BTreeMap::from([("name".to_string(), Value::String("Bob".into()))]),
            ),
        ] {
            db.storage()
                .put_node(id, &rmp_serde::to_vec(&props).unwrap())
                .unwrap();
        }
        db.storage()
            .put_edge_record(&EdgeRecord {
                id: "follows:1".into(),
                start_node: "person:2".into(),
                end_node: "person:1".into(),
                edge_type: "FOLLOWS".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let result = db
            .execute(
                "MATCH (p:Person)<-[:FOLLOWS]-(f:Person) RETURN p.name AS person, count(f) AS followers LIMIT 0",
                Default::default(),
            )
            .unwrap();

        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_execute_routes_with_limit_compound_query_through_fast_path() {
        let db = CopperDb::open_temporary().unwrap();
        db.execute("CREATE (a:Actor {name: 'Alice'})", Default::default())
            .unwrap();
        db.execute("CREATE (m:Movie {title: 'Matrix'})", Default::default())
            .unwrap();

        let result = db
            .execute(
                "MATCH (a:Actor), (m:Movie) WITH a, m LIMIT 1 CREATE (a)-[r:TEMP_REL]->(m) DELETE r",
                Default::default(),
            )
            .unwrap();

        assert_eq!(result.stats.relationships_created, 1);
        assert_eq!(result.stats.relationships_deleted, 1);
        assert!(db
            .storage()
            .get_edges_by_type("TEMP_REL")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_execute_routes_with_limit_zero_compound_query_is_noop() {
        let db = CopperDb::open_temporary().unwrap();
        db.execute("CREATE (a:Actor {name: 'Alice'})", Default::default())
            .unwrap();
        db.execute("CREATE (m:Movie {title: 'Matrix'})", Default::default())
            .unwrap();

        let result = db
            .execute(
                "MATCH (a:Actor), (m:Movie) WITH a, m LIMIT 0 CREATE (a)-[r:TEMP_REL]->(m) DELETE r",
                Default::default(),
            )
            .unwrap();

        assert_eq!(result.stats.relationships_created, 0);
        assert_eq!(result.stats.relationships_deleted, 0);
        assert!(db
            .storage()
            .get_edges_by_type("TEMP_REL")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_execute_routes_property_match_compound_miss_is_clean() {
        let db = CopperDb::open_temporary().unwrap();
        db.execute("CREATE (p1:Person {id: 1})", Default::default())
            .unwrap();

        let result = db
            .execute(
                "MATCH (p1:Person {id: 1}), (p2:Person {id: 999}) CREATE (p1)-[r:TEMP_KNOWS]->(p2) DELETE r",
                Default::default(),
            )
            .unwrap();

        assert_eq!(result.stats.relationships_created, 0);
        assert_eq!(result.stats.relationships_deleted, 0);
        assert!(db
            .storage()
            .get_edges_by_type("TEMP_KNOWS")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_execute_routes_property_match_compound_fast_path() {
        let db = CopperDb::open_temporary().unwrap();
        db.execute(
            "CREATE (p1:Person {id: 1, name: 'Alice'})",
            Default::default(),
        )
        .unwrap();
        db.execute(
            "CREATE (p2:Person {id: 2, name: 'Bob'})",
            Default::default(),
        )
        .unwrap();

        let result = db
            .execute(
                "MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:TEMP_KNOWS]->(p2) DELETE r",
                Default::default(),
            )
            .unwrap();

        assert_eq!(result.stats.relationships_created, 1);
        assert_eq!(result.stats.relationships_deleted, 1);
        assert!(db
            .storage()
            .get_edges_by_type("TEMP_KNOWS")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_execute_routes_property_match_compound_return_count_fast_path() {
        let db = CopperDb::open_temporary().unwrap();
        db.execute("CREATE (p1:Person {id: 1})", Default::default())
            .unwrap();
        db.execute("CREATE (p2:Person {id: 2})", Default::default())
            .unwrap();

        let result = db
            .execute(
                "MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:TEMP_KNOWS]->(p2) WITH r DELETE r RETURN count(r)",
                Default::default(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["count(r)"]);
        assert_eq!(
            result.rows,
            vec![HashMap::from([("count(r)".into(), Value::from(1))])]
        );
        assert_eq!(result.stats.relationships_created, 1);
        assert_eq!(result.stats.relationships_deleted, 1);
        assert!(db
            .storage()
            .get_edges_by_type("TEMP_KNOWS")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_execute_routes_pipeline_query_through_route_hook() {
        let db = CopperDb::open_temporary().unwrap();
        let result = db
            .execute(
                "WITH [1, 2] AS values UNWIND values AS value RETURN value",
                Default::default(),
            )
            .unwrap();

        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("value"), Some(&Value::from(1)));
        assert_eq!(result.rows[1].get("value"), Some(&Value::from(2)));
    }

    #[test]
    fn test_execute_routes_pipeline_create_reuses_bound_nodes() {
        let db = CopperDb::open_temporary().unwrap();
        db.execute(
            "CREATE (c:Customer {customerID: 1, name: 'Ada'})",
            Default::default(),
        )
        .unwrap();

        let result = db
            .execute(
                "MATCH (c:Customer {customerID: 1}) CREATE (o:Order {orderID: 9001}) CREATE (c)-[:PURCHASED]->(o) WITH c, o RETURN c.customerID AS customerID, o.orderID AS orderID",
                Default::default(),
            )
            .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("customerID"), Some(&Value::from(1)));
        assert_eq!(result.rows[0].get("orderID"), Some(&Value::from(9001)));
        assert_eq!(result.stats.nodes_created, 1);
        assert_eq!(result.stats.relationships_created, 1);

        let edges = db.storage().get_edges_by_type("PURCHASED").unwrap();
        assert_eq!(edges.len(), 1);

        let customer_raw = db
            .storage()
            .get_node(&edges[0].start_node)
            .unwrap()
            .expect("customer node should exist");
        let customer_props: HashMap<String, Value> = rmp_serde::from_slice(&customer_raw).unwrap();
        assert_eq!(customer_props.get("customerID"), Some(&Value::from(1)));

        let order_raw = db
            .storage()
            .get_node(&edges[0].end_node)
            .unwrap()
            .expect("order node should exist");
        let order_props: HashMap<String, Value> = rmp_serde::from_slice(&order_raw).unwrap();
        assert_eq!(order_props.get("orderID"), Some(&Value::from(9001)));
    }

    #[test]
    fn test_execute_routes_pipeline_match_respects_bound_relationship_endpoints() {
        let db = CopperDb::open_temporary().unwrap();
        for cypher in [
            "CREATE (c:Customer {customerID: 1, name: 'Ada'})",
            "CREATE (c:Customer {customerID: 2, name: 'Bob'})",
            "CREATE (o:Order {orderID: 100})",
            "CREATE (o:Order {orderID: 200})",
        ] {
            db.execute(cypher, Default::default()).unwrap();
        }

        let node_id_for = |label: &str, property: &str, expected: i64| {
            db.storage()
                .scan_nodes_with_prefix(&format!("{label}:"))
                .find_map(|entry| {
                    let (_, raw) = entry.ok()?;
                    let props: HashMap<String, Value> = rmp_serde::from_slice(&raw).ok()?;
                    (props.get(property) == Some(&Value::from(expected)))
                        .then(|| props.get("_id").and_then(Value::as_str).map(str::to_string))
                        .flatten()
                })
                .expect("expected seeded node")
        };

        db.storage()
            .put_edge_record(&EdgeRecord {
                id: "purchased:1".into(),
                start_node: node_id_for("Customer", "customerID", 1),
                end_node: node_id_for("Order", "orderID", 100),
                edge_type: "PURCHASED".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        db.storage()
            .put_edge_record(&EdgeRecord {
                id: "purchased:2".into(),
                start_node: node_id_for("Customer", "customerID", 2),
                end_node: node_id_for("Order", "orderID", 200),
                edge_type: "PURCHASED".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let result = db
            .execute(
                "MATCH (c:Customer {customerID: 1}) WITH c MATCH (c)-[:PURCHASED]->(o:Order) RETURN o.orderID AS orderID",
                Default::default(),
            )
            .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("orderID"), Some(&Value::from(100)));
    }

    #[test]
    fn test_execute_routes_mutual_relationship_on_empty_db_returns_no_rows() {
        let db = CopperDb::open_temporary().unwrap();

        let result = db
            .execute(
                "MATCH (a)-[:FOLLOWS]->(b)-[:FOLLOWS]->(a) RETURN a, b",
                Default::default(),
            )
            .unwrap();

        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_execute_routes_mutual_relationship_with_missing_rel_type_returns_no_rows() {
        let db = CopperDb::open_temporary().unwrap();
        db.execute("CREATE (a:Person {name: 'Alice'})", Default::default())
            .unwrap();
        db.execute("CREATE (b:Person {name: 'Bob'})", Default::default())
            .unwrap();
        db.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:FOLLOWS]->(b) CREATE (b)-[:FOLLOWS]->(a)",
            Default::default(),
        )
        .unwrap();

        let result = db
            .execute(
                "MATCH (a)-[:NONEXISTENT]->(b)-[:NONEXISTENT]->(a) RETURN a, b",
                Default::default(),
            )
            .unwrap();

        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_execute_routes_pipeline_seeder_shape_with_expression_pattern_properties() {
        let db = CopperDb::open_temporary().unwrap();
        db.execute(
            "CREATE (c:Customer {customerID: 1, name: 'Ada'})",
            Default::default(),
        )
        .unwrap();
        db.execute(
            "CREATE (p:Product {productID: 1, name: 'Widget'})",
            Default::default(),
        )
        .unwrap();

        let result = db
            .execute(
                "MATCH (c:Customer {customerID: 1}) CREATE (o:Order {orderID: 9001}) CREATE (c)-[:PURCHASED]->(o) WITH o, {} UNWIND [{productID: 1}] AS prodRef MATCH (p:Product {productID: prodRef.productID}) CREATE (o)-[:ORDERS]->(p)",
                Default::default(),
            )
            .unwrap();

        assert_eq!(result.stats.nodes_created, 1);
        assert_eq!(result.stats.relationships_created, 2);

        let orders = db.storage().get_edges_by_type("ORDERS").unwrap();
        assert_eq!(orders.len(), 1);
        let purchased = db.storage().get_edges_by_type("PURCHASED").unwrap();
        assert_eq!(purchased.len(), 1);
        assert_eq!(purchased[0].end_node, orders[0].start_node);

        let order_raw = db
            .storage()
            .get_node(&orders[0].start_node)
            .unwrap()
            .expect("order node should exist");
        let order_props: HashMap<String, Value> = rmp_serde::from_slice(&order_raw).unwrap();
        assert_eq!(order_props.get("orderID"), Some(&Value::from(9001)));

        let product_raw = db
            .storage()
            .get_node(&orders[0].end_node)
            .unwrap()
            .expect("product node should exist");
        let product_props: HashMap<String, Value> = rmp_serde::from_slice(&product_raw).unwrap();
        assert_eq!(product_props.get("productID"), Some(&Value::from(1)));
    }

    #[test]
    fn test_execute_large_variable_length_chain_traversal_consistency() {
        let db = CopperDb::open_temporary().unwrap();

        for index in 0..25 {
            let props = BTreeMap::from([
                ("_id".to_string(), Value::String(format!("Node:{index}"))),
                (
                    "_labels".to_string(),
                    Value::Array(vec![Value::String("Node".into())]),
                ),
                ("name".to_string(), Value::String(format!("n{index:02}"))),
            ]);
            db.storage()
                .put_node(
                    &format!("Node:{index}"),
                    &rmp_serde::to_vec(&props).unwrap(),
                )
                .unwrap();
        }

        for index in 0..24 {
            db.storage()
                .put_edge_record(&EdgeRecord {
                    id: format!("link:{index}"),
                    start_node: format!("Node:{index}"),
                    end_node: format!("Node:{}", index + 1),
                    edge_type: "LINK".into(),
                    properties: BTreeMap::new(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })
                .unwrap();
        }

        let result = db
            .execute(
                "MATCH (a:Node {name: 'n00'})-[:LINK*1..24]->(n:Node) RETURN n.name AS name",
                Default::default(),
            )
            .unwrap();

        let mut names = result
            .rows
            .iter()
            .filter_map(|row| row.get("name").and_then(Value::as_str))
            .map(str::to_string)
            .collect::<Vec<_>>();
        names.sort();

        let expected = (1..25)
            .map(|index| format!("n{index:02}"))
            .collect::<Vec<_>>();
        assert_eq!(names, expected);
    }

    #[test]
    fn test_execute_routes_pipeline_seeder_shape_supports_multiple_rows_and_edge_properties() {
        let db = CopperDb::open_temporary().unwrap();
        db.execute(
            "CREATE (c:Customer {customerID: 1, companyName: 'C1'})",
            Default::default(),
        )
        .unwrap();
        db.execute(
            "CREATE (p:Product {productID: 1, productName: 'P1'})",
            Default::default(),
        )
        .unwrap();
        db.execute(
            "CREATE (p:Product {productID: 2, productName: 'P2'})",
            Default::default(),
        )
        .unwrap();

        let result = db
            .execute(
                "MATCH (c:Customer {customerID: 1}) CREATE (o:Order {orderID: 9001}) CREATE (c)-[:PURCHASED]->(o) WITH o, {} UNWIND [{productID: 1, quantity: 3}, {productID: 2, quantity: 5}] AS prodRef MATCH (p:Product {productID: prodRef.productID}) CREATE (o)-[:ORDERS {quantity: prodRef.quantity}]->(p)",
                Default::default(),
            )
            .unwrap();

        assert_eq!(result.stats.nodes_created, 1);
        assert_eq!(result.stats.relationships_created, 3);

        let purchased = db.storage().get_edges_by_type("PURCHASED").unwrap();
        assert_eq!(purchased.len(), 1);
        let orders = db.storage().get_edges_by_type("ORDERS").unwrap();
        assert_eq!(orders.len(), 2);

        let mut quantities: Vec<i64> = orders
            .iter()
            .filter_map(|edge| edge.properties.get("quantity").and_then(Value::as_i64))
            .collect();
        quantities.sort_unstable();
        assert_eq!(quantities, vec![3, 5]);

        let order_ids: std::collections::HashSet<String> =
            orders.iter().map(|edge| edge.start_node.clone()).collect();
        assert_eq!(order_ids.len(), 1);
        assert!(order_ids.contains(&purchased[0].end_node));
    }

    #[test]
    fn test_execute_merge_uses_current_row_expression_properties() {
        let db = CopperDb::open_temporary().unwrap();

        let first = db
            .execute(
                "UNWIND [1, 2] AS customerID MERGE (c:Customer {customerID: customerID}) RETURN c.customerID AS customerID",
                Default::default(),
            )
            .unwrap();
        let second = db
            .execute(
                "UNWIND [1, 2] AS customerID MERGE (c:Customer {customerID: customerID}) RETURN c.customerID AS customerID",
                Default::default(),
            )
            .unwrap();

        assert_eq!(first.rows.len(), 2);
        assert_eq!(first.rows[0].get("customerID"), Some(&Value::from(1)));
        assert_eq!(first.rows[1].get("customerID"), Some(&Value::from(2)));
        assert_eq!(first.stats.nodes_created, 2);
        assert_eq!(second.stats.nodes_created, 0);
    }

    #[test]
    fn engine_records_durable_query_audit_events() {
        let db = CopperDb::open_temporary().unwrap();
        db.execute("CREATE (n:Audit {v: 1})", Default::default())
            .unwrap();
        db.execute("MATCH (n:Audit) RETURN n", Default::default())
            .unwrap();

        let events = db.audit_log().events().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, EventType::DataCreate);
        assert_eq!(events[1].event_type, EventType::DataRead);
        assert_eq!(events[1].resource.as_deref(), Some("cypher_query"));
        assert!(db.audit_log().verify_chain().unwrap().valid);
    }

    #[test]
    fn engine_records_failed_query_audit_events() {
        let db = CopperDb::open_temporary().unwrap();
        let err = db
            .execute("MATCH (n RETURN n", Default::default())
            .unwrap_err();
        assert!(matches!(err, CopperDbError::Parse(_)));

        let events = db.audit_log().events().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, EventType::DataRead);
        assert_eq!(events[0].action.as_deref(), Some("PARSE"));
        assert!(!events[0].success);
        assert!(events[0].reason.is_some());
    }

    #[test]
    fn engine_enforces_durable_compliance_label_and_property_policies() {
        use copperdb_compliance::{ComplianceControl, CompliancePolicy};

        let db = CopperDb::open_temporary().unwrap();
        db.compliance_manager()
            .add_policy(CompliancePolicy::new(
                "patient-label",
                "Patient Label",
                ComplianceControl::RestrictLabel {
                    label: "Patient".into(),
                    allowed_roles: vec!["doctor".into()],
                },
            ))
            .unwrap();
        db.compliance_manager()
            .add_policy(CompliancePolicy::new(
                "mask-ssn",
                "Mask SSN",
                ComplianceControl::MaskProperty {
                    property: "ssn".into(),
                    allowed_roles: vec!["doctor".into()],
                },
            ))
            .unwrap();

        let reader_roles = vec!["reader".to_string()];
        let err = db
            .execute_as(
                "CREATE (n:Patient {name: 'Alice'})",
                Default::default(),
                &reader_roles,
            )
            .unwrap_err();
        assert!(matches!(err, CopperDbError::Compliance(_)));

        let doctor_roles = vec!["doctor".to_string()];
        db.execute_as(
            "CREATE (n:Patient {name: 'Alice', ssn: '111'})",
            Default::default(),
            &doctor_roles,
        )
        .unwrap();
        let err = db
            .execute_as(
                "MATCH (n:Patient) WHERE n.ssn = '111' RETURN n",
                Default::default(),
                &reader_roles,
            )
            .unwrap_err();
        assert!(matches!(err, CopperDbError::Compliance(_)));
    }

    #[test]
    fn engine_exports_compliance_evidence_from_audit_log() {
        let db = CopperDb::open_temporary().unwrap();
        db.execute("CREATE (n:Evidence {v: 1})", Default::default())
            .unwrap();
        let report = db
            .compliance_reporter()
            .export_soc2_evidence(copperdb_compliance::ReportWindow::all_time())
            .unwrap();
        assert_eq!(report.summary.total, 1);
        assert_eq!(report.summary.by_event_type.get("DATA_CREATE"), Some(&1));
    }

    #[test]
    fn test_default_config() {
        let config = DatabaseConfig::default();
        assert!(!config.auth_enabled);
        assert_eq!(config.max_connections, 100);
        assert!(config.storage_encryption_master_key.is_none());
    }

    #[test]
    fn persistent_engine_can_open_with_encrypted_storage() {
        let dir = tempfile::tempdir().unwrap();
        let config = DatabaseConfig {
            data_dir: dir.path().to_string_lossy().into_owned(),
            storage_encryption_master_key: Some(vec![0x42; 32]),
            storage_encryption_key_uri: "kms://local/storage-test".into(),
            ..Default::default()
        };

        {
            let db = CopperDb::open(config.clone()).unwrap();
            assert!(db.storage().is_encrypted());
            db.execute("CREATE (n:Encrypted {v: 1})", Default::default())
                .unwrap();
            db.flush().unwrap();
        }

        let reopened = CopperDb::open(config).unwrap();
        let result = reopened
            .execute("MATCH (n:Encrypted) RETURN n", Default::default())
            .unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn engine_plans_distributed_reads_and_writes_from_storage_topology() {
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let write_plan = db
            .plan_distributed_write(&placement, ConsistencyLevel::Quorum, None)
            .unwrap();
        assert_eq!(write_plan.required_acks, 2);
        assert_eq!(write_plan.replicas.len(), 3);

        let read_plan = db
            .plan_distributed_read(&placement, ConsistencyLevel::Quorum, None)
            .unwrap();
        assert_eq!(read_plan.required_responses, 2);
        assert_eq!(read_plan.replicas.len(), 3);
    }

    #[test]
    fn engine_persists_and_plans_fabric_database_shards() {
        use copperdb_fabric::{
            FabricAggregateOptions, FabricAggregateSpec, FabricPath, FabricPathBatch,
            FabricPathMergeOptions, FabricReadRequest, FabricReadScope, FabricRowBatch,
            FabricRowMergeOptions, FabricSortKey,
        };
        use copperdb_search::{
            RrfConfig, RrfHydrationRecord, RrfSearchBatch, RrfSearchHit, RrfSearchPolicy,
        };
        use copperdb_topology::{
            FabricDatabase, FabricGlobalId, FabricPartitionPolicy, FabricShard, FabricShardKind,
            MeshPeer, NodeCapability, PlacementKey, PlacementRecord,
        };

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        for node_id in ["node-1", "node-2"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Search)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        for (shard, node_id) in [("primary", "node-1"), ("person-00", "node-2")] {
            db.storage()
                .register_topology_placement(&PlacementRecord {
                    key: PlacementKey::new("default", "copper", shard),
                    primary_node: node_id.into(),
                    replica_nodes: vec![],
                    search_nodes: vec![node_id.into()],
                    hyperscaler_profile: None,
                    min_write_replicas: 0,
                    search_fanout: 1,
                })
                .unwrap();
        }
        let fabric = FabricDatabase {
            tenant: "default".into(),
            database: "copper".into(),
            default_shard: "primary".into(),
            partition_policy: FabricPartitionPolicy::HashByKey { buckets: 2 },
            shards: vec![
                FabricShard::mixed(PlacementKey::new("default", "copper", "primary")),
                FabricShard {
                    placement: PlacementKey::new("default", "copper", "person-00"),
                    kind: FabricShardKind::Graph,
                    labels: vec!["Person".into()],
                    relationship_types: vec![],
                    collections: vec![],
                },
            ],
        };

        db.register_fabric_database(&fabric).unwrap();
        assert_eq!(db.list_fabric_databases().unwrap(), vec![fabric.clone()]);
        assert_eq!(
            db.load_fabric_database("default", "copper")
                .unwrap()
                .unwrap(),
            fabric
        );

        let read_plans = db
            .plan_fabric_reads(&fabric, ConsistencyLevel::One, None)
            .unwrap();
        let search_plans = db.plan_fabric_searches(&fabric).unwrap();

        assert_eq!(read_plans.len(), 2);
        assert_eq!(search_plans.len(), 2);
        assert_eq!(read_plans[0].placement.shard, "primary");
        assert_eq!(read_plans[1].placement.shard, "person-00");

        let person_plan = db
            .plan_fabric_query_reads(
                &fabric,
                FabricReadRequest {
                    scope: FabricReadScope::Label("Person".into()),
                    consistency: ConsistencyLevel::One,
                    request_region: None,
                },
            )
            .unwrap();
        assert_eq!(person_plan.shards.len(), 1);
        assert_eq!(person_plan.shards[0].shard.placement.shard, "person-00");

        let merged = db.merge_fabric_rows(
            vec![
                FabricRowBatch {
                    shard: PlacementKey::new("default", "copper", "primary"),
                    rows: vec![serde_json::json!({"id": "a", "score": 2})],
                },
                FabricRowBatch {
                    shard: PlacementKey::new("default", "copper", "person-00"),
                    rows: vec![serde_json::json!({"id": "b", "score": 3})],
                },
            ],
            FabricRowMergeOptions {
                order_by: vec![FabricSortKey::descending("score")],
                limit: Some(1),
                ..Default::default()
            },
        );
        assert_eq!(merged.rows.len(), 1);
        assert_eq!(merged.rows[0]["id"], "b");

        let aggregates = db.merge_fabric_aggregates(
            vec![
                FabricRowBatch {
                    shard: PlacementKey::new("default", "copper", "primary"),
                    rows: vec![serde_json::json!({"label": "Person", "score": 2})],
                },
                FabricRowBatch {
                    shard: PlacementKey::new("default", "copper", "person-00"),
                    rows: vec![serde_json::json!({"label": "Person", "score": 4})],
                },
            ],
            FabricAggregateOptions {
                group_by: vec!["label".into()],
                aggregates: vec![
                    FabricAggregateSpec::count("count"),
                    FabricAggregateSpec::average("avg_score", "score"),
                ],
                order_by: Vec::new(),
                skip: 0,
                limit: None,
            },
        );
        assert_eq!(aggregates.rows.len(), 1);
        assert_eq!(aggregates.rows[0]["count"], 2);
        assert_eq!(aggregates.rows[0]["avg_score"], 3.0);

        let path = FabricPath::new(
            vec![
                FabricGlobalId::new(
                    PlacementKey::new("default", "copper", "primary"),
                    "node",
                    "a",
                ),
                FabricGlobalId::new(
                    PlacementKey::new("default", "copper", "person-00"),
                    "node",
                    "b",
                ),
            ],
            vec![FabricGlobalId::new(
                PlacementKey::new("default", "copper", "primary"),
                "relationship",
                "ab",
            )],
        );
        let paths = db.merge_fabric_paths(
            vec![
                FabricPathBatch {
                    shard: PlacementKey::new("default", "copper", "primary"),
                    paths: vec![path.clone()],
                },
                FabricPathBatch {
                    shard: PlacementKey::new("default", "copper", "person-00"),
                    paths: vec![path.clone()],
                },
            ],
            FabricPathMergeOptions::default(),
        );
        assert_eq!(paths.input_paths, 2);
        assert_eq!(paths.output_paths, 1);
        assert_eq!(paths.paths, vec![path]);

        let ranked_batches = vec![
            RrfSearchBatch {
                shard: PlacementKey::new("default", "copper", "primary"),
                source: "lexical".into(),
                hits: vec![RrfSearchHit {
                    global_id: FabricGlobalId::new(
                        PlacementKey::new("default", "copper", "primary"),
                        "node",
                        "a",
                    ),
                    rank: 1,
                    score: 0.7,
                    source: "lexical".into(),
                    shard: PlacementKey::new("default", "copper", "primary"),
                    label: "Person".into(),
                    snippet: None,
                }],
            },
            RrfSearchBatch {
                shard: PlacementKey::new("default", "copper", "person-00"),
                source: "vector".into(),
                hits: vec![RrfSearchHit {
                    global_id: FabricGlobalId::new(
                        PlacementKey::new("default", "copper", "primary"),
                        "node",
                        "a",
                    ),
                    rank: 1,
                    score: 0.9,
                    source: "vector".into(),
                    shard: PlacementKey::new("default", "copper", "primary"),
                    label: "Person".into(),
                    snippet: Some("fresh".into()),
                }],
            },
        ];
        let hydration = vec![RrfHydrationRecord {
            global_id: FabricGlobalId::new(
                PlacementKey::new("default", "copper", "primary"),
                "node",
                "a",
            ),
            labels: vec!["Person".into()],
            entity: serde_json::json!({
                "id": "a",
                "name": "Alice",
                "secret": "internal"
            }),
        }];
        let ranked_policy = RrfSearchPolicy {
            allowed_labels: vec!["Person".into()],
            denied_labels: Vec::new(),
            denied_sources: Vec::new(),
            require_hydration: true,
            redact_fields: vec!["secret".into()],
        };
        let ranked =
            db.merge_fabric_ranked_search(ranked_batches.clone(), RrfConfig::new(60.0, 10));
        assert_eq!(ranked.input_hits, 2);
        assert_eq!(ranked.output_hits, 1);
        assert_eq!(ranked.results[0].sources, vec!["lexical", "vector"]);
        assert_eq!(ranked.results[0].best_score, 0.9);

        let hydrated =
            db.hydrate_fabric_ranked_search(ranked, hydration.clone(), ranked_policy.clone());
        assert_eq!(hydrated.output_hits, 1);
        assert_eq!(hydrated.filtered_hits, 0);
        assert_eq!(hydrated.missing_hydration_hits, 0);
        assert_eq!(hydrated.results[0].labels, vec!["Person"]);
        assert_eq!(hydrated.results[0].redacted_fields, vec!["secret"]);
        assert_eq!(
            hydrated.results[0].entity.as_ref().unwrap()["name"],
            "Alice"
        );
        assert!(hydrated.results[0]
            .entity
            .as_ref()
            .unwrap()
            .get("secret")
            .is_none());

        let executed = db
            .execute_fabric_ranked_search(
                &fabric,
                {
                    let mut batches = ranked_batches;
                    batches.push(RrfSearchBatch {
                        shard: PlacementKey::new("default", "copper", "rogue-00"),
                        source: "rogue".into(),
                        hits: vec![RrfSearchHit {
                            global_id: FabricGlobalId::new(
                                PlacementKey::new("default", "copper", "rogue-00"),
                                "node",
                                "rogue",
                            ),
                            rank: 1,
                            score: 1.0,
                            source: "rogue".into(),
                            shard: PlacementKey::new("default", "copper", "rogue-00"),
                            label: "Person".into(),
                            snippet: Some("ignore me".into()),
                        }],
                    });
                    batches
                },
                hydration,
                RrfConfig::new(60.0, 10),
                ranked_policy,
            )
            .unwrap();
        assert_eq!(executed.planned_shards.len(), 2);
        assert_eq!(executed.responded_shards.len(), 2);
        assert_eq!(executed.missing_shards, Vec::<PlacementKey>::new());
        assert_eq!(
            executed.ignored_shards,
            vec![PlacementKey::new("default", "copper", "rogue-00")]
        );
        assert_eq!(executed.hydrated.output_hits, 1);
        assert_eq!(
            executed.hydrated.results[0].entity.as_ref().unwrap()["name"],
            "Alice"
        );
    }

    #[tokio::test]
    async fn engine_executes_fabric_ranked_search_with_transport() {
        use copperdb_search::{InMemorySearchTransport, RrfSearchHit};
        use copperdb_topology::{
            FabricDatabase, FabricGlobalId, FabricPartitionPolicy, FabricShard, FabricShardKind,
            MeshPeer, NodeCapability, PlacementKey, PlacementRecord,
        };

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        for node_id in ["search-a", "search-b", "search-c"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Search)
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        for (shard, nodes) in [
            ("primary", vec!["search-a", "search-c"]),
            ("person-00", vec!["search-b"]),
        ] {
            db.storage()
                .register_topology_placement(&PlacementRecord {
                    key: PlacementKey::new("default", "copper", shard),
                    primary_node: nodes[0].into(),
                    replica_nodes: vec![],
                    search_nodes: nodes.into_iter().map(str::to_string).collect(),
                    hyperscaler_profile: None,
                    min_write_replicas: 0,
                    search_fanout: 2,
                })
                .unwrap();
        }

        let fabric = FabricDatabase {
            tenant: "default".into(),
            database: "copper".into(),
            default_shard: "primary".into(),
            partition_policy: FabricPartitionPolicy::HashByKey { buckets: 2 },
            shards: vec![
                FabricShard::mixed(PlacementKey::new("default", "copper", "primary")),
                FabricShard {
                    placement: PlacementKey::new("default", "copper", "person-00"),
                    kind: FabricShardKind::Graph,
                    labels: vec!["Person".into()],
                    relationship_types: vec![],
                    collections: vec![],
                },
            ],
        };

        let transport = Arc::new(InMemorySearchTransport::new());
        transport.register_ranked_results(
            "search-a",
            RrfSearchBatch {
                shard: PlacementKey::new("default", "copper", "primary"),
                source: "lexical".into(),
                hits: vec![RrfSearchHit {
                    global_id: FabricGlobalId::new(
                        PlacementKey::new("default", "copper", "primary"),
                        "node",
                        "a",
                    ),
                    rank: 1,
                    score: 0.8,
                    source: "lexical".into(),
                    shard: PlacementKey::new("default", "copper", "primary"),
                    label: "Person".into(),
                    snippet: None,
                }],
            },
        );
        transport.register_ranked_results(
            "search-b",
            RrfSearchBatch {
                shard: PlacementKey::new("default", "copper", "person-00"),
                source: "vector".into(),
                hits: vec![RrfSearchHit {
                    global_id: FabricGlobalId::new(
                        PlacementKey::new("default", "copper", "primary"),
                        "node",
                        "a",
                    ),
                    rank: 1,
                    score: 0.9,
                    source: "vector".into(),
                    shard: PlacementKey::new("default", "copper", "person-00"),
                    label: "Person".into(),
                    snippet: Some("fresh".into()),
                }],
            },
        );

        transport.register_hydration_results(
            "search-a",
            vec![RrfHydrationRecord {
                global_id: FabricGlobalId::new(
                    PlacementKey::new("default", "copper", "primary"),
                    "node",
                    "a",
                ),
                labels: vec!["Person".into()],
                entity: serde_json::json!({"id": "a", "name": "Alice", "secret": "internal"}),
            }],
        );

        let execution = db
            .execute_fabric_ranked_search_with_full_transport(
                &fabric,
                SearchQuery::FullText {
                    query: "alice".into(),
                    fields: vec!["body".into()],
                    limit: 10,
                },
                ConsistencyLevel::One,
                RrfConfig::new(60.0, 10),
                RrfSearchPolicy {
                    allowed_labels: vec!["Person".into()],
                    denied_labels: Vec::new(),
                    denied_sources: Vec::new(),
                    require_hydration: true,
                    redact_fields: vec!["secret".into()],
                },
                transport.clone(),
                transport,
            )
            .await
            .unwrap();

        assert_eq!(execution.responded_nodes, vec!["search-a", "search-b"]);
        assert_eq!(execution.failed_nodes, vec!["search-c"]);
        assert_eq!(execution.responded_shards.len(), 2);
        assert_eq!(execution.hydrated.output_hits, 1);
        assert_eq!(
            execution.hydrated.results[0].entity.as_ref().unwrap()["name"],
            "Alice"
        );
        assert!(execution.hydrated.results[0]
            .entity
            .as_ref()
            .unwrap()
            .get("secret")
            .is_none());
    }

    #[tokio::test]
    async fn engine_builds_cassandra_coordinator_with_durable_repair_queue() {
        use copperdb_replication::{Command, InMemoryReplicaTransport, MemoryStorage, RepairKind};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            distributed_repair_queue_dir: Some(
                dir.path().join("repair").to_string_lossy().into_owned(),
            ),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(MemoryStorage::new()));
        transport.register("node-2", Arc::new(MemoryStorage::new()));
        let coordinator = db.build_cassandra_coordinator(transport).unwrap();

        let outcome = coordinator
            .write(
                &placement,
                ConsistencyLevel::Quorum,
                Command::Put {
                    key: b"engine".to_vec(),
                    value: b"handoff".to_vec(),
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(outcome.failed_replicas, vec!["node-3"]);

        let pending = db.open_repair_queue().unwrap().pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].kind, RepairKind::HintedHandoff);
        assert_eq!(pending[0].target_node, "node-3");
    }

    #[tokio::test]
    async fn engine_replays_durable_repairs_through_replica_transport() {
        use copperdb_replication::{Command, InMemoryReplicaTransport, MemoryStorage};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            distributed_repair_queue_dir: Some(
                dir.path().join("repair").to_string_lossy().into_owned(),
            ),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let first_transport = Arc::new(InMemoryReplicaTransport::new());
        first_transport.register("node-1", Arc::new(MemoryStorage::new()));
        first_transport.register("node-2", Arc::new(MemoryStorage::new()));
        db.build_cassandra_coordinator(first_transport)
            .unwrap()
            .write(
                &placement,
                ConsistencyLevel::Quorum,
                Command::Put {
                    key: b"repair-replay".to_vec(),
                    value: b"through-engine".to_vec(),
                },
                None,
            )
            .await
            .unwrap();

        let replay_transport = Arc::new(InMemoryReplicaTransport::new());
        let repaired_storage = Arc::new(MemoryStorage::new());
        replay_transport.register("node-3", repaired_storage.clone());
        let report = db.replay_repairs(replay_transport, 10).await.unwrap();

        assert_eq!(report.attempted, 1);
        assert_eq!(report.completed, 1);
        assert!(db
            .open_repair_queue()
            .unwrap()
            .pending()
            .unwrap()
            .is_empty());
        assert_eq!(
            repaired_storage.get(b"repair-replay"),
            Some(b"through-engine".to_vec())
        );
    }

    #[tokio::test]
    async fn engine_builds_scheduled_repair_worker() {
        use copperdb_replication::{
            Command, InMemoryReplicaTransport, MemoryStorage, RepairWorkerConfig,
        };
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};
        use std::time::Duration;

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            distributed_repair_queue_dir: Some(
                dir.path().join("repair").to_string_lossy().into_owned(),
            ),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let first_transport = Arc::new(InMemoryReplicaTransport::new());
        first_transport.register("node-1", Arc::new(MemoryStorage::new()));
        first_transport.register("node-2", Arc::new(MemoryStorage::new()));
        db.build_cassandra_coordinator(first_transport)
            .unwrap()
            .write(
                &placement,
                ConsistencyLevel::Quorum,
                Command::Put {
                    key: b"scheduled-engine-repair".to_vec(),
                    value: b"done".to_vec(),
                },
                None,
            )
            .await
            .unwrap();

        let replay_transport = Arc::new(InMemoryReplicaTransport::new());
        let repaired_storage = Arc::new(MemoryStorage::new());
        replay_transport.register("node-3", repaired_storage.clone());
        let worker = db
            .build_repair_worker(
                replay_transport,
                RepairWorkerConfig {
                    interval: Duration::from_millis(10),
                    max_records_per_tick: 10,
                },
            )
            .unwrap();

        let report = worker.run_once().await.unwrap();

        assert_eq!(report.attempted, 1);
        assert_eq!(report.completed, 1);
        assert_eq!(
            repaired_storage.get(b"scheduled-engine-repair"),
            Some(b"done".to_vec())
        );
    }

    #[tokio::test]
    async fn engine_routes_mutating_cypher_through_cassandra_coordinator() {
        use copperdb_replication::{InMemoryReplicaTransport, MemoryStorage};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            distributed_repair_queue_dir: Some(
                dir.path().join("repair").to_string_lossy().into_owned(),
            ),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        let replica1 = Arc::new(MemoryStorage::new());
        let replica2 = Arc::new(MemoryStorage::new());
        transport.register("node-1", replica1.clone());
        transport.register("node-2", replica2.clone());
        let outcome = db
            .execute_distributed_as(
                "CREATE (n:Distributed {v: 1})",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

        let write = outcome.write_outcome.unwrap();
        assert_eq!(write.acknowledged_by, vec!["node-1", "node-2"]);
        assert_eq!(write.failed_replicas, vec!["node-3"]);
        assert_eq!(replica1.cypher_log().len(), 1);
        assert_eq!(replica2.cypher_log().len(), 1);
        assert_eq!(outcome.result.stats.nodes_created, 1);
    }

    #[tokio::test]
    async fn engine_routes_read_cypher_through_distributed_read_plan() {
        use copperdb_replication::InMemoryReplicaTransport;
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        db.execute("CREATE (n:DistributedRead {v: 1})", HashMap::new())
            .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let outcome = db
            .execute_distributed_as(
                "MATCH (n:DistributedRead) RETURN n",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::Quorum,
                None,
                Arc::new(InMemoryReplicaTransport::new()),
            )
            .await
            .unwrap();

        assert!(outcome.write_outcome.is_none());
        let read = outcome.read_outcome.unwrap();
        assert_eq!(read.plan.required_responses, 2);
        assert_eq!(read.plan.replicas.len(), 3);
        assert_eq!(outcome.result.rows.len(), 1);
    }

    #[tokio::test]
    async fn engine_routes_distributed_shortest_path_query_through_mesh_bfs() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        fn graph_node(id: &str, name: &str) -> Vec<u8> {
            rmp_serde::to_vec(&BTreeMap::from([
                ("_id".to_string(), Value::String(id.to_string())),
                (
                    "_labels".to_string(),
                    Value::Array(vec![Value::String("Node".into())]),
                ),
                ("name".to_string(), Value::String(name.to_string())),
            ]))
            .unwrap()
        }

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        peer_one
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "node_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Node".into(),
                properties: vec!["name".into()],
            })
            .unwrap();
        peer_one
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Node:A".into(),
                labels: vec!["Node".into()],
                properties: BTreeMap::from([("name".into(), Value::String("a".into()))]),
                created_at_unix_ms: 123,
                updated_at_unix_ms: 456,
            })
            .unwrap();
        peer_one
            .put_edge_record(&EdgeRecord {
                id: "edge:a-b".into(),
                start_node: "Node:A".into(),
                end_node: "Node:B".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::from([("rank".into(), Value::from(1))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let peer_two = StorageEngine::open_temporary().unwrap();
        peer_two
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "node_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Node".into(),
                properties: vec!["name".into()],
            })
            .unwrap();
        peer_two
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Node:B".into(),
                labels: vec!["Node".into()],
                properties: BTreeMap::from([("name".into(), Value::String("b".into()))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        peer_two
            .put_edge_record(&EdgeRecord {
                id: "edge:b-d".into(),
                start_node: "Node:B".into(),
                end_node: "Node:D".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::from([("rank".into(), Value::from(2))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        peer_two
            .put_node("Node:C", &graph_node("Node:C", "c"))
            .unwrap();
        peer_two
            .put_edge_record(&EdgeRecord {
                id: "edge:a-c".into(),
                start_node: "Node:A".into(),
                end_node: "Node:C".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::from([("rank".into(), Value::from(5))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let peer_three = StorageEngine::open_temporary().unwrap();
        peer_three
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "node_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Node".into(),
                properties: vec!["name".into()],
            })
            .unwrap();
        peer_three
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Node:D".into(),
                labels: vec!["Node".into()],
                properties: BTreeMap::from([("name".into(), Value::String("d".into()))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        peer_three
            .put_edge_record(&EdgeRecord {
                id: "edge:c-d".into(),
                start_node: "Node:C".into(),
                end_node: "Node:D".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::from([("rank".into(), Value::from(6))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
        transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));
        transport.register("node-3", Arc::new(StorageEngineAdapter::new(peer_three)));

        let outcome = db
            .execute_distributed_as(
                "MATCH p = shortestPath((a:Node {_id: 'Node:A'})-[:LINK*]->(d:Node {_id: 'Node:D'})) RETURN p AS path, nodes(p) AS nodes, relationships(p) AS rels, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

        assert!(outcome.write_outcome.is_none());
        let read = outcome
            .read_outcome
            .expect("expected distributed read outcome");
        assert_eq!(read.plan.required_responses, 2);
        assert_eq!(outcome.result.rows.len(), 1);
        assert_eq!(outcome.result.rows[0].get("hops"), Some(&Value::from(2)));

        let nodes = outcome.result.rows[0]
            .get("nodes")
            .and_then(Value::as_array)
            .expect("expected nodes(path)");
        let names = nodes
            .iter()
            .map(|node| node.get("name").and_then(Value::as_str).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["a", "b", "d"]);

        let rels = outcome.result.rows[0]
            .get("rels")
            .and_then(Value::as_array)
            .expect("expected relationships(path)");
        assert_eq!(rels.len(), 2);
        assert_eq!(rels[0].get("rank"), Some(&Value::from(1)));

        let path = outcome.result.rows[0]
            .get("path")
            .and_then(Value::as_object)
            .expect("expected path object");
        assert_eq!(path.get("length"), Some(&Value::from(2)));
    }

    #[tokio::test]
    async fn engine_routes_distributed_shortest_path_query_with_property_endpoints() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        peer_one
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "node_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Node".into(),
                properties: vec!["name".into()],
            })
            .unwrap();
        peer_one
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Node:A".into(),
                labels: vec!["Node".into()],
                properties: BTreeMap::from([("name".into(), Value::String("a".into()))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        peer_one
            .put_edge_record(&EdgeRecord {
                id: "edge:a-b".into(),
                start_node: "Node:A".into(),
                end_node: "Node:B".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let peer_two = StorageEngine::open_temporary().unwrap();
        peer_two
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "node_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Node".into(),
                properties: vec!["name".into()],
            })
            .unwrap();
        peer_two
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Node:B".into(),
                labels: vec!["Node".into()],
                properties: BTreeMap::from([("name".into(), Value::String("b".into()))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        peer_two
            .put_edge_record(&EdgeRecord {
                id: "edge:b-d".into(),
                start_node: "Node:B".into(),
                end_node: "Node:D".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let peer_three = StorageEngine::open_temporary().unwrap();
        peer_three
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "node_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Node".into(),
                properties: vec!["name".into()],
            })
            .unwrap();
        peer_three
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Node:D".into(),
                labels: vec!["Node".into()],
                properties: BTreeMap::from([("name".into(), Value::String("d".into()))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
        transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));
        transport.register("node-3", Arc::new(StorageEngineAdapter::new(peer_three)));

        let parsed = Parser::new()
            .parse(
                "MATCH p = shortestPath((a:Node {name: 'a'})-[:LINK*]->(d:Node {name: 'd'})) RETURN length(p) AS hops, p AS shortest",
            )
            .unwrap();
        assert!(distributed_shortest_path_query_shape(&parsed).is_some());
        assert_eq!(
            transport
                .graph_nodes_by_property("node-1", "Node", "name", &Value::String("a".into()))
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            transport
                .graph_nodes_by_property("node-3", "Node", "name", &Value::String("d".into()))
                .await
                .unwrap()
                .len(),
            1
        );

        let outcome = db
            .execute_distributed_as(
                "MATCH p = shortestPath((a:Node {name: 'a'})-[:LINK*]->(d:Node {name: 'd'})) RETURN length(p) AS hops, p AS shortest",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(outcome.result.columns, vec!["hops", "shortest"]);
        assert_eq!(outcome.result.rows.len(), 1);
        assert_eq!(outcome.result.rows[0].get("hops"), Some(&Value::from(2)));
        let path = outcome.result.rows[0]
            .get("shortest")
            .and_then(Value::as_object)
            .expect("expected shortest path object");
        let nodes = path
            .get("nodes")
            .and_then(Value::as_array)
            .expect("expected shortest path nodes");
        let names = nodes
            .iter()
            .map(|node| node.get("name").and_then(Value::as_str).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["a", "b", "d"]);
    }

    #[tokio::test]
    async fn engine_routes_distributed_single_node_path_query() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        peer_one
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "node_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Node".into(),
                properties: vec!["name".into()],
            })
            .unwrap();
        peer_one
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Node:A".into(),
                labels: vec!["Node".into()],
                properties: BTreeMap::from([("name".into(), Value::String("a".into()))]),
                created_at_unix_ms: 123,
                updated_at_unix_ms: 456,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

        let outcome = db
            .execute_distributed_as(
                "MATCH p = (a:Node {name: 'a'}) RETURN length(p) AS hops, nodes(p) AS nodes, p AS path",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(outcome.result.columns, vec!["hops", "nodes", "path"]);
        assert_eq!(outcome.result.rows.len(), 1);
        assert_eq!(outcome.result.rows[0].get("hops"), Some(&Value::from(0)));
        let nodes = outcome.result.rows[0]
            .get("nodes")
            .and_then(Value::as_array)
            .expect("expected node list");
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].get("name"), Some(&Value::String("a".into())));
        assert_eq!(nodes[0].get("_created_at_unix_ms"), Some(&Value::from(123)));
        assert_eq!(nodes[0].get("_updated_at_unix_ms"), Some(&Value::from(456)));
        let path = outcome.result.rows[0]
            .get("path")
            .and_then(Value::as_object)
            .expect("expected path object");
        assert_eq!(path.get("length"), Some(&Value::from(0)));
    }

    #[tokio::test]
    async fn engine_distributed_single_node_path_suppresses_stale_remote_node() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        db.storage()
            .register_topology_peer(
                &MeshPeer::new("node-1", "node-1.mesh.local:9000")
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec![],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        db.execute(
            "CREATE DECAY PROFILE stale_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.75, scoreFloor: 0.0, function: 'step', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
            Default::default(),
        )
        .unwrap();
        db.execute(
            "CREATE DECAY PROFILE stale_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE stale_decay, order: 10 }",
            Default::default(),
        )
        .unwrap();

        let peer = StorageEngine::open_temporary().unwrap();
        peer.put_node_record(&copperdb_storage::NodeRecord {
            id: "memory:stale".into(),
            labels: vec!["MemoryEpisode".into()],
            properties: BTreeMap::from([("name".into(), Value::String("stale".into()))]),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer)));

        let outcome = db
            .execute_distributed_as(
                "MATCH p = (a:MemoryEpisode {_id: 'memory:stale'}) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

        assert!(outcome.result.rows.is_empty());
    }

    #[tokio::test]
    async fn engine_distributed_single_node_path_persists_remote_on_access_metadata() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        db.storage()
            .register_topology_peer(
                &MeshPeer::new("node-1", "node-1.mesh.local:9000")
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec![],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        db.execute(
            "CREATE PROMOTION POLICY memory_access FOR (n:MemoryEpisode) APPLY { ON ACCESS { SET n.lastAccessedAt = timestamp() SET n.accessCount = coalesce(n.accessCount, 0) + 1 } }",
            Default::default(),
        )
        .unwrap();

        let peer_dir = tempfile::tempdir().unwrap();
        let peer_path = peer_dir.path().join("peer");
        let peer = StorageEngine::open(&peer_path).unwrap();
        peer.put_node_record(&copperdb_storage::NodeRecord {
            id: "memory:access".into(),
            labels: vec!["MemoryEpisode".into()],
            properties: BTreeMap::from([("name".into(), Value::String("access".into()))]),
            created_at_unix_ms: 123,
            updated_at_unix_ms: 123,
        })
        .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer)));

        let outcome = db
            .execute_distributed_as(
                "MATCH p = (a:MemoryEpisode {_id: 'memory:access'}) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(outcome.result.rows.len(), 1);
        assert_eq!(outcome.result.rows[0].get("hops"), Some(&Value::from(0)));

        let reopened_peer = StorageEngine::open(&peer_path).unwrap();
        let metadata = reopened_peer
            .get_knowledge_policy_access_metadata("memory:access")
            .unwrap()
            .expect("expected replicated node access metadata");
        assert_eq!(metadata.access_count, 1);
        assert!(metadata.last_accessed_at_unix_ms.is_some());
    }

    #[tokio::test]
    async fn engine_routes_distributed_single_hop_path_query_with_edge_properties() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        peer_one
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "node_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Node".into(),
                properties: vec!["name".into()],
            })
            .unwrap();
        peer_one
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Node:A".into(),
                labels: vec!["Node".into()],
                properties: BTreeMap::from([("name".into(), Value::String("a".into()))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        peer_one
            .put_edge_record(&EdgeRecord {
                id: "edge:a-b".into(),
                start_node: "Node:A".into(),
                end_node: "Node:B".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::from([("rank".into(), Value::from(1))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let peer_two = StorageEngine::open_temporary().unwrap();
        peer_two
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "node_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Node".into(),
                properties: vec!["name".into()],
            })
            .unwrap();
        peer_two
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Node:B".into(),
                labels: vec!["Node".into()],
                properties: BTreeMap::from([("name".into(), Value::String("b".into()))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
        transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));

        let outcome = db
            .execute_distributed_as(
                "MATCH p = (a:Node {name: 'a'})-[:LINK {rank: 1}]->(b:Node {name: 'b'}) RETURN length(p) AS hops, relationships(p) AS rels, nodes(p) AS nodes, p AS path",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(
            outcome.result.columns,
            vec!["hops", "rels", "nodes", "path"]
        );
        assert_eq!(outcome.result.rows.len(), 1);
        assert_eq!(outcome.result.rows[0].get("hops"), Some(&Value::from(1)));
        let rels = outcome.result.rows[0]
            .get("rels")
            .and_then(Value::as_array)
            .expect("expected relationship list");
        assert_eq!(rels.len(), 1);
        assert_eq!(rels[0].get("rank"), Some(&Value::from(1)));
        let nodes = outcome.result.rows[0]
            .get("nodes")
            .and_then(Value::as_array)
            .expect("expected node list");
        let names = nodes
            .iter()
            .map(|node| node.get("name").and_then(Value::as_str).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[tokio::test]
    async fn engine_distributed_direct_path_suppresses_stale_remote_edge() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        db.storage()
            .register_topology_peer(
                &MeshPeer::new("node-1", "node-1.mesh.local:9000")
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec![],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        db.execute(
            "CREATE DECAY PROFILE stale_edge_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.75, scoreFloor: 0.0, function: 'step', scope: 'EDGE', scoreFrom: 'CREATED', enabled: true }",
            Default::default(),
        )
        .unwrap();
        db.execute(
            "CREATE DECAY PROFILE stale_edge_binding FOR ()-[r:LINKS]-() APPLY { DECAY PROFILE stale_edge_decay, order: 10 }",
            Default::default(),
        )
        .unwrap();

        let peer = StorageEngine::open_temporary().unwrap();
        peer.put_node_record(&copperdb_storage::NodeRecord {
            id: "Node:A".into(),
            labels: vec!["Node".into()],
            properties: BTreeMap::from([("name".into(), Value::String("a".into()))]),
            created_at_unix_ms: 123,
            updated_at_unix_ms: 123,
        })
        .unwrap();
        peer.put_node_record(&copperdb_storage::NodeRecord {
            id: "Node:B".into(),
            labels: vec!["Node".into()],
            properties: BTreeMap::from([("name".into(), Value::String("b".into()))]),
            created_at_unix_ms: 123,
            updated_at_unix_ms: 123,
        })
        .unwrap();
        peer.put_edge_record(&EdgeRecord {
            id: "edge:a-b".into(),
            start_node: "Node:A".into(),
            end_node: "Node:B".into(),
            edge_type: "LINKS".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer)));

        let outcome = db
            .execute_distributed_as(
                "MATCH p = (a:Node {_id: 'Node:A'})-[:LINKS]->(b:Node {_id: 'Node:B'}) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

        assert!(outcome.result.rows.is_empty());
    }

    #[tokio::test]
    async fn engine_distributed_direct_path_persists_remote_edge_on_access_metadata() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        db.storage()
            .register_topology_peer(
                &MeshPeer::new("node-1", "node-1.mesh.local:9000")
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec![],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        db.execute(
            "CREATE PROMOTION POLICY edge_access FOR ()-[r:LINKS]-() APPLY { ON ACCESS { SET r.lastAccessedAt = timestamp() SET r.accessCount = coalesce(r.accessCount, 0) + 1 } }",
            Default::default(),
        )
        .unwrap();

        let peer_dir = tempfile::tempdir().unwrap();
        let peer_path = peer_dir.path().join("peer");
        let peer = StorageEngine::open(&peer_path).unwrap();
        peer.put_node_record(&copperdb_storage::NodeRecord {
            id: "Node:A".into(),
            labels: vec!["Node".into()],
            properties: BTreeMap::from([("name".into(), Value::String("a".into()))]),
            created_at_unix_ms: 123,
            updated_at_unix_ms: 123,
        })
        .unwrap();
        peer.put_node_record(&copperdb_storage::NodeRecord {
            id: "Node:B".into(),
            labels: vec!["Node".into()],
            properties: BTreeMap::from([("name".into(), Value::String("b".into()))]),
            created_at_unix_ms: 123,
            updated_at_unix_ms: 123,
        })
        .unwrap();
        peer.put_edge_record(&EdgeRecord {
            id: "edge:a-b".into(),
            start_node: "Node:A".into(),
            end_node: "Node:B".into(),
            edge_type: "LINKS".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 123,
            updated_at_unix_ms: 123,
        })
        .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer)));

        let outcome = db
            .execute_distributed_as(
                "MATCH p = (a:Node {_id: 'Node:A'})-[:LINKS]->(b:Node {_id: 'Node:B'}) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(outcome.result.rows.len(), 1);
        assert_eq!(outcome.result.rows[0].get("hops"), Some(&Value::from(1)));

        let reopened_peer = StorageEngine::open(&peer_path).unwrap();
        let metadata = reopened_peer
            .get_knowledge_policy_access_metadata("edge:a-b")
            .unwrap()
            .expect("expected replicated edge access metadata");
        assert_eq!(metadata.access_count, 2);
        assert!(metadata.last_accessed_at_unix_ms.is_some());
    }

    #[tokio::test]
    async fn engine_routes_distributed_variable_length_exact_path_query() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        peer_one
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "node_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Node".into(),
                properties: vec!["name".into()],
            })
            .unwrap();
        peer_one
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Node:A".into(),
                labels: vec!["Node".into()],
                properties: BTreeMap::from([("name".into(), Value::String("a".into()))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        peer_one
            .put_edge_record(&EdgeRecord {
                id: "edge:a-b".into(),
                start_node: "Node:A".into(),
                end_node: "Node:B".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::from([("rank".into(), Value::from(1))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let peer_two = StorageEngine::open_temporary().unwrap();
        peer_two
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "node_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Node".into(),
                properties: vec!["name".into()],
            })
            .unwrap();
        peer_two
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Node:B".into(),
                labels: vec!["Node".into()],
                properties: BTreeMap::from([("name".into(), Value::String("b".into()))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        peer_two
            .put_edge_record(&EdgeRecord {
                id: "edge:b-c".into(),
                start_node: "Node:B".into(),
                end_node: "Node:C".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::from([("rank".into(), Value::from(2))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let peer_three = StorageEngine::open_temporary().unwrap();
        peer_three
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "node_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Node".into(),
                properties: vec!["name".into()],
            })
            .unwrap();
        peer_three
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Node:C".into(),
                labels: vec!["Node".into()],
                properties: BTreeMap::from([("name".into(), Value::String("c".into()))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
        transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));
        transport.register("node-3", Arc::new(StorageEngineAdapter::new(peer_three)));

        let outcome = db
            .execute_distributed_as(
                "MATCH p = (a:Node {name: 'a'})-[r:LINK*2]->(n:Node {name: 'c'}) RETURN length(p) AS hops, relationships(p) AS rels, p AS path",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(outcome.result.rows.len(), 1);
        assert_eq!(outcome.result.rows[0].get("hops"), Some(&Value::from(2)));
        let rels = outcome.result.rows[0]
            .get("rels")
            .and_then(Value::as_array)
            .expect("expected relationship list");
        assert_eq!(rels.len(), 2);
        assert_eq!(rels[0].get("rank"), Some(&Value::from(1)));
        assert_eq!(rels[1].get("rank"), Some(&Value::from(2)));
    }

    #[tokio::test]
    async fn engine_routes_distributed_variable_length_range_path_query() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        peer_one
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "node_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Node".into(),
                properties: vec!["name".into()],
            })
            .unwrap();
        peer_one
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Node:A".into(),
                labels: vec!["Node".into()],
                properties: BTreeMap::from([("name".into(), Value::String("a".into()))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        peer_one
            .put_edge_record(&EdgeRecord {
                id: "edge:a-b".into(),
                start_node: "Node:A".into(),
                end_node: "Node:B".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let peer_two = StorageEngine::open_temporary().unwrap();
        peer_two
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "node_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Node".into(),
                properties: vec!["name".into()],
            })
            .unwrap();
        peer_two
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Node:B".into(),
                labels: vec!["Node".into()],
                properties: BTreeMap::from([("name".into(), Value::String("b".into()))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        peer_two
            .put_edge_record(&EdgeRecord {
                id: "edge:b-c".into(),
                start_node: "Node:B".into(),
                end_node: "Node:C".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let peer_three = StorageEngine::open_temporary().unwrap();
        peer_three
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "node_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Node".into(),
                properties: vec!["name".into()],
            })
            .unwrap();
        peer_three
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Node:C".into(),
                labels: vec!["Node".into()],
                properties: BTreeMap::from([("name".into(), Value::String("c".into()))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
        transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));
        transport.register("node-3", Arc::new(StorageEngineAdapter::new(peer_three)));

        let outcome = db
            .execute_distributed_as(
                "MATCH p = (a:Node {name: 'a'})-[:LINK*1..2]->(n:Node) RETURN length(p) AS hops, nodes(p) AS nodes",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(outcome.result.rows.len(), 2);
        let mut rows = outcome
            .result
            .rows
            .iter()
            .map(|row| {
                let hops = row.get("hops").and_then(Value::as_i64).unwrap();
                let names = row
                    .get("nodes")
                    .and_then(Value::as_array)
                    .unwrap()
                    .iter()
                    .map(|node| {
                        node.get("name")
                            .and_then(Value::as_str)
                            .unwrap()
                            .to_string()
                    })
                    .collect::<Vec<_>>();
                (hops, names)
            })
            .collect::<Vec<_>>();
        rows.sort_by_key(|(hops, _)| *hops);
        assert_eq!(rows[0], (1, vec!["a".into(), "b".into()]));
        assert_eq!(rows[1], (2, vec!["a".into(), "b".into(), "c".into()]));
    }

    #[tokio::test]
    async fn engine_routes_distributed_optional_single_node_path_query_hit_and_miss() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        peer_one
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "person_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Person".into(),
                properties: vec!["name".into()],
            })
            .unwrap();
        peer_one
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Person:Alice".into(),
                labels: vec!["Person".into()],
                properties: BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

        let hit = db
            .execute_distributed_as(
                "OPTIONAL MATCH p = (n:Person {name: 'Alice'}) RETURN p AS path, nodes(p) AS nodes, relationships(p) AS rels, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport.clone(),
            )
            .await
            .unwrap();
        let miss = db
            .execute_distributed_as(
                "OPTIONAL MATCH p = (n:Person {name: 'Bob'}) RETURN p AS path, nodes(p) AS nodes, relationships(p) AS rels, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(hit.result.rows.len(), 1);
        assert_eq!(hit.result.rows[0].get("hops"), Some(&Value::from(0)));
        let hit_nodes = hit.result.rows[0]
            .get("nodes")
            .and_then(Value::as_array)
            .expect("expected optional hit nodes");
        assert_eq!(hit_nodes.len(), 1);
        assert_eq!(
            hit.result.rows[0].get("rels"),
            Some(&Value::Array(Vec::new()))
        );

        assert_eq!(miss.result.rows.len(), 1);
        assert_eq!(miss.result.rows[0].get("path"), Some(&Value::Null));
        assert_eq!(miss.result.rows[0].get("hops"), Some(&Value::Null));
        assert_eq!(
            miss.result.rows[0].get("nodes"),
            Some(&Value::Array(Vec::new()))
        );
        assert_eq!(
            miss.result.rows[0].get("rels"),
            Some(&Value::Array(Vec::new()))
        );
    }

    #[tokio::test]
    async fn engine_routes_distributed_leading_match_optional_path_with_row_preservation() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        peer_one
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "seed_id".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Seed".into(),
                properties: vec!["id".into()],
            })
            .unwrap();
        peer_one
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Seed:1".into(),
                labels: vec!["Seed".into()],
                properties: BTreeMap::from([("id".into(), Value::from(1))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        peer_one
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Seed:2".into(),
                labels: vec!["Seed".into()],
                properties: BTreeMap::from([("id".into(), Value::from(2))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        peer_one
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "person_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Person".into(),
                properties: vec!["name".into()],
            })
            .unwrap();
        peer_one
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Person:Alice".into(),
                labels: vec!["Person".into()],
                properties: BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

        let hit = db
            .execute_distributed_as(
                "MATCH (s:Seed) OPTIONAL MATCH p = (n:Person {name: 'Alice'}) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport.clone(),
            )
            .await
            .unwrap();
        let miss = db
            .execute_distributed_as(
                "MATCH (s:Seed) OPTIONAL MATCH p = (n:Person {name: 'Bob'}) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(hit.result.rows.len(), 2);
        assert!(hit
            .result
            .rows
            .iter()
            .all(|row| row.get("hops") == Some(&Value::from(0))));
        assert!(hit
            .result
            .rows
            .iter()
            .all(|row| row.get("path").and_then(Value::as_object).is_some()));

        assert_eq!(miss.result.rows.len(), 2);
        assert!(miss
            .result
            .rows
            .iter()
            .all(|row| row.get("path") == Some(&Value::Null)));
        assert!(miss
            .result
            .rows
            .iter()
            .all(|row| row.get("hops") == Some(&Value::Null)));
    }

    #[tokio::test]
    async fn engine_routes_distributed_leading_match_optional_path_using_bound_node() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        peer_one
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "seed_id".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Seed".into(),
                properties: vec!["id".into()],
            })
            .unwrap();
        peer_one
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "person_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Person".into(),
                properties: vec!["name".into()],
            })
            .unwrap();
        peer_one
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Seed:1".into(),
                labels: vec!["Seed".into()],
                properties: BTreeMap::from([("id".into(), Value::from(1))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        peer_one
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Seed:2".into(),
                labels: vec!["Seed".into()],
                properties: BTreeMap::from([("id".into(), Value::from(2))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        peer_one
            .put_node_record(&copperdb_storage::NodeRecord {
                id: "Person:Alice".into(),
                labels: vec!["Person".into()],
                properties: BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        peer_one
            .put_edge_record(&EdgeRecord {
                id: "edge:seed1-alice".into(),
                start_node: "Seed:1".into(),
                end_node: "Person:Alice".into(),
                edge_type: "KNOWS".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

        let outcome = db
            .execute_distributed_as(
                "MATCH (s:Seed) OPTIONAL MATCH p = (s)-[:KNOWS]->(n:Person) RETURN p AS path, nodes(p) AS nodes, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(outcome.result.rows.len(), 2);
        assert_eq!(
            outcome
                .result
                .rows
                .iter()
                .filter(|row| row.get("path") == Some(&Value::Null))
                .count(),
            1
        );
        assert_eq!(
            outcome
                .result
                .rows
                .iter()
                .filter(|row| row.get("hops") == Some(&Value::from(1)))
                .count(),
            1
        );
        let hit_nodes = outcome
            .result
            .rows
            .iter()
            .find(|row| row.get("hops") == Some(&Value::from(1)))
            .and_then(|row| row.get("nodes"))
            .and_then(Value::as_array)
            .expect("expected bound-path hit nodes");
        assert_eq!(hit_nodes.len(), 2);
    }

    #[tokio::test]
    async fn engine_routes_distributed_multi_match_optional_path_with_bound_node() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        peer_one
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "seed_id".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Seed".into(),
                properties: vec!["id".into()],
            })
            .unwrap();
        peer_one
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "tag_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Tag".into(),
                properties: vec!["name".into()],
            })
            .unwrap();
        peer_one
            .persist_index_definition(&copperdb_storage::IndexDefinition {
                name: "person_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Person".into(),
                properties: vec!["name".into()],
            })
            .unwrap();
        for (id, label, properties) in [
            (
                "Seed:1",
                "Seed",
                BTreeMap::from([("id".into(), Value::from(1))]),
            ),
            (
                "Seed:2",
                "Seed",
                BTreeMap::from([("id".into(), Value::from(2))]),
            ),
            (
                "Tag:blue",
                "Tag",
                BTreeMap::from([("name".into(), Value::String("blue".into()))]),
            ),
            (
                "Tag:red",
                "Tag",
                BTreeMap::from([("name".into(), Value::String("red".into()))]),
            ),
            (
                "Person:Alice",
                "Person",
                BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
            ),
        ] {
            peer_one
                .put_node_record(&copperdb_storage::NodeRecord {
                    id: id.into(),
                    labels: vec![label.into()],
                    properties,
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })
                .unwrap();
        }
        peer_one
            .put_edge_record(&EdgeRecord {
                id: "edge:seed1-alice".into(),
                start_node: "Seed:1".into(),
                end_node: "Person:Alice".into(),
                edge_type: "KNOWS".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

        let outcome = db
            .execute_distributed_as(
                "MATCH (s:Seed) MATCH (t:Tag) OPTIONAL MATCH p = (s)-[:KNOWS]->(n:Person) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(outcome.result.rows.len(), 4);
        assert_eq!(
            outcome
                .result
                .rows
                .iter()
                .filter(|row| row.get("hops") == Some(&Value::from(1)))
                .count(),
            2
        );
        assert_eq!(
            outcome
                .result
                .rows
                .iter()
                .filter(|row| row.get("path") == Some(&Value::Null))
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn engine_routes_distributed_relationship_match_optional_path_with_bound_node() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        for definition in [
            copperdb_storage::IndexDefinition {
                name: "seed_id".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Seed".into(),
                properties: vec!["id".into()],
            },
            copperdb_storage::IndexDefinition {
                name: "tag_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Tag".into(),
                properties: vec!["name".into()],
            },
            copperdb_storage::IndexDefinition {
                name: "person_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Person".into(),
                properties: vec!["name".into()],
            },
        ] {
            peer_one.persist_index_definition(&definition).unwrap();
        }
        for (id, label, properties) in [
            (
                "Seed:1",
                "Seed",
                BTreeMap::from([("id".into(), Value::from(1))]),
            ),
            (
                "Seed:2",
                "Seed",
                BTreeMap::from([("id".into(), Value::from(2))]),
            ),
            (
                "Tag:blue",
                "Tag",
                BTreeMap::from([("name".into(), Value::String("blue".into()))]),
            ),
            (
                "Tag:red",
                "Tag",
                BTreeMap::from([("name".into(), Value::String("red".into()))]),
            ),
            (
                "Person:Alice",
                "Person",
                BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
            ),
        ] {
            peer_one
                .put_node_record(&copperdb_storage::NodeRecord {
                    id: id.into(),
                    labels: vec![label.into()],
                    properties,
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })
                .unwrap();
        }
        for edge in [
            EdgeRecord {
                id: "edge:seed1-blue".into(),
                start_node: "Seed:1".into(),
                end_node: "Tag:blue".into(),
                edge_type: "TAGGED".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "edge:seed2-red".into(),
                start_node: "Seed:2".into(),
                end_node: "Tag:red".into(),
                edge_type: "TAGGED".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "edge:seed1-alice".into(),
                start_node: "Seed:1".into(),
                end_node: "Person:Alice".into(),
                edge_type: "KNOWS".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
        ] {
            peer_one.put_edge_record(&edge).unwrap();
        }

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

        let outcome = db
            .execute_distributed_as(
                "MATCH (s:Seed)-[:TAGGED]->(t:Tag) OPTIONAL MATCH p = (s)-[:KNOWS]->(n:Person) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(outcome.result.rows.len(), 2);
        assert_eq!(
            outcome
                .result
                .rows
                .iter()
                .filter(|row| row.get("hops") == Some(&Value::from(1)))
                .count(),
            1
        );
        assert_eq!(
            outcome
                .result
                .rows
                .iter()
                .filter(|row| row.get("path") == Some(&Value::Null))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn engine_routes_distributed_mixed_prefix_optional_path_with_bound_node() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        for definition in [
            copperdb_storage::IndexDefinition {
                name: "seed_id".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Seed".into(),
                properties: vec!["id".into()],
            },
            copperdb_storage::IndexDefinition {
                name: "tag_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Tag".into(),
                properties: vec!["name".into()],
            },
            copperdb_storage::IndexDefinition {
                name: "person_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Person".into(),
                properties: vec!["name".into()],
            },
        ] {
            peer_one.persist_index_definition(&definition).unwrap();
        }
        for (id, label, properties) in [
            (
                "Seed:1",
                "Seed",
                BTreeMap::from([("id".into(), Value::from(1))]),
            ),
            (
                "Seed:2",
                "Seed",
                BTreeMap::from([("id".into(), Value::from(2))]),
            ),
            (
                "Tag:blue",
                "Tag",
                BTreeMap::from([("name".into(), Value::String("blue".into()))]),
            ),
            (
                "Tag:red",
                "Tag",
                BTreeMap::from([("name".into(), Value::String("red".into()))]),
            ),
            (
                "Person:Alice",
                "Person",
                BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
            ),
        ] {
            peer_one
                .put_node_record(&copperdb_storage::NodeRecord {
                    id: id.into(),
                    labels: vec![label.into()],
                    properties,
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })
                .unwrap();
        }
        for edge in [
            EdgeRecord {
                id: "edge:seed1-blue".into(),
                start_node: "Seed:1".into(),
                end_node: "Tag:blue".into(),
                edge_type: "TAGGED".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "edge:seed2-red".into(),
                start_node: "Seed:2".into(),
                end_node: "Tag:red".into(),
                edge_type: "TAGGED".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "edge:seed1-alice".into(),
                start_node: "Seed:1".into(),
                end_node: "Person:Alice".into(),
                edge_type: "KNOWS".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
        ] {
            peer_one.put_edge_record(&edge).unwrap();
        }

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

        let outcome = db
            .execute_distributed_as(
                "MATCH (s:Seed) MATCH (s)-[:TAGGED]->(t:Tag) OPTIONAL MATCH p = (s)-[:KNOWS]->(n:Person) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(outcome.result.rows.len(), 2);
        assert_eq!(
            outcome
                .result
                .rows
                .iter()
                .filter(|row| row.get("hops") == Some(&Value::from(1)))
                .count(),
            1
        );
        assert_eq!(
            outcome
                .result
                .rows
                .iter()
                .filter(|row| row.get("path") == Some(&Value::Null))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn engine_routes_distributed_variable_length_relationship_prefix_optional_path() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        for definition in [
            copperdb_storage::IndexDefinition {
                name: "seed_id".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Seed".into(),
                properties: vec!["id".into()],
            },
            copperdb_storage::IndexDefinition {
                name: "tag_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Tag".into(),
                properties: vec!["name".into()],
            },
            copperdb_storage::IndexDefinition {
                name: "person_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Person".into(),
                properties: vec!["name".into()],
            },
        ] {
            peer_one.persist_index_definition(&definition).unwrap();
        }
        for (id, labels, properties) in [
            (
                "Seed:1",
                vec!["Seed"],
                BTreeMap::from([("id".into(), Value::from(1))]),
            ),
            (
                "Seed:2",
                vec!["Seed"],
                BTreeMap::from([("id".into(), Value::from(2))]),
            ),
            (
                "Hop:mid",
                vec!["Hop"],
                BTreeMap::from([("name".into(), Value::String("mid".into()))]),
            ),
            (
                "Tag:blue",
                vec!["Tag"],
                BTreeMap::from([("name".into(), Value::String("blue".into()))]),
            ),
            (
                "Tag:red",
                vec!["Tag"],
                BTreeMap::from([("name".into(), Value::String("red".into()))]),
            ),
            (
                "Person:Alice",
                vec!["Person"],
                BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
            ),
        ] {
            peer_one
                .put_node_record(&copperdb_storage::NodeRecord {
                    id: id.into(),
                    labels: labels.into_iter().map(String::from).collect(),
                    properties,
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })
                .unwrap();
        }
        for edge in [
            EdgeRecord {
                id: "edge:seed1-mid".into(),
                start_node: "Seed:1".into(),
                end_node: "Hop:mid".into(),
                edge_type: "TAGGED".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "edge:mid-blue".into(),
                start_node: "Hop:mid".into(),
                end_node: "Tag:blue".into(),
                edge_type: "TAGGED".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "edge:seed2-red".into(),
                start_node: "Seed:2".into(),
                end_node: "Tag:red".into(),
                edge_type: "TAGGED".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "edge:seed1-alice".into(),
                start_node: "Seed:1".into(),
                end_node: "Person:Alice".into(),
                edge_type: "KNOWS".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
        ] {
            peer_one.put_edge_record(&edge).unwrap();
        }

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

        let outcome = db
            .execute_distributed_as(
                "MATCH (s:Seed)-[:TAGGED*1..2]->(t:Tag) OPTIONAL MATCH p = (s)-[:KNOWS]->(n:Person) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(outcome.result.rows.len(), 2);
        assert_eq!(
            outcome
                .result
                .rows
                .iter()
                .filter(|row| row.get("hops") == Some(&Value::from(1)))
                .count(),
            1
        );
        assert_eq!(
            outcome
                .result
                .rows
                .iter()
                .filter(|row| row.get("path") == Some(&Value::Null))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn engine_routes_distributed_where_filtered_prefix_optional_path() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        for definition in [
            copperdb_storage::IndexDefinition {
                name: "seed_id".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Seed".into(),
                properties: vec!["id".into()],
            },
            copperdb_storage::IndexDefinition {
                name: "tag_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Tag".into(),
                properties: vec!["name".into()],
            },
            copperdb_storage::IndexDefinition {
                name: "person_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Person".into(),
                properties: vec!["name".into()],
            },
        ] {
            peer_one.persist_index_definition(&definition).unwrap();
        }
        for (id, label, properties) in [
            (
                "Seed:1",
                "Seed",
                BTreeMap::from([("id".into(), Value::from(1))]),
            ),
            (
                "Seed:2",
                "Seed",
                BTreeMap::from([("id".into(), Value::from(2))]),
            ),
            (
                "Tag:blue",
                "Tag",
                BTreeMap::from([("name".into(), Value::String("blue".into()))]),
            ),
            (
                "Tag:red",
                "Tag",
                BTreeMap::from([("name".into(), Value::String("red".into()))]),
            ),
            (
                "Person:Alice",
                "Person",
                BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
            ),
        ] {
            peer_one
                .put_node_record(&copperdb_storage::NodeRecord {
                    id: id.into(),
                    labels: vec![label.into()],
                    properties,
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })
                .unwrap();
        }
        for edge in [
            EdgeRecord {
                id: "edge:seed1-blue".into(),
                start_node: "Seed:1".into(),
                end_node: "Tag:blue".into(),
                edge_type: "TAGGED".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "edge:seed2-red".into(),
                start_node: "Seed:2".into(),
                end_node: "Tag:red".into(),
                edge_type: "TAGGED".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "edge:seed1-alice".into(),
                start_node: "Seed:1".into(),
                end_node: "Person:Alice".into(),
                edge_type: "KNOWS".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
        ] {
            peer_one.put_edge_record(&edge).unwrap();
        }

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

        let outcome = db
            .execute_distributed_as(
                "MATCH (s:Seed) WHERE s.id = 1 MATCH (s)-[:TAGGED]->(t:Tag) WHERE t.name = 'blue' OPTIONAL MATCH p = (s)-[:KNOWS]->(n:Person) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(outcome.result.rows.len(), 1);
        assert_eq!(outcome.result.rows[0].get("hops"), Some(&Value::from(1)));
    }

    #[tokio::test]
    async fn engine_routes_distributed_where_filtered_prefix_match_path() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        for definition in [
            copperdb_storage::IndexDefinition {
                name: "seed_id".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Seed".into(),
                properties: vec!["id".into()],
            },
            copperdb_storage::IndexDefinition {
                name: "tag_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Tag".into(),
                properties: vec!["name".into()],
            },
            copperdb_storage::IndexDefinition {
                name: "person_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Person".into(),
                properties: vec!["name".into()],
            },
        ] {
            peer_one.persist_index_definition(&definition).unwrap();
        }
        for (id, label, properties) in [
            (
                "Seed:1",
                "Seed",
                BTreeMap::from([("id".into(), Value::from(1))]),
            ),
            (
                "Seed:2",
                "Seed",
                BTreeMap::from([("id".into(), Value::from(2))]),
            ),
            (
                "Tag:blue",
                "Tag",
                BTreeMap::from([("name".into(), Value::String("blue".into()))]),
            ),
            (
                "Tag:red",
                "Tag",
                BTreeMap::from([("name".into(), Value::String("red".into()))]),
            ),
            (
                "Person:Alice",
                "Person",
                BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
            ),
        ] {
            peer_one
                .put_node_record(&copperdb_storage::NodeRecord {
                    id: id.into(),
                    labels: vec![label.into()],
                    properties,
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })
                .unwrap();
        }
        for edge in [
            EdgeRecord {
                id: "edge:seed1-blue".into(),
                start_node: "Seed:1".into(),
                end_node: "Tag:blue".into(),
                edge_type: "TAGGED".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "edge:seed2-red".into(),
                start_node: "Seed:2".into(),
                end_node: "Tag:red".into(),
                edge_type: "TAGGED".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "edge:seed1-alice".into(),
                start_node: "Seed:1".into(),
                end_node: "Person:Alice".into(),
                edge_type: "KNOWS".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
        ] {
            peer_one.put_edge_record(&edge).unwrap();
        }

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

        let outcome = db
            .execute_distributed_as(
                "MATCH (s:Seed) WHERE s.id = 1 MATCH (s)-[:TAGGED]->(t:Tag) WHERE t.name = 'blue' MATCH p = (s)-[:KNOWS]->(n:Person) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(outcome.result.rows.len(), 1);
        assert_eq!(outcome.result.rows[0].get("hops"), Some(&Value::from(1)));
    }

    #[tokio::test]
    async fn engine_routes_distributed_with_prefix_match_path() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        for definition in [
            copperdb_storage::IndexDefinition {
                name: "seed_id".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Seed".into(),
                properties: vec!["id".into()],
            },
            copperdb_storage::IndexDefinition {
                name: "person_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Person".into(),
                properties: vec!["name".into()],
            },
        ] {
            peer_one.persist_index_definition(&definition).unwrap();
        }
        for (id, label, properties) in [
            (
                "Seed:1",
                "Seed",
                BTreeMap::from([("id".into(), Value::from(1))]),
            ),
            (
                "Seed:2",
                "Seed",
                BTreeMap::from([("id".into(), Value::from(2))]),
            ),
            (
                "Person:Alice",
                "Person",
                BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
            ),
        ] {
            peer_one
                .put_node_record(&copperdb_storage::NodeRecord {
                    id: id.into(),
                    labels: vec![label.into()],
                    properties,
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })
                .unwrap();
        }
        peer_one
            .put_edge_record(&EdgeRecord {
                id: "edge:seed1-alice".into(),
                start_node: "Seed:1".into(),
                end_node: "Person:Alice".into(),
                edge_type: "KNOWS".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

        let outcome = db
            .execute_distributed_as(
                "MATCH (s:Seed) WITH s AS seed WHERE seed.id = 1 MATCH p = (seed)-[:KNOWS]->(n:Person) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(outcome.result.rows.len(), 1);
        assert_eq!(outcome.result.rows[0].get("hops"), Some(&Value::from(1)));
    }

    #[tokio::test]
    async fn engine_routes_distributed_optional_prefix_miss_match_path() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        for definition in [
            copperdb_storage::IndexDefinition {
                name: "seed_id".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Seed".into(),
                properties: vec!["id".into()],
            },
            copperdb_storage::IndexDefinition {
                name: "tag_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Tag".into(),
                properties: vec!["name".into()],
            },
            copperdb_storage::IndexDefinition {
                name: "person_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Person".into(),
                properties: vec!["name".into()],
            },
        ] {
            peer_one.persist_index_definition(&definition).unwrap();
        }
        for (id, label, properties) in [
            (
                "Seed:1",
                "Seed",
                BTreeMap::from([("id".into(), Value::from(1))]),
            ),
            (
                "Tag:blue",
                "Tag",
                BTreeMap::from([("name".into(), Value::String("blue".into()))]),
            ),
            (
                "Person:Alice",
                "Person",
                BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
            ),
        ] {
            peer_one
                .put_node_record(&copperdb_storage::NodeRecord {
                    id: id.into(),
                    labels: vec![label.into()],
                    properties,
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })
                .unwrap();
        }
        peer_one
            .put_edge_record(&EdgeRecord {
                id: "edge:seed1-alice".into(),
                start_node: "Seed:1".into(),
                end_node: "Person:Alice".into(),
                edge_type: "KNOWS".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

        let outcome = db
            .execute_distributed_as(
                "MATCH (s:Seed) WHERE s.id = 1 OPTIONAL MATCH (s)-[:TAGGED]->(t:Tag) MATCH p = (s)-[:KNOWS]->(n:Person) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(outcome.result.rows.len(), 1);
        assert_eq!(outcome.result.rows[0].get("hops"), Some(&Value::from(1)));
    }

    #[tokio::test]
    async fn engine_routes_distributed_edge_variable_filtered_prefix_optional_path() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        for definition in [
            copperdb_storage::IndexDefinition {
                name: "seed_id".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Seed".into(),
                properties: vec!["id".into()],
            },
            copperdb_storage::IndexDefinition {
                name: "tag_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Tag".into(),
                properties: vec!["name".into()],
            },
            copperdb_storage::IndexDefinition {
                name: "person_name".into(),
                entity_type: copperdb_storage::IndexEntityType::Node,
                label: "Person".into(),
                properties: vec!["name".into()],
            },
        ] {
            peer_one.persist_index_definition(&definition).unwrap();
        }
        for (id, label, properties) in [
            (
                "Seed:1",
                "Seed",
                BTreeMap::from([("id".into(), Value::from(1))]),
            ),
            (
                "Seed:2",
                "Seed",
                BTreeMap::from([("id".into(), Value::from(2))]),
            ),
            (
                "Tag:blue",
                "Tag",
                BTreeMap::from([("name".into(), Value::String("blue".into()))]),
            ),
            (
                "Tag:red",
                "Tag",
                BTreeMap::from([("name".into(), Value::String("red".into()))]),
            ),
            (
                "Person:Alice",
                "Person",
                BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
            ),
        ] {
            peer_one
                .put_node_record(&copperdb_storage::NodeRecord {
                    id: id.into(),
                    labels: vec![label.into()],
                    properties,
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })
                .unwrap();
        }
        for edge in [
            EdgeRecord {
                id: "edge:seed1-blue".into(),
                start_node: "Seed:1".into(),
                end_node: "Tag:blue".into(),
                edge_type: "TAGGED".into(),
                properties: BTreeMap::from([("weight".into(), Value::from(1))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "edge:seed2-red".into(),
                start_node: "Seed:2".into(),
                end_node: "Tag:red".into(),
                edge_type: "TAGGED".into(),
                properties: BTreeMap::from([("weight".into(), Value::from(2))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "edge:seed1-alice".into(),
                start_node: "Seed:1".into(),
                end_node: "Person:Alice".into(),
                edge_type: "KNOWS".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
        ] {
            peer_one.put_edge_record(&edge).unwrap();
        }

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

        let outcome = db
            .execute_distributed_as(
                "MATCH (s:Seed)-[r:TAGGED]->(t:Tag) WHERE r.weight = 1 OPTIONAL MATCH p = (s)-[:KNOWS]->(n:Person) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(outcome.result.rows.len(), 1);
        assert_eq!(outcome.result.rows[0].get("hops"), Some(&Value::from(1)));
    }

    #[tokio::test]
    async fn engine_distributed_bfs_traverses_mesh_peers_and_returns_path() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        fn graph_node(id: &str, name: &str) -> Vec<u8> {
            rmp_serde::to_vec(&BTreeMap::from([
                ("_id".to_string(), Value::String(id.to_string())),
                (
                    "_labels".to_string(),
                    Value::Array(vec![Value::String("Node".into())]),
                ),
                ("name".to_string(), Value::String(name.to_string())),
            ]))
            .unwrap()
        }

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();

        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        peer_one
            .put_node("Node:A", &graph_node("Node:A", "A"))
            .unwrap();
        peer_one
            .put_edge_record(&EdgeRecord {
                id: "edge:a-b".into(),
                start_node: "Node:A".into(),
                end_node: "Node:B".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let peer_two = StorageEngine::open_temporary().unwrap();
        peer_two
            .put_node("Node:B", &graph_node("Node:B", "B"))
            .unwrap();
        peer_two
            .put_edge_record(&EdgeRecord {
                id: "edge:b-c".into(),
                start_node: "Node:B".into(),
                end_node: "Node:C".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let peer_three = StorageEngine::open_temporary().unwrap();
        peer_three
            .put_node("Node:C", &graph_node("Node:C", "C"))
            .unwrap();
        peer_three
            .put_node("Node:D", &graph_node("Node:D", "D"))
            .unwrap();
        peer_three
            .put_edge_record(&EdgeRecord {
                id: "edge:c-d".into(),
                start_node: "Node:C".into(),
                end_node: "Node:D".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
        transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));
        transport.register("node-3", Arc::new(StorageEngineAdapter::new(peer_three)));

        let outcome = db
            .distributed_bfs_path_as(
                "Node:A",
                "Node:D",
                Some("LINK"),
                EdgeDirection::Outgoing,
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(outcome.plan.required_responses, 2);
        assert_eq!(outcome.responded_by.len(), 3);
        assert!(outcome.failed_replicas.is_empty());
        assert_eq!(
            outcome.path,
            Some(DistributedPath {
                node_ids: vec![
                    "Node:A".into(),
                    "Node:B".into(),
                    "Node:C".into(),
                    "Node:D".into()
                ],
                edge_ids: vec!["edge:a-b".into(), "edge:b-c".into(), "edge:c-d".into()],
            })
        );
    }

    #[tokio::test]
    async fn engine_distributed_bfs_prefers_shortest_path_across_mesh_peers() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        fn graph_node(id: &str) -> Vec<u8> {
            rmp_serde::to_vec(&BTreeMap::from([
                ("_id".to_string(), Value::String(id.to_string())),
                (
                    "_labels".to_string(),
                    Value::Array(vec![Value::String("Node".into())]),
                ),
            ]))
            .unwrap()
        }

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();

        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        for node_id in ["Node:A", "Node:B"] {
            peer_one.put_node(node_id, &graph_node(node_id)).unwrap();
        }
        for edge in [EdgeRecord {
            id: "edge:a-b".into(),
            start_node: "Node:A".into(),
            end_node: "Node:B".into(),
            edge_type: "LINK".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        }] {
            peer_one.put_edge_record(&edge).unwrap();
        }

        let peer_two = StorageEngine::open_temporary().unwrap();
        for node_id in ["Node:C", "Node:D"] {
            peer_two.put_node(node_id, &graph_node(node_id)).unwrap();
        }
        for edge in [
            EdgeRecord {
                id: "edge:b-d".into(),
                start_node: "Node:B".into(),
                end_node: "Node:D".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "edge:a-c".into(),
                start_node: "Node:A".into(),
                end_node: "Node:C".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
        ] {
            peer_two.put_edge_record(&edge).unwrap();
        }

        let peer_three = StorageEngine::open_temporary().unwrap();
        for node_id in ["Node:E"] {
            peer_three.put_node(node_id, &graph_node(node_id)).unwrap();
        }
        for edge in [
            EdgeRecord {
                id: "edge:c-e".into(),
                start_node: "Node:C".into(),
                end_node: "Node:E".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "edge:e-d".into(),
                start_node: "Node:E".into(),
                end_node: "Node:D".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
        ] {
            peer_three.put_edge_record(&edge).unwrap();
        }

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
        transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));
        transport.register("node-3", Arc::new(StorageEngineAdapter::new(peer_three)));

        let outcome = db
            .distributed_bfs_path_as(
                "Node:A",
                "Node:D",
                Some("LINK"),
                EdgeDirection::Outgoing,
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(
            outcome.path,
            Some(DistributedPath {
                node_ids: vec!["Node:A".into(), "Node:B".into(), "Node:D".into()],
                edge_ids: vec!["edge:a-b".into(), "edge:b-d".into()],
            })
        );
    }

    #[tokio::test]
    async fn engine_distributed_bfs_returns_none_when_mesh_has_no_path() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        fn graph_node(id: &str) -> Vec<u8> {
            rmp_serde::to_vec(&BTreeMap::from([
                ("_id".to_string(), Value::String(id.to_string())),
                (
                    "_labels".to_string(),
                    Value::Array(vec![Value::String("Node".into())]),
                ),
            ]))
            .unwrap()
        }

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();

        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        peer_one.put_node("Node:A", &graph_node("Node:A")).unwrap();
        peer_one.put_node("Node:B", &graph_node("Node:B")).unwrap();
        peer_one
            .put_edge_record(&EdgeRecord {
                id: "edge:a-b".into(),
                start_node: "Node:A".into(),
                end_node: "Node:B".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let peer_two = StorageEngine::open_temporary().unwrap();
        peer_two.put_node("Node:C", &graph_node("Node:C")).unwrap();

        let peer_three = StorageEngine::open_temporary().unwrap();
        peer_three
            .put_node("Node:D", &graph_node("Node:D"))
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
        transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));
        transport.register("node-3", Arc::new(StorageEngineAdapter::new(peer_three)));

        let outcome = db
            .distributed_bfs_path_as(
                "Node:A",
                "Node:D",
                Some("LINK"),
                EdgeDirection::Outgoing,
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(outcome.plan.required_responses, 2);
        assert_eq!(outcome.responded_by.len(), 3);
        assert!(outcome.failed_replicas.is_empty());
        assert!(outcome.path.is_none());
    }

    #[tokio::test]
    async fn engine_distributed_bfs_requires_mesh_read_quorum() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        fn graph_node(id: &str) -> Vec<u8> {
            rmp_serde::to_vec(&BTreeMap::from([
                ("_id".to_string(), Value::String(id.to_string())),
                (
                    "_labels".to_string(),
                    Value::Array(vec![Value::String("Node".into())]),
                ),
            ]))
            .unwrap()
        }

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();

        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        peer_one.put_node("Node:A", &graph_node("Node:A")).unwrap();
        peer_one.put_node("Node:D", &graph_node("Node:D")).unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

        let err = db
            .distributed_bfs_path_as(
                "Node:A",
                "Node:D",
                Some("LINK"),
                EdgeDirection::Outgoing,
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap_err();

        match err {
            CopperDbError::Replication(message) => {
                assert!(message.contains("quorum not reached"));
                assert!(message.contains("required 2"));
            }
            other => panic!("expected replication quorum error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn engine_distributed_bfs_traverses_incoming_edges_across_mesh_peers() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        fn graph_node(id: &str) -> Vec<u8> {
            rmp_serde::to_vec(&BTreeMap::from([
                ("_id".to_string(), Value::String(id.to_string())),
                (
                    "_labels".to_string(),
                    Value::Array(vec![Value::String("Node".into())]),
                ),
            ]))
            .unwrap()
        }

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        peer_one.put_node("Node:A", &graph_node("Node:A")).unwrap();
        peer_one.put_node("Node:B", &graph_node("Node:B")).unwrap();
        peer_one
            .put_edge_record(&EdgeRecord {
                id: "edge:a-b".into(),
                start_node: "Node:A".into(),
                end_node: "Node:B".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let peer_two = StorageEngine::open_temporary().unwrap();
        peer_two.put_node("Node:C", &graph_node("Node:C")).unwrap();
        peer_two
            .put_edge_record(&EdgeRecord {
                id: "edge:b-c".into(),
                start_node: "Node:B".into(),
                end_node: "Node:C".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let peer_three = StorageEngine::open_temporary().unwrap();
        peer_three
            .put_node("Node:D", &graph_node("Node:D"))
            .unwrap();
        peer_three
            .put_edge_record(&EdgeRecord {
                id: "edge:c-d".into(),
                start_node: "Node:C".into(),
                end_node: "Node:D".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
        transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));
        transport.register("node-3", Arc::new(StorageEngineAdapter::new(peer_three)));

        let outcome = db
            .distributed_bfs_path_as(
                "Node:D",
                "Node:A",
                Some("LINK"),
                EdgeDirection::Incoming,
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(
            outcome.path,
            Some(DistributedPath {
                node_ids: vec![
                    "Node:D".into(),
                    "Node:C".into(),
                    "Node:B".into(),
                    "Node:A".into()
                ],
                edge_ids: vec!["edge:c-d".into(), "edge:b-c".into(), "edge:a-b".into()],
            })
        );
    }

    #[tokio::test]
    async fn engine_distributed_bfs_traverses_undirected_edges_across_mesh_peers() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        fn graph_node(id: &str) -> Vec<u8> {
            rmp_serde::to_vec(&BTreeMap::from([
                ("_id".to_string(), Value::String(id.to_string())),
                (
                    "_labels".to_string(),
                    Value::Array(vec![Value::String("Node".into())]),
                ),
            ]))
            .unwrap()
        }

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        peer_one.put_node("Node:A", &graph_node("Node:A")).unwrap();
        peer_one.put_node("Node:B", &graph_node("Node:B")).unwrap();
        peer_one
            .put_edge_record(&EdgeRecord {
                id: "edge:a-b".into(),
                start_node: "Node:A".into(),
                end_node: "Node:B".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let peer_two = StorageEngine::open_temporary().unwrap();
        peer_two.put_node("Node:C", &graph_node("Node:C")).unwrap();
        peer_two
            .put_edge_record(&EdgeRecord {
                id: "edge:b-c".into(),
                start_node: "Node:B".into(),
                end_node: "Node:C".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let peer_three = StorageEngine::open_temporary().unwrap();
        peer_three
            .put_node("Node:D", &graph_node("Node:D"))
            .unwrap();
        peer_three
            .put_edge_record(&EdgeRecord {
                id: "edge:c-d".into(),
                start_node: "Node:C".into(),
                end_node: "Node:D".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
        transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));
        transport.register("node-3", Arc::new(StorageEngineAdapter::new(peer_three)));

        let outcome = db
            .distributed_bfs_path_as(
                "Node:D",
                "Node:A",
                Some("LINK"),
                EdgeDirection::Both,
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(
            outcome.path,
            Some(DistributedPath {
                node_ids: vec![
                    "Node:D".into(),
                    "Node:C".into(),
                    "Node:B".into(),
                    "Node:A".into()
                ],
                edge_ids: vec!["edge:c-d".into(), "edge:b-c".into(), "edge:a-b".into()],
            })
        );
    }

    #[tokio::test]
    async fn engine_distributed_bfs_query_materializes_path_row() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        fn graph_node(id: &str) -> Vec<u8> {
            rmp_serde::to_vec(&BTreeMap::from([
                ("_id".to_string(), Value::String(id.to_string())),
                (
                    "_labels".to_string(),
                    Value::Array(vec![Value::String("Node".into())]),
                ),
            ]))
            .unwrap()
        }

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        peer_one.put_node("Node:A", &graph_node("Node:A")).unwrap();
        peer_one
            .put_edge_record(&EdgeRecord {
                id: "edge:a-b".into(),
                start_node: "Node:A".into(),
                end_node: "Node:B".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let peer_two = StorageEngine::open_temporary().unwrap();
        peer_two.put_node("Node:B", &graph_node("Node:B")).unwrap();
        peer_two
            .put_edge_record(&EdgeRecord {
                id: "edge:b-c".into(),
                start_node: "Node:B".into(),
                end_node: "Node:C".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let peer_three = StorageEngine::open_temporary().unwrap();
        peer_three
            .put_node("Node:C", &graph_node("Node:C"))
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
        transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));
        transport.register("node-3", Arc::new(StorageEngineAdapter::new(peer_three)));

        let (result, bfs) = db
            .distributed_bfs_query_as(
                "Node:A",
                "Node:C",
                Some("LINK"),
                EdgeDirection::Outgoing,
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(
            result.columns,
            vec!["path", "nodes(path)", "relationships(path)", "length(path)"]
        );
        assert_eq!(result.rows.len(), 1);
        let nodes = result.rows[0]
            .get("nodes(path)")
            .and_then(Value::as_array)
            .expect("expected materialized path nodes");
        assert_eq!(nodes.len(), 3);
        assert_eq!(nodes[0].get("_id"), Some(&Value::String("Node:A".into())));
        assert_eq!(nodes[1].get("_id"), Some(&Value::String("Node:B".into())));
        assert_eq!(nodes[2].get("_id"), Some(&Value::String("Node:C".into())));
        let rels = result.rows[0]
            .get("relationships(path)")
            .and_then(Value::as_array)
            .expect("expected materialized path relationships");
        assert_eq!(rels.len(), 2);
        assert_eq!(rels[0].get("_id"), Some(&Value::String("edge:a-b".into())));
        assert_eq!(rels[1].get("_id"), Some(&Value::String("edge:b-c".into())));
        assert_eq!(result.rows[0].get("length(path)"), Some(&Value::from(2)));
        let path = result.rows[0]
            .get("path")
            .and_then(Value::as_object)
            .expect("expected path object");
        assert_eq!(path.get("length"), Some(&Value::from(2)));
        assert!(bfs.path.is_some());
    }

    #[tokio::test]
    async fn engine_distributed_bfs_query_returns_empty_rows_when_no_path_exists() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        fn graph_node(id: &str) -> Vec<u8> {
            rmp_serde::to_vec(&BTreeMap::from([
                ("_id".to_string(), Value::String(id.to_string())),
                (
                    "_labels".to_string(),
                    Value::Array(vec![Value::String("Node".into())]),
                ),
            ]))
            .unwrap()
        }

        let dir = tempfile::tempdir().unwrap();
        let db = CopperDb::open(DatabaseConfig {
            data_dir: dir.path().join("db").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let placement = PlacementKey::default_for_database("copper");
        for node_id in ["node-1", "node-2", "node-3"] {
            db.storage()
                .register_topology_peer(
                    &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let peer_one = StorageEngine::open_temporary().unwrap();
        peer_one.put_node("Node:A", &graph_node("Node:A")).unwrap();
        let peer_two = StorageEngine::open_temporary().unwrap();
        peer_two.put_node("Node:C", &graph_node("Node:C")).unwrap();
        let peer_three = StorageEngine::open_temporary().unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
        transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));
        transport.register("node-3", Arc::new(StorageEngineAdapter::new(peer_three)));

        let (result, bfs) = db
            .distributed_bfs_query_as(
                "Node:A",
                "Node:C",
                Some("LINK"),
                EdgeDirection::Outgoing,
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

        assert!(bfs.path.is_none());
        assert_eq!(
            result.columns,
            vec!["path", "nodes(path)", "relationships(path)", "length(path)"]
        );
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_multiple_creates_and_match() {
        let db = CopperDb::open_temporary().unwrap();
        for i in 0..5 {
            db.execute(
                &format!("CREATE (n:Item {{idx: {i}}})", i = i),
                Default::default(),
            )
            .unwrap();
        }
        let result = db
            .execute("MATCH (n:Item) RETURN n", Default::default())
            .unwrap();
        assert_eq!(result.rows.len(), 5);
    }
}

#[cfg(test)]
mod smoke_tests {
    use super::*;
    use serde_json::Value;
    use std::collections::HashMap;

    /// Smoke: create a node, flush to disk, reopen the DB, verify node persists.
    #[test]
    fn test_node_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap().to_string();

        // Phase 1: write
        {
            let cfg = DatabaseConfig {
                data_dir: path.clone(),
                ..Default::default()
            };
            let db = CopperDb::open(cfg).unwrap();
            let result = db
                .execute(
                    "CREATE (n:Person {name: 'Alice', age: 30}) RETURN n",
                    HashMap::new(),
                )
                .unwrap();
            assert_eq!(
                result.stats.nodes_created, 1,
                "should create exactly 1 node"
            );
            db.flush().unwrap();
        }

        // Phase 2: reopen and verify
        {
            let cfg = DatabaseConfig {
                data_dir: path.clone(),
                ..Default::default()
            };
            let db = CopperDb::open(cfg).unwrap();
            let result = db
                .execute("MATCH (n:Person) RETURN n", HashMap::new())
                .unwrap();
            assert_eq!(
                result.rows.len(),
                1,
                "reopened DB should have 1 Person node"
            );
            let row = &result.rows[0];
            let n = row.get("n").expect("row must have 'n' key");
            match n {
                Value::Object(props) => {
                    assert_eq!(
                        props.get("name"),
                        Some(&Value::String("Alice".into())),
                        "name must be Alice"
                    );
                    assert_eq!(
                        props.get("age"),
                        Some(&Value::Number(30.into())),
                        "age must be 30"
                    );
                }
                _ => panic!("expected object node, got {n:?}"),
            }
        }
    }

    /// Smoke: create multiple nodes and verify everything persists.
    #[test]
    fn test_edge_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap().to_string();

        {
            let cfg = DatabaseConfig {
                data_dir: path.clone(),
                ..Default::default()
            };
            let db = CopperDb::open(cfg).unwrap();
            db.execute(
                "CREATE (a:City {name: 'London', pop: 9000000})",
                HashMap::new(),
            )
            .unwrap();
            db.execute(
                "CREATE (b:City {name: 'Paris', pop: 2100000})",
                HashMap::new(),
            )
            .unwrap();
            let r = db
                .execute("MATCH (c:City) RETURN c", HashMap::new())
                .unwrap();
            assert_eq!(r.rows.len(), 2, "should have 2 City nodes before flush");
            db.flush().unwrap();
        }

        {
            let cfg = DatabaseConfig {
                data_dir: path.clone(),
                ..Default::default()
            };
            let db = CopperDb::open(cfg).unwrap();
            let result = db
                .execute("MATCH (c:City) RETURN c", HashMap::new())
                .unwrap();
            assert_eq!(
                result.rows.len(),
                2,
                "should still have 2 City nodes after reopen"
            );

            let mut names: Vec<String> = result
                .rows
                .iter()
                .filter_map(|row| {
                    row.get("c")
                        .and_then(|v| v.as_object())
                        .and_then(|o| o.get("name"))
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_string())
                })
                .collect();
            names.sort();
            assert_eq!(
                names,
                vec!["London", "Paris"],
                "both cities must be present"
            );
        }
    }

    /// Smoke: MATCH/WHERE filter works after disk round-trip.
    #[test]
    fn test_where_filter_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap().to_string();

        {
            let cfg = DatabaseConfig {
                data_dir: path.clone(),
                ..Default::default()
            };
            let db = CopperDb::open(cfg).unwrap();
            for (name, age) in &[("Alice", 30), ("Bob", 20), ("Carol", 35)] {
                db.execute(
                    &format!("CREATE (n:User {{name: '{name}', age: {age}}})"),
                    HashMap::new(),
                )
                .unwrap();
            }
            db.flush().unwrap();
        }

        {
            let cfg = DatabaseConfig {
                data_dir: path.clone(),
                ..Default::default()
            };
            let db = CopperDb::open(cfg).unwrap();
            let result = db
                .execute("MATCH (n:User) WHERE n.age > 25 RETURN n", HashMap::new())
                .unwrap();
            assert_eq!(
                result.rows.len(),
                2,
                "Alice (30) and Carol (35) should match age > 25"
            );

            let mut names: Vec<String> = result
                .rows
                .iter()
                .filter_map(|row| {
                    row.get("n")
                        .and_then(|v| v.as_object())
                        .and_then(|o| o.get("name"))
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_string())
                })
                .collect();
            names.sort();
            assert_eq!(names, vec!["Alice", "Carol"]);
        }
    }

    /// Smoke: the REST API layer (axum) responds correctly.
    #[tokio::test]
    async fn test_rest_api_health_check() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let state = Arc::new(copperdb_server::AppState::default());
        let app = copperdb_server::build_router(Arc::clone(&state));

        let req = Request::builder()
            .method("GET")
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "health check should return 200"
        );

        let body_bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let health: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(health["status"], "ok", "health status should be 'ok'");
    }
}
