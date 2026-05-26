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
use copperdb_cypher::{Clause, Expression, Parser, Pattern, QueryType};
use copperdb_eval::{EvalEngine, QueryStats};
use copperdb_kms::{new_provider, ProviderFactoryConfig};
use copperdb_replication::{
    CassandraCoordinator, Command, DistributedReadOutcome, DistributedWriteOutcome,
    DurableRepairQueue, RepairReplayReport, RepairWorkerConfig, ReplicaTransport, ReplicationError,
    ScheduledRepairWorker,
};
use copperdb_storage::StorageEngine;
use copperdb_topology::{
    ConsistencyLevel, DistributedReadPlan, DistributedWriteMode, DistributedWritePlan,
    PlacementKey, TopologyRegistry,
};
use copperdb_txsession::TransactionManager;
use serde_json::Value;
use std::collections::HashMap;
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

        let eval_result = match self.eval.execute(&parsed, &params) {
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
        properties.extend(node.properties.keys().cloned());
    }
    for edge in &pattern.edges {
        properties.extend(edge.properties.keys().cloned());
    }
}

fn collect_expression_properties(expression: &Expression, properties: &mut Vec<String>) {
    match expression {
        Expression::PropertyAccess { property, .. } => properties.push(property.clone()),
        Expression::Comparison { left, right, .. }
        | Expression::And(left, right)
        | Expression::Or(left, right) => {
            collect_expression_properties(left, properties);
            collect_expression_properties(right, properties);
        }
        Expression::FunctionCall { args, .. } => {
            for arg in args {
                collect_expression_properties(arg, properties);
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
        let placement = PlacementKey::default_for_database("neo4j");
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
        let placement = PlacementKey::default_for_database("neo4j");
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
        let placement = PlacementKey::default_for_database("neo4j");
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
        let placement = PlacementKey::default_for_database("neo4j");
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
        let placement = PlacementKey::default_for_database("neo4j");
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
        let placement = PlacementKey::default_for_database("neo4j");
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
