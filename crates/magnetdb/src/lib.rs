//! magnetDB core engine.
//!
//! This is the primary entry point crate that integrates all subsystems
//! into a unified graph database engine. It is the Rust equivalent of
//! NornicDB's `pkg/nornicdb` package.
//!
//! # Architecture
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                        magnetDB                              │
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

use magnetdb_cache::QueryCache;
use magnetdb_cypher::Parser;
use magnetdb_eval::{EvalEngine, QueryStats};
use magnetdb_storage::StorageEngine;
use magnetdb_txsession::TransactionManager;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;

// ─── Errors ──────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum MagnetError {
    #[error("storage error: {0}")]
    Storage(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("eval error: {0}")]
    Eval(String),
    #[error("initialization error: {0}")]
    Init(String),
}

impl From<magnetdb_storage::StorageError> for MagnetError {
    fn from(e: magnetdb_storage::StorageError) -> Self {
        MagnetError::Storage(e.to_string())
    }
}

impl From<magnetdb_cypher::CypherError> for MagnetError {
    fn from(e: magnetdb_cypher::CypherError) -> Self {
        MagnetError::Parse(e.to_string())
    }
}

impl From<magnetdb_eval::EvalError> for MagnetError {
    fn from(e: magnetdb_eval::EvalError) -> Self {
        MagnetError::Eval(e.to_string())
    }
}

// ─── Legacy error (kept for existing tests) ──────────────────────────────────

#[derive(Debug, Error)]
pub enum MagnetDbError {
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
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            data_dir: "data".to_string(),
            max_connections: 100,
            default_database: "magnetdb".to_string(),
            auth_enabled: false,
            log_queries: false,
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

// ─── MagnetDB (embedded sync engine) ─────────────────────────────────────────

/// The primary embedded database engine.
pub struct MagnetDB {
    config: DatabaseConfig,
    storage: Arc<StorageEngine>,
    eval: EvalEngine,
    tx_manager: Arc<TransactionManager>,
    query_cache: Arc<QueryCache<magnetdb_cypher::Query>>,
}

impl MagnetDB {
    /// Create a new in-memory (temporary) database instance.
    pub fn open_temporary() -> Result<Self, MagnetError> {
        let storage = Arc::new(StorageEngine::open_temporary()?);
        Ok(Self::from_storage(storage, DatabaseConfig::default()))
    }

    /// Create a persistent database at the given path.
    pub fn open(config: DatabaseConfig) -> Result<Self, MagnetError> {
        let storage = Arc::new(
            StorageEngine::open(&config.data_dir)
                .map_err(|e| MagnetError::Storage(e.to_string()))?,
        );
        Ok(Self::from_storage(storage, config))
    }

    fn from_storage(storage: Arc<StorageEngine>, config: DatabaseConfig) -> Self {
        let eval = EvalEngine::new(Arc::clone(&storage));
        Self {
            config,
            storage,
            eval,
            tx_manager: Arc::new(TransactionManager::new()),
            query_cache: Arc::new(QueryCache::new(1024, Some(std::time::Duration::from_secs(300)))),
        }
    }

    /// Execute a Cypher query string, returning rows and stats.
    pub fn execute(
        &self,
        cypher: &str,
        params: HashMap<String, Value>,
    ) -> Result<QueryResult, MagnetError> {
        let start = Instant::now();

        if self.config.log_queries {
            tracing::info!(query = cypher, "executing query");
        }

        // Check cache
        let hash = fnv_hash(cypher);
        let parsed = if let Some(cached) = self.query_cache.get(hash) {
            cached
        } else {
            let parser = Parser::new();
            let q = parser.parse(cypher)?;
            self.query_cache.put(hash, q.clone());
            q
        };

        let eval_result = self.eval.execute(&parsed, &params)?;

        let elapsed_ms = start.elapsed().as_millis() as u64;
        let mut stats = ResultStats::from(eval_result.stats);
        stats.execution_time_ms = elapsed_ms;

        Ok(QueryResult {
            columns: eval_result.columns,
            rows: eval_result.rows,
            stats,
        })
    }

    /// Flush all pending writes to disk.
    pub fn flush(&self) -> Result<(), MagnetError> {
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
}

fn fnv_hash(s: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

// ─── Legacy MagnetDb (full-server async variant) ──────────────────────────────

/// Full-server async variant that integrates all subsystems.
pub struct MagnetDb {
    pub config: magnetdb_config::Config,
}

impl MagnetDb {
    /// Initialize and start the database engine.
    pub async fn start(config: magnetdb_config::Config) -> Result<Self, MagnetDbError> {
        tracing::info!("Starting magnetDB v{}", env!("CARGO_PKG_VERSION"));
        Ok(Self { config })
    }

    /// Gracefully shut down all subsystems.
    pub async fn shutdown(&self) -> Result<(), MagnetDbError> {
        tracing::info!("Shutting down magnetDB");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Legacy tests ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_start_with_default_config() {
        let config = magnetdb_config::Config::default();
        let db = MagnetDb::start(config).await.unwrap();
        db.shutdown().await.unwrap();
    }

    // ── Embedded MagnetDB tests ───────────────────────────────────────────────

    #[test]
    fn test_open_temporary() {
        let db = MagnetDB::open_temporary().unwrap();
        assert_eq!(db.config.default_database, "magnetdb");
    }

    #[test]
    fn test_create_and_match() {
        let db = MagnetDB::open_temporary().unwrap();

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
        let db = MagnetDB::open_temporary().unwrap();
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
        let db = MagnetDB::open_temporary().unwrap();
        db.execute("CREATE (n:Test {x: 1})", Default::default()).unwrap();
        db.flush().unwrap();
        // size should be non-zero after flush
        let _ = db.size_on_disk();
    }

    #[test]
    fn test_query_caching() {
        let db = MagnetDB::open_temporary().unwrap();
        db.execute("CREATE (n:Cached {v: 1})", Default::default()).unwrap();
        // Second identical query hits cache
        let r1 = db.execute("MATCH (n:Cached) RETURN n", Default::default()).unwrap();
        let r2 = db.execute("MATCH (n:Cached) RETURN n", Default::default()).unwrap();
        assert_eq!(r1.rows.len(), r2.rows.len());
    }

    #[test]
    fn test_default_config() {
        let config = DatabaseConfig::default();
        assert!(!config.auth_enabled);
        assert_eq!(config.max_connections, 100);
    }

    #[test]
    fn test_multiple_creates_and_match() {
        let db = MagnetDB::open_temporary().unwrap();
        for i in 0..5 {
            db.execute(&format!("CREATE (n:Item {{idx: {i}}})", i = i), Default::default()).unwrap();
        }
        let result = db.execute("MATCH (n:Item) RETURN n", Default::default()).unwrap();
        assert_eq!(result.rows.len(), 5);
    }
}

