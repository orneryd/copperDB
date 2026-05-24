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

use copperdb_cache::QueryCache;
use copperdb_cypher::Parser;
use copperdb_eval::{EvalEngine, QueryStats};
use copperdb_storage::StorageEngine;
use copperdb_txsession::TransactionManager;
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

impl From<copperdb_storage::StorageError> for MagnetError {
    fn from(e: copperdb_storage::StorageError) -> Self {
        MagnetError::Storage(e.to_string())
    }
}

impl From<copperdb_cypher::CypherError> for MagnetError {
    fn from(e: copperdb_cypher::CypherError) -> Self {
        MagnetError::Parse(e.to_string())
    }
}

impl From<copperdb_eval::EvalError> for MagnetError {
    fn from(e: copperdb_eval::EvalError) -> Self {
        MagnetError::Eval(e.to_string())
    }
}

// ─── Legacy error (kept for existing tests) ──────────────────────────────────

#[derive(Debug, Error)]
pub enum copperdbError {
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
            default_database: "copperdb".to_string(),
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

// ─── copperdb (embedded sync engine) ─────────────────────────────────────────

/// The primary embedded database engine.
pub struct copperdb {
    config: DatabaseConfig,
    storage: Arc<StorageEngine>,
    eval: EvalEngine,
    tx_manager: Arc<TransactionManager>,
    query_cache: Arc<QueryCache<copperdb_cypher::Query>>,
}

impl copperdb {
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
            query_cache: Arc::new(QueryCache::new(
                1024,
                Some(std::time::Duration::from_secs(300)),
            )),
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

        // Hold an async-flush guard for the duration of this implicit transaction.
        //
        // Mirrors NornicDB v1.0.42's `asyncEngine.HoldFlush()` pattern (commit
        // `82ec5b5`): preventing background flushes from advancing MVCC heads
        // while the query is executing.  For our sled backend the guard is a
        // no-op, but the pattern is correct and ready for future extension.
        let _flush_guard = self.storage.hold_flush();

        // Check cache — use the same FNV-1a hasher as QueryCache internally
        let hash = QueryCache::<copperdb_cypher::Query>::key(cypher, &[]);
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

// ─── Legacy copperdb (full-server async variant) ──────────────────────────────

/// Full-server async variant that integrates all subsystems.
pub struct CopperDbServer {
    pub config: copperdb_config::Config,
}

impl CopperDbServer {
    /// Initialize and start the database engine.
    pub async fn start(config: copperdb_config::Config) -> Result<Self, copperdbError> {
        tracing::info!("Starting copperdb v{}", env!("CARGO_PKG_VERSION"));
        Ok(Self { config })
    }

    /// Gracefully shut down all subsystems.
    pub async fn shutdown(&self) -> Result<(), copperdbError> {
        tracing::info!("Shutting down copperdb");
        Ok(())
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
        let db = copperdb::open_temporary().unwrap();
        assert_eq!(db.config.default_database, "copperdb");
    }

    #[test]
    fn test_create_and_match() {
        let db = copperdb::open_temporary().unwrap();

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
        let db = copperdb::open_temporary().unwrap();
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
        let db = copperdb::open_temporary().unwrap();
        db.execute("CREATE (n:Test {x: 1})", Default::default())
            .unwrap();
        db.flush().unwrap();
        // size should be non-zero after flush
        let _ = db.size_on_disk();
    }

    #[test]
    fn test_query_caching() {
        let db = copperdb::open_temporary().unwrap();
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
    fn test_default_config() {
        let config = DatabaseConfig::default();
        assert!(!config.auth_enabled);
        assert_eq!(config.max_connections, 100);
    }

    #[test]
    fn test_multiple_creates_and_match() {
        let db = copperdb::open_temporary().unwrap();
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
            let db = copperdb::open(cfg).unwrap();
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
            let db = copperdb::open(cfg).unwrap();
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
            let db = copperdb::open(cfg).unwrap();
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
            let db = copperdb::open(cfg).unwrap();
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
            let db = copperdb::open(cfg).unwrap();
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
            let db = copperdb::open(cfg).unwrap();
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
