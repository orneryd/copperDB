//! Bolt server TCP listener and connection handler.
//!
//! Supports both raw TCP (standard Bolt) and WebSocket upgrades on the same port.
//! Mirrors NornicDB's `pkg/bolt/server.go` + `pkg/bolt/transport_ws.go`.

use crate::BoltError;
use crate::dispatch;
use crate::messages::BoltMessage;
use crate::packstream::Value;
use crate::wsconn;
use copperdb_errors::{TransientTransactionCode, map_transient_transaction_error};
use copperdb_localization::{LanguageTag, Manager, messages as localized_messages};
use copperdb_otel::{CancellationProtocol, CancellationStage, Telemetry};
use opentelemetry::global;
use opentelemetry::propagation::Extractor;
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::accept_async;
use tracing::{debug, info, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

const BOLT_RECEIVE_TIMEOUT: Duration = Duration::from_secs(120);
const BOLT_STATEMENT_TIMEOUT_ENV: &str = "COPPERDB_BOLT_STATEMENT_TIMEOUT";
const UPSTREAM_BOLT_STATEMENT_TIMEOUT_ENV: &str = "NORNICDB_BOLT_STATEMENT_TIMEOUT";
const MAX_BOLT_CURSORS: usize = 64;

struct BoltTraceContext<'a> {
    metadata: &'a HashMap<String, serde_json::Value>,
}

impl<'a> BoltTraceContext<'a> {
    fn new(metadata: &'a HashMap<String, serde_json::Value>) -> Self {
        Self { metadata }
    }
}

impl Extractor for BoltTraceContext<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        let metadata_key = match key {
            "traceparent" => "nornicdb.traceparent",
            "tracestate" => "nornicdb.tracestate",
            _ => return None,
        };
        self.metadata
            .get(metadata_key)
            .and_then(serde_json::Value::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        [
            ("nornicdb.traceparent", "traceparent"),
            ("nornicdb.tracestate", "tracestate"),
        ]
        .into_iter()
        .filter_map(|(metadata_key, propagation_key)| {
            self.metadata
                .contains_key(metadata_key)
                .then_some(propagation_key)
        })
        .collect()
    }
}

/// Result of executing a Cypher query through Bolt.
#[derive(Debug, Clone)]
pub struct BoltQueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub encoded_rows: Option<Arc<Vec<Vec<u8>>>>,
    pub stats: BoltResultStats,
    pub notifications: Vec<BoltNotification>,
}

impl BoltQueryResult {
    fn row_count(&self) -> usize {
        self.encoded_rows
            .as_ref()
            .map_or_else(|| self.rows.len(), |rows| rows.len())
    }
}

#[derive(Debug, Clone, Default)]
pub struct BoltResultStats {
    pub nodes_created: usize,
    pub nodes_deleted: usize,
    pub relationships_created: usize,
    pub relationships_deleted: usize,
    pub properties_set: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoltNotification {
    pub code: String,
    pub title: String,
    pub description: String,
    pub severity: String,
    pub category: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoltPrincipal {
    pub username: String,
    pub roles: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoltTransaction {
    pub id: String,
    pub database: String,
}

#[derive(Debug)]
pub struct BoltTransactionError {
    message: String,
    transient_code: Option<TransientTransactionCode>,
}

impl BoltTransactionError {
    pub fn from_error<E>(error: E) -> Self
    where
        E: Error + 'static,
    {
        Self {
            message: error.to_string(),
            transient_code: map_transient_transaction_error(&error),
        }
    }

    fn neo4j_code(&self, fallback: &'static str) -> &'static str {
        self.transient_code
            .map(TransientTransactionCode::as_neo4j_code)
            .unwrap_or(fallback)
    }
}

impl std::fmt::Display for BoltTransactionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message.fmt(formatter)
    }
}

impl From<String> for BoltTransactionError {
    fn from(message: String) -> Self {
        Self {
            message,
            transient_code: None,
        }
    }
}

impl From<&str> for BoltTransactionError {
    fn from(message: &str) -> Self {
        Self::from(message.to_owned())
    }
}

#[derive(Debug)]
pub enum BoltExecutionError {
    Message(String),
    RequestCancelled(copperdb_util::RequestCancelled),
}

impl BoltExecutionError {
    fn neo4j_code(&self) -> &'static str {
        match self {
            Self::RequestCancelled(_) => "Neo.ClientError.Statement.SyntaxError",
            Self::Message(_) => "Neo.ClientError.Statement.ExecutionFailed",
        }
    }
}

impl std::fmt::Display for BoltExecutionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Message(message) => message.fmt(formatter),
            Self::RequestCancelled(error) => error.fmt(formatter),
        }
    }
}

impl From<String> for BoltExecutionError {
    fn from(message: String) -> Self {
        Self::Message(message)
    }
}

impl From<&str> for BoltExecutionError {
    fn from(message: &str) -> Self {
        Self::Message(message.to_owned())
    }
}

impl From<copperdb_util::RequestCancelled> for BoltExecutionError {
    fn from(error: copperdb_util::RequestCancelled) -> Self {
        Self::RequestCancelled(error)
    }
}

pub trait BoltAuthProvider: Send + Sync {
    fn authenticate(&self, username: &str, password: &str) -> Result<BoltPrincipal, String>;
}

/// Trait for components that can execute Cypher queries.
/// Mirrors NornicDB's `QueryExecutor` interface.
pub trait QueryExecutor: Send + Sync {
    fn execute(
        &self,
        query: &str,
        params: &HashMap<String, serde_json::Value>,
    ) -> Result<BoltQueryResult, String>;

    fn execute_on_database(
        &self,
        database: Option<&str>,
        query: &str,
        params: &HashMap<String, serde_json::Value>,
    ) -> Result<BoltQueryResult, String> {
        let _ = database;
        self.execute(query, params)
    }

    /// Execute on a specific database with a pre-created [`RequestContext`].
    ///
    /// The caller is responsible for creating the context via
    /// [`RequestContext::root`] and keeping the guard in the outer async scope
    /// so that a client-disconnect drops the guard and cancels the query.
    fn execute_on_database_with_context(
        &self,
        database: &str,
        query: &str,
        params: &HashMap<String, serde_json::Value>,
        request_context: copperdb_util::RequestContext,
    ) -> Result<BoltQueryResult, BoltExecutionError> {
        let _ = request_context;
        self.execute_on_database(Some(database), query, params)
            .map_err(BoltExecutionError::from)
    }

    fn execute_as_on_database_with_context(
        &self,
        database: &str,
        query: &str,
        params: &HashMap<String, serde_json::Value>,
        request_context: copperdb_util::RequestContext,
        _principal: Option<&BoltPrincipal>,
    ) -> Result<BoltQueryResult, BoltExecutionError> {
        self.execute_on_database_with_context(database, query, params, request_context)
    }

    fn execute_as_on_database_with_context_and_bookmarks(
        &self,
        database: &str,
        query: &str,
        params: &HashMap<String, serde_json::Value>,
        request_context: copperdb_util::RequestContext,
        principal: Option<&BoltPrincipal>,
        _bookmarks: &[String],
    ) -> Result<BoltQueryResult, BoltExecutionError> {
        self.execute_as_on_database_with_context(
            database,
            query,
            params,
            request_context,
            principal,
        )
    }

    fn begin_transaction(
        &self,
        _database: &str,
        _metadata: &HashMap<String, serde_json::Value>,
        _principal: Option<&BoltPrincipal>,
    ) -> Result<BoltTransaction, String> {
        Err("explicit transactions are not supported by this executor".into())
    }

    fn commit_transaction(
        &self,
        _transaction: &BoltTransaction,
    ) -> Result<String, BoltTransactionError> {
        Err("explicit transactions are not supported by this executor".into())
    }

    fn rollback_transaction(&self, _transaction: &BoltTransaction) -> Result<(), String> {
        Err("explicit transactions are not supported by this executor".into())
    }

    fn execute_in_transaction_with_context(
        &self,
        transaction: &BoltTransaction,
        query: &str,
        params: &HashMap<String, serde_json::Value>,
        request_context: copperdb_util::RequestContext,
        principal: Option<&BoltPrincipal>,
    ) -> Result<BoltQueryResult, BoltExecutionError> {
        self.execute_as_on_database_with_context(
            &transaction.database,
            query,
            params,
            request_context,
            principal,
        )
    }
}

/// A no-op query executor that returns empty results for all queries.
/// Used as a placeholder when no real executor is wired.
pub struct NoopExecutor;

impl QueryExecutor for NoopExecutor {
    fn execute(
        &self,
        query: &str,
        _params: &HashMap<String, serde_json::Value>,
    ) -> Result<BoltQueryResult, String> {
        let upper = query.trim().to_ascii_uppercase();
        // Handle common system queries with proper column names so the
        // Neo4j browser doesn't hang waiting for results.
        if upper.starts_with("SHOW DATABASES") || upper.starts_with("SHOW DBS") {
            return Ok(BoltQueryResult {
                columns: vec![
                    "name".into(),
                    "type".into(),
                    "aliases".into(),
                    "access".into(),
                    "address".into(),
                    "role".into(),
                    "writer".into(),
                    "requestedStatus".into(),
                    "currentStatus".into(),
                    "statusMessage".into(),
                    "default".into(),
                    "home".into(),
                    "constituents".into(),
                ],
                rows: vec![vec![
                    serde_json::json!("copperdb"),
                    serde_json::json!("standard"),
                    serde_json::json!([]),
                    serde_json::json!("read-write"),
                    serde_json::json!("localhost:7687"),
                    serde_json::json!("standalone"),
                    serde_json::json!(true),
                    serde_json::json!("online"),
                    serde_json::json!("online"),
                    serde_json::json!(""),
                    serde_json::json!(true),
                    serde_json::json!(true),
                    serde_json::json!([]),
                ]],
                encoded_rows: None,
                stats: BoltResultStats::default(),
                notifications: vec![],
            });
        }
        if upper.starts_with("CALL DBMS.CLUSTER.OVERVIEW") {
            return Ok(BoltQueryResult {
                columns: vec![
                    "id".into(),
                    "addresses".into(),
                    "role".into(),
                    "database".into(),
                    "routing".into(),
                ],
                rows: vec![vec![
                    serde_json::json!("00000000-0000-0000-0000-000000000000"),
                    serde_json::json!(["localhost:7687"]),
                    serde_json::json!("standalone"),
                    serde_json::json!("copperdb"),
                    serde_json::json!(null),
                ]],
                encoded_rows: None,
                stats: BoltResultStats::default(),
                notifications: vec![],
            });
        }
        // For MATCH ... RETURN queries, return empty rows with proper column names
        if upper.starts_with("MATCH ") || upper.starts_with("RETURN ") || upper.starts_with("CALL ")
        {
            return Ok(BoltQueryResult {
                columns: vec!["n".into()],
                rows: vec![],
                encoded_rows: None,
                stats: BoltResultStats::default(),
                notifications: vec![],
            });
        }
        Ok(BoltQueryResult {
            columns: vec![],
            rows: vec![],
            encoded_rows: None,
            stats: BoltResultStats::default(),
            notifications: vec![],
        })
    }
}

#[derive(Debug, Clone)]
pub struct BoltServerConfig {
    pub listen_addr: String,
    pub web_socket_enabled: bool,
    pub auth_enabled: bool,
}

impl Default for BoltServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:7687".into(),
            web_socket_enabled: true,
            auth_enabled: true,
        }
    }
}

impl std::fmt::Debug for BoltServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoltServer")
            .field("listen_addr", &self.listen_addr)
            .field("config", &self.config)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoltRuntimeStatus {
    pub active_connections: u64,
    pub active_sessions: u64,
    pub active_transactions: u64,
    pub failures: u64,
}

#[derive(Default)]
pub struct BoltRuntimeCounters {
    active_connections: AtomicU64,
    active_sessions: AtomicU64,
    active_transactions: AtomicU64,
    failures: AtomicU64,
}

impl BoltRuntimeCounters {
    pub fn snapshot(&self) -> BoltRuntimeStatus {
        BoltRuntimeStatus {
            active_connections: self.active_connections.load(Ordering::Relaxed),
            active_sessions: self.active_sessions.load(Ordering::Relaxed),
            active_transactions: self.active_transactions.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
        }
    }

    fn connection_opened(&self) -> u64 {
        self.active_connections.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn connection_closed(&self) -> u64 {
        self.active_connections.fetch_sub(1, Ordering::SeqCst) - 1
    }

    fn record_failure(&self) {
        self.failures.fetch_add(1, Ordering::Relaxed);
    }

    fn open_session(self: &Arc<Self>) -> ActiveBoltSession {
        self.active_sessions.fetch_add(1, Ordering::SeqCst);
        ActiveBoltSession(Arc::clone(self))
    }

    fn open_transaction(self: &Arc<Self>) -> ActiveBoltTransaction {
        self.active_transactions.fetch_add(1, Ordering::SeqCst);
        ActiveBoltTransaction(Arc::clone(self))
    }
}

struct ActiveBoltSession(Arc<BoltRuntimeCounters>);

impl Drop for ActiveBoltSession {
    fn drop(&mut self) {
        self.0.active_sessions.fetch_sub(1, Ordering::SeqCst);
    }
}

struct ActiveBoltTransaction(Arc<BoltRuntimeCounters>);

impl Drop for ActiveBoltTransaction {
    fn drop(&mut self) {
        self.0.active_transactions.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
pub struct BoltServer {
    pub listen_addr: String,
    config: BoltServerConfig,
    telemetry: Arc<Telemetry>,
    runtime_counters: Arc<BoltRuntimeCounters>,
    executor: Arc<dyn QueryExecutor>,
    auth_provider: Option<Arc<dyn BoltAuthProvider>>,
    language_preferences: Vec<LanguageTag>,
}

impl BoltServer {
    pub fn new(
        listen_addr: impl Into<String>,
        telemetry: Arc<Telemetry>,
        executor: Arc<dyn QueryExecutor>,
    ) -> Self {
        let addr = listen_addr.into();
        Self {
            listen_addr: addr.clone(),
            config: BoltServerConfig {
                listen_addr: addr,
                web_socket_enabled: true,
                auth_enabled: true,
            },
            telemetry,
            runtime_counters: Arc::new(BoltRuntimeCounters::default()),
            executor,
            auth_provider: None,
            language_preferences: Vec::new(),
        }
    }

    pub fn with_auth_enabled(mut self, auth_enabled: bool) -> Self {
        self.config.auth_enabled = auth_enabled;
        self
    }

    pub fn auth_enabled(&self) -> bool {
        self.config.auth_enabled
    }

    pub fn with_auth_provider(mut self, auth_provider: Arc<dyn BoltAuthProvider>) -> Self {
        self.auth_provider = Some(auth_provider);
        self
    }

    pub fn with_runtime_counters(mut self, runtime_counters: Arc<BoltRuntimeCounters>) -> Self {
        self.runtime_counters = runtime_counters;
        self
    }

    pub fn with_language_preferences(mut self, preferences: Vec<LanguageTag>) -> Self {
        self.language_preferences = preferences;
        self
    }

    pub fn runtime_status(&self) -> BoltRuntimeStatus {
        self.runtime_counters.snapshot()
    }

    pub async fn serve(&self) -> Result<(), BoltError> {
        let listener = TcpListener::bind(&self.listen_addr).await?;
        self.serve_listener(listener).await
    }

    /// Serve connections from a pre-bound listener.
    ///
    /// This supports hosts that own socket binding and integration tests that
    /// need an ephemeral port without a bind/connect race.
    pub async fn serve_listener(&self, listener: TcpListener) -> Result<(), BoltError> {
        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let mut peek_buf = [0u8; 4];
            let n = stream.peek(&mut peek_buf).await?;

            if n >= 4 && &peek_buf[..4] == b"GET " {
                self.spawn_ws(stream, peer_addr);
            } else {
                self.spawn_tcp(stream, peer_addr);
            }
        }
    }

    fn spawn_ws(&self, stream: TcpStream, peer_addr: std::net::SocketAddr) {
        if !self.config.web_socket_enabled {
            let body = "WebSocket connections not enabled. Use Bolt TCP.\n";
            let response = format!(
                "HTTP/1.1 426 Upgrade Required\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            tokio::spawn(async move {
                let mut s = stream;
                let _ = s.write_all(response.as_bytes()).await;
            });
            return;
        }

        let started = std::time::Instant::now();
        let telemetry = Arc::clone(&self.telemetry);
        let runtime_counters = Arc::clone(&self.runtime_counters);
        let executor = Arc::clone(&self.executor);
        let auth_enabled = self.config.auth_enabled;
        let auth_provider = self.auth_provider.clone();
        let session_counters = Arc::clone(&self.runtime_counters);
        let language_preferences = self.language_preferences.clone();
        let _ = telemetry.record_counter(
            "nornicdb_bolt_connections_total",
            &[("result", "success"), ("transport", "ws")],
        );
        let active = runtime_counters.connection_opened();
        let _ = telemetry.set_gauge(
            "nornicdb_bolt_connections_active",
            &[("transport", "ws")],
            active as f64,
        );

        tokio::spawn(async move {
            match accept_async(stream).await {
                Ok(mut ws_stream) => {
                    let result = handle_ws_session_with_counters(
                        &mut ws_stream,
                        &telemetry,
                        executor,
                        auth_enabled,
                        auth_provider,
                        session_counters,
                        language_preferences,
                    )
                    .await;
                    if let Err(ref e) = result {
                        runtime_counters.record_failure();
                        warn!(event_id = "bolt.log.message_handling_error", %peer_addr, error = %e, "bolt ws connection failed");
                        let _ = telemetry.record_counter(
                            "nornicdb_bolt_connections_total",
                            &[("result", "error"), ("transport", "ws")],
                        );
                    }
                }
                Err(e) => {
                    runtime_counters.record_failure();
                    warn!(event_id = "bolt.log.websocket_upgrade_failed", %peer_addr, error = %e, "ws upgrade failed");
                }
            }
            let active = runtime_counters.connection_closed();
            let _ = telemetry.set_gauge(
                "nornicdb_bolt_connections_active",
                &[("transport", "ws")],
                active as f64,
            );
            let _ = telemetry.observe_histogram(
                "nornicdb_bolt_session_duration_seconds",
                &[("transport", "ws")],
                started.elapsed().as_secs_f64(),
            );
        });
    }

    fn spawn_tcp(&self, mut stream: TcpStream, peer_addr: std::net::SocketAddr) {
        if let Err(error) = stream.set_nodelay(true) {
            warn!(event_id = "bolt.log.tcp_nodelay_failed", %peer_addr, %error, "failed to disable Nagle buffering for bolt tcp connection");
        }
        let started = std::time::Instant::now();
        let telemetry = Arc::clone(&self.telemetry);
        let runtime_counters = Arc::clone(&self.runtime_counters);
        let executor = Arc::clone(&self.executor);
        let auth_enabled = self.config.auth_enabled;
        let auth_provider = self.auth_provider.clone();
        let session_counters = Arc::clone(&self.runtime_counters);
        let language_preferences = self.language_preferences.clone();
        let _ = telemetry.record_counter(
            "nornicdb_bolt_connections_total",
            &[("result", "success"), ("transport", "tcp")],
        );
        let active = runtime_counters.connection_opened();
        let _ = telemetry.set_gauge(
            "nornicdb_bolt_connections_active",
            &[("transport", "tcp")],
            active as f64,
        );
        debug!(%peer_addr, "accepted bolt tcp");

        tokio::spawn(async move {
            let result = handle_tcp_session_with_counters(
                &mut stream,
                &telemetry,
                executor,
                auth_enabled,
                auth_provider,
                session_counters,
                language_preferences,
            )
            .await;
            if let Err(ref e) = result {
                runtime_counters.record_failure();
                let _ = telemetry.record_counter(
                    "nornicdb_bolt_connections_total",
                    &[("result", "error"), ("transport", "tcp")],
                );
                warn!(event_id = "bolt.log.message_handling_error", %peer_addr, error = %e, "bolt tcp failed");
            }
            let active = runtime_counters.connection_closed();
            let _ = telemetry.set_gauge(
                "nornicdb_bolt_connections_active",
                &[("transport", "tcp")],
                active as f64,
            );
            let _ = telemetry.observe_histogram(
                "nornicdb_bolt_session_duration_seconds",
                &[("transport", "tcp")],
                started.elapsed().as_secs_f64(),
            );
        });
    }
} // end impl BoltServer

/// Handle a Bolt session over raw TCP.
#[cfg(test)]
async fn handle_tcp_session(
    stream: &mut TcpStream,
    telemetry: &Telemetry,
    executor: Arc<dyn QueryExecutor>,
    auth_enabled: bool,
    auth_provider: Option<Arc<dyn BoltAuthProvider>>,
) -> Result<(), BoltError> {
    handle_tcp_session_with_timeout(
        stream,
        telemetry,
        executor,
        auth_enabled,
        auth_provider,
        BOLT_RECEIVE_TIMEOUT,
    )
    .await
}

async fn handle_tcp_session_with_counters(
    stream: &mut TcpStream,
    telemetry: &Telemetry,
    executor: Arc<dyn QueryExecutor>,
    auth_enabled: bool,
    auth_provider: Option<Arc<dyn BoltAuthProvider>>,
    runtime_counters: Arc<BoltRuntimeCounters>,
    language_preferences: Vec<LanguageTag>,
) -> Result<(), BoltError> {
    handle_tcp_session_with_timeout_and_counters(
        stream,
        telemetry,
        executor,
        BOLT_RECEIVE_TIMEOUT,
        BoltSessionOptions {
            auth_enabled,
            auth_provider,
            runtime_counters: Some(runtime_counters),
            language_preferences,
        },
    )
    .await
}

#[cfg(test)]
async fn handle_tcp_session_with_timeout(
    stream: &mut TcpStream,
    telemetry: &Telemetry,
    executor: Arc<dyn QueryExecutor>,
    auth_enabled: bool,
    auth_provider: Option<Arc<dyn BoltAuthProvider>>,
    receive_timeout: Duration,
) -> Result<(), BoltError> {
    handle_tcp_session_with_timeout_and_counters(
        stream,
        telemetry,
        executor,
        receive_timeout,
        BoltSessionOptions {
            auth_enabled,
            auth_provider,
            runtime_counters: None,
            language_preferences: Vec::new(),
        },
    )
    .await
}

struct BoltSessionOptions {
    auth_enabled: bool,
    auth_provider: Option<Arc<dyn BoltAuthProvider>>,
    runtime_counters: Option<Arc<BoltRuntimeCounters>>,
    language_preferences: Vec<LanguageTag>,
}

async fn handle_tcp_session_with_timeout_and_counters(
    stream: &mut TcpStream,
    telemetry: &Telemetry,
    executor: Arc<dyn QueryExecutor>,
    receive_timeout: Duration,
    options: BoltSessionOptions,
) -> Result<(), BoltError> {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    info!(event_id = "bolt.log.hello", %peer, transport = "tcp", "bolt tcp session started");
    let mut preamble = [0u8; 20];
    stream.read_exact(&mut preamble).await?;
    if preamble[..4] != [0x60, 0x60, 0xB0, 0x17] {
        return Err(BoltError::ProtocolViolation(
            "invalid bolt magic preamble".into(),
        ));
    }
    stream.write_all(&[0x00, 0x00, 0x04, 0x04]).await?;
    info!(event_id = "bolt.log.hello", %peer, transport = "tcp", protocol_version = "4.4", "bolt tcp version sent");

    let mut session = BoltSession::new_with_preferences(
        options.auth_enabled,
        options.runtime_counters,
        options.language_preferences,
    );
    let mut decoder = wsconn::BoltChunkDecoder::new();
    let mut temp_buf = [0u8; 4096];
    let mut pending_frames = VecDeque::new();

    let result = 'session_loop: loop {
        if pending_frames.is_empty() {
            let bytes_read = match tokio::time::timeout(receive_timeout, stream.read(&mut temp_buf))
                .await
            {
                Ok(Ok(bytes_read)) => bytes_read,
                Ok(Err(error)) => break Err(error.into()),
                Err(_) => break Err(BoltError::ProtocolViolation("Bolt receive timeout".into())),
            };
            if bytes_read == 0 {
                break Ok(());
            }
            pending_frames.extend(decoder.push(&temp_buf[..bytes_read]));
        }

        let mut framed_responses = Vec::new();
        let mut response_count = 0usize;
        while let Some(frame) = pending_frames.pop_front() {
            let processing = process_frame(
                &frame,
                &mut session,
                telemetry,
                Arc::clone(&executor),
                options.auth_provider.clone(),
            );
            tokio::pin!(processing);
            let mut interrupted = false;
            let responses = loop {
                tokio::select! {
                    result = &mut processing => break result,
                    read = stream.read(&mut temp_buf) => {
                        let bytes_read = match read {
                            Ok(bytes_read) => bytes_read,
                            Err(error) => break 'session_loop Err(error.into()),
                        };
                        if bytes_read == 0 {
                            break 'session_loop Ok(());
                        }
                        let frames = decoder.push(&temp_buf[..bytes_read]);
                        if let Some(reset_index) = frames.iter().position(|frame| {
                            decoded_message_signature(frame) == Some(0x0F)
                        }) {
                            pending_frames.clear();
                            pending_frames.extend(frames.into_iter().skip(reset_index));
                            interrupted = true;
                            break Ok(Vec::new());
                        }
                        pending_frames.extend(frames);
                    }
                }
            }?;
            if interrupted {
                continue;
            }
            if !responses.is_empty() {
                response_count += responses.len();
                framed_responses.reserve(
                    responses
                        .iter()
                        .map(|response| response.len().saturating_add(6))
                        .sum(),
                );
                for response in responses {
                    wsconn::encode_bolt_chunks_into(&mut framed_responses, &response);
                }
            }
        }
        if !framed_responses.is_empty() {
            info!(
                event_id = "bolt.log.run",
                response_count,
                response_bytes = framed_responses.len(),
                transport = "tcp",
                "bolt sending responses"
            );
            stream.write_all(&framed_responses).await?;
        }
    };
    rollback_active_transaction(&mut session, executor.as_ref());
    result
}

async fn handle_ws_session_with_counters<S>(
    ws: &mut tokio_tungstenite::WebSocketStream<S>,
    telemetry: &Telemetry,
    executor: Arc<dyn QueryExecutor>,
    auth_enabled: bool,
    auth_provider: Option<Arc<dyn BoltAuthProvider>>,
    runtime_counters: Arc<BoltRuntimeCounters>,
    language_preferences: Vec<LanguageTag>,
) -> Result<(), BoltError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    handle_ws_session_with_timeout_and_counters(
        ws,
        telemetry,
        executor,
        BOLT_RECEIVE_TIMEOUT,
        BoltSessionOptions {
            auth_enabled,
            auth_provider,
            runtime_counters: Some(runtime_counters),
            language_preferences,
        },
    )
    .await
}

async fn handle_ws_session_with_timeout_and_counters<S>(
    ws: &mut tokio_tungstenite::WebSocketStream<S>,
    telemetry: &Telemetry,
    executor: Arc<dyn QueryExecutor>,
    receive_timeout: Duration,
    options: BoltSessionOptions,
) -> Result<(), BoltError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    // The first WS binary message after upgrade is the Bolt preamble
    // (magic 0x60 0x60 0xB0 0x17 + four 4-byte version proposals = 20 bytes).
    // This is sent as a raw WS binary frame — NOT chunk-encoded.
    // We must read it directly before entering chunk-encoded message loop.
    let preamble = {
        use futures::StreamExt;
        match ws.next().await {
            Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(data))) => data.to_vec(),
            Some(Ok(other)) => {
                return Err(BoltError::ProtocolViolation(format!(
                    "expected Binary preamble, got {other:?}"
                )));
            }
            Some(Err(e)) => {
                return Err(BoltError::ProtocolViolation(format!(
                    "WS preamble read error: {e}"
                )));
            }
            None => return Ok(()),
        }
    };
    if preamble.len() < 4 || preamble[..4] != [0x60, 0x60, 0xB0, 0x17] {
        return Err(BoltError::ProtocolViolation(
            "invalid bolt magic preamble on WS".into(),
        ));
    }
    // Respond with Bolt 4.4 (raw — handshake, not chunked)
    info!(
        event_id = "bolt.log.hello",
        transport = "websocket",
        protocol_version = "4.4",
        "bolt WS preamble OK"
    );
    wsconn::write_ws_raw(ws, &[0x00, 0x00, 0x04, 0x04])
        .await
        .map_err(|e| BoltError::ProtocolViolation(format!("WS version response error: {e}")))?;
    info!(
        event_id = "bolt.log.hello",
        transport = "websocket",
        protocol_version = "4.4",
        "bolt WS version response sent"
    );

    let mut session = BoltSession::new_with_preferences(
        options.auth_enabled,
        options.runtime_counters,
        options.language_preferences,
    );
    let mut decoder = wsconn::BoltChunkDecoder::new();
    let mut pending_frames = VecDeque::new();

    let result = 'session_loop: loop {
        if pending_frames.is_empty() {
            match tokio::time::timeout(receive_timeout, wsconn::read_ws_message(ws, &mut decoder))
                .await
            {
                Ok(Some(Ok(frames))) => pending_frames.extend(frames),
                Ok(Some(Err(error))) => {
                    break Err(BoltError::ProtocolViolation(format!(
                        "WS read error: {error}"
                    )));
                }
                Ok(None) => break Ok(()),
                Err(_) => break Err(BoltError::ProtocolViolation("Bolt receive timeout".into())),
            }
        }

        while let Some(frame) = pending_frames.pop_front() {
            let processing = process_frame(
                &frame,
                &mut session,
                telemetry,
                Arc::clone(&executor),
                options.auth_provider.clone(),
            );
            tokio::pin!(processing);
            let mut interrupted = false;
            let responses = loop {
                tokio::select! {
                    result = &mut processing => break result,
                    read = wsconn::read_ws_message(ws, &mut decoder) => {
                        match read {
                            Some(Ok(frames)) => {
                                if let Some(reset_index) = frames.iter().position(|frame| {
                                    decoded_message_signature(frame) == Some(0x0F)
                                }) {
                                    pending_frames.clear();
                                    pending_frames.extend(frames.into_iter().skip(reset_index));
                                    interrupted = true;
                                    break Ok(Vec::new());
                                }
                                pending_frames.extend(frames);
                            }
                            Some(Err(error)) => {
                                break 'session_loop Err(BoltError::ProtocolViolation(format!(
                                    "WS read error: {error}"
                                )))
                            }
                            None => break 'session_loop Ok(()),
                        }
                    }
                }
            }?;
            if interrupted {
                continue;
            }
            for response_bytes in responses {
                info!(
                    event_id = "bolt.log.run",
                    response_bytes = response_bytes.len(),
                    transport = "websocket",
                    "bolt WS sending response"
                );
                if let Err(error) = wsconn::write_ws_message(ws, &response_bytes).await {
                    break 'session_loop Err(BoltError::ProtocolViolation(error.to_string()));
                }
            }
        }
    };
    rollback_active_transaction(&mut session, executor.as_ref());
    result
}

fn decoded_message_signature(frame: &[u8]) -> Option<u8> {
    match crate::packstream::decode(frame).ok()?.0 {
        Value::Struct { signature, .. } => Some(signature),
        _ => None,
    }
}

struct BoltCancellationGuard<'a> {
    request_guard: Option<copperdb_util::RequestContextGuard>,
    request_context: copperdb_util::RequestContext,
    telemetry: Option<&'a Telemetry>,
    finished: bool,
}

impl BoltCancellationGuard<'_> {
    fn finish(&mut self) {
        self.finished = true;
    }
}

impl Drop for BoltCancellationGuard<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        drop(self.request_guard.take());
        if let (Some(telemetry), Some(reason)) =
            (self.telemetry, self.request_context.cancellation_reason())
        {
            let _ = telemetry.record_request_cancellation(
                CancellationProtocol::Bolt,
                CancellationStage::Ingress,
                reason,
            );
        }
    }
}

async fn process_frame(
    frame: &[u8],
    session: &mut BoltSession,
    telemetry: &Telemetry,
    executor: Arc<dyn QueryExecutor>,
    auth_provider: Option<Arc<dyn BoltAuthProvider>>,
) -> Result<Vec<Vec<u8>>, BoltError> {
    let (value, consumed) = crate::packstream::decode(frame)?;
    if consumed != frame.len() {
        return Err(BoltError::ProtocolViolation(format!(
            "trailing bytes after Bolt message: {}",
            frame.len() - consumed
        )));
    }
    match process_message_with_telemetry(
        &value,
        session,
        Arc::clone(&executor),
        auth_provider,
        Some(telemetry),
    )
    .await
    {
        Ok(responses) => {
            let _ = telemetry.record_counter(
                "nornicdb_bolt_messages_total",
                &[("op", "run"), ("result", "success")],
            );
            Ok(responses)
        }
        Err(e) => {
            let failure = BoltMessage::Failure {
                metadata: HashMap::from([
                    (
                        "code".into(),
                        serde_json::json!("Neo.TransientError.General.UnknownError"),
                    ),
                    (
                        "message".into(),
                        serde_json::json!(localize_display(session, &e)),
                    ),
                ]),
            };
            let response = dispatch::encode_message(&failure);
            let _ = telemetry.record_counter(
                "nornicdb_bolt_messages_total",
                &[("op", "run"), ("result", "error")],
            );
            Ok(vec![response])
        }
    }
}

/// Per-connection Bolt session state. Mirrors NornicDB's Session struct.
struct BoltSession {
    auth_enabled: bool,
    authenticated: bool,
    principal: Option<BoltPrincipal>,
    database: Option<String>,
    last_query_database: Option<String>,
    current_query: Option<String>,
    cursors: HashMap<i64, BoltCursor>,
    last_qid: Option<i64>,
    next_qid: i64,
    last_bookmark: Option<String>,
    transaction: Option<BoltTransaction>,
    _session_counter: Option<ActiveBoltSession>,
    transaction_counter: Option<ActiveBoltTransaction>,
    runtime_counters: Option<Arc<BoltRuntimeCounters>>,
    language: Option<LanguageTag>,
    localizer: Manager,
}

struct BoltCursor {
    result: BoltQueryResult,
    index: usize,
    database: Option<String>,
}

impl BoltSession {
    #[cfg(test)]
    fn new(auth_enabled: bool) -> Self {
        Self::new_with_counters(auth_enabled, None)
    }

    #[cfg(test)]
    fn new_with_counters(
        auth_enabled: bool,
        runtime_counters: Option<Arc<BoltRuntimeCounters>>,
    ) -> Self {
        Self::new_with_preferences(auth_enabled, runtime_counters, Vec::new())
    }

    fn new_with_preferences(
        auth_enabled: bool,
        runtime_counters: Option<Arc<BoltRuntimeCounters>>,
        language_preferences: Vec<LanguageTag>,
    ) -> Self {
        let session_counter = runtime_counters
            .as_ref()
            .map(BoltRuntimeCounters::open_session);
        Self {
            auth_enabled,
            authenticated: !auth_enabled,
            principal: None,
            database: None,
            last_query_database: None,
            current_query: None,
            cursors: HashMap::new(),
            last_qid: None,
            next_qid: 0,
            last_bookmark: None,
            transaction: None,
            _session_counter: session_counter,
            transaction_counter: None,
            runtime_counters,
            language: language_preferences.first().cloned(),
            localizer: Manager::new(&language_preferences),
        }
    }

    fn set_transaction(&mut self, transaction: BoltTransaction) {
        self.transaction_counter = self
            .runtime_counters
            .as_ref()
            .map(BoltRuntimeCounters::open_transaction);
        self.transaction = Some(transaction);
    }

    fn take_transaction(&mut self) -> Option<BoltTransaction> {
        self.transaction_counter.take();
        self.transaction.take()
    }
}

fn locale_from_metadata(metadata: &HashMap<String, serde_json::Value>) -> Option<LanguageTag> {
    metadata
        .get("locale")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| LanguageTag::parse(value).ok().flatten())
}

fn localize(session: &BoltSession, message: &copperdb_localization::Message) -> String {
    let preferences = session.language.as_slice();
    session
        .localizer
        .render(preferences, message)
        .map(|rendered| rendered.text)
        .unwrap_or_else(|_| message.fallback.to_string())
}

fn localize_id(session: &BoltSession, id: &'static str) -> String {
    copperdb_localization::Message::from_catalog(id)
        .map(|message| localize(session, &message))
        .unwrap_or_else(|| id.to_string())
}

fn localize_display(session: &BoltSession, display: &dyn std::fmt::Display) -> String {
    session
        .localizer
        .render_display(session.language.as_slice(), display)
        .map(|rendered| rendered.text)
        .unwrap_or_else(|| display.to_string())
}

fn localized_authentication_error(session: &BoltSession, error: &str) -> String {
    let id = if error.eq_ignore_ascii_case("Bolt authentication is unavailable") {
        "bolt.authentication_not_configured"
    } else if error.to_ascii_lowercase().contains("token") {
        "bolt.invalid_or_expired_token"
    } else {
        "bolt.invalid_credentials"
    };
    localize_id(session, id)
}

fn database_from_metadata(metadata: &HashMap<String, serde_json::Value>) -> Option<String> {
    metadata
        .get("db")
        .or_else(|| metadata.get("database"))
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn bolt_statement_timeout(extra: &HashMap<String, serde_json::Value>) -> Option<Duration> {
    extra
        .get("tx_timeout")
        .and_then(serde_json::Value::as_u64)
        .filter(|millis| *millis > 0)
        .map(Duration::from_millis)
        .or_else(|| {
            [
                BOLT_STATEMENT_TIMEOUT_ENV,
                UPSTREAM_BOLT_STATEMENT_TIMEOUT_ENV,
            ]
            .into_iter()
            .find_map(|name| {
                std::env::var(name)
                    .ok()
                    .and_then(|value| copperdb_envutil::parse_duration(value.trim()))
                    .filter(|duration| !duration.is_zero())
            })
        })
}

fn bookmarks_from_metadata(metadata: &HashMap<String, serde_json::Value>) -> Vec<String> {
    metadata
        .get("bookmarks")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

fn credentials_from_hello(
    metadata: &HashMap<String, serde_json::Value>,
) -> Option<(String, String)> {
    let username = metadata
        .get("principal")
        .or_else(|| metadata.get("username"))?
        .as_str()?;
    let password = metadata
        .get("credentials")
        .or_else(|| metadata.get("password"))?
        .as_str()?;
    Some((username.to_owned(), password.to_owned()))
}

fn authenticate_session(
    session: &mut BoltSession,
    auth_provider: Option<&dyn BoltAuthProvider>,
    username: &str,
    password: &str,
) -> Result<(), String> {
    let auth_provider =
        auth_provider.ok_or_else(|| "Bolt authentication is unavailable".to_owned())?;
    let principal = auth_provider.authenticate(username, password)?;
    session.principal = Some(principal);
    session.authenticated = true;
    Ok(())
}

fn success_response() -> Vec<Vec<u8>> {
    vec![dispatch::encode_message(&BoltMessage::Success {
        metadata: HashMap::from([("server".into(), serde_json::json!("copperdb/1.0"))]),
    })]
}

fn authentication_failure_response(message: &str) -> Vec<Vec<u8>> {
    vec![dispatch::encode_message(&BoltMessage::Failure {
        metadata: HashMap::from([
            (
                "code".into(),
                serde_json::json!("Neo.ClientError.Security.Unauthorized"),
            ),
            ("message".into(), serde_json::json!(message)),
        ]),
    })]
}

fn client_failure_response(code: &str, message: &str) -> Vec<Vec<u8>> {
    vec![dispatch::encode_message(&BoltMessage::Failure {
        metadata: HashMap::from([
            ("code".into(), serde_json::json!(code)),
            ("message".into(), serde_json::json!(message)),
        ]),
    })]
}

fn authentication_required(session: &BoltSession) -> bool {
    session.auth_enabled && !session.authenticated
}

fn rollback_active_transaction(session: &mut BoltSession, executor: &dyn QueryExecutor) {
    if let Some(transaction) = session.take_transaction()
        && let Err(error) = executor.rollback_transaction(&transaction)
    {
        warn!(event_id = "bolt.log.transaction_cleanup_failed", error = %error, transaction_id = %transaction.id, database = %transaction.database, "Bolt session cleanup rollback failed");
    }
}

fn cursor_qid(session: &BoltSession, qid: i64) -> Option<i64> {
    if qid == -1 {
        session.last_qid
    } else {
        Some(qid)
    }
}

fn cursor_limit(n: i64, remaining: usize) -> usize {
    if n < 0 {
        remaining
    } else {
        usize::try_from(n).unwrap_or(usize::MAX).min(remaining)
    }
}

fn cursor_summary(
    session: &BoltSession,
    database: Option<&str>,
    has_more: bool,
    stats: &BoltResultStats,
    notifications: &[BoltNotification],
) -> Vec<Vec<u8>> {
    let mut metadata = HashMap::from([
        ("type".into(), serde_json::json!("r")),
        ("t_last".into(), serde_json::json!(0)),
        (
            "db".into(),
            serde_json::json!(
                database
                    .or(session.last_query_database.as_deref())
                    .or(session.database.as_deref())
                    .unwrap_or("copperdb")
            ),
        ),
    ]);
    if has_more {
        metadata.insert("has_more".into(), serde_json::json!(true));
    }
    if let Some(bookmark) = &session.last_bookmark {
        metadata.insert("bookmark".into(), serde_json::json!(bookmark));
    }
    if stats.nodes_created > 0
        || stats.nodes_deleted > 0
        || stats.relationships_created > 0
        || stats.relationships_deleted > 0
        || stats.properties_set > 0
    {
        metadata.insert(
            "stats".into(),
            serde_json::json!({
                "nodes-created": stats.nodes_created,
                "nodes-deleted": stats.nodes_deleted,
                "relationships-created": stats.relationships_created,
                "relationships-deleted": stats.relationships_deleted,
                "properties-set": stats.properties_set,
            }),
        );
    }
    if !notifications.is_empty() {
        metadata.insert(
            "notifications".into(),
            serde_json::json!(
                notifications
                    .iter()
                    .map(|notification| serde_json::json!({
                        "code": notification.code,
                        "title": notification.title,
                        "description": notification.description,
                        "severity": notification.severity,
                        "category": notification.category,
                    }))
                    .collect::<Vec<_>>()
            ),
        );
    }
    vec![dispatch::encode_message(&BoltMessage::Success { metadata })]
}

fn transaction_failure_response(code: &str, message: &str) -> Vec<Vec<u8>> {
    client_failure_response(code, message)
}

#[cfg(test)]
async fn process_message(
    value: &Value,
    session: &mut BoltSession,
    executor: Arc<dyn QueryExecutor>,
    auth_provider: Option<Arc<dyn BoltAuthProvider>>,
) -> Result<Vec<Vec<u8>>, BoltError> {
    process_message_with_telemetry(value, session, executor, auth_provider, None).await
}

async fn process_message_with_telemetry(
    value: &Value,
    session: &mut BoltSession,
    executor: Arc<dyn QueryExecutor>,
    auth_provider: Option<Arc<dyn BoltAuthProvider>>,
    telemetry: Option<&Telemetry>,
) -> Result<Vec<Vec<u8>>, BoltError> {
    let (sig, fields) = match value {
        Value::Struct { signature, fields } => (*signature, fields.as_slice()),
        _ => {
            return Err(BoltError::ProtocolViolation(
                "expected struct message".into(),
            ));
        }
    };
    let msg = dispatch::decode_message(sig, fields)?;
    info!(
        event_id = "bolt.log.run",
        signature = format!("0x{sig:02X}"),
        "bolt message received"
    );
    match msg {
        BoltMessage::Hello { extra } => {
            session.authenticated = !session.auth_enabled;
            session.principal = None;
            session.database = database_from_metadata(&extra);
            session.language = locale_from_metadata(&extra).or_else(|| session.language.clone());
            if session.auth_enabled
                && let Some((username, password)) = credentials_from_hello(&extra)
            {
                return Ok(
                    match authenticate_session(
                        session,
                        auth_provider.as_deref(),
                        &username,
                        &password,
                    ) {
                        Ok(()) => success_response(),
                        Err(message) => authentication_failure_response(
                            &localized_authentication_error(session, &message),
                        ),
                    },
                );
            }
            let meta = HashMap::from([
                ("server".into(), serde_json::json!("copperdb/1.0")),
                ("connection_id".into(), serde_json::json!("copperdb-1")),
                ("hints".into(), serde_json::json!({})),
                ("patch_bolt".into(), serde_json::json!(["utc"])),
            ]);
            info!(
                event_id = "bolt.log.hello",
                authenticated = session.authenticated,
                "bolt HELLO succeeded"
            );
            Ok(vec![dispatch::encode_message(&BoltMessage::Success {
                metadata: meta,
            })])
        }
        BoltMessage::Logon { auth } => {
            if !session.auth_enabled {
                return Ok(success_response());
            }
            let username = auth
                .get("principal")
                .or_else(|| auth.get("username"))
                .map(String::as_str);
            let password = auth
                .get("credentials")
                .or_else(|| auth.get("password"))
                .map(String::as_str);
            match (username, password) {
                (Some(username), Some(password)) => {
                    match authenticate_session(
                        session,
                        auth_provider.as_deref(),
                        username,
                        password,
                    ) {
                        Ok(()) => Ok(success_response()),
                        Err(message) => Ok(authentication_failure_response(
                            &localized_authentication_error(session, &message),
                        )),
                    }
                }
                _ => Ok(authentication_failure_response(&localize_id(
                    session,
                    "bolt.invalid_credentials",
                ))),
            }
        }
        BoltMessage::Logoff => {
            if let Some(transaction) = session.take_transaction() {
                let _ = executor.rollback_transaction(&transaction);
            }
            session.authenticated = !session.auth_enabled;
            session.principal = None;
            Ok(vec![dispatch::encode_message(&BoltMessage::Success {
                metadata: HashMap::new(),
            })])
        }
        BoltMessage::Run {
            query,
            parameters,
            extra,
        } => {
            if authentication_required(session) {
                return Ok(authentication_failure_response(&localize(
                    session,
                    &localized_messages::not_authenticated(),
                )));
            }
            session.language = locale_from_metadata(&extra).or_else(|| session.language.clone());
            session.current_query = Some(query.clone());
            let requested_database = database_from_metadata(&extra);
            if let (Some(transaction), Some(requested_database)) =
                (session.transaction.as_ref(), requested_database.as_ref())
                && requested_database != &transaction.database
            {
                return Ok(transaction_failure_response(
                    "Neo.ClientError.Transaction.TransactionAccessedConcurrently",
                    &localize_id(session, "bolt.database_switch_during_transaction"),
                ));
            }
            let database = session
                .transaction
                .as_ref()
                .map(|transaction| transaction.database.clone())
                .or(requested_database)
                .or_else(|| session.database.clone());
            let execution_database = database.clone();
            let execution_query = query.clone();
            let execution_parameters = parameters.clone();
            let principal = session.principal.clone();
            let transaction = session.transaction.clone();
            let bookmarks = bookmarks_from_metadata(&extra);
            let execution_executor = Arc::clone(&executor);
            let statement_timeout = bolt_statement_timeout(&extra);
            let run_span = tracing::info_span!("nornicdb.bolt.run", db.system = "neo4j");
            let parent = global::get_text_map_propagator(|propagator| {
                propagator.extract(&BoltTraceContext::new(&extra))
            });
            let _ = run_span.set_parent(parent);

            // Create request context OUTSIDE spawn_blocking so the guard
            // lives in the async scope.  When the client disconnects, the
            // guard is dropped → cancel fires → the BFS (or any long-running
            // query) observes RequestCancelled and aborts.
            let (request_context, request_guard) = copperdb_util::RequestContext::root(None);
            let request_context = request_context.with_language_preferences(
                session
                    .language
                    .iter()
                    .map(|language| language.as_str().to_string()),
            );
            let mut cancellation_guard = BoltCancellationGuard {
                request_guard: Some(request_guard),
                request_context: request_context.clone(),
                telemetry,
                finished: false,
            };
            let execution_context = request_context.clone();

            let execute = move || {
                run_span.in_scope(|| match transaction.as_ref() {
                    Some(transaction) => execution_executor.execute_in_transaction_with_context(
                        transaction,
                        &execution_query,
                        &execution_parameters,
                        execution_context,
                        principal.as_ref(),
                    ),
                    None => execution_executor.execute_as_on_database_with_context_and_bookmarks(
                        execution_database.as_deref().unwrap_or("copperdb"),
                        &execution_query,
                        &execution_parameters,
                        execution_context,
                        principal.as_ref(),
                        &bookmarks,
                    ),
                })
            };
            let execution_result = match (
                statement_timeout,
                tokio::runtime::Handle::current().runtime_flavor(),
            ) {
                (None, tokio::runtime::RuntimeFlavor::MultiThread) => {
                    tokio::task::block_in_place(execute)
                }
                (timeout, _) => {
                    let execution_task = tokio::task::spawn_blocking(execute);
                    match timeout {
                        Some(timeout) => {
                            match tokio::time::timeout(timeout, execution_task).await {
                                Ok(Ok(result)) => result,
                                Ok(Err(error)) => {
                                    cancellation_guard.finish();
                                    return Err(BoltError::ProtocolViolation(format!(
                                        "bolt executor task failed: {error}"
                                    )));
                                }
                                Err(_) => {
                                    request_context.cancel_due_to_deadline();
                                    Err(BoltExecutionError::RequestCancelled(
                                        copperdb_util::RequestCancelled,
                                    ))
                                }
                            }
                        }
                        None => match execution_task.await {
                            Ok(result) => result,
                            Err(error) => {
                                cancellation_guard.finish();
                                return Err(BoltError::ProtocolViolation(format!(
                                    "bolt executor task failed: {error}"
                                )));
                            }
                        },
                    }
                }
            };

            match execution_result {
                Ok(result) => {
                    cancellation_guard.finish();
                    let columns = result.columns.clone();
                    if session.cursors.len() == MAX_BOLT_CURSORS
                        && let Some(oldest_qid) = session.cursors.keys().min().copied()
                    {
                        session.cursors.remove(&oldest_qid);
                    }
                    let qid = session.next_qid;
                    session.next_qid = session.next_qid.wrapping_add(1);
                    session.cursors.insert(
                        qid,
                        BoltCursor {
                            result,
                            index: 0,
                            database: database.clone(),
                        },
                    );
                    session.last_qid = Some(qid);
                    session.last_query_database = database;
                    let fields_json: Vec<serde_json::Value> = columns
                        .iter()
                        .map(|c| serde_json::Value::String(c.clone()))
                        .collect();
                    info!(event_id = "bolt.log.query", %query, fields = ?columns, "bolt RUN executed");
                    let mut metadata = HashMap::from([
                        ("fields".into(), serde_json::json!(fields_json)),
                        ("t_first".into(), serde_json::json!(0)),
                    ]);
                    if session.transaction.is_some() {
                        metadata.insert("qid".into(), serde_json::json!(qid));
                    }
                    Ok(vec![dispatch::encode_message(&BoltMessage::Success {
                        metadata,
                    })])
                }
                Err(e) => {
                    if matches!(e, BoltExecutionError::RequestCancelled(_)) {
                        if request_context.cancellation_reason().is_none() {
                            request_context.cancel();
                        }
                        if let (Some(telemetry), Some(reason)) =
                            (telemetry, request_context.cancellation_reason())
                        {
                            let _ = telemetry.record_request_cancellation(
                                CancellationProtocol::Bolt,
                                CancellationStage::Execution,
                                reason,
                            );
                        }
                    }
                    cancellation_guard.finish();
                    warn!(event_id = "bolt.log.query_error", %query, error = %e, code = e.neo4j_code(), "bolt RUN failed");
                    rollback_active_transaction(session, executor.as_ref());
                    Ok(vec![dispatch::encode_message(&BoltMessage::Failure {
                        metadata: HashMap::from([
                            ("code".into(), serde_json::json!(e.neo4j_code())),
                            (
                                "message".into(),
                                serde_json::json!(localize_display(session, &e)),
                            ),
                        ]),
                    })])
                }
            }
        }
        BoltMessage::Pull { n, qid } | BoltMessage::Discard { n, qid } => {
            let Some(qid) = cursor_qid(session, qid) else {
                return Ok(client_failure_response(
                    "Neo.ClientError.Request.Invalid",
                    &localize_id(session, "bolt.no_active_cursor"),
                ));
            };
            let pull = matches!(msg, BoltMessage::Pull { .. });
            let Some(cursor) = session.cursors.get_mut(&qid) else {
                return Ok(client_failure_response(
                    "Neo.ClientError.Request.Invalid",
                    &localize_id(session, "bolt.unknown_cursor"),
                ));
            };
            let row_count = cursor.result.row_count();
            let end = cursor.index + cursor_limit(n, row_count - cursor.index);
            let mut responses = if pull {
                match cursor.result.encoded_rows.as_ref() {
                    Some(rows) => rows[cursor.index..end].to_vec(),
                    None => cursor.result.rows[cursor.index..end]
                        .iter()
                        .map(|row| dispatch::encode_record(row))
                        .collect(),
                }
            } else {
                Vec::new()
            };
            cursor.index = end;
            let has_more = cursor.index < row_count;
            let database = cursor.database.clone();
            let stats = cursor.result.stats.clone();
            let notifications = cursor.result.notifications.clone();
            if !has_more {
                session.cursors.remove(&qid);
                if session.last_qid == Some(qid) {
                    session.last_qid = session.cursors.keys().max().copied();
                }
            }
            responses.extend(cursor_summary(
                session,
                database.as_deref(),
                has_more,
                &stats,
                &notifications,
            ));
            Ok(responses)
        }
        BoltMessage::Begin { extra } => {
            if authentication_required(session) {
                return Ok(authentication_failure_response(&localize(
                    session,
                    &localized_messages::not_authenticated(),
                )));
            }
            session.language = locale_from_metadata(&extra).or_else(|| session.language.clone());
            if session.transaction.is_some() {
                return Ok(transaction_failure_response(
                    "Neo.ClientError.Transaction.TransactionStartFailed",
                    &localize_id(session, "bolt.transaction_already_active"),
                ));
            }
            let database = database_from_metadata(&extra)
                .or_else(|| session.database.clone())
                .unwrap_or_else(|| "copperdb".into());
            match executor.begin_transaction(&database, &extra, session.principal.as_ref()) {
                Ok(transaction) => {
                    session.database = Some(transaction.database.clone());
                    session.set_transaction(transaction);
                    Ok(vec![dispatch::encode_message(&BoltMessage::Success {
                        metadata: HashMap::new(),
                    })])
                }
                Err(message) => Ok(transaction_failure_response(
                    "Neo.ClientError.Transaction.TransactionStartFailed",
                    &localize_display(session, &message),
                )),
            }
        }
        BoltMessage::Commit => {
            if authentication_required(session) {
                return Ok(authentication_failure_response(&localize(
                    session,
                    &localized_messages::not_authenticated(),
                )));
            }
            let Some(transaction) = session.take_transaction() else {
                return Ok(transaction_failure_response(
                    "Neo.ClientError.Transaction.TransactionNotFound",
                    &localize(
                        session,
                        &localized_messages::bolt_no_transaction_to_commit(),
                    ),
                ));
            };
            match executor.commit_transaction(&transaction) {
                Ok(bookmark) => {
                    session.last_bookmark = Some(bookmark.clone());
                    Ok(vec![dispatch::encode_message(&BoltMessage::Success {
                        metadata: HashMap::from([("bookmark".into(), serde_json::json!(bookmark))]),
                    })])
                }
                Err(error) => Ok(transaction_failure_response(
                    error.neo4j_code("Neo.ClientError.Transaction.TransactionCommitFailed"),
                    &localize_display(session, &error),
                )),
            }
        }
        BoltMessage::Rollback => {
            if authentication_required(session) {
                return Ok(authentication_failure_response(&localize(
                    session,
                    &localized_messages::bolt_authentication_required(),
                )));
            }
            let Some(transaction) = session.take_transaction() else {
                return Ok(vec![dispatch::encode_message(&BoltMessage::Success {
                    metadata: HashMap::new(),
                })]);
            };
            match executor.rollback_transaction(&transaction) {
                Ok(()) => Ok(vec![dispatch::encode_message(&BoltMessage::Success {
                    metadata: HashMap::new(),
                })]),
                Err(message) => Ok(transaction_failure_response(
                    "Neo.ClientError.Transaction.TransactionRollbackFailed",
                    &localize_display(session, &message),
                )),
            }
        }
        BoltMessage::Reset => {
            if let Some(transaction) = session.take_transaction() {
                let _ = executor.rollback_transaction(&transaction);
            }
            session.current_query = None;
            session.cursors.clear();
            session.last_qid = None;
            Ok(vec![dispatch::encode_message(&BoltMessage::Success {
                metadata: HashMap::new(),
            })])
        }
        BoltMessage::Route { .. } => {
            if authentication_required(session) {
                return Ok(authentication_failure_response(&localize(
                    session,
                    &localized_messages::bolt_authentication_required(),
                )));
            }
            Ok(vec![dispatch::encode_message(&BoltMessage::Success {
                metadata: HashMap::new(),
            })])
        }
        _ => Ok(vec![]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use neo4rs::{ConfigBuilder, Graph, query};
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    #[test]
    fn bolt_trace_context_allows_only_w3c_parent_fields() {
        let metadata = HashMap::from([
            (
                "nornicdb.traceparent".into(),
                serde_json::Value::String(
                    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into(),
                ),
            ),
            (
                "nornicdb.tracestate".into(),
                serde_json::Value::String("vendor=value".into()),
            ),
            (
                "authorization".into(),
                serde_json::Value::String("secret".into()),
            ),
        ]);
        let extractor = BoltTraceContext::new(&metadata);

        assert_eq!(
            extractor.get("traceparent"),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        );
        assert_eq!(extractor.get("tracestate"), Some("vendor=value"));
        assert_eq!(extractor.get("authorization"), None);
        assert_eq!(extractor.keys(), vec!["traceparent", "tracestate"]);
    }

    /// Helper: encode a Bolt struct message into PackStream bytes.
    fn encode_bolt_struct(signature: u8, fields: &[Value]) -> Vec<u8> {
        use bytes::BytesMut;
        let mut buf = BytesMut::new();
        crate::packstream::encode_struct_header(&mut buf, fields.len(), signature);
        for field in fields {
            encode_value(&mut buf, field);
        }
        buf.to_vec()
    }

    fn encode_value(buf: &mut bytes::BytesMut, value: &Value) {
        match value {
            Value::Null => crate::packstream::encode_null(buf),
            Value::Bool(b) => crate::packstream::encode_bool(buf, *b),
            Value::Integer(n) => crate::packstream::encode_int(buf, *n),
            Value::String(s) => crate::packstream::encode_string(buf, s),
            Value::Map(pairs) => {
                crate::packstream::encode_map_header(buf, pairs.len());
                for (k, v) in pairs {
                    crate::packstream::encode_string(buf, k);
                    encode_value(buf, v);
                }
            }
            Value::List(items) => {
                crate::packstream::encode_list_header(buf, items.len());
                for item in items {
                    encode_value(buf, item);
                }
            }
            Value::Struct { signature, fields } => {
                crate::packstream::encode_struct_header(buf, fields.len(), *signature);
                for field in fields {
                    encode_value(buf, field);
                }
            }
            _ => crate::packstream::encode_null(buf),
        }
    }

    fn run_message() -> Value {
        Value::Struct {
            signature: 0x10,
            fields: vec![
                Value::String("RETURN 1".into()),
                Value::Map(vec![]),
                Value::Map(vec![]),
            ],
        }
    }

    struct TestAuthProvider;

    impl BoltAuthProvider for TestAuthProvider {
        fn authenticate(&self, username: &str, password: &str) -> Result<BoltPrincipal, String> {
            if username == "reader" && password == "correct-password" {
                Ok(BoltPrincipal {
                    username: username.into(),
                    roles: vec!["reader".into()],
                })
            } else {
                Err("invalid credentials".into())
            }
        }
    }

    struct PrincipalRecordingExecutor {
        principal: Arc<Mutex<Option<BoltPrincipal>>>,
    }

    struct MultiRowExecutor;

    struct MutationStatsExecutor;

    struct NotificationExecutor;

    struct BookmarkRecordingExecutor {
        bookmarks: Arc<Mutex<Vec<String>>>,
    }

    impl QueryExecutor for BookmarkRecordingExecutor {
        fn execute(
            &self,
            _query: &str,
            _params: &HashMap<String, serde_json::Value>,
        ) -> Result<BoltQueryResult, String> {
            Ok(BoltQueryResult {
                columns: vec![],
                rows: vec![],
                encoded_rows: None,
                stats: BoltResultStats::default(),
                notifications: vec![],
            })
        }

        fn execute_as_on_database_with_context_and_bookmarks(
            &self,
            _database: &str,
            _query: &str,
            _params: &HashMap<String, serde_json::Value>,
            _request_context: copperdb_util::RequestContext,
            _principal: Option<&BoltPrincipal>,
            bookmarks: &[String],
        ) -> Result<BoltQueryResult, BoltExecutionError> {
            *self.bookmarks.lock().unwrap() = bookmarks.to_vec();
            self.execute("", &HashMap::new())
                .map_err(BoltExecutionError::from)
        }
    }

    impl QueryExecutor for MutationStatsExecutor {
        fn execute(
            &self,
            _query: &str,
            _params: &HashMap<String, serde_json::Value>,
        ) -> Result<BoltQueryResult, String> {
            Ok(BoltQueryResult {
                columns: vec![],
                rows: vec![],
                encoded_rows: None,
                stats: BoltResultStats {
                    nodes_created: 2,
                    relationships_created: 1,
                    relationships_deleted: 1,
                    properties_set: 3,
                    ..BoltResultStats::default()
                },
                notifications: vec![],
            })
        }
    }

    impl QueryExecutor for NotificationExecutor {
        fn execute(
            &self,
            _query: &str,
            _params: &HashMap<String, serde_json::Value>,
        ) -> Result<BoltQueryResult, String> {
            Ok(BoltQueryResult {
                columns: vec![],
                rows: vec![],
                encoded_rows: None,
                stats: BoltResultStats::default(),
                notifications: vec![BoltNotification {
                    code: "Neo.ClientNotification.Statement.UnknownLabelWarning".into(),
                    title: "Unknown label".into(),
                    description: "The query references an unknown label.".into(),
                    severity: "WARNING".into(),
                    category: "UNRECOGNIZED".into(),
                }],
            })
        }
    }

    impl QueryExecutor for MultiRowExecutor {
        fn execute(
            &self,
            _query: &str,
            _params: &HashMap<String, serde_json::Value>,
        ) -> Result<BoltQueryResult, String> {
            Ok(BoltQueryResult {
                columns: vec!["value".into()],
                rows: vec![],
                encoded_rows: Some(Arc::new(
                    [1, 2, 3]
                        .into_iter()
                        .map(|value| dispatch::encode_record(&[serde_json::json!(value)]))
                        .collect(),
                )),
                stats: BoltResultStats::default(),
                notifications: vec![],
            })
        }

        fn begin_transaction(
            &self,
            database: &str,
            _metadata: &HashMap<String, serde_json::Value>,
            _principal: Option<&BoltPrincipal>,
        ) -> Result<BoltTransaction, String> {
            Ok(BoltTransaction {
                id: "driver-e2e-transaction".into(),
                database: database.into(),
            })
        }

        fn commit_transaction(
            &self,
            _transaction: &BoltTransaction,
        ) -> Result<String, BoltTransactionError> {
            Ok("copperdb:bookmark:driver-e2e".into())
        }

        fn execute_in_transaction_with_context(
            &self,
            _transaction: &BoltTransaction,
            query: &str,
            params: &HashMap<String, serde_json::Value>,
            _request_context: copperdb_util::RequestContext,
            _principal: Option<&BoltPrincipal>,
        ) -> Result<BoltQueryResult, BoltExecutionError> {
            self.execute(query, params)
                .map_err(BoltExecutionError::from)
        }
    }

    impl QueryExecutor for PrincipalRecordingExecutor {
        fn execute(
            &self,
            _query: &str,
            _params: &HashMap<String, serde_json::Value>,
        ) -> Result<BoltQueryResult, String> {
            Ok(BoltQueryResult {
                columns: vec![],
                rows: vec![],
                encoded_rows: None,
                stats: BoltResultStats::default(),
                notifications: vec![],
            })
        }

        fn execute_as_on_database_with_context(
            &self,
            _database: &str,
            _query: &str,
            _params: &HashMap<String, serde_json::Value>,
            _request_context: copperdb_util::RequestContext,
            principal: Option<&BoltPrincipal>,
        ) -> Result<BoltQueryResult, BoltExecutionError> {
            *self.principal.lock().unwrap() = principal.cloned();
            self.execute("", &HashMap::new())
                .map_err(BoltExecutionError::from)
        }
    }

    struct TransactionRecordingExecutor {
        operations: Arc<Mutex<Vec<&'static str>>>,
    }

    impl QueryExecutor for TransactionRecordingExecutor {
        fn execute(
            &self,
            _query: &str,
            _params: &HashMap<String, serde_json::Value>,
        ) -> Result<BoltQueryResult, String> {
            Ok(BoltQueryResult {
                columns: vec![],
                rows: vec![],
                encoded_rows: None,
                stats: BoltResultStats::default(),
                notifications: vec![],
            })
        }

        fn begin_transaction(
            &self,
            database: &str,
            _metadata: &HashMap<String, serde_json::Value>,
            _principal: Option<&BoltPrincipal>,
        ) -> Result<BoltTransaction, String> {
            self.operations.lock().unwrap().push("begin");
            Ok(BoltTransaction {
                id: "test-transaction".into(),
                database: database.into(),
            })
        }

        fn commit_transaction(
            &self,
            _transaction: &BoltTransaction,
        ) -> Result<String, BoltTransactionError> {
            self.operations.lock().unwrap().push("commit");
            Ok("copperdb:bookmark:test".into())
        }

        fn rollback_transaction(&self, _transaction: &BoltTransaction) -> Result<(), String> {
            self.operations.lock().unwrap().push("rollback");
            Ok(())
        }

        fn execute_in_transaction_with_context(
            &self,
            _transaction: &BoltTransaction,
            _query: &str,
            _params: &HashMap<String, serde_json::Value>,
            _request_context: copperdb_util::RequestContext,
            _principal: Option<&BoltPrincipal>,
        ) -> Result<BoltQueryResult, BoltExecutionError> {
            self.operations.lock().unwrap().push("run");
            self.execute("", &HashMap::new())
                .map_err(BoltExecutionError::from)
        }
    }

    struct FailingTransactionExecutor {
        operations: Arc<Mutex<Vec<&'static str>>>,
    }

    struct CancellingExecutor {
        operations: Arc<Mutex<Vec<&'static str>>>,
    }

    struct DeadlineAwareExecutor;

    struct DisconnectAwareExecutor {
        started: Arc<std::sync::atomic::AtomicBool>,
        cancelled: Arc<std::sync::atomic::AtomicBool>,
    }

    impl QueryExecutor for DisconnectAwareExecutor {
        fn execute(
            &self,
            _query: &str,
            _params: &HashMap<String, serde_json::Value>,
        ) -> Result<BoltQueryResult, String> {
            unreachable!("context-aware execution is required")
        }

        fn execute_as_on_database_with_context(
            &self,
            _database: &str,
            _query: &str,
            _params: &HashMap<String, serde_json::Value>,
            request_context: copperdb_util::RequestContext,
            _principal: Option<&BoltPrincipal>,
        ) -> Result<BoltQueryResult, BoltExecutionError> {
            self.started.store(true, Ordering::Release);
            loop {
                if let Err(error) = request_context.check_active() {
                    self.cancelled.store(true, Ordering::Release);
                    return Err(error.into());
                }
                std::thread::yield_now();
            }
        }
    }

    impl QueryExecutor for DeadlineAwareExecutor {
        fn execute(
            &self,
            _query: &str,
            _params: &HashMap<String, serde_json::Value>,
        ) -> Result<BoltQueryResult, String> {
            unreachable!("context-aware execution is required")
        }

        fn execute_as_on_database_with_context(
            &self,
            _database: &str,
            _query: &str,
            _params: &HashMap<String, serde_json::Value>,
            request_context: copperdb_util::RequestContext,
            _principal: Option<&BoltPrincipal>,
        ) -> Result<BoltQueryResult, BoltExecutionError> {
            loop {
                if let Err(error) = request_context.check_active() {
                    return Err(error.into());
                }
                std::thread::yield_now();
            }
        }
    }

    impl QueryExecutor for CancellingExecutor {
        fn execute(
            &self,
            _query: &str,
            _params: &HashMap<String, serde_json::Value>,
        ) -> Result<BoltQueryResult, String> {
            unreachable!("context-aware execution is required")
        }

        fn execute_as_on_database_with_context(
            &self,
            _database: &str,
            _query: &str,
            _params: &HashMap<String, serde_json::Value>,
            _request_context: copperdb_util::RequestContext,
            _principal: Option<&BoltPrincipal>,
        ) -> Result<BoltQueryResult, BoltExecutionError> {
            self.operations.lock().unwrap().push("run");
            Err(BoltExecutionError::RequestCancelled(
                copperdb_util::RequestCancelled,
            ))
        }

        fn rollback_transaction(&self, _transaction: &BoltTransaction) -> Result<(), String> {
            self.operations.lock().unwrap().push("rollback");
            Ok(())
        }
    }

    struct ConflictingTransactionExecutor;

    impl QueryExecutor for ConflictingTransactionExecutor {
        fn execute(
            &self,
            _query: &str,
            _params: &HashMap<String, serde_json::Value>,
        ) -> Result<BoltQueryResult, String> {
            Ok(BoltQueryResult {
                columns: vec![],
                rows: vec![],
                encoded_rows: None,
                stats: BoltResultStats::default(),
                notifications: vec![],
            })
        }

        fn begin_transaction(
            &self,
            database: &str,
            _metadata: &HashMap<String, serde_json::Value>,
            _principal: Option<&BoltPrincipal>,
        ) -> Result<BoltTransaction, String> {
            Ok(BoltTransaction {
                id: "conflicting-transaction".into(),
                database: database.into(),
            })
        }

        fn commit_transaction(
            &self,
            _transaction: &BoltTransaction,
        ) -> Result<String, BoltTransactionError> {
            Err(BoltTransactionError::from_error(
                copperdb_errors::CopperDbError::TransactionConflict,
            ))
        }
    }

    impl QueryExecutor for FailingTransactionExecutor {
        fn execute(
            &self,
            _query: &str,
            _params: &HashMap<String, serde_json::Value>,
        ) -> Result<BoltQueryResult, String> {
            Ok(BoltQueryResult {
                columns: vec![],
                rows: vec![],
                encoded_rows: None,
                stats: BoltResultStats::default(),
                notifications: vec![],
            })
        }

        fn begin_transaction(
            &self,
            database: &str,
            _metadata: &HashMap<String, serde_json::Value>,
            _principal: Option<&BoltPrincipal>,
        ) -> Result<BoltTransaction, String> {
            self.operations.lock().unwrap().push("begin");
            Ok(BoltTransaction {
                id: "test-transaction".into(),
                database: database.into(),
            })
        }

        fn rollback_transaction(&self, _transaction: &BoltTransaction) -> Result<(), String> {
            self.operations.lock().unwrap().push("rollback");
            Ok(())
        }

        fn execute_in_transaction_with_context(
            &self,
            _transaction: &BoltTransaction,
            _query: &str,
            _params: &HashMap<String, serde_json::Value>,
            _request_context: copperdb_util::RequestContext,
            _principal: Option<&BoltPrincipal>,
        ) -> Result<BoltQueryResult, BoltExecutionError> {
            self.operations.lock().unwrap().push("run");
            Err("query failed".into())
        }
    }

    fn logon_message(username: &str, password: &str) -> Value {
        Value::Struct {
            signature: 0x6A,
            fields: vec![Value::Map(vec![
                ("scheme".into(), Value::String("basic".into())),
                ("principal".into(), Value::String(username.into())),
                ("credentials".into(), Value::String(password.into())),
            ])],
        }
    }

    fn run_message_for_database(database: &str) -> Value {
        Value::Struct {
            signature: 0x10,
            fields: vec![
                Value::String("RETURN 1".into()),
                Value::Map(vec![]),
                Value::Map(vec![("db".into(), Value::String(database.into()))]),
            ],
        }
    }

    async fn response_signature(
        message: &Value,
        session: &mut BoltSession,
        provider: Option<Arc<dyn BoltAuthProvider>>,
    ) -> u8 {
        response_signature_with_executor(message, session, Arc::new(NoopExecutor), provider).await
    }

    async fn response_signature_with_executor(
        message: &Value,
        session: &mut BoltSession,
        executor: Arc<dyn QueryExecutor>,
        provider: Option<Arc<dyn BoltAuthProvider>>,
    ) -> u8 {
        let responses = process_message(message, session, executor, provider)
            .await
            .unwrap();
        let (value, _) = crate::packstream::decode(&responses[0]).unwrap();
        let Value::Struct { signature, .. } = value else {
            panic!("expected Bolt struct response");
        };
        signature
    }

    async fn failure_code(
        message: &Value,
        session: &mut BoltSession,
        provider: Option<Arc<dyn BoltAuthProvider>>,
    ) -> String {
        failure_code_with_executor(message, session, Arc::new(NoopExecutor), provider).await
    }

    async fn failure_code_with_executor(
        message: &Value,
        session: &mut BoltSession,
        executor: Arc<dyn QueryExecutor>,
        provider: Option<Arc<dyn BoltAuthProvider>>,
    ) -> String {
        let responses = process_message(message, session, executor, provider)
            .await
            .unwrap();
        failure_code_from_response(&responses[0])
    }

    fn failure_code_from_response(response: &[u8]) -> String {
        let (value, _) = crate::packstream::decode(response).unwrap();
        let Value::Struct {
            signature: 0x7F,
            fields,
        } = value
        else {
            panic!("expected Bolt FAILURE response");
        };
        let [Value::Map(metadata)] = fields.as_slice() else {
            panic!("expected Bolt FAILURE metadata");
        };
        let code = metadata
            .iter()
            .find_map(|(key, value)| (key == "code").then_some(value))
            .expect("expected Bolt failure code");
        let Value::String(code) = code else {
            panic!("expected Bolt failure code string");
        };
        code.clone()
    }

    fn failure_message_from_response(response: &[u8]) -> String {
        let (value, _) = crate::packstream::decode(response).unwrap();
        let Value::Struct {
            signature: 0x7F,
            fields,
        } = value
        else {
            panic!("expected Bolt FAILURE response");
        };
        let [Value::Map(metadata)] = fields.as_slice() else {
            panic!("expected Bolt FAILURE metadata");
        };
        let Value::String(message) = metadata
            .iter()
            .find_map(|(key, value)| (key == "message").then_some(value))
            .expect("expected Bolt failure message")
        else {
            panic!("expected Bolt failure message string");
        };
        message.clone()
    }

    #[tokio::test]
    async fn bolt_locale_metadata_localizes_failures_without_changing_codes() {
        for (locale, expected) in [
            ("es-ES", "No hay ninguna transacción que confirmar"),
            ("en-XA", "[!! No transaction to commit !!]"),
        ] {
            let mut session = BoltSession::new(false);
            let begin = Value::Struct {
                signature: 0x11,
                fields: vec![Value::Map(vec![(
                    "locale".into(),
                    Value::String(locale.into()),
                )])],
            };
            process_message(&begin, &mut session, Arc::new(NoopExecutor), None)
                .await
                .unwrap();
            session.take_transaction();

            let commit = Value::Struct {
                signature: 0x12,
                fields: vec![],
            };
            let response = process_message(&commit, &mut session, Arc::new(NoopExecutor), None)
                .await
                .unwrap();
            assert_eq!(
                failure_code_from_response(&response[0]),
                "Neo.ClientError.Transaction.TransactionNotFound"
            );
            assert_eq!(failure_message_from_response(&response[0]), expected);
        }
    }

    #[tokio::test]
    async fn hello_without_locale_preserves_inherited_language() {
        let preferences = vec![LanguageTag::parse("es-ES").unwrap().unwrap()];
        let mut session = BoltSession::new_with_preferences(false, None, preferences);
        let hello = Value::Struct {
            signature: 0x01,
            fields: vec![Value::Map(Vec::new())],
        };
        process_message(&hello, &mut session, Arc::new(NoopExecutor), None)
            .await
            .unwrap();

        let commit = Value::Struct {
            signature: 0x12,
            fields: vec![],
        };
        let response = process_message(&commit, &mut session, Arc::new(NoopExecutor), None)
            .await
            .unwrap();
        assert_eq!(
            failure_code_from_response(&response[0]),
            "Neo.ClientError.Transaction.TransactionNotFound"
        );
        assert_eq!(
            failure_message_from_response(&response[0]),
            "No hay ninguna transacción que confirmar"
        );
    }

    #[tokio::test]
    async fn reachable_bolt_state_failures_are_localized_with_stable_codes() {
        let spanish = vec![LanguageTag::parse("es-ES").unwrap().unwrap()];
        let pull = |qid| Value::Struct {
            signature: 0x3F,
            fields: vec![Value::Integer(-1), Value::Integer(qid)],
        };

        let mut switch_session = BoltSession::new_with_preferences(false, None, spanish.clone());
        switch_session.set_transaction(BoltTransaction {
            id: "tx-switch".into(),
            database: "alpha".into(),
        });
        let switch = process_message(
            &run_message_for_database("beta"),
            &mut switch_session,
            Arc::new(NoopExecutor),
            None,
        )
        .await
        .unwrap();

        let mut duplicate_session = BoltSession::new_with_preferences(false, None, spanish.clone());
        duplicate_session.set_transaction(BoltTransaction {
            id: "tx-duplicate".into(),
            database: "copperdb".into(),
        });
        let duplicate = process_message(
            &Value::Struct {
                signature: 0x11,
                fields: vec![Value::Map(vec![])],
            },
            &mut duplicate_session,
            Arc::new(NoopExecutor),
            None,
        )
        .await
        .unwrap();

        let mut cursor_session = BoltSession::new_with_preferences(false, None, spanish.clone());
        let missing = process_message(&pull(-1), &mut cursor_session, Arc::new(NoopExecutor), None)
            .await
            .unwrap();
        let unknown = process_message(&pull(42), &mut cursor_session, Arc::new(NoopExecutor), None)
            .await
            .unwrap();

        for (response, code, message) in [
            (
                &switch[0],
                "Neo.ClientError.Transaction.TransactionAccessedConcurrently",
                "no se puede cambiar de base de datos durante una transacción activa",
            ),
            (
                &duplicate[0],
                "Neo.ClientError.Transaction.TransactionStartFailed",
                "ya hay una transacción activa",
            ),
            (
                &missing[0],
                "Neo.ClientError.Request.Invalid",
                "no hay ningún cursor de resultados Bolt activo",
            ),
            (
                &unknown[0],
                "Neo.ClientError.Request.Invalid",
                "cursor de resultados Bolt desconocido",
            ),
        ] {
            assert_eq!(failure_code_from_response(response), code);
            assert_eq!(failure_message_from_response(response), message);
        }
    }

    #[tokio::test]
    async fn cancelled_run_uses_nornicdb_failure_code_and_rolls_back_transaction() {
        let operations = Arc::new(Mutex::new(Vec::new()));
        let executor: Arc<dyn QueryExecutor> = Arc::new(CancellingExecutor {
            operations: Arc::clone(&operations),
        });
        let mut session = BoltSession::new(false);
        session.transaction = Some(BoltTransaction {
            id: "cancelled-transaction".into(),
            database: "copperdb".into(),
        });
        let telemetry = Telemetry::new();

        let responses = process_message_with_telemetry(
            &run_message(),
            &mut session,
            executor,
            None,
            Some(&telemetry),
        )
        .await
        .unwrap();
        let (value, _) = crate::packstream::decode(&responses[0]).unwrap();
        let Value::Struct {
            signature: 0x7F,
            fields,
        } = value
        else {
            panic!("expected Bolt FAILURE response");
        };
        let [Value::Map(metadata)] = fields.as_slice() else {
            panic!("expected Bolt FAILURE metadata");
        };

        assert_eq!(
            metadata
                .iter()
                .find_map(|(key, value)| (key == "code").then_some(value)),
            Some(&Value::String(
                "Neo.ClientError.Statement.SyntaxError".into()
            ))
        );
        assert_eq!(
            metadata
                .iter()
                .find_map(|(key, value)| (key == "message").then_some(value)),
            Some(&Value::String("request cancelled".into()))
        );
        assert!(session.transaction.is_none());
        assert_eq!(*operations.lock().unwrap(), vec!["run", "rollback"]);
        assert_eq!(
            telemetry
                .snapshot_metric("copperdb_request_cancellations_total")
                .unwrap(),
            vec![copperdb_otel::MetricSample {
                labels: vec![
                    ("protocol".into(), "bolt".into()),
                    ("reason".into(), "explicit".into()),
                    ("stage".into(), "execution".into()),
                ],
                value: copperdb_otel::MetricValue::Counter(1.0),
            }]
        );
    }

    #[tokio::test]
    async fn client_statement_timeout_cancels_run_and_records_deadline() {
        let mut session = BoltSession::new(false);
        let telemetry = Telemetry::new();
        let message = Value::Struct {
            signature: 0x10,
            fields: vec![
                Value::String("RETURN 1".into()),
                Value::Map(vec![]),
                Value::Map(vec![("tx_timeout".into(), Value::Integer(10))]),
            ],
        };

        let responses = process_message_with_telemetry(
            &message,
            &mut session,
            Arc::new(DeadlineAwareExecutor),
            None,
            Some(&telemetry),
        )
        .await
        .unwrap();

        assert_eq!(
            failure_code_from_response(&responses[0]),
            "Neo.ClientError.Statement.SyntaxError"
        );
        assert_eq!(
            telemetry
                .snapshot_metric("copperdb_request_cancellations_total")
                .unwrap(),
            vec![copperdb_otel::MetricSample {
                labels: vec![
                    ("protocol".into(), "bolt".into()),
                    ("reason".into(), "deadline".into()),
                    ("stage".into(), "execution".into()),
                ],
                value: copperdb_otel::MetricValue::Counter(1.0),
            }]
        );
    }

    #[tokio::test]
    async fn logon_authenticates_and_logoff_clears_the_principal() {
        let provider: Arc<dyn BoltAuthProvider> = Arc::new(TestAuthProvider);
        let mut session = BoltSession::new(true);

        assert_eq!(
            response_signature(
                &logon_message("reader", "correct-password"),
                &mut session,
                Some(Arc::clone(&provider)),
            )
            .await,
            0x70
        );
        assert_eq!(session.principal.as_ref().unwrap().roles, vec!["reader"]);
        assert!(session.authenticated);

        let logoff = Value::Struct {
            signature: 0x6B,
            fields: vec![],
        };
        assert_eq!(
            response_signature(&logoff, &mut session, Some(provider)).await,
            0x70
        );
        assert!(session.principal.is_none());
        assert!(!session.authenticated);
    }

    #[tokio::test]
    async fn failed_logon_returns_unauthorized_and_does_not_authenticate_session() {
        let provider: Arc<dyn BoltAuthProvider> = Arc::new(TestAuthProvider);
        let mut session = BoltSession::new(true);

        assert_eq!(
            response_signature(
                &logon_message("reader", "wrong-password"),
                &mut session,
                Some(Arc::clone(&provider)),
            )
            .await,
            0x7F
        );
        assert!(session.principal.is_none());
        assert!(!session.authenticated);
        assert_eq!(
            response_signature(&run_message(), &mut session, Some(provider)).await,
            0x7F
        );
    }

    #[tokio::test]
    async fn run_forwards_authenticated_principal_to_executor() {
        let provider: Arc<dyn BoltAuthProvider> = Arc::new(TestAuthProvider);
        let recorded_principal = Arc::new(Mutex::new(None));
        let executor = Arc::new(PrincipalRecordingExecutor {
            principal: Arc::clone(&recorded_principal),
        });
        let mut session = BoltSession::new(true);

        response_signature(
            &logon_message("reader", "correct-password"),
            &mut session,
            Some(Arc::clone(&provider)),
        )
        .await;
        let responses = process_message(&run_message(), &mut session, executor, Some(provider))
            .await
            .unwrap();
        let (value, _) = crate::packstream::decode(&responses[0]).unwrap();
        assert!(matches!(
            value,
            Value::Struct {
                signature: 0x70,
                ..
            }
        ));
        assert_eq!(
            *recorded_principal.lock().unwrap(),
            Some(BoltPrincipal {
                username: "reader".into(),
                roles: vec!["reader".into()],
            })
        );
    }

    #[tokio::test]
    async fn explicit_transactions_require_authentication_and_use_executor_lifecycle() {
        let provider: Arc<dyn BoltAuthProvider> = Arc::new(TestAuthProvider);
        let operations = Arc::new(Mutex::new(Vec::new()));
        let executor: Arc<dyn QueryExecutor> = Arc::new(TransactionRecordingExecutor {
            operations: Arc::clone(&operations),
        });
        let begin = Value::Struct {
            signature: 0x11,
            fields: vec![Value::Map(vec![])],
        };
        let commit = Value::Struct {
            signature: 0x12,
            fields: vec![],
        };
        let rollback = Value::Struct {
            signature: 0x13,
            fields: vec![],
        };
        let mut session = BoltSession::new(true);

        assert_eq!(
            failure_code(&begin, &mut session, Some(Arc::clone(&provider))).await,
            "Neo.ClientError.Security.Unauthorized"
        );

        response_signature(
            &logon_message("reader", "correct-password"),
            &mut session,
            Some(Arc::clone(&provider)),
        )
        .await;
        assert_eq!(
            response_signature_with_executor(
                &begin,
                &mut session,
                Arc::clone(&executor),
                Some(Arc::clone(&provider)),
            )
            .await,
            0x70
        );
        assert!(session.transaction.is_some());
        assert_eq!(
            response_signature_with_executor(
                &commit,
                &mut session,
                Arc::clone(&executor),
                Some(Arc::clone(&provider)),
            )
            .await,
            0x70
        );
        assert!(session.transaction.is_none());
        assert_eq!(*operations.lock().unwrap(), vec!["begin", "commit"]);

        assert_eq!(
            response_signature_with_executor(
                &begin,
                &mut session,
                Arc::clone(&executor),
                Some(Arc::clone(&provider)),
            )
            .await,
            0x70
        );
        assert_eq!(
            response_signature_with_executor(&rollback, &mut session, executor, Some(provider),)
                .await,
            0x70
        );
        assert_eq!(
            *operations.lock().unwrap(),
            vec!["begin", "commit", "begin", "rollback"]
        );
    }

    #[tokio::test]
    async fn runtime_status_tracks_session_and_transaction_guards() {
        let counters = Arc::new(BoltRuntimeCounters::default());
        let operations = Arc::new(Mutex::new(Vec::new()));
        let executor: Arc<dyn QueryExecutor> = Arc::new(TransactionRecordingExecutor {
            operations: Arc::clone(&operations),
        });
        let begin = Value::Struct {
            signature: 0x11,
            fields: vec![Value::Map(vec![])],
        };
        let commit = Value::Struct {
            signature: 0x12,
            fields: vec![],
        };
        let mut session = BoltSession::new_with_counters(false, Some(Arc::clone(&counters)));
        assert_eq!(counters.snapshot().active_sessions, 1);
        assert_eq!(counters.snapshot().active_transactions, 0);

        response_signature_with_executor(&begin, &mut session, Arc::clone(&executor), None).await;
        assert_eq!(counters.snapshot().active_transactions, 1);

        response_signature_with_executor(&commit, &mut session, executor, None).await;
        assert_eq!(counters.snapshot().active_transactions, 0);
        drop(session);
        assert_eq!(counters.snapshot().active_sessions, 0);
    }

    #[tokio::test]
    async fn retryable_commit_conflicts_use_the_neo4j_outdated_status() {
        let provider: Arc<dyn BoltAuthProvider> = Arc::new(TestAuthProvider);
        let executor: Arc<dyn QueryExecutor> = Arc::new(ConflictingTransactionExecutor);
        let begin = Value::Struct {
            signature: 0x11,
            fields: vec![Value::Map(vec![])],
        };
        let commit = Value::Struct {
            signature: 0x12,
            fields: vec![],
        };
        let mut session = BoltSession::new(true);

        response_signature(
            &logon_message("reader", "correct-password"),
            &mut session,
            Some(Arc::clone(&provider)),
        )
        .await;
        assert_eq!(
            response_signature_with_executor(
                &begin,
                &mut session,
                Arc::clone(&executor),
                Some(Arc::clone(&provider)),
            )
            .await,
            0x70
        );
        assert_eq!(
            failure_code_with_executor(&commit, &mut session, executor, Some(provider)).await,
            "Neo.TransientError.Transaction.Outdated"
        );
    }

    #[tokio::test]
    async fn transaction_run_uses_active_transaction_and_rejects_database_switches() {
        let provider: Arc<dyn BoltAuthProvider> = Arc::new(TestAuthProvider);
        let operations = Arc::new(Mutex::new(Vec::new()));
        let executor: Arc<dyn QueryExecutor> = Arc::new(TransactionRecordingExecutor {
            operations: Arc::clone(&operations),
        });
        let begin = Value::Struct {
            signature: 0x11,
            fields: vec![Value::Map(vec![(
                "db".into(),
                Value::String("primary".into()),
            )])],
        };
        let mut session = BoltSession::new(true);

        response_signature(
            &logon_message("reader", "correct-password"),
            &mut session,
            Some(Arc::clone(&provider)),
        )
        .await;
        assert_eq!(
            response_signature_with_executor(
                &begin,
                &mut session,
                Arc::clone(&executor),
                Some(Arc::clone(&provider)),
            )
            .await,
            0x70
        );
        assert_eq!(
            response_signature_with_executor(
                &run_message(),
                &mut session,
                Arc::clone(&executor),
                Some(Arc::clone(&provider)),
            )
            .await,
            0x70
        );
        assert_eq!(*operations.lock().unwrap(), vec!["begin", "run"]);
        assert_eq!(
            failure_code_with_executor(
                &run_message_for_database("other"),
                &mut session,
                executor,
                Some(provider),
            )
            .await,
            "Neo.ClientError.Transaction.TransactionAccessedConcurrently"
        );
        assert_eq!(*operations.lock().unwrap(), vec!["begin", "run"]);
    }

    #[tokio::test]
    async fn failed_transactional_run_rolls_back_and_clears_the_active_handle() {
        let operations = Arc::new(Mutex::new(Vec::new()));
        let executor: Arc<dyn QueryExecutor> = Arc::new(FailingTransactionExecutor {
            operations: Arc::clone(&operations),
        });
        let begin = Value::Struct {
            signature: 0x11,
            fields: vec![Value::Map(vec![])],
        };
        let mut session = BoltSession::new(false);

        assert_eq!(
            response_signature_with_executor(&begin, &mut session, Arc::clone(&executor), None)
                .await,
            0x70
        );
        assert_eq!(
            response_signature_with_executor(&run_message(), &mut session, executor, None).await,
            0x7F
        );
        assert!(session.transaction.is_none());
        assert_eq!(
            *operations.lock().unwrap(),
            vec!["begin", "run", "rollback"]
        );
    }

    #[tokio::test]
    async fn pull_and_discard_page_the_requested_result_cursor() {
        let mut session = BoltSession::new(false);
        let executor: Arc<dyn QueryExecutor> = Arc::new(MultiRowExecutor);

        let run = process_message(&run_message(), &mut session, Arc::clone(&executor), None)
            .await
            .unwrap();
        let (run_success, _) = crate::packstream::decode(&run[0]).unwrap();
        let Value::Struct { fields, .. } = run_success else {
            panic!("expected RUN success");
        };
        let [Value::Map(metadata)] = fields.as_slice() else {
            panic!("expected RUN metadata");
        };
        assert!(metadata.iter().all(|(key, _)| key != "qid"));

        let pull = Value::Struct {
            signature: 0x3F,
            fields: vec![Value::Integer(1), Value::Integer(0)],
        };
        let pulled = process_message(&pull, &mut session, Arc::clone(&executor), None)
            .await
            .unwrap();
        assert_eq!(pulled.len(), 2);
        let (record, _) = crate::packstream::decode(&pulled[0]).unwrap();
        assert!(matches!(
            record,
            Value::Struct {
                signature: 0x71,
                ..
            }
        ));
        let (summary, _) = crate::packstream::decode(&pulled[1]).unwrap();
        let Value::Struct { fields, .. } = summary else {
            panic!("expected PULL summary");
        };
        let [Value::Map(metadata)] = fields.as_slice() else {
            panic!("expected PULL summary metadata");
        };
        assert!(
            metadata
                .iter()
                .any(|(key, value)| { key == "has_more" && value == &Value::Bool(true) })
        );

        let discard = Value::Struct {
            signature: 0x2F,
            fields: vec![Value::Integer(-1), Value::Integer(0)],
        };
        let discarded = process_message(&discard, &mut session, Arc::clone(&executor), None)
            .await
            .unwrap();
        assert_eq!(discarded.len(), 1);
        assert!(session.cursors.is_empty());

        let invalid_pull = Value::Struct {
            signature: 0x3F,
            fields: vec![Value::Integer(1), Value::Integer(99)],
        };
        assert_eq!(
            response_signature_with_executor(&invalid_pull, &mut session, executor, None).await,
            0x7F
        );
    }

    #[tokio::test]
    async fn pull_with_latest_qid_selects_the_newest_remaining_cursor() {
        let mut session = BoltSession::new(false);
        let executor: Arc<dyn QueryExecutor> = Arc::new(MultiRowExecutor);

        process_message(&run_message(), &mut session, Arc::clone(&executor), None)
            .await
            .unwrap();
        process_message(&run_message(), &mut session, Arc::clone(&executor), None)
            .await
            .unwrap();
        assert_eq!(session.last_qid, Some(1));

        let latest_pull = Value::Struct {
            signature: 0x3F,
            fields: vec![Value::Integer(-1), Value::Integer(-1)],
        };
        let responses = process_message(&latest_pull, &mut session, executor, None)
            .await
            .unwrap();
        assert_eq!(responses.len(), 4);
        assert!(session.cursors.contains_key(&0));
        assert!(!session.cursors.contains_key(&1));
        assert_eq!(session.last_qid, Some(0));
    }

    #[tokio::test]
    async fn terminal_result_summary_includes_mutation_counters() {
        let mut session = BoltSession::new(false);
        let executor: Arc<dyn QueryExecutor> = Arc::new(MutationStatsExecutor);

        process_message(&run_message(), &mut session, Arc::clone(&executor), None)
            .await
            .unwrap();
        let pull = Value::Struct {
            signature: 0x3F,
            fields: vec![Value::Integer(-1), Value::Integer(-1)],
        };
        let responses = process_message(&pull, &mut session, executor, None)
            .await
            .unwrap();
        let (summary, _) = crate::packstream::decode(&responses[0]).unwrap();
        let Value::Struct { fields, .. } = summary else {
            panic!("expected terminal result summary");
        };
        let [Value::Map(metadata)] = fields.as_slice() else {
            panic!("expected summary metadata");
        };
        let stats = metadata
            .iter()
            .find_map(|(key, value)| (key == "stats").then_some(value))
            .expect("expected stats metadata");
        let Value::Map(counters) = stats else {
            panic!("expected stats map");
        };
        assert!(
            counters
                .iter()
                .any(|(key, value)| { key == "nodes-created" && value == &Value::Integer(2) })
        );
        assert!(
            counters.iter().any(|(key, value)| {
                key == "relationships-created" && value == &Value::Integer(1)
            })
        );
        assert!(
            counters.iter().any(|(key, value)| {
                key == "relationships-deleted" && value == &Value::Integer(1)
            })
        );
        assert!(
            counters
                .iter()
                .any(|(key, value)| { key == "properties-set" && value == &Value::Integer(3) })
        );
    }

    #[tokio::test]
    async fn terminal_result_summary_includes_notifications() {
        let mut session = BoltSession::new(false);
        let executor: Arc<dyn QueryExecutor> = Arc::new(NotificationExecutor);

        process_message(&run_message(), &mut session, Arc::clone(&executor), None)
            .await
            .unwrap();
        let pull = Value::Struct {
            signature: 0x3F,
            fields: vec![Value::Integer(-1), Value::Integer(-1)],
        };
        let responses = process_message(&pull, &mut session, executor, None)
            .await
            .unwrap();
        let (summary, _) = crate::packstream::decode(&responses[0]).unwrap();
        let Value::Struct { fields, .. } = summary else {
            panic!("expected terminal result summary");
        };
        let [Value::Map(metadata)] = fields.as_slice() else {
            panic!("expected summary metadata");
        };
        let notifications = metadata
            .iter()
            .find_map(|(key, value)| (key == "notifications").then_some(value))
            .expect("expected notifications metadata");
        let Value::List(notifications) = notifications else {
            panic!("expected notifications list");
        };
        let [Value::Map(notification)] = notifications.as_slice() else {
            panic!("expected one notification");
        };
        assert!(notification.iter().any(|(key, value)| {
            key == "code"
                && value
                    == &Value::String("Neo.ClientNotification.Statement.UnknownLabelWarning".into())
        }));
        assert!(notification.iter().any(|(key, value)| {
            key == "severity" && value == &Value::String("WARNING".into())
        }));
        assert!(notification.iter().any(|(key, value)| {
            key == "category" && value == &Value::String("UNRECOGNIZED".into())
        }));
    }

    #[tokio::test]
    async fn implicit_run_forwards_bookmarks_to_the_executor() {
        let recorded_bookmarks = Arc::new(Mutex::new(Vec::new()));
        let executor: Arc<dyn QueryExecutor> = Arc::new(BookmarkRecordingExecutor {
            bookmarks: Arc::clone(&recorded_bookmarks),
        });
        let mut session = BoltSession::new(false);
        let run = Value::Struct {
            signature: 0x10,
            fields: vec![
                Value::String("RETURN 1".into()),
                Value::Map(vec![]),
                Value::Map(vec![(
                    "bookmarks".into(),
                    Value::List(vec![Value::String("7:2a:9".into())]),
                )]),
            ],
        };

        assert_eq!(
            response_signature_with_executor(&run, &mut session, executor, None).await,
            0x70
        );
        assert_eq!(*recorded_bookmarks.lock().unwrap(), vec!["7:2a:9"]);
    }

    #[tokio::test]
    async fn run_requires_logon_when_authentication_is_enabled() {
        let mut session = BoltSession::new(true);
        let responses = process_message(&run_message(), &mut session, Arc::new(NoopExecutor), None)
            .await
            .unwrap();
        let (value, _) = crate::packstream::decode(&responses[0]).unwrap();
        let Value::Struct { signature, fields } = value else {
            panic!("expected Bolt FAILURE response");
        };

        assert_eq!(signature, 0x7F);
        let [Value::Map(metadata)] = fields.as_slice() else {
            panic!("expected Bolt FAILURE metadata");
        };
        let code = metadata
            .iter()
            .find_map(|(key, value)| (key == "code").then_some(value));
        assert_eq!(
            code,
            Some(&Value::String(
                "Neo.ClientError.Security.Unauthorized".into()
            ))
        );
    }

    #[tokio::test]
    async fn run_is_allowed_when_authentication_is_disabled() {
        let mut session = BoltSession::new(false);
        let responses = process_message(&run_message(), &mut session, Arc::new(NoopExecutor), None)
            .await
            .unwrap();
        let (value, _) = crate::packstream::decode(&responses[0]).unwrap();
        let Value::Struct { signature, .. } = value else {
            panic!("expected Bolt SUCCESS response");
        };

        assert_eq!(signature, 0x70);
    }

    #[tokio::test]
    async fn test_full_tcp_handshake_and_message_flow() {
        // Bind to an ephemeral port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let telemetry = Arc::new(Telemetry::new());
        telemetry.seed_zero_catalog_metrics();

        // Spawn the handler
        let server_telemetry = Arc::clone(&telemetry);
        tokio::spawn(async move {
            let (mut stream, _peer) = listener.accept().await.unwrap();
            let exec = Arc::new(NoopExecutor);
            let result =
                handle_tcp_session(&mut stream, &server_telemetry, exec, false, None).await;
            if let Err(ref e) = result {
                eprintln!("server error: {e}");
            }
        });

        // Connect client
        let mut client = TcpStream::connect(addr).await.unwrap();

        // Step 1: Send Bolt preamble (magic + version proposals)
        let preamble: Vec<u8> = vec![
            0x60, 0x60, 0xB0, 0x17, // magic
            0x00, 0x00, 0x05, 0x00, // v5.0
            0x00, 0x00, 0x04, 0x04, // v4.4
            0x00, 0x00, 0x04, 0x03, // v4.3
            0x00, 0x00, 0x04, 0x02, // v4.2
        ];
        client.write_all(&preamble).await.unwrap();

        // Read version response (4 bytes)
        let mut version_resp = [0u8; 4];
        client.read_exact(&mut version_resp).await.unwrap();
        assert_eq!(
            version_resp,
            [0x00, 0x00, 0x04, 0x04],
            "server should accept v4.4"
        );

        // Step 2: Send HELLO
        let hello_bytes = encode_bolt_struct(
            0x01,
            &[Value::Map(vec![
                (
                    "user_agent".into(),
                    Value::String("neo4j-browser/5.0".into()),
                ),
                ("scheme".into(), Value::String("none".into())),
            ])],
        );
        client.write_all(&chunk_encode(&hello_bytes)).await.unwrap();

        // Read SUCCESS response
        let mut resp_buf = Vec::new();
        let mut tmp = [0u8; 4096];
        let n = client.read(&mut tmp).await.unwrap();
        assert!(n > 0, "should receive SUCCESS response for HELLO");
        resp_buf.extend_from_slice(&tmp[..n]);

        let response = chunk_decode(&resp_buf);
        let (value, _consumed) = crate::packstream::decode(&response).unwrap();
        match value {
            Value::Struct { signature, fields } => {
                assert_eq!(
                    signature, 0x70,
                    "expected SUCCESS (0x70), got 0x{signature:02X}"
                );
                assert_eq!(
                    fields.len(),
                    1,
                    "SUCCESS should have 1 field (metadata map)"
                );
                if let Value::Map(ref meta) = fields[0] {
                    let server = meta.iter().find(|(k, _)| k == "server").map(|(_, v)| v);
                    assert!(server.is_some(), "SUCCESS metadata should include 'server'");
                    let hints = meta.iter().find(|(k, _)| k == "hints").map(|(_, v)| v);
                    assert_eq!(hints, Some(&Value::Map(vec![])));
                }
            }
            other => panic!("expected struct, got {other:?}"),
        }

        // Step 3: Send RUN
        let run_bytes = encode_bolt_struct(
            0x10,
            &[
                Value::String("RETURN 1 AS n".into()),
                Value::Map(vec![]),
                Value::Map(vec![]),
            ],
        );
        client.write_all(&chunk_encode(&run_bytes)).await.unwrap();

        // Read SUCCESS for RUN
        resp_buf.clear();
        let n = client.read(&mut tmp).await.unwrap();
        assert!(n > 0, "should receive SUCCESS for RUN");
        resp_buf.extend_from_slice(&tmp[..n]);
        let response = chunk_decode(&resp_buf);
        let (value, _) = crate::packstream::decode(&response).unwrap();
        match value {
            Value::Struct { signature, .. } => {
                assert_eq!(
                    signature, 0x70,
                    "expected SUCCESS for RUN, got 0x{signature:02X}"
                );
            }
            other => panic!("expected SUCCESS, got {other:?}"),
        }

        // Step 4: Send PULL (Bolt 4.x: two direct integer fields)
        let pull_bytes = encode_bolt_struct(0x3F, &[Value::Integer(-1), Value::Integer(-1)]);
        client.write_all(&chunk_encode(&pull_bytes)).await.unwrap();

        // Read SUCCESS for PULL (stream done)
        resp_buf.clear();
        let n = client.read(&mut tmp).await.unwrap();
        assert!(n > 0, "should receive SUCCESS for PULL");
        resp_buf.extend_from_slice(&tmp[..n]);
        let response = chunk_decode(&resp_buf);
        let (value, _) = crate::packstream::decode(&response).unwrap();
        match value {
            Value::Struct { signature, .. } => {
                assert_eq!(
                    signature, 0x70,
                    "expected SUCCESS for PULL, got 0x{signature:02X}"
                );
            }
            other => panic!("expected SUCCESS, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_tcp_invalid_magic_rejected() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let telemetry = Arc::new(Telemetry::new());
        telemetry.seed_zero_catalog_metrics();
        let st = Arc::clone(&telemetry);

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let exec = Arc::new(NoopExecutor);
            let result = handle_tcp_session(&mut stream, &st, exec, false, None).await;
            assert!(result.is_err());
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        // Send invalid magic
        client
            .write_all(&[
                0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ])
            .await
            .unwrap();
        // Server should close connection
        let mut buf = [0u8; 1];
        let result = client.read(&mut buf).await;
        // Should get EOF or error
        assert!(result.unwrap_or(0) == 0);
    }

    #[tokio::test]
    async fn test_tcp_eof_handled_gracefully() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let telemetry = Arc::new(Telemetry::new());
        telemetry.seed_zero_catalog_metrics();
        let st = Arc::clone(&telemetry);

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let exec = Arc::new(NoopExecutor);
            let result = handle_tcp_session(&mut stream, &st, exec, false, None).await;
            // EOF after preamble should be Ok (clean disconnect)
            assert!(result.is_ok());
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        // Send valid preamble...
        let preamble: Vec<u8> = vec![
            0x60, 0x60, 0xB0, 0x17, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x04, 0x03, 0x00, 0x00,
            0x04, 0x02, 0x00, 0x00, 0x04, 0x01,
        ];
        client.write_all(&preamble).await.unwrap();
        // Read version response
        let mut vr = [0u8; 4];
        client.read_exact(&mut vr).await.unwrap();
        // Close immediately — server should handle gracefully
        drop(client);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn runtime_status_tracks_connection_lifecycle() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let telemetry = Arc::new(Telemetry::new());
        let server = BoltServer::new(address.to_string(), telemetry, Arc::new(NoopExecutor))
            .with_auth_enabled(false);
        let handler = server.clone();
        let accept_task = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            handler.spawn_tcp(stream, peer);
        });

        let client = TcpStream::connect(address).await.unwrap();
        accept_task.await.unwrap();
        assert_eq!(server.runtime_status().active_connections, 1);
        assert_eq!(server.runtime_status().failures, 0);

        drop(client);
        tokio::time::timeout(Duration::from_secs(1), async {
            while server.runtime_status().active_connections != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("Bolt connection counter should return to zero after disconnect");
    }

    #[tokio::test]
    async fn tcp_disconnect_rolls_back_active_transaction() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let telemetry = Arc::new(Telemetry::new());
        telemetry.seed_zero_catalog_metrics();
        let operations = Arc::new(Mutex::new(Vec::new()));
        let executor: Arc<dyn QueryExecutor> = Arc::new(TransactionRecordingExecutor {
            operations: Arc::clone(&operations),
        });
        let server_telemetry = Arc::clone(&telemetry);

        let handler = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handle_tcp_session(&mut stream, &server_telemetry, executor, false, None).await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&[
                0x60, 0x60, 0xB0, 0x17, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x04, 0x03, 0x00, 0x00,
                0x04, 0x02, 0x00, 0x00, 0x04, 0x01,
            ])
            .await
            .unwrap();
        let mut version = [0u8; 4];
        client.read_exact(&mut version).await.unwrap();

        let begin = encode_bolt_struct(0x11, &[Value::Map(vec![])]);
        client.write_all(&chunk_encode(&begin)).await.unwrap();
        let mut response = [0u8; 128];
        assert!(client.read(&mut response).await.unwrap() > 0);
        drop(client);

        assert!(handler.await.unwrap().is_ok());
        assert_eq!(*operations.lock().unwrap(), vec!["begin", "rollback"]);
    }

    #[tokio::test]
    async fn tcp_disconnect_cancels_active_run_and_records_ingress() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let telemetry = Arc::new(Telemetry::new());
        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let executor: Arc<dyn QueryExecutor> = Arc::new(DisconnectAwareExecutor {
            started: Arc::clone(&started),
            cancelled: Arc::clone(&cancelled),
        });
        let server_telemetry = Arc::clone(&telemetry);
        let handler = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handle_tcp_session(&mut stream, &server_telemetry, executor, false, None).await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&[
                0x60, 0x60, 0xB0, 0x17, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x04, 0x03, 0x00, 0x00,
                0x04, 0x02, 0x00, 0x00, 0x04, 0x01,
            ])
            .await
            .unwrap();
        let mut version = [0u8; 4];
        client.read_exact(&mut version).await.unwrap();
        let run = encode_bolt_struct(
            0x10,
            &[
                Value::String("RETURN 1".into()),
                Value::Map(vec![]),
                Value::Map(vec![]),
            ],
        );
        client.write_all(&chunk_encode(&run)).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("RUN started");
        drop(client);

        assert!(handler.await.unwrap().is_ok());
        tokio::time::timeout(Duration::from_secs(1), async {
            while !cancelled.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("executor observed disconnect cancellation");
        assert_eq!(
            telemetry
                .snapshot_metric("copperdb_request_cancellations_total")
                .unwrap(),
            vec![copperdb_otel::MetricSample {
                labels: vec![
                    ("protocol".into(), "bolt".into()),
                    ("reason".into(), "explicit".into()),
                    ("stage".into(), "ingress".into()),
                ],
                value: copperdb_otel::MetricValue::Counter(1.0),
            }]
        );
    }

    #[tokio::test]
    async fn tcp_reset_cancels_active_run_and_recovers_session() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let telemetry = Arc::new(Telemetry::new());
        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let executor: Arc<dyn QueryExecutor> = Arc::new(DisconnectAwareExecutor {
            started: Arc::clone(&started),
            cancelled: Arc::clone(&cancelled),
        });
        let server_telemetry = Arc::clone(&telemetry);
        let handler = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handle_tcp_session(&mut stream, &server_telemetry, executor, false, None).await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&[
                0x60, 0x60, 0xB0, 0x17, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x04, 0x03, 0x00, 0x00,
                0x04, 0x02, 0x00, 0x00, 0x04, 0x01,
            ])
            .await
            .unwrap();
        let mut version = [0u8; 4];
        client.read_exact(&mut version).await.unwrap();
        let run = encode_bolt_struct(
            0x10,
            &[
                Value::String("RETURN 1".into()),
                Value::Map(vec![]),
                Value::Map(vec![]),
            ],
        );
        client.write_all(&chunk_encode(&run)).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("RUN started");
        let reset = encode_bolt_struct(0x0F, &[]);
        client.write_all(&chunk_encode(&reset)).await.unwrap();

        let mut response = [0u8; 128];
        let bytes_read = tokio::time::timeout(Duration::from_secs(1), client.read(&mut response))
            .await
            .expect("RESET response arrived")
            .unwrap();
        let decoded = chunk_decode(&response[..bytes_read]);
        let decoded_response = crate::packstream::decode(&decoded).unwrap().0;
        assert_eq!(
            decoded_message_signature(&decoded),
            Some(0x70),
            "unexpected RESET response: {decoded_response:?}"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while !cancelled.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("executor observed RESET cancellation");
        assert!(cancelled.load(Ordering::Acquire));
        drop(client);
        assert!(handler.await.unwrap().is_ok());
        assert_eq!(
            telemetry
                .snapshot_metric("copperdb_request_cancellations_total")
                .unwrap()[0]
                .labels,
            vec![
                ("protocol".into(), "bolt".into()),
                ("reason".into(), "explicit".into()),
                ("stage".into(), "ingress".into()),
            ]
        );
    }

    #[tokio::test]
    async fn tcp_receive_timeout_rolls_back_active_transaction() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let telemetry = Arc::new(Telemetry::new());
        telemetry.seed_zero_catalog_metrics();
        let operations = Arc::new(Mutex::new(Vec::new()));
        let executor: Arc<dyn QueryExecutor> = Arc::new(TransactionRecordingExecutor {
            operations: Arc::clone(&operations),
        });
        let server_telemetry = Arc::clone(&telemetry);

        let handler = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handle_tcp_session_with_timeout(
                &mut stream,
                &server_telemetry,
                executor,
                false,
                None,
                Duration::from_millis(20),
            )
            .await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&[
                0x60, 0x60, 0xB0, 0x17, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x04, 0x03, 0x00, 0x00,
                0x04, 0x02, 0x00, 0x00, 0x04, 0x01,
            ])
            .await
            .unwrap();
        let mut version = [0u8; 4];
        client.read_exact(&mut version).await.unwrap();

        let begin = encode_bolt_struct(0x11, &[Value::Map(vec![])]);
        client.write_all(&chunk_encode(&begin)).await.unwrap();
        let mut response = [0u8; 128];
        assert!(client.read(&mut response).await.unwrap() > 0);

        let error = handler
            .await
            .unwrap()
            .expect_err("idle session should time out");
        assert!(error.to_string().contains("Bolt receive timeout"));
        assert_eq!(*operations.lock().unwrap(), vec!["begin", "rollback"]);
    }

    /// Wrap raw PackStream bytes in Bolt chunk encoding.
    fn chunk_encode(data: &[u8]) -> Vec<u8> {
        crate::wsconn::encode_bolt_chunks(data)
    }

    /// Decode Bolt chunk encoding to raw PackStream bytes (first message only).
    fn chunk_decode(data: &[u8]) -> Vec<u8> {
        crate::wsconn::decode_bolt_chunks(data)
            .into_iter()
            .next()
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn test_ws_full_handshake_hello_run_pull_flow() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let telemetry = Arc::new(Telemetry::new());
        telemetry.seed_zero_catalog_metrics();
        let server = BoltServer::new(
            addr.to_string(),
            Arc::clone(&telemetry),
            Arc::new(NoopExecutor),
        )
        .with_auth_enabled(false);

        // Spawn server that accepts one connection
        tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            server.spawn_ws(stream, "127.0.0.1:0".parse().unwrap());
            // Give the WS handler time to run
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        });

        // Give server time to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Connect via TCP and perform WS upgrade manually
        use tokio_tungstenite::tungstenite::http::Request;
        let request = Request::builder()
            .uri(format!("ws://{addr}/"))
            .header("Host", addr.to_string())
            .header("Upgrade", "websocket")
            .header("Connection", "Upgrade")
            .header("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
            .header("Sec-WebSocket-Version", "13")
            .body(())
            .unwrap();

        let (mut ws, _) = tokio_tungstenite::connect_async(request).await.unwrap();

        // Step 1: Send Bolt preamble (unchunked)
        let preamble: Vec<u8> = vec![
            0x60, 0x60, 0xB0, 0x17, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x04, 0x03, 0x00, 0x00,
            0x04, 0x02, 0x00, 0x00, 0x04, 0x01,
        ];
        use futures::SinkExt;
        ws.send(tokio_tungstenite::tungstenite::Message::Binary(
            preamble.into(),
        ))
        .await
        .unwrap();

        // Read version response
        use futures::StreamExt;
        let vr = match ws.next().await.unwrap().unwrap() {
            tokio_tungstenite::tungstenite::Message::Binary(data) => data,
            other => panic!("expected Binary version response, got {other:?}"),
        };
        assert_eq!(vr.as_ref(), &[0x00, 0x00, 0x04, 0x04]);

        // Step 2: Send HELLO (chunk-encoded per Bolt spec)
        let hello_bytes = encode_bolt_struct(
            0x01,
            &[Value::Map(vec![
                (
                    "user_agent".to_string(),
                    Value::String("neo4j-test/1.0".to_string()),
                ),
                ("scheme".to_string(), Value::String("none".to_string())),
            ])],
        );
        ws.send(tokio_tungstenite::tungstenite::Message::Binary(
            chunk_encode(&hello_bytes).into(),
        ))
        .await
        .unwrap();

        // Read SUCCESS for HELLO (chunked response → decode chunks first)
        let resp = match ws.next().await.unwrap().unwrap() {
            tokio_tungstenite::tungstenite::Message::Binary(data) => data,
            other => panic!("expected Binary SUCCESS, got {other:?}"),
        };
        let decoded = chunk_decode(&resp);
        let (value, _) = crate::packstream::decode(&decoded).unwrap();
        match value {
            Value::Struct {
                signature: 0x70, ..
            } => {}
            other => panic!("expected SUCCESS (0x70), got {other:?}"),
        }

        // Step 3: Send RUN (chunk-encoded)
        let run_bytes = encode_bolt_struct(
            0x10,
            &[
                Value::String("RETURN 1 AS n".to_string()),
                Value::Map(vec![]),
                Value::Map(vec![]),
            ],
        );
        ws.send(tokio_tungstenite::tungstenite::Message::Binary(
            chunk_encode(&run_bytes).into(),
        ))
        .await
        .unwrap();

        // Read SUCCESS for RUN
        let resp = match ws.next().await.unwrap().unwrap() {
            tokio_tungstenite::tungstenite::Message::Binary(data) => data,
            other => panic!("expected Binary SUCCESS for RUN, got {other:?}"),
        };
        let decoded = chunk_decode(&resp);
        let (value, _) = crate::packstream::decode(&decoded).unwrap();
        match value {
            Value::Struct {
                signature: 0x70, ..
            } => {}
            other => panic!("expected SUCCESS for RUN, got {other:?}"),
        }

        // Step 4: Send PULL (chunk-encoded)
        let pull_bytes = encode_bolt_struct(0x3F, &[Value::Integer(-1), Value::Integer(-1)]);
        ws.send(tokio_tungstenite::tungstenite::Message::Binary(
            chunk_encode(&pull_bytes).into(),
        ))
        .await
        .unwrap();

        // Read SUCCESS for PULL
        let resp = match ws.next().await.unwrap().unwrap() {
            tokio_tungstenite::tungstenite::Message::Binary(data) => data,
            other => panic!("expected Binary SUCCESS for PULL, got {other:?}"),
        };
        let decoded = chunk_decode(&resp);
        let (value, _) = crate::packstream::decode(&decoded).unwrap();
        match value {
            Value::Struct {
                signature: 0x70, ..
            } => {}
            other => panic!("expected SUCCESS for PULL, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn ws_close_cancels_active_run_and_records_ingress() {
        use futures::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::{Message, http::Request};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let telemetry = Arc::new(Telemetry::new());
        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let server = BoltServer::new(
            addr.to_string(),
            Arc::clone(&telemetry),
            Arc::new(DisconnectAwareExecutor {
                started: Arc::clone(&started),
                cancelled: Arc::clone(&cancelled),
            }),
        )
        .with_auth_enabled(false);
        tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            server.spawn_ws(stream, peer);
        });
        let request = Request::builder()
            .uri(format!("ws://{addr}/"))
            .header("Host", addr.to_string())
            .header("Upgrade", "websocket")
            .header("Connection", "Upgrade")
            .header("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
            .header("Sec-WebSocket-Version", "13")
            .body(())
            .unwrap();
        let (mut ws, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        ws.send(Message::Binary(
            vec![
                0x60, 0x60, 0xB0, 0x17, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x04, 0x03, 0x00, 0x00,
                0x04, 0x02, 0x00, 0x00, 0x04, 0x01,
            ]
            .into(),
        ))
        .await
        .unwrap();
        assert!(matches!(ws.next().await, Some(Ok(Message::Binary(_)))));
        let run = encode_bolt_struct(
            0x10,
            &[
                Value::String("RETURN 1".into()),
                Value::Map(vec![]),
                Value::Map(vec![]),
            ],
        );
        ws.send(Message::Binary(chunk_encode(&run).into()))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("WebSocket RUN started");
        ws.close(None).await.unwrap();
        drop(ws);

        tokio::time::timeout(Duration::from_secs(1), async {
            while !cancelled.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("executor observed WebSocket close cancellation");
        assert_eq!(
            telemetry
                .snapshot_metric("copperdb_request_cancellations_total")
                .unwrap()[0]
                .labels,
            vec![
                ("protocol".into(), "bolt".into()),
                ("reason".into(), "explicit".into()),
                ("stage".into(), "ingress".into()),
            ]
        );
    }

    #[tokio::test]
    async fn neo4rs_driver_executes_a_query_over_bolt_tcp() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let telemetry = Arc::new(Telemetry::new());
        telemetry.seed_zero_catalog_metrics();
        let server = BoltServer::new(address.to_string(), telemetry, Arc::new(MultiRowExecutor))
            .with_auth_enabled(false);
        let server_task = tokio::spawn(async move { server.serve_listener(listener).await });

        let graph = Graph::connect(
            ConfigBuilder::new()
                .uri(format!("bolt://{address}"))
                .user("neo4j")
                .password("password")
                .build()
                .unwrap(),
        )
        .unwrap();
        let mut result = tokio::time::timeout(
            Duration::from_secs(3),
            graph.execute(query("RETURN 1 AS value")),
        )
        .await
        .expect("Neo4rs query should not time out")
        .expect("Neo4rs query should succeed");
        let first: i64 = result
            .next()
            .await
            .expect("Neo4rs should return a record")
            .expect("Neo4rs result stream should succeed")
            .get("value")
            .expect("Bolt result should expose the value column");
        assert_eq!(first, 1);
        assert!(
            result
                .next()
                .await
                .expect("Neo4rs result stream should succeed")
                .is_some()
        );

        drop(result);
        drop(graph);
        server_task.abort();
    }

    #[tokio::test]
    async fn browser_neo4j_driver_executes_a_query_over_bolt_websocket() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let telemetry = Arc::new(Telemetry::new());
        telemetry.seed_zero_catalog_metrics();
        let server = BoltServer::new(address.to_string(), telemetry, Arc::new(MultiRowExecutor))
            .with_auth_enabled(false);
        let server_task = tokio::spawn(async move { server.serve_listener(listener).await });
        let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../ui/scripts/bolt-websocket-driver-e2e.mjs");
        let output = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::process::Command::new("node")
                .arg(script)
                .arg(format!("bolt://{address}"))
                .output(),
        )
        .await
        .expect("browser Neo4j driver should not time out")
        .expect("browser Neo4j driver process should start");

        server_task.abort();
        assert!(
            output.status.success(),
            "browser Neo4j driver failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
