//! Bolt server TCP listener and connection handler.
//!
//! Supports both raw TCP (standard Bolt) and WebSocket upgrades on the same port.
//! Mirrors NornicDB's `pkg/bolt/server.go` + `pkg/bolt/transport_ws.go`.

use crate::dispatch;
use crate::messages::BoltMessage;
use crate::packstream::Value;
use crate::wsconn;
use crate::BoltError;
use copperdb_errors::{map_transient_transaction_error, TransientTransactionCode};
use copperdb_otel::Telemetry;
use std::error::Error;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::accept_async;
use tracing::{debug, info, warn};

const BOLT_RECEIVE_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_BOLT_CURSORS: usize = 64;

/// Result of executing a Cypher query through Bolt.
#[derive(Debug, Clone)]
pub struct BoltQueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub stats: BoltResultStats,
    pub notifications: Vec<BoltNotification>,
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
    ) -> Result<BoltQueryResult, String> {
        let _ = request_context;
        self.execute_on_database(Some(database), query, params)
    }

    fn execute_as_on_database_with_context(
        &self,
        database: &str,
        query: &str,
        params: &HashMap<String, serde_json::Value>,
        request_context: copperdb_util::RequestContext,
        _principal: Option<&BoltPrincipal>,
    ) -> Result<BoltQueryResult, String> {
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
    ) -> Result<BoltQueryResult, String> {
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
    ) -> Result<BoltQueryResult, String> {
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
                stats: BoltResultStats::default(),
                notifications: vec![],
            });
        }
        Ok(BoltQueryResult {
            columns: vec![],
            rows: vec![],
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

#[derive(Clone)]
pub struct BoltServer {
    pub listen_addr: String,
    config: BoltServerConfig,
    telemetry: Arc<Telemetry>,
    active_connections: Arc<AtomicU64>,
    executor: Arc<dyn QueryExecutor>,
    auth_provider: Option<Arc<dyn BoltAuthProvider>>,
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
            active_connections: Arc::new(AtomicU64::new(0)),
            executor,
            auth_provider: None,
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
        let active_connections = Arc::clone(&self.active_connections);
        let executor = Arc::clone(&self.executor);
        let auth_enabled = self.config.auth_enabled;
        let auth_provider = self.auth_provider.clone();
        let _ = telemetry.record_counter(
            "nornicdb_bolt_connections_total",
            &[("result", "success"), ("transport", "ws")],
        );
        let active = active_connections.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = telemetry.set_gauge(
            "nornicdb_bolt_connections_active",
            &[("transport", "ws")],
            active as f64,
        );

        tokio::spawn(async move {
            match accept_async(stream).await {
                Ok(mut ws_stream) => {
                    let result = handle_ws_session(
                        &mut ws_stream,
                        &telemetry,
                        executor,
                        auth_enabled,
                        auth_provider,
                    )
                    .await;
                    if let Err(ref e) = result {
                        warn!(%peer_addr, %e, "bolt ws connection failed");
                        let _ = telemetry.record_counter(
                            "nornicdb_bolt_connections_total",
                            &[("result", "error"), ("transport", "ws")],
                        );
                    }
                }
                Err(e) => {
                    warn!(%peer_addr, %e, "ws upgrade failed");
                }
            }
            let active = active_connections.fetch_sub(1, Ordering::SeqCst) - 1;
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
        let started = std::time::Instant::now();
        let telemetry = Arc::clone(&self.telemetry);
        let active_connections = Arc::clone(&self.active_connections);
        let executor = Arc::clone(&self.executor);
        let auth_enabled = self.config.auth_enabled;
        let auth_provider = self.auth_provider.clone();
        let _ = telemetry.record_counter(
            "nornicdb_bolt_connections_total",
            &[("result", "success"), ("transport", "tcp")],
        );
        let active = active_connections.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = telemetry.set_gauge(
            "nornicdb_bolt_connections_active",
            &[("transport", "tcp")],
            active as f64,
        );
        debug!(%peer_addr, "accepted bolt tcp");

        tokio::spawn(async move {
            let result = handle_tcp_session(
                &mut stream,
                &telemetry,
                executor,
                auth_enabled,
                auth_provider,
            )
            .await;
            if let Err(ref e) = result {
                let _ = telemetry.record_counter(
                    "nornicdb_bolt_connections_total",
                    &[("result", "error"), ("transport", "tcp")],
                );
                warn!(%peer_addr, %e, "bolt tcp failed");
            }
            let active = active_connections.fetch_sub(1, Ordering::SeqCst) - 1;
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

async fn handle_tcp_session_with_timeout(
    stream: &mut TcpStream,
    telemetry: &Telemetry,
    executor: Arc<dyn QueryExecutor>,
    auth_enabled: bool,
    auth_provider: Option<Arc<dyn BoltAuthProvider>>,
    receive_timeout: Duration,
) -> Result<(), BoltError> {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    info!(%peer, "bolt tcp session started");
    let mut preamble = [0u8; 20];
    stream.read_exact(&mut preamble).await?;
    if preamble[..4] != [0x60, 0x60, 0xB0, 0x17] {
        return Err(BoltError::ProtocolViolation(
            "invalid bolt magic preamble".into(),
        ));
    }
    stream.write_all(&[0x00, 0x00, 0x04, 0x04]).await?;
    info!(%peer, "bolt tcp version 4.4 sent, entering message loop");

    let mut session = BoltSession::new(auth_enabled);
    let mut decoder = wsconn::BoltChunkDecoder::new();
    let mut temp_buf = [0u8; 4096];

    let result = 'session_loop: loop {
        let bytes_read = match tokio::time::timeout(receive_timeout, stream.read(&mut temp_buf)).await {
            Ok(Ok(bytes_read)) => bytes_read,
            Ok(Err(error)) => break Err(error.into()),
            Err(_) => break Err(BoltError::ProtocolViolation("Bolt receive timeout".into())),
        };
        if bytes_read == 0 {
            break Ok(());
        }
        for frame in decoder.push(&temp_buf[..bytes_read]) {
            if let Err(error) = process_buffer(
                &frame,
                stream,
                &mut session,
                telemetry,
                Arc::clone(&executor),
                auth_provider.clone(),
            )
            .await
            {
                break 'session_loop Err(error);
            }
        }
    };
    rollback_active_transaction(&mut session, executor.as_ref());
    result
}

/// Handle a Bolt session over WebSocket.
async fn handle_ws_session<S>(
    ws: &mut tokio_tungstenite::WebSocketStream<S>,
    _telemetry: &Telemetry,
    executor: Arc<dyn QueryExecutor>,
    auth_enabled: bool,
    auth_provider: Option<Arc<dyn BoltAuthProvider>>,
) -> Result<(), BoltError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    handle_ws_session_with_timeout(
        ws,
        _telemetry,
        executor,
        auth_enabled,
        auth_provider,
        BOLT_RECEIVE_TIMEOUT,
    )
    .await
}

async fn handle_ws_session_with_timeout<S>(
    ws: &mut tokio_tungstenite::WebSocketStream<S>,
    _telemetry: &Telemetry,
    executor: Arc<dyn QueryExecutor>,
    auth_enabled: bool,
    auth_provider: Option<Arc<dyn BoltAuthProvider>>,
    receive_timeout: Duration,
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
                )))
            }
            Some(Err(e)) => {
                return Err(BoltError::ProtocolViolation(format!(
                    "WS preamble read error: {e}"
                )))
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
    info!("bolt WS preamble OK, sending version 4.4");
    wsconn::write_ws_raw(ws, &[0x00, 0x00, 0x04, 0x04])
        .await
        .map_err(|e| BoltError::ProtocolViolation(format!("WS version response error: {e}")))?;
    info!("bolt WS version response sent, entering message loop");

    let mut session = BoltSession::new(auth_enabled);
    let mut decoder = wsconn::BoltChunkDecoder::new();

    let result = 'session_loop: loop {
        let frames = match tokio::time::timeout(
            receive_timeout,
            wsconn::read_ws_message(ws, &mut decoder),
        )
        .await
        {
            Ok(Some(Ok(messages))) => {
                info!(count = messages.len(), "bolt WS messages received");
                messages
            }
            Ok(Some(Err(e))) => {
                break 'session_loop Err(BoltError::ProtocolViolation(format!("WS read error: {e}")))
            }
            Ok(None) => {
                info!("bolt WS connection closed");
                break 'session_loop Ok(());
            }
            Err(_) => break 'session_loop Err(BoltError::ProtocolViolation("Bolt receive timeout".into())),
        };

        for frame in frames {
            let (value, consumed) = match crate::packstream::decode(&frame) {
                Ok(decoded) => decoded,
                Err(error) => break 'session_loop Err(error),
            };
            if consumed != frame.len() {
                break 'session_loop Err(BoltError::ProtocolViolation(format!(
                    "trailing bytes after Bolt message: {}",
                    frame.len() - consumed
                )));
            }

            match process_message(
                &value,
                &mut session,
                Arc::clone(&executor),
                auth_provider.clone(),
            )
            .await
            {
                Ok(responses) => {
                    let has_responses = !responses.is_empty();
                    for response_bytes in &responses {
                        info!(len = response_bytes.len(), "bolt WS sending response");
                        if let Err(error) = wsconn::write_ws_message(ws, response_bytes).await {
                            break 'session_loop Err(BoltError::ProtocolViolation(error.to_string()));
                        }
                    }
                    if has_responses {
                        info!("bolt WS responses sent");
                    }
                }
                Err(e) => {
                    let failure = BoltMessage::Failure {
                        metadata: HashMap::from([
                            (
                                "code".into(),
                                serde_json::json!("Neo.TransientError.General.UnknownError"),
                            ),
                            ("message".into(), serde_json::json!(e.to_string())),
                        ]),
                    };
                    let bytes = dispatch::encode_message(&failure);
                    if let Err(error) = wsconn::write_ws_message(ws, &bytes).await {
                        break 'session_loop Err(BoltError::ProtocolViolation(error.to_string()));
                    }
                }
            }
        }
    };
    rollback_active_transaction(&mut session, executor.as_ref());
    result
}

/// Process one decoded Bolt chunk frame and write chunk-framed responses.
async fn process_buffer(
    frame: &[u8],
    stream: &mut TcpStream,
    session: &mut BoltSession,
    telemetry: &Telemetry,
    executor: Arc<dyn QueryExecutor>,
    auth_provider: Option<Arc<dyn BoltAuthProvider>>,
) -> Result<(), BoltError> {
    let (value, consumed) = crate::packstream::decode(frame)?;
    if consumed != frame.len() {
        return Err(BoltError::ProtocolViolation(format!(
            "trailing bytes after Bolt message: {}",
            frame.len() - consumed
        )));
    }
    let (op, result) = match process_message(
        &value,
        session,
        Arc::clone(&executor),
        auth_provider,
    )
    .await
    {
        Ok(responses) => {
            let len = responses.len();
            for response_bytes in responses {
                info!(len = response_bytes.len(), "bolt sending response");
                stream
                    .write_all(&wsconn::encode_bolt_chunks(&response_bytes))
                    .await?;
            }
            if len > 0 {
                info!("bolt TCP responses sent");
            }
            ("run", "success")
        }
        Err(e) => {
            let failure = BoltMessage::Failure {
                metadata: HashMap::from([
                    (
                        "code".into(),
                        serde_json::json!("Neo.TransientError.General.UnknownError"),
                    ),
                    ("message".into(), serde_json::json!(e.to_string())),
                ]),
            };
            let response = dispatch::encode_message(&failure);
            stream
                .write_all(&wsconn::encode_bolt_chunks(&response))
                .await?;
            ("run", "error")
        }
    };
    let _ = telemetry.record_counter(
        "nornicdb_bolt_messages_total",
        &[("op", op), ("result", result)],
    );
    Ok(())
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
}

struct BoltCursor {
    result: BoltQueryResult,
    index: usize,
    database: Option<String>,
}

impl BoltSession {
    fn new(auth_enabled: bool) -> Self {
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
        }
    }
}

fn database_from_metadata(metadata: &HashMap<String, serde_json::Value>) -> Option<String> {
    metadata
        .get("db")
        .or_else(|| metadata.get("database"))
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
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
    if let Some(transaction) = session.transaction.take() {
        if let Err(error) = executor.rollback_transaction(&transaction) {
            warn!(%error, transaction_id = %transaction.id, "Bolt session cleanup rollback failed");
        }
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
            serde_json::json!(database
                .or(session.last_query_database.as_deref())
                .or(session.database.as_deref())
                .unwrap_or("copperdb")),
        ),
    ]);
    if has_more {
        metadata.insert("has_more".into(), serde_json::json!(true));
    }
    if let Some(bookmark) = &session.last_bookmark {
        metadata.insert("bookmark".into(), serde_json::json!(bookmark));
    }
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
    if !notifications.is_empty() {
        metadata.insert(
            "notifications".into(),
            serde_json::json!(notifications
                .iter()
                .map(|notification| serde_json::json!({
                    "code": notification.code,
                    "title": notification.title,
                    "description": notification.description,
                    "severity": notification.severity,
                    "category": notification.category,
                }))
                .collect::<Vec<_>>()),
        );
    }
    vec![dispatch::encode_message(&BoltMessage::Success { metadata })]
}

fn transaction_failure_response(code: &str, message: &str) -> Vec<Vec<u8>> {
    client_failure_response(code, message)
}

async fn process_message(
    value: &Value,
    session: &mut BoltSession,
    executor: Arc<dyn QueryExecutor>,
    auth_provider: Option<Arc<dyn BoltAuthProvider>>,
) -> Result<Vec<Vec<u8>>, BoltError> {
    let (sig, fields) = match value {
        Value::Struct { signature, fields } => (*signature, fields.as_slice()),
        _ => {
            return Err(BoltError::ProtocolViolation(
                "expected struct message".into(),
            ))
        }
    };
    let msg = dispatch::decode_message(sig, fields)?;
    info!(signature = format!("0x{sig:02X}"), "bolt message received");
    match msg {
        BoltMessage::Hello { extra } => {
            session.authenticated = !session.auth_enabled;
            session.principal = None;
            session.database = database_from_metadata(&extra);
            if session.auth_enabled {
                if let Some((username, password)) = credentials_from_hello(&extra) {
                    return Ok(
                        match authenticate_session(
                            session,
                            auth_provider.as_deref(),
                            &username,
                            &password,
                        ) {
                            Ok(()) => success_response(),
                            Err(message) => authentication_failure_response(&message),
                        },
                    );
                }
            }
            let meta = HashMap::from([
                ("server".into(), serde_json::json!("copperdb/1.0")),
                ("connection_id".into(), serde_json::json!("copperdb-1")),
                (
                    "hints".into(),
                    serde_json::json!({"connection.recv_timeout_seconds": 120}),
                ),
                ("patch_bolt".into(), serde_json::json!(["utc"])),
            ]);
            info!("bolt HELLO → SUCCESS");
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
                        Err(message) => Ok(authentication_failure_response(&message)),
                    }
                }
                _ => Ok(authentication_failure_response("missing Bolt credentials")),
            }
        }
        BoltMessage::Logoff => {
            if let Some(transaction) = session.transaction.take() {
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
                return Ok(authentication_failure_response("authentication required"));
            }
            session.current_query = Some(query.clone());
            let requested_database = database_from_metadata(&extra);
            if let (Some(transaction), Some(requested_database)) =
                (session.transaction.as_ref(), requested_database.as_ref())
            {
                if requested_database != &transaction.database {
                    return Ok(transaction_failure_response(
                        "Neo.ClientError.Transaction.TransactionAccessedConcurrently",
                        "cannot change database during an active transaction",
                    ));
                }
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

            // Create request context OUTSIDE spawn_blocking so the guard
            // lives in the async scope.  When the client disconnects, the
            // guard is dropped → cancel fires → the BFS (or any long-running
            // query) observes RequestCancelled and aborts.
            let (request_context, _request_guard) = copperdb_util::RequestContext::root(None);

            let execution_result =
                tokio::task::spawn_blocking(move || match transaction.as_ref() {
                    Some(transaction) => execution_executor.execute_in_transaction_with_context(
                        transaction,
                        &execution_query,
                        &execution_parameters,
                        request_context,
                        principal.as_ref(),
                    ),
                    None => execution_executor.execute_as_on_database_with_context_and_bookmarks(
                        execution_database.as_deref().unwrap_or("copperdb"),
                        &execution_query,
                        &execution_parameters,
                        request_context,
                        principal.as_ref(),
                        &bookmarks,
                    ),
                })
                .await
                .map_err(|error| {
                    BoltError::ProtocolViolation(format!("bolt executor task failed: {error}"))
                })?;

            match execution_result {
                Ok(result) => {
                    let columns = result.columns.clone();
                    if session.cursors.len() == MAX_BOLT_CURSORS {
                        if let Some(oldest_qid) = session.cursors.keys().min().copied() {
                            session.cursors.remove(&oldest_qid);
                        }
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
                    info!(%query, fields = ?columns, "bolt RUN executed");
                    Ok(vec![dispatch::encode_message(&BoltMessage::Success {
                        metadata: HashMap::from([
                            ("fields".into(), serde_json::json!(fields_json)),
                            ("qid".into(), serde_json::json!(qid)),
                            ("t_first".into(), serde_json::json!(0)),
                            ("result_available_after".into(), serde_json::json!(0)),
                        ]),
                    })])
                }
                Err(e) => {
                    warn!(%query, %e, "bolt RUN failed");
                    rollback_active_transaction(session, executor.as_ref());
                    Ok(vec![dispatch::encode_message(&BoltMessage::Failure {
                        metadata: HashMap::from([
                            (
                                "code".into(),
                                serde_json::json!("Neo.ClientError.Statement.ExecutionFailed"),
                            ),
                            ("message".into(), serde_json::json!(e)),
                        ]),
                    })])
                }
            }
        }
        BoltMessage::Pull { n, qid } | BoltMessage::Discard { n, qid } => {
            let Some(qid) = cursor_qid(session, qid) else {
                return Ok(client_failure_response(
                    "Neo.ClientError.Request.Invalid",
                    "no Bolt result cursor is active",
                ));
            };
            let pull = matches!(msg, BoltMessage::Pull { .. });
            let Some(cursor) = session.cursors.get_mut(&qid) else {
                return Ok(client_failure_response(
                    "Neo.ClientError.Request.Invalid",
                    "unknown Bolt result cursor",
                ));
            };
            let end = cursor.index + cursor_limit(n, cursor.result.rows.len() - cursor.index);
            let rows = if pull {
                cursor.result.rows[cursor.index..end].to_vec()
            } else {
                Vec::new()
            };
            cursor.index = end;
            let has_more = cursor.index < cursor.result.rows.len();
            let database = cursor.database.clone();
            let stats = cursor.result.stats.clone();
            let notifications = cursor.result.notifications.clone();
            if !has_more {
                session.cursors.remove(&qid);
                if session.last_qid == Some(qid) {
                    session.last_qid = session.cursors.keys().max().copied();
                }
            }
            let mut responses: Vec<Vec<u8>> = rows
                .into_iter()
                .map(|row| dispatch::encode_message(&BoltMessage::Record { data: row }))
                .collect();
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
                return Ok(authentication_failure_response("authentication required"));
            }
            if session.transaction.is_some() {
                return Ok(transaction_failure_response(
                    "Neo.ClientError.Transaction.TransactionStartFailed",
                    "transaction already active",
                ));
            }
            let database = database_from_metadata(&extra)
                .or_else(|| session.database.clone())
                .unwrap_or_else(|| "copperdb".into());
            match executor.begin_transaction(&database, &extra, session.principal.as_ref()) {
                Ok(transaction) => {
                    session.database = Some(transaction.database.clone());
                    session.transaction = Some(transaction);
                    Ok(vec![dispatch::encode_message(&BoltMessage::Success {
                        metadata: HashMap::new(),
                    })])
                }
                Err(message) => Ok(transaction_failure_response(
                    "Neo.ClientError.Transaction.TransactionStartFailed",
                    &message,
                )),
            }
        }
        BoltMessage::Commit => {
            if authentication_required(session) {
                return Ok(authentication_failure_response("authentication required"));
            }
            let Some(transaction) = session.transaction.take() else {
                return Ok(transaction_failure_response(
                    "Neo.ClientError.Transaction.TransactionNotFound",
                    "no transaction to commit",
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
                    &error.to_string(),
                )),
            }
        }
        BoltMessage::Rollback => {
            if authentication_required(session) {
                return Ok(authentication_failure_response("authentication required"));
            }
            let Some(transaction) = session.transaction.take() else {
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
                    &message,
                )),
            }
        }
        BoltMessage::Reset => {
            if let Some(transaction) = session.transaction.take() {
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
                return Ok(authentication_failure_response("authentication required"));
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
    use neo4rs::{query, ConfigBuilder, Graph};
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

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
        ) -> Result<BoltQueryResult, String> {
            *self.bookmarks.lock().unwrap() = bookmarks.to_vec();
            self.execute("", &HashMap::new())
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
                rows: vec![
                    vec![serde_json::json!(1)],
                    vec![serde_json::json!(2)],
                    vec![serde_json::json!(3)],
                ],
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
        ) -> Result<BoltQueryResult, String> {
            self.execute(query, params)
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
        ) -> Result<BoltQueryResult, String> {
            *self.principal.lock().unwrap() = principal.cloned();
            self.execute("", &HashMap::new())
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
        ) -> Result<BoltQueryResult, String> {
            self.operations.lock().unwrap().push("run");
            self.execute("", &HashMap::new())
        }
    }

    struct FailingTransactionExecutor {
        operations: Arc<Mutex<Vec<&'static str>>>,
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
        ) -> Result<BoltQueryResult, String> {
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
        let code = metadata
            .iter()
            .find_map(|(key, value)| (key == "code").then_some(value))
            .expect("expected Bolt failure code");
        let Value::String(code) = code else {
            panic!("expected Bolt failure code string");
        };
        code.clone()
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
        assert_eq!(*operations.lock().unwrap(), vec!["begin", "run", "rollback"]);
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
        assert!(metadata.iter().any(|(key, value)| {
            key == "qid" && value == &Value::Integer(0)
        }));

        let pull = Value::Struct {
            signature: 0x3F,
            fields: vec![Value::Integer(1), Value::Integer(0)],
        };
        let pulled = process_message(&pull, &mut session, Arc::clone(&executor), None)
            .await
            .unwrap();
        assert_eq!(pulled.len(), 2);
        let (record, _) = crate::packstream::decode(&pulled[0]).unwrap();
        assert!(matches!(record, Value::Struct { signature: 0x71, .. }));
        let (summary, _) = crate::packstream::decode(&pulled[1]).unwrap();
        let Value::Struct { fields, .. } = summary else {
            panic!("expected PULL summary");
        };
        let [Value::Map(metadata)] = fields.as_slice() else {
            panic!("expected PULL summary metadata");
        };
        assert!(metadata.iter().any(|(key, value)| {
            key == "has_more" && value == &Value::Bool(true)
        }));

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
        assert!(counters.iter().any(|(key, value)| {
            key == "nodes-created" && value == &Value::Integer(2)
        }));
        assert!(counters.iter().any(|(key, value)| {
            key == "relationships-created" && value == &Value::Integer(1)
        }));
        assert!(counters.iter().any(|(key, value)| {
            key == "relationships-deleted" && value == &Value::Integer(1)
        }));
        assert!(counters.iter().any(|(key, value)| {
            key == "properties-set" && value == &Value::Integer(3)
        }));
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
                    == &Value::String(
                        "Neo.ClientNotification.Statement.UnknownLabelWarning".into(),
                    )
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
                0x60, 0x60, 0xB0, 0x17, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x04, 0x03, 0x00,
                0x00, 0x04, 0x02, 0x00, 0x00, 0x04, 0x01,
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
                0x60, 0x60, 0xB0, 0x17, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x04, 0x03, 0x00,
                0x00, 0x04, 0x02, 0x00, 0x00, 0x04, 0x01,
            ])
            .await
            .unwrap();
        let mut version = [0u8; 4];
        client.read_exact(&mut version).await.unwrap();

        let begin = encode_bolt_struct(0x11, &[Value::Map(vec![])]);
        client.write_all(&chunk_encode(&begin)).await.unwrap();
        let mut response = [0u8; 128];
        assert!(client.read(&mut response).await.unwrap() > 0);

        let error = handler.await.unwrap().expect_err("idle session should time out");
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
    async fn neo4rs_driver_executes_a_query_over_bolt_tcp() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let telemetry = Arc::new(Telemetry::new());
        telemetry.seed_zero_catalog_metrics();
        let server = BoltServer::new(
            address.to_string(),
            telemetry,
            Arc::new(MultiRowExecutor),
        )
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
        assert!(result
            .next()
            .await
            .expect("Neo4rs result stream should succeed")
            .is_some());

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
        let server = BoltServer::new(
            address.to_string(),
            telemetry,
            Arc::new(MultiRowExecutor),
        )
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
