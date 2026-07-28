//! Bolt server TCP listener and connection handler.
//!
//! Supports both raw TCP (standard Bolt) and WebSocket upgrades on the same port.
//! Mirrors NornicDB's `pkg/bolt/server.go` + `pkg/bolt/transport_ws.go`.

use crate::dispatch;
use crate::messages::BoltMessage;
use crate::packstream::Value;
use crate::wsconn;
use crate::BoltError;
use copperdb_otel::Telemetry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::accept_async;
use tracing::{debug, info, warn};

/// Result of executing a Cypher query through Bolt.
#[derive(Debug, Clone)]
pub struct BoltQueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
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
            });
        }
        // For MATCH ... RETURN queries, return empty rows with proper column names
        if upper.starts_with("MATCH ") || upper.starts_with("RETURN ") || upper.starts_with("CALL ")
        {
            return Ok(BoltQueryResult {
                columns: vec!["n".into()],
                rows: vec![],
            });
        }
        Ok(BoltQueryResult {
            columns: vec![],
            rows: vec![],
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
        }
    }

    pub fn with_auth_enabled(mut self, auth_enabled: bool) -> Self {
        self.config.auth_enabled = auth_enabled;
        self
    }

    pub fn auth_enabled(&self) -> bool {
        self.config.auth_enabled
    }

    pub async fn serve(&self) -> Result<(), BoltError> {
        let listener = TcpListener::bind(&self.listen_addr).await?;
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
                    let result =
                        handle_ws_session(&mut ws_stream, &telemetry, executor, auth_enabled).await;
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
            let result = handle_tcp_session(&mut stream, &telemetry, executor, auth_enabled).await;
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
    let mut read_buf = Vec::with_capacity(4096);
    let mut temp_buf = [0u8; 4096];

    loop {
        let bytes_read = stream.read(&mut temp_buf).await?;
        if bytes_read == 0 {
            break;
        }
        read_buf.extend_from_slice(&temp_buf[..bytes_read]);
        process_buffer(
            &mut read_buf,
            stream,
            &mut session,
            telemetry,
            Arc::clone(&executor),
        )
        .await?;
    }
    Ok(())
}

/// Handle a Bolt session over WebSocket.
async fn handle_ws_session<S>(
    ws: &mut tokio_tungstenite::WebSocketStream<S>,
    _telemetry: &Telemetry,
    executor: Arc<dyn QueryExecutor>,
    auth_enabled: bool,
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

    loop {
        let frames = match wsconn::read_ws_message(ws, &mut decoder).await {
            Some(Ok(messages)) => {
                info!(count = messages.len(), "bolt WS messages received");
                messages
            }
            Some(Err(e)) => {
                return Err(BoltError::ProtocolViolation(format!("WS read error: {e}")))
            }
            None => {
                info!("bolt WS connection closed");
                break;
            }
        };

        for frame in frames {
            let (value, consumed) = crate::packstream::decode(&frame)?;
            if consumed != frame.len() {
                return Err(BoltError::ProtocolViolation(format!(
                    "trailing bytes after Bolt message: {}",
                    frame.len() - consumed
                )));
            }

            match process_message(&value, &mut session, Arc::clone(&executor)).await {
                Ok(responses) => {
                    let has_responses = !responses.is_empty();
                    for response_bytes in &responses {
                        info!(len = response_bytes.len(), "bolt WS sending response");
                        wsconn::write_ws_message(ws, response_bytes)
                            .await
                            .map_err(|e| BoltError::ProtocolViolation(e.to_string()))?;
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
                    wsconn::write_ws_message(ws, &bytes)
                        .await
                        .map_err(|e| BoltError::ProtocolViolation(e.to_string()))?;
                }
            }
        }
    }
    Ok(())
}

/// Process buffered bytes, dispatching complete messages and writing responses.
async fn process_buffer(
    read_buf: &mut Vec<u8>,
    stream: &mut TcpStream,
    session: &mut BoltSession,
    telemetry: &Telemetry,
    executor: Arc<dyn QueryExecutor>,
) -> Result<(), BoltError> {
    loop {
        match crate::packstream::decode(read_buf) {
            Ok((value, consumed)) => {
                read_buf.drain(..consumed);
                let (op, result) = match process_message(&value, session, Arc::clone(&executor))
                    .await
                {
                    Ok(responses) => {
                        let len = responses.len();
                        for response_bytes in responses.iter() {
                            info!(len = response_bytes.len(), "bolt sending response");
                            stream.write_all(response_bytes).await?;
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
                        let fb = dispatch::encode_message(&failure);
                        stream.write_all(&fb).await?;
                        ("run", "error")
                    }
                };
                let _ = telemetry.record_counter(
                    "nornicdb_bolt_messages_total",
                    &[("op", op), ("result", result)],
                );
            }
            Err(BoltError::PackStream(ref msg))
                if msg.contains("unexpected end") || msg.contains("truncated") =>
            {
                break
            }
            Err(e) => return Err(e),
        }
        if read_buf.is_empty() {
            break;
        }
    }
    Ok(())
}

/// Per-connection Bolt session state. Mirrors NornicDB's Session struct.
struct BoltSession {
    auth_enabled: bool,
    authenticated: bool,
    database: Option<String>,
    last_query_database: Option<String>,
    current_query: Option<String>,
    last_result: Option<BoltQueryResult>,
    result_index: usize,
}

impl BoltSession {
    fn new(auth_enabled: bool) -> Self {
        Self {
            auth_enabled,
            authenticated: !auth_enabled,
            database: None,
            last_query_database: None,
            current_query: None,
            last_result: None,
            result_index: 0,
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

async fn process_message(
    value: &Value,
    session: &mut BoltSession,
    executor: Arc<dyn QueryExecutor>,
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
            session.authenticated = false;
            session.database = database_from_metadata(&extra);
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
        BoltMessage::Logon { auth: _ } => {
            session.authenticated = true;
            Ok(vec![dispatch::encode_message(&BoltMessage::Success {
                metadata: HashMap::from([("server".into(), serde_json::json!("copperdb/1.0"))]),
            })])
        }
        BoltMessage::Logoff => {
            session.authenticated = false;
            Ok(vec![dispatch::encode_message(&BoltMessage::Success {
                metadata: HashMap::new(),
            })])
        }
        BoltMessage::Run {
            query,
            parameters,
            extra,
        } => {
            if session.auth_enabled && !session.authenticated {
                return Ok(vec![dispatch::encode_message(&BoltMessage::Failure {
                    metadata: HashMap::from([
                        (
                            "code".into(),
                            serde_json::json!("Neo.ClientError.Security.Unauthorized"),
                        ),
                        (
                            "message".into(),
                            serde_json::json!("authentication required"),
                        ),
                    ]),
                })]);
            }
            session.current_query = Some(query.clone());
            let database = database_from_metadata(&extra).or_else(|| session.database.clone());
            let execution_database = database.clone();
            let execution_query = query.clone();
            let execution_parameters = parameters.clone();

            // Create request context OUTSIDE spawn_blocking so the guard
            // lives in the async scope.  When the client disconnects, the
            // guard is dropped → cancel fires → the BFS (or any long-running
            // query) observes RequestCancelled and aborts.
            let (request_context, _request_guard) =
                copperdb_util::RequestContext::root(None);

            let execution_result = tokio::task::spawn_blocking(move || {
                executor.execute_on_database_with_context(
                    execution_database.as_deref().unwrap_or("copperdb"),
                    &execution_query,
                    &execution_parameters,
                    request_context,
                )
            })
            .await
            .map_err(|error| {
                BoltError::ProtocolViolation(format!("bolt executor task failed: {error}"))
            })?;

            match execution_result {
                Ok(result) => {
                    let columns = result.columns.clone();
                    session.last_result = Some(result);
                    session.last_query_database = database;
                    session.result_index = 0;
                    let fields_json: Vec<serde_json::Value> = columns
                        .iter()
                        .map(|c| serde_json::Value::String(c.clone()))
                        .collect();
                    info!(%query, fields = ?columns, "bolt RUN executed");
                    Ok(vec![dispatch::encode_message(&BoltMessage::Success {
                        metadata: HashMap::from([
                            ("fields".into(), serde_json::json!(fields_json)),
                            ("t_first".into(), serde_json::json!(0)),
                            ("result_available_after".into(), serde_json::json!(0)),
                        ]),
                    })])
                }
                Err(e) => {
                    warn!(%query, %e, "bolt RUN failed");
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
        BoltMessage::Pull { .. } | BoltMessage::Discard { .. } => {
            let mut responses: Vec<Vec<u8>> = Vec::new();
            if let Some(ref result) = session.last_result {
                info!(rows = result.rows.len(), "bolt PULL → streaming records");
                // Send one RECORD message per row (rows are position-based Vecs)
                for row in &result.rows {
                    responses.push(dispatch::encode_message(&BoltMessage::Record {
                        data: row.clone(),
                    }));
                }
            } else {
                info!("bolt PULL → no prior result");
            }
            // Always send summary SUCCESS (matching NornicDB format:
            // no has_more when false, includes db and bookmark).
            responses.push(dispatch::encode_message(&BoltMessage::Success {
                metadata: HashMap::from([
                    ("type".into(), serde_json::json!("r")),
                    ("t_last".into(), serde_json::json!(0)),
                    ("bookmark".into(), serde_json::json!("")),
                    (
                        "db".into(),
                        serde_json::json!(session
                            .last_query_database
                            .as_deref()
                            .or(session.database.as_deref())
                            .unwrap_or("copperdb")),
                    ),
                ]),
            }));
            Ok(responses)
        }
        BoltMessage::Begin { extra: _ } => {
            Ok(vec![dispatch::encode_message(&BoltMessage::Success {
                metadata: HashMap::new(),
            })])
        }
        BoltMessage::Commit => Ok(vec![dispatch::encode_message(&BoltMessage::Success {
            metadata: HashMap::from([("bookmark".into(), serde_json::json!(""))]),
        })]),
        BoltMessage::Rollback => Ok(vec![dispatch::encode_message(&BoltMessage::Success {
            metadata: HashMap::new(),
        })]),
        BoltMessage::Reset => {
            session.current_query = None;
            Ok(vec![dispatch::encode_message(&BoltMessage::Success {
                metadata: HashMap::new(),
            })])
        }
        BoltMessage::Route { .. } => Ok(vec![dispatch::encode_message(&BoltMessage::Success {
            metadata: HashMap::new(),
        })]),
        _ => Ok(vec![]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[tokio::test]
    async fn run_requires_logon_when_authentication_is_enabled() {
        let mut session = BoltSession::new(true);
        let responses = process_message(&run_message(), &mut session, Arc::new(NoopExecutor))
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
        let responses = process_message(&run_message(), &mut session, Arc::new(NoopExecutor))
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
            let result = handle_tcp_session(&mut stream, &server_telemetry, exec, false).await;
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
        client.write_all(&hello_bytes).await.unwrap();

        // Read SUCCESS response
        let mut resp_buf = Vec::new();
        let mut tmp = [0u8; 4096];
        let n = client.read(&mut tmp).await.unwrap();
        assert!(n > 0, "should receive SUCCESS response for HELLO");
        resp_buf.extend_from_slice(&tmp[..n]);

        let (value, _consumed) = crate::packstream::decode(&resp_buf).unwrap();
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
        client.write_all(&run_bytes).await.unwrap();

        // Read SUCCESS for RUN
        resp_buf.clear();
        let n = client.read(&mut tmp).await.unwrap();
        assert!(n > 0, "should receive SUCCESS for RUN");
        resp_buf.extend_from_slice(&tmp[..n]);
        let (value, _) = crate::packstream::decode(&resp_buf).unwrap();
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
        client.write_all(&pull_bytes).await.unwrap();

        // Read SUCCESS for PULL (stream done)
        resp_buf.clear();
        let n = client.read(&mut tmp).await.unwrap();
        assert!(n > 0, "should receive SUCCESS for PULL");
        resp_buf.extend_from_slice(&tmp[..n]);
        let (value, _) = crate::packstream::decode(&resp_buf).unwrap();
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
            let result = handle_tcp_session(&mut stream, &st, exec, false).await;
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
            let result = handle_tcp_session(&mut stream, &st, exec, false).await;
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
}
