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

#[derive(Debug, Clone)]
pub struct BoltServerConfig {
    pub listen_addr: String,
    pub web_socket_enabled: bool,
}

impl Default for BoltServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:7687".into(),
            web_socket_enabled: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BoltServer {
    pub listen_addr: String,
    config: BoltServerConfig,
    telemetry: Arc<Telemetry>,
    active_connections: Arc<AtomicU64>,
}

impl BoltServer {
    pub fn new(listen_addr: impl Into<String>, telemetry: Arc<Telemetry>) -> Self {
        let addr = listen_addr.into();
        Self {
            listen_addr: addr.clone(),
            config: BoltServerConfig {
                listen_addr: addr,
                web_socket_enabled: true,
            },
            telemetry,
            active_connections: Arc::new(AtomicU64::new(0)),
        }
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
        let _ = telemetry.record_counter(
            "nornicdb_bolt_connections_total",
            &[("result", "success"), ("transport", "ws")],
        );
        let active = active_connections.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = telemetry.set_gauge("nornicdb_bolt_connections_active", &[("transport", "ws")], active as f64);

        tokio::spawn(async move {
            match accept_async(stream).await {
                Ok(mut ws_stream) => {
                    let result = handle_ws_session(&mut ws_stream, &telemetry).await;
                    if let Err(ref e) = result {
                        warn!(%peer_addr, %e, "bolt ws connection failed");
                        let _ = telemetry.record_counter("nornicdb_bolt_connections_total", &[("result", "error"), ("transport", "ws")]);
                    }
                }
                Err(e) => {
                    warn!(%peer_addr, %e, "ws upgrade failed");
                }
            }
            let active = active_connections.fetch_sub(1, Ordering::SeqCst) - 1;
            let _ = telemetry.set_gauge("nornicdb_bolt_connections_active", &[("transport", "ws")], active as f64);
            let _ = telemetry.observe_histogram("nornicdb_bolt_session_duration_seconds", &[("transport", "ws")], started.elapsed().as_secs_f64());
        });
    }

    fn spawn_tcp(&self, mut stream: TcpStream, peer_addr: std::net::SocketAddr) {
        let started = std::time::Instant::now();
        let telemetry = Arc::clone(&self.telemetry);
        let active_connections = Arc::clone(&self.active_connections);
        let _ = telemetry.record_counter("nornicdb_bolt_connections_total", &[("result", "success"), ("transport", "tcp")]);
        let active = active_connections.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = telemetry.set_gauge("nornicdb_bolt_connections_active", &[("transport", "tcp")], active as f64);
        debug!(%peer_addr, "accepted bolt tcp");

        tokio::spawn(async move {
            let result = handle_tcp_session(&mut stream, &telemetry).await;
            if let Err(ref e) = result {
                let _ = telemetry.record_counter("nornicdb_bolt_connections_total", &[("result", "error"), ("transport", "tcp")]);
                warn!(%peer_addr, %e, "bolt tcp failed");
            }
            let active = active_connections.fetch_sub(1, Ordering::SeqCst) - 1;
            let _ = telemetry.set_gauge("nornicdb_bolt_connections_active", &[("transport", "tcp")], active as f64);
            let _ = telemetry.observe_histogram("nornicdb_bolt_session_duration_seconds", &[("transport", "tcp")], started.elapsed().as_secs_f64());
        });
    }
}

/// Handle a Bolt session over raw TCP.
async fn handle_tcp_session(stream: &mut TcpStream, telemetry: &Telemetry) -> Result<(), BoltError> {
    let peer = stream.peer_addr().map(|a| a.to_string()).unwrap_or_default();
    info!(%peer, "bolt tcp session started");
    let mut preamble = [0u8; 20];
    stream.read_exact(&mut preamble).await?;
    if preamble[..4] != [0x60, 0x60, 0xB0, 0x17] {
        return Err(BoltError::ProtocolViolation("invalid bolt magic preamble".into()));
    }
    stream.write_all(&[0x00, 0x00, 0x04, 0x04]).await?;
    info!(%peer, "bolt tcp version 4.4 sent, entering message loop");

    let mut read_buf = Vec::with_capacity(4096);
    let mut temp_buf = [0u8; 4096];
    let mut authenticated = false;
    let mut current_query: Option<String> = None;

    loop {
        let bytes_read = stream.read(&mut temp_buf).await?;
        if bytes_read == 0 { break; }
        read_buf.extend_from_slice(&temp_buf[..bytes_read]);
        process_buffer(&mut read_buf, stream, &mut authenticated, &mut current_query, telemetry).await?;
    }
    Ok(())
}

/// Handle a Bolt session over WebSocket.
async fn handle_ws_session<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>, _telemetry: &Telemetry) -> Result<(), BoltError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    // The first WS binary message after upgrade is the Bolt preamble
    // (magic 0x60 0x60 0xB0 0x17 + four 4-byte version proposals = 20 bytes).
    // We must read and respond before entering the PackStream message loop.
    let preamble = match wsconn::read_ws_message(ws).await {
        Some(Ok(data)) => data,
        Some(Err(e)) => return Err(BoltError::ProtocolViolation(format!("WS preamble read error: {e}"))),
        None => return Ok(()),
    };
    if preamble.len() < 4 || preamble[..4] != [0x60, 0x60, 0xB0, 0x17] {
        return Err(BoltError::ProtocolViolation("invalid bolt magic preamble on WS".into()));
    }
    // Respond with Bolt 4.4
    info!("bolt WS preamble OK, sending version 4.4");
    wsconn::write_ws_message(ws, &[0x00, 0x00, 0x04, 0x04]).await
        .map_err(|e| BoltError::ProtocolViolation(format!("WS version response error: {e}")))?;
    info!("bolt WS version response sent, entering message loop");

    let mut authenticated = false;
    let mut current_query: Option<String> = None;

    loop {
        let frame = match wsconn::read_ws_message(ws).await {
            Some(Ok(data)) => {
                info!(len = data.len(), "bolt WS frame received");
                data
            }
            Some(Err(e)) => return Err(BoltError::ProtocolViolation(format!("WS read error: {e}"))),
            None => { info!("bolt WS connection closed"); break; }
        };

        let mut read_buf = frame;
        loop {
            match crate::packstream::decode(&read_buf) {
                Ok((value, consumed)) => {
                    read_buf.drain(..consumed);
                    match process_message(&value, &mut authenticated, &mut current_query) {
                        Ok(Some(response_bytes)) => {
                            info!(len = response_bytes.len(), "bolt WS sending response");
                            wsconn::write_ws_message(ws, &response_bytes).await.map_err(|e| BoltError::ProtocolViolation(e.to_string()))?;
                            info!("bolt WS response sent");
                        }
                        Ok(None) => {}
                        Err(e) => {
                            let failure = BoltMessage::Failure {
                                metadata: HashMap::from([
                                    ("code".into(), serde_json::json!("Neo.TransientError.General.UnknownError")),
                                    ("message".into(), serde_json::json!(e.to_string())),
                                ]),
                            };
                            let bytes = dispatch::encode_message(&failure);
                            wsconn::write_ws_message(ws, &bytes).await.map_err(|e| BoltError::ProtocolViolation(e.to_string()))?;
                        }
                    }
                }
                Err(BoltError::PackStream(ref msg)) if msg.contains("unexpected end") || msg.contains("truncated") => break,
                Err(e) => return Err(e),
            }
            if read_buf.is_empty() { break; }
        }
    }
    Ok(())
}

/// Process buffered bytes, dispatching complete messages and writing responses.
async fn process_buffer(
    read_buf: &mut Vec<u8>,
    stream: &mut TcpStream,
    authenticated: &mut bool,
    current_query: &mut Option<String>,
    telemetry: &Telemetry,
) -> Result<(), BoltError> {
    loop {
        match crate::packstream::decode(read_buf) {
            Ok((value, consumed)) => {
                read_buf.drain(..consumed);
                let (op, result) = match process_message(&value, authenticated, current_query) {
                    Ok(Some(response_bytes)) => {
                        info!(len = response_bytes.len(), "bolt sending response");
                        stream.write_all(&response_bytes).await?;
                        ("run", "success")
                    }
                    Ok(None) => ("run", "success"),
                    Err(e) => {
                        let failure = BoltMessage::Failure {
                            metadata: HashMap::from([
                                ("code".into(), serde_json::json!("Neo.TransientError.General.UnknownError")),
                                ("message".into(), serde_json::json!(e.to_string())),
                            ]),
                        };
                        let fb = dispatch::encode_message(&failure);
                        stream.write_all(&fb).await?;
                        ("run", "error")
                    }
                };
                let _ = telemetry.record_counter("nornicdb_bolt_messages_total", &[("op", op), ("result", result)]);
            }
            Err(BoltError::PackStream(ref msg)) if msg.contains("unexpected end") || msg.contains("truncated") => break,
            Err(e) => return Err(e),
        }
        if read_buf.is_empty() { break; }
    }
    Ok(())
}

fn process_message(
    value: &Value,
    authenticated: &mut bool,
    current_query: &mut Option<String>,
) -> Result<Option<Vec<u8>>, BoltError> {
    let (sig, fields) = match value {
        Value::Struct { signature, fields } => (*signature, fields.as_slice()),
        _ => return Err(BoltError::ProtocolViolation("expected struct message".into())),
    };
    let msg = dispatch::decode_message(sig, fields)?;
    info!(signature = format!("0x{sig:02X}"), "bolt message received");
    match msg {
        BoltMessage::Hello { extra: _ } => {
            *authenticated = false;
            let meta = HashMap::from([
                ("server".into(), serde_json::json!("copperdb/1.0")),
                ("connection_id".into(), serde_json::json!("copperdb-1")),
                ("hints".into(), serde_json::json!({})),
                ("patch_bolt".into(), serde_json::json!(["utc"])),
            ]);
            info!("bolt HELLO → SUCCESS");
            Ok(Some(dispatch::encode_message(&BoltMessage::Success {
                metadata: meta,
            })))
        }
        BoltMessage::Logon { auth: _ } => {
            *authenticated = true;
            Ok(Some(dispatch::encode_message(&BoltMessage::Success {
                metadata: HashMap::from([("server".into(), serde_json::json!("copperdb/1.0"))]),
            })))
        }
        BoltMessage::Logoff => {
            *authenticated = false;
            Ok(Some(dispatch::encode_message(&BoltMessage::Success { metadata: HashMap::new() })))
        }
        BoltMessage::Run { query, parameters: _, extra: _ } => {
            *current_query = Some(query.clone());
            Ok(Some(dispatch::encode_message(&BoltMessage::Success {
                metadata: HashMap::from([("fields".into(), serde_json::json!([] as [serde_json::Value; 0])), ("t_first".into(), serde_json::json!(0))]),
            })))
        }
        BoltMessage::Pull { .. } | BoltMessage::Discard { .. } => {
            Ok(Some(dispatch::encode_message(&BoltMessage::Success {
                metadata: HashMap::from([("type".into(), serde_json::json!("r")), ("has_more".into(), serde_json::json!(false))]),
            })))
        }
        BoltMessage::Begin { extra: _ } => Ok(Some(dispatch::encode_message(&BoltMessage::Success { metadata: HashMap::new() }))),
        BoltMessage::Commit => Ok(Some(dispatch::encode_message(&BoltMessage::Success {
            metadata: HashMap::from([("bookmark".into(), serde_json::json!(""))]),
        }))),
        BoltMessage::Rollback => Ok(Some(dispatch::encode_message(&BoltMessage::Success { metadata: HashMap::new() }))),
        BoltMessage::Reset => { *current_query = None; Ok(Some(dispatch::encode_message(&BoltMessage::Success { metadata: HashMap::new() }))) }
        BoltMessage::Route { .. } => Ok(Some(dispatch::encode_message(&BoltMessage::Success { metadata: HashMap::new() }))),
        _ => Ok(None),
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
            let result = handle_tcp_session(&mut stream, &server_telemetry).await;
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
        assert_eq!(version_resp, [0x00, 0x00, 0x04, 0x04], "server should accept v4.4");

        // Step 2: Send HELLO
        let hello_bytes = encode_bolt_struct(0x01, &[Value::Map(vec![
            ("user_agent".into(), Value::String("neo4j-browser/5.0".into())),
            ("scheme".into(), Value::String("none".into())),
        ])]);
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
                assert_eq!(signature, 0x70, "expected SUCCESS (0x70), got 0x{signature:02X}");
                assert_eq!(fields.len(), 1, "SUCCESS should have 1 field (metadata map)");
                if let Value::Map(ref meta) = fields[0] {
                    let server = meta.iter().find(|(k, _)| k == "server").map(|(_, v)| v);
                    assert!(server.is_some(), "SUCCESS metadata should include 'server'");
                }
            }
            other => panic!("expected struct, got {other:?}"),
        }

        // Step 3: Send RUN
        let run_bytes = encode_bolt_struct(0x10, &[
            Value::String("RETURN 1 AS n".into()),
            Value::Map(vec![]),
            Value::Map(vec![]),
        ]);
        client.write_all(&run_bytes).await.unwrap();

        // Read SUCCESS for RUN
        resp_buf.clear();
        let n = client.read(&mut tmp).await.unwrap();
        assert!(n > 0, "should receive SUCCESS for RUN");
        resp_buf.extend_from_slice(&tmp[..n]);
        let (value, _) = crate::packstream::decode(&resp_buf).unwrap();
        match value {
            Value::Struct { signature, .. } => {
                assert_eq!(signature, 0x70, "expected SUCCESS for RUN, got 0x{signature:02X}");
            }
            other => panic!("expected SUCCESS, got {other:?}"),
        }

        // Step 4: Send PULL (Bolt 4.x: two direct integer fields)
        let pull_bytes = encode_bolt_struct(0x3F, &[
            Value::Integer(-1),
            Value::Integer(-1),
        ]);
        client.write_all(&pull_bytes).await.unwrap();

        // Read SUCCESS for PULL (stream done)
        resp_buf.clear();
        let n = client.read(&mut tmp).await.unwrap();
        assert!(n > 0, "should receive SUCCESS for PULL");
        resp_buf.extend_from_slice(&tmp[..n]);
        let (value, _) = crate::packstream::decode(&resp_buf).unwrap();
        match value {
            Value::Struct { signature, .. } => {
                assert_eq!(signature, 0x70, "expected SUCCESS for PULL, got 0x{signature:02X}");
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
            let result = handle_tcp_session(&mut stream, &st).await;
            assert!(result.is_err());
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        // Send invalid magic
        client.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]).await.unwrap();
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
            let result = handle_tcp_session(&mut stream, &st).await;
            // EOF after preamble should be Ok (clean disconnect)
            assert!(result.is_ok());
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        // Send valid preamble...
        let preamble: Vec<u8> = vec![
            0x60, 0x60, 0xB0, 0x17,
            0x00, 0x00, 0x04, 0x04,
            0x00, 0x00, 0x04, 0x03,
            0x00, 0x00, 0x04, 0x02,
            0x00, 0x00, 0x04, 0x01,
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
    async fn test_ws_full_handshake_hello_run_pull_flow() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let telemetry = Arc::new(Telemetry::new());
        telemetry.seed_zero_catalog_metrics();
        let server = BoltServer::new(addr.to_string(), Arc::clone(&telemetry));

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

        // Step 1: Send Bolt preamble
        let preamble: Vec<u8> = vec![
            0x60, 0x60, 0xB0, 0x17,
            0x00, 0x00, 0x04, 0x04,
            0x00, 0x00, 0x04, 0x03,
            0x00, 0x00, 0x04, 0x02,
            0x00, 0x00, 0x04, 0x01,
        ];
        use futures::SinkExt;
        ws.send(tokio_tungstenite::tungstenite::Message::Binary(preamble.into())).await.unwrap();

        // Read version response
        use futures::StreamExt;
        let vr = match ws.next().await.unwrap().unwrap() {
            tokio_tungstenite::tungstenite::Message::Binary(data) => data,
            other => panic!("expected Binary version response, got {other:?}"),
        };
        assert_eq!(vr.as_ref(), &[0x00, 0x00, 0x04, 0x04]);

        // Step 2: Send HELLO
        let hello_bytes = encode_bolt_struct(0x01, &[Value::Map(vec![
            ("user_agent".to_string(), Value::String("neo4j-test/1.0".to_string())),
            ("scheme".to_string(), Value::String("none".to_string())),
        ])]);
        ws.send(tokio_tungstenite::tungstenite::Message::Binary(hello_bytes.into())).await.unwrap();

        // Read SUCCESS for HELLO
        let resp = match ws.next().await.unwrap().unwrap() {
            tokio_tungstenite::tungstenite::Message::Binary(data) => data,
            other => panic!("expected Binary SUCCESS, got {other:?}"),
        };
        let (value, _) = crate::packstream::decode(&resp).unwrap();
        match value {
            Value::Struct { signature: 0x70, .. } => {}
            other => panic!("expected SUCCESS (0x70), got {other:?}"),
        }

        // Step 3: Send RUN
        let run_bytes = encode_bolt_struct(0x10, &[
            Value::String("RETURN 1 AS n".to_string()),
            Value::Map(vec![]),
            Value::Map(vec![]),
        ]);
        ws.send(tokio_tungstenite::tungstenite::Message::Binary(run_bytes.into())).await.unwrap();

        // Read SUCCESS for RUN
        let resp = match ws.next().await.unwrap().unwrap() {
            tokio_tungstenite::tungstenite::Message::Binary(data) => data,
            other => panic!("expected Binary SUCCESS for RUN, got {other:?}"),
        };
        let (value, _) = crate::packstream::decode(&resp).unwrap();
        match value {
            Value::Struct { signature: 0x70, .. } => {}
            other => panic!("expected SUCCESS for RUN, got {other:?}"),
        }

        // Step 4: Send PULL
        let pull_bytes = encode_bolt_struct(0x3F, &[
            Value::Integer(-1),
            Value::Integer(-1),
        ]);
        ws.send(tokio_tungstenite::tungstenite::Message::Binary(pull_bytes.into())).await.unwrap();

        // Read SUCCESS for PULL
        let resp = match ws.next().await.unwrap().unwrap() {
            tokio_tungstenite::tungstenite::Message::Binary(data) => data,
            other => panic!("expected Binary SUCCESS for PULL, got {other:?}"),
        };
        let (value, _) = crate::packstream::decode(&resp).unwrap();
        match value {
            Value::Struct { signature: 0x70, .. } => {}
            other => panic!("expected SUCCESS for PULL, got {other:?}"),
        }
    }
}
