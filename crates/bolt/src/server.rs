//! Bolt server TCP listener and connection handler.

use crate::dispatch;
use crate::messages::BoltMessage;
use crate::packstream::Value;
use crate::BoltError;
use copperdb_otel::Telemetry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, warn};

/// Bolt server configuration.
#[derive(Debug, Clone)]
pub struct BoltServer {
    pub listen_addr: String,
    telemetry: Arc<Telemetry>,
    active_connections: Arc<AtomicU64>,
}

impl BoltServer {
    pub fn new(listen_addr: impl Into<String>, telemetry: Arc<Telemetry>) -> Self {
        Self {
            listen_addr: listen_addr.into(),
            telemetry,
            active_connections: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Start accepting Bolt connections.
    pub async fn serve(&self) -> Result<(), BoltError> {
        let listener = TcpListener::bind(&self.listen_addr).await?;

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let _ = self.telemetry.record_counter(
                "nornicdb_bolt_connections_total",
                &[("result", "success"), ("transport", "tcp")],
            );
            let active = self.active_connections.fetch_add(1, Ordering::SeqCst) + 1;
            let _ = self.telemetry.set_gauge(
                "nornicdb_bolt_connections_active",
                &[("transport", "tcp")],
                active as f64,
            );
            let telemetry = Arc::clone(&self.telemetry);
            let active_connections = Arc::clone(&self.active_connections);
            debug!(%peer_addr, "accepted bolt connection");
            tokio::spawn(async move {
                if let Err(error) = handle_connection(stream, &telemetry).await {
                    let _ = telemetry.record_counter(
                        "nornicdb_bolt_connections_total",
                        &[("result", "error"), ("transport", "tcp")],
                    );
                    warn!(%peer_addr, %error, "bolt connection failed");
                }
                let active = active_connections.fetch_sub(1, Ordering::SeqCst) - 1;
                let _ = telemetry.set_gauge(
                    "nornicdb_bolt_connections_active",
                    &[("transport", "tcp")],
                    active as f64,
                );
            });
        }
    }
}

async fn handle_connection(mut stream: TcpStream, telemetry: &Telemetry) -> Result<(), BoltError> {
    let started = std::time::Instant::now();
    let mut preamble = [0u8; 20];
    stream.read_exact(&mut preamble).await?;

    if preamble[..4] != [0x60, 0x60, 0xB0, 0x17] {
        let _ = telemetry.record_counter(
            "nornicdb_bolt_packstream_decode_errors_total",
            &[("reason", "invalid_marker")],
        );
        return Err(BoltError::ProtocolViolation(
            "invalid bolt magic preamble".into(),
        ));
    }

    // Advertise Bolt 4.4 so clients can complete version negotiation.
    stream.write_all(&[0x00, 0x00, 0x04, 0x04]).await?;

    let mut read_buf = Vec::with_capacity(4096);
    let mut temp_buf = [0u8; 4096];
    let mut authenticated = false;
    let mut current_query: Option<String> = None;

    loop {
        let loop_started = std::time::Instant::now();
        let bytes_read = stream.read(&mut temp_buf).await?;
        if bytes_read == 0 {
            break;
        }

        read_buf.extend_from_slice(&temp_buf[..bytes_read]);

        // Decode all complete PackStream values from the buffer
        loop {
            match crate::packstream::decode(&read_buf) {
                Ok((value, consumed)) => {
                    read_buf.drain(..consumed);

                    let (op, result) = match process_message(
                        &value,
                        &mut authenticated,
                        &mut current_query,
                    ) {
                        Ok(Some(response_bytes)) => {
                            stream.write_all(&response_bytes).await?;
                            ("run", "success")
                        }
                        Ok(None) => ("run", "success"),
                        Err(e) => {
                            let failure =
                                BoltMessage::Failure {
                                    metadata: HashMap::from([
                                        ("code".into(), serde_json::json!("Neo.TransientError.General.UnknownError")),
                                        ("message".into(), serde_json::json!(e.to_string())),
                                    ]),
                                };
                            let response_bytes = dispatch::encode_message(&failure);
                            stream.write_all(&response_bytes).await?;
                            ("run", "error")
                        }
                    };

                    let _ = telemetry.record_counter(
                        "nornicdb_bolt_messages_total",
                        &[("op", op), ("result", result)],
                    );
                    let _ = telemetry.observe_histogram(
                        "nornicdb_bolt_message_duration_seconds",
                        &[("op", op)],
                        loop_started.elapsed().as_secs_f64(),
                    );
                }
                Err(BoltError::PackStream(ref msg))
                    if msg.contains("unexpected end") || msg.contains("truncated") =>
                {
                    // Not enough data yet — wait for more bytes
                    break;
                }
                Err(e) => {
                    warn!(error = %e, "bolt decode error");
                    return Err(e);
                }
            }

            if read_buf.is_empty() {
                break;
            }
        }
    }

    let _ = telemetry.observe_histogram(
        "nornicdb_bolt_session_duration_seconds",
        &[],
        started.elapsed().as_secs_f64(),
    );
    Ok(())
}

/// Process a single decoded Bolt message value (a struct) and produce a
/// wire-encoded response. Returns `None` for messages that don't produce
/// a direct response (e.g., GOODBYE which closes the connection).
fn process_message(
    value: &Value,
    authenticated: &mut bool,
    current_query: &mut Option<String>,
) -> Result<Option<Vec<u8>>, BoltError> {
    let (sig, fields) = match value {
        Value::Struct { signature, fields } => (*signature, fields.as_slice()),
        _ => {
            return Err(BoltError::ProtocolViolation(
                "expected struct message".into(),
            ))
        }
    };

    let msg = dispatch::decode_message(sig, fields)?;

    match msg {
        BoltMessage::Hello { extra } => {
            *authenticated = false; // reset state
            let metadata = HashMap::from([
                ("server".into(), serde_json::json!("copperdb/1.0")),
                (
                    "connection_id".into(),
                    serde_json::json!("bolt-0"),
                ),
            ]);
            let _ = extra;
            Ok(Some(dispatch::encode_message(&BoltMessage::Success {
                metadata,
            })))
        }
        BoltMessage::Logon { auth: _ } => {
            *authenticated = true;
            let metadata =
                HashMap::from([("server".into(), serde_json::json!("copperdb/1.0"))]);
            Ok(Some(dispatch::encode_message(&BoltMessage::Success {
                metadata,
            })))
        }
        BoltMessage::Logoff => {
            *authenticated = false;
            Ok(Some(dispatch::encode_message(&BoltMessage::Success {
                metadata: HashMap::new(),
            })))
        }
        BoltMessage::Run { query, parameters: _, extra: _ } => {
            *current_query = Some(query.clone());
            let metadata = HashMap::from([
                ("fields".into(), serde_json::json!([] as [serde_json::Value; 0])),
                ("t_first".into(), serde_json::json!(0)),
            ]);
            Ok(Some(dispatch::encode_message(&BoltMessage::Success {
                metadata,
            })))
        }
        BoltMessage::Pull { n: _, qid: _ } => {
            // Return a SUCCESS with type "r" to indicate the stream is done
            let metadata = HashMap::from([
                ("type".into(), serde_json::json!("r")),
                ("has_more".into(), serde_json::json!(false)),
            ]);
            Ok(Some(dispatch::encode_message(&BoltMessage::Success {
                metadata,
            })))
        }
        BoltMessage::Discard { .. } => {
            let metadata = HashMap::from([("has_more".into(), serde_json::json!(false))]);
            Ok(Some(dispatch::encode_message(&BoltMessage::Success {
                metadata,
            })))
        }
        BoltMessage::Begin { extra: _ } => {
            let metadata = HashMap::new();
            Ok(Some(dispatch::encode_message(&BoltMessage::Success {
                metadata,
            })))
        }
        BoltMessage::Commit => {
            Ok(Some(dispatch::encode_message(&BoltMessage::Success {
                metadata: HashMap::from([("bookmark".into(), serde_json::json!(""))]),
            })))
        }
        BoltMessage::Rollback => {
            Ok(Some(dispatch::encode_message(&BoltMessage::Success {
                metadata: HashMap::new(),
            })))
        }
        BoltMessage::Reset => {
            *current_query = None;
            Ok(Some(dispatch::encode_message(&BoltMessage::Success {
                metadata: HashMap::new(),
            })))
        }
        BoltMessage::Route { .. } => {
            Ok(Some(dispatch::encode_message(&BoltMessage::Success {
                metadata: HashMap::new(),
            })))
        }
        _ => Ok(None),
    }
}
