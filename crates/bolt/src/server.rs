//! Bolt server TCP listener and connection handler.

use crate::BoltError;
use copperdb_otel::Telemetry;
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

    let mut buffer = [0u8; 1024];
    loop {
        let loop_started = std::time::Instant::now();
        let bytes_read = stream.read(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }
        let _ = telemetry.record_counter(
            "nornicdb_bolt_messages_total",
            &[("op", "run"), ("result", "success")],
        );
        let _ = telemetry.observe_histogram(
            "nornicdb_bolt_message_duration_seconds",
            &[("op", "run")],
            loop_started.elapsed().as_secs_f64(),
        );
    }

    let _ = telemetry.observe_histogram(
        "nornicdb_bolt_session_duration_seconds",
        &[],
        started.elapsed().as_secs_f64(),
    );
    Ok(())
}
