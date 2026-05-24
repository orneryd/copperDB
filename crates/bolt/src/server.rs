//! Bolt server TCP listener and connection handler.

use crate::BoltError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, warn};

/// Bolt server configuration.
#[derive(Debug, Clone)]
pub struct BoltServer {
    pub listen_addr: String,
}

impl BoltServer {
    pub fn new(listen_addr: impl Into<String>) -> Self {
        Self {
            listen_addr: listen_addr.into(),
        }
    }

    /// Start accepting Bolt connections.
    pub async fn serve(&self) -> Result<(), BoltError> {
        let listener = TcpListener::bind(&self.listen_addr).await?;

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            debug!(%peer_addr, "accepted bolt connection");
            tokio::spawn(async move {
                if let Err(error) = handle_connection(stream).await {
                    warn!(%peer_addr, %error, "bolt connection failed");
                }
            });
        }
    }
}

async fn handle_connection(mut stream: TcpStream) -> Result<(), BoltError> {
    let mut preamble = [0u8; 20];
    stream.read_exact(&mut preamble).await?;

    if preamble[..4] != [0x60, 0x60, 0xB0, 0x17] {
        return Err(BoltError::ProtocolViolation(
            "invalid bolt magic preamble".into(),
        ));
    }

    // Advertise Bolt 4.4 so clients can complete version negotiation.
    stream.write_all(&[0x00, 0x00, 0x04, 0x04]).await?;

    let mut buffer = [0u8; 1024];
    loop {
        let bytes_read = stream.read(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }
    }

    Ok(())
}
