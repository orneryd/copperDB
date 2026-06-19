//! WebSocket transport helpers for Bolt. Uses tokio-tungstenite directly.

use std::io;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// Read the next binary WebSocket message as raw bytes.
pub async fn read_ws_message<S>(ws: &mut WebSocketStream<S>) -> Option<io::Result<Vec<u8>>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use futures::StreamExt;
    loop {
        match ws.next().await {
            Some(Ok(Message::Binary(data))) => return Some(Ok(data.to_vec())),
            Some(Ok(Message::Close(_))) => return None,
            Some(Ok(_)) => continue,
            Some(Err(e)) => return Some(Err(io::Error::other(e.to_string()))),
            None => return None,
        }
    }
}

/// Write bytes as a binary WebSocket message.
pub async fn write_ws_message<S>(ws: &mut WebSocketStream<S>, data: &[u8]) -> io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use bytes::Bytes;
    use futures::SinkExt;
    ws.send(Message::Binary(Bytes::copy_from_slice(data)))
        .await
        .map_err(|e| io::Error::other(e.to_string()))
}
