//! WebSocket transport helpers for Bolt. Uses tokio-tungstenite directly.

use std::io;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Default)]
pub struct BoltChunkDecoder {
    pending: Vec<u8>,
    current: Vec<u8>,
}

impl BoltChunkDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, data: &[u8]) -> Vec<Vec<u8>> {
        self.pending.extend_from_slice(data);
        let mut messages = Vec::new();

        loop {
            if self.pending.len() < 2 {
                break;
            }

            let size = ((self.pending[0] as usize) << 8) | (self.pending[1] as usize);
            if size == 0 {
                self.pending.drain(..2);
                if !self.current.is_empty() {
                    messages.push(std::mem::take(&mut self.current));
                }
                continue;
            }

            let chunk_end = 2 + size;
            if self.pending.len() < chunk_end {
                break;
            }

            self.current.extend_from_slice(&self.pending[2..chunk_end]);
            self.pending.drain(..chunk_end);
        }

        messages
    }
}

/// Read the next binary WebSocket message, decoding Bolt chunks.
/// Returns all decoded messages (multiple if batched in one WS frame).
pub async fn read_ws_message<S>(
    ws: &mut WebSocketStream<S>,
    decoder: &mut BoltChunkDecoder,
) -> Option<io::Result<Vec<Vec<u8>>>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use futures::StreamExt;
    loop {
        match ws.next().await {
            Some(Ok(Message::Binary(data))) => {
                tracing::info!("wsconn: recv Binary frame len={}", data.len());
                let messages = decoder.push(&data);
                return Some(Ok(messages));
            }
            Some(Ok(Message::Close(_))) => {
                tracing::info!("wsconn: recv Close frame");
                return None;
            }
            Some(Ok(Message::Ping(_))) => {
                tracing::debug!("wsconn: recv Ping (auto-pong)");
                continue;
            }
            Some(Ok(Message::Pong(_))) => {
                tracing::debug!("wsconn: recv Pong");
                continue;
            }
            Some(Ok(other)) => {
                tracing::info!("wsconn: recv non-Binary: {:?}", other);
                continue;
            }
            Some(Err(e)) => {
                tracing::warn!("wsconn: read error: {}", e);
                return Some(Err(io::Error::other(e.to_string())));
            }
            None => {
                tracing::info!("wsconn: stream ended");
                return None;
            }
        }
    }
}

/// Write Bolt-chunked data as a binary WebSocket message.
/// Mirrors NornicDB's writeMessageNoFlush: wraps data in 2-byte chunk
/// headers with a 0x00 0x00 terminator, then sends as one WS frame.
pub async fn write_ws_message<S>(ws: &mut WebSocketStream<S>, data: &[u8]) -> io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use bytes::Bytes;
    use futures::SinkExt;
    let chunked = encode_bolt_chunks(data);
    ws.send(Message::Binary(Bytes::from(chunked)))
        .await
        .map_err(|e| io::Error::other(e.to_string()))
}

/// Write raw bytes as a binary WebSocket message (no chunk encoding).
/// Used for Bolt handshake preamble and version response.
pub async fn write_ws_raw<S>(ws: &mut WebSocketStream<S>, data: &[u8]) -> io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use bytes::Bytes;
    use futures::SinkExt;
    ws.send(Message::Binary(Bytes::copy_from_slice(data)))
        .await
        .map_err(|e| io::Error::other(e.to_string()))
}

/// Decode Bolt-chunked data into one or more contiguous message buffers.
/// Each message is [size_hi][size_lo][data...]; 0x0000 terminates a message.
/// Multiple messages may be concatenated in a single byte slice (e.g., when
/// the client batches RUN+PULL in one WebSocket frame).
pub fn decode_bolt_chunks(data: &[u8]) -> Vec<Vec<u8>> {
    let mut decoder = BoltChunkDecoder::new();
    decoder.push(data)
}

/// Encode data into Bolt chunked format with 2-byte size headers and
/// a 0x00 0x00 terminator. Mirrors NornicDB's writeMessageNoFlush.
pub fn encode_bolt_chunks(data: &[u8]) -> Vec<u8> {
    const MAX_CHUNK: usize = 0xFFFF;
    let mut out = Vec::with_capacity(data.len() + 4);
    let mut remaining = data;
    while !remaining.is_empty() {
        let chunk_size = remaining.len().min(MAX_CHUNK);
        out.push((chunk_size >> 8) as u8);
        out.push(chunk_size as u8);
        out.extend_from_slice(&remaining[..chunk_size]);
        remaining = &remaining[chunk_size..];
    }
    // Terminator chunk
    out.push(0x00);
    out.push(0x00);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bolt_chunk_decoder_waits_for_terminator() {
        let raw_hello = vec![0xB1, 0x01, 0xA0];
        let mut frame = Vec::new();
        frame.extend_from_slice(&[0x00, 0x03]);
        frame.extend_from_slice(&raw_hello);

        let mut decoder = BoltChunkDecoder::new();
        assert!(decoder.push(&frame).is_empty());
        assert_eq!(decoder.push(&[0x00, 0x00]), vec![raw_hello]);
    }

    #[test]
    fn bolt_chunk_decoder_reassembles_split_chunk_body() {
        let raw_hello = vec![0xB1, 0x01, 0xA0];
        let encoded = encode_bolt_chunks(&raw_hello);
        let mut decoder = BoltChunkDecoder::new();

        assert!(decoder.push(&encoded[..3]).is_empty());
        assert_eq!(decoder.push(&encoded[3..]), vec![raw_hello]);
    }

    #[test]
    fn decode_bolt_chunks_decodes_complete_classic_frame() {
        let raw_hello = vec![0xB1, 0x01, 0xA0];
        assert_eq!(
            decode_bolt_chunks(&encode_bolt_chunks(&raw_hello)),
            vec![raw_hello]
        );
    }
}
