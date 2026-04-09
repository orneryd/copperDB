//! Text chunking strategies for embedding pipelines.
//!
//! Equivalent to Go's `pkg/textchunk` in NornicDB.
//! Splits documents into chunks suitable for embedding, respecting
//! token limits and semantic boundaries.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChunkError {
    #[error("invalid chunk size: {0}")]
    InvalidChunkSize(usize),
}

/// A text chunk ready for embedding.
#[derive(Debug, Clone)]
pub struct Chunk {
    pub text: String,
    pub char_offset: usize,
    pub char_length: usize,
}

/// Split text into overlapping fixed-size character chunks.
pub fn chunk_by_chars(text: &str, chunk_size: usize, overlap: usize) -> Result<Vec<Chunk>, ChunkError> {
    if chunk_size == 0 {
        return Err(ChunkError::InvalidChunkSize(0));
    }
    let chars: Vec<char> = text.chars().collect();
    let step = chunk_size.saturating_sub(overlap).max(1);
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < chars.len() {
        let end = (start + chunk_size).min(chars.len());
        let chunk_text: String = chars[start..end].iter().collect();
        chunks.push(Chunk {
            text: chunk_text,
            char_offset: start,
            char_length: end - start,
        });
        if end == chars.len() { break; }
        start += step;
    }
    Ok(chunks)
}

/// Split text by sentences (simple period/newline heuristic).
pub fn chunk_by_sentences(text: &str, max_chunk_size: usize) -> Vec<Chunk> {
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut chunk_start = 0usize;
    let mut cursor = 0usize;

    for sentence in text.split_inclusive(|c| c == '.' || c == '!' || c == '?' || c == '\n') {
        if current.len() + sentence.len() > max_chunk_size && !current.is_empty() {
            let len = current.len();
            chunks.push(Chunk { text: current.clone(), char_offset: chunk_start, char_length: len });
            chunk_start = cursor;
            current.clear();
        }
        current.push_str(sentence);
        cursor += sentence.len();
    }
    if !current.is_empty() {
        let len = current.len();
        chunks.push(Chunk { text: current, char_offset: chunk_start, char_length: len });
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_by_chars() {
        let text = "hello world foo bar";
        let chunks = chunk_by_chars(text, 5, 0).unwrap();
        assert!(!chunks.is_empty());
        assert_eq!(chunks[0].text, "hello");
    }

    #[test]
    fn test_chunk_by_sentences() {
        let text = "Hello world. This is a test. Another sentence here.";
        let chunks = chunk_by_sentences(text, 25);
        assert!(!chunks.is_empty());
    }
}
