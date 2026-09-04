//! Text chunking strategies for embedding pipelines.
//!
//! Equivalent to Go's `pkg/textchunk` in NornicDB.
//! Splits documents into chunks suitable for embedding, respecting
//! token limits and semantic boundaries.

use std::fmt;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChunkError {
    #[error("invalid chunk size: {0}")]
    InvalidChunkSize(usize),
    #[error("token counter error: {0}")]
    TokenCounter(String),
}

/// A text chunk ready for embedding.
#[derive(Debug, Clone)]
pub struct Chunk {
    pub text: String,
    pub char_offset: usize,
    pub char_length: usize,
}

/// Split text into overlapping fixed-size character chunks.
pub fn chunk_by_chars(
    text: &str,
    chunk_size: usize,
    overlap: usize,
) -> Result<Vec<Chunk>, ChunkError> {
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
        if end == chars.len() {
            break;
        }
        start += step;
    }
    Ok(chunks)
}

/// Split text by sentences (simple period/newline heuristic).
///
/// `max_chunk_size` and the returned `char_offset`/`char_length` are all
/// measured in Unicode scalar values (chars), not bytes.
pub fn chunk_by_sentences(text: &str, max_chunk_size: usize) -> Vec<Chunk> {
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut current_char_len = 0usize;
    let mut chunk_start = 0usize;
    let mut char_cursor = 0usize;

    for sentence in text.split_inclusive(['.', '!', '?', '\n']) {
        let sentence_char_len = sentence.chars().count();
        if current_char_len + sentence_char_len > max_chunk_size && !current.is_empty() {
            chunks.push(Chunk {
                text: current.clone(),
                char_offset: chunk_start,
                char_length: current_char_len,
            });
            chunk_start = char_cursor;
            current.clear();
            current_char_len = 0;
        }
        current.push_str(sentence);
        current_char_len += sentence_char_len;
        char_cursor += sentence_char_len;
    }
    if !current.is_empty() {
        chunks.push(Chunk {
            text: current,
            char_offset: chunk_start,
            char_length: current_char_len,
        });
    }
    chunks
}

/// Split text into token-bounded chunks using a caller-provided token counter.
pub fn chunk_by_token_count<F, E>(
    text: &str,
    max_tokens: isize,
    overlap: isize,
    mut count_tokens: F,
) -> Result<Vec<String>, ChunkError>
where
    F: FnMut(&str) -> Result<usize, E>,
    E: fmt::Display,
{
    if max_tokens <= 0 {
        let trimmed = text.trim();
        return Ok(vec![
            if trimmed.is_empty() { text } else { trimmed }.to_string(),
        ]);
    }

    let max_tokens = max_tokens as usize;
    let mut overlap = overlap.max(0) as usize;
    if overlap >= max_tokens {
        overlap = max_tokens.saturating_sub(1);
    }

    let total_tokens = count_tokens(text).map_err(counter_error)?;
    if total_tokens <= max_tokens {
        let trimmed = text.trim();
        return Ok(vec![
            if trimmed.is_empty() { text } else { trimmed }.to_string(),
        ]);
    }

    let offsets = rune_byte_offsets(text);
    if offsets.len() <= 1 {
        return Ok(vec![text.to_string()]);
    }

    let mut chunks = Vec::with_capacity(total_tokens / max_tokens + 1);
    let mut start = 0usize;
    while start < offsets.len() - 1 {
        let mut end = max_fitting_chunk_end(text, &offsets, start, max_tokens, &mut count_tokens)?;
        if end <= start {
            end = (start + 1).min(offsets.len() - 1);
        }

        let chunk = text[offsets[start]..offsets[end]].trim();
        if !chunk.is_empty() {
            chunks.push(chunk.to_string());
        }

        if end >= offsets.len() - 1 {
            break;
        }

        let mut next_start = end;
        if overlap > 0 {
            next_start =
                overlapping_chunk_start(text, &offsets, start, end, overlap, &mut count_tokens)?;
            if next_start <= start {
                next_start = start + 1;
            }
            if next_start > end {
                next_start = end;
            }
        }
        start = next_start;
    }

    if chunks.is_empty() {
        let trimmed = text.trim();
        return Ok(vec![
            if trimmed.is_empty() { text } else { trimmed }.to_string(),
        ]);
    }
    Ok(chunks)
}

fn max_fitting_chunk_end<F, E>(
    text: &str,
    offsets: &[usize],
    start: usize,
    max_tokens: usize,
    count_tokens: &mut F,
) -> Result<usize, ChunkError>
where
    F: FnMut(&str) -> Result<usize, E>,
    E: fmt::Display,
{
    let mut low = start + 1;
    let mut high = offsets.len() - 1;
    let mut best = start;
    while low <= high {
        let mid = low + (high - low) / 2;
        let count = count_tokens(&text[offsets[start]..offsets[mid]]).map_err(counter_error)?;
        if count <= max_tokens {
            best = mid;
            low = mid + 1;
        } else {
            high = mid.saturating_sub(1);
        }
    }
    Ok(best)
}

fn overlapping_chunk_start<F, E>(
    text: &str,
    offsets: &[usize],
    chunk_start: usize,
    chunk_end: usize,
    overlap: usize,
    count_tokens: &mut F,
) -> Result<usize, ChunkError>
where
    F: FnMut(&str) -> Result<usize, E>,
    E: fmt::Display,
{
    let mut low = chunk_start + 1;
    let mut high = chunk_end;
    let mut best = chunk_end;
    while low <= high {
        let mid = low + (high - low) / 2;
        let count = count_tokens(&text[offsets[mid]..offsets[chunk_end]]).map_err(counter_error)?;
        if count <= overlap {
            best = mid;
            high = mid.saturating_sub(1);
        } else {
            low = mid + 1;
        }
    }
    Ok(best)
}

fn rune_byte_offsets(text: &str) -> Vec<usize> {
    let mut offsets = text
        .char_indices()
        .map(|(offset, _)| offset)
        .collect::<Vec<_>>();
    offsets.push(text.len());
    offsets
}

fn counter_error<E: fmt::Display>(err: E) -> ChunkError {
    ChunkError::TokenCounter(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn word_count(text: &str) -> Result<usize, &'static str> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            Ok(0)
        } else {
            Ok(trimmed.split_whitespace().count())
        }
    }

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

    #[test]
    fn test_chunk_by_sentences_char_offsets_non_ascii() {
        // "Héllo." is 6 chars but 7 bytes — offsets must be in chars.
        let text = "Héllo. World!";
        let chunks = chunk_by_sentences(text, 100);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].char_offset, 0);
        assert_eq!(chunks[0].char_length, text.chars().count());
    }

    #[test]
    fn test_chunk_by_sentences_split_tracking() {
        // Force a split so we can verify the second chunk's char_offset.
        let text = "Short. Another longer sentence here!";
        let chunks = chunk_by_sentences(text, 10);
        // Second chunk must start where the first ended (in chars).
        let first_len = chunks[0].char_length;
        assert_eq!(chunks[1].char_offset, first_len);
    }

    #[test]
    fn test_chunk_by_token_count_max_tokens_zero_empty_text() {
        let chunks = chunk_by_token_count("", 0, 0, word_count).unwrap();
        assert_eq!(chunks, vec![""]);
    }

    #[test]
    fn test_chunk_by_token_count_max_tokens_negative_whitespace_only() {
        let chunks = chunk_by_token_count("   ", -1, 0, word_count).unwrap();
        assert_eq!(chunks, vec!["   "]);
    }

    #[test]
    fn test_chunk_by_token_count_overlap_equals_max_tokens_one() {
        let chunks = chunk_by_token_count("one two three four", 1, 5, word_count).unwrap();
        assert!(chunks.len() >= 2);
        let total_tokens: usize = chunks
            .iter()
            .map(|chunk| chunk.split_whitespace().count())
            .sum();
        assert_eq!(total_tokens, 4);
    }

    #[test]
    fn test_chunk_by_token_count_offsets_len_one_short_circuit() {
        let chunks = chunk_by_token_count("", 5, 0, |_| Ok::<usize, &'static str>(10)).unwrap();
        assert_eq!(chunks, vec![""]);
    }

    #[test]
    fn test_chunk_by_token_count_end_less_than_start_fallback() {
        let chunks = chunk_by_token_count("abc", 1, 0, |_| Ok::<usize, &'static str>(99)).unwrap();
        assert_eq!(chunks, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_chunk_by_token_count_fallback_cap_at_end() {
        let chunks = chunk_by_token_count("x", 1, 0, |_| Ok::<usize, &'static str>(99)).unwrap();
        assert_eq!(chunks, vec!["x"]);
    }

    #[test]
    fn test_chunk_by_token_count_overlap_counter_error_propagates() {
        let full = "alpha bravo charlie delta echo foxtrot";
        let err = chunk_by_token_count(full, 2, 1, |candidate| {
            if candidate == full {
                Ok(100)
            } else if full.starts_with(candidate) {
                Ok(candidate.split_whitespace().count())
            } else {
                Err("counter failed inside overlap calculation")
            }
        })
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("counter failed inside overlap calculation")
        );
    }

    #[test]
    fn test_chunk_by_token_count_overlap_clamp_paths() {
        let text = "token ".repeat(50);
        let chunks = chunk_by_token_count(&text, 4, 3, |candidate: &str| {
            Ok::<usize, &'static str>(candidate.split_whitespace().count() * 2)
        })
        .unwrap();
        assert!(chunks.len() >= 2);
    }

    #[test]
    fn test_chunk_by_token_count_all_chunks_empty_returns_original() {
        let text = "     ";
        let chunks = chunk_by_token_count(text, 1, 0, |_| Ok::<usize, &'static str>(99)).unwrap();
        assert_eq!(chunks, vec![text]);
    }

    #[test]
    fn test_chunk_by_token_count_all_chunks_empty_empty_text() {
        let chunks = chunk_by_token_count(" ", 1, 0, |_| Ok::<usize, &'static str>(99)).unwrap();
        assert_eq!(chunks, vec![" "]);
    }

    #[test]
    fn test_overlapping_chunk_start_bubbles_counter_error() {
        let text = "hello world foo bar";
        let offsets = rune_byte_offsets(text);
        let err = overlapping_chunk_start(text, &offsets, 0, offsets.len() - 1, 1, &mut |_| {
            Err::<usize, &'static str>("boom inside overlap")
        })
        .unwrap_err();
        assert!(err.to_string().contains("boom inside overlap"));
    }

    #[test]
    fn test_max_fitting_chunk_end_happy_path_and_error() {
        let text = "a b c d e f";
        let offsets = rune_byte_offsets(text);
        let end = max_fitting_chunk_end(text, &offsets, 0, 4, &mut word_count).unwrap();
        assert!(end > 0);
        assert!(text[offsets[0]..offsets[end]].split_whitespace().count() <= 4);

        let err = max_fitting_chunk_end(text, &offsets, 0, 4, &mut |_| {
            Err::<usize, &'static str>("boom inside max-fitting")
        })
        .unwrap_err();
        assert!(err.to_string().contains("boom inside max-fitting"));
    }
}
