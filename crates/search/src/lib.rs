//! Full-text and semantic search for magnetDB.
//!
//! Equivalent to Go's `pkg/search` in NornicDB.
//! Combines:
//! - Full-text search via Tantivy (BM25 scoring)
//! - Semantic/vector similarity search via magnetdb-vectorspace

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SearchError {
    #[error("tantivy error: {0}")]
    Tantivy(String),
    #[error("index not ready")]
    IndexNotReady,
}

/// A search result with relevance score.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub id: String,
    pub score: f32,
    pub label: String,
    pub snippet: Option<String>,
}

/// Search query types.
#[derive(Debug, Clone)]
pub enum SearchQuery {
    /// BM25 full-text search.
    FullText { query: String, fields: Vec<String>, limit: usize },
    /// Vector similarity search.
    Semantic { vector: Vec<f32>, k: usize, min_score: f32 },
    /// Hybrid: BM25 + semantic with RRF fusion.
    Hybrid { text: String, vector: Vec<f32>, k: usize },
}

// TODO: Implement full-text indexing using Tantivy.
// TODO: Implement vector search using magnetdb-vectorspace.
// TODO: Implement hybrid search with Reciprocal Rank Fusion (RRF).

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search_query_construction() {
        let q = SearchQuery::FullText {
            query: "Alice".to_string(),
            fields: vec!["name".to_string()],
            limit: 10,
        };
        assert!(matches!(q, SearchQuery::FullText { .. }));
    }
}
