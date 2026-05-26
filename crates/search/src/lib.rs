//! Full-text and semantic search for copperdb.
//!
//! Equivalent to Go's `pkg/search` in NornicDB.
//! Combines in-memory inverted-index full-text search with optional
//! vector similarity search via copperdb-vectorspace.

use std::collections::{HashMap, HashSet};
use thiserror::Error;

use copperdb_topology::{
    DistributedSearchPlan, PlacementKey, SearchRoutingPolicy, TopologyError, TopologyRegistry,
};

#[derive(Debug, Error)]
pub enum SearchError {
    #[error("tantivy error: {0}")]
    Tantivy(String),
    #[error("index not ready")]
    IndexNotReady,
    #[error("document not found: {0}")]
    NotFound(String),
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
    FullText {
        query: String,
        fields: Vec<String>,
        limit: usize,
    },
    /// Vector similarity search.
    Semantic {
        vector: Vec<f32>,
        k: usize,
        min_score: f32,
    },
    /// Hybrid: BM25 + semantic with RRF fusion.
    Hybrid {
        text: String,
        vector: Vec<f32>,
        k: usize,
    },
}

/// In-memory full-text search index using an inverted index.
///
/// Documents are stored as `id -> field -> text`. Words are tokenized
/// (lowercased, split on whitespace/punctuation) and stored in an
/// inverted index mapping `word -> set of doc IDs`.
pub struct SearchIndex {
    /// id -> field -> text
    documents: HashMap<String, HashMap<String, String>>,
    /// word -> set of doc IDs
    inverted: HashMap<String, HashSet<String>>,
    /// word -> field -> set of doc IDs (for field-scoped search)
    field_inverted: HashMap<String, HashMap<String, HashSet<String>>>,
}

/// Distributed search planning seam.
///
/// The search engine remains local today. This router makes the cluster fan-out
/// contract explicit so mesh execution can be added without changing query APIs.
#[derive(Debug, Clone, Default)]
pub struct DistributedSearchRouter {
    topology: TopologyRegistry,
}

impl DistributedSearchRouter {
    pub fn new(topology: TopologyRegistry) -> Self {
        Self { topology }
    }

    pub fn topology(&self) -> &TopologyRegistry {
        &self.topology
    }

    pub fn plan(&self, placement: &PlacementKey) -> Result<DistributedSearchPlan, TopologyError> {
        self.topology.plan_search(placement)
    }

    pub fn plan_low_latency(
        &self,
        placement: &PlacementKey,
        request_region: impl Into<String>,
        max_fanout: usize,
    ) -> Result<DistributedSearchPlan, TopologyError> {
        self.topology.plan_search_with_policy(
            placement,
            SearchRoutingPolicy::low_latency(request_region, max_fanout),
        )
    }
}

impl Default for SearchIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl SearchIndex {
    pub fn new() -> Self {
        Self {
            documents: HashMap::new(),
            inverted: HashMap::new(),
            field_inverted: HashMap::new(),
        }
    }

    /// Tokenize text into lowercase words (split on non-alphanumeric chars).
    fn tokenize(text: &str) -> Vec<String> {
        text.split(|c: char| !c.is_alphanumeric())
            .filter(|w| !w.is_empty())
            .map(|w| w.to_lowercase())
            .collect()
    }

    /// Index a document. Re-indexing the same id replaces the previous document.
    pub fn index_document(&mut self, id: &str, fields: HashMap<String, String>) {
        // Remove old index entries if re-indexing
        self.remove_document(id);

        for (field, text) in &fields {
            for word in Self::tokenize(text) {
                self.inverted
                    .entry(word.clone())
                    .or_default()
                    .insert(id.to_string());
                self.field_inverted
                    .entry(word)
                    .or_default()
                    .entry(field.clone())
                    .or_default()
                    .insert(id.to_string());
            }
        }
        self.documents.insert(id.to_string(), fields);
    }

    /// Remove a document and its index entries.
    pub fn remove_document(&mut self, id: &str) {
        if let Some(fields) = self.documents.remove(id) {
            for (_field, text) in &fields {
                for word in Self::tokenize(text) {
                    // Prune empty inverted-index entries (avoids unbounded memory
                    // growth when documents are frequently added/removed).
                    let remove_inverted = if let Some(set) = self.inverted.get_mut(&word) {
                        set.remove(id);
                        set.is_empty()
                    } else {
                        false
                    };
                    if remove_inverted {
                        self.inverted.remove(&word);
                    }

                    let remove_field_word =
                        if let Some(field_map) = self.field_inverted.get_mut(&word) {
                            let remove_field = if let Some(set) = field_map.get_mut(_field) {
                                set.remove(id);
                                set.is_empty()
                            } else {
                                false
                            };
                            if remove_field {
                                field_map.remove(_field);
                            }
                            field_map.is_empty()
                        } else {
                            false
                        };
                    if remove_field_word {
                        self.field_inverted.remove(&word);
                    }
                }
            }
        }
    }

    /// Search across all fields. Returns doc IDs sorted by match count (descending).
    pub fn search(&self, query: &str) -> Vec<String> {
        let tokens = Self::tokenize(query);
        if tokens.is_empty() {
            return vec![];
        }
        let mut scores: HashMap<String, usize> = HashMap::new();
        for token in &tokens {
            if let Some(ids) = self.inverted.get(token) {
                for id in ids {
                    *scores.entry(id.clone()).or_default() += 1;
                }
            }
        }
        let mut results: Vec<(String, usize)> = scores.into_iter().collect();
        results.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        results.into_iter().map(|(id, _)| id).collect()
    }

    /// Search within a specific field. Returns doc IDs sorted by match count.
    pub fn search_field(&self, field: &str, query: &str) -> Vec<String> {
        let tokens = Self::tokenize(query);
        if tokens.is_empty() {
            return vec![];
        }
        let mut scores: HashMap<String, usize> = HashMap::new();
        for token in &tokens {
            if let Some(field_map) = self.field_inverted.get(token) {
                if let Some(ids) = field_map.get(field) {
                    for id in ids {
                        *scores.entry(id.clone()).or_default() += 1;
                    }
                }
            }
        }
        let mut results: Vec<(String, usize)> = scores.into_iter().collect();
        results.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        results.into_iter().map(|(id, _)| id).collect()
    }

    pub fn document_count(&self) -> usize {
        self.documents.len()
    }

    pub fn get_document(&self, id: &str) -> Option<&HashMap<String, String>> {
        self.documents.get(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_doc(text: &str) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("body".into(), text.into());
        m
    }

    #[test]
    fn test_search_query_construction() {
        let q = SearchQuery::FullText {
            query: "Alice".to_string(),
            fields: vec!["name".to_string()],
            limit: 10,
        };
        assert!(matches!(q, SearchQuery::FullText { .. }));
    }

    #[test]
    fn test_index_and_search() {
        let mut idx = SearchIndex::new();
        idx.index_document("1", make_doc("hello world"));
        idx.index_document("2", make_doc("goodbye world"));
        let results = idx.search("hello");
        assert_eq!(results, vec!["1"]);
    }

    #[test]
    fn test_search_multi_token() {
        let mut idx = SearchIndex::new();
        idx.index_document("1", make_doc("rust is fast and safe"));
        idx.index_document("2", make_doc("go is fast"));
        // "fast" matches both; "rust" only matches 1 — doc 1 should rank higher
        let results = idx.search("rust fast");
        assert_eq!(results[0], "1");
    }

    #[test]
    fn test_search_field() {
        let mut idx = SearchIndex::new();
        let mut d1 = HashMap::new();
        d1.insert("name".into(), "Alice Smith".into());
        d1.insert("bio".into(), "engineer at company".into());
        idx.index_document("1", d1);
        let results = idx.search_field("name", "alice");
        assert_eq!(results, vec!["1"]);
        let results2 = idx.search_field("bio", "alice");
        assert!(results2.is_empty());
    }

    #[test]
    fn test_remove_document() {
        let mut idx = SearchIndex::new();
        idx.index_document("1", make_doc("hello world"));
        idx.remove_document("1");
        assert_eq!(idx.document_count(), 0);
        assert!(idx.search("hello").is_empty());
    }

    #[test]
    fn test_reindex_document() {
        let mut idx = SearchIndex::new();
        idx.index_document("1", make_doc("old content here"));
        idx.index_document("1", make_doc("new content there"));
        assert_eq!(idx.document_count(), 1);
        let results = idx.search("old");
        assert!(results.is_empty());
        let results2 = idx.search("new");
        assert_eq!(results2, vec!["1"]);
    }

    #[test]
    fn test_empty_search() {
        let idx = SearchIndex::new();
        assert!(idx.search("hello").is_empty());
    }

    #[test]
    fn test_document_count() {
        let mut idx = SearchIndex::new();
        assert_eq!(idx.document_count(), 0);
        idx.index_document("1", make_doc("a"));
        idx.index_document("2", make_doc("b"));
        assert_eq!(idx.document_count(), 2);
    }

    #[test]
    fn test_tokenize_punctuation() {
        let tokens = SearchIndex::tokenize("Hello, world! It's great.");
        assert!(tokens.contains(&"hello".to_string()));
        assert!(tokens.contains(&"world".to_string()));
    }

    #[test]
    fn distributed_router_plans_mesh_fanout() {
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementRecord};

        let mut topology = TopologyRegistry::new();
        topology
            .register_peer(
                MeshPeer::new("search-a", "search-a.mesh.local:9000")
                    .with_capability(NodeCapability::Search)
                    .with_capability(NodeCapability::WriteLeader),
            )
            .unwrap();
        topology
            .register_placement(PlacementRecord::standalone("neo4j", "search-a"))
            .unwrap();

        let router = DistributedSearchRouter::new(topology);
        let plan = router
            .plan(&PlacementKey::default_for_database("neo4j"))
            .unwrap();
        assert_eq!(plan.fanout[0].node_id, "search-a");
    }

    #[test]
    fn distributed_router_prefers_low_latency_local_peers() {
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let mut topology = TopologyRegistry::new();
        topology
            .register_peer(
                MeshPeer::new("search-remote", "search-remote.mesh.local:9000")
                    .with_capability(NodeCapability::Search)
                    .with_region_zone("eu-west-1", "eu-west-1a")
                    .with_observed_rtt_micros(500)
                    .with_load(0, 8),
            )
            .unwrap();
        topology
            .register_peer(
                MeshPeer::new("search-local", "search-local.mesh.local:9000")
                    .with_capability(NodeCapability::Search)
                    .with_region_zone("us-east-1", "us-east-1a")
                    .with_observed_rtt_micros(1_500)
                    .with_load(0, 8),
            )
            .unwrap();
        topology
            .register_placement(PlacementRecord {
                key: PlacementKey::default_for_database("neo4j"),
                primary_node: "search-local".into(),
                replica_nodes: vec![],
                search_nodes: vec!["search-remote".into(), "search-local".into()],
                hyperscaler_profile: None,
                min_write_replicas: 0,
                search_fanout: 2,
            })
            .unwrap();

        let router = DistributedSearchRouter::new(topology);
        let plan = router
            .plan_low_latency(&PlacementKey::default_for_database("neo4j"), "us-east-1", 2)
            .unwrap();
        assert_eq!(plan.parallelism, 2);
        assert_eq!(plan.fanout[0].node_id, "search-local");
    }
}
