//! Full-text and semantic search for copperdb.
//!
//! Equivalent to Go's `pkg/search` in NornicDB.
//! Combines in-memory inverted-index full-text search with optional
//! vector similarity search via copperdb-vectorspace.

use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
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
    #[error("topology error: {0}")]
    Topology(String),
    #[error("transport error: {0}")]
    Transport(String),
}

/// A search result with relevance score.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    pub id: String,
    pub score: f32,
    pub label: String,
    pub snippet: Option<String>,
}

pub fn merge_distributed_results(
    shard_results: Vec<Vec<SearchResult>>,
    limit: usize,
) -> Vec<SearchResult> {
    let mut best_by_id: HashMap<String, SearchResult> = HashMap::new();
    for result in shard_results.into_iter().flatten() {
        let replace = best_by_id
            .get(&result.id)
            .map(|existing| result.score > existing.score)
            .unwrap_or(true);
        if replace {
            best_by_id.insert(result.id.clone(), result);
        }
    }
    let mut merged = best_by_id.into_values().collect::<Vec<_>>();
    merged.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then(left.id.cmp(&right.id))
    });
    merged.truncate(limit);
    merged
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

#[async_trait]
pub trait SearchTransport: Send + Sync {
    async fn search_node(
        &self,
        node_id: &str,
        query: &SearchQuery,
    ) -> Result<Vec<SearchResult>, SearchError>;
}

#[derive(Default)]
pub struct InMemorySearchTransport {
    node_results: RwLock<HashMap<String, Vec<SearchResult>>>,
}

impl InMemorySearchTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_results(&self, node_id: impl Into<String>, results: Vec<SearchResult>) {
        self.node_results
            .write()
            .unwrap()
            .insert(node_id.into(), results);
    }
}

#[async_trait]
impl SearchTransport for InMemorySearchTransport {
    async fn search_node(
        &self,
        node_id: &str,
        _query: &SearchQuery,
    ) -> Result<Vec<SearchResult>, SearchError> {
        self.node_results
            .read()
            .unwrap()
            .get(node_id)
            .cloned()
            .ok_or_else(|| SearchError::Transport(format!("unknown search node {node_id}")))
    }
}

#[derive(Debug, Clone)]
pub struct DistributedSearchOutcome {
    pub plan: DistributedSearchPlan,
    pub responded_by: Vec<String>,
    pub failed_nodes: Vec<String>,
    pub results: Vec<SearchResult>,
}

pub struct DistributedSearchExecutor {
    router: DistributedSearchRouter,
    transport: Arc<dyn SearchTransport>,
}

impl DistributedSearchExecutor {
    pub fn new(router: DistributedSearchRouter, transport: Arc<dyn SearchTransport>) -> Self {
        Self { router, transport }
    }

    pub fn router(&self) -> &DistributedSearchRouter {
        &self.router
    }

    pub async fn search(
        &self,
        placement: &PlacementKey,
        query: SearchQuery,
        limit: usize,
    ) -> Result<DistributedSearchOutcome, SearchError> {
        let plan = self
            .router
            .plan(placement)
            .map_err(|error| SearchError::Topology(error.to_string()))?;
        self.execute_plan(plan, query, limit).await
    }

    pub async fn search_low_latency(
        &self,
        placement: &PlacementKey,
        query: SearchQuery,
        request_region: impl Into<String>,
        max_fanout: usize,
        limit: usize,
    ) -> Result<DistributedSearchOutcome, SearchError> {
        let plan = self
            .router
            .plan_low_latency(placement, request_region, max_fanout)
            .map_err(|error| SearchError::Topology(error.to_string()))?;
        self.execute_plan(plan, query, limit).await
    }

    async fn execute_plan(
        &self,
        plan: DistributedSearchPlan,
        query: SearchQuery,
        limit: usize,
    ) -> Result<DistributedSearchOutcome, SearchError> {
        let mut responded_by = Vec::new();
        let mut failed_nodes = Vec::new();
        let mut shard_results = Vec::new();
        for peer in &plan.fanout {
            match self.transport.search_node(&peer.node_id, &query).await {
                Ok(results) => {
                    responded_by.push(peer.node_id.clone());
                    shard_results.push(results);
                }
                Err(_) => failed_nodes.push(peer.node_id.clone()),
            }
        }
        if responded_by.is_empty() {
            return Err(SearchError::Transport(
                "distributed search returned no shard responses".into(),
            ));
        }
        let results = merge_distributed_results(shard_results, limit);
        Ok(DistributedSearchOutcome {
            plan,
            responded_by,
            failed_nodes,
            results,
        })
    }
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
            .register_placement(PlacementRecord::standalone("copper", "search-a"))
            .unwrap();

        let router = DistributedSearchRouter::new(topology);
        let plan = router
            .plan(&PlacementKey::default_for_database("copper"))
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
                key: PlacementKey::default_for_database("copper"),
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
            .plan_low_latency(
                &PlacementKey::default_for_database("copper"),
                "us-east-1",
                2,
            )
            .unwrap();
        assert_eq!(plan.parallelism, 2);
        assert_eq!(plan.fanout[0].node_id, "search-local");
    }

    #[test]
    fn distributed_result_merge_is_score_ordered_and_stable() {
        let merged = merge_distributed_results(
            vec![
                vec![
                    SearchResult {
                        id: "b".into(),
                        score: 0.9,
                        label: "Node".into(),
                        snippet: None,
                    },
                    SearchResult {
                        id: "a".into(),
                        score: 0.7,
                        label: "Node".into(),
                        snippet: None,
                    },
                ],
                vec![
                    SearchResult {
                        id: "a".into(),
                        score: 0.95,
                        label: "Node".into(),
                        snippet: Some("fresh".into()),
                    },
                    SearchResult {
                        id: "c".into(),
                        score: 0.9,
                        label: "Node".into(),
                        snippet: None,
                    },
                ],
            ],
            3,
        );

        assert_eq!(merged[0].id, "a");
        assert_eq!(merged[0].snippet, Some("fresh".into()));
        assert_eq!(merged[1].id, "b");
        assert_eq!(merged[2].id, "c");
    }

    #[tokio::test]
    async fn distributed_executor_fans_out_and_merges_results() {
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let placement = PlacementKey::default_for_database("copper");
        let mut topology = TopologyRegistry::new();
        for node_id in ["search-a", "search-b", "search-c"] {
            topology
                .register_peer(
                    MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Search),
                )
                .unwrap();
        }
        topology
            .register_placement(PlacementRecord {
                key: placement.clone(),
                primary_node: "search-a".into(),
                replica_nodes: vec![],
                search_nodes: vec!["search-a".into(), "search-b".into(), "search-c".into()],
                hyperscaler_profile: None,
                min_write_replicas: 0,
                search_fanout: 3,
            })
            .unwrap();

        let transport = Arc::new(InMemorySearchTransport::new());
        transport.register_results(
            "search-a",
            vec![SearchResult {
                id: "node-1".into(),
                score: 0.82,
                label: "Node".into(),
                snippet: None,
            }],
        );
        transport.register_results(
            "search-b",
            vec![SearchResult {
                id: "node-2".into(),
                score: 0.91,
                label: "Node".into(),
                snippet: None,
            }],
        );

        let executor =
            DistributedSearchExecutor::new(DistributedSearchRouter::new(topology), transport);
        let outcome = executor
            .search(
                &placement,
                SearchQuery::FullText {
                    query: "graph".into(),
                    fields: vec!["body".into()],
                    limit: 10,
                },
                10,
            )
            .await
            .unwrap();

        assert_eq!(outcome.responded_by, vec!["search-a", "search-b"]);
        assert_eq!(outcome.failed_nodes, vec!["search-c"]);
        assert_eq!(outcome.results[0].id, "node-2");
        assert_eq!(outcome.results[1].id, "node-1");
    }
}
