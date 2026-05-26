//! Qdrant vector database client integration.
//!
//! Equivalent to Go's `pkg/qdrantgrpc` in NornicDB (uses `github.com/qdrant/go-client`).
//! Provides a production client path for offloading vector search to Qdrant
//! while keeping distributed fan-out and merge semantics in copperDB.

use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;

use copperdb_topology::DistributedSearchPlan;

#[derive(Debug, Error)]
pub enum QdrantError {
    #[error("connection error: {0}")]
    Connection(String),
    #[error("operation failed: {0}")]
    OperationFailed(String),
    #[error("invalid search plan: {0}")]
    InvalidPlan(String),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct QdrantVectorQuery {
    pub vector: Vec<f32>,
    pub limit: usize,
    pub min_score: Option<f32>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QdrantSearchTarget {
    pub node_id: String,
    pub endpoint: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct QdrantSearchRequest {
    pub collection: String,
    pub placement_id: String,
    pub targets: Vec<QdrantSearchTarget>,
    pub query: QdrantVectorQuery,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct QdrantSearchHit {
    pub id: String,
    pub score: f32,
    pub target_node: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct QdrantSearchResponse {
    pub hits: Vec<QdrantSearchHit>,
    pub failed_targets: Vec<QdrantSearchTarget>,
}

#[async_trait]
pub trait QdrantRemoteClient: Send + Sync {
    async fn search(
        &self,
        collection: &str,
        target: &QdrantSearchTarget,
        query: &QdrantVectorQuery,
    ) -> Result<Vec<QdrantSearchHit>, QdrantError>;
}

pub struct QdrantDistributedSearchExecutor {
    client: Arc<dyn QdrantRemoteClient>,
}

#[derive(Debug, Clone)]
pub struct QdrantHttpClient {
    client: reqwest::Client,
}

#[derive(Debug, serde::Serialize)]
struct QdrantSearchBody<'a> {
    vector: &'a [f32],
    limit: usize,
    with_payload: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    score_threshold: Option<f32>,
}

#[derive(Debug, serde::Deserialize)]
struct QdrantRestSearchResponse {
    result: Vec<QdrantRestPoint>,
}

#[derive(Debug, serde::Deserialize)]
struct QdrantRestPoint {
    id: serde_json::Value,
    score: f32,
    #[serde(default)]
    payload: serde_json::Value,
}

impl QdrantHttpClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    pub fn with_client(client: reqwest::Client) -> Self {
        Self { client }
    }

    fn search_url(target: &QdrantSearchTarget, collection: &str) -> String {
        format!(
            "{}/collections/{}/points/search",
            Self::endpoint_base(&target.endpoint),
            collection.trim_matches('/')
        )
    }

    fn endpoint_base(endpoint: &str) -> String {
        let endpoint = endpoint.trim_end_matches('/');
        if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            endpoint.into()
        } else {
            format!("http://{endpoint}")
        }
    }

    fn search_body(query: &QdrantVectorQuery) -> QdrantSearchBody<'_> {
        QdrantSearchBody {
            vector: &query.vector,
            limit: query.limit,
            with_payload: true,
            score_threshold: query.min_score,
        }
    }

    fn decode_hits(
        target: &QdrantSearchTarget,
        bytes: &[u8],
    ) -> Result<Vec<QdrantSearchHit>, QdrantError> {
        let response: QdrantRestSearchResponse = serde_json::from_slice(bytes)
            .map_err(|error| QdrantError::OperationFailed(error.to_string()))?;
        Ok(response
            .result
            .into_iter()
            .map(|point| QdrantSearchHit {
                id: match point.id {
                    serde_json::Value::String(id) => id,
                    other => other.to_string(),
                },
                score: point.score,
                target_node: target.node_id.clone(),
                payload: point.payload,
            })
            .collect())
    }
}

impl Default for QdrantHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl QdrantRemoteClient for QdrantHttpClient {
    async fn search(
        &self,
        collection: &str,
        target: &QdrantSearchTarget,
        query: &QdrantVectorQuery,
    ) -> Result<Vec<QdrantSearchHit>, QdrantError> {
        let response = self
            .client
            .post(Self::search_url(target, collection))
            .json(&Self::search_body(query))
            .send()
            .await
            .map_err(|error| QdrantError::Connection(error.to_string()))?
            .error_for_status()
            .map_err(|error| QdrantError::OperationFailed(error.to_string()))?;
        let bytes = response
            .bytes()
            .await
            .map_err(|error| QdrantError::OperationFailed(error.to_string()))?;
        Self::decode_hits(target, &bytes)
    }
}

impl QdrantDistributedSearchExecutor {
    pub fn new(client: Arc<dyn QdrantRemoteClient>) -> Self {
        Self { client }
    }

    pub async fn execute(
        &self,
        request: QdrantSearchRequest,
    ) -> Result<QdrantSearchResponse, QdrantError> {
        if request.targets.is_empty() {
            return Err(QdrantError::InvalidPlan(
                "distributed qdrant request has no targets".into(),
            ));
        }
        let mut hits = Vec::new();
        let mut failed_targets = Vec::new();
        for target in &request.targets {
            match self
                .client
                .search(&request.collection, target, &request.query)
                .await
            {
                Ok(mut target_hits) => hits.append(&mut target_hits),
                Err(_) => failed_targets.push(target.clone()),
            }
        }
        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.target_node.cmp(&right.target_node))
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(QdrantSearchResponse {
            hits: hits.into_iter().take(request.query.limit).collect(),
            failed_targets,
        })
    }
}

impl QdrantSearchRequest {
    pub fn from_search_plan(
        collection: impl Into<String>,
        plan: &DistributedSearchPlan,
        query: QdrantVectorQuery,
    ) -> Result<Self, QdrantError> {
        if query.vector.is_empty() {
            return Err(QdrantError::InvalidPlan(
                "vector query must not be empty".into(),
            ));
        }
        let targets = plan
            .fanout
            .iter()
            .map(|peer| QdrantSearchTarget {
                node_id: peer.node_id.clone(),
                endpoint: peer.advertise_addr.clone(),
            })
            .collect::<Vec<_>>();
        if targets.is_empty() {
            return Err(QdrantError::InvalidPlan(
                "distributed search plan has no qdrant targets".into(),
            ));
        }
        Ok(Self {
            collection: collection.into(),
            placement_id: plan.placement.stable_id(),
            targets,
            query,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use copperdb_topology::{
        MeshPeer, NodeCapability, PlacementKey, PlacementRecord, TopologyRegistry,
    };
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingQdrantClient {
        calls: Mutex<Vec<QdrantSearchTarget>>,
        failures: Mutex<HashSet<String>>,
        hits: Mutex<HashMap<String, Vec<QdrantSearchHit>>>,
    }

    #[async_trait]
    impl QdrantRemoteClient for RecordingQdrantClient {
        async fn search(
            &self,
            _collection: &str,
            target: &QdrantSearchTarget,
            _query: &QdrantVectorQuery,
        ) -> Result<Vec<QdrantSearchHit>, QdrantError> {
            self.calls.lock().unwrap().push(target.clone());
            if self.failures.lock().unwrap().contains(&target.node_id) {
                return Err(QdrantError::Connection(target.node_id.clone()));
            }
            Ok(self
                .hits
                .lock()
                .unwrap()
                .get(&target.node_id)
                .cloned()
                .unwrap_or_default())
        }
    }

    #[test]
    fn qdrant_request_preserves_distributed_search_targets() {
        let placement = PlacementKey::default_for_database("neo4j");
        let mut topology = TopologyRegistry::new();
        for node_id in ["search-a", "search-b"] {
            topology
                .register_peer(
                    MeshPeer::new(node_id, format!("{node_id}.mesh.local:6334"))
                        .with_capability(NodeCapability::Search),
                )
                .unwrap();
        }
        topology
            .register_placement(PlacementRecord {
                key: placement.clone(),
                primary_node: "search-a".into(),
                replica_nodes: vec![],
                search_nodes: vec!["search-a".into(), "search-b".into()],
                hyperscaler_profile: None,
                min_write_replicas: 0,
                search_fanout: 2,
            })
            .unwrap();
        let plan = topology.plan_search(&placement).unwrap();

        let request = QdrantSearchRequest::from_search_plan(
            "neo4j_vectors",
            &plan,
            QdrantVectorQuery {
                vector: vec![0.1, 0.2, 0.3],
                limit: 10,
                min_score: Some(0.7),
            },
        )
        .unwrap();

        assert_eq!(request.collection, "neo4j_vectors");
        assert_eq!(request.placement_id, "default/neo4j/primary");
        assert_eq!(request.targets.len(), 2);
        assert_eq!(request.targets[0].node_id, "search-a");
    }

    #[tokio::test]
    async fn qdrant_executor_fans_out_tracks_failures_and_merges_hits() {
        let client = Arc::new(RecordingQdrantClient::default());
        client.failures.lock().unwrap().insert("search-b".into());
        client.hits.lock().unwrap().insert(
            "search-a".into(),
            vec![
                QdrantSearchHit {
                    id: "a-low".into(),
                    score: 0.5,
                    target_node: "search-a".into(),
                    payload: serde_json::json!({"label":"low"}),
                },
                QdrantSearchHit {
                    id: "a-high".into(),
                    score: 0.9,
                    target_node: "search-a".into(),
                    payload: serde_json::json!({"label":"high"}),
                },
            ],
        );
        let executor = QdrantDistributedSearchExecutor::new(client.clone());
        let request = QdrantSearchRequest {
            collection: "neo4j_vectors".into(),
            placement_id: "default/neo4j/primary".into(),
            targets: vec![
                QdrantSearchTarget {
                    node_id: "search-a".into(),
                    endpoint: "search-a.mesh.local:6334".into(),
                },
                QdrantSearchTarget {
                    node_id: "search-b".into(),
                    endpoint: "search-b.mesh.local:6334".into(),
                },
            ],
            query: QdrantVectorQuery {
                vector: vec![0.1, 0.2, 0.3],
                limit: 1,
                min_score: None,
            },
        };

        let response = executor.execute(request).await.unwrap();

        assert_eq!(client.calls.lock().unwrap().len(), 2);
        assert_eq!(response.hits.len(), 1);
        assert_eq!(response.hits[0].id, "a-high");
        assert_eq!(response.failed_targets.len(), 1);
        assert_eq!(response.failed_targets[0].node_id, "search-b");
    }

    #[test]
    fn qdrant_http_client_builds_search_url_and_body() {
        let target = QdrantSearchTarget {
            node_id: "search-a".into(),
            endpoint: "search-a.mesh.local:6333/".into(),
        };
        let query = QdrantVectorQuery {
            vector: vec![0.1, 0.2, 0.3],
            limit: 7,
            min_score: Some(0.75),
        };

        let url = QdrantHttpClient::search_url(&target, "neo4j_vectors");
        let body = serde_json::to_value(QdrantHttpClient::search_body(&query)).unwrap();

        assert_eq!(
            url,
            "http://search-a.mesh.local:6333/collections/neo4j_vectors/points/search"
        );
        assert_eq!(body["vector"].as_array().unwrap().len(), 3);
        assert_eq!(body["limit"], 7);
        assert_eq!(body["with_payload"], true);
        assert_eq!(body["score_threshold"], 0.75);
    }

    #[test]
    fn qdrant_http_client_decodes_search_hits() {
        let target = QdrantSearchTarget {
            node_id: "search-a".into(),
            endpoint: "http://search-a.mesh.local:6333".into(),
        };
        let body = serde_json::json!({
            "result": [
                {"id": "node-1", "score": 0.91, "payload": {"label": "A"}},
                {"id": 42, "score": 0.82}
            ],
            "status": "ok",
            "time": 0.001
        });

        let hits = QdrantHttpClient::decode_hits(&target, body.to_string().as_bytes()).unwrap();

        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].id, "node-1");
        assert_eq!(hits[0].target_node, "search-a");
        assert_eq!(hits[0].payload, serde_json::json!({"label": "A"}));
        assert_eq!(hits[1].id, "42");
        assert_eq!(hits[1].payload, serde_json::Value::Null);
    }
}
