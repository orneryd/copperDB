//! GraphQL API layer for copperdb.
//!
//! Equivalent to Go's `pkg/graphql` in NornicDB (uses `github.com/99designs/gqlgen`).
//! Exposes a GraphQL interface for querying and mutating the graph database.
//!
//! ## Rust equivalent
//! Uses `async-graphql` (the most feature-complete Rust GraphQL library),
//! which is directly equivalent to gqlgen in feature set.

use async_graphql::{Context, Object, Schema, SimpleObject};
use copperdb_storage::{NodeRecord, StorageEngine};
use parking_lot::Mutex;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GraphQlError {
    #[error("schema error: {0}")]
    Schema(String),
    #[error("storage error: {0}")]
    Storage(#[from] copperdb_storage::StorageError),
    #[error("context error: {0}")]
    Context(String),
}

impl From<async_graphql::Error> for GraphQlError {
    fn from(err: async_graphql::Error) -> Self {
        GraphQlError::Context(err.message)
    }
}

/// GraphQL type for a graph Node.

/// GraphQL type for a graph Node.
#[derive(SimpleObject, Clone)]
pub struct GraphNode {
    pub id: String,
    pub labels: Vec<String>,
    pub properties: serde_json::Value,
}

impl From<NodeRecord> for GraphNode {
    fn from(node: NodeRecord) -> Self {
        Self {
            id: node.id,
            labels: node.labels,
            properties: serde_json::Value::Object(
                node.properties
                    .into_iter()
                    .map(|(k, v)| (k, v))
                    .collect(),
            ),
        }
    }
}

/// Shared engine handle accessible from GraphQL resolvers via Context.
/// Wrapped in a Mutex because `StorageEngine` (sled-backed) is `!Sync`.
pub struct GraphQlContext {
    pub engine: Arc<Mutex<StorageEngine>>,
}

/// Root query type.
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Fetch a node by ID.
    async fn node(&self, ctx: &Context<'_>, id: String) -> Option<GraphNode> {
        let gql_ctx = ctx.data::<GraphQlContext>().ok()?;
        let engine = gql_ctx.engine.lock();
        let node = engine.get_node_record(&id).ok()??;
        Some(GraphNode::from(node))
    }

    /// List all nodes.
    async fn nodes(&self, ctx: &Context<'_>) -> Result<Vec<GraphNode>, GraphQlError> {
        let gql_ctx = ctx.data::<GraphQlContext>()?;
        let engine = gql_ctx.engine.lock();
        let nodes = engine.all_node_records()?;
        Ok(nodes.into_iter().map(GraphNode::from).collect())
    }
}

/// Root mutation type.
pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// Create a node with the given labels and properties.
    async fn create_node(
        &self,
        ctx: &Context<'_>,
        id: Option<String>,
        labels: Vec<String>,
        properties: serde_json::Value,
    ) -> Result<GraphNode, GraphQlError> {
        let gql_ctx = ctx.data::<GraphQlContext>()?;
        let engine = gql_ctx.engine.lock();
        let node_id = id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let props = match properties {
            serde_json::Value::Object(map) => {
                map.into_iter().collect()
            }
            _ => std::collections::BTreeMap::new(),
        };
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let node = NodeRecord {
            id: node_id,
            labels,
            properties: props,
            named_embeddings: std::collections::BTreeMap::new(),
            chunk_embeddings: vec![],
            embed_meta: copperdb_storage::NodeEmbeddingMetadata::default(),
            created_at_unix_ms: created_at,
            updated_at_unix_ms: created_at,
        };
        engine.put_node_record(&node)?;
        Ok(GraphNode::from(node))
    }
}

/// Wrapper type that exposes the async-graphql Schema publicly.
#[derive(Clone)]
pub struct GraphQlSchema(pub async_graphql::Schema<QueryRoot, MutationRoot, async_graphql::EmptySubscription>);

impl GraphQlSchema {
    /// Execute a GraphQL request against the schema.
    pub async fn execute(&self, request: async_graphql::Request) -> async_graphql::Response {
        self.0.execute(request).await
    }
}

/// Build the copperdb GraphQL schema backed by the given storage engine.
pub fn build_schema(
    engine: Arc<Mutex<StorageEngine>>,
) -> GraphQlSchema {
    GraphQlSchema(
        async_graphql::Schema::build(QueryRoot, MutationRoot, async_graphql::EmptySubscription)
            .data(GraphQlContext { engine })
            .finish(),
    )
}

/// Build a default, non-functional schema for testing/fixtures (no storage backend).
pub fn build_default_schema() -> GraphQlSchema {
    let engine = Arc::new(Mutex::new(StorageEngine::open_temporary().unwrap()));
    build_schema(engine)
}
