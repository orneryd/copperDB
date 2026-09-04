//! GraphQL API layer for copperdb.
//!
//! Equivalent to Go's `pkg/graphql` in NornicDB (uses `github.com/99designs/gqlgen`).
//! Exposes a GraphQL interface for querying and mutating the graph database.
//!
//! ## Rust equivalent
//! Uses `async-graphql` (the most feature-complete Rust GraphQL library),
//! which is directly equivalent to gqlgen in feature set.

use async_graphql::{Context, Object, SimpleObject};
use copperdb_storage::{NodeRecord, StorageEngine, StorageError};
use copperdb_util::{RequestCancelled, RequestContext};
use parking_lot::Mutex;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GraphQlError {
    #[error("schema error: {0}")]
    Schema(String),
    #[error("storage error: {0}")]
    Storage(StorageError),
    #[error(transparent)]
    RequestCancelled(#[from] RequestCancelled),
    #[error("context error: {0}")]
    Context(String),
}

impl From<StorageError> for GraphQlError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::RequestCancelled(cancelled) => Self::RequestCancelled(cancelled),
            error => Self::Storage(error),
        }
    }
}

impl From<async_graphql::Error> for GraphQlError {
    fn from(err: async_graphql::Error) -> Self {
        GraphQlError::Context(err.message)
    }
}

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
            properties: serde_json::Value::Object(node.properties.into_iter().collect()),
        }
    }
}

/// Shared engine handle accessible from GraphQL resolvers via Context.
/// Wrapped in a Mutex because `StorageEngine` (fjall-backed) is `!Sync`.
pub struct GraphQlContext {
    pub engine: Arc<Mutex<StorageEngine>>,
}

/// Root query type.
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Fetch a node by ID.
    async fn node(&self, ctx: &Context<'_>, id: String) -> Result<Option<GraphNode>, GraphQlError> {
        let request_context = ctx.data::<RequestContext>()?;
        request_context.check_active()?;
        let gql_ctx = ctx.data::<GraphQlContext>()?;
        let engine = gql_ctx.engine.lock();
        let node = engine.get_node_record(&id)?;
        Ok(node.map(GraphNode::from))
    }

    /// List all nodes.
    async fn nodes(&self, ctx: &Context<'_>) -> Result<Vec<GraphNode>, GraphQlError> {
        let request_context = ctx.data::<RequestContext>()?;
        request_context.check_active()?;
        let gql_ctx = ctx.data::<GraphQlContext>()?;
        let engine = gql_ctx.engine.lock();
        let mut nodes = Vec::new();
        engine.stream_node_records_with_cancellation(request_context.cancellation(), |node| {
            nodes.push(GraphNode::from(node));
            Ok(())
        })?;
        Ok(nodes)
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
        let request_context = ctx.data::<RequestContext>()?;
        request_context.check_active()?;
        let gql_ctx = ctx.data::<GraphQlContext>()?;
        let engine = gql_ctx.engine.lock();
        let node_id = id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let props = match properties {
            serde_json::Value::Object(map) => map.into_iter().collect(),
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
        request_context.check_active()?;
        engine.put_node_record(&node)?;
        Ok(GraphNode::from(node))
    }
}

/// Wrapper type that exposes the async-graphql Schema publicly.
#[derive(Clone)]
pub struct GraphQlSchema(
    pub async_graphql::Schema<QueryRoot, MutationRoot, async_graphql::EmptySubscription>,
);

impl GraphQlSchema {
    /// Execute a GraphQL request against the schema.
    pub async fn execute(&self, request: async_graphql::Request) -> async_graphql::Response {
        self.execute_with_context(RequestContext::detached(), request)
            .await
    }

    pub async fn execute_with_context(
        &self,
        request_context: RequestContext,
        request: async_graphql::Request,
    ) -> async_graphql::Response {
        self.0.execute(request.data(request_context)).await
    }
}

/// Build the copperdb GraphQL schema backed by the given storage engine.
pub fn build_schema(engine: Arc<Mutex<StorageEngine>>) -> GraphQlSchema {
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_graphql::{Request, Variables};

    #[tokio::test]
    async fn cancelled_context_stops_node_listing() {
        let schema = build_default_schema();
        let request_context = RequestContext::detached();
        request_context.cancel();

        let response = schema
            .execute_with_context(request_context, Request::new("{ nodes { id } }"))
            .await;

        assert_eq!(response.errors.len(), 1);
        assert_eq!(response.errors[0].message, "request cancelled");
    }

    #[tokio::test]
    async fn cancelled_context_stops_node_creation_without_writing() {
        let engine = Arc::new(Mutex::new(StorageEngine::open_temporary().unwrap()));
        let schema = build_schema(Arc::clone(&engine));
        let request_context = RequestContext::detached();
        request_context.cancel();
        let request = Request::new(
            "mutation($properties: JSON!) { createNode(id: \"cancelled\", labels: [\"Test\"], properties: $properties) { id } }",
        )
        .variables(Variables::from_json(serde_json::json!({ "properties": {} })));

        let response = schema.execute_with_context(request_context, request).await;

        assert_eq!(response.errors.len(), 1);
        assert_eq!(response.errors[0].message, "request cancelled");
        assert!(
            engine
                .lock()
                .get_node_record("cancelled")
                .unwrap()
                .is_none()
        );
    }
}
