//! GraphQL API layer for copperdb.
//!
//! Equivalent to Go's `pkg/graphql` in NornicDB (uses `github.com/99designs/gqlgen`).
//! Exposes a GraphQL interface for querying and mutating the graph database.
//!
//! ## Rust equivalent
//! Uses `async-graphql` (the most feature-complete Rust GraphQL library),
//! which is directly equivalent to gqlgen in feature set.

use async_graphql::{Context, Object, Schema, SimpleObject};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GraphQlError {
    #[error("schema error: {0}")]
    SchemaError(String),
}

/// Example GraphQL type for a graph Node.
#[derive(SimpleObject, Clone)]
pub struct GraphNode {
    pub id: String,
    pub labels: Vec<String>,
    pub properties: serde_json::Value,
}

/// Root query type.
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Fetch a node by ID.
    async fn node(&self, _ctx: &Context<'_>, id: String) -> Option<GraphNode> {
        // TODO: Wire up to storage engine
        Some(GraphNode {
            id,
            labels: vec!["Node".into()],
            properties: serde_json::json!({}),
        })
    }
}

/// Root mutation type.
pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// Create a node with the given labels and properties.
    async fn create_node(
        &self,
        _ctx: &Context<'_>,
        labels: Vec<String>,
        properties: serde_json::Value,
    ) -> GraphNode {
        // TODO: Wire up to storage engine
        GraphNode {
            id: uuid::Uuid::new_v4().to_string(),
            labels,
            properties,
        }
    }
}

/// Build the copperdb GraphQL schema.
pub fn build_schema() -> Schema<QueryRoot, MutationRoot, async_graphql::EmptySubscription> {
    Schema::new(QueryRoot, MutationRoot, async_graphql::EmptySubscription)
}
