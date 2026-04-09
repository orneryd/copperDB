//! HTTP/REST API server for magnetDB.
//!
//! Equivalent to Go's `pkg/server` in NornicDB.
//! Provides a management REST API and serves the GraphQL endpoint.
//! Uses `axum` (Rust equivalent of Go's `net/http` + `gorilla/mux`).

use axum::{
    routing::{get, post},
    Router,
    Json,
    extract::State,
};
use std::sync::Arc;
use thiserror::Error;
use serde::{Deserialize, Serialize};

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("bind error: {0}")]
    Bind(String),
}

/// Application state shared across request handlers.
#[derive(Clone)]
pub struct AppState {
    // TODO: Add storage engine, auth manager, etc.
}

/// Server health check response.
#[derive(Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

/// Build the application router.
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/db/data/cypher", post(cypher_handler))
        .with_state(state)
}

/// GET /health — liveness probe.
async fn health_handler() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".into(),
        version: env!("CARGO_PKG_VERSION").into(),
    })
}

/// POST /db/data/cypher — execute a Cypher query (HTTP API).
async fn cypher_handler(
    State(_state): State<Arc<AppState>>,
    Json(_body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    // TODO: Wire up to eval engine
    Json(serde_json::json!({"results": [], "errors": []}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[tokio::test]
    async fn test_health_endpoint() {
        let state = Arc::new(AppState {});
        let app = build_router(state);
        let response = axum::serve(
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
            app,
        );
        // Basic compile test - actual HTTP test would use axum-test or similar
        let _ = response;
    }
}
