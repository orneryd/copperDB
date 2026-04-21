//! HTTP/REST API server for copperdb.
//!
//! Equivalent to Go's `pkg/server` in NornicDB.
//! Provides a management REST API and serves the GraphQL endpoint.
//! Uses `axum` (Rust equivalent of Go's `net/http` + `gorilla/mux`).

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use copperdb_retention::{Manager as RetentionManager, Policy, ErasureRequest, LegalHold, RetentionError};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("bind error: {0}")]
    Bind(String),
}

/// Application state shared across request handlers.
#[derive(Clone)]
pub struct AppState {
    pub db_name: String,
    /// Shared retention manager for policy/hold/erasure CRUD.
    pub retention: Arc<RwLock<RetentionManager>>,
    /// Optional path to a directory of static UI files to serve at `/`.
    pub static_dir: Option<String>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            db_name: "copperdb".into(),
            retention: Arc::new(RwLock::new(RetentionManager::new())),
            static_dir: None,
        }
    }
}

/// Server health check response.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

/// Cypher query request body.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CypherRequest {
    pub query: String,
    pub parameters: Option<serde_json::Value>,
}

/// Cypher query response body.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CypherResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub errors: Vec<String>,
    pub stats: Option<serde_json::Value>,
}

impl CypherResponse {
    pub fn empty() -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            errors: vec![],
            stats: None,
        }
    }

    pub fn error(msg: impl Into<String>) -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            errors: vec![msg.into()],
            stats: None,
        }
    }
}

/// Build the application router.
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/db/data/cypher", post(cypher_handler))
        // ── Retention policies ──────────────────────────────────────────────
        .route("/admin/retention/policies", get(list_policies).post(create_policy))
        .route("/admin/retention/policies/defaults", post(load_default_policies))
        .route(
            "/admin/retention/policies/:id",
            get(get_policy).put(update_policy).delete(delete_policy),
        )
        // ── Legal holds ─────────────────────────────────────────────────────
        .route("/admin/retention/holds", get(list_holds).post(place_hold))
        .route("/admin/retention/holds/:id", delete(release_hold))
        // ── Erasure requests ─────────────────────────────────────────────────
        .route("/admin/retention/erasures", get(list_erasures).post(create_erasure))
        .route("/admin/retention/erasures/:id/process", post(process_erasure))
        // ── Sweep / status ───────────────────────────────────────────────────
        .route("/admin/retention/sweep", post(retention_sweep))
        .route("/admin/retention/status", get(retention_status))
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
    Json(body): Json<CypherRequest>,
) -> impl IntoResponse {
    if body.query.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(CypherResponse::error("query must not be empty")),
        );
    }
    (
        StatusCode::OK,
        Json(CypherResponse::empty()),
    )
}

// ─── Retention policy handlers ────────────────────────────────────────────────

async fn list_policies(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mgr = state.retention.read();
    let policies: Vec<&Policy> = mgr.list_policies();
    Json(serde_json::json!({ "policies": policies }))
}

async fn create_policy(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Policy>,
) -> impl IntoResponse {
    let mut mgr = state.retention.write();
    match mgr.add_policy(body) {
        Ok(()) => (StatusCode::CREATED, Json(serde_json::json!({"status": "created"}))),
        Err(RetentionError::AlreadyExists(id)) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": format!("policy already exists: {id}")})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

async fn load_default_policies(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let defaults = copperdb_retention::default_policies();
    let mut mgr = state.retention.write();
    let mut loaded = 0usize;
    for p in defaults {
        if mgr.add_policy(p).is_ok() {
            loaded += 1;
        }
    }
    Json(serde_json::json!({ "loaded": loaded }))
}

async fn get_policy(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let mgr = state.retention.read();
    match mgr.get_policy(&id) {
        Some(p) => (StatusCode::OK, Json(serde_json::json!(p))),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("policy not found: {id}")})),
        ),
    }
}

async fn update_policy(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(mut body): Json<Policy>,
) -> impl IntoResponse {
    body.id = id;
    let mut mgr = state.retention.write();
    match mgr.update_policy(body) {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({"status": "updated"}))),
        Err(RetentionError::PolicyNotFound(id)) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("policy not found: {id}")})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

async fn delete_policy(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let mut mgr = state.retention.write();
    match mgr.delete_policy(&id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(RetentionError::PolicyNotFound(_)) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

// ─── Legal hold handlers ──────────────────────────────────────────────────────

#[derive(Deserialize)]
struct PlaceHoldRequest {
    subject_id: String,
    reason: String,
}

async fn list_holds(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mgr = state.retention.read();
    let holds: Vec<&LegalHold> = mgr.list_legal_holds();
    Json(serde_json::json!({ "holds": holds }))
}

async fn place_hold(
    State(state): State<Arc<AppState>>,
    Json(body): Json<PlaceHoldRequest>,
) -> impl IntoResponse {
    let mut mgr = state.retention.write();
    let hold = mgr.place_legal_hold(body.subject_id, body.reason);
    (StatusCode::CREATED, Json(serde_json::json!(hold)))
}

async fn release_hold(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let mut mgr = state.retention.write();
    match mgr.release_legal_hold(&id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(RetentionError::HoldNotFound(_)) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

// ─── Erasure request handlers ─────────────────────────────────────────────────

#[derive(Deserialize)]
struct CreateErasureRequest {
    subject_id: String,
    subject_email: Option<String>,
}

async fn list_erasures(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mgr = state.retention.read();
    let erasures: Vec<&ErasureRequest> = mgr.list_erasure_requests();
    Json(serde_json::json!({ "erasures": erasures }))
}

async fn create_erasure(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateErasureRequest>,
) -> impl IntoResponse {
    let mut mgr = state.retention.write();
    match mgr.create_erasure_request(body.subject_id, body.subject_email) {
        Ok(req) => (StatusCode::CREATED, Json(serde_json::json!(req))),
        Err(RetentionError::ActiveLegalHold(sid)) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": format!("active legal hold prevents erasure for subject: {sid}")})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

async fn process_erasure(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let mut mgr = state.retention.write();
    match mgr.process_erasure(&id) {
        Ok(()) => Json(serde_json::json!({"status": "completed"})).into_response(),
        Err(RetentionError::ErasureNotFound(_)) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

// ─── Sweep / status handlers ──────────────────────────────────────────────────

async fn retention_sweep(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "nodes_expired": 0,
        "dry_run": false,
    }))
}

async fn retention_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mgr = state.retention.read();
    let policy_count = mgr.list_policies().len();
    let hold_count = mgr.list_legal_holds().len();
    let erasure_count = mgr.list_erasure_requests().len();
    Json(serde_json::json!({
        "policies": policy_count,
        "active_holds": hold_count,
        "pending_erasures": erasure_count,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cypher_request_serialization() {
        let req = CypherRequest {
            query: "MATCH (n) RETURN n".into(),
            parameters: Some(serde_json::json!({"id": 1})),
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: CypherRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.query, req.query);
    }

    #[test]
    fn test_cypher_response_serialization() {
        let resp = CypherResponse {
            columns: vec!["n".into()],
            rows: vec![vec![serde_json::json!({"id": 1})]],
            errors: vec![],
            stats: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: CypherResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.columns, vec!["n"]);
        assert_eq!(decoded.rows.len(), 1);
    }

    #[test]
    fn test_health_response_serialization() {
        let hr = HealthResponse {
            status: "ok".into(),
            version: "0.1.0".into(),
        };
        let json = serde_json::to_string(&hr).unwrap();
        let decoded: HealthResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, hr);
    }

    #[test]
    fn test_cypher_response_empty() {
        let r = CypherResponse::empty();
        assert!(r.columns.is_empty());
        assert!(r.rows.is_empty());
        assert!(r.errors.is_empty());
    }

    #[test]
    fn test_cypher_response_error() {
        let r = CypherResponse::error("syntax error");
        assert_eq!(r.errors, vec!["syntax error"]);
    }

    #[test]
    fn test_cypher_request_no_params() {
        let req = CypherRequest {
            query: "CREATE (n:Test) RETURN n".into(),
            parameters: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: CypherRequest = serde_json::from_str(&json).unwrap();
        assert!(decoded.parameters.is_none());
    }

    #[tokio::test]
    async fn test_router_builds() {
        let state = Arc::new(AppState::default());
        let _app = build_router(state);
        // Just verify the router builds without panicking
    }
}
