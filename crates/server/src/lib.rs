//! HTTP/REST API server for copperdb.
//!
//! Equivalent to Go's `pkg/server` in NornicDB.
//! Provides a management REST API and serves the GraphQL endpoint.
//! Uses `axum` (Rust equivalent of Go's `net/http` + `gorilla/mux`).

use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use copperdb_auth::{AuthError, TokenManager};
use copperdb_copperdb::{copperdb as GraphEngine, DatabaseConfig as EngineConfig};
use copperdb_multidb::{DatabaseManager, DatabaseStatus, MultiDbError};
use copperdb_retention::{
    ErasureRequest, LegalHold, Manager as RetentionManager, Policy, RetentionError,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("bind error: {0}")]
    Bind(String),
    #[error("engine error: {0}")]
    Engine(String),
}

#[derive(Clone)]
pub struct AuthState {
    pub security_enabled: bool,
    pub dev_login_enabled: bool,
    pub username: String,
    pub password: String,
    pub cookie_name: String,
    pub token_manager: Arc<TokenManager>,
}

impl Default for AuthState {
    fn default() -> Self {
        let username = std::env::var("COPPERDB_AUTH_USERNAME").unwrap_or_else(|_| "admin".into());
        let password =
            std::env::var("COPPERDB_AUTH_PASSWORD").unwrap_or_else(|_| "password".into());
        let secret = std::env::var("COPPERDB_AUTH_JWT_SECRET")
            .unwrap_or_else(|_| "copperdb-development-secret-change-me".into());
        Self {
            security_enabled: true,
            dev_login_enabled: true,
            username,
            password,
            cookie_name: "nornicdb_token".into(),
            token_manager: Arc::new(TokenManager::new(secret)),
        }
    }
}

/// Application state shared across request handlers.
#[derive(Clone)]
pub struct AppState {
    pub db_name: String,
    /// Shared retention manager for policy/hold/erasure CRUD.
    pub retention: Arc<RwLock<RetentionManager>>,
    /// Optional path to a directory of static UI files to serve at `/`.
    pub static_dir: Option<String>,
    /// Base path for reverse proxy deployments.
    pub base_path: String,
    /// Disable the browser UI when true.
    pub headless: bool,
    /// Registered logical databases.
    pub db_manager: Arc<DatabaseManager>,
    /// Browser and API authentication settings.
    pub auth: AuthState,
}

impl Default for AppState {
    fn default() -> Self {
        let db_manager = Arc::new(DatabaseManager::new());
        let _ = db_manager.create("copperdb", "./data/copperdb");
        Self {
            db_name: "copperdb".into(),
            retention: Arc::new(RwLock::new(RetentionManager::new())),
            static_dir: None,
            base_path: "/".into(),
            headless: false,
            db_manager,
            auth: AuthState::default(),
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
    let mut router = Router::new()
        .route("/", get(root_handler))
        .route("/login", get(ui_handler))
        .route("/security", get(ui_handler))
        .route("/security/{*path}", get(ui_handler))
        .route("/databases", get(ui_handler))
        .route("/assets/{*path}", get(asset_handler))
        .route("/favicon.ico", get(favicon_handler))
        .route("/nornicdb.svg", get(nornic_logo_handler))
        .route("/copperdb.svg", get(copper_logo_handler))
        .route("/health", get(health_handler))
        .route("/status", get(status_handler))
        .route("/auth/config", get(auth_config_handler))
        .route("/auth/token", post(auth_token_handler))
        .route("/auth/logout", post(auth_logout_handler))
        .route("/auth/me", get(auth_me_handler))
        .route("/db/{database}", get(database_info_handler))
        .route("/db/{database}/tx/commit", post(neo4j_tx_commit_handler))
        .route("/db/data/cypher", post(cypher_handler))
        // ── Retention policies ──────────────────────────────────────────────
        .route(
            "/admin/retention/policies",
            get(list_policies).post(create_policy),
        )
        .route(
            "/admin/retention/policies/defaults",
            post(load_default_policies),
        )
        .route(
            "/admin/retention/policies/{id}",
            get(get_policy).put(update_policy).delete(delete_policy),
        )
        // ── Legal holds ─────────────────────────────────────────────────────
        .route("/admin/retention/holds", get(list_holds).post(place_hold))
        .route("/admin/retention/holds/{id}", delete(release_hold))
        // ── Erasure requests ─────────────────────────────────────────────────
        .route(
            "/admin/retention/erasures",
            get(list_erasures).post(create_erasure),
        )
        .route(
            "/admin/retention/erasures/{id}/process",
            post(process_erasure),
        )
        // ── Sweep / status ───────────────────────────────────────────────────
        .route("/admin/retention/sweep", post(retention_sweep))
        .route("/admin/retention/status", get(retention_status));

    let normalized = normalize_base_path(&state.base_path);
    if normalized == "/" {
        router.with_state(state)
    } else {
        Router::new().nest(&normalized, router).with_state(state)
    }
}

fn normalize_base_path(base_path: &str) -> String {
    let trimmed = base_path.trim();
    if trimmed.is_empty() || trimmed == "/" {
        "/".into()
    } else {
        format!("/{}", trimmed.trim_matches('/'))
    }
}

fn base_prefix(state: &AppState) -> String {
    let normalized = normalize_base_path(&state.base_path);
    if normalized == "/" {
        String::new()
    } else {
        normalized
    }
}

fn host_for_request(headers: &HeaderMap, state: &AppState) -> String {
    headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .map(|host| host.to_string())
        .unwrap_or_else(|| format!("localhost:7474"))
}

fn bolt_host(host: &str) -> String {
    host.split(':').next().unwrap_or("localhost").to_string()
}

fn is_ui_request(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.contains("text/html"))
        .unwrap_or(false)
        || headers
            .get("sec-fetch-dest")
            .and_then(|value| value.to_str().ok())
            .map(|value| value.eq_ignore_ascii_case("document"))
            .unwrap_or(false)
}

fn static_root(state: &AppState) -> Option<PathBuf> {
    state.static_dir.as_ref().map(PathBuf::from)
}

fn read_static_file(state: &AppState, relative_path: &str) -> Option<Vec<u8>> {
    let root = static_root(state)?;
    let path = root.join(relative_path.trim_start_matches('/'));
    std::fs::read(path).ok()
}

fn rewrite_index_html(state: &AppState, html: String) -> String {
    let base = base_prefix(state);
    if base.is_empty() {
        html
    } else {
        html.replace("\"/assets/", &format!("\"{}/assets/", base))
            .replace("\"/favicon.ico\"", &format!("\"{}/favicon.ico\"", base))
            .replace("\"/nornicdb.svg\"", &format!("\"{}/nornicdb.svg\"", base))
            .replace("\"/copperdb.svg\"", &format!("\"{}/copperdb.svg\"", base))
    }
}

fn content_type_for(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or_default() {
        "html" => "text/html; charset=utf-8",
        "js" => "application/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "svg" => "image/svg+xml",
        "ico" => "image/x-icon",
        "png" => "image/png",
        "map" => "application/json; charset=utf-8",
        _ => "application/octet-stream",
    }
}

fn binary_response(status: StatusCode, content_type: &str, bytes: Vec<u8>) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::from(bytes))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

fn serve_ui_index(state: &AppState) -> Response {
    if state.headless {
        return StatusCode::NOT_FOUND.into_response();
    }

    if let Some(bytes) = read_static_file(state, "index.html") {
        if let Ok(contents) = String::from_utf8(bytes) {
            return Html(rewrite_index_html(state, contents)).into_response();
        }
    }

    StatusCode::SERVICE_UNAVAILABLE.into_response()
}

async fn root_handler(State(state): State<Arc<AppState>>, headers: HeaderMap) -> impl IntoResponse {
    if is_ui_request(&headers) && !state.headless {
        return serve_ui_index(&state);
    }

    let host = host_for_request(&headers, &state);
    let bolt_host = bolt_host(&host);
    let base = base_prefix(&state);
    Json(serde_json::json!({
        "bolt_direct": format!("bolt://{}:7687", bolt_host),
        "bolt_routing": format!("neo4j://{}:7687", bolt_host),
        "transaction": format!("http://{}{}/db/{{databaseName}}/tx", host, base),
        "neo4j_version": "5.0.0",
        "neo4j_edition": "community",
        "default_database": state.db_name,
    }))
    .into_response()
}

async fn ui_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    serve_ui_index(&state)
}

async fn asset_handler(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
) -> impl IntoResponse {
    let relative_path = format!("assets/{}", path);
    match read_static_file(&state, &relative_path) {
        Some(bytes) => binary_response(StatusCode::OK, content_type_for(&relative_path), bytes),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn favicon_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match read_static_file(&state, "favicon.ico") {
        Some(bytes) => binary_response(StatusCode::OK, "image/x-icon", bytes),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn nornic_logo_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match read_static_file(&state, "nornicdb.svg")
        .or_else(|| read_static_file(&state, "copperdb.svg"))
    {
        Some(bytes) => binary_response(StatusCode::OK, "image/svg+xml", bytes),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn copper_logo_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match read_static_file(&state, "copperdb.svg") {
        Some(bytes) => binary_response(StatusCode::OK, "image/svg+xml", bytes),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// GET /health — liveness probe.
async fn health_handler() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".into(),
        version: env!("CARGO_PKG_VERSION").into(),
    })
}

async fn status_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if state.auth.security_enabled && authenticated_user(&state, &headers).is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let databases = state.db_manager.list();
    Json(serde_json::json!({
        "status": "running",
        "server": {
            "uptime_seconds": 0,
            "requests": 0,
            "errors": 0,
            "active": 0,
            "version": env!("CARGO_PKG_VERSION"),
        },
        "database": {
            "nodes": 0,
            "edges": 0,
            "databases": databases.iter().filter(|db| db.name != "system").count(),
        }
    }))
    .into_response()
}

#[derive(Serialize)]
struct AuthConfigResponse {
    #[serde(rename = "devLoginEnabled")]
    dev_login_enabled: bool,
    #[serde(rename = "securityEnabled")]
    security_enabled: bool,
    #[serde(rename = "oauthProviders")]
    oauth_providers: Vec<OAuthProvider>,
}

#[derive(Serialize)]
struct OAuthProvider {
    name: String,
    url: String,
    #[serde(rename = "displayName")]
    display_name: String,
}

async fn auth_config_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(AuthConfigResponse {
        dev_login_enabled: state.auth.dev_login_enabled,
        security_enabled: state.auth.security_enabled,
        oauth_providers: vec![],
    })
}

#[derive(Deserialize)]
struct AuthTokenRequest {
    username: String,
    password: String,
    grant_type: Option<String>,
}

async fn auth_token_handler(
    State(state): State<Arc<AppState>>,
    Json(request): Json<AuthTokenRequest>,
) -> impl IntoResponse {
    if let Some(grant_type) = &request.grant_type {
        if grant_type != "password" {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"message": "unsupported grant_type"})),
            )
                .into_response();
        }
    }

    if request.username != state.auth.username || request.password != state.auth.password {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"message": AuthError::InvalidCredentials.to_string()})),
        )
            .into_response();
    }

    let token = match state.auth.token_manager.issue(
        &request.username,
        vec!["admin".into()],
        7 * 24 * 60 * 60,
    ) {
        Ok(token) => token,
        Err(error) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"message": error.to_string()})),
            )
                .into_response();
        }
    };

    let cookie = format!(
        "{}={}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}",
        state.auth.cookie_name,
        token,
        7 * 24 * 60 * 60
    );

    (
        StatusCode::OK,
        [(header::SET_COOKIE, cookie)],
        Json(serde_json::json!({
            "access_token": token,
            "token_type": "Bearer",
            "expires_in": 7 * 24 * 60 * 60,
        })),
    )
        .into_response()
}

async fn auth_logout_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [(
            header::SET_COOKIE,
            format!("{}=; Path=/; HttpOnly; Max-Age=0", state.auth.cookie_name),
        )],
        Json(serde_json::json!({"status": "logged out"})),
    )
        .into_response()
}

async fn auth_me_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    match authenticated_user(&state, &headers) {
        Some(claims) => Json(serde_json::json!({
            "id": claims.sub,
            "username": claims.sub,
            "roles": claims.roles,
            "auth_method": "password",
        }))
        .into_response(),
        None => StatusCode::UNAUTHORIZED.into_response(),
    }
}

fn authenticated_user(state: &AppState, headers: &HeaderMap) -> Option<copperdb_auth::Claims> {
    let cookie_header = headers.get(header::COOKIE)?.to_str().ok()?;
    let token = cookie_header
        .split(';')
        .map(str::trim)
        .find_map(|pair| pair.strip_prefix(&format!("{}=", state.auth.cookie_name)))?;
    state.auth.token_manager.verify(token).ok()
}

#[derive(Deserialize)]
struct Neo4jStatement {
    statement: String,
    parameters: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Deserialize)]
struct Neo4jCommitRequest {
    statements: Vec<Neo4jStatement>,
}

#[derive(Serialize)]
struct Neo4jCommitResponse {
    results: Vec<Neo4jResult>,
    errors: Vec<Neo4jError>,
}

#[derive(Serialize)]
struct Neo4jResult {
    columns: Vec<String>,
    data: Vec<Neo4jRow>,
}

#[derive(Serialize)]
struct Neo4jRow {
    row: Vec<serde_json::Value>,
    meta: Vec<serde_json::Value>,
}

#[derive(Serialize)]
struct Neo4jError {
    code: String,
    message: String,
}

async fn database_info_handler(
    State(state): State<Arc<AppState>>,
    Path(database): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if state.auth.security_enabled && authenticated_user(&state, &headers).is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    match state.db_manager.get(&database) {
        Some(db) => {
            let storage_bytes = open_engine(&state, &database)
                .map(|engine| engine.size_on_disk())
                .unwrap_or(0);
            Json(serde_json::json!({
                "name": db.name,
                "status": database_status_name(db.status),
                "default": db.name == state.db_name,
                "type": if db.name == "system" { "system" } else { "standard" },
                "nodeCount": 0,
                "edgeCount": 0,
                "nodeStorageBytes": storage_bytes,
                "managedEmbeddingBytes": 0,
                "searchReady": false,
                "searchBuilding": false,
                "searchInitialized": false,
            }))
            .into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn neo4j_tx_commit_handler(
    State(state): State<Arc<AppState>>,
    Path(database): Path<String>,
    headers: HeaderMap,
    Json(request): Json<Neo4jCommitRequest>,
) -> impl IntoResponse {
    if state.auth.security_enabled && authenticated_user(&state, &headers).is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let mut results = Vec::new();
    let mut errors = Vec::new();

    for statement in request.statements {
        match execute_statement(
            &state,
            &database,
            &statement.statement,
            statement.parameters.unwrap_or_default(),
        ) {
            Ok(result) => results.push(result),
            Err(error) => errors.push(Neo4jError {
                code: "Neo.ClientError.Statement.ExecutionFailed".into(),
                message: error,
            }),
        }
    }

    Json(Neo4jCommitResponse { results, errors }).into_response()
}

fn execute_statement(
    state: &AppState,
    database: &str,
    statement: &str,
    parameters: HashMap<String, serde_json::Value>,
) -> Result<Neo4jResult, String> {
    let normalized = statement.trim();
    let upper = normalized.to_ascii_uppercase();

    if database == "system" {
        if upper == "SHOW DATABASES" {
            return Ok(show_databases_result(state));
        }
        if upper.starts_with("CREATE DATABASE ") {
            let name = parse_database_name(normalized, "CREATE DATABASE ")?;
            create_database(state, &name)?;
            return Ok(empty_neo4j_result());
        }
        if upper.starts_with("DROP DATABASE ") {
            let name = parse_database_name(normalized, "DROP DATABASE ")?;
            drop_database(state, &name)?;
            return Ok(empty_neo4j_result());
        }
        return Err(format!("unsupported system statement: {}", statement));
    }

    if state.db_manager.get(database).is_none() {
        create_database(state, database)?;
    }

    let engine = open_engine(state, database)?;
    let result = engine
        .execute(normalized, parameters)
        .map_err(|error| error.to_string())?;
    Ok(convert_engine_result(result))
}

fn convert_engine_result(result: copperdb_copperdb::QueryResult) -> Neo4jResult {
    let columns = result.columns;
    let data = result
        .rows
        .into_iter()
        .map(|row| Neo4jRow {
            row: columns
                .iter()
                .map(|column| row.get(column).cloned().unwrap_or(serde_json::Value::Null))
                .collect(),
            meta: vec![],
        })
        .collect();
    Neo4jResult { columns, data }
}

fn show_databases_result(state: &AppState) -> Neo4jResult {
    let columns = vec![
        "name".into(),
        "type".into(),
        "access".into(),
        "role".into(),
        "status".into(),
        "default".into(),
    ];
    let data = state
        .db_manager
        .list()
        .into_iter()
        .map(|db| Neo4jRow {
            row: vec![
                serde_json::Value::String(db.name.clone()),
                serde_json::Value::String(
                    if db.name == "system" {
                        "system"
                    } else {
                        "standard"
                    }
                    .into(),
                ),
                serde_json::Value::String("read-write".into()),
                serde_json::Value::String("primary".into()),
                serde_json::Value::String(database_status_name(db.status).into()),
                serde_json::Value::Bool(db.name == state.db_name),
            ],
            meta: vec![],
        })
        .collect();
    Neo4jResult { columns, data }
}

fn empty_neo4j_result() -> Neo4jResult {
    Neo4jResult {
        columns: vec![],
        data: vec![],
    }
}

fn parse_database_name(statement: &str, prefix: &str) -> Result<String, String> {
    let suffix = statement
        .trim()
        .strip_prefix(prefix)
        .ok_or_else(|| format!("invalid database statement: {}", statement))?
        .trim();
    let token = suffix
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .trim_matches('`')
        .trim_matches(';');
    if token.is_empty() {
        Err("database name is required".into())
    } else {
        Ok(token.into())
    }
}

fn create_database(state: &AppState, name: &str) -> Result<(), String> {
    let path = format!("./data/{}", name);
    match state.db_manager.create(name, path) {
        Ok(()) => Ok(()),
        Err(MultiDbError::AlreadyExists(_)) => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

fn drop_database(state: &AppState, name: &str) -> Result<(), String> {
    DatabaseManager::drop(&state.db_manager, name).map_err(|error| error.to_string())
}

fn open_engine(state: &AppState, database: &str) -> Result<GraphEngine, String> {
    let config = EngineConfig {
        data_dir: format!("data/{}", database),
        default_database: database.into(),
        auth_enabled: state.auth.security_enabled,
        log_queries: false,
        ..Default::default()
    };
    GraphEngine::open(config).map_err(|error| error.to_string())
}

fn database_status_name(status: DatabaseStatus) -> &'static str {
    match status {
        DatabaseStatus::Online => "online",
        DatabaseStatus::Offline => "offline",
        DatabaseStatus::Deleted => "deleted",
    }
}

/// POST /db/data/cypher — execute a Cypher query (HTTP API).
async fn cypher_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CypherRequest>,
) -> impl IntoResponse {
    if body.query.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(CypherResponse::error("query must not be empty")),
        );
    }
    match open_engine(&state, &state.db_name).and_then(|engine| {
        engine
            .execute(&body.query, HashMap::new())
            .map_err(|error| error.to_string())
    }) {
        Ok(result) => {
            let rows = result
                .rows
                .into_iter()
                .map(|row| {
                    result
                        .columns
                        .iter()
                        .map(|column| row.get(column).cloned().unwrap_or(serde_json::Value::Null))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            (
                StatusCode::OK,
                Json(CypherResponse {
                    columns: result.columns,
                    rows,
                    errors: vec![],
                    stats: Some(serde_json::json!({
                        "execution_time_ms": result.stats.execution_time_ms,
                    })),
                }),
            )
        }
        Err(error) => (StatusCode::OK, Json(CypherResponse::error(error))),
    }
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
        Ok(()) => (
            StatusCode::CREATED,
            Json(serde_json::json!({"status": "created"})),
        ),
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
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({"status": "updated"})),
        ),
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
            Json(
                serde_json::json!({"error": format!("active legal hold prevents erasure for subject: {sid}")}),
            ),
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
