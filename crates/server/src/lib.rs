//! HTTP/REST API server for copperdb.
//!
//! Equivalent to Go's `pkg/server` in NornicDB.
//! Provides a management REST API and serves the GraphQL endpoint.
//! Uses `axum` (Rust equivalent of Go's `net/http` + `gorilla/mux`).

use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use copperdb_auth::{AuthConfig, AuthError, Authenticator, Claims, DatabaseAccessMode};
use copperdb_buildinfo::{display_version, server_announcement, version};
use copperdb_engine::{CopperDb as GraphEngine, DatabaseConfig as EngineConfig, QueryResult};
use copperdb_envutil::{get as env_get, get_bool_loose};
use copperdb_multidb::{DatabaseManager, DatabaseStatus, MultiDbError};
use copperdb_otel::{classify_cypher_op_type, Telemetry};
use copperdb_replication::{InMemoryReplicaTransport, MemoryStorage};
use copperdb_retention::{
    ErasureRequest, LegalHold, Manager as RetentionManager, Policy, RetentionError,
    RetentionSweepConfig,
};
use copperdb_security::{
    RequestTarget, RequestViolation, SecurityConfig, SecurityMiddleware, SecurityRequest,
};
use copperdb_storage::StorageEngine;
use copperdb_topology::{ConsistencyLevel, PlacementKey};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
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
    pub jwt_secret: String,
    pub auth_storage_path: String,
}

impl Default for AuthState {
    fn default() -> Self {
        let username = env_get("COPPERDB_AUTH_USERNAME", "admin");
        let password = env_get("COPPERDB_AUTH_PASSWORD", "password");
        let secret = env_get(
            "COPPERDB_AUTH_JWT_SECRET",
            "copperdb-development-secret-change-me",
        );
        let security_enabled = get_bool_loose("COPPERDB_SECURITY_ENABLED", true);
        let dev_login_enabled = get_bool_loose("COPPERDB_DEV_LOGIN_ENABLED", true);
        let default_storage_path = default_auth_storage_path();
        let storage_path = env_get("COPPERDB_AUTH_STORAGE_PATH", &default_storage_path);
        Self::from_storage_path(
            storage_path,
            security_enabled,
            dev_login_enabled,
            username,
            password,
            secret,
        )
        .expect("durable authenticator must initialize during server startup")
    }
}

impl AuthState {
    pub fn from_storage_path(
        auth_storage_path: String,
        security_enabled: bool,
        dev_login_enabled: bool,
        username: String,
        password: String,
        jwt_secret: String,
    ) -> Result<Self, AuthError> {
        let state = Self {
            security_enabled,
            dev_login_enabled,
            username,
            password,
            cookie_name: "nornicdb_token".into(),
            jwt_secret,
            auth_storage_path,
        };
        let authenticator = state.open_authenticator()?;
        authenticator.seed_builtin_access_if_empty()?;
        if state.security_enabled
            && matches!(
                authenticator.get_user(&state.username),
                Err(AuthError::UserNotFound(_))
            )
        {
            authenticator.create_user(&state.username, &state.password, vec!["admin".into()])?;
        }
        Ok(state)
    }

    fn auth_config(&self) -> AuthConfig {
        AuthConfig {
            jwt_secret: self.jwt_secret.clone().into_bytes(),
            token_expiry: Some(Duration::from_secs(7 * 24 * 60 * 60)),
            default_admin_username: self.username.clone(),
            security_enabled: self.security_enabled,
            ..Default::default()
        }
    }

    fn open_authenticator(&self) -> Result<Authenticator, AuthError> {
        let storage = Arc::new(StorageEngine::open(&self.auth_storage_path)?);
        Authenticator::new(self.auth_config(), storage)
    }
}

#[cfg(not(test))]
fn default_auth_storage_path() -> String {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(|crates_dir| crates_dir.parent())
        .unwrap_or(&manifest_dir)
        .join("data/copperdb-auth")
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
fn default_auth_storage_path() -> String {
    std::env::temp_dir()
        .join(format!("copperdb-auth-{}", uuid::Uuid::new_v4()))
        .to_string_lossy()
        .into_owned()
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
    /// Shared metrics surface (ported from NornicDB observability catalog).
    pub telemetry: Arc<Telemetry>,
    /// Protocol-neutral ingress validation for headers, tokens, and URL query parameters.
    pub security: SecurityMiddleware,
    /// Route supported Cypher requests through the distributed coordinator when enabled.
    pub distributed_cypher_enabled: bool,
}

impl Default for AppState {
    fn default() -> Self {
        let db_manager = Arc::new(
            DatabaseManager::open("./data/copperdb-multidb")
                .unwrap_or_else(|_| DatabaseManager::new()),
        );
        let _ = db_manager.create("copperdb", "./data/copperdb");
        let retention = db_manager
            .get("copperdb")
            .and_then(|database| RetentionManager::open(database.storage_path).ok())
            .unwrap_or_else(RetentionManager::new);
        Self {
            db_name: "copperdb".into(),
            retention: Arc::new(RwLock::new(retention)),
            static_dir: None,
            base_path: "/".into(),
            headless: false,
            db_manager,
            auth: AuthState::default(),
            telemetry: Arc::new(Telemetry::new()),
            security: SecurityMiddleware::with_config(SecurityConfig {
                environment: env_get("COPPERDB_ENV", "development"),
                allow_http: get_bool_loose("COPPERDB_ALLOW_HTTP", true),
            }),
            distributed_cypher_enabled: get_bool_loose("COPPERDB_DISTRIBUTED_CYPHER", false),
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
    let router = Router::new()
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
    let router = router.layer(middleware::from_fn_with_state(
        Arc::clone(&state),
        security_validation_middleware,
    ));

    if normalized == "/" {
        router.with_state(state)
    } else {
        Router::new().nest(&normalized, router).with_state(state)
    }
}

async fn security_validation_middleware(
    State(state): State<Arc<AppState>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let security_request = match security_request_from_http(&request) {
        Ok(request) => request,
        Err(response) => return response,
    };

    match state.security.validate_request(&security_request) {
        Ok(()) => next.run(request).await,
        Err(violation) => security_violation_response(violation),
    }
}

fn security_request_from_http(request: &Request<Body>) -> Result<SecurityRequest, Response> {
    let mut security_request = SecurityRequest::new();
    for (name, value) in request.headers() {
        let value = value
            .to_str()
            .map_err(|_| StatusCode::BAD_REQUEST.into_response())?;
        security_request = security_request.with_header(name.as_str(), value);
    }

    if let Some(query) = request.uri().query() {
        for (name, value) in url::form_urlencoded::parse(query.as_bytes()) {
            security_request =
                security_request.with_query_param(name.into_owned(), value.into_owned());
        }
    }

    Ok(security_request)
}

fn security_violation_response(violation: RequestViolation) -> Response {
    let status = match violation.target {
        RequestTarget::Authorization => StatusCode::UNAUTHORIZED,
        RequestTarget::QueryParam(ref name) if name == "token" => StatusCode::UNAUTHORIZED,
        RequestTarget::Header(_) | RequestTarget::QueryParam(_) => StatusCode::BAD_REQUEST,
    };
    (status, violation.source.to_string()).into_response()
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

fn host_for_request(headers: &HeaderMap, _state: &AppState) -> String {
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
        "server": server_announcement(),
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
async fn health_handler(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    let _ = state.telemetry.record_counter(
        "nornicdb_http_requests_total",
        &[
            ("method", "GET"),
            ("path_template", "/health"),
            ("status_class", "2xx"),
        ],
    );
    Json(HealthResponse {
        status: "ok".into(),
        version: version().into(),
    })
}

async fn status_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(status) = authorize_database_access(&state, &headers, &state.db_name, false) {
        return status.into_response();
    }

    let databases = state.db_manager.list();
    Json(serde_json::json!({
        "status": "running",
        "server": {
            "uptime_seconds": 0,
            "requests": 0,
            "errors": 0,
            "active": 0,
            "version": display_version(),
            "announcement": server_announcement(),
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
    let started = std::time::Instant::now();
    if let Some(grant_type) = &request.grant_type {
        if grant_type != "password" {
            let _ = state.telemetry.record_counter(
                "nornicdb_auth_attempts_total",
                &[("result", "denied"), ("protocol", "http")],
            );
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"message": "unsupported grant_type"})),
            )
                .into_response();
        }
    }

    let authenticator = match state.auth.open_authenticator() {
        Ok(authenticator) => authenticator,
        Err(error) => {
            let _ = state.telemetry.record_counter(
                "nornicdb_auth_attempts_total",
                &[("result", "failure"), ("protocol", "http")],
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"message": error.to_string()})),
            )
                .into_response();
        }
    };

    let (token_response, _user) =
        match authenticator.authenticate(&request.username, &request.password) {
            Ok(result) => result,
            Err(error) => {
                let _ = state.telemetry.record_counter(
                    "nornicdb_auth_attempts_total",
                    &[("result", "failure"), ("protocol", "http")],
                );
                let status = match error {
                    AuthError::InvalidCredentials
                    | AuthError::AccountLocked
                    | AuthError::UserDisabled => StatusCode::UNAUTHORIZED,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                return (
                    status,
                    Json(serde_json::json!({"message": error.to_string()})),
                )
                    .into_response();
            }
        };

    let _ = state.telemetry.record_counter(
        "nornicdb_auth_attempts_total",
        &[("result", "success"), ("protocol", "http")],
    );
    let _ = state.telemetry.observe_histogram(
        "nornicdb_http_request_duration_seconds",
        &[
            ("method", "POST"),
            ("path_template", "/auth/token"),
            ("status_class", "2xx"),
        ],
        started.elapsed().as_secs_f64(),
    );

    let cookie = format!(
        "{}={}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}",
        state.auth.cookie_name,
        token_response.access_token,
        token_response.expires_in.unwrap_or(7 * 24 * 60 * 60)
    );

    (
        StatusCode::OK,
        [(header::SET_COOKIE, cookie)],
        Json(serde_json::json!({
            "access_token": token_response.access_token,
            "token_type": "Bearer",
            "expires_in": token_response.expires_in.unwrap_or(7 * 24 * 60 * 60),
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

fn authenticated_user(state: &AppState, headers: &HeaderMap) -> Option<Claims> {
    let token = bearer_token(headers).or_else(|| cookie_token(state, headers))?;
    state
        .auth
        .open_authenticator()
        .ok()?
        .validate_token(token)
        .ok()
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .trim()
        .strip_prefix("Bearer ")
        .map(str::trim)
        .filter(|token| !token.is_empty())
}

fn cookie_token<'a>(state: &AppState, headers: &'a HeaderMap) -> Option<&'a str> {
    let cookie_header = headers.get(header::COOKIE)?.to_str().ok()?;
    cookie_header
        .split(';')
        .map(str::trim)
        .find_map(|pair| pair.strip_prefix(&format!("{}=", state.auth.cookie_name)))
}

fn authorize_database_access(
    state: &AppState,
    headers: &HeaderMap,
    database: &str,
    write: bool,
) -> Result<Option<Claims>, StatusCode> {
    if !state.auth.security_enabled {
        return Ok(None);
    }
    let claims = authenticated_user(state, headers).ok_or(StatusCode::UNAUTHORIZED)?;
    let roles = claims.roles.clone();
    let authenticator = state
        .auth
        .open_authenticator()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let access_mode = authenticator.allowlist.access_mode_for_roles(roles.clone());
    if !access_mode.can_access_database(database) {
        return Err(StatusCode::FORBIDDEN);
    }
    let resolved = authenticator.privileges.resolve(&roles, database);
    if (write && !resolved.write) || (!write && !resolved.read) {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(Some(claims))
}

fn roles_for_claims(claims: Option<&Claims>) -> Vec<String> {
    claims
        .map(|claims| claims.roles.clone())
        .unwrap_or_else(|| vec!["admin".into()])
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
    if let Err(status) = authorize_database_access(&state, &headers, &database, false) {
        return status.into_response();
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
    let write = request
        .statements
        .iter()
        .any(|statement| statement_requires_write(&statement.statement));
    let claims = match authorize_database_access(&state, &headers, &database, write) {
        Ok(claims) => claims,
        Err(status) => return status.into_response(),
    };
    let roles = roles_for_claims(claims.as_ref());
    let distributed = distributed_cypher_requested(&state, &headers);
    let request_region = headers
        .get("x-copperdb-region")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);

    let mut results = Vec::new();
    let mut errors = Vec::new();

    for statement in request.statements {
        match execute_statement(
            &state,
            &database,
            &statement.statement,
            statement.parameters.unwrap_or_default(),
            &roles,
            distributed,
            request_region.clone(),
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
    roles: &[String],
    distributed: bool,
    request_region: Option<String>,
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
    let result = if distributed {
        let placement = PlacementKey::default_for_database(database);
        let consistency = ConsistencyLevel::Quorum;
        let request_region = request_region.as_deref();
        let transport = build_local_replica_transport(
            &engine,
            &placement,
            consistency,
            request_region,
            statement_requires_write(normalized),
        )?;
        futures::executor::block_on(async {
            engine
                .execute_distributed_as(
                    normalized,
                    parameters,
                    roles,
                    &placement,
                    consistency,
                    request_region,
                    transport,
                )
                .await
                .map(|outcome| outcome.result)
                .map_err(|error| error.to_string())
        })?
    } else {
        engine
            .execute_as(normalized, parameters, roles)
            .map_err(|error| error.to_string())?
    };
    Ok(convert_engine_result(result))
}

fn convert_engine_result(result: copperdb_engine::QueryResult) -> Neo4jResult {
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

fn statement_requires_write(statement: &str) -> bool {
    let upper = statement.trim_start().to_ascii_uppercase();
    !(upper.starts_with("MATCH ")
        || upper.starts_with("RETURN ")
        || upper.starts_with("WITH ")
        || upper.starts_with("SHOW "))
}

fn distributed_cypher_requested(state: &AppState, headers: &HeaderMap) -> bool {
    state.distributed_cypher_enabled
        || headers
            .get("x-copperdb-distributed")
            .and_then(|value| value.to_str().ok())
            .map(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(false)
}

fn cypher_parameters(
    parameters: Option<serde_json::Value>,
) -> Result<HashMap<String, serde_json::Value>, String> {
    match parameters.unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new())) {
        serde_json::Value::Object(map) => Ok(map.into_iter().collect()),
        _ => Err("parameters must be a JSON object".into()),
    }
}

fn build_local_replica_transport(
    engine: &GraphEngine,
    placement: &PlacementKey,
    consistency: ConsistencyLevel,
    request_region: Option<&str>,
    write: bool,
) -> Result<Arc<InMemoryReplicaTransport>, String> {
    let replicas = if write {
        engine
            .plan_distributed_write(placement, consistency, request_region)
            .map_err(|error| error.to_string())?
            .replicas
    } else {
        engine
            .plan_distributed_read(placement, consistency, request_region)
            .map_err(|error| error.to_string())?
            .replicas
    };
    let transport = Arc::new(InMemoryReplicaTransport::new());
    for replica in replicas {
        transport.register(&replica.node_id, Arc::new(MemoryStorage::new()));
    }
    Ok(transport)
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
    let data_dir = state
        .db_manager
        .get(database)
        .map(|database| database.storage_path)
        .unwrap_or_else(|| format!("data/{}", database));
    let config = EngineConfig {
        data_dir,
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
    headers: HeaderMap,
    Json(body): Json<CypherRequest>,
) -> impl IntoResponse {
    let started = std::time::Instant::now();
    let op_type = classify_cypher_op_type(&body.query);
    if body.query.trim().is_empty() {
        let _ = state.telemetry.record_counter(
            "nornicdb_cypher_queries_total",
            &[("op_type", "parse_error"), ("database", &state.db_name)],
        );
        return (
            StatusCode::BAD_REQUEST,
            Json(CypherResponse::error("query must not be empty")),
        );
    }
    let claims = match authorize_database_access(
        &state,
        &headers,
        &state.db_name,
        statement_requires_write(&body.query),
    ) {
        Ok(claims) => claims,
        Err(status) => return (status, Json(CypherResponse::error(status.to_string()))),
    };
    let roles = roles_for_claims(claims.as_ref());
    let distributed = distributed_cypher_requested(&state, &headers);
    let request_region = headers
        .get("x-copperdb-region")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    match execute_http_cypher(&state, &body, &roles, distributed, request_region) {
        Ok(result) => {
            let _ = state.telemetry.record_counter(
                "nornicdb_cypher_queries_total",
                &[("op_type", op_type), ("database", &state.db_name)],
            );
            let _ = state.telemetry.observe_histogram(
                "nornicdb_cypher_query_duration_seconds",
                &[("op_type", op_type), ("database", &state.db_name)],
                started.elapsed().as_secs_f64(),
            );
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
        Err(error) => {
            let _ = state.telemetry.record_counter(
                "nornicdb_cypher_queries_total",
                &[("op_type", op_type), ("database", &state.db_name)],
            );
            (StatusCode::OK, Json(CypherResponse::error(error)))
        }
    }
}

fn execute_http_cypher(
    state: &AppState,
    body: &CypherRequest,
    roles: &[String],
    distributed: bool,
    request_region: Option<String>,
) -> Result<QueryResult, String> {
    let parameters = cypher_parameters(body.parameters.clone())?;
    let engine = open_engine(state, &state.db_name)?;
    if !distributed {
        return engine
            .execute_as(&body.query, parameters, roles)
            .map_err(|error| error.to_string());
    }

    let placement = PlacementKey::default_for_database(&state.db_name);
    let consistency = ConsistencyLevel::Quorum;
    let request_region = request_region.as_deref();
    let transport = build_local_replica_transport(
        &engine,
        &placement,
        consistency,
        request_region,
        statement_requires_write(&body.query),
    )?;
    let query = body.query.clone();
    let roles = roles.to_vec();
    let request_region = request_region.map(str::to_owned);
    futures::executor::block_on(async move {
        engine
            .execute_distributed_as(
                &query,
                parameters,
                &roles,
                &placement,
                consistency,
                request_region.as_deref(),
                transport,
            )
            .await
            .map(|outcome| outcome.result)
            .map_err(|error| error.to_string())
    })
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
    match mgr.place_legal_hold(body.subject_id, body.reason) {
        Ok(hold) => (StatusCode::CREATED, Json(serde_json::json!(hold))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
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

async fn retention_sweep(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mgr = state.retention.read();
    match mgr.sweep(RetentionSweepConfig::default()) {
        Ok(report) => (StatusCode::OK, Json(serde_json::json!(report))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
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

    #[tokio::test]
    async fn health_uses_buildinfo_version() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let state = Arc::new(AppState::default());
        let app = build_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let health: HealthResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(health.version, copperdb_buildinfo::version());
    }

    #[tokio::test]
    async fn root_advertises_buildinfo_server_announcement() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let state = Arc::new(AppState::default());
        let app = build_router(state);
        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let root: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(root["server"], copperdb_buildinfo::server_announcement());
    }

    #[tokio::test]
    async fn auth_token_uses_durable_authenticator_for_cookie_access() {
        use axum::body::Body;
        use axum::http::{header, Request, StatusCode};
        use tower::ServiceExt;

        let auth_path = unique_auth_path();
        let mut state = AppState::default();
        state.auth = AuthState::from_storage_path(
            auth_path,
            true,
            true,
            "admin".into(),
            "password".into(),
            "test-secret".into(),
        )
        .unwrap();
        let app = build_router(Arc::new(state));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/auth/token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({"username":"admin","password":"password"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let cookie = response
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .header(header::COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn write_query_requires_write_privilege_from_durable_roles() {
        use axum::body::Body;
        use axum::http::{header, Request, StatusCode};
        use tower::ServiceExt;

        let auth_path = unique_auth_path();
        let mut state = AppState::default();
        state.auth = AuthState::from_storage_path(
            auth_path,
            true,
            true,
            "admin".into(),
            "password".into(),
            "test-secret".into(),
        )
        .unwrap();
        state
            .auth
            .open_authenticator()
            .unwrap()
            .create_user(
                "viewer",
                "password",
                vec![copperdb_auth::ROLE_VIEWER.into()],
            )
            .unwrap();
        let token = state
            .auth
            .open_authenticator()
            .unwrap()
            .authenticate("viewer", "password")
            .unwrap()
            .0
            .access_token;
        let app = build_router(Arc::new(state));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/db/data/cypher")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({"query":"CREATE (n:Denied {v: 1})"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn http_cypher_can_opt_into_distributed_engine_routing() {
        use axum::body::Body;
        use axum::http::{header, Request, StatusCode};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementRecord};
        use tower::ServiceExt;

        let temp_dir = tempfile::tempdir().unwrap();
        let storage_path = temp_dir
            .path()
            .join("clinic")
            .to_string_lossy()
            .into_owned();
        let db_manager = Arc::new(DatabaseManager::new());
        db_manager.create("clinic", storage_path.clone()).unwrap();
        let mut state = AppState {
            db_name: "clinic".into(),
            db_manager,
            distributed_cypher_enabled: false,
            ..Default::default()
        };
        state.auth.security_enabled = false;
        let placement = PlacementKey::default_for_database("clinic");
        {
            let engine = GraphEngine::open(EngineConfig {
                data_dir: storage_path,
                default_database: "clinic".into(),
                ..Default::default()
            })
            .unwrap();
            for node_id in ["node-1", "node-2", "node-3"] {
                engine
                    .storage()
                    .register_topology_peer(
                        &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                            .with_capability(NodeCapability::Storage)
                            .with_capability(NodeCapability::Coordinator),
                    )
                    .unwrap();
            }
            engine
                .storage()
                .register_topology_placement(&PlacementRecord {
                    key: placement,
                    primary_node: "node-1".into(),
                    replica_nodes: vec!["node-2".into(), "node-3".into()],
                    search_nodes: vec![],
                    hyperscaler_profile: None,
                    min_write_replicas: 1,
                    search_fanout: 1,
                })
                .unwrap();
        }

        let app = build_router(Arc::new(state));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/db/data/cypher")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-copperdb-distributed", "true")
                    .body(Body::from(
                        serde_json::json!({"query":"CREATE (n:DistributedHttp {v: 1})"})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let decoded: CypherResponse = serde_json::from_slice(&body).unwrap();
        assert!(decoded.errors.is_empty(), "{:?}", decoded.errors);
        assert!(decoded.stats.is_some());
    }

    #[test]
    fn server_statement_execution_passes_roles_to_compliance() {
        use copperdb_compliance::{ComplianceControl, CompliancePolicy};

        let temp_dir = tempfile::tempdir().unwrap();
        let storage_path = temp_dir.path().to_string_lossy().into_owned();
        let db_manager = Arc::new(DatabaseManager::new());
        db_manager.create("clinic", storage_path.clone()).unwrap();
        let state = AppState {
            db_name: "clinic".into(),
            db_manager,
            ..Default::default()
        };

        {
            let engine = GraphEngine::open(EngineConfig {
                data_dir: storage_path,
                ..Default::default()
            })
            .unwrap();
            engine
                .compliance_manager()
                .add_policy(CompliancePolicy::new(
                    "patient-label",
                    "Patient Label",
                    ComplianceControl::RestrictLabel {
                        label: "Patient".into(),
                        allowed_roles: vec!["doctor".into()],
                    },
                ))
                .unwrap();
            engine.flush().unwrap();
        }

        let reader_roles = vec!["reader".to_string()];
        let err = match execute_statement(
            &state,
            "clinic",
            "CREATE (n:Patient {name: 'Alice'})",
            HashMap::new(),
            &reader_roles,
            false,
            None,
        ) {
            Ok(_) => panic!("reader role should be denied by compliance policy"),
            Err(err) => err,
        };
        assert!(err.contains("compliance error"));

        let doctor_roles = vec!["doctor".to_string()];
        execute_statement(
            &state,
            "clinic",
            "CREATE (n:Patient {name: 'Alice'})",
            HashMap::new(),
            &doctor_roles,
            false,
            None,
        )
        .unwrap();
    }

    fn unique_auth_path() -> String {
        std::env::temp_dir()
            .join(format!("copperdb-auth-{}", uuid::Uuid::new_v4()))
            .to_string_lossy()
            .into_owned()
    }
}
