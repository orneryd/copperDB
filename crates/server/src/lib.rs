//! HTTP/REST API server for copperdb.
//!
//! Equivalent to Go's `pkg/server` in NornicDB.
//! Provides a management REST API and serves the GraphQL endpoint.
//! Uses `axum` (Rust equivalent of Go's `net/http` + `gorilla/mux`).

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post, post_service},
    Json, Router,
};
use copperdb_auth::{AuthConfig, AuthError, Authenticator, Claims, DatabaseAccessMode};
use copperdb_buildinfo::{display_version, server_announcement, version};
use copperdb_engine::{CopperDb as GraphEngine, DatabaseConfig as EngineConfig, QueryResult};
use copperdb_envutil::{get as env_get, get_bool_loose};
use copperdb_fabric::{FabricReadRequest, FabricReadScope};
use copperdb_multidb::{DatabaseManager, DatabaseStatus, MultiDbError};
use copperdb_nornicgrpc::{
    NornicGrpcHydrationTransport, NornicGrpcRankedSearchTransport, TonicRemoteHydrationClient,
    TonicRemoteRankedSearchClient,
};
use copperdb_otel::{classify_cypher_op_type, Telemetry};
use copperdb_replication::{InMemoryReplicaTransport, MemoryStorage};
use copperdb_retention::{
    ErasureRequest, LegalHold, Manager as RetentionManager, Policy, RetentionError,
    RetentionSweepConfig,
};
use copperdb_search::{
    collect_fabric_hydration_records, collect_planned_fabric_ranked_batches,
    execute_planned_fabric_ranked_search, merge_rrf_search_batches, FabricHydrationRequest,
    HydrationTransport, RankedSearchTransport, RrfConfig, RrfSearchPolicy, SearchQuery,
};
use copperdb_security::{
    RequestTarget, RequestViolation, SecurityConfig, SecurityMiddleware, SecurityRequest,
};
use copperdb_storage::StorageEngine;
use copperdb_topology::{ConsistencyLevel, FabricDatabase, FabricGlobalId, PlacementKey};
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
        .route(
            "/admin/fabric/databases",
            get(list_fabric_databases).post(register_fabric_database),
        )
        .route(
            "/admin/fabric/databases/{tenant}/{database}/plans",
            get(plan_fabric_database),
        )
        .route(
            "/admin/fabric/databases/{tenant}/{database}/ranked-search",
            post_service(tower::service_fn({
                let state = Arc::clone(&state);
                move |request: Request<Body>| {
                    let state = Arc::clone(&state);
                    async move {
                        Ok::<_, std::convert::Infallible>(
                            execute_fabric_ranked_search_admin_service(state, request).await,
                        )
                    }
                }
            })),
        )
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
    ensure_database_access(state, &claims, database, write)?;
    Ok(Some(claims))
}

fn ensure_database_access(
    state: &AppState,
    claims: &Claims,
    database: &str,
    write: bool,
) -> Result<(), StatusCode> {
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
    Ok(())
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

async fn list_fabric_databases(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let claims = if state.auth.security_enabled {
        match authenticated_user(&state, &headers) {
            Some(claims) => Some(claims),
            None => return StatusCode::UNAUTHORIZED.into_response(),
        }
    } else {
        None
    };
    let mut databases = Vec::new();
    for database in state.db_manager.list() {
        if let Some(claims) = claims.as_ref() {
            if let Err(status) = ensure_database_access(&state, claims, &database.name, false) {
                if status == StatusCode::FORBIDDEN {
                    continue;
                }
                return status.into_response();
            }
        }
        match open_engine(&state, &database.name).and_then(|engine| {
            engine
                .list_fabric_databases()
                .map_err(|error| error.to_string())
        }) {
            Ok(mut entries) => databases.append(&mut entries),
            Err(error) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": error})),
                )
                    .into_response();
            }
        }
    }
    Json(serde_json::json!({ "databases": databases })).into_response()
}

async fn register_fabric_database(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(database): Json<FabricDatabase>,
) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &database.database, true) {
        return status.into_response();
    }
    if let Err(error) = database.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": error.to_string()})),
        )
            .into_response();
    }
    if let Err(error) = create_database(&state, &database.database) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": error})),
        )
            .into_response();
    }
    match open_engine(&state, &database.database).and_then(|engine| {
        engine
            .register_fabric_database(&database)
            .map_err(|error| error.to_string())
    }) {
        Ok(()) => (StatusCode::CREATED, Json(serde_json::json!(database))).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": error})),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct FabricPlanQuery {
    scope: Option<String>,
    value: Option<String>,
    consistency: Option<String>,
    region: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FabricRankedSearchRequest {
    query: SearchQuery,
    #[serde(default)]
    config: RrfConfig,
    #[serde(default)]
    policy: RrfSearchPolicy,
    hydration_consistency: Option<String>,
}

fn parse_consistency_level(
    value: Option<&str>,
    default: ConsistencyLevel,
) -> Result<ConsistencyLevel, String> {
    match value
        .unwrap_or(match default {
            ConsistencyLevel::One => "one",
            ConsistencyLevel::Quorum => "quorum",
            ConsistencyLevel::All => "all",
            ConsistencyLevel::LocalQuorum => "localquorum",
        })
        .to_ascii_lowercase()
        .as_str()
    {
        "one" => Ok(ConsistencyLevel::One),
        "quorum" => Ok(ConsistencyLevel::Quorum),
        "all" => Ok(ConsistencyLevel::All),
        "localquorum" | "local_quorum" | "local-quorum" => Ok(ConsistencyLevel::LocalQuorum),
        value => Err(format!("unsupported consistency level: {value}")),
    }
}

fn parse_fabric_plan_query(query: FabricPlanQuery) -> Result<FabricReadRequest, String> {
    let consistency =
        parse_consistency_level(query.consistency.as_deref(), ConsistencyLevel::Quorum)?;
    let scope_name = query.scope.as_deref().unwrap_or("all").to_ascii_lowercase();
    let value = query.value.unwrap_or_default();
    let scope = match scope_name.as_str() {
        "all" | "scatter" | "scatter-gather" => FabricReadScope::AllShards,
        "default" | "default-shard" => FabricReadScope::DefaultShard,
        "shard" => FabricReadScope::Shard(required_scope_value(&scope_name, value)?),
        "label" => FabricReadScope::Label(required_scope_value(&scope_name, value)?),
        "relationship" | "relationship-type" | "relationshiptype" => {
            FabricReadScope::RelationshipType(required_scope_value(&scope_name, value)?)
        }
        "collection" => FabricReadScope::Collection(required_scope_value(&scope_name, value)?),
        "global" | "global-id" | "globalid" => FabricReadScope::GlobalId(
            FabricGlobalId::parse(&required_scope_value(&scope_name, value)?)
                .map_err(|error| error.to_string())?,
        ),
        value => return Err(format!("unsupported fabric plan scope: {value}")),
    };
    Ok(FabricReadRequest {
        scope,
        consistency,
        request_region: query.region,
    })
}

fn required_scope_value(scope: &str, value: String) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        Err(format!("fabric plan scope {scope} requires a value"))
    } else {
        Ok(trimmed.to_owned())
    }
}

async fn plan_fabric_database(
    State(state): State<Arc<AppState>>,
    Path((tenant, database)): Path<(String, String)>,
    headers: HeaderMap,
    Query(query): Query<FabricPlanQuery>,
) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &database, false) {
        return status.into_response();
    }
    let read_request = match parse_fabric_plan_query(query) {
        Ok(request) => request,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": error})),
            )
                .into_response();
        }
    };
    let engine = match open_engine(&state, &database) {
        Ok(engine) => engine,
        Err(error) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": error})),
            )
                .into_response();
        }
    };
    let fabric = match engine.load_fabric_database(&tenant, &database) {
        Ok(Some(fabric)) => fabric,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "fabric database not found"})),
            )
                .into_response();
        }
        Err(error) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": error.to_string()})),
            )
                .into_response();
        }
    };
    let read_plan = match engine.plan_fabric_query_reads(&fabric, read_request) {
        Ok(plan) => plan,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": error.to_string()})),
            )
                .into_response();
        }
    };
    let read_plans = read_plan
        .shards
        .iter()
        .map(|shard| shard.read_plan.clone())
        .collect::<Vec<_>>();
    let search_plans = match engine.plan_fabric_searches(&fabric) {
        Ok(plans) => plans,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": error.to_string()})),
            )
                .into_response();
        }
    };
    Json(serde_json::json!({
        "database": fabric,
        "readPlan": read_plan,
        "readPlans": read_plans,
        "searchPlans": search_plans,
    }))
    .into_response()
}

fn build_fabric_ranked_search_context(
    engine: &GraphEngine,
    fabric: &FabricDatabase,
    hydration_consistency: ConsistencyLevel,
) -> Result<
    (
        Vec<copperdb_topology::DistributedSearchPlan>,
        HashMap<String, String>,
        Arc<dyn RankedSearchTransport>,
        Arc<dyn HydrationTransport>,
    ),
    String,
> {
    let mut search_endpoints = HashMap::new();
    let search_plans = engine
        .plan_fabric_searches(fabric)
        .map_err(|error| error.to_string())?;
    for plan in &search_plans {
        for peer in &plan.fanout {
            search_endpoints.insert(peer.node_id.clone(), peer.advertise_addr.clone());
        }
    }

    let mut hydration_endpoints = HashMap::new();
    let mut hydration_coordinators = HashMap::new();
    for placement in fabric.placement_keys() {
        let plan = engine
            .plan_distributed_read(&placement, hydration_consistency, None)
            .map_err(|error| error.to_string())?;
        hydration_coordinators.insert(placement.stable_id(), plan.coordinator.node_id.clone());
        hydration_endpoints.insert(
            plan.coordinator.node_id.clone(),
            plan.coordinator.advertise_addr.clone(),
        );
        for peer in plan.replicas {
            hydration_endpoints.insert(peer.node_id, peer.advertise_addr);
        }
    }

    let ranked_transport: Arc<dyn RankedSearchTransport> =
        Arc::new(NornicGrpcRankedSearchTransport::new(
            search_endpoints,
            Arc::new(TonicRemoteRankedSearchClient::new()),
        ));
    let hydration_transport: Arc<dyn HydrationTransport> =
        Arc::new(NornicGrpcHydrationTransport::new(
            hydration_endpoints,
            Arc::new(TonicRemoteHydrationClient::new()),
        ));
    Ok((
        search_plans,
        hydration_coordinators,
        ranked_transport,
        hydration_transport,
    ))
}

fn build_fabric_hydration_requests(
    merged: &copperdb_search::RrfSearchOutcome,
    hydration_coordinators: &HashMap<String, String>,
) -> Result<Vec<FabricHydrationRequest>, String> {
    let mut grouped: HashMap<String, (PlacementKey, Vec<FabricGlobalId>)> = HashMap::new();
    for hit in &merged.results {
        let stable_id = hit.global_id.placement.stable_id();
        grouped
            .entry(stable_id)
            .or_insert_with(|| (hit.global_id.placement.clone(), Vec::new()))
            .1
            .push(hit.global_id.clone());
    }

    let mut requests = Vec::new();
    for (stable_id, (placement, global_ids)) in grouped {
        let node_id = hydration_coordinators
            .get(&stable_id)
            .cloned()
            .ok_or_else(|| format!("missing hydration coordinator for placement {stable_id}"))?;
        requests.push(FabricHydrationRequest {
            node_id,
            placement,
            global_ids,
        });
    }
    Ok(requests)
}

fn parse_fabric_ranked_search_path(path: &str) -> Result<(String, String), Response> {
    let segments = path
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.len() < 6 || segments[segments.len() - 1] != "ranked-search" {
        return Err(StatusCode::NOT_FOUND.into_response());
    }
    Ok((
        segments[segments.len() - 3].to_owned(),
        segments[segments.len() - 2].to_owned(),
    ))
}

async fn execute_fabric_ranked_search_admin_service(
    state: Arc<AppState>,
    request: Request<Body>,
) -> Response {
    let (tenant, database) = match parse_fabric_ranked_search_path(request.uri().path()) {
        Ok(path) => path,
        Err(response) => return response,
    };
    if let Err(status) = authorize_database_access(&state, request.headers(), &database, false) {
        return status.into_response();
    }
    let body = match axum::body::to_bytes(request.into_body(), 1024 * 1024).await {
        Ok(body) => body,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": error.to_string()})),
            )
                .into_response();
        }
    };
    let request = match serde_json::from_slice::<FabricRankedSearchRequest>(&body) {
        Ok(request) => request,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": error.to_string()})),
            )
                .into_response();
        }
    };
    execute_fabric_ranked_search_admin_impl(state, tenant, database, request).await
}

async fn execute_fabric_ranked_search_admin_impl(
    state: Arc<AppState>,
    tenant: String,
    database: String,
    request: FabricRankedSearchRequest,
) -> Response {
    let hydration_consistency = match parse_consistency_level(
        request.hydration_consistency.as_deref(),
        ConsistencyLevel::One,
    ) {
        Ok(consistency) => consistency,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": error})),
            )
                .into_response();
        }
    };
    let (_fabric, search_plans, hydration_coordinators, ranked_transport, hydration_transport) = {
        let engine = match open_engine(&state, &database) {
            Ok(engine) => engine,
            Err(error) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": error})),
                )
                    .into_response();
            }
        };
        let fabric = match engine.load_fabric_database(&tenant, &database) {
            Ok(Some(fabric)) => fabric,
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": "fabric database not found"})),
                )
                    .into_response();
            }
            Err(error) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": error.to_string()})),
                )
                    .into_response();
            }
        };
        let (search_plans, hydration_coordinators, ranked_transport, hydration_transport) =
            match build_fabric_ranked_search_context(&engine, &fabric, hydration_consistency) {
                Ok(transports) => transports,
                Err(error) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({"error": error})),
                    )
                        .into_response();
                }
            };
        (
            fabric,
            search_plans,
            hydration_coordinators,
            ranked_transport,
            hydration_transport,
        )
    };
    let collected = match collect_planned_fabric_ranked_batches(
        search_plans.clone(),
        request.query,
        ranked_transport,
    )
    .await
    {
        Ok(collected) => collected,
        Err(error) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({"error": error.to_string()})),
            )
                .into_response();
        }
    };
    let merged = merge_rrf_search_batches(collected.batches.clone(), request.config);
    let hydration_requests = match build_fabric_hydration_requests(&merged, &hydration_coordinators)
    {
        Ok(requests) => requests,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": error})),
            )
                .into_response();
        }
    };
    let hydration =
        match collect_fabric_hydration_records(hydration_requests, hydration_transport).await {
            Ok(hydration) => hydration,
            Err(error) => {
                return (
                    StatusCode::BAD_GATEWAY,
                    Json(serde_json::json!({"error": error.to_string()})),
                )
                    .into_response();
            }
        };
    let mut execution = execute_planned_fabric_ranked_search(
        search_plans,
        collected.batches,
        hydration.records,
        request.config,
        request.policy,
    );
    execution.responded_nodes = collected.responded_nodes;
    execution.failed_nodes = collected.failed_nodes;
    for node_id in hydration.responded_nodes {
        if !execution
            .responded_nodes
            .iter()
            .any(|existing| existing == &node_id)
        {
            execution.responded_nodes.push(node_id);
        }
    }
    for node_id in hydration.failed_nodes {
        if !execution
            .failed_nodes
            .iter()
            .any(|existing| existing == &node_id)
        {
            execution.failed_nodes.push(node_id);
        }
    }
    Json(execution).into_response()
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

async fn list_policies(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &state.db_name, false) {
        return status.into_response();
    }
    let mgr = state.retention.read();
    let policies: Vec<&Policy> = mgr.list_policies();
    Json(serde_json::json!({ "policies": policies })).into_response()
}

async fn create_policy(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<Policy>,
) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &state.db_name, true) {
        return status.into_response();
    }
    let mut mgr = state.retention.write();
    match mgr.add_policy(body) {
        Ok(()) => (
            StatusCode::CREATED,
            Json(serde_json::json!({"status": "created"})),
        )
            .into_response(),
        Err(RetentionError::AlreadyExists(id)) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": format!("policy already exists: {id}")})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn load_default_policies(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &state.db_name, true) {
        return status.into_response();
    }
    let defaults = copperdb_retention::default_policies();
    let mut mgr = state.retention.write();
    let mut loaded = 0usize;
    for p in defaults {
        if mgr.add_policy(p).is_ok() {
            loaded += 1;
        }
    }
    Json(serde_json::json!({ "loaded": loaded })).into_response()
}

async fn get_policy(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &state.db_name, false) {
        return status.into_response();
    }
    let mgr = state.retention.read();
    match mgr.get_policy(&id) {
        Some(p) => (StatusCode::OK, Json(serde_json::json!(p))).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("policy not found: {id}")})),
        )
            .into_response(),
    }
}

async fn update_policy(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(mut body): Json<Policy>,
) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &state.db_name, true) {
        return status.into_response();
    }
    body.id = id;
    let mut mgr = state.retention.write();
    match mgr.update_policy(body) {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({"status": "updated"})),
        )
            .into_response(),
        Err(RetentionError::PolicyNotFound(id)) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("policy not found: {id}")})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn delete_policy(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &state.db_name, true) {
        return status.into_response();
    }
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

async fn list_holds(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &state.db_name, false) {
        return status.into_response();
    }
    let mgr = state.retention.read();
    let holds: Vec<&LegalHold> = mgr.list_legal_holds();
    Json(serde_json::json!({ "holds": holds })).into_response()
}

async fn place_hold(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<PlaceHoldRequest>,
) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &state.db_name, true) {
        return status.into_response();
    }
    let mut mgr = state.retention.write();
    match mgr.place_legal_hold(body.subject_id, body.reason) {
        Ok(hold) => (StatusCode::CREATED, Json(serde_json::json!(hold))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn release_hold(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &state.db_name, true) {
        return status.into_response();
    }
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

async fn list_erasures(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &state.db_name, false) {
        return status.into_response();
    }
    let mgr = state.retention.read();
    let erasures: Vec<&ErasureRequest> = mgr.list_erasure_requests();
    Json(serde_json::json!({ "erasures": erasures })).into_response()
}

async fn create_erasure(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<CreateErasureRequest>,
) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &state.db_name, true) {
        return status.into_response();
    }
    let mut mgr = state.retention.write();
    match mgr.create_erasure_request(body.subject_id, body.subject_email) {
        Ok(req) => (StatusCode::CREATED, Json(serde_json::json!(req))).into_response(),
        Err(RetentionError::ActiveLegalHold(sid)) => (
            StatusCode::CONFLICT,
            Json(
                serde_json::json!({"error": format!("active legal hold prevents erasure for subject: {sid}")}),
            ),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn process_erasure(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &state.db_name, true) {
        return status.into_response();
    }
    let mut mgr = state.retention.write();
    match mgr.process_erasure(&id) {
        Ok(()) => Json(serde_json::json!({"status": "completed"})).into_response(),
        Err(RetentionError::ErasureNotFound(_)) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

// ─── Sweep / status handlers ──────────────────────────────────────────────────

async fn retention_sweep(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &state.db_name, true) {
        return status.into_response();
    }
    let mgr = state.retention.read();
    match mgr.sweep(RetentionSweepConfig::default()) {
        Ok(report) => (StatusCode::OK, Json(serde_json::json!(report))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn retention_status(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &state.db_name, false) {
        return status.into_response();
    }
    let mgr = state.retention.read();
    let policy_count = mgr.list_policies().len();
    let hold_count = mgr.list_legal_holds().len();
    let erasure_count = mgr.list_erasure_requests().len();
    Json(serde_json::json!({
        "policies": policy_count,
        "active_holds": hold_count,
        "pending_erasures": erasure_count,
    }))
    .into_response()
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

    #[tokio::test]
    async fn fabric_admin_api_registers_lists_and_plans_database() {
        use axum::body::Body;
        use axum::http::{header, Request, StatusCode};
        use copperdb_topology::{
            FabricPartitionPolicy, FabricShard, MeshPeer, NodeCapability, PlacementRecord,
        };
        use tower::ServiceExt;

        let temp_dir = tempfile::tempdir().unwrap();
        let storage_path = temp_dir
            .path()
            .join("copper")
            .to_string_lossy()
            .into_owned();
        let db_manager = Arc::new(DatabaseManager::new());
        db_manager.create("copper", storage_path.clone()).unwrap();
        let mut state = AppState {
            db_name: "copper".into(),
            db_manager,
            ..Default::default()
        };
        state.auth.security_enabled = false;

        let primary = PlacementKey::new("default", "copper", "primary");
        let vector = PlacementKey::new("default", "copper", "vector-00");
        {
            let engine = GraphEngine::open(EngineConfig {
                data_dir: storage_path,
                default_database: "copper".into(),
                ..Default::default()
            })
            .unwrap();
            for node_id in [
                "primary-a",
                "primary-b",
                "primary-search",
                "vector-a",
                "vector-b",
                "vector-search",
            ] {
                engine
                    .storage()
                    .register_topology_peer(
                        &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                            .with_capability(NodeCapability::Storage)
                            .with_capability(NodeCapability::Coordinator)
                            .with_capability(NodeCapability::Search),
                    )
                    .unwrap();
            }
            engine
                .storage()
                .register_topology_placement(&PlacementRecord {
                    key: primary.clone(),
                    primary_node: "primary-a".into(),
                    replica_nodes: vec!["primary-b".into()],
                    search_nodes: vec!["primary-search".into()],
                    hyperscaler_profile: None,
                    min_write_replicas: 1,
                    search_fanout: 1,
                })
                .unwrap();
            engine
                .storage()
                .register_topology_placement(&PlacementRecord {
                    key: vector.clone(),
                    primary_node: "vector-a".into(),
                    replica_nodes: vec!["vector-b".into()],
                    search_nodes: vec!["vector-search".into()],
                    hyperscaler_profile: None,
                    min_write_replicas: 1,
                    search_fanout: 1,
                })
                .unwrap();
        }

        let fabric = FabricDatabase {
            tenant: "default".into(),
            database: "copper".into(),
            default_shard: "primary".into(),
            partition_policy: FabricPartitionPolicy::LabelAware,
            shards: vec![
                FabricShard {
                    placement: primary,
                    kind: copperdb_topology::FabricShardKind::Graph,
                    labels: vec!["Person".into()],
                    relationship_types: vec![],
                    collections: vec![],
                },
                FabricShard {
                    placement: vector,
                    kind: copperdb_topology::FabricShardKind::Vector,
                    labels: vec!["Memory".into()],
                    relationship_types: vec![],
                    collections: vec!["memories".into()],
                },
            ],
        };
        let app = build_router(Arc::new(state));
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/fabric/databases")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&fabric).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/fabric/databases")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let decoded: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(decoded["databases"].as_array().unwrap().len(), 1);
        assert_eq!(decoded["databases"][0]["database"], "copper");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/fabric/databases/default/copper/plans")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let decoded: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(decoded["readPlans"].as_array().unwrap().len(), 2);
        assert_eq!(decoded["searchPlans"].as_array().unwrap().len(), 2);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/fabric/databases/default/copper/plans?scope=label&value=Person&consistency=one")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let decoded: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(decoded["readPlans"].as_array().unwrap().len(), 1);
        assert_eq!(
            decoded["readPlan"]["shards"][0]["shard"]["placement"]["shard"],
            "primary"
        );
    }

    #[tokio::test]
    async fn fabric_admin_api_executes_ranked_search_over_grpc_transports() {
        use axum::body::Body;
        use axum::http::{header, Request, StatusCode};
        use copperdb_nornicgrpc::{
            GrpcError, NornicReplicaService, RemoteHydrationClient, RemoteHydrationRequest,
            RemoteRankedSearchClient, RemoteRankedSearchRequest, RemoteReplicaApplyRequest,
            RemoteReplicaClient, RemoteReplicaReadRequest,
        };
        use copperdb_search::{
            FabricRankedSearchExecution, RrfHydrationRecord, RrfSearchBatch, RrfSearchHit,
        };
        use copperdb_topology::{
            FabricGlobalId, FabricPartitionPolicy, FabricShard, FabricShardKind, MeshPeer,
            NodeCapability, PlacementRecord,
        };
        use std::sync::Arc;
        use tonic::transport::Server;
        use tower::ServiceExt;

        struct NoopReplicaClient;

        #[async_trait::async_trait]
        impl RemoteReplicaClient for NoopReplicaClient {
            async fn apply_replica(
                &self,
                _request: RemoteReplicaApplyRequest,
            ) -> Result<(), GrpcError> {
                Ok(())
            }

            async fn read_replica(
                &self,
                _request: RemoteReplicaReadRequest,
            ) -> Result<Option<Vec<u8>>, GrpcError> {
                Ok(None)
            }
        }

        struct FixedRankedSearchClient;

        #[async_trait::async_trait]
        impl RemoteRankedSearchClient for FixedRankedSearchClient {
            async fn search_ranked(
                &self,
                request: RemoteRankedSearchRequest,
            ) -> Result<RrfSearchBatch, GrpcError> {
                let primary = PlacementKey::new("default", "copper", "primary");
                let person = PlacementKey::new("default", "copper", "person-00");
                match request.target_node.as_str() {
                    "search-a" => Ok(RrfSearchBatch {
                        shard: primary.clone(),
                        source: "lexical".into(),
                        hits: vec![RrfSearchHit {
                            global_id: FabricGlobalId::new(primary.clone(), "node", "a"),
                            rank: 1,
                            score: 0.8,
                            source: "lexical".into(),
                            shard: primary,
                            label: "Person".into(),
                            snippet: None,
                        }],
                    }),
                    "search-b" => Ok(RrfSearchBatch {
                        shard: person.clone(),
                        source: "vector".into(),
                        hits: vec![RrfSearchHit {
                            global_id: FabricGlobalId::new(
                                PlacementKey::new("default", "copper", "primary"),
                                "node",
                                "a",
                            ),
                            rank: 1,
                            score: 0.9,
                            source: "vector".into(),
                            shard: person,
                            label: "Person".into(),
                            snippet: Some("fresh".into()),
                        }],
                    }),
                    other => Err(GrpcError::Transport(format!("no ranked batch for {other}"))),
                }
            }
        }

        struct FixedHydrationClient;

        #[async_trait::async_trait]
        impl RemoteHydrationClient for FixedHydrationClient {
            async fn hydrate_entities(
                &self,
                request: RemoteHydrationRequest,
            ) -> Result<Vec<RrfHydrationRecord>, GrpcError> {
                match request.target_node.as_str() {
                    "search-a" => Ok(vec![RrfHydrationRecord {
                        global_id: FabricGlobalId::new(
                            PlacementKey::new("default", "copper", "primary"),
                            "node",
                            "a",
                        ),
                        labels: vec!["Person".into()],
                        entity: serde_json::json!({"id": "a", "name": "Alice", "secret": "internal"}),
                    }]),
                    _ => Ok(Vec::new()),
                }
            }
        }

        let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let grpc_addr = reserved.local_addr().unwrap();
        drop(reserved);
        let service = NornicReplicaService::new(Arc::new(NoopReplicaClient))
            .with_ranked_search_handler(Arc::new(FixedRankedSearchClient))
            .with_hydration_handler(Arc::new(FixedHydrationClient));
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(service.into_server())
                .serve(grpc_addr)
                .await
                .unwrap();
        });
        tokio::task::yield_now().await;

        let temp_dir = tempfile::tempdir().unwrap();
        let storage_path = temp_dir
            .path()
            .join("copper")
            .to_string_lossy()
            .into_owned();
        let db_manager = Arc::new(DatabaseManager::new());
        db_manager.create("copper", storage_path.clone()).unwrap();
        let mut state = AppState {
            db_name: "copper".into(),
            db_manager,
            ..Default::default()
        };
        state.auth.security_enabled = false;

        {
            let engine = GraphEngine::open(EngineConfig {
                data_dir: storage_path,
                default_database: "copper".into(),
                ..Default::default()
            })
            .unwrap();
            for node_id in ["search-a", "search-b", "search-c"] {
                engine
                    .storage()
                    .register_topology_peer(
                        &MeshPeer::new(node_id, grpc_addr.to_string())
                            .with_capability(NodeCapability::Search)
                            .with_capability(NodeCapability::Storage)
                            .with_capability(NodeCapability::Coordinator),
                    )
                    .unwrap();
            }
            for (shard, nodes) in [
                ("primary", vec!["search-a", "search-c"]),
                ("person-00", vec!["search-b"]),
            ] {
                engine
                    .storage()
                    .register_topology_placement(&PlacementRecord {
                        key: PlacementKey::new("default", "copper", shard),
                        primary_node: nodes[0].into(),
                        replica_nodes: vec![],
                        search_nodes: nodes.into_iter().map(str::to_string).collect(),
                        hyperscaler_profile: None,
                        min_write_replicas: 0,
                        search_fanout: 2,
                    })
                    .unwrap();
            }
        }

        let fabric = FabricDatabase {
            tenant: "default".into(),
            database: "copper".into(),
            default_shard: "primary".into(),
            partition_policy: FabricPartitionPolicy::HashByKey { buckets: 2 },
            shards: vec![
                FabricShard::mixed(PlacementKey::new("default", "copper", "primary")),
                FabricShard {
                    placement: PlacementKey::new("default", "copper", "person-00"),
                    kind: FabricShardKind::Graph,
                    labels: vec!["Person".into()],
                    relationship_types: vec![],
                    collections: vec![],
                },
            ],
        };

        let app = build_router(Arc::new(state));
        let register = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/fabric/databases")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&fabric).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(register.status(), StatusCode::CREATED);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/fabric/databases/default/copper/ranked-search")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "query": SearchQuery::FullText {
                                query: "alice".into(),
                                fields: vec!["body".into()],
                                limit: 10,
                            },
                            "config": RrfConfig::new(60.0, 10),
                            "policy": RrfSearchPolicy {
                                allowed_labels: vec!["Person".into()],
                                denied_labels: Vec::new(),
                                denied_sources: Vec::new(),
                                require_hydration: true,
                                redact_fields: vec!["secret".into()],
                            },
                            "hydration_consistency": "one"
                        })
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
        let decoded: FabricRankedSearchExecution = serde_json::from_slice(&body).unwrap();
        assert_eq!(decoded.responded_nodes, vec!["search-a", "search-b"]);
        assert_eq!(decoded.failed_nodes, vec!["search-c"]);
        assert_eq!(decoded.hydrated.output_hits, 1);
        assert_eq!(
            decoded.hydrated.results[0].entity.as_ref().unwrap()["name"],
            "Alice"
        );
        assert!(decoded.hydrated.results[0]
            .entity
            .as_ref()
            .unwrap()
            .get("secret")
            .is_none());

        server.abort();
    }

    #[tokio::test]
    async fn fabric_admin_api_requires_auth_when_security_enabled() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
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

        let list = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/fabric/databases")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list.status(), StatusCode::UNAUTHORIZED);

        let plan = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/fabric/databases/default/copper/plans")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(plan.status(), StatusCode::UNAUTHORIZED);

        let ranked = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/fabric/databases/default/copper/ranked-search")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "query": SearchQuery::FullText {
                                query: "alice".into(),
                                fields: vec!["body".into()],
                                limit: 10,
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ranked.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn fabric_admin_api_filters_by_database_access_and_blocks_writes_for_viewers() {
        use axum::body::Body;
        use axum::http::{header, Request, StatusCode};
        use copperdb_topology::{
            FabricPartitionPolicy, FabricShard, MeshPeer, NodeCapability, PlacementRecord,
        };
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

        let temp_dir = tempfile::tempdir().unwrap();
        let copper_path = temp_dir
            .path()
            .join("copper")
            .to_string_lossy()
            .into_owned();
        let secret_path = temp_dir
            .path()
            .join("secret")
            .to_string_lossy()
            .into_owned();
        let db_manager = Arc::new(DatabaseManager::new());
        db_manager.create("copper", copper_path.clone()).unwrap();
        db_manager.create("secret", secret_path.clone()).unwrap();
        state.db_name = "copper".into();
        state.db_manager = db_manager;

        {
            let auth = state.auth.open_authenticator().unwrap();
            auth.allowlist
                .save_role_databases(copperdb_auth::ROLE_VIEWER, vec!["copper".into()])
                .unwrap();
            auth.privileges
                .save_privilege(copperdb_auth::ROLE_VIEWER, "copper", true, false)
                .unwrap();
            auth.privileges
                .save_privilege(copperdb_auth::ROLE_VIEWER, "secret", false, false)
                .unwrap();
            auth.create_user(
                "viewer",
                "password",
                vec![copperdb_auth::ROLE_VIEWER.into()],
            )
            .unwrap();
        }

        for (database, path) in [("copper", copper_path), ("secret", secret_path)] {
            let engine = GraphEngine::open(EngineConfig {
                data_dir: path,
                default_database: database.into(),
                ..Default::default()
            })
            .unwrap();
            engine
                .storage()
                .register_topology_peer(
                    &MeshPeer::new(
                        format!("{database}-search"),
                        format!("{database}-search.mesh.local:9000"),
                    )
                    .with_capability(NodeCapability::Search)
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
            engine
                .storage()
                .register_topology_placement(&PlacementRecord {
                    key: PlacementKey::new("default", database, "primary"),
                    primary_node: format!("{database}-search"),
                    replica_nodes: vec![],
                    search_nodes: vec![format!("{database}-search")],
                    hyperscaler_profile: None,
                    min_write_replicas: 0,
                    search_fanout: 1,
                })
                .unwrap();
            engine
                .register_fabric_database(&FabricDatabase {
                    tenant: "default".into(),
                    database: database.into(),
                    default_shard: "primary".into(),
                    partition_policy: FabricPartitionPolicy::Manual,
                    shards: vec![FabricShard::mixed(PlacementKey::new(
                        "default", database, "primary",
                    ))],
                })
                .unwrap();
        }

        let viewer_token = state
            .auth
            .open_authenticator()
            .unwrap()
            .authenticate("viewer", "password")
            .unwrap()
            .0
            .access_token;
        let app = build_router(Arc::new(state));

        let list = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/fabric/databases")
                    .header(header::AUTHORIZATION, format!("Bearer {viewer_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list.status(), StatusCode::OK);
        let body = axum::body::to_bytes(list.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let decoded: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(decoded["databases"].as_array().unwrap().len(), 1);
        assert_eq!(decoded["databases"][0]["database"], "copper");

        let plan_ok = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/fabric/databases/default/copper/plans")
                    .header(header::AUTHORIZATION, format!("Bearer {viewer_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(plan_ok.status(), StatusCode::OK);

        let plan_denied = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/fabric/databases/default/secret/plans")
                    .header(header::AUTHORIZATION, format!("Bearer {viewer_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(plan_denied.status(), StatusCode::FORBIDDEN);

        let register_denied = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/fabric/databases")
                    .header(header::AUTHORIZATION, format!("Bearer {viewer_token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_string(&FabricDatabase {
                            tenant: "default".into(),
                            database: "secret".into(),
                            default_shard: "primary".into(),
                            partition_policy: FabricPartitionPolicy::Manual,
                            shards: vec![FabricShard::mixed(PlacementKey::new(
                                "default", "secret", "primary",
                            ))],
                        })
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(register_denied.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn fabric_ranked_search_respects_per_database_viewer_access() {
        use axum::body::Body;
        use axum::http::{header, Request, StatusCode};
        use copperdb_nornicgrpc::{
            GrpcError, NornicReplicaService, RemoteHydrationClient, RemoteHydrationRequest,
            RemoteRankedSearchClient, RemoteRankedSearchRequest, RemoteReplicaApplyRequest,
            RemoteReplicaClient, RemoteReplicaReadRequest,
        };
        use copperdb_search::{
            FabricRankedSearchExecution, RrfHydrationRecord, RrfSearchBatch, RrfSearchHit,
        };
        use copperdb_topology::{
            FabricGlobalId, FabricPartitionPolicy, FabricShard, FabricShardKind, MeshPeer,
            NodeCapability, PlacementRecord,
        };
        use tonic::transport::Server;
        use tower::ServiceExt;

        struct NoopReplicaClient;

        #[async_trait::async_trait]
        impl RemoteReplicaClient for NoopReplicaClient {
            async fn apply_replica(
                &self,
                _request: RemoteReplicaApplyRequest,
            ) -> Result<(), GrpcError> {
                Ok(())
            }

            async fn read_replica(
                &self,
                _request: RemoteReplicaReadRequest,
            ) -> Result<Option<Vec<u8>>, GrpcError> {
                Ok(None)
            }
        }

        struct FixedRankedSearchClient;

        #[async_trait::async_trait]
        impl RemoteRankedSearchClient for FixedRankedSearchClient {
            async fn search_ranked(
                &self,
                request: RemoteRankedSearchRequest,
            ) -> Result<RrfSearchBatch, GrpcError> {
                let primary = PlacementKey::new("default", "copper", "primary");
                let person = PlacementKey::new("default", "copper", "person-00");
                match request.target_node.as_str() {
                    "search-a" => Ok(RrfSearchBatch {
                        shard: primary.clone(),
                        source: "lexical".into(),
                        hits: vec![RrfSearchHit {
                            global_id: FabricGlobalId::new(primary.clone(), "node", "a"),
                            rank: 1,
                            score: 0.8,
                            source: "lexical".into(),
                            shard: primary,
                            label: "Person".into(),
                            snippet: None,
                        }],
                    }),
                    "search-b" => Ok(RrfSearchBatch {
                        shard: person.clone(),
                        source: "vector".into(),
                        hits: vec![RrfSearchHit {
                            global_id: FabricGlobalId::new(
                                PlacementKey::new("default", "copper", "primary"),
                                "node",
                                "a",
                            ),
                            rank: 1,
                            score: 0.9,
                            source: "vector".into(),
                            shard: person,
                            label: "Person".into(),
                            snippet: Some("fresh".into()),
                        }],
                    }),
                    other => Err(GrpcError::Transport(format!("no ranked batch for {other}"))),
                }
            }
        }

        struct FixedHydrationClient;

        #[async_trait::async_trait]
        impl RemoteHydrationClient for FixedHydrationClient {
            async fn hydrate_entities(
                &self,
                request: RemoteHydrationRequest,
            ) -> Result<Vec<RrfHydrationRecord>, GrpcError> {
                match request.target_node.as_str() {
                    "search-a" => Ok(vec![RrfHydrationRecord {
                        global_id: FabricGlobalId::new(
                            PlacementKey::new("default", "copper", "primary"),
                            "node",
                            "a",
                        ),
                        labels: vec!["Person".into()],
                        entity: serde_json::json!({"id": "a", "name": "Alice", "secret": "internal"}),
                    }]),
                    _ => Ok(Vec::new()),
                }
            }
        }

        let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let grpc_addr = reserved.local_addr().unwrap();
        drop(reserved);
        let service = NornicReplicaService::new(Arc::new(NoopReplicaClient))
            .with_ranked_search_handler(Arc::new(FixedRankedSearchClient))
            .with_hydration_handler(Arc::new(FixedHydrationClient));
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(service.into_server())
                .serve(grpc_addr)
                .await
                .unwrap();
        });
        tokio::task::yield_now().await;

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

        let temp_dir = tempfile::tempdir().unwrap();
        let copper_path = temp_dir
            .path()
            .join("copper")
            .to_string_lossy()
            .into_owned();
        let secret_path = temp_dir
            .path()
            .join("secret")
            .to_string_lossy()
            .into_owned();
        let db_manager = Arc::new(DatabaseManager::new());
        db_manager.create("copper", copper_path.clone()).unwrap();
        db_manager.create("secret", secret_path.clone()).unwrap();
        state.db_name = "copper".into();
        state.db_manager = db_manager;

        {
            let auth = state.auth.open_authenticator().unwrap();
            auth.allowlist
                .save_role_databases(copperdb_auth::ROLE_VIEWER, vec!["copper".into()])
                .unwrap();
            auth.privileges
                .save_privilege(copperdb_auth::ROLE_VIEWER, "copper", true, false)
                .unwrap();
            auth.privileges
                .save_privilege(copperdb_auth::ROLE_VIEWER, "secret", false, false)
                .unwrap();
            auth.create_user(
                "viewer",
                "password",
                vec![copperdb_auth::ROLE_VIEWER.into()],
            )
            .unwrap();
        }

        for (database, path, shards) in [
            (
                "copper",
                copper_path,
                vec![
                    ("primary", vec!["search-a", "search-c"]),
                    ("person-00", vec!["search-b"]),
                ],
            ),
            ("secret", secret_path, vec![("primary", vec!["search-a"])]),
        ] {
            let engine = GraphEngine::open(EngineConfig {
                data_dir: path,
                default_database: database.into(),
                ..Default::default()
            })
            .unwrap();
            for node_id in ["search-a", "search-b", "search-c"] {
                engine
                    .storage()
                    .register_topology_peer(
                        &MeshPeer::new(node_id, grpc_addr.to_string())
                            .with_capability(NodeCapability::Search)
                            .with_capability(NodeCapability::Storage)
                            .with_capability(NodeCapability::Coordinator),
                    )
                    .unwrap();
            }
            for (shard, nodes) in shards {
                engine
                    .storage()
                    .register_topology_placement(&PlacementRecord {
                        key: PlacementKey::new("default", database, shard),
                        primary_node: nodes[0].into(),
                        replica_nodes: vec![],
                        search_nodes: nodes.into_iter().map(str::to_string).collect(),
                        hyperscaler_profile: None,
                        min_write_replicas: 0,
                        search_fanout: 2,
                    })
                    .unwrap();
            }
            let shards = if database == "copper" {
                vec![
                    FabricShard::mixed(PlacementKey::new("default", database, "primary")),
                    FabricShard {
                        placement: PlacementKey::new("default", database, "person-00"),
                        kind: FabricShardKind::Graph,
                        labels: vec!["Person".into()],
                        relationship_types: vec![],
                        collections: vec![],
                    },
                ]
            } else {
                vec![FabricShard::mixed(PlacementKey::new(
                    "default", database, "primary",
                ))]
            };
            engine
                .register_fabric_database(&FabricDatabase {
                    tenant: "default".into(),
                    database: database.into(),
                    default_shard: "primary".into(),
                    partition_policy: FabricPartitionPolicy::HashByKey { buckets: 2 },
                    shards,
                })
                .unwrap();
        }

        let viewer_token = state
            .auth
            .open_authenticator()
            .unwrap()
            .authenticate("viewer", "password")
            .unwrap()
            .0
            .access_token;
        let app = build_router(Arc::new(state));

        let allowed = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/fabric/databases/default/copper/ranked-search")
                    .header(header::AUTHORIZATION, format!("Bearer {viewer_token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "query": SearchQuery::FullText {
                                query: "alice".into(),
                                fields: vec!["body".into()],
                                limit: 10,
                            },
                            "config": RrfConfig::new(60.0, 10),
                            "policy": RrfSearchPolicy {
                                allowed_labels: vec!["Person".into()],
                                denied_labels: Vec::new(),
                                denied_sources: Vec::new(),
                                require_hydration: true,
                                redact_fields: vec!["secret".into()],
                            },
                            "hydration_consistency": "one"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::OK);
        let body = axum::body::to_bytes(allowed.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let decoded: FabricRankedSearchExecution = serde_json::from_slice(&body).unwrap();
        assert_eq!(decoded.hydrated.output_hits, 1);
        assert_eq!(
            decoded.hydrated.results[0].entity.as_ref().unwrap()["name"],
            "Alice"
        );

        let denied = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/fabric/databases/default/secret/ranked-search")
                    .header(header::AUTHORIZATION, format!("Bearer {viewer_token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "query": SearchQuery::FullText {
                                query: "alice".into(),
                                fields: vec!["body".into()],
                                limit: 10,
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);

        server.abort();
    }

    #[tokio::test]
    async fn retention_admin_routes_require_auth_when_security_enabled() {
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

        let list = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/retention/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list.status(), StatusCode::UNAUTHORIZED);

        let create = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/retention/policies")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "id": "policy-1",
                            "label": "Person",
                            "max_age_seconds": 86400,
                            "cascade_delete": false,
                            "description": null,
                            "data_category": null,
                            "enabled": true
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn retention_admin_routes_allow_viewer_reads_and_deny_writes() {
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

        let status = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/retention/status")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);

        let list = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/retention/policies")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list.status(), StatusCode::OK);

        let create = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/retention/policies")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "id": "policy-1",
                            "label": "Person",
                            "max_age_seconds": 86400,
                            "cascade_delete": false,
                            "description": null,
                            "data_category": null,
                            "enabled": true
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::FORBIDDEN);
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
