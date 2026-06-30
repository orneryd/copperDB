//! HTTP/REST API server for copperdb.
//!
//! Equivalent to Go's `pkg/server` in NornicDB.
//! Provides a management REST API and serves the GraphQL endpoint.
//! Uses `axum` (Rust equivalent of Go's `net/http` + `gorilla/mux`).

use async_graphql_axum::GraphQLRequest;
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderMap, Method, Request, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post, post_service},
    Json, Router,
};
use copperdb_auth::{
    AuthConfig, AuthError, Authenticator, Claims, DatabaseAccessMode, TokenManager,
};
use copperdb_bolt::server::{BoltQueryResult, QueryExecutor};
use copperdb_buildinfo::{display_version, server_announcement, version};
use copperdb_config::Config as RuntimeConfig;
use copperdb_engine::{CopperDb as GraphEngine, DatabaseConfig as EngineConfig};
use copperdb_envutil::{get as env_get, get_bool_loose};
use copperdb_fabric::{FabricReadRequest, FabricReadScope};
use copperdb_graphql::GraphQlSchema;
use copperdb_multidb::{DatabaseManager, DatabaseStatus, MultiDbError};
use copperdb_nornicgrpc::{
    GrpcAuthValidator, GrpcError, NornicGrpcHydrationTransport, NornicGrpcRankedSearchTransport,
    NornicGrpcReplicaTransport, NornicReplicaService, RemoteHydrationClient,
    RemoteHydrationRequest, RemoteRankedSearchClient, RemoteRankedSearchRequest,
    RemoteReplicaClient, TonicRemoteHydrationClient, TonicRemoteRankedSearchClient,
    TonicRemoteReplicaClient,
};
use copperdb_otel::Telemetry;
use copperdb_replication::{Command, ReplicaTransport, ReplicationStorage, StorageEngineAdapter};
use copperdb_retention::{
    ErasureRequest, LegalHold, Manager as RetentionManager, Policy, RetentionError,
    RetentionSweepConfig,
};
use copperdb_search::{
    collect_fabric_hydration_records_with_context,
    collect_planned_fabric_ranked_batches_with_context, execute_planned_fabric_ranked_search,
    merge_rrf_search_batches, FabricHydrationRequest, HydrationTransport, RankedSearchTransport,
    RrfConfig, RrfSearchPolicy, SearchQuery,
};
use copperdb_security::{
    RequestTarget, RequestViolation, SecurityConfig, SecurityMiddleware, SecurityRequest,
};
use copperdb_storage::StorageEngine;

mod ui_assets;
use copperdb_topology::{
    ConsistencyLevel, FabricDatabase, FabricGlobalId, LogicalTransactionId, PlacementKey,
};
use copperdb_txsession::{BookmarkMode, SessionConfig, TransactionMode};
use copperdb_util::RequestContext;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
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
        let security_enabled = get_bool_loose("COPPERDB_SECURITY_ENABLED", false);
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

    #[allow(clippy::arc_with_non_send_sync)]
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
    pub runtime_config: Arc<RuntimeConfig>,
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
    /// GraphQL schema wired to storage.
    pub graphql_schema: GraphQlSchema,
    /// Cached graph engines keyed by database name.  Opening a storage engine
    /// from disk triggers LSM-tree recovery (WAL replay, manifest rebuild)
    /// which can take 400–600 ms.  Caching avoids this cost per query.
    pub engine_cache: Arc<RwLock<HashMap<String, Arc<GraphEngine>>>>,
}

#[derive(Clone)]
struct LocalEngineReplicaHandler {
    state: Arc<AppState>,
}

#[derive(Clone)]
struct LocalEngineRankedSearchHandler {
    state: Arc<AppState>,
}

#[derive(Clone)]
struct LocalEngineHydrationHandler {
    state: Arc<AppState>,
}

#[derive(Clone)]
struct UnifiedClusterAuthValidator {
    state: Arc<AppState>,
}

impl LocalEngineReplicaHandler {
    fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    fn database_for_command(&self, command: &Command) -> String {
        match command {
            Command::CypherMutation { database, .. } if !database.is_empty() => database.clone(),
            _ => self.state.db_name.clone(),
        }
    }

    fn open_replication_storage(&self, database: &str) -> Result<StorageEngineAdapter, GrpcError> {
        let data_dir = self
            .state
            .db_manager
            .get(database)
            .map(|database| database.storage_path)
            .unwrap_or_else(|| format!("data/{database}"));
        let engine = StorageEngine::open(&data_dir)
            .map_err(|error| GrpcError::Transport(error.to_string()))?;
        Ok(StorageEngineAdapter::new(engine))
    }
}

#[async_trait::async_trait]
impl RemoteReplicaClient for LocalEngineReplicaHandler {
    async fn apply_replica(
        &self,
        request: copperdb_nornicgrpc::RemoteReplicaApplyRequest,
    ) -> Result<(), GrpcError> {
        let database = self.database_for_command(&request.command);
        self.open_replication_storage(&database)?
            .apply_command(&request.command)
            .map_err(|error| GrpcError::Transport(error.to_string()))
    }

    async fn read_replica(
        &self,
        request: copperdb_nornicgrpc::RemoteReplicaReadRequest,
    ) -> Result<Option<Vec<u8>>, GrpcError> {
        self.open_replication_storage(&self.state.db_name)?
            .read_key(&request.key)
            .map_err(|error| GrpcError::Transport(error.to_string()))
    }

    async fn graph_node(
        &self,
        request: copperdb_nornicgrpc::RemoteGraphNodeRequest,
    ) -> Result<Option<Vec<u8>>, GrpcError> {
        forwarded_caller_claims(
            &self.state,
            request.caller_auth_token.as_deref(),
            &request.database,
            false,
        )?;
        observe_remote_read_fence(&self.state, &request.database, request.read_fence)
            .map_err(GrpcError::Transport)?;
        self.open_replication_storage(&request.database)?
            .graph_node(&request.node_id)
            .map_err(|error| GrpcError::Transport(error.to_string()))
    }

    async fn graph_edges_from_node(
        &self,
        request: copperdb_nornicgrpc::RemoteGraphEdgesRequest,
    ) -> Result<Vec<copperdb_storage::EdgeRecord>, GrpcError> {
        forwarded_caller_claims(
            &self.state,
            request.caller_auth_token.as_deref(),
            &request.database,
            false,
        )?;
        observe_remote_read_fence(&self.state, &request.database, request.read_fence)
            .map_err(GrpcError::Transport)?;
        self.open_replication_storage(&request.database)?
            .graph_edges_from_node(&request.node_id, request.rel_type.as_deref())
            .map_err(|error| GrpcError::Transport(error.to_string()))
    }

    async fn graph_edges_to_node(
        &self,
        request: copperdb_nornicgrpc::RemoteGraphEdgesRequest,
    ) -> Result<Vec<copperdb_storage::EdgeRecord>, GrpcError> {
        forwarded_caller_claims(
            &self.state,
            request.caller_auth_token.as_deref(),
            &request.database,
            false,
        )?;
        observe_remote_read_fence(&self.state, &request.database, request.read_fence)
            .map_err(GrpcError::Transport)?;
        self.open_replication_storage(&request.database)?
            .graph_edges_to_node(&request.node_id, request.rel_type.as_deref())
            .map_err(|error| GrpcError::Transport(error.to_string()))
    }

    async fn graph_nodes_by_label(
        &self,
        request: copperdb_nornicgrpc::RemoteGraphNodesByLabelRequest,
    ) -> Result<Vec<Vec<u8>>, GrpcError> {
        forwarded_caller_claims(
            &self.state,
            request.caller_auth_token.as_deref(),
            &request.database,
            false,
        )?;
        observe_remote_read_fence(&self.state, &request.database, request.read_fence)
            .map_err(GrpcError::Transport)?;
        self.open_replication_storage(&request.database)?
            .graph_nodes_by_label(&request.label)
            .map_err(|error| GrpcError::Transport(error.to_string()))
    }

    async fn graph_nodes_by_property(
        &self,
        request: copperdb_nornicgrpc::RemoteGraphNodesByPropertyRequest,
    ) -> Result<Vec<Vec<u8>>, GrpcError> {
        forwarded_caller_claims(
            &self.state,
            request.caller_auth_token.as_deref(),
            &request.database,
            false,
        )?;
        observe_remote_read_fence(&self.state, &request.database, request.read_fence)
            .map_err(GrpcError::Transport)?;
        self.open_replication_storage(&request.database)?
            .graph_nodes_by_property(&request.label, &request.property, &request.value)
            .map_err(|error| GrpcError::Transport(error.to_string()))
    }

    async fn graph_access_metadata(
        &self,
        request: copperdb_nornicgrpc::RemoteGraphAccessMetadataRequest,
    ) -> Result<Option<copperdb_storage::KnowledgePolicyAccessMetadata>, GrpcError> {
        forwarded_caller_claims(
            &self.state,
            request.caller_auth_token.as_deref(),
            &request.database,
            false,
        )?;
        observe_remote_read_fence(&self.state, &request.database, request.read_fence)
            .map_err(GrpcError::Transport)?;
        self.open_replication_storage(&request.database)?
            .graph_access_metadata(&request.entity_id)
            .map_err(|error| GrpcError::Transport(error.to_string()))
    }
}

impl LocalEngineRankedSearchHandler {
    fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

fn forwarded_caller_claims(
    state: &AppState,
    caller_auth_token: Option<&str>,
    database: &str,
    write: bool,
) -> Result<(), GrpcError> {
    if !state.auth.security_enabled {
        return Ok(());
    }
    let token = caller_auth_token.ok_or_else(|| {
        GrpcError::Unauthenticated("missing forwarded caller authorization token".into())
    })?;
    let claims = state
        .auth
        .open_authenticator()
        .map_err(|error| GrpcError::Transport(error.to_string()))?
        .validate_token(token)
        .map_err(|error| GrpcError::Unauthenticated(error.to_string()))?;
    ensure_database_access(state, &claims, database, write).map_err(|status| match status {
        StatusCode::UNAUTHORIZED => GrpcError::Unauthenticated("unauthorized caller".into()),
        StatusCode::FORBIDDEN => {
            GrpcError::PermissionDenied(format!("caller is not authorized for database {database}"))
        }
        other => GrpcError::Transport(format!("unexpected authorization status {other}")),
    })?;
    Ok(())
}

impl GrpcAuthValidator for UnifiedClusterAuthValidator {
    fn validate(&self, token: &str) -> Result<(), GrpcError> {
        if !self.state.auth.security_enabled {
            return Ok(());
        }
        let claims = self
            .state
            .auth
            .open_authenticator()
            .map_err(|error| GrpcError::Transport(error.to_string()))?
            .validate_token(token)
            .map_err(|error| GrpcError::Unauthenticated(error.to_string()))?;
        if claims
            .roles
            .iter()
            .any(|role| role.eq_ignore_ascii_case("admin"))
        {
            return Ok(());
        }
        Err(GrpcError::PermissionDenied(
            "cluster gRPC authorization requires admin role".into(),
        ))
    }
}

#[async_trait::async_trait]
impl RemoteRankedSearchClient for LocalEngineRankedSearchHandler {
    async fn search_ranked(
        &self,
        request: RemoteRankedSearchRequest,
    ) -> Result<copperdb_search::RrfSearchBatch, GrpcError> {
        forwarded_caller_claims(
            &self.state,
            request.caller_auth_token.as_deref(),
            &request.placement.database,
            false,
        )?;
        observe_remote_read_fence(&self.state, &request.placement.database, request.read_fence)
            .map_err(GrpcError::Transport)?;
        let (request_context, _request_guard) = request
            .request_context
            .map(RequestContext::from_metadata)
            .unwrap_or_else(|| RequestContext::root(None));
        let state = Arc::clone(&self.state);
        let database = request.placement.database.clone();
        let placement = request.placement.clone();
        let query = request.query.clone();
        tokio::task::spawn_blocking(move || {
            let engine = open_engine(&state, &database).map_err(GrpcError::Transport)?;
            engine
                .search_fabric_ranked_batch_locally_with_context(
                    &request_context,
                    &placement,
                    &query,
                )
                .map_err(|error| GrpcError::Transport(error.to_string()))
        })
        .await
        .map_err(|error| GrpcError::Transport(error.to_string()))?
    }
}

impl LocalEngineHydrationHandler {
    fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl RemoteHydrationClient for LocalEngineHydrationHandler {
    async fn hydrate_entities(
        &self,
        request: RemoteHydrationRequest,
    ) -> Result<Vec<copperdb_search::RrfHydrationRecord>, GrpcError> {
        forwarded_caller_claims(
            &self.state,
            request.caller_auth_token.as_deref(),
            &request.placement.database,
            false,
        )?;
        observe_remote_read_fence(&self.state, &request.placement.database, request.read_fence)
            .map_err(GrpcError::Transport)?;
        let (request_context, _request_guard) = request
            .request_context
            .map(RequestContext::from_metadata)
            .unwrap_or_else(|| RequestContext::root(None));
        let state = Arc::clone(&self.state);
        let database = request.placement.database.clone();
        let global_ids = request.global_ids.clone();
        tokio::task::spawn_blocking(move || {
            let engine = open_engine(&state, &database).map_err(GrpcError::Transport)?;
            engine
                .hydrate_fabric_entities_locally_with_context(&request_context, &global_ids)
                .map_err(|error| GrpcError::Transport(error.to_string()))
        })
        .await
        .map_err(|error| GrpcError::Transport(error.to_string()))?
    }
}

pub fn build_engine_backed_nornic_replica_service(
    state: Arc<AppState>,
    replica_handler: Arc<dyn RemoteReplicaClient>,
) -> NornicReplicaService {
    let mut service = NornicReplicaService::new(replica_handler);
    if state.auth.security_enabled {
        service = service.with_auth_validator(Arc::new(UnifiedClusterAuthValidator {
            state: Arc::clone(&state),
        }));
    }
    service
        .with_ranked_search_handler(Arc::new(LocalEngineRankedSearchHandler::new(Arc::clone(
            &state,
        ))))
        .with_hydration_handler(Arc::new(LocalEngineHydrationHandler::new(state)))
}

pub fn build_local_nornic_replica_service(state: Arc<AppState>) -> NornicReplicaService {
    let replica_handler: Arc<dyn RemoteReplicaClient> =
        Arc::new(LocalEngineReplicaHandler::new(Arc::clone(&state)));
    build_engine_backed_nornic_replica_service(state, replica_handler)
}

impl Default for AppState {
    fn default() -> Self {
        let db_manager = Arc::new(
            DatabaseManager::open("./data/copperdb-multidb")
                .unwrap_or_else(|_| DatabaseManager::new()),
        );
        let _ = db_manager.create("copperdb", "./data/copperdb");
        Self {
            db_name: "copperdb".into(),
            runtime_config: Arc::new(RuntimeConfig::default()),
            retention: Arc::new(RwLock::new(RetentionManager::default())),
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
            graphql_schema: copperdb_graphql::build_default_schema(),
            engine_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

/// Server health check response.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
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
        .route("/db/{database}/search", post(search_handler))
        .route(
            "/admin/databases/{database}/config",
            get(get_database_config_handler).put(update_database_config_handler),
        )
        .route(
            "/admin/databases/{database}/config/effective",
            get(get_effective_database_config_handler),
        )
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
        .route("/admin/retention/status", get(retention_status))
        // ── GraphQL ──────────────────────────────────────────────────────
        .route("/graphql", post(graphql_handler))
        .route("/graphql", get(graphql_playground_handler))
        // ── MCP (Model Context Protocol) ──────────────────────────────────
        .route("/mcp", post(mcp_handler))
        // ── SPA fallback: any unmatched route serves index.html when UI is available ──
        .fallback(ui_fallback);

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

#[allow(clippy::result_large_err)]
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
        .unwrap_or_else(|| "localhost:7474".to_string())
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

/// Returns true when the UI can actually be served (index.html exists).
/// Checks static_dir first, then embedded assets (matching NornicDB's embed.FS).
fn ui_available(state: &AppState) -> bool {
    if let Some(root) = static_root(state) {
        if root.join("index.html").exists() {
            return true;
        }
    }
    ui_assets::embedded_ui_available()
}

fn read_static_file(state: &AppState, relative_path: &str) -> Option<Vec<u8>> {
    // Static dir override takes precedence (dev mode / custom UI)
    if let Some(root) = static_root(state) {
        let path = root.join(relative_path.trim_start_matches('/'));
        if let Ok(bytes) = std::fs::read(&path) {
            return Some(bytes);
        }
    }
    // Fall back to embedded assets (production binary)
    ui_assets::get_embedded(relative_path)
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
    // Only serve UI for browser requests when a UI dist is actually available
    // (matching NornicDB: uiHandler != nil check — skip UI when init failed).
    if is_ui_request(&headers) && !state.headless && ui_available(&state) {
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

/// SPA fallback: any unmatched GET request serves index.html when the UI
/// is available. Mimics NornicDB's uiHandler.ServeHTTP which serves
/// index.html for all non-asset paths when uiHandler != nil.
async fn ui_fallback(
    State(state): State<Arc<AppState>>,
    request: Request<Body>,
) -> impl IntoResponse {
    if request.method() != Method::GET {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    if ui_available(&state) && !state.headless {
        serve_ui_index(&state)
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
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
    let token = authenticated_token(state, headers)?;
    state
        .auth
        .open_authenticator()
        .ok()?
        .validate_token(token)
        .ok()
}

fn authenticated_token<'a>(state: &AppState, headers: &'a HeaderMap) -> Option<&'a str> {
    bearer_token(headers).or_else(|| cookie_token(state, headers))
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
    #[serde(default)]
    bookmarks: Vec<String>,
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

#[derive(Deserialize)]
struct DatabaseConfigUpdateRequest {
    overrides: BTreeMap<String, String>,
}

/// POST /db/{database}/search — BM25 fulltext search with optional RRF vector fusion.
/// Request body: {"query": "...", "labels": ["Label"], "limit": 10}
/// Response matches NornicDB's search endpoint shape.
async fn search_handler(
    State(state): State<Arc<AppState>>,
    Path(database): Path<String>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    if let Err(status) = authorize_database_access(&state, &headers, &database, false) {
        return status.into_response();
    }

    let query_text = body
        .get("query")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let labels: Vec<String> = body
        .get("labels")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();
    let limit = body
        .get("limit")
        .and_then(|v| v.as_u64())
        .unwrap_or(10)
        .max(1)
        .min(1000) as usize;

    if query_text.is_empty() {
        return Json(serde_json::json!({
            "status": "ok",
            "query": "",
            "results": [],
            "total_candidates": 0,
            "returned": 0,
            "search_method": "bm25",
            "metrics": {"bm25_time_ms": 0}
        }))
        .into_response();
    }

    let engine = match open_engine(&state, &database) {
        Ok(e) => e,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": err})),
            )
                .into_response();
        }
    };

    let index_defs = match engine.list_index_definitions() {
        Ok(defs) => defs,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "failed to load indexes"})),
            )
                .into_response();
        }
    };

    // Check if BM25 is configured
    let bm25_indexes: Vec<_> = index_defs
        .iter()
        .filter(|idx| idx.kind == copperdb_storage::IndexKind::FullText
            && (labels.is_empty() || labels.iter().any(|l| l == &idx.label)))
        .cloned()
        .collect();

    // Check if vector indexes exist for potential RRF
    let vector_indexes: Vec<_> = index_defs
        .iter()
        .filter(|idx| idx.kind == copperdb_storage::IndexKind::Vector
            && (labels.is_empty() || labels.iter().any(|l| l == &idx.label)))
        .cloned()
        .collect();

    if bm25_indexes.is_empty() && vector_indexes.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "no search indexes configured",
                "hint": "Create a fulltext index: CREATE FULLTEXT INDEX ... FOR (n:Label) ON EACH [n.prop]"
            })),
        )
            .into_response();
    }

    let bm25_start = std::time::Instant::now();

    // BM25 fulltext search
    let mut bm25_results: Vec<serde_json::Value> = Vec::new();
    let mut bm25_candidates: usize = 0;
    if !bm25_indexes.is_empty() {
        let fetch_limit = limit * 3; // overfetch for RRF merging
        // Collect raw hits with ranks for RRF fusion
        let mut all_hits: Vec<(String, f32, String)> = Vec::new(); // (id, score, snippet)
        for index in &bm25_indexes {
            if let Ok(hits) = engine.search_fulltext_nodes(
                &index.label,
                &index.properties,
                &query_text,
                fetch_limit,
            ) {
                bm25_candidates = bm25_candidates.max(hits.len());
                for hit in hits {
                    all_hits.push((hit.id, hit.score, hit.snippet.unwrap_or_default()));
                }
            }
        }
        // Sort by score descending, deduplicate by id
        all_hits.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        let mut seen = std::collections::HashSet::new();
        all_hits.retain(|(id, _, _)| seen.insert(id.clone()));

        for (rank, (id, score, snippet)) in all_hits.iter().enumerate().take(limit) {
            let mut result = serde_json::json!({
                "id": id,
                "score": score,
                "snippet": snippet,
                "bm25_rank": rank + 1,
            });
            // Enrich with node properties if available
            if let Ok(Some(node)) = engine.get_node(id) {
                result["properties"] = serde_json::Value::Object(
                    node.properties
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                );
                result["labels"] = serde_json::json!(node.labels);
            }
            bm25_results.push(result);
        }
    }
    let bm25_time_ms = bm25_start.elapsed().as_millis() as u64;

    let search_method = if !vector_indexes.is_empty() {
        "bm25" // TODO: wire RRF hybrid when vector search API is available
    } else {
        "bm25"
    };
    let total_candidates = bm25_candidates;

    Json(serde_json::json!({
        "status": "success",
        "query": query_text,
        "results": bm25_results,
        "total_candidates": total_candidates,
        "returned": bm25_results.len(),
        "search_method": search_method,
        "bm25_candidates": bm25_candidates,
        "metrics": {
            "bm25_time_ms": bm25_time_ms,
        }
    }))
    .into_response()
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

async fn get_database_config_handler(
    State(state): State<Arc<AppState>>,
    Path(database): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(status) = authorize_database_access(&state, &headers, &database, false) {
        return status.into_response();
    }

    if state.db_manager.get(&database).is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }

    Json(serde_json::json!({
        "database": database,
        "overrides": state.db_manager.get_config_overrides(&database),
        "allowedKeys": state.db_manager.allowed_config_keys(),
    }))
    .into_response()
}

async fn update_database_config_handler(
    State(state): State<Arc<AppState>>,
    Path(database): Path<String>,
    headers: HeaderMap,
    Json(request): Json<DatabaseConfigUpdateRequest>,
) -> impl IntoResponse {
    if let Err(status) = authorize_database_access(&state, &headers, &database, true) {
        return status.into_response();
    }

    match state
        .db_manager
        .set_config_overrides(&database, request.overrides)
    {
        Ok(()) => Json(serde_json::json!({
            "database": database,
            "overrides": state.db_manager.get_config_overrides(&database),
            "allowedKeys": state.db_manager.allowed_config_keys(),
        }))
        .into_response(),
        Err(MultiDbError::NotFound(_)) => StatusCode::NOT_FOUND.into_response(),
        Err(MultiDbError::InvalidConfig(error)) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": error})),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": error.to_string()})),
        )
            .into_response(),
    }
}

async fn get_effective_database_config_handler(
    State(state): State<Arc<AppState>>,
    Path(database): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(status) = authorize_database_access(&state, &headers, &database, false) {
        return status.into_response();
    }

    match state
        .db_manager
        .effective_config(&database, state.runtime_config.as_ref())
    {
        Ok(config) => Json(serde_json::json!({
            "database": database,
            "effective": config,
        }))
        .into_response(),
        Err(MultiDbError::NotFound(_)) => StatusCode::NOT_FOUND.into_response(),
        Err(MultiDbError::InvalidConfig(error)) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": error})),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": error.to_string()})),
        )
            .into_response(),
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
    let caller_auth_token = authenticated_token(&state, &headers).map(str::to_owned);
    let request_region = headers
        .get("x-copperdb-region")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let distributed_read_fence = if distributed {
        match derive_distributed_read_fence(&state, &database, &request.bookmarks) {
            Ok(read_fence) => read_fence,
            Err(error) => {
                return Json(Neo4jCommitResponse {
                    results: Vec::new(),
                    errors: vec![Neo4jError {
                        code: "Neo.ClientError.Statement.ExecutionFailed".into(),
                        message: error,
                    }],
                })
                .into_response();
            }
        }
    } else {
        None
    };

    let mut results = Vec::new();
    let mut errors = Vec::new();

    for statement in request.statements {
        let state = Arc::clone(&state);
        let database = database.clone();
        let roles = roles.clone();
        let caller_auth_token = caller_auth_token.clone();
        let request_region = request_region.clone();
        let (request_context, _request_guard) = RequestContext::root(None);
        let result = tokio::task::spawn_blocking(move || {
            execute_statement(
                Arc::clone(&state),
                database,
                request_context,
                statement.statement,
                statement.parameters.unwrap_or_default(),
                roles,
                distributed,
                distributed_read_fence,
                caller_auth_token,
                request_region,
            )
        })
        .await
        .map_err(|error| error.to_string())
        .and_then(|result| result);
        match result {
            Ok(result) => results.push(result),
            Err(error) => errors.push(Neo4jError {
                code: "Neo.ClientError.Statement.ExecutionFailed".into(),
                message: error,
            }),
        }
    }

    Json(Neo4jCommitResponse { results, errors }).into_response()
}

#[allow(clippy::too_many_arguments)]
fn execute_statement(
    state: Arc<AppState>,
    database: String,
    request_context: RequestContext,
    statement: String,
    parameters: HashMap<String, serde_json::Value>,
    roles: Vec<String>,
    distributed: bool,
    distributed_read_fence: Option<LogicalTransactionId>,
    caller_auth_token: Option<String>,
    request_region: Option<String>,
) -> Result<Neo4jResult, String> {
    let normalized = statement.trim();
    let upper = normalized.to_ascii_uppercase();

    if database == "system" {
        if upper == "SHOW DATABASES" {
            return Ok(show_databases_result(&state));
        }
        if upper.starts_with("CREATE DATABASE ") {
            let name = parse_database_name(normalized, "CREATE DATABASE ")?;
            create_database(&state, &name)?;
            return Ok(empty_neo4j_result());
        }
        if upper.starts_with("DROP DATABASE ") {
            let name = parse_database_name(normalized, "DROP DATABASE ")?;
            drop_database(&state, &name)?;
            return Ok(empty_neo4j_result());
        }
        return Err(format!("unsupported system statement: {}", statement));
    }

    if state.db_manager.get(&database).is_none() {
        return Err(format!("database not found: {database}"));
    }

    let engine = open_engine(&state, &database)?;
    let result = if distributed {
        let placement = PlacementKey::default_for_database(&database);
        let consistency = ConsistencyLevel::Quorum;
        let request_region = request_region.as_deref();
        let transport = build_local_replica_transport(
            &state,
            &engine,
            &placement,
            consistency,
            request_region,
            caller_auth_token.as_deref(),
            statement_requires_write(normalized),
        )?;
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?
            .block_on(async {
                engine
                    .execute_distributed_with_read_fence_as_with_context(
                        &request_context,
                        normalized,
                        parameters,
                        &roles,
                        &placement,
                        consistency,
                        request_region,
                        distributed_read_fence,
                        transport,
                    )
                    .await
                    .map(|outcome| outcome.result)
                    .map_err(|error| error.to_string())
            })?
    } else {
        engine
            .execute_as_with_context(&request_context, normalized, parameters, &roles)
            .map_err(|error| error.to_string())?
    };
    Ok(convert_engine_result(result))
}

#[derive(Clone)]
pub struct AppStateBoltExecutor {
    state: Arc<AppState>,
}

impl AppStateBoltExecutor {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

impl QueryExecutor for AppStateBoltExecutor {
    fn execute(
        &self,
        query: &str,
        params: &HashMap<String, serde_json::Value>,
    ) -> Result<BoltQueryResult, String> {
        self.execute_on_database(None, query, params)
    }

    fn execute_on_database(
        &self,
        database: Option<&str>,
        query: &str,
        params: &HashMap<String, serde_json::Value>,
    ) -> Result<BoltQueryResult, String> {
        let database = database.unwrap_or(&self.state.db_name).to_owned();
        let (request_context, _request_guard) = RequestContext::root(None);
        self.execute_on_database_with_context(&database, query, params, request_context)
    }

    fn execute_on_database_with_context(
        &self,
        database: &str,
        query: &str,
        params: &HashMap<String, serde_json::Value>,
        request_context: RequestContext,
    ) -> Result<BoltQueryResult, String> {
        let result = execute_statement(
            Arc::clone(&self.state),
            database.to_owned(),
            request_context,
            query.to_owned(),
            params.clone(),
            vec!["admin".into()],
            false,
            None,
            None,
            None,
        )?;
        Ok(BoltQueryResult {
            columns: result.columns,
            rows: result.data.into_iter().map(|row| row.row).collect(),
        })
    }
}

fn derive_distributed_read_fence(
    state: &AppState,
    database: &str,
    bookmarks: &[String],
) -> Result<Option<LogicalTransactionId>, String> {
    let engine = open_engine(state, database)?;
    let config = SessionConfig {
        mode: TransactionMode::Read,
        database: Some(database.to_string()),
        bookmarks: bookmarks.to_vec(),
        bookmark_mode: if bookmarks.is_empty() {
            BookmarkMode::None
        } else {
            BookmarkMode::Required
        },
        ..SessionConfig::default()
    };
    let transaction_id = engine
        .begin_transaction(&config)
        .map_err(|error| error.to_string())?;
    let read_fence = engine
        .transaction_read_fence(&transaction_id)
        .map_err(|error| error.to_string())?;
    engine.tx_manager().remove(&transaction_id);
    Ok(Some(read_fence))
}

fn observe_remote_read_fence(
    state: &AppState,
    database: &str,
    read_fence: Option<LogicalTransactionId>,
) -> Result<Option<LogicalTransactionId>, String> {
    let Some(read_fence) = read_fence else {
        return Ok(None);
    };

    derive_distributed_read_fence(state, database, &[read_fence.stable_id()])
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

fn build_local_replica_transport(
    state: &AppState,
    engine: &GraphEngine,
    placement: &PlacementKey,
    consistency: ConsistencyLevel,
    request_region: Option<&str>,
    caller_auth_token: Option<&str>,
    write: bool,
) -> Result<Arc<dyn ReplicaTransport>, String> {
    let mut client = TonicRemoteReplicaClient::new();
    if state.auth.security_enabled {
        let token = TokenManager::new(state.auth.jwt_secret.clone())
            .issue("copperdb-cluster", vec!["admin".into()], 300)
            .map_err(|error| error.to_string())?;
        client = client.with_auth_token(token);
    }
    if !write {
        if state.auth.security_enabled && caller_auth_token.is_none() {
            return Err(
                "distributed server reads require a forwarded caller authorization token".into(),
            );
        }
        if let Some(token) = caller_auth_token {
            client = client.with_caller_auth_token(token.to_owned());
        }
    }
    if state.runtime_config.server.grpc_tls_enabled {
        client = client.with_tls_enabled(true);
    }
    if let Some(domain_name) = state.runtime_config.server.grpc_tls_domain_name.clone() {
        client = client.with_tls_domain_name(domain_name);
    }
    if let Some(ca_cert_path) = state.runtime_config.server.grpc_tls_ca_cert.as_deref() {
        let ca_cert = std::fs::read_to_string(ca_cert_path).map_err(|error| {
            format!("failed to read gRPC TLS CA certificate {ca_cert_path}: {error}")
        })?;
        client = client.with_tls_ca_certificate_pem(ca_cert);
    }
    if let (Some(client_cert_path), Some(client_key_path)) = (
        state.runtime_config.server.grpc_tls_client_cert.as_deref(),
        state.runtime_config.server.grpc_tls_client_key.as_deref(),
    ) {
        let client_cert = std::fs::read_to_string(client_cert_path).map_err(|error| {
            format!("failed to read gRPC TLS client certificate {client_cert_path}: {error}")
        })?;
        let client_key = std::fs::read_to_string(client_key_path).map_err(|error| {
            format!("failed to read gRPC TLS client key {client_key_path}: {error}")
        })?;
        client = client.with_tls_identity_pem(client_cert, client_key);
    }

    if write {
        let plan = engine
            .plan_distributed_write(placement, consistency, request_region)
            .map_err(|error| error.to_string())?;

        return Ok(Arc::new(NornicGrpcReplicaTransport::from_write_plan(
            &plan,
            Arc::new(client),
        )));
    }

    let plan = engine
        .plan_distributed_read(placement, consistency, request_region)
        .map_err(|error| error.to_string())?;

    Ok(Arc::new(NornicGrpcReplicaTransport::from_read_plan(
        &plan,
        Arc::new(client),
    )))
}

fn create_database(state: &AppState, name: &str) -> Result<(), String> {
    let path = state.db_manager.default_storage_path(name);
    match state.db_manager.create(name, path) {
        Ok(()) => Ok(()),
        Err(MultiDbError::AlreadyExists(_)) => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

fn drop_database(state: &AppState, name: &str) -> Result<(), String> {
    let database =
        DatabaseManager::drop(&state.db_manager, name).map_err(|error| error.to_string())?;
    // Remove fjall data files so disk space is reclaimed.
    let _ = std::fs::remove_dir_all(&database.storage_path);
    Ok(())
}

fn open_engine(state: &AppState, database: &str) -> Result<Arc<GraphEngine>, String> {
    // Check cache first — avoids LSM-tree recovery (400–600 ms) per query.
    {
        let cache = state.engine_cache.read();
        if let Some(engine) = cache.get(database) {
            return Ok(Arc::clone(engine));
        }
    }
    let database_record = state
        .db_manager
        .get(database)
        .ok_or_else(|| format!("database not found: {database}"))?;
    let runtime_config = state
        .db_manager
        .effective_config(database, state.runtime_config.as_ref())
        .map_err(|error| error.to_string())?;
    let config = EngineConfig {
        data_dir: database_record.storage_path,
        default_database: database.into(),
        auth_enabled: state.auth.security_enabled,
        log_queries: false,
        runtime_config,
        ..Default::default()
    };
    let engine = Arc::new(GraphEngine::open(config).map_err(|error| error.to_string())?);
    // Lazy-load retention data from the shared storage (avoids a second StorageEngine::open).
    if database == "copperdb" {
        let storage = Arc::clone(engine.storage_engine());
        let _ = state.retention.write().ensure_loaded(storage);
    }
    let mut cache = state.engine_cache.write();
    cache.insert(database.to_string(), Arc::clone(&engine));
    Ok(engine)
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
    #[serde(default)]
    bookmarks: Vec<String>,
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

#[allow(clippy::type_complexity)]
fn build_fabric_ranked_search_context(
    state: &AppState,
    engine: &GraphEngine,
    fabric: &FabricDatabase,
    hydration_consistency: ConsistencyLevel,
    caller_auth_token: Option<&str>,
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

    let mut ranked_client = TonicRemoteRankedSearchClient::new();
    if let Some(token) = caller_auth_token {
        ranked_client = ranked_client.with_caller_auth_token(token.to_owned());
    }
    if state.runtime_config.server.grpc_tls_enabled {
        ranked_client = ranked_client.with_tls_enabled(true);
    }
    if let Some(domain_name) = state.runtime_config.server.grpc_tls_domain_name.clone() {
        ranked_client = ranked_client.with_tls_domain_name(domain_name);
    }
    if let Some(ca_cert_path) = state.runtime_config.server.grpc_tls_ca_cert.as_deref() {
        let ca_cert = std::fs::read_to_string(ca_cert_path).map_err(|error| {
            format!("failed to read gRPC TLS CA certificate {ca_cert_path}: {error}")
        })?;
        ranked_client = ranked_client.with_tls_ca_certificate_pem(ca_cert);
    }
    if let (Some(client_cert_path), Some(client_key_path)) = (
        state.runtime_config.server.grpc_tls_client_cert.as_deref(),
        state.runtime_config.server.grpc_tls_client_key.as_deref(),
    ) {
        let client_cert = std::fs::read_to_string(client_cert_path).map_err(|error| {
            format!("failed to read gRPC TLS client certificate {client_cert_path}: {error}")
        })?;
        let client_key = std::fs::read_to_string(client_key_path).map_err(|error| {
            format!("failed to read gRPC TLS client key {client_key_path}: {error}")
        })?;
        ranked_client = ranked_client.with_tls_identity_pem(client_cert, client_key);
    }
    let mut hydration_client = TonicRemoteHydrationClient::new();
    if let Some(token) = caller_auth_token {
        hydration_client = hydration_client.with_caller_auth_token(token.to_owned());
    }
    if state.runtime_config.server.grpc_tls_enabled {
        hydration_client = hydration_client.with_tls_enabled(true);
    }
    if let Some(domain_name) = state.runtime_config.server.grpc_tls_domain_name.clone() {
        hydration_client = hydration_client.with_tls_domain_name(domain_name);
    }
    if let Some(ca_cert_path) = state.runtime_config.server.grpc_tls_ca_cert.as_deref() {
        let ca_cert = std::fs::read_to_string(ca_cert_path).map_err(|error| {
            format!("failed to read gRPC TLS CA certificate {ca_cert_path}: {error}")
        })?;
        hydration_client = hydration_client.with_tls_ca_certificate_pem(ca_cert);
    }
    if let (Some(client_cert_path), Some(client_key_path)) = (
        state.runtime_config.server.grpc_tls_client_cert.as_deref(),
        state.runtime_config.server.grpc_tls_client_key.as_deref(),
    ) {
        let client_cert = std::fs::read_to_string(client_cert_path).map_err(|error| {
            format!("failed to read gRPC TLS client certificate {client_cert_path}: {error}")
        })?;
        let client_key = std::fs::read_to_string(client_key_path).map_err(|error| {
            format!("failed to read gRPC TLS client key {client_key_path}: {error}")
        })?;
        hydration_client = hydration_client.with_tls_identity_pem(client_cert, client_key);
    }

    let ranked_transport: Arc<dyn RankedSearchTransport> = Arc::new(
        NornicGrpcRankedSearchTransport::new(search_endpoints, Arc::new(ranked_client)),
    );
    let hydration_transport: Arc<dyn HydrationTransport> = Arc::new(
        NornicGrpcHydrationTransport::new(hydration_endpoints, Arc::new(hydration_client)),
    );
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
    read_fence: Option<LogicalTransactionId>,
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
            read_fence,
        });
    }
    Ok(requests)
}

#[allow(clippy::result_large_err)]
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
    let caller_auth_token = authenticated_token(&state, request.headers()).map(str::to_owned);
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
    execute_fabric_ranked_search_admin_impl(state, tenant, database, request, caller_auth_token)
        .await
}

async fn execute_fabric_ranked_search_admin_impl(
    state: Arc<AppState>,
    tenant: String,
    database: String,
    request: FabricRankedSearchRequest,
    caller_auth_token: Option<String>,
) -> Response {
    let (request_context, _request_guard) = RequestContext::root(None);
    let read_fence = match derive_distributed_read_fence(&state, &database, &request.bookmarks) {
        Ok(read_fence) => read_fence,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": error.to_string()})),
            )
                .into_response();
        }
    };
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
        if let Err(error) = engine.validate_ranked_search_query(&request.query) {
            return match error {
                copperdb_engine::CopperDbError::Config(message) => (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": message})),
                )
                    .into_response(),
                other => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": other.to_string()})),
                )
                    .into_response(),
            };
        }
        let (search_plans, hydration_coordinators, ranked_transport, hydration_transport) =
            match build_fabric_ranked_search_context(
                &state,
                &engine,
                &fabric,
                hydration_consistency,
                caller_auth_token.as_deref(),
            ) {
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
    let collected = match collect_planned_fabric_ranked_batches_with_context(
        &request_context,
        search_plans.clone(),
        request.query,
        read_fence,
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
    let hydration_requests =
        match build_fabric_hydration_requests(&merged, &hydration_coordinators, read_fence) {
            Ok(requests) => requests,
            Err(error) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": error})),
                )
                    .into_response();
            }
        };
    let hydration = match collect_fabric_hydration_records_with_context(
        &request_context,
        hydration_requests,
        hydration_transport,
    )
    .await
    {
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

// ── GraphQL endpoint ──────────────────────────────────────────────────────

async fn graphql_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    request: GraphQLRequest,
) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &state.db_name, false) {
        return status.into_response();
    }
    let gql_response = state.graphql_schema.execute(request.into_inner()).await;
    let body = axum::body::Body::from(serde_json::to_vec(&gql_response).unwrap_or_default());
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

async fn graphql_playground_handler() -> impl IntoResponse {
    Html(include_str!("../playground.html"))
}

// ── MCP (Model Context Protocol) handler ───────────────────────────────────

async fn mcp_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &state.db_name, false) {
        return status.into_response();
    }
    let request: copperdb_mcp::McpRequest = match serde_json::from_value(body) {
        Ok(req) => req,
        Err(e) => {
            return Json(serde_json::json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": { "code": -32700, "message": e.to_string() }
            }))
            .into_response()
        }
    };
    let engine = match open_engine(&state, &state.db_name) {
        Ok(engine) => engine,  // already Arc<GraphEngine>
        Err(e) => {
            return Json(serde_json::json!({
                "jsonrpc": "2.0",
                "id": request.id,
                "error": { "code": -32000, "message": e }
            }))
            .into_response()
        }
    };
    let registry = copperdb_mcp::ToolRegistry::with_engine(engine);
    let response = registry.dispatch(&request);
    Json(response).into_response()
}

#[cfg(test)]
mod tests;
