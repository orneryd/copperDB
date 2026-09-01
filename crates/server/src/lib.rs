//! HTTP/REST API server for copperdb.
//!
//! Equivalent to Go's `pkg/server` in NornicDB.
//! Provides a management REST API and serves the GraphQL endpoint.
//! Uses `axum` (Rust equivalent of Go's `net/http` + `gorilla/mux`).

use async_graphql_axum::GraphQLRequest;
use axum::{
    body::Body,
    extract::{rejection::JsonRejection, DefaultBodyLimit, MatchedPath, Path, Query, State},
    http::{header, HeaderMap, HeaderValue, Method, Request, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{any, delete, get, post, post_service},
    Extension, Json, Router,
};
use copperdb_auth::{
    AuthConfig, AuthError, Authenticator, Claims, DatabaseAccessMode, Permission,
    PermissionsForRoles, TokenManager,
};
use copperdb_bolt::server::{
    BoltAuthProvider, BoltExecutionError, BoltPrincipal, BoltQueryResult, BoltResultStats,
    BoltRuntimeCounters, BoltTransaction, BoltTransactionError, QueryExecutor,
};
use copperdb_buildinfo::{display_version, server_announcement, version, BUILD_INFO};
use copperdb_config::Config as RuntimeConfig;
use copperdb_engine::{
    query_procedure_mode, CopperDb as GraphEngine, DatabaseConfig as EngineConfig,
    QueryProcedureMode, ResultStats,
};
use copperdb_envutil::{get as env_get, get_bool_loose, parse_duration};
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
use copperdb_otel::{
    classify_cypher_op_type, CancellationProtocol, CancellationStage, Health, Telemetry,
};
use copperdb_plugin::{
    resolve_packages, ActionCallContext, ActionError, ActionQueryResult, ActionQueryService,
    DatabaseEvent, DatabaseEventType, PackageFactory, PackageRuntime, PackageSpec,
    ResolvedPackageSet,
};
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
    validate_http_origin, RequestTarget, RequestViolation, SecurityConfig, SecurityMiddleware,
    SecurityRequest,
};
use copperdb_storage::{StorageEngine, StorageError, StorageTransaction};

mod ui_assets;
use copperdb_topology::{
    ConsistencyLevel, FabricDatabase, FabricGlobalId, LogicalTransactionId, PlacementKey,
};
use copperdb_txsession::{BookmarkMode, SessionConfig, TransactionMode};
use copperdb_util::{RequestContext, RequestContextGuard};
use opentelemetry::global;
use opentelemetry::propagation::Extractor;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

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
    authenticator: Arc<Authenticator>,
}

impl Default for AuthState {
    fn default() -> Self {
        Self::from_runtime_config(&RuntimeConfig::default())
            .expect("durable authenticator must initialize during server startup")
    }
}

impl AuthState {
    pub fn from_runtime_config(config: &RuntimeConfig) -> Result<Self, AuthError> {
        let username = env_get("COPPERDB_AUTH_USERNAME", "admin");
        let password = env_get("COPPERDB_AUTH_PASSWORD", "password");
        let secret = env_get("COPPERDB_AUTH_JWT_SECRET", &config.auth.jwt_secret);
        let security_enabled = config.auth.enabled;
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
    }

    pub fn from_storage_path(
        auth_storage_path: String,
        security_enabled: bool,
        dev_login_enabled: bool,
        username: String,
        password: String,
        jwt_secret: String,
    ) -> Result<Self, AuthError> {
        let auth_config = AuthConfig {
            jwt_secret: jwt_secret.clone().into_bytes(),
            token_expiry: Some(Duration::from_secs(7 * 24 * 60 * 60)),
            default_admin_username: username.clone(),
            security_enabled,
            ..Default::default()
        };
        let storage = Arc::new(StorageEngine::open(&auth_storage_path)?);
        let authenticator = Arc::new(Authenticator::new(auth_config, storage)?);
        authenticator.seed_builtin_access_if_empty()?;
        if security_enabled
            && matches!(
                authenticator.get_user(&username),
                Err(AuthError::UserNotFound(_))
            )
        {
            authenticator.create_user(&username, &password, vec!["admin".into()])?;
        }
        Ok(Self {
            security_enabled,
            dev_login_enabled,
            username,
            password,
            cookie_name: "nornicdb_token".into(),
            jwt_secret,
            auth_storage_path,
            authenticator,
        })
    }

    #[allow(clippy::arc_with_non_send_sync)]
    fn open_authenticator(&self) -> Result<Arc<Authenticator>, AuthError> {
        Ok(Arc::clone(&self.authenticator))
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
    /// Monotonic process-local origin for operational uptime reporting.
    pub started_at: Instant,
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
    /// Owner-maintained HTTP lifecycle counters.
    pub http_counters: Arc<HttpCounters>,
    mcp_sessions: Arc<McpSessionStore>,
    /// Owner-maintained Bolt protocol counters, populated only when Bolt is enabled.
    pub bolt_counters: Arc<BoltRuntimeCounters>,
    /// Whether this process runs the Bolt protocol listener.
    pub bolt_enabled: bool,
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
    /// Process-wide package registries resolved before any database is opened.
    pub packages: Arc<ResolvedPackageSet>,
    /// Started package instances retained for health and reverse shutdown.
    pub package_runtime: Option<Arc<PackageRuntime>>,
}

#[derive(Default)]
pub struct HttpCounters {
    requests: AtomicU64,
    errors: AtomicU64,
    active: AtomicU64,
}

const DEFAULT_MCP_SESSION_TTL: Duration = Duration::from_secs(30 * 60);
const DEFAULT_MAX_MCP_SESSIONS: usize = 4096;
const MCP_SESSION_ID_HEADER: &str = "mcp-session-id";
const MAX_MCP_SESSION_ID_BYTES: usize = 128;

struct McpSessionStore {
    sessions: Mutex<HashMap<String, Instant>>,
    ttl: Duration,
    max_sessions: usize,
}

impl Default for McpSessionStore {
    fn default() -> Self {
        Self::new(DEFAULT_MCP_SESSION_TTL, DEFAULT_MAX_MCP_SESSIONS)
    }
}

impl McpSessionStore {
    fn new(ttl: Duration, max_sessions: usize) -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            ttl: if ttl.is_zero() {
                DEFAULT_MCP_SESSION_TTL
            } else {
                ttl
            },
            max_sessions: max_sessions.max(1),
        }
    }

    fn create(&self) -> String {
        self.create_at(Instant::now())
    }

    fn create_at(&self, now: Instant) -> String {
        let mut sessions = self.sessions.lock();
        sessions.retain(|_, expires_at| *expires_at > now);
        if sessions.len() >= self.max_sessions {
            if let Some(session_id) = sessions
                .iter()
                .min_by_key(|(_, expires_at)| **expires_at)
                .map(|(session_id, _)| session_id.clone())
            {
                sessions.remove(&session_id);
            }
        }
        let session_id = uuid::Uuid::new_v4().to_string();
        sessions.insert(session_id.clone(), now + self.ttl);
        session_id
    }

    fn validate(&self, session_id: &str) -> bool {
        self.validate_at(session_id, Instant::now())
    }

    fn validate_at(&self, session_id: &str, now: Instant) -> bool {
        let mut sessions = self.sessions.lock();
        let Some(expires_at) = sessions.get_mut(session_id) else {
            return false;
        };
        if *expires_at <= now {
            sessions.remove(session_id);
            return false;
        }
        *expires_at = now + self.ttl;
        true
    }

    fn terminate(&self, session_id: &str) -> bool {
        self.sessions.lock().remove(session_id).is_some()
    }
}

struct ActiveHttpRequest {
    counters: Arc<HttpCounters>,
}

impl Drop for ActiveHttpRequest {
    fn drop(&mut self) {
        self.counters.active.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct LocalEngineReplicaHandler {
    state: Arc<AppState>,
    storage_cache: Arc<RwLock<HashMap<String, Arc<StorageEngineAdapter>>>>,
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
        Self {
            state,
            storage_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn database_for_command(&self, command: &Command) -> String {
        match command {
            Command::CypherMutation { database, .. } if !database.is_empty() => database.clone(),
            _ => self.state.db_name.clone(),
        }
    }

    fn open_replication_storage(
        &self,
        database: &str,
    ) -> Result<Arc<StorageEngineAdapter>, GrpcError> {
        if let Some(storage) = self.storage_cache.read().get(database) {
            return Ok(Arc::clone(storage));
        }

        let mut cache = self.storage_cache.write();
        if let Some(storage) = cache.get(database) {
            return Ok(Arc::clone(storage));
        }

        let engine = open_engine(&self.state, database).map_err(GrpcError::Transport)?;
        let storage = Arc::new(StorageEngineAdapter::from_shared(Arc::clone(
            engine.storage(),
        )));
        cache.insert(database.to_owned(), Arc::clone(&storage));
        Ok(storage)
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
    forwarded_caller_roles(state, caller_auth_token, database, write).map(|_| ())
}

fn forwarded_caller_roles(
    state: &AppState,
    caller_auth_token: Option<&str>,
    database: &str,
    write: bool,
) -> Result<Vec<String>, GrpcError> {
    if !state.auth.security_enabled {
        return Ok(vec!["admin".into()]);
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
    Ok(claims.roles)
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
        let roles = forwarded_caller_roles(
            &self.state,
            request.caller_auth_token.as_deref(),
            &request.placement.database,
            false,
        )?;
        observe_remote_read_fence(&self.state, &request.placement.database, request.read_fence)
            .map_err(GrpcError::Transport)?;
        let (request_context, request_guard) = request
            .request_context
            .map(RequestContext::from_metadata)
            .unwrap_or_else(|| RequestContext::root(None));
        let mut cancellation_guard = GrpcCancellationGuard::new(
            request_context.clone(),
            request_guard,
            Arc::clone(&self.state.telemetry),
        );
        let state = Arc::clone(&self.state);
        let database = request.placement.database.clone();
        let placement = request.placement.clone();
        let query = request.query.clone();
        let result = tokio::task::spawn_blocking(move || {
            let engine = open_engine(&state, &database).map_err(GrpcError::Transport)?;
            engine
                .search_fabric_ranked_batch_locally_scoped_with_context_and_roles(
                    &request_context,
                    &placement,
                    &query,
                    &[],
                    &BTreeMap::new(),
                    &roles,
                )
                .map_err(grpc_error_from_engine)
        })
        .await;
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                cancellation_guard.finish();
                return Err(GrpcError::Transport(error.to_string()));
            }
        };
        cancellation_guard.finish_with_result(&result);
        result
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
        let (request_context, request_guard) = request
            .request_context
            .map(RequestContext::from_metadata)
            .unwrap_or_else(|| RequestContext::root(None));
        let mut cancellation_guard = GrpcCancellationGuard::new(
            request_context.clone(),
            request_guard,
            Arc::clone(&self.state.telemetry),
        );
        let state = Arc::clone(&self.state);
        let database = request.placement.database.clone();
        let global_ids = request.global_ids.clone();
        let result = tokio::task::spawn_blocking(move || {
            let engine = open_engine(&state, &database).map_err(GrpcError::Transport)?;
            engine
                .hydrate_fabric_entities_locally_with_context(&request_context, &global_ids)
                .map_err(grpc_error_from_engine)
        })
        .await;
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                cancellation_guard.finish();
                return Err(GrpcError::Transport(error.to_string()));
            }
        };
        cancellation_guard.finish_with_result(&result);
        result
    }
}

fn grpc_error_from_engine(error: copperdb_engine::CopperDbError) -> GrpcError {
    match error {
        copperdb_engine::CopperDbError::RequestCancelled(cancelled) => {
            GrpcError::RequestCancelled(cancelled)
        }
        other => GrpcError::Transport(other.to_string()),
    }
}

struct GrpcCancellationGuard {
    request_guard: Option<RequestContextGuard>,
    request_context: RequestContext,
    telemetry: Arc<Telemetry>,
    finished: bool,
}

impl GrpcCancellationGuard {
    fn new(
        request_context: RequestContext,
        request_guard: RequestContextGuard,
        telemetry: Arc<Telemetry>,
    ) -> Self {
        Self {
            request_guard: Some(request_guard),
            request_context,
            telemetry,
            finished: false,
        }
    }

    fn finish(&mut self) {
        self.finished = true;
    }

    fn finish_with_result<T>(&mut self, result: &Result<T, GrpcError>) {
        if matches!(result, Err(GrpcError::RequestCancelled(_))) {
            if self.request_context.cancellation_reason().is_none() {
                self.request_context.cancel();
            }
            record_request_context_cancellation(
                self.telemetry.as_ref(),
                CancellationProtocol::Grpc,
                CancellationStage::Execution,
                &self.request_context,
            );
        }
        self.finish();
    }
}

impl Drop for GrpcCancellationGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        drop(self.request_guard.take());
        record_request_context_cancellation(
            self.telemetry.as_ref(),
            CancellationProtocol::Grpc,
            CancellationStage::Ingress,
            &self.request_context,
        );
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
        Self::with_auth(AuthState::default())
    }
}

impl AppState {
    pub fn with_auth(auth: AuthState) -> Self {
        let db_manager = Arc::new(
            DatabaseManager::open("./data/copperdb-multidb")
                .unwrap_or_else(|_| DatabaseManager::new()),
        );
        let _ = db_manager.create("copperdb", "./data/copperdb");
        Self {
            started_at: Instant::now(),
            db_name: "copperdb".into(),
            runtime_config: Arc::new(RuntimeConfig::default()),
            retention: Arc::new(RwLock::new(RetentionManager::default())),
            static_dir: None,
            base_path: "/".into(),
            headless: false,
            db_manager,
            auth,
            telemetry: Arc::new(Telemetry::new()),
            http_counters: Arc::new(HttpCounters::default()),
            mcp_sessions: Arc::new(McpSessionStore::default()),
            bolt_counters: Arc::new(BoltRuntimeCounters::default()),
            bolt_enabled: false,
            security: SecurityMiddleware::with_config(SecurityConfig {
                environment: env_get("COPPERDB_ENV", "development"),
                allow_http: get_bool_loose("COPPERDB_ALLOW_HTTP", true),
            }),
            distributed_cypher_enabled: get_bool_loose("COPPERDB_DISTRIBUTED_CYPHER", false),
            graphql_schema: copperdb_graphql::build_default_schema(),
            engine_cache: Arc::new(RwLock::new(HashMap::new())),
            packages: Arc::new(resolve_packages([]).expect("empty package set is valid")),
            package_runtime: None,
        }
    }

    pub async fn configure_runtime(
        &mut self,
        runtime_config: Arc<RuntimeConfig>,
    ) -> Result<(), ServerError> {
        for package_id in runtime_config
            .packages
            .configuration
            .keys()
            .chain(runtime_config.packages.grants.keys())
        {
            if !runtime_config.packages.enabled.contains(package_id) {
                return Err(ServerError::Engine(format!(
                    "package settings target a disabled package: {package_id}"
                )));
            }
        }
        let mut factories = Vec::<Arc<dyn PackageFactory>>::new();
        let mut specs = Vec::new();
        for package_id in &runtime_config.packages.enabled {
            match package_id.as_str() {
                copperdb_apoc::PACKAGE_ID => factories.push(Arc::new(copperdb_apoc::factory())),
                copperdb_heimdall::PACKAGE_ID => {
                    factories.push(Arc::new(copperdb_heimdall::factory()))
                }
                _ => {
                    return Err(ServerError::Engine(format!(
                        "unknown configured package: {package_id}"
                    )))
                }
            }
            specs.push(
                PackageSpec::new(package_id)
                    .required(runtime_config.packages.required.contains(package_id))
                    .with_configuration(
                        runtime_config
                            .packages
                            .configuration
                            .get(package_id)
                            .cloned()
                            .unwrap_or_else(|| Value::Object(Default::default())),
                    )
                    .granting(
                        runtime_config
                            .packages
                            .grants
                            .get(package_id)
                            .cloned()
                            .unwrap_or_default(),
                    ),
            );
        }
        for package_id in &runtime_config.packages.required {
            if !runtime_config.packages.enabled.contains(package_id) {
                return Err(ServerError::Engine(format!(
                    "required package is not enabled: {package_id}"
                )));
            }
        }
        let package_runtime = Arc::new(
            PackageRuntime::start(
                factories,
                specs,
                Duration::from_millis(runtime_config.packages.lifecycle_timeout_ms.max(1)),
            )
            .await
            .map_err(|error| ServerError::Engine(error.to_string()))?,
        );
        self.packages = package_runtime.packages();
        self.package_runtime = Some(package_runtime);
        self.runtime_config = runtime_config;
        Ok(())
    }

    pub async fn shutdown_packages(&self) -> Result<(), ServerError> {
        match &self.package_runtime {
            Some(runtime) => runtime
                .shutdown()
                .await
                .map_err(|error| ServerError::Engine(error.to_string())),
            None => Ok(()),
        }
    }

    pub fn execute_package_action(
        &self,
        request_context: &RequestContext,
        name: &str,
        input: &Value,
        default_database: &str,
        caller_roles: &[String],
    ) -> Result<Value, ActionError> {
        self.packages.action_registry().execute(
            name,
            &ActionCallContext {
                request_context,
                default_database,
                caller_roles,
                query_service: self,
            },
            input,
        )
    }
}

impl ActionQueryService for AppState {
    fn query_read(
        &self,
        request_context: &RequestContext,
        database: &str,
        cypher: &str,
        params: &BTreeMap<String, Value>,
        caller_roles: &[String],
    ) -> Result<ActionQueryResult, ActionError> {
        request_context
            .check_active()
            .map_err(|_| ActionError::new("request_cancelled"))?;
        if statement_requires_write(cypher) {
            return Err(ActionError::new("query_write_forbidden"));
        }
        if self.auth.security_enabled {
            ensure_roles_database_access(self, caller_roles, database, false)
                .map_err(|_| ActionError::new("database_read_forbidden"))?;
        }
        let engine = open_engine(self, database).map_err(ActionError::new)?;
        let started = Instant::now();
        let result = match engine.execute_as_with_context(
            request_context,
            cypher,
            params
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            caller_roles,
        ) {
            Ok(result) => result,
            Err(error) => {
                self.emit_query_event(QueryEventDetails {
                    event_type: DatabaseEventType::QueryFailed,
                    database,
                    query: cypher,
                    params,
                    duration: started.elapsed(),
                    rows_affected: 0,
                    error: Some(error.to_string()),
                });
                return Err(ActionError::new(error.to_string()));
            }
        };
        self.emit_query_event(QueryEventDetails {
            event_type: DatabaseEventType::QueryExecuted,
            database,
            query: cypher,
            params,
            duration: started.elapsed(),
            rows_affected: result.rows.len(),
            error: None,
        });
        Ok(ActionQueryResult {
            rows: result
                .rows
                .into_iter()
                .map(|row| row.into_iter().collect())
                .collect(),
        })
    }
}

struct QueryEventDetails<'a> {
    event_type: DatabaseEventType,
    database: &'a str,
    query: &'a str,
    params: &'a BTreeMap<String, Value>,
    duration: Duration,
    rows_affected: usize,
    error: Option<String>,
}

impl AppState {
    fn emit_query_event(&self, details: QueryEventDetails<'_>) {
        let Some(runtime) = &self.package_runtime else {
            return;
        };
        let mut event = DatabaseEvent::new(details.event_type);
        event.query = details.query.into();
        event.query_params = details.params.clone();
        event.duration = details.duration.as_nanos().min(u128::from(u64::MAX)) as u64;
        event.rows_affected = details.rows_affected.try_into().unwrap_or(i64::MAX);
        event
            .metadata
            .insert("database".into(), Value::String(details.database.into()));
        if let Some(error) = details.error {
            event.error = error;
        }
        let _ = runtime.emit_event(event);
    }
}

/// Server health check response.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

const STATUS_SCHEMA_VERSION: u32 = 1;
const STORAGE_SIZE_SNAPSHOT_MAX_AGE: Duration = Duration::from_secs(5);
const STATUS_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const SEARCH_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const MCP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_TRANSACTION_REQUEST_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const TRANSACTION_REQUEST_TIMEOUT_ENV: &str = "COPPERDB_HTTP_TX_TIMEOUT";
const UPSTREAM_TRANSACTION_REQUEST_TIMEOUT_ENV: &str = "NORNICDB_HTTP_TX_TIMEOUT";

#[derive(Clone, Copy)]
struct HttpRequestTimeout {
    duration: Duration,
    message: &'static str,
}

#[derive(Serialize)]
struct ServerStatusSnapshot {
    schema_version: u32,
    collected_at_unix_ms: u64,
    status: &'static str,
    startup: StartupRuntimeStatus,
    server: ServerRuntimeStatus,
    bolt: BoltRuntimeStatus,
    database: DatabaseRuntimeStatus,
    embeddings: EmbeddingRuntimeSummary,
}

#[derive(Serialize)]
struct StartupRuntimeStatus {
    phase: &'static str,
    search_ready_databases: usize,
    search_building_databases: usize,
}

#[derive(Serialize)]
struct ServerRuntimeStatus {
    uptime_seconds: u64,
    counters_state: &'static str,
    requests: Option<u64>,
    errors: Option<u64>,
    active: Option<u64>,
    version: String,
    announcement: String,
}

#[derive(Serialize)]
struct BoltRuntimeStatus {
    state: &'static str,
    active_connections: Option<u64>,
    active_sessions: Option<u64>,
    active_transactions: Option<u64>,
    failures: Option<u64>,
}

#[derive(Serialize)]
struct DatabaseRuntimeStatus {
    state: &'static str,
    nodes: Option<u64>,
    edges: Option<u64>,
    databases: usize,
    mvcc: Option<MvccRuntimeStatus>,
}

#[derive(Serialize)]
struct EmbeddingRuntimeSummary {
    enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    processed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failed: Option<u64>,
}

#[derive(Serialize)]
struct MvccRuntimeStatus {
    enabled: bool,
    paused: bool,
    schedule_interval_ms: u64,
    floor: u64,
    head: u64,
    active_readers: u64,
    retained_versions: Option<usize>,
    prune_debt: Option<usize>,
    suggested_prune_floor: Option<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DatabaseInfoSnapshot {
    schema_version: u32,
    collected_at_unix_ms: u64,
    name: String,
    status: &'static str,
    #[serde(rename = "default")]
    is_default: bool,
    #[serde(rename = "type")]
    database_type: &'static str,
    node_count: Option<u64>,
    label_node_count: Option<u64>,
    edge_count: Option<u64>,
    node_storage_bytes: u64,
    node_storage_sampled_at_unix_ms: Option<u64>,
    node_storage_sample_age_ms: Option<u64>,
    managed_embedding_bytes: Option<u64>,
    embedding_state: String,
    embedding_pending: Option<u64>,
    search_ready: bool,
    search_building: bool,
    search_initialized: bool,
}

fn collected_at_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn graph_counts(engine: &GraphEngine) -> (Option<u64>, Option<u64>) {
    (
        engine.storage().total_node_count().ok(),
        engine.storage().total_edge_count().ok(),
    )
}

fn mvcc_runtime_status(engine: &GraphEngine) -> MvccRuntimeStatus {
    let status = engine.storage().operational_mvcc_status();
    MvccRuntimeStatus {
        enabled: status.enabled,
        paused: status.paused,
        schedule_interval_ms: status.schedule_interval_ms,
        floor: status.floor,
        head: status.head,
        active_readers: status.active_reader_count,
        retained_versions: None,
        prune_debt: None,
        suggested_prune_floor: None,
    }
}

fn embedding_runtime_summary(engine: Option<&GraphEngine>) -> EmbeddingRuntimeSummary {
    let Some(engine) = engine else {
        return EmbeddingRuntimeSummary {
            enabled: None,
            status: Some("unknown"),
            processed: None,
            failed: None,
        };
    };
    let status = engine.embedding_operational_status();
    if status.state == copperdb_engine::EmbeddingRuntimeState::Disabled {
        return EmbeddingRuntimeSummary {
            enabled: Some(false),
            status: None,
            processed: None,
            failed: None,
        };
    }
    EmbeddingRuntimeSummary {
        enabled: Some(true),
        status: Some(if status.worker_count > 0 {
            "processing"
        } else {
            "idle"
        }),
        processed: Some(status.completed),
        failed: Some(status.failed),
    }
}

#[derive(Deserialize)]
struct DatabaseInfoQuery {
    label: Option<String>,
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
        .route("/copperdb.svg", get(copper_logo_handler))
        .route("/health", get(health_handler))
        .route("/status", get(status_handler))
        .route("/auth/config", get(auth_config_handler))
        .route("/auth/token", post(auth_token_handler))
        .route("/auth/logout", post(auth_logout_handler))
        .route("/auth/me", get(auth_me_handler))
        .route("/db/{database}", get(database_info_handler))
        .route("/db/{database}/tx/commit", post(neo4j_tx_commit_handler))
        .route("/db/{database}/search", post(database_search_handler))
        .route("/copperdb/search", post(copperdb_search_handler))
        .route(
            "/admin/databases/{database}/config",
            get(get_database_config_handler).put(update_database_config_handler),
        )
        .route(
            "/admin/databases/{database}/config/effective",
            get(get_effective_database_config_handler),
        )
        .route(
            "/admin/databases/{database}/wal",
            get(inspect_wal_handler).post(repair_wal_handler),
        )
        .route(
            "/admin/databases/{database}/mvcc/status",
            get(mvcc_lifecycle_status_handler),
        )
        .route(
            "/admin/databases/{database}/mvcc/debt",
            get(mvcc_lifecycle_debt_handler),
        )
        .route(
            "/admin/databases/{database}/mvcc/prune",
            post(trigger_mvcc_prune_handler),
        )
        .route(
            "/admin/databases/{database}/mvcc/pause",
            post(pause_mvcc_lifecycle_handler),
        )
        .route(
            "/admin/databases/{database}/mvcc/resume",
            post(resume_mvcc_lifecycle_handler),
        )
        .route(
            "/admin/databases/{database}/mvcc/schedule",
            post(set_mvcc_lifecycle_schedule_handler),
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
        .route(
            "/mcp",
            post(mcp_handler)
                .delete(mcp_delete_handler)
                .layer(DefaultBodyLimit::max(
                    copperdb_mcp::DEFAULT_MAX_REQUEST_BYTES,
                )),
        )
        // ── SPA fallback: any unmatched route serves index.html when UI is available ──
        .fallback(ui_fallback);

    let normalized = normalize_base_path(&state.base_path);
    let router = router.layer(middleware::from_fn_with_state(
        Arc::clone(&state),
        security_validation_middleware,
    ));
    let router = router.layer(middleware::from_fn_with_state(
        Arc::clone(&state),
        request_context_middleware,
    ));
    let router = router.layer(middleware::from_fn_with_state(
        Arc::clone(&state),
        http_metrics_middleware,
    ));

    if normalized == "/" {
        router.with_state(state)
    } else {
        Router::new().nest(&normalized, router).with_state(state)
    }
}

/// Build the unauthenticated telemetry probe router.
///
/// This is deliberately separate from the application router, matching
/// NornicDB's network-isolated observability listener.
pub fn build_telemetry_router(health: Arc<Health>) -> Router {
    build_observability_router(
        health,
        Arc::new(Telemetry::new()),
        false,
        "standalone".into(),
    )
}

#[derive(Clone)]
struct TelemetryRouterState {
    health: Arc<Health>,
    telemetry: Arc<Telemetry>,
    service_instance_id: String,
    started_at: Instant,
}

pub fn build_observability_router(
    health: Arc<Health>,
    telemetry: Arc<Telemetry>,
    metrics_enabled: bool,
    service_instance_id: String,
) -> Router {
    let state = TelemetryRouterState {
        health,
        telemetry,
        service_instance_id,
        started_at: Instant::now(),
    };
    if metrics_enabled {
        let _ = state.telemetry.set_gauge("nornicdb_build_info", &[], 1.0);
    }
    let mut router = Router::new()
        .route("/livez", any(livez_handler))
        .route("/readyz", any(readyz_handler))
        .route("/version", any(version_handler));
    if metrics_enabled {
        router = router.route("/metrics", any(metrics_handler));
    }
    router.with_state(state)
}

async fn request_context_middleware(
    State(state): State<Arc<AppState>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let timeout = http_request_timeout(request.uri().path());
    run_with_request_context(request, next, timeout, state.telemetry.as_ref()).await
}

async fn run_with_request_context(
    mut request: Request<Body>,
    next: Next,
    timeout: Option<HttpRequestTimeout>,
    telemetry: &Telemetry,
) -> Response {
    let protocol = match request.uri().path() {
        "/graphql" => CancellationProtocol::Graphql,
        "/mcp" => CancellationProtocol::Mcp,
        _ => CancellationProtocol::Http,
    };
    let deadline = timeout.and_then(|policy| SystemTime::now().checked_add(policy.duration));
    let (request_context, request_guard) = RequestContext::root(deadline);
    let mut cancellation_guard = HttpCancellationGuard {
        request_guard: Some(request_guard),
        request_context: request_context.clone(),
        telemetry,
        protocol,
        finished: false,
    };
    request.extensions_mut().insert(request_context.clone());
    let response = match timeout {
        Some(policy) => match tokio::time::timeout(policy.duration, next.run(request)).await {
            Ok(response) => {
                record_request_context_cancellation(
                    telemetry,
                    protocol,
                    CancellationStage::Execution,
                    &request_context,
                );
                response
            }
            Err(_) => {
                request_context.cancel_due_to_deadline();
                record_request_context_cancellation(
                    telemetry,
                    protocol,
                    CancellationStage::Ingress,
                    &request_context,
                );
                (StatusCode::SERVICE_UNAVAILABLE, policy.message).into_response()
            }
        },
        None => {
            let response = next.run(request).await;
            record_request_context_cancellation(
                telemetry,
                protocol,
                CancellationStage::Execution,
                &request_context,
            );
            response
        }
    };
    cancellation_guard.finished = true;
    response
}

struct HttpCancellationGuard<'a> {
    request_guard: Option<RequestContextGuard>,
    request_context: RequestContext,
    telemetry: &'a Telemetry,
    protocol: CancellationProtocol,
    finished: bool,
}

impl Drop for HttpCancellationGuard<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        drop(self.request_guard.take());
        record_request_context_cancellation(
            self.telemetry,
            self.protocol,
            CancellationStage::Ingress,
            &self.request_context,
        );
    }
}

fn record_request_context_cancellation(
    telemetry: &Telemetry,
    protocol: CancellationProtocol,
    stage: CancellationStage,
    request_context: &RequestContext,
) {
    if let Some(reason) = request_context.cancellation_reason() {
        let _ = telemetry.record_request_cancellation(protocol, stage, reason);
    }
}

fn http_request_timeout(path: &str) -> Option<HttpRequestTimeout> {
    match path {
        "/status" => Some(HttpRequestTimeout {
            duration: STATUS_REQUEST_TIMEOUT,
            message: "request timeout: status busy",
        }),
        "/copperdb/search" => Some(HttpRequestTimeout {
            duration: SEARCH_REQUEST_TIMEOUT,
            message: "request timeout: search busy",
        }),
        "/mcp" => Some(HttpRequestTimeout {
            duration: MCP_REQUEST_TIMEOUT,
            message: "request timeout: mcp busy",
        }),
        path if path.starts_with("/db/") && path.ends_with("/search") => Some(HttpRequestTimeout {
            duration: SEARCH_REQUEST_TIMEOUT,
            message: "request timeout: search busy",
        }),
        path if path.starts_with("/db/") && path.contains("/tx") => Some(HttpRequestTimeout {
            duration: transaction_request_timeout(),
            message: "request timeout: transaction busy",
        }),
        _ => None,
    }
}

fn transaction_request_timeout() -> Duration {
    let configured = env_get(TRANSACTION_REQUEST_TIMEOUT_ENV, "");
    let upstream = env_get(UPSTREAM_TRANSACTION_REQUEST_TIMEOUT_ENV, "");
    transaction_request_timeout_from(configured.trim(), upstream.trim())
}

fn transaction_request_timeout_from(configured: &str, upstream: &str) -> Duration {
    parse_duration(configured)
        .filter(|duration| !duration.is_zero())
        .or_else(|| parse_duration(upstream).filter(|duration| !duration.is_zero()))
        .unwrap_or(DEFAULT_TRANSACTION_REQUEST_TIMEOUT)
}

async fn http_metrics_middleware(
    State(state): State<Arc<AppState>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let started = Instant::now();
    let method = request.method().as_str().to_string();
    let path_template = request
        .extensions()
        .get::<MatchedPath>()
        .map_or("_NOT_FOUND_", MatchedPath::as_str)
        .to_string();
    state.http_counters.requests.fetch_add(1, Ordering::Relaxed);
    let active = state.http_counters.active.fetch_add(1, Ordering::Relaxed) + 1;
    let active_request = ActiveHttpRequest {
        counters: Arc::clone(&state.http_counters),
    };
    let _ = state
        .telemetry
        .set_gauge("nornicdb_http_in_flight_requests", &[], active as f64);

    let span = tracing::info_span!(
        "nornicdb.http.request",
        http.request.method = %method,
        http.route = %path_template,
        http.response.status_code = tracing::field::Empty,
    );
    let parent = global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor(request.headers()))
    });
    let _ = span.set_parent(parent);

    let response = next.run(request).instrument(span.clone()).await;
    span.record("http.response.status_code", response.status().as_u16());
    if response.status().is_client_error() || response.status().is_server_error() {
        state.http_counters.errors.fetch_add(1, Ordering::Relaxed);
    }
    let status_class = http_status_class(response.status());
    let labels = [
        ("method", method.as_str()),
        ("path_template", path_template.as_str()),
        ("status_class", status_class),
    ];
    let _ = state
        .telemetry
        .record_counter("nornicdb_http_requests_total", &labels);
    let _ = state.telemetry.observe_histogram(
        "nornicdb_http_request_duration_seconds",
        &labels,
        started.elapsed().as_secs_f64(),
    );
    drop(active_request);
    let remaining = state.http_counters.active.load(Ordering::Relaxed);
    let _ = state
        .telemetry
        .set_gauge("nornicdb_http_in_flight_requests", &[], remaining as f64);
    response
}

struct HeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        if !matches!(key, "traceparent" | "tracestate") {
            return None;
        }
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        ["traceparent", "tracestate"]
            .into_iter()
            .filter(|key| self.0.contains_key(*key))
            .collect()
    }
}

fn http_status_class(status: StatusCode) -> &'static str {
    match status.as_u16() / 100 {
        1 => "1xx",
        2 => "2xx",
        3 => "3xx",
        4 => "4xx",
        _ => "5xx",
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

/// /livez is an unconditional, bodyless process liveness probe.
async fn livez_handler() -> StatusCode {
    StatusCode::OK
}

/// /readyz reports the telemetry-owned readiness registry.
async fn readyz_handler(State(state): State<TelemetryRouterState>) -> Response {
    let response = state.health.ready().await;
    let status = if response.ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(response)).into_response()
}

const MAX_METRICS_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

async fn metrics_handler(
    State(state): State<TelemetryRouterState>,
    headers: HeaderMap,
) -> Response {
    let _ = state.telemetry.set_gauge(
        "nornicdb_process_uptime_seconds",
        &[],
        state.started_at.elapsed().as_secs_f64(),
    );
    let openmetrics = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("application/openmetrics-text"));
    let body = if openmetrics {
        state.telemetry.encode_openmetrics()
    } else {
        state.telemetry.encode_prometheus()
    };
    if body.len() > MAX_METRICS_RESPONSE_BYTES {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let content_type = if openmetrics {
        "application/openmetrics-text; version=1.0.0; charset=utf-8"
    } else {
        "text/plain; version=0.0.4; charset=utf-8"
    };
    ([(header::CONTENT_TYPE, content_type)], body).into_response()
}

#[derive(Serialize)]
struct TelemetryVersionResponse {
    version: String,
    commit: String,
    go: String,
    build_date: String,
    service_instance_id: String,
}

async fn version_handler(
    State(state): State<TelemetryRouterState>,
) -> Json<TelemetryVersionResponse> {
    Json(TelemetryVersionResponse {
        version: version().into(),
        commit: BUILD_INFO.git_commit.into(),
        go: BUILD_INFO.rust_version.into(),
        build_date: BUILD_INFO.build_date.into(),
        service_instance_id: state.service_instance_id,
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
    let (engine, search_ready_databases, search_building_databases) = {
        let engine_cache = state.engine_cache.read();
        let engine = engine_cache.get(&state.db_name).cloned();
        let mut search_ready_databases = 0;
        let mut search_building_databases = 0;
        for database in databases
            .iter()
            .filter(|database| database.name != "system")
        {
            let Some(database_engine) = engine_cache.get(&database.name) else {
                continue;
            };
            let search = database_engine.search_operational_status();
            search_ready_databases += usize::from(search.ready);
            search_building_databases += usize::from(search.building);
        }
        (engine, search_ready_databases, search_building_databases)
    };
    let (nodes, edges) = engine.as_deref().map(graph_counts).unwrap_or((None, None));
    let mvcc = engine.as_deref().map(mvcc_runtime_status);
    let embeddings = embedding_runtime_summary(engine.as_deref());
    Json(ServerStatusSnapshot {
        schema_version: STATUS_SCHEMA_VERSION,
        collected_at_unix_ms: collected_at_unix_ms(),
        status: "running",
        startup: StartupRuntimeStatus {
            phase: if search_building_databases > 0 {
                "search_warming"
            } else {
                "ready"
            },
            search_ready_databases,
            search_building_databases,
        },
        server: ServerRuntimeStatus {
            uptime_seconds: state.started_at.elapsed().as_secs(),
            counters_state: "ready",
            requests: Some(state.http_counters.requests.load(Ordering::Relaxed)),
            errors: Some(state.http_counters.errors.load(Ordering::Relaxed)),
            active: Some(state.http_counters.active.load(Ordering::Relaxed)),
            version: display_version(),
            announcement: server_announcement(),
        },
        bolt: if state.bolt_enabled {
            let bolt = state.bolt_counters.snapshot();
            BoltRuntimeStatus {
                state: "ready",
                active_connections: Some(bolt.active_connections),
                active_sessions: Some(bolt.active_sessions),
                active_transactions: Some(bolt.active_transactions),
                failures: Some(bolt.failures),
            }
        } else {
            BoltRuntimeStatus {
                state: "unknown",
                active_connections: None,
                active_sessions: None,
                active_transactions: None,
                failures: None,
            }
        },
        database: DatabaseRuntimeStatus {
            state: if engine.is_some() { "ready" } else { "unknown" },
            nodes,
            edges,
            databases: databases.iter().filter(|db| db.name != "system").count(),
            mvcc,
        },
        embeddings,
    })
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
    ensure_roles_database_access(state, &claims.roles, database, write)
}

fn ensure_roles_database_access(
    state: &AppState,
    roles: &[String],
    database: &str,
    write: bool,
) -> Result<(), StatusCode> {
    let authenticator = state
        .auth
        .open_authenticator()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let access_mode = authenticator
        .allowlist
        .access_mode_for_roles(roles.to_vec());
    if !access_mode.can_access_database(database) {
        return Err(StatusCode::FORBIDDEN);
    }
    let resolved = authenticator.privileges.resolve(roles, database);
    if (write && !resolved.write) || (!write && !resolved.read) {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(())
}

fn ensure_admin_access(state: &AppState, claims: &Claims) -> Result<(), StatusCode> {
    ensure_roles_admin_access(state, &claims.roles)
}

fn ensure_roles_admin_access(state: &AppState, roles: &[String]) -> Result<(), StatusCode> {
    let authenticator = state
        .auth
        .open_authenticator()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if PermissionsForRoles::from_role_names(roles, Some(&authenticator.entitlements))
        .contains(&Permission::Admin)
    {
        Ok(())
    } else {
        Err(StatusCode::FORBIDDEN)
    }
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
    #[serde(skip)]
    stats: ResultStats,
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

#[derive(Default, Deserialize)]
struct SearchRequest {
    #[serde(default)]
    database: String,
    #[serde(default)]
    mode: Option<SearchMode>,
    #[serde(default)]
    query: String,
    #[serde(default)]
    vector: Option<Vec<f32>>,
    #[serde(default)]
    labels: Vec<String>,
    #[serde(default)]
    indexes: Vec<String>,
    #[serde(default)]
    limit: usize,
    #[serde(default)]
    offset: usize,
    #[serde(default)]
    min_score: Option<f32>,
    #[serde(default)]
    rrf_k: Option<f32>,
    #[serde(default)]
    vector_weight: Option<f32>,
    #[serde(default)]
    bm25_weight: Option<f32>,
    #[serde(default)]
    include_diagnostics: bool,
    #[serde(default)]
    filters: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum SearchMode {
    Lexical,
    Semantic,
    Hybrid,
}

/// POST /copperdb/search — CopperDB's branded equivalent of NornicDB search.
async fn copperdb_search_handler(
    State(state): State<Arc<AppState>>,
    Extension(request_context): Extension<RequestContext>,
    headers: HeaderMap,
    Json(request): Json<SearchRequest>,
) -> impl IntoResponse {
    search_handler(state, request_context, headers, request, None).await
}

/// POST /db/{database}/search — database-scoped CopperDB search.
async fn database_search_handler(
    State(state): State<Arc<AppState>>,
    Path(database): Path<String>,
    Extension(request_context): Extension<RequestContext>,
    headers: HeaderMap,
    Json(request): Json<SearchRequest>,
) -> impl IntoResponse {
    search_handler(state, request_context, headers, request, Some(database)).await
}

async fn search_handler(
    state: Arc<AppState>,
    request_context: RequestContext,
    headers: HeaderMap,
    request: SearchRequest,
    path_database: Option<String>,
) -> Response {
    let database = path_database
        .or_else(|| (!request.database.is_empty()).then_some(request.database))
        .unwrap_or_else(|| state.db_name.clone());
    let claims = match authorize_database_access(&state, &headers, &database, false) {
        Ok(claims) => claims,
        Err(status) => return status.into_response(),
    };
    let roles = roles_for_claims(claims.as_ref());
    if state.db_manager.get(&database).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("database not found: {database}")})),
        )
            .into_response();
    }
    let limit = if request.limit == 0 {
        10
    } else {
        request.limit
    };
    let candidate_limit = limit.saturating_add(request.offset);
    let min_score = match request.min_score {
        Some(min_score) if !min_score.is_finite() => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "min_score must be finite"})),
            )
                .into_response();
        }
        Some(min_score) => min_score,
        None => f32::NEG_INFINITY,
    };
    let rrf_config = match (request.rrf_k, request.vector_weight, request.bm25_weight) {
        (None, None, None) => None,
        (rrf_k, vector_weight, bm25_weight) => {
            let rrf_k = rrf_k.unwrap_or(60.0);
            let vector_weight = vector_weight.unwrap_or(1.0);
            let bm25_weight = bm25_weight.unwrap_or(1.0);
            if !rrf_k.is_finite()
                || rrf_k <= 0.0
                || !vector_weight.is_finite()
                || vector_weight < 0.0
                || !bm25_weight.is_finite()
                || bm25_weight < 0.0
            {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": "rrf_k must be finite and positive; vector_weight and bm25_weight must be finite and non-negative"
                    })),
                )
                    .into_response();
            }
            Some(
                RrfConfig::new(rrf_k, candidate_limit)
                    .with_min_score(0.01)
                    .with_weights(vector_weight, bm25_weight),
            )
        }
    };

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
    let index_defs = if request.indexes.is_empty() {
        index_defs
    } else {
        let selected = index_defs
            .into_iter()
            .filter(|index| request.indexes.contains(&index.name))
            .collect::<Vec<_>>();
        if selected.len() != request.indexes.len()
            || selected.iter().any(|index| {
                index.entity_type != copperdb_storage::IndexEntityType::Node
                    || !matches!(
                        index.kind,
                        copperdb_storage::IndexKind::FullText | copperdb_storage::IndexKind::Vector
                    )
            })
        {
            let _ = state.telemetry.record_counter(
                "nornicdb_search_requests_total",
                &[("mode", "unknown"), ("result", "error")],
            );
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "indexes must name declared node FULLTEXT or VECTOR indexes"
                })),
            )
                .into_response();
        }
        selected
    };

    // Check if BM25 is configured
    let bm25_indexes: Vec<_> = index_defs
        .iter()
        .filter(|idx| {
            idx.kind == copperdb_storage::IndexKind::FullText
                && (request.labels.is_empty() || request.labels.iter().any(|l| l == &idx.label))
        })
        .cloned()
        .collect();

    // Check if vector indexes exist for potential RRF
    let vector_indexes: Vec<_> = index_defs
        .iter()
        .filter(|idx| {
            idx.kind == copperdb_storage::IndexKind::Vector
                && (request.labels.is_empty() || request.labels.iter().any(|l| l == &idx.label))
        })
        .cloned()
        .collect();

    if bm25_indexes.is_empty() && vector_indexes.is_empty() {
        let _ = state.telemetry.record_counter(
            "nornicdb_search_requests_total",
            &[("mode", "unknown"), ("result", "error")],
        );
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "no search indexes configured",
                "database": database,
                "retryable": true,
                "request_status": "search_not_ready",
                "hint": "Create a fulltext index: CREATE FULLTEXT INDEX ... FOR (n:Label) ON EACH [n.prop]"
            })),
        )
            .into_response();
    }

    let embedding_started = std::time::Instant::now();
    let embedding_span = tracing::info_span!("nornicdb.search.embed");
    let embedding_span_guard = embedding_span.enter();
    let query_vector = if request.vector.is_some() {
        request.vector.clone()
    } else if vector_indexes.is_empty() {
        None
    } else {
        match engine.embed_search_query_with_context(&request_context, &request.query) {
            Ok(vector) => vector,
            Err(error @ copperdb_engine::CopperDbError::RequestCancelled(_)) => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({"error": error.to_string()})),
                )
                    .into_response();
            }
            Err(error) if !bm25_indexes.is_empty() => {
                tracing::warn!(database, %error, "query embedding failed; falling back to BM25");
                None
            }
            Err(error) => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({"error": error.to_string()})),
                )
                    .into_response();
            }
        }
    };
    drop(embedding_span_guard);
    let embedding_elapsed = embedding_started.elapsed();
    let embedding_time_ms = embedding_elapsed.as_millis() as u64;
    let missing_query = || {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "search requires non-empty query text or a query vector"
            })),
        )
            .into_response()
    };
    let (query, search_method) = match (request.mode, query_vector) {
        (Some(SearchMode::Lexical), _) if !request.query.is_empty() => (
            SearchQuery::FullText {
                query: request.query.clone(),
                fields: bm25_indexes
                    .iter()
                    .flat_map(|index| index.properties.iter().cloned())
                    .collect(),
                limit: candidate_limit,
            },
            "bm25",
        ),
        (Some(SearchMode::Semantic), Some(vector)) => (
            SearchQuery::Semantic {
                vector,
                k: candidate_limit,
                min_score,
            },
            "semantic",
        ),
        (Some(SearchMode::Hybrid), Some(vector)) if !request.query.is_empty() => (
            SearchQuery::Hybrid {
                text: request.query.clone(),
                vector,
                k: candidate_limit,
            },
            "hybrid",
        ),
        (Some(_), _) => {
            return missing_query();
        }
        (None, Some(vector)) if !request.query.is_empty() && !bm25_indexes.is_empty() => (
            SearchQuery::Hybrid {
                text: request.query.clone(),
                vector,
                k: candidate_limit,
            },
            "hybrid",
        ),
        (None, Some(vector)) if request.query.is_empty() => (
            SearchQuery::Semantic {
                vector,
                k: candidate_limit,
                min_score,
            },
            "semantic",
        ),
        (None, None) if !request.query.is_empty() && !bm25_indexes.is_empty() => (
            SearchQuery::FullText {
                query: request.query.clone(),
                fields: bm25_indexes
                    .iter()
                    .flat_map(|index| index.properties.iter().cloned())
                    .collect(),
                limit: candidate_limit,
            },
            "bm25",
        ),
        _ => return missing_query(),
    };
    let metric_mode = if search_method == "semantic" {
        "vector"
    } else {
        search_method
    };

    let search_started = std::time::Instant::now();
    let search_span = tracing::info_span!("nornicdb.search", search.mode = search_method);
    let search_span_guard = search_span.enter();
    let placement = PlacementKey::default_for_database(&database);
    let outcome = match engine
        .search_fabric_ranked_outcome_locally_scoped_with_context_and_roles_and_indexes_and_rrf_config(
            &request_context,
            &placement,
            &query,
            &request.labels,
            &request.filters,
            &roles,
            &request.indexes,
            rrf_config,
        ) {
        Ok(outcome) => outcome,
        Err(error) => {
            let _ = state.telemetry.record_counter(
                "nornicdb_search_requests_total",
                &[("mode", metric_mode), ("result", "error")],
            );
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": error.to_string()})),
            )
                .into_response();
        }
    };
    drop(search_span_guard);
    let search_elapsed = search_started.elapsed();
    let search_time_ms = search_elapsed.as_millis() as u64;
    let hydration_started = std::time::Instant::now();
    let candidate_count = outcome.input_hits;
    let fused_count = outcome.fused_hits;
    let output_count = outcome.output_hits;
    let filtered_count = outcome.filtered_hits;
    let sources = outcome.sources.clone();
    let results = outcome
        .results
        .into_iter()
        .skip(request.offset)
        .take(limit)
        .filter_map(|hit| {
            let node = engine.get_node(&hit.global_id.local_id).ok()??;
            Some(serde_json::json!({
                "node": {
                    "id": node.id,
                    "labels": node.labels,
                    "properties": node.properties,
                },
                "score": hit.rrf_score,
                "rrf_score": hit.rrf_score,
                "vector_rank": hit.vector_rank,
                "bm25_rank": hit.bm25_rank,
            }))
        })
        .collect::<Vec<_>>();
    let hydration_elapsed = hydration_started.elapsed();
    let hydration_time_ms = hydration_elapsed.as_millis() as u64;
    let result = if results.is_empty() {
        "no_results"
    } else {
        "success"
    };
    let _ = state.telemetry.record_counter(
        "nornicdb_search_requests_total",
        &[("mode", metric_mode), ("result", result)],
    );
    for (stage, elapsed) in [("embed", embedding_elapsed), ("index", search_elapsed)] {
        let _ = state.telemetry.observe_histogram(
            "nornicdb_search_duration_seconds",
            &[("mode", metric_mode), ("stage", stage)],
            elapsed.as_secs_f64(),
        );
    }
    let _ = state.telemetry.set_gauge(
        "nornicdb_search_candidates_rows",
        &[],
        candidate_count as f64,
    );
    tracing::debug!(
        search_method,
        embedding_time_ms,
        search_time_ms,
        hydration_time_ms,
        returned = results.len(),
        "search completed"
    );

    if request.include_diagnostics {
        Json(serde_json::json!({
            "results": results,
            "diagnostics": {
                "status": result,
                "search_method": search_method,
                "ready": true,
                "sources": sources,
                "input_candidates": candidate_count,
                "fused_candidates": fused_count,
                "output_candidates": output_count,
                "filtered_candidates": filtered_count,
                "returned": results.len(),
                "partial": false,
                "timings": {
                    "embedding_ms": embedding_time_ms,
                    "index_ms": search_time_ms,
                    "hydration_ms": hydration_time_ms,
                },
            },
        }))
        .into_response()
    } else {
        Json(results).into_response()
    }
}

async fn database_info_handler(
    State(state): State<Arc<AppState>>,
    Path(database): Path<String>,
    Query(query): Query<DatabaseInfoQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(status) = authorize_database_access(&state, &headers, &database, false) {
        return status.into_response();
    }

    match state.db_manager.get(&database) {
        Some(db) => {
            let engine = open_engine(&state, &database).ok();
            let (node_count, edge_count) =
                engine.as_deref().map(graph_counts).unwrap_or((None, None));
            let label_node_count = query.label.as_deref().and_then(|label| {
                engine
                    .as_deref()
                    .and_then(|engine| engine.storage().node_count_by_label(label).ok())
            });
            let storage_size = engine.as_ref().map(|engine| {
                engine
                    .storage()
                    .size_on_disk_snapshot(STORAGE_SIZE_SNAPSHOT_MAX_AGE)
            });
            let search_status = engine
                .as_ref()
                .map(|engine| engine.search_operational_status());
            let embedding_status = engine
                .as_ref()
                .and_then(|engine| engine.embedding_runtime_status().ok());
            Json(DatabaseInfoSnapshot {
                schema_version: STATUS_SCHEMA_VERSION,
                collected_at_unix_ms: collected_at_unix_ms(),
                name: db.name.clone(),
                status: database_status_name(db.status),
                is_default: db.name == state.db_name,
                database_type: if db.name == "system" {
                    "system"
                } else {
                    "standard"
                },
                node_count,
                label_node_count,
                edge_count,
                node_storage_bytes: storage_size.map(|sample| sample.bytes).unwrap_or(0),
                node_storage_sampled_at_unix_ms: storage_size
                    .map(|sample| sample.sampled_at_unix_ms),
                node_storage_sample_age_ms: storage_size.map(|sample| sample.age_ms),
                managed_embedding_bytes: None,
                embedding_state: embedding_status
                    .as_ref()
                    .map(|status| format!("{:?}", status.state).to_ascii_lowercase())
                    .unwrap_or_else(|| "unknown".into()),
                embedding_pending: embedding_status.map(|status| status.pending),
                search_ready: db.status == DatabaseStatus::Online
                    && search_status.as_ref().is_some_and(|status| status.ready),
                search_building: search_status.as_ref().is_some_and(|status| status.building),
                search_initialized: search_status.is_some_and(|status| status.initialized),
            })
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
    Extension(request_context): Extension<RequestContext>,
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
    if request
        .statements
        .iter()
        .any(|statement| statement_requires_admin(&statement.statement))
    {
        if let Some(claims) = claims.as_ref() {
            if let Err(status) = ensure_admin_access(&state, claims) {
                return status.into_response();
            }
        }
    }
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
        let request_context = request_context.clone();
        let parent_span = tracing::Span::current();
        let result = tokio::task::spawn_blocking(move || {
            parent_span.in_scope(|| {
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
        })
        .await
        .map_err(|error| StatementExecutionError::Message(error.to_string()))
        .and_then(|result| result);
        match result {
            Ok(result) => results.push(result),
            Err(StatementExecutionError::RequestCancelled(_)) => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "request timeout: transaction busy",
                )
                    .into_response();
            }
            Err(error) => errors.push(Neo4jError {
                code: "Neo.ClientError.Statement.ExecutionFailed".into(),
                message: error.to_string(),
            }),
        }
    }

    Json(Neo4jCommitResponse { results, errors }).into_response()
}

#[derive(Debug, Error)]
enum StatementExecutionError {
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    RequestCancelled(#[from] copperdb_util::RequestCancelled),
}

impl From<String> for StatementExecutionError {
    fn from(message: String) -> Self {
        Self::Message(message)
    }
}

impl From<copperdb_engine::CopperDbError> for StatementExecutionError {
    fn from(error: copperdb_engine::CopperDbError) -> Self {
        match error {
            copperdb_engine::CopperDbError::RequestCancelled(cancelled) => {
                Self::RequestCancelled(cancelled)
            }
            other => Self::Message(other.to_string()),
        }
    }
}

fn bolt_execution_error(error: StatementExecutionError) -> BoltExecutionError {
    match error {
        StatementExecutionError::RequestCancelled(cancelled) => {
            BoltExecutionError::RequestCancelled(cancelled)
        }
        StatementExecutionError::Message(message) => BoltExecutionError::Message(message),
    }
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
) -> Result<Neo4jResult, StatementExecutionError> {
    let query_started = Instant::now();
    let normalized = statement.trim();
    let upper = normalized.to_ascii_uppercase();
    let op_type = classify_cypher_op_type(normalized);
    let query_span = tracing::info_span!("nornicdb.cypher.execute", db.operation.name = op_type,);
    let _query_span_guard = query_span.enter();
    let fulltext_started = is_fulltext_procedure_call(&upper).then(std::time::Instant::now);

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
        return Err(format!("unsupported system statement: {}", statement).into());
    }

    if state.db_manager.get(&database).is_none() {
        return Err(format!("database not found: {database}").into());
    }

    let result = (|| -> Result<copperdb_engine::QueryResult, StatementExecutionError> {
        let engine = open_engine(&state, &database)?;
        if distributed {
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
                        .map_err(StatementExecutionError::from)
                })
        } else {
            engine
                .execute_as_with_context(&request_context, normalized, parameters, &roles)
                .map_err(StatementExecutionError::from)
        }
    })();

    if let Some(started) = fulltext_started {
        observe_fulltext_procedure(
            &state,
            started,
            result.as_ref().ok().map(|result| result.rows.len()),
        );
    }
    let _ = state
        .telemetry
        .record_counter("nornicdb_cypher_queries_total", &[("op_type", op_type)]);
    let _ = state.telemetry.observe_histogram(
        "nornicdb_cypher_query_duration_seconds",
        &[("op_type", op_type)],
        query_started.elapsed().as_secs_f64(),
    );
    if let Ok(result) = &result {
        let _ = state.telemetry.set_gauge(
            "nornicdb_cypher_rows_returned_rows",
            &[("op_type", op_type)],
            result.rows.len() as f64,
        );
    }
    result.map(convert_engine_result)
}

fn is_fulltext_procedure_call(upper_statement: &str) -> bool {
    upper_statement.starts_with("CALL DB.INDEX.FULLTEXT.QUERYNODES")
        || upper_statement.starts_with("CALL DB.INDEX.FULLTEXT.QUERYRELATIONSHIPS")
}

fn observe_fulltext_procedure(
    state: &AppState,
    started: std::time::Instant,
    candidate_count: Option<usize>,
) {
    let result = match candidate_count {
        Some(0) => "no_results",
        Some(_) => "success",
        None => "error",
    };
    let _ = state.telemetry.record_counter(
        "nornicdb_search_requests_total",
        &[("mode", "bm25"), ("result", result)],
    );
    let _ = state.telemetry.observe_histogram(
        "nornicdb_search_duration_seconds",
        &[("mode", "bm25"), ("stage", "index")],
        started.elapsed().as_secs_f64(),
    );
    if let Some(candidate_count) = candidate_count {
        let _ = state.telemetry.set_gauge(
            "nornicdb_search_candidates_rows",
            &[],
            candidate_count as f64,
        );
    }
}

#[derive(Clone)]
pub struct AppStateBoltExecutor {
    state: Arc<AppState>,
    storage_transactions: Arc<Mutex<HashMap<uuid::Uuid, StorageTransaction<'static>>>>,
}

impl AppStateBoltExecutor {
    pub fn new(state: Arc<AppState>) -> Self {
        Self {
            state,
            storage_transactions: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl BoltAuthProvider for AppStateBoltExecutor {
    fn authenticate(&self, username: &str, password: &str) -> Result<BoltPrincipal, String> {
        let authenticator = self
            .state
            .auth
            .open_authenticator()
            .map_err(|error| error.to_string())?;
        let (_, user) = authenticator
            .authenticate(username, password)
            .map_err(|error| error.to_string())?;
        let roles = user.role_names();
        Ok(BoltPrincipal {
            username: user.username,
            roles,
        })
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
            .map_err(|error| error.to_string())
    }

    fn execute_on_database_with_context(
        &self,
        database: &str,
        query: &str,
        params: &HashMap<String, serde_json::Value>,
        request_context: RequestContext,
    ) -> Result<BoltQueryResult, BoltExecutionError> {
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
        )
        .map_err(bolt_execution_error)?;
        Ok(BoltQueryResult {
            columns: result.columns,
            rows: result.data.into_iter().map(|row| row.row).collect(),
            stats: bolt_result_stats(result.stats),
            notifications: Vec::new(),
        })
    }

    fn execute_as_on_database_with_context(
        &self,
        database: &str,
        query: &str,
        params: &HashMap<String, serde_json::Value>,
        request_context: RequestContext,
        principal: Option<&BoltPrincipal>,
    ) -> Result<BoltQueryResult, BoltExecutionError> {
        let roles = match principal {
            Some(principal) => principal.roles.clone(),
            None if !self.state.auth.security_enabled => vec!["admin".into()],
            None => return Err("authentication required".into()),
        };
        if self.state.auth.security_enabled {
            ensure_roles_database_access(
                &self.state,
                &roles,
                database,
                statement_requires_write(query),
            )
            .map_err(|_| "caller is not authorized for database")?;
            if statement_requires_admin(query) {
                ensure_roles_admin_access(&self.state, &roles)
                    .map_err(|_| "procedure requires admin permission")?;
            }
        }
        let result = execute_statement(
            Arc::clone(&self.state),
            database.to_owned(),
            request_context,
            query.to_owned(),
            params.clone(),
            roles,
            false,
            None,
            None,
            None,
        )
        .map_err(bolt_execution_error)?;
        Ok(BoltQueryResult {
            columns: result.columns,
            rows: result.data.into_iter().map(|row| row.row).collect(),
            stats: bolt_result_stats(result.stats),
            notifications: Vec::new(),
        })
    }

    fn execute_as_on_database_with_context_and_bookmarks(
        &self,
        database: &str,
        query: &str,
        params: &HashMap<String, serde_json::Value>,
        request_context: RequestContext,
        principal: Option<&BoltPrincipal>,
        bookmarks: &[String],
    ) -> Result<BoltQueryResult, BoltExecutionError> {
        if !bookmarks.is_empty() {
            derive_distributed_read_fence(&self.state, database, bookmarks)
                .map_err(BoltExecutionError::from)?;
        }
        self.execute_as_on_database_with_context(
            database,
            query,
            params,
            request_context,
            principal,
        )
    }

    fn begin_transaction(
        &self,
        database: &str,
        metadata: &HashMap<String, serde_json::Value>,
        _principal: Option<&BoltPrincipal>,
    ) -> Result<BoltTransaction, String> {
        let engine = open_engine(&self.state, database)?;
        let bookmarks: Vec<String> = metadata
            .get("bookmarks")
            .and_then(serde_json::Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(|value| value.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        let config = SessionConfig {
            database: Some(database.to_owned()),
            bookmark_mode: if bookmarks.is_empty() {
                BookmarkMode::None
            } else {
                BookmarkMode::Required
            },
            bookmarks,
            ..SessionConfig::default()
        };
        let transaction_id = engine
            .begin_transaction(&config)
            .map_err(|error| error.to_string())?;
        let storage_transaction = match engine.begin_storage_transaction() {
            Ok(transaction) => transaction,
            Err(error) => {
                let _ = engine.tx_manager().rollback(transaction_id);
                return Err(error.to_string());
            }
        };
        self.storage_transactions
            .lock()
            .insert(transaction_id, storage_transaction);
        Ok(BoltTransaction {
            id: transaction_id.to_string(),
            database: database.to_owned(),
        })
    }

    fn commit_transaction(
        &self,
        transaction: &BoltTransaction,
    ) -> Result<String, BoltTransactionError> {
        let transaction_id = uuid::Uuid::parse_str(&transaction.id)
            .map_err(|_| BoltTransactionError::from("invalid Bolt transaction identifier"))?;
        let engine =
            open_engine(&self.state, &transaction.database).map_err(BoltTransactionError::from)?;
        let mut storage_transactions = self.storage_transactions.lock();
        let storage_transaction =
            storage_transactions
                .get_mut(&transaction_id)
                .ok_or_else(|| {
                    BoltTransactionError::from("Bolt storage transaction is no longer active")
                })?;
        if let Err(error) = storage_transaction.commit() {
            let is_conflict = matches!(&error, StorageError::TransactionConflict { .. });
            storage_transactions.remove(&transaction_id);
            drop(storage_transactions);
            let _ = engine.tx_manager().rollback(transaction_id);
            if is_conflict {
                let _ = self
                    .state
                    .telemetry
                    .record_counter("nornicdb_cypher_transaction_conflicts_total", &[]);
            }
            return Err(BoltTransactionError::from_error(error));
        }
        storage_transactions.remove(&transaction_id);
        drop(storage_transactions);
        engine
            .tx_manager()
            .commit_with_bookmark(transaction_id)
            .map_err(|error| BoltTransactionError::from(error.to_string()))
    }

    fn rollback_transaction(&self, transaction: &BoltTransaction) -> Result<(), String> {
        let transaction_id = uuid::Uuid::parse_str(&transaction.id)
            .map_err(|_| "invalid Bolt transaction identifier".to_owned())?;
        self.storage_transactions
            .lock()
            .get_mut(&transaction_id)
            .ok_or_else(|| "Bolt storage transaction is no longer active".to_owned())?
            .rollback();
        self.storage_transactions.lock().remove(&transaction_id);
        open_engine(&self.state, &transaction.database)?
            .tx_manager()
            .rollback(transaction_id)
            .map_err(|error| error.to_string())
    }

    fn execute_in_transaction_with_context(
        &self,
        transaction: &BoltTransaction,
        query: &str,
        params: &HashMap<String, serde_json::Value>,
        request_context: RequestContext,
        principal: Option<&BoltPrincipal>,
    ) -> Result<BoltQueryResult, BoltExecutionError> {
        let transaction_id = uuid::Uuid::parse_str(&transaction.id)
            .map_err(|_| BoltExecutionError::from("invalid Bolt transaction identifier"))?;
        let engine =
            open_engine(&self.state, &transaction.database).map_err(BoltExecutionError::from)?;
        let active = engine
            .tx_manager()
            .get(&transaction_id)
            .ok_or_else(|| BoltExecutionError::from("Bolt transaction is no longer active"))?;
        if active.database.as_deref() != Some(transaction.database.as_str()) || !active.is_active()
        {
            return Err("Bolt transaction is no longer active".into());
        }
        drop(active);
        let roles = match principal {
            Some(principal) => principal.roles.clone(),
            None if !self.state.auth.security_enabled => vec!["admin".into()],
            None => return Err("authentication required".into()),
        };
        let mut storage_transactions = self.storage_transactions.lock();
        let storage_transaction = storage_transactions
            .get_mut(&transaction_id)
            .ok_or_else(|| "Bolt storage transaction is no longer active".to_owned())?;
        let result = engine
            .execute_in_storage_transaction_as_with_context(
                &request_context,
                storage_transaction,
                query,
                params.clone(),
                &roles,
            )
            .map_err(StatementExecutionError::from)
            .map_err(bolt_execution_error)?;
        let result = convert_engine_result(result);
        Ok(BoltQueryResult {
            columns: result.columns,
            rows: result.data.into_iter().map(|row| row.row).collect(),
            stats: bolt_result_stats(result.stats),
            notifications: Vec::new(),
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
    Neo4jResult {
        columns,
        data,
        stats: result.stats,
    }
}

fn bolt_result_stats(stats: ResultStats) -> BoltResultStats {
    BoltResultStats {
        nodes_created: stats.nodes_created,
        nodes_deleted: stats.nodes_deleted,
        relationships_created: stats.relationships_created,
        relationships_deleted: stats.relationships_deleted,
        properties_set: stats.properties_set,
    }
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
    Neo4jResult {
        columns,
        data,
        stats: ResultStats::default(),
    }
}

fn empty_neo4j_result() -> Neo4jResult {
    Neo4jResult {
        columns: vec![],
        data: vec![],
        stats: ResultStats::default(),
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
    let mutates = upper.contains("CREATE")
        || upper.contains("DELETE")
        || upper.contains("SET ")
        || upper.contains("MERGE")
        || upper.contains("REMOVE ");
    if mutates {
        return true;
    }
    match query_procedure_mode(statement) {
        Some(QueryProcedureMode::Read | QueryProcedureMode::Dbms) => false,
        Some(QueryProcedureMode::Write) => true,
        None => {
            !(upper.starts_with("MATCH ")
                || upper.starts_with("RETURN ")
                || upper.starts_with("WITH ")
                || upper.starts_with("SHOW "))
        }
    }
}

fn statement_requires_admin(statement: &str) -> bool {
    query_procedure_mode(statement) == Some(QueryProcedureMode::Dbms)
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
    let apoc_import_granted = state
        .runtime_config
        .packages
        .grants
        .get(copperdb_apoc::PACKAGE_ID)
        .is_some_and(|grants| grants.contains(&copperdb_plugin::PackageCapability::FileImport));
    let package_graph_write_enabled = state
        .runtime_config
        .packages
        .grants
        .get(copperdb_apoc::PACKAGE_ID)
        .is_some_and(|grants| grants.contains(&copperdb_plugin::PackageCapability::QueryWrite));
    let apoc_network_granted = state
        .runtime_config
        .packages
        .grants
        .get(copperdb_apoc::PACKAGE_ID)
        .is_some_and(|grants| grants.contains(&copperdb_plugin::PackageCapability::Network));
    let package_import_file_root = apoc_import_granted
        .then(|| {
            state
                .runtime_config
                .packages
                .configuration
                .get(copperdb_apoc::PACKAGE_ID)
                .and_then(|config| config.get("file_access_root"))
                .and_then(Value::as_str)
                .filter(|root| !root.is_empty())
                .map(str::to_string)
        })
        .flatten();
    let package_remote_url_allowlist = if apoc_network_granted {
        state
            .runtime_config
            .packages
            .configuration
            .get(copperdb_apoc::PACKAGE_ID)
            .and_then(|config| config.get("remote_url_allowlist"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect()
    } else {
        Vec::new()
    };
    let config = EngineConfig {
        data_dir: database_record.storage_path,
        default_database: database.into(),
        auth_enabled: state.auth.security_enabled,
        log_queries: false,
        sync_writes: state.runtime_config.storage.sync_writes,
        runtime_config,
        package_import_file_root,
        package_remote_url_allowlist,
        package_graph_write_enabled,
        ..Default::default()
    };
    let engine = Arc::new(
        GraphEngine::open_with_packages(config, state.packages.as_ref())
            .map_err(|error| error.to_string())?,
    );
    // Lazy-load retention data from the shared storage (avoids a second StorageEngine::open).
    if database == "copperdb" {
        let storage = Arc::clone(engine.storage_engine());
        let _ = state.retention.write().ensure_loaded(storage);
    }
    let mut cache = state.engine_cache.write();
    cache.insert(database.to_string(), Arc::clone(&engine));
    Ok(engine)
}

fn offline_database_storage_path(state: &AppState, database: &str) -> Result<String, StatusCode> {
    let database = state
        .db_manager
        .get(database)
        .ok_or(StatusCode::NOT_FOUND)?;
    if state.engine_cache.read().contains_key(&database.name) {
        return Err(StatusCode::CONFLICT);
    }
    Ok(database.storage_path)
}

fn wal_integrity_response(status: copperdb_storage::WALIntegrityStatus) -> serde_json::Value {
    match status {
        copperdb_storage::WALIntegrityStatus::Healthy {
            applied_sequence,
            latest_sequence,
        } => serde_json::json!({
            "status": "healthy",
            "applied_sequence": applied_sequence,
            "latest_sequence": latest_sequence,
        }),
        copperdb_storage::WALIntegrityStatus::ChecksumCorrupt {
            applied_sequence,
            corrupted_sequence,
        } => serde_json::json!({
            "status": "checksum_corrupt",
            "applied_sequence": applied_sequence,
            "corrupted_sequence": corrupted_sequence,
        }),
        copperdb_storage::WALIntegrityStatus::Malformed { applied_sequence } => {
            serde_json::json!({
                "status": "malformed",
                "applied_sequence": applied_sequence,
            })
        }
    }
}

#[derive(Deserialize)]
struct MvccDebtQuery {
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct MvccScheduleRequest {
    interval: String,
}

fn mvcc_lifecycle_response(database: &str, engine: &GraphEngine) -> serde_json::Value {
    let status = engine.storage().lifecycle_status();
    serde_json::json!({
        "database": database,
        "enabled": status.enabled,
        "running": !status.paused,
        "paused": status.paused,
        "automatic": status.schedule_interval_ms > 0,
        "cycle_interval": format!("{}ms", status.schedule_interval_ms),
        "mvcc_active_snapshot_readers": status.active_reader_count,
        "mvcc_compaction_debt_keys": status.prune_debt,
        "mvcc_prunable_bytes_total": status.prune_debt,
        "mvcc_floor_lag_versions": status.head.saturating_sub(status.floor),
        "head": status.head,
        "floor": status.floor,
        "oldest_active_reader": status.oldest_active_reader,
        "retained_versions": status.retained_versions,
        "suggested_prune_floor": status.suggested_prune_floor,
    })
}

fn parse_mvcc_schedule_ms(interval: &str) -> Option<u64> {
    let interval = interval.trim();
    if let Some(value) = interval.strip_suffix("ms") {
        return value.trim().parse().ok();
    }
    if let Some(value) = interval.strip_suffix('s') {
        return value.trim().parse::<u64>().ok()?.checked_mul(1_000);
    }
    if let Some(value) = interval.strip_suffix('m') {
        return value.trim().parse::<u64>().ok()?.checked_mul(60_000);
    }
    interval.parse().ok()
}

async fn mvcc_lifecycle_status_handler(
    State(state): State<Arc<AppState>>,
    Path(database): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &database, false) {
        return status.into_response();
    }
    match open_engine(&state, &database) {
        Ok(engine) => Json(mvcc_lifecycle_response(&database, &engine)).into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn mvcc_lifecycle_debt_handler(
    State(state): State<Arc<AppState>>,
    Path(database): Path<String>,
    Query(query): Query<MvccDebtQuery>,
    headers: HeaderMap,
) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &database, false) {
        return status.into_response();
    }
    match open_engine(&state, &database) {
        Ok(engine) => Json(serde_json::json!({
            "database": database,
            "limit": query.limit.unwrap_or(20),
            "keys": engine.storage().top_lifecycle_debt_keys(query.limit.unwrap_or(20)),
        }))
        .into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn trigger_mvcc_prune_handler(
    State(state): State<Arc<AppState>>,
    Path(database): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &database, true) {
        return status.into_response();
    }
    match open_engine(&state, &database) {
        Ok(engine) => {
            engine.storage().trigger_prune_now(0);
            Json(serde_json::json!({"status": "ok", "database": database})).into_response()
        }
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn pause_mvcc_lifecycle_handler(
    State(state): State<Arc<AppState>>,
    Path(database): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &database, true) {
        return status.into_response();
    }
    match open_engine(&state, &database) {
        Ok(engine) => {
            engine.storage().pause_lifecycle();
            Json(serde_json::json!({"status": "ok", "database": database})).into_response()
        }
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn resume_mvcc_lifecycle_handler(
    State(state): State<Arc<AppState>>,
    Path(database): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &database, true) {
        return status.into_response();
    }
    match open_engine(&state, &database) {
        Ok(engine) => {
            engine.storage().resume_lifecycle();
            Json(serde_json::json!({"status": "ok", "database": database})).into_response()
        }
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn set_mvcc_lifecycle_schedule_handler(
    State(state): State<Arc<AppState>>,
    Path(database): Path<String>,
    headers: HeaderMap,
    Json(request): Json<MvccScheduleRequest>,
) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &database, true) {
        return status.into_response();
    }
    let Some(interval_ms) = parse_mvcc_schedule_ms(&request.interval) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid interval"})),
        )
            .into_response();
    };
    match open_engine(&state, &database) {
        Ok(engine) => {
            engine.storage().set_lifecycle_schedule_ms(interval_ms);
            Json(mvcc_lifecycle_response(&database, &engine)).into_response()
        }
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn inspect_wal_handler(
    State(state): State<Arc<AppState>>,
    Path(database): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &database, true) {
        return status.into_response();
    }
    let path = match offline_database_storage_path(&state, &database) {
        Ok(path) => path,
        Err(status) => return status.into_response(),
    };
    match StorageEngine::inspect_wal(path) {
        Ok(status) => Json(wal_integrity_response(status)).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": error.to_string()})),
        )
            .into_response(),
    }
}

async fn repair_wal_handler(
    State(state): State<Arc<AppState>>,
    Path(database): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &database, true) {
        return status.into_response();
    }
    let path = match offline_database_storage_path(&state, &database) {
        Ok(path) => path,
        Err(status) => return status.into_response(),
    };
    match StorageEngine::repair_wal_if_fully_applied(path) {
        Ok(removed_entries) => Json(serde_json::json!({
            "status": "repaired",
            "removed_entries": removed_entries,
        }))
        .into_response(),
        Err(copperdb_storage::StorageError::WalRepairWouldLoseUnappliedEntries {
            applied_sequence,
        }) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "repair would discard unapplied WAL entries",
                "applied_sequence": applied_sequence,
            })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": error.to_string()})),
        )
            .into_response(),
    }
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
    let Some(request_context) = request.extensions().get::<RequestContext>().cloned() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
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
    execute_fabric_ranked_search_admin_impl(
        state,
        tenant,
        database,
        request_context,
        request,
        caller_auth_token,
    )
    .await
}

async fn execute_fabric_ranked_search_admin_impl(
    state: Arc<AppState>,
    tenant: String,
    database: String,
    request_context: RequestContext,
    request: FabricRankedSearchRequest,
    caller_auth_token: Option<String>,
) -> Response {
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
    Extension(request_context): Extension<RequestContext>,
    headers: HeaderMap,
    request: GraphQLRequest,
) -> Response {
    if let Err(status) = authorize_database_access(&state, &headers, &state.db_name, false) {
        return status.into_response();
    }
    let gql_response = state
        .graphql_schema
        .execute_with_context(request_context, request.into_inner())
        .await;
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
    Extension(request_context): Extension<RequestContext>,
    headers: HeaderMap,
    body: Result<Json<serde_json::Value>, JsonRejection>,
) -> Response {
    if let Err(status) = validate_mcp_origin(&headers) {
        return status.into_response();
    }
    if !mcp_accepts_json(&headers) {
        return StatusCode::NOT_ACCEPTABLE.into_response();
    }
    let session_id = match mcp_session_id(&headers) {
        Ok(session_id) => session_id,
        Err(status) => return status.into_response(),
    };
    if session_id.is_some_and(|session_id| !state.mcp_sessions.validate(session_id)) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let Json(body) = match body {
        Ok(body) => body,
        Err(rejection)
            if matches!(
                rejection.status(),
                StatusCode::PAYLOAD_TOO_LARGE | StatusCode::UNSUPPORTED_MEDIA_TYPE
            ) =>
        {
            return rejection.into_response()
        }
        Err(rejection) => {
            return Json(copperdb_mcp::McpResponse::error_with_data(
                serde_json::Value::Null,
                -32700,
                "Parse error",
                Some(serde_json::json!({"detail": rejection.body_text()})),
            ))
            .into_response()
        }
    };
    if let serde_json::Value::Array(entries) = body {
        if entries.is_empty() || entries.len() > copperdb_mcp::DEFAULT_MAX_BATCH_ENTRIES {
            return Json(copperdb_mcp::McpResponse::error_with_data(
                serde_json::Value::Null,
                -32600,
                "Invalid Request",
                Some(serde_json::json!({
                    "batchEntries": entries.len(),
                    "maxBatchEntries": copperdb_mcp::DEFAULT_MAX_BATCH_ENTRIES
                })),
            ))
            .into_response();
        }
        let mut responses = Vec::with_capacity(entries.len());
        for entry in entries {
            let request: copperdb_mcp::McpRequest = match serde_json::from_value(entry) {
                Ok(request) => request,
                Err(error) => {
                    responses.push(copperdb_mcp::McpResponse::error_with_data(
                        serde_json::Value::Null,
                        -32600,
                        "Invalid Request",
                        Some(serde_json::json!({"detail": error.to_string()})),
                    ));
                    continue;
                }
            };
            let response_id = request
                .id
                .clone()
                .flatten()
                .unwrap_or(serde_json::Value::Null);
            match execute_mcp_request(
                Arc::clone(&state),
                request_context.clone(),
                &headers,
                request,
            )
            .await
            {
                Ok(Some(response)) => responses.push(response),
                Ok(None) => {}
                Err(status) => responses.push(mcp_authorization_error(response_id, status)),
            }
        }
        if responses.is_empty() {
            return StatusCode::ACCEPTED.into_response();
        }
        return Json(responses).into_response();
    }
    let request: copperdb_mcp::McpRequest = match serde_json::from_value(body) {
        Ok(request) => request,
        Err(error) => {
            return Json(copperdb_mcp::McpResponse::error_with_data(
                serde_json::Value::Null,
                -32600,
                "Invalid Request",
                Some(serde_json::json!({"detail": error.to_string()})),
            ))
            .into_response()
        }
    };
    let is_initialize = request.method == "initialize";
    if is_initialize && session_id.is_some() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    match execute_mcp_request(Arc::clone(&state), request_context, &headers, request).await {
        Ok(Some(response)) => {
            let create_session = is_initialize && response.error.is_none();
            let mut response = Json(response).into_response();
            if create_session {
                let session_id = state.mcp_sessions.create();
                response.headers_mut().insert(
                    MCP_SESSION_ID_HEADER,
                    HeaderValue::from_str(&session_id).expect("UUID session ID is a valid header"),
                );
            }
            response
        }
        Ok(None) => StatusCode::ACCEPTED.into_response(),
        Err(status) => status.into_response(),
    }
}

async fn mcp_delete_handler(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Err(status) = validate_mcp_origin(&headers) {
        return status.into_response();
    }
    let session_id = match mcp_session_id(&headers) {
        Ok(Some(session_id)) => session_id,
        Ok(None) | Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    if state.mcp_sessions.terminate(session_id) {
        StatusCode::NO_CONTENT.into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

fn validate_mcp_origin(headers: &HeaderMap) -> Result<(), StatusCode> {
    let origins = headers.get_all(header::ORIGIN);
    if origins.iter().next().is_none() {
        return Ok(());
    }
    if origins.iter().count() != 1 || headers.get_all(header::HOST).iter().count() != 1 {
        return Err(StatusCode::FORBIDDEN);
    }
    let origin = origins
        .iter()
        .next()
        .and_then(|value| value.to_str().ok())
        .ok_or(StatusCode::FORBIDDEN)?;
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .ok_or(StatusCode::FORBIDDEN)?;
    validate_http_origin(origin, host).map_err(|_| StatusCode::FORBIDDEN)
}

fn mcp_session_id(headers: &HeaderMap) -> Result<Option<&str>, StatusCode> {
    if headers.get_all(MCP_SESSION_ID_HEADER).iter().count() > 1 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let Some(value) = headers.get(MCP_SESSION_ID_HEADER) else {
        return Ok(None);
    };
    let session_id = value.to_str().map_err(|_| StatusCode::BAD_REQUEST)?;
    if session_id.is_empty()
        || session_id.len() > MAX_MCP_SESSION_ID_BYTES
        || !session_id.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(Some(session_id))
}

fn mcp_accepts_json(headers: &HeaderMap) -> bool {
    let Some(accept) = headers.get(header::ACCEPT) else {
        return true;
    };
    let Ok(accept) = accept.to_str() else {
        return false;
    };
    accept
        .split(',')
        .filter_map(|range| {
            let mut parts = range.split(';');
            let media_type = parts.next().unwrap_or_default().trim().to_ascii_lowercase();
            let specificity = match media_type.as_str() {
                "*/*" => 0,
                "application/*" => 1,
                "application/json" => 2,
                media_type
                    if media_type.starts_with("application/") && media_type.ends_with("+json") =>
                {
                    2
                }
                _ => return None,
            };
            let quality = parts
                .find_map(|parameter| {
                    let mut pair = parameter.trim().splitn(2, '=');
                    pair.next()
                        .is_some_and(|name| name.trim().eq_ignore_ascii_case("q"))
                        .then(|| {
                            pair.next()
                                .and_then(|value| value.trim().parse::<f32>().ok())
                                .filter(|quality| (0.0..=1.0).contains(quality))
                                .unwrap_or(0.0)
                        })
                })
                .unwrap_or(1.0);
            Some((specificity, quality))
        })
        .max_by_key(|(specificity, _)| *specificity)
        .is_some_and(|(_, quality)| quality > 0.0)
}

async fn execute_mcp_request(
    state: Arc<AppState>,
    request_context: RequestContext,
    headers: &HeaderMap,
    mut request: copperdb_mcp::McpRequest,
) -> Result<Option<copperdb_mcp::McpResponse>, StatusCode> {
    let is_notification = request.is_notification();
    let registry = copperdb_mcp::ToolRegistry::new();
    let access = match registry.required_access(&request) {
        Ok(access) => access,
        Err(_) => {
            if is_notification {
                return Ok(None);
            }
            return Ok(Some(
                registry
                    .dispatch_with_context(&request_context, &request)
                    .await,
            ));
        }
    };
    let registry = if let Some(access) = access {
        let database = request
            .take_database()
            .unwrap_or_else(|| state.db_name.clone());
        let claims =
            match authorize_database_access(&state, headers, &database, access.requires_write()) {
                Ok(claims) => claims,
                Err(_) if is_notification => return Ok(None),
                Err(status) => return Err(status),
            };
        if access.requires_admin() {
            if let Some(claims) = claims.as_ref() {
                if let Err(status) = ensure_admin_access(&state, claims) {
                    if is_notification {
                        return Ok(None);
                    }
                    return Err(status);
                }
            }
        }
        let engine = match open_engine(&state, &database) {
            Ok(engine) => engine,
            Err(error) => {
                if is_notification {
                    return Ok(None);
                }
                return Ok(Some(copperdb_mcp::McpResponse::error(
                    request.id.flatten().unwrap_or(serde_json::Value::Null),
                    -32000,
                    error,
                )));
            }
        };
        copperdb_mcp::ToolRegistry::with_engine_and_roles(engine, roles_for_claims(claims.as_ref()))
    } else {
        registry
    };
    let response = registry
        .dispatch_with_context(&request_context, &request)
        .await;
    if is_notification {
        Ok(None)
    } else {
        Ok(Some(response))
    }
}

fn mcp_authorization_error(id: serde_json::Value, status: StatusCode) -> copperdb_mcp::McpResponse {
    let (code, message) = match status {
        StatusCode::UNAUTHORIZED => (-32001, "Unauthorized"),
        StatusCode::FORBIDDEN => (-32003, "Forbidden"),
        _ => (-32603, "Internal error"),
    };
    copperdb_mcp::McpResponse::error_with_data(
        id,
        code,
        message,
        Some(serde_json::json!({"httpStatus": status.as_u16()})),
    )
}

#[cfg(test)]
mod tests;
