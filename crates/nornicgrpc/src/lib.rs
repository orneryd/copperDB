//! gRPC server interface for copperdb.
//!
//! Equivalent to Go's `pkg/nornicgrpc` in NornicDB.
//! Exposes a Protobuf/gRPC API as an alternative to the Bolt protocol.
//! Uses `tonic` (Rust gRPC) + `prost` (Protobuf codegen).

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Response, Status};

use copperdb_replication::{Command, ReplicaTransport, ReplicationError};
use copperdb_search::{
    HydrationTransport, RankedSearchTransport, RrfHydrationRecord, RrfSearchBatch, SearchError,
    SearchQuery,
};
use copperdb_storage::{EdgeRecord, KnowledgePolicyAccessMetadata};
use copperdb_topology::{
    ConsistencyLevel, DistributedReadPlan, DistributedSearchPlan, DistributedWriteMode,
    DistributedWritePlan, FabricGlobalId, LogicalTransactionId, PlacementKey,
};
use serde_json::Value;

pub mod proto {
    tonic::include_proto!("copperdb.nornic.v1");
}

const GRPC_AUTH_HEADER: &str = "authorization";
const GRPC_CALLER_AUTH_HEADER: &str = "x-copperdb-caller-authorization";

pub trait GrpcAuthValidator: Send + Sync {
    fn validate(&self, token: &str) -> Result<(), GrpcError>;
}

fn ensure_tls_crypto_provider() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn bearer_value(token: &str) -> Result<MetadataValue<Ascii>, GrpcError> {
    MetadataValue::try_from(format!("Bearer {token}"))
        .map_err(|error| GrpcError::Encoding(error.to_string()))
}

fn authorize_request<T>(
    request: &Request<T>,
    auth_validator: Option<&Arc<dyn GrpcAuthValidator>>,
) -> Result<(), Status> {
    if auth_validator.is_none() {
        return Ok(());
    }
    let Some(value) = request.metadata().get(GRPC_AUTH_HEADER) else {
        return Err(Status::unauthenticated("missing gRPC authorization token"));
    };
    let value = value
        .to_str()
        .map_err(|_| Status::unauthenticated("invalid gRPC authorization metadata"))?;
    let Some(actual) = value.strip_prefix("Bearer ") else {
        return Err(Status::unauthenticated("invalid gRPC authorization scheme"));
    };
    auth_validator
        .expect("auth validator presence already checked")
        .validate(actual)
        .map_err(status_from_grpc_error)
}

fn request_with_auth<T>(message: T, auth_token: Option<&str>) -> Result<Request<T>, GrpcError> {
    let mut request = Request::new(message);
    if let Some(token) = auth_token {
        request
            .metadata_mut()
            .insert(GRPC_AUTH_HEADER, bearer_value(token)?);
    }
    Ok(request)
}

fn caller_auth_token_from_metadata<T>(request: &Request<T>) -> Result<Option<String>, Status> {
    let Some(value) = request.metadata().get(GRPC_CALLER_AUTH_HEADER) else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| Status::unauthenticated("invalid forwarded caller authorization metadata"))?;
    let Some(token) = value.strip_prefix("Bearer ") else {
        return Err(Status::unauthenticated(
            "invalid forwarded caller authorization scheme",
        ));
    };
    let token = token.trim();
    if token.is_empty() {
        return Err(Status::unauthenticated(
            "empty forwarded caller authorization token",
        ));
    }
    Ok(Some(token.to_string()))
}

fn request_with_auth_headers<T>(
    message: T,
    caller_auth_token: Option<&str>,
) -> Result<Request<T>, GrpcError> {
    let mut request = Request::new(message);
    if let Some(token) = caller_auth_token {
        request
            .metadata_mut()
            .insert(GRPC_CALLER_AUTH_HEADER, bearer_value(token)?);
    }
    Ok(request)
}

fn encode_read_fence(read_fence: Option<LogicalTransactionId>) -> String {
    read_fence
        .map(|value| value.stable_id())
        .unwrap_or_default()
}

fn decode_read_fence(read_fence: &str) -> Result<Option<LogicalTransactionId>, GrpcError> {
    let trimmed = read_fence.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let mut parts = trimmed.split(':');
    let Some(epoch_hex) = parts.next() else {
        return Err(GrpcError::Encoding(format!("invalid read fence {trimmed}")));
    };
    let Some(counter_hex) = parts.next() else {
        return Err(GrpcError::Encoding(format!("invalid read fence {trimmed}")));
    };
    let Some(node_hex) = parts.next() else {
        return Err(GrpcError::Encoding(format!("invalid read fence {trimmed}")));
    };
    if parts.next().is_some() {
        return Err(GrpcError::Encoding(format!("invalid read fence {trimmed}")));
    }

    let epoch = u64::from_str_radix(epoch_hex, 16)
        .map_err(|_| GrpcError::Encoding(format!("invalid read fence {trimmed}")))?;
    let counter = u64::from_str_radix(counter_hex, 16)
        .map_err(|_| GrpcError::Encoding(format!("invalid read fence {trimmed}")))?;
    let node_ordinal = u32::from_str_radix(node_hex, 16)
        .map_err(|_| GrpcError::Encoding(format!("invalid read fence {trimmed}")))?;

    Ok(Some(LogicalTransactionId::new(
        epoch,
        counter,
        node_ordinal,
    )))
}

fn status_from_grpc_error(error: GrpcError) -> Status {
    match error {
        GrpcError::Unauthenticated(message) => Status::unauthenticated(message),
        GrpcError::PermissionDenied(message) => Status::permission_denied(message),
        other => Status::internal(other.to_string()),
    }
}

#[derive(Debug, Error)]
pub enum GrpcError {
    #[error("gRPC transport error: {0}")]
    Transport(String),
    #[error("proto encoding error: {0}")]
    Encoding(String),
    #[error("gRPC unauthenticated: {0}")]
    Unauthenticated(String),
    #[error("gRPC permission denied: {0}")]
    PermissionDenied(String),
    #[error("server not started")]
    NotStarted,
}

/// gRPC server configuration.
#[derive(Debug, Clone)]
pub struct GrpcConfig {
    pub listen_addr: String,
    pub max_connections: usize,
    pub max_message_size_bytes: usize,
    pub tls_enabled: bool,
}

impl GrpcConfig {
    pub fn new(listen_addr: impl Into<String>) -> Self {
        Self {
            listen_addr: listen_addr.into(),
            max_connections: 1000,
            max_message_size_bytes: 4 * 1024 * 1024, // 4 MiB
            tls_enabled: false,
        }
    }
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self::new("[::1]:50051")
    }
}

/// gRPC server handle.
pub struct GrpcServer {
    config: GrpcConfig,
}

impl GrpcServer {
    pub fn new(config: GrpcConfig) -> Self {
        Self { config }
    }

    pub fn listen_addr(&self) -> &str {
        &self.config.listen_addr
    }

    pub fn max_connections(&self) -> usize {
        self.config.max_connections
    }

    pub fn max_message_size(&self) -> usize {
        self.config.max_message_size_bytes
    }

    pub fn tls_enabled(&self) -> bool {
        self.config.tls_enabled
    }
}

/// A Cypher query request over gRPC.
#[derive(Debug, Clone)]
pub struct GrpcCypherRequest {
    pub query: String,
    pub database: String,
    pub parameters: std::collections::HashMap<String, serde_json::Value>,
}

/// A single row of results from a Cypher query.
#[derive(Debug, Clone)]
pub struct GrpcCypherRow {
    pub values: Vec<serde_json::Value>,
}

/// A Cypher query response over gRPC.
#[derive(Debug, Clone)]
pub struct GrpcCypherResponse {
    pub columns: Vec<String>,
    pub rows: Vec<GrpcCypherRow>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RemoteExecutionKind {
    Write,
    Read,
    Search,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RemoteExecutionEnvelope {
    pub request_id: String,
    pub target_node: String,
    pub target_addr: String,
    pub kind: RemoteExecutionKind,
    pub placement: PlacementKey,
    pub consistency: Option<ConsistencyLevel>,
    pub write_mode: Option<DistributedWriteMode>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RemoteReplicaApplyRequest {
    pub target_node: String,
    pub target_addr: String,
    pub command: Command,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RemoteReplicaReadRequest {
    pub target_node: String,
    pub target_addr: String,
    pub key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RemoteGraphNodeRequest {
    pub target_node: String,
    pub target_addr: String,
    pub database: String,
    pub node_id: String,
    pub read_fence: Option<LogicalTransactionId>,
    pub caller_auth_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RemoteGraphEdgesRequest {
    pub target_node: String,
    pub target_addr: String,
    pub database: String,
    pub node_id: String,
    pub rel_type: Option<String>,
    pub read_fence: Option<LogicalTransactionId>,
    pub caller_auth_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RemoteGraphNodesByLabelRequest {
    pub target_node: String,
    pub target_addr: String,
    pub database: String,
    pub label: String,
    pub read_fence: Option<LogicalTransactionId>,
    pub caller_auth_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RemoteGraphNodesByPropertyRequest {
    pub target_node: String,
    pub target_addr: String,
    pub database: String,
    pub label: String,
    pub property: String,
    pub value: Value,
    pub read_fence: Option<LogicalTransactionId>,
    pub caller_auth_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RemoteGraphAccessMetadataRequest {
    pub target_node: String,
    pub target_addr: String,
    pub database: String,
    pub entity_id: String,
    pub read_fence: Option<LogicalTransactionId>,
    pub caller_auth_token: Option<String>,
}

#[async_trait]
pub trait RemoteReplicaClient: Send + Sync {
    async fn apply_replica(&self, request: RemoteReplicaApplyRequest) -> Result<(), GrpcError>;
    async fn read_replica(
        &self,
        request: RemoteReplicaReadRequest,
    ) -> Result<Option<Vec<u8>>, GrpcError>;

    async fn graph_node(
        &self,
        _request: RemoteGraphNodeRequest,
    ) -> Result<Option<Vec<u8>>, GrpcError> {
        Err(GrpcError::Transport(
            "graph-node RPC handler is not configured".into(),
        ))
    }

    async fn graph_edges_from_node(
        &self,
        _request: RemoteGraphEdgesRequest,
    ) -> Result<Vec<EdgeRecord>, GrpcError> {
        Err(GrpcError::Transport(
            "graph-edges-from-node RPC handler is not configured".into(),
        ))
    }

    async fn graph_edges_to_node(
        &self,
        _request: RemoteGraphEdgesRequest,
    ) -> Result<Vec<EdgeRecord>, GrpcError> {
        Err(GrpcError::Transport(
            "graph-edges-to-node RPC handler is not configured".into(),
        ))
    }

    async fn graph_nodes_by_label(
        &self,
        _request: RemoteGraphNodesByLabelRequest,
    ) -> Result<Vec<Vec<u8>>, GrpcError> {
        Err(GrpcError::Transport(
            "graph-nodes-by-label RPC handler is not configured".into(),
        ))
    }

    async fn graph_nodes_by_property(
        &self,
        _request: RemoteGraphNodesByPropertyRequest,
    ) -> Result<Vec<Vec<u8>>, GrpcError> {
        Err(GrpcError::Transport(
            "graph-nodes-by-property RPC handler is not configured".into(),
        ))
    }

    async fn graph_access_metadata(
        &self,
        _request: RemoteGraphAccessMetadataRequest,
    ) -> Result<Option<KnowledgePolicyAccessMetadata>, GrpcError> {
        Err(GrpcError::Transport(
            "graph-access-metadata RPC handler is not configured".into(),
        ))
    }
}

pub struct NornicGrpcReplicaTransport {
    endpoints: HashMap<String, String>,
    database: Option<String>,
    client: Arc<dyn RemoteReplicaClient>,
}

#[derive(Debug, Clone)]
pub struct RemoteRankedSearchRequest {
    pub target_node: String,
    pub target_addr: String,
    pub placement: PlacementKey,
    pub query: SearchQuery,
    pub read_fence: Option<LogicalTransactionId>,
    pub caller_auth_token: Option<String>,
}

#[async_trait]
pub trait RemoteRankedSearchClient: Send + Sync {
    async fn search_ranked(
        &self,
        request: RemoteRankedSearchRequest,
    ) -> Result<RrfSearchBatch, GrpcError>;
}

pub struct NornicGrpcRankedSearchTransport {
    endpoints: HashMap<String, String>,
    client: Arc<dyn RemoteRankedSearchClient>,
}

#[derive(Debug, Clone)]
pub struct RemoteHydrationRequest {
    pub target_node: String,
    pub target_addr: String,
    pub placement: PlacementKey,
    pub global_ids: Vec<FabricGlobalId>,
    pub read_fence: Option<LogicalTransactionId>,
    pub caller_auth_token: Option<String>,
}

#[async_trait]
pub trait RemoteHydrationClient: Send + Sync {
    async fn hydrate_entities(
        &self,
        request: RemoteHydrationRequest,
    ) -> Result<Vec<RrfHydrationRecord>, GrpcError>;
}

pub struct NornicGrpcHydrationTransport {
    endpoints: HashMap<String, String>,
    client: Arc<dyn RemoteHydrationClient>,
}

#[derive(Clone)]
pub struct NornicReplicaService {
    handler: Arc<dyn RemoteReplicaClient>,
    ranked_search_handler: Arc<dyn RemoteRankedSearchClient>,
    hydration_handler: Arc<dyn RemoteHydrationClient>,
    auth_validator: Option<Arc<dyn GrpcAuthValidator>>,
}

#[derive(Debug, Clone, Default)]
struct UnsupportedRemoteRankedSearchClient;

#[derive(Debug, Clone, Default)]
struct UnsupportedRemoteHydrationClient;

#[async_trait]
impl RemoteRankedSearchClient for UnsupportedRemoteRankedSearchClient {
    async fn search_ranked(
        &self,
        _request: RemoteRankedSearchRequest,
    ) -> Result<RrfSearchBatch, GrpcError> {
        Err(GrpcError::Transport(
            "ranked search RPC handler is not configured".into(),
        ))
    }
}

#[async_trait]
impl RemoteHydrationClient for UnsupportedRemoteHydrationClient {
    async fn hydrate_entities(
        &self,
        _request: RemoteHydrationRequest,
    ) -> Result<Vec<RrfHydrationRecord>, GrpcError> {
        Err(GrpcError::Transport(
            "hydration RPC handler is not configured".into(),
        ))
    }
}

impl NornicReplicaService {
    pub fn new(handler: Arc<dyn RemoteReplicaClient>) -> Self {
        Self {
            handler,
            ranked_search_handler: Arc::new(UnsupportedRemoteRankedSearchClient),
            hydration_handler: Arc::new(UnsupportedRemoteHydrationClient),
            auth_validator: None,
        }
    }

    pub fn with_auth_validator(mut self, validator: Arc<dyn GrpcAuthValidator>) -> Self {
        self.auth_validator = Some(validator);
        self
    }

    pub fn with_ranked_search_handler(
        mut self,
        handler: Arc<dyn RemoteRankedSearchClient>,
    ) -> Self {
        self.ranked_search_handler = handler;
        self
    }

    pub fn with_hydration_handler(mut self, handler: Arc<dyn RemoteHydrationClient>) -> Self {
        self.hydration_handler = handler;
        self
    }

    pub fn into_server(self) -> proto::nornic_replica_server::NornicReplicaServer<Self> {
        proto::nornic_replica_server::NornicReplicaServer::new(self)
    }
}

#[async_trait]
impl proto::nornic_replica_server::NornicReplica for NornicReplicaService {
    async fn apply_replica(
        &self,
        request: Request<proto::RemoteReplicaApplyRequest>,
    ) -> Result<Response<proto::RemoteReplicaApplyResponse>, Status> {
        authorize_request(&request, self.auth_validator.as_ref())?;
        let request = RemoteReplicaApplyRequest::try_from(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.handler
            .apply_replica(request)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(proto::RemoteReplicaApplyResponse {}))
    }

    async fn read_replica(
        &self,
        request: Request<proto::RemoteReplicaReadRequest>,
    ) -> Result<Response<proto::RemoteReplicaReadResponse>, Status> {
        authorize_request(&request, self.auth_validator.as_ref())?;
        let response = self
            .handler
            .read_replica(RemoteReplicaReadRequest::from(request.into_inner()))
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(proto::RemoteReplicaReadResponse::from(
            response,
        )))
    }

    async fn graph_node(
        &self,
        request: Request<proto::RemoteGraphNodeRequest>,
    ) -> Result<Response<proto::RemoteGraphNodeResponse>, Status> {
        let caller_auth_token = caller_auth_token_from_metadata(&request)?;
        let request = RemoteGraphNodeRequest {
            caller_auth_token,
            ..RemoteGraphNodeRequest::from(request.into_inner())
        };
        let response = self
            .handler
            .graph_node(request)
            .await
            .map_err(status_from_grpc_error)?;
        Ok(Response::new(proto::RemoteGraphNodeResponse::from(
            response,
        )))
    }

    async fn graph_edges_from_node(
        &self,
        request: Request<proto::RemoteGraphEdgesRequest>,
    ) -> Result<Response<proto::RemoteGraphEdgesResponse>, Status> {
        let caller_auth_token = caller_auth_token_from_metadata(&request)?;
        let request = RemoteGraphEdgesRequest {
            caller_auth_token,
            ..RemoteGraphEdgesRequest::from(request.into_inner())
        };
        let response = self
            .handler
            .graph_edges_from_node(request)
            .await
            .map_err(status_from_grpc_error)?;
        let response = proto::RemoteGraphEdgesResponse::try_from(response)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(response))
    }

    async fn graph_edges_to_node(
        &self,
        request: Request<proto::RemoteGraphEdgesRequest>,
    ) -> Result<Response<proto::RemoteGraphEdgesResponse>, Status> {
        let caller_auth_token = caller_auth_token_from_metadata(&request)?;
        let request = RemoteGraphEdgesRequest {
            caller_auth_token,
            ..RemoteGraphEdgesRequest::from(request.into_inner())
        };
        let response = self
            .handler
            .graph_edges_to_node(request)
            .await
            .map_err(status_from_grpc_error)?;
        let response = proto::RemoteGraphEdgesResponse::try_from(response)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(response))
    }

    async fn graph_nodes_by_label(
        &self,
        request: Request<proto::RemoteGraphNodesByLabelRequest>,
    ) -> Result<Response<proto::RemoteGraphNodesResponse>, Status> {
        let caller_auth_token = caller_auth_token_from_metadata(&request)?;
        let request = RemoteGraphNodesByLabelRequest {
            caller_auth_token,
            ..RemoteGraphNodesByLabelRequest::from(request.into_inner())
        };
        let response = self
            .handler
            .graph_nodes_by_label(request)
            .await
            .map_err(status_from_grpc_error)?;
        Ok(Response::new(proto::RemoteGraphNodesResponse::from(
            response,
        )))
    }

    async fn graph_nodes_by_property(
        &self,
        request: Request<proto::RemoteGraphNodesByPropertyRequest>,
    ) -> Result<Response<proto::RemoteGraphNodesResponse>, Status> {
        let caller_auth_token = caller_auth_token_from_metadata(&request)?;
        let request = RemoteGraphNodesByPropertyRequest {
            caller_auth_token,
            ..RemoteGraphNodesByPropertyRequest::try_from(request.into_inner())
                .map_err(|error| Status::invalid_argument(error.to_string()))?
        };
        let response = self
            .handler
            .graph_nodes_by_property(request)
            .await
            .map_err(status_from_grpc_error)?;
        Ok(Response::new(proto::RemoteGraphNodesResponse::from(
            response,
        )))
    }

    async fn graph_access_metadata(
        &self,
        request: Request<proto::RemoteGraphAccessMetadataRequest>,
    ) -> Result<Response<proto::RemoteGraphAccessMetadataResponse>, Status> {
        let caller_auth_token = caller_auth_token_from_metadata(&request)?;
        let request = RemoteGraphAccessMetadataRequest {
            caller_auth_token,
            ..RemoteGraphAccessMetadataRequest::from(request.into_inner())
        };
        let response = self
            .handler
            .graph_access_metadata(request)
            .await
            .map_err(status_from_grpc_error)?;
        let response = proto::RemoteGraphAccessMetadataResponse::try_from(response)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(response))
    }

    async fn search_ranked(
        &self,
        request: Request<proto::RemoteRankedSearchRequest>,
    ) -> Result<Response<proto::RemoteRankedSearchResponse>, Status> {
        let caller_auth_token = caller_auth_token_from_metadata(&request)?;
        let request = RemoteRankedSearchRequest::try_from(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let request = RemoteRankedSearchRequest {
            caller_auth_token,
            ..request
        };
        let response = self
            .ranked_search_handler
            .search_ranked(request)
            .await
            .map_err(status_from_grpc_error)?;
        let response = proto::RemoteRankedSearchResponse::try_from(response)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(response))
    }

    async fn hydrate_entities(
        &self,
        request: Request<proto::RemoteHydrationRequest>,
    ) -> Result<Response<proto::RemoteHydrationResponse>, Status> {
        let caller_auth_token = caller_auth_token_from_metadata(&request)?;
        let request = RemoteHydrationRequest::try_from(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let request = RemoteHydrationRequest {
            caller_auth_token,
            ..request
        };
        let response = self
            .hydration_handler
            .hydrate_entities(request)
            .await
            .map_err(status_from_grpc_error)?;
        let response = proto::RemoteHydrationResponse::try_from(response)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(response))
    }
}

#[derive(Debug, Clone)]
pub struct TonicRemoteReplicaClient {
    auth_token: Option<String>,
    caller_auth_token: Option<String>,
    tls_enabled: bool,
    tls_ca_certificate_pem: Option<String>,
    tls_domain_name: Option<String>,
    tls_identity_pem: Option<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct TonicRemoteRankedSearchClient {
    caller_auth_token: Option<String>,
    tls_enabled: bool,
    tls_ca_certificate_pem: Option<String>,
    tls_domain_name: Option<String>,
    tls_identity_pem: Option<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct TonicRemoteHydrationClient {
    caller_auth_token: Option<String>,
    tls_enabled: bool,
    tls_ca_certificate_pem: Option<String>,
    tls_domain_name: Option<String>,
    tls_identity_pem: Option<(String, String)>,
}

impl TonicRemoteReplicaClient {
    pub fn new() -> Self {
        Self {
            auth_token: None,
            caller_auth_token: None,
            tls_enabled: false,
            tls_ca_certificate_pem: None,
            tls_domain_name: None,
            tls_identity_pem: None,
        }
    }

    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    pub fn with_caller_auth_token(mut self, token: impl Into<String>) -> Self {
        self.caller_auth_token = Some(token.into());
        self
    }

    pub fn with_tls_enabled(mut self, enabled: bool) -> Self {
        self.tls_enabled = enabled;
        self
    }

    pub fn with_tls_ca_certificate_pem(mut self, pem: impl Into<String>) -> Self {
        self.tls_ca_certificate_pem = Some(pem.into());
        self
    }

    pub fn with_tls_domain_name(mut self, domain_name: impl Into<String>) -> Self {
        self.tls_domain_name = Some(domain_name.into());
        self
    }

    pub fn with_tls_identity_pem(
        mut self,
        certificate_pem: impl Into<String>,
        key_pem: impl Into<String>,
    ) -> Self {
        self.tls_identity_pem = Some((certificate_pem.into(), key_pem.into()));
        self
    }

    fn endpoint_uri(target_addr: &str, tls_enabled: bool) -> String {
        if target_addr.starts_with("http://") || target_addr.starts_with("https://") {
            target_addr.into()
        } else if tls_enabled {
            format!("https://{target_addr}")
        } else {
            format!("http://{target_addr}")
        }
    }

    async fn connect(
        &self,
        target_addr: &str,
    ) -> Result<proto::nornic_replica_client::NornicReplicaClient<Channel>, GrpcError> {
        let uri = Self::endpoint_uri(target_addr, self.tls_enabled);
        let mut endpoint = Endpoint::from_shared(uri.clone())
            .map_err(|error| GrpcError::Transport(error.to_string()))?;
        if self.tls_enabled || uri.starts_with("https://") {
            ensure_tls_crypto_provider();
            let mut tls = ClientTlsConfig::new();
            if let Some(pem) = &self.tls_ca_certificate_pem {
                tls = tls.ca_certificate(Certificate::from_pem(pem.clone()));
            }
            if let Some(domain_name) = &self.tls_domain_name {
                tls = tls.domain_name(domain_name.clone());
            }
            if let Some((cert_pem, key_pem)) = &self.tls_identity_pem {
                tls = tls.identity(tonic::transport::Identity::from_pem(
                    cert_pem.clone(),
                    key_pem.clone(),
                ));
            }
            endpoint = endpoint
                .tls_config(tls)
                .map_err(|error| GrpcError::Transport(error.to_string()))?;
        }
        let channel = endpoint
            .connect()
            .await
            .map_err(|error| GrpcError::Transport(error.to_string()))?;
        Ok(proto::nornic_replica_client::NornicReplicaClient::new(
            channel,
        ))
    }
}

impl TonicRemoteRankedSearchClient {
    pub fn new() -> Self {
        Self {
            caller_auth_token: None,
            tls_enabled: false,
            tls_ca_certificate_pem: None,
            tls_domain_name: None,
            tls_identity_pem: None,
        }
    }

    pub fn with_caller_auth_token(mut self, token: impl Into<String>) -> Self {
        self.caller_auth_token = Some(token.into());
        self
    }

    pub fn with_tls_enabled(mut self, enabled: bool) -> Self {
        self.tls_enabled = enabled;
        self
    }

    pub fn with_tls_ca_certificate_pem(mut self, pem: impl Into<String>) -> Self {
        self.tls_ca_certificate_pem = Some(pem.into());
        self
    }

    pub fn with_tls_domain_name(mut self, domain_name: impl Into<String>) -> Self {
        self.tls_domain_name = Some(domain_name.into());
        self
    }

    pub fn with_tls_identity_pem(
        mut self,
        certificate_pem: impl Into<String>,
        key_pem: impl Into<String>,
    ) -> Self {
        self.tls_identity_pem = Some((certificate_pem.into(), key_pem.into()));
        self
    }
}

impl TonicRemoteHydrationClient {
    pub fn new() -> Self {
        Self {
            caller_auth_token: None,
            tls_enabled: false,
            tls_ca_certificate_pem: None,
            tls_domain_name: None,
            tls_identity_pem: None,
        }
    }

    pub fn with_caller_auth_token(mut self, token: impl Into<String>) -> Self {
        self.caller_auth_token = Some(token.into());
        self
    }

    pub fn with_tls_enabled(mut self, enabled: bool) -> Self {
        self.tls_enabled = enabled;
        self
    }

    pub fn with_tls_ca_certificate_pem(mut self, pem: impl Into<String>) -> Self {
        self.tls_ca_certificate_pem = Some(pem.into());
        self
    }

    pub fn with_tls_domain_name(mut self, domain_name: impl Into<String>) -> Self {
        self.tls_domain_name = Some(domain_name.into());
        self
    }

    pub fn with_tls_identity_pem(
        mut self,
        certificate_pem: impl Into<String>,
        key_pem: impl Into<String>,
    ) -> Self {
        self.tls_identity_pem = Some((certificate_pem.into(), key_pem.into()));
        self
    }
}

impl Default for TonicRemoteReplicaClient {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for TonicRemoteRankedSearchClient {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for TonicRemoteHydrationClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RemoteReplicaClient for TonicRemoteReplicaClient {
    async fn apply_replica(&self, request: RemoteReplicaApplyRequest) -> Result<(), GrpcError> {
        let target_addr = request.target_addr.clone();
        let proto_request = request_with_auth(
            proto::RemoteReplicaApplyRequest::try_from(request)?,
            self.auth_token.as_deref(),
        )?;
        self.connect(&target_addr)
            .await?
            .apply_replica(proto_request)
            .await
            .map_err(|error| GrpcError::Transport(error.to_string()))?;
        Ok(())
    }

    async fn read_replica(
        &self,
        request: RemoteReplicaReadRequest,
    ) -> Result<Option<Vec<u8>>, GrpcError> {
        let target_addr = request.target_addr.clone();
        let response = self
            .connect(&target_addr)
            .await?
            .read_replica(request_with_auth(
                proto::RemoteReplicaReadRequest::from(request),
                self.auth_token.as_deref(),
            )?)
            .await
            .map_err(|error| GrpcError::Transport(error.to_string()))?
            .into_inner();
        Ok(Option::<Vec<u8>>::from(response))
    }

    async fn graph_node(
        &self,
        request: RemoteGraphNodeRequest,
    ) -> Result<Option<Vec<u8>>, GrpcError> {
        let target_addr = request.target_addr.clone();
        let caller_auth_token = request
            .caller_auth_token
            .clone()
            .or_else(|| self.caller_auth_token.clone());
        let response = self
            .connect(&target_addr)
            .await?
            .graph_node(request_with_auth_headers(
                proto::RemoteGraphNodeRequest::from(request),
                caller_auth_token.as_deref(),
            )?)
            .await
            .map_err(|error| GrpcError::Transport(error.to_string()))?
            .into_inner();
        Ok(Option::<Vec<u8>>::from(response))
    }

    async fn graph_edges_from_node(
        &self,
        request: RemoteGraphEdgesRequest,
    ) -> Result<Vec<EdgeRecord>, GrpcError> {
        let target_addr = request.target_addr.clone();
        let caller_auth_token = request
            .caller_auth_token
            .clone()
            .or_else(|| self.caller_auth_token.clone());
        let response = self
            .connect(&target_addr)
            .await?
            .graph_edges_from_node(request_with_auth_headers(
                proto::RemoteGraphEdgesRequest::from(request),
                caller_auth_token.as_deref(),
            )?)
            .await
            .map_err(|error| GrpcError::Transport(error.to_string()))?
            .into_inner();
        Vec::<EdgeRecord>::try_from(response)
    }

    async fn graph_edges_to_node(
        &self,
        request: RemoteGraphEdgesRequest,
    ) -> Result<Vec<EdgeRecord>, GrpcError> {
        let target_addr = request.target_addr.clone();
        let caller_auth_token = request
            .caller_auth_token
            .clone()
            .or_else(|| self.caller_auth_token.clone());
        let response = self
            .connect(&target_addr)
            .await?
            .graph_edges_to_node(request_with_auth_headers(
                proto::RemoteGraphEdgesRequest::from(request),
                caller_auth_token.as_deref(),
            )?)
            .await
            .map_err(|error| GrpcError::Transport(error.to_string()))?
            .into_inner();
        Vec::<EdgeRecord>::try_from(response)
    }

    async fn graph_nodes_by_label(
        &self,
        request: RemoteGraphNodesByLabelRequest,
    ) -> Result<Vec<Vec<u8>>, GrpcError> {
        let target_addr = request.target_addr.clone();
        let caller_auth_token = request
            .caller_auth_token
            .clone()
            .or_else(|| self.caller_auth_token.clone());
        let response = self
            .connect(&target_addr)
            .await?
            .graph_nodes_by_label(request_with_auth_headers(
                proto::RemoteGraphNodesByLabelRequest::from(request),
                caller_auth_token.as_deref(),
            )?)
            .await
            .map_err(|error| GrpcError::Transport(error.to_string()))?
            .into_inner();
        Ok(Vec::<Vec<u8>>::from(response))
    }

    async fn graph_nodes_by_property(
        &self,
        request: RemoteGraphNodesByPropertyRequest,
    ) -> Result<Vec<Vec<u8>>, GrpcError> {
        let target_addr = request.target_addr.clone();
        let caller_auth_token = request
            .caller_auth_token
            .clone()
            .or_else(|| self.caller_auth_token.clone());
        let response = self
            .connect(&target_addr)
            .await?
            .graph_nodes_by_property(request_with_auth_headers(
                proto::RemoteGraphNodesByPropertyRequest::try_from(request)?,
                caller_auth_token.as_deref(),
            )?)
            .await
            .map_err(|error| GrpcError::Transport(error.to_string()))?
            .into_inner();
        Ok(Vec::<Vec<u8>>::from(response))
    }

    async fn graph_access_metadata(
        &self,
        request: RemoteGraphAccessMetadataRequest,
    ) -> Result<Option<KnowledgePolicyAccessMetadata>, GrpcError> {
        let target_addr = request.target_addr.clone();
        let caller_auth_token = request
            .caller_auth_token
            .clone()
            .or_else(|| self.caller_auth_token.clone());
        let response = self
            .connect(&target_addr)
            .await?
            .graph_access_metadata(request_with_auth_headers(
                proto::RemoteGraphAccessMetadataRequest::from(request),
                caller_auth_token.as_deref(),
            )?)
            .await
            .map_err(|error| GrpcError::Transport(error.to_string()))?
            .into_inner();
        Option::<KnowledgePolicyAccessMetadata>::try_from(response)
    }
}

#[async_trait]
impl RemoteRankedSearchClient for TonicRemoteRankedSearchClient {
    async fn search_ranked(
        &self,
        request: RemoteRankedSearchRequest,
    ) -> Result<RrfSearchBatch, GrpcError> {
        let target_addr = request.target_addr.clone();
        let response = TonicRemoteReplicaClient {
            auth_token: None,
            caller_auth_token: None,
            tls_enabled: self.tls_enabled,
            tls_ca_certificate_pem: self.tls_ca_certificate_pem.clone(),
            tls_domain_name: self.tls_domain_name.clone(),
            tls_identity_pem: self.tls_identity_pem.clone(),
        }
        .connect(&target_addr)
        .await?
        .search_ranked(request_with_auth_headers(
            proto::RemoteRankedSearchRequest::try_from(request)?,
            self.caller_auth_token.as_deref(),
        )?)
        .await
        .map_err(|error| GrpcError::Transport(error.to_string()))?
        .into_inner();
        RrfSearchBatch::try_from(response)
    }
}

#[async_trait]
impl RemoteHydrationClient for TonicRemoteHydrationClient {
    async fn hydrate_entities(
        &self,
        request: RemoteHydrationRequest,
    ) -> Result<Vec<RrfHydrationRecord>, GrpcError> {
        let target_addr = request.target_addr.clone();
        let response = TonicRemoteReplicaClient {
            auth_token: None,
            caller_auth_token: None,
            tls_enabled: self.tls_enabled,
            tls_ca_certificate_pem: self.tls_ca_certificate_pem.clone(),
            tls_domain_name: self.tls_domain_name.clone(),
            tls_identity_pem: self.tls_identity_pem.clone(),
        }
        .connect(&target_addr)
        .await?
        .hydrate_entities(request_with_auth_headers(
            proto::RemoteHydrationRequest::try_from(request)?,
            self.caller_auth_token.as_deref(),
        )?)
        .await
        .map_err(|error| GrpcError::Transport(error.to_string()))?
        .into_inner();
        Vec::<RrfHydrationRecord>::try_from(response)
    }
}

impl NornicGrpcReplicaTransport {
    pub fn new(
        endpoints: impl IntoIterator<Item = (String, String)>,
        client: Arc<dyn RemoteReplicaClient>,
    ) -> Self {
        Self {
            endpoints: endpoints.into_iter().collect(),
            database: None,
            client,
        }
    }

    pub fn from_write_plan(
        plan: &DistributedWritePlan,
        client: Arc<dyn RemoteReplicaClient>,
    ) -> Self {
        Self {
            endpoints: plan
                .replicas
                .iter()
                .map(|peer| (peer.node_id.clone(), peer.advertise_addr.clone()))
                .collect(),
            database: Some(plan.placement.database.clone()),
            client,
        }
    }

    pub fn from_read_plan(
        plan: &DistributedReadPlan,
        client: Arc<dyn RemoteReplicaClient>,
    ) -> Self {
        Self {
            endpoints: plan
                .replicas
                .iter()
                .map(|peer| (peer.node_id.clone(), peer.advertise_addr.clone()))
                .collect(),
            database: Some(plan.placement.database.clone()),
            client,
        }
    }

    fn endpoint_for(&self, target: &str) -> Result<String, ReplicationError> {
        self.endpoints
            .get(target)
            .cloned()
            .ok_or_else(|| ReplicationError::Transport(format!("unknown remote replica {target}")))
    }

    fn graph_database(&self) -> Result<String, ReplicationError> {
        self.database.clone().ok_or_else(|| {
            ReplicationError::Transport(
                "remote graph read database is not configured for nornic gRPC replica transport"
                    .into(),
            )
        })
    }
}

impl NornicGrpcRankedSearchTransport {
    pub fn new(
        endpoints: impl IntoIterator<Item = (String, String)>,
        client: Arc<dyn RemoteRankedSearchClient>,
    ) -> Self {
        Self {
            endpoints: endpoints.into_iter().collect(),
            client,
        }
    }

    pub fn from_search_plan(
        plan: &DistributedSearchPlan,
        client: Arc<dyn RemoteRankedSearchClient>,
    ) -> Self {
        Self::new(
            plan.fanout
                .iter()
                .map(|peer| (peer.node_id.clone(), peer.advertise_addr.clone())),
            client,
        )
    }

    fn endpoint_for(&self, target: &str) -> Result<String, SearchError> {
        self.endpoints.get(target).cloned().ok_or_else(|| {
            SearchError::Transport(format!("unknown remote ranked search node {target}"))
        })
    }
}

impl NornicGrpcHydrationTransport {
    pub fn new(
        endpoints: impl IntoIterator<Item = (String, String)>,
        client: Arc<dyn RemoteHydrationClient>,
    ) -> Self {
        Self {
            endpoints: endpoints.into_iter().collect(),
            client,
        }
    }

    pub fn from_read_plan(
        plan: &DistributedReadPlan,
        client: Arc<dyn RemoteHydrationClient>,
    ) -> Self {
        Self::new(
            plan.replicas
                .iter()
                .map(|peer| (peer.node_id.clone(), peer.advertise_addr.clone())),
            client,
        )
    }

    fn endpoint_for(&self, target: &str) -> Result<String, SearchError> {
        self.endpoints.get(target).cloned().ok_or_else(|| {
            SearchError::Transport(format!("unknown remote hydration node {target}"))
        })
    }
}

#[async_trait]
impl ReplicaTransport for NornicGrpcReplicaTransport {
    async fn apply_replica(&self, target: &str, command: Command) -> Result<(), ReplicationError> {
        self.client
            .apply_replica(RemoteReplicaApplyRequest {
                target_node: target.into(),
                target_addr: self.endpoint_for(target)?,
                command,
            })
            .await
            .map_err(|error| ReplicationError::Transport(error.to_string()))
    }

    async fn read_replica(
        &self,
        target: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, ReplicationError> {
        self.client
            .read_replica(RemoteReplicaReadRequest {
                target_node: target.into(),
                target_addr: self.endpoint_for(target)?,
                key: key.to_vec(),
            })
            .await
            .map_err(|error| ReplicationError::Transport(error.to_string()))
    }

    async fn graph_node(
        &self,
        target: &str,
        node_id: &str,
        read_fence: Option<LogicalTransactionId>,
    ) -> Result<Option<Vec<u8>>, ReplicationError> {
        self.client
            .graph_node(RemoteGraphNodeRequest {
                target_node: target.into(),
                target_addr: self.endpoint_for(target)?,
                database: self.graph_database()?,
                node_id: node_id.into(),
                read_fence,
                caller_auth_token: None,
            })
            .await
            .map_err(|error| ReplicationError::Transport(error.to_string()))
    }

    async fn graph_edges_from_node(
        &self,
        target: &str,
        node_id: &str,
        rel_type: Option<&str>,
        read_fence: Option<LogicalTransactionId>,
    ) -> Result<Vec<EdgeRecord>, ReplicationError> {
        self.client
            .graph_edges_from_node(RemoteGraphEdgesRequest {
                target_node: target.into(),
                target_addr: self.endpoint_for(target)?,
                database: self.graph_database()?,
                node_id: node_id.into(),
                rel_type: rel_type.map(str::to_owned),
                read_fence,
                caller_auth_token: None,
            })
            .await
            .map_err(|error| ReplicationError::Transport(error.to_string()))
    }

    async fn graph_edges_to_node(
        &self,
        target: &str,
        node_id: &str,
        rel_type: Option<&str>,
        read_fence: Option<LogicalTransactionId>,
    ) -> Result<Vec<EdgeRecord>, ReplicationError> {
        self.client
            .graph_edges_to_node(RemoteGraphEdgesRequest {
                target_node: target.into(),
                target_addr: self.endpoint_for(target)?,
                database: self.graph_database()?,
                node_id: node_id.into(),
                rel_type: rel_type.map(str::to_owned),
                read_fence,
                caller_auth_token: None,
            })
            .await
            .map_err(|error| ReplicationError::Transport(error.to_string()))
    }

    async fn graph_nodes_by_label(
        &self,
        target: &str,
        label: &str,
        read_fence: Option<LogicalTransactionId>,
    ) -> Result<Vec<Vec<u8>>, ReplicationError> {
        self.client
            .graph_nodes_by_label(RemoteGraphNodesByLabelRequest {
                target_node: target.into(),
                target_addr: self.endpoint_for(target)?,
                database: self.graph_database()?,
                label: label.into(),
                read_fence,
                caller_auth_token: None,
            })
            .await
            .map_err(|error| ReplicationError::Transport(error.to_string()))
    }

    async fn graph_nodes_by_property(
        &self,
        target: &str,
        label: &str,
        property: &str,
        value: &Value,
        read_fence: Option<LogicalTransactionId>,
    ) -> Result<Vec<Vec<u8>>, ReplicationError> {
        self.client
            .graph_nodes_by_property(RemoteGraphNodesByPropertyRequest {
                target_node: target.into(),
                target_addr: self.endpoint_for(target)?,
                database: self.graph_database()?,
                label: label.into(),
                property: property.into(),
                value: value.clone(),
                read_fence,
                caller_auth_token: None,
            })
            .await
            .map_err(|error| ReplicationError::Transport(error.to_string()))
    }

    async fn graph_access_metadata(
        &self,
        target: &str,
        entity_id: &str,
        read_fence: Option<LogicalTransactionId>,
    ) -> Result<Option<copperdb_storage::KnowledgePolicyAccessMetadata>, ReplicationError> {
        self.client
            .graph_access_metadata(RemoteGraphAccessMetadataRequest {
                target_node: target.into(),
                target_addr: self.endpoint_for(target)?,
                database: self.graph_database()?,
                entity_id: entity_id.into(),
                read_fence,
                caller_auth_token: None,
            })
            .await
            .map_err(|error| ReplicationError::Transport(error.to_string()))
    }
}

#[async_trait]
impl RankedSearchTransport for NornicGrpcRankedSearchTransport {
    async fn search_ranked_node(
        &self,
        node_id: &str,
        placement: &PlacementKey,
        query: &SearchQuery,
        read_fence: Option<LogicalTransactionId>,
    ) -> Result<RrfSearchBatch, SearchError> {
        self.client
            .search_ranked(RemoteRankedSearchRequest {
                target_node: node_id.into(),
                target_addr: self.endpoint_for(node_id)?,
                placement: placement.clone(),
                query: query.clone(),
                read_fence,
                caller_auth_token: None,
            })
            .await
            .map_err(|error| SearchError::Transport(error.to_string()))
    }
}

#[async_trait]
impl HydrationTransport for NornicGrpcHydrationTransport {
    async fn hydrate_node(
        &self,
        node_id: &str,
        placement: &PlacementKey,
        global_ids: &[FabricGlobalId],
        read_fence: Option<LogicalTransactionId>,
    ) -> Result<Vec<RrfHydrationRecord>, SearchError> {
        self.client
            .hydrate_entities(RemoteHydrationRequest {
                target_node: node_id.into(),
                target_addr: self.endpoint_for(node_id)?,
                placement: placement.clone(),
                global_ids: global_ids.to_vec(),
                read_fence,
                caller_auth_token: None,
            })
            .await
            .map_err(|error| SearchError::Transport(error.to_string()))
    }
}

impl RemoteExecutionEnvelope {
    pub fn write_fanout(request_id: impl Into<String>, plan: &DistributedWritePlan) -> Vec<Self> {
        let request_id = request_id.into();
        plan.replicas
            .iter()
            .map(|peer| Self {
                request_id: request_id.clone(),
                target_node: peer.node_id.clone(),
                target_addr: peer.advertise_addr.clone(),
                kind: RemoteExecutionKind::Write,
                placement: plan.placement.clone(),
                consistency: Some(plan.consistency),
                write_mode: Some(plan.mode),
            })
            .collect()
    }

    pub fn read_fanout(request_id: impl Into<String>, plan: &DistributedReadPlan) -> Vec<Self> {
        let request_id = request_id.into();
        plan.replicas
            .iter()
            .map(|peer| Self {
                request_id: request_id.clone(),
                target_node: peer.node_id.clone(),
                target_addr: peer.advertise_addr.clone(),
                kind: RemoteExecutionKind::Read,
                placement: plan.placement.clone(),
                consistency: Some(plan.consistency),
                write_mode: None,
            })
            .collect()
    }

    pub fn search_fanout(request_id: impl Into<String>, plan: &DistributedSearchPlan) -> Vec<Self> {
        let request_id = request_id.into();
        plan.fanout
            .iter()
            .map(|peer| Self {
                request_id: request_id.clone(),
                target_node: peer.node_id.clone(),
                target_addr: peer.advertise_addr.clone(),
                kind: RemoteExecutionKind::Search,
                placement: plan.placement.clone(),
                consistency: None,
                write_mode: None,
            })
            .collect()
    }
}

impl TryFrom<RemoteReplicaApplyRequest> for proto::RemoteReplicaApplyRequest {
    type Error = GrpcError;

    fn try_from(request: RemoteReplicaApplyRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            target_node: request.target_node,
            target_addr: request.target_addr,
            command_json: serde_json::to_vec(&request.command)
                .map_err(|error| GrpcError::Encoding(error.to_string()))?,
        })
    }
}

impl TryFrom<proto::RemoteReplicaApplyRequest> for RemoteReplicaApplyRequest {
    type Error = GrpcError;

    fn try_from(request: proto::RemoteReplicaApplyRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            target_node: request.target_node,
            target_addr: request.target_addr,
            command: serde_json::from_slice(&request.command_json)
                .map_err(|error| GrpcError::Encoding(error.to_string()))?,
        })
    }
}

impl From<RemoteReplicaReadRequest> for proto::RemoteReplicaReadRequest {
    fn from(request: RemoteReplicaReadRequest) -> Self {
        Self {
            target_node: request.target_node,
            target_addr: request.target_addr,
            key: request.key,
        }
    }
}

impl From<proto::RemoteReplicaReadRequest> for RemoteReplicaReadRequest {
    fn from(request: proto::RemoteReplicaReadRequest) -> Self {
        Self {
            target_node: request.target_node,
            target_addr: request.target_addr,
            key: request.key,
        }
    }
}

impl From<Option<Vec<u8>>> for proto::RemoteReplicaReadResponse {
    fn from(value: Option<Vec<u8>>) -> Self {
        match value {
            Some(value) => Self { found: true, value },
            None => Self {
                found: false,
                value: Vec::new(),
            },
        }
    }
}

impl From<proto::RemoteReplicaReadResponse> for Option<Vec<u8>> {
    fn from(response: proto::RemoteReplicaReadResponse) -> Self {
        response.found.then_some(response.value)
    }
}

impl From<RemoteGraphNodeRequest> for proto::RemoteGraphNodeRequest {
    fn from(request: RemoteGraphNodeRequest) -> Self {
        Self {
            target_node: request.target_node,
            target_addr: request.target_addr,
            database: request.database,
            node_id: request.node_id,
            read_fence: encode_read_fence(request.read_fence),
        }
    }
}

impl From<proto::RemoteGraphNodeRequest> for RemoteGraphNodeRequest {
    fn from(request: proto::RemoteGraphNodeRequest) -> Self {
        Self {
            target_node: request.target_node,
            target_addr: request.target_addr,
            database: request.database,
            node_id: request.node_id,
            read_fence: decode_read_fence(&request.read_fence)
                .expect("generated proto graph node read fence should decode"),
            caller_auth_token: None,
        }
    }
}

impl From<Option<Vec<u8>>> for proto::RemoteGraphNodeResponse {
    fn from(value: Option<Vec<u8>>) -> Self {
        match value {
            Some(value) => Self { found: true, value },
            None => Self {
                found: false,
                value: Vec::new(),
            },
        }
    }
}

impl From<proto::RemoteGraphNodeResponse> for Option<Vec<u8>> {
    fn from(response: proto::RemoteGraphNodeResponse) -> Self {
        response.found.then_some(response.value)
    }
}

impl From<RemoteGraphEdgesRequest> for proto::RemoteGraphEdgesRequest {
    fn from(request: RemoteGraphEdgesRequest) -> Self {
        Self {
            target_node: request.target_node,
            target_addr: request.target_addr,
            database: request.database,
            node_id: request.node_id,
            rel_type: request.rel_type.clone().unwrap_or_default(),
            has_rel_type: request.rel_type.is_some(),
            read_fence: encode_read_fence(request.read_fence),
        }
    }
}

impl From<proto::RemoteGraphEdgesRequest> for RemoteGraphEdgesRequest {
    fn from(request: proto::RemoteGraphEdgesRequest) -> Self {
        Self {
            target_node: request.target_node,
            target_addr: request.target_addr,
            database: request.database,
            node_id: request.node_id,
            rel_type: request.has_rel_type.then_some(request.rel_type),
            read_fence: decode_read_fence(&request.read_fence)
                .expect("generated proto graph edges read fence should decode"),
            caller_auth_token: None,
        }
    }
}

impl TryFrom<Vec<EdgeRecord>> for proto::RemoteGraphEdgesResponse {
    type Error = GrpcError;

    fn try_from(edges: Vec<EdgeRecord>) -> Result<Self, Self::Error> {
        Ok(Self {
            edges_json: serde_json::to_vec(&edges)
                .map_err(|error| GrpcError::Encoding(error.to_string()))?,
        })
    }
}

impl TryFrom<proto::RemoteGraphEdgesResponse> for Vec<EdgeRecord> {
    type Error = GrpcError;

    fn try_from(response: proto::RemoteGraphEdgesResponse) -> Result<Self, Self::Error> {
        serde_json::from_slice(&response.edges_json)
            .map_err(|error| GrpcError::Encoding(error.to_string()))
    }
}

impl From<RemoteGraphNodesByLabelRequest> for proto::RemoteGraphNodesByLabelRequest {
    fn from(request: RemoteGraphNodesByLabelRequest) -> Self {
        Self {
            target_node: request.target_node,
            target_addr: request.target_addr,
            database: request.database,
            label: request.label,
            read_fence: encode_read_fence(request.read_fence),
        }
    }
}

impl From<proto::RemoteGraphNodesByLabelRequest> for RemoteGraphNodesByLabelRequest {
    fn from(request: proto::RemoteGraphNodesByLabelRequest) -> Self {
        Self {
            target_node: request.target_node,
            target_addr: request.target_addr,
            database: request.database,
            label: request.label,
            read_fence: decode_read_fence(&request.read_fence)
                .expect("generated proto graph label read fence should decode"),
            caller_auth_token: None,
        }
    }
}

impl TryFrom<RemoteGraphNodesByPropertyRequest> for proto::RemoteGraphNodesByPropertyRequest {
    type Error = GrpcError;

    fn try_from(request: RemoteGraphNodesByPropertyRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            target_node: request.target_node,
            target_addr: request.target_addr,
            database: request.database,
            label: request.label,
            property: request.property,
            value_json: serde_json::to_vec(&request.value)
                .map_err(|error| GrpcError::Encoding(error.to_string()))?,
            read_fence: encode_read_fence(request.read_fence),
        })
    }
}

impl TryFrom<proto::RemoteGraphNodesByPropertyRequest> for RemoteGraphNodesByPropertyRequest {
    type Error = GrpcError;

    fn try_from(request: proto::RemoteGraphNodesByPropertyRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            target_node: request.target_node,
            target_addr: request.target_addr,
            database: request.database,
            label: request.label,
            property: request.property,
            value: serde_json::from_slice(&request.value_json)
                .map_err(|error| GrpcError::Encoding(error.to_string()))?,
            read_fence: decode_read_fence(&request.read_fence)?,
            caller_auth_token: None,
        })
    }
}

impl From<Vec<Vec<u8>>> for proto::RemoteGraphNodesResponse {
    fn from(values: Vec<Vec<u8>>) -> Self {
        Self { values }
    }
}

impl From<proto::RemoteGraphNodesResponse> for Vec<Vec<u8>> {
    fn from(response: proto::RemoteGraphNodesResponse) -> Self {
        response.values
    }
}

impl From<RemoteGraphAccessMetadataRequest> for proto::RemoteGraphAccessMetadataRequest {
    fn from(request: RemoteGraphAccessMetadataRequest) -> Self {
        Self {
            target_node: request.target_node,
            target_addr: request.target_addr,
            database: request.database,
            entity_id: request.entity_id,
            read_fence: encode_read_fence(request.read_fence),
        }
    }
}

impl From<proto::RemoteGraphAccessMetadataRequest> for RemoteGraphAccessMetadataRequest {
    fn from(request: proto::RemoteGraphAccessMetadataRequest) -> Self {
        Self {
            target_node: request.target_node,
            target_addr: request.target_addr,
            database: request.database,
            entity_id: request.entity_id,
            read_fence: decode_read_fence(&request.read_fence)
                .expect("generated proto graph metadata read fence should decode"),
            caller_auth_token: None,
        }
    }
}

impl TryFrom<Option<KnowledgePolicyAccessMetadata>> for proto::RemoteGraphAccessMetadataResponse {
    type Error = GrpcError;

    fn try_from(value: Option<KnowledgePolicyAccessMetadata>) -> Result<Self, Self::Error> {
        match value {
            Some(metadata) => Ok(Self {
                found: true,
                metadata_json: serde_json::to_vec(&metadata)
                    .map_err(|error| GrpcError::Encoding(error.to_string()))?,
            }),
            None => Ok(Self {
                found: false,
                metadata_json: Vec::new(),
            }),
        }
    }
}

impl TryFrom<proto::RemoteGraphAccessMetadataResponse> for Option<KnowledgePolicyAccessMetadata> {
    type Error = GrpcError;

    fn try_from(response: proto::RemoteGraphAccessMetadataResponse) -> Result<Self, Self::Error> {
        if !response.found {
            return Ok(None);
        }
        let metadata = serde_json::from_slice(&response.metadata_json)
            .map_err(|error| GrpcError::Encoding(error.to_string()))?;
        Ok(Some(metadata))
    }
}

impl TryFrom<RemoteRankedSearchRequest> for proto::RemoteRankedSearchRequest {
    type Error = GrpcError;

    fn try_from(request: RemoteRankedSearchRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            target_node: request.target_node,
            target_addr: request.target_addr,
            placement_json: serde_json::to_vec(&request.placement)
                .map_err(|error| GrpcError::Encoding(error.to_string()))?,
            query_json: serde_json::to_vec(&request.query)
                .map_err(|error| GrpcError::Encoding(error.to_string()))?,
            read_fence: encode_read_fence(request.read_fence),
        })
    }
}

impl TryFrom<proto::RemoteRankedSearchRequest> for RemoteRankedSearchRequest {
    type Error = GrpcError;

    fn try_from(request: proto::RemoteRankedSearchRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            target_node: request.target_node,
            target_addr: request.target_addr,
            placement: serde_json::from_slice(&request.placement_json)
                .map_err(|error| GrpcError::Encoding(error.to_string()))?,
            query: serde_json::from_slice(&request.query_json)
                .map_err(|error| GrpcError::Encoding(error.to_string()))?,
            read_fence: decode_read_fence(&request.read_fence)?,
            caller_auth_token: None,
        })
    }
}

impl TryFrom<RrfSearchBatch> for proto::RemoteRankedSearchResponse {
    type Error = GrpcError;

    fn try_from(batch: RrfSearchBatch) -> Result<Self, Self::Error> {
        Ok(Self {
            batch_json: serde_json::to_vec(&batch)
                .map_err(|error| GrpcError::Encoding(error.to_string()))?,
        })
    }
}

impl TryFrom<proto::RemoteRankedSearchResponse> for RrfSearchBatch {
    type Error = GrpcError;

    fn try_from(response: proto::RemoteRankedSearchResponse) -> Result<Self, Self::Error> {
        serde_json::from_slice(&response.batch_json)
            .map_err(|error| GrpcError::Encoding(error.to_string()))
    }
}

impl TryFrom<RemoteHydrationRequest> for proto::RemoteHydrationRequest {
    type Error = GrpcError;

    fn try_from(request: RemoteHydrationRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            target_node: request.target_node,
            target_addr: request.target_addr,
            placement_json: serde_json::to_vec(&request.placement)
                .map_err(|error| GrpcError::Encoding(error.to_string()))?,
            global_ids_json: serde_json::to_vec(&request.global_ids)
                .map_err(|error| GrpcError::Encoding(error.to_string()))?,
            read_fence: encode_read_fence(request.read_fence),
        })
    }
}

impl TryFrom<proto::RemoteHydrationRequest> for RemoteHydrationRequest {
    type Error = GrpcError;

    fn try_from(request: proto::RemoteHydrationRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            target_node: request.target_node,
            target_addr: request.target_addr,
            placement: serde_json::from_slice(&request.placement_json)
                .map_err(|error| GrpcError::Encoding(error.to_string()))?,
            global_ids: serde_json::from_slice(&request.global_ids_json)
                .map_err(|error| GrpcError::Encoding(error.to_string()))?,
            read_fence: decode_read_fence(&request.read_fence)?,
            caller_auth_token: None,
        })
    }
}

impl TryFrom<Vec<RrfHydrationRecord>> for proto::RemoteHydrationResponse {
    type Error = GrpcError;

    fn try_from(records: Vec<RrfHydrationRecord>) -> Result<Self, Self::Error> {
        Ok(Self {
            records_json: serde_json::to_vec(&records)
                .map_err(|error| GrpcError::Encoding(error.to_string()))?,
        })
    }
}

impl TryFrom<proto::RemoteHydrationResponse> for Vec<RrfHydrationRecord> {
    type Error = GrpcError;

    fn try_from(response: proto::RemoteHydrationResponse) -> Result<Self, Self::Error> {
        serde_json::from_slice(&response.records_json)
            .map_err(|error| GrpcError::Encoding(error.to_string()))
    }
}

impl GrpcCypherResponse {
    pub fn ok(columns: Vec<String>, rows: Vec<GrpcCypherRow>) -> Self {
        Self {
            columns,
            rows,
            error: None,
        }
    }

    pub fn error(msg: impl Into<String>) -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            error: Some(msg.into()),
        }
    }
}

#[cfg(test)]
mod tests;
