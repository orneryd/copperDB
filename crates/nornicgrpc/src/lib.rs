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
    DistributedWritePlan, FabricGlobalId, PlacementKey,
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
    pub caller_auth_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RemoteGraphEdgesRequest {
    pub target_node: String,
    pub target_addr: String,
    pub database: String,
    pub node_id: String,
    pub rel_type: Option<String>,
    pub caller_auth_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RemoteGraphNodesByLabelRequest {
    pub target_node: String,
    pub target_addr: String,
    pub database: String,
    pub label: String,
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
    pub caller_auth_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RemoteGraphAccessMetadataRequest {
    pub target_node: String,
    pub target_addr: String,
    pub database: String,
    pub entity_id: String,
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
        Ok(Response::new(proto::RemoteGraphNodeResponse::from(response)))
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
        Ok(Response::new(proto::RemoteGraphNodesResponse::from(response)))
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
        Ok(Response::new(proto::RemoteGraphNodesResponse::from(response)))
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
    ) -> Result<Option<Vec<u8>>, ReplicationError> {
        self.client
            .graph_node(RemoteGraphNodeRequest {
                target_node: target.into(),
                target_addr: self.endpoint_for(target)?,
                database: self.graph_database()?,
                node_id: node_id.into(),
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
    ) -> Result<Vec<EdgeRecord>, ReplicationError> {
        self.client
            .graph_edges_from_node(RemoteGraphEdgesRequest {
                target_node: target.into(),
                target_addr: self.endpoint_for(target)?,
                database: self.graph_database()?,
                node_id: node_id.into(),
                rel_type: rel_type.map(str::to_owned),
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
    ) -> Result<Vec<EdgeRecord>, ReplicationError> {
        self.client
            .graph_edges_to_node(RemoteGraphEdgesRequest {
                target_node: target.into(),
                target_addr: self.endpoint_for(target)?,
                database: self.graph_database()?,
                node_id: node_id.into(),
                rel_type: rel_type.map(str::to_owned),
                caller_auth_token: None,
            })
            .await
            .map_err(|error| ReplicationError::Transport(error.to_string()))
    }

    async fn graph_nodes_by_label(
        &self,
        target: &str,
        label: &str,
    ) -> Result<Vec<Vec<u8>>, ReplicationError> {
        self.client
            .graph_nodes_by_label(RemoteGraphNodesByLabelRequest {
                target_node: target.into(),
                target_addr: self.endpoint_for(target)?,
                database: self.graph_database()?,
                label: label.into(),
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
    ) -> Result<Vec<Vec<u8>>, ReplicationError> {
        self.client
            .graph_nodes_by_property(RemoteGraphNodesByPropertyRequest {
                target_node: target.into(),
                target_addr: self.endpoint_for(target)?,
                database: self.graph_database()?,
                label: label.into(),
                property: property.into(),
                value: value.clone(),
                caller_auth_token: None,
            })
            .await
            .map_err(|error| ReplicationError::Transport(error.to_string()))
    }

    async fn graph_access_metadata(
        &self,
        target: &str,
        entity_id: &str,
    ) -> Result<Option<copperdb_storage::KnowledgePolicyAccessMetadata>, ReplicationError> {
        self.client
            .graph_access_metadata(RemoteGraphAccessMetadataRequest {
                target_node: target.into(),
                target_addr: self.endpoint_for(target)?,
                database: self.graph_database()?,
                entity_id: entity_id.into(),
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
    ) -> Result<RrfSearchBatch, SearchError> {
        self.client
            .search_ranked(RemoteRankedSearchRequest {
                target_node: node_id.into(),
                target_addr: self.endpoint_for(node_id)?,
                placement: placement.clone(),
                query: query.clone(),
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
    ) -> Result<Vec<RrfHydrationRecord>, SearchError> {
        self.client
            .hydrate_entities(RemoteHydrationRequest {
                target_node: node_id.into(),
                target_addr: self.endpoint_for(node_id)?,
                placement: placement.clone(),
                global_ids: global_ids.to_vec(),
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
mod tests {
    use super::*;
    use crate::proto::nornic_replica_server::NornicReplica;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingRemoteReplicaClient {
        applies: Mutex<Vec<RemoteReplicaApplyRequest>>,
        reads: Mutex<Vec<RemoteReplicaReadRequest>>,
        read_response: Mutex<Option<Vec<u8>>>,
        graph_nodes: Mutex<Vec<RemoteGraphNodeRequest>>,
        graph_node_response: Mutex<Option<Vec<u8>>>,
        graph_edges_from: Mutex<Vec<RemoteGraphEdgesRequest>>,
        graph_edges_to: Mutex<Vec<RemoteGraphEdgesRequest>>,
        graph_edges_response: Mutex<Vec<EdgeRecord>>,
        graph_nodes_by_label: Mutex<Vec<RemoteGraphNodesByLabelRequest>>,
        graph_nodes_by_property: Mutex<Vec<RemoteGraphNodesByPropertyRequest>>,
        graph_nodes_response: Mutex<Vec<Vec<u8>>>,
        graph_access_metadata: Mutex<Vec<RemoteGraphAccessMetadataRequest>>,
        graph_access_metadata_response: Mutex<Option<KnowledgePolicyAccessMetadata>>,
    }

    #[derive(Default)]
    struct RecordingRemoteRankedSearchClient {
        requests: Mutex<Vec<RemoteRankedSearchRequest>>,
        batches: Mutex<HashMap<String, RrfSearchBatch>>,
    }

    #[derive(Default)]
    struct RecordingRemoteHydrationClient {
        requests: Mutex<Vec<RemoteHydrationRequest>>,
        records: Mutex<HashMap<String, Vec<RrfHydrationRecord>>>,
    }

    #[async_trait]
    impl RemoteReplicaClient for RecordingRemoteReplicaClient {
        async fn apply_replica(&self, request: RemoteReplicaApplyRequest) -> Result<(), GrpcError> {
            self.applies.lock().unwrap().push(request);
            Ok(())
        }

        async fn read_replica(
            &self,
            request: RemoteReplicaReadRequest,
        ) -> Result<Option<Vec<u8>>, GrpcError> {
            self.reads.lock().unwrap().push(request);
            Ok(self.read_response.lock().unwrap().clone())
        }

        async fn graph_node(
            &self,
            request: RemoteGraphNodeRequest,
        ) -> Result<Option<Vec<u8>>, GrpcError> {
            self.graph_nodes.lock().unwrap().push(request);
            Ok(self.graph_node_response.lock().unwrap().clone())
        }

        async fn graph_edges_from_node(
            &self,
            request: RemoteGraphEdgesRequest,
        ) -> Result<Vec<EdgeRecord>, GrpcError> {
            self.graph_edges_from.lock().unwrap().push(request);
            Ok(self.graph_edges_response.lock().unwrap().clone())
        }

        async fn graph_edges_to_node(
            &self,
            request: RemoteGraphEdgesRequest,
        ) -> Result<Vec<EdgeRecord>, GrpcError> {
            self.graph_edges_to.lock().unwrap().push(request);
            Ok(self.graph_edges_response.lock().unwrap().clone())
        }

        async fn graph_nodes_by_label(
            &self,
            request: RemoteGraphNodesByLabelRequest,
        ) -> Result<Vec<Vec<u8>>, GrpcError> {
            self.graph_nodes_by_label.lock().unwrap().push(request);
            Ok(self.graph_nodes_response.lock().unwrap().clone())
        }

        async fn graph_nodes_by_property(
            &self,
            request: RemoteGraphNodesByPropertyRequest,
        ) -> Result<Vec<Vec<u8>>, GrpcError> {
            self.graph_nodes_by_property.lock().unwrap().push(request);
            Ok(self.graph_nodes_response.lock().unwrap().clone())
        }

        async fn graph_access_metadata(
            &self,
            request: RemoteGraphAccessMetadataRequest,
        ) -> Result<Option<KnowledgePolicyAccessMetadata>, GrpcError> {
            self.graph_access_metadata.lock().unwrap().push(request);
            Ok(self.graph_access_metadata_response.lock().unwrap().clone())
        }
    }

    #[async_trait]
    impl RemoteRankedSearchClient for RecordingRemoteRankedSearchClient {
        async fn search_ranked(
            &self,
            request: RemoteRankedSearchRequest,
        ) -> Result<RrfSearchBatch, GrpcError> {
            self.requests.lock().unwrap().push(request.clone());
            self.batches
                .lock()
                .unwrap()
                .get(&request.target_node)
                .cloned()
                .ok_or_else(|| {
                    GrpcError::Transport(format!("no ranked batch for {}", request.target_node))
                })
        }
    }

    #[async_trait]
    impl RemoteHydrationClient for RecordingRemoteHydrationClient {
        async fn hydrate_entities(
            &self,
            request: RemoteHydrationRequest,
        ) -> Result<Vec<RrfHydrationRecord>, GrpcError> {
            self.requests.lock().unwrap().push(request.clone());
            Ok(self
                .records
                .lock()
                .unwrap()
                .get(&request.target_node)
                .cloned()
                .unwrap_or_default())
        }
    }

    #[test]
    fn test_grpc_config_defaults() {
        let cfg = GrpcConfig::default();
        assert_eq!(cfg.listen_addr, "[::1]:50051");
        assert_eq!(cfg.max_connections, 1000);
        assert!(!cfg.tls_enabled);
    }

    #[test]
    fn test_grpc_server_accessors() {
        let cfg = GrpcConfig::new("0.0.0.0:50051");
        let server = GrpcServer::new(cfg);
        assert_eq!(server.listen_addr(), "0.0.0.0:50051");
        assert_eq!(server.max_connections(), 1000);
    }

    #[test]
    fn test_grpc_server_custom_config() {
        let mut cfg = GrpcConfig::new("127.0.0.1:9090");
        cfg.max_connections = 50;
        cfg.tls_enabled = true;
        let server = GrpcServer::new(cfg);
        assert_eq!(server.max_connections(), 50);
        assert!(server.tls_enabled());
    }

    #[test]
    fn data_path_clients_do_not_default_cluster_auth_tokens() {
        let ranked = TonicRemoteRankedSearchClient::new();
        assert!(ranked.caller_auth_token.is_none());

        let hydration = TonicRemoteHydrationClient::new();
        assert!(hydration.caller_auth_token.is_none());
    }

    #[test]
    fn test_grpc_cypher_response_ok() {
        let resp = GrpcCypherResponse::ok(
            vec!["n".into()],
            vec![GrpcCypherRow {
                values: vec![serde_json::json!(1)],
            }],
        );
        assert!(resp.error.is_none());
        assert_eq!(resp.columns.len(), 1);
    }

    #[test]
    fn test_grpc_cypher_response_error() {
        let resp = GrpcCypherResponse::error("syntax error");
        assert!(resp.error.is_some());
        assert!(resp.rows.is_empty());
    }

    #[test]
    fn remote_execution_envelopes_preserve_write_plan_targets() {
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementRecord, TopologyRegistry};

        let placement = PlacementKey::default_for_database("copper");
        let mut topology = TopologyRegistry::new();
        for node_id in ["node-1", "node-2", "node-3"] {
            topology
                .register_peer(
                    MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        topology
            .register_placement(PlacementRecord {
                key: placement.clone(),
                primary_node: "node-1".into(),
                replica_nodes: vec!["node-2".into(), "node-3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();
        let plan = topology
            .plan_write_with_consistency(
                &placement,
                DistributedWriteMode::DynamoQuorum,
                ConsistencyLevel::Quorum,
                None,
            )
            .unwrap();

        let envelopes = RemoteExecutionEnvelope::write_fanout("req-1", &plan);
        assert_eq!(envelopes.len(), 3);
        assert!(envelopes.iter().all(|envelope| {
            envelope.kind == RemoteExecutionKind::Write
                && envelope.consistency == Some(ConsistencyLevel::Quorum)
                && envelope.write_mode == Some(DistributedWriteMode::DynamoQuorum)
        }));
    }

    #[test]
    fn remote_execution_envelopes_preserve_search_plan_targets() {
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementRecord, TopologyRegistry};

        let placement = PlacementKey::default_for_database("copper");
        let mut topology = TopologyRegistry::new();
        for node_id in ["search-a", "search-b"] {
            topology
                .register_peer(
                    MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Search),
                )
                .unwrap();
        }
        topology
            .register_placement(PlacementRecord {
                key: placement.clone(),
                primary_node: "search-a".into(),
                replica_nodes: vec![],
                search_nodes: vec!["search-a".into(), "search-b".into()],
                hyperscaler_profile: None,
                min_write_replicas: 0,
                search_fanout: 2,
            })
            .unwrap();
        let plan = topology.plan_search(&placement).unwrap();

        let envelopes = RemoteExecutionEnvelope::search_fanout("req-search", &plan);
        assert_eq!(envelopes.len(), 2);
        assert!(envelopes.iter().all(|envelope| {
            envelope.kind == RemoteExecutionKind::Search
                && envelope.consistency.is_none()
                && envelope.write_mode.is_none()
                && envelope.placement == placement
        }));
    }

    #[tokio::test]
    async fn remote_replica_transport_sends_write_requests_to_target_endpoint() {
        let client = Arc::new(RecordingRemoteReplicaClient::default());
        let transport = NornicGrpcReplicaTransport::new(
            [("node-2".to_string(), "node-2.mesh.local:50051".to_string())],
            client.clone(),
        );

        transport
            .apply_replica(
                "node-2",
                Command::Put {
                    key: b"remote-write".to_vec(),
                    value: b"ok".to_vec(),
                },
            )
            .await
            .unwrap();

        let applies = client.applies.lock().unwrap();
        assert_eq!(applies.len(), 1);
        assert_eq!(applies[0].target_node, "node-2");
        assert_eq!(applies[0].target_addr, "node-2.mesh.local:50051");
        assert_eq!(
            applies[0].command,
            Command::Put {
                key: b"remote-write".to_vec(),
                value: b"ok".to_vec(),
            }
        );
    }

    #[tokio::test]
    async fn remote_replica_transport_sends_read_requests_to_target_endpoint() {
        let client = Arc::new(RecordingRemoteReplicaClient::default());
        *client.read_response.lock().unwrap() = Some(b"remote-value".to_vec());
        let transport = NornicGrpcReplicaTransport::new(
            [("node-3".to_string(), "node-3.mesh.local:50051".to_string())],
            client.clone(),
        );

        let value = transport
            .read_replica("node-3", b"remote-read")
            .await
            .unwrap();

        assert_eq!(value, Some(b"remote-value".to_vec()));
        let reads = client.reads.lock().unwrap();
        assert_eq!(reads.len(), 1);
        assert_eq!(reads[0].target_node, "node-3");
        assert_eq!(reads[0].target_addr, "node-3.mesh.local:50051");
        assert_eq!(reads[0].key, b"remote-read".to_vec());
    }

    #[tokio::test]
    async fn remote_replica_transport_sends_graph_node_requests_to_target_endpoint() {
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementRecord, TopologyRegistry};

        let placement = PlacementKey::default_for_database("copper");
        let mut topology = TopologyRegistry::new();
        topology
            .register_peer(
                MeshPeer::new("node-4", "node-4.mesh.local:50051")
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
        topology
            .register_placement(PlacementRecord {
                key: placement.clone(),
                primary_node: "node-4".into(),
                replica_nodes: vec![],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 0,
                search_fanout: 1,
            })
            .unwrap();
        let plan = topology
            .plan_read(&placement, ConsistencyLevel::One, None)
            .unwrap();

        let client = Arc::new(RecordingRemoteReplicaClient::default());
        *client.graph_node_response.lock().unwrap() = Some(b"node-bytes".to_vec());
        let transport = NornicGrpcReplicaTransport::from_read_plan(&plan, client.clone());

        let value = transport.graph_node("node-4", "person-1").await.unwrap();

        assert_eq!(value, Some(b"node-bytes".to_vec()));
        let requests = client.graph_nodes.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].target_node, "node-4");
        assert_eq!(requests[0].target_addr, "node-4.mesh.local:50051");
        assert_eq!(requests[0].database, "copper");
        assert_eq!(requests[0].node_id, "person-1");
    }

    #[tokio::test]
    async fn remote_replica_transport_sends_graph_edge_label_property_and_metadata_requests() {
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementRecord, TopologyRegistry};

        let placement = PlacementKey::default_for_database("copper");
        let mut topology = TopologyRegistry::new();
        topology
            .register_peer(
                MeshPeer::new("node-4", "node-4.mesh.local:50051")
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
        topology
            .register_placement(PlacementRecord {
                key: placement.clone(),
                primary_node: "node-4".into(),
                replica_nodes: vec![],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 0,
                search_fanout: 1,
            })
            .unwrap();
        let plan = topology
            .plan_read(&placement, ConsistencyLevel::One, None)
            .unwrap();

        let client = Arc::new(RecordingRemoteReplicaClient::default());
        *client.graph_edges_response.lock().unwrap() = vec![EdgeRecord {
            id: "edge-1".into(),
            start_node: "person-1".into(),
            end_node: "person-2".into(),
            edge_type: "KNOWS".into(),
            properties: Default::default(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 2,
        }];
        *client.graph_nodes_response.lock().unwrap() = vec![b"node-bytes".to_vec()];
        *client.graph_access_metadata_response.lock().unwrap() = Some(KnowledgePolicyAccessMetadata {
            last_accessed_at_unix_ms: Some(42),
            access_count: 7,
        });
        let transport = NornicGrpcReplicaTransport::from_read_plan(&plan, client.clone());

        let edges_from = transport
            .graph_edges_from_node("node-4", "person-1", Some("KNOWS"))
            .await
            .unwrap();
        let edges_to = transport
            .graph_edges_to_node("node-4", "person-2", None)
            .await
            .unwrap();
        let labeled = transport.graph_nodes_by_label("node-4", "Person").await.unwrap();
        let by_property = transport
            .graph_nodes_by_property("node-4", "Person", "name", &Value::String("Alice".into()))
            .await
            .unwrap();
        let metadata = transport
            .graph_access_metadata("node-4", "person-1")
            .await
            .unwrap();

        assert_eq!(edges_from.len(), 1);
        assert_eq!(edges_from[0].edge_type, "KNOWS");
        assert_eq!(edges_to.len(), 1);
        assert_eq!(labeled, vec![b"node-bytes".to_vec()]);
        assert_eq!(by_property, vec![b"node-bytes".to_vec()]);
        assert_eq!(
            metadata,
            Some(KnowledgePolicyAccessMetadata {
                last_accessed_at_unix_ms: Some(42),
                access_count: 7,
            })
        );

        let edges_from_requests = client.graph_edges_from.lock().unwrap();
        assert_eq!(edges_from_requests.len(), 1);
        assert_eq!(edges_from_requests[0].database, "copper");
        assert_eq!(edges_from_requests[0].node_id, "person-1");
        assert_eq!(edges_from_requests[0].rel_type.as_deref(), Some("KNOWS"));

        let edges_to_requests = client.graph_edges_to.lock().unwrap();
        assert_eq!(edges_to_requests.len(), 1);
        assert_eq!(edges_to_requests[0].database, "copper");
        assert_eq!(edges_to_requests[0].node_id, "person-2");
        assert!(edges_to_requests[0].rel_type.is_none());

        let label_requests = client.graph_nodes_by_label.lock().unwrap();
        assert_eq!(label_requests.len(), 1);
        assert_eq!(label_requests[0].database, "copper");
        assert_eq!(label_requests[0].label, "Person");

        let property_requests = client.graph_nodes_by_property.lock().unwrap();
        assert_eq!(property_requests.len(), 1);
        assert_eq!(property_requests[0].database, "copper");
        assert_eq!(property_requests[0].label, "Person");
        assert_eq!(property_requests[0].property, "name");
        assert_eq!(property_requests[0].value, Value::String("Alice".into()));

        let metadata_requests = client.graph_access_metadata.lock().unwrap();
        assert_eq!(metadata_requests.len(), 1);
        assert_eq!(metadata_requests[0].database, "copper");
        assert_eq!(metadata_requests[0].entity_id, "person-1");
    }

    #[tokio::test]
    async fn generated_replica_service_decodes_requests_and_encodes_responses() {
        let client = Arc::new(RecordingRemoteReplicaClient::default());
        *client.read_response.lock().unwrap() = Some(b"service-value".to_vec());
        let service = NornicReplicaService::new(client.clone());

        let apply_request = proto::RemoteReplicaApplyRequest::try_from(RemoteReplicaApplyRequest {
            target_node: "node-2".into(),
            target_addr: "node-2.mesh.local:50051".into(),
            command: Command::Put {
                key: b"service-write".to_vec(),
                value: b"ok".to_vec(),
            },
        })
        .unwrap();
        service
            .apply_replica(Request::new(apply_request))
            .await
            .unwrap();
        let read_response = service
            .read_replica(Request::new(proto::RemoteReplicaReadRequest {
                target_node: "node-3".into(),
                target_addr: "node-3.mesh.local:50051".into(),
                key: b"service-read".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(client.applies.lock().unwrap().len(), 1);
        assert_eq!(client.reads.lock().unwrap().len(), 1);
        assert!(read_response.found);
        assert_eq!(read_response.value, b"service-value".to_vec());
        let _server = service.into_server();
    }

    #[tokio::test]
    async fn generated_replica_service_handles_graph_node_rpc_and_forwards_caller_token() {
        let client = Arc::new(RecordingRemoteReplicaClient::default());
        *client.graph_node_response.lock().unwrap() = Some(b"graph-value".to_vec());
        let service = NornicReplicaService::new(client.clone());

        let mut request = Request::new(proto::RemoteGraphNodeRequest {
            target_node: "node-4".into(),
            target_addr: "node-4.mesh.local:50051".into(),
            database: "copper".into(),
            node_id: "person-1".into(),
        });
        request.metadata_mut().insert(
            GRPC_CALLER_AUTH_HEADER,
            MetadataValue::try_from("Bearer viewer-token").unwrap(),
        );

        let response = service.graph_node(request).await.unwrap().into_inner();

        assert!(response.found);
        assert_eq!(response.value, b"graph-value".to_vec());
        let requests = client.graph_nodes.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].database, "copper");
        assert_eq!(requests[0].node_id, "person-1");
        assert_eq!(requests[0].caller_auth_token.as_deref(), Some("viewer-token"));
    }

    #[tokio::test]
    async fn generated_replica_service_handles_graph_edges_and_metadata_rpcs() {
        let client = Arc::new(RecordingRemoteReplicaClient::default());
        *client.graph_edges_response.lock().unwrap() = vec![EdgeRecord {
            id: "edge-1".into(),
            start_node: "person-1".into(),
            end_node: "person-2".into(),
            edge_type: "KNOWS".into(),
            properties: Default::default(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 2,
        }];
        *client.graph_access_metadata_response.lock().unwrap() = Some(KnowledgePolicyAccessMetadata {
            last_accessed_at_unix_ms: Some(77),
            access_count: 3,
        });
        let service = NornicReplicaService::new(client.clone());

        let mut edges_request = Request::new(proto::RemoteGraphEdgesRequest {
            target_node: "node-4".into(),
            target_addr: "node-4.mesh.local:50051".into(),
            database: "copper".into(),
            node_id: "person-1".into(),
            rel_type: "KNOWS".into(),
            has_rel_type: true,
        });
        edges_request.metadata_mut().insert(
            GRPC_CALLER_AUTH_HEADER,
            MetadataValue::try_from("Bearer viewer-token").unwrap(),
        );
        let edges_response = service
            .graph_edges_from_node(edges_request)
            .await
            .unwrap()
            .into_inner();
        let decoded_edges = Vec::<EdgeRecord>::try_from(edges_response).unwrap();

        assert_eq!(decoded_edges.len(), 1);
        assert_eq!(decoded_edges[0].edge_type, "KNOWS");
        let edges_requests = client.graph_edges_from.lock().unwrap();
        assert_eq!(edges_requests.len(), 1);
        assert_eq!(edges_requests[0].caller_auth_token.as_deref(), Some("viewer-token"));
        assert_eq!(edges_requests[0].rel_type.as_deref(), Some("KNOWS"));

        let mut metadata_request = Request::new(proto::RemoteGraphAccessMetadataRequest {
            target_node: "node-4".into(),
            target_addr: "node-4.mesh.local:50051".into(),
            database: "copper".into(),
            entity_id: "person-1".into(),
        });
        metadata_request.metadata_mut().insert(
            GRPC_CALLER_AUTH_HEADER,
            MetadataValue::try_from("Bearer viewer-token").unwrap(),
        );
        let metadata_response = service
            .graph_access_metadata(metadata_request)
            .await
            .unwrap()
            .into_inner();
        let decoded_metadata =
            Option::<KnowledgePolicyAccessMetadata>::try_from(metadata_response).unwrap();

        assert_eq!(
            decoded_metadata,
            Some(KnowledgePolicyAccessMetadata {
                last_accessed_at_unix_ms: Some(77),
                access_count: 3,
            })
        );
        let metadata_requests = client.graph_access_metadata.lock().unwrap();
        assert_eq!(metadata_requests.len(), 1);
        assert_eq!(
            metadata_requests[0].caller_auth_token.as_deref(),
            Some("viewer-token")
        );
        assert_eq!(metadata_requests[0].entity_id, "person-1");
    }

    #[tokio::test]
    async fn ranked_search_transport_sends_requests_to_target_endpoint() {
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementRecord, TopologyRegistry};

        let placement = PlacementKey::new("default", "copper", "primary");
        let doc = copperdb_topology::FabricGlobalId::new(placement.clone(), "node", "a");
        let mut topology = TopologyRegistry::new();
        for node_id in ["search-a", "search-b"] {
            topology
                .register_peer(
                    MeshPeer::new(node_id, format!("{node_id}.mesh.local:50051"))
                        .with_capability(NodeCapability::Search),
                )
                .unwrap();
        }
        topology
            .register_placement(PlacementRecord {
                key: placement.clone(),
                primary_node: "search-a".into(),
                replica_nodes: vec![],
                search_nodes: vec!["search-a".into(), "search-b".into()],
                hyperscaler_profile: None,
                min_write_replicas: 0,
                search_fanout: 2,
            })
            .unwrap();
        let plan = topology.plan_search(&placement).unwrap();

        let client = Arc::new(RecordingRemoteRankedSearchClient::default());
        client.batches.lock().unwrap().insert(
            "search-a".into(),
            RrfSearchBatch {
                shard: placement.clone(),
                source: "lexical".into(),
                hits: vec![copperdb_search::RrfSearchHit {
                    global_id: doc,
                    rank: 1,
                    score: 0.8,
                    source: "lexical".into(),
                    shard: placement.clone(),
                    label: "Person".into(),
                    snippet: None,
                }],
            },
        );

        let transport = NornicGrpcRankedSearchTransport::from_search_plan(&plan, client.clone());
        let batch = transport
            .search_ranked_node(
                "search-a",
                &placement,
                &SearchQuery::FullText {
                    query: "alice".into(),
                    fields: vec!["body".into()],
                    limit: 10,
                },
            )
            .await
            .unwrap();

        assert_eq!(batch.shard, placement);
        assert_eq!(batch.source, "lexical");
        let requests = client.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].target_node, "search-a");
        assert_eq!(requests[0].target_addr, "search-a.mesh.local:50051");
        assert_eq!(requests[0].placement.database, "copper");
        match &requests[0].query {
            SearchQuery::FullText { query, .. } => assert_eq!(query, "alice"),
            other => panic!("expected full-text query, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn hydration_transport_sends_requests_to_target_endpoint() {
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementRecord, TopologyRegistry};

        let placement = PlacementKey::new("default", "copper", "primary");
        let doc = copperdb_topology::FabricGlobalId::new(placement.clone(), "node", "a");
        let mut topology = TopologyRegistry::new();
        topology
            .register_peer(
                MeshPeer::new("node-a", "node-a.mesh.local:50051")
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
        topology
            .register_placement(PlacementRecord {
                key: placement.clone(),
                primary_node: "node-a".into(),
                replica_nodes: vec![],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 0,
                search_fanout: 1,
            })
            .unwrap();
        let plan = topology
            .plan_read(&placement, ConsistencyLevel::One, None)
            .unwrap();

        let client = Arc::new(RecordingRemoteHydrationClient::default());
        client.records.lock().unwrap().insert(
            "node-a".into(),
            vec![RrfHydrationRecord {
                global_id: doc.clone(),
                labels: vec!["Person".into()],
                entity: serde_json::json!({"id": "a", "name": "Alice"}),
            }],
        );

        let transport = NornicGrpcHydrationTransport::from_read_plan(&plan, client.clone());
        let records = transport
            .hydrate_node("node-a", &placement, &[doc.clone()])
            .await
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].global_id, doc);
        let requests = client.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].target_node, "node-a");
        assert_eq!(requests[0].target_addr, "node-a.mesh.local:50051");
        assert_eq!(requests[0].placement, placement);
    }

    #[test]
    fn generated_proto_converts_ranked_search_messages() {
        let request = RemoteRankedSearchRequest {
            target_node: "search-a".into(),
            target_addr: "search-a.mesh.local:50051".into(),
            placement: PlacementKey::new("default", "copper", "primary"),
            query: SearchQuery::FullText {
                query: "alice".into(),
                fields: vec!["body".into()],
                limit: 10,
            },
            caller_auth_token: None,
        };
        let batch = RrfSearchBatch {
            shard: PlacementKey::new("default", "copper", "primary"),
            source: "lexical".into(),
            hits: vec![],
        };

        let proto_request = proto::RemoteRankedSearchRequest::try_from(request.clone()).unwrap();
        let decoded_request = RemoteRankedSearchRequest::try_from(proto_request).unwrap();
        let proto_response = proto::RemoteRankedSearchResponse::try_from(batch.clone()).unwrap();
        let decoded_batch = RrfSearchBatch::try_from(proto_response).unwrap();

        assert_eq!(decoded_request.target_node, request.target_node);
        assert_eq!(decoded_request.target_addr, request.target_addr);
        assert_eq!(decoded_request.placement, request.placement);
        assert_eq!(decoded_request.query, request.query);
        assert_eq!(decoded_batch, batch);
    }

    #[test]
    fn generated_proto_converts_hydration_messages() {
        let request = RemoteHydrationRequest {
            target_node: "node-a".into(),
            target_addr: "node-a.mesh.local:50051".into(),
            placement: PlacementKey::new("default", "copper", "primary"),
            global_ids: vec![FabricGlobalId::new(
                PlacementKey::new("default", "copper", "primary"),
                "node",
                "a",
            )],
            caller_auth_token: None,
        };
        let records = vec![RrfHydrationRecord {
            global_id: FabricGlobalId::new(
                PlacementKey::new("default", "copper", "primary"),
                "node",
                "a",
            ),
            labels: vec!["Person".into()],
            entity: serde_json::json!({"id": "a", "name": "Alice"}),
        }];

        let proto_request = proto::RemoteHydrationRequest::try_from(request.clone()).unwrap();
        let decoded_request = RemoteHydrationRequest::try_from(proto_request).unwrap();
        let proto_response = proto::RemoteHydrationResponse::try_from(records.clone()).unwrap();
        let decoded_records = Vec::<RrfHydrationRecord>::try_from(proto_response).unwrap();

        assert_eq!(decoded_request.target_node, request.target_node);
        assert_eq!(decoded_request.target_addr, request.target_addr);
        assert_eq!(decoded_request.placement, request.placement);
        assert_eq!(decoded_request.global_ids, request.global_ids);
        assert_eq!(decoded_records, records);
    }

    #[tokio::test]
    async fn generated_replica_service_handles_ranked_search_rpc() {
        let ranked = Arc::new(RecordingRemoteRankedSearchClient::default());
        let placement = PlacementKey::new("default", "copper", "primary");
        ranked.batches.lock().unwrap().insert(
            "search-a".into(),
            RrfSearchBatch {
                shard: placement.clone(),
                source: "lexical".into(),
                hits: vec![],
            },
        );
        let service = NornicReplicaService::new(Arc::new(RecordingRemoteReplicaClient::default()))
            .with_ranked_search_handler(ranked.clone());

        let response = service
            .search_ranked(Request::new(
                proto::RemoteRankedSearchRequest::try_from(RemoteRankedSearchRequest {
                    target_node: "search-a".into(),
                    target_addr: "search-a.mesh.local:50051".into(),
                    placement: placement.clone(),
                    query: SearchQuery::FullText {
                        query: "alice".into(),
                        fields: vec!["body".into()],
                        limit: 10,
                    },
                    caller_auth_token: None,
                })
                .unwrap(),
            ))
            .await
            .unwrap()
            .into_inner();

        let batch = RrfSearchBatch::try_from(response).unwrap();
        assert_eq!(batch.shard, placement);
        assert_eq!(ranked.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn generated_replica_service_handles_hydration_rpc() {
        let hydration = Arc::new(RecordingRemoteHydrationClient::default());
        let placement = PlacementKey::new("default", "copper", "primary");
        hydration.records.lock().unwrap().insert(
            "node-a".into(),
            vec![RrfHydrationRecord {
                global_id: FabricGlobalId::new(placement.clone(), "node", "a"),
                labels: vec!["Person".into()],
                entity: serde_json::json!({"id": "a"}),
            }],
        );
        let service = NornicReplicaService::new(Arc::new(RecordingRemoteReplicaClient::default()))
            .with_hydration_handler(hydration.clone());

        let response = service
            .hydrate_entities(Request::new(
                proto::RemoteHydrationRequest::try_from(RemoteHydrationRequest {
                    target_node: "node-a".into(),
                    target_addr: "node-a.mesh.local:50051".into(),
                    placement: placement.clone(),
                    global_ids: vec![FabricGlobalId::new(placement.clone(), "node", "a")],
                    caller_auth_token: None,
                })
                .unwrap(),
            ))
            .await
            .unwrap()
            .into_inner();

        let records = Vec::<RrfHydrationRecord>::try_from(response).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].global_id,
            FabricGlobalId::new(placement, "node", "a")
        );
        assert_eq!(hydration.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn generated_replica_service_rejects_missing_auth_token_when_validator_is_configured() {
        struct RejectViewerValidator;

        impl GrpcAuthValidator for RejectViewerValidator {
            fn validate(&self, token: &str) -> Result<(), GrpcError> {
                if token == "admin-token" {
                    Ok(())
                } else {
                    Err(GrpcError::Unauthenticated(
                        "missing or invalid gRPC authorization token".into(),
                    ))
                }
            }
        }

        let service = NornicReplicaService::new(Arc::new(RecordingRemoteReplicaClient::default()))
            .with_auth_validator(Arc::new(RejectViewerValidator));

        let error = service
            .apply_replica(Request::new(
                proto::RemoteReplicaApplyRequest::try_from(RemoteReplicaApplyRequest {
                    target_node: "node-a".into(),
                    target_addr: "node-a.mesh.local:50051".into(),
                    command: Command::Put {
                        key: b"auth-test".to_vec(),
                        value: b"denied".to_vec(),
                    },
                })
                .unwrap(),
            ))
            .await
            .unwrap_err();

        assert_eq!(error.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn tonic_ranked_search_client_forwards_caller_auth_token() {
        use tonic::transport::Server;

        let ranked = Arc::new(RecordingRemoteRankedSearchClient::default());
        let placement = PlacementKey::new("default", "copper", "primary");
        ranked.batches.lock().unwrap().insert(
            "search-a".into(),
            RrfSearchBatch {
                shard: placement.clone(),
                source: "lexical".into(),
                hits: vec![],
            },
        );

        let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let grpc_addr = reserved.local_addr().unwrap();
        drop(reserved);

        let service = NornicReplicaService::new(Arc::new(RecordingRemoteReplicaClient::default()))
            .with_ranked_search_handler(ranked.clone());
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(service.into_server())
                .serve(grpc_addr)
                .await
                .unwrap();
        });
        tokio::task::yield_now().await;

        let batch = TonicRemoteRankedSearchClient::new()
            .with_caller_auth_token("viewer-token")
            .search_ranked(RemoteRankedSearchRequest {
                target_node: "search-a".into(),
                target_addr: grpc_addr.to_string(),
                placement: placement.clone(),
                query: SearchQuery::FullText {
                    query: "alice".into(),
                    fields: vec!["body".into()],
                    limit: 10,
                },
                caller_auth_token: None,
            })
            .await
            .unwrap();

        assert_eq!(batch.shard, placement);
        let requests = ranked.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].caller_auth_token.as_deref(), Some("viewer-token"));
        server.abort();
    }

    #[tokio::test]
    async fn tonic_replica_client_forwards_caller_auth_token_for_graph_property_reads() {
        use tonic::transport::Server;

        let client = Arc::new(RecordingRemoteReplicaClient::default());
        *client.graph_nodes_response.lock().unwrap() = vec![b"graph-node".to_vec()];

        let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let grpc_addr = reserved.local_addr().unwrap();
        drop(reserved);

        let service = NornicReplicaService::new(client.clone());
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(service.into_server())
                .serve(grpc_addr)
                .await
                .unwrap();
        });
        tokio::task::yield_now().await;

        let nodes = TonicRemoteReplicaClient::new()
            .with_caller_auth_token("viewer-token")
            .graph_nodes_by_property(RemoteGraphNodesByPropertyRequest {
                target_node: "node-4".into(),
                target_addr: grpc_addr.to_string(),
                database: "copper".into(),
                label: "Person".into(),
                property: "name".into(),
                value: Value::String("Alice".into()),
                caller_auth_token: None,
            })
            .await
            .unwrap();

        assert_eq!(nodes, vec![b"graph-node".to_vec()]);
        let requests = client.graph_nodes_by_property.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].database, "copper");
        assert_eq!(requests[0].label, "Person");
        assert_eq!(requests[0].property, "name");
        assert_eq!(requests[0].value, Value::String("Alice".into()));
        assert_eq!(requests[0].caller_auth_token.as_deref(), Some("viewer-token"));
        server.abort();
    }

    #[tokio::test]
    async fn tonic_ranked_search_client_connects_over_tls() {
        use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

        ensure_tls_crypto_provider();
        let server_certified = rcgen::generate_simple_self_signed(["localhost".to_string()]).unwrap();
        let server_cert_pem = server_certified.cert.pem();
        let server_key_pem = server_certified.signing_key.serialize_pem();
        let client_certified = rcgen::generate_simple_self_signed(["client.mesh.local".to_string()]).unwrap();
        let client_cert_pem = client_certified.cert.pem();
        let client_key_pem = client_certified.signing_key.serialize_pem();

        let ranked = Arc::new(RecordingRemoteRankedSearchClient::default());
        let placement = PlacementKey::new("default", "copper", "primary");
        ranked.batches.lock().unwrap().insert(
            "search-a".into(),
            RrfSearchBatch {
                shard: placement.clone(),
                source: "lexical".into(),
                hits: vec![],
            },
        );

        let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let grpc_addr = reserved.local_addr().unwrap();
        drop(reserved);

        let service = NornicReplicaService::new(Arc::new(RecordingRemoteReplicaClient::default()))
            .with_ranked_search_handler(ranked.clone());
        let server_cert_pem_clone = server_cert_pem.clone();
        let client_cert_pem_clone = client_cert_pem.clone();
        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(
                    ServerTlsConfig::new()
                        .identity(Identity::from_pem(server_cert_pem_clone, server_key_pem))
                        .client_ca_root(Certificate::from_pem(client_cert_pem_clone)),
                )
                .unwrap()
                .add_service(service.into_server())
                .serve(grpc_addr)
                .await
                .unwrap();
        });
        tokio::task::yield_now().await;

        let batch = TonicRemoteRankedSearchClient::new()
            .with_tls_enabled(true)
            .with_tls_ca_certificate_pem(server_cert_pem)
            .with_tls_domain_name("localhost")
            .with_tls_identity_pem(client_cert_pem, client_key_pem)
            .search_ranked(RemoteRankedSearchRequest {
                target_node: "search-a".into(),
                target_addr: format!("localhost:{}", grpc_addr.port()),
                placement: placement.clone(),
                query: SearchQuery::FullText {
                    query: "alice".into(),
                    fields: vec!["body".into()],
                    limit: 10,
                },
                caller_auth_token: None,
            })
            .await
            .unwrap();

        assert_eq!(batch.shard, placement);
        assert_eq!(ranked.requests.lock().unwrap().len(), 1);
        server.abort();
    }

    #[test]
    fn generated_proto_converts_replica_apply_requests() {
        let request = RemoteReplicaApplyRequest {
            target_node: "node-2".into(),
            target_addr: "node-2.mesh.local:50051".into(),
            command: Command::CypherMutation {
                database: "copper".into(),
                query: "CREATE (n:Proto)".into(),
                params: serde_json::json!({"v": 1}),
            },
        };

        let proto_request = proto::RemoteReplicaApplyRequest::try_from(request.clone()).unwrap();
        let decoded = RemoteReplicaApplyRequest::try_from(proto_request).unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn generated_proto_converts_replica_read_messages() {
        let request = RemoteReplicaReadRequest {
            target_node: "node-3".into(),
            target_addr: "node-3.mesh.local:50051".into(),
            key: b"read-key".to_vec(),
        };

        let proto_request = proto::RemoteReplicaReadRequest::from(request.clone());
        let decoded = RemoteReplicaReadRequest::from(proto_request);
        let found = proto::RemoteReplicaReadResponse::from(Some(b"value".to_vec()));
        let missing = proto::RemoteReplicaReadResponse::from(None);

        assert_eq!(decoded, request);
        assert_eq!(Option::<Vec<u8>>::from(found), Some(b"value".to_vec()));
        assert_eq!(Option::<Vec<u8>>::from(missing), None);
    }

    #[test]
    fn generated_proto_converts_graph_read_messages() {
        let node_request = RemoteGraphNodeRequest {
            target_node: "node-4".into(),
            target_addr: "node-4.mesh.local:50051".into(),
            database: "copper".into(),
            node_id: "person-1".into(),
            caller_auth_token: Some("viewer-token".into()),
        };
        let property_request = RemoteGraphNodesByPropertyRequest {
            target_node: "node-4".into(),
            target_addr: "node-4.mesh.local:50051".into(),
            database: "copper".into(),
            label: "Person".into(),
            property: "name".into(),
            value: serde_json::json!("Alice"),
            caller_auth_token: Some("viewer-token".into()),
        };
        let edges = vec![EdgeRecord {
            id: "edge-1".into(),
            start_node: "person-1".into(),
            end_node: "person-2".into(),
            edge_type: "KNOWS".into(),
            properties: Default::default(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 2,
        }];
        let metadata = KnowledgePolicyAccessMetadata {
            last_accessed_at_unix_ms: Some(42),
            access_count: 2,
        };

        let proto_node = proto::RemoteGraphNodeRequest::from(node_request.clone());
        let decoded_node = RemoteGraphNodeRequest::from(proto_node);
        let proto_property =
            proto::RemoteGraphNodesByPropertyRequest::try_from(property_request.clone()).unwrap();
        let decoded_property = RemoteGraphNodesByPropertyRequest::try_from(proto_property).unwrap();
        let proto_edges = proto::RemoteGraphEdgesResponse::try_from(edges.clone()).unwrap();
        let decoded_edges = Vec::<EdgeRecord>::try_from(proto_edges).unwrap();
        let proto_metadata =
            proto::RemoteGraphAccessMetadataResponse::try_from(Some(metadata.clone())).unwrap();
        let decoded_metadata =
            Option::<KnowledgePolicyAccessMetadata>::try_from(proto_metadata).unwrap();

        assert_eq!(decoded_node.target_node, node_request.target_node);
        assert_eq!(decoded_node.target_addr, node_request.target_addr);
        assert_eq!(decoded_node.database, node_request.database);
        assert_eq!(decoded_node.node_id, node_request.node_id);
        assert!(decoded_node.caller_auth_token.is_none());
        assert_eq!(decoded_property.database, property_request.database);
        assert_eq!(decoded_property.label, property_request.label);
        assert_eq!(decoded_property.property, property_request.property);
        assert_eq!(decoded_property.value, property_request.value);
        assert_eq!(decoded_edges, edges);
        assert_eq!(decoded_metadata, Some(metadata));
    }

    #[test]
    fn tonic_remote_client_normalizes_endpoint_uris() {
        assert_eq!(
            TonicRemoteReplicaClient::endpoint_uri("node-1.mesh.local:50051", false),
            "http://node-1.mesh.local:50051"
        );
        assert_eq!(
            TonicRemoteReplicaClient::endpoint_uri("node-1.mesh.local:50051", true),
            "https://node-1.mesh.local:50051"
        );
        assert_eq!(
            TonicRemoteReplicaClient::endpoint_uri("https://node-1.mesh.local:50051", false),
            "https://node-1.mesh.local:50051"
        );
    }
}
