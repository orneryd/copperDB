//! gRPC server interface for copperdb.
//!
//! Equivalent to Go's `pkg/nornicgrpc` in NornicDB.
//! Exposes a Protobuf/gRPC API as an alternative to the Bolt protocol.
//! Uses `tonic` (Rust gRPC) + `prost` (Protobuf codegen).

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

use copperdb_replication::{Command, ReplicaTransport, ReplicationError};
use copperdb_search::{
    HydrationTransport, RankedSearchTransport, RrfHydrationRecord, RrfSearchBatch, SearchError,
    SearchQuery,
};
use copperdb_storage::EdgeRecord;
use copperdb_topology::{
    ConsistencyLevel, DistributedReadPlan, DistributedSearchPlan, DistributedWriteMode,
    DistributedWritePlan, FabricGlobalId, PlacementKey,
};
use serde_json::Value;

pub mod proto {
    tonic::include_proto!("copperdb.nornic.v1");
}

#[derive(Debug, Error)]
pub enum GrpcError {
    #[error("gRPC transport error: {0}")]
    Transport(String),
    #[error("proto encoding error: {0}")]
    Encoding(String),
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

#[async_trait]
pub trait RemoteReplicaClient: Send + Sync {
    async fn apply_replica(&self, request: RemoteReplicaApplyRequest) -> Result<(), GrpcError>;
    async fn read_replica(
        &self,
        request: RemoteReplicaReadRequest,
    ) -> Result<Option<Vec<u8>>, GrpcError>;
}

pub struct NornicGrpcReplicaTransport {
    endpoints: HashMap<String, String>,
    client: Arc<dyn RemoteReplicaClient>,
}

#[derive(Debug, Clone)]
pub struct RemoteRankedSearchRequest {
    pub target_node: String,
    pub target_addr: String,
    pub placement: PlacementKey,
    pub query: SearchQuery,
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
        }
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
        let response = self
            .handler
            .read_replica(RemoteReplicaReadRequest::from(request.into_inner()))
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(proto::RemoteReplicaReadResponse::from(
            response,
        )))
    }

    async fn search_ranked(
        &self,
        request: Request<proto::RemoteRankedSearchRequest>,
    ) -> Result<Response<proto::RemoteRankedSearchResponse>, Status> {
        let request = RemoteRankedSearchRequest::try_from(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let response = self
            .ranked_search_handler
            .search_ranked(request)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        let response = proto::RemoteRankedSearchResponse::try_from(response)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(response))
    }

    async fn hydrate_entities(
        &self,
        request: Request<proto::RemoteHydrationRequest>,
    ) -> Result<Response<proto::RemoteHydrationResponse>, Status> {
        let request = RemoteHydrationRequest::try_from(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let response = self
            .hydration_handler
            .hydrate_entities(request)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        let response = proto::RemoteHydrationResponse::try_from(response)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(response))
    }
}

#[derive(Debug, Clone, Default)]
pub struct TonicRemoteReplicaClient;

#[derive(Debug, Clone, Default)]
pub struct TonicRemoteRankedSearchClient;

#[derive(Debug, Clone, Default)]
pub struct TonicRemoteHydrationClient;

impl TonicRemoteReplicaClient {
    pub fn new() -> Self {
        Self
    }

    fn endpoint_uri(target_addr: &str) -> String {
        if target_addr.starts_with("http://") || target_addr.starts_with("https://") {
            target_addr.into()
        } else {
            format!("http://{target_addr}")
        }
    }

    async fn connect(
        target_addr: &str,
    ) -> Result<proto::nornic_replica_client::NornicReplicaClient<Channel>, GrpcError> {
        let endpoint = Endpoint::from_shared(Self::endpoint_uri(target_addr))
            .map_err(|error| GrpcError::Transport(error.to_string()))?;
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
        Self
    }
}

impl TonicRemoteHydrationClient {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl RemoteReplicaClient for TonicRemoteReplicaClient {
    async fn apply_replica(&self, request: RemoteReplicaApplyRequest) -> Result<(), GrpcError> {
        let target_addr = request.target_addr.clone();
        let proto_request = proto::RemoteReplicaApplyRequest::try_from(request)?;
        Self::connect(&target_addr)
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
        let response = Self::connect(&target_addr)
            .await?
            .read_replica(proto::RemoteReplicaReadRequest::from(request))
            .await
            .map_err(|error| GrpcError::Transport(error.to_string()))?
            .into_inner();
        Ok(Option::<Vec<u8>>::from(response))
    }
}

#[async_trait]
impl RemoteRankedSearchClient for TonicRemoteRankedSearchClient {
    async fn search_ranked(
        &self,
        request: RemoteRankedSearchRequest,
    ) -> Result<RrfSearchBatch, GrpcError> {
        let target_addr = request.target_addr.clone();
        let response = TonicRemoteReplicaClient::connect(&target_addr)
            .await?
            .search_ranked(proto::RemoteRankedSearchRequest::try_from(request)?)
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
        let response = TonicRemoteReplicaClient::connect(&target_addr)
            .await?
            .hydrate_entities(proto::RemoteHydrationRequest::try_from(request)?)
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
            client,
        }
    }

    pub fn from_write_plan(
        plan: &DistributedWritePlan,
        client: Arc<dyn RemoteReplicaClient>,
    ) -> Self {
        Self::new(
            plan.replicas
                .iter()
                .map(|peer| (peer.node_id.clone(), peer.advertise_addr.clone())),
            client,
        )
    }

    pub fn from_read_plan(
        plan: &DistributedReadPlan,
        client: Arc<dyn RemoteReplicaClient>,
    ) -> Self {
        Self::new(
            plan.replicas
                .iter()
                .map(|peer| (peer.node_id.clone(), peer.advertise_addr.clone())),
            client,
        )
    }

    fn endpoint_for(&self, target: &str) -> Result<String, ReplicationError> {
        self.endpoints
            .get(target)
            .cloned()
            .ok_or_else(|| ReplicationError::Transport(format!("unknown remote replica {target}")))
    }

    fn graph_read_unavailable(&self, target: &str, operation: &str) -> ReplicationError {
        ReplicationError::Transport(format!(
            "remote graph read operation {operation} is not implemented for nornic gRPC replica {target}"
        ))
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
        _node_id: &str,
    ) -> Result<Option<Vec<u8>>, ReplicationError> {
        self.endpoint_for(target)?;
        Err(self.graph_read_unavailable(target, "graph_node"))
    }

    async fn graph_edges_from_node(
        &self,
        target: &str,
        _node_id: &str,
        _rel_type: Option<&str>,
    ) -> Result<Vec<EdgeRecord>, ReplicationError> {
        self.endpoint_for(target)?;
        Err(self.graph_read_unavailable(target, "graph_edges_from_node"))
    }

    async fn graph_edges_to_node(
        &self,
        target: &str,
        _node_id: &str,
        _rel_type: Option<&str>,
    ) -> Result<Vec<EdgeRecord>, ReplicationError> {
        self.endpoint_for(target)?;
        Err(self.graph_read_unavailable(target, "graph_edges_to_node"))
    }

    async fn graph_nodes_by_label(
        &self,
        target: &str,
        _label: &str,
    ) -> Result<Vec<Vec<u8>>, ReplicationError> {
        self.endpoint_for(target)?;
        Err(self.graph_read_unavailable(target, "graph_nodes_by_label"))
    }

    async fn graph_nodes_by_property(
        &self,
        target: &str,
        _label: &str,
        _property: &str,
        _value: &Value,
    ) -> Result<Vec<Vec<u8>>, ReplicationError> {
        self.endpoint_for(target)?;
        Err(self.graph_read_unavailable(target, "graph_nodes_by_property"))
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
    fn tonic_remote_client_normalizes_endpoint_uris() {
        assert_eq!(
            TonicRemoteReplicaClient::endpoint_uri("node-1.mesh.local:50051"),
            "http://node-1.mesh.local:50051"
        );
        assert_eq!(
            TonicRemoteReplicaClient::endpoint_uri("https://node-1.mesh.local:50051"),
            "https://node-1.mesh.local:50051"
        );
    }
}
