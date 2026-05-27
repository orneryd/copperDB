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
use copperdb_storage::EdgeRecord;
use copperdb_topology::{
    ConsistencyLevel, DistributedReadPlan, DistributedSearchPlan, DistributedWriteMode,
    DistributedWritePlan, PlacementKey,
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

#[derive(Clone)]
pub struct NornicReplicaService {
    handler: Arc<dyn RemoteReplicaClient>,
}

impl NornicReplicaService {
    pub fn new(handler: Arc<dyn RemoteReplicaClient>) -> Self {
        Self { handler }
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
}

#[derive(Debug, Clone, Default)]
pub struct TonicRemoteReplicaClient;

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
