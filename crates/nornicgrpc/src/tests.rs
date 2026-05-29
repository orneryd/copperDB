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

    let value = transport
        .graph_node("node-4", "person-1", None)
        .await
        .unwrap();

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
        .graph_edges_from_node("node-4", "person-1", Some("KNOWS"), None)
        .await
        .unwrap();
    let edges_to = transport
        .graph_edges_to_node("node-4", "person-2", None, None)
        .await
        .unwrap();
    let labeled = transport
        .graph_nodes_by_label("node-4", "Person", None)
        .await
        .unwrap();
    let by_property = transport
        .graph_nodes_by_property(
            "node-4",
            "Person",
            "name",
            &Value::String("Alice".into()),
            None,
        )
        .await
        .unwrap();
    let metadata = transport
        .graph_access_metadata("node-4", "person-1", None)
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
        read_fence: String::new(),
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
    assert_eq!(
        requests[0].caller_auth_token.as_deref(),
        Some("viewer-token")
    );
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
        read_fence: String::new(),
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
    {
        let edges_requests = client.graph_edges_from.lock().unwrap();
        assert_eq!(edges_requests.len(), 1);
        assert_eq!(
            edges_requests[0].caller_auth_token.as_deref(),
            Some("viewer-token")
        );
        assert_eq!(edges_requests[0].rel_type.as_deref(), Some("KNOWS"));
    }

    let mut metadata_request = Request::new(proto::RemoteGraphAccessMetadataRequest {
        target_node: "node-4".into(),
        target_addr: "node-4.mesh.local:50051".into(),
        database: "copper".into(),
        entity_id: "person-1".into(),
        read_fence: String::new(),
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
            None,
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
        .hydrate_node("node-a", &placement, std::slice::from_ref(&doc), None)
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
        read_fence: None,
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
    assert_eq!(decoded_request.read_fence, request.read_fence);
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
        read_fence: None,
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
    assert_eq!(decoded_request.read_fence, request.read_fence);
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
                read_fence: None,
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
                read_fence: None,
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
        tonic::transport::Server::builder()
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
            read_fence: None,
            caller_auth_token: None,
        })
        .await
        .unwrap();

    assert_eq!(batch.shard, placement);
    let requests = ranked.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].caller_auth_token.as_deref(),
        Some("viewer-token")
    );
    server.abort();
}

#[tokio::test]
async fn tonic_replica_client_forwards_caller_auth_token_for_graph_property_reads() {
    let client = Arc::new(RecordingRemoteReplicaClient::default());
    *client.graph_nodes_response.lock().unwrap() = vec![b"graph-node".to_vec()];

    let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let grpc_addr = reserved.local_addr().unwrap();
    drop(reserved);

    let service = NornicReplicaService::new(client.clone());
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
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
            read_fence: None,
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
    assert_eq!(
        requests[0].caller_auth_token.as_deref(),
        Some("viewer-token")
    );
    server.abort();
}

#[tokio::test]
async fn tonic_ranked_search_client_connects_over_tls() {
    use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

    ensure_tls_crypto_provider();
    let server_certified = rcgen::generate_simple_self_signed(["localhost".to_string()]).unwrap();
    let server_cert_pem = server_certified.cert.pem();
    let server_key_pem = server_certified.signing_key.serialize_pem();
    let client_certified =
        rcgen::generate_simple_self_signed(["client.mesh.local".to_string()]).unwrap();
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
            read_fence: None,
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
        read_fence: Some(LogicalTransactionId::new(7, 41, 9)),
        caller_auth_token: Some("viewer-token".into()),
    };
    let property_request = RemoteGraphNodesByPropertyRequest {
        target_node: "node-4".into(),
        target_addr: "node-4.mesh.local:50051".into(),
        database: "copper".into(),
        label: "Person".into(),
        property: "name".into(),
        value: serde_json::json!("Alice"),
        read_fence: Some(LogicalTransactionId::new(7, 42, 9)),
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
    assert_eq!(decoded_node.read_fence, node_request.read_fence);
    assert!(decoded_node.caller_auth_token.is_none());
    assert_eq!(decoded_property.database, property_request.database);
    assert_eq!(decoded_property.label, property_request.label);
    assert_eq!(decoded_property.property, property_request.property);
    assert_eq!(decoded_property.value, property_request.value);
    assert_eq!(decoded_property.read_fence, property_request.read_fence);
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
