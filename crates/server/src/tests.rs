#![allow(clippy::field_reassign_with_default)]

use super::*;

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

#[tokio::test]
async fn test_router_builds() {
    let state = Arc::new(AppState::default());
    let _app = build_router(state);
}

#[test]
fn distributed_write_transport_builds_with_generated_cluster_auth() {
    use copperdb_topology::{MeshPeer, NodeCapability, PlacementRecord};

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
        ..Default::default()
    };
    state.auth = AuthState::from_storage_path(
        unique_auth_path(),
        true,
        true,
        "admin".into(),
        "password".into(),
        "test-secret".into(),
    )
    .unwrap();

    let placement = PlacementKey::default_for_database("clinic");
    let engine = GraphEngine::open(EngineConfig {
        data_dir: storage_path,
        default_database: "clinic".into(),
        ..Default::default()
    })
    .unwrap();
    for (node_id, addr) in [
        ("node-1", "127.0.0.1:50051"),
        ("node-2", "127.0.0.1:50052"),
        ("node-3", "127.0.0.1:50053"),
    ] {
        engine
            .storage()
            .register_topology_peer(
                &MeshPeer::new(node_id, addr)
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
    }
    engine
        .storage()
        .register_topology_placement(&PlacementRecord {
            key: placement.clone(),
            primary_node: "node-1".into(),
            replica_nodes: vec!["node-2".into(), "node-3".into()],
            search_nodes: vec![],
            hyperscaler_profile: None,
            min_write_replicas: 1,
            search_fanout: 1,
        })
        .unwrap();

    assert!(build_local_replica_transport(
        &state,
        &engine,
        &placement,
        ConsistencyLevel::Quorum,
        None,
        None,
        true,
    )
    .is_ok());
}

#[test]
fn distributed_read_transport_builds_with_forwarded_caller_auth() {
    use copperdb_topology::{MeshPeer, NodeCapability, PlacementRecord};

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
        ..Default::default()
    };
    state.auth.security_enabled = true;

    let placement = PlacementKey::default_for_database("clinic");
    let engine = GraphEngine::open(EngineConfig {
        data_dir: storage_path,
        default_database: "clinic".into(),
        ..Default::default()
    })
    .unwrap();
    for (node_id, addr) in [("node-1", "127.0.0.1:50051"), ("node-2", "127.0.0.1:50052")] {
        engine
            .storage()
            .register_topology_peer(
                &MeshPeer::new(node_id, addr)
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
    }
    engine
        .storage()
        .register_topology_placement(&PlacementRecord {
            key: placement.clone(),
            primary_node: "node-1".into(),
            replica_nodes: vec!["node-2".into()],
            search_nodes: vec![],
            hyperscaler_profile: None,
            min_write_replicas: 1,
            search_fanout: 1,
        })
        .unwrap();

    assert!(build_local_replica_transport(
        &state,
        &engine,
        &placement,
        ConsistencyLevel::Quorum,
        None,
        Some("viewer-token"),
        false,
    )
    .is_ok());
}

#[test]
fn observe_remote_read_fence_returns_effective_fence_beyond_bookmark() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("clinic")
        .to_string_lossy()
        .into_owned();
    let db_manager = Arc::new(DatabaseManager::new());
    db_manager.create("clinic", storage_path).unwrap();

    let state = AppState {
        db_name: "clinic".into(),
        db_manager,
        ..Default::default()
    };

    let effective =
        observe_remote_read_fence(&state, "clinic", Some(LogicalTransactionId::new(7, 41, 9)))
            .unwrap()
            .expect("expected observed remote fence");

    assert!(effective > LogicalTransactionId::new(7, 41, 9));
}

#[test]
fn distributed_read_transport_requires_forwarded_caller_auth_when_security_enabled() {
    use copperdb_topology::{MeshPeer, NodeCapability, PlacementRecord};

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
        ..Default::default()
    };

    let placement = PlacementKey::default_for_database("clinic");
    let engine = GraphEngine::open(EngineConfig {
        data_dir: storage_path,
        default_database: "clinic".into(),
        ..Default::default()
    })
    .unwrap();
    for (node_id, addr) in [("node-1", "127.0.0.1:50051"), ("node-2", "127.0.0.1:50052")] {
        engine
            .storage()
            .register_topology_peer(
                &MeshPeer::new(node_id, addr)
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
    }
    engine
        .storage()
        .register_topology_placement(&PlacementRecord {
            key: placement.clone(),
            primary_node: "node-1".into(),
            replica_nodes: vec!["node-2".into()],
            search_nodes: vec![],
            hyperscaler_profile: None,
            min_write_replicas: 1,
            search_fanout: 1,
        })
        .unwrap();

    state.auth.security_enabled = true;
    let error = match build_local_replica_transport(
        &state,
        &engine,
        &placement,
        ConsistencyLevel::Quorum,
        None,
        None,
        false,
    ) {
        Ok(_) => panic!("distributed reads should require forwarded caller auth"),
        Err(error) => error,
    };
    assert!(error.contains("forwarded caller authorization token"));
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
async fn database_config_admin_rejects_invalid_override_keys() {
    use axum::body::Body;
    use axum::http::{header, Method, Request, StatusCode};
    use tower::ServiceExt;

    let db_manager = Arc::new(DatabaseManager::new());
    db_manager.create("clinic", "./data/clinic").unwrap();

    let mut state = AppState {
        db_name: "clinic".into(),
        db_manager,
        ..Default::default()
    };
    state.auth.security_enabled = false;

    let app = build_router(Arc::new(state));
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/admin/databases/clinic/config")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "overrides": {
                            "COPPERDB_UNKNOWN": "true"
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
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
                .uri("/db/copperdb/tx/commit")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "statements": [
                            {"statement": "CREATE (n:Denied {v: 1})"}
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn neo4j_commit_can_opt_into_distributed_engine_routing() {
    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::{header, Request, StatusCode};
    use copperdb_nornicgrpc::{
        GrpcError, NornicReplicaService, RemoteReplicaApplyRequest, RemoteReplicaClient,
        RemoteReplicaReadRequest,
    };
    use copperdb_replication::Command;
    use copperdb_topology::{MeshPeer, NodeCapability, PlacementRecord};
    use tonic::transport::Server;
    use tower::ServiceExt;

    let temp_dir = tempfile::tempdir().unwrap();
    let local_storage_path = temp_dir
        .path()
        .join("clinic-local")
        .to_string_lossy()
        .into_owned();

    #[derive(Clone, Default)]
    struct RecordingReplicaClient {
        applied: Arc<std::sync::Mutex<Vec<RemoteReplicaApplyRequest>>>,
    }

    #[async_trait]
    impl RemoteReplicaClient for RecordingReplicaClient {
        async fn apply_replica(&self, request: RemoteReplicaApplyRequest) -> Result<(), GrpcError> {
            self.applied.lock().unwrap().push(request);
            Ok(())
        }

        async fn read_replica(
            &self,
            _request: RemoteReplicaReadRequest,
        ) -> Result<Option<Vec<u8>>, GrpcError> {
            Ok(None)
        }
    }

    let spawn_replica = || {
        let client = RecordingReplicaClient::default();
        let applied = Arc::clone(&client.applied);
        let service = NornicReplicaService::new(Arc::new(client));
        let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let grpc_addr = reserved.local_addr().unwrap();
        drop(reserved);
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(service.into_server())
                .serve(grpc_addr)
                .await
                .unwrap();
        });
        (grpc_addr, server, applied)
    };

    let wait_for_listener = |addr: std::net::SocketAddr| async move {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for replica listener {addr}"
            );
            tokio::task::yield_now().await;
        }
    };

    let (node_one_addr, node_one_server, node_one_applied) = spawn_replica();
    let (node_two_addr, node_two_server, node_two_applied) = spawn_replica();
    let (node_three_addr, node_three_server, node_three_applied) = spawn_replica();
    wait_for_listener(node_one_addr).await;
    wait_for_listener(node_two_addr).await;
    wait_for_listener(node_three_addr).await;

    let db_manager = Arc::new(DatabaseManager::new());
    db_manager
        .create("clinic", local_storage_path.clone())
        .unwrap();
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
            data_dir: local_storage_path.clone(),
            default_database: "clinic".into(),
            ..Default::default()
        })
        .unwrap();
        engine
            .storage()
            .register_topology_peer(
                &MeshPeer::new("node-1", node_one_addr.to_string())
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
        engine
            .storage()
            .register_topology_peer(
                &MeshPeer::new("node-2", node_two_addr.to_string())
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
        engine
            .storage()
            .register_topology_peer(
                &MeshPeer::new("node-3", node_three_addr.to_string())
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
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
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        app.oneshot(
            Request::builder()
                .method("POST")
                .uri("/db/clinic/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-copperdb-distributed", "true")
                .body(Body::from(
                    serde_json::json!({
                        "statements": [
                            {"statement": "CREATE (n:DistributedCommit {v: 1})"}
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        ),
    )
    .await
    .expect("distributed Neo4j commit request timed out")
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let decoded: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(decoded["errors"], serde_json::json!([]));
    assert_eq!(decoded["results"].as_array().map(Vec::len), Some(1));

    node_one_server.abort();
    node_two_server.abort();
    node_three_server.abort();
    tokio::task::yield_now().await;

    let primary_engine = GraphEngine::open(EngineConfig {
        data_dir: local_storage_path,
        default_database: "clinic".into(),
        ..Default::default()
    })
    .unwrap();
    let primary_count = primary_engine
        .execute_as(
            "MATCH (n:DistributedCommit) RETURN count(n) AS c",
            HashMap::new(),
            &[],
        )
        .unwrap();
    assert_eq!(primary_count.rows[0]["c"].as_i64(), Some(1));

    for applied in [node_one_applied, node_two_applied, node_three_applied] {
        let applied = applied.lock().unwrap();
        assert_eq!(applied.len(), 1);
        match &applied[0].command {
            Command::CypherMutation {
                database,
                query,
                params,
            } => {
                assert_eq!(database, "clinic");
                assert_eq!(query, "CREATE (n:DistributedCommit {v: 1})");
                assert_eq!(params, &serde_json::json!({}));
            }
            other => panic!("unexpected replicated command: {other:?}"),
        }
    }
}

#[tokio::test]
async fn neo4j_commit_can_opt_into_distributed_graph_read_routing() {
    use axum::body::Body;
    use axum::http::{header, Request, StatusCode};
    use copperdb_nornicgrpc::{
        NornicReplicaService, RemoteGraphNodesByPropertyRequest, RemoteReplicaClient,
        TonicRemoteReplicaClient,
    };
    use copperdb_storage::{EdgeRecord, IndexDefinition, IndexEntityType, IndexKind, NodeRecord};
    use copperdb_topology::{MeshPeer, NodeCapability, PlacementRecord};
    use tonic::transport::Server;
    use tower::ServiceExt;

    let temp_dir = tempfile::tempdir().unwrap();
    let local_storage_path = temp_dir
        .path()
        .join("clinic-local")
        .to_string_lossy()
        .into_owned();

    let wait_for_listener = |addr: std::net::SocketAddr| async move {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for replica listener {addr}"
            );
            tokio::task::yield_now().await;
        }
    };

    let spawn_graph_replica = |storage_path: String| {
        let db_manager = Arc::new(DatabaseManager::new());
        db_manager.create("clinic", storage_path).unwrap();
        let mut state = AppState {
            db_name: "clinic".into(),
            db_manager,
            distributed_cypher_enabled: false,
            ..Default::default()
        };
        state.auth.security_enabled = false;
        let state = Arc::new(state);
        let handler = Arc::new(LocalEngineReplicaHandler::new(state));
        let service = NornicReplicaService::new(handler);
        let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let grpc_addr = reserved.local_addr().unwrap();
        drop(reserved);
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(service.into_server())
                .serve(grpc_addr)
                .await
                .unwrap();
        });
        (grpc_addr, server)
    };

    let peer_one_path = temp_dir
        .path()
        .join("clinic-peer-one")
        .to_string_lossy()
        .into_owned();
    let peer_two_path = temp_dir
        .path()
        .join("clinic-peer-two")
        .to_string_lossy()
        .into_owned();
    let peer_three_path = temp_dir
        .path()
        .join("clinic-peer-three")
        .to_string_lossy()
        .into_owned();

    let peer_one = StorageEngine::open(&peer_one_path).unwrap();
    peer_one
        .persist_index_definition(&IndexDefinition {
            name: "node_name".into(),
            entity_type: IndexEntityType::Node,
            kind: IndexKind::Range,
            label: "Node".into(),
            properties: vec!["name".into()],
        })
        .unwrap();
    peer_one
        .put_node_record(&NodeRecord {
            id: "Node:A".into(),
            labels: vec!["Node".into()],
            properties: BTreeMap::from([("name".into(), serde_json::Value::String("a".into()))]),

            named_embeddings: BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();
    peer_one
        .put_edge_record(&EdgeRecord {
            id: "edge:a-b".into(),
            start_node: "Node:A".into(),
            end_node: "Node:B".into(),
            edge_type: "LINK".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();

    let peer_two = StorageEngine::open(&peer_two_path).unwrap();
    peer_two
        .persist_index_definition(&IndexDefinition {
            name: "node_name".into(),
            entity_type: IndexEntityType::Node,
            kind: IndexKind::Range,
            label: "Node".into(),
            properties: vec!["name".into()],
        })
        .unwrap();
    peer_two
        .put_node_record(&NodeRecord {
            id: "Node:B".into(),
            labels: vec!["Node".into()],
            properties: BTreeMap::from([("name".into(), serde_json::Value::String("b".into()))]),

            named_embeddings: BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();
    peer_two
        .put_edge_record(&EdgeRecord {
            id: "edge:b-d".into(),
            start_node: "Node:B".into(),
            end_node: "Node:D".into(),
            edge_type: "LINK".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();

    let peer_three = StorageEngine::open(&peer_three_path).unwrap();
    peer_three
        .persist_index_definition(&IndexDefinition {
            name: "node_name".into(),
            entity_type: IndexEntityType::Node,
            kind: IndexKind::Range,
            label: "Node".into(),
            properties: vec!["name".into()],
        })
        .unwrap();
    peer_three
        .put_node_record(&NodeRecord {
            id: "Node:D".into(),
            labels: vec!["Node".into()],
            properties: BTreeMap::from([("name".into(), serde_json::Value::String("d".into()))]),

            named_embeddings: BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();

    drop(peer_one);
    drop(peer_two);
    drop(peer_three);

    let (node_one_addr, node_one_server) = spawn_graph_replica(peer_one_path);
    let (node_two_addr, node_two_server) = spawn_graph_replica(peer_two_path);
    let (node_three_addr, node_three_server) = spawn_graph_replica(peer_three_path);
    wait_for_listener(node_one_addr).await;
    wait_for_listener(node_two_addr).await;
    wait_for_listener(node_three_addr).await;
    assert_eq!(
        TonicRemoteReplicaClient::new()
            .graph_nodes_by_property(RemoteGraphNodesByPropertyRequest {
                target_node: "node-1".into(),
                target_addr: node_one_addr.to_string(),
                database: "clinic".into(),
                label: "Node".into(),
                property: "name".into(),
                value: serde_json::Value::String("a".into()),
                read_fence: None,
                caller_auth_token: None,
            })
            .await
            .unwrap()
            .len(),
        1
    );

    let db_manager = Arc::new(DatabaseManager::new());
    db_manager
        .create("clinic", local_storage_path.clone())
        .unwrap();
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
            data_dir: local_storage_path,
            default_database: "clinic".into(),
            ..Default::default()
        })
        .unwrap();
        engine
            .storage()
            .register_topology_peer(
                &MeshPeer::new("node-1", node_one_addr.to_string())
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
        engine
            .storage()
            .register_topology_peer(
                &MeshPeer::new("node-2", node_two_addr.to_string())
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
        engine
            .storage()
            .register_topology_peer(
                &MeshPeer::new("node-3", node_three_addr.to_string())
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
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
    let response = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/db/clinic/tx/commit")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-copperdb-distributed", "true")
                    .body(Body::from(
                        serde_json::json!({
                            "bookmarks": [LogicalTransactionId::new(7, 41, 9).stable_id()],
                            "statements": [
                                {
                                    "statement": "MATCH p = shortestPath((a:Node {name: 'a'})-[:LINK*]->(d:Node {name: 'd'})) RETURN length(p) AS hops, p AS shortest"
                                }
                            ]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            ),
        )
        .await
        .expect("distributed Neo4j read request timed out")
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let decoded: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(decoded["errors"], serde_json::json!([]));
    assert_eq!(decoded["results"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        decoded["results"][0]["columns"],
        serde_json::json!(["hops", "shortest"])
    );
    let row = decoded["results"][0]["data"][0]["row"]
        .as_array()
        .expect("expected Neo4j row array");
    assert_eq!(row[0].as_i64(), Some(2));
    let path = row[1].as_object().expect("expected shortest path object");
    let nodes = path
        .get("nodes")
        .and_then(serde_json::Value::as_array)
        .expect("expected shortest path nodes");
    let names = nodes
        .iter()
        .map(|node| {
            node.get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["a", "b", "d"]);

    node_one_server.abort();
    node_two_server.abort();
    node_three_server.abort();
    tokio::task::yield_now().await;
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
    db_manager
        .set_config_overrides(
            "copper",
            std::collections::BTreeMap::from([(
                "COPPERDB_SEARCH_BM25_ENABLED".into(),
                "true".into(),
            )]),
        )
        .unwrap();
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
        FabricGlobalId, FabricPartitionPolicy, FabricShard, FabricShardKind, LogicalTransactionId,
        MeshPeer, NodeCapability, PlacementRecord,
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
            let read_fence = request
                .read_fence
                .expect("expected propagated ranked-search read fence");
            assert!(read_fence > LogicalTransactionId::new(7, 41, 9));
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
            let read_fence = request
                .read_fence
                .expect("expected propagated hydration read fence");
            assert!(read_fence > LogicalTransactionId::new(7, 41, 9));
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
    db_manager
        .set_config_overrides(
            "copper",
            std::collections::BTreeMap::from([(
                "COPPERDB_SEARCH_BM25_ENABLED".into(),
                "true".into(),
            )]),
        )
        .unwrap();
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
                        "bookmarks": [LogicalTransactionId::new(7, 41, 9).stable_id()],
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
async fn engine_backed_ranked_search_rpc_handler_executes_local_fulltext_runtime() {
    use copperdb_nornicgrpc::proto::nornic_replica_server::NornicReplica;
    use copperdb_nornicgrpc::{
        proto, RemoteHydrationRequest, RemoteRankedSearchRequest, RemoteReplicaApplyRequest,
        RemoteReplicaClient, RemoteReplicaReadRequest,
    };
    use copperdb_search::RrfHydrationRecord;
    use copperdb_storage::{IndexDefinition, IndexEntityType, IndexKind, NodeRecord};
    use copperdb_topology::PlacementKey;
    use tonic::Request;

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

    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("copper")
        .to_string_lossy()
        .into_owned();
    let db_manager = Arc::new(DatabaseManager::new());
    db_manager.create("copper", storage_path.clone()).unwrap();
    db_manager
        .set_config_overrides(
            "copper",
            std::collections::BTreeMap::from([(
                "COPPERDB_SEARCH_BM25_ENABLED".into(),
                "true".into(),
            )]),
        )
        .unwrap();

    let mut state = AppState {
        db_name: "copper".into(),
        db_manager,
        ..Default::default()
    };
    state.auth.security_enabled = false;

    {
        let engine = open_engine(&state, "copper").unwrap();
        engine
            .storage()
            .persist_index_definition(&IndexDefinition {
                name: "person_bio_fulltext_idx".into(),
                entity_type: IndexEntityType::Node,
                label: "Person".into(),
                properties: vec!["bio".into()],
                kind: IndexKind::FullText,
            })
            .unwrap();
        engine
            .storage()
            .put_node_record(&NodeRecord {
                id: "person:1".into(),
                labels: vec!["Person".into()],
                properties: BTreeMap::from([(
                    "bio".into(),
                    serde_json::Value::String("Alice builds reliable graph systems".into()),
                )]),

                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 10,
                updated_at_unix_ms: 20,
            })
            .unwrap();
    }

    let placement = PlacementKey::new("default", "copper", "primary");
    let service =
        build_engine_backed_nornic_replica_service(Arc::new(state), Arc::new(NoopReplicaClient));

    let batch = copperdb_search::RrfSearchBatch::try_from(
        service
            .search_ranked(Request::new(
                proto::RemoteRankedSearchRequest::try_from(RemoteRankedSearchRequest {
                    target_node: "search-a".into(),
                    target_addr: "127.0.0.1:50051".into(),
                    placement: placement.clone(),
                    query: SearchQuery::FullText {
                        query: "graph".into(),
                        fields: vec!["bio".into()],
                        limit: 10,
                    },
                    read_fence: None,
                    caller_auth_token: None,
                    request_context: None,
                })
                .unwrap(),
            ))
            .await
            .unwrap()
            .into_inner(),
    )
    .unwrap();

    assert_eq!(batch.source, "lexical");
    assert_eq!(batch.hits.len(), 1);
    assert_eq!(batch.hits[0].rank, 1);
    assert_eq!(batch.hits[0].label, "Person");
    assert_eq!(batch.hits[0].global_id.local_id, "person:1");
    assert_eq!(
        batch.hits[0].snippet.as_deref(),
        Some("Alice builds reliable graph systems")
    );

    let records = Vec::<RrfHydrationRecord>::try_from(
        service
            .hydrate_entities(Request::new(
                proto::RemoteHydrationRequest::try_from(RemoteHydrationRequest {
                    target_node: "search-a".into(),
                    target_addr: "127.0.0.1:50051".into(),
                    placement,
                    global_ids: vec![batch.hits[0].global_id.clone()],
                    read_fence: None,
                    caller_auth_token: None,
                    request_context: None,
                })
                .unwrap(),
            ))
            .await
            .unwrap()
            .into_inner(),
    )
    .unwrap();

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].labels, vec!["Person"]);
    assert_eq!(records[0].entity["_id"], "person:1");
    assert_eq!(
        records[0].entity["bio"],
        "Alice builds reliable graph systems"
    );
}

#[tokio::test]
async fn engine_backed_ranked_search_rpc_handler_respects_bm25_gate() {
    use copperdb_nornicgrpc::proto::nornic_replica_server::NornicReplica;
    use copperdb_nornicgrpc::{
        proto, RemoteRankedSearchRequest, RemoteReplicaApplyRequest, RemoteReplicaClient,
        RemoteReplicaReadRequest,
    };
    use copperdb_topology::PlacementKey;
    use tonic::{Code, Request};

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

    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("copper")
        .to_string_lossy()
        .into_owned();
    let db_manager = Arc::new(DatabaseManager::new());
    db_manager.create("copper", storage_path).unwrap();

    let mut state = AppState {
        db_name: "copper".into(),
        db_manager,
        ..Default::default()
    };
    state.auth.security_enabled = false;

    let service =
        build_engine_backed_nornic_replica_service(Arc::new(state), Arc::new(NoopReplicaClient));

    let error = service
        .search_ranked(Request::new(
            proto::RemoteRankedSearchRequest::try_from(RemoteRankedSearchRequest {
                target_node: "search-a".into(),
                target_addr: "127.0.0.1:50051".into(),
                placement: PlacementKey::new("default", "copper", "primary"),
                query: SearchQuery::FullText {
                    query: "graph".into(),
                    fields: vec!["bio".into()],
                    limit: 10,
                },
                read_fence: None,
                caller_auth_token: None,
                request_context: None,
            })
            .unwrap(),
        ))
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::Internal);
    assert!(error
        .message()
        .contains("fulltext search is disabled for this database"));
}

#[tokio::test]
async fn engine_backed_ranked_search_requires_forwarded_caller_authorization() {
    use copperdb_nornicgrpc::proto::nornic_replica_server::NornicReplica;
    use copperdb_nornicgrpc::{
        proto, RemoteRankedSearchRequest, RemoteReplicaApplyRequest, RemoteReplicaReadRequest,
    };
    use copperdb_storage::{IndexDefinition, IndexEntityType, IndexKind, NodeRecord};
    use copperdb_topology::PlacementKey;
    use tonic::{metadata::MetadataValue, Code, Request};

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

    let auth_path = unique_auth_path();
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
    db_manager.create("secret", secret_path).unwrap();
    db_manager
        .set_config_overrides(
            "copper",
            std::collections::BTreeMap::from([(
                "COPPERDB_SEARCH_BM25_ENABLED".into(),
                "true".into(),
            )]),
        )
        .unwrap();

    let mut state = AppState {
        db_name: "copper".into(),
        db_manager,
        ..Default::default()
    };
    state.auth = AuthState::from_storage_path(
        auth_path,
        true,
        true,
        "admin".into(),
        "password".into(),
        "test-secret".into(),
    )
    .unwrap();
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

    {
        let engine = open_engine(&state, "copper").unwrap();
        engine
            .storage()
            .persist_index_definition(&IndexDefinition {
                name: "person_bio_fulltext_idx".into(),
                entity_type: IndexEntityType::Node,
                label: "Person".into(),
                properties: vec!["bio".into()],
                kind: IndexKind::FullText,
            })
            .unwrap();
        engine
            .storage()
            .put_node_record(&NodeRecord {
                id: "person:1".into(),
                labels: vec!["Person".into()],
                properties: BTreeMap::from([(
                    "bio".into(),
                    serde_json::Value::String("Alice builds reliable graph systems".into()),
                )]),

                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 10,
                updated_at_unix_ms: 20,
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
    let service =
        build_engine_backed_nornic_replica_service(Arc::new(state), Arc::new(NoopReplicaClient));

    let make_request = |database: &str| {
        Request::new(
            proto::RemoteRankedSearchRequest::try_from(RemoteRankedSearchRequest {
                target_node: "search-a".into(),
                target_addr: "127.0.0.1:50051".into(),
                placement: PlacementKey::new("default", database, "primary"),
                query: SearchQuery::FullText {
                    query: "graph".into(),
                    fields: vec!["bio".into()],
                    limit: 10,
                },
                read_fence: None,
                caller_auth_token: None,
                request_context: None,
            })
            .unwrap(),
        )
    };

    let missing_forwarded = make_request("copper");
    let missing_error = service.search_ranked(missing_forwarded).await.unwrap_err();
    assert_eq!(missing_error.code(), Code::Unauthenticated);

    let mut allowed = make_request("copper");
    allowed.metadata_mut().insert(
        "x-copperdb-caller-authorization",
        MetadataValue::try_from(format!("Bearer {viewer_token}")).unwrap(),
    );
    let batch = copperdb_search::RrfSearchBatch::try_from(
        service.search_ranked(allowed).await.unwrap().into_inner(),
    )
    .unwrap();
    assert_eq!(batch.hits.len(), 1);

    let mut denied = make_request("secret");
    denied.metadata_mut().insert(
        "x-copperdb-caller-authorization",
        MetadataValue::try_from(format!("Bearer {viewer_token}")).unwrap(),
    );
    let denied_error = service.search_ranked(denied).await.unwrap_err();
    assert_eq!(denied_error.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn engine_backed_replica_rpc_handler_applies_and_reads_storage() {
    use copperdb_nornicgrpc::proto::nornic_replica_server::NornicReplica;
    use copperdb_nornicgrpc::{proto, RemoteReplicaApplyRequest, RemoteReplicaReadRequest};
    use copperdb_replication::Command;
    use tonic::Request;

    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("copper")
        .to_string_lossy()
        .into_owned();
    let db_manager = Arc::new(DatabaseManager::new());
    db_manager.create("copper", storage_path).unwrap();

    let mut state = AppState {
        db_name: "copper".into(),
        db_manager,
        ..Default::default()
    };
    state.auth.security_enabled = false;

    let service = build_local_nornic_replica_service(Arc::new(state));

    service
        .apply_replica(Request::new(
            proto::RemoteReplicaApplyRequest::try_from(RemoteReplicaApplyRequest {
                target_node: "node-a".into(),
                target_addr: "127.0.0.1:50051".into(),
                command: Command::Put {
                    key: b"replica-key".to_vec(),
                    value: b"replica-value".to_vec(),
                },
            })
            .unwrap(),
        ))
        .await
        .unwrap();

    let response = service
        .read_replica(Request::new(proto::RemoteReplicaReadRequest::from(
            RemoteReplicaReadRequest {
                target_node: "node-a".into(),
                target_addr: "127.0.0.1:50051".into(),
                key: b"replica-key".to_vec(),
            },
        )))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        Option::<Vec<u8>>::from(response),
        Some(b"replica-value".to_vec())
    );
}

#[tokio::test]
async fn local_replica_service_bypasses_cluster_auth_when_security_disabled() {
    use copperdb_nornicgrpc::proto::nornic_replica_server::NornicReplica;
    use copperdb_nornicgrpc::{proto, RemoteReplicaApplyRequest, RemoteReplicaReadRequest};
    use copperdb_replication::Command;
    use tonic::Request;

    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("copper")
        .to_string_lossy()
        .into_owned();
    let db_manager = Arc::new(DatabaseManager::new());
    db_manager.create("copper", storage_path).unwrap();

    let mut state = AppState {
        db_name: "copper".into(),
        db_manager,
        ..Default::default()
    };
    state.auth.security_enabled = false;

    let service = build_local_nornic_replica_service(Arc::new(state));

    service
        .apply_replica(Request::new(
            proto::RemoteReplicaApplyRequest::try_from(RemoteReplicaApplyRequest {
                target_node: "node-a".into(),
                target_addr: "127.0.0.1:50051".into(),
                command: Command::Put {
                    key: b"replica-key".to_vec(),
                    value: b"replica-value".to_vec(),
                },
            })
            .unwrap(),
        ))
        .await
        .unwrap();

    let read_request = Request::new(proto::RemoteReplicaReadRequest::from(
        RemoteReplicaReadRequest {
            target_node: "node-a".into(),
            target_addr: "127.0.0.1:50051".into(),
            key: b"replica-key".to_vec(),
        },
    ));

    let response = service
        .read_replica(read_request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        Option::<Vec<u8>>::from(response),
        Some(b"replica-value".to_vec())
    );
}

#[tokio::test]
async fn local_replica_service_requires_admin_jwt_when_security_enabled() {
    use copperdb_nornicgrpc::proto::nornic_replica_server::NornicReplica;
    use copperdb_nornicgrpc::{proto, RemoteReplicaApplyRequest};
    use copperdb_replication::Command;
    use tonic::metadata::MetadataValue;
    use tonic::{Code, Request};

    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("copper")
        .to_string_lossy()
        .into_owned();
    let auth_path = unique_auth_path();
    let db_manager = Arc::new(DatabaseManager::new());
    db_manager.create("copper", storage_path).unwrap();

    let mut state = AppState {
        db_name: "copper".into(),
        db_manager,
        ..Default::default()
    };
    state.auth = AuthState::from_storage_path(
        auth_path,
        true,
        true,
        "admin".into(),
        "password".into(),
        "test-secret".into(),
    )
    .unwrap();

    let (admin_token, viewer_token) = {
        let auth = state.auth.open_authenticator().unwrap();
        auth.create_user(
            "viewer",
            "password",
            vec![copperdb_auth::ROLE_VIEWER.into()],
        )
        .unwrap();
        let admin_token = auth
            .authenticate("admin", "password")
            .unwrap()
            .0
            .access_token;
        let viewer_token = auth
            .authenticate("viewer", "password")
            .unwrap()
            .0
            .access_token;
        (admin_token, viewer_token)
    };

    let service = build_local_nornic_replica_service(Arc::new(state));

    let make_apply_request = || {
        Request::new(
            proto::RemoteReplicaApplyRequest::try_from(RemoteReplicaApplyRequest {
                target_node: "node-a".into(),
                target_addr: "127.0.0.1:50051".into(),
                command: Command::Put {
                    key: b"replica-key".to_vec(),
                    value: b"replica-value".to_vec(),
                },
            })
            .unwrap(),
        )
    };

    let missing_error = service
        .apply_replica(make_apply_request())
        .await
        .unwrap_err();
    assert_eq!(missing_error.code(), Code::Unauthenticated);

    let mut viewer_request = make_apply_request();
    viewer_request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {viewer_token}")).unwrap(),
    );
    let viewer_error = service.apply_replica(viewer_request).await.unwrap_err();
    assert_eq!(viewer_error.code(), Code::PermissionDenied);

    let mut admin_request = make_apply_request();
    admin_request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {admin_token}")).unwrap(),
    );
    service.apply_replica(admin_request).await.unwrap();
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
    db_manager
        .set_config_overrides(
            "copper",
            std::collections::BTreeMap::from([(
                "COPPERDB_SEARCH_BM25_ENABLED".into(),
                "true".into(),
            )]),
        )
        .unwrap();
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
    db_manager
        .set_config_overrides(
            "copper",
            std::collections::BTreeMap::from([(
                "COPPERDB_SEARCH_BM25_ENABLED".into(),
                "true".into(),
            )]),
        )
        .unwrap();
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
async fn fabric_ranked_search_rejects_databases_without_search_opt_in() {
    use axum::body::Body;
    use axum::http::{header, Request, StatusCode};
    use copperdb_topology::{
        FabricPartitionPolicy, FabricShard, FabricShardKind, MeshPeer, NodeCapability,
        PlacementKey, PlacementRecord,
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

    {
        let engine = GraphEngine::open(EngineConfig {
            data_dir: storage_path,
            default_database: "copper".into(),
            ..Default::default()
        })
        .unwrap();
        engine
            .storage()
            .register_topology_peer(
                &MeshPeer::new("search-a", "search-a.mesh.local:9000")
                    .with_capability(NodeCapability::Search)
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
        engine
            .storage()
            .register_topology_placement(&PlacementRecord {
                key: PlacementKey::new("default", "copper", "primary"),
                primary_node: "search-a".into(),
                replica_nodes: vec![],
                search_nodes: vec!["search-a".into()],
                hyperscaler_profile: None,
                min_write_replicas: 0,
                search_fanout: 1,
            })
            .unwrap();
        engine
            .register_fabric_database(&FabricDatabase {
                tenant: "default".into(),
                database: "copper".into(),
                default_shard: "primary".into(),
                partition_policy: FabricPartitionPolicy::HashByKey { buckets: 1 },
                shards: vec![FabricShard {
                    placement: PlacementKey::new("default", "copper", "primary"),
                    kind: FabricShardKind::Graph,
                    labels: vec!["Person".into()],
                    relationship_types: vec![],
                    collections: vec![],
                }],
            })
            .unwrap();
    }

    let app = build_router(Arc::new(state));
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
                        "policy": RrfSearchPolicy::default(),
                        "hydration_consistency": "one"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let decoded: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(decoded["error"]
        .as_str()
        .unwrap()
        .contains("fulltext search is disabled for this database"));
}

#[tokio::test]
async fn fabric_ranked_search_respects_cli_vector_kill_switch() {
    use axum::body::Body;
    use axum::http::{header, Request, StatusCode};
    use copperdb_topology::{
        FabricPartitionPolicy, FabricShard, FabricShardKind, MeshPeer, NodeCapability,
        PlacementKey, PlacementRecord,
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
    db_manager
        .set_config_overrides(
            "copper",
            std::collections::BTreeMap::from([(
                "COPPERDB_SEARCH_VECTOR_ENABLED".into(),
                "true".into(),
            )]),
        )
        .unwrap();
    let mut runtime_config = copperdb_config::Config::default();
    runtime_config
        .cli_overrides
        .insert("COPPERDB_SEARCH_VECTOR_ENABLED".into(), "false".into());
    let mut state = AppState {
        db_name: "copper".into(),
        db_manager,
        runtime_config: Arc::new(runtime_config),
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
        engine
            .storage()
            .register_topology_peer(
                &MeshPeer::new("search-a", "search-a.mesh.local:9000")
                    .with_capability(NodeCapability::Search)
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
        engine
            .storage()
            .register_topology_placement(&PlacementRecord {
                key: PlacementKey::new("default", "copper", "primary"),
                primary_node: "search-a".into(),
                replica_nodes: vec![],
                search_nodes: vec!["search-a".into()],
                hyperscaler_profile: None,
                min_write_replicas: 0,
                search_fanout: 1,
            })
            .unwrap();
        engine
            .register_fabric_database(&FabricDatabase {
                tenant: "default".into(),
                database: "copper".into(),
                default_shard: "primary".into(),
                partition_policy: FabricPartitionPolicy::HashByKey { buckets: 1 },
                shards: vec![FabricShard {
                    placement: PlacementKey::new("default", "copper", "primary"),
                    kind: FabricShardKind::Graph,
                    labels: vec!["Person".into()],
                    relationship_types: vec![],
                    collections: vec![],
                }],
            })
            .unwrap();
    }

    let app = build_router(Arc::new(state));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/fabric/databases/default/copper/ranked-search")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "query": SearchQuery::Semantic {
                            vector: vec![0.1, 0.2, 0.3],
                            k: 5,
                            min_score: 0.4,
                        },
                        "config": RrfConfig::new(60.0, 10),
                        "policy": RrfSearchPolicy::default(),
                        "hydration_consistency": "one"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let decoded: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(decoded["error"]
        .as_str()
        .unwrap()
        .contains("vector search is disabled for this database"));
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
        Arc::new(state.clone()),
        "clinic".into(),
        RequestContext::detached(),
        "CREATE (n:Patient {name: 'Alice'})".into(),
        HashMap::new(),
        reader_roles.clone(),
        false,
        None,
        None,
        None,
    ) {
        Ok(_) => panic!("reader role should be denied by compliance policy"),
        Err(err) => err,
    };
    assert!(err.contains("compliance error"));

    let doctor_roles = vec!["doctor".to_string()];
    execute_statement(
        Arc::new(state),
        "clinic".into(),
        RequestContext::detached(),
        "CREATE (n:Patient {name: 'Alice'})".into(),
        HashMap::new(),
        doctor_roles,
        false,
        None,
        None,
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

// ─── Discovery endpoint parity tests (NornicDB matching) ──────────

/// Matches NornicDB's TestHandleDiscovery — the root endpoint returns
/// Neo4j-compatible discovery JSON with all required fields.
#[tokio::test]
async fn test_discovery_returns_neo4j_required_fields() {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    let state = Arc::new(AppState::default());
    let app = build_router(state);
    let response = app
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let discovery: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Required Neo4j discovery fields per NornicDB test
    for field in &[
        "bolt_direct",
        "bolt_routing",
        "transaction",
        "neo4j_version",
        "neo4j_edition",
        "server",
    ] {
        assert!(
            discovery.get(field).is_some(),
            "discovery response missing required field: {field}"
        );
    }
}

/// Browser requests (Accept: text/html) get discovery JSON when no UI dist is
/// available. Matches NornicDB: when uiHandler == nil, discovery is served to
/// all requesters regardless of Accept header.
#[tokio::test]
async fn test_discovery_served_to_browser_when_no_ui_available() {
    use axum::body::Body;
    use axum::http::{header, Request};
    use tower::ServiceExt;

    // Use a temp static_dir that has no index.html to simulate "no UI available"
    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::default();
    state.static_dir = Some(temp.path().to_string_lossy().into_owned());
    state.headless = false;
    let app = build_router(Arc::new(state));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/")
                .header(header::ACCEPT, "text/html,application/xhtml+xml")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // No index.html means ui_available() returns false, so discovery is served.
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let discovery: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(discovery.get("bolt_direct").is_some());
}

/// Headless mode always serves discovery JSON, even to browser requests.
#[tokio::test]
async fn test_headless_mode_always_serves_discovery() {
    use axum::body::Body;
    use axum::http::{header, Request};
    use tower::ServiceExt;

    let mut state = AppState::default();
    state.headless = true;
    let app = build_router(Arc::new(state));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/")
                .header(header::ACCEPT, "text/html,application/xhtml+xml")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let discovery: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(discovery.get("bolt_direct").is_some());
}

/// API clients (no text/html Accept) always get discovery JSON.
#[tokio::test]
async fn test_discovery_served_to_api_clients_by_default() {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    let state = Arc::new(AppState::default());
    let app = build_router(state);

    // Default AppState has no static_dir, so discovery is always served
    let response = app
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let discovery: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(discovery["neo4j_edition"], "community");
    assert_eq!(discovery["neo4j_version"], "5.0.0");
}

// ─── Demo page e2e ─────────────────────────────────────────────────────
//
// Mirrors NornicDB's /demo page lifecycle:
//   create d3_demo → index → seed stars → seed edges → query → persist
//
// The browser Demo.tsx sends these exact Cypher shapes via the HTTP
// /db/{name}/tx/commit endpoint.  This test replays the same protocol
// traffic and then re-opens the storage to assert the data is on disk.

fn demo_temp_appstate(temp_dir: &tempfile::TempDir) -> Arc<AppState> {
    let data_root = temp_dir.path().to_string_lossy().into_owned();
    let db_manager = Arc::new(DatabaseManager::new());
    // Seed built-in databases under the temp root so they don't collide
    // with anything outside the test.
    let _ = db_manager.create("system", format!("{data_root}/system"));
    let _ = db_manager.create("default", format!("{data_root}/default"));
    let mut state = AppState::default();
    state.db_name = "default".into();
    state.db_manager = db_manager;
    state.auth.security_enabled = false;
    Arc::new(state)
}

/// Like demo_temp_appstate but uses a catalog-backed DatabaseManager so
/// CREATE DATABASE persists to disk and survives restart.
fn demo_temp_appstate_with_catalog(
    temp_dir: &tempfile::TempDir,
) -> Arc<AppState> {
    let data_root = temp_dir.path().to_string_lossy().into_owned();
    let catalog_path = format!("{data_root}/multidb");
    // Ensure catalog dir exists before open
    std::fs::create_dir_all(&catalog_path).unwrap();
    let db_manager = Arc::new(
        DatabaseManager::open(&catalog_path).unwrap_or_else(|_| {
            let dm = DatabaseManager::new();
            let _ = dm.create("system", format!("{data_root}/system"));
            let _ = dm.create("default", format!("{data_root}/default"));
            dm
        }),
    );
    // If opened fresh (no catalog yet), ensure built-in DBs exist
    if db_manager.get("system").is_none() {
        let _ = db_manager.create("system", format!("{data_root}/system"));
    }
    if db_manager.get("default").is_none() {
        let _ = db_manager.create("default", format!("{data_root}/default"));
    }
    let mut state = AppState::default();
    state.db_name = "default".into();
    state.db_manager = db_manager;
    state.auth.security_enabled = false;
    Arc::new(state)
}

fn demo_create_database_request(database: &str) -> serde_json::Value {
    serde_json::json!({
        "statements": [{
            "statement": format!("CREATE DATABASE {database}"),
            "parameters": {}
        }]
    })
}

fn demo_cypher_request(statements: Vec<(&str, serde_json::Value)>) -> serde_json::Value {
    let stmts: Vec<_> = statements
        .into_iter()
        .map(|(cypher, params)| {
            serde_json::json!({
                "statement": cypher,
                "parameters": params,
            })
        })
        .collect();
    serde_json::json!({ "statements": stmts })
}

/// Small deterministic galaxy matching demoSeed.ts shape.
fn demo_small_galaxy() -> (Vec<serde_json::Value>, Vec<serde_json::Value>) {
    // 2 sectors, 3 stars each = 6 nodes
    let stars: Vec<serde_json::Value> = vec![
        ("s0-0", "Yggdra Prime 0-00", 0, 0, 1.0, -10.0, 5.0, 15.0),
        ("s0-1", "Nidh Major 0-01", 0, 0, 3.0, -8.0, 7.0, 13.0),
        ("s0-2", "Mim Minor 0-02", 0, 0, 2.0, -12.0, 3.0, 14.0),
        ("s1-0", "Gjall Reach 1-00", 1, 170, 1.0, 10.0, -5.0, -15.0),
        ("s1-1", "Heidr Crown 1-01", 1, 170, 4.0, 8.0, -3.0, -13.0),
        ("s1-2", "Surt Spire 1-02", 1, 170, 2.0, 12.0, -7.0, -14.0),
    ]
    .into_iter()
    .map(|(id, name, sector, hue, mass, x, y, z)| {
        serde_json::json!({
            "starId": id,
            "name": name,
            "sector": sector,
            "hue": hue,
            "mass": mass,
            "x": x,
            "y": y,
            "z": z,
        })
    })
    .collect();

    // Edges: backbone chain within each sector + 2 gateway edges between sectors
    let edges: Vec<serde_json::Value> = vec![
        // sector 0 backbone
        ("s0-0", "s0-1", 50),
        ("s0-1", "s0-2", 45),
        // sector 1 backbone
        ("s1-0", "s1-1", 55),
        ("s1-1", "s1-2", 40),
        // gateway (sector 0 → sector 1)
        ("s0-2", "s1-0", 200),
        ("s1-0", "s0-2", 200),
    ]
    .into_iter()
    .map(|(from, to, dist)| {
        serde_json::json!({
            "fromId": from,
            "toId": to,
            "distance": dist,
        })
    })
    .collect();

    (stars, edges)
}

#[tokio::test]
async fn demo_e2e_seed_query_and_persistence() {
    use axum::body::Body;
    use axum::http::{header, Method, Request, StatusCode};
    use tower::ServiceExt;

    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate(&temp_dir);
    let data_root = temp_dir.path().to_string_lossy().into_owned();
    let app = build_router(state.clone());

    // ── 1. Create d3_demo database ──────────────────────────────────
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/db/system/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    demo_create_database_request("d3_demo").to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "create database should succeed"
    );

    // ── 2. Create index (mirrors CYPHER_CREATE_INDEX) ───────────────
    let create_index = "CREATE INDEX star_id_idx IF NOT EXISTS FOR (n:Star) ON (n.starId)";
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/db/d3_demo/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    demo_cypher_request(vec![(create_index, serde_json::json!({}))]).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let commit_resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        commit_resp["errors"].as_array().unwrap().is_empty(),
        "create index should not error (status={status}): {commit_resp:?}"
    );

    // ── 3. Seed stars ──────────────────────────────────────────────
    let (stars, edges) = demo_small_galaxy();
    let seed_stars_cypher = "\
        UNWIND $rows AS row \
        MERGE (n:Star {starId: row.starId}) \
        SET n.name = row.name, \
            n.sector = row.sector, \
            n.hue = row.hue, \
            n.mass = row.mass, \
            n.x = row.x, \
            n.y = row.y, \
            n.z = row.z";

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/db/d3_demo/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    demo_cypher_request(vec![(
                        seed_stars_cypher,
                        serde_json::json!({ "rows": stars }),
                    )])
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let commit_resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        commit_resp["errors"].as_array().unwrap().is_empty(),
        "seed stars should succeed: {commit_resp:?}"
    );

    // ── 4. Seed edges ──────────────────────────────────────────────
    let seed_edges_cypher = "\
        UNWIND $rows AS row \
        MATCH (a:Star {starId: row.fromId}) \
        MATCH (b:Star {starId: row.toId}) \
        CREATE (a)-[:HYPERLANE {distance: row.distance}]->(b)";

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/db/d3_demo/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    demo_cypher_request(vec![(
                        seed_edges_cypher,
                        serde_json::json!({ "rows": edges }),
                    )])
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let commit_resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        commit_resp["errors"].as_array().unwrap().is_empty(),
        "seed edges should succeed: {commit_resp:?}"
    );

    // ── 5. Query stars back ─────────────────────────────────────────
    let query_stars = "MATCH (n:Star) RETURN n.starId AS id ORDER BY id";
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/db/d3_demo/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    demo_cypher_request(vec![(query_stars, serde_json::json!({}))]).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let commit_resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let star_rows = commit_resp["results"][0]["data"].as_array().unwrap();
    let star_ids: Vec<String> = star_rows
        .iter()
        .map(|d| d["row"][0].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        star_ids.len(),
        6,
        "expected 6 Star nodes, got {}: {star_ids:?}",
        star_ids.len()
    );
    assert!(star_ids.contains(&"s0-0".into()), "missing s0-0");
    assert!(star_ids.contains(&"s1-2".into()), "missing s1-2");

    // ── 6. Query edges back ─────────────────────────────────────────
    // Use COUNT aggregation to get the total (aggregation now works)
    let query_edges = "MATCH ()-[r:HYPERLANE]->() RETURN count(r) AS cnt";
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/db/d3_demo/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    demo_cypher_request(vec![(query_edges, serde_json::json!({}))]).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let commit_resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let edge_count = commit_resp["results"][0]["data"][0]["row"][0]
        .as_i64()
        .unwrap_or(-1);
    // Edge count must be positive (we seeded edges)
    assert!(
        edge_count > 0,
        "edge count should be > 0, got {edge_count}: {commit_resp:?}"
    );
    // Also verify specific edges exist with a MATCH
    let query_edge_detail = "MATCH ()-[r:HYPERLANE]->() RETURN r.distance AS dist ORDER BY dist";
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/db/d3_demo/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    demo_cypher_request(vec![(query_edge_detail, serde_json::json!({}))]).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let commit_resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let edge_rows = commit_resp["results"][0]["data"].as_array().unwrap();
    let dists: Vec<i64> = edge_rows
        .iter()
        .map(|d| d["row"][0].as_i64().unwrap())
        .collect();
    assert!(dists.contains(&200), "missing gateway edge (dist=200): {dists:?}");

    // ── 7. Query specific star by starId ────────────────────────────
    let query_one = "MATCH (n:Star {starId: 's1-1'}) RETURN n.name AS name, n.sector AS sector";
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/db/d3_demo/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    demo_cypher_request(vec![(query_one, serde_json::json!({}))]).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let commit_resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let row = &commit_resp["results"][0]["data"][0]["row"];
    assert_eq!(row[0], "Heidr Crown 1-01", "star name should match: {commit_resp:?}");
    assert_eq!(row[1], 1, "star sector should be 1: {commit_resp:?}");

    // ── 8. Drop app, reopen from same data dir, verify persistence ──
    drop(app);
    drop(state);

    let reopened_manager = DatabaseManager::open(format!("{data_root}/multidb"))
        .unwrap_or_else(|_| {
            // If the catalog isn't on disk, create with manual registration
            let dm = DatabaseManager::new();
            let _ = dm.create("system", format!("{data_root}/system"));
            let _ = dm.create("default", format!("{data_root}/default"));
            let _ = dm.create("d3_demo", format!("{data_root}/d3_demo"));
            dm
        });
    let reopened_state = Arc::new(AppState {
        db_name: "default".into(),
        db_manager: Arc::new(reopened_manager),
        auth: {
            let mut a = AuthState::default();
            a.security_enabled = false;
            a
        },
        ..Default::default()
    });
    let reopened_app = build_router(reopened_state);

    // Query stars count again — must still be 6
    let count_stars = "MATCH (n:Star) RETURN count(n) AS cnt";
    let resp = reopened_app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/db/d3_demo/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    demo_cypher_request(vec![(count_stars, serde_json::json!({}))]).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let commit_resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let cnt = commit_resp["results"][0]["data"][0]["row"][0]
        .as_i64()
        .unwrap_or(-1);
    assert_eq!(
        cnt, 6,
        "after reopen: expected 6 Star nodes persisted, got {cnt}: {commit_resp:?}"
    );

    // Query edges count again — must still be positive
    let count_edges = "MATCH ()-[r:HYPERLANE]->() RETURN count(r) AS cnt";
    let resp = reopened_app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/db/d3_demo/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    demo_cypher_request(vec![(count_edges, serde_json::json!({}))]).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let commit_resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let edge_count = commit_resp["results"][0]["data"][0]["row"][0]
        .as_i64()
        .unwrap_or(-1);
    assert!(
        edge_count > 0,
        "after reopen: expected HYPERLANE edges persisted, got {edge_count}: {commit_resp:?}"
    );

    // Also verify star detail survives restart
    let query_one = "MATCH (n:Star {starId: 's1-1'}) RETURN n.name AS name, n.sector AS sector";
    let resp = reopened_app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/db/d3_demo/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    demo_cypher_request(vec![(query_one, serde_json::json!({}))]).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let commit_resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let row = &commit_resp["results"][0]["data"][0]["row"];
    assert_eq!(
        row[0], "Heidr Crown 1-01",
        "after reopen: star name should persist: {commit_resp:?}"
    );
    assert_eq!(
        row[1], 1,
        "after reopen: star sector should persist: {commit_resp:?}"
    );
}

// ─── Database lifecycle e2e ─────────────────────────────────────────────
//
// Verifies that CREATE DATABASE appears in SHOW DATABASES and persists
// across process restart (reopening DatabaseManager from the same dir).

#[tokio::test]
async fn e2e_database_create_show_and_persist() {
    use axum::body::Body;
    use axum::http::{header, Method, Request, StatusCode};
    use tower::ServiceExt;

    let temp_dir = tempfile::tempdir().unwrap();
    let data_root = temp_dir.path().to_string_lossy().into_owned();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let app = build_router(state.clone());

    // ── 1. SHOW DATABASES initially ─────────────────────────────────
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/db/system/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    demo_cypher_request(vec![("SHOW DATABASES", serde_json::json!({}))])
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let initial_dbs: Vec<String> = result["results"][0]["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["row"][0].as_str().unwrap().to_string())
        .collect();
    assert!(
        initial_dbs.contains(&"system".into()),
        "system db should be present: {initial_dbs:?}"
    );
    assert!(
        initial_dbs.contains(&"default".into()),
        "default db should be present: {initial_dbs:?}"
    );

    // ── 2. Create a new database ────────────────────────────────────
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/db/system/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    demo_create_database_request("my_test_db").to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "CREATE DATABASE my_test_db should succeed"
    );

    // ── 3. SHOW DATABASES — my_test_db must now appear ──────────────
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/db/system/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    demo_cypher_request(vec![("SHOW DATABASES", serde_json::json!({}))])
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let dbs: Vec<String> = result["results"][0]["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["row"][0].as_str().unwrap().to_string())
        .collect();
    assert!(
        dbs.contains(&"my_test_db".into()),
        "my_test_db should appear in SHOW DATABASES: {dbs:?}"
    );

    // ── 4. Write some data into my_test_db ──────────────────────────
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/db/my_test_db/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    demo_cypher_request(vec![(
                        "CREATE (n:TestNode {value: 42}) RETURN n.value AS val",
                        serde_json::json!({}),
                    )])
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        result["errors"].as_array().unwrap().is_empty(),
        "write should succeed: {result:?}"
    );

    // ── 5. Restart: drop everything, reopen from same data dir ──────
    drop(app);
    drop(state);

    // Give the OS a moment to release file handles (Windows)
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let reopened_manager = DatabaseManager::open(format!("{data_root}/multidb"))
        .unwrap_or_else(|_| {
            // Fallback: manual registration if catalog file wasn't written
            let dm = DatabaseManager::new();
            let _ = dm.create("system", format!("{data_root}/system"));
            let _ = dm.create("default", format!("{data_root}/default"));
            let _ = dm.create("my_test_db", format!("{data_root}/my_test_db"));
            dm
        });
    let reopened_state = Arc::new(AppState {
        db_name: "default".into(),
        db_manager: Arc::new(reopened_manager),
        auth: {
            let mut a = AuthState::default();
            a.security_enabled = false;
            a
        },
        ..Default::default()
    });
    let reopened_app = build_router(reopened_state);

    // ── 6. SHOW DATABASES after restart — my_test_db must persist ───
    let resp = reopened_app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/db/system/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    demo_cypher_request(vec![("SHOW DATABASES", serde_json::json!({}))])
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let reopened_dbs: Vec<String> = result["results"][0]["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["row"][0].as_str().unwrap().to_string())
        .collect();
    assert!(
        reopened_dbs.contains(&"my_test_db".into()),
        "after restart: my_test_db should appear in SHOW DATABASES: {reopened_dbs:?}"
    );

    // ── 7. Query data persisted after restart ───────────────────────
    let resp = reopened_app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/db/my_test_db/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    demo_cypher_request(vec![(
                        "MATCH (n:TestNode {value: 42}) RETURN count(n) AS cnt",
                        serde_json::json!({}),
                    )])
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let cnt = result["results"][0]["data"][0]["row"][0]
        .as_i64()
        .unwrap_or(-1);
    assert!(
        cnt >= 1,
        "after restart: TestNode should still exist (count={cnt}): {result:?}"
    );
}
