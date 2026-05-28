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
    async fn database_config_admin_round_trips_overrides_and_effective_view() {
        use axum::body::Body;
        use axum::http::{header, Method, Request, StatusCode};
        use tower::ServiceExt;

        let temp_dir = tempfile::tempdir().unwrap();
        let storage_path = temp_dir
            .path()
            .join("clinic")
            .to_string_lossy()
            .into_owned();
        let db_manager = Arc::new(DatabaseManager::new());
        db_manager.create("clinic", storage_path).unwrap();

        let mut runtime_config = copperdb_config::Config::default();
        runtime_config.cli_overrides.insert(
            "COPPERDB_SEARCH_VECTOR_ENABLED".into(),
            "false".into(),
        );

        let mut state = AppState {
            db_name: "clinic".into(),
            db_manager,
            runtime_config: Arc::new(runtime_config),
            ..Default::default()
        };
        state.auth.security_enabled = false;

        let app = build_router(Arc::new(state));
        let put_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/admin/databases/clinic/config")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "overrides": {
                                "COPPERDB_SEARCH_BM25_ENABLED": "true",
                                "COPPERDB_SEARCH_VECTOR_ENABLED": "true"
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put_response.status(), StatusCode::OK);

        let get_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/databases/clinic/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_response.status(), StatusCode::OK);
        let get_body = axum::body::to_bytes(get_response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let overrides_json: serde_json::Value = serde_json::from_slice(&get_body).unwrap();
        assert_eq!(
            overrides_json["overrides"]["COPPERDB_SEARCH_BM25_ENABLED"],
            "true"
        );
        assert_eq!(
            overrides_json["allowedKeys"].as_array().unwrap().len(),
            13
        );

        let effective_response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/databases/clinic/config/effective")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(effective_response.status(), StatusCode::OK);
        let effective_body = axum::body::to_bytes(effective_response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let effective_json: serde_json::Value = serde_json::from_slice(&effective_body).unwrap();
        assert_eq!(effective_json["effective"]["bm25_enabled"], true);
        assert_eq!(effective_json["effective"]["vector_enabled"], false);
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
        use copperdb_nornicgrpc::{
            proto, RemoteHydrationRequest, RemoteRankedSearchRequest, RemoteReplicaApplyRequest,
            RemoteReplicaClient, RemoteReplicaReadRequest,
        };
        use copperdb_nornicgrpc::proto::nornic_replica_server::NornicReplica;
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
        let storage_path = temp_dir.path().join("copper").to_string_lossy().into_owned();
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
                    created_at_unix_ms: 10,
                    updated_at_unix_ms: 20,
                })
                .unwrap();
        }

        let placement = PlacementKey::new("default", "copper", "primary");
        let service = build_engine_backed_nornic_replica_service(
            Arc::new(state),
            Arc::new(NoopReplicaClient),
        );

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
        assert_eq!(records[0].entity["bio"], "Alice builds reliable graph systems");
    }

    #[tokio::test]
    async fn engine_backed_ranked_search_rpc_handler_respects_bm25_gate() {
        use copperdb_nornicgrpc::{
            proto, RemoteRankedSearchRequest, RemoteReplicaApplyRequest, RemoteReplicaClient,
            RemoteReplicaReadRequest,
        };
        use copperdb_nornicgrpc::proto::nornic_replica_server::NornicReplica;
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
        let storage_path = temp_dir.path().join("copper").to_string_lossy().into_owned();
        let db_manager = Arc::new(DatabaseManager::new());
        db_manager.create("copper", storage_path).unwrap();

        let mut state = AppState {
            db_name: "copper".into(),
            db_manager,
            ..Default::default()
        };
        state.auth.security_enabled = false;

        let service = build_engine_backed_nornic_replica_service(
            Arc::new(state),
            Arc::new(NoopReplicaClient),
        );

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
    async fn engine_backed_replica_rpc_handler_applies_and_reads_storage() {
        use copperdb_nornicgrpc::proto::nornic_replica_server::NornicReplica;
        use copperdb_nornicgrpc::{proto, RemoteReplicaApplyRequest, RemoteReplicaReadRequest};
        use copperdb_replication::Command;
        use tonic::Request;

        let temp_dir = tempfile::tempdir().unwrap();
        let storage_path = temp_dir.path().join("copper").to_string_lossy().into_owned();
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
    async fn local_replica_service_uses_runtime_config_grpc_auth_token() {
        use copperdb_nornicgrpc::proto::nornic_replica_server::NornicReplica;
        use copperdb_nornicgrpc::{proto, RemoteReplicaApplyRequest, RemoteReplicaReadRequest};
        use copperdb_replication::Command;
        use tonic::metadata::MetadataValue;
        use tonic::{Code, Request};

        let temp_dir = tempfile::tempdir().unwrap();
        let storage_path = temp_dir.path().join("copper").to_string_lossy().into_owned();
        let db_manager = Arc::new(DatabaseManager::new());
        db_manager.create("copper", storage_path).unwrap();

        let mut runtime_config = RuntimeConfig::default();
        runtime_config.server.grpc_auth_token = Some("shared-secret".into());

        let mut state = AppState {
            db_name: "copper".into(),
            db_manager,
            runtime_config: Arc::new(runtime_config),
            ..Default::default()
        };
        state.auth.security_enabled = false;

        let service = build_local_nornic_replica_service(Arc::new(state));

        let error = service
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
            .unwrap_err();
        assert_eq!(error.code(), Code::Unauthenticated);

        let mut apply_request = Request::new(
            proto::RemoteReplicaApplyRequest::try_from(RemoteReplicaApplyRequest {
                target_node: "node-a".into(),
                target_addr: "127.0.0.1:50051".into(),
                command: Command::Put {
                    key: b"replica-key".to_vec(),
                    value: b"replica-value".to_vec(),
                },
            })
            .unwrap(),
        );
        apply_request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from("Bearer shared-secret").unwrap(),
        );
        service.apply_replica(apply_request).await.unwrap();

        let mut read_request = Request::new(proto::RemoteReplicaReadRequest::from(
            RemoteReplicaReadRequest {
                target_node: "node-a".into(),
                target_addr: "127.0.0.1:50051".into(),
                key: b"replica-key".to_vec(),
            },
        ));
        read_request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from("Bearer shared-secret").unwrap(),
        );

        let response = service.read_replica(read_request).await.unwrap().into_inner();
        assert_eq!(
            Option::<Vec<u8>>::from(response),
            Some(b"replica-value".to_vec())
        );
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
        runtime_config.cli_overrides.insert(
            "COPPERDB_SEARCH_VECTOR_ENABLED".into(),
            "false".into(),
        );
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
