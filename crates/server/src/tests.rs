#![allow(clippy::field_reassign_with_default)]

use super::*;

#[tokio::test]
async fn runtime_configuration_loads_apoc_only_when_enabled() {
    let mut state = AppState::default();
    assert!(state.packages.packages().is_empty());

    let mut config = RuntimeConfig::default();
    config.packages.enabled = vec![copperdb_apoc::PACKAGE_ID.into()];
    config.packages.required = vec![copperdb_apoc::PACKAGE_ID.into()];
    config.packages.grants.insert(
        copperdb_apoc::PACKAGE_ID.into(),
        vec![copperdb_plugin::PackageCapability::QueryRead],
    );
    state.configure_runtime(Arc::new(config)).await.unwrap();

    assert_eq!(state.packages.packages().len(), 1);
    assert_eq!(state.packages.packages()[0].id, copperdb_apoc::PACKAGE_ID);
    assert!(state
        .packages
        .function_registry()
        .get("apoc.create.uuid")
        .is_some());
    assert_eq!(
        state
            .package_runtime
            .as_ref()
            .unwrap()
            .health()
            .await
            .get(copperdb_apoc::PACKAGE_ID)
            .unwrap()
            .status,
        copperdb_plugin::PackageStatus::Running
    );
    state.shutdown_packages().await.unwrap();
}

#[tokio::test]
async fn apoc_load_json_uses_explicit_rooted_file_import_grant() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("payload.json"), br#"[{"id":1},{"id":2}]"#).unwrap();
    let mut state = AppState::default();
    let mut config = RuntimeConfig::default();
    config.packages.enabled = vec![copperdb_apoc::PACKAGE_ID.into()];
    config.packages.grants.insert(
        copperdb_apoc::PACKAGE_ID.into(),
        vec![
            copperdb_plugin::PackageCapability::QueryRead,
            copperdb_plugin::PackageCapability::FileImport,
        ],
    );
    config.packages.configuration.insert(
        copperdb_apoc::PACKAGE_ID.into(),
        serde_json::json!({"file_access_root": root.path()}),
    );
    state.configure_runtime(Arc::new(config)).await.unwrap();

    let result = open_engine(&state, "copperdb")
        .unwrap()
        .execute(
            "CALL apoc.load.json('payload.json') YIELD value RETURN value",
            HashMap::new(),
        )
        .unwrap();

    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0]["value"]["id"].as_f64(), Some(1.0));
    assert_eq!(result.rows[1]["value"]["id"].as_f64(), Some(2.0));
    state.shutdown_packages().await.unwrap();
}

#[tokio::test]
async fn runtime_configuration_loads_heimdall_only_when_enabled() {
    let mut state = AppState::default();
    assert!(state
        .packages
        .action_registry()
        .get(copperdb_heimdall::QUERY_ACTION)
        .is_none());

    let mut config = RuntimeConfig::default();
    config.packages.enabled = vec![copperdb_heimdall::PACKAGE_ID.into()];
    config.packages.required = vec![copperdb_heimdall::PACKAGE_ID.into()];
    config.packages.grants.insert(
        copperdb_heimdall::PACKAGE_ID.into(),
        vec![
            copperdb_plugin::PackageCapability::QueryRead,
            copperdb_plugin::PackageCapability::Events,
        ],
    );
    state.configure_runtime(Arc::new(config)).await.unwrap();

    assert_eq!(state.packages.packages().len(), 1);
    assert_eq!(
        state.packages.packages()[0].id,
        copperdb_heimdall::PACKAGE_ID
    );
    let action_registry = state.packages.action_registry();
    let action = action_registry
        .get(copperdb_heimdall::QUERY_ACTION)
        .unwrap();
    assert_eq!(action.package_id(), Some(copperdb_heimdall::PACKAGE_ID));
    assert_eq!(
        state.package_runtime.as_ref().unwrap().health().await[copperdb_heimdall::PACKAGE_ID]
            .status,
        copperdb_plugin::PackageStatus::Running
    );

    state.shutdown_packages().await.unwrap();
}

#[tokio::test]
async fn heimdall_action_executes_through_database_scoped_host_service() {
    let mut state = AppState::default();
    let mut config = RuntimeConfig::default();
    config.packages.enabled = vec![copperdb_heimdall::PACKAGE_ID.into()];
    config.packages.grants.insert(
        copperdb_heimdall::PACKAGE_ID.into(),
        vec![
            copperdb_plugin::PackageCapability::QueryRead,
            copperdb_plugin::PackageCapability::Events,
        ],
    );
    state.configure_runtime(Arc::new(config)).await.unwrap();
    let request = copperdb_util::RequestContext::detached();

    let result = state
        .execute_package_action(
            &request,
            copperdb_heimdall::QUERY_ACTION,
            &serde_json::json!({"cypher": "RETURN 1 AS one"}),
            "copperdb",
            &["admin".into()],
        )
        .unwrap();

    assert_eq!(result["success"], true);
    assert_eq!(result["message"], "Query returned 1 row(s)");
    assert_eq!(result["data"]["rows"][0]["one"], 1);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let metrics = state.package_runtime.as_ref().unwrap().event_metrics()
                [copperdb_heimdall::PACKAGE_ID];
            if metrics.delivered == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    state.shutdown_packages().await.unwrap();
}

#[tokio::test]
async fn runtime_configuration_rejects_settings_for_disabled_packages() {
    let mut state = AppState::default();
    let mut config = RuntimeConfig::default();
    config
        .packages
        .configuration
        .insert(copperdb_apoc::PACKAGE_ID.into(), serde_json::json!({}));

    let error = state.configure_runtime(Arc::new(config)).await.unwrap_err();

    assert_eq!(
        error.to_string(),
        "engine error: package settings target a disabled package: copperdb.apoc"
    );
    assert!(state.packages.packages().is_empty());
    assert!(state.package_runtime.is_none());
}

#[tokio::test]
async fn runtime_configuration_rejects_undeclared_package_settings() {
    let mut state = AppState::default();
    let mut config = RuntimeConfig::default();
    config.packages.enabled = vec![copperdb_apoc::PACKAGE_ID.into()];
    config.packages.required = vec![copperdb_apoc::PACKAGE_ID.into()];
    config.packages.grants.insert(
        copperdb_apoc::PACKAGE_ID.into(),
        vec![copperdb_plugin::PackageCapability::QueryRead],
    );
    config.packages.configuration.insert(
        copperdb_apoc::PACKAGE_ID.into(),
        serde_json::json!({"unknown": true}),
    );

    let error = state.configure_runtime(Arc::new(config)).await.unwrap_err();

    assert_eq!(
        error.to_string(),
        "engine error: package copperdb.apoc configuration does not match its schema"
    );
    assert!(state.packages.packages().is_empty());
    assert!(state.package_runtime.is_none());
}

#[test]
fn http_trace_context_excludes_baggage_and_credentials() {
    use axum::http::{header, HeaderValue};

    let mut headers = HeaderMap::new();
    headers.insert(
        "traceparent",
        HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
    );
    headers.insert("baggage", HeaderValue::from_static("tenant=secret"));
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_static("Bearer secret"),
    );
    let extractor = HeaderExtractor(&headers);

    assert!(extractor.get("traceparent").is_some());
    assert_eq!(extractor.get("baggage"), None);
    assert_eq!(extractor.get("authorization"), None);
    assert_eq!(extractor.keys(), vec!["traceparent"]);
}

#[tokio::test]
async fn http_request_span_preserves_remote_parent_trace_identity() {
    use opentelemetry::trace::{SpanId, TraceId, TracerProvider as _};
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tower::ServiceExt as _;
    use tracing::instrument::WithSubscriber as _;
    use tracing_subscriber::layer::SubscriberExt as _;

    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("http-parent-test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let state = Arc::new(AppState::default());

    let response = build_router(state)
        .oneshot(
            Request::get("/health")
                .header(
                    "traceparent",
                    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .with_subscriber(subscriber)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    let request_span = spans
        .iter()
        .find(|span| span.name == "nornicdb.http.request")
        .expect("HTTP request span was not exported");
    assert_eq!(
        request_span.span_context.trace_id(),
        TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap()
    );
    assert_eq!(
        request_span.parent_span_id,
        SpanId::from_hex("00f067aa0ba902b7").unwrap()
    );
}

#[test]
fn procedure_modes_drive_statement_authorization_classification() {
    assert!(!statement_requires_write("CALL db.labels()"));
    assert!(statement_requires_write(
        "CALL db.create.setNodeVectorProperty(null, 'embedding', [])"
    ));
    assert!(!statement_requires_write("CALL dbms.procedures()"));
    assert!(statement_requires_admin("CALL dbms.procedures()"));
    assert!(!statement_requires_admin("CALL db.labels()"));
    assert!(statement_requires_write("CALL extension.unknown()"));
}

fn encode_bolt_message(signature: u8, fields: &[copperdb_bolt::packstream::Value]) -> Vec<u8> {
    use bytes::BytesMut;

    let mut bytes = BytesMut::new();
    copperdb_bolt::packstream::encode_struct_header(&mut bytes, fields.len(), signature);
    for field in fields {
        copperdb_bolt::packstream::encode_value(&mut bytes, field);
    }
    bytes.to_vec()
}

fn encode_bolt_chunks(message: &[u8]) -> Vec<u8> {
    let mut chunks = Vec::with_capacity(message.len() + 4);
    chunks.extend_from_slice(&(message.len() as u16).to_be_bytes());
    chunks.extend_from_slice(message);
    chunks.extend_from_slice(&0u16.to_be_bytes());
    chunks
}

#[tokio::test]
async fn request_context_middleware_owns_http_request_cancellation() {
    use axum::{body::Body, http::Request, routing::get, Extension, Router};
    use tower::ServiceExt;

    let captured = Arc::new(Mutex::new(None));
    let handler_capture = Arc::clone(&captured);
    let app = Router::new()
        .route(
            "/",
            get(move |Extension(context): Extension<RequestContext>| {
                let handler_capture = Arc::clone(&handler_capture);
                async move {
                    assert!(!context.request_id().is_empty());
                    assert!(!context.is_cancelled());
                    *handler_capture.lock() = Some(context);
                    StatusCode::NO_CONTENT
                }
            }),
        )
        .layer(middleware::from_fn_with_state(
            Arc::new(AppState::default()),
            request_context_middleware,
        ));

    let response = app
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let context = captured.lock().clone().expect("handler captured context");
    assert!(context.is_cancelled());
}

#[tokio::test]
async fn request_context_middleware_cancels_dropped_http_request() {
    use axum::{body::Body, http::Request, routing::get, Extension, Router};
    use tower::ServiceExt;

    let (context_tx, context_rx) = tokio::sync::oneshot::channel();
    let context_tx = Arc::new(Mutex::new(Some(context_tx)));
    let handler_tx = Arc::clone(&context_tx);
    let state = Arc::new(AppState::default());
    let telemetry = Arc::clone(&state.telemetry);
    let app = Router::new()
        .route(
            "/",
            get(move |Extension(context): Extension<RequestContext>| {
                let handler_tx = Arc::clone(&handler_tx);
                async move {
                    handler_tx
                        .lock()
                        .take()
                        .expect("handler sends context once")
                        .send(context)
                        .expect("test receives context");
                    std::future::pending::<StatusCode>().await
                }
            }),
        )
        .layer(middleware::from_fn_with_state(
            state,
            request_context_middleware,
        ));

    let request_task =
        tokio::spawn(app.oneshot(Request::builder().uri("/").body(Body::empty()).unwrap()));
    let context = context_rx.await.expect("handler started");
    assert!(!context.is_cancelled());

    request_task.abort();
    assert!(request_task.await.unwrap_err().is_cancelled());
    assert!(context.is_cancelled());
    assert_eq!(
        telemetry
            .snapshot_metric("copperdb_request_cancellations_total")
            .unwrap(),
        vec![copperdb_otel::MetricSample {
            labels: vec![
                ("protocol".into(), "http".into()),
                ("reason".into(), "explicit".into()),
                ("stage".into(), "ingress".into()),
            ],
            value: copperdb_otel::MetricValue::Counter(1.0),
        }]
    );
}

#[test]
fn request_context_middleware_selects_upstream_route_timeouts() {
    let status = http_request_timeout("/status").expect("status timeout");
    assert_eq!(status.duration, Duration::from_secs(5));
    assert_eq!(status.message, "request timeout: status busy");

    for path in ["/copperdb/search", "/db/copperdb/search"] {
        let search = http_request_timeout(path).expect("search timeout");
        assert_eq!(search.duration, Duration::from_secs(20));
        assert_eq!(search.message, "request timeout: search busy");
    }

    assert!(http_request_timeout("/health").is_none());
    assert!(http_request_timeout("/admin/retention/status").is_none());
    assert!(http_request_timeout("/admin/fabric/databases/t/d/ranked-search").is_none());
}

#[test]
fn transaction_request_timeout_accepts_only_positive_duration_overrides() {
    assert_eq!(
        transaction_request_timeout_from("5ms", "10ms"),
        Duration::from_millis(5)
    );
    assert_eq!(
        transaction_request_timeout_from("", "10ms"),
        Duration::from_millis(10)
    );
    assert_eq!(
        transaction_request_timeout_from("0ms", "0ms"),
        DEFAULT_TRANSACTION_REQUEST_TIMEOUT
    );
    assert_eq!(
        transaction_request_timeout_from("invalid", "invalid"),
        DEFAULT_TRANSACTION_REQUEST_TIMEOUT
    );
}

#[tokio::test]
async fn transaction_handler_maps_engine_cancellation_to_upstream_timeout_response() {
    use axum::body::to_bytes;
    use axum::response::IntoResponse;

    let request_context = RequestContext::detached();
    request_context.cancel();
    let mut state = AppState::default();
    state.auth.security_enabled = false;
    let response = neo4j_tx_commit_handler(
        State(Arc::new(state)),
        Path("copperdb".into()),
        Extension(request_context),
        HeaderMap::new(),
        Json(Neo4jCommitRequest {
            statements: vec![Neo4jStatement {
                statement: "RETURN 1 AS value".into(),
                parameters: None,
            }],
            bookmarks: Vec::new(),
        }),
    )
    .await
    .into_response();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    assert_eq!(&body[..], b"request timeout: transaction busy");
}

#[tokio::test]
async fn request_context_middleware_returns_timeout_and_cancels_context() {
    use axum::{body::Body, http::Request, routing::get, Extension, Router};
    use tower::ServiceExt;

    let (context_tx, context_rx) = tokio::sync::oneshot::channel();
    let context_tx = Arc::new(Mutex::new(Some(context_tx)));
    let handler_tx = Arc::clone(&context_tx);
    let timeout = HttpRequestTimeout {
        duration: Duration::from_millis(10),
        message: "request timeout: transaction busy",
    };
    let telemetry = Arc::new(Telemetry::new());
    let middleware_telemetry = Arc::clone(&telemetry);
    let app = Router::new()
        .route(
            "/",
            get(move |Extension(context): Extension<RequestContext>| {
                let handler_tx = Arc::clone(&handler_tx);
                async move {
                    assert!(context.deadline().is_some());
                    assert!(context.check_active().is_ok());
                    handler_tx
                        .lock()
                        .take()
                        .expect("handler sends context once")
                        .send(context)
                        .expect("test receives context");
                    std::future::pending::<StatusCode>().await
                }
            }),
        )
        .layer(middleware::from_fn(move |request, next| {
            let telemetry = Arc::clone(&middleware_telemetry);
            async move {
                run_with_request_context(request, next, Some(timeout), telemetry.as_ref()).await
            }
        }));

    let request_task =
        tokio::spawn(app.oneshot(Request::builder().uri("/").body(Body::empty()).unwrap()));
    let context = context_rx.await.expect("handler started");
    let response = tokio::time::timeout(Duration::from_secs(1), request_task)
        .await
        .expect("middleware completed within bound")
        .expect("request task completed")
        .expect("router returned response");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(response.into_body(), 1024)
        .await
        .unwrap();
    assert_eq!(&body[..], b"request timeout: transaction busy");
    assert!(context.is_cancelled());
    assert!(context.check_active().is_err());
    assert_eq!(
        context.cancellation_reason(),
        Some(copperdb_util::RequestCancellationReason::Deadline)
    );
    assert_eq!(
        telemetry
            .snapshot_metric("copperdb_request_cancellations_total")
            .unwrap(),
        vec![copperdb_otel::MetricSample {
            labels: vec![
                ("protocol".into(), "http".into()),
                ("reason".into(), "deadline".into()),
                ("stage".into(), "ingress".into()),
            ],
            value: copperdb_otel::MetricValue::Counter(1.0),
        }]
    );
}

#[tokio::test]
async fn graphql_handler_executes_with_ingress_request_context() {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    let mut state = AppState::default();
    state.auth.security_enabled = false;
    let response = build_router(Arc::new(state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/graphql")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":"{ nodes { id } }"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(payload.get("errors").is_none(), "{payload}");
    assert!(payload["data"]["nodes"].is_array());
}

#[tokio::test]
async fn copperdb_search_returns_not_found_for_an_authorized_missing_database() {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    let mut state = AppState::default();
    state.auth.security_enabled = false;
    let response = build_router(Arc::new(state.clone()))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/copperdb/search")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "database": "missing-db",
                        "query": "hello"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["error"], "database not found: missing-db");
}

#[tokio::test]
async fn copperdb_search_reports_not_ready_when_no_search_indexes_are_declared() {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("cold-db")
        .to_string_lossy()
        .into_owned();
    let db_manager = Arc::new(DatabaseManager::new());
    db_manager.create("cold-db", storage_path).unwrap();
    let mut state = AppState {
        db_name: "cold-db".into(),
        db_manager,
        ..Default::default()
    };
    state.auth.security_enabled = false;

    let response = build_router(Arc::new(state.clone()))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/copperdb/search")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"query": "hello"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["database"], "cold-db");
    assert_eq!(payload["retryable"], true);
    assert_eq!(payload["request_status"], "search_not_ready");
    assert_eq!(
        state
            .telemetry
            .snapshot_metric("nornicdb_search_requests_total")
            .unwrap(),
        Vec::<copperdb_otel::MetricSample>::new()
    );
}

#[tokio::test]
async fn copperdb_search_accepts_direct_vector_when_embedding_is_disabled() {
    use axum::{body::Body, http::Request};
    use copperdb_storage::{
        IndexDefinition, IndexEntityType, IndexKind, NodeRecord, StorageEngine,
    };
    use tower::ServiceExt;

    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("vector-only")
        .to_string_lossy()
        .into_owned();
    let db_manager = Arc::new(DatabaseManager::new());
    db_manager
        .create("vector-only", storage_path.clone())
        .unwrap();
    db_manager
        .set_config_overrides(
            "vector-only",
            BTreeMap::from([
                ("COPPERDB_SEARCH_BM25_ENABLED".into(), "false".into()),
                ("COPPERDB_SEARCH_VECTOR_ENABLED".into(), "true".into()),
                ("COPPERDB_EMBEDDING_ENABLED".into(), "false".into()),
            ]),
        )
        .unwrap();
    let mut state = AppState {
        db_name: "vector-only".into(),
        db_manager,
        ..Default::default()
    };
    state.auth.security_enabled = false;
    let storage = StorageEngine::open(&storage_path).unwrap();
    storage
        .persist_index_definition(&IndexDefinition {
            name: "document_embedding".into(),
            entity_type: IndexEntityType::Node,
            label: "Document".into(),
            properties: vec!["embedding".into()],
            kind: IndexKind::Vector,
        })
        .unwrap();
    storage
        .persist_index_options(
            "document_embedding",
            &std::collections::HashMap::from([(
                "indexConfig".into(),
                serde_json::json!({
                    "vector.dimensions": 3,
                    "vector.similarity_function": "cosine"
                }),
            )]),
        )
        .unwrap();
    storage
        .put_node_record(&NodeRecord {
            id: "document:vector".into(),
            labels: vec!["Document".into()],
            properties: BTreeMap::new(),
            named_embeddings: BTreeMap::from([("embedding".into(), vec![1.0, 0.0, 0.0])]),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();
    drop(storage);

    let response = build_router(Arc::new(state.clone()))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/copperdb/search")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"vector": [1.0, 0.0, 0.0]}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload.as_array().unwrap().len(), 1);
    assert_eq!(payload[0]["node"]["id"], "document:vector");
    assert_eq!(payload[0]["vector_rank"], 1);
    assert_eq!(payload[0]["bm25_rank"], 0);

    let diagnostics_response = build_router(Arc::new(state.clone()))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/copperdb/search")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "vector": [1.0, 0.0, 0.0],
                        "include_diagnostics": true
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(diagnostics_response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(diagnostics_response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["results"][0]["node"]["id"], "document:vector");
    assert_eq!(payload["diagnostics"]["status"], "success");
    assert_eq!(payload["diagnostics"]["search_method"], "semantic");
    assert_eq!(payload["diagnostics"]["ready"], true);
    assert_eq!(payload["diagnostics"]["input_candidates"], 1);
    assert_eq!(payload["diagnostics"]["fused_candidates"], 1);
    assert_eq!(payload["diagnostics"]["output_candidates"], 1);
    assert_eq!(payload["diagnostics"]["filtered_candidates"], 0);
    assert_eq!(payload["diagnostics"]["returned"], 1);
    assert_eq!(payload["diagnostics"]["partial"], false);
    for stage in ["embedding_ms", "index_ms", "hydration_ms"] {
        assert!(payload["diagnostics"]["timings"][stage].is_u64());
    }
    assert_eq!(
        state
            .telemetry
            .snapshot_metric("nornicdb_search_requests_total")
            .unwrap(),
        vec![copperdb_otel::MetricSample {
            labels: vec![
                ("mode".to_string(), "vector".to_string()),
                ("result".to_string(), "success".to_string()),
            ],
            value: copperdb_otel::MetricValue::Counter(2.0),
        }]
    );
}

#[tokio::test]
async fn copperdb_search_applies_min_score_to_direct_vector_search() {
    use axum::{body::Body, http::Request};
    use copperdb_storage::{
        IndexDefinition, IndexEntityType, IndexKind, NodeRecord, StorageEngine,
    };
    use tower::ServiceExt;

    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("vector-min-score")
        .to_string_lossy()
        .into_owned();
    let db_manager = Arc::new(DatabaseManager::new());
    db_manager
        .create("vector-min-score", storage_path.clone())
        .unwrap();
    db_manager
        .set_config_overrides(
            "vector-min-score",
            BTreeMap::from([
                ("COPPERDB_SEARCH_BM25_ENABLED".into(), "false".into()),
                ("COPPERDB_SEARCH_VECTOR_ENABLED".into(), "true".into()),
                ("COPPERDB_EMBEDDING_ENABLED".into(), "false".into()),
            ]),
        )
        .unwrap();
    let mut state = AppState {
        db_name: "vector-min-score".into(),
        db_manager,
        ..Default::default()
    };
    state.auth.security_enabled = false;
    let storage = StorageEngine::open(&storage_path).unwrap();
    storage
        .persist_index_definition(&IndexDefinition {
            name: "document_embedding".into(),
            entity_type: IndexEntityType::Node,
            label: "Document".into(),
            properties: vec!["embedding".into()],
            kind: IndexKind::Vector,
        })
        .unwrap();
    storage
        .persist_index_options(
            "document_embedding",
            &std::collections::HashMap::from([(
                "indexConfig".into(),
                serde_json::json!({
                    "vector.dimensions": 3,
                    "vector.similarity_function": "cosine"
                }),
            )]),
        )
        .unwrap();
    storage
        .put_node_record(&NodeRecord {
            id: "document:vector".into(),
            labels: vec!["Document".into()],
            properties: BTreeMap::new(),
            named_embeddings: BTreeMap::from([("embedding".into(), vec![1.0, 0.0, 0.0])]),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();
    drop(storage);

    let response = build_router(Arc::new(state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/copperdb/search")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "vector": [1.0, 0.0, 0.0],
                        "min_score": 1.1
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
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload, serde_json::json!([]));
}

#[tokio::test]
async fn copperdb_search_applies_offset_before_hydration() {
    use axum::{body::Body, http::Request};
    use copperdb_storage::{
        IndexDefinition, IndexEntityType, IndexKind, NodeRecord, StorageEngine,
    };
    use tower::ServiceExt;

    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("fulltext-offset")
        .to_string_lossy()
        .into_owned();
    let db_manager = Arc::new(DatabaseManager::new());
    db_manager
        .create("fulltext-offset", storage_path.clone())
        .unwrap();
    db_manager
        .set_config_overrides(
            "fulltext-offset",
            BTreeMap::from([("COPPERDB_SEARCH_BM25_ENABLED".into(), "true".into())]),
        )
        .unwrap();
    let mut state = AppState {
        db_name: "fulltext-offset".into(),
        db_manager,
        ..Default::default()
    };
    state.auth.security_enabled = false;
    let storage = StorageEngine::open(&storage_path).unwrap();
    storage
        .persist_index_definition(&IndexDefinition {
            name: "document_title".into(),
            entity_type: IndexEntityType::Node,
            label: "Document".into(),
            properties: vec!["title".into()],
            kind: IndexKind::FullText,
        })
        .unwrap();
    for id in ["document:a", "document:b"] {
        storage
            .put_node_record(&NodeRecord {
                id: id.into(),
                labels: vec!["Document".into()],
                properties: BTreeMap::from([(
                    "title".into(),
                    serde_json::Value::String("graph".into()),
                )]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
    }
    drop(storage);

    let response = build_router(Arc::new(state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/copperdb/search")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "query": "graph",
                        "limit": 1,
                        "offset": 1
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
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload.as_array().unwrap().len(), 1);
    assert_eq!(payload[0]["node"]["id"], "document:b");
}

#[tokio::test]
async fn copperdb_search_combines_text_and_direct_vector_when_embedding_is_disabled() {
    use axum::{body::Body, http::Request};
    use copperdb_storage::{
        IndexDefinition, IndexEntityType, IndexKind, NodeRecord, StorageEngine,
    };
    use tower::ServiceExt;

    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("direct-hybrid")
        .to_string_lossy()
        .into_owned();
    let db_manager = Arc::new(DatabaseManager::new());
    db_manager
        .create("direct-hybrid", storage_path.clone())
        .unwrap();
    db_manager
        .set_config_overrides(
            "direct-hybrid",
            BTreeMap::from([
                ("COPPERDB_SEARCH_BM25_ENABLED".into(), "true".into()),
                ("COPPERDB_SEARCH_VECTOR_ENABLED".into(), "true".into()),
                ("COPPERDB_EMBEDDING_ENABLED".into(), "false".into()),
            ]),
        )
        .unwrap();
    let mut state = AppState {
        db_name: "direct-hybrid".into(),
        db_manager,
        ..Default::default()
    };
    state.auth.security_enabled = false;

    let storage = StorageEngine::open(&storage_path).unwrap();
    storage
        .persist_index_definition(&IndexDefinition {
            name: "document_title".into(),
            entity_type: IndexEntityType::Node,
            label: "Document".into(),
            properties: vec!["title".into()],
            kind: IndexKind::FullText,
        })
        .unwrap();
    storage
        .persist_index_definition(&IndexDefinition {
            name: "document_embedding".into(),
            entity_type: IndexEntityType::Node,
            label: "Document".into(),
            properties: vec!["embedding".into()],
            kind: IndexKind::Vector,
        })
        .unwrap();
    storage
        .persist_index_options(
            "document_embedding",
            &std::collections::HashMap::from([(
                "indexConfig".into(),
                serde_json::json!({
                    "vector.dimensions": 3,
                    "vector.similarity_function": "cosine"
                }),
            )]),
        )
        .unwrap();
    storage
        .put_node_record(&NodeRecord {
            id: "document:hybrid".into(),
            labels: vec!["Document".into()],
            properties: BTreeMap::from([(
                "title".into(),
                serde_json::Value::String("graph database".into()),
            )]),
            named_embeddings: BTreeMap::from([("embedding".into(), vec![1.0, 0.0, 0.0])]),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();
    drop(storage);

    let response = build_router(Arc::new(state.clone()))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/db/direct-hybrid/search")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "query": "graph",
                        "vector": [1.0, 0.0, 0.0],
                        "include_diagnostics": true
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
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["results"].as_array().unwrap().len(), 1);
    assert_eq!(payload["results"][0]["node"]["id"], "document:hybrid");
    assert_eq!(payload["results"][0]["bm25_rank"], 1);
    assert_eq!(payload["results"][0]["vector_rank"], 1);
    assert_eq!(payload["diagnostics"]["status"], "success");
    assert_eq!(payload["diagnostics"]["search_method"], "hybrid");
    assert_eq!(payload["diagnostics"]["ready"], true);
    assert_eq!(
        payload["diagnostics"]["sources"],
        serde_json::json!(["lexical", "semantic"])
    );
    assert_eq!(payload["diagnostics"]["input_candidates"], 2);
    assert_eq!(payload["diagnostics"]["fused_candidates"], 1);
    assert_eq!(payload["diagnostics"]["output_candidates"], 1);
    assert_eq!(payload["diagnostics"]["filtered_candidates"], 0);
    assert_eq!(payload["diagnostics"]["returned"], 1);
    assert_eq!(payload["diagnostics"]["partial"], false);
    for stage in ["embedding_ms", "index_ms", "hydration_ms"] {
        assert!(payload["diagnostics"]["timings"][stage].is_u64());
    }

    let semantic_response = build_router(Arc::new(state.clone()))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/db/direct-hybrid/search")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "mode": "semantic",
                        "query": "graph",
                        "vector": [1.0, 0.0, 0.0]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(semantic_response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(semantic_response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload.as_array().unwrap().len(), 1);
    assert_eq!(payload[0]["node"]["id"], "document:hybrid");
    assert_eq!(payload[0]["bm25_rank"], 0);
    assert_eq!(payload[0]["vector_rank"], 1);
    assert_eq!(
        state
            .telemetry
            .snapshot_metric("nornicdb_search_candidates_rows")
            .unwrap(),
        vec![copperdb_otel::MetricSample {
            labels: Vec::new(),
            value: copperdb_otel::MetricValue::Gauge(1.0),
        }]
    );
    assert_eq!(
        state
            .telemetry
            .snapshot_metric("nornicdb_search_requests_total")
            .unwrap(),
        vec![
            copperdb_otel::MetricSample {
                labels: vec![
                    ("mode".to_string(), "hybrid".to_string()),
                    ("result".to_string(), "success".to_string()),
                ],
                value: copperdb_otel::MetricValue::Counter(1.0),
            },
            copperdb_otel::MetricSample {
                labels: vec![
                    ("mode".to_string(), "vector".to_string()),
                    ("result".to_string(), "success".to_string()),
                ],
                value: copperdb_otel::MetricValue::Counter(1.0),
            },
        ]
    );
    let durations = state
        .telemetry
        .snapshot_metric("nornicdb_search_duration_seconds")
        .unwrap();
    assert_eq!(durations.len(), 4);
    for mode in ["hybrid", "vector"] {
        for stage in ["embed", "index"] {
            assert!(durations.iter().any(|sample| {
                matches!(
                    sample,
                    copperdb_otel::MetricSample {
                        labels,
                        value: copperdb_otel::MetricValue::Histogram { count, .. },
                    } if labels == &vec![
                        ("mode".to_string(), mode.to_string()),
                        ("stage".to_string(), stage.to_string()),
                    ] && *count == 1
                )
            }));
        }
    }

    let weighted_response = build_router(Arc::new(state.clone()))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/db/direct-hybrid/search")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "mode": "hybrid",
                        "query": "graph",
                        "vector": [1.0, 0.0, 0.0],
                        "rrf_k": 30.0,
                        "vector_weight": 2.0,
                        "bm25_weight": 0.5
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(weighted_response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(weighted_response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload[0]["node"]["id"], "document:hybrid");

    let request_context = RequestContext::detached();
    request_context.cancel();
    let cancelled_response = search_handler(
        Arc::new(state),
        request_context,
        HeaderMap::new(),
        SearchRequest {
            database: "direct-hybrid".into(),
            query: "graph".into(),
            ..SearchRequest::default()
        },
        None,
    )
    .await;

    assert_eq!(cancelled_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(cancelled_response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload, serde_json::json!({"error": "request cancelled"}));
}

#[tokio::test]
async fn copperdb_search_rejects_empty_text_without_a_direct_vector() {
    use axum::{body::Body, http::Request};
    use copperdb_storage::{IndexDefinition, IndexEntityType, IndexKind, StorageEngine};
    use tower::ServiceExt;

    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("empty-query")
        .to_string_lossy()
        .into_owned();
    let db_manager = Arc::new(DatabaseManager::new());
    db_manager
        .create("empty-query", storage_path.clone())
        .unwrap();
    let mut state = AppState {
        db_name: "empty-query".into(),
        db_manager,
        ..Default::default()
    };
    state.auth.security_enabled = false;

    let storage = StorageEngine::open(&storage_path).unwrap();
    storage
        .persist_index_definition(&IndexDefinition {
            name: "document_title".into(),
            entity_type: IndexEntityType::Node,
            label: "Document".into(),
            properties: vec!["title".into()],
            kind: IndexKind::FullText,
        })
        .unwrap();
    drop(storage);

    let response = build_router(Arc::new(state.clone()))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/copperdb/search")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::json!({}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload["error"],
        "search requires non-empty query text or a query vector"
    );
    assert_eq!(
        state
            .telemetry
            .snapshot_metric("nornicdb_search_requests_total")
            .unwrap(),
        Vec::<copperdb_otel::MetricSample>::new()
    );
}

#[tokio::test]
async fn copperdb_search_records_unavailable_query_embedding_before_mode_selection() {
    use axum::{body::Body, http::Request};
    use copperdb_storage::{IndexDefinition, IndexEntityType, IndexKind, StorageEngine};
    use tower::ServiceExt;

    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("embedding-failure")
        .to_string_lossy()
        .into_owned();
    let db_manager = Arc::new(DatabaseManager::new());
    db_manager
        .create("embedding-failure", storage_path.clone())
        .unwrap();
    db_manager
        .set_config_overrides(
            "embedding-failure",
            BTreeMap::from([
                ("COPPERDB_SEARCH_BM25_ENABLED".into(), "false".into()),
                ("COPPERDB_SEARCH_VECTOR_ENABLED".into(), "true".into()),
                ("COPPERDB_EMBEDDING_ENABLED".into(), "false".into()),
            ]),
        )
        .unwrap();
    let mut state = AppState {
        db_name: "embedding-failure".into(),
        db_manager,
        ..Default::default()
    };
    state.auth.security_enabled = false;

    let storage = StorageEngine::open(&storage_path).unwrap();
    storage
        .persist_index_definition(&IndexDefinition {
            name: "document_embedding".into(),
            entity_type: IndexEntityType::Node,
            label: "Document".into(),
            properties: vec!["embedding".into()],
            kind: IndexKind::Vector,
        })
        .unwrap();
    storage
        .persist_index_options(
            "document_embedding",
            &std::collections::HashMap::from([(
                "indexConfig".into(),
                serde_json::json!({"vector.dimensions": 3}),
            )]),
        )
        .unwrap();
    drop(storage);

    let response = build_router(Arc::new(state.clone()))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/copperdb/search")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"query": "graph"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload["error"],
        "search requires non-empty query text or a query vector"
    );
    assert_eq!(
        state
            .telemetry
            .snapshot_metric("nornicdb_search_requests_total")
            .unwrap(),
        Vec::<copperdb_otel::MetricSample>::new()
    );
}

#[tokio::test]
async fn copperdb_search_records_selected_bm25_engine_failures() {
    use axum::{body::Body, http::Request};
    use copperdb_storage::{IndexDefinition, IndexEntityType, IndexKind};
    use tower::ServiceExt;

    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("bm25-disabled")
        .to_string_lossy()
        .into_owned();
    let db_manager = Arc::new(DatabaseManager::new());
    db_manager.create("bm25-disabled", storage_path).unwrap();
    db_manager
        .set_config_overrides(
            "bm25-disabled",
            BTreeMap::from([("COPPERDB_SEARCH_BM25_ENABLED".into(), "false".into())]),
        )
        .unwrap();
    let mut state = AppState {
        db_name: "bm25-disabled".into(),
        db_manager,
        ..Default::default()
    };
    state.auth.security_enabled = false;
    open_engine(&state, "bm25-disabled")
        .unwrap()
        .storage()
        .persist_index_definition(&IndexDefinition {
            name: "document_title".into(),
            entity_type: IndexEntityType::Node,
            label: "Document".into(),
            properties: vec!["title".into()],
            kind: IndexKind::FullText,
        })
        .unwrap();

    let response = build_router(Arc::new(state.clone()))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/copperdb/search")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"query": "hello"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload["error"],
        "configuration error: fulltext search is disabled for this database"
    );
    assert_eq!(
        state
            .telemetry
            .snapshot_metric("nornicdb_search_requests_total")
            .unwrap(),
        vec![copperdb_otel::MetricSample {
            labels: vec![
                ("mode".to_string(), "bm25".to_string()),
                ("result".to_string(), "error".to_string()),
            ],
            value: copperdb_otel::MetricValue::Counter(1.0),
        }]
    );
}

async fn read_bolt_message(stream: &mut tokio::net::TcpStream) -> copperdb_bolt::packstream::Value {
    use tokio::io::AsyncReadExt;

    let mut message = Vec::new();
    loop {
        let mut header = [0u8; 2];
        stream.read_exact(&mut header).await.unwrap();
        let length = u16::from_be_bytes(header) as usize;
        if length == 0 {
            break;
        }
        let start = message.len();
        message.resize(start + length, 0);
        stream.read_exact(&mut message[start..]).await.unwrap();
    }
    copperdb_bolt::packstream::decode(&message)
        .expect("Bolt response should be valid PackStream")
        .0
}

#[tokio::test]
async fn copperdb_search_endpoints_fall_back_to_bm25_when_query_embedding_is_disabled() {
    use axum::{body::Body, http::Request};
    use copperdb_storage::{IndexDefinition, IndexEntityType, IndexKind, NodeRecord};
    use tower::ServiceExt;

    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("copper")
        .to_string_lossy()
        .into_owned();
    let db_manager = Arc::new(DatabaseManager::new());
    db_manager.create("copper", storage_path).unwrap();
    db_manager
        .set_config_overrides(
            "copper",
            BTreeMap::from([
                ("COPPERDB_SEARCH_BM25_ENABLED".into(), "true".into()),
                ("COPPERDB_SEARCH_VECTOR_ENABLED".into(), "true".into()),
                ("COPPERDB_EMBEDDING_ENABLED".into(), "false".into()),
            ]),
        )
        .unwrap();
    let mut state = AppState {
        db_name: "copper".into(),
        db_manager,
        ..Default::default()
    };
    state.auth.security_enabled = false;
    let engine = open_engine(&state, "copper").unwrap();
    for definition in [
        IndexDefinition {
            name: "document_title".into(),
            entity_type: IndexEntityType::Node,
            label: "Document".into(),
            properties: vec!["title".into()],
            kind: IndexKind::FullText,
        },
        IndexDefinition {
            name: "document_embedding".into(),
            entity_type: IndexEntityType::Node,
            label: "Document".into(),
            properties: vec!["embedding".into()],
            kind: IndexKind::Vector,
        },
        IndexDefinition {
            name: "document_summary".into(),
            entity_type: IndexEntityType::Node,
            label: "Document".into(),
            properties: vec!["summary".into()],
            kind: IndexKind::FullText,
        },
    ] {
        engine
            .storage()
            .persist_index_definition(&definition)
            .unwrap();
    }
    engine
        .storage()
        .put_node_record(&NodeRecord {
            id: "document:graph".into(),
            labels: vec!["Document".into()],
            properties: BTreeMap::from([
                (
                    "title".into(),
                    serde_json::Value::String("graph database internals".into()),
                ),
                (
                    "collection".into(),
                    serde_json::Value::String("keep".into()),
                ),
            ]),
            named_embeddings: BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();
    engine
        .storage()
        .put_node_record(&NodeRecord {
            id: "document:summary".into(),
            labels: vec!["Document".into()],
            properties: BTreeMap::from([
                (
                    "title".into(),
                    serde_json::Value::String("unrelated title".into()),
                ),
                (
                    "summary".into(),
                    serde_json::Value::String("graph graph graph graph".into()),
                ),
            ]),
            named_embeddings: BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();
    engine
        .storage()
        .put_node_record(&NodeRecord {
            id: "document:unrelated".into(),
            labels: vec!["Document".into()],
            properties: BTreeMap::from([
                (
                    "title".into(),
                    serde_json::Value::String("graph graph graph graph".into()),
                ),
                (
                    "collection".into(),
                    serde_json::Value::String("discard".into()),
                ),
            ]),
            named_embeddings: BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();

    let database_path_response = build_router(Arc::new(state.clone()))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/db/copper/search")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "query": "graph",
                        "labels": ["Document"]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(database_path_response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(database_path_response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload.as_array().unwrap().len(), 3);

    let response = build_router(Arc::new(state.clone()))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/copperdb/search")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "database": "copper",
                        "query": "graph",
                        "labels": ["Document"],
                        "limit": 1,
                        "include_diagnostics": true,
                        "filters": {"collection": ["keep"]}
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
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["results"].as_array().unwrap().len(), 1);
    assert_eq!(payload["results"][0]["node"]["id"], "document:graph");
    assert_eq!(payload["results"][0]["node"]["labels"][0], "Document");
    assert_eq!(payload["results"][0]["vector_rank"], 0);
    assert_eq!(payload["results"][0]["bm25_rank"], 1);
    assert_eq!(payload["diagnostics"]["status"], "success");
    assert_eq!(payload["diagnostics"]["search_method"], "bm25");
    assert_eq!(payload["diagnostics"]["ready"], true);
    assert_eq!(
        payload["diagnostics"]["sources"],
        serde_json::json!(["lexical"])
    );
    assert_eq!(payload["diagnostics"]["input_candidates"], 1);
    assert_eq!(payload["diagnostics"]["fused_candidates"], 1);
    assert_eq!(payload["diagnostics"]["output_candidates"], 1);
    assert_eq!(payload["diagnostics"]["filtered_candidates"], 2);
    assert_eq!(payload["diagnostics"]["returned"], 1);
    assert_eq!(payload["diagnostics"]["partial"], false);
    for stage in ["embedding_ms", "index_ms", "hydration_ms"] {
        assert!(payload["diagnostics"]["timings"][stage].is_u64());
    }

    let no_results_response = build_router(Arc::new(state.clone()))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/copperdb/search")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "database": "copper",
                        "query": "absent",
                        "labels": ["Document"],
                        "include_diagnostics": true
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(no_results_response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(no_results_response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(payload["results"].as_array().unwrap().is_empty());
    assert_eq!(payload["diagnostics"]["status"], "no_results");
    assert_eq!(payload["diagnostics"]["search_method"], "bm25");
    assert_eq!(payload["diagnostics"]["ready"], true);
    assert_eq!(
        payload["diagnostics"]["sources"],
        serde_json::json!(["lexical"])
    );
    assert_eq!(payload["diagnostics"]["input_candidates"], 0);
    assert_eq!(payload["diagnostics"]["fused_candidates"], 0);
    assert_eq!(payload["diagnostics"]["output_candidates"], 0);
    assert_eq!(payload["diagnostics"]["filtered_candidates"], 0);
    assert_eq!(payload["diagnostics"]["returned"], 0);
    assert_eq!(payload["diagnostics"]["partial"], false);

    let selected_index_response = build_router(Arc::new(state.clone()))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/copperdb/search")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "database": "copper",
                        "query": "graph",
                        "indexes": ["document_summary"],
                        "limit": 1
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(selected_index_response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(selected_index_response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload.as_array().unwrap().len(), 1);
    assert_eq!(payload[0]["node"]["id"], "document:summary");

    let invalid_index_response = build_router(Arc::new(state.clone()))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/copperdb/search")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "database": "copper",
                        "query": "graph",
                        "indexes": ["missing_index"]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(invalid_index_response.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(invalid_index_response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload["error"],
        "indexes must name declared node FULLTEXT or VECTOR indexes"
    );

    assert_eq!(
        state
            .telemetry
            .snapshot_metric("nornicdb_search_requests_total")
            .unwrap(),
        vec![
            copperdb_otel::MetricSample {
                labels: vec![
                    ("mode".to_string(), "bm25".to_string()),
                    ("result".to_string(), "no_results".to_string()),
                ],
                value: copperdb_otel::MetricValue::Counter(1.0),
            },
            copperdb_otel::MetricSample {
                labels: vec![
                    ("mode".to_string(), "bm25".to_string()),
                    ("result".to_string(), "success".to_string()),
                ],
                value: copperdb_otel::MetricValue::Counter(3.0),
            },
        ]
    );
    assert_eq!(
        state
            .telemetry
            .snapshot_metric("nornicdb_search_candidates_rows")
            .unwrap(),
        vec![copperdb_otel::MetricSample {
            labels: Vec::new(),
            value: copperdb_otel::MetricValue::Gauge(1.0),
        }]
    );
    let durations = state
        .telemetry
        .snapshot_metric("nornicdb_search_duration_seconds")
        .unwrap();
    assert_eq!(durations.len(), 2);
    assert!(durations.iter().all(|sample| {
        matches!(
            sample,
            copperdb_otel::MetricSample {
                labels,
                value: copperdb_otel::MetricValue::Histogram { count, .. },
            } if labels.iter().any(|(key, value)| key == "mode" && value == "bm25")
                && *count == 4
        )
    }));
}

#[test]
fn open_engine_maps_storage_sync_writes_to_immediate_wal_durability() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("copper")
        .to_string_lossy()
        .into_owned();
    let db_manager = Arc::new(DatabaseManager::new());
    db_manager.create("copper", storage_path).unwrap();
    let mut runtime_config = RuntimeConfig::default();
    runtime_config.storage.sync_writes = true;
    let state = AppState {
        db_name: "copper".into(),
        db_manager,
        runtime_config: Arc::new(runtime_config),
        ..Default::default()
    };

    let engine = open_engine(&state, "copper").unwrap();
    assert_eq!(
        engine.storage().wal_sync_mode(),
        copperdb_storage::WALSyncMode::Immediate
    );
}

#[test]
fn offline_wal_maintenance_refuses_cached_engines_and_formats_integrity_status() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("copper")
        .to_string_lossy()
        .into_owned();
    let db_manager = Arc::new(DatabaseManager::new());
    db_manager.create("copper", storage_path.clone()).unwrap();
    let state = AppState {
        db_name: "copper".into(),
        db_manager,
        ..Default::default()
    };

    assert_eq!(
        offline_database_storage_path(&state, "copper").unwrap(),
        storage_path
    );
    open_engine(&state, "copper").unwrap();
    assert_eq!(
        offline_database_storage_path(&state, "copper").unwrap_err(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        wal_integrity_response(copperdb_storage::WALIntegrityStatus::ChecksumCorrupt {
            applied_sequence: 3,
            corrupted_sequence: 4,
        }),
        serde_json::json!({
            "status": "checksum_corrupt",
            "applied_sequence": 3,
            "corrupted_sequence": 4,
        })
    );
}

#[test]
fn mvcc_lifecycle_status_uses_live_storage_state_and_parses_ui_intervals() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("copper")
        .to_string_lossy()
        .into_owned();
    let db_manager = Arc::new(DatabaseManager::new());
    db_manager.create("copper", storage_path).unwrap();
    let state = AppState {
        db_name: "copper".into(),
        db_manager,
        ..Default::default()
    };
    let engine = open_engine(&state, "copper").unwrap();
    engine.storage().pause_lifecycle();
    engine.storage().set_lifecycle_schedule_ms(2_000);

    assert_eq!(parse_mvcc_schedule_ms("250ms"), Some(250));
    assert_eq!(parse_mvcc_schedule_ms("2s"), Some(2_000));
    assert_eq!(parse_mvcc_schedule_ms("3m"), Some(180_000));
    assert_eq!(parse_mvcc_schedule_ms("invalid"), None);
    assert_eq!(
        mvcc_lifecycle_response("copper", &engine),
        serde_json::json!({
            "database": "copper",
            "enabled": true,
            "running": false,
            "paused": true,
            "automatic": true,
            "cycle_interval": "2000ms",
            "mvcc_active_snapshot_readers": 0,
            "mvcc_compaction_debt_keys": 0,
            "mvcc_prunable_bytes_total": 0,
            "mvcc_floor_lag_versions": 0,
            "head": 0,
            "floor": 0,
            "oldest_active_reader": serde_json::Value::Null,
            "retained_versions": 0,
            "suggested_prune_floor": 0,
        })
    );
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
async fn liveness_and_readiness_probes_are_public_and_report_database_availability() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use copperdb_otel::Health;
    use tower::ServiceExt;

    let health = Arc::new(Health::new());
    let ready_app = build_telemetry_router(Arc::clone(&health));

    let response = ready_app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/livez")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    assert!(body.is_empty());

    let response = ready_app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/livez")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = ready_app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let readiness: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(readiness["ok"], true);
    assert_eq!(readiness["checks"], serde_json::json!({}));

    health.register("info", false, || Err("informational failure".into()));
    health.register("required", true, || Err("required failure".into()));
    let response = ready_app
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let readiness: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(readiness["ok"], false);
    assert_eq!(readiness["checks"]["info"]["ok"], false);
    assert_eq!(
        readiness["checks"]["info"]["error"],
        "informational failure"
    );
    assert_eq!(readiness["checks"]["required"]["error"], "required failure");
}

#[tokio::test]
async fn observability_router_negotiates_metrics_and_reports_version() {
    use axum::body::Body;
    use axum::http::{header, Request, StatusCode};
    use tower::ServiceExt;

    let telemetry = Arc::new(Telemetry::new());
    telemetry
        .record_counter(
            "nornicdb_http_requests_total",
            &[
                ("method", "GET"),
                ("path_template", "/health"),
                ("status_class", "2xx"),
            ],
        )
        .unwrap();
    let app = build_observability_router(
        Arc::new(Health::new()),
        telemetry,
        true,
        "instance-1".into(),
    );

    let prometheus = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(prometheus.status(), StatusCode::OK);
    assert!(prometheus.headers()[header::CONTENT_TYPE]
        .to_str()
        .unwrap()
        .starts_with("text/plain"));
    let body = axum::body::to_bytes(prometheus.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("nornicdb_http_requests_total"));
    assert!(!body.contains("# EOF"));

    let openmetrics = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .header(header::ACCEPT, "application/openmetrics-text")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(openmetrics.headers()[header::CONTENT_TYPE]
        .to_str()
        .unwrap()
        .starts_with("application/openmetrics-text"));
    let body = axum::body::to_bytes(openmetrics.into_body(), 1024 * 1024)
        .await
        .unwrap();
    assert!(std::str::from_utf8(&body).unwrap().ends_with("# EOF\n"));

    let version = app
        .oneshot(
            Request::builder()
                .uri("/version")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(version.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let version: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(version["service_instance_id"], "instance-1");
    assert_eq!(version.as_object().unwrap().len(), 5);
}

#[tokio::test]
async fn observability_router_omits_metrics_when_disabled() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_observability_router(
        Arc::new(Health::new()),
        Arc::new(Telemetry::new()),
        false,
        "instance-1".into(),
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
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
        .clone()
        .oneshot(
            Request::builder()
                .uri("/status")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/db/copperdb/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::COOKIE, &cookie)
                .body(Body::from(
                    serde_json::json!({
                        "statements": [{"statement": "MATCH (n) RETURN n LIMIT 1"}]
                    })
                    .to_string(),
                ))
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
                    filtered_hits: 0,
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
                    filtered_hits: 0,
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
    let telemetry = Arc::clone(&state.telemetry);
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
                    placement: placement.clone(),
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

    let mut cancelled_request = Request::new(
        proto::RemoteRankedSearchRequest::try_from(RemoteRankedSearchRequest {
            target_node: "search-a".into(),
            target_addr: "127.0.0.1:50051".into(),
            placement,
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
    );
    cancelled_request.metadata_mut().insert(
        "x-copperdb-request-id",
        "expired-ranked-search".parse().unwrap(),
    );
    cancelled_request
        .metadata_mut()
        .insert("x-copperdb-request-deadline-ms", "0".parse().unwrap());
    let cancelled = service.search_ranked(cancelled_request).await.unwrap_err();
    assert_eq!(cancelled.code(), tonic::Code::Cancelled);
    assert_eq!(cancelled.message(), "request cancelled");
    assert_eq!(
        telemetry
            .snapshot_metric("copperdb_request_cancellations_total")
            .unwrap(),
        vec![copperdb_otel::MetricSample {
            labels: vec![
                ("protocol".into(), "grpc".into()),
                ("reason".into(), "deadline".into()),
                ("stage".into(), "execution".into()),
            ],
            value: copperdb_otel::MetricValue::Counter(1.0),
        }]
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
                    filtered_hits: 0,
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
                    filtered_hits: 0,
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
    assert!(err.to_string().contains("compliance error"));

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

/// Browser requests fall back to the embedded UI when a configured static
/// directory does not contain an index.
#[tokio::test]
async fn test_embedded_ui_served_when_static_directory_has_no_index() {
    use axum::body::Body;
    use axum::http::{header, Request};
    use tower::ServiceExt;

    // A custom directory takes precedence per file, then embedded assets fill misses.
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
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/html; charset=utf-8"
    );
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let html = std::str::from_utf8(&body).unwrap();
    assert!(html.contains("<!doctype html>") || html.contains("<!DOCTYPE html>"));
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

#[tokio::test]
async fn database_info_allows_unauthenticated_access_when_security_disabled() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use copperdb_storage::{EdgeRecord, IndexDefinition, IndexEntityType, IndexKind, NodeRecord};
    use tower::ServiceExt;

    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("copperdb")
        .to_string_lossy()
        .into_owned();
    let db_manager = Arc::new(DatabaseManager::new());
    db_manager.create("copperdb", storage_path).unwrap();
    let mut state = AppState {
        db_name: "copperdb".into(),
        db_manager,
        ..Default::default()
    };
    state.auth.security_enabled = false;
    let engine = open_engine(&state, "copperdb").unwrap();
    engine
        .storage()
        .persist_index_definition(&IndexDefinition {
            name: "document_title".into(),
            entity_type: IndexEntityType::Node,
            label: "Document".into(),
            properties: vec!["title".into()],
            kind: IndexKind::FullText,
        })
        .unwrap();
    engine
        .storage()
        .put_node_record(&NodeRecord {
            id: "document:1".into(),
            labels: vec!["Document".into()],
            properties: BTreeMap::from([(
                "title".into(),
                serde_json::Value::String("Operational status".into()),
            )]),
            named_embeddings: BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();
    engine
        .storage()
        .put_edge_record(&EdgeRecord {
            id: "edge:1".into(),
            start_node: "document:1".into(),
            end_node: "document:1".into(),
            edge_type: "REFERENCES".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();
    let state = Arc::new(state);
    let app = build_router(Arc::clone(&state));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/db/copperdb")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["searchReady"], true);
    assert_eq!(payload["searchBuilding"], false);
    assert_eq!(payload["searchInitialized"], true);
    assert_eq!(payload["schemaVersion"], STATUS_SCHEMA_VERSION);
    assert!(payload["collectedAtUnixMs"].as_u64().unwrap() > 0);
    assert_eq!(payload["nodeCount"], 1);
    assert_eq!(payload["edgeCount"], 1);
    assert!(payload["nodeStorageSampledAtUnixMs"].as_u64().unwrap() > 0);
    assert!(payload["nodeStorageSampleAgeMs"].as_u64().is_some());
    assert_eq!(payload["embeddingState"], "disabled");
    assert_eq!(payload["embeddingPending"], 1);
    assert!(payload["managedEmbeddingBytes"].is_null());
    assert_eq!(state.http_counters.active.load(Ordering::Relaxed), 0);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/db/missing")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(state.http_counters.active.load(Ordering::Relaxed), 0);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["schema_version"], STATUS_SCHEMA_VERSION);
    assert!(payload["collected_at_unix_ms"].as_u64().unwrap() > 0);
    assert_eq!(payload["startup"]["phase"], "ready");
    assert_eq!(payload["startup"]["search_ready_databases"], 1);
    assert_eq!(payload["startup"]["search_building_databases"], 0);
    assert_eq!(payload["server"]["counters_state"], "ready");
    assert_eq!(payload["server"]["requests"], 3);
    assert_eq!(payload["server"]["errors"], 1);
    assert_eq!(payload["server"]["active"], 1);
    assert!(payload["server"]["uptime_seconds"].is_u64());
    assert_eq!(payload["bolt"]["state"], "unknown");
    assert!(payload["bolt"]["active_connections"].is_null());
    assert!(payload["bolt"]["active_sessions"].is_null());
    assert!(payload["bolt"]["active_transactions"].is_null());
    assert!(payload["bolt"]["failures"].is_null());
    assert_eq!(payload["database"]["state"], "ready");
    assert_eq!(payload["database"]["nodes"], 1);
    assert_eq!(payload["database"]["edges"], 1);
    assert_eq!(payload["database"]["mvcc"]["enabled"], true);
    assert_eq!(payload["database"]["mvcc"]["paused"], false);
    assert_eq!(payload["database"]["mvcc"]["head"], 2);
    assert_eq!(payload["database"]["mvcc"]["floor"], 0);
    assert_eq!(payload["database"]["mvcc"]["active_readers"], 0);
    assert!(payload["database"]["mvcc"]["retained_versions"].is_null());
    assert!(payload["database"]["mvcc"]["prune_debt"].is_null());
    assert!(payload["database"]["mvcc"]["suggested_prune_floor"].is_null());
    assert_eq!(payload["embeddings"]["enabled"], false);
    assert_eq!(payload["embeddings"].as_object().unwrap().len(), 1);
    assert_eq!(state.http_counters.active.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn status_reports_unknown_storage_snapshot_when_engine_is_unopened() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let mut state = AppState::default();
    state.auth.security_enabled = false;
    let response = build_router(Arc::new(state))
        .oneshot(
            Request::builder()
                .uri("/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["database"]["state"], "unknown");
    assert_eq!(payload["startup"]["phase"], "ready");
    assert_eq!(payload["startup"]["search_ready_databases"], 0);
    assert_eq!(payload["startup"]["search_building_databases"], 0);
    assert!(payload["database"]["nodes"].is_null());
    assert!(payload["database"]["edges"].is_null());
    assert!(payload["database"]["mvcc"].is_null());
    assert!(payload["embeddings"]["enabled"].is_null());
    assert_eq!(payload["embeddings"]["status"], "unknown");
    assert_eq!(payload["embeddings"].as_object().unwrap().len(), 2);
}

#[tokio::test]
async fn status_remains_responsive_under_concurrent_load() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let mut state = AppState::default();
    state.auth.security_enabled = false;
    open_engine(&state, &state.db_name).unwrap();
    let app = build_router(Arc::new(state));
    let mut requests = tokio::task::JoinSet::new();

    for _ in 0..16 {
        let app = app.clone();
        requests.spawn(async move {
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                app.oneshot(
                    Request::builder()
                        .uri("/status")
                        .body(Body::empty())
                        .unwrap(),
                ),
            )
            .await
        });
    }

    while let Some(result) = requests.join_next().await {
        let response = result.unwrap().unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn database_info_reports_search_unready_without_declared_search_indexes() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("copperdb")
        .to_string_lossy()
        .into_owned();
    let db_manager = Arc::new(DatabaseManager::new());
    db_manager.create("copperdb", storage_path).unwrap();
    let mut state = AppState {
        db_name: "copperdb".into(),
        db_manager,
        ..Default::default()
    };
    state.auth.security_enabled = false;

    let response = build_router(Arc::new(state))
        .oneshot(
            Request::builder()
                .uri("/db/copperdb")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["searchReady"], false);
    assert_eq!(payload["searchBuilding"], false);
    assert_eq!(payload["searchInitialized"], false);
}

// ─── Demo page e2e ─────────────────────────────────────────────────────
//
// Mirrors NornicDB's /demo page lifecycle:
//   create d3_demo → index → seed stars → seed edges → query → persist
//
// The browser Demo.tsx sends these exact Cypher shapes via the HTTP
// /db/{name}/tx/commit endpoint.  This test replays the same protocol
// traffic and then re-opens the storage to assert the data is on disk.

/// Like demo_temp_appstate but uses a catalog-backed DatabaseManager so
/// CREATE DATABASE persists to disk and survives restart.
fn demo_temp_appstate_with_catalog(temp_dir: &tempfile::TempDir) -> Arc<AppState> {
    let data_root = temp_dir.path().to_string_lossy().into_owned();
    let catalog_path = format!("{data_root}/multidb");
    std::fs::create_dir_all(&catalog_path).unwrap();
    let db_manager = Arc::new(DatabaseManager::open(&catalog_path).unwrap());
    let mut state = AppState::default();
    state.db_name = "copperdb".into();
    state.db_manager = db_manager;
    let _ = state
        .db_manager
        .create("copperdb", format!("{data_root}/copperdb"));
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
    let state = demo_temp_appstate_with_catalog(&temp_dir);
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
    let show_resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let database_names: Vec<String> = show_resp["results"][0]["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["row"][0].as_str().unwrap().to_string())
        .collect();
    assert!(
        database_names.contains(&"d3_demo".into()),
        "d3_demo should appear in SHOW DATABASES after create: {show_resp:?}"
    );

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/db/d3_demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "GET /db/d3_demo must succeed so the UI list does not drop it"
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
                    demo_cypher_request(vec![(query_edge_detail, serde_json::json!({}))])
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
    let edge_rows = commit_resp["results"][0]["data"].as_array().unwrap();
    let dists: Vec<i64> = edge_rows
        .iter()
        .map(|d| d["row"][0].as_i64().unwrap())
        .collect();
    assert!(
        dists.contains(&200),
        "missing gateway edge (dist=200): {dists:?}"
    );

    let demo_counts = "\
        MATCH (n:Star) \
        WITH count(n) AS stars \
        MATCH ()-[r:HYPERLANE]->() \
        RETURN stars, count(r) AS hyperlanes";
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/db/d3_demo/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    demo_cypher_request(vec![(demo_counts, serde_json::json!({}))]).to_string(),
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
        "demo count validation query should not error: {commit_resp:?}"
    );
    let count_row = &commit_resp["results"][0]["data"][0]["row"];
    assert_eq!(
        count_row[0], 6,
        "demo count query should see 6 stars: {commit_resp:?}"
    );
    assert!(
        count_row[1].as_i64().unwrap_or(0) > 0,
        "demo count query should see hyperlanes: {commit_resp:?}"
    );

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
    assert_eq!(
        row[0], "Heidr Crown 1-01",
        "star name should match: {commit_resp:?}"
    );
    assert_eq!(row[1], 1, "star sector should be 1: {commit_resp:?}");

    let shortest_path = "\
        MATCH (start:Star {starId: $startId}), (end:Star {starId: $endId}) \
        MATCH p = shortestPath((start)-[:HYPERLANE*]-(end)) \
        RETURN [n IN nodes(p) | n.starId] AS pathIds, length(p) AS hops \
        LIMIT 1";
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/db/d3_demo/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    demo_cypher_request(vec![(
                        shortest_path,
                        serde_json::json!({ "startId": "s0-0", "endId": "s1-2" }),
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
        "shortest path query should not error: {commit_resp:?}"
    );
    let path_rows = commit_resp["results"][0]["data"].as_array().unwrap();
    assert_eq!(
        path_rows.len(),
        1,
        "shortest path should return one row: {commit_resp:?}"
    );
    let path_ids = path_rows[0]["row"][0].as_array().unwrap();
    assert_eq!(path_ids.first().unwrap(), "s0-0");
    assert_eq!(path_ids.last().unwrap(), "s1-2");
    let hops = path_rows[0]["row"][1].as_i64().unwrap_or(0);
    assert!(
        hops > 0,
        "shortest path hops should be positive: {commit_resp:?}"
    );

    // ── 8. Drop app, reopen from same data dir, verify persistence ──
    drop(app);
    drop(state);

    let reopened_manager = DatabaseManager::open(format!("{data_root}/multidb")).unwrap();
    let reopened_state = Arc::new(AppState {
        db_name: "copperdb".into(),
        db_manager: Arc::new(reopened_manager),
        auth: {
            let mut a = AuthState::default();
            a.security_enabled = false;
            a
        },
        ..Default::default()
    });
    let reopened_app = build_router(reopened_state);

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
    let show_resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let database_names: Vec<String> = show_resp["results"][0]["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["row"][0].as_str().unwrap().to_string())
        .collect();
    assert!(
        database_names.contains(&"d3_demo".into()),
        "after reopen: d3_demo should appear in SHOW DATABASES: {show_resp:?}"
    );

    let resp = reopened_app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/db/d3_demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "after reopen: GET /db/d3_demo must succeed"
    );

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

    let resp = reopened_app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/db/d3_demo/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    demo_cypher_request(vec![(
                        shortest_path,
                        serde_json::json!({ "startId": "s0-0", "endId": "s1-2" }),
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
    let path_rows = commit_resp["results"][0]["data"].as_array().unwrap();
    assert_eq!(
        path_rows.len(),
        1,
        "after reopen: shortest path should return one row: {commit_resp:?}"
    );
    let path_ids = path_rows[0]["row"][0].as_array().unwrap();
    assert_eq!(path_ids.first().unwrap(), "s0-0");
    assert_eq!(path_ids.last().unwrap(), "s1-2");
    let hops = path_rows[0]["row"][1].as_i64().unwrap_or(0);
    assert!(
        hops > 0,
        "after reopen: shortest path hops should be positive: {commit_resp:?}"
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

#[tokio::test]
async fn demo_shortest_path_e2e_warms_result_cache() {
    use axum::body::Body;
    use axum::http::{header, Method, Request, StatusCode};
    use copperdb_storage::{EdgeRecord, IndexDefinition, IndexEntityType, IndexKind, NodeRecord};
    use tower::ServiceExt;

    const SECTORS: usize = 20;
    const STARS_PER_SECTOR: usize = 100;
    const EXTRA_LINKS_PER_STAR: usize = 7;
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    state
        .db_manager
        .create(
            "d3_demo",
            temp_dir
                .path()
                .join("d3_demo")
                .to_string_lossy()
                .into_owned(),
        )
        .unwrap();
    let engine = open_engine(&state, "d3_demo").unwrap();
    engine
        .storage()
        .persist_index_definition(&IndexDefinition {
            name: "star_id_idx".into(),
            entity_type: IndexEntityType::Node,
            kind: IndexKind::Range,
            label: "Star".into(),
            properties: vec!["starId".into()],
        })
        .unwrap();

    let mut nodes = Vec::with_capacity(SECTORS * STARS_PER_SECTOR);
    for sector in 0..SECTORS {
        for star in 0..STARS_PER_SECTOR {
            let star_id = format!("s{sector}-{star}");
            nodes.push(NodeRecord {
                id: format!("star:{star_id}"),
                labels: vec!["Star".into()],
                properties: BTreeMap::from([("starId".into(), serde_json::json!(star_id))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            });
        }
    }
    engine.storage().put_node_records_batch(&nodes).unwrap();

    let mut random_state = 0x00fe_edd3_u32;
    let mut next_random = || {
        random_state = random_state
            .wrapping_mul(1_664_525)
            .wrapping_add(1_013_904_223);
        (random_state as usize) % STARS_PER_SECTOR
    };
    let mut seen_links = std::collections::BTreeSet::new();
    let mut edges = Vec::new();
    let mut add_link = |from: String, to: String| {
        if from == to {
            return;
        }
        let key = if from < to {
            format!("{from}|{to}")
        } else {
            format!("{to}|{from}")
        };
        if !seen_links.insert(key) {
            return;
        }
        for (start, end) in [(&from, &to), (&to, &from)] {
            let edge_id = format!("lane:{}", edges.len());
            edges.push(EdgeRecord {
                id: edge_id,
                start_node: format!("star:{start}"),
                end_node: format!("star:{end}"),
                edge_type: "HYPERLANE".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            });
        }
    };
    for sector in 0..SECTORS {
        for star in 1..STARS_PER_SECTOR {
            add_link(
                format!("s{sector}-{}", star - 1),
                format!("s{sector}-{star}"),
            );
        }
        for star in 0..STARS_PER_SECTOR {
            for _ in 0..EXTRA_LINKS_PER_STAR {
                add_link(
                    format!("s{sector}-{star}"),
                    format!("s{sector}-{}", next_random()),
                );
            }
        }
    }
    for sector in 0..(SECTORS - 1) {
        for _ in 0..2 {
            add_link(
                format!("s{sector}-{}", next_random()),
                format!("s{}-{}", sector + 1, next_random()),
            );
        }
    }
    engine.storage().put_edge_records_batch(&edges).unwrap();

    let app = build_router(state);
    let query = "MATCH (start:Star {starId: $startId}), (end:Star {starId: $endId}) MATCH p = shortestPath((start)-[:HYPERLANE*]-(end)) RETURN [n IN nodes(p) | n.starId] AS pathIds, length(p) AS hops LIMIT 1";
    let request = || {
        Request::builder()
            .method(Method::POST)
            .uri("/db/d3_demo/tx/commit")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                demo_cypher_request(vec![(
                    query,
                    serde_json::json!({"startId": "s0-16", "endId": "s19-79"}),
                )])
                .to_string(),
            ))
            .unwrap()
    };

    let cold_started = Instant::now();
    let cold = app.clone().oneshot(request()).await.unwrap();
    let cold_elapsed = cold_started.elapsed();
    assert_eq!(cold.status(), StatusCode::OK);
    let cold_body = axum::body::to_bytes(cold.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let cold_payload: serde_json::Value = serde_json::from_slice(&cold_body).unwrap();
    assert!(
        cold_payload["errors"].as_array().unwrap().is_empty(),
        "{cold_payload:?}"
    );
    assert_eq!(
        cold_payload["results"][0]["data"].as_array().unwrap().len(),
        1
    );
    let started = Instant::now();
    let response = app.oneshot(request()).await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        payload["errors"].as_array().unwrap().is_empty(),
        "{payload:?}"
    );
    assert_eq!(payload["results"][0]["data"].as_array().unwrap().len(), 1);
    assert_eq!(engine.cypher_result_cache_stats().hits, 1);
    eprintln!("d3_demo HTTP shortestPath: cold={cold_elapsed:?} warm_result_cache_hit={elapsed:?}");
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
        initial_dbs.contains(&"copperdb".into()),
        "copperdb db should be present: {initial_dbs:?}"
    );
    assert!(
        !initial_dbs.contains(&"default".into()),
        "legacy default db should not be present: {initial_dbs:?}"
    );

    // Posting to a missing database must not implicitly create it. Neo4j and
    // NornicDB require explicit CREATE DATABASE through the system database.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/db/implicit_missing/tx/commit")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    demo_cypher_request(vec![(
                        "CREATE (n:ShouldNotExist {value: 1}) RETURN n.value AS val",
                        serde_json::json!({}),
                    )])
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
    assert!(
        result["errors"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error["message"].as_str() == Some("database not found: implicit_missing")),
        "missing database write should fail without auto-create: {result:?}"
    );

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
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let dbs_after_missing_write: Vec<String> = result["results"][0]["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["row"][0].as_str().unwrap().to_string())
        .collect();
    assert!(
        !dbs_after_missing_write.contains(&"implicit_missing".into()),
        "missing database write must not register a catalog entry: {dbs_after_missing_write:?}"
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

    let reopened_manager = DatabaseManager::open(format!("{data_root}/multidb")).unwrap();
    let reopened_state = Arc::new(AppState {
        db_name: "copperdb".into(),
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

#[test]
fn appstate_bolt_executor_routes_system_and_named_database_queries() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();

    executor
        .execute_on_database(Some("system"), "CREATE DATABASE d3_demo", &empty)
        .expect("Bolt executor should create databases through system");

    let show = executor
        .execute_on_database(Some("system"), "SHOW DATABASES", &empty)
        .expect("Bolt executor should query system database");
    assert!(
        show.rows
            .iter()
            .any(|row| row.first() == Some(&serde_json::json!("d3_demo"))),
        "d3_demo should appear in SHOW DATABASES over Bolt executor: {show:?}"
    );

    let write = executor
        .execute_on_database(
            Some("d3_demo"),
            "CREATE (n:Star {starId: 's0-16'}) RETURN n.starId AS id",
            &empty,
        )
        .expect("Bolt executor should write to selected database");
    assert_eq!(write.columns, vec!["id"]);
    assert_eq!(write.rows[0][0], serde_json::json!("s0-16"));
    assert_eq!(write.stats.nodes_created, 1);
    assert!(write.stats.properties_set >= 1);

    let count = executor
        .execute_on_database(
            Some("d3_demo"),
            "MATCH (n:Star {starId: 's0-16'}) RETURN count(n) AS cnt",
            &empty,
        )
        .expect("Bolt executor should read from selected database");
    assert_eq!(count.columns, vec!["cnt"]);
    assert_eq!(count.rows[0][0], serde_json::json!(1));

    let missing = executor
        .execute_on_database(Some("missing_db"), "RETURN 1 AS n", &empty)
        .expect_err("Bolt executor must not auto-create missing databases");
    assert_eq!(missing, "database not found: missing_db");
}

#[test]
fn appstate_bolt_executor_records_fulltext_procedure_metrics() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(Arc::clone(&state));
    let empty = HashMap::new();

    executor
        .execute_on_database(
            Some("copperdb"),
            "CREATE (:Doc {content: 'CloudTrail audit logging'})",
            &empty,
        )
        .unwrap();
    executor
        .execute_on_database(
            Some("copperdb"),
            "CREATE FULLTEXT INDEX doc_content_ft FOR (n:Doc) ON (n.content)",
            &empty,
        )
        .unwrap();
    let result = executor
        .execute_on_database(
            Some("copperdb"),
            "CALL db.index.fulltext.queryNodes('doc_content_ft', 'cloudtrail')",
            &empty,
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);

    assert_eq!(
        state
            .telemetry
            .snapshot_metric("nornicdb_search_requests_total")
            .unwrap(),
        vec![copperdb_otel::MetricSample {
            labels: vec![
                ("mode".to_string(), "bm25".to_string()),
                ("result".to_string(), "success".to_string()),
            ],
            value: copperdb_otel::MetricValue::Counter(1.0),
        }]
    );
    assert_eq!(
        state
            .telemetry
            .snapshot_metric("nornicdb_search_candidates_rows")
            .unwrap(),
        vec![copperdb_otel::MetricSample {
            labels: Vec::new(),
            value: copperdb_otel::MetricValue::Gauge(1.0),
        }]
    );
    let duration = state
        .telemetry
        .snapshot_metric("nornicdb_search_duration_seconds")
        .unwrap();
    assert!(matches!(
        duration.as_slice(),
        [copperdb_otel::MetricSample {
            labels,
            value: copperdb_otel::MetricValue::Histogram { count, .. },
        }] if labels == &vec![
            ("mode".to_string(), "bm25".to_string()),
            ("stage".to_string(), "index".to_string()),
        ] && *count == 1
    ));
}

#[test]
fn appstate_bolt_executor_retains_storage_context_until_commit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let transaction = executor
        .begin_transaction("copperdb", &HashMap::new(), None)
        .unwrap();
    let transaction_id = uuid::Uuid::parse_str(&transaction.id).unwrap();

    assert!(executor
        .storage_transactions
        .lock()
        .contains_key(&transaction_id));
    executor.commit_transaction(&transaction).unwrap();
    assert!(executor.storage_transactions.lock().is_empty());
}

#[test]
fn appstate_bolt_executor_discards_storage_context_on_rollback() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let transaction = executor
        .begin_transaction("copperdb", &HashMap::new(), None)
        .unwrap();

    executor.rollback_transaction(&transaction).unwrap();
    assert!(executor.storage_transactions.lock().is_empty());
}

#[test]
fn appstate_bolt_executor_reports_and_counts_edge_snapshot_conflicts() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(Arc::clone(&state));
    let empty = HashMap::new();
    executor
        .execute_on_database(
            Some("copperdb"),
            "CREATE (:TxEdgeConflict {id: 'a'})-[:LINKS {value: 0}]->(:TxEdgeConflict {id: 'b'})",
            &empty,
        )
        .expect("seed relationship should be created");

    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .expect("BEGIN should create a transaction context");
    executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (a:TxEdgeConflict {id: 'a'}), (b:TxEdgeConflict {id: 'b'}) MERGE (a)-[r:LINKS]->(b) ON MATCH SET r.value = 1",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transactional relationship MERGE should stage an edge update");
    executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (:TxEdgeConflict {id: 'a'})-[r:LINKS]->(:TxEdgeConflict {id: 'b'}) SET r.value = 2",
            &empty,
        )
        .expect("external relationship update should win the race");

    let error = executor
        .commit_transaction(&transaction)
        .expect_err("stale relationship update must fail at commit");
    assert!(error.to_string().contains("transaction conflict on edge:"));
    assert!(
        executor.storage_transactions.lock().is_empty(),
        "a failed commit must discard its storage transaction context"
    );
    assert_eq!(
        state
            .telemetry
            .snapshot_metric("nornicdb_cypher_transaction_conflicts_total")
            .unwrap(),
        vec![copperdb_otel::MetricSample {
            labels: Vec::new(),
            value: copperdb_otel::MetricValue::Counter(1.0),
        }]
    );
}

#[tokio::test]
async fn bolt_tcp_reports_outdated_for_a_live_edge_snapshot_conflict() {
    use copperdb_bolt::packstream::Value;
    use copperdb_bolt::server::{BoltServer, QueryExecutor};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = Arc::new(AppStateBoltExecutor::new(Arc::clone(&state)));
    let empty = HashMap::new();
    executor
        .execute_on_database(
            Some("copperdb"),
            "CREATE (:BoltEdgeConflict {id: 'a'})-[:LINKS {value: 0}]->(:BoltEdgeConflict {id: 'b'})",
            &empty,
        )
        .expect("seed relationship should be created");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = BoltServer::new(
        address.to_string(),
        Arc::clone(&state.telemetry),
        Arc::clone(&executor) as Arc<dyn QueryExecutor>,
    )
    .with_auth_enabled(false);
    let server_task = tokio::spawn(async move { server.serve_listener(listener).await });

    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
    client
        .write_all(&[
            0x60, 0x60, 0xB0, 0x17, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x04, 0x03, 0x00, 0x00,
            0x04, 0x02, 0x00, 0x00, 0x04, 0x01,
        ])
        .await
        .unwrap();
    let mut version = [0u8; 4];
    client.read_exact(&mut version).await.unwrap();
    assert_eq!(version, [0x00, 0x00, 0x04, 0x04]);

    for (signature, fields) in [
        (
            0x01,
            vec![Value::Map(vec![ (
                "user_agent".into(),
                Value::String("copperdb-edge-conflict-test".into()),
            )])],
        ),
        (0x11, vec![Value::Map(vec![])]),
        (
            0x10,
            vec![
                Value::String(
                    "MATCH (a:BoltEdgeConflict {id: 'a'}), (b:BoltEdgeConflict {id: 'b'}) MERGE (a)-[r:LINKS]->(b) ON MATCH SET r.value = 1".into(),
                ),
                Value::Map(vec![]),
                Value::Map(vec![]),
            ],
        ),
    ] {
        client
            .write_all(&encode_bolt_chunks(&encode_bolt_message(signature, &fields)))
            .await
            .unwrap();
        let Value::Struct { signature, .. } = read_bolt_message(&mut client).await else {
            panic!("expected Bolt SUCCESS response");
        };
        assert_eq!(signature, 0x70);
    }

    executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (:BoltEdgeConflict {id: 'a'})-[r:LINKS]->(:BoltEdgeConflict {id: 'b'}) SET r.value = 2",
            &empty,
        )
        .expect("external relationship update should win the race");

    client
        .write_all(&encode_bolt_chunks(&encode_bolt_message(0x12, &[])))
        .await
        .unwrap();
    let Value::Struct { signature, fields } = read_bolt_message(&mut client).await else {
        panic!("expected Bolt FAILURE response");
    };
    assert_eq!(signature, 0x7F);
    let [Value::Map(metadata)] = fields.as_slice() else {
        panic!("expected Bolt FAILURE metadata");
    };
    assert_eq!(
        metadata
            .iter()
            .find_map(|(key, value)| { (key == "code").then_some(value) }),
        Some(&Value::String(
            "Neo.TransientError.Transaction.Outdated".into()
        ))
    );

    server_task.abort();
}

#[test]
fn appstate_bolt_executor_validates_implicit_run_bookmarks() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .unwrap();
    let bookmark = executor.commit_transaction(&transaction).unwrap();

    let result = executor
        .execute_as_on_database_with_context_and_bookmarks(
            "copperdb",
            "RETURN 1 AS value",
            &empty,
            RequestContext::detached(),
            None,
            &[bookmark],
        )
        .expect("a valid implicit RUN bookmark should be accepted");
    assert_eq!(result.rows, vec![vec![serde_json::json!(1)]]);

    let invalid = executor
        .execute_as_on_database_with_context_and_bookmarks(
            "copperdb",
            "RETURN 1 AS value",
            &empty,
            RequestContext::detached(),
            None,
            &["not-a-bookmark".into()],
        )
        .expect_err("an invalid implicit RUN bookmark must fail before execution");
    assert!(invalid.to_string().contains("invalid bookmark"));
}

#[test]
fn appstate_bolt_executor_preserves_explicit_transaction_cancellation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .unwrap();
    let request_context = RequestContext::detached();
    request_context.cancel();

    let error = executor
        .execute_in_transaction_with_context(
            &transaction,
            "RETURN 1 AS value",
            &empty,
            request_context,
            None,
        )
        .unwrap_err();

    assert!(matches!(error, BoltExecutionError::RequestCancelled(_)));
    assert_eq!(error.to_string(), "request cancelled");
    executor.rollback_transaction(&transaction).unwrap();
}

#[test]
fn appstate_bolt_executor_keeps_run_writes_private_until_commit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .expect("BEGIN should create a transaction context");

    let created = executor
        .execute_in_transaction_with_context(
            &transaction,
            "CREATE (n:TxProbe {value: 7}) RETURN n.value AS value",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transactional CREATE should use the private overlay");
    assert_eq!(created.columns, vec!["value"]);
    assert_eq!(created.rows, vec![vec![serde_json::json!(7)]]);

    let in_transaction = executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (n:TxProbe {value: 7}) RETURN count(n) AS count",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transaction should read its staged write");
    assert_eq!(in_transaction.rows, vec![vec![serde_json::json!(1)]]);

    let outside = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (n:TxProbe {value: 7}) RETURN count(n) AS count",
            &empty,
        )
        .expect("outside query should execute normally");
    assert_eq!(outside.rows, vec![vec![serde_json::json!(0)]]);

    executor.commit_transaction(&transaction).unwrap();
    let committed = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (n:TxProbe {value: 7}) RETURN count(n) AS count",
            &empty,
        )
        .expect("committed write should become visible");
    assert_eq!(committed.rows, vec![vec![serde_json::json!(1)]]);

    let rollback = executor
        .begin_transaction("copperdb", &empty, None)
        .expect("BEGIN should create a transaction context");
    executor
        .execute_in_transaction_with_context(
            &rollback,
            "CREATE (n:TxProbe {value: 8}) RETURN n.value AS value",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transactional CREATE should stage a rollback candidate");
    executor.rollback_transaction(&rollback).unwrap();
    let rolled_back = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (n:TxProbe {value: 8}) RETURN count(n) AS count",
            &empty,
        )
        .expect("outside query should execute normally");
    assert_eq!(rolled_back.rows, vec![vec![serde_json::json!(0)]]);
}

#[test]
fn appstate_bolt_executor_reads_staged_relationships_before_commit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .expect("BEGIN should create a transaction context");

    executor
        .execute_in_transaction_with_context(
            &transaction,
            "CREATE (:TxStart {value: 1})-[:LINKS {distance: 3}]->(:TxEnd {value: 2})",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transactional relationship CREATE should use the private overlay");

    let in_transaction = executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (a:TxStart)-[r:LINKS]->(b:TxEnd) RETURN a.value AS start, r.distance AS distance, b.value AS end",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transaction should traverse its staged relationship");
    assert_eq!(
        in_transaction.rows,
        vec![vec![
            serde_json::json!(1),
            serde_json::json!(3),
            serde_json::json!(2),
        ]]
    );

    let outside = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (:TxStart)-[:LINKS]->(:TxEnd) RETURN count(*) AS count",
            &empty,
        )
        .expect("outside query should execute normally");
    assert_eq!(outside.rows, vec![vec![serde_json::json!(0)]]);

    executor.commit_transaction(&transaction).unwrap();
    let committed = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (:TxStart)-[:LINKS]->(:TxEnd) RETURN count(*) AS count",
            &empty,
        )
        .expect("committed relationship should become visible");
    assert_eq!(committed.rows, vec![vec![serde_json::json!(1)]]);
}

#[test]
fn appstate_bolt_executor_traverses_staged_fixed_length_chains() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .expect("BEGIN should create a transaction context");

    executor
        .execute_in_transaction_with_context(
            &transaction,
            "CREATE (:TxChain {id: 'a'})-[:NEXT]->(:TxChain {id: 'b'})-[:NEXT]->(:TxChain {id: 'c'})",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transactional CREATE should stage a fixed-length chain");
    let inside = executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (a:TxChain {id: 'a'})-[:NEXT]->(b:TxChain {id: 'b'})-[:NEXT]->(c:TxChain {id: 'c'}) RETURN c.id AS id",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transaction should traverse its staged fixed-length chain");
    assert_eq!(inside.rows, vec![vec![serde_json::json!("c")]]);
    let outside = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (:TxChain {id: 'a'})-[:NEXT]->(:TxChain {id: 'b'})-[:NEXT]->(:TxChain {id: 'c'}) RETURN count(*) AS count",
            &empty,
        )
        .expect("outside query should execute normally");
    assert_eq!(outside.rows, vec![vec![serde_json::json!(0)]]);
}

#[test]
fn appstate_bolt_executor_reads_schema_catalogs_inside_explicit_transactions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    executor
        .execute_on_database(
            Some("copperdb"),
            "CREATE INDEX tx_catalog_idx FOR (n:TxCatalog) ON (n.id)",
            &empty,
        )
        .unwrap();
    executor
        .execute_on_database(
            Some("copperdb"),
            "CREATE CONSTRAINT tx_catalog_constraint FOR (n:TxCatalog) REQUIRE n.id IS UNIQUE",
            &empty,
        )
        .unwrap();

    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .unwrap();
    let indexes = executor
        .execute_in_transaction_with_context(
            &transaction,
            "SHOW INDEXES",
            &empty,
            RequestContext::detached(),
            None,
        )
        .unwrap();
    assert!(indexes
        .rows
        .iter()
        .any(|row| row[0] == serde_json::json!("tx_catalog_idx")));
    let constraints = executor
        .execute_in_transaction_with_context(
            &transaction,
            "SHOW CONSTRAINTS",
            &empty,
            RequestContext::detached(),
            None,
        )
        .unwrap();
    assert!(constraints
        .rows
        .iter()
        .any(|row| row[0] == serde_json::json!("tx_catalog_constraint")));
    executor.rollback_transaction(&transaction).unwrap();
}

#[test]
fn appstate_bolt_executor_schema_catalog_reads_are_pinned_at_begin() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .unwrap();

    executor
        .execute_on_database(
            Some("copperdb"),
            "CREATE INDEX after_begin_idx FOR (n:AfterBegin) ON (n.id)",
            &empty,
        )
        .unwrap();

    let indexes = executor
        .execute_in_transaction_with_context(
            &transaction,
            "SHOW INDEXES",
            &empty,
            RequestContext::detached(),
            None,
        )
        .unwrap();
    assert!(!indexes
        .rows
        .iter()
        .any(|row| row[0] == serde_json::json!("after_begin_idx")));
    executor.rollback_transaction(&transaction).unwrap();
}

#[test]
fn appstate_bolt_executor_stages_constraint_ddl_until_commit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .unwrap();

    executor
        .execute_in_transaction_with_context(
            &transaction,
            "CREATE CONSTRAINT tx_staged_constraint FOR (n:TxStaged) REQUIRE n.id IS UNIQUE",
            &empty,
            RequestContext::detached(),
            None,
        )
        .unwrap();
    let inside = executor
        .execute_in_transaction_with_context(
            &transaction,
            "SHOW CONSTRAINTS",
            &empty,
            RequestContext::detached(),
            None,
        )
        .unwrap();
    assert!(inside
        .rows
        .iter()
        .any(|row| row[0] == serde_json::json!("tx_staged_constraint")));
    let outside = executor
        .execute_on_database(Some("copperdb"), "SHOW CONSTRAINTS", &empty)
        .unwrap();
    assert!(!outside
        .rows
        .iter()
        .any(|row| row[0] == serde_json::json!("tx_staged_constraint")));

    executor.commit_transaction(&transaction).unwrap();
    let committed = executor
        .execute_on_database(Some("copperdb"), "SHOW CONSTRAINTS", &empty)
        .unwrap();
    assert!(committed
        .rows
        .iter()
        .any(|row| row[0] == serde_json::json!("tx_staged_constraint")));
}

#[test]
fn appstate_bolt_executor_rolls_back_staged_constraint_ddl() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .unwrap();

    executor
        .execute_in_transaction_with_context(
            &transaction,
            "CREATE CONSTRAINT tx_rolled_back_constraint FOR (n:TxRolledBack) REQUIRE n.id IS UNIQUE",
            &empty,
            RequestContext::detached(),
            None,
        )
        .unwrap();
    executor.rollback_transaction(&transaction).unwrap();

    let outside = executor
        .execute_on_database(Some("copperdb"), "SHOW CONSTRAINTS", &empty)
        .unwrap();
    assert!(!outside
        .rows
        .iter()
        .any(|row| row[0] == serde_json::json!("tx_rolled_back_constraint")));
}

#[test]
fn appstate_bolt_executor_stages_index_ddl_until_commit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .unwrap();

    executor
        .execute_in_transaction_with_context(
            &transaction,
            "CREATE INDEX tx_staged_index FOR (n:TxStaged) ON (n.id)",
            &empty,
            RequestContext::detached(),
            None,
        )
        .unwrap();
    let inside = executor
        .execute_in_transaction_with_context(
            &transaction,
            "SHOW INDEXES",
            &empty,
            RequestContext::detached(),
            None,
        )
        .unwrap();
    assert!(inside
        .rows
        .iter()
        .any(|row| row[0] == serde_json::json!("tx_staged_index")));
    let outside = executor
        .execute_on_database(Some("copperdb"), "SHOW INDEXES", &empty)
        .unwrap();
    assert!(!outside
        .rows
        .iter()
        .any(|row| row[0] == serde_json::json!("tx_staged_index")));

    executor.commit_transaction(&transaction).unwrap();
    let committed = executor
        .execute_on_database(Some("copperdb"), "SHOW INDEXES", &empty)
        .unwrap();
    assert!(committed
        .rows
        .iter()
        .any(|row| row[0] == serde_json::json!("tx_staged_index")));
}

#[test]
fn appstate_bolt_executor_rolls_back_staged_index_ddl() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .unwrap();

    executor
        .execute_in_transaction_with_context(
            &transaction,
            "CREATE INDEX tx_rolled_back_index FOR (n:TxRolledBack) ON (n.id)",
            &empty,
            RequestContext::detached(),
            None,
        )
        .unwrap();
    executor.rollback_transaction(&transaction).unwrap();

    let outside = executor
        .execute_on_database(Some("copperdb"), "SHOW INDEXES", &empty)
        .unwrap();
    assert!(!outside
        .rows
        .iter()
        .any(|row| row[0] == serde_json::json!("tx_rolled_back_index")));
}

#[test]
fn appstate_bolt_executor_stages_knowledge_policy_ddl_until_commit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .unwrap();

    executor
        .execute_in_transaction_with_context(
            &transaction,
            "CREATE DECAY PROFILE tx_slow_decay OPTIONS { halfLifeSeconds: 60, visibilityThreshold: 0.1, scoreFloor: 0.0, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
            &empty,
            RequestContext::detached(),
            None,
        )
        .unwrap();
    let inside = executor
        .execute_in_transaction_with_context(
            &transaction,
            "SHOW DECAY PROFILES",
            &empty,
            RequestContext::detached(),
            None,
        )
        .unwrap();
    assert!(inside
        .rows
        .iter()
        .any(|row| row[1] == serde_json::json!("tx_slow_decay")));
    let outside = executor
        .execute_on_database(Some("copperdb"), "SHOW DECAY PROFILES", &empty)
        .unwrap();
    assert!(!outside
        .rows
        .iter()
        .any(|row| row[1] == serde_json::json!("tx_slow_decay")));

    executor.commit_transaction(&transaction).unwrap();
    let committed = executor
        .execute_on_database(Some("copperdb"), "SHOW DECAY PROFILES", &empty)
        .unwrap();
    assert!(committed
        .rows
        .iter()
        .any(|row| row[1] == serde_json::json!("tx_slow_decay")));
}

#[test]
fn appstate_bolt_executor_rolls_back_staged_knowledge_policy_ddl() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .unwrap();

    executor
        .execute_in_transaction_with_context(
            &transaction,
            "CREATE DECAY PROFILE tx_rolled_back_decay OPTIONS { halfLifeSeconds: 60, visibilityThreshold: 0.1, scoreFloor: 0.0, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
            &empty,
            RequestContext::detached(),
            None,
        )
        .unwrap();
    executor.rollback_transaction(&transaction).unwrap();

    let outside = executor
        .execute_on_database(Some("copperdb"), "SHOW DECAY PROFILES", &empty)
        .unwrap();
    assert!(!outside
        .rows
        .iter()
        .any(|row| row[1] == serde_json::json!("tx_rolled_back_decay")));
}

#[test]
fn appstate_bolt_executor_traverses_staged_variable_length_chains() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .expect("BEGIN should create a transaction context");

    executor
        .execute_in_transaction_with_context(
            &transaction,
            "CREATE (:TxVariableChain {id: 'a'})-[:NEXT]->(:TxVariableChain {id: 'b'})-[:NEXT]->(:TxVariableChain {id: 'c'})-[:NEXT]->(:TxVariableChain {id: 'd'})",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transactional CREATE should stage a variable-length chain");
    let inside = executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (:TxVariableChain {id: 'a'})-[r:NEXT*1..3]->(end:TxVariableChain {id: 'd'}) RETURN size(r) AS hops, end.id AS id",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transaction should traverse its staged variable-length chain");
    assert_eq!(
        inside.rows,
        vec![vec![serde_json::json!(3), serde_json::json!("d")]]
    );
    let outside = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (:TxVariableChain {id: 'a'})-[:NEXT*1..3]->(:TxVariableChain {id: 'd'}) RETURN count(*) AS count",
            &empty,
        )
        .expect("outside query should execute normally");
    assert_eq!(outside.rows, vec![vec![serde_json::json!(0)]]);
}

#[test]
fn appstate_bolt_executor_traverses_staged_mixed_length_chains() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .expect("BEGIN should create a transaction context");

    executor
        .execute_in_transaction_with_context(
            &transaction,
            "CREATE (:TxMixedChain {id: 'a'})-[:FIRST]->(:TxMixedChain {id: 'b'})-[:NEXT]->(:TxMixedChain {id: 'c'})-[:NEXT]->(:TxMixedChain {id: 'd'})",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transactional CREATE should stage a mixed-length chain");
    let inside = executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (:TxMixedChain {id: 'a'})-[:FIRST]->(mid:TxMixedChain)-[r:NEXT*1..2]->(end:TxMixedChain {id: 'd'}) RETURN mid.id AS mid, size(r) AS hops, end.id AS end",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transaction should traverse fixed and variable staged edges together");
    assert_eq!(
        inside.rows,
        vec![vec![
            serde_json::json!("b"),
            serde_json::json!(2),
            serde_json::json!("d"),
        ]]
    );
    let outside = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (:TxMixedChain {id: 'a'})-[:FIRST]->(:TxMixedChain)-[:NEXT*1..2]->(:TxMixedChain {id: 'd'}) RETURN count(*) AS count",
            &empty,
        )
        .expect("outside query should execute normally");
    assert_eq!(outside.rows, vec![vec![serde_json::json!(0)]]);
}

#[test]
fn appstate_bolt_executor_matches_staged_disconnected_patterns() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .expect("BEGIN should create a transaction context");

    executor
        .execute_in_transaction_with_context(
            &transaction,
            "CREATE (:TxDisconnected {id: 'a'})-[:LEFT]->(:TxDisconnected {id: 'b'}), (:TxDisconnected {id: 'c'})-[:RIGHT]->(:TxDisconnected {id: 'd'})",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transactional CREATE should stage disconnected paths");
    let inside = executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (a:TxDisconnected {id: 'a'})-[:LEFT]->(b:TxDisconnected {id: 'b'}), (c:TxDisconnected {id: 'c'})-[:RIGHT]->(d:TxDisconnected {id: 'd'}) RETURN a.id AS a, b.id AS b, c.id AS c, d.id AS d",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transaction should match all staged disconnected segments");
    assert_eq!(
        inside.rows,
        vec![vec![
            serde_json::json!("a"),
            serde_json::json!("b"),
            serde_json::json!("c"),
            serde_json::json!("d"),
        ]]
    );
    let outside = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (:TxDisconnected)-[:LEFT]->(), (:TxDisconnected)-[:RIGHT]->() RETURN count(*) AS count",
            &empty,
        )
        .expect("outside query should execute normally");
    assert_eq!(outside.rows, vec![vec![serde_json::json!(0)]]);
}

#[test]
fn appstate_bolt_executor_optionally_matches_staged_relationships() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .expect("BEGIN should create a transaction context");

    executor
        .execute_in_transaction_with_context(
            &transaction,
            "CREATE (:TxOptional {id: 'a'})",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transactional CREATE should stage the optional-match source node");
    let missing = executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (a:TxOptional {id: 'a'}) OPTIONAL MATCH (a)-[:KNOWS]->(b:TxOptional) RETURN a.id AS a, b.id AS b",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("optional match should preserve the staged source row");
    assert_eq!(
        missing.rows,
        vec![vec![serde_json::json!("a"), serde_json::Value::Null]]
    );

    executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (a:TxOptional {id: 'a'}) CREATE (a)-[:KNOWS]->(:TxOptional {id: 'b'})",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transactional CREATE should stage the optional relationship");
    let matched = executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (a:TxOptional {id: 'a'}) OPTIONAL MATCH (a)-[:KNOWS]->(b:TxOptional) RETURN a.id AS a, b.id AS b",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("optional match should find the staged relationship");
    assert_eq!(
        matched.rows,
        vec![vec![serde_json::json!("a"), serde_json::json!("b")]]
    );
}

#[test]
fn appstate_bolt_executor_filters_staged_matches_with_where() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .expect("BEGIN should create a transaction context");

    executor
        .execute_in_transaction_with_context(
            &transaction,
            "CREATE (:TxWhere {id: 'a', score: 1}), (:TxWhere {id: 'b', score: 2})",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transactional CREATE should stage filter candidates");
    let inside = executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (n:TxWhere) WHERE n.score > 1 RETURN n.id AS id",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("WHERE should filter staged transaction rows");
    assert_eq!(inside.rows, vec![vec![serde_json::json!("b")]]);
    let outside = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (:TxWhere) RETURN count(*) AS count",
            &empty,
        )
        .expect("outside query should execute normally");
    assert_eq!(outside.rows, vec![vec![serde_json::json!(0)]]);
}

#[test]
fn appstate_bolt_executor_pipelines_staged_rows_through_with() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .expect("BEGIN should create a transaction context");

    executor
        .execute_in_transaction_with_context(
            &transaction,
            "CREATE (:TxWith {id: 'a', score: 1})-[:NEXT]->(:TxWith {id: 'b', score: 2})",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transactional CREATE should stage WITH pipeline data");
    let inside = executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (a:TxWith {id: 'a'})-[:NEXT]->(b:TxWith) WITH a, b WHERE b.score > 1 MATCH (a)-[:NEXT]->(b) RETURN a.id AS a, b.id AS b",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("WITH should retain staged bindings for later transaction-local matching");
    assert_eq!(
        inside.rows,
        vec![vec![serde_json::json!("a"), serde_json::json!("b")]]
    );
    let outside = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (:TxWith) RETURN count(*) AS count",
            &empty,
        )
        .expect("outside query should execute normally");
    assert_eq!(outside.rows, vec![vec![serde_json::json!(0)]]);
}

#[test]
fn appstate_bolt_executor_unwinds_staged_transaction_rows() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .expect("BEGIN should create a transaction context");

    let created = executor
        .execute_in_transaction_with_context(
            &transaction,
            "UNWIND ['a', 'b'] AS id CREATE (:TxUnwind {id: id}) RETURN id",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("UNWIND should create staged nodes for every list item");
    assert_eq!(
        created.rows,
        vec![vec![serde_json::json!("a")], vec![serde_json::json!("b")]]
    );
    assert_eq!(created.stats.nodes_created, 2);
    assert_eq!(created.stats.properties_set, 2);
    let inside = executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (n:TxUnwind) RETURN count(*) AS count",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transaction should read nodes created through UNWIND");
    assert_eq!(inside.rows, vec![vec![serde_json::json!(2)]]);
    let outside = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (:TxUnwind) RETURN count(*) AS count",
            &empty,
        )
        .expect("outside query should execute normally");
    assert_eq!(outside.rows, vec![vec![serde_json::json!(0)]]);
}

#[test]
fn appstate_bolt_executor_unwind_match_with_mutations_aggregate_counters() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    executor
        .execute_on_database(
            Some("copperdb"),
            "CREATE (:TxWithMutation {uid: 'a', legacy: true}), (:TxWithMutation {uid: 'b', legacy: true})",
            &empty,
        )
        .unwrap();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .unwrap();

    let marked = executor
        .execute_in_transaction_with_context(
            &transaction,
            "UNWIND ['a', 'b'] AS uid MATCH (node:TxWithMutation {uid: uid}) WITH node SET node.marked = true",
            &empty,
            RequestContext::detached(),
            None,
        )
        .unwrap();
    assert_eq!(marked.stats.properties_set, 2);
    let marked_count = executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (node:TxWithMutation {marked: true}) RETURN count(node) AS count",
            &empty,
            RequestContext::detached(),
            None,
        )
        .unwrap();
    assert_eq!(marked_count.rows, vec![vec![serde_json::json!(2)]]);

    let removed = executor
        .execute_in_transaction_with_context(
            &transaction,
            "UNWIND ['a', 'b'] AS uid MATCH (node:TxWithMutation {uid: uid}) WITH node REMOVE node.legacy",
            &empty,
            RequestContext::detached(),
            None,
        )
        .unwrap();
    assert_eq!(removed.stats.properties_set, 2);
    let legacy_count = executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (node:TxWithMutation {legacy: true}) RETURN count(node) AS count",
            &empty,
            RequestContext::detached(),
            None,
        )
        .unwrap();
    assert_eq!(legacy_count.rows, vec![vec![serde_json::json!(0)]]);

    let deleted = executor
        .execute_in_transaction_with_context(
            &transaction,
            "UNWIND ['a', 'b'] AS uid MATCH (node:TxWithMutation {uid: uid}) WITH node DETACH DELETE node",
            &empty,
            RequestContext::detached(),
            None,
        )
        .unwrap();
    assert_eq!(deleted.stats.nodes_deleted, 2);
    executor.commit_transaction(&transaction).unwrap();

    let remaining = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (:TxWithMutation) RETURN count(*) AS count",
            &empty,
        )
        .unwrap();
    assert_eq!(remaining.rows, vec![vec![serde_json::json!(0)]]);
}

#[test]
fn appstate_bolt_executor_unwind_relationship_delete_commits_or_rolls_back() {
    for (commit, expected_remaining) in [(true, 0), (false, 1)] {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = demo_temp_appstate_with_catalog(&temp_dir);
        let executor = AppStateBoltExecutor::new(state);
        let empty = HashMap::new();
        executor
            .execute_on_database(
                Some("copperdb"),
                "CREATE (:Function {uid: 'source'}), (:Function {uid: 'target'})",
                &empty,
            )
            .unwrap();
        executor
            .execute_on_database(
                Some("copperdb"),
                "MATCH (source:Function {uid: 'source'}), (target:Function {uid: 'target'}) CREATE (source)-[:TAINT_FLOWS_TO {evidence_source: 'wanted'}]->(target)",
                &empty,
            )
            .unwrap();

        let transaction = executor
            .begin_transaction("copperdb", &empty, None)
            .unwrap();
        let params = HashMap::from([
            (
                "uids".to_string(),
                serde_json::json!(["source", "missing", "source"]),
            ),
            ("evidence_source".to_string(), serde_json::json!("wanted")),
        ]);
        let deleted = executor
            .execute_in_transaction_with_context(
                &transaction,
                "UNWIND $uids AS source_uid MATCH (source:Function {uid: source_uid})-[rel:TAINT_FLOWS_TO]->() WHERE rel.evidence_source = $evidence_source DELETE rel",
                &params,
                RequestContext::detached(),
                None,
            )
            .unwrap();
        assert_eq!(deleted.stats.relationships_deleted, 1);

        if commit {
            executor.commit_transaction(&transaction).unwrap();
        } else {
            executor.rollback_transaction(&transaction).unwrap();
        }

        let remaining = executor
            .execute_on_database(
                Some("copperdb"),
                "MATCH (:Function {uid: 'source'})-[rel:TAINT_FLOWS_TO]->(:Function {uid: 'target'}) RETURN count(rel) AS count",
                &empty,
            )
            .unwrap();
        assert_eq!(
            remaining.rows,
            vec![vec![serde_json::json!(expected_remaining)]]
        );
    }
}

#[test]
fn appstate_bolt_executor_unwind_relationship_delete_empty_input_is_noop() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    executor
        .execute_on_database(
            Some("copperdb"),
            "CREATE (:Function {uid: 'source'})-[:TAINT_FLOWS_TO]->(:Function {uid: 'target'})",
            &empty,
        )
        .unwrap();

    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .unwrap();
    let params = HashMap::from([("uids".to_string(), serde_json::json!([]))]);
    let deleted = executor
        .execute_in_transaction_with_context(
            &transaction,
            "UNWIND $uids AS source_uid MATCH (source:Function {uid: source_uid})-[rel:TAINT_FLOWS_TO]->() DELETE rel",
            &params,
            RequestContext::detached(),
            None,
        )
        .unwrap();
    assert_eq!(deleted.stats.relationships_deleted, 0);
    executor.commit_transaction(&transaction).unwrap();

    let remaining = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (:Function {uid: 'source'})-[rel:TAINT_FLOWS_TO]->(:Function {uid: 'target'}) RETURN count(rel) AS count",
            &empty,
        )
        .unwrap();
    assert_eq!(remaining.rows, vec![vec![serde_json::json!(1)]]);
}

#[test]
fn appstate_bolt_executor_rejects_connected_node_delete_in_transaction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    executor
        .execute_on_database(
            Some("copperdb"),
            "CREATE (:TxDeleteGuard {id: 'connected'})-[:LINKS]->(:TxDeleteGuard {id: 'peer'})",
            &empty,
        )
        .unwrap();

    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .unwrap();
    let error = executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (node:TxDeleteGuard {id: 'connected'}) DELETE node",
            &empty,
            RequestContext::detached(),
            None,
        )
        .unwrap_err();
    assert!(error.to_string().contains("still has relationships"));
    executor.rollback_transaction(&transaction).unwrap();

    let remaining = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (:TxDeleteGuard {id: 'connected'})-[:LINKS]->(:TxDeleteGuard {id: 'peer'}) RETURN count(*) AS count",
            &empty,
        )
        .unwrap();
    assert_eq!(remaining.rows, vec![vec![serde_json::json!(1)]]);
}

#[test]
fn appstate_bolt_executor_updates_staged_rows_with_foreach() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .expect("BEGIN should create a transaction context");

    executor
        .execute_in_transaction_with_context(
            &transaction,
            "CREATE (:TxForeach {id: 'a', score: 0})",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transactional CREATE should stage the FOREACH target");
    let inside = executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (n:TxForeach {id: 'a'}) FOREACH (increment IN [1, 2] | SET n.score = n.score + increment) RETURN n.score AS score",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("FOREACH should update the staged target through the transaction overlay");
    assert_eq!(inside.rows, vec![vec![serde_json::json!(3)]]);
    let outside = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (:TxForeach) RETURN count(*) AS count",
            &empty,
        )
        .expect("outside query should execute normally");
    assert_eq!(outside.rows, vec![vec![serde_json::json!(0)]]);
}

#[test]
fn appstate_bolt_executor_stages_set_and_detach_delete_writes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    executor
        .execute_on_database(
            Some("copperdb"),
            "CREATE (:TxUpdate {id: 'a', value: 1})-[:LINKS]->(:TxUpdate {id: 'b', value: 2})",
            &empty,
        )
        .expect("seed graph should be visible outside transactions");

    let update = executor
        .begin_transaction("copperdb", &empty, None)
        .expect("BEGIN should create a transaction context");
    executor
        .execute_in_transaction_with_context(
            &update,
            "MATCH (n:TxUpdate {id: 'a'}) SET n.value = 9 RETURN n.value AS value",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transactional SET should use the private overlay");
    let inside_update = executor
        .execute_in_transaction_with_context(
            &update,
            "MATCH (n:TxUpdate {id: 'a'}) RETURN n.value AS value",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transaction should read its staged property update");
    assert_eq!(inside_update.rows, vec![vec![serde_json::json!(9)]]);
    let outside_update = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (n:TxUpdate {id: 'a'}) RETURN n.value AS value",
            &empty,
        )
        .expect("outside query should execute normally");
    assert_eq!(outside_update.rows, vec![vec![serde_json::json!(1)]]);
    executor.commit_transaction(&update).unwrap();

    let delete = executor
        .begin_transaction("copperdb", &empty, None)
        .expect("BEGIN should create a transaction context");
    executor
        .execute_in_transaction_with_context(
            &delete,
            "MATCH (n:TxUpdate {id: 'a'}) DETACH DELETE n",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transactional detach delete should stage overlay tombstones");
    let inside_delete = executor
        .execute_in_transaction_with_context(
            &delete,
            "MATCH (:TxUpdate {id: 'a'})-[:LINKS]->(:TxUpdate {id: 'b'}) RETURN count(*) AS count",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transaction should not traverse its staged deletions");
    assert_eq!(inside_delete.rows, vec![vec![serde_json::json!(0)]]);
    executor.rollback_transaction(&delete).unwrap();

    let after_rollback = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (:TxUpdate {id: 'a'})-[:LINKS]->(:TxUpdate {id: 'b'}) RETURN count(*) AS count",
            &empty,
        )
        .expect("rollback should leave the original graph visible");
    assert_eq!(after_rollback.rows, vec![vec![serde_json::json!(1)]]);
}

#[test]
fn appstate_bolt_executor_stages_remove_writes_until_commit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    executor
        .execute_on_database(
            Some("copperdb"),
            "CREATE (:TxRemove:Temporary {id: 'remove-me', note: 'present'})",
            &empty,
        )
        .expect("seed node should be created");

    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .expect("BEGIN should create a transaction context");
    executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (n:TxRemove {id: 'remove-me'}) REMOVE n.note, n:Temporary RETURN n.id AS id",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transactional REMOVE should stage the update");
    let inside = executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (n:TxRemove {id: 'remove-me'}) RETURN n.note AS note",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transaction should read its staged removal");
    assert_eq!(inside.rows, vec![vec![serde_json::Value::Null]]);
    let outside = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (n:TxRemove:Temporary {id: 'remove-me'}) RETURN n.note AS note",
            &empty,
        )
        .expect("outside query should retain original state");
    assert_eq!(outside.rows, vec![vec![serde_json::json!("present")]]);

    executor.rollback_transaction(&transaction).unwrap();
    let restored = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (n:TxRemove:Temporary {id: 'remove-me'}) RETURN n.note AS note",
            &empty,
        )
        .expect("rollback should restore the original record");
    assert_eq!(restored.rows, vec![vec![serde_json::json!("present")]]);
}

#[test]
fn appstate_bolt_executor_stages_node_merge_writes_until_commit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .expect("BEGIN should create a transaction context");

    let created = executor
        .execute_in_transaction_with_context(
            &transaction,
            "MERGE (n:TxMerge {id: 'm1'}) ON CREATE SET n.value = 1 RETURN n.value AS value",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transactional MERGE should stage node creation");
    assert_eq!(created.rows, vec![vec![serde_json::json!(1)]]);
    let matched = executor
        .execute_in_transaction_with_context(
            &transaction,
            "MERGE (n:TxMerge {id: 'm1'}) ON MATCH SET n.value = 2 RETURN n.value AS value",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("repeated transactional MERGE should match the staged node");
    assert_eq!(matched.rows, vec![vec![serde_json::json!(2)]]);
    let outside = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (n:TxMerge {id: 'm1'}) RETURN count(n) AS count",
            &empty,
        )
        .expect("outside query should execute normally");
    assert_eq!(outside.rows, vec![vec![serde_json::json!(0)]]);

    executor.rollback_transaction(&transaction).unwrap();
    let rolled_back = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (n:TxMerge {id: 'm1'}) RETURN count(n) AS count",
            &empty,
        )
        .expect("rollback should discard the staged merge");
    assert_eq!(rolled_back.rows, vec![vec![serde_json::json!(0)]]);
}

#[test]
fn appstate_bolt_executor_stages_relationship_merge_writes_until_commit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    executor
        .execute_on_database(
            Some("copperdb"),
            "CREATE (:TxRelMerge {id: 'a'}), (:TxRelMerge {id: 'b'})",
            &empty,
        )
        .expect("merge endpoints should be created");

    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .expect("BEGIN should create a transaction context");
    let created = executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (a:TxRelMerge {id: 'a'}), (b:TxRelMerge {id: 'b'}) MERGE (a)-[r:LINKS]->(b) ON CREATE SET r.value = 1 RETURN r.value AS value",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transactional relationship MERGE should stage edge creation");
    assert_eq!(created.rows, vec![vec![serde_json::json!(1)]]);
    let matched = executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (a:TxRelMerge {id: 'a'}), (b:TxRelMerge {id: 'b'}) MERGE (a)-[r:LINKS]->(b) ON MATCH SET r.value = 2 RETURN r.value AS value",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("repeated relationship MERGE should match the staged edge");
    assert_eq!(matched.rows, vec![vec![serde_json::json!(2)]]);
    let outside = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (:TxRelMerge {id: 'a'})-[:LINKS]->(:TxRelMerge {id: 'b'}) RETURN count(*) AS count",
            &empty,
        )
        .expect("outside query should execute normally");
    assert_eq!(outside.rows, vec![vec![serde_json::json!(0)]]);

    executor.commit_transaction(&transaction).unwrap();
    let committed = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH ()-[r:LINKS]->() RETURN r.value AS value",
            &empty,
        )
        .expect("committed relationship merge should become visible");
    assert_eq!(committed.rows, vec![vec![serde_json::json!(2)]]);
}

#[test]
fn appstate_bolt_executor_stages_map_and_label_set_writes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();
    executor
        .execute_on_database(
            Some("copperdb"),
            "CREATE (:TxSetMap {id: 'map-1', original: true})",
            &empty,
        )
        .expect("seed node should be created");

    let transaction = executor
        .begin_transaction("copperdb", &empty, None)
        .expect("BEGIN should create a transaction context");
    executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (n:TxSetMap {id: 'map-1'}) SET n += {extra: 4}, n:TxSetLabel RETURN n.extra AS extra",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transactional map merge and label assignment should stage updates");
    let inside = executor
        .execute_in_transaction_with_context(
            &transaction,
            "MATCH (n:TxSetMap:TxSetLabel {id: 'map-1'}) RETURN n.extra AS extra",
            &empty,
            RequestContext::detached(),
            None,
        )
        .expect("transaction should read map and label updates");
    assert_eq!(inside.rows, vec![vec![serde_json::json!(4)]]);
    executor.rollback_transaction(&transaction).unwrap();

    let outside = executor
        .execute_on_database(
            Some("copperdb"),
            "MATCH (n:TxSetMap:TxSetLabel {id: 'map-1'}) RETURN count(n) AS count",
            &empty,
        )
        .expect("outside query should not see rolled-back updates");
    assert_eq!(outside.rows, vec![vec![serde_json::json!(0)]]);
}

#[test]
fn appstate_bolt_executor_seeds_demo_sized_star_batch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();

    executor
        .execute_on_database(Some("system"), "CREATE DATABASE d3_demo", &empty)
        .expect("Bolt executor should create the demo database");
    executor
        .execute_on_database(
            Some("d3_demo"),
            "CREATE INDEX star_id_idx IF NOT EXISTS FOR (n:Star) ON (n.starId)",
            &empty,
        )
        .expect("Bolt executor should create the demo index");

    let rows: Vec<serde_json::Value> = (0..400)
        .map(|idx| {
            serde_json::json!({
                "starId": format!("s0-{idx}"),
                "name": format!("Star {idx}"),
                "sector": 0,
                "hue": idx % 360,
                "mass": 1 + (idx % 18),
                "x": idx,
                "y": idx * 2,
                "z": idx * 3,
            })
        })
        .collect();
    let mut params = HashMap::new();
    params.insert("rows".into(), serde_json::json!(rows));

    executor
        .execute_on_database(
            Some("d3_demo"),
            "UNWIND $rows AS row MERGE (n:Star {starId: row.starId}) SET n.name = row.name, n.sector = row.sector, n.hue = row.hue, n.mass = row.mass, n.x = row.x, n.y = row.y, n.z = row.z",
            &params,
        )
        .expect("Bolt executor should seed a demo-sized star batch");

    let count = executor
        .execute_on_database(
            Some("d3_demo"),
            "MATCH (n:Star) RETURN count(n) AS stars",
            &empty,
        )
        .expect("Bolt executor should query seeded demo stars");
    assert_eq!(count.columns, vec!["stars"]);
    assert_eq!(count.rows[0][0], serde_json::json!(400));
}

#[test]
fn appstate_bolt_executor_links_demo_sized_hyperlane_batch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = demo_temp_appstate_with_catalog(&temp_dir);
    let executor = AppStateBoltExecutor::new(state);
    let empty = HashMap::new();

    executor
        .execute_on_database(Some("system"), "CREATE DATABASE d3_demo", &empty)
        .expect("Bolt executor should create the demo database");

    let rows: Vec<serde_json::Value> = (0..=400)
        .map(|idx| {
            serde_json::json!({
                "starId": format!("s0-{idx}"),
                "name": format!("Star {idx}"),
            })
        })
        .collect();
    let mut params = HashMap::new();
    params.insert("rows".into(), serde_json::json!(rows));

    executor
        .execute_on_database(
            Some("d3_demo"),
            "UNWIND $rows AS row MERGE (n:Star {starId: row.starId}) SET n.name = row.name",
            &params,
        )
        .expect("Bolt executor should seed demo stars for link batch");

    let rows: Vec<serde_json::Value> = (0..400)
        .map(|idx| {
            serde_json::json!({
                "fromId": format!("s0-{idx}"),
                "toId": format!("s0-{}", idx + 1),
                "distance": idx,
            })
        })
        .collect();
    params.insert("rows".into(), serde_json::json!(rows));

    let started = std::time::Instant::now();
    executor
        .execute_on_database(
            Some("d3_demo"),
            "UNWIND $rows AS row MATCH (a:Star {starId: row.fromId}) MATCH (b:Star {starId: row.toId}) MERGE (a)-[r:HYPERLANE]->(b) SET r.distance = row.distance",
            &params,
        )
        .expect("Bolt executor should link a demo-sized hyperlane batch");
    let elapsed = started.elapsed();

    let count = executor
        .execute_on_database(
            Some("d3_demo"),
            "MATCH ()-[r:HYPERLANE]->() RETURN count(r) AS hyperlanes",
            &empty,
        )
        .expect("Bolt executor should query linked demo hyperlanes");
    assert_eq!(count.columns, vec!["hyperlanes"]);
    assert_eq!(count.rows[0][0], serde_json::json!(400));
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "400-row Bolt relationship link batch should stay on the fast path, took {elapsed:?}"
    );

    let shortest_path = "MATCH (start:Star {starId: $startId}), (end:Star {starId: $endId}) MATCH p = shortestPath((start)-[:HYPERLANE*]-(end)) RETURN [n IN nodes(p) | n.starId] AS pathIds, length(p) AS hops LIMIT 1";
    let shortest_params = HashMap::from([
        ("startId".into(), serde_json::json!("s0-0")),
        ("endId".into(), serde_json::json!("s0-400")),
    ]);
    let started = std::time::Instant::now();
    let shortest = executor
        .execute_on_database(Some("d3_demo"), shortest_path, &shortest_params)
        .expect("Bolt executor should answer demo-sized shortest path quickly");
    let elapsed = started.elapsed();

    assert_eq!(shortest.columns, vec!["pathIds", "hops"]);
    assert_eq!(shortest.rows.len(), 1);
    let path_ids = shortest.rows[0][0]
        .as_array()
        .expect("expected shortest path star ids");
    assert_eq!(path_ids.first(), Some(&serde_json::json!("s0-0")));
    assert_eq!(path_ids.last(), Some(&serde_json::json!("s0-400")));
    assert_eq!(shortest.rows[0][1], serde_json::json!(400));
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "400-hop Bolt shortest path should stay on the optimized BFS path, took {elapsed:?}"
    );
}
