use super::*;

fn row<'a>(result: &'a QueryResult, name: &str) -> &'a HashMap<String, Value> {
    result
        .rows
        .iter()
        .find(|row| row.get("name") == Some(&Value::String(name.into())))
        .unwrap_or_else(|| panic!("missing SHOW SETTINGS row for {name}"))
}

#[test]
fn show_settings_returns_sorted_registry_metadata_and_selection() {
    let database = CopperDb::from_storage(
        Arc::new(StorageEngine::open_memory().unwrap()),
        DatabaseConfig::default(),
    )
    .unwrap();
    let result = database
        .execute(
            "SHOW SETTINGS db.copper.query_plan_cache.max_entries, db.copper.memory.storage.mode, db.copper.memory.storage.mode",
            HashMap::new(),
        )
        .unwrap();

    assert_eq!(
        result.columns,
        [
            "name",
            "description",
            "value",
            "isDynamic",
            "defaultValue",
            "startupValue",
            "validValues",
            "isExplicitlySet",
            "isDeprecated",
        ]
    );
    assert_eq!(result.rows.len(), 2);
    assert_eq!(
        result.rows[0]["name"],
        Value::String("db.copper.memory.storage.mode".into())
    );
    assert_eq!(
        result.rows[1]["name"],
        Value::String("db.copper.query_plan_cache.max_entries".into())
    );
    assert_eq!(result.rows[0]["value"], Value::String("default".into()));
    assert_eq!(result.rows[0]["isDynamic"], Value::Bool(false));
    assert_eq!(
        result.rows[0]["validValues"],
        serde_json::json!(["default", "low"])
    );
}

#[test]
fn show_settings_uses_active_values_and_redacts_secrets() {
    let mut runtime_config = DatabaseConfig::default().runtime_config;
    runtime_config
        .effective
        .insert("db.copper.embedding.api.key".into(), "active-secret".into());
    runtime_config
        .effective
        .insert("db.copper.search.vector.warming".into(), "lazy".into());
    let config = DatabaseConfig {
        configured_settings: BTreeMap::from([
            (
                "db.copper.embedding.api.key".into(),
                "configured-secret".into(),
            ),
            ("db.copper.search.vector.warming".into(), "eager".into()),
        ]),
        runtime_config,
        ..Default::default()
    };
    let database =
        CopperDb::from_storage(Arc::new(StorageEngine::open_memory().unwrap()), config).unwrap();

    let result = database
        .execute(
            "SHOW SETTINGS db.copper.search.vector.warming, db.copper.embedding.api.key",
            HashMap::new(),
        )
        .unwrap();
    let secret = row(&result, "db.copper.embedding.api.key");
    assert_eq!(secret["value"], Value::String("<REDACTED>".into()));
    assert_eq!(secret["startupValue"], Value::String("<REDACTED>".into()));
    assert_eq!(secret["isExplicitlySet"], Value::Bool(true));

    let warming = row(&result, "db.copper.search.vector.warming");
    assert_eq!(warming["value"], Value::String("lazy".into()));
    assert_eq!(warming["startupValue"], Value::String("lazy".into()));
    assert_eq!(warming["isExplicitlySet"], Value::Bool(true));
}

#[test]
fn engine_applies_resolved_index_capacity_policy() {
    let mut global = copperdb_config::Config::default();
    global.search.bm25_enabled = true;
    global.search.vector_enabled = true;
    let runtime_config = copperdb_config::resolve_per_database_config(
        &global,
        &BTreeMap::from([
            ("db.copper.memory.index.bm25.max".into(), "1m".into()),
            ("db.copper.memory.index.vector.max".into(), "2m".into()),
            ("db.copper.memory.index.metadata.max".into(), "3m".into()),
            ("db.copper.index.vector.storage".into(), "disk".into()),
        ]),
    )
    .unwrap();
    let database = CopperDb::from_storage(
        Arc::new(StorageEngine::open_memory().unwrap()),
        DatabaseConfig {
            runtime_config,
            ..Default::default()
        },
    )
    .unwrap();

    let policy = database.storage_engine().index_capacity_policy();
    assert!(policy.bm25_enabled);
    assert!(policy.vector_enabled);
    assert_eq!(policy.vector_dimensions, 1_024);
    assert_eq!(policy.bm25_memory_max_bytes, 1 << 20);
    assert_eq!(policy.vector_memory_max_bytes, 2 << 20);
    assert_eq!(policy.metadata_memory_max_bytes, 3 << 20);
    assert_eq!(policy.bm25_storage_mode, "memory");
    assert_eq!(policy.vector_storage_mode, "disk");
}

#[test]
fn engine_constructs_external_reranker_from_database_settings() {
    let runtime_config = copperdb_config::resolve_per_database_config(
        &copperdb_config::Config::default(),
        &BTreeMap::from([
            ("COPPERDB_SEARCH_RERANK_ENABLED".into(), "true".into()),
            ("COPPERDB_SEARCH_RERANK_PROVIDER".into(), "ollama".into()),
            ("COPPERDB_SEARCH_RERANK_MODEL".into(), "reranker".into()),
            (
                "COPPERDB_SEARCH_RERANK_API_URL".into(),
                "http://127.0.0.1:1/rerank".into(),
            ),
        ]),
    )
    .unwrap();
    let database = CopperDb::from_storage(
        Arc::new(StorageEngine::open_memory().unwrap()),
        DatabaseConfig {
            runtime_config,
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(database.reranker_name(), Some("cross_encoder"));
}

#[test]
fn engine_keeps_database_available_when_local_reranker_model_is_missing() {
    let runtime_config = copperdb_config::resolve_per_database_config(
        &copperdb_config::Config::default(),
        &BTreeMap::from([
            ("COPPERDB_SEARCH_RERANK_ENABLED".into(), "true".into()),
            ("COPPERDB_SEARCH_RERANK_PROVIDER".into(), "local".into()),
            (
                "COPPERDB_SEARCH_RERANK_MODEL".into(),
                "/definitely/missing/reranker.gguf".into(),
            ),
        ]),
    )
    .unwrap();
    let database = CopperDb::from_storage(
        Arc::new(StorageEngine::open_memory().unwrap()),
        DatabaseConfig {
            runtime_config,
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(database.reranker_name(), None);
}

#[test]
fn engine_applies_external_reranker_to_search_outcome() {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request).unwrap();
        let body = r#"{"scores":[0.1,0.9]}"#;
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .unwrap();
    });
    let runtime_config = copperdb_config::resolve_per_database_config(
        &copperdb_config::Config::default(),
        &BTreeMap::from([
            ("COPPERDB_SEARCH_RERANK_ENABLED".into(), "true".into()),
            (
                "COPPERDB_SEARCH_RERANK_API_URL".into(),
                format!("http://{address}/rerank"),
            ),
        ]),
    )
    .unwrap();
    let storage = Arc::new(StorageEngine::open_memory().unwrap());
    for (id, text) in [("first", "alpha"), ("second", "beta")] {
        storage
            .put_node_record(&NodeRecord {
                id: id.into(),
                labels: vec!["Document".into()],
                properties: BTreeMap::from([("text".into(), Value::String(text.into()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
    }
    let database = CopperDb::from_storage(
        storage,
        DatabaseConfig {
            runtime_config,
            ..Default::default()
        },
    )
    .unwrap();
    let placement = PlacementKey::default_for_database("copperdb");
    let hit = |id: &str, score: f32| copperdb_search::RrfMergedHit {
        global_id: FabricGlobalId::new(placement.clone(), "node", id),
        rrf_score: score,
        best_score: score,
        vector_rank: 0,
        bm25_rank: 0,
        sources: vec!["lexical".into()],
        shard: placement.clone(),
        label: "Document".into(),
        snippet: None,
    };
    let outcome = RrfSearchOutcome {
        results: vec![hit("first", 0.6), hit("second", 0.5)],
        touched_shards: vec![placement],
        sources: vec!["lexical".into()],
        input_hits: 2,
        fused_hits: 2,
        output_hits: 2,
        filtered_hits: 0,
    };

    let reranked = database.rerank_search_outcome(
        &copperdb_util::RequestContext::detached(),
        "query",
        outcome,
        2,
    );
    assert_eq!(reranked.results[0].global_id.local_id, "second");
    assert_eq!(reranked.results[1].global_id.local_id, "first");
    server.join().unwrap();
}
