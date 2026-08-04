use super::*;
use crate::copperdb::distributed_shortest_path_query_shape;
use copperdb_storage::EdgeRecord;

#[test]
fn local_semantic_search_uses_compatible_maintained_node_indexes() {
    use copperdb_storage::{
        IndexDefinition, IndexEntityType, IndexKind, NodeRecord, StorageEngine,
    };
    use copperdb_topology::PlacementKey;

    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("db");
    let storage = StorageEngine::open(&data_dir).unwrap();
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
            &HashMap::from([(
                "indexConfig".into(),
                serde_json::json!({
                    "vector.dimensions": 2,
                    "vector.similarity_function": "cosine"
                }),
            )]),
        )
        .unwrap();
    for (id, embedding) in [
        ("document:a", vec![1.0, 0.0]),
        ("document:b", vec![0.8, 0.2]),
        ("document:c", vec![0.0, 1.0]),
    ] {
        storage
            .put_node_record(&NodeRecord {
                id: id.into(),
                labels: vec!["Document".into()],
                properties: BTreeMap::new(),
                named_embeddings: BTreeMap::from([("embedding".into(), embedding)]),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
    }
    drop(storage);

    let mut config = DatabaseConfig {
        data_dir: data_dir.to_string_lossy().into_owned(),
        ..Default::default()
    };
    config.runtime_config.vector_enabled = true;
    let db = CopperDb::open(config).unwrap();
    let batch = db
        .search_fabric_ranked_batch_locally(
            &PlacementKey::default_for_database("copper"),
            &SearchQuery::Semantic {
                vector: vec![1.0, 0.0],
                k: 3,
                min_score: 0.9,
            },
        )
        .unwrap();

    assert_eq!(batch.source, "semantic");
    assert_eq!(batch.hits.len(), 2);
    assert_eq!(batch.hits[0].global_id.local_id, "document:a");
    assert_eq!(batch.hits[0].rank, 1);
    assert_eq!(batch.hits[0].label, "Document");
    assert_eq!(batch.hits[1].global_id.local_id, "document:b");
    assert!(batch.hits.iter().all(|hit| hit.score >= 0.9));
}

#[test]
fn local_semantic_search_returns_request_cancelled_before_candidate_work() {
    use copperdb_topology::PlacementKey;
    use copperdb_util::RequestContext;

    let dir = tempfile::tempdir().unwrap();
    let mut config = DatabaseConfig {
        data_dir: dir.path().join("db").to_string_lossy().into_owned(),
        ..Default::default()
    };
    config.runtime_config.vector_enabled = true;
    let db = CopperDb::open(config).unwrap();
    let request_context = RequestContext::detached();
    request_context.cancel();

    let error = db
        .search_fabric_ranked_batch_locally_with_context(
            &request_context,
            &PlacementKey::default_for_database("copper"),
            &SearchQuery::Semantic {
                vector: vec![1.0, 0.0],
                k: 1,
                min_score: f32::NEG_INFINITY,
            },
        )
        .unwrap_err();

    assert!(matches!(error, CopperDbError::RequestCancelled(_)));
}

#[test]
fn local_semantic_search_returns_request_cancelled_after_deadline() {
    use copperdb_topology::PlacementKey;
    use copperdb_util::RequestContext;
    use std::time::UNIX_EPOCH;

    let dir = tempfile::tempdir().unwrap();
    let mut config = DatabaseConfig {
        data_dir: dir.path().join("db").to_string_lossy().into_owned(),
        ..Default::default()
    };
    config.runtime_config.vector_enabled = true;
    let db = CopperDb::open(config).unwrap();
    let (request_context, _request_guard) = RequestContext::root(Some(UNIX_EPOCH));

    let error = db
        .search_fabric_ranked_batch_locally_with_context(
            &request_context,
            &PlacementKey::default_for_database("copper"),
            &SearchQuery::Semantic {
                vector: vec![1.0, 0.0],
                k: 1,
                min_score: f32::NEG_INFINITY,
            },
        )
        .unwrap_err();

    assert!(matches!(error, CopperDbError::RequestCancelled(_)));
}

#[test]
fn local_hybrid_search_fuses_duplicate_lexical_and_semantic_hits() {
    use copperdb_storage::{
        IndexDefinition, IndexEntityType, IndexKind, NodeRecord, StorageEngine,
    };
    use copperdb_topology::PlacementKey;

    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("db");
    let storage = StorageEngine::open(&data_dir).unwrap();
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
    ] {
        storage.persist_index_definition(&definition).unwrap();
    }
    storage
        .persist_index_options(
            "document_embedding",
            &HashMap::from([(
                "indexConfig".into(),
                serde_json::json!({
                    "vector.dimensions": 2,
                    "vector.similarity_function": "cosine"
                }),
            )]),
        )
        .unwrap();
    for (id, title, embedding) in [
        ("document:a", "database internals", vec![1.0, 0.0]),
        ("document:b", "graph database", vec![0.8, 0.2]),
    ] {
        storage
            .put_node_record(&NodeRecord {
                id: id.into(),
                labels: vec!["Document".into()],
                properties: BTreeMap::from([("title".into(), Value::String(title.into()))]),
                named_embeddings: BTreeMap::from([("embedding".into(), embedding)]),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
    }
    drop(storage);

    let mut config = DatabaseConfig {
        data_dir: data_dir.to_string_lossy().into_owned(),
        ..Default::default()
    };
    config.runtime_config.bm25_enabled = true;
    config.runtime_config.vector_enabled = true;
    let db = CopperDb::open(config).unwrap();
    let batch = db
        .search_fabric_ranked_batch_locally(
            &PlacementKey::default_for_database("copper"),
            &SearchQuery::Hybrid {
                text: "graph".into(),
                vector: vec![1.0, 0.0],
                k: 2,
            },
        )
        .unwrap();

    assert_eq!(batch.source, "hybrid");
    assert_eq!(batch.hits.len(), 2);
    assert_eq!(batch.hits[0].global_id.local_id, "document:b");
    assert_eq!(batch.hits[0].rank, 1);
    assert_eq!(batch.hits[1].global_id.local_id, "document:a");
    assert!(batch.hits[0].score > batch.hits[1].score);
}

#[test]
fn local_fulltext_search_suppresses_decayed_candidates_before_limit() {
    use copperdb_storage::{
        DecayProfileBindingSchema, DecayProfileSchema, IndexDefinition, IndexEntityType, IndexKind,
        NodeRecord, PromotionPolicySchema, PromotionProfileSchema, PromotionWhenClauseSchema,
        StorageEngine,
    };
    use copperdb_topology::PlacementKey;

    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("db");
    let storage = StorageEngine::open(&data_dir).unwrap();
    storage
        .persist_index_definition(&IndexDefinition {
            name: "document_title".into(),
            entity_type: IndexEntityType::Node,
            label: "Document".into(),
            properties: vec!["title".into()],
            kind: IndexKind::FullText,
        })
        .unwrap();
    let mut transaction = storage.begin_transaction().unwrap();
    transaction
        .put_decay_profile(DecayProfileSchema {
            name: "document_decay".into(),
            half_life_seconds: 1,
            visibility_threshold: 0.9,
            score_floor: 0.0,
            function: "exponential".into(),
            scope: "NODE".into(),
            decay_enabled: true,
            score_from: "CREATED".into(),
            score_from_property: None,
            enabled: true,
        })
        .unwrap();
    transaction
        .put_decay_binding(DecayProfileBindingSchema {
            name: "document_decay_binding".into(),
            target_labels: vec!["Document".into()],
            target_edge_type: None,
            is_wildcard: false,
            is_edge: false,
            profile_ref: Some("document_decay".into()),
            no_decay: false,
            visibility_threshold: None,
            order: 0,
        })
        .unwrap();
    transaction
        .put_promotion_profile(PromotionProfileSchema {
            name: "urgent_document".into(),
            scope: "NODE".into(),
            multiplier: 1.0,
            score_floor: 0.95,
            score_cap: 1.0,
            enabled: true,
        })
        .unwrap();
    transaction
        .put_promotion_policy(PromotionPolicySchema {
            name: "urgent_document_policy".into(),
            target_labels: vec!["Document".into()],
            target_edge_type: None,
            is_wildcard: false,
            is_edge: false,
            enabled: true,
            on_access_mutations: Vec::new(),
            when_clauses: vec![PromotionWhenClauseSchema {
                profile_ref: "urgent_document".into(),
                predicate: "n.priority = 'urgent'".into(),
                order: 0,
            }],
        })
        .unwrap();
    transaction.commit().unwrap();
    for (id, title, priority, created_at_unix_ms) in [
        ("document:expired", "graph graph graph graph", "urgent", 0),
        ("document:fresh", "graph", "normal", i64::MAX),
    ] {
        storage
            .put_node_record(&NodeRecord {
                id: id.into(),
                labels: vec!["Document".into()],
                properties: BTreeMap::from([
                    ("title".into(), Value::String(title.into())),
                    ("priority".into(), Value::String(priority.into())),
                ]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms,
                updated_at_unix_ms: created_at_unix_ms,
            })
            .unwrap();
    }
    drop(storage);

    let mut config = DatabaseConfig {
        data_dir: data_dir.to_string_lossy().into_owned(),
        ..Default::default()
    };
    config.runtime_config.bm25_enabled = true;
    let db = CopperDb::open(config).unwrap();
    let batch = db
        .search_fabric_ranked_batch_locally(
            &PlacementKey::default_for_database("copper"),
            &SearchQuery::FullText {
                query: "graph".into(),
                fields: Vec::new(),
                limit: 1,
            },
        )
        .unwrap();

    assert_eq!(batch.hits.len(), 1);
    assert_eq!(batch.hits[0].global_id.local_id, "document:expired");
    assert_eq!(batch.hits[0].rank, 1);
}

#[test]
fn local_fulltext_search_suppresses_restricted_labels_before_limit() {
    use copperdb_compliance::{ComplianceControl, CompliancePolicy};
    use copperdb_storage::{IndexDefinition, IndexEntityType, IndexKind, NodeRecord};
    use copperdb_topology::PlacementKey;
    use copperdb_util::RequestContext;

    let dir = tempfile::tempdir().unwrap();
    let mut config = DatabaseConfig {
        data_dir: dir.path().join("db").to_string_lossy().into_owned(),
        ..Default::default()
    };
    config.runtime_config.bm25_enabled = true;
    let db = CopperDb::open(config).unwrap();
    db.compliance_manager()
        .add_policy(CompliancePolicy::new(
            "patient-label",
            "Patient Label",
            ComplianceControl::RestrictLabel {
                label: "Patient".into(),
                allowed_roles: vec!["doctor".into()],
            },
        ))
        .unwrap();
    for label in ["Document", "Patient"] {
        db.storage()
            .persist_index_definition(&IndexDefinition {
                name: format!("{label}_title"),
                entity_type: IndexEntityType::Node,
                label: label.into(),
                properties: vec!["title".into()],
                kind: IndexKind::FullText,
            })
            .unwrap();
    }
    for (id, label, title) in [
        ("patient:alice", "Patient", "graph graph graph graph"),
        ("document:graph", "Document", "graph"),
    ] {
        db.storage()
            .put_node_record(&NodeRecord {
                id: id.into(),
                labels: vec![label.into()],
                properties: BTreeMap::from([("title".into(), Value::String(title.into()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
    }

    let request_context = RequestContext::detached();
    let reader_roles = vec!["reader".into()];
    let batch = db
        .search_fabric_ranked_batch_locally_scoped_with_context_and_roles(
            &request_context,
            &PlacementKey::default_for_database("copper"),
            &SearchQuery::FullText {
                query: "graph".into(),
                fields: Vec::new(),
                limit: 1,
            },
            &[],
            &BTreeMap::new(),
            &reader_roles,
        )
        .unwrap();

    assert_eq!(batch.hits.len(), 1);
    assert_eq!(batch.hits[0].global_id.local_id, "document:graph");
    assert_eq!(batch.hits[0].rank, 1);
}

#[tokio::test]
async fn engine_executes_fabric_ranked_search_with_transport() {
    use copperdb_search::{InMemorySearchTransport, RrfSearchHit};
    use copperdb_topology::{
        FabricDatabase, FabricGlobalId, FabricPartitionPolicy, FabricShard, FabricShardKind,
        MeshPeer, NodeCapability, PlacementKey, PlacementRecord,
    };

    let dir = tempfile::tempdir().unwrap();
    let mut config = DatabaseConfig {
        data_dir: dir.path().join("db").to_string_lossy().into_owned(),
        ..Default::default()
    };
    config.runtime_config.bm25_enabled = true;
    let db = CopperDb::open(config).unwrap();
    for node_id in ["search-a", "search-b", "search-c"] {
        db.storage()
            .register_topology_peer(
                &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
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
        db.storage()
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

    let transport = Arc::new(InMemorySearchTransport::new());
    transport.register_ranked_results(
        "search-a",
        RrfSearchBatch {
            shard: PlacementKey::new("default", "copper", "primary"),
            source: "lexical".into(),
            hits: vec![RrfSearchHit {
                global_id: FabricGlobalId::new(
                    PlacementKey::new("default", "copper", "primary"),
                    "node",
                    "a",
                ),
                rank: 1,
                score: 0.8,
                source: "lexical".into(),
                shard: PlacementKey::new("default", "copper", "primary"),
                label: "Person".into(),
                snippet: None,
            }],
        },
    );
    transport.register_ranked_results(
        "search-b",
        RrfSearchBatch {
            shard: PlacementKey::new("default", "copper", "person-00"),
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
                shard: PlacementKey::new("default", "copper", "person-00"),
                label: "Person".into(),
                snippet: Some("fresh".into()),
            }],
        },
    );

    transport.register_hydration_results(
        "search-a",
        vec![RrfHydrationRecord {
            global_id: FabricGlobalId::new(
                PlacementKey::new("default", "copper", "primary"),
                "node",
                "a",
            ),
            labels: vec!["Person".into()],
            entity: serde_json::json!({"id": "a", "name": "Alice", "secret": "internal"}),
        }],
    );

    let execution = db
        .execute_fabric_ranked_search_with_full_transport(
            &fabric,
            SearchQuery::FullText {
                query: "alice".into(),
                fields: vec!["body".into()],
                limit: 10,
            },
            ConsistencyLevel::One,
            RrfConfig::new(60.0, 10),
            RrfSearchPolicy {
                allowed_labels: vec!["Person".into()],
                denied_labels: Vec::new(),
                denied_sources: Vec::new(),
                require_hydration: true,
                redact_fields: vec!["secret".into()],
            },
            None,
            transport.clone(),
            transport,
        )
        .await
        .unwrap();

    assert_eq!(execution.responded_nodes, vec!["search-a", "search-b"]);
    assert_eq!(execution.failed_nodes, vec!["search-c"]);
    assert_eq!(execution.responded_shards.len(), 2);
    assert_eq!(execution.hydrated.output_hits, 1);
    assert_eq!(
        execution.hydrated.results[0].entity.as_ref().unwrap()["name"],
        "Alice"
    );
    assert!(execution.hydrated.results[0]
        .entity
        .as_ref()
        .unwrap()
        .get("secret")
        .is_none());
}

#[tokio::test]
async fn engine_blocks_ranked_search_when_database_search_is_disabled() {
    use copperdb_search::InMemorySearchTransport;
    use copperdb_topology::{
        FabricDatabase, FabricPartitionPolicy, FabricShard, FabricShardKind, MeshPeer,
        NodeCapability, PlacementKey, PlacementRecord,
    };

    let dir = tempfile::tempdir().unwrap();
    let mut config = DatabaseConfig {
        data_dir: dir.path().join("db").to_string_lossy().into_owned(),
        ..Default::default()
    };
    config.runtime_config.bm25_enabled = false;

    let db = CopperDb::open(config).unwrap();
    db.storage()
        .register_topology_peer(
            &MeshPeer::new("search-a", "search-a.mesh.local:9000")
                .with_capability(NodeCapability::Search)
                .with_capability(NodeCapability::Storage)
                .with_capability(NodeCapability::Coordinator),
        )
        .unwrap();
    db.storage()
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

    let fabric = FabricDatabase {
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
    };

    let error = db
        .execute_fabric_ranked_search_with_transport(
            &fabric,
            SearchQuery::FullText {
                query: "alice".into(),
                fields: vec!["body".into()],
                limit: 10,
            },
            Vec::new(),
            RrfConfig::new(60.0, 10),
            RrfSearchPolicy::default(),
            None,
            Arc::new(InMemorySearchTransport::new()),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, CopperDbError::Config(_)));
    assert!(error
        .to_string()
        .contains("fulltext search is disabled for this database"));
}

#[tokio::test]
async fn engine_blocks_semantic_and_hybrid_search_when_vector_search_is_disabled() {
    use copperdb_search::InMemorySearchTransport;
    use copperdb_topology::{
        FabricDatabase, FabricPartitionPolicy, FabricShard, FabricShardKind, MeshPeer,
        NodeCapability, PlacementKey, PlacementRecord,
    };

    let dir = tempfile::tempdir().unwrap();
    let mut config = DatabaseConfig {
        data_dir: dir.path().join("db").to_string_lossy().into_owned(),
        ..Default::default()
    };
    config.runtime_config.bm25_enabled = true;
    config.runtime_config.vector_enabled = false;

    let db = CopperDb::open(config).unwrap();
    db.storage()
        .register_topology_peer(
            &MeshPeer::new("search-a", "search-a.mesh.local:9000")
                .with_capability(NodeCapability::Search)
                .with_capability(NodeCapability::Storage)
                .with_capability(NodeCapability::Coordinator),
        )
        .unwrap();
    db.storage()
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

    let fabric = FabricDatabase {
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
    };

    let semantic_error = db
        .execute_fabric_ranked_search_with_transport(
            &fabric,
            SearchQuery::Semantic {
                vector: vec![0.1, 0.2, 0.3],
                k: 5,
                min_score: 0.4,
            },
            Vec::new(),
            RrfConfig::new(60.0, 10),
            RrfSearchPolicy::default(),
            None,
            Arc::new(InMemorySearchTransport::new()),
        )
        .await
        .unwrap_err();

    assert!(matches!(semantic_error, CopperDbError::Config(_)));
    assert!(semantic_error
        .to_string()
        .contains("vector search is disabled for this database"));

    let hybrid_error = db
        .execute_fabric_ranked_search_with_transport(
            &fabric,
            SearchQuery::Hybrid {
                text: "alice".into(),
                vector: vec![0.1, 0.2, 0.3],
                k: 5,
            },
            Vec::new(),
            RrfConfig::new(60.0, 10),
            RrfSearchPolicy::default(),
            None,
            Arc::new(InMemorySearchTransport::new()),
        )
        .await
        .unwrap_err();

    assert!(matches!(hybrid_error, CopperDbError::Config(_)));
    assert!(hybrid_error.to_string().contains(
        "hybrid search requires both fulltext and vector search to be enabled for this database"
    ));
}

#[tokio::test]
async fn engine_builds_cassandra_coordinator_with_durable_repair_queue() {
    use copperdb_replication::{Command, InMemoryReplicaTransport, MemoryStorage, RepairKind};
    use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

    let dir = tempfile::tempdir().unwrap();
    let db = CopperDb::open(DatabaseConfig {
        data_dir: dir.path().join("db").to_string_lossy().into_owned(),
        distributed_repair_queue_dir: Some(
            dir.path().join("repair").to_string_lossy().into_owned(),
        ),
        ..Default::default()
    })
    .unwrap();
    let placement = PlacementKey::default_for_database("copper");
    for node_id in ["node-1", "node-2", "node-3"] {
        db.storage()
            .register_topology_peer(
                &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
    }
    db.storage()
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

    let transport = Arc::new(InMemoryReplicaTransport::new());
    transport.register("node-1", Arc::new(MemoryStorage::new()));
    transport.register("node-2", Arc::new(MemoryStorage::new()));
    let coordinator = db.build_cassandra_coordinator(transport).unwrap();

    let outcome = coordinator
        .write(
            &placement,
            ConsistencyLevel::Quorum,
            Command::Put {
                key: b"engine".to_vec(),
                value: b"handoff".to_vec(),
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(outcome.failed_replicas, vec!["node-3"]);

    let pending = db.open_repair_queue().unwrap().pending().unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].kind, RepairKind::HintedHandoff);
    assert_eq!(pending[0].target_node, "node-3");
}

#[tokio::test]
async fn engine_replays_durable_repairs_through_replica_transport() {
    use copperdb_replication::{Command, InMemoryReplicaTransport, MemoryStorage};
    use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

    let dir = tempfile::tempdir().unwrap();
    let db = CopperDb::open(DatabaseConfig {
        data_dir: dir.path().join("db").to_string_lossy().into_owned(),
        distributed_repair_queue_dir: Some(
            dir.path().join("repair").to_string_lossy().into_owned(),
        ),
        ..Default::default()
    })
    .unwrap();
    let placement = PlacementKey::default_for_database("copper");
    for node_id in ["node-1", "node-2", "node-3"] {
        db.storage()
            .register_topology_peer(
                &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
    }
    db.storage()
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

    let first_transport = Arc::new(InMemoryReplicaTransport::new());
    first_transport.register("node-1", Arc::new(MemoryStorage::new()));
    first_transport.register("node-2", Arc::new(MemoryStorage::new()));
    db.build_cassandra_coordinator(first_transport)
        .unwrap()
        .write(
            &placement,
            ConsistencyLevel::Quorum,
            Command::Put {
                key: b"repair-replay".to_vec(),
                value: b"through-engine".to_vec(),
            },
            None,
        )
        .await
        .unwrap();

    let replay_transport = Arc::new(InMemoryReplicaTransport::new());
    let repaired_storage = Arc::new(MemoryStorage::new());
    replay_transport.register("node-3", repaired_storage.clone());
    let report = db.replay_repairs(replay_transport, 10).await.unwrap();

    assert_eq!(report.attempted, 1);
    assert_eq!(report.completed, 1);
    assert!(db
        .open_repair_queue()
        .unwrap()
        .pending()
        .unwrap()
        .is_empty());
    assert_eq!(
        repaired_storage.get(b"repair-replay"),
        Some(b"through-engine".to_vec())
    );
}

#[tokio::test]
async fn engine_builds_scheduled_repair_worker() {
    use copperdb_replication::{
        Command, InMemoryReplicaTransport, MemoryStorage, RepairWorkerConfig,
    };
    use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let db = CopperDb::open(DatabaseConfig {
        data_dir: dir.path().join("db").to_string_lossy().into_owned(),
        distributed_repair_queue_dir: Some(
            dir.path().join("repair").to_string_lossy().into_owned(),
        ),
        ..Default::default()
    })
    .unwrap();
    let placement = PlacementKey::default_for_database("copper");
    for node_id in ["node-1", "node-2", "node-3"] {
        db.storage()
            .register_topology_peer(
                &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
    }
    db.storage()
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

    let first_transport = Arc::new(InMemoryReplicaTransport::new());
    first_transport.register("node-1", Arc::new(MemoryStorage::new()));
    first_transport.register("node-2", Arc::new(MemoryStorage::new()));
    db.build_cassandra_coordinator(first_transport)
        .unwrap()
        .write(
            &placement,
            ConsistencyLevel::Quorum,
            Command::Put {
                key: b"scheduled-engine-repair".to_vec(),
                value: b"done".to_vec(),
            },
            None,
        )
        .await
        .unwrap();

    let replay_transport = Arc::new(InMemoryReplicaTransport::new());
    let repaired_storage = Arc::new(MemoryStorage::new());
    replay_transport.register("node-3", repaired_storage.clone());
    let worker = db
        .build_repair_worker(
            replay_transport,
            RepairWorkerConfig {
                interval: Duration::from_millis(10),
                max_records_per_tick: 10,
            },
        )
        .unwrap();

    let report = worker.run_once().await.unwrap();

    assert_eq!(report.attempted, 1);
    assert_eq!(report.completed, 1);
    assert_eq!(
        repaired_storage.get(b"scheduled-engine-repair"),
        Some(b"done".to_vec())
    );
}

#[tokio::test]
async fn engine_routes_mutating_cypher_through_cassandra_coordinator() {
    use copperdb_replication::{InMemoryReplicaTransport, MemoryStorage};
    use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

    let dir = tempfile::tempdir().unwrap();
    let db = CopperDb::open(DatabaseConfig {
        data_dir: dir.path().join("db").to_string_lossy().into_owned(),
        distributed_repair_queue_dir: Some(
            dir.path().join("repair").to_string_lossy().into_owned(),
        ),
        ..Default::default()
    })
    .unwrap();
    let placement = PlacementKey::default_for_database("copper");
    for node_id in ["node-1", "node-2", "node-3"] {
        db.storage()
            .register_topology_peer(
                &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
    }
    db.storage()
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

    let transport = Arc::new(InMemoryReplicaTransport::new());
    let replica1 = Arc::new(MemoryStorage::new());
    let replica2 = Arc::new(MemoryStorage::new());
    transport.register("node-1", replica1.clone());
    transport.register("node-2", replica2.clone());
    let outcome = db
        .execute_distributed_as(
            "CREATE (n:Distributed {v: 1})",
            HashMap::new(),
            &["admin".into()],
            &placement,
            ConsistencyLevel::Quorum,
            None,
            transport,
        )
        .await
        .unwrap();

    let write = outcome.write_outcome.unwrap();
    assert_eq!(write.acknowledged_by, vec!["node-1", "node-2"]);
    assert_eq!(write.failed_replicas, vec!["node-3"]);
    assert_eq!(replica1.cypher_log().len(), 1);
    assert_eq!(replica2.cypher_log().len(), 1);
    assert_eq!(outcome.result.stats.nodes_created, 1);
}

#[tokio::test]
async fn engine_routes_read_cypher_through_distributed_read_plan() {
    use copperdb_replication::InMemoryReplicaTransport;
    use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

    let dir = tempfile::tempdir().unwrap();
    let db = CopperDb::open(DatabaseConfig {
        data_dir: dir.path().join("db").to_string_lossy().into_owned(),
        ..Default::default()
    })
    .unwrap();
    db.execute("CREATE (n:DistributedRead {v: 1})", HashMap::new())
        .unwrap();
    let placement = PlacementKey::default_for_database("copper");
    for node_id in ["node-1", "node-2", "node-3"] {
        db.storage()
            .register_topology_peer(
                &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
    }
    db.storage()
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

    let outcome = db
        .execute_distributed_as(
            "MATCH (n:DistributedRead) RETURN n",
            HashMap::new(),
            &["admin".into()],
            &placement,
            ConsistencyLevel::Quorum,
            None,
            Arc::new(InMemoryReplicaTransport::new()),
        )
        .await
        .unwrap();

    assert!(outcome.write_outcome.is_none());
    let read = outcome.read_outcome.unwrap();
    assert_eq!(read.plan.required_responses, 2);
    assert_eq!(read.plan.replicas.len(), 3);
    assert_eq!(outcome.result.rows.len(), 1);
}

#[tokio::test]
async fn engine_routes_distributed_shortest_path_query_through_mesh_bfs() {
    use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
    use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

    fn graph_node(id: &str, name: &str) -> Vec<u8> {
        rmp_serde::to_vec(&BTreeMap::from([
            ("_id".to_string(), Value::String(id.to_string())),
            (
                "_labels".to_string(),
                Value::Array(vec![Value::String("Node".into())]),
            ),
            ("name".to_string(), Value::String(name.to_string())),
        ]))
        .unwrap()
    }

    let dir = tempfile::tempdir().unwrap();
    let db = CopperDb::open(DatabaseConfig {
        data_dir: dir.path().join("db").to_string_lossy().into_owned(),
        ..Default::default()
    })
    .unwrap();
    let placement = PlacementKey::default_for_database("copper");
    for node_id in ["node-1", "node-2", "node-3"] {
        db.storage()
            .register_topology_peer(
                &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
    }
    db.storage()
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

    let peer_one = StorageEngine::open_temporary().unwrap();
    peer_one
        .persist_index_definition(&copperdb_storage::IndexDefinition {
            name: "node_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Node".into(),
            properties: vec!["name".into()],
        })
        .unwrap();
    peer_one
        .put_node_record(&copperdb_storage::NodeRecord {
            id: "Node:A".into(),
            labels: vec!["Node".into()],
            properties: BTreeMap::from([("name".into(), Value::String("a".into()))]),

            named_embeddings: BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 123,
            updated_at_unix_ms: 456,
        })
        .unwrap();
    peer_one
        .put_edge_record(&EdgeRecord {
            id: "edge:a-b".into(),
            start_node: "Node:A".into(),
            end_node: "Node:B".into(),
            edge_type: "LINK".into(),
            properties: BTreeMap::from([("rank".into(), Value::from(1))]),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();

    let peer_two = StorageEngine::open_temporary().unwrap();
    peer_two
        .persist_index_definition(&copperdb_storage::IndexDefinition {
            name: "node_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Node".into(),
            properties: vec!["name".into()],
        })
        .unwrap();
    peer_two
        .put_node_record(&copperdb_storage::NodeRecord {
            id: "Node:B".into(),
            labels: vec!["Node".into()],
            properties: BTreeMap::from([("name".into(), Value::String("b".into()))]),

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
            properties: BTreeMap::from([("rank".into(), Value::from(2))]),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();
    peer_two
        .put_node("Node:C", &graph_node("Node:C", "c"))
        .unwrap();
    peer_two
        .put_edge_record(&EdgeRecord {
            id: "edge:a-c".into(),
            start_node: "Node:A".into(),
            end_node: "Node:C".into(),
            edge_type: "LINK".into(),
            properties: BTreeMap::from([("rank".into(), Value::from(5))]),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();

    let peer_three = StorageEngine::open_temporary().unwrap();
    peer_three
        .persist_index_definition(&copperdb_storage::IndexDefinition {
            name: "node_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Node".into(),
            properties: vec!["name".into()],
        })
        .unwrap();
    peer_three
        .put_node_record(&copperdb_storage::NodeRecord {
            id: "Node:D".into(),
            labels: vec!["Node".into()],
            properties: BTreeMap::from([("name".into(), Value::String("d".into()))]),

            named_embeddings: BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();
    peer_three
        .put_edge_record(&EdgeRecord {
            id: "edge:c-d".into(),
            start_node: "Node:C".into(),
            end_node: "Node:D".into(),
            edge_type: "LINK".into(),
            properties: BTreeMap::from([("rank".into(), Value::from(6))]),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();

    let transport = Arc::new(InMemoryReplicaTransport::new());
    transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
    transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));
    transport.register("node-3", Arc::new(StorageEngineAdapter::new(peer_three)));

    let outcome = db
            .execute_distributed_as(
                "MATCH p = shortestPath((a:Node {_id: 'Node:A'})-[:LINK*]->(d:Node {_id: 'Node:D'})) RETURN p AS path, nodes(p) AS nodes, relationships(p) AS rels, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

    assert!(outcome.write_outcome.is_none());
    let read = outcome
        .read_outcome
        .expect("expected distributed read outcome");
    assert_eq!(read.plan.required_responses, 2);
    assert_eq!(outcome.result.rows.len(), 1);
    assert_eq!(outcome.result.rows[0].get("hops"), Some(&Value::from(2)));

    let nodes = outcome.result.rows[0]
        .get("nodes")
        .and_then(Value::as_array)
        .expect("expected nodes(path)");
    let names = nodes
        .iter()
        .map(|node| node.get("name").and_then(Value::as_str).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["a", "b", "d"]);

    let rels = outcome.result.rows[0]
        .get("rels")
        .and_then(Value::as_array)
        .expect("expected relationships(path)");
    assert_eq!(rels.len(), 2);
    assert_eq!(rels[0].get("rank"), Some(&Value::from(1)));

    let path = outcome.result.rows[0]
        .get("path")
        .and_then(Value::as_object)
        .expect("expected path object");
    assert_eq!(path.get("length"), Some(&Value::from(2)));
}

#[tokio::test]
async fn engine_routes_distributed_shortest_path_query_with_property_endpoints() {
    use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
    use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

    let dir = tempfile::tempdir().unwrap();
    let db = CopperDb::open(DatabaseConfig {
        data_dir: dir.path().join("db").to_string_lossy().into_owned(),
        ..Default::default()
    })
    .unwrap();
    let placement = PlacementKey::default_for_database("copper");
    for node_id in ["node-1", "node-2", "node-3"] {
        db.storage()
            .register_topology_peer(
                &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
    }
    db.storage()
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

    let peer_one = StorageEngine::open_temporary().unwrap();
    peer_one
        .persist_index_definition(&copperdb_storage::IndexDefinition {
            name: "node_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Node".into(),
            properties: vec!["name".into()],
        })
        .unwrap();
    peer_one
        .put_node_record(&copperdb_storage::NodeRecord {
            id: "Node:A".into(),
            labels: vec!["Node".into()],
            properties: BTreeMap::from([("name".into(), Value::String("a".into()))]),

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

    let peer_two = StorageEngine::open_temporary().unwrap();
    peer_two
        .persist_index_definition(&copperdb_storage::IndexDefinition {
            name: "node_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Node".into(),
            properties: vec!["name".into()],
        })
        .unwrap();
    peer_two
        .put_node_record(&copperdb_storage::NodeRecord {
            id: "Node:B".into(),
            labels: vec!["Node".into()],
            properties: BTreeMap::from([("name".into(), Value::String("b".into()))]),

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

    let peer_three = StorageEngine::open_temporary().unwrap();
    peer_three
        .persist_index_definition(&copperdb_storage::IndexDefinition {
            name: "node_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Node".into(),
            properties: vec!["name".into()],
        })
        .unwrap();
    peer_three
        .put_node_record(&copperdb_storage::NodeRecord {
            id: "Node:D".into(),
            labels: vec!["Node".into()],
            properties: BTreeMap::from([("name".into(), Value::String("d".into()))]),

            named_embeddings: BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();

    let transport = Arc::new(InMemoryReplicaTransport::new());
    transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
    transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));
    transport.register("node-3", Arc::new(StorageEngineAdapter::new(peer_three)));

    let parsed = Parser::new()
            .parse(
                "MATCH p = shortestPath((a:Node {name: 'a'})-[:LINK*]->(d:Node {name: 'd'})) RETURN length(p) AS hops, p AS shortest",
            )
            .unwrap();
    assert!(distributed_shortest_path_query_shape(&parsed).is_some());
    assert_eq!(
        transport
            .graph_nodes_by_property("node-1", "Node", "name", &Value::String("a".into()), None,)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        transport
            .graph_nodes_by_property("node-3", "Node", "name", &Value::String("d".into()), None,)
            .await
            .unwrap()
            .len(),
        1
    );

    let outcome = db
            .execute_distributed_as(
                "MATCH p = shortestPath((a:Node {name: 'a'})-[:LINK*]->(d:Node {name: 'd'})) RETURN length(p) AS hops, p AS shortest",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

    assert_eq!(outcome.result.columns, vec!["hops", "shortest"]);
    assert_eq!(outcome.result.rows.len(), 1);
    assert_eq!(outcome.result.rows[0].get("hops"), Some(&Value::from(2)));
    let path = outcome.result.rows[0]
        .get("shortest")
        .and_then(Value::as_object)
        .expect("expected shortest path object");
    let nodes = path
        .get("nodes")
        .and_then(Value::as_array)
        .expect("expected shortest path nodes");
    let names = nodes
        .iter()
        .map(|node| node.get("name").and_then(Value::as_str).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["a", "b", "d"]);
}

#[tokio::test]
async fn engine_routes_distributed_single_node_path_query() {
    use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
    use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

    let dir = tempfile::tempdir().unwrap();
    let db = CopperDb::open(DatabaseConfig {
        data_dir: dir.path().join("db").to_string_lossy().into_owned(),
        ..Default::default()
    })
    .unwrap();
    let placement = PlacementKey::default_for_database("copper");
    for node_id in ["node-1", "node-2", "node-3"] {
        db.storage()
            .register_topology_peer(
                &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
    }
    db.storage()
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

    let peer_one = StorageEngine::open_temporary().unwrap();
    peer_one
        .persist_index_definition(&copperdb_storage::IndexDefinition {
            name: "node_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Node".into(),
            properties: vec!["name".into()],
        })
        .unwrap();
    peer_one
        .put_node_record(&copperdb_storage::NodeRecord {
            id: "Node:A".into(),
            labels: vec!["Node".into()],
            properties: BTreeMap::from([("name".into(), Value::String("a".into()))]),

            named_embeddings: BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 123,
            updated_at_unix_ms: 456,
        })
        .unwrap();

    let transport = Arc::new(InMemoryReplicaTransport::new());
    transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

    let outcome = db
        .execute_distributed_as(
            "MATCH p = (a:Node {name: 'a'}) RETURN length(p) AS hops, nodes(p) AS nodes, p AS path",
            HashMap::new(),
            &["admin".into()],
            &placement,
            ConsistencyLevel::One,
            None,
            transport,
        )
        .await
        .unwrap();

    assert_eq!(outcome.result.columns, vec!["hops", "nodes", "path"]);
    assert_eq!(outcome.result.rows.len(), 1);
    assert_eq!(outcome.result.rows[0].get("hops"), Some(&Value::from(0)));
    let nodes = outcome.result.rows[0]
        .get("nodes")
        .and_then(Value::as_array)
        .expect("expected node list");
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].get("name"), Some(&Value::String("a".into())));
    assert_eq!(nodes[0].get("_created_at_unix_ms"), Some(&Value::from(123)));
    assert_eq!(nodes[0].get("_updated_at_unix_ms"), Some(&Value::from(456)));
    let path = outcome.result.rows[0]
        .get("path")
        .and_then(Value::as_object)
        .expect("expected path object");
    assert_eq!(path.get("length"), Some(&Value::from(0)));
}

#[tokio::test]
async fn engine_distributed_single_node_path_suppresses_stale_remote_node() {
    use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
    use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

    let dir = tempfile::tempdir().unwrap();
    let db = CopperDb::open(DatabaseConfig {
        data_dir: dir.path().join("db").to_string_lossy().into_owned(),
        ..Default::default()
    })
    .unwrap();
    let placement = PlacementKey::default_for_database("copper");
    db.storage()
        .register_topology_peer(
            &MeshPeer::new("node-1", "node-1.mesh.local:9000")
                .with_capability(NodeCapability::Storage)
                .with_capability(NodeCapability::Coordinator),
        )
        .unwrap();
    db.storage()
        .register_topology_placement(&PlacementRecord {
            key: placement.clone(),
            primary_node: "node-1".into(),
            replica_nodes: vec![],
            search_nodes: vec![],
            hyperscaler_profile: None,
            min_write_replicas: 1,
            search_fanout: 1,
        })
        .unwrap();

    db.execute(
            "CREATE DECAY PROFILE stale_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.75, scoreFloor: 0.0, function: 'step', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
            Default::default(),
        )
        .unwrap();
    db.execute(
            "CREATE DECAY PROFILE stale_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE stale_decay, order: 10 }",
            Default::default(),
        )
        .unwrap();

    let peer = StorageEngine::open_temporary().unwrap();
    peer.put_node_record(&copperdb_storage::NodeRecord {
        id: "memory:stale".into(),
        labels: vec!["MemoryEpisode".into()],
        properties: BTreeMap::from([("name".into(), Value::String("stale".into()))]),

        named_embeddings: BTreeMap::new(),
        chunk_embeddings: Vec::new(),
        embed_meta: Default::default(),
        created_at_unix_ms: 0,
        updated_at_unix_ms: 0,
    })
    .unwrap();

    let transport = Arc::new(InMemoryReplicaTransport::new());
    transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer)));

    let outcome = db
        .execute_distributed_as(
            "MATCH p = (a:MemoryEpisode {_id: 'memory:stale'}) RETURN p AS path, length(p) AS hops",
            HashMap::new(),
            &["admin".into()],
            &placement,
            ConsistencyLevel::One,
            None,
            transport,
        )
        .await
        .unwrap();

    assert!(outcome.result.rows.is_empty());
}

#[tokio::test]
async fn engine_distributed_single_node_path_persists_remote_on_access_metadata() {
    use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
    use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

    let dir = tempfile::tempdir().unwrap();
    let db = CopperDb::open(DatabaseConfig {
        data_dir: dir.path().join("db").to_string_lossy().into_owned(),
        ..Default::default()
    })
    .unwrap();
    let placement = PlacementKey::default_for_database("copper");
    db.storage()
        .register_topology_peer(
            &MeshPeer::new("node-1", "node-1.mesh.local:9000")
                .with_capability(NodeCapability::Storage)
                .with_capability(NodeCapability::Coordinator),
        )
        .unwrap();
    db.storage()
        .register_topology_placement(&PlacementRecord {
            key: placement.clone(),
            primary_node: "node-1".into(),
            replica_nodes: vec![],
            search_nodes: vec![],
            hyperscaler_profile: None,
            min_write_replicas: 1,
            search_fanout: 1,
        })
        .unwrap();

    db.execute(
            "CREATE PROMOTION POLICY memory_access FOR (n:MemoryEpisode) APPLY { ON ACCESS { SET n.lastAccessedAt = timestamp() SET n.accessCount = coalesce(n.accessCount, 0) + 1 } }",
            Default::default(),
        )
        .unwrap();

    let peer_dir = tempfile::tempdir().unwrap();
    let peer_path = peer_dir.path().join("peer");
    let peer = StorageEngine::open(&peer_path).unwrap();
    peer.put_node_record(&copperdb_storage::NodeRecord {
        id: "memory:access".into(),
        labels: vec!["MemoryEpisode".into()],
        properties: BTreeMap::from([("name".into(), Value::String("access".into()))]),

        named_embeddings: BTreeMap::new(),
        chunk_embeddings: Vec::new(),
        embed_meta: Default::default(),
        created_at_unix_ms: 123,
        updated_at_unix_ms: 123,
    })
    .unwrap();

    let transport = Arc::new(InMemoryReplicaTransport::new());
    transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer)));

    let outcome = db
            .execute_distributed_as(
                "MATCH p = (a:MemoryEpisode {_id: 'memory:access'}) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

    assert_eq!(outcome.result.rows.len(), 1);
    assert_eq!(outcome.result.rows[0].get("hops"), Some(&Value::from(0)));

    let reopened_peer = StorageEngine::open(&peer_path).unwrap();
    let metadata = reopened_peer
        .get_knowledge_policy_access_metadata("memory:access")
        .unwrap()
        .expect("expected replicated node access metadata");
    assert_eq!(metadata.access_count, 1);
    assert!(metadata.last_accessed_at_unix_ms.is_some());
}

#[tokio::test]
async fn engine_routes_distributed_single_hop_path_query_with_edge_properties() {
    use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
    use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

    let dir = tempfile::tempdir().unwrap();
    let db = CopperDb::open(DatabaseConfig {
        data_dir: dir.path().join("db").to_string_lossy().into_owned(),
        ..Default::default()
    })
    .unwrap();
    let placement = PlacementKey::default_for_database("copper");
    for node_id in ["node-1", "node-2", "node-3"] {
        db.storage()
            .register_topology_peer(
                &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
    }
    db.storage()
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

    let peer_one = StorageEngine::open_temporary().unwrap();
    peer_one
        .persist_index_definition(&copperdb_storage::IndexDefinition {
            name: "node_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Node".into(),
            properties: vec!["name".into()],
        })
        .unwrap();
    peer_one
        .put_node_record(&copperdb_storage::NodeRecord {
            id: "Node:A".into(),
            labels: vec!["Node".into()],
            properties: BTreeMap::from([("name".into(), Value::String("a".into()))]),

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
            properties: BTreeMap::from([("rank".into(), Value::from(1))]),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();

    let peer_two = StorageEngine::open_temporary().unwrap();
    peer_two
        .persist_index_definition(&copperdb_storage::IndexDefinition {
            name: "node_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Node".into(),
            properties: vec!["name".into()],
        })
        .unwrap();
    peer_two
        .put_node_record(&copperdb_storage::NodeRecord {
            id: "Node:B".into(),
            labels: vec!["Node".into()],
            properties: BTreeMap::from([("name".into(), Value::String("b".into()))]),

            named_embeddings: BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();

    let transport = Arc::new(InMemoryReplicaTransport::new());
    transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
    transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));

    let outcome = db
            .execute_distributed_as(
                "MATCH p = (a:Node {name: 'a'})-[:LINK {rank: 1}]->(b:Node {name: 'b'}) RETURN length(p) AS hops, relationships(p) AS rels, nodes(p) AS nodes, p AS path",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

    assert_eq!(
        outcome.result.columns,
        vec!["hops", "rels", "nodes", "path"]
    );
    assert_eq!(outcome.result.rows.len(), 1);
    assert_eq!(outcome.result.rows[0].get("hops"), Some(&Value::from(1)));
    let rels = outcome.result.rows[0]
        .get("rels")
        .and_then(Value::as_array)
        .expect("expected relationship list");
    assert_eq!(rels.len(), 1);
    assert_eq!(rels[0].get("rank"), Some(&Value::from(1)));
    let nodes = outcome.result.rows[0]
        .get("nodes")
        .and_then(Value::as_array)
        .expect("expected node list");
    let names = nodes
        .iter()
        .map(|node| node.get("name").and_then(Value::as_str).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["a", "b"]);
}

#[tokio::test]
async fn engine_distributed_direct_path_suppresses_stale_remote_edge() {
    use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
    use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

    let dir = tempfile::tempdir().unwrap();
    let db = CopperDb::open(DatabaseConfig {
        data_dir: dir.path().join("db").to_string_lossy().into_owned(),
        ..Default::default()
    })
    .unwrap();
    let placement = PlacementKey::default_for_database("copper");
    db.storage()
        .register_topology_peer(
            &MeshPeer::new("node-1", "node-1.mesh.local:9000")
                .with_capability(NodeCapability::Storage)
                .with_capability(NodeCapability::Coordinator),
        )
        .unwrap();
    db.storage()
        .register_topology_placement(&PlacementRecord {
            key: placement.clone(),
            primary_node: "node-1".into(),
            replica_nodes: vec![],
            search_nodes: vec![],
            hyperscaler_profile: None,
            min_write_replicas: 1,
            search_fanout: 1,
        })
        .unwrap();

    db.execute(
            "CREATE DECAY PROFILE stale_edge_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.75, scoreFloor: 0.0, function: 'step', scope: 'EDGE', scoreFrom: 'CREATED', enabled: true }",
            Default::default(),
        )
        .unwrap();
    db.execute(
            "CREATE DECAY PROFILE stale_edge_binding FOR ()-[r:LINKS]-() APPLY { DECAY PROFILE stale_edge_decay, order: 10 }",
            Default::default(),
        )
        .unwrap();

    let peer = StorageEngine::open_temporary().unwrap();
    peer.put_node_record(&copperdb_storage::NodeRecord {
        id: "Node:A".into(),
        labels: vec!["Node".into()],
        properties: BTreeMap::from([("name".into(), Value::String("a".into()))]),

        named_embeddings: BTreeMap::new(),
        chunk_embeddings: Vec::new(),
        embed_meta: Default::default(),
        created_at_unix_ms: 123,
        updated_at_unix_ms: 123,
    })
    .unwrap();
    peer.put_node_record(&copperdb_storage::NodeRecord {
        id: "Node:B".into(),
        labels: vec!["Node".into()],
        properties: BTreeMap::from([("name".into(), Value::String("b".into()))]),

        named_embeddings: BTreeMap::new(),
        chunk_embeddings: Vec::new(),
        embed_meta: Default::default(),
        created_at_unix_ms: 123,
        updated_at_unix_ms: 123,
    })
    .unwrap();
    peer.put_edge_record(&EdgeRecord {
        id: "edge:a-b".into(),
        start_node: "Node:A".into(),
        end_node: "Node:B".into(),
        edge_type: "LINKS".into(),
        properties: BTreeMap::new(),
        created_at_unix_ms: 0,
        updated_at_unix_ms: 0,
    })
    .unwrap();

    let transport = Arc::new(InMemoryReplicaTransport::new());
    transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer)));

    let outcome = db
            .execute_distributed_as(
                "MATCH p = (a:Node {_id: 'Node:A'})-[:LINKS]->(b:Node {_id: 'Node:B'}) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

    assert!(outcome.result.rows.is_empty());
}
