use super::*;
use copperdb_kms::{LocalKms, LocalKmsConfig};
use copperdb_util::RequestCancellation;
use serde_json::json;
use std::fs;
use std::thread;
use std::time::{Duration, Instant};

fn local_provider(byte: u8) -> Arc<dyn KeyProvider> {
    Arc::new(LocalKms::new(LocalKmsConfig::new(vec![byte; 32])).unwrap())
}

fn sample_node(id: &str, labels: &[&str]) -> NodeRecord {
    NodeRecord {
        id: id.to_string(),
        labels: labels.iter().map(|s| s.to_string()).collect(),
        properties: BTreeMap::from([
            ("name".to_string(), json!("alice")),
            ("score".to_string(), json!(42)),
        ]),
        named_embeddings: BTreeMap::new(),
        chunk_embeddings: Vec::new(),
        embed_meta: NodeEmbeddingMetadata::default(),
        created_at_unix_ms: 1000,
        updated_at_unix_ms: 2000,
    }
}

fn sample_edge(id: &str, t: &str, start: &str, end: &str) -> EdgeRecord {
    EdgeRecord {
        id: id.to_string(),
        start_node: start.to_string(),
        end_node: end.to_string(),
        edge_type: t.to_string(),
        properties: BTreeMap::from([("weight".to_string(), json!(0.9))]),
        created_at_unix_ms: 123,
        updated_at_unix_ms: 456,
    }
}

#[test]
fn creates_and_reads_layout_manifest_v0() {
    let engine = StorageEngine::open_temporary().unwrap();
    assert!(engine.is_temporary());
    let manifest = engine.layout_manifest().unwrap();
    assert_eq!(manifest.version, STORAGE_LAYOUT_VERSION);
    assert!(manifest.created_at_unix_ms > 0);
    assert_eq!(engine.storage_layout_version().unwrap(), 0);
}

#[test]
fn rejects_non_v0_layout_manifest() {
    let test_dir = std::env::temp_dir().join(format!(
        "copperdb-storage-layout-version-rejection-test-{}",
        uuid::Uuid::new_v4()
    ));
    fs::create_dir_all(&test_dir).unwrap();
    let db = sled::open(&test_dir).unwrap();
    let meta = db.open_tree("meta").unwrap();

    let bad_manifest = StorageLayoutManifest {
        version: 1,
        created_at_unix_ms: 1,
    };
    meta.insert(
        META_LAYOUT_MANIFEST_KEY,
        rmp_serde::to_vec(&bad_manifest).unwrap(),
    )
    .unwrap();
    db.flush().unwrap();
    drop(meta);
    drop(db);

    let err = StorageEngine::open(&test_dir).err().unwrap();
    match err {
        StorageError::UnsupportedLayoutVersion { expected, actual } => {
            assert_eq!(expected, 0);
            assert_eq!(actual, 1);
        }
        _ => panic!("expected UnsupportedLayoutVersion"),
    }
    let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn raw_node_edge_round_trip() {
    let engine = StorageEngine::open_temporary().unwrap();
    engine.put_node("node:1", b"node_data").unwrap();
    engine.put_edge("edge:1", b"edge_data").unwrap();

    assert_eq!(
        engine.get_node("node:1").unwrap(),
        Some(b"node_data".to_vec())
    );
    assert_eq!(
        engine.get_edge("edge:1").unwrap(),
        Some(b"edge_data".to_vec())
    );

    engine.delete_node("node:1").unwrap();
    engine.delete_edge("edge:1").unwrap();

    assert!(engine.get_node("node:1").unwrap().is_none());
    assert!(engine.get_edge("edge:1").unwrap().is_none());
}

#[test]
fn encrypted_storage_round_trips_records_and_rejects_plain_open() {
    let test_dir = std::env::temp_dir().join(format!(
        "copperdb-storage-encryption-test-{}",
        uuid::Uuid::new_v4()
    ));
    fs::create_dir_all(&test_dir).unwrap();

    let engine =
        StorageEngine::open_encrypted(&test_dir, local_provider(0x42), "kms://local/default")
            .unwrap();
    assert!(engine.is_encrypted());
    let manifest = engine.encryption_manifest().unwrap().unwrap();
    assert_eq!(manifest.key_uri, "kms://local/default");

    let node = sample_node("db1:n1", &["Secret"]);
    let edge = sample_edge("db1:e1", "SECRET_EDGE", "db1:n1", "db1:n2");
    engine.put_node_record(&node).unwrap();
    engine.put_edge_record(&edge).unwrap();
    engine.put_node("raw:1", b"classified").unwrap();
    engine.flush().unwrap();
    drop(engine);

    let raw_db = sled::open(&test_dir).unwrap();
    let raw_nodes = raw_db.open_tree("nodes").unwrap();
    let stored = raw_nodes.get("db1:n1").unwrap().unwrap();
    assert_ne!(
        stored.as_ref(),
        rmp_serde::to_vec(&node).unwrap().as_slice()
    );
    drop(raw_nodes);
    drop(raw_db);

    let reopened =
        StorageEngine::open_encrypted(&test_dir, local_provider(0x42), "kms://local/default")
            .unwrap();
    assert_eq!(reopened.get_node_record("db1:n1").unwrap(), Some(node));
    assert_eq!(reopened.get_edge_record("db1:e1").unwrap(), Some(edge));
    assert_eq!(
        reopened.get_node("raw:1").unwrap(),
        Some(b"classified".to_vec())
    );
    drop(reopened);
    assert!(matches!(
        StorageEngine::open(&test_dir),
        Err(StorageError::EncryptionRequired)
    ));
    let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn encrypted_storage_rejects_wrong_key_uri() {
    let test_dir = std::env::temp_dir().join(format!(
        "copperdb-storage-encryption-key-uri-test-{}",
        uuid::Uuid::new_v4()
    ));
    fs::create_dir_all(&test_dir).unwrap();

    let engine =
        StorageEngine::open_encrypted(&test_dir, local_provider(0x42), "kms://local/a").unwrap();
    engine.flush().unwrap();
    drop(engine);

    let err = StorageEngine::open_encrypted(&test_dir, local_provider(0x42), "kms://local/b")
        .err()
        .unwrap();
    assert!(matches!(err, StorageError::EncryptionMismatch(_)));
    let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn node_record_indexes_are_maintained_and_updated() {
    let engine = StorageEngine::open_temporary().unwrap();

    let mut node = sample_node("db1:n1", &["Person", "Employee"]);
    engine.put_node_record(&node).unwrap();

    let person_nodes = engine.get_nodes_by_label("Person").unwrap();
    assert_eq!(person_nodes.len(), 1);
    assert_eq!(person_nodes[0], node);

    let employee_nodes = engine.get_nodes_by_label("Employee").unwrap();
    assert_eq!(employee_nodes.len(), 1);
    assert_eq!(employee_nodes[0].id, "db1:n1");

    node.labels = vec!["Person".to_string(), "Founder".to_string()];
    node.updated_at_unix_ms = 3000;
    node.properties.insert("rank".to_string(), json!("A"));
    engine.put_node_record(&node).unwrap();

    let founder_nodes = engine.get_nodes_by_label("Founder").unwrap();
    assert_eq!(founder_nodes.len(), 1);
    assert_eq!(founder_nodes[0].properties.get("rank"), Some(&json!("A")));

    let stale_employee_nodes = engine.get_nodes_by_label("Employee").unwrap();
    assert!(stale_employee_nodes.is_empty());

    engine.delete_node_record("db1:n1").unwrap();
    assert!(engine.get_node_record("db1:n1").unwrap().is_none());
    assert!(engine.get_nodes_by_label("Person").unwrap().is_empty());
    assert!(engine.get_nodes_by_label("Founder").unwrap().is_empty());
}

#[test]
fn edge_record_indexes_are_maintained_and_updated() {
    let engine = StorageEngine::open_temporary().unwrap();

    let mut edge = sample_edge("db1:e1", "KNOWS", "db1:n1", "db1:n2");
    engine.put_edge_record(&edge).unwrap();

    let knows = engine.get_edges_by_type("KNOWS").unwrap();
    assert_eq!(knows.len(), 1);
    assert_eq!(knows[0], edge);
    assert_eq!(
        engine.get_edges_from_node("db1:n1").unwrap(),
        vec![edge.clone()]
    );
    assert_eq!(
        engine.get_edges_to_node("db1:n2").unwrap(),
        vec![edge.clone()]
    );
    assert_eq!(
        engine
            .get_edges_from_node_by_type("db1:n1", "KNOWS")
            .unwrap(),
        vec![edge.clone()]
    );
    assert!(engine
        .get_edges_from_node_by_type("db1:n1", "MENTORS")
        .unwrap()
        .is_empty());

    edge.edge_type = "MENTORS".to_string();
    edge.properties.insert("years".to_string(), json!(5));
    engine.put_edge_record(&edge).unwrap();

    assert!(engine.get_edges_by_type("KNOWS").unwrap().is_empty());
    assert!(engine
        .get_edges_from_node_by_type("db1:n1", "KNOWS")
        .unwrap()
        .is_empty());
    let mentors = engine.get_edges_by_type("MENTORS").unwrap();
    assert_eq!(mentors.len(), 1);
    assert_eq!(mentors[0].properties.get("years"), Some(&json!(5)));
    assert_eq!(
        engine
            .get_edges_to_node_by_type("db1:n2", "MENTORS")
            .unwrap(),
        mentors
    );

    engine.delete_edge_record("db1:e1").unwrap();
    assert!(engine.get_edge_record("db1:e1").unwrap().is_none());
    assert!(engine.get_edges_by_type("MENTORS").unwrap().is_empty());
    assert!(engine.get_edges_from_node("db1:n1").unwrap().is_empty());
    assert!(engine.get_edges_to_node("db1:n2").unwrap().is_empty());
}

#[test]
fn adjacent_edge_queries_respect_direction_and_type_filters() {
    let engine = StorageEngine::open_temporary().unwrap();

    let knows_out = sample_edge("db1:e1", "KNOWS", "db1:n1", "db1:n2");
    let mentors_out = sample_edge("db1:e2", "MENTORS", "db1:n1", "db1:n3");
    let knows_in = sample_edge("db1:e3", "KNOWS", "db1:n4", "db1:n1");

    for edge in [&knows_out, &mentors_out, &knows_in] {
        engine.put_edge_record(edge).unwrap();
    }

    assert_eq!(
        engine
            .get_adjacent_edges("db1:n1", EdgeAdjacencyDirection::Outgoing, None)
            .unwrap(),
        vec![knows_out.clone(), mentors_out.clone()]
    );
    assert_eq!(
        engine
            .get_adjacent_edges("db1:n1", EdgeAdjacencyDirection::Incoming, None)
            .unwrap(),
        vec![knows_in.clone()]
    );
    assert_eq!(
        engine
            .get_adjacent_edges("db1:n1", EdgeAdjacencyDirection::Both, None)
            .unwrap(),
        vec![knows_out.clone(), mentors_out.clone(), knows_in.clone()]
    );
    assert_eq!(
        engine
            .get_adjacent_edges("db1:n1", EdgeAdjacencyDirection::Both, Some("KNOWS"))
            .unwrap(),
        vec![knows_out, knows_in]
    );
    assert!(engine
        .get_adjacent_edges("db1:n1", EdgeAdjacencyDirection::Both, Some("LIKES"))
        .unwrap()
        .is_empty());
}

#[test]
fn prefix_scan_counts_and_namespace_listing_are_deterministic() {
    let engine = StorageEngine::open_temporary().unwrap();

    engine
        .put_node_record(&sample_node("alpha:n1", &["Person"]))
        .unwrap();
    engine
        .put_node_record(&sample_node("alpha:n2", &["Person"]))
        .unwrap();
    engine
        .put_node_record(&sample_node("beta:n1", &["Robot"]))
        .unwrap();

    engine
        .put_edge_record(&sample_edge("alpha:e1", "LINKS", "alpha:n1", "alpha:n2"))
        .unwrap();
    engine
        .put_edge_record(&sample_edge("beta:e1", "LINKS", "beta:n1", "beta:n2"))
        .unwrap();

    assert_eq!(engine.node_count_by_prefix("alpha:").unwrap(), 2);
    assert_eq!(engine.node_count_by_prefix("beta:").unwrap(), 1);
    assert_eq!(engine.edge_count_by_prefix("alpha:").unwrap(), 1);
    assert_eq!(engine.edge_count_by_prefix("beta:").unwrap(), 1);
    assert_eq!(
        engine
            .node_count_by_label_in_namespace("alpha", "Person")
            .unwrap(),
        2
    );
    assert_eq!(
        engine
            .node_count_by_label_in_namespace("beta", "Robot")
            .unwrap(),
        1
    );
    assert_eq!(
        engine
            .node_count_by_label_in_namespace("beta", "Person")
            .unwrap(),
        0
    );

    let namespaces = engine.list_namespaces().unwrap();
    assert_eq!(namespaces, vec!["alpha".to_string(), "beta".to_string()]);

    engine
        .put_node_record(&sample_node("alpha:n1", &["Robot"]))
        .unwrap();
    assert_eq!(engine.node_count_by_prefix("alpha:").unwrap(), 2);
    assert_eq!(
        engine
            .node_count_by_label_in_namespace("alpha", "Person")
            .unwrap(),
        1
    );
    assert_eq!(
        engine
            .node_count_by_label_in_namespace("alpha", "Robot")
            .unwrap(),
        1
    );

    engine.delete_node_record("beta:n1").unwrap();
    engine.delete_edge_record("beta:e1").unwrap();
    assert_eq!(engine.node_count_by_prefix("beta:").unwrap(), 0);
    assert_eq!(engine.edge_count_by_prefix("beta:").unwrap(), 0);
    assert_eq!(
        engine
            .node_count_by_label_in_namespace("beta", "Robot")
            .unwrap(),
        0
    );
    assert_eq!(engine.list_namespaces().unwrap(), vec!["alpha".to_string()]);
}

#[test]
fn namespace_scoped_schema_is_isolated_from_global_catalog() {
    let engine = StorageEngine::open_temporary().unwrap();

    let global_constraint = Constraint {
        name: "global_person_email_unique".to_string(),
        constraint_type: ConstraintType::Unique,
        entity_type: ConstraintEntityType::Node,
        label: "Person".to_string(),
        properties: vec!["email".to_string()],
    };
    let alpha_constraint = Constraint {
        name: "alpha_person_name_exists".to_string(),
        constraint_type: ConstraintType::Exists,
        entity_type: ConstraintEntityType::Node,
        label: "Person".to_string(),
        properties: vec!["name".to_string()],
    };
    let alpha_index = IndexDefinition {
        name: "alpha_person_name_idx".to_string(),
        entity_type: IndexEntityType::Node,
        label: "Person".to_string(),
        properties: vec!["name".to_string()],
        kind: IndexKind::Range,
    };
    let beta_index = IndexDefinition {
        name: "beta_robot_model_idx".to_string(),
        entity_type: IndexEntityType::Node,
        label: "Robot".to_string(),
        properties: vec!["model".to_string()],
        kind: IndexKind::Range,
    };

    engine.persist_constraint(&global_constraint).unwrap();
    engine
        .persist_constraint_for_namespace("alpha", &alpha_constraint)
        .unwrap();
    engine
        .persist_index_definition_for_namespace("alpha", &alpha_index)
        .unwrap();
    engine
        .persist_index_definition_for_namespace("beta", &beta_index)
        .unwrap();

    let alpha_schema = engine.schema_for_namespace("alpha").unwrap();
    assert_eq!(alpha_schema.constraints, vec![alpha_constraint.clone()]);
    assert_eq!(alpha_schema.indexes, vec![alpha_index.clone()]);

    let beta_schema = engine.schema_for_namespace("beta").unwrap();
    assert!(beta_schema.constraints.is_empty());
    assert_eq!(beta_schema.indexes, vec![beta_index]);

    assert_eq!(engine.load_constraints().unwrap(), vec![global_constraint]);
}

#[test]
fn delete_by_prefix_removes_namespace_records_indexes_stats_and_schema() {
    let engine = StorageEngine::open_temporary().unwrap();

    engine
        .put_node_record(&sample_node("alpha:n1", &["Person"]))
        .unwrap();
    engine
        .put_node_record(&sample_node("alpha:n2", &["Person"]))
        .unwrap();
    engine
        .put_node_record(&sample_node("beta:n1", &["Person"]))
        .unwrap();
    engine
        .put_edge_record(&sample_edge("alpha:e1", "KNOWS", "alpha:n1", "alpha:n2"))
        .unwrap();
    engine
        .put_edge_record(&sample_edge("beta:e1", "KNOWS", "beta:n1", "beta:n1"))
        .unwrap();

    let alpha_constraint = Constraint {
        name: "alpha_person_name_exists".to_string(),
        constraint_type: ConstraintType::Exists,
        entity_type: ConstraintEntityType::Node,
        label: "Person".to_string(),
        properties: vec!["name".to_string()],
    };
    let alpha_index = IndexDefinition {
        name: "alpha_person_name_idx".to_string(),
        entity_type: IndexEntityType::Node,
        label: "Person".to_string(),
        properties: vec!["name".to_string()],
        kind: IndexKind::Range,
    };
    engine
        .persist_constraint_for_namespace("alpha", &alpha_constraint)
        .unwrap();
    engine
        .persist_index_definition_for_namespace("alpha", &alpha_index)
        .unwrap();

    let (nodes_deleted, edges_deleted) = engine.delete_by_prefix("alpha:").unwrap();
    assert_eq!((nodes_deleted, edges_deleted), (2, 1));

    assert!(engine.get_node_record("alpha:n1").unwrap().is_none());
    assert!(engine.get_edge_record("alpha:e1").unwrap().is_none());
    assert!(engine.get_node_record("beta:n1").unwrap().is_some());
    assert!(engine.get_edge_record("beta:e1").unwrap().is_some());

    assert_eq!(
        engine
            .get_nodes_by_label("Person")
            .unwrap()
            .into_iter()
            .map(|node| node.id)
            .collect::<Vec<_>>(),
        vec!["beta:n1".to_string()]
    );
    assert_eq!(
        engine
            .get_edges_by_type("KNOWS")
            .unwrap()
            .into_iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        vec!["beta:e1".to_string()]
    );
    assert_eq!(engine.node_count_by_prefix("alpha:").unwrap(), 0);
    assert_eq!(engine.edge_count_by_prefix("alpha:").unwrap(), 0);
    assert_eq!(
        engine
            .node_count_by_label_in_namespace("alpha", "Person")
            .unwrap(),
        0
    );
    assert_eq!(
        engine.schema_for_namespace("alpha").unwrap(),
        NamespaceSchema::default()
    );
    assert_eq!(engine.list_namespaces().unwrap(), vec!["beta".to_string()]);
}

#[test]
fn delete_by_prefix_rejects_empty_prefix_and_reports_missing_prefix() {
    let engine = StorageEngine::open_temporary().unwrap();

    let err = engine.delete_by_prefix("").unwrap_err();
    assert!(matches!(err, StorageError::EmptyPrefix));
    assert_eq!(err.to_string(), "prefix cannot be empty");

    engine
        .put_node_record(&sample_node("alpha:n1", &["Person"]))
        .unwrap();

    assert_eq!(engine.delete_by_prefix("missing:").unwrap(), (0, 0));
    assert!(engine.get_node_record("alpha:n1").unwrap().is_some());
}

#[test]
fn streaming_apis_visit_nodes_edges_prefixes_and_chunks_in_order() {
    let engine = StorageEngine::open_temporary().unwrap();

    for node_id in ["alpha:n1", "alpha:n2", "beta:n1"] {
        engine
            .put_node_record(&sample_node(node_id, &["Person"]))
            .unwrap();
    }
    for edge in [
        sample_edge("alpha:e1", "KNOWS", "alpha:n1", "alpha:n2"),
        sample_edge("beta:e1", "KNOWS", "beta:n1", "beta:n1"),
    ] {
        engine.put_edge_record(&edge).unwrap();
    }

    let mut node_ids = Vec::new();
    let streamed = engine
        .stream_node_records(|node| {
            node_ids.push(node.id);
            Ok(())
        })
        .unwrap();
    assert_eq!(streamed, 3);
    assert_eq!(node_ids, vec!["alpha:n1", "alpha:n2", "beta:n1"]);

    let mut alpha_node_ids = Vec::new();
    let streamed = engine
        .stream_node_records_by_prefix("alpha:", |node| {
            alpha_node_ids.push(node.id);
            Ok(())
        })
        .unwrap();
    assert_eq!(streamed, 2);
    assert_eq!(alpha_node_ids, vec!["alpha:n1", "alpha:n2"]);

    let mut edge_ids = Vec::new();
    let streamed = engine
        .stream_edge_records(|edge| {
            edge_ids.push(edge.id);
            Ok(())
        })
        .unwrap();
    assert_eq!(streamed, 2);
    assert_eq!(edge_ids, vec!["alpha:e1", "beta:e1"]);

    let mut chunks = Vec::new();
    let streamed = engine
        .stream_node_record_chunks(2, |chunk| {
            chunks.push(chunk.iter().map(|node| node.id.clone()).collect::<Vec<_>>());
            Ok(())
        })
        .unwrap();
    assert_eq!(streamed, 3);
    assert_eq!(
        chunks,
        vec![
            vec!["alpha:n1".to_string(), "alpha:n2".to_string()],
            vec!["beta:n1".to_string()],
        ]
    );
}

#[test]
fn streaming_apis_propagate_callback_errors_and_reject_zero_chunk_size() {
    let engine = StorageEngine::open_temporary().unwrap();
    engine
        .put_node_record(&sample_node("alpha:n1", &["Person"]))
        .unwrap();
    engine
        .put_edge_record(&sample_edge("alpha:e1", "KNOWS", "alpha:n1", "alpha:n1"))
        .unwrap();

    let err = engine
        .stream_node_records(|_| Err(StorageError::NotFound("node callback".to_string())))
        .unwrap_err();
    assert!(matches!(err, StorageError::NotFound(message) if message == "node callback"));

    let err = engine
        .stream_edge_records(|_| Err(StorageError::NotFound("edge callback".to_string())))
        .unwrap_err();
    assert!(matches!(err, StorageError::NotFound(message) if message == "edge callback"));

    let err = engine.stream_node_record_chunks(0, |_| Ok(())).unwrap_err();
    assert!(matches!(err, StorageError::InvalidChunkSize(0)));
    assert_eq!(err.to_string(), "invalid chunk size: 0");
}

#[test]
fn streaming_apis_treat_iteration_stopped_as_normal_completion() {
    let engine = StorageEngine::open_temporary().unwrap();
    for node in [
        sample_node("alpha:n1", &["Person"]),
        sample_node("alpha:n2", &["Person"]),
        sample_node("beta:n1", &["Person"]),
    ] {
        engine.put_node_record(&node).unwrap();
    }
    for edge in [
        sample_edge("alpha:e1", "KNOWS", "alpha:n1", "alpha:n2"),
        sample_edge("beta:e1", "KNOWS", "beta:n1", "beta:n1"),
    ] {
        engine.put_edge_record(&edge).unwrap();
    }

    let mut node_ids = Vec::new();
    let streamed = engine
        .stream_node_records(|node| {
            node_ids.push(node.id);
            if node_ids.len() == 2 {
                return Err(StorageError::IterationStopped);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(streamed, 2);
    assert_eq!(node_ids, vec!["alpha:n1", "alpha:n2"]);

    let mut alpha_ids = Vec::new();
    let streamed = engine
        .stream_node_records_by_prefix("alpha:", |node| {
            alpha_ids.push(node.id);
            Err(StorageError::IterationStopped)
        })
        .unwrap();
    assert_eq!(streamed, 1);
    assert_eq!(alpha_ids, vec!["alpha:n1"]);

    let mut edge_ids = Vec::new();
    let streamed = engine
        .stream_edge_records(|edge| {
            edge_ids.push(edge.id);
            Err(StorageError::IterationStopped)
        })
        .unwrap();
    assert_eq!(streamed, 1);
    assert_eq!(edge_ids, vec!["alpha:e1"]);

    let mut chunks = Vec::new();
    let streamed = engine
        .stream_node_record_chunks(2, |chunk| {
            chunks.push(chunk.iter().map(|node| node.id.clone()).collect::<Vec<_>>());
            Err(StorageError::IterationStopped)
        })
        .unwrap();
    assert_eq!(streamed, 2);
    assert_eq!(
        chunks,
        vec![vec!["alpha:n1".to_string(), "alpha:n2".to_string()]]
    );
}

#[test]
fn streaming_apis_surface_external_cancellation() {
    let engine = StorageEngine::open_temporary().unwrap();
    for node in [
        sample_node("alpha:n1", &["Person"]),
        sample_node("alpha:n2", &["Person"]),
        sample_node("beta:n1", &["Person"]),
    ] {
        engine.put_node_record(&node).unwrap();
    }
    for edge in [
        sample_edge("alpha:e1", "KNOWS", "alpha:n1", "alpha:n2"),
        sample_edge("beta:e1", "KNOWS", "beta:n1", "beta:n1"),
    ] {
        engine.put_edge_record(&edge).unwrap();
    }

    let cancel = RequestCancellation::new();
    let mut node_ids = Vec::new();
    let err = engine
        .stream_node_records_with_cancellation(&cancel, |node| {
            node_ids.push(node.id);
            cancel.cancel();
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(err, StorageError::RequestCancelled(_)));
    assert_eq!(node_ids, vec!["alpha:n1"]);

    let cancel = RequestCancellation::new();
    let mut alpha_ids = Vec::new();
    let err = engine
        .stream_node_records_by_prefix_with_cancellation("alpha:", &cancel, |node| {
            alpha_ids.push(node.id);
            cancel.cancel();
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(err, StorageError::RequestCancelled(_)));
    assert_eq!(alpha_ids, vec!["alpha:n1"]);

    let cancel = RequestCancellation::new();
    let mut edge_ids = Vec::new();
    let err = engine
        .stream_edge_records_with_cancellation(&cancel, |edge| {
            edge_ids.push(edge.id);
            cancel.cancel();
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(err, StorageError::RequestCancelled(_)));
    assert_eq!(edge_ids, vec!["alpha:e1"]);

    let cancel = RequestCancellation::new();
    let mut chunks = Vec::new();
    let err = engine
        .stream_node_record_chunks_with_cancellation(2, &cancel, |chunk| {
            chunks.push(chunk.iter().map(|node| node.id.clone()).collect::<Vec<_>>());
            cancel.cancel();
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(err, StorageError::RequestCancelled(_)));
    assert_eq!(
        chunks,
        vec![vec!["alpha:n1".to_string(), "alpha:n2".to_string()]]
    );
}

#[test]
fn namespaced_storage_engine_isolates_crud_queries_and_counts() {
    let engine = StorageEngine::open_temporary().unwrap();
    let tenant_a = engine.for_namespace("tenant_a");
    let tenant_b = engine.for_namespace("tenant_b");

    let mut tenant_a_n1 = sample_node("n1", &["Person"]);
    tenant_a_n1
        .properties
        .insert("tenant".to_string(), json!("a"));
    let mut tenant_a_n2 = sample_node("n2", &["Person"]);
    tenant_a_n2
        .properties
        .insert("tenant".to_string(), json!("a"));
    let mut tenant_b_n1 = sample_node("n1", &["Person"]);
    tenant_b_n1
        .properties
        .insert("tenant".to_string(), json!("b"));

    tenant_a.put_node_record(&tenant_a_n1).unwrap();
    tenant_a.put_node_record(&tenant_a_n2).unwrap();
    tenant_b.put_node_record(&tenant_b_n1).unwrap();

    let mut tenant_a_edge = sample_edge("e1", "KNOWS", "n1", "n2");
    tenant_a_edge
        .properties
        .insert("tenant".to_string(), json!("a"));
    tenant_a.put_edge_record(&tenant_a_edge).unwrap();

    assert!(engine.get_node_record("tenant_a:n1").unwrap().is_some());
    assert!(engine.get_node_record("tenant_b:n1").unwrap().is_some());
    assert_eq!(tenant_a.get_node_record("n1").unwrap().unwrap().id, "n1");
    assert_eq!(tenant_b.get_node_record("n1").unwrap().unwrap().id, "n1");
    assert!(tenant_a.get_edge_record("e1").unwrap().is_some());

    assert_eq!(tenant_a.node_count().unwrap(), 2);
    assert_eq!(tenant_b.node_count().unwrap(), 1);
    assert_eq!(tenant_a.edge_count().unwrap(), 1);
    assert_eq!(tenant_b.edge_count().unwrap(), 0);
    assert_eq!(tenant_a.node_count_by_label("Person").unwrap(), 2);
    assert_eq!(tenant_b.node_count_by_label("Person").unwrap(), 1);

    assert_eq!(
        tenant_a
            .get_nodes_by_label("Person")
            .unwrap()
            .into_iter()
            .map(|node| node.id)
            .collect::<Vec<_>>(),
        vec!["n1".to_string(), "n2".to_string()]
    );
    assert_eq!(
        tenant_b
            .get_nodes_by_label("Person")
            .unwrap()
            .into_iter()
            .map(|node| node.id)
            .collect::<Vec<_>>(),
        vec!["n1".to_string()]
    );
    assert_eq!(
        tenant_a
            .get_edges_from_node("n1")
            .unwrap()
            .into_iter()
            .map(|edge| (edge.id, edge.start_node, edge.end_node))
            .collect::<Vec<_>>(),
        vec![("e1".to_string(), "n1".to_string(), "n2".to_string())]
    );
    assert_eq!(
        tenant_a
            .get_adjacent_edges("n2", EdgeAdjacencyDirection::Incoming, Some("KNOWS"))
            .unwrap()
            .into_iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        vec!["e1".to_string()]
    );
    assert!(tenant_b.get_edges_by_type("KNOWS").unwrap().is_empty());
}

#[test]
fn namespaced_storage_engine_streams_schema_and_deletes_within_namespace() {
    let engine = StorageEngine::open_temporary().unwrap();
    let tenant_a = engine.for_namespace("tenant_a");
    let tenant_b = engine.for_namespace("tenant_b");

    tenant_a
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();
    tenant_a
        .put_node_record(&sample_node("n2", &["Person"]))
        .unwrap();
    tenant_b
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();
    tenant_a
        .put_edge_record(&sample_edge("e1", "KNOWS", "n1", "n2"))
        .unwrap();
    tenant_b
        .put_edge_record(&sample_edge("e1", "KNOWS", "n1", "n1"))
        .unwrap();

    let tenant_a_constraint = Constraint {
        name: "tenant_a_person_name_exists".to_string(),
        constraint_type: ConstraintType::Exists,
        entity_type: ConstraintEntityType::Node,
        label: "Person".to_string(),
        properties: vec!["name".to_string()],
    };
    let tenant_a_index = IndexDefinition {
        name: "tenant_a_person_name_idx".to_string(),
        entity_type: IndexEntityType::Node,
        label: "Person".to_string(),
        properties: vec!["name".to_string()],
        kind: IndexKind::Range,
    };
    engine
        .persist_constraint_for_namespace("tenant_a", &tenant_a_constraint)
        .unwrap();
    engine
        .persist_index_definition_for_namespace("tenant_a", &tenant_a_index)
        .unwrap();

    assert_eq!(
        tenant_a.schema().unwrap(),
        NamespaceSchema {
            constraints: vec![tenant_a_constraint],
            indexes: vec![tenant_a_index],
        }
    );

    let mut streamed_nodes = Vec::new();
    let streamed = tenant_a
        .stream_node_records(|node| {
            streamed_nodes.push(node.id);
            Ok(())
        })
        .unwrap();
    assert_eq!(streamed, 2);
    assert_eq!(streamed_nodes, vec!["n1", "n2"]);

    let mut streamed_edges = Vec::new();
    let streamed = tenant_a
        .stream_edge_records(|edge| {
            streamed_edges.push((edge.id, edge.start_node, edge.end_node));
            Ok(())
        })
        .unwrap();
    assert_eq!(streamed, 1);
    assert_eq!(
        streamed_edges,
        vec![("e1".to_string(), "n1".to_string(), "n2".to_string())]
    );

    assert_eq!(tenant_a.all_nodes().unwrap().len(), 2);
    assert_eq!(tenant_b.all_nodes().unwrap().len(), 1);

    assert_eq!(tenant_a.delete_all().unwrap(), (2, 1));
    assert!(tenant_a.all_nodes().unwrap().is_empty());
    assert!(tenant_a.all_edges().unwrap().is_empty());
    assert_eq!(tenant_a.schema().unwrap(), NamespaceSchema::default());
    assert_eq!(tenant_b.all_nodes().unwrap().len(), 1);
    assert_eq!(tenant_b.all_edges().unwrap().len(), 1);
}

#[test]
fn namespaced_storage_engine_scopes_pending_embedding_helpers() {
    let engine = StorageEngine::open_temporary().unwrap();
    let tenant_a = engine.for_namespace("tenant_a");
    let tenant_b = engine.for_namespace("tenant_b");

    let mut tenant_a_n1 = sample_node("n1", &["File"]);
    tenant_a_n1
        .properties
        .insert("content".to_string(), json!("tenant a file"));
    let mut tenant_b_n1 = sample_node("n1", &["File"]);
    tenant_b_n1
        .properties
        .insert("content".to_string(), json!("tenant b file"));

    tenant_a.put_node_record(&tenant_a_n1).unwrap();
    tenant_b.put_node_record(&tenant_b_n1).unwrap();

    assert_eq!(tenant_a.pending_embeddings_count().unwrap(), 1);
    assert_eq!(tenant_b.pending_embeddings_count().unwrap(), 1);
    assert_eq!(
        tenant_a.find_node_needing_embedding().unwrap().unwrap().id,
        "n1"
    );
    assert_eq!(
        tenant_b.find_node_needing_embedding().unwrap().unwrap().id,
        "n1"
    );

    tenant_a.mark_node_embedded("n1").unwrap();
    assert_eq!(tenant_a.pending_embeddings_count().unwrap(), 0);
    assert_eq!(tenant_b.pending_embeddings_count().unwrap(), 1);
    assert!(tenant_a.find_node_needing_embedding().unwrap().is_none());

    tenant_a.add_to_pending_embeddings("n1").unwrap();
    assert_eq!(tenant_a.pending_embeddings_count().unwrap(), 1);

    engine.mark_node_embedded("tenant_a:n1").unwrap();
    engine.mark_node_embedded("tenant_b:n1").unwrap();
    assert_eq!(tenant_a.pending_embeddings_count().unwrap(), 0);
    assert_eq!(tenant_b.pending_embeddings_count().unwrap(), 0);

    assert_eq!(tenant_a.refresh_pending_embeddings_index().unwrap(), 1);
    assert_eq!(tenant_b.refresh_pending_embeddings_index().unwrap(), 1);
    assert_eq!(tenant_a.pending_embeddings_count().unwrap(), 1);
    assert_eq!(tenant_b.pending_embeddings_count().unwrap(), 1);
}

#[test]
fn namespaced_storage_engine_clear_all_embeddings_only_requeues_that_namespace() {
    let engine = StorageEngine::open_temporary().unwrap();
    let tenant_a = engine.for_namespace("tenant_a");
    let tenant_b = engine.for_namespace("tenant_b");

    let mut tenant_a_n1 = sample_node("n1", &["File"]);
    tenant_a_n1
        .properties
        .insert("content".to_string(), json!("tenant a file"));
    tenant_a_n1.set_managed_chunk_embeddings(
        vec![vec![0.1, 0.2, 0.3]],
        Some("test-model".to_string()),
        Some("2026-05-29T00:00:00Z".to_string()),
    );
    let mut tenant_b_n1 = sample_node("n1", &["File"]);
    tenant_b_n1
        .properties
        .insert("content".to_string(), json!("tenant b file"));
    tenant_b_n1.set_managed_chunk_embeddings(
        vec![vec![0.4, 0.5, 0.6]],
        Some("test-model".to_string()),
        Some("2026-05-29T00:00:00Z".to_string()),
    );
    tenant_b_n1
        .named_embeddings
        .insert("qdrant".to_string(), vec![9.0, 8.0, 7.0]);

    tenant_a.put_node_record(&tenant_a_n1).unwrap();
    tenant_b.put_node_record(&tenant_b_n1).unwrap();

    let cleared = tenant_a.clear_all_embeddings().unwrap();
    assert_eq!(cleared, 1);
    assert_eq!(tenant_a.pending_embeddings_count().unwrap(), 1);
    assert_eq!(tenant_b.pending_embeddings_count().unwrap(), 0);

    let tenant_a_after = tenant_a.get_node_record("n1").unwrap().unwrap();
    assert!(tenant_a_after.chunk_embeddings.is_empty());
    assert!(tenant_a_after.embed_meta.has_embedding.is_none());
    let tenant_b_after = tenant_b.get_node_record("n1").unwrap().unwrap();
    assert_eq!(tenant_b_after.chunk_embeddings, vec![vec![0.4, 0.5, 0.6]]);
    assert_eq!(tenant_b_after.embed_meta.has_embedding, Some(true));
    assert_eq!(
        tenant_b_after.named_embeddings.get("qdrant"),
        Some(&vec![9.0, 8.0, 7.0])
    );
}

#[test]
fn namespaced_storage_engine_streaming_surfaces_external_cancellation() {
    let engine = StorageEngine::open_temporary().unwrap();
    let tenant = engine.for_namespace("tenant_a");

    tenant
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();
    tenant
        .put_node_record(&sample_node("n2", &["Person"]))
        .unwrap();
    tenant
        .put_node_record(&sample_node("n3", &["Person"]))
        .unwrap();
    tenant
        .put_edge_record(&sample_edge("e1", "KNOWS", "n1", "n2"))
        .unwrap();
    tenant
        .put_edge_record(&sample_edge("e2", "KNOWS", "n2", "n3"))
        .unwrap();

    let cancel = RequestCancellation::new();
    let mut node_ids = Vec::new();
    let err = tenant
        .stream_node_records_with_cancellation(&cancel, |node| {
            node_ids.push(node.id);
            cancel.cancel();
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(err, StorageError::RequestCancelled(_)));
    assert_eq!(node_ids, vec!["n1"]);

    let cancel = RequestCancellation::new();
    let mut edge_ids = Vec::new();
    let err = tenant
        .stream_edge_records_with_cancellation(&cancel, |edge| {
            edge_ids.push(edge.id);
            cancel.cancel();
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(err, StorageError::RequestCancelled(_)));
    assert_eq!(edge_ids, vec!["e1"]);
}

#[test]
fn storage_engine_exposes_mvcc_visible_reads_and_lifecycle_controls() {
    let engine = StorageEngine::open_temporary().unwrap();

    let mut node = sample_node("n1", &["Person"]);
    node.properties.insert("state".to_string(), json!("v1"));
    engine.put_node_record(&node).unwrap();
    engine
        .put_edge_record(&sample_edge("e1", "KNOWS", "n1", "n1"))
        .unwrap();
    let snapshot = engine.begin_mvcc_snapshot();

    let mut updated = sample_node("n1", &["Device"]);
    updated.properties.insert("state".to_string(), json!("v2"));
    engine.put_node_record(&updated).unwrap();
    engine
        .put_edge_record(&sample_edge("e1", "SEES", "n1", "n1"))
        .unwrap();

    assert!(engine.get_nodes_by_label("Person").unwrap().is_empty());
    assert!(engine.get_edges_by_type("KNOWS").unwrap().is_empty());
    assert_eq!(
        engine
            .get_nodes_by_label_visible_at(&snapshot, "Person")
            .unwrap()
            .into_iter()
            .map(|node| node.id)
            .collect::<Vec<_>>(),
        vec!["n1".to_string()]
    );
    assert_eq!(
        engine
            .get_edges_by_type_visible_at(&snapshot, "KNOWS")
            .unwrap()
            .into_iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        vec!["e1".to_string()]
    );

    engine.pause_lifecycle();
    assert!(engine.lifecycle_status().paused);
    engine.set_lifecycle_schedule_ms(7_500);
    assert_eq!(engine.lifecycle_status().schedule_interval_ms, 7_500);
    assert!(!engine.top_lifecycle_debt_keys(4).is_empty());
    engine.resume_lifecycle();
    assert!(!engine.lifecycle_status().paused);
}

#[test]
fn namespaced_storage_engine_delegates_mvcc_visible_reads_and_lifecycle_controls() {
    let engine = StorageEngine::open_temporary().unwrap();
    let tenant_a = engine.for_namespace("tenant_a");
    let tenant_b = engine.for_namespace("tenant_b");

    tenant_a
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();
    tenant_b
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();
    tenant_a
        .put_edge_record(&sample_edge("e1", "KNOWS", "n1", "n1"))
        .unwrap();
    let snapshot = tenant_a.begin_mvcc_snapshot();

    tenant_a
        .put_node_record(&sample_node("n1", &["Device"]))
        .unwrap();
    tenant_a
        .put_edge_record(&sample_edge("e1", "SEES", "n1", "n1"))
        .unwrap();

    assert_eq!(
        tenant_a
            .get_nodes_by_label_visible_at(&snapshot, "Person")
            .unwrap()
            .into_iter()
            .map(|node| node.id)
            .collect::<Vec<_>>(),
        vec!["n1".to_string()]
    );
    assert!(tenant_b
        .get_edges_by_type_visible_at(&snapshot, "KNOWS")
        .unwrap()
        .is_empty());

    tenant_a.pause_lifecycle();
    assert!(tenant_a.lifecycle_status().paused);
    tenant_a.set_lifecycle_schedule_ms(12_000);
    assert_eq!(tenant_a.lifecycle_status().schedule_interval_ms, 12_000);
    let debt = tenant_a.top_lifecycle_debt_keys(8);
    assert!(debt
        .iter()
        .all(|entry| !entry.logical_key.contains("tenant_a:")));
    let pruned = tenant_a.prune_mvcc_versions(MvccPruneOptions {
        max_versions_per_key: Some(1),
    });
    assert!(pruned > 0);
    tenant_a.resume_lifecycle();
    assert!(!tenant_a.lifecycle_status().paused);
}

#[test]
fn storage_engine_rebuild_mvcc_repairs_raw_storage_drift_and_blocks_active_readers() {
    let engine = StorageEngine::open_temporary().unwrap();
    engine
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();

    let snapshot = engine.begin_mvcc_snapshot();
    assert!(engine.rebuild_mvcc_from_current_state().err().is_none());
    assert_eq!(
        engine
            .get_nodes_by_label_visible_at(&snapshot, "Person")
            .unwrap()
            .len(),
        1
    );

    let lease = engine.begin_registered_mvcc_snapshot();
    let err = engine.rebuild_mvcc_from_current_state().err().unwrap();
    assert!(matches!(
        err,
        StorageError::MvccRebuildBlocked { active_readers: 1 }
    ));
    drop(lease);

    engine.delete_node("n1").unwrap();
    assert!(engine.get_node_record("n1").unwrap().is_none());

    let stale_snapshot = engine.begin_mvcc_snapshot();
    assert!(engine
        .get_node_record_visible_at(&stale_snapshot, "n1")
        .unwrap()
        .is_some());
    assert_eq!(
        engine
            .get_nodes_by_label_visible_at(&stale_snapshot, "Person")
            .unwrap()
            .into_iter()
            .map(|node| node.id)
            .collect::<Vec<_>>(),
        vec!["n1".to_string()]
    );

    engine.rebuild_mvcc_from_current_state().unwrap();
    let repaired_snapshot = engine.begin_mvcc_snapshot();
    assert!(engine
        .get_node_record_visible_at(&repaired_snapshot, "n1")
        .unwrap()
        .is_none());
    assert!(engine
        .get_nodes_by_label_visible_at(&repaired_snapshot, "Person")
        .unwrap()
        .is_empty());
}

#[test]
fn topology_metadata_round_trip_builds_valid_registry() {
    use copperdb_topology::{
        DistributedWriteMode, HyperscalerProfile as TopologyHyperscalerProfile, MeshPeer,
        NodeCapability, PlacementKey, PlacementRecord, SearchRoutingPolicy,
    };

    let engine = StorageEngine::open_temporary().unwrap();
    engine
        .register_topology_hyperscaler_profile(&TopologyHyperscalerProfile::local("local-prod"))
        .unwrap();
    engine
        .register_topology_peer(
            &MeshPeer::new("node-a", "node-a.mesh.local:9000")
                .with_capability(NodeCapability::Search)
                .with_capability(NodeCapability::WriteLeader)
                .with_hyperscaler_profile("local-prod")
                .with_region_zone("us-east-1", "us-east-1a")
                .with_observed_rtt_micros(1_000),
        )
        .unwrap();
    engine
        .register_topology_peer(
            &MeshPeer::new("node-b", "node-b.mesh.local:9000")
                .with_capability(NodeCapability::Search)
                .with_capability(NodeCapability::WriteReplica)
                .with_hyperscaler_profile("local-prod")
                .with_region_zone("us-east-1", "us-east-1b")
                .with_observed_rtt_micros(2_000),
        )
        .unwrap();
    engine
        .register_topology_placement(&PlacementRecord {
            key: PlacementKey::default_for_database("copper"),
            primary_node: "node-a".into(),
            replica_nodes: vec!["node-b".into()],
            search_nodes: vec!["node-a".into(), "node-b".into()],
            hyperscaler_profile: Some("local-prod".into()),
            min_write_replicas: 1,
            search_fanout: 2,
        })
        .unwrap();

    let registry = engine.load_topology_registry().unwrap();
    let placement = PlacementKey::default_for_database("copper");
    let search_plan = registry
        .plan_search_with_policy(&placement, SearchRoutingPolicy::low_latency("us-east-1", 2))
        .unwrap();
    let write_plan = registry
        .plan_write(&placement, DistributedWriteMode::LeaderLease)
        .unwrap();

    assert_eq!(search_plan.fanout.len(), 2);
    assert_eq!(search_plan.fanout[0].node_id, "node-a");
    assert_eq!(write_plan.required_acks, 2);
}

#[test]
fn fabric_database_metadata_round_trip_lists_shard_map() {
    use copperdb_topology::{
        FabricDatabase, FabricPartitionPolicy, FabricShard, FabricShardKind, PlacementKey,
    };

    let engine = StorageEngine::open_temporary().unwrap();
    let fabric = FabricDatabase {
        tenant: "default".into(),
        database: "copper".into(),
        default_shard: "primary".into(),
        partition_policy: FabricPartitionPolicy::HashByKey { buckets: 2 },
        shards: vec![
            FabricShard::mixed(PlacementKey::default_for_database("copper")),
            FabricShard {
                placement: PlacementKey::new("default", "copper", "person-00"),
                kind: FabricShardKind::Graph,
                labels: vec!["Person".into()],
                relationship_types: vec!["KNOWS".into()],
                collections: vec![],
            },
        ],
    };

    engine.register_fabric_database(&fabric).unwrap();
    let databases = engine.list_fabric_databases().unwrap();

    assert_eq!(databases, vec![fabric]);
    assert_eq!(databases[0].placement_keys().len(), 2);
}

#[test]
fn index_and_prefix_scan_apis_work() {
    let engine = StorageEngine::open_temporary().unwrap();
    engine.put_index(b"idx:key", b"idx:value").unwrap();
    assert_eq!(
        engine.get_index(b"idx:key").unwrap(),
        Some(b"idx:value".to_vec())
    );

    engine.put_node("Person:1", b"alice").unwrap();
    engine.put_node("Person:2", b"bob").unwrap();
    engine.put_node("Movie:1", b"matrix").unwrap();

    let rows: Vec<_> = engine.scan_nodes_with_prefix("Person:").collect();
    assert_eq!(rows.len(), 2);
    let mut keys = rows
        .into_iter()
        .map(|r| String::from_utf8(r.unwrap().0.to_vec()).unwrap())
        .collect::<Vec<_>>();
    keys.sort();
    assert_eq!(keys, vec!["Person:1", "Person:2"]);
}

#[test]
fn flush_guard_and_size_api_are_stable() {
    let engine = StorageEngine::open_temporary().unwrap();
    {
        let _guard = engine.hold_flush();
        engine.put_node("n1", b"v1").unwrap();
    }
    engine.flush().unwrap();
    assert!(engine.size_on_disk() > 0);
}

#[test]
fn async_storage_engine_buffers_structured_node_writes_until_flush() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    async_engine
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();

    assert!(async_engine
        .get_persisted_node_record("n1")
        .unwrap()
        .is_none());
    assert_eq!(
        async_engine
            .get_node_record_latest_effective("n1")
            .unwrap()
            .unwrap()
            .labels,
        vec!["Person".to_string()]
    );

    let flushed = async_engine.flush().unwrap();
    assert_eq!(flushed.nodes_written, 1);
    assert!(async_engine
        .get_persisted_node_record("n1")
        .unwrap()
        .is_some());
    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_background_flush_persists_pending_writes() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 10,
            ..Default::default()
        }),
    );

    async_engine
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();

    wait_until(
        || {
            async_engine
                .get_persisted_node_record("n1")
                .unwrap()
                .is_some()
        },
        Duration::from_secs(1),
    );

    assert_eq!(
        async_engine
            .get_persisted_nodes_by_label("Person")
            .unwrap()
            .len(),
        1
    );
    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_adaptive_flush_uses_min_interval_instead_of_flush_interval() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            adaptive_flush: true,
            min_flush_interval_ms: 10,
            max_flush_interval_ms: 10,
            target_flush_size: 1,
            ..Default::default()
        }),
    );

    async_engine
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();

    wait_until(
        || {
            async_engine
                .get_persisted_node_record("n1")
                .unwrap()
                .is_some()
        },
        Duration::from_secs(1),
    );

    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_find_node_needing_embedding_skips_pending_embedding_update() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    let mut first = sample_node("n1", &["File"]);
    first
        .properties
        .insert("content".to_string(), json!("first file"));
    let mut second = sample_node("n2", &["File"]);
    second
        .properties
        .insert("content".to_string(), json!("second file"));

    async_engine.put_node_record(&first).unwrap();
    async_engine.put_node_record(&second).unwrap();
    async_engine.flush().unwrap();

    assert_eq!(
        async_engine
            .find_node_needing_embedding()
            .unwrap()
            .unwrap()
            .id,
        "n1"
    );

    first.set_default_embedding(vec![0.1, 0.2, 0.3]);
    first.embed_meta.has_embedding = Some(true);
    first.updated_at_unix_ms += 1;
    async_engine.put_node_record(&first).unwrap();

    assert_eq!(
        async_engine
            .find_node_needing_embedding()
            .unwrap()
            .unwrap()
            .id,
        "n2"
    );

    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_find_node_needing_embedding_skips_pending_delete() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    let mut first = sample_node("n1", &["File"]);
    first
        .properties
        .insert("content".to_string(), json!("first file"));
    let mut second = sample_node("n2", &["File"]);
    second
        .properties
        .insert("content".to_string(), json!("second file"));

    async_engine.put_node_record(&first).unwrap();
    async_engine.put_node_record(&second).unwrap();
    async_engine.flush().unwrap();

    async_engine.delete_node_record("n1").unwrap();

    assert_eq!(
        async_engine
            .find_node_needing_embedding()
            .unwrap()
            .unwrap()
            .id,
        "n2"
    );

    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_pending_embeddings_queue_tracks_mark_and_requeue() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    let mut node = sample_node("n1", &["File"]);
    node.properties
        .insert("content".to_string(), json!("needs embedding"));
    async_engine.put_node_record(&node).unwrap();

    assert_eq!(async_engine.pending_embeddings_count(), 1);
    assert_eq!(
        async_engine
            .find_node_needing_embedding()
            .unwrap()
            .unwrap()
            .id,
        "n1"
    );

    async_engine.mark_node_embedded("n1");
    assert_eq!(async_engine.pending_embeddings_count(), 0);
    assert!(async_engine
        .find_node_needing_embedding()
        .unwrap()
        .is_none());

    async_engine.add_to_pending_embeddings("n1").unwrap();
    assert_eq!(async_engine.pending_embeddings_count(), 1);
    assert_eq!(
        async_engine
            .find_node_needing_embedding()
            .unwrap()
            .unwrap()
            .id,
        "n1"
    );

    node.set_default_embedding(vec![0.1, 0.2, 0.3]);
    node.embed_meta.has_embedding = Some(true);
    node.updated_at_unix_ms += 1;
    async_engine.put_node_record(&node).unwrap();

    assert_eq!(async_engine.pending_embeddings_count(), 0);
    async_engine.add_to_pending_embeddings("n1").unwrap();
    assert_eq!(async_engine.pending_embeddings_count(), 0);

    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_pending_embeddings_refresh_rebuilds_from_current_state() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    let mut first = sample_node("n1", &["File"]);
    first
        .properties
        .insert("content".to_string(), json!("needs embedding"));
    let mut second = sample_node("n2", &["File"]);
    second
        .properties
        .insert("embedding_skipped".to_string(), json!("no content"));
    let mut third = sample_node("n3", &["File"]);
    third.set_default_embedding(vec![0.1, 0.2]);
    third.embed_meta.has_embedding = Some(true);

    async_engine.put_node_record(&first).unwrap();
    async_engine.put_node_record(&second).unwrap();
    async_engine.put_node_record(&third).unwrap();

    async_engine.mark_node_embedded("n1");
    assert_eq!(async_engine.pending_embeddings_count(), 0);

    assert_eq!(async_engine.refresh_pending_embeddings_index().unwrap(), 1);
    assert_eq!(
        async_engine
            .find_node_needing_embedding()
            .unwrap()
            .unwrap()
            .id,
        "n1"
    );

    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_initializes_pending_embeddings_from_persisted_nodes() {
    let engine = StorageEngine::open_temporary().unwrap();
    let mut node = sample_node("n1", &["File"]);
    node.properties
        .insert("content".to_string(), json!("needs embedding"));
    engine.put_node_record(&node).unwrap();
    engine.flush().unwrap();

    let async_engine = AsyncStorageEngine::new(
        engine,
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    assert_eq!(async_engine.pending_embeddings_count(), 1);
    assert_eq!(
        async_engine
            .find_node_needing_embedding()
            .unwrap()
            .unwrap()
            .id,
        "n1"
    );

    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_add_to_pending_embeddings_skips_pending_delete() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    let mut node = sample_node("n1", &["File"]);
    node.properties
        .insert("content".to_string(), json!("needs embedding"));
    async_engine.put_node_record(&node).unwrap();
    async_engine.flush().unwrap();
    async_engine.mark_node_embedded("n1");

    async_engine.delete_node_record("n1").unwrap();
    async_engine.add_to_pending_embeddings("n1").unwrap();

    assert_eq!(async_engine.pending_embeddings_count(), 0);
    assert!(async_engine
        .find_node_needing_embedding()
        .unwrap()
        .is_none());

    async_engine.close().unwrap();
}

#[test]
fn node_record_needs_embedding_ignores_has_embedding_without_vector_and_skips_underscore_labels() {
    let mut needs = sample_node("n1", &["File"]);
    needs.embed_meta.has_embedding = Some(true);
    assert!(needs.needs_embedding());

    let mut skipped = sample_node("n2", &["_Internal"]);
    skipped
        .properties
        .insert("content".to_string(), json!("internal"));
    assert!(!skipped.needs_embedding());

    let mut embedded = sample_node("n3", &["File"]);
    embedded.set_default_embedding(vec![0.1, 0.2]);
    assert!(!embedded.needs_embedding());
}

#[test]
fn storage_engine_pending_embeddings_index_tracks_create_mark_refresh_and_delete() {
    let engine = StorageEngine::open_temporary().unwrap();

    let mut first = sample_node("n1", &["File"]);
    first
        .properties
        .insert("content".to_string(), json!("first file"));
    let mut second = sample_node("n2", &["File"]);
    second
        .properties
        .insert("content".to_string(), json!("second file"));
    second.set_default_embedding(vec![0.1, 0.2]);

    engine.put_node_record(&first).unwrap();
    engine.put_node_record(&second).unwrap();

    assert_eq!(engine.pending_embeddings_count().unwrap(), 1);
    assert_eq!(
        engine.find_node_needing_embedding().unwrap().unwrap().id,
        "n1"
    );

    engine.mark_node_embedded("n1").unwrap();
    assert_eq!(engine.pending_embeddings_count().unwrap(), 0);
    assert!(engine.find_node_needing_embedding().unwrap().is_none());

    engine.add_to_pending_embeddings("n1").unwrap();
    assert_eq!(engine.pending_embeddings_count().unwrap(), 1);

    engine.delete_node_record("n1").unwrap();
    assert_eq!(engine.pending_embeddings_count().unwrap(), 0);

    let mut third = sample_node("n3", &["File"]);
    third
        .properties
        .insert("content".to_string(), json!("third file"));
    engine.put_node_record(&third).unwrap();
    engine.mark_node_embedded("n3").unwrap();
    assert_eq!(engine.pending_embeddings_count().unwrap(), 0);
    assert_eq!(engine.refresh_pending_embeddings_index().unwrap(), 1);
    assert_eq!(engine.pending_embeddings_count().unwrap(), 1);
    assert_eq!(
        engine.find_node_needing_embedding().unwrap().unwrap().id,
        "n3"
    );
}

#[test]
fn storage_engine_add_to_pending_embeddings_skips_embedded_or_missing_nodes() {
    let engine = StorageEngine::open_temporary().unwrap();

    let mut embedded = sample_node("n1", &["File"]);
    embedded.set_default_embedding(vec![0.1, 0.2]);
    engine.put_node_record(&embedded).unwrap();

    engine.mark_node_embedded("n1").unwrap();
    engine.add_to_pending_embeddings("n1").unwrap();
    engine.add_to_pending_embeddings("missing").unwrap();

    assert_eq!(engine.pending_embeddings_count().unwrap(), 0);
    assert!(engine.find_node_needing_embedding().unwrap().is_none());
}

#[test]
fn storage_engine_clear_all_embeddings_requeues_nodes_for_regeneration() {
    let engine = StorageEngine::open_temporary().unwrap();

    let mut first = sample_node("n1", &["File"]);
    first
        .properties
        .insert("content".to_string(), json!("first file"));
    first.set_managed_chunk_embeddings(
        vec![vec![0.1, 0.2, 0.3]],
        Some("test-model".to_string()),
        Some("2026-05-29T00:00:00Z".to_string()),
    );
    first
        .named_embeddings
        .insert("qdrant".to_string(), vec![3.0, 2.0, 1.0]);

    let mut second = sample_node("tenant_a:n1", &["File"]);
    second
        .properties
        .insert("content".to_string(), json!("tenant file"));
    second.set_managed_chunk_embeddings(
        vec![vec![0.4, 0.5, 0.6]],
        Some("test-model".to_string()),
        Some("2026-05-29T00:00:00Z".to_string()),
    );

    engine.put_node_record(&first).unwrap();
    engine.put_node_record(&second).unwrap();

    assert_eq!(engine.pending_embeddings_count().unwrap(), 0);

    let cleared = engine.clear_all_embeddings().unwrap();
    assert_eq!(cleared, 2);
    assert_eq!(engine.pending_embeddings_count().unwrap(), 1);

    let first_after = engine.get_node_record("n1").unwrap().unwrap();
    assert_eq!(
        first_after.named_embeddings.get("qdrant"),
        Some(&vec![3.0, 2.0, 1.0])
    );
    assert!(!first_after.needs_embedding());
    assert!(first_after.embed_meta.has_embedding.is_none());
    let second_after = engine.get_node_record("tenant_a:n1").unwrap().unwrap();
    assert!(second_after.chunk_embeddings.is_empty());
    assert!(second_after.embed_meta.has_chunks.is_none());
    assert!(second_after.embed_meta.chunk_count.is_none());
    assert!(second_after.needs_embedding());
}

#[test]
fn storage_engine_clear_all_embeddings_for_prefix_only_clears_matching_namespace() {
    let engine = StorageEngine::open_temporary().unwrap();

    let mut tenant_a = sample_node("tenant_a:n1", &["File"]);
    tenant_a
        .properties
        .insert("content".to_string(), json!("tenant a file"));
    tenant_a.set_managed_chunk_embeddings(
        vec![vec![0.1, 0.2, 0.3]],
        Some("test-model".to_string()),
        Some("2026-05-29T00:00:00Z".to_string()),
    );
    let mut tenant_b = sample_node("tenant_b:n1", &["File"]);
    tenant_b
        .properties
        .insert("content".to_string(), json!("tenant b file"));
    tenant_b.set_managed_chunk_embeddings(
        vec![vec![0.4, 0.5, 0.6]],
        Some("test-model".to_string()),
        Some("2026-05-29T00:00:00Z".to_string()),
    );
    tenant_b
        .named_embeddings
        .insert("qdrant".to_string(), vec![6.0, 5.0, 4.0]);

    engine.put_node_record(&tenant_a).unwrap();
    engine.put_node_record(&tenant_b).unwrap();

    let cleared = engine.clear_all_embeddings_for_prefix("tenant_a:").unwrap();
    assert_eq!(cleared, 1);
    assert_eq!(engine.pending_embeddings_count().unwrap(), 1);

    let tenant_a_after = engine.get_node_record("tenant_a:n1").unwrap().unwrap();
    assert!(tenant_a_after.chunk_embeddings.is_empty());
    let tenant_b_after = engine.get_node_record("tenant_b:n1").unwrap().unwrap();
    assert_eq!(tenant_b_after.chunk_embeddings, vec![vec![0.4, 0.5, 0.6]]);
    assert_eq!(
        tenant_b_after.named_embeddings.get("qdrant"),
        Some(&vec![6.0, 5.0, 4.0])
    );
}

#[test]
fn storage_engine_update_node_embedding_preserves_non_embedding_properties_and_removes_pending() {
    let engine = StorageEngine::open_temporary().unwrap();

    let mut node = sample_node("n1", &["File"]);
    node.properties
        .insert("content".to_string(), json!("needs embedding"));
    node.properties
        .insert("title".to_string(), json!("Original title"));
    engine.put_node_record(&node).unwrap();

    assert_eq!(engine.pending_embeddings_count().unwrap(), 1);
    assert_eq!(engine.node_count_by_prefix("").unwrap(), 1);

    node.named_embeddings
        .insert("qdrant".to_string(), vec![5.0, 5.0, 5.0]);
    engine.put_node_record(&node).unwrap();

    let mut embedding_update = sample_node("n1", &["Ignored"]);
    embedding_update.set_managed_chunk_embeddings(
        vec![vec![0.1, 0.2, 0.3]],
        Some("test-model".to_string()),
        Some("2026-05-29T00:00:00Z".to_string()),
    );
    embedding_update.updated_at_unix_ms += 100;

    engine.update_node_embedding(&embedding_update).unwrap();

    let updated = engine.get_node_record("n1").unwrap().unwrap();
    assert_eq!(updated.labels, vec!["File".to_string()]);
    assert_eq!(
        updated.properties.get("title"),
        Some(&json!("Original title"))
    );
    assert_eq!(
        updated.properties.get("content"),
        Some(&json!("needs embedding"))
    );
    assert_eq!(
        updated.embed_meta.embedding_model.as_deref(),
        Some("test-model")
    );
    assert_eq!(updated.chunk_embeddings, vec![vec![0.1, 0.2, 0.3]]);
    assert_eq!(
        updated.named_embeddings.get("qdrant"),
        Some(&vec![5.0, 5.0, 5.0])
    );
    assert_eq!(engine.node_count_by_prefix("").unwrap(), 1);
    assert_eq!(engine.pending_embeddings_count().unwrap(), 0);
}

#[test]
fn storage_engine_update_node_embedding_clears_omitted_embedding_metadata() {
    let engine = StorageEngine::open_temporary().unwrap();

    let mut node = sample_node("n1", &["File"]);
    node.properties
        .insert("content".to_string(), json!("needs embedding"));
    engine.put_node_record(&node).unwrap();

    let mut first_embedding = sample_node("n1", &["Ignored"]);
    first_embedding.set_managed_chunk_embeddings(
        vec![vec![0.1, 0.2, 0.3]],
        Some("model-a".to_string()),
        Some("2026-05-29T00:00:00Z".to_string()),
    );
    engine.update_node_embedding(&first_embedding).unwrap();

    let mut second_embedding = sample_node("n1", &["Ignored"]);
    second_embedding.set_managed_chunk_embeddings(vec![vec![0.4, 0.5, 0.6]], None, None);
    engine.update_node_embedding(&second_embedding).unwrap();

    let updated = engine.get_node_record("n1").unwrap().unwrap();
    assert_eq!(updated.chunk_embeddings, vec![vec![0.4, 0.5, 0.6]]);
    assert!(updated.embed_meta.embedding_model.is_none());
    assert_eq!(updated.embed_meta.embedding_dimensions, Some(3));
    assert!(updated.embed_meta.embedded_at.is_none());
    assert_eq!(
        updated.properties.get("content"),
        Some(&json!("needs embedding"))
    );
}

#[test]
fn async_storage_engine_update_node_embedding_does_not_change_node_count() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    let mut node = sample_node("n1", &["File"]);
    node.properties
        .insert("content".to_string(), json!("needs embedding"));
    async_engine.put_node_record(&node).unwrap();

    let count_before = async_engine.node_count_by_prefix("").unwrap();
    assert_eq!(count_before, 1);

    node.named_embeddings
        .insert("qdrant".to_string(), vec![8.0, 8.0, 8.0]);
    async_engine.put_node_record(&node).unwrap();

    let mut embedding_update = sample_node("n1", &["Ignored"]);
    embedding_update.set_managed_chunk_embeddings(
        vec![vec![0.1, 0.2, 0.3]],
        Some("test-model".to_string()),
        Some("2026-05-29T00:00:00Z".to_string()),
    );
    embedding_update.updated_at_unix_ms += 100;

    async_engine
        .update_node_embedding(&embedding_update)
        .unwrap();

    assert_eq!(async_engine.node_count_by_prefix("").unwrap(), count_before);
    assert_eq!(async_engine.pending_embeddings_count(), 0);
    let updated = async_engine.get_node_record("n1").unwrap().unwrap();
    assert_eq!(
        updated.properties.get("content"),
        Some(&json!("needs embedding"))
    );
    assert_eq!(updated.chunk_embeddings, vec![vec![0.1, 0.2, 0.3]]);
    assert_eq!(
        updated.named_embeddings.get("qdrant"),
        Some(&vec![8.0, 8.0, 8.0])
    );

    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_effective_label_reads_reflect_pending_updates_before_flush() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    async_engine
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();
    async_engine.flush().unwrap();

    let mut updated = sample_node("n1", &["Device"]);
    updated.updated_at_unix_ms += 1;
    async_engine.put_node_record(&updated).unwrap();

    assert_eq!(
        async_engine
            .get_persisted_nodes_by_label("Person")
            .unwrap()
            .len(),
        1
    );
    assert!(async_engine.pending_node_ids_for_label("Person").is_empty());
    assert_eq!(
        async_engine.pending_node_ids_for_label("Device"),
        vec!["n1".to_string()]
    );
    assert!(async_engine
        .get_nodes_by_label("Person")
        .unwrap()
        .is_empty());
    assert_eq!(async_engine.get_nodes_by_label("Device").unwrap().len(), 1);

    async_engine.flush().unwrap();
    assert!(async_engine
        .get_persisted_nodes_by_label("Person")
        .unwrap()
        .is_empty());
    assert_eq!(
        async_engine
            .get_persisted_nodes_by_label("Device")
            .unwrap()
            .len(),
        1
    );
    assert!(async_engine.pending_node_ids_for_label("Person").is_empty());
    assert!(async_engine.pending_node_ids_for_label("Device").is_empty());
    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_pending_counts_reflect_namespace_updates_before_flush() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    async_engine
        .put_node_record(&sample_node("alpha:n1", &["Person"]))
        .unwrap();
    async_engine
        .put_node_record(&sample_node("beta:n1", &["Person"]))
        .unwrap();
    async_engine
        .put_edge_record(&sample_edge("alpha:e1", "KNOWS", "alpha:n1", "beta:n1"))
        .unwrap();
    async_engine.flush().unwrap();

    let mut relabeled = sample_node("alpha:n1", &["Device"]);
    relabeled.updated_at_unix_ms += 1;
    async_engine.put_node_record(&relabeled).unwrap();
    async_engine
        .put_node_record(&sample_node("alpha:n2", &["Person"]))
        .unwrap();
    async_engine.delete_node_record("beta:n1").unwrap();
    async_engine
        .put_edge_record(&sample_edge("alpha:e2", "KNOWS", "alpha:n2", "alpha:n1"))
        .unwrap();

    assert_eq!(
        async_engine
            .get_persisted_node_count_by_prefix("alpha:")
            .unwrap(),
        1
    );
    assert_eq!(async_engine.node_count_by_prefix("alpha:").unwrap(), 2);
    assert_eq!(
        async_engine
            .get_persisted_node_count_by_prefix("beta:")
            .unwrap(),
        1
    );
    assert_eq!(async_engine.node_count_by_prefix("beta:").unwrap(), 0);
    assert_eq!(
        async_engine
            .get_persisted_node_count_by_label_in_namespace("alpha", "Person")
            .unwrap(),
        1
    );
    assert_eq!(
        async_engine
            .node_count_by_label_in_namespace("alpha", "Person")
            .unwrap(),
        1
    );
    assert_eq!(
        async_engine
            .node_count_by_label_in_namespace("alpha", "Device")
            .unwrap(),
        1
    );
    assert_eq!(
        async_engine
            .get_persisted_edge_count_by_prefix("alpha:")
            .unwrap(),
        1
    );
    assert_eq!(async_engine.edge_count_by_prefix("alpha:").unwrap(), 2);

    async_engine.flush().unwrap();
    assert_eq!(
        async_engine
            .get_persisted_node_count_by_prefix("alpha:")
            .unwrap(),
        2
    );
    assert_eq!(
        async_engine
            .get_persisted_node_count_by_prefix("beta:")
            .unwrap(),
        0
    );
    assert_eq!(
        async_engine
            .get_persisted_node_count_by_label_in_namespace("alpha", "Device")
            .unwrap(),
        1
    );
    assert_eq!(
        async_engine
            .get_persisted_edge_count_by_prefix("alpha:")
            .unwrap(),
        2
    );
    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_effective_edge_type_reads_reflect_pending_updates_before_flush() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    async_engine
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();
    async_engine
        .put_node_record(&sample_node("n2", &["Person"]))
        .unwrap();
    async_engine
        .put_edge_record(&sample_edge("e1", "KNOWS", "n1", "n2"))
        .unwrap();
    async_engine.flush().unwrap();

    let mut updated = sample_edge("e1", "LIKES", "n1", "n2");
    updated.updated_at_unix_ms += 1;
    async_engine.put_edge_record(&updated).unwrap();

    assert_eq!(
        async_engine
            .get_persisted_edges_by_type("KNOWS")
            .unwrap()
            .len(),
        1
    );
    assert!(async_engine.pending_edge_ids_for_type("KNOWS").is_empty());
    assert_eq!(
        async_engine.pending_edge_ids_for_type("LIKES"),
        vec!["e1".to_string()]
    );
    assert!(async_engine.get_edges_by_type("KNOWS").unwrap().is_empty());
    assert_eq!(async_engine.get_edges_by_type("LIKES").unwrap().len(), 1);

    async_engine.flush().unwrap();
    assert!(async_engine
        .get_persisted_edges_by_type("KNOWS")
        .unwrap()
        .is_empty());
    assert_eq!(
        async_engine
            .get_persisted_edges_by_type("LIKES")
            .unwrap()
            .len(),
        1
    );
    assert!(async_engine.pending_edge_ids_for_type("KNOWS").is_empty());
    assert!(async_engine.pending_edge_ids_for_type("LIKES").is_empty());
    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_pending_label_index_rebuilds_and_evicts_on_flush() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    async_engine
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();
    async_engine
        .put_node_record(&sample_node("n2", &["Person"]))
        .unwrap();
    assert_eq!(
        async_engine.pending_node_ids_for_label("Person"),
        vec!["n1".to_string(), "n2".to_string()]
    );

    let mut updated = sample_node("n1", &["Device"]);
    updated.updated_at_unix_ms += 1;
    async_engine.put_node_record(&updated).unwrap();

    assert_eq!(
        async_engine.pending_node_ids_for_label("Person"),
        vec!["n2".to_string()]
    );
    assert_eq!(
        async_engine.pending_node_ids_for_label("Device"),
        vec!["n1".to_string()]
    );

    async_engine.flush().unwrap();
    assert!(async_engine.pending_node_ids_for_label("Person").is_empty());
    assert!(async_engine.pending_node_ids_for_label("Device").is_empty());
    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_pending_edge_type_index_rebuilds_and_evicts_on_flush() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    async_engine
        .put_edge_record(&sample_edge("e1", "KNOWS", "n1", "n2"))
        .unwrap();
    async_engine
        .put_edge_record(&sample_edge("e2", "KNOWS", "n2", "n3"))
        .unwrap();
    assert_eq!(
        async_engine.pending_edge_ids_for_type("KNOWS"),
        vec!["e1".to_string(), "e2".to_string()]
    );

    let mut updated = sample_edge("e1", "LIKES", "n1", "n2");
    updated.updated_at_unix_ms += 1;
    async_engine.put_edge_record(&updated).unwrap();

    assert_eq!(
        async_engine.pending_edge_ids_for_type("KNOWS"),
        vec!["e2".to_string()]
    );
    assert_eq!(
        async_engine.pending_edge_ids_for_type("LIKES"),
        vec!["e1".to_string()]
    );

    async_engine.flush().unwrap();
    assert!(async_engine.pending_edge_ids_for_type("KNOWS").is_empty());
    assert!(async_engine.pending_edge_ids_for_type("LIKES").is_empty());
    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_pending_adjacency_indexes_rebuild_and_evict_on_flush() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    async_engine
        .put_edge_record(&sample_edge("e1", "KNOWS", "n1", "n2"))
        .unwrap();
    async_engine
        .put_edge_record(&sample_edge("e2", "KNOWS", "n1", "n3"))
        .unwrap();

    assert_eq!(
        async_engine.pending_edge_ids_from_start("n1"),
        vec!["e1".to_string(), "e2".to_string()]
    );
    assert_eq!(
        async_engine.pending_edge_ids_to_end("n2"),
        vec!["e1".to_string()]
    );

    let mut moved = sample_edge("e1", "KNOWS", "n4", "n1");
    moved.updated_at_unix_ms += 1;
    async_engine.put_edge_record(&moved).unwrap();

    assert_eq!(
        async_engine.pending_edge_ids_from_start("n1"),
        vec!["e2".to_string()]
    );
    assert!(async_engine.pending_edge_ids_to_end("n2").is_empty());
    assert_eq!(
        async_engine.pending_edge_ids_from_start("n4"),
        vec!["e1".to_string()]
    );
    assert_eq!(
        async_engine.pending_edge_ids_to_end("n1"),
        vec!["e1".to_string()]
    );

    async_engine.flush().unwrap();
    assert!(async_engine.pending_edge_ids_from_start("n1").is_empty());
    assert!(async_engine.pending_edge_ids_to_end("n1").is_empty());
    assert!(async_engine.pending_edge_ids_from_start("n4").is_empty());
    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_effective_adjacency_reads_reflect_pending_edge_updates_before_flush() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    async_engine
        .put_edge_record(&sample_edge("e1", "KNOWS", "n1", "n2"))
        .unwrap();
    async_engine
        .put_edge_record(&sample_edge("e2", "KNOWS", "n3", "n1"))
        .unwrap();
    async_engine.flush().unwrap();

    let mut moved = sample_edge("e1", "KNOWS", "n4", "n1");
    moved.updated_at_unix_ms += 1;
    async_engine.put_edge_record(&moved).unwrap();
    async_engine.delete_edge_record("e2").unwrap();
    async_engine
        .put_edge_record(&sample_edge("e3", "LIKES", "n1", "n5"))
        .unwrap();

    assert_eq!(
        async_engine
            .get_persisted_adjacent_edges("n1", EdgeAdjacencyDirection::Outgoing, None)
            .unwrap()
            .into_iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        vec!["e1".to_string()]
    );
    assert_eq!(
        async_engine
            .get_edges_from_node("n1")
            .unwrap()
            .into_iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        vec!["e3".to_string()]
    );
    assert_eq!(
        async_engine
            .get_edges_to_node("n1")
            .unwrap()
            .into_iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        vec!["e1".to_string()]
    );
    assert_eq!(
        async_engine
            .get_adjacent_edges("n1", EdgeAdjacencyDirection::Both, Some("KNOWS"))
            .unwrap()
            .into_iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        vec!["e1".to_string()]
    );
    assert_eq!(
        {
            let mut ids = async_engine
                .get_adjacent_edges("n1", EdgeAdjacencyDirection::Both, None)
                .unwrap()
                .into_iter()
                .map(|edge| edge.id)
                .collect::<Vec<_>>();
            ids.sort();
            ids
        },
        vec!["e1".to_string(), "e3".to_string()]
    );

    async_engine.flush().unwrap();
    assert_eq!(
        {
            let mut ids = async_engine
                .get_persisted_adjacent_edges("n1", EdgeAdjacencyDirection::Both, None)
                .unwrap()
                .into_iter()
                .map(|edge| edge.id)
                .collect::<Vec<_>>();
            ids.sort();
            ids
        },
        vec!["e1".to_string(), "e3".to_string()]
    );
    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_typed_adjacency_reads_reflect_pending_edge_updates_before_flush() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    async_engine
        .put_edge_record(&sample_edge("e1", "KNOWS", "n1", "n2"))
        .unwrap();
    async_engine
        .put_edge_record(&sample_edge("e2", "MENTORS", "n1", "n2"))
        .unwrap();
    async_engine
        .put_edge_record(&sample_edge("e3", "KNOWS", "n3", "n1"))
        .unwrap();
    async_engine.flush().unwrap();

    let mut moved = sample_edge("e1", "MENTORS", "n4", "n1");
    moved.updated_at_unix_ms += 1;
    async_engine.put_edge_record(&moved).unwrap();
    async_engine.delete_edge_record("e2").unwrap();
    async_engine
        .put_edge_record(&sample_edge("e4", "KNOWS", "n1", "n5"))
        .unwrap();

    assert_eq!(
        async_engine
            .get_edges_from_node_by_type("n1", "KNOWS")
            .unwrap()
            .into_iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        vec!["e4".to_string()]
    );
    assert!(async_engine
        .get_edges_from_node_by_type("n1", "MENTORS")
        .unwrap()
        .is_empty());
    assert_eq!(
        async_engine
            .get_edges_to_node_by_type("n1", "MENTORS")
            .unwrap()
            .into_iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        vec!["e1".to_string()]
    );
    assert_eq!(
        async_engine
            .get_edges_to_node_by_type("n1", "KNOWS")
            .unwrap()
            .into_iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        vec!["e3".to_string()]
    );

    async_engine.flush().unwrap();
    assert_eq!(
        async_engine
            .get_persisted_adjacent_edges("n1", EdgeAdjacencyDirection::Outgoing, Some("KNOWS"))
            .unwrap()
            .into_iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        vec!["e4".to_string()]
    );
    assert_eq!(
        async_engine
            .get_persisted_adjacent_edges("n1", EdgeAdjacencyDirection::Incoming, Some("MENTORS"))
            .unwrap()
            .into_iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        vec!["e1".to_string()]
    );
    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_all_and_stream_reads_merge_pending_updates_before_flush() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    async_engine
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();
    async_engine
        .put_node_record(&sample_node("n2", &["Person"]))
        .unwrap();
    async_engine
        .put_edge_record(&sample_edge("e1", "KNOWS", "n1", "n2"))
        .unwrap();
    async_engine.flush().unwrap();

    let mut updated = sample_node("n1", &["Device"]);
    updated.updated_at_unix_ms += 1;
    async_engine.put_node_record(&updated).unwrap();
    async_engine
        .put_node_record(&sample_node("n3", &["Robot"]))
        .unwrap();
    async_engine.delete_node_record("n2").unwrap();
    async_engine.delete_edge_record("e1").unwrap();
    async_engine
        .put_edge_record(&sample_edge("e2", "LIKES", "n3", "n1"))
        .unwrap();

    assert_eq!(
        async_engine
            .get_persisted_all_node_records()
            .unwrap()
            .into_iter()
            .map(|node| node.id)
            .collect::<Vec<_>>(),
        vec!["n1".to_string(), "n2".to_string()]
    );
    assert_eq!(
        async_engine
            .all_nodes()
            .unwrap()
            .into_iter()
            .map(|node| (node.id, node.labels))
            .collect::<Vec<_>>(),
        vec![
            ("n1".to_string(), vec!["Device".to_string()]),
            ("n3".to_string(), vec!["Robot".to_string()]),
        ]
    );

    let mut streamed_nodes = Vec::new();
    let streamed_node_count = async_engine
        .stream_node_records(|node| {
            streamed_nodes.push((node.id, node.labels));
            Ok(())
        })
        .unwrap();
    assert_eq!(streamed_node_count, 2);
    assert_eq!(
        streamed_nodes,
        vec![
            ("n1".to_string(), vec!["Device".to_string()]),
            ("n3".to_string(), vec!["Robot".to_string()]),
        ]
    );

    assert_eq!(
        async_engine
            .get_persisted_all_edges()
            .unwrap()
            .into_iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        vec!["e1".to_string()]
    );
    assert_eq!(
        async_engine
            .all_edges()
            .unwrap()
            .into_iter()
            .map(|edge| (edge.id, edge.edge_type, edge.start_node, edge.end_node))
            .collect::<Vec<_>>(),
        vec![(
            "e2".to_string(),
            "LIKES".to_string(),
            "n3".to_string(),
            "n1".to_string(),
        )]
    );

    let mut streamed_edges = Vec::new();
    let streamed_edge_count = async_engine
        .stream_edge_records(|edge| {
            streamed_edges.push((edge.id, edge.edge_type, edge.start_node, edge.end_node));
            Ok(())
        })
        .unwrap();
    assert_eq!(streamed_edge_count, 1);
    assert_eq!(
        streamed_edges,
        vec![(
            "e2".to_string(),
            "LIKES".to_string(),
            "n3".to_string(),
            "n1".to_string(),
        )]
    );

    async_engine.flush().unwrap();
    assert_eq!(
        async_engine
            .get_persisted_all_node_records()
            .unwrap()
            .into_iter()
            .map(|node| (node.id, node.labels))
            .collect::<Vec<_>>(),
        vec![
            ("n1".to_string(), vec!["Device".to_string()]),
            ("n3".to_string(), vec!["Robot".to_string()]),
        ]
    );
    assert_eq!(
        async_engine
            .get_persisted_all_edges()
            .unwrap()
            .into_iter()
            .map(|edge| (edge.id, edge.edge_type, edge.start_node, edge.end_node))
            .collect::<Vec<_>>(),
        vec![(
            "e2".to_string(),
            "LIKES".to_string(),
            "n3".to_string(),
            "n1".to_string(),
        )]
    );
    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_prefix_stream_reads_merge_pending_updates_before_flush() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    async_engine
        .put_node_record(&sample_node("alpha:n1", &["Person"]))
        .unwrap();
    async_engine
        .put_node_record(&sample_node("alpha:n2", &["Person"]))
        .unwrap();
    async_engine
        .put_node_record(&sample_node("beta:n1", &["Person"]))
        .unwrap();
    async_engine.flush().unwrap();

    let mut updated = sample_node("alpha:n1", &["Device"]);
    updated.updated_at_unix_ms += 1;
    async_engine.put_node_record(&updated).unwrap();
    async_engine
        .put_node_record(&sample_node("alpha:n3", &["Robot"]))
        .unwrap();
    async_engine.delete_node_record("alpha:n2").unwrap();
    async_engine
        .put_node_record(&sample_node("beta:n2", &["Ignored"]))
        .unwrap();

    let mut streamed_nodes = Vec::new();
    let streamed_count = async_engine
        .stream_node_records_by_prefix("alpha:", |node| {
            streamed_nodes.push((node.id, node.labels));
            Ok(())
        })
        .unwrap();
    assert_eq!(streamed_count, 2);
    assert_eq!(
        streamed_nodes,
        vec![
            ("alpha:n1".to_string(), vec!["Device".to_string()]),
            ("alpha:n3".to_string(), vec!["Robot".to_string()]),
        ]
    );

    async_engine.flush().unwrap();

    let mut persisted_nodes = Vec::new();
    let persisted_count = async_engine
        .stream_node_records_by_prefix("alpha:", |node| {
            persisted_nodes.push((node.id, node.labels));
            Ok(())
        })
        .unwrap();
    assert_eq!(persisted_count, 2);
    assert_eq!(
        persisted_nodes,
        vec![
            ("alpha:n1".to_string(), vec!["Device".to_string()]),
            ("alpha:n3".to_string(), vec!["Robot".to_string()]),
        ]
    );

    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_chunk_stream_reads_merge_pending_updates_before_flush() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    async_engine
        .put_node_record(&sample_node("alpha:n1", &["Person"]))
        .unwrap();
    async_engine
        .put_node_record(&sample_node("alpha:n2", &["Person"]))
        .unwrap();
    async_engine
        .put_node_record(&sample_node("beta:n1", &["Person"]))
        .unwrap();
    async_engine.flush().unwrap();

    let mut updated = sample_node("alpha:n1", &["Device"]);
    updated.updated_at_unix_ms += 1;
    async_engine.put_node_record(&updated).unwrap();
    async_engine.delete_node_record("alpha:n2").unwrap();
    async_engine
        .put_node_record(&sample_node("gamma:n1", &["Robot"]))
        .unwrap();

    let mut chunks = Vec::new();
    let streamed = async_engine
        .stream_node_record_chunks(2, |chunk| {
            chunks.push(
                chunk
                    .iter()
                    .map(|node| (node.id.clone(), node.labels.clone()))
                    .collect::<Vec<_>>(),
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(streamed, 3);
    assert_eq!(
        chunks,
        vec![
            vec![
                ("alpha:n1".to_string(), vec!["Device".to_string()]),
                ("beta:n1".to_string(), vec!["Person".to_string()]),
            ],
            vec![("gamma:n1".to_string(), vec!["Robot".to_string()])],
        ]
    );

    let err = async_engine
        .stream_node_record_chunks(0, |_| Ok(()))
        .unwrap_err();
    assert!(matches!(err, StorageError::InvalidChunkSize(0)));

    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_streaming_apis_treat_iteration_stopped_as_normal_completion() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    async_engine
        .put_node_record(&sample_node("alpha:n1", &["Person"]))
        .unwrap();
    async_engine
        .put_node_record(&sample_node("alpha:n2", &["Person"]))
        .unwrap();
    async_engine
        .put_node_record(&sample_node("beta:n1", &["Person"]))
        .unwrap();
    async_engine
        .put_edge_record(&sample_edge("alpha:e1", "KNOWS", "alpha:n1", "alpha:n2"))
        .unwrap();
    async_engine
        .put_edge_record(&sample_edge("beta:e1", "KNOWS", "beta:n1", "beta:n1"))
        .unwrap();
    async_engine.flush().unwrap();

    let mut updated = sample_node("alpha:n1", &["Device"]);
    updated.updated_at_unix_ms += 1;
    async_engine.put_node_record(&updated).unwrap();
    async_engine.delete_node_record("alpha:n2").unwrap();
    async_engine
        .put_node_record(&sample_node("gamma:n1", &["Robot"]))
        .unwrap();
    async_engine.delete_edge_record("beta:e1").unwrap();
    async_engine
        .put_edge_record(&sample_edge("gamma:e1", "LIKES", "gamma:n1", "alpha:n1"))
        .unwrap();

    let mut node_ids = Vec::new();
    let streamed = async_engine
        .stream_node_records(|node| {
            node_ids.push(node.id);
            if node_ids.len() == 2 {
                return Err(StorageError::IterationStopped);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(streamed, 2);
    assert_eq!(node_ids, vec!["alpha:n1", "beta:n1"]);

    let mut alpha_ids = Vec::new();
    let streamed = async_engine
        .stream_node_records_by_prefix("alpha:", |node| {
            alpha_ids.push(node.id);
            Err(StorageError::IterationStopped)
        })
        .unwrap();
    assert_eq!(streamed, 1);
    assert_eq!(alpha_ids, vec!["alpha:n1"]);

    let mut edge_ids = Vec::new();
    let streamed = async_engine
        .stream_edge_records(|edge| {
            edge_ids.push(edge.id);
            Err(StorageError::IterationStopped)
        })
        .unwrap();
    assert_eq!(streamed, 1);
    assert_eq!(edge_ids, vec!["alpha:e1"]);

    let mut chunks = Vec::new();
    let streamed = async_engine
        .stream_node_record_chunks(2, |chunk| {
            chunks.push(chunk.iter().map(|node| node.id.clone()).collect::<Vec<_>>());
            Err(StorageError::IterationStopped)
        })
        .unwrap();
    assert_eq!(streamed, 2);
    assert_eq!(
        chunks,
        vec![vec!["alpha:n1".to_string(), "beta:n1".to_string()]]
    );

    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_streaming_apis_surface_external_cancellation() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    async_engine
        .put_node_record(&sample_node("alpha:n1", &["Person"]))
        .unwrap();
    async_engine
        .put_node_record(&sample_node("alpha:n2", &["Person"]))
        .unwrap();
    async_engine
        .put_node_record(&sample_node("beta:n1", &["Person"]))
        .unwrap();
    async_engine
        .put_edge_record(&sample_edge("alpha:e1", "KNOWS", "alpha:n1", "alpha:n2"))
        .unwrap();
    async_engine
        .put_edge_record(&sample_edge("beta:e1", "KNOWS", "beta:n1", "beta:n1"))
        .unwrap();

    let cancel = RequestCancellation::new();
    let mut node_ids = Vec::new();
    let err = async_engine
        .stream_node_records_with_cancellation(&cancel, |node| {
            node_ids.push(node.id);
            cancel.cancel();
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(err, StorageError::RequestCancelled(_)));
    assert_eq!(node_ids, vec!["alpha:n1"]);

    let cancel = RequestCancellation::new();
    let mut alpha_ids = Vec::new();
    let err = async_engine
        .stream_node_records_by_prefix_with_cancellation("alpha:", &cancel, |node| {
            alpha_ids.push(node.id);
            cancel.cancel();
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(err, StorageError::RequestCancelled(_)));
    assert_eq!(alpha_ids, vec!["alpha:n1"]);

    let cancel = RequestCancellation::new();
    let mut edge_ids = Vec::new();
    let err = async_engine
        .stream_edge_records_with_cancellation(&cancel, |edge| {
            edge_ids.push(edge.id);
            cancel.cancel();
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(err, StorageError::RequestCancelled(_)));
    assert_eq!(edge_ids, vec!["alpha:e1"]);

    let cancel = RequestCancellation::new();
    let mut chunks = Vec::new();
    let err = async_engine
        .stream_node_record_chunks_with_cancellation(2, &cancel, |chunk| {
            chunks.push(chunk.iter().map(|node| node.id.clone()).collect::<Vec<_>>());
            cancel.cancel();
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(err, StorageError::RequestCancelled(_)));
    assert_eq!(
        chunks,
        vec![vec!["alpha:n1".to_string(), "alpha:n2".to_string()]]
    );

    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_hold_flush_blocks_background_auto_flush_until_release() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 10,
            ..Default::default()
        }),
    );
    async_engine
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();

    let guard = async_engine.hold_flush();
    thread::sleep(Duration::from_millis(40));
    assert!(async_engine
        .get_persisted_node_record("n1")
        .unwrap()
        .is_none());
    drop(guard);

    wait_until(
        || {
            async_engine
                .get_persisted_node_record("n1")
                .unwrap()
                .is_some()
        },
        Duration::from_secs(1),
    );
    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_hold_flush_prevents_try_flush_until_release() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );
    async_engine
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();

    let guard = async_engine.hold_flush();
    assert!(async_engine.try_flush().unwrap().is_none());
    drop(guard);

    let result = async_engine.try_flush().unwrap().unwrap();
    assert_eq!(result.nodes_written, 1);
    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_forces_node_flush_when_pending_cache_limit_is_reached() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            max_node_cache_size: 1,
            ..Default::default()
        }),
    );

    async_engine
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();
    assert!(async_engine
        .get_persisted_node_record("n1")
        .unwrap()
        .is_none());

    async_engine
        .put_node_record(&sample_node("n2", &["Person"]))
        .unwrap();

    assert!(async_engine
        .get_persisted_node_record("n1")
        .unwrap()
        .is_some());
    assert!(async_engine
        .get_persisted_node_record("n2")
        .unwrap()
        .is_none());
    assert!(async_engine
        .get_node_record_latest_effective("n2")
        .unwrap()
        .is_some());
    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_forces_edge_flush_when_pending_cache_limit_is_reached() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            max_edge_cache_size: 1,
            ..Default::default()
        }),
    );

    async_engine
        .put_edge_record(&sample_edge("e1", "KNOWS", "n1", "n2"))
        .unwrap();
    assert!(async_engine
        .get_persisted_edge_record("e1")
        .unwrap()
        .is_none());

    async_engine
        .put_edge_record(&sample_edge("e2", "KNOWS", "n2", "n3"))
        .unwrap();

    assert!(async_engine
        .get_persisted_edge_record("e1")
        .unwrap()
        .is_some());
    assert!(async_engine
        .get_persisted_edge_record("e2")
        .unwrap()
        .is_none());
    assert!(async_engine
        .get_edge_record_latest_effective("e2")
        .unwrap()
        .is_some());
    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_delete_pending_created_node_notifies_callback_without_deadlock() {
    let async_engine = Arc::new(AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    ));

    async_engine
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();

    let weak_engine = Arc::downgrade(&async_engine);
    let (callback_tx, callback_rx) = std::sync::mpsc::channel();
    async_engine.on_node_deleted(Arc::new(move |id| {
        let engine = weak_engine.upgrade().expect("async engine still alive");
        assert!(engine.get_node_record(&id).unwrap().is_none());
        callback_tx.send(id).unwrap();
    }));

    let delete_engine = Arc::clone(&async_engine);
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        done_tx
            .send(delete_engine.delete_node_record("n1"))
            .unwrap();
    });

    done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("delete_node_record deadlocked")
        .unwrap();
    assert_eq!(
        callback_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        "n1".to_string()
    );

    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_delete_pending_updated_node_does_not_notify_callback() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    async_engine
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();
    async_engine.flush().unwrap();

    let mut updated = sample_node("n1", &["Device"]);
    updated.updated_at_unix_ms += 1;
    async_engine.put_node_record(&updated).unwrap();

    let (callback_tx, callback_rx) = std::sync::mpsc::channel();
    async_engine.on_node_deleted(Arc::new(move |id| {
        callback_tx.send(id).unwrap();
    }));

    async_engine.delete_node_record("n1").unwrap();
    assert!(callback_rx.recv_timeout(Duration::from_millis(50)).is_err());

    async_engine.close().unwrap();
}

#[test]
fn async_storage_engine_delete_pending_created_edge_notifies_callback() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    async_engine
        .put_edge_record(&sample_edge("e1", "KNOWS", "n1", "n2"))
        .unwrap();

    let (callback_tx, callback_rx) = std::sync::mpsc::channel();
    async_engine.on_edge_deleted(Arc::new(move |id| {
        callback_tx.send(id).unwrap();
    }));

    async_engine.delete_edge_record("e1").unwrap();
    assert_eq!(
        callback_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        "e1".to_string()
    );

    async_engine.close().unwrap();
}

fn wait_until(predicate: impl Fn() -> bool, timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if predicate() {
            return;
        }
        thread::sleep(Duration::from_millis(5));
    }
    assert!(predicate());
}

#[test]
fn async_storage_engine_delegates_mvcc_lifecycle_controls() {
    let async_engine = AsyncStorageEngine::new(
        StorageEngine::open_temporary().unwrap(),
        Some(AsyncStorageConfig {
            flush_interval_ms: 60_000,
            ..Default::default()
        }),
    );

    async_engine
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();
    async_engine.flush().unwrap();
    let snapshot = async_engine.begin_mvcc_snapshot();

    let mut updated = sample_node("n1", &["Device"]);
    updated.updated_at_unix_ms += 1;
    async_engine.put_node_record(&updated).unwrap();
    async_engine.flush().unwrap();

    assert_eq!(
        async_engine
            .get_nodes_by_label_visible_at(&snapshot, "Person")
            .unwrap()
            .len(),
        1
    );

    async_engine.pause_lifecycle();
    assert!(async_engine.lifecycle_status().paused);
    async_engine.set_lifecycle_schedule_ms(7_500);
    assert_eq!(async_engine.lifecycle_status().schedule_interval_ms, 7_500);
    let removed = async_engine.prune_mvcc_versions(MvccPruneOptions {
        max_versions_per_key: Some(1),
    });
    assert!(removed > 0);
    async_engine.resume_lifecycle();
    assert!(!async_engine.lifecycle_status().paused);
    async_engine.close().unwrap();
}

#[test]
fn mvcc_snapshot_isolation_and_pruning_work() {
    let mvcc = MvccStore::new();
    let snapshot0 = mvcc.begin_snapshot();
    assert_eq!(snapshot0.read_ts, 0);

    let v1 = mvcc.commit_batch(vec![("node:1".to_string(), Some(b"alice".to_vec()))]);
    assert_eq!(v1, 1);
    let snapshot1 = mvcc.begin_snapshot();

    let v2 = mvcc.commit_batch(vec![("node:1".to_string(), Some(b"bob".to_vec()))]);
    assert_eq!(v2, 2);
    let snapshot2 = mvcc.begin_snapshot();

    assert_eq!(mvcc.read(&snapshot0, "node:1"), None);
    assert_eq!(mvcc.read(&snapshot1, "node:1"), Some(b"alice".to_vec()));
    assert_eq!(mvcc.read(&snapshot2, "node:1"), Some(b"bob".to_vec()));

    mvcc.prune_versions_older_than(2);
    assert_eq!(mvcc.read(&snapshot2, "node:1"), Some(b"bob".to_vec()));
    let head = mvcc.head();
    assert_eq!(head.floor, 2);
    assert_eq!(head.head, 2);
}

#[test]
fn mvcc_registered_snapshot_blocks_pruning_past_active_reader() {
    let mvcc = MvccStore::new();

    let v1 = mvcc.commit_batch(vec![("node:1".to_string(), Some(b"alice".to_vec()))]);
    assert_eq!(v1, 1);
    let snapshot1 = mvcc.begin_registered_snapshot();

    let v2 = mvcc.commit_batch(vec![("node:1".to_string(), Some(b"bob".to_vec()))]);
    assert_eq!(v2, 2);
    let snapshot2 = mvcc.begin_snapshot();

    mvcc.prune_versions_older_than(2);
    assert_eq!(mvcc.oldest_active_reader(), Some(1));
    assert_eq!(
        mvcc.read(snapshot1.snapshot(), "node:1"),
        Some(b"alice".to_vec())
    );
    assert_eq!(mvcc.read(&snapshot2, "node:1"), Some(b"bob".to_vec()));

    let head = mvcc.head();
    assert_eq!(head.floor, 1);
    assert_eq!(head.head, 2);

    drop(snapshot1);
    mvcc.prune_versions_older_than(2);

    assert_eq!(mvcc.oldest_active_reader(), None);
    let head = mvcc.head();
    assert_eq!(head.floor, 2);
    assert_eq!(head.head, 2);
    assert_eq!(mvcc.read(&snapshot2, "node:1"), Some(b"bob".to_vec()));
}

#[test]
fn mvcc_lifecycle_status_reports_debt_and_reader_pressure() {
    let mvcc = MvccStore::new();
    mvcc.commit_batch(vec![("node:1".to_string(), Some(b"v1".to_vec()))]);
    let reader = mvcc.begin_registered_snapshot();
    mvcc.commit_batch(vec![("node:1".to_string(), Some(b"v2".to_vec()))]);
    mvcc.commit_batch(vec![("node:1".to_string(), Some(b"v3".to_vec()))]);

    let status = mvcc.lifecycle_status();
    assert!(status.enabled);
    assert!(!status.paused);
    assert_eq!(status.schedule_interval_ms, 60_000);
    assert_eq!(status.floor, 0);
    assert_eq!(status.head, 3);
    assert_eq!(status.oldest_active_reader, Some(1));
    assert_eq!(status.active_reader_count, 1);
    assert_eq!(status.retained_versions, 3);
    assert_eq!(status.prune_debt, 0);
    assert_eq!(status.suggested_prune_floor, 1);

    drop(reader);
    let status = mvcc.lifecycle_status();
    assert_eq!(status.oldest_active_reader, None);
    assert_eq!(status.active_reader_count, 0);
    assert_eq!(status.suggested_prune_floor, 3);
    assert_eq!(status.prune_debt, 2);
}

#[test]
fn mvcc_churn_prune_bounds_retained_chain_and_hides_pre_floor_reads() {
    let mvcc = MvccStore::new();
    mvcc.commit_batch(vec![("node:1".to_string(), Some(b"seed".to_vec()))]);

    for iteration in 1..=24 {
        mvcc.commit_batch(vec![(
            "node:1".to_string(),
            Some(format!("live-{iteration}").into_bytes()),
        )]);
        if iteration % 6 == 0 {
            mvcc.commit_batch(vec![("node:1".to_string(), None)]);
            mvcc.commit_batch(vec![(
                "node:1".to_string(),
                Some(format!("ghost-reset-{iteration}").into_bytes()),
            )]);
        }
    }

    let head_before = mvcc.current_head_for_key("node:1").unwrap();
    assert!(mvcc.retained_version_count() > 12);

    let removed = mvcc.trigger_prune_now(3);
    assert!(removed > 0);

    let head_after = mvcc.current_head_for_key("node:1").unwrap();
    assert_eq!(head_after.head, head_before.head);
    assert!(!head_after.tombstoned);
    assert!(head_after.floor >= head_before.floor);
    assert!(mvcc.retained_version_count() <= 4);

    let latest_snapshot = mvcc.begin_snapshot();
    assert_eq!(
        mvcc.read(&latest_snapshot, "node:1"),
        Some(b"ghost-reset-24".to_vec())
    );

    let floor_snapshot = MvccSnapshot {
        id: head_after.floor,
        read_ts: head_after.floor,
    };
    assert!(mvcc.read(&floor_snapshot, "node:1").is_some());

    let pre_floor = head_after.floor.saturating_sub(1);
    let pre_floor_snapshot = MvccSnapshot {
        id: pre_floor,
        read_ts: pre_floor,
    };
    assert_eq!(mvcc.read(&pre_floor_snapshot, "node:1"), None);
}

#[test]
fn mvcc_prune_cleans_ghost_label_history_candidates_and_bounds_shared_scan_fanout() {
    let mvcc = MvccStore::new();

    for index in 0..24 {
        let node_id = format!("ghost-node-{index}");
        mvcc.put_node_record(&sample_node(&node_id, &["Ghost"]))
            .unwrap();
        mvcc.delete_node_record(&node_id).unwrap();
    }

    let latest = mvcc.begin_snapshot();
    assert!(mvcc.get_nodes_by_label("Ghost").unwrap().is_empty());
    assert!(mvcc
        .get_nodes_by_label_visible_at(&latest, "Ghost")
        .unwrap()
        .is_empty());
    assert_eq!(mvcc.label_history_candidate_count("Ghost"), 24);

    let removed = mvcc.trigger_prune_now(0);
    assert!(removed >= 24);
    assert_eq!(mvcc.label_history_candidate_count("Ghost"), 0);
    assert!(mvcc
        .get_nodes_by_label_visible_at(&mvcc.begin_snapshot(), "Ghost")
        .unwrap()
        .is_empty());
}

#[test]
fn mvcc_prune_cleans_ghost_edge_type_history_candidates_and_bounds_shared_scan_fanout() {
    let mvcc = MvccStore::new();

    for index in 0..24 {
        let node_id = format!("edge-node-{index}");
        mvcc.put_node_record(&sample_node(&node_id, &["Vertex"]))
            .unwrap();

        let edge_id = format!("ghost-edge-{index}");
        mvcc.put_edge_record(&sample_edge(&edge_id, "KNOWS", &node_id, &node_id))
            .unwrap();
        mvcc.delete_edge_record(&edge_id).unwrap();
    }

    let latest = mvcc.begin_snapshot();
    assert!(mvcc.get_edges_by_type("KNOWS").unwrap().is_empty());
    assert!(mvcc
        .get_edges_by_type_visible_at(&latest, "KNOWS")
        .unwrap()
        .is_empty());
    assert_eq!(mvcc.edge_type_history_candidate_count("KNOWS"), 24);

    let removed = mvcc.trigger_prune_now(0);
    assert!(removed >= 24);
    assert_eq!(mvcc.edge_type_history_candidate_count("KNOWS"), 0);
    assert!(mvcc
        .get_edges_by_type_visible_at(&mvcc.begin_snapshot(), "KNOWS")
        .unwrap()
        .is_empty());
}

#[test]
fn mvcc_prune_versions_uses_explicit_max_versions_per_key_and_preserves_reader_anchor() {
    let mvcc = MvccStore::new();
    mvcc.commit_batch(vec![("node:1".to_string(), Some(b"v1".to_vec()))]);
    let reader = mvcc.begin_registered_snapshot();
    mvcc.commit_batch(vec![("node:1".to_string(), Some(b"v2".to_vec()))]);
    mvcc.commit_batch(vec![("node:1".to_string(), Some(b"v3".to_vec()))]);

    let removed = mvcc.prune_mvcc_versions(MvccPruneOptions {
        max_versions_per_key: Some(1),
    });
    assert_eq!(removed, 1);
    assert_eq!(mvcc.head().floor, 1);
    assert_eq!(mvcc.retained_version_count(), 2);
    assert_eq!(mvcc.read(reader.snapshot(), "node:1"), Some(b"v1".to_vec()));

    drop(reader);
    let removed = mvcc.prune_mvcc_versions(MvccPruneOptions {
        max_versions_per_key: Some(1),
    });
    assert_eq!(removed, 1);
    assert_eq!(mvcc.head().floor, 3);
    assert_eq!(mvcc.retained_version_count(), 1);
    assert_eq!(
        mvcc.read(&mvcc.begin_snapshot(), "node:1"),
        Some(b"v3".to_vec())
    );
}

#[test]
fn mvcc_lifecycle_admin_controls_surface_pause_schedule_and_debt_keys() {
    let mvcc = MvccStore::new();
    mvcc.commit_batch(vec![("node:1".to_string(), Some(b"v1".to_vec()))]);
    mvcc.commit_batch(vec![("node:1".to_string(), Some(b"v2".to_vec()))]);
    mvcc.commit_batch(vec![("node:1".to_string(), Some(b"v3".to_vec()))]);
    mvcc.commit_batch(vec![("node:2".to_string(), Some(b"a1".to_vec()))]);
    mvcc.commit_batch(vec![("node:2".to_string(), Some(b"a2".to_vec()))]);

    let debt = mvcc.top_lifecycle_debt_keys(2);
    assert_eq!(debt.len(), 2);
    assert_eq!(
        debt[0],
        MvccLifecycleDebtKey {
            logical_key: "node:1".to_string(),
            retained_versions: 3,
            prune_debt: 2,
        }
    );
    assert_eq!(
        debt[1],
        MvccLifecycleDebtKey {
            logical_key: "node:2".to_string(),
            retained_versions: 2,
            prune_debt: 1,
        }
    );

    mvcc.pause_lifecycle();
    assert!(mvcc.lifecycle_status().paused);

    mvcc.set_lifecycle_schedule_ms(15_000);
    assert_eq!(mvcc.lifecycle_status().schedule_interval_ms, 15_000);

    mvcc.resume_lifecycle();
    assert!(!mvcc.lifecycle_status().paused);
}

#[test]
fn mvcc_indexed_visible_reads_follow_history_without_polluting_current_indexes() {
    let mvcc = MvccStore::new();

    let mut node = sample_node("n1", &["Person"]);
    node.properties.insert("state".to_string(), json!("v1"));
    mvcc.put_node_record(&node).unwrap();

    let edge = sample_edge("e1", "KNOWS", "n1", "n1");
    mvcc.put_edge_record(&edge).unwrap();
    let before_change = mvcc.begin_snapshot();

    let mut updated_node = sample_node("n1", &["Device"]);
    updated_node
        .properties
        .insert("state".to_string(), json!("v2"));
    mvcc.put_node_record(&updated_node).unwrap();

    let updated_edge = sample_edge("e1", "SEES", "n1", "n1");
    mvcc.put_edge_record(&updated_edge).unwrap();

    assert!(mvcc.get_nodes_by_label("Person").unwrap().is_empty());
    assert_eq!(
        mvcc.get_nodes_by_label("Device")
            .unwrap()
            .into_iter()
            .map(|node| node.id)
            .collect::<Vec<_>>(),
        vec!["n1".to_string()]
    );
    assert!(mvcc.get_edges_by_type("KNOWS").unwrap().is_empty());
    assert_eq!(
        mvcc.get_edges_by_type("SEES")
            .unwrap()
            .into_iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        vec!["e1".to_string()]
    );

    assert_eq!(
        mvcc.get_nodes_by_label_visible_at(&before_change, "Person")
            .unwrap()
            .into_iter()
            .map(|node| (node.id, node.properties.get("state").cloned()))
            .collect::<Vec<_>>(),
        vec![("n1".to_string(), Some(json!("v1")))]
    );
    assert!(mvcc
        .get_nodes_by_label_visible_at(&before_change, "Device")
        .unwrap()
        .is_empty());
    assert_eq!(
        mvcc.get_edges_by_type_visible_at(&before_change, "KNOWS")
            .unwrap()
            .into_iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        vec!["e1".to_string()]
    );
    assert!(mvcc
        .get_edges_by_type_visible_at(&before_change, "SEES")
        .unwrap()
        .is_empty());
}

#[test]
fn namespaced_mvcc_store_delegates_lifecycle_controls_and_visible_reads() {
    let mvcc = MvccStore::new();
    let tenant_a = mvcc.for_namespace("tenant_a");
    let tenant_b = mvcc.for_namespace("tenant_b");

    tenant_a
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();
    tenant_b
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();
    tenant_a
        .put_edge_record(&sample_edge("e1", "KNOWS", "n1", "n1"))
        .unwrap();
    let snapshot = tenant_a.begin_snapshot();

    tenant_a
        .put_node_record(&sample_node("n1", &["Device"]))
        .unwrap();
    tenant_a
        .put_edge_record(&sample_edge("e1", "SEES", "n1", "n1"))
        .unwrap();

    assert_eq!(
        tenant_a
            .get_nodes_by_label_visible_at(&snapshot, "Person")
            .unwrap()
            .into_iter()
            .map(|node| node.id)
            .collect::<Vec<_>>(),
        vec!["n1".to_string()]
    );
    assert!(tenant_b
        .get_nodes_by_label_visible_at(&snapshot, "Person")
        .unwrap()
        .into_iter()
        .all(|node| node.id == "n1"));
    assert_eq!(
        tenant_a
            .get_edges_by_type_visible_at(&snapshot, "KNOWS")
            .unwrap()
            .into_iter()
            .map(|edge| (edge.id, edge.start_node, edge.end_node))
            .collect::<Vec<_>>(),
        vec![("e1".to_string(), "n1".to_string(), "n1".to_string())]
    );

    tenant_a.pause_lifecycle();
    assert!(tenant_a.lifecycle_status().paused);
    tenant_a.set_lifecycle_schedule_ms(5_000);
    assert_eq!(tenant_a.lifecycle_status().schedule_interval_ms, 5_000);
    tenant_a.resume_lifecycle();
    assert!(!tenant_a.lifecycle_status().paused);

    let debt = tenant_a.top_lifecycle_debt_keys(4);
    assert!(debt
        .iter()
        .all(|entry| entry.logical_key.starts_with("node:")
            || entry.logical_key.starts_with("edge:")));
    assert!(debt
        .iter()
        .all(|entry| !entry.logical_key.contains("tenant_a:")));

    let pruned = tenant_a.prune_mvcc_versions(MvccPruneOptions {
        max_versions_per_key: Some(1),
    });
    assert!(pruned > 0);
}

#[test]
fn storage_engine_prune_mvcc_versions_delegates_and_preserves_reader_anchor() {
    let engine = StorageEngine::open_temporary().unwrap();
    engine
        .put_node_record(&sample_node("n1", &["Person"]))
        .unwrap();
    let lease = engine.begin_registered_mvcc_snapshot();

    let mut updated = sample_node("n1", &["Device"]);
    updated.updated_at_unix_ms += 1;
    engine.put_node_record(&updated).unwrap();
    updated.updated_at_unix_ms += 1;
    engine.put_node_record(&updated).unwrap();

    let removed = engine.prune_mvcc_versions(MvccPruneOptions {
        max_versions_per_key: Some(1),
    });
    assert_eq!(removed, 1);
    assert_eq!(engine.lifecycle_status().floor, 1);
    assert_eq!(
        engine
            .get_node_record_visible_at(lease.snapshot(), "n1")
            .unwrap()
            .unwrap()
            .labels,
        vec!["Person".to_string()]
    );

    drop(lease);
    let removed = engine.prune_mvcc_versions(MvccPruneOptions {
        max_versions_per_key: Some(1),
    });
    assert_eq!(removed, 1);
    assert_eq!(
        engine.lifecycle_status().floor,
        engine.lifecycle_status().head
    );
}

#[test]
fn mvcc_trigger_prune_now_respects_active_readers_and_reports_removed_versions() {
    let mvcc = MvccStore::new();
    mvcc.commit_batch(vec![("node:1".to_string(), Some(b"v1".to_vec()))]);
    let reader = mvcc.begin_registered_snapshot();
    mvcc.commit_batch(vec![("node:1".to_string(), Some(b"v2".to_vec()))]);
    mvcc.commit_batch(vec![("node:1".to_string(), Some(b"v3".to_vec()))]);

    let removed = mvcc.trigger_prune_now(0);
    assert_eq!(removed, 0);
    assert_eq!(mvcc.head().floor, 1);
    assert_eq!(mvcc.read(reader.snapshot(), "node:1"), Some(b"v1".to_vec()));

    drop(reader);
    let removed = mvcc.trigger_prune_now(0);
    assert_eq!(removed, 2);
    assert_eq!(mvcc.head().floor, 3);
    let snapshot = mvcc.begin_snapshot();
    assert_eq!(mvcc.read(&snapshot, "node:1"), Some(b"v3".to_vec()));
}

#[test]
fn mvcc_head_decode_errors_match_contract() {
    let err = MvccStore::decode_head(&[1, 2, 3]).unwrap_err();
    assert!(matches!(err, StorageError::MvccHeadTruncated(3)));
    assert_eq!(err.to_string(), "mvcc head truncated: 3 bytes");

    let err = MvccStore::decode_head(&[0; 10]).unwrap_err();
    assert!(matches!(err, StorageError::MvccHeadMissingFloor(10)));
    assert_eq!(err.to_string(), "mvcc head missing floor: 10 bytes");
}

#[test]
fn wal_batch_replay_and_checksum_error_paths_work() {
    let wal = WAL::new(WALConfig {
        enabled: true,
        max_entries_per_segment: 2,
    });
    let (start, end) = wal
        .append_batch(vec![
            ("put".to_string(), "node:1".to_string(), b"a".to_vec()),
            ("put".to_string(), "node:2".to_string(), b"b".to_vec()),
            ("delete".to_string(), "node:1".to_string(), Vec::new()),
        ])
        .unwrap();
    assert_eq!((start, end), (1, 3));
    assert_eq!(wal.stats().segments, 2);

    let replay = wal.replay_after(1).unwrap();
    assert_eq!(replay.len(), 2);
    assert_eq!(replay[0].key, "node:2");
    assert_eq!(replay[1].op, "delete");

    wal.inject_corruption_for_test(2).unwrap();
    let err = wal.replay_after(0).unwrap_err();
    assert!(matches!(err, StorageError::WalChecksumVerificationFailed));
    assert!(wal.is_degraded());
}

#[test]
fn wal_persists_entries_and_reopens_next_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("wal.rmp");
    let config = WALConfig {
        enabled: true,
        max_entries_per_segment: 2,
    };

    let wal = WAL::open(&wal_path, config.clone()).unwrap();
    let first = wal.append("put", "node:1", b"a").unwrap();
    assert_eq!(first.seq, 1);
    let (_start, end) = wal
        .append_batch(vec![
            ("put".to_string(), "node:2".to_string(), b"b".to_vec()),
            ("delete".to_string(), "node:1".to_string(), Vec::new()),
        ])
        .unwrap();
    assert_eq!(end, 3);
    assert_eq!(wal.stats().segments, 2);
    drop(wal);

    let reopened = WAL::open(&wal_path, config).unwrap();
    let replay = reopened.replay_after(0).unwrap();
    assert_eq!(replay.len(), 3);
    assert_eq!(replay[0].key, "node:1");
    assert_eq!(replay[2].op, "delete");
    let next = reopened.append("put", "node:3", b"c").unwrap();
    assert_eq!(next.seq, 4);
}

#[test]
fn wal_compaction_truncates_replay_and_preserves_sequence_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("wal.rmp");
    let config = WALConfig {
        enabled: true,
        max_entries_per_segment: 2,
    };

    let wal = WAL::open(&wal_path, config.clone()).unwrap();
    let (_start, end) = wal
        .append_batch(vec![
            ("put".to_string(), "node:1".to_string(), b"a".to_vec()),
            ("put".to_string(), "node:2".to_string(), b"b".to_vec()),
            ("delete".to_string(), "node:1".to_string(), Vec::new()),
        ])
        .unwrap();
    assert_eq!(end, 3);

    let removed = wal.compact_up_to(2).unwrap();
    assert_eq!(removed, 2);
    let stats = wal.stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.segments, 1);
    assert_eq!(stats.compacted_through, 2);
    assert_eq!(stats.next_seq, 3);

    let replay = wal.replay_after(0).unwrap();
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].seq, 3);
    drop(wal);

    let reopened = WAL::open(&wal_path, config).unwrap();
    let stats = reopened.stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.compacted_through, 2);
    assert_eq!(stats.next_seq, 3);

    let replay = reopened.replay_after(0).unwrap();
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].seq, 3);
    let next = reopened.append("put", "node:3", b"c").unwrap();
    assert_eq!(next.seq, 4);
}

#[test]
fn wal_rejects_invalid_persistent_file() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("wal.rmp");
    fs::write(&wal_path, b"not-messagepack").unwrap();

    let err = WAL::open(&wal_path, WALConfig::default()).unwrap_err();
    assert!(matches!(err, StorageError::WalMissingOrInvalidTrailer));
}

#[test]
fn wal_close_and_partial_write_errors_match_contract() {
    let wal = WAL::new(WALConfig::default());
    let err = wal.mark_partial_write_detected().unwrap_err();
    assert!(matches!(err, StorageError::WalPartialWriteDetected));
    assert_eq!(err.to_string(), "wal: partial write detected");

    wal.close();
    let err = wal.append("put", "node:1", b"x").unwrap_err();
    assert!(matches!(err, StorageError::WalClosed));
    assert_eq!(err.to_string(), "wal: closed");
}

#[test]
fn schema_constraints_validate_and_persist() {
    let schema = SchemaManager::new();
    schema
        .add_constraint(Constraint {
            name: "person_email_unique".to_string(),
            constraint_type: ConstraintType::Unique,
            entity_type: ConstraintEntityType::Node,
            label: "Person".to_string(),
            properties: vec!["email".to_string()],
        })
        .unwrap();
    schema
        .add_constraint(Constraint {
            name: "person_email_exists".to_string(),
            constraint_type: ConstraintType::Exists,
            entity_type: ConstraintEntityType::Node,
            label: "Person".to_string(),
            properties: vec!["email".to_string()],
        })
        .unwrap();

    let missing = BTreeMap::new();
    let err = schema.validate_node("n1", "Person", &missing).unwrap_err();
    assert!(matches!(
        err,
        StorageError::ConstraintMissingProperty { .. }
    ));

    let mut alice = BTreeMap::new();
    alice.insert("email".to_string(), json!("alice@example.com"));
    schema.validate_node("n1", "Person", &alice).unwrap();

    let mut duplicate = BTreeMap::new();
    duplicate.insert("email".to_string(), json!("alice@example.com"));
    let err = schema
        .validate_node("n2", "Person", &duplicate)
        .unwrap_err();
    assert!(matches!(
        err,
        StorageError::UniqueConstraintViolation { .. }
    ));
    assert_eq!(
        err.to_string(),
        "Node(Person) already exists with email = \"alice@example.com\""
    );

    let mut updated = BTreeMap::new();
    updated.insert("email".to_string(), json!("alice+new@example.com"));
    schema.validate_node("n1", "Person", &updated).unwrap();
    schema.validate_node("n2", "Person", &duplicate).unwrap();

    let engine = StorageEngine::open_temporary().unwrap();
    for constraint in schema.list_constraints() {
        engine.persist_constraint(&constraint).unwrap();
    }
    let loaded = engine.load_constraints().unwrap();
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].name, "person_email_exists");
    assert_eq!(loaded[1].name, "person_email_unique");
}

#[test]
fn schema_index_definitions_roundtrip() {
    let engine = StorageEngine::open_temporary().unwrap();
    engine
        .persist_index_definition(&IndexDefinition {
            name: "person_email_idx".to_string(),
            entity_type: IndexEntityType::Node,
            kind: IndexKind::Range,
            label: "Person".to_string(),
            properties: vec!["email".to_string()],
        })
        .unwrap();

    let indexes = engine.load_index_definitions().unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].name, "person_email_idx");

    let deleted = engine.delete_index_definition("person_email_idx").unwrap();
    assert!(deleted);
    assert!(engine.load_index_definitions().unwrap().is_empty());
}

#[test]
fn metadata_only_index_definitions_do_not_build_exact_property_lookup_state() {
    let engine = StorageEngine::open_temporary().unwrap();

    let mut alice = sample_node("db:n1", &["Person"]);
    alice
        .properties
        .insert("bio".into(), json!("Rust graph database engineer"));
    engine.put_node_record(&alice).unwrap();

    let mut edge = sample_edge("db:e1", "KNOWS", "db:n1", "db:n2");
    edge.properties
        .insert("embedding".to_string(), json!([0.1, 0.2, 0.3]));
    engine.put_edge_record(&edge).unwrap();

    engine
        .persist_index_definition(&IndexDefinition {
            name: "person_bio_fulltext_idx".to_string(),
            entity_type: IndexEntityType::Node,
            kind: IndexKind::FullText,
            label: "Person".to_string(),
            properties: vec!["bio".to_string()],
        })
        .unwrap();
    engine
        .persist_index_definition(&IndexDefinition {
            name: "knows_embedding_vector_idx".to_string(),
            entity_type: IndexEntityType::Relationship,
            kind: IndexKind::Vector,
            label: "KNOWS".to_string(),
            properties: vec!["embedding".to_string()],
        })
        .unwrap();

    let indexes = engine.load_index_definitions().unwrap();
    assert_eq!(indexes.len(), 2);
    assert_eq!(indexes[0].name, "knows_embedding_vector_idx");
    assert_eq!(indexes[0].kind, IndexKind::Vector);
    assert_eq!(indexes[1].name, "person_bio_fulltext_idx");
    assert_eq!(indexes[1].kind, IndexKind::FullText);

    assert!(engine
        .get_nodes_by_property("Person", "bio", &json!("Rust graph database engineer"))
        .unwrap()
        .is_empty());
    assert!(engine
        .get_edges_by_property("KNOWS", "embedding", &json!([0.1, 0.2, 0.3]))
        .unwrap()
        .is_empty());

    alice
        .properties
        .insert("bio".into(), json!("Updated searchable biography"));
    engine.put_node_record(&alice).unwrap();

    edge.properties
        .insert("embedding".to_string(), json!([0.4, 0.5, 0.6]));
    engine.put_edge_record(&edge).unwrap();

    assert!(engine
        .get_nodes_by_property("Person", "bio", &json!("Updated searchable biography"))
        .unwrap()
        .is_empty());
    assert!(engine
        .get_edges_by_property("KNOWS", "embedding", &json!([0.4, 0.5, 0.6]))
        .unwrap()
        .is_empty());
}

#[test]
fn fulltext_index_rebuilds_and_tracks_mutations() {
    let engine = StorageEngine::open_temporary().unwrap();
    let mut alice = sample_node("db:n1", &["Person"]);
    alice
        .properties
        .insert("bio".into(), json!("Rust graph database engineer"));
    let mut bob = sample_node("db:n2", &["Person"]);
    bob.properties
        .insert("bio".into(), json!("Storage systems specialist"));
    engine.put_node_record(&alice).unwrap();
    engine.put_node_record(&bob).unwrap();

    assert!(engine
        .search_fulltext_nodes_by_properties("Person", &["bio".into()], "graph engineer", 10,)
        .unwrap()
        .is_empty());

    engine
        .persist_index_definition(&IndexDefinition {
            name: "person_bio_fulltext_idx".to_string(),
            entity_type: IndexEntityType::Node,
            kind: IndexKind::FullText,
            label: "Person".to_string(),
            properties: vec!["bio".to_string()],
        })
        .unwrap();

    let results = engine
        .search_fulltext_nodes_by_properties("Person", &["bio".into()], "graph engineer", 10)
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0.id, "db:n1");
    assert_eq!(results[0].1, 2);

    alice
        .properties
        .insert("bio".into(), json!("Updated biography about storage"));
    engine.put_node_record(&alice).unwrap();

    assert!(engine
        .search_fulltext_nodes_by_properties("Person", &["bio".into()], "graph engineer", 10)
        .unwrap()
        .is_empty());
    let updated = engine
        .search_fulltext_nodes_by_properties("Person", &["bio".into()], "updated biography", 10)
        .unwrap();
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].0.id, "db:n1");
    assert_eq!(updated[0].1, 2);

    engine.delete_node_record("db:n1").unwrap();
    assert!(engine
        .search_fulltext_nodes_by_properties("Person", &["bio".into()], "updated biography", 10)
        .unwrap()
        .is_empty());
}

#[test]
fn node_property_index_rebuilds_and_tracks_mutations() {
    let engine = StorageEngine::open_temporary().unwrap();
    let mut alice = sample_node("db:n1", &["Person"]);
    alice
        .properties
        .insert("email".into(), json!("alice@example.com"));
    let mut bob = sample_node("db:n2", &["Person"]);
    bob.properties
        .insert("email".into(), json!("bob@example.com"));
    engine.put_node_record(&alice).unwrap();
    engine.put_node_record(&bob).unwrap();

    assert!(engine
        .get_nodes_by_property("Person", "email", &json!("alice@example.com"))
        .unwrap()
        .is_empty());

    engine
        .persist_index_definition(&IndexDefinition {
            name: "person_email_idx".to_string(),
            entity_type: IndexEntityType::Node,
            kind: IndexKind::Range,
            label: "Person".to_string(),
            properties: vec!["email".to_string()],
        })
        .unwrap();

    let alice_hits = engine
        .get_nodes_by_property("Person", "email", &json!("alice@example.com"))
        .unwrap();
    assert_eq!(alice_hits.len(), 1);
    assert_eq!(alice_hits[0].id, "db:n1");

    alice
        .properties
        .insert("email".into(), json!("alice@new.test"));
    engine.put_node_record(&alice).unwrap();
    assert!(engine
        .get_nodes_by_property("Person", "email", &json!("alice@example.com"))
        .unwrap()
        .is_empty());
    assert_eq!(
        engine
            .get_nodes_by_property("Person", "email", &json!("alice@new.test"))
            .unwrap()[0]
            .id,
        "db:n1"
    );

    engine.delete_node_record("db:n1").unwrap();
    assert!(engine
        .get_nodes_by_property("Person", "email", &json!("alice@new.test"))
        .unwrap()
        .is_empty());

    engine.delete_index_definition("person_email_idx").unwrap();
    assert!(engine
        .get_nodes_by_property("Person", "email", &json!("bob@example.com"))
        .unwrap()
        .is_empty());
}

#[test]
fn composite_node_property_index_rebuilds_and_tracks_mutations() {
    let engine = StorageEngine::open_temporary().unwrap();
    let mut alice_us = sample_node("db:n1", &["Person"]);
    alice_us
        .properties
        .insert("email".into(), json!("alice@example.com"));
    alice_us.properties.insert("country".into(), json!("US"));

    let mut alice_ca = sample_node("db:n2", &["Person"]);
    alice_ca
        .properties
        .insert("email".into(), json!("alice@example.com"));
    alice_ca.properties.insert("country".into(), json!("CA"));

    let mut bob_us = sample_node("db:n3", &["Person"]);
    bob_us
        .properties
        .insert("email".into(), json!("bob@example.com"));
    bob_us.properties.insert("country".into(), json!("US"));

    engine.put_node_record(&alice_us).unwrap();
    engine.put_node_record(&alice_ca).unwrap();
    engine.put_node_record(&bob_us).unwrap();

    let composite_properties = vec!["email".to_string(), "country".to_string()];
    let lookup = std::collections::HashMap::from([
        ("email".to_string(), json!("alice@example.com")),
        ("country".to_string(), json!("US")),
    ]);
    assert!(engine
        .get_nodes_by_properties("Person", &composite_properties, &lookup)
        .unwrap()
        .is_empty());

    engine
        .persist_index_definition(&IndexDefinition {
            name: "person_email_country_idx".to_string(),
            entity_type: IndexEntityType::Node,
            kind: IndexKind::Range,
            label: "Person".to_string(),
            properties: composite_properties.clone(),
        })
        .unwrap();

    let hits = engine
        .get_nodes_by_properties("Person", &composite_properties, &lookup)
        .unwrap();
    assert_eq!(
        hits.iter().map(|node| node.id.as_str()).collect::<Vec<_>>(),
        vec!["db:n1"]
    );

    alice_us.properties.insert("country".into(), json!("GB"));
    engine.put_node_record(&alice_us).unwrap();
    assert!(engine
        .get_nodes_by_properties("Person", &composite_properties, &lookup)
        .unwrap()
        .is_empty());

    let updated_lookup = std::collections::HashMap::from([
        ("email".to_string(), json!("alice@example.com")),
        ("country".to_string(), json!("GB")),
    ]);
    let updated_hits = engine
        .get_nodes_by_properties("Person", &composite_properties, &updated_lookup)
        .unwrap();
    assert_eq!(
        updated_hits
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>(),
        vec!["db:n1"]
    );

    engine.delete_node_record("db:n1").unwrap();
    assert!(engine
        .get_nodes_by_properties("Person", &composite_properties, &updated_lookup)
        .unwrap()
        .is_empty());

    engine
        .delete_index_definition("person_email_country_idx")
        .unwrap();
    assert!(engine
        .get_nodes_by_properties(
            "Person",
            &composite_properties,
            &std::collections::HashMap::from([
                ("email".to_string(), json!("bob@example.com")),
                ("country".to_string(), json!("US")),
            ])
        )
        .unwrap()
        .is_empty());
}

#[test]
fn node_property_range_index_filters_numeric_and_string_values() {
    let engine = StorageEngine::open_temporary().unwrap();
    let mut alice = sample_node("db:n1", &["Person"]);
    alice.properties.insert("age".into(), json!(29));
    alice.properties.insert("name".into(), json!("Alice"));

    let mut bob = sample_node("db:n2", &["Person"]);
    bob.properties.insert("age".into(), json!(35));
    bob.properties.insert("name".into(), json!("Bob"));

    let mut carol = sample_node("db:n3", &["Person"]);
    carol.properties.insert("age".into(), json!(41));
    carol.properties.insert("name".into(), json!("Carol"));

    let mut device = sample_node("db:n4", &["Device"]);
    device.properties.insert("age".into(), json!(99));
    device.properties.insert("name".into(), json!("Zeta"));

    engine.put_node_record(&alice).unwrap();
    engine.put_node_record(&bob).unwrap();
    engine.put_node_record(&carol).unwrap();
    engine.put_node_record(&device).unwrap();

    engine
        .persist_index_definition(&IndexDefinition {
            name: "person_age_idx".to_string(),
            entity_type: IndexEntityType::Node,
            kind: IndexKind::Range,
            label: "Person".to_string(),
            properties: vec!["age".to_string()],
        })
        .unwrap();
    engine
        .persist_index_definition(&IndexDefinition {
            name: "person_name_idx".to_string(),
            entity_type: IndexEntityType::Node,
            kind: IndexKind::Range,
            label: "Person".to_string(),
            properties: vec!["name".to_string()],
        })
        .unwrap();

    let greater_than = engine
        .get_nodes_by_property_range(
            "Person",
            "age",
            RangeIndexComparison::GreaterThan,
            &json!(30),
        )
        .unwrap();
    assert_eq!(
        greater_than
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>(),
        vec!["db:n2", "db:n3"]
    );

    let less_than_or_equal = engine
        .get_nodes_by_property_range(
            "Person",
            "age",
            RangeIndexComparison::LessThanOrEqual,
            &json!(35),
        )
        .unwrap();
    assert_eq!(
        less_than_or_equal
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>(),
        vec!["db:n1", "db:n2"]
    );

    let string_range = engine
        .get_nodes_by_property_range(
            "Person",
            "name",
            RangeIndexComparison::GreaterThanOrEqual,
            &json!("Bob"),
        )
        .unwrap();
    assert_eq!(
        string_range
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>(),
        vec!["db:n2", "db:n3"]
    );

    assert!(engine
        .get_nodes_by_property_range(
            "Person",
            "missing",
            RangeIndexComparison::GreaterThan,
            &json!(1)
        )
        .unwrap()
        .is_empty());
}

#[test]
fn property_index_value_keys_preserve_numeric_and_string_order() {
    assert!(
        property_index_value_key(&json!(-5)) < property_index_value_key(&json!(0)),
        "negative numbers must sort before zero"
    );
    assert!(
        property_index_value_key(&json!(2)) < property_index_value_key(&json!(10)),
        "numeric key encoding must preserve numeric ordering"
    );
    assert!(
        property_index_value_key(&json!("Bob")) < property_index_value_key(&json!("Carol")),
        "string key encoding must preserve lexical ordering"
    );
}

#[test]
fn composite_node_property_range_index_filters_exact_suffix_values() {
    let engine = StorageEngine::open_temporary().unwrap();

    let mut alice = sample_node("db:n1", &["Person"]);
    alice.properties.insert("age".to_string(), json!(29));
    alice.properties.insert("team".to_string(), json!("ops"));

    let mut bob = sample_node("db:n2", &["Person"]);
    bob.properties.insert("age".to_string(), json!(35));
    bob.properties.insert("team".to_string(), json!("ops"));

    let mut carol = sample_node("db:n3", &["Person"]);
    carol.properties.insert("age".to_string(), json!(41));
    carol.properties.insert("team".to_string(), json!("sales"));

    let mut dan = sample_node("db:n4", &["Person"]);
    dan.properties.insert("age".to_string(), json!(43));
    dan.properties.insert("team".to_string(), json!("ops"));

    engine.put_node_record(&alice).unwrap();
    engine.put_node_record(&bob).unwrap();
    engine.put_node_record(&carol).unwrap();
    engine.put_node_record(&dan).unwrap();

    let properties = vec!["age".to_string(), "team".to_string()];
    engine
        .persist_index_definition(&IndexDefinition {
            name: "person_age_team_idx".to_string(),
            entity_type: IndexEntityType::Node,
            kind: IndexKind::Range,
            label: "Person".to_string(),
            properties: properties.clone(),
        })
        .unwrap();

    let matched = engine
        .get_nodes_by_properties_range(
            "Person",
            &properties,
            "age",
            RangeIndexComparison::GreaterThan,
            &json!(30),
            &std::collections::HashMap::from([("team".to_string(), json!("ops"))]),
        )
        .unwrap();
    assert_eq!(
        matched
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>(),
        vec!["db:n2", "db:n4"]
    );
}

#[test]
fn composite_node_property_range_index_allows_missing_exact_suffix_values() {
    let engine = StorageEngine::open_temporary().unwrap();

    let mut alice = sample_node("db:n1", &["Person"]);
    alice.properties.insert("age".to_string(), json!(29));
    alice.properties.insert("team".to_string(), json!("ops"));

    let mut bob = sample_node("db:n2", &["Person"]);
    bob.properties.insert("age".to_string(), json!(35));
    bob.properties.insert("team".to_string(), json!("ops"));

    let mut carol = sample_node("db:n3", &["Person"]);
    carol.properties.insert("age".to_string(), json!(41));
    carol.properties.insert("team".to_string(), json!("sales"));

    engine.put_node_record(&alice).unwrap();
    engine.put_node_record(&bob).unwrap();
    engine.put_node_record(&carol).unwrap();

    let properties = vec!["age".to_string(), "team".to_string()];
    engine
        .persist_index_definition(&IndexDefinition {
            name: "person_age_team_idx".to_string(),
            entity_type: IndexEntityType::Node,
            kind: IndexKind::Range,
            label: "Person".to_string(),
            properties: properties.clone(),
        })
        .unwrap();

    let matched = engine
        .get_nodes_by_properties_range(
            "Person",
            &properties,
            "age",
            RangeIndexComparison::GreaterThan,
            &json!(30),
            &std::collections::HashMap::new(),
        )
        .unwrap();
    assert_eq!(
        matched
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>(),
        vec!["db:n2", "db:n3"]
    );
}

#[test]
fn composite_node_property_range_index_supports_non_leading_range_with_exact_prefix_values() {
    let engine = StorageEngine::open_temporary().unwrap();

    let mut alice = sample_node("db:n1", &["Person"]);
    alice.properties.insert("team".to_string(), json!("ops"));
    alice.properties.insert("age".to_string(), json!(29));

    let mut bob = sample_node("db:n2", &["Person"]);
    bob.properties.insert("team".to_string(), json!("ops"));
    bob.properties.insert("age".to_string(), json!(35));

    let mut carol = sample_node("db:n3", &["Person"]);
    carol.properties.insert("team".to_string(), json!("sales"));
    carol.properties.insert("age".to_string(), json!(41));

    let mut drew = sample_node("db:n4", &["Person"]);
    drew.properties.insert("team".to_string(), json!("ops"));
    drew.properties.insert("age".to_string(), json!(43));

    engine.put_node_record(&alice).unwrap();
    engine.put_node_record(&bob).unwrap();
    engine.put_node_record(&carol).unwrap();
    engine.put_node_record(&drew).unwrap();

    let properties = vec!["team".to_string(), "age".to_string()];
    engine
        .persist_index_definition(&IndexDefinition {
            name: "person_team_age_idx".to_string(),
            entity_type: IndexEntityType::Node,
            kind: IndexKind::Range,
            label: "Person".to_string(),
            properties: properties.clone(),
        })
        .unwrap();

    let matched = engine
        .get_nodes_by_properties_range(
            "Person",
            &properties,
            "age",
            RangeIndexComparison::GreaterThan,
            &json!(30),
            &std::collections::HashMap::from([("team".to_string(), json!("ops"))]),
        )
        .unwrap();
    assert_eq!(
        matched
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>(),
        vec!["db:n2", "db:n4"]
    );
}

#[test]
fn relationship_property_index_rebuilds_and_tracks_mutations() {
    let engine = StorageEngine::open_temporary().unwrap();
    let mut edge = sample_edge("db:e1", "KNOWS", "db:n1", "db:n2");
    engine.put_edge_record(&edge).unwrap();

    assert!(engine
        .get_edges_by_property("KNOWS", "weight", &json!(0.9))
        .unwrap()
        .is_empty());

    engine
        .persist_index_definition(&IndexDefinition {
            name: "knows_weight_idx".to_string(),
            entity_type: IndexEntityType::Relationship,
            kind: IndexKind::Range,
            label: "KNOWS".to_string(),
            properties: vec!["weight".to_string()],
        })
        .unwrap();

    assert_eq!(
        engine
            .get_edges_by_property("KNOWS", "weight", &json!(0.9))
            .unwrap()
            .iter()
            .map(|edge| edge.id.as_str())
            .collect::<Vec<_>>(),
        vec!["db:e1"]
    );

    edge.properties.insert("weight".to_string(), json!(1.5));
    engine.put_edge_record(&edge).unwrap();
    assert!(engine
        .get_edges_by_property("KNOWS", "weight", &json!(0.9))
        .unwrap()
        .is_empty());
    assert_eq!(
        engine
            .get_edges_by_property("KNOWS", "weight", &json!(1.5))
            .unwrap()
            .iter()
            .map(|edge| edge.id.as_str())
            .collect::<Vec<_>>(),
        vec!["db:e1"]
    );

    engine.delete_edge_record("db:e1").unwrap();
    assert!(engine
        .get_edges_by_property("KNOWS", "weight", &json!(1.5))
        .unwrap()
        .is_empty());

    engine.delete_index_definition("knows_weight_idx").unwrap();
    assert!(engine
        .get_edges_by_property("KNOWS", "weight", &json!(1.5))
        .unwrap()
        .is_empty());
}

#[test]
fn composite_relationship_property_index_rebuilds_and_tracks_mutations() {
    let engine = StorageEngine::open_temporary().unwrap();

    let mut edge_one = sample_edge("db:e1", "KNOWS", "db:n1", "db:n2");
    edge_one.properties.insert("years".to_string(), json!(5));
    let mut edge_two = sample_edge("db:e2", "KNOWS", "db:n2", "db:n3");
    edge_two.properties.insert("years".to_string(), json!(3));
    engine.put_edge_record(&edge_one).unwrap();
    engine.put_edge_record(&edge_two).unwrap();

    let composite_properties = vec!["weight".to_string(), "years".to_string()];
    let lookup = std::collections::HashMap::from([
        ("weight".to_string(), json!(0.9)),
        ("years".to_string(), json!(5)),
    ]);
    assert!(engine
        .get_edges_by_properties("KNOWS", &composite_properties, &lookup)
        .unwrap()
        .is_empty());

    engine
        .persist_index_definition(&IndexDefinition {
            name: "knows_weight_years_idx".to_string(),
            entity_type: IndexEntityType::Relationship,
            kind: IndexKind::Range,
            label: "KNOWS".to_string(),
            properties: composite_properties.clone(),
        })
        .unwrap();

    assert_eq!(
        engine
            .get_edges_by_properties("KNOWS", &composite_properties, &lookup)
            .unwrap()
            .iter()
            .map(|edge| edge.id.as_str())
            .collect::<Vec<_>>(),
        vec!["db:e1"]
    );

    edge_one.properties.insert("years".to_string(), json!(8));
    engine.put_edge_record(&edge_one).unwrap();
    assert!(engine
        .get_edges_by_properties("KNOWS", &composite_properties, &lookup)
        .unwrap()
        .is_empty());

    let updated_lookup = std::collections::HashMap::from([
        ("weight".to_string(), json!(0.9)),
        ("years".to_string(), json!(8)),
    ]);
    assert_eq!(
        engine
            .get_edges_by_properties("KNOWS", &composite_properties, &updated_lookup)
            .unwrap()
            .iter()
            .map(|edge| edge.id.as_str())
            .collect::<Vec<_>>(),
        vec!["db:e1"]
    );

    engine.delete_edge_record("db:e1").unwrap();
    assert!(engine
        .get_edges_by_properties("KNOWS", &composite_properties, &updated_lookup)
        .unwrap()
        .is_empty());

    engine
        .delete_index_definition("knows_weight_years_idx")
        .unwrap();
    assert!(engine
        .get_edges_by_properties(
            "KNOWS",
            &composite_properties,
            &std::collections::HashMap::from([
                ("weight".to_string(), json!(0.9)),
                ("years".to_string(), json!(3)),
            ]),
        )
        .unwrap()
        .is_empty());
}

#[test]
fn relationship_property_range_index_filters_numeric_and_string_values() {
    let engine = StorageEngine::open_temporary().unwrap();

    let mut edge_one = sample_edge("db:e1", "KNOWS", "db:n1", "db:n2");
    edge_one.properties.insert("weight".to_string(), json!(0.5));
    edge_one
        .properties
        .insert("kind".to_string(), json!("close"));

    let mut edge_two = sample_edge("db:e2", "KNOWS", "db:n2", "db:n3");
    edge_two.properties.insert("weight".to_string(), json!(1.5));
    edge_two
        .properties
        .insert("kind".to_string(), json!("trusted"));

    let mut edge_three = sample_edge("db:e3", "KNOWS", "db:n3", "db:n4");
    edge_three
        .properties
        .insert("weight".to_string(), json!(3.0));
    edge_three
        .properties
        .insert("kind".to_string(), json!("vip"));

    let mut other_type = sample_edge("db:e4", "LIKES", "db:n1", "db:n4");
    other_type
        .properties
        .insert("weight".to_string(), json!(9.0));
    other_type
        .properties
        .insert("kind".to_string(), json!("zzz"));

    engine.put_edge_record(&edge_one).unwrap();
    engine.put_edge_record(&edge_two).unwrap();
    engine.put_edge_record(&edge_three).unwrap();
    engine.put_edge_record(&other_type).unwrap();

    engine
        .persist_index_definition(&IndexDefinition {
            name: "knows_weight_idx".to_string(),
            entity_type: IndexEntityType::Relationship,
            kind: IndexKind::Range,
            label: "KNOWS".to_string(),
            properties: vec!["weight".to_string()],
        })
        .unwrap();
    engine
        .persist_index_definition(&IndexDefinition {
            name: "knows_kind_idx".to_string(),
            entity_type: IndexEntityType::Relationship,
            kind: IndexKind::Range,
            label: "KNOWS".to_string(),
            properties: vec!["kind".to_string()],
        })
        .unwrap();

    let greater_than = engine
        .get_edges_by_property_range(
            "KNOWS",
            "weight",
            RangeIndexComparison::GreaterThan,
            &json!(1.0),
        )
        .unwrap();
    assert_eq!(
        greater_than
            .iter()
            .map(|edge| edge.id.as_str())
            .collect::<Vec<_>>(),
        vec!["db:e2", "db:e3"]
    );

    let less_than_or_equal = engine
        .get_edges_by_property_range(
            "KNOWS",
            "weight",
            RangeIndexComparison::LessThanOrEqual,
            &json!(1.5),
        )
        .unwrap();
    assert_eq!(
        less_than_or_equal
            .iter()
            .map(|edge| edge.id.as_str())
            .collect::<Vec<_>>(),
        vec!["db:e1", "db:e2"]
    );

    let string_range = engine
        .get_edges_by_property_range(
            "KNOWS",
            "kind",
            RangeIndexComparison::GreaterThanOrEqual,
            &json!("trusted"),
        )
        .unwrap();
    assert_eq!(
        string_range
            .iter()
            .map(|edge| edge.id.as_str())
            .collect::<Vec<_>>(),
        vec!["db:e2", "db:e3"]
    );
}

#[test]
fn composite_relationship_property_range_index_filters_exact_suffix_values() {
    let engine = StorageEngine::open_temporary().unwrap();

    let mut edge_one = sample_edge("db:e1", "KNOWS", "db:n1", "db:n2");
    edge_one.properties.insert("weight".to_string(), json!(1.5));
    edge_one.properties.insert("years".to_string(), json!(3));

    let mut edge_two = sample_edge("db:e2", "KNOWS", "db:n2", "db:n3");
    edge_two.properties.insert("weight".to_string(), json!(2.5));
    edge_two.properties.insert("years".to_string(), json!(5));

    let mut edge_three = sample_edge("db:e3", "KNOWS", "db:n3", "db:n4");
    edge_three
        .properties
        .insert("weight".to_string(), json!(3.0));
    edge_three.properties.insert("years".to_string(), json!(5));

    engine.put_edge_record(&edge_one).unwrap();
    engine.put_edge_record(&edge_two).unwrap();
    engine.put_edge_record(&edge_three).unwrap();

    let properties = vec!["weight".to_string(), "years".to_string()];
    engine
        .persist_index_definition(&IndexDefinition {
            name: "knows_weight_years_idx".to_string(),
            entity_type: IndexEntityType::Relationship,
            kind: IndexKind::Range,
            label: "KNOWS".to_string(),
            properties: properties.clone(),
        })
        .unwrap();

    let matched = engine
        .get_edges_by_properties_range(
            "KNOWS",
            &properties,
            "weight",
            RangeIndexComparison::GreaterThan,
            &json!(2.0),
            &std::collections::HashMap::from([("years".to_string(), json!(5))]),
        )
        .unwrap();
    assert_eq!(
        matched
            .iter()
            .map(|edge| edge.id.as_str())
            .collect::<Vec<_>>(),
        vec!["db:e2", "db:e3"]
    );
}

#[test]
fn composite_relationship_property_range_index_allows_missing_exact_suffix_values() {
    let engine = StorageEngine::open_temporary().unwrap();

    let mut edge_one = sample_edge("db:e1", "KNOWS", "db:n1", "db:n2");
    edge_one.properties.insert("weight".to_string(), json!(1.5));
    edge_one.properties.insert("years".to_string(), json!(3));

    let mut edge_two = sample_edge("db:e2", "KNOWS", "db:n2", "db:n3");
    edge_two.properties.insert("weight".to_string(), json!(2.5));
    edge_two.properties.insert("years".to_string(), json!(5));

    let mut edge_three = sample_edge("db:e3", "KNOWS", "db:n3", "db:n4");
    edge_three
        .properties
        .insert("weight".to_string(), json!(3.0));
    edge_three.properties.insert("years".to_string(), json!(5));

    engine.put_edge_record(&edge_one).unwrap();
    engine.put_edge_record(&edge_two).unwrap();
    engine.put_edge_record(&edge_three).unwrap();

    let properties = vec!["weight".to_string(), "years".to_string()];
    engine
        .persist_index_definition(&IndexDefinition {
            name: "knows_weight_years_idx".to_string(),
            entity_type: IndexEntityType::Relationship,
            kind: IndexKind::Range,
            label: "KNOWS".to_string(),
            properties: properties.clone(),
        })
        .unwrap();

    let matched = engine
        .get_edges_by_properties_range(
            "KNOWS",
            &properties,
            "weight",
            RangeIndexComparison::GreaterThan,
            &json!(2.0),
            &std::collections::HashMap::new(),
        )
        .unwrap();
    assert_eq!(
        matched
            .iter()
            .map(|edge| edge.id.as_str())
            .collect::<Vec<_>>(),
        vec!["db:e2", "db:e3"]
    );
}

#[test]
fn composite_relationship_property_range_index_supports_non_leading_range_with_exact_prefix_values()
{
    let engine = StorageEngine::open_temporary().unwrap();

    let mut edge_one = sample_edge("db:e1", "KNOWS", "db:n1", "db:n2");
    edge_one.properties.insert("years".to_string(), json!(5));
    edge_one.properties.insert("weight".to_string(), json!(1.5));

    let mut edge_two = sample_edge("db:e2", "KNOWS", "db:n2", "db:n3");
    edge_two.properties.insert("years".to_string(), json!(5));
    edge_two.properties.insert("weight".to_string(), json!(2.5));

    let mut edge_three = sample_edge("db:e3", "KNOWS", "db:n3", "db:n4");
    edge_three.properties.insert("years".to_string(), json!(2));
    edge_three
        .properties
        .insert("weight".to_string(), json!(7.0));

    let mut edge_four = sample_edge("db:e4", "KNOWS", "db:n4", "db:n5");
    edge_four.properties.insert("years".to_string(), json!(5));
    edge_four
        .properties
        .insert("weight".to_string(), json!(3.0));

    engine.put_edge_record(&edge_one).unwrap();
    engine.put_edge_record(&edge_two).unwrap();
    engine.put_edge_record(&edge_three).unwrap();
    engine.put_edge_record(&edge_four).unwrap();

    let properties = vec!["years".to_string(), "weight".to_string()];
    engine
        .persist_index_definition(&IndexDefinition {
            name: "knows_years_weight_idx".to_string(),
            entity_type: IndexEntityType::Relationship,
            kind: IndexKind::Range,
            label: "KNOWS".to_string(),
            properties: properties.clone(),
        })
        .unwrap();

    let matched = engine
        .get_edges_by_properties_range(
            "KNOWS",
            &properties,
            "weight",
            RangeIndexComparison::GreaterThan,
            &json!(2.0),
            &std::collections::HashMap::from([("years".to_string(), json!(5))]),
        )
        .unwrap();
    assert_eq!(
        matched
            .iter()
            .map(|edge| edge.id.as_str())
            .collect::<Vec<_>>(),
        vec!["db:e2", "db:e4"]
    );
}

#[test]
fn knowledge_policy_decay_profile_roundtrip_and_update() {
    let engine = StorageEngine::open_temporary().unwrap();
    let profile = DecayProfileSchema {
        name: "slow_decay".to_string(),
        half_life_seconds: 604_800,
        visibility_threshold: 0.1,
        score_floor: 0.0,
        function: "exponential".to_string(),
        scope: "NODE".to_string(),
        decay_enabled: true,
        score_from: "CREATED".to_string(),
        score_from_property: None,
        enabled: true,
    };
    engine.persist_decay_profile_schema(&profile).unwrap();

    let mut updates = BTreeMap::new();
    updates.insert("visibilityThreshold".to_string(), json!(0.2));
    updates.insert("scoreFloor".to_string(), json!(0.05));
    engine
        .alter_decay_profile_schema("slow_decay", &updates)
        .unwrap();

    let loaded = engine.load_decay_profile_schemas().unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].name, "slow_decay");
    assert_eq!(loaded[0].visibility_threshold, 0.2);
    assert_eq!(loaded[0].score_floor, 0.05);
}

#[test]
fn knowledge_policy_promotion_schema_roundtrip_and_reference_guards() {
    let engine = StorageEngine::open_temporary().unwrap();
    engine
        .persist_promotion_profile_schema(&PromotionProfileSchema {
            name: "boost_profile".to_string(),
            scope: "NODE".to_string(),
            multiplier: 1.5,
            score_floor: 0.0,
            score_cap: 1.0,
            enabled: true,
        })
        .unwrap();

    engine
        .persist_promotion_policy_schema(&PromotionPolicySchema {
            name: "fact_policy".to_string(),
            target_labels: vec!["KnowledgeFact".to_string()],
            target_edge_type: None,
            is_wildcard: false,
            is_edge: false,
            enabled: true,
            on_access_mutations: vec![PromotionOnAccessMutationSchema {
                kind: PromotionOnAccessMutationKindSchema::SetLastAccessedNow,
            }],
            when_clauses: vec![PromotionWhenClauseSchema {
                profile_ref: "boost_profile".to_string(),
                predicate: "n.evidence >= 3".to_string(),
                order: 1,
            }],
        })
        .unwrap();

    let err = engine
        .delete_promotion_profile_schema("boost_profile", false)
        .unwrap_err();
    assert!(matches!(err, StorageError::KnowledgePolicyInUse(_)));

    let mut updates = BTreeMap::new();
    updates.insert("enabled".to_string(), json!(false));
    engine
        .alter_promotion_policy_schema("fact_policy", &updates)
        .unwrap();
    let policies = engine.load_promotion_policy_schemas().unwrap();
    assert_eq!(policies.len(), 1);
    assert_eq!(policies[0].target_labels, vec!["KnowledgeFact".to_string()]);
    assert_eq!(policies[0].target_edge_type, None);
    assert!(!policies[0].is_edge);
    assert_eq!(policies[0].on_access_mutations.len(), 1);
    assert!(!policies[0].enabled);

    engine
        .delete_promotion_policy_schema("fact_policy", false)
        .unwrap();
    engine
        .delete_promotion_profile_schema("boost_profile", false)
        .unwrap();
    assert!(engine.load_promotion_policy_schemas().unwrap().is_empty());
    assert!(engine.load_promotion_profile_schemas().unwrap().is_empty());
}

#[test]
fn knowledge_policy_promotion_schema_rejects_duplicate_targets() {
    let engine = StorageEngine::open_temporary().unwrap();
    engine
        .persist_promotion_profile_schema(&PromotionProfileSchema {
            name: "boost_profile".to_string(),
            scope: "NODE".to_string(),
            multiplier: 1.5,
            score_floor: 0.0,
            score_cap: 1.0,
            enabled: true,
        })
        .unwrap();

    engine
        .persist_promotion_policy_schema(&PromotionPolicySchema {
            name: "fact_policy_a".to_string(),
            target_labels: vec!["KnowledgeFact".to_string()],
            target_edge_type: None,
            is_wildcard: false,
            is_edge: false,
            enabled: true,
            on_access_mutations: vec![PromotionOnAccessMutationSchema {
                kind: PromotionOnAccessMutationKindSchema::SetLastAccessedNow,
            }],
            when_clauses: vec![PromotionWhenClauseSchema {
                profile_ref: "boost_profile".to_string(),
                predicate: "true".to_string(),
                order: 1,
            }],
        })
        .unwrap();

    let err = engine
        .persist_promotion_policy_schema(&PromotionPolicySchema {
            name: "fact_policy_b".to_string(),
            target_labels: vec!["KnowledgeFact".to_string()],
            target_edge_type: None,
            is_wildcard: false,
            is_edge: false,
            enabled: true,
            on_access_mutations: vec![PromotionOnAccessMutationSchema {
                kind: PromotionOnAccessMutationKindSchema::IncrementAccessCount,
            }],
            when_clauses: vec![],
        })
        .unwrap_err();
    assert!(matches!(err, StorageError::KnowledgePolicyInvalid(_)));
}

#[test]
fn knowledge_policy_decay_binding_schema_roundtrip_and_reference_guards() {
    let engine = StorageEngine::open_temporary().unwrap();
    engine
        .persist_decay_profile_schema(&DecayProfileSchema {
            name: "slow_decay".to_string(),
            half_life_seconds: 604_800,
            visibility_threshold: 0.1,
            score_floor: 0.0,
            function: "exponential".to_string(),
            scope: "NODE".to_string(),
            decay_enabled: true,
            score_from: "CREATED".to_string(),
            score_from_property: None,
            enabled: true,
        })
        .unwrap();

    engine
        .persist_decay_profile_binding_schema(&DecayProfileBindingSchema {
            name: "memory_binding".to_string(),
            target_labels: vec!["MemoryEpisode".to_string()],
            target_edge_type: None,
            is_wildcard: false,
            is_edge: false,
            profile_ref: Some("slow_decay".to_string()),
            no_decay: false,
            visibility_threshold: Some(0.2),
            order: 10,
        })
        .unwrap();

    let bindings = engine.load_decay_profile_binding_schemas().unwrap();
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].name, "memory_binding");
    assert_eq!(bindings[0].target_labels, vec!["MemoryEpisode".to_string()]);
    assert_eq!(bindings[0].profile_ref.as_deref(), Some("slow_decay"));

    let err = engine
        .delete_decay_profile_schema("slow_decay", false)
        .unwrap_err();
    assert!(matches!(err, StorageError::KnowledgePolicyInUse(_)));

    engine
        .delete_decay_profile_binding_schema("memory_binding", false)
        .unwrap();
    engine
        .delete_decay_profile_schema("slow_decay", false)
        .unwrap();

    assert!(engine
        .load_decay_profile_binding_schemas()
        .unwrap()
        .is_empty());
    assert!(engine.load_decay_profile_schemas().unwrap().is_empty());
}

#[test]
fn knowledge_policy_access_metadata_roundtrip() {
    let engine = StorageEngine::open_temporary().unwrap();
    let metadata = KnowledgePolicyAccessMetadata {
        last_accessed_at_unix_ms: Some(1_717_171_717_000),
        access_count: 3,
    };

    engine
        .put_knowledge_policy_access_metadata("memory:1", &metadata)
        .unwrap();

    let loaded = engine
        .get_knowledge_policy_access_metadata("memory:1")
        .unwrap()
        .expect("metadata missing");
    assert_eq!(loaded, metadata);

    engine
        .delete_knowledge_policy_access_metadata("memory:1")
        .unwrap();
    assert!(engine
        .get_knowledge_policy_access_metadata("memory:1")
        .unwrap()
        .is_none());
}
