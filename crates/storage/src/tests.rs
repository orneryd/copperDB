use super::*;
use copperdb_kms::{LocalKms, LocalKmsConfig};
use serde_json::json;
use std::fs;

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

    let namespaces = engine.list_namespaces().unwrap();
    assert_eq!(namespaces, vec!["alpha".to_string(), "beta".to_string()]);
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
