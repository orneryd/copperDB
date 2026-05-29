use super::*;
use copperdb_storage::EdgeRecord;
#[tokio::test]
async fn engine_distributed_direct_path_persists_remote_edge_on_access_metadata() {
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
            "CREATE PROMOTION POLICY edge_access FOR ()-[r:LINKS]-() APPLY { ON ACCESS { SET r.lastAccessedAt = timestamp() SET r.accessCount = coalesce(r.accessCount, 0) + 1 } }",
            Default::default(),
        )
        .unwrap();

    let peer_dir = tempfile::tempdir().unwrap();
    let peer_path = peer_dir.path().join("peer");
    let peer = StorageEngine::open(&peer_path).unwrap();
    peer.put_node_record(&copperdb_storage::NodeRecord {
        id: "Node:A".into(),
        labels: vec!["Node".into()],
        properties: BTreeMap::from([("name".into(), Value::String("a".into()))]),
        created_at_unix_ms: 123,
        updated_at_unix_ms: 123,
    })
    .unwrap();
    peer.put_node_record(&copperdb_storage::NodeRecord {
        id: "Node:B".into(),
        labels: vec!["Node".into()],
        properties: BTreeMap::from([("name".into(), Value::String("b".into()))]),
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
        created_at_unix_ms: 123,
        updated_at_unix_ms: 123,
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

    assert_eq!(outcome.result.rows.len(), 1);
    assert_eq!(outcome.result.rows[0].get("hops"), Some(&Value::from(1)));

    let reopened_peer = StorageEngine::open(&peer_path).unwrap();
    let metadata = reopened_peer
        .get_knowledge_policy_access_metadata("edge:a-b")
        .unwrap()
        .expect("expected replicated edge access metadata");
    assert_eq!(metadata.access_count, 2);
    assert!(metadata.last_accessed_at_unix_ms.is_some());
}

#[tokio::test]
async fn engine_routes_distributed_variable_length_exact_path_query() {
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
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();
    peer_two
        .put_edge_record(&EdgeRecord {
            id: "edge:b-c".into(),
            start_node: "Node:B".into(),
            end_node: "Node:C".into(),
            edge_type: "LINK".into(),
            properties: BTreeMap::from([("rank".into(), Value::from(2))]),
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
            id: "Node:C".into(),
            labels: vec!["Node".into()],
            properties: BTreeMap::from([("name".into(), Value::String("c".into()))]),
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
                "MATCH p = (a:Node {name: 'a'})-[r:LINK*2]->(n:Node {name: 'c'}) RETURN length(p) AS hops, relationships(p) AS rels, p AS path",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

    assert_eq!(outcome.result.rows.len(), 1);
    assert_eq!(outcome.result.rows[0].get("hops"), Some(&Value::from(2)));
    let rels = outcome.result.rows[0]
        .get("rels")
        .and_then(Value::as_array)
        .expect("expected relationship list");
    assert_eq!(rels.len(), 2);
    assert_eq!(rels[0].get("rank"), Some(&Value::from(1)));
    assert_eq!(rels[1].get("rank"), Some(&Value::from(2)));
}

#[tokio::test]
async fn engine_routes_distributed_variable_length_range_path_query() {
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
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();
    peer_two
        .put_edge_record(&EdgeRecord {
            id: "edge:b-c".into(),
            start_node: "Node:B".into(),
            end_node: "Node:C".into(),
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
            id: "Node:C".into(),
            labels: vec!["Node".into()],
            properties: BTreeMap::from([("name".into(), Value::String("c".into()))]),
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
                "MATCH p = (a:Node {name: 'a'})-[:LINK*1..2]->(n:Node) RETURN length(p) AS hops, nodes(p) AS nodes",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

    assert_eq!(outcome.result.rows.len(), 2);
    let mut rows = outcome
        .result
        .rows
        .iter()
        .map(|row| {
            let hops = row.get("hops").and_then(Value::as_i64).unwrap();
            let names = row
                .get("nodes")
                .and_then(Value::as_array)
                .unwrap()
                .iter()
                .map(|node| {
                    node.get("name")
                        .and_then(Value::as_str)
                        .unwrap()
                        .to_string()
                })
                .collect::<Vec<_>>();
            (hops, names)
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|(hops, _)| *hops);
    assert_eq!(rows[0], (1, vec!["a".into(), "b".into()]));
    assert_eq!(rows[1], (2, vec!["a".into(), "b".into(), "c".into()]));
}

#[tokio::test]
async fn engine_routes_distributed_optional_single_node_path_query_hit_and_miss() {
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
            name: "person_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Person".into(),
            properties: vec!["name".into()],
        })
        .unwrap();
    peer_one
        .put_node_record(&copperdb_storage::NodeRecord {
            id: "Person:Alice".into(),
            labels: vec!["Person".into()],
            properties: BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();

    let transport = Arc::new(InMemoryReplicaTransport::new());
    transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

    let hit = db
            .execute_distributed_as(
                "OPTIONAL MATCH p = (n:Person {name: 'Alice'}) RETURN p AS path, nodes(p) AS nodes, relationships(p) AS rels, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport.clone(),
            )
            .await
            .unwrap();
    let miss = db
            .execute_distributed_as(
                "OPTIONAL MATCH p = (n:Person {name: 'Bob'}) RETURN p AS path, nodes(p) AS nodes, relationships(p) AS rels, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

    assert_eq!(hit.result.rows.len(), 1);
    assert_eq!(hit.result.rows[0].get("hops"), Some(&Value::from(0)));
    let hit_nodes = hit.result.rows[0]
        .get("nodes")
        .and_then(Value::as_array)
        .expect("expected optional hit nodes");
    assert_eq!(hit_nodes.len(), 1);
    assert_eq!(
        hit.result.rows[0].get("rels"),
        Some(&Value::Array(Vec::new()))
    );

    assert_eq!(miss.result.rows.len(), 1);
    assert_eq!(miss.result.rows[0].get("path"), Some(&Value::Null));
    assert_eq!(miss.result.rows[0].get("hops"), Some(&Value::Null));
    assert_eq!(
        miss.result.rows[0].get("nodes"),
        Some(&Value::Array(Vec::new()))
    );
    assert_eq!(
        miss.result.rows[0].get("rels"),
        Some(&Value::Array(Vec::new()))
    );
}

#[tokio::test]
async fn engine_routes_distributed_leading_match_optional_path_with_row_preservation() {
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
            name: "seed_id".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Seed".into(),
            properties: vec!["id".into()],
        })
        .unwrap();
    peer_one
        .put_node_record(&copperdb_storage::NodeRecord {
            id: "Seed:1".into(),
            labels: vec!["Seed".into()],
            properties: BTreeMap::from([("id".into(), Value::from(1))]),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();
    peer_one
        .put_node_record(&copperdb_storage::NodeRecord {
            id: "Seed:2".into(),
            labels: vec!["Seed".into()],
            properties: BTreeMap::from([("id".into(), Value::from(2))]),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();
    peer_one
        .persist_index_definition(&copperdb_storage::IndexDefinition {
            name: "person_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Person".into(),
            properties: vec!["name".into()],
        })
        .unwrap();
    peer_one
        .put_node_record(&copperdb_storage::NodeRecord {
            id: "Person:Alice".into(),
            labels: vec!["Person".into()],
            properties: BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();

    let transport = Arc::new(InMemoryReplicaTransport::new());
    transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

    let hit = db
            .execute_distributed_as(
                "MATCH (s:Seed) OPTIONAL MATCH p = (n:Person {name: 'Alice'}) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport.clone(),
            )
            .await
            .unwrap();
    let miss = db
            .execute_distributed_as(
                "MATCH (s:Seed) OPTIONAL MATCH p = (n:Person {name: 'Bob'}) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

    assert_eq!(hit.result.rows.len(), 2);
    assert!(hit
        .result
        .rows
        .iter()
        .all(|row| row.get("hops") == Some(&Value::from(0))));
    assert!(hit
        .result
        .rows
        .iter()
        .all(|row| row.get("path").and_then(Value::as_object).is_some()));

    assert_eq!(miss.result.rows.len(), 2);
    assert!(miss
        .result
        .rows
        .iter()
        .all(|row| row.get("path") == Some(&Value::Null)));
    assert!(miss
        .result
        .rows
        .iter()
        .all(|row| row.get("hops") == Some(&Value::Null)));
}

#[tokio::test]
async fn engine_routes_distributed_leading_match_optional_path_using_bound_node() {
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
            name: "seed_id".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Seed".into(),
            properties: vec!["id".into()],
        })
        .unwrap();
    peer_one
        .persist_index_definition(&copperdb_storage::IndexDefinition {
            name: "person_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Person".into(),
            properties: vec!["name".into()],
        })
        .unwrap();
    peer_one
        .put_node_record(&copperdb_storage::NodeRecord {
            id: "Seed:1".into(),
            labels: vec!["Seed".into()],
            properties: BTreeMap::from([("id".into(), Value::from(1))]),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();
    peer_one
        .put_node_record(&copperdb_storage::NodeRecord {
            id: "Seed:2".into(),
            labels: vec!["Seed".into()],
            properties: BTreeMap::from([("id".into(), Value::from(2))]),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();
    peer_one
        .put_node_record(&copperdb_storage::NodeRecord {
            id: "Person:Alice".into(),
            labels: vec!["Person".into()],
            properties: BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();
    peer_one
        .put_edge_record(&EdgeRecord {
            id: "edge:seed1-alice".into(),
            start_node: "Seed:1".into(),
            end_node: "Person:Alice".into(),
            edge_type: "KNOWS".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();

    let transport = Arc::new(InMemoryReplicaTransport::new());
    transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

    let outcome = db
            .execute_distributed_as(
                "MATCH (s:Seed) OPTIONAL MATCH p = (s)-[:KNOWS]->(n:Person) RETURN p AS path, nodes(p) AS nodes, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

    assert_eq!(outcome.result.rows.len(), 2);
    assert_eq!(
        outcome
            .result
            .rows
            .iter()
            .filter(|row| row.get("path") == Some(&Value::Null))
            .count(),
        1
    );
    assert_eq!(
        outcome
            .result
            .rows
            .iter()
            .filter(|row| row.get("hops") == Some(&Value::from(1)))
            .count(),
        1
    );
    let hit_nodes = outcome
        .result
        .rows
        .iter()
        .find(|row| row.get("hops") == Some(&Value::from(1)))
        .and_then(|row| row.get("nodes"))
        .and_then(Value::as_array)
        .expect("expected bound-path hit nodes");
    assert_eq!(hit_nodes.len(), 2);
}

#[tokio::test]
async fn engine_routes_distributed_multi_match_optional_path_with_bound_node() {
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
            name: "seed_id".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Seed".into(),
            properties: vec!["id".into()],
        })
        .unwrap();
    peer_one
        .persist_index_definition(&copperdb_storage::IndexDefinition {
            name: "tag_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Tag".into(),
            properties: vec!["name".into()],
        })
        .unwrap();
    peer_one
        .persist_index_definition(&copperdb_storage::IndexDefinition {
            name: "person_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Person".into(),
            properties: vec!["name".into()],
        })
        .unwrap();
    for (id, label, properties) in [
        (
            "Seed:1",
            "Seed",
            BTreeMap::from([("id".into(), Value::from(1))]),
        ),
        (
            "Seed:2",
            "Seed",
            BTreeMap::from([("id".into(), Value::from(2))]),
        ),
        (
            "Tag:blue",
            "Tag",
            BTreeMap::from([("name".into(), Value::String("blue".into()))]),
        ),
        (
            "Tag:red",
            "Tag",
            BTreeMap::from([("name".into(), Value::String("red".into()))]),
        ),
        (
            "Person:Alice",
            "Person",
            BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
        ),
    ] {
        peer_one
            .put_node_record(&copperdb_storage::NodeRecord {
                id: id.into(),
                labels: vec![label.into()],
                properties,
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
    }
    peer_one
        .put_edge_record(&EdgeRecord {
            id: "edge:seed1-alice".into(),
            start_node: "Seed:1".into(),
            end_node: "Person:Alice".into(),
            edge_type: "KNOWS".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();

    let transport = Arc::new(InMemoryReplicaTransport::new());
    transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

    let outcome = db
            .execute_distributed_as(
                "MATCH (s:Seed) MATCH (t:Tag) OPTIONAL MATCH p = (s)-[:KNOWS]->(n:Person) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

    assert_eq!(outcome.result.rows.len(), 4);
    assert_eq!(
        outcome
            .result
            .rows
            .iter()
            .filter(|row| row.get("hops") == Some(&Value::from(1)))
            .count(),
        2
    );
    assert_eq!(
        outcome
            .result
            .rows
            .iter()
            .filter(|row| row.get("path") == Some(&Value::Null))
            .count(),
        2
    );
}

#[tokio::test]
async fn engine_routes_distributed_relationship_match_optional_path_with_bound_node() {
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
    for definition in [
        copperdb_storage::IndexDefinition {
            name: "seed_id".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Seed".into(),
            properties: vec!["id".into()],
        },
        copperdb_storage::IndexDefinition {
            name: "tag_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Tag".into(),
            properties: vec!["name".into()],
        },
        copperdb_storage::IndexDefinition {
            name: "person_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Person".into(),
            properties: vec!["name".into()],
        },
    ] {
        peer_one.persist_index_definition(&definition).unwrap();
    }
    for (id, label, properties) in [
        (
            "Seed:1",
            "Seed",
            BTreeMap::from([("id".into(), Value::from(1))]),
        ),
        (
            "Seed:2",
            "Seed",
            BTreeMap::from([("id".into(), Value::from(2))]),
        ),
        (
            "Tag:blue",
            "Tag",
            BTreeMap::from([("name".into(), Value::String("blue".into()))]),
        ),
        (
            "Tag:red",
            "Tag",
            BTreeMap::from([("name".into(), Value::String("red".into()))]),
        ),
        (
            "Person:Alice",
            "Person",
            BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
        ),
    ] {
        peer_one
            .put_node_record(&copperdb_storage::NodeRecord {
                id: id.into(),
                labels: vec![label.into()],
                properties,
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
    }
    for edge in [
        EdgeRecord {
            id: "edge:seed1-blue".into(),
            start_node: "Seed:1".into(),
            end_node: "Tag:blue".into(),
            edge_type: "TAGGED".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
        EdgeRecord {
            id: "edge:seed2-red".into(),
            start_node: "Seed:2".into(),
            end_node: "Tag:red".into(),
            edge_type: "TAGGED".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
        EdgeRecord {
            id: "edge:seed1-alice".into(),
            start_node: "Seed:1".into(),
            end_node: "Person:Alice".into(),
            edge_type: "KNOWS".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
    ] {
        peer_one.put_edge_record(&edge).unwrap();
    }

    let transport = Arc::new(InMemoryReplicaTransport::new());
    transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

    let outcome = db
            .execute_distributed_as(
                "MATCH (s:Seed)-[:TAGGED]->(t:Tag) OPTIONAL MATCH p = (s)-[:KNOWS]->(n:Person) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

    assert_eq!(outcome.result.rows.len(), 2);
    assert_eq!(
        outcome
            .result
            .rows
            .iter()
            .filter(|row| row.get("hops") == Some(&Value::from(1)))
            .count(),
        1
    );
    assert_eq!(
        outcome
            .result
            .rows
            .iter()
            .filter(|row| row.get("path") == Some(&Value::Null))
            .count(),
        1
    );
}

#[tokio::test]
async fn engine_routes_distributed_mixed_prefix_optional_path_with_bound_node() {
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
    for definition in [
        copperdb_storage::IndexDefinition {
            name: "seed_id".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Seed".into(),
            properties: vec!["id".into()],
        },
        copperdb_storage::IndexDefinition {
            name: "tag_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Tag".into(),
            properties: vec!["name".into()],
        },
        copperdb_storage::IndexDefinition {
            name: "person_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Person".into(),
            properties: vec!["name".into()],
        },
    ] {
        peer_one.persist_index_definition(&definition).unwrap();
    }
    for (id, label, properties) in [
        (
            "Seed:1",
            "Seed",
            BTreeMap::from([("id".into(), Value::from(1))]),
        ),
        (
            "Seed:2",
            "Seed",
            BTreeMap::from([("id".into(), Value::from(2))]),
        ),
        (
            "Tag:blue",
            "Tag",
            BTreeMap::from([("name".into(), Value::String("blue".into()))]),
        ),
        (
            "Tag:red",
            "Tag",
            BTreeMap::from([("name".into(), Value::String("red".into()))]),
        ),
        (
            "Person:Alice",
            "Person",
            BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
        ),
    ] {
        peer_one
            .put_node_record(&copperdb_storage::NodeRecord {
                id: id.into(),
                labels: vec![label.into()],
                properties,
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
    }
    for edge in [
        EdgeRecord {
            id: "edge:seed1-blue".into(),
            start_node: "Seed:1".into(),
            end_node: "Tag:blue".into(),
            edge_type: "TAGGED".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
        EdgeRecord {
            id: "edge:seed2-red".into(),
            start_node: "Seed:2".into(),
            end_node: "Tag:red".into(),
            edge_type: "TAGGED".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
        EdgeRecord {
            id: "edge:seed1-alice".into(),
            start_node: "Seed:1".into(),
            end_node: "Person:Alice".into(),
            edge_type: "KNOWS".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
    ] {
        peer_one.put_edge_record(&edge).unwrap();
    }

    let transport = Arc::new(InMemoryReplicaTransport::new());
    transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

    let outcome = db
            .execute_distributed_as(
                "MATCH (s:Seed) MATCH (s)-[:TAGGED]->(t:Tag) OPTIONAL MATCH p = (s)-[:KNOWS]->(n:Person) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

    assert_eq!(outcome.result.rows.len(), 2);
    assert_eq!(
        outcome
            .result
            .rows
            .iter()
            .filter(|row| row.get("hops") == Some(&Value::from(1)))
            .count(),
        1
    );
    assert_eq!(
        outcome
            .result
            .rows
            .iter()
            .filter(|row| row.get("path") == Some(&Value::Null))
            .count(),
        1
    );
}

#[tokio::test]
async fn engine_routes_distributed_variable_length_relationship_prefix_optional_path() {
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
    for definition in [
        copperdb_storage::IndexDefinition {
            name: "seed_id".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Seed".into(),
            properties: vec!["id".into()],
        },
        copperdb_storage::IndexDefinition {
            name: "tag_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Tag".into(),
            properties: vec!["name".into()],
        },
        copperdb_storage::IndexDefinition {
            name: "person_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Person".into(),
            properties: vec!["name".into()],
        },
    ] {
        peer_one.persist_index_definition(&definition).unwrap();
    }
    for (id, labels, properties) in [
        (
            "Seed:1",
            vec!["Seed"],
            BTreeMap::from([("id".into(), Value::from(1))]),
        ),
        (
            "Seed:2",
            vec!["Seed"],
            BTreeMap::from([("id".into(), Value::from(2))]),
        ),
        (
            "Hop:mid",
            vec!["Hop"],
            BTreeMap::from([("name".into(), Value::String("mid".into()))]),
        ),
        (
            "Tag:blue",
            vec!["Tag"],
            BTreeMap::from([("name".into(), Value::String("blue".into()))]),
        ),
        (
            "Tag:red",
            vec!["Tag"],
            BTreeMap::from([("name".into(), Value::String("red".into()))]),
        ),
        (
            "Person:Alice",
            vec!["Person"],
            BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
        ),
    ] {
        peer_one
            .put_node_record(&copperdb_storage::NodeRecord {
                id: id.into(),
                labels: labels.into_iter().map(String::from).collect(),
                properties,
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
    }
    for edge in [
        EdgeRecord {
            id: "edge:seed1-mid".into(),
            start_node: "Seed:1".into(),
            end_node: "Hop:mid".into(),
            edge_type: "TAGGED".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
        EdgeRecord {
            id: "edge:mid-blue".into(),
            start_node: "Hop:mid".into(),
            end_node: "Tag:blue".into(),
            edge_type: "TAGGED".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
        EdgeRecord {
            id: "edge:seed2-red".into(),
            start_node: "Seed:2".into(),
            end_node: "Tag:red".into(),
            edge_type: "TAGGED".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
        EdgeRecord {
            id: "edge:seed1-alice".into(),
            start_node: "Seed:1".into(),
            end_node: "Person:Alice".into(),
            edge_type: "KNOWS".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
    ] {
        peer_one.put_edge_record(&edge).unwrap();
    }

    let transport = Arc::new(InMemoryReplicaTransport::new());
    transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

    let outcome = db
            .execute_distributed_as(
                "MATCH (s:Seed)-[:TAGGED*1..2]->(t:Tag) OPTIONAL MATCH p = (s)-[:KNOWS]->(n:Person) RETURN p AS path, length(p) AS hops",
                HashMap::new(),
                &["admin".into()],
                &placement,
                ConsistencyLevel::One,
                None,
                transport,
            )
            .await
            .unwrap();

    assert_eq!(outcome.result.rows.len(), 2);
    assert_eq!(
        outcome
            .result
            .rows
            .iter()
            .filter(|row| row.get("hops") == Some(&Value::from(1)))
            .count(),
        1
    );
    assert_eq!(
        outcome
            .result
            .rows
            .iter()
            .filter(|row| row.get("path") == Some(&Value::Null))
            .count(),
        1
    );
}

#[tokio::test]
async fn engine_routes_distributed_where_filtered_prefix_optional_path() {
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
    for definition in [
        copperdb_storage::IndexDefinition {
            name: "seed_id".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Seed".into(),
            properties: vec!["id".into()],
        },
        copperdb_storage::IndexDefinition {
            name: "tag_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Tag".into(),
            properties: vec!["name".into()],
        },
        copperdb_storage::IndexDefinition {
            name: "person_name".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            kind: copperdb_storage::IndexKind::Range,
            label: "Person".into(),
            properties: vec!["name".into()],
        },
    ] {
        peer_one.persist_index_definition(&definition).unwrap();
    }
    for (id, label, properties) in [
        (
            "Seed:1",
            "Seed",
            BTreeMap::from([("id".into(), Value::from(1))]),
        ),
        (
            "Seed:2",
            "Seed",
            BTreeMap::from([("id".into(), Value::from(2))]),
        ),
        (
            "Tag:blue",
            "Tag",
            BTreeMap::from([("name".into(), Value::String("blue".into()))]),
        ),
        (
            "Tag:red",
            "Tag",
            BTreeMap::from([("name".into(), Value::String("red".into()))]),
        ),
        (
            "Person:Alice",
            "Person",
            BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
        ),
    ] {
        peer_one
            .put_node_record(&copperdb_storage::NodeRecord {
                id: id.into(),
                labels: vec![label.into()],
                properties,
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
    }
    for edge in [
        EdgeRecord {
            id: "edge:seed1-blue".into(),
            start_node: "Seed:1".into(),
            end_node: "Tag:blue".into(),
            edge_type: "TAGGED".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
        EdgeRecord {
            id: "edge:seed2-red".into(),
            start_node: "Seed:2".into(),
            end_node: "Tag:red".into(),
            edge_type: "TAGGED".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
        EdgeRecord {
            id: "edge:seed1-alice".into(),
            start_node: "Seed:1".into(),
            end_node: "Person:Alice".into(),
            edge_type: "KNOWS".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
    ] {
        peer_one.put_edge_record(&edge).unwrap();
    }

    let transport = Arc::new(InMemoryReplicaTransport::new());
    transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

    let outcome = db
            .execute_distributed_as(
                "MATCH (s:Seed) WHERE s.id = 1 MATCH (s)-[:TAGGED]->(t:Tag) WHERE t.name = 'blue' OPTIONAL MATCH p = (s)-[:KNOWS]->(n:Person) RETURN p AS path, length(p) AS hops",
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
    assert_eq!(outcome.result.rows[0].get("hops"), Some(&Value::from(1)));
}
