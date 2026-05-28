use super::*;
use copperdb_storage::EdgeRecord;
    #[tokio::test]
    async fn engine_routes_distributed_where_filtered_prefix_match_path() {
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
                "MATCH (s:Seed) WHERE s.id = 1 MATCH (s)-[:TAGGED]->(t:Tag) WHERE t.name = 'blue' MATCH p = (s)-[:KNOWS]->(n:Person) RETURN p AS path, length(p) AS hops",
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

    #[tokio::test]
    async fn engine_routes_distributed_with_prefix_match_path() {
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
                "MATCH (s:Seed) WITH s AS seed WHERE seed.id = 1 MATCH p = (seed)-[:KNOWS]->(n:Person) RETURN p AS path, length(p) AS hops",
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

    #[tokio::test]
    async fn engine_routes_distributed_optional_prefix_miss_match_path() {
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
                "Tag:blue",
                "Tag",
                BTreeMap::from([("name".into(), Value::String("blue".into()))]),
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
                "MATCH (s:Seed) WHERE s.id = 1 OPTIONAL MATCH (s)-[:TAGGED]->(t:Tag) MATCH p = (s)-[:KNOWS]->(n:Person) RETURN p AS path, length(p) AS hops",
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

    #[tokio::test]
    async fn engine_routes_distributed_edge_variable_filtered_prefix_optional_path() {
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
                properties: BTreeMap::from([("weight".into(), Value::from(1))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "edge:seed2-red".into(),
                start_node: "Seed:2".into(),
                end_node: "Tag:red".into(),
                edge_type: "TAGGED".into(),
                properties: BTreeMap::from([("weight".into(), Value::from(2))]),
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
                "MATCH (s:Seed)-[r:TAGGED]->(t:Tag) WHERE r.weight = 1 OPTIONAL MATCH p = (s)-[:KNOWS]->(n:Person) RETURN p AS path, length(p) AS hops",
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

    #[tokio::test]
    async fn engine_distributed_bfs_traverses_mesh_peers_and_returns_path() {
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
            .put_node("Node:A", &graph_node("Node:A", "A"))
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
            .put_node("Node:B", &graph_node("Node:B", "B"))
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
            .put_node("Node:C", &graph_node("Node:C", "C"))
            .unwrap();
        peer_three
            .put_node("Node:D", &graph_node("Node:D", "D"))
            .unwrap();
        peer_three
            .put_edge_record(&EdgeRecord {
                id: "edge:c-d".into(),
                start_node: "Node:C".into(),
                end_node: "Node:D".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
        transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));
        transport.register("node-3", Arc::new(StorageEngineAdapter::new(peer_three)));

        let outcome = db
            .distributed_bfs_path_as(
                "Node:A",
                "Node:D",
                Some("LINK"),
                EdgeDirection::Outgoing,
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(outcome.plan.required_responses, 2);
        assert_eq!(outcome.responded_by.len(), 3);
        assert!(outcome.failed_replicas.is_empty());
        assert_eq!(
            outcome.path,
            Some(DistributedPath {
                node_ids: vec![
                    "Node:A".into(),
                    "Node:B".into(),
                    "Node:C".into(),
                    "Node:D".into()
                ],
                edge_ids: vec!["edge:a-b".into(), "edge:b-c".into(), "edge:c-d".into()],
            })
        );
    }

    #[tokio::test]
    async fn engine_distributed_bfs_prefers_shortest_path_across_mesh_peers() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        fn graph_node(id: &str) -> Vec<u8> {
            rmp_serde::to_vec(&BTreeMap::from([
                ("_id".to_string(), Value::String(id.to_string())),
                (
                    "_labels".to_string(),
                    Value::Array(vec![Value::String("Node".into())]),
                ),
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
        for node_id in ["Node:A", "Node:B"] {
            peer_one.put_node(node_id, &graph_node(node_id)).unwrap();
        }
        for edge in [EdgeRecord {
            id: "edge:a-b".into(),
            start_node: "Node:A".into(),
            end_node: "Node:B".into(),
            edge_type: "LINK".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        }] {
            peer_one.put_edge_record(&edge).unwrap();
        }

        let peer_two = StorageEngine::open_temporary().unwrap();
        for node_id in ["Node:C", "Node:D"] {
            peer_two.put_node(node_id, &graph_node(node_id)).unwrap();
        }
        for edge in [
            EdgeRecord {
                id: "edge:b-d".into(),
                start_node: "Node:B".into(),
                end_node: "Node:D".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "edge:a-c".into(),
                start_node: "Node:A".into(),
                end_node: "Node:C".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
        ] {
            peer_two.put_edge_record(&edge).unwrap();
        }

        let peer_three = StorageEngine::open_temporary().unwrap();
        for node_id in ["Node:E"] {
            peer_three.put_node(node_id, &graph_node(node_id)).unwrap();
        }
        for edge in [
            EdgeRecord {
                id: "edge:c-e".into(),
                start_node: "Node:C".into(),
                end_node: "Node:E".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "edge:e-d".into(),
                start_node: "Node:E".into(),
                end_node: "Node:D".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
        ] {
            peer_three.put_edge_record(&edge).unwrap();
        }

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
        transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));
        transport.register("node-3", Arc::new(StorageEngineAdapter::new(peer_three)));

        let outcome = db
            .distributed_bfs_path_as(
                "Node:A",
                "Node:D",
                Some("LINK"),
                EdgeDirection::Outgoing,
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(
            outcome.path,
            Some(DistributedPath {
                node_ids: vec!["Node:A".into(), "Node:B".into(), "Node:D".into()],
                edge_ids: vec!["edge:a-b".into(), "edge:b-d".into()],
            })
        );
    }

    #[tokio::test]
    async fn engine_distributed_bfs_returns_none_when_mesh_has_no_path() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        fn graph_node(id: &str) -> Vec<u8> {
            rmp_serde::to_vec(&BTreeMap::from([
                ("_id".to_string(), Value::String(id.to_string())),
                (
                    "_labels".to_string(),
                    Value::Array(vec![Value::String("Node".into())]),
                ),
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
        peer_one.put_node("Node:A", &graph_node("Node:A")).unwrap();
        peer_one.put_node("Node:B", &graph_node("Node:B")).unwrap();
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
        peer_two.put_node("Node:C", &graph_node("Node:C")).unwrap();

        let peer_three = StorageEngine::open_temporary().unwrap();
        peer_three
            .put_node("Node:D", &graph_node("Node:D"))
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
        transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));
        transport.register("node-3", Arc::new(StorageEngineAdapter::new(peer_three)));

        let outcome = db
            .distributed_bfs_path_as(
                "Node:A",
                "Node:D",
                Some("LINK"),
                EdgeDirection::Outgoing,
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(outcome.plan.required_responses, 2);
        assert_eq!(outcome.responded_by.len(), 3);
        assert!(outcome.failed_replicas.is_empty());
        assert!(outcome.path.is_none());
    }

    #[tokio::test]
    async fn engine_distributed_bfs_requires_mesh_read_quorum() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        fn graph_node(id: &str) -> Vec<u8> {
            rmp_serde::to_vec(&BTreeMap::from([
                ("_id".to_string(), Value::String(id.to_string())),
                (
                    "_labels".to_string(),
                    Value::Array(vec![Value::String("Node".into())]),
                ),
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
        peer_one.put_node("Node:A", &graph_node("Node:A")).unwrap();
        peer_one.put_node("Node:D", &graph_node("Node:D")).unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));

        let err = db
            .distributed_bfs_path_as(
                "Node:A",
                "Node:D",
                Some("LINK"),
                EdgeDirection::Outgoing,
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap_err();

        match err {
            CopperDbError::Replication(message) => {
                assert!(message.contains("quorum not reached"));
                assert!(message.contains("required 2"));
            }
            other => panic!("expected replication quorum error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn engine_distributed_bfs_traverses_incoming_edges_across_mesh_peers() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        fn graph_node(id: &str) -> Vec<u8> {
            rmp_serde::to_vec(&BTreeMap::from([
                ("_id".to_string(), Value::String(id.to_string())),
                (
                    "_labels".to_string(),
                    Value::Array(vec![Value::String("Node".into())]),
                ),
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
        peer_one.put_node("Node:A", &graph_node("Node:A")).unwrap();
        peer_one.put_node("Node:B", &graph_node("Node:B")).unwrap();
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
        peer_two.put_node("Node:C", &graph_node("Node:C")).unwrap();
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
            .put_node("Node:D", &graph_node("Node:D"))
            .unwrap();
        peer_three
            .put_edge_record(&EdgeRecord {
                id: "edge:c-d".into(),
                start_node: "Node:C".into(),
                end_node: "Node:D".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
        transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));
        transport.register("node-3", Arc::new(StorageEngineAdapter::new(peer_three)));

        let outcome = db
            .distributed_bfs_path_as(
                "Node:D",
                "Node:A",
                Some("LINK"),
                EdgeDirection::Incoming,
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(
            outcome.path,
            Some(DistributedPath {
                node_ids: vec![
                    "Node:D".into(),
                    "Node:C".into(),
                    "Node:B".into(),
                    "Node:A".into()
                ],
                edge_ids: vec!["edge:c-d".into(), "edge:b-c".into(), "edge:a-b".into()],
            })
        );
    }

    #[tokio::test]
    async fn engine_distributed_bfs_traverses_undirected_edges_across_mesh_peers() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        fn graph_node(id: &str) -> Vec<u8> {
            rmp_serde::to_vec(&BTreeMap::from([
                ("_id".to_string(), Value::String(id.to_string())),
                (
                    "_labels".to_string(),
                    Value::Array(vec![Value::String("Node".into())]),
                ),
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
        peer_one.put_node("Node:A", &graph_node("Node:A")).unwrap();
        peer_one.put_node("Node:B", &graph_node("Node:B")).unwrap();
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
        peer_two.put_node("Node:C", &graph_node("Node:C")).unwrap();
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
            .put_node("Node:D", &graph_node("Node:D"))
            .unwrap();
        peer_three
            .put_edge_record(&EdgeRecord {
                id: "edge:c-d".into(),
                start_node: "Node:C".into(),
                end_node: "Node:D".into(),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
        transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));
        transport.register("node-3", Arc::new(StorageEngineAdapter::new(peer_three)));

        let outcome = db
            .distributed_bfs_path_as(
                "Node:D",
                "Node:A",
                Some("LINK"),
                EdgeDirection::Both,
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(
            outcome.path,
            Some(DistributedPath {
                node_ids: vec![
                    "Node:D".into(),
                    "Node:C".into(),
                    "Node:B".into(),
                    "Node:A".into()
                ],
                edge_ids: vec!["edge:c-d".into(), "edge:b-c".into(), "edge:a-b".into()],
            })
        );
    }

    #[tokio::test]
    async fn engine_distributed_bfs_query_materializes_path_row() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        fn graph_node(id: &str) -> Vec<u8> {
            rmp_serde::to_vec(&BTreeMap::from([
                ("_id".to_string(), Value::String(id.to_string())),
                (
                    "_labels".to_string(),
                    Value::Array(vec![Value::String("Node".into())]),
                ),
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
        peer_one.put_node("Node:A", &graph_node("Node:A")).unwrap();
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
        peer_two.put_node("Node:B", &graph_node("Node:B")).unwrap();
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
            .put_node("Node:C", &graph_node("Node:C"))
            .unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
        transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));
        transport.register("node-3", Arc::new(StorageEngineAdapter::new(peer_three)));

        let (result, bfs) = db
            .distributed_bfs_query_as(
                "Node:A",
                "Node:C",
                Some("LINK"),
                EdgeDirection::Outgoing,
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

        assert_eq!(
            result.columns,
            vec!["path", "nodes(path)", "relationships(path)", "length(path)"]
        );
        assert_eq!(result.rows.len(), 1);
        let nodes = result.rows[0]
            .get("nodes(path)")
            .and_then(Value::as_array)
            .expect("expected materialized path nodes");
        assert_eq!(nodes.len(), 3);
        assert_eq!(nodes[0].get("_id"), Some(&Value::String("Node:A".into())));
        assert_eq!(nodes[1].get("_id"), Some(&Value::String("Node:B".into())));
        assert_eq!(nodes[2].get("_id"), Some(&Value::String("Node:C".into())));
        let rels = result.rows[0]
            .get("relationships(path)")
            .and_then(Value::as_array)
            .expect("expected materialized path relationships");
        assert_eq!(rels.len(), 2);
        assert_eq!(rels[0].get("_id"), Some(&Value::String("edge:a-b".into())));
        assert_eq!(rels[1].get("_id"), Some(&Value::String("edge:b-c".into())));
        assert_eq!(result.rows[0].get("length(path)"), Some(&Value::from(2)));
        let path = result.rows[0]
            .get("path")
            .and_then(Value::as_object)
            .expect("expected path object");
        assert_eq!(path.get("length"), Some(&Value::from(2)));
        assert!(bfs.path.is_some());
    }

    #[tokio::test]
    async fn engine_distributed_bfs_query_returns_empty_rows_when_no_path_exists() {
        use copperdb_replication::{InMemoryReplicaTransport, StorageEngineAdapter};
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        fn graph_node(id: &str) -> Vec<u8> {
            rmp_serde::to_vec(&BTreeMap::from([
                ("_id".to_string(), Value::String(id.to_string())),
                (
                    "_labels".to_string(),
                    Value::Array(vec![Value::String("Node".into())]),
                ),
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
        peer_one.put_node("Node:A", &graph_node("Node:A")).unwrap();
        let peer_two = StorageEngine::open_temporary().unwrap();
        peer_two.put_node("Node:C", &graph_node("Node:C")).unwrap();
        let peer_three = StorageEngine::open_temporary().unwrap();

        let transport = Arc::new(InMemoryReplicaTransport::new());
        transport.register("node-1", Arc::new(StorageEngineAdapter::new(peer_one)));
        transport.register("node-2", Arc::new(StorageEngineAdapter::new(peer_two)));
        transport.register("node-3", Arc::new(StorageEngineAdapter::new(peer_three)));

        let (result, bfs) = db
            .distributed_bfs_query_as(
                "Node:A",
                "Node:C",
                Some("LINK"),
                EdgeDirection::Outgoing,
                &placement,
                ConsistencyLevel::Quorum,
                None,
                transport,
            )
            .await
            .unwrap();

        assert!(bfs.path.is_none());
        assert_eq!(
            result.columns,
            vec!["path", "nodes(path)", "relationships(path)", "length(path)"]
        );
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_multiple_creates_and_match() {
        let db = CopperDb::open_temporary().unwrap();
        for i in 0..5 {
            db.execute(
                &format!("CREATE (n:Item {{idx: {i}}})", i = i),
                Default::default(),
            )
            .unwrap();
        }
        let result = db
            .execute("MATCH (n:Item) RETURN n", Default::default())
            .unwrap();
        assert_eq!(result.rows.len(), 5);
    }

#[cfg(test)]
mod smoke_tests {
    use super::*;
    use serde_json::Value;
    use std::collections::HashMap;

    /// Smoke: create a node, flush to disk, reopen the DB, verify node persists.
    #[test]
    fn test_node_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap().to_string();

        // Phase 1: write
        {
            let cfg = DatabaseConfig {
                data_dir: path.clone(),
                ..Default::default()
            };
            let db = CopperDb::open(cfg).unwrap();
            let result = db
                .execute(
                    "CREATE (n:Person {name: 'Alice', age: 30}) RETURN n",
                    HashMap::new(),
                )
                .unwrap();
            assert_eq!(
                result.stats.nodes_created, 1,
                "should create exactly 1 node"
            );
            db.flush().unwrap();
        }

        // Phase 2: reopen and verify
        {
            let cfg = DatabaseConfig {
                data_dir: path.clone(),
                ..Default::default()
            };
            let db = CopperDb::open(cfg).unwrap();
            let result = db
                .execute("MATCH (n:Person) RETURN n", HashMap::new())
                .unwrap();
            assert_eq!(
                result.rows.len(),
                1,
                "reopened DB should have 1 Person node"
            );
            let row = &result.rows[0];
            let n = row.get("n").expect("row must have 'n' key");
            match n {
                Value::Object(props) => {
                    assert_eq!(
                        props.get("name"),
                        Some(&Value::String("Alice".into())),
                        "name must be Alice"
                    );
                    assert_eq!(
                        props.get("age"),
                        Some(&Value::Number(30.into())),
                        "age must be 30"
                    );
                }
                _ => panic!("expected object node, got {n:?}"),
            }
        }
    }

    /// Smoke: create multiple nodes and verify everything persists.
    #[test]
    fn test_edge_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap().to_string();

        {
            let cfg = DatabaseConfig {
                data_dir: path.clone(),
                ..Default::default()
            };
            let db = CopperDb::open(cfg).unwrap();
            db.execute(
                "CREATE (a:City {name: 'London', pop: 9000000})",
                HashMap::new(),
            )
            .unwrap();
            db.execute(
                "CREATE (b:City {name: 'Paris', pop: 2100000})",
                HashMap::new(),
            )
            .unwrap();
            let r = db
                .execute("MATCH (c:City) RETURN c", HashMap::new())
                .unwrap();
            assert_eq!(r.rows.len(), 2, "should have 2 City nodes before flush");
            db.flush().unwrap();
        }

        {
            let cfg = DatabaseConfig {
                data_dir: path.clone(),
                ..Default::default()
            };
            let db = CopperDb::open(cfg).unwrap();
            let result = db
                .execute("MATCH (c:City) RETURN c", HashMap::new())
                .unwrap();
            assert_eq!(
                result.rows.len(),
                2,
                "should still have 2 City nodes after reopen"
            );

            let mut names: Vec<String> = result
                .rows
                .iter()
                .filter_map(|row| {
                    row.get("c")
                        .and_then(|v| v.as_object())
                        .and_then(|o| o.get("name"))
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_string())
                })
                .collect();
            names.sort();
            assert_eq!(
                names,
                vec!["London", "Paris"],
                "both cities must be present"
            );
        }
    }

    /// Smoke: MATCH/WHERE filter works after disk round-trip.
    #[test]
    fn test_where_filter_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap().to_string();

        {
            let cfg = DatabaseConfig {
                data_dir: path.clone(),
                ..Default::default()
            };
            let db = CopperDb::open(cfg).unwrap();
            for (name, age) in &[("Alice", 30), ("Bob", 20), ("Carol", 35)] {
                db.execute(
                    &format!("CREATE (n:User {{name: '{name}', age: {age}}})"),
                    HashMap::new(),
                )
                .unwrap();
            }
            db.flush().unwrap();
        }

        {
            let cfg = DatabaseConfig {
                data_dir: path.clone(),
                ..Default::default()
            };
            let db = CopperDb::open(cfg).unwrap();
            let result = db
                .execute("MATCH (n:User) WHERE n.age > 25 RETURN n", HashMap::new())
                .unwrap();
            assert_eq!(
                result.rows.len(),
                2,
                "Alice (30) and Carol (35) should match age > 25"
            );

            let mut names: Vec<String> = result
                .rows
                .iter()
                .filter_map(|row| {
                    row.get("n")
                        .and_then(|v| v.as_object())
                        .and_then(|o| o.get("name"))
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_string())
                })
                .collect();
            names.sort();
            assert_eq!(names, vec!["Alice", "Carol"]);
        }
    }

    /// Smoke: the REST API layer (axum) responds correctly.
    #[tokio::test]
    async fn test_rest_api_health_check() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let state = Arc::new(copperdb_server::AppState::default());
        let app = copperdb_server::build_router(Arc::clone(&state));

        let req = Request::builder()
            .method("GET")
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "health check should return 200"
        );

        let body_bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let health: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(health["status"], "ok", "health status should be 'ok'");
    }
}
