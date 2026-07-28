use super::*;
use copperdb_storage::{EdgeRecord, MvccPruneOptions, NodeRecord};
use copperdb_txsession::{BookmarkMode, SessionConfig};
use std::collections::BTreeMap;

// ── Legacy tests ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_start_with_default_config() {
    let config = copperdb_config::Config::default();
    let db = CopperDbServer::start(config).await.unwrap();
    db.shutdown().await.unwrap();
}

// ── Embedded copperdb tests ───────────────────────────────────────────────

#[test]
fn test_open_temporary() {
    let db = CopperDb::open_temporary().unwrap();
    assert_eq!(db.config.default_database, "copperdb");
}

#[test]
fn test_begin_transaction_exposes_effective_read_fence() {
    let db = CopperDb::open_temporary().unwrap();
    let bookmark = LogicalTransactionId::new(7, 41, 9).stable_id();
    let config = SessionConfig {
        database: Some("copperdb".to_string()),
        bookmarks: vec![bookmark],
        bookmark_mode: BookmarkMode::Required,
        ..SessionConfig::default()
    };

    let transaction_id = db.begin_transaction(&config).unwrap();
    let read_fence = db.transaction_read_fence(&transaction_id).unwrap();

    assert!(read_fence > LogicalTransactionId::new(7, 41, 9));
    assert_eq!(
        db.tx_manager().get(&transaction_id).unwrap().read_fence(),
        read_fence
    );
}

#[test]
fn test_begin_storage_transaction_uses_owned_storage_context() {
    let db = CopperDb::open_temporary().unwrap();
    let mut transaction = db.begin_storage_transaction();
    transaction.put_node_record(NodeRecord {
        id: "n1".to_string(),
        labels: vec!["Person".to_string()],
        properties: Default::default(),
        named_embeddings: Default::default(),
        chunk_embeddings: Vec::new(),
        embed_meta: Default::default(),
        created_at_unix_ms: 1,
        updated_at_unix_ms: 1,
    });

    assert!(transaction.get_node_record("n1").unwrap().is_some());
    assert!(db.storage().get_node_record("n1").unwrap().is_none());
    transaction.commit().unwrap();
    assert!(db.storage().get_node_record("n1").unwrap().is_some());
}

#[test]
fn test_create_and_match() {
    let db = CopperDb::open_temporary().unwrap();

    let result = db
        .execute(
            "CREATE (n:Person {name: 'Alice', age: 30})",
            Default::default(),
        )
        .unwrap();
    assert_eq!(result.stats.nodes_created, 1);

    let result = db
        .execute("MATCH (n:Person) RETURN n", Default::default())
        .unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn test_match_with_where() {
    let db = CopperDb::open_temporary().unwrap();
    db.execute(
        "CREATE (n:Person {name: 'Alice', age: 30})",
        Default::default(),
    )
    .unwrap();
    db.execute(
        "CREATE (n:Person {name: 'Bob', age: 25})",
        Default::default(),
    )
    .unwrap();

    let result = db
        .execute(
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n",
            Default::default(),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    if let Some(Value::Object(props)) = result.rows[0].get("n") {
        assert_eq!(props.get("name"), Some(&Value::String("Alice".into())));
    } else {
        panic!("expected object with n");
    }
}

#[test]
fn test_optional_match_relationship_pattern_preserves_row_with_nulls() {
    let db = CopperDb::open_temporary().unwrap();
    db.execute(
        "CREATE (p:Person {id: 1, name: 'Alice'})",
        Default::default(),
    )
    .unwrap();

    let result = db
            .execute(
                "MATCH (p:Person {id: 1}) OPTIONAL MATCH (p)-[r:FOLLOWS]->(friend:Person) RETURN p.name AS person, friend AS friend, r AS rel",
                Default::default(),
            )
            .unwrap();

    assert_eq!(result.columns, vec!["person", "friend", "rel"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0].get("person"),
        Some(&Value::String("Alice".into()))
    );
    assert_eq!(result.rows[0].get("friend"), Some(&Value::Null));
    assert_eq!(result.rows[0].get("rel"), Some(&Value::Null));
}

#[test]
fn test_optional_match_relationship_pattern_returns_bound_values_on_match() {
    let db = CopperDb::open_temporary().unwrap();
    for cypher in [
        "CREATE (p:Person {id: 1, name: 'Alice'})",
        "CREATE (p:Person {id: 2, name: 'Bob'})",
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:FOLLOWS]->(b)",
    ] {
        db.execute(cypher, Default::default()).unwrap();
    }

    let result = db
            .execute(
                "MATCH (p:Person {id: 1}) OPTIONAL MATCH (p)-[r:FOLLOWS]->(friend:Person) RETURN p.name AS person, friend.name AS friendName, r._type AS relType",
                Default::default(),
            )
            .unwrap();

    assert_eq!(result.columns, vec!["person", "friendName", "relType"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0].get("person"),
        Some(&Value::String("Alice".into()))
    );
    assert_eq!(
        result.rows[0].get("friendName"),
        Some(&Value::String("Bob".into()))
    );
    assert_eq!(
        result.rows[0].get("relType"),
        Some(&Value::String("FOLLOWS".into()))
    );
}

#[test]
fn test_flush_and_size() {
    let db = CopperDb::open_temporary().unwrap();
    db.execute("CREATE (n:Test {x: 1})", Default::default())
        .unwrap();
    db.flush().unwrap();
    // size should be non-zero after flush
    let _ = db.size_on_disk();
}

#[test]
fn test_query_caching() {
    let db = CopperDb::open_temporary().unwrap();
    db.execute("CREATE (n:Cached {v: 1})", Default::default())
        .unwrap();
    // Second identical query hits cache
    let r1 = db
        .execute("MATCH (n:Cached) RETURN n", Default::default())
        .unwrap();
    let r2 = db
        .execute("MATCH (n:Cached) RETURN n", Default::default())
        .unwrap();
    assert_eq!(r1.rows.len(), r2.rows.len());
}

#[test]
fn test_local_fulltext_search_uses_catalogued_fulltext_index() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = DatabaseConfig {
        data_dir: dir.path().join("db").to_string_lossy().into_owned(),
        ..Default::default()
    };
    config.runtime_config.bm25_enabled = true;

    let db = CopperDb::open(config).unwrap();
    db.storage()
        .persist_index_definition(&copperdb_storage::IndexDefinition {
            name: "person_bio_fulltext_idx".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            label: "Person".into(),
            properties: vec!["bio".into(), "tags".into()],
            kind: copperdb_storage::IndexKind::FullText,
        })
        .unwrap();
    db.storage()
        .put_node_record(&copperdb_storage::NodeRecord {
            id: "person:1".into(),
            labels: vec!["Person".into()],
            properties: BTreeMap::from([
                (
                    "bio".into(),
                    Value::String("Alice builds reliable graph systems".into()),
                ),
                (
                    "tags".into(),
                    Value::Array(vec![
                        Value::String("graph".into()),
                        Value::String("rust".into()),
                    ]),
                ),
            ]),

            named_embeddings: BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
        })
        .unwrap();
    db.storage()
        .put_node_record(&copperdb_storage::NodeRecord {
            id: "person:2".into(),
            labels: vec!["Person".into()],
            properties: BTreeMap::from([(
                "bio".into(),
                Value::String("Bob writes storage engines".into()),
            )]),

            named_embeddings: BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 2,
            updated_at_unix_ms: 2,
        })
        .unwrap();

    let results = db
        .search_fulltext_nodes("Person", &["bio".into(), "tags".into()], "graph rust", 10)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "person:1");
    assert_eq!(results[0].label, "Person");
    assert!(
        results[0].score > 0.0,
        "expected positive BM25 score, got {}",
        results[0].score
    );
    assert_eq!(
        results[0].snippet.as_deref(),
        Some("Alice builds reliable graph systems")
    );
}

#[test]
fn test_local_fulltext_search_respects_bm25_toggle() {
    let db = CopperDb::open_temporary().unwrap();

    let error = db
        .search_fulltext_nodes("Person", &["bio".into()], "alice", 10)
        .unwrap_err();

    assert!(matches!(error, CopperDbError::Config(_)));
    assert!(error
        .to_string()
        .contains("fulltext search is disabled for this database"));
}

#[test]
fn test_execute_routes_simple_edge_property_aggregation_through_fast_path() {
    let db = CopperDb::open_temporary().unwrap();
    for (id, labels, props) in [
        (
            "customer:1",
            vec!["Customer"],
            BTreeMap::from([("name".to_string(), Value::String("Alice".into()))]),
        ),
        (
            "customer:2",
            vec!["Customer"],
            BTreeMap::from([("name".to_string(), Value::String("Bob".into()))]),
        ),
        (
            "customer:3",
            vec!["Customer"],
            BTreeMap::from([("name".to_string(), Value::String("Carol".into()))]),
        ),
        (
            "product:1",
            vec!["Product"],
            BTreeMap::from([("name".to_string(), Value::String("Widget".into()))]),
        ),
        (
            "product:2",
            vec!["Product"],
            BTreeMap::from([("name".to_string(), Value::String("Thing".into()))]),
        ),
    ] {
        db.storage()
            .put_node_record(&copperdb_storage::NodeRecord {
                id: id.to_string(),
                labels: labels.iter().map(|l| l.to_string()).collect(),
                properties: props.clone().into_iter().collect(),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
    }
    for edge in [
        EdgeRecord {
            id: "review:1".into(),
            start_node: "customer:1".into(),
            end_node: "product:1".into(),
            edge_type: "REVIEWED".into(),
            properties: BTreeMap::from([("rating".into(), Value::from(4))]),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
        EdgeRecord {
            id: "review:2".into(),
            start_node: "customer:2".into(),
            end_node: "product:1".into(),
            edge_type: "REVIEWED".into(),
            properties: BTreeMap::from([("rating".into(), Value::from(5))]),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
        EdgeRecord {
            id: "review:3".into(),
            start_node: "customer:3".into(),
            end_node: "product:2".into(),
            edge_type: "REVIEWED".into(),
            properties: BTreeMap::from([("rating".into(), Value::from(5))]),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
    ] {
        db.storage().put_edge_record(&edge).unwrap();
    }

    let result = db
            .execute(
                "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) RETURN p.name AS product, avg(r.rating) AS avgRating, count(r) AS reviewCount ORDER BY avgRating DESC LIMIT 2",
                Default::default(),
            )
            .unwrap();

    assert_eq!(result.columns, vec!["product", "avgRating", "reviewCount"]);
    assert_eq!(result.rows.len(), 2);
    assert_eq!(
        result.rows[0].get("product"),
        Some(&Value::String("Thing".into()))
    );
    assert_eq!(result.rows[0].get("reviewCount"), Some(&Value::from(1)));
    assert_eq!(
        result.rows[1].get("product"),
        Some(&Value::String("Widget".into()))
    );
    assert_eq!(result.rows[1].get("reviewCount"), Some(&Value::from(2)));
}

#[test]
fn test_execute_routes_edge_property_aggregation_branch_coverage() {
    let db = CopperDb::open_temporary().unwrap();
    for (id, labels, props) in [
        (
            "customer:1",
            vec!["Customer"],
            BTreeMap::from([("name".to_string(), Value::String("C1".into()))]),
        ),
        (
            "product:1",
            vec!["Product"],
            BTreeMap::from([("name".to_string(), Value::String("P1".into()))]),
        ),
        (
            "product:2",
            vec!["Product"],
            BTreeMap::from([("name".to_string(), Value::String("P2".into()))]),
        ),
    ] {
        db.storage()
            .put_node_record(&copperdb_storage::NodeRecord {
                id: id.to_string(),
                labels: labels.iter().map(|l| l.to_string()).collect(),
                properties: props.clone().into_iter().collect(),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
    }
    for edge in [
        EdgeRecord {
            id: "review:1".into(),
            start_node: "customer:1".into(),
            end_node: "product:1".into(),
            edge_type: "REVIEWED".into(),
            properties: BTreeMap::from([("rating".into(), Value::from(4.5))]),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
        EdgeRecord {
            id: "review:2".into(),
            start_node: "customer:1".into(),
            end_node: "product:1".into(),
            edge_type: "REVIEWED".into(),
            properties: BTreeMap::from([("rating".into(), Value::from(5))]),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
        EdgeRecord {
            id: "review:3".into(),
            start_node: "customer:1".into(),
            end_node: "product:2".into(),
            edge_type: "REVIEWED".into(),
            properties: BTreeMap::from([("other".into(), Value::from(9))]),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
        EdgeRecord {
            id: "review:4".into(),
            start_node: "customer:1".into(),
            end_node: "product:2".into(),
            edge_type: "REVIEWED".into(),
            properties: BTreeMap::from([("rating".into(), Value::String("bad".into()))]),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
        EdgeRecord {
            id: "review:5".into(),
            start_node: "customer:1".into(),
            end_node: "product:missing".into(),
            edge_type: "REVIEWED".into(),
            properties: BTreeMap::from([("rating".into(), Value::from(2))]),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
    ] {
        db.storage().put_edge_record(&edge).unwrap();
    }

    let result = db
            .execute(
                "MATCH (c)-[r:REVIEWED]->(p) RETURN p.name AS product, avg(r.rating) AS avgRating, count(r) AS reviewCount, min(r.rating) AS minRating, max(r.rating) AS maxRating, sum(r.rating) AS totalRating",
                Default::default(),
            )
            .unwrap();

    assert_eq!(
        result.columns,
        vec![
            "product",
            "avgRating",
            "reviewCount",
            "minRating",
            "maxRating",
            "totalRating"
        ]
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0].get("product"),
        Some(&Value::String("P1".into()))
    );
    assert_eq!(result.rows[0].get("avgRating"), Some(&Value::from(4.75)));
    assert_eq!(result.rows[0].get("reviewCount"), Some(&Value::from(2)));
    assert_eq!(result.rows[0].get("minRating"), Some(&Value::from(4.5)));
    assert_eq!(result.rows[0].get("maxRating"), Some(&Value::from(5.0)));
    assert_eq!(result.rows[0].get("totalRating"), Some(&Value::from(9.5)));
}

#[test]
fn test_execute_routes_incoming_count_star_through_fast_path() {
    let db = CopperDb::open_temporary().unwrap();
    for (id, labels, props) in [
        (
            "person:1",
            vec!["Person"],
            BTreeMap::from([("name".to_string(), Value::String("Alice".into()))]),
        ),
        (
            "person:2",
            vec!["Person"],
            BTreeMap::from([("name".to_string(), Value::String("Bob".into()))]),
        ),
        (
            "person:3",
            vec!["Person"],
            BTreeMap::from([("name".to_string(), Value::String("Carol".into()))]),
        ),
        (
            "person:4",
            vec!["Person"],
            BTreeMap::from([("name".to_string(), Value::String("Dana".into()))]),
        ),
        (
            "person:5",
            vec!["Person"],
            BTreeMap::from([("name".to_string(), Value::String("Eve".into()))]),
        ),
    ] {
        db.storage()
            .put_node_record(&copperdb_storage::NodeRecord {
                id: id.to_string(),
                labels: labels.iter().map(|l| l.to_string()).collect(),
                properties: props.clone().into_iter().collect(),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
    }
    for edge in [
        EdgeRecord {
            id: "follows:1".into(),
            start_node: "person:2".into(),
            end_node: "person:1".into(),
            edge_type: "FOLLOWS".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
        EdgeRecord {
            id: "follows:2".into(),
            start_node: "person:3".into(),
            end_node: "person:1".into(),
            edge_type: "FOLLOWS".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
        EdgeRecord {
            id: "follows:3".into(),
            start_node: "person:4".into(),
            end_node: "person:1".into(),
            edge_type: "FOLLOWS".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
        EdgeRecord {
            id: "follows:4".into(),
            start_node: "person:5".into(),
            end_node: "person:2".into(),
            edge_type: "FOLLOWS".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        },
    ] {
        db.storage().put_edge_record(&edge).unwrap();
    }

    let result = db
            .execute(
                "MATCH (p:Person)<-[:FOLLOWS]-(f:Person) RETURN p.name AS person, count(*) AS followers LIMIT 2",
                Default::default(),
            )
            .unwrap();

    assert_eq!(result.rows.len(), 2);
    assert_eq!(
        result.rows[0].get("person"),
        Some(&Value::String("Alice".into()))
    );
    assert_eq!(result.rows[0].get("followers"), Some(&Value::from(3)));
}

#[test]
fn test_execute_routes_incoming_count_limit_zero_returns_empty() {
    let db = CopperDb::open_temporary().unwrap();
    for (id, labels, props) in [
        (
            "person:1",
            vec!["Person"],
            BTreeMap::from([("name".to_string(), Value::String("Alice".into()))]),
        ),
        (
            "person:2",
            vec!["Person"],
            BTreeMap::from([("name".to_string(), Value::String("Bob".into()))]),
        ),
    ] {
        db.storage()
            .put_node_record(&copperdb_storage::NodeRecord {
                id: id.to_string(),
                labels: labels.iter().map(|l| l.to_string()).collect(),
                properties: props.clone().into_iter().collect(),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
    }
    db.storage()
        .put_edge_record(&EdgeRecord {
            id: "follows:1".into(),
            start_node: "person:2".into(),
            end_node: "person:1".into(),
            edge_type: "FOLLOWS".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();

    let result = db
            .execute(
                "MATCH (p:Person)<-[:FOLLOWS]-(f:Person) RETURN p.name AS person, count(f) AS followers LIMIT 0",
                Default::default(),
            )
            .unwrap();

    assert!(result.rows.is_empty());
}

#[test]
fn test_execute_routes_with_limit_compound_query_through_fast_path() {
    let db = CopperDb::open_temporary().unwrap();
    db.execute("CREATE (a:Actor {name: 'Alice'})", Default::default())
        .unwrap();
    db.execute("CREATE (m:Movie {title: 'Matrix'})", Default::default())
        .unwrap();

    let result = db
        .execute(
            "MATCH (a:Actor), (m:Movie) WITH a, m LIMIT 1 CREATE (a)-[r:TEMP_REL]->(m) DELETE r",
            Default::default(),
        )
        .unwrap();

    assert_eq!(result.stats.relationships_created, 1);
    assert_eq!(result.stats.relationships_deleted, 1);
    assert!(db
        .storage()
        .get_edges_by_type("TEMP_REL")
        .unwrap()
        .is_empty());
}

#[test]
fn test_execute_routes_with_limit_zero_compound_query_is_noop() {
    let db = CopperDb::open_temporary().unwrap();
    db.execute("CREATE (a:Actor {name: 'Alice'})", Default::default())
        .unwrap();
    db.execute("CREATE (m:Movie {title: 'Matrix'})", Default::default())
        .unwrap();

    let result = db
        .execute(
            "MATCH (a:Actor), (m:Movie) WITH a, m LIMIT 0 CREATE (a)-[r:TEMP_REL]->(m) DELETE r",
            Default::default(),
        )
        .unwrap();

    assert_eq!(result.stats.relationships_created, 0);
    assert_eq!(result.stats.relationships_deleted, 0);
    assert!(db
        .storage()
        .get_edges_by_type("TEMP_REL")
        .unwrap()
        .is_empty());
}

#[test]
fn test_execute_routes_property_match_compound_miss_is_clean() {
    let db = CopperDb::open_temporary().unwrap();
    db.execute("CREATE (p1:Person {id: 1})", Default::default())
        .unwrap();

    let result = db
            .execute(
                "MATCH (p1:Person {id: 1}), (p2:Person {id: 999}) CREATE (p1)-[r:TEMP_KNOWS]->(p2) DELETE r",
                Default::default(),
            )
            .unwrap();

    assert_eq!(result.stats.relationships_created, 0);
    assert_eq!(result.stats.relationships_deleted, 0);
    assert!(db
        .storage()
        .get_edges_by_type("TEMP_KNOWS")
        .unwrap()
        .is_empty());
}

#[test]
fn test_execute_routes_property_match_compound_fast_path() {
    let db = CopperDb::open_temporary().unwrap();
    db.execute(
        "CREATE (p1:Person {id: 1, name: 'Alice'})",
        Default::default(),
    )
    .unwrap();
    db.execute(
        "CREATE (p2:Person {id: 2, name: 'Bob'})",
        Default::default(),
    )
    .unwrap();

    let result = db
            .execute(
                "MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:TEMP_KNOWS]->(p2) DELETE r",
                Default::default(),
            )
            .unwrap();

    assert_eq!(result.stats.relationships_created, 1);
    assert_eq!(result.stats.relationships_deleted, 1);
    assert!(db
        .storage()
        .get_edges_by_type("TEMP_KNOWS")
        .unwrap()
        .is_empty());
}

#[test]
fn test_execute_routes_property_match_compound_return_count_fast_path() {
    let db = CopperDb::open_temporary().unwrap();
    db.execute("CREATE (p1:Person {id: 1})", Default::default())
        .unwrap();
    db.execute("CREATE (p2:Person {id: 2})", Default::default())
        .unwrap();

    let result = db
            .execute(
                "MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:TEMP_KNOWS]->(p2) WITH r DELETE r RETURN count(r)",
                Default::default(),
            )
            .unwrap();

    assert_eq!(result.columns, vec!["count(r)"]);
    assert_eq!(
        result.rows,
        vec![HashMap::from([("count(r)".into(), Value::from(1))])]
    );
    assert_eq!(result.stats.relationships_created, 1);
    assert_eq!(result.stats.relationships_deleted, 1);
    assert!(db
        .storage()
        .get_edges_by_type("TEMP_KNOWS")
        .unwrap()
        .is_empty());
}

#[test]
fn test_execute_routes_pipeline_query_through_route_hook() {
    let db = CopperDb::open_temporary().unwrap();
    let result = db
        .execute(
            "WITH [1, 2] AS values UNWIND values AS value RETURN value",
            Default::default(),
        )
        .unwrap();

    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get("value"), Some(&Value::from(1)));
    assert_eq!(result.rows[1].get("value"), Some(&Value::from(2)));
}

#[test]
fn test_execute_routes_pipeline_create_reuses_bound_nodes() {
    let db = CopperDb::open_temporary().unwrap();
    db.execute(
        "CREATE (c:Customer {customerID: 1, name: 'Ada'})",
        Default::default(),
    )
    .unwrap();

    let result = db
            .execute(
                "MATCH (c:Customer {customerID: 1}) CREATE (o:Order {orderID: 9001}) CREATE (c)-[:PURCHASED]->(o) WITH c, o RETURN c.customerID AS customerID, o.orderID AS orderID",
                Default::default(),
            )
            .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("customerID"), Some(&Value::from(1)));
    assert_eq!(result.rows[0].get("orderID"), Some(&Value::from(9001)));
    assert_eq!(result.stats.nodes_created, 1);
    assert_eq!(result.stats.relationships_created, 1);

    let edges = db.storage().get_edges_by_type("PURCHASED").unwrap();
    assert_eq!(edges.len(), 1);

    let customer_raw = db
        .storage()
        .get_node(&edges[0].start_node)
        .unwrap()
        .expect("customer node should exist");
    let customer_props: HashMap<String, Value> = rmp_serde::from_slice(&customer_raw).unwrap();
    assert_eq!(customer_props.get("customerID"), Some(&Value::from(1)));

    let order_raw = db
        .storage()
        .get_node(&edges[0].end_node)
        .unwrap()
        .expect("order node should exist");
    let order_props: HashMap<String, Value> = rmp_serde::from_slice(&order_raw).unwrap();
    assert_eq!(order_props.get("orderID"), Some(&Value::from(9001)));
}

#[test]
fn test_storage_mvcc_visible_reads_are_reachable_from_copperdb() {
    let db = CopperDb::open_temporary().unwrap();
    db.execute("CREATE (n:Person {name: 'Ada'})", Default::default())
        .unwrap();
    let snapshot = db.storage().begin_mvcc_snapshot();
    let mut current = db.storage().get_nodes_by_label("Person").unwrap();
    assert_eq!(current.len(), 1);
    let mut updated = current.pop().unwrap();
    updated.labels = vec!["Device".to_string()];
    updated.updated_at_unix_ms += 1;
    db.storage().put_node_record(&updated).unwrap();

    assert!(db
        .storage()
        .get_nodes_by_label("Person")
        .unwrap()
        .is_empty());
    let visible_then = db
        .storage()
        .get_nodes_by_label_visible_at(&snapshot, "Person")
        .unwrap();
    assert_eq!(visible_then.len(), 1);
    assert_eq!(visible_then[0].labels, vec!["Person".to_string()]);
}

#[test]
fn test_storage_mvcc_rebuild_is_reachable_from_copperdb() {
    let db = CopperDb::open_temporary().unwrap();
    db.execute("CREATE (n:Person {name: 'Ada'})", Default::default())
        .unwrap();

    let mut current = db.storage().get_nodes_by_label("Person").unwrap();
    assert_eq!(current.len(), 1);
    let node_id = current.pop().unwrap().id;

    let lease = db.storage().begin_registered_mvcc_snapshot();
    assert!(matches!(
        db.storage().rebuild_mvcc_from_current_state(),
        Err(copperdb_storage::StorageError::MvccRebuildBlocked { active_readers: 1 })
    ));
    drop(lease);

    db.storage().delete_node(&node_id).unwrap();

    let stale_snapshot = db.storage().begin_mvcc_snapshot();
    assert_eq!(
        db.storage()
            .get_nodes_by_label_visible_at(&stale_snapshot, "Person")
            .unwrap()
            .len(),
        1
    );

    db.storage().rebuild_mvcc_from_current_state().unwrap();

    let repaired_snapshot = db.storage().begin_mvcc_snapshot();
    assert!(db
        .storage()
        .get_nodes_by_label_visible_at(&repaired_snapshot, "Person")
        .unwrap()
        .is_empty());
}

#[test]
fn test_storage_mvcc_prune_versions_is_reachable_from_copperdb() {
    let db = CopperDb::open_temporary().unwrap();
    db.execute("CREATE (n:Person {name: 'Ada'})", Default::default())
        .unwrap();

    let mut current = db.storage().get_nodes_by_label("Person").unwrap();
    assert_eq!(current.len(), 1);
    let mut node = current.pop().unwrap();

    let lease = db.storage().begin_registered_mvcc_snapshot();
    node.labels = vec!["Device".to_string()];
    node.updated_at_unix_ms += 1;
    db.storage().put_node_record(&node).unwrap();
    node.updated_at_unix_ms += 1;
    db.storage().put_node_record(&node).unwrap();

    let removed = db.storage().prune_mvcc_versions(MvccPruneOptions {
        max_versions_per_key: Some(1),
    });
    assert_eq!(removed, 1);
    assert_eq!(db.storage().lifecycle_status().floor, 1);
    assert_eq!(
        db.storage()
            .get_nodes_by_label_visible_at(lease.snapshot(), "Person")
            .unwrap()
            .len(),
        1
    );

    drop(lease);
    let removed = db.storage().prune_mvcc_versions(MvccPruneOptions {
        max_versions_per_key: Some(1),
    });
    assert!(removed > 0);
    let latest = db.storage().begin_mvcc_snapshot();
    assert!(db
        .storage()
        .get_nodes_by_label_visible_at(&latest, "Person")
        .unwrap()
        .is_empty());
    assert_eq!(
        db.storage()
            .get_nodes_by_label_visible_at(&latest, "Device")
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn test_execute_routes_pipeline_match_respects_bound_relationship_endpoints() {
    let db = CopperDb::open_temporary().unwrap();
    for cypher in [
        "CREATE (c:Customer {customerID: 1, name: 'Ada'})",
        "CREATE (c:Customer {customerID: 2, name: 'Bob'})",
        "CREATE (o:Order {orderID: 100})",
        "CREATE (o:Order {orderID: 200})",
    ] {
        db.execute(cypher, Default::default()).unwrap();
    }

    let node_id_for = |label: &str, property: &str, expected: i64| {
        db.storage()
            .scan_nodes_with_prefix(&format!("{label}:"))
            .find_map(|entry| {
                let (_, raw) = entry.ok()?;
                let props: HashMap<String, Value> = rmp_serde::from_slice(&raw).ok()?;
                (props.get(property) == Some(&Value::from(expected)))
                    .then(|| props.get("_id").and_then(Value::as_str).map(str::to_string))
                    .flatten()
            })
            .expect("expected seeded node")
    };

    db.storage()
        .put_edge_record(&EdgeRecord {
            id: "purchased:1".into(),
            start_node: node_id_for("Customer", "customerID", 1),
            end_node: node_id_for("Order", "orderID", 100),
            edge_type: "PURCHASED".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();
    db.storage()
        .put_edge_record(&EdgeRecord {
            id: "purchased:2".into(),
            start_node: node_id_for("Customer", "customerID", 2),
            end_node: node_id_for("Order", "orderID", 200),
            edge_type: "PURCHASED".into(),
            properties: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        })
        .unwrap();

    let result = db
            .execute(
                "MATCH (c:Customer {customerID: 1}) WITH c MATCH (c)-[:PURCHASED]->(o:Order) RETURN o.orderID AS orderID",
                Default::default(),
            )
            .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("orderID"), Some(&Value::from(100)));
}

#[test]
fn test_execute_routes_mutual_relationship_on_empty_db_returns_no_rows() {
    let db = CopperDb::open_temporary().unwrap();

    let result = db
        .execute(
            "MATCH (a)-[:FOLLOWS]->(b)-[:FOLLOWS]->(a) RETURN a, b",
            Default::default(),
        )
        .unwrap();

    assert!(result.rows.is_empty());
}

#[test]
fn test_execute_routes_mutual_relationship_with_missing_rel_type_returns_no_rows() {
    let db = CopperDb::open_temporary().unwrap();
    db.execute("CREATE (a:Person {name: 'Alice'})", Default::default())
        .unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})", Default::default())
        .unwrap();
    db.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:FOLLOWS]->(b) CREATE (b)-[:FOLLOWS]->(a)",
            Default::default(),
        )
        .unwrap();

    let result = db
        .execute(
            "MATCH (a)-[:NONEXISTENT]->(b)-[:NONEXISTENT]->(a) RETURN a, b",
            Default::default(),
        )
        .unwrap();

    assert!(result.rows.is_empty());
}

#[test]
fn test_execute_routes_pipeline_seeder_shape_with_expression_pattern_properties() {
    let db = CopperDb::open_temporary().unwrap();
    db.execute(
        "CREATE (c:Customer {customerID: 1, name: 'Ada'})",
        Default::default(),
    )
    .unwrap();
    db.execute(
        "CREATE (p:Product {productID: 1, name: 'Widget'})",
        Default::default(),
    )
    .unwrap();

    let result = db
            .execute(
                "MATCH (c:Customer {customerID: 1}) CREATE (o:Order {orderID: 9001}) CREATE (c)-[:PURCHASED]->(o) WITH o, {} UNWIND [{productID: 1}] AS prodRef MATCH (p:Product {productID: prodRef.productID}) CREATE (o)-[:ORDERS]->(p)",
                Default::default(),
            )
            .unwrap();

    assert_eq!(result.stats.nodes_created, 1);
    assert_eq!(result.stats.relationships_created, 2);

    let orders = db.storage().get_edges_by_type("ORDERS").unwrap();
    assert_eq!(orders.len(), 1);
    let purchased = db.storage().get_edges_by_type("PURCHASED").unwrap();
    assert_eq!(purchased.len(), 1);
    assert_eq!(purchased[0].end_node, orders[0].start_node);

    let order_raw = db
        .storage()
        .get_node(&orders[0].start_node)
        .unwrap()
        .expect("order node should exist");
    let order_props: HashMap<String, Value> = rmp_serde::from_slice(&order_raw).unwrap();
    assert_eq!(order_props.get("orderID"), Some(&Value::from(9001)));

    let product_raw = db
        .storage()
        .get_node(&orders[0].end_node)
        .unwrap()
        .expect("product node should exist");
    let product_props: HashMap<String, Value> = rmp_serde::from_slice(&product_raw).unwrap();
    assert_eq!(product_props.get("productID"), Some(&Value::from(1)));
}

#[test]
fn test_execute_large_variable_length_chain_traversal_consistency() {
    let db = CopperDb::open_temporary().unwrap();

    for index in 0..25 {
        db.storage()
            .put_node_record(&copperdb_storage::NodeRecord {
                id: format!("Node:{index}"),
                labels: vec!["Node".to_string()],
                properties: BTreeMap::from([(
                    "name".to_string(),
                    Value::String(format!("n{index:02}")),
                )])
                .into_iter()
                .collect(),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
    }

    for index in 0..24 {
        db.storage()
            .put_edge_record(&EdgeRecord {
                id: format!("link:{index}"),
                start_node: format!("Node:{index}"),
                end_node: format!("Node:{}", index + 1),
                edge_type: "LINK".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
    }

    let result = db
        .execute(
            "MATCH (a:Node {name: 'n00'})-[:LINK*1..24]->(n:Node) RETURN n.name AS name",
            Default::default(),
        )
        .unwrap();

    let mut names = result
        .rows
        .iter()
        .filter_map(|row| row.get("name").and_then(Value::as_str))
        .map(str::to_string)
        .collect::<Vec<_>>();
    names.sort();

    let expected = (1..25)
        .map(|index| format!("n{index:02}"))
        .collect::<Vec<_>>();
    assert_eq!(names, expected);
}

#[test]
fn test_execute_routes_pipeline_seeder_shape_supports_multiple_rows_and_edge_properties() {
    let db = CopperDb::open_temporary().unwrap();
    db.execute(
        "CREATE (c:Customer {customerID: 1, companyName: 'C1'})",
        Default::default(),
    )
    .unwrap();
    db.execute(
        "CREATE (p:Product {productID: 1, productName: 'P1'})",
        Default::default(),
    )
    .unwrap();
    db.execute(
        "CREATE (p:Product {productID: 2, productName: 'P2'})",
        Default::default(),
    )
    .unwrap();

    let result = db
            .execute(
                "MATCH (c:Customer {customerID: 1}) CREATE (o:Order {orderID: 9001}) CREATE (c)-[:PURCHASED]->(o) WITH o, {} UNWIND [{productID: 1, quantity: 3}, {productID: 2, quantity: 5}] AS prodRef MATCH (p:Product {productID: prodRef.productID}) CREATE (o)-[:ORDERS {quantity: prodRef.quantity}]->(p)",
                Default::default(),
            )
            .unwrap();

    assert_eq!(result.stats.nodes_created, 1);
    assert_eq!(result.stats.relationships_created, 3);

    let purchased = db.storage().get_edges_by_type("PURCHASED").unwrap();
    assert_eq!(purchased.len(), 1);
    let orders = db.storage().get_edges_by_type("ORDERS").unwrap();
    assert_eq!(orders.len(), 2);

    let mut quantities: Vec<i64> = orders
        .iter()
        .filter_map(|edge| edge.properties.get("quantity").and_then(Value::as_i64))
        .collect();
    quantities.sort_unstable();
    assert_eq!(quantities, vec![3, 5]);

    let order_ids: std::collections::HashSet<String> =
        orders.iter().map(|edge| edge.start_node.clone()).collect();
    assert_eq!(order_ids.len(), 1);
    assert!(order_ids.contains(&purchased[0].end_node));
}

#[test]
fn test_execute_merge_uses_current_row_expression_properties() {
    let db = CopperDb::open_temporary().unwrap();

    let first = db
            .execute(
                "UNWIND [1, 2] AS customerID MERGE (c:Customer {customerID: customerID}) RETURN c.customerID AS customerID",
                Default::default(),
            )
            .unwrap();
    let second = db
            .execute(
                "UNWIND [1, 2] AS customerID MERGE (c:Customer {customerID: customerID}) RETURN c.customerID AS customerID",
                Default::default(),
            )
            .unwrap();

    assert_eq!(first.rows.len(), 2);
    assert_eq!(first.rows[0].get("customerID"), Some(&Value::from(1)));
    assert_eq!(first.rows[1].get("customerID"), Some(&Value::from(2)));
    assert_eq!(first.stats.nodes_created, 2);
    assert_eq!(second.stats.nodes_created, 0);
}

#[test]
fn engine_records_durable_query_audit_events() {
    let db = CopperDb::open_temporary().unwrap();
    db.execute("CREATE (n:Audit {v: 1})", Default::default())
        .unwrap();
    db.execute("MATCH (n:Audit) RETURN n", Default::default())
        .unwrap();

    let events = db.audit_log().events().unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event_type, EventType::DataCreate);
    assert_eq!(events[1].event_type, EventType::DataRead);
    assert_eq!(events[1].resource.as_deref(), Some("cypher_query"));
    assert!(db.audit_log().verify_chain().unwrap().valid);
}

#[test]
fn engine_records_failed_query_audit_events() {
    let db = CopperDb::open_temporary().unwrap();
    let err = db
        .execute("MATCH (n RETURN n", Default::default())
        .unwrap_err();
    assert!(matches!(err, CopperDbError::Parse(_)));

    let events = db.audit_log().events().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, EventType::DataRead);
    assert_eq!(events[0].action.as_deref(), Some("PARSE"));
    assert!(!events[0].success);
    assert!(events[0].reason.is_some());
}

#[test]
fn engine_enforces_durable_compliance_label_and_property_policies() {
    use copperdb_compliance::{ComplianceControl, CompliancePolicy};

    let db = CopperDb::open_temporary().unwrap();
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
    db.compliance_manager()
        .add_policy(CompliancePolicy::new(
            "mask-ssn",
            "Mask SSN",
            ComplianceControl::MaskProperty {
                property: "ssn".into(),
                allowed_roles: vec!["doctor".into()],
            },
        ))
        .unwrap();

    let reader_roles = vec!["reader".to_string()];
    let err = db
        .execute_as(
            "CREATE (n:Patient {name: 'Alice'})",
            Default::default(),
            &reader_roles,
        )
        .unwrap_err();
    assert!(matches!(err, CopperDbError::Compliance(_)));

    let doctor_roles = vec!["doctor".to_string()];
    db.execute_as(
        "CREATE (n:Patient {name: 'Alice', ssn: '111'})",
        Default::default(),
        &doctor_roles,
    )
    .unwrap();
    let err = db
        .execute_as(
            "MATCH (n:Patient) WHERE n.ssn = '111' RETURN n",
            Default::default(),
            &reader_roles,
        )
        .unwrap_err();
    assert!(matches!(err, CopperDbError::Compliance(_)));
}

#[test]
fn engine_exports_compliance_evidence_from_audit_log() {
    let db = CopperDb::open_temporary().unwrap();
    db.execute("CREATE (n:Evidence {v: 1})", Default::default())
        .unwrap();
    let report = db
        .compliance_reporter()
        .export_soc2_evidence(copperdb_compliance::ReportWindow::all_time())
        .unwrap();
    assert_eq!(report.summary.total, 1);
    assert_eq!(report.summary.by_event_type.get("DATA_CREATE"), Some(&1));
}

#[test]
fn test_default_config() {
    let config = DatabaseConfig::default();
    assert!(config.auth_enabled);
    assert_eq!(config.max_connections, 100);
    assert!(!config.runtime_config.bm25_enabled);
    assert!(!config.runtime_config.vector_enabled);
    assert!(config.storage_encryption_master_key.is_none());
}

#[test]
fn persistent_engine_can_open_with_encrypted_storage() {
    let dir = tempfile::tempdir().unwrap();
    let config = DatabaseConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        storage_encryption_master_key: Some(vec![0x42; 32]),
        storage_encryption_key_uri: "kms://local/storage-test".into(),
        ..Default::default()
    };

    {
        let db = CopperDb::open(config.clone()).unwrap();
        assert!(db.storage().is_encrypted());
        db.execute("CREATE (n:Encrypted {v: 1})", Default::default())
            .unwrap();
        db.flush().unwrap();
    }

    let reopened = CopperDb::open(config).unwrap();
    let result = reopened
        .execute("MATCH (n:Encrypted) RETURN n", Default::default())
        .unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn engine_plans_distributed_reads_and_writes_from_storage_topology() {
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

    let write_plan = db
        .plan_distributed_write(&placement, ConsistencyLevel::Quorum, None)
        .unwrap();
    assert_eq!(write_plan.required_acks, 2);
    assert_eq!(write_plan.replicas.len(), 3);

    let read_plan = db
        .plan_distributed_read(&placement, ConsistencyLevel::Quorum, None)
        .unwrap();
    assert_eq!(read_plan.required_responses, 2);
    assert_eq!(read_plan.replicas.len(), 3);
}

#[test]
fn engine_persists_and_plans_fabric_database_shards() {
    use copperdb_fabric::{
        FabricAggregateOptions, FabricAggregateSpec, FabricPath, FabricPathBatch,
        FabricPathMergeOptions, FabricReadRequest, FabricReadScope, FabricRowBatch,
        FabricRowMergeOptions, FabricSortKey,
    };
    use copperdb_search::{
        RrfConfig, RrfHydrationRecord, RrfSearchBatch, RrfSearchHit, RrfSearchPolicy,
    };
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
    db.storage()
        .persist_index_definition(&copperdb_storage::IndexDefinition {
            name: "person_bio_fulltext_idx".into(),
            entity_type: copperdb_storage::IndexEntityType::Node,
            label: "Person".into(),
            properties: vec!["bio".into()],
            kind: copperdb_storage::IndexKind::FullText,
        })
        .unwrap();
    db.storage()
        .put_node_record(&copperdb_storage::NodeRecord {
            id: "person:1".into(),
            labels: vec!["Person".into()],
            properties: BTreeMap::from([(
                "bio".into(),
                Value::String("Alice builds reliable graph systems".into()),
            )]),

            named_embeddings: BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 10,
            updated_at_unix_ms: 20,
        })
        .unwrap();
    for node_id in ["node-1", "node-2"] {
        db.storage()
            .register_topology_peer(
                &MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Search)
                    .with_capability(NodeCapability::Coordinator),
            )
            .unwrap();
    }
    for (shard, node_id) in [("primary", "node-1"), ("person-00", "node-2")] {
        db.storage()
            .register_topology_placement(&PlacementRecord {
                key: PlacementKey::new("default", "copper", shard),
                primary_node: node_id.into(),
                replica_nodes: vec![],
                search_nodes: vec![node_id.into()],
                hyperscaler_profile: None,
                min_write_replicas: 0,
                search_fanout: 1,
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

    db.register_fabric_database(&fabric).unwrap();
    assert_eq!(db.list_fabric_databases().unwrap(), vec![fabric.clone()]);
    assert_eq!(
        db.load_fabric_database("default", "copper")
            .unwrap()
            .unwrap(),
        fabric
    );

    let read_plans = db
        .plan_fabric_reads(&fabric, ConsistencyLevel::One, None)
        .unwrap();
    let search_plans = db.plan_fabric_searches(&fabric).unwrap();
    let local_ranked_batch = db
        .search_fabric_ranked_batch_locally(
            &PlacementKey::new("default", "copper", "person-00"),
            &SearchQuery::FullText {
                query: "graph".into(),
                fields: vec!["bio".into()],
                limit: 10,
            },
        )
        .unwrap();
    let local_hydration = db
        .hydrate_fabric_entities_locally(&[local_ranked_batch.hits[0].global_id.clone()])
        .unwrap();

    assert_eq!(read_plans.len(), 2);
    assert_eq!(search_plans.len(), 2);
    assert_eq!(read_plans[0].placement.shard, "primary");
    assert_eq!(read_plans[1].placement.shard, "person-00");
    assert_eq!(local_ranked_batch.source, "lexical");
    assert_eq!(local_ranked_batch.hits.len(), 1);
    assert_eq!(local_ranked_batch.hits[0].rank, 1);
    assert_eq!(local_ranked_batch.hits[0].label, "Person");
    assert_eq!(
        local_ranked_batch.hits[0].global_id,
        FabricGlobalId::new(
            PlacementKey::new("default", "copper", "person-00"),
            "node",
            "person:1",
        )
    );
    assert_eq!(local_hydration.len(), 1);
    assert_eq!(local_hydration[0].labels, vec!["Person"]);
    assert_eq!(
        local_hydration[0].entity["bio"],
        "Alice builds reliable graph systems"
    );
    assert_eq!(local_hydration[0].entity["_id"], "person:1");

    let person_plan = db
        .plan_fabric_query_reads(
            &fabric,
            FabricReadRequest {
                scope: FabricReadScope::Label("Person".into()),
                consistency: ConsistencyLevel::One,
                request_region: None,
            },
        )
        .unwrap();
    assert_eq!(person_plan.shards.len(), 1);
    assert_eq!(person_plan.shards[0].shard.placement.shard, "person-00");

    let merged = db.merge_fabric_rows(
        vec![
            FabricRowBatch {
                shard: PlacementKey::new("default", "copper", "primary"),
                rows: vec![serde_json::json!({"id": "a", "score": 2})],
            },
            FabricRowBatch {
                shard: PlacementKey::new("default", "copper", "person-00"),
                rows: vec![serde_json::json!({"id": "b", "score": 3})],
            },
        ],
        FabricRowMergeOptions {
            order_by: vec![FabricSortKey::descending("score")],
            limit: Some(1),
            ..Default::default()
        },
    );
    assert_eq!(merged.rows.len(), 1);
    assert_eq!(merged.rows[0]["id"], "b");

    let aggregates = db.merge_fabric_aggregates(
        vec![
            FabricRowBatch {
                shard: PlacementKey::new("default", "copper", "primary"),
                rows: vec![serde_json::json!({"label": "Person", "score": 2})],
            },
            FabricRowBatch {
                shard: PlacementKey::new("default", "copper", "person-00"),
                rows: vec![serde_json::json!({"label": "Person", "score": 4})],
            },
        ],
        FabricAggregateOptions {
            group_by: vec!["label".into()],
            aggregates: vec![
                FabricAggregateSpec::count("count"),
                FabricAggregateSpec::average("avg_score", "score"),
            ],
            order_by: Vec::new(),
            skip: 0,
            limit: None,
        },
    );
    assert_eq!(aggregates.rows.len(), 1);
    assert_eq!(aggregates.rows[0]["count"], 2);
    assert_eq!(aggregates.rows[0]["avg_score"], 3.0);

    let path = FabricPath::new(
        vec![
            FabricGlobalId::new(
                PlacementKey::new("default", "copper", "primary"),
                "node",
                "a",
            ),
            FabricGlobalId::new(
                PlacementKey::new("default", "copper", "person-00"),
                "node",
                "b",
            ),
        ],
        vec![FabricGlobalId::new(
            PlacementKey::new("default", "copper", "primary"),
            "relationship",
            "ab",
        )],
    );
    let paths = db.merge_fabric_paths(
        vec![
            FabricPathBatch {
                shard: PlacementKey::new("default", "copper", "primary"),
                paths: vec![path.clone()],
            },
            FabricPathBatch {
                shard: PlacementKey::new("default", "copper", "person-00"),
                paths: vec![path.clone()],
            },
        ],
        FabricPathMergeOptions::default(),
    );
    assert_eq!(paths.input_paths, 2);
    assert_eq!(paths.output_paths, 1);
    assert_eq!(paths.paths, vec![path]);

    let ranked_batches = vec![
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
                score: 0.7,
                source: "lexical".into(),
                shard: PlacementKey::new("default", "copper", "primary"),
                label: "Person".into(),
                snippet: None,
            }],
        },
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
                shard: PlacementKey::new("default", "copper", "primary"),
                label: "Person".into(),
                snippet: Some("fresh".into()),
            }],
        },
    ];
    let hydration = vec![RrfHydrationRecord {
        global_id: FabricGlobalId::new(
            PlacementKey::new("default", "copper", "primary"),
            "node",
            "a",
        ),
        labels: vec!["Person".into()],
        entity: serde_json::json!({
            "id": "a",
            "name": "Alice",
            "secret": "internal"
        }),
    }];
    let ranked_policy = RrfSearchPolicy {
        allowed_labels: vec!["Person".into()],
        denied_labels: Vec::new(),
        denied_sources: Vec::new(),
        require_hydration: true,
        redact_fields: vec!["secret".into()],
    };
    let ranked = db.merge_fabric_ranked_search(ranked_batches.clone(), RrfConfig::new(60.0, 10));
    assert_eq!(ranked.input_hits, 2);
    assert_eq!(ranked.output_hits, 1);
    assert_eq!(ranked.results[0].sources, vec!["lexical", "vector"]);
    assert_eq!(ranked.results[0].best_score, 0.9);

    let hydrated =
        db.hydrate_fabric_ranked_search(ranked, hydration.clone(), ranked_policy.clone());
    assert_eq!(hydrated.output_hits, 1);
    assert_eq!(hydrated.filtered_hits, 0);
    assert_eq!(hydrated.missing_hydration_hits, 0);
    assert_eq!(hydrated.results[0].labels, vec!["Person"]);
    assert_eq!(hydrated.results[0].redacted_fields, vec!["secret"]);
    assert_eq!(
        hydrated.results[0].entity.as_ref().unwrap()["name"],
        "Alice"
    );
    assert!(hydrated.results[0]
        .entity
        .as_ref()
        .unwrap()
        .get("secret")
        .is_none());

    let executed = db
        .execute_fabric_ranked_search(
            &fabric,
            {
                let mut batches = ranked_batches;
                batches.push(RrfSearchBatch {
                    shard: PlacementKey::new("default", "copper", "rogue-00"),
                    source: "rogue".into(),
                    hits: vec![RrfSearchHit {
                        global_id: FabricGlobalId::new(
                            PlacementKey::new("default", "copper", "rogue-00"),
                            "node",
                            "rogue",
                        ),
                        rank: 1,
                        score: 1.0,
                        source: "rogue".into(),
                        shard: PlacementKey::new("default", "copper", "rogue-00"),
                        label: "Person".into(),
                        snippet: Some("ignore me".into()),
                    }],
                });
                batches
            },
            hydration,
            RrfConfig::new(60.0, 10),
            ranked_policy,
        )
        .unwrap();
    assert_eq!(executed.planned_shards.len(), 2);
    assert_eq!(executed.responded_shards.len(), 2);
    assert_eq!(executed.missing_shards, Vec::<PlacementKey>::new());
    assert_eq!(
        executed.ignored_shards,
        vec![PlacementKey::new("default", "copper", "rogue-00")]
    );
    assert_eq!(executed.hydrated.output_hits, 1);
    assert_eq!(
        executed.hydrated.results[0].entity.as_ref().unwrap()["name"],
        "Alice"
    );
}
