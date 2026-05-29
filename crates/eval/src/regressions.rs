use super::*;
    // G��G�� NornicDB v1.0.42 regression tests G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��G��

    /// MERGE must not create a duplicate when the node already exists.
    /// Mirrors NornicDB v1.0.42 `TestMergeNode_MatchWhenExists`.
    #[test]
    fn test_merge_match_when_exists() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("CREATE (n:Person {name: 'Alice'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // MERGE twice G�� should match the existing node, not create two more.
        let q = parser.parse("MERGE (n:Person {name: 'Alice'})").unwrap();
        engine.execute(&q, &HashMap::new()).unwrap();
        engine.execute(&q, &HashMap::new()).unwrap();

        let count_q = parser
            .parse("MATCH (n:Person {name: 'Alice'}) RETURN n")
            .unwrap();
        let result = engine.execute(&count_q, &HashMap::new()).unwrap();
        assert_eq!(
            result.rows.len(),
            1,
            "MERGE must not duplicate an existing node"
        );
    }

    /// MERGE node-lookup cache must evict stale entries after a DELETE.
    ///
    /// Mirrors NornicDB v1.0.42's `TestMergeNode_FindMergeNodeIgnoresStaleCacheEntry`
    /// and the `invalidateNodeLookupCache` call after implicit-tx rollback/commit
    /// failures (commit `4cdee7c`).
    #[test]
    fn test_merge_cache_evicted_after_delete() {
        let engine = make_engine();
        let parser = Parser::new();

        // First MERGE G�� creates the node and caches it.
        engine
            .execute(
                &parser.parse("MERGE (n:Tag {name: 'rust'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Delete the node G�� this must invalidate the cache.
        engine
            .execute(
                &parser
                    .parse("MATCH (n:Tag {name: 'rust'}) DELETE n")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Second MERGE G�� the cache was cleared so MERGE must re-scan storage,
        // find nothing, and create a new node.
        let merge_result = engine
            .execute(
                &parser.parse("MERGE (n:Tag {name: 'rust'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(
            merge_result.stats.nodes_created, 1,
            "MERGE should recreate the node after the stale cache entry was evicted"
        );

        let count_q = parser
            .parse("MATCH (n:Tag {name: 'rust'}) RETURN n")
            .unwrap();
        let result = engine.execute(&count_q, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    /// Multi-label MATCH: `MATCH (n:Person:Employee)` must only return nodes
    /// that carry BOTH labels.
    ///
    /// Mirrors NornicDB v1.0.42 commit `6283009` (make hot paths n-ary and generic).
    #[test]
    #[allow(clippy::arc_with_non_send_sync)]
    fn test_match_multi_label_filters_correctly() {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        let engine = EvalEngine::new(Arc::clone(&storage));

        // Directly insert a node with two labels [:Person, :Employee].
        {
            let mut props: HashMap<String, Value> = HashMap::new();
            props.insert(
                "_id".to_string(),
                Value::String("Person:alice-id".to_string()),
            );
            props.insert("name".to_string(), Value::String("Alice".to_string()));
            props.insert(
                "_labels".to_string(),
                Value::Array(vec![
                    Value::String("Person".to_string()),
                    Value::String("Employee".to_string()),
                ]),
            );
            store_node(storage.as_ref(), "Person:alice-id", &["Person", "Employee"], props);
        }

        // Directly insert a node with only [:Person].
        {
            let mut props: HashMap<String, Value> = HashMap::new();
            props.insert(
                "_id".to_string(),
                Value::String("Person:bob-id".to_string()),
            );
            props.insert("name".to_string(), Value::String("Bob".to_string()));
            props.insert(
                "_labels".to_string(),
                Value::Array(vec![Value::String("Person".to_string())]),
            );
            store_node(storage.as_ref(), "Person:bob-id", &["Person"], props);
        }

        let parser = Parser::new();

        // MATCH (n:Person) should return BOTH Alice and Bob (prefix = "Person:").
        let q_person = parser.parse("MATCH (n:Person) RETURN n").unwrap();
        let result = engine.execute(&q_person, &HashMap::new()).unwrap();
        assert_eq!(
            result.rows.len(),
            2,
            "MATCH :Person should return both nodes"
        );

        // MATCH (n:Person:Employee) should return ONLY Alice.
        let q_both = parser.parse("MATCH (n:Person:Employee) RETURN n").unwrap();
        let result_both = engine.execute(&q_both, &HashMap::new()).unwrap();
        assert_eq!(
            result_both.rows.len(),
            1,
            "MATCH :Person:Employee should return only Alice"
        );
        if let Some(Value::Object(p)) = result_both.rows[0].get("n") {
            assert_eq!(p.get("name"), Some(&Value::String("Alice".into())));
        } else {
            panic!("expected object row");
        }
    }

    /// MERGE is idempotent across multiple engine calls (cache-hit path).
    ///
    /// Verifies that the node-lookup cache correctly short-circuits repeated
    /// MERGEs without creating duplicates.
    #[test]
    fn test_merge_idempotent_via_cache() {
        let engine = make_engine();
        let parser = Parser::new();

        let q = parser.parse("MERGE (n:Counter {key: 'hits'})").unwrap();
        for _ in 0..5 {
            engine.execute(&q, &HashMap::new()).unwrap();
        }

        let count_q = parser
            .parse("MATCH (n:Counter {key: 'hits'}) RETURN n")
            .unwrap();
        let result = engine.execute(&count_q, &HashMap::new()).unwrap();
        assert_eq!(
            result.rows.len(),
            1,
            "five MERGEs must produce exactly one node"
        );
    }

    #[test]
    fn test_unwind_list_literal_returns_rows() {
        let engine = make_engine();
        let parser = Parser::new();
        let query = parser
            .parse("UNWIND [1, 2, 3] AS value RETURN value")
            .unwrap();

        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.columns, vec!["value"]);
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0].get("value"), Some(&Value::from(1)));
        assert_eq!(result.rows[1].get("value"), Some(&Value::from(2)));
        assert_eq!(result.rows[2].get("value"), Some(&Value::from(3)));
    }

    #[test]
    fn test_unwind_map_literal_returns_projected_properties() {
        let engine = make_engine();
        let parser = Parser::new();
        let query = parser
            .parse("UNWIND [{name: 'Ada'}, {name: 'Linus'}] AS row RETURN row.name AS name")
            .unwrap();

        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.columns, vec!["name"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("name"), Some(&Value::from("Ada")));
        assert_eq!(result.rows[1].get("name"), Some(&Value::from("Linus")));
    }

    /// UNWIND + MERGE should execute a MERGE for each unwound item, but must
    /// not create duplicate nodes when the same label+property is encountered.
    ///
    /// Mirrors NornicDB v1.0.42 regression coverage for UNWIND/MERGE fallback paths.
    #[test]
    fn test_merge_after_create_sees_new_node() {
        let engine = make_engine();
        let parser = Parser::new();

        // Create the node first.
        engine
            .execute(
                &parser.parse("CREATE (n:Service {name: 'api'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // MERGE must match the created node, not create a second one.
        let merge_result = engine
            .execute(
                &parser.parse("MERGE (n:Service {name: 'api'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(
            merge_result.stats.nodes_created, 0,
            "MERGE should find the existing node, not create a new one"
        );

        let count_q = parser.parse("MATCH (n:Service) RETURN n").unwrap();
        let result = engine.execute(&count_q, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_constraint_ddl_roundtrip() {
        let engine = make_engine();
        let parser = Parser::new();

        let create = parser
            .parse("CREATE CONSTRAINT person_email_unique FOR (n:Person) REQUIRE n.email IS UNIQUE")
            .unwrap();
        engine.execute(&create, &HashMap::new()).unwrap();

        let show = parser.parse("SHOW CONSTRAINTS").unwrap();
        let shown = engine.execute(&show, &HashMap::new()).unwrap();
        assert_eq!(shown.rows.len(), 1);
        assert_eq!(
            shown.rows[0].get("name"),
            Some(&Value::String("person_email_unique".to_string()))
        );
    }

    #[test]
    fn test_constraint_drop_if_exists_and_error_path() {
        let engine = make_engine();
        let parser = Parser::new();

        let err = match engine.execute(
            &parser.parse("DROP CONSTRAINT missing_constraint").unwrap(),
            &HashMap::new(),
        ) {
            Ok(_) => panic!("expected drop constraint to fail"),
            Err(err) => err,
        };
        assert!(err
            .to_string()
            .contains("constraint \"missing_constraint\" not found"));

        engine
            .execute(
                &parser
                    .parse("DROP CONSTRAINT missing_constraint IF EXISTS")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
    }

    #[test]
    fn test_index_ddl_roundtrip() {
        let engine = make_engine();
        let parser = Parser::new();

        let create = parser
            .parse("CREATE INDEX person_idx FOR (n:Person) ON (n.email)")
            .unwrap();
        engine.execute(&create, &HashMap::new()).unwrap();

        let show = parser.parse("SHOW INDEXES").unwrap();
        let shown = engine.execute(&show, &HashMap::new()).unwrap();
        assert_eq!(shown.rows.len(), 1);
        assert_eq!(
            shown.rows[0].get("name"),
            Some(&Value::String("person_idx".to_string()))
        );
        assert_eq!(
            shown.rows[0].get("kind"),
            Some(&Value::String("RANGE".to_string()))
        );
    }

    #[test]
    fn test_range_index_ddl_roundtrip() {
        let engine = make_engine();
        let parser = Parser::new();

        let create = parser
            .parse("CREATE RANGE INDEX person_idx FOR (n:Person) ON (n.email)")
            .unwrap();
        engine.execute(&create, &HashMap::new()).unwrap();

        let show = parser.parse("SHOW RANGE INDEXES").unwrap();
        let shown = engine.execute(&show, &HashMap::new()).unwrap();
        assert_eq!(shown.rows.len(), 1);
        assert_eq!(
            shown.rows[0].get("name"),
            Some(&Value::String("person_idx".to_string()))
        );
        assert_eq!(
            shown.rows[0].get("kind"),
            Some(&Value::String("RANGE".to_string()))
        );
    }

    #[test]
    fn test_relationship_index_ddl_roundtrip() {
        let engine = make_engine();
        let parser = Parser::new();

        let create = parser
            .parse("CREATE INDEX follows_weight_idx FOR ()-[r:FOLLOWS]-() ON (r.weight)")
            .unwrap();
        engine.execute(&create, &HashMap::new()).unwrap();

        let show = parser.parse("SHOW INDEXES").unwrap();
        let shown = engine.execute(&show, &HashMap::new()).unwrap();
        assert_eq!(shown.rows.len(), 1);
        assert_eq!(
            shown.rows[0].get("name"),
            Some(&Value::String("follows_weight_idx".to_string()))
        );
        assert_eq!(
            shown.rows[0].get("entityType"),
            Some(&Value::String("RELATIONSHIP".to_string()))
        );
        assert_eq!(
            shown.rows[0].get("kind"),
            Some(&Value::String("RANGE".to_string()))
        );
        assert_eq!(
            shown.rows[0].get("label"),
            Some(&Value::String("FOLLOWS".to_string()))
        );
        assert_eq!(
            shown.rows[0].get("properties"),
            Some(&Value::Array(vec![Value::String("weight".to_string())]))
        );
    }

    #[test]
    fn test_relationship_range_index_ddl_roundtrip() {
        let engine = make_engine();
        let parser = Parser::new();

        let create = parser
            .parse("CREATE RANGE INDEX follows_weight_idx FOR ()-[r:FOLLOWS]-() ON (r.weight)")
            .unwrap();
        engine.execute(&create, &HashMap::new()).unwrap();

        let show = parser.parse("SHOW RANGE INDEXES").unwrap();
        let shown = engine.execute(&show, &HashMap::new()).unwrap();
        assert_eq!(shown.rows.len(), 1);
        assert_eq!(
            shown.rows[0].get("name"),
            Some(&Value::String("follows_weight_idx".to_string()))
        );
        assert_eq!(
            shown.rows[0].get("entityType"),
            Some(&Value::String("RELATIONSHIP".to_string()))
        );
        assert_eq!(
            shown.rows[0].get("kind"),
            Some(&Value::String("RANGE".to_string()))
        );
        assert_eq!(
            shown.rows[0].get("label"),
            Some(&Value::String("FOLLOWS".to_string()))
        );
        assert_eq!(
            shown.rows[0].get("properties"),
            Some(&Value::Array(vec![Value::String("weight".to_string())]))
        );
    }

    #[test]
    fn test_non_range_index_ddl_roundtrip() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE TEMPORAL INDEX person_seen_at_idx FOR (n:Person) ON (n.seenAt)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE FULLTEXT INDEX follows_note_idx FOR ()-[r:FOLLOWS]-() ON (r.note)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE VECTOR INDEX person_embedding_idx FOR (n:Person) ON (n.embedding)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let temporal = engine
            .execute(&parser.parse("SHOW TEMPORAL INDEXES").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(temporal.rows.len(), 1);
        assert_eq!(
            temporal.rows[0].get("name"),
            Some(&Value::String("person_seen_at_idx".to_string()))
        );
        assert_eq!(
            temporal.rows[0].get("kind"),
            Some(&Value::String("TEMPORAL".to_string()))
        );

        let fulltext = engine
            .execute(&parser.parse("SHOW FULLTEXT INDEXES").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(fulltext.rows.len(), 1);
        assert_eq!(
            fulltext.rows[0].get("name"),
            Some(&Value::String("follows_note_idx".to_string()))
        );
        assert_eq!(
            fulltext.rows[0].get("entityType"),
            Some(&Value::String("RELATIONSHIP".to_string()))
        );
        assert_eq!(
            fulltext.rows[0].get("kind"),
            Some(&Value::String("FULLTEXT".to_string()))
        );

        let vector = engine
            .execute(&parser.parse("SHOW VECTOR INDEXES").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(vector.rows.len(), 1);
        assert_eq!(
            vector.rows[0].get("name"),
            Some(&Value::String("person_embedding_idx".to_string()))
        );
        assert_eq!(
            vector.rows[0].get("kind"),
            Some(&Value::String("VECTOR".to_string()))
        );
    }

    #[test]
    fn test_relationship_index_ddl_if_not_exists_and_duplicate_error_path() {
        let engine = make_engine();
        let parser = Parser::new();

        let create = parser
            .parse("CREATE INDEX follows_weight_idx FOR ()-[r:FOLLOWS]-() ON (r.weight)")
            .unwrap();
        engine.execute(&create, &HashMap::new()).unwrap();

        let err = match engine.execute(&create, &HashMap::new()) {
            Ok(_) => panic!("expected duplicate relationship index create to fail"),
            Err(err) => err,
        };
        assert!(err
            .to_string()
            .contains("index already exists: follows_weight_idx"));

        let create_if_not_exists = parser
            .parse(
                "CREATE INDEX follows_weight_idx IF NOT EXISTS FOR ()-[r:FOLLOWS]-() ON (r.weight)",
            )
            .unwrap();
        engine
            .execute(&create_if_not_exists, &HashMap::new())
            .unwrap();

        let show = parser.parse("SHOW INDEXES").unwrap();
        let shown = engine.execute(&show, &HashMap::new()).unwrap();
        assert_eq!(shown.rows.len(), 1);
        assert_eq!(
            shown.rows[0].get("name"),
            Some(&Value::String("follows_weight_idx".to_string()))
        );
        assert_eq!(
            shown.rows[0].get("entityType"),
            Some(&Value::String("RELATIONSHIP".to_string()))
        );
    }

    #[test]
    fn test_relationship_range_index_ddl_if_not_exists_and_duplicate_error_path() {
        let engine = make_engine();
        let parser = Parser::new();

        let create = parser
            .parse("CREATE RANGE INDEX follows_weight_idx FOR ()-[r:FOLLOWS]-() ON (r.weight)")
            .unwrap();
        engine.execute(&create, &HashMap::new()).unwrap();

        let err = match engine.execute(&create, &HashMap::new()) {
            Ok(_) => panic!("expected duplicate relationship range index create to fail"),
            Err(err) => err,
        };
        assert!(err
            .to_string()
            .contains("index already exists: follows_weight_idx"));

        let create_if_not_exists = parser
            .parse(
                "CREATE RANGE INDEX follows_weight_idx IF NOT EXISTS FOR ()-[r:FOLLOWS]-() ON (r.weight)",
            )
            .unwrap();
        engine
            .execute(&create_if_not_exists, &HashMap::new())
            .unwrap();

        let show = parser.parse("SHOW RANGE INDEXES").unwrap();
        let shown = engine.execute(&show, &HashMap::new()).unwrap();
        assert_eq!(shown.rows.len(), 1);
        assert_eq!(
            shown.rows[0].get("name"),
            Some(&Value::String("follows_weight_idx".to_string()))
        );
        assert_eq!(
            shown.rows[0].get("entityType"),
            Some(&Value::String("RELATIONSHIP".to_string()))
        );
        assert_eq!(
            shown.rows[0].get("kind"),
            Some(&Value::String("RANGE".to_string()))
        );
    }

    #[test]
    fn test_non_range_index_ddl_if_not_exists_and_duplicate_error_paths() {
        let engine = make_engine();
        let parser = Parser::new();

        let temporal_create = parser
            .parse("CREATE TEMPORAL INDEX person_seen_at_idx FOR (n:Person) ON (n.seenAt)")
            .unwrap();
        engine.execute(&temporal_create, &HashMap::new()).unwrap();

        let temporal_err = match engine.execute(&temporal_create, &HashMap::new()) {
            Ok(_) => panic!("expected duplicate temporal index create to fail"),
            Err(err) => err,
        };
        assert!(temporal_err
            .to_string()
            .contains("index already exists: person_seen_at_idx"));

        let temporal_create_if_not_exists = parser
            .parse(
                "CREATE TEMPORAL INDEX person_seen_at_idx IF NOT EXISTS FOR (n:Person) ON (n.seenAt)",
            )
            .unwrap();
        engine
            .execute(&temporal_create_if_not_exists, &HashMap::new())
            .unwrap();

        let temporal_show = engine
            .execute(&parser.parse("SHOW TEMPORAL INDEXES").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(temporal_show.rows.len(), 1);
        assert_eq!(
            temporal_show.rows[0].get("name"),
            Some(&Value::String("person_seen_at_idx".to_string()))
        );
        assert_eq!(
            temporal_show.rows[0].get("kind"),
            Some(&Value::String("TEMPORAL".to_string()))
        );

        let fulltext_create = parser
            .parse("CREATE FULLTEXT INDEX follows_note_idx FOR ()-[r:FOLLOWS]-() ON (r.note)")
            .unwrap();
        engine.execute(&fulltext_create, &HashMap::new()).unwrap();

        let fulltext_err = match engine.execute(&fulltext_create, &HashMap::new()) {
            Ok(_) => panic!("expected duplicate fulltext index create to fail"),
            Err(err) => err,
        };
        assert!(fulltext_err
            .to_string()
            .contains("index already exists: follows_note_idx"));

        let fulltext_create_if_not_exists = parser
            .parse(
                "CREATE FULLTEXT INDEX follows_note_idx IF NOT EXISTS FOR ()-[r:FOLLOWS]-() ON (r.note)",
            )
            .unwrap();
        engine
            .execute(&fulltext_create_if_not_exists, &HashMap::new())
            .unwrap();

        let fulltext_show = engine
            .execute(&parser.parse("SHOW FULLTEXT INDEXES").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(fulltext_show.rows.len(), 1);
        assert_eq!(
            fulltext_show.rows[0].get("name"),
            Some(&Value::String("follows_note_idx".to_string()))
        );
        assert_eq!(
            fulltext_show.rows[0].get("entityType"),
            Some(&Value::String("RELATIONSHIP".to_string()))
        );
        assert_eq!(
            fulltext_show.rows[0].get("kind"),
            Some(&Value::String("FULLTEXT".to_string()))
        );

        let vector_create = parser
            .parse("CREATE VECTOR INDEX person_embedding_idx FOR (n:Person) ON (n.embedding)")
            .unwrap();
        engine.execute(&vector_create, &HashMap::new()).unwrap();

        let vector_err = match engine.execute(&vector_create, &HashMap::new()) {
            Ok(_) => panic!("expected duplicate vector index create to fail"),
            Err(err) => err,
        };
        assert!(vector_err
            .to_string()
            .contains("index already exists: person_embedding_idx"));

        let vector_create_if_not_exists = parser
            .parse(
                "CREATE VECTOR INDEX person_embedding_idx IF NOT EXISTS FOR (n:Person) ON (n.embedding)",
            )
            .unwrap();
        engine
            .execute(&vector_create_if_not_exists, &HashMap::new())
            .unwrap();

        let vector_show = engine
            .execute(&parser.parse("SHOW VECTOR INDEXES").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(vector_show.rows.len(), 1);
        assert_eq!(
            vector_show.rows[0].get("name"),
            Some(&Value::String("person_embedding_idx".to_string()))
        );
        assert_eq!(
            vector_show.rows[0].get("kind"),
            Some(&Value::String("VECTOR".to_string()))
        );
    }

    #[test]
    fn test_composite_relationship_index_ddl_roundtrip_preserves_property_order() {
        let engine = make_engine();
        let parser = Parser::new();

        let create = parser
            .parse(
                "CREATE INDEX follows_weight_since_idx FOR ()-[r:FOLLOWS]-() ON (r.weight, r.since)",
            )
            .unwrap();
        engine.execute(&create, &HashMap::new()).unwrap();

        let show = parser.parse("SHOW INDEXES").unwrap();
        let shown = engine.execute(&show, &HashMap::new()).unwrap();
        assert_eq!(shown.rows.len(), 1);
        assert_eq!(
            shown.rows[0].get("name"),
            Some(&Value::String("follows_weight_since_idx".to_string()))
        );
        assert_eq!(
            shown.rows[0].get("entityType"),
            Some(&Value::String("RELATIONSHIP".to_string()))
        );
        assert_eq!(
            shown.rows[0].get("properties"),
            Some(&Value::Array(vec![
                Value::String("weight".to_string()),
                Value::String("since".to_string()),
            ]))
        );
    }

    #[test]
    fn test_composite_relationship_range_index_ddl_roundtrip_preserves_property_order() {
        let engine = make_engine();
        let parser = Parser::new();

        let create = parser
            .parse(
                "CREATE RANGE INDEX follows_weight_since_idx FOR ()-[r:FOLLOWS]-() ON (r.weight, r.since)",
            )
            .unwrap();
        engine.execute(&create, &HashMap::new()).unwrap();

        let show = parser.parse("SHOW RANGE INDEXES").unwrap();
        let shown = engine.execute(&show, &HashMap::new()).unwrap();
        assert_eq!(shown.rows.len(), 1);
        assert_eq!(
            shown.rows[0].get("name"),
            Some(&Value::String("follows_weight_since_idx".to_string()))
        );
        assert_eq!(
            shown.rows[0].get("entityType"),
            Some(&Value::String("RELATIONSHIP".to_string()))
        );
        assert_eq!(
            shown.rows[0].get("properties"),
            Some(&Value::Array(vec![
                Value::String("weight".to_string()),
                Value::String("since".to_string()),
            ]))
        );
    }

    #[test]
    fn test_range_index_supports_simple_where_comparisons() {
        let engine = make_engine();
        let parser = Parser::new();

        store_node(
            engine.storage.as_ref(),
            "person:1",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Alice".to_string())),
                ("age".to_string(), Value::from(29)),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:2",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Bob".to_string())),
                ("age".to_string(), Value::from(35)),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:3",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Carol".to_string())),
                ("age".to_string(), Value::from(41)),
            ]),
        );

        engine
            .execute(
                &parser
                    .parse("CREATE RANGE INDEX person_age_idx FOR (n:Person) ON (n.age)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person) WHERE n.age >= 35 RETURN n.name AS name ORDER BY n.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["name"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("name"), Some(&Value::String("Bob".to_string())));
        assert_eq!(result.rows[1].get("name"), Some(&Value::String("Carol".to_string())));
    }

    #[test]
    fn test_temporal_index_supports_simple_where_comparisons() {
        let engine = make_engine();
        let parser = Parser::new();

        store_node(
            engine.storage.as_ref(),
            "person:1",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Alice".to_string())),
                (
                    "seenAt".to_string(),
                    Value::String("2024-01-01T00:00:00Z".to_string()),
                ),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:2",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Bob".to_string())),
                (
                    "seenAt".to_string(),
                    Value::String("2024-06-01T00:00:00Z".to_string()),
                ),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:3",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Carol".to_string())),
                (
                    "seenAt".to_string(),
                    Value::String("2025-01-01T00:00:00Z".to_string()),
                ),
            ]),
        );

        engine
            .execute(
                &parser
                    .parse("CREATE TEMPORAL INDEX person_seen_at_idx FOR (n:Person) ON (n.seenAt)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person) WHERE n.seenAt > '2024-03-01T00:00:00Z' RETURN n.name AS name ORDER BY n.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["name"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("name"), Some(&Value::String("Bob".to_string())));
        assert_eq!(result.rows[1].get("name"), Some(&Value::String("Carol".to_string())));
    }

    #[test]
    fn test_composite_temporal_index_supports_simple_where_comparisons_with_exact_suffix_properties() {
        let engine = make_engine();
        let parser = Parser::new();

        store_node(
            engine.storage.as_ref(),
            "person:1",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Alice".to_string())),
                ("seenAt".to_string(), Value::String("2024-01-01T00:00:00Z".to_string())),
                ("team".to_string(), Value::String("ops".to_string())),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:2",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Bob".to_string())),
                ("seenAt".to_string(), Value::String("2024-06-01T00:00:00Z".to_string())),
                ("team".to_string(), Value::String("ops".to_string())),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:3",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Carol".to_string())),
                ("seenAt".to_string(), Value::String("2025-01-01T00:00:00Z".to_string())),
                ("team".to_string(), Value::String("sales".to_string())),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:4",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Drew".to_string())),
                ("seenAt".to_string(), Value::String("2025-03-01T00:00:00Z".to_string())),
                ("team".to_string(), Value::String("ops".to_string())),
            ]),
        );

        engine
            .execute(
                &parser
                    .parse("CREATE TEMPORAL INDEX person_seen_at_team_idx FOR (n:Person) ON (n.seenAt, n.team)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person {team: 'ops'}) WHERE n.seenAt > '2024-03-01T00:00:00Z' RETURN n.name AS name ORDER BY n.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["name"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("name"), Some(&Value::String("Bob".to_string())));
        assert_eq!(result.rows[1].get("name"), Some(&Value::String("Drew".to_string())));
    }

    #[test]
    fn test_non_leading_composite_temporal_index_supports_simple_where_comparisons_with_exact_prefix_properties() {
        let engine = make_engine();
        let parser = Parser::new();

        store_node(
            engine.storage.as_ref(),
            "person:1",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Alice".to_string())),
                ("team".to_string(), Value::String("ops".to_string())),
                ("seenAt".to_string(), Value::String("2024-01-01T00:00:00Z".to_string())),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:2",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Bob".to_string())),
                ("team".to_string(), Value::String("ops".to_string())),
                ("seenAt".to_string(), Value::String("2024-06-01T00:00:00Z".to_string())),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:3",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Carol".to_string())),
                ("team".to_string(), Value::String("sales".to_string())),
                ("seenAt".to_string(), Value::String("2025-01-01T00:00:00Z".to_string())),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:4",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Drew".to_string())),
                ("team".to_string(), Value::String("ops".to_string())),
                ("seenAt".to_string(), Value::String("2025-03-01T00:00:00Z".to_string())),
            ]),
        );

        engine
            .execute(
                &parser
                    .parse("CREATE TEMPORAL INDEX person_team_seen_at_idx FOR (n:Person) ON (n.team, n.seenAt)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person {team: 'ops'}) WHERE n.seenAt > '2024-03-01T00:00:00Z' RETURN n.name AS name ORDER BY n.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["name"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("name"), Some(&Value::String("Bob".to_string())));
        assert_eq!(result.rows[1].get("name"), Some(&Value::String("Drew".to_string())));
    }

    #[test]
    fn test_composite_range_index_supports_simple_where_comparisons_with_exact_suffix_properties() {
        let engine = make_engine();
        let parser = Parser::new();

        store_node(
            engine.storage.as_ref(),
            "person:1",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Alice".to_string())),
                ("age".to_string(), Value::from(29)),
                ("team".to_string(), Value::String("ops".to_string())),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:2",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Bob".to_string())),
                ("age".to_string(), Value::from(35)),
                ("team".to_string(), Value::String("ops".to_string())),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:3",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Carol".to_string())),
                ("age".to_string(), Value::from(41)),
                ("team".to_string(), Value::String("sales".to_string())),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:4",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Drew".to_string())),
                ("age".to_string(), Value::from(43)),
                ("team".to_string(), Value::String("ops".to_string())),
            ]),
        );

        engine
            .execute(
                &parser
                    .parse("CREATE RANGE INDEX person_age_team_idx FOR (n:Person) ON (n.age, n.team)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person {team: 'ops'}) WHERE n.age > 30 RETURN n.name AS name ORDER BY n.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["name"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("name"), Some(&Value::String("Bob".to_string())));
        assert_eq!(result.rows[1].get("name"), Some(&Value::String("Drew".to_string())));
    }

    #[test]
    fn test_composite_range_index_supports_all_simple_comparison_operators_with_exact_suffix_properties() {
        let engine = make_engine();
        let parser = Parser::new();

        store_node(
            engine.storage.as_ref(),
            "person:1",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Alice".to_string())),
                ("age".to_string(), Value::from(29)),
                ("team".to_string(), Value::String("ops".to_string())),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:2",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Bob".to_string())),
                ("age".to_string(), Value::from(35)),
                ("team".to_string(), Value::String("ops".to_string())),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:3",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Carol".to_string())),
                ("age".to_string(), Value::from(41)),
                ("team".to_string(), Value::String("sales".to_string())),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:4",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Drew".to_string())),
                ("age".to_string(), Value::from(43)),
                ("team".to_string(), Value::String("ops".to_string())),
            ]),
        );

        engine
            .execute(
                &parser
                    .parse("CREATE RANGE INDEX person_age_team_idx FOR (n:Person) ON (n.age, n.team)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let greater_than_or_equal = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person {team: 'ops'}) WHERE n.age >= 35 RETURN n.name AS name ORDER BY n.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(greater_than_or_equal.columns, vec!["name"]);
        assert_eq!(greater_than_or_equal.rows.len(), 2);
        assert_eq!(
            greater_than_or_equal.rows[0].get("name"),
            Some(&Value::String("Bob".to_string()))
        );
        assert_eq!(
            greater_than_or_equal.rows[1].get("name"),
            Some(&Value::String("Drew".to_string()))
        );

        let less_than = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person {team: 'ops'}) WHERE n.age < 40 RETURN n.name AS name ORDER BY n.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(less_than.columns, vec!["name"]);
        assert_eq!(less_than.rows.len(), 2);
        assert_eq!(less_than.rows[0].get("name"), Some(&Value::String("Alice".to_string())));
        assert_eq!(less_than.rows[1].get("name"), Some(&Value::String("Bob".to_string())));

        let less_than_or_equal = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person {team: 'ops'}) WHERE n.age <= 35 RETURN n.name AS name ORDER BY n.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(less_than_or_equal.columns, vec!["name"]);
        assert_eq!(less_than_or_equal.rows.len(), 2);
        assert_eq!(
            less_than_or_equal.rows[0].get("name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(
            less_than_or_equal.rows[1].get("name"),
            Some(&Value::String("Bob".to_string()))
        );
    }

    #[test]
    fn test_composite_range_index_supports_reversed_operand_comparisons_with_exact_suffix_properties() {
        let engine = make_engine();
        let parser = Parser::new();

        store_node(
            engine.storage.as_ref(),
            "person:1",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Alice".to_string())),
                ("age".to_string(), Value::from(29)),
                ("team".to_string(), Value::String("ops".to_string())),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:2",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Bob".to_string())),
                ("age".to_string(), Value::from(35)),
                ("team".to_string(), Value::String("ops".to_string())),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:3",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Carol".to_string())),
                ("age".to_string(), Value::from(41)),
                ("team".to_string(), Value::String("sales".to_string())),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:4",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Drew".to_string())),
                ("age".to_string(), Value::from(43)),
                ("team".to_string(), Value::String("ops".to_string())),
            ]),
        );

        engine
            .execute(
                &parser
                    .parse("CREATE RANGE INDEX person_age_team_idx FOR (n:Person) ON (n.age, n.team)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let less_than_reversed = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person {team: 'ops'}) WHERE 35 >= n.age RETURN n.name AS name ORDER BY n.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(less_than_reversed.columns, vec!["name"]);
        assert_eq!(less_than_reversed.rows.len(), 2);
        assert_eq!(
            less_than_reversed.rows[0].get("name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(
            less_than_reversed.rows[1].get("name"),
            Some(&Value::String("Bob".to_string()))
        );

        let greater_than_reversed = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person {team: 'ops'}) WHERE 35 <= n.age RETURN n.name AS name ORDER BY n.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(greater_than_reversed.columns, vec!["name"]);
        assert_eq!(greater_than_reversed.rows.len(), 2);
        assert_eq!(
            greater_than_reversed.rows[0].get("name"),
            Some(&Value::String("Bob".to_string()))
        );
        assert_eq!(
            greater_than_reversed.rows[1].get("name"),
            Some(&Value::String("Drew".to_string()))
        );
    }

    #[test]
    fn test_composite_range_index_supports_simple_where_comparisons_without_exact_suffix_properties() {
        let engine = make_engine();
        let parser = Parser::new();

        store_node(
            engine.storage.as_ref(),
            "person:1",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Alice".to_string())),
                ("age".to_string(), Value::from(29)),
                ("team".to_string(), Value::String("ops".to_string())),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:2",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Bob".to_string())),
                ("age".to_string(), Value::from(35)),
                ("team".to_string(), Value::String("ops".to_string())),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:3",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Carol".to_string())),
                ("age".to_string(), Value::from(41)),
                ("team".to_string(), Value::String("sales".to_string())),
            ]),
        );

        engine
            .execute(
                &parser
                    .parse("CREATE RANGE INDEX person_age_team_idx FOR (n:Person) ON (n.age, n.team)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person) WHERE n.age > 30 RETURN n.name AS name ORDER BY n.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["name"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("name"), Some(&Value::String("Bob".to_string())));
        assert_eq!(result.rows[1].get("name"), Some(&Value::String("Carol".to_string())));
    }

    #[test]
    fn test_non_leading_composite_range_index_supports_simple_where_comparisons_with_exact_prefix_properties() {
        let engine = make_engine();
        let parser = Parser::new();

        store_node(
            engine.storage.as_ref(),
            "person:1",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Alice".to_string())),
                ("team".to_string(), Value::String("ops".to_string())),
                ("age".to_string(), Value::from(29)),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:2",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Bob".to_string())),
                ("team".to_string(), Value::String("ops".to_string())),
                ("age".to_string(), Value::from(35)),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:3",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Carol".to_string())),
                ("team".to_string(), Value::String("sales".to_string())),
                ("age".to_string(), Value::from(41)),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:4",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Drew".to_string())),
                ("team".to_string(), Value::String("ops".to_string())),
                ("age".to_string(), Value::from(43)),
            ]),
        );

        engine
            .execute(
                &parser
                    .parse("CREATE RANGE INDEX person_team_age_idx FOR (n:Person) ON (n.team, n.age)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person {team: 'ops'}) WHERE n.age > 30 RETURN n.name AS name ORDER BY n.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["name"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("name"), Some(&Value::String("Bob".to_string())));
        assert_eq!(result.rows[1].get("name"), Some(&Value::String("Drew".to_string())));
    }

    #[test]
    fn test_non_leading_composite_range_index_supports_all_simple_comparison_operators_with_exact_prefix_properties() {
        let engine = make_engine();
        let parser = Parser::new();

        store_node(
            engine.storage.as_ref(),
            "person:1",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Alice".to_string())),
                ("team".to_string(), Value::String("ops".to_string())),
                ("age".to_string(), Value::from(29)),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:2",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Bob".to_string())),
                ("team".to_string(), Value::String("ops".to_string())),
                ("age".to_string(), Value::from(35)),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:3",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Carol".to_string())),
                ("team".to_string(), Value::String("sales".to_string())),
                ("age".to_string(), Value::from(41)),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:4",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Drew".to_string())),
                ("team".to_string(), Value::String("ops".to_string())),
                ("age".to_string(), Value::from(43)),
            ]),
        );

        engine
            .execute(
                &parser
                    .parse("CREATE RANGE INDEX person_team_age_idx FOR (n:Person) ON (n.team, n.age)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let greater_than_or_equal = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person {team: 'ops'}) WHERE n.age >= 35 RETURN n.name AS name ORDER BY n.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(greater_than_or_equal.columns, vec!["name"]);
        assert_eq!(greater_than_or_equal.rows.len(), 2);
        assert_eq!(
            greater_than_or_equal.rows[0].get("name"),
            Some(&Value::String("Bob".to_string()))
        );
        assert_eq!(
            greater_than_or_equal.rows[1].get("name"),
            Some(&Value::String("Drew".to_string()))
        );

        let less_than = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person {team: 'ops'}) WHERE n.age < 40 RETURN n.name AS name ORDER BY n.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(less_than.columns, vec!["name"]);
        assert_eq!(less_than.rows.len(), 2);
        assert_eq!(less_than.rows[0].get("name"), Some(&Value::String("Alice".to_string())));
        assert_eq!(less_than.rows[1].get("name"), Some(&Value::String("Bob".to_string())));

        let less_than_or_equal = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person {team: 'ops'}) WHERE n.age <= 35 RETURN n.name AS name ORDER BY n.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(less_than_or_equal.columns, vec!["name"]);
        assert_eq!(less_than_or_equal.rows.len(), 2);
        assert_eq!(
            less_than_or_equal.rows[0].get("name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(
            less_than_or_equal.rows[1].get("name"),
            Some(&Value::String("Bob".to_string()))
        );
    }

    #[test]
    fn test_non_leading_composite_range_index_supports_reversed_operand_comparisons_with_exact_prefix_properties() {
        let engine = make_engine();
        let parser = Parser::new();

        store_node(
            engine.storage.as_ref(),
            "person:1",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Alice".to_string())),
                ("team".to_string(), Value::String("ops".to_string())),
                ("age".to_string(), Value::from(29)),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:2",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Bob".to_string())),
                ("team".to_string(), Value::String("ops".to_string())),
                ("age".to_string(), Value::from(35)),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:3",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Carol".to_string())),
                ("team".to_string(), Value::String("sales".to_string())),
                ("age".to_string(), Value::from(41)),
            ]),
        );
        store_node(
            engine.storage.as_ref(),
            "person:4",
            &["Person"],
            HashMap::from([
                ("name".to_string(), Value::String("Drew".to_string())),
                ("team".to_string(), Value::String("ops".to_string())),
                ("age".to_string(), Value::from(43)),
            ]),
        );

        engine
            .execute(
                &parser
                    .parse("CREATE RANGE INDEX person_team_age_idx FOR (n:Person) ON (n.team, n.age)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let less_than_reversed = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person {team: 'ops'}) WHERE 35 >= n.age RETURN n.name AS name ORDER BY n.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(less_than_reversed.columns, vec!["name"]);
        assert_eq!(less_than_reversed.rows.len(), 2);
        assert_eq!(
            less_than_reversed.rows[0].get("name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(
            less_than_reversed.rows[1].get("name"),
            Some(&Value::String("Bob".to_string()))
        );

        let greater_than_reversed = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person {team: 'ops'}) WHERE 35 < n.age RETURN n.name AS name ORDER BY n.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(greater_than_reversed.columns, vec!["name"]);
        assert_eq!(greater_than_reversed.rows.len(), 1);
        assert_eq!(
            greater_than_reversed.rows[0].get("name"),
            Some(&Value::String("Drew".to_string()))
        );
    }

    #[test]
    fn test_index_drop_if_exists_and_error_path() {
        let engine = make_engine();
        let parser = Parser::new();

        let err = match engine.execute(
            &parser.parse("DROP INDEX missing_idx").unwrap(),
            &HashMap::new(),
        ) {
            Ok(_) => panic!("expected drop index to fail"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("index not found: missing_idx"));

        engine
            .execute(
                &parser.parse("DROP INDEX missing_idx IF EXISTS").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
    }

    #[test]
    fn test_non_range_index_drop_if_exists_and_error_paths() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE TEMPORAL INDEX person_seen_at_idx FOR (n:Person) ON (n.seenAt)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE FULLTEXT INDEX follows_note_idx FOR ()-[r:FOLLOWS]-() ON (r.note)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE VECTOR INDEX person_embedding_idx FOR (n:Person) ON (n.embedding)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .execute(
                &parser.parse("DROP INDEX person_seen_at_idx").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("DROP INDEX follows_note_idx").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("DROP INDEX person_embedding_idx").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let temporal = engine
            .execute(&parser.parse("SHOW TEMPORAL INDEXES").unwrap(), &HashMap::new())
            .unwrap();
        assert!(temporal.rows.is_empty());

        let fulltext = engine
            .execute(&parser.parse("SHOW FULLTEXT INDEXES").unwrap(), &HashMap::new())
            .unwrap();
        assert!(fulltext.rows.is_empty());

        let vector = engine
            .execute(&parser.parse("SHOW VECTOR INDEXES").unwrap(), &HashMap::new())
            .unwrap();
        assert!(vector.rows.is_empty());

        let err = match engine.execute(
            &parser.parse("DROP INDEX person_seen_at_idx").unwrap(),
            &HashMap::new(),
        ) {
            Ok(_) => panic!("expected drop temporal index to fail after deletion"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("index not found: person_seen_at_idx"));

        engine
            .execute(
                &parser.parse("DROP INDEX person_seen_at_idx IF EXISTS").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("DROP INDEX follows_note_idx IF EXISTS").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("DROP INDEX person_embedding_idx IF EXISTS").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
    }

    #[test]
    fn test_show_indexes_filters_non_range_kinds() {
        let engine = make_engine();
        let parser = Parser::new();
        let catalog = copperdb_indexing::IndexCatalog::new(engine.storage.as_ref());

        catalog
            .create(copperdb_indexing::CatalogIndexDefinition {
                name: "person_age_idx".to_string(),
                entity_type: copperdb_indexing::CatalogIndexEntityType::Node,
                kind: copperdb_indexing::CatalogIndexKind::Range,
                label: "Person".to_string(),
                properties: vec!["age".to_string()],
            })
            .unwrap();
        catalog
            .create(copperdb_indexing::CatalogIndexDefinition {
                name: "person_seen_at_idx".to_string(),
                entity_type: copperdb_indexing::CatalogIndexEntityType::Node,
                kind: copperdb_indexing::CatalogIndexKind::Temporal,
                label: "Person".to_string(),
                properties: vec!["seenAt".to_string()],
            })
            .unwrap();
        catalog
            .create(copperdb_indexing::CatalogIndexDefinition {
                name: "person_bio_idx".to_string(),
                entity_type: copperdb_indexing::CatalogIndexEntityType::Node,
                kind: copperdb_indexing::CatalogIndexKind::FullText,
                label: "Person".to_string(),
                properties: vec!["bio".to_string()],
            })
            .unwrap();
        catalog
            .create(copperdb_indexing::CatalogIndexDefinition {
                name: "person_embedding_idx".to_string(),
                entity_type: copperdb_indexing::CatalogIndexEntityType::Node,
                kind: copperdb_indexing::CatalogIndexKind::Vector,
                label: "Person".to_string(),
                properties: vec!["embedding".to_string()],
            })
            .unwrap();

        let temporal = engine
            .execute(&parser.parse("SHOW TEMPORAL INDEXES").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(temporal.columns, vec!["name", "entityType", "kind", "label", "properties"]);
        assert_eq!(temporal.rows.len(), 1);
        assert_eq!(
            temporal.rows[0].get("name"),
            Some(&Value::String("person_seen_at_idx".to_string()))
        );
        assert_eq!(
            temporal.rows[0].get("kind"),
            Some(&Value::String("TEMPORAL".to_string()))
        );

        let fulltext = engine
            .execute(&parser.parse("SHOW FULLTEXT INDEXES").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(fulltext.rows.len(), 1);
        assert_eq!(
            fulltext.rows[0].get("name"),
            Some(&Value::String("person_bio_idx".to_string()))
        );
        assert_eq!(
            fulltext.rows[0].get("kind"),
            Some(&Value::String("FULLTEXT".to_string()))
        );

        let vector = engine
            .execute(&parser.parse("SHOW VECTOR INDEXES").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(vector.rows.len(), 1);
        assert_eq!(
            vector.rows[0].get("name"),
            Some(&Value::String("person_embedding_idx".to_string()))
        );
        assert_eq!(
            vector.rows[0].get("kind"),
            Some(&Value::String("VECTOR".to_string()))
        );
    }

    #[test]
    fn test_knowledge_policy_decay_profile_ddl_roundtrip() {
        let engine = make_engine();
        let parser = Parser::new();

        let create = parser
            .parse(
                "CREATE DECAY PROFILE slow_decay OPTIONS { halfLifeSeconds: 604800, visibilityThreshold: 0.1, scoreFloor: 0.0, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
            )
            .unwrap();
        engine.execute(&create, &HashMap::new()).unwrap();

        let show = parser.parse("SHOW DECAY PROFILES").unwrap();
        let shown = engine.execute(&show, &HashMap::new()).unwrap();
        assert_eq!(shown.rows.len(), 1);
        assert_eq!(shown.rows[0].get("kind"), Some(&Value::String("bundle".to_string())));
        assert_eq!(
            shown.rows[0].get("name"),
            Some(&Value::String("slow_decay".to_string()))
        );

        let alter = parser
            .parse("ALTER DECAY PROFILE slow_decay SET OPTIONS { visibilityThreshold: 0.2 }")
            .unwrap();
        engine.execute(&alter, &HashMap::new()).unwrap();
        let shown = engine.execute(&show, &HashMap::new()).unwrap();
        assert_eq!(
            shown.rows[0].get("enabled"),
            Some(&Value::Bool(true))
        );

        let profiles = engine.storage.load_decay_profile_schemas().unwrap();
        assert_eq!(profiles[0].visibility_threshold, 0.2);
    }

    #[test]
    fn test_knowledge_policy_decay_binding_ddl_roundtrip() {
        let engine = make_engine();
        let parser = Parser::new();

        let create_bundle = parser
            .parse(
                "CREATE DECAY PROFILE slow_decay OPTIONS { halfLifeSeconds: 604800, visibilityThreshold: 0.1, scoreFloor: 0.0, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
            )
            .unwrap();
        engine.execute(&create_bundle, &HashMap::new()).unwrap();

        let create_binding = parser
            .parse(
                "CREATE DECAY PROFILE memory_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE slow_decay, visibilityThreshold: 0.2, order: 10 }",
            )
            .unwrap();
        engine.execute(&create_binding, &HashMap::new()).unwrap();

        let show = parser.parse("SHOW DECAY PROFILES").unwrap();
        let shown = engine.execute(&show, &HashMap::new()).unwrap();
        assert_eq!(shown.rows.len(), 2);

        let binding_row = shown
            .rows
            .iter()
            .find(|row| row.get("kind") == Some(&Value::String("binding".to_string())))
            .expect("binding row missing");
        assert_eq!(
            binding_row.get("name"),
            Some(&Value::String("memory_binding".to_string()))
        );
        assert_eq!(
            binding_row.get("target"),
            Some(&Value::String("MemoryEpisode".to_string()))
        );
        assert_eq!(
            binding_row.get("profileRef"),
            Some(&Value::String("slow_decay".to_string()))
        );

        let stored = engine.storage.load_decay_profile_binding_schemas().unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].order, 10);

        let drop_binding = parser.parse("DROP DECAY PROFILE memory_binding").unwrap();
        engine.execute(&drop_binding, &HashMap::new()).unwrap();
        assert!(engine.storage.load_decay_profile_binding_schemas().unwrap().is_empty());
    }

    #[test]
    fn test_knowledge_policy_resolver_builds_from_persisted_catalog() {
        let engine = make_engine();
        let parser = Parser::new();

        let create_bundle = parser
            .parse(
                "CREATE DECAY PROFILE slow_decay OPTIONS { halfLifeSeconds: 604800, visibilityThreshold: 0.1, scoreFloor: 0.0, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
            )
            .unwrap();
        engine.execute(&create_bundle, &HashMap::new()).unwrap();

        let create_binding = parser
            .parse(
                "CREATE DECAY PROFILE memory_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE slow_decay, visibilityThreshold: 0.2, order: 10 }",
            )
            .unwrap();
        engine.execute(&create_binding, &HashMap::new()).unwrap();

        let resolver = engine.knowledge_policy_resolver().unwrap();
        let resolved = resolver
            .resolve_node(&["MemoryEpisode".to_string()])
            .expect("binding should resolve");

        assert_eq!(resolved.decay_binding.name, "memory_binding");
        assert_eq!(resolved.decay_profile.as_ref().map(|profile| profile.name.as_str()), Some("slow_decay"));
        assert_eq!(resolved.visibility_threshold, 0.2);
    }

    #[test]
    fn test_match_hides_nodes_suppressed_by_created_age_threshold() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE short_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.5, scoreFloor: 0.0, function: 'step', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE memory_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE short_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "memory:old".to_string(),
                labels: vec!["MemoryEpisode".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Old memory".to_string()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: now_unix_ms() - 5_000,
                updated_at_unix_ms: now_unix_ms() - 5_000,
            })
            .unwrap();

        let query = parser
            .parse("MATCH (n:MemoryEpisode) RETURN n")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_match_keeps_fresh_nodes_visible_under_created_age_threshold() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE short_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.5, scoreFloor: 0.0, function: 'step', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE memory_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE short_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "memory:fresh".to_string(),
                labels: vec!["MemoryEpisode".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Fresh memory".to_string()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .unwrap();

        let query = parser
            .parse("MATCH (n:MemoryEpisode) RETURN n")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0]
                .get("n")
                .and_then(Value::as_object)
                .and_then(|props| props.get("name")),
            Some(&Value::String("Fresh memory".to_string()))
        );
    }

    #[test]
    fn test_match_hides_edges_suppressed_by_created_age_threshold() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE short_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.5, scoreFloor: 0.0, function: 'step', scope: 'EDGE', scoreFrom: 'CREATED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE stale_edge_binding FOR ()-[r:LINKS]-() APPLY { DECAY PROFILE short_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        for node in [
            NodeRecord {
                id: "person:a".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
            NodeRecord {
                id: "person:b".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
        ] {
            engine.storage.put_node_record(&node).unwrap();
        }
        engine
            .storage
            .put_edge_record(&EdgeRecord {
                id: "links:1".to_string(),
                start_node: "person:a".to_string(),
                end_node: "person:b".to_string(),
                edge_type: "LINKS".to_string(),
                properties: BTreeMap::from([("kind".to_string(), Value::String("stale".to_string()))]),
                created_at_unix_ms: now - 5_000,
                updated_at_unix_ms: now - 5_000,
            })
            .unwrap();

        let query = parser
            .parse("MATCH (:Person)-[r:LINKS]->(:Person) RETURN r")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_match_hides_nodes_suppressed_by_custom_anchor_threshold() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE review_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.5, scoreFloor: 0.0, function: 'step', scope: 'NODE', scoreFrom: 'CUSTOM', scoreFromProperty: 'reviewedAt', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE reviewed_memory_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE review_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "memory:custom-old".to_string(),
                labels: vec!["MemoryEpisode".to_string()],
                properties: BTreeMap::from([
                    ("name".to_string(), Value::String("Reviewed memory".to_string())),
                    ("reviewedAt".to_string(), Value::from(now - 5_000)),
                ]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .unwrap();

        let query = parser.parse("MATCH (n:MemoryEpisode) RETURN n").unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_match_hides_edges_suppressed_by_custom_anchor_threshold() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE review_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.5, scoreFloor: 0.0, function: 'step', scope: 'EDGE', scoreFrom: 'CUSTOM', scoreFromProperty: 'reviewedAt', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE reviewed_edge_binding FOR ()-[r:LINKS]-() APPLY { DECAY PROFILE review_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        for node in [
            NodeRecord {
                id: "person:custom-a".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
            NodeRecord {
                id: "person:custom-b".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
        ] {
            engine.storage.put_node_record(&node).unwrap();
        }
        engine
            .storage
            .put_edge_record(&EdgeRecord {
                id: "links:custom-1".to_string(),
                start_node: "person:custom-a".to_string(),
                end_node: "person:custom-b".to_string(),
                edge_type: "LINKS".to_string(),
                properties: BTreeMap::from([
                    ("kind".to_string(), Value::String("reviewed".to_string())),
                    ("reviewedAt".to_string(), Value::from(now - 5_000)),
                ]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .unwrap();

        let query = parser
            .parse("MATCH (:Person)-[r:LINKS]->(:Person) RETURN r")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_match_keeps_stale_nodes_visible_under_recent_last_access_anchor() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE access_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.5, scoreFloor: 0.0, function: 'step', scope: 'NODE', scoreFrom: 'LAST_ACCESSED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE access_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE access_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let stale_time = now_unix_ms() - 5_000;
        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "memory:recent-access".to_string(),
                labels: vec!["MemoryEpisode".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Recently accessed memory".to_string()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: stale_time,
                updated_at_unix_ms: stale_time,
            })
            .unwrap();
        engine
            .storage
            .put_knowledge_policy_access_metadata(
                "memory:recent-access",
                &copperdb_storage::KnowledgePolicyAccessMetadata {
                    last_accessed_at_unix_ms: Some(now_unix_ms()),
                    access_count: 1,
                },
            )
            .unwrap();

        let query = parser.parse("MATCH (n:MemoryEpisode) RETURN n").unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_match_keeps_stale_edges_visible_under_recent_last_access_anchor() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE access_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.5, scoreFloor: 0.0, function: 'step', scope: 'EDGE', scoreFrom: 'LAST_ACCESSED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE access_binding FOR ()-[r:LINKS]-() APPLY { DECAY PROFILE access_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        for node in [
            NodeRecord {
                id: "person:access-a".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
            NodeRecord {
                id: "person:access-b".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
        ] {
            engine.storage.put_node_record(&node).unwrap();
        }

        let stale_time = now - 5_000;
        engine
            .storage
            .put_edge_record(&EdgeRecord {
                id: "links:recent-access".to_string(),
                start_node: "person:access-a".to_string(),
                end_node: "person:access-b".to_string(),
                edge_type: "LINKS".to_string(),
                properties: BTreeMap::from([("kind".to_string(), Value::String("recently-accessed".to_string()))]),
                created_at_unix_ms: stale_time,
                updated_at_unix_ms: stale_time,
            })
            .unwrap();
        engine
            .storage
            .put_knowledge_policy_access_metadata(
                "links:recent-access",
                &copperdb_storage::KnowledgePolicyAccessMetadata {
                    last_accessed_at_unix_ms: Some(now_unix_ms()),
                    access_count: 2,
                },
            )
            .unwrap();

        let query = parser
            .parse("MATCH (:Person)-[r:LINKS]->(:Person) RETURN r")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_persist_node_props_refreshes_version_anchor_visibility_for_nodes() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE version_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.5, scoreFloor: 0.0, function: 'step', scope: 'NODE', scoreFrom: 'VERSION', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE version_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE version_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let stale_time = now_unix_ms() - 5_000;
        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "memory:versioned".to_string(),
                labels: vec!["MemoryEpisode".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Versioned memory".to_string()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: stale_time,
                updated_at_unix_ms: stale_time,
            })
            .unwrap();

        let match_query = parser.parse("MATCH (n:MemoryEpisode) RETURN n").unwrap();
        let hidden = engine.execute(&match_query, &HashMap::new()).unwrap();
        assert!(hidden.rows.is_empty());

        let mut refreshed_props = node_record_to_props(
            &engine
                .storage
                .get_node_record("memory:versioned")
                .unwrap()
                .unwrap(),
        );
        refreshed_props.insert("status".to_string(), Value::String("fresh".to_string()));
        engine.persist_node_props(&refreshed_props).unwrap();

        let shown = engine.execute(&match_query, &HashMap::new()).unwrap();
        assert_eq!(shown.rows.len(), 1);
        assert_eq!(
            shown.rows[0]
                .get("n")
                .and_then(Value::as_object)
                .and_then(|props| props.get("status")),
            Some(&Value::String("fresh".to_string()))
        );

        let stored = engine.storage.get_node_record("memory:versioned").unwrap().unwrap();
        assert!(stored.updated_at_unix_ms > stale_time);
        assert_eq!(stored.created_at_unix_ms, stale_time);
    }

    #[test]
    fn test_create_keeps_fresh_edges_visible_under_version_anchor() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE version_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.5, scoreFloor: 0.0, function: 'step', scope: 'EDGE', scoreFrom: 'VERSION', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE version_binding FOR ()-[r:LINKS]-() APPLY { DECAY PROFILE version_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let create_query = parser
            .parse(
                "CREATE (a:Person { id: 'person:version-a', name: 'Alice' })-[:LINKS { kind: 'fresh' }]->(b:Person { id: 'person:version-b', name: 'Bob' })",
            )
            .unwrap();
        engine.execute(&create_query, &HashMap::new()).unwrap();

        let match_query = parser
            .parse("MATCH (:Person)-[r:LINKS]->(:Person) RETURN r")
            .unwrap();
        let visible = engine.execute(&match_query, &HashMap::new()).unwrap();
        assert_eq!(visible.rows.len(), 1);

        let edge_id = visible.rows[0]
            .get("r")
            .and_then(Value::as_object)
            .and_then(|props| props.get("_id"))
            .and_then(Value::as_str)
            .expect("edge id should be present")
            .to_string();
        let stored = engine.storage.get_edge_record(&edge_id).unwrap().unwrap();
        assert!(stored.updated_at_unix_ms > 0);
        assert!(stored.created_at_unix_ms > 0);
    }

    #[test]
    fn test_knowledge_policy_promotion_profile_and_policy_roundtrip() {
        let engine = make_engine();
        let parser = Parser::new();

        let create_profile = parser
            .parse(
                "CREATE PROMOTION PROFILE boost_profile OPTIONS { scope: 'NODE', multiplier: 1.5, scoreFloor: 0.0, scoreCap: 1.0, enabled: true }",
            )
            .unwrap();
        engine.execute(&create_profile, &HashMap::new()).unwrap();

        let create_policy = parser
            .parse(
                "CREATE PROMOTION POLICY fact_policy FOR (n:KnowledgeFact) APPLY { ON ACCESS { SET n.lastAccessedAt = timestamp() } APPLY PROFILE boost_profile WHEN 'n.evidence >= 3' }",
            )
            .unwrap();
        engine.execute(&create_policy, &HashMap::new()).unwrap();

        let show_policies = parser.parse("SHOW PROMOTION POLICIES").unwrap();
        let shown = engine.execute(&show_policies, &HashMap::new()).unwrap();
        assert_eq!(shown.rows.len(), 1);
        assert_eq!(
            shown.rows[0].get("name"),
            Some(&Value::String("fact_policy".to_string()))
        );
        assert_eq!(shown.rows[0].get("isEdge"), Some(&Value::Bool(false)));
        assert_eq!(shown.rows[0].get("enabled"), Some(&Value::Bool(true)));
        assert_eq!(
            shown.rows[0].get("onAccessMutations"),
            Some(&Value::Array(vec![Value::String(
                "SET_LAST_ACCESSED_NOW".to_string(),
            )]))
        );

        let alter_policy = parser
            .parse("ALTER PROMOTION POLICY fact_policy SET ENABLED false")
            .unwrap();
        engine.execute(&alter_policy, &HashMap::new()).unwrap();
        let shown = engine.execute(&show_policies, &HashMap::new()).unwrap();
        assert_eq!(shown.rows[0].get("enabled"), Some(&Value::Bool(false)));
    }

    #[test]
    fn test_call_knowledgepolicy_resolve_by_entity_id_reports_scoring() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE access_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.75, scoreFloor: 0.0, function: 'step', scope: 'NODE', scoreFrom: 'LAST_ACCESSED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE access_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE access_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION PROFILE reinforcement OPTIONS { scope: 'NODE', multiplier: 2.0, scoreFloor: 0.8, scoreCap: 1.0, enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION POLICY reinforcement_policy FOR (n:MemoryEpisode) APPLY PROFILE reinforcement WHEN 'n.accessCount >= 3'",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let stale_time = now_unix_ms() - 5_000;
        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "memory:resolve-1".to_string(),
                labels: vec!["MemoryEpisode".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Inspectable memory".to_string()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: stale_time,
                updated_at_unix_ms: stale_time,
            })
            .unwrap();
        engine
            .storage
            .put_knowledge_policy_access_metadata(
                "memory:resolve-1",
                &copperdb_storage::KnowledgePolicyAccessMetadata {
                    last_accessed_at_unix_ms: Some(now_unix_ms() - 10_000),
                    access_count: 3,
                },
            )
            .unwrap();

        let query = parser
            .parse("CALL nornicdb.knowledgepolicy.resolve('memory:resolve-1', '', '')")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        assert_eq!(row.get("entityId"), Some(&Value::String("memory:resolve-1".to_string())));
        assert_eq!(row.get("decayBinding"), Some(&Value::String("access_binding".to_string())));
        assert_eq!(
            row.get("promotionPolicy"),
            Some(&Value::String("reinforcement_policy".to_string()))
        );
        assert_eq!(
            row.get("matchedPromotionProfile"),
            Some(&Value::String("reinforcement".to_string()))
        );
        assert_eq!(row.get("suppressed"), Some(&Value::Bool(false)));
        assert_eq!(row.get("dryRun"), Some(&Value::Bool(false)));
        assert_eq!(row.get("scoreFrom"), Some(&Value::String("LASTACCESSED".to_string())));
        assert_eq!(row.get("accessCount"), Some(&Value::from(3u64)));
    }

    #[test]
    fn test_call_knowledgepolicy_resolve_by_labels_is_dry_run() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE slow_decay OPTIONS { halfLifeSeconds: 3600, visibilityThreshold: 0.1, scoreFloor: 0.05, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE memory_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE slow_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let query = parser
            .parse("CALL nornicdb.knowledgepolicy.resolve('', 'MemoryEpisode', '')")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        assert_eq!(row.get("dryRun"), Some(&Value::Bool(true)));
        assert_eq!(row.get("decayBinding"), Some(&Value::String("memory_binding".to_string())));
        assert_eq!(row.get("targetKind"), Some(&Value::String("NODE".to_string())));
        assert_eq!(row.get("suppressed"), Some(&Value::Bool(false)));
        assert_eq!(row.get("anchorUnixMs"), Some(&Value::Null));
    }

    #[test]
    fn test_match_updates_node_access_metadata_via_on_access_policy() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE visible_decay OPTIONS { halfLifeSeconds: 3600, visibilityThreshold: 0.5, scoreFloor: 0.5, function: 'step', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE memory_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE visible_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION POLICY memory_access FOR (n:MemoryEpisode) APPLY { ON ACCESS { SET n.lastAccessedAt = timestamp() SET n.accessCount = coalesce(n.accessCount, 0) + 1 } }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "memory:on-access".to_string(),
                labels: vec!["MemoryEpisode".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Tracked memory".to_string()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .unwrap();

        let query = parser.parse("MATCH (n:MemoryEpisode) RETURN n").unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);

        let metadata = engine
            .storage
            .get_knowledge_policy_access_metadata("memory:on-access")
            .unwrap()
            .unwrap();
        assert_eq!(metadata.access_count, 1);
        assert!(metadata.last_accessed_at_unix_ms.is_some());
    }

    #[test]
    fn test_match_updates_node_access_metadata_with_policy_only_target() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION POLICY memory_access FOR (n:MemoryEpisode) APPLY { ON ACCESS { SET n.lastAccessedAt = timestamp() SET n.accessCount = coalesce(n.accessCount, 0) + 1 } }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "memory:policy-only".to_string(),
                labels: vec!["MemoryEpisode".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Tracked memory".to_string()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .unwrap();

        let query = parser.parse("MATCH (n:MemoryEpisode) RETURN n").unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);

        let metadata = engine
            .storage
            .get_knowledge_policy_access_metadata("memory:policy-only")
            .unwrap()
            .unwrap();
        assert_eq!(metadata.access_count, 1);
        assert!(metadata.last_accessed_at_unix_ms.is_some());
    }

    #[test]
    fn test_match_updates_edge_access_metadata_via_on_access_policy() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE visible_decay OPTIONS { halfLifeSeconds: 3600, visibilityThreshold: 0.5, scoreFloor: 0.5, function: 'step', scope: 'EDGE', scoreFrom: 'CREATED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE edge_binding FOR ()-[r:LINKS]-() APPLY { DECAY PROFILE visible_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION POLICY edge_access FOR ()-[r:LINKS]-() APPLY { ON ACCESS { SET r.lastAccessedAt = timestamp() SET r.accessCount = coalesce(r.accessCount, 0) + 1 } }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        for node in [
            NodeRecord {
                id: "person:access-left".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
            NodeRecord {
                id: "person:access-right".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
        ] {
            engine.storage.put_node_record(&node).unwrap();
        }
        engine
            .storage
            .put_edge_record(&EdgeRecord {
                id: "links:on-access".to_string(),
                start_node: "person:access-left".to_string(),
                end_node: "person:access-right".to_string(),
                edge_type: "LINKS".to_string(),
                properties: BTreeMap::from([("kind".to_string(), Value::String("tracked".to_string()))]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .unwrap();

        let query = parser
            .parse("MATCH (:Person)-[r:LINKS]->(:Person) RETURN r")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);

        let metadata = engine
            .storage
            .get_knowledge_policy_access_metadata("links:on-access")
            .unwrap()
            .unwrap();
        assert_eq!(metadata.access_count, 1);
        assert!(metadata.last_accessed_at_unix_ms.is_some());
    }

    #[test]
    fn test_match_updates_edge_access_metadata_with_policy_only_target() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION POLICY edge_access FOR ()-[r:LINKS]-() APPLY { ON ACCESS { SET r.lastAccessedAt = timestamp() SET r.accessCount = coalesce(r.accessCount, 0) + 1 } }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        for node in [
            NodeRecord {
                id: "person:policy-edge-left".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
            NodeRecord {
                id: "person:policy-edge-right".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
        ] {
            engine.storage.put_node_record(&node).unwrap();
        }
        engine
            .storage
            .put_edge_record(&EdgeRecord {
                id: "links:policy-only".to_string(),
                start_node: "person:policy-edge-left".to_string(),
                end_node: "person:policy-edge-right".to_string(),
                edge_type: "LINKS".to_string(),
                properties: BTreeMap::from([("kind".to_string(), Value::String("tracked".to_string()))]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .unwrap();

        let query = parser
            .parse("MATCH (:Person)-[r:LINKS]->(:Person) RETURN r")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);

        let metadata = engine
            .storage
            .get_knowledge_policy_access_metadata("links:policy-only")
            .unwrap()
            .unwrap();
        assert_eq!(metadata.access_count, 1);
        assert!(metadata.last_accessed_at_unix_ms.is_some());
    }

    #[test]
    fn test_match_keeps_stale_nodes_visible_when_promotion_predicate_matches() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE stale_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.75, scoreFloor: 0.0, function: 'step', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE stale_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE stale_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let stale_time = now_unix_ms() - 5_000;
        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "memory:promotion-visible".to_string(),
                labels: vec!["MemoryEpisode".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Reinforced memory".to_string()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: stale_time,
                updated_at_unix_ms: stale_time,
            })
            .unwrap();

        let query = parser.parse("MATCH (n:MemoryEpisode) RETURN n").unwrap();
        let hidden = engine.execute(&query, &HashMap::new()).unwrap();
        assert!(hidden.rows.is_empty());

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION PROFILE reinforcement OPTIONS { scope: 'NODE', multiplier: 2.0, scoreFloor: 0.8, scoreCap: 1.0, enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION POLICY reinforcement_policy FOR (n:MemoryEpisode) APPLY PROFILE reinforcement WHEN 'n.accessCount >= 3'",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .storage
            .put_knowledge_policy_access_metadata(
                "memory:promotion-visible",
                &copperdb_storage::KnowledgePolicyAccessMetadata {
                    last_accessed_at_unix_ms: None,
                    access_count: 3,
                },
            )
            .unwrap();

        let visible = engine.execute(&query, &HashMap::new()).unwrap();
        assert_eq!(visible.rows.len(), 1);
    }

    #[test]
    fn test_match_keeps_stale_edges_visible_when_promotion_predicate_matches() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE stale_edge_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.75, scoreFloor: 0.0, function: 'step', scope: 'EDGE', scoreFrom: 'CREATED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE stale_edge_binding FOR ()-[r:LINKS]-() APPLY { DECAY PROFILE stale_edge_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        for node in [
            NodeRecord {
                id: "person:promo-edge-left".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
            NodeRecord {
                id: "person:promo-edge-right".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
        ] {
            engine.storage.put_node_record(&node).unwrap();
        }

        let stale_time = now - 5_000;
        engine
            .storage
            .put_edge_record(&EdgeRecord {
                id: "links:promotion-visible".to_string(),
                start_node: "person:promo-edge-left".to_string(),
                end_node: "person:promo-edge-right".to_string(),
                edge_type: "LINKS".to_string(),
                properties: BTreeMap::from([("kind".to_string(), Value::String("reinforced".to_string()))]),
                created_at_unix_ms: stale_time,
                updated_at_unix_ms: stale_time,
            })
            .unwrap();

        let query = parser
            .parse("MATCH (:Person)-[r:LINKS]->(:Person) RETURN r")
            .unwrap();
        let hidden = engine.execute(&query, &HashMap::new()).unwrap();
        assert!(hidden.rows.is_empty());

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION PROFILE reinforcement_edge OPTIONS { scope: 'EDGE', multiplier: 2.0, scoreFloor: 0.8, scoreCap: 1.0, enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION POLICY reinforcement_edge_policy FOR ()-[r:LINKS]-() APPLY PROFILE reinforcement_edge WHEN 'r.accessCount >= 2'",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .storage
            .put_knowledge_policy_access_metadata(
                "links:promotion-visible",
                &copperdb_storage::KnowledgePolicyAccessMetadata {
                    last_accessed_at_unix_ms: None,
                    access_count: 2,
                },
            )
            .unwrap();

        let visible = engine.execute(&query, &HashMap::new()).unwrap();
        assert_eq!(visible.rows.len(), 1);
    }

    #[test]
    fn test_match_does_not_flush_access_metadata_on_query_error() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION POLICY memory_access FOR (n:MemoryEpisode) APPLY { ON ACCESS { SET n.lastAccessedAt = timestamp() SET n.accessCount = coalesce(n.accessCount, 0) + 1 } }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "memory:error-buffer".to_string(),
                labels: vec!["MemoryEpisode".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Tracked memory".to_string()))]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .unwrap();

        let query = parser
            .parse("MATCH (n:MemoryEpisode) RETURN abs('x') AS bad")
            .unwrap();
        let err = match engine.execute(&query, &HashMap::new()) {
            Ok(_) => panic!("query should fail"),
            Err(err) => err,
        };
        assert!(matches!(err, EvalError::FilterError(_)));
        assert!(engine
            .storage
            .get_knowledge_policy_access_metadata("memory:error-buffer")
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_set_relationship_property_persists_edge_binding() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 1, name: 'Alice'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 2, name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:FOLLOWS]->(p2)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let query = parser
            .parse("MATCH (:Person {id: 1})-[r:FOLLOWS]->(:Person {id: 2}) SET r.weight = 5 RETURN r.weight AS weight")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.stats.properties_set, 1);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("weight"), Some(&Value::Number(5.into())));

        let stored = engine.storage.get_edges_by_type("FOLLOWS").unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].properties.get("weight"), Some(&serde_json::json!(5)));
    }

    #[test]
    fn test_remove_relationship_property_persists_edge_binding() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 1, name: 'Alice'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 2, name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:FOLLOWS {weight: 5}]->(p2)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let query = parser
            .parse("MATCH (:Person {id: 1})-[r:FOLLOWS]->(:Person {id: 2}) REMOVE r.weight RETURN r.weight AS weight")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("weight"), Some(&Value::Null));

        let stored = engine.storage.get_edges_by_type("FOLLOWS").unwrap();
        assert_eq!(stored.len(), 1);
        assert!(!stored[0].properties.contains_key("weight"));
    }

    #[test]
    fn test_relationship_range_index_supports_simple_where_comparisons() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 1, name: 'Alice'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 2, name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 3, name: 'Carol'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:FOLLOWS {weight: 3}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 3}) CREATE (a)-[:FOLLOWS {weight: 7}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .execute(
                &parser
                    .parse("CREATE RANGE INDEX follows_weight_idx FOR ()-[r:FOLLOWS]-() ON (r.weight)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (:Person {id: 1})-[r:FOLLOWS]->(p:Person) WHERE r.weight >= 5 RETURN p.name AS name ORDER BY p.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["name"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("name"), Some(&Value::String("Carol".to_string())));
    }

    #[test]
    fn test_relationship_temporal_index_supports_simple_where_comparisons() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 1, name: 'Alice'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 2, name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 3, name: 'Carol'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:FOLLOWS {seenAt: '2024-01-01T00:00:00Z'}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 3}) CREATE (a)-[:FOLLOWS {seenAt: '2024-06-01T00:00:00Z'}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .execute(
                &parser
                    .parse("CREATE TEMPORAL INDEX follows_seen_at_idx FOR ()-[r:FOLLOWS]-() ON (r.seenAt)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (:Person {id: 1})-[r:FOLLOWS]->(p:Person) WHERE r.seenAt > '2024-03-01T00:00:00Z' RETURN p.name AS name ORDER BY p.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["name"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("name"), Some(&Value::String("Carol".to_string())));
    }

    #[test]
    fn test_composite_relationship_temporal_index_supports_simple_where_comparisons_with_exact_suffix_properties() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(&parser.parse("CREATE (:Person {id: 1, name: 'Alice'})").unwrap(), &HashMap::new())
            .unwrap();
        engine
            .execute(&parser.parse("CREATE (:Person {id: 2, name: 'Bob'})").unwrap(), &HashMap::new())
            .unwrap();
        engine
            .execute(&parser.parse("CREATE (:Person {id: 3, name: 'Carol'})").unwrap(), &HashMap::new())
            .unwrap();
        engine
            .execute(&parser.parse("CREATE (:Person {id: 4, name: 'Drew'})").unwrap(), &HashMap::new())
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:FOLLOWS {seenAt: '2024-01-01T00:00:00Z', years: 5}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 3}) CREATE (a)-[:FOLLOWS {seenAt: '2024-06-01T00:00:00Z', years: 5}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 4}) CREATE (a)-[:FOLLOWS {seenAt: '2025-01-01T00:00:00Z', years: 2}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 2}), (b:Person {id: 4}) CREATE (a)-[:FOLLOWS {seenAt: '2025-03-01T00:00:00Z', years: 5}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .execute(
                &parser
                    .parse("CREATE TEMPORAL INDEX follows_seen_at_years_idx FOR ()-[r:FOLLOWS]-() ON (r.seenAt, r.years)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (:Person)-[r:FOLLOWS {years: 5}]->(p:Person) WHERE r.seenAt > '2024-03-01T00:00:00Z' RETURN p.name AS name ORDER BY p.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["name"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("name"), Some(&Value::String("Carol".to_string())));
        assert_eq!(result.rows[1].get("name"), Some(&Value::String("Drew".to_string())));
    }

    #[test]
    fn test_non_leading_composite_relationship_temporal_index_supports_simple_where_comparisons_with_exact_prefix_properties() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(&parser.parse("CREATE (:Person {id: 1, name: 'Alice'})").unwrap(), &HashMap::new())
            .unwrap();
        engine
            .execute(&parser.parse("CREATE (:Person {id: 2, name: 'Bob'})").unwrap(), &HashMap::new())
            .unwrap();
        engine
            .execute(&parser.parse("CREATE (:Person {id: 3, name: 'Carol'})").unwrap(), &HashMap::new())
            .unwrap();
        engine
            .execute(&parser.parse("CREATE (:Person {id: 4, name: 'Drew'})").unwrap(), &HashMap::new())
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:FOLLOWS {years: 5, seenAt: '2024-01-01T00:00:00Z'}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 3}) CREATE (a)-[:FOLLOWS {years: 5, seenAt: '2024-06-01T00:00:00Z'}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 4}) CREATE (a)-[:FOLLOWS {years: 2, seenAt: '2025-01-01T00:00:00Z'}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 2}), (b:Person {id: 4}) CREATE (a)-[:FOLLOWS {years: 5, seenAt: '2025-03-01T00:00:00Z'}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .execute(
                &parser
                    .parse("CREATE TEMPORAL INDEX follows_years_seen_at_idx FOR ()-[r:FOLLOWS]-() ON (r.years, r.seenAt)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (:Person)-[r:FOLLOWS {years: 5}]->(p:Person) WHERE r.seenAt > '2024-03-01T00:00:00Z' RETURN p.name AS name ORDER BY p.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["name"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("name"), Some(&Value::String("Carol".to_string())));
        assert_eq!(result.rows[1].get("name"), Some(&Value::String("Drew".to_string())));
    }

    #[test]
    fn test_composite_relationship_range_index_supports_simple_where_comparisons_with_exact_suffix_properties() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 1, name: 'Alice'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 2, name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 3, name: 'Carol'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 4, name: 'Drew'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:FOLLOWS {weight: 1, years: 5}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 3}) CREATE (a)-[:FOLLOWS {weight: 7, years: 5}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 4}) CREATE (a)-[:FOLLOWS {weight: 9, years: 2}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .execute(
                &parser
                    .parse("CREATE RANGE INDEX follows_weight_years_idx FOR ()-[r:FOLLOWS]-() ON (r.weight, r.years)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (:Person {id: 1})-[r:FOLLOWS {years: 5}]->(p:Person) WHERE r.weight > 5 RETURN p.name AS name ORDER BY p.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["name"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("name"), Some(&Value::String("Carol".to_string())));
    }

    #[test]
    fn test_composite_relationship_range_index_supports_all_simple_comparison_operators_with_exact_suffix_properties() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 1, name: 'Alice'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 2, name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 3, name: 'Carol'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 4, name: 'Drew'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:FOLLOWS {weight: 1, years: 5}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 3}) CREATE (a)-[:FOLLOWS {weight: 7, years: 5}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 4}) CREATE (a)-[:FOLLOWS {weight: 9, years: 2}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .execute(
                &parser
                    .parse("CREATE RANGE INDEX follows_weight_years_idx FOR ()-[r:FOLLOWS]-() ON (r.weight, r.years)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let greater_than_or_equal = engine
            .execute(
                &parser
                    .parse("MATCH (:Person {id: 1})-[r:FOLLOWS {years: 5}]->(p:Person) WHERE r.weight >= 7 RETURN p.name AS name ORDER BY p.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(greater_than_or_equal.columns, vec!["name"]);
        assert_eq!(greater_than_or_equal.rows.len(), 1);
        assert_eq!(
            greater_than_or_equal.rows[0].get("name"),
            Some(&Value::String("Carol".to_string()))
        );

        let less_than = engine
            .execute(
                &parser
                    .parse("MATCH (:Person {id: 1})-[r:FOLLOWS {years: 5}]->(p:Person) WHERE r.weight < 7 RETURN p.name AS name ORDER BY p.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(less_than.columns, vec!["name"]);
        assert_eq!(less_than.rows.len(), 1);
        assert_eq!(less_than.rows[0].get("name"), Some(&Value::String("Bob".to_string())));

        let less_than_or_equal = engine
            .execute(
                &parser
                    .parse("MATCH (:Person {id: 1})-[r:FOLLOWS {years: 5}]->(p:Person) WHERE r.weight <= 7 RETURN p.name AS name ORDER BY p.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(less_than_or_equal.columns, vec!["name"]);
        assert_eq!(less_than_or_equal.rows.len(), 2);
        assert_eq!(
            less_than_or_equal.rows[0].get("name"),
            Some(&Value::String("Bob".to_string()))
        );
        assert_eq!(
            less_than_or_equal.rows[1].get("name"),
            Some(&Value::String("Carol".to_string()))
        );
    }

    #[test]
    fn test_composite_relationship_range_index_supports_reversed_operand_comparisons_with_exact_suffix_properties() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 1, name: 'Alice'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 2, name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 3, name: 'Carol'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 4, name: 'Drew'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:FOLLOWS {weight: 1, years: 5}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 3}) CREATE (a)-[:FOLLOWS {weight: 7, years: 5}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 4}) CREATE (a)-[:FOLLOWS {weight: 9, years: 2}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .execute(
                &parser
                    .parse("CREATE RANGE INDEX follows_weight_years_idx FOR ()-[r:FOLLOWS]-() ON (r.weight, r.years)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let less_than_reversed = engine
            .execute(
                &parser
                    .parse("MATCH (:Person {id: 1})-[r:FOLLOWS {years: 5}]->(p:Person) WHERE 7 >= r.weight RETURN p.name AS name ORDER BY p.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(less_than_reversed.columns, vec!["name"]);
        assert_eq!(less_than_reversed.rows.len(), 2);
        assert_eq!(
            less_than_reversed.rows[0].get("name"),
            Some(&Value::String("Bob".to_string()))
        );
        assert_eq!(
            less_than_reversed.rows[1].get("name"),
            Some(&Value::String("Carol".to_string()))
        );

        let greater_than_reversed = engine
            .execute(
                &parser
                    .parse("MATCH (:Person {id: 1})-[r:FOLLOWS {years: 5}]->(p:Person) WHERE 7 <= r.weight RETURN p.name AS name ORDER BY p.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(greater_than_reversed.columns, vec!["name"]);
        assert_eq!(greater_than_reversed.rows.len(), 1);
        assert_eq!(
            greater_than_reversed.rows[0].get("name"),
            Some(&Value::String("Carol".to_string()))
        );
    }

    #[test]
    fn test_composite_relationship_range_index_supports_simple_where_comparisons_without_exact_suffix_properties() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 1, name: 'Alice'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 2, name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 3, name: 'Carol'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:FOLLOWS {weight: 1, years: 5}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 3}) CREATE (a)-[:FOLLOWS {weight: 7, years: 5}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .execute(
                &parser
                    .parse("CREATE RANGE INDEX follows_weight_years_idx FOR ()-[r:FOLLOWS]-() ON (r.weight, r.years)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (:Person {id: 1})-[r:FOLLOWS]->(p:Person) WHERE r.weight > 5 RETURN p.name AS name ORDER BY p.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["name"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("name"), Some(&Value::String("Carol".to_string())));
    }

    #[test]
    fn test_non_leading_composite_relationship_range_index_supports_simple_where_comparisons_with_exact_prefix_properties() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 1, name: 'Alice'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 2, name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 3, name: 'Carol'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 4, name: 'Drew'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:FOLLOWS {years: 5, weight: 1.5}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 3}) CREATE (a)-[:FOLLOWS {years: 5, weight: 2.5}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 4}) CREATE (a)-[:FOLLOWS {years: 2, weight: 7.0}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .execute(
                &parser
                    .parse("CREATE RANGE INDEX follows_years_weight_idx FOR ()-[r:FOLLOWS]-() ON (r.years, r.weight)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (:Person {id: 1})-[r:FOLLOWS {years: 5}]->(p:Person) WHERE r.weight > 2 RETURN p.name AS name ORDER BY p.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["name"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("name"), Some(&Value::String("Carol".to_string())));
    }

    #[test]
    fn test_non_leading_composite_relationship_range_index_supports_all_simple_comparison_operators_with_exact_prefix_properties() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 1, name: 'Alice'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 2, name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 3, name: 'Carol'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 4, name: 'Drew'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:FOLLOWS {years: 5, weight: 1.5}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 3}) CREATE (a)-[:FOLLOWS {years: 5, weight: 2.5}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 4}) CREATE (a)-[:FOLLOWS {years: 2, weight: 7.0}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 2}), (b:Person {id: 4}) CREATE (a)-[:FOLLOWS {years: 5, weight: 6.0}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .execute(
                &parser
                    .parse("CREATE RANGE INDEX follows_years_weight_idx FOR ()-[r:FOLLOWS]-() ON (r.years, r.weight)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let greater_than_or_equal = engine
            .execute(
                &parser
                    .parse("MATCH (:Person)-[r:FOLLOWS {years: 5}]->(p:Person) WHERE r.weight >= 2.5 RETURN p.name AS name ORDER BY p.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(greater_than_or_equal.columns, vec!["name"]);
        assert_eq!(greater_than_or_equal.rows.len(), 2);
        assert_eq!(
            greater_than_or_equal.rows[0].get("name"),
            Some(&Value::String("Carol".to_string()))
        );
        assert_eq!(
            greater_than_or_equal.rows[1].get("name"),
            Some(&Value::String("Drew".to_string()))
        );

        let less_than = engine
            .execute(
                &parser
                    .parse("MATCH (:Person)-[r:FOLLOWS {years: 5}]->(p:Person) WHERE r.weight < 6 RETURN p.name AS name ORDER BY p.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(less_than.columns, vec!["name"]);
        assert_eq!(less_than.rows.len(), 2);
        assert_eq!(less_than.rows[0].get("name"), Some(&Value::String("Bob".to_string())));
        assert_eq!(less_than.rows[1].get("name"), Some(&Value::String("Carol".to_string())));

        let less_than_or_equal = engine
            .execute(
                &parser
                    .parse("MATCH (:Person)-[r:FOLLOWS {years: 5}]->(p:Person) WHERE r.weight <= 2.5 RETURN p.name AS name ORDER BY p.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(less_than_or_equal.columns, vec!["name"]);
        assert_eq!(less_than_or_equal.rows.len(), 2);
        assert_eq!(
            less_than_or_equal.rows[0].get("name"),
            Some(&Value::String("Bob".to_string()))
        );
        assert_eq!(
            less_than_or_equal.rows[1].get("name"),
            Some(&Value::String("Carol".to_string()))
        );
    }

    #[test]
    fn test_non_leading_composite_relationship_range_index_supports_reversed_operand_comparisons_with_exact_prefix_properties() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 1, name: 'Alice'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 2, name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 3, name: 'Carol'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 4, name: 'Drew'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:FOLLOWS {years: 5, weight: 1.5}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 3}) CREATE (a)-[:FOLLOWS {years: 5, weight: 2.5}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 4}) CREATE (a)-[:FOLLOWS {years: 2, weight: 7.0}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 2}), (b:Person {id: 4}) CREATE (a)-[:FOLLOWS {years: 5, weight: 6.0}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .execute(
                &parser
                    .parse("CREATE RANGE INDEX follows_years_weight_idx FOR ()-[r:FOLLOWS]-() ON (r.years, r.weight)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let less_than_reversed = engine
            .execute(
                &parser
                    .parse("MATCH (:Person)-[r:FOLLOWS {years: 5}]->(p:Person) WHERE 2.5 >= r.weight RETURN p.name AS name ORDER BY p.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(less_than_reversed.columns, vec!["name"]);
        assert_eq!(less_than_reversed.rows.len(), 2);
        assert_eq!(
            less_than_reversed.rows[0].get("name"),
            Some(&Value::String("Bob".to_string()))
        );
        assert_eq!(
            less_than_reversed.rows[1].get("name"),
            Some(&Value::String("Carol".to_string()))
        );

        let greater_than_reversed = engine
            .execute(
                &parser
                    .parse("MATCH (:Person)-[r:FOLLOWS {years: 5}]->(p:Person) WHERE 2.5 < r.weight RETURN p.name AS name ORDER BY p.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(greater_than_reversed.columns, vec!["name"]);
        assert_eq!(greater_than_reversed.rows.len(), 1);
        assert_eq!(
            greater_than_reversed.rows[0].get("name"),
            Some(&Value::String("Drew".to_string()))
        );
    }

    #[test]
    fn test_remove_node_label_persists_label_change() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (n:Person:VIP {id: 1, name: 'Alice'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let query = parser
            .parse("MATCH (n:Person {id: 1}) REMOVE n:VIP RETURN n")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.rows.len(), 1);
        let Value::Object(props) = result.rows[0].get("n").cloned().unwrap() else {
            panic!("expected node binding");
        };
        assert_eq!(
            props.get("_labels"),
            Some(&Value::Array(vec![Value::String("Person".into())]))
        );

        let node_id = props.get("_id").and_then(Value::as_str).unwrap();
        let stored = engine
            .storage
            .get_node_record(node_id)
            .unwrap()
            .expect("node should persist after label removal");
        assert!(!stored.labels.iter().any(|label| label == "VIP"));
    }

    #[test]
    fn test_execute_with_routes_pipeline_delete_relationship_binding() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (p:Person {id: 1, name: 'Alice'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let cypher = "MATCH (p:Person {id: 1}) CREATE (q:Person {id: 2, name: 'Bob'}) CREATE (p)-[r:FOLLOWS]->(q) WITH r DELETE r RETURN r._type AS relType";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);
        let (clauses, ok) = can_execute_as_pipeline(cypher);

        let result = engine
            .execute_with_routes(
                &query,
                &HashMap::new(),
                &pattern,
                None,
                ok.then_some(clauses.as_slice()),
            )
            .unwrap();

        assert!(ok);
        assert_eq!(result.stats.nodes_created, 1);
        assert_eq!(result.stats.relationships_created, 1);
        assert_eq!(result.stats.relationships_deleted, 1);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("relType"), Some(&Value::String("FOLLOWS".into())));
        assert!(engine.storage.get_edges_by_type("FOLLOWS").unwrap().is_empty());
    }

    #[test]
    fn test_execute_with_routes_pipeline_set_relationship_binding() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (p:Person {id: 1, name: 'Alice'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 2, name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:FOLLOWS]->(p2)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let cypher = "MATCH (:Person {id: 1})-[r:FOLLOWS]->(:Person {id: 2}) WITH r SET r.weight = 5 RETURN r.weight AS weight";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);
        let (clauses, ok) = can_execute_as_pipeline(cypher);

        let result = engine
            .execute_with_routes(
                &query,
                &HashMap::new(),
                &pattern,
                None,
                ok.then_some(clauses.as_slice()),
            )
            .unwrap();

        assert!(ok);
        assert_eq!(result.stats.properties_set, 1);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("weight"), Some(&Value::Number(5.into())));

        let stored = engine.storage.get_edges_by_type("FOLLOWS").unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].properties.get("weight"), Some(&serde_json::json!(5)));
    }

    #[test]
    fn test_execute_with_routes_pipeline_remove_relationship_binding() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 1, name: 'Alice'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {id: 2, name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:FOLLOWS {weight: 5}]->(p2)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let cypher = "MATCH (:Person {id: 1})-[r:FOLLOWS]->(:Person {id: 2}) WITH r REMOVE r.weight RETURN r.weight AS weight";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);
        let (clauses, ok) = can_execute_as_pipeline(cypher);

        let result = engine
            .execute_with_routes(
                &query,
                &HashMap::new(),
                &pattern,
                None,
                ok.then_some(clauses.as_slice()),
            )
            .unwrap();

        assert!(ok);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("weight"), Some(&Value::Null));

        let stored = engine.storage.get_edges_by_type("FOLLOWS").unwrap();
        assert_eq!(stored.len(), 1);
        assert!(!stored[0].properties.contains_key("weight"));
    }
