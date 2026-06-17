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

    #[test]
    fn test_with_where_order_skip_limit_projects_in_pipeline_order() {
        let engine = make_engine();
        let parser = Parser::new();
        let query = parser
            .parse(
                "UNWIND [3, 1, 2, 0] AS value WITH value WHERE value > 0 ORDER BY value DESC SKIP 1 LIMIT 1 RETURN value",
            )
            .unwrap();

        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.columns, vec!["value"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("value"), Some(&Value::from(2)));
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

    // ─── Index/constraint DDL regression tests ──────────────
    include!("index_ddl.rs");

    // ─── Knowledge-policy regression tests ──────────────────
    include!("knowledge_policy.rs");

    // ─── CALL procedure regression tests ────────────────────
    include!("call_procedures.rs");

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
