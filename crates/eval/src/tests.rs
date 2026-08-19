    use super::*;
    use copperdb_cypher::{
        can_execute_as_pipeline, detect_query_pattern, match_compound_query_shape, Parser,
        QueryPattern,
    };
    use copperdb_storage::{
        EdgeRecord, IndexDefinition, IndexEntityType, IndexKind, NodeRecord, StorageEngine,
    };

    fn node_props(name: &str) -> HashMap<String, Value> {
        [("name".to_string(), Value::String(name.to_string()))]
            .into_iter()
            .collect()
    }

    fn store_node(
        storage: &StorageEngine,
        id: &str,
        labels: &[&str],
        mut properties: HashMap<String, Value>,
    ) {
        properties.remove("_id");
        properties.remove("_labels");
        storage
            .put_node_record(&NodeRecord {
                id: id.to_string(),
                labels: labels.iter().map(|label| (*label).to_string()).collect(),
                properties: properties.into_iter().collect(),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
    }

    fn review_edge(id: &str, start: &str, end: &str, rating: i64) -> EdgeRecord {
        EdgeRecord {
            id: id.to_string(),
            start_node: start.to_string(),
            end_node: end.to_string(),
            edge_type: "REVIEWED".to_string(),
            properties: [("rating".to_string(), Value::from(rating))]
                .into_iter()
                .collect(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        }
    }

    fn seed_review_graph(engine: &EvalEngine) {
        for (id, props) in [
            ("customer:1", node_props("Alice")),
            ("customer:2", node_props("Bob")),
            ("customer:3", node_props("Carol")),
            ("customer:4", node_props("Dave")),
            ("product:1", node_props("Widget")),
            ("product:2", node_props("Gadget")),
            ("product:3", node_props("Thing")),
        ] {
            let label = id.split(':').next().unwrap_or("Node");
            store_node(engine.storage.as_ref(), id, &[label], props);
        }

        for edge in [
            review_edge("review:1", "customer:1", "product:1", 5),
            review_edge("review:2", "customer:2", "product:1", 4),
            review_edge("review:3", "customer:3", "product:1", 4),
            review_edge("review:4", "customer:1", "product:2", 3),
            review_edge("review:5", "customer:4", "product:2", 3),
            review_edge("review:6", "customer:2", "product:3", 5),
        ] {
            engine.storage.put_edge_record(&edge).unwrap();
        }
    }

    fn seed_social_graph(engine: &EvalEngine) {
        for (id, props) in [
            ("person:1", node_props("Alice")),
            ("person:2", node_props("Bob")),
            ("person:3", node_props("Carol")),
            ("person:4", node_props("Dave")),
        ] {
            let label = id.split(':').next().unwrap_or("Node");
            store_node(engine.storage.as_ref(), id, &[label], props);
        }

        for edge in [
            EdgeRecord {
                id: "follows:1".into(),
                start_node: "person:1".into(),
                end_node: "person:2".into(),
                edge_type: "FOLLOWS".into(),
                properties: HashMap::new().into_iter().collect(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "follows:2".into(),
                start_node: "person:2".into(),
                end_node: "person:1".into(),
                edge_type: "FOLLOWS".into(),
                properties: HashMap::new().into_iter().collect(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "follows:3".into(),
                start_node: "person:3".into(),
                end_node: "person:1".into(),
                edge_type: "FOLLOWS".into(),
                properties: HashMap::new().into_iter().collect(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "follows:4".into(),
                start_node: "person:4".into(),
                end_node: "person:1".into(),
                edge_type: "FOLLOWS".into(),
                properties: HashMap::new().into_iter().collect(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "follows:5".into(),
                start_node: "person:1".into(),
                end_node: "person:3".into(),
                edge_type: "FOLLOWS".into(),
                properties: HashMap::new().into_iter().collect(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
        ] {
            engine.storage.put_edge_record(&edge).unwrap();
        }
    }

    #[allow(clippy::arc_with_non_send_sync)]
    fn make_engine() -> EvalEngine {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        EvalEngine::new(storage)
    }

    #[test]
    fn test_create_node() {
        let engine = make_engine();
        let parser = Parser::new();
        let query = parser
            .parse("CREATE (n:Person {name: 'Alice', age: 30})")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert_eq!(result.stats.nodes_created, 1);
        assert_eq!(result.stats.properties_set, 2);
    }

    #[test]
    fn test_match_returns_created_node() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (n:Person {name: 'Alice'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let q2 = parser.parse("MATCH (n:Person) RETURN n").unwrap();
        let result = engine.execute(&q2, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert!(result.columns.contains(&"n".to_string()));
    }

    #[test]
    fn test_match_single_node_path_variable_materializes_path_accessors() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (:Person {name: 'Alice'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let query = parser
            .parse(
                "MATCH p = (n:Person {name: 'Alice'}) RETURN p AS path, nodes(p) AS nodes, relationships(p) AS rels, length(p) AS hops",
            )
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("hops"), Some(&Value::from(0)));
        assert_eq!(result.rows[0].get("rels"), Some(&Value::Array(Vec::new())));
        let nodes = result.rows[0]
            .get("nodes")
            .and_then(Value::as_array)
            .expect("expected path nodes");
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].get("name"), Some(&Value::String("Alice".into())));
        let path = result.rows[0]
            .get("path")
            .and_then(Value::as_object)
            .expect("expected path object");
        assert_eq!(path.get("length"), Some(&Value::from(0)));
    }

    #[test]
    fn test_match_where_filter() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser
                    .parse("CREATE (n:Person {name: 'Alice', age: 30})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (n:Person {name: 'Bob', age: 25})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let q = parser
            .parse("MATCH (n:Person) WHERE n.name = 'Alice' RETURN n")
            .unwrap();
        let result = engine.execute(&q, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
        if let Some(Value::Object(props)) = result.rows[0].get("n") {
            assert_eq!(props.get("name"), Some(&Value::String("Alice".into())));
        } else {
            panic!("expected object");
        }
    }

    #[test]
    fn test_delete_node() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (n:Person {name: 'Alice'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let q = parser.parse("MATCH (n:Person) DELETE n").unwrap();
        let result = engine.execute(&q, &HashMap::new()).unwrap();
        assert_eq!(result.stats.nodes_deleted, 1);

        let q2 = parser.parse("MATCH (n:Person) RETURN n").unwrap();
        let after = engine.execute(&q2, &HashMap::new()).unwrap();
        assert_eq!(after.rows.len(), 0);
    }

    #[test]
    fn test_match_with_inline_properties() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser
                    .parse("CREATE (n:Car {make: 'Toyota', year: 2020})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (n:Car {make: 'Honda', year: 2019})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let q = parser
            .parse("MATCH (n:Car {make: 'Toyota'}) RETURN n")
            .unwrap();
        let result = engine.execute(&q, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_return_property() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (n:City {name: 'London'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let q = parser.parse("MATCH (n:City) RETURN n.name").unwrap();
        let result = engine.execute(&q, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("n.name"),
            Some(&Value::String("London".into()))
        );
    }

    #[test]
    fn test_merge_creates_if_absent() {
        let engine = make_engine();
        let parser = Parser::new();
        let q = parser.parse("MERGE (n:Animal {species: 'Cat'})").unwrap();
        engine.execute(&q, &HashMap::new()).unwrap();
        engine.execute(&q, &HashMap::new()).unwrap(); // second merge should not create

        let q2 = parser.parse("MATCH (n:Animal) RETURN n").unwrap();
        let result = engine.execute(&q2, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_merge_marks_scan_fallback_without_schema_lookup_index() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("MERGE (n:Animal {species: 'Cat'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let trace = engine.hot_path_trace_snapshot();
        assert!(trace.merge_scan_fallback_used);
        assert!(!trace.merge_schema_lookup_used);
    }

    #[test]
    fn test_merge_marks_schema_lookup_when_index_exists() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE INDEX animal_species_idx FOR (n:Animal) ON (n.species)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .execute(
                &parser.parse("MERGE (n:Animal {species: 'Cat'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let trace = engine.hot_path_trace_snapshot();
        assert!(trace.merge_schema_lookup_used);
        assert!(!trace.merge_scan_fallback_used);
    }

    #[test]
    fn test_return_limit() {
        let engine = make_engine();
        let parser = Parser::new();
        for i in 0..5 {
            engine
                .execute(
                    &parser
                        .parse(&format!("CREATE (n:Num {{val: {i}}})"))
                        .unwrap(),
                    &HashMap::new(),
                )
                .unwrap();
        }
        let q = parser.parse("MATCH (n:Num) RETURN n LIMIT 3").unwrap();
        let result = engine.execute(&q, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 3);
    }

    #[test]
    fn test_execute_create_uses_current_row_expression_properties() {
        let engine = make_engine();
        let parser = Parser::new();
        let query = parser
            .parse("UNWIND [1, 2] AS orderID CREATE (o:Order {orderID: orderID}) RETURN o.orderID AS orderID")
            .unwrap();

        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("orderID"), Some(&Value::from(1)));
        assert_eq!(result.rows[1].get("orderID"), Some(&Value::from(2)));
    }

    #[test]
    fn test_execute_merge_uses_current_row_expression_properties() {
        let engine = make_engine();
        let parser = Parser::new();
        let query = parser
            .parse("UNWIND [1, 2] AS customerID MERGE (c:Customer {customerID: customerID}) RETURN c.customerID AS customerID")
            .unwrap();

        let first = engine.execute(&query, &HashMap::new()).unwrap();
        let second = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(first.rows.len(), 2);
        assert_eq!(first.rows[0].get("customerID"), Some(&Value::from(1)));
        assert_eq!(first.rows[1].get("customerID"), Some(&Value::from(2)));
        assert_eq!(first.stats.nodes_created, 2);
        assert_eq!(second.stats.nodes_created, 0);

        let all_customers = engine
            .execute(
                &parser
                    .parse("MATCH (c:Customer) RETURN c.customerID AS customerID ORDER BY c.customerID")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(all_customers.rows.len(), 2);
        assert_eq!(
            all_customers.rows[0].get("customerID"),
            Some(&Value::from(1))
        );
        assert_eq!(
            all_customers.rows[1].get("customerID"),
            Some(&Value::from(2))
        );
    }

    #[test]
    fn test_execute_with_pattern_optimizes_edge_property_aggregation() {
        let engine = make_engine();
        let parser = Parser::new();
        seed_review_graph(&engine);

        let cypher = "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) RETURN p.name AS product, avg(r.rating) AS avgRating, count(r) AS reviewCount ORDER BY avgRating DESC LIMIT 2";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        assert_eq!(pattern.pattern, QueryPattern::EdgePropertyAgg);

        let result = engine
            .execute_with_pattern(&query, &HashMap::new(), &pattern)
            .unwrap();

        assert_eq!(result.columns, vec!["product", "avgRating", "reviewCount"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(
            result.rows[0].get("product"),
            Some(&Value::String("Thing".into()))
        );
        assert_eq!(result.rows[0].get("avgRating"), Some(&Value::from(5.0)));
        assert_eq!(result.rows[0].get("reviewCount"), Some(&Value::from(1)));
        assert_eq!(
            result.rows[1].get("product"),
            Some(&Value::String("Widget".into()))
        );
        assert_eq!(
            result.rows[1].get("avgRating"),
            Some(&Value::from(13.0 / 3.0))
        );
        assert_eq!(result.rows[1].get("reviewCount"), Some(&Value::from(3)));
    }

    #[test]
    fn test_execute_with_pattern_edge_property_aggregation_branch_coverage() {
        let engine = make_engine();
        let parser = Parser::new();

        for (id, label, props) in [
            (
                "product:1",
                "Product",
                HashMap::from([("name".to_string(), Value::String("P1".into()))]),
            ),
            (
                "product:2",
                "Product",
                HashMap::from([("name".to_string(), Value::String("P2".into()))]),
            ),
            (
                "customer:1",
                "Customer",
                HashMap::from([("name".to_string(), Value::String("C1".into()))]),
            ),
        ] {
            store_node(engine.storage.as_ref(), id, &[label], props);
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
            engine.storage.put_edge_record(&edge).unwrap();
        }

        let cypher = "MATCH (c)-[r:REVIEWED]->(p) RETURN p.name AS product, avg(r.rating) AS avgRating, count(r) AS reviewCount, min(r.rating) AS minRating, max(r.rating) AS maxRating, sum(r.rating) AS totalRating";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_pattern(&query, &HashMap::new(), &pattern)
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
    fn test_execute_with_routes_optimizes_mutual_relationships() {
        let engine = make_engine();
        let parser = Parser::new();
        seed_social_graph(&engine);

        let cypher = "MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:FOLLOWS]->(a) RETURN a.name AS a, b.name AS b";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_routes(&query, &HashMap::new(), &pattern, None, None)
            .unwrap();

        assert_eq!(pattern.pattern, QueryPattern::MutualRelationship);
        assert_eq!(result.rows.len(), 2);
        let pairs: HashSet<(String, String)> = result
            .rows
            .iter()
            .map(|row| {
                let mut pair = [
                    row.get("a").and_then(Value::as_str).unwrap().to_string(),
                    row.get("b").and_then(Value::as_str).unwrap().to_string(),
                ];
                pair.sort();
                (pair[0].clone(), pair[1].clone())
            })
            .collect();
        assert!(pairs.contains(&("Alice".into(), "Bob".into())));
        assert!(pairs.contains(&("Alice".into(), "Carol".into())));
    }

    #[test]
    fn test_execute_with_routes_optimizes_simple_match_limit() {
        let engine = make_engine();
        let parser = Parser::new();

        for (id, name) in [
            ("person:01", "Alice"),
            ("person:02", "Bob"),
            ("person:03", "Carol"),
        ] {
            store_node(
                engine.storage.as_ref(),
                id,
                &["Person"],
                HashMap::from([("name".to_string(), Value::String(name.into()))]),
            );
        }

        let cypher = "MATCH (p:Person) RETURN p LIMIT 2";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_routes(&query, &HashMap::new(), &pattern, None, None)
            .unwrap();

        assert_eq!(pattern.pattern, QueryPattern::SimpleMatchLimit);
        assert_eq!(result.columns, vec!["p"]);
        assert_eq!(result.rows.len(), 2);

        let returned_ids: Vec<&str> = result
            .rows
            .iter()
            .map(|row| {
                row.get("p")
                    .and_then(Value::as_object)
                    .and_then(|node| node.get("_id"))
                    .and_then(Value::as_str)
                    .unwrap()
            })
            .collect();
        assert_eq!(returned_ids, vec!["person:01", "person:02"]);

        let trace = engine.hot_path_trace_snapshot();
        assert!(trace.simple_match_limit_fast_path);
        assert!(!trace.unwind_simple_merge_batch);
    }

    #[test]
    fn test_execute_with_routes_mutual_relationship_on_empty_db_returns_no_rows() {
        let engine = make_engine();
        let parser = Parser::new();

        let cypher = "MATCH (a)-[:FOLLOWS]->(b)-[:FOLLOWS]->(a) RETURN a, b";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_routes(&query, &HashMap::new(), &pattern, None, None)
            .unwrap();

        assert_eq!(pattern.pattern, QueryPattern::MutualRelationship);
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_execute_with_routes_mutual_relationship_with_missing_rel_type_returns_no_rows() {
        let engine = make_engine();
        let parser = Parser::new();
        seed_social_graph(&engine);

        let cypher = "MATCH (a)-[:NONEXISTENT]->(b)-[:NONEXISTENT]->(a) RETURN a, b";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_routes(&query, &HashMap::new(), &pattern, None, None)
            .unwrap();

        assert_eq!(pattern.pattern, QueryPattern::MutualRelationship);
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_execute_with_routes_optimizes_incoming_count_aggregation() {
        let engine = make_engine();
        let parser = Parser::new();
        seed_social_graph(&engine);

        let cypher = "MATCH (p:Person)<-[:FOLLOWS]-(f:Person) RETURN p.name AS person, count(f) AS followers LIMIT 2";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_routes(&query, &HashMap::new(), &pattern, None, None)
            .unwrap();

        assert_eq!(pattern.pattern, QueryPattern::IncomingCountAgg);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(
            result.rows[0].get("person"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(result.rows[0].get("followers"), Some(&Value::from(3)));
    }

    #[test]
    fn test_execute_with_routes_optimizes_incoming_count_star_aggregation() {
        let engine = make_engine();
        let parser = Parser::new();
        seed_social_graph(&engine);

        let cypher = "MATCH (p:Person)<-[:FOLLOWS]-(f:Person) RETURN p.name AS person, count(*) AS followers LIMIT 2";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_routes(&query, &HashMap::new(), &pattern, None, None)
            .unwrap();

        assert_eq!(pattern.pattern, QueryPattern::IncomingCountAgg);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(
            result.rows[0].get("person"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(result.rows[0].get("followers"), Some(&Value::from(3)));
    }

    #[test]
    fn test_execute_with_routes_optimized_incoming_count_limit_zero_returns_empty() {
        let engine = make_engine();
        let parser = Parser::new();
        seed_social_graph(&engine);

        let cypher = "MATCH (p:Person)<-[:FOLLOWS]-(f:Person) RETURN p.name AS person, count(f) AS followers LIMIT 0";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_routes(&query, &HashMap::new(), &pattern, None, None)
            .unwrap();

        assert_eq!(pattern.pattern, QueryPattern::IncomingCountAgg);
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_execute_with_routes_optimizes_untyped_incoming_count_aggregation() {
        let engine = make_engine();
        let parser = Parser::new();
        seed_social_graph(&engine);

        let cypher = "MATCH (p:Person)<-[r]-(f:Person) RETURN p.name AS person, count(f) AS followers LIMIT 2";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_routes(&query, &HashMap::new(), &pattern, None, None)
            .unwrap();

        assert_eq!(pattern.pattern, QueryPattern::IncomingCountAgg);
        assert_eq!(pattern.rel_type, "");
        assert_eq!(result.rows.len(), 2);
        assert_eq!(
            result.rows[0].get("person"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(result.rows[0].get("followers"), Some(&Value::from(3)));
    }

    #[test]
    fn count_all_relationships_uses_the_unfiltered_storage_count() {
        let engine = make_engine();
        let parser = Parser::new();
        seed_social_graph(&engine);

        let result = engine
            .execute(
                &parser
                    .parse("MATCH ()-[r]->() RETURN count(r) AS count")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(
            result.rows,
            vec![HashMap::from([("count".into(), Value::from(5_u64))])]
        );
    }

    #[test]
    fn count_all_relationships_fast_path_rejects_typed_patterns() {
        let engine = make_engine();
        let parser = Parser::new();
        seed_social_graph(&engine);

        let result = engine
            .execute(
                &parser
                    .parse("MATCH ()-[r:FOLLOWS]->() RETURN count(r) AS count")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.rows[0].get("count"), Some(&Value::from(5)));
    }

    #[test]
    fn raw_count_all_relationships_uses_the_storage_executor_fast_path() {
        let engine = make_engine();
        seed_social_graph(&engine);

        let result = engine
            .execute_cypher(
                "MATCH ()-[r]->() RETURN count(r) AS count",
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows[0].get("count"), Some(&Value::from(5_u64)));
    }

    #[test]
    fn raw_optional_match_count_uses_the_typed_edge_fast_path() {
        let engine = make_engine();
        seed_social_graph(&engine);

        let result = engine
            .execute_cypher(
                "MATCH (p) OPTIONAL MATCH (p)-[:FOLLOWS]->(friend) RETURN count(friend) AS count",
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows[0].get("count"), Some(&Value::from(5_u64)));
    }

    #[test]
    fn raw_two_hop_match_count_uses_the_typed_adjacency_fast_path() {
        let engine = make_engine();
        seed_social_graph(&engine);

        let result = engine
            .execute_cypher(
                "MATCH (a)-[:FOLLOWS]->(b)-[:FOLLOWS]->(c) RETURN count(c) AS count",
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows[0].get("count"), Some(&Value::from(8_u64)));
    }

    #[test]
    fn raw_two_hop_match_count_excludes_dangling_endpoints() {
        let engine = make_engine();
        seed_social_graph(&engine);
        engine
            .storage
            .put_edge_record(&EdgeRecord {
                id: "follows:dangling".into(),
                start_node: "person:1".into(),
                end_node: "person:missing".into(),
                edge_type: "FOLLOWS".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let result = engine
            .execute_cypher(
                "MATCH (a)-[:FOLLOWS]->(b)-[:FOLLOWS]->(c) RETURN count(c) AS count",
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.rows[0].get("count"), Some(&Value::from(8_u64)));
    }

    #[test]
    fn prof_raw_two_hop_match_count_benchmark_breakdown() {
        const NODE_COUNT: usize = 1_000;
        const QUERY: &str =
            "MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN count(c) AS count";

        let engine = make_engine();
        let nodes = (0..NODE_COUNT)
            .map(|index| NodeRecord {
                id: format!("n{index}"),
                labels: Vec::new(),
                properties: BTreeMap::new(),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .collect::<Vec<_>>();
        engine.storage.put_node_records_batch(&nodes).unwrap();
        let edges = (0..NODE_COUNT - 1)
            .map(|index| EdgeRecord {
                id: format!("e{index}"),
                start_node: format!("n{index}"),
                end_node: format!("n{}", index + 1),
                edge_type: "KNOWS".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .collect::<Vec<_>>();
        engine.storage.put_edge_records_batch(&edges).unwrap();

        let parser = Parser::new();
        let parse_started = std::time::Instant::now();
        let _query = parser.parse(QUERY).unwrap();
        let parse_elapsed = parse_started.elapsed();

        let first_hop_started = std::time::Instant::now();
        let first_hop_edges = engine.storage.get_edges_by_type("KNOWS").unwrap();
        let first_hop_elapsed = first_hop_started.elapsed();

        let expansion_started = std::time::Instant::now();
        let mut count = 0_u64;
        for edge in &first_hop_edges {
            if engine.storage.get_node_record(&edge.start_node).unwrap().is_none()
                || engine.storage.get_node_record(&edge.end_node).unwrap().is_none()
            {
                continue;
            }
            for next_edge in engine
                .storage
                .get_edges_from_node_by_type(&edge.end_node, "KNOWS")
                .unwrap()
            {
                if engine
                    .storage
                    .get_node_record(&next_edge.end_node)
                    .unwrap()
                    .is_some()
                {
                    count += 1;
                }
            }
        }
        let expansion_elapsed = expansion_started.elapsed();

        let raw_started = std::time::Instant::now();
        let result = engine.execute_cypher(QUERY, &HashMap::new()).unwrap();
        let raw_elapsed = raw_started.elapsed();
        let cache_hit_started = std::time::Instant::now();
        let cached_result = engine.execute_cypher(QUERY, &HashMap::new()).unwrap();
        let cache_hit_elapsed = cache_hit_started.elapsed();

        assert_eq!(count, (NODE_COUNT - 2) as u64);
        assert_eq!(result.rows[0].get("count"), Some(&Value::from(count)));
        assert_eq!(cached_result.rows[0].get("count"), Some(&Value::from(count)));
        eprintln!(
            "raw_two_hop_match_count_1000: parse={parse_elapsed:.2?} first_hop_type_scan={first_hop_elapsed:.2?} per_edge_expansion={expansion_elapsed:.2?} result_cache_miss_graph_warm={raw_elapsed:.2?} result_cache_hit={cache_hit_elapsed:.2?}"
        );
    }

    #[test]
    fn two_hop_match_count_with_source_label_falls_back_to_general_evaluation() {
        let engine = make_engine();
        seed_social_graph(&engine);

        let result = engine
            .execute_cypher(
                "MATCH (a:person)-[:FOLLOWS]->(b)-[:FOLLOWS]->(c) RETURN count(c) AS count",
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.rows[0].get("count"), Some(&Value::from(8)));
    }

    #[test]
    fn optional_match_count_with_source_label_falls_back_to_general_evaluation() {
        let engine = make_engine();
        seed_social_graph(&engine);

        let result = engine
            .execute_cypher(
                "MATCH (p:person) OPTIONAL MATCH (p)-[:FOLLOWS]->(friend) RETURN count(friend) AS count",
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.rows[0].get("count"), Some(&Value::from(5)));
    }

    #[test]
    fn test_execute_with_routes_optimizes_outgoing_count_aggregation() {
        let engine = make_engine();
        let parser = Parser::new();
        seed_social_graph(&engine);

        let cypher = "MATCH (p:Person)-[:FOLLOWS]->(f:Person) RETURN p.name AS person, count(f) AS following LIMIT 2";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_routes(&query, &HashMap::new(), &pattern, None, None)
            .unwrap();

        assert_eq!(pattern.pattern, QueryPattern::OutgoingCountAgg);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(
            result.rows[0].get("person"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(result.rows[0].get("following"), Some(&Value::from(2)));
    }

    #[test]
    fn test_execute_with_routes_optimizes_untyped_edge_property_aggregation() {
        let engine = make_engine();
        let parser = Parser::new();
        seed_review_graph(&engine);

        let cypher = "MATCH (c)-[r]->(p) RETURN p.name AS product, avg(r.rating) AS avgRating, count(r) AS reviewCount ORDER BY avgRating DESC LIMIT 2";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_routes(&query, &HashMap::new(), &pattern, None, None)
            .unwrap();

        assert_eq!(pattern.pattern, QueryPattern::EdgePropertyAgg);
        assert_eq!(pattern.rel_type, "");
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
        assert_eq!(result.rows[1].get("reviewCount"), Some(&Value::from(3)));
    }

    #[test]
    fn test_execute_with_routes_edge_property_aggregation_on_empty_db_returns_no_rows() {
        let engine = make_engine();
        let parser = Parser::new();

        let cypher = "MATCH (c)-[r]->(p) RETURN p.name AS product, avg(r.rating) AS avgRating";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_routes(&query, &HashMap::new(), &pattern, None, None)
            .unwrap();

        assert_eq!(pattern.pattern, QueryPattern::EdgePropertyAgg);
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_execute_with_routes_uses_compound_shape_fast_path() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (p1:Person {id: 1})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (p2:Person {id: 2})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let cypher = "MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:TEMP_KNOWS]->(p2) WITH r DELETE r RETURN count(r)";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);
        let (shape_match, ok) = match_compound_query_shape(cypher);

        let result = engine
            .execute_with_routes(
                &query,
                &HashMap::new(),
                &pattern,
                ok.then_some(&shape_match),
                None,
            )
            .unwrap();

        assert!(ok);
        assert_eq!(result.columns, vec!["count(r)"]);
        assert_eq!(
            result.rows,
            vec![HashMap::from([("count(r)".into(), Value::from(1))])]
        );
        assert_eq!(result.stats.relationships_created, 1);
        assert_eq!(result.stats.relationships_deleted, 1);
        assert!(engine
            .storage
            .get_edges_by_type("TEMP_KNOWS")
            .unwrap()
            .is_empty());

        let trace = engine.hot_path_trace_snapshot();
        assert!(trace.compound_query_fast_path);
        assert!(!trace.simple_match_limit_fast_path);
    }

    #[test]
    fn test_execute_with_routes_compound_fast_path_limit_zero_is_noop() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (a:Actor {name: 'Alice'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (m:Movie {title: 'Matrix'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let cypher =
            "MATCH (a:Actor), (m:Movie) WITH a, m LIMIT 0 CREATE (a)-[r:TEMP_REL]->(m) DELETE r";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);
        let (shape_match, ok) = match_compound_query_shape(cypher);

        let result = engine
            .execute_with_routes(
                &query,
                &HashMap::new(),
                &pattern,
                ok.then_some(&shape_match),
                None,
            )
            .unwrap();

        assert!(ok);
        assert!(result.columns.is_empty());
        assert!(result.rows.is_empty());
        assert_eq!(result.stats.relationships_created, 0);
        assert_eq!(result.stats.relationships_deleted, 0);
        assert!(engine
            .storage
            .get_edges_by_type("TEMP_REL")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_execute_with_routes_pipelines_unwind_merge_return() {
        let engine = make_engine();
        let parser = Parser::new();

        let cypher = "UNWIND [1, 2] AS customerID MERGE (c:Customer {customerID: customerID}) RETURN c.customerID AS customerID";
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
        assert!(engine.can_execute_pipeline_route(&query, &clauses));
        assert_eq!(result.columns, vec!["customerID"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("customerID"), Some(&Value::from(1)));
        assert_eq!(result.rows[1].get("customerID"), Some(&Value::from(2)));
        assert_eq!(result.stats.nodes_created, 2);

        let stored = engine.storage.get_nodes_by_label("Customer").unwrap();
        assert_eq!(stored.len(), 2);

        let trace = engine.hot_path_trace_snapshot();
        assert!(trace.unwind_simple_merge_batch);
        assert!(trace.merge_scan_fallback_used);
        assert!(!trace.simple_match_limit_fast_path);
        assert!(!trace.merge_schema_lookup_used);
    }

    #[test]
    fn test_unwind_merge_set_batch_duplicate_keys_last_row_wins_and_upserts() {
        let engine = make_engine();
        let parser = Parser::new();

        let cypher = "UNWIND $rows AS row MERGE (n:Star {starId: row.starId}) SET n.name = row.name, n.mass = row.mass";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);
        let (clauses, ok) = can_execute_as_pipeline(cypher);

        let mut params = HashMap::new();
        params.insert(
            "rows".to_string(),
            Value::Array(vec![
                serde_json::json!({"starId": "s1", "name": "first", "mass": 1}),
                serde_json::json!({"starId": "s1", "name": "second", "mass": 2}),
                serde_json::json!({"starId": "s2", "name": "third", "mass": 3}),
            ]),
        );

        let result = engine
            .execute_with_routes(
                &query,
                &params,
                &pattern,
                None,
                ok.then_some(clauses.as_slice()),
            )
            .unwrap();
        assert!(ok);
        assert_eq!(result.stats.nodes_created, 2);

        params.insert(
            "rows".to_string(),
            Value::Array(vec![
                serde_json::json!({"starId": "s1", "name": "updated", "mass": 10}),
                serde_json::json!({"starId": "s2", "name": "still-two", "mass": 20}),
            ]),
        );

        let result = engine
            .execute_with_routes(
                &query,
                &params,
                &pattern,
                None,
                ok.then_some(clauses.as_slice()),
            )
            .unwrap();
        assert_eq!(result.stats.nodes_created, 0);

        let stored = engine.storage.get_nodes_by_label("Star").unwrap();
        assert_eq!(stored.len(), 2);

        let s1 = stored
            .iter()
            .find(|node| node.properties.get("starId") == Some(&Value::String("s1".into())))
            .unwrap();
        assert_eq!(s1.properties.get("name"), Some(&Value::String("updated".into())));
        assert_eq!(s1.properties.get("mass"), Some(&Value::from(10)));

        let s2 = stored
            .iter()
            .find(|node| node.properties.get("starId") == Some(&Value::String("s2".into())))
            .unwrap();
        assert_eq!(s2.properties.get("name"), Some(&Value::String("still-two".into())));
        assert_eq!(s2.properties.get("mass"), Some(&Value::from(20)));

        let trace = engine.hot_path_trace_snapshot();
        assert!(trace.unwind_simple_merge_batch);
    }

    #[test]
    fn test_pipeline_route_with_then_return_aggregation_collapses_rows() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:A {x: 1}), (:A {x: 2}), (:B {z: 10}), (:B {z: 20})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let cypher = "MATCH (a:A) WITH count(a) AS aCount MATCH (b:B) RETURN aCount, count(b) AS bCount";
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
        assert_eq!(result.rows[0].get("aCount"), Some(&Value::from(2)));
        assert_eq!(result.rows[0].get("bCount"), Some(&Value::from(2)));
    }

    #[test]
    fn test_unwind_match_merge_relationship_set_batch_idempotent_last_row_wins() {
        let engine = make_engine();
        let parser = Parser::new();
        for index in 1..=3 {
            store_node(
                engine.storage.as_ref(),
                &format!("star:{index}"),
                &["Star"],
                HashMap::from([("starId".into(), Value::String(format!("s{index}")))]),
            );
        }

        let cypher = "UNWIND $rows AS row MATCH (a:Star {starId: row.fromId}) MATCH (b:Star {starId: row.toId}) MERGE (a)-[r:HYPERLANE]->(b) SET r.distance = row.distance RETURN count(r) AS c";
        let query = parser.parse(cypher).unwrap();
        let mut params = HashMap::new();
        params.insert(
            "rows".to_string(),
            Value::Array(vec![
                serde_json::json!({"fromId": "s1", "toId": "s2", "distance": 10}),
                serde_json::json!({"fromId": "s1", "toId": "s2", "distance": 20}),
                serde_json::json!({"fromId": "s2", "toId": "s3", "distance": 30}),
            ]),
        );

        let result = engine.execute(&query, &params).unwrap();
        assert_eq!(result.rows[0].get("c"), Some(&Value::from(3)));
        assert_eq!(result.stats.relationships_created, 2);
        assert!(
            engine
                .hot_path_trace_snapshot()
                .unwind_multi_match_relationship_batch
        );

        let edges = engine.storage.get_edges_by_type("HYPERLANE").unwrap();
        assert_eq!(edges.len(), 2);
        let s1_s2 = engine
            .storage
            .find_edge_between("star:1", "HYPERLANE", "star:2")
            .unwrap()
            .unwrap();
        assert_eq!(s1_s2.properties.get("distance"), Some(&Value::from(20)));

        params.insert(
            "rows".to_string(),
            Value::Array(vec![
                serde_json::json!({"fromId": "s1", "toId": "s2", "distance": 40}),
                serde_json::json!({"fromId": "s2", "toId": "s3", "distance": 50}),
            ]),
        );
        let result = engine.execute(&query, &params).unwrap();
        assert_eq!(result.rows[0].get("c"), Some(&Value::from(2)));
        assert_eq!(result.stats.relationships_created, 0);
        assert_eq!(engine.storage.get_edges_by_type("HYPERLANE").unwrap().len(), 2);

        let s2_s3 = engine
            .storage
            .find_edge_between("star:2", "HYPERLANE", "star:3")
            .unwrap()
            .unwrap();
        assert_eq!(s2_s3.properties.get("distance"), Some(&Value::from(50)));
    }

    #[test]
    fn test_unwind_match_merge_relationship_set_batch_browser_sized_performance() {
        let engine = make_engine();
        let parser = Parser::new();
        for index in 0..=400 {
            store_node(
                engine.storage.as_ref(),
                &format!("star:{index}"),
                &["Star"],
                HashMap::from([("starId".into(), Value::String(format!("s{index}")))]),
            );
        }

        let rows: Vec<Value> = (0..400)
            .map(|index| {
                serde_json::json!({
                    "fromId": format!("s{index}"),
                    "toId": format!("s{}", index + 1),
                    "distance": index,
                })
            })
            .collect();
        let params = HashMap::from([("rows".to_string(), Value::Array(rows))]);
        let query = parser
            .parse("UNWIND $rows AS row MATCH (a:Star {starId: row.fromId}) MATCH (b:Star {starId: row.toId}) MERGE (a)-[r:HYPERLANE]->(b) SET r.distance = row.distance")
            .unwrap();

        let started = std::time::Instant::now();
        let result = engine.execute(&query, &params).unwrap();
        let elapsed = started.elapsed();

        assert_eq!(result.stats.relationships_created, 400);
        assert_eq!(engine.storage.get_edges_by_type("HYPERLANE").unwrap().len(), 400);
        assert!(
            engine
                .hot_path_trace_snapshot()
                .unwind_multi_match_relationship_batch
        );
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "400-row relationship seed batch should stay on the fast path, took {elapsed:?}"
        );
    }

    #[test]
    fn test_execute_with_routes_compound_fast_path_property_miss_falls_back_cleanly() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (p1:Person {id: 1})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let cypher = "MATCH (p1:Person {id: 1}), (p2:Person {id: 999}) CREATE (p1)-[r:TEMP_KNOWS]->(p2) DELETE r";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);
        let (shape_match, ok) = match_compound_query_shape(cypher);

        let result = engine
            .execute_with_routes(
                &query,
                &HashMap::new(),
                &pattern,
                ok.then_some(&shape_match),
                None,
            )
            .unwrap();

        assert!(ok);
        assert!(result.rows.is_empty());
        assert_eq!(result.stats.relationships_created, 0);
        assert_eq!(result.stats.relationships_deleted, 0);
        assert!(engine
            .storage
            .get_edges_by_type("TEMP_KNOWS")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_execute_with_routes_compound_property_match_fast_path() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser
                    .parse("CREATE (p1:Person {id: 1, name: 'Alice'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (p2:Person {id: 2, name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let cypher = "MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:TEMP_KNOWS]->(p2) DELETE r";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);
        let (shape_match, ok) = match_compound_query_shape(cypher);

        let result = engine
            .execute_with_routes(
                &query,
                &HashMap::new(),
                &pattern,
                ok.then_some(&shape_match),
                None,
            )
            .unwrap();

        assert!(ok);
        assert!(result.rows.is_empty());
        assert_eq!(result.stats.relationships_created, 1);
        assert_eq!(result.stats.relationships_deleted, 1);
        assert!(engine
            .storage
            .get_edges_by_type("TEMP_KNOWS")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_execute_with_routes_uses_pipeline_hook() {
        let engine = make_engine();
        let parser = Parser::new();
        let cypher = "WITH [1, 2] AS values UNWIND values AS value RETURN value";
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
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("value"), Some(&Value::from(1)));
        assert_eq!(result.rows[1].get("value"), Some(&Value::from(2)));
    }

    #[test]
    fn test_execute_with_routes_pipeline_create_reuses_bound_nodes() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (c:Customer {customerID: 1, name: 'Ada'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let cypher = "MATCH (c:Customer {customerID: 1}) CREATE (o:Order {orderID: 9001}) CREATE (c)-[:PURCHASED]->(o) WITH c, o RETURN c.customerID AS customerID, o.orderID AS orderID";
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
        assert_eq!(result.rows[0].get("customerID"), Some(&Value::from(1)));
        assert_eq!(result.rows[0].get("orderID"), Some(&Value::from(9001)));
        assert_eq!(result.stats.nodes_created, 1);
        assert_eq!(result.stats.relationships_created, 1);

        let edges = engine.storage.get_edges_by_type("PURCHASED").unwrap();
        assert_eq!(edges.len(), 1);

        let start_raw = engine
            .storage
            .get_node_record(&edges[0].start_node)
            .unwrap()
            .expect("customer node should exist");
        let start_props = node_record_to_props(&start_raw);
        assert_eq!(start_props.get("customerID"), Some(&Value::from(1)));

        let end_raw = engine
            .storage
            .get_node_record(&edges[0].end_node)
            .unwrap()
            .expect("order node should exist");
        let end_props = node_record_to_props(&end_raw);
        assert_eq!(end_props.get("orderID"), Some(&Value::from(9001)));
    }

    #[test]
    fn test_execute_with_routes_pipeline_match_respects_bound_relationship_endpoints() {
        let engine = make_engine();
        let parser = Parser::new();

        for cypher in [
            "CREATE (c:Customer {customerID: 1, name: 'Ada'})",
            "CREATE (c:Customer {customerID: 2, name: 'Bob'})",
            "CREATE (o:Order {orderID: 100})",
            "CREATE (o:Order {orderID: 200})",
        ] {
            engine
                .execute(&parser.parse(cypher).unwrap(), &HashMap::new())
                .unwrap();
        }

        let node_id_for = |label: &str, property: &str, expected: i64| {
            engine
                .storage
                .get_nodes_by_label(label)
                .expect("label lookup should succeed")
                .into_iter()
                .find_map(|node| {
                    let props = node_record_to_props(&node);
                    (props.get(property) == Some(&Value::from(expected))).then_some(node.id)
                })
                .expect("expected seeded node")
        };

        engine
            .storage
            .put_edge_record(&EdgeRecord {
                id: "purchased:1".into(),
                start_node: node_id_for("Customer", "customerID", 1),
                end_node: node_id_for("Order", "orderID", 100),
                edge_type: "PURCHASED".into(),
                properties: HashMap::new().into_iter().collect(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        engine
            .storage
            .put_edge_record(&EdgeRecord {
                id: "purchased:2".into(),
                start_node: node_id_for("Customer", "customerID", 2),
                end_node: node_id_for("Order", "orderID", 200),
                edge_type: "PURCHASED".into(),
                properties: HashMap::new().into_iter().collect(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let cypher = "MATCH (c:Customer {customerID: 1}) WITH c MATCH (c)-[:PURCHASED]->(o:Order) RETURN o.orderID AS orderID";
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
        assert_eq!(result.rows[0].get("orderID"), Some(&Value::from(100)));
    }

    #[test]
    fn test_execute_with_routes_pipeline_seeder_shape_supports_multiple_rows_and_edge_properties() {
        let engine = make_engine();
        let parser = Parser::new();

        for cypher in [
            "CREATE (c:Customer {customerID: 1, companyName: 'C1'})",
            "CREATE (p:Product {productID: 1, productName: 'P1'})",
            "CREATE (p:Product {productID: 2, productName: 'P2'})",
        ] {
            engine
                .execute(&parser.parse(cypher).unwrap(), &HashMap::new())
                .unwrap();
        }

        let cypher = "MATCH (c:Customer {customerID: 1}) CREATE (o:Order {orderID: 9001}) CREATE (c)-[:PURCHASED]->(o) WITH o, {} UNWIND [{productID: 1, quantity: 3}, {productID: 2, quantity: 5}] AS prodRef MATCH (p:Product {productID: prodRef.productID}) CREATE (o)-[:ORDERS {quantity: prodRef.quantity}]->(p)";
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
        assert_eq!(result.stats.relationships_created, 3);

        let purchased = engine.storage.get_edges_by_type("PURCHASED").unwrap();
        assert_eq!(purchased.len(), 1);
        let orders = engine.storage.get_edges_by_type("ORDERS").unwrap();
        assert_eq!(orders.len(), 2);

        let mut quantities: Vec<i64> = orders
            .iter()
            .filter_map(|edge| edge.properties.get("quantity").and_then(Value::as_i64))
            .collect();
        quantities.sort_unstable();
        assert_eq!(quantities, vec![3, 5]);

        let order_ids: HashSet<String> =
            orders.iter().map(|edge| edge.start_node.clone()).collect();
        assert_eq!(order_ids.len(), 1);
        assert!(order_ids.contains(&purchased[0].end_node));

        let product_ids: HashSet<i64> = orders
            .iter()
            .filter_map(|edge| {
                let node = engine.storage.get_node_record(&edge.end_node).ok().flatten()?;
                let props = node_record_to_props(&node);
                props.get("productID").and_then(Value::as_i64)
            })
            .collect();
        assert_eq!(product_ids, HashSet::from([1, 2]));

        let trace = engine.hot_path_trace_snapshot();
        assert!(trace.unwind_fixed_chain_link_batch);
        assert!(!trace.unwind_simple_merge_batch);
    }

    #[test]
    fn test_execute_pipeline_routed_direct_invocation_for_seeder_shape() {
        let engine = make_engine();
        let parser = Parser::new();

        for cypher in [
            "CREATE (c:Customer {customerID: 1, companyName: 'C1'})",
            "CREATE (p:Product {productID: 1, productName: 'P1'})",
            "CREATE (p:Product {productID: 2, productName: 'P2'})",
        ] {
            engine
                .execute(&parser.parse(cypher).unwrap(), &HashMap::new())
                .unwrap();
        }

        let cypher = "MATCH (c:Customer {customerID: 1}) CREATE (o:Order {orderID: 9001}) CREATE (c)-[:PURCHASED]->(o) WITH o, {} UNWIND [{productID: 1, quantity: 3}, {productID: 2, quantity: 5}] AS prodRef MATCH (p:Product {productID: prodRef.productID}) CREATE (o)-[:ORDERS {quantity: prodRef.quantity}]->(p)";
        let query = parser.parse(cypher).unwrap();
        let (clauses, ok) = can_execute_as_pipeline(cypher);

        assert!(ok);
        assert!(engine.can_execute_pipeline_route(&query, &clauses));

        let result = engine
            .execute_pipeline_routed(&query, &HashMap::new(), clauses.as_slice())
            .unwrap();

        assert_eq!(result.stats.nodes_created, 1);
        assert_eq!(result.stats.relationships_created, 3);

        let purchased = engine.storage.get_edges_by_type("PURCHASED").unwrap();
        assert_eq!(purchased.len(), 1);
        let orders = engine.storage.get_edges_by_type("ORDERS").unwrap();
        assert_eq!(orders.len(), 2);

        let mut quantities: Vec<i64> = orders
            .iter()
            .filter_map(|edge| edge.properties.get("quantity").and_then(Value::as_i64))
            .collect();
        quantities.sort_unstable();
        assert_eq!(quantities, vec![3, 5]);

        let order_ids: HashSet<String> =
            orders.iter().map(|edge| edge.start_node.clone()).collect();
        assert_eq!(order_ids.len(), 1);
        assert!(order_ids.contains(&purchased[0].end_node));

        let product_ids: HashSet<i64> = orders
            .iter()
            .filter_map(|edge| {
                let node = engine.storage.get_node_record(&edge.end_node).ok().flatten()?;
                let props = node_record_to_props(&node);
                props.get("productID").and_then(Value::as_i64)
            })
            .collect();
        assert_eq!(product_ids, HashSet::from([1, 2]));
    }

    #[test]
    fn test_optional_match_relationship_pattern_preserves_row_with_nulls() {
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

        let result = engine
            .execute(
                &parser
                    .parse(
                        "MATCH (p:Person {id: 1}) OPTIONAL MATCH (p)-[r:FOLLOWS]->(friend:Person) RETURN p.name AS person, friend AS friend, r AS rel",
                    )
                    .unwrap(),
                &HashMap::new(),
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
        let engine = make_engine();
        let parser = Parser::new();

        for cypher in [
            "CREATE (p:Person {id: 1, name: 'Alice'})",
            "CREATE (p:Person {id: 2, name: 'Bob'})",
            "MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:FOLLOWS]->(b)",
        ] {
            engine
                .execute(&parser.parse(cypher).unwrap(), &HashMap::new())
                .unwrap();
        }

        let result = engine
            .execute(
                &parser
                    .parse(
                        "MATCH (p:Person {id: 1}) OPTIONAL MATCH (p)-[r:FOLLOWS]->(friend:Person) RETURN p.name AS person, friend.name AS friendName, r._type AS relType",
                    )
                    .unwrap(),
                &HashMap::new(),
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
    fn test_optional_match_single_node_path_variable_hit_and_miss() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:Seed {id: 1}), (:Person {name: 'Alice'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let hit = parser
            .parse(
                "MATCH (s:Seed {id: 1}) OPTIONAL MATCH p = (n:Person {name: 'Alice'}) RETURN p AS path, nodes(p) AS nodes, relationships(p) AS rels, length(p) AS hops",
            )
            .unwrap();
        let miss = parser
            .parse(
                "MATCH (s:Seed {id: 1}) OPTIONAL MATCH p = (n:Person {name: 'Bob'}) RETURN p AS path, nodes(p) AS nodes, relationships(p) AS rels, length(p) AS hops",
            )
            .unwrap();

        let hit_result = engine.execute(&hit, &HashMap::new()).unwrap();
        let miss_result = engine.execute(&miss, &HashMap::new()).unwrap();

        assert_eq!(hit_result.rows.len(), 1);
        assert_eq!(hit_result.rows[0].get("hops"), Some(&Value::from(0)));
        let hit_nodes = hit_result.rows[0]
            .get("nodes")
            .and_then(Value::as_array)
            .expect("expected optional hit nodes");
        assert_eq!(hit_nodes.len(), 1);
        assert_eq!(
            hit_result.rows[0].get("rels"),
            Some(&Value::Array(Vec::new()))
        );

        assert_eq!(miss_result.rows.len(), 1);
        assert_eq!(miss_result.rows[0].get("path"), Some(&Value::Null));
        assert_eq!(miss_result.rows[0].get("hops"), Some(&Value::Null));
        assert_eq!(
            miss_result.rows[0].get("nodes"),
            Some(&Value::Array(Vec::new()))
        );
        assert_eq!(
            miss_result.rows[0].get("rels"),
            Some(&Value::Array(Vec::new()))
        );
    }

    #[test]
    fn test_match_multi_node_cross_join() {
        // MATCH (a:A), (b:B) should produce a cross-join: 2 A nodes +� 3 B nodes = 6 rows,
        // and each row must carry bindings for BOTH `a` and `b`.
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (n:A {v: 1})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (n:A {v: 2})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (n:B {v: 10})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (n:B {v: 20})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (n:B {v: 30})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let q = parser.parse("MATCH (a:A), (b:B) RETURN a, b").unwrap();
        let result = engine.execute(&q, &HashMap::new()).unwrap();

        // 2 +� 3 = 6 rows
        assert_eq!(result.rows.len(), 6, "expected 6 cross-join rows");
        // every row must have both bindings
        for row in &result.rows {
            assert!(row.contains_key("a"), "row missing 'a' binding");
            assert!(row.contains_key("b"), "row missing 'b' binding");
        }
    }

    #[test]
    fn test_match_with_edge_pattern_uses_adjacency_index() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser
                    .parse("CREATE (a:Person {name: 'Alice'})-[r:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let q = parser
            .parse("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS a, r.since AS since, b.name AS b")
            .unwrap();
        let result = engine.execute(&q, &HashMap::new()).unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("a"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(result.rows[0].get("since"), Some(&Value::from(2020)));
        assert_eq!(result.rows[0].get("b"), Some(&Value::String("Bob".into())));
    }

    #[test]
    fn test_match_incoming_and_undirected_relationship_patterns() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser
                    .parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let incoming = parser
            .parse("MATCH (b:Person {name: 'Bob'})<-[r:KNOWS]-(a:Person) RETURN a.name AS name")
            .unwrap();
        let undirected = parser
            .parse("MATCH (b:Person {name: 'Bob'})-[r:KNOWS]-(a:Person) RETURN a.name AS name")
            .unwrap();

        let incoming_result = engine.execute(&incoming, &HashMap::new()).unwrap();
        let undirected_result = engine.execute(&undirected, &HashMap::new()).unwrap();

        assert_eq!(incoming_result.rows.len(), 1);
        assert_eq!(undirected_result.rows.len(), 1);
        assert_eq!(
            incoming_result.rows[0].get("name"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(
            undirected_result.rows[0].get("name"),
            Some(&Value::String("Alice".into()))
        );
    }

    #[test]
    fn test_match_variable_length_relationship_uses_bfs() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser
                    .parse("CREATE (a:Node {name: 'a'})-[:LINK]->(b:Node {name: 'b'})-[:LINK]->(c:Node {name: 'c'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let query = parser
            .parse("MATCH (a:Node {name: 'a'})-[:LINK*1..2]->(n:Node) RETURN n.name AS name")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        let mut names = result
            .rows
            .iter()
            .filter_map(|row| row.get("name").and_then(Value::as_str))
            .collect::<Vec<_>>();
        names.sort();

        assert_eq!(names, vec!["b", "c"]);
    }

    #[test]
    fn test_match_variable_length_exact_hops_and_edge_list_binding() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser
                    .parse("CREATE (a:Node {name: 'a'})-[:LINK {rank: 1}]->(b:Node {name: 'b'})-[:LINK {rank: 2}]->(c:Node {name: 'c'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let query = parser
            .parse(
                "MATCH (a:Node {name: 'a'})-[r:LINK*2]->(n:Node) RETURN n.name AS name, r AS rels",
            )
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("name"), Some(&Value::String("c".into())));
        let rels = result.rows[0]
            .get("rels")
            .and_then(Value::as_array)
            .expect("expected relationship list binding");
        assert_eq!(rels.len(), 2);
    }

    #[test]
    fn test_match_variable_length_path_variable_materializes_path_accessors() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser
                    .parse("CREATE (a:Node {name: 'a'})-[:LINK {rank: 1}]->(b:Node {name: 'b'})-[:LINK {rank: 2}]->(c:Node {name: 'c'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let query = parser
            .parse(
                "MATCH p = (a:Node {name: 'a'})-[r:LINK*2]->(n:Node) RETURN p AS path, nodes(p) AS nodes, relationships(p) AS rels, length(p) AS hops",
            )
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("hops"), Some(&Value::from(2)));

        let nodes = result.rows[0]
            .get("nodes")
            .and_then(Value::as_array)
            .expect("expected path nodes");
        assert_eq!(nodes.len(), 3);
        assert_eq!(nodes[0].get("name").and_then(Value::as_str), Some("a"));
        assert_eq!(nodes[2].get("name").and_then(Value::as_str), Some("c"));

        let rels = result.rows[0]
            .get("rels")
            .and_then(Value::as_array)
            .expect("expected path relationships");
        assert_eq!(rels.len(), 2);
        assert_eq!(rels[0].get("rank").and_then(Value::as_i64), Some(1));

        let path = result.rows[0]
            .get("path")
            .and_then(Value::as_object)
            .expect("expected path value");
        assert_eq!(path.get("length"), Some(&Value::from(2)));
    }

    #[test]
    fn test_match_shortest_path_returns_single_shortest_bfs_path() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE (a:Node {name: 'a'}), (b:Node {name: 'b'}), (c:Node {name: 'c'}), (d:Node {name: 'd'}), (e:Node {name: 'e'})",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "MATCH (a:Node {name: 'a'}), (b:Node {name: 'b'}), (c:Node {name: 'c'}), (d:Node {name: 'd'}), (e:Node {name: 'e'}) CREATE (a)-[:LINK]->(b), (b)-[:LINK]->(d), (a)-[:LINK]->(c), (c)-[:LINK]->(e), (e)-[:LINK]->(d)",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let query = parser
            .parse(
                "MATCH p = shortestPath((a:Node {name: 'a'})-[:LINK*]->(d:Node {name: 'd'})) RETURN length(p) AS hops, nodes(p) AS nodes",
            )
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("hops"), Some(&Value::from(2)));
        let nodes = result.rows[0]
            .get("nodes")
            .and_then(Value::as_array)
            .expect("expected shortest path nodes");
        let names = nodes
            .iter()
            .map(|node| {
                node.get("name")
                    .and_then(Value::as_str)
                    .expect("expected node name")
            })
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["a", "b", "d"]);
    }

    #[test]
    fn raw_shortest_path_uses_the_dedicated_bfs_executor() {
        let engine = make_engine();
        for name in ["a", "b", "c"] {
            store_node(
                engine.storage.as_ref(),
                name,
                &["Node"],
                HashMap::from([("name".into(), Value::String(name.into()))]),
            );
        }
        for (id, start_node, end_node) in [("link:ab", "a", "b"), ("link:bc", "b", "c")] {
            engine
                .storage
                .put_edge_record(&EdgeRecord {
                    id: id.into(),
                    start_node: start_node.into(),
                    end_node: end_node.into(),
                    edge_type: "LINK".into(),
                    properties: BTreeMap::new(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })
                .unwrap();
        }

        let result = engine
            .execute_cypher(
                "MATCH (start:Node {name: 'a'}), (end:Node {name: 'c'}) MATCH p = shortestPath((start)-[:LINK*]->(end)) RETURN length(p) AS hops",
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["hops"]);
        assert_eq!(result.rows[0].get("hops"), Some(&Value::from(2)));
    }

    #[test]
    fn raw_read_result_cache_is_invalidated_by_graph_mutation() {
        let engine = make_engine();
        for name in ["a", "b", "c"] {
            store_node(
                engine.storage.as_ref(),
                name,
                &["Node"],
                HashMap::from([("name".into(), Value::String(name.into()))]),
            );
        }
        for (id, start_node, end_node) in [("link:ab", "a", "b"), ("link:bc", "b", "c")] {
            engine
                .storage
                .put_edge_record(&EdgeRecord {
                    id: id.into(),
                    start_node: start_node.into(),
                    end_node: end_node.into(),
                    edge_type: "LINK".into(),
                    properties: BTreeMap::new(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })
                .unwrap();
        }

        const QUERY: &str =
            "MATCH (a)-[:LINK]->(b)-[:LINK]->(c) RETURN count(c) AS count";
        let first = engine.execute_cypher(QUERY, &HashMap::new()).unwrap();
        let cached = engine.execute_cypher(QUERY, &HashMap::new()).unwrap();
        assert_eq!(first.rows[0].get("count"), Some(&Value::from(1)));
        assert_eq!(cached.rows[0].get("count"), Some(&Value::from(1)));
        assert_eq!(engine.query_result_cache.stats().hits, 1);

        engine.storage.delete_edge_record("link:bc").unwrap();
        let after_delete = engine.execute_cypher(QUERY, &HashMap::new()).unwrap();
        assert_eq!(after_delete.rows[0].get("count"), Some(&Value::from(0)));
    }

    #[test]
    fn policy_schema_change_invalidates_resolver_and_raw_result_cache() {
        let engine = make_engine();
        let parser = Parser::new();
        let stale_time = now_unix_ms() - 5_000;
        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "memory:cached-before-policy".into(),
                labels: vec!["MemoryEpisode".into()],
                properties: BTreeMap::new(),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: stale_time,
                updated_at_unix_ms: stale_time,
            })
            .unwrap();

        const QUERY: &str = "MATCH (n:MemoryEpisode) RETURN n";
        let visible = engine.execute_cypher(QUERY, &HashMap::new()).unwrap();
        let cached = engine.execute_cypher(QUERY, &HashMap::new()).unwrap();
        assert_eq!(visible.rows.len(), 1);
        assert_eq!(cached.rows.len(), 1);

        for cypher in [
            "CREATE DECAY PROFILE cached_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.75, scoreFloor: 0.0, function: 'step', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
            "CREATE DECAY PROFILE cached_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE cached_decay, order: 10 }",
        ] {
            engine
                .execute(&parser.parse(cypher).unwrap(), &HashMap::new())
                .unwrap();
        }

        let hidden = engine.execute_cypher(QUERY, &HashMap::new()).unwrap();
        assert!(hidden.rows.is_empty());
    }

    #[test]
    fn prof_raw_shortest_path_benchmark_breakdown() {
        const NODE_COUNT: usize = 1_000;
        const QUERY: &str = "MATCH (start:Star {starId: 's0'}), (end:Star {starId: 's999'}) MATCH p = shortestPath((start)-[:HYPERLANE*]->(end)) RETURN length(p) AS hops";

        let engine = make_engine();
        engine
            .storage
            .persist_index_definition(&IndexDefinition {
                name: "star_id".into(),
                entity_type: IndexEntityType::Node,
                label: "Star".into(),
                properties: vec!["starId".into()],
                kind: IndexKind::Range,
            })
            .unwrap();
        let nodes = (0..NODE_COUNT)
            .map(|index| NodeRecord {
                id: format!("s{index}"),
                labels: vec!["Star".into()],
                properties: BTreeMap::from([(
                    "starId".into(),
                    Value::String(format!("s{index}")),
                )]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .collect::<Vec<_>>();
        engine.storage.put_node_records_batch(&nodes).unwrap();
        let edges = (0..NODE_COUNT - 1)
            .map(|index| EdgeRecord {
                id: format!("hyperlane:{index}"),
                start_node: format!("s{index}"),
                end_node: format!("s{}", index + 1),
                edge_type: "HYPERLANE".into(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .collect::<Vec<_>>();
        engine.storage.put_edge_records_batch(&edges).unwrap();

        let parser = Parser::new();
        let parse_started = std::time::Instant::now();
        let _query = parser.parse(QUERY).unwrap();
        let parse_elapsed = parse_started.elapsed();

        let resolver = engine.knowledge_policy_resolver().unwrap();
        let adjacency_started = std::time::Instant::now();
        let adjacency = engine
            .bfs_adjacency_map(&["HYPERLANE".into()], &EdgeDirection::Outgoing, &resolver)
            .unwrap();
        let adjacency_elapsed = adjacency_started.elapsed();

        let bfs_started = std::time::Instant::now();
        let path = engine
            .bfs_shortest_path(
                "s0",
                "s999",
                &["HYPERLANE".into()],
                &EdgeDirection::Outgoing,
                NODE_COUNT,
            )
            .unwrap()
            .unwrap();
        let bfs_and_reconstruction_elapsed = bfs_started.elapsed();

        let raw_started = std::time::Instant::now();
        let raw_result = engine.execute_cypher(QUERY, &HashMap::new()).unwrap();
        let raw_elapsed = raw_started.elapsed();
        let cache_hit_started = std::time::Instant::now();
        let cached_result = engine.execute_cypher(QUERY, &HashMap::new()).unwrap();
        let cache_hit_elapsed = cache_hit_started.elapsed();

        assert_eq!(adjacency.len(), NODE_COUNT - 1);
        assert_eq!(path.hops, NODE_COUNT - 1);
        assert_eq!(raw_result.rows[0].get("hops"), Some(&Value::from(999)));
        assert_eq!(cached_result.rows[0].get("hops"), Some(&Value::from(999)));
        eprintln!(
            "raw_shortest_path_1000: parse={parse_elapsed:.2?} adjacency={adjacency_elapsed:.2?} bfs_plus_reconstruction={bfs_and_reconstruction_elapsed:.2?} result_cache_miss_graph_warm={raw_elapsed:.2?} result_cache_hit={cache_hit_elapsed:.2?}"
        );
    }

    #[test]
    fn test_unbounded_shortest_path_can_exceed_fifty_hops() {
        let engine = make_engine();
        let parser = Parser::new();

        for i in 0..=60 {
            let name = format!("n{i}");
            store_node(
                engine.storage.as_ref(),
                &name,
                &["Node"],
                [("name".to_string(), Value::String(name.clone()))]
                    .into_iter()
                    .collect(),
            );
        }
        for i in 0..60 {
            engine
                .storage
                .put_edge_record(&EdgeRecord {
                    id: format!("link:{i}"),
                    start_node: format!("n{i}"),
                    end_node: format!("n{}", i + 1),
                    edge_type: "LINK".to_string(),
                    properties: BTreeMap::new(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })
                .unwrap();
        }

        let result = engine
            .execute(
                &parser
                    .parse(
                        "MATCH p = shortestPath((a:Node {name: 'n0'})-[:LINK*]->(z:Node {name: 'n60'})) RETURN length(p) AS hops",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("hops"), Some(&Value::from(60)));
    }

    #[test]
    fn test_execute_with_routes_shortest_path_demo_mesh_performance() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser
                    .parse("CREATE INDEX star_id_idx IF NOT EXISTS FOR (n:Star) ON (n.starId)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let star_count = 512;
        for index in 0..=star_count {
            store_node(
                engine.storage.as_ref(),
                &format!("star:{index}"),
                &["Star"],
                HashMap::from([("starId".into(), Value::String(format!("s{index}")))]),
            );
        }

        let mut edges = Vec::new();
        for index in 0..star_count {
            edges.push(EdgeRecord {
                id: format!("lane:chain:{index}"),
                start_node: format!("star:{index}"),
                end_node: format!("star:{}", index + 1),
                edge_type: "HYPERLANE".to_string(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            });
            if index + 7 <= star_count {
                edges.push(EdgeRecord {
                    id: format!("lane:skip7:{index}"),
                    start_node: format!("star:{index}"),
                    end_node: format!("star:{}", index + 7),
                    edge_type: "HYPERLANE".to_string(),
                    properties: BTreeMap::new(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                });
            }
            if index + 31 <= star_count {
                edges.push(EdgeRecord {
                    id: format!("lane:skip31:{index}"),
                    start_node: format!("star:{index}"),
                    end_node: format!("star:{}", index + 31),
                    edge_type: "HYPERLANE".to_string(),
                    properties: BTreeMap::new(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                });
            }
        }
        engine.storage.put_edge_records_batch(&edges).unwrap();

        let cypher = "MATCH (start:Star {starId: $startId}), (end:Star {starId: $endId}) MATCH p = shortestPath((start)-[:HYPERLANE*]-(end)) RETURN [n IN nodes(p) | n.starId] AS pathIds, length(p) AS hops LIMIT 1";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);
        let (shape_match, compound_ok) = match_compound_query_shape(cypher);
        let (pipeline_clauses, pipeline_ok) = can_execute_as_pipeline(cypher);
        let params = HashMap::from([
            ("startId".to_string(), Value::String("s0".to_string())),
            (
                "endId".to_string(),
                Value::String(format!("s{star_count}")),
            ),
        ]);

        let started = std::time::Instant::now();
        let result = engine
            .execute_with_routes(
                &query,
                &params,
                &pattern,
                compound_ok.then_some(&shape_match),
                pipeline_ok.then_some(pipeline_clauses.as_slice()),
            )
            .unwrap();
        let elapsed = started.elapsed();

        assert_eq!(result.rows.len(), 1);
        let path_ids = result.rows[0]
            .get("pathIds")
            .and_then(Value::as_array)
            .expect("expected star id path");
        assert_eq!(path_ids.first(), Some(&Value::String("s0".to_string())));
        assert_eq!(
            path_ids.last(),
            Some(&Value::String(format!("s{star_count}")))
        );
        assert!(result.rows[0].get("hops").and_then(Value::as_i64).unwrap_or(0) > 0);
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "demo-shaped shortestPath should stay on the optimized BFS path, took {elapsed:?}"
        );
    }

    /// d3_demo profiling harness: measures per-phase latency so we can
    /// isolate bottlenecks and compare before/after optimisations.
    /// Run with: cargo test -p copperdb-eval --lib prof_d3_demo -- --nocapture
    #[test]
    fn prof_d3_demo_phases() {
        let warmup_iters = 2usize;
        let timed_iters = 5usize;
        let node_count = 401usize;
        let edge_count = 400usize;

        macro_rules! phase {
            ($label:expr, $timings:expr, $body:block) => {{
                let start = std::time::Instant::now();
                let _r = { $body };
                let elapsed = start.elapsed();
                eprintln!("  [{}]: {:.2?}", $label, elapsed);
                $timings.push(($label, elapsed));
            }};
        }

        // ── Warmup (discarded) ──────────────────────────────────────
        for _ in 0..warmup_iters {
            let engine = make_engine();
            let storage = engine.storage.clone();
            let parser = Parser::new();
            engine
                .execute(
                    &parser
                        .parse("CREATE INDEX star_id_idx IF NOT EXISTS FOR (n:Star) ON (n.starId)")
                        .unwrap(),
                    &HashMap::new(),
                )
                .unwrap();
            let mut nodes = Vec::with_capacity(node_count);
            for i in 0..node_count {
                nodes.push(NodeRecord {
                    id: format!("star:{i}"),
                    labels: vec!["Star".to_string()],
                    properties: BTreeMap::from([("starId".into(), Value::String(format!("s0-{i}")))]),
                    named_embeddings: BTreeMap::new(),
                    chunk_embeddings: Vec::new(),
                    embed_meta: Default::default(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                });
            }
            storage.put_node_records_batch(&nodes).unwrap();
            let mut edges = Vec::with_capacity(edge_count);
            for i in 0..edge_count {
                edges.push(EdgeRecord {
                    id: format!("lane:chain:{i}"),
                    start_node: format!("star:{i}"),
                    end_node: format!("star:{}", i + 1),
                    edge_type: "HYPERLANE".to_string(),
                    properties: BTreeMap::new(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                });
            }
            storage.put_edge_records_batch(&edges).unwrap();
            let cypher = "MATCH (start:Star {starId: $startId}), (end:Star {starId: $endId}) MATCH p = shortestPath((start)-[:HYPERLANE*]-(end)) RETURN [n IN nodes(p) | n.starId] AS pathIds, length(p) AS hops LIMIT 1";
            let query = parser.parse(cypher).unwrap();
            let pattern = detect_query_pattern(cypher);
            let (shape_match, compound_ok) = match_compound_query_shape(cypher);
            let (pipeline_clauses, pipeline_ok) = can_execute_as_pipeline(cypher);
            let params = HashMap::from([
                ("startId".into(), Value::String("s0-0".into())),
                ("endId".into(), Value::String(format!("s0-{edge_count}"))),
            ]);
            engine.execute_with_routes(&query, &params, &pattern, compound_ok.then_some(&shape_match), pipeline_ok.then_some(pipeline_clauses.as_slice())).unwrap();
        }

        // ── Timed iterations ────────────────────────────────────────
        let mut phase_sums: HashMap<&str, Vec<std::time::Duration>> = HashMap::new();
        for _iter in 0..timed_iters {
            let engine = make_engine();
            let storage = engine.storage.clone();
            let parser = Parser::new();

            let mut iter_timings: Vec<(&str, std::time::Duration)> = Vec::new();
            phase!("create-index", iter_timings, {
                engine.execute(&parser.parse("CREATE INDEX star_id_idx IF NOT EXISTS FOR (n:Star) ON (n.starId)").unwrap(), &HashMap::new()).unwrap();
            });
            phase!("seed-401-nodes", iter_timings, {
                let mut nodes = Vec::with_capacity(node_count);
                for i in 0..node_count {
                    nodes.push(NodeRecord {
                        id: format!("star:{i}"),
                        labels: vec!["Star".to_string()],
                        properties: BTreeMap::from([("starId".into(), Value::String(format!("s0-{i}")))]),
                        named_embeddings: BTreeMap::new(),
                        chunk_embeddings: Vec::new(),
                        embed_meta: Default::default(),
                        created_at_unix_ms: 0,
                        updated_at_unix_ms: 0,
                    });
                }
                storage.put_node_records_batch(&nodes).unwrap();
            });
            let mut edges = Vec::with_capacity(edge_count);
            for i in 0..edge_count {
                edges.push(EdgeRecord {
                    id: format!("lane:chain:{i}"),
                    start_node: format!("star:{i}"),
                    end_node: format!("star:{}", i + 1),
                    edge_type: "HYPERLANE".to_string(),
                    properties: BTreeMap::new(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                });
            }
            phase!("seed-400-edges-batch", iter_timings, {
                storage.put_edge_records_batch(&edges).unwrap();
            });
            let cypher = "MATCH (start:Star {starId: $startId}), (end:Star {starId: $endId}) MATCH p = shortestPath((start)-[:HYPERLANE*]-(end)) RETURN [n IN nodes(p) | n.starId] AS pathIds, length(p) AS hops LIMIT 1";
            let query = parser.parse(cypher).unwrap();
            let pattern = detect_query_pattern(cypher);
            let (shape_match, compound_ok) = match_compound_query_shape(cypher);
            let (pipeline_clauses, pipeline_ok) = can_execute_as_pipeline(cypher);
            let params: HashMap<String, Value> = HashMap::from([
                ("startId".into(), Value::String("s0-0".into())),
                ("endId".into(), Value::String(format!("s0-{edge_count}"))),
            ]);
            phase!("shortest-path", iter_timings, {
                let t0 = std::time::Instant::now();
                let result = engine.execute_with_routes(&query, &params, &pattern, compound_ok.then_some(&shape_match), pipeline_ok.then_some(pipeline_clauses.as_slice())).unwrap();
                let t1 = t0.elapsed();
                eprintln!("    execute_with_routes={:.2?}", t1);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0].get("hops").and_then(Value::as_i64), Some(400));
            });

            for (label, dur) in iter_timings {
                phase_sums.entry(label).or_default().push(dur);
            }
        }

        // ── Print summary ───────────────────────────────────────────
        eprintln!("\n=== d3_demo profiling summary ({timed_iters} iters) ===");
        for label in &["create-index", "seed-401-nodes", "seed-400-edges-batch", "shortest-path"] {
            let durs = &phase_sums[*label];
            let min = durs.iter().min().unwrap();
            let max = durs.iter().max().unwrap();
            let avg = durs.iter().sum::<std::time::Duration>() / durs.len() as u32;
            eprintln!("  {label:30} min={min:.2?} max={max:.2?} avg={avg:.2?}");
        }
        eprintln!("=== end profiling ===\n");
    }

    /// Break down shortestPath cost into parse + plan + execute.
    #[test]
    fn prof_shortest_path_cost_breakdown() {
        let node_count = 401usize;
        let edge_count = 400usize;
        let iters = 3usize;

        let cypher = "MATCH (start:Star {starId: $startId}), (end:Star {starId: $endId}) MATCH p = shortestPath((start)-[:HYPERLANE*]-(end)) RETURN [n IN nodes(p) | n.starId] AS pathIds, length(p) AS hops LIMIT 1";
        let params: HashMap<String, Value> = HashMap::from([
            ("startId".into(), Value::String("s0-0".into())),
            ("endId".into(), Value::String(format!("s0-{edge_count}"))),
        ]);

        for _ in 0..iters {
            let engine = make_engine();
            let storage = engine.storage.clone();
            let parser = Parser::new();

            // Seed data
            engine.execute(&parser.parse("CREATE INDEX star_id_idx IF NOT EXISTS FOR (n:Star) ON (n.starId)").unwrap(), &HashMap::new()).unwrap();
            let mut nodes = Vec::with_capacity(node_count);
            for i in 0..node_count {
                nodes.push(NodeRecord {
                    id: format!("star:{i}"),
                    labels: vec!["Star".to_string()],
                    properties: BTreeMap::from([("starId".into(), Value::String(format!("s0-{i}")))]),
                    named_embeddings: BTreeMap::new(),
                    chunk_embeddings: Vec::new(),
                    embed_meta: Default::default(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                });
            }
            storage.put_node_records_batch(&nodes).unwrap();
            let mut edges = Vec::with_capacity(edge_count);
            for i in 0..edge_count {
                edges.push(EdgeRecord {
                    id: format!("lane:chain:{i}"),
                    start_node: format!("star:{i}"),
                    end_node: format!("star:{}", i + 1),
                    edge_type: "HYPERLANE".to_string(),
                    properties: BTreeMap::new(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                });
            }
            storage.put_edge_records_batch(&edges).unwrap();

            // ── Parse ───────────────────────────────────────────────
            let t0 = std::time::Instant::now();
            let query = parser.parse(cypher).unwrap();
            let parse_time = t0.elapsed();

            // ── Plan (pattern + shape + pipeline detection) ─────────
            let t1 = std::time::Instant::now();
            let pattern = detect_query_pattern(cypher);
            let (shape_match, compound_ok) = match_compound_query_shape(cypher);
            let (pipeline_clauses, pipeline_ok) = can_execute_as_pipeline(cypher);
            let plan_time = t1.elapsed();

            // ── Execute ─────────────────────────────────────────────
            let t2 = std::time::Instant::now();
            let result = engine
                .execute_with_routes(
                    &query,
                    &params,
                    &pattern,
                    compound_ok.then_some(&shape_match),
                    pipeline_ok.then_some(pipeline_clauses.as_slice()),
                )
                .unwrap();
            let exec_time = t2.elapsed();
            let total = parse_time + plan_time + exec_time;

            assert_eq!(result.rows.len(), 1);
            assert_eq!(
                result.rows[0].get("hops").and_then(Value::as_i64),
                Some(400)
            );

            eprintln!(
                "shortestPath:  parse={parse_time:.2?}  plan={plan_time:.2?}  exec={exec_time:.2?}  total={total:.2?}  hop_count=400"
            );
        }
    }

    /// Profile BFS adjacency map build and queue loop for a dense mesh.
    /// Run with: cargo test -p copperdb-eval --lib prof_bfs_mesh_cost_breakdown -- --nocapture
    #[test]
    fn prof_bfs_mesh_cost_breakdown() {
        let iters = 5usize;

        for _round in 0..iters {
            let engine = make_engine();
            let storage = engine.storage.clone();

            // Create a chain of 48 nodes (guaranteed 47-hop path) but also add
            // cross-edges to create a mesh — each node connects to 4 neighbors
            // ahead, simulating a realistic dense graph.
            let node_count = 48usize;
            let cross_edges_per_node = 4usize;

            let mut nodes = Vec::with_capacity(node_count);
            for i in 0..node_count {
                nodes.push(NodeRecord {
                    id: format!("n{i}"),
                    labels: vec!["Mesh".to_string()],
                    properties: BTreeMap::from([(
                        "idx".to_string(),
                        Value::from(i as i64),
                    )]),
                    named_embeddings: BTreeMap::new(),
                    chunk_embeddings: Vec::new(),
                    embed_meta: Default::default(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                });
            }
            storage.put_node_records_batch(&nodes).unwrap();

            let mut edges = Vec::new();
            let mut edge_id = 0usize;
            // Chain backbone: n0→n1→n2→...→n47
            for i in 0..node_count - 1 {
                edges.push(EdgeRecord {
                    id: format!("e_chain_{i}"),
                    start_node: format!("n{i}"),
                    end_node: format!("n{}", i + 1),
                    edge_type: "CONNECTS".to_string(),
                    properties: BTreeMap::new(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                });
                edge_id += 1;
            }
            // Cross-edges: each node connects to nodes i+2, i+3, i+4, i+5
            for i in 0..node_count {
                for skip in 2..2 + cross_edges_per_node {
                    let j = i + skip;
                    if j < node_count {
                        edges.push(EdgeRecord {
                            id: format!("e_x_{edge_id}"),
                            start_node: format!("n{i}"),
                            end_node: format!("n{j}"),
                            edge_type: "CONNECTS".to_string(),
                            properties: BTreeMap::new(),
                            created_at_unix_ms: 0,
                            updated_at_unix_ms: 0,
                        });
                        edge_id += 1;
                    }
                }
            }
            let edge_count = edges.len();
            storage.put_edge_records_batch(&edges).unwrap();

            // ── Adjacency map build ────────────────────────────────
            let resolver = engine.knowledge_policy_resolver().unwrap();
            let direction = crate::EdgeDirection::Both;
            let rel_types = vec!["CONNECTS".to_string()];

            let t0 = std::time::Instant::now();
            let adjacency =
                engine.bfs_adjacency_map(&rel_types, &direction, &resolver).unwrap();
            let adj_time = t0.elapsed();
            let adj_entries: usize = adjacency.values().map(|v| v.len()).sum();

            // ── BFS queue loop ─────────────────────────────────────
            let t1 = std::time::Instant::now();
            let path = engine
                .bfs_shortest_path("n0", "n47", &rel_types, &direction, 100)
                .unwrap();
            let bfs_time = t1.elapsed();

            // ── Full Cypher e2e (public API, now includes dedicated BFS) ──
            let parser = Parser::new();
            let cypher = "MATCH (start:Mesh {idx: 0}), (end:Mesh {idx: 47}) MATCH p = shortestPath((start)-[:CONNECTS*]-(end)) RETURN length(p) AS hops LIMIT 1";
            let params: HashMap<String, Value> = HashMap::new();

            let t_start = std::time::Instant::now();
            let parsed = parser.parse(cypher).unwrap();
            let parse_us = t_start.elapsed().as_micros();

            let t_exec_start = std::time::Instant::now();
            let result = engine
                .execute(&parsed, &params)
                .unwrap();
            let exec_us = t_exec_start.elapsed().as_micros();
            let e2e_us = t_start.elapsed().as_micros();

            let hops = result.rows[0]
                .get("hops")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let bfs_hops = path.as_ref().map(|p| p.hops).unwrap_or(0);

            eprintln!(
                "bfs_mesh  nodes={node_count} edges={edge_count} adj_entries={adj_entries}  adj={adj_time:.2?}  bfs={bfs_time:.2?}  e2e_total={e2e_us}µs  hops={hops}  bfs_hops={bfs_hops}  parse={parse_us}µs  exec={exec_us}µs",
            );
        }
    }

    /// Break down `put_node_records_batch` cost into construction vs storage I/O.
    #[test]
    fn prof_batch_node_insert_cost_breakdown() {
        let node_count = 401usize;
        let iters = 3usize;

        for _ in 0..iters {
            let engine = make_engine();
            let storage = engine.storage.clone();

            // ── Build + alloc cost ──────────────────────────────────
            let t0 = std::time::Instant::now();
            let mut nodes = Vec::with_capacity(node_count);
            for i in 0..node_count {
                nodes.push(NodeRecord {
                    id: format!("star:{i}"),
                    labels: vec!["Star".to_string()],
                    properties: BTreeMap::from([(
                        "starId".into(),
                        Value::String(format!("s0-{i}")),
                    )]),
                    named_embeddings: BTreeMap::new(),
                    chunk_embeddings: Vec::new(),
                    embed_meta: Default::default(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                });
            }
            let build_time = t0.elapsed();

            // ── Full batch insert ───────────────────────────────────
            let t1 = std::time::Instant::now();
            storage.put_node_records_batch(&nodes).unwrap();
            let insert_time = t1.elapsed();

            // ── Serialisation-only (estimate using storage's encode) ─
            // Build fresh nodes each time to avoid allocator reuse bias
            let mut nodes2 = Vec::with_capacity(node_count);
            for i in 0..node_count {
                nodes2.push(NodeRecord {
                    id: format!("star:{i}"),
                    labels: vec!["Star".to_string()],
                    properties: BTreeMap::from([(
                        "starId".into(),
                        Value::String(format!("s0-{i}")),
                    )]),
                    named_embeddings: BTreeMap::new(),
                    chunk_embeddings: Vec::new(),
                    embed_meta: Default::default(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                });
            }
            let t2 = std::time::Instant::now();
            // Simulate what storage does: ser + (optionally encrypt)
            let mut ser_buf = Vec::with_capacity(node_count * 128);
            for node in &nodes2 {
                ser_buf.push(rmp_serde::to_vec(node).unwrap());
            }
            let ser_only = t2.elapsed();

            let io_and_index = insert_time
                .checked_sub(ser_only)
                .unwrap_or_default();

            eprintln!(
                "nodes={node_count}  build={build_time:.2?}  ser_only={ser_only:.2?}  insert={insert_time:.2?}  io_and_index={io_and_index:.2?}  per_node_insert={:.1}µs  per_node_io={:.1}µs",
                insert_time.as_micros() as f64 / node_count as f64,
                io_and_index.as_micros() as f64 / node_count as f64,
            );
        }
    }

    #[test]
    fn test_create_path_variable_materializes_path_accessors() {
        let engine = make_engine();
        let parser = Parser::new();

        let create = parser
            .parse(
                "CREATE p = (a:Node {name: 'a'})-[:LINK {rank: 1}]->(b:Node {name: 'b'}) RETURN p AS path, nodes(p) AS nodes, relationships(p) AS rels, length(p) AS hops",
            )
            .unwrap();
        let result = engine.execute(&create, &HashMap::new()).unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("hops"), Some(&Value::from(1)));

        let nodes = result.rows[0]
            .get("nodes")
            .and_then(Value::as_array)
            .expect("expected path nodes");
        assert_eq!(nodes.len(), 2);

        let rels = result.rows[0]
            .get("rels")
            .and_then(Value::as_array)
            .expect("expected path relationships");
        assert_eq!(rels.len(), 1);
        assert_eq!(rels[0].get("rank").and_then(Value::as_i64), Some(1));

        let path = result.rows[0]
            .get("path")
            .and_then(Value::as_object)
            .expect("expected path value");
        assert_eq!(path.get("length"), Some(&Value::from(1)));
    }

    #[test]
    fn test_match_variable_length_relationship_large_chain_consistency() {
        let engine = make_engine();
        let parser = Parser::new();

        for index in 0..25 {
            let mut props = HashMap::new();
            props.insert("_id".to_string(), Value::String(format!("Node:{index}")));
            props.insert(
                "_labels".to_string(),
                Value::Array(vec![Value::String("Node".into())]),
            );
            props.insert("name".to_string(), Value::String(format!("n{index:02}")));
            store_node(
                engine.storage.as_ref(),
                &format!("Node:{index}"),
                &["Node"],
                props,
            );
        }

        for index in 0..24 {
            engine
                .storage
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

        let query = parser
            .parse("MATCH (a:Node {name: 'n00'})-[:LINK*1..24]->(n:Node) RETURN n.name AS name")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
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

    /// Benchmark just the serialization + batch-apply cost of edge creation.
    #[test]
    fn prof_edge_batch_raw_cost() {
        let edge_count = 400usize;

        let engine = make_engine();
        let storage = engine.storage.clone();

        let mut edges = Vec::with_capacity(edge_count);
        for i in 0..edge_count {
            edges.push(EdgeRecord {
                id: format!("lane:chain:{i}"),
                start_node: format!("star:{i}"),
                end_node: format!("star:{}", i + 1),
                edge_type: "HYPERLANE".to_string(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            });
        }

        // Ser-only
        let t0 = std::time::Instant::now();
        let mut ser_buf = Vec::with_capacity(edge_count);
        for edge in &edges {
            ser_buf.push(rmp_serde::to_vec(edge).unwrap());
        }
        let ser_time = t0.elapsed();

        // Full batch API
        let t1 = std::time::Instant::now();
        storage.put_edge_records_batch(&edges).unwrap();
        let batch_time = t1.elapsed();

        eprintln!(
            "edges={edge_count}  ser={ser_time:.2?}  batch={batch_time:.2?}  per_edge={:.1}µs",
            batch_time.as_micros() as f64 / edge_count as f64
        );

        // ── Index key format cost ──────────────────────────────────
        let t3 = std::time::Instant::now();
        let mut key_count = 0usize;
        for edge in &edges {
            let _k1 = format!("edge_type/{}/{}", &edge.edge_type, &edge.id);
            let _k2 = format!("edge_start/{}/{}", &edge.start_node, &edge.id);
            let _k3 = format!("edge_end/{}/{}", &edge.end_node, &edge.id);
            key_count += 3;
            std::hint::black_box((&_k1, &_k2, &_k3));
        }
        let key_fmt_time = t3.elapsed();

        eprintln!(
            "key_format: {key_fmt_time:.2?} for {key_count} keys ({:.1}µs/key, {:.1}µs/edge)",
            key_fmt_time.as_micros() as f64 / key_count as f64,
            key_fmt_time.as_micros() as f64 / edge_count as f64,
        );
    }

mod regressions;
