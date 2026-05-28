    use super::*;
    use copperdb_cypher::{
        can_execute_as_pipeline, detect_query_pattern, match_compound_query_shape, Parser,
        QueryPattern,
    };
    use copperdb_storage::{EdgeRecord, NodeRecord, StorageEngine};

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
                    (props.get(property) == Some(&Value::from(expected))).then(|| node.id)
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


mod regressions;
