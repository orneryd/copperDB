#[test]
    fn test_call_vector_query_nodes_prefers_named_embeddings_over_property_and_chunk_fallback() {
        let engine = make_engine();
        let parser = Parser::new();

        let put_node = |node: NodeRecord| {
            engine.storage.put_node_record(&node).unwrap();
        };

        put_node(NodeRecord {
            id: "doc-named".to_string(),
            labels: vec!["Doc".to_string()],
            properties: BTreeMap::from([(
                "title".to_string(),
                Value::String("not a vector".to_string()),
            )]),
            named_embeddings: BTreeMap::from([("title".to_string(), vec![1.0, 0.0, 0.0])]),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        });
        put_node(NodeRecord {
            id: "doc-prop".to_string(),
            labels: vec!["Doc".to_string()],
            properties: BTreeMap::from([(
                "title".to_string(),
                Value::Array(vec![Value::from(0.8), Value::from(0.2), Value::from(0.0)]),
            )]),
            named_embeddings: BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        });
        put_node(NodeRecord {
            id: "doc-chunk".to_string(),
            labels: vec!["Doc".to_string()],
            properties: BTreeMap::new(),
            named_embeddings: BTreeMap::new(),
            chunk_embeddings: vec![vec![0.5, 0.5, 0.0]],
            embed_meta: Default::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        });
        put_node(NodeRecord {
            id: "doc-both".to_string(),
            labels: vec!["Doc".to_string()],
            properties: BTreeMap::from([(
                "title".to_string(),
                Value::Array(vec![Value::from(1.0), Value::from(0.0), Value::from(0.0)]),
            )]),
            named_embeddings: BTreeMap::from([("title".to_string(), vec![0.0, 1.0, 0.0])]),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        });
        put_node(NodeRecord {
            id: "other-label".to_string(),
            labels: vec!["Other".to_string()],
            properties: BTreeMap::new(),
            named_embeddings: BTreeMap::from([("title".to_string(), vec![1.0, 0.0, 0.0])]),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        });

        engine
            .execute(
                &parser
                    .parse("CREATE VECTOR INDEX title_idx FOR (n:Doc) ON (n.title)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("CALL db.index.vector.queryNodes('title_idx', 10, [1,0,0])")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["node", "score"]);
        assert_eq!(result.rows.len(), 4);

        let ids = result
            .rows
            .iter()
            .map(|row| {
                row.get("node")
                    .and_then(Value::as_object)
                    .and_then(|node| node.get("_id"))
                    .and_then(Value::as_str)
                    .unwrap()
                    .to_string()
            })
            .collect::<Vec<_>>();
        let scores = result
            .rows
            .iter()
            .map(|row| row.get("score").and_then(Value::as_f64).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(ids, vec!["doc-named", "doc-prop", "doc-chunk", "doc-both"]);
        assert!(scores[0] > scores[1]);
        assert!(scores[1] > scores[2]);
        assert!(scores[2] > scores[3]);
        assert_eq!(
            result.rows[0]
                .get("node")
                .and_then(Value::as_object)
                .and_then(|node| node.get("title")),
            Some(&Value::String("not a vector".to_string()))
        );
    }

    #[test]
    fn test_call_vector_query_nodes_yield_and_return_pipeline() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "doc-1".to_string(),
                labels: vec!["Doc".to_string()],
                properties: BTreeMap::from([(
                    "title".to_string(),
                    Value::String("Document one".to_string()),
                )]),
                named_embeddings: BTreeMap::from([("title".to_string(), vec![1.0, 0.0, 0.0])]),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "doc-2".to_string(),
                labels: vec!["Doc".to_string()],
                properties: BTreeMap::new(),
                named_embeddings: BTreeMap::from([("title".to_string(), vec![0.8, 0.2, 0.0])]),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        engine
            .execute(
                &parser
                    .parse("CREATE VECTOR INDEX title_idx FOR (n:Doc) ON (n.title)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse(
                        "CALL db.index.vector.queryNodes('title_idx', 5, [1,0,0]) YIELD node, score RETURN node._id AS id, score ORDER BY score DESC",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["id", "score"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("id"), Some(&Value::String("doc-1".to_string())));
        assert_eq!(result.rows[1].get("id"), Some(&Value::String("doc-2".to_string())));
        assert!(
            result.rows[0]
                .get("score")
                .and_then(Value::as_f64)
                .unwrap()
                > result.rows[1]
                    .get("score")
                    .and_then(Value::as_f64)
                    .unwrap()
        );
    }

    #[test]
    fn test_call_vector_query_nodes_yield_aliases_flow_into_return() {
        let engine = make_engine();
        let parser = Parser::new();

        for (id, vector) in [("doc-1", vec![1.0, 0.0, 0.0]), ("doc-2", vec![0.8, 0.2, 0.0])] {
            engine
                .storage
                .put_node_record(&NodeRecord {
                    id: id.to_string(),
                    labels: vec!["Doc".to_string()],
                    properties: BTreeMap::new(),
                    named_embeddings: BTreeMap::from([("title".to_string(), vector)]),
                    chunk_embeddings: Vec::new(),
                    embed_meta: Default::default(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })
                .unwrap();
        }

        engine
            .execute(
                &parser
                    .parse("CREATE VECTOR INDEX title_idx FOR (n:Doc) ON (n.title)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse(
                        "CALL db.index.vector.queryNodes('title_idx', 5, [1,0,0]) YIELD node AS hit, score AS similarity RETURN hit._id AS id, similarity AS value ORDER BY similarity DESC",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["id", "value"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("id"), Some(&Value::String("doc-1".to_string())));
        assert_eq!(result.rows[1].get("id"), Some(&Value::String("doc-2".to_string())));
        assert!(
            result.rows[0]
                .get("value")
                .and_then(Value::as_f64)
                .unwrap()
                > result.rows[1]
                    .get("value")
                    .and_then(Value::as_f64)
                    .unwrap()
        );
    }

    #[test]
    fn test_call_vector_query_nodes_yield_wildcard_flows_into_return() {
        let engine = make_engine();
        let parser = Parser::new();

        for (id, vector) in [("doc-1", vec![1.0, 0.0, 0.0]), ("doc-2", vec![0.8, 0.2, 0.0])] {
            engine
                .storage
                .put_node_record(&NodeRecord {
                    id: id.to_string(),
                    labels: vec!["Doc".to_string()],
                    properties: BTreeMap::new(),
                    named_embeddings: BTreeMap::from([("title".to_string(), vector)]),
                    chunk_embeddings: Vec::new(),
                    embed_meta: Default::default(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })
                .unwrap();
        }

        engine
            .execute(
                &parser
                    .parse("CREATE VECTOR INDEX title_idx FOR (n:Doc) ON (n.title)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse(
                        "CALL db.index.vector.queryNodes('title_idx', 5, [1,0,0]) YIELD * RETURN node._id AS id, score ORDER BY score DESC",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["id", "score"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("id"), Some(&Value::String("doc-1".to_string())));
        assert_eq!(result.rows[1].get("id"), Some(&Value::String("doc-2".to_string())));
        assert!(
            result.rows[0]
                .get("score")
                .and_then(Value::as_f64)
                .unwrap()
                > result.rows[1]
                    .get("score")
                    .and_then(Value::as_f64)
                    .unwrap()
        );
    }

    #[test]
    fn test_call_vector_query_nodes_yield_where_elementid_filters_rows() {
        let engine = make_engine();
        let parser = Parser::new();

        for (id, vector) in [("doc-1", vec![1.0, 0.0, 0.0]), ("doc-2", vec![0.8, 0.2, 0.0])] {
            engine
                .storage
                .put_node_record(&NodeRecord {
                    id: id.to_string(),
                    labels: vec!["Doc".to_string()],
                    properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String(id.to_string()),
                    )]),
                    named_embeddings: BTreeMap::from([("title".to_string(), vector)]),
                    chunk_embeddings: Vec::new(),
                    embed_meta: Default::default(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })
                .unwrap();
        }

        engine
            .execute(
                &parser
                    .parse("CREATE VECTOR INDEX title_idx FOR (n:Doc) ON (n.title)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let mut params = HashMap::new();
        params.insert("rootID".to_string(), Value::String("doc-1".to_string()));

        let result = engine
            .execute(
                &parser
                    .parse(
                        "CALL db.index.vector.queryNodes('title_idx', 10, [1,0,0]) YIELD node, score WHERE elementId(node) = $rootID RETURN node.title AS title, score",
                    )
                    .unwrap(),
                &params,
            )
            .unwrap();

        assert_eq!(result.columns, vec!["title", "score"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("title"),
            Some(&Value::String("doc-1".to_string()))
        );
    }

    #[test]
    fn test_call_db_labels_yield_where_return_filters_rows() {
        let engine = make_engine();
        let parser = Parser::new();

        for node in [
            NodeRecord {
                id: "memory:1".to_string(),
                labels: vec!["Memory".to_string()],
                properties: BTreeMap::new(),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            NodeRecord {
                id: "todo:1".to_string(),
                labels: vec!["Todo".to_string()],
                properties: BTreeMap::new(),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
        ] {
            engine.storage.put_node_record(&node).unwrap();
        }

        let result = engine
            .execute(
                &parser
                    .parse("CALL db.labels() YIELD label WHERE label = 'Memory' RETURN label")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["label"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("label"),
            Some(&Value::String("Memory".to_string()))
        );
    }

    #[test]
    fn test_call_db_relationship_types_yield_return_orders_rows() {
        let engine = make_engine();
        let parser = Parser::new();

        for node in [
            NodeRecord {
                id: "person:1".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::new(),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            NodeRecord {
                id: "person:2".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::new(),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
        ] {
            engine.storage.put_node_record(&node).unwrap();
        }
        for edge in [
            EdgeRecord {
                id: "edge:1".to_string(),
                start_node: "person:1".to_string(),
                end_node: "person:2".to_string(),
                edge_type: "WORKS_WITH".to_string(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "edge:2".to_string(),
                start_node: "person:2".to_string(),
                end_node: "person:1".to_string(),
                edge_type: "KNOWS".to_string(),
                properties: BTreeMap::new(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
        ] {
            engine.storage.put_edge_record(&edge).unwrap();
        }

        let result = engine
            .execute(
                &parser
                    .parse(
                        "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType ORDER BY relationshipType ASC",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["relationshipType"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(
            result.rows[0].get("relationshipType"),
            Some(&Value::String("KNOWS".to_string()))
        );
        assert_eq!(
            result.rows[1].get("relationshipType"),
            Some(&Value::String("WORKS_WITH".to_string()))
        );
    }

    #[test]
    fn test_call_db_property_keys_yield_return_sorted() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "person:1".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([
                    ("name".to_string(), Value::String("Alice".to_string())),
                    ("age".to_string(), Value::from(30)),
                ]),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        engine
            .storage
            .put_edge_record(&EdgeRecord {
                id: "edge:1".to_string(),
                start_node: "person:1".to_string(),
                end_node: "person:2".to_string(),
                edge_type: "KNOWS".to_string(),
                properties: BTreeMap::from([(
                    "since".to_string(),
                    Value::String("2024".to_string()),
                )]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey ORDER BY propertyKey ASC")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["propertyKey"]);
        assert_eq!(result.rows.len(), 3);
        assert_eq!(
            result.rows[0].get("propertyKey"),
            Some(&Value::String("age".to_string()))
        );
        assert_eq!(
            result.rows[1].get("propertyKey"),
            Some(&Value::String("name".to_string()))
        );
        assert_eq!(
            result.rows[2].get("propertyKey"),
            Some(&Value::String("since".to_string()))
        );
    }

    #[test]
    fn test_call_fulltext_query_nodes_node_search_with_limit_zero_returns_empty() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("CREATE (:Doc {content: 'searchable content'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE FULLTEXT INDEX idx_ft FOR (n:Doc) ON (n.content)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse(
                        "CALL db.index.fulltext.queryNodes('node_search', 'searchable') YIELD node, score RETURN node.content AS content, score LIMIT 0",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn test_call_fulltext_query_nodes_node_search_skip_beyond_returns_empty() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("CREATE (:Doc {content: 'searchable content'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE FULLTEXT INDEX idx_ft FOR (n:Doc) ON (n.content)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse(
                        "CALL db.index.fulltext.queryNodes('node_search', 'searchable') YIELD node, score RETURN node.content AS content, score SKIP 1000",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn test_call_fulltext_query_nodes_node_search_order_by_nonexistent_does_not_panic() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("CREATE (:Doc {content: 'searchable content'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE FULLTEXT INDEX idx_ft FOR (n:Doc) ON (n.content)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine.execute(
            &parser
                .parse(
                    "CALL db.index.fulltext.queryNodes('node_search', 'searchable') YIELD node, score RETURN node.content AS content, score ORDER BY nonexistent DESC LIMIT 3",
                )
                .unwrap(),
            &HashMap::new(),
        );

        // Must not panic — either clean error or stable result with null-key ordering.
        if let Ok(result) = result {
            assert!(result.rows.len() <= 3);
        }
    }

    #[test]
    fn test_call_dbms_procedures_yield_projection_and_limit() {
        let engine = make_engine();
        let parser = Parser::new();

        let result = engine
            .execute(
                &parser
                    .parse(
                        "CALL dbms.procedures() YIELD name, signature RETURN name, signature ORDER BY name ASC LIMIT 5",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["name", "signature"]);
        assert_eq!(result.rows.len(), 5);
        // Alphabetically sorted: db.constraints is first with the new entries.
        assert_eq!(
            result.rows[0].get("name"),
            Some(&Value::String("db.constraints".to_string()))
        );
    }

    #[test]
    fn test_call_db_constraints_yield_return_rows() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE CONSTRAINT person_email_unique FOR (n:Person) REQUIRE n.email IS UNIQUE")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("CALL db.constraints() YIELD name, type, labelsOrTypes, properties RETURN name, type ORDER BY name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["name", "type"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("name"),
            Some(&Value::String("person_email_unique".to_string()))
        );
        assert_eq!(
            result.rows[0].get("type"),
            Some(&Value::String("UNIQUENESS".to_string()))
        );
    }

    #[test]
    fn test_call_db_indexes_yield_return_rows() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE INDEX person_idx FOR (n:Person) ON (n.email)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("CALL db.indexes() YIELD name, type, state RETURN name, type, state ORDER BY name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["name", "type", "state"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("name"),
            Some(&Value::String("person_idx".to_string()))
        );
        assert_eq!(
            result.rows[0].get("type"),
            Some(&Value::String("RANGE".to_string()))
        );
        assert_eq!(
            result.rows[0].get("state"),
            Some(&Value::String("ONLINE".to_string()))
        );
    }

    #[test]
    fn test_call_db_ping_returns_success() {
        let engine = make_engine();
        let parser = Parser::new();

        let result = engine
            .execute(
                &parser.parse("CALL db.ping() YIELD success RETURN success").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["success"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("success"), Some(&Value::Bool(true)));
    }

    #[test]
    fn test_call_dbms_components_yields_edition() {
        let engine = make_engine();
        let parser = Parser::new();
        let result = engine
            .execute(
                &parser
                    .parse("CALL dbms.components() YIELD name, versions, edition RETURN name, versions, edition")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.columns, vec!["name", "versions", "edition"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("name"), Some(&Value::String("CopperDB".to_string())));
        assert_eq!(result.rows[0].get("edition"), Some(&Value::String("community".to_string())));
    }

    #[test]
    fn test_call_dbms_list_config_returns_rows() {
        let engine = make_engine();
        let parser = Parser::new();
        let result = engine
            .execute(
                &parser
                    .parse("CALL dbms.listConfig() YIELD name, value, dynamic RETURN name, value, dynamic ORDER BY name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.columns, vec!["name", "value", "dynamic"]);
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0].get("name"), Some(&Value::String("nornicdb.bolt.enabled".to_string())));
        assert_eq!(result.rows[0].get("value"), Some(&Value::Bool(true)));
    }

    #[test]
    fn test_call_fulltext_list_analyzers_returns_standard_set() {
        let engine = make_engine();
        let parser = Parser::new();
        let result = engine
            .execute(
                &parser
                    .parse("CALL db.index.fulltext.listAvailableAnalyzers() YIELD analyzer, description RETURN analyzer, description ORDER BY analyzer")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.columns, vec!["analyzer", "description"]);
        assert_eq!(result.rows.len(), 5);
        assert_eq!(result.rows[0].get("analyzer"), Some(&Value::String("keyword".to_string())));
        assert_eq!(result.rows[4].get("analyzer"), Some(&Value::String("whitespace".to_string())));
    }

    #[test]
    fn test_call_db_info_yields_counts() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("CREATE (:Person {name: 'Alice'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (:Person {name: 'Bob'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("CALL db.info() YIELD nodeCount, relationshipCount RETURN nodeCount, relationshipCount")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["nodeCount", "relationshipCount"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("nodeCount"), Some(&Value::from(2u64)));
        assert_eq!(
            result.rows[0].get("relationshipCount"),
            Some(&Value::from(0u64))
        );
    }

    #[test]
    fn test_call_nornicdb_version_yields_edition() {
        let engine = make_engine();
        let parser = Parser::new();

        let result = engine
            .execute(
                &parser
                    .parse("CALL nornicdb.version() YIELD version, edition RETURN version, edition")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["version", "edition"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("edition"),
            Some(&Value::String("community".to_string()))
        );
    }

    #[test]
    fn test_call_nornicdb_stats_yields_counts() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("CREATE (:Person {name: 'Alice'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (:Todo {task: 'Write code'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person), (b:Todo) CREATE (a)-[:ASSIGNED]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("CALL nornicdb.stats() YIELD nodes, relationships, labels, relationshipTypes RETURN nodes, relationships, labels, relationshipTypes")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(
            result.columns,
            vec!["nodes", "relationships", "labels", "relationshipTypes"]
        );
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("nodes"), Some(&Value::from(2u64)));
        assert_eq!(result.rows[0].get("relationships"), Some(&Value::from(1u64)));
        assert_eq!(result.rows[0].get("labels"), Some(&Value::from(2u64)));
        assert_eq!(
            result.rows[0].get("relationshipTypes"),
            Some(&Value::from(1u64))
        );
    }

    #[test]
    fn test_call_db_schema_node_properties_yields_label_property_pairs() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {name: 'Alice', age: 30})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("CALL db.schema.nodeProperties() YIELD nodeLabel, propertyName, propertyType RETURN nodeLabel, propertyName, propertyType ORDER BY nodeLabel, propertyName")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(
            result.columns,
            vec!["nodeLabel", "propertyName", "propertyType"]
        );
        assert_eq!(result.rows.len(), 2);
        assert_eq!(
            result.rows[0].get("nodeLabel"),
            Some(&Value::String("Person".to_string()))
        );
        assert_eq!(
            result.rows[0].get("propertyName"),
            Some(&Value::String("age".to_string()))
        );
        assert_eq!(
            result.rows[1].get("propertyName"),
            Some(&Value::String("name".to_string()))
        );
    }

    #[test]
    fn test_call_db_schema_rel_properties_yields_type_property_pairs() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("CREATE (:Person {id: 1})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (:Person {id: 2})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:KNOWS {since: 2024}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("CALL db.schema.relProperties() YIELD relType, propertyName, propertyType RETURN relType, propertyName, propertyType ORDER BY relType, propertyName")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(
            result.columns,
            vec!["relType", "propertyName", "propertyType"]
        );
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("relType"),
            Some(&Value::String("KNOWS".to_string()))
        );
        assert_eq!(
            result.rows[0].get("propertyName"),
            Some(&Value::String("since".to_string()))
        );
    }

    #[test]
    fn test_call_db_schema_visualization_yields_label_and_type_lists() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("CREATE (:Person {name: 'Alice'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (:Person {name: 'Bob'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("CALL db.schema.visualization() YIELD nodes, relationships RETURN nodes, relationships")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["nodes", "relationships"]);
        assert_eq!(result.rows.len(), 1);

        let nodes_val = result.rows[0].get("nodes").unwrap();
        let rels_val = result.rows[0].get("relationships").unwrap();

        assert!(nodes_val.is_array());
        assert!(rels_val.is_array());
    }

    #[test]
    fn test_call_dbms_functions_yields_all_filter_functions() {
        let engine = make_engine();
        let parser = Parser::new();

        let result = engine
            .execute(
                &parser
                    .parse("CALL dbms.functions() YIELD name, category RETURN name, category ORDER BY name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["name", "category"]);
        // Verify key functions exist with correct categories
        let functions: HashMap<&str, &str> = result
            .rows
            .iter()
            .map(|row| {
                (
                    row.get("name").and_then(Value::as_str).unwrap(),
                    row.get("category").and_then(Value::as_str).unwrap(),
                )
            })
            .collect();
        assert_eq!(functions.get("count"), Some(&"Aggregating"));
        assert_eq!(functions.get("id"), Some(&"Scalar"));
        assert_eq!(functions.get("elementId"), Some(&"Scalar"));
        assert_eq!(functions.get("labels"), Some(&"Scalar"));
        assert_eq!(functions.get("coalesce"), Some(&"Scalar"));
        assert_eq!(functions.get("toUpper"), Some(&"String"));
        assert_eq!(functions.get("trim"), Some(&"String"));
        assert_eq!(functions.get("contains"), Some(&"String"));
        assert_eq!(functions.get("abs"), Some(&"Numeric"));
        assert_eq!(functions.get("now"), Some(&"Temporal"));
        assert_eq!(functions.get("head"), Some(&"List"));
        assert!(functions.len() >= 30);
    }

    #[test]
    fn test_call_nornicdb_decay_info_reports_disabled_when_no_profiles() {
        let engine = make_engine();
        let parser = Parser::new();

        let result = engine
            .execute(
                &parser
                    .parse("CALL nornicdb.decay.info() YIELD enabled, system, configuredVia RETURN enabled, system, configuredVia")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["enabled", "system", "configuredVia"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("enabled"), Some(&Value::Bool(false)));
        assert!(result.rows[0]
            .get("system")
            .and_then(Value::as_str)
            .unwrap()
            .contains("knowledge-layer"));
    }

    #[test]
    fn test_call_nornicdb_decay_info_reports_enabled_when_profile_exists() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE slow_decay OPTIONS { halfLifeSeconds: 604800, visibilityThreshold: 0.1, scoreFloor: 0.0, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("CALL nornicdb.decay.info() YIELD enabled RETURN enabled")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("enabled"), Some(&Value::Bool(true)));
    }

    #[test]
    fn test_call_nornicdb_knowledgepolicy_info_reports_catalog_counts() {
        let engine = make_engine();
        let parser = Parser::new();

        // Create a decay profile and binding
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE slow_decay OPTIONS { halfLifeSeconds: 604800, visibilityThreshold: 0.1, scoreFloor: 0.0, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
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
        // Create a promotion profile and policy
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION PROFILE boost_profile OPTIONS { scope: 'NODE', multiplier: 1.5, scoreFloor: 0.0, scoreCap: 1.0, enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION POLICY fact_policy FOR (n:KnowledgeFact) APPLY PROFILE boost_profile WHEN 'n.evidence >= 3'",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("CALL nornicdb.knowledgepolicy.info() YIELD enabled, decayProfiles, decayBindings, promotionProfiles, promotionPolicies RETURN enabled, decayProfiles, decayBindings, promotionProfiles, promotionPolicies")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(
            result.columns,
            vec![
                "enabled",
                "decayProfiles",
                "decayBindings",
                "promotionProfiles",
                "promotionPolicies"
            ]
        );
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("enabled"), Some(&Value::Bool(true)));
        assert_eq!(
            result.rows[0].get("decayProfiles"),
            Some(&Value::from(1u64))
        );
        assert_eq!(
            result.rows[0].get("decayBindings"),
            Some(&Value::from(1u64))
        );
        assert_eq!(
            result.rows[0].get("promotionProfiles"),
            Some(&Value::from(1u64))
        );
        assert_eq!(
            result.rows[0].get("promotionPolicies"),
            Some(&Value::from(1u64))
        );
    }

    #[test]
    fn test_call_fulltext_query_nodes_accepts_third_options_map() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:Doc {content: 'alpha'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Doc {content: 'alpha beta gamma'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE FULLTEXT INDEX idx_ft FOR (n:Doc) ON (n.content)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // "alpha beta" scores 2 (matches "alpha" and "beta"), "alpha" scores 1.
        // ORDER BY score DESC → higher-scored doc first; {skip: 1, limit: 1} → lower-scored doc.
        let result = engine
            .execute(
                &parser
                    .parse(
                        "CALL db.index.fulltext.queryNodes('idx_ft', 'alpha beta', {skip: 1, limit: 1}) YIELD node, score RETURN node.content AS content, score",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["content", "score"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("content"),
            Some(&Value::String("alpha".to_string()))
        );
    }

    #[test]
    fn test_call_fulltext_query_nodes_rejects_non_map_third_arg() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("CREATE (:Doc {content: 'alpha first'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE FULLTEXT INDEX idx_ft FOR (n:Doc) ON (n.content)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let error = match engine.execute(
            &parser
                .parse("CALL db.index.fulltext.queryNodes('idx_ft', 'alpha', 5) YIELD node, score RETURN node, score")
                .unwrap(),
            &HashMap::new(),
        ) {
            Ok(_) => panic!("expected non-map third arg to fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("MAP"));
    }

    #[test]
    fn test_call_fulltext_query_nodes_node_search_alias_uses_declared_indexes() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("CREATE (:Doc {content: 'alpha first'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (:Doc {content: 'alpha second'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE FULLTEXT INDEX idx_ft FOR (n:Doc) ON (n.content)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse(
                        "CALL db.index.fulltext.queryNodes('node_search', 'alpha', {limit: 1}) YIELD node, score RETURN node.content AS content, score",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["content", "score"]);
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_call_fulltext_query_nodes_default_alias_uses_declared_indexes() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("CREATE (:Doc {content: 'alpha only'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE FULLTEXT INDEX idx_ft FOR (n:Doc) ON (n.content)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse(
                        "CALL db.index.fulltext.queryNodes('default', 'alpha') YIELD node, score RETURN node.content AS content, score",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["content", "score"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("content"),
            Some(&Value::String("alpha only".to_string()))
        );
    }

    #[test]
    fn test_call_nornicdb_knowledgepolicy_profiles_yields_bundles_and_bindings() {
        let engine = make_engine();
        let parser = Parser::new();

        // Create a decay profile bundle
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE slow_decay OPTIONS { halfLifeSeconds: 604800, visibilityThreshold: 0.1, scoreFloor: 0.05, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        // Create a decay binding referencing the bundle
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE memory_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE slow_decay, visibilityThreshold: 0.2, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("CALL nornicdb.knowledgepolicy.profiles() YIELD kind, Name, HalfLifeSeconds, Scope, Enabled, ProfileRef, NoDecay, Order RETURN kind, Name, HalfLifeSeconds, Scope, Enabled, ProfileRef, NoDecay, Order ORDER BY kind, Name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(
            result.columns,
            vec![
                "kind", "Name", "HalfLifeSeconds", "Scope", "Enabled",
                "ProfileRef", "NoDecay", "Order"
            ]
        );

        // Expect 2 rows: binding alphabetically before bundle
        assert_eq!(result.rows.len(), 2);

        // Binding row first (alphabetical)
        let binding = &result.rows[0];
        assert_eq!(
            binding.get("kind"),
            Some(&Value::String("binding".to_string()))
        );
        assert_eq!(
            binding.get("Name"),
            Some(&Value::String("memory_binding".to_string()))
        );
        assert_eq!(
            binding.get("HalfLifeSeconds"),
            Some(&Value::from(604800i64))
        );
        assert_eq!(binding.get("Enabled"), Some(&Value::Bool(true)));
        assert_eq!(
            binding.get("ProfileRef"),
            Some(&Value::String("slow_decay".to_string()))
        );
        assert_eq!(binding.get("NoDecay"), Some(&Value::Bool(false)));
        assert_eq!(binding.get("Order"), Some(&Value::from(10i64)));

        // Bundle row second
        let bundle = &result.rows[1];
        assert_eq!(bundle.get("kind"), Some(&Value::String("bundle".to_string())));
        assert_eq!(
            bundle.get("Name"),
            Some(&Value::String("slow_decay".to_string()))
        );
        assert_eq!(
            bundle.get("HalfLifeSeconds"),
            Some(&Value::from(604800i64))
        );
        assert_eq!(bundle.get("Scope"), Some(&Value::String("NODE".to_string())));
        assert_eq!(bundle.get("Enabled"), Some(&Value::Bool(true)));
        assert_eq!(bundle.get("ProfileRef"), Some(&Value::String("".to_string())));
        assert_eq!(bundle.get("NoDecay"), Some(&Value::Bool(false)));
        assert_eq!(bundle.get("Order"), Some(&Value::from(0i64)));
    }

    #[test]
    fn test_call_nornicdb_knowledgepolicy_policies_yields_profiles_and_policies() {
        let engine = make_engine();
        let parser = Parser::new();

        // Create a promotion profile
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION PROFILE boost_profile OPTIONS { scope: 'NODE', multiplier: 1.5, scoreFloor: 0.2, scoreCap: 1.0, enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        // Create a promotion policy referencing the profile
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION POLICY fact_policy FOR (n:KnowledgeFact) APPLY PROFILE boost_profile WHEN 'n.evidence >= 3'",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("CALL nornicdb.knowledgepolicy.policies() YIELD kind, Name, Scope, Multiplier, ScoreFloor, ScoreCap, Enabled, TargetLabels, IsWildcard, IsEdge RETURN kind, Name, Scope, Multiplier, ScoreFloor, ScoreCap, Enabled, TargetLabels, IsWildcard, IsEdge ORDER BY kind, Name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(
            result.columns,
            vec![
                "kind", "Name", "Scope", "Multiplier", "ScoreFloor",
                "ScoreCap", "Enabled", "TargetLabels", "IsWildcard", "IsEdge"
            ]
        );

        // Expect 2 rows: policy alphabetically before profile
        assert_eq!(result.rows.len(), 2);

        // Policy row first (alphabetical)
        let policy = &result.rows[0];
        assert_eq!(
            policy.get("kind"),
            Some(&Value::String("policy".to_string()))
        );
        assert_eq!(
            policy.get("Name"),
            Some(&Value::String("fact_policy".to_string()))
        );
        assert_eq!(
            policy.get("Scope"),
            Some(&Value::String("NODE".to_string()))
        );
        assert_eq!(policy.get("Multiplier"), Some(&Value::Null));
        assert_eq!(policy.get("ScoreFloor"), Some(&Value::Null));
        assert_eq!(policy.get("ScoreCap"), Some(&Value::Null));
        assert_eq!(policy.get("Enabled"), Some(&Value::Bool(true)));
        assert_eq!(policy.get("IsWildcard"), Some(&Value::Bool(false)));
        assert_eq!(policy.get("IsEdge"), Some(&Value::Bool(false)));
        assert_eq!(
            policy.get("TargetLabels"),
            Some(&Value::Array(vec![Value::String(
                "KnowledgeFact".to_string()
            )]))
        );

        // Profile row second
        let profile = &result.rows[1];
        assert_eq!(
            profile.get("kind"),
            Some(&Value::String("profile".to_string()))
        );
        assert_eq!(
            profile.get("Name"),
            Some(&Value::String("boost_profile".to_string()))
        );
        assert_eq!(
            profile.get("Scope"),
            Some(&Value::String("NODE".to_string()))
        );
        assert_eq!(profile.get("Multiplier"), Some(&Value::from(1.5)));
        assert_eq!(profile.get("ScoreFloor"), Some(&Value::from(0.2)));
        assert_eq!(profile.get("ScoreCap"), Some(&Value::from(1.0)));
        assert_eq!(profile.get("Enabled"), Some(&Value::Bool(true)));
        assert_eq!(profile.get("TargetLabels"), Some(&Value::Null));
    }
