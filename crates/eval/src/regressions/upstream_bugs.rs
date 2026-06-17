    // ─── Upstream bug regression mirrors ─────────────────────────────────

    /// Mirrors NornicDB `TestReturnAfterSet_ReturnsUpdatedValue`.
    #[test]
    fn test_return_after_set_returns_updated_value() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (s:Step {title: 'original', content: 'old content'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let result = engine
            .execute(
                &parser.parse("MATCH (s:Step {title: 'original'}) SET s.content = 'new content' RETURN s.title AS title").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.columns, vec!["title"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("title"), Some(&Value::String("original".to_string())));
        assert_eq!(result.stats.properties_set, 1);
    }

    /// Mirrors NornicDB `TestReturnAfterDetachDelete_ReturnsLiteral`.
    #[test]
    fn test_return_after_detach_delete_returns_literal() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (s:Step {title: 'to-delete'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let result = engine
            .execute(
                &parser.parse("MATCH (s:Step {title: 'to-delete'}) DETACH DELETE s RETURN 'done' AS result").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.columns, vec!["result"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("result"), Some(&Value::String("done".to_string())));
        assert_eq!(result.stats.nodes_deleted, 1);
    }

    /// Mirrors NornicDB `TestMultiHopOptionalMatch_ReturnsRowsForUnmatchedPattern`.
    /// ORDER BY resolves through RETURN aliases (Neo4j-compatible).
    #[test]
    fn test_multihop_optional_match_returns_rows_for_unmatched() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (p:Protocol {trigger: 'end session'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        for i in 1..=3 {
            engine
                .execute(
                    &parser.parse(&format!("MATCH (p:Protocol {{trigger: 'end session'}}) CREATE (p)-[:HAS_STEP]->(s:ProtocolStep {{title: 'step{i}'}})")).unwrap(),
                    &HashMap::new(),
                )
                .unwrap();
        }
        engine
            .execute(
                &parser.parse("MATCH (s1:ProtocolStep {title: 'step1'}) MATCH (s2:ProtocolStep {title: 'step2'}) CREATE (s1)-[:SUPERSEDES]->(s2)").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let result = engine
            .execute(
                &parser.parse("MATCH (p:Protocol {trigger: 'end session'})-[:HAS_STEP]->(s:ProtocolStep) OPTIONAL MATCH (s)-[:SUPERSEDES]->(newer:ProtocolStep) RETURN s.title AS title, newer.title AS newer_title ORDER BY title").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.columns, vec!["title", "newer_title"]);
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0].get("title"), Some(&Value::String("step1".to_string())));
        assert_eq!(result.rows[0].get("newer_title"), Some(&Value::String("step2".to_string())));
        assert_eq!(result.rows[1].get("title"), Some(&Value::String("step2".to_string())));
        assert_eq!(result.rows[1].get("newer_title"), Some(&Value::Null));
        assert_eq!(result.rows[2].get("title"), Some(&Value::String("step3".to_string())));
        assert_eq!(result.rows[2].get("newer_title"), Some(&Value::Null));
    }

    /// Same as above but ORDER BY references the pre-projection variable form.
    #[test]
    fn test_multihop_optional_match_order_by_variable_reference() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (p:Protocol {trigger: 'end session'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        for i in 1..=3 {
            engine
                .execute(
                    &parser.parse(&format!("MATCH (p:Protocol {{trigger: 'end session'}}) CREATE (p)-[:HAS_STEP]->(s:ProtocolStep {{title: 'step{i}'}})")).unwrap(),
                    &HashMap::new(),
                )
                .unwrap();
        }
        engine
            .execute(
                &parser.parse("MATCH (s1:ProtocolStep {title: 'step1'}) MATCH (s2:ProtocolStep {title: 'step2'}) CREATE (s1)-[:SUPERSEDES]->(s2)").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let result = engine
            .execute(
                &parser.parse("MATCH (p:Protocol {trigger: 'end session'})-[:HAS_STEP]->(s:ProtocolStep) OPTIONAL MATCH (s)-[:SUPERSEDES]->(newer:ProtocolStep) RETURN s.title AS title, newer.title AS newer_title ORDER BY s.title").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.columns, vec!["title", "newer_title"]);
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0].get("title"), Some(&Value::String("step1".to_string())));
        assert_eq!(result.rows[0].get("newer_title"), Some(&Value::String("step2".to_string())));
        assert_eq!(result.rows[1].get("title"), Some(&Value::String("step2".to_string())));
        assert_eq!(result.rows[1].get("newer_title"), Some(&Value::Null));
        assert_eq!(result.rows[2].get("title"), Some(&Value::String("step3".to_string())));
        assert_eq!(result.rows[2].get("newer_title"), Some(&Value::Null));
    }

    /// Diagnostic: single MERGE with UNWIND inline list, no relationship MERGE.
    #[test]
    fn test_unwind_merge_single_node_inline_list() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (n:Node {id: 'anchor'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (a:Node {id: 'anchor'}) UNWIND ['aa', 'bb'] AS val MERGE (p:Node {id: val}) RETURN p.id AS linked ORDER BY linked")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("linked"), Some(&Value::String("aa".to_string())));
        assert_eq!(result.rows[1].get("linked"), Some(&Value::String("bb".to_string())));
    }

    /// Diagnostic: dual MERGE without ORDER BY to isolate sort issue.
    #[test]
    fn test_unwind_merge_dual_no_order() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (a:Node {id: 'anchor'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (a:Node {id: 'anchor'}) UNWIND ['aa', 'bb'] AS val MERGE (p:Node {id: val}) MERGE (a)-[:LINKS]->(p) RETURN p.id AS linked")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 2, "expected 2 rows, got {:?}", result.rows.iter().map(|r| r.get("linked")).collect::<Vec<_>>());
        let mut linked: Vec<String> = result.rows.iter().map(|r| r.get("linked").and_then(Value::as_str).unwrap().to_string()).collect();
        linked.sort();
        assert_eq!(linked, vec!["aa".to_string(), "bb".to_string()]);
    }

    /// Mirrors NornicDB `TestBug_MatchUnwindParamMerge_ExpandsListValues`.
    #[test]
    fn test_match_unwind_param_merge_expands_list_values() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (n:Node {id: 'test-node'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let mut params = HashMap::new();
        params.insert("anchor".to_string(), Value::String("test-node".to_string()));
        params.insert("names".to_string(), Value::Array(vec![Value::String("proj-a".to_string()), Value::String("proj-b".to_string())]));
        let result = engine
            .execute(
                &parser.parse("MATCH (anchor:Node {id: $anchor}) UNWIND $names AS name MERGE (p:Node {id: name}) MERGE (anchor)-[:LINKS]->(p) RETURN p.id AS linked").unwrap(),
                &params,
            )
            .unwrap();
        assert_eq!(result.columns, vec!["linked"]);
        assert_eq!(result.rows.len(), 2);
        let mut linked: Vec<String> = result.rows.iter().map(|r| r.get("linked").and_then(Value::as_str).unwrap().to_string()).collect();
        linked.sort();
        assert_eq!(linked, vec!["proj-a".to_string(), "proj-b".to_string()]);
    }

    /// Mirrors NornicDB `TestMatchUnwindMerge_InlineList`.
    /// TODO: same pipeline UNWIND+MERGE variable bug as above.
    #[test]
    fn test_match_unwind_merge_inline_list() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (n:Node {id: 'root'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let result = engine
            .execute(
                &parser.parse("MATCH (anchor:Node {id: 'root'}) UNWIND ['x', 'y', 'z'] AS name MERGE (p:Node {id: name}) MERGE (anchor)-[:LINKS]->(p) RETURN p.id AS linked").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.columns, vec!["linked"]);
        assert_eq!(result.rows.len(), 3);
        let mut linked: Vec<String> = result.rows.iter().map(|r| r.get("linked").and_then(Value::as_str).unwrap().to_string()).collect();
        linked.sort();
        assert_eq!(linked, vec!["x".to_string(), "y".to_string(), "z".to_string()]);
    }

    /// Mirrors NornicDB `TestMatchUnwindMerge_EmptyList`.
    #[test]
    fn test_match_unwind_merge_empty_list() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (n:Node {id: 'anchor'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let mut params = HashMap::new();
        params.insert("anchor".to_string(), Value::String("anchor".to_string()));
        params.insert("names".to_string(), Value::Array(vec![]));
        let result = engine
            .execute(
                &parser.parse("MATCH (anchor:Node {id: $anchor}) UNWIND $names AS name MERGE (p:Node {id: name}) RETURN p.id AS linked").unwrap(),
                &params,
            )
            .unwrap();
        assert_eq!(result.rows.len(), 0);
    }

    /// Mirrors NornicDB `TestMatchUnwindMerge_NoMatchProducesNoRows`.
    #[test]
    fn test_match_unwind_merge_no_match_produces_no_rows() {
        let engine = make_engine();
        let parser = Parser::new();
        let result = engine
            .execute(
                &parser.parse("MATCH (anchor:Node {id: 'nonexistent'}) UNWIND ['a'] AS name MERGE (p:Node {id: name}) RETURN p.id AS linked").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 0);
    }

    // ─── match_comma_create regressions ─────────────────────────────────

    /// Mirrors NornicDB `TestRegression191Followup_MatchCommaCreateRelationship`.
    #[test]
    fn test_match_comma_create_where_not_edge() {
        let engine = make_engine();
        let parser = Parser::new();
        engine.execute(&parser.parse("CREATE (t:Task {project:'dimension'})").unwrap(), &HashMap::new()).unwrap();
        engine.execute(&parser.parse("CREATE (s:Session {id:'sid2026'})").unwrap(), &HashMap::new()).unwrap();
        let result = engine
            .execute(
                &parser.parse("MATCH (t:Task {project:'dimension'}), (s:Session {id:'sid2026'}) WHERE NOT (t)-[:RAISED_IN]->(s) CREATE (t)-[:RAISED_IN]->(s) RETURN count(*) AS edges_added").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("edges_added"), Some(&Value::from(1)));
        assert_eq!(result.stats.relationships_created, 1);
        let verify = engine.execute(&parser.parse("MATCH (t:Task)-[r:RAISED_IN]->(s:Session) RETURN count(r) AS c").unwrap(), &HashMap::new()).unwrap();
        assert_eq!(verify.rows[0].get("c"), Some(&Value::from(1)));
    }

    /// Mirrors NornicDB `TestRegression191Followup_MatchCreateNodeToMatchedNode`.
    #[test]
    fn test_match_create_node_to_matched_node() {
        let engine = make_engine();
        let parser = Parser::new();
        engine.execute(&parser.parse("CREATE (s:Session {id:'sid'})").unwrap(), &HashMap::new()).unwrap();
        let result = engine
            .execute(
                &parser.parse("MATCH (s:Session {id:'sid'}) CREATE (n:Foo)-[:RAISED_IN]->(s) RETURN n").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.stats.nodes_created, 1);
        assert_eq!(result.stats.relationships_created, 1);
        let verify_nodes = engine.execute(&parser.parse("MATCH (n:Foo) RETURN count(n)").unwrap(), &HashMap::new()).unwrap();
        assert_eq!(verify_nodes.rows[0].get("count(n)"), Some(&Value::from(1)));
        let verify_rels = engine.execute(&parser.parse("MATCH (:Foo)-[r:RAISED_IN]->(:Session) RETURN count(r)").unwrap(), &HashMap::new()).unwrap();
        assert_eq!(verify_rels.rows[0].get("count(r)"), Some(&Value::from(1)));
    }

    /// Mirrors NornicDB `TestRegression191Followup_SetSelfStringConcat`.
    #[test]
    fn test_set_self_string_concat() {
        let engine = make_engine();
        let parser = Parser::new();
        engine.execute(&parser.parse("CREATE (h:Heuristic {content:'hello'})").unwrap(), &HashMap::new()).unwrap();
        engine.execute(&parser.parse("MATCH (h:Heuristic) WHERE h.content = 'hello' SET h.content = h.content + ', world'").unwrap(), &HashMap::new()).unwrap();
        let got = engine.execute(&parser.parse("MATCH (h:Heuristic) RETURN h.content").unwrap(), &HashMap::new()).unwrap();
        assert_eq!(got.rows.len(), 1);
        assert_eq!(got.rows[0].get("h.content"), Some(&Value::String("hello, world".to_string())));
    }

    /// Mirrors NornicDB `TestBug1_SetMapReplacementDropsProperties`.
    #[test]
    fn test_set_map_replacement_preserves_properties() {
        let engine = make_engine();
        let parser = Parser::new();
        let mut params = HashMap::new();
        params.insert("uuid".to_string(), Value::String("n1".to_string()));
        params.insert("d".to_string(), serde_json::json!({"uuid":"n1","name":"Ada","group_id":"g"}));
        let result = engine
            .execute(
                &parser.parse("MERGE (n:Bug {uuid: $uuid}) SET n = $d RETURN properties(n) AS p").unwrap(),
                &params,
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        let p = result.rows[0].get("p").and_then(Value::as_object).unwrap();
        assert_eq!(p.get("uuid"), Some(&Value::String("n1".to_string())));
        assert_eq!(p.get("name"), Some(&Value::String("Ada".to_string())));
        assert_eq!(p.get("group_id"), Some(&Value::String("g".to_string())));
    }

    /// Mirrors NornicDB `TestParamMapPropertyAccess_ReturnShapes`.
    #[test]
    fn test_param_map_property_access_return() {
        let engine = make_engine();
        let parser = Parser::new();
        let mut params = HashMap::new();
        params.insert(
            "d".to_string(),
            serde_json::json!({"uuid": "k", "name": "Ada"}),
        );
        let result = engine
            .execute(
                &parser.parse("RETURN $d.uuid AS uuid").unwrap(),
                &params,
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("uuid"),
            Some(&Value::String("k".to_string()))
        );
    }

    /// Mirrors NornicDB `TestBug1_SetMapReplacementDropsProperties` — MERGE with $d.uuid.
    #[test]
    fn test_param_property_access_in_merge() {
        let engine = make_engine();
        let parser = Parser::new();
        let mut params = HashMap::new();
        params.insert(
            "d".to_string(),
            serde_json::json!({"uuid": "n1", "name": "Ada", "group_id": "g"}),
        );
        let result = engine
            .execute(
                &parser
                    .parse(
                        "MERGE (n:Bug {uuid: $d.uuid}) SET n = $d RETURN properties(n) AS p",
                    )
                    .unwrap(),
                &params,
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        let p = result.rows[0]
            .get("p")
            .and_then(Value::as_object)
            .unwrap();
        assert_eq!(
            p.get("uuid"),
            Some(&Value::String("n1".to_string()))
        );
        assert_eq!(p.get("name"), Some(&Value::String("Ada".to_string())));
        assert_eq!(
            p.get("group_id"),
            Some(&Value::String("g".to_string()))
        );
    }

    /// Mirrors NornicDB `TestBug1_ChainedSetLabelAndMapPreservesPropertiesAndLabels` — label first.
    #[test]
    fn test_chained_set_label_then_map() {
        let engine = make_engine();
        let parser = Parser::new();
        let key = "chained_label_then_map";
        let mut params = HashMap::new();
        params.insert(
            "d".to_string(),
            serde_json::json!({
                "uuid": key,
                "name": "Ada",
                "group_id": "g"
            }),
        );
        let result = engine
            .execute(
                &parser
                    .parse(&format!(
                        "MERGE (n:M {{uuid: $d.uuid}}) SET n:Extra SET n = $d RETURN properties(n) AS p, labels(n) AS labels"
                    ))
                    .unwrap(),
                &params,
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        let p = result.rows[0]
            .get("p")
            .and_then(Value::as_object)
            .unwrap();
        assert_eq!(
            p.get("uuid"),
            Some(&Value::String(key.to_string()))
        );
        let labels = result.rows[0]
            .get("labels")
            .and_then(Value::as_array)
            .unwrap();
        let label_strs: Vec<&str> = labels.iter().filter_map(Value::as_str).collect();
        assert!(label_strs.contains(&"M"), "labels must contain M");
        assert!(label_strs.contains(&"Extra"), "labels must contain Extra");
    }

    /// Mirrors NornicDB `TestBug1_ChainedSetLabelAndMapPreservesPropertiesAndLabels` — map first.
    #[test]
    fn test_chained_set_map_then_label() {
        let engine = make_engine();
        let parser = Parser::new();
        let key = "chained_map_then_label";
        let mut params = HashMap::new();
        params.insert(
            "d".to_string(),
            serde_json::json!({
                "uuid": key,
                "name": "Ada",
                "group_id": "g"
            }),
        );
        let result = engine
            .execute(
                &parser
                    .parse(&format!(
                        "MERGE (n:M {{uuid: $d.uuid}}) SET n = $d SET n:Extra RETURN properties(n) AS p, labels(n) AS labels"
                    ))
                    .unwrap(),
                &params,
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        let labels = result.rows[0]
            .get("labels")
            .and_then(Value::as_array)
            .unwrap();
        let label_strs: Vec<&str> = labels.iter().filter_map(Value::as_str).collect();
        assert!(label_strs.contains(&"M"), "labels must contain M");
        assert!(label_strs.contains(&"Extra"), "labels must contain Extra");
    }

    /// Mirrors NornicDB `TestBug1_ChainedSetLabelAndMapPreservesPropertiesAndLabels` — dynamic label.
    #[test]
    fn test_chained_set_dynamic_label_then_map() {
        let engine = make_engine();
        let parser = Parser::new();
        let key = "dynamic_label_then_map";
        let mut params = HashMap::new();
        params.insert(
            "d".to_string(),
            serde_json::json!({
                "uuid": key,
                "name": "Ada",
                "group_id": "g",
                "labels": ["Extra"]
            }),
        );
        let result = engine
            .execute(
                &parser
                    .parse(&format!(
                        "MERGE (n:M {{uuid: $d.uuid}}) SET n:$(d.labels) SET n = $d RETURN properties(n) AS p, labels(n) AS labels"
                    ))
                    .unwrap(),
                &params,
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        let p = result.rows[0]
            .get("p")
            .and_then(Value::as_object)
            .unwrap();
        assert_eq!(
            p.get("uuid"),
            Some(&Value::String(key.to_string()))
        );
        let labels = result.rows[0]
            .get("labels")
            .and_then(Value::as_array)
            .unwrap();
        let label_strs: Vec<&str> = labels.iter().filter_map(Value::as_str).collect();
        assert!(label_strs.contains(&"M"), "labels must contain M");
        assert!(label_strs.contains(&"Extra"), "labels must contain Extra");
    }

    /// Mirrors NornicDB `TestCaseExpressionSearched` — searched CASE WHEN.
    #[test]
    fn test_case_expression_searched() {
        let engine = make_engine();
        let parser = Parser::new();
        for (name, age) in [("Alice", 15), ("Bob", 30), ("Charlie", 70), ("Diana", 25)] {
            engine
                .execute(
                    &parser
                        .parse(&format!(
                            "CREATE (p:Person {{name: '{name}', age: {age}}})"
                        ))
                        .unwrap(),
                    &HashMap::new(),
                )
                .unwrap();
        }
        let result = engine
            .execute(
                &parser
                    .parse(
                        "MATCH (p:Person) RETURN p.name, CASE WHEN p.age < 18 THEN 'minor' WHEN p.age < 65 THEN 'adult' ELSE 'senior' END AS ageGroup",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 4);
        let expected: std::collections::HashMap<&str, &str> = [
            ("Alice", "minor"),
            ("Bob", "adult"),
            ("Charlie", "senior"),
            ("Diana", "adult"),
        ]
        .iter()
        .cloned()
        .collect();
        for row in &result.rows {
            let name = row.get("p.name").and_then(Value::as_str).unwrap();
            let age_group = row.get("ageGroup").and_then(Value::as_str).unwrap();
            assert_eq!(age_group, expected[name]);
        }
    }

    /// Mirrors NornicDB `TestCaseExpressionSearched` — CASE without ELSE returns null.
    #[test]
    fn test_case_expression_no_else() {
        let engine = make_engine();
        let parser = Parser::new();
        for (name, age) in [("Alice", 15), ("Bob", 30), ("Charlie", 70)] {
            engine
                .execute(
                    &parser
                        .parse(&format!(
                            "CREATE (p:Person {{name: '{name}', age: {age}}})"
                        ))
                        .unwrap(),
                    &HashMap::new(),
                )
                .unwrap();
        }
        let result = engine
            .execute(
                &parser
                    .parse(
                        "MATCH (p:Person) RETURN p.name, CASE WHEN p.age < 18 THEN 'minor' WHEN p.age >= 65 THEN 'senior' END AS category",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        for row in &result.rows {
            let name = row.get("p.name").and_then(Value::as_str).unwrap();
            let cat = row.get("category");
            if name == "Bob" {
                assert!(cat.map_or(true, |v| v.is_null()), "Bob should be null");
            } else {
                assert!(cat.and_then(Value::as_str).is_some(), "{name} should have category");
            }
        }
    }

    /// Mirrors NornicDB `TestCaseExpressionSimple` — simple CASE expr WHEN val.
    #[test]
    fn test_case_expression_simple() {
        let engine = make_engine();
        let parser = Parser::new();
        for (name, status) in [
            ("Task1", "pending"),
            ("Task2", "active"),
            ("Task3", "done"),
            ("Task4", "cancelled"),
        ] {
            engine
                .execute(
                    &parser
                        .parse(&format!(
                            "CREATE (t:Task {{name: '{name}', status: '{status}'}})"
                        ))
                        .unwrap(),
                    &HashMap::new(),
                )
                .unwrap();
        }
        let result = engine
            .execute(
                &parser
                    .parse(
                        "MATCH (t:Task) RETURN t.name, CASE t.status WHEN 'pending' THEN 'Not Started' WHEN 'active' THEN 'In Progress' WHEN 'done' THEN 'Completed' ELSE 'Unknown' END AS statusLabel",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 4);
        let expected: std::collections::HashMap<&str, &str> = [
            ("Task1", "Not Started"),
            ("Task2", "In Progress"),
            ("Task3", "Completed"),
            ("Task4", "Unknown"),
        ]
        .iter()
        .cloned()
        .collect();
        for row in &result.rows {
            let name = row.get("t.name").and_then(Value::as_str).unwrap();
            let label = row.get("statusLabel").and_then(Value::as_str).unwrap();
            assert_eq!(label, expected[name]);
        }
    }

    /// Mirrors upstream CASE-in-WITH usage.
    #[test]
    fn test_case_expression_in_with() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser
                    .parse("CREATE (n:Val {value: 10})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (n:Val {value: 3})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let result = engine
            .execute(
                &parser
                    .parse(
                        "MATCH (n:Val) WITH n, CASE WHEN n.value > 5 THEN 1 ELSE 0 END AS hasValue RETURN hasValue",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let values: Vec<i64> = result
            .rows
            .iter()
            .filter_map(|r| r.get("hasValue").and_then(Value::as_i64))
            .collect();
        assert!(values.contains(&1), "should have value 1 for >5");
        assert!(values.contains(&0), "should have value 0 for <=5");
    }

    /// Mirrors upstream CASE with IS NOT NULL.
    #[test]
    fn test_case_expression_is_not_null() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser
                    .parse("CREATE (n:Node {name: 'hasValue'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (n:Node)").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let result = engine
            .execute(
                &parser
                    .parse(
                        "MATCH (n:Node) RETURN n.name, CASE WHEN n.name IS NOT NULL THEN 1 ELSE 0 END AS hasName",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        for row in &result.rows {
            let name = row.get("n.name").and_then(Value::as_str);
            let has = row.get("hasName").and_then(Value::as_i64).unwrap();
            if name.is_some() {
                assert_eq!(has, 1);
            } else {
                assert_eq!(has, 0);
            }
        }
    }

    /// Tests list functions: range, head, tail, last, reverse.
    #[test]
    fn test_list_functions() {
        let engine = make_engine();
        let parser = Parser::new();

        let result = engine
            .execute(&parser.parse("RETURN range(1, 5) AS r").unwrap(), &HashMap::new())
            .unwrap();
        let arr = result.rows[0].get("r").and_then(Value::as_array).unwrap();
        assert_eq!(arr.len(), 5);

        let result = engine
            .execute(&parser.parse("RETURN head([1, 2, 3]) AS h").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(result.rows[0].get("h"), Some(&Value::Number(1.into())));

        let result = engine
            .execute(&parser.parse("RETURN tail([1, 2, 3]) AS t").unwrap(), &HashMap::new())
            .unwrap();
        let arr = result.rows[0].get("t").and_then(Value::as_array).unwrap();
        assert_eq!(arr, &vec![Value::Number(2.into()), Value::Number(3.into())]);

        let result = engine
            .execute(&parser.parse("RETURN last([1, 2, 3]) AS l").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(result.rows[0].get("l"), Some(&Value::Number(3.into())));

        let result = engine
            .execute(&parser.parse("RETURN reverse([1, 2, 3]) AS r").unwrap(), &HashMap::new())
            .unwrap();
        let arr = result.rows[0].get("r").and_then(Value::as_array).unwrap();
        assert_eq!(arr, &vec![Value::Number(3.into()), Value::Number(2.into()), Value::Number(1.into())]);
    }

    /// Tests math functions: abs, sign, sqrt, pi.
    #[test]
    fn test_math_functions() {
        let engine = make_engine();
        let parser = Parser::new();

        let result = engine
            .execute(&parser.parse("RETURN abs(-5) AS a").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(result.rows[0].get("a"), Some(&Value::Number(5.into())));

        let result = engine
            .execute(&parser.parse("RETURN sign(-10) AS s").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(result.rows[0].get("s"), Some(&Value::Number((-1).into())));

        let result = engine
            .execute(&parser.parse("RETURN sqrt(16) AS s").unwrap(), &HashMap::new())
            .unwrap();
        let sqrt = result.rows[0].get("s").and_then(Value::as_f64).unwrap();
        assert!((sqrt - 4.0).abs() < 0.001);

        let result = engine
            .execute(&parser.parse("RETURN pi() AS p").unwrap(), &HashMap::new())
            .unwrap();
        let pi = result.rows[0].get("p").and_then(Value::as_f64).unwrap();
        assert!((pi - std::f64::consts::PI).abs() < 0.001);
    }

    /// Tests predicate functions: all, any, none, single.
    #[test]
    fn test_predicate_functions() {
        let engine = make_engine();
        let parser = Parser::new();

        let result = engine
            .execute(&parser.parse("RETURN none([]) AS n").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(result.rows[0].get("n"), Some(&Value::Bool(true)));

        let result = engine
            .execute(&parser.parse("RETURN single([1]) AS s").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(result.rows[0].get("s"), Some(&Value::Bool(true)));

        let result = engine
            .execute(&parser.parse("RETURN single([1, 2]) AS s").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(result.rows[0].get("s"), Some(&Value::Bool(false)));
    }

    /// Tests arithmetic operators: multiply, divide, modulo, XOR.
    #[test]
    fn test_arithmetic_operators() {
        let engine = make_engine();
        let parser = Parser::new();

        let result = engine
            .execute(&parser.parse("RETURN 3 * 4 AS m").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(result.rows[0].get("m"), Some(&Value::Number(12.into())));

        let result = engine
            .execute(&parser.parse("RETURN 10 / 3 AS d").unwrap(), &HashMap::new())
            .unwrap();
        let d = result.rows[0].get("d").and_then(Value::as_f64).unwrap();
        assert!((d - 3.333).abs() < 0.01);

        let result = engine
            .execute(&parser.parse("RETURN 10 % 3 AS m").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(result.rows[0].get("m"), Some(&Value::Number(1.into())));

        // XOR
        let result = engine
            .execute(
                &parser.parse("RETURN true XOR false AS x").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("x"), Some(&Value::Bool(true)));

        let result = engine
            .execute(
                &parser.parse("RETURN true XOR true AS x").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("x"), Some(&Value::Bool(false)));
    }

    /// Tests trig functions: sin, cos, tan.
    #[test]
    fn test_trig_functions() {
        let engine = make_engine();
        let parser = Parser::new();

        let result = engine
            .execute(&parser.parse("RETURN sin(0) AS s").unwrap(), &HashMap::new())
            .unwrap();
        let s = result.rows[0].get("s").and_then(Value::as_f64).unwrap();
        assert!(s.abs() < 0.001, "sin(0) should be 0");

        let result = engine
            .execute(&parser.parse("RETURN cos(0) AS c").unwrap(), &HashMap::new())
            .unwrap();
        let c = result.rows[0].get("c").and_then(Value::as_f64).unwrap();
        assert!((c - 1.0).abs() < 0.001, "cos(0) should be 1");
    }

    /// Tests power and log functions.
    #[test]
    fn test_power_functions() {
        let engine = make_engine();
        let parser = Parser::new();

        let result = engine
            .execute(&parser.parse("RETURN pow(2, 3) AS p").unwrap(), &HashMap::new())
            .unwrap();
        let p = result.rows[0].get("p").and_then(Value::as_f64).unwrap();
        assert!((p - 8.0).abs() < 0.001, "2^3 should be 8");

        let result = engine
            .execute(&parser.parse("RETURN exp(1) AS e").unwrap(), &HashMap::new())
            .unwrap();
        let e = result.rows[0].get("e").and_then(Value::as_f64).unwrap();
        assert!((e - std::f64::consts::E).abs() < 0.001, "exp(1) should be e");
    }

    /// Tests utility functions: randomUUID, toBoolean, isEmpty.
    #[test]
    fn test_utility_functions() {
        let engine = make_engine();
        let parser = Parser::new();

        let result = engine
            .execute(
                &parser.parse("RETURN randomUUID() AS u").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let u = result.rows[0].get("u").and_then(Value::as_str).unwrap();
        assert_eq!(u.len(), 36); // UUID v4 format

        let result = engine
            .execute(
                &parser.parse("RETURN toBoolean('true') AS b").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("b"), Some(&Value::Bool(true)));

        let result = engine
            .execute(
                &parser.parse("RETURN isEmpty([]) AS e").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("e"), Some(&Value::Bool(true)));

        let result = engine
            .execute(
                &parser.parse("RETURN isEmpty('hi') AS e").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("e"), Some(&Value::Bool(false)));
    }

    /// Tests null-safe, type-checking, and string info functions.
    #[test]
    fn test_null_safe_and_type_functions() {
        let engine = make_engine();
        let parser = Parser::new();

        let result = engine
            .execute(&parser.parse("RETURN nullIf('a', 'b') AS n").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(result.rows[0].get("n"), Some(&Value::String("a".into())));

        let result = engine
            .execute(&parser.parse("RETURN nullIf('a', 'a') AS n").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(result.rows[0].get("n"), Some(&Value::Null));

        let result = engine
            .execute(&parser.parse("RETURN valueType(42) AS v").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(result.rows[0].get("v"), Some(&Value::String("INTEGER".into())));

        let result = engine
            .execute(&parser.parse("RETURN valueType('hi') AS v").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(result.rows[0].get("v"), Some(&Value::String("STRING".into())));

        let result = engine
            .execute(&parser.parse("RETURN char_length('hello') AS c").unwrap(), &HashMap::new())
            .unwrap();
        assert_eq!(result.rows[0].get("c"), Some(&Value::Number(5.into())));

        let result = engine
            .execute(&parser.parse("RETURN e() AS e").unwrap(), &HashMap::new())
            .unwrap();
        let e = result.rows[0].get("e").and_then(Value::as_f64).unwrap();
        assert!((e - std::f64::consts::E).abs() < 0.001);
    }

    /// Tests list comprehension: [x IN list | expr] and [x IN list WHERE pred | expr].
    #[test]
    fn test_list_comprehension() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (n:Test {nums: [1, 2, 3]})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Test) RETURN [x IN n.nums | x * 2] AS doubled")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let arr = result.rows[0].get("doubled").and_then(Value::as_array).unwrap();
        assert_eq!(arr.len(), 3);

        // With WHERE predicate
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Test) RETURN [x IN n.nums WHERE x > 1 | x] AS filtered")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let arr = result.rows[0].get("filtered").and_then(Value::as_array).unwrap();
        assert_eq!(arr.len(), 2);
    }

    /// Tests slice() and indexOf().
    #[test]
    fn test_slice_and_indexof() {
        let engine = make_engine();
        let parser = Parser::new();

        let result = engine
            .execute(
                &parser.parse("RETURN slice([1, 2, 3, 4, 5], 1, 3) AS s").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let arr = result.rows[0].get("s").and_then(Value::as_array).unwrap();
        assert_eq!(arr.len(), 2);

        let result = engine
            .execute(
                &parser.parse("RETURN indexOf([10, 20, 30], 20) AS i").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("i"), Some(&Value::Number(1.into())));

        let result = engine
            .execute(
                &parser.parse("RETURN indexOf([10, 20, 30], 99) AS i").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("i"), Some(&Value::Number((-1).into())));
    }

    /// Tests REDUCE function: reduce(acc = init, var IN list | expr).
    #[test]
    fn test_reduce_function() {
        let engine = make_engine();
        let parser = Parser::new();

        let result = engine
            .execute(
                &parser.parse("RETURN reduce(acc = 0, x IN [1, 2, 3] | acc + x) AS total").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("total"), Some(&Value::Number(6.into())));

        let result = engine
            .execute(
                &parser.parse("RETURN reduce(s = '', w IN ['a', 'b', 'c'] | s + w) AS concat").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("concat"), Some(&Value::String("abc".into())));

        let result = engine
            .execute(
                &parser.parse("RETURN reduce(acc = 1, x IN [1, 2, 3] | acc * x) AS product").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("product"), Some(&Value::Number(6.into())));
    }

    /// Tests SET n += map augmented assignment.
    #[test]
    fn test_set_map_merge() {
        let engine = make_engine();
        let parser = Parser::new();
        let mut params = HashMap::new();
        params.insert(
            "extra".to_string(),
            serde_json::json!({"age": 30, "city": "NYC"}),
        );
        engine
            .execute(
                &parser
                    .parse("CREATE (n:Person {name: 'Alice', age: 25})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person) SET n += $extra RETURN properties(n) AS p")
                    .unwrap(),
                &params,
            )
            .unwrap();
        let p = result.rows[0].get("p").and_then(Value::as_object).unwrap();
        assert_eq!(p.get("name"), Some(&Value::String("Alice".into())));
        // age should be overwritten
        assert_eq!(p.get("age"), Some(&Value::Number(30.into())));
        assert_eq!(p.get("city"), Some(&Value::String("NYC".into())));
    }

    /// Tests BETWEEN operator.
    #[test]
    fn test_between_operator() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (n:Person {age: 25})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("RETURN 5 BETWEEN 1 AND 10 AS b")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("b"), Some(&Value::Bool(true)));

        let result = engine
            .execute(
                &parser
                    .parse("RETURN 15 BETWEEN 1 AND 10 AS b")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("b"), Some(&Value::Bool(false)));

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person) RETURN n.age BETWEEN 18 AND 30 AS b")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("b"), Some(&Value::Bool(true)));
    }

    /// Tests vector similarity functions.
    #[test]
    fn test_vector_similarity() {
        let engine = make_engine();
        let parser = Parser::new();

        // Cosine of identical vectors = 1.0
        let result = engine
            .execute(
                &parser
                    .parse("RETURN vector.similarity.cosine([1,0,0], [1,0,0]) AS c")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let c = result.rows[0].get("c").and_then(Value::as_f64).unwrap();
        assert!((c - 1.0).abs() < 0.001, "identical vectors should have cosine 1.0");

        // Cosine of orthogonal vectors = 0.0
        let result = engine
            .execute(
                &parser
                    .parse("RETURN vector.similarity.cosine([1,0], [0,1]) AS c")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let c = result.rows[0].get("c").and_then(Value::as_f64).unwrap();
        assert!(c.abs() < 0.001, "orthogonal vectors should have cosine ~0");

        // Euclidean of identical vectors
        let result = engine
            .execute(
                &parser
                    .parse("RETURN vector.similarity.euclidean([1,0,0], [1,0,0]) AS e")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let e = result.rows[0].get("e").and_then(Value::as_f64).unwrap();
        assert!((e - 1.0).abs() < 0.001, "identical vectors should have euclidean 1.0");
    }

    /// Tests vector search hot path: CREATE VECTOR INDEX + CALL db.index.vector.queryNodes.
    #[test]
    fn test_vector_search_index_hot_path() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE VECTOR INDEX vec_idx FOR (n:Doc) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .execute(
                &parser.parse("CREATE (a:Doc {name: 'a', emb: [1.0, 0.0, 0.0]})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (c:Doc {name: 'c', emb: [1.0, 0.0, 0.0]})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("CALL db.index.vector.queryNodes('vec_idx', 10, [1.0, 0.0, 0.0]) YIELD node, score RETURN node.name AS name, score ORDER BY score DESC")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert!(!result.rows.is_empty(), "vector search should return results");
        let top_score = result.rows[0].get("score").and_then(Value::as_f64).unwrap();
        assert!((top_score - 1.0).abs() < 0.01);
    }

    /// Mirrors NornicDB `GraphitiScenarioE2E` bulk node save with vector properties.
    #[test]
    fn test_graphiti_bulk_node_save_with_vectors() {
        let engine = make_engine();
        let parser = Parser::new();
        let mut params = HashMap::new();
        params.insert(
            "nodes".to_string(),
            serde_json::json!([
                {"uuid": "n1", "name": "Alpha", "labels": ["Entity"], "name_embedding": [1.0, 0.0, 0.0]},
                {"uuid": "n2", "name": "Beta",  "labels": ["Entity"], "name_embedding": [0.0, 1.0, 0.0]},
                {"uuid": "n3", "name": "Gamma", "labels": ["Entity"], "name_embedding": [1.0, 0.0, 0.0]},
            ]),
        );

        let result = engine
            .execute(
                &parser
                    .parse(
                        "UNWIND $nodes AS node \
                         MERGE (n:Entity {uuid: node.uuid}) \
                         SET n:$(node.labels) \
                         SET n = node \
                         RETURN n.uuid AS uuid, n.name_embedding AS emb",
                    )
                    .unwrap(),
                &params,
            )
            .unwrap();
        assert_eq!(result.rows.len(), 3, "should create/merge 3 entities");
        // Verify vector properties persisted
        for row in &result.rows {
            let emb = row.get("emb").and_then(Value::as_array);
            assert!(emb.is_some(), "each node should have a name_embedding vector");
        }
    }

    /// Mirrors NornicDB `TestE2E_VectorCosine_QueryShapes_StayOnIndexedPaths` —
    /// verifies all five vector cosine query shapes produce correct results.
    #[test]
    fn test_vector_cosine_query_shapes() {
        let engine = make_engine();
        let parser = Parser::new();
        let mut params = HashMap::new();
        let q: Vec<f64> = vec![1.0, 0.0, 0.0];
        params.insert("q".to_string(), serde_json::to_value(&q).unwrap());
        params.insert("g".to_string(), Value::String("g".into()));
        params.insert("groups".to_string(), serde_json::json!(["g"]));
        params.insert("lim".to_string(), Value::from(5));
        params.insert("min".to_string(), Value::from(0.1));

        engine
            .execute(
                &parser
                    .parse("CREATE VECTOR INDEX chunk_emb FOR (c:Chunk) ON (c.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        for i in 0..20 {
            let mut v = vec![0.0f64; 3];
            v[i % 3] = 1.0;
            engine.execute(
                &parser.parse(&format!("CREATE (c:Chunk {{uuid:'c-{i}', group_id:'g', emb: {v:?}}})")).unwrap(),
                &HashMap::new(),
            ).unwrap();
        }

        // V1: direct return cosine
        let r = engine.execute(
            &parser.parse("MATCH (c:Chunk) RETURN vector.similarity.cosine(c.emb, $q) AS s ORDER BY s DESC LIMIT 5").unwrap(),
            &params,
        ).unwrap();
        assert!(!r.rows.is_empty());

        // V2: direct return cosine with where
        let r = engine.execute(
            &parser.parse("MATCH (c:Chunk) WHERE c.group_id = $g RETURN vector.similarity.cosine(c.emb, $q) AS s ORDER BY s DESC LIMIT 5").unwrap(),
            &params,
        ).unwrap();
        assert!(!r.rows.is_empty());

        // V3: WITH projection cosine
        let r = engine.execute(
            &parser.parse("MATCH (c:Chunk) WITH c, vector.similarity.cosine(c.emb, $q) AS s RETURN c.uuid, s ORDER BY s DESC LIMIT 5").unwrap(),
            &params,
        ).unwrap();
        assert!(!r.rows.is_empty());

        // V4: parameterized limit
        let r = engine.execute(
            &parser.parse("MATCH (c:Chunk) RETURN vector.similarity.cosine(c.emb, $q) AS s ORDER BY s DESC LIMIT $lim").unwrap(),
            &params,
        ).unwrap();
        assert_eq!(r.rows.len(), 5);

        // V5: graphiti projection with pre and post where
        let r = engine.execute(
            &parser.parse("MATCH (c:Chunk) WHERE c.group_id IN $groups WITH c, vector.similarity.cosine(c.emb, $q) AS score WHERE score > $min RETURN c.uuid, score ORDER BY score DESC LIMIT $lim").unwrap(),
            &params,
        ).unwrap();
        assert!(!r.rows.is_empty());
    }

    /// Tests CALL db.index.fulltext.queryRelationships — Phase 3 Graphiti parity.
    #[test]
    fn test_fulltext_query_relationships() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE FULLTEXT INDEX rel_ft FOR ()-[r:RELATES_TO]-() ON EACH [r.fact, r.name]")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (a:Entity {name: 'A'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (b:Entity {name: 'B'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Entity {name: 'A'}), (b:Entity {name: 'B'}) CREATE (a)-[:RELATES_TO {fact: 'authentication flow', name: 'auth'}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("CALL db.index.fulltext.queryRelationships('rel_ft', 'authentication', {limit: 10}) YIELD relationship, score RETURN relationship.fact AS fact, score")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert!(!result.rows.is_empty(), "should find authentication relationship");
    }

    /// Tests Phase 3: CREATE VECTOR INDEX on relationships + CALL db.index.vector.queryRelationships.
    #[test]
    fn test_vector_relationship_index_search() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE VECTOR INDEX rel_vec_idx FOR ()-[r:RELATES_TO]-() ON (r.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (a:Entity {name: 'A'}), (b:Entity {name: 'B'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Entity {name: 'A'}), (b:Entity {name: 'B'}) CREATE (a)-[:RELATES_TO {fact_embedding: [1.0, 0.0, 0.0]}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("CALL db.index.vector.queryRelationships('rel_vec_idx', 10, [1.0, 0.0, 0.0]) YIELD relationship, score RETURN relationship.fact_embedding AS emb, score")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert!(!result.rows.is_empty(), "should find vector-similar relationship");
    }

    /// Tests FOREACH clause: FOREACH (var IN list | SET ...).
    #[test]
    fn test_foreach_set() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (a:Item {id: 'a'}), (b:Item {id: 'b'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Item) FOREACH (x IN [1] | SET n.marked = true) RETURN n.id, n.marked")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        for row in &result.rows {
            assert_eq!(row.get("n.marked"), Some(&Value::Bool(true)));
        }
    }

    /// Mirrors NornicDB `TestCreateVectorIndex_RelationshipSyntaxFormsAccepted` —
    /// relationship vector index with IF NOT EXISTS and directional arrow.
    #[test]
    fn test_rel_vector_index_syntax_variants() {
        let engine = make_engine();
        let parser = Parser::new();

        // Undirected form
        engine.execute(
            &parser.parse("CREATE VECTOR INDEX rel_emb_idx IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}").unwrap(),
            &HashMap::new(),
        ).unwrap();

        // Directed form (->)
        engine.execute(
            &parser.parse("CREATE VECTOR INDEX rel_emb_dir IF NOT EXISTS FOR ()-[e:RELATES_TO]->() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}").unwrap(),
            &HashMap::new(),
        ).unwrap();

        // Both should be queryable via SHOW INDEXES
        let result = engine.execute(
            &parser.parse("SHOW VECTOR INDEXES").unwrap(),
            &HashMap::new(),
        ).unwrap();
        assert!(result.rows.len() >= 2, "should list at least 2 vector indexes");
    }

    /// Tests improved temporal functions: date(), datetime(), timestamp(), duration().
    #[test]
    fn test_temporal_functions() {
        let engine = make_engine();
        let parser = Parser::new();

        // timestamp() returns millis since epoch
        let result = engine
            .execute(
                &parser.parse("RETURN timestamp() AS ts").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let ts = result.rows[0].get("ts").and_then(Value::as_u64).unwrap();
        assert!(ts > 1_700_000_000_000, "timestamp should be recent");

        // date() returns ISO date string
        let result = engine
            .execute(
                &parser.parse("RETURN date() AS d").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let d = result.rows[0].get("d").and_then(Value::as_str).unwrap();
        assert!(d.contains('-'), "date should contain dashes: {d}");
        assert_eq!(d.len(), 10, "date should be YYYY-MM-DD");

        // datetime() returns ISO 8601
        let result = engine
            .execute(
                &parser.parse("RETURN datetime() AS dt").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let dt = result.rows[0].get("dt").and_then(Value::as_str).unwrap();
        assert!(dt.contains('T'), "datetime should contain T: {dt}");

        // duration() returns ISO duration
        let result = engine
            .execute(
                &parser.parse("RETURN duration() AS dur").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let dur = result.rows[0].get("dur").and_then(Value::as_str).unwrap();
        assert!(!dur.is_empty(), "duration should not be empty");
    }

    /// Tests USING INDEX hints: queries with USING INDEX/S/JOIN/SCAN parse correctly.
    #[test]
    fn test_using_index_hints() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person) USING INDEX n:Person(name) WHERE n.name = 'Alice' RETURN n.name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person) USING SCAN n:Person WHERE n.age > 20 RETURN n.age")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    /// Tests temporal component functions: date.year(), date.month(), date.day(), etc.
    #[test]
    fn test_temporal_components() {
        let engine = make_engine();
        let parser = Parser::new();

        // date.year()
        let result = engine
            .execute(
                &parser.parse("RETURN date.year('2024-06-15') AS y").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("y").and_then(Value::as_i64), Some(2024));

        // date.month()
        let result = engine
            .execute(
                &parser.parse("RETURN date.month('2024-06-15') AS m").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("m").and_then(Value::as_i64), Some(6));

        // date.day()
        let result = engine
            .execute(
                &parser.parse("RETURN date.day('2024-06-15') AS d").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("d").and_then(Value::as_i64), Some(15));

        // date.quarter()
        let result = engine
            .execute(
                &parser.parse("RETURN date.quarter('2024-02-15') AS q").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("q").and_then(Value::as_i64), Some(1));

        // date.week (approx)
        let result = engine
            .execute(
                &parser.parse("RETURN date.week('2024-01-07') AS w").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let w = result.rows[0].get("w").and_then(Value::as_i64).unwrap();
        assert!(w >= 1 && w <= 2, "week should be 1 or 2, got {w}");

        // date.dayOfWeek (2024-01-01 is Monday)
        let result = engine
            .execute(
                &parser.parse("RETURN date.dayOfWeek('2024-01-01') AS dow").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("dow").and_then(Value::as_i64), Some(1));

        // date.dayOfYear
        let result = engine
            .execute(
                &parser.parse("RETURN date.dayOfYear('2024-01-15') AS doy").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("doy").and_then(Value::as_i64), Some(15));

        // date.truncate('month', ...)
        let result = engine
            .execute(
                &parser.parse("RETURN date.truncate('month', '2024-06-15') AS t").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(
            result.rows[0].get("t").and_then(Value::as_str),
            Some("2024-06-01")
        );
    }

    /// Tests temporal component functions for datetime.
    #[test]
    fn test_datetime_components() {
        let engine = make_engine();
        let parser = Parser::new();

        // datetime.year()
        let result = engine
            .execute(
                &parser.parse("RETURN datetime.year('2024-06-15T14:30:00Z') AS y").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("y").and_then(Value::as_i64), Some(2024));

        // datetime.month()
        let result = engine
            .execute(
                &parser.parse("RETURN datetime.month('2024-06-15T14:30:00Z') AS mo").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("mo").and_then(Value::as_i64), Some(6));

        // datetime.day()
        let result = engine
            .execute(
                &parser.parse("RETURN datetime.day('2024-06-15T14:30:00Z') AS d").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("d").and_then(Value::as_i64), Some(15));

        // datetime.hour()
        let result = engine
            .execute(
                &parser.parse("RETURN datetime.hour('2024-06-15T14:30:00Z') AS h").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("h").and_then(Value::as_i64), Some(14));

        // datetime.minute()
        let result = engine
            .execute(
                &parser.parse("RETURN datetime.minute('2024-06-15T14:30:00Z') AS mi").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("mi").and_then(Value::as_i64), Some(30));

        // datetime.second()
        let result = engine
            .execute(
                &parser.parse("RETURN datetime.second('2024-06-15T14:30:45Z') AS s").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("s").and_then(Value::as_i64), Some(45));

        // datetime.truncate('hour', ...)
        let result = engine
            .execute(
                &parser.parse("RETURN datetime.truncate('hour', '2024-06-15T14:30:45Z') AS t").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(
            result.rows[0].get("t").and_then(Value::as_str),
            Some("2024-06-15T14:00:00Z")
        );
    }

    /// Tests time() and localtime() functions.
    #[test]
    fn test_time_and_localtime() {
        let engine = make_engine();
        let parser = Parser::new();

        // time() returns current time
        let result = engine
            .execute(
                &parser.parse("RETURN time() AS t").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let t = result.rows[0].get("t").and_then(Value::as_str).unwrap();
        assert!(t.contains(':'), "time should contain colons: {t}");

        // localtime() returns time without Z
        let result = engine
            .execute(
                &parser.parse("RETURN localtime() AS lt").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let lt = result.rows[0].get("lt").and_then(Value::as_str).unwrap();
        assert!(lt.contains(':'), "localtime should contain colons: {lt}");

        // localdatetime() returns datetime without Z
        let result = engine
            .execute(
                &parser.parse("RETURN localdatetime() AS ldt").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let ldt = result.rows[0].get("ldt").and_then(Value::as_str).unwrap();
        assert!(ldt.contains('T'), "localdatetime should contain T: {ldt}");
        assert!(!ldt.ends_with('Z'), "localdatetime should not end with Z: {ldt}");
    }

    /// Tests time component accessors: time.hour(), time.minute(), time.second().
    #[test]
    fn test_time_components() {
        let engine = make_engine();
        let parser = Parser::new();

        let result = engine
            .execute(
                &parser.parse("RETURN time.hour('14:30:45Z') AS h").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("h").and_then(Value::as_i64), Some(14));

        let result = engine
            .execute(
                &parser.parse("RETURN time.minute('14:30:45Z') AS mi").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("mi").and_then(Value::as_i64), Some(30));

        let result = engine
            .execute(
                &parser.parse("RETURN time.second('14:30:45Z') AS s").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("s").and_then(Value::as_i64), Some(45));
    }
