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

    /// Mirrors NornicDB `TestMatchRelationshipWithLimitReturnsBoundRows`.
    #[test]
    fn test_match_relationship_with_limit_returns_bound_rows() {
        let engine = make_engine();
        let parser = Parser::new();

        for cypher in [
            "CREATE (:T {id: 'a'})",
            "CREATE (:T {id: 'b'})",
            "CREATE (:T {id: 'c'})",
            "CREATE (:T {id: 'd'})",
            "MATCH (a:T {id: 'a'}), (b:T {id: 'b'}) CREATE (a)-[:REL {group_id: 'old'}]->(b)",
            "MATCH (b:T {id: 'b'}), (c:T {id: 'c'}) CREATE (b)-[:REL {group_id: 'old'}]->(c)",
            "MATCH (c:T {id: 'c'}), (d:T {id: 'd'}) CREATE (c)-[:REL {group_id: 'old'}]->(d)",
        ] {
            engine
                .execute(&parser.parse(cypher).unwrap(), &HashMap::new())
                .unwrap();
        }

        let result = engine
            .execute(
                &parser
                    .parse("MATCH ()-[r:REL]->() WHERE r.group_id = 'old' WITH r LIMIT 1 RETURN r")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.rows.len(), 1);
    }

    /// Mirrors NornicDB `TestSetReturnRelationshipCount`.
    #[test]
    fn test_set_return_relationship_count() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:SetCountNode {id: 'a'})-[:REL {group_id: 'old'}]->(:SetCountNode {id: 'b'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse(
                        "MATCH ()-[r:REL]->() WHERE r.group_id = 'old' WITH r LIMIT 1 SET r.group_id = 'new' RETURN count(r) AS updated",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["updated"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("updated"), Some(&Value::from(1)));

        let verify = engine
            .execute(
                &parser
                    .parse("MATCH ()-[r:REL]->() WHERE r.group_id = 'new' RETURN count(r) AS updated")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(verify.rows[0].get("updated"), Some(&Value::from(1)));
    }

    /// Mirrors NornicDB `TestCreateEvaluatesToStringConcatenationProperty`.
    #[test]
    fn test_create_evaluates_to_string_concatenation_property() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:T {uuid: 't' + toString(0), label: toString(42)})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:T {uuid: 't0'}) RETURN n.uuid AS uuid, n.label AS label")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("uuid"), Some(&Value::String("t0".to_string())));
        assert_eq!(result.rows[0].get("label"), Some(&Value::String("42".to_string())));
    }

    /// Mirrors NornicDB `TestUnwindMatchCreateRelationshipCreatesEdges`.
    #[test]
    fn test_unwind_match_create_relationship_creates_edges() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:T {uuid: 't' + toString(0)}), (:T {uuid: 't' + toString(1)})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let params = HashMap::from([(
            "rows".to_string(),
            Value::Array(vec![serde_json::json!({
                "source": "t0",
                "target": "t1",
                "relID": "t0-t1"
            })]),
        )]);

        engine
            .execute(
                &parser
                    .parse("UNWIND $rows AS row MATCH (source:T {uuid: row.source}) MATCH (target:T {uuid: row.target}) CREATE (source)-[:REL {uuid: row.relID}]->(target)")
                    .unwrap(),
                &params,
            )
            .unwrap();

        let count = engine
            .execute(
                &parser
                    .parse("MATCH ()-[r:REL]->() RETURN count(r) AS count")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(count.rows[0].get("count"), Some(&Value::from(1)));
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

    /// Mirrors NornicDB `TestE2E_VectorCosine_TinyChunkPropertyPatternUsesIndexedPath` at the
    /// observable query-behavior level; copperDB does not expose the Go search-service counters.
    #[test]
    fn test_vector_cosine_tiny_chunk_property_pattern_round_trips() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE VECTOR INDEX tiny_chunk_idx FOR (c:Chunk) ON (c.emb) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        for (uuid, vector) in [
            ("c-00", vec![1.0, 0.0, 0.0, 0.0]),
            ("c-01", vec![0.8, 0.6, 0.0, 0.0]),
            ("c-02", vec![0.0, 1.0, 0.0, 0.0]),
            ("c-03", vec![0.0, 0.0, 1.0, 0.0]),
        ] {
            engine
                .execute(
                    &parser
                        .parse(&format!(
                            "CREATE (:Chunk {{uuid: '{uuid}', group_id: 'kg', emb: {vector:?}}})"
                        ))
                        .unwrap(),
                    &HashMap::new(),
                )
                .unwrap();
        }
        engine
            .execute(
                &parser
                    .parse("CREATE (:Chunk {uuid: 'other-group', group_id: 'else', emb: [1.0, 0.0, 0.0, 0.0]}), (:Chunk {uuid: 'no-embedding', group_id: 'kg'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let params = HashMap::from([("q".to_string(), serde_json::json!([1.0, 0.0, 0.0, 0.0]))]);
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (c:Chunk {group_id: 'kg'}) WHERE c.emb IS NOT NULL WITH c, vector.similarity.cosine(c.emb, $q) AS sim RETURN c.uuid AS uuid, sim ORDER BY sim DESC, c.uuid ASC LIMIT 3")
                    .unwrap(),
                &params,
            )
            .unwrap();

        let uuids = result
            .rows
            .iter()
            .map(|row| row.get("uuid").and_then(Value::as_str).unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(uuids, vec!["c-00", "c-01", "c-02"]);
        assert!(result.rows[0].get("sim").and_then(Value::as_f64).unwrap() > 0.99);
    }

    /// Adapts NornicDB `TestNorthwindSeeder_ProductsIncompleteIndexedMatchBucketKeepsFastPath`.
    /// copperDB's pipeline route scans live storage for matches, so this pins the equivalent
    /// two-MATCH Northwind product seed shape and its fast-path trace flag.
    #[test]
    fn test_northwind_products_seed_two_match_create_keeps_fast_path() {
        let engine = make_engine();
        let parser = Parser::new();

        for statement in [
            "CREATE INDEX category_id IF NOT EXISTS FOR (n:Category) ON (n.categoryID)",
            "CREATE INDEX supplier_id IF NOT EXISTS FOR (n:Supplier) ON (n.supplierID)",
            "CREATE (:Category {categoryID: 1, categoryName: 'C1'}), (:Category {categoryID: 2, categoryName: 'C2'}), (:Category {categoryID: 3, categoryName: 'C3'})",
            "CREATE (:Supplier {supplierID: 1, companyName: 'S1'}), (:Supplier {supplierID: 2, companyName: 'S2'}), (:Supplier {supplierID: 3, companyName: 'S3'})",
        ] {
            engine
                .execute(&parser.parse(statement).unwrap(), &HashMap::new())
                .unwrap();
        }

        let rows = Value::Array(vec![
            serde_json::json!({"productID": 101, "productName": "P101", "sku": "SKU-00101", "unitPrice": 1.25, "unitsInStock": 10, "discontinued": false, "description": "indexed supplier", "categoryID": 1, "supplierID": 1}),
            serde_json::json!({"productID": 102, "productName": "P102", "sku": "SKU-00102", "unitPrice": 2.50, "unitsInStock": 11, "discontinued": false, "description": "middle supplier", "categoryID": 2, "supplierID": 2}),
            serde_json::json!({"productID": 103, "productName": "P103", "sku": "SKU-00103", "unitPrice": 3.75, "unitsInStock": 12, "discontinued": false, "description": "indexed supplier again", "categoryID": 3, "supplierID": 3}),
        ]);
        let params = HashMap::from([("rows".to_string(), rows)]);
        let cypher = "UNWIND $rows AS row MATCH (c:Category {categoryID: row.categoryID}) MATCH (s:Supplier {supplierID: row.supplierID}) CREATE (p:Product {productID: row.productID, productName: row.productName, sku: row.sku, unitPrice: row.unitPrice, unitsInStock: row.unitsInStock, discontinued: row.discontinued, description: row.description}) CREATE (p)-[:PART_OF]->(c) CREATE (s)-[:SUPPLIES]->(p)";
        let pattern = detect_query_pattern(cypher);
        let (clauses, ok) = can_execute_as_pipeline(cypher);
        assert!(ok);
        let result = engine
            .execute_with_routes(
                &parser.parse(cypher).unwrap(),
                &params,
                &pattern,
                None,
                Some(clauses.as_slice()),
            )
            .unwrap();

        assert_eq!(result.stats.nodes_created, 3);
        assert_eq!(result.stats.relationships_created, 6);
        assert!(engine.hot_path_trace_snapshot().unwind_fixed_chain_link_batch);

        let product_count = engine
            .execute(
                &parser
                    .parse("MATCH (p:Product) RETURN count(p) AS count")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(product_count.rows[0].get("count"), Some(&Value::from(3)));

        let supplies_count = engine
            .execute(
                &parser
                    .parse("MATCH ()-[r:SUPPLIES]->() RETURN count(r) AS count")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(supplies_count.rows[0].get("count"), Some(&Value::from(3)));
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

    /// Mirrors NornicDB `TestGraphitiExactShape_Search_EdgeFulltext_TemporalFilterUsesCallTailFastPath` —
    /// verifies edge fulltext search with grouped temporal WHERE: ((e.invalid_at IS NULL) OR (e.invalid_at > $dt)) AND e.group_id IN $group_ids.
    #[test]
    fn test_fulltext_query_relationships_temporal_filter() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE FULLTEXT INDEX edge_ft FOR ()-[e:RELATES_TO]-() ON EACH [e.name, e.fact, e.group_id]")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (a:Entity {name: 'A'}), (b:Entity {name: 'B'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        // Edge 1: valid (invalid_at is null), should pass temporal filter
        engine
            .execute(
                &parser.parse(
                    "MATCH (a:Entity {name: 'A'}), (b:Entity {name: 'B'}) \
                     CREATE (a)-[:RELATES_TO {name: 'knows', fact: 'alice knows bob', group_id: 'g1', invalid_at: null}]->(b)"
                ).unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        // Edge 2: invalid_at in the future (after $dt), should pass temporal filter
        engine
            .execute(
                &parser.parse(
                    "MATCH (a:Entity {name: 'A'}), (b:Entity {name: 'B'}) \
                     CREATE (a)-[:RELATES_TO {name: 'knows', fact: 'also knows', group_id: 'g1', invalid_at: '2025-06-01'}]->(b)"
                ).unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        // Edge 3: different fulltext content, should be excluded by fulltext
        engine
            .execute(
                &parser.parse(
                    "MATCH (a:Entity {name: 'B'}), (b:Entity {name: 'A'}) \
                     CREATE (a)-[:RELATES_TO {name: 'other', fact: 'not relevant', group_id: 'g1', invalid_at: null}]->(b)"
                ).unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let mut params = HashMap::new();
        params.insert("query".to_string(), Value::String("knows".into()));
        params.insert("dt".to_string(), Value::String("2024-06-01".into()));
        params.insert("group_ids".to_string(), serde_json::json!(["g1"]));
        params.insert("limit".to_string(), Value::from(5));

        // Just verify fulltext + YIELD + WHERE with grouped parens compiles and runs
        let result = engine
            .execute(
                &parser
                    .parse(
                        "CALL db.index.fulltext.queryRelationships('edge_ft', $query, {limit: $limit}) \
                         YIELD relationship AS rel, score \
                         WHERE ((rel.invalid_at IS NULL) OR (rel.invalid_at > $dt)) AND rel.group_id IN $group_ids \
                         RETURN rel.name AS name, rel.fact AS fact, rel.group_id AS group_id, score \
                         ORDER BY score DESC \
                         LIMIT $limit",
                    )
                    .unwrap(),
                &params,
            )
            .unwrap();
        assert!(!result.rows.is_empty(), "should find at least one knows relationship passing temporal filter");
        for row in &result.rows {
            let name = row.get("name").and_then(Value::as_str).unwrap_or("");
            assert!(name.contains("knows"), "only 'knows' edges should match fulltext");
        }
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

    /// Mirrors NornicDB `TestVectorProcedures_NodeAndRelationshipManualVectorParityE2E`.
    #[test]
    fn test_vector_procedures_node_and_relationship_manual_vector_parity_e2e() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE VECTOR INDEX nzz_idx FOR (n:NZZ) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:NZZ {uuid: 'n1'}), (:NZZ {uuid: 'n2'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        for (uuid, emb) in [
            ("n1", serde_json::json!([1.0, 0.0, 0.0, 0.0])),
            ("n2", serde_json::json!([0.0, 1.0, 0.0, 0.0])),
        ] {
            let params = HashMap::from([("v".to_string(), emb)]);
            engine
                .execute(
                    &parser
                        .parse(&format!(
                            "MATCH (a:NZZ {{uuid: '{uuid}'}}) WITH a CALL db.create.setNodeVectorProperty(a, 'emb', $v) RETURN a"
                        ))
                        .unwrap(),
                    &params,
                )
                .unwrap();
        }

        let node_prop = engine
            .execute(
                &parser
                    .parse("MATCH (a:NZZ {uuid: 'n1'}) RETURN a.emb AS emb")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(node_prop.rows.len(), 1);
        assert_eq!(
            node_prop.rows[0].get("emb"),
            Some(&serde_json::json!([1.0, 0.0, 0.0, 0.0]))
        );

        let query_params = HashMap::from([(
            "v".to_string(),
            serde_json::json!([0.9, 0.1, 0.0, 0.0]),
        )]);
        let node_hits = engine
            .execute(
                &parser
                    .parse("CALL db.index.vector.queryNodes('nzz_idx', 5, $v) YIELD node, score RETURN node.uuid AS u, score ORDER BY score DESC")
                    .unwrap(),
                &query_params,
            )
            .unwrap();
        assert_eq!(node_hits.rows.len(), 2);
        assert_eq!(node_hits.rows[0].get("u"), Some(&Value::String("n1".to_string())));
        assert_eq!(node_hits.rows[1].get("u"), Some(&Value::String("n2".to_string())));
        assert!(node_hits.rows[0].get("score").and_then(Value::as_f64).unwrap() > 0.99);
        assert!(node_hits.rows[1].get("score").and_then(Value::as_f64).unwrap() > 0.10);

        engine
            .execute(
                &parser
                    .parse("CREATE VECTOR INDEX rzz_idx FOR ()-[r:RZZ_REL]-() ON (r.emb) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:RZZ {uuid: 'a'})-[:RZZ_REL {uuid: 'z1'}]->(:RZZ {uuid: 'b'}), (:RZZ {uuid: 'c'})-[:RZZ_REL {uuid: 'z2'}]->(:RZZ {uuid: 'd'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        for (uuid, emb) in [
            ("z1", serde_json::json!([1.0, 0.0, 0.0, 0.0])),
            ("z2", serde_json::json!([0.0, 1.0, 0.0, 0.0])),
        ] {
            let params = HashMap::from([("v".to_string(), emb)]);
            engine
                .execute(
                    &parser
                        .parse(&format!(
                            "MATCH (:RZZ)-[r:RZZ_REL {{uuid: '{uuid}'}}]->(:RZZ) WITH r CALL db.create.setRelationshipVectorProperty(r, 'emb', $v) RETURN r"
                        ))
                        .unwrap(),
                    &params,
                )
                .unwrap();
        }

        let rel_prop = engine
            .execute(
                &parser
                    .parse("MATCH (:RZZ)-[r:RZZ_REL {uuid: 'z1'}]->(:RZZ) RETURN r.emb AS emb")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(rel_prop.rows.len(), 1);
        assert_eq!(
            rel_prop.rows[0].get("emb"),
            Some(&serde_json::json!([1.0, 0.0, 0.0, 0.0]))
        );

        let rel_hits = engine
            .execute(
                &parser
                    .parse("CALL db.index.vector.queryRelationships('rzz_idx', 5, $v) YIELD relationship, score RETURN relationship.uuid AS u, score ORDER BY score DESC")
                    .unwrap(),
                &query_params,
            )
            .unwrap();
        assert_eq!(rel_hits.rows.len(), 2);
        assert_eq!(rel_hits.rows[0].get("u"), Some(&Value::String("z1".to_string())));
        assert_eq!(rel_hits.rows[1].get("u"), Some(&Value::String("z2".to_string())));
        assert!(rel_hits.rows[0].get("score").and_then(Value::as_f64).unwrap() > 0.99);
        assert!(rel_hits.rows[1].get("score").and_then(Value::as_f64).unwrap() > 0.10);
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

    /// Tests CALL YIELD with implicit RETURN SKIP/LIMIT (no RETURN keyword).
    #[test]
    fn test_call_yield_implicit_return_skip_limit() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (n:Person {name: 'Alice'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (n:Person:Dog {name: 'Rover'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // CALL YIELD with implicit LIMIT (no RETURN keyword)
        let result = engine
            .execute(
                &parser.parse("CALL db.labels() YIELD label LIMIT 1").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert!(result.columns.contains(&"label".to_string()));

        // CALL YIELD with implicit SKIP + LIMIT
        let result = engine
            .execute(
                &parser
                    .parse("CALL db.labels() YIELD label SKIP 0 LIMIT 2")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert!(result.rows.len() >= 2, "expected >=2 labels, got {}", result.rows.len());
    }

    /// Tests CALL YIELD * (wildcard) passthrough.
    #[test]
    fn test_call_yield_wildcard() {
        let engine = make_engine();
        let parser = Parser::new();

        let result = engine
            .execute(
                &parser
                    .parse("CALL db.constraints() YIELD * RETURN name, type")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert!(!result.columns.is_empty());
        assert!(result.columns.contains(&"name".to_string()));
        assert!(result.columns.contains(&"type".to_string()));
    }

    /// Tests CALL YIELD with explicit RETURN + SKIP.
    #[test]
    fn test_call_yield_return_skip() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("CREATE (n:Alpha {name: 'A'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (n:Beta {name: 'B'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("CALL db.labels() YIELD label RETURN label SKIP 1")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert!(result.rows.len() >= 1);
    }

    /// Tests temporal $param round-trip: datetime string survives CREATE + READ.
    #[test]
    fn test_temporal_param_round_trip() {
        let engine = make_engine();
        let parser = Parser::new();
        let mut params = HashMap::new();
        params.insert(
            "dt".to_string(),
            Value::String("2026-06-09T12:00:00+02:00".to_string()),
        );

        // Create node with datetime param
        engine
            .execute(
                &parser
                    .parse("CREATE (n:TemporalTest {uuid: 't', created_at: $dt})")
                    .unwrap(),
                &params,
            )
            .unwrap();

        // Read back and verify the datetime string survived
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:TemporalTest {uuid: 't'}) RETURN n.created_at AS created_at")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        let created_at = result.rows[0]
            .get("created_at")
            .and_then(Value::as_str);
        assert!(
            created_at.is_some(),
            "datetime should survive round-trip"
        );
        assert!(created_at.unwrap().contains("2026-06-09"));

        // Verify datetime.year() can parse the stored value
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:TemporalTest {uuid: 't'}) RETURN datetime.year(n.created_at) AS y")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("y").and_then(Value::as_i64), Some(2026));
    }

    /// Mirrors NornicDB `TestTemporalParam_UnwindNestedRowDatetimeRoundTripsAsTime`.
    #[test]
    fn test_temporal_param_unwind_nested_row_datetime_round_trips() {
        let engine = make_engine();
        let parser = Parser::new();
        let dt = "2026-06-19T12:00:00.123456Z";
        let params = HashMap::from([(
            "rows".to_string(),
            Value::Array(vec![serde_json::json!({"uuid": "1", "created_at": dt})]),
        )]);

        engine
            .execute(
                &parser
                    .parse("UNWIND $rows AS row MERGE (n:X {uuid: row.uuid}) SET n.created_at = row.created_at")
                    .unwrap(),
                &params,
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:X {uuid: '1'}) RETURN n.created_at AS created_at")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("created_at").and_then(Value::as_str), Some(dt));
    }

    /// Mirrors NornicDB `TestTemporalParam_UnwindNestedRowDatetimeInMapLiteralRoundTripsAsTime`.
    #[test]
    fn test_temporal_param_unwind_nested_row_datetime_in_map_literal_round_trips() {
        let engine = make_engine();
        let parser = Parser::new();
        let dt = "2026-06-19T12:00:00.123456Z";
        let params = HashMap::from([(
            "rows".to_string(),
            Value::Array(vec![serde_json::json!({"uuid": "1", "created_at": dt})]),
        )]);

        engine
            .execute(
                &parser
                    .parse("UNWIND $rows AS row MERGE (n:X {uuid: row.uuid}) SET n = {uuid: row.uuid, created_at: row.created_at}")
                    .unwrap(),
                &params,
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:X {uuid: '1'}) RETURN n.created_at AS created_at")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("created_at").and_then(Value::as_str), Some(dt));
    }

    /// Mirrors NornicDB `TestTemporalParam_UnwindNestedRowDatetimeOnRelationshipRoundTripsAsTime`.
    #[test]
    fn test_temporal_param_unwind_nested_row_datetime_on_relationship_round_trips() {
        let engine = make_engine();
        let parser = Parser::new();
        let dt = "2026-06-19T12:00:00.123456Z";
        let params = HashMap::from([(
            "rows".to_string(),
            Value::Array(vec![serde_json::json!({"uuid": "m1", "created_at": dt})]),
        )]);

        engine
            .execute(
                &parser.parse("CREATE (:X {uuid: 'a'}), (:X {uuid: 'b'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("UNWIND $rows AS row MATCH (a:X {uuid: 'a'}) MATCH (b:X {uuid: 'b'}) MERGE (a)-[r:MENTIONS {uuid: row.uuid}]->(b) SET r.created_at = row.created_at")
                    .unwrap(),
                &params,
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (:X)-[r:MENTIONS {uuid: 'm1'}]->(:X) RETURN r.created_at AS created_at")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("created_at").and_then(Value::as_str), Some(dt));
    }

    /// Mirrors NornicDB `TestTemporalParam_UnwindNestedRowDatetimeInWholeRowMapsRoundTripsAsTime`.
    #[test]
    fn test_temporal_param_unwind_nested_row_datetime_in_whole_row_maps_round_trips() {
        let engine = make_engine();
        let parser = Parser::new();
        let dt = "2026-06-19T12:00:00.123456Z";
        let node_params = HashMap::from([(
            "rows".to_string(),
            Value::Array(vec![serde_json::json!({"uuid": "1", "created_at": dt})]),
        )]);
        let rel_params = HashMap::from([(
            "rows".to_string(),
            Value::Array(vec![serde_json::json!({"uuid": "m1", "created_at": dt})]),
        )]);

        engine
            .execute(
                &parser
                    .parse("UNWIND $rows AS row MERGE (n:X {uuid: row.uuid}) SET n = row")
                    .unwrap(),
                &node_params,
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (:X {uuid: 'a'}), (:X {uuid: 'b'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("UNWIND $rows AS row MATCH (a:X {uuid: 'a'}) MATCH (b:X {uuid: 'b'}) MERGE (a)-[r:MENTIONS {uuid: row.uuid}]->(b) SET r += row")
                    .unwrap(),
                &rel_params,
            )
            .unwrap();

        let node_result = engine
            .execute(
                &parser
                    .parse("MATCH (n:X {uuid: '1'}) RETURN n.created_at AS created_at")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(node_result.rows.len(), 1);
        assert_eq!(node_result.rows[0].get("created_at").and_then(Value::as_str), Some(dt));

        let rel_result = engine
            .execute(
                &parser
                    .parse("MATCH (:X)-[r:MENTIONS {uuid: 'm1'}]->(:X) RETURN r.created_at AS created_at")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(rel_result.rows.len(), 1);
        assert_eq!(rel_result.rows[0].get("created_at").and_then(Value::as_str), Some(dt));
    }

    /// Mirrors NornicDB `TestUnwindRelationshipMergeBatch_NArityMatchAndRowReplace`.
    #[test]
    fn test_unwind_relationship_merge_batch_n_arity_match_and_row_replace() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:Service {key: 'svc-a'}), (:Topic {key: 'topic-b'}), (:Tenant {key: 'tenant-c'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let query = parser
            .parse(
                "UNWIND $rows AS row MATCH (source:Service {key: row.source_key}) MATCH (target:Topic {key: row.target_key}) MATCH (tenant:Tenant {key: row.tenant}) MERGE (source)-[rel:PUBLISHES {uuid: row.uuid, tenant: row.tenant}]->(target) SET rel = row RETURN row.uuid AS uuid, row.tenant AS tenant",
            )
            .unwrap();
        let mut params = HashMap::from([(
            "rows".to_string(),
            Value::Array(vec![serde_json::json!({
                "source_key": "svc-a",
                "target_key": "topic-b",
                "tenant": "tenant-c",
                "uuid": "edge-1",
                "fact": "first",
                "embedding": [1.0, 0.0, 0.0]
            })]),
        )]);

        let result = engine.execute(&query, &params).unwrap();
        assert_eq!(result.columns, vec!["uuid", "tenant"]);
        assert_eq!(result.rows.len(), 1);

        params.insert(
            "rows".to_string(),
            Value::Array(vec![serde_json::json!({
                "source_key": "svc-a",
                "target_key": "topic-b",
                "tenant": "tenant-c",
                "uuid": "edge-1",
                "fact": "updated",
                "embedding": [0.0, 1.0, 0.0]
            })]),
        );
        let result = engine.execute(&query, &params).unwrap();
        assert_eq!(result.rows.len(), 1);

        let count = engine
            .execute(
                &parser
                    .parse("MATCH (:Service {key: 'svc-a'})-[rel:PUBLISHES]->(:Topic {key: 'topic-b'}) WHERE rel.uuid = 'edge-1' RETURN count(rel) AS count")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(count.rows[0].get("count"), Some(&Value::from(1)));

        let stored = engine
            .execute(
                &parser
                    .parse("MATCH (:Service {key: 'svc-a'})-[rel:PUBLISHES {uuid: 'edge-1'}]->(:Topic {key: 'topic-b'}) RETURN rel.fact AS fact, rel.embedding AS embedding, rel.tenant AS tenant")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(stored.rows.len(), 1);
        assert_eq!(stored.rows[0].get("fact"), Some(&Value::String("updated".to_string())));
        assert_eq!(stored.rows[0].get("tenant"), Some(&Value::String("tenant-c".to_string())));
        assert_eq!(
            stored.rows[0].get("embedding"),
            Some(&serde_json::json!([0.0, 1.0, 0.0]))
        );
    }

    /// Mirrors NornicDB `TestRelationshipBatchScalarEdgeKeyMatchesStoredProperties` at the
    /// query contract level: scalar relationship MERGE keys must correspond to stored properties.
    #[test]
    fn test_relationship_merge_scalar_key_properties_match_stored_edges() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:Service {key: 'svc'}), (:Topic {key: 'topic'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let query = parser
            .parse(
                "UNWIND $rows AS row MATCH (source:Service {key: row.source_key}) MATCH (target:Topic {key: row.target_key}) MERGE (source)-[rel:PUBLISHES {uuid: row.uuid, tenant: row.tenant, scope: 'public'}]->(target) SET rel.fact = row.fact RETURN row.uuid AS uuid ORDER BY uuid",
            )
            .unwrap();
        let params = HashMap::from([(
            "rows".to_string(),
            Value::Array(vec![
                serde_json::json!({"source_key": "svc", "target_key": "topic", "uuid": "edge-001", "tenant": "tenant-a", "fact": "first"}),
                serde_json::json!({"source_key": "svc", "target_key": "topic", "uuid": "edge-002", "tenant": "tenant-a", "fact": "second"}),
            ]),
        )]);

        let result = engine.execute(&query, &params).unwrap();
        assert_eq!(result.rows.len(), 2);

        let count = engine
            .execute(
                &parser
                    .parse("MATCH (:Service)-[rel:PUBLISHES]->(:Topic) RETURN count(rel) AS count")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(count.rows[0].get("count"), Some(&Value::from(2)));

        let stored = engine
            .execute(
                &parser
                    .parse("MATCH (:Service)-[rel:PUBLISHES {uuid: 'edge-002', tenant: 'tenant-a', scope: 'public'}]->(:Topic) RETURN rel.fact AS fact")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(stored.rows.len(), 1);
        assert_eq!(
            stored.rows[0].get("fact"),
            Some(&Value::String("second".to_string()))
        );
    }

    /// Mirrors NornicDB `TestUnwindRelationshipMergeBatch_AmbiguousMatchFallsBack`.
    #[test]
    fn test_unwind_relationship_merge_batch_ambiguous_match_falls_back() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:Service {key: 'svc'}), (:Service {key: 'svc'}), (:Topic {key: 'topic'}), (:Tenant {key: 'tenant-a'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let params = HashMap::from([(
            "rows".to_string(),
            Value::Array(vec![serde_json::json!({
                "source_key": "svc",
                "target_key": "topic",
                "tenant": "tenant-a",
                "uuid": "edge-ambiguous",
                "embedding": [1.0, 0.0, 0.0, 0.0]
            })]),
        )]);
        let result = engine
            .execute(
                &parser
                    .parse("UNWIND $rows AS row MATCH (source:Service {key: row.source_key}) MATCH (target:Topic {key: row.target_key}) MATCH (tenant:Tenant {key: row.tenant}) MERGE (source)-[rel:PUBLISHES {uuid: row.uuid, tenant: row.tenant}]->(target) SET rel = row RETURN row.uuid AS uuid")
                    .unwrap(),
                &params,
            )
            .unwrap();

        assert_eq!(result.rows.len(), 2);
        assert!(!engine.hot_path_trace_snapshot().unwind_multi_match_relationship_batch);
    }

    /// Mirrors NornicDB `TestUnwindRelationshipMergeBatch_RepeatedIndexedMatchKeyUsesAllRowFields`.
    #[test]
    fn test_unwind_relationship_merge_batch_repeated_match_key_uses_all_row_fields() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:Component {key: 'left-1'}), (:Component {key: 'left-2'}), (:Component {key: 'right-1'}), (:Component {key: 'right-2'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let params = HashMap::from([(
            "rows".to_string(),
            Value::Array(vec![
                serde_json::json!({"left_key": "left-1", "right_key": "right-1", "uuid": "edge-1", "embedding": [1.0, 0.0, 0.0]}),
                serde_json::json!({"left_key": "left-2", "right_key": "right-2", "uuid": "edge-2", "embedding": [0.0, 1.0, 0.0]}),
            ]),
        )]);
        let result = engine
            .execute(
                &parser
                    .parse("UNWIND $rows AS row MATCH (left:Component {key: row.left_key}) MATCH (right:Component {key: row.right_key}) MERGE (left)-[rel:DEPENDS_ON {uuid: row.uuid}]->(right) SET rel = row RETURN row.uuid AS uuid")
                    .unwrap(),
                &params,
            )
            .unwrap();

        assert_eq!(result.rows.len(), 2);
        let count = engine
            .execute(
                &parser
                    .parse("MATCH (:Component)-[rel:DEPENDS_ON]->(:Component) RETURN count(rel) AS count")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(count.rows[0].get("count"), Some(&Value::from(2)));
    }

    /// Adapts NornicDB `TestUnwindRelationshipMergeBatch_IncompleteIndexedMatchBucketKeepsFastPath`.
    /// copperDB builds the batch endpoint index from live label scans rather than trusting property-index buckets,
    /// so this pins the equivalent fast-path contract for the supported two-MATCH relationship batch shape.
    #[test]
    fn test_unwind_relationship_merge_batch_indexed_labels_keep_fast_path() {
        let engine = make_engine();
        let parser = Parser::new();

        for statement in [
            "CREATE INDEX service_key_idx IF NOT EXISTS FOR (n:Service) ON (n.key)",
            "CREATE INDEX topic_key_idx IF NOT EXISTS FOR (n:Topic) ON (n.key)",
            "CREATE (:Service {key: 'svc-1'}), (:Service {key: 'svc-2'}), (:Topic {key: 'topic-1'}), (:Topic {key: 'topic-2'})",
        ] {
            engine
                .execute(&parser.parse(statement).unwrap(), &HashMap::new())
                .unwrap();
        }

        let params = HashMap::from([(
            "rows".to_string(),
            Value::Array(vec![
                serde_json::json!({"source_key": "svc-1", "target_key": "topic-1", "tenant": "tenant-a", "uuid": "edge-1", "embedding": [1.0, 0.0, 0.0, 0.0]}),
                serde_json::json!({"source_key": "svc-2", "target_key": "topic-2", "tenant": "tenant-b", "uuid": "edge-2", "embedding": [0.0, 1.0, 0.0, 0.0]}),
            ]),
        )]);
        let result = engine
            .execute(
                &parser
                    .parse("UNWIND $rows AS row MATCH (source:Service {key: row.source_key}) MATCH (target:Topic {key: row.target_key}) MERGE (source)-[rel:PUBLISHES]->(target) SET rel.uuid = row.uuid, rel.tenant = row.tenant, rel.embedding = row.embedding RETURN count(rel) AS count")
                    .unwrap(),
                &params,
            )
            .unwrap();

        assert_eq!(result.rows[0].get("count"), Some(&Value::from(2)));
        assert_eq!(result.stats.relationships_created, 2);
        assert!(
            engine
                .hot_path_trace_snapshot()
                .unwind_multi_match_relationship_batch
        );

        let stored = engine
            .execute(
                &parser
                    .parse("MATCH (:Service)-[rel:PUBLISHES]->(:Topic) RETURN rel.uuid AS uuid, rel.tenant AS tenant ORDER BY uuid")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(stored.rows.len(), 2);
        assert_eq!(
            stored.rows[0].get("uuid"),
            Some(&Value::String("edge-1".to_string()))
        );
        assert_eq!(
            stored.rows[1].get("tenant"),
            Some(&Value::String("tenant-b".to_string()))
        );
    }

    /// Mirrors NornicDB `TestCallSubqueryVariableScopeInTransactionsDetachDelete`.
    /// copperDB currently has CALL subquery scoping but not the Go `IN TRANSACTIONS` batch suffix.
    #[test]
    fn test_call_subquery_variable_scope_detach_delete() {
        let engine = make_engine();
        let parser = Parser::new();

        for cypher in [
            "CREATE (:ScopedTxDelete {uuid: 'a', group_id: 'g'})",
            "CREATE (:ScopedTxDelete {uuid: 'b', group_id: 'g'})",
            "MATCH (a:ScopedTxDelete {uuid: 'a'}), (b:ScopedTxDelete {uuid: 'b'}) CREATE (a)-[:SCOPED_TX_REL]->(b)",
        ] {
            engine
                .execute(&parser.parse(cypher).unwrap(), &HashMap::new())
                .unwrap();
        }

        let params = HashMap::from([("group_id".to_string(), Value::String("g".to_string()))]);
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:ScopedTxDelete {group_id: $group_id}) CALL { DETACH DELETE n }")
                    .unwrap(),
                &params,
            )
            .unwrap();

        assert_eq!(result.stats.nodes_deleted, 2);
        assert_eq!(result.stats.relationships_deleted, 1);

        let count = engine
            .execute(
                &parser
                    .parse("MATCH (n:ScopedTxDelete) RETURN count(n) AS count")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(count.rows[0].get("count"), Some(&Value::from(0)));
    }

    /// Mirrors NornicDB `TestExecuteChainedCallSubquery_ImplicitScalarCorrelationOptionalAggregate`.
    #[test]
    fn test_chained_call_subquery_implicit_scalar_correlation_optional_aggregate() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:Person {person_id: 'a1', person_name: 'Alice'}), (:Person {person_id: 'a2', person_name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (:Order {owner_id: 'a1', order_id: 'ORD-001', amount: 125}), (:Order {owner_id: 'a2', order_id: 'ORD-002', amount: 90})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let outer = engine
            .execute(
                &parser
                    .parse("MATCH (p:Person) WITH p.person_id AS person_id RETURN person_id ORDER BY person_id")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(outer.rows[0].get("person_id"), Some(&Value::String("a1".to_string())));

        let params = HashMap::from([("min_amount".to_string(), Value::from(100))]);
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (p:Person) WITH p.person_id AS person_id, p.person_name AS person_name CALL { OPTIONAL MATCH (o:Order) WHERE o.owner_id = person_id AND o.amount >= $min_amount RETURN collect(o.order_id) AS order_ids, count(o) AS order_count } RETURN person_id, person_name, order_ids, order_count ORDER BY person_id")
                    .unwrap(),
                &params,
            )
            .unwrap();

        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("person_id"), Some(&Value::String("a1".to_string())));
        assert_eq!(
            result.rows[0].get("order_ids"),
            Some(&Value::Array(vec![Value::String("ORD-001".to_string())]))
        );
        assert_eq!(result.rows[0].get("order_count"), Some(&Value::from(1)));
        assert_eq!(result.rows[1].get("person_id"), Some(&Value::String("a2".to_string())));
        assert_eq!(result.rows[1].get("order_ids"), Some(&Value::Array(Vec::new())));
        assert_eq!(result.rows[1].get("order_count"), Some(&Value::from(0)));
    }

    /// Tests MERGE idempotency: repeated MERGE creates only one node.
    #[test]
    fn test_merge_node_idempotent() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("MERGE (n:Singleton {key: 'unique-key'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MERGE (n:Singleton {key: 'unique-key'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Singleton {key: 'unique-key'}) RETURN count(n) AS c")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("c"), Some(&Value::from(1)));
    }

    /// Tests MATCH two nodes + MERGE relationship between them.
    #[test]
    fn test_match_merge_relationship() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) MERGE (a)-[r:KNOWS]->(b) RETURN type(r)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.stats.relationships_created, 1);

        // Idempotent: second MERGE should not create duplicate
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) MERGE (a)-[r:KNOWS]->(b) RETURN count(r) AS c")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("c"), Some(&Value::from(1)));
        assert_eq!(result.stats.relationships_created, 0);
    }

    /// Tests OPTIONAL MATCH → MERGE edge case.
    #[test]
    fn test_optional_match_merge() {
        let engine = make_engine();
        let parser = Parser::new();

        // OPTIONAL MATCH on missing node; MERGE still executes
        let result = engine
            .execute(
                &parser
                    .parse("OPTIONAL MATCH (a:Missing) MERGE (m:Merged {id: 'opt'}) RETURN m.id")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("m.id"),
            Some(&Value::String("opt".into()))
        );
    }

    /// Tests MERGE before MATCH chain.
    #[test]
    fn test_merge_before_match_chain() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("CREATE (p:P {name: 'exists'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MERGE (pre:Scratch {id: 'pre'}) MATCH (a:P) MERGE (m:Merged {id: 'ok'}) RETURN m.id")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("m.id"),
            Some(&Value::String("ok".into()))
        );
    }

    /// Tests CREATE SET CREATE boundary: SET terminates before next CREATE.
    #[test]
    fn test_create_set_create_clause_boundary() {
        let engine = make_engine();
        let parser = Parser::new();

        let result = engine
            .execute(
                &parser
                    .parse("CREATE (t:Foo) SET t.x = 'wyrd' CREATE (u:Foo) RETURN t.x")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("t.x"),
            Some(&Value::String("wyrd".into()))
        );
        assert_eq!(result.stats.nodes_created, 2);
    }

    /// Tests inline property filter on relationship edge itself: [r:TYPE {prop: val}].
    #[test]
    fn test_relationship_inline_property_filter() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (a:A {name: 'a'}), (b:B {name: 'b'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:A), (b:B) CREATE (a)-[:REL {score: 42}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH ()-[r:REL {score: 42}]->() RETURN count(r) AS c")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("c"), Some(&Value::from(1)));

        // With param
        let mut params = HashMap::new();
        params.insert("v".to_string(), Value::from(42));
        let result = engine
            .execute(
                &parser
                    .parse("MATCH ()-[r:REL {score: $v}]->() RETURN r.score")
                    .unwrap(),
                &params,
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("r.score"), Some(&Value::from(42)));
    }

    /// Tests inline property filter on relationship target with special characters.
    #[test]
    fn test_relationship_target_inline_filter_special_chars() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (e:Entry {name: 'test'}), (i:Type {name: 'Other Issue'}) CREATE (e)-[:HAS]->(i)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (e:Entry)-[:HAS]->(i:Type {name: 'Other Issue'}) RETURN count(e) AS c")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("c"), Some(&Value::from(1)));
    }

    /// Tests AVG aggregation on relationship property through generic eval path.
    #[test]
    fn test_avg_relationship_property_aggregation() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (n1:Node {id: 1}), (n2:Node {id: 2})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        for (from, to, weight) in [(1, 2, 10), (2, 1, 20), (1, 2, 30)] {
            engine
                .execute(
                    &parser
                        .parse(&format!(
                            "MATCH (a:Node {{id: {from}}}), (b:Node {{id: {to}}}) CREATE (a)-[:EDGE {{weight: {weight}}}]->(b)"
                        ))
                        .unwrap(),
                    &HashMap::new(),
                )
                .unwrap();
        }

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (a:Node {id: 1})-[r:EDGE]->(b:Node {id: 2}) RETURN avg(r.weight) AS avgWeight")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("avgWeight"), Some(&Value::from(20.0)));
    }

    /// Tests multiple aggregation functions in RETURN.
    #[test]
    fn test_aggregation_count_sum_min_max() {
        let engine = make_engine();
        let parser = Parser::new();

        for (cat, val) in [("a", 10), ("a", 30), ("b", 5)] {
            engine
                .execute(
                    &parser
                        .parse(&format!("CREATE (:Item {{cat: '{cat}', val: {val}}})"))
                        .unwrap(),
                    &HashMap::new(),
                )
                .unwrap();
        }

        // Full aggregation (no group by)
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Item) RETURN count(n) AS cnt, sum(n.val) AS total, min(n.val) AS lo, max(n.val) AS hi")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("cnt"), Some(&Value::from(3)));
        assert_eq!(result.rows[0].get("total"), Some(&Value::from(45.0)));
        assert_eq!(result.rows[0].get("lo"), Some(&Value::from(5.0)));
        assert_eq!(result.rows[0].get("hi"), Some(&Value::from(30.0)));
    }

    /// Tests aggregation with GROUP BY implicit via non-aggregate RETURN column.
    #[test]
    fn test_aggregation_implicit_group_by() {
        let engine = make_engine();
        let parser = Parser::new();

        for (cat, val) in [("a", 10), ("a", 30), ("b", 5)] {
            engine
                .execute(
                    &parser
                        .parse(&format!("CREATE (:Item {{cat: '{cat}', val: {val}}})"))
                        .unwrap(),
                    &HashMap::new(),
                )
                .unwrap();
        }

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Item) RETURN n.cat AS cat, count(n) AS cnt, avg(n.val) AS avgVal ORDER BY cat")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        // Order by cat → a then b
        assert_eq!(result.rows[0].get("cat"), Some(&Value::String("a".into())));
        assert_eq!(result.rows[0].get("cnt"), Some(&Value::from(2)));
        assert_eq!(result.rows[0].get("avgVal"), Some(&Value::from(20.0)));
        assert_eq!(result.rows[1].get("cat"), Some(&Value::String("b".into())));
        assert_eq!(result.rows[1].get("cnt"), Some(&Value::from(1)));
        assert_eq!(result.rows[1].get("avgVal"), Some(&Value::from(5.0)));
    }

    /// Tests MERGE chain: MERGE→WITH→MATCH→MERGE relationship.
    #[test]
    fn test_merge_chain_with_match() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("CREATE (b:Node {id: 'b-exists'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MERGE (a:Node {id: 'a1'}) WITH a MATCH (b:Node {id: 'b-exists'}) MERGE (a)-[:REL]->(b) RETURN a.id AS aid")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("aid"),
            Some(&Value::String("a1".into()))
        );
        assert!(result.stats.relationships_created >= 1 || result.stats.nodes_created >= 1);
    }

    /// Tests MERGE→WITH→OPTIONAL MATCH chain.
    #[test]
    fn test_merge_with_optional_match() {
        let engine = make_engine();
        let parser = Parser::new();

        let result = engine
            .execute(
                &parser
                    .parse("MERGE (a:Node {id: 'a2'}) WITH a OPTIONAL MATCH (b:Node {id: 'missing'}) RETURN a.id AS aid, b.id AS bid")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("aid"),
            Some(&Value::String("a2".into()))
        );
        assert_eq!(result.rows[0].get("bid"), Some(&Value::Null));
    }

    /// Tests FOREACH inside MERGE chain.
    #[test]
    fn test_merge_chain_foreach() {
        let engine = make_engine();
        let parser = Parser::new();

        let result = engine
            .execute(
                &parser
                    .parse("MERGE (a:Node {id: 'a4'}) WITH a FOREACH (i IN [1,2] | CREATE (:Tmp {k: i})) RETURN a.id AS aid")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("aid"),
            Some(&Value::String("a4".into()))
        );
    }

    /// Tests MERGE with context variable: MERGE (n:Doc {k: s.name}) where s is from prior clause.
    #[test]
    fn test_merge_with_context_variable() {
        let engine = make_engine();
        let parser = Parser::new();
        let mut params = HashMap::new();
        params.insert(
            "node".to_string(),
            serde_json::json!({"name": "context-test", "val": 42}),
        );

        let result = engine
            .execute(
                &parser
                    .parse("UNWIND [$node] AS s MERGE (n:Doc {name: s.name}) ON CREATE SET n.val = s.val RETURN n.name AS name, n.val AS val")
                    .unwrap(),
                &params,
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("name"),
            Some(&Value::String("context-test".into()))
        );
        assert_eq!(result.rows[0].get("val"), Some(&Value::from(42)));
    }

    /// Tests chained MATCH→WITH aggregation: aggregation in WITH clauses.
    #[test]
    fn test_chained_match_with_aggregation() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("CREATE (:A {x: 1}), (:A {x: 2}), (:B {z: 10}), (:B {z: 20})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (a:A) WITH count(a) AS aCount MATCH (b:B) WITH aCount, count(b) AS bCount RETURN aCount, bCount")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("aCount"), Some(&Value::from(2)));
        assert_eq!(result.rows[0].get("bCount"), Some(&Value::from(2)));
    }

    /// Tests aggregation identity: MATCH with no results + aggregation returns identity.
    #[test]
    fn test_aggregation_identity_on_empty_match() {
        let engine = make_engine();
        let parser = Parser::new();

        // count(n) on empty match → 1 row with count=0
        let r = engine
            .execute(
                &parser.parse("MATCH (n:Missing) RETURN count(n) AS c").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].get("c"), Some(&Value::from(0)));

        // avg(n.x) on empty match → 1 row with null
        let r = engine
            .execute(
                &parser.parse("MATCH (n:Missing) RETURN avg(n.x) AS a").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].get("a"), Some(&Value::Null));

        // sum(n.x) on empty → 0
        let r = engine
            .execute(
                &parser.parse("MATCH (n:Missing) RETURN sum(n.x) AS s").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].get("s"), Some(&Value::from(0)));

        // No aggregation on empty match → 0 rows
        let r = engine
            .execute(
                &parser.parse("MATCH (n:Missing) RETURN n.x AS x").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(r.rows.len(), 0);
    }

    /// Tests MATCH→WITH count→MATCH pattern where no B nodes exist.
    /// Standard Cypher: MATCH fails → 0 rows → aggregation identity row.
    #[test]
    fn test_chained_match_with_aggregation_no_second_match() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("CREATE (:A {x: 1}), (:A {x: 2})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // MATCH fails → 0 rows → RETURN count(b) produces identity row (count=0)
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (a:A) WITH count(a) AS aCount MATCH (b:Missing) RETURN aCount, count(b) AS bCount")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        // Aggregation on empty input: 1 row with identity values
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("aCount"), Some(&Value::Null));
        assert_eq!(result.rows[0].get("bCount"), Some(&Value::from(0)));
    }

    /// Tests MATCH→WITH count→OPTIONAL MATCH: aggregation survives OPTIONAL.
    #[test]
    fn test_chained_match_with_aggregation_optional_match() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("CREATE (:A {x: 1}), (:A {x: 2})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (a:A) WITH count(a) AS aCount OPTIONAL MATCH (b:Missing) RETURN aCount, count(b) AS bCount")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        // OPTIONAL MATCH preserves the WITH aggregation row
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("aCount"), Some(&Value::from(2)));
        assert_eq!(result.rows[0].get("bCount"), Some(&Value::from(0)));
    }

    /// Tests bracket access: $d['uuid'] and d['uuid'] syntax.
    #[test]
    fn test_bracket_access() {
        let engine = make_engine();
        let parser = Parser::new();
        let mut params = HashMap::new();
        params.insert(
            "d".to_string(),
            serde_json::json!({"uuid": "abc-123", "name": "Test"}),
        );

        // $d['uuid'] on param
        let result = engine
            .execute(
                &parser.parse("RETURN $d['uuid'] AS uuid").unwrap(),
                &params,
            )
            .unwrap();
        assert_eq!(
            result.rows[0].get("uuid"),
            Some(&Value::String("abc-123".into()))
        );

        // Bracket access through WITH alias
        let result = engine
            .execute(
                &parser.parse("WITH $d AS d RETURN d['uuid'] AS uuid").unwrap(),
                &params,
            )
            .unwrap();
        assert_eq!(
            result.rows[0].get("uuid"),
            Some(&Value::String("abc-123".into()))
        );
    }

    /// Tests `+=` map merge on matched node (SET-only, no MERGE).
    #[test]
    fn test_set_map_merge_on_matched_node() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("CREATE (n:Test {name: 'Alice', age: 30})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // SET += should merge new keys and overwrite existing
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Test {name: 'Alice'}) SET n += {name: 'Bob', city: 'NYC'} RETURN n.name AS name, n.city AS city")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("name"), Some(&Value::String("Bob".into())));
        assert_eq!(result.rows[0].get("city"), Some(&Value::String("NYC".into())));
    }

    /// Tests `+=` map merge inside MERGE ON CREATE/ON MATCH.
    #[test]
    fn test_merge_on_create_map_merge() {
        let engine = make_engine();
        let parser = Parser::new();
        let mut params = HashMap::new();
        params.insert(
            "node".to_string(),
            serde_json::json!({"url": "https://example.com", "name": "First"}),
        );

        // ON CREATE: map merge
        let result = engine
            .execute(
                &parser
                    .parse("MERGE (p:Pds {url: $node.url}) ON CREATE SET p += $node RETURN p.name AS name")
                    .unwrap(),
                &params,
            )
            .unwrap();
        assert_eq!(result.rows[0].get("name"), Some(&Value::String("First".into())));

        // ON MATCH: map merge should overwrite
        params.insert("node".to_string(), serde_json::json!({"url": "https://example.com", "name": "Updated"}));
        let result = engine
            .execute(
                &parser
                    .parse("MERGE (p:Pds {url: $node.url}) ON MATCH SET p += $node RETURN p.name AS name")
                    .unwrap(),
                &params,
            )
            .unwrap();
        assert_eq!(result.rows[0].get("name"), Some(&Value::String("Updated".into())));
    }

    /// Tests chain break: MATCH fails → skips subsequent FOREACH/CREATE.
    #[test]
    fn test_chain_break_match_fails_skips_foreach() {
        let engine = make_engine();
        let parser = Parser::new();

        // Set up: create nodes, then run a chain where MATCH fails
        engine
            .execute(
                &parser
                    .parse("MERGE (a:Node {id: 'a-skip'}) MERGE (b:Node {id: 'b-skip'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // MATCH on Missing fails → FOREACH should NOT execute
        let result = engine
            .execute(
                &parser
                    .parse("MERGE (a:Node {id: 'a-skip'}) MERGE (b:Node {id: 'b-skip'}) WITH a, b MATCH (m:Missing {id: 'none'}) FOREACH (i IN [1,2] | CREATE (:Tmp {k: i})) RETURN a.id AS aid")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        // MATCH fails → 0 rows, FOREACH never runs
        assert_eq!(result.rows.len(), 0);
    }

    /// Tests multi-label MERGE: MERGE with two labels only matches nodes with all.
    #[test]
    fn test_merge_multi_label_matching() {
        let engine = make_engine();
        let parser = Parser::new();

        // Single-label node
        engine
            .execute(
                &parser.parse("CREATE (c:FileChunk {id: 'single'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Basic single-label MATCH works
        let r = engine
            .execute(
                &parser.parse("MATCH (n:FileChunk) RETURN n.id AS id ORDER BY id")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].get("id"), Some(&Value::String("single".into())));

        // MERGE dual-label on single-label node → should create new
        let r = engine
            .execute(
                &parser
                    .parse("MERGE (n:FileChunk:Node {id: 'single'}) RETURN n.id AS id")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.stats.nodes_created, 1,
            "MERGE with extra label should create new node when existing lacks that label");

        // Now 2 FileChunk nodes
        let r = engine
            .execute(
                &parser.parse("MATCH (n:FileChunk) RETURN n.id AS id ORDER BY id")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    /// Tests inline property match after SET mutation.
    #[test]
    fn test_inline_property_match_after_set() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("CREATE (h:Heuristic {title: 'T'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // SET a property, then verify inline {prop: val} match finds it
        engine
            .execute(
                &parser.parse("MATCH (h:Heuristic {title: 'T'}) SET h.tested_against = 'v'")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse("MATCH (h:Heuristic {tested_against: 'v'}) RETURN count(h) AS c")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows[0].get("c"), Some(&Value::from(1)));
    }

    /// Tests MERGE relationship with ON CREATE/ON MATCH SET.
    #[test]
    fn test_chained_relationship_merge_with_set() {
        let engine = make_engine();
        let parser = Parser::new();

        // MERGE nodes + MERGE relationship + ON CREATE SET
        let result = engine
            .execute(
                &parser
                    .parse("MERGE (a:A {name: 'a'}) MERGE (b:B {name: 'b'}) WITH a, b MERGE (a)-[r:KNOWS]->(b) ON CREATE SET r.weight = 1 RETURN r.weight AS rw")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("rw"), Some(&Value::from(1)));
        assert_eq!(result.stats.relationships_created, 1);

        // Second MERGE should match existing + ON MATCH SET update
        let result = engine
            .execute(
                &parser
                    .parse("MERGE (a:A {name: 'a'}) MERGE (b:B {name: 'b'}) WITH a, b MERGE (a)-[r:KNOWS]->(b) ON MATCH SET r.weight = 2 RETURN r.weight AS rw")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("rw"), Some(&Value::from(2)));
        assert_eq!(result.stats.relationships_created, 0);
    }

    /// Tests multi-MERGE chain: MERGE→WITH→MERGE→WITH→MERGE relationship.
    #[test]
    fn test_multi_merge_chain_with_relationship() {
        let engine = make_engine();
        let parser = Parser::new();

        let result = engine
            .execute(
                &parser
                    .parse("MERGE (a:Node {id: 'a3'}) WITH a MERGE (b:Node {id: 'b3'}) WITH a, b MERGE (a)-[:REL]->(b) RETURN a.id AS aid, b.id AS bid")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("aid"), Some(&Value::String("a3".into())));
        assert_eq!(result.rows[0].get("bid"), Some(&Value::String("b3".into())));
        assert!(result.stats.nodes_created >= 2);
        assert_eq!(result.stats.relationships_created, 1);
    }

    /// Tests `+=` map merge with nil audit keys — nil values must not clobber explicit SET.
    #[test]
    fn test_merge_on_create_map_merge_nil_keys() {
        let engine = make_engine();
        let parser = Parser::new();
        let mut params = HashMap::new();
        // Input has `created: null` — must not overwrite `p.created` set explicitly
        params.insert(
            "node".to_string(),
            serde_json::json!({"url": "https://example.com", "created": null, "name": "first"}),
        );

        let result = engine
            .execute(
                &parser
                    .parse("MERGE (p:Pds {url: $node.url}) ON CREATE SET p.created = timestamp(), p += $node RETURN p.name, p.created")
                    .unwrap(),
                &params,
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("p.name"), Some(&Value::String("first".into())));
        // p.created should be set (not null) since ON CREATE explicitly sets it
        assert!(
            result.rows[0].get("p.created").and_then(Value::as_u64).unwrap_or(0) > 0,
            "created should be a timestamp, not null"
        );
    }

    /// Tests unique constraint enforcement.
    #[test]
    fn test_unique_constraint_enforcement() {
        let engine = make_engine();
        let parser = Parser::new();

        // Create constraint
        engine
            .execute(
                &parser
                    .parse("CREATE CONSTRAINT node_id_unique IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // First insert should succeed
        engine
            .execute(
                &parser.parse("CREATE (n:Node {id: 'u1'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Second insert with same id should fail
        let err = engine
            .execute(
                &parser.parse("CREATE (n:Node {id: 'u1'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("already exists") || msg.contains("unique"));

        // Duplicate assertion removed
    }

    /// Tests exists constraint enforcement.
    #[test]
    fn test_exists_constraint_enforcement() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE CONSTRAINT person_name_required IF NOT EXISTS FOR (p:Person) REQUIRE p.name IS NOT NULL")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // CREATE without required property should fail
        let err = engine
            .execute(
                &parser.parse("CREATE (p:Person {age: 30})").unwrap(),
                &HashMap::new(),
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("missing") || msg.contains("required") || msg.contains("null"));

        // CREATE with required property should succeed
        engine
            .execute(
                &parser.parse("CREATE (p:Person {name: 'Alice', age: 30})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
    }

    /// Tests Node Key constraint enforcement.
    #[test]
    fn test_node_key_constraint_enforcement() {
        let engine = make_engine();
        let parser = Parser::new();

        // Create a NodeKey constraint on (first_name, last_name)
        engine
            .execute(
                &parser
                    .parse("CREATE CONSTRAINT person_key IF NOT EXISTS FOR (p:Person) REQUIRE (p.first_name, p.last_name) IS NODE KEY")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // First insert should succeed
        engine
            .execute(
                &parser
                    .parse("CREATE (p:Person {first_name: 'John', last_name: 'Doe'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Insert with same key should fail
        let err = engine
            .execute(
                &parser
                    .parse("CREATE (p:Person {first_name: 'John', last_name: 'Doe'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("already exists") || msg.contains("key"), "got: {msg}");

        // Insert with null key property should fail
        let err = engine
            .execute(
                &parser
                    .parse("CREATE (p:Person {first_name: 'Jane'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("cannot be null") || msg.contains("NODE KEY"), "got: {msg}");

        // Insert with different key should succeed
        engine
            .execute(
                &parser
                    .parse("CREATE (p:Person {first_name: 'Jane', last_name: 'Smith'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
    }

    /// Tests Type constraint enforcement (IS :: TYPE).
    #[test]
    fn test_type_constraint_enforcement() {
        let engine = make_engine();
        let parser = Parser::new();

        // Create a type constraint on Person.age IS :: INTEGER
        engine
            .execute(
                &parser
                    .parse("CREATE CONSTRAINT person_age_type IF NOT EXISTS FOR (p:Person) REQUIRE p.age IS :: INTEGER")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // CREATE with correct type should succeed
        engine
            .execute(
                &parser
                    .parse("CREATE (p:Person {name: 'Alice', age: 30})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // CREATE with wrong type (string instead of int) should fail
        let err = engine
            .execute(
                &parser
                    .parse("CREATE (p:Person {name: 'Bob', age: 'thirty'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("must be of type") || msg.contains("INTEGER"), "got: {msg}");
    }

    /// Tests Relationship Key constraint enforcement.
    #[test]
    fn test_relationship_key_constraint_enforcement() {
        let engine = make_engine();
        let parser = Parser::new();

        // Create a RelationshipKey constraint
        engine
            .execute(
                &parser
                    .parse("CREATE CONSTRAINT rel_key IF NOT EXISTS FOR ()-[r:KNOWS]-() REQUIRE (r.since) IS RELATIONSHIP KEY")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Create two nodes
        engine
            .execute(
                &parser.parse("CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Create relationship with required property should succeed
        engine
            .execute(
                &parser.parse("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[r:KNOWS {since: 2020}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Create relationship without required key property should fail
        let err = engine
            .execute(
                &parser.parse("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[r:KNOWS]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("cannot be null") || msg.contains("RELATIONSHIP KEY"), "got: {msg}");
    }

    /// Tests that Relationship Key constraint blocks duplicate keys.
    #[test]
    fn test_relationship_key_unique_enforcement() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE CONSTRAINT rel_key2 IF NOT EXISTS FOR ()-[r:KNOWS]-() REQUIRE (r.since) IS RELATIONSHIP KEY")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .execute(
                &parser.parse("CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // First relationship should succeed
        engine
            .execute(
                &parser.parse("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[r:KNOWS {since: 2020}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Second relationship with same key between same nodes should fail
        let err = engine
            .execute(
                &parser.parse("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[r:KNOWS {since: 2020}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("already exists") || msg.contains("key") || msg.contains("RELATIONSHIP"), "got: {msg}");
    }

    /// Tests that VECTOR INDEX OPTIONS are persisted and retrievable.
    #[test]
    fn test_vector_index_options_persistence() {
        let engine = make_engine();
        let parser = Parser::new();

        // Create vector index with options
        engine
            .execute(
                &parser
                    .parse("CREATE VECTOR INDEX idx_embed IF NOT EXISTS FOR (n:Doc) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 768, `vector.similarity_function`: 'cosine'}}")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Verify via SHOW INDEXES that the index exists on the same engine storage.
        let result = engine
            .execute(
                &parser.parse("SHOW VECTOR INDEXES").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("name").and_then(|v| v.as_str()),
            Some("idx_embed")
        );

        // Drop index should work
        engine
            .execute(
                &parser.parse("DROP INDEX idx_embed").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Verify index is gone
        let result = engine
            .execute(
                &parser.parse("SHOW VECTOR INDEXES").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 0);
    }

    /// Tests Domain constraint enforcement.
    #[test]
    fn test_domain_constraint_enforcement() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE CONSTRAINT domain_status FOR (n:Task) REQUIRE n.status IN ['open', 'closed', 'in-progress']")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Valid value should succeed
        engine
            .execute(
                &parser
                    .parse("CREATE (n:Task {name: 'task1', status: 'open'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Invalid value should fail
        let err = engine
            .execute(
                &parser
                    .parse("CREATE (n:Task {name: 'task2', status: 'unknown'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("DOMAIN") || msg.contains("not in allowed domain"), "got: {msg}");

        // NULL value should be allowed
        engine
            .execute(
                &parser
                    .parse("CREATE (n:Task {name: 'task3'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Numeric domain values
        engine
            .execute(
                &parser
                    .parse("CREATE CONSTRAINT domain_priority FOR (n:Task) REQUIRE n.priority IN [1, 2, 3]")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .execute(
                &parser.parse("CREATE (n:Task {name: 't4', priority: 1})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let err = engine
            .execute(
                &parser.parse("CREATE (n:Task {name: 't5', priority: 99})").unwrap(),
                &HashMap::new(),
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("DOMAIN") || msg.contains("not in allowed domain"), "got: {msg}");
    }

    /// Tests Temporal constraint enforcement.
    #[test]
    fn test_temporal_constraint_enforcement() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE CONSTRAINT fact_temporal FOR (n:FactVersion) REQUIRE (n.fact_key, n.valid_from, n.valid_to) IS TEMPORAL")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Valid first insert
        engine
            .execute(
                &parser
                    .parse("CREATE (n:FactVersion {fact_key: 'fact1', valid_from: 1000, valid_to: 2000})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Non-overlapping second insert should succeed
        engine
            .execute(
                &parser
                    .parse("CREATE (n:FactVersion {fact_key: 'fact1', valid_from: 3000, valid_to: 4000})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Overlapping insert should fail
        let err = engine
            .execute(
                &parser
                    .parse("CREATE (n:FactVersion {fact_key: 'fact1', valid_from: 1500, valid_to: 2500})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("TEMPORAL") || msg.contains("overlap"), "got: {msg}");

        // Null key should fail
        let err = engine
            .execute(
                &parser
                    .parse("CREATE (n:FactVersion {valid_from: 10, valid_to: 20})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("TEMPORAL") || msg.contains("cannot be null"), "got: {msg}");
    }

    /// Tests Temporal constraint with NO OVERLAP syntax.
    #[test]
    fn test_temporal_constraint_no_overlap_syntax() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE CONSTRAINT no_overlap_temporal FOR (n:FactVersion) REQUIRE (n.fact_key, n.valid_from, n.valid_to) IS TEMPORAL NO OVERLAP")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .execute(
                &parser
                    .parse("CREATE (n:FactVersion {fact_key: 'f1', valid_from: 1, valid_to: 10})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let err = engine
            .execute(
                &parser
                    .parse("CREATE (n:FactVersion {fact_key: 'f1', valid_from: 5, valid_to: 15})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("TEMPORAL") || msg.contains("overlap"), "got: {msg}");
    }

    /// Tests allShortestPaths returns all paths at the minimum distance.
    #[test]
    fn test_all_shortest_paths_returns_all_minimum_distance_paths() {
        let engine = make_engine();
        let parser = Parser::new();

        // Create a diamond graph: a -> b -> d and a -> c -> d (both length 2)
        engine
            .execute(
                &parser
                    .parse("CREATE (a:Node {name: 'a'}), (b:Node {name: 'b'}), (c:Node {name: 'c'}), (d:Node {name: 'd'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Node {name: 'a'}), (b:Node {name: 'b'}) CREATE (a)-[:LINK]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (b:Node {name: 'b'}), (d:Node {name: 'd'}) CREATE (b)-[:LINK]->(d)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Node {name: 'a'}), (c:Node {name: 'c'}) CREATE (a)-[:LINK]->(c)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (c:Node {name: 'c'}), (d:Node {name: 'd'}) CREATE (c)-[:LINK]->(d)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // allShortestPaths should return both paths of length 2
        let result = engine
            .execute(
                &parser
                    .parse("MATCH p = allShortestPaths((a:Node {name: 'a'})-[:LINK*]->(d:Node {name: 'd'})) RETURN length(p) AS hops, nodes(p) AS nodes")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.rows.len(), 2, "should return 2 shortest paths");
        for row in &result.rows {
            let hops = row.get("hops").and_then(|v| v.as_i64()).unwrap_or(0);
            assert_eq!(hops, 2, "all paths should have minimum length 2");
        }
    }

    /// Tests allShortestPaths with a longer alternative path — only shortest paths returned.
    #[test]
    fn test_all_shortest_paths_excludes_longer_paths() {
        let engine = make_engine();
        let parser = Parser::new();

        // Create: a -> b -> c (length 2) and a -> d -> e -> c (length 3)
        engine
            .execute(
                &parser
                    .parse("CREATE (a:Node {name: 'a'}), (b:Node {name: 'b'}), (c:Node {name: 'c'}), (d:Node {name: 'd'}), (e:Node {name: 'e'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        for (from, to) in [("a", "b"), ("b", "c"), ("a", "d"), ("d", "e"), ("e", "c")] {
            engine
                .execute(
                    &parser
                        .parse(&format!(
                            "MATCH (a:Node {{name: '{from}'}}), (b:Node {{name: '{to}'}}) CREATE (a)-[:LINK]->(b)"
                        ))
                        .unwrap(),
                    &HashMap::new(),
                )
                .unwrap();
        }

        let result = engine
            .execute(
                &parser
                    .parse("MATCH p = allShortestPaths((a:Node {name: 'a'})-[:LINK*]->(c:Node {name: 'c'})) RETURN length(p) AS hops")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Should return only the path of length 2, not length 3
        assert_eq!(result.rows.len(), 1, "should return only paths at minimum distance");
        for row in &result.rows {
            let hops = row.get("hops").and_then(|v| v.as_i64()).unwrap_or(0);
            assert_eq!(hops, 2, "minimum path length should be 2");
        }
    }

    /// Tests that MERGE ... ON CREATE SET enforces constraints after SET is applied.
    #[test]
    fn test_merge_on_create_set_enforces_constraints_on_updated_properties() {
        let engine = make_engine();
        let parser = Parser::new();

        // Create a NOT NULL constraint on Person.name
        engine
            .execute(
                &parser
                    .parse("CREATE CONSTRAINT person_name_required FOR (n:Person) REQUIRE n.name IS NOT NULL")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // MERGE creates a node with no name, then ON CREATE SET adds name — should succeed
        engine
            .execute(
                &parser
                    .parse("MERGE (n:Person {id: 1}) ON CREATE SET n.name = 'Alice', n.age = 30")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // MERGE creates a node with no name, ON CREATE SET does NOT set name — should fail
        let err = engine
            .execute(
                &parser
                    .parse("MERGE (n:Person {id: 2}) ON CREATE SET n.age = 25")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("missing") || msg.contains("required") || msg.contains("null"), "got: {msg}");
    }

    /// Tests that MERGE ... ON CREATE SET enforces constraints on relationship properties.
    #[test]
    fn test_merge_on_create_set_enforces_relationship_constraints() {
        let engine = make_engine();
        let parser = Parser::new();

        // Create an EXISTS constraint on relationship property
        engine
            .execute(
                &parser
                    .parse("CREATE CONSTRAINT knows_since_exists FOR ()-[r:KNOWS]-() REQUIRE r.since IS NOT NULL")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .execute(
                &parser.parse("CREATE (a:Node {name: 'A'}), (b:Node {name: 'B'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // MERGE with ON CREATE SET that sets the required property — should succeed
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) MERGE (a)-[r:KNOWS]->(b) ON CREATE SET r.since = 2024")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // MERGE with ON CREATE SET that does NOT set the required property — should fail
        engine
            .execute(
                &parser.parse("CREATE (c:Node {name: 'C'}), (d:Node {name: 'D'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let err = engine
            .execute(
                &parser
                    .parse("MATCH (c:Node {name: 'C'}), (d:Node {name: 'D'}) MERGE (c)-[r:KNOWS]->(d) ON CREATE SET r.weight = 1.0")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("missing") || msg.contains("required") || msg.contains("null"), "got: {msg}");
    }

    /// Tests that vector index options (dimensions, similarity) are consumed at query time.
    #[test]
    fn test_vector_index_options_consumed_at_query_time() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (n:Doc {name: 'doc1', embedding: [1.0, 0.0, 0.0]})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .execute(
                &parser
                    .parse("CREATE VECTOR INDEX idx_doc_vec FOR (n:Doc) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'euclidean'}}")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Query with correct dimensions succeeds
        let result = engine
            .execute(
                &parser
                    .parse("CALL db.index.vector.queryNodes('idx_doc_vec', 5, [1.0, 0.0, 0.0])")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert!(!result.rows.is_empty(), "should return results");

        // Query with wrong dimensions fails
        let err = engine
            .execute(
                &parser
                    .parse("CALL db.index.vector.queryNodes('idx_doc_vec', 5, [1.0, 0.0])")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("dimensions") || msg.contains("expects"), "got: {msg}");
    }

    /// Tests pattern comprehension evaluation: [(n)-->(m) | expr] in RETURN context.
    #[test]
    fn test_pattern_comprehension_in_return() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (a:Node {name: 'a'}), (b:Node {name: 'b'}), (c:Node {name: 'c'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Node {name: 'a'}), (b:Node {name: 'b'}) CREATE (a)-[:LINK]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Node {name: 'a'}), (c:Node {name: 'c'}) CREATE (a)-[:LINK]->(c)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // First, verify direct MATCH works
        let direct = engine
            .execute(
                &parser.parse("MATCH (a:Node {name: 'a'})-[r:LINK]->(m:Node) RETURN m.name AS name").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(direct.rows.len(), 2, "direct MATCH should find 2 nodes");

        // Pattern comprehension
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (a:Node {name: 'a'}) RETURN [(a)-[:LINK]->(m:Node) | m.name] AS names")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1, "should have 1 result row");
        let names = result.rows[0].get("names").and_then(|v| v.as_array());
        assert!(names.is_some(), "should return a list, got: {:?}", result.rows[0]);
        let names = names.unwrap();
        assert_eq!(names.len(), 2, "should have 2 reachable nodes, got names: {:?}", names);
    }

    /// Tests pattern comprehension with WHERE predicate.
    #[test]
    fn test_pattern_comprehension_with_where() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (a:Node {name: 'a'}), (b:Node {name: 'b'}), (c:Node {name: 'c'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Node {name: 'a'}), (b:Node {name: 'b'}) CREATE (a)-[:LINK]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Node {name: 'a'}), (c:Node {name: 'c'}) CREATE (a)-[:LINK]->(c)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Filter with WHERE — only nodes where name != 'b'
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (a:Node {name: 'a'}) RETURN [(a)-[:LINK]->(m:Node) WHERE m.name = 'b' | m.name] AS names")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let names = result.rows[0].get("names").and_then(|v| v.as_array()).unwrap();
        assert_eq!(names.len(), 1, "WHERE should filter to only 'b'");
        assert_eq!(names[0].as_str().unwrap(), "b");
    }

    /// Tests CALL {} subquery basic importing behavior.
    #[test]
    fn test_call_subquery_importing() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (a:Node {name: 'a'}), (b:Node {name: 'b'}), (c:Node {name: 'c'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Node {name: 'a'}), (b:Node {name: 'b'}) CREATE (a)-[:LINK]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Node {name: 'a'}), (c:Node {name: 'c'}) CREATE (a)-[:LINK]->(c)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // CALL {} subquery that imports outer bindings and returns filtered results
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (a:Node {name: 'a'}) CALL { MATCH (a)-[:LINK]->(m:Node) RETURN m.name AS name } RETURN name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 2, "should find 2 reachable nodes via subquery");
    }

    /// Tests CALL {} subquery with UNION.
    #[test]
    fn test_call_subquery_union() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (a:Node {name: 'a'}), (b:Node {name: 'b'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Node {name: 'a'}), (b:Node {name: 'b'}) CREATE (a)-[:LINK]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // UNION combines results from two subquery branches
        let result = engine
            .execute(
                &parser
                    .parse("CALL { MATCH (n:Node {name: 'a'}) RETURN n.name AS name UNION MATCH (n:Node {name: 'b'}) RETURN n.name AS name } RETURN name ORDER BY name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 2, "UNION should combine 2 results");
    }

    /// Tests WHERE EXISTS { ... } existential subquery.
    #[test]
    fn test_where_exists_subquery() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (a:Node {name: 'a'}), (b:Node {name: 'b'}), (c:Node {name: 'c'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Node {name: 'a'}), (b:Node {name: 'b'}) CREATE (a)-[:LINK]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // WHERE EXISTS should filter to only nodes with outgoing LINK edges
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Node) WHERE EXISTS { MATCH (n)-[:LINK]->() } RETURN n.name AS name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        // a has outgoing edges, b and c don't
        assert_eq!(result.rows.len(), 1, "only 'a' should have outgoing edges");
        assert_eq!(
            result.rows[0].get("name").and_then(|v| v.as_str()),
            Some("a")
        );
    }

    /// Tests WHERE EXISTS returns no rows when subquery empty.
    #[test]
    fn test_where_exists_subquery_empty() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("CREATE (a:Node {name: 'a'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // a has no outgoing LINK edges, so WHERE EXISTS returns nothing
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Node) WHERE EXISTS { MATCH (n)-[:LINK]->() } RETURN n.name AS name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 0, "no node has outgoing LINK edges");
    }

    /// Tests that property index maintenance tracks node updates correctly.
    #[test]
    fn test_property_index_maintenance_on_update() {
        let engine = make_engine();
        let parser = Parser::new();

        // Create an index on Person.name
        engine
            .execute(
                &parser
                    .parse("CREATE INDEX person_name_idx FOR (n:Person) ON (n.name)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Create a node
        engine
            .execute(
                &parser
                    .parse("CREATE (n:Person {name: 'Alice', age: 30})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Update the node — change the indexed property
        engine
            .execute(
                &parser
                    .parse("MATCH (n:Person {name: 'Alice'}) SET n.name = 'Alicia'")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Query by old name should find nothing
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person {name: 'Alice'}) RETURN n.name AS name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 0, "old indexed value should not match");

        // Query by new name should find the node
        let result = engine
            .execute(
                &parser
                    .parse("MATCH (n:Person {name: 'Alicia'}) RETURN n.name AS name")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1, "new indexed value should match");
        assert_eq!(
            result.rows[0].get("name").and_then(|v| v.as_str()),
            Some("Alicia")
        );
    }

    /// Tests that DELETE properly cleans up node property index entries.
    #[test]
    fn test_delete_cleans_up_property_index_entries() {
        let engine = make_engine();
        let parser = Parser::new();

        // Create index on Person.name
        engine
            .execute(
                &parser
                    .parse("CREATE INDEX person_idx FOR (n:Person) ON (n.name)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Create two nodes
        engine
            .execute(
                &parser
                    .parse("CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Both should be findable
        let result = engine
            .execute(
                &parser.parse("MATCH (n:Person) RETURN n.name AS name ORDER BY name").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 2);

        // Delete Alice
        engine
            .execute(
                &parser.parse("MATCH (n:Person {name: 'Alice'}) DELETE n").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Alice should be gone, Bob remains
        let result = engine
            .execute(
                &parser.parse("MATCH (n:Person {name: 'Alice'}) RETURN n").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 0, "deleted node should not be found via index");

        let result = engine
            .execute(
                &parser.parse("MATCH (n:Person {name: 'Bob'}) RETURN n.name AS name").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1, "non-deleted node should still be found");
        assert_eq!(
            result.rows[0].get("name").and_then(|v| v.as_str()),
            Some("Bob")
        );

        // Re-create Alice — should succeed without index conflicts
        engine
            .execute(
                &parser.parse("CREATE (a:Person {name: 'Alice'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser.parse("MATCH (n:Person {name: 'Alice'}) RETURN n.name AS name").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1, "re-created node should be found via index");
    }

    /// Tests that DELETE on edges cleans up edge property indexes.
    #[test]
    fn test_delete_cleans_up_edge_property_index_entries() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE INDEX rel_idx FOR ()-[r:KNOWS]-() ON (r.since)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .execute(
                &parser
                    .parse("CREATE (a:Node {name: 'A'}), (b:Node {name: 'B'}), (c:Node {name: 'C'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) CREATE (a)-[:KNOWS {since: 2020}]->(b)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'}) CREATE (a)-[:KNOWS {since: 2021}]->(c)")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Delete one relationship
        engine
            .execute(
                &parser
                    .parse("MATCH (a:Node {name: 'A'})-[r:KNOWS {since: 2020}]->() DELETE r")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Deleted edge should not be found via property query
        let result = engine
            .execute(
                &parser
                    .parse("MATCH ()-[r:KNOWS {since: 2020}]->() RETURN r.since AS since")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 0, "deleted edge should not be found via index");

        // Non-deleted edge should still be found
        let result = engine
            .execute(
                &parser
                    .parse("MATCH ()-[r:KNOWS {since: 2021}]->() RETURN r.since AS since")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1, "non-deleted edge should still be found");
        assert_eq!(
            result.rows[0].get("since").and_then(|v| v.as_i64()),
            Some(2021)
        );
    }
