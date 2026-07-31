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
                    .parse("CREATE VECTOR INDEX person_embedding_idx FOR (n:Person) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3}}")
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
            .parse("CREATE VECTOR INDEX person_embedding_idx FOR (n:Person) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3}}")
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
                "CREATE VECTOR INDEX person_embedding_idx IF NOT EXISTS FOR (n:Person) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3}}",
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
                    .parse("CREATE VECTOR INDEX person_embedding_idx FOR (n:Person) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3}}")
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
