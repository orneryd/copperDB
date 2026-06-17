use copperdb_cypher::{Clause, Expression, Parser};

#[test]
fn validate_accepts_supported_query() {
    let parser = Parser::new();
    parser
        .validate("MATCH (n:Person) WHERE n.age > 25 RETURN n")
        .expect("validation should succeed for supported query");
}

#[test]
fn validate_rejects_invalid_query() {
    let parser = Parser::new();
    let error = parser
        .validate("MATCH (n RETURN n")
        .expect_err("validation should fail for malformed query");
    assert!(error.to_string().contains("expected ')'"));
}

#[test]
fn validate_shallow_checks_start_and_balance() {
    let parser = Parser::new();
    parser
        .validate_shallow("MATCH (n:Person) RETURN n")
        .expect("shallow validation should accept balanced query");

    let error = parser
        .validate_shallow("NOT_A_CLAUSE (n) RETURN n")
        .expect_err("shallow validation should reject invalid start clause");
    assert!(error.to_string().contains("valid clause"));

    let error = parser
        .validate_shallow("MATCH (n RETURN n")
        .expect_err("shallow validation should reject unbalanced query");
    assert!(error.to_string().contains("unbalanced parentheses"));
}

#[test]
fn parse_simple_match_return_fast_path_shape() {
    let parser = Parser::new();
    let query = parser
        .parse("MATCH (p:Person) RETURN p LIMIT 10")
        .expect("simple raw MATCH RETURN query should parse");

    assert_eq!(query.clauses.len(), 2);
    match &query.clauses[0] {
        Clause::Match(match_clause) => {
            assert_eq!(match_clause.pattern.nodes.len(), 1);
            assert_eq!(match_clause.pattern.nodes[0].variable.as_deref(), Some("p"));
            assert_eq!(match_clause.pattern.nodes[0].labels, vec!["Person"]);
        }
        other => panic!("expected MATCH clause, got {other:?}"),
    }
    match &query.clauses[1] {
        Clause::Return(return_clause) => {
            assert_eq!(return_clause.limit, Some(10));
            assert_eq!(return_clause.items.len(), 1);
            assert!(matches!(
                &return_clause.items[0].expression,
                Expression::Variable(name) if name == "p"
            ));
        }
        other => panic!("expected RETURN clause, got {other:?}"),
    }
}

#[test]
fn validate_and_parse_with_order_skip_limit() {
    let parser = Parser::new();
    let query_text = "MATCH (n) WITH n AS person WHERE person.age > 18 ORDER BY person.name DESC SKIP 1 LIMIT 2 RETURN person";

    parser
        .validate(query_text)
        .expect("validation should accept WITH ORDER BY SKIP LIMIT");

    let query = parser
        .parse(query_text)
        .expect("parser should accept WITH ORDER BY SKIP LIMIT");

    let with_clause = query
        .clauses
        .iter()
        .find_map(|clause| match clause {
            Clause::With(with_clause) => Some(with_clause),
            _ => None,
        })
        .expect("expected WITH clause");

    assert_eq!(with_clause.order_by.len(), 1);
    assert!(with_clause.order_by[0].descending);
    assert_eq!(with_clause.skip, Some(1));
    assert_eq!(with_clause.limit, Some(2));
    assert!(with_clause.where_clause.is_some());
}
