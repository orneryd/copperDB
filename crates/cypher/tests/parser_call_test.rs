use copperdb_cypher::{Clause, Parser};

#[test]
fn parse_call_procedure_clause() {
    let parser = Parser::new();
    let query = parser.parse("CALL db.labels()").expect("CALL should parse");

    assert_eq!(query.clauses.len(), 1);
    match &query.clauses[0] {
        Clause::Call(call) => {
            assert_eq!(call.procedure, "db.labels");
            assert!(call.args.is_empty());
        }
        other => panic!("expected CALL clause, got {other:?}"),
    }
}
