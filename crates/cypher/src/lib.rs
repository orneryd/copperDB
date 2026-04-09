//! Cypher query language parser and AST for magnetDB.
//!
//! Equivalent to Go's `pkg/cypher` in NornicDB (which uses ANTLR4).
//!
//! ⚠️ **Note on Implementation**: NornicDB uses an ANTLR4-generated parser via
//! `github.com/antlr4-go/antlr/v4`. There is no direct Rust ANTLR4 runtime.
//! This crate provides:
//! - A hand-crafted Cypher AST (Abstract Syntax Tree)
//! - A stub parser module that must be completed using `pest` or `lalrpop`
//!
//! See the README for implementation notes on the Cypher grammar.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CypherError {
    #[error("parse error at position {position}: {message}")]
    ParseError { position: usize, message: String },
    #[error("unsupported clause: {0}")]
    UnsupportedClause(String),
    #[error("semantic error: {0}")]
    SemanticError(String),
}

// ─── AST Nodes ────────────────────────────────────────────────────────────────

/// A complete Cypher query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    pub clauses: Vec<Clause>,
}

/// A single Cypher clause.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Clause {
    Match(MatchClause),
    OptionalMatch(MatchClause),
    Where(Expression),
    Return(ReturnClause),
    With(WithClause),
    Create(CreateClause),
    Merge(MergeClause),
    Set(SetClause),
    Delete(DeleteClause),
    Unwind(UnwindClause),
    Call(CallClause),
    OrderBy(Vec<SortItem>),
    Skip(Expression),
    Limit(Expression),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchClause {
    pub patterns: Vec<Pattern>,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pattern {
    pub elements: Vec<PatternElement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PatternElement {
    Node(NodePattern),
    Relationship(RelationshipPattern),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodePattern {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub properties: Option<Expression>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationshipPattern {
    pub variable: Option<String>,
    pub types: Vec<String>,
    pub direction: RelationshipDirection,
    pub length: Option<RangeQuantifier>,
    pub properties: Option<Expression>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RelationshipDirection {
    Outgoing,
    Incoming,
    Undirected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeQuantifier {
    pub min: Option<u32>,
    pub max: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReturnClause {
    pub distinct: bool,
    pub items: Vec<ReturnItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReturnItem {
    pub expression: Expression,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithClause {
    pub items: Vec<ReturnItem>,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateClause {
    pub patterns: Vec<Pattern>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeClause {
    pub pattern: Pattern,
    pub on_match: Vec<SetItem>,
    pub on_create: Vec<SetItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetClause {
    pub items: Vec<SetItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetItem {
    pub target: Expression,
    pub value: Expression,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteClause {
    pub detach: bool,
    pub expressions: Vec<Expression>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnwindClause {
    pub expression: Expression,
    pub variable: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallClause {
    pub procedure: String,
    pub args: Vec<Expression>,
    pub yield_items: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortItem {
    pub expression: Expression,
    pub descending: bool,
}

/// Cypher expression (recursive).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expression {
    Literal(Literal),
    Variable(String),
    Parameter(String),
    Property { base: Box<Expression>, key: String },
    FunctionCall { name: String, args: Vec<Expression>, distinct: bool },
    BinaryOp { op: BinaryOperator, left: Box<Expression>, right: Box<Expression> },
    UnaryOp { op: UnaryOperator, operand: Box<Expression> },
    IsNull(Box<Expression>),
    IsNotNull(Box<Expression>),
    In { value: Box<Expression>, list: Box<Expression> },
    ListLiteral(Vec<Expression>),
    MapLiteral(Vec<(String, Expression)>),
    Subscript { base: Box<Expression>, index: Box<Expression> },
    Slice { base: Box<Expression>, start: Option<Box<Expression>>, end: Option<Box<Expression>> },
    Labels(Box<Expression>),
    Keys(Box<Expression>),
    CaseExpression { operand: Option<Box<Expression>>, alternatives: Vec<(Expression, Expression)>, default: Option<Box<Expression>> },
    PatternComprehension { variable: Option<String>, pattern: Pattern, filter: Option<Box<Expression>>, projection: Box<Expression> },
    ListComprehension { variable: String, list: Box<Expression>, filter: Option<Box<Expression>>, projection: Option<Box<Expression>> },
    Exists(Pattern),
    Count { distinct: bool, expression: Option<Box<Expression>> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Literal {
    Integer(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BinaryOperator {
    Add, Sub, Mul, Div, Mod, Pow,
    Eq, Ne, Lt, Lte, Gt, Gte,
    And, Or, Xor,
    Contains, StartsWith, EndsWith, Matches,
    Concat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UnaryOperator {
    Neg,
    Not,
}

// ─── Parser (stub) ────────────────────────────────────────────────────────────

/// Parse a Cypher query string into an AST.
///
/// ⚠️ **Not yet implemented.** The parser must be built using `pest` with a
/// Cypher PEG grammar or via `lalrpop`. See `README.md` for details.
pub fn parse(input: &str) -> Result<Query, CypherError> {
    let _ = input;
    Err(CypherError::UnsupportedClause(
        "Cypher parser not yet implemented. \
         Implement using pest PEG grammar or lalrpop. \
         See README.md for details."
            .to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_returns_not_implemented() {
        let result = parse("MATCH (n) RETURN n");
        assert!(result.is_err());
    }

    #[test]
    fn test_ast_node_construction() {
        let node = NodePattern {
            variable: Some("n".into()),
            labels: vec!["Person".into()],
            properties: None,
        };
        assert_eq!(node.variable, Some("n".into()));
        assert_eq!(node.labels[0], "Person");
    }

    #[test]
    fn test_literal_serialization() {
        let lit = Literal::Integer(42);
        let json = serde_json::to_string(&lit).unwrap();
        assert!(json.contains("42"));
    }
}
