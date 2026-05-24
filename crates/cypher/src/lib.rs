//! Cypher query language parser and AST for copperdb.
//!
//! Hand-rolled recursive-descent parser for a subset of the openCypher grammar,
//! equivalent to the ANTLR4-based parser in NornicDB (Go).
//!
//! # v1.0.40 (Kiyote) — hot-path optimization
//!
//! All regex-based keyword detection and pattern matching has been replaced with
//! the scanner-based modules introduced in NornicDB v1.0.40:
//!
//! - [`keyword_scan`] — allocation-free keyword finder (respects string
//!   literals, comments, nested parentheses)
//! - [`string_patterns`] — `split_by_keyword`, `extract_limit/skip`,
//!   `parse_aggregation`, `extract_parameters` — all without regex
//! - [`query_patterns`] — pre-execution pattern detection for optimised routing
//! - [`hot_path_trace`] — per-query hot-path execution tracing

// ─── Sub-modules (NornicDB v1.0.40 additions) ─────────────────────────────────
pub mod hot_path_trace;
pub mod keyword_scan;
pub mod query_patterns;
pub mod string_patterns;

use std::collections::HashMap;

use serde_json::Value;
use thiserror::Error;

// ─── Errors ───────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum CypherError {
    #[error("parse error: {0}")]
    ParseError(String),
    #[error("empty query")]
    EmptyQuery,
    #[error("unterminated string")]
    UnterminatedString,
}

// ─── Query types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum QueryType {
    Match,
    Create,
    Merge,
    Delete,
    Set,
    Return,
    With,
    Ddl,
}

#[derive(Debug, Clone)]
pub struct Query {
    pub query_type: QueryType,
    pub clauses: Vec<Clause>,
    pub parameters: HashMap<String, Value>,
}

// ─── Clauses ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Clause {
    Match(MatchClause),
    OptionalMatch(MatchClause),
    Return(ReturnClause),
    Where(WhereClause),
    Set(SetClause),
    Delete(DeleteClause),
    Merge(MergeClause),
    With(WithClause),
    Unwind(UnwindClause),
    Create(CreateClause),
    CreateConstraint(CreateConstraintClause),
    DropConstraint(DropConstraintClause),
    ShowConstraints(ShowConstraintsClause),
    CreateIndex(CreateIndexClause),
    DropIndex(DropIndexClause),
    ShowIndexes(ShowIndexesClause),
    CreateDecayProfile(CreateDecayProfileClause),
    AlterDecayProfile(AlterDecayProfileClause),
    DropDecayProfile(DropDecayProfileClause),
    ShowDecayProfiles(ShowDecayProfilesClause),
    CreatePromotionProfile(CreatePromotionProfileClause),
    AlterPromotionProfile(AlterPromotionProfileClause),
    DropPromotionProfile(DropPromotionProfileClause),
    ShowPromotionProfiles(ShowPromotionProfilesClause),
    CreatePromotionPolicy(CreatePromotionPolicyClause),
    AlterPromotionPolicy(AlterPromotionPolicyClause),
    DropPromotionPolicy(DropPromotionPolicyClause),
    ShowPromotionPolicies(ShowPromotionPoliciesClause),
}

#[derive(Debug, Clone)]
pub struct MatchClause {
    pub pattern: Pattern,
    pub optional: bool,
}

#[derive(Debug, Clone)]
pub struct CreateClause {
    pub pattern: Pattern,
}

#[derive(Debug, Clone)]
pub struct MergeClause {
    pub pattern: Pattern,
}

#[derive(Debug, Clone)]
pub struct ReturnClause {
    pub items: Vec<ReturnItem>,
    pub order_by: Vec<OrderItem>,
    pub skip: Option<i64>,
    pub limit: Option<i64>,
    pub distinct: bool,
}

#[derive(Debug, Clone)]
pub struct WhereClause {
    pub expression: Expression,
}

#[derive(Debug, Clone)]
pub struct SetClause {
    pub items: Vec<SetItem>,
}

#[derive(Debug, Clone)]
pub struct DeleteClause {
    pub variables: Vec<String>,
    pub detach: bool,
}

#[derive(Debug, Clone)]
pub struct WithClause {
    pub items: Vec<ReturnItem>,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug, Clone)]
pub struct UnwindClause {
    pub expression: Expression,
    pub variable: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstraintKind {
    Unique,
    Exists,
}

#[derive(Debug, Clone)]
pub struct CreateConstraintClause {
    pub name: String,
    pub if_not_exists: bool,
    pub label: String,
    pub property: String,
    pub kind: ConstraintKind,
}

#[derive(Debug, Clone)]
pub struct DropConstraintClause {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct ShowConstraintsClause;

#[derive(Debug, Clone)]
pub struct CreateIndexClause {
    pub name: String,
    pub if_not_exists: bool,
    pub label: String,
    pub properties: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DropIndexClause {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct ShowIndexesClause;

#[derive(Debug, Clone)]
pub struct CreateDecayProfileClause {
    pub name: String,
    pub options: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct AlterDecayProfileClause {
    pub name: String,
    pub options: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct DropDecayProfileClause {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct ShowDecayProfilesClause;

#[derive(Debug, Clone)]
pub struct CreatePromotionProfileClause {
    pub name: String,
    pub options: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct AlterPromotionProfileClause {
    pub name: String,
    pub options: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct DropPromotionProfileClause {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct ShowPromotionProfilesClause;

#[derive(Debug, Clone)]
pub struct PromotionWhenClause {
    pub profile_ref: String,
    pub predicate: String,
    pub order: i64,
}

#[derive(Debug, Clone)]
pub struct CreatePromotionPolicyClause {
    pub name: String,
    pub target_labels: Vec<String>,
    pub enabled: bool,
    pub when_clauses: Vec<PromotionWhenClause>,
}

#[derive(Debug, Clone)]
pub struct AlterPromotionPolicyClause {
    pub name: String,
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct DropPromotionPolicyClause {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct ShowPromotionPoliciesClause;

// ─── Patterns ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Pattern {
    pub nodes: Vec<NodePattern>,
    pub edges: Vec<EdgePattern>,
}

#[derive(Debug, Clone)]
pub struct NodePattern {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub properties: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct EdgePattern {
    pub variable: Option<String>,
    pub rel_type: Option<String>,
    pub direction: EdgeDirection,
    pub properties: HashMap<String, Value>,
    pub min_hops: Option<u32>,
    pub max_hops: Option<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EdgeDirection {
    Both,
    Outgoing,
    Incoming,
}

// ─── Expressions ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Expression {
    PropertyAccess {
        variable: String,
        property: String,
    },
    Comparison {
        left: Box<Expression>,
        op: String,
        right: Box<Expression>,
    },
    Literal(Value),
    Parameter(String),
    FunctionCall {
        name: String,
        args: Vec<Expression>,
        distinct: bool,
    },
    Variable(String),
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
    IsNull(Box<Expression>),
    IsNotNull(Box<Expression>),
}

// ─── Return / Order / Set items ───────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ReturnItem {
    pub expression: Expression,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OrderItem {
    pub expression: Expression,
    pub descending: bool,
}

#[derive(Debug, Clone)]
pub struct SetItem {
    pub variable: String,
    pub property: String,
    pub value: Expression,
}

// ─── Tokenizer ────────────────────────────────────────────────────────────────

const SINGLE_CHAR_TOKENS: &[char] = &[
    '(', ')', '[', ']', '{', '}', ':', ',', '.', '=', '<', '>', '-', '+', '*', '/',
];

/// Tokenize a Cypher string.
///
/// Rules:
/// - Split on whitespace.
/// - Split on single-char tokens (see `SINGLE_CHAR_TOKENS`), each becomes its own token.
/// - Quoted strings (`'…'` or `"…"`) are kept as a single token including the quotes.
///   String scanning uses the same escape-aware logic as [`keyword_scan`] so that
///   keywords inside quoted values are never mistaken for clause delimiters.
/// - `<>`, `<=`, `>=` are kept as two-character tokens.
pub fn tokenize(input: &str) -> Result<Vec<String>, CypherError> {
    use crate::keyword_scan::is_ascii_space;

    let sb = input.as_bytes();
    let len = sb.len();
    let mut tokens: Vec<String> = Vec::new();
    let mut i = 0;

    while i < len {
        let b = sb[i];

        // Skip whitespace (using scanner helper for consistency)
        if is_ascii_space(b) {
            i += 1;
            continue;
        }

        // Quoted string — use escape-aware scanning (same as keyword_scan)
        if b == b'\'' || b == b'"' {
            let quote = b;
            let mut s = String::new();
            s.push(b as char);
            i += 1;
            loop {
                if i >= len {
                    return Err(CypherError::UnterminatedString);
                }
                let c = sb[i];
                if c == b'\\' && i + 1 < len {
                    s.push(c as char);
                    s.push(sb[i + 1] as char);
                    i += 2;
                    continue;
                }
                if c == quote {
                    // SQL-style doubled quote ('Alice''s')
                    if i + 1 < len && sb[i + 1] == quote {
                        s.push(c as char);
                        s.push(c as char);
                        i += 2;
                        continue;
                    }
                    s.push(c as char);
                    i += 1;
                    break;
                }
                s.push(c as char);
                i += 1;
            }
            tokens.push(s);
            continue;
        }

        // Two-character comparison operators
        if i + 1 < len {
            let pair = (b, sb[i + 1]);
            if matches!(
                pair,
                (b'<', b'>') | (b'<', b'=') | (b'>', b'=') | (b'!', b'=')
            ) {
                tokens.push(format!("{}{}", b as char, sb[i + 1] as char));
                i += 2;
                continue;
            }
        }

        // Single-char tokens (ASCII fast path)
        if b.is_ascii() && SINGLE_CHAR_TOKENS.contains(&(b as char)) {
            tokens.push((b as char).to_string());
            i += 1;
            continue;
        }

        // Word / identifier / number — accumulate until whitespace or single-char token.
        let start = i;
        while i < len {
            let c = sb[i];
            if is_ascii_space(c) {
                break;
            }
            if c.is_ascii() && SINGLE_CHAR_TOKENS.contains(&(c as char)) {
                break;
            }
            i += 1;
        }
        if i > start {
            tokens.push(input[start..i].to_owned());
        }
    }

    Ok(tokens)
}

// ─── Parser ───────────────────────────────────────────────────────────────────

pub struct Parser;

impl Parser {
    pub fn new() -> Self {
        Parser
    }

    pub fn parse(&self, cypher: &str) -> Result<Query, CypherError> {
        if cypher.trim().is_empty() {
            return Err(CypherError::EmptyQuery);
        }

        let tokens = tokenize(cypher)?;
        if tokens.is_empty() {
            return Err(CypherError::EmptyQuery);
        }

        let mut ctx = ParseContext::new(tokens);
        ctx.parse_query()
    }
}

impl Default for Parser {
    fn default() -> Self {
        Parser::new()
    }
}

// ─── Parse context ────────────────────────────────────────────────────────────

struct ParseContext {
    tokens: Vec<String>,
    pos: usize,
}

impl ParseContext {
    fn new(tokens: Vec<String>) -> Self {
        ParseContext { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&str> {
        self.tokens.get(self.pos).map(|s| s.as_str())
    }

    fn peek_upper(&self) -> Option<String> {
        self.peek().map(|s| s.to_uppercase())
    }

    fn advance(&mut self) -> Option<&str> {
        let t = self.tokens.get(self.pos).map(|s| s.as_str());
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn expect(&mut self, expected: &str) -> Result<(), CypherError> {
        match self.advance() {
            Some(t) if t.eq_ignore_ascii_case(expected) => Ok(()),
            Some(t) => Err(CypherError::ParseError(format!(
                "expected '{}', got '{}'",
                expected, t
            ))),
            None => Err(CypherError::ParseError(format!(
                "expected '{}', got end of input",
                expected
            ))),
        }
    }

    // ── Top-level dispatcher ─────────────────────────────────────────────────

    fn parse_query(&mut self) -> Result<Query, CypherError> {
        let mut clauses: Vec<Clause> = Vec::new();

        while self.pos < self.tokens.len() {
            let upper = match self.peek_upper() {
                Some(u) => u,
                None => break,
            };

            match upper.as_str() {
                "MATCH" => {
                    self.advance();
                    let clause = self.parse_match(false)?;
                    clauses.push(Clause::Match(clause));
                }
                "OPTIONAL" => {
                    self.advance();
                    self.expect("MATCH")?;
                    let clause = self.parse_match(true)?;
                    clauses.push(Clause::OptionalMatch(clause));
                }
                "CREATE" => {
                    self.advance();
                    match self.peek_upper().as_deref() {
                        Some("CONSTRAINT") => {
                            self.advance();
                            clauses.push(Clause::CreateConstraint(self.parse_create_constraint()?));
                        }
                        Some("INDEX") => {
                            self.advance();
                            clauses.push(Clause::CreateIndex(self.parse_create_index()?));
                        }
                        Some("DECAY") => {
                            self.advance();
                            clauses.push(Clause::CreateDecayProfile(
                                self.parse_create_decay_profile()?,
                            ));
                        }
                        Some("PROMOTION") => {
                            self.advance();
                            match self.peek_upper().as_deref() {
                                Some("PROFILE") => {
                                    self.advance();
                                    clauses.push(Clause::CreatePromotionProfile(
                                        self.parse_create_promotion_profile()?,
                                    ));
                                }
                                Some("POLICY") => {
                                    self.advance();
                                    clauses.push(Clause::CreatePromotionPolicy(
                                        self.parse_create_promotion_policy()?,
                                    ));
                                }
                                Some(other) => {
                                    return Err(CypherError::ParseError(format!(
                                        "unsupported CREATE PROMOTION target '{}'",
                                        other
                                    )));
                                }
                                None => {
                                    return Err(CypherError::ParseError(
                                        "expected CREATE PROMOTION target, got end of input"
                                            .into(),
                                    ));
                                }
                            }
                        }
                        _ => {
                            let clause = self.parse_create()?;
                            clauses.push(Clause::Create(clause));
                        }
                    }
                }
                "MERGE" => {
                    self.advance();
                    let clause = self.parse_merge()?;
                    clauses.push(Clause::Merge(clause));
                }
                "RETURN" => {
                    self.advance();
                    let clause = self.parse_return()?;
                    clauses.push(Clause::Return(clause));
                }
                "WHERE" => {
                    self.advance();
                    let clause = self.parse_where()?;
                    clauses.push(Clause::Where(clause));
                }
                "SET" => {
                    self.advance();
                    let clause = self.parse_set()?;
                    clauses.push(Clause::Set(clause));
                }
                "DELETE" => {
                    self.advance();
                    let clause = self.parse_delete(false)?;
                    clauses.push(Clause::Delete(clause));
                }
                "DETACH" => {
                    self.advance();
                    self.expect("DELETE")?;
                    let clause = self.parse_delete(true)?;
                    clauses.push(Clause::Delete(clause));
                }
                "WITH" => {
                    self.advance();
                    let clause = self.parse_with()?;
                    clauses.push(Clause::With(clause));
                }
                "UNWIND" => {
                    self.advance();
                    let clause = self.parse_unwind()?;
                    clauses.push(Clause::Unwind(clause));
                }
                "DROP" => {
                    self.advance();
                    match self.peek_upper().as_deref() {
                        Some("CONSTRAINT") => {
                            self.advance();
                            clauses.push(Clause::DropConstraint(self.parse_drop_constraint()?));
                        }
                        Some("INDEX") => {
                            self.advance();
                            clauses.push(Clause::DropIndex(self.parse_drop_index()?));
                        }
                        Some("DECAY") => {
                            self.advance();
                            clauses.push(Clause::DropDecayProfile(self.parse_drop_decay_profile()?));
                        }
                        Some("PROMOTION") => {
                            self.advance();
                            match self.peek_upper().as_deref() {
                                Some("PROFILE") => {
                                    self.advance();
                                    clauses.push(Clause::DropPromotionProfile(
                                        self.parse_drop_promotion_profile()?,
                                    ));
                                }
                                Some("POLICY") => {
                                    self.advance();
                                    clauses.push(Clause::DropPromotionPolicy(
                                        self.parse_drop_promotion_policy()?,
                                    ));
                                }
                                Some(other) => {
                                    return Err(CypherError::ParseError(format!(
                                        "unsupported DROP PROMOTION target '{}'",
                                        other
                                    )));
                                }
                                None => {
                                    return Err(CypherError::ParseError(
                                        "expected DROP PROMOTION target, got end of input".into(),
                                    ));
                                }
                            }
                        }
                        Some(other) => {
                            return Err(CypherError::ParseError(format!(
                                "unsupported DROP target '{}'",
                                other
                            )));
                        }
                        None => {
                            return Err(CypherError::ParseError(
                                "expected DROP target, got end of input".into(),
                            ));
                        }
                    }
                }
                "SHOW" => {
                    self.advance();
                    match self.peek_upper().as_deref() {
                        Some("CONSTRAINTS") => {
                            self.advance();
                            clauses.push(Clause::ShowConstraints(self.parse_show_constraints()?));
                        }
                        Some("INDEXES") => {
                            self.advance();
                            clauses.push(Clause::ShowIndexes(self.parse_show_indexes()?));
                        }
                        Some("DECAY") => {
                            self.advance();
                            clauses.push(Clause::ShowDecayProfiles(self.parse_show_decay_profiles()?));
                        }
                        Some("PROMOTION") => {
                            self.advance();
                            match self.peek_upper().as_deref() {
                                Some("PROFILES") => {
                                    self.advance();
                                    clauses.push(Clause::ShowPromotionProfiles(
                                        self.parse_show_promotion_profiles()?,
                                    ));
                                }
                                Some("POLICIES") => {
                                    self.advance();
                                    clauses.push(Clause::ShowPromotionPolicies(
                                        self.parse_show_promotion_policies()?,
                                    ));
                                }
                                Some(other) => {
                                    return Err(CypherError::ParseError(format!(
                                        "unsupported SHOW PROMOTION target '{}'",
                                        other
                                    )));
                                }
                                None => {
                                    return Err(CypherError::ParseError(
                                        "expected SHOW PROMOTION target, got end of input".into(),
                                    ));
                                }
                            }
                        }
                        Some(other) => {
                            return Err(CypherError::ParseError(format!(
                                "unsupported SHOW target '{}'",
                                other
                            )));
                        }
                        None => {
                            return Err(CypherError::ParseError(
                                "expected SHOW target, got end of input".into(),
                            ));
                        }
                    }
                }
                "ALTER" => {
                    self.advance();
                    match self.peek_upper().as_deref() {
                        Some("DECAY") => {
                            self.advance();
                            clauses.push(Clause::AlterDecayProfile(self.parse_alter_decay_profile()?));
                        }
                        Some("PROMOTION") => {
                            self.advance();
                            match self.peek_upper().as_deref() {
                                Some("PROFILE") => {
                                    self.advance();
                                    clauses.push(Clause::AlterPromotionProfile(
                                        self.parse_alter_promotion_profile()?,
                                    ));
                                }
                                Some("POLICY") => {
                                    self.advance();
                                    clauses.push(Clause::AlterPromotionPolicy(
                                        self.parse_alter_promotion_policy()?,
                                    ));
                                }
                                Some(other) => {
                                    return Err(CypherError::ParseError(format!(
                                        "unsupported ALTER PROMOTION target '{}'",
                                        other
                                    )));
                                }
                                None => {
                                    return Err(CypherError::ParseError(
                                        "expected ALTER PROMOTION target, got end of input".into(),
                                    ));
                                }
                            }
                        }
                        Some(other) => {
                            return Err(CypherError::ParseError(format!(
                                "unsupported ALTER target '{}'",
                                other
                            )));
                        }
                        None => {
                            return Err(CypherError::ParseError(
                                "expected ALTER target, got end of input".into(),
                            ));
                        }
                    }
                }
                _ => {
                    return Err(CypherError::ParseError(format!(
                        "unexpected token '{}'",
                        upper
                    )));
                }
            }
        }

        if clauses.is_empty() {
            return Err(CypherError::ParseError("no recognisable clause".into()));
        }

        let query_type = dominant_query_type(&clauses);

        Ok(Query {
            query_type,
            clauses,
            parameters: HashMap::new(),
        })
    }

    // ── MATCH ────────────────────────────────────────────────────────────────

    fn parse_match(&mut self, optional: bool) -> Result<MatchClause, CypherError> {
        let pattern = self.parse_pattern()?;
        // WHERE is handled at the top-level dispatcher as a standalone Clause::Where
        Ok(MatchClause { pattern, optional })
    }

    // ── CREATE ───────────────────────────────────────────────────────────────

    fn parse_create(&mut self) -> Result<CreateClause, CypherError> {
        let pattern = self.parse_pattern()?;
        Ok(CreateClause { pattern })
    }

    // ── MERGE ────────────────────────────────────────────────────────────────

    fn parse_merge(&mut self) -> Result<MergeClause, CypherError> {
        let pattern = self.parse_pattern()?;
        Ok(MergeClause { pattern })
    }

    // ── RETURN ───────────────────────────────────────────────────────────────

    fn parse_return(&mut self) -> Result<ReturnClause, CypherError> {
        let distinct = if self.peek_upper().as_deref() == Some("DISTINCT") {
            self.advance();
            true
        } else {
            false
        };

        let mut items: Vec<ReturnItem> = Vec::new();
        items.push(self.parse_return_item()?);
        while self.peek() == Some(",") {
            self.advance(); // consume ','
            items.push(self.parse_return_item()?);
        }

        // Optional ORDER BY
        let mut order_by: Vec<OrderItem> = Vec::new();
        if self.peek_upper().as_deref() == Some("ORDER") {
            self.advance();
            self.expect("BY")?;
            order_by.push(self.parse_order_item()?);
            while self.peek() == Some(",") {
                self.advance();
                order_by.push(self.parse_order_item()?);
            }
        }

        // Optional SKIP / LIMIT (in any order)
        let mut skip: Option<i64> = None;
        let mut limit: Option<i64> = None;
        loop {
            match self.peek_upper().as_deref() {
                Some("SKIP") => {
                    self.advance();
                    skip = Some(self.parse_i64()?);
                }
                Some("LIMIT") => {
                    self.advance();
                    limit = Some(self.parse_i64()?);
                }
                _ => break,
            }
        }

        Ok(ReturnClause {
            items,
            order_by,
            skip,
            limit,
            distinct,
        })
    }

    fn parse_return_item(&mut self) -> Result<ReturnItem, CypherError> {
        let expression = self.parse_expression()?;
        let alias = if self.peek_upper().as_deref() == Some("AS") {
            self.advance();
            Some(self.advance_identifier()?)
        } else {
            None
        };
        Ok(ReturnItem { expression, alias })
    }

    fn parse_order_item(&mut self) -> Result<OrderItem, CypherError> {
        let expression = self.parse_expression()?;
        let descending = matches!(
            self.peek_upper().as_deref(),
            Some("DESC") | Some("DESCENDING")
        );
        if descending {
            self.advance();
        } else if matches!(
            self.peek_upper().as_deref(),
            Some("ASC") | Some("ASCENDING")
        ) {
            self.advance();
        }
        Ok(OrderItem {
            expression,
            descending,
        })
    }

    fn parse_i64(&mut self) -> Result<i64, CypherError> {
        match self.advance() {
            Some(t) => t
                .parse::<i64>()
                .map_err(|_| CypherError::ParseError(format!("expected integer, got '{}'", t))),
            None => Err(CypherError::ParseError(
                "expected integer, got end of input".into(),
            )),
        }
    }

    // ── WHERE ────────────────────────────────────────────────────────────────

    fn parse_where(&mut self) -> Result<WhereClause, CypherError> {
        let expression = self.parse_expression()?;
        Ok(WhereClause { expression })
    }

    // ── SET ──────────────────────────────────────────────────────────────────

    fn parse_set(&mut self) -> Result<SetClause, CypherError> {
        let mut items: Vec<SetItem> = Vec::new();
        items.push(self.parse_set_item()?);
        while self.peek() == Some(",") {
            self.advance();
            items.push(self.parse_set_item()?);
        }
        Ok(SetClause { items })
    }

    fn parse_set_item(&mut self) -> Result<SetItem, CypherError> {
        let variable = self.advance_identifier()?;
        self.expect(".")?;
        let property = self.advance_identifier()?;
        self.expect("=")?;
        let value = self.parse_expression()?;
        Ok(SetItem {
            variable,
            property,
            value,
        })
    }

    // ── DELETE ───────────────────────────────────────────────────────────────

    fn parse_delete(&mut self, detach: bool) -> Result<DeleteClause, CypherError> {
        let mut variables: Vec<String> = Vec::new();
        variables.push(self.advance_identifier()?);
        while self.peek() == Some(",") {
            self.advance();
            variables.push(self.advance_identifier()?);
        }
        Ok(DeleteClause { variables, detach })
    }

    // ── WITH ─────────────────────────────────────────────────────────────────

    fn parse_with(&mut self) -> Result<WithClause, CypherError> {
        let mut items: Vec<ReturnItem> = Vec::new();
        items.push(self.parse_return_item()?);
        while self.peek() == Some(",") {
            self.advance();
            items.push(self.parse_return_item()?);
        }

        let where_clause = if self.peek_upper().as_deref() == Some("WHERE") {
            self.advance();
            Some(self.parse_where()?)
        } else {
            None
        };

        Ok(WithClause {
            items,
            where_clause,
        })
    }

    // ── UNWIND ───────────────────────────────────────────────────────────────

    fn parse_unwind(&mut self) -> Result<UnwindClause, CypherError> {
        let expression = self.parse_expression()?;
        self.expect("AS")?;
        let variable = self.advance_identifier()?;
        Ok(UnwindClause {
            expression,
            variable,
        })
    }

    fn parse_create_constraint(&mut self) -> Result<CreateConstraintClause, CypherError> {
        let name = self.advance_identifier()?;
        let if_not_exists = self.consume_if_not_exists()?;
        self.expect("FOR")?;
        self.expect("(")?;
        let variable = self.advance_identifier()?;
        self.expect(":")?;
        let label = self.advance_identifier()?;
        self.expect(")")?;
        self.expect("REQUIRE")?;
        let (prop_variable, property) = self.parse_qualified_property()?;
        if prop_variable != variable {
            return Err(CypherError::ParseError(format!(
                "constraint variable mismatch: expected '{}', got '{}'",
                variable, prop_variable
            )));
        }
        self.expect("IS")?;

        let kind = match self.peek_upper().as_deref() {
            Some("UNIQUE") => {
                self.advance();
                ConstraintKind::Unique
            }
            Some("NOT") => {
                self.advance();
                self.expect("NULL")?;
                ConstraintKind::Exists
            }
            Some(other) => {
                return Err(CypherError::ParseError(format!(
                    "unsupported constraint predicate '{}'",
                    other
                )));
            }
            None => {
                return Err(CypherError::ParseError(
                    "expected constraint predicate, got end of input".into(),
                ));
            }
        };

        Ok(CreateConstraintClause {
            name,
            if_not_exists,
            label,
            property,
            kind,
        })
    }

    fn parse_drop_constraint(&mut self) -> Result<DropConstraintClause, CypherError> {
        let name = self.advance_identifier()?;
        let if_exists = self.consume_if_exists()?;
        Ok(DropConstraintClause { name, if_exists })
    }

    fn parse_show_constraints(&mut self) -> Result<ShowConstraintsClause, CypherError> {
        if self.peek().is_some() {
            return Err(CypherError::ParseError(format!(
                "unexpected token '{}' after SHOW CONSTRAINTS",
                self.peek().unwrap_or_default()
            )));
        }
        Ok(ShowConstraintsClause)
    }

    fn parse_create_index(&mut self) -> Result<CreateIndexClause, CypherError> {
        let name = self.advance_identifier()?;
        let if_not_exists = self.consume_if_not_exists()?;
        self.expect("FOR")?;
        self.expect("(")?;
        let variable = self.advance_identifier()?;
        self.expect(":")?;
        let label = self.advance_identifier()?;
        self.expect(")")?;
        self.expect("ON")?;
        self.expect("(")?;
        let mut properties = Vec::new();
        loop {
            let (prop_variable, property) = self.parse_qualified_property()?;
            if prop_variable != variable {
                return Err(CypherError::ParseError(format!(
                    "index variable mismatch: expected '{}', got '{}'",
                    variable, prop_variable
                )));
            }
            properties.push(property);
            if self.peek() == Some(",") {
                self.advance();
                continue;
            }
            break;
        }
        self.expect(")")?;
        if properties.is_empty() {
            return Err(CypherError::ParseError(
                "index definition must include at least one property".into(),
            ));
        }
        Ok(CreateIndexClause {
            name,
            if_not_exists,
            label,
            properties,
        })
    }

    fn parse_drop_index(&mut self) -> Result<DropIndexClause, CypherError> {
        let name = self.advance_identifier()?;
        let if_exists = self.consume_if_exists()?;
        Ok(DropIndexClause { name, if_exists })
    }

    fn parse_show_indexes(&mut self) -> Result<ShowIndexesClause, CypherError> {
        if self.peek().is_some() {
            return Err(CypherError::ParseError(format!(
                "unexpected token '{}' after SHOW INDEXES",
                self.peek().unwrap_or_default()
            )));
        }
        Ok(ShowIndexesClause)
    }

    fn parse_create_decay_profile(&mut self) -> Result<CreateDecayProfileClause, CypherError> {
        self.expect("PROFILE")?;
        let name = self.advance_identifier()?;
        self.expect("OPTIONS")?;
        let options = self.parse_options_map()?;
        Ok(CreateDecayProfileClause { name, options })
    }

    fn parse_alter_decay_profile(&mut self) -> Result<AlterDecayProfileClause, CypherError> {
        self.expect("PROFILE")?;
        let name = self.advance_identifier()?;
        self.expect("SET")?;
        self.expect("OPTIONS")?;
        let options = self.parse_options_map()?;
        Ok(AlterDecayProfileClause { name, options })
    }

    fn parse_drop_decay_profile(&mut self) -> Result<DropDecayProfileClause, CypherError> {
        self.expect("PROFILE")?;
        let name = self.advance_identifier()?;
        let if_exists = self.consume_if_exists()?;
        Ok(DropDecayProfileClause { name, if_exists })
    }

    fn parse_show_decay_profiles(&mut self) -> Result<ShowDecayProfilesClause, CypherError> {
        self.expect("PROFILES")?;
        if self.peek().is_some() {
            return Err(CypherError::ParseError(format!(
                "unexpected token '{}' after SHOW DECAY PROFILES",
                self.peek().unwrap_or_default()
            )));
        }
        Ok(ShowDecayProfilesClause)
    }

    fn parse_create_promotion_profile(
        &mut self,
    ) -> Result<CreatePromotionProfileClause, CypherError> {
        let name = self.advance_identifier()?;
        self.expect("OPTIONS")?;
        let options = self.parse_options_map()?;
        Ok(CreatePromotionProfileClause { name, options })
    }

    fn parse_alter_promotion_profile(&mut self) -> Result<AlterPromotionProfileClause, CypherError> {
        let name = self.advance_identifier()?;
        self.expect("SET")?;
        self.expect("OPTIONS")?;
        let options = self.parse_options_map()?;
        Ok(AlterPromotionProfileClause { name, options })
    }

    fn parse_drop_promotion_profile(&mut self) -> Result<DropPromotionProfileClause, CypherError> {
        let name = self.advance_identifier()?;
        let if_exists = self.consume_if_exists()?;
        Ok(DropPromotionProfileClause { name, if_exists })
    }

    fn parse_show_promotion_profiles(
        &mut self,
    ) -> Result<ShowPromotionProfilesClause, CypherError> {
        self.expect("PROFILES")?;
        if self.peek().is_some() {
            return Err(CypherError::ParseError(format!(
                "unexpected token '{}' after SHOW PROMOTION PROFILES",
                self.peek().unwrap_or_default()
            )));
        }
        Ok(ShowPromotionProfilesClause)
    }

    fn parse_create_promotion_policy(&mut self) -> Result<CreatePromotionPolicyClause, CypherError> {
        let name = self.advance_identifier()?;
        self.expect("FOR")?;
        self.expect("(")?;
        self.advance_identifier()?;
        let mut target_labels = Vec::new();
        while self.peek() == Some(":") {
            self.advance();
            target_labels.push(self.advance_identifier()?);
        }
        self.expect(")")?;
        if target_labels.is_empty() {
            return Err(CypherError::ParseError(
                "promotion policy target labels are required".into(),
            ));
        }
        self.expect("APPLY")?;
        self.expect("PROFILE")?;
        let profile_ref = self.advance_identifier()?;
        let mut predicate = "true".to_string();
        if self.peek_upper().as_deref() == Some("WHEN") {
            self.advance();
            let token = self
                .advance()
                .ok_or_else(|| CypherError::ParseError("expected predicate after WHEN".into()))?;
            predicate = trim_quotes(token).to_string();
        }
        let when_clauses = vec![PromotionWhenClause {
            profile_ref,
            predicate,
            order: 1,
        }];
        Ok(CreatePromotionPolicyClause {
            name,
            target_labels,
            enabled: true,
            when_clauses,
        })
    }

    fn parse_alter_promotion_policy(&mut self) -> Result<AlterPromotionPolicyClause, CypherError> {
        let name = self.advance_identifier()?;
        self.expect("SET")?;
        self.expect("ENABLED")?;
        let token = self.advance().ok_or_else(|| {
            CypherError::ParseError("expected boolean value after SET ENABLED".into())
        })?;
        let enabled = parse_bool_token(token).ok_or_else(|| {
            CypherError::ParseError(format!("invalid boolean value '{}'", token))
        })?;
        Ok(AlterPromotionPolicyClause { name, enabled })
    }

    fn parse_drop_promotion_policy(&mut self) -> Result<DropPromotionPolicyClause, CypherError> {
        let name = self.advance_identifier()?;
        let if_exists = self.consume_if_exists()?;
        Ok(DropPromotionPolicyClause { name, if_exists })
    }

    fn parse_show_promotion_policies(
        &mut self,
    ) -> Result<ShowPromotionPoliciesClause, CypherError> {
        self.expect("POLICIES")?;
        if self.peek().is_some() {
            return Err(CypherError::ParseError(format!(
                "unexpected token '{}' after SHOW PROMOTION POLICIES",
                self.peek().unwrap_or_default()
            )));
        }
        Ok(ShowPromotionPoliciesClause)
    }

    fn parse_options_map(&mut self) -> Result<HashMap<String, Value>, CypherError> {
        self.expect("{")?;
        let mut options = HashMap::new();
        while self.peek() != Some("}") {
            let key = self.advance_identifier()?;
            self.expect(":")?;
            let value = self.parse_option_value()?;
            options.insert(key, value);
            if self.peek() == Some(",") {
                self.advance();
                continue;
            }
            if self.peek() != Some("}") {
                return Err(CypherError::ParseError(format!(
                    "expected ',' or '}}' in options map, got '{}'",
                    self.peek().unwrap_or_default()
                )));
            }
        }
        self.expect("}")?;
        Ok(options)
    }

    fn parse_option_value(&mut self) -> Result<Value, CypherError> {
        let token = self
            .advance()
            .ok_or_else(|| CypherError::ParseError("expected option value".into()))?;
        if token == "[" {
            let mut values = Vec::new();
            while self.peek() != Some("]") {
                values.push(self.parse_option_value()?);
                if self.peek() == Some(",") {
                    self.advance();
                } else if self.peek() != Some("]") {
                    return Err(CypherError::ParseError(format!(
                        "expected ',' or ']' in array value, got '{}'",
                        self.peek().unwrap_or_default()
                    )));
                }
            }
            self.expect("]")?;
            return Ok(Value::Array(values));
        }
        if let Some(bool_value) = parse_bool_token(token) {
            return Ok(Value::Bool(bool_value));
        }
        if let Ok(i) = token.parse::<i64>() {
            return Ok(Value::from(i));
        }
        if let Ok(f) = token.parse::<f64>() {
            return Ok(Value::from(f));
        }
        Ok(Value::String(trim_quotes(token).to_string()))
    }

    fn consume_if_not_exists(&mut self) -> Result<bool, CypherError> {
        if self.peek_upper().as_deref() == Some("IF") {
            self.advance();
            self.expect("NOT")?;
            self.expect("EXISTS")?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn consume_if_exists(&mut self) -> Result<bool, CypherError> {
        if self.peek_upper().as_deref() == Some("IF") {
            self.advance();
            self.expect("EXISTS")?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn parse_qualified_property(&mut self) -> Result<(String, String), CypherError> {
        let variable = self.advance_identifier()?;
        self.expect(".")?;
        let property = self.advance_identifier()?;
        Ok((variable, property))
    }

    // ── Pattern ──────────────────────────────────────────────────────────────

    fn parse_pattern(&mut self) -> Result<Pattern, CypherError> {
        let mut nodes: Vec<NodePattern> = Vec::new();
        let mut edges: Vec<EdgePattern> = Vec::new();

        // Parse one or more comma-separated path segments: `(a), (b)-[r]->(c), …`
        loop {
            // Expect at least one node
            self.expect("(")?;
            nodes.push(self.parse_node_inner()?);

            // Try to parse edges: `-[…]->` / `<-[…]-` / `-[…]-`
            loop {
                // Check for relationship arrow start: `-` or `<`
                let next = self.peek();
                if next == Some("-") || next == Some("<") {
                    if let Some(edge) = self.try_parse_edge()? {
                        edges.push(edge);
                        // Next must be a node
                        self.expect("(")?;
                        nodes.push(self.parse_node_inner()?);
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }

            // Comma separates additional path segments: `MATCH (a), (b)`
            if self.peek() == Some(",") {
                self.advance(); // consume ','
            } else {
                break;
            }
        }

        Ok(Pattern { nodes, edges })
    }

    /// Parse the interior of a `(…)` – variable, labels, properties.
    fn parse_node_inner(&mut self) -> Result<NodePattern, CypherError> {
        let mut variable: Option<String> = None;
        let mut labels: Vec<String> = Vec::new();
        let mut properties: HashMap<String, Value> = HashMap::new();

        // Variable name (optional – not a keyword, not `:` or `)`)
        if let Some(t) = self.peek() {
            if t != ")" && t != ":" && t != "{" && !is_keyword(t) {
                variable = Some(self.advance().unwrap().to_string());
            }
        }

        // Labels: `:Label` (may repeat)
        while self.peek() == Some(":") {
            self.advance(); // consume `:`
            let label = self.advance_identifier()?;
            labels.push(label);
        }

        // Properties: `{key: value, …}`
        if self.peek() == Some("{") {
            self.advance(); // consume `{`
            properties = self.parse_properties_map()?;
        }

        self.expect(")")?;

        Ok(NodePattern {
            variable,
            labels,
            properties,
        })
    }

    /// Try to parse an edge pattern. Returns `None` if it isn't a valid edge start.
    fn try_parse_edge(&mut self) -> Result<Option<EdgePattern>, CypherError> {
        let saved_pos = self.pos;

        // Determine direction prefix: `<-` (incoming) or `-` (out/both)
        let prefix_incoming = if self.peek() == Some("<") {
            self.advance(); // consume `<`
            if self.peek() != Some("-") {
                self.pos = saved_pos;
                return Ok(None);
            }
            self.advance(); // consume `-`
            true
        } else {
            self.advance(); // consume `-`
            false
        };

        // If next is not `[`, this isn't a proper edge – e.g. just stray `-`
        if self.peek() != Some("[") {
            self.pos = saved_pos;
            return Ok(None);
        }
        self.advance(); // consume `[`

        let edge = self.parse_edge_inner(prefix_incoming)?;

        self.expect("]")?;

        // Direction suffix: `->` (outgoing) or `-` (both/incoming)
        let suffix_arrow = if self.peek() == Some("-") {
            self.advance(); // consume `-`
            if self.peek() == Some(">") {
                self.advance(); // consume `>`
                true
            } else {
                false
            }
        } else {
            self.pos = saved_pos;
            return Ok(None);
        };

        let direction = if prefix_incoming && !suffix_arrow {
            EdgeDirection::Incoming
        } else if !prefix_incoming && suffix_arrow {
            EdgeDirection::Outgoing
        } else {
            EdgeDirection::Both
        };

        Ok(Some(EdgePattern { direction, ..edge }))
    }

    /// Parse the interior of `[…]` for a relationship.
    fn parse_edge_inner(&mut self, _prefix_incoming: bool) -> Result<EdgePattern, CypherError> {
        let mut variable: Option<String> = None;
        let mut rel_type: Option<String> = None;
        let mut properties: HashMap<String, Value> = HashMap::new();
        let mut min_hops: Option<u32> = None;
        let mut max_hops: Option<u32> = None;

        // Variable (optional)
        if let Some(t) = self.peek() {
            if t != "]" && t != ":" && t != "*" && t != "{" {
                variable = Some(self.advance().unwrap().to_string());
            }
        }

        // Relationship type: `:TYPE`
        if self.peek() == Some(":") {
            self.advance();
            rel_type = Some(self.advance_identifier()?);
        }

        // Variable-length: `*min..max` or `*n` or `*`
        // The tokenizer splits `1..3` into `1`, `.`, `.`, `3`
        if self.peek() == Some("*") {
            self.advance(); // consume `*`
                            // Try to read optional min value
            if let Some(t) = self.peek() {
                if t != "]" && t != "{" {
                    if let Ok(min) = t.parse::<u32>() {
                        self.advance(); // consume min
                        min_hops = Some(min);
                        // Check for `..` (two separate `.` tokens from tokenizer)
                        if self.peek() == Some(".") {
                            self.advance(); // consume first `.`
                            if self.peek() == Some(".") {
                                self.advance(); // consume second `.`
                                if let Some(max_t) = self.peek() {
                                    if let Ok(max) = max_t.parse::<u32>() {
                                        self.advance();
                                        max_hops = Some(max);
                                    }
                                }
                            }
                        } else {
                            max_hops = Some(min);
                        }
                    }
                }
            }
        }

        // Properties map
        if self.peek() == Some("{") {
            self.advance();
            properties = self.parse_properties_map()?;
        }

        Ok(EdgePattern {
            variable,
            rel_type,
            direction: EdgeDirection::Both, // overwritten by caller
            properties,
            min_hops,
            max_hops,
        })
    }

    /// Parse `key: value, …` until `}`.
    fn parse_properties_map(&mut self) -> Result<HashMap<String, Value>, CypherError> {
        let mut map = HashMap::new();
        while self.peek() != Some("}") && self.peek().is_some() {
            let key = self.advance_identifier()?;
            self.expect(":")?;
            let val = self.parse_json_value()?;
            map.insert(key, val);
            if self.peek() == Some(",") {
                self.advance();
            }
        }
        self.expect("}")?;
        Ok(map)
    }

    /// Parse a literal JSON-compatible value from tokens.
    fn parse_json_value(&mut self) -> Result<Value, CypherError> {
        match self.peek() {
            Some(t) if t.starts_with('\'') || t.starts_with('"') => {
                let raw = self.advance().unwrap().to_string();
                if raw.len() < 2 {
                    return Err(CypherError::UnterminatedString);
                }
                let s = raw[1..raw.len() - 1].to_string();
                Ok(Value::String(s))
            }
            Some(t) if t.eq_ignore_ascii_case("true") => {
                self.advance();
                Ok(Value::Bool(true))
            }
            Some(t) if t.eq_ignore_ascii_case("false") => {
                self.advance();
                Ok(Value::Bool(false))
            }
            Some(t) if t.eq_ignore_ascii_case("null") => {
                self.advance();
                Ok(Value::Null)
            }
            Some(t) => {
                let t = t.to_string();
                self.advance();
                if let Ok(i) = t.parse::<i64>() {
                    Ok(Value::Number(i.into()))
                } else if let Ok(f) = t.parse::<f64>() {
                    let n = serde_json::Number::from_f64(f).ok_or_else(|| {
                        CypherError::ParseError(format!("invalid float value '{}'", t))
                    })?;
                    Ok(Value::Number(n))
                } else {
                    Err(CypherError::ParseError(format!(
                        "unexpected value token '{}'",
                        t
                    )))
                }
            }
            None => Err(CypherError::ParseError(
                "expected value, got end of input".into(),
            )),
        }
    }

    // ── Expressions ──────────────────────────────────────────────────────────

    /// Entry: parse `OR` level.
    fn parse_expression(&mut self) -> Result<Expression, CypherError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expression, CypherError> {
        let mut left = self.parse_and()?;
        while self.peek_upper().as_deref() == Some("OR") {
            self.advance();
            let right = self.parse_and()?;
            left = Expression::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expression, CypherError> {
        let mut left = self.parse_not()?;
        while self.peek_upper().as_deref() == Some("AND") {
            self.advance();
            let right = self.parse_not()?;
            left = Expression::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expression, CypherError> {
        if self.peek_upper().as_deref() == Some("NOT") {
            self.advance();
            let expr = self.parse_not()?;
            return Ok(Expression::Not(Box::new(expr)));
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Expression, CypherError> {
        let left = self.parse_primary()?;

        // IS NULL / IS NOT NULL
        if self.peek_upper().as_deref() == Some("IS") {
            self.advance();
            if self.peek_upper().as_deref() == Some("NOT") {
                self.advance();
                self.expect("NULL")?;
                return Ok(Expression::IsNotNull(Box::new(left)));
            }
            self.expect("NULL")?;
            return Ok(Expression::IsNull(Box::new(left)));
        }

        // Keyword comparison operators: CONTAINS, STARTS WITH, ENDS WITH
        if let Some(kw) = self.peek_upper() {
            match kw.as_str() {
                "CONTAINS" => {
                    self.advance();
                    let right = self.parse_primary()?;
                    return Ok(Expression::Comparison {
                        left: Box::new(left),
                        op: "CONTAINS".to_string(),
                        right: Box::new(right),
                    });
                }
                "STARTS" => {
                    self.advance();
                    self.expect("WITH")?;
                    let right = self.parse_primary()?;
                    return Ok(Expression::Comparison {
                        left: Box::new(left),
                        op: "STARTS WITH".to_string(),
                        right: Box::new(right),
                    });
                }
                "ENDS" => {
                    self.advance();
                    self.expect("WITH")?;
                    let right = self.parse_primary()?;
                    return Ok(Expression::Comparison {
                        left: Box::new(left),
                        op: "ENDS WITH".to_string(),
                        right: Box::new(right),
                    });
                }
                _ => {}
            }
        }

        // Symbolic comparison operators
        let op = match self.peek() {
            Some("=") | Some("<") | Some(">") | Some("<=") | Some(">=") | Some("<>") => {
                self.advance().unwrap().to_string()
            }
            // != is an alias for <> — tokenizer emits it as a single token if present
            Some("!=") => {
                self.advance();
                "<>".to_string()
            }
            _ => return Ok(left),
        };

        let right = self.parse_primary()?;
        Ok(Expression::Comparison {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }

    fn parse_primary(&mut self) -> Result<Expression, CypherError> {
        match self.peek() {
            // Quoted string literal – tokenizer guarantees at least opening+closing quote
            Some(t) if t.starts_with('\'') || t.starts_with('"') => {
                let raw = self.advance().unwrap().to_string();
                if raw.len() < 2 {
                    return Err(CypherError::UnterminatedString);
                }
                let s = raw[1..raw.len() - 1].to_string();
                Ok(Expression::Literal(Value::String(s)))
            }
            // Parameter: `$name`
            Some(t) if t.starts_with('$') => {
                let raw = self.advance().unwrap().to_string();
                Ok(Expression::Parameter(raw[1..].to_string()))
            }
            // Parenthesised expression
            Some("(") => {
                self.advance();
                let expr = self.parse_expression()?;
                self.expect(")")?;
                Ok(expr)
            }
            // Keyword literals
            Some(t) if t.eq_ignore_ascii_case("true") => {
                self.advance();
                Ok(Expression::Literal(Value::Bool(true)))
            }
            Some(t) if t.eq_ignore_ascii_case("false") => {
                self.advance();
                Ok(Expression::Literal(Value::Bool(false)))
            }
            Some(t) if t.eq_ignore_ascii_case("null") => {
                self.advance();
                Ok(Expression::Literal(Value::Null))
            }
            // Number: integer or float
            Some(t)
                if t.chars()
                    .next()
                    .map(|c| c.is_ascii_digit())
                    .unwrap_or(false) =>
            {
                let t = self.advance().unwrap().to_string();
                if t.contains('.') {
                    let f: f64 = t
                        .parse()
                        .map_err(|_| CypherError::ParseError(format!("invalid float '{}'", t)))?;
                    let n = serde_json::Number::from_f64(f).ok_or_else(|| {
                        CypherError::ParseError(format!("invalid float value '{}'", t))
                    })?;
                    Ok(Expression::Literal(Value::Number(n)))
                } else {
                    let i: i64 = t
                        .parse()
                        .map_err(|_| CypherError::ParseError(format!("invalid integer '{}'", t)))?;
                    Ok(Expression::Literal(Value::Number(i.into())))
                }
            }
            // Identifier: variable, property access, or function call
            Some(_) => {
                let name = self.advance().unwrap().to_string();

                // Function call: `name(`
                if self.peek() == Some("(") {
                    self.advance(); // consume `(`
                    let distinct = if self.peek_upper().as_deref() == Some("DISTINCT") {
                        self.advance();
                        true
                    } else {
                        false
                    };
                    let mut args: Vec<Expression> = Vec::new();
                    if self.peek() != Some(")") {
                        args.push(self.parse_expression()?);
                        while self.peek() == Some(",") {
                            self.advance();
                            args.push(self.parse_expression()?);
                        }
                    }
                    self.expect(")")?;
                    return Ok(Expression::FunctionCall {
                        name,
                        args,
                        distinct,
                    });
                }

                // Property access: `var.prop`
                if self.peek() == Some(".") {
                    self.advance(); // consume `.`
                    let property = self.advance_identifier()?;
                    return Ok(Expression::PropertyAccess {
                        variable: name,
                        property,
                    });
                }

                Ok(Expression::Variable(name))
            }
            None => Err(CypherError::ParseError(
                "unexpected end of expression".into(),
            )),
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /// Advance and return the next token as an identifier (not a keyword).
    fn advance_identifier(&mut self) -> Result<String, CypherError> {
        match self.advance() {
            Some(t) => Ok(t.to_string()),
            None => Err(CypherError::ParseError(
                "expected identifier, got end of input".into(),
            )),
        }
    }
}

/// Returns `true` if `s` is an openCypher keyword that cannot be a bare variable name.
fn trim_quotes(token: &str) -> &str {
    if (token.starts_with('\'') && token.ends_with('\'')) || (token.starts_with('"') && token.ends_with('"')) {
        &token[1..token.len().saturating_sub(1)]
    } else {
        token
    }
}

fn parse_bool_token(token: &str) -> Option<bool> {
    match token.to_ascii_uppercase().as_str() {
        "TRUE" => Some(true),
        "FALSE" => Some(false),
        _ => None,
    }
}

/// Returns `true` if `s` is an openCypher keyword that cannot be a bare variable name.
fn is_keyword(s: &str) -> bool {
    matches!(
        s.to_uppercase().as_str(),
        "MATCH"
            | "OPTIONAL"
            | "CREATE"
            | "RETURN"
            | "WHERE"
            | "SET"
            | "DELETE"
            | "DETACH"
            | "WITH"
            | "MERGE"
            | "UNWIND"
            | "ORDER"
            | "BY"
            | "LIMIT"
            | "SKIP"
            | "AS"
            | "AND"
            | "OR"
            | "NOT"
            | "NULL"
            | "TRUE"
            | "FALSE"
            | "IS"
            | "IN"
            | "DISTINCT"
            | "ASC"
            | "DESC"
            | "ASCENDING"
            | "DESCENDING"
            | "CONTAINS"
            | "STARTS"
            | "ENDS"
            | "DROP"
            | "SHOW"
            | "CONSTRAINT"
            | "CONSTRAINTS"
            | "INDEX"
            | "INDEXES"
            | "ALTER"
            | "DECAY"
            | "PROFILE"
            | "PROFILES"
            | "PROMOTION"
            | "POLICY"
            | "POLICIES"
            | "FOR"
            | "APPLY"
            | "REQUIRE"
            | "UNIQUE"
            | "EXISTS"
            | "ON"
            | "OPTIONS"
            | "ENABLED"
            | "WHEN"
    )
}

/// Determine the dominant `QueryType` from all parsed clauses.
///
/// Priority (highest wins): Delete > Set > Merge > Create > Match > With > Return
fn dominant_query_type(clauses: &[Clause]) -> QueryType {
    fn priority(c: &Clause) -> u8 {
        match c {
            Clause::CreateConstraint(_)
            | Clause::DropConstraint(_)
            | Clause::ShowConstraints(_)
            | Clause::CreateIndex(_)
            | Clause::DropIndex(_)
            | Clause::ShowIndexes(_)
            | Clause::CreateDecayProfile(_)
            | Clause::AlterDecayProfile(_)
            | Clause::DropDecayProfile(_)
            | Clause::ShowDecayProfiles(_)
            | Clause::CreatePromotionProfile(_)
            | Clause::AlterPromotionProfile(_)
            | Clause::DropPromotionProfile(_)
            | Clause::ShowPromotionProfiles(_)
            | Clause::CreatePromotionPolicy(_)
            | Clause::AlterPromotionPolicy(_)
            | Clause::DropPromotionPolicy(_)
            | Clause::ShowPromotionPolicies(_) => 7,
            Clause::Delete(_) => 6,
            Clause::Set(_) => 5,
            Clause::Merge(_) => 4,
            Clause::Create(_) => 3,
            Clause::Match(_) | Clause::OptionalMatch(_) => 2,
            Clause::With(_) => 1,
            _ => 0,
        }
    }

    let best = clauses.iter().max_by_key(|c| priority(c));
    match best {
        Some(Clause::CreateConstraint(_))
        | Some(Clause::DropConstraint(_))
        | Some(Clause::ShowConstraints(_))
        | Some(Clause::CreateIndex(_))
        | Some(Clause::DropIndex(_))
        | Some(Clause::ShowIndexes(_))
        | Some(Clause::CreateDecayProfile(_))
        | Some(Clause::AlterDecayProfile(_))
        | Some(Clause::DropDecayProfile(_))
        | Some(Clause::ShowDecayProfiles(_))
        | Some(Clause::CreatePromotionProfile(_))
        | Some(Clause::AlterPromotionProfile(_))
        | Some(Clause::DropPromotionProfile(_))
        | Some(Clause::ShowPromotionProfiles(_))
        | Some(Clause::CreatePromotionPolicy(_))
        | Some(Clause::AlterPromotionPolicy(_))
        | Some(Clause::DropPromotionPolicy(_))
        | Some(Clause::ShowPromotionPolicies(_)) => QueryType::Ddl,
        Some(Clause::Delete(_)) => QueryType::Delete,
        Some(Clause::Set(_)) => QueryType::Set,
        Some(Clause::Merge(_)) => QueryType::Merge,
        Some(Clause::Create(_)) => QueryType::Create,
        Some(Clause::Match(_) | Clause::OptionalMatch(_)) => QueryType::Match,
        Some(Clause::With(_)) => QueryType::With,
        _ => QueryType::Return,
    }
}

// ─── Executor ─────────────────────────────────────────────────────────────────

pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<HashMap<String, Value>>,
}

impl QueryResult {
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
}

pub struct Executor {
    parser: Parser,
}

impl Executor {
    pub fn new() -> Self {
        Executor {
            parser: Parser::new(),
        }
    }

    /// Parse `cypher`, apply optional `params`, and return an (empty) result set.
    pub fn execute(
        &self,
        _ctx: &(),
        cypher: &str,
        params: Option<HashMap<String, Value>>,
    ) -> Result<QueryResult, CypherError> {
        let mut query = self.parser.parse(cypher)?;
        if let Some(p) = params {
            query.parameters = p;
        }
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        })
    }
}

impl Default for Executor {
    fn default() -> Self {
        Executor::new()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_parser() {
        let _p = Parser::new();
    }

    #[test]
    fn test_parse_empty_query() {
        let p = Parser::new();
        assert!(p.parse("").is_err());
    }

    #[test]
    fn test_parse_match_simple() {
        let p = Parser::new();
        let q = p.parse("MATCH (n) RETURN n").unwrap();
        assert!(matches!(q.query_type, QueryType::Match));
    }

    #[test]
    fn test_parse_match_with_label() {
        let p = Parser::new();
        let q = p.parse("MATCH (n:Person) RETURN n").unwrap();
        assert!(matches!(q.query_type, QueryType::Match));
    }

    #[test]
    fn test_parse_match_with_relationship() {
        let p = Parser::new();
        let q = p.parse("MATCH (a)-[r]->(b) RETURN a, b").unwrap();
        assert!(matches!(q.query_type, QueryType::Match));
    }

    #[test]
    fn test_parse_optional_match() {
        let p = Parser::new();
        let q = p.parse("OPTIONAL MATCH (n) RETURN n").unwrap();
        assert!(matches!(q.query_type, QueryType::Match));
    }

    #[test]
    fn test_parse_create_simple() {
        let p = Parser::new();
        let q = p.parse("CREATE (n)").unwrap();
        assert!(matches!(q.query_type, QueryType::Create));
    }

    #[test]
    fn test_parse_create_with_label() {
        let p = Parser::new();
        let q = p.parse("CREATE (n:Person)").unwrap();
        assert!(matches!(q.query_type, QueryType::Create));
    }

    #[test]
    fn test_parse_create_with_properties() {
        let p = Parser::new();
        let q = p.parse("CREATE (n:Person {name: 'Alice'})").unwrap();
        assert!(matches!(q.query_type, QueryType::Create));
    }

    #[test]
    fn test_parse_return_clause() {
        let p = Parser::new();
        let q = p.parse("MATCH (n) RETURN n").unwrap();
        let has_return = q.clauses.iter().any(|c| matches!(c, Clause::Return(_)));
        assert!(has_return);
    }

    #[test]
    fn test_parse_where_clause() {
        let p = Parser::new();
        let q = p
            .parse("MATCH (n) WHERE n.name = 'Alice' RETURN n")
            .unwrap();
        let has_where = q.clauses.iter().any(|c| matches!(c, Clause::Where(_)));
        assert!(has_where);
    }

    #[test]
    fn test_parse_set() {
        let p = Parser::new();
        let q = p.parse("MATCH (n) SET n.name = 'Bob' RETURN n").unwrap();
        assert!(matches!(q.query_type, QueryType::Set));
    }

    #[test]
    fn test_parse_delete() {
        let p = Parser::new();
        let q = p.parse("MATCH (n) DELETE n").unwrap();
        assert!(matches!(q.query_type, QueryType::Delete));
    }

    #[test]
    fn test_parse_detach_delete() {
        let p = Parser::new();
        let q = p.parse("MATCH (n) DETACH DELETE n").unwrap();
        assert!(matches!(q.query_type, QueryType::Delete));
        let has_detach = q.clauses.iter().any(|c| {
            if let Clause::Delete(d) = c {
                d.detach
            } else {
                false
            }
        });
        assert!(has_detach);
    }

    #[test]
    fn test_tokenize_simple() {
        let tokens = tokenize("MATCH (n)").unwrap();
        assert_eq!(tokens, vec!["MATCH", "(", "n", ")"]);
    }

    #[test]
    fn test_tokenize_with_label() {
        let tokens = tokenize("MATCH (n:Person)").unwrap();
        assert_eq!(tokens, vec!["MATCH", "(", "n", ":", "Person", ")"]);
    }

    #[test]
    fn test_tokenize_with_string() {
        let tokens = tokenize("CREATE (n {name: 'Alice'})").unwrap();
        assert_eq!(
            tokens,
            vec!["CREATE", "(", "n", "{", "name", ":", "'Alice'", "}", ")"]
        );
    }

    #[test]
    fn test_tokenize_with_relationship() {
        let tokens = tokenize("MATCH (a)-[r]->(b)").unwrap();
        assert_eq!(
            tokens,
            vec!["MATCH", "(", "a", ")", "-", "[", "r", "]", "-", ">", "(", "b", ")"]
        );
    }

    #[test]
    fn test_parse_complex_query() {
        let p = Parser::new();
        let cypher = "MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText) \
                      WHERE t.language = 'fr' \
                      RETURN o, t, t.createdAt \
                      ORDER BY t.createdAt DESC LIMIT 10";
        let q = p.parse(cypher).unwrap();
        assert!(matches!(q.query_type, QueryType::Match));
        let has_where = q.clauses.iter().any(|c| matches!(c, Clause::Where(_)));
        let has_return = q.clauses.iter().any(|c| matches!(c, Clause::Return(_)));
        assert!(has_where);
        assert!(has_return);
    }

    #[test]
    fn test_executor_basic() {
        let exec = Executor::new();
        let result = exec.execute(&(), "MATCH (n) RETURN n", None).unwrap();
        assert_eq!(result.row_count(), 0);
    }

    #[test]
    fn test_executor_empty_query_fails() {
        let exec = Executor::new();
        assert!(exec.execute(&(), "", None).is_err());
    }

    #[test]
    fn test_node_pattern_variable() {
        let p = NodePattern {
            variable: Some("n".into()),
            labels: vec![],
            properties: Default::default(),
        };
        assert_eq!(p.variable, Some("n".into()));
    }

    #[test]
    fn test_edge_direction() {
        assert!(matches!(EdgeDirection::Outgoing, EdgeDirection::Outgoing));
        assert!(matches!(EdgeDirection::Incoming, EdgeDirection::Incoming));
        assert!(matches!(EdgeDirection::Both, EdgeDirection::Both));
    }

    // ── Extra coverage ────────────────────────────────────────────────────────

    #[test]
    fn test_parse_match_multiple_labels() {
        let p = Parser::new();
        let q = p.parse("MATCH (n:Person:Employee) RETURN n").unwrap();
        assert!(matches!(q.query_type, QueryType::Match));
        if let Some(Clause::Match(m)) = q.clauses.first() {
            let node = &m.pattern.nodes[0];
            assert_eq!(node.labels, vec!["Person", "Employee"]);
        } else {
            panic!("expected Match clause");
        }
    }

    #[test]
    fn test_parse_property_access_in_where() {
        let p = Parser::new();
        let q = p.parse("MATCH (n) WHERE n.age > 18 RETURN n").unwrap();
        let where_clause = q.clauses.iter().find_map(|c| {
            if let Clause::Where(w) = c {
                Some(w)
            } else {
                None
            }
        });
        assert!(where_clause.is_some());
        assert!(matches!(
            where_clause.unwrap().expression,
            Expression::Comparison { .. }
        ));
    }

    #[test]
    fn test_parse_return_with_alias() {
        let p = Parser::new();
        let q = p.parse("MATCH (n) RETURN n.name AS name").unwrap();
        if let Some(Clause::Return(r)) = q.clauses.iter().find(|c| matches!(c, Clause::Return(_))) {
            assert_eq!(r.items[0].alias, Some("name".into()));
        } else {
            panic!("expected Return clause");
        }
    }

    #[test]
    fn test_parse_return_with_limit() {
        let p = Parser::new();
        let q = p.parse("MATCH (n) RETURN n LIMIT 5").unwrap();
        if let Some(Clause::Return(r)) = q.clauses.iter().find(|c| matches!(c, Clause::Return(_))) {
            assert_eq!(r.limit, Some(5));
        } else {
            panic!("expected Return clause");
        }
    }

    #[test]
    fn test_parse_function_call() {
        let p = Parser::new();
        let q = p.parse("MATCH (n) RETURN count(n) AS total").unwrap();
        if let Some(Clause::Return(r)) = q.clauses.iter().find(|c| matches!(c, Clause::Return(_))) {
            assert!(matches!(
                r.items[0].expression,
                Expression::FunctionCall { .. }
            ));
        } else {
            panic!("expected Return clause");
        }
    }

    #[test]
    fn test_parse_incoming_relationship() {
        let p = Parser::new();
        let q = p.parse("MATCH (a)<-[r:KNOWS]-(b) RETURN a").unwrap();
        assert!(matches!(q.query_type, QueryType::Match));
        if let Some(Clause::Match(m)) = q.clauses.first() {
            assert_eq!(m.pattern.edges[0].direction, EdgeDirection::Incoming);
        } else {
            panic!("expected Match clause");
        }
    }

    #[test]
    fn test_parse_variable_length_relationship() {
        let p = Parser::new();
        let q = p.parse("MATCH (a)-[r*1..3]->(b) RETURN a").unwrap();
        assert!(matches!(q.query_type, QueryType::Match));
        if let Some(Clause::Match(m)) = q.clauses.first() {
            let edge = &m.pattern.edges[0];
            assert_eq!(edge.min_hops, Some(1));
            assert_eq!(edge.max_hops, Some(3));
        } else {
            panic!("expected Match clause");
        }
    }

    #[test]
    fn test_parse_and_expression() {
        let p = Parser::new();
        let q = p
            .parse("MATCH (n) WHERE n.name = 'Alice' AND n.age > 18 RETURN n")
            .unwrap();
        let where_clause = q.clauses.iter().find_map(|c| {
            if let Clause::Where(w) = c {
                Some(w)
            } else {
                None
            }
        });
        assert!(matches!(
            where_clause.unwrap().expression,
            Expression::And(_, _)
        ));
    }

    #[test]
    fn test_parse_merge() {
        let p = Parser::new();
        let q = p.parse("MERGE (n:Person {name: 'Alice'})").unwrap();
        assert!(matches!(q.query_type, QueryType::Merge));
    }

    #[test]
    fn test_parse_where_contains() {
        let p = Parser::new();
        let q = p
            .parse("MATCH (n) WHERE n.name CONTAINS 'Ali' RETURN n")
            .unwrap();
        let where_clause = q.clauses.iter().find_map(|c| {
            if let Clause::Where(w) = c {
                Some(w)
            } else {
                None
            }
        });
        assert!(matches!(
            where_clause.unwrap().expression,
            Expression::Comparison { ref op, .. } if op == "CONTAINS"
        ));
    }

    #[test]
    fn test_parse_where_starts_with() {
        let p = Parser::new();
        let q = p
            .parse("MATCH (n) WHERE n.name STARTS WITH 'Al' RETURN n")
            .unwrap();
        let where_clause = q.clauses.iter().find_map(|c| {
            if let Clause::Where(w) = c {
                Some(w)
            } else {
                None
            }
        });
        assert!(matches!(
            where_clause.unwrap().expression,
            Expression::Comparison { ref op, .. } if op == "STARTS WITH"
        ));
    }

    #[test]
    fn test_parse_where_ends_with() {
        let p = Parser::new();
        let q = p
            .parse("MATCH (n) WHERE n.name ENDS WITH 'ice' RETURN n")
            .unwrap();
        let where_clause = q.clauses.iter().find_map(|c| {
            if let Clause::Where(w) = c {
                Some(w)
            } else {
                None
            }
        });
        assert!(matches!(
            where_clause.unwrap().expression,
            Expression::Comparison { ref op, .. } if op == "ENDS WITH"
        ));
    }

    #[test]
    fn test_parse_where_not_equal() {
        let p = Parser::new();
        // != should be normalised to <>
        let q = p.parse("MATCH (n) WHERE n.age != 0 RETURN n").unwrap();
        let where_clause = q.clauses.iter().find_map(|c| {
            if let Clause::Where(w) = c {
                Some(w)
            } else {
                None
            }
        });
        assert!(matches!(
            where_clause.unwrap().expression,
            Expression::Comparison { ref op, .. } if op == "<>"
        ));
    }

    #[test]
    fn test_edge_missing_close_bracket_is_error() {
        let p = Parser::new();
        // Missing `]` — should produce a parse error, not silently desync
        let result = p.parse("MATCH (a)-[r:KNOWS-(b) RETURN a");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_create_unique_constraint() {
        let p = Parser::new();
        let q = p
            .parse(
                "CREATE CONSTRAINT person_email_unique IF NOT EXISTS FOR (n:Person) REQUIRE n.email IS UNIQUE",
            )
            .unwrap();
        assert!(matches!(q.query_type, QueryType::Ddl));
        if let Clause::CreateConstraint(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.name, "person_email_unique");
            assert!(c.if_not_exists);
            assert_eq!(c.label, "Person");
            assert_eq!(c.property, "email");
            assert!(matches!(c.kind, ConstraintKind::Unique));
        } else {
            panic!("expected CreateConstraint clause");
        }
    }

    #[test]
    fn test_parse_create_exists_constraint() {
        let p = Parser::new();
        let q = p
            .parse(
                "CREATE CONSTRAINT person_email_exists FOR (n:Person) REQUIRE n.email IS NOT NULL",
            )
            .unwrap();
        if let Clause::CreateConstraint(c) = q.clauses.first().expect("clause missing") {
            assert!(matches!(c.kind, ConstraintKind::Exists));
        } else {
            panic!("expected CreateConstraint clause");
        }
    }

    #[test]
    fn test_parse_create_constraint_variable_mismatch_errors() {
        let p = Parser::new();
        let err = p
            .parse("CREATE CONSTRAINT c FOR (n:Person) REQUIRE m.email IS UNIQUE")
            .unwrap_err();
        assert!(err.to_string().contains("constraint variable mismatch"));
    }

    #[test]
    fn test_parse_drop_constraint_if_exists() {
        let p = Parser::new();
        let q = p
            .parse("DROP CONSTRAINT person_email_unique IF EXISTS")
            .unwrap();
        if let Clause::DropConstraint(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.name, "person_email_unique");
            assert!(c.if_exists);
        } else {
            panic!("expected DropConstraint clause");
        }
    }

    #[test]
    fn test_parse_show_constraints() {
        let p = Parser::new();
        let q = p.parse("SHOW CONSTRAINTS").unwrap();
        assert!(matches!(
            q.clauses.first().expect("clause missing"),
            Clause::ShowConstraints(_)
        ));
    }

    #[test]
    fn test_parse_create_index() {
        let p = Parser::new();
        let q = p
            .parse("CREATE INDEX person_idx IF NOT EXISTS FOR (n:Person) ON (n.email, n.name)")
            .unwrap();
        if let Clause::CreateIndex(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.name, "person_idx");
            assert!(c.if_not_exists);
            assert_eq!(c.label, "Person");
            assert_eq!(c.properties, vec!["email", "name"]);
        } else {
            panic!("expected CreateIndex clause");
        }
    }

    #[test]
    fn test_parse_create_index_missing_on_errors() {
        let p = Parser::new();
        let err = p
            .parse("CREATE INDEX person_idx FOR (n:Person)")
            .unwrap_err();
        assert!(err.to_string().contains("expected 'ON'"));
    }

    #[test]
    fn test_parse_drop_index_if_exists() {
        let p = Parser::new();
        let q = p.parse("DROP INDEX person_idx IF EXISTS").unwrap();
        if let Clause::DropIndex(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.name, "person_idx");
            assert!(c.if_exists);
        } else {
            panic!("expected DropIndex clause");
        }
    }

    #[test]
    fn test_parse_show_indexes() {
        let p = Parser::new();
        let q = p.parse("SHOW INDEXES").unwrap();
        assert!(matches!(
            q.clauses.first().expect("clause missing"),
            Clause::ShowIndexes(_)
        ));
    }

    #[test]
    fn test_parse_create_decay_profile() {
        let p = Parser::new();
        let q = p
            .parse(
                "CREATE DECAY PROFILE slow_decay OPTIONS { halfLifeSeconds: 604800, visibilityThreshold: 0.1, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED' }",
            )
            .unwrap();
        assert!(matches!(q.query_type, QueryType::Ddl));
        if let Clause::CreateDecayProfile(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.name, "slow_decay");
            assert_eq!(c.options.get("halfLifeSeconds"), Some(&Value::from(604800)));
            assert_eq!(c.options.get("scope"), Some(&Value::String("NODE".to_string())));
        } else {
            panic!("expected CreateDecayProfile clause");
        }
    }

    #[test]
    fn test_parse_promotion_profile_and_policy_ddl() {
        let p = Parser::new();
        let create_profile = p
            .parse(
                "CREATE PROMOTION PROFILE boost_profile OPTIONS { scope: 'NODE', multiplier: 1.5, scoreFloor: 0.0, scoreCap: 1.0, enabled: true }",
            )
            .unwrap();
        assert!(matches!(
            create_profile.clauses.first().expect("clause missing"),
            Clause::CreatePromotionProfile(_)
        ));

        let create_policy = p
            .parse("CREATE PROMOTION POLICY fact_policy FOR (n:KnowledgeFact) APPLY PROFILE boost_profile WHEN 'n.evidence >= 3'")
            .unwrap();
        if let Clause::CreatePromotionPolicy(c) =
            create_policy.clauses.first().expect("clause missing")
        {
            assert_eq!(c.name, "fact_policy");
            assert_eq!(c.target_labels, vec!["KnowledgeFact"]);
            assert_eq!(c.when_clauses.len(), 1);
            assert_eq!(c.when_clauses[0].profile_ref, "boost_profile");
        } else {
            panic!("expected CreatePromotionPolicy clause");
        }
    }

    #[test]
    fn test_parse_alter_and_show_knowledge_policy_statements() {
        let p = Parser::new();
        let alter_decay = p
            .parse("ALTER DECAY PROFILE slow_decay SET OPTIONS { visibilityThreshold: 0.2 }")
            .unwrap();
        assert!(matches!(
            alter_decay.clauses.first().expect("clause missing"),
            Clause::AlterDecayProfile(_)
        ));

        let alter_policy = p
            .parse("ALTER PROMOTION POLICY fact_policy SET ENABLED false")
            .unwrap();
        assert!(matches!(
            alter_policy.clauses.first().expect("clause missing"),
            Clause::AlterPromotionPolicy(_)
        ));

        let show_decay = p.parse("SHOW DECAY PROFILES").unwrap();
        assert!(matches!(
            show_decay.clauses.first().expect("clause missing"),
            Clause::ShowDecayProfiles(_)
        ));
        let show_promo = p.parse("SHOW PROMOTION POLICIES").unwrap();
        assert!(matches!(
            show_promo.clauses.first().expect("clause missing"),
            Clause::ShowPromotionPolicies(_)
        ));
    }
}
