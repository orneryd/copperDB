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

pub mod ast;
mod compound_query_shape_matcher;
mod dispatcher;
pub mod executor;
mod expression_parser;
pub mod hot_path_trace;
pub mod keyword_scan;
mod parse_context;
pub mod parser;
mod parser_support;
mod pattern_parser;
pub mod pipeline_probe;
pub mod query_patterns;
pub mod shape_matcher;
pub mod string_patterns;
pub mod syntax_ir;
pub mod tokenizer;
mod validator;

use std::collections::HashMap;

pub use ast::*;
pub use compound_query_shape_matcher::{
    match_compound_prop_create_delete_return_count_rel_shape, match_compound_query_shape,
};
pub use executor::{Executor, QueryResult};
use parse_context::ParseContext;
pub use parser::Parser;
use parser_support::{parse_bool_token, trim_quotes};
pub use pipeline_probe::{
    can_execute_as_pipeline, pending_pipeline_execution_todo, PipelineClause, PipelineClauseKind,
};
pub use query_patterns::{detect_query_pattern, PatternInfo, QueryPattern};
use serde_json::Value;
pub use shape_matcher::{
    pending_shape_execution_todo, ShapeCaptures, ShapeKind, ShapeMatch, ShapeProbe, ShapeValue,
};
pub use syntax_ir::{
    parse_syntax, SyntaxClause, SyntaxClauseContent, SyntaxClauseKind, SyntaxExprKind,
    SyntaxExprRef, SyntaxNode, SyntaxOrderItem, SyntaxPattern, SyntaxQuery, SyntaxRelationship,
    SyntaxReturnItem, SyntaxSetItem,
};
use thiserror::Error;
pub use tokenizer::tokenize;

#[derive(Debug, Error)]
pub enum CypherError {
    #[error("parse error: {0}")]
    ParseError(String),
    #[error("empty query")]
    EmptyQuery,
    #[error("unterminated string")]
    UnterminatedString,
}

fn is_simple_expression_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return false;
    }
    bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

impl<'a> ParseContext<'a> {
    fn parse_pattern_with_optional_path_variable(
        &mut self,
        allow_shortest_path: bool,
    ) -> Result<Pattern, CypherError> {
        let path_variable = if self.tokens.get(self.pos + 1).copied() == Some("=") {
            let variable = self.advance_identifier()?;
            self.expect("=")?;
            Some(variable)
        } else {
            None
        };

        let mut pattern = if allow_shortest_path && self.peek_is("SHORTESTPATH") {
            self.advance();
            self.expect("(")?;
            let mut pattern = self.parse_pattern()?;
            self.expect(")")?;
            pattern.shortest_path = true;
            pattern
        } else if allow_shortest_path && self.peek_is("ALLSHORTESTPATHS") {
            self.advance();
            self.expect("(")?;
            let mut pattern = self.parse_pattern()?;
            self.expect(")")?;
            pattern.all_shortest_paths = true;
            pattern
        } else {
            self.parse_pattern()?
        };
        if pattern.shortest_path && pattern.segment_edge_counts.len() != 1 {
            return Err(CypherError::ParseError(
                "shortestPath requires a single connected pattern".to_string(),
            ));
        }
        if pattern.all_shortest_paths && pattern.segment_edge_counts.len() != 1 {
            return Err(CypherError::ParseError(
                "allShortestPaths requires a single connected pattern".to_string(),
            ));
        }
        if path_variable.is_some() && pattern.segment_edge_counts.len() != 1 {
            return Err(CypherError::ParseError(
                "path variables require a single connected pattern".to_string(),
            ));
        }
        pattern.path_variable = path_variable;
        Ok(pattern)
    }

    fn parse_match(&mut self, optional: bool) -> Result<MatchClause, CypherError> {
        let pattern = self.parse_pattern_with_optional_path_variable(true)?;
        // WHERE is handled at the top-level dispatcher as a standalone Clause::Where
        Ok(MatchClause { pattern, optional })
    }

    // ── CREATE ───────────────────────────────────────────────────────────────

    fn parse_create(&mut self) -> Result<CreateClause, CypherError> {
        let pattern = self.parse_pattern_with_optional_path_variable(false)?;
        Ok(CreateClause { pattern })
    }

    // ── MERGE ────────────────────────────────────────────────────────────────

    fn parse_merge(&mut self) -> Result<MergeClause, CypherError> {
        let pattern = self.parse_pattern_with_optional_path_variable(false)?;
        let mut on_create = Vec::new();
        let mut on_match = Vec::new();

        // ON CREATE SET ...
        if self.peek_is("ON") {
            self.advance();
            let branch = self.advance_identifier()?;
            if branch.eq_ignore_ascii_case("CREATE") {
                self.expect("SET")?;
                on_create = self.parse_set_items_until_clause_boundary()?;
                // Check for ON MATCH after ON CREATE
                if self.peek_is("ON") {
                    self.advance();
                    self.expect_identifier_matching("MATCH")?;
                    self.expect("SET")?;
                    on_match = self.parse_set_items_until_clause_boundary()?;
                }
            } else if branch.eq_ignore_ascii_case("MATCH") {
                self.expect("SET")?;
                on_match = self.parse_set_items_until_clause_boundary()?;
                // Check for ON CREATE after ON MATCH
                if self.peek_is("ON") {
                    self.advance();
                    self.expect_identifier_matching("CREATE")?;
                    self.expect("SET")?;
                    on_create = self.parse_set_items_until_clause_boundary()?;
                }
            } else {
                return Err(CypherError::ParseError(format!(
                    "expected CREATE or MATCH after ON, got '{}'",
                    branch
                )));
            }
        }

        Ok(MergeClause { pattern, on_create, on_match })
    }

    /// Parse SET items until a clause boundary (next clause keyword or end).
    fn parse_set_items_until_clause_boundary(&mut self) -> Result<Vec<SetItem>, CypherError> {
        let clause_keywords = [
            "RETURN", "WITH", "MATCH", "CREATE", "MERGE", "DELETE", "DETACH",
            "REMOVE", "SET", "CALL", "UNWIND", "FOREACH", "WHERE", "ORDER",
            "SKIP", "LIMIT", "ON",
        ];
        let mut items = Vec::new();
        items.push(self.parse_set_item_with_terminators(&clause_keywords)?);
        while self.peek() == Some(",") {
            self.advance();
            let item = self.parse_set_item_with_terminators(&clause_keywords)?;
            items.push(item);
        }
        Ok(items)
    }

    /// Like expect() but for identifiers (case-insensitive match).
    fn expect_identifier_matching(&mut self, expected: &str) -> Result<String, CypherError> {
        let id = self.advance_identifier()?;
        if !id.eq_ignore_ascii_case(expected) {
            return Err(CypherError::ParseError(format!(
                "expected '{}', got '{}'",
                expected, id
            )));
        }
        Ok(id)
    }

    fn parse_call(&mut self) -> Result<CallClause, CypherError> {
        let mut procedure_parts = vec![self.advance_identifier()?];
        while self.peek() == Some(".") {
            self.advance();
            procedure_parts.push(self.advance_identifier()?);
        }

        self.expect("(")?;
        let mut args = Vec::new();
        if self.peek() != Some(")") {
            args.push(self.parse_expression_item(&[",", ")"])?);
            while self.peek() == Some(",") {
                self.advance();
                args.push(self.parse_expression_item(&[",", ")"])?);
            }
        }
        self.expect(")")?;

        let mut yield_items = Vec::new();
        if self.peek_is("YIELD") {
            self.advance();
            yield_items.push(self.parse_projection_item(&[
                ",", "RETURN", "WITH", "WHERE", "ORDER", "SKIP", "LIMIT",
            ])?);
            while self.peek() == Some(",") {
                self.advance();
                yield_items.push(self.parse_projection_item(&[
                    ",", "RETURN", "WITH", "WHERE", "ORDER", "SKIP", "LIMIT",
                ])?);
            }
        }

        Ok(CallClause {
            procedure: procedure_parts.join("."),
            args,
            yield_items,
        })
    }

    // ── RETURN ───────────────────────────────────────────────────────────────

    fn parse_return(&mut self) -> Result<ReturnClause, CypherError> {
        let distinct = if self.peek_is("DISTINCT") {
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
        if self.peek_is("ORDER") {
            self.advance();
            self.expect("BY")?;
            order_by.push(self.parse_order_item()?);
            while self.peek() == Some(",") {
                self.advance();
                order_by.push(self.parse_order_item()?);
            }
        }

        // Optional SKIP / LIMIT (in any order)
        let mut skip: Option<Expression> = None;
        let mut limit: Option<Expression> = None;
        loop {
            if self.peek_is("SKIP") {
                self.advance();
                skip = Some(self.parse_expression_item(&["LIMIT", "RETURN", "WITH", "MATCH", "CREATE", "MERGE", "SET", "DELETE", "DETACH", "REMOVE", "CALL", "UNWIND", "ORDER", "WHERE"])?);
            } else if self.peek_is("LIMIT") {
                self.advance();
                limit = Some(self.parse_expression_item(&["SKIP", "RETURN", "WITH", "MATCH", "CREATE", "MERGE", "SET", "DELETE", "DETACH", "REMOVE", "CALL", "UNWIND", "ORDER", "WHERE"])?);
            } else {
                break;
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
        self.parse_projection_item(&[",", "AS", "ORDER", "SKIP", "LIMIT"])
    }

    fn parse_projection_item(&mut self, terminators: &[&str]) -> Result<ReturnItem, CypherError> {
        let mut expression_terminators = vec![",", "AS"];
        expression_terminators.extend_from_slice(terminators);
        let expression = self.parse_expression_item(&expression_terminators)?;
        let alias = if self.peek_is("AS") {
            self.advance();
            Some(self.advance_identifier()?)
        } else {
            None
        };
        Ok(ReturnItem { expression, alias })
    }

    fn parse_order_item(&mut self) -> Result<OrderItem, CypherError> {
        let expression = self.parse_expression_item(&[
            ",",
            "ASC",
            "ASCENDING",
            "DESC",
            "DESCENDING",
            "SKIP",
            "LIMIT",
        ])?;
        let descending = self.peek_is_one_of(&["DESC", "DESCENDING"]);
        if descending || self.peek_is_one_of(&["ASC", "ASCENDING"]) {
            self.advance();
        }
        Ok(OrderItem {
            expression,
            descending,
        })
    }

    #[allow(dead_code)]
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

    fn parse_set_item_with_terminators(
        &mut self,
        terminators: &[&str],
    ) -> Result<SetItem, CypherError> {
        let variable = self.advance_identifier()?;
        // Map-merge form: SET n += expr
        if self.peek() == Some("+=") {
            self.advance();
            let mut expr_terms = vec![","];
            expr_terms.extend_from_slice(terminators);
            let value = self.parse_expression_item(&expr_terms)?;
            return Ok(SetItem::MapMerge { variable, value });
        }
        // Map-assignment form: SET n = expr
        if self.peek() == Some("=") {
            self.advance();
            let mut expr_terms = vec![","];
            expr_terms.extend_from_slice(terminators);
            let value = self.parse_expression_item(&expr_terms)?;
            return Ok(SetItem::MapAssignment { variable, value });
        }
        // Label form: SET n:Label or SET n:$(expr)
        if self.peek() == Some(":") {
            self.advance();
            if self.peek() == Some("$") {
                let next_is_paren =
                    self.tokens.get(self.pos + 1).map(|t| *t == "(").unwrap_or(false);
                if next_is_paren {
                    self.advance();
                    self.advance();
                    let expr = self.parse_expression_item(&[")"])?;
                    self.expect(")")?;
                    return Ok(SetItem::DynamicLabel {
                        variable,
                        expression: expr,
                    });
                }
            }
            let label = self.advance_identifier()?;
            return Ok(SetItem::Label { variable, label });
        }
        // Property form: SET n.prop = expr
        self.expect(".")?;
        let property = self.advance_identifier()?;
        self.expect("=")?;
        let mut expr_terms = vec![","];
        expr_terms.extend_from_slice(terminators);
        let value = self.parse_expression_item(&expr_terms)?;
        Ok(SetItem::Property {
            variable,
            property,
            value,
        })
    }

    fn parse_set_item(&mut self) -> Result<SetItem, CypherError> {
        let default_terms = &[
            "MATCH", "CREATE", "MERGE", "SET", "REMOVE", "DELETE", "DETACH", "RETURN", "WITH",
            "UNWIND", "CALL", "ON",
        ];
        self.parse_set_item_with_terminators(default_terms)
    }

    // ── REMOVE ─────────────────────────────────────────────────────────────

    fn parse_remove(&mut self) -> Result<RemoveClause, CypherError> {
        let mut items: Vec<RemoveItem> = Vec::new();
        items.push(self.parse_remove_item()?);
        while self.peek() == Some(",") {
            self.advance();
            items.push(self.parse_remove_item()?);
        }
        Ok(RemoveClause { items })
    }

    fn parse_remove_item(&mut self) -> Result<RemoveItem, CypherError> {
        let variable = self.advance_identifier()?;
        if self.peek() == Some(".") {
            self.advance();
            let property = self.advance_identifier()?;
            Ok(RemoveItem::Property { variable, property })
        } else if self.peek() == Some(":") {
            self.advance();
            let label = self.advance_identifier()?;
            Ok(RemoveItem::Label { variable, label })
        } else {
            Err(CypherError::ParseError(
                "REMOVE items must target a property or label".to_string(),
            ))
        }
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

        let where_clause = if self.peek_is("WHERE") {
            self.advance();
            Some(self.parse_where()?)
        } else {
            None
        };

        let mut order_by: Vec<OrderItem> = Vec::new();
        if self.peek_is("ORDER") {
            self.advance();
            self.expect("BY")?;
            order_by.push(self.parse_order_item()?);
            while self.peek() == Some(",") {
                self.advance();
                order_by.push(self.parse_order_item()?);
            }
        }

        let mut skip: Option<Expression> = None;
        let mut limit: Option<Expression> = None;
        loop {
            if self.peek_is("SKIP") {
                self.advance();
                skip = Some(self.parse_expression_item(&["LIMIT", "RETURN", "WITH", "MATCH", "CREATE", "MERGE", "SET", "DELETE", "DETACH", "REMOVE", "CALL", "UNWIND", "ORDER", "WHERE"])?);
            } else if self.peek_is("LIMIT") {
                self.advance();
                limit = Some(self.parse_expression_item(&["SKIP", "RETURN", "WITH", "MATCH", "CREATE", "MERGE", "SET", "DELETE", "DETACH", "REMOVE", "CALL", "UNWIND", "ORDER", "WHERE"])?);
            } else {
                break;
            }
        }

        Ok(WithClause {
            items,
            order_by,
            skip,
            limit,
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

    fn parse_foreach(&mut self) -> Result<ForeachClause, CypherError> {
        self.expect("(")?;
        let variable = self.advance_identifier()?;
        self.expect("IN")?;
        let list = self.parse_expression_item(&["|"])?;
        self.expect("|")?;
        // Parse inner update clause — support SET, MERGE, CREATE, DELETE, REMOVE
        let mut updates = Vec::new();
        loop {
            match self.peek() {
                Some(t) if t.eq_ignore_ascii_case("SET") => {
                    self.advance();
                    updates.push(Clause::Set(self.parse_set()?));
                }
                Some(t) if t.eq_ignore_ascii_case("MERGE") => {
                    self.advance();
                    updates.push(Clause::Merge(self.parse_merge()?));
                }
                Some(t) if t.eq_ignore_ascii_case("CREATE") => {
                    self.advance();
                    updates.push(Clause::Create(self.parse_create()?));
                }
                Some(t) if t.eq_ignore_ascii_case("DELETE") => {
                    self.advance();
                    updates.push(Clause::Delete(self.parse_delete(false)?));
                }
                Some(t) if t.eq_ignore_ascii_case("DETACH") => {
                    self.advance();
                    updates.push(Clause::Delete(self.parse_delete(true)?));
                }
                Some(t) if t.eq_ignore_ascii_case("REMOVE") => {
                    self.advance();
                    updates.push(Clause::Remove(self.parse_remove()?));
                }
                _ => break,
            }
        }
        self.expect(")")?;
        Ok(ForeachClause {
            variable,
            list,
            updates,
        })
    }

    pub(crate) fn parse_expression_item(
        &mut self,
        terminators: &[&str],
    ) -> Result<Expression, CypherError> {
        let start = self.pos;
        if let Some(expression) = self.try_parse_simple_expression_item(terminators) {
            return Ok(expression);
        }

        self.pos = start;
        self.parse_expression()
    }

    fn try_parse_simple_expression_item(&mut self, terminators: &[&str]) -> Option<Expression> {
        let start = self.pos;
        let first = self.advance()?;
        let expression = if let Some(value) = parse_bool_token(first) {
            Expression::Literal(LiteralValue::Bool(value))
        } else if first.eq_ignore_ascii_case("null") {
            Expression::Literal(LiteralValue::Null)
        } else if first.starts_with('"') || first.starts_with('\'') {
            Expression::Literal(LiteralValue::String(trim_quotes(first).to_string()))
        } else if first.bytes().all(|byte| byte.is_ascii_digit()) {
            Expression::Literal(LiteralValue::Integer(first.parse().ok()?))
        } else if is_simple_expression_identifier(first) {
            if self.peek() == Some(".") {
                self.advance();
                let property = self.advance()?;
                if !is_simple_expression_identifier(property) {
                    self.pos = start;
                    return None;
                }
                Expression::PropertyAccess {
                    variable: first.to_string(),
                    property: property.to_string(),
                }
            } else {
                Expression::Variable(first.to_string())
            }
        } else {
            self.pos = start;
            return None;
        };

        if self.peek().is_none()
            || matches!(self.peek(), Some(token) if terminators.iter().any(|terminator| token.eq_ignore_ascii_case(terminator)))
        {
            Some(expression)
        } else {
            self.pos = start;
            None
        }
    }

    fn parse_create_constraint(&mut self) -> Result<CreateConstraintClause, CypherError> {
        let name = self.advance_identifier()?;
        let if_not_exists = self.consume_if_not_exists()?;
        self.expect("FOR")?;

        // Determine entity type: (n:Label) or ()-[r:TYPE]-()
        let (entity_type, label) =
            if self.tokens.get(self.pos) == Some(&"(") && self.tokens.get(self.pos + 1) == Some(&")") {
                // Relationship constraint: FOR ()-[r:TYPE]-()
                self.advance(); // (
                self.advance(); // )
            self.expect("-")?;
            self.expect("[")?;
            let _variable = self.advance_identifier()?;
            self.expect(":")?;
            let rel_type = self.advance_identifier()?;
            self.expect("]")?;
            self.expect("-")?;
            if self.peek() == Some(">") {
                self.advance();
            }
            self.expect("(")?;
            self.advance(); // )
            (ConstraintEntityType::Relationship, rel_type)
        } else {
            // Node constraint: FOR (n:Label)
            self.expect("(")?;
            let _variable = self.advance_identifier()?;
            self.expect(":")?;
            let label = self.advance_identifier()?;
            self.expect(")")?;
            (ConstraintEntityType::Node, label)
        };

        self.expect("REQUIRE")?;

        // Multi-entry block: REQUIRE { n.p IS UNIQUE; n.q IS NOT NULL; (n.a,n.b) IS NODE KEY }
        // Single entry: REQUIRE n.p IS UNIQUE
        let entries = if self.peek() == Some("{") {
            self.advance(); // {
            let mut entries = Vec::new();
            loop {
                entries.push(self.parse_constraint_entry()?);
                if self.peek() == Some("}") {
                    self.advance();
                    break;
                }
                self.expect(";")?;
                if self.peek() == Some("}") {
                    self.advance();
                    break;
                }
            }
            entries
        } else {
            let entry = self.parse_constraint_entry()?;
            vec![entry]
        };

        Ok(CreateConstraintClause {
            name,
            if_not_exists,
            entity_type,
            label,
            entries,
        })
    }

    fn parse_constraint_entry(&mut self) -> Result<ConstraintEntry, CypherError> {
        // Parse property list: n.prop or (n.a, n.b)
        let properties = if self.peek() == Some("(") {
            self.advance();
            let _var1 = self.advance_identifier()?;
            self.expect(".")?;
            let prop1 = self.advance_identifier()?;
            let mut props = vec![prop1];
            while self.peek() == Some(",") {
                self.advance();
                let _var = self.advance_identifier()?;
                self.expect(".")?;
                props.push(self.advance_identifier()?);
            }
            self.expect(")")?;
            props
        } else {
            let _var = self.advance_identifier()?;
            self.expect(".")?;
            let prop = self.advance_identifier()?;
            vec![prop]
        };

        // Check for IN [...] (Domain constraint) vs IS ... (others)
        if self.peek_is("IN") {
            self.advance(); // IN
            let values = self.parse_constraint_domain_values()?;
            return Ok(ConstraintEntry {
                properties,
                kind: ConstraintKind::Domain(values),
            });
        }

        self.expect("IS")?;

        let kind = match self.peek() {
            Some(t) if t.eq_ignore_ascii_case("UNIQUE") => {
                self.advance();
                ConstraintKind::Unique
            }
            Some(t) if t.eq_ignore_ascii_case("NOT") => {
                self.advance();
                self.expect("NULL")?;
                ConstraintKind::Exists
            }
            Some(t) if t.eq_ignore_ascii_case("NODE") => {
                self.advance();
                self.expect("KEY")?;
                ConstraintKind::NodeKey
            }
            Some(t) if t.eq_ignore_ascii_case("RELATIONSHIP") => {
                self.advance();
                self.expect("KEY")?;
                ConstraintKind::RelationshipKey
            }
            Some(t) if t.eq_ignore_ascii_case("TEMPORAL") => {
                self.advance();
                // Optionally consume NO OVERLAP
                if self.peek_is("NO") {
                    self.advance();
                    self.expect("OVERLAP")?;
                }
                ConstraintKind::Temporal
            }
            Some("::") => {
                self.advance();
                let type_name = self.advance_identifier()?;
                ConstraintKind::Type(type_name)
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

        Ok(ConstraintEntry {
            properties,
            kind,
        })
    }

    /// Parse a domain constraint value list: `['active', 'inactive']` or `[1, 2, 3]`
    fn parse_constraint_domain_values(&mut self) -> Result<Vec<Value>, CypherError> {
        self.expect("[")?;
        let mut values = Vec::new();
        loop {
            if self.peek() == Some("]") {
                self.advance();
                break;
            }
            let val = match self.peek() {
                Some(t) if t.starts_with('\'') || t.starts_with('"') => {
                    let s = self.advance_identifier()?;
                    // Strip surrounding quotes from string tokens
                    let unquoted = if s.len() >= 2
                        && ((s.starts_with('\'') && s.ends_with('\''))
                            || (s.starts_with('"') && s.ends_with('"')))
                    {
                        s[1..s.len() - 1].to_string()
                    } else {
                        s
                    };
                    Value::String(unquoted)
                }
                Some(t) => {
                    // Try to parse as number
                    if let Ok(i) = t.parse::<i64>() {
                        self.advance();
                        Value::from(i)
                    } else if let Ok(f) = t.parse::<f64>() {
                        self.advance();
                        Value::from(f)
                    } else if t.eq_ignore_ascii_case("true") {
                        self.advance();
                        Value::Bool(true)
                    } else if t.eq_ignore_ascii_case("false") {
                        self.advance();
                        Value::Bool(false)
                    } else {
                        let s = self.advance_identifier()?;
                        Value::String(s)
                    }
                }
                None => {
                    return Err(CypherError::ParseError(
                        "unexpected end of input in domain value list".into(),
                    ));
                }
            };
            values.push(val);
            if self.peek() == Some(",") {
                self.advance();
            } else if self.peek() == Some("]") {
                self.advance();
                break;
            } else {
                return Err(CypherError::ParseError(format!(
                    "expected ',' or ']' in domain value list, got {:?}",
                    self.peek()
                )));
            }
        }
        Ok(values)
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

    fn parse_create_index(&mut self, kind: IndexKind) -> Result<CreateIndexClause, CypherError> {
        let name = self.advance_identifier()?;
        let if_not_exists = self.consume_if_not_exists()?;
        self.expect("FOR")?;
        self.expect("(")?;
        let (entity_type, variable, label) = if self.peek() == Some(")") {
            self.advance();
            self.expect("-")?;
            self.expect("[")?;
            let variable = self.advance_identifier()?;
            self.expect(":")?;
            let label = self.advance_identifier()?;
            self.expect("]")?;
            self.expect("-")?;
            if self.peek() == Some(">") {
                self.advance();
            }
            self.expect("(")?;
            self.expect(")")?;
            (IndexEntityType::Relationship, variable, label)
        } else {
            let variable = self.advance_identifier()?;
            self.expect(":")?;
            let label = self.advance_identifier()?;
            self.expect(")")?;
            (IndexEntityType::Node, variable, label)
        };
        self.expect("ON")?;
        let mut properties = Vec::new();
        // Fulltext indexes use ON EACH [prop1, prop2, ...] syntax
        if self.peek_is("EACH") {
            self.advance();
            self.expect("[")?;
            loop {
                let prop_variable = self.advance_identifier()?;
                self.expect(".")?;
                let property = self.advance_identifier()?;
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
            self.expect("]")?;
        } else {
            self.expect("(")?;
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
        }
        if properties.is_empty() {
            return Err(CypherError::ParseError(
                "index definition must include at least one property".into(),
            ));
        }
        // Optional OPTIONS clause (e.g., for VECTOR index config)
        let mut options = HashMap::new();
        if self.peek_is("OPTIONS") {
            self.advance();
            options = self.parse_options_map()?;
        }
        Ok(CreateIndexClause {
            name,
            if_not_exists,
            kind,
            entity_type,
            label,
            properties,
            options,
        })
    }

    fn parse_drop_index(&mut self) -> Result<DropIndexClause, CypherError> {
        let name = self.advance_identifier()?;
        let if_exists = self.consume_if_exists()?;
        Ok(DropIndexClause { name, if_exists })
    }

    fn parse_show_indexes(
        &mut self,
        kind: Option<IndexKind>,
    ) -> Result<ShowIndexesClause, CypherError> {
        if self.peek().is_some() {
            return Err(CypherError::ParseError(format!(
                "unexpected token '{}' after SHOW INDEXES",
                self.peek().unwrap_or_default()
            )));
        }
        Ok(ShowIndexesClause { kind })
    }

    fn parse_create_decay_profile(&mut self) -> Result<CreateDecayProfileClause, CypherError> {
        self.expect("PROFILE")?;
        let name = self.advance_identifier()?;
        if self.peek_is("OPTIONS") {
            self.advance();
            let options = self.parse_options_map()?;
            return Ok(CreateDecayProfileClause {
                name,
                options,
                target: None,
            });
        }
        if self.peek_is("FOR") {
            self.advance();
            let target = self.parse_knowledge_policy_target()?;
            self.expect("APPLY")?;
            let options = self.parse_decay_apply_map()?;
            return Ok(CreateDecayProfileClause {
                name,
                options,
                target: Some(target),
            });
        }
        Err(CypherError::ParseError(format!(
            "expected OPTIONS or FOR after decay profile name '{}'",
            name
        )))
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

    fn parse_alter_promotion_profile(
        &mut self,
    ) -> Result<AlterPromotionProfileClause, CypherError> {
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
        if self.peek().is_some() {
            return Err(CypherError::ParseError(format!(
                "unexpected token '{}' after SHOW PROMOTION PROFILES",
                self.peek().unwrap_or_default()
            )));
        }
        Ok(ShowPromotionProfilesClause)
    }

    fn parse_create_promotion_policy(
        &mut self,
    ) -> Result<CreatePromotionPolicyClause, CypherError> {
        let name = self.advance_identifier()?;
        self.expect("FOR")?;
        let target = self.parse_knowledge_policy_target()?;
        self.expect("APPLY")?;
        let (on_access_mutations, when_clauses) = if self.peek() == Some("{") {
            self.parse_promotion_apply_block()?
        } else {
            (
                Vec::new(),
                vec![self.parse_promotion_profile_clause_after_apply(1)?],
            )
        };
        Ok(CreatePromotionPolicyClause {
            name,
            target,
            enabled: true,
            on_access_mutations,
            when_clauses,
        })
    }

    fn parse_promotion_apply_block(
        &mut self,
    ) -> Result<(Vec<PromotionOnAccessMutation>, Vec<PromotionWhenClause>), CypherError> {
        self.expect("{")?;
        let mut on_access_mutations = Vec::new();
        let mut when_clauses = Vec::new();
        let mut next_order = 1_i64;

        while self.peek() != Some("}") {
            if self.peek_is("ON") {
                self.advance();
                self.expect("ACCESS")?;
                on_access_mutations.extend(self.parse_on_access_mutations()?);
                continue;
            }
            if self.peek_is("APPLY") {
                when_clauses.push(self.parse_promotion_apply_profile_clause(next_order)?);
                next_order += 1;
                continue;
            }
            if self.peek_is("WHEN") {
                let predicate = self.parse_when_predicate()?;
                self.expect("APPLY")?;
                self.expect("PROFILE")?;
                let profile_ref = self.advance_identifier()?;
                when_clauses.push(PromotionWhenClause {
                    profile_ref,
                    predicate,
                    order: next_order,
                });
                next_order += 1;
                continue;
            }
            return Err(CypherError::ParseError(format!(
                "unexpected token '{}' in promotion APPLY block",
                self.peek().unwrap_or_default()
            )));
        }

        self.expect("}")?;
        Ok((on_access_mutations, when_clauses))
    }

    fn parse_promotion_apply_profile_clause(
        &mut self,
        order: i64,
    ) -> Result<PromotionWhenClause, CypherError> {
        self.expect("APPLY")?;
        self.parse_promotion_profile_clause_after_apply(order)
    }

    fn parse_promotion_profile_clause_after_apply(
        &mut self,
        order: i64,
    ) -> Result<PromotionWhenClause, CypherError> {
        self.expect("PROFILE")?;
        let profile_ref = self.advance_identifier()?;
        let predicate = if self.peek_is("WHEN") {
            self.parse_when_predicate()?
        } else {
            "true".to_string()
        };
        Ok(PromotionWhenClause {
            profile_ref,
            predicate,
            order,
        })
    }

    fn parse_when_predicate(&mut self) -> Result<String, CypherError> {
        self.expect("WHEN")?;
        let token = self
            .advance()
            .ok_or_else(|| CypherError::ParseError("expected predicate after WHEN".into()))?;
        Ok(trim_quotes(token).to_string())
    }

    fn parse_on_access_mutations(&mut self) -> Result<Vec<PromotionOnAccessMutation>, CypherError> {
        self.expect("{")?;
        let mut mutations = Vec::new();
        while self.peek() != Some("}") {
            self.expect("SET")?;
            self.advance_identifier()?;
            self.expect(".")?;
            let field = self.advance_identifier()?;
            self.expect("=")?;
            let kind = match field.as_str() {
                "lastAccessedAt" => {
                    self.expect("timestamp")?;
                    self.expect("(")?;
                    self.expect(")")?;
                    PromotionOnAccessMutationKind::SetLastAccessedNow
                }
                "accessCount" => {
                    self.expect("coalesce")?;
                    self.expect("(")?;
                    self.advance_identifier()?;
                    self.expect(".")?;
                    let coalesced_field = self.advance_identifier()?;
                    if coalesced_field != "accessCount" {
                        return Err(CypherError::ParseError(
                            "ON ACCESS accessCount mutation must coalesce accessCount".into(),
                        ));
                    }
                    self.expect(",")?;
                    match self.advance() {
                        Some("0") => {}
                        _ => {
                            return Err(CypherError::ParseError(
                                "ON ACCESS accessCount mutation must use coalesce(..., 0)".into(),
                            ));
                        }
                    }
                    self.expect(")")?;
                    self.expect("+")?;
                    match self.advance() {
                        Some("1") => {}
                        _ => {
                            return Err(CypherError::ParseError(
                                "ON ACCESS accessCount mutation only supports + 1".into(),
                            ));
                        }
                    }
                    PromotionOnAccessMutationKind::IncrementAccessCount
                }
                other => {
                    return Err(CypherError::ParseError(format!(
                        "unsupported ON ACCESS field '{}'",
                        other
                    )));
                }
            };
            mutations.push(PromotionOnAccessMutation { kind });
        }
        self.expect("}")?;
        Ok(mutations)
    }

    fn parse_alter_promotion_policy(&mut self) -> Result<AlterPromotionPolicyClause, CypherError> {
        let name = self.advance_identifier()?;
        self.expect("SET")?;
        self.expect("ENABLED")?;
        let token = self.advance().ok_or_else(|| {
            CypherError::ParseError("expected boolean value after SET ENABLED".into())
        })?;
        let enabled = parse_bool_token(token)
            .ok_or_else(|| CypherError::ParseError(format!("invalid boolean value '{}'", token)))?;
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
        if self.peek().is_some() {
            return Err(CypherError::ParseError(format!(
                "unexpected token '{}' after SHOW PROMOTION POLICIES",
                self.peek().unwrap_or_default()
            )));
        }
        Ok(ShowPromotionPoliciesClause)
    }

    fn parse_knowledge_policy_target(&mut self) -> Result<KnowledgePolicyTarget, CypherError> {
        self.expect("(")?;
        if self.peek() == Some(")") {
            self.advance();
            if self.peek() == Some("-") {
                self.advance();
                self.expect("[")?;
                self.advance_identifier()?;
                self.expect(":")?;
                let edge_type = self.advance_identifier()?;
                self.expect("]")?;
                self.expect("-")?;
                self.expect("(")?;
                self.expect(")")?;
                return Ok(KnowledgePolicyTarget {
                    target_labels: Vec::new(),
                    target_edge_type: Some(edge_type),
                    is_wildcard: false,
                    is_edge: true,
                });
            }
            return Ok(KnowledgePolicyTarget {
                target_labels: Vec::new(),
                target_edge_type: None,
                is_wildcard: true,
                is_edge: false,
            });
        }

        self.advance_identifier()?;
        let mut target_labels = Vec::new();
        while self.peek() == Some(":") {
            self.advance();
            target_labels.push(self.advance_identifier()?);
        }
        self.expect(")")?;
        if target_labels.is_empty() {
            return Err(CypherError::ParseError(
                "knowledge policy target labels are required".into(),
            ));
        }
        Ok(KnowledgePolicyTarget {
            target_labels,
            target_edge_type: None,
            is_wildcard: false,
            is_edge: false,
        })
    }

    fn parse_decay_apply_map(&mut self) -> Result<HashMap<String, Value>, CypherError> {
        self.expect("{")?;
        let mut options = HashMap::new();
        while self.peek() != Some("}") {
            if self.peek_is("DECAY") {
                self.advance();
                self.expect("PROFILE")?;
                let profile_ref = self.advance_identifier()?;
                options.insert("profileRef".to_string(), Value::String(profile_ref));
            } else {
                let key = self.advance_identifier()?;
                self.expect(":")?;
                let value = self.parse_option_value()?;
                options.insert(key, value);
            }

            if self.peek() == Some(",") {
                self.advance();
                continue;
            }
            if self.peek() != Some("}") {
                return Err(CypherError::ParseError(format!(
                    "expected ',' or '}}' in decay APPLY block, got '{}'",
                    self.peek().unwrap_or_default()
                )));
            }
        }
        self.expect("}")?;
        Ok(options)
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
        if token == "{" {
            let mut map = serde_json::Map::new();
            while self.peek() != Some("}") {
                let key = self.advance_identifier()?;
                self.expect(":")?;
                let value = self.parse_option_value()?;
                map.insert(key, value);
                if self.peek() == Some(",") {
                    self.advance();
                } else if self.peek() != Some("}") {
                    return Err(CypherError::ParseError(format!(
                        "expected ',' or '}}' in nested map, got '{}'",
                        self.peek().unwrap_or_default()
                    )));
                }
            }
            self.expect("}")?;
            return Ok(Value::Object(map));
        }
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
        if let Ok(i) = token.parse::<i64>() {
            if self.peek() == Some(".") {
                self.advance();
                let frac = self.advance().ok_or_else(|| {
                    CypherError::ParseError("expected fractional digits after '.'".into())
                })?;
                let number = format!("{}.{}", i, frac);
                let parsed = number.parse::<f64>().map_err(|_| {
                    CypherError::ParseError(format!("invalid numeric option '{}'", number))
                })?;
                return Ok(Value::from(parsed));
            }
            return Ok(Value::from(i));
        }
        if let Some(bool_value) = parse_bool_token(token) {
            return Ok(Value::Bool(bool_value));
        }
        if let Ok(f) = token.parse::<f64>() {
            return Ok(Value::from(f));
        }
        Ok(Value::String(trim_quotes(token).to_string()))
    }

    fn consume_if_not_exists(&mut self) -> Result<bool, CypherError> {
        if self.peek_is("IF") {
            self.advance();
            self.expect("NOT")?;
            self.expect("EXISTS")?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn consume_if_exists(&mut self) -> Result<bool, CypherError> {
        if self.peek_is("IF") {
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
}

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
    fn test_parse_remove() {
        let p = Parser::new();
        let q = p
            .parse("MATCH (n:Person)-[r:FOLLOWS]->() REMOVE n:Person, r.weight RETURN n, r")
            .unwrap();
        assert!(matches!(q.query_type, QueryType::Remove));
        let has_remove = q.clauses.iter().any(|c| matches!(c, Clause::Remove(_)));
        assert!(has_remove);
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
            assert_eq!(
                node.labels,
                vec!["Person".to_string(), "Employee".to_string()]
            );
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
            assert_eq!(r.limit, Some(Expression::Literal(LiteralValue::Integer(5))));
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
    fn test_parse_call_with_yield_aliases() {
        let p = Parser::new();
        let q = p
            .parse(
                "CALL db.index.vector.queryNodes('title_idx', 5, [1,0,0]) YIELD node AS hit, score AS similarity RETURN hit._id AS id, similarity AS value",
            )
            .unwrap();

        let Some(Clause::Call(call)) = q.clauses.first() else {
            panic!("expected Call clause");
        };

        assert_eq!(call.procedure, "db.index.vector.queryNodes");
        assert_eq!(call.args.len(), 3);
        assert_eq!(call.yield_items.len(), 2);
        assert_eq!(call.yield_items[0].alias.as_deref(), Some("hit"));
        assert_eq!(call.yield_items[1].alias.as_deref(), Some("similarity"));
    }

    #[test]
    fn test_parse_call_with_yield_wildcard() {
        let p = Parser::new();
        let q = p
            .parse(
                "CALL db.index.vector.queryNodes('title_idx', 5, [1,0,0]) YIELD * RETURN score",
            )
            .unwrap();

        let Some(Clause::Call(call)) = q.clauses.first() else {
            panic!("expected Call clause");
        };

        assert_eq!(call.yield_items.len(), 1);
        assert!(matches!(
            &call.yield_items[0].expression,
            Expression::Variable(name) if name == "*"
        ));
        assert!(call.yield_items[0].alias.is_none());
    }

    #[test]
    fn test_parse_with_order_skip_limit() {
        let p = Parser::new();
        let q = p
            .parse("MATCH (n) WITH n AS person WHERE person.age > 18 ORDER BY person.name DESC SKIP 2 LIMIT 5 RETURN person")
            .unwrap();

        let with_clause = q
            .clauses
            .iter()
            .find_map(|clause| match clause {
                Clause::With(with_clause) => Some(with_clause),
                _ => None,
            })
            .expect("expected WITH clause");

        assert_eq!(with_clause.items.len(), 1);
        assert_eq!(with_clause.items[0].alias.as_deref(), Some("person"));
        assert!(with_clause.where_clause.is_some());
        assert_eq!(with_clause.order_by.len(), 1);
        assert!(with_clause.order_by[0].descending);
        assert_eq!(with_clause.skip, Some(Expression::Literal(LiteralValue::Integer(2))));
        assert_eq!(with_clause.limit, Some(Expression::Literal(LiteralValue::Integer(5))));
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
    fn test_parse_match_with_path_variable() {
        let p = Parser::new();
        let q = p.parse("MATCH p = (a)-[:LINK]->(b) RETURN p").unwrap();
        assert!(matches!(q.query_type, QueryType::Match));
        if let Some(Clause::Match(m)) = q.clauses.first() {
            assert_eq!(m.pattern.path_variable.as_deref(), Some("p"));
        } else {
            panic!("expected Match clause");
        }
    }

    #[test]
    fn test_parse_create_with_path_variable() {
        let p = Parser::new();
        let q = p.parse("CREATE p = (a)-[:LINK]->(b)").unwrap();
        assert!(matches!(q.query_type, QueryType::Create));
        if let Some(Clause::Create(c)) = q.clauses.first() {
            assert_eq!(c.pattern.path_variable.as_deref(), Some("p"));
        } else {
            panic!("expected Create clause");
        }
    }

    #[test]
    fn test_parse_match_with_shortest_path_variable() {
        let p = Parser::new();
        let q = p
            .parse("MATCH p = shortestPath((a)-[:LINK*]->(b)) RETURN p")
            .unwrap();
        assert!(matches!(q.query_type, QueryType::Match));
        if let Some(Clause::Match(m)) = q.clauses.first() {
            assert_eq!(m.pattern.path_variable.as_deref(), Some("p"));
            assert!(m.pattern.shortest_path);
        } else {
            panic!("expected Match clause");
        }
    }

    #[test]
    fn test_parse_match_tracks_pattern_segments() {
        let p = Parser::new();
        let q = p
            .parse("MATCH (a)-[:LINK]->(b), (c)-[:LINK]->(d), (e) RETURN a, b, c, d, e")
            .unwrap();
        if let Some(Clause::Match(m)) = q.clauses.first() {
            assert_eq!(m.pattern.segment_edge_counts, vec![1, 1, 0]);
            assert_eq!(m.pattern.segments().len(), 3);
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
            Expression::And(_)
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
            assert_eq!(c.entries.len(), 1);
            assert_eq!(c.entries[0].properties, vec!["email"]);
            assert!(matches!(c.entries[0].kind, ConstraintKind::Unique));
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
            assert_eq!(c.entries.len(), 1);
            assert!(matches!(c.entries[0].kind, ConstraintKind::Exists));
        } else {
            panic!("expected CreateConstraint clause");
        }
    }

    #[test]
    fn test_parse_create_constraint_node_key() {
        let p = Parser::new();
        let q = p
            .parse(
                "CREATE CONSTRAINT person_key FOR (n:Person) REQUIRE (n.id, n.email) IS NODE KEY",
            )
            .unwrap();
        if let Clause::CreateConstraint(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.entries.len(), 1);
            assert_eq!(c.entries[0].properties, vec!["id", "email"]);
            assert!(matches!(c.entries[0].kind, ConstraintKind::NodeKey));
        } else {
            panic!("expected CreateConstraint clause");
        }
    }

    #[test]
    fn test_parse_create_constraint_multi_entry_block() {
        let p = Parser::new();
        let q = p
            .parse(
                "CREATE CONSTRAINT multi_entry FOR (n:Person) REQUIRE { n.id IS UNIQUE; n.email IS NOT NULL; (n.a, n.b) IS NODE KEY }",
            )
            .unwrap();
        if let Clause::CreateConstraint(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.entries.len(), 3);
            assert_eq!(c.entries[0].properties, vec!["id"]);
            assert!(matches!(c.entries[0].kind, ConstraintKind::Unique));
            assert_eq!(c.entries[1].properties, vec!["email"]);
            assert!(matches!(c.entries[1].kind, ConstraintKind::Exists));
            assert_eq!(c.entries[2].properties, vec!["a", "b"]);
            assert!(matches!(c.entries[2].kind, ConstraintKind::NodeKey));
        } else {
            panic!("expected CreateConstraint clause");
        }
    }

    #[test]
    fn test_parse_create_relationship_constraint() {
        let p = Parser::new();
        let q = p
            .parse(
                "CREATE CONSTRAINT rel_unique FOR ()-[r:KNOWS]-() REQUIRE r.since IS UNIQUE",
            )
            .unwrap();
        if let Clause::CreateConstraint(c) = q.clauses.first().expect("clause missing") {
            assert!(matches!(c.entity_type, ConstraintEntityType::Relationship));
            assert_eq!(c.label, "KNOWS");
            assert_eq!(c.entries.len(), 1);
            assert!(matches!(c.entries[0].kind, ConstraintKind::Unique));
        } else {
            panic!("expected CreateConstraint clause");
        }
    }

    #[test]
    fn test_parse_create_type_constraint() {
        let p = Parser::new();
        let q = p
            .parse(
                "CREATE CONSTRAINT age_type FOR (n:Person) REQUIRE n.age IS :: INTEGER",
            )
            .unwrap();
        if let Clause::CreateConstraint(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.entries.len(), 1);
            assert!(matches!(&c.entries[0].kind, ConstraintKind::Type(t) if t == "INTEGER"));
        } else {
            panic!("expected CreateConstraint clause");
        }
    }

    #[test]
    fn test_parse_create_constraint_variable_mismatch_errors() {
        let p = Parser::new();
        // Still parses; variable validation is now deferred to eval
        let q = p
            .parse("CREATE CONSTRAINT c FOR (n:Person) REQUIRE m.email IS UNIQUE")
            .unwrap();
        if let Clause::CreateConstraint(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.entries.len(), 1);
            assert_eq!(c.entries[0].properties, vec!["email"]);
        } else {
            panic!("expected CreateConstraint clause");
        }
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
            assert_eq!(c.kind, IndexKind::Range);
            assert_eq!(c.entity_type, IndexEntityType::Node);
            assert_eq!(c.label, "Person");
            assert_eq!(c.properties, vec!["email".to_string(), "name".to_string()]);
        } else {
            panic!("expected CreateIndex clause");
        }
    }

    #[test]
    fn test_parse_create_range_index() {
        let p = Parser::new();
        let q = p
            .parse("CREATE RANGE INDEX person_idx FOR (n:Person) ON (n.email)")
            .unwrap();
        if let Clause::CreateIndex(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.name, "person_idx");
            assert_eq!(c.kind, IndexKind::Range);
            assert_eq!(c.entity_type, IndexEntityType::Node);
            assert_eq!(c.label, "Person");
            assert_eq!(c.properties, vec!["email".to_string()]);
        } else {
            panic!("expected CreateIndex clause");
        }
    }

    #[test]
    fn test_parse_create_relationship_range_index() {
        let p = Parser::new();
        let q = p
            .parse("CREATE RANGE INDEX follows_weight_idx FOR ()-[r:FOLLOWS]-() ON (r.weight)")
            .unwrap();
        if let Clause::CreateIndex(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.name, "follows_weight_idx");
            assert_eq!(c.kind, IndexKind::Range);
            assert_eq!(c.entity_type, IndexEntityType::Relationship);
            assert_eq!(c.label, "FOLLOWS");
            assert_eq!(c.properties, vec!["weight".to_string()]);
        } else {
            panic!("expected CreateIndex clause");
        }
    }

    #[test]
    fn test_parse_create_relationship_index() {
        let p = Parser::new();
        let q = p
            .parse("CREATE INDEX follows_weight_idx IF NOT EXISTS FOR ()-[r:FOLLOWS]-() ON (r.weight, r.since)")
            .unwrap();
        if let Clause::CreateIndex(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.name, "follows_weight_idx");
            assert!(c.if_not_exists);
            assert_eq!(c.kind, IndexKind::Range);
            assert_eq!(c.entity_type, IndexEntityType::Relationship);
            assert_eq!(c.label, "FOLLOWS");
            assert_eq!(
                c.properties,
                vec!["weight".to_string(), "since".to_string()]
            );
        } else {
            panic!("expected CreateIndex clause");
        }
    }

    #[test]
    fn test_parse_create_temporal_index() {
        let p = Parser::new();
        let q = p
            .parse("CREATE TEMPORAL INDEX person_seen_at_idx FOR (n:Person) ON (n.seenAt)")
            .unwrap();
        if let Clause::CreateIndex(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.name, "person_seen_at_idx");
            assert_eq!(c.kind, IndexKind::Temporal);
            assert_eq!(c.entity_type, IndexEntityType::Node);
            assert_eq!(c.label, "Person");
            assert_eq!(c.properties, vec!["seenAt".to_string()]);
        } else {
            panic!("expected CreateIndex clause");
        }
    }

    #[test]
    fn test_parse_create_temporal_index_if_not_exists() {
        let p = Parser::new();
        let q = p
            .parse(
                "CREATE TEMPORAL INDEX person_seen_at_idx IF NOT EXISTS FOR (n:Person) ON (n.seenAt)",
            )
            .unwrap();
        if let Clause::CreateIndex(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.name, "person_seen_at_idx");
            assert!(c.if_not_exists);
            assert_eq!(c.kind, IndexKind::Temporal);
            assert_eq!(c.entity_type, IndexEntityType::Node);
            assert_eq!(c.label, "Person");
            assert_eq!(c.properties, vec!["seenAt".to_string()]);
        } else {
            panic!("expected CreateIndex clause");
        }
    }

    #[test]
    fn test_parse_create_fulltext_relationship_index() {
        let p = Parser::new();
        let q = p
            .parse("CREATE FULLTEXT INDEX follows_note_idx FOR ()-[r:FOLLOWS]-() ON (r.note)")
            .unwrap();
        if let Clause::CreateIndex(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.name, "follows_note_idx");
            assert_eq!(c.kind, IndexKind::FullText);
            assert_eq!(c.entity_type, IndexEntityType::Relationship);
            assert_eq!(c.label, "FOLLOWS");
            assert_eq!(c.properties, vec!["note".to_string()]);
        } else {
            panic!("expected CreateIndex clause");
        }
    }

    #[test]
    fn test_parse_create_fulltext_relationship_index_if_not_exists() {
        let p = Parser::new();
        let q = p
            .parse(
                "CREATE FULLTEXT INDEX follows_note_idx IF NOT EXISTS FOR ()-[r:FOLLOWS]-() ON (r.note)",
            )
            .unwrap();
        if let Clause::CreateIndex(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.name, "follows_note_idx");
            assert!(c.if_not_exists);
            assert_eq!(c.kind, IndexKind::FullText);
            assert_eq!(c.entity_type, IndexEntityType::Relationship);
            assert_eq!(c.label, "FOLLOWS");
            assert_eq!(c.properties, vec!["note".to_string()]);
        } else {
            panic!("expected CreateIndex clause");
        }
    }

    #[test]
    fn test_parse_create_vector_index() {
        let p = Parser::new();
        let q = p
            .parse("CREATE VECTOR INDEX person_embedding_idx FOR (n:Person) ON (n.embedding)")
            .unwrap();
        if let Clause::CreateIndex(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.name, "person_embedding_idx");
            assert_eq!(c.kind, IndexKind::Vector);
            assert_eq!(c.entity_type, IndexEntityType::Node);
            assert_eq!(c.label, "Person");
            assert_eq!(c.properties, vec!["embedding".to_string()]);
        } else {
            panic!("expected CreateIndex clause");
        }
    }

    #[test]
    fn test_parse_create_vector_index_if_not_exists() {
        let p = Parser::new();
        let q = p
            .parse(
                "CREATE VECTOR INDEX person_embedding_idx IF NOT EXISTS FOR (n:Person) ON (n.embedding)",
            )
            .unwrap();
        if let Clause::CreateIndex(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.name, "person_embedding_idx");
            assert!(c.if_not_exists);
            assert_eq!(c.kind, IndexKind::Vector);
            assert_eq!(c.entity_type, IndexEntityType::Node);
            assert_eq!(c.label, "Person");
            assert_eq!(c.properties, vec!["embedding".to_string()]);
        } else {
            panic!("expected CreateIndex clause");
        }
    }

    #[test]
    fn test_parse_create_composite_relationship_range_index() {
        let p = Parser::new();
        let q = p
            .parse(
                "CREATE RANGE INDEX follows_weight_since_idx FOR ()-[r:FOLLOWS]-() ON (r.weight, r.since)",
            )
            .unwrap();
        if let Clause::CreateIndex(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.name, "follows_weight_since_idx");
            assert_eq!(c.kind, IndexKind::Range);
            assert_eq!(c.entity_type, IndexEntityType::Relationship);
            assert_eq!(c.label, "FOLLOWS");
            assert_eq!(
                c.properties,
                vec!["weight".to_string(), "since".to_string()]
            );
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
    fn test_parse_create_relationship_index_variable_mismatch_errors() {
        let p = Parser::new();
        let err = p
            .parse("CREATE RANGE INDEX follows_weight_idx FOR ()-[r:FOLLOWS]-() ON (x.weight)")
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("index variable mismatch: expected 'r', got 'x'"));
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
        if let Clause::ShowIndexes(c) = q.clauses.first().expect("clause missing") {
            assert!(c.kind.is_none());
        } else {
            panic!("expected ShowIndexes clause");
        }
    }

    #[test]
    fn test_parse_show_range_indexes() {
        let p = Parser::new();
        let q = p.parse("SHOW RANGE INDEXES").unwrap();
        if let Clause::ShowIndexes(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.kind, Some(IndexKind::Range));
        } else {
            panic!("expected ShowIndexes clause");
        }
    }

    #[test]
    fn test_parse_show_temporal_indexes() {
        let p = Parser::new();
        let q = p.parse("SHOW TEMPORAL INDEXES").unwrap();
        if let Clause::ShowIndexes(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.kind, Some(IndexKind::Temporal));
        } else {
            panic!("expected ShowIndexes clause");
        }
    }

    #[test]
    fn test_parse_show_fulltext_indexes() {
        let p = Parser::new();
        let q = p.parse("SHOW FULLTEXT INDEXES").unwrap();
        if let Clause::ShowIndexes(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.kind, Some(IndexKind::FullText));
        } else {
            panic!("expected ShowIndexes clause");
        }
    }

    #[test]
    fn test_parse_show_vector_indexes() {
        let p = Parser::new();
        let q = p.parse("SHOW VECTOR INDEXES").unwrap();
        if let Clause::ShowIndexes(c) = q.clauses.first().expect("clause missing") {
            assert_eq!(c.kind, Some(IndexKind::Vector));
        } else {
            panic!("expected ShowIndexes clause");
        }
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
            assert!(c.target.is_none());
            assert_eq!(c.options.get("halfLifeSeconds"), Some(&Value::from(604800)));
            assert_eq!(
                c.options.get("scope"),
                Some(&Value::String("NODE".to_string()))
            );
        } else {
            panic!("expected CreateDecayProfile clause");
        }
    }

    #[test]
    fn test_parse_create_decay_profile_binding() {
        let p = Parser::new();
        let q = p
            .parse(
                "CREATE DECAY PROFILE memory_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE slow_decay, visibilityThreshold: 0.2, order: 10 }",
            )
            .unwrap();
        if let Clause::CreateDecayProfile(c) = q.clauses.first().expect("clause missing") {
            let target = c.target.as_ref().expect("binding target missing");
            assert_eq!(target.target_labels, vec!["MemoryEpisode".to_string()]);
            assert!(!target.is_wildcard);
            assert!(!target.is_edge);
            assert_eq!(
                c.options.get("profileRef"),
                Some(&Value::String("slow_decay".to_string()))
            );
            assert_eq!(c.options.get("order"), Some(&Value::from(10)));
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
            assert_eq!(c.target.target_labels, vec!["KnowledgeFact".to_string()]);
            assert_eq!(c.when_clauses.len(), 1);
            assert_eq!(c.when_clauses[0].profile_ref, "boost_profile");
            assert!(c.on_access_mutations.is_empty());
        } else {
            panic!("expected CreatePromotionPolicy clause");
        }

        let access_policy = p
            .parse(
                "CREATE PROMOTION POLICY access_policy FOR ()-[r:LINKS]-() APPLY { ON ACCESS { SET r.lastAccessedAt = timestamp() SET r.accessCount = coalesce(r.accessCount, 0) + 1 } }",
            )
            .unwrap();
        if let Clause::CreatePromotionPolicy(c) =
            access_policy.clauses.first().expect("clause missing")
        {
            assert!(c.target.is_edge);
            assert_eq!(c.target.target_edge_type.as_deref(), Some("LINKS"));
            assert_eq!(c.on_access_mutations.len(), 2);
            assert!(c.when_clauses.is_empty());
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
