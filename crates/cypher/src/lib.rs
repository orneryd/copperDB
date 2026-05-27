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
        } else {
            self.parse_pattern()?
        };
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
        Ok(MergeClause { pattern })
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

        Ok(CallClause {
            procedure: procedure_parts.join("."),
            args,
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
        let mut skip: Option<i64> = None;
        let mut limit: Option<i64> = None;
        loop {
            if self.peek_is("SKIP") {
                self.advance();
                skip = Some(self.parse_i64()?);
            } else if self.peek_is("LIMIT") {
                self.advance();
                limit = Some(self.parse_i64()?);
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
        let expression = self.parse_expression_item(&[",", "AS", "ORDER", "SKIP", "LIMIT"])?;
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
        if descending {
            self.advance();
        } else if self.peek_is_one_of(&["ASC", "ASCENDING"]) {
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
        let value = self.parse_expression_item(&[
            ",", "MATCH", "CREATE", "MERGE", "SET", "DELETE", "DETACH", "RETURN", "WITH", "UNWIND",
            "CALL",
        ])?;
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

        let where_clause = if self.peek_is("WHERE") {
            self.advance();
            Some(self.parse_where()?)
        } else {
            None
        };

        let limit = if self.peek_is("LIMIT") {
            self.advance();
            Some(self.parse_i64()?)
        } else {
            None
        };

        Ok(WithClause {
            items,
            where_clause,
            limit,
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

        let kind = match self.peek() {
            Some(other) if other.eq_ignore_ascii_case("UNIQUE") => {
                self.advance();
                ConstraintKind::Unique
            }
            Some(other) if other.eq_ignore_ascii_case("NOT") => {
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
        if self.peek_is("WHEN") {
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
            assert_eq!(c.properties, vec!["email".to_string(), "name".to_string()]);
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
            assert_eq!(
                c.options.get("scope"),
                Some(&Value::String("NODE".to_string()))
            );
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
            assert_eq!(c.target_labels, vec!["KnowledgeFact".to_string()]);
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
