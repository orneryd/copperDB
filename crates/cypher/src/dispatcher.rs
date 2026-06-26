use std::collections::HashMap;

use crate::{
    parse_context::ParseContext, parser_support::dominant_query_type, Clause, CypherError,
    Expression, Query, ReturnClause, ReturnItem, SubqueryBlock, SubqueryClause,
};

impl<'a> ParseContext<'a> {
    pub(crate) fn parse_query(&mut self) -> Result<Query, CypherError> {
        let mut clauses: Vec<Clause> = Vec::new();

        while self.pos < self.tokens.len() {
            let token = match self.peek() {
                Some(token) => token,
                None => break,
            };

            match token {
                token if token.eq_ignore_ascii_case("CALL") => {
                    self.advance();
                    // CALL { ... } subquery vs CALL procedureName(...)
                    if self.peek() == Some("{") {
                        self.advance(); // consume {
                        let mut blocks: Vec<SubqueryBlock> = Vec::new();
                        loop {
                            let mut sub_clauses = Vec::new();
                            loop {
                                if self.peek() == Some("}") || self.peek() == Some("UNION") {
                                    break;
                                }
                                if self.pos >= self.tokens.len() {
                                    return Err(CypherError::ParseError(
                                        "unterminated CALL {} subquery".into(),
                                    ));
                                }
                                let inner_token = match self.peek() {
                                    Some(t) => t,
                                    None => break,
                                };
                                match inner_token {
                                    t if t.eq_ignore_ascii_case("MATCH") => {
                                        self.advance();
                                        sub_clauses.push(Clause::Match(self.parse_match(false)?));
                                    }
                                    t if t.eq_ignore_ascii_case("OPTIONAL") => {
                                        self.advance();
                                        self.expect("MATCH")?;
                                        sub_clauses
                                            .push(Clause::OptionalMatch(self.parse_match(true)?));
                                    }
                                    t if t.eq_ignore_ascii_case("CREATE") => {
                                        self.advance();
                                        sub_clauses.push(Clause::Create(self.parse_create()?));
                                    }
                                    t if t.eq_ignore_ascii_case("MERGE") => {
                                        self.advance();
                                        sub_clauses.push(Clause::Merge(self.parse_merge()?));
                                    }
                                    t if t.eq_ignore_ascii_case("WITH") => {
                                        self.advance();
                                        sub_clauses.push(Clause::With(self.parse_with()?));
                                    }
                                    t if t.eq_ignore_ascii_case("RETURN") => {
                                        self.advance();
                                        sub_clauses.push(Clause::Return(self.parse_return()?));
                                    }
                                    t if t.eq_ignore_ascii_case("SET") => {
                                        self.advance();
                                        sub_clauses.push(Clause::Set(self.parse_set()?));
                                    }
                                    t if t.eq_ignore_ascii_case("DELETE") => {
                                        self.advance();
                                        sub_clauses.push(Clause::Delete(self.parse_delete(false)?));
                                    }
                                    t if t.eq_ignore_ascii_case("DETACH") => {
                                        self.advance();
                                        self.expect("DELETE")?;
                                        sub_clauses.push(Clause::Delete(self.parse_delete(true)?));
                                    }
                                    t if t.eq_ignore_ascii_case("WHERE") => {
                                        self.advance();
                                        sub_clauses.push(Clause::Where(self.parse_where()?));
                                    }
                                    t if t.eq_ignore_ascii_case("UNWIND") => {
                                        self.advance();
                                        sub_clauses.push(Clause::Unwind(self.parse_unwind()?));
                                    }
                                    other => {
                                        return Err(CypherError::ParseError(format!(
                                            "unsupported clause in CALL {{}}: {}",
                                            other
                                        )));
                                    }
                                }
                            }
                            let union_all = self.peek_is("UNION");
                            blocks.push(SubqueryBlock {
                                clauses: sub_clauses,
                                union_all,
                            });
                            if union_all {
                                self.advance(); // UNION
                                if self.peek_is("ALL") {
                                    self.advance();
                                    // set union_all on the already-pushed block
                                    blocks.last_mut().unwrap().union_all = true;
                                }
                            } else if self.peek() == Some("}") {
                                self.advance();
                                break;
                            }
                        }
                        clauses.push(Clause::Subquery(SubqueryClause { blocks }));
                    } else {
                        let clause = self.parse_call()?;
                        let has_yield = !clause.yield_items.is_empty();
                        clauses.push(Clause::Call(clause));

                        // Neo4j-compatible: CALL ... YIELD x SKIP n / LIMIT n
                        // generates an implicit RETURN * with those modifiers
                        if has_yield && self.peek_is_one_of(&["SKIP", "LIMIT"]) {
                            let mut skip: Option<Expression> = None;
                            let mut limit: Option<Expression> = None;
                            loop {
                                if self.peek_is("SKIP") {
                                    self.advance();
                                    skip = Some(self.parse_expression_item(&[
                                        "LIMIT", "RETURN", "WITH", "MATCH", "CREATE", "MERGE",
                                        "SET", "DELETE", "DETACH", "REMOVE", "CALL", "UNWIND",
                                        "ORDER", "WHERE",
                                    ])?);
                                } else if self.peek_is("LIMIT") {
                                    self.advance();
                                    limit = Some(self.parse_expression_item(&[
                                        "SKIP", "RETURN", "WITH", "MATCH", "CREATE", "MERGE",
                                        "SET", "DELETE", "DETACH", "REMOVE", "CALL", "UNWIND",
                                        "ORDER", "WHERE",
                                    ])?);
                                } else {
                                    break;
                                }
                            }
                            let wildcard = ReturnItem {
                                expression: Expression::Variable("*".to_string()),
                                alias: None,
                            };
                            clauses.push(Clause::Return(ReturnClause {
                                items: vec![wildcard],
                                order_by: vec![],
                                skip,
                                limit,
                                distinct: false,
                            }));
                        }
                    }
                }
                token if token.eq_ignore_ascii_case("MATCH") => {
                    self.advance();
                    let clause = self.parse_match(false)?;
                    clauses.push(Clause::Match(clause));
                }
                token if token.eq_ignore_ascii_case("OPTIONAL") => {
                    self.advance();
                    self.expect("MATCH")?;
                    let clause = self.parse_match(true)?;
                    clauses.push(Clause::OptionalMatch(clause));
                }
                token if token.eq_ignore_ascii_case("CREATE") => {
                    self.advance();
                    if self.peek_is("CONSTRAINT") {
                        self.advance();
                        clauses.push(Clause::CreateConstraint(self.parse_create_constraint()?));
                    } else if self.peek_is("RANGE") {
                        self.advance();
                        self.expect("INDEX")?;
                        clauses.push(Clause::CreateIndex(
                            self.parse_create_index(crate::IndexKind::Range)?,
                        ));
                    } else if self.peek_is("TEMPORAL") {
                        self.advance();
                        self.expect("INDEX")?;
                        clauses.push(Clause::CreateIndex(
                            self.parse_create_index(crate::IndexKind::Temporal)?,
                        ));
                    } else if self.peek_is("FULLTEXT") {
                        self.advance();
                        self.expect("INDEX")?;
                        clauses.push(Clause::CreateIndex(
                            self.parse_create_index(crate::IndexKind::FullText)?,
                        ));
                    } else if self.peek_is("VECTOR") {
                        self.advance();
                        self.expect("INDEX")?;
                        clauses.push(Clause::CreateIndex(
                            self.parse_create_index(crate::IndexKind::Vector)?,
                        ));
                    } else if self.peek_is("INDEX") {
                        self.advance();
                        clauses.push(Clause::CreateIndex(
                            self.parse_create_index(crate::IndexKind::Range)?,
                        ));
                    } else if self.peek_is("DECAY") {
                        self.advance();
                        clauses.push(Clause::CreateDecayProfile(
                            self.parse_create_decay_profile()?,
                        ));
                    } else if self.peek_is("PROMOTION") {
                        self.advance();
                        if self.peek_is("PROFILE") {
                            self.advance();
                            clauses.push(Clause::CreatePromotionProfile(
                                self.parse_create_promotion_profile()?,
                            ));
                        } else if self.peek_is("POLICY") {
                            self.advance();
                            clauses.push(Clause::CreatePromotionPolicy(
                                self.parse_create_promotion_policy()?,
                            ));
                        } else if let Some(other) = self.peek() {
                            return Err(CypherError::ParseError(format!(
                                "unsupported CREATE PROMOTION target '{}'",
                                other
                            )));
                        } else {
                            return Err(CypherError::ParseError(
                                "expected CREATE PROMOTION target, got end of input".into(),
                            ));
                        }
                    } else {
                        let clause = self.parse_create()?;
                        clauses.push(Clause::Create(clause));
                    }
                }
                token if token.eq_ignore_ascii_case("MERGE") => {
                    self.advance();
                    let clause = self.parse_merge()?;
                    clauses.push(Clause::Merge(clause));
                }
                token if token.eq_ignore_ascii_case("RETURN") => {
                    self.advance();
                    let clause = self.parse_return()?;
                    clauses.push(Clause::Return(clause));
                }
                token if token.eq_ignore_ascii_case("WHERE") => {
                    self.advance();
                    if self.peek_is("EXISTS") && self.tokens.get(self.pos + 1) == Some(&"{") {
                        self.advance(); // EXISTS
                        let subquery = self.parse_subquery_body()?;
                        clauses.push(Clause::WhereExists(subquery));
                    } else {
                        let clause = self.parse_where()?;
                        clauses.push(Clause::Where(clause));
                    }
                }
                token if token.eq_ignore_ascii_case("SET") => {
                    self.advance();
                    let clause = self.parse_set()?;
                    clauses.push(Clause::Set(clause));
                }
                token if token.eq_ignore_ascii_case("REMOVE") => {
                    self.advance();
                    let clause = self.parse_remove()?;
                    clauses.push(Clause::Remove(clause));
                }
                token if token.eq_ignore_ascii_case("DELETE") => {
                    self.advance();
                    let clause = self.parse_delete(false)?;
                    clauses.push(Clause::Delete(clause));
                }
                token if token.eq_ignore_ascii_case("DETACH") => {
                    self.advance();
                    self.expect("DELETE")?;
                    let clause = self.parse_delete(true)?;
                    clauses.push(Clause::Delete(clause));
                }
                token if token.eq_ignore_ascii_case("WITH") => {
                    self.advance();
                    let clause = self.parse_with()?;
                    clauses.push(Clause::With(clause));
                }
                token if token.eq_ignore_ascii_case("UNWIND") => {
                    self.advance();
                    let clause = self.parse_unwind()?;
                    clauses.push(Clause::Unwind(clause));
                }
                token if token.eq_ignore_ascii_case("FOREACH") => {
                    self.advance();
                    let clause = self.parse_foreach()?;
                    clauses.push(Clause::Foreach(clause));
                }
                token if token.eq_ignore_ascii_case("DROP") => {
                    self.advance();
                    if self.peek_is("CONSTRAINT") {
                        self.advance();
                        clauses.push(Clause::DropConstraint(self.parse_drop_constraint()?));
                    } else if self.peek_is("INDEX") {
                        self.advance();
                        clauses.push(Clause::DropIndex(self.parse_drop_index()?));
                    } else if self.peek_is("DECAY") {
                        self.advance();
                        clauses.push(Clause::DropDecayProfile(self.parse_drop_decay_profile()?));
                    } else if self.peek_is("PROMOTION") {
                        self.advance();
                        if self.peek_is("PROFILE") {
                            self.advance();
                            clauses.push(Clause::DropPromotionProfile(
                                self.parse_drop_promotion_profile()?,
                            ));
                        } else if self.peek_is("POLICY") {
                            self.advance();
                            clauses.push(Clause::DropPromotionPolicy(
                                self.parse_drop_promotion_policy()?,
                            ));
                        } else if let Some(other) = self.peek() {
                            return Err(CypherError::ParseError(format!(
                                "unsupported DROP PROMOTION target '{}'",
                                other
                            )));
                        } else {
                            return Err(CypherError::ParseError(
                                "expected DROP PROMOTION target, got end of input".into(),
                            ));
                        }
                    } else if let Some(other) = self.peek() {
                        return Err(CypherError::ParseError(format!(
                            "unsupported DROP target '{}'",
                            other
                        )));
                    } else {
                        return Err(CypherError::ParseError(
                            "expected DROP target, got end of input".into(),
                        ));
                    }
                }
                token if token.eq_ignore_ascii_case("SHOW") => {
                    self.advance();
                    if self.peek_is("CONSTRAINTS") {
                        self.advance();
                        clauses.push(Clause::ShowConstraints(self.parse_show_constraints()?));
                    } else if self.peek_is("RANGE") {
                        self.advance();
                        self.expect("INDEXES")?;
                        clauses.push(Clause::ShowIndexes(
                            self.parse_show_indexes(Some(crate::IndexKind::Range))?,
                        ));
                    } else if self.peek_is("TEMPORAL") {
                        self.advance();
                        self.expect("INDEXES")?;
                        clauses.push(Clause::ShowIndexes(
                            self.parse_show_indexes(Some(crate::IndexKind::Temporal))?,
                        ));
                    } else if self.peek_is("FULLTEXT") {
                        self.advance();
                        self.expect("INDEXES")?;
                        clauses.push(Clause::ShowIndexes(
                            self.parse_show_indexes(Some(crate::IndexKind::FullText))?,
                        ));
                    } else if self.peek_is("VECTOR") {
                        self.advance();
                        self.expect("INDEXES")?;
                        clauses.push(Clause::ShowIndexes(
                            self.parse_show_indexes(Some(crate::IndexKind::Vector))?,
                        ));
                    } else if self.peek_is("INDEXES") {
                        self.advance();
                        clauses.push(Clause::ShowIndexes(self.parse_show_indexes(None)?));
                    } else if self.peek_is("DECAY") {
                        self.advance();
                        clauses.push(Clause::ShowDecayProfiles(self.parse_show_decay_profiles()?));
                    } else if self.peek_is("PROMOTION") {
                        self.advance();
                        if self.peek_is("PROFILES") {
                            self.advance();
                            clauses.push(Clause::ShowPromotionProfiles(
                                self.parse_show_promotion_profiles()?,
                            ));
                        } else if self.peek_is("POLICIES") {
                            self.advance();
                            clauses.push(Clause::ShowPromotionPolicies(
                                self.parse_show_promotion_policies()?,
                            ));
                        } else if let Some(other) = self.peek() {
                            return Err(CypherError::ParseError(format!(
                                "unsupported SHOW PROMOTION target '{}'",
                                other
                            )));
                        } else {
                            return Err(CypherError::ParseError(
                                "expected SHOW PROMOTION target, got end of input".into(),
                            ));
                        }
                    } else if let Some(other) = self.peek() {
                        return Err(CypherError::ParseError(format!(
                            "unsupported SHOW target '{}'",
                            other
                        )));
                    } else {
                        return Err(CypherError::ParseError(
                            "expected SHOW target, got end of input".into(),
                        ));
                    }
                }
                token if token.eq_ignore_ascii_case("ALTER") => {
                    self.advance();
                    if self.peek_is("DECAY") {
                        self.advance();
                        clauses.push(Clause::AlterDecayProfile(self.parse_alter_decay_profile()?));
                    } else if self.peek_is("PROMOTION") {
                        self.advance();
                        if self.peek_is("PROFILE") {
                            self.advance();
                            clauses.push(Clause::AlterPromotionProfile(
                                self.parse_alter_promotion_profile()?,
                            ));
                        } else if self.peek_is("POLICY") {
                            self.advance();
                            clauses.push(Clause::AlterPromotionPolicy(
                                self.parse_alter_promotion_policy()?,
                            ));
                        } else if let Some(other) = self.peek() {
                            return Err(CypherError::ParseError(format!(
                                "unsupported ALTER PROMOTION target '{}'",
                                other
                            )));
                        } else {
                            return Err(CypherError::ParseError(
                                "expected ALTER PROMOTION target, got end of input".into(),
                            ));
                        }
                    } else if let Some(other) = self.peek() {
                        return Err(CypherError::ParseError(format!(
                            "unsupported ALTER target '{}'",
                            other
                        )));
                    } else {
                        return Err(CypherError::ParseError(
                            "expected ALTER target, got end of input".into(),
                        ));
                    }
                }
                _ => {
                    return Err(CypherError::ParseError(format!(
                        "unexpected token '{}'",
                        token
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

    /// Parse `{ MATCH ... [WHERE ...] }` body for EXISTS / CALL {} subqueries.
    fn parse_subquery_body(&mut self) -> Result<SubqueryClause, CypherError> {
        self.expect("{")?;
        let mut sub_clauses = Vec::new();
        loop {
            if self.peek() == Some("}") {
                self.advance();
                break;
            }
            if self.pos >= self.tokens.len() {
                return Err(CypherError::ParseError("unterminated subquery body".into()));
            }
            let token = match self.peek() {
                Some(t) => t,
                None => break,
            };
            match token {
                t if t.eq_ignore_ascii_case("MATCH") => {
                    self.advance();
                    sub_clauses.push(Clause::Match(self.parse_match(false)?));
                }
                t if t.eq_ignore_ascii_case("OPTIONAL") => {
                    self.advance();
                    self.expect("MATCH")?;
                    sub_clauses.push(Clause::OptionalMatch(self.parse_match(true)?));
                }
                t if t.eq_ignore_ascii_case("WHERE") => {
                    self.advance();
                    sub_clauses.push(Clause::Where(self.parse_where()?));
                }
                t if t.eq_ignore_ascii_case("WITH") => {
                    self.advance();
                    sub_clauses.push(Clause::With(self.parse_with()?));
                }
                t if t.eq_ignore_ascii_case("RETURN") => {
                    self.advance();
                    sub_clauses.push(Clause::Return(self.parse_return()?));
                }
                t if t.eq_ignore_ascii_case("CREATE") => {
                    self.advance();
                    sub_clauses.push(Clause::Create(self.parse_create()?));
                }
                other => {
                    return Err(CypherError::ParseError(format!(
                        "unsupported clause in subquery body: {}",
                        other
                    )));
                }
            }
        }
        Ok(SubqueryClause {
            blocks: vec![SubqueryBlock {
                clauses: sub_clauses,
                union_all: false,
            }],
        })
    }
}
