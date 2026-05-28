use std::collections::HashMap;

use crate::{
    parse_context::ParseContext, parser_support::dominant_query_type, Clause, CypherError, Query,
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
                    let clause = self.parse_call()?;
                    clauses.push(Clause::Call(clause));
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
                    } else if self.peek_is("INDEX") {
                        self.advance();
                        clauses.push(Clause::CreateIndex(self.parse_create_index()?));
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
                    let clause = self.parse_where()?;
                    clauses.push(Clause::Where(clause));
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
                    } else if self.peek_is("INDEXES") {
                        self.advance();
                        clauses.push(Clause::ShowIndexes(self.parse_show_indexes()?));
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
}
