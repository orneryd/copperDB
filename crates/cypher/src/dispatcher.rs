use std::collections::HashMap;

use crate::{
    parse_context::ParseContext, parser_support::dominant_query_type, Clause, CypherError, Query,
};

impl ParseContext {
    pub(crate) fn parse_query(&mut self) -> Result<Query, CypherError> {
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
                                        "expected CREATE PROMOTION target, got end of input".into(),
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
                            clauses
                                .push(Clause::DropDecayProfile(self.parse_drop_decay_profile()?));
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
                            clauses
                                .push(Clause::ShowDecayProfiles(self.parse_show_decay_profiles()?));
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
                            clauses
                                .push(Clause::AlterDecayProfile(self.parse_alter_decay_profile()?));
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
}
