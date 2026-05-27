use crate::{Clause, QueryType};

pub(crate) fn trim_quotes(token: &str) -> &str {
    if (token.starts_with('\'') && token.ends_with('\''))
        || (token.starts_with('"') && token.ends_with('"'))
    {
        &token[1..token.len().saturating_sub(1)]
    } else {
        token
    }
}

pub(crate) fn parse_bool_token(token: &str) -> Option<bool> {
    if token.eq_ignore_ascii_case("TRUE") {
        Some(true)
    } else if token.eq_ignore_ascii_case("FALSE") {
        Some(false)
    } else {
        None
    }
}

/// Returns `true` if `s` is an openCypher keyword that cannot be a bare variable name.
pub(crate) fn is_keyword(s: &str) -> bool {
    [
        "MATCH",
        "OPTIONAL",
        "CREATE",
        "RETURN",
        "WHERE",
        "SET",
        "DELETE",
        "DETACH",
        "WITH",
        "MERGE",
        "UNWIND",
        "CALL",
        "ORDER",
        "BY",
        "LIMIT",
        "SKIP",
        "AS",
        "AND",
        "OR",
        "NOT",
        "NULL",
        "TRUE",
        "FALSE",
        "IS",
        "IN",
        "DISTINCT",
        "ASC",
        "DESC",
        "ASCENDING",
        "DESCENDING",
        "CONTAINS",
        "STARTS",
        "ENDS",
        "DROP",
        "SHOW",
        "CONSTRAINT",
        "CONSTRAINTS",
        "INDEX",
        "INDEXES",
        "ALTER",
        "DECAY",
        "PROFILE",
        "PROFILES",
        "PROMOTION",
        "POLICY",
        "POLICIES",
        "FOR",
        "APPLY",
        "REQUIRE",
        "UNIQUE",
        "EXISTS",
        "ON",
        "OPTIONS",
        "ENABLED",
        "WHEN",
    ]
    .iter()
    .any(|keyword| s.eq_ignore_ascii_case(keyword))
}

/// Determine the dominant `QueryType` from all parsed clauses.
///
/// Priority (highest wins): Delete > Set > Merge > Create > Match > With > Return
pub(crate) fn dominant_query_type(clauses: &[Clause]) -> QueryType {
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
