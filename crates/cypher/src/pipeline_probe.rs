use crate::keyword_scan::{keyword_index_from, KeywordScanOpts};
use crate::string_patterns::find_keyword_index;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineClauseKind {
    Match,
    Create,
    With,
    Unwind,
    Return,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineClause {
    pub kind: PipelineClauseKind,
    pub text: String,
}

pub fn can_execute_as_pipeline(cypher: &str) -> (Vec<PipelineClause>, bool) {
    let clauses = match split_pipeline_clauses(cypher) {
        Some(clauses) => clauses,
        None => return (Vec::new(), false),
    };
    if clauses.len() < 2 {
        return (Vec::new(), false);
    }
    if !clauses.iter().any(|clause| {
        matches!(clause.kind, PipelineClauseKind::With | PipelineClauseKind::Unwind)
    }) {
        return (Vec::new(), false);
    }
    (clauses, true)
}

pub fn pending_pipeline_execution_todo() -> &'static str {
    "Parser-approved pipeline clause sequences now route through a dedicated eval/engine pipeline executor; remaining work is broader shape coverage, not a missing route."
}

fn split_pipeline_clauses(cypher: &str) -> Option<Vec<PipelineClause>> {
    let upper = cypher.to_ascii_uppercase();
    for unsupported in ["OPTIONAL MATCH", "MERGE", "FOREACH", "CALL", "DELETE", "REMOVE", "SET"] {
        if find_keyword_index(&upper, unsupported).is_some() {
            return None;
        }
    }

    let mut boundaries = Vec::new();
    for (keyword, kind) in [
        ("MATCH", PipelineClauseKind::Match),
        ("CREATE", PipelineClauseKind::Create),
        ("WITH", PipelineClauseKind::With),
        ("UNWIND", PipelineClauseKind::Unwind),
        ("RETURN", PipelineClauseKind::Return),
    ] {
        for pos in find_all_keyword_positions(cypher, keyword) {
            if keyword == "WITH" && with_is_operator_suffix(cypher.as_bytes(), pos) {
                continue;
            }
            boundaries.push((pos, kind));
        }
    }
    if boundaries.is_empty() {
        return None;
    }
    boundaries.sort_by_key(|(pos, _)| *pos);

    let trimmed_left = cypher.len() - cypher.trim_start_matches(|c: char| c.is_ascii_whitespace()).len();
    if boundaries[0].0 != trimmed_left {
        return None;
    }

    let mut clauses = Vec::new();
    for (index, (start, kind)) in boundaries.iter().enumerate() {
        let end = boundaries.get(index + 1).map(|(pos, _)| *pos).unwrap_or(cypher.len());
        let text = cypher[*start..end].trim();
        if !text.is_empty() {
            clauses.push(PipelineClause {
                kind: *kind,
                text: text.to_string(),
            });
        }
    }
    Some(clauses)
}

fn find_all_keyword_positions(cypher: &str, keyword: &str) -> Vec<usize> {
    let mut positions = Vec::new();
    let mut from = 0;
    let opts = KeywordScanOpts::default();
    while let Some(pos) = keyword_index_from(cypher, keyword, from, opts) {
        positions.push(pos);
        from = pos + keyword.len();
    }
    positions
}

fn with_is_operator_suffix(bytes: &[u8], with_pos: usize) -> bool {
    use crate::keyword_scan::{ascii_upper, is_ident_byte};

    let mut pos = with_pos;
    while pos > 0 && bytes[pos - 1].is_ascii_whitespace() {
        pos -= 1;
    }
    let token_end = pos;
    while pos > 0 && is_ident_byte(bytes[pos - 1]) {
        pos -= 1;
    }
    let token = &bytes[pos..token_end];
    let equals_ci = |keyword: &[u8]| {
        token.len() == keyword.len()
            && token
                .iter()
                .zip(keyword.iter())
                .all(|(&left, &right)| ascii_upper(left) == right)
    };
    equals_ci(b"STARTS") || equals_ci(b"ENDS")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_rejects_comma_match_then_create() {
        let query =
            "MATCH (o:OriginalText {id:'o1'}), (t:TranslatedText {id:'t1'}) CREATE (o)-[:TRANSLATES_TO]->(t)";
        let (_, ok) = can_execute_as_pipeline(query);
        assert!(!ok);
    }

    #[test]
    fn test_pipeline_accepts_seeder_shape() {
        let query = "MATCH (c:Customer {customerID: 1}) CREATE (o:Order {orderID: 9001}) CREATE (c)-[:PURCHASED]->(o) WITH o, {} UNWIND [{productID: 1}] AS prodRef MATCH (p:Product {productID: prodRef.productID}) CREATE (o)-[:ORDERS]->(p)";
        let (clauses, ok) = can_execute_as_pipeline(query);
        assert!(ok);
        assert_eq!(clauses.len(), 7);
        assert_eq!(clauses[0].kind, PipelineClauseKind::Match);
        assert_eq!(clauses[1].kind, PipelineClauseKind::Create);
        assert_eq!(clauses[2].kind, PipelineClauseKind::Create);
        assert_eq!(clauses[3].kind, PipelineClauseKind::With);
        assert_eq!(clauses[4].kind, PipelineClauseKind::Unwind);
        assert_eq!(clauses[5].kind, PipelineClauseKind::Match);
        assert_eq!(clauses[6].kind, PipelineClauseKind::Create);
        assert!(pending_pipeline_execution_todo().contains("dedicated eval/engine pipeline executor"));
    }
}