use crate::{string_patterns::find_keyword_index, CypherError, EdgeDirection, Expression, QueryType};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyntaxClauseKind {
    Match,
    OptionalMatch,
    Create,
    Merge,
    Delete,
    DetachDelete,
    Set,
    Remove,
    Return,
    With,
    Where,
    Unwind,
    OrderBy,
    Limit,
    Skip,
    Call,
    Union,
    Foreach,
    Show,
    Drop,
    Alter,
    Load,
    Explain,
    Profile,
    Use,
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyntaxExprKind {
    Raw,
    Literal,
    Parameter,
    Variable,
    PropertyAccess,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyntaxExprRef<'a> {
    pub kind: SyntaxExprKind,
    pub raw_text: &'a str,
    pub variable: Option<&'a str>,
    pub property: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct SyntaxQuery<'a> {
    pub raw_query: &'a str,
    pub query_type: QueryType,
    pub is_read_only: bool,
    pub is_compound: bool,
    pub clauses: Vec<SyntaxClause<'a>>,
}

#[derive(Debug, Clone)]
pub struct SyntaxClause<'a> {
    pub kind: SyntaxClauseKind,
    pub raw_text: &'a str,
    pub start: usize,
    pub end: usize,
    pub body: &'a str,
    pub content: SyntaxClauseContent<'a>,
}

#[derive(Debug, Clone)]
pub enum SyntaxClauseContent<'a> {
    Match {
        optional: bool,
    },
    Create,
    Merge {
        pattern_range: std::ops::Range<usize>,
        on_create_range: Option<std::ops::Range<usize>>,
        on_match_range: Option<std::ops::Range<usize>>,
    },
    Delete {
        variables: Vec<&'a str>,
        detach: bool,
    },
    Set,
    Return {
        distinct: bool,
    },
    With {
        distinct: bool,
    },
    Where {
        expression: SyntaxExprRef<'a>,
    },
    Unwind {
        expression: SyntaxExprRef<'a>,
        variable: Option<&'a str>,
    },
    OrderBy,
    Call {
        procedure: &'a str,
        raw_args: &'a str,
        yield_raw: Option<&'a str>,
    },
    Limit {
        value: &'a str,
    },
    Skip {
        value: &'a str,
    },
    Raw,
}

#[derive(Debug, Clone)]
pub struct SyntaxPattern<'a> {
    pub raw_text: &'a str,
    pub nodes: Vec<SyntaxNode<'a>>,
    pub relationships: Vec<SyntaxRelationship<'a>>,
}

#[derive(Debug, Clone)]
pub struct SyntaxNode<'a> {
    pub variable: Option<&'a str>,
    pub labels: Vec<&'a str>,
    pub raw_properties: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct SyntaxRelationship<'a> {
    pub variable: Option<&'a str>,
    pub rel_type: Option<&'a str>,
    pub direction: EdgeDirection,
    pub raw_properties: Option<&'a str>,
    pub min_hops: Option<u32>,
    pub max_hops: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct SyntaxReturnItem<'a> {
    pub expression: SyntaxExprRef<'a>,
    pub alias: Option<&'a str>,
    pub raw_text: &'a str,
}

#[derive(Debug, Clone)]
pub struct SyntaxOrderItem<'a> {
    pub expression: SyntaxExprRef<'a>,
    pub descending: bool,
    pub raw_text: &'a str,
}

#[derive(Debug, Clone)]
pub struct SyntaxSetItem<'a> {
    pub variable: Option<&'a str>,
    pub property: Option<&'a str>,
    pub value: SyntaxExprRef<'a>,
    pub raw_text: &'a str,
}

struct ClauseKeyword {
    text: &'static str,
    kind: SyntaxClauseKind,
}

const OPTIONAL_MATCH: ClauseKeyword = ClauseKeyword {
    text: "OPTIONAL MATCH",
    kind: SyntaxClauseKind::OptionalMatch,
};
const DETACH_DELETE: ClauseKeyword = ClauseKeyword {
    text: "DETACH DELETE",
    kind: SyntaxClauseKind::DetachDelete,
};
const ORDER_BY: ClauseKeyword = ClauseKeyword {
    text: "ORDER BY",
    kind: SyntaxClauseKind::OrderBy,
};
const MATCH_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "MATCH",
    kind: SyntaxClauseKind::Match,
};
const CREATE_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "CREATE",
    kind: SyntaxClauseKind::Create,
};
const MERGE_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "MERGE",
    kind: SyntaxClauseKind::Merge,
};
const DELETE_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "DELETE",
    kind: SyntaxClauseKind::Delete,
};
const REMOVE_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "REMOVE",
    kind: SyntaxClauseKind::Remove,
};
const RETURN_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "RETURN",
    kind: SyntaxClauseKind::Return,
};
const WITH_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "WITH",
    kind: SyntaxClauseKind::With,
};
const WHERE_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "WHERE",
    kind: SyntaxClauseKind::Where,
};
const UNWIND_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "UNWIND",
    kind: SyntaxClauseKind::Unwind,
};
const LIMIT_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "LIMIT",
    kind: SyntaxClauseKind::Limit,
};
const SKIP_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "SKIP",
    kind: SyntaxClauseKind::Skip,
};
const CALL_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "CALL",
    kind: SyntaxClauseKind::Call,
};
const UNION_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "UNION",
    kind: SyntaxClauseKind::Union,
};
const FOREACH_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "FOREACH",
    kind: SyntaxClauseKind::Foreach,
};
const SHOW_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "SHOW",
    kind: SyntaxClauseKind::Show,
};
const DROP_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "DROP",
    kind: SyntaxClauseKind::Drop,
};
const ALTER_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "ALTER",
    kind: SyntaxClauseKind::Alter,
};
const LOAD_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "LOAD",
    kind: SyntaxClauseKind::Load,
};
const EXPLAIN_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "EXPLAIN",
    kind: SyntaxClauseKind::Explain,
};
const PROFILE_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "PROFILE",
    kind: SyntaxClauseKind::Profile,
};
const USE_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "USE",
    kind: SyntaxClauseKind::Use,
};
const BEGIN_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "BEGIN",
    kind: SyntaxClauseKind::Begin,
};
const COMMIT_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "COMMIT",
    kind: SyntaxClauseKind::Commit,
};
const ROLLBACK_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "ROLLBACK",
    kind: SyntaxClauseKind::Rollback,
};
const SET_KEYWORD: ClauseKeyword = ClauseKeyword {
    text: "SET",
    kind: SyntaxClauseKind::Set,
};

pub fn parse_syntax(cypher: &str) -> Result<SyntaxQuery<'_>, CypherError> {
    let raw_query = cypher.trim();
    if raw_query.is_empty() {
        return Err(CypherError::EmptyQuery);
    }

    let clauses = split_into_clauses(raw_query)?;
    let query_type = determine_query_type(&clauses);
    let is_read_only = clauses.iter().all(|clause| is_read_only_clause(clause.kind));

    Ok(SyntaxQuery {
        raw_query,
        query_type,
        is_read_only,
        is_compound: clauses.len() > 1,
        clauses,
    })
}

impl<'a> SyntaxClause<'a> {
    pub fn patterns(&self) -> Vec<SyntaxPattern<'a>> {
        match &self.content {
            SyntaxClauseContent::Match { .. } | SyntaxClauseContent::Create => {
                parse_patterns(self.body)
            }
            SyntaxClauseContent::Merge { pattern_range, .. } => {
                let pattern_text = self.body[pattern_range.clone()].trim();
                parse_patterns(pattern_text)
            }
            _ => Vec::new(),
        }
    }

    pub fn return_items(&self) -> Vec<SyntaxReturnItem<'a>> {
        match &self.content {
            SyntaxClauseContent::Return { distinct } | SyntaxClauseContent::With { distinct } => {
                let items_text = if *distinct { strip_distinct(self.body).1 } else { self.body };
                parse_return_items(items_text)
            }
            _ => Vec::new(),
        }
    }

    pub fn set_items(&self) -> Vec<SyntaxSetItem<'a>> {
        match &self.content {
            SyntaxClauseContent::Set => parse_set_items(self.body),
            SyntaxClauseContent::Merge {
                on_create_range,
                on_match_range,
                ..
            } => {
                let mut items = Vec::new();
                if let Some(range) = on_create_range {
                    items.extend(parse_set_items(self.body[range.clone()].trim()));
                }
                if let Some(range) = on_match_range {
                    items.extend(parse_set_items(self.body[range.clone()].trim()));
                }
                items
            }
            _ => Vec::new(),
        }
    }

    pub fn order_items(&self) -> Vec<SyntaxOrderItem<'a>> {
        match self.content {
            SyntaxClauseContent::OrderBy => parse_order_items(self.body),
            _ => Vec::new(),
        }
    }

    pub fn call_args(&self) -> Vec<SyntaxExprRef<'a>> {
        match &self.content {
            SyntaxClauseContent::Call { raw_args, .. } => split_top_level(raw_args, ',')
                .into_iter()
                .map(str::trim)
                .filter(|arg| !arg.is_empty())
                .map(classify_expression)
                .collect(),
            _ => Vec::new(),
        }
    }

    pub fn yield_items(&self) -> Vec<&'a str> {
        match &self.content {
            SyntaxClauseContent::Call {
                yield_raw: Some(yield_raw),
                ..
            } => split_top_level(yield_raw, ',')
                .into_iter()
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .collect(),
            _ => Vec::new(),
        }
    }
}

fn split_into_clauses(cypher: &str) -> Result<Vec<SyntaxClause<'_>>, CypherError> {
    let bytes = cypher.as_bytes();
    let mut boundaries: Vec<(usize, SyntaxClauseKind, &'static str)> = Vec::new();

    let mut idx = 0usize;
    let mut paren_depth = 0i32;
    let mut bracket_depth = 0i32;
    let mut brace_depth = 0i32;
    let mut in_string = false;
    let mut string_char = b'\0';

    while idx < bytes.len() {
        let byte = bytes[idx];

        if in_string {
            if byte == b'\\' && idx + 1 < bytes.len() {
                idx += 2;
                continue;
            }
            if byte == string_char {
                if idx + 1 < bytes.len() && bytes[idx + 1] == string_char {
                    idx += 2;
                    continue;
                }
                in_string = false;
            }
            idx += 1;
            continue;
        }

        match byte {
            b'\'' | b'"' => {
                in_string = true;
                string_char = byte;
                idx += 1;
                continue;
            }
            b'(' => paren_depth += 1,
            b')' => paren_depth -= 1,
            b'[' => bracket_depth += 1,
            b']' => bracket_depth -= 1,
            b'{' => brace_depth += 1,
            b'}' => brace_depth -= 1,
            _ => {}
        }

        if paren_depth < 0 || bracket_depth < 0 || brace_depth < 0 {
            return Err(CypherError::ParseError(format!(
                "unbalanced brackets near byte {}",
                idx
            )));
        }

        if paren_depth == 0
            && bracket_depth == 0
            && brace_depth == 0
            && is_boundary_start(bytes, idx)
        {
            if let Some(keyword) = candidate_keyword(cypher, idx) {
                boundaries.push((idx, keyword.kind, keyword.text));
                idx += keyword.text.len();
                continue;
            }
        }

        idx += 1;
    }

    if in_string {
        return Err(CypherError::UnterminatedString);
    }
    if paren_depth != 0 || bracket_depth != 0 || brace_depth != 0 {
        return Err(CypherError::ParseError("unbalanced brackets in query".into()));
    }
    if boundaries.is_empty() {
        return Err(CypherError::ParseError(
            "query must start with a valid clause".into(),
        ));
    }

    let mut clauses = Vec::with_capacity(boundaries.len());
    for (idx, (start, kind, keyword)) in boundaries.iter().copied().enumerate() {
        let next_start = boundaries
            .get(idx + 1)
            .map(|entry| entry.0)
            .unwrap_or(cypher.len());
        let (trimmed_start, trimmed_end, raw_text) = trimmed_segment(cypher, start, next_start);
        if raw_text.is_empty() {
            continue;
        }
        let body = trim_clause_body(raw_text, keyword);
        let content = parse_clause_content(kind, body);
        clauses.push(SyntaxClause {
            kind,
            raw_text,
            start: trimmed_start,
            end: trimmed_end,
            body,
            content,
        });
    }

    Ok(clauses)
}

fn candidate_keyword(s: &str, idx: usize) -> Option<&'static ClauseKeyword> {
    let candidates: &[ClauseKeyword] = match s.as_bytes()[idx].to_ascii_uppercase() {
        b'A' => &[ALTER_KEYWORD],
        b'B' => &[BEGIN_KEYWORD],
        b'C' => &[CREATE_KEYWORD, CALL_KEYWORD, COMMIT_KEYWORD],
        b'D' => &[DETACH_DELETE, DELETE_KEYWORD, DROP_KEYWORD],
        b'E' => &[EXPLAIN_KEYWORD],
        b'F' => &[FOREACH_KEYWORD],
        b'L' => &[LOAD_KEYWORD, LIMIT_KEYWORD],
        b'M' => &[MATCH_KEYWORD, MERGE_KEYWORD],
        b'O' => &[OPTIONAL_MATCH, ORDER_BY],
        b'P' => &[PROFILE_KEYWORD],
        b'R' => &[REMOVE_KEYWORD, RETURN_KEYWORD, ROLLBACK_KEYWORD],
        b'S' => &[SHOW_KEYWORD, SKIP_KEYWORD, SET_KEYWORD],
        b'U' => &[UNWIND_KEYWORD, UNION_KEYWORD, USE_KEYWORD],
        b'W' => &[WITH_KEYWORD, WHERE_KEYWORD],
        _ => &[],
    };

    candidates.iter().find(|keyword| {
        matches_keyword(s, idx, keyword.text) && !is_merge_modifier_keyword(s, idx, keyword.kind)
    })
}

fn parse_clause_content<'a>(kind: SyntaxClauseKind, body: &'a str) -> SyntaxClauseContent<'a> {
    match kind {
        SyntaxClauseKind::Match => SyntaxClauseContent::Match { optional: false },
        SyntaxClauseKind::OptionalMatch => SyntaxClauseContent::Match { optional: true },
        SyntaxClauseKind::Create => SyntaxClauseContent::Create,
        SyntaxClauseKind::Merge => parse_merge_clause(body),
        SyntaxClauseKind::Delete => SyntaxClauseContent::Delete {
            variables: split_top_level(body, ',')
                .into_iter()
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .collect(),
            detach: false,
        },
        SyntaxClauseKind::DetachDelete => SyntaxClauseContent::Delete {
            variables: split_top_level(body, ',')
                .into_iter()
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .collect(),
            detach: true,
        },
        SyntaxClauseKind::Set => SyntaxClauseContent::Set,
        SyntaxClauseKind::Return => {
            let (distinct, _) = strip_distinct(body);
            SyntaxClauseContent::Return { distinct }
        }
        SyntaxClauseKind::With => {
            let (distinct, _) = strip_distinct(body);
            SyntaxClauseContent::With { distinct }
        }
        SyntaxClauseKind::Where => SyntaxClauseContent::Where {
            expression: classify_expression(body),
        },
        SyntaxClauseKind::Unwind => {
            let (expr_text, variable) = parse_unwind_body(body);
            SyntaxClauseContent::Unwind {
                expression: classify_expression(expr_text),
                variable,
            }
        }
        SyntaxClauseKind::OrderBy => SyntaxClauseContent::OrderBy,
        SyntaxClauseKind::Call => parse_call_clause(body),
        SyntaxClauseKind::Limit => SyntaxClauseContent::Limit { value: body.trim() },
        SyntaxClauseKind::Skip => SyntaxClauseContent::Skip { value: body.trim() },
        _ => SyntaxClauseContent::Raw,
    }
}

fn parse_merge_clause(body: &str) -> SyntaxClauseContent<'_> {
    let on_create_idx = find_keyword_index(body, "ON CREATE SET");
    let on_match_idx = find_keyword_index(body, "ON MATCH SET");

    let mut pattern_end = body.len();
    if let Some(idx) = on_create_idx {
        pattern_end = pattern_end.min(idx);
    }
    if let Some(idx) = on_match_idx {
        pattern_end = pattern_end.min(idx);
    }

    let on_create_range = on_create_idx.map(|idx| {
        let end = on_match_idx.filter(|next| *next > idx).unwrap_or(body.len());
        idx + "ON CREATE SET".len()..end
    });
    let on_match_range = on_match_idx.map(|idx| idx + "ON MATCH SET".len()..body.len());

    SyntaxClauseContent::Merge {
        pattern_range: 0..pattern_end,
        on_create_range,
        on_match_range,
    }
}

fn parse_call_clause(body: &str) -> SyntaxClauseContent<'_> {
    let body = body.trim();
    let Some(open_paren) = body.find('(') else {
        return SyntaxClauseContent::Call {
            procedure: body,
            raw_args: "",
            yield_raw: None,
        };
    };
    let Some(close_paren) = find_matching_char(body, open_paren, '(', ')') else {
        return SyntaxClauseContent::Call {
            procedure: body,
            raw_args: "",
            yield_raw: None,
        };
    };

    let procedure = body[..open_paren].trim();
    let raw_args = body[open_paren + 1..close_paren].trim();
    let yield_raw = find_keyword_index(&body[close_paren + 1..], "YIELD")
        .map(|relative_idx| close_paren + 1 + relative_idx)
        .map(|yield_idx| body[yield_idx + "YIELD".len()..].trim())
        .filter(|yield_text| !yield_text.is_empty());

    SyntaxClauseContent::Call {
        procedure,
        raw_args,
        yield_raw,
    }
}

fn parse_unwind_body(body: &str) -> (&str, Option<&str>) {
    if let Some(as_idx) = find_keyword_index(body, "AS") {
        (
            body[..as_idx].trim(),
            Some(body[as_idx + "AS".len()..].trim()).filter(|value| !value.is_empty()),
        )
    } else {
        (body.trim(), None)
    }
}

fn parse_return_items(body: &str) -> Vec<SyntaxReturnItem<'_>> {
    split_top_level(body, ',')
        .into_iter()
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (expr_text, alias) = split_alias(part);
            SyntaxReturnItem {
                expression: classify_expression(expr_text),
                alias,
                raw_text: part,
            }
        })
        .collect()
}

fn parse_order_items(body: &str) -> Vec<SyntaxOrderItem<'_>> {
    split_top_level(body, ',')
        .into_iter()
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| {
            let upper = part.to_ascii_uppercase();
            let descending = upper.ends_with(" DESC");
            let expression = if descending || upper.ends_with(" ASC") {
                let trim_len = part.rfind(char::is_whitespace).unwrap_or(part.len());
                part[..trim_len].trim()
            } else {
                part
            };
            SyntaxOrderItem {
                expression: classify_expression(expression),
                descending,
                raw_text: part,
            }
        })
        .collect()
}

fn parse_set_items(body: &str) -> Vec<SyntaxSetItem<'_>> {
    split_top_level(body, ',')
        .into_iter()
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut variable = None;
            let mut property = None;
            let mut value_text = "";

            if let Some(eq_idx) = part.find('=') {
                let left = part[..eq_idx].trim().trim_end_matches('+').trim();
                value_text = part[eq_idx + 1..].trim();
                if let Some(dot_idx) = left.rfind('.') {
                    variable = Some(left[..dot_idx].trim()).filter(|value| !value.is_empty());
                    property = Some(left[dot_idx + 1..].trim()).filter(|value| !value.is_empty());
                } else {
                    variable = Some(left).filter(|value| !value.is_empty());
                }
            }

            SyntaxSetItem {
                variable,
                property,
                value: classify_expression(value_text),
                raw_text: part,
            }
        })
        .collect()
}

fn parse_patterns(body: &str) -> Vec<SyntaxPattern<'_>> {
    split_top_level(body, ',')
        .into_iter()
        .map(str::trim)
        .filter(|pattern| !pattern.is_empty())
        .map(parse_pattern)
        .collect()
}

fn parse_pattern(text: &str) -> SyntaxPattern<'_> {
    let mut nodes = Vec::new();
    let mut relationships = Vec::new();
    let bytes = text.as_bytes();
    let mut idx = 0usize;

    while idx < bytes.len() {
        match bytes[idx] {
            b'(' => {
                if let Some(end_idx) = find_matching_char(text, idx, '(', ')') {
                    nodes.push(parse_node(&text[idx + 1..end_idx]));
                    idx = end_idx + 1;
                    continue;
                }
            }
            b'<' | b'-' => {
                if let Some((relationship, next_idx)) = parse_relationship(text, idx) {
                    relationships.push(relationship);
                    idx = next_idx;
                    continue;
                }
            }
            _ => {}
        }
        idx += 1;
    }

    SyntaxPattern {
        raw_text: text,
        nodes,
        relationships,
    }
}

fn parse_node(inner: &str) -> SyntaxNode<'_> {
    let inner = inner.trim();
    let properties = extract_braced_section(inner);
    let header = if let Some(raw_props) = properties {
        inner[..inner.find(raw_props).unwrap_or(inner.len()) - 1].trim()
    } else {
        inner
    };

    let mut variable = None;
    let mut labels = Vec::new();
    let mut parts = header.split(':').map(str::trim).filter(|part| !part.is_empty());
    if let Some(first) = parts.next() {
        if is_identifier(first) {
            variable = Some(first);
        } else {
            labels.push(first.trim_start_matches(':'));
        }
    }
    for label in parts {
        labels.push(label.trim_start_matches(':'));
    }

    SyntaxNode {
        variable,
        labels,
        raw_properties: properties,
    }
}

fn parse_relationship(text: &str, start: usize) -> Option<(SyntaxRelationship<'_>, usize)> {
    let bytes = text.as_bytes();
    let mut idx = start;
    let mut prefix_incoming = false;

    if bytes.get(idx) == Some(&b'<') {
        prefix_incoming = true;
        idx += 1;
    }
    if bytes.get(idx) != Some(&b'-') {
        return None;
    }
    idx += 1;

    let inner = if bytes.get(idx) == Some(&b'[') {
        let end_idx = find_matching_char(text, idx, '[', ']')?;
        let inner = &text[idx + 1..end_idx];
        idx = end_idx + 1;
        inner
    } else {
        ""
    };

    if bytes.get(idx) != Some(&b'-') {
        return None;
    }
    idx += 1;
    let suffix_arrow = if bytes.get(idx) == Some(&b'>') {
        idx += 1;
        true
    } else {
        false
    };

    let direction = if prefix_incoming {
        EdgeDirection::Incoming
    } else if suffix_arrow {
        EdgeDirection::Outgoing
    } else {
        EdgeDirection::Both
    };

    let (variable, rel_type, raw_properties, min_hops, max_hops) = parse_relationship_inner(inner);
    Some((
        SyntaxRelationship {
            variable,
            rel_type,
            direction,
            raw_properties,
            min_hops,
            max_hops,
        },
        idx,
    ))
}

fn parse_relationship_inner(
    inner: &str,
) -> (
    Option<&str>,
    Option<&str>,
    Option<&str>,
    Option<u32>,
    Option<u32>,
) {
    let inner = inner.trim();
    let raw_properties = extract_braced_section(inner);
    let header = if let Some(raw_props) = raw_properties {
        inner[..inner.find(raw_props).unwrap_or(inner.len()) - 1].trim()
    } else {
        inner
    };

    let mut variable = None;
    let mut rel_type = None;
    let mut min_hops = None;
    let mut max_hops = None;

    let hop_idx = header.find('*');
    let type_end = hop_idx.unwrap_or(header.len());
    let type_part = header[..type_end].trim();
    if let Some(colon_idx) = type_part.find(':') {
        let left = type_part[..colon_idx].trim();
        let right = type_part[colon_idx + 1..].trim();
        if !left.is_empty() {
            variable = Some(left);
        }
        if !right.is_empty() {
            rel_type = Some(right);
        }
    } else if !type_part.is_empty() {
        variable = Some(type_part);
    }

    if let Some(hop_idx) = hop_idx {
        let hop_text = header[hop_idx + 1..].trim();
        if let Some(range_idx) = hop_text.find("..") {
            let min_text = hop_text[..range_idx].trim();
            let max_text = hop_text[range_idx + 2..].trim();
            min_hops = min_text.parse().ok().or(Some(1)).filter(|_| !min_text.is_empty() || max_text.is_empty());
            max_hops = max_text.parse().ok();
        } else if let Ok(hops) = hop_text.parse() {
            min_hops = Some(hops);
            max_hops = Some(hops);
        } else if hop_text.is_empty() {
            min_hops = Some(1);
        }
    }

    (variable, rel_type, raw_properties, min_hops, max_hops)
}

fn classify_expression(text: &str) -> SyntaxExprRef<'_> {
    let raw_text = text.trim();
    if raw_text.is_empty() {
        return SyntaxExprRef {
            kind: SyntaxExprKind::Raw,
            raw_text,
            variable: None,
            property: None,
        };
    }
    if is_quoted(raw_text) || is_numeric(raw_text) || is_bool_or_null(raw_text) {
        return SyntaxExprRef {
            kind: SyntaxExprKind::Literal,
            raw_text,
            variable: None,
            property: None,
        };
    }
    if let Some(parameter) = raw_text.strip_prefix('$') {
        if is_identifier(parameter) {
            return SyntaxExprRef {
                kind: SyntaxExprKind::Parameter,
                raw_text,
                variable: Some(parameter),
                property: None,
            };
        }
    }
    if let Some(dot_idx) = raw_text.find('.') {
        let variable = raw_text[..dot_idx].trim();
        let property = raw_text[dot_idx + 1..].trim();
        if is_identifier(variable) && is_identifier(property) {
            return SyntaxExprRef {
                kind: SyntaxExprKind::PropertyAccess,
                raw_text,
                variable: Some(variable),
                property: Some(property),
            };
        }
    }
    if is_identifier(raw_text) {
        return SyntaxExprRef {
            kind: SyntaxExprKind::Variable,
            raw_text,
            variable: Some(raw_text),
            property: None,
        };
    }
    SyntaxExprRef {
        kind: SyntaxExprKind::Raw,
        raw_text,
        variable: None,
        property: None,
    }
}

fn strip_distinct(text: &str) -> (bool, &str) {
    if text.len() >= 8 && text[..8].eq_ignore_ascii_case("DISTINCT") {
        (true, text[8..].trim())
    } else {
        (false, text.trim())
    }
}

fn split_alias(text: &str) -> (&str, Option<&str>) {
    if let Some(as_idx) = find_keyword_index(text, "AS") {
        let alias = text[as_idx + "AS".len()..].trim();
        if !alias.is_empty() {
            return (text[..as_idx].trim(), Some(alias));
        }
    }
    (text.trim(), None)
}

fn split_top_level(s: &str, delimiter: char) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut paren_depth = 0i32;
    let mut bracket_depth = 0i32;
    let mut brace_depth = 0i32;
    let mut in_string = false;
    let mut string_char = '\0';

    for (idx, ch) in s.char_indices() {
        if in_string {
            if ch == string_char {
                in_string = false;
            } else if ch == '\\' {
                continue;
            }
            continue;
        }

        match ch {
            '\'' | '"' => {
                in_string = true;
                string_char = ch;
            }
            '(' => paren_depth += 1,
            ')' => paren_depth -= 1,
            '[' => bracket_depth += 1,
            ']' => bracket_depth -= 1,
            '{' => brace_depth += 1,
            '}' => brace_depth -= 1,
            _ => {}
        }

        if ch == delimiter && paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 {
            out.push(&s[start..idx]);
            start = idx + ch.len_utf8();
        }
    }

    out.push(&s[start..]);
    out
}

fn trimmed_segment(s: &str, start: usize, end: usize) -> (usize, usize, &str) {
    let segment = &s[start..end];
    let trim_start = segment.len() - segment.trim_start().len();
    let trim_end = segment.trim_end().len();
    let trimmed_start = start + trim_start;
    let trimmed_end = start + trim_end;
    (trimmed_start, trimmed_end, &s[trimmed_start..trimmed_end])
}

fn trim_clause_body<'a>(raw_text: &'a str, keyword: &str) -> &'a str {
    raw_text[keyword.len()..].trim()
}

fn matches_keyword(s: &str, idx: usize, keyword: &str) -> bool {
    let bytes = s.as_bytes();
    let keyword_bytes = keyword.as_bytes();
    if idx + keyword_bytes.len() > bytes.len() {
        return false;
    }
    if !bytes[idx..idx + keyword_bytes.len()]
        .iter()
        .zip(keyword_bytes.iter())
        .all(|(left, right)| left.eq_ignore_ascii_case(right))
    {
        return false;
    }
    is_boundary_end(bytes, idx + keyword_bytes.len())
}

fn is_merge_modifier_keyword(s: &str, idx: usize, kind: SyntaxClauseKind) -> bool {
    if !matches!(kind, SyntaxClauseKind::Create | SyntaxClauseKind::Set) {
        return false;
    }
    let prefix = s[..idx].trim_end();
    prefix.ends_with("ON") || prefix.ends_with("ON CREATE") || prefix.ends_with("ON MATCH")
}

fn is_boundary_start(bytes: &[u8], idx: usize) -> bool {
    idx == 0 || !is_word_char(bytes[idx - 1])
}

fn is_boundary_end(bytes: &[u8], idx: usize) -> bool {
    idx >= bytes.len() || !is_word_char(bytes[idx])
}

fn is_word_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn find_matching_char(s: &str, start: usize, open: char, close: char) -> Option<usize> {
    let mut depth = 0i32;
    let mut in_string = false;
    let mut string_char = '\0';

    for (idx, ch) in s.char_indices().skip_while(|(idx, _)| *idx < start) {
        if in_string {
            if ch == string_char {
                in_string = false;
            }
            continue;
        }
        if ch == '\'' || ch == '"' {
            in_string = true;
            string_char = ch;
            continue;
        }
        if ch == open {
            depth += 1;
        } else if ch == close {
            depth -= 1;
            if depth == 0 {
                return Some(idx);
            }
        }
    }
    None
}

fn extract_braced_section(s: &str) -> Option<&str> {
    let start = s.find('{')?;
    let end = find_matching_char(s, start, '{', '}')?;
    Some(s[start + 1..end].trim())
}

fn determine_query_type(clauses: &[SyntaxClause<'_>]) -> QueryType {
    for clause in clauses {
        match clause.kind {
            SyntaxClauseKind::Match | SyntaxClauseKind::OptionalMatch => return QueryType::Match,
            SyntaxClauseKind::Create => return QueryType::Create,
            SyntaxClauseKind::Merge => return QueryType::Merge,
            SyntaxClauseKind::Delete | SyntaxClauseKind::DetachDelete => {
                return QueryType::Delete;
            }
            SyntaxClauseKind::Set => return QueryType::Set,
            SyntaxClauseKind::Return => return QueryType::Return,
            SyntaxClauseKind::With => return QueryType::With,
            SyntaxClauseKind::Show
            | SyntaxClauseKind::Drop
            | SyntaxClauseKind::Alter
            | SyntaxClauseKind::Use
            | SyntaxClauseKind::Begin
            | SyntaxClauseKind::Commit
            | SyntaxClauseKind::Rollback => return QueryType::Ddl,
            _ => {}
        }
    }
    QueryType::Match
}

fn is_read_only_clause(kind: SyntaxClauseKind) -> bool {
    matches!(
        kind,
        SyntaxClauseKind::Match
            | SyntaxClauseKind::OptionalMatch
            | SyntaxClauseKind::Return
            | SyntaxClauseKind::With
            | SyntaxClauseKind::Where
            | SyntaxClauseKind::OrderBy
            | SyntaxClauseKind::Limit
            | SyntaxClauseKind::Skip
            | SyntaxClauseKind::Call
            | SyntaxClauseKind::Show
            | SyntaxClauseKind::Explain
            | SyntaxClauseKind::Profile
            | SyntaxClauseKind::Use
    )
}

fn is_identifier(text: &str) -> bool {
    let mut chars = text.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn is_quoted(text: &str) -> bool {
    (text.starts_with('\'') && text.ends_with('\'')) || (text.starts_with('"') && text.ends_with('"'))
}

fn is_numeric(text: &str) -> bool {
    text.parse::<i64>().is_ok() || text.parse::<f64>().is_ok()
}

fn is_bool_or_null(text: &str) -> bool {
    text.eq_ignore_ascii_case("true")
        || text.eq_ignore_ascii_case("false")
        || text.eq_ignore_ascii_case("null")
}

pub(crate) fn parse_expression_text(text: &str) -> Result<Expression, CypherError> {
    use crate::parse_context::ParseContext;

    let tokens = crate::tokenize(text)?;
    if tokens.is_empty() {
        return Err(CypherError::ParseError("empty expression".into()));
    }

    let mut ctx = ParseContext::new(tokens);
    let expression = ctx.parse_expression()?;
    if ctx.peek().is_some() {
        return Err(CypherError::ParseError(
            "unexpected trailing tokens after expression".into(),
        ));
    }
    Ok(expression)
}

#[cfg(test)]
mod tests {
    use super::{parse_expression_text, parse_syntax, SyntaxClauseContent, SyntaxClauseKind, SyntaxExprKind};

    #[test]
    fn syntax_ir_splits_top_level_clauses() {
        let syntax = parse_syntax("MATCH (n {name: 'RETURN'}) WHERE n.age > 1 RETURN n.name ORDER BY n.name").unwrap();
        assert_eq!(syntax.clauses.len(), 4);
        assert_eq!(syntax.clauses[0].kind, SyntaxClauseKind::Match);
        assert_eq!(syntax.clauses[1].kind, SyntaxClauseKind::Where);
        assert_eq!(syntax.clauses[2].kind, SyntaxClauseKind::Return);
        assert_eq!(syntax.clauses[3].kind, SyntaxClauseKind::OrderBy);
    }

    #[test]
    fn syntax_ir_keeps_complex_return_expression_raw() {
        let syntax = parse_syntax("RETURN coalesce(n.name, 'unknown') AS name").unwrap();
        match &syntax.clauses[0].content {
            SyntaxClauseContent::Return { .. } => {
                let items = syntax.clauses[0].return_items();
                assert_eq!(items[0].expression.kind, SyntaxExprKind::Raw);
                assert_eq!(items[0].alias, Some("name"));
            }
            other => panic!("unexpected content: {other:?}"),
        }
    }

    #[test]
    fn syntax_ir_promotes_expression_lazily() {
        let expression = parse_expression_text("n.age >= 21").unwrap();
        assert!(matches!(expression, crate::Expression::Comparison { .. }));
    }
}