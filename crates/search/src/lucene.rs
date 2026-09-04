//! Typed parsing and evaluation for the Lucene-classic query shapes accepted
//! by Neo4j full-text procedures.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use regex::RegexBuilder;

#[derive(Debug, Clone, PartialEq)]
pub struct FulltextQuery {
    pub root: QueryNode,
}

impl FulltextQuery {
    pub fn is_empty(&self) -> bool {
        matches!(&self.root, QueryNode::Empty)
    }

    /// Terms that can seed an exact inverted-index candidate lookup.
    /// Operators needing vocabulary expansion return `None` explicitly.
    pub fn primary_terms(&self) -> Option<Vec<String>> {
        let mut terms = BTreeSet::new();
        let has_positive_term = collect_primary_terms(&self.root, &mut terms)?;
        has_positive_term.then(|| terms.into_iter().collect())
    }

    /// Expand a query into normalized terms from a complete bounded index
    /// vocabulary. Callers must reject truncated vocabularies rather than use
    /// these candidates against a partial universe.
    pub fn expand_candidate_terms(
        &self,
        vocabulary: &[String],
    ) -> Result<Vec<String>, FulltextQueryError> {
        if self.is_empty() {
            return Ok(Vec::new());
        }
        let vocabulary = vocabulary
            .iter()
            .map(|term| term.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        let mut terms = BTreeSet::new();
        let has_positive_term = expand_candidate_terms(&self.root, &vocabulary, &mut terms)?;
        if has_positive_term {
            Ok(terms.into_iter().collect())
        } else {
            // A pure-negative query must evaluate every indexed candidate so
            // prohibited clauses can exclude only their matching documents.
            Ok(vocabulary.into_iter().collect())
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryNode {
    Empty,
    Term {
        field: Option<String>,
        value: String,
    },
    Phrase {
        field: Option<String>,
        value: String,
        slop: Option<u32>,
    },
    Presence {
        field: String,
    },
    Wildcard {
        field: Option<String>,
        value: String,
    },
    Fuzzy {
        field: Option<String>,
        value: String,
        max_edits: u8,
    },
    Regex {
        field: Option<String>,
        value: String,
    },
    Range {
        field: Option<String>,
        lower: String,
        upper: String,
        include_lower: bool,
        include_upper: bool,
    },
    Boolean {
        operator: BooleanOperator,
        clauses: Vec<QueryNode>,
    },
    Required(Box<QueryNode>),
    Prohibited(Box<QueryNode>),
    Boost {
        query: Box<QueryNode>,
        factor: f32,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BooleanOperator {
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FulltextQueryError {
    message: String,
}

impl fmt::Display for FulltextQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid Lucene full-text query: {}",
            self.message
        )
    }
}

impl std::error::Error for FulltextQueryError {}

impl FulltextQueryError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

pub fn parse_fulltext_query(input: &str) -> Result<FulltextQuery, FulltextQueryError> {
    let tokens = tokenize(input)?;
    if matches!(tokens.as_slice(), [Token::End]) {
        return Ok(FulltextQuery {
            root: QueryNode::Empty,
        });
    }
    let mut parser = Parser {
        tokens,
        position: 0,
    };
    let root = parser.parse_or()?;
    if !matches!(parser.peek(), Token::End) {
        return Err(FulltextQueryError::new("unexpected trailing input"));
    }
    Ok(FulltextQuery { root })
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FulltextDocument {
    fields: BTreeMap<String, String>,
}

impl FulltextDocument {
    pub fn from_fields(fields: impl IntoIterator<Item = (String, String)>) -> Self {
        Self {
            fields: fields
                .into_iter()
                .map(|(field, value)| (field.to_ascii_lowercase(), value.to_ascii_lowercase()))
                .collect(),
        }
    }

    fn fields_for<'a>(&'a self, field: Option<&str>) -> impl Iterator<Item = &'a str> {
        match field {
            Some(field) => self
                .fields
                .get(&field.to_ascii_lowercase())
                .map(String::as_str)
                .into_iter()
                .collect::<Vec<_>>(),
            None => self.fields.values().map(String::as_str).collect(),
        }
        .into_iter()
    }

    fn has_field(&self, field: &str) -> bool {
        self.fields
            .get(&field.to_ascii_lowercase())
            .is_some_and(|value| !value.is_empty())
    }
}

pub fn evaluate_fulltext_query(
    query: &FulltextQuery,
    document: &FulltextDocument,
) -> Result<Option<f64>, FulltextQueryError> {
    evaluate_node(&query.root, document)
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Term(String),
    Phrase(String),
    Regex(String),
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Colon,
    Plus,
    Minus,
    Caret,
    Tilde,
    And,
    Or,
    Not,
    To,
    End,
}

fn tokenize(input: &str) -> Result<Vec<Token>, FulltextQueryError> {
    let bytes = input.as_bytes();
    let mut tokens = Vec::new();
    let mut position = 0;
    while position < bytes.len() {
        if bytes[position].is_ascii_whitespace() {
            position += 1;
            continue;
        }
        let token = match bytes[position] {
            b'(' => Token::LParen,
            b')' => Token::RParen,
            b'[' => Token::LBracket,
            b']' => Token::RBracket,
            b'{' => Token::LBrace,
            b'}' => Token::RBrace,
            b':' => Token::Colon,
            b'+' => Token::Plus,
            b'-' => Token::Minus,
            b'^' => Token::Caret,
            b'~' => Token::Tilde,
            b'&' if bytes.get(position + 1) == Some(&b'&') => {
                position += 1;
                Token::And
            }
            b'|' if bytes.get(position + 1) == Some(&b'|') => {
                position += 1;
                Token::Or
            }
            b'"' => {
                position += 1;
                Token::Phrase(read_delimited(bytes, &mut position, b'"', false)?)
            }
            b'/' => {
                position += 1;
                Token::Regex(read_delimited(bytes, &mut position, b'/', true)?)
            }
            _ => {
                let value = read_term(bytes, &mut position)?;
                match value.as_str() {
                    "AND" => Token::And,
                    "OR" => Token::Or,
                    "NOT" => Token::Not,
                    "TO" => Token::To,
                    _ => Token::Term(value),
                }
            }
        };
        tokens.push(token);
        position += 1;
    }
    tokens.push(Token::End);
    Ok(tokens)
}

fn read_delimited(
    bytes: &[u8],
    position: &mut usize,
    delimiter: u8,
    preserve_escapes: bool,
) -> Result<String, FulltextQueryError> {
    let mut value = String::new();
    while *position < bytes.len() {
        match bytes[*position] {
            byte if byte == delimiter => return Ok(value),
            b'\\' => {
                let Some(escaped) = bytes.get(*position + 1) else {
                    return Err(FulltextQueryError::new("unterminated escape sequence"));
                };
                if preserve_escapes {
                    value.push('\\');
                }
                value.push(*escaped as char);
                *position += 2;
                continue;
            }
            byte => value.push(byte as char),
        }
        *position += 1;
    }
    Err(FulltextQueryError::new("unterminated quoted expression"))
}

fn read_term(bytes: &[u8], position: &mut usize) -> Result<String, FulltextQueryError> {
    let mut value = String::new();
    while *position < bytes.len() {
        match bytes[*position] {
            byte if byte.is_ascii_whitespace() || b"()[]{}:+-^~\"/&|".contains(&byte) => break,
            b'\\' => {
                let Some(escaped) = bytes.get(*position + 1) else {
                    return Err(FulltextQueryError::new("unterminated escape sequence"));
                };
                value.push(*escaped as char);
                *position += 2;
                continue;
            }
            byte => value.push(byte as char),
        }
        *position += 1;
    }
    if value.is_empty() {
        return Err(FulltextQueryError::new("unexpected character"));
    }
    *position = position.saturating_sub(1);
    Ok(value)
}

struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    fn peek(&self) -> &Token {
        &self.tokens[self.position]
    }

    fn advance(&mut self) -> Token {
        let token = self.tokens[self.position].clone();
        if !matches!(token, Token::End) {
            self.position += 1;
        }
        token
    }

    fn parse_or(&mut self) -> Result<QueryNode, FulltextQueryError> {
        let mut clauses = vec![self.parse_and()?];
        while matches!(self.peek(), Token::Or) {
            self.advance();
            clauses.push(self.parse_and()?);
        }
        collapse_boolean(BooleanOperator::Or, clauses)
    }

    fn parse_and(&mut self) -> Result<QueryNode, FulltextQueryError> {
        let mut clauses = vec![self.parse_clause()?];
        let mut explicit_and = false;
        loop {
            if matches!(self.peek(), Token::And) {
                explicit_and = true;
                self.advance();
                clauses.push(self.parse_clause()?);
            } else if starts_clause(self.peek()) {
                clauses.push(self.parse_clause()?);
            } else {
                break;
            }
        }
        if explicit_and {
            collapse_boolean(BooleanOperator::And, clauses)
        } else {
            collapse_boolean(BooleanOperator::Or, clauses)
        }
    }

    fn parse_clause(&mut self) -> Result<QueryNode, FulltextQueryError> {
        let modifier = match self.peek() {
            Token::Plus => Some(true),
            Token::Minus | Token::Not => Some(false),
            _ => None,
        };
        if modifier.is_some() {
            self.advance();
        }
        let mut query = self.parse_primary()?;
        if matches!(self.peek(), Token::Caret) {
            self.advance();
            let Token::Term(factor) = self.advance() else {
                return Err(FulltextQueryError::new("expected numeric boost after ^"));
            };
            let factor = factor
                .parse::<f32>()
                .map_err(|_| FulltextQueryError::new("invalid boost factor"))?;
            query = QueryNode::Boost {
                query: Box::new(query),
                factor,
            };
        }
        Ok(match modifier {
            Some(true) => QueryNode::Required(Box::new(query)),
            Some(false) => QueryNode::Prohibited(Box::new(query)),
            None => query,
        })
    }

    fn parse_primary(&mut self) -> Result<QueryNode, FulltextQueryError> {
        if matches!(self.peek(), Token::LParen) {
            self.advance();
            let query = self.parse_or()?;
            self.expect("missing ')'", |token| matches!(token, Token::RParen))?;
            return Ok(query);
        }
        if let (Token::Term(field), Token::Colon) = (self.peek().clone(), self.peek_at(1).clone()) {
            self.advance();
            self.advance();
            return self.parse_field_value(field);
        }
        self.parse_atom(None)
    }

    fn parse_field_value(&mut self, field: String) -> Result<QueryNode, FulltextQueryError> {
        if matches!(self.peek(), Token::LParen) {
            self.advance();
            let query = rebind_field(self.parse_or()?, &field);
            self.expect("missing ')'", |token| matches!(token, Token::RParen))?;
            return Ok(query);
        }
        if matches!(self.peek(), Token::LBracket | Token::LBrace) {
            return self.parse_range(field);
        }
        self.parse_atom(Some(field))
    }

    fn parse_range(&mut self, field: String) -> Result<QueryNode, FulltextQueryError> {
        let include_lower = matches!(self.advance(), Token::LBracket);
        let lower = self.range_endpoint()?;
        self.expect("expected TO in range", |token| matches!(token, Token::To))?;
        let upper = self.range_endpoint()?;
        let include_upper = match self.advance() {
            Token::RBracket => true,
            Token::RBrace => false,
            _ => return Err(FulltextQueryError::new("expected ] or } to close range")),
        };
        Ok(QueryNode::Range {
            field: Some(field),
            lower,
            upper,
            include_lower,
            include_upper,
        })
    }

    fn range_endpoint(&mut self) -> Result<String, FulltextQueryError> {
        match self.advance() {
            Token::Term(value) | Token::Phrase(value) => Ok(value),
            _ => Err(FulltextQueryError::new("expected range endpoint")),
        }
    }

    fn parse_atom(&mut self, field: Option<String>) -> Result<QueryNode, FulltextQueryError> {
        match self.advance() {
            Token::Term(value) if field.is_some() && value == "*" => Ok(QueryNode::Presence {
                field: field.unwrap(),
            }),
            Token::Term(value) if value.contains('*') || value.contains('?') => {
                Ok(QueryNode::Wildcard { field, value })
            }
            Token::Term(value) => {
                if matches!(self.peek(), Token::Tilde) {
                    self.advance();
                    let max_edits = match self.peek() {
                        Token::Term(distance) => distance
                            .parse::<u8>()
                            .map_err(|_| FulltextQueryError::new("invalid fuzzy edit distance"))?,
                        _ => 2,
                    };
                    if matches!(self.peek(), Token::Term(_)) {
                        self.advance();
                    }
                    Ok(QueryNode::Fuzzy {
                        field,
                        value,
                        max_edits,
                    })
                } else {
                    Ok(QueryNode::Term { field, value })
                }
            }
            Token::Phrase(value) => {
                let slop = if matches!(self.peek(), Token::Tilde) {
                    self.advance();
                    match self.peek() {
                        Token::Term(distance) => Some(
                            distance
                                .parse::<u32>()
                                .map_err(|_| FulltextQueryError::new("invalid phrase proximity"))?,
                        ),
                        _ => Some(0),
                    }
                } else {
                    None
                };
                if slop.is_some() && matches!(self.peek(), Token::Term(_)) {
                    self.advance();
                }
                Ok(QueryNode::Phrase { field, value, slop })
            }
            Token::Regex(value) => Ok(QueryNode::Regex { field, value }),
            _ => Err(FulltextQueryError::new(
                "expected term, phrase, group, or regex",
            )),
        }
    }

    fn expect(
        &mut self,
        message: &'static str,
        predicate: impl FnOnce(&Token) -> bool,
    ) -> Result<(), FulltextQueryError> {
        if predicate(self.peek()) {
            self.advance();
            Ok(())
        } else {
            Err(FulltextQueryError::new(message))
        }
    }

    fn peek_at(&self, offset: usize) -> &Token {
        self.tokens
            .get(self.position + offset)
            .unwrap_or(&Token::End)
    }
}

fn collapse_boolean(
    operator: BooleanOperator,
    clauses: Vec<QueryNode>,
) -> Result<QueryNode, FulltextQueryError> {
    match clauses.len() {
        0 => Err(FulltextQueryError::new("query is empty")),
        1 => Ok(clauses.into_iter().next().unwrap()),
        _ => Ok(QueryNode::Boolean { operator, clauses }),
    }
}

fn starts_clause(token: &Token) -> bool {
    matches!(
        token,
        Token::Term(_)
            | Token::Phrase(_)
            | Token::Regex(_)
            | Token::LParen
            | Token::Plus
            | Token::Minus
            | Token::Not
    )
}

fn rebind_field(query: QueryNode, field: &str) -> QueryNode {
    match query {
        QueryNode::Term { field: None, value } => QueryNode::Term {
            field: Some(field.into()),
            value,
        },
        QueryNode::Phrase {
            field: None,
            value,
            slop,
        } => QueryNode::Phrase {
            field: Some(field.into()),
            value,
            slop,
        },
        QueryNode::Presence { field: existing } if existing.is_empty() => QueryNode::Presence {
            field: field.into(),
        },
        QueryNode::Wildcard { field: None, value } => QueryNode::Wildcard {
            field: Some(field.into()),
            value,
        },
        QueryNode::Fuzzy {
            field: None,
            value,
            max_edits,
        } => QueryNode::Fuzzy {
            field: Some(field.into()),
            value,
            max_edits,
        },
        QueryNode::Regex { field: None, value } => QueryNode::Regex {
            field: Some(field.into()),
            value,
        },
        QueryNode::Boolean { operator, clauses } => QueryNode::Boolean {
            operator,
            clauses: clauses
                .into_iter()
                .map(|clause| rebind_field(clause, field))
                .collect(),
        },
        QueryNode::Required(query) => QueryNode::Required(Box::new(rebind_field(*query, field))),
        QueryNode::Prohibited(query) => {
            QueryNode::Prohibited(Box::new(rebind_field(*query, field)))
        }
        QueryNode::Boost { query, factor } => QueryNode::Boost {
            query: Box::new(rebind_field(*query, field)),
            factor,
        },
        query => query,
    }
}

fn collect_primary_terms(query: &QueryNode, terms: &mut BTreeSet<String>) -> Option<bool> {
    match query {
        QueryNode::Empty => Some(false),
        QueryNode::Term { value, .. } => {
            terms.insert(value.to_ascii_lowercase());
            Some(true)
        }
        QueryNode::Phrase { value, slop, .. } if slop.is_none() || *slop == Some(0) => {
            terms.extend(fulltext_tokens(&value.to_ascii_lowercase()).map(str::to_owned));
            Some(true)
        }
        QueryNode::Boolean { clauses, .. } => {
            let mut has_positive_term = false;
            for clause in clauses {
                has_positive_term |= collect_primary_terms(clause, terms)?;
            }
            Some(has_positive_term)
        }
        QueryNode::Required(query) | QueryNode::Boost { query, .. } => {
            collect_primary_terms(query, terms)
        }
        QueryNode::Prohibited(_) => Some(false),
        QueryNode::Presence { .. }
        | QueryNode::Wildcard { .. }
        | QueryNode::Fuzzy { .. }
        | QueryNode::Regex { .. }
        | QueryNode::Range { .. }
        | QueryNode::Phrase { .. } => None,
    }
}

fn expand_candidate_terms(
    query: &QueryNode,
    vocabulary: &BTreeSet<String>,
    terms: &mut BTreeSet<String>,
) -> Result<bool, FulltextQueryError> {
    match query {
        QueryNode::Empty => Ok(false),
        QueryNode::Term { value, .. } => {
            let value = value.to_ascii_lowercase();
            if vocabulary.contains(&value) {
                terms.insert(value);
            }
            Ok(true)
        }
        QueryNode::Phrase { value, .. } => {
            let phrase = value.to_ascii_lowercase();
            terms.extend(
                fulltext_tokens(&phrase)
                    .filter(|term| vocabulary.contains(*term))
                    .map(str::to_owned),
            );
            Ok(true)
        }
        QueryNode::Presence { .. } => {
            terms.extend(vocabulary.iter().cloned());
            Ok(true)
        }
        QueryNode::Wildcard { value, .. } => {
            terms.extend(
                vocabulary
                    .iter()
                    .filter(|term| wildcard_matches(value, term))
                    .cloned(),
            );
            Ok(true)
        }
        QueryNode::Fuzzy {
            value, max_edits, ..
        } => {
            let value = value.to_ascii_lowercase();
            terms.extend(
                vocabulary
                    .iter()
                    .filter(|term| levenshtein_distance(&value, term) <= usize::from(*max_edits))
                    .cloned(),
            );
            Ok(true)
        }
        QueryNode::Regex { value, .. } => {
            let regex = RegexBuilder::new(value)
                .case_insensitive(true)
                .build()
                .map_err(|error| FulltextQueryError::new(format!("invalid regex: {error}")))?;
            terms.extend(
                vocabulary
                    .iter()
                    .filter(|term| regex.is_match(term))
                    .cloned(),
            );
            Ok(true)
        }
        QueryNode::Range {
            lower,
            upper,
            include_lower,
            include_upper,
            ..
        } => {
            let lower = lower.to_ascii_lowercase();
            let upper = upper.to_ascii_lowercase();
            terms.extend(
                vocabulary
                    .iter()
                    .filter(|term| {
                        let above_lower = if *include_lower {
                            *term >= &lower
                        } else {
                            *term > &lower
                        };
                        let below_upper = if *include_upper {
                            *term <= &upper
                        } else {
                            *term < &upper
                        };
                        above_lower && below_upper
                    })
                    .cloned(),
            );
            Ok(true)
        }
        QueryNode::Boolean { clauses, .. } => {
            let mut has_positive_term = false;
            for clause in clauses {
                has_positive_term |= expand_candidate_terms(clause, vocabulary, terms)?;
            }
            Ok(has_positive_term)
        }
        QueryNode::Required(query) | QueryNode::Boost { query, .. } => {
            expand_candidate_terms(query, vocabulary, terms)
        }
        QueryNode::Prohibited(_) => Ok(false),
    }
}

fn evaluate_node(
    query: &QueryNode,
    document: &FulltextDocument,
) -> Result<Option<f64>, FulltextQueryError> {
    match query {
        QueryNode::Empty => Ok(None),
        QueryNode::Term { field, value } => {
            let matched = document.fields_for(field.as_deref()).any(|text| {
                text.split(|character: char| !character.is_alphanumeric() && character != '_')
                    .any(|token| token == value.to_ascii_lowercase())
            });
            Ok(matched.then_some(1.0))
        }
        QueryNode::Phrase { field, value, slop } => {
            let matched = match slop {
                Some(slop) if *slop > 0 => document
                    .fields_for(field.as_deref())
                    .any(|text| phrase_matches_with_slop(text, value, *slop)),
                _ => {
                    let phrase = value.to_ascii_lowercase();
                    document
                        .fields_for(field.as_deref())
                        .any(|text| text.contains(&phrase))
                }
            };
            Ok(matched.then_some(1.0))
        }
        QueryNode::Presence { field } => Ok(document.has_field(field).then_some(1.0)),
        QueryNode::Boolean { operator, clauses } => evaluate_boolean(*operator, clauses, document),
        QueryNode::Required(query) => evaluate_node(query, document),
        QueryNode::Prohibited(query) => {
            Ok(evaluate_node(query, document)?.is_none().then_some(0.0))
        }
        QueryNode::Boost { query, factor } => {
            Ok(evaluate_node(query, document)?.map(|score| score * f64::from(*factor)))
        }
        QueryNode::Wildcard { field, value } => Ok(document
            .fields_for(field.as_deref())
            .flat_map(fulltext_tokens)
            .any(|token| wildcard_matches(value, token))
            .then_some(1.0)),
        QueryNode::Fuzzy {
            field,
            value,
            max_edits,
        } => Ok(document
            .fields_for(field.as_deref())
            .flat_map(fulltext_tokens)
            .any(|token| levenshtein_distance(value, token) <= usize::from(*max_edits))
            .then_some(1.0)),
        QueryNode::Regex { field, value } => {
            let regex = RegexBuilder::new(value)
                .case_insensitive(true)
                .build()
                .map_err(|error| FulltextQueryError::new(format!("invalid regex: {error}")))?;
            Ok(document
                .fields_for(field.as_deref())
                .any(|text| regex.is_match(text))
                .then_some(1.0))
        }
        QueryNode::Range {
            field,
            lower,
            upper,
            include_lower,
            include_upper,
        } => {
            let lower = lower.to_ascii_lowercase();
            let upper = upper.to_ascii_lowercase();
            Ok(document
                .fields_for(field.as_deref())
                .any(|text| {
                    let above_lower = if *include_lower {
                        text >= lower.as_str()
                    } else {
                        text > lower.as_str()
                    };
                    let below_upper = if *include_upper {
                        text <= upper.as_str()
                    } else {
                        text < upper.as_str()
                    };
                    above_lower && below_upper
                })
                .then_some(1.0))
        }
    }
}

fn fulltext_tokens(text: &str) -> impl Iterator<Item = &str> {
    text.split(|character: char| !character.is_alphanumeric() && character != '_')
        .filter(|token| !token.is_empty())
}

fn wildcard_matches(pattern: &str, value: &str) -> bool {
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();
    let mut table = vec![vec![false; value.len() + 1]; pattern.len() + 1];
    table[0][0] = true;
    for index in 1..=pattern.len() {
        table[index][0] = pattern[index - 1] == b'*' && table[index - 1][0];
    }
    for pattern_index in 1..=pattern.len() {
        for value_index in 1..=value.len() {
            table[pattern_index][value_index] = match pattern[pattern_index - 1] {
                b'*' => {
                    table[pattern_index - 1][value_index] || table[pattern_index][value_index - 1]
                }
                b'?' => table[pattern_index - 1][value_index - 1],
                character => {
                    character.eq_ignore_ascii_case(&value[value_index - 1])
                        && table[pattern_index - 1][value_index - 1]
                }
            };
        }
    }
    table[pattern.len()][value.len()]
}

fn levenshtein_distance(left: &str, right: &str) -> usize {
    let right_chars = right.chars().collect::<Vec<_>>();
    let mut previous = (0..=right_chars.len()).collect::<Vec<_>>();
    for (left_index, left_char) in left.chars().enumerate() {
        let mut current = vec![left_index + 1];
        for (right_index, right_char) in right_chars.iter().enumerate() {
            let substitution = previous[right_index] + usize::from(left_char != *right_char);
            current.push(
                (current[right_index] + 1)
                    .min(previous[right_index + 1] + 1)
                    .min(substitution),
            );
        }
        previous = current;
    }
    previous[right_chars.len()]
}

fn phrase_matches_with_slop(text: &str, phrase: &str, slop: u32) -> bool {
    let normalized_phrase = phrase.to_ascii_lowercase();
    let phrase_tokens = fulltext_tokens(&normalized_phrase).collect::<Vec<_>>();
    if phrase_tokens.is_empty() {
        return false;
    }
    let tokens = fulltext_tokens(text).collect::<Vec<_>>();
    for (start, token) in tokens.iter().enumerate() {
        if *token != phrase_tokens[0] {
            continue;
        }
        let mut last = start;
        let mut gaps = 0u32;
        let mut matched = true;
        for phrase_token in phrase_tokens.iter().skip(1) {
            let Some(next) =
                ((last + 1)..tokens.len()).find(|index| tokens[*index] == *phrase_token)
            else {
                matched = false;
                break;
            };
            gaps += (next - last - 1) as u32;
            if gaps > slop {
                matched = false;
                break;
            }
            last = next;
        }
        if matched {
            return true;
        }
    }
    false
}

fn evaluate_boolean(
    operator: BooleanOperator,
    clauses: &[QueryNode],
    document: &FulltextDocument,
) -> Result<Option<f64>, FulltextQueryError> {
    match operator {
        BooleanOperator::And => {
            let mut score = 0.0;
            for clause in clauses {
                let Some(clause_score) = evaluate_node(clause, document)? else {
                    return Ok(None);
                };
                score += clause_score;
            }
            Ok(Some(score))
        }
        BooleanOperator::Or => {
            let mut score = 0.0;
            let mut matched_optional = false;
            let mut has_required = false;
            for clause in clauses {
                match clause {
                    QueryNode::Required(query) => {
                        has_required = true;
                        let Some(clause_score) = evaluate_node(query, document)? else {
                            return Ok(None);
                        };
                        score += clause_score;
                    }
                    QueryNode::Prohibited(query) => {
                        if evaluate_node(query, document)?.is_some() {
                            return Ok(None);
                        }
                    }
                    _ => {
                        if let Some(clause_score) = evaluate_node(clause, document)? {
                            matched_optional = true;
                            score += clause_score;
                        }
                    }
                }
            }
            if has_required || matched_optional {
                Ok(Some(score))
            } else {
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nornicdb_fulltext_query_parsing_benchmark_inputs() {
        for input in [
            "simple query",
            "\"exact phrase\"",
            "word1 \"phrase one\" word2 \"phrase two\"",
            "complex AND query OR \"multiple phrases\" NOT excluded",
        ] {
            assert!(
                parse_fulltext_query(input).is_ok(),
                "NornicDB benchmark input must parse: {input}"
            );
        }
    }

    #[test]
    fn parses_boolean_precedence_and_implicit_or() {
        assert_eq!(
            parse_fulltext_query("alpha AND beta OR gamma")
                .unwrap()
                .root,
            QueryNode::Boolean {
                operator: BooleanOperator::Or,
                clauses: vec![
                    QueryNode::Boolean {
                        operator: BooleanOperator::And,
                        clauses: vec![
                            QueryNode::Term {
                                field: None,
                                value: "alpha".into()
                            },
                            QueryNode::Term {
                                field: None,
                                value: "beta".into()
                            },
                        ],
                    },
                    QueryNode::Term {
                        field: None,
                        value: "gamma".into()
                    },
                ],
            }
        );
        assert!(matches!(
            parse_fulltext_query("alpha beta").unwrap().root,
            QueryNode::Boolean {
                operator: BooleanOperator::Or,
                ..
            }
        ));
        assert_eq!(
            parse_fulltext_query("alpha && beta || gamma").unwrap().root,
            QueryNode::Boolean {
                operator: BooleanOperator::Or,
                clauses: vec![
                    QueryNode::Boolean {
                        operator: BooleanOperator::And,
                        clauses: vec![
                            QueryNode::Term {
                                field: None,
                                value: "alpha".into()
                            },
                            QueryNode::Term {
                                field: None,
                                value: "beta".into()
                            },
                        ],
                    },
                    QueryNode::Term {
                        field: None,
                        value: "gamma".into()
                    },
                ],
            }
        );
    }

    #[test]
    fn parses_field_scopes_groups_ranges_and_modifiers() {
        let parsed =
            parse_fulltext_query("+group:(alpha OR \"beta gamma\") -state:[a TO z}^2").unwrap();
        let QueryNode::Boolean {
            operator: BooleanOperator::Or,
            clauses,
        } = parsed.root
        else {
            panic!("expected an implicit OR group");
        };
        assert!(matches!(clauses[0], QueryNode::Required(_)));
        assert!(matches!(clauses[1], QueryNode::Prohibited(_)));
        let QueryNode::Prohibited(query) = &clauses[1] else {
            unreachable!()
        };
        assert!(matches!(
            query.as_ref(),
            QueryNode::Boost { query, factor }
                if *factor == 2.0 && matches!(query.as_ref(), QueryNode::Range { field: Some(field), include_lower: true, include_upper: false, .. } if field == "state")
        ));
    }

    #[test]
    fn rejects_malformed_structural_input() {
        for query in ["(alpha", "field:[a z]", "alpha^word", "\"unterminated"] {
            assert!(parse_fulltext_query(query).is_err(), "{query} should fail");
        }
    }

    #[test]
    fn parses_empty_queries_as_no_match() {
        for input in ["", "   ", "\t\n"] {
            let query = parse_fulltext_query(input).unwrap();
            assert!(query.is_empty());
            assert_eq!(
                evaluate_fulltext_query(&query, &FulltextDocument::default()).unwrap(),
                None
            );
            assert!(
                query
                    .expand_candidate_terms(&["alpha".into()])
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[test]
    fn decodes_lucene_escapes_in_terms_and_phrases() {
        assert_eq!(
            parse_fulltext_query(r"name:Cloud\ Trail").unwrap().root,
            QueryNode::Term {
                field: Some("name".into()),
                value: "Cloud Trail".into()
            }
        );
        assert_eq!(
            parse_fulltext_query(r#""quoted \"phrase\"""#).unwrap().root,
            QueryNode::Phrase {
                field: None,
                value: "quoted \"phrase\"".into(),
                slop: None
            }
        );
    }

    #[test]
    fn parses_wildcard_fuzzy_and_phrase_proximity_modifiers() {
        assert_eq!(
            parse_fulltext_query("name:*trail").unwrap().root,
            QueryNode::Wildcard {
                field: Some("name".into()),
                value: "*trail".into()
            }
        );
        assert_eq!(
            parse_fulltext_query("cloud~1").unwrap().root,
            QueryNode::Fuzzy {
                field: None,
                value: "cloud".into(),
                max_edits: 1
            }
        );
        assert_eq!(
            parse_fulltext_query("\"cloud trail\"~3").unwrap().root,
            QueryNode::Phrase {
                field: None,
                value: "cloud trail".into(),
                slop: Some(3)
            }
        );
    }

    #[test]
    fn evaluates_terms_fields_phrases_and_boolean_occurrence() {
        let document = FulltextDocument::from_fields([
            ("name".into(), "CloudTrail".into()),
            ("summary".into(), "AWS audit logging".into()),
            ("group_id".into(), "ft_repro".into()),
        ]);

        for query in [
            "group_id:ft_repro AND (CloudTrail OR Redis)",
            "group_id:ft_repro && CloudTrail || Redis",
            "group_id:\"ft_repro\" AND +CloudTrail -Redis",
            "summary:\"aws audit\"",
            "group_id:*",
        ] {
            let query = parse_fulltext_query(query).unwrap();
            assert!(
                evaluate_fulltext_query(&query, &document)
                    .unwrap()
                    .is_some(),
                "{query:?}"
            );
        }
        let query = parse_fulltext_query("group_id:other AND CloudTrail").unwrap();
        assert_eq!(evaluate_fulltext_query(&query, &document).unwrap(), None);
        for query in ["*", "group_id:*"] {
            let query = parse_fulltext_query(query).unwrap();
            assert!(
                evaluate_fulltext_query(&query, &document)
                    .unwrap()
                    .is_some(),
                "{query:?}"
            );
        }
    }

    #[test]
    fn evaluates_wildcard_fuzzy_regex_ranges_and_phrase_proximity() {
        let document = FulltextDocument::from_fields([
            ("name".into(), "CloudTrail".into()),
            ("summary".into(), "cloud audit trail".into()),
            ("rank".into(), "m".into()),
        ]);
        for query in [
            "name:*trail",
            "cloudtrai~1",
            "name:/cloud.*/",
            "rank:[a TO z]",
            "summary:\"cloud trail\"~1",
        ] {
            let query = parse_fulltext_query(query).unwrap();
            assert!(
                evaluate_fulltext_query(&query, &document)
                    .unwrap()
                    .is_some(),
                "{query:?}"
            );
        }
    }

    #[test]
    fn mirrors_nornicdb_lucene_parser_and_evaluator_matrix() {
        let documents = [
            FulltextDocument::from_fields([
                ("content".into(), "alpha beta gamma cloudtrail".into()),
                ("name".into(), "cloudtrail".into()),
                ("summary".into(), "hello world".into()),
                ("group_id".into(), "g1".into()),
                ("tag".into(), "gold".into()),
                ("rank".into(), "apple".into()),
            ]),
            FulltextDocument::from_fields([
                ("content".into(), "alpha delta cloudy cloudtale".into()),
                ("name".into(), "cloudtale".into()),
                ("summary".into(), "hello there".into()),
                ("group_id".into(), "g1".into()),
                ("tag".into(), "".into()),
                ("rank".into(), "banana".into()),
            ]),
            FulltextDocument::from_fields([
                ("content".into(), "gamma delta sunny".into()),
                ("name".into(), "ladybug".into()),
                ("summary".into(), "different record".into()),
                ("group_id".into(), "g2".into()),
                ("rank".into(), "cherry".into()),
            ]),
        ];

        for (input, expected) in [
            ("alpha OR gamma", vec![0, 1, 2]),
            ("alpha || gamma", vec![0, 1, 2]),
            ("alpha AND beta", vec![0]),
            ("alpha && beta", vec![0]),
            ("alpha AND NOT beta", vec![1]),
            ("alpha -beta", vec![1]),
            ("NOT beta", vec![1, 2]),
            ("+alpha gamma", vec![0, 1]),
            ("\"alpha beta\"", vec![0]),
            ("\"alpha gamma\"~1", vec![0]),
            ("name:cloudtrail", vec![0]),
            ("name:(cloudtrail OR cloudtale)", vec![0, 1]),
            ("tag:*", vec![0]),
            ("unknown:*", vec![]),
            ("cloud*", vec![0, 1]),
            ("*trail", vec![0]),
            ("clo?dy", vec![1]),
            ("clou*ail", vec![0]),
            ("rank:[apple TO cherry]", vec![0, 1, 2]),
            ("rank:{apple TO cherry}", vec![1]),
            ("rank:[apple TO cherry}", vec![0, 1]),
            ("rank:{apple TO cherry]", vec![1, 2]),
            ("name:/cloud.*/", vec![0, 1]),
            ("cloudtrail~", vec![0]),
            ("cloudtrail~0", vec![0]),
            ("cloudtrail~3", vec![0, 1]),
            (r"Cloud\Trail", vec![0]),
            ("totally_unknown:whatever AND alpha", vec![]),
            ("((alpha OR gamma) AND NOT beta)", vec![1, 2]),
        ] {
            let query = parse_fulltext_query(input).unwrap();
            let actual = documents
                .iter()
                .enumerate()
                .filter_map(|(index, document)| {
                    evaluate_fulltext_query(&query, document)
                        .unwrap()
                        .is_some()
                        .then_some(index)
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, expected, "query={input}");
        }

        let document = &documents[0];
        let bare_score = evaluate_fulltext_query(&parse_fulltext_query("alpha").unwrap(), document)
            .unwrap()
            .unwrap();
        let boosted_score =
            evaluate_fulltext_query(&parse_fulltext_query("alpha^2").unwrap(), document)
                .unwrap()
                .unwrap();
        assert!(boosted_score > bare_score);
    }

    #[test]
    fn exposes_exact_terms_for_index_candidate_planning() {
        assert_eq!(
            parse_fulltext_query("group_id:ft_repro AND \"CloudTrail audit\"")
                .unwrap()
                .primary_terms(),
            Some(vec!["audit".into(), "cloudtrail".into(), "ft_repro".into()])
        );
        assert_eq!(
            parse_fulltext_query("name:*trail").unwrap().primary_terms(),
            None
        );
        assert_eq!(
            parse_fulltext_query("-retired").unwrap().primary_terms(),
            None
        );
    }

    #[test]
    fn expands_advanced_terms_from_bounded_vocabulary() {
        let vocabulary = vec![
            "audit".into(),
            "cloudtrail".into(),
            "cloudwatch".into(),
            "redis".into(),
        ];
        assert_eq!(
            parse_fulltext_query("cloudtrai~1 OR /redis/")
                .unwrap()
                .expand_candidate_terms(&vocabulary)
                .unwrap(),
            vec!["cloudtrail", "redis"]
        );
        assert_eq!(
            parse_fulltext_query("cloud*")
                .unwrap()
                .expand_candidate_terms(&vocabulary)
                .unwrap(),
            vec!["cloudtrail", "cloudwatch"]
        );
        assert_eq!(
            parse_fulltext_query("-redis")
                .unwrap()
                .expand_candidate_terms(&vocabulary)
                .unwrap(),
            vocabulary
        );
    }
}
