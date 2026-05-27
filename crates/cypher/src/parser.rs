use crate::{keyword_scan::starts_with_keyword_fold, tokenize, CypherError, ParseContext, Query};

pub struct Parser;

const SHALLOW_VALID_STARTS: &[&str] = &[
    "MATCH", "CREATE", "MERGE", "DELETE", "DETACH", "CALL", "RETURN", "WITH", "UNWIND", "OPTIONAL",
    "DROP", "SHOW", "FOREACH", "LOAD", "EXPLAIN", "PROFILE", "ALTER", "USE", "BEGIN", "COMMIT",
    "ROLLBACK",
];

fn validate_shallow_query(cypher: &str) -> Result<(), CypherError> {
    let cypher = cypher.trim();
    if cypher.is_empty() {
        return Err(CypherError::EmptyQuery);
    }

    if !SHALLOW_VALID_STARTS
        .iter()
        .any(|keyword| starts_with_keyword_fold(cypher, keyword))
    {
        return Err(CypherError::ParseError(
            "query must start with a valid clause".into(),
        ));
    }

    let bytes = cypher.as_bytes();
    let mut paren_depth = 0i32;
    let mut bracket_depth = 0i32;
    let mut brace_depth = 0i32;
    let mut in_string = false;
    let mut string_char = b'\0';

    let mut idx = 0usize;
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

        idx += 1;
    }

    if in_string {
        return Err(CypherError::UnterminatedString);
    }
    if paren_depth != 0 {
        return Err(CypherError::ParseError("unbalanced parentheses".into()));
    }
    if bracket_depth != 0 {
        return Err(CypherError::ParseError("unbalanced square brackets".into()));
    }
    if brace_depth != 0 {
        return Err(CypherError::ParseError("unbalanced curly braces".into()));
    }

    Ok(())
}

impl Parser {
    pub fn new() -> Self {
        Parser
    }

    #[doc(hidden)]
    pub fn tokenize_only<'a>(&self, cypher: &'a str) -> Result<Vec<&'a str>, CypherError> {
        if cypher.trim().is_empty() {
            return Err(CypherError::EmptyQuery);
        }

        let tokens = tokenize(cypher)?;
        if tokens.is_empty() {
            return Err(CypherError::EmptyQuery);
        }

        Ok(tokens)
    }

    #[doc(hidden)]
    pub fn validate_tokenized<'a>(&self, tokens: Vec<&'a str>) -> Result<(), CypherError> {
        if tokens.is_empty() {
            return Err(CypherError::EmptyQuery);
        }

        let mut ctx = ParseContext::new(tokens);
        ctx.validate_query()
    }

    #[doc(hidden)]
    pub fn parse_tokenized<'a>(&self, tokens: Vec<&'a str>) -> Result<Query, CypherError> {
        if tokens.is_empty() {
            return Err(CypherError::EmptyQuery);
        }

        let mut ctx = ParseContext::new(tokens);
        ctx.parse_query()
    }

    pub fn validate(&self, cypher: &str) -> Result<(), CypherError> {
        let tokens = self.tokenize_only(cypher)?;
        self.validate_tokenized(tokens)
    }

    pub fn validate_shallow(&self, cypher: &str) -> Result<(), CypherError> {
        validate_shallow_query(cypher)
    }

    pub fn parse(&self, cypher: &str) -> Result<Query, CypherError> {
        let tokens = self.tokenize_only(cypher)?;
        self.parse_tokenized(tokens)
    }
}

impl Default for Parser {
    fn default() -> Self {
        Parser::new()
    }
}
