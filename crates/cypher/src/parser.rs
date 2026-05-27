use crate::{
    keyword_scan::starts_with_keyword_fold, parse_syntax, syntax_ir::parse_expression_text,
    tokenize, CypherError, Expression, ParseContext, Query, SyntaxClause, SyntaxExprRef,
    SyntaxQuery,
};

pub struct Parser;

const SHALLOW_A_KEYWORDS: &[&str] = &["ALTER"];
const SHALLOW_B_KEYWORDS: &[&str] = &["BEGIN"];
const SHALLOW_C_KEYWORDS: &[&str] = &["CREATE", "CALL", "COMMIT"];
const SHALLOW_D_KEYWORDS: &[&str] = &["DETACH", "DELETE", "DROP"];
const SHALLOW_E_KEYWORDS: &[&str] = &["EXPLAIN"];
const SHALLOW_F_KEYWORDS: &[&str] = &["FOREACH"];
const SHALLOW_L_KEYWORDS: &[&str] = &["LOAD"];
const SHALLOW_M_KEYWORDS: &[&str] = &["MATCH", "MERGE"];
const SHALLOW_O_KEYWORDS: &[&str] = &["OPTIONAL"];
const SHALLOW_P_KEYWORDS: &[&str] = &["PROFILE"];
const SHALLOW_R_KEYWORDS: &[&str] = &["RETURN", "ROLLBACK"];
const SHALLOW_S_KEYWORDS: &[&str] = &["SHOW"];
const SHALLOW_U_KEYWORDS: &[&str] = &["UNWIND", "USE"];
const SHALLOW_W_KEYWORDS: &[&str] = &["WITH"];

fn has_valid_shallow_start(cypher: &str) -> bool {
    let Some(&first) = cypher.as_bytes().first() else {
        return false;
    };

    let candidates = match first.to_ascii_uppercase() {
        b'A' => SHALLOW_A_KEYWORDS,
        b'B' => SHALLOW_B_KEYWORDS,
        b'C' => SHALLOW_C_KEYWORDS,
        b'D' => SHALLOW_D_KEYWORDS,
        b'E' => SHALLOW_E_KEYWORDS,
        b'F' => SHALLOW_F_KEYWORDS,
        b'L' => SHALLOW_L_KEYWORDS,
        b'M' => SHALLOW_M_KEYWORDS,
        b'O' => SHALLOW_O_KEYWORDS,
        b'P' => SHALLOW_P_KEYWORDS,
        b'R' => SHALLOW_R_KEYWORDS,
        b'S' => SHALLOW_S_KEYWORDS,
        b'U' => SHALLOW_U_KEYWORDS,
        b'W' => SHALLOW_W_KEYWORDS,
        _ => return false,
    };

    candidates
        .iter()
        .any(|keyword| starts_with_keyword_fold(cypher, keyword))
}

fn validate_shallow_query(cypher: &str) -> Result<(), CypherError> {
    let cypher = cypher.trim();
    if cypher.is_empty() {
        return Err(CypherError::EmptyQuery);
    }

    if !has_valid_shallow_start(cypher) {
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
            if byte == string_char && (idx == 0 || bytes[idx - 1] != b'\\') {
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

    pub fn parse_syntax<'a>(&self, cypher: &'a str) -> Result<SyntaxQuery<'a>, CypherError> {
        parse_syntax(cypher)
    }

    pub fn promote_syntax_query(&self, syntax: &SyntaxQuery<'_>) -> Result<Query, CypherError> {
        self.parse(syntax.raw_query)
    }

    pub fn promote_syntax_clause(&self, clause: &SyntaxClause<'_>) -> Result<Query, CypherError> {
        self.parse(clause.raw_text)
    }

    pub fn promote_syntax_expression(
        &self,
        expression: &SyntaxExprRef<'_>,
    ) -> Result<Expression, CypherError> {
        parse_expression_text(expression.raw_text)
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
