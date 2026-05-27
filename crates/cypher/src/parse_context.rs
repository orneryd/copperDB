use crate::CypherError;

pub(crate) struct ParseContext {
    pub(crate) tokens: Vec<String>,
    pub(crate) pos: usize,
}

impl ParseContext {
    pub(crate) fn new(tokens: Vec<String>) -> Self {
        ParseContext { tokens, pos: 0 }
    }

    pub(crate) fn peek(&self) -> Option<&str> {
        self.tokens.get(self.pos).map(|s| s.as_str())
    }

    pub(crate) fn peek_upper(&self) -> Option<String> {
        self.peek().map(|s| s.to_uppercase())
    }

    pub(crate) fn advance(&mut self) -> Option<&str> {
        let t = self.tokens.get(self.pos).map(|s| s.as_str());
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    pub(crate) fn expect(&mut self, expected: &str) -> Result<(), CypherError> {
        match self.advance() {
            Some(t) if t.eq_ignore_ascii_case(expected) => Ok(()),
            Some(t) => Err(CypherError::ParseError(format!(
                "expected '{}', got '{}'",
                expected, t
            ))),
            None => Err(CypherError::ParseError(format!(
                "expected '{}', got end of input",
                expected
            ))),
        }
    }

    /// Advance and return the next token as an identifier.
    pub(crate) fn advance_identifier(&mut self) -> Result<String, CypherError> {
        match self.advance() {
            Some(t) => Ok(t.to_string()),
            None => Err(CypherError::ParseError(
                "expected identifier, got end of input".into(),
            )),
        }
    }
}
