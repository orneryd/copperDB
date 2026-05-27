use crate::CypherError;

pub(crate) struct ParseContext<'a> {
    pub(crate) tokens: Vec<&'a str>,
    pub(crate) pos: usize,
}

impl<'a> ParseContext<'a> {
    pub(crate) fn new(tokens: Vec<&'a str>) -> Self {
        ParseContext { tokens, pos: 0 }
    }

    pub(crate) fn peek(&self) -> Option<&'a str> {
        self.tokens.get(self.pos).copied()
    }

    pub(crate) fn peek_is(&self, expected: &str) -> bool {
        matches!(self.peek(), Some(token) if token.eq_ignore_ascii_case(expected))
    }

    pub(crate) fn peek_is_one_of(&self, expected: &[&str]) -> bool {
        matches!(
            self.peek(),
            Some(token) if expected.iter().any(|item| token.eq_ignore_ascii_case(item))
        )
    }

    pub(crate) fn advance(&mut self) -> Option<&'a str> {
        let t = self.tokens.get(self.pos).copied();
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

    pub(crate) fn expect_identifier(&mut self) -> Result<&'a str, CypherError> {
        self.advance()
            .ok_or_else(|| CypherError::ParseError("expected identifier, got end of input".into()))
    }

    /// Advance and return the next token as an identifier.
    pub(crate) fn advance_identifier(&mut self) -> Result<String, CypherError> {
        self.expect_identifier().map(str::to_string)
    }
}
