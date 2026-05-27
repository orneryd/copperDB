use crate::{tokenize, CypherError, ParseContext, Query};

pub struct Parser;

impl Parser {
    pub fn new() -> Self {
        Parser
    }

    pub fn parse(&self, cypher: &str) -> Result<Query, CypherError> {
        if cypher.trim().is_empty() {
            return Err(CypherError::EmptyQuery);
        }

        let tokens = tokenize(cypher)?;
        if tokens.is_empty() {
            return Err(CypherError::EmptyQuery);
        }

        let mut ctx = ParseContext::new(tokens);
        ctx.parse_query()
    }
}

impl Default for Parser {
    fn default() -> Self {
        Parser::new()
    }
}
