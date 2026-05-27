use std::collections::HashMap;

use serde_json::Value;

use crate::{CypherError, Parser};

pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<HashMap<String, Value>>,
}

impl QueryResult {
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
}

pub struct Executor {
    parser: Parser,
}

impl Executor {
    pub fn new() -> Self {
        Executor {
            parser: Parser::new(),
        }
    }

    /// Parse `cypher`, apply optional `params`, and return an (empty) result set.
    pub fn execute(
        &self,
        _ctx: &(),
        cypher: &str,
        params: Option<HashMap<String, Value>>,
    ) -> Result<QueryResult, CypherError> {
        let mut query = self.parser.parse(cypher)?;
        if let Some(p) = params {
            query.parameters = p;
        }
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        })
    }
}

impl Default for Executor {
    fn default() -> Self {
        Executor::new()
    }
}
