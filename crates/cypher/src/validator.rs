use crate::{CypherError, Query, parse_context::ParseContext, parser_support::is_keyword};

impl<'a> ParseContext<'a> {
    pub(crate) fn validate_query(&mut self) -> Result<(), CypherError> {
        let mut saw_clause = false;

        while self.pos < self.tokens.len() {
            let token = match self.peek() {
                Some(token) => token,
                None => break,
            };

            match token {
                token if token.eq_ignore_ascii_case("CALL") => {
                    saw_clause = true;
                    self.advance();
                    self.validate_call()?;
                }
                token if token.eq_ignore_ascii_case("MATCH") => {
                    saw_clause = true;
                    self.advance();
                    self.validate_match(true)?;
                }
                token if token.eq_ignore_ascii_case("OPTIONAL") => {
                    saw_clause = true;
                    self.advance();
                    self.expect("MATCH")?;
                    self.validate_match(true)?;
                }
                token if token.eq_ignore_ascii_case("CREATE") => {
                    saw_clause = true;
                    if self.tokens.get(self.pos + 1).is_some_and(|next| {
                        next.eq_ignore_ascii_case("CONSTRAINT")
                            || next.eq_ignore_ascii_case("INDEX")
                            || next.eq_ignore_ascii_case("DECAY")
                            || next.eq_ignore_ascii_case("PROMOTION")
                    }) {
                        self.parse_query().map(|_: Query| ())?;
                        return Ok(());
                    }
                    self.advance();
                    self.validate_create()?;
                }
                token if token.eq_ignore_ascii_case("MERGE") => {
                    saw_clause = true;
                    self.advance();
                    self.validate_merge()?;
                }
                token if token.eq_ignore_ascii_case("RETURN") => {
                    saw_clause = true;
                    self.advance();
                    self.validate_return()?;
                }
                token if token.eq_ignore_ascii_case("WHERE") => {
                    saw_clause = true;
                    self.advance();
                    self.validate_where()?;
                }
                token if token.eq_ignore_ascii_case("SET") => {
                    saw_clause = true;
                    self.advance();
                    self.validate_set()?;
                }
                token if token.eq_ignore_ascii_case("DELETE") => {
                    saw_clause = true;
                    self.advance();
                    self.validate_delete()?;
                }
                token if token.eq_ignore_ascii_case("DETACH") => {
                    saw_clause = true;
                    self.advance();
                    self.expect("DELETE")?;
                    self.validate_delete()?;
                }
                token if token.eq_ignore_ascii_case("WITH") => {
                    saw_clause = true;
                    self.advance();
                    self.validate_with()?;
                }
                token if token.eq_ignore_ascii_case("UNWIND") => {
                    saw_clause = true;
                    self.advance();
                    self.validate_unwind()?;
                }
                token
                    if token.eq_ignore_ascii_case("DROP")
                        || token.eq_ignore_ascii_case("SHOW")
                        || token.eq_ignore_ascii_case("ALTER") =>
                {
                    self.parse_query().map(|_: Query| ())?;
                    return Ok(());
                }
                _ => {
                    return Err(CypherError::ParseError(format!(
                        "unexpected token '{}'",
                        token
                    )));
                }
            }
        }

        if !saw_clause {
            return Err(CypherError::ParseError("no recognisable clause".into()));
        }

        Ok(())
    }

    fn validate_match(&mut self, allow_shortest_path: bool) -> Result<(), CypherError> {
        self.validate_pattern_with_optional_path_variable(allow_shortest_path)
    }

    fn validate_create(&mut self) -> Result<(), CypherError> {
        self.validate_pattern_with_optional_path_variable(false)
    }

    fn validate_merge(&mut self) -> Result<(), CypherError> {
        self.validate_pattern_with_optional_path_variable(false)
    }

    fn validate_call(&mut self) -> Result<(), CypherError> {
        self.expect_identifier()?;
        while self.peek() == Some(".") {
            self.advance();
            self.expect_identifier()?;
        }
        self.expect("(")?;
        if self.peek() != Some(")") {
            self.validate_expression()?;
            while self.peek() == Some(",") {
                self.advance();
                self.validate_expression()?;
            }
        }
        self.expect(")")
    }

    fn validate_return(&mut self) -> Result<(), CypherError> {
        if self.peek_is("DISTINCT") {
            self.advance();
        }

        self.validate_return_item()?;
        while self.peek() == Some(",") {
            self.advance();
            self.validate_return_item()?;
        }

        if self.peek_is("ORDER") {
            self.advance();
            self.expect("BY")?;
            self.validate_order_item()?;
            while self.peek() == Some(",") {
                self.advance();
                self.validate_order_item()?;
            }
        }

        loop {
            if self.peek_is("SKIP") || self.peek_is("LIMIT") {
                self.advance();
                self.validate_i64()?;
            } else {
                break;
            }
        }

        Ok(())
    }

    fn validate_return_item(&mut self) -> Result<(), CypherError> {
        self.validate_expression()?;
        if self.peek_is("AS") {
            self.advance();
            self.expect_identifier()?;
        }
        Ok(())
    }

    fn validate_order_item(&mut self) -> Result<(), CypherError> {
        self.validate_expression()?;
        if self.peek_is_one_of(&["DESC", "DESCENDING", "ASC", "ASCENDING"]) {
            self.advance();
        }
        Ok(())
    }

    fn validate_where(&mut self) -> Result<(), CypherError> {
        self.validate_expression()
    }

    fn validate_set(&mut self) -> Result<(), CypherError> {
        self.validate_set_item()?;
        while self.peek() == Some(",") {
            self.advance();
            self.validate_set_item()?;
        }
        Ok(())
    }

    fn validate_set_item(&mut self) -> Result<(), CypherError> {
        self.expect_identifier()?;
        // Map-merge form: SET n += expr
        if self.peek() == Some("+=") {
            self.advance();
            return self.validate_expression();
        }
        // Map-assignment: SET n = expr
        if self.peek() == Some("=") {
            self.advance();
            return self.validate_expression();
        }
        // Label form: SET n:Label or SET n:$(expr)
        if self.peek() == Some(":") {
            self.advance();
            if self.peek() == Some("$") {
                let next_is_paren = self
                    .tokens
                    .get(self.pos + 1)
                    .map(|t| *t == "(")
                    .unwrap_or(false);
                if next_is_paren {
                    self.advance(); // consume $
                    self.advance(); // consume (
                    self.validate_expression()?;
                    self.expect(")")?;
                    return Ok(());
                }
            }
            self.expect_identifier()?;
            return Ok(());
        }
        // Property form: SET n.prop = expr
        self.expect(".")?;
        self.expect_identifier()?;
        self.expect("=")?;
        self.validate_expression()
    }

    fn validate_delete(&mut self) -> Result<(), CypherError> {
        self.expect_identifier()?;
        while self.peek() == Some(",") {
            self.advance();
            self.expect_identifier()?;
        }
        Ok(())
    }

    fn validate_with(&mut self) -> Result<(), CypherError> {
        self.validate_return_item()?;
        while self.peek() == Some(",") {
            self.advance();
            self.validate_return_item()?;
        }

        if self.peek_is("WHERE") {
            self.advance();
            self.validate_where()?;
        }

        if self.peek_is("ORDER") {
            self.advance();
            self.expect("BY")?;
            self.validate_order_item()?;
            while self.peek() == Some(",") {
                self.advance();
                self.validate_order_item()?;
            }
        }

        loop {
            if self.peek_is("SKIP") || self.peek_is("LIMIT") {
                self.advance();
                self.validate_i64()?;
            } else {
                break;
            }
        }

        Ok(())
    }

    fn validate_unwind(&mut self) -> Result<(), CypherError> {
        self.validate_expression()?;
        self.expect("AS")?;
        self.expect_identifier()?;
        Ok(())
    }

    fn validate_pattern_with_optional_path_variable(
        &mut self,
        allow_shortest_path: bool,
    ) -> Result<(), CypherError> {
        let has_path_variable = self.tokens.get(self.pos + 1).copied() == Some("=");
        if has_path_variable {
            self.expect_identifier()?;
            self.expect("=")?;
        }

        let segment_count = if allow_shortest_path && self.peek_is("SHORTESTPATH") {
            self.advance();
            self.expect("(")?;
            let segment_count = self.validate_pattern()?;
            self.expect(")")?;
            if segment_count != 1 {
                return Err(CypherError::ParseError(
                    "shortestPath requires a single connected pattern".to_string(),
                ));
            }
            segment_count
        } else if allow_shortest_path && self.peek_is("ALLSHORTESTPATHS") {
            self.advance();
            self.expect("(")?;
            let segment_count = self.validate_pattern()?;
            self.expect(")")?;
            if segment_count != 1 {
                return Err(CypherError::ParseError(
                    "allShortestPaths requires a single connected pattern".to_string(),
                ));
            }
            segment_count
        } else {
            self.validate_pattern()?
        };

        if has_path_variable && segment_count != 1 {
            return Err(CypherError::ParseError(
                "path variables require a single connected pattern".to_string(),
            ));
        }

        Ok(())
    }

    fn validate_pattern(&mut self) -> Result<usize, CypherError> {
        let mut segment_count = 0_usize;
        loop {
            self.expect("(")?;
            self.validate_node_inner()?;
            segment_count += 1;

            loop {
                let next = self.peek();
                if next == Some("-") || next == Some("<") {
                    if !self.validate_try_parse_edge()? {
                        break;
                    }
                    self.expect("(")?;
                    self.validate_node_inner()?;
                } else {
                    break;
                }
            }

            if self.peek() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }

        Ok(segment_count)
    }

    fn validate_node_inner(&mut self) -> Result<(), CypherError> {
        if let Some(token) = self.peek()
            && token != ")"
            && token != ":"
            && token != "{"
            && !is_keyword(token)
        {
            self.advance();
        }

        while self.peek() == Some(":") {
            self.advance();
            self.expect_identifier()?;
        }

        if self.peek() == Some("{") {
            self.advance();
            self.validate_properties_map()?;
        }

        self.expect(")")
    }

    fn validate_try_parse_edge(&mut self) -> Result<bool, CypherError> {
        let saved_pos = self.pos;

        let prefix_incoming = if self.peek() == Some("<") {
            self.advance();
            if self.peek() != Some("-") {
                self.pos = saved_pos;
                return Ok(false);
            }
            self.advance();
            true
        } else {
            self.advance();
            false
        };

        if self.peek() != Some("[") {
            self.pos = saved_pos;
            return Ok(false);
        }
        self.advance();

        self.validate_edge_inner()?;
        self.expect("]")?;

        let suffix_arrow = if self.peek() == Some("-") {
            self.advance();
            if self.peek() == Some(">") {
                self.advance();
                true
            } else {
                false
            }
        } else {
            self.pos = saved_pos;
            return Ok(false);
        };

        let _direction = if prefix_incoming && !suffix_arrow {
            0u8
        } else if !prefix_incoming && suffix_arrow {
            1u8
        } else {
            2u8
        };

        Ok(true)
    }

    fn validate_edge_inner(&mut self) -> Result<(), CypherError> {
        if let Some(token) = self.peek()
            && token != "]"
            && token != ":"
            && token != "*"
            && token != "{"
        {
            self.advance();
        }

        if self.peek() == Some(":") {
            self.advance();
            self.expect_identifier()?;
        }

        if self.peek() == Some("*") {
            self.advance();
            if let Some(token) = self.peek() {
                if token.parse::<u32>().is_ok() {
                    self.advance();
                    if self.peek() == Some(".") {
                        self.advance();
                        self.expect(".")?;
                        if let Some(max_token) = self.peek()
                            && max_token.parse::<u32>().is_ok()
                        {
                            self.advance();
                        }
                    }
                } else if token == "." {
                    self.advance();
                    self.expect(".")?;
                    if let Some(max_token) = self.peek()
                        && max_token.parse::<u32>().is_ok()
                    {
                        self.advance();
                    }
                }
            }
        }

        if self.peek() == Some("{") {
            self.advance();
            self.validate_properties_map()?;
        }

        Ok(())
    }

    fn validate_properties_map(&mut self) -> Result<(), CypherError> {
        while self.peek() != Some("}") && self.peek().is_some() {
            self.expect_identifier()?;
            self.expect(":")?;
            self.validate_simple_value_or_expression(&[",", "}"])?;
            if self.peek() == Some(",") {
                self.advance();
            }
        }
        self.expect("}")
    }

    fn validate_simple_value_or_expression(
        &mut self,
        terminators: &[&str],
    ) -> Result<(), CypherError> {
        let value_start = self.pos;
        self.validate_primary()?;
        if !matches!(self.peek(), Some(token) if terminators.contains(&token)) {
            self.pos = value_start;
            self.validate_expression()?;
        }
        Ok(())
    }

    fn validate_expression(&mut self) -> Result<(), CypherError> {
        self.validate_or()
    }

    fn validate_or(&mut self) -> Result<(), CypherError> {
        self.validate_and()?;
        while self.peek_is("OR") {
            self.advance();
            self.validate_and()?;
        }
        Ok(())
    }

    fn validate_and(&mut self) -> Result<(), CypherError> {
        self.validate_xor()?;
        while self.peek_is("AND") {
            self.advance();
            self.validate_xor()?;
        }
        Ok(())
    }

    fn validate_xor(&mut self) -> Result<(), CypherError> {
        self.validate_not()?;
        while self.peek_is("XOR") {
            self.advance();
            self.validate_not()?;
        }
        Ok(())
    }

    fn validate_not(&mut self) -> Result<(), CypherError> {
        if self.peek_is("NOT") {
            self.advance();
            return self.validate_not();
        }
        self.validate_comparison()
    }

    fn validate_comparison(&mut self) -> Result<(), CypherError> {
        self.validate_addition()?;

        if self.peek_is("IS") {
            self.advance();
            if self.peek_is("NOT") {
                self.advance();
            }
            self.expect("NULL")?;
            return Ok(());
        }

        if let Some(token) = self.peek() {
            if token.eq_ignore_ascii_case("IN") {
                self.advance();
                return self.validate_primary();
            }
            if token.eq_ignore_ascii_case("NOT") {
                self.advance();
                self.expect("IN")?;
                return self.validate_primary();
            }
            if token.eq_ignore_ascii_case("CONTAINS") {
                self.advance();
                return self.validate_primary();
            }
            if token.eq_ignore_ascii_case("STARTS") || token.eq_ignore_ascii_case("ENDS") {
                self.advance();
                self.expect("WITH")?;
                return self.validate_primary();
            }
            if token.eq_ignore_ascii_case("BETWEEN") {
                self.advance();
                self.validate_addition()?;
                self.expect("AND")?;
                return self.validate_addition();
            }
        }

        match self.peek() {
            Some("=") | Some("<") | Some(">") | Some("<=") | Some(">=") | Some("<>")
            | Some("=~") | Some("!=") => {
                self.advance();
                self.validate_primary()?;
            }
            _ => {}
        }

        Ok(())
    }

    fn validate_addition(&mut self) -> Result<(), CypherError> {
        self.validate_multiplication()?;
        while self.peek() == Some("+") || self.peek() == Some("-") {
            self.advance();
            self.validate_multiplication()?;
        }
        Ok(())
    }

    fn validate_multiplication(&mut self) -> Result<(), CypherError> {
        self.validate_primary()?;
        while self.peek() == Some("*") || self.peek() == Some("/") || self.peek() == Some("%") {
            self.advance();
            self.validate_primary()?;
        }
        Ok(())
    }

    fn validate_primary(&mut self) -> Result<(), CypherError> {
        match self.peek() {
            Some("-") => {
                self.advance();
                self.validate_primary()
            }
            Some(token) if token.starts_with('\'') || token.starts_with('"') => {
                self.advance();
                Ok(())
            }
            Some(token) if token.starts_with('$') => {
                self.advance();
                Ok(())
            }
            Some("(") => {
                self.advance();
                self.validate_expression()?;
                self.expect(")")
            }
            Some("[") => {
                self.advance();
                if self.peek() == Some("]") {
                    self.advance();
                    return Ok(());
                }
                let saved = self.pos;
                if self.advance().is_some() && self.peek_is("IN") {
                    self.pos = saved;
                    return self.validate_list_comprehension();
                }
                self.pos = saved;
                self.validate_list_literal()
            }
            Some("{") => self.validate_map_literal(),
            Some(token)
                if token.eq_ignore_ascii_case("true")
                    || token.eq_ignore_ascii_case("false")
                    || token.eq_ignore_ascii_case("null") =>
            {
                self.advance();
                Ok(())
            }
            Some(token)
                if token
                    .chars()
                    .next()
                    .map(|c| c.is_ascii_digit())
                    .unwrap_or(false) =>
            {
                let token = self.expect_identifier()?;
                if token.parse::<i64>().is_ok() {
                    if self.peek() == Some(".") {
                        self.advance();
                        let fraction = self.expect_identifier()?;
                        fraction.parse::<u64>().map_err(|_| {
                            CypherError::ParseError(format!(
                                "expected fractional digits after '.', got '{}'",
                                fraction
                            ))
                        })?;
                    }
                    Ok(())
                } else if token.parse::<f64>().is_ok() {
                    Ok(())
                } else {
                    Err(CypherError::ParseError(format!(
                        "invalid number '{}'",
                        token
                    )))
                }
            }
            Some(_) => {
                let ident = self.expect_identifier()?;
                // CASE expression
                if ident.eq_ignore_ascii_case("CASE") {
                    // Simple CASE: CASE expr WHEN ... END
                    if !self.peek_is("WHEN") {
                        self.validate_expression()?;
                    }
                    while self.peek_is("WHEN") {
                        self.advance();
                        self.validate_expression()?;
                        self.expect("THEN")?;
                        self.validate_expression()?;
                    }
                    if self.peek_is("ELSE") {
                        self.advance();
                        self.validate_expression()?;
                    }
                    self.expect("END")?;
                    return Ok(());
                }
                // REDUCE expression
                if ident.eq_ignore_ascii_case("REDUCE") && self.peek() == Some("(") {
                    self.advance();
                    self.expect_identifier()?; // accumulator
                    self.expect("=")?;
                    self.validate_expression()?;
                    self.expect(",")?;
                    self.expect_identifier()?; // variable
                    self.expect("IN")?;
                    self.validate_expression()?;
                    self.expect("|")?;
                    self.validate_expression()?;
                    self.expect(")")?;
                    return Ok(());
                }
                if self.peek() == Some("(") {
                    self.advance();
                    if self.peek_is("DISTINCT") {
                        self.advance();
                    }
                    if self.peek() != Some(")") {
                        self.validate_expression()?;
                        while self.peek() == Some(",") {
                            self.advance();
                            self.validate_expression()?;
                        }
                    }
                    self.expect(")")?;
                    Ok(())
                } else if self.peek() == Some(".") {
                    // Accumulate dotted identifiers (e.g., vector.similarity.cosine)
                    while self.peek() == Some(".") {
                        self.advance();
                        self.expect_identifier()?;
                    }
                    // If followed by (, it's a dotted function call
                    if self.peek() == Some("(") {
                        self.advance();
                        if self.peek_is("DISTINCT") {
                            self.advance();
                        }
                        if self.peek() != Some(")") {
                            self.validate_expression()?;
                            while self.peek() == Some(",") {
                                self.advance();
                                self.validate_expression()?;
                            }
                        }
                        self.expect(")")?;
                    }
                    Ok(())
                } else {
                    Ok(())
                }
            }
            None => Err(CypherError::ParseError(
                "unexpected end of expression".into(),
            )),
        }
    }

    fn validate_list_literal(&mut self) -> Result<(), CypherError> {
        self.expect("[")?;
        if self.peek() != Some("]") {
            loop {
                self.validate_simple_value_or_expression(&[",", "]"])?;
                if self.peek() == Some(",") {
                    self.advance();
                    continue;
                }
                break;
            }
        }
        self.expect("]")
    }

    fn validate_list_comprehension(&mut self) -> Result<(), CypherError> {
        self.expect("[")?;
        self.expect_identifier()?;
        self.expect("IN")?;
        if self.peek_is("WHERE") {
            self.advance();
            self.validate_expression()?;
        }
        self.expect("|")?;
        self.validate_expression()?;
        self.expect("]")
    }

    fn validate_map_literal(&mut self) -> Result<(), CypherError> {
        self.expect("{")?;
        if self.peek() != Some("}") {
            loop {
                self.expect_identifier()?;
                self.expect(":")?;
                self.validate_simple_value_or_expression(&[",", "}"])?;
                if self.peek() == Some(",") {
                    self.advance();
                    continue;
                }
                break;
            }
        }
        self.expect("}")
    }

    fn validate_i64(&mut self) -> Result<(), CypherError> {
        let token = self.expect_identifier()?;
        token
            .parse::<i64>()
            .map(|_| ())
            .map_err(|_| CypherError::ParseError(format!("expected integer, got '{}'", token)))
    }
}
