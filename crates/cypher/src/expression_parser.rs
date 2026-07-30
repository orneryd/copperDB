use crate::{
    parse_context::ParseContext, BinaryExpression, CypherError, EdgePattern, Expression,
    LiteralValue, NodePattern, Pattern, PatternComprehension, PropertyEntry,
};

impl<'a> ParseContext<'a> {
    fn binary_operands(left: Expression, right: Expression) -> Box<BinaryExpression> {
        Box::new(BinaryExpression { left, right })
    }

    /// Entry: parse `OR` level.
    pub(crate) fn parse_expression(&mut self) -> Result<Expression, CypherError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expression, CypherError> {
        let mut left = self.parse_and()?;
        while self.peek_is("OR") {
            self.advance();
            let right = self.parse_and()?;
            left = Expression::Or(Self::binary_operands(left, right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expression, CypherError> {
        let mut left = self.parse_xor()?;
        while self.peek_is("AND") {
            self.advance();
            let right = self.parse_xor()?;
            left = Expression::And(Self::binary_operands(left, right));
        }
        Ok(left)
    }

    fn parse_xor(&mut self) -> Result<Expression, CypherError> {
        let mut left = self.parse_not()?;
        while self.peek_is("XOR") {
            self.advance();
            let right = self.parse_not()?;
            left = Expression::Xor(Self::binary_operands(left, right));
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expression, CypherError> {
        if self.peek_is("NOT") {
            self.advance();
            let expr = self.parse_not()?;
            return Ok(Expression::Not(Box::new(expr)));
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Expression, CypherError> {
        let left = self.parse_addition()?;

        if self.peek_is("IS") {
            self.advance();
            if self.peek_is("NOT") {
                self.advance();
                self.expect("NULL")?;
                return Ok(Expression::IsNotNull(Box::new(left)));
            }
            self.expect("NULL")?;
            return Ok(Expression::IsNull(Box::new(left)));
        }

        if let Some(kw) = self.peek() {
            if kw.eq_ignore_ascii_case("IN") {
                self.advance();
                let list = self.parse_primary()?;
                return Ok(Expression::InList {
                    operands: Self::binary_operands(left, list),
                    negated: false,
                });
            }
            if kw.eq_ignore_ascii_case("NOT") {
                self.advance();
                self.expect("IN")?;
                let list = self.parse_primary()?;
                return Ok(Expression::InList {
                    operands: Self::binary_operands(left, list),
                    negated: true,
                });
            }
            if kw.eq_ignore_ascii_case("CONTAINS") {
                self.advance();
                let right = self.parse_primary()?;
                return Ok(Expression::Comparison {
                    operands: Self::binary_operands(left, right),
                    op: "CONTAINS".to_string(),
                });
            }
            if kw.eq_ignore_ascii_case("STARTS") {
                self.advance();
                self.expect("WITH")?;
                let right = self.parse_primary()?;
                return Ok(Expression::Comparison {
                    operands: Self::binary_operands(left, right),
                    op: "STARTS WITH".to_string(),
                });
            }
            if kw.eq_ignore_ascii_case("ENDS") {
                self.advance();
                self.expect("WITH")?;
                let right = self.parse_primary()?;
                return Ok(Expression::Comparison {
                    operands: Self::binary_operands(left, right),
                    op: "ENDS WITH".to_string(),
                });
            }
            if kw.eq_ignore_ascii_case("BETWEEN") {
                self.advance();
                let lower = self.parse_addition()?;
                self.expect("AND")?;
                let upper = self.parse_addition()?;
                return Ok(Expression::Between {
                    expression: Box::new(left),
                    lower: Box::new(lower),
                    upper: Box::new(upper),
                });
            }
        }

        let op = match self.peek() {
            Some("=") | Some("<") | Some(">") | Some("<=") | Some(">=") | Some("<>")
            | Some("=~") => self.advance().unwrap().to_string(),
            Some("!=") => {
                self.advance();
                "<>".to_string()
            }
            _ => return Ok(left),
        };

        let right = self.parse_primary()?;
        Ok(Expression::Comparison {
            operands: Self::binary_operands(left, right),
            op,
        })
    }

    fn parse_addition(&mut self) -> Result<Expression, CypherError> {
        let mut left = self.parse_multiplication()?;
        loop {
            if self.peek() == Some("+") {
                self.advance();
                let right = self.parse_multiplication()?;
                left = Expression::Add(Self::binary_operands(left, right));
            } else if self.peek() == Some("-") {
                self.advance();
                let right = self.parse_multiplication()?;
                left = Expression::Subtract(Self::binary_operands(left, right));
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_multiplication(&mut self) -> Result<Expression, CypherError> {
        let mut left = self.parse_primary()?;
        loop {
            // Bracket access: expr[key]
            if self.peek() == Some("[") {
                self.advance(); // consume [
                let key = self.parse_expression_item(&["]"])?;
                self.expect("]")?;
                left = Expression::BracketAccess {
                    expression: Box::new(left),
                    key: Box::new(key),
                };
                continue;
            }
            if self.peek() == Some("*") {
                self.advance();
                let right = self.parse_primary()?;
                left = Expression::Multiply(Self::binary_operands(left, right));
            } else if self.peek() == Some("/") {
                self.advance();
                let right = self.parse_primary()?;
                left = Expression::Divide(Self::binary_operands(left, right));
            } else if self.peek() == Some("%") {
                self.advance();
                let right = self.parse_primary()?;
                left = Expression::Modulo(Self::binary_operands(left, right));
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_primary(&mut self) -> Result<Expression, CypherError> {
        // Try pattern predicate: (n)-[:REL]->(m)
        if self.peek() == Some("(") {
            if let Some(pred) = self.try_parse_pattern_predicate() {
                return Ok(pred);
            }
        }

        // CASE expression
        if self.peek_is("CASE") {
            return self.parse_case_expression();
        }

        match self.peek() {
            Some("-") => {
                self.advance();
                let inner = self.parse_primary()?;
                // Negate via 0 - inner
                Ok(Expression::Subtract(Self::binary_operands(
                    Expression::Literal(LiteralValue::Integer(0)),
                    inner,
                )))
            }
            Some(t) if t.starts_with('\'') || t.starts_with('"') => {
                let raw = self.advance().unwrap();
                if raw.len() < 2 {
                    return Err(CypherError::UnterminatedString);
                }
                let s = raw[1..raw.len() - 1].to_string();
                Ok(Expression::Literal(LiteralValue::String(s)))
            }
            Some(t) if t.starts_with('$') => {
                let raw = self.advance().unwrap();
                let param_name = raw[1..].to_string();
                // Support $param.property for parameter map property access
                if self.peek() == Some(".") {
                    self.advance();
                    let property = self.advance_identifier()?;
                    Ok(Expression::ParameterPropertyAccess {
                        parameter: param_name,
                        property,
                    })
                } else {
                    Ok(Expression::Parameter(param_name))
                }
            }
            Some("(") => {
                self.advance();
                let expr = self.parse_expression()?;
                self.expect(")")?;
                Ok(expr)
            }
            Some("[") => {
                // Try list comprehension: [var IN list [WHERE pred] | expr]
                // Try pattern comprehension: [(pattern) [WHERE pred] | expr]
                let saved = self.pos;
                self.advance(); // consume [
                if self.peek() == Some("]") {
                    self.advance();
                    return Ok(Expression::ListLiteral(Vec::new()));
                }
                // Check for pattern comprehension: starts with (
                if self.peek() == Some("(") {
                    self.pos = saved;
                    return self.parse_pattern_comprehension();
                }
                if self.advance().is_some() && self.peek_is("IN") {
                    self.pos = saved;
                    return self.parse_list_comprehension();
                }
                self.pos = saved;
                self.parse_list_literal()
            }
            Some("{") => self.parse_map_literal(),
            Some(t) if t.eq_ignore_ascii_case("true") => {
                self.advance();
                Ok(Expression::Literal(LiteralValue::Bool(true)))
            }
            Some(t) if t.eq_ignore_ascii_case("false") => {
                self.advance();
                Ok(Expression::Literal(LiteralValue::Bool(false)))
            }
            Some(t) if t.eq_ignore_ascii_case("null") => {
                self.advance();
                Ok(Expression::Literal(LiteralValue::Null))
            }
            Some(t)
                if t.chars()
                    .next()
                    .map(|c| c.is_ascii_digit())
                    .unwrap_or(false) =>
            {
                let t = self.advance().unwrap().to_string();
                if let Ok(i) = t.parse::<i64>() {
                    if self.peek() == Some(".") {
                        self.advance();
                        let frac = self.advance().ok_or_else(|| {
                            CypherError::ParseError("expected fractional digits after '.'".into())
                        })?;
                        let composed = format!("{}.{}", i, frac);
                        let f: f64 = composed.parse().map_err(|_| {
                            CypherError::ParseError(format!("invalid float '{}'", composed))
                        })?;
                        Ok(Expression::Literal(LiteralValue::Float(f)))
                    } else {
                        Ok(Expression::Literal(LiteralValue::Integer(i)))
                    }
                } else {
                    let f: f64 = t
                        .parse()
                        .map_err(|_| CypherError::ParseError(format!("invalid number '{}'", t)))?;
                    Ok(Expression::Literal(LiteralValue::Float(f)))
                }
            }
            Some(_) => {
                let name = self.advance().unwrap().to_string();

                // Special form: reduce(acc = init, var IN list | expr)
                if name.eq_ignore_ascii_case("reduce") && self.peek() == Some("(") {
                    return self.parse_reduce_expression();
                }

                // Dotted function call: vector.similarity.cosine(args)
                if self.peek() == Some(".") {
                    let mut full_name = name;
                    // Accumulate dotted name components
                    while self.peek() == Some(".") {
                        self.advance();
                        full_name.push('.');
                        full_name.push_str(self.advance_identifier()?.as_str());
                    }
                    // If followed by (, it's a function call
                    if self.peek() == Some("(") {
                        self.advance();
                        let distinct = if self.peek_is("DISTINCT") {
                            self.advance();
                            true
                        } else {
                            false
                        };
                        let mut args: Vec<Expression> = Vec::new();
                        if self.peek() != Some(")") {
                            args.push(self.parse_expression_item(&[",", ")"])?);
                            while self.peek() == Some(",") {
                                self.advance();
                                args.push(self.parse_expression_item(&[",", ")"])?);
                            }
                        }
                        self.expect(")")?;
                        return Ok(Expression::FunctionCall {
                            name: full_name,
                            args,
                            distinct,
                        });
                    }
                    // Otherwise, it's a nested property access
                    // Parse back: split into variable + property chain
                    // For now, return the last dotted pair as PropertyAccess
                    if let Some(last_dot) = full_name.rfind('.') {
                        let variable = full_name[..last_dot].to_string();
                        let property = full_name[last_dot + 1..].to_string();
                        return Ok(Expression::PropertyAccess { variable, property });
                    }
                    return Ok(Expression::Variable(full_name));
                }

                if self.peek() == Some("(") {
                    self.advance();
                    let distinct = if self.peek_is("DISTINCT") {
                        self.advance();
                        true
                    } else {
                        false
                    };
                    let mut args: Vec<Expression> = Vec::new();
                    if self.peek() != Some(")") {
                        args.push(self.parse_expression_item(&[",", ")"])?);
                        while self.peek() == Some(",") {
                            self.advance();
                            args.push(self.parse_expression_item(&[",", ")"])?);
                        }
                    }
                    self.expect(")")?;
                    return Ok(Expression::FunctionCall {
                        name,
                        args,
                        distinct,
                    });
                }

                if self.peek() == Some(".") {
                    self.advance();
                    let property = self.advance_identifier()?;
                    return Ok(Expression::PropertyAccess {
                        variable: name,
                        property,
                    });
                }

                Ok(Expression::Variable(name))
            }
            None => Err(CypherError::ParseError(
                "unexpected end of expression".into(),
            )),
        }
    }

    fn try_parse_pattern_predicate(&mut self) -> Option<Expression> {
        let start = self.pos;
        // (variable)
        if self.peek() != Some("(") {
            self.pos = start;
            return None;
        }
        self.advance();
        let variable = self.advance_identifier().ok()?;
        if self.peek() == Some(":") {
            // Skip optional label
            self.advance();
            self.advance_identifier().ok()?;
        }
        if self.peek() != Some(")") {
            self.pos = start;
            return None;
        }
        self.advance();
        // -[:REL_TYPE]->
        if self.peek() != Some("-") {
            self.pos = start;
            return None;
        }
        self.advance();
        if self.peek() != Some("[") {
            self.pos = start;
            return None;
        }
        self.advance();
        if self.peek() == Some(":") {
            self.advance();
        }
        let rel_type = self.advance_identifier().ok()?;
        if self.peek() != Some("]") {
            self.pos = start;
            return None;
        }
        self.advance();
        // -> or -
        if self.peek() != Some("-") {
            self.pos = start;
            return None;
        }
        self.advance();
        if self.peek() == Some(">") {
            self.advance();
        }
        // (target_variable)
        if self.peek() != Some("(") {
            self.pos = start;
            return None;
        }
        self.advance();
        let target_variable = if self.peek() == Some(")") {
            String::new()
        } else {
            self.advance_identifier().ok()?
        };
        if self.peek() == Some(":") {
            self.advance();
            self.advance_identifier().ok()?;
        }
        if self.peek() != Some(")") {
            self.pos = start;
            return None;
        }
        self.advance();

        Some(Expression::PatternExists {
            variable,
            rel_type,
            target_variable,
        })
    }

    fn parse_case_expression(&mut self) -> Result<Expression, CypherError> {
        use crate::CaseAlternative;
        use crate::CaseExpression;

        self.expect("CASE")?;

        // Simple CASE: CASE expr WHEN val THEN result ... END
        // Searched CASE: CASE WHEN cond THEN result ... END
        let expression = if self.peek_is("WHEN") {
            None
        } else {
            let expr = self.parse_expression_item(&["WHEN"])?;
            Some(Box::new(expr))
        };

        let mut alternatives: Vec<CaseAlternative> = Vec::new();
        while self.peek_is("WHEN") {
            self.advance(); // WHEN
            let condition = self.parse_expression_item(&["THEN"])?;
            self.expect("THEN")?;
            let result = self.parse_expression_item(&["WHEN", "ELSE", "END"])?;
            alternatives.push(CaseAlternative { condition, result });
        }

        let default = if self.peek_is("ELSE") {
            self.advance();
            let expr = self.parse_expression_item(&["END"])?;
            Some(Box::new(expr))
        } else {
            None
        };

        self.expect("END")?;

        Ok(Expression::Case(CaseExpression {
            expression,
            alternatives,
            default,
        }))
    }

    fn parse_list_literal(&mut self) -> Result<Expression, CypherError> {
        self.expect("[")?;
        let mut items = Vec::new();
        if self.peek() != Some("]") {
            loop {
                items.push(self.parse_expression_item(&[",", "]"])?);
                if self.peek() == Some(",") {
                    self.advance();
                    continue;
                }
                break;
            }
        }
        self.expect("]")?;
        Ok(Expression::ListLiteral(items))
    }

    fn parse_list_comprehension(&mut self) -> Result<Expression, CypherError> {
        use crate::ListComprehension;

        self.expect("[")?;
        let variable = self.advance_identifier()?;
        self.expect("IN")?;
        let list = self.parse_expression_item(&["WHERE", "|"])?;
        let predicate = if self.peek_is("WHERE") {
            self.advance();
            Some(Box::new(self.parse_expression_item(&["|"])?))
        } else {
            None
        };
        self.expect("|")?;
        let expression = self.parse_expression_item(&["]"])?;
        self.expect("]")?;
        Ok(Expression::ListComprehension(ListComprehension {
            variable,
            list: Box::new(list),
            predicate,
            expression: Box::new(expression),
        }))
    }

    fn parse_pattern_comprehension(&mut self) -> Result<Expression, CypherError> {
        self.expect("[")?;

        // Parse start node: (var:Label) or ()
        let start_node = self.parse_pc_node()?;

        // Parse relationship: -[:TYPE*..]-> or -->
        let edge = self.parse_pc_edge()?;

        // Parse end node: (var:Label) or ()
        let end_node = self.parse_pc_node()?;

        let pattern = Pattern {
            path_variable: None,
            shortest_path: false,
            all_shortest_paths: false,
            nodes: vec![start_node, end_node],
            edges: vec![edge],
            segment_edge_counts: vec![1],
        };

        // Optional WHERE clause
        let predicate = if self.peek_is("WHERE") {
            self.advance();
            Some(Box::new(self.parse_expression_item(&["|", "]"])?))
        } else {
            None
        };

        self.expect("|")?;
        let expression = self.parse_expression_item(&["]"])?;
        self.expect("]")?;

        Ok(Expression::PatternComprehension(PatternComprehension {
            pattern,
            predicate,
            expression: Box::new(expression),
        }))
    }

    fn parse_pc_node(&mut self) -> Result<NodePattern, CypherError> {
        self.expect("(")?;
        if self.peek() == Some(")") {
            self.advance();
            return Ok(NodePattern {
                variable: None,
                labels: Vec::new(),
                properties: Vec::new(),
            });
        }
        let variable = if self.peek().is_some_and(|t| !t.starts_with(':') && t != "{") {
            Some(self.advance_identifier()?)
        } else {
            None
        };
        let mut labels = Vec::new();
        while self.peek() == Some(":") {
            self.advance();
            labels.push(self.advance_identifier()?);
        }
        self.expect(")")?;
        Ok(NodePattern {
            variable,
            labels,
            properties: Vec::new(),
        })
    }

    fn parse_pc_edge(&mut self) -> Result<EdgePattern, CypherError> {
        use crate::{EdgeDirection, EdgePattern};
        // Determine direction: <--, -->, or --
        let direction = if self.peek() == Some("<") {
            self.advance();
            self.expect("-")?;
            EdgeDirection::Incoming
        } else {
            self.expect("-")?;
            EdgeDirection::Outgoing
        };
        let (rel_type, min_hops, max_hops) = if self.peek() == Some("[") {
            self.advance();
            // Skip optional variable
            if self
                .peek()
                .is_some_and(|t| t != ":" && t != "*" && t != "]")
            {
                let id = self.advance_identifier()?;
                if self.peek() == Some(":") {
                    self.advance();
                    let rt = self.advance_identifier()?;
                    self.expect("]")?;
                    (Some(rt), None, None)
                } else {
                    // Just a variable, no type
                    self.expect("]")?;
                    (Some(id), None, None)
                }
            } else if self.peek() == Some(":") {
                self.advance();
                let rt = self.advance_identifier()?;
                self.expect("]")?;
                (Some(rt), None, None)
            } else {
                self.expect("]")?;
                (None, None, None)
            }
        } else if self.peek() == Some(">") {
            // Simple --> (no brackets)
            self.advance();
            return Ok(EdgePattern {
                variable: None,
                rel_type: None,
                direction: EdgeDirection::Outgoing,
                min_hops: None,
                max_hops: None,
                properties: Vec::new(),
            });
        } else {
            (None, None, None)
        };

        // After bracket, consume optional - and > for directed edges
        if self.peek() == Some("-") {
            self.advance();
            if self.peek() == Some(">") {
                self.advance();
            }
        } else if self.peek() == Some(">") {
            self.advance();
        }

        Ok(EdgePattern {
            variable: None,
            rel_type,
            direction,
            min_hops,
            max_hops,
            properties: Vec::new(),
        })
    }

    fn parse_reduce_expression(&mut self) -> Result<Expression, CypherError> {
        use crate::ReduceExpression;

        self.expect("(")?;
        let accumulator = self.advance_identifier()?;
        self.expect("=")?;
        let initial = self.parse_expression_item(&[","])?;
        self.expect(",")?;
        let variable = self.advance_identifier()?;
        self.expect("IN")?;
        let list = self.parse_expression_item(&["|"])?;
        self.expect("|")?;
        let expression = self.parse_expression_item(&[")"])?;
        self.expect(")")?;
        Ok(Expression::Reduce(ReduceExpression {
            accumulator,
            initial: Box::new(initial),
            variable,
            list: Box::new(list),
            expression: Box::new(expression),
        }))
    }

    fn parse_map_literal(&mut self) -> Result<Expression, CypherError> {
        self.expect("{")?;
        let mut entries = Vec::new();
        if self.peek() != Some("}") {
            loop {
                let key = self.advance_map_key()?;
                self.expect(":")?;
                entries.push(PropertyEntry {
                    key,
                    value: self.parse_expression_item(&[",", "}"])?,
                });
                if self.peek() == Some(",") {
                    self.advance();
                    continue;
                }
                break;
            }
        }
        self.expect("}")?;
        Ok(Expression::MapLiteral(entries))
    }
}
