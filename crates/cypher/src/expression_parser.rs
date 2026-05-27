use crate::{
    parse_context::ParseContext, BinaryExpression, CypherError, Expression, LiteralValue,
    PropertyEntry,
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
        let mut left = self.parse_not()?;
        while self.peek_is("AND") {
            self.advance();
            let right = self.parse_not()?;
            left = Expression::And(Self::binary_operands(left, right));
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
        let left = self.parse_primary()?;

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

    fn parse_primary(&mut self) -> Result<Expression, CypherError> {
        match self.peek() {
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
                Ok(Expression::Parameter(raw[1..].to_string()))
            }
            Some("(") => {
                self.advance();
                let expr = self.parse_expression()?;
                self.expect(")")?;
                Ok(expr)
            }
            Some("[") => self.parse_list_literal(),
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

    fn parse_map_literal(&mut self) -> Result<Expression, CypherError> {
        self.expect("{")?;
        let mut entries = Vec::new();
        if self.peek() != Some("}") {
            loop {
                let key = self.advance_identifier()?;
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
