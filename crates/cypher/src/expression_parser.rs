use serde_json::Value;

use crate::{parse_context::ParseContext, CypherError, Expression};

impl ParseContext {
    /// Entry: parse `OR` level.
    pub(crate) fn parse_expression(&mut self) -> Result<Expression, CypherError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expression, CypherError> {
        let mut left = self.parse_and()?;
        while self.peek_upper().as_deref() == Some("OR") {
            self.advance();
            let right = self.parse_and()?;
            left = Expression::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expression, CypherError> {
        let mut left = self.parse_not()?;
        while self.peek_upper().as_deref() == Some("AND") {
            self.advance();
            let right = self.parse_not()?;
            left = Expression::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expression, CypherError> {
        if self.peek_upper().as_deref() == Some("NOT") {
            self.advance();
            let expr = self.parse_not()?;
            return Ok(Expression::Not(Box::new(expr)));
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Expression, CypherError> {
        let left = self.parse_primary()?;

        if self.peek_upper().as_deref() == Some("IS") {
            self.advance();
            if self.peek_upper().as_deref() == Some("NOT") {
                self.advance();
                self.expect("NULL")?;
                return Ok(Expression::IsNotNull(Box::new(left)));
            }
            self.expect("NULL")?;
            return Ok(Expression::IsNull(Box::new(left)));
        }

        if let Some(kw) = self.peek_upper() {
            match kw.as_str() {
                "IN" => {
                    self.advance();
                    let list = self.parse_primary()?;
                    return Ok(Expression::InList {
                        value: Box::new(left),
                        list: Box::new(list),
                        negated: false,
                    });
                }
                "NOT" => {
                    self.advance();
                    self.expect("IN")?;
                    let list = self.parse_primary()?;
                    return Ok(Expression::InList {
                        value: Box::new(left),
                        list: Box::new(list),
                        negated: true,
                    });
                }
                "CONTAINS" => {
                    self.advance();
                    let right = self.parse_primary()?;
                    return Ok(Expression::Comparison {
                        left: Box::new(left),
                        op: "CONTAINS".to_string(),
                        right: Box::new(right),
                    });
                }
                "STARTS" => {
                    self.advance();
                    self.expect("WITH")?;
                    let right = self.parse_primary()?;
                    return Ok(Expression::Comparison {
                        left: Box::new(left),
                        op: "STARTS WITH".to_string(),
                        right: Box::new(right),
                    });
                }
                "ENDS" => {
                    self.advance();
                    self.expect("WITH")?;
                    let right = self.parse_primary()?;
                    return Ok(Expression::Comparison {
                        left: Box::new(left),
                        op: "ENDS WITH".to_string(),
                        right: Box::new(right),
                    });
                }
                _ => {}
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
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }

    fn parse_primary(&mut self) -> Result<Expression, CypherError> {
        match self.peek() {
            Some(t) if t.starts_with('\'') || t.starts_with('"') => {
                let raw = self.advance().unwrap().to_string();
                if raw.len() < 2 {
                    return Err(CypherError::UnterminatedString);
                }
                let s = raw[1..raw.len() - 1].to_string();
                Ok(Expression::Literal(Value::String(s)))
            }
            Some(t) if t.starts_with('$') => {
                let raw = self.advance().unwrap().to_string();
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
                Ok(Expression::Literal(Value::Bool(true)))
            }
            Some(t) if t.eq_ignore_ascii_case("false") => {
                self.advance();
                Ok(Expression::Literal(Value::Bool(false)))
            }
            Some(t) if t.eq_ignore_ascii_case("null") => {
                self.advance();
                Ok(Expression::Literal(Value::Null))
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
                        let n = serde_json::Number::from_f64(f).ok_or_else(|| {
                            CypherError::ParseError(format!("invalid float value '{}'", composed))
                        })?;
                        Ok(Expression::Literal(Value::Number(n)))
                    } else {
                        Ok(Expression::Literal(Value::Number(i.into())))
                    }
                } else {
                    let f: f64 = t
                        .parse()
                        .map_err(|_| CypherError::ParseError(format!("invalid number '{}'", t)))?;
                    let n = serde_json::Number::from_f64(f).ok_or_else(|| {
                        CypherError::ParseError(format!("invalid float value '{}'", t))
                    })?;
                    Ok(Expression::Literal(Value::Number(n)))
                }
            }
            Some(_) => {
                let name = self.advance().unwrap().to_string();

                if self.peek() == Some("(") {
                    self.advance();
                    let distinct = if self.peek_upper().as_deref() == Some("DISTINCT") {
                        self.advance();
                        true
                    } else {
                        false
                    };
                    let mut args: Vec<Expression> = Vec::new();
                    if self.peek() != Some(")") {
                        args.push(self.parse_expression()?);
                        while self.peek() == Some(",") {
                            self.advance();
                            args.push(self.parse_expression()?);
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
                items.push(self.parse_expression()?);
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
                entries.push((key, self.parse_expression()?));
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
