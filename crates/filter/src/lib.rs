//! Result set filtering and predicate evaluation.
//!
//! Equivalent to Go's `pkg/filter` in NornicDB.
//! Applies WHERE clause predicates and result projections to query output rows.

use copperdb_cypher::Expression;
use serde_json::Value;
use std::collections::HashMap;
use thiserror::Error;

/// A result row: variable → value bindings.
pub type Row = HashMap<String, Value>;

#[derive(Debug, Error)]
pub enum FilterError {
    #[error("predicate evaluation error: {0}")]
    PredicateError(String),
    #[error("type error: {0}")]
    TypeError(String),
    #[error("unknown variable: {0}")]
    UnknownVariable(String),
    #[error("unknown function: {0}")]
    UnknownFunction(String),
}

// ─── Expression evaluator ────────────────────────────────────────────────────

/// Evaluate an Expression against a row of bindings and a parameter map,
/// returning a `serde_json::Value`.
pub fn eval_expression(
    expr: &Expression,
    row: &Row,
    params: &HashMap<String, Value>,
) -> Result<Value, FilterError> {
    match expr {
        Expression::Literal(v) => Ok(v.clone()),

        Expression::Parameter(name) => params
            .get(name)
            .cloned()
            .ok_or_else(|| FilterError::UnknownVariable(format!("parameter ${name}"))),

        Expression::Variable(name) => row
            .get(name)
            .cloned()
            .ok_or_else(|| FilterError::UnknownVariable(name.clone())),

        Expression::PropertyAccess { variable, property } => {
            // Try row["variable.property"] first, then row["variable"]["property"]
            let dot_key = format!("{variable}.{property}");
            if let Some(v) = row.get(&dot_key) {
                return Ok(v.clone());
            }
            if let Some(obj) = row.get(variable.as_str()) {
                if let Value::Object(map) = obj {
                    return Ok(map.get(property.as_str()).cloned().unwrap_or(Value::Null));
                }
            }
            Ok(Value::Null)
        }

        Expression::Comparison { left, op, right } => {
            let lv = eval_expression(left, row, params)?;
            let rv = eval_expression(right, row, params)?;
            Ok(Value::Bool(compare_values(&lv, op, &rv)?))
        }

        Expression::And(a, b) => {
            // Short-circuit
            if !eval_predicate(a, row, params)? {
                return Ok(Value::Bool(false));
            }
            Ok(Value::Bool(eval_predicate(b, row, params)?))
        }

        Expression::Or(a, b) => {
            if eval_predicate(a, row, params)? {
                return Ok(Value::Bool(true));
            }
            Ok(Value::Bool(eval_predicate(b, row, params)?))
        }

        Expression::Not(inner) => {
            Ok(Value::Bool(!eval_predicate(inner, row, params)?))
        }

        Expression::IsNull(inner) => {
            let v = eval_expression(inner, row, params)?;
            Ok(Value::Bool(v == Value::Null))
        }

        Expression::IsNotNull(inner) => {
            let v = eval_expression(inner, row, params)?;
            Ok(Value::Bool(v != Value::Null))
        }

        Expression::FunctionCall { name, args, distinct: _ } => {
            eval_function(name, args, row, params)
        }
    }
}

/// Evaluate an Expression as a boolean predicate.
pub fn eval_predicate(
    expr: &Expression,
    row: &Row,
    params: &HashMap<String, Value>,
) -> Result<bool, FilterError> {
    let v = eval_expression(expr, row, params)?;
    Ok(value_is_truthy(&v))
}

// ─── Value comparison ────────────────────────────────────────────────────────

fn compare_values(left: &Value, op: &str, right: &Value) -> Result<bool, FilterError> {
    let op_upper = op.to_uppercase();
    match op_upper.as_str() {
        "=" => Ok(values_equal(left, right)),
        "<>" | "!=" => Ok(!values_equal(left, right)),
        "<" => numeric_cmp(left, right).map(|o| o < 0),
        "<=" => numeric_cmp(left, right).map(|o| o <= 0),
        ">" => numeric_cmp(left, right).map(|o| o > 0),
        ">=" => numeric_cmp(left, right).map(|o| o >= 0),
        "CONTAINS" => {
            let s = coerce_string(left)?;
            let pat = coerce_string(right)?;
            Ok(s.contains(pat.as_str()))
        }
        "STARTS WITH" => {
            let s = coerce_string(left)?;
            let pat = coerce_string(right)?;
            Ok(s.starts_with(pat.as_str()))
        }
        "ENDS WITH" => {
            let s = coerce_string(left)?;
            let pat = coerce_string(right)?;
            Ok(s.ends_with(pat.as_str()))
        }
        _ => Err(FilterError::TypeError(format!("unknown operator: {op}"))),
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    // Numeric equality across int/float
    match (a, b) {
        (Value::Number(n1), Value::Number(n2)) => {
            // compare as f64
            let f1 = n1.as_f64().unwrap_or(f64::NAN);
            let f2 = n2.as_f64().unwrap_or(f64::NAN);
            (f1 - f2).abs() < f64::EPSILON
        }
        _ => a == b,
    }
}

/// Returns -1, 0, or 1 for ordering.
fn numeric_cmp(a: &Value, b: &Value) -> Result<i32, FilterError> {
    match (a, b) {
        (Value::Number(n1), Value::Number(n2)) => {
            let f1 = n1.as_f64().unwrap_or(f64::NAN);
            let f2 = n2.as_f64().unwrap_or(f64::NAN);
            if f1 < f2 { Ok(-1) } else if f1 > f2 { Ok(1) } else { Ok(0) }
        }
        (Value::String(s1), Value::String(s2)) => match s1.cmp(s2) {
            std::cmp::Ordering::Less => Ok(-1),
            std::cmp::Ordering::Equal => Ok(0),
            std::cmp::Ordering::Greater => Ok(1),
        },
        _ => Err(FilterError::TypeError(format!(
            "cannot compare {:?} with {:?}",
            a, b
        ))),
    }
}

fn coerce_string(v: &Value) -> Result<String, FilterError> {
    match v {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Null => Ok("null".to_string()),
        _ => Err(FilterError::TypeError(format!("cannot coerce {v:?} to string"))),
    }
}

fn value_is_truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(_) => true,
    }
}

// ─── Built-in functions ──────────────────────────────────────────────────────

fn eval_function(
    name: &str,
    args: &[Expression],
    row: &Row,
    params: &HashMap<String, Value>,
) -> Result<Value, FilterError> {
    let name_lower = name.to_lowercase();
    let eval_arg = |i: usize| -> Result<Value, FilterError> {
        args.get(i)
            .ok_or_else(|| FilterError::PredicateError(format!("missing arg {i} for {name}")))
            .and_then(|e| eval_expression(e, row, params))
    };

    match name_lower.as_str() {
        "count" => {
            if args.is_empty() {
                return Ok(Value::Number(0.into()));
            }
            // count(*) or count(expr) — for per-row evaluation just return 1
            Ok(Value::Number(1.into()))
        }
        "size" => {
            let v = eval_arg(0)?;
            match &v {
                Value::Array(a) => Ok(Value::Number(a.len().into())),
                Value::String(s) => Ok(Value::Number(s.len().into())),
                Value::Null => Ok(Value::Null),
                _ => Err(FilterError::TypeError(format!("size() not applicable to {v:?}"))),
            }
        }
        "type" => {
            // type(rel) — return the relationship type string stored as "_type"
            let v = eval_arg(0)?;
            if let Value::Object(map) = &v {
                Ok(map.get("_type").cloned().unwrap_or(Value::Null))
            } else {
                Ok(Value::Null)
            }
        }
        "labels" => {
            let v = eval_arg(0)?;
            if let Value::Object(map) = &v {
                Ok(map.get("_labels").cloned().unwrap_or(Value::Array(vec![])))
            } else {
                Ok(Value::Array(vec![]))
            }
        }
        "id" => {
            let v = eval_arg(0)?;
            if let Value::Object(map) = &v {
                Ok(map.get("_id").cloned().unwrap_or(Value::Null))
            } else {
                Ok(Value::Null)
            }
        }
        "tostring" | "str" => {
            let v = eval_arg(0)?;
            Ok(Value::String(coerce_string(&v)?))
        }
        "tointeger" | "int" | "integer" => {
            let v = eval_arg(0)?;
            match &v {
                Value::Number(n) => Ok(Value::Number(
                    serde_json::Number::from(n.as_i64().unwrap_or(0)),
                )),
                Value::String(s) => {
                    let i: i64 = s.parse().unwrap_or(0);
                    Ok(Value::Number(i.into()))
                }
                _ => Ok(Value::Null),
            }
        }
        "tofloat" | "float" => {
            let v = eval_arg(0)?;
            match &v {
                Value::Number(n) => Ok(Value::Number(
                    serde_json::Number::from_f64(n.as_f64().unwrap_or(0.0))
                        .unwrap_or(serde_json::Number::from(0)),
                )),
                Value::String(s) => {
                    let f: f64 = s.parse().unwrap_or(0.0);
                    Ok(Value::Number(
                        serde_json::Number::from_f64(f)
                            .unwrap_or(serde_json::Number::from(0)),
                    ))
                }
                _ => Ok(Value::Null),
            }
        }
        "toupper" | "upper" => {
            let v = eval_arg(0)?;
            Ok(Value::String(coerce_string(&v)?.to_uppercase()))
        }
        "tolower" | "lower" => {
            let v = eval_arg(0)?;
            Ok(Value::String(coerce_string(&v)?.to_lowercase()))
        }
        "trim" => {
            let v = eval_arg(0)?;
            Ok(Value::String(coerce_string(&v)?.trim().to_string()))
        }
        "ltrim" => {
            let v = eval_arg(0)?;
            Ok(Value::String(coerce_string(&v)?.trim_start().to_string()))
        }
        "rtrim" => {
            let v = eval_arg(0)?;
            Ok(Value::String(coerce_string(&v)?.trim_end().to_string()))
        }
        "split" => {
            let s = coerce_string(&eval_arg(0)?)?;
            let delim = coerce_string(&eval_arg(1)?)?;
            let parts: Vec<Value> = s.split(delim.as_str())
                .map(|p| Value::String(p.to_string()))
                .collect();
            Ok(Value::Array(parts))
        }
        "replace" => {
            let s = coerce_string(&eval_arg(0)?)?;
            let from = coerce_string(&eval_arg(1)?)?;
            let to = coerce_string(&eval_arg(2)?)?;
            Ok(Value::String(s.replace(from.as_str(), to.as_str())))
        }
        "substring" => {
            let s = coerce_string(&eval_arg(0)?)?;
            let start = eval_arg(1)?
                .as_i64()
                .unwrap_or(0)
                .max(0) as usize;
            if args.len() > 2 {
                let len = eval_arg(2)?.as_i64().unwrap_or(0).max(0) as usize;
                let end = (start + len).min(s.len());
                Ok(Value::String(s.get(start..end).unwrap_or("").to_string()))
            } else {
                Ok(Value::String(s.get(start..).unwrap_or("").to_string()))
            }
        }
        "left" => {
            let s = coerce_string(&eval_arg(0)?)?;
            let n = eval_arg(1)?.as_i64().unwrap_or(0).max(0) as usize;
            Ok(Value::String(s.chars().take(n).collect()))
        }
        "right" => {
            let s = coerce_string(&eval_arg(0)?)?;
            let n = eval_arg(1)?.as_i64().unwrap_or(0).max(0) as usize;
            let chars: Vec<char> = s.chars().collect();
            let start = chars.len().saturating_sub(n);
            Ok(Value::String(chars[start..].iter().collect()))
        }
        "startswith" | "starts_with" => {
            let s = coerce_string(&eval_arg(0)?)?;
            let pat = coerce_string(&eval_arg(1)?)?;
            Ok(Value::Bool(s.starts_with(pat.as_str())))
        }
        "endswith" | "ends_with" => {
            let s = coerce_string(&eval_arg(0)?)?;
            let pat = coerce_string(&eval_arg(1)?)?;
            Ok(Value::Bool(s.ends_with(pat.as_str())))
        }
        "contains" => {
            let s = coerce_string(&eval_arg(0)?)?;
            let pat = coerce_string(&eval_arg(1)?)?;
            Ok(Value::Bool(s.contains(pat.as_str())))
        }
        "now" | "timestamp" => {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            Ok(Value::Number(ts.into()))
        }
        "date" | "datetime" | "duration" => {
            // Return current time as ISO string (simplified)
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            Ok(Value::String(ts.to_string()))
        }
        "exists" => {
            let v = eval_arg(0)?;
            Ok(Value::Bool(v != Value::Null))
        }
        "keys" => {
            let v = eval_arg(0)?;
            if let Value::Object(map) = &v {
                let keys: Vec<Value> = map.keys()
                    .filter(|k| !k.starts_with('_'))
                    .map(|k| Value::String(k.clone()))
                    .collect();
                Ok(Value::Array(keys))
            } else {
                Ok(Value::Array(vec![]))
            }
        }
        "properties" => {
            let v = eval_arg(0)?;
            if let Value::Object(map) = &v {
                let props: serde_json::Map<String, Value> = map
                    .iter()
                    .filter(|(k, _)| !k.starts_with('_'))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                Ok(Value::Object(props))
            } else {
                Ok(Value::Null)
            }
        }
        "coalesce" => {
            for arg in args {
                let v = eval_expression(arg, row, params)?;
                if v != Value::Null {
                    return Ok(v);
                }
            }
            Ok(Value::Null)
        }
        "abs" => {
            let v = eval_arg(0)?;
            if let Value::Number(n) = &v {
                if let Some(f) = n.as_f64() {
                    return Ok(Value::Number(
                        serde_json::Number::from_f64(f.abs())
                            .unwrap_or(serde_json::Number::from(0)),
                    ));
                }
            }
            Err(FilterError::TypeError("abs() requires a number".into()))
        }
        "ceil" | "floor" | "round" => {
            let v = eval_arg(0)?;
            if let Value::Number(n) = &v {
                if let Some(f) = n.as_f64() {
                    let result = match name_lower.as_str() {
                        "ceil" => f.ceil(),
                        "floor" => f.floor(),
                        _ => f.round(),
                    };
                    return Ok(Value::Number(
                        serde_json::Number::from_f64(result)
                            .unwrap_or(serde_json::Number::from(0)),
                    ));
                }
            }
            Err(FilterError::TypeError(format!("{name}() requires a number")))
        }
        _ => Err(FilterError::UnknownFunction(name.to_string())),
    }
}

// ─── Legacy predicate trait ──────────────────────────────────────────────────

/// Represents a predicate that can be applied to a result row.
pub trait Predicate: Send + Sync {
    fn evaluate(&self, row: &Row) -> Result<bool, FilterError>;
}

/// Filter a list of rows using a predicate.
pub fn filter_rows<P: Predicate>(
    rows: Vec<Row>,
    predicate: &P,
) -> Result<Vec<Row>, FilterError> {
    rows.into_iter()
        .filter_map(|row| match predicate.evaluate(&row) {
            Ok(true) => Some(Ok(row)),
            Ok(false) => None,
            Err(e) => Some(Err(e)),
        })
        .collect()
}

/// A predicate that checks for key equality.
pub struct EqPredicate {
    pub key: String,
    pub value: Value,
}

impl Predicate for EqPredicate {
    fn evaluate(&self, row: &Row) -> Result<bool, FilterError> {
        Ok(row.get(&self.key).map(|v| values_equal(v, &self.value)).unwrap_or(false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use copperdb_cypher::Expression;
    use serde_json::json;

    #[test]
    fn test_eq_predicate() {
        let pred = EqPredicate {
            key: "name".to_string(),
            value: json!("Alice"),
        };
        let mut row = HashMap::new();
        row.insert("name".to_string(), json!("Alice"));
        assert!(pred.evaluate(&row).unwrap());
        let mut row2 = HashMap::new();
        row2.insert("name".to_string(), json!("Bob"));
        assert!(!pred.evaluate(&row2).unwrap());
    }

    #[test]
    fn test_literal() {
        let expr = Expression::Literal(json!(42));
        let row = HashMap::new();
        let params = HashMap::new();
        assert_eq!(eval_expression(&expr, &row, &params).unwrap(), json!(42));
    }

    #[test]
    fn test_variable_lookup() {
        let expr = Expression::Variable("n".to_string());
        let mut row = HashMap::new();
        row.insert("n".to_string(), json!({"name": "Alice"}));
        let params = HashMap::new();
        assert_eq!(
            eval_expression(&expr, &row, &params).unwrap(),
            json!({"name": "Alice"})
        );
    }

    #[test]
    fn test_property_access_nested() {
        let expr = Expression::PropertyAccess {
            variable: "n".to_string(),
            property: "name".to_string(),
        };
        let mut row = HashMap::new();
        row.insert("n".to_string(), json!({"name": "Alice", "age": 30}));
        let params = HashMap::new();
        assert_eq!(eval_expression(&expr, &row, &params).unwrap(), json!("Alice"));
    }

    #[test]
    fn test_property_access_dotted() {
        let expr = Expression::PropertyAccess {
            variable: "n".to_string(),
            property: "age".to_string(),
        };
        let mut row = HashMap::new();
        row.insert("n.age".to_string(), json!(30));
        let params = HashMap::new();
        assert_eq!(eval_expression(&expr, &row, &params).unwrap(), json!(30));
    }

    #[test]
    fn test_comparison_eq() {
        let expr = Expression::Comparison {
            left: Box::new(Expression::Literal(json!(5))),
            op: "=".to_string(),
            right: Box::new(Expression::Literal(json!(5))),
        };
        let row = HashMap::new();
        let params = HashMap::new();
        assert_eq!(eval_expression(&expr, &row, &params).unwrap(), json!(true));
    }

    #[test]
    fn test_comparison_lt() {
        let expr = Expression::Comparison {
            left: Box::new(Expression::Literal(json!(3))),
            op: "<".to_string(),
            right: Box::new(Expression::Literal(json!(5))),
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new(), &HashMap::new()).unwrap(),
            json!(true)
        );
    }

    #[test]
    fn test_comparison_contains() {
        let expr = Expression::Comparison {
            left: Box::new(Expression::Literal(json!("Hello World"))),
            op: "CONTAINS".to_string(),
            right: Box::new(Expression::Literal(json!("World"))),
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new(), &HashMap::new()).unwrap(),
            json!(true)
        );
    }

    #[test]
    fn test_and_short_circuit() {
        let expr = Expression::And(
            Box::new(Expression::Literal(json!(false))),
            Box::new(Expression::Literal(json!(true))),
        );
        assert_eq!(
            eval_predicate(&expr, &HashMap::new(), &HashMap::new()).unwrap(),
            false
        );
    }

    #[test]
    fn test_or_short_circuit() {
        let expr = Expression::Or(
            Box::new(Expression::Literal(json!(true))),
            Box::new(Expression::Literal(json!(false))),
        );
        assert_eq!(
            eval_predicate(&expr, &HashMap::new(), &HashMap::new()).unwrap(),
            true
        );
    }

    #[test]
    fn test_not() {
        let expr = Expression::Not(Box::new(Expression::Literal(json!(false))));
        assert_eq!(
            eval_predicate(&expr, &HashMap::new(), &HashMap::new()).unwrap(),
            true
        );
    }

    #[test]
    fn test_is_null() {
        let expr = Expression::IsNull(Box::new(Expression::Literal(Value::Null)));
        assert_eq!(
            eval_predicate(&expr, &HashMap::new(), &HashMap::new()).unwrap(),
            true
        );
    }

    #[test]
    fn test_is_not_null() {
        let expr = Expression::IsNotNull(Box::new(Expression::Literal(json!("hello"))));
        assert_eq!(
            eval_predicate(&expr, &HashMap::new(), &HashMap::new()).unwrap(),
            true
        );
    }

    #[test]
    fn test_function_toupper() {
        let expr = Expression::FunctionCall {
            name: "toUpper".to_string(),
            args: vec![Expression::Literal(json!("hello"))],
            distinct: false,
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new(), &HashMap::new()).unwrap(),
            json!("HELLO")
        );
    }

    #[test]
    fn test_function_size_array() {
        let expr = Expression::FunctionCall {
            name: "size".to_string(),
            args: vec![Expression::Literal(json!([1, 2, 3]))],
            distinct: false,
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new(), &HashMap::new()).unwrap(),
            json!(3)
        );
    }

    #[test]
    fn test_function_substring() {
        let expr = Expression::FunctionCall {
            name: "substring".to_string(),
            args: vec![
                Expression::Literal(json!("Hello World")),
                Expression::Literal(json!(6)),
                Expression::Literal(json!(5)),
            ],
            distinct: false,
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new(), &HashMap::new()).unwrap(),
            json!("World")
        );
    }

    #[test]
    fn test_parameter_lookup() {
        let expr = Expression::Parameter("name".to_string());
        let row = HashMap::new();
        let mut params = HashMap::new();
        params.insert("name".to_string(), json!("Alice"));
        assert_eq!(eval_expression(&expr, &row, &params).unwrap(), json!("Alice"));
    }

    #[test]
    fn test_string_ordering() {
        // "apple" < "banana" in lexicographic order
        let expr_lt = Expression::Comparison {
            left: Box::new(Expression::Literal(json!("apple"))),
            op: "<".to_string(),
            right: Box::new(Expression::Literal(json!("banana"))),
        };
        assert_eq!(
            eval_expression(&expr_lt, &HashMap::new(), &HashMap::new()).unwrap(),
            json!(true)
        );

        // "zebra" > "ant"
        let expr_gt = Expression::Comparison {
            left: Box::new(Expression::Literal(json!("zebra"))),
            op: ">".to_string(),
            right: Box::new(Expression::Literal(json!("ant"))),
        };
        assert_eq!(
            eval_expression(&expr_gt, &HashMap::new(), &HashMap::new()).unwrap(),
            json!(true)
        );

        // equal strings
        let expr_eq = Expression::Comparison {
            left: Box::new(Expression::Literal(json!("same"))),
            op: "=".to_string(),
            right: Box::new(Expression::Literal(json!("same"))),
        };
        assert_eq!(
            eval_expression(&expr_eq, &HashMap::new(), &HashMap::new()).unwrap(),
            json!(true)
        );
    }

    #[test]
    fn test_type_error_propagated_from_predicate() {
        // Comparing a number and a string with < should return a FilterError,
        // not silently return false.
        let expr = Expression::Comparison {
            left: Box::new(Expression::Literal(json!(42))),
            op: "<".to_string(),
            right: Box::new(Expression::Literal(json!("text"))),
        };
        assert!(eval_predicate(&expr, &HashMap::new(), &HashMap::new()).is_err());
    }
}
