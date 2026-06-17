//! Result set filtering and predicate evaluation.
//!
//! Equivalent to Go's `pkg/filter` in NornicDB.
//! Applies WHERE clause predicates and result projections to query output rows.

use copperdb_cypher::{Expression, LiteralValue};
use serde_json::{Map, Value};
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
        Expression::Literal(v) => Ok(literal_value_to_json(v)),

        Expression::Parameter(name) => params
            .get(name)
            .cloned()
            .ok_or_else(|| FilterError::UnknownVariable(format!("parameter ${name}"))),

        Expression::ParameterPropertyAccess {
            parameter,
            property,
        } => {
            let param_val = params
                .get(parameter.as_str())
                .cloned()
                .ok_or_else(|| {
                    FilterError::UnknownVariable(format!("parameter ${parameter}"))
                })?;
            if let Value::Object(map) = param_val {
                Ok(map
                    .get(property.as_str())
                    .cloned()
                    .unwrap_or(Value::Null))
            } else {
                Ok(Value::Null)
            }
        }

        Expression::Variable(name) => row
            .get(name)
            .cloned()
            .ok_or_else(|| FilterError::UnknownVariable(name.clone())),

        Expression::PropertyAccess { variable, property } => {
            // Try row["variable.property"] first, then row["variable"]["property"],
            // then params["variable"]["property"] for dynamic labels like $(d.labels)
            let dot_key = format!("{variable}.{property}");
            if let Some(v) = row.get(&dot_key) {
                return Ok(v.clone());
            }
            if let Some(Value::Object(map)) = row.get(variable.as_str()) {
                return Ok(map.get(property.as_str()).cloned().unwrap_or(Value::Null));
            }
            // Fall back to params for bare-identifier context-path refs (SET n:$(d.labels))
            if let Some(Value::Object(map)) = params.get(variable.as_str()) {
                return Ok(map.get(property.as_str()).cloned().unwrap_or(Value::Null));
            }
            Ok(Value::Null)
        }

        Expression::Comparison { operands, op } => {
            let lv = eval_expression(&operands.left, row, params)?;
            let rv = eval_expression(&operands.right, row, params)?;
            Ok(Value::Bool(compare_values(&lv, op, &rv)?))
        }

        Expression::InList { operands, negated } => {
            let needle = eval_expression(&operands.left, row, params)?;
            let haystack = eval_expression(&operands.right, row, params)?;
            let contains = match haystack {
                Value::Array(items) => items.iter().any(|item| values_equal(item, &needle)),
                _ => return Err(FilterError::TypeError("IN requires a list value".into())),
            };
            Ok(Value::Bool(if *negated { !contains } else { contains }))
        }

        Expression::Between {
            expression,
            lower,
            upper,
        } => {
            let val = eval_expression(expression, row, params)?;
            let lo = eval_expression(lower, row, params)?;
            let hi = eval_expression(upper, row, params)?;
            Ok(Value::Bool(
                compare_values(&val, ">=", &lo).unwrap_or(false)
                    && compare_values(&val, "<=", &hi).unwrap_or(false),
            ))
        }

        Expression::And(operands) => {
            // Short-circuit
            if !eval_predicate(&operands.left, row, params)? {
                return Ok(Value::Bool(false));
            }
            Ok(Value::Bool(eval_predicate(&operands.right, row, params)?))
        }

        Expression::Or(operands) => {
            if eval_predicate(&operands.left, row, params)? {
                return Ok(Value::Bool(true));
            }
            Ok(Value::Bool(eval_predicate(&operands.right, row, params)?))
        }

        Expression::Not(inner) => Ok(Value::Bool(!eval_predicate(inner, row, params)?)),

        Expression::IsNull(inner) => {
            let v = eval_expression(inner, row, params)?;
            Ok(Value::Bool(v == Value::Null))
        }

        Expression::IsNotNull(inner) => {
            let v = eval_expression(inner, row, params)?;
            Ok(Value::Bool(v != Value::Null))
        }

        Expression::Add(operands) => {
            let lv = eval_expression(&operands.left, row, params)?;
            let rv = eval_expression(&operands.right, row, params)?;
            // Numeric addition when both sides are numbers
            match (&lv, &rv) {
                (Value::Number(a), Value::Number(b)) => {
                    if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
                        return Ok(Value::Number((ai + bi).into()));
                    }
                    let af = a.as_f64().unwrap_or(0.0);
                    let bf = b.as_f64().unwrap_or(0.0);
                    return Ok(Value::Number(
                        serde_json::Number::from_f64(af + bf)
                            .unwrap_or(serde_json::Number::from(0)),
                    ));
                }
                _ => {}
            }
            // String concatenation otherwise
            Ok(Value::String(format!("{}{}", coerce_string(&lv)?, coerce_string(&rv)?)))
        }

        Expression::Subtract(operands) => {
            let lv = eval_expression(&operands.left, row, params)?;
            let rv = eval_expression(&operands.right, row, params)?;
            match (&lv, &rv) {
                (Value::Number(a), Value::Number(b)) => {
                    if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
                        Ok(Value::Number((ai - bi).into()))
                    } else {
                        let af = a.as_f64().unwrap_or(0.0);
                        let bf = b.as_f64().unwrap_or(0.0);
                        Ok(Value::Number(
                            serde_json::Number::from_f64(af - bf)
                                .unwrap_or(serde_json::Number::from(0)),
                        ))
                    }
                }
                _ => Err(FilterError::TypeError(
                    "subtraction requires numeric operands".into(),
                )),
            }
        }

        Expression::Multiply(operands) => {
            let lv = eval_expression(&operands.left, row, params)?;
            let rv = eval_expression(&operands.right, row, params)?;
            match (&lv, &rv) {
                (Value::Number(a), Value::Number(b)) => {
                    if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
                        Ok(Value::Number((ai * bi).into()))
                    } else {
                        let af = a.as_f64().unwrap_or(0.0);
                        let bf = b.as_f64().unwrap_or(0.0);
                        Ok(Value::Number(
                            serde_json::Number::from_f64(af * bf)
                                .unwrap_or(serde_json::Number::from(0)),
                        ))
                    }
                }
                _ => Err(FilterError::TypeError(
                    "multiplication requires numeric operands".into(),
                )),
            }
        }

        Expression::Divide(operands) => {
            let lv = eval_expression(&operands.left, row, params)?;
            let rv = eval_expression(&operands.right, row, params)?;
            match (&lv, &rv) {
                (Value::Number(a), Value::Number(b)) => {
                    let bf = b.as_f64().unwrap_or(0.0);
                    if bf == 0.0 {
                        return Ok(Value::Null);
                    }
                    if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
                        if bi != 0 && ai % bi == 0 {
                            return Ok(Value::Number((ai / bi).into()));
                        }
                    }
                    let af = a.as_f64().unwrap_or(0.0);
                    Ok(Value::Number(
                        serde_json::Number::from_f64(af / bf)
                            .unwrap_or(serde_json::Number::from(0)),
                    ))
                }
                _ => Err(FilterError::TypeError(
                    "division requires numeric operands".into(),
                )),
            }
        }

        Expression::Modulo(operands) => {
            let lv = eval_expression(&operands.left, row, params)?;
            let rv = eval_expression(&operands.right, row, params)?;
            match (&lv, &rv) {
                (Value::Number(a), Value::Number(b)) => {
                    if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
                        if bi != 0 {
                            return Ok(Value::Number((ai % bi).into()));
                        }
                    }
                    let af = a.as_f64().unwrap_or(0.0);
                    let bf = b.as_f64().unwrap_or(0.0);
                    if bf == 0.0 {
                        return Ok(Value::Null);
                    }
                    Ok(Value::Number(
                        serde_json::Number::from_f64(af % bf)
                            .unwrap_or(serde_json::Number::from(0)),
                    ))
                }
                _ => Err(FilterError::TypeError(
                    "modulo requires numeric operands".into(),
                )),
            }
        }

        Expression::Xor(operands) => {
            let lv = eval_predicate(&operands.left, row, params)?;
            let rv = eval_predicate(&operands.right, row, params)?;
            Ok(Value::Bool(lv != rv))
        }

        Expression::PatternExists { .. } => {
            // PatternExists is evaluated by the eval engine (has storage access).
            // At the filter level, return true to let eval handle it.
            Ok(Value::Bool(true))
        }

        Expression::FunctionCall {
            name,
            args,
            distinct: _,
        } => eval_function(name, args, row, params),

        Expression::ListLiteral(items) => items
            .iter()
            .map(|item| eval_expression(item, row, params))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),

        Expression::ListComprehension(comp) => {
            let list_val = eval_expression(&comp.list, row, params)?;
            let Value::Array(items) = list_val else {
                return Ok(Value::Array(vec![]));
            };
            let mut results = Vec::new();
            for item in &items {
                // Bind variable to current item
                let mut ext_row = row.clone();
                ext_row.insert(comp.variable.clone(), item.clone());
                // Evaluate predicate if present
                if let Some(ref pred) = comp.predicate {
                    if !eval_predicate(pred, &ext_row, params)? {
                        continue;
                    }
                }
                let result = eval_expression(&comp.expression, &ext_row, params)?;
                results.push(result);
            }
            Ok(Value::Array(results))
        }

        Expression::Reduce(reduce) => {
            let list_val = eval_expression(&reduce.list, row, params)?;
            let Value::Array(items) = list_val else {
                return Ok(Value::Null);
            };
            let mut acc = eval_expression(&reduce.initial, row, params)?;
            for item in &items {
                let mut ext_row = row.clone();
                ext_row.insert(reduce.accumulator.clone(), acc.clone());
                ext_row.insert(reduce.variable.clone(), item.clone());
                acc = eval_expression(&reduce.expression, &ext_row, params)?;
            }
            Ok(acc)
        }

        Expression::MapLiteral(entries) => {
            let mut map = Map::new();
            for entry in entries {
                map.insert(
                    entry.key.clone(),
                    eval_expression(&entry.value, row, params)?,
                );
            }
            Ok(Value::Object(map))
        }

        Expression::Case(case) => {
            // Simple CASE: compare expression to each WHEN value
            if let Some(ref input) = case.expression {
                for alt in &case.alternatives {
                    let cond_val = eval_expression(&alt.condition, row, params)?;
                    let input_val = eval_expression(input, row, params)?;
                    if values_equal(&input_val, &cond_val) {
                        return eval_expression(&alt.result, row, params);
                    }
                }
            } else {
                // Searched CASE: evaluate each WHEN predicate
                for alt in &case.alternatives {
                    if eval_predicate(&alt.condition, row, params)? {
                        return eval_expression(&alt.result, row, params);
                    }
                }
            }
            // ELSE default
            if let Some(ref default) = case.default {
                eval_expression(default, row, params)
            } else {
                Ok(Value::Null)
            }
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
        "=~" => {
            let s = coerce_string(left)?;
            let pat = coerce_string(right)?;
            let re = regex::Regex::new(&pat)
                .map_err(|e| FilterError::TypeError(format!("invalid regex: {e}")))?;
            Ok(re.is_match(&s))
        }
        _ => Err(FilterError::TypeError(format!("unknown operator: {op}"))),
    }
}

fn literal_value_to_json(value: &LiteralValue) -> Value {
    match value {
        LiteralValue::String(value) => Value::String(value.clone()),
        LiteralValue::Integer(value) => Value::Number((*value).into()),
        LiteralValue::Float(value) => serde_json::Number::from_f64(*value)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        LiteralValue::Bool(value) => Value::Bool(*value),
        LiteralValue::Null => Value::Null,
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
            if f1 < f2 {
                Ok(-1)
            } else if f1 > f2 {
                Ok(1)
            } else {
                Ok(0)
            }
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
        _ => Err(FilterError::TypeError(format!(
            "cannot coerce {v:?} to string"
        ))),
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
                _ => Err(FilterError::TypeError(format!(
                    "size() not applicable to {v:?}"
                ))),
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
        "nodes" => {
            let v = eval_arg(0)?;
            Ok(path_component(&v, "nodes").unwrap_or(Value::Array(vec![])))
        }
        "relationships" => {
            let v = eval_arg(0)?;
            Ok(path_component(&v, "relationships").unwrap_or(Value::Array(vec![])))
        }
        "length" => {
            let v = eval_arg(0)?;
            Ok(path_component(&v, "length").unwrap_or(Value::Null))
        }
        "id" | "elementid" => {
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
                Value::Number(n) => Ok(Value::Number(serde_json::Number::from(
                    n.as_i64().unwrap_or(0),
                ))),
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
                        serde_json::Number::from_f64(f).unwrap_or(serde_json::Number::from(0)),
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
            let parts: Vec<Value> = s
                .split(delim.as_str())
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
            let start = eval_arg(1)?.as_i64().unwrap_or(0).max(0) as usize;
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
                let keys: Vec<Value> = map
                    .keys()
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
                // Preserve integer type when possible
                if let Some(i) = n.as_i64() {
                    return Ok(Value::Number(i.abs().into()));
                }
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
                        serde_json::Number::from_f64(result).unwrap_or(serde_json::Number::from(0)),
                    ));
                }
            }
            Err(FilterError::TypeError(format!(
                "{name}() requires a number"
            )))
        }
        "sign" => {
            let v = eval_arg(0)?;
            if let Value::Number(n) = &v {
                if let Some(f) = n.as_f64() {
                    let s = if f > 0.0 { 1 } else if f < 0.0 { -1 } else { 0 };
                    return Ok(Value::Number(s.into()));
                }
            }
            Err(FilterError::TypeError("sign() requires a number".into()))
        }
        "sqrt" => {
            let v = eval_arg(0)?;
            if let Value::Number(n) = &v {
                if let Some(f) = n.as_f64() {
                    if f >= 0.0 {
                        return Ok(Value::Number(
                            serde_json::Number::from_f64(f.sqrt())
                                .unwrap_or(serde_json::Number::from(0)),
                        ));
                    }
                }
            }
            Err(FilterError::TypeError("sqrt() requires a non-negative number".into()))
        }
        "rand" => {
            let v = eval_arg(0)?;
            let max = v.as_f64().unwrap_or(1.0);
            let r: f64 = rand::random::<f64>() * max;
            Ok(Value::Number(
                serde_json::Number::from_f64((r * 1_000_000.0).round() / 1_000_000.0)
                    .unwrap_or(serde_json::Number::from(0)),
            ))
        }
        "pi" => Ok(Value::Number(
            serde_json::Number::from_f64(std::f64::consts::PI)
                .unwrap_or(serde_json::Number::from(3)),
        )),
        "range" => {
            let start = eval_arg(0)?.as_i64().unwrap_or(0);
            let end = eval_arg(1)?.as_i64().unwrap_or(0);
            let step: i64 = args
                .get(2)
                .map(|e| eval_expression(e, row, params))
                .transpose()?
                .and_then(|v| v.as_i64())
                .unwrap_or(1);
            let list: Vec<Value> = if step > 0 {
                (start..=end).step_by(step as usize).map(|i| Value::Number(i.into())).collect()
            } else {
                Vec::new()
            };
            Ok(Value::Array(list))
        }
        "head" => {
            let v = eval_arg(0)?;
            if let Value::Array(arr) = &v {
                Ok(arr.first().cloned().unwrap_or(Value::Null))
            } else {
                Ok(Value::Null)
            }
        }
        "tail" => {
            let v = eval_arg(0)?;
            if let Value::Array(arr) = &v {
                if arr.is_empty() {
                    Ok(Value::Array(vec![]))
                } else {
                    Ok(Value::Array(arr[1..].to_vec()))
                }
            } else {
                Ok(Value::Array(vec![]))
            }
        }
        "last" => {
            let v = eval_arg(0)?;
            if let Value::Array(arr) = &v {
                Ok(arr.last().cloned().unwrap_or(Value::Null))
            } else {
                Ok(Value::Null)
            }
        }
        "reverse" => {
            let v = eval_arg(0)?;
            match &v {
                Value::Array(arr) => {
                    let mut rev = arr.clone();
                    rev.reverse();
                    Ok(Value::Array(rev))
                }
                Value::String(s) => Ok(Value::String(s.chars().rev().collect())),
                _ => Ok(v),
            }
        }
        "all" => {
            let v = eval_arg(0)?;
            let _predicate_name = name; // reserved for future predicate evaluation
            if let Value::Array(arr) = &v {
                for item in arr {
                    // Evaluate the predicate expression against current row,
                    // substituting `item` into a variable.
                    // For now, return true if list is non-empty.
                    if item == &Value::Null {
                        return Ok(Value::Bool(false));
                    }
                }
                Ok(Value::Bool(true))
            } else {
                Ok(Value::Bool(false))
            }
        }
        "any" => {
            let v = eval_arg(0)?;
            if let Value::Array(arr) = &v {
                for _item in arr {
                    return Ok(Value::Bool(true));
                }
                Ok(Value::Bool(false))
            } else {
                Ok(Value::Bool(false))
            }
        }
        "none" => {
            let v = eval_arg(0)?;
            if let Value::Array(arr) = &v {
                Ok(Value::Bool(arr.is_empty()))
            } else {
                Ok(Value::Bool(true))
            }
        }
        "single" => {
            let v = eval_arg(0)?;
            if let Value::Array(arr) = &v {
                Ok(Value::Bool(arr.len() == 1))
            } else {
                Ok(Value::Bool(false))
            }
        }
        // ── Trig functions ──
        "sin" | "cos" | "tan" | "asin" | "acos" | "atan" | "atan2"
        | "degrees" | "radians" => {
            let v = eval_arg(0)?;
            if let Some(f) = v.as_f64() {
                let result = match name_lower.as_str() {
                    "sin" => f.sin(),
                    "cos" => f.cos(),
                    "tan" => f.tan(),
                    "asin" => f.asin(),
                    "acos" => f.acos(),
                    "atan" => f.atan(),
                    "atan2" => {
                        let x = eval_arg(1)?.as_f64().unwrap_or(0.0);
                        f.atan2(x)
                    }
                    "degrees" => f.to_degrees(),
                    "radians" => f.to_radians(),
                    _ => f,
                };
                return Ok(Value::Number(
                    serde_json::Number::from_f64(result).unwrap_or(serde_json::Number::from(0)),
                ));
            }
            Err(FilterError::TypeError(format!("{name}() requires a number")))
        }
        // ── Power / log functions ──
        "pow" | "power" => {
            let base = eval_arg(0)?.as_f64().unwrap_or(0.0);
            let exp = eval_arg(1)?.as_f64().unwrap_or(0.0);
            Ok(Value::Number(
                serde_json::Number::from_f64(base.powf(exp))
                    .unwrap_or(serde_json::Number::from(0)),
            ))
        }
        "exp" => {
            let v = eval_arg(0)?.as_f64().unwrap_or(0.0);
            Ok(Value::Number(
                serde_json::Number::from_f64(v.exp()).unwrap_or(serde_json::Number::from(0)),
            ))
        }
        "log" => {
            let v = eval_arg(0)?.as_f64().unwrap_or(1.0);
            Ok(Value::Number(
                serde_json::Number::from_f64(v.ln()).unwrap_or(serde_json::Number::from(0)),
            ))
        }
        "log10" => {
            let v = eval_arg(0)?.as_f64().unwrap_or(1.0);
            Ok(Value::Number(
                serde_json::Number::from_f64(v.log10()).unwrap_or(serde_json::Number::from(0)),
            ))
        }
        "randomuuid" => {
            let uuid = uuid::Uuid::new_v4().to_string();
            Ok(Value::String(uuid))
        }
        "toboolean" | "bool" => {
            let v = eval_arg(0)?;
            match &v {
                Value::Bool(b) => Ok(Value::Bool(*b)),
                Value::String(s) => Ok(Value::Bool(!s.is_empty() && s != "false")),
                Value::Number(n) => Ok(Value::Bool(n.as_f64().unwrap_or(0.0) != 0.0)),
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Bool(true)),
            }
        }
        "tolist" => {
            let v = eval_arg(0)?;
            match &v {
                Value::Array(_) => Ok(v),
                Value::Null => Ok(Value::Array(vec![])),
                _ => Ok(Value::Array(vec![v])),
            }
        }
        "isempty" => {
            let v = eval_arg(0)?;
            match &v {
                Value::String(s) => Ok(Value::Bool(s.is_empty())),
                Value::Array(a) => Ok(Value::Bool(a.is_empty())),
                Value::Object(m) => Ok(Value::Bool(m.is_empty())),
                Value::Null => Ok(Value::Bool(true)),
                _ => Ok(Value::Bool(false)),
            }
        }
        // ── Hyperbolic functions ──
        "sinh" | "cosh" | "tanh" => {
            let v = eval_arg(0)?;
            if let Some(f) = v.as_f64() {
                let result = match name_lower.as_str() {
                    "sinh" => f.sinh(),
                    "cosh" => f.cosh(),
                    "tanh" => f.tanh(),
                    _ => f,
                };
                return Ok(Value::Number(
                    serde_json::Number::from_f64(result).unwrap_or(serde_json::Number::from(0)),
                ));
            }
            Err(FilterError::TypeError(format!("{name}() requires a number")))
        }
        // ── Math constants ──
        "e" => Ok(Value::Number(
            serde_json::Number::from_f64(std::f64::consts::E)
                .unwrap_or(serde_json::Number::from(2)),
        )),
        // ── Null-safe functions ──
        "nullif" => {
            let a = eval_arg(0)?;
            let b = eval_arg(1)?;
            if values_equal(&a, &b) {
                Ok(Value::Null)
            } else {
                Ok(a)
            }
        }
        "valuetype" | "valuetypeof" => {
            let v = eval_arg(0)?;
            let s = match &v {
                Value::Null => "NULL",
                Value::Bool(_) => "BOOLEAN",
                Value::Number(n) => {
                    if n.is_f64() { "FLOAT" } else { "INTEGER" }
                }
                Value::String(_) => "STRING",
                Value::Array(_) => "LIST",
                Value::Object(_) => "MAP",
            };
            Ok(Value::String(s.to_string()))
        }
        "char_length" | "character_length" => {
            let s = coerce_string(&eval_arg(0)?)?;
            Ok(Value::Number(s.chars().count().into()))
        }
        "tointegerornull" => {
            let v = eval_arg(0)?;
            match &v {
                Value::String(s) => Ok(s.parse::<i64>()
                    .ok()
                    .map(|i| Value::Number(i.into()))
                    .unwrap_or(Value::Null)),
                Value::Number(n) => Ok(Value::Number(n.as_i64().unwrap_or(0).into())),
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "tofloatornull" => {
            let v = eval_arg(0)?;
            match &v {
                Value::String(s) => Ok(s.parse::<f64>()
                    .ok()
                    .and_then(|f| serde_json::Number::from_f64(f))
                    .map(Value::Number)
                    .unwrap_or(Value::Null)),
                Value::Number(n) => Ok(Value::Number(
                    serde_json::Number::from_f64(n.as_f64().unwrap_or(0.0))
                        .unwrap_or(serde_json::Number::from(0)),
                )),
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "tobooleanornull" => {
            let v = eval_arg(0)?;
            match &v {
                Value::Bool(b) => Ok(Value::Bool(*b)),
                Value::String(s) => {
                    if s == "true" { Ok(Value::Bool(true)) }
                    else if s == "false" { Ok(Value::Bool(false)) }
                    else { Ok(Value::Null) }
                }
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "slice" => {
            let v = eval_arg(0)?;
            if let Value::Array(arr) = &v {
                let from = eval_arg(1)?.as_i64().unwrap_or(0).max(0) as usize;
                let to = args.get(2)
                    .map(|e| eval_expression(e, row, params))
                    .transpose()?
                    .and_then(|v| v.as_i64())
                    .map(|i| i.max(0) as usize)
                    .unwrap_or(arr.len());
                let from = from.min(arr.len());
                let to = to.min(arr.len());
                if from < to {
                    Ok(Value::Array(arr[from..to].to_vec()))
                } else {
                    Ok(Value::Array(vec![]))
                }
            } else {
                Ok(Value::Array(vec![]))
            }
        }
        "indexof" => {
            let v = eval_arg(0)?;
            let needle = eval_arg(1)?;
            if let Value::Array(arr) = &v {
                for (i, item) in arr.iter().enumerate() {
                    if values_equal(item, &needle) {
                        return Ok(Value::Number((i as i64).into()));
                    }
                }
                Ok(Value::Number((-1).into()))
            } else {
                Ok(Value::Number((-1).into()))
            }
        }
        _ => Err(FilterError::UnknownFunction(name.to_string())),
    }
}

fn path_component(value: &Value, key: &str) -> Option<Value> {
    let Value::Object(map) = value else {
        return None;
    };
    map.get(key).cloned()
}

// ─── Legacy predicate trait ──────────────────────────────────────────────────

/// Represents a predicate that can be applied to a result row.
pub trait Predicate: Send + Sync {
    fn evaluate(&self, row: &Row) -> Result<bool, FilterError>;
}

/// Filter a list of rows using a predicate.
pub fn filter_rows<P: Predicate>(rows: Vec<Row>, predicate: &P) -> Result<Vec<Row>, FilterError> {
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
        Ok(row
            .get(&self.key)
            .map(|v| values_equal(v, &self.value))
            .unwrap_or(false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use copperdb_cypher::{BinaryExpression, Expression, LiteralValue, PropertyEntry};
    use serde_json::json;

    fn literal_int(value: i64) -> Expression {
        Expression::Literal(LiteralValue::Integer(value))
    }

    fn literal_string(value: &str) -> Expression {
        Expression::Literal(LiteralValue::String(value.to_string()))
    }

    fn literal_bool(value: bool) -> Expression {
        Expression::Literal(LiteralValue::Bool(value))
    }

    fn literal_null() -> Expression {
        Expression::Literal(LiteralValue::Null)
    }

    fn list(items: Vec<Expression>) -> Expression {
        Expression::ListLiteral(items)
    }

    fn binary(left: Expression, right: Expression) -> Box<BinaryExpression> {
        Box::new(BinaryExpression { left, right })
    }

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
        let expr = literal_int(42);
        let row = HashMap::new();
        let params = HashMap::new();
        assert_eq!(eval_expression(&expr, &row, &params).unwrap(), json!(42));
    }

    #[test]
    fn test_list_literal_evaluates_items() {
        let expr = list(vec![
            literal_int(1),
            Expression::Parameter("second".into()),
            Expression::Variable("third".into()),
        ]);
        let mut row = HashMap::new();
        row.insert("third".into(), json!(3));
        let mut params = HashMap::new();
        params.insert("second".into(), json!(2));

        assert_eq!(
            eval_expression(&expr, &row, &params).unwrap(),
            json!([1, 2, 3])
        );
    }

    #[test]
    fn test_map_literal_evaluates_values() {
        let expr = Expression::MapLiteral(vec![
            PropertyEntry {
                key: "name".into(),
                value: Expression::Parameter("name".into()),
            },
            PropertyEntry {
                key: "tags".into(),
                value: list(vec![Expression::Variable("tag".into())]),
            },
        ]);
        let mut row = HashMap::new();
        row.insert("tag".into(), json!("engineer"));
        let mut params = HashMap::new();
        params.insert("name".into(), json!("Ada"));

        assert_eq!(
            eval_expression(&expr, &row, &params).unwrap(),
            json!({"name": "Ada", "tags": ["engineer"]})
        );
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
        assert_eq!(
            eval_expression(&expr, &row, &params).unwrap(),
            json!("Alice")
        );
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
            operands: binary(literal_int(5), literal_int(5)),
            op: "=".to_string(),
        };
        let row = HashMap::new();
        let params = HashMap::new();
        assert_eq!(eval_expression(&expr, &row, &params).unwrap(), json!(true));
    }

    #[test]
    fn test_comparison_lt() {
        let expr = Expression::Comparison {
            operands: binary(literal_int(3), literal_int(5)),
            op: "<".to_string(),
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new(), &HashMap::new()).unwrap(),
            json!(true)
        );
    }

    #[test]
    fn test_comparison_contains() {
        let expr = Expression::Comparison {
            operands: binary(literal_string("Hello World"), literal_string("World")),
            op: "CONTAINS".to_string(),
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new(), &HashMap::new()).unwrap(),
            json!(true)
        );
    }

    #[test]
    fn test_comparison_regex_match() {
        let expr = Expression::Comparison {
            operands: binary(literal_string("Alice"), literal_string("A.*")),
            op: "=~".to_string(),
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new(), &HashMap::new()).unwrap(),
            json!(true)
        );
    }

    #[test]
    fn test_in_and_not_in_list() {
        let expr = Expression::InList {
            operands: binary(
                Expression::Variable("status".into()),
                list(vec![literal_string("active"), literal_string("pending")]),
            ),
            negated: false,
        };
        let negated = Expression::InList {
            operands: binary(
                Expression::Variable("status".into()),
                list(vec![literal_string("deleted")]),
            ),
            negated: true,
        };
        let mut row = HashMap::new();
        row.insert("status".into(), json!("active"));

        assert_eq!(
            eval_expression(&expr, &row, &HashMap::new()).unwrap(),
            json!(true)
        );
        assert_eq!(
            eval_expression(&negated, &row, &HashMap::new()).unwrap(),
            json!(true)
        );
    }

    #[test]
    fn test_and_short_circuit() {
        let expr = Expression::And(binary(literal_bool(false), literal_bool(true)));
        assert!(!eval_predicate(&expr, &HashMap::new(), &HashMap::new()).unwrap());
    }

    #[test]
    fn test_or_short_circuit() {
        let expr = Expression::Or(binary(literal_bool(true), literal_bool(false)));
        assert!(eval_predicate(&expr, &HashMap::new(), &HashMap::new()).unwrap());
    }

    #[test]
    fn test_not() {
        let expr = Expression::Not(Box::new(literal_bool(false)));
        assert!(eval_predicate(&expr, &HashMap::new(), &HashMap::new()).unwrap());
    }

    #[test]
    fn test_is_null() {
        let expr = Expression::IsNull(Box::new(literal_null()));
        assert!(eval_predicate(&expr, &HashMap::new(), &HashMap::new()).unwrap());
    }

    #[test]
    fn test_is_not_null() {
        let expr = Expression::IsNotNull(Box::new(literal_string("hello")));
        assert!(eval_predicate(&expr, &HashMap::new(), &HashMap::new()).unwrap());
    }

    #[test]
    fn test_function_toupper() {
        let expr = Expression::FunctionCall {
            name: "toUpper".to_string(),
            args: vec![literal_string("hello")],
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
            args: vec![list(vec![literal_int(1), literal_int(2), literal_int(3)])],
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
                literal_string("Hello World"),
                literal_int(6),
                literal_int(5),
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
        assert_eq!(
            eval_expression(&expr, &row, &params).unwrap(),
            json!("Alice")
        );
    }

    #[test]
    fn test_string_ordering() {
        // "apple" < "banana" in lexicographic order
        let expr_lt = Expression::Comparison {
            operands: binary(literal_string("apple"), literal_string("banana")),
            op: "<".to_string(),
        };
        assert_eq!(
            eval_expression(&expr_lt, &HashMap::new(), &HashMap::new()).unwrap(),
            json!(true)
        );

        // "zebra" > "ant"
        let expr_gt = Expression::Comparison {
            operands: binary(literal_string("zebra"), literal_string("ant")),
            op: ">".to_string(),
        };
        assert_eq!(
            eval_expression(&expr_gt, &HashMap::new(), &HashMap::new()).unwrap(),
            json!(true)
        );

        // equal strings
        let expr_eq = Expression::Comparison {
            operands: binary(literal_string("same"), literal_string("same")),
            op: "=".to_string(),
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
            operands: binary(literal_int(42), literal_string("text")),
            op: "<".to_string(),
        };
        assert!(eval_predicate(&expr, &HashMap::new(), &HashMap::new()).is_err());
    }
}
