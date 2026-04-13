//! Cypher query evaluator for magnetDB.
//!
//! Executes Cypher ASTs from `magnetdb-cypher` against the storage engine.

use magnetdb_cypher::{Clause, Expression, Query, ReturnItem};
use magnetdb_filter::{eval_predicate, eval_expression};
use magnetdb_storage::StorageEngine;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

pub use magnetdb_filter::Row;

#[derive(Debug, Error)]
pub enum EvalError {
    #[error("execution error: {0}")]
    ExecutionError(String),
    #[error("type error: {0}")]
    TypeError(String),
    #[error("storage error: {0}")]
    StorageError(String),
    #[error("filter error: {0}")]
    FilterError(String),
    #[error("serialization error: {0}")]
    SerializationError(String),
}

impl From<magnetdb_storage::StorageError> for EvalError {
    fn from(e: magnetdb_storage::StorageError) -> Self {
        EvalError::StorageError(e.to_string())
    }
}

impl From<magnetdb_filter::FilterError> for EvalError {
    fn from(e: magnetdb_filter::FilterError) -> Self {
        EvalError::FilterError(e.to_string())
    }
}

/// Statistics about what a query did.
#[derive(Debug, Default, Clone)]
pub struct QueryStats {
    pub nodes_created: usize,
    pub nodes_deleted: usize,
    pub relationships_created: usize,
    pub relationships_deleted: usize,
    pub properties_set: usize,
}

/// The result of executing a query.
pub struct EvalResult {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    pub stats: QueryStats,
}

/// The query executor.
pub struct EvalEngine {
    storage: Arc<StorageEngine>,
}

impl EvalEngine {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }

    /// Execute a parsed Cypher query against the storage engine.
    pub fn execute(
        &self,
        query: &Query,
        params: &HashMap<String, Value>,
    ) -> Result<EvalResult, EvalError> {
        let mut current_rows: Vec<Row> = vec![HashMap::new()];
        let mut stats = QueryStats::default();
        let mut columns: Vec<String> = vec![];
        let mut result_rows: Vec<Row> = vec![];

        for clause in &query.clauses {
            match clause {
                Clause::Create(create) => {
                    for node_pat in &create.pattern.nodes {
                        let label = node_pat.labels.first().cloned().unwrap_or_else(|| "Node".to_string());
                        let id = Uuid::new_v4().to_string();
                        let key = format!("{label}:{id}");

                        // Build the stored properties map
                        let mut props: HashMap<String, Value> = node_pat.properties.clone();
                        props.insert("_id".to_string(), Value::String(key.clone()));
                        props.insert(
                            "_labels".to_string(),
                            Value::Array(
                                node_pat.labels.iter().map(|l| Value::String(l.clone())).collect(),
                            ),
                        );

                        let bytes = rmp_serde::to_vec_named(&props)
                            .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                        self.storage.put_node(&key, &bytes)?;
                        stats.nodes_created += 1;
                        stats.properties_set += node_pat.properties.len();

                        // Bind the variable in current rows
                        if let Some(var) = &node_pat.variable {
                            let node_val = serde_json::to_value(&props)
                                .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                            for row in &mut current_rows {
                                row.insert(var.clone(), node_val.clone());
                            }
                        }
                    }
                    // Handle edges in CREATE
                    for edge_pat in &create.pattern.edges {
                        if let Some(var) = &edge_pat.variable {
                            let rel_type = edge_pat.rel_type.clone().unwrap_or_else(|| "REL".to_string());
                            let id = Uuid::new_v4().to_string();
                            let key = format!("{rel_type}:{id}");
                            let mut props: HashMap<String, Value> = edge_pat.properties.clone();
                            props.insert("_id".to_string(), Value::String(key.clone()));
                            props.insert("_type".to_string(), Value::String(rel_type));
                            let bytes = rmp_serde::to_vec_named(&props)
                                .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                            self.storage.put_edge(&key, &bytes)?;
                            stats.relationships_created += 1;
                            let edge_val = serde_json::to_value(&props)
                                .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                            for row in &mut current_rows {
                                row.insert(var.clone(), edge_val.clone());
                            }
                        } else {
                            // Anonymous edge still created
                            let rel_type = edge_pat.rel_type.clone().unwrap_or_else(|| "REL".to_string());
                            let id = Uuid::new_v4().to_string();
                            let key = format!("{rel_type}:{id}");
                            let mut props: HashMap<String, Value> = edge_pat.properties.clone();
                            props.insert("_id".to_string(), Value::String(key.clone()));
                            props.insert("_type".to_string(), Value::String(rel_type));
                            let bytes = rmp_serde::to_vec_named(&props)
                                .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                            self.storage.put_edge(&key, &bytes)?;
                            stats.relationships_created += 1;
                        }
                    }
                }

                Clause::Match(match_clause) => {
                    let mut new_rows: Vec<Row> = vec![];
                    for node_pat in &match_clause.pattern.nodes {
                        let label = node_pat.labels.first().cloned().unwrap_or_default();
                        let prefix = if label.is_empty() {
                            String::new()
                        } else {
                            format!("{label}:")
                        };

                        let scan_iter: Vec<_> = self.storage.scan_nodes_with_prefix(&prefix).collect();
                        for item in scan_iter {
                            let (_key, val) = item.map_err(|e| EvalError::StorageError(e.to_string()))?;
                            let props: HashMap<String, Value> = rmp_serde::from_slice(&val)
                                .map_err(|e| EvalError::SerializationError(e.to_string()))?;

                            // Check inline property constraints
                            let matches = node_pat.properties.iter().all(|(k, v)| {
                                props.get(k).map(|pv| pv == v).unwrap_or(false)
                            });
                            if !matches {
                                continue;
                            }

                            let node_val = serde_json::to_value(&props)
                                .map_err(|e| EvalError::SerializationError(e.to_string()))?;

                            // Combine with all current rows
                            for base_row in &current_rows {
                                let mut row = base_row.clone();
                                if let Some(var) = &node_pat.variable {
                                    row.insert(var.clone(), node_val.clone());
                                }
                                new_rows.push(row);
                            }
                        }
                    }
                    if !match_clause.pattern.nodes.is_empty() {
                        current_rows = new_rows;
                    }
                }

                Clause::OptionalMatch(match_clause) => {
                    // Like MATCH but preserve rows that don't match
                    let mut new_rows: Vec<Row> = vec![];
                    for node_pat in &match_clause.pattern.nodes {
                        let label = node_pat.labels.first().cloned().unwrap_or_default();
                        let prefix = if label.is_empty() {
                            String::new()
                        } else {
                            format!("{label}:")
                        };
                        let scan_iter: Vec<_> = self.storage.scan_nodes_with_prefix(&prefix).collect();
                        let mut found_any = false;
                        for item in scan_iter {
                            let (_key, val) = item.map_err(|e| EvalError::StorageError(e.to_string()))?;
                            let props: HashMap<String, Value> = rmp_serde::from_slice(&val)
                                .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                            let matches = node_pat.properties.iter().all(|(k, v)| {
                                props.get(k).map(|pv| pv == v).unwrap_or(false)
                            });
                            if !matches { continue; }
                            let node_val = serde_json::to_value(&props)
                                .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                            for base_row in &current_rows {
                                let mut row = base_row.clone();
                                if let Some(var) = &node_pat.variable {
                                    row.insert(var.clone(), node_val.clone());
                                }
                                new_rows.push(row);
                                found_any = true;
                            }
                        }
                        if !found_any {
                            // Preserve rows with null binding
                            for base_row in &current_rows {
                                let mut row = base_row.clone();
                                if let Some(var) = &node_pat.variable {
                                    row.insert(var.clone(), Value::Null);
                                }
                                new_rows.push(row);
                            }
                        }
                    }
                    if !match_clause.pattern.nodes.is_empty() {
                        current_rows = new_rows;
                    }
                }

                Clause::Where(where_clause) => {
                    let expr = &where_clause.expression;
                    let mut filtered = Vec::with_capacity(current_rows.len());
                    for row in current_rows {
                        match eval_predicate(expr, &row, params) {
                            Ok(true) => filtered.push(row),
                            Ok(false) => {}
                            Err(e) => return Err(EvalError::FilterError(e.to_string())),
                        }
                    }
                    current_rows = filtered;
                }

                Clause::Return(ret) => {
                    columns = ret.items.iter().map(|item| column_name(item)).collect();

                    let mut rows: Vec<Row> = current_rows
                        .iter()
                        .map(|row| project_row(row, &ret.items, params))
                        .collect::<Result<Vec<_>, _>>()?;

                    // ORDER BY
                    if !ret.order_by.is_empty() {
                        let order = ret.order_by.clone();
                        let params_clone = params.clone();
                        rows.sort_by(|a, b| {
                            for item in &order {
                                let av = eval_expression(&item.expression, a, &params_clone)
                                    .unwrap_or(Value::Null);
                                let bv = eval_expression(&item.expression, b, &params_clone)
                                    .unwrap_or(Value::Null);
                                let ord = compare_json(&av, &bv);
                                if ord != std::cmp::Ordering::Equal {
                                    return if item.descending { ord.reverse() } else { ord };
                                }
                            }
                            std::cmp::Ordering::Equal
                        });
                    }

                    // SKIP / LIMIT
                    if let Some(skip) = ret.skip {
                        let skip = skip.max(0) as usize;
                        rows = rows.into_iter().skip(skip).collect();
                    }
                    if let Some(limit) = ret.limit {
                        let limit = limit.max(0) as usize;
                        rows.truncate(limit);
                    }

                    // DISTINCT
                    if ret.distinct {
                        let mut seen = std::collections::HashSet::new();
                        rows = rows
                            .into_iter()
                            .filter(|r| seen.insert(row_key(r)))
                            .collect();
                    }

                    result_rows = rows;
                }

                Clause::Delete(del) => {
                    let vars_to_delete: Vec<String> = del.variables.clone();
                    let mut remaining_rows: Vec<Row> = vec![];
                    for row in &current_rows {
                        for var in &vars_to_delete {
                            if let Some(Value::Object(props)) = row.get(var) {
                                if let Some(Value::String(id)) = props.get("_id") {
                                    self.storage.delete_node(id)?;
                                    stats.nodes_deleted += 1;
                                }
                            }
                        }
                        remaining_rows.push(row.clone());
                    }
                    current_rows = remaining_rows;
                }

                Clause::Set(set) => {
                    for row in &mut current_rows {
                        for item in &set.items {
                            let new_val = eval_expression(&item.value, row, params)?;
                            // Update in-memory row
                            if let Some(Value::Object(ref mut props)) = row.get_mut(&item.variable) {
                                props.insert(item.property.clone(), new_val.clone());
                                stats.properties_set += 1;
                                // Persist to storage
                                if let Some(Value::String(id)) = props.get("_id") {
                                    let id = id.clone();
                                    let new_props: HashMap<String, Value> = props.clone().into_iter().collect();
                                    let bytes = rmp_serde::to_vec_named(&new_props)
                                        .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                                    self.storage.put_node(&id, &bytes)?;
                                }
                            }
                        }
                    }
                }

                Clause::With(with) => {
                    // Project rows like RETURN but continue pipeline
                    let items = &with.items;
                    let projected: Vec<Row> = current_rows
                        .iter()
                        .map(|row| project_row(row, items, params))
                        .collect::<Result<Vec<_>, _>>()?;

                    if let Some(where_clause) = &with.where_clause {
                        current_rows = projected
                            .into_iter()
                            .filter(|row| {
                                eval_predicate(&where_clause.expression, row, params).unwrap_or(false)
                            })
                            .collect();
                    } else {
                        current_rows = projected;
                    }
                }

                Clause::Unwind(unwind) => {
                    let mut new_rows: Vec<Row> = vec![];
                    for row in &current_rows {
                        let list_val = eval_expression(&unwind.expression, row, params)?;
                        if let Value::Array(items) = list_val {
                            for item in items {
                                let mut new_row = row.clone();
                                new_row.insert(unwind.variable.clone(), item);
                                new_rows.push(new_row);
                            }
                        }
                    }
                    current_rows = new_rows;
                }

                Clause::Merge(merge) => {
                    // MERGE: match-or-create
                    for node_pat in &merge.pattern.nodes {
                        let label = node_pat.labels.first().cloned().unwrap_or_else(|| "Node".to_string());
                        let prefix = format!("{label}:");
                        let mut found = false;
                        let scan_iter: Vec<_> = self.storage.scan_nodes_with_prefix(&prefix).collect();
                        for item in scan_iter {
                            let (_key, val) = item.map_err(|e| EvalError::StorageError(e.to_string()))?;
                            let props: HashMap<String, Value> = rmp_serde::from_slice(&val)
                                .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                            let matches = node_pat.properties.iter().all(|(k, v)| {
                                props.get(k).map(|pv| pv == v).unwrap_or(false)
                            });
                            if matches {
                                found = true;
                                let node_val = serde_json::to_value(&props)
                                    .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                                if let Some(var) = &node_pat.variable {
                                    for row in &mut current_rows {
                                        row.insert(var.clone(), node_val.clone());
                                    }
                                }
                                break;
                            }
                        }
                        if !found {
                            let id = Uuid::new_v4().to_string();
                            let key = format!("{label}:{id}");
                            let mut props: HashMap<String, Value> = node_pat.properties.clone();
                            props.insert("_id".to_string(), Value::String(key.clone()));
                            props.insert(
                                "_labels".to_string(),
                                Value::Array(node_pat.labels.iter().map(|l| Value::String(l.clone())).collect()),
                            );
                            let bytes = rmp_serde::to_vec_named(&props)
                                .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                            self.storage.put_node(&key, &bytes)?;
                            stats.nodes_created += 1;
                            stats.properties_set += node_pat.properties.len();
                            let node_val = serde_json::to_value(&props)
                                .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                            if let Some(var) = &node_pat.variable {
                                for row in &mut current_rows {
                                    row.insert(var.clone(), node_val.clone());
                                }
                            }
                        }
                    }
                }
            }
        }

        // If no RETURN clause, result_rows is empty
        Ok(EvalResult {
            columns,
            rows: result_rows,
            stats,
        })
    }
}

// ─── Legacy Executor (kept for backwards compat) ─────────────────────────────

/// Legacy executor stub (use EvalEngine instead).
pub struct Executor {}

impl Executor {
    pub fn new() -> Self {
        Self {}
    }
    pub fn execute(
        &self,
        _query: &str,
        _params: HashMap<String, Value>,
    ) -> Result<Vec<Row>, EvalError> {
        Err(EvalError::ExecutionError("use EvalEngine instead".into()))
    }
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn column_name(item: &ReturnItem) -> String {
    if let Some(alias) = &item.alias {
        return alias.clone();
    }
    match &item.expression {
        Expression::Variable(v) => v.clone(),
        Expression::PropertyAccess { variable, property } => format!("{variable}.{property}"),
        Expression::FunctionCall { name, args, .. } => {
            if args.is_empty() {
                name.clone()
            } else {
                format!("{name}({})", expression_name(args.first().unwrap()))
            }
        }
        other => expression_name(other),
    }
}

fn expression_name(expr: &Expression) -> String {
    match expr {
        Expression::Variable(v) => v.clone(),
        Expression::PropertyAccess { variable, property } => format!("{variable}.{property}"),
        Expression::Literal(v) => v.to_string(),
        _ => "expr".to_string(),
    }
}

fn project_row(row: &Row, items: &[ReturnItem], params: &HashMap<String, Value>) -> Result<Row, EvalError> {
    let mut result = HashMap::new();
    for item in items {
        let col = column_name(item);
        let val = eval_expression(&item.expression, row, params)
            .map_err(|e| EvalError::FilterError(e.to_string()))?;
        result.insert(col, val);
    }
    Ok(result)
}

fn compare_json(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Number(n1), Value::Number(n2)) => {
            let f1 = n1.as_f64().unwrap_or(f64::NAN);
            let f2 = n2.as_f64().unwrap_or(f64::NAN);
            f1.partial_cmp(&f2).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::String(s1), Value::String(s2)) => s1.cmp(s2),
        _ => std::cmp::Ordering::Equal,
    }
}

fn row_key(row: &Row) -> String {
    let mut keys: Vec<_> = row.keys().collect();
    keys.sort();
    keys.iter().map(|k| format!("{}={}", k, row[*k])).collect::<Vec<_>>().join(",")
}

#[cfg(test)]
mod tests {
    use super::*;
    use magnetdb_cypher::Parser;
    use magnetdb_storage::StorageEngine;

    fn make_engine() -> EvalEngine {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        EvalEngine::new(storage)
    }

    #[test]
    fn test_create_node() {
        let engine = make_engine();
        let parser = Parser::new();
        let query = parser.parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert_eq!(result.stats.nodes_created, 1);
        assert_eq!(result.stats.properties_set, 2);
    }

    #[test]
    fn test_match_returns_created_node() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (n:Person {name: 'Alice'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let q2 = parser.parse("MATCH (n:Person) RETURN n").unwrap();
        let result = engine.execute(&q2, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert!(result.columns.contains(&"n".to_string()));
    }

    #[test]
    fn test_match_where_filter() {
        let engine = make_engine();
        let parser = Parser::new();
        engine.execute(&parser.parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap(), &HashMap::new()).unwrap();
        engine.execute(&parser.parse("CREATE (n:Person {name: 'Bob', age: 25})").unwrap(), &HashMap::new()).unwrap();

        let q = parser.parse("MATCH (n:Person) WHERE n.name = 'Alice' RETURN n").unwrap();
        let result = engine.execute(&q, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
        if let Some(Value::Object(props)) = result.rows[0].get("n") {
            assert_eq!(props.get("name"), Some(&Value::String("Alice".into())));
        } else {
            panic!("expected object");
        }
    }

    #[test]
    fn test_delete_node() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(&parser.parse("CREATE (n:Person {name: 'Alice'})").unwrap(), &HashMap::new())
            .unwrap();
        let q = parser.parse("MATCH (n:Person) DELETE n").unwrap();
        let result = engine.execute(&q, &HashMap::new()).unwrap();
        assert_eq!(result.stats.nodes_deleted, 1);

        let q2 = parser.parse("MATCH (n:Person) RETURN n").unwrap();
        let after = engine.execute(&q2, &HashMap::new()).unwrap();
        assert_eq!(after.rows.len(), 0);
    }

    #[test]
    fn test_match_with_inline_properties() {
        let engine = make_engine();
        let parser = Parser::new();
        engine.execute(&parser.parse("CREATE (n:Car {make: 'Toyota', year: 2020})").unwrap(), &HashMap::new()).unwrap();
        engine.execute(&parser.parse("CREATE (n:Car {make: 'Honda', year: 2019})").unwrap(), &HashMap::new()).unwrap();

        let q = parser.parse("MATCH (n:Car {make: 'Toyota'}) RETURN n").unwrap();
        let result = engine.execute(&q, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_return_property() {
        let engine = make_engine();
        let parser = Parser::new();
        engine.execute(&parser.parse("CREATE (n:City {name: 'London'})").unwrap(), &HashMap::new()).unwrap();
        let q = parser.parse("MATCH (n:City) RETURN n.name").unwrap();
        let result = engine.execute(&q, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("n.name"), Some(&Value::String("London".into())));
    }

    #[test]
    fn test_merge_creates_if_absent() {
        let engine = make_engine();
        let parser = Parser::new();
        let q = parser.parse("MERGE (n:Animal {species: 'Cat'})").unwrap();
        engine.execute(&q, &HashMap::new()).unwrap();
        engine.execute(&q, &HashMap::new()).unwrap(); // second merge should not create

        let q2 = parser.parse("MATCH (n:Animal) RETURN n").unwrap();
        let result = engine.execute(&q2, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_return_limit() {
        let engine = make_engine();
        let parser = Parser::new();
        for i in 0..5 {
            engine.execute(
                &parser.parse(&format!("CREATE (n:Num {{val: {i}}})")).unwrap(),
                &HashMap::new(),
            ).unwrap();
        }
        let q = parser.parse("MATCH (n:Num) RETURN n LIMIT 3").unwrap();
        let result = engine.execute(&q, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 3);
    }
}

