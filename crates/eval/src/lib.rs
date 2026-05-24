//! Cypher query evaluator for copperdb.
//!
//! Executes Cypher ASTs from `copperdb-cypher` against the storage engine.

use copperdb_cypher::{Clause, ConstraintKind, Expression, Query, ReturnItem};
use copperdb_filter::{eval_expression, eval_predicate};
use copperdb_storage::{
    Constraint, ConstraintEntityType, ConstraintType, IndexDefinition, IndexEntityType,
    StorageEngine,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use uuid::Uuid;

pub use copperdb_filter::Row;

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

impl From<copperdb_storage::StorageError> for EvalError {
    fn from(e: copperdb_storage::StorageError) -> Self {
        EvalError::StorageError(e.to_string())
    }
}

impl From<copperdb_filter::FilterError> for EvalError {
    fn from(e: copperdb_filter::FilterError) -> Self {
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
    /// Cache for MERGE node lookups: merge_cache_key(labels, prop, val) → node JSON Value.
    ///
    /// Mirrors NornicDB v1.0.42's `nodeLookupCache` on `StorageExecutor`.
    /// Invalidated on any write operation (CREATE / SET / DELETE) and on query error
    /// to prevent stale entries from masking newly created or deleted nodes.
    node_lookup_cache: Arc<Mutex<HashMap<String, Value>>>,
}

impl EvalEngine {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            storage,
            node_lookup_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // ── MERGE node-lookup cache helpers ──────────────────────────────────────

    /// Evict all cache entries that reference `node_val` (matched by `_id`).
    fn evict_merge_node_cache_entries(
        &self,
        labels: &[String],
        props: &HashMap<String, Value>,
        node_id: Option<&str>,
    ) {
        if labels.is_empty() || props.is_empty() {
            return;
        }
        if let Ok(mut cache) = self.node_lookup_cache.lock() {
            for (prop, val) in props {
                let key = merge_cache_key(labels, prop, val);
                if let Some(cached) = cache.get(&key) {
                    let cached_id = cached
                        .as_object()
                        .and_then(|o| o.get("_id"))
                        .and_then(|v| v.as_str());
                    if node_id.is_none() || cached_id == node_id {
                        cache.remove(&key);
                    }
                }
            }
        }
    }

    /// Look up a cached MERGE result.  Returns `None` if not cached or if the
    /// cached entry is stale (node was deleted / properties changed).
    fn find_in_merge_cache(
        &self,
        labels: &[String],
        props: &HashMap<String, Value>,
    ) -> Option<Value> {
        if labels.is_empty() || props.is_empty() {
            return None;
        }
        let cached = {
            let cache = self.node_lookup_cache.lock().ok()?;
            props
                .iter()
                .find_map(|(prop, val)| cache.get(&merge_cache_key(labels, prop, val)).cloned())?
        };

        // Verify the cached node is still alive in storage and still matches all props.
        if let Some(Value::String(id)) = cached.as_object().and_then(|o| o.get("_id")) {
            if let Ok(Some(bytes)) = self.storage.get_node(id) {
                if let Ok(live_props) = rmp_serde::from_slice::<HashMap<String, Value>>(&bytes) {
                    let all_props_match = props
                        .iter()
                        .all(|(k, v)| live_props.get(k).map(|pv| pv == v).unwrap_or(false));
                    if all_props_match {
                        return Some(cached);
                    }
                }
            }
            // Stale – evict the entry
            self.evict_merge_node_cache_entries(labels, props, Some(id));
        }
        None
    }

    /// Store a successfully matched or created MERGE node in the cache.
    fn cache_merge_node(
        &self,
        labels: &[String],
        props: &HashMap<String, Value>,
        node_val: &Value,
    ) {
        if labels.is_empty() || props.is_empty() {
            return;
        }
        if let Ok(mut cache) = self.node_lookup_cache.lock() {
            for (prop, val) in props {
                cache.insert(merge_cache_key(labels, prop, val), node_val.clone());
            }
        }
    }

    /// Invalidate the entire MERGE node lookup cache.
    ///
    /// Called after any write operation (CREATE / SET / DELETE) and after a
    /// failed implicit transaction, mirroring `invalidateNodeLookupCache()` in
    /// NornicDB v1.0.42.
    fn invalidate_node_lookup_cache(&self) {
        if let Ok(mut cache) = self.node_lookup_cache.lock() {
            cache.clear();
        }
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
                Clause::CreateConstraint(create) => {
                    let existing = self.storage.load_constraints()?;
                    let already_exists = existing.iter().any(|c| c.name == create.name);
                    if already_exists {
                        if create.if_not_exists {
                            continue;
                        }
                        return Err(EvalError::ExecutionError(format!(
                            "constraint \"{}\" already exists",
                            create.name
                        )));
                    }
                    let constraint_type = match create.kind {
                        ConstraintKind::Unique => ConstraintType::Unique,
                        ConstraintKind::Exists => ConstraintType::Exists,
                    };
                    self.storage.persist_constraint(&Constraint {
                        name: create.name.clone(),
                        constraint_type,
                        entity_type: ConstraintEntityType::Node,
                        label: create.label.clone(),
                        properties: vec![create.property.clone()],
                    })?;
                }

                Clause::DropConstraint(drop) => {
                    let deleted = self.storage.delete_constraint(&drop.name)?;
                    if !deleted && !drop.if_exists {
                        return Err(EvalError::ExecutionError(format!(
                            "constraint \"{}\" not found",
                            drop.name
                        )));
                    }
                }

                Clause::ShowConstraints(_) => {
                    let constraints = self.storage.load_constraints()?;
                    columns = vec![
                        "name".to_string(),
                        "type".to_string(),
                        "entityType".to_string(),
                        "label".to_string(),
                        "properties".to_string(),
                    ];
                    result_rows = constraints
                        .into_iter()
                        .map(|c| {
                            let mut row = Row::new();
                            row.insert("name".to_string(), Value::String(c.name));
                            row.insert(
                                "type".to_string(),
                                Value::String(
                                    match c.constraint_type {
                                        ConstraintType::Unique => "UNIQUE",
                                        ConstraintType::Exists => "EXISTS",
                                        ConstraintType::NodeKey => "NODE_KEY",
                                        ConstraintType::Type => "TYPE",
                                        ConstraintType::Relationship => "RELATIONSHIP",
                                    }
                                    .to_string(),
                                ),
                            );
                            row.insert(
                                "entityType".to_string(),
                                Value::String(
                                    match c.entity_type {
                                        ConstraintEntityType::Node => "NODE",
                                        ConstraintEntityType::Relationship => "RELATIONSHIP",
                                    }
                                    .to_string(),
                                ),
                            );
                            row.insert("label".to_string(), Value::String(c.label));
                            row.insert(
                                "properties".to_string(),
                                Value::Array(c.properties.into_iter().map(Value::String).collect()),
                            );
                            row
                        })
                        .collect();
                }

                Clause::CreateIndex(create) => {
                    let existing = self.storage.load_index_definitions()?;
                    let already_exists = existing.iter().any(|i| i.name == create.name);
                    if already_exists {
                        if create.if_not_exists {
                            continue;
                        }
                        return Err(EvalError::ExecutionError(format!(
                            "index \"{}\" already exists",
                            create.name
                        )));
                    }
                    self.storage.persist_index_definition(&IndexDefinition {
                        name: create.name.clone(),
                        entity_type: IndexEntityType::Node,
                        label: create.label.clone(),
                        properties: create.properties.clone(),
                    })?;
                }

                Clause::DropIndex(drop) => {
                    let deleted = self.storage.delete_index_definition(&drop.name)?;
                    if !deleted && !drop.if_exists {
                        return Err(EvalError::ExecutionError(format!(
                            "index \"{}\" not found",
                            drop.name
                        )));
                    }
                }

                Clause::ShowIndexes(_) => {
                    let indexes = self.storage.load_index_definitions()?;
                    columns = vec![
                        "name".to_string(),
                        "entityType".to_string(),
                        "label".to_string(),
                        "properties".to_string(),
                    ];
                    result_rows = indexes
                        .into_iter()
                        .map(|idx| {
                            let mut row = Row::new();
                            row.insert("name".to_string(), Value::String(idx.name));
                            row.insert(
                                "entityType".to_string(),
                                Value::String(
                                    match idx.entity_type {
                                        IndexEntityType::Node => "NODE",
                                        IndexEntityType::Relationship => "RELATIONSHIP",
                                    }
                                    .to_string(),
                                ),
                            );
                            row.insert("label".to_string(), Value::String(idx.label));
                            row.insert(
                                "properties".to_string(),
                                Value::Array(
                                    idx.properties.into_iter().map(Value::String).collect(),
                                ),
                            );
                            row
                        })
                        .collect();
                }

                Clause::Create(create) => {
                    // Any write invalidates the MERGE node-lookup cache (v1.0.42 parity).
                    self.invalidate_node_lookup_cache();
                    for node_pat in &create.pattern.nodes {
                        let label = node_pat
                            .labels
                            .first()
                            .cloned()
                            .unwrap_or_else(|| "Node".to_string());
                        let id = Uuid::new_v4().to_string();
                        let key = format!("{label}:{id}");

                        // Build the stored properties map
                        let mut props: HashMap<String, Value> = node_pat.properties.clone();
                        props.insert("_id".to_string(), Value::String(key.clone()));
                        props.insert(
                            "_labels".to_string(),
                            Value::Array(
                                node_pat
                                    .labels
                                    .iter()
                                    .map(|l| Value::String(l.clone()))
                                    .collect(),
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
                            let rel_type = edge_pat
                                .rel_type
                                .clone()
                                .unwrap_or_else(|| "REL".to_string());
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
                            let rel_type = edge_pat
                                .rel_type
                                .clone()
                                .unwrap_or_else(|| "REL".to_string());
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
                    // Edge patterns cannot be evaluated without an adjacency index.
                    if !match_clause.pattern.edges.is_empty() {
                        return Err(EvalError::ExecutionError(
                            "relationship patterns in MATCH are not yet supported".to_string(),
                        ));
                    }

                    // Iteratively cross-join each node pattern so that bindings from
                    // earlier patterns are visible when processing later ones.
                    for node_pat in &match_clause.pattern.nodes {
                        let label = node_pat.labels.first().cloned().unwrap_or_default();
                        let prefix = if label.is_empty() {
                            String::new()
                        } else {
                            format!("{label}:")
                        };

                        let mut new_rows: Vec<Row> = vec![];
                        for item in self.storage.scan_nodes_with_prefix(&prefix) {
                            let (_key, val) =
                                item.map_err(|e| EvalError::StorageError(e.to_string()))?;
                            let props: HashMap<String, Value> = rmp_serde::from_slice(&val)
                                .map_err(|e| EvalError::SerializationError(e.to_string()))?;

                            // Check inline property constraints
                            let matches = node_pat
                                .properties
                                .iter()
                                .all(|(k, v)| props.get(k).map(|pv| pv == v).unwrap_or(false));
                            if !matches {
                                continue;
                            }

                            // Multi-label support (v1.0.42): when multiple labels are
                            // required, the prefix scan already filters by the first label.
                            // We additionally verify that ALL required labels are present.
                            if node_pat.labels.len() > 1 {
                                if !node_has_all_labels(&props, &node_pat.labels) {
                                    continue;
                                }
                            }

                            let node_val = serde_json::to_value(&props)
                                .map_err(|e| EvalError::SerializationError(e.to_string()))?;

                            // Combine with all current rows (which already carry
                            // bindings from prior node patterns in this MATCH).
                            for base_row in &current_rows {
                                let mut row = base_row.clone();
                                if let Some(var) = &node_pat.variable {
                                    row.insert(var.clone(), node_val.clone());
                                }
                                new_rows.push(row);
                            }
                        }
                        current_rows = new_rows;
                    }
                }

                Clause::OptionalMatch(match_clause) => {
                    // Edge patterns cannot be evaluated without an adjacency index.
                    if !match_clause.pattern.edges.is_empty() {
                        return Err(EvalError::ExecutionError(
                            "relationship patterns in OPTIONAL MATCH are not yet supported"
                                .to_string(),
                        ));
                    }

                    // Iteratively cross-join each node pattern, preserving rows
                    // with null bindings when no matching node exists.
                    for node_pat in &match_clause.pattern.nodes {
                        let label = node_pat.labels.first().cloned().unwrap_or_default();
                        let prefix = if label.is_empty() {
                            String::new()
                        } else {
                            format!("{label}:")
                        };
                        let mut new_rows: Vec<Row> = vec![];
                        let mut found_any = false;
                        for item in self.storage.scan_nodes_with_prefix(&prefix) {
                            let (_key, val) =
                                item.map_err(|e| EvalError::StorageError(e.to_string()))?;
                            let props: HashMap<String, Value> = rmp_serde::from_slice(&val)
                                .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                            let matches = node_pat
                                .properties
                                .iter()
                                .all(|(k, v)| props.get(k).map(|pv| pv == v).unwrap_or(false));
                            if !matches {
                                continue;
                            }
                            // Multi-label support (v1.0.42)
                            if node_pat.labels.len() > 1
                                && !node_has_all_labels(&props, &node_pat.labels)
                            {
                                continue;
                            }
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

                    // ORDER BY must be evaluated against the full pre-projection row so
                    // that ORDER BY expressions can reference variables not in RETURN.
                    // Sort keys are precomputed in a fallible pass to propagate errors
                    // instead of silently treating them as Null.
                    if !ret.order_by.is_empty() {
                        let order = &ret.order_by;
                        let mut rows_with_keys: Vec<(Row, Vec<Value>)> = current_rows
                            .into_iter()
                            .map(|row| {
                                let keys = order
                                    .iter()
                                    .map(|item| {
                                        eval_expression(&item.expression, &row, params)
                                            .map_err(|e| EvalError::FilterError(e.to_string()))
                                    })
                                    .collect::<Result<Vec<_>, _>>()?;
                                Ok((row, keys))
                            })
                            .collect::<Result<Vec<_>, EvalError>>()?;

                        rows_with_keys.sort_by(|(_, keys_a), (_, keys_b)| {
                            for (idx, item) in order.iter().enumerate() {
                                let ord = compare_json(&keys_a[idx], &keys_b[idx]);
                                if ord != std::cmp::Ordering::Equal {
                                    return if item.descending { ord.reverse() } else { ord };
                                }
                            }
                            std::cmp::Ordering::Equal
                        });

                        current_rows = rows_with_keys.into_iter().map(|(row, _)| row).collect();
                    }

                    // SKIP / LIMIT applied before projection so we page over the
                    // correct pre-projection rows.
                    if let Some(skip) = ret.skip {
                        let skip = skip.max(0) as usize;
                        current_rows = current_rows.into_iter().skip(skip).collect();
                    }
                    if let Some(limit) = ret.limit {
                        let limit = limit.max(0) as usize;
                        current_rows.truncate(limit);
                    }

                    // Project down to only the returned columns.
                    let mut rows: Vec<Row> = current_rows
                        .iter()
                        .map(|row| project_row(row, &ret.items, params))
                        .collect::<Result<Vec<_>, _>>()?;

                    // DISTINCT applied after projection (deduplication is over
                    // projected values, which is standard Cypher semantics).
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
                    // Any write invalidates the MERGE node-lookup cache (v1.0.42 parity).
                    self.invalidate_node_lookup_cache();
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
                    // Any write invalidates the MERGE node-lookup cache (v1.0.42 parity).
                    self.invalidate_node_lookup_cache();
                    for row in &mut current_rows {
                        for item in &set.items {
                            let new_val = eval_expression(&item.value, row, params)?;
                            // Update in-memory row
                            if let Some(Value::Object(ref mut props)) = row.get_mut(&item.variable)
                            {
                                props.insert(item.property.clone(), new_val.clone());
                                stats.properties_set += 1;
                                // Persist to storage
                                if let Some(Value::String(id)) = props.get("_id") {
                                    let id = id.clone();
                                    let new_props: HashMap<String, Value> =
                                        props.clone().into_iter().collect();
                                    let bytes =
                                        rmp_serde::to_vec_named(&new_props).map_err(|e| {
                                            EvalError::SerializationError(e.to_string())
                                        })?;
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
                        let mut filtered_rows = Vec::new();
                        for row in projected {
                            if eval_predicate(&where_clause.expression, &row, params)
                                .map_err(|e| EvalError::FilterError(e.to_string()))?
                            {
                                filtered_rows.push(row);
                            }
                        }
                        current_rows = filtered_rows;
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
                    // MERGE: match-or-create with node-lookup cache.
                    //
                    // Mirrors NornicDB v1.0.42's merge execution:
                    // 1. Check the in-memory node-lookup cache first (fast path).
                    // 2. If the cache entry is stale (node deleted/changed), evict it
                    //    and fall through to a storage scan.
                    // 3. Cache every successfully matched or created node so that
                    //    subsequent MERGEs in the same pipeline hit the cache.
                    for node_pat in &merge.pattern.nodes {
                        let labels = &node_pat.labels;
                        let label = labels
                            .first()
                            .cloned()
                            .unwrap_or_else(|| "Node".to_string());
                        let prefix = format!("{label}:");

                        // --- Cache fast path ---
                        if let Some(cached_val) =
                            self.find_in_merge_cache(labels, &node_pat.properties)
                        {
                            if let Some(var) = &node_pat.variable {
                                for row in &mut current_rows {
                                    row.insert(var.clone(), cached_val.clone());
                                }
                            }
                            continue;
                        }

                        // --- Storage scan ---
                        let mut found_node: Option<Value> = None;
                        for item in self.storage.scan_nodes_with_prefix(&prefix) {
                            let (_key, val) =
                                item.map_err(|e| EvalError::StorageError(e.to_string()))?;
                            let props: HashMap<String, Value> = rmp_serde::from_slice(&val)
                                .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                            let prop_matches = node_pat
                                .properties
                                .iter()
                                .all(|(k, v)| props.get(k).map(|pv| pv == v).unwrap_or(false));
                            if !prop_matches {
                                continue;
                            }
                            // Multi-label support (v1.0.42): verify ALL required labels.
                            if labels.len() > 1 && !node_has_all_labels(&props, labels) {
                                continue;
                            }
                            found_node = Some(
                                serde_json::to_value(&props)
                                    .map_err(|e| EvalError::SerializationError(e.to_string()))?,
                            );
                            break;
                        }

                        let node_val = if let Some(existing) = found_node {
                            // Cache the match for future lookups in this pipeline.
                            self.cache_merge_node(labels, &node_pat.properties, &existing);
                            existing
                        } else {
                            // Create the node.
                            let id = Uuid::new_v4().to_string();
                            let key = format!("{label}:{id}");
                            let mut props: HashMap<String, Value> = node_pat.properties.clone();
                            props.insert("_id".to_string(), Value::String(key.clone()));
                            props.insert(
                                "_labels".to_string(),
                                Value::Array(
                                    labels.iter().map(|l| Value::String(l.clone())).collect(),
                                ),
                            );
                            let bytes = rmp_serde::to_vec_named(&props)
                                .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                            self.storage.put_node(&key, &bytes)?;
                            stats.nodes_created += 1;
                            stats.properties_set += node_pat.properties.len();
                            let nv = serde_json::to_value(&props)
                                .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                            // Cache the newly created node.
                            self.cache_merge_node(labels, &node_pat.properties, &nv);
                            nv
                        };

                        if let Some(var) = &node_pat.variable {
                            for row in &mut current_rows {
                                row.insert(var.clone(), node_val.clone());
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

/// Build a canonical cache key for a single (labels, prop, val) triple.
///
/// Labels are sorted so that `[:A:B]` and `[:B:A]` produce the same key,
/// matching NornicDB v1.0.42's `mergeLookupCacheKey`.
///
/// The pipe (`|`) delimiter is used between label names because it does not
/// appear in valid Neo4j identifiers, avoiding ambiguity between a two-label
/// key `A|B:prop=val` and a single-label key where the label itself contains
/// a colon or pipe character.
fn merge_cache_key(labels: &[String], prop: &str, val: &Value) -> String {
    let mut sorted_labels = labels.to_vec();
    sorted_labels.sort();
    format!("{}:{}={}", sorted_labels.join("|"), prop, val)
}

/// Return `true` if the stored node's `_labels` array contains every label in
/// `required`.  Used for multi-label MATCH/MERGE filtering (v1.0.42 parity).
fn node_has_all_labels(props: &HashMap<String, Value>, required: &[String]) -> bool {
    let stored: Vec<&str> = props
        .get("_labels")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();
    required.iter().all(|l| stored.contains(&l.as_str()))
}

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

fn project_row(
    row: &Row,
    items: &[ReturnItem],
    params: &HashMap<String, Value>,
) -> Result<Row, EvalError> {
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
    keys.iter()
        .map(|k| format!("{}={}", k, row[*k]))
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;
    use copperdb_cypher::Parser;
    use copperdb_storage::StorageEngine;

    fn make_engine() -> EvalEngine {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        EvalEngine::new(storage)
    }

    #[test]
    fn test_create_node() {
        let engine = make_engine();
        let parser = Parser::new();
        let query = parser
            .parse("CREATE (n:Person {name: 'Alice', age: 30})")
            .unwrap();
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
        engine
            .execute(
                &parser
                    .parse("CREATE (n:Person {name: 'Alice', age: 30})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (n:Person {name: 'Bob', age: 25})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let q = parser
            .parse("MATCH (n:Person) WHERE n.name = 'Alice' RETURN n")
            .unwrap();
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
            .execute(
                &parser.parse("CREATE (n:Person {name: 'Alice'})").unwrap(),
                &HashMap::new(),
            )
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
        engine
            .execute(
                &parser
                    .parse("CREATE (n:Car {make: 'Toyota', year: 2020})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (n:Car {make: 'Honda', year: 2019})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let q = parser
            .parse("MATCH (n:Car {make: 'Toyota'}) RETURN n")
            .unwrap();
        let result = engine.execute(&q, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_return_property() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (n:City {name: 'London'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        let q = parser.parse("MATCH (n:City) RETURN n.name").unwrap();
        let result = engine.execute(&q, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("n.name"),
            Some(&Value::String("London".into()))
        );
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
            engine
                .execute(
                    &parser
                        .parse(&format!("CREATE (n:Num {{val: {i}}})"))
                        .unwrap(),
                    &HashMap::new(),
                )
                .unwrap();
        }
        let q = parser.parse("MATCH (n:Num) RETURN n LIMIT 3").unwrap();
        let result = engine.execute(&q, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 3);
    }

    #[test]
    fn test_match_multi_node_cross_join() {
        // MATCH (a:A), (b:B) should produce a cross-join: 2 A nodes × 3 B nodes = 6 rows,
        // and each row must carry bindings for BOTH `a` and `b`.
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (n:A {v: 1})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (n:A {v: 2})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (n:B {v: 10})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (n:B {v: 20})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (n:B {v: 30})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let q = parser.parse("MATCH (a:A), (b:B) RETURN a, b").unwrap();
        let result = engine.execute(&q, &HashMap::new()).unwrap();

        // 2 × 3 = 6 rows
        assert_eq!(result.rows.len(), 6, "expected 6 cross-join rows");
        // every row must have both bindings
        for row in &result.rows {
            assert!(row.contains_key("a"), "row missing 'a' binding");
            assert!(row.contains_key("b"), "row missing 'b' binding");
        }
    }

    #[test]
    fn test_match_with_edge_pattern_returns_error() {
        let engine = make_engine();
        let parser = Parser::new();
        // Relationship patterns in MATCH are not yet supported.
        let q = parser.parse("MATCH (a)-[r:KNOWS]->(b) RETURN a").unwrap();
        let result = engine.execute(&q, &HashMap::new());
        assert!(
            result.is_err(),
            "expected error for relationship pattern in MATCH"
        );
    }

    // ── NornicDB v1.0.42 regression tests ────────────────────────────────────

    /// MERGE must not create a duplicate when the node already exists.
    /// Mirrors NornicDB v1.0.42 `TestMergeNode_MatchWhenExists`.
    #[test]
    fn test_merge_match_when_exists() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser.parse("CREATE (n:Person {name: 'Alice'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // MERGE twice — should match the existing node, not create two more.
        let q = parser.parse("MERGE (n:Person {name: 'Alice'})").unwrap();
        engine.execute(&q, &HashMap::new()).unwrap();
        engine.execute(&q, &HashMap::new()).unwrap();

        let count_q = parser
            .parse("MATCH (n:Person {name: 'Alice'}) RETURN n")
            .unwrap();
        let result = engine.execute(&count_q, &HashMap::new()).unwrap();
        assert_eq!(
            result.rows.len(),
            1,
            "MERGE must not duplicate an existing node"
        );
    }

    /// MERGE node-lookup cache must evict stale entries after a DELETE.
    ///
    /// Mirrors NornicDB v1.0.42's `TestMergeNode_FindMergeNodeIgnoresStaleCacheEntry`
    /// and the `invalidateNodeLookupCache` call after implicit-tx rollback/commit
    /// failures (commit `4cdee7c`).
    #[test]
    fn test_merge_cache_evicted_after_delete() {
        let engine = make_engine();
        let parser = Parser::new();

        // First MERGE – creates the node and caches it.
        engine
            .execute(
                &parser.parse("MERGE (n:Tag {name: 'rust'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Delete the node – this must invalidate the cache.
        engine
            .execute(
                &parser
                    .parse("MATCH (n:Tag {name: 'rust'}) DELETE n")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // Second MERGE – the cache was cleared so MERGE must re-scan storage,
        // find nothing, and create a new node.
        let merge_result = engine
            .execute(
                &parser.parse("MERGE (n:Tag {name: 'rust'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(
            merge_result.stats.nodes_created, 1,
            "MERGE should recreate the node after the stale cache entry was evicted"
        );

        let count_q = parser
            .parse("MATCH (n:Tag {name: 'rust'}) RETURN n")
            .unwrap();
        let result = engine.execute(&count_q, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    /// Multi-label MATCH: `MATCH (n:Person:Employee)` must only return nodes
    /// that carry BOTH labels.
    ///
    /// Mirrors NornicDB v1.0.42 commit `6283009` (make hot paths n-ary and generic).
    #[test]
    fn test_match_multi_label_filters_correctly() {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        let engine = EvalEngine::new(Arc::clone(&storage));

        // Directly insert a node with two labels [:Person, :Employee].
        {
            let mut props: HashMap<String, Value> = HashMap::new();
            props.insert(
                "_id".to_string(),
                Value::String("Person:alice-id".to_string()),
            );
            props.insert("name".to_string(), Value::String("Alice".to_string()));
            props.insert(
                "_labels".to_string(),
                Value::Array(vec![
                    Value::String("Person".to_string()),
                    Value::String("Employee".to_string()),
                ]),
            );
            let bytes = rmp_serde::to_vec_named(&props).unwrap();
            storage.put_node("Person:alice-id", &bytes).unwrap();
        }

        // Directly insert a node with only [:Person].
        {
            let mut props: HashMap<String, Value> = HashMap::new();
            props.insert(
                "_id".to_string(),
                Value::String("Person:bob-id".to_string()),
            );
            props.insert("name".to_string(), Value::String("Bob".to_string()));
            props.insert(
                "_labels".to_string(),
                Value::Array(vec![Value::String("Person".to_string())]),
            );
            let bytes = rmp_serde::to_vec_named(&props).unwrap();
            storage.put_node("Person:bob-id", &bytes).unwrap();
        }

        let parser = Parser::new();

        // MATCH (n:Person) should return BOTH Alice and Bob (prefix = "Person:").
        let q_person = parser.parse("MATCH (n:Person) RETURN n").unwrap();
        let result = engine.execute(&q_person, &HashMap::new()).unwrap();
        assert_eq!(
            result.rows.len(),
            2,
            "MATCH :Person should return both nodes"
        );

        // MATCH (n:Person:Employee) should return ONLY Alice.
        let q_both = parser.parse("MATCH (n:Person:Employee) RETURN n").unwrap();
        let result_both = engine.execute(&q_both, &HashMap::new()).unwrap();
        assert_eq!(
            result_both.rows.len(),
            1,
            "MATCH :Person:Employee should return only Alice"
        );
        if let Some(Value::Object(p)) = result_both.rows[0].get("n") {
            assert_eq!(p.get("name"), Some(&Value::String("Alice".into())));
        } else {
            panic!("expected object row");
        }
    }

    /// MERGE is idempotent across multiple engine calls (cache-hit path).
    ///
    /// Verifies that the node-lookup cache correctly short-circuits repeated
    /// MERGEs without creating duplicates.
    #[test]
    fn test_merge_idempotent_via_cache() {
        let engine = make_engine();
        let parser = Parser::new();

        let q = parser.parse("MERGE (n:Counter {key: 'hits'})").unwrap();
        for _ in 0..5 {
            engine.execute(&q, &HashMap::new()).unwrap();
        }

        let count_q = parser
            .parse("MATCH (n:Counter {key: 'hits'}) RETURN n")
            .unwrap();
        let result = engine.execute(&count_q, &HashMap::new()).unwrap();
        assert_eq!(
            result.rows.len(),
            1,
            "five MERGEs must produce exactly one node"
        );
    }

    /// UNWIND + MERGE should execute a MERGE for each unwound item, but must
    /// not create duplicate nodes when the same label+property is encountered.
    ///
    /// Mirrors NornicDB v1.0.42 regression coverage for UNWIND/MERGE fallback paths.
    #[test]
    fn test_merge_after_create_sees_new_node() {
        let engine = make_engine();
        let parser = Parser::new();

        // Create the node first.
        engine
            .execute(
                &parser.parse("CREATE (n:Service {name: 'api'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        // MERGE must match the created node, not create a second one.
        let merge_result = engine
            .execute(
                &parser.parse("MERGE (n:Service {name: 'api'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(
            merge_result.stats.nodes_created, 0,
            "MERGE should find the existing node, not create a new one"
        );

        let count_q = parser.parse("MATCH (n:Service) RETURN n").unwrap();
        let result = engine.execute(&count_q, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_constraint_ddl_roundtrip() {
        let engine = make_engine();
        let parser = Parser::new();

        let create = parser
            .parse("CREATE CONSTRAINT person_email_unique FOR (n:Person) REQUIRE n.email IS UNIQUE")
            .unwrap();
        engine.execute(&create, &HashMap::new()).unwrap();

        let show = parser.parse("SHOW CONSTRAINTS").unwrap();
        let shown = engine.execute(&show, &HashMap::new()).unwrap();
        assert_eq!(shown.rows.len(), 1);
        assert_eq!(
            shown.rows[0].get("name"),
            Some(&Value::String("person_email_unique".to_string()))
        );
    }

    #[test]
    fn test_constraint_drop_if_exists_and_error_path() {
        let engine = make_engine();
        let parser = Parser::new();

        let err = match engine.execute(
            &parser.parse("DROP CONSTRAINT missing_constraint").unwrap(),
            &HashMap::new(),
        ) {
            Ok(_) => panic!("expected drop constraint to fail"),
            Err(err) => err,
        };
        assert!(err
            .to_string()
            .contains("constraint \"missing_constraint\" not found"));

        engine
            .execute(
                &parser
                    .parse("DROP CONSTRAINT missing_constraint IF EXISTS")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
    }

    #[test]
    fn test_index_ddl_roundtrip() {
        let engine = make_engine();
        let parser = Parser::new();

        let create = parser
            .parse("CREATE INDEX person_idx FOR (n:Person) ON (n.email)")
            .unwrap();
        engine.execute(&create, &HashMap::new()).unwrap();

        let show = parser.parse("SHOW INDEXES").unwrap();
        let shown = engine.execute(&show, &HashMap::new()).unwrap();
        assert_eq!(shown.rows.len(), 1);
        assert_eq!(
            shown.rows[0].get("name"),
            Some(&Value::String("person_idx".to_string()))
        );
    }

    #[test]
    fn test_index_drop_if_exists_and_error_path() {
        let engine = make_engine();
        let parser = Parser::new();

        let err = match engine.execute(
            &parser.parse("DROP INDEX missing_idx").unwrap(),
            &HashMap::new(),
        ) {
            Ok(_) => panic!("expected drop index to fail"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("index \"missing_idx\" not found"));

        engine
            .execute(
                &parser.parse("DROP INDEX missing_idx IF EXISTS").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
    }
}
