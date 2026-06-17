//! Cypher query evaluator for copperdb.
//!
//! Executes Cypher ASTs from `copperdb-cypher` against the storage engine.

use copperdb_cypher::{
    hot_path_trace::{HotPathTrace, HotPathTraceState},
    Clause, ConstraintKind, EdgeDirection, EdgePattern, Expression, LiteralValue, NodePattern,
    Pattern, PatternInfo, PipelineClause, PipelineClauseKind, PropertyEntry, Query, QueryPattern,
    RemoveItem, ReturnItem, SetItem, ShapeKind, ShapeMatch, ShapeValue, WithClause,
};
use copperdb_filter::{eval_expression, eval_predicate};
use copperdb_indexing::{CatalogRangeIndexComparison, IndexCatalog, IndexError};
use copperdb_knowledgepolicy::{
    access_metadata_after_policy_access, build_binding_table, build_bundles_by_name,
    build_decay_bindings, build_promotion_policies_by_name, build_promotion_profiles_by_name,
    merge_access_metadata, score_binding, AccessFlusher, AccessMutationBuffer, CompiledBinding,
    PromotionProfileDef, Resolver, ScoreFromMode,
};
use copperdb_storage::{
    Constraint, ConstraintEntityType, ConstraintType, DecayProfileBindingSchema,
    DecayProfileSchema, EdgeRecord, KnowledgePolicyAccessMetadata, NodeRecord,
    PromotionOnAccessMutationKindSchema, PromotionOnAccessMutationSchema, PromotionPolicySchema,
    PromotionProfileSchema, PromotionWhenClauseSchema, StorageEngine,
};
use copperdb_util::{RequestCancelled, RequestContext};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use uuid::Uuid;

pub use copperdb_filter::Row;

const VAR_LENGTH_UNBOUNDED_MAX_HOPS: u32 = 1 << 16;

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
    #[error(transparent)]
    RequestCancelled(#[from] RequestCancelled),
}

impl From<copperdb_storage::StorageError> for EvalError {
    fn from(e: copperdb_storage::StorageError) -> Self {
        EvalError::StorageError(e.to_string())
    }
}

impl From<IndexError> for EvalError {
    fn from(e: IndexError) -> Self {
        EvalError::ExecutionError(e.to_string())
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

struct RelationshipMatchRow {
    row: Row,
    hops: usize,
}

struct RelationshipStepMatch {
    next_props: HashMap<String, Value>,
    next_value: Value,
    edge_binding_value: Value,
    node_values_tail: Vec<Value>,
    edge_values: Vec<Value>,
    hops: usize,
}

#[derive(Debug, Clone)]
struct KnowledgePolicyInspection {
    entity_id: Option<String>,
    target_kind: String,
    target_labels: Vec<String>,
    target_edge_type: Option<String>,
    decay_binding: Option<String>,
    promotion_policy: Option<String>,
    matched_promotion_profile: Option<String>,
    matched_promotion_predicate: Option<String>,
    score_from: Option<String>,
    anchor_unix_ms: Option<i64>,
    access_count: Option<u64>,
    last_accessed_at_unix_ms: Option<i64>,
    base_score: f64,
    final_score: f64,
    visibility_threshold: f64,
    suppressed: bool,
    dry_run: bool,
    explanation: String,
}

impl KnowledgePolicyInspection {
    fn into_row(self) -> Row {
        let mut row = Row::new();
        row.insert(
            "entityId".to_string(),
            self.entity_id.map(Value::String).unwrap_or(Value::Null),
        );
        row.insert("targetKind".to_string(), Value::String(self.target_kind));
        row.insert(
            "targetLabels".to_string(),
            Value::Array(self.target_labels.into_iter().map(Value::String).collect()),
        );
        row.insert(
            "targetEdgeType".to_string(),
            self.target_edge_type
                .map(Value::String)
                .unwrap_or(Value::Null),
        );
        row.insert(
            "decayBinding".to_string(),
            self.decay_binding.map(Value::String).unwrap_or(Value::Null),
        );
        row.insert(
            "promotionPolicy".to_string(),
            self.promotion_policy
                .map(Value::String)
                .unwrap_or(Value::Null),
        );
        row.insert(
            "matchedPromotionProfile".to_string(),
            self.matched_promotion_profile
                .map(Value::String)
                .unwrap_or(Value::Null),
        );
        row.insert(
            "matchedPromotionPredicate".to_string(),
            self.matched_promotion_predicate
                .map(Value::String)
                .unwrap_or(Value::Null),
        );
        row.insert(
            "scoreFrom".to_string(),
            self.score_from.map(Value::String).unwrap_or(Value::Null),
        );
        row.insert(
            "anchorUnixMs".to_string(),
            self.anchor_unix_ms.map(Value::from).unwrap_or(Value::Null),
        );
        row.insert(
            "accessCount".to_string(),
            self.access_count.map(Value::from).unwrap_or(Value::Null),
        );
        row.insert(
            "lastAccessedAtUnixMs".to_string(),
            self.last_accessed_at_unix_ms
                .map(Value::from)
                .unwrap_or(Value::Null),
        );
        row.insert("baseScore".to_string(), Value::from(self.base_score));
        row.insert("finalScore".to_string(), Value::from(self.final_score));
        row.insert(
            "visibilityThreshold".to_string(),
            Value::from(self.visibility_threshold),
        );
        row.insert("suppressed".to_string(), Value::Bool(self.suppressed));
        row.insert("dryRun".to_string(), Value::Bool(self.dry_run));
        row.insert("explanation".to_string(), Value::String(self.explanation));
        row
    }
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
    access_flusher: Arc<AccessFlusher>,
    hot_path_trace: Arc<HotPathTraceState>,
}

mod eval_engine;
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

fn pooled_binding_rows() -> Vec<Row> {
    copperdb_pool::get_binding_row_slice()
}

fn recycle_binding_rows(rows: Vec<Row>) {
    copperdb_pool::put_binding_row_slice(rows);
}

fn replace_binding_rows(current_rows: &mut Vec<Row>, new_rows: Vec<Row>) {
    let old_rows = std::mem::replace(current_rows, new_rows);
    recycle_binding_rows(old_rows);
}

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

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

#[allow(clippy::too_many_arguments)]
fn binding_visible_under_anchor(
    _engine: &EvalEngine,
    binding: &CompiledBinding,
    entity_id: &str,
    created_at_unix_ms: i64,
    updated_at_unix_ms: i64,
    access_metadata: Option<KnowledgePolicyAccessMetadata>,
    properties: &BTreeMap<String, Value>,
    params: &HashMap<String, Value>,
) -> Result<bool, EvalError> {
    if binding.no_decay {
        return Ok(true);
    }

    let Some(anchor_unix_ms) = binding_anchor_unix_ms(
        binding,
        created_at_unix_ms,
        updated_at_unix_ms,
        access_metadata
            .as_ref()
            .and_then(|metadata| metadata.last_accessed_at_unix_ms),
        properties,
    ) else {
        return Ok(true);
    };

    let matched_promotion =
        match_promotion_profile(binding, properties, access_metadata.as_ref(), params).map_err(
            |error| {
                EvalError::FilterError(format!(
                    "promotion predicate evaluation failed for {entity_id}: {error}"
                ))
            },
        )?;

    Ok(!score_binding(
        binding,
        Some(anchor_unix_ms),
        now_unix_ms(),
        matched_promotion.as_ref(),
    )
    .suppressed)
}

fn binding_anchor_unix_ms(
    binding: &copperdb_knowledgepolicy::CompiledBinding,
    created_at_unix_ms: i64,
    updated_at_unix_ms: i64,
    last_accessed_at_unix_ms: Option<i64>,
    properties: &BTreeMap<String, Value>,
) -> Option<i64> {
    match binding.score_from {
        ScoreFromMode::Created => Some(created_at_unix_ms),
        ScoreFromMode::Version => Some(updated_at_unix_ms),
        ScoreFromMode::LastAccessed => last_accessed_at_unix_ms.or(Some(created_at_unix_ms)),
        ScoreFromMode::Custom => binding
            .score_from_property
            .as_deref()
            .and_then(|property| properties.get(property))
            .and_then(value_as_unix_ms),
    }
}

fn value_as_unix_ms(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        .or_else(|| value.as_f64().map(|value| value as i64))
}

fn match_promotion_profile(
    binding: &CompiledBinding,
    properties: &BTreeMap<String, Value>,
    access_metadata: Option<&KnowledgePolicyAccessMetadata>,
    params: &HashMap<String, Value>,
) -> Result<Option<PromotionProfileDef>, copperdb_filter::FilterError> {
    Ok(
        matched_promotion_rule(binding, properties, access_metadata, params)?
            .map(|rule| rule.profile),
    )
}

fn matched_promotion_rule(
    binding: &CompiledBinding,
    properties: &BTreeMap<String, Value>,
    access_metadata: Option<&KnowledgePolicyAccessMetadata>,
    params: &HashMap<String, Value>,
) -> Result<Option<copperdb_knowledgepolicy::CompiledPromotionRule>, copperdb_filter::FilterError> {
    if binding.compiled_promotion_rules.is_empty() {
        return Ok(None);
    }

    for rule in &binding.compiled_promotion_rules {
        let row = promotion_predicate_row(&rule.expression, properties, access_metadata);
        if eval_predicate(&rule.expression, &row, params)? {
            return Ok(Some(rule.clone()));
        }
    }

    Ok(None)
}

fn promotion_predicate_row(
    expression: &Expression,
    properties: &BTreeMap<String, Value>,
    access_metadata: Option<&KnowledgePolicyAccessMetadata>,
) -> Row {
    let mut object = serde_json::Map::new();
    for (key, value) in properties {
        object.insert(key.clone(), value.clone());
    }
    object.insert(
        "accessCount".to_string(),
        Value::from(
            access_metadata
                .map(|metadata| metadata.access_count)
                .unwrap_or(0),
        ),
    );
    object.insert(
        "lastAccessedAt".to_string(),
        access_metadata
            .and_then(|metadata| metadata.last_accessed_at_unix_ms)
            .map(Value::from)
            .unwrap_or(Value::Null),
    );

    let binding_value = Value::Object(object);
    let mut row = Row::new();
    let mut variables = HashSet::new();
    collect_expression_variables(expression, &mut variables);
    for variable in variables {
        row.insert(variable, binding_value.clone());
    }
    row
}

fn collect_expression_variables(expression: &Expression, variables: &mut HashSet<String>) {
    match expression {
        Expression::PropertyAccess { variable, .. } | Expression::Variable(variable) => {
            variables.insert(variable.clone());
        }
        Expression::Comparison { operands, .. }
        | Expression::InList { operands, .. }
        | Expression::And(operands)
        | Expression::And(operands)
        | Expression::Or(operands)
        | Expression::Xor(operands)
        | Expression::Add(operands)
        | Expression::Subtract(operands)
        | Expression::Multiply(operands)
        | Expression::Divide(operands)
        | Expression::Modulo(operands) => {
            collect_expression_variables(&operands.left, variables);
            collect_expression_variables(&operands.right, variables);
        }
        Expression::FunctionCall { args, .. } | Expression::ListLiteral(args) => {
            for argument in args {
                collect_expression_variables(argument, variables);
            }
        }
        Expression::ListComprehension(comp) => {
            collect_expression_variables(&comp.list, variables);
            if let Some(ref pred) = comp.predicate {
                collect_expression_variables(pred, variables);
            }
            collect_expression_variables(&comp.expression, variables);
        }
        Expression::Reduce(reduce) => {
            collect_expression_variables(&reduce.initial, variables);
            collect_expression_variables(&reduce.list, variables);
            collect_expression_variables(&reduce.expression, variables);
        }
        Expression::MapLiteral(entries) => {
            for entry in entries {
                collect_expression_variables(&entry.value, variables);
            }
        }
        Expression::Not(inner) | Expression::IsNull(inner) | Expression::IsNotNull(inner) => {
            collect_expression_variables(inner, variables)
        }
        Expression::Between {
            expression,
            lower,
            upper,
        } => {
            collect_expression_variables(expression, variables);
            collect_expression_variables(lower, variables);
            collect_expression_variables(upper, variables);
        }
        Expression::Literal(_)
        | Expression::Parameter(_)
        | Expression::ParameterPropertyAccess { .. }
        | Expression::PatternExists { .. } => {}
        Expression::Case(case) => {
            if let Some(ref expr) = case.expression {
                collect_expression_variables(expr, variables);
            }
            for alt in &case.alternatives {
                collect_expression_variables(&alt.condition, variables);
                collect_expression_variables(&alt.result, variables);
            }
            if let Some(ref default) = case.default {
                collect_expression_variables(default, variables);
            }
        }
    }
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

fn node_matches_pattern(
    props: &HashMap<String, Value>,
    labels: &[String],
    expected_props: &HashMap<String, Value>,
) -> bool {
    expected_props.iter().all(|(key, value)| {
        props
            .get(key)
            .map(|actual| actual == value)
            .unwrap_or(false)
    }) && (labels.is_empty() || node_has_all_labels(props, labels))
}

fn edge_matches_pattern(edge: &EdgeRecord, expected_props: &HashMap<String, Value>) -> bool {
    expected_props.iter().all(|(key, value)| {
        edge.properties
            .get(key)
            .map(|actual| actual == value)
            .unwrap_or(false)
    })
}

fn evaluate_pattern_properties(
    properties: &[PropertyEntry],
    row: &Row,
    params: &HashMap<String, Value>,
) -> Result<HashMap<String, Value>, EvalError> {
    let mut out = HashMap::with_capacity(properties.len());
    for property in properties {
        let value = eval_expression(&property.value, row, params)
            .map_err(|e| EvalError::FilterError(e.to_string()))?;
        out.insert(property.key.clone(), value);
    }
    Ok(out)
}

fn node_id(props: &HashMap<String, Value>) -> Option<&str> {
    props.get("_id").and_then(Value::as_str)
}

fn pipeline_bound_node<'a>(
    row: &'a Row,
    variable: &str,
) -> Option<&'a serde_json::Map<String, Value>> {
    row.get(variable).and_then(|value| match value {
        Value::Object(props) => Some(props),
        _ => None,
    })
}

fn bound_row_object_props(row: &Row, variable: &str) -> Option<HashMap<String, Value>> {
    pipeline_bound_node(row, variable).map(|props| props.clone().into_iter().collect())
}

fn bound_node_matches_row(
    row: &Row,
    variable: Option<&str>,
    props: &HashMap<String, Value>,
) -> bool {
    let Some(variable) = variable else {
        return true;
    };
    let Some(bound_props) = bound_row_object_props(row, variable) else {
        return true;
    };

    match (node_id(&bound_props), node_id(props)) {
        (Some(bound_id), Some(actual_id)) => bound_id == actual_id,
        _ => false,
    }
}

fn node_record_to_props(node: &NodeRecord) -> HashMap<String, Value> {
    let mut props: HashMap<String, Value> = node
        .properties
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    props.insert("_id".to_string(), Value::String(node.id.clone()));
    props.insert(
        "_labels".to_string(),
        Value::Array(
            node.labels
                .iter()
                .map(|label| Value::String(label.clone()))
                .collect(),
        ),
    );
    props
}

fn node_record_from_props(props: &HashMap<String, Value>) -> Result<NodeRecord, EvalError> {
    let id = node_id(props)
        .ok_or_else(|| EvalError::ExecutionError("node is missing _id metadata".to_string()))?;
    let labels = props
        .get("_labels")
        .and_then(Value::as_array)
        .ok_or_else(|| EvalError::ExecutionError("node is missing _labels metadata".to_string()))?
        .iter()
        .map(|label| {
            label.as_str().map(str::to_string).ok_or_else(|| {
                EvalError::ExecutionError(
                    "node _labels metadata must be a string array".to_string(),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let properties = props
        .iter()
        .filter(|(key, _)| key.as_str() != "_id" && key.as_str() != "_labels")
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();

    Ok(NodeRecord {
        id: id.to_string(),
        labels,
        properties,
        named_embeddings: BTreeMap::new(),
        chunk_embeddings: Vec::new(),
        embed_meta: Default::default(),
        created_at_unix_ms: 0,
        updated_at_unix_ms: 0,
    })
}

fn edge_record_from_props(props: &HashMap<String, Value>) -> Result<EdgeRecord, EvalError> {
    let id = props.get("_id").and_then(Value::as_str).ok_or_else(|| {
        EvalError::ExecutionError("relationship is missing _id metadata".to_string())
    })?;
    let start_node = props.get("_start").and_then(Value::as_str).ok_or_else(|| {
        EvalError::ExecutionError("relationship is missing _start metadata".to_string())
    })?;
    let end_node = props.get("_end").and_then(Value::as_str).ok_or_else(|| {
        EvalError::ExecutionError("relationship is missing _end metadata".to_string())
    })?;
    let edge_type = props.get("_type").and_then(Value::as_str).ok_or_else(|| {
        EvalError::ExecutionError("relationship is missing _type metadata".to_string())
    })?;

    let properties = props
        .iter()
        .filter(|(key, _)| !matches!(key.as_str(), "_id" | "_start" | "_end" | "_type"))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();

    Ok(EdgeRecord {
        id: id.to_string(),
        start_node: start_node.to_string(),
        end_node: end_node.to_string(),
        edge_type: edge_type.to_string(),
        properties,
        created_at_unix_ms: 0,
        updated_at_unix_ms: 0,
    })
}

fn bound_edge_matches_row(row: &Row, variable: Option<&str>, edge: &EdgeRecord) -> bool {
    let Some(variable) = variable else {
        return true;
    };
    let Some(bound_props) = bound_row_object_props(row, variable) else {
        return true;
    };

    bound_props
        .get("_id")
        .and_then(Value::as_str)
        .map(|bound_id| bound_id == edge.id)
        .unwrap_or(false)
}

fn related_node_id<'a>(
    start_id: &str,
    edge: &'a EdgeRecord,
    direction: &EdgeDirection,
) -> Option<&'a str> {
    match direction {
        EdgeDirection::Outgoing if edge.start_node == start_id => Some(edge.end_node.as_str()),
        EdgeDirection::Incoming if edge.end_node == start_id => Some(edge.start_node.as_str()),
        EdgeDirection::Both if edge.start_node == start_id => Some(edge.end_node.as_str()),
        EdgeDirection::Both if edge.end_node == start_id => Some(edge.start_node.as_str()),
        _ => None,
    }
}

fn bind_optional_pattern_nulls(row: &mut Row, pattern: &Pattern) {
    for node in &pattern.nodes {
        if let Some(var) = &node.variable {
            row.entry(var.clone()).or_insert(Value::Null);
        }
    }
    for edge in &pattern.edges {
        if let Some(var) = &edge.variable {
            row.entry(var.clone()).or_insert(Value::Null);
        }
    }
    if let Some(path_var) = &pattern.path_variable {
        row.entry(path_var.clone()).or_insert(Value::Null);
    }
}

fn bind_single_node_path_variable(row: &mut Row, pattern: &Pattern, node_value: Value) {
    if pattern.edges.is_empty() && pattern.nodes.len() == 1 {
        if let Some(path_var) = &pattern.path_variable {
            row.insert(path_var.clone(), path_value(vec![node_value], Vec::new()));
        }
    }
}

fn path_value(node_values: Vec<Value>, edge_values: Vec<Value>) -> Value {
    Value::Object(
        [
            ("nodes".to_string(), Value::Array(node_values)),
            (
                "relationships".to_string(),
                Value::Array(edge_values.clone()),
            ),
            ("length".to_string(), Value::from(edge_values.len() as i64)),
        ]
        .into_iter()
        .collect(),
    )
}

fn edge_record_to_value(edge: &EdgeRecord) -> Result<Value, EvalError> {
    let mut props = serde_json::Map::new();
    for (key, value) in &edge.properties {
        props.insert(key.clone(), value.clone());
    }
    props.insert("_id".to_string(), Value::String(edge.id.clone()));
    props.insert("_type".to_string(), Value::String(edge.edge_type.clone()));
    props.insert("_start".to_string(), Value::String(edge.start_node.clone()));
    props.insert("_end".to_string(), Value::String(edge.end_node.clone()));
    Ok(Value::Object(props))
}

fn options_to_btreemap(options: &HashMap<String, Value>) -> BTreeMap<String, Value> {
    options
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<BTreeMap<_, _>>()
}

fn binding_scope(binding: &DecayProfileBindingSchema) -> &'static str {
    if binding.is_edge {
        "EDGE"
    } else {
        "NODE"
    }
}

fn binding_target(binding: &DecayProfileBindingSchema) -> String {
    if binding.is_wildcard {
        return "*".to_string();
    }
    if binding.is_edge {
        return binding.target_edge_type.clone().unwrap_or_default();
    }
    binding.target_labels.join(":")
}

fn option_string(
    options: &HashMap<String, Value>,
    key: &str,
    default: &str,
) -> Result<String, EvalError> {
    match options.get(key) {
        Some(v) => v
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| EvalError::ExecutionError(format!("{} must be a string", key))),
        None => Ok(default.to_string()),
    }
}

fn option_bool(
    options: &HashMap<String, Value>,
    key: &str,
    default: bool,
) -> Result<bool, EvalError> {
    match options.get(key) {
        Some(v) => v
            .as_bool()
            .ok_or_else(|| EvalError::ExecutionError(format!("{} must be a boolean", key))),
        None => Ok(default),
    }
}

fn option_f64(options: &HashMap<String, Value>, key: &str, default: f64) -> Result<f64, EvalError> {
    match options.get(key) {
        Some(v) => v
            .as_f64()
            .ok_or_else(|| EvalError::ExecutionError(format!("{} must be a number", key))),
        None => Ok(default),
    }
}

fn option_i64(options: &HashMap<String, Value>, key: &str, default: i64) -> Result<i64, EvalError> {
    match options.get(key) {
        Some(v) => v
            .as_i64()
            .ok_or_else(|| EvalError::ExecutionError(format!("{} must be an integer", key))),
        None => Ok(default),
    }
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
        Expression::Literal(v) => literal_name(v),
        Expression::ListLiteral(_) => "list".to_string(),
        Expression::MapLiteral(_) => "map".to_string(),
        Expression::InList { .. } => "expr".to_string(),
        _ => "expr".to_string(),
    }
}

fn literal_name(value: &LiteralValue) -> String {
    match value {
        LiteralValue::String(value) => value.clone(),
        LiteralValue::Integer(value) => value.to_string(),
        LiteralValue::Float(value) => value.to_string(),
        LiteralValue::Bool(value) => value.to_string(),
        LiteralValue::Null => "null".to_string(),
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

fn return_clause(query: &Query) -> Result<&copperdb_cypher::ReturnClause, EvalError> {
    query
        .clauses
        .iter()
        .find_map(|clause| match clause {
            Clause::Return(ret) => Some(ret),
            _ => None,
        })
        .ok_or_else(|| EvalError::ExecutionError("query requires a RETURN clause".into()))
}

/// Build a map from RETURN/WITH alias to the underlying expression.
fn return_alias_map(items: &[ReturnItem]) -> HashMap<String, Expression> {
    items
        .iter()
        .filter_map(|item| {
            item.alias
                .as_ref()
                .map(|alias| (alias.clone(), item.expression.clone()))
        })
        .collect()
}

/// Resolve an ORDER BY expression through RETURN/WITH aliases.
///
/// If the expression references a RETURN alias (e.g., `ORDER BY title`
/// where `title` is `s.title AS title`), rewrite it to the underlying
/// expression so it can be evaluated against the pre-projection row.
fn resolve_order_expression(
    expr: &Expression,
    alias_map: &HashMap<String, Expression>,
) -> Expression {
    match expr {
        Expression::Variable(name) => {
            alias_map.get(name).cloned().unwrap_or_else(|| expr.clone())
        }
        _ => expr.clone(),
    }
}

fn sort_rows_by_return_order(rows: &mut [Row], ret: &copperdb_cypher::ReturnClause) {
    let alias_map = return_alias_map(&ret.items);
    rows.sort_by(|left, right| {
        for item in &ret.order_by {
            let resolved = resolve_order_expression(&item.expression, &alias_map);
            let left_key = optimized_order_key(left, &resolved);
            let right_key = optimized_order_key(right, &resolved);
            let ord = compare_json(&left_key, &right_key);
            if ord != std::cmp::Ordering::Equal {
                return if item.descending { ord.reverse() } else { ord };
            }
        }
        std::cmp::Ordering::Equal
    });
}

fn apply_return_window(rows: &mut Vec<Row>, ret: &copperdb_cypher::ReturnClause) {
    if let Some(skip) = ret.skip {
        *rows = rows.drain(..).skip(skip.max(0) as usize).collect();
    }
    if let Some(limit) = ret.limit {
        rows.truncate(limit.max(0) as usize);
    }
    if ret.distinct {
        let mut seen = HashSet::new();
        rows.retain(|row| seen.insert(row_key(row)));
    }
}

fn sort_rows_by_with_order(rows: &mut [Row], with_clause: &WithClause) {
    let alias_map = return_alias_map(&with_clause.items);
    rows.sort_by(|left, right| {
        for item in &with_clause.order_by {
            let resolved = resolve_order_expression(&item.expression, &alias_map);
            let left_key = optimized_order_key(left, &resolved);
            let right_key = optimized_order_key(right, &resolved);
            let ord = compare_json(&left_key, &right_key);
            if ord != std::cmp::Ordering::Equal {
                return if item.descending { ord.reverse() } else { ord };
            }
        }
        std::cmp::Ordering::Equal
    });
}

fn apply_with_window(rows: &mut Vec<Row>, with_clause: &WithClause) {
    if let Some(skip) = with_clause.skip {
        *rows = rows.drain(..).skip(skip.max(0) as usize).collect();
    }
    if let Some(limit) = with_clause.limit {
        rows.truncate(limit.max(0) as usize);
    }
}

fn first_count_column_name(items: &[ReturnItem]) -> Option<String> {
    items.iter().find_map(|item| match &item.expression {
        Expression::FunctionCall { name, .. } if name.eq_ignore_ascii_case("count") => {
            Some(column_name(item))
        }
        _ => None,
    })
}

fn capture_value(captures: &copperdb_cypher::ShapeCaptures, name: &str) -> Option<String> {
    match captures.by_name.get(name) {
        Some(ShapeValue::String(value)) if !value.is_empty() => Some(value.clone()),
        _ => None,
    }
}

fn capture_json_value(captures: &copperdb_cypher::ShapeCaptures, name: &str) -> Option<Value> {
    match captures.by_name.get(name) {
        Some(ShapeValue::String(value)) if !value.is_empty() => {
            let trimmed = value.trim();
            if let Some(stripped) = trimmed
                .strip_prefix('\'')
                .and_then(|inner| inner.strip_suffix('\''))
            {
                Some(Value::String(stripped.to_string()))
            } else if let Ok(number) = trimmed.parse::<i64>() {
                Some(Value::from(number))
            } else if let Ok(number) = trimmed.parse::<u64>() {
                Some(Value::from(number))
            } else if let Ok(number) = trimmed.parse::<f64>() {
                Some(Value::from(number))
            } else if trimmed.eq_ignore_ascii_case("true") {
                Some(Value::Bool(true))
            } else if trimmed.eq_ignore_ascii_case("false") {
                Some(Value::Bool(false))
            } else if trimmed.eq_ignore_ascii_case("null") {
                Some(Value::Null)
            } else {
                Some(Value::String(trimmed.to_string()))
            }
        }
        Some(ShapeValue::Int(value)) => Some(Value::from(*value)),
        _ => None,
    }
}

#[derive(Debug, Default, Clone)]
struct EdgeAggStats {
    sum: f64,
    count: i64,
    min: Option<f64>,
    max: Option<f64>,
}

fn json_number_as_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|number| number as f64))
        .or_else(|| value.as_u64().map(|number| number as f64))
}

fn optimized_order_key(row: &Row, expression: &Expression) -> Value {
    match expression {
        Expression::Variable(variable) => row.get(variable).cloned().unwrap_or(Value::Null),
        Expression::PropertyAccess { variable, property } => {
            let dot_key = format!("{variable}.{property}");
            if let Some(v) = row.get(&dot_key) {
                return v.clone();
            }
            if let Some(Value::Object(map)) = row.get(variable.as_str()) {
                return map.get(property.as_str()).cloned().unwrap_or(Value::Null);
            }
            Value::Null
        }
        Expression::FunctionCall { name, args, .. } => {
            let key = if let Some(arg) = args.first() {
                format!("{}({})", name, expression_name(arg))
            } else {
                name.clone()
            };
            row.get(&key).cloned().unwrap_or(Value::Null)
        }
        _ => Value::Null,
    }
}

fn row_agg_value(row: &Row, func_name: &str, agg_property: &str) -> Value {
    row.get(&format!("{}(r.{})", func_name, agg_property))
        .cloned()
        .or_else(|| {
            row.iter()
                .find(|(key, _)| key.starts_with(func_name))
                .map(|(_, value)| value.clone())
        })
        .unwrap_or(Value::Null)
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
    include!("tests.rs");
}
