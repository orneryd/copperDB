//! Cypher query evaluator for copperdb.
//!
//! Executes Cypher ASTs from `copperdb-cypher` against the storage engine.

use copperdb_cypher::{
    Clause, ConstraintKind, EdgeDirection, EdgePattern, Expression, NodePattern, Pattern,
    PatternInfo, PipelineClause, PipelineClauseKind, PropertyEntry, Query, QueryPattern,
    ReturnItem, ShapeKind, ShapeMatch, ShapeValue, LiteralValue,
};
use copperdb_filter::{eval_expression, eval_predicate};
use copperdb_indexing::{IndexCatalog, IndexError};
use copperdb_knowledgepolicy::{
    access_metadata_after_policy_access,
    build_binding_table, build_bundles_by_name, build_decay_bindings,
    build_promotion_policies_by_name, build_promotion_profiles_by_name, merge_access_metadata,
    score_binding, AccessFlusher, AccessMutationBuffer, CompiledBinding,
    PromotionProfileDef, Resolver, ScoreFromMode,
};
use copperdb_storage::{
    Constraint, ConstraintEntityType, ConstraintType, DecayProfileBindingSchema,
    DecayProfileSchema, EdgeRecord, KnowledgePolicyAccessMetadata, NodeRecord,
    PromotionOnAccessMutationKindSchema, PromotionOnAccessMutationSchema,
    PromotionPolicySchema, PromotionProfileSchema, PromotionWhenClauseSchema, StorageEngine,
};
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
            self.target_edge_type.map(Value::String).unwrap_or(Value::Null),
        );
        row.insert(
            "decayBinding".to_string(),
            self.decay_binding.map(Value::String).unwrap_or(Value::Null),
        );
        row.insert(
            "promotionPolicy".to_string(),
            self.promotion_policy.map(Value::String).unwrap_or(Value::Null),
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
}

impl EvalEngine {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            storage,
            node_lookup_cache: Arc::new(Mutex::new(HashMap::new())),
            access_flusher: Arc::new(AccessFlusher::new()),
        }
    }

    fn with_access_buffer<T, F>(&self, operation: F) -> Result<T, EvalError>
    where
        F: FnOnce() -> Result<T, EvalError>,
    {
        self.access_flusher
            .with_buffer(operation, |buffer| self.flush_access_mutation_buffer(buffer))
    }

    fn flush_access_mutation_buffer(
        &self,
        buffer: AccessMutationBuffer,
    ) -> Result<(), EvalError> {
        for (entity_id, pending) in buffer {
            if pending.access_count_delta == 0 && pending.last_accessed_at_unix_ms.is_none() {
                continue;
            }
            let mut metadata = self
                .storage
                .get_knowledge_policy_access_metadata(&entity_id)?
                .unwrap_or_default();
            if let Some(last_accessed_at_unix_ms) = pending.last_accessed_at_unix_ms {
                metadata.last_accessed_at_unix_ms = Some(
                    metadata
                        .last_accessed_at_unix_ms
                        .map(|current| current.max(last_accessed_at_unix_ms))
                        .unwrap_or(last_accessed_at_unix_ms),
                );
            }
            metadata.access_count = metadata
                .access_count
                .saturating_add(pending.access_count_delta);
            self.storage
                .put_knowledge_policy_access_metadata(&entity_id, &metadata)?;
        }
        Ok(())
    }

    pub fn knowledge_policy_resolver(&self) -> Result<Resolver, EvalError> {
        let bundles = build_bundles_by_name(&self.storage.load_decay_profile_schemas()?)
            .map_err(|error| EvalError::ExecutionError(error.to_string()))?;
        let bindings = build_decay_bindings(&self.storage.load_decay_profile_binding_schemas()?);
        let promotion_profiles =
            build_promotion_profiles_by_name(&self.storage.load_promotion_profile_schemas()?)
                .map_err(|error| EvalError::ExecutionError(error.to_string()))?;
        let promotion_policies =
            build_promotion_policies_by_name(&self.storage.load_promotion_policy_schemas()?);
        let binding_table = build_binding_table(
            &bundles,
            &bindings,
            &promotion_profiles,
            &promotion_policies,
        )
        .map_err(|error| EvalError::ExecutionError(error.to_string()))?;
        Ok(Resolver::new(binding_table))
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
            if let Ok(Some(live_props)) = self.node_props_by_id(id) {
                let all_props_match = props
                    .iter()
                    .all(|(k, v)| live_props.get(k).map(|pv| pv == v).unwrap_or(false));
                if all_props_match {
                    return Some(cached);
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
        self.with_access_buffer(|| self.execute_inner(query, params))
    }

    fn execute_inner(
        &self,
        query: &Query,
        params: &HashMap<String, Value>,
    ) -> Result<EvalResult, EvalError> {
        let mut current_rows = pooled_binding_rows();
        current_rows.push(HashMap::new());
        let mut stats = QueryStats::default();
        let mut columns: Vec<String> = vec![];
        let mut result_rows: Vec<Row> = vec![];

        for clause in &query.clauses {
            match clause {
                Clause::Call(call) => {
                    return self.execute_call_clause(call, params);
                }

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
                    let catalog = IndexCatalog::new(self.storage.as_ref());
                    let definition = copperdb_indexing::CatalogIndexDefinition {
                        name: create.name.clone(),
                        entity_type: copperdb_indexing::CatalogIndexEntityType::Node,
                        label: create.label.clone(),
                        properties: create.properties.clone(),
                    };
                    if create.if_not_exists {
                        catalog.create_if_absent(definition)?;
                    } else {
                        catalog.create(definition)?;
                    }
                }

                Clause::DropIndex(drop) => {
                    let catalog = IndexCatalog::new(self.storage.as_ref());
                    if drop.if_exists {
                        catalog.drop_if_present(&drop.name)?;
                    } else {
                        catalog.drop(&drop.name)?;
                    }
                }

                Clause::ShowIndexes(_) => {
                    let indexes = IndexCatalog::new(self.storage.as_ref()).list()?;
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
                                        copperdb_indexing::CatalogIndexEntityType::Node => "NODE",
                                        copperdb_indexing::CatalogIndexEntityType::Relationship => {
                                            "RELATIONSHIP"
                                        }
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

                Clause::CreateDecayProfile(create) => {
                    if let Some(target) = &create.target {
                        let binding = DecayProfileBindingSchema {
                            name: create.name.clone(),
                            target_labels: target.target_labels.clone(),
                            target_edge_type: target.target_edge_type.clone(),
                            is_wildcard: target.is_wildcard,
                            is_edge: target.is_edge,
                            profile_ref: create
                                .options
                                .get("profileRef")
                                .and_then(|v| v.as_str().map(|s| s.to_string())),
                            no_decay: option_bool(&create.options, "noDecay", false)?,
                            visibility_threshold: match create.options.get("visibilityThreshold") {
                                Some(_) => Some(option_f64(
                                    &create.options,
                                    "visibilityThreshold",
                                    0.0,
                                )?),
                                None => None,
                            },
                            order: option_i64(&create.options, "order", 0)?,
                        };
                        self.storage.persist_decay_profile_binding_schema(&binding)?;
                    } else {
                        let profile = DecayProfileSchema {
                            name: create.name.clone(),
                            half_life_seconds: option_i64(&create.options, "halfLifeSeconds", 0)?,
                            visibility_threshold: option_f64(
                                &create.options,
                                "visibilityThreshold",
                                0.0,
                            )?,
                            score_floor: option_f64(&create.options, "scoreFloor", 0.0)?,
                            function: option_string(&create.options, "function", "none")?,
                            scope: option_string(&create.options, "scope", "NODE")?,
                            decay_enabled: option_bool(&create.options, "decayEnabled", true)?,
                            score_from: option_string(&create.options, "scoreFrom", "CREATED")?,
                            score_from_property: create
                                .options
                                .get("scoreFromProperty")
                                .and_then(|v| v.as_str().map(|s| s.to_string())),
                            enabled: option_bool(&create.options, "enabled", true)?,
                        };
                        self.storage.persist_decay_profile_schema(&profile)?;
                    }
                }

                Clause::AlterDecayProfile(alter) => {
                    let updates = options_to_btreemap(&alter.options);
                    self.storage
                        .alter_decay_profile_schema(&alter.name, &updates)?;
                }

                Clause::DropDecayProfile(drop) => {
                    if self
                        .storage
                        .load_decay_profile_binding_schemas()?
                        .iter()
                        .any(|binding| binding.name == drop.name)
                    {
                        self.storage
                            .delete_decay_profile_binding_schema(&drop.name, drop.if_exists)?;
                    } else {
                        self.storage
                            .delete_decay_profile_schema(&drop.name, drop.if_exists)?;
                    }
                }

                Clause::ShowDecayProfiles(_) => {
                    let profiles = self.storage.load_decay_profile_schemas()?;
                    let bindings = self.storage.load_decay_profile_binding_schemas()?;
                    columns = vec![
                        "kind".to_string(),
                        "name".to_string(),
                        "scope".to_string(),
                        "target".to_string(),
                        "profileRef".to_string(),
                        "enabled".to_string(),
                    ];
                    result_rows = profiles.into_iter().map(|p| {
                            let mut row = Row::new();
                            row.insert("kind".to_string(), Value::String("bundle".to_string()));
                            row.insert("name".to_string(), Value::String(p.name));
                            row.insert("scope".to_string(), Value::String(p.scope));
                            row.insert(
                                "target".to_string(),
                                Value::String(String::new()),
                            );
                            row.insert("profileRef".to_string(), Value::Null);
                            row.insert("enabled".to_string(), Value::Bool(p.enabled));
                            row
                        })
                        .chain(bindings.into_iter().map(|binding| {
                            let scope = binding_scope(&binding).to_string();
                            let target = binding_target(&binding);
                            let profile_ref = binding.profile_ref.clone();
                            let mut row = Row::new();
                            row.insert("kind".to_string(), Value::String("binding".to_string()));
                            row.insert("name".to_string(), Value::String(binding.name));
                            row.insert("scope".to_string(), Value::String(scope));
                            row.insert("target".to_string(), Value::String(target));
                            row.insert(
                                "profileRef".to_string(),
                                profile_ref
                                    .map(Value::String)
                                    .unwrap_or(Value::Null),
                            );
                            row.insert("enabled".to_string(), Value::Bool(true));
                            row
                        }))
                        .collect();
                }

                Clause::CreatePromotionProfile(create) => {
                    let profile = PromotionProfileSchema {
                        name: create.name.clone(),
                        scope: option_string(&create.options, "scope", "NODE")?,
                        multiplier: option_f64(&create.options, "multiplier", 1.0)?,
                        score_floor: option_f64(&create.options, "scoreFloor", 0.0)?,
                        score_cap: option_f64(&create.options, "scoreCap", 1.0)?,
                        enabled: option_bool(&create.options, "enabled", true)?,
                    };
                    self.storage.persist_promotion_profile_schema(&profile)?;
                }

                Clause::AlterPromotionProfile(alter) => {
                    let updates = options_to_btreemap(&alter.options);
                    self.storage
                        .alter_promotion_profile_schema(&alter.name, &updates)?;
                }

                Clause::DropPromotionProfile(drop) => {
                    self.storage
                        .delete_promotion_profile_schema(&drop.name, drop.if_exists)?;
                }

                Clause::ShowPromotionProfiles(_) => {
                    let profiles = self.storage.load_promotion_profile_schemas()?;
                    columns = vec![
                        "name".to_string(),
                        "scope".to_string(),
                        "multiplier".to_string(),
                        "scoreFloor".to_string(),
                        "scoreCap".to_string(),
                        "enabled".to_string(),
                    ];
                    result_rows = profiles
                        .into_iter()
                        .map(|p| {
                            let mut row = Row::new();
                            row.insert("name".to_string(), Value::String(p.name));
                            row.insert("scope".to_string(), Value::String(p.scope));
                            row.insert("multiplier".to_string(), Value::from(p.multiplier));
                            row.insert("scoreFloor".to_string(), Value::from(p.score_floor));
                            row.insert("scoreCap".to_string(), Value::from(p.score_cap));
                            row.insert("enabled".to_string(), Value::Bool(p.enabled));
                            row
                        })
                        .collect();
                }

                Clause::CreatePromotionPolicy(create) => {
                    let policy = PromotionPolicySchema {
                        name: create.name.clone(),
                        target_labels: create.target.target_labels.clone(),
                        target_edge_type: create.target.target_edge_type.clone(),
                        is_wildcard: create.target.is_wildcard,
                        is_edge: create.target.is_edge,
                        enabled: create.enabled,
                        on_access_mutations: create
                            .on_access_mutations
                            .iter()
                            .map(|mutation| PromotionOnAccessMutationSchema {
                                kind: match mutation.kind {
                                    copperdb_cypher::PromotionOnAccessMutationKind::SetLastAccessedNow => {
                                        PromotionOnAccessMutationKindSchema::SetLastAccessedNow
                                    }
                                    copperdb_cypher::PromotionOnAccessMutationKind::IncrementAccessCount => {
                                        PromotionOnAccessMutationKindSchema::IncrementAccessCount
                                    }
                                },
                            })
                            .collect(),
                        when_clauses: create
                            .when_clauses
                            .iter()
                            .map(|clause| PromotionWhenClauseSchema {
                                profile_ref: clause.profile_ref.clone(),
                                predicate: clause.predicate.clone(),
                                order: clause.order,
                            })
                            .collect(),
                    };
                    self.storage.persist_promotion_policy_schema(&policy)?;
                }

                Clause::AlterPromotionPolicy(alter) => {
                    let updates =
                        BTreeMap::from([("enabled".to_string(), Value::Bool(alter.enabled))]);
                    self.storage
                        .alter_promotion_policy_schema(&alter.name, &updates)?;
                }

                Clause::DropPromotionPolicy(drop) => {
                    self.storage
                        .delete_promotion_policy_schema(&drop.name, drop.if_exists)?;
                }

                Clause::ShowPromotionPolicies(_) => {
                    let policies = self.storage.load_promotion_policy_schemas()?;
                    columns = vec![
                        "name".to_string(),
                        "targetLabels".to_string(),
                        "targetEdgeType".to_string(),
                        "isWildcard".to_string(),
                        "isEdge".to_string(),
                        "enabled".to_string(),
                        "onAccessMutations".to_string(),
                        "whenClauses".to_string(),
                    ];
                    result_rows = policies
                        .into_iter()
                        .map(|p| {
                            let mut row = Row::new();
                            row.insert("name".to_string(), Value::String(p.name));
                            row.insert(
                                "targetLabels".to_string(),
                                Value::Array(
                                    p.target_labels
                                        .into_iter()
                                        .map(Value::String)
                                        .collect::<Vec<_>>(),
                                ),
                            );
                            row.insert(
                                "targetEdgeType".to_string(),
                                p.target_edge_type.map(Value::String).unwrap_or(Value::Null),
                            );
                            row.insert("isWildcard".to_string(), Value::Bool(p.is_wildcard));
                            row.insert("isEdge".to_string(), Value::Bool(p.is_edge));
                            row.insert("enabled".to_string(), Value::Bool(p.enabled));
                            row.insert(
                                "onAccessMutations".to_string(),
                                Value::Array(
                                    p.on_access_mutations
                                        .into_iter()
                                        .map(|mutation| Value::String(match mutation.kind {
                                            PromotionOnAccessMutationKindSchema::SetLastAccessedNow => {
                                                "SET_LAST_ACCESSED_NOW".to_string()
                                            }
                                            PromotionOnAccessMutationKindSchema::IncrementAccessCount => {
                                                "INCREMENT_ACCESS_COUNT".to_string()
                                            }
                                        }))
                                        .collect(),
                                ),
                            );
                            row.insert(
                                "whenClauses".to_string(),
                                Value::Array(
                                    p.when_clauses
                                        .into_iter()
                                        .map(|w| {
                                            serde_json::json!({
                                                "profileRef": w.profile_ref,
                                                "predicate": w.predicate,
                                                "order": w.order,
                                            })
                                        })
                                        .collect(),
                                ),
                            );
                            row
                        })
                        .collect();
                }

                Clause::Create(create) => {
                    current_rows = self.execute_pipeline_create_clause(
                        &current_rows,
                        create,
                        params,
                        &mut stats,
                    )?;
                }

                Clause::Match(match_clause) => {
                    if !match_clause.pattern.edges.is_empty() {
                        current_rows = self.match_relationship_pattern(
                            &current_rows,
                            &match_clause.pattern,
                            params,
                        )?;
                        continue;
                    }

                    // Iteratively cross-join each node pattern so that bindings from
                    // earlier patterns are visible when processing later ones.
                    for node_pat in &match_clause.pattern.nodes {
                        let mut new_rows = pooled_binding_rows();
                        for base_row in &current_rows {
                            for props in self.matching_node_props(node_pat, base_row, params)? {
                                let node_val = serde_json::to_value(&props)
                                    .map_err(|e| EvalError::SerializationError(e.to_string()))?;

                                let mut row = base_row.clone();
                                if let Some(var) = &node_pat.variable {
                                    row.insert(var.clone(), node_val.clone());
                                }
                                bind_single_node_path_variable(
                                    &mut row,
                                    &match_clause.pattern,
                                    node_val,
                                );
                                new_rows.push(row);
                            }
                        }
                        replace_binding_rows(&mut current_rows, new_rows);
                    }
                }

                Clause::OptionalMatch(match_clause) => {
                    if !match_clause.pattern.edges.is_empty() {
                        let mut optional_rows = pooled_binding_rows();
                        for base_row in &current_rows {
                            let matched = self.match_relationship_pattern(
                                std::slice::from_ref(base_row),
                                &match_clause.pattern,
                                params,
                            )?;
                            if matched.is_empty() {
                                let mut row = base_row.clone();
                                bind_optional_pattern_nulls(&mut row, &match_clause.pattern);
                                optional_rows.push(row);
                            } else {
                                optional_rows.extend(matched);
                            }
                        }
                        replace_binding_rows(&mut current_rows, optional_rows);
                        continue;
                    }

                    // Iteratively cross-join each node pattern, preserving rows
                    // with null bindings when no matching node exists.
                    for node_pat in &match_clause.pattern.nodes {
                        let mut new_rows = pooled_binding_rows();
                        let mut found_any = false;
                        for base_row in &current_rows {
                            for props in self.matching_node_props(node_pat, base_row, params)? {
                                let node_val = serde_json::to_value(&props)
                                    .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                                let mut row = base_row.clone();
                                if let Some(var) = &node_pat.variable {
                                    row.insert(var.clone(), node_val.clone());
                                }
                                bind_single_node_path_variable(
                                    &mut row,
                                    &match_clause.pattern,
                                    node_val,
                                );
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
                                bind_optional_pattern_nulls(&mut row, &match_clause.pattern);
                                new_rows.push(row);
                            }
                        }
                        replace_binding_rows(&mut current_rows, new_rows);
                    }
                }

                Clause::Where(where_clause) => {
                    let expr = &where_clause.expression;
                    let mut filtered = pooled_binding_rows();
                    let mut old_rows = std::mem::take(&mut current_rows);
                    for row in old_rows.drain(..) {
                        match eval_predicate(expr, &row, params) {
                            Ok(true) => filtered.push(row),
                            Ok(false) => {}
                            Err(e) => return Err(EvalError::FilterError(e.to_string())),
                        }
                    }
                    recycle_binding_rows(old_rows);
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
                    let mut remaining_rows = pooled_binding_rows();
                    for row in &current_rows {
                        for var in &vars_to_delete {
                            if let Some(Value::Object(props)) = row.get(var) {
                                if let Some(Value::String(id)) = props.get("_id") {
                                    self.storage.delete_node_record(id)?;
                                    stats.nodes_deleted += 1;
                                }
                            }
                        }
                        remaining_rows.push(row.clone());
                    }
                    replace_binding_rows(&mut current_rows, remaining_rows);
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
                                if let Some(Value::String(_)) = props.get("_id") {
                                    let new_props: HashMap<String, Value> =
                                        props.clone().into_iter().collect();
                                    self.persist_node_props(&new_props)?;
                                }
                            }
                        }
                    }
                }

                Clause::With(with) => {
                    // Project rows like RETURN but continue pipeline
                    let items = &with.items;
                    let mut projected: Vec<Row> = current_rows
                        .iter()
                        .map(|row| project_row(row, items, params))
                        .collect::<Result<Vec<_>, _>>()?;

                    if let Some(limit) = with.limit {
                        projected.truncate(limit.max(0) as usize);
                    }

                    if let Some(where_clause) = &with.where_clause {
                        let mut filtered_rows = pooled_binding_rows();
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
                    let mut new_rows = pooled_binding_rows();
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
                    replace_binding_rows(&mut current_rows, new_rows);
                }

                Clause::Merge(merge) => {
                    current_rows =
                        self.execute_merge_clause(&current_rows, merge, params, &mut stats)?;
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

    pub fn execute_with_pattern(
        &self,
        query: &Query,
        params: &HashMap<String, Value>,
        pattern_info: &PatternInfo,
    ) -> Result<EvalResult, EvalError> {
        self.execute_with_routes(query, params, pattern_info, None, None)
    }

    pub fn execute_with_routes(
        &self,
        query: &Query,
        params: &HashMap<String, Value>,
        pattern_info: &PatternInfo,
        compound_match: Option<&ShapeMatch>,
        pipeline_clauses: Option<&[PipelineClause]>,
    ) -> Result<EvalResult, EvalError> {
        self.with_access_buffer(|| {
            match pattern_info.pattern {
                QueryPattern::MutualRelationship if self.can_execute_simple_match_return(query) => {
                    return self.execute_mutual_relationship_optimized(query, pattern_info);
                }
                QueryPattern::IncomingCountAgg if self.can_execute_simple_match_return(query) => {
                    return self.execute_count_agg_optimized(query, pattern_info, true);
                }
                QueryPattern::OutgoingCountAgg if self.can_execute_simple_match_return(query) => {
                    return self.execute_count_agg_optimized(query, pattern_info, false);
                }
                QueryPattern::EdgePropertyAgg if self.can_execute_edge_property_agg(query) => {
                    return self.execute_edge_property_agg_optimized(query, pattern_info, params);
                }
                _ => {}
            }

            if let Some(shape_match) = compound_match {
                if self.can_execute_compound_fast_path(query, shape_match) {
                    if let Some(result) = self.execute_compound_fast_path(query, shape_match)? {
                        return Ok(result);
                    }
                }
            }

            if let Some(clauses) = pipeline_clauses {
                if self.can_execute_pipeline_route(query, clauses) {
                    return self.execute_pipeline_routed(query, params, clauses);
                }
            }

            self.execute_inner(query, params)
        })
    }

    fn can_execute_simple_match_return(&self, query: &Query) -> bool {
        query
            .clauses
            .iter()
            .all(|clause| matches!(clause, Clause::Match(_) | Clause::Return(_)))
    }

    fn can_execute_edge_property_agg(&self, query: &Query) -> bool {
        query
            .clauses
            .iter()
            .all(|clause| matches!(clause, Clause::Match(_) | Clause::Return(_)))
    }

    fn execute_mutual_relationship_optimized(
        &self,
        query: &Query,
        pattern_info: &PatternInfo,
    ) -> Result<EvalResult, EvalError> {
        let ret = return_clause(query)?;
        let columns: Vec<String> = ret.items.iter().map(column_name).collect();
        let edges = self.lookup_edges(Some(pattern_info.rel_type.as_str()))?;
        let edge_set: HashSet<(String, String)> = edges
            .iter()
            .map(|edge| (edge.start_node.clone(), edge.end_node.clone()))
            .collect();
        let mut seen_pairs = HashSet::new();
        let mut rows = Vec::new();

        for edge in edges {
            if !edge_set.contains(&(edge.end_node.clone(), edge.start_node.clone())) {
                continue;
            }

            let pair_key = if edge.start_node < edge.end_node {
                format!("{}:{}", edge.start_node, edge.end_node)
            } else {
                format!("{}:{}", edge.end_node, edge.start_node)
            };
            if !seen_pairs.insert(pair_key) {
                continue;
            }

            let Some(start_props) = self.node_props_by_id(&edge.start_node)? else {
                continue;
            };
            let Some(end_props) = self.node_props_by_id(&edge.end_node)? else {
                continue;
            };

            let mut binding_row = Row::new();
            binding_row.insert(
                pattern_info.start_var.clone(),
                Value::Object(start_props.clone().into_iter().collect()),
            );
            binding_row.insert(
                pattern_info.end_var.clone(),
                Value::Object(end_props.clone().into_iter().collect()),
            );
            rows.push(project_row(&binding_row, &ret.items, &HashMap::new())?);
        }

        if !ret.order_by.is_empty() {
            sort_rows_by_return_order(&mut rows, ret);
        }
        apply_return_window(&mut rows, ret);

        Ok(EvalResult {
            columns,
            rows,
            stats: QueryStats::default(),
        })
    }

    fn execute_count_agg_optimized(
        &self,
        query: &Query,
        pattern_info: &PatternInfo,
        incoming: bool,
    ) -> Result<EvalResult, EvalError> {
        let ret = return_clause(query)?;
        let columns: Vec<String> = ret.items.iter().map(column_name).collect();
        let edge_type = (!pattern_info.rel_type.is_empty()).then_some(pattern_info.rel_type.as_str());
        let edges = self.lookup_edges(edge_type)?;
        let mut counts: HashMap<String, i64> = HashMap::new();

        for edge in edges {
            let node_id = if incoming {
                edge.end_node.clone()
            } else {
                edge.start_node.clone()
            };
            *counts.entry(node_id).or_insert(0) += 1;
        }

        let mut rows = Vec::new();
        for (node_id, count) in counts {
            let Some(group_props) = self.node_props_by_id(&node_id)? else {
                continue;
            };
            let mut row = Row::new();
            for item in &ret.items {
                row.insert(
                    column_name(item),
                    self.project_count_agg_item(
                        item,
                        &group_props,
                        count,
                        &pattern_info.start_var,
                        &pattern_info.end_var,
                    )?,
                );
            }
            rows.push(row);
        }

        if !ret.order_by.is_empty() {
            sort_rows_by_return_order(&mut rows, ret);
        } else if let Some(count_column) = first_count_column_name(&ret.items) {
            rows.sort_by(|left, right| {
                compare_json(
                    left.get(&count_column).unwrap_or(&Value::Null),
                    right.get(&count_column).unwrap_or(&Value::Null),
                )
                .reverse()
            });
        }
        apply_return_window(&mut rows, ret);

        Ok(EvalResult {
            columns,
            rows,
            stats: QueryStats::default(),
        })
    }

    fn execute_edge_property_agg_optimized(
        &self,
        query: &Query,
        pattern_info: &PatternInfo,
        _params: &HashMap<String, Value>,
    ) -> Result<EvalResult, EvalError> {
        let ret = query
            .clauses
            .iter()
            .find_map(|clause| match clause {
                Clause::Return(ret) => Some(ret),
                _ => None,
            })
            .ok_or_else(|| {
                EvalError::ExecutionError(
                    "optimized edge-property aggregation requires a RETURN clause".into(),
                )
            })?;

        let edge_type = if pattern_info.rel_type.is_empty() {
            None
        } else {
            Some(pattern_info.rel_type.as_str())
        };
        let edges = self.lookup_edges(edge_type)?;

        let mut stats_by_end: HashMap<String, EdgeAggStats> = HashMap::new();
        for edge in edges {
            let Some(value) = edge.properties.get(&pattern_info.agg_property) else {
                continue;
            };
            let Some(number) = json_number_as_f64(value) else {
                continue;
            };

            let stats = stats_by_end.entry(edge.end_node.clone()).or_default();
            stats.sum += number;
            stats.count += 1;
            stats.min = Some(match stats.min {
                Some(current) => current.min(number),
                None => number,
            });
            stats.max = Some(match stats.max {
                Some(current) => current.max(number),
                None => number,
            });
        }

        let columns: Vec<String> = ret.items.iter().map(column_name).collect();
        let mut rows = Vec::new();
        for (end_node_id, stats) in stats_by_end {
            if stats.count == 0 {
                continue;
            }
            let Some(end_props) = self.node_props_by_id(&end_node_id)? else {
                continue;
            };
            let mut row = Row::new();
            for item in &ret.items {
                let column = column_name(item);
                let value = self.project_edge_agg_item(item, &end_props, &stats)?;
                row.insert(column, value);
            }
            rows.push(row);
        }

        if !ret.order_by.is_empty() {
            rows.sort_by(|left, right| {
                for item in &ret.order_by {
                    let left_key = optimized_order_key(left, &item.expression);
                    let right_key = optimized_order_key(right, &item.expression);
                    let ord = compare_json(&left_key, &right_key);
                    if ord != std::cmp::Ordering::Equal {
                        return if item.descending { ord.reverse() } else { ord };
                    }
                }
                std::cmp::Ordering::Equal
            });
        } else if pattern_info
            .agg_functions
            .iter()
            .any(|name| name.eq_ignore_ascii_case("avg"))
        {
            rows.sort_by(|left, right| {
                let left_avg = row_agg_value(left, "avg", &pattern_info.agg_property);
                let right_avg = row_agg_value(right, "avg", &pattern_info.agg_property);
                compare_json(&left_avg, &right_avg).reverse()
            });
        }

        if let Some(skip) = ret.skip {
            rows = rows.into_iter().skip(skip.max(0) as usize).collect();
        }
        if let Some(limit) = ret.limit {
            rows.truncate(limit.max(0) as usize);
        }
        if ret.distinct {
            let mut seen = std::collections::HashSet::new();
            rows.retain(|row| seen.insert(row_key(row)));
        }

        Ok(EvalResult {
            columns,
            rows,
            stats: QueryStats::default(),
        })
    }

    fn project_edge_agg_item(
        &self,
        item: &ReturnItem,
        end_props: &HashMap<String, Value>,
        stats: &EdgeAggStats,
    ) -> Result<Value, EvalError> {
        match &item.expression {
            Expression::PropertyAccess { property, .. } if property == "name" => {
                end_props.get("name").cloned().ok_or_else(|| {
                    EvalError::ExecutionError(
                        "optimized edge aggregation requires end-node name".into(),
                    )
                })
            }
            Expression::FunctionCall { name, .. } => match name.to_ascii_lowercase().as_str() {
                "count" => Ok(Value::from(stats.count)),
                "sum" => Ok(Value::from(stats.sum)),
                "avg" => Ok(Value::from(stats.sum / stats.count as f64)),
                "min" => Ok(Value::from(stats.min.unwrap_or(0.0))),
                "max" => Ok(Value::from(stats.max.unwrap_or(0.0))),
                other => Err(EvalError::ExecutionError(format!(
                    "optimized edge aggregation does not support function '{}' yet",
                    other
                ))),
            },
            other => Err(EvalError::ExecutionError(format!(
                "optimized edge aggregation does not support return expression {:?}",
                other
            ))),
        }
    }

    fn project_count_agg_item(
        &self,
        item: &ReturnItem,
        group_props: &HashMap<String, Value>,
        count: i64,
        group_var: &str,
        counted_var: &str,
    ) -> Result<Value, EvalError> {
        match &item.expression {
            Expression::Variable(variable) if variable == group_var => {
                Ok(Value::Object(group_props.clone().into_iter().collect()))
            }
            Expression::PropertyAccess { variable, property } if variable == group_var => {
                group_props.get(property).cloned().ok_or_else(|| {
                    EvalError::ExecutionError(format!(
                        "missing property '{}.{}'",
                        variable, property
                    ))
                })
            }
            Expression::FunctionCall { name, args, .. }
                if name.eq_ignore_ascii_case("count")
                    && (args.is_empty()
                        || matches!(&args[0], Expression::Variable(variable) if variable == counted_var || variable == "*")) =>
            {
                Ok(Value::from(count))
            }
            other => Err(EvalError::ExecutionError(format!(
                "optimized count aggregation does not support return expression {:?}",
                other
            ))),
        }
    }

    fn can_execute_compound_fast_path(&self, query: &Query, shape_match: &ShapeMatch) -> bool {
        match shape_match.kind {
            ShapeKind::CompoundCreateDeleteRel => query.clauses.iter().all(|clause| {
                matches!(
                    clause,
                    Clause::Match(_) | Clause::With(_) | Clause::Create(_) | Clause::Delete(_)
                )
            }),
            ShapeKind::CompoundPropCreateDeleteRel => query.clauses.iter().all(|clause| {
                matches!(
                    clause,
                    Clause::Match(_) | Clause::Create(_) | Clause::Delete(_)
                )
            }),
            ShapeKind::CompoundPropCreateDeleteReturnCountRel => {
                query.clauses.iter().all(|clause| {
                    matches!(
                        clause,
                        Clause::Match(_)
                            | Clause::Create(_)
                            | Clause::With(_)
                            | Clause::Delete(_)
                            | Clause::Return(_)
                    )
                })
            }
            ShapeKind::Unknown => false,
        }
    }

    fn execute_compound_fast_path(
        &self,
        query: &Query,
        shape_match: &ShapeMatch,
    ) -> Result<Option<EvalResult>, EvalError> {
        if matches!(shape_match.kind, ShapeKind::CompoundCreateDeleteRel)
            && shape_match.captures.int("limit") == 0
        {
            return Ok(Some(EvalResult {
                columns: Vec::new(),
                rows: Vec::new(),
                stats: QueryStats::default(),
            }));
        }

        let left_exists = self.compound_node_exists(
            &shape_match.captures.string("label1"),
            capture_value(&shape_match.captures, "prop1").as_deref(),
            capture_json_value(&shape_match.captures, "value1").as_ref(),
        )?;
        if !left_exists {
            return Ok(None);
        }

        let right_exists = self.compound_node_exists(
            &shape_match.captures.string("label2"),
            capture_value(&shape_match.captures, "prop2").as_deref(),
            capture_json_value(&shape_match.captures, "value2").as_ref(),
        )?;
        if !right_exists {
            return Ok(None);
        }

        let mut stats = QueryStats::default();
        stats.relationships_created = 1;
        stats.relationships_deleted = 1;

        let (columns, rows) = if matches!(
            shape_match.kind,
            ShapeKind::CompoundPropCreateDeleteReturnCountRel
        ) {
            let ret = return_clause(query)?;
            let mut row = Row::new();
            for item in &ret.items {
                row.insert(column_name(item), Value::from(1));
            }
            (ret.items.iter().map(column_name).collect(), vec![row])
        } else {
            (Vec::new(), Vec::new())
        };

        Ok(Some(EvalResult {
            columns,
            rows,
            stats,
        }))
    }

    fn compound_node_exists(
        &self,
        label: &str,
        prop: Option<&str>,
        value: Option<&Value>,
    ) -> Result<bool, EvalError> {
        if label.is_empty() {
            return Ok(false);
        }

        let prefix = format!("{label}:");
        for item in self.storage.scan_nodes_with_prefix(&prefix) {
            let (_key, raw) = item.map_err(|e| EvalError::StorageError(e.to_string()))?;
            let props: HashMap<String, Value> = rmp_serde::from_slice(&raw)
                .map_err(|e| EvalError::SerializationError(e.to_string()))?;
            match (prop, value) {
                (Some(prop), Some(value)) if props.get(prop) == Some(value) => return Ok(true),
                (Some(_), Some(_)) => continue,
                (None, None) => return Ok(true),
                _ => continue,
            }
        }

        Ok(false)
    }

    fn can_execute_pipeline_route(
        &self,
        query: &Query,
        pipeline_clauses: &[PipelineClause],
    ) -> bool {
        if pipeline_clauses.is_empty() {
            return false;
        }

        let mut query_kinds = Vec::new();
        for clause in &query.clauses {
            match clause {
                Clause::Match(_) => query_kinds.push(PipelineClauseKind::Match),
                Clause::Create(_) => query_kinds.push(PipelineClauseKind::Create),
                Clause::With(_) => query_kinds.push(PipelineClauseKind::With),
                Clause::Unwind(_) => query_kinds.push(PipelineClauseKind::Unwind),
                Clause::Return(_) => query_kinds.push(PipelineClauseKind::Return),
                Clause::Where(_) => {}
                _ => return false,
            }
        }

        query_kinds.len() == pipeline_clauses.len()
            && query_kinds
                .iter()
                .zip(pipeline_clauses)
                .all(|(query_kind, clause)| query_kind == &clause.kind)
    }

    fn execute_pipeline_routed(
        &self,
        query: &Query,
        params: &HashMap<String, Value>,
        _pipeline_clauses: &[PipelineClause],
    ) -> Result<EvalResult, EvalError> {
        let mut current_rows = pooled_binding_rows();
        current_rows.push(Row::new());
        let mut stats = QueryStats::default();

        for clause in &query.clauses {
            match clause {
                Clause::Match(match_clause) => {
                    current_rows = self.execute_pipeline_match_clause(
                        &current_rows,
                        &match_clause.pattern,
                        params,
                    )?;
                }
                Clause::Where(where_clause) => {
                    let mut filtered = pooled_binding_rows();
                    let mut old_rows = std::mem::take(&mut current_rows);
                    for row in old_rows.drain(..) {
                        match eval_predicate(&where_clause.expression, &row, params) {
                            Ok(true) => filtered.push(row),
                            Ok(false) => {}
                            Err(e) => return Err(EvalError::FilterError(e.to_string())),
                        }
                    }
                    recycle_binding_rows(old_rows);
                    current_rows = filtered;
                }
                Clause::Create(create) => {
                    current_rows = self.execute_pipeline_create_clause(
                        &current_rows,
                        create,
                        params,
                        &mut stats,
                    )?;
                }
                Clause::With(with) => {
                    let mut projected: Vec<Row> = current_rows
                        .iter()
                        .map(|row| project_row(row, &with.items, params))
                        .collect::<Result<Vec<_>, _>>()?;

                    if let Some(limit) = with.limit {
                        projected.truncate(limit.max(0) as usize);
                    }

                    if let Some(where_clause) = &with.where_clause {
                        let mut filtered = pooled_binding_rows();
                        for row in projected {
                            if eval_predicate(&where_clause.expression, &row, params)
                                .map_err(|e| EvalError::FilterError(e.to_string()))?
                            {
                                filtered.push(row);
                            }
                        }
                        current_rows = filtered;
                    } else {
                        current_rows = projected;
                    }
                }
                Clause::Unwind(unwind) => {
                    let mut new_rows = pooled_binding_rows();
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
                    replace_binding_rows(&mut current_rows, new_rows);
                }
                Clause::Return(ret) => {
                    let columns: Vec<String> = ret.items.iter().map(column_name).collect();

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

                    if let Some(skip) = ret.skip {
                        let skip = skip.max(0) as usize;
                        current_rows = current_rows.into_iter().skip(skip).collect();
                    }
                    if let Some(limit) = ret.limit {
                        current_rows.truncate(limit.max(0) as usize);
                    }

                    let mut rows: Vec<Row> = current_rows
                        .iter()
                        .map(|row| project_row(row, &ret.items, params))
                        .collect::<Result<Vec<_>, _>>()?;

                    if ret.distinct {
                        let mut seen = HashSet::new();
                        rows.retain(|row| seen.insert(row_key(row)));
                    }

                    return Ok(EvalResult {
                        columns,
                        rows,
                        stats,
                    });
                }
                _ => {
                    return Err(EvalError::ExecutionError(
                        "pipeline route encountered unsupported clause".to_string(),
                    ));
                }
            }
        }

        Ok(EvalResult {
            columns: Vec::new(),
            rows: Vec::new(),
            stats,
        })
    }

    fn execute_pipeline_match_clause(
        &self,
        base_rows: &[Row],
        pattern: &Pattern,
        params: &HashMap<String, Value>,
    ) -> Result<Vec<Row>, EvalError> {
        if !pattern.edges.is_empty() {
            return self.match_relationship_pattern(base_rows, pattern, params);
        }

        let mut current_rows = base_rows.to_vec();
        for node_pat in &pattern.nodes {
            let mut new_rows = pooled_binding_rows();
            for base_row in &current_rows {
                let Some(bound_node) = node_pat
                    .variable
                    .as_ref()
                    .and_then(|variable| pipeline_bound_node(base_row, variable))
                else {
                    for props in self.matching_node_props(node_pat, base_row, params)? {
                        let node_val = serde_json::to_value(&props)
                            .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                        let mut row = base_row.clone();
                        if let Some(var) = &node_pat.variable {
                            row.insert(var.clone(), node_val.clone());
                        }
                        bind_single_node_path_variable(&mut row, pattern, node_val);
                        new_rows.push(row);
                    }
                    continue;
                };

                let bound_node_props: HashMap<String, Value> =
                    bound_node.clone().into_iter().collect();
                let expected_props =
                    evaluate_pattern_properties(&node_pat.properties, base_row, params)?;
                if node_matches_pattern(&bound_node_props, &node_pat.labels, &expected_props) {
                    let mut row = base_row.clone();
                    bind_single_node_path_variable(
                        &mut row,
                        pattern,
                        Value::Object(bound_node.clone()),
                    );
                    new_rows.push(row);
                }
            }
            current_rows = new_rows;
        }

        Ok(current_rows)
    }

    fn execute_pipeline_create_clause(
        &self,
        base_rows: &[Row],
        create: &copperdb_cypher::CreateClause,
        params: &HashMap<String, Value>,
        stats: &mut QueryStats,
    ) -> Result<Vec<Row>, EvalError> {
        self.invalidate_node_lookup_cache();
        let mut output_rows = pooled_binding_rows();

        for base_row in base_rows {
            let mut row = base_row.clone();
            let mut resolved_node_ids = Vec::with_capacity(create.pattern.nodes.len());
            let mut path_node_values = Vec::with_capacity(create.pattern.nodes.len());
            let mut path_edge_values = Vec::with_capacity(create.pattern.edges.len());

            for node_pat in &create.pattern.nodes {
                if let Some((existing_id, existing_value)) =
                    self.resolve_pipeline_node_binding(&row, node_pat, params)?
                {
                    resolved_node_ids.push(existing_id);
                    path_node_values.push(existing_value.clone());
                    if let Some(var) = &node_pat.variable {
                        row.insert(var.clone(), existing_value);
                    }
                    continue;
                }

                let label = node_pat
                    .labels
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "Node".to_string());
                let id = Uuid::new_v4().to_string();
                let key = format!("{label}:{id}");

                let mut props = evaluate_pattern_properties(&node_pat.properties, &row, params)?;
                props.insert("_id".to_string(), Value::String(key.clone()));
                props.insert(
                    "_labels".to_string(),
                    Value::Array(
                        node_pat
                            .labels
                            .iter()
                            .map(|label| Value::String(label.clone()))
                            .collect(),
                    ),
                );
                self.persist_node_props(&props)?;
                stats.nodes_created += 1;
                stats.properties_set += node_pat.properties.len();
                resolved_node_ids.push(key.clone());

                let node_val = serde_json::to_value(&props)
                    .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                path_node_values.push(node_val.clone());

                if let Some(var) = &node_pat.variable {
                    row.insert(var.clone(), node_val);
                }
            }

            for (edge_index, edge_pat) in create.pattern.edges.iter().enumerate() {
                let Some(start_node) = resolved_node_ids.get(edge_index) else {
                    return Err(EvalError::ExecutionError(
                        "pipeline CREATE is missing a start node".to_string(),
                    ));
                };
                let Some(end_node) = resolved_node_ids.get(edge_index + 1) else {
                    return Err(EvalError::ExecutionError(
                        "pipeline CREATE is missing an end node".to_string(),
                    ));
                };

                let rel_type = edge_pat
                    .rel_type
                    .clone()
                    .unwrap_or_else(|| "REL".to_string());
                let id = format!("{}:{}", rel_type, Uuid::new_v4());
                let edge = self.persist_edge_record(EdgeRecord {
                    id: id.clone(),
                    start_node: start_node.clone(),
                    end_node: end_node.clone(),
                    edge_type: rel_type,
                    properties: evaluate_pattern_properties(&edge_pat.properties, &row, params)?
                        .into_iter()
                        .collect(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })?;
                stats.relationships_created += 1;

                let edge_value = edge_record_to_value(&edge)?;
                path_edge_values.push(edge_value.clone());

                if let Some(var) = &edge_pat.variable {
                    row.insert(var.clone(), edge_value);
                }
            }

            if let Some(path_var) = &create.pattern.path_variable {
                row.insert(
                    path_var.clone(),
                    path_value(path_node_values, path_edge_values),
                );
            }

            output_rows.push(row);
        }

        Ok(output_rows)
    }

    fn execute_merge_clause(
        &self,
        base_rows: &[Row],
        merge: &copperdb_cypher::MergeClause,
        params: &HashMap<String, Value>,
        stats: &mut QueryStats,
    ) -> Result<Vec<Row>, EvalError> {
        let mut current_rows = base_rows.to_vec();

        for node_pat in &merge.pattern.nodes {
            let labels = &node_pat.labels;
            let label = labels
                .first()
                .cloned()
                .unwrap_or_else(|| "Node".to_string());
            let mut next_rows = pooled_binding_rows();

            for base_row in &current_rows {
                let merge_props =
                    evaluate_pattern_properties(&node_pat.properties, base_row, params)?;

                let node_val =
                    if let Some(cached_val) = self.find_in_merge_cache(labels, &merge_props) {
                        cached_val
                    } else {
                        let mut found_node: Option<Value> = None;
                        for props in self.lookup_matching_node_props(labels, &merge_props)? {
                            if !node_matches_pattern(&props, labels, &merge_props) {
                                continue;
                            }
                            found_node = Some(
                                serde_json::to_value(&props)
                                    .map_err(|e| EvalError::SerializationError(e.to_string()))?,
                            );
                            break;
                        }

                        if let Some(existing) = found_node {
                            self.cache_merge_node(labels, &merge_props, &existing);
                            existing
                        } else {
                            let id = Uuid::new_v4().to_string();
                            let key = format!("{label}:{id}");
                            let mut props = merge_props.clone();
                            props.insert("_id".to_string(), Value::String(key.clone()));
                            props.insert(
                                "_labels".to_string(),
                                Value::Array(
                                    labels
                                        .iter()
                                        .map(|entry| Value::String(entry.clone()))
                                        .collect(),
                                ),
                            );
                            self.persist_node_props(&props)?;
                            stats.nodes_created += 1;
                            stats.properties_set += node_pat.properties.len();
                            let created = serde_json::to_value(&props)
                                .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                            self.cache_merge_node(labels, &merge_props, &created);
                            created
                        }
                    };

                let mut row = base_row.clone();
                if let Some(var) = &node_pat.variable {
                    row.insert(var.clone(), node_val);
                }
                next_rows.push(row);
            }

            current_rows = next_rows;
        }

        Ok(current_rows)
    }

    fn resolve_pipeline_node_binding(
        &self,
        row: &Row,
        node_pat: &NodePattern,
        params: &HashMap<String, Value>,
    ) -> Result<Option<(String, Value)>, EvalError> {
        let Some(var) = &node_pat.variable else {
            return Ok(None);
        };
        let Some(Value::Object(props)) = row.get(var) else {
            return Ok(None);
        };

        let props_map: HashMap<String, Value> = props.clone().into_iter().collect();
        let expected_props = evaluate_pattern_properties(&node_pat.properties, row, params)?;
        if !node_matches_pattern(&props_map, &node_pat.labels, &expected_props) {
            return Err(EvalError::ExecutionError(format!(
                "pipeline CREATE variable '{}' does not match the requested node pattern",
                var
            )));
        }
        let Some(existing_id) = node_id(&props_map) else {
            return Err(EvalError::ExecutionError(format!(
                "pipeline CREATE variable '{}' is missing _id",
                var
            )));
        };

        Ok(Some((
            existing_id.to_string(),
            Value::Object(props.clone()),
        )))
    }

    fn match_relationship_pattern(
        &self,
        base_rows: &[Row],
        pattern: &Pattern,
        params: &HashMap<String, Value>,
    ) -> Result<Vec<Row>, EvalError> {
        if pattern.nodes.len() != 2 || pattern.edges.len() != 1 {
            return Err(EvalError::ExecutionError(
                "only single-hop relationship MATCH is currently supported".to_string(),
            ));
        }
        let start_pattern = &pattern.nodes[0];
        let edge_pattern = &pattern.edges[0];
        let end_pattern = &pattern.nodes[1];

        if pattern.shortest_path
            || edge_pattern.min_hops.is_some()
            || edge_pattern.max_hops.is_some()
        {
            return self.match_variable_length_relationship_pattern(
                base_rows,
                pattern,
                start_pattern,
                edge_pattern,
                end_pattern,
                params,
            );
        }

        let mut rows = pooled_binding_rows();
        for base_row in base_rows {
            let start_candidates =
                self.bound_or_matching_node_props(base_row, start_pattern, params)?;
            for start_props in start_candidates {
                let Some(start_id) = node_id(&start_props) else {
                    continue;
                };
                let start_value = serde_json::to_value(&start_props)
                    .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                let expected_edge_props =
                    evaluate_pattern_properties(&edge_pattern.properties, base_row, params)?;
                let expected_end_props =
                    evaluate_pattern_properties(&end_pattern.properties, base_row, params)?;
                for edge in self.relationship_candidates(start_id, edge_pattern)? {
                    if !edge_matches_pattern(&edge, &expected_edge_props) {
                        continue;
                    }
                    if !bound_edge_matches_row(base_row, edge_pattern.variable.as_deref(), &edge) {
                        continue;
                    }
                    let Some(end_id) = related_node_id(start_id, &edge, &edge_pattern.direction)
                    else {
                        continue;
                    };
                    let Some(end_props) = self.node_props_by_id(end_id)? else {
                        continue;
                    };
                    if !node_matches_pattern(&end_props, &end_pattern.labels, &expected_end_props) {
                        continue;
                    }
                    if !bound_node_matches_row(
                        base_row,
                        end_pattern.variable.as_deref(),
                        &end_props,
                    ) {
                        continue;
                    }
                    let end_value = serde_json::to_value(&end_props)
                        .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                    let edge_value = edge_record_to_value(&edge)?;

                    let mut row = base_row.clone();
                    if let Some(var) = &start_pattern.variable {
                        row.insert(var.clone(), start_value.clone());
                    }
                    if let Some(var) = &edge_pattern.variable {
                        row.insert(var.clone(), edge_value.clone());
                    }
                    if let Some(var) = &end_pattern.variable {
                        row.insert(var.clone(), end_value.clone());
                    }
                    if let Some(path_var) = &pattern.path_variable {
                        row.insert(
                            path_var.clone(),
                            path_value(
                                vec![start_value.clone(), end_value.clone()],
                                vec![edge_value.clone()],
                            ),
                        );
                    }
                    rows.push(row);
                }
            }
        }
        Ok(rows)
    }

    fn match_variable_length_relationship_pattern(
        &self,
        base_rows: &[Row],
        pattern: &Pattern,
        start_pattern: &NodePattern,
        edge_pattern: &EdgePattern,
        end_pattern: &NodePattern,
        params: &HashMap<String, Value>,
    ) -> Result<Vec<Row>, EvalError> {
        let min_hops = edge_pattern.min_hops.unwrap_or(1);
        let max_hops = if pattern.shortest_path {
            edge_pattern
                .max_hops
                .unwrap_or(VAR_LENGTH_UNBOUNDED_MAX_HOPS)
                .max(min_hops)
        } else {
            edge_pattern
                .max_hops
                .unwrap_or(VAR_LENGTH_UNBOUNDED_MAX_HOPS)
                .max(min_hops)
        };
        let mut rows = pooled_binding_rows();

        for base_row in base_rows {
            for start_props in self.bound_or_matching_node_props(base_row, start_pattern, params)? {
                let Some(start_id) = node_id(&start_props).map(str::to_string) else {
                    continue;
                };
                let start_value = serde_json::to_value(&start_props)
                    .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                let expected_end_props =
                    evaluate_pattern_properties(&end_pattern.properties, base_row, params)?;
                let expected_edge_props =
                    evaluate_pattern_properties(&edge_pattern.properties, base_row, params)?;
                let mut frontier = VecDeque::new();
                let mut visited = HashSet::new();
                frontier.push_back((
                    start_id.clone(),
                    0_u32,
                    vec![start_id.clone()],
                    Vec::<EdgeRecord>::new(),
                ));
                visited.insert((start_id.clone(), 0_u32));

                while let Some((current_id, depth, path_node_ids, path_edges)) =
                    frontier.pop_front()
                {
                    if depth >= min_hops {
                        if let Some(end_props) = self.node_props_by_id(&current_id)? {
                            if node_matches_pattern(
                                &end_props,
                                &end_pattern.labels,
                                &expected_end_props,
                            ) {
                                let end_value = serde_json::to_value(&end_props)
                                    .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                                let node_values = path_node_ids
                                    .iter()
                                    .map(|node_id| {
                                        let props =
                                            self.node_props_by_id(node_id)?.ok_or_else(|| {
                                                EvalError::ExecutionError(format!(
                                                    "path node '{}' disappeared during traversal",
                                                    node_id
                                                ))
                                            })?;
                                        serde_json::to_value(&props).map_err(|e| {
                                            EvalError::SerializationError(e.to_string())
                                        })
                                    })
                                    .collect::<Result<Vec<_>, _>>()?;
                                let edge_values = path_edges
                                    .iter()
                                    .map(edge_record_to_value)
                                    .collect::<Result<Vec<_>, _>>()?;
                                let mut row = base_row.clone();
                                if let Some(var) = &start_pattern.variable {
                                    row.insert(var.clone(), start_value.clone());
                                }
                                if let Some(var) = &edge_pattern.variable {
                                    row.insert(var.clone(), Value::Array(edge_values.clone()));
                                }
                                if let Some(var) = &end_pattern.variable {
                                    row.insert(var.clone(), end_value.clone());
                                }
                                if let Some(path_var) = &pattern.path_variable {
                                    row.insert(
                                        path_var.clone(),
                                        path_value(node_values, edge_values.clone()),
                                    );
                                }
                                rows.push(row);
                                if pattern.shortest_path {
                                    break;
                                }
                            }
                        }
                    }

                    if depth >= max_hops {
                        continue;
                    }

                    for edge in self.relationship_candidates(&current_id, edge_pattern)? {
                        if !edge_matches_pattern(&edge, &expected_edge_props) {
                            continue;
                        }
                        let Some(next_id) =
                            related_node_id(&current_id, &edge, &edge_pattern.direction)
                                .map(str::to_string)
                        else {
                            continue;
                        };
                        let next_depth = depth + 1;
                        let visit_key = (next_id.clone(), next_depth);
                        if !visited.insert(visit_key) {
                            continue;
                        }
                        let mut next_node_ids = path_node_ids.clone();
                        next_node_ids.push(next_id.clone());
                        let mut next_edges = path_edges.clone();
                        next_edges.push(edge);
                        frontier.push_back((next_id, next_depth, next_node_ids, next_edges));
                    }
                }
            }
        }

        Ok(rows)
    }

    fn matching_node_props(
        &self,
        pattern: &NodePattern,
        row: &Row,
        params: &HashMap<String, Value>,
    ) -> Result<Vec<HashMap<String, Value>>, EvalError> {
        let expected_props = evaluate_pattern_properties(&pattern.properties, row, params)?;
        self.lookup_matching_node_props(&pattern.labels, &expected_props)
    }

    fn lookup_matching_node_props(
        &self,
        labels: &[String],
        expected_props: &HashMap<String, Value>,
    ) -> Result<Vec<HashMap<String, Value>>, EvalError> {
        let catalog = IndexCatalog::new(self.storage.as_ref());
        let resolver = self.knowledge_policy_resolver()?;
        let mut out = Vec::new();
        for node in catalog.lookup_nodes(labels, expected_props)? {
            if !self.node_visible_under_policy(&node, &resolver)? {
                continue;
            }
            let props = node_record_to_props(&node);
            if node_matches_pattern(&props, labels, expected_props) {
                self.apply_on_access_for_node(&node, &resolver)?;
                out.push(props);
            }
        }
        Ok(out)
    }

    fn bound_or_matching_node_props(
        &self,
        row: &Row,
        pattern: &NodePattern,
        params: &HashMap<String, Value>,
    ) -> Result<Vec<HashMap<String, Value>>, EvalError> {
        let expected_props = evaluate_pattern_properties(&pattern.properties, row, params)?;
        if let Some(variable) = &pattern.variable {
            if let Some(bound_props) = bound_row_object_props(row, variable) {
                if node_matches_pattern(&bound_props, &pattern.labels, &expected_props) {
                    return Ok(vec![bound_props]);
                }
                return Ok(Vec::new());
            }
        }

        self.matching_node_props(pattern, row, params)
    }

    fn node_props_by_id(&self, node_id: &str) -> Result<Option<HashMap<String, Value>>, EvalError> {
        let Some(node) = self.storage.get_node_record(node_id)? else {
            return Ok(None);
        };
        let resolver = self.knowledge_policy_resolver()?;
        if !self.node_visible_under_policy(&node, &resolver)? {
            return Ok(None);
        }
        self.apply_on_access_for_node(&node, &resolver)?;
        Ok(Some(node_record_to_props(&node)))
    }

    fn node_visible_under_policy(&self, node: &NodeRecord, resolver: &Resolver) -> Result<bool, EvalError> {
        self.node_visible_under_policy_with_params(node, resolver, &HashMap::new())
    }

    fn node_visible_under_policy_with_params(
        &self,
        node: &NodeRecord,
        resolver: &Resolver,
        params: &HashMap<String, Value>,
    ) -> Result<bool, EvalError> {
        let Some(binding) = resolver.resolve_node(&node.labels) else {
            return Ok(true);
        };
        let access_metadata = self.knowledge_policy_access_metadata(&node.id)?;
        binding_visible_under_anchor(
            self,
            &binding,
            &node.id,
            node.created_at_unix_ms,
            node.updated_at_unix_ms,
            access_metadata,
            &node.properties,
            params,
        )
    }

    fn edge_visible_under_policy(&self, edge: &EdgeRecord, resolver: &Resolver) -> Result<bool, EvalError> {
        self.edge_visible_under_policy_with_params(edge, resolver, &HashMap::new())
    }

    fn edge_visible_under_policy_with_params(
        &self,
        edge: &EdgeRecord,
        resolver: &Resolver,
        params: &HashMap<String, Value>,
    ) -> Result<bool, EvalError> {
        let Some(binding) = resolver.resolve_edge(&edge.edge_type) else {
            return Ok(true);
        };
        let access_metadata = self.knowledge_policy_access_metadata(&edge.id)?;
        binding_visible_under_anchor(
            self,
            &binding,
            &edge.id,
            edge.created_at_unix_ms,
            edge.updated_at_unix_ms,
            access_metadata,
            &edge.properties,
            params,
        )
    }

    fn knowledge_policy_access_metadata(
        &self,
        entity_id: &str,
    ) -> Result<Option<KnowledgePolicyAccessMetadata>, EvalError> {
        let persisted = self.storage.get_knowledge_policy_access_metadata(entity_id)?;
        let pending = self.access_flusher.pending_mutation(entity_id);
        Ok(merge_access_metadata(persisted, pending.as_ref()))
    }

    fn apply_on_access_for_node(&self, node: &NodeRecord, resolver: &Resolver) -> Result<(), EvalError> {
        self.apply_on_access_mutations(
            &node.id,
            resolver
                .resolve_node(&node.labels)
                .and_then(|binding| binding.promotion_policy)
                .or_else(|| resolver.resolve_node_promotion(&node.labels)),
        )
    }

    fn apply_on_access_for_edge(&self, edge: &EdgeRecord, resolver: &Resolver) -> Result<(), EvalError> {
        self.apply_on_access_mutations(
            &edge.id,
            resolver
                .resolve_edge(&edge.edge_type)
                .and_then(|binding| binding.promotion_policy)
                .or_else(|| resolver.resolve_edge_promotion(&edge.edge_type)),
        )
    }

    fn apply_on_access_mutations(
        &self,
        entity_id: &str,
        policy: Option<copperdb_knowledgepolicy::PromotionPolicyDef>,
    ) -> Result<(), EvalError> {
        self.access_flusher
            .record_policy_access(entity_id, policy.as_ref(), now_unix_ms());
        Ok(())
    }

    fn execute_call_clause(
        &self,
        call: &copperdb_cypher::CallClause,
        params: &HashMap<String, Value>,
    ) -> Result<EvalResult, EvalError> {
        if call.procedure.eq_ignore_ascii_case("nornicdb.knowledgepolicy.resolve") {
            return self.execute_knowledge_policy_resolve_call(call, params);
        }

        Err(EvalError::ExecutionError(format!(
            "CALL {} is not supported yet",
            call.procedure
        )))
    }

    fn execute_knowledge_policy_resolve_call(
        &self,
        call: &copperdb_cypher::CallClause,
        params: &HashMap<String, Value>,
    ) -> Result<EvalResult, EvalError> {
        if call.args.len() != 3 {
            return Err(EvalError::ExecutionError(
                "nornicdb.knowledgepolicy.resolve expects 3 arguments: entityId, labelsCsv, edgeType"
                    .to_string(),
            ));
        }

        let row = Row::new();
        let entity_id = eval_expression(&call.args[0], &row, params)?;
        let labels_csv = eval_expression(&call.args[1], &row, params)?;
        let edge_type = eval_expression(&call.args[2], &row, params)?;

        let entity_id = entity_id.as_str().unwrap_or_default().trim().to_string();
        let labels_csv = labels_csv.as_str().unwrap_or_default().trim().to_string();
        let edge_type = edge_type.as_str().unwrap_or_default().trim().to_string();

        let inspection = if !entity_id.is_empty() {
            self.inspect_knowledge_policy_by_id(&entity_id, params)?
        } else if !edge_type.is_empty() {
            self.inspect_knowledge_policy_for_edge_type(&edge_type, params)?
        } else {
            let labels = labels_csv
                .split(',')
                .map(str::trim)
                .filter(|label| !label.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>();
            self.inspect_knowledge_policy_for_labels(&labels, params)?
        };

        Ok(EvalResult {
            columns: vec![
                "entityId".to_string(),
                "targetKind".to_string(),
                "targetLabels".to_string(),
                "targetEdgeType".to_string(),
                "decayBinding".to_string(),
                "promotionPolicy".to_string(),
                "matchedPromotionProfile".to_string(),
                "matchedPromotionPredicate".to_string(),
                "scoreFrom".to_string(),
                "anchorUnixMs".to_string(),
                "accessCount".to_string(),
                "lastAccessedAtUnixMs".to_string(),
                "baseScore".to_string(),
                "finalScore".to_string(),
                "visibilityThreshold".to_string(),
                "suppressed".to_string(),
                "dryRun".to_string(),
                "explanation".to_string(),
            ],
            rows: vec![inspection.into_row()],
            stats: QueryStats::default(),
        })
    }

    fn inspect_knowledge_policy_by_id(
        &self,
        entity_id: &str,
        params: &HashMap<String, Value>,
    ) -> Result<KnowledgePolicyInspection, EvalError> {
        let resolver = self.knowledge_policy_resolver()?;
        if let Some(node) = self.storage.get_node_record(entity_id)? {
            return self.inspect_node_policy(&resolver, &node, params, false);
        }
        if let Some(edge) = self.storage.get_edge_record(entity_id)? {
            return self.inspect_edge_policy(&resolver, &edge, params, false);
        }
        Err(EvalError::ExecutionError(format!(
            "entity {entity_id:?} not found"
        )))
    }

    fn inspect_knowledge_policy_for_labels(
        &self,
        labels: &[String],
        params: &HashMap<String, Value>,
    ) -> Result<KnowledgePolicyInspection, EvalError> {
        let resolver = self.knowledge_policy_resolver()?;
        let binding = resolver.resolve_node(labels);
        let promotion_policy = binding
            .as_ref()
            .and_then(|compiled| compiled.promotion_policy.clone())
            .or_else(|| resolver.resolve_node_promotion(labels));

        Ok(self.inspect_resolved_target(
            None,
            "NODE".to_string(),
            labels.to_vec(),
            None,
            binding.as_ref(),
            promotion_policy.as_ref(),
            None,
            None,
            params,
            true,
        )?)
    }

    fn inspect_knowledge_policy_for_edge_type(
        &self,
        edge_type: &str,
        params: &HashMap<String, Value>,
    ) -> Result<KnowledgePolicyInspection, EvalError> {
        let resolver = self.knowledge_policy_resolver()?;
        let binding = resolver.resolve_edge(edge_type);
        let promotion_policy = binding
            .as_ref()
            .and_then(|compiled| compiled.promotion_policy.clone())
            .or_else(|| resolver.resolve_edge_promotion(edge_type));

        Ok(self.inspect_resolved_target(
            None,
            "EDGE".to_string(),
            Vec::new(),
            Some(edge_type.to_string()),
            binding.as_ref(),
            promotion_policy.as_ref(),
            None,
            None,
            params,
            true,
        )?)
    }

    fn inspect_node_policy(
        &self,
        resolver: &Resolver,
        node: &NodeRecord,
        params: &HashMap<String, Value>,
        dry_run: bool,
    ) -> Result<KnowledgePolicyInspection, EvalError> {
        let access_metadata = self.knowledge_policy_access_metadata(&node.id)?;

        self.inspect_node_policy_with_access_metadata(
            resolver,
            node,
            access_metadata,
            params,
            dry_run,
        )
    }

    fn inspect_node_policy_with_access_metadata(
        &self,
        resolver: &Resolver,
        node: &NodeRecord,
        access_metadata: Option<KnowledgePolicyAccessMetadata>,
        params: &HashMap<String, Value>,
        dry_run: bool,
    ) -> Result<KnowledgePolicyInspection, EvalError> {
        let binding = resolver.resolve_node(&node.labels);
        let promotion_policy = binding
            .as_ref()
            .and_then(|compiled| compiled.promotion_policy.clone())
            .or_else(|| resolver.resolve_node_promotion(&node.labels));

        self.inspect_resolved_target(
            Some(node.id.clone()),
            "NODE".to_string(),
            node.labels.clone(),
            None,
            binding.as_ref(),
            promotion_policy.as_ref(),
            Some((
                node.created_at_unix_ms,
                node.updated_at_unix_ms,
                &node.properties,
            )),
            access_metadata,
            params,
            dry_run,
        )
    }

    fn inspect_edge_policy(
        &self,
        resolver: &Resolver,
        edge: &EdgeRecord,
        params: &HashMap<String, Value>,
        dry_run: bool,
    ) -> Result<KnowledgePolicyInspection, EvalError> {
        let access_metadata = self.knowledge_policy_access_metadata(&edge.id)?;

        self.inspect_edge_policy_with_access_metadata(
            resolver,
            edge,
            access_metadata,
            params,
            dry_run,
        )
    }

    fn inspect_edge_policy_with_access_metadata(
        &self,
        resolver: &Resolver,
        edge: &EdgeRecord,
        access_metadata: Option<KnowledgePolicyAccessMetadata>,
        params: &HashMap<String, Value>,
        dry_run: bool,
    ) -> Result<KnowledgePolicyInspection, EvalError> {
        let binding = resolver.resolve_edge(&edge.edge_type);
        let promotion_policy = binding
            .as_ref()
            .and_then(|compiled| compiled.promotion_policy.clone())
            .or_else(|| resolver.resolve_edge_promotion(&edge.edge_type));

        self.inspect_resolved_target(
            Some(edge.id.clone()),
            "EDGE".to_string(),
            Vec::new(),
            Some(edge.edge_type.clone()),
            binding.as_ref(),
            promotion_policy.as_ref(),
            Some((
                edge.created_at_unix_ms,
                edge.updated_at_unix_ms,
                &edge.properties,
            )),
            access_metadata,
            params,
            dry_run,
        )
    }

    fn inspect_resolved_target(
        &self,
        entity_id: Option<String>,
        target_kind: String,
        target_labels: Vec<String>,
        target_edge_type: Option<String>,
        binding: Option<&CompiledBinding>,
        promotion_policy: Option<&copperdb_knowledgepolicy::PromotionPolicyDef>,
        entity_state: Option<(i64, i64, &BTreeMap<String, Value>)>,
        access_metadata: Option<KnowledgePolicyAccessMetadata>,
        params: &HashMap<String, Value>,
        dry_run: bool,
    ) -> Result<KnowledgePolicyInspection, EvalError> {
        let Some(binding) = binding else {
            let explanation = if let Some(policy) = promotion_policy {
                format!(
                    "policy-only target using promotion policy {}; no decay binding applies so final score defaults to 1.0",
                    policy.name
                )
            } else {
                "no decay binding or promotion policy matched; final score defaults to 1.0".to_string()
            };
            return Ok(KnowledgePolicyInspection {
                entity_id,
                target_kind,
                target_labels,
                target_edge_type,
                decay_binding: None,
                promotion_policy: promotion_policy.map(|policy| policy.name.clone()),
                matched_promotion_profile: None,
                matched_promotion_predicate: None,
                score_from: None,
                anchor_unix_ms: None,
                access_count: access_metadata.as_ref().map(|metadata| metadata.access_count),
                last_accessed_at_unix_ms: access_metadata
                    .as_ref()
                    .and_then(|metadata| metadata.last_accessed_at_unix_ms),
                base_score: 1.0,
                final_score: 1.0,
                visibility_threshold: 0.0,
                suppressed: false,
                dry_run,
                explanation,
            });
        };

        let (anchor_unix_ms, matched_rule, score) = if let Some((created_at_unix_ms, updated_at_unix_ms, properties)) =
            entity_state
        {
            let anchor_unix_ms = binding_anchor_unix_ms(
                binding,
                created_at_unix_ms,
                updated_at_unix_ms,
                access_metadata
                    .as_ref()
                    .and_then(|metadata| metadata.last_accessed_at_unix_ms),
                properties,
            );
            let matched_rule = matched_promotion_rule(
                binding,
                properties,
                access_metadata.as_ref(),
                params,
            )?;
            let score = score_binding(
                binding,
                anchor_unix_ms,
                now_unix_ms(),
                matched_rule.as_ref().map(|rule| &rule.profile),
            );
            (anchor_unix_ms, matched_rule, score)
        } else {
            (
                None,
                None,
                score_binding(binding, None, now_unix_ms(), None),
            )
        };

        let explanation = if dry_run {
            format!(
                "dry-run target resolution using decay binding {}{}",
                binding.decay_binding.name,
                promotion_policy
                    .map(|policy| format!(" and promotion policy {}", policy.name))
                    .unwrap_or_default()
            )
        } else if score.suppressed {
            format!(
                "final score {:.4} is below visibility threshold {:.4}",
                score.final_score, binding.visibility_threshold
            )
        } else if let Some(rule) = &matched_rule {
            format!(
                "promotion predicate {:?} matched profile {} and produced final score {:.4}",
                rule.predicate, rule.profile.name, score.final_score
            )
        } else {
            format!(
                "no promotion predicate matched; final score {:.4} remains visible against threshold {:.4}",
                score.final_score, binding.visibility_threshold
            )
        };

        Ok(KnowledgePolicyInspection {
            entity_id,
            target_kind,
            target_labels,
            target_edge_type,
            decay_binding: Some(binding.decay_binding.name.clone()),
            promotion_policy: promotion_policy.map(|policy| policy.name.clone()),
            matched_promotion_profile: matched_rule.as_ref().map(|rule| rule.profile.name.clone()),
            matched_promotion_predicate: matched_rule.as_ref().map(|rule| rule.predicate.clone()),
            score_from: Some(format!("{:?}", binding.score_from).to_ascii_uppercase()),
            anchor_unix_ms,
            access_count: access_metadata.as_ref().map(|metadata| metadata.access_count),
            last_accessed_at_unix_ms: access_metadata
                .as_ref()
                .and_then(|metadata| metadata.last_accessed_at_unix_ms),
            base_score: score.base_score,
            final_score: score.final_score,
            visibility_threshold: binding.visibility_threshold,
            suppressed: score.suppressed,
            dry_run,
            explanation,
        })
    }

    pub fn node_visible_with_access_metadata(
        &self,
        node: &NodeRecord,
        access_metadata: Option<KnowledgePolicyAccessMetadata>,
        params: &HashMap<String, Value>,
    ) -> Result<bool, EvalError> {
        let resolver = self.knowledge_policy_resolver()?;
        Ok(!self
            .inspect_node_policy_with_access_metadata(&resolver, node, access_metadata, params, false)?
            .suppressed)
    }

    pub fn edge_visible_with_access_metadata(
        &self,
        edge: &EdgeRecord,
        access_metadata: Option<KnowledgePolicyAccessMetadata>,
        params: &HashMap<String, Value>,
    ) -> Result<bool, EvalError> {
        let resolver = self.knowledge_policy_resolver()?;
        Ok(!self
            .inspect_edge_policy_with_access_metadata(&resolver, edge, access_metadata, params, false)?
            .suppressed)
    }

    pub fn node_access_metadata_after_read(
        &self,
        node: &NodeRecord,
        access_metadata: Option<KnowledgePolicyAccessMetadata>,
    ) -> Result<Option<KnowledgePolicyAccessMetadata>, EvalError> {
        let resolver = self.knowledge_policy_resolver()?;
        let policy = resolver
            .resolve_node(&node.labels)
            .and_then(|binding| binding.promotion_policy)
            .or_else(|| resolver.resolve_node_promotion(&node.labels));
        Ok(access_metadata_after_policy_access(
            access_metadata,
            policy.as_ref(),
            now_unix_ms(),
        ))
    }

    pub fn edge_access_metadata_after_read(
        &self,
        edge: &EdgeRecord,
        access_metadata: Option<KnowledgePolicyAccessMetadata>,
    ) -> Result<Option<KnowledgePolicyAccessMetadata>, EvalError> {
        let resolver = self.knowledge_policy_resolver()?;
        let policy = resolver
            .resolve_edge(&edge.edge_type)
            .and_then(|binding| binding.promotion_policy)
            .or_else(|| resolver.resolve_edge_promotion(&edge.edge_type));
        Ok(access_metadata_after_policy_access(
            access_metadata,
            policy.as_ref(),
            now_unix_ms(),
        ))
    }

    fn persist_node_props(&self, props: &HashMap<String, Value>) -> Result<(), EvalError> {
        let now = now_unix_ms();
        let mut record = node_record_from_props(props)?;
        if let Some(existing) = self.storage.get_node_record(&record.id)? {
            record.created_at_unix_ms = existing.created_at_unix_ms;
            record.updated_at_unix_ms = now;
        } else {
            record.created_at_unix_ms = now;
            record.updated_at_unix_ms = now;
        }
        self.storage.put_node_record(&record)?;
        Ok(())
    }

    fn persist_edge_record(&self, mut edge: EdgeRecord) -> Result<EdgeRecord, EvalError> {
        let now = now_unix_ms();
        if let Some(existing) = self.storage.get_edge_record(&edge.id)? {
            edge.created_at_unix_ms = existing.created_at_unix_ms;
            edge.updated_at_unix_ms = now;
        } else {
            edge.created_at_unix_ms = now;
            edge.updated_at_unix_ms = now;
        }
        self.storage.put_edge_record(&edge)?;
        Ok(edge)
    }

    fn relationship_candidates(
        &self,
        node_id: &str,
        edge: &EdgePattern,
    ) -> Result<Vec<EdgeRecord>, EvalError> {
        let candidates = match (&edge.direction, edge.rel_type.as_deref()) {
            (EdgeDirection::Outgoing, Some(edge_type)) => self
                .storage
                .get_edges_from_node_by_type(node_id, edge_type)?,
            (EdgeDirection::Outgoing, None) => self.storage.get_edges_from_node(node_id)?,
            (EdgeDirection::Incoming, Some(edge_type)) => {
                self.storage.get_edges_to_node_by_type(node_id, edge_type)?
            }
            (EdgeDirection::Incoming, None) => self.storage.get_edges_to_node(node_id)?,
            (EdgeDirection::Both, Some(edge_type)) => {
                let mut edges = self
                    .storage
                    .get_edges_from_node_by_type(node_id, edge_type)?;
                edges.extend(self.storage.get_edges_to_node_by_type(node_id, edge_type)?);
                edges
            }
            (EdgeDirection::Both, None) => {
                let mut edges = self.storage.get_edges_from_node(node_id)?;
                edges.extend(self.storage.get_edges_to_node(node_id)?);
                edges
            }
        };
        let resolver = self.knowledge_policy_resolver()?;
        let mut visible = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            if self.edge_visible_under_policy(&candidate, &resolver)? {
                visible.push(candidate);
            }
        }
        visible.sort_by(|a, b| a.id.cmp(&b.id));
        visible.dedup_by(|a, b| a.id == b.id);
        for edge in &visible {
            self.apply_on_access_for_edge(edge, &resolver)?;
        }
        Ok(visible)
    }

    fn lookup_edges(&self, edge_type: Option<&str>) -> Result<Vec<EdgeRecord>, EvalError> {
        let resolver = self.knowledge_policy_resolver()?;
        let mut visible = Vec::new();
        for edge in IndexCatalog::new(self.storage.as_ref()).lookup_edges(edge_type)? {
            if self.edge_visible_under_policy(&edge, &resolver)? {
                self.apply_on_access_for_edge(&edge, &resolver)?;
                visible.push(edge);
            }
        }
        Ok(visible)
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

    let matched_promotion = match_promotion_profile(
        binding,
        properties,
        access_metadata.as_ref(),
        params,
    )
    .map_err(|error| {
        EvalError::FilterError(format!(
            "promotion predicate evaluation failed for {entity_id}: {error}"
        ))
    })?;

    Ok(
        !score_binding(binding, Some(anchor_unix_ms), now_unix_ms(), matched_promotion.as_ref())
            .suppressed,
    )
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
    Ok(matched_promotion_rule(binding, properties, access_metadata, params)?
        .map(|rule| rule.profile))
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
        Value::from(access_metadata.map(|metadata| metadata.access_count).unwrap_or(0)),
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
        | Expression::Or(operands) => {
            collect_expression_variables(&operands.left, variables);
            collect_expression_variables(&operands.right, variables);
        }
        Expression::FunctionCall { args, .. } | Expression::ListLiteral(args) => {
            for argument in args {
                collect_expression_variables(argument, variables);
            }
        }
        Expression::MapLiteral(entries) => {
            for entry in entries {
                collect_expression_variables(&entry.value, variables);
            }
        }
        Expression::Not(inner)
        | Expression::IsNull(inner)
        | Expression::IsNotNull(inner) => collect_expression_variables(inner, variables),
        Expression::Literal(_) | Expression::Parameter(_) => {}
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
    let id = node_id(props).ok_or_else(|| {
        EvalError::ExecutionError("node is missing _id metadata".to_string())
    })?;
    let labels = props
        .get("_labels")
        .and_then(Value::as_array)
        .ok_or_else(|| EvalError::ExecutionError("node is missing _labels metadata".to_string()))?
        .iter()
        .map(|label| {
            label.as_str().map(str::to_string).ok_or_else(|| {
                EvalError::ExecutionError("node _labels metadata must be a string array".to_string())
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

fn sort_rows_by_return_order(rows: &mut [Row], ret: &copperdb_cypher::ReturnClause) {
    rows.sort_by(|left, right| {
        for item in &ret.order_by {
            let left_key = optimized_order_key(left, &item.expression);
            let right_key = optimized_order_key(right, &item.expression);
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
        Expression::PropertyAccess { variable, property } => row
            .get(&format!("{}.{}", variable, property))
            .cloned()
            .unwrap_or(Value::Null),
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
    use super::*;
    use copperdb_cypher::{
        can_execute_as_pipeline, detect_query_pattern, match_compound_query_shape, Parser,
        QueryPattern,
    };
    use copperdb_storage::{EdgeRecord, NodeRecord, StorageEngine};

    fn node_props(name: &str) -> HashMap<String, Value> {
        [("name".to_string(), Value::String(name.to_string()))]
            .into_iter()
            .collect()
    }

    fn store_node(
        storage: &StorageEngine,
        id: &str,
        labels: &[&str],
        mut properties: HashMap<String, Value>,
    ) {
        properties.remove("_id");
        properties.remove("_labels");
        storage
            .put_node_record(&NodeRecord {
                id: id.to_string(),
                labels: labels.iter().map(|label| (*label).to_string()).collect(),
                properties: properties.into_iter().collect(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
    }

    fn review_edge(id: &str, start: &str, end: &str, rating: i64) -> EdgeRecord {
        EdgeRecord {
            id: id.to_string(),
            start_node: start.to_string(),
            end_node: end.to_string(),
            edge_type: "REVIEWED".to_string(),
            properties: [("rating".to_string(), Value::from(rating))]
                .into_iter()
                .collect(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        }
    }

    fn seed_review_graph(engine: &EvalEngine) {
        for (id, props) in [
            ("customer:1", node_props("Alice")),
            ("customer:2", node_props("Bob")),
            ("customer:3", node_props("Carol")),
            ("customer:4", node_props("Dave")),
            ("product:1", node_props("Widget")),
            ("product:2", node_props("Gadget")),
            ("product:3", node_props("Thing")),
        ] {
            let label = id.split(':').next().unwrap_or("Node");
            store_node(engine.storage.as_ref(), id, &[label], props);
        }

        for edge in [
            review_edge("review:1", "customer:1", "product:1", 5),
            review_edge("review:2", "customer:2", "product:1", 4),
            review_edge("review:3", "customer:3", "product:1", 4),
            review_edge("review:4", "customer:1", "product:2", 3),
            review_edge("review:5", "customer:4", "product:2", 3),
            review_edge("review:6", "customer:2", "product:3", 5),
        ] {
            engine.storage.put_edge_record(&edge).unwrap();
        }
    }

    fn seed_social_graph(engine: &EvalEngine) {
        for (id, props) in [
            ("person:1", node_props("Alice")),
            ("person:2", node_props("Bob")),
            ("person:3", node_props("Carol")),
            ("person:4", node_props("Dave")),
        ] {
            let label = id.split(':').next().unwrap_or("Node");
            store_node(engine.storage.as_ref(), id, &[label], props);
        }

        for edge in [
            EdgeRecord {
                id: "follows:1".into(),
                start_node: "person:1".into(),
                end_node: "person:2".into(),
                edge_type: "FOLLOWS".into(),
                properties: HashMap::new().into_iter().collect(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "follows:2".into(),
                start_node: "person:2".into(),
                end_node: "person:1".into(),
                edge_type: "FOLLOWS".into(),
                properties: HashMap::new().into_iter().collect(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "follows:3".into(),
                start_node: "person:3".into(),
                end_node: "person:1".into(),
                edge_type: "FOLLOWS".into(),
                properties: HashMap::new().into_iter().collect(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "follows:4".into(),
                start_node: "person:4".into(),
                end_node: "person:1".into(),
                edge_type: "FOLLOWS".into(),
                properties: HashMap::new().into_iter().collect(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "follows:5".into(),
                start_node: "person:1".into(),
                end_node: "person:3".into(),
                edge_type: "FOLLOWS".into(),
                properties: HashMap::new().into_iter().collect(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
        ] {
            engine.storage.put_edge_record(&edge).unwrap();
        }
    }

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
    fn test_match_single_node_path_variable_materializes_path_accessors() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (:Person {name: 'Alice'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let query = parser
            .parse(
                "MATCH p = (n:Person {name: 'Alice'}) RETURN p AS path, nodes(p) AS nodes, relationships(p) AS rels, length(p) AS hops",
            )
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("hops"), Some(&Value::from(0)));
        assert_eq!(result.rows[0].get("rels"), Some(&Value::Array(Vec::new())));
        let nodes = result.rows[0]
            .get("nodes")
            .and_then(Value::as_array)
            .expect("expected path nodes");
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].get("name"), Some(&Value::String("Alice".into())));
        let path = result.rows[0]
            .get("path")
            .and_then(Value::as_object)
            .expect("expected path object");
        assert_eq!(path.get("length"), Some(&Value::from(0)));
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
    fn test_execute_create_uses_current_row_expression_properties() {
        let engine = make_engine();
        let parser = Parser::new();
        let query = parser
            .parse("UNWIND [1, 2] AS orderID CREATE (o:Order {orderID: orderID}) RETURN o.orderID AS orderID")
            .unwrap();

        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("orderID"), Some(&Value::from(1)));
        assert_eq!(result.rows[1].get("orderID"), Some(&Value::from(2)));
    }

    #[test]
    fn test_execute_merge_uses_current_row_expression_properties() {
        let engine = make_engine();
        let parser = Parser::new();
        let query = parser
            .parse("UNWIND [1, 2] AS customerID MERGE (c:Customer {customerID: customerID}) RETURN c.customerID AS customerID")
            .unwrap();

        let first = engine.execute(&query, &HashMap::new()).unwrap();
        let second = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(first.rows.len(), 2);
        assert_eq!(first.rows[0].get("customerID"), Some(&Value::from(1)));
        assert_eq!(first.rows[1].get("customerID"), Some(&Value::from(2)));
        assert_eq!(first.stats.nodes_created, 2);
        assert_eq!(second.stats.nodes_created, 0);

        let all_customers = engine
            .execute(
                &parser
                    .parse("MATCH (c:Customer) RETURN c.customerID AS customerID ORDER BY c.customerID")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(all_customers.rows.len(), 2);
        assert_eq!(
            all_customers.rows[0].get("customerID"),
            Some(&Value::from(1))
        );
        assert_eq!(
            all_customers.rows[1].get("customerID"),
            Some(&Value::from(2))
        );
    }

    #[test]
    fn test_execute_with_pattern_optimizes_edge_property_aggregation() {
        let engine = make_engine();
        let parser = Parser::new();
        seed_review_graph(&engine);

        let cypher = "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) RETURN p.name AS product, avg(r.rating) AS avgRating, count(r) AS reviewCount ORDER BY avgRating DESC LIMIT 2";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        assert_eq!(pattern.pattern, QueryPattern::EdgePropertyAgg);

        let result = engine
            .execute_with_pattern(&query, &HashMap::new(), &pattern)
            .unwrap();

        assert_eq!(result.columns, vec!["product", "avgRating", "reviewCount"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(
            result.rows[0].get("product"),
            Some(&Value::String("Thing".into()))
        );
        assert_eq!(result.rows[0].get("avgRating"), Some(&Value::from(5.0)));
        assert_eq!(result.rows[0].get("reviewCount"), Some(&Value::from(1)));
        assert_eq!(
            result.rows[1].get("product"),
            Some(&Value::String("Widget".into()))
        );
        assert_eq!(
            result.rows[1].get("avgRating"),
            Some(&Value::from(13.0 / 3.0))
        );
        assert_eq!(result.rows[1].get("reviewCount"), Some(&Value::from(3)));
    }

    #[test]
    fn test_execute_with_pattern_edge_property_aggregation_branch_coverage() {
        let engine = make_engine();
        let parser = Parser::new();

        for (id, label, props) in [
            (
                "product:1",
                "Product",
                HashMap::from([("name".to_string(), Value::String("P1".into()))]),
            ),
            (
                "product:2",
                "Product",
                HashMap::from([("name".to_string(), Value::String("P2".into()))]),
            ),
            (
                "customer:1",
                "Customer",
                HashMap::from([("name".to_string(), Value::String("C1".into()))]),
            ),
        ] {
            store_node(engine.storage.as_ref(), id, &[label], props);
        }

        for edge in [
            EdgeRecord {
                id: "review:1".into(),
                start_node: "customer:1".into(),
                end_node: "product:1".into(),
                edge_type: "REVIEWED".into(),
                properties: BTreeMap::from([("rating".into(), Value::from(4.5))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "review:2".into(),
                start_node: "customer:1".into(),
                end_node: "product:1".into(),
                edge_type: "REVIEWED".into(),
                properties: BTreeMap::from([("rating".into(), Value::from(5))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "review:3".into(),
                start_node: "customer:1".into(),
                end_node: "product:2".into(),
                edge_type: "REVIEWED".into(),
                properties: BTreeMap::from([("other".into(), Value::from(9))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "review:4".into(),
                start_node: "customer:1".into(),
                end_node: "product:2".into(),
                edge_type: "REVIEWED".into(),
                properties: BTreeMap::from([("rating".into(), Value::String("bad".into()))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
            EdgeRecord {
                id: "review:5".into(),
                start_node: "customer:1".into(),
                end_node: "product:missing".into(),
                edge_type: "REVIEWED".into(),
                properties: BTreeMap::from([("rating".into(), Value::from(2))]),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            },
        ] {
            engine.storage.put_edge_record(&edge).unwrap();
        }

        let cypher = "MATCH (c)-[r:REVIEWED]->(p) RETURN p.name AS product, avg(r.rating) AS avgRating, count(r) AS reviewCount, min(r.rating) AS minRating, max(r.rating) AS maxRating, sum(r.rating) AS totalRating";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_pattern(&query, &HashMap::new(), &pattern)
            .unwrap();

        assert_eq!(
            result.columns,
            vec![
                "product",
                "avgRating",
                "reviewCount",
                "minRating",
                "maxRating",
                "totalRating"
            ]
        );
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("product"),
            Some(&Value::String("P1".into()))
        );
        assert_eq!(result.rows[0].get("avgRating"), Some(&Value::from(4.75)));
        assert_eq!(result.rows[0].get("reviewCount"), Some(&Value::from(2)));
        assert_eq!(result.rows[0].get("minRating"), Some(&Value::from(4.5)));
        assert_eq!(result.rows[0].get("maxRating"), Some(&Value::from(5.0)));
        assert_eq!(result.rows[0].get("totalRating"), Some(&Value::from(9.5)));
    }

    #[test]
    fn test_execute_with_routes_optimizes_mutual_relationships() {
        let engine = make_engine();
        let parser = Parser::new();
        seed_social_graph(&engine);

        let cypher = "MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:FOLLOWS]->(a) RETURN a.name AS a, b.name AS b";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_routes(&query, &HashMap::new(), &pattern, None, None)
            .unwrap();

        assert_eq!(pattern.pattern, QueryPattern::MutualRelationship);
        assert_eq!(result.rows.len(), 2);
        let pairs: HashSet<(String, String)> = result
            .rows
            .iter()
            .map(|row| {
                let mut pair = [
                    row.get("a").and_then(Value::as_str).unwrap().to_string(),
                    row.get("b").and_then(Value::as_str).unwrap().to_string(),
                ];
                pair.sort();
                (pair[0].clone(), pair[1].clone())
            })
            .collect();
        assert!(pairs.contains(&("Alice".into(), "Bob".into())));
        assert!(pairs.contains(&("Alice".into(), "Carol".into())));
    }

    #[test]
    fn test_execute_with_routes_mutual_relationship_on_empty_db_returns_no_rows() {
        let engine = make_engine();
        let parser = Parser::new();

        let cypher = "MATCH (a)-[:FOLLOWS]->(b)-[:FOLLOWS]->(a) RETURN a, b";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_routes(&query, &HashMap::new(), &pattern, None, None)
            .unwrap();

        assert_eq!(pattern.pattern, QueryPattern::MutualRelationship);
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_execute_with_routes_mutual_relationship_with_missing_rel_type_returns_no_rows() {
        let engine = make_engine();
        let parser = Parser::new();
        seed_social_graph(&engine);

        let cypher = "MATCH (a)-[:NONEXISTENT]->(b)-[:NONEXISTENT]->(a) RETURN a, b";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_routes(&query, &HashMap::new(), &pattern, None, None)
            .unwrap();

        assert_eq!(pattern.pattern, QueryPattern::MutualRelationship);
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_execute_with_routes_optimizes_incoming_count_aggregation() {
        let engine = make_engine();
        let parser = Parser::new();
        seed_social_graph(&engine);

        let cypher = "MATCH (p:Person)<-[:FOLLOWS]-(f:Person) RETURN p.name AS person, count(f) AS followers LIMIT 2";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_routes(&query, &HashMap::new(), &pattern, None, None)
            .unwrap();

        assert_eq!(pattern.pattern, QueryPattern::IncomingCountAgg);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(
            result.rows[0].get("person"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(result.rows[0].get("followers"), Some(&Value::from(3)));
    }

    #[test]
    fn test_execute_with_routes_optimizes_incoming_count_star_aggregation() {
        let engine = make_engine();
        let parser = Parser::new();
        seed_social_graph(&engine);

        let cypher = "MATCH (p:Person)<-[:FOLLOWS]-(f:Person) RETURN p.name AS person, count(*) AS followers LIMIT 2";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_routes(&query, &HashMap::new(), &pattern, None, None)
            .unwrap();

        assert_eq!(pattern.pattern, QueryPattern::IncomingCountAgg);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(
            result.rows[0].get("person"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(result.rows[0].get("followers"), Some(&Value::from(3)));
    }

    #[test]
    fn test_execute_with_routes_optimized_incoming_count_limit_zero_returns_empty() {
        let engine = make_engine();
        let parser = Parser::new();
        seed_social_graph(&engine);

        let cypher = "MATCH (p:Person)<-[:FOLLOWS]-(f:Person) RETURN p.name AS person, count(f) AS followers LIMIT 0";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_routes(&query, &HashMap::new(), &pattern, None, None)
            .unwrap();

        assert_eq!(pattern.pattern, QueryPattern::IncomingCountAgg);
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_execute_with_routes_optimizes_untyped_incoming_count_aggregation() {
        let engine = make_engine();
        let parser = Parser::new();
        seed_social_graph(&engine);

        let cypher = "MATCH (p:Person)<-[r]-(f:Person) RETURN p.name AS person, count(f) AS followers LIMIT 2";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_routes(&query, &HashMap::new(), &pattern, None, None)
            .unwrap();

        assert_eq!(pattern.pattern, QueryPattern::IncomingCountAgg);
        assert_eq!(pattern.rel_type, "");
        assert_eq!(result.rows.len(), 2);
        assert_eq!(
            result.rows[0].get("person"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(result.rows[0].get("followers"), Some(&Value::from(3)));
    }

    #[test]
    fn test_execute_with_routes_optimizes_outgoing_count_aggregation() {
        let engine = make_engine();
        let parser = Parser::new();
        seed_social_graph(&engine);

        let cypher = "MATCH (p:Person)-[:FOLLOWS]->(f:Person) RETURN p.name AS person, count(f) AS following LIMIT 2";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_routes(&query, &HashMap::new(), &pattern, None, None)
            .unwrap();

        assert_eq!(pattern.pattern, QueryPattern::OutgoingCountAgg);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(
            result.rows[0].get("person"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(result.rows[0].get("following"), Some(&Value::from(2)));
    }

    #[test]
    fn test_execute_with_routes_optimizes_untyped_edge_property_aggregation() {
        let engine = make_engine();
        let parser = Parser::new();
        seed_review_graph(&engine);

        let cypher = "MATCH (c)-[r]->(p) RETURN p.name AS product, avg(r.rating) AS avgRating, count(r) AS reviewCount ORDER BY avgRating DESC LIMIT 2";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_routes(&query, &HashMap::new(), &pattern, None, None)
            .unwrap();

        assert_eq!(pattern.pattern, QueryPattern::EdgePropertyAgg);
        assert_eq!(pattern.rel_type, "");
        assert_eq!(result.rows.len(), 2);
        assert_eq!(
            result.rows[0].get("product"),
            Some(&Value::String("Thing".into()))
        );
        assert_eq!(result.rows[0].get("reviewCount"), Some(&Value::from(1)));
        assert_eq!(
            result.rows[1].get("product"),
            Some(&Value::String("Widget".into()))
        );
        assert_eq!(result.rows[1].get("reviewCount"), Some(&Value::from(3)));
    }

    #[test]
    fn test_execute_with_routes_edge_property_aggregation_on_empty_db_returns_no_rows() {
        let engine = make_engine();
        let parser = Parser::new();

        let cypher = "MATCH (c)-[r]->(p) RETURN p.name AS product, avg(r.rating) AS avgRating";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);

        let result = engine
            .execute_with_routes(&query, &HashMap::new(), &pattern, None, None)
            .unwrap();

        assert_eq!(pattern.pattern, QueryPattern::EdgePropertyAgg);
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_execute_with_routes_uses_compound_shape_fast_path() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (p1:Person {id: 1})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (p2:Person {id: 2})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let cypher = "MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:TEMP_KNOWS]->(p2) WITH r DELETE r RETURN count(r)";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);
        let (shape_match, ok) = match_compound_query_shape(cypher);

        let result = engine
            .execute_with_routes(
                &query,
                &HashMap::new(),
                &pattern,
                ok.then_some(&shape_match),
                None,
            )
            .unwrap();

        assert!(ok);
        assert_eq!(result.columns, vec!["count(r)"]);
        assert_eq!(
            result.rows,
            vec![HashMap::from([("count(r)".into(), Value::from(1))])]
        );
        assert_eq!(result.stats.relationships_created, 1);
        assert_eq!(result.stats.relationships_deleted, 1);
        assert!(engine
            .storage
            .get_edges_by_type("TEMP_KNOWS")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_execute_with_routes_compound_fast_path_limit_zero_is_noop() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (a:Actor {name: 'Alice'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser.parse("CREATE (m:Movie {title: 'Matrix'})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let cypher =
            "MATCH (a:Actor), (m:Movie) WITH a, m LIMIT 0 CREATE (a)-[r:TEMP_REL]->(m) DELETE r";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);
        let (shape_match, ok) = match_compound_query_shape(cypher);

        let result = engine
            .execute_with_routes(
                &query,
                &HashMap::new(),
                &pattern,
                ok.then_some(&shape_match),
                None,
            )
            .unwrap();

        assert!(ok);
        assert!(result.columns.is_empty());
        assert!(result.rows.is_empty());
        assert_eq!(result.stats.relationships_created, 0);
        assert_eq!(result.stats.relationships_deleted, 0);
        assert!(engine
            .storage
            .get_edges_by_type("TEMP_REL")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_execute_with_routes_compound_fast_path_property_miss_falls_back_cleanly() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser.parse("CREATE (p1:Person {id: 1})").unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let cypher = "MATCH (p1:Person {id: 1}), (p2:Person {id: 999}) CREATE (p1)-[r:TEMP_KNOWS]->(p2) DELETE r";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);
        let (shape_match, ok) = match_compound_query_shape(cypher);

        let result = engine
            .execute_with_routes(
                &query,
                &HashMap::new(),
                &pattern,
                ok.then_some(&shape_match),
                None,
            )
            .unwrap();

        assert!(ok);
        assert!(result.rows.is_empty());
        assert_eq!(result.stats.relationships_created, 0);
        assert_eq!(result.stats.relationships_deleted, 0);
        assert!(engine
            .storage
            .get_edges_by_type("TEMP_KNOWS")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_execute_with_routes_compound_property_match_fast_path() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser
                    .parse("CREATE (p1:Person {id: 1, name: 'Alice'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse("CREATE (p2:Person {id: 2, name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let cypher = "MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:TEMP_KNOWS]->(p2) DELETE r";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);
        let (shape_match, ok) = match_compound_query_shape(cypher);

        let result = engine
            .execute_with_routes(
                &query,
                &HashMap::new(),
                &pattern,
                ok.then_some(&shape_match),
                None,
            )
            .unwrap();

        assert!(ok);
        assert!(result.rows.is_empty());
        assert_eq!(result.stats.relationships_created, 1);
        assert_eq!(result.stats.relationships_deleted, 1);
        assert!(engine
            .storage
            .get_edges_by_type("TEMP_KNOWS")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_execute_with_routes_uses_pipeline_hook() {
        let engine = make_engine();
        let parser = Parser::new();
        let cypher = "WITH [1, 2] AS values UNWIND values AS value RETURN value";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);
        let (clauses, ok) = can_execute_as_pipeline(cypher);

        let result = engine
            .execute_with_routes(
                &query,
                &HashMap::new(),
                &pattern,
                None,
                ok.then_some(clauses.as_slice()),
            )
            .unwrap();

        assert!(ok);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("value"), Some(&Value::from(1)));
        assert_eq!(result.rows[1].get("value"), Some(&Value::from(2)));
    }

    #[test]
    fn test_execute_with_routes_pipeline_create_reuses_bound_nodes() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (c:Customer {customerID: 1, name: 'Ada'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let cypher = "MATCH (c:Customer {customerID: 1}) CREATE (o:Order {orderID: 9001}) CREATE (c)-[:PURCHASED]->(o) WITH c, o RETURN c.customerID AS customerID, o.orderID AS orderID";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);
        let (clauses, ok) = can_execute_as_pipeline(cypher);

        let result = engine
            .execute_with_routes(
                &query,
                &HashMap::new(),
                &pattern,
                None,
                ok.then_some(clauses.as_slice()),
            )
            .unwrap();

        assert!(ok);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("customerID"), Some(&Value::from(1)));
        assert_eq!(result.rows[0].get("orderID"), Some(&Value::from(9001)));
        assert_eq!(result.stats.nodes_created, 1);
        assert_eq!(result.stats.relationships_created, 1);

        let edges = engine.storage.get_edges_by_type("PURCHASED").unwrap();
        assert_eq!(edges.len(), 1);

        let start_raw = engine
            .storage
            .get_node_record(&edges[0].start_node)
            .unwrap()
            .expect("customer node should exist");
        let start_props = node_record_to_props(&start_raw);
        assert_eq!(start_props.get("customerID"), Some(&Value::from(1)));

        let end_raw = engine
            .storage
            .get_node_record(&edges[0].end_node)
            .unwrap()
            .expect("order node should exist");
        let end_props = node_record_to_props(&end_raw);
        assert_eq!(end_props.get("orderID"), Some(&Value::from(9001)));
    }

    #[test]
    fn test_execute_with_routes_pipeline_match_respects_bound_relationship_endpoints() {
        let engine = make_engine();
        let parser = Parser::new();

        for cypher in [
            "CREATE (c:Customer {customerID: 1, name: 'Ada'})",
            "CREATE (c:Customer {customerID: 2, name: 'Bob'})",
            "CREATE (o:Order {orderID: 100})",
            "CREATE (o:Order {orderID: 200})",
        ] {
            engine
                .execute(&parser.parse(cypher).unwrap(), &HashMap::new())
                .unwrap();
        }

        let node_id_for = |label: &str, property: &str, expected: i64| {
            engine
                .storage
                .get_nodes_by_label(label)
                .expect("label lookup should succeed")
                .into_iter()
                .find_map(|node| {
                    let props = node_record_to_props(&node);
                    (props.get(property) == Some(&Value::from(expected))).then(|| node.id)
                })
                .expect("expected seeded node")
        };

        engine
            .storage
            .put_edge_record(&EdgeRecord {
                id: "purchased:1".into(),
                start_node: node_id_for("Customer", "customerID", 1),
                end_node: node_id_for("Order", "orderID", 100),
                edge_type: "PURCHASED".into(),
                properties: HashMap::new().into_iter().collect(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();
        engine
            .storage
            .put_edge_record(&EdgeRecord {
                id: "purchased:2".into(),
                start_node: node_id_for("Customer", "customerID", 2),
                end_node: node_id_for("Order", "orderID", 200),
                edge_type: "PURCHASED".into(),
                properties: HashMap::new().into_iter().collect(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let cypher = "MATCH (c:Customer {customerID: 1}) WITH c MATCH (c)-[:PURCHASED]->(o:Order) RETURN o.orderID AS orderID";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);
        let (clauses, ok) = can_execute_as_pipeline(cypher);

        let result = engine
            .execute_with_routes(
                &query,
                &HashMap::new(),
                &pattern,
                None,
                ok.then_some(clauses.as_slice()),
            )
            .unwrap();

        assert!(ok);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("orderID"), Some(&Value::from(100)));
    }

    #[test]
    fn test_execute_with_routes_pipeline_seeder_shape_supports_multiple_rows_and_edge_properties() {
        let engine = make_engine();
        let parser = Parser::new();

        for cypher in [
            "CREATE (c:Customer {customerID: 1, companyName: 'C1'})",
            "CREATE (p:Product {productID: 1, productName: 'P1'})",
            "CREATE (p:Product {productID: 2, productName: 'P2'})",
        ] {
            engine
                .execute(&parser.parse(cypher).unwrap(), &HashMap::new())
                .unwrap();
        }

        let cypher = "MATCH (c:Customer {customerID: 1}) CREATE (o:Order {orderID: 9001}) CREATE (c)-[:PURCHASED]->(o) WITH o, {} UNWIND [{productID: 1, quantity: 3}, {productID: 2, quantity: 5}] AS prodRef MATCH (p:Product {productID: prodRef.productID}) CREATE (o)-[:ORDERS {quantity: prodRef.quantity}]->(p)";
        let query = parser.parse(cypher).unwrap();
        let pattern = detect_query_pattern(cypher);
        let (clauses, ok) = can_execute_as_pipeline(cypher);

        let result = engine
            .execute_with_routes(
                &query,
                &HashMap::new(),
                &pattern,
                None,
                ok.then_some(clauses.as_slice()),
            )
            .unwrap();

        assert!(ok);
        assert_eq!(result.stats.nodes_created, 1);
        assert_eq!(result.stats.relationships_created, 3);

        let purchased = engine.storage.get_edges_by_type("PURCHASED").unwrap();
        assert_eq!(purchased.len(), 1);
        let orders = engine.storage.get_edges_by_type("ORDERS").unwrap();
        assert_eq!(orders.len(), 2);

        let mut quantities: Vec<i64> = orders
            .iter()
            .filter_map(|edge| edge.properties.get("quantity").and_then(Value::as_i64))
            .collect();
        quantities.sort_unstable();
        assert_eq!(quantities, vec![3, 5]);

        let order_ids: HashSet<String> =
            orders.iter().map(|edge| edge.start_node.clone()).collect();
        assert_eq!(order_ids.len(), 1);
        assert!(order_ids.contains(&purchased[0].end_node));

        let product_ids: HashSet<i64> = orders
            .iter()
            .filter_map(|edge| {
                let node = engine.storage.get_node_record(&edge.end_node).ok().flatten()?;
                let props = node_record_to_props(&node);
                props.get("productID").and_then(Value::as_i64)
            })
            .collect();
        assert_eq!(product_ids, HashSet::from([1, 2]));
    }

    #[test]
    fn test_execute_pipeline_routed_direct_invocation_for_seeder_shape() {
        let engine = make_engine();
        let parser = Parser::new();

        for cypher in [
            "CREATE (c:Customer {customerID: 1, companyName: 'C1'})",
            "CREATE (p:Product {productID: 1, productName: 'P1'})",
            "CREATE (p:Product {productID: 2, productName: 'P2'})",
        ] {
            engine
                .execute(&parser.parse(cypher).unwrap(), &HashMap::new())
                .unwrap();
        }

        let cypher = "MATCH (c:Customer {customerID: 1}) CREATE (o:Order {orderID: 9001}) CREATE (c)-[:PURCHASED]->(o) WITH o, {} UNWIND [{productID: 1, quantity: 3}, {productID: 2, quantity: 5}] AS prodRef MATCH (p:Product {productID: prodRef.productID}) CREATE (o)-[:ORDERS {quantity: prodRef.quantity}]->(p)";
        let query = parser.parse(cypher).unwrap();
        let (clauses, ok) = can_execute_as_pipeline(cypher);

        assert!(ok);
        assert!(engine.can_execute_pipeline_route(&query, &clauses));

        let result = engine
            .execute_pipeline_routed(&query, &HashMap::new(), clauses.as_slice())
            .unwrap();

        assert_eq!(result.stats.nodes_created, 1);
        assert_eq!(result.stats.relationships_created, 3);

        let purchased = engine.storage.get_edges_by_type("PURCHASED").unwrap();
        assert_eq!(purchased.len(), 1);
        let orders = engine.storage.get_edges_by_type("ORDERS").unwrap();
        assert_eq!(orders.len(), 2);

        let mut quantities: Vec<i64> = orders
            .iter()
            .filter_map(|edge| edge.properties.get("quantity").and_then(Value::as_i64))
            .collect();
        quantities.sort_unstable();
        assert_eq!(quantities, vec![3, 5]);

        let order_ids: HashSet<String> =
            orders.iter().map(|edge| edge.start_node.clone()).collect();
        assert_eq!(order_ids.len(), 1);
        assert!(order_ids.contains(&purchased[0].end_node));

        let product_ids: HashSet<i64> = orders
            .iter()
            .filter_map(|edge| {
                let node = engine.storage.get_node_record(&edge.end_node).ok().flatten()?;
                let props = node_record_to_props(&node);
                props.get("productID").and_then(Value::as_i64)
            })
            .collect();
        assert_eq!(product_ids, HashSet::from([1, 2]));
    }

    #[test]
    fn test_optional_match_relationship_pattern_preserves_row_with_nulls() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (p:Person {id: 1, name: 'Alice'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let result = engine
            .execute(
                &parser
                    .parse(
                        "MATCH (p:Person {id: 1}) OPTIONAL MATCH (p)-[r:FOLLOWS]->(friend:Person) RETURN p.name AS person, friend AS friend, r AS rel",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["person", "friend", "rel"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("person"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(result.rows[0].get("friend"), Some(&Value::Null));
        assert_eq!(result.rows[0].get("rel"), Some(&Value::Null));
    }

    #[test]
    fn test_optional_match_relationship_pattern_returns_bound_values_on_match() {
        let engine = make_engine();
        let parser = Parser::new();

        for cypher in [
            "CREATE (p:Person {id: 1, name: 'Alice'})",
            "CREATE (p:Person {id: 2, name: 'Bob'})",
            "MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:FOLLOWS]->(b)",
        ] {
            engine
                .execute(&parser.parse(cypher).unwrap(), &HashMap::new())
                .unwrap();
        }

        let result = engine
            .execute(
                &parser
                    .parse(
                        "MATCH (p:Person {id: 1}) OPTIONAL MATCH (p)-[r:FOLLOWS]->(friend:Person) RETURN p.name AS person, friend.name AS friendName, r._type AS relType",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        assert_eq!(result.columns, vec!["person", "friendName", "relType"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("person"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(
            result.rows[0].get("friendName"),
            Some(&Value::String("Bob".into()))
        );
        assert_eq!(
            result.rows[0].get("relType"),
            Some(&Value::String("FOLLOWS".into()))
        );
    }

    #[test]
    fn test_optional_match_single_node_path_variable_hit_and_miss() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse("CREATE (:Seed {id: 1}), (:Person {name: 'Alice'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let hit = parser
            .parse(
                "MATCH (s:Seed {id: 1}) OPTIONAL MATCH p = (n:Person {name: 'Alice'}) RETURN p AS path, nodes(p) AS nodes, relationships(p) AS rels, length(p) AS hops",
            )
            .unwrap();
        let miss = parser
            .parse(
                "MATCH (s:Seed {id: 1}) OPTIONAL MATCH p = (n:Person {name: 'Bob'}) RETURN p AS path, nodes(p) AS nodes, relationships(p) AS rels, length(p) AS hops",
            )
            .unwrap();

        let hit_result = engine.execute(&hit, &HashMap::new()).unwrap();
        let miss_result = engine.execute(&miss, &HashMap::new()).unwrap();

        assert_eq!(hit_result.rows.len(), 1);
        assert_eq!(hit_result.rows[0].get("hops"), Some(&Value::from(0)));
        let hit_nodes = hit_result.rows[0]
            .get("nodes")
            .and_then(Value::as_array)
            .expect("expected optional hit nodes");
        assert_eq!(hit_nodes.len(), 1);
        assert_eq!(
            hit_result.rows[0].get("rels"),
            Some(&Value::Array(Vec::new()))
        );

        assert_eq!(miss_result.rows.len(), 1);
        assert_eq!(miss_result.rows[0].get("path"), Some(&Value::Null));
        assert_eq!(miss_result.rows[0].get("hops"), Some(&Value::Null));
        assert_eq!(
            miss_result.rows[0].get("nodes"),
            Some(&Value::Array(Vec::new()))
        );
        assert_eq!(
            miss_result.rows[0].get("rels"),
            Some(&Value::Array(Vec::new()))
        );
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
    fn test_match_with_edge_pattern_uses_adjacency_index() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser
                    .parse("CREATE (a:Person {name: 'Alice'})-[r:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let q = parser
            .parse("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS a, r.since AS since, b.name AS b")
            .unwrap();
        let result = engine.execute(&q, &HashMap::new()).unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("a"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(result.rows[0].get("since"), Some(&Value::from(2020)));
        assert_eq!(result.rows[0].get("b"), Some(&Value::String("Bob".into())));
    }

    #[test]
    fn test_match_incoming_and_undirected_relationship_patterns() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser
                    .parse("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let incoming = parser
            .parse("MATCH (b:Person {name: 'Bob'})<-[r:KNOWS]-(a:Person) RETURN a.name AS name")
            .unwrap();
        let undirected = parser
            .parse("MATCH (b:Person {name: 'Bob'})-[r:KNOWS]-(a:Person) RETURN a.name AS name")
            .unwrap();

        let incoming_result = engine.execute(&incoming, &HashMap::new()).unwrap();
        let undirected_result = engine.execute(&undirected, &HashMap::new()).unwrap();

        assert_eq!(incoming_result.rows.len(), 1);
        assert_eq!(undirected_result.rows.len(), 1);
        assert_eq!(
            incoming_result.rows[0].get("name"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(
            undirected_result.rows[0].get("name"),
            Some(&Value::String("Alice".into()))
        );
    }

    #[test]
    fn test_match_variable_length_relationship_uses_bfs() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser
                    .parse("CREATE (a:Node {name: 'a'})-[:LINK]->(b:Node {name: 'b'})-[:LINK]->(c:Node {name: 'c'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let query = parser
            .parse("MATCH (a:Node {name: 'a'})-[:LINK*1..2]->(n:Node) RETURN n.name AS name")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        let mut names = result
            .rows
            .iter()
            .filter_map(|row| row.get("name").and_then(Value::as_str))
            .collect::<Vec<_>>();
        names.sort();

        assert_eq!(names, vec!["b", "c"]);
    }

    #[test]
    fn test_match_variable_length_exact_hops_and_edge_list_binding() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser
                    .parse("CREATE (a:Node {name: 'a'})-[:LINK {rank: 1}]->(b:Node {name: 'b'})-[:LINK {rank: 2}]->(c:Node {name: 'c'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let query = parser
            .parse(
                "MATCH (a:Node {name: 'a'})-[r:LINK*2]->(n:Node) RETURN n.name AS name, r AS rels",
            )
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("name"), Some(&Value::String("c".into())));
        let rels = result.rows[0]
            .get("rels")
            .and_then(Value::as_array)
            .expect("expected relationship list binding");
        assert_eq!(rels.len(), 2);
    }

    #[test]
    fn test_match_variable_length_path_variable_materializes_path_accessors() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser
                    .parse("CREATE (a:Node {name: 'a'})-[:LINK {rank: 1}]->(b:Node {name: 'b'})-[:LINK {rank: 2}]->(c:Node {name: 'c'})")
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let query = parser
            .parse(
                "MATCH p = (a:Node {name: 'a'})-[r:LINK*2]->(n:Node) RETURN p AS path, nodes(p) AS nodes, relationships(p) AS rels, length(p) AS hops",
            )
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("hops"), Some(&Value::from(2)));

        let nodes = result.rows[0]
            .get("nodes")
            .and_then(Value::as_array)
            .expect("expected path nodes");
        assert_eq!(nodes.len(), 3);
        assert_eq!(nodes[0].get("name").and_then(Value::as_str), Some("a"));
        assert_eq!(nodes[2].get("name").and_then(Value::as_str), Some("c"));

        let rels = result.rows[0]
            .get("rels")
            .and_then(Value::as_array)
            .expect("expected path relationships");
        assert_eq!(rels.len(), 2);
        assert_eq!(rels[0].get("rank").and_then(Value::as_i64), Some(1));

        let path = result.rows[0]
            .get("path")
            .and_then(Value::as_object)
            .expect("expected path value");
        assert_eq!(path.get("length"), Some(&Value::from(2)));
    }

    #[test]
    fn test_match_shortest_path_returns_single_shortest_bfs_path() {
        let engine = make_engine();
        let parser = Parser::new();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE (a:Node {name: 'a'}), (b:Node {name: 'b'}), (c:Node {name: 'c'}), (d:Node {name: 'd'}), (e:Node {name: 'e'})",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "MATCH (a:Node {name: 'a'}), (b:Node {name: 'b'}), (c:Node {name: 'c'}), (d:Node {name: 'd'}), (e:Node {name: 'e'}) CREATE (a)-[:LINK]->(b), (b)-[:LINK]->(d), (a)-[:LINK]->(c), (c)-[:LINK]->(e), (e)-[:LINK]->(d)",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let query = parser
            .parse(
                "MATCH p = shortestPath((a:Node {name: 'a'})-[:LINK*]->(d:Node {name: 'd'})) RETURN length(p) AS hops, nodes(p) AS nodes",
            )
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("hops"), Some(&Value::from(2)));
        let nodes = result.rows[0]
            .get("nodes")
            .and_then(Value::as_array)
            .expect("expected shortest path nodes");
        let names = nodes
            .iter()
            .map(|node| {
                node.get("name")
                    .and_then(Value::as_str)
                    .expect("expected node name")
            })
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["a", "b", "d"]);
    }

    #[test]
    fn test_create_path_variable_materializes_path_accessors() {
        let engine = make_engine();
        let parser = Parser::new();

        let create = parser
            .parse(
                "CREATE p = (a:Node {name: 'a'})-[:LINK {rank: 1}]->(b:Node {name: 'b'}) RETURN p AS path, nodes(p) AS nodes, relationships(p) AS rels, length(p) AS hops",
            )
            .unwrap();
        let result = engine.execute(&create, &HashMap::new()).unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("hops"), Some(&Value::from(1)));

        let nodes = result.rows[0]
            .get("nodes")
            .and_then(Value::as_array)
            .expect("expected path nodes");
        assert_eq!(nodes.len(), 2);

        let rels = result.rows[0]
            .get("rels")
            .and_then(Value::as_array)
            .expect("expected path relationships");
        assert_eq!(rels.len(), 1);
        assert_eq!(rels[0].get("rank").and_then(Value::as_i64), Some(1));

        let path = result.rows[0]
            .get("path")
            .and_then(Value::as_object)
            .expect("expected path value");
        assert_eq!(path.get("length"), Some(&Value::from(1)));
    }

    #[test]
    fn test_match_variable_length_relationship_large_chain_consistency() {
        let engine = make_engine();
        let parser = Parser::new();

        for index in 0..25 {
            let mut props = HashMap::new();
            props.insert("_id".to_string(), Value::String(format!("Node:{index}")));
            props.insert(
                "_labels".to_string(),
                Value::Array(vec![Value::String("Node".into())]),
            );
            props.insert("name".to_string(), Value::String(format!("n{index:02}")));
            store_node(
                engine.storage.as_ref(),
                &format!("Node:{index}"),
                &["Node"],
                props,
            );
        }

        for index in 0..24 {
            engine
                .storage
                .put_edge_record(&EdgeRecord {
                    id: format!("link:{index}"),
                    start_node: format!("Node:{index}"),
                    end_node: format!("Node:{}", index + 1),
                    edge_type: "LINK".into(),
                    properties: BTreeMap::new(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })
                .unwrap();
        }

        let query = parser
            .parse("MATCH (a:Node {name: 'n00'})-[:LINK*1..24]->(n:Node) RETURN n.name AS name")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        let mut names = result
            .rows
            .iter()
            .filter_map(|row| row.get("name").and_then(Value::as_str))
            .map(str::to_string)
            .collect::<Vec<_>>();
        names.sort();

        let expected = (1..25)
            .map(|index| format!("n{index:02}"))
            .collect::<Vec<_>>();
        assert_eq!(names, expected);
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
            store_node(storage.as_ref(), "Person:alice-id", &["Person", "Employee"], props);
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
            store_node(storage.as_ref(), "Person:bob-id", &["Person"], props);
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

    #[test]
    fn test_unwind_list_literal_returns_rows() {
        let engine = make_engine();
        let parser = Parser::new();
        let query = parser
            .parse("UNWIND [1, 2, 3] AS value RETURN value")
            .unwrap();

        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.columns, vec!["value"]);
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0].get("value"), Some(&Value::from(1)));
        assert_eq!(result.rows[1].get("value"), Some(&Value::from(2)));
        assert_eq!(result.rows[2].get("value"), Some(&Value::from(3)));
    }

    #[test]
    fn test_unwind_map_literal_returns_projected_properties() {
        let engine = make_engine();
        let parser = Parser::new();
        let query = parser
            .parse("UNWIND [{name: 'Ada'}, {name: 'Linus'}] AS row RETURN row.name AS name")
            .unwrap();

        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.columns, vec!["name"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("name"), Some(&Value::from("Ada")));
        assert_eq!(result.rows[1].get("name"), Some(&Value::from("Linus")));
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
        assert!(err.to_string().contains("index not found: missing_idx"));

        engine
            .execute(
                &parser.parse("DROP INDEX missing_idx IF EXISTS").unwrap(),
                &HashMap::new(),
            )
            .unwrap();
    }

    #[test]
    fn test_knowledge_policy_decay_profile_ddl_roundtrip() {
        let engine = make_engine();
        let parser = Parser::new();

        let create = parser
            .parse(
                "CREATE DECAY PROFILE slow_decay OPTIONS { halfLifeSeconds: 604800, visibilityThreshold: 0.1, scoreFloor: 0.0, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
            )
            .unwrap();
        engine.execute(&create, &HashMap::new()).unwrap();

        let show = parser.parse("SHOW DECAY PROFILES").unwrap();
        let shown = engine.execute(&show, &HashMap::new()).unwrap();
        assert_eq!(shown.rows.len(), 1);
        assert_eq!(shown.rows[0].get("kind"), Some(&Value::String("bundle".to_string())));
        assert_eq!(
            shown.rows[0].get("name"),
            Some(&Value::String("slow_decay".to_string()))
        );

        let alter = parser
            .parse("ALTER DECAY PROFILE slow_decay SET OPTIONS { visibilityThreshold: 0.2 }")
            .unwrap();
        engine.execute(&alter, &HashMap::new()).unwrap();
        let shown = engine.execute(&show, &HashMap::new()).unwrap();
        assert_eq!(
            shown.rows[0].get("enabled"),
            Some(&Value::Bool(true))
        );

        let profiles = engine.storage.load_decay_profile_schemas().unwrap();
        assert_eq!(profiles[0].visibility_threshold, 0.2);
    }

    #[test]
    fn test_knowledge_policy_decay_binding_ddl_roundtrip() {
        let engine = make_engine();
        let parser = Parser::new();

        let create_bundle = parser
            .parse(
                "CREATE DECAY PROFILE slow_decay OPTIONS { halfLifeSeconds: 604800, visibilityThreshold: 0.1, scoreFloor: 0.0, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
            )
            .unwrap();
        engine.execute(&create_bundle, &HashMap::new()).unwrap();

        let create_binding = parser
            .parse(
                "CREATE DECAY PROFILE memory_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE slow_decay, visibilityThreshold: 0.2, order: 10 }",
            )
            .unwrap();
        engine.execute(&create_binding, &HashMap::new()).unwrap();

        let show = parser.parse("SHOW DECAY PROFILES").unwrap();
        let shown = engine.execute(&show, &HashMap::new()).unwrap();
        assert_eq!(shown.rows.len(), 2);

        let binding_row = shown
            .rows
            .iter()
            .find(|row| row.get("kind") == Some(&Value::String("binding".to_string())))
            .expect("binding row missing");
        assert_eq!(
            binding_row.get("name"),
            Some(&Value::String("memory_binding".to_string()))
        );
        assert_eq!(
            binding_row.get("target"),
            Some(&Value::String("MemoryEpisode".to_string()))
        );
        assert_eq!(
            binding_row.get("profileRef"),
            Some(&Value::String("slow_decay".to_string()))
        );

        let stored = engine.storage.load_decay_profile_binding_schemas().unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].order, 10);

        let drop_binding = parser.parse("DROP DECAY PROFILE memory_binding").unwrap();
        engine.execute(&drop_binding, &HashMap::new()).unwrap();
        assert!(engine.storage.load_decay_profile_binding_schemas().unwrap().is_empty());
    }

    #[test]
    fn test_knowledge_policy_resolver_builds_from_persisted_catalog() {
        let engine = make_engine();
        let parser = Parser::new();

        let create_bundle = parser
            .parse(
                "CREATE DECAY PROFILE slow_decay OPTIONS { halfLifeSeconds: 604800, visibilityThreshold: 0.1, scoreFloor: 0.0, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
            )
            .unwrap();
        engine.execute(&create_bundle, &HashMap::new()).unwrap();

        let create_binding = parser
            .parse(
                "CREATE DECAY PROFILE memory_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE slow_decay, visibilityThreshold: 0.2, order: 10 }",
            )
            .unwrap();
        engine.execute(&create_binding, &HashMap::new()).unwrap();

        let resolver = engine.knowledge_policy_resolver().unwrap();
        let resolved = resolver
            .resolve_node(&["MemoryEpisode".to_string()])
            .expect("binding should resolve");

        assert_eq!(resolved.decay_binding.name, "memory_binding");
        assert_eq!(resolved.decay_profile.as_ref().map(|profile| profile.name.as_str()), Some("slow_decay"));
        assert_eq!(resolved.visibility_threshold, 0.2);
    }

    #[test]
    fn test_match_hides_nodes_suppressed_by_created_age_threshold() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE short_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.5, scoreFloor: 0.0, function: 'step', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE memory_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE short_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "memory:old".to_string(),
                labels: vec!["MemoryEpisode".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Old memory".to_string()))]),
                created_at_unix_ms: now_unix_ms() - 5_000,
                updated_at_unix_ms: now_unix_ms() - 5_000,
            })
            .unwrap();

        let query = parser
            .parse("MATCH (n:MemoryEpisode) RETURN n")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_match_keeps_fresh_nodes_visible_under_created_age_threshold() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE short_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.5, scoreFloor: 0.0, function: 'step', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE memory_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE short_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "memory:fresh".to_string(),
                labels: vec!["MemoryEpisode".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Fresh memory".to_string()))]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .unwrap();

        let query = parser
            .parse("MATCH (n:MemoryEpisode) RETURN n")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0]
                .get("n")
                .and_then(Value::as_object)
                .and_then(|props| props.get("name")),
            Some(&Value::String("Fresh memory".to_string()))
        );
    }

    #[test]
    fn test_match_hides_edges_suppressed_by_created_age_threshold() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE short_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.5, scoreFloor: 0.0, function: 'step', scope: 'EDGE', scoreFrom: 'CREATED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE stale_edge_binding FOR ()-[r:LINKS]-() APPLY { DECAY PROFILE short_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        for node in [
            NodeRecord {
                id: "person:a".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
            NodeRecord {
                id: "person:b".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
        ] {
            engine.storage.put_node_record(&node).unwrap();
        }
        engine
            .storage
            .put_edge_record(&EdgeRecord {
                id: "links:1".to_string(),
                start_node: "person:a".to_string(),
                end_node: "person:b".to_string(),
                edge_type: "LINKS".to_string(),
                properties: BTreeMap::from([("kind".to_string(), Value::String("stale".to_string()))]),
                created_at_unix_ms: now - 5_000,
                updated_at_unix_ms: now - 5_000,
            })
            .unwrap();

        let query = parser
            .parse("MATCH (:Person)-[r:LINKS]->(:Person) RETURN r")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_match_hides_nodes_suppressed_by_custom_anchor_threshold() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE review_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.5, scoreFloor: 0.0, function: 'step', scope: 'NODE', scoreFrom: 'CUSTOM', scoreFromProperty: 'reviewedAt', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE reviewed_memory_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE review_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "memory:custom-old".to_string(),
                labels: vec!["MemoryEpisode".to_string()],
                properties: BTreeMap::from([
                    ("name".to_string(), Value::String("Reviewed memory".to_string())),
                    ("reviewedAt".to_string(), Value::from(now - 5_000)),
                ]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .unwrap();

        let query = parser.parse("MATCH (n:MemoryEpisode) RETURN n").unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_match_hides_edges_suppressed_by_custom_anchor_threshold() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE review_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.5, scoreFloor: 0.0, function: 'step', scope: 'EDGE', scoreFrom: 'CUSTOM', scoreFromProperty: 'reviewedAt', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE reviewed_edge_binding FOR ()-[r:LINKS]-() APPLY { DECAY PROFILE review_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        for node in [
            NodeRecord {
                id: "person:custom-a".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
            NodeRecord {
                id: "person:custom-b".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
        ] {
            engine.storage.put_node_record(&node).unwrap();
        }
        engine
            .storage
            .put_edge_record(&EdgeRecord {
                id: "links:custom-1".to_string(),
                start_node: "person:custom-a".to_string(),
                end_node: "person:custom-b".to_string(),
                edge_type: "LINKS".to_string(),
                properties: BTreeMap::from([
                    ("kind".to_string(), Value::String("reviewed".to_string())),
                    ("reviewedAt".to_string(), Value::from(now - 5_000)),
                ]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .unwrap();

        let query = parser
            .parse("MATCH (:Person)-[r:LINKS]->(:Person) RETURN r")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_match_keeps_stale_nodes_visible_under_recent_last_access_anchor() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE access_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.5, scoreFloor: 0.0, function: 'step', scope: 'NODE', scoreFrom: 'LAST_ACCESSED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE access_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE access_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let stale_time = now_unix_ms() - 5_000;
        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "memory:recent-access".to_string(),
                labels: vec!["MemoryEpisode".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Recently accessed memory".to_string()))]),
                created_at_unix_ms: stale_time,
                updated_at_unix_ms: stale_time,
            })
            .unwrap();
        engine
            .storage
            .put_knowledge_policy_access_metadata(
                "memory:recent-access",
                &copperdb_storage::KnowledgePolicyAccessMetadata {
                    last_accessed_at_unix_ms: Some(now_unix_ms()),
                    access_count: 1,
                },
            )
            .unwrap();

        let query = parser.parse("MATCH (n:MemoryEpisode) RETURN n").unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_match_keeps_stale_edges_visible_under_recent_last_access_anchor() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE access_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.5, scoreFloor: 0.0, function: 'step', scope: 'EDGE', scoreFrom: 'LAST_ACCESSED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE access_binding FOR ()-[r:LINKS]-() APPLY { DECAY PROFILE access_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        for node in [
            NodeRecord {
                id: "person:access-a".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
            NodeRecord {
                id: "person:access-b".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
        ] {
            engine.storage.put_node_record(&node).unwrap();
        }

        let stale_time = now - 5_000;
        engine
            .storage
            .put_edge_record(&EdgeRecord {
                id: "links:recent-access".to_string(),
                start_node: "person:access-a".to_string(),
                end_node: "person:access-b".to_string(),
                edge_type: "LINKS".to_string(),
                properties: BTreeMap::from([("kind".to_string(), Value::String("recently-accessed".to_string()))]),
                created_at_unix_ms: stale_time,
                updated_at_unix_ms: stale_time,
            })
            .unwrap();
        engine
            .storage
            .put_knowledge_policy_access_metadata(
                "links:recent-access",
                &copperdb_storage::KnowledgePolicyAccessMetadata {
                    last_accessed_at_unix_ms: Some(now_unix_ms()),
                    access_count: 2,
                },
            )
            .unwrap();

        let query = parser
            .parse("MATCH (:Person)-[r:LINKS]->(:Person) RETURN r")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_persist_node_props_refreshes_version_anchor_visibility_for_nodes() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE version_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.5, scoreFloor: 0.0, function: 'step', scope: 'NODE', scoreFrom: 'VERSION', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE version_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE version_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let stale_time = now_unix_ms() - 5_000;
        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "memory:versioned".to_string(),
                labels: vec!["MemoryEpisode".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Versioned memory".to_string()))]),
                created_at_unix_ms: stale_time,
                updated_at_unix_ms: stale_time,
            })
            .unwrap();

        let match_query = parser.parse("MATCH (n:MemoryEpisode) RETURN n").unwrap();
        let hidden = engine.execute(&match_query, &HashMap::new()).unwrap();
        assert!(hidden.rows.is_empty());

        let mut refreshed_props = node_record_to_props(
            &engine
                .storage
                .get_node_record("memory:versioned")
                .unwrap()
                .unwrap(),
        );
        refreshed_props.insert("status".to_string(), Value::String("fresh".to_string()));
        engine.persist_node_props(&refreshed_props).unwrap();

        let shown = engine.execute(&match_query, &HashMap::new()).unwrap();
        assert_eq!(shown.rows.len(), 1);
        assert_eq!(
            shown.rows[0]
                .get("n")
                .and_then(Value::as_object)
                .and_then(|props| props.get("status")),
            Some(&Value::String("fresh".to_string()))
        );

        let stored = engine.storage.get_node_record("memory:versioned").unwrap().unwrap();
        assert!(stored.updated_at_unix_ms > stale_time);
        assert_eq!(stored.created_at_unix_ms, stale_time);
    }

    #[test]
    fn test_create_keeps_fresh_edges_visible_under_version_anchor() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE version_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.5, scoreFloor: 0.0, function: 'step', scope: 'EDGE', scoreFrom: 'VERSION', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE version_binding FOR ()-[r:LINKS]-() APPLY { DECAY PROFILE version_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let create_query = parser
            .parse(
                "CREATE (a:Person { id: 'person:version-a', name: 'Alice' })-[:LINKS { kind: 'fresh' }]->(b:Person { id: 'person:version-b', name: 'Bob' })",
            )
            .unwrap();
        engine.execute(&create_query, &HashMap::new()).unwrap();

        let match_query = parser
            .parse("MATCH (:Person)-[r:LINKS]->(:Person) RETURN r")
            .unwrap();
        let visible = engine.execute(&match_query, &HashMap::new()).unwrap();
        assert_eq!(visible.rows.len(), 1);

        let edge_id = visible.rows[0]
            .get("r")
            .and_then(Value::as_object)
            .and_then(|props| props.get("_id"))
            .and_then(Value::as_str)
            .expect("edge id should be present")
            .to_string();
        let stored = engine.storage.get_edge_record(&edge_id).unwrap().unwrap();
        assert!(stored.updated_at_unix_ms > 0);
        assert!(stored.created_at_unix_ms > 0);
    }

    #[test]
    fn test_knowledge_policy_promotion_profile_and_policy_roundtrip() {
        let engine = make_engine();
        let parser = Parser::new();

        let create_profile = parser
            .parse(
                "CREATE PROMOTION PROFILE boost_profile OPTIONS { scope: 'NODE', multiplier: 1.5, scoreFloor: 0.0, scoreCap: 1.0, enabled: true }",
            )
            .unwrap();
        engine.execute(&create_profile, &HashMap::new()).unwrap();

        let create_policy = parser
            .parse(
                "CREATE PROMOTION POLICY fact_policy FOR (n:KnowledgeFact) APPLY { ON ACCESS { SET n.lastAccessedAt = timestamp() } APPLY PROFILE boost_profile WHEN 'n.evidence >= 3' }",
            )
            .unwrap();
        engine.execute(&create_policy, &HashMap::new()).unwrap();

        let show_policies = parser.parse("SHOW PROMOTION POLICIES").unwrap();
        let shown = engine.execute(&show_policies, &HashMap::new()).unwrap();
        assert_eq!(shown.rows.len(), 1);
        assert_eq!(
            shown.rows[0].get("name"),
            Some(&Value::String("fact_policy".to_string()))
        );
        assert_eq!(shown.rows[0].get("isEdge"), Some(&Value::Bool(false)));
        assert_eq!(shown.rows[0].get("enabled"), Some(&Value::Bool(true)));
        assert_eq!(
            shown.rows[0].get("onAccessMutations"),
            Some(&Value::Array(vec![Value::String(
                "SET_LAST_ACCESSED_NOW".to_string(),
            )]))
        );

        let alter_policy = parser
            .parse("ALTER PROMOTION POLICY fact_policy SET ENABLED false")
            .unwrap();
        engine.execute(&alter_policy, &HashMap::new()).unwrap();
        let shown = engine.execute(&show_policies, &HashMap::new()).unwrap();
        assert_eq!(shown.rows[0].get("enabled"), Some(&Value::Bool(false)));
    }

    #[test]
    fn test_call_knowledgepolicy_resolve_by_entity_id_reports_scoring() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE access_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.75, scoreFloor: 0.0, function: 'step', scope: 'NODE', scoreFrom: 'LAST_ACCESSED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE access_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE access_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION PROFILE reinforcement OPTIONS { scope: 'NODE', multiplier: 2.0, scoreFloor: 0.8, scoreCap: 1.0, enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION POLICY reinforcement_policy FOR (n:MemoryEpisode) APPLY PROFILE reinforcement WHEN 'n.accessCount >= 3'",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let stale_time = now_unix_ms() - 5_000;
        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "memory:resolve-1".to_string(),
                labels: vec!["MemoryEpisode".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Inspectable memory".to_string()))]),
                created_at_unix_ms: stale_time,
                updated_at_unix_ms: stale_time,
            })
            .unwrap();
        engine
            .storage
            .put_knowledge_policy_access_metadata(
                "memory:resolve-1",
                &copperdb_storage::KnowledgePolicyAccessMetadata {
                    last_accessed_at_unix_ms: Some(now_unix_ms() - 10_000),
                    access_count: 3,
                },
            )
            .unwrap();

        let query = parser
            .parse("CALL nornicdb.knowledgepolicy.resolve('memory:resolve-1', '', '')")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        assert_eq!(row.get("entityId"), Some(&Value::String("memory:resolve-1".to_string())));
        assert_eq!(row.get("decayBinding"), Some(&Value::String("access_binding".to_string())));
        assert_eq!(
            row.get("promotionPolicy"),
            Some(&Value::String("reinforcement_policy".to_string()))
        );
        assert_eq!(
            row.get("matchedPromotionProfile"),
            Some(&Value::String("reinforcement".to_string()))
        );
        assert_eq!(row.get("suppressed"), Some(&Value::Bool(false)));
        assert_eq!(row.get("dryRun"), Some(&Value::Bool(false)));
        assert_eq!(row.get("scoreFrom"), Some(&Value::String("LASTACCESSED".to_string())));
        assert_eq!(row.get("accessCount"), Some(&Value::from(3u64)));
    }

    #[test]
    fn test_call_knowledgepolicy_resolve_by_labels_is_dry_run() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE slow_decay OPTIONS { halfLifeSeconds: 3600, visibilityThreshold: 0.1, scoreFloor: 0.05, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE memory_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE slow_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let query = parser
            .parse("CALL nornicdb.knowledgepolicy.resolve('', 'MemoryEpisode', '')")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();

        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        assert_eq!(row.get("dryRun"), Some(&Value::Bool(true)));
        assert_eq!(row.get("decayBinding"), Some(&Value::String("memory_binding".to_string())));
        assert_eq!(row.get("targetKind"), Some(&Value::String("NODE".to_string())));
        assert_eq!(row.get("suppressed"), Some(&Value::Bool(false)));
        assert_eq!(row.get("anchorUnixMs"), Some(&Value::Null));
    }

    #[test]
    fn test_match_updates_node_access_metadata_via_on_access_policy() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE visible_decay OPTIONS { halfLifeSeconds: 3600, visibilityThreshold: 0.5, scoreFloor: 0.5, function: 'step', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE memory_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE visible_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION POLICY memory_access FOR (n:MemoryEpisode) APPLY { ON ACCESS { SET n.lastAccessedAt = timestamp() SET n.accessCount = coalesce(n.accessCount, 0) + 1 } }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "memory:on-access".to_string(),
                labels: vec!["MemoryEpisode".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Tracked memory".to_string()))]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .unwrap();

        let query = parser.parse("MATCH (n:MemoryEpisode) RETURN n").unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);

        let metadata = engine
            .storage
            .get_knowledge_policy_access_metadata("memory:on-access")
            .unwrap()
            .unwrap();
        assert_eq!(metadata.access_count, 1);
        assert!(metadata.last_accessed_at_unix_ms.is_some());
    }

    #[test]
    fn test_match_updates_node_access_metadata_with_policy_only_target() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION POLICY memory_access FOR (n:MemoryEpisode) APPLY { ON ACCESS { SET n.lastAccessedAt = timestamp() SET n.accessCount = coalesce(n.accessCount, 0) + 1 } }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "memory:policy-only".to_string(),
                labels: vec!["MemoryEpisode".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Tracked memory".to_string()))]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .unwrap();

        let query = parser.parse("MATCH (n:MemoryEpisode) RETURN n").unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);

        let metadata = engine
            .storage
            .get_knowledge_policy_access_metadata("memory:policy-only")
            .unwrap()
            .unwrap();
        assert_eq!(metadata.access_count, 1);
        assert!(metadata.last_accessed_at_unix_ms.is_some());
    }

    #[test]
    fn test_match_updates_edge_access_metadata_via_on_access_policy() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE visible_decay OPTIONS { halfLifeSeconds: 3600, visibilityThreshold: 0.5, scoreFloor: 0.5, function: 'step', scope: 'EDGE', scoreFrom: 'CREATED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE edge_binding FOR ()-[r:LINKS]-() APPLY { DECAY PROFILE visible_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION POLICY edge_access FOR ()-[r:LINKS]-() APPLY { ON ACCESS { SET r.lastAccessedAt = timestamp() SET r.accessCount = coalesce(r.accessCount, 0) + 1 } }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        for node in [
            NodeRecord {
                id: "person:access-left".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
            NodeRecord {
                id: "person:access-right".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
        ] {
            engine.storage.put_node_record(&node).unwrap();
        }
        engine
            .storage
            .put_edge_record(&EdgeRecord {
                id: "links:on-access".to_string(),
                start_node: "person:access-left".to_string(),
                end_node: "person:access-right".to_string(),
                edge_type: "LINKS".to_string(),
                properties: BTreeMap::from([("kind".to_string(), Value::String("tracked".to_string()))]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .unwrap();

        let query = parser
            .parse("MATCH (:Person)-[r:LINKS]->(:Person) RETURN r")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);

        let metadata = engine
            .storage
            .get_knowledge_policy_access_metadata("links:on-access")
            .unwrap()
            .unwrap();
        assert_eq!(metadata.access_count, 1);
        assert!(metadata.last_accessed_at_unix_ms.is_some());
    }

    #[test]
    fn test_match_updates_edge_access_metadata_with_policy_only_target() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION POLICY edge_access FOR ()-[r:LINKS]-() APPLY { ON ACCESS { SET r.lastAccessedAt = timestamp() SET r.accessCount = coalesce(r.accessCount, 0) + 1 } }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        for node in [
            NodeRecord {
                id: "person:policy-edge-left".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
            NodeRecord {
                id: "person:policy-edge-right".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
        ] {
            engine.storage.put_node_record(&node).unwrap();
        }
        engine
            .storage
            .put_edge_record(&EdgeRecord {
                id: "links:policy-only".to_string(),
                start_node: "person:policy-edge-left".to_string(),
                end_node: "person:policy-edge-right".to_string(),
                edge_type: "LINKS".to_string(),
                properties: BTreeMap::from([("kind".to_string(), Value::String("tracked".to_string()))]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .unwrap();

        let query = parser
            .parse("MATCH (:Person)-[r:LINKS]->(:Person) RETURN r")
            .unwrap();
        let result = engine.execute(&query, &HashMap::new()).unwrap();
        assert_eq!(result.rows.len(), 1);

        let metadata = engine
            .storage
            .get_knowledge_policy_access_metadata("links:policy-only")
            .unwrap()
            .unwrap();
        assert_eq!(metadata.access_count, 1);
        assert!(metadata.last_accessed_at_unix_ms.is_some());
    }

    #[test]
    fn test_match_keeps_stale_nodes_visible_when_promotion_predicate_matches() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE stale_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.75, scoreFloor: 0.0, function: 'step', scope: 'NODE', scoreFrom: 'CREATED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE stale_binding FOR (n:MemoryEpisode) APPLY { DECAY PROFILE stale_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let stale_time = now_unix_ms() - 5_000;
        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "memory:promotion-visible".to_string(),
                labels: vec!["MemoryEpisode".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Reinforced memory".to_string()))]),
                created_at_unix_ms: stale_time,
                updated_at_unix_ms: stale_time,
            })
            .unwrap();

        let query = parser.parse("MATCH (n:MemoryEpisode) RETURN n").unwrap();
        let hidden = engine.execute(&query, &HashMap::new()).unwrap();
        assert!(hidden.rows.is_empty());

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION PROFILE reinforcement OPTIONS { scope: 'NODE', multiplier: 2.0, scoreFloor: 0.8, scoreCap: 1.0, enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION POLICY reinforcement_policy FOR (n:MemoryEpisode) APPLY PROFILE reinforcement WHEN 'n.accessCount >= 3'",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .storage
            .put_knowledge_policy_access_metadata(
                "memory:promotion-visible",
                &copperdb_storage::KnowledgePolicyAccessMetadata {
                    last_accessed_at_unix_ms: None,
                    access_count: 3,
                },
            )
            .unwrap();

        let visible = engine.execute(&query, &HashMap::new()).unwrap();
        assert_eq!(visible.rows.len(), 1);
    }

    #[test]
    fn test_match_keeps_stale_edges_visible_when_promotion_predicate_matches() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE stale_edge_decay OPTIONS { halfLifeSeconds: 1, visibilityThreshold: 0.75, scoreFloor: 0.0, function: 'step', scope: 'EDGE', scoreFrom: 'CREATED', enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE DECAY PROFILE stale_edge_binding FOR ()-[r:LINKS]-() APPLY { DECAY PROFILE stale_edge_decay, order: 10 }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        for node in [
            NodeRecord {
                id: "person:promo-edge-left".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
            NodeRecord {
                id: "person:promo-edge-right".to_string(),
                labels: vec!["Person".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            },
        ] {
            engine.storage.put_node_record(&node).unwrap();
        }

        let stale_time = now - 5_000;
        engine
            .storage
            .put_edge_record(&EdgeRecord {
                id: "links:promotion-visible".to_string(),
                start_node: "person:promo-edge-left".to_string(),
                end_node: "person:promo-edge-right".to_string(),
                edge_type: "LINKS".to_string(),
                properties: BTreeMap::from([("kind".to_string(), Value::String("reinforced".to_string()))]),
                created_at_unix_ms: stale_time,
                updated_at_unix_ms: stale_time,
            })
            .unwrap();

        let query = parser
            .parse("MATCH (:Person)-[r:LINKS]->(:Person) RETURN r")
            .unwrap();
        let hidden = engine.execute(&query, &HashMap::new()).unwrap();
        assert!(hidden.rows.is_empty());

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION PROFILE reinforcement_edge OPTIONS { scope: 'EDGE', multiplier: 2.0, scoreFloor: 0.8, scoreCap: 1.0, enabled: true }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION POLICY reinforcement_edge_policy FOR ()-[r:LINKS]-() APPLY PROFILE reinforcement_edge WHEN 'r.accessCount >= 2'",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();
        engine
            .storage
            .put_knowledge_policy_access_metadata(
                "links:promotion-visible",
                &copperdb_storage::KnowledgePolicyAccessMetadata {
                    last_accessed_at_unix_ms: None,
                    access_count: 2,
                },
            )
            .unwrap();

        let visible = engine.execute(&query, &HashMap::new()).unwrap();
        assert_eq!(visible.rows.len(), 1);
    }

    #[test]
    fn test_match_does_not_flush_access_metadata_on_query_error() {
        let engine = make_engine();
        let parser = Parser::new();

        engine
            .execute(
                &parser
                    .parse(
                        "CREATE PROMOTION POLICY memory_access FOR (n:MemoryEpisode) APPLY { ON ACCESS { SET n.lastAccessedAt = timestamp() SET n.accessCount = coalesce(n.accessCount, 0) + 1 } }",
                    )
                    .unwrap(),
                &HashMap::new(),
            )
            .unwrap();

        let now = now_unix_ms();
        engine
            .storage
            .put_node_record(&NodeRecord {
                id: "memory:error-buffer".to_string(),
                labels: vec!["MemoryEpisode".to_string()],
                properties: BTreeMap::from([("name".to_string(), Value::String("Tracked memory".to_string()))]),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .unwrap();

        let query = parser
            .parse("MATCH (n:MemoryEpisode) RETURN abs('x') AS bad")
            .unwrap();
        let err = match engine.execute(&query, &HashMap::new()) {
            Ok(_) => panic!("query should fail"),
            Err(err) => err,
        };
        assert!(matches!(err, EvalError::FilterError(_)));
        assert!(engine
            .storage
            .get_knowledge_policy_access_metadata("memory:error-buffer")
            .unwrap()
            .is_none());
    }
}
