use std::cell::RefCell;

thread_local! {
    static CURRENT_REQUEST_CONTEXT: RefCell<Option<RequestContext>> = const { RefCell::new(None) };
}

use super::*;
impl EvalEngine {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            storage,
            node_lookup_cache: Arc::new(Mutex::new(HashMap::new())),
            access_flusher: Arc::new(AccessFlusher::new()),
            hot_path_trace: HotPathTraceState::new(),
        }
    }

    pub fn hot_path_trace_snapshot(&self) -> HotPathTrace {
        self.hot_path_trace.snapshot()
    }

    fn with_access_buffer<T, F>(&self, operation: F) -> Result<T, EvalError>
    where
        F: FnOnce() -> Result<T, EvalError>,
    {
        self.access_flusher.with_buffer(operation, |buffer| {
            self.flush_access_mutation_buffer(buffer)
        })
    }

    fn flush_access_mutation_buffer(&self, buffer: AccessMutationBuffer) -> Result<(), EvalError> {
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

    /// Evaluate a WHERE predicate, handling PatternExists against storage.
    fn eval_where_predicate(
        &self,
        expr: &Expression,
        row: &Row,
        params: &HashMap<String, Value>,
    ) -> Result<bool, EvalError> {
        match expr {
            Expression::Not(inner) => {
                if let Expression::PatternExists { variable, rel_type, target_variable } = inner.as_ref() {
                    let exists = self.check_edge_exists(row, variable, rel_type, target_variable)?;
                    return Ok(!exists);
                }
                let inner_result = self.eval_where_predicate(inner, row, params)?;
                Ok(!inner_result)
            }
            Expression::PatternExists { variable, rel_type, target_variable } => {
                self.check_edge_exists(row, variable, rel_type, target_variable)
            }
            _ => eval_predicate(expr, row, params)
                .map_err(|e| EvalError::FilterError(e.to_string())),
        }
    }

    fn check_edge_exists(
        &self,
        row: &Row,
        left_var: &str,
        rel_type: &str,
        right_var: &str,
    ) -> Result<bool, EvalError> {
        let left_props = row.get(left_var).and_then(Value::as_object);
        let right_props = row.get(right_var).and_then(Value::as_object);
        let (Some(left), Some(right)) = (left_props, right_props) else {
            return Ok(false);
        };
        let left_id = left.get("_id").and_then(Value::as_str);
        let right_id = right.get("_id").and_then(Value::as_str);
        let (Some(left_id), Some(right_id)) = (left_id, right_id) else {
            return Ok(false);
        };
        let edges = self.storage.get_edges_by_type(rel_type)?;
        for edge in &edges {
            if edge.start_node == left_id && edge.end_node == right_id {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Execute a parsed Cypher query against the storage engine.
    pub fn execute(
        &self,
        query: &Query,
        params: &HashMap<String, Value>,
    ) -> Result<EvalResult, EvalError> {
        let request_context = RequestContext::detached();
        self.execute_with_context(&request_context, query, params)
    }

    pub fn execute_with_context(
        &self,
        request_context: &RequestContext,
        query: &Query,
        params: &HashMap<String, Value>,
    ) -> Result<EvalResult, EvalError> {
        self.hot_path_trace.reset();
        self.with_request_context(request_context, || {
            self.with_access_buffer(|| self.execute_inner(request_context, query, params))
        })
    }

    fn with_request_context<T>(
        &self,
        request_context: &RequestContext,
        run: impl FnOnce() -> Result<T, EvalError>,
    ) -> Result<T, EvalError> {
        CURRENT_REQUEST_CONTEXT.with(|slot| {
            let previous = slot.replace(Some(request_context.clone()));
            let result = run();
            slot.replace(previous);
            result
        })
    }

    fn check_current_request_context() -> Result<(), EvalError> {
        CURRENT_REQUEST_CONTEXT.with(|slot| {
            if let Some(request_context) = slot.borrow().as_ref() {
                request_context.check_active()?;
            }
            Ok(())
        })
    }

    fn execute_inner(
        &self,
        request_context: &RequestContext,
        query: &Query,
        params: &HashMap<String, Value>,
    ) -> Result<EvalResult, EvalError> {
        // Clear per-query caches so MERGE ON MATCH fires across statements
        self.invalidate_node_lookup_cache();
        let mut current_rows = pooled_binding_rows();
        current_rows.push(HashMap::new());
        let mut stats = QueryStats::default();
        let mut columns: Vec<String> = vec![];
        let mut result_rows: Vec<Row> = vec![];

        let mut clause_index = 0;
        while clause_index < query.clauses.len() {
            request_context.check_active()?;
            let clause = &query.clauses[clause_index];
            let next_where_expression =
                query
                    .clauses
                    .get(clause_index + 1)
                    .and_then(|clause| match clause {
                        Clause::Where(where_clause) => Some(&where_clause.expression),
                        _ => None,
                    });
            match clause {
                Clause::Call(call) => {
                    let mut call_result = self.execute_call_clause(call, params, &current_rows)?;
                    // Apply YIELD projection — restrict columns to those requested.
                    // YIELD * (Variable("*")) means passthrough all columns.
                    if !call.yield_items.is_empty() {
                        let is_wildcard = call.yield_items.iter().any(|item| {
                            matches!(&item.expression, Expression::Variable(v) if v == "*")
                        });
                        if !is_wildcard {
                            let yield_columns: Vec<String> = call
                                .yield_items
                                .iter()
                                .map(|item| {
                                    item.alias
                                        .clone()
                                        .unwrap_or_else(|| column_name(item))
                                })
                                .collect();
                            call_result.rows = call_result
                                .rows
                                .into_iter()
                                .map(|mut row| {
                                    let mut filtered = Row::new();
                                    for col in &yield_columns {
                                        if let Some(val) = row.remove(col) {
                                            filtered.insert(col.clone(), val);
                                        }
                                    }
                                    filtered
                                })
                                .collect();
                            call_result.columns = yield_columns;
                        }
                    }
                    columns = call_result.columns;
                    if clause_index + 1 == query.clauses.len() {
                        result_rows = call_result.rows;
                    } else {
                        current_rows = call_result.rows;
                    }
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
                    for entry in &create.entries {
                        let (constraint_type, type_name, allowed_values) = match &entry.kind {
                            ConstraintKind::Unique => (ConstraintType::Unique, None, Vec::new()),
                            ConstraintKind::Exists => (ConstraintType::Exists, None, Vec::new()),
                            ConstraintKind::NodeKey => (ConstraintType::NodeKey, None, Vec::new()),
                            ConstraintKind::RelationshipKey => (ConstraintType::Relationship, None, Vec::new()),
                            ConstraintKind::Type(name) => (ConstraintType::Type, Some(name.clone()), Vec::new()),
                            ConstraintKind::Temporal => (ConstraintType::Temporal, None, Vec::new()),
                            ConstraintKind::Domain(values) => (ConstraintType::Domain, None, values.clone()),
                        };
                        self.storage.persist_constraint(&Constraint {
                            name: create.name.clone(),
                            constraint_type,
                            entity_type: match create.entity_type {
                                CypherConstraintEntityType::Node => {
                                    ConstraintEntityType::Node
                                }
                                CypherConstraintEntityType::Relationship => {
                                    ConstraintEntityType::Relationship
                                }
                            },
                            label: create.label.clone(),
                            properties: entry.properties.clone(),
                            type_name,
                            allowed_values,
                        })?;
                    }
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
                                        ConstraintType::Temporal => "TEMPORAL",
                                        ConstraintType::Domain => "DOMAIN",
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
                        entity_type: match create.entity_type {
                            copperdb_cypher::IndexEntityType::Node => {
                                copperdb_indexing::CatalogIndexEntityType::Node
                            }
                            copperdb_cypher::IndexEntityType::Relationship => {
                                copperdb_indexing::CatalogIndexEntityType::Relationship
                            }
                        },
                        kind: match create.kind {
                            copperdb_cypher::IndexKind::Range => {
                                copperdb_indexing::CatalogIndexKind::Range
                            }
                            copperdb_cypher::IndexKind::Temporal => {
                                copperdb_indexing::CatalogIndexKind::Temporal
                            }
                            copperdb_cypher::IndexKind::FullText => {
                                copperdb_indexing::CatalogIndexKind::FullText
                            }
                            copperdb_cypher::IndexKind::Vector => {
                                copperdb_indexing::CatalogIndexKind::Vector
                            }
                        },
                        label: create.label.clone(),
                        properties: create.properties.clone(),
                    };
                    if create.if_not_exists {
                        catalog.create_if_absent(definition)?;
                    } else {
                        catalog.create(definition)?;
                    }

                    // Persist vector index options separately
                    if matches!(create.kind, copperdb_cypher::IndexKind::Vector)
                        && !create.options.is_empty()
                    {
                        self.storage
                            .persist_index_options(&create.name, &create.options)?;
                    }
                }

                Clause::DropIndex(drop) => {
                    let catalog = IndexCatalog::new(self.storage.as_ref());
                    if drop.if_exists {
                        catalog.drop_if_present(&drop.name)?;
                    } else {
                        catalog.drop(&drop.name)?;
                    }
                    // Clean up any associated index options
                    self.storage.delete_index_options(&drop.name)?;
                }

                Clause::ShowIndexes(show) => {
                    let indexes = IndexCatalog::new(self.storage.as_ref()).list()?;
                    columns = vec![
                        "name".to_string(),
                        "entityType".to_string(),
                        "kind".to_string(),
                        "label".to_string(),
                        "properties".to_string(),
                    ];
                    result_rows = indexes
                        .into_iter()
                        .filter(|idx| match show.kind {
                            Some(copperdb_cypher::IndexKind::Range) => {
                                idx.kind == copperdb_indexing::CatalogIndexKind::Range
                            }
                            Some(copperdb_cypher::IndexKind::Temporal) => {
                                idx.kind == copperdb_indexing::CatalogIndexKind::Temporal
                            }
                            Some(copperdb_cypher::IndexKind::FullText) => {
                                idx.kind == copperdb_indexing::CatalogIndexKind::FullText
                            }
                            Some(copperdb_cypher::IndexKind::Vector) => {
                                idx.kind == copperdb_indexing::CatalogIndexKind::Vector
                            }
                            None => true,
                        })
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
                            row.insert(
                                "kind".to_string(),
                                Value::String(
                                    match idx.kind {
                                        copperdb_indexing::CatalogIndexKind::Range => "RANGE",
                                        copperdb_indexing::CatalogIndexKind::Temporal => "TEMPORAL",
                                        copperdb_indexing::CatalogIndexKind::FullText => "FULLTEXT",
                                        copperdb_indexing::CatalogIndexKind::Vector => "VECTOR",
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
                                Some(_) => {
                                    Some(option_f64(&create.options, "visibilityThreshold", 0.0)?)
                                }
                                None => None,
                            },
                            order: option_i64(&create.options, "order", 0)?,
                        };
                        self.storage
                            .persist_decay_profile_binding_schema(&binding)?;
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
                    result_rows = profiles
                        .into_iter()
                        .map(|p| {
                            let mut row = Row::new();
                            row.insert("kind".to_string(), Value::String("bundle".to_string()));
                            row.insert("name".to_string(), Value::String(p.name));
                            row.insert("scope".to_string(), Value::String(p.scope));
                            row.insert("target".to_string(), Value::String(String::new()));
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
                                profile_ref.map(Value::String).unwrap_or(Value::Null),
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
                            next_where_expression,
                        )?;
                    } else {
                        // Iteratively cross-join each node pattern so that bindings from
                        // earlier patterns are visible when processing later ones.
                        for node_pat in &match_clause.pattern.nodes {
                            let mut new_rows = pooled_binding_rows();
                            for base_row in &current_rows {
                                for props in self.matching_node_props_with_where(
                                    node_pat,
                                    base_row,
                                    params,
                                    next_where_expression,
                                )? {
                                    let node_val = serde_json::to_value(&props).map_err(|e| {
                                        EvalError::SerializationError(e.to_string())
                                    })?;

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
                }

                Clause::OptionalMatch(match_clause) => {
                    current_rows = self.execute_optional_match_clause(
                        &current_rows,
                        &match_clause.pattern,
                        params,
                    )?;
                }

                Clause::Where(where_clause) => {
                    let expr = &where_clause.expression;
                    let mut filtered = pooled_binding_rows();
                    let mut old_rows = std::mem::take(&mut current_rows);
                    for row in old_rows.drain(..) {
                        match self.eval_where_predicate(expr, &row, params) {
                            Ok(true) => filtered.push(row),
                            Ok(false) => {}
                            Err(e) => return Err(e),
                        }
                    }
                    recycle_binding_rows(old_rows);
                    current_rows = filtered;
                }

                Clause::Return(ret) => {
                    // Expand * wildcard to all current columns
                    let has_wildcard = ret.items.iter().any(|item| {
                        matches!(&item.expression, Expression::Variable(v) if v == "*")
                    });
                    if has_wildcard {
                        columns = current_rows
                            .first()
                            .map(|row| row.keys().cloned().collect())
                            .unwrap_or_default();
                    } else {
                        columns = ret.items.iter().map(column_name).collect();
                    }

                    if !ret.order_by.is_empty() {
                        sort_rows_by_return_order(&mut current_rows, ret);
                    }

                    // SKIP / LIMIT — resolve expressions to i64
                    let skip_val = resolve_limit(&ret.skip, &params);
                    let limit_val = resolve_limit(&ret.limit, &params);
                    if let Some(skip) = skip_val {
                        let skip = skip.max(0) as usize;
                        current_rows = current_rows.into_iter().skip(skip).collect();
                    }
                    if let Some(limit) = limit_val {
                        let limit = limit.max(0) as usize;
                        current_rows.truncate(limit);
                    }

                    // Project down to only the returned columns, or apply
                    // aggregation when the RETURN contains agg functions.
                    // NornicDB-style: aggregations always produce 1 row on
                    // empty input with identity values (count→0, sum→0,
                    // avg→null, min→null, max→null).
                    let any_agg = has_aggregation_items(&ret.items);
                    let non_count_agg = has_non_count_aggregation(&ret.items);
                    let mut rows: Vec<Row> = if current_rows.is_empty() && any_agg {
                        // Empty rows + aggregation → produce identity row
                        vec![aggregate_identity_row(&ret.items, params)?]
                    } else if non_count_agg && !current_rows.is_empty() {
                        apply_aggregation_to_rows(&current_rows, &ret.items, params)?
                    } else {
                        current_rows
                            .iter()
                            .map(|row| project_row(row, &ret.items, params))
                            .collect::<Result<Vec<_>, _>>()?
                    };
                    // If aggregation produced empty result but we had rows, fall back
                    if non_count_agg && rows.is_empty() && !current_rows.is_empty() {
                        rows = current_rows
                            .iter()
                            .map(|row| project_row(row, &ret.items, params))
                            .collect::<Result<Vec<_>, _>>()?;
                    }

                    // DISTINCT applied after projection (deduplication is over
                    // projected values, which is standard Cypher semantics).
                    if ret.distinct {
                        let mut seen = std::collections::HashSet::new();
                        rows.retain(|r| seen.insert(row_key(r)));
                    }

                    result_rows = rows;
                }

                Clause::Delete(del) => {
                    current_rows =
                        self.execute_delete_clause(&current_rows, &del.variables, &mut stats)?;
                }

                Clause::Set(set) => {
                    self.execute_set_clause(&mut current_rows, &set.items, params, &mut stats)?;
                }

                Clause::Remove(remove) => {
                    self.execute_remove_clause(&mut current_rows, &remove.items)?;
                }

                Clause::With(with) => {
                    // Project rows like RETURN but continue pipeline;
                    // WITH always triggers aggregation when agg functions present
                    let with_agg = has_aggregation_items(&with.items) && !current_rows.is_empty();
                    let mut projected: Vec<Row> = if with_agg {
                        apply_aggregation_to_rows(&current_rows, &with.items, params)?
                    } else {
                        current_rows
                            .iter()
                            .map(|row| project_row(row, &with.items, params))
                            .collect::<Result<Vec<_>, _>>()?
                    };

                    if let Some(where_clause) = &with.where_clause {
                        let mut filtered_rows = pooled_binding_rows();
                        for row in projected {
                            if eval_predicate(&where_clause.expression, &row, params)
                                .map_err(|e| EvalError::FilterError(e.to_string()))?
                            {
                                filtered_rows.push(row);
                            }
                        }
                        projected = filtered_rows;
                    }

                    if !with.order_by.is_empty() {
                        sort_rows_by_with_order(&mut projected, with);
                    }
                    apply_with_window(&mut projected, with, params);

                    current_rows = projected;
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

                Clause::Foreach(foreach) => {
                    for row in &mut *current_rows {
                        let list_val = eval_expression(&foreach.list, row, params)?;
                        if let Value::Array(items) = list_val {
                            for item in items {
                                // Bind loop variable directly on the row
                                row.insert(foreach.variable.clone(), item);
                                // Execute each inner SET clause
                                for update in &foreach.updates {
                                    if let Clause::Set(set) = update {
                                        self.execute_set_clause(
                                            std::slice::from_mut(row),
                                            &set.items,
                                            params,
                                            &mut stats,
                                        )?;
                                    }
                                }
                            }
                        }
                    }
                }

                Clause::Merge(merge) => {
                    current_rows =
                        self.execute_merge_clause(&current_rows, merge, params, &mut stats)?;
                }
            }
            clause_index += 1;
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
        let request_context = RequestContext::detached();
        self.execute_with_routes_with_context(
            &request_context,
            query,
            params,
            pattern_info,
            None,
            None,
        )
    }

    pub fn execute_with_routes(
        &self,
        query: &Query,
        params: &HashMap<String, Value>,
        pattern_info: &PatternInfo,
        compound_match: Option<&ShapeMatch>,
        pipeline_clauses: Option<&[PipelineClause]>,
    ) -> Result<EvalResult, EvalError> {
        let request_context = RequestContext::detached();
        self.execute_with_routes_with_context(
            &request_context,
            query,
            params,
            pattern_info,
            compound_match,
            pipeline_clauses,
        )
    }

    pub fn execute_with_routes_with_context(
        &self,
        request_context: &RequestContext,
        query: &Query,
        params: &HashMap<String, Value>,
        pattern_info: &PatternInfo,
        compound_match: Option<&ShapeMatch>,
        pipeline_clauses: Option<&[PipelineClause]>,
    ) -> Result<EvalResult, EvalError> {
        self.hot_path_trace.reset();
        self.with_access_buffer(|| {
            request_context.check_active()?;
            match pattern_info.pattern {
                QueryPattern::SimpleMatchLimit if self.can_execute_simple_match_limit(query) => {
                    return self.execute_simple_match_limit_optimized(
                        request_context,
                        query,
                        params,
                    );
                }
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

            self.execute_inner(request_context, query, params)
        })
    }

    fn can_execute_simple_match_return(&self, query: &Query) -> bool {
        query
            .clauses
            .iter()
            .all(|clause| matches!(clause, Clause::Match(_) | Clause::Return(_)))
    }

    fn can_execute_simple_match_limit(&self, query: &Query) -> bool {
        if query.clauses.len() != 2 {
            return false;
        }

        let Some(Clause::Match(match_clause)) = query.clauses.first() else {
            return false;
        };
        let Some(Clause::Return(ret)) = query.clauses.get(1) else {
            return false;
        };

        if ret.limit.is_none()
            || ret.skip.is_some()
            || ret.distinct
            || !ret.order_by.is_empty()
            || ret.items.len() != 1
        {
            return false;
        }

        let pattern = &match_clause.pattern;
        if pattern.shortest_path
            || pattern.path_variable.is_some()
            || pattern.nodes.len() != 1
            || !pattern.edges.is_empty()
        {
            return false;
        }

        let node_pattern = &pattern.nodes[0];
        let Some(variable) = node_pattern.variable.as_ref() else {
            return false;
        };
        if node_pattern.labels.is_empty() || !node_pattern.properties.is_empty() {
            return false;
        }

        matches!(&ret.items[0].expression, Expression::Variable(returned) if returned == variable)
    }

    fn can_execute_edge_property_agg(&self, query: &Query) -> bool {
        query
            .clauses
            .iter()
            .all(|clause| matches!(clause, Clause::Match(_) | Clause::Return(_)))
    }

    fn execute_simple_match_limit_optimized(
        &self,
        request_context: &RequestContext,
        query: &Query,
        params: &HashMap<String, Value>,
    ) -> Result<EvalResult, EvalError> {
        self.hot_path_trace.mark_simple_match_limit_fast_path();
        let Some(Clause::Match(match_clause)) = query.clauses.first() else {
            return self.execute_inner(request_context, query, params);
        };
        let ret = return_clause(query)?;
        let limit = resolve_limit(&ret.limit, params).unwrap_or(0).max(0) as usize;
        let columns: Vec<String> = ret.items.iter().map(column_name).collect();
        if limit == 0 {
            return Ok(EvalResult {
                columns,
                rows: Vec::new(),
                stats: QueryStats::default(),
            });
        }

        let node_pattern = &match_clause.pattern.nodes[0];
        let variable = node_pattern.variable.clone().ok_or_else(|| {
            EvalError::ExecutionError(
                "optimized simple MATCH LIMIT requires a bound node variable".to_string(),
            )
        })?;
        let primary_label = node_pattern.labels.first().ok_or_else(|| {
            EvalError::ExecutionError(
                "optimized simple MATCH LIMIT requires at least one label".to_string(),
            )
        })?;

        let resolver = self.knowledge_policy_resolver()?;
        let mut rows = Vec::with_capacity(limit);
        for node in self.storage.get_nodes_by_label(primary_label)? {
            request_context.check_active()?;
            if !node_pattern
                .labels
                .iter()
                .all(|label| node.labels.iter().any(|node_label| node_label == label))
            {
                continue;
            }
            if !self.node_visible_under_policy(&node, &resolver)? {
                continue;
            }
            self.apply_on_access_for_node(&node, &resolver)?;

            let mut binding_row = Row::new();
            binding_row.insert(
                variable.clone(),
                Value::Object(node_record_to_props(&node).into_iter().collect()),
            );
            rows.push(project_row(&binding_row, &ret.items, params)?);

            if rows.len() == limit {
                break;
            }
        }

        Ok(EvalResult {
            columns,
            rows,
            stats: QueryStats::default(),
        })
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
        apply_return_window(&mut rows, ret, &HashMap::new());

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
        let edge_type =
            (!pattern_info.rel_type.is_empty()).then_some(pattern_info.rel_type.as_str());
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
        apply_return_window(&mut rows, ret, &HashMap::new());

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

        if let Some(skip) = resolve_limit(&ret.skip, &HashMap::new()) {
            rows = rows.into_iter().skip(skip.max(0) as usize).collect();
        }
        if let Some(limit) = resolve_limit(&ret.limit, &HashMap::new()) {
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
        self.hot_path_trace.mark_compound_query_fast_path();
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

        let stats = QueryStats {
            relationships_created: 1,
            relationships_deleted: 1,
            ..QueryStats::default()
        };

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

    pub(crate) fn can_execute_pipeline_route(
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
                Clause::OptionalMatch(_) => query_kinds.push(PipelineClauseKind::OptionalMatch),
                Clause::Create(_) => query_kinds.push(PipelineClauseKind::Create),
                Clause::Merge(_) => query_kinds.push(PipelineClauseKind::Merge),
                Clause::With(_) => query_kinds.push(PipelineClauseKind::With),
                Clause::Unwind(_) => query_kinds.push(PipelineClauseKind::Unwind),
                Clause::Delete(_) => query_kinds.push(PipelineClauseKind::Delete),
                Clause::Set(_) => query_kinds.push(PipelineClauseKind::Set),
                Clause::Remove(_) => query_kinds.push(PipelineClauseKind::Remove),
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

    pub(crate) fn execute_pipeline_routed(
        &self,
        query: &Query,
        params: &HashMap<String, Value>,
        pipeline_clauses: &[PipelineClause],
    ) -> Result<EvalResult, EvalError> {
        let pipeline_has_unwind = pipeline_clauses
            .iter()
            .any(|clause| clause.kind == PipelineClauseKind::Unwind);
        let pipeline_has_match = pipeline_clauses.iter().any(|clause| {
            matches!(
                clause.kind,
                PipelineClauseKind::Match | PipelineClauseKind::OptionalMatch
            )
        });
        let unwind_index = pipeline_clauses
            .iter()
            .position(|clause| clause.kind == PipelineClauseKind::Unwind);
        let match_after_unwind_index = unwind_index.and_then(|index| {
            pipeline_clauses
                .iter()
                .enumerate()
                .skip(index + 1)
                .find(|(_, clause)| clause.kind == PipelineClauseKind::Match)
                .map(|(match_index, _)| match_index)
        });
        let pipeline_has_unwind_match_create_tail = match_after_unwind_index.is_some_and(|index| {
            pipeline_clauses
                .iter()
                .skip(index + 1)
                .any(|clause| clause.kind == PipelineClauseKind::Create)
        });
        let mut current_rows = pooled_binding_rows();
        current_rows.push(Row::new());
        let mut stats = QueryStats::default();

        let mut clause_index = 0;
        while clause_index < query.clauses.len() {
            let clause = &query.clauses[clause_index];
            let next_where_expression =
                query
                    .clauses
                    .get(clause_index + 1)
                    .and_then(|clause| match clause {
                        Clause::Where(where_clause) => Some(&where_clause.expression),
                        _ => None,
                    });
            match clause {
                Clause::Match(match_clause) => {
                    current_rows = self.execute_pipeline_match_clause(
                        &current_rows,
                        &match_clause.pattern,
                        params,
                        next_where_expression,
                    )?;
                }
                Clause::OptionalMatch(match_clause) => {
                    current_rows = self.execute_optional_match_clause(
                        &current_rows,
                        &match_clause.pattern,
                        params,
                    )?;
                }
                Clause::Where(where_clause) => {
                    let mut filtered = pooled_binding_rows();
                    let mut old_rows = std::mem::take(&mut current_rows);
                    for row in old_rows.drain(..) {
                        match self.eval_where_predicate(&where_clause.expression, &row, params) {
                            Ok(true) => filtered.push(row),
                            Ok(false) => {}
                            Err(e) => return Err(e),
                        }
                    }
                    recycle_binding_rows(old_rows);
                    current_rows = filtered;
                }
                Clause::Create(create) => {
                    if pipeline_has_unwind_match_create_tail {
                        self.hot_path_trace.mark_unwind_fixed_chain_link_batch();
                    }
                    current_rows = self.execute_pipeline_create_clause(
                        &current_rows,
                        create,
                        params,
                        &mut stats,
                    )?;
                }
                Clause::Merge(merge) => {
                    if pipeline_has_unwind && !pipeline_has_match {
                        self.hot_path_trace.mark_unwind_simple_merge_batch();
                    }
                    current_rows =
                        self.execute_merge_clause(&current_rows, merge, params, &mut stats)?;
                }
                Clause::With(with) => {
                    let mut projected: Vec<Row> = current_rows
                        .iter()
                        .map(|row| project_row(row, &with.items, params))
                        .collect::<Result<Vec<_>, _>>()?;

                    if let Some(where_clause) = &with.where_clause {
                        let mut filtered = pooled_binding_rows();
                        for row in projected {
                            if eval_predicate(&where_clause.expression, &row, params)
                                .map_err(|e| EvalError::FilterError(e.to_string()))?
                            {
                                filtered.push(row);
                            }
                        }
                        projected = filtered;
                    }

                    if !with.order_by.is_empty() {
                        sort_rows_by_with_order(&mut projected, with);
                    }
                    apply_with_window(&mut projected, with, params);

                    current_rows = projected;
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
                Clause::Delete(del) => {
                    current_rows =
                        self.execute_delete_clause(&current_rows, &del.variables, &mut stats)?;
                }
                Clause::Set(set) => {
                    self.execute_set_clause(&mut current_rows, &set.items, params, &mut stats)?;
                }
                Clause::Remove(remove) => {
                    self.execute_remove_clause(&mut current_rows, &remove.items)?;
                }
                Clause::Return(ret) => {
                    let columns: Vec<String> = ret.items.iter().map(column_name).collect();

                    if !ret.order_by.is_empty() {
                        sort_rows_by_return_order(&mut current_rows, ret);
                    }

                    if let Some(skip) = resolve_limit(&ret.skip, params) {
                        let skip = skip.max(0) as usize;
                        current_rows = current_rows.into_iter().skip(skip).collect();
                    }
                    if let Some(limit) = resolve_limit(&ret.limit, params) {
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
            clause_index += 1;
        }

        Ok(EvalResult {
            columns: Vec::new(),
            rows: Vec::new(),
            stats,
        })
    }

    fn execute_delete_clause(
        &self,
        base_rows: &[Row],
        variables: &[String],
        stats: &mut QueryStats,
    ) -> Result<Vec<Row>, EvalError> {
        self.invalidate_node_lookup_cache();
        let mut remaining_rows = pooled_binding_rows();

        for row in base_rows {
            for var in variables {
                self.delete_bound_value(row.get(var), stats)?;
            }
            remaining_rows.push(row.clone());
        }

        Ok(remaining_rows)
    }

    fn execute_set_clause(
        &self,
        rows: &mut [Row],
        items: &[SetItem],
        params: &HashMap<String, Value>,
        stats: &mut QueryStats,
    ) -> Result<(), EvalError> {
        self.invalidate_node_lookup_cache();
        for row in rows {
            for item in items {
                match item {
                    SetItem::Property { variable, property, value } => {
                        let new_val = eval_expression(value, row, params)?;
                        if let Some(Value::Object(props)) = row.get_mut(variable) {
                            props.insert(property.clone(), new_val);
                            stats.properties_set += 1;
                            let persisted_props: HashMap<String, Value> =
                                props.clone().into_iter().collect();
                            self.persist_bound_props(&persisted_props)?;
                        }
                    }
                    SetItem::MapAssignment { variable, value } => {
                        let new_val = eval_expression(value, row, params)?;
                        if let Value::Object(map) = &new_val {
                            if let Some(Value::Object(props)) = row.get_mut(variable) {
                                for (k, v) in map {
                                    props.insert(k.clone(), v.clone());
                                }
                                stats.properties_set += map.len();
                                let persisted_props: HashMap<String, Value> =
                                    props.clone().into_iter().collect();
                                self.persist_bound_props(&persisted_props)?;
                            }
                        }
                    }
                    SetItem::MapMerge { variable, value } => {
                        let new_val = eval_expression(value, row, params)?;
                        if let Value::Object(map) = &new_val {
                            if let Some(Value::Object(props)) = row.get_mut(variable) {
                                let mut merged = 0usize;
                                for (k, v) in map {
                                    // Nil/null values must not clobber existing properties
                                    // (NornicDB parity: applySetMapMergeToNode skips nil).
                                    if matches!(v, Value::Null) {
                                        continue;
                                    }
                                    props.insert(k.clone(), v.clone());
                                    merged += 1;
                                }
                                stats.properties_set += merged;
                                let persisted_props: HashMap<String, Value> =
                                    props.clone().into_iter().collect();
                                self.persist_bound_props(&persisted_props)?;
                            }
                        }
                    }
                    SetItem::Label { variable, label } => {
                        let Some(Value::Object(props)) = row.get_mut(variable) else {
                            continue;
                        };
                        // Ensure _labels array exists, then add if not present
                        let labels = props
                            .entry("_labels".to_string())
                            .or_insert_with(|| Value::Array(Vec::new()));
                        if let Value::Array(arr) = labels {
                            let label_val = Value::String(label.clone());
                            if !arr.contains(&label_val) {
                                arr.push(label_val);
                            }
                        }
                        let persisted_props: HashMap<String, Value> =
                            props.clone().into_iter().collect();
                        self.persist_bound_props(&persisted_props)?;
                    }
                    SetItem::DynamicLabel {
                        variable,
                        expression,
                    } => {
                        let val = eval_expression(expression, row, params)?;
                        let Some(Value::Object(props)) = row.get_mut(variable) else {
                            continue;
                        };
                        let labels = props
                            .entry("_labels".to_string())
                            .or_insert_with(|| Value::Array(Vec::new()));
                        if let Value::Array(arr) = labels {
                            match val {
                                Value::String(s) => {
                                    let label_val = Value::String(s);
                                    if !arr.contains(&label_val) {
                                        arr.push(label_val);
                                    }
                                }
                                Value::Array(items) => {
                                    for item in items {
                                        if let Some(s) = item.as_str() {
                                            let label_val = Value::String(s.to_string());
                                            if !arr.contains(&label_val) {
                                                arr.push(label_val);
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                        let persisted_props: HashMap<String, Value> =
                            props.clone().into_iter().collect();
                        self.persist_bound_props(&persisted_props)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn execute_remove_clause(
        &self,
        rows: &mut [Row],
        items: &[RemoveItem],
    ) -> Result<(), EvalError> {
        self.invalidate_node_lookup_cache();
        for row in rows {
            for item in items {
                match item {
                    RemoveItem::Property { variable, property } => {
                        if property.starts_with('_') {
                            return Err(EvalError::ExecutionError(
                                "cannot remove internal metadata properties".to_string(),
                            ));
                        }
                        if let Some(Value::Object(props)) = row.get_mut(variable) {
                            props.remove(property);
                            let persisted_props: HashMap<String, Value> =
                                props.clone().into_iter().collect();
                            self.persist_bound_props(&persisted_props)?;
                        }
                    }
                    RemoveItem::Label { variable, label } => {
                        let Some(Value::Object(props)) = row.get_mut(variable) else {
                            continue;
                        };
                        let Some(Value::Array(labels)) = props.get_mut("_labels") else {
                            return Err(EvalError::ExecutionError(
                                "REMOVE label targets must be node bindings".to_string(),
                            ));
                        };
                        labels.retain(|value| value.as_str() != Some(label.as_str()));
                        let persisted_props: HashMap<String, Value> =
                            props.clone().into_iter().collect();
                        self.persist_node_props(&persisted_props)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn delete_bound_value(
        &self,
        value: Option<&Value>,
        stats: &mut QueryStats,
    ) -> Result<(), EvalError> {
        let Some(Value::Object(props)) = value else {
            return Ok(());
        };
        let Some(id) = props.get("_id").and_then(Value::as_str) else {
            return Ok(());
        };

        if props.contains_key("_type") && props.contains_key("_start") && props.contains_key("_end")
        {
            self.storage.delete_edge_record(id)?;
            stats.relationships_deleted += 1;
        } else {
            self.storage.delete_node_record(id)?;
            stats.nodes_deleted += 1;
        }

        Ok(())
    }

    fn persist_bound_props(&self, props: &HashMap<String, Value>) -> Result<(), EvalError> {
        if props.contains_key("_type") && props.contains_key("_start") && props.contains_key("_end")
        {
            self.persist_edge_props(props)?;
        } else {
            self.persist_node_props(props)?;
        }
        Ok(())
    }
}

/// Resolve a SKIP/LIMIT expression to an i64, supporting literals and $param references.
fn resolve_limit(expr: &Option<Expression>, params: &HashMap<String, Value>) -> Option<i64> {
    let expr = expr.as_ref()?;
    match expr {
        Expression::Literal(LiteralValue::Integer(i)) => Some(*i),
        Expression::Parameter(name) => params.get(name)?.as_i64(),
        _ => None,
    }
}

impl EvalEngine {
    fn execute_optional_match_clause(
        &self,
        base_rows: &[Row],
        pattern: &Pattern,
        params: &HashMap<String, Value>,
    ) -> Result<Vec<Row>, EvalError> {
        if !pattern.edges.is_empty() {
            let mut optional_rows = pooled_binding_rows();
            for base_row in base_rows {
                let matched = self.match_relationship_pattern(
                    std::slice::from_ref(base_row),
                    pattern,
                    params,
                    None,
                )?;
                if matched.is_empty() {
                    let mut row = base_row.clone();
                    bind_optional_pattern_nulls(&mut row, pattern);
                    optional_rows.push(row);
                } else {
                    optional_rows.extend(matched);
                }
            }
            return Ok(optional_rows);
        }

        let mut current_rows = base_rows.to_vec();
        for node_pat in &pattern.nodes {
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
                    bind_single_node_path_variable(&mut row, pattern, node_val);
                    new_rows.push(row);
                    found_any = true;
                }
            }
            if !found_any {
                for base_row in &current_rows {
                    let mut row = base_row.clone();
                    if let Some(var) = &node_pat.variable {
                        row.insert(var.clone(), Value::Null);
                    }
                    bind_optional_pattern_nulls(&mut row, pattern);
                    new_rows.push(row);
                }
            }
            replace_binding_rows(&mut current_rows, new_rows);
        }

        Ok(current_rows)
    }

    fn execute_pipeline_match_clause(
        &self,
        base_rows: &[Row],
        pattern: &Pattern,
        params: &HashMap<String, Value>,
        where_expression: Option<&Expression>,
    ) -> Result<Vec<Row>, EvalError> {
        if !pattern.edges.is_empty() {
            return self.match_relationship_pattern(base_rows, pattern, params, where_expression);
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
                    for props in self.matching_node_props_with_where(
                        node_pat,
                        base_row,
                        params,
                        where_expression,
                    )? {
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

    fn execute_node_match_clause(
        &self,
        base_rows: &[Row],
        pattern: &Pattern,
        params: &HashMap<String, Value>,
        where_expression: Option<&Expression>,
    ) -> Result<Vec<Row>, EvalError> {
        let mut current_rows = base_rows.to_vec();
        for node_pat in &pattern.nodes {
            let mut new_rows = pooled_binding_rows();
            for base_row in &current_rows {
                for props in self.matching_node_props_with_where(
                    node_pat,
                    base_row,
                    params,
                    where_expression,
                )? {
                    let node_val = serde_json::to_value(&props)
                        .map_err(|e| EvalError::SerializationError(e.to_string()))?;

                    let mut row = base_row.clone();
                    if let Some(var) = &node_pat.variable {
                        row.insert(var.clone(), node_val.clone());
                    }
                    bind_single_node_path_variable(&mut row, pattern, node_val);
                    new_rows.push(row);
                }
            }
            replace_binding_rows(&mut current_rows, new_rows);
        }

        Ok(current_rows)
    }

    /// Returns whether a JSON value matches an expected Cypher type name.
    /// Type names are case-insensitive.
    fn value_matches_type(value: &Value, type_name: &str) -> bool {
        let normalized = type_name.to_uppercase();
        match &normalized[..] {
            "INTEGER" | "INT" => value.is_i64() || value.is_u64() || value.is_number(),
            "FLOAT" | "DOUBLE" => value.is_f64() || value.is_number(),
            "NUMBER" => value.is_number(),
            "STRING" => value.is_string(),
            "BOOLEAN" | "BOOL" => value.is_boolean(),
            "NULL" => value.is_null(),
            "LIST" | "ARRAY" => value.is_array(),
            "MAP" | "OBJECT" => value.is_object(),
            "DATE" => {
                value.as_str()
                    .map(|s| s.len() >= 10 && s.chars().filter(|&c| c == '-').count() == 2)
                    .unwrap_or(false)
            }
            "DATETIME" | "TIMESTAMP" => {
                value.as_str()
                    .map(|s| s.len() >= 19 && s.contains('T'))
                    .unwrap_or(false)
                    || value.is_number()
            }
            "POINT" | "GEOMETRY" => {
                value.as_object()
                    .map(|o| o.contains_key("x") || o.contains_key("latitude"))
                    .unwrap_or(false)
            }
            _ => true, // Unknown types pass through
        }
    }

    /// Parse a JSON value as a temporal (i64) timestamp for temporal constraint comparison.
    fn parse_temporal_value(value: Option<&Value>) -> Option<i64> {
        match value {
            None | Some(Value::Null) => None,
            Some(Value::Number(n)) => n.as_i64(),
            Some(Value::String(s)) => s.parse::<i64>().ok(),
            _ => None,
        }
    }

    /// Check if two temporal ranges overlap.
    fn temporal_ranges_overlap(
        new_from: Option<i64>,
        new_to: Option<i64>,
        existing_from: Option<i64>,
        existing_to: Option<i64>,
    ) -> bool {
        // Unbounded new range overlaps everything
        if new_from.is_none() && new_to.is_none() {
            return true;
        }
        let start_before_existing_end = match (new_from, existing_to) {
            (_, None) | (None, _) => true,
            (Some(nf), Some(et)) => nf < et,
        };
        let end_after_existing_start = match (new_to, existing_from) {
            (_, None) | (None, _) => true,
            (Some(nt), Some(ef)) => nt > ef,
        };
        start_before_existing_end && end_after_existing_start
    }

    /// Compare two JSON values for equality (domain constraint checking).
    fn values_equal(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Number(a_num), Value::Number(b_num)) => {
                a_num.as_f64() == b_num.as_f64() && a_num.as_i64() == b_num.as_i64()
            }
            _ => a == b,
        }
    }

    /// Validate node properties against stored constraints (unique, exists, node key, type, temporal, domain).
    fn check_node_constraints(
        &self,
        labels: &[String],
        props: &HashMap<String, Value>,
    ) -> Result<(), EvalError> {
        let constraints = self.storage.load_constraints()?;
        for c in &constraints {
            if c.entity_type != ConstraintEntityType::Node {
                continue;
            }
            if !labels.contains(&c.label) {
                continue;
            }
            match c.constraint_type {
                ConstraintType::Unique => {
                    let Some(prop) = c.properties.first() else { continue };
                    let val = match props.get(prop) {
                        None | Some(Value::Null) => continue,
                        Some(v) => v,
                    };
                    // Scan nodes with matching label to find duplicate
                    if let Ok(nodes) = self.storage.get_nodes_by_label(&c.label) {
                        for node in &nodes {
                            if node.properties.get(prop) == Some(val) {
                                return Err(EvalError::ExecutionError(format!(
                                    "Node already exists with label `{}` and property `{}` = {:?}",
                                    c.label, prop, val
                                )));
                            }
                        }
                    }
                }
                ConstraintType::Exists => {
                    for prop in &c.properties {
                        match props.get(prop) {
                            None | Some(Value::Null) => {
                                return Err(EvalError::ExecutionError(format!(
                                    "Required property `{}` is missing on node with label `{}`",
                                    prop, c.label
                                )));
                            }
                            _ => {}
                        }
                    }
                }
                ConstraintType::NodeKey => {
                    // All key properties must be non-null
                    let mut key_vals: Vec<(&String, &Value)> = Vec::new();
                    for prop in &c.properties {
                        match props.get(prop) {
                            None | Some(Value::Null) => {
                                return Err(EvalError::ExecutionError(format!(
                                    "NODE KEY property `{}` cannot be null on node with label `{}`",
                                    prop, c.label
                                )));
                            }
                            Some(v) => key_vals.push((prop, v)),
                        };
                    }
                    // Scan for existing node with all matching key properties
                    if let Ok(nodes) = self.storage.get_nodes_by_label(&c.label) {
                        for node in &nodes {
                            let all_match = c.properties.iter().all(|prop| {
                                node.properties.get(prop)
                                    == props.get(prop)
                            });
                            if all_match {
                                return Err(EvalError::ExecutionError(format!(
                                    "Node already exists with label `{}` and key {:?}",
                                    c.label,
                                    c.properties.iter()
                                        .map(|p| format!("{}={:?}", p, props.get(p)))
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                )));
                            }
                        }
                    }
                }
                ConstraintType::Type => {
                    let Some(type_name) = &c.type_name else { continue };
                    for prop in &c.properties {
                        if let Some(val) = props.get(prop) {
                            if !Self::value_matches_type(val, type_name) {
                                return Err(EvalError::ExecutionError(format!(
                                    "Property `{}` must be of type `{}` on node with label `{}`",
                                    prop, type_name, c.label
                                )));
                            }
                        }
                    }
                }
                ConstraintType::Temporal => {
                    if c.properties.len() < 3 {
                        continue;
                    }
                    let key_prop = &c.properties[0];
                    let from_prop = &c.properties[1];
                    let to_prop = &c.properties[2];
                    match props.get(key_prop) {
                        None | Some(Value::Null) => {
                            return Err(EvalError::ExecutionError(format!(
                                "TEMPORAL key property `{}` cannot be null on node with label `{}`",
                                key_prop, c.label
                            )));
                        }
                        _ => {}
                    }
                    if let Ok(nodes) = self.storage.get_nodes_by_label(&c.label) {
                        let new_from =
                            Self::parse_temporal_value(props.get(from_prop));
                        let new_to =
                            Self::parse_temporal_value(props.get(to_prop));
                        for node in &nodes {
                            if node.properties.get(key_prop) != props.get(key_prop) {
                                continue;
                            }
                            let existing_from =
                                Self::parse_temporal_value(node.properties.get(from_prop));
                            let existing_to =
                                Self::parse_temporal_value(node.properties.get(to_prop));
                            if Self::temporal_ranges_overlap(
                                new_from, new_to, existing_from, existing_to,
                            ) {
                                return Err(EvalError::ExecutionError(format!(
                                    "TEMPORAL overlap on node with label `{}` and key `{}`",
                                    c.label,
                                    props.get(key_prop)
                                        .map(|v| format!("{:?}", v))
                                        .unwrap_or_default()
                                )));
                            }
                        }
                    }
                }
                ConstraintType::Domain => {
                    if c.allowed_values.is_empty() {
                        continue;
                    }
                    for prop in &c.properties {
                        if let Some(val) = props.get(prop) {
                            if matches!(val, Value::Null) {
                                continue;
                            }
                            if !c.allowed_values.iter().any(|a| Self::values_equal(a, val))
                            {
                                return Err(EvalError::ExecutionError(format!(
                                    "Property `{}` value {:?} is not in allowed domain for label `{}`",
                                    prop, val, c.label
                                )));
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Validate relationship properties against stored constraints.
    fn check_relationship_constraints(
        &self,
        rel_type: &str,
        props: &HashMap<String, Value>,
        start_id: &str,
        end_id: &str,
    ) -> Result<(), EvalError> {
        let constraints = self.storage.load_constraints()?;
        for c in &constraints {
            if c.entity_type != ConstraintEntityType::Relationship {
                continue;
            }
            if c.label != rel_type {
                continue;
            }
            match c.constraint_type {
                ConstraintType::Unique => {
                    let Some(prop) = c.properties.first() else { continue };
                    let val = match props.get(prop) {
                        None | Some(Value::Null) => continue,
                        Some(v) => v,
                    };
                    if let Ok(edges) = self.storage.get_edges_by_type(rel_type) {
                        for edge in &edges {
                            if edge.start_node == start_id
                                && edge.end_node == end_id
                                && edge.properties.get(prop) == Some(val)
                            {
                                return Err(EvalError::ExecutionError(format!(
                                    "Relationship already exists with type `{}` and property `{}` = {:?}",
                                    rel_type, prop, val
                                )));
                            }
                        }
                    }
                }
                ConstraintType::Exists => {
                    for prop in &c.properties {
                        match props.get(prop) {
                            None | Some(Value::Null) => {
                                return Err(EvalError::ExecutionError(format!(
                                    "Required property `{}` is missing on relationship with type `{}`",
                                    prop, rel_type
                                )));
                            }
                            _ => {}
                        }
                    }
                }
                ConstraintType::Relationship => {
                    // Relationship key: all key properties must be non-null
                    for prop in &c.properties {
                        match props.get(prop) {
                            None | Some(Value::Null) => {
                                return Err(EvalError::ExecutionError(format!(
                                    "RELATIONSHIP KEY property `{}` cannot be null on relationship with type `{}`",
                                    prop, rel_type
                                )));
                            }
                            _ => {}
                        };
                    }
                    // Check for existing relationship with matching key
                    if let Ok(edges) = self.storage.get_edges_by_type(rel_type) {
                        for edge in &edges {
                            if edge.start_node == start_id
                                && edge.end_node == end_id
                                && c.properties.iter().all(|prop| {
                                    edge.properties.get(prop)
                                        == props.get(prop)
                                })
                            {
                                return Err(EvalError::ExecutionError(format!(
                                    "Relationship already exists with type `{}` and key {:?}",
                                    rel_type,
                                    c.properties.iter()
                                        .map(|p| format!("{}={:?}", p, props.get(p)))
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                )));
                            }
                        }
                    }
                }
                ConstraintType::Type => {
                    let Some(type_name) = &c.type_name else { continue };
                    for prop in &c.properties {
                        if let Some(val) = props.get(prop) {
                            if !Self::value_matches_type(val, type_name) {
                                return Err(EvalError::ExecutionError(format!(
                                    "Property `{}` must be of type `{}` on relationship with type `{}`",
                                    prop, type_name, rel_type
                                )));
                            }
                        }
                    }
                }
                ConstraintType::Temporal => {
                    if c.properties.len() < 3 {
                        continue;
                    }
                    let key_prop = &c.properties[0];
                    let from_prop = &c.properties[1];
                    let to_prop = &c.properties[2];
                    match props.get(key_prop) {
                        None | Some(Value::Null) => {
                            return Err(EvalError::ExecutionError(format!(
                                "TEMPORAL key property `{}` cannot be null on relationship `{}`",
                                key_prop, rel_type
                            )));
                        }
                        _ => {}
                    }
                    let new_from = Self::parse_temporal_value(props.get(from_prop));
                    let new_to = Self::parse_temporal_value(props.get(to_prop));
                    if let Ok(edges) = self.storage.get_edges_by_type(rel_type) {
                        for edge in &edges {
                            if edge.properties.get(key_prop) != props.get(key_prop) {
                                continue;
                            }
                            let existing_from =
                                Self::parse_temporal_value(edge.properties.get(from_prop));
                            let existing_to =
                                Self::parse_temporal_value(edge.properties.get(to_prop));
                            if Self::temporal_ranges_overlap(
                                new_from, new_to, existing_from, existing_to,
                            ) {
                                return Err(EvalError::ExecutionError(format!(
                                    "TEMPORAL overlap on relationship `{}` with key `{}`",
                                    rel_type,
                                    props.get(key_prop)
                                        .map(|v| format!("{:?}", v))
                                        .unwrap_or_default()
                                )));
                            }
                        }
                    }
                }
                ConstraintType::Domain => {
                    if c.allowed_values.is_empty() {
                        continue;
                    }
                    for prop in &c.properties {
                        if let Some(val) = props.get(prop) {
                            if matches!(val, Value::Null) {
                                continue;
                            }
                            if !c.allowed_values.iter().any(|a| Self::values_equal(a, val))
                            {
                                return Err(EvalError::ExecutionError(format!(
                                    "Property `{}` value {:?} is not in allowed domain for relationship `{}`",
                                    prop, val, rel_type
                                )));
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn execute_pattern_create_segment(
        &self,
        row: &mut Row,
        pattern: &Pattern,
        stats: &mut QueryStats,
        params: &HashMap<String, Value>,
    ) -> Result<(), EvalError> {
        let mut resolved_node_ids = Vec::with_capacity(pattern.nodes.len());
        let mut path_node_values = Vec::with_capacity(pattern.nodes.len());
        let mut path_edge_values = Vec::with_capacity(pattern.edges.len());

        for node_pat in &pattern.nodes {
            if let Some((existing_id, existing_value)) =
                self.resolve_pipeline_node_binding(row, node_pat, params)?
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

            let mut props = evaluate_pattern_properties(&node_pat.properties, row, params)?;
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
            self.check_node_constraints(&node_pat.labels, &props)?;
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

        for (edge_index, edge_pat) in pattern.edges.iter().enumerate() {
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
            let edge_props: HashMap<String, Value> =
                evaluate_pattern_properties(&edge_pat.properties, row, params)?
                    .into_iter()
                    .collect();
            self.check_relationship_constraints(
                &rel_type,
                &edge_props,
                start_node,
                end_node,
            )?;
            let edge = self.persist_edge_record(EdgeRecord {
                id: id.clone(),
                start_node: start_node.clone(),
                end_node: end_node.clone(),
                edge_type: rel_type.clone(),
                properties: edge_props.into_iter().collect(),
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

        if let Some(path_var) = &pattern.path_variable {
            row.insert(
                path_var.clone(),
                path_value(path_node_values, path_edge_values),
            );
        }

        Ok(())
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
        let pattern_segments = create.pattern.split_segments();

        for base_row in base_rows {
            let mut row = base_row.clone();
            for segment in &pattern_segments {
                self.execute_pattern_create_segment(&mut row, segment, stats, params)?;
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
                // If the node variable is already bound in the row, reuse it
                // instead of doing a fresh lookup. This preserves pipeline
                // bindings from MATCH, UNWIND, and prior MERGE clauses.
                if let Some((_existing_id, existing_node)) =
                    self.resolve_pipeline_node_binding(base_row, node_pat, params)?
                {
                    let mut row = base_row.clone();
                    let mut node_val = existing_node;
                    // ON MATCH SET for pipeline-bound (already-existing) nodes
                    if !merge.on_match.is_empty() {
                        if let Some(var) = &node_pat.variable {
                            row.insert(var.clone(), node_val.clone());
                        }
                        self.execute_set_clause(
                            std::slice::from_mut(&mut row),
                            &merge.on_match,
                            params,
                            stats,
                        )?;
                        if let Some(var) = &node_pat.variable {
                            if let Some(updated) = row.get(var) {
                                node_val = updated.clone();
                            }
                        }
                    }
                    if let Some(var) = &node_pat.variable {
                        row.insert(var.clone(), node_val);
                    }
                    next_rows.push(row);
                    continue;
                }

                let merge_props =
                    evaluate_pattern_properties(&node_pat.properties, base_row, params)?;
                let catalog = IndexCatalog::new(self.storage.as_ref());

                let node_val = if let Some(cached_val) =
                    self.find_in_merge_cache(labels, &merge_props)
                {
                    cached_val
                } else {
                    if catalog.has_preferred_node_lookup_index(labels, &merge_props)? {
                        self.hot_path_trace.mark_merge_schema_lookup();
                    } else {
                        self.hot_path_trace.mark_merge_scan_fallback();
                    }
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
                        // ON MATCH SET items: apply to matched row, re-extract updated value
                        let mut matched_val = existing;
                        if !merge.on_match.is_empty() {
                            let mut match_row = base_row.clone();
                            if let Some(var) = &node_pat.variable {
                                match_row.insert(var.clone(), matched_val.clone());
                            }
                            self.execute_set_clause(
                                std::slice::from_mut(&mut match_row),
                                &merge.on_match,
                                params,
                                stats,
                            )?;
                            if let Some(var) = &node_pat.variable {
                                if let Some(Value::Object(updated_props)) = match_row.get(var) {
                                    let persisted: HashMap<String, Value> =
                                        updated_props.clone().into_iter().collect();
                                    self.persist_node_props(&persisted)?;
                                    let serialized = Value::Object(updated_props.clone());
                                    self.cache_merge_node(labels, &merge_props, &serialized);
                                    matched_val = serialized;
                                }
                            }
                        }
                        matched_val
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
                        self.check_node_constraints(labels, &props)?;
                        self.persist_node_props(&props)?;
                        stats.nodes_created += 1;
                        stats.properties_set += node_pat.properties.len();
                        let mut created = serde_json::to_value(&props)
                            .map_err(|e| EvalError::SerializationError(e.to_string()))?;

                        // ON CREATE SET items
                        if !merge.on_create.is_empty() {
                            let mut create_row = base_row.clone();
                            if let Some(var) = &node_pat.variable {
                                create_row.insert(var.clone(), created.clone());
                            }
                            self.execute_set_clause(
                                std::slice::from_mut(&mut create_row),
                                &merge.on_create,
                                params,
                                stats,
                            )?;
                            if let Some(var) = &node_pat.variable {
                                if let Some(Value::Object(updated_props)) = create_row.get(var) {
                                    let persisted: HashMap<String, Value> =
                                        updated_props.clone().into_iter().collect();
                                    self.persist_node_props(&persisted)?;
                                    created = Value::Object(updated_props.clone());
                                }
                            }
                        }
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

        // Relationship MERGE: for each edge in the pattern, match-or-create
        for (edge_idx, edge_pat) in merge.pattern.edges.iter().enumerate() {
            let mut next_rows = pooled_binding_rows();
            for base_row in &current_rows {
                let start_node = &merge.pattern.nodes[edge_idx];
                let end_node = &merge.pattern.nodes[edge_idx + 1];
                let edge_type = edge_pat
                    .rel_type
                    .clone()
                    .unwrap_or_else(|| "REL".to_string());

                let start_id = self.resolve_bound_node_id(base_row, start_node)?;
                let end_id = self.resolve_bound_node_id(base_row, end_node)?;

                let existing_edges = self.storage.get_edges_by_type(&edge_type)?;
                let found = existing_edges.iter().find(|e| {
                    e.start_node == start_id && e.end_node == end_id
                });
                let (edge_val, is_new) = if let Some(edge) = found {
                    let mut props: HashMap<String, Value> =
                        edge.properties.clone().into_iter().collect();
                    props.insert("_id".to_string(), Value::String(edge.id.clone()));
                    props.insert("_type".to_string(), Value::String(edge.edge_type.clone()));
                    props.insert("_start".to_string(), Value::String(edge.start_node.clone()));
                    props.insert("_end".to_string(), Value::String(edge.end_node.clone()));
                    (serde_json::to_value(&props)
                        .map_err(|e| EvalError::SerializationError(e.to_string()))?,
                     false)
                } else {
                    let id = format!("edge:{}", Uuid::new_v4());
                    let edge_record = EdgeRecord {
                        id,
                        start_node: start_id.clone(),
                        end_node: end_id.clone(),
                        edge_type: edge_type.clone(),
                        properties: BTreeMap::new(),
                        created_at_unix_ms: 0,
                        updated_at_unix_ms: 0,
                    };
                    self.storage.put_edge_record(&edge_record)?;
                    stats.relationships_created += 1;
                    let mut props: HashMap<String, Value> = HashMap::new();
                    props.insert("_id".to_string(), Value::String(edge_record.id.clone()));
                    props.insert("_type".to_string(), Value::String(edge_type));
                    props.insert("_start".to_string(), Value::String(start_id));
                    props.insert("_end".to_string(), Value::String(end_id));
                    (serde_json::to_value(&props)
                        .map_err(|e| EvalError::SerializationError(e.to_string()))?,
                     true)
                };

                let mut row = base_row.clone();
                if let Some(var) = &edge_pat.variable {
                    row.insert(var.clone(), edge_val);
                }

                // Apply ON CREATE/ON MATCH SET for relationship
                if is_new && !merge.on_create.is_empty() {
                    self.execute_set_clause(
                        std::slice::from_mut(&mut row),
                        &merge.on_create,
                        params,
                        stats,
                    )?;
                } else if !is_new && !merge.on_match.is_empty() {
                    self.execute_set_clause(
                        std::slice::from_mut(&mut row),
                        &merge.on_match,
                        params,
                        stats,
                    )?;
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

    fn resolve_bound_node_id(
        &self,
        row: &Row,
        node_pat: &NodePattern,
    ) -> Result<String, EvalError> {
        let Some(var) = &node_pat.variable else {
            return Err(EvalError::ExecutionError(
                "MERGE relationship requires bound node variables".into(),
            ));
        };
        let Some(Value::Object(props)) = row.get(var) else {
            return Err(EvalError::ExecutionError(format!(
                "node variable '{}' is not bound in MERGE relationship",
                var
            )));
        };
        let id = props
            .get("_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                EvalError::ExecutionError(format!(
                    "node variable '{}' is missing _id",
                    var
                ))
            })?;
        Ok(id.to_string())
    }

    fn match_relationship_pattern(
        &self,
        base_rows: &[Row],
        pattern: &Pattern,
        params: &HashMap<String, Value>,
        where_expression: Option<&Expression>,
    ) -> Result<Vec<Row>, EvalError> {
        let segments = pattern.split_segments();
        if segments.len() > 1 {
            let mut current_rows = base_rows.to_vec();
            for segment in &segments {
                current_rows = if segment.edges.is_empty() {
                    self.execute_node_match_clause(&current_rows, segment, params, None)?
                } else {
                    self.match_connected_relationship_pattern(
                        &current_rows,
                        segment,
                        params,
                        where_expression,
                    )?
                };
            }
            return Ok(current_rows);
        }

        self.match_connected_relationship_pattern(base_rows, pattern, params, where_expression)
    }

    fn match_connected_relationship_pattern(
        &self,
        base_rows: &[Row],
        pattern: &Pattern,
        params: &HashMap<String, Value>,
        where_expression: Option<&Expression>,
    ) -> Result<Vec<Row>, EvalError> {
        if pattern.nodes.len() < 2 || pattern.edges.len() + 1 != pattern.nodes.len() {
            return Err(EvalError::ExecutionError(
                "relationship MATCH pattern is structurally invalid".to_string(),
            ));
        }
        let mut rows = pooled_binding_rows();
        let start_pattern = &pattern.nodes[0];
        for base_row in base_rows {
            let start_candidates =
                self.bound_or_matching_node_props(base_row, start_pattern, params)?;
            for start_props in start_candidates {
                let start_value = serde_json::to_value(&start_props)
                    .map_err(|e| EvalError::SerializationError(e.to_string()))?;
                let mut row = base_row.clone();
                if let Some(var) = &start_pattern.variable {
                    row.insert(var.clone(), start_value.clone());
                }

                let mut matched_rows = Vec::new();
                self.expand_relationship_chain(
                    pattern,
                    params,
                    0,
                    where_expression,
                    row,
                    start_props,
                    vec![start_value],
                    Vec::new(),
                    0,
                    &mut matched_rows,
                )?;

                if pattern.shortest_path {
                    if let Some(best) = matched_rows.into_iter().min_by_key(|matched| matched.hops)
                    {
                        rows.push(best.row);
                    }
                } else {
                    rows.extend(matched_rows.into_iter().map(|matched| matched.row));
                }
            }
        }
        Ok(rows)
    }

    #[allow(clippy::too_many_arguments)]
    fn expand_relationship_chain(
        &self,
        pattern: &Pattern,
        params: &HashMap<String, Value>,
        edge_index: usize,
        where_expression: Option<&Expression>,
        row: Row,
        current_node_props: HashMap<String, Value>,
        node_values: Vec<Value>,
        edge_values: Vec<Value>,
        total_hops: usize,
        rows: &mut Vec<RelationshipMatchRow>,
    ) -> Result<(), EvalError> {
        if edge_index == pattern.edges.len() {
            let mut final_row = row;
            if let Some(path_var) = &pattern.path_variable {
                final_row.insert(path_var.clone(), path_value(node_values, edge_values));
            }
            rows.push(RelationshipMatchRow {
                row: final_row,
                hops: total_hops,
            });
            return Ok(());
        }

        let edge_pattern = &pattern.edges[edge_index];
        let next_node_pattern = &pattern.nodes[edge_index + 1];
        for step_match in self.relationship_step_matches(
            &row,
            &current_node_props,
            edge_pattern,
            next_node_pattern,
            params,
            where_expression,
        )? {
            let mut next_row = row.clone();
            if let Some(var) = &edge_pattern.variable {
                next_row.insert(var.clone(), step_match.edge_binding_value.clone());
            }
            if let Some(var) = &next_node_pattern.variable {
                next_row.insert(var.clone(), step_match.next_value.clone());
            }

            let mut next_node_values = node_values.clone();
            next_node_values.extend(step_match.node_values_tail.clone());
            let mut next_edge_values = edge_values.clone();
            next_edge_values.extend(step_match.edge_values.clone());

            self.expand_relationship_chain(
                pattern,
                params,
                edge_index + 1,
                where_expression,
                next_row,
                step_match.next_props,
                next_node_values,
                next_edge_values,
                total_hops + step_match.hops,
                rows,
            )?;
        }

        Ok(())
    }

    fn relationship_step_matches(
        &self,
        row: &Row,
        current_node_props: &HashMap<String, Value>,
        edge_pattern: &EdgePattern,
        end_pattern: &NodePattern,
        params: &HashMap<String, Value>,
        where_expression: Option<&Expression>,
    ) -> Result<Vec<RelationshipStepMatch>, EvalError> {
        if edge_pattern.min_hops.is_none() && edge_pattern.max_hops.is_none() {
            return self.fixed_relationship_step_matches(
                row,
                current_node_props,
                edge_pattern,
                end_pattern,
                params,
                where_expression,
            );
        }

        self.variable_relationship_step_matches(
            row,
            current_node_props,
            edge_pattern,
            end_pattern,
            params,
            where_expression,
        )
    }

    fn fixed_relationship_step_matches(
        &self,
        row: &Row,
        current_node_props: &HashMap<String, Value>,
        edge_pattern: &EdgePattern,
        end_pattern: &NodePattern,
        params: &HashMap<String, Value>,
        where_expression: Option<&Expression>,
    ) -> Result<Vec<RelationshipStepMatch>, EvalError> {
        let Some(current_node_id) = node_id(current_node_props) else {
            return Ok(Vec::new());
        };
        let expected_edge_props =
            evaluate_pattern_properties(&edge_pattern.properties, row, params)?;
        let expected_end_props = evaluate_pattern_properties(&end_pattern.properties, row, params)?;
        let mut matches = Vec::new();

        for edge in self.relationship_candidates(
            current_node_id,
            edge_pattern,
            &expected_edge_props,
            row,
            params,
            where_expression,
        )? {
            if !edge_matches_pattern(&edge, &expected_edge_props) {
                continue;
            }
            if !bound_edge_matches_row(row, edge_pattern.variable.as_deref(), &edge) {
                continue;
            }
            let Some(end_id) = related_node_id(current_node_id, &edge, &edge_pattern.direction)
            else {
                continue;
            };
            let Some(end_props) = self.node_props_by_id(end_id)? else {
                continue;
            };
            if !node_matches_pattern(&end_props, &end_pattern.labels, &expected_end_props) {
                continue;
            }
            if !bound_node_matches_row(row, end_pattern.variable.as_deref(), &end_props) {
                continue;
            }

            let edge_value = edge_record_to_value(&edge)?;
            let end_value = serde_json::to_value(&end_props)
                .map_err(|e| EvalError::SerializationError(e.to_string()))?;
            matches.push(RelationshipStepMatch {
                next_props: end_props,
                next_value: end_value.clone(),
                edge_binding_value: edge_value.clone(),
                node_values_tail: vec![end_value],
                edge_values: vec![edge_value],
                hops: 1,
            });
        }

        Ok(matches)
    }

    fn variable_relationship_step_matches(
        &self,
        row: &Row,
        current_node_props: &HashMap<String, Value>,
        edge_pattern: &EdgePattern,
        end_pattern: &NodePattern,
        params: &HashMap<String, Value>,
        where_expression: Option<&Expression>,
    ) -> Result<Vec<RelationshipStepMatch>, EvalError> {
        let min_hops = edge_pattern.min_hops.unwrap_or(1);
        let max_hops = edge_pattern
            .max_hops
            .unwrap_or(VAR_LENGTH_UNBOUNDED_MAX_HOPS)
            .max(min_hops);
        let Some(start_id) = node_id(current_node_props).map(str::to_string) else {
            return Ok(Vec::new());
        };
        let current_value = serde_json::to_value(current_node_props)
            .map_err(|e| EvalError::SerializationError(e.to_string()))?;
        let expected_end_props = evaluate_pattern_properties(&end_pattern.properties, row, params)?;
        let expected_edge_props =
            evaluate_pattern_properties(&edge_pattern.properties, row, params)?;
        let mut frontier = VecDeque::new();
        let mut visited = HashSet::new();
        let mut matches = Vec::new();

        frontier.push_back((
            start_id.clone(),
            0_u32,
            vec![start_id.clone()],
            Vec::<EdgeRecord>::new(),
        ));
        visited.insert((start_id.clone(), 0_u32));

        while let Some((current_id, depth, path_node_ids, path_edges)) = frontier.pop_front() {
            Self::check_current_request_context()?;
            if depth >= min_hops {
                let end_props = if depth == 0 {
                    Some(current_node_props.clone())
                } else {
                    self.node_props_by_id(&current_id)?
                };
                if let Some(end_props) = end_props {
                    if node_matches_pattern(&end_props, &end_pattern.labels, &expected_end_props)
                        && bound_node_matches_row(row, end_pattern.variable.as_deref(), &end_props)
                    {
                        let end_value = if depth == 0 {
                            current_value.clone()
                        } else {
                            serde_json::to_value(&end_props)
                                .map_err(|e| EvalError::SerializationError(e.to_string()))?
                        };
                        let node_values_tail = if depth == 0 {
                            Vec::new()
                        } else {
                            path_node_ids
                                .iter()
                                .skip(1)
                                .map(|node_id| {
                                    let props =
                                        self.node_props_by_id(node_id)?.ok_or_else(|| {
                                            EvalError::ExecutionError(format!(
                                                "path node '{}' disappeared during traversal",
                                                node_id
                                            ))
                                        })?;
                                    serde_json::to_value(&props)
                                        .map_err(|e| EvalError::SerializationError(e.to_string()))
                                })
                                .collect::<Result<Vec<_>, _>>()?
                        };
                        let edge_values = path_edges
                            .iter()
                            .map(edge_record_to_value)
                            .collect::<Result<Vec<_>, _>>()?;
                        matches.push(RelationshipStepMatch {
                            next_props: end_props,
                            next_value: end_value,
                            edge_binding_value: Value::Array(edge_values.clone()),
                            node_values_tail,
                            edge_values,
                            hops: depth as usize,
                        });
                    }
                }
            }

            if depth >= max_hops {
                continue;
            }

            for edge in self.relationship_candidates(
                &current_id,
                edge_pattern,
                &expected_edge_props,
                row,
                params,
                where_expression,
            )? {
                if !edge_matches_pattern(&edge, &expected_edge_props) {
                    continue;
                }
                let Some(next_id) = related_node_id(&current_id, &edge, &edge_pattern.direction)
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

        Ok(matches)
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

    fn matching_node_props_with_where(
        &self,
        pattern: &NodePattern,
        row: &Row,
        params: &HashMap<String, Value>,
        where_expression: Option<&Expression>,
    ) -> Result<Vec<HashMap<String, Value>>, EvalError> {
        let expected_props = evaluate_pattern_properties(&pattern.properties, row, params)?;
        if let Some(range_predicate) =
            extract_node_range_predicate(pattern, where_expression, row, params)?
        {
            return self.lookup_matching_node_props_by_range(
                &pattern.labels,
                &expected_props,
                &range_predicate.property,
                range_predicate.comparison,
                &range_predicate.value,
            );
        }
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

    fn lookup_matching_node_props_by_range(
        &self,
        labels: &[String],
        expected_props: &HashMap<String, Value>,
        property: &str,
        comparison: CatalogRangeIndexComparison,
        value: &Value,
    ) -> Result<Vec<HashMap<String, Value>>, EvalError> {
        let catalog = IndexCatalog::new(self.storage.as_ref());
        let resolver = self.knowledge_policy_resolver()?;
        let mut out = Vec::new();
        for node in
            catalog.lookup_nodes_by_range(labels, property, comparison, value, expected_props)?
        {
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

    fn node_visible_under_policy(
        &self,
        node: &NodeRecord,
        resolver: &Resolver,
    ) -> Result<bool, EvalError> {
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

    fn edge_visible_under_policy(
        &self,
        edge: &EdgeRecord,
        resolver: &Resolver,
    ) -> Result<bool, EvalError> {
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
        let persisted = self
            .storage
            .get_knowledge_policy_access_metadata(entity_id)?;
        let pending = self.access_flusher.pending_mutation(entity_id);
        Ok(merge_access_metadata(persisted, pending.as_ref()))
    }

    fn apply_on_access_for_node(
        &self,
        node: &NodeRecord,
        resolver: &Resolver,
    ) -> Result<(), EvalError> {
        self.apply_on_access_mutations(
            &node.id,
            resolver
                .resolve_node(&node.labels)
                .and_then(|binding| binding.promotion_policy)
                .or_else(|| resolver.resolve_node_promotion(&node.labels)),
        )
    }

    fn apply_on_access_for_edge(
        &self,
        edge: &EdgeRecord,
        resolver: &Resolver,
    ) -> Result<(), EvalError> {
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
}

struct NodeRangePredicate {
    property: String,
    comparison: CatalogRangeIndexComparison,
    value: Value,
}

fn extract_node_range_predicate(
    pattern: &NodePattern,
    where_expression: Option<&Expression>,
    row: &Row,
    params: &HashMap<String, Value>,
) -> Result<Option<NodeRangePredicate>, EvalError> {
    let Some(variable) = pattern.variable.as_deref() else {
        return Ok(None);
    };
    let Some(expression) = where_expression else {
        return Ok(None);
    };

    match expression {
        Expression::Comparison { operands, op } => {
            let Some(comparison) = range_comparison_from_operator(op) else {
                return Ok(None);
            };

            if let Expression::PropertyAccess {
                variable: left_variable,
                property,
            } = &operands.left
            {
                if left_variable == variable {
                    let value = eval_expression(&operands.right, row, params)?;
                    return Ok(
                        is_range_comparable_value(&value).then(|| NodeRangePredicate {
                            property: property.clone(),
                            comparison,
                            value,
                        }),
                    );
                }
            }

            if let Expression::PropertyAccess {
                variable: right_variable,
                property,
            } = &operands.right
            {
                if right_variable == variable {
                    let value = eval_expression(&operands.left, row, params)?;
                    return Ok(
                        is_range_comparable_value(&value).then(|| NodeRangePredicate {
                            property: property.clone(),
                            comparison: invert_range_comparison(comparison),
                            value,
                        }),
                    );
                }
            }

            Ok(None)
        }
        _ => Ok(None),
    }
}

fn range_comparison_from_operator(operator: &str) -> Option<CatalogRangeIndexComparison> {
    match operator {
        ">" => Some(CatalogRangeIndexComparison::GreaterThan),
        ">=" => Some(CatalogRangeIndexComparison::GreaterThanOrEqual),
        "<" => Some(CatalogRangeIndexComparison::LessThan),
        "<=" => Some(CatalogRangeIndexComparison::LessThanOrEqual),
        _ => None,
    }
}

fn invert_range_comparison(comparison: CatalogRangeIndexComparison) -> CatalogRangeIndexComparison {
    match comparison {
        CatalogRangeIndexComparison::GreaterThan => CatalogRangeIndexComparison::LessThan,
        CatalogRangeIndexComparison::GreaterThanOrEqual => {
            CatalogRangeIndexComparison::LessThanOrEqual
        }
        CatalogRangeIndexComparison::LessThan => CatalogRangeIndexComparison::GreaterThan,
        CatalogRangeIndexComparison::LessThanOrEqual => {
            CatalogRangeIndexComparison::GreaterThanOrEqual
        }
    }
}

fn is_range_comparable_value(value: &Value) -> bool {
    matches!(value, Value::Number(_) | Value::String(_))
}

#[path = "eval_engine_policy.rs"]
mod eval_engine_policy;

// ── Aggregation helpers ────────────────────────────────────────────────────

fn is_agg_function(expr: &Expression) -> bool {
    match expr {
        Expression::FunctionCall { name, .. } => {
            let lower = name.to_ascii_lowercase();
            matches!(lower.as_str(), "avg" | "sum" | "min" | "max" | "count")
        }
        _ => false,
    }
}

fn has_aggregation_items(items: &[ReturnItem]) -> bool {
    items.iter().any(|item| is_agg_function(&item.expression))
}

/// Like has_aggregation_items but count-only doesn't qualify (for final RETURN).
fn has_non_count_aggregation(items: &[ReturnItem]) -> bool {
    items.iter().any(|item| {
        matches!(&item.expression, Expression::FunctionCall { name, .. }
            if is_agg_function(&item.expression) && !name.eq_ignore_ascii_case("count"))
    })
}

fn agg_func_info(expr: &Expression) -> Option<(&str, Option<&Expression>)> {
    match expr {
        Expression::FunctionCall { name, args, .. } if is_agg_function(expr) => {
            Some((name.as_str(), args.first()))
        }
        _ => None,
    }
}

/// Build a single row with identity values for aggregation on empty input.
/// count→0, sum→0, avg→null, min→null, max→null
fn aggregate_identity_row(
    items: &[ReturnItem],
    params: &HashMap<String, Value>,
) -> Result<Row, EvalError> {
    let mut row = Row::new();
    for item in items {
        let col = column_name(item);
        let val = match &item.expression {
            Expression::FunctionCall { name, .. } => match name.to_ascii_lowercase().as_str() {
                "count" => Value::from(0),
                "sum" => Value::from(0),
                "avg" | "min" | "max" => Value::Null,
                _ => Value::Null,
            },
            _ => Value::Null,
        };
        row.insert(col, val);
    }
    Ok(row)
}

fn apply_aggregation_to_rows(
    rows: &[Row],
    items: &[ReturnItem],
    params: &HashMap<String, Value>,
) -> Result<Vec<Row>, EvalError> {
    let non_agg_items: Vec<&ReturnItem> = items
        .iter()
        .filter(|item| !is_agg_function(&item.expression))
        .collect();

    if non_agg_items.is_empty() {
        let mut row = Row::new();
        for item in items {
            let col = column_name(item);
            if let Some((fn_name, arg)) = agg_func_info(&item.expression) {
                let borrowed: Vec<&Row> = rows.iter().collect();
                row.insert(col, compute_agg(fn_name, arg, &borrowed, params)?);
            } else if let Some(first) = rows.first() {
                let projected = project_row(first, std::slice::from_ref(item), params)?;
                if let Some((_, v)) = projected.into_iter().next() {
                    row.insert(col, v);
                }
            }
        }
        return Ok(vec![row]);
    }

    let mut groups: HashMap<Vec<Value>, Vec<&Row>> = HashMap::new();
    for row in rows {
        let key: Vec<Value> = non_agg_items
            .iter()
            .map(|item| eval_expression(&item.expression, row, params).unwrap_or(Value::Null))
            .collect();
        groups.entry(key).or_default().push(row);
    }

    let sort_cols: Vec<String> = non_agg_items.iter().map(|item| column_name(item)).collect();
    let mut result: Vec<(Vec<Value>, Row)> = Vec::new();
    for (key_vals, group_rows) in groups {
        let mut row = Row::new();
        for (item, key_val) in non_agg_items.iter().zip(key_vals.iter()) {
            row.insert(column_name(item), key_val.clone());
        }
        for item in items {
            if let Some((fn_name, arg)) = agg_func_info(&item.expression) {
                let col = column_name(item);
                row.insert(col, compute_agg(fn_name, arg, &group_rows, params)?);
            }
        }
        result.push((key_vals, row));
    }
    result.sort_by(|(ak, _), (bk, _)| {
        for (a, b) in ak.iter().zip(bk.iter()) {
            let ord = compare_json(a, b);
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
    Ok(result.into_iter().map(|(_, row)| row).collect())
}

fn compute_agg(
    fn_name: &str,
    arg: Option<&Expression>,
    rows: &[&Row],
    params: &HashMap<String, Value>,
) -> Result<Value, EvalError> {
    match fn_name.to_ascii_lowercase().as_str() {
        "count" => {
            if let Some(arg_expr) = arg {
                Ok(Value::from(
                    rows.iter()
                        .filter(|row| {
                            eval_expression(arg_expr, row, params)
                                .map(|v| v != Value::Null)
                                .unwrap_or(false)
                        })
                        .count() as i64,
                ))
            } else {
                Ok(Value::from(rows.len() as i64))
            }
        }
        "sum" => {
            let arg = arg.ok_or_else(|| {
                EvalError::ExecutionError("sum() requires an argument".into())
            })?;
            let total: f64 = rows
                .iter()
                .filter_map(|row| eval_expression(arg, row, params).ok()?.as_f64())
                .sum();
            Ok(Value::from(total))
        }
        "avg" => {
            let arg = arg.ok_or_else(|| {
                EvalError::ExecutionError("avg() requires an argument".into())
            })?;
            let values: Vec<f64> = rows
                .iter()
                .filter_map(|row| eval_expression(arg, row, params).ok()?.as_f64())
                .collect();
            if values.is_empty() {
                Ok(Value::Null)
            } else {
                Ok(Value::from(values.iter().sum::<f64>() / values.len() as f64))
            }
        }
        "min" => {
            let arg = arg.ok_or_else(|| {
                EvalError::ExecutionError("min() requires an argument".into())
            })?;
            let min_val = rows
                .iter()
                .filter_map(|row| eval_expression(arg, row, params).ok()?.as_f64())
                .fold(f64::NAN, |a, b| if a.is_nan() { b } else { a.min(b) });
            if min_val.is_nan() {
                Ok(Value::Null)
            } else {
                Ok(Value::from(min_val))
            }
        }
        "max" => {
            let arg = arg.ok_or_else(|| {
                EvalError::ExecutionError("max() requires an argument".into())
            })?;
            let max_val = rows
                .iter()
                .filter_map(|row| eval_expression(arg, row, params).ok()?.as_f64())
                .fold(f64::NAN, |a, b| if a.is_nan() { b } else { a.max(b) });
            if max_val.is_nan() {
                Ok(Value::Null)
            } else {
                Ok(Value::from(max_val))
            }
        }
        _ => Err(EvalError::ExecutionError(format!(
            "unsupported aggregation function: {fn_name}"
        ))),
    }
}

#[path = "eval_engine_tail.rs"]
mod eval_engine_tail;
