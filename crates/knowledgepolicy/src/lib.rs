//! Knowledge-policy runtime contracts for copperdb.
//!
//! This crate ports the first Layer 4 knowledge-policy runtime baseline from
//! NornicDB: typed decay/promotion definitions, deterministic binding
//! compilation, and label/edge/property resolution. Copper storage already
//! persists profile and promotion schemas; this crate owns the runtime logic
//! that will consume those schemas once binding persistence is threaded in.

use copperdb_cypher::{Expression, Parser};
use copperdb_storage::{
    DecayProfileBindingSchema, DecayProfileSchema, KnowledgePolicyAccessMetadata,
    PromotionOnAccessMutationKindSchema, PromotionOnAccessMutationSchema, PromotionPolicySchema,
    PromotionProfileSchema, PromotionWhenClauseSchema,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::f64::consts::LN_2;
use std::sync::{Mutex, RwLock};
use std::thread::ThreadId;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum KnowledgePolicyError {
    #[error("invalid decay function: {0}")]
    InvalidDecayFunction(String),
    #[error("invalid score-from mode: {0}")]
    InvalidScoreFrom(String),
    #[error("invalid scope type: {0}")]
    InvalidScope(String),
    #[error("binding {binding:?} references unknown decay profile {profile:?}")]
    UnknownDecayProfile { binding: String, profile: String },
    #[error("property rule on binding {binding:?} references unknown decay profile {profile:?}")]
    UnknownPropertyProfile { binding: String, profile: String },
    #[error("promotion policy {policy:?} references unknown promotion profile {profile:?}")]
    UnknownPromotionProfile { policy: String, profile: String },
    #[error("promotion policy {policy:?} has invalid predicate {predicate:?}: {message}")]
    InvalidPromotionPredicate {
        policy: String,
        predicate: String,
        message: String,
    },
    #[error("conflict: bindings {left:?} and {right:?} both target {target:?} with order {order}")]
    BindingConflict {
        left: String,
        right: String,
        target: String,
        order: i64,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DecayFunction {
    Exponential,
    Linear,
    Step,
    None,
}

impl TryFrom<&str> for DecayFunction {
    type Error = KnowledgePolicyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_ascii_lowercase().as_str() {
            "" | "exponential" => Ok(Self::Exponential),
            "linear" => Ok(Self::Linear),
            "step" => Ok(Self::Step),
            "none" => Ok(Self::None),
            other => Err(KnowledgePolicyError::InvalidDecayFunction(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ScoreFromMode {
    Created,
    Version,
    Custom,
    LastAccessed,
}

impl TryFrom<&str> for ScoreFromMode {
    type Error = KnowledgePolicyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_ascii_uppercase().as_str() {
            "" | "CREATED" => Ok(Self::Created),
            "VERSION" => Ok(Self::Version),
            "CUSTOM" => Ok(Self::Custom),
            "LAST_ACCESSED" => Ok(Self::LastAccessed),
            other => Err(KnowledgePolicyError::InvalidScoreFrom(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ScopeType {
    Node,
    Edge,
    Property,
}

impl TryFrom<&str> for ScopeType {
    type Error = KnowledgePolicyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_ascii_uppercase().as_str() {
            "" | "NODE" => Ok(Self::Node),
            "EDGE" => Ok(Self::Edge),
            "PROPERTY" => Ok(Self::Property),
            other => Err(KnowledgePolicyError::InvalidScope(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DecayProfileBundle {
    pub name: String,
    pub half_life_seconds: i64,
    pub visibility_threshold: f64,
    pub score_floor: f64,
    pub function: DecayFunction,
    pub scope: ScopeType,
    pub decay_enabled: bool,
    pub score_from: ScoreFromMode,
    pub score_from_property: Option<String>,
    pub enabled: bool,
}

impl TryFrom<&DecayProfileSchema> for DecayProfileBundle {
    type Error = KnowledgePolicyError;

    fn try_from(value: &DecayProfileSchema) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name.clone(),
            half_life_seconds: value.half_life_seconds,
            visibility_threshold: value.visibility_threshold,
            score_floor: value.score_floor,
            function: DecayFunction::try_from(value.function.as_str())?,
            scope: ScopeType::try_from(value.scope.as_str())?,
            decay_enabled: value.decay_enabled,
            score_from: ScoreFromMode::try_from(value.score_from.as_str())?,
            score_from_property: value.score_from_property.clone(),
            enabled: value.enabled,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DecayProfilePropertyRule {
    pub property_path: String,
    pub no_decay: bool,
    pub profile_ref: Option<String>,
    pub half_life_seconds: Option<i64>,
    pub score_floor: Option<f64>,
    pub order: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DecayProfileBinding {
    pub name: String,
    pub target_labels: Vec<String>,
    pub target_edge_type: Option<String>,
    pub is_wildcard: bool,
    pub is_edge: bool,
    pub profile_ref: Option<String>,
    pub no_decay: bool,
    pub visibility_threshold: Option<f64>,
    pub property_rules: Vec<DecayProfilePropertyRule>,
    pub order: i64,
}

impl From<&DecayProfileBindingSchema> for DecayProfileBinding {
    fn from(value: &DecayProfileBindingSchema) -> Self {
        Self {
            name: value.name.clone(),
            target_labels: value.target_labels.clone(),
            target_edge_type: value.target_edge_type.clone(),
            is_wildcard: value.is_wildcard,
            is_edge: value.is_edge,
            profile_ref: value.profile_ref.clone(),
            no_decay: value.no_decay,
            visibility_threshold: value.visibility_threshold,
            property_rules: Vec::new(),
            order: value.order,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PromotionProfileDef {
    pub name: String,
    pub scope: ScopeType,
    pub multiplier: f64,
    pub score_floor: f64,
    pub score_cap: f64,
    pub enabled: bool,
}

impl TryFrom<&PromotionProfileSchema> for PromotionProfileDef {
    type Error = KnowledgePolicyError;

    fn try_from(value: &PromotionProfileSchema) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name.clone(),
            scope: ScopeType::try_from(value.scope.as_str())?,
            multiplier: value.multiplier,
            score_floor: value.score_floor,
            score_cap: value.score_cap,
            enabled: value.enabled,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromotionPolicyWhenClause {
    pub predicate: String,
    pub profile_ref: String,
    pub order: i64,
}

impl From<&PromotionWhenClauseSchema> for PromotionPolicyWhenClause {
    fn from(value: &PromotionWhenClauseSchema) -> Self {
        Self {
            predicate: value.predicate.clone(),
            profile_ref: value.profile_ref.clone(),
            order: value.order,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PromotionOnAccessMutationKind {
    SetLastAccessedNow,
    IncrementAccessCount,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromotionOnAccessMutation {
    pub kind: PromotionOnAccessMutationKind,
}

impl From<&PromotionOnAccessMutationSchema> for PromotionOnAccessMutation {
    fn from(value: &PromotionOnAccessMutationSchema) -> Self {
        let kind = match value.kind {
            PromotionOnAccessMutationKindSchema::SetLastAccessedNow => {
                PromotionOnAccessMutationKind::SetLastAccessedNow
            }
            PromotionOnAccessMutationKindSchema::IncrementAccessCount => {
                PromotionOnAccessMutationKind::IncrementAccessCount
            }
        };
        Self { kind }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PromotionPolicyDef {
    pub name: String,
    pub target_labels: Vec<String>,
    pub target_edge_type: Option<String>,
    pub is_wildcard: bool,
    pub is_edge: bool,
    pub on_access_mutations: Vec<PromotionOnAccessMutation>,
    pub when_clauses: Vec<PromotionPolicyWhenClause>,
    pub enabled: bool,
}

impl From<&PromotionPolicySchema> for PromotionPolicyDef {
    fn from(value: &PromotionPolicySchema) -> Self {
        Self {
            name: value.name.clone(),
            target_labels: value.target_labels.clone(),
            target_edge_type: value.target_edge_type.clone(),
            is_wildcard: value.is_wildcard,
            is_edge: value.is_edge,
            on_access_mutations: value.on_access_mutations.iter().map(Into::into).collect(),
            when_clauses: value.when_clauses.iter().map(Into::into).collect(),
            enabled: value.enabled,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompiledPropertyOverride {
    pub no_decay: bool,
    pub half_life_nanos: i64,
    pub threshold_age_nanos: i64,
    pub decay_floor: f64,
    pub function: DecayFunction,
}

#[derive(Debug, Clone)]
pub struct CompiledPromotionRule {
    pub predicate: String,
    pub expression: Expression,
    pub profile: PromotionProfileDef,
    pub order: i64,
}

impl PartialEq for CompiledPromotionRule {
    fn eq(&self, other: &Self) -> bool {
        self.predicate == other.predicate
            && self.profile == other.profile
            && self.order == other.order
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompiledBinding {
    pub decay_profile: Option<DecayProfileBundle>,
    pub decay_binding: DecayProfileBinding,
    pub promotion_policy: Option<PromotionPolicyDef>,
    pub visibility_threshold: f64,
    pub score_from: ScoreFromMode,
    pub score_from_property: Option<String>,
    pub function: DecayFunction,
    pub half_life_nanos: i64,
    pub threshold_age_nanos: i64,
    pub decay_floor: f64,
    pub no_decay: bool,
    pub has_no_decay_property: bool,
    pub compiled_property_rules: HashMap<String, CompiledPropertyOverride>,
    pub compiled_promotion_rules: Vec<CompiledPromotionRule>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BufferedAccessMutation {
    pub last_accessed_at_unix_ms: Option<i64>,
    pub access_count_delta: u64,
}

pub type AccessMutationBuffer = HashMap<String, BufferedAccessMutation>;

#[derive(Debug, Default)]
pub struct AccessFlusher {
    buffers: Mutex<HashMap<ThreadId, AccessMutationBuffer>>,
}

impl AccessFlusher {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_buffer<T, E, F, Flush>(&self, operation: F, flush: Flush) -> Result<T, E>
    where
        F: FnOnce() -> Result<T, E>,
        Flush: FnOnce(AccessMutationBuffer) -> Result<(), E>,
    {
        let thread_id = std::thread::current().id();
        let owner = {
            let mut buffers = self.buffers.lock().unwrap();
            if let std::collections::hash_map::Entry::Vacant(entry) = buffers.entry(thread_id) {
                entry.insert(HashMap::new());
                true
            } else {
                false
            }
        };

        let result = operation();

        if !owner {
            return result;
        }

        let buffer = self
            .buffers
            .lock()
            .unwrap()
            .remove(&thread_id)
            .unwrap_or_default();
        if result.is_ok() {
            flush(buffer)?;
        }
        result
    }

    pub fn record_policy_access(
        &self,
        entity_id: &str,
        policy: Option<&PromotionPolicyDef>,
        now_unix_ms: i64,
    ) {
        let Some(buffered) = buffered_access_mutation(policy, now_unix_ms) else {
            return;
        };

        let mut buffers = self.buffers.lock().unwrap();
        let Some(buffer) = buffers.get_mut(&std::thread::current().id()) else {
            return;
        };
        let entry = buffer.entry(entity_id.to_string()).or_default();
        if let Some(last_accessed_at_unix_ms) = buffered.last_accessed_at_unix_ms {
            entry.last_accessed_at_unix_ms = Some(
                entry
                    .last_accessed_at_unix_ms
                    .map(|current| current.max(last_accessed_at_unix_ms))
                    .unwrap_or(last_accessed_at_unix_ms),
            );
        }
        entry.access_count_delta = entry
            .access_count_delta
            .saturating_add(buffered.access_count_delta);
    }

    pub fn pending_mutation(&self, entity_id: &str) -> Option<BufferedAccessMutation> {
        self.buffers
            .lock()
            .unwrap()
            .get(&std::thread::current().id())
            .and_then(|buffer| buffer.get(entity_id).cloned())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScoreOutcome {
    pub base_score: f64,
    pub final_score: f64,
    pub suppressed: bool,
}

pub fn merge_access_metadata(
    base: Option<KnowledgePolicyAccessMetadata>,
    pending: Option<&BufferedAccessMutation>,
) -> Option<KnowledgePolicyAccessMetadata> {
    let mut merged = match (base, pending) {
        (None, None) => return None,
        (Some(metadata), None) => metadata,
        (None, Some(_)) => KnowledgePolicyAccessMetadata::default(),
        (Some(metadata), Some(_)) => metadata,
    };
    if let Some(pending) = pending {
        if let Some(last_accessed_at_unix_ms) = pending.last_accessed_at_unix_ms {
            merged.last_accessed_at_unix_ms = Some(
                merged
                    .last_accessed_at_unix_ms
                    .map(|current| current.max(last_accessed_at_unix_ms))
                    .unwrap_or(last_accessed_at_unix_ms),
            );
        }
        merged.access_count = merged
            .access_count
            .saturating_add(pending.access_count_delta);
    }
    Some(merged)
}

pub fn buffered_access_mutation(
    policy: Option<&PromotionPolicyDef>,
    now_unix_ms: i64,
) -> Option<BufferedAccessMutation> {
    let policy = policy?;
    if !policy.enabled || policy.on_access_mutations.is_empty() {
        return None;
    }

    let mut buffered = BufferedAccessMutation::default();
    for mutation in &policy.on_access_mutations {
        match mutation.kind {
            PromotionOnAccessMutationKind::SetLastAccessedNow => {
                buffered.last_accessed_at_unix_ms = Some(
                    buffered
                        .last_accessed_at_unix_ms
                        .map(|current| current.max(now_unix_ms))
                        .unwrap_or(now_unix_ms),
                );
            }
            PromotionOnAccessMutationKind::IncrementAccessCount => {
                buffered.access_count_delta = buffered.access_count_delta.saturating_add(1);
            }
        }
    }

    Some(buffered)
}

pub fn access_metadata_after_policy_access(
    base: Option<KnowledgePolicyAccessMetadata>,
    policy: Option<&PromotionPolicyDef>,
    now_unix_ms: i64,
) -> Option<KnowledgePolicyAccessMetadata> {
    let pending = buffered_access_mutation(policy, now_unix_ms)?;
    merge_access_metadata(base, Some(&pending))
}

pub fn score_binding(
    binding: &CompiledBinding,
    anchor_unix_ms: Option<i64>,
    now_unix_ms: i64,
    matched_promotion: Option<&PromotionProfileDef>,
) -> ScoreOutcome {
    if binding.no_decay {
        return ScoreOutcome {
            base_score: 1.0,
            final_score: 1.0,
            suppressed: false,
        };
    }

    let base_score = anchor_unix_ms
        .map(|anchor| compute_decay_score(binding, now_unix_ms.saturating_sub(anchor)))
        .unwrap_or(1.0);

    let promoted_score = if let Some(profile) = matched_promotion {
        let promoted = base_score * profile.multiplier;
        profile.score_cap.min(profile.score_floor.max(promoted))
    } else {
        base_score
    };
    let final_score = binding.decay_floor.max(promoted_score);

    ScoreOutcome {
        base_score,
        final_score,
        suppressed: final_score < binding.visibility_threshold,
    }
}

#[derive(Debug, Default)]
pub struct BindingTable {
    nodes: RwLock<HashMap<String, CompiledBinding>>,
    edges: RwLock<HashMap<String, CompiledBinding>>,
    wild_node: RwLock<Option<CompiledBinding>>,
    wild_edge: RwLock<Option<CompiledBinding>>,
    promotion_nodes: RwLock<HashMap<String, PromotionPolicyDef>>,
    promotion_edges: RwLock<HashMap<String, PromotionPolicyDef>>,
    wild_promotion_node: RwLock<Option<PromotionPolicyDef>>,
}

impl BindingTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn lookup_node(&self, label_key: &str) -> Option<CompiledBinding> {
        self.nodes
            .read()
            .unwrap()
            .get(label_key)
            .cloned()
            .or_else(|| self.wild_node.read().unwrap().clone())
    }

    pub fn lookup_node_exact(&self, label_key: &str) -> Option<CompiledBinding> {
        self.nodes.read().unwrap().get(label_key).cloned()
    }

    pub fn lookup_edge(&self, edge_type: &str) -> Option<CompiledBinding> {
        self.edges
            .read()
            .unwrap()
            .get(edge_type)
            .cloned()
            .or_else(|| self.wild_edge.read().unwrap().clone())
    }

    pub fn lookup_node_promotion(&self, label_key: &str) -> Option<PromotionPolicyDef> {
        self.promotion_nodes
            .read()
            .unwrap()
            .get(label_key)
            .cloned()
            .or_else(|| self.wild_promotion_node.read().unwrap().clone())
    }

    pub fn lookup_node_promotion_exact(&self, label_key: &str) -> Option<PromotionPolicyDef> {
        self.promotion_nodes.read().unwrap().get(label_key).cloned()
    }

    pub fn lookup_edge_promotion(&self, edge_type: &str) -> Option<PromotionPolicyDef> {
        self.promotion_edges.read().unwrap().get(edge_type).cloned()
    }

    fn set_node(&self, label_key: String, binding: CompiledBinding) {
        self.nodes.write().unwrap().insert(label_key, binding);
    }

    fn set_edge(&self, edge_type: String, binding: CompiledBinding) {
        self.edges.write().unwrap().insert(edge_type, binding);
    }

    fn set_wild_node(&self, binding: CompiledBinding) {
        *self.wild_node.write().unwrap() = Some(binding);
    }

    fn set_wild_edge(&self, binding: CompiledBinding) {
        *self.wild_edge.write().unwrap() = Some(binding);
    }

    fn set_node_promotion(&self, label_key: String, policy: PromotionPolicyDef) {
        self.promotion_nodes
            .write()
            .unwrap()
            .insert(label_key, policy);
    }

    fn set_edge_promotion(&self, edge_type: String, policy: PromotionPolicyDef) {
        self.promotion_edges
            .write()
            .unwrap()
            .insert(edge_type, policy);
    }

    fn set_wild_node_promotion(&self, policy: PromotionPolicyDef) {
        *self.wild_promotion_node.write().unwrap() = Some(policy);
    }
}

#[derive(Debug)]
pub struct Resolver {
    binding_table: BindingTable,
}

impl Resolver {
    pub fn new(binding_table: BindingTable) -> Self {
        Self { binding_table }
    }

    pub fn resolve_node(&self, labels: &[String]) -> Option<CompiledBinding> {
        if labels.is_empty() {
            return self.binding_table.lookup_node("");
        }

        let sorted = binding_label_key(labels);
        if let Some(binding) = self.binding_table.lookup_node_exact(&sorted) {
            return Some(binding);
        }

        let mut sorted_labels = labels.to_vec();
        sorted_labels.sort();

        for subset_size in (1..sorted_labels.len()).rev() {
            let matches = collect_subset_matches(&self.binding_table, &sorted_labels, subset_size);
            if matches.is_empty() {
                continue;
            }
            if matches.len() == 1 {
                return matches.into_iter().next();
            }
            return Some(resolve_conflict(matches));
        }

        self.binding_table.lookup_node("")
    }

    pub fn resolve_edge(&self, edge_type: &str) -> Option<CompiledBinding> {
        self.binding_table.lookup_edge(edge_type)
    }

    pub fn resolve_node_promotion(&self, labels: &[String]) -> Option<PromotionPolicyDef> {
        if labels.is_empty() {
            return self.binding_table.lookup_node_promotion("");
        }

        let sorted = binding_label_key(labels);
        if let Some(policy) = self.binding_table.lookup_node_promotion_exact(&sorted) {
            return Some(policy);
        }

        let mut sorted_labels = labels.to_vec();
        sorted_labels.sort();

        for subset_size in (1..sorted_labels.len()).rev() {
            let matches =
                collect_promotion_subset_matches(&self.binding_table, &sorted_labels, subset_size);
            if matches.is_empty() {
                continue;
            }
            if matches.len() == 1 {
                return matches.into_iter().next();
            }
            return Some(resolve_promotion_conflict(matches));
        }

        self.binding_table.lookup_node_promotion("")
    }

    pub fn resolve_edge_promotion(&self, edge_type: &str) -> Option<PromotionPolicyDef> {
        self.binding_table.lookup_edge_promotion(edge_type)
    }

    pub fn resolve_property(
        &self,
        labels: &[String],
        property_path: &str,
    ) -> Option<CompiledBinding> {
        resolve_property_override(self.resolve_node(labels), property_path)
    }

    pub fn resolve_edge_property(
        &self,
        edge_type: &str,
        property_path: &str,
    ) -> Option<CompiledBinding> {
        resolve_property_override(self.resolve_edge(edge_type), property_path)
    }
}

pub fn build_bundles_by_name(
    schemas: &[DecayProfileSchema],
) -> Result<HashMap<String, DecayProfileBundle>, KnowledgePolicyError> {
    schemas
        .iter()
        .map(|schema| Ok((schema.name.clone(), DecayProfileBundle::try_from(schema)?)))
        .collect()
}

pub fn build_promotion_profiles_by_name(
    schemas: &[PromotionProfileSchema],
) -> Result<HashMap<String, PromotionProfileDef>, KnowledgePolicyError> {
    schemas
        .iter()
        .map(|schema| Ok((schema.name.clone(), PromotionProfileDef::try_from(schema)?)))
        .collect()
}

pub fn build_decay_bindings(schemas: &[DecayProfileBindingSchema]) -> Vec<DecayProfileBinding> {
    schemas.iter().map(DecayProfileBinding::from).collect()
}

pub fn build_promotion_policies_by_name(
    schemas: &[PromotionPolicySchema],
) -> HashMap<String, PromotionPolicyDef> {
    schemas
        .iter()
        .map(|schema| (schema.name.clone(), PromotionPolicyDef::from(schema)))
        .collect()
}

pub fn build_binding_table(
    bundles: &HashMap<String, DecayProfileBundle>,
    bindings: &[DecayProfileBinding],
    promotion_profiles: &HashMap<String, PromotionProfileDef>,
    promotion_policies: &HashMap<String, PromotionPolicyDef>,
) -> Result<BindingTable, KnowledgePolicyError> {
    for binding in bindings {
        if !binding.no_decay {
            if let Some(profile_ref) = &binding.profile_ref {
                if !bundles.contains_key(profile_ref) {
                    return Err(KnowledgePolicyError::UnknownDecayProfile {
                        binding: binding.name.clone(),
                        profile: profile_ref.clone(),
                    });
                }
            }
        }

        for rule in &binding.property_rules {
            if let Some(profile_ref) = &rule.profile_ref {
                if !bundles.contains_key(profile_ref) {
                    return Err(KnowledgePolicyError::UnknownPropertyProfile {
                        binding: binding.name.clone(),
                        profile: profile_ref.clone(),
                    });
                }
            }
        }
    }

    for policy in promotion_policies.values() {
        for clause in &policy.when_clauses {
            if !promotion_profiles.contains_key(&clause.profile_ref) {
                return Err(KnowledgePolicyError::UnknownPromotionProfile {
                    policy: policy.name.clone(),
                    profile: clause.profile_ref.clone(),
                });
            }
        }
    }

    let table = BindingTable::new();
    let mut sorted_policies = promotion_policies.values().cloned().collect::<Vec<_>>();
    sorted_policies.sort_by(|left, right| left.name.cmp(&right.name));
    for policy in sorted_policies {
        if policy.is_edge {
            table.set_edge_promotion(policy.target_edge_type.clone().unwrap_or_default(), policy);
        } else if policy.is_wildcard {
            table.set_wild_node_promotion(policy);
        } else {
            table.set_node_promotion(binding_label_key(&policy.target_labels), policy);
        }
    }
    let mut seen_node_targets: HashMap<String, (String, i64)> = HashMap::new();

    for binding in bindings {
        let compiled = compile_binding(binding, bundles, promotion_profiles, promotion_policies)?;
        if binding.is_wildcard {
            if binding.is_edge {
                table.set_wild_edge(compiled);
            } else {
                table.set_wild_node(compiled);
            }
            continue;
        }

        if binding.is_edge {
            table.set_edge(
                binding.target_edge_type.clone().unwrap_or_default(),
                compiled,
            );
            continue;
        }

        let target = binding_label_key(&binding.target_labels);
        if let Some((previous_name, previous_order)) = seen_node_targets.get(&target) {
            if *previous_order == binding.order {
                return Err(KnowledgePolicyError::BindingConflict {
                    left: previous_name.clone(),
                    right: binding.name.clone(),
                    target,
                    order: binding.order,
                });
            }
            if binding.order < *previous_order {
                table.set_node(target.clone(), compiled);
                seen_node_targets.insert(target, (binding.name.clone(), binding.order));
            }
        } else {
            table.set_node(target.clone(), compiled);
            seen_node_targets.insert(target, (binding.name.clone(), binding.order));
        }
    }

    Ok(table)
}

fn compile_binding(
    binding: &DecayProfileBinding,
    bundles: &HashMap<String, DecayProfileBundle>,
    promotion_profiles: &HashMap<String, PromotionProfileDef>,
    promotion_policies: &HashMap<String, PromotionPolicyDef>,
) -> Result<CompiledBinding, KnowledgePolicyError> {
    let decay_profile = binding
        .profile_ref
        .as_ref()
        .and_then(|profile_ref| bundles.get(profile_ref))
        .cloned();

    let mut compiled = if binding.no_decay {
        CompiledBinding {
            decay_profile: None,
            decay_binding: binding.clone(),
            promotion_policy: None,
            visibility_threshold: 0.0,
            score_from: ScoreFromMode::Created,
            score_from_property: None,
            function: DecayFunction::None,
            half_life_nanos: 0,
            threshold_age_nanos: i64::MAX,
            decay_floor: 0.0,
            no_decay: true,
            has_no_decay_property: false,
            compiled_property_rules: HashMap::new(),
            compiled_promotion_rules: Vec::new(),
        }
    } else {
        let bundle =
            decay_profile
                .clone()
                .ok_or_else(|| KnowledgePolicyError::UnknownDecayProfile {
                    binding: binding.name.clone(),
                    profile: binding.profile_ref.clone().unwrap_or_default(),
                })?;
        let visibility_threshold = binding
            .visibility_threshold
            .unwrap_or(bundle.visibility_threshold);
        let half_life_nanos = bundle.half_life_seconds.saturating_mul(1_000_000_000);
        let mut compiled = CompiledBinding {
            decay_profile: Some(bundle.clone()),
            decay_binding: binding.clone(),
            promotion_policy: None,
            visibility_threshold,
            score_from: bundle.score_from,
            score_from_property: bundle.score_from_property.clone(),
            function: bundle.function,
            half_life_nanos,
            threshold_age_nanos: compute_threshold_age_nanos(
                bundle.function,
                half_life_nanos,
                visibility_threshold,
            ),
            decay_floor: bundle.score_floor,
            no_decay: !bundle.decay_enabled || bundle.function == DecayFunction::None,
            has_no_decay_property: false,
            compiled_property_rules: HashMap::new(),
            compiled_promotion_rules: Vec::new(),
        };

        for rule in &binding.property_rules {
            let override_bundle = rule
                .profile_ref
                .as_ref()
                .and_then(|profile_ref| bundles.get(profile_ref))
                .cloned();
            let function = override_bundle
                .as_ref()
                .map(|bundle| bundle.function)
                .unwrap_or(compiled.function);
            let half_life_nanos = rule
                .half_life_seconds
                .or_else(|| {
                    override_bundle
                        .as_ref()
                        .map(|bundle| bundle.half_life_seconds)
                })
                .unwrap_or(compiled.half_life_nanos / 1_000_000_000)
                .saturating_mul(1_000_000_000);
            let decay_floor = rule
                .score_floor
                .or_else(|| override_bundle.as_ref().map(|bundle| bundle.score_floor))
                .unwrap_or(compiled.decay_floor);
            compiled.compiled_property_rules.insert(
                rule.property_path.clone(),
                CompiledPropertyOverride {
                    no_decay: rule.no_decay,
                    half_life_nanos,
                    threshold_age_nanos: if rule.no_decay {
                        i64::MAX
                    } else {
                        compute_threshold_age_nanos(function, half_life_nanos, visibility_threshold)
                    },
                    decay_floor,
                    function,
                },
            );
            if rule.no_decay {
                compiled.has_no_decay_property = true;
            }
        }

        compiled
    };

    let promotion_target = promotion_target_key(&compiled.decay_binding);
    if let Some(policy) =
        find_promotion_policy_for_binding_target(&promotion_target, promotion_policies)
    {
        compiled.compiled_promotion_rules = compile_promotion_rules(&policy, promotion_profiles)?;
        compiled.promotion_policy = Some(policy);
    }

    Ok(compiled)
}

fn compile_promotion_rules(
    policy: &PromotionPolicyDef,
    profiles: &HashMap<String, PromotionProfileDef>,
) -> Result<Vec<CompiledPromotionRule>, KnowledgePolicyError> {
    let parser = Parser::new();
    let mut compiled = policy
        .when_clauses
        .iter()
        .filter_map(|clause| {
            profiles
                .get(&clause.profile_ref)
                .and_then(|profile| profile.enabled.then_some((clause, profile)))
        })
        .map(|(clause, profile)| {
            let expression = parser
                .parse_expression_text(&clause.predicate)
                .map_err(|error| KnowledgePolicyError::InvalidPromotionPredicate {
                    policy: policy.name.clone(),
                    predicate: clause.predicate.clone(),
                    message: error.to_string(),
                })?;
            Ok(CompiledPromotionRule {
                predicate: clause.predicate.clone(),
                expression,
                profile: profile.clone(),
                order: clause.order,
            })
        })
        .collect::<Result<Vec<_>, KnowledgePolicyError>>()?;
    compiled.sort_by_key(|left| left.order);
    Ok(compiled)
}

fn compute_decay_score(binding: &CompiledBinding, age_millis: i64) -> f64 {
    if binding.function == DecayFunction::None {
        return 1.0;
    }

    let age_nanos = age_millis.saturating_mul(1_000_000);
    let half_life_nanos = binding.half_life_nanos;
    if half_life_nanos == 0 {
        return 1.0;
    }

    let inverted = half_life_nanos < 0;
    let age_nanos = age_nanos.max(0) as f64;
    let half_life = half_life_nanos.abs() as f64;

    let raw_score = match binding.function {
        DecayFunction::Exponential => 2f64.powf(-age_nanos / half_life),
        DecayFunction::Linear => (1.0 - (age_nanos / (half_life * 2.0))).max(0.0),
        DecayFunction::Step => {
            if age_nanos <= half_life {
                1.0
            } else {
                0.0
            }
        }
        DecayFunction::None => 1.0,
    };

    if inverted {
        1.0 - raw_score
    } else {
        raw_score
    }
}

fn binding_label_key(labels: &[String]) -> String {
    let mut sorted = labels.to_vec();
    sorted.sort();
    sorted.join("\0")
}

fn promotion_target_key(binding: &DecayProfileBinding) -> String {
    if binding.is_edge {
        return format!(
            "edge:{}",
            binding.target_edge_type.clone().unwrap_or_default()
        );
    }
    if binding.is_wildcard {
        return "wild:node".to_string();
    }
    format!("node:{}", binding_label_key(&binding.target_labels))
}

fn promotion_policy_target_key(policy: &PromotionPolicyDef) -> String {
    if policy.is_edge {
        return format!(
            "edge:{}",
            policy.target_edge_type.clone().unwrap_or_default()
        );
    }
    if policy.is_wildcard {
        return "wild:node".to_string();
    }
    format!("node:{}", binding_label_key(&policy.target_labels))
}

fn collect_subset_matches(
    table: &BindingTable,
    sorted_labels: &[String],
    subset_size: usize,
) -> Vec<CompiledBinding> {
    let mut indices = (0..subset_size).collect::<Vec<_>>();
    let mut matches = Vec::new();

    loop {
        let subset = indices
            .iter()
            .map(|index| sorted_labels[*index].clone())
            .collect::<Vec<_>>();
        let key = subset.join("\0");
        if let Some(binding) = table.lookup_node_exact(&key) {
            matches.push(binding);
        }

        let mut cursor = subset_size;
        while cursor > 0 && indices[cursor - 1] == cursor - 1 + sorted_labels.len() - subset_size {
            cursor -= 1;
        }
        if cursor == 0 {
            break;
        }

        indices[cursor - 1] += 1;
        for next in cursor..subset_size {
            indices[next] = indices[next - 1] + 1;
        }
    }

    matches
}

fn resolve_conflict(matches: Vec<CompiledBinding>) -> CompiledBinding {
    matches
        .into_iter()
        .min_by(|left, right| left.decay_binding.order.cmp(&right.decay_binding.order))
        .expect("conflict resolution requires at least one match")
}

fn resolve_property_override(
    binding: Option<CompiledBinding>,
    property_path: &str,
) -> Option<CompiledBinding> {
    let mut binding = binding?;
    let override_rule = binding.compiled_property_rules.get(property_path)?.clone();
    binding.no_decay = override_rule.no_decay;
    if !override_rule.no_decay {
        binding.half_life_nanos = override_rule.half_life_nanos;
        binding.threshold_age_nanos = override_rule.threshold_age_nanos;
        binding.decay_floor = override_rule.decay_floor;
        binding.function = override_rule.function;
    }
    binding.compiled_property_rules.clear();
    binding.has_no_decay_property = false;
    Some(binding)
}

fn compute_threshold_age_nanos(
    function: DecayFunction,
    half_life_nanos: i64,
    threshold: f64,
) -> i64 {
    if threshold <= 0.0 || half_life_nanos <= 0 {
        return i64::MAX;
    }

    match function {
        DecayFunction::Exponential => {
            (-(half_life_nanos as f64) * threshold.ln() / LN_2).round() as i64
        }
        DecayFunction::Linear => {
            ((1.0 - threshold) * (half_life_nanos as f64) * 2.0).round() as i64
        }
        DecayFunction::Step => half_life_nanos,
        DecayFunction::None => i64::MAX,
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;

    fn decay_binding(
        name: &str,
        labels: &[&str],
        profile_ref: &str,
        order: i64,
    ) -> DecayProfileBinding {
        DecayProfileBinding {
            name: name.to_string(),
            target_labels: labels.iter().map(|label| (*label).to_string()).collect(),
            target_edge_type: None,
            is_wildcard: false,
            is_edge: false,
            profile_ref: Some(profile_ref.to_string()),
            no_decay: false,
            visibility_threshold: None,
            property_rules: Vec::new(),
            order,
        }
    }

    fn bundle(name: &str, half_life_seconds: i64, function: DecayFunction) -> DecayProfileBundle {
        DecayProfileBundle {
            name: name.to_string(),
            half_life_seconds,
            visibility_threshold: 0.1,
            score_floor: 0.05,
            function,
            scope: ScopeType::Node,
            decay_enabled: true,
            score_from: ScoreFromMode::Created,
            score_from_property: None,
            enabled: true,
        }
    }

    #[test]
    fn storage_schema_conversion_is_typed_and_lossless() {
        let schema = DecayProfileSchema {
            name: "fresh".to_string(),
            half_life_seconds: 3600,
            visibility_threshold: 0.2,
            score_floor: 0.1,
            function: "exponential".to_string(),
            scope: "NODE".to_string(),
            decay_enabled: true,
            score_from: "LAST_ACCESSED".to_string(),
            score_from_property: Some("lastSeenAt".to_string()),
            enabled: true,
        };

        let converted = DecayProfileBundle::try_from(&schema).unwrap();
        assert_eq!(converted.name, "fresh");
        assert_eq!(converted.function, DecayFunction::Exponential);
        assert_eq!(converted.scope, ScopeType::Node);
        assert_eq!(converted.score_from, ScoreFromMode::LastAccessed);
        assert_eq!(converted.score_from_property.as_deref(), Some("lastSeenAt"));
    }

    #[test]
    fn decay_binding_schema_conversion_is_lossless() {
        let schema = DecayProfileBindingSchema {
            name: "memory_binding".to_string(),
            target_labels: vec!["MemoryEpisode".to_string()],
            target_edge_type: None,
            is_wildcard: false,
            is_edge: false,
            profile_ref: Some("slow_decay".to_string()),
            no_decay: false,
            visibility_threshold: Some(0.25),
            order: 10,
        };

        let converted = DecayProfileBinding::from(&schema);
        assert_eq!(converted.name, "memory_binding");
        assert_eq!(converted.target_labels, vec!["MemoryEpisode".to_string()]);
        assert_eq!(converted.profile_ref.as_deref(), Some("slow_decay"));
        assert_eq!(converted.visibility_threshold, Some(0.25));
        assert!(converted.property_rules.is_empty());
    }

    #[test]
    fn binding_table_resolves_most_specific_then_subset_then_wildcard() {
        let bundles = HashMap::from([
            (
                "single".to_string(),
                bundle("single", 1800, DecayFunction::Linear),
            ),
            (
                "pair".to_string(),
                bundle("pair", 7200, DecayFunction::Exponential),
            ),
            ("wild".to_string(), bundle("wild", 600, DecayFunction::Step)),
        ]);
        let bindings = vec![
            decay_binding("single", &["Person"], "single", 20),
            decay_binding("pair", &["Employee", "Person"], "pair", 10),
            DecayProfileBinding {
                name: "wild".to_string(),
                target_labels: Vec::new(),
                target_edge_type: None,
                is_wildcard: true,
                is_edge: false,
                profile_ref: Some("wild".to_string()),
                no_decay: false,
                visibility_threshold: None,
                property_rules: Vec::new(),
                order: 99,
            },
        ];

        let resolver = Resolver::new(
            build_binding_table(&bundles, &bindings, &HashMap::new(), &HashMap::new()).unwrap(),
        );

        let exact = resolver
            .resolve_node(&["Person".to_string(), "Employee".to_string()])
            .unwrap();
        assert_eq!(exact.half_life_nanos, 7_200 * 1_000_000_000);

        let subset = resolver
            .resolve_node(&["Person".to_string(), "Admin".to_string()])
            .unwrap();
        assert_eq!(subset.half_life_nanos, 1_800 * 1_000_000_000);

        let wildcard = resolver.resolve_node(&["Robot".to_string()]).unwrap();
        assert_eq!(wildcard.function, DecayFunction::Step);
    }

    #[test]
    fn binding_table_rejects_same_target_same_order_conflicts() {
        let bundles = HashMap::from([(
            "base".to_string(),
            bundle("base", 3600, DecayFunction::Exponential),
        )]);
        let bindings = vec![
            decay_binding("left", &["Person"], "base", 5),
            decay_binding("right", &["Person"], "base", 5),
        ];

        let error =
            build_binding_table(&bundles, &bindings, &HashMap::new(), &HashMap::new()).unwrap_err();
        assert_eq!(
            error,
            KnowledgePolicyError::BindingConflict {
                left: "left".to_string(),
                right: "right".to_string(),
                target: "Person".to_string(),
                order: 5,
            }
        );
    }

    #[test]
    fn property_resolution_applies_override_without_leaking_nested_rules() {
        let bundles = HashMap::from([
            (
                "base".to_string(),
                bundle("base", 3600, DecayFunction::Exponential),
            ),
            (
                "fast".to_string(),
                bundle("fast", 600, DecayFunction::Linear),
            ),
        ]);
        let bindings = vec![DecayProfileBinding {
            name: "person".to_string(),
            target_labels: vec!["Person".to_string()],
            target_edge_type: None,
            is_wildcard: false,
            is_edge: false,
            profile_ref: Some("base".to_string()),
            no_decay: false,
            visibility_threshold: Some(0.25),
            property_rules: vec![
                DecayProfilePropertyRule {
                    property_path: "bio".to_string(),
                    no_decay: false,
                    profile_ref: Some("fast".to_string()),
                    half_life_seconds: None,
                    score_floor: Some(0.2),
                    order: 0,
                },
                DecayProfilePropertyRule {
                    property_path: "name".to_string(),
                    no_decay: true,
                    profile_ref: None,
                    half_life_seconds: None,
                    score_floor: None,
                    order: 1,
                },
            ],
            order: 0,
        }];

        let resolver = Resolver::new(
            build_binding_table(&bundles, &bindings, &HashMap::new(), &HashMap::new()).unwrap(),
        );
        let bio = resolver
            .resolve_property(&["Person".to_string()], "bio")
            .unwrap();
        assert_eq!(bio.function, DecayFunction::Linear);
        assert_eq!(bio.half_life_nanos, 600 * 1_000_000_000);
        assert_eq!(bio.decay_floor, 0.2);
        assert!(bio.compiled_property_rules.is_empty());

        let name = resolver
            .resolve_property(&["Person".to_string()], "name")
            .unwrap();
        assert!(name.no_decay);
    }

    #[test]
    fn promotion_rules_attach_and_sort_by_order() {
        let bundles = HashMap::from([(
            "base".to_string(),
            bundle("base", 3600, DecayFunction::Exponential),
        )]);
        let bindings = vec![decay_binding("person", &["Person"], "base", 0)];
        let promotion_profiles = HashMap::from([
            (
                "boost".to_string(),
                PromotionProfileDef {
                    name: "boost".to_string(),
                    scope: ScopeType::Node,
                    multiplier: 1.5,
                    score_floor: 0.2,
                    score_cap: 1.0,
                    enabled: true,
                },
            ),
            (
                "cap".to_string(),
                PromotionProfileDef {
                    name: "cap".to_string(),
                    scope: ScopeType::Node,
                    multiplier: 1.1,
                    score_floor: 0.1,
                    score_cap: 0.8,
                    enabled: true,
                },
            ),
        ]);
        let promotion_policies = HashMap::from([(
            "person_policy".to_string(),
            PromotionPolicyDef {
                name: "person_policy".to_string(),
                target_labels: vec!["Person".to_string()],
                target_edge_type: None,
                is_wildcard: false,
                is_edge: false,
                on_access_mutations: vec![PromotionOnAccessMutation {
                    kind: PromotionOnAccessMutationKind::SetLastAccessedNow,
                }],
                when_clauses: vec![
                    PromotionPolicyWhenClause {
                        predicate: "n.score > 10".to_string(),
                        profile_ref: "cap".to_string(),
                        order: 20,
                    },
                    PromotionPolicyWhenClause {
                        predicate: "n.hot = true".to_string(),
                        profile_ref: "boost".to_string(),
                        order: 10,
                    },
                ],
                enabled: true,
            },
        )]);

        let resolver = Resolver::new(
            build_binding_table(
                &bundles,
                &bindings,
                &promotion_profiles,
                &promotion_policies,
            )
            .unwrap(),
        );
        let resolved = resolver.resolve_node(&["Person".to_string()]).unwrap();
        assert_eq!(resolved.promotion_policy.unwrap().name, "person_policy");
        assert_eq!(resolved.compiled_promotion_rules.len(), 2);
        assert_eq!(resolved.compiled_promotion_rules[0].profile.name, "boost");
        assert_eq!(
            resolved.compiled_promotion_rules[0].predicate,
            "n.hot = true"
        );
        assert_eq!(resolved.compiled_promotion_rules[1].profile.name, "cap");
    }

    #[test]
    fn score_binding_applies_promotion_floor_cap_and_decay_floor() {
        let binding = CompiledBinding {
            decay_profile: None,
            decay_binding: decay_binding("person", &["Person"], "base", 0),
            promotion_policy: None,
            visibility_threshold: 0.8,
            score_from: ScoreFromMode::Created,
            score_from_property: None,
            function: DecayFunction::Step,
            half_life_nanos: 1,
            threshold_age_nanos: 1,
            decay_floor: 0.6,
            no_decay: false,
            has_no_decay_property: false,
            compiled_property_rules: HashMap::new(),
            compiled_promotion_rules: Vec::new(),
        };
        let promotion = PromotionProfileDef {
            name: "boost".to_string(),
            scope: ScopeType::Node,
            multiplier: 2.0,
            score_floor: 0.75,
            score_cap: 0.9,
            enabled: true,
        };

        let scored = score_binding(&binding, Some(0), 10, Some(&promotion));
        assert_eq!(scored.base_score, 0.0);
        assert_eq!(scored.final_score, 0.75);
        assert!(scored.suppressed);
    }

    #[test]
    fn access_flusher_merges_pending_metadata_and_skips_flush_on_error() {
        let flusher = AccessFlusher::new();
        let policy = PromotionPolicyDef {
            name: "memory_access".to_string(),
            target_labels: vec!["MemoryEpisode".to_string()],
            target_edge_type: None,
            is_wildcard: false,
            is_edge: false,
            on_access_mutations: vec![
                PromotionOnAccessMutation {
                    kind: PromotionOnAccessMutationKind::IncrementAccessCount,
                },
                PromotionOnAccessMutation {
                    kind: PromotionOnAccessMutationKind::SetLastAccessedNow,
                },
            ],
            when_clauses: Vec::new(),
            enabled: true,
        };

        let flushed = flusher
            .with_buffer(
                || {
                    flusher.record_policy_access("memory:1", Some(&policy), 100);
                    let merged = merge_access_metadata(
                        Some(KnowledgePolicyAccessMetadata {
                            last_accessed_at_unix_ms: Some(50),
                            access_count: 2,
                        }),
                        flusher.pending_mutation("memory:1").as_ref(),
                    )
                    .unwrap();
                    assert_eq!(merged.access_count, 3);
                    assert_eq!(merged.last_accessed_at_unix_ms, Some(100));
                    Ok::<_, ()>(())
                },
                |buffer| {
                    assert_eq!(buffer.get("memory:1").unwrap().access_count_delta, 1);
                    Ok::<_, ()>(())
                },
            )
            .is_ok();
        assert!(flushed);

        let failed = flusher.with_buffer(
            || {
                flusher.record_policy_access("memory:2", Some(&policy), 200);
                Err::<(), _>(())
            },
            |_| panic!("flush should not run on error"),
        );
        assert!(failed.is_err());
    }
}

fn find_promotion_policy_for_binding_target(
    promotion_target: &str,
    promotion_policies: &HashMap<String, PromotionPolicyDef>,
) -> Option<PromotionPolicyDef> {
    let mut policies = promotion_policies.values().cloned().collect::<Vec<_>>();
    policies.sort_by(|left, right| left.name.cmp(&right.name));
    policies
        .into_iter()
        .find(|policy| promotion_policy_target_key(policy) == promotion_target)
}

fn collect_promotion_subset_matches(
    table: &BindingTable,
    sorted_labels: &[String],
    subset_size: usize,
) -> Vec<PromotionPolicyDef> {
    let mut indices = (0..subset_size).collect::<Vec<_>>();
    let mut matches = Vec::new();

    loop {
        let subset = indices
            .iter()
            .map(|index| sorted_labels[*index].clone())
            .collect::<Vec<_>>();
        let key = subset.join("\0");
        if let Some(policy) = table.lookup_node_promotion_exact(&key) {
            matches.push(policy);
        }

        let mut cursor = subset_size;
        while cursor > 0 && indices[cursor - 1] == cursor - 1 + sorted_labels.len() - subset_size {
            cursor -= 1;
        }
        if cursor == 0 {
            break;
        }

        indices[cursor - 1] += 1;
        for next in cursor..subset_size {
            indices[next] = indices[next - 1] + 1;
        }
    }

    matches
}

fn resolve_promotion_conflict(matches: Vec<PromotionPolicyDef>) -> PromotionPolicyDef {
    matches
        .into_iter()
        .min_by(|left, right| left.name.cmp(&right.name))
        .expect("promotion conflict resolution requires at least one match")
}
