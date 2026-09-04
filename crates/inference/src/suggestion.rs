use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use copperdb_audit::{AuditLog, Event, EventType};
use copperdb_auth::{Permission, User};
use copperdb_storage::{EdgeAdjacencyDirection, EdgeRecord, NodeRecord, StorageEngine};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::InferenceError;

const SYSTEM_LABEL: &str = "_System";
const SUGGESTION_LABEL: &str = "_InferenceSuggestion";
const EVIDENCE_LABEL: &str = "_InferenceEvidence";
const DECISION_LABEL: &str = "_InferenceDecision";
const REVIEW_LABEL: &str = "_InferenceReview";
const PENDING_REVIEW_LABEL: &str = "_InferencePendingReview";
const SUGGESTION_PREFIX: &str = "inference:suggestion:v1:";
const EVIDENCE_PREFIX: &str = "inference:evidence:v1:";
const DECISION_PREFIX: &str = "inference:decision:v1:";
const REVIEW_PREFIX: &str = "inference:review:v1:";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuggestionStatus {
    Collecting,
    PendingReview,
    Approved,
    Rejected,
    Materialized,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HeimdallReviewState {
    NotRequested,
    Pending,
    Approved,
    Rejected,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    Approve,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewActor {
    user_id: String,
    username: String,
    admin: bool,
    request_id: Option<String>,
}

impl ReviewActor {
    pub fn from_authenticated_user(user: &User, request_id: Option<String>) -> Self {
        Self {
            user_id: user.id.clone(),
            username: user.username.clone(),
            admin: !user.disabled && user.has_permission(Permission::Admin),
            request_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Provenance {
    pub algorithm: String,
    pub algorithm_version: String,
    pub model_provider: Option<String>,
    pub model_id: Option<String>,
    pub model_version: Option<String>,
    pub embedding_identity: Option<String>,
    pub policy_id: Option<String>,
    pub policy_version: Option<String>,
    pub input_digest: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Evidence {
    pub id: String,
    pub database: String,
    pub source_id: String,
    pub target_id: String,
    pub relationship_type: String,
    pub signal: String,
    pub score: f64,
    pub session_id: String,
    pub request_id: Option<String>,
    pub observed_at_unix_ms: i64,
    pub reason: String,
    pub provenance: Provenance,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Suggestion {
    pub id: String,
    pub database: String,
    pub source_id: String,
    pub target_id: String,
    pub relationship_type: String,
    pub status: SuggestionStatus,
    pub heimdall_review: HeimdallReviewState,
    pub revision: u64,
    pub evidence_count: usize,
    pub score_sum: f64,
    pub average_score: f64,
    pub session_count: usize,
    pub signals: BTreeSet<String>,
    pub first_evidence_at_unix_ms: i64,
    pub last_evidence_at_unix_ms: i64,
    pub cooldown_until_unix_ms: i64,
    pub latest_provenance: Provenance,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HeimdallReview {
    pub id: String,
    pub suggestion_id: String,
    pub state: HeimdallReviewState,
    pub model_provider: String,
    pub model_id: String,
    pub model_version: String,
    pub policy_id: String,
    pub policy_version: String,
    pub input_digest: String,
    pub output_digest: String,
    pub reasoning: String,
    pub relationship_type_override: Option<String>,
    pub reviewed_at_unix_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EvidenceThreshold {
    pub minimum_signals: usize,
    pub minimum_average_score: f64,
    pub minimum_sessions: usize,
    pub window_ms: i64,
}

impl EvidenceThreshold {
    pub fn for_relationship_type(relationship_type: &str) -> Self {
        match relationship_type.to_ascii_lowercase().as_str() {
            "relates_to" => Self::new(3, 0.5, 2, 24),
            "similar_to" => Self::new(2, 0.7, 1, 48),
            "coaccess" => Self::new(5, 0.3, 3, 12),
            "topology" => Self::new(2, 0.6, 1, 72),
            "depends_on" => Self::new(3, 0.6, 2, 168),
            _ => Self::new(3, 0.5, 2, 24),
        }
    }

    const fn new(signals: usize, score: f64, sessions: usize, hours: i64) -> Self {
        Self {
            minimum_signals: signals,
            minimum_average_score: score,
            minimum_sessions: sessions,
            window_ms: hours * 60 * 60 * 1_000,
        }
    }
}

pub struct SuggestionRepository {
    storage: Arc<StorageEngine>,
    audit: Arc<AuditLog>,
    max_evidence_per_suggestion: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaterializationPolicy {
    auto_links_enabled: bool,
    auto_tlp_enabled: bool,
}

impl MaterializationPolicy {
    pub fn from_effective_config(config: &copperdb_config::EffectiveDatabaseConfig) -> Self {
        Self {
            auto_links_enabled: config.auto_links_enabled,
            auto_tlp_enabled: config.auto_tlp_enabled,
        }
    }
}

impl SuggestionRepository {
    pub fn new(storage: Arc<StorageEngine>, audit: Arc<AuditLog>) -> Self {
        Self {
            storage,
            audit,
            max_evidence_per_suggestion: 1_000,
        }
    }

    pub fn storage(&self) -> &Arc<StorageEngine> {
        &self.storage
    }

    pub fn record_evidence(&self, evidence: Evidence) -> Result<Suggestion, InferenceError> {
        for attempt in 0..3 {
            match self.record_evidence_once(evidence.clone()) {
                Err(InferenceError::DecisionConflict) if attempt < 2 => continue,
                result => return result,
            }
        }
        unreachable!("evidence retry loop always returns")
    }

    fn record_evidence_once(&self, mut evidence: Evidence) -> Result<Suggestion, InferenceError> {
        if evidence.source_id == evidence.target_id {
            return Err(InferenceError::InvalidInput("self suggestion".into()));
        }
        if evidence.observed_at_unix_ms == 0 {
            evidence.observed_at_unix_ms = now_unix_ms();
        }
        let suggestion_id = suggestion_id(
            &evidence.database,
            &evidence.source_id,
            &evidence.target_id,
            &evidence.relationship_type,
        );
        if evidence.id.is_empty() {
            evidence.id = digest_json(&evidence)?;
        }
        let evidence_record_id = format!("{EVIDENCE_PREFIX}{}:{}", suggestion_id, evidence.id);
        if self
            .storage
            .get_node_record(&evidence_record_id)
            .map_err(storage_error)?
            .is_some()
        {
            return self
                .get(&suggestion_id)?
                .ok_or(InferenceError::SuggestionNotFound(suggestion_id));
        }
        if let Some(existing) = self.get(&suggestion_id)?
            && matches!(
                existing.status,
                SuggestionStatus::Approved
                    | SuggestionStatus::Rejected
                    | SuggestionStatus::Materialized
                    | SuggestionStatus::Cancelled
            )
        {
            return Ok(existing);
        }
        let mut transaction = self.storage.begin_transaction().map_err(storage_error)?;
        if transaction
            .get_node_record(&evidence_record_id)
            .map_err(storage_error)?
            .is_some()
        {
            return self
                .get(&suggestion_id)?
                .ok_or(InferenceError::SuggestionNotFound(suggestion_id));
        }
        transaction.put_node_record(payload_node(
            evidence_record_id,
            EVIDENCE_LABEL,
            &evidence,
            evidence.observed_at_unix_ms,
        )?);

        let threshold = EvidenceThreshold::for_relationship_type(&evidence.relationship_type);
        let previous = transaction
            .get_node_record(&format!("{SUGGESTION_PREFIX}{suggestion_id}"))
            .map_err(storage_error)?
            .map(|node| from_payload::<Suggestion>(&node))
            .transpose()?;
        let aggregate_start = previous
            .as_ref()
            .filter(|item| {
                evidence.observed_at_unix_ms
                    <= item
                        .first_evidence_at_unix_ms
                        .saturating_add(threshold.window_ms)
            })
            .map_or(evidence.observed_at_unix_ms, |item| {
                item.first_evidence_at_unix_ms
            });
        let mut active = transaction
            .get_nodes_by_label(EVIDENCE_LABEL)
            .map_err(storage_error)?
            .into_iter()
            .filter_map(|node| from_payload::<Evidence>(&node).ok())
            .filter(|item| {
                item.database == evidence.database
                    && item.source_id == evidence.source_id
                    && item.target_id == evidence.target_id
                    && item.relationship_type == evidence.relationship_type
                    && item.observed_at_unix_ms >= aggregate_start
            })
            .collect::<Vec<_>>();
        if active.len() > self.max_evidence_per_suggestion {
            return Err(InferenceError::BoundExceeded("suggestion evidence".into()));
        }
        active.sort_by(|left, right| {
            left.observed_at_unix_ms
                .cmp(&right.observed_at_unix_ms)
                .then_with(|| left.id.cmp(&right.id))
        });
        let suggestion = aggregate_suggestion(suggestion_id, active, threshold, previous);
        transaction.put_node_record(suggestion_node(
            format!("{SUGGESTION_PREFIX}{}", suggestion.id),
            &suggestion,
            suggestion.last_evidence_at_unix_ms,
        )?);
        transaction.commit().map_err(storage_error)?;
        Ok(suggestion)
    }

    pub fn get(&self, id: &str) -> Result<Option<Suggestion>, InferenceError> {
        self.storage
            .get_node_record(&format!("{SUGGESTION_PREFIX}{id}"))
            .map_err(storage_error)?
            .map(|node| from_payload(&node))
            .transpose()
    }

    pub fn list_pending(&self, limit: usize) -> Result<Vec<Suggestion>, InferenceError> {
        let mut suggestions = self
            .storage
            .get_nodes_by_label_bounded(PENDING_REVIEW_LABEL, limit)
            .map_err(storage_error)?
            .into_iter()
            .filter_map(|node| from_payload::<Suggestion>(&node).ok())
            .filter(|suggestion| {
                suggestion.status == SuggestionStatus::PendingReview
                    && suggestion.heimdall_review == HeimdallReviewState::NotRequested
            })
            .collect::<Vec<_>>();
        suggestions.sort_by(|left, right| {
            left.last_evidence_at_unix_ms
                .cmp(&right.last_evidence_at_unix_ms)
                .then_with(|| left.id.cmp(&right.id))
        });
        suggestions.truncate(limit);
        Ok(suggestions)
    }

    pub(crate) fn record_heimdall_review(
        &self,
        review: HeimdallReview,
    ) -> Result<Suggestion, InferenceError> {
        let mut transaction = self.storage.begin_transaction().map_err(storage_error)?;
        let review_id = format!("{REVIEW_PREFIX}{}:{}", review.suggestion_id, review.id);
        if transaction
            .get_node_record(&review_id)
            .map_err(storage_error)?
            .is_some()
        {
            return self
                .get(&review.suggestion_id)?
                .ok_or(InferenceError::SuggestionNotFound(review.suggestion_id));
        }
        let suggestion_id = review.suggestion_id.clone();
        let record_id = format!("{SUGGESTION_PREFIX}{suggestion_id}");
        let mut suggestion: Suggestion = transaction
            .get_node_record(&record_id)
            .map_err(storage_error)?
            .ok_or_else(|| InferenceError::SuggestionNotFound(suggestion_id.clone()))
            .and_then(|node| from_payload(&node))?;
        if !matches!(
            suggestion.status,
            SuggestionStatus::Collecting | SuggestionStatus::PendingReview
        ) {
            return Err(InferenceError::DecisionConflict);
        }
        if let Some(relationship_type) = &review.relationship_type_override {
            let allowed = ["RELATES_TO", "SIMILAR_TO", "DEPENDS_ON", "REFERENCES"];
            if !allowed.contains(&relationship_type.as_str()) {
                return Err(InferenceError::ProviderFailure(
                    "invalid relationship type override".into(),
                ));
            }
        }
        suggestion.heimdall_review = review.state;
        suggestion.revision += 1;
        if let Some(relationship_type) = review
            .relationship_type_override
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            suggestion.relationship_type = relationship_type.to_string();
        }
        transaction.put_node_record(payload_node(
            review_id,
            REVIEW_LABEL,
            &review,
            review.reviewed_at_unix_ms,
        )?);
        transaction.put_node_record(suggestion_node(
            record_id,
            &suggestion,
            review.reviewed_at_unix_ms,
        )?);
        transaction.commit().map_err(storage_error)?;
        Ok(suggestion)
    }

    pub fn decide(
        &self,
        suggestion_id: &str,
        decision: Decision,
        idempotency_key: &str,
        actor: &ReviewActor,
    ) -> Result<Suggestion, InferenceError> {
        for attempt in 0..3 {
            match self.decide_once(suggestion_id, decision, idempotency_key, actor) {
                Err(InferenceError::DecisionConflict) if attempt < 2 => {
                    let receipt_id = decision_receipt_id(suggestion_id, idempotency_key);
                    if let Some(receipt) = self
                        .storage
                        .get_node_record(&receipt_id)
                        .map_err(storage_error)?
                    {
                        let recorded: DecisionReceipt = from_payload(&receipt)?;
                        if recorded.decision == decision {
                            return self.get(suggestion_id)?.ok_or_else(|| {
                                InferenceError::SuggestionNotFound(suggestion_id.into())
                            });
                        }
                    }
                    continue;
                }
                result => return result,
            }
        }
        unreachable!("decision retry loop always returns")
    }

    fn decide_once(
        &self,
        suggestion_id: &str,
        decision: Decision,
        idempotency_key: &str,
        actor: &ReviewActor,
    ) -> Result<Suggestion, InferenceError> {
        if !actor.admin {
            return Err(InferenceError::UnauthorizedDecision);
        }
        if idempotency_key.trim().is_empty() {
            return Err(InferenceError::InvalidInput("empty idempotency key".into()));
        }
        let receipt_id = decision_receipt_id(suggestion_id, idempotency_key);
        let mut transaction = self.storage.begin_transaction().map_err(storage_error)?;
        if let Some(receipt) = transaction
            .get_node_record(&receipt_id)
            .map_err(storage_error)?
        {
            let recorded: DecisionReceipt = from_payload(&receipt)?;
            if recorded.decision != decision {
                return Err(InferenceError::DecisionConflict);
            }
            return self
                .get(suggestion_id)?
                .ok_or_else(|| InferenceError::SuggestionNotFound(suggestion_id.into()));
        }
        let record_id = format!("{SUGGESTION_PREFIX}{suggestion_id}");
        let node = transaction
            .get_node_record(&record_id)
            .map_err(storage_error)?
            .ok_or_else(|| InferenceError::SuggestionNotFound(suggestion_id.into()))?;
        let mut suggestion: Suggestion = from_payload(&node)?;
        let desired = match decision {
            Decision::Approve => SuggestionStatus::Approved,
            Decision::Reject => SuggestionStatus::Rejected,
        };
        if suggestion.status == desired {
            return Ok(suggestion);
        }
        if suggestion.status != SuggestionStatus::PendingReview {
            return Err(InferenceError::DecisionConflict);
        }
        suggestion.status = desired;
        suggestion.revision += 1;
        transaction.put_node_record(suggestion_node(record_id, &suggestion, now_unix_ms())?);
        transaction.put_node_record(payload_node(
            receipt_id,
            DECISION_LABEL,
            &DecisionReceipt {
                suggestion_id: suggestion_id.into(),
                decision,
                actor_id: actor.user_id.clone(),
                idempotency_key: idempotency_key.into(),
                resulting_revision: suggestion.revision,
                decided_at_unix_ms: now_unix_ms(),
            },
            now_unix_ms(),
        )?);
        let mut event = Event {
            event_type: match decision {
                Decision::Approve => EventType::SuggestionApproved,
                Decision::Reject => EventType::SuggestionRejected,
            },
            user_id: Some(actor.user_id.clone()),
            username: Some(actor.username.clone()),
            resource: Some("inference_suggestion".into()),
            resource_id: Some(suggestion_id.into()),
            action: Some(match decision {
                Decision::Approve => "APPROVE".into(),
                Decision::Reject => "REJECT".into(),
            }),
            request_id: actor.request_id.clone(),
            data_classification: Some("DATABASE".into()),
            ..Event::new(EventType::DataUpdate)
        };
        event
            .metadata
            .insert("database".into(), suggestion.database.clone());
        event.metadata.insert(
            "algorithm".into(),
            suggestion.latest_provenance.algorithm.clone(),
        );
        event.metadata.insert(
            "policy_version".into(),
            suggestion
                .latest_provenance
                .policy_version
                .clone()
                .unwrap_or_default(),
        );
        self.audit
            .commit_transaction_with_event(&mut transaction, event)
            .map_err(|error| match error {
                copperdb_audit::AuditError::Storage(
                    copperdb_storage::StorageError::TransactionConflict { .. },
                ) => InferenceError::DecisionConflict,
                other => InferenceError::Audit(other.to_string()),
            })?;
        Ok(suggestion)
    }

    pub fn materialize(
        &self,
        suggestion_id: &str,
        policy: MaterializationPolicy,
        actor: &ReviewActor,
    ) -> Result<Suggestion, InferenceError> {
        for attempt in 0..3 {
            match self.materialize_once(suggestion_id, policy, actor) {
                Err(InferenceError::DecisionConflict) if attempt < 2 => {
                    if let Some(materialized) = self.get(suggestion_id)?
                        && materialized.status == SuggestionStatus::Materialized
                    {
                        return Ok(materialized);
                    }
                    continue;
                }
                result => return result,
            }
        }
        unreachable!("materialization retry loop always returns")
    }

    fn materialize_once(
        &self,
        suggestion_id: &str,
        policy: MaterializationPolicy,
        actor: &ReviewActor,
    ) -> Result<Suggestion, InferenceError> {
        if !policy.auto_links_enabled {
            return Err(InferenceError::MaterializationDisabled);
        }
        if !actor.admin {
            return Err(InferenceError::UnauthorizedDecision);
        }
        let mut transaction = self.storage.begin_transaction().map_err(storage_error)?;
        let record_id = format!("{SUGGESTION_PREFIX}{suggestion_id}");
        let mut suggestion: Suggestion = transaction
            .get_node_record(&record_id)
            .map_err(storage_error)?
            .ok_or_else(|| InferenceError::SuggestionNotFound(suggestion_id.into()))
            .and_then(|node| from_payload(&node))?;
        if suggestion.status == SuggestionStatus::Materialized {
            return Ok(suggestion);
        }
        let topology_signal = suggestion
            .signals
            .iter()
            .any(|signal| signal.eq_ignore_ascii_case("topology"));
        let evidence_expired = now_unix_ms()
            > suggestion.first_evidence_at_unix_ms.saturating_add(
                EvidenceThreshold::for_relationship_type(&suggestion.relationship_type).window_ms,
            );
        if suggestion.status != SuggestionStatus::Approved
            || suggestion.heimdall_review != HeimdallReviewState::Approved
            || (topology_signal && !policy.auto_tlp_enabled)
            || suggestion.source_id == suggestion.target_id
            || evidence_expired
        {
            return Err(InferenceError::PolicyDenied);
        }
        if transaction
            .get_node_record(&suggestion.source_id)
            .map_err(storage_error)?
            .is_none()
            || transaction
                .get_node_record(&suggestion.target_id)
                .map_err(storage_error)?
                .is_none()
        {
            return Err(InferenceError::PolicyDenied);
        }
        let existing = transaction
            .get_adjacent_edges(
                &suggestion.source_id,
                EdgeAdjacencyDirection::Outgoing,
                Some(&suggestion.relationship_type),
            )
            .map_err(storage_error)?
            .into_iter()
            .any(|edge| edge.end_node == suggestion.target_id);
        if existing {
            return Err(InferenceError::PolicyDenied);
        }
        let timestamp = now_unix_ms();
        let edge_id = format!("inference:edge:v1:{suggestion_id}");
        transaction.put_edge_record(EdgeRecord {
            id: edge_id,
            start_node: suggestion.source_id.clone(),
            end_node: suggestion.target_id.clone(),
            edge_type: suggestion.relationship_type.clone(),
            properties: BTreeMap::from([
                ("inference_suggestion_id".into(), json!(suggestion.id)),
                (
                    "inference_algorithm".into(),
                    json!(suggestion.latest_provenance.algorithm),
                ),
                (
                    "inference_model_version".into(),
                    json!(suggestion.latest_provenance.model_version),
                ),
                (
                    "inference_policy_version".into(),
                    json!(suggestion.latest_provenance.policy_version),
                ),
            ]),
            created_at_unix_ms: timestamp,
            updated_at_unix_ms: timestamp,
        });
        suggestion.status = SuggestionStatus::Materialized;
        suggestion.revision += 1;
        suggestion.cooldown_until_unix_ms = timestamp + cooldown_ms(&suggestion.relationship_type);
        transaction.put_node_record(suggestion_node(record_id, &suggestion, timestamp)?);
        let mut event = Event {
            event_type: EventType::SuggestionMaterialized,
            user_id: Some(actor.user_id.clone()),
            username: Some(actor.username.clone()),
            resource: Some("inference_suggestion".into()),
            resource_id: Some(suggestion_id.into()),
            action: Some("MATERIALIZE".into()),
            request_id: actor.request_id.clone(),
            data_classification: Some("DATABASE".into()),
            ..Event::new(EventType::DataCreate)
        };
        event
            .metadata
            .insert("database".into(), suggestion.database.clone());
        self.audit
            .commit_transaction_with_event(&mut transaction, event)
            .map_err(|error| match error {
                copperdb_audit::AuditError::Storage(
                    copperdb_storage::StorageError::TransactionConflict { .. },
                ) => InferenceError::DecisionConflict,
                other => InferenceError::Audit(other.to_string()),
            })?;
        Ok(suggestion)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct DecisionReceipt {
    suggestion_id: String,
    decision: Decision,
    actor_id: String,
    idempotency_key: String,
    resulting_revision: u64,
    decided_at_unix_ms: i64,
}

fn decision_receipt_id(suggestion_id: &str, idempotency_key: &str) -> String {
    format!(
        "{DECISION_PREFIX}{}:{}",
        suggestion_id,
        hex::encode(Sha256::digest(idempotency_key.as_bytes()))
    )
}

fn aggregate_suggestion(
    id: String,
    evidence: Vec<Evidence>,
    threshold: EvidenceThreshold,
    previous: Option<Suggestion>,
) -> Suggestion {
    let count = evidence.len();
    let score_sum = evidence.iter().map(|item| item.score).sum::<f64>();
    let sessions = evidence
        .iter()
        .map(|item| item.session_id.clone())
        .filter(|session| !session.is_empty())
        .collect::<BTreeSet<_>>();
    let signals = evidence
        .iter()
        .map(|item| item.signal.clone())
        .collect::<BTreeSet<_>>();
    let average = if count == 0 {
        0.0
    } else {
        score_sum / count as f64
    };
    let qualifies = count >= threshold.minimum_signals
        && average >= threshold.minimum_average_score
        && sessions.len() >= threshold.minimum_sessions;
    let first = evidence
        .first()
        .expect("new evidence aggregate is non-empty");
    let last = evidence
        .last()
        .expect("new evidence aggregate is non-empty");
    Suggestion {
        id,
        database: first.database.clone(),
        source_id: first.source_id.clone(),
        target_id: first.target_id.clone(),
        relationship_type: first.relationship_type.clone(),
        status: if qualifies {
            SuggestionStatus::PendingReview
        } else {
            SuggestionStatus::Collecting
        },
        heimdall_review: previous
            .as_ref()
            .map_or(HeimdallReviewState::NotRequested, |item| {
                item.heimdall_review
            }),
        revision: previous.map_or(1, |item| item.revision + 1),
        evidence_count: count,
        score_sum,
        average_score: average,
        session_count: sessions.len(),
        signals,
        first_evidence_at_unix_ms: first.observed_at_unix_ms,
        last_evidence_at_unix_ms: last.observed_at_unix_ms,
        cooldown_until_unix_ms: 0,
        latest_provenance: last.provenance.clone(),
    }
}

fn suggestion_id(database: &str, source: &str, target: &str, relationship_type: &str) -> String {
    hex::encode(Sha256::digest(
        format!("{database}\0{source}\0{target}\0{relationship_type}").as_bytes(),
    ))
}

pub(crate) fn cooldown_ms(relationship_type: &str) -> i64 {
    let minutes = match relationship_type.to_ascii_lowercase().as_str() {
        "relates_to" => 5,
        "similar_to" => 10,
        "coaccess" => 1,
        "topology" => 15,
        "depends_on" => 30,
        "references" => 5,
        "semantic_link" => 10,
        _ => 5,
    };
    minutes * 60 * 1_000
}

fn payload_node(
    id: String,
    label: &str,
    payload: &impl Serialize,
    timestamp: i64,
) -> Result<NodeRecord, InferenceError> {
    Ok(NodeRecord {
        id,
        labels: vec![label.into(), SYSTEM_LABEL.into()],
        properties: BTreeMap::from([(
            "payload".into(),
            serde_json::to_value(payload).map_err(serialization_error)?,
        )]),
        named_embeddings: BTreeMap::new(),
        chunk_embeddings: Vec::new(),
        embed_meta: Default::default(),
        created_at_unix_ms: timestamp,
        updated_at_unix_ms: timestamp,
    })
}

fn suggestion_node(
    id: String,
    suggestion: &Suggestion,
    timestamp: i64,
) -> Result<NodeRecord, InferenceError> {
    let mut node = payload_node(id, SUGGESTION_LABEL, suggestion, timestamp)?;
    if suggestion.status == SuggestionStatus::PendingReview
        && suggestion.heimdall_review == HeimdallReviewState::NotRequested
    {
        node.labels.push(PENDING_REVIEW_LABEL.into());
    }
    Ok(node)
}

fn from_payload<T: DeserializeOwned>(node: &NodeRecord) -> Result<T, InferenceError> {
    serde_json::from_value(
        node.properties
            .get("payload")
            .cloned()
            .unwrap_or(json!(null)),
    )
    .map_err(serialization_error)
}

fn digest_json(value: &impl Serialize) -> Result<String, InferenceError> {
    let bytes = serde_json::to_vec(value).map_err(serialization_error)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn storage_error(error: copperdb_storage::StorageError) -> InferenceError {
    if matches!(
        error,
        copperdb_storage::StorageError::TransactionConflict { .. }
    ) {
        InferenceError::DecisionConflict
    } else {
        InferenceError::Storage(error.to_string())
    }
}

fn serialization_error(error: serde_json::Error) -> InferenceError {
    InferenceError::Storage(format!("invalid suggestion record: {error}"))
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use copperdb_audit::AuditConfig;
    use copperdb_auth::{Privilege, ROLE_ADMIN, Role};
    use std::collections::HashMap;
    use std::sync::Barrier;
    use std::thread;

    fn repository() -> (Arc<StorageEngine>, Arc<AuditLog>, SuggestionRepository) {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        let audit = Arc::new(AuditLog::new(Arc::clone(&storage), AuditConfig::default()).unwrap());
        let repository = SuggestionRepository::new(Arc::clone(&storage), Arc::clone(&audit));
        (storage, audit, repository)
    }

    fn evidence(id: &str, session: &str, observed_at: i64) -> Evidence {
        Evidence {
            id: id.into(),
            database: "copperdb".into(),
            source_id: "a".into(),
            target_id: "b".into(),
            relationship_type: "RELATES_TO".into(),
            signal: "similarity".into(),
            score: 0.8,
            session_id: session.into(),
            request_id: Some(format!("request-{id}")),
            observed_at_unix_ms: observed_at,
            reason: "High embedding similarity".into(),
            provenance: Provenance {
                algorithm: "similarity".into(),
                algorithm_version: "1".into(),
                model_id: Some("bge-m3".into()),
                policy_version: Some("policy-v1".into()),
                input_digest: format!("digest-{id}"),
                ..Provenance::default()
            },
            metadata: BTreeMap::new(),
        }
    }

    fn admin() -> ReviewActor {
        ReviewActor::from_authenticated_user(
            &authenticated_admin_user(),
            Some("review-request".into()),
        )
    }

    fn authenticated_admin_user() -> User {
        User {
            id: "admin-1".into(),
            username: "admin".into(),
            email: None,
            roles: vec![Role {
                name: ROLE_ADMIN.into(),
                privileges: Privilege::ADMIN,
                databases: vec!["*".into()],
            }],
            metadata: HashMap::new(),
            disabled: false,
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
            last_login_unix_ms: None,
        }
    }

    fn graph_node(id: &str) -> NodeRecord {
        NodeRecord {
            id: id.into(),
            labels: vec!["Entity".into()],
            properties: BTreeMap::new(),
            named_embeddings: BTreeMap::new(),
            chunk_embeddings: Vec::new(),
            embed_meta: Default::default(),
            created_at_unix_ms: now_unix_ms(),
            updated_at_unix_ms: now_unix_ms(),
        }
    }

    #[test]
    fn evidence_accumulates_idempotently_and_survives_repository_reload() {
        let (storage, audit, repository) = repository();
        let base = now_unix_ms();
        let first = repository
            .record_evidence(evidence("e1", "s1", base))
            .unwrap();
        assert_eq!(first.status, SuggestionStatus::Collecting);
        repository
            .record_evidence(evidence("e2", "s2", base + 1))
            .unwrap();
        let pending = repository
            .record_evidence(evidence("e3", "s2", base + 2))
            .unwrap();
        assert_eq!(pending.status, SuggestionStatus::PendingReview);
        assert_eq!(pending.evidence_count, 3);
        assert!(
            storage
                .get_node_record(&format!("{EVIDENCE_PREFIX}{}:e3", pending.id))
                .unwrap()
                .is_some()
        );
        let duplicate = repository
            .record_evidence(evidence("e3", "s2", base + 2))
            .unwrap();
        assert_eq!(duplicate.revision, pending.revision);
        let reloaded = SuggestionRepository::new(storage, audit);
        assert_eq!(reloaded.get(&pending.id).unwrap(), Some(pending));
    }

    #[test]
    fn expired_evidence_is_excluded_from_thresholds() {
        let (_storage, _audit, repository) = repository();
        let now = now_unix_ms();
        let window = EvidenceThreshold::for_relationship_type("RELATES_TO").window_ms;
        repository
            .record_evidence(evidence("old-1", "s1", now))
            .unwrap();
        repository
            .record_evidence(evidence("old-2", "s2", now + 1))
            .unwrap();
        let suggestion = repository
            .record_evidence(evidence("new", "s3", now + window + 1))
            .unwrap();
        assert_eq!(suggestion.evidence_count, 1);
        assert_eq!(suggestion.status, SuggestionStatus::Collecting);
        assert_eq!(suggestion.first_evidence_at_unix_ms, now + window + 1);
    }

    #[test]
    fn heimdall_failure_is_durable_and_does_not_approve() {
        let (_storage, _audit, repository) = repository();
        let base = now_unix_ms();
        repository
            .record_evidence(evidence("e1", "s1", base))
            .unwrap();
        repository
            .record_evidence(evidence("e2", "s2", base + 1))
            .unwrap();
        let pending = repository
            .record_evidence(evidence("e3", "s2", base + 2))
            .unwrap();
        let reviewed = repository
            .record_heimdall_review(HeimdallReview {
                id: "review-1".into(),
                suggestion_id: pending.id,
                state: HeimdallReviewState::Failed,
                model_provider: "local".into(),
                model_id: "reviewer".into(),
                model_version: "1".into(),
                policy_id: "links".into(),
                policy_version: "1".into(),
                input_digest: "input".into(),
                output_digest: "".into(),
                reasoning: "provider timeout".into(),
                relationship_type_override: None,
                reviewed_at_unix_ms: base + 3,
            })
            .unwrap();
        assert_eq!(reviewed.status, SuggestionStatus::PendingReview);
        assert_eq!(reviewed.heimdall_review, HeimdallReviewState::Failed);
    }

    #[test]
    fn admin_decision_is_atomic_audited_idempotent_and_never_creates_an_edge() {
        let (storage, audit, repository) = repository();
        let base = now_unix_ms();
        repository
            .record_evidence(evidence("e1", "s1", base))
            .unwrap();
        repository
            .record_evidence(evidence("e2", "s2", base + 1))
            .unwrap();
        let pending = repository
            .record_evidence(evidence("e3", "s2", base + 2))
            .unwrap();
        assert!(matches!(
            repository.decide(
                &pending.id,
                Decision::Approve,
                "decision-1",
                &ReviewActor::from_authenticated_user(
                    &User {
                        roles: vec![],
                        ..authenticated_admin_user()
                    },
                    None,
                ),
            ),
            Err(InferenceError::UnauthorizedDecision)
        ));
        let approved = repository
            .decide(&pending.id, Decision::Approve, "decision-1", &admin())
            .unwrap();
        assert_eq!(approved.status, SuggestionStatus::Approved);
        assert_eq!(audit.events().unwrap().len(), 1);
        let duplicate = repository
            .decide(&pending.id, Decision::Approve, "decision-1", &admin())
            .unwrap();
        assert_eq!(duplicate, approved);
        assert_eq!(audit.events().unwrap().len(), 1);
        assert!(matches!(
            repository.decide(&pending.id, Decision::Reject, "decision-2", &admin()),
            Err(InferenceError::DecisionConflict)
        ));
        assert!(storage.all_edges().unwrap().is_empty());
        assert!(audit.verify_chain().unwrap().valid);
    }

    #[test]
    fn concurrent_conflicting_decisions_have_one_winner_and_one_conflict() {
        let (storage, audit, repository) = repository();
        let base = now_unix_ms();
        repository
            .record_evidence(evidence("e1", "s1", base))
            .unwrap();
        repository
            .record_evidence(evidence("e2", "s2", base + 1))
            .unwrap();
        let pending = repository
            .record_evidence(evidence("e3", "s2", base + 2))
            .unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let handles = [Decision::Approve, Decision::Reject].map(|decision| {
            let storage = Arc::clone(&storage);
            let audit = Arc::clone(&audit);
            let barrier = Arc::clone(&barrier);
            let suggestion_id = pending.id.clone();
            thread::spawn(move || {
                let repository = SuggestionRepository::new(storage, audit);
                barrier.wait();
                repository.decide(
                    &suggestion_id,
                    decision,
                    match decision {
                        Decision::Approve => "approve",
                        Decision::Reject => "reject",
                    },
                    &admin(),
                )
            })
        });
        let results = handles.map(|handle| handle.join().unwrap());
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(InferenceError::DecisionConflict)))
                .count(),
            1
        );
        assert_eq!(audit.events().unwrap().len(), 1);
        assert!(audit.verify_chain().unwrap().valid);
    }

    #[test]
    fn concurrent_identical_decisions_converge_idempotently() {
        let (storage, audit, repository) = repository();
        let base = now_unix_ms();
        for (id, session) in [("e1", "s1"), ("e2", "s2"), ("e3", "s2")] {
            repository
                .record_evidence(evidence(id, session, base))
                .unwrap();
        }
        let pending = repository.list_pending(1).unwrap().remove(0);
        let barrier = Arc::new(Barrier::new(2));
        let handles = [(), ()].map(|()| {
            let storage = Arc::clone(&storage);
            let audit = Arc::clone(&audit);
            let barrier = Arc::clone(&barrier);
            let suggestion_id = pending.id.clone();
            thread::spawn(move || {
                let repository = SuggestionRepository::new(storage, audit);
                barrier.wait();
                repository.decide(&suggestion_id, Decision::Approve, "same-decision", &admin())
            })
        });
        let results = handles.map(|handle| handle.join().unwrap().unwrap());
        assert_eq!(results[0], results[1]);
        assert_eq!(results[0].status, SuggestionStatus::Approved);
        assert_eq!(audit.events().unwrap().len(), 1);
    }

    #[test]
    fn materialization_is_default_off_review_gated_atomic_and_idempotent() {
        let (storage, audit, repository) = repository();
        storage.put_node_record(&graph_node("a")).unwrap();
        storage.put_node_record(&graph_node("b")).unwrap();
        let base = now_unix_ms();
        repository
            .record_evidence(evidence("e1", "s1", base))
            .unwrap();
        repository
            .record_evidence(evidence("e2", "s2", base + 1))
            .unwrap();
        let pending = repository
            .record_evidence(evidence("e3", "s2", base + 2))
            .unwrap();
        let reviewed = repository
            .record_heimdall_review(HeimdallReview {
                id: "review-approved".into(),
                suggestion_id: pending.id.clone(),
                state: HeimdallReviewState::Approved,
                model_provider: "local".into(),
                model_id: "reviewer".into(),
                model_version: "1".into(),
                policy_id: "auto-links".into(),
                policy_version: "1".into(),
                input_digest: "input".into(),
                output_digest: "output".into(),
                reasoning: "approved".into(),
                relationship_type_override: None,
                reviewed_at_unix_ms: base + 3,
            })
            .unwrap();
        repository
            .decide(&reviewed.id, Decision::Approve, "human-approval", &admin())
            .unwrap();
        let disabled = MaterializationPolicy {
            auto_links_enabled: false,
            auto_tlp_enabled: false,
        };
        assert!(matches!(
            repository.materialize(&pending.id, disabled, &admin()),
            Err(InferenceError::MaterializationDisabled)
        ));
        assert!(storage.all_edges().unwrap().is_empty());

        let enabled = MaterializationPolicy {
            auto_links_enabled: true,
            auto_tlp_enabled: false,
        };
        let materialized = repository
            .materialize(&pending.id, enabled, &admin())
            .unwrap();
        assert_eq!(materialized.status, SuggestionStatus::Materialized);
        assert!(materialized.cooldown_until_unix_ms > base);
        assert_eq!(storage.all_edges().unwrap().len(), 1);
        assert_eq!(
            repository
                .materialize(&pending.id, enabled, &admin())
                .unwrap(),
            materialized
        );
        assert_eq!(storage.all_edges().unwrap().len(), 1);
        assert_eq!(audit.events().unwrap().len(), 2);
        assert!(audit.verify_chain().unwrap().valid);

        let after_terminal = repository
            .record_evidence(evidence("e4", "s3", base + 4))
            .unwrap();
        assert_eq!(after_terminal, materialized);
        assert_eq!(storage.all_edges().unwrap().len(), 1);
    }

    #[test]
    fn materialization_rejects_expired_approved_evidence() {
        let (storage, _audit, repository) = repository();
        storage.put_node_record(&graph_node("a")).unwrap();
        storage.put_node_record(&graph_node("b")).unwrap();
        let window = EvidenceThreshold::for_relationship_type("RELATES_TO").window_ms;
        let base = now_unix_ms() - window - 10;
        for (id, session, offset) in [("e1", "s1", 0), ("e2", "s2", 1), ("e3", "s2", 2)] {
            repository
                .record_evidence(evidence(id, session, base + offset))
                .unwrap();
        }
        let pending = repository.list_pending(1).unwrap().remove(0);
        let reviewed = repository
            .record_heimdall_review(HeimdallReview {
                id: "stale-review".into(),
                suggestion_id: pending.id.clone(),
                state: HeimdallReviewState::Approved,
                model_provider: "local".into(),
                model_id: "reviewer".into(),
                model_version: "1".into(),
                policy_id: "auto-links".into(),
                policy_version: "1".into(),
                input_digest: "input".into(),
                output_digest: "output".into(),
                reasoning: "approved".into(),
                relationship_type_override: None,
                reviewed_at_unix_ms: now_unix_ms(),
            })
            .unwrap();
        repository
            .decide(&reviewed.id, Decision::Approve, "stale-approval", &admin())
            .unwrap();

        assert!(matches!(
            repository.materialize(
                &pending.id,
                MaterializationPolicy {
                    auto_links_enabled: true,
                    auto_tlp_enabled: false,
                },
                &admin(),
            ),
            Err(InferenceError::PolicyDenied)
        ));
        assert!(storage.all_edges().unwrap().is_empty());
    }

    #[test]
    fn concurrent_identical_materializations_converge() {
        let (storage, audit, repository) = repository();
        storage.put_node_record(&graph_node("a")).unwrap();
        storage.put_node_record(&graph_node("b")).unwrap();
        let base = now_unix_ms();
        for (id, session, offset) in [("e1", "s1", 0), ("e2", "s2", 1), ("e3", "s2", 2)] {
            repository
                .record_evidence(evidence(id, session, base + offset))
                .unwrap();
        }
        let pending = repository.list_pending(1).unwrap().remove(0);
        let reviewed = repository
            .record_heimdall_review(HeimdallReview {
                id: "race-review".into(),
                suggestion_id: pending.id.clone(),
                state: HeimdallReviewState::Approved,
                model_provider: "local".into(),
                model_id: "reviewer".into(),
                model_version: "1".into(),
                policy_id: "auto-links".into(),
                policy_version: "1".into(),
                input_digest: "input".into(),
                output_digest: "output".into(),
                reasoning: "approved".into(),
                relationship_type_override: None,
                reviewed_at_unix_ms: base + 3,
            })
            .unwrap();
        repository
            .decide(&reviewed.id, Decision::Approve, "race-approval", &admin())
            .unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let handles = [(), ()].map(|()| {
            let storage = Arc::clone(&storage);
            let audit = Arc::clone(&audit);
            let barrier = Arc::clone(&barrier);
            let suggestion_id = pending.id.clone();
            thread::spawn(move || {
                let repository = SuggestionRepository::new(storage, audit);
                barrier.wait();
                repository.materialize(
                    &suggestion_id,
                    MaterializationPolicy {
                        auto_links_enabled: true,
                        auto_tlp_enabled: false,
                    },
                    &admin(),
                )
            })
        });
        let results = handles.map(|handle| handle.join().unwrap().unwrap());

        assert_eq!(results[0], results[1]);
        assert_eq!(results[0].status, SuggestionStatus::Materialized);
        assert_eq!(storage.all_edges().unwrap().len(), 1);
        assert_eq!(audit.events().unwrap().len(), 2);
        assert!(audit.verify_chain().unwrap().valid);
    }

    #[test]
    fn heimdall_relationship_override_is_allowlisted() {
        let (_storage, _audit, repository) = repository();
        let base = now_unix_ms();
        repository
            .record_evidence(evidence("e1", "s1", base))
            .unwrap();
        repository
            .record_evidence(evidence("e2", "s2", base + 1))
            .unwrap();
        let pending = repository
            .record_evidence(evidence("e3", "s2", base + 2))
            .unwrap();
        assert!(matches!(
            repository.record_heimdall_review(HeimdallReview {
                id: "malicious".into(),
                suggestion_id: pending.id,
                state: HeimdallReviewState::Approved,
                model_provider: "local".into(),
                model_id: "reviewer".into(),
                model_version: "1".into(),
                policy_id: "links".into(),
                policy_version: "1".into(),
                input_digest: "input".into(),
                output_digest: "output".into(),
                reasoning: "override".into(),
                relationship_type_override: Some("DROP_DATABASE".into()),
                reviewed_at_unix_ms: base + 3,
            }),
            Err(InferenceError::ProviderFailure(_))
        ));
    }
}
