use std::collections::{HashMap, HashSet};

mod cross_encoder;
mod local;
mod mmr;

pub use cross_encoder::{CrossEncoderConfig, CrossEncoderReranker};
pub use local::{GgufRerankScorer, LocalReranker, LocalRerankerConfig, RerankScorer};
pub use mmr::{CandidateEmbeddingSource, MmrReranker};

use copperdb_util::RequestContext;
use serde::{Deserialize, Serialize};

use crate::{RrfHydratedHit, RrfHydratedSearchOutcome, SearchError};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RerankCandidate {
    pub id: String,
    pub content: String,
    pub score: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RerankResult {
    pub id: String,
    pub content: String,
    pub original_rank: usize,
    pub new_rank: usize,
    pub bi_score: f64,
    pub cross_score: f64,
    pub final_score: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RerankApplication {
    pub provider: String,
    pub applied: bool,
    pub results: Vec<RerankResult>,
}

pub trait Reranker: Send + Sync {
    fn name(&self) -> &str;
    fn enabled(&self) -> bool;
    fn is_available(&self, request_context: &RequestContext) -> bool;
    fn rerank(
        &self,
        request_context: &RequestContext,
        query: &str,
        candidates: &[RerankCandidate],
    ) -> Result<Vec<RerankResult>, SearchError>;
}

#[derive(Debug, Default)]
pub struct IdentityReranker;

impl Reranker for IdentityReranker {
    fn name(&self) -> &str {
        "identity"
    }

    fn enabled(&self) -> bool {
        true
    }

    fn is_available(&self, request_context: &RequestContext) -> bool {
        request_context.check_active().is_ok()
    }

    fn rerank(
        &self,
        request_context: &RequestContext,
        _query: &str,
        candidates: &[RerankCandidate],
    ) -> Result<Vec<RerankResult>, SearchError> {
        request_context.check_active()?;
        Ok(pass_through(candidates))
    }
}

pub fn apply_reranker_to_hydrated_outcome(
    request_context: &RequestContext,
    query: &str,
    mut outcome: RrfHydratedSearchOutcome,
    reranker: Option<&dyn Reranker>,
    top_k: usize,
) -> (RrfHydratedSearchOutcome, RerankApplication) {
    let Some(reranker) = reranker.filter(|reranker| reranker.enabled()) else {
        let candidates = rerank_candidates(&outcome.results, outcome.results.len());
        return (
            outcome,
            RerankApplication {
                provider: "identity".into(),
                applied: false,
                results: pass_through(&candidates),
            },
        );
    };

    let candidate_limit = if top_k == 0 { 100 } else { top_k };
    let candidates = rerank_candidates(&outcome.results, candidate_limit);
    if candidates.is_empty() {
        return (
            outcome,
            RerankApplication {
                provider: reranker.name().into(),
                applied: false,
                results: Vec::new(),
            },
        );
    }

    let original_by_id = outcome
        .results
        .iter()
        .enumerate()
        .map(|(index, hit)| (hit.hit.global_id.stable_id(), index))
        .collect::<HashMap<_, _>>();
    let candidate_by_id = candidates
        .iter()
        .enumerate()
        .map(|(index, candidate)| (candidate.id.clone(), (index, candidate)))
        .collect::<HashMap<_, _>>();
    let reranked = match reranker.rerank(request_context, query, &candidates) {
        Ok(results) => results,
        Err(_) => {
            return (
                outcome,
                RerankApplication {
                    provider: reranker.name().into(),
                    applied: false,
                    results: pass_through(&candidates),
                },
            );
        }
    };

    let mut accepted = Vec::with_capacity(candidates.len());
    let mut seen = HashSet::new();
    for mut result in reranked {
        if let Some((index, candidate)) = candidate_by_id.get(&result.id) {
            if !seen.insert(result.id.clone()) {
                continue;
            }
            result.content.clone_from(&candidate.content);
            result.original_rank = index + 1;
            result.bi_score = candidate.score;
            accepted.push(result);
        }
    }
    for result in pass_through(&candidates) {
        if seen.insert(result.id.clone()) {
            accepted.push(result);
        }
    }
    for (new_rank, result) in accepted.iter_mut().enumerate() {
        result.new_rank = new_rank + 1;
    }

    let mut reordered = Vec::with_capacity(outcome.results.len());
    let mut moved = HashSet::new();
    for result in &accepted {
        if let Some(index) = original_by_id.get(&result.id).copied() {
            reordered.push(outcome.results[index].clone());
            moved.insert(index);
        }
    }
    reordered.extend(
        outcome
            .results
            .iter()
            .enumerate()
            .filter(|(index, _)| !moved.contains(index))
            .map(|(_, hit)| hit.clone()),
    );
    outcome.results = reordered;
    outcome.output_hits = outcome.results.len();

    (
        outcome,
        RerankApplication {
            provider: reranker.name().into(),
            applied: true,
            results: accepted,
        },
    )
}

fn rerank_candidates(results: &[RrfHydratedHit], limit: usize) -> Vec<RerankCandidate> {
    results
        .iter()
        .take(limit)
        .filter_map(|result| {
            let content = searchable_content(result)?;
            Some(RerankCandidate {
                id: result.hit.global_id.stable_id(),
                content,
                score: f64::from(result.hit.rrf_score),
            })
        })
        .collect()
}

fn searchable_content(result: &RrfHydratedHit) -> Option<String> {
    let entity = result.entity.as_ref()?.as_object()?;
    const PRIORITY_FIELDS: [&str; 8] = [
        "content",
        "text",
        "title",
        "name",
        "description",
        "path",
        "workerRole",
        "requirements",
    ];
    let mut parts = result.labels.clone();
    for field in PRIORITY_FIELDS {
        if let Some(value) = entity.get(field).and_then(searchable_value) {
            parts.push(value);
        }
    }
    let mut remaining_fields = entity
        .iter()
        .filter(|(field, _)| !PRIORITY_FIELDS.contains(&field.as_str()))
        .collect::<Vec<_>>();
    remaining_fields.sort_by(|left, right| left.0.cmp(right.0));
    for (field, value) in remaining_fields {
        if let Some(value) = searchable_value(value) {
            parts.push(field.clone());
            parts.push(value);
        }
    }
    (!parts.is_empty()).then(|| parts.join(" "))
}

fn searchable_value(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::String(value) => {
            let value = value.trim();
            (!value.is_empty()).then(|| value.to_owned())
        }
        value => Some(value.to_string()),
    }
}

fn pass_through(candidates: &[RerankCandidate]) -> Vec<RerankResult> {
    candidates
        .iter()
        .enumerate()
        .map(|(index, candidate)| RerankResult {
            id: candidate.id.clone(),
            content: candidate.content.clone(),
            original_rank: index + 1,
            new_rank: index + 1,
            bi_score: candidate.score,
            cross_score: candidate.score,
            final_score: candidate.score,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use copperdb_topology::{FabricGlobalId, PlacementKey};
    use serde_json::json;

    use super::*;
    use crate::RrfMergedHit;

    struct RecordingReranker {
        candidates: Mutex<Vec<RerankCandidate>>,
        fail: bool,
    }

    impl Reranker for RecordingReranker {
        fn name(&self) -> &str {
            "recording"
        }

        fn enabled(&self) -> bool {
            true
        }

        fn is_available(&self, _request_context: &RequestContext) -> bool {
            true
        }

        fn rerank(
            &self,
            _request_context: &RequestContext,
            _query: &str,
            candidates: &[RerankCandidate],
        ) -> Result<Vec<RerankResult>, SearchError> {
            *self.candidates.lock().unwrap() = candidates.to_vec();
            if self.fail {
                return Err(SearchError::Transport("provider failed".into()));
            }
            Ok(vec![
                RerankResult {
                    id: candidates[1].id.clone(),
                    content: "provider must not replace content".into(),
                    original_rank: 99,
                    new_rank: 1,
                    bi_score: -1.0,
                    cross_score: 0.95,
                    final_score: 0.95,
                },
                RerankResult {
                    id: "hidden:node".into(),
                    content: "must not appear".into(),
                    original_rank: 1,
                    new_rank: 2,
                    bi_score: 0.0,
                    cross_score: 1.0,
                    final_score: 1.0,
                },
            ])
        }
    }

    fn hydrated_hit(local_id: &str, score: f32, entity: serde_json::Value) -> RrfHydratedHit {
        let shard = PlacementKey::new("default", "copper", "primary");
        RrfHydratedHit {
            hit: RrfMergedHit {
                global_id: FabricGlobalId::new(shard.clone(), "node", local_id),
                rrf_score: score,
                best_score: score,
                vector_rank: 0,
                bm25_rank: 1,
                sources: vec!["lexical".into()],
                shard,
                label: "Document".into(),
                snippet: None,
            },
            labels: vec!["Document".into()],
            entity: Some(entity),
            redacted_fields: vec!["secret".into()],
        }
    }

    fn outcome() -> RrfHydratedSearchOutcome {
        RrfHydratedSearchOutcome {
            results: vec![
                hydrated_hit("a", 0.03, json!({"title": "Alpha", "content": "first"})),
                hydrated_hit("b", 0.02, json!({"title": "Beta", "text": "second"})),
                hydrated_hit("c", 0.01, json!({"name": "no searchable content"})),
            ],
            touched_shards: Vec::new(),
            sources: vec!["lexical".into()],
            input_hits: 3,
            output_hits: 3,
            filtered_hits: 1,
            missing_hydration_hits: 0,
        }
    }

    #[test]
    fn identity_reranker_preserves_upstream_rank_and_score_contract() {
        let candidates = vec![
            RerankCandidate {
                id: "a".into(),
                content: "alpha".into(),
                score: 0.9,
            },
            RerankCandidate {
                id: "b".into(),
                content: "beta".into(),
                score: 0.8,
            },
        ];
        let reranker = IdentityReranker;

        let results = reranker
            .rerank(&RequestContext::detached(), "query", &candidates)
            .unwrap();

        assert_eq!(reranker.name(), "identity");
        assert!(reranker.enabled());
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].original_rank, 1);
        assert_eq!(results[0].new_rank, 1);
        assert_eq!(results[0].bi_score, 0.9);
        assert_eq!(results[0].cross_score, 0.9);
        assert_eq!(results[0].final_score, 0.9);
        assert_eq!(results[1].id, "b");
    }

    #[test]
    fn post_policy_gate_reorders_only_visible_candidates_and_appends_omissions() {
        let reranker = RecordingReranker {
            candidates: Mutex::new(Vec::new()),
            fail: false,
        };

        let (reranked, application) = apply_reranker_to_hydrated_outcome(
            &RequestContext::detached(),
            "query",
            outcome(),
            Some(&reranker),
            100,
        );

        let seen = reranker.candidates.lock().unwrap();
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[0].content, "Document first Alpha");
        assert!(!seen[0].content.contains("redacted"));
        assert_eq!(seen[1].content, "Document second Beta");
        assert_eq!(application.provider, "recording");
        assert!(application.applied);
        assert_eq!(seen[2].content, "Document no searchable content");
        assert_eq!(application.results.len(), 3);
        assert_eq!(application.results[0].id, seen[1].id);
        assert_eq!(application.results[0].original_rank, 2);
        assert_eq!(application.results[0].new_rank, 1);
        assert_eq!(application.results[0].bi_score, f64::from(0.02_f32));
        assert_eq!(application.results[0].content, "Document second Beta");
        assert_eq!(application.results[1].id, seen[0].id);
        assert_eq!(application.results[1].new_rank, 2);
        assert_eq!(application.results[2].id, seen[2].id);
        assert_eq!(application.results[2].new_rank, 3);
        assert_eq!(reranked.results.len(), 3);
        assert_eq!(reranked.results[0].hit.global_id.local_id, "b");
        assert_eq!(reranked.results[1].hit.global_id.local_id, "a");
        assert_eq!(reranked.results[2].hit.global_id.local_id, "c");
        assert_eq!(reranked.filtered_hits, 1);
    }

    #[test]
    fn disabled_reranking_is_identity_and_cancellation_is_explicit() {
        let original = outcome();
        let (unchanged, application) = apply_reranker_to_hydrated_outcome(
            &RequestContext::detached(),
            "query",
            original.clone(),
            None,
            1,
        );
        assert_eq!(unchanged, original);
        assert!(!application.applied);
        assert_eq!(application.results.len(), 3);

        let request_context = RequestContext::detached();
        request_context.cancel();
        let error = IdentityReranker
            .rerank(&request_context, "query", &[])
            .unwrap_err();
        assert!(matches!(error, SearchError::RequestCancelled(_)));
    }

    #[test]
    fn provider_error_fails_open_without_changing_membership_or_order() {
        let original = outcome();
        let reranker = RecordingReranker {
            candidates: Mutex::new(Vec::new()),
            fail: true,
        };

        let (unchanged, application) = apply_reranker_to_hydrated_outcome(
            &RequestContext::detached(),
            "query",
            original.clone(),
            Some(&reranker),
            100,
        );

        assert_eq!(unchanged, original);
        assert!(!application.applied);
        assert_eq!(application.provider, "recording");
        assert_eq!(application.results[0].new_rank, 1);
        assert_eq!(application.results[1].new_rank, 2);
    }
}
