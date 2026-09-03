use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use copperdb_util::RequestContext;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::InferenceError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuggestionMethod {
    Similarity,
    CoAccess,
    Temporal,
    Transitive,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeSuggestion {
    pub source_id: String,
    pub target_id: String,
    pub relationship_type: String,
    pub confidence: f64,
    pub reason: String,
    pub method: SuggestionMethod,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SimilarityResult {
    pub id: String,
    pub score: f64,
}

pub trait SimilaritySearch: Send + Sync {
    fn search(
        &self,
        request_context: &RequestContext,
        embedding: &[f32],
        limit: usize,
    ) -> Result<Vec<SimilarityResult>, InferenceError>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExistingEdge {
    pub source_id: String,
    pub target_id: String,
    pub confidence: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InferenceBounds {
    pub max_chunks: usize,
    pub max_provider_results: usize,
    pub max_suggestions: usize,
    pub max_access_records: usize,
    pub max_tracked_pairs: usize,
    pub max_transitive_edges: usize,
    pub max_transitive_paths: usize,
}

impl Default for InferenceBounds {
    fn default() -> Self {
        Self {
            max_chunks: 64,
            max_provider_results: 1_000,
            max_suggestions: 100,
            max_access_records: 10_000,
            max_tracked_pairs: 100_000,
            max_transitive_edges: 10_000,
            max_transitive_paths: 100_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SignalConfig {
    pub similarity_enabled: bool,
    pub similarity_threshold: f64,
    pub similarity_top_k: usize,
    pub co_access_enabled: bool,
    pub co_access_window: Duration,
    pub co_access_min_count: u64,
    pub temporal_enabled: bool,
    pub temporal_window: Duration,
    pub transitive_enabled: bool,
    pub transitive_min_confidence: f64,
    pub bounds: InferenceBounds,
}

impl Default for SignalConfig {
    fn default() -> Self {
        Self {
            similarity_enabled: true,
            similarity_threshold: 0.82,
            similarity_top_k: 10,
            co_access_enabled: true,
            co_access_window: Duration::from_secs(30),
            co_access_min_count: 3,
            temporal_enabled: true,
            temporal_window: Duration::from_secs(30 * 60),
            transitive_enabled: true,
            transitive_min_confidence: 0.5,
            bounds: InferenceBounds::default(),
        }
    }
}

#[derive(Debug, Clone)]
struct AccessRecord {
    node_id: String,
    observed_at_ms: u64,
}

#[derive(Default)]
struct AccessState {
    history: VecDeque<AccessRecord>,
    pair_counts: BTreeMap<(String, String), u64>,
}

pub struct SignalEngine {
    config: SignalConfig,
    similarity_search: Option<Arc<dyn SimilaritySearch>>,
    access: Mutex<AccessState>,
}

impl SignalEngine {
    pub fn new(config: SignalConfig) -> Self {
        Self {
            config,
            similarity_search: None,
            access: Mutex::new(AccessState::default()),
        }
    }

    pub fn with_similarity_search(mut self, search: Arc<dyn SimilaritySearch>) -> Self {
        self.similarity_search = Some(search);
        self
    }

    pub fn on_store(
        &self,
        request_context: &RequestContext,
        source_id: &str,
        embeddings: &[Vec<f32>],
    ) -> Result<Vec<EdgeSuggestion>, InferenceError> {
        check_active(request_context)?;
        if !self.config.similarity_enabled || embeddings.is_empty() {
            return Ok(Vec::new());
        }
        if embeddings.len() > self.config.bounds.max_chunks {
            return Err(InferenceError::BoundExceeded("embedding chunks".into()));
        }
        let Some(search) = &self.similarity_search else {
            return Ok(Vec::new());
        };
        let top_k = self.config.similarity_top_k.max(1);
        let mut best = BTreeMap::<String, f64>::new();
        for embedding in embeddings.iter().filter(|embedding| !embedding.is_empty()) {
            check_active(request_context)?;
            let results = match search.search(request_context, embedding, top_k) {
                Ok(results) => results,
                Err(InferenceError::RequestCancelled) => {
                    return Err(InferenceError::RequestCancelled)
                }
                Err(_) => continue,
            };
            if results.len() > self.config.bounds.max_provider_results {
                return Err(InferenceError::BoundExceeded(
                    "similarity provider results".into(),
                ));
            }
            for result in results {
                check_active(request_context)?;
                let target_id = canonical_result_id(&result.id);
                if target_id == source_id || result.score < self.config.similarity_threshold {
                    continue;
                }
                best.entry(target_id)
                    .and_modify(|score| *score = score.max(result.score))
                    .or_insert(result.score);
            }
        }
        let mut ranked = best.into_iter().collect::<Vec<_>>();
        ranked.sort_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        ranked.truncate(top_k.min(self.config.bounds.max_suggestions));
        let suggestions = ranked
            .into_iter()
            .map(|(target_id, score)| EdgeSuggestion {
                source_id: source_id.into(),
                target_id,
                relationship_type: "RELATES_TO".into(),
                confidence: similarity_confidence(score),
                reason: "High embedding similarity".into(),
                method: SuggestionMethod::Similarity,
            })
            .collect::<Vec<_>>();
        Ok(suggestions)
    }

    pub fn on_access_at(
        &self,
        request_context: &RequestContext,
        node_id: &str,
        observed_at_ms: u64,
    ) -> Result<Vec<EdgeSuggestion>, InferenceError> {
        check_active(request_context)?;
        if !self.config.co_access_enabled && !self.config.temporal_enabled {
            return Ok(Vec::new());
        }
        let longest_window_ms = self
            .config
            .co_access_window
            .max(self.config.temporal_window)
            .as_millis() as u64;
        let mut state = self.access.lock();
        state.history.retain(|record| {
            observed_at_ms.saturating_sub(record.observed_at_ms) < longest_window_ms
        });
        if state.history.len() >= self.config.bounds.max_access_records {
            return Err(InferenceError::BoundExceeded("access history".into()));
        }

        let mut suggestions = Vec::new();
        for record in state.history.iter() {
            check_active(request_context)?;
            if record.node_id == node_id {
                continue;
            }
            let age_ms = observed_at_ms.saturating_sub(record.observed_at_ms);
            if self.config.temporal_enabled
                && age_ms < self.config.temporal_window.as_millis() as u64
            {
                suggestions.push(EdgeSuggestion {
                    source_id: node_id.into(),
                    target_id: record.node_id.clone(),
                    relationship_type: "RELATES_TO".into(),
                    confidence: 1.0
                        - age_ms as f64 / self.config.temporal_window.as_millis() as f64,
                    reason: "Accessed within temporal window".into(),
                    method: SuggestionMethod::Temporal,
                });
            }
        }

        if self.config.co_access_enabled {
            let co_access_window_ms = self.config.co_access_window.as_millis() as u64;
            let recent_nodes = state
                .history
                .iter()
                .filter(|record| {
                    record.node_id != node_id
                        && observed_at_ms.saturating_sub(record.observed_at_ms)
                            < co_access_window_ms
                })
                .map(|record| record.node_id.clone())
                .collect::<Vec<_>>();
            for target_id in recent_nodes {
                let pair = canonical_pair(node_id, &target_id);
                if !state.pair_counts.contains_key(&pair)
                    && state.pair_counts.len() >= self.config.bounds.max_tracked_pairs
                {
                    return Err(InferenceError::BoundExceeded("co-access pairs".into()));
                }
                let count = state.pair_counts.entry(pair).or_default();
                *count += 1;
                if *count >= self.config.co_access_min_count {
                    suggestions.push(EdgeSuggestion {
                        source_id: node_id.into(),
                        target_id,
                        relationship_type: "RELATES_TO".into(),
                        confidence: (*count as f64 / 10.0).min(0.8),
                        reason: "Frequently accessed together".into(),
                        method: SuggestionMethod::CoAccess,
                    });
                }
            }
        }
        state.history.push_back(AccessRecord {
            node_id: node_id.into(),
            observed_at_ms,
        });
        drop(state);
        sort_suggestions(&mut suggestions);
        suggestions.dedup_by(|left, right| {
            left.target_id == right.target_id && left.method == right.method
        });
        suggestions.truncate(self.config.bounds.max_suggestions);
        Ok(suggestions)
    }

    pub fn transitive(
        &self,
        request_context: &RequestContext,
        edges: &[ExistingEdge],
    ) -> Result<Vec<EdgeSuggestion>, InferenceError> {
        check_active(request_context)?;
        if !self.config.transitive_enabled {
            return Ok(Vec::new());
        }
        if edges.len() > self.config.bounds.max_transitive_edges {
            return Err(InferenceError::BoundExceeded(
                "transitive input edges".into(),
            ));
        }
        let mut paths = 0usize;
        let mut suggestions = Vec::new();
        for first in edges {
            for second in edges
                .iter()
                .filter(|edge| edge.source_id == first.target_id)
            {
                check_active(request_context)?;
                paths += 1;
                if paths > self.config.bounds.max_transitive_paths {
                    return Err(InferenceError::BoundExceeded("transitive paths".into()));
                }
                let confidence = first.confidence * second.confidence;
                if first.source_id == second.target_id
                    || confidence < self.config.transitive_min_confidence
                {
                    continue;
                }
                suggestions.push(EdgeSuggestion {
                    source_id: first.source_id.clone(),
                    target_id: second.target_id.clone(),
                    relationship_type: "RELATES_TO".into(),
                    confidence,
                    reason: format!("Transitive via {}", first.target_id),
                    method: SuggestionMethod::Transitive,
                });
                if suggestions.len() >= self.config.bounds.max_suggestions {
                    sort_suggestions(&mut suggestions);
                    return Ok(suggestions);
                }
            }
        }
        sort_suggestions(&mut suggestions);
        Ok(suggestions)
    }
}

fn canonical_result_id(id: &str) -> String {
    if let Some((base, suffix)) = id.rsplit_once("-chunk-") {
        if !base.is_empty() && suffix.parse::<usize>().is_ok() {
            return base.into();
        }
    }
    for marker in ["-named-", "-prop-"] {
        if let Some((base, suffix)) = id.rsplit_once(marker) {
            if !base.is_empty() && !suffix.is_empty() {
                return base.into();
            }
        }
    }
    id.into()
}

fn canonical_pair(left: &str, right: &str) -> (String, String) {
    if left <= right {
        (left.into(), right.into())
    } else {
        (right.into(), left.into())
    }
}

fn similarity_confidence(score: f64) -> f64 {
    if score >= 0.95 {
        0.9
    } else if score >= 0.90 {
        0.7
    } else if score >= 0.85 {
        0.5
    } else {
        0.3
    }
}

fn sort_suggestions(suggestions: &mut [EdgeSuggestion]) {
    suggestions.sort_by(|left, right| {
        right
            .confidence
            .total_cmp(&left.confidence)
            .then_with(|| left.source_id.cmp(&right.source_id))
            .then_with(|| left.target_id.cmp(&right.target_id))
            .then_with(|| left.method.cmp(&right.method))
            .then_with(|| left.reason.cmp(&right.reason))
    });
}

fn check_active(request_context: &RequestContext) -> Result<(), InferenceError> {
    request_context
        .check_active()
        .map_err(|_| InferenceError::RequestCancelled)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Search;

    impl SimilaritySearch for Search {
        fn search(
            &self,
            _request_context: &RequestContext,
            embedding: &[f32],
            _limit: usize,
        ) -> Result<Vec<SimilarityResult>, InferenceError> {
            Ok(if embedding[0] == 1.0 {
                vec![
                    SimilarityResult {
                        id: "source-chunk-0".into(),
                        score: 1.0,
                    },
                    SimilarityResult {
                        id: "beta-named-default".into(),
                        score: 0.91,
                    },
                    SimilarityResult {
                        id: "alpha-prop-body".into(),
                        score: 0.91,
                    },
                ]
            } else {
                vec![SimilarityResult {
                    id: "alpha-chunk-2".into(),
                    score: 0.96,
                }]
            })
        }
    }

    #[test]
    fn similarity_normalizes_deduplicates_and_orders() {
        let engine =
            SignalEngine::new(SignalConfig::default()).with_similarity_search(Arc::new(Search));
        let suggestions = engine
            .on_store(
                &RequestContext::detached(),
                "source",
                &[vec![1.0], vec![2.0]],
            )
            .unwrap();
        assert_eq!(suggestions.len(), 2);
        assert_eq!(suggestions[0].target_id, "alpha");
        assert_eq!(suggestions[0].confidence, 0.9);
        assert_eq!(suggestions[1].target_id, "beta");
        assert_eq!(suggestions[1].confidence, 0.7);
        assert_eq!(canonical_result_id("foo-chunk-bar-0"), "foo-chunk-bar-0");
    }

    #[test]
    fn co_access_and_temporal_are_clock_controlled() {
        let engine = SignalEngine::new(SignalConfig::default());
        let context = RequestContext::detached();
        assert!(engine
            .on_access_at(&context, "a", 1_000)
            .unwrap()
            .is_empty());
        let first = engine.on_access_at(&context, "b", 2_000).unwrap();
        assert_eq!(first[0].method, SuggestionMethod::Temporal);
        engine.on_access_at(&context, "a", 3_000).unwrap();
        let third = engine.on_access_at(&context, "b", 4_000).unwrap();
        assert!(third.iter().any(|suggestion| {
            suggestion.method == SuggestionMethod::CoAccess && suggestion.confidence == 0.4
        }));
    }

    #[test]
    fn transitive_formula_bounds_and_cancellation_are_deterministic() {
        let engine = SignalEngine::new(SignalConfig::default());
        let edges = vec![
            ExistingEdge {
                source_id: "a".into(),
                target_id: "b".into(),
                confidence: 0.8,
            },
            ExistingEdge {
                source_id: "b".into(),
                target_id: "c".into(),
                confidence: 0.75,
            },
            ExistingEdge {
                source_id: "b".into(),
                target_id: "a".into(),
                confidence: 1.0,
            },
        ];
        let suggestions = engine
            .transitive(&RequestContext::detached(), &edges)
            .unwrap();
        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].source_id, "a");
        assert_eq!(suggestions[0].target_id, "c");
        assert!((suggestions[0].confidence - 0.6).abs() < 1e-12);

        let context = RequestContext::detached();
        context.cancel();
        assert!(matches!(
            engine.transitive(&context, &edges),
            Err(InferenceError::RequestCancelled)
        ));
    }

    #[test]
    fn all_signal_inputs_are_bounded() {
        let mut config = SignalConfig::default();
        config.bounds.max_chunks = 1;
        config.bounds.max_access_records = 1;
        config.bounds.max_transitive_edges = 1;
        let engine = SignalEngine::new(config).with_similarity_search(Arc::new(Search));
        let context = RequestContext::detached();
        assert!(matches!(
            engine.on_store(&context, "a", &[vec![1.0], vec![2.0]]),
            Err(InferenceError::BoundExceeded(_))
        ));
        engine.on_access_at(&context, "a", 1).unwrap();
        assert!(matches!(
            engine.on_access_at(&context, "b", 2),
            Err(InferenceError::BoundExceeded(_))
        ));
        assert!(matches!(
            engine.transitive(
                &context,
                &[
                    ExistingEdge {
                        source_id: "a".into(),
                        target_id: "b".into(),
                        confidence: 1.0
                    },
                    ExistingEdge {
                        source_id: "b".into(),
                        target_id: "c".into(),
                        confidence: 1.0
                    },
                ],
            ),
            Err(InferenceError::BoundExceeded(_))
        ));
    }
}
