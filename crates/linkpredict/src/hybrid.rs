use std::collections::BTreeMap;
use std::sync::Arc;

use copperdb_util::RequestContext;
use serde::{Deserialize, Serialize};

use crate::{
    GraphSnapshot, LinkPredictError, Prediction, adamic_adar, common_neighbors, jaccard,
    preferential_attachment, resource_allocation,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopologyAlgorithm {
    CommonNeighbors,
    Jaccard,
    AdamicAdar,
    PreferentialAttachment,
    ResourceAllocation,
    Ensemble,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HybridConfig {
    pub topology_weight: f64,
    pub semantic_weight: f64,
    pub topology_algorithm: TopologyAlgorithm,
    pub use_ensemble: bool,
    pub normalize_scores: bool,
    pub min_threshold: f64,
}

impl Default for HybridConfig {
    fn default() -> Self {
        Self {
            topology_weight: 0.5,
            semantic_weight: 0.5,
            topology_algorithm: TopologyAlgorithm::AdamicAdar,
            use_ensemble: false,
            normalize_scores: true,
            min_threshold: 0.3,
        }
    }
}

pub trait SemanticScorer: Send + Sync {
    fn score(&self, request_context: &RequestContext, source_id: &str, target_id: &str) -> f64;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HybridPrediction {
    pub target_id: String,
    pub score: f64,
    pub topology_score: f64,
    pub semantic_score: f64,
    pub topology_method: TopologyAlgorithm,
    pub reason: String,
}

pub struct HybridScorer {
    config: HybridConfig,
    semantic_scorer: Option<Arc<dyn SemanticScorer>>,
}

impl HybridScorer {
    pub fn new(mut config: HybridConfig) -> Self {
        if config.topology_weight + config.semantic_weight == 0.0 {
            config.topology_weight = 0.5;
            config.semantic_weight = 0.5;
        }
        Self {
            config,
            semantic_scorer: None,
        }
    }

    pub fn with_semantic_scorer(mut self, scorer: Arc<dyn SemanticScorer>) -> Self {
        self.semantic_scorer = Some(scorer);
        self
    }

    pub fn predict(
        &self,
        request_context: &RequestContext,
        graph: &GraphSnapshot,
        source_id: &str,
        top_k: usize,
    ) -> Result<Vec<HybridPrediction>, LinkPredictError> {
        check_active(request_context)?;
        let mut topology = self.topology_predictions(graph, source_id, top_k.saturating_mul(3));
        if self.config.normalize_scores {
            normalize_scores(&mut topology);
        }
        let mut predictions = Vec::with_capacity(topology.len());
        for prediction in topology {
            check_active(request_context)?;
            let semantic_score = match &self.semantic_scorer {
                Some(scorer) => scorer.score(request_context, source_id, &prediction.target_id),
                None => 0.0,
            };
            let score = self.config.topology_weight * prediction.score
                + self.config.semantic_weight * semantic_score;
            if score < self.config.min_threshold {
                continue;
            }
            predictions.push(HybridPrediction {
                target_id: prediction.target_id,
                score,
                topology_score: prediction.score,
                semantic_score,
                topology_method: self.effective_topology_algorithm(),
                reason: explanation(prediction.score, semantic_score).into(),
            });
        }
        predictions.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.target_id.cmp(&right.target_id))
        });
        if top_k > 0 {
            predictions.truncate(top_k);
        }
        Ok(predictions)
    }

    fn topology_predictions(
        &self,
        graph: &GraphSnapshot,
        source_id: &str,
        top_k: usize,
    ) -> Vec<Prediction> {
        if self.config.use_ensemble {
            return ensemble_topology(graph, source_id, top_k);
        }
        match self.config.topology_algorithm {
            TopologyAlgorithm::CommonNeighbors => common_neighbors(graph, source_id, top_k),
            TopologyAlgorithm::Jaccard => jaccard(graph, source_id, top_k),
            TopologyAlgorithm::AdamicAdar => adamic_adar(graph, source_id, top_k),
            TopologyAlgorithm::PreferentialAttachment => {
                preferential_attachment(graph, source_id, top_k)
            }
            TopologyAlgorithm::ResourceAllocation => resource_allocation(graph, source_id, top_k),
            TopologyAlgorithm::Ensemble => adamic_adar(graph, source_id, top_k),
        }
    }

    fn effective_topology_algorithm(&self) -> TopologyAlgorithm {
        if self.config.use_ensemble {
            TopologyAlgorithm::Ensemble
        } else if self.config.topology_algorithm == TopologyAlgorithm::Ensemble {
            TopologyAlgorithm::AdamicAdar
        } else {
            self.config.topology_algorithm
        }
    }
}

fn ensemble_topology(graph: &GraphSnapshot, source_id: &str, top_k: usize) -> Vec<Prediction> {
    let algorithms = [
        (common_neighbors(graph, source_id, top_k), 0.1),
        (jaccard(graph, source_id, top_k), 0.2),
        (adamic_adar(graph, source_id, top_k), 0.3),
        (resource_allocation(graph, source_id, top_k), 0.25),
        (preferential_attachment(graph, source_id, top_k), 0.15),
    ];
    let mut scores = BTreeMap::<String, f64>::new();
    for (mut predictions, weight) in algorithms {
        normalize_scores(&mut predictions);
        for prediction in predictions {
            *scores.entry(prediction.target_id).or_default() += weight * prediction.score;
        }
    }
    let mut predictions = scores
        .into_iter()
        .map(|(target_id, score)| Prediction {
            target_id,
            score,
            algorithm: "ensemble".into(),
            reason: "Ensemble of 5 topology algorithms".into(),
        })
        .collect::<Vec<_>>();
    predictions.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.target_id.cmp(&right.target_id))
    });
    predictions.truncate(top_k);
    predictions
}

fn check_active(request_context: &RequestContext) -> Result<(), LinkPredictError> {
    request_context
        .check_active()
        .map_err(|_| LinkPredictError::RequestCancelled)
}

fn normalize_scores(predictions: &mut [Prediction]) {
    let Some(minimum) = predictions
        .iter()
        .map(|prediction| prediction.score)
        .reduce(f64::min)
    else {
        return;
    };
    let maximum = predictions
        .iter()
        .map(|prediction| prediction.score)
        .reduce(f64::max)
        .expect("non-empty predictions have a maximum");
    let range = maximum - minimum;
    for prediction in predictions {
        prediction.score = if range == 0.0 {
            1.0
        } else {
            (prediction.score - minimum) / range
        };
    }
}

fn explanation(topology_score: f64, semantic_score: f64) -> &'static str {
    match (topology_score > 0.6, semantic_score > 0.6) {
        (true, true) => "Strong structural connection and semantic similarity",
        (true, false) => "Strong structural connection, moderate semantic match",
        (false, true) => "Weak structural connection, strong semantic similarity",
        (false, false) => "Moderate structural and semantic signals",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Scores;
    impl SemanticScorer for Scores {
        fn score(
            &self,
            _request_context: &RequestContext,
            _source_id: &str,
            target_id: &str,
        ) -> f64 {
            if target_id == "diana" { 0.9 } else { 0.2 }
        }
    }

    fn fixture() -> GraphSnapshot {
        GraphSnapshot::from_edges(
            ["alice", "bob", "charlie", "diana", "eve"],
            &[
                ("alice", "bob"),
                ("alice", "charlie"),
                ("bob", "diana"),
                ("charlie", "diana"),
                ("bob", "eve"),
            ],
            true,
        )
    }

    #[test]
    fn hybrid_scores_only_topology_candidates_and_preserves_membership() {
        let graph = fixture();
        let scorer =
            HybridScorer::new(HybridConfig::default()).with_semantic_scorer(Arc::new(Scores));
        let predictions = scorer
            .predict(&RequestContext::detached(), &graph, "alice", 10)
            .unwrap();
        assert_eq!(predictions[0].target_id, "diana");
        assert_eq!(predictions[0].semantic_score, 0.9);
        assert!(
            predictions
                .iter()
                .all(|prediction| prediction.target_id != "alice"
                    && prediction.target_id != "bob"
                    && prediction.target_id != "charlie")
        );
    }

    #[test]
    fn hybrid_honors_threshold_ties_and_cancellation() {
        let graph = fixture();
        let scorer = HybridScorer::new(HybridConfig {
            topology_weight: 1.0,
            semantic_weight: 0.0,
            topology_algorithm: TopologyAlgorithm::CommonNeighbors,
            use_ensemble: false,
            normalize_scores: true,
            min_threshold: 1.0,
        });
        let predictions = scorer
            .predict(&RequestContext::detached(), &graph, "alice", 10)
            .unwrap();
        assert_eq!(predictions.len(), 1);
        assert_eq!(predictions[0].target_id, "diana");
        let context = RequestContext::detached();
        context.cancel();
        assert_eq!(
            scorer.predict(&context, &graph, "alice", 10).unwrap_err(),
            LinkPredictError::RequestCancelled
        );
    }

    #[test]
    fn hybrid_ensemble_combines_all_five_topology_algorithms() {
        let graph = fixture();
        let scorer = HybridScorer::new(HybridConfig {
            topology_weight: 1.0,
            semantic_weight: 0.0,
            topology_algorithm: TopologyAlgorithm::AdamicAdar,
            use_ensemble: true,
            normalize_scores: true,
            min_threshold: 0.0,
        });

        let predictions = scorer
            .predict(&RequestContext::detached(), &graph, "alice", 10)
            .unwrap();

        assert_eq!(predictions[0].target_id, "diana");
        assert_eq!(predictions[0].topology_method, TopologyAlgorithm::Ensemble);
        assert!(predictions.iter().all(|prediction| {
            prediction.target_id != "alice"
                && prediction.target_id != "bob"
                && prediction.target_id != "charlie"
        }));
    }

    #[test]
    fn hybrid_matches_upstream_ensemble_zero_and_selector_fallback() {
        let graph = fixture();
        let ensemble = HybridScorer::new(HybridConfig {
            topology_weight: 1.0,
            semantic_weight: 0.0,
            topology_algorithm: TopologyAlgorithm::AdamicAdar,
            use_ensemble: true,
            normalize_scores: true,
            min_threshold: 0.0,
        });
        assert!(
            ensemble
                .predict(&RequestContext::detached(), &graph, "alice", 0)
                .unwrap()
                .is_empty()
        );

        let fallback = HybridScorer::new(HybridConfig {
            topology_algorithm: TopologyAlgorithm::Ensemble,
            use_ensemble: false,
            ..HybridConfig::default()
        });
        let predictions = fallback
            .predict(&RequestContext::detached(), &graph, "alice", 10)
            .unwrap();
        let expected = HybridScorer::new(HybridConfig::default())
            .predict(&RequestContext::detached(), &graph, "alice", 10)
            .unwrap();
        assert_eq!(predictions, expected);
        assert!(
            predictions
                .iter()
                .all(|prediction| prediction.topology_method == TopologyAlgorithm::AdamicAdar)
        );
    }
}
