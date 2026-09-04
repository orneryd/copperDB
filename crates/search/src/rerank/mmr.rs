use std::sync::Arc;

use copperdb_math::cosine_similarity;
use copperdb_util::RequestContext;

use super::{RerankCandidate, RerankResult, Reranker, pass_through};
use crate::SearchError;

pub trait CandidateEmbeddingSource: Send + Sync {
    fn embedding(
        &self,
        request_context: &RequestContext,
        candidate_id: &str,
    ) -> Result<Option<Vec<f32>>, SearchError>;
}

pub struct MmrReranker {
    enabled: bool,
    lambda: f64,
    embeddings: Arc<dyn CandidateEmbeddingSource>,
}

impl MmrReranker {
    pub fn new(embeddings: Arc<dyn CandidateEmbeddingSource>, lambda: f64) -> Self {
        Self {
            enabled: true,
            lambda: lambda.clamp(0.0, 1.0),
            embeddings,
        }
    }

    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}

impl Reranker for MmrReranker {
    fn name(&self) -> &str {
        "mmr"
    }

    fn enabled(&self) -> bool {
        self.enabled
    }

    fn is_available(&self, request_context: &RequestContext) -> bool {
        self.enabled && request_context.check_active().is_ok()
    }

    fn rerank(
        &self,
        request_context: &RequestContext,
        _query: &str,
        candidates: &[RerankCandidate],
    ) -> Result<Vec<RerankResult>, SearchError> {
        request_context.check_active()?;
        if !self.enabled || candidates.len() <= 1 || self.lambda >= 1.0 {
            return Ok(pass_through(candidates));
        }

        let mut embeddings = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            embeddings.push(self.embeddings.embedding(request_context, &candidate.id)?);
        }

        let mut remaining = (0..candidates.len()).collect::<Vec<_>>();
        let mut selected: Vec<usize> = Vec::with_capacity(candidates.len());
        while !remaining.is_empty() {
            request_context.check_active()?;
            let mut best_position = 0;
            let mut best_score = f64::NEG_INFINITY;
            for (position, &candidate_index) in remaining.iter().enumerate() {
                let relevance = candidates[candidate_index].score;
                let max_similarity = selected
                    .iter()
                    .filter_map(|&selected_index| {
                        let candidate_embedding = embeddings[candidate_index].as_deref()?;
                        let selected_embedding = embeddings[selected_index].as_deref()?;
                        cosine_similarity(candidate_embedding, selected_embedding)
                            .ok()
                            .map(f64::from)
                    })
                    .fold(0.0_f64, f64::max);
                let mmr_score = self.lambda * relevance - (1.0 - self.lambda) * max_similarity;
                if mmr_score > best_score {
                    best_score = mmr_score;
                    best_position = position;
                }
            }
            selected.push(remaining.remove(best_position));
        }

        Ok(selected
            .into_iter()
            .enumerate()
            .map(|(new_index, original_index)| {
                let candidate = &candidates[original_index];
                RerankResult {
                    id: candidate.id.clone(),
                    content: candidate.content.clone(),
                    original_rank: original_index + 1,
                    new_rank: new_index + 1,
                    bi_score: candidate.score,
                    cross_score: candidate.score,
                    final_score: candidate.score,
                }
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    struct Embeddings(HashMap<String, Vec<f32>>);

    impl CandidateEmbeddingSource for Embeddings {
        fn embedding(
            &self,
            _request_context: &RequestContext,
            candidate_id: &str,
        ) -> Result<Option<Vec<f32>>, SearchError> {
            Ok(self.0.get(candidate_id).cloned())
        }
    }

    #[test]
    fn mmr_deterministically_promotes_diversity_without_replacing_scores() {
        let candidates = vec![
            RerankCandidate {
                id: "a".into(),
                content: "a".into(),
                score: 0.9,
            },
            RerankCandidate {
                id: "b".into(),
                content: "b".into(),
                score: 0.8,
            },
            RerankCandidate {
                id: "c".into(),
                content: "c".into(),
                score: 0.7,
            },
        ];
        let embeddings = Arc::new(Embeddings(HashMap::from([
            ("a".into(), vec![1.0, 0.0]),
            ("b".into(), vec![1.0, 0.0]),
            ("c".into(), vec![0.0, 1.0]),
        ])));
        let reranker = MmrReranker::new(embeddings, 0.2);

        let results = reranker
            .rerank(&RequestContext::detached(), "query", &candidates)
            .unwrap();

        assert_eq!(
            results
                .iter()
                .map(|result| result.id.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "c", "b"]
        );
        assert_eq!(results[1].original_rank, 3);
        assert_eq!(results[1].new_rank, 2);
        assert_eq!(results[1].bi_score, 0.7);
        assert_eq!(results[1].final_score, 0.7);
    }

    #[test]
    fn mmr_identity_modes_match_upstream_contract() {
        let candidates = vec![RerankCandidate {
            id: "a".into(),
            content: "a".into(),
            score: 0.9,
        }];
        let embeddings = Arc::new(Embeddings(HashMap::new()));
        let disabled = MmrReranker::new(embeddings.clone(), 0.2).with_enabled(false);
        let relevance_only = MmrReranker::new(embeddings, 1.0);

        assert_eq!(
            disabled
                .rerank(&RequestContext::detached(), "q", &candidates)
                .unwrap(),
            pass_through(&candidates)
        );
        assert_eq!(
            relevance_only
                .rerank(&RequestContext::detached(), "q", &candidates)
                .unwrap(),
            pass_through(&candidates)
        );
    }
}
