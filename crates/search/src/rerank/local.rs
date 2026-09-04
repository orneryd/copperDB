use std::sync::{Arc, mpsc};
use std::time::Duration;

use copperdb_localllm::{GgufConfig, LocalRerankerModel};
use copperdb_util::RequestContext;

use super::{RerankCandidate, RerankResult, Reranker, pass_through};
use crate::SearchError;

pub trait RerankScorer: Send + Sync + 'static {
    fn score(
        &self,
        request_context: &RequestContext,
        query: &str,
        document: &str,
    ) -> Result<f64, SearchError>;
}

pub struct GgufRerankScorer {
    model: LocalRerankerModel,
}

impl GgufRerankScorer {
    pub fn load(model_path: impl Into<std::path::PathBuf>) -> Result<Self, SearchError> {
        LocalRerankerModel::load(GgufConfig::with_model(model_path))
            .map(|model| Self { model })
            .map_err(|error| SearchError::Transport(error.to_string()))
    }

    pub fn backend(&self) -> &'static str {
        self.model.backend().as_str()
    }
}

impl RerankScorer for GgufRerankScorer {
    fn score(
        &self,
        request_context: &RequestContext,
        query: &str,
        document: &str,
    ) -> Result<f64, SearchError> {
        request_context.check_active()?;
        self.model
            .score(query, document)
            .map(f64::from)
            .map_err(|error| SearchError::Transport(error.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalRerankerConfig {
    pub enabled: bool,
    pub timeout: Duration,
    pub max_candidates: usize,
    pub max_document_chars: usize,
}

impl Default for LocalRerankerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            timeout: Duration::from_secs(15),
            max_candidates: 50,
            max_document_chars: 512,
        }
    }
}

pub struct LocalReranker {
    scorer: Arc<dyn RerankScorer>,
    config: LocalRerankerConfig,
}

impl LocalReranker {
    pub fn new(scorer: Arc<dyn RerankScorer>, config: LocalRerankerConfig) -> Self {
        Self { scorer, config }
    }
}

impl Reranker for LocalReranker {
    fn name(&self) -> &str {
        "local_gguf"
    }

    fn enabled(&self) -> bool {
        self.config.enabled
    }

    fn is_available(&self, request_context: &RequestContext) -> bool {
        self.config.enabled && request_context.check_active().is_ok()
    }

    fn rerank(
        &self,
        request_context: &RequestContext,
        query: &str,
        candidates: &[RerankCandidate],
    ) -> Result<Vec<RerankResult>, SearchError> {
        request_context.check_active()?;
        if !self.config.enabled || candidates.is_empty() {
            return Ok(pass_through(candidates));
        }

        let max_candidates = self.config.max_candidates.max(1);
        let max_document_chars = self.config.max_document_chars.max(1);
        let bounded = candidates
            .iter()
            .take(max_candidates)
            .cloned()
            .collect::<Vec<_>>();
        let scorer = Arc::clone(&self.scorer);
        let worker_context = request_context.clone();
        let query = query.trim().to_owned();
        let worker_candidates = bounded.clone();
        let (sender, receiver) = mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let scored = worker_candidates
                .iter()
                .enumerate()
                .map(|(index, candidate)| {
                    worker_context.check_active()?;
                    let document = candidate
                        .content
                        .trim()
                        .chars()
                        .take(max_document_chars)
                        .collect::<String>();
                    scorer
                        .score(&worker_context, &query, &document)
                        .map(|score| (index, score))
                })
                .collect::<Result<Vec<_>, SearchError>>();
            let _ = sender.send(scored);
        });

        let Ok(Ok(mut scores)) = receiver.recv_timeout(self.config.timeout) else {
            return Ok(pass_through(&bounded));
        };
        scores.sort_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
        });

        Ok(scores
            .into_iter()
            .enumerate()
            .map(|(new_index, (original_index, score))| {
                let candidate = &bounded[original_index];
                RerankResult {
                    id: candidate.id.clone(),
                    content: candidate.content.clone(),
                    original_rank: original_index + 1,
                    new_rank: new_index + 1,
                    bi_score: candidate.score,
                    cross_score: score,
                    final_score: score,
                }
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    struct Scores(HashMap<String, f64>);

    impl RerankScorer for Scores {
        fn score(
            &self,
            _request_context: &RequestContext,
            _query: &str,
            document: &str,
        ) -> Result<f64, SearchError> {
            Ok(*self.0.get(document).unwrap())
        }
    }

    struct Fails;

    impl RerankScorer for Fails {
        fn score(
            &self,
            _request_context: &RequestContext,
            _query: &str,
            _document: &str,
        ) -> Result<f64, SearchError> {
            Err(SearchError::Transport("scoring failed".into()))
        }
    }

    fn candidates() -> Vec<RerankCandidate> {
        vec![
            RerankCandidate {
                id: "a".into(),
                content: "alpha-long".into(),
                score: 0.9,
            },
            RerankCandidate {
                id: "b".into(),
                content: "beta-long".into(),
                score: 0.8,
            },
        ]
    }

    #[test]
    fn local_reranker_bounds_content_and_orders_deterministically() {
        let reranker = LocalReranker::new(
            Arc::new(Scores(HashMap::from([
                ("alpha".into(), 0.2),
                ("beta-".into(), 0.9),
            ]))),
            LocalRerankerConfig {
                enabled: true,
                max_document_chars: 5,
                ..LocalRerankerConfig::default()
            },
        );

        let results = reranker
            .rerank(&RequestContext::detached(), " query ", &candidates())
            .unwrap();

        assert_eq!(results[0].id, "b");
        assert_eq!(results[0].original_rank, 2);
        assert_eq!(results[0].bi_score, 0.8);
        assert_eq!(results[0].final_score, 0.9);
        assert_eq!(results[1].id, "a");
    }

    #[test]
    fn local_reranker_fails_open_on_error_and_timeout() {
        let input = candidates();
        let failing = LocalReranker::new(
            Arc::new(Fails),
            LocalRerankerConfig {
                enabled: true,
                ..LocalRerankerConfig::default()
            },
        );
        assert_eq!(
            failing
                .rerank(&RequestContext::detached(), "q", &input)
                .unwrap(),
            pass_through(&input)
        );

        let timed_out = LocalReranker::new(
            Arc::new(Scores(HashMap::from([
                ("alpha-long".into(), 0.2),
                ("beta-long".into(), 0.9),
            ]))),
            LocalRerankerConfig {
                enabled: true,
                timeout: Duration::ZERO,
                ..LocalRerankerConfig::default()
            },
        );
        assert_eq!(
            timed_out
                .rerank(&RequestContext::detached(), "q", &input)
                .unwrap(),
            pass_through(&input)
        );
    }

    #[test]
    fn local_reranker_caps_candidates_and_disabled_is_identity() {
        let input = candidates();
        let scorer = Arc::new(Scores(HashMap::from([("alpha-long".into(), 0.2)])));
        let capped = LocalReranker::new(
            scorer.clone(),
            LocalRerankerConfig {
                enabled: true,
                max_candidates: 1,
                ..LocalRerankerConfig::default()
            },
        );
        assert_eq!(
            capped
                .rerank(&RequestContext::detached(), "query", &input)
                .unwrap()
                .len(),
            1
        );

        let disabled = LocalReranker::new(
            scorer,
            LocalRerankerConfig {
                enabled: false,
                ..LocalRerankerConfig::default()
            },
        );
        assert_eq!(
            disabled
                .rerank(&RequestContext::detached(), "query", &input)
                .unwrap(),
            pass_through(&input)
        );
    }
}
