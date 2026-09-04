use std::time::Duration;

use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};

use super::{RerankCandidate, RerankResult, Reranker, pass_through};
use crate::SearchError;
use copperdb_util::RequestContext;

#[derive(Debug, Clone)]
pub struct CrossEncoderConfig {
    pub enabled: bool,
    pub api_url: String,
    pub api_key: String,
    pub model: String,
    pub top_k: usize,
    pub timeout: Duration,
    pub min_score: f64,
}

impl Default for CrossEncoderConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            api_url: "http://localhost:8081/rerank".into(),
            api_key: String::new(),
            model: "cross-encoder/ms-marco-MiniLM-L-6-v2".into(),
            top_k: 100,
            timeout: Duration::from_secs(30),
            min_score: 0.0,
        }
    }
}

pub struct CrossEncoderReranker {
    config: CrossEncoderConfig,
    client: Client,
}

impl CrossEncoderReranker {
    pub fn new(config: CrossEncoderConfig) -> Result<Self, SearchError> {
        let client = Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|error| SearchError::Transport(error.to_string()))?;
        Ok(Self { config, client })
    }

    fn request_scores(
        &self,
        request_context: &RequestContext,
        query: &str,
        candidates: &[RerankCandidate],
    ) -> Result<Vec<f64>, SearchError> {
        request_context.check_active()?;
        let body = CrossEncoderRequest {
            query,
            documents: candidates
                .iter()
                .map(|candidate| candidate.content.as_str())
                .collect(),
            model: &self.config.model,
            top_n: candidates.len(),
        };
        let mut request = self.client.post(&self.config.api_url).json(&body);
        if !self.config.api_key.is_empty() {
            request = request.bearer_auth(&self.config.api_key);
        }
        let response = request
            .send()
            .and_then(reqwest::blocking::Response::error_for_status)
            .map_err(|error| SearchError::Transport(error.to_string()))?;
        let response: CrossEncoderResponse = response
            .json()
            .map_err(|error| SearchError::Transport(error.to_string()))?;
        response
            .scores(candidates.len())
            .ok_or_else(|| SearchError::Transport("unrecognized rerank response".into()))
    }
}

impl Reranker for CrossEncoderReranker {
    fn name(&self) -> &str {
        "cross_encoder"
    }

    fn enabled(&self) -> bool {
        self.config.enabled
    }

    fn is_available(&self, request_context: &RequestContext) -> bool {
        request_context.check_active().is_ok()
            && self.config.enabled
            && !self.config.api_url.is_empty()
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
        let limit = self.config.top_k.max(1).min(candidates.len());
        let candidates = &candidates[..limit];
        let Ok(scores) = self.request_scores(request_context, query, candidates) else {
            return Ok(pass_through(candidates));
        };
        let mut results = candidates
            .iter()
            .zip(scores)
            .enumerate()
            .map(|(index, (candidate, score))| RerankResult {
                id: candidate.id.clone(),
                content: candidate.content.clone(),
                original_rank: index + 1,
                new_rank: 0,
                bi_score: candidate.score,
                cross_score: score,
                final_score: score,
            })
            .collect::<Vec<_>>();
        results.sort_by(|left, right| {
            right
                .cross_score
                .total_cmp(&left.cross_score)
                .then(left.original_rank.cmp(&right.original_rank))
        });
        for (index, result) in results.iter_mut().enumerate() {
            result.new_rank = index + 1;
        }
        results.retain(|result| result.cross_score >= self.config.min_score);
        Ok(results)
    }
}

#[derive(Serialize)]
struct CrossEncoderRequest<'a> {
    query: &'a str,
    documents: Vec<&'a str>,
    model: &'a str,
    top_n: usize,
}

#[derive(Deserialize)]
struct CrossEncoderResponse {
    #[serde(default)]
    results: Vec<CohereScore>,
    #[serde(default)]
    scores: Vec<f64>,
    #[serde(default)]
    rankings: Vec<SimpleScore>,
}

impl CrossEncoderResponse {
    fn scores(self, count: usize) -> Option<Vec<f64>> {
        let mut scores = vec![0.0; count];
        if !self.results.is_empty() {
            for result in self.results {
                if result.index < count {
                    scores[result.index] = result.relevance_score;
                }
            }
            return Some(scores);
        }
        if !self.scores.is_empty() {
            for (target, score) in scores.iter_mut().zip(self.scores) {
                *target = score;
            }
            return Some(scores);
        }
        if !self.rankings.is_empty() {
            for result in self.rankings {
                if result.index < count {
                    scores[result.index] = result.score;
                }
            }
            return Some(scores);
        }
        None
    }
}

#[derive(Deserialize)]
struct CohereScore {
    index: usize,
    relevance_score: f64,
}

#[derive(Deserialize)]
struct SimpleScore {
    index: usize,
    score: f64,
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;

    use super::*;

    fn candidates() -> Vec<RerankCandidate> {
        vec![
            RerankCandidate {
                id: "first".into(),
                content: "alpha".into(),
                score: 0.6,
            },
            RerankCandidate {
                id: "second".into(),
                content: "beta".into(),
                score: 0.5,
            },
        ]
    }

    #[test]
    fn cross_encoder_sends_upstream_request_and_orders_scores() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, request_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let read = stream.read(&mut buffer).unwrap();
                request.extend_from_slice(&buffer[..read]);
                let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n")
                else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&request[..header_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .map(str::to_owned)
                    })
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap();
                if request.len() >= header_end + 4 + content_length {
                    break;
                }
            }
            request_tx
                .send(String::from_utf8(request).unwrap())
                .unwrap();
            let body = r#"{"results":[{"index":0,"relevance_score":0.1},{"index":1,"relevance_score":0.9}]}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        let reranker = CrossEncoderReranker::new(CrossEncoderConfig {
            enabled: true,
            api_url: format!("http://{address}/rerank"),
            api_key: "secret".into(),
            model: "reranker".into(),
            ..Default::default()
        })
        .unwrap();

        let results = reranker
            .rerank(&RequestContext::detached(), "query", &candidates())
            .unwrap();
        assert_eq!(results[0].id, "second");
        assert_eq!(results[0].cross_score, 0.9);
        assert_eq!(results[1].id, "first");
        let request = request_rx.recv().unwrap();
        assert!(
            request
                .to_ascii_lowercase()
                .contains("authorization: bearer secret\r\n")
        );
        let body = request.split("\r\n\r\n").nth(1).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(body).unwrap(),
            serde_json::json!({
                "query": "query",
                "documents": ["alpha", "beta"],
                "model": "reranker",
                "top_n": 2
            })
        );
        server.join().unwrap();
    }

    #[test]
    fn cross_encoder_fails_open_when_service_is_unavailable() {
        let reranker = CrossEncoderReranker::new(CrossEncoderConfig {
            enabled: true,
            api_url: "http://127.0.0.1:1/rerank".into(),
            timeout: Duration::from_millis(100),
            ..Default::default()
        })
        .unwrap();

        let results = reranker
            .rerank(&RequestContext::detached(), "query", &candidates())
            .unwrap();
        assert_eq!(results[0].id, "first");
        assert_eq!(results[1].id, "second");
    }
}
