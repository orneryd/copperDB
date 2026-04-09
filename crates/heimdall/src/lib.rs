//! Security monitoring and anomaly detection.
//!
//! Equivalent to Go's `pkg/heimdall` in NornicDB.
//! Named after the Norse god who watches over the Bifrost.
//! Monitors query patterns, detects anomalies, and triggers responses.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum HeimdallError {
    #[error("anomaly detected: {0}")]
    AnomalyDetected(String),
    #[error("rate limit exceeded for {0}")]
    RateLimitExceeded(String),
}

/// Anomaly severity levels.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AnomalyLevel {
    Low,
    Medium,
    High,
    Critical,
}

/// A detected security anomaly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Anomaly {
    pub level: AnomalyLevel,
    pub description: String,
    pub username: String,
    pub source_ip: Option<String>,
}

/// Simple rate limiter using a sliding window counter.
pub struct RateLimiter {
    counter: Arc<AtomicU64>,
    max_per_second: u64,
    window_start: std::sync::Mutex<std::time::Instant>,
}

impl RateLimiter {
    pub fn new(max_per_second: u64) -> Self {
        Self {
            counter: Arc::new(AtomicU64::new(0)),
            max_per_second,
            window_start: std::sync::Mutex::new(std::time::Instant::now()),
        }
    }

    /// Try to allow a request. Returns `Ok(())` if within rate limit.
    pub fn check(&self, username: &str) -> Result<(), HeimdallError> {
        let mut start = self.window_start.lock().unwrap();
        if start.elapsed().as_secs() >= 1 {
            self.counter.store(0, Ordering::Relaxed);
            *start = std::time::Instant::now();
        }
        let count = self.counter.fetch_add(1, Ordering::Relaxed);
        if count >= self.max_per_second {
            Err(HeimdallError::RateLimitExceeded(username.to_owned()))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_allows_within_limit() {
        let limiter = RateLimiter::new(10);
        for _ in 0..10 {
            assert!(limiter.check("alice").is_ok());
        }
    }

    #[test]
    fn test_rate_limiter_blocks_over_limit() {
        let limiter = RateLimiter::new(2);
        assert!(limiter.check("alice").is_ok());
        assert!(limiter.check("alice").is_ok());
        assert!(limiter.check("alice").is_err());
    }
}
