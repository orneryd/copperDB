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

    #[test]
    fn test_rate_limiter_error_type() {
        let limiter = RateLimiter::new(1);
        let _ = limiter.check("bob");
        let err = limiter.check("bob").unwrap_err();
        assert!(matches!(err, HeimdallError::RateLimitExceeded(_)));
    }

    #[test]
    fn test_anomaly_levels() {
        let anomaly = Anomaly {
            level: AnomalyLevel::High,
            description: "unusual query pattern".into(),
            username: "alice".into(),
            source_ip: Some("10.0.0.1".into()),
        };
        assert_eq!(anomaly.level, AnomalyLevel::High);
    }

    #[test]
    fn test_anomaly_serialization() {
        let anomaly = Anomaly {
            level: AnomalyLevel::Critical,
            description: "brute force".into(),
            username: "attacker".into(),
            source_ip: None,
        };
        let json = serde_json::to_string(&anomaly).unwrap();
        let decoded: Anomaly = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.level, AnomalyLevel::Critical);
        assert_eq!(decoded.username, "attacker");
    }
}
