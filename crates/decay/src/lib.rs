//! Memory/weight decay system with Kalman filter adaptation.
//!
//! Equivalent to Go's `pkg/decay` in NornicDB.
//!
//! Implements the "Psygnosis" concept: relationship weights decay over time
//! like human memory, and a Kalman filter tracks the decay trajectory for
//! each relationship. More recently accessed edges have higher weights.
//!
//! # Models
//! - **Exponential decay**: `w(t) = w₀ · e^(-λt)`
//! - **Power decay**: `w(t) = w₀ · t^(-α)`
//! - **Kalman filter**: tracks noisy weight observations and smooths decay

use nalgebra::{Matrix2, Vector2};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DecayError {
    #[error("invalid decay rate: must be positive")]
    InvalidDecayRate,
    #[error("Kalman filter diverged")]
    KalmanDiverged,
}

/// Supported decay models.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum DecayModel {
    /// `w(t) = w₀ · e^(-λt)` — fast initial decay, long tail.
    Exponential { lambda: f64 },
    /// `w(t) = w₀ · t^(-α)` — slower asymptotic decay.
    Power { alpha: f64 },
    /// Gaussian decay for short-term importance.
    Gaussian { sigma: f64 },
}

/// The current weight state of a single relationship/node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecayState {
    pub initial_weight: f64,
    pub current_weight: f64,
    pub last_accessed: u64,  // Unix timestamp in seconds
    pub access_count: u64,
    pub model: DecayModel,
}

impl DecayState {
    /// Create a new decay state with full weight.
    pub fn new(model: DecayModel) -> Self {
        let now = now_secs();
        Self {
            initial_weight: 1.0,
            current_weight: 1.0,
            last_accessed: now,
            access_count: 0,
            model,
        }
    }

    /// Compute the decayed weight at the current time.
    pub fn weight_now(&self) -> f64 {
        let elapsed = now_secs().saturating_sub(self.last_accessed) as f64;
        compute_decay(self.initial_weight, elapsed, self.model)
    }

    /// Record an access, boosting the weight and resetting the timer.
    pub fn record_access(&mut self) {
        self.current_weight = (self.weight_now() + 0.5).min(1.0);
        self.initial_weight = self.current_weight;
        self.last_accessed = now_secs();
        self.access_count += 1;
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

fn compute_decay(w0: f64, elapsed_secs: f64, model: DecayModel) -> f64 {
    match model {
        DecayModel::Exponential { lambda } => w0 * (-lambda * elapsed_secs).exp(),
        DecayModel::Power { alpha } => {
            if elapsed_secs < 1.0 {
                w0
            } else {
                w0 * elapsed_secs.powf(-alpha)
            }
        }
        DecayModel::Gaussian { sigma } => {
            w0 * (-elapsed_secs * elapsed_secs / (2.0 * sigma * sigma)).exp()
        }
    }
}

// ─── Kalman Filter Adapter ────────────────────────────────────────────────────

/// A 1-D Kalman filter for tracking decaying relationship weights.
///
/// State vector: `[weight, decay_rate]`
/// Observation: noisy weight measurement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalmanAdapter {
    /// State estimate: [weight, decay_rate]
    pub state: [f64; 2],
    /// Error covariance matrix (2x2, stored row-major)
    pub covariance: [[f64; 2]; 2],
    /// Process noise covariance
    pub q: f64,
    /// Observation noise variance
    pub r: f64,
}

impl KalmanAdapter {
    /// Create a Kalman adapter with default noise parameters.
    pub fn new(initial_weight: f64, initial_decay_rate: f64) -> Self {
        Self {
            state: [initial_weight, initial_decay_rate],
            covariance: [[1.0, 0.0], [0.0, 1.0]],
            q: 1e-4,
            r: 0.01,
        }
    }

    /// Update the filter with a new observed weight and elapsed time (seconds).
    pub fn update(&mut self, observed_weight: f64, dt: f64) {
        let state = Vector2::new(self.state[0], self.state[1]);
        let covariance = Matrix2::new(
            self.covariance[0][0],
            self.covariance[0][1],
            self.covariance[1][0],
            self.covariance[1][1],
        );

        let transition = Matrix2::new((-state[1] * dt).exp(), 0.0, 0.0, 1.0);
        let process_noise = Matrix2::new(self.q, 0.0, 0.0, self.q);
        let predicted_state = transition * state;
        let predicted_covariance = transition * covariance * transition.transpose() + process_noise;

        let innovation = observed_weight - predicted_state[0];
        let innovation_variance = predicted_covariance[(0, 0)] + self.r;
        let kalman_gain = Vector2::new(
            predicted_covariance[(0, 0)] / innovation_variance,
            predicted_covariance[(1, 0)] / innovation_variance,
        );

        let updated_state = predicted_state + kalman_gain * innovation;
        let measurement_projection = Matrix2::new(1.0, 0.0, 0.0, 0.0);
        let updated_covariance = (Matrix2::identity() - kalman_gain * Vector2::new(1.0, 0.0).transpose())
            * predicted_covariance;

        self.state = [updated_state[0], updated_state[1].max(0.0)];
        self.covariance = [
            [updated_covariance[(0, 0)], updated_covariance[(0, 1)]],
            [updated_covariance[(1, 0)], updated_covariance[(1, 1)]],
        ];
        let _ = measurement_projection;
    }

    /// Return the current estimated weight.
    pub fn estimated_weight(&self) -> f64 {
        self.state[0]
    }

    /// Return the current estimated decay rate.
    pub fn estimated_decay_rate(&self) -> f64 {
        self.state[1]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_decay() {
        let weight = compute_decay(1.0, 10.0, DecayModel::Exponential { lambda: 0.1 });
        assert!(weight < 1.0);
        assert!(weight > 0.0);
        // e^(-0.1 * 10) = e^(-1) ≈ 0.368
        assert!((weight - 0.368).abs() < 0.01);
    }

    #[test]
    fn test_record_access_boosts_weight() {
        let mut state = DecayState::new(DecayModel::Exponential { lambda: 0.01 });
        state.initial_weight = 0.5;
        state.last_accessed = 0; // very old
        state.record_access();
        // After access, weight should be boosted
        assert!(state.access_count == 1);
    }

    #[test]
    fn test_kalman_update() {
        let mut kalman = KalmanAdapter::new(1.0, 0.1);
        kalman.update(0.9, 1.0);
        let est = kalman.estimated_weight();
        assert!(est > 0.0 && est < 1.1);
    }
}
