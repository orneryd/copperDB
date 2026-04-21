//! Environment variable utilities.
//!
//! Equivalent to Go's `pkg/envutil` in NornicDB.
//! Provides typed, validated access to environment variables.

use std::env;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EnvError {
    #[error("missing required environment variable: {0}")]
    Missing(String),
    #[error("invalid value for {key}: {source}")]
    ParseError {
        key: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Get a required environment variable as a `String`.
pub fn require(key: &str) -> Result<String, EnvError> {
    env::var(key).map_err(|_| EnvError::Missing(key.to_owned()))
}

/// Get an optional environment variable, returning `None` if unset.
pub fn optional(key: &str) -> Option<String> {
    env::var(key).ok()
}

/// Get an environment variable parsed into type `T`.
pub fn parse<T>(key: &str) -> Result<T, EnvError>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let raw = require(key)?;
    raw.parse::<T>().map_err(|e| EnvError::ParseError {
        key: key.to_owned(),
        source: Box::new(e),
    })
}

/// Get an environment variable parsed into `T`, or return a default.
pub fn parse_or<T>(key: &str, default: T) -> T
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    parse(key).unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optional_missing() {
        assert!(optional("__copperdb_NONEXISTENT_VAR__").is_none());
    }

    #[test]
    fn test_require_missing() {
        assert!(matches!(
            require("__copperdb_NONEXISTENT_VAR__"),
            Err(EnvError::Missing(_))
        ));
    }
}
