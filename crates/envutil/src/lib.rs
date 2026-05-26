//! Environment variable utilities.
//!
//! Equivalent to Go's `pkg/envutil` in NornicDB.
//! Provides typed, validated access to environment variables.

use std::env;
use std::time::Duration;
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

pub fn get(key: &str, fallback: &str) -> String {
    env::var(key)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

pub fn get_int(key: &str, fallback: i64) -> i64 {
    env::var(key)
        .ok()
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(fallback)
}

pub fn get_float(key: &str, fallback: f64) -> f64 {
    env::var(key)
        .ok()
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(fallback)
}

pub fn get_bool_strict(key: &str, fallback: bool) -> bool {
    env::var(key)
        .ok()
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse::<bool>().ok())
        .unwrap_or(fallback)
}

pub fn get_bool_loose(key: &str, fallback: bool) -> bool {
    match lookup_bool_loose(key) {
        Some((value, true)) => value,
        _ => fallback,
    }
}

pub fn lookup_bool_loose(key: &str) -> Option<(bool, bool)> {
    let value = env::var(key).ok()?;
    parse_loose_bool_value(&value).map(|value| (value, true))
}

pub fn parse_loose_bool_value(value: &str) -> Option<bool> {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }
    Some(matches!(normalized.as_str(), "true" | "1" | "yes" | "on"))
}

pub fn get_duration(key: &str, fallback: Duration) -> Duration {
    env::var(key)
        .ok()
        .filter(|value| !value.is_empty())
        .and_then(|value| parse_duration(&value))
        .unwrap_or(fallback)
}

pub fn get_duration_or_seconds(key: &str, fallback: Duration) -> Duration {
    env::var(key)
        .ok()
        .filter(|value| !value.is_empty())
        .and_then(|value| {
            parse_duration(&value).or_else(|| value.parse::<u64>().ok().map(Duration::from_secs))
        })
        .unwrap_or(fallback)
}

pub fn parse_duration(value: &str) -> Option<Duration> {
    let trimmed = value.trim();
    let (number, unit) = trimmed
        .strip_suffix("ms")
        .map(|number| (number, "ms"))
        .or_else(|| trimmed.strip_suffix('s').map(|number| (number, "s")))
        .or_else(|| trimmed.strip_suffix('m').map(|number| (number, "m")))
        .or_else(|| trimmed.strip_suffix('h').map(|number| (number, "h")))?;
    let amount = number.parse::<u64>().ok()?;
    match unit {
        "ms" => Some(Duration::from_millis(amount)),
        "s" => Some(Duration::from_secs(amount)),
        "m" => Some(Duration::from_secs(amount * 60)),
        "h" => Some(Duration::from_secs(amount * 60 * 60)),
        _ => None,
    }
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

    #[test]
    fn loose_bool_parser_matches_nornic_env_contract() {
        assert_eq!(parse_loose_bool_value("true"), Some(true));
        assert_eq!(parse_loose_bool_value("1"), Some(true));
        assert_eq!(parse_loose_bool_value("yes"), Some(true));
        assert_eq!(parse_loose_bool_value("on"), Some(true));
        assert_eq!(parse_loose_bool_value("false"), Some(false));
        assert_eq!(parse_loose_bool_value(""), None);
    }

    #[test]
    fn duration_parser_accepts_go_style_units() {
        assert_eq!(parse_duration("250ms"), Some(Duration::from_millis(250)));
        assert_eq!(parse_duration("5s"), Some(Duration::from_secs(5)));
        assert_eq!(parse_duration("2m"), Some(Duration::from_secs(120)));
        assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_duration("bad"), None);
    }
}
