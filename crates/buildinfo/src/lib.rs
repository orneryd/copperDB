//! Build metadata and version information.
//!
//! Equivalent to Go's `pkg/buildinfo` in NornicDB.
//! Exposes compile-time version, git commit, and build date.

use serde::{Deserialize, Serialize};

/// Build-time metadata populated at compile time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildInfo {
    /// Semantic version string (e.g. "0.1.0").
    pub version: &'static str,
    /// Git commit SHA at build time.
    pub git_commit: &'static str,
    /// ISO-8601 build timestamp.
    pub build_date: &'static str,
    /// Target triple (e.g. "x86_64-unknown-linux-gnu").
    pub target: &'static str,
    /// Rust compiler version used for this build.
    pub rust_version: &'static str,
}

// Compile-time constants for build metadata.
const GIT_COMMIT_STR: &str = match option_env!("GIT_COMMIT") {
    Some(s) => s,
    None => "unknown",
};
const BUILD_DATE_STR: &str = match option_env!("BUILD_DATE") {
    Some(s) => s,
    None => "unknown",
};
const TARGET_STR: &str = match option_env!("MAGNETDB_TARGET") {
    Some(s) => s,
    None => "unknown",
};
const RUST_VERSION_STR: &str = match option_env!("CARGO_PKG_RUST_VERSION") {
    Some(s) => s,
    None => "unknown",
};

/// Global build info populated by compile-time environment variables.
///
/// `GIT_COMMIT` and `BUILD_DATE` are optionally injected by CI (e.g.
/// `GIT_COMMIT=$(git rev-parse --short HEAD) cargo build`).
/// `MAGNETDB_TARGET` can be set to the target triple in a build script if needed.
pub const BUILD_INFO: BuildInfo = BuildInfo {
    version: env!("CARGO_PKG_VERSION"),
    git_commit: GIT_COMMIT_STR,
    build_date: BUILD_DATE_STR,
    target: TARGET_STR,
    rust_version: RUST_VERSION_STR,
};

/// Return a human-readable build summary string.
pub fn summary() -> String {
    format!(
        "magnetDB v{} ({}) built {} for {}",
        BUILD_INFO.version,
        BUILD_INFO.git_commit,
        BUILD_INFO.build_date,
        BUILD_INFO.target,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_info_version() {
        assert!(!BUILD_INFO.version.is_empty());
    }

    #[test]
    fn test_summary_non_empty() {
        assert!(!summary().is_empty());
    }
}
