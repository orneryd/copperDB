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
const TARGET_STR: &str = match option_env!("copperdb_TARGET") {
    Some(s) => s,
    None => "unknown",
};
const RUST_VERSION_STR: &str = match option_env!("CARGO_PKG_RUST_VERSION") {
    Some(s) => s,
    None => "unknown",
};

const PRODUCT_NAME: &str = "CopperDB";

/// Global build info populated by compile-time environment variables.
///
/// `GIT_COMMIT` and `BUILD_DATE` are optionally injected by CI (e.g.
/// `GIT_COMMIT=$(git rev-parse --short HEAD) cargo build`).
/// `copperdb_TARGET` can be set to the target triple in a build script if needed.
pub const BUILD_INFO: BuildInfo = BuildInfo {
    version: env!("CARGO_PKG_VERSION"),
    git_commit: GIT_COMMIT_STR,
    build_date: BUILD_DATE_STR,
    target: TARGET_STR,
    rust_version: RUST_VERSION_STR,
};

pub fn version() -> &'static str {
    let version = BUILD_INFO.version.trim();
    if version.is_empty() {
        "dev"
    } else {
        BUILD_INFO.version
    }
}

pub fn product_version() -> String {
    format!("v{}", version())
}

pub fn short_commit() -> &'static str {
    short_commit_from(BUILD_INFO.git_commit)
}

pub fn short_commit_from(commit: &'static str) -> &'static str {
    let commit = commit.trim();
    if commit.is_empty() || commit == "unknown" {
        return "dev";
    }
    if commit.len() > 7 {
        &commit[..7]
    } else {
        commit
    }
}

pub fn display_version() -> String {
    let mut version_info = product_version();
    let commit = short_commit();
    if commit != "dev" {
        version_info = format!("{version_info}-{commit}");
    }
    let build_date = BUILD_INFO.build_date.trim();
    if !build_date.is_empty() && build_date != "unknown" {
        version_info = format!("{version_info} (built: {build_date})");
    }
    version_info
}

pub fn server_announcement() -> String {
    format!("{PRODUCT_NAME}/{}", version())
}

/// Return a human-readable build summary string.
pub fn summary() -> String {
    format!(
        "copperdb v{} ({}) built {} for {}",
        BUILD_INFO.version, BUILD_INFO.git_commit, BUILD_INFO.build_date, BUILD_INFO.target,
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

    #[test]
    fn product_version_and_server_announcement_match_contract() {
        assert_eq!(product_version(), format!("v{}", version()));
        assert_eq!(server_announcement(), format!("CopperDB/{}", version()));
    }

    #[test]
    fn short_commit_handles_dev_empty_and_long_hashes() {
        assert_eq!(short_commit_from("dev"), "dev");
        assert_eq!(short_commit_from(""), "dev");
        assert_eq!(short_commit_from("   "), "dev");
        assert_eq!(short_commit_from("abc1234"), "abc1234");
        assert_eq!(short_commit_from("abc1234567890def"), "abc1234");
    }

    #[test]
    fn display_version_contains_product_version() {
        assert!(display_version().starts_with(&product_version()));
    }
}
