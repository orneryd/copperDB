//! Embedded UI assets matching NornicDB's `ui/embed.go`.
//!
//! The UI is built with `npm run build` (output to `ui/dist/`)
//! and embedded into the binary at compile time via `rust-embed`.
//! No disk files are needed at runtime.
//!
//! When the `static_dir` config is set, it takes precedence over
//! embedded assets (for development / custom UIs).

use rust_embed::RustEmbed;

/// Embedded UI assets from `ui/dist/`.
///
/// This is the Rust equivalent of Go's `//go:embed all:dist` directive.
/// The `ui/dist/` directory must exist at compile time (build with `make build-ui`).
#[derive(RustEmbed)]
#[folder = "../../ui/dist"]
pub struct UiAssets;

/// Returns the embedded asset as bytes, or None if not found.
pub fn get_embedded(path: &str) -> Option<Vec<u8>> {
    let path = path.trim_start_matches('/');
    UiAssets::get(path).map(|f| f.data.to_vec())
}

/// Returns true if embedded UI assets are available (index.html exists).
pub fn embedded_ui_available() -> bool {
    UiAssets::get("index.html").is_some()
}
