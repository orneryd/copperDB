//! Embedding utility functions for copperdb.
//!
//! Equivalent to Go's `pkg/embeddingutil` in NornicDB.
//! Helpers for normalizing, comparing, and batching embeddings.

use std::collections::{BTreeMap, BTreeSet};

pub use copperdb_math::{MathError, cosine_similarity, dot, l2_norm, normalize};

const MANAGED_PROPERTIES: [&str; 11] = [
    "embedding",
    "has_embedding",
    "embedding_skipped",
    "embedding_model",
    "embedding_dimensions",
    "embedded_at",
    "has_chunks",
    "chunk_count",
    "createdAt",
    "updatedAt",
    "id",
];

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EmbedTextOptions {
    pub include_labels: bool,
    pub include_properties: BTreeSet<String>,
    pub exclude_properties: BTreeSet<String>,
}

pub fn build_text(
    labels: &[String],
    properties: &BTreeMap<String, serde_json::Value>,
    options: &EmbedTextOptions,
) -> String {
    let mut lines = Vec::new();
    if options.include_labels && !labels.is_empty() {
        lines.push(format!("labels: {}", labels.join(", ")));
    }
    for (key, value) in properties {
        if MANAGED_PROPERTIES.contains(&key.as_str())
            || options.exclude_properties.contains(key)
            || (!options.include_properties.is_empty() && !options.include_properties.contains(key))
        {
            continue;
        }
        lines.push(format!("{key}: {}", display_value(value)));
    }
    if lines.is_empty() {
        "node".into()
    } else {
        lines.join("\n")
    }
}

fn display_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Array(values) => values
            .iter()
            .map(display_value)
            .collect::<Vec<_>>()
            .join(", "),
        _ => value.to_string(),
    }
}

/// Batch-normalize a collection of embedding vectors in place.
pub fn normalize_batch(embeddings: &mut [Vec<f32>]) -> Result<(), MathError> {
    for v in embeddings.iter_mut() {
        normalize(v)?;
    }
    Ok(())
}

/// Find the index of the most similar vector in `candidates` to `query`.
pub fn nearest(query: &[f32], candidates: &[Vec<f32>]) -> Option<usize> {
    candidates
        .iter()
        .enumerate()
        .map(|(i, c)| (i, cosine_similarity(query, c).unwrap_or(f32::NEG_INFINITY)))
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(i, _)| i)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nearest() {
        let query = vec![1.0f32, 0.0, 0.0];
        let candidates = vec![vec![0.0f32, 1.0, 0.0], vec![1.0f32, 0.0, 0.0]];
        assert_eq!(nearest(&query, &candidates), Some(1));
    }

    #[test]
    fn build_text_is_stable_and_excludes_managed_metadata() {
        let properties = BTreeMap::from([
            ("active".into(), serde_json::json!(true)),
            ("embedding".into(), serde_json::json!([0.1, 0.2])),
            ("name".into(), serde_json::json!("Ada")),
            ("tags".into(), serde_json::json!(["math", "code"])),
        ]);
        assert_eq!(
            build_text(
                &["Person".into(), "Researcher".into()],
                &properties,
                &EmbedTextOptions {
                    include_labels: true,
                    ..EmbedTextOptions::default()
                },
            ),
            "labels: Person, Researcher\nactive: true\nname: Ada\ntags: math, code"
        );
    }

    #[test]
    fn build_text_include_then_exclude_and_empty_fallback_match_upstream() {
        let properties = BTreeMap::from([
            ("name".into(), serde_json::json!("Ada")),
            ("secret".into(), serde_json::json!("hidden")),
        ]);
        let options = EmbedTextOptions {
            include_labels: false,
            include_properties: BTreeSet::from(["name".into(), "secret".into()]),
            exclude_properties: BTreeSet::from(["secret".into()]),
        };
        assert_eq!(build_text(&[], &properties, &options), "name: Ada");
        assert_eq!(
            build_text(&[], &BTreeMap::new(), &EmbedTextOptions::default()),
            "node"
        );
    }
}
