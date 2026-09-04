use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::GraphSnapshot;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Prediction {
    pub target_id: String,
    pub score: f64,
    pub algorithm: String,
    pub reason: String,
}

pub fn common_neighbors(graph: &GraphSnapshot, source: &str, top_k: usize) -> Vec<Prediction> {
    score_candidates(graph, source, top_k, "common_neighbors", |_, _, common| {
        common.len() as f64
    })
}

pub fn jaccard(graph: &GraphSnapshot, source: &str, top_k: usize) -> Vec<Prediction> {
    score_candidates(
        graph,
        source,
        top_k,
        "jaccard",
        |source_set, target_set, common| {
            let union = source_set.len() + target_set.len() - common.len();
            if union == 0 {
                0.0
            } else {
                common.len() as f64 / union as f64
            }
        },
    )
}

pub fn adamic_adar(graph: &GraphSnapshot, source: &str, top_k: usize) -> Vec<Prediction> {
    score_candidates(graph, source, top_k, "adamic_adar", |_, _, common| {
        common
            .iter()
            .map(|neighbor| graph.degree(neighbor))
            .filter(|degree| *degree > 1)
            .map(|degree| 1.0 / (degree as f64).ln())
            .sum()
    })
}

pub fn resource_allocation(graph: &GraphSnapshot, source: &str, top_k: usize) -> Vec<Prediction> {
    score_candidates(
        graph,
        source,
        top_k,
        "resource_allocation",
        |_, _, common| {
            common
                .iter()
                .map(|neighbor| graph.degree(neighbor))
                .filter(|degree| *degree > 0)
                .map(|degree| 1.0 / degree as f64)
                .sum()
        },
    )
}

pub fn preferential_attachment(
    graph: &GraphSnapshot,
    source: &str,
    top_k: usize,
) -> Vec<Prediction> {
    let source_neighbors = graph.neighbors(source);
    if !graph.contains_node(source) {
        return Vec::new();
    }
    let scores = graph
        .node_ids()
        .filter(|candidate| *candidate != source && !source_neighbors.contains(*candidate))
        .map(|candidate| {
            (
                candidate.to_owned(),
                (source_neighbors.len() * graph.degree(candidate)) as f64,
            )
        })
        .collect();
    predictions(scores, top_k, "preferential_attachment")
}

fn score_candidates(
    graph: &GraphSnapshot,
    source: &str,
    top_k: usize,
    algorithm: &str,
    score: impl Fn(&BTreeSet<String>, &BTreeSet<String>, &BTreeSet<String>) -> f64,
) -> Vec<Prediction> {
    if !graph.contains_node(source) {
        return Vec::new();
    }
    let source_neighbors = graph.neighbors(source);
    let scores = graph
        .two_hop_candidates(source)
        .into_iter()
        .filter_map(|candidate| {
            let target_neighbors = graph.neighbors(&candidate);
            let common = source_neighbors
                .intersection(target_neighbors)
                .cloned()
                .collect::<BTreeSet<_>>();
            let value = score(source_neighbors, target_neighbors, &common);
            (value > 0.0).then_some((candidate, value))
        })
        .collect();
    predictions(scores, top_k, algorithm)
}

fn predictions(scores: BTreeMap<String, f64>, top_k: usize, algorithm: &str) -> Vec<Prediction> {
    let mut predictions = scores
        .into_iter()
        .map(|(target_id, score)| Prediction {
            target_id,
            score: normalize_algorithm_score(score, algorithm),
            algorithm: algorithm.into(),
            reason: "Topological similarity".into(),
        })
        .collect::<Vec<_>>();
    predictions.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.target_id.cmp(&right.target_id))
    });
    if top_k > 0 {
        predictions.truncate(top_k);
    }
    predictions
}

fn normalize_algorithm_score(score: f64, algorithm: &str) -> f64 {
    match algorithm {
        "jaccard" => score.clamp(0.0, 1.0),
        "common_neighbors" => 1.0 - 1.0 / (1.0 + score / 2.0),
        "adamic_adar" | "resource_allocation" => (score / 5.0).tanh(),
        "preferential_attachment" if score > 1.0 => (score.log10() / 4.0).min(1.0),
        "preferential_attachment" => 0.0,
        _ => score.clamp(0.0, 1.0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> GraphSnapshot {
        GraphSnapshot::from_edges(
            ["alice", "bob", "charlie", "diana", "eve"],
            &[
                ("alice", "bob"),
                ("alice", "charlie"),
                ("bob", "diana"),
                ("charlie", "diana"),
                ("bob", "eve"),
            ],
            true,
        )
    }

    #[test]
    fn all_upstream_algorithms_exclude_self_and_existing_edges() {
        let graph = fixture();
        let algorithms = [
            common_neighbors(&graph, "alice", 10),
            jaccard(&graph, "alice", 10),
            adamic_adar(&graph, "alice", 10),
            resource_allocation(&graph, "alice", 10),
            preferential_attachment(&graph, "alice", 10),
        ];
        for predictions in algorithms {
            assert!(!predictions.is_empty());
            assert!(
                predictions
                    .iter()
                    .all(|prediction| prediction.target_id != "alice"
                        && prediction.target_id != "bob"
                        && prediction.target_id != "charlie"
                        && (0.0..=1.0).contains(&prediction.score))
            );
        }
    }

    #[test]
    fn formulas_and_ties_are_deterministic() {
        let graph = fixture();
        let common = common_neighbors(&graph, "alice", 10);
        assert_eq!(common[0].target_id, "diana");
        assert_eq!(common[0].score, 0.5);
        assert_eq!(common[1].target_id, "eve");
        let expected_resource_score = ((1.0 / 3.0 + 1.0 / 2.0) / 5.0_f64).tanh();
        assert!(
            (resource_allocation(&graph, "alice", 10)[0].score - expected_resource_score).abs()
                < 1e-12
        );
        assert!(common_neighbors(&graph, "missing", 10).is_empty());
        assert_eq!(common_neighbors(&graph, "alice", 1).len(), 1);
    }

    #[test]
    fn complete_graph_has_no_link_candidates() {
        let graph = GraphSnapshot::from_edges(
            ["a", "b", "c", "d"],
            &[
                ("a", "b"),
                ("a", "c"),
                ("a", "d"),
                ("b", "c"),
                ("b", "d"),
                ("c", "d"),
            ],
            true,
        );

        assert!(common_neighbors(&graph, "a", 10).is_empty());
        assert!(jaccard(&graph, "a", 10).is_empty());
        assert!(adamic_adar(&graph, "a", 10).is_empty());
        assert!(resource_allocation(&graph, "a", 10).is_empty());
        assert!(preferential_attachment(&graph, "a", 10).is_empty());
    }
}
