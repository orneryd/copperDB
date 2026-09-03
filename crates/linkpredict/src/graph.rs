use std::collections::{BTreeMap, BTreeSet};

use copperdb_util::RequestContext;

use crate::LinkPredictError;

pub trait AdjacencyStream {
    fn stream_nodes(
        &self,
        request_context: &RequestContext,
        visit: &mut dyn FnMut(&str) -> Result<(), LinkPredictError>,
    ) -> Result<(), LinkPredictError>;

    fn stream_outgoing(
        &self,
        request_context: &RequestContext,
        node_id: &str,
        visit: &mut dyn FnMut(&str) -> Result<(), LinkPredictError>,
    ) -> Result<(), LinkPredictError>;
}

impl AdjacencyStream for copperdb_storage::StorageEngine {
    fn stream_nodes(
        &self,
        request_context: &RequestContext,
        visit: &mut dyn FnMut(&str) -> Result<(), LinkPredictError>,
    ) -> Result<(), LinkPredictError> {
        let mut callback_error = None;
        self.stream_node_records_with_cancellation(request_context.cancellation(), |node| {
            if let Err(error) = visit(&node.id) {
                callback_error = Some(error);
                return Err(copperdb_storage::StorageError::IterationStopped);
            }
            Ok(())
        })
        .map_err(storage_error)?;
        callback_error.map_or(Ok(()), Err)
    }

    fn stream_outgoing(
        &self,
        request_context: &RequestContext,
        node_id: &str,
        visit: &mut dyn FnMut(&str) -> Result<(), LinkPredictError>,
    ) -> Result<(), LinkPredictError> {
        check_active(request_context)?;
        for edge in self.get_edges_from_node(node_id).map_err(storage_error)? {
            check_active(request_context)?;
            visit(&edge.end_node)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphBuildConfig {
    pub undirected: bool,
    pub max_nodes: usize,
    pub max_edges: usize,
}

impl Default for GraphBuildConfig {
    fn default() -> Self {
        Self {
            undirected: true,
            max_nodes: 1_000_000,
            max_edges: 10_000_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphBuildStats {
    pub node_count: usize,
    pub edge_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GraphSnapshot {
    adjacency: BTreeMap<String, BTreeSet<String>>,
    edge_count: usize,
}

impl GraphSnapshot {
    pub fn from_stream(
        request_context: &RequestContext,
        stream: &dyn AdjacencyStream,
        config: GraphBuildConfig,
    ) -> Result<(Self, GraphBuildStats), LinkPredictError> {
        check_active(request_context)?;
        let mut node_ids = Vec::new();
        stream.stream_nodes(request_context, &mut |node_id| {
            check_active(request_context)?;
            if node_ids.len() >= config.max_nodes {
                return Err(LinkPredictError::Adjacency(format!(
                    "node limit {} exceeded",
                    config.max_nodes
                )));
            }
            node_ids.push(node_id.to_owned());
            Ok(())
        })?;
        node_ids.sort();
        node_ids.dedup();

        let known_nodes = node_ids.iter().cloned().collect::<BTreeSet<_>>();
        let mut adjacency = node_ids
            .iter()
            .cloned()
            .map(|node_id| (node_id, BTreeSet::new()))
            .collect::<BTreeMap<_, _>>();
        let mut directed_edges = BTreeSet::new();
        for node_id in &node_ids {
            check_active(request_context)?;
            stream.stream_outgoing(request_context, node_id, &mut |target_id| {
                check_active(request_context)?;
                if !known_nodes.contains(target_id) {
                    return Ok(());
                }
                if directed_edges.insert((node_id.clone(), target_id.to_owned()))
                    && directed_edges.len() > config.max_edges
                {
                    return Err(LinkPredictError::Adjacency(format!(
                        "edge limit {} exceeded",
                        config.max_edges
                    )));
                }
                adjacency
                    .get_mut(node_id)
                    .expect("streamed node must exist")
                    .insert(target_id.to_owned());
                if config.undirected {
                    adjacency
                        .get_mut(target_id)
                        .expect("known target must exist")
                        .insert(node_id.clone());
                }
                Ok(())
            })?;
        }

        let stats = GraphBuildStats {
            node_count: adjacency.len(),
            edge_count: directed_edges.len(),
        };
        Ok((
            Self {
                adjacency,
                edge_count: directed_edges.len(),
            },
            stats,
        ))
    }

    pub fn from_edges<I, S>(nodes: I, edges: &[(S, S)], undirected: bool) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut adjacency = nodes
            .into_iter()
            .map(|node| (node.as_ref().to_owned(), BTreeSet::new()))
            .collect::<BTreeMap<_, _>>();
        let mut edge_count = 0;
        for (source, target) in edges {
            let source = source.as_ref();
            let target = target.as_ref();
            if !adjacency.contains_key(source) || !adjacency.contains_key(target) {
                continue;
            }
            if adjacency
                .get_mut(source)
                .expect("source exists")
                .insert(target.to_owned())
            {
                edge_count += 1;
            }
            if undirected {
                adjacency
                    .get_mut(target)
                    .expect("target exists")
                    .insert(source.to_owned());
            }
        }
        Self {
            adjacency,
            edge_count,
        }
    }

    pub fn node_count(&self) -> usize {
        self.adjacency.len()
    }

    pub fn edge_count(&self) -> usize {
        self.edge_count
    }

    pub fn contains_node(&self, node_id: &str) -> bool {
        self.adjacency.contains_key(node_id)
    }

    pub fn contains_edge(&self, source: &str, target: &str) -> bool {
        self.neighbors(source).contains(target)
    }

    pub fn degree(&self, node_id: &str) -> usize {
        self.neighbors(node_id).len()
    }

    pub fn neighbors(&self, node_id: &str) -> &BTreeSet<String> {
        static EMPTY: BTreeSet<String> = BTreeSet::new();
        self.adjacency.get(node_id).unwrap_or(&EMPTY)
    }

    pub(crate) fn node_ids(&self) -> impl Iterator<Item = &str> {
        self.adjacency.keys().map(String::as_str)
    }

    pub(crate) fn two_hop_candidates(&self, source: &str) -> BTreeSet<String> {
        let neighbors = self.neighbors(source);
        neighbors
            .iter()
            .flat_map(|neighbor| self.neighbors(neighbor))
            .filter(|candidate| candidate.as_str() != source && !neighbors.contains(*candidate))
            .cloned()
            .collect()
    }
}

fn check_active(request_context: &RequestContext) -> Result<(), LinkPredictError> {
    request_context
        .check_active()
        .map_err(|_| LinkPredictError::RequestCancelled)
}

fn storage_error(error: copperdb_storage::StorageError) -> LinkPredictError {
    match error {
        copperdb_storage::StorageError::RequestCancelled(_) => LinkPredictError::RequestCancelled,
        error => LinkPredictError::Adjacency(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use copperdb_storage::{EdgeRecord, NodeEmbeddingMetadata, NodeRecord, StorageEngine};

    use super::*;

    struct Stream {
        nodes: Vec<String>,
        outgoing: BTreeMap<String, Vec<String>>,
    }

    impl AdjacencyStream for Stream {
        fn stream_nodes(
            &self,
            _request_context: &RequestContext,
            visit: &mut dyn FnMut(&str) -> Result<(), LinkPredictError>,
        ) -> Result<(), LinkPredictError> {
            for node in &self.nodes {
                visit(node)?;
            }
            Ok(())
        }

        fn stream_outgoing(
            &self,
            _request_context: &RequestContext,
            node_id: &str,
            visit: &mut dyn FnMut(&str) -> Result<(), LinkPredictError>,
        ) -> Result<(), LinkPredictError> {
            for target in self.outgoing.get(node_id).into_iter().flatten() {
                visit(target)?;
            }
            Ok(())
        }
    }

    #[test]
    fn streamed_snapshot_is_bounded_deterministic_and_supports_direction() {
        let stream = Stream {
            nodes: vec!["c".into(), "a".into(), "b".into()],
            outgoing: BTreeMap::from([
                ("a".into(), vec!["b".into(), "missing".into()]),
                ("b".into(), vec!["c".into()]),
            ]),
        };
        let (undirected, stats) = GraphSnapshot::from_stream(
            &RequestContext::detached(),
            &stream,
            GraphBuildConfig::default(),
        )
        .unwrap();
        assert_eq!(
            stats,
            GraphBuildStats {
                node_count: 3,
                edge_count: 2
            }
        );
        assert!(undirected.contains_edge("b", "a"));
        assert!(!undirected.contains_node("missing"));

        let (directed, _) = GraphSnapshot::from_stream(
            &RequestContext::detached(),
            &stream,
            GraphBuildConfig {
                undirected: false,
                ..GraphBuildConfig::default()
            },
        )
        .unwrap();
        assert!(directed.contains_edge("a", "b"));
        assert!(!directed.contains_edge("b", "a"));
    }

    #[test]
    fn streamed_snapshot_honors_cancellation_and_limits() {
        let stream = Stream {
            nodes: vec!["a".into(), "b".into()],
            outgoing: BTreeMap::new(),
        };
        let context = RequestContext::detached();
        context.cancel();
        assert_eq!(
            GraphSnapshot::from_stream(&context, &stream, GraphBuildConfig::default()).unwrap_err(),
            LinkPredictError::RequestCancelled
        );
        assert_eq!(
            GraphSnapshot::from_stream(
                &RequestContext::detached(),
                &stream,
                GraphBuildConfig {
                    max_nodes: 1,
                    ..GraphBuildConfig::default()
                },
            )
            .unwrap_err(),
            LinkPredictError::Adjacency("node limit 1 exceeded".into())
        );

        let edge_stream = Stream {
            nodes: vec!["a".into(), "b".into(), "c".into()],
            outgoing: BTreeMap::from([("a".into(), vec!["b".into(), "c".into()])]),
        };
        assert_eq!(
            GraphSnapshot::from_stream(
                &RequestContext::detached(),
                &edge_stream,
                GraphBuildConfig {
                    max_edges: 1,
                    ..GraphBuildConfig::default()
                },
            )
            .unwrap_err(),
            LinkPredictError::Adjacency("edge limit 1 exceeded".into())
        );
    }

    #[test]
    fn storage_engine_streams_maintained_adjacency_into_snapshot() {
        let storage = StorageEngine::open_memory().unwrap();
        for id in ["a", "b", "c"] {
            storage
                .put_node_record(&NodeRecord {
                    id: id.into(),
                    labels: vec!["Document".into()],
                    properties: BTreeMap::new(),
                    named_embeddings: BTreeMap::new(),
                    chunk_embeddings: Vec::new(),
                    embed_meta: NodeEmbeddingMetadata::default(),
                    created_at_unix_ms: 1,
                    updated_at_unix_ms: 1,
                })
                .unwrap();
        }
        for (id, source, target) in [("e1", "a", "b"), ("e2", "b", "c")] {
            storage
                .put_edge_record(&EdgeRecord {
                    id: id.into(),
                    start_node: source.into(),
                    end_node: target.into(),
                    edge_type: "LINKS".into(),
                    properties: BTreeMap::new(),
                    created_at_unix_ms: 1,
                    updated_at_unix_ms: 1,
                })
                .unwrap();
        }

        let (graph, stats) = GraphSnapshot::from_stream(
            &RequestContext::detached(),
            &storage,
            GraphBuildConfig::default(),
        )
        .unwrap();

        assert_eq!(
            stats,
            GraphBuildStats {
                node_count: 3,
                edge_count: 2
            }
        );
        assert_eq!(
            graph.neighbors("b").iter().cloned().collect::<Vec<_>>(),
            vec!["a", "c"]
        );
        assert!(graph.contains_edge("c", "b"));
    }
}
