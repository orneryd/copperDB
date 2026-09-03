use std::collections::BTreeMap;

use copperdb_linkpredict::{
    common_neighbors, AdjacencyStream, GraphBuildConfig, GraphSnapshot, LinkPredictError,
};
use copperdb_util::RequestContext;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

struct GeneratedGraph {
    nodes: Vec<String>,
    outgoing: BTreeMap<String, Vec<String>>,
}

impl GeneratedGraph {
    fn new(node_count: usize, degree: usize) -> Self {
        let nodes = (0..node_count)
            .map(|index| format!("node-{index:05}"))
            .collect::<Vec<_>>();
        let outgoing = nodes
            .iter()
            .enumerate()
            .map(|(index, node)| {
                let targets = (1..=degree)
                    .map(|offset| nodes[(index + offset) % node_count].clone())
                    .collect();
                (node.clone(), targets)
            })
            .collect();
        Self { nodes, outgoing }
    }
}

impl AdjacencyStream for GeneratedGraph {
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

fn bench_graph_build(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("link_prediction_graph_build");
    for (node_count, degree) in [(1_000_usize, 10_usize), (10_000, 10)] {
        let stream = GeneratedGraph::new(node_count, degree);
        group.throughput(Throughput::Elements((node_count * degree) as u64));
        group.bench_with_input(
            BenchmarkId::new("streamed_snapshot", node_count),
            &stream,
            |bench, stream| {
                bench.iter(|| {
                    black_box(
                        GraphSnapshot::from_stream(
                            &RequestContext::detached(),
                            black_box(stream),
                            GraphBuildConfig::default(),
                        )
                        .unwrap(),
                    )
                });
            },
        );
    }
    group.finish();
}

fn bench_candidate_generation_by_degree(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("link_prediction_candidates_by_degree");
    for degree in [4_usize, 16, 64] {
        let stream = GeneratedGraph::new(10_000, degree);
        let (graph, _) = GraphSnapshot::from_stream(
            &RequestContext::detached(),
            &stream,
            GraphBuildConfig::default(),
        )
        .unwrap();
        group.throughput(Throughput::Elements(degree as u64));
        group.bench_with_input(
            BenchmarkId::new("common_neighbors", degree),
            &graph,
            |bench, graph| {
                bench.iter(|| black_box(common_neighbors(black_box(graph), "node-05000", 20)));
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_graph_build,
    bench_candidate_generation_by_degree
);
criterion_main!(benches);
