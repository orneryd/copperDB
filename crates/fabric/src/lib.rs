//! Distributed fabric for copperdb.
//!
//! Equivalent to Go's `pkg/fabric` in NornicDB.
//! Provides multi-datacenter routing, workload distribution, and
//! network topology awareness for the distributed cluster.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

pub use copperdb_topology::{
    ConsistencyLevel, DistributedReadPlan, DistributedSearchPlan, DistributedWriteMode,
    DistributedWritePlan, FabricDatabase, FabricGlobalId, FabricPartitionPolicy, FabricShard,
    FabricShardKind, HyperscalerProfile, MeshPeer, NodeCapability, PeerHealth, PlacementKey,
    PlacementRecord, TopologyError, TopologyRegistry,
};

#[derive(Debug, Error)]
pub enum FabricError {
    #[error("no available node for database {0}")]
    NoNodeAvailable(String),
    #[error("routing error: {0}")]
    RoutingError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FabricReadScope {
    AllShards,
    DefaultShard,
    Shard(String),
    Label(String),
    RelationshipType(String),
    Collection(String),
    GlobalId(FabricGlobalId),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FabricReadRequest {
    pub scope: FabricReadScope,
    pub consistency: ConsistencyLevel,
    pub request_region: Option<String>,
}

impl FabricReadRequest {
    pub fn scatter(consistency: ConsistencyLevel) -> Self {
        Self {
            scope: FabricReadScope::AllShards,
            consistency,
            request_region: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FabricShardReadPlan {
    pub shard: FabricShard,
    pub read_plan: DistributedReadPlan,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FabricReadPlan {
    pub database: FabricDatabase,
    pub scope: FabricReadScope,
    pub shards: Vec<FabricShardReadPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FabricRowBatch {
    pub shard: PlacementKey,
    pub rows: Vec<Value>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum FabricSortDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FabricSortKey {
    pub column: String,
    pub direction: FabricSortDirection,
}

impl FabricSortKey {
    pub fn ascending(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            direction: FabricSortDirection::Ascending,
        }
    }

    pub fn descending(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            direction: FabricSortDirection::Descending,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct FabricRowMergeOptions {
    pub distinct: bool,
    pub order_by: Vec<FabricSortKey>,
    pub skip: usize,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FabricMergedRows {
    pub rows: Vec<Value>,
    pub touched_shards: Vec<PlacementKey>,
    pub input_rows: usize,
    pub output_rows: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum FabricAggregateKind {
    Count,
    Sum,
    Average,
    Min,
    Max,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FabricAggregateSpec {
    pub output: String,
    pub kind: FabricAggregateKind,
    pub column: Option<String>,
    pub distinct: bool,
}

impl FabricAggregateSpec {
    pub fn count(output: impl Into<String>) -> Self {
        Self {
            output: output.into(),
            kind: FabricAggregateKind::Count,
            column: None,
            distinct: false,
        }
    }

    pub fn sum(output: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            output: output.into(),
            kind: FabricAggregateKind::Sum,
            column: Some(column.into()),
            distinct: false,
        }
    }

    pub fn average(output: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            output: output.into(),
            kind: FabricAggregateKind::Average,
            column: Some(column.into()),
            distinct: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FabricAggregateOptions {
    pub group_by: Vec<String>,
    pub aggregates: Vec<FabricAggregateSpec>,
    pub order_by: Vec<FabricSortKey>,
    pub skip: usize,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
struct FabricAggregateAccumulator {
    spec: FabricAggregateSpec,
    count: u64,
    sum: f64,
    min: Option<Value>,
    max: Option<Value>,
    distinct_values: BTreeSet<String>,
}

#[derive(Debug, Clone)]
struct FabricGroupAccumulator {
    values: Vec<Value>,
    aggregates: Vec<FabricAggregateAccumulator>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FabricPath {
    pub nodes: Vec<FabricGlobalId>,
    pub relationships: Vec<FabricGlobalId>,
    pub cost: Option<f64>,
    pub metadata: Value,
}

impl FabricPath {
    pub fn new(nodes: Vec<FabricGlobalId>, relationships: Vec<FabricGlobalId>) -> Self {
        Self {
            nodes,
            relationships,
            cost: None,
            metadata: Value::Null,
        }
    }

    pub fn length(&self) -> usize {
        self.relationships.len()
    }

    pub fn stable_id(&self) -> String {
        let nodes = self
            .nodes
            .iter()
            .map(FabricGlobalId::stable_id)
            .collect::<Vec<_>>()
            .join(">");
        let relationships = self
            .relationships
            .iter()
            .map(FabricGlobalId::stable_id)
            .collect::<Vec<_>>()
            .join(">");
        format!("nodes:{nodes}|relationships:{relationships}")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FabricPathBatch {
    pub shard: PlacementKey,
    pub paths: Vec<FabricPath>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FabricPathMergeOptions {
    pub distinct: bool,
    pub shortest_first: bool,
    pub lowest_cost_first: bool,
    pub skip: usize,
    pub limit: Option<usize>,
}

impl Default for FabricPathMergeOptions {
    fn default() -> Self {
        Self {
            distinct: true,
            shortest_first: true,
            lowest_cost_first: false,
            skip: 0,
            limit: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FabricMergedPaths {
    pub paths: Vec<FabricPath>,
    pub touched_shards: Vec<PlacementKey>,
    pub input_paths: usize,
    pub output_paths: usize,
}

#[derive(Debug, Clone)]
struct PositionedPath {
    shard: PlacementKey,
    path_index: usize,
    path: FabricPath,
}

#[derive(Debug, Clone)]
struct PositionedRow {
    shard: PlacementKey,
    row_index: usize,
    row: Value,
}

pub fn merge_fabric_rows(
    shard_rows: Vec<FabricRowBatch>,
    options: FabricRowMergeOptions,
) -> FabricMergedRows {
    let touched_shards = shard_rows
        .iter()
        .map(|batch| batch.shard.clone())
        .collect::<Vec<_>>();
    let input_rows = shard_rows.iter().map(|batch| batch.rows.len()).sum();
    let mut rows = Vec::new();
    let mut seen = BTreeSet::new();

    for batch in shard_rows {
        for (row_index, row) in batch.rows.into_iter().enumerate() {
            if options.distinct {
                let stable = serde_json::to_string(&row).unwrap_or_else(|_| format!("{row:?}"));
                if !seen.insert(stable) {
                    continue;
                }
            }
            rows.push(PositionedRow {
                shard: batch.shard.clone(),
                row_index,
                row,
            });
        }
    }

    if !options.order_by.is_empty() {
        rows.sort_by(|left, right| compare_positioned_rows(left, right, &options.order_by));
    }

    let rows = rows
        .into_iter()
        .skip(options.skip)
        .take(options.limit.unwrap_or(usize::MAX))
        .map(|positioned| positioned.row)
        .collect::<Vec<_>>();
    let output_rows = rows.len();
    FabricMergedRows {
        rows,
        touched_shards,
        input_rows,
        output_rows,
    }
}

pub fn merge_fabric_aggregates(
    shard_rows: Vec<FabricRowBatch>,
    options: FabricAggregateOptions,
) -> FabricMergedRows {
    let touched_shards = shard_rows
        .iter()
        .map(|batch| batch.shard.clone())
        .collect::<Vec<_>>();
    let input_rows = shard_rows.iter().map(|batch| batch.rows.len()).sum();
    let mut groups = BTreeMap::<String, FabricGroupAccumulator>::new();

    for batch in shard_rows {
        for row in batch.rows {
            let group_values = options
                .group_by
                .iter()
                .map(|column| {
                    lookup_row_value(&row, column)
                        .cloned()
                        .unwrap_or(Value::Null)
                })
                .collect::<Vec<_>>();
            let group_key = serde_json::to_string(&group_values)
                .unwrap_or_else(|_| format!("{group_values:?}"));
            let group = groups
                .entry(group_key)
                .or_insert_with(|| FabricGroupAccumulator {
                    values: group_values,
                    aggregates: options
                        .aggregates
                        .iter()
                        .cloned()
                        .map(FabricAggregateAccumulator::new)
                        .collect(),
                });
            for aggregate in &mut group.aggregates {
                aggregate.apply(&row);
            }
        }
    }

    if groups.is_empty() && options.group_by.is_empty() {
        groups.insert(
            "[]".into(),
            FabricGroupAccumulator {
                values: Vec::new(),
                aggregates: options
                    .aggregates
                    .iter()
                    .cloned()
                    .map(FabricAggregateAccumulator::new)
                    .collect(),
            },
        );
    }

    let rows = groups
        .into_values()
        .map(|group| group.into_row(&options.group_by))
        .collect::<Vec<_>>();
    merge_fabric_rows(
        vec![FabricRowBatch {
            shard: PlacementKey::new("fabric", "aggregate", "merge"),
            rows,
        }],
        FabricRowMergeOptions {
            distinct: false,
            order_by: options.order_by,
            skip: options.skip,
            limit: options.limit,
        },
    )
    .with_touched_shards(touched_shards, input_rows)
}

pub fn merge_fabric_paths(
    shard_paths: Vec<FabricPathBatch>,
    options: FabricPathMergeOptions,
) -> FabricMergedPaths {
    let touched_shards = shard_paths
        .iter()
        .map(|batch| batch.shard.clone())
        .collect::<Vec<_>>();
    let input_paths = shard_paths.iter().map(|batch| batch.paths.len()).sum();
    let mut paths = Vec::new();
    let mut seen = BTreeSet::new();

    for batch in shard_paths {
        for (path_index, path) in batch.paths.into_iter().enumerate() {
            if options.distinct && !seen.insert(path.stable_id()) {
                continue;
            }
            paths.push(PositionedPath {
                shard: batch.shard.clone(),
                path_index,
                path,
            });
        }
    }

    paths.sort_by(|left, right| compare_positioned_paths(left, right, &options));
    let paths = paths
        .into_iter()
        .skip(options.skip)
        .take(options.limit.unwrap_or(usize::MAX))
        .map(|positioned| positioned.path)
        .collect::<Vec<_>>();
    let output_paths = paths.len();
    FabricMergedPaths {
        paths,
        touched_shards,
        input_paths,
        output_paths,
    }
}

impl FabricMergedRows {
    fn with_touched_shards(mut self, touched_shards: Vec<PlacementKey>, input_rows: usize) -> Self {
        self.touched_shards = touched_shards;
        self.input_rows = input_rows;
        self
    }
}

impl FabricAggregateAccumulator {
    fn new(spec: FabricAggregateSpec) -> Self {
        Self {
            spec,
            count: 0,
            sum: 0.0,
            min: None,
            max: None,
            distinct_values: BTreeSet::new(),
        }
    }

    fn apply(&mut self, row: &Value) {
        let value = self
            .spec
            .column
            .as_deref()
            .and_then(|column| lookup_row_value(row, column));
        if self.spec.column.is_some() && value.is_none() {
            return;
        }
        if self.spec.distinct {
            let stable = value
                .map(|value| serde_json::to_string(value).unwrap_or_else(|_| format!("{value:?}")))
                .unwrap_or_else(|| "*".into());
            if !self.distinct_values.insert(stable) {
                return;
            }
        }

        match self.spec.kind {
            FabricAggregateKind::Count => self.count += 1,
            FabricAggregateKind::Sum | FabricAggregateKind::Average => {
                if let Some(number) = value.and_then(Value::as_f64) {
                    self.count += 1;
                    self.sum += number;
                }
            }
            FabricAggregateKind::Min => {
                if let Some(value) = value {
                    let replace = self
                        .min
                        .as_ref()
                        .map(|current| compare_values(value, current) == Ordering::Less)
                        .unwrap_or(true);
                    if replace {
                        self.min = Some(value.clone());
                    }
                }
            }
            FabricAggregateKind::Max => {
                if let Some(value) = value {
                    let replace = self
                        .max
                        .as_ref()
                        .map(|current| compare_values(value, current) == Ordering::Greater)
                        .unwrap_or(true);
                    if replace {
                        self.max = Some(value.clone());
                    }
                }
            }
        }
    }

    fn finish(&self) -> Value {
        match self.spec.kind {
            FabricAggregateKind::Count => Value::Number(self.count.into()),
            FabricAggregateKind::Sum => number_value(self.sum),
            FabricAggregateKind::Average => {
                if self.count == 0 {
                    Value::Null
                } else {
                    number_value(self.sum / self.count as f64)
                }
            }
            FabricAggregateKind::Min => self.min.clone().unwrap_or(Value::Null),
            FabricAggregateKind::Max => self.max.clone().unwrap_or(Value::Null),
        }
    }
}

impl FabricGroupAccumulator {
    fn into_row(self, group_by: &[String]) -> Value {
        let mut row = serde_json::Map::new();
        for (column, value) in group_by.iter().zip(self.values) {
            row.insert(column.clone(), value);
        }
        for aggregate in self.aggregates {
            row.insert(aggregate.spec.output.clone(), aggregate.finish());
        }
        Value::Object(row)
    }
}

fn number_value(value: f64) -> Value {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

fn compare_positioned_rows(
    left: &PositionedRow,
    right: &PositionedRow,
    order_by: &[FabricSortKey],
) -> Ordering {
    for key in order_by {
        let ordering = compare_optional_values(
            lookup_row_value(&left.row, &key.column),
            lookup_row_value(&right.row, &key.column),
        );
        let ordering = match key.direction {
            FabricSortDirection::Ascending => ordering,
            FabricSortDirection::Descending => ordering.reverse(),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.shard
        .stable_id()
        .cmp(&right.shard.stable_id())
        .then(left.row_index.cmp(&right.row_index))
}

fn compare_positioned_paths(
    left: &PositionedPath,
    right: &PositionedPath,
    options: &FabricPathMergeOptions,
) -> Ordering {
    if options.shortest_first {
        let ordering = left.path.length().cmp(&right.path.length());
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    if options.lowest_cost_first {
        let ordering = compare_optional_f64(left.path.cost, right.path.cost);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.path
        .stable_id()
        .cmp(&right.path.stable_id())
        .then(left.shard.stable_id().cmp(&right.shard.stable_id()))
        .then(left.path_index.cmp(&right.path_index))
}

fn compare_optional_f64(left: Option<f64>, right: Option<f64>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.total_cmp(&right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn lookup_row_value<'a>(row: &'a Value, column: &str) -> Option<&'a Value> {
    let mut current = row;
    for part in column.split('.') {
        current = current.as_object()?.get(part)?;
    }
    Some(current)
}

fn compare_optional_values(left: Option<&Value>, right: Option<&Value>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => compare_values(left, right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn compare_values(left: &Value, right: &Value) -> Ordering {
    let left_rank = value_rank(left);
    let right_rank = value_rank(right);
    if left_rank != right_rank {
        return left_rank.cmp(&right_rank);
    }
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        (Value::Number(left), Value::Number(right)) => left
            .as_f64()
            .unwrap_or(f64::NAN)
            .total_cmp(&right.as_f64().unwrap_or(f64::NAN)),
        (Value::String(left), Value::String(right)) => left.cmp(right),
        (Value::Array(left), Value::Array(right)) => compare_slices(left, right),
        (Value::Object(left), Value::Object(right)) => serde_json::to_string(left)
            .unwrap_or_default()
            .cmp(&serde_json::to_string(right).unwrap_or_default()),
        _ => Ordering::Equal,
    }
}

fn compare_slices(left: &[Value], right: &[Value]) -> Ordering {
    for (left, right) in left.iter().zip(right.iter()) {
        let ordering = compare_values(left, right);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.len().cmp(&right.len())
}

fn value_rank(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

/// A cluster node descriptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNode {
    pub id: u64,
    pub address: String,
    pub region: String,
    pub role: NodeRole,
    pub databases: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeRole {
    Primary,
    Secondary,
    ReadReplica,
}

/// Route a query to the appropriate cluster node.
#[derive(Default)]
pub struct Router {
    nodes: Vec<ClusterNode>,
}

/// Shared topology-backed routing facade for future distributed execution.
///
/// This is a planning seam only: callers get a search or write plan, while
/// transport execution remains in `search`/`replication` until HA is enabled.
#[derive(Debug, Clone, Default)]
pub struct FabricTopology {
    registry: TopologyRegistry,
}

impl FabricTopology {
    pub fn new(registry: TopologyRegistry) -> Self {
        Self { registry }
    }

    pub fn registry(&self) -> &TopologyRegistry {
        &self.registry
    }

    pub fn registry_mut(&mut self) -> &mut TopologyRegistry {
        &mut self.registry
    }

    pub fn plan_search(
        &self,
        placement: &PlacementKey,
    ) -> Result<DistributedSearchPlan, TopologyError> {
        self.registry.plan_search(placement)
    }

    pub fn plan_write(
        &self,
        placement: &PlacementKey,
        mode: DistributedWriteMode,
    ) -> Result<DistributedWritePlan, TopologyError> {
        self.registry.plan_write(placement, mode)
    }

    pub fn plan_write_with_consistency(
        &self,
        placement: &PlacementKey,
        mode: DistributedWriteMode,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
    ) -> Result<DistributedWritePlan, TopologyError> {
        self.registry
            .plan_write_with_consistency(placement, mode, consistency, request_region)
    }

    pub fn plan_read(
        &self,
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
    ) -> Result<DistributedReadPlan, TopologyError> {
        self.registry
            .plan_read(placement, consistency, request_region)
    }

    pub fn plan_fabric_reads(
        &self,
        database: &FabricDatabase,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
    ) -> Result<Vec<DistributedReadPlan>, TopologyError> {
        database.validate()?;
        database
            .placement_keys()
            .iter()
            .map(|placement| self.plan_read(placement, consistency, request_region))
            .collect()
    }

    pub fn plan_fabric_searches(
        &self,
        database: &FabricDatabase,
    ) -> Result<Vec<DistributedSearchPlan>, TopologyError> {
        database.validate()?;
        database
            .placement_keys()
            .iter()
            .map(|placement| self.plan_search(placement))
            .collect()
    }

    pub fn resolve_fabric_read_shards(
        &self,
        database: &FabricDatabase,
        scope: &FabricReadScope,
    ) -> Result<Vec<FabricShard>, TopologyError> {
        database.validate()?;
        let shards = match scope {
            FabricReadScope::AllShards => database.shards.clone(),
            FabricReadScope::DefaultShard => database
                .shards
                .iter()
                .filter(|shard| shard.placement.shard == database.default_shard)
                .cloned()
                .collect(),
            FabricReadScope::Shard(shard_name) => database
                .shards
                .iter()
                .filter(|shard| shard.placement.shard == *shard_name)
                .cloned()
                .collect(),
            FabricReadScope::Label(label) => database
                .shards
                .iter()
                .filter(|shard| shard.labels.iter().any(|value| value == label))
                .cloned()
                .collect(),
            FabricReadScope::RelationshipType(relationship_type) => database
                .shards
                .iter()
                .filter(|shard| {
                    shard
                        .relationship_types
                        .iter()
                        .any(|value| value == relationship_type)
                })
                .cloned()
                .collect(),
            FabricReadScope::Collection(collection) => database
                .shards
                .iter()
                .filter(|shard| shard.collections.iter().any(|value| value == collection))
                .cloned()
                .collect(),
            FabricReadScope::GlobalId(global_id) => database
                .shards
                .iter()
                .filter(|shard| shard.placement == global_id.placement)
                .cloned()
                .collect(),
        };
        if shards.is_empty() {
            return Err(TopologyError::MissingPlacement(format!(
                "no fabric shard in {} matched {:?}",
                database.stable_id(),
                scope
            )));
        }
        Ok(shards)
    }

    pub fn plan_fabric_query_reads(
        &self,
        database: &FabricDatabase,
        request: FabricReadRequest,
    ) -> Result<FabricReadPlan, TopologyError> {
        let shards = self.resolve_fabric_read_shards(database, &request.scope)?;
        let request_region = request.request_region.as_deref();
        let mut planned = Vec::with_capacity(shards.len());
        for shard in shards {
            let read_plan =
                self.plan_read(&shard.placement, request.consistency, request_region)?;
            planned.push(FabricShardReadPlan { shard, read_plan });
        }
        Ok(FabricReadPlan {
            database: database.clone(),
            scope: request.scope,
            shards: planned,
        })
    }
}

impl Router {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_node(&mut self, node: ClusterNode) {
        self.nodes.push(node);
    }

    /// Find the primary node for a given database.
    pub fn primary_for(&self, database: &str) -> Option<&ClusterNode> {
        self.nodes
            .iter()
            .find(|n| n.role == NodeRole::Primary && n.databases.contains(&database.to_string()))
    }

    /// Find any readable node for a given database (prefer local region).
    pub fn readable_for(&self, database: &str, preferred_region: &str) -> Option<&ClusterNode> {
        // Prefer local region
        self.nodes
            .iter()
            .filter(|n| n.databases.contains(&database.to_string()))
            .min_by_key(|n| if n.region == preferred_region { 0 } else { 1 })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_router_find_primary() {
        let mut router = Router::new();
        router.add_node(ClusterNode {
            id: 1,
            address: "localhost:7687".into(),
            region: "us-east".into(),
            role: NodeRole::Primary,
            databases: vec!["default".into()],
        });
        let node = router.primary_for("default");
        assert!(node.is_some());
        assert_eq!(node.unwrap().id, 1);
    }

    #[test]
    fn test_router_find_readable() {
        let mut router = Router::new();
        router.add_node(ClusterNode {
            id: 1,
            address: "us-east:7687".into(),
            region: "us-east".into(),
            role: NodeRole::Primary,
            databases: vec!["default".into()],
        });
        router.add_node(ClusterNode {
            id: 2,
            address: "eu-west:7687".into(),
            region: "eu-west".into(),
            role: NodeRole::ReadReplica,
            databases: vec!["default".into()],
        });
        // Prefer local region
        let node = router.readable_for("default", "eu-west").unwrap();
        assert_eq!(node.id, 2);
    }

    #[test]
    fn test_router_no_primary() {
        let router = Router::new();
        assert!(router.primary_for("nonexistent").is_none());
    }

    #[test]
    fn test_cluster_node_serialization() {
        let node = ClusterNode {
            id: 42,
            address: "host:7687".into(),
            region: "ap-south".into(),
            role: NodeRole::Secondary,
            databases: vec!["db1".into(), "db2".into()],
        };
        let json = serde_json::to_string(&node).unwrap();
        let decoded: ClusterNode = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.id, 42);
        assert_eq!(decoded.databases.len(), 2);
    }

    #[test]
    fn test_node_roles() {
        assert_ne!(NodeRole::Primary, NodeRole::Secondary);
        assert_ne!(NodeRole::Secondary, NodeRole::ReadReplica);
    }

    #[test]
    fn topology_facade_returns_search_and_write_plans() {
        let mut registry = TopologyRegistry::new();
        registry
            .register_peer(
                MeshPeer::new("n1", "n1.mesh.local:9000")
                    .with_capability(NodeCapability::Storage)
                    .with_capability(NodeCapability::Search)
                    .with_capability(NodeCapability::Coordinator)
                    .with_capability(NodeCapability::WriteLeader),
            )
            .unwrap();
        registry
            .register_placement(PlacementRecord::standalone("copper", "n1"))
            .unwrap();

        let fabric = FabricTopology::new(registry);
        let placement = PlacementKey::default_for_database("copper");
        assert_eq!(fabric.plan_search(&placement).unwrap().fanout.len(), 1);
        assert_eq!(
            fabric
                .plan_write(&placement, DistributedWriteMode::Standalone)
                .unwrap()
                .required_acks,
            1
        );
        assert_eq!(
            fabric
                .plan_read(&placement, ConsistencyLevel::One, None)
                .unwrap()
                .required_responses,
            1
        );
    }

    #[test]
    fn topology_facade_plans_all_fabric_shard_reads_and_searches() {
        let mut registry = TopologyRegistry::new();
        for node in ["n1", "n2"] {
            registry
                .register_peer(
                    MeshPeer::new(node, format!("{node}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Search)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        for (shard, node) in [("primary", "n1"), ("person-00", "n2")] {
            registry
                .register_placement(PlacementRecord {
                    key: PlacementKey::new("default", "copper", shard),
                    primary_node: node.into(),
                    replica_nodes: vec![],
                    search_nodes: vec![node.into()],
                    hyperscaler_profile: None,
                    min_write_replicas: 0,
                    search_fanout: 1,
                })
                .unwrap();
        }

        let database = FabricDatabase {
            tenant: "default".into(),
            database: "copper".into(),
            default_shard: "primary".into(),
            partition_policy: FabricPartitionPolicy::HashByKey { buckets: 2 },
            shards: vec![
                FabricShard::mixed(PlacementKey::new("default", "copper", "primary")),
                FabricShard {
                    placement: PlacementKey::new("default", "copper", "person-00"),
                    kind: FabricShardKind::Graph,
                    labels: vec!["Person".into()],
                    relationship_types: vec![],
                    collections: vec![],
                },
            ],
        };
        let fabric = FabricTopology::new(registry);

        let reads = fabric
            .plan_fabric_reads(&database, ConsistencyLevel::One, None)
            .unwrap();
        let searches = fabric.plan_fabric_searches(&database).unwrap();

        assert_eq!(reads.len(), 2);
        assert_eq!(searches.len(), 2);
        assert_eq!(reads[0].placement.shard, "primary");
        assert_eq!(reads[1].placement.shard, "person-00");
    }

    #[test]
    fn fabric_read_planner_targets_scope_or_scatters_deterministically() {
        let mut registry = TopologyRegistry::new();
        for node in ["primary-node", "person-node", "memory-node"] {
            registry
                .register_peer(
                    MeshPeer::new(node, format!("{node}.mesh.local:9000"))
                        .with_capability(NodeCapability::Storage)
                        .with_capability(NodeCapability::Coordinator),
                )
                .unwrap();
        }
        for (shard, node) in [
            ("primary", "primary-node"),
            ("person-00", "person-node"),
            ("memory-00", "memory-node"),
        ] {
            registry
                .register_placement(PlacementRecord {
                    key: PlacementKey::new("default", "copper", shard),
                    primary_node: node.into(),
                    replica_nodes: vec![],
                    search_nodes: vec![],
                    hyperscaler_profile: None,
                    min_write_replicas: 0,
                    search_fanout: 1,
                })
                .unwrap();
        }

        let database = FabricDatabase {
            tenant: "default".into(),
            database: "copper".into(),
            default_shard: "primary".into(),
            partition_policy: FabricPartitionPolicy::LabelAware,
            shards: vec![
                FabricShard::mixed(PlacementKey::new("default", "copper", "primary")),
                FabricShard {
                    placement: PlacementKey::new("default", "copper", "person-00"),
                    kind: FabricShardKind::Graph,
                    labels: vec!["Person".into()],
                    relationship_types: vec!["KNOWS".into()],
                    collections: vec![],
                },
                FabricShard {
                    placement: PlacementKey::new("default", "copper", "memory-00"),
                    kind: FabricShardKind::Vector,
                    labels: vec!["Memory".into()],
                    relationship_types: vec![],
                    collections: vec!["memories".into()],
                },
            ],
        };
        let fabric = FabricTopology::new(registry);

        let person_plan = fabric
            .plan_fabric_query_reads(
                &database,
                FabricReadRequest {
                    scope: FabricReadScope::Label("Person".into()),
                    consistency: ConsistencyLevel::One,
                    request_region: None,
                },
            )
            .unwrap();
        assert_eq!(person_plan.shards.len(), 1);
        assert_eq!(person_plan.shards[0].shard.placement.shard, "person-00");
        assert_eq!(
            person_plan.shards[0].read_plan.coordinator.node_id,
            "person-node"
        );

        let global_id = FabricGlobalId::new(
            PlacementKey::new("default", "copper", "memory-00"),
            "node",
            "Memory:7",
        );
        let global_plan = fabric
            .plan_fabric_query_reads(
                &database,
                FabricReadRequest {
                    scope: FabricReadScope::GlobalId(global_id),
                    consistency: ConsistencyLevel::One,
                    request_region: None,
                },
            )
            .unwrap();
        assert_eq!(global_plan.shards.len(), 1);
        assert_eq!(global_plan.shards[0].shard.placement.shard, "memory-00");

        let scatter_plan = fabric
            .plan_fabric_query_reads(&database, FabricReadRequest::scatter(ConsistencyLevel::One))
            .unwrap();
        let shards = scatter_plan
            .shards
            .iter()
            .map(|plan| plan.shard.placement.shard.as_str())
            .collect::<Vec<_>>();
        assert_eq!(shards, vec!["primary", "person-00", "memory-00"]);
    }

    #[test]
    fn fabric_row_merge_orders_deduplicates_and_paginates_deterministically() {
        let primary = PlacementKey::new("default", "copper", "primary");
        let person = PlacementKey::new("default", "copper", "person-00");
        let merged = merge_fabric_rows(
            vec![
                FabricRowBatch {
                    shard: primary.clone(),
                    rows: vec![
                        serde_json::json!({"id": "p2", "score": 8, "name": "B"}),
                        serde_json::json!({"id": "p1", "score": 10, "name": "A"}),
                        serde_json::json!({"id": "dup", "score": 7, "name": "D"}),
                    ],
                },
                FabricRowBatch {
                    shard: person.clone(),
                    rows: vec![
                        serde_json::json!({"id": "p3", "score": 9, "name": "C"}),
                        serde_json::json!({"id": "dup", "score": 7, "name": "D"}),
                        serde_json::json!({"id": "p4", "score": 8, "name": "A"}),
                    ],
                },
            ],
            FabricRowMergeOptions {
                distinct: true,
                order_by: vec![
                    FabricSortKey::descending("score"),
                    FabricSortKey::ascending("name"),
                ],
                skip: 1,
                limit: Some(3),
            },
        );

        let ids = merged
            .rows
            .iter()
            .map(|row| row["id"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["p3", "p4", "p2"]);
        assert_eq!(merged.touched_shards, vec![primary, person]);
        assert_eq!(merged.input_rows, 6);
        assert_eq!(merged.output_rows, 3);
    }

    #[test]
    fn fabric_row_merge_preserves_shard_order_without_explicit_sort() {
        let primary = PlacementKey::new("default", "copper", "primary");
        let person = PlacementKey::new("default", "copper", "person-00");
        let merged = merge_fabric_rows(
            vec![
                FabricRowBatch {
                    shard: primary,
                    rows: vec![
                        serde_json::json!({"id": "a"}),
                        serde_json::json!({"id": "b"}),
                    ],
                },
                FabricRowBatch {
                    shard: person,
                    rows: vec![serde_json::json!({"id": "c"})],
                },
            ],
            FabricRowMergeOptions::default(),
        );

        let ids = merged
            .rows
            .iter()
            .map(|row| row["id"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    #[test]
    fn fabric_aggregate_merge_groups_counts_and_averages_across_shards() {
        let primary = PlacementKey::new("default", "copper", "primary");
        let person = PlacementKey::new("default", "copper", "person-00");
        let merged = merge_fabric_aggregates(
            vec![
                FabricRowBatch {
                    shard: primary.clone(),
                    rows: vec![
                        serde_json::json!({"label": "Person", "score": 10, "name": "Ada"}),
                        serde_json::json!({"label": "Person", "score": 6, "name": "Ada"}),
                    ],
                },
                FabricRowBatch {
                    shard: person.clone(),
                    rows: vec![
                        serde_json::json!({"label": "Person", "score": 8, "name": "Grace"}),
                        serde_json::json!({"label": "Memory", "score": 4, "name": "Note"}),
                    ],
                },
            ],
            FabricAggregateOptions {
                group_by: vec!["label".into()],
                aggregates: vec![
                    FabricAggregateSpec::count("count"),
                    FabricAggregateSpec::average("avg_score", "score"),
                    FabricAggregateSpec::sum("sum_score", "score"),
                    FabricAggregateSpec {
                        output: "distinct_names".into(),
                        kind: FabricAggregateKind::Count,
                        column: Some("name".into()),
                        distinct: true,
                    },
                ],
                order_by: vec![FabricSortKey::descending("count")],
                skip: 0,
                limit: None,
            },
        );

        assert_eq!(merged.touched_shards, vec![primary, person]);
        assert_eq!(merged.input_rows, 4);
        assert_eq!(merged.output_rows, 2);
        assert_eq!(merged.rows[0]["label"], "Person");
        assert_eq!(merged.rows[0]["count"], 3);
        assert_eq!(merged.rows[0]["avg_score"], 8.0);
        assert_eq!(merged.rows[0]["sum_score"], 24.0);
        assert_eq!(merged.rows[0]["distinct_names"], 2);
        assert_eq!(merged.rows[1]["label"], "Memory");
        assert_eq!(merged.rows[1]["count"], 1);
    }

    #[test]
    fn fabric_aggregate_merge_returns_global_zero_count_for_empty_input() {
        let merged = merge_fabric_aggregates(
            Vec::new(),
            FabricAggregateOptions {
                group_by: Vec::new(),
                aggregates: vec![
                    FabricAggregateSpec::count("count"),
                    FabricAggregateSpec::average("avg_score", "score"),
                ],
                order_by: Vec::new(),
                skip: 0,
                limit: None,
            },
        );

        assert_eq!(merged.input_rows, 0);
        assert_eq!(merged.output_rows, 1);
        assert_eq!(merged.rows[0]["count"], 0);
        assert_eq!(merged.rows[0]["avg_score"], Value::Null);
    }

    #[test]
    fn fabric_path_merge_deduplicates_and_prefers_shortest_paths() {
        let primary = PlacementKey::new("default", "copper", "primary");
        let person = PlacementKey::new("default", "copper", "person-00");
        let node_a = FabricGlobalId::new(primary.clone(), "node", "A");
        let node_b = FabricGlobalId::new(person.clone(), "node", "B");
        let node_c = FabricGlobalId::new(person.clone(), "node", "C");
        let edge_ab = FabricGlobalId::new(primary.clone(), "relationship", "AB");
        let edge_bc = FabricGlobalId::new(person.clone(), "relationship", "BC");
        let direct = FabricPath::new(vec![node_a.clone(), node_b.clone()], vec![edge_ab.clone()]);
        let longer = FabricPath::new(
            vec![node_a.clone(), node_c, node_b.clone()],
            vec![edge_ab.clone(), edge_bc],
        );

        let merged = merge_fabric_paths(
            vec![
                FabricPathBatch {
                    shard: primary.clone(),
                    paths: vec![longer.clone(), direct.clone()],
                },
                FabricPathBatch {
                    shard: person.clone(),
                    paths: vec![direct.clone()],
                },
            ],
            FabricPathMergeOptions::default(),
        );

        assert_eq!(merged.touched_shards, vec![primary, person]);
        assert_eq!(merged.input_paths, 3);
        assert_eq!(merged.output_paths, 2);
        assert_eq!(merged.paths[0], direct);
        assert_eq!(merged.paths[1], longer);
    }

    #[test]
    fn fabric_path_merge_uses_cost_and_pagination_after_length() {
        let primary = PlacementKey::new("default", "copper", "primary");
        let node_a = FabricGlobalId::new(primary.clone(), "node", "A");
        let node_b = FabricGlobalId::new(primary.clone(), "node", "B");
        let node_c = FabricGlobalId::new(primary.clone(), "node", "C");
        let edge_ab = FabricGlobalId::new(primary.clone(), "relationship", "AB");
        let edge_ac = FabricGlobalId::new(primary.clone(), "relationship", "AC");
        let mut expensive = FabricPath::new(vec![node_a.clone(), node_b], vec![edge_ab]);
        expensive.cost = Some(10.0);
        let mut cheap = FabricPath::new(vec![node_a, node_c], vec![edge_ac]);
        cheap.cost = Some(2.0);

        let merged = merge_fabric_paths(
            vec![FabricPathBatch {
                shard: primary,
                paths: vec![expensive.clone(), cheap.clone()],
            }],
            FabricPathMergeOptions {
                lowest_cost_first: true,
                skip: 0,
                limit: Some(1),
                ..Default::default()
            },
        );

        assert_eq!(merged.input_paths, 2);
        assert_eq!(merged.output_paths, 1);
        assert_eq!(merged.paths, vec![cheap]);
    }
}
