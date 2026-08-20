//! Full-text and semantic search for copperdb.
//!
//! Equivalent to Go's `pkg/search` in NornicDB.
//! Combines in-memory inverted-index full-text search with optional
//! vector similarity search via copperdb-vectorspace.

pub mod lucene;

use async_trait::async_trait;
use copperdb_util::{RequestCancelled, RequestContext};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, RwLock};
use thiserror::Error;

use copperdb_topology::{
    DistributedSearchPlan, FabricGlobalId, LogicalTransactionId, PlacementKey, SearchRoutingPolicy,
    TopologyError, TopologyRegistry,
};

#[derive(Debug, Error)]
pub enum SearchError {
    #[error("tantivy error: {0}")]
    Tantivy(String),
    #[error("index not ready")]
    IndexNotReady,
    #[error("document not found: {0}")]
    NotFound(String),
    #[error("topology error: {0}")]
    Topology(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error(transparent)]
    RequestCancelled(#[from] RequestCancelled),
}

/// A search result with relevance score.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchResult {
    pub id: String,
    pub score: f32,
    pub label: String,
    pub snippet: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RrfSearchHit {
    pub global_id: FabricGlobalId,
    pub rank: usize,
    pub score: f32,
    pub source: String,
    pub shard: PlacementKey,
    pub label: String,
    pub snippet: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RrfMergedHit {
    pub global_id: FabricGlobalId,
    pub rrf_score: f32,
    pub best_score: f32,
    pub vector_rank: usize,
    pub bm25_rank: usize,
    pub sources: Vec<String>,
    pub shard: PlacementKey,
    pub label: String,
    pub snippet: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RrfConfig {
    pub k: f32,
    pub limit: usize,
    #[serde(default)]
    pub min_score: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RrfSearchBatch {
    pub shard: PlacementKey,
    pub source: String,
    pub hits: Vec<RrfSearchHit>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RrfSearchOutcome {
    pub results: Vec<RrfMergedHit>,
    pub touched_shards: Vec<PlacementKey>,
    pub sources: Vec<String>,
    pub input_hits: usize,
    #[serde(default)]
    pub fused_hits: usize,
    pub output_hits: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RrfHydrationRecord {
    pub global_id: FabricGlobalId,
    pub labels: Vec<String>,
    pub entity: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RrfSearchPolicy {
    pub allowed_labels: Vec<String>,
    pub denied_labels: Vec<String>,
    pub denied_sources: Vec<String>,
    pub require_hydration: bool,
    pub redact_fields: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RrfHydratedHit {
    pub hit: RrfMergedHit,
    pub labels: Vec<String>,
    pub entity: Option<Value>,
    pub redacted_fields: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RrfHydratedSearchOutcome {
    pub results: Vec<RrfHydratedHit>,
    pub touched_shards: Vec<PlacementKey>,
    pub sources: Vec<String>,
    pub input_hits: usize,
    pub output_hits: usize,
    pub filtered_hits: usize,
    pub missing_hydration_hits: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FabricRankedSearchExecution {
    pub planned_shards: Vec<PlacementKey>,
    pub responded_shards: Vec<PlacementKey>,
    pub missing_shards: Vec<PlacementKey>,
    pub ignored_shards: Vec<PlacementKey>,
    pub responded_nodes: Vec<String>,
    pub failed_nodes: Vec<String>,
    pub merged: RrfSearchOutcome,
    pub hydrated: RrfHydratedSearchOutcome,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FabricRankedBatchCollection {
    pub responded_nodes: Vec<String>,
    pub failed_nodes: Vec<String>,
    pub batches: Vec<RrfSearchBatch>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FabricHydrationRequest {
    pub node_id: String,
    pub placement: PlacementKey,
    pub global_ids: Vec<FabricGlobalId>,
    pub read_fence: Option<LogicalTransactionId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FabricHydrationCollection {
    pub responded_nodes: Vec<String>,
    pub failed_nodes: Vec<String>,
    pub records: Vec<RrfHydrationRecord>,
    pub missing_global_ids: Vec<FabricGlobalId>,
}

impl RrfConfig {
    pub fn new(k: f32, limit: usize) -> Self {
        Self {
            k: k.max(1.0),
            limit,
            min_score: 0.0,
        }
    }

    pub fn with_min_score(mut self, min_score: f32) -> Self {
        self.min_score = min_score.max(0.0);
        self
    }
}

impl Default for RrfConfig {
    fn default() -> Self {
        Self {
            k: 60.0,
            limit: 10,
            min_score: 0.0,
        }
    }
}

pub fn merge_rrf_search_hits(
    ranked_hits: Vec<Vec<RrfSearchHit>>,
    config: RrfConfig,
) -> Vec<RrfMergedHit> {
    let mut out = merge_rrf_search_hits_before_limit(ranked_hits, config);
    out.truncate(config.limit);
    out
}

fn merge_rrf_search_hits_before_limit(
    ranked_hits: Vec<Vec<RrfSearchHit>>,
    config: RrfConfig,
) -> Vec<RrfMergedHit> {
    let input_hit_count = ranked_hits.iter().map(Vec::len).sum();
    let mut merged: HashMap<FabricGlobalId, RrfMergedHit> = HashMap::with_capacity(input_hit_count);
    for hit in ranked_hits.into_iter().flatten() {
        let contribution = 1.0 / (config.k + hit.rank.max(1) as f32);
        if let Some(entry) = merged.get_mut(&hit.global_id) {
            entry.rrf_score += contribution;
            if hit.score > entry.best_score {
                entry.best_score = hit.score;
                entry.label = hit.label;
                entry.snippet = hit.snippet;
            }
            match hit.source.as_str() {
                "lexical" => entry.bm25_rank = hit.rank,
                "semantic" | "vector" => entry.vector_rank = hit.rank,
                _ => {}
            }
            if !entry.sources.iter().any(|source| source == &hit.source) {
                entry.sources.push(hit.source);
                entry.sources.sort();
            }
        } else {
            let source = hit.source;
            let mut merged_hit = RrfMergedHit {
                global_id: hit.global_id.clone(),
                rrf_score: contribution,
                best_score: hit.score,
                vector_rank: 0,
                bm25_rank: 0,
                sources: vec![source.clone()],
                shard: hit.shard,
                label: hit.label,
                snippet: hit.snippet,
            };
            match source.as_str() {
                "lexical" => merged_hit.bm25_rank = hit.rank,
                "semantic" | "vector" => merged_hit.vector_rank = hit.rank,
                _ => {}
            }
            merged.insert(hit.global_id, merged_hit);
        }
    }

    let mut out = merged.into_values().collect::<Vec<_>>();
    out.retain(|hit| hit.rrf_score >= config.min_score);
    out.sort_by(|left, right| {
        right
            .rrf_score
            .total_cmp(&left.rrf_score)
            .then(right.best_score.total_cmp(&left.best_score))
            .then(left.global_id.stable_id().cmp(&right.global_id.stable_id()))
    });
    out
}

pub fn merge_rrf_search_batches(
    batches: Vec<RrfSearchBatch>,
    config: RrfConfig,
) -> RrfSearchOutcome {
    let mut touched_shards = Vec::new();
    let mut sources = BTreeSet::new();
    let mut input_hits = 0;
    let mut ranked_hits = Vec::with_capacity(batches.len());

    for batch in batches {
        if !touched_shards.iter().any(|shard| shard == &batch.shard) {
            touched_shards.push(batch.shard.clone());
        }
        if !batch.source.is_empty() {
            sources.insert(batch.source.clone());
        }
        for hit in &batch.hits {
            if !hit.source.is_empty() {
                sources.insert(hit.source.clone());
            }
        }
        input_hits += batch.hits.len();
        ranked_hits.push(batch.hits);
    }

    let mut results = merge_rrf_search_hits_before_limit(ranked_hits, config);
    let fused_hits = results.len();
    results.truncate(config.limit);
    let output_hits = results.len();
    RrfSearchOutcome {
        results,
        touched_shards,
        sources: sources.into_iter().collect(),
        input_hits,
        fused_hits,
        output_hits,
    }
}

pub fn hydrate_rrf_search_outcome(
    outcome: RrfSearchOutcome,
    hydration: Vec<RrfHydrationRecord>,
    policy: RrfSearchPolicy,
) -> RrfHydratedSearchOutcome {
    let hydration_by_id = hydration
        .into_iter()
        .map(|record| (record.global_id.stable_id(), record))
        .collect::<HashMap<_, _>>();
    let mut results = Vec::new();
    let mut filtered_hits = 0;
    let mut missing_hydration_hits = 0;

    for hit in outcome.results {
        let stable_id = hit.global_id.stable_id();
        let hydration = hydration_by_id.get(&stable_id);
        if hydration.is_none() && policy.require_hydration {
            missing_hydration_hits += 1;
            filtered_hits += 1;
            continue;
        }

        let labels = hydration
            .map(|record| record.labels.clone())
            .unwrap_or_else(|| vec![hit.label.clone()]);
        if labels_denied(&labels, &policy) || sources_denied(&hit.sources, &policy) {
            filtered_hits += 1;
            continue;
        }

        let mut redacted_fields = Vec::new();
        let entity =
            hydration.map(|record| redact_entity(&record.entity, &policy, &mut redacted_fields));
        results.push(RrfHydratedHit {
            hit,
            labels,
            entity,
            redacted_fields,
        });
    }

    let output_hits = results.len();
    RrfHydratedSearchOutcome {
        results,
        touched_shards: outcome.touched_shards,
        sources: outcome.sources,
        input_hits: outcome.input_hits,
        output_hits,
        filtered_hits,
        missing_hydration_hits,
    }
}

pub fn execute_planned_fabric_ranked_search(
    plans: Vec<DistributedSearchPlan>,
    batches: Vec<RrfSearchBatch>,
    hydration: Vec<RrfHydrationRecord>,
    config: RrfConfig,
    policy: RrfSearchPolicy,
) -> FabricRankedSearchExecution {
    let planned_shards = plans
        .into_iter()
        .map(|plan| plan.placement)
        .collect::<Vec<_>>();
    let mut responded_shards = Vec::new();
    let mut ignored_shards = Vec::new();
    let mut filtered_batches = Vec::new();

    for batch in batches {
        if planned_shards.iter().any(|shard| shard == &batch.shard) {
            if !responded_shards.iter().any(|shard| shard == &batch.shard) {
                responded_shards.push(batch.shard.clone());
            }
            filtered_batches.push(batch);
        } else if !ignored_shards.iter().any(|shard| shard == &batch.shard) {
            ignored_shards.push(batch.shard);
        }
    }

    let missing_shards = planned_shards
        .iter()
        .filter(|planned| !responded_shards.iter().any(|shard| shard == *planned))
        .cloned()
        .collect::<Vec<_>>();
    let merged = merge_rrf_search_batches(filtered_batches, config);
    let hydrated = hydrate_rrf_search_outcome(merged.clone(), hydration, policy);
    FabricRankedSearchExecution {
        planned_shards,
        responded_shards,
        missing_shards,
        ignored_shards,
        responded_nodes: Vec::new(),
        failed_nodes: Vec::new(),
        merged,
        hydrated,
    }
}

fn labels_denied(labels: &[String], policy: &RrfSearchPolicy) -> bool {
    if !policy.allowed_labels.is_empty()
        && !labels
            .iter()
            .any(|label| policy.allowed_labels.iter().any(|allowed| allowed == label))
    {
        return true;
    }
    labels
        .iter()
        .any(|label| policy.denied_labels.iter().any(|denied| denied == label))
}

fn sources_denied(sources: &[String], policy: &RrfSearchPolicy) -> bool {
    sources
        .iter()
        .any(|source| policy.denied_sources.iter().any(|denied| denied == source))
}

fn redact_entity(
    entity: &Value,
    policy: &RrfSearchPolicy,
    redacted_fields: &mut Vec<String>,
) -> Value {
    let mut entity = entity.clone();
    for field in &policy.redact_fields {
        if remove_path(&mut entity, field) {
            redacted_fields.push(field.clone());
        }
    }
    entity
}

fn remove_path(entity: &mut Value, path: &str) -> bool {
    let mut current = entity;
    let mut parts = path.split('.').peekable();
    while let Some(part) = parts.next() {
        if parts.peek().is_none() {
            return current
                .as_object_mut()
                .map(|object| object.remove(part).is_some())
                .unwrap_or(false);
        }
        let Some(next) = current
            .as_object_mut()
            .and_then(|object| object.get_mut(part))
        else {
            return false;
        };
        current = next;
    }
    false
}

pub fn merge_distributed_results(
    shard_results: Vec<Vec<SearchResult>>,
    limit: usize,
) -> Vec<SearchResult> {
    let mut best_by_id: HashMap<String, SearchResult> = HashMap::new();
    for result in shard_results.into_iter().flatten() {
        let replace = best_by_id
            .get(&result.id)
            .map(|existing| result.score > existing.score)
            .unwrap_or(true);
        if replace {
            best_by_id.insert(result.id.clone(), result);
        }
    }
    let mut merged = best_by_id.into_values().collect::<Vec<_>>();
    merged.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then(left.id.cmp(&right.id))
    });
    merged.truncate(limit);
    merged
}

/// Search query types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SearchQuery {
    /// BM25 full-text search.
    FullText {
        query: String,
        fields: Vec<String>,
        limit: usize,
    },
    /// Vector similarity search.
    Semantic {
        vector: Vec<f32>,
        k: usize,
        min_score: f32,
    },
    /// Hybrid: BM25 + semantic with RRF fusion.
    Hybrid {
        text: String,
        vector: Vec<f32>,
        k: usize,
    },
}

/// In-memory full-text search index using an inverted index.
///
/// Documents are stored as `id -> field -> text`. Words are tokenized
/// (lowercased, split on whitespace/punctuation) and stored in an
/// inverted index mapping `word -> set of doc IDs`.
pub struct SearchIndex {
    /// id -> field -> text
    documents: HashMap<String, HashMap<String, String>>,
    /// word -> set of doc IDs
    inverted: HashMap<String, HashSet<String>>,
    /// word -> field -> set of doc IDs (for field-scoped search)
    field_inverted: HashMap<String, HashMap<String, HashSet<String>>>,
}

/// Distributed search planning seam.
///
/// The search engine remains local today. This router makes the cluster fan-out
/// contract explicit so mesh execution can be added without changing query APIs.
#[derive(Debug, Clone, Default)]
pub struct DistributedSearchRouter {
    topology: TopologyRegistry,
}

#[async_trait]
pub trait SearchTransport: Send + Sync {
    async fn search_node(
        &self,
        node_id: &str,
        query: &SearchQuery,
    ) -> Result<Vec<SearchResult>, SearchError>;
}

#[async_trait]
pub trait RankedSearchTransport: Send + Sync {
    async fn search_ranked_node(
        &self,
        node_id: &str,
        placement: &PlacementKey,
        query: &SearchQuery,
        read_fence: Option<LogicalTransactionId>,
        request_context: Option<&RequestContext>,
    ) -> Result<RrfSearchBatch, SearchError>;
}

#[async_trait]
pub trait HydrationTransport: Send + Sync {
    async fn hydrate_node(
        &self,
        node_id: &str,
        placement: &PlacementKey,
        global_ids: &[FabricGlobalId],
        read_fence: Option<LogicalTransactionId>,
        request_context: Option<&RequestContext>,
    ) -> Result<Vec<RrfHydrationRecord>, SearchError>;
}

#[derive(Default)]
pub struct InMemorySearchTransport {
    node_results: RwLock<HashMap<String, Vec<SearchResult>>>,
    node_ranked_results: RwLock<HashMap<String, RrfSearchBatch>>,
    node_hydration_results: RwLock<HashMap<String, Vec<RrfHydrationRecord>>>,
}

impl InMemorySearchTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_results(&self, node_id: impl Into<String>, results: Vec<SearchResult>) {
        self.node_results
            .write()
            .unwrap()
            .insert(node_id.into(), results);
    }

    pub fn register_ranked_results(&self, node_id: impl Into<String>, batch: RrfSearchBatch) {
        self.node_ranked_results
            .write()
            .unwrap()
            .insert(node_id.into(), batch);
    }

    pub fn register_hydration_results(
        &self,
        node_id: impl Into<String>,
        records: Vec<RrfHydrationRecord>,
    ) {
        self.node_hydration_results
            .write()
            .unwrap()
            .insert(node_id.into(), records);
    }
}

#[async_trait]
impl SearchTransport for InMemorySearchTransport {
    async fn search_node(
        &self,
        node_id: &str,
        _query: &SearchQuery,
    ) -> Result<Vec<SearchResult>, SearchError> {
        self.node_results
            .read()
            .unwrap()
            .get(node_id)
            .cloned()
            .ok_or_else(|| SearchError::Transport(format!("unknown search node {node_id}")))
    }
}

#[async_trait]
impl RankedSearchTransport for InMemorySearchTransport {
    async fn search_ranked_node(
        &self,
        node_id: &str,
        _placement: &PlacementKey,
        _query: &SearchQuery,
        _read_fence: Option<LogicalTransactionId>,
        _request_context: Option<&RequestContext>,
    ) -> Result<RrfSearchBatch, SearchError> {
        self.node_ranked_results
            .read()
            .unwrap()
            .get(node_id)
            .cloned()
            .ok_or_else(|| SearchError::Transport(format!("unknown ranked search node {node_id}")))
    }
}

#[async_trait]
impl HydrationTransport for InMemorySearchTransport {
    async fn hydrate_node(
        &self,
        node_id: &str,
        _placement: &PlacementKey,
        global_ids: &[FabricGlobalId],
        _read_fence: Option<LogicalTransactionId>,
        _request_context: Option<&RequestContext>,
    ) -> Result<Vec<RrfHydrationRecord>, SearchError> {
        let requested = global_ids
            .iter()
            .map(FabricGlobalId::stable_id)
            .collect::<HashSet<_>>();
        let records = self
            .node_hydration_results
            .read()
            .unwrap()
            .get(node_id)
            .cloned()
            .ok_or_else(|| SearchError::Transport(format!("unknown hydration node {node_id}")))?;
        Ok(records
            .into_iter()
            .filter(|record| requested.contains(&record.global_id.stable_id()))
            .collect())
    }
}

#[derive(Debug, Clone)]
pub struct DistributedSearchOutcome {
    pub plan: DistributedSearchPlan,
    pub responded_by: Vec<String>,
    pub failed_nodes: Vec<String>,
    pub results: Vec<SearchResult>,
}

pub struct DistributedSearchExecutor {
    router: DistributedSearchRouter,
    transport: Arc<dyn SearchTransport>,
}

pub struct DistributedRankedSearchExecutor {
    transport: Arc<dyn RankedSearchTransport>,
}

impl DistributedSearchExecutor {
    pub fn new(router: DistributedSearchRouter, transport: Arc<dyn SearchTransport>) -> Self {
        Self { router, transport }
    }

    pub fn router(&self) -> &DistributedSearchRouter {
        &self.router
    }

    pub async fn search(
        &self,
        placement: &PlacementKey,
        query: SearchQuery,
        limit: usize,
    ) -> Result<DistributedSearchOutcome, SearchError> {
        let plan = self
            .router
            .plan(placement)
            .map_err(|error| SearchError::Topology(error.to_string()))?;
        self.execute_plan(plan, query, limit).await
    }

    pub async fn search_low_latency(
        &self,
        placement: &PlacementKey,
        query: SearchQuery,
        request_region: impl Into<String>,
        max_fanout: usize,
        limit: usize,
    ) -> Result<DistributedSearchOutcome, SearchError> {
        let plan = self
            .router
            .plan_low_latency(placement, request_region, max_fanout)
            .map_err(|error| SearchError::Topology(error.to_string()))?;
        self.execute_plan(plan, query, limit).await
    }

    async fn execute_plan(
        &self,
        plan: DistributedSearchPlan,
        query: SearchQuery,
        limit: usize,
    ) -> Result<DistributedSearchOutcome, SearchError> {
        let mut responded_by = Vec::new();
        let mut failed_nodes = Vec::new();
        let mut shard_results = Vec::new();
        for peer in &plan.fanout {
            match self.transport.search_node(&peer.node_id, &query).await {
                Ok(results) => {
                    responded_by.push(peer.node_id.clone());
                    shard_results.push(results);
                }
                Err(_) => failed_nodes.push(peer.node_id.clone()),
            }
        }
        if responded_by.is_empty() {
            return Err(SearchError::Transport(
                "distributed search returned no shard responses".into(),
            ));
        }
        let results = merge_distributed_results(shard_results, limit);
        Ok(DistributedSearchOutcome {
            plan,
            responded_by,
            failed_nodes,
            results,
        })
    }
}

impl DistributedRankedSearchExecutor {
    pub fn new(transport: Arc<dyn RankedSearchTransport>) -> Self {
        Self { transport }
    }

    pub async fn collect_planned(
        &self,
        plans: Vec<DistributedSearchPlan>,
        query: SearchQuery,
        read_fence: Option<LogicalTransactionId>,
    ) -> Result<FabricRankedBatchCollection, SearchError> {
        collect_planned_fabric_ranked_batches(plans, query, read_fence, self.transport.clone())
            .await
    }

    pub async fn collect_planned_with_context(
        &self,
        request_context: &RequestContext,
        plans: Vec<DistributedSearchPlan>,
        query: SearchQuery,
        read_fence: Option<LogicalTransactionId>,
    ) -> Result<FabricRankedBatchCollection, SearchError> {
        collect_planned_fabric_ranked_batches_with_context(
            request_context,
            plans,
            query,
            read_fence,
            self.transport.clone(),
        )
        .await
    }
}

pub async fn collect_planned_fabric_ranked_batches(
    plans: Vec<DistributedSearchPlan>,
    query: SearchQuery,
    read_fence: Option<LogicalTransactionId>,
    transport: Arc<dyn RankedSearchTransport>,
) -> Result<FabricRankedBatchCollection, SearchError> {
    let request_context = RequestContext::detached();
    collect_planned_fabric_ranked_batches_with_context(
        &request_context,
        plans,
        query,
        read_fence,
        transport,
    )
    .await
}

pub async fn collect_planned_fabric_ranked_batches_with_context(
    request_context: &RequestContext,
    plans: Vec<DistributedSearchPlan>,
    query: SearchQuery,
    read_fence: Option<LogicalTransactionId>,
    transport: Arc<dyn RankedSearchTransport>,
) -> Result<FabricRankedBatchCollection, SearchError> {
    let mut responded_nodes = Vec::new();
    let mut failed_nodes = Vec::new();
    let mut batches = Vec::new();

    for plan in plans {
        request_context.check_active()?;
        for peer in &plan.fanout {
            request_context.check_active()?;
            match transport
                .search_ranked_node(
                    &peer.node_id,
                    &plan.placement,
                    &query,
                    read_fence,
                    Some(request_context),
                )
                .await
            {
                Ok(batch) => {
                    responded_nodes.push(peer.node_id.clone());
                    batches.push(batch);
                }
                Err(_) => failed_nodes.push(peer.node_id.clone()),
            }
        }
    }

    if batches.is_empty() {
        return Err(SearchError::Transport(
            "distributed ranked search returned no shard responses".into(),
        ));
    }

    Ok(FabricRankedBatchCollection {
        responded_nodes,
        failed_nodes,
        batches,
    })
}

pub async fn collect_fabric_hydration_records(
    requests: Vec<FabricHydrationRequest>,
    transport: Arc<dyn HydrationTransport>,
) -> Result<FabricHydrationCollection, SearchError> {
    let request_context = RequestContext::detached();
    collect_fabric_hydration_records_with_context(&request_context, requests, transport).await
}

pub async fn collect_fabric_hydration_records_with_context(
    request_context: &RequestContext,
    requests: Vec<FabricHydrationRequest>,
    transport: Arc<dyn HydrationTransport>,
) -> Result<FabricHydrationCollection, SearchError> {
    let mut responded_nodes = Vec::new();
    let mut failed_nodes = Vec::new();
    let mut records = Vec::new();
    let mut requested_ids = Vec::new();

    for request in requests {
        request_context.check_active()?;
        requested_ids.extend(request.global_ids.iter().cloned());
        match transport
            .hydrate_node(
                &request.node_id,
                &request.placement,
                &request.global_ids,
                request.read_fence,
                Some(request_context),
            )
            .await
        {
            Ok(mut node_records) => {
                responded_nodes.push(request.node_id);
                records.append(&mut node_records);
            }
            Err(_) => failed_nodes.push(request.node_id),
        }
    }

    let returned = records
        .iter()
        .map(|record| record.global_id.stable_id())
        .collect::<HashSet<_>>();
    let mut missing_global_ids = Vec::new();
    for global_id in requested_ids {
        if !returned.contains(&global_id.stable_id())
            && !missing_global_ids
                .iter()
                .any(|candidate: &FabricGlobalId| candidate == &global_id)
        {
            missing_global_ids.push(global_id);
        }
    }

    if records.is_empty() && responded_nodes.is_empty() {
        return Err(SearchError::Transport(
            "distributed hydration returned no shard responses".into(),
        ));
    }

    Ok(FabricHydrationCollection {
        responded_nodes,
        failed_nodes,
        records,
        missing_global_ids,
    })
}

impl DistributedSearchRouter {
    pub fn new(topology: TopologyRegistry) -> Self {
        Self { topology }
    }

    pub fn topology(&self) -> &TopologyRegistry {
        &self.topology
    }

    pub fn plan(&self, placement: &PlacementKey) -> Result<DistributedSearchPlan, TopologyError> {
        self.topology.plan_search(placement)
    }

    pub fn plan_low_latency(
        &self,
        placement: &PlacementKey,
        request_region: impl Into<String>,
        max_fanout: usize,
    ) -> Result<DistributedSearchPlan, TopologyError> {
        self.topology.plan_search_with_policy(
            placement,
            SearchRoutingPolicy::low_latency(request_region, max_fanout),
        )
    }
}

impl Default for SearchIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl SearchIndex {
    pub fn new() -> Self {
        Self {
            documents: HashMap::new(),
            inverted: HashMap::new(),
            field_inverted: HashMap::new(),
        }
    }

    /// Tokenize text into lowercase words (split on non-alphanumeric chars).
    fn tokenize(text: &str) -> Vec<String> {
        text.split(|c: char| !c.is_alphanumeric())
            .filter(|w| !w.is_empty())
            .map(|w| w.to_lowercase())
            .collect()
    }

    /// Index a document. Re-indexing the same id replaces the previous document.
    pub fn index_document(&mut self, id: &str, fields: HashMap<String, String>) {
        // Remove old index entries if re-indexing
        self.remove_document(id);

        for (field, text) in &fields {
            for word in Self::tokenize(text) {
                self.inverted
                    .entry(word.clone())
                    .or_default()
                    .insert(id.to_string());
                self.field_inverted
                    .entry(word)
                    .or_default()
                    .entry(field.clone())
                    .or_default()
                    .insert(id.to_string());
            }
        }
        self.documents.insert(id.to_string(), fields);
    }

    /// Remove a document and its index entries.
    pub fn remove_document(&mut self, id: &str) {
        if let Some(fields) = self.documents.remove(id) {
            for (_field, text) in &fields {
                for word in Self::tokenize(text) {
                    // Prune empty inverted-index entries (avoids unbounded memory
                    // growth when documents are frequently added/removed).
                    let remove_inverted = if let Some(set) = self.inverted.get_mut(&word) {
                        set.remove(id);
                        set.is_empty()
                    } else {
                        false
                    };
                    if remove_inverted {
                        self.inverted.remove(&word);
                    }

                    let remove_field_word =
                        if let Some(field_map) = self.field_inverted.get_mut(&word) {
                            let remove_field = if let Some(set) = field_map.get_mut(_field) {
                                set.remove(id);
                                set.is_empty()
                            } else {
                                false
                            };
                            if remove_field {
                                field_map.remove(_field);
                            }
                            field_map.is_empty()
                        } else {
                            false
                        };
                    if remove_field_word {
                        self.field_inverted.remove(&word);
                    }
                }
            }
        }
    }

    /// Search across all fields. Returns doc IDs sorted by match count (descending).
    pub fn search(&self, query: &str) -> Vec<String> {
        let tokens = Self::tokenize(query);
        if tokens.is_empty() {
            return vec![];
        }
        let mut scores: HashMap<String, usize> = HashMap::new();
        for token in &tokens {
            if let Some(ids) = self.inverted.get(token) {
                for id in ids {
                    *scores.entry(id.clone()).or_default() += 1;
                }
            }
        }
        let mut results: Vec<(String, usize)> = scores.into_iter().collect();
        results.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        results.into_iter().map(|(id, _)| id).collect()
    }

    /// Search within a specific field. Returns doc IDs sorted by match count.
    pub fn search_field(&self, field: &str, query: &str) -> Vec<String> {
        let tokens = Self::tokenize(query);
        if tokens.is_empty() {
            return vec![];
        }
        let mut scores: HashMap<String, usize> = HashMap::new();
        for token in &tokens {
            if let Some(field_map) = self.field_inverted.get(token) {
                if let Some(ids) = field_map.get(field) {
                    for id in ids {
                        *scores.entry(id.clone()).or_default() += 1;
                    }
                }
            }
        }
        let mut results: Vec<(String, usize)> = scores.into_iter().collect();
        results.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        results.into_iter().map(|(id, _)| id).collect()
    }

    pub fn document_count(&self) -> usize {
        self.documents.len()
    }

    pub fn get_document(&self, id: &str) -> Option<&HashMap<String, String>> {
        self.documents.get(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_doc(text: &str) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("body".into(), text.into());
        m
    }

    #[test]
    fn test_search_query_construction() {
        let q = SearchQuery::FullText {
            query: "Alice".to_string(),
            fields: vec!["name".to_string()],
            limit: 10,
        };
        assert!(matches!(q, SearchQuery::FullText { .. }));
    }

    #[test]
    fn test_index_and_search() {
        let mut idx = SearchIndex::new();
        idx.index_document("1", make_doc("hello world"));
        idx.index_document("2", make_doc("goodbye world"));
        let results = idx.search("hello");
        assert_eq!(results, vec!["1"]);
    }

    #[test]
    fn test_search_multi_token() {
        let mut idx = SearchIndex::new();
        idx.index_document("1", make_doc("rust is fast and safe"));
        idx.index_document("2", make_doc("go is fast"));
        // "fast" matches both; "rust" only matches 1 — doc 1 should rank higher
        let results = idx.search("rust fast");
        assert_eq!(results[0], "1");
    }

    #[test]
    fn test_search_field() {
        let mut idx = SearchIndex::new();
        let mut d1 = HashMap::new();
        d1.insert("name".into(), "Alice Smith".into());
        d1.insert("bio".into(), "engineer at company".into());
        idx.index_document("1", d1);
        let results = idx.search_field("name", "alice");
        assert_eq!(results, vec!["1"]);
        let results2 = idx.search_field("bio", "alice");
        assert!(results2.is_empty());
    }

    #[test]
    fn test_remove_document() {
        let mut idx = SearchIndex::new();
        idx.index_document("1", make_doc("hello world"));
        idx.remove_document("1");
        assert_eq!(idx.document_count(), 0);
        assert!(idx.search("hello").is_empty());
    }

    #[test]
    fn test_reindex_document() {
        let mut idx = SearchIndex::new();
        idx.index_document("1", make_doc("old content here"));
        idx.index_document("1", make_doc("new content there"));
        assert_eq!(idx.document_count(), 1);
        let results = idx.search("old");
        assert!(results.is_empty());
        let results2 = idx.search("new");
        assert_eq!(results2, vec!["1"]);
    }

    #[test]
    fn test_empty_search() {
        let idx = SearchIndex::new();
        assert!(idx.search("hello").is_empty());
    }

    #[test]
    fn test_document_count() {
        let mut idx = SearchIndex::new();
        assert_eq!(idx.document_count(), 0);
        idx.index_document("1", make_doc("a"));
        idx.index_document("2", make_doc("b"));
        assert_eq!(idx.document_count(), 2);
    }

    #[test]
    fn test_tokenize_punctuation() {
        let tokens = SearchIndex::tokenize("Hello, world! It's great.");
        assert!(tokens.contains(&"hello".to_string()));
        assert!(tokens.contains(&"world".to_string()));
    }

    #[test]
    fn distributed_router_plans_mesh_fanout() {
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementRecord};

        let mut topology = TopologyRegistry::new();
        topology
            .register_peer(
                MeshPeer::new("search-a", "search-a.mesh.local:9000")
                    .with_capability(NodeCapability::Search)
                    .with_capability(NodeCapability::WriteLeader),
            )
            .unwrap();
        topology
            .register_placement(PlacementRecord::standalone("copper", "search-a"))
            .unwrap();

        let router = DistributedSearchRouter::new(topology);
        let plan = router
            .plan(&PlacementKey::default_for_database("copper"))
            .unwrap();
        assert_eq!(plan.fanout[0].node_id, "search-a");
    }

    #[test]
    fn distributed_router_prefers_low_latency_local_peers() {
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let mut topology = TopologyRegistry::new();
        topology
            .register_peer(
                MeshPeer::new("search-remote", "search-remote.mesh.local:9000")
                    .with_capability(NodeCapability::Search)
                    .with_region_zone("eu-west-1", "eu-west-1a")
                    .with_observed_rtt_micros(500)
                    .with_load(0, 8),
            )
            .unwrap();
        topology
            .register_peer(
                MeshPeer::new("search-local", "search-local.mesh.local:9000")
                    .with_capability(NodeCapability::Search)
                    .with_region_zone("us-east-1", "us-east-1a")
                    .with_observed_rtt_micros(1_500)
                    .with_load(0, 8),
            )
            .unwrap();
        topology
            .register_placement(PlacementRecord {
                key: PlacementKey::default_for_database("copper"),
                primary_node: "search-local".into(),
                replica_nodes: vec![],
                search_nodes: vec!["search-remote".into(), "search-local".into()],
                hyperscaler_profile: None,
                min_write_replicas: 0,
                search_fanout: 2,
            })
            .unwrap();

        let router = DistributedSearchRouter::new(topology);
        let plan = router
            .plan_low_latency(
                &PlacementKey::default_for_database("copper"),
                "us-east-1",
                2,
            )
            .unwrap();
        assert_eq!(plan.parallelism, 2);
        assert_eq!(plan.fanout[0].node_id, "search-local");
    }

    #[test]
    fn distributed_result_merge_is_score_ordered_and_stable() {
        let merged = merge_distributed_results(
            vec![
                vec![
                    SearchResult {
                        id: "b".into(),
                        score: 0.9,
                        label: "Node".into(),
                        snippet: None,
                    },
                    SearchResult {
                        id: "a".into(),
                        score: 0.7,
                        label: "Node".into(),
                        snippet: None,
                    },
                ],
                vec![
                    SearchResult {
                        id: "a".into(),
                        score: 0.95,
                        label: "Node".into(),
                        snippet: Some("fresh".into()),
                    },
                    SearchResult {
                        id: "c".into(),
                        score: 0.9,
                        label: "Node".into(),
                        snippet: None,
                    },
                ],
            ],
            3,
        );

        assert_eq!(merged[0].id, "a");
        assert_eq!(merged[0].snippet, Some("fresh".into()));
        assert_eq!(merged[1].id, "b");
        assert_eq!(merged[2].id, "c");
    }

    #[test]
    fn rrf_merge_fuses_ranked_hits_across_sources_and_shards() {
        let primary = PlacementKey::new("default", "copper", "primary");
        let vector = PlacementKey::new("default", "copper", "vector-00");
        let doc_a = FabricGlobalId::new(primary.clone(), "node", "Person:1");
        let doc_b = FabricGlobalId::new(vector.clone(), "node", "Memory:7");
        let doc_c = FabricGlobalId::new(primary.clone(), "node", "Person:2");

        let merged = merge_rrf_search_hits(
            vec![
                vec![
                    RrfSearchHit {
                        global_id: doc_a.clone(),
                        rank: 1,
                        score: 0.70,
                        source: "lexical".into(),
                        shard: primary.clone(),
                        label: "Person".into(),
                        snippet: Some("lexical a".into()),
                    },
                    RrfSearchHit {
                        global_id: doc_b.clone(),
                        rank: 2,
                        score: 0.95,
                        source: "lexical".into(),
                        shard: vector.clone(),
                        label: "Memory".into(),
                        snippet: Some("lexical b".into()),
                    },
                ],
                vec![
                    RrfSearchHit {
                        global_id: doc_b,
                        rank: 1,
                        score: 0.88,
                        source: "vector".into(),
                        shard: vector.clone(),
                        label: "Memory".into(),
                        snippet: Some("vector b".into()),
                    },
                    RrfSearchHit {
                        global_id: doc_a,
                        rank: 2,
                        score: 0.99,
                        source: "vector".into(),
                        shard: primary,
                        label: "Person".into(),
                        snippet: Some("vector a".into()),
                    },
                    RrfSearchHit {
                        global_id: doc_c,
                        rank: 3,
                        score: 0.80,
                        source: "vector".into(),
                        shard: PlacementKey::new("default", "copper", "primary"),
                        label: "Person".into(),
                        snippet: None,
                    },
                ],
            ],
            RrfConfig::new(60.0, 2),
        );

        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].global_id.local_id, "Person:1");
        assert_eq!(merged[0].best_score, 0.99);
        assert_eq!(merged[0].snippet, Some("vector a".into()));
        assert_eq!(merged[0].sources, vec!["lexical", "vector"]);
        assert_eq!(merged[0].vector_rank, 2);
        assert_eq!(merged[0].bm25_rank, 1);
        assert_eq!(merged[1].global_id.local_id, "Memory:7");
        assert_eq!(merged[1].sources, vec!["lexical", "vector"]);
        assert_eq!(merged[1].vector_rank, 1);
        assert_eq!(merged[1].bm25_rank, 2);
    }

    #[test]
    fn rrf_batch_merge_tracks_sources_shards_and_counts() {
        let primary = PlacementKey::new("default", "copper", "primary");
        let vector = PlacementKey::new("default", "copper", "vector-00");
        let doc_a = FabricGlobalId::new(primary.clone(), "node", "Person:1");
        let doc_b = FabricGlobalId::new(vector.clone(), "node", "Memory:7");

        let outcome = merge_rrf_search_batches(
            vec![
                RrfSearchBatch {
                    shard: primary.clone(),
                    source: "lexical".into(),
                    hits: vec![RrfSearchHit {
                        global_id: doc_a.clone(),
                        rank: 1,
                        score: 0.7,
                        source: "lexical".into(),
                        shard: primary.clone(),
                        label: "Person".into(),
                        snippet: None,
                    }],
                },
                RrfSearchBatch {
                    shard: vector.clone(),
                    source: "vector".into(),
                    hits: vec![
                        RrfSearchHit {
                            global_id: doc_a,
                            rank: 1,
                            score: 0.9,
                            source: "vector".into(),
                            shard: primary.clone(),
                            label: "Person".into(),
                            snippet: Some("fresh".into()),
                        },
                        RrfSearchHit {
                            global_id: doc_b,
                            rank: 2,
                            score: 0.8,
                            source: "vector".into(),
                            shard: vector.clone(),
                            label: "Memory".into(),
                            snippet: None,
                        },
                    ],
                },
            ],
            RrfConfig::new(60.0, 10),
        );

        assert_eq!(outcome.touched_shards, vec![primary, vector]);
        assert_eq!(outcome.sources, vec!["lexical", "vector"]);
        assert_eq!(outcome.input_hits, 3);
        assert_eq!(outcome.output_hits, 2);
        assert_eq!(outcome.results[0].global_id.local_id, "Person:1");
        assert_eq!(outcome.results[0].best_score, 0.9);
        assert_eq!(outcome.results[0].snippet, Some("fresh".into()));
    }

    #[test]
    fn rrf_merge_applies_the_configured_minimum_score() {
        let shard = PlacementKey::new("default", "copper", "primary");
        let hits = vec![RrfSearchHit {
            global_id: FabricGlobalId::new(shard.clone(), "node", "low-score"),
            rank: 100,
            score: 0.9,
            source: "vector".into(),
            shard,
            label: "Document".into(),
            snippet: None,
        }];

        let merged =
            merge_rrf_search_hits(vec![hits], RrfConfig::new(60.0, 10).with_min_score(0.01));

        assert!(merged.is_empty());
    }

    #[test]
    fn rrf_merge_matches_nornicdb_fifty_result_fixture_threshold() {
        let shard = PlacementKey::new("default", "copper", "primary");
        let fixture_id = |position: usize, offset: usize| {
            let prefix = char::from_u32('a' as u32 + ((position + offset) % 26) as u32)
                .unwrap();
            let suffix = char::from_u32(position as u32).unwrap();
            format!("{prefix}{suffix}")
        };
        let semantic = (0..50)
            .map(|position| RrfSearchHit {
                global_id: FabricGlobalId::new(shard.clone(), "node", fixture_id(position, 0)),
                rank: position + 1,
                score: (50 - position) as f32 / 50.0,
                source: "semantic".into(),
                shard: shard.clone(),
                label: "Node".into(),
                snippet: None,
            })
            .collect::<Vec<_>>();
        let lexical = (0..50)
            .map(|position| RrfSearchHit {
                global_id: FabricGlobalId::new(shard.clone(), "node", fixture_id(position, 10)),
                rank: position + 1,
                score: (50 - position) as f32,
                source: "lexical".into(),
                shard: shard.clone(),
                label: "Node".into(),
                snippet: None,
            })
            .collect::<Vec<_>>();

        let merged = merge_rrf_search_hits(
            vec![semantic, lexical],
            RrfConfig::new(60.0, usize::MAX).with_min_score(0.01),
        );

        assert_eq!(merged.len(), 80);
        assert!(merged.iter().all(|hit| hit.rrf_score >= 0.01));
        let semantic_first = merged
            .iter()
            .find(|hit| hit.global_id.local_id == fixture_id(0, 0))
            .expect("semantic rank-one fixture hit must remain present");
        assert_eq!(semantic_first.rrf_score, 1.0 / 61.0);
        assert_eq!(semantic_first.vector_rank, 1);
        assert_eq!(semantic_first.bm25_rank, 0);
    }

    #[test]
    fn rrf_hydration_filters_and_redacts_ranked_results() {
        let primary = PlacementKey::new("default", "copper", "primary");
        let doc_a = FabricGlobalId::new(primary.clone(), "node", "Person:1");
        let doc_b = FabricGlobalId::new(primary.clone(), "node", "Secret:2");
        let outcome = RrfSearchOutcome {
            results: vec![
                RrfMergedHit {
                    global_id: doc_a.clone(),
                    rrf_score: 0.032,
                    best_score: 0.9,
                    vector_rank: 2,
                    bm25_rank: 1,
                    sources: vec!["lexical".into(), "vector".into()],
                    shard: primary.clone(),
                    label: "Person".into(),
                    snippet: Some("Alice".into()),
                },
                RrfMergedHit {
                    global_id: doc_b,
                    rrf_score: 0.016,
                    best_score: 0.4,
                    vector_rank: 0,
                    bm25_rank: 1,
                    sources: vec!["lexical".into()],
                    shard: primary.clone(),
                    label: "Secret".into(),
                    snippet: None,
                },
            ],
            touched_shards: vec![primary.clone()],
            sources: vec!["lexical".into(), "vector".into()],
            input_hits: 3,
            fused_hits: 2,
            output_hits: 2,
        };

        let hydrated = hydrate_rrf_search_outcome(
            outcome,
            vec![RrfHydrationRecord {
                global_id: doc_a,
                labels: vec!["Person".into()],
                entity: serde_json::json!({
                    "id": "Person:1",
                    "name": "Alice",
                    "secret": "internal",
                    "profile": {"ssn": "000-00-0000", "city": "Boston"}
                }),
            }],
            RrfSearchPolicy {
                allowed_labels: vec!["Person".into()],
                denied_labels: vec!["Secret".into()],
                denied_sources: Vec::new(),
                require_hydration: true,
                redact_fields: vec!["secret".into(), "profile.ssn".into()],
            },
        );

        assert_eq!(hydrated.input_hits, 3);
        assert_eq!(hydrated.output_hits, 1);
        assert_eq!(hydrated.filtered_hits, 1);
        assert_eq!(hydrated.missing_hydration_hits, 1);
        assert_eq!(hydrated.results[0].labels, vec!["Person"]);
        assert_eq!(
            hydrated.results[0].redacted_fields,
            vec!["secret", "profile.ssn"]
        );
        let entity = hydrated.results[0].entity.as_ref().unwrap();
        assert_eq!(entity["name"], "Alice");
        assert!(entity.get("secret").is_none());
        assert!(entity["profile"].get("ssn").is_none());
        assert_eq!(entity["profile"]["city"], "Boston");
    }

    #[test]
    fn planned_fabric_ranked_search_tracks_missing_and_ignored_shards() {
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementRecord};

        let primary = PlacementKey::new("default", "copper", "primary");
        let person = PlacementKey::new("default", "copper", "person-00");
        let ignored = PlacementKey::new("default", "copper", "rogue-00");
        let doc_a = FabricGlobalId::new(primary.clone(), "node", "Person:1");
        let mut topology = TopologyRegistry::new();
        for node_id in ["search-a", "search-b"] {
            topology
                .register_peer(
                    MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Search),
                )
                .unwrap();
        }
        for (placement, node_id) in [(primary.clone(), "search-a"), (person.clone(), "search-b")] {
            topology
                .register_placement(PlacementRecord {
                    key: placement,
                    primary_node: node_id.into(),
                    replica_nodes: vec![],
                    search_nodes: vec![node_id.into()],
                    hyperscaler_profile: None,
                    min_write_replicas: 0,
                    search_fanout: 1,
                })
                .unwrap();
        }

        let router = DistributedSearchRouter::new(topology);
        let execution = execute_planned_fabric_ranked_search(
            vec![
                router.plan(&primary).unwrap(),
                router.plan(&person).unwrap(),
            ],
            vec![
                RrfSearchBatch {
                    shard: primary.clone(),
                    source: "lexical".into(),
                    hits: vec![RrfSearchHit {
                        global_id: doc_a.clone(),
                        rank: 1,
                        score: 0.7,
                        source: "lexical".into(),
                        shard: primary.clone(),
                        label: "Person".into(),
                        snippet: None,
                    }],
                },
                RrfSearchBatch {
                    shard: ignored.clone(),
                    source: "rogue".into(),
                    hits: vec![RrfSearchHit {
                        global_id: doc_a.clone(),
                        rank: 1,
                        score: 1.0,
                        source: "rogue".into(),
                        shard: ignored.clone(),
                        label: "Person".into(),
                        snippet: Some("ignored".into()),
                    }],
                },
            ],
            vec![RrfHydrationRecord {
                global_id: doc_a,
                labels: vec!["Person".into()],
                entity: serde_json::json!({"id": "Person:1", "name": "Alice"}),
            }],
            RrfConfig::new(60.0, 10),
            RrfSearchPolicy {
                allowed_labels: vec!["Person".into()],
                denied_labels: Vec::new(),
                denied_sources: Vec::new(),
                require_hydration: true,
                redact_fields: Vec::new(),
            },
        );

        assert_eq!(
            execution.planned_shards,
            vec![primary.clone(), person.clone()]
        );
        assert_eq!(execution.responded_shards, vec![primary.clone()]);
        assert_eq!(execution.missing_shards, vec![person]);
        assert_eq!(execution.ignored_shards, vec![ignored]);
        assert!(execution.responded_nodes.is_empty());
        assert!(execution.failed_nodes.is_empty());
        assert_eq!(execution.merged.output_hits, 1);
        assert_eq!(execution.hydrated.output_hits, 1);
        assert_eq!(
            execution.hydrated.results[0].entity.as_ref().unwrap()["name"],
            "Alice"
        );
    }

    #[tokio::test]
    async fn ranked_search_transport_collects_planned_batches() {
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementRecord};

        let primary = PlacementKey::new("default", "copper", "primary");
        let person = PlacementKey::new("default", "copper", "person-00");
        let doc_a = FabricGlobalId::new(primary.clone(), "node", "Person:1");
        let mut topology = TopologyRegistry::new();
        for node_id in ["search-a", "search-b", "search-c"] {
            topology
                .register_peer(
                    MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Search),
                )
                .unwrap();
        }
        topology
            .register_placement(PlacementRecord {
                key: primary.clone(),
                primary_node: "search-a".into(),
                replica_nodes: vec![],
                search_nodes: vec!["search-a".into(), "search-c".into()],
                hyperscaler_profile: None,
                min_write_replicas: 0,
                search_fanout: 2,
            })
            .unwrap();
        topology
            .register_placement(PlacementRecord {
                key: person.clone(),
                primary_node: "search-b".into(),
                replica_nodes: vec![],
                search_nodes: vec!["search-b".into()],
                hyperscaler_profile: None,
                min_write_replicas: 0,
                search_fanout: 1,
            })
            .unwrap();

        let transport = Arc::new(InMemorySearchTransport::new());
        transport.register_ranked_results(
            "search-a",
            RrfSearchBatch {
                shard: primary.clone(),
                source: "lexical".into(),
                hits: vec![RrfSearchHit {
                    global_id: doc_a.clone(),
                    rank: 1,
                    score: 0.8,
                    source: "lexical".into(),
                    shard: primary.clone(),
                    label: "Person".into(),
                    snippet: None,
                }],
            },
        );
        transport.register_ranked_results(
            "search-b",
            RrfSearchBatch {
                shard: person.clone(),
                source: "vector".into(),
                hits: vec![RrfSearchHit {
                    global_id: doc_a,
                    rank: 1,
                    score: 0.9,
                    source: "vector".into(),
                    shard: person.clone(),
                    label: "Person".into(),
                    snippet: Some("fresh".into()),
                }],
            },
        );

        let router = DistributedSearchRouter::new(topology);
        let collector = DistributedRankedSearchExecutor::new(transport);
        let collected = collector
            .collect_planned(
                vec![
                    router.plan(&primary).unwrap(),
                    router.plan(&person).unwrap(),
                ],
                SearchQuery::FullText {
                    query: "alice".into(),
                    fields: vec!["body".into()],
                    limit: 10,
                },
                None,
            )
            .await
            .unwrap();

        assert_eq!(collected.responded_nodes, vec!["search-a", "search-b"]);
        assert_eq!(collected.failed_nodes, vec!["search-c"]);
        assert_eq!(collected.batches.len(), 2);
        assert_eq!(collected.batches[0].shard, primary);
        assert_eq!(collected.batches[1].shard, person);
    }

    #[tokio::test]
    async fn hydration_transport_collects_records_and_tracks_missing_ids() {
        let primary = PlacementKey::new("default", "copper", "primary");
        let person = PlacementKey::new("default", "copper", "person-00");
        let doc_a = FabricGlobalId::new(primary.clone(), "node", "Person:1");
        let doc_b = FabricGlobalId::new(person.clone(), "node", "Person:2");
        let transport = Arc::new(InMemorySearchTransport::new());
        transport.register_hydration_results(
            "node-a",
            vec![RrfHydrationRecord {
                global_id: doc_a.clone(),
                labels: vec!["Person".into()],
                entity: serde_json::json!({"id": "Person:1", "name": "Alice"}),
            }],
        );

        let collected = collect_fabric_hydration_records(
            vec![
                FabricHydrationRequest {
                    node_id: "node-a".into(),
                    placement: primary,
                    global_ids: vec![doc_a.clone()],
                    read_fence: None,
                },
                FabricHydrationRequest {
                    node_id: "node-b".into(),
                    placement: person,
                    global_ids: vec![doc_b.clone()],
                    read_fence: None,
                },
            ],
            transport,
        )
        .await
        .unwrap();

        assert_eq!(collected.responded_nodes, vec!["node-a"]);
        assert_eq!(collected.failed_nodes, vec!["node-b"]);
        assert_eq!(collected.records.len(), 1);
        assert_eq!(collected.records[0].global_id, doc_a);
        assert_eq!(collected.missing_global_ids, vec![doc_b]);
    }

    #[tokio::test]
    async fn distributed_executor_fans_out_and_merges_results() {
        use copperdb_topology::{MeshPeer, NodeCapability, PlacementKey, PlacementRecord};

        let placement = PlacementKey::default_for_database("copper");
        let mut topology = TopologyRegistry::new();
        for node_id in ["search-a", "search-b", "search-c"] {
            topology
                .register_peer(
                    MeshPeer::new(node_id, format!("{node_id}.mesh.local:9000"))
                        .with_capability(NodeCapability::Search),
                )
                .unwrap();
        }
        topology
            .register_placement(PlacementRecord {
                key: placement.clone(),
                primary_node: "search-a".into(),
                replica_nodes: vec![],
                search_nodes: vec!["search-a".into(), "search-b".into(), "search-c".into()],
                hyperscaler_profile: None,
                min_write_replicas: 0,
                search_fanout: 3,
            })
            .unwrap();

        let transport = Arc::new(InMemorySearchTransport::new());
        transport.register_results(
            "search-a",
            vec![SearchResult {
                id: "node-1".into(),
                score: 0.82,
                label: "Node".into(),
                snippet: None,
            }],
        );
        transport.register_results(
            "search-b",
            vec![SearchResult {
                id: "node-2".into(),
                score: 0.91,
                label: "Node".into(),
                snippet: None,
            }],
        );

        let executor =
            DistributedSearchExecutor::new(DistributedSearchRouter::new(topology), transport);
        let outcome = executor
            .search(
                &placement,
                SearchQuery::FullText {
                    query: "graph".into(),
                    fields: vec!["body".into()],
                    limit: 10,
                },
                10,
            )
            .await
            .unwrap();

        assert_eq!(outcome.responded_by, vec!["search-a", "search-b"]);
        assert_eq!(outcome.failed_nodes, vec!["search-c"]);
        assert_eq!(outcome.results[0].id, "node-2");
        assert_eq!(outcome.results[1].id, "node-1");
    }
}
