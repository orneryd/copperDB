//! Vector space registry for copperdb.
//!
//! Equivalent to Go's `pkg/vectorspace` in NornicDB.
//! Manages named embedding spaces (collections of high-dimensional vectors)
//! and supports explicit exact cosine similarity search.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum VectorSpaceError {
    #[error("space not found: {0}")]
    SpaceNotFound(String),
    #[error("dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },
    #[error("vector not found: {0}")]
    VectorNotFound(String),
    #[error("invalid HNSW configuration: {0}")]
    InvalidHnswConfiguration(&'static str),
    #[error("vector already exists: {0}")]
    DuplicateVector(String),
    #[error("vector index not found: {0}")]
    IndexNotFound(String),
    #[error("vector index already exists: {0}")]
    IndexAlreadyExists(String),
}

/// A named vector space (collection of embeddings).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSpace {
    pub name: String,
    pub dimensions: usize,
    entries: HashMap<String, Vec<f32>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SimilarityMetric {
    ExactCosine,
    ExactEuclidean,
    HnswCosine,
}

/// Immutable construction and query limits for an in-memory HNSW index.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct HnswConfig {
    pub m: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            m: 16,
            ef_construction: 200,
            ef_search: 64,
        }
    }
}

/// Evidence that a query traversed the graph rather than score-scanning records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswSearchStats {
    pub visited_nodes: usize,
}

/// A deterministic, in-memory hierarchical navigable small-world graph.
///
/// This is intentionally separate from [`VectorSpace`]'s exact fallback. It
/// owns its graph from construction onwards, and query reads only traverse the
/// existing graph; they do not build, warm, or switch strategy.
#[derive(Debug, Clone)]
pub struct HnswIndex {
    dimensions: usize,
    config: HnswConfig,
    entries: BTreeMap<String, Vec<f32>>,
    tombstones: BTreeSet<String>,
    levels: BTreeMap<String, usize>,
    neighbors: BTreeMap<(String, usize), Vec<String>>,
    entry_point: Option<String>,
    max_level: usize,
}

impl HnswIndex {
    pub fn new(dimensions: usize, config: HnswConfig) -> Result<Self, VectorSpaceError> {
        if dimensions == 0 {
            return Err(VectorSpaceError::InvalidHnswConfiguration(
                "dimensions must be greater than zero",
            ));
        }
        if config.m == 0 {
            return Err(VectorSpaceError::InvalidHnswConfiguration(
                "m must be greater than zero",
            ));
        }
        if config.ef_construction == 0 || config.ef_search == 0 {
            return Err(VectorSpaceError::InvalidHnswConfiguration(
                "ef construction and search must be greater than zero",
            ));
        }
        Ok(Self {
            dimensions,
            config,
            entries: BTreeMap::new(),
            tombstones: BTreeSet::new(),
            levels: BTreeMap::new(),
            neighbors: BTreeMap::new(),
            entry_point: None,
            max_level: 0,
        })
    }

    pub fn metric(&self) -> SimilarityMetric {
        SimilarityMetric::HnswCosine
    }

    pub fn len(&self) -> usize {
        self.entries.len().saturating_sub(self.tombstones.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn insert(
        &mut self,
        id: impl Into<String>,
        vector: Vec<f32>,
    ) -> Result<(), VectorSpaceError> {
        if vector.len() != self.dimensions {
            return Err(VectorSpaceError::DimensionMismatch {
                expected: self.dimensions,
                got: vector.len(),
            });
        }
        let id = id.into();
        if self.entries.contains_key(&id) {
            return Err(VectorSpaceError::DuplicateVector(id));
        }

        let level = deterministic_level(&id, self.config.m);
        if self.entry_point.is_none() {
            self.entries.insert(id.clone(), vector);
            self.levels.insert(id.clone(), level);
            self.entry_point = Some(id);
            self.max_level = level;
            return Ok(());
        }

        let mut entry = self.entry_point.clone().expect("entry point is present");
        let prior_max_level = self.max_level;
        let mut visited = BTreeSet::new();
        for current_level in ((level + 1)..=prior_max_level).rev() {
            entry = self.greedy_search(&vector, entry, current_level, &mut visited);
        }

        self.entries.insert(id.clone(), vector.clone());
        self.levels.insert(id.clone(), level);
        for current_level in (0..=level.min(prior_max_level)).rev() {
            let candidates = self.search_layer(
                &vector,
                &entry,
                self.config.ef_construction,
                current_level,
                &mut visited,
            );
            let selected = self.select_neighbors(&vector, candidates, self.config.m);
            self.neighbors
                .insert((id.clone(), current_level), selected.clone());
            for neighbor in selected {
                self.connect(&id, &neighbor, current_level);
            }
            if let Some(next_entry) = self
                .neighbors
                .get(&(id.clone(), current_level))
                .and_then(|v| v.first())
            {
                entry = next_entry.clone();
            }
        }

        if level > self.max_level {
            self.entry_point = Some(id);
            self.max_level = level;
        }
        Ok(())
    }

    /// Replace a vector and rebuild the graph from active entries so every
    /// retained edge reflects the replacement embedding.
    pub fn upsert(
        &mut self,
        id: impl Into<String>,
        vector: Vec<f32>,
    ) -> Result<(), VectorSpaceError> {
        if vector.len() != self.dimensions {
            return Err(VectorSpaceError::DimensionMismatch {
                expected: self.dimensions,
                got: vector.len(),
            });
        }
        let id = id.into();
        if !self.entries.contains_key(&id) {
            return self.insert(id, vector);
        }
        self.entries.insert(id.clone(), vector);
        self.tombstones.remove(&id);
        self.rebuild();
        Ok(())
    }

    /// Exclude a vector from results immediately. Stale graph links are
    /// compacted after a bounded tombstone threshold.
    pub fn remove(&mut self, id: &str) -> Result<(), VectorSpaceError> {
        if !self.entries.contains_key(id) || self.tombstones.contains(id) {
            return Err(VectorSpaceError::VectorNotFound(id.to_string()));
        }
        self.tombstones.insert(id.to_string());
        if self.tombstones.len() >= self.rebuild_threshold() {
            self.rebuild();
        }
        Ok(())
    }

    pub fn knn(
        &self,
        query: &[f32],
        k: usize,
    ) -> Result<(Vec<(String, f32)>, HnswSearchStats), VectorSpaceError> {
        if query.len() != self.dimensions {
            return Err(VectorSpaceError::DimensionMismatch {
                expected: self.dimensions,
                got: query.len(),
            });
        }
        if self.is_empty() {
            return Ok((Vec::new(), HnswSearchStats { visited_nodes: 0 }));
        }
        let Some(mut entry) = self.entry_point.clone() else {
            return Ok((Vec::new(), HnswSearchStats { visited_nodes: 0 }));
        };
        if k == 0 {
            return Ok((Vec::new(), HnswSearchStats { visited_nodes: 0 }));
        }

        let mut visited = BTreeSet::new();
        for current_level in (1..=self.max_level).rev() {
            entry = self.greedy_search(query, entry, current_level, &mut visited);
        }
        let candidates =
            self.search_layer(query, &entry, self.config.ef_search.max(k), 0, &mut visited);
        let mut results = self
            .scored(candidates, query)
            .into_iter()
            .filter(|(id, _)| !self.tombstones.contains(id))
            .collect::<Vec<_>>();
        results.truncate(k);
        Ok((
            results,
            HnswSearchStats {
                visited_nodes: visited.len(),
            },
        ))
    }

    fn greedy_search(
        &self,
        query: &[f32],
        mut current: String,
        level: usize,
        visited: &mut BTreeSet<String>,
    ) -> String {
        loop {
            visited.insert(current.clone());
            let current_score = self.score(query, &current);
            let next = self
                .neighbors
                .get(&(current.clone(), level))
                .into_iter()
                .flatten()
                .filter(|candidate| self.score(query, candidate) > current_score)
                .max_by(|left, right| {
                    compare_scored_ids(
                        self.score(query, left),
                        left,
                        self.score(query, right),
                        right,
                    )
                })
                .cloned();
            match next {
                Some(next) => current = next,
                None => return current,
            }
        }
    }

    fn search_layer(
        &self,
        query: &[f32],
        entry: &str,
        ef: usize,
        level: usize,
        visited: &mut BTreeSet<String>,
    ) -> Vec<String> {
        let mut candidates = vec![entry.to_string()];
        let mut results = vec![entry.to_string()];
        visited.insert(entry.to_string());
        while !candidates.is_empty() {
            let next_index = candidates
                .iter()
                .enumerate()
                .max_by(|(_, left), (_, right)| {
                    compare_scored_ids(
                        self.score(query, left),
                        left,
                        self.score(query, right),
                        right,
                    )
                })
                .map(|(index, _)| index)
                .expect("candidates is not empty");
            let current = candidates.swap_remove(next_index);
            let worst_score = results
                .iter()
                .map(|id| self.score(query, id))
                .min_by(f32::total_cmp)
                .unwrap_or(f32::NEG_INFINITY);
            if results.len() >= ef && self.score(query, &current) < worst_score {
                break;
            }
            for neighbor in self.neighbors.get(&(current, level)).into_iter().flatten() {
                if !visited.insert(neighbor.clone()) {
                    continue;
                }
                let score = self.score(query, neighbor);
                if results.len() < ef || score >= worst_score {
                    candidates.push(neighbor.clone());
                    results.push(neighbor.clone());
                    self.trim_to(query, &mut results, ef);
                }
            }
        }
        results
    }

    fn connect(&mut self, id: &str, neighbor: &str, level: usize) {
        let links = self
            .neighbors
            .entry((neighbor.to_string(), level))
            .or_default();
        links.push(id.to_string());
        let neighbor_vector = self.entries.get(neighbor).expect("known neighbor").clone();
        links.sort_by(|left, right| {
            cosine_score(
                &neighbor_vector,
                self.entries.get(right).expect("known vector"),
            )
            .total_cmp(&cosine_score(
                &neighbor_vector,
                self.entries.get(left).expect("known vector"),
            ))
            .then(left.cmp(right))
        });
        links.dedup();
        links.truncate(self.config.m);
    }

    fn select_neighbors(
        &self,
        query: &[f32],
        candidates: Vec<String>,
        limit: usize,
    ) -> Vec<String> {
        let mut selected = self.scored(candidates, query);
        selected.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
        selected.into_iter().take(limit).map(|(id, _)| id).collect()
    }

    fn scored(&self, ids: Vec<String>, query: &[f32]) -> Vec<(String, f32)> {
        let mut scores = ids
            .into_iter()
            .map(|id| {
                let score = self.score(query, &id);
                (id, score)
            })
            .collect::<Vec<_>>();
        scores.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
        scores
    }

    fn trim_to(&self, query: &[f32], ids: &mut Vec<String>, limit: usize) {
        ids.sort_by(|left, right| {
            compare_scored_ids(
                self.score(query, right),
                right,
                self.score(query, left),
                left,
            )
        });
        ids.truncate(limit);
    }

    fn score(&self, query: &[f32], id: &str) -> f32 {
        cosine_score(
            query,
            self.entries
                .get(id)
                .expect("HNSW graph references a known vector"),
        )
    }

    fn rebuild_threshold(&self) -> usize {
        (self.entries.len() / 4).max(8)
    }

    fn rebuild(&mut self) {
        let active_entries = self
            .entries
            .iter()
            .filter(|(id, _)| !self.tombstones.contains(*id))
            .map(|(id, vector)| (id.clone(), vector.clone()))
            .collect::<Vec<_>>();
        self.entries.clear();
        self.tombstones.clear();
        self.levels.clear();
        self.neighbors.clear();
        self.entry_point = None;
        self.max_level = 0;
        for (id, vector) in active_entries {
            self.insert(id, vector)
                .expect("active HNSW entries retain validated dimensions");
        }
    }
}

/// Observable state for an engine-owned named HNSW index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswIndexStatus {
    pub dimensions: usize,
    pub generation: u64,
    pub strategy: SimilarityMetric,
    pub ready: bool,
}

#[derive(Debug)]
struct ManagedHnswIndex {
    index: HnswIndex,
    generation: u64,
}

#[derive(Debug)]
struct ManagedExactEuclideanIndex {
    dimensions: usize,
    entries: BTreeMap<String, Vec<f32>>,
    generation: u64,
}

/// Thread-safe registry intended to be owned once per database by the engine.
///
/// Indexes are created explicitly during lifecycle work. Querying an absent or
/// empty index never triggers a build, warmup, or strategy change.
#[derive(Debug, Default)]
pub struct HnswRegistry {
    indexes: RwLock<BTreeMap<String, ManagedHnswIndex>>,
    exact_euclidean_indexes: RwLock<BTreeMap<String, ManagedExactEuclideanIndex>>,
}

impl HnswRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_index(
        &self,
        name: impl Into<String>,
        dimensions: usize,
        config: HnswConfig,
    ) -> Result<(), VectorSpaceError> {
        let name = name.into();
        let index = HnswIndex::new(dimensions, config)?;
        let mut indexes = self.indexes.write();
        if indexes.contains_key(&name) || self.exact_euclidean_indexes.read().contains_key(&name) {
            return Err(VectorSpaceError::IndexAlreadyExists(name));
        }
        indexes.insert(
            name,
            ManagedHnswIndex {
                index,
                generation: 0,
            },
        );
        Ok(())
    }

    pub fn create_exact_euclidean_index(
        &self,
        name: impl Into<String>,
        dimensions: usize,
    ) -> Result<(), VectorSpaceError> {
        if dimensions == 0 {
            return Err(VectorSpaceError::InvalidHnswConfiguration(
                "dimensions must be greater than zero",
            ));
        }
        let name = name.into();
        let mut indexes = self.exact_euclidean_indexes.write();
        if indexes.contains_key(&name) || self.indexes.read().contains_key(&name) {
            return Err(VectorSpaceError::IndexAlreadyExists(name));
        }
        indexes.insert(
            name,
            ManagedExactEuclideanIndex {
                dimensions,
                entries: BTreeMap::new(),
                generation: 0,
            },
        );
        Ok(())
    }

    pub fn status(&self, name: &str) -> Result<HnswIndexStatus, VectorSpaceError> {
        let indexes = self.indexes.read();
        if let Some(managed) = indexes.get(name) {
            return Ok(HnswIndexStatus {
                dimensions: managed.index.dimensions,
                generation: managed.generation,
                strategy: managed.index.metric(),
                ready: true,
            });
        }
        let indexes = self.exact_euclidean_indexes.read();
        let managed = indexes
            .get(name)
            .ok_or_else(|| VectorSpaceError::IndexNotFound(name.to_string()))?;
        Ok(HnswIndexStatus {
            dimensions: managed.dimensions,
            generation: managed.generation,
            strategy: SimilarityMetric::ExactEuclidean,
            ready: true,
        })
    }

    pub fn upsert(
        &self,
        name: &str,
        id: impl Into<String>,
        vector: Vec<f32>,
    ) -> Result<(), VectorSpaceError> {
        let id = id.into();
        let mut indexes = self.indexes.write();
        if let Some(managed) = indexes.get_mut(name) {
            managed.index.upsert(id, vector)?;
            managed.generation = managed.generation.saturating_add(1);
            return Ok(());
        }
        let mut indexes = self.exact_euclidean_indexes.write();
        let managed = indexes
            .get_mut(name)
            .ok_or_else(|| VectorSpaceError::IndexNotFound(name.to_string()))?;
        if vector.len() != managed.dimensions {
            return Err(VectorSpaceError::DimensionMismatch {
                expected: managed.dimensions,
                got: vector.len(),
            });
        }
        managed.entries.insert(id, vector);
        managed.generation = managed.generation.saturating_add(1);
        Ok(())
    }

    pub fn remove(&self, name: &str, id: &str) -> Result<(), VectorSpaceError> {
        let mut indexes = self.indexes.write();
        if let Some(managed) = indexes.get_mut(name) {
            managed.index.remove(id)?;
            managed.generation = managed.generation.saturating_add(1);
            return Ok(());
        }
        let mut indexes = self.exact_euclidean_indexes.write();
        let managed = indexes
            .get_mut(name)
            .ok_or_else(|| VectorSpaceError::IndexNotFound(name.to_string()))?;
        if managed.entries.remove(id).is_none() {
            return Err(VectorSpaceError::VectorNotFound(id.to_string()));
        }
        managed.generation = managed.generation.saturating_add(1);
        Ok(())
    }

    pub fn knn(
        &self,
        name: &str,
        query: &[f32],
        k: usize,
    ) -> Result<(Vec<(String, f32)>, HnswSearchStats), VectorSpaceError> {
        let indexes = self.indexes.read();
        if let Some(managed) = indexes.get(name) {
            return managed.index.knn(query, k);
        }
        let indexes = self.exact_euclidean_indexes.read();
        let managed = indexes
            .get(name)
            .ok_or_else(|| VectorSpaceError::IndexNotFound(name.to_string()))?;
        if query.len() != managed.dimensions {
            return Err(VectorSpaceError::DimensionMismatch {
                expected: managed.dimensions,
                got: query.len(),
            });
        }
        let mut scores = managed
            .entries
            .iter()
            .map(|(id, vector)| (id.clone(), euclidean_score(query, vector)))
            .collect::<Vec<_>>();
        scores.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
        scores.truncate(k);
        Ok((
            scores,
            HnswSearchStats {
                visited_nodes: managed.entries.len(),
            },
        ))
    }
}

fn euclidean_score(a: &[f32], b: &[f32]) -> f32 {
    let distance = a
        .iter()
        .zip(b.iter())
        .map(|(left, right)| (left - right).powi(2))
        .sum::<f32>()
        .sqrt();
    1.0 / (1.0 + distance)
}

fn compare_scored_ids(
    left_score: f32,
    left_id: &str,
    right_score: f32,
    right_id: &str,
) -> std::cmp::Ordering {
    left_score
        .total_cmp(&right_score)
        .then_with(|| right_id.cmp(left_id))
}

fn deterministic_level(id: &str, m: usize) -> usize {
    let mut state = 0xcbf2_9ce4_8422_2325_u64;
    for byte in id.bytes() {
        state ^= u64::from(byte);
        state = state.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let mut level = 0;
    while level < 16 && state % m as u64 == 0 {
        level += 1;
        state = state.rotate_left(17).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    }
    level
}

impl VectorSpace {
    pub fn new(name: impl Into<String>, dimensions: usize) -> Self {
        Self {
            name: name.into(),
            dimensions,
            entries: HashMap::new(),
        }
    }

    pub fn metric(&self) -> SimilarityMetric {
        SimilarityMetric::ExactCosine
    }

    /// Insert a vector with the given ID.
    pub fn insert(
        &mut self,
        id: impl Into<String>,
        vector: Vec<f32>,
    ) -> Result<(), VectorSpaceError> {
        if vector.len() != self.dimensions {
            return Err(VectorSpaceError::DimensionMismatch {
                expected: self.dimensions,
                got: vector.len(),
            });
        }
        self.entries.insert(id.into(), vector);
        Ok(())
    }

    /// Find the k nearest neighbors with an exact cosine scan.
    ///
    /// This method deliberately does not claim HNSW behavior until a graph
    /// traversal implementation owns the query path.
    pub fn knn(&self, query: &[f32], k: usize) -> Result<Vec<(String, f32)>, VectorSpaceError> {
        if query.len() != self.dimensions {
            return Err(VectorSpaceError::DimensionMismatch {
                expected: self.dimensions,
                got: query.len(),
            });
        }
        let mut scores: Vec<(String, f32)> = self
            .entries
            .iter()
            .map(|(id, v)| (id.clone(), cosine_score(query, v)))
            .collect();
        scores.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
        scores.truncate(k);
        Ok(scores)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

fn cosine_score(a: &[f32], b: &[f32]) -> f32 {
    let d = dot(a, b);
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        d / (na * nb)
    }
}

/// Global registry of vector spaces.
#[derive(Default)]
pub struct Registry {
    spaces: HashMap<String, VectorSpace>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_space(&mut self, space: VectorSpace) {
        self.spaces.insert(space.name.clone(), space);
    }

    pub fn get_space(&self, name: &str) -> Option<&VectorSpace> {
        self.spaces.get(name)
    }

    pub fn get_space_mut(&mut self, name: &str) -> Option<&mut VectorSpace> {
        self.spaces.get_mut(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_knn() {
        let mut space = VectorSpace::new("test", 4);
        space.insert("a", vec![1.0, 0.0, 0.0, 0.0]).unwrap();
        space.insert("b", vec![0.0, 1.0, 0.0, 0.0]).unwrap();
        let results = space.knn(&[1.0, 0.0, 0.0, 0.0], 2).unwrap();
        assert_eq!(results[0].0, "a");
        assert!((results[0].1 - 1.0).abs() < 1e-5);
        assert_eq!(space.metric(), SimilarityMetric::ExactCosine);
    }

    #[test]
    fn test_dimension_mismatch() {
        let mut space = VectorSpace::new("test", 4);
        assert!(space.insert("bad", vec![1.0, 2.0]).is_err());
    }

    #[test]
    fn exact_cosine_orders_equal_scores_by_id() {
        let mut space = VectorSpace::new("test", 2);
        space.insert("zeta", vec![1.0, 0.0]).unwrap();
        space.insert("alpha", vec![1.0, 0.0]).unwrap();

        let results = space.knn(&[1.0, 0.0], 2).unwrap();
        assert_eq!(
            results.into_iter().map(|(id, _)| id).collect::<Vec<_>>(),
            vec!["alpha", "zeta"]
        );
    }

    #[test]
    fn hnsw_traverses_a_sparse_graph_and_finds_the_exact_target() {
        let mut index = HnswIndex::new(
            2,
            HnswConfig {
                m: 4,
                ef_construction: 12,
                ef_search: 8,
            },
        )
        .unwrap();
        let mut exact_oracle = VectorSpace::new("exact-oracle", 2);
        for position in 0..64 {
            let angle = position as f32 * std::f32::consts::TAU / 64.0;
            let id = format!("point-{position:02}");
            let vector = vec![angle.cos(), angle.sin()];
            index.insert(id.clone(), vector.clone()).unwrap();
            exact_oracle.insert(id, vector).unwrap();
        }

        let target = 37;
        let angle = target as f32 * std::f32::consts::TAU / 64.0;
        let query = [angle.cos(), angle.sin()];
        let (results, stats) = index.knn(&query, 3).unwrap();
        let oracle = exact_oracle.knn(&query, 3).unwrap();

        assert_eq!(index.metric(), SimilarityMetric::HnswCosine);
        assert_eq!(results[0].0, "point-37");
        assert_eq!(results, oracle);
        assert!(
            stats.visited_nodes < index.len(),
            "HNSW query must traverse graph neighbors instead of scanning all vectors"
        );
    }

    #[test]
    fn hnsw_rejects_invalid_configuration_and_dimension_mismatches() {
        assert!(HnswIndex::new(0, HnswConfig::default()).is_err());
        assert!(HnswIndex::new(
            2,
            HnswConfig {
                m: 0,
                ..HnswConfig::default()
            }
        )
        .is_err());

        let mut index = HnswIndex::new(2, HnswConfig::default()).unwrap();
        assert!(index.insert("bad", vec![1.0]).is_err());
        index.insert("zero", vec![0.0, 0.0]).unwrap();
        assert!(index.knn(&[1.0], 1).is_err());
        assert_eq!(index.knn(&[1.0, 0.0], 1).unwrap().0[0].1, 0.0);
    }

    #[test]
    fn hnsw_tombstones_are_filtered_and_upserts_rebuild_active_graph() {
        let mut index = HnswIndex::new(
            2,
            HnswConfig {
                m: 4,
                ef_construction: 8,
                ef_search: 8,
            },
        )
        .unwrap();
        index.insert("removed", vec![1.0, 0.0]).unwrap();
        index.insert("updated", vec![0.0, 1.0]).unwrap();
        index.insert("other", vec![-1.0, 0.0]).unwrap();

        index.remove("removed").unwrap();
        let (after_remove, _) = index.knn(&[1.0, 0.0], 3).unwrap();
        assert!(after_remove.iter().all(|(id, _)| id != "removed"));
        assert_eq!(index.len(), 2);

        index.upsert("updated", vec![1.0, 0.0]).unwrap();
        let (after_upsert, _) = index.knn(&[1.0, 0.0], 1).unwrap();
        assert_eq!(after_upsert[0].0, "updated");
    }

    #[test]
    fn hnsw_compacts_tombstones_without_a_query_side_effect() {
        let mut index = HnswIndex::new(2, HnswConfig::default()).unwrap();
        for position in 0..8 {
            index
                .insert(format!("vector-{position}"), vec![position as f32, 1.0])
                .unwrap();
        }
        for position in 0..8 {
            index.remove(&format!("vector-{position}")).unwrap();
        }

        assert!(index.is_empty());
        assert_eq!(index.knn(&[1.0, 0.0], 1).unwrap().0, Vec::new());
    }

    #[test]
    fn hnsw_registry_exposes_readiness_generation_and_non_warming_queries() {
        let registry = HnswRegistry::new();
        registry
            .create_index("documents.embedding", 2, HnswConfig::default())
            .unwrap();

        let before_query = registry.status("documents.embedding").unwrap();
        assert_eq!(before_query.strategy, SimilarityMetric::HnswCosine);
        assert!(before_query.ready);
        assert_eq!(before_query.generation, 0);
        assert!(registry
            .knn("documents.embedding", &[1.0, 0.0], 3)
            .unwrap()
            .0
            .is_empty());
        assert_eq!(
            registry.status("documents.embedding").unwrap().generation,
            before_query.generation,
            "querying must not build or mutate the index"
        );

        registry
            .upsert("documents.embedding", "doc-1", vec![1.0, 0.0])
            .unwrap();
        assert_eq!(
            registry.status("documents.embedding").unwrap().generation,
            1
        );
        assert_eq!(
            registry
                .knn("documents.embedding", &[1.0, 0.0], 1)
                .unwrap()
                .0[0]
                .0,
            "doc-1"
        );
        assert!(registry
            .create_index("documents.embedding", 2, HnswConfig::default())
            .is_err());
    }

    #[test]
    fn exact_euclidean_registry_orders_candidates_and_reports_its_strategy() {
        let registry = HnswRegistry::new();
        registry
            .create_exact_euclidean_index("documents.embedding", 2)
            .unwrap();
        registry
            .upsert("documents.embedding", "near", vec![0.0, 1.0])
            .unwrap();
        registry
            .upsert("documents.embedding", "far", vec![3.0, 4.0])
            .unwrap();

        let (results, stats) = registry.knn("documents.embedding", &[0.0, 0.0], 2).unwrap();
        assert_eq!(results[0].0, "near");
        assert_eq!(stats.visited_nodes, 2);

        let status = registry.status("documents.embedding").unwrap();
        assert_eq!(status.strategy, SimilarityMetric::ExactEuclidean);
        assert_eq!(status.generation, 2);
        assert!(registry
            .upsert("documents.embedding", "invalid", vec![1.0])
            .is_err());
    }
}
