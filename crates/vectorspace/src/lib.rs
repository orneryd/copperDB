//! Vector space registry for copperdb.
//!
//! Equivalent to Go's `pkg/vectorspace` in NornicDB.
//! Manages named embedding spaces (collections of high-dimensional vectors)
//! and supports explicit exact cosine similarity search.

use copperdb_util::RequestCancellation;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::fs;
use std::hash::Hasher;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;

const REGISTRY_ARTIFACT_MAGIC: &[u8] = b"COPPERDB-HNSW\0";
const REGISTRY_ARTIFACT_FORMAT_VERSION: u32 = 1;
const VECTOR_FILE_STORE_MAGIC: &[u8] = b"COPPERDB-VECTOR-FILE\0";
const VECTOR_FILE_STORE_FORMAT_VERSION: u32 = 1;
const VECTOR_FILE_STORE_HEADER_BYTES: u64 =
    (VECTOR_FILE_STORE_MAGIC.len() + std::mem::size_of::<u32>() * 2) as u64;
const VECTOR_FILE_STORE_UPSERT: u8 = 1;
const VECTOR_FILE_STORE_DELETE: u8 = 2;
const MAX_VECTOR_FILE_STORE_ID_BYTES: usize = 1024 * 1024;

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
    #[error("vector registry artifact is corrupt: {0}")]
    CorruptArtifact(&'static str),
    #[error("unsupported vector registry artifact format: expected {expected}, got {actual}")]
    UnsupportedArtifactFormat { expected: u32, actual: u32 },
    #[error("vector registry artifact I/O failed: {0}")]
    ArtifactIo(#[from] io::Error),
    #[error("vector registry artifact serialization failed: {0}")]
    ArtifactSerialization(String),
    #[error("vector file store is corrupt: {0}")]
    CorruptVectorFileStore(&'static str),
    #[error("unsupported vector file store format: expected {expected}, got {actual}")]
    UnsupportedVectorFileStoreFormat { expected: u32, actual: u32 },
    #[error("vector file store I/O failed: {0}")]
    VectorFileStoreIo(#[source] io::Error),
    #[error("request cancelled")]
    RequestCancelled,
}

/// Append-only normalized vector storage with an in-memory ID-to-offset map.
///
/// The map is reconstructed from durable upsert/delete records on open. This
/// foundation is intentionally separate from the current in-memory HNSW
/// registry until its lifecycle is wired through the engine.
#[derive(Debug)]
pub struct VectorFileStore {
    path: PathBuf,
    dimensions: usize,
    offsets: BTreeMap<String, u64>,
}

impl VectorFileStore {
    /// Create or open a version-1 vector file store at `path`.
    pub fn open(path: impl AsRef<Path>, dimensions: usize) -> Result<Self, VectorSpaceError> {
        if dimensions == 0 {
            return Err(VectorSpaceError::InvalidHnswConfiguration(
                "dimensions must be greater than zero",
            ));
        }
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            fs::create_dir_all(parent).map_err(VectorSpaceError::VectorFileStoreIo)?;
            let mut file = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)
                .map_err(VectorSpaceError::VectorFileStoreIo)?;
            file.write_all(VECTOR_FILE_STORE_MAGIC)
                .and_then(|()| file.write_all(&VECTOR_FILE_STORE_FORMAT_VERSION.to_le_bytes()))
                .and_then(|()| file.write_all(&(dimensions as u32).to_le_bytes()))
                .and_then(|()| file.sync_all())
                .map_err(VectorSpaceError::VectorFileStoreIo)?;
        }

        let mut file = fs::File::open(&path).map_err(VectorSpaceError::VectorFileStoreIo)?;
        let offsets = Self::scan_offsets(&mut file, dimensions)?;
        Ok(Self {
            path,
            dimensions,
            offsets,
        })
    }

    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Append a normalized replacement vector and update the live offset map.
    pub fn upsert(&mut self, id: impl AsRef<str>, vector: &[f32]) -> Result<(), VectorSpaceError> {
        if vector.len() != self.dimensions {
            return Err(VectorSpaceError::DimensionMismatch {
                expected: self.dimensions,
                got: vector.len(),
            });
        }
        let id = id.as_ref();
        let id_length = u32::try_from(id.len()).map_err(|_| {
            VectorSpaceError::CorruptVectorFileStore("vector ID exceeds format length limit")
        })?;
        let vector = normalize_vector(vector);
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(VectorSpaceError::VectorFileStoreIo)?;
        let offset = file
            .seek(SeekFrom::End(0))
            .map_err(VectorSpaceError::VectorFileStoreIo)?;
        file.write_all(&[VECTOR_FILE_STORE_UPSERT])
            .and_then(|()| file.write_all(&id_length.to_le_bytes()))
            .and_then(|()| file.write_all(id.as_bytes()))
            .map_err(VectorSpaceError::VectorFileStoreIo)?;
        for value in vector {
            file.write_all(&value.to_le_bytes())
                .map_err(VectorSpaceError::VectorFileStoreIo)?;
        }
        file.sync_data()
            .map_err(VectorSpaceError::VectorFileStoreIo)?;
        self.offsets.insert(id.to_owned(), offset);
        Ok(())
    }

    /// Append a rebuild/import batch with one file open and one durability sync.
    pub fn upsert_batch<I>(&mut self, entries: I) -> Result<(), VectorSpaceError>
    where
        I: IntoIterator<Item = (String, Vec<f32>)>,
    {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(VectorSpaceError::VectorFileStoreIo)?;
        let mut offset = file
            .seek(SeekFrom::End(0))
            .map_err(VectorSpaceError::VectorFileStoreIo)?;
        let mut pending_offsets = Vec::new();
        for (id, vector) in entries {
            if vector.len() != self.dimensions {
                return Err(VectorSpaceError::DimensionMismatch {
                    expected: self.dimensions,
                    got: vector.len(),
                });
            }
            let id_length = u32::try_from(id.len()).map_err(|_| {
                VectorSpaceError::CorruptVectorFileStore("vector ID exceeds format length limit")
            })?;
            let vector = normalize_vector(&vector);
            let record_offset = offset;
            file.write_all(&[VECTOR_FILE_STORE_UPSERT])
                .and_then(|()| file.write_all(&id_length.to_le_bytes()))
                .and_then(|()| file.write_all(id.as_bytes()))
                .map_err(VectorSpaceError::VectorFileStoreIo)?;
            for value in vector {
                file.write_all(&value.to_le_bytes())
                    .map_err(VectorSpaceError::VectorFileStoreIo)?;
            }
            offset = offset
                .checked_add(1 + 4 + id.len() as u64 + (self.dimensions as u64 * 4))
                .ok_or(VectorSpaceError::CorruptVectorFileStore(
                    "vector file offset overflow",
                ))?;
            pending_offsets.push((id, record_offset));
        }
        file.sync_data()
            .map_err(VectorSpaceError::VectorFileStoreIo)?;
        self.offsets.extend(pending_offsets);
        Ok(())
    }

    /// Append a delete record and remove the ID from the live offset map.
    pub fn remove(&mut self, id: &str) -> Result<bool, VectorSpaceError> {
        if !self.offsets.contains_key(id) {
            return Ok(false);
        }
        let id_length = u32::try_from(id.len()).map_err(|_| {
            VectorSpaceError::CorruptVectorFileStore("vector ID exceeds format length limit")
        })?;
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(VectorSpaceError::VectorFileStoreIo)?;
        file.write_all(&[VECTOR_FILE_STORE_DELETE])
            .and_then(|()| file.write_all(&id_length.to_le_bytes()))
            .and_then(|()| file.write_all(id.as_bytes()))
            .and_then(|()| file.sync_data())
            .map_err(VectorSpaceError::VectorFileStoreIo)?;
        self.offsets.remove(id);
        Ok(true)
    }

    /// Read the normalized live vector for `id` without scanning the file.
    pub fn get(&self, id: &str) -> Result<Option<Vec<f32>>, VectorSpaceError> {
        let Some(offset) = self.offsets.get(id) else {
            return Ok(None);
        };
        let mut file = fs::File::open(&self.path).map_err(VectorSpaceError::VectorFileStoreIo)?;
        file.seek(SeekFrom::Start(*offset))
            .map_err(VectorSpaceError::VectorFileStoreIo)?;
        let operation = read_vector_file_byte(&mut file)?;
        if operation != VECTOR_FILE_STORE_UPSERT {
            return Err(VectorSpaceError::CorruptVectorFileStore(
                "live offset does not reference an upsert record",
            ));
        }
        let stored_id = read_vector_file_id(&mut file)?;
        if stored_id != id {
            return Err(VectorSpaceError::CorruptVectorFileStore(
                "live offset points to another vector ID",
            ));
        }
        read_vector_file_values(&mut file, self.dimensions).map(Some)
    }

    /// Score and rank candidate IDs using exact cosine similarity.
    ///
    /// Missing IDs are ignored so a stale derived candidate cannot escape as
    /// a result. The backing file is opened once for the entire candidate set.
    pub fn score_candidates<I, S>(
        &self,
        query: &[f32],
        candidates: I,
        limit: usize,
    ) -> Result<Vec<(String, f32)>, VectorSpaceError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.score_candidates_with_cancellation(query, candidates, limit, None)
    }

    pub fn score_candidates_cancellable<I, S>(
        &self,
        query: &[f32],
        candidates: I,
        limit: usize,
        cancellation: &RequestCancellation,
    ) -> Result<Vec<(String, f32)>, VectorSpaceError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.score_candidates_with_cancellation(query, candidates, limit, Some(cancellation))
    }

    fn score_candidates_with_cancellation<I, S>(
        &self,
        query: &[f32],
        candidates: I,
        limit: usize,
        cancellation: Option<&RequestCancellation>,
    ) -> Result<Vec<(String, f32)>, VectorSpaceError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if query.len() != self.dimensions {
            return Err(VectorSpaceError::DimensionMismatch {
                expected: self.dimensions,
                got: query.len(),
            });
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let query = normalize_vector(query);
        let mut candidate_offsets = candidates
            .into_iter()
            .filter_map(|candidate| {
                let id = candidate.as_ref();
                self.offsets
                    .get(id)
                    .copied()
                    .map(|offset| (id.to_owned(), offset))
            })
            .collect::<Vec<_>>();
        candidate_offsets.sort_by_key(|(_, offset)| *offset);
        let mut file = fs::File::open(&self.path).map_err(VectorSpaceError::VectorFileStoreIo)?;
        let mut scores = Vec::new();
        for (position, (id, offset)) in candidate_offsets.into_iter().enumerate() {
            if position & 0xFF == 0 && cancellation.is_some_and(RequestCancellation::is_cancelled) {
                return Err(VectorSpaceError::RequestCancelled);
            }
            file.seek(SeekFrom::Start(offset))
                .map_err(VectorSpaceError::VectorFileStoreIo)?;
            if read_vector_file_byte(&mut file)? != VECTOR_FILE_STORE_UPSERT {
                return Err(VectorSpaceError::CorruptVectorFileStore(
                    "live offset does not reference an upsert record",
                ));
            }
            if read_vector_file_id(&mut file)? != id {
                return Err(VectorSpaceError::CorruptVectorFileStore(
                    "live offset points to another vector ID",
                ));
            }
            let vector = read_vector_file_values(&mut file, self.dimensions)?;
            let score = query
                .iter()
                .zip(vector.iter())
                .map(|(left, right)| left * right)
                .sum::<f32>();
            scores.push((id, score));
        }
        scores.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
        scores.truncate(limit);
        Ok(scores)
    }

    fn scan_offsets(
        file: &mut fs::File,
        dimensions: usize,
    ) -> Result<BTreeMap<String, u64>, VectorSpaceError> {
        let file_len = file
            .metadata()
            .map_err(VectorSpaceError::VectorFileStoreIo)?
            .len();
        if file_len < VECTOR_FILE_STORE_HEADER_BYTES {
            return Err(VectorSpaceError::CorruptVectorFileStore("truncated header"));
        }
        let mut magic = vec![0; VECTOR_FILE_STORE_MAGIC.len()];
        file.read_exact(&mut magic)
            .map_err(VectorSpaceError::VectorFileStoreIo)?;
        if magic != VECTOR_FILE_STORE_MAGIC {
            return Err(VectorSpaceError::CorruptVectorFileStore("invalid magic"));
        }
        let version = read_vector_file_u32(file)?;
        if version != VECTOR_FILE_STORE_FORMAT_VERSION {
            return Err(VectorSpaceError::UnsupportedVectorFileStoreFormat {
                expected: VECTOR_FILE_STORE_FORMAT_VERSION,
                actual: version,
            });
        }
        let stored_dimensions = read_vector_file_u32(file)? as usize;
        if stored_dimensions != dimensions {
            return Err(VectorSpaceError::DimensionMismatch {
                expected: dimensions,
                got: stored_dimensions,
            });
        }

        let mut offsets = BTreeMap::new();
        let mut offset = VECTOR_FILE_STORE_HEADER_BYTES;
        while offset < file_len {
            file.seek(SeekFrom::Start(offset))
                .map_err(VectorSpaceError::VectorFileStoreIo)?;
            let operation = read_vector_file_byte(file)?;
            let id = read_vector_file_id(file)?;
            match operation {
                VECTOR_FILE_STORE_UPSERT => {
                    read_vector_file_values(file, dimensions)?;
                    offsets.insert(id, offset);
                }
                VECTOR_FILE_STORE_DELETE => {
                    offsets.remove(&id);
                }
                _ => {
                    return Err(VectorSpaceError::CorruptVectorFileStore(
                        "unknown record operation",
                    ));
                }
            }
            offset = file
                .stream_position()
                .map_err(VectorSpaceError::VectorFileStoreIo)?;
        }
        Ok(offsets)
    }
}

fn normalize_vector(vector: &[f32]) -> Vec<f32> {
    let magnitude = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if magnitude == 0.0 {
        return vector.to_vec();
    }
    vector.iter().map(|value| value / magnitude).collect()
}

fn read_vector_file_byte(file: &mut fs::File) -> Result<u8, VectorSpaceError> {
    let mut byte = [0];
    file.read_exact(&mut byte)
        .map_err(VectorSpaceError::VectorFileStoreIo)?;
    Ok(byte[0])
}

fn read_vector_file_u32(file: &mut fs::File) -> Result<u32, VectorSpaceError> {
    let mut bytes = [0; std::mem::size_of::<u32>()];
    file.read_exact(&mut bytes)
        .map_err(VectorSpaceError::VectorFileStoreIo)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_vector_file_id(file: &mut fs::File) -> Result<String, VectorSpaceError> {
    let length = read_vector_file_u32(file)? as usize;
    if length > MAX_VECTOR_FILE_STORE_ID_BYTES {
        return Err(VectorSpaceError::CorruptVectorFileStore(
            "vector ID exceeds the format size limit",
        ));
    }
    let mut bytes = vec![0; length];
    file.read_exact(&mut bytes)
        .map_err(VectorSpaceError::VectorFileStoreIo)?;
    String::from_utf8(bytes)
        .map_err(|_| VectorSpaceError::CorruptVectorFileStore("vector ID is not UTF-8"))
}

fn read_vector_file_values(
    file: &mut fs::File,
    dimensions: usize,
) -> Result<Vec<f32>, VectorSpaceError> {
    let mut bytes = vec![0_u8; dimensions * std::mem::size_of::<f32>()];
    file.read_exact(&mut bytes)
        .map_err(VectorSpaceError::VectorFileStoreIo)?;
    Ok(bytes
        .chunks_exact(std::mem::size_of::<f32>())
        .map(|value| f32::from_le_bytes(value.try_into().expect("f32 chunk has four bytes")))
        .collect())
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
            m: 32,
            ef_construction: 400,
            ef_search: 200,
        }
    }
}

/// Evidence that a query traversed the graph rather than score-scanning records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswSearchStats {
    pub visited_nodes: usize,
    pub returned_candidates: usize,
    pub exact_scored_candidates: usize,
}

/// A deterministic, in-memory hierarchical navigable small-world graph.
///
/// This is intentionally separate from [`VectorSpace`]'s exact fallback. It
/// owns its graph from construction onwards, and query reads only traverse the
/// existing graph; they do not build, warm, or switch strategy.
#[derive(Debug, Clone, PartialEq)]
pub struct HnswIndex {
    dimensions: usize,
    config: HnswConfig,
    external_ids: Vec<String>,
    id_to_internal: HashMap<String, u32>,
    vectors: Vec<f32>,
    levels: Vec<usize>,
    neighbors: Vec<Vec<Vec<u32>>>,
    deleted: Vec<bool>,
    live_count: usize,
    entry_point: Option<u32>,
    max_level: usize,
}

#[derive(Debug, Clone, Copy)]
struct ScoredNode {
    score: f32,
    internal_id: u32,
}

#[derive(Debug, Clone, Copy)]
struct ExactScoredEntry<'a> {
    score: f32,
    id: &'a str,
}

impl PartialEq for ExactScoredEntry<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.score.to_bits() == other.score.to_bits() && self.id == other.id
    }
}

impl Eq for ExactScoredEntry<'_> {}

impl PartialOrd for ExactScoredEntry<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ExactScoredEntry<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .score
            .total_cmp(&self.score)
            .then_with(|| self.id.cmp(other.id))
    }
}

impl PartialEq for ScoredNode {
    fn eq(&self, other: &Self) -> bool {
        self.score.to_bits() == other.score.to_bits() && self.internal_id == other.internal_id
    }
}

impl Eq for ScoredNode {}

impl PartialOrd for ScoredNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredNode {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.internal_id.cmp(&self.internal_id))
    }
}

#[derive(Default)]
struct HnswSearchScratch {
    visited_generations: Vec<u32>,
    generation: u32,
    visited_count: usize,
    candidates: BinaryHeap<ScoredNode>,
    results: BinaryHeap<Reverse<ScoredNode>>,
}

impl HnswSearchScratch {
    fn reset(&mut self, node_count: usize, entry: ScoredNode, ef: usize) {
        self.visited_generations.resize(node_count, 0);
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.visited_generations.fill(0);
            self.generation = 1;
        }
        self.visited_count = 1;
        self.candidates.clear();
        self.results.clear();
        if self.candidates.capacity() < ef.saturating_mul(2) {
            self.candidates.reserve(ef.saturating_mul(2));
        }
        if self.results.capacity() < ef.saturating_mul(2) {
            self.results.reserve(ef.saturating_mul(2));
        }
        self.visited_generations[entry.internal_id as usize] = self.generation;
        self.candidates.push(entry);
        self.results.push(Reverse(entry));
    }

    fn mark_visited(&mut self, internal_id: u32) -> bool {
        let visited = &mut self.visited_generations[internal_id as usize];
        if *visited == self.generation {
            return false;
        }
        *visited = self.generation;
        self.visited_count += 1;
        true
    }
}

thread_local! {
    static HNSW_SEARCH_SCRATCH: RefCell<HnswSearchScratch> = RefCell::default();
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
            external_ids: Vec::new(),
            id_to_internal: HashMap::new(),
            vectors: Vec::new(),
            levels: Vec::new(),
            neighbors: Vec::new(),
            deleted: Vec::new(),
            live_count: 0,
            entry_point: None,
            max_level: 0,
        })
    }

    pub fn metric(&self) -> SimilarityMetric {
        SimilarityMetric::HnswCosine
    }

    pub fn len(&self) -> usize {
        self.live_count
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Estimate bytes owned by index vectors and graph buffers.
    ///
    /// This excludes allocator bookkeeping and hash-table bucket overhead, which are
    /// implementation-dependent. It is intended for relative lifecycle
    /// observability rather than a process-RSS measurement.
    pub fn estimated_memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .external_ids
                .iter()
                .map(|id| std::mem::size_of::<String>() + id.capacity())
                .sum::<usize>()
            + self.vectors.capacity() * std::mem::size_of::<f32>()
            + self
                .neighbors
                .iter()
                .flat_map(|levels| levels.iter())
                .map(|links| links.capacity() * std::mem::size_of::<u32>())
                .sum::<usize>()
            + self.levels.capacity() * std::mem::size_of::<usize>()
            + self.deleted.capacity() * std::mem::size_of::<bool>()
            + self.id_to_internal.capacity()
                * (std::mem::size_of::<String>() + std::mem::size_of::<u32>())
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
        if self.id_to_internal.contains_key(&id) {
            return Err(VectorSpaceError::DuplicateVector(id));
        }
        let vector = normalize_vector(&vector);
        let level = deterministic_level(&id, self.config.m);
        let internal_id = u32::try_from(self.external_ids.len()).map_err(|_| {
            VectorSpaceError::InvalidHnswConfiguration("index exceeds u32 node capacity")
        })?;
        if self.entry_point.is_none() {
            self.push_node(id, vector, level);
            self.entry_point = Some(internal_id);
            self.max_level = level;
            return Ok(());
        }

        let mut entry = self.entry_point.expect("entry point is present");
        let prior_max_level = self.max_level;
        for current_level in ((level + 1)..=prior_max_level).rev() {
            entry = self.greedy_search(&vector, entry, current_level);
        }

        self.push_node(id, vector.clone(), level);
        for current_level in (0..=level.min(prior_max_level)).rev() {
            let (candidates, _) = self.search_layer(
                &vector,
                entry,
                self.config.ef_construction,
                current_level,
                None,
            )?;
            let selected = self.select_neighbors(candidates, self.config.m);
            self.neighbors[internal_id as usize][current_level] = selected.clone();
            for neighbor in selected.iter().copied() {
                self.connect(internal_id, neighbor, current_level);
            }
            if let Some(next_entry) = selected.first() {
                entry = *next_entry;
            }
        }

        if level > self.max_level {
            self.entry_point = Some(internal_id);
            self.max_level = level;
        }
        Ok(())
    }

    /// Replace a vector by tombstoning its old dense node and appending a new one.
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
        if let Some(internal_id) = self.id_to_internal.get(&id).copied() {
            self.tombstone(internal_id);
        }
        self.insert(id, vector)?;
        self.compact_if_needed();
        Ok(())
    }

    /// Exclude a vector from results immediately. Stale graph links are
    /// compacted after a bounded tombstone threshold.
    pub fn remove(&mut self, id: &str) -> Result<(), VectorSpaceError> {
        let Some(internal_id) = self.id_to_internal.get(id).copied() else {
            return Err(VectorSpaceError::VectorNotFound(id.to_string()));
        };
        self.tombstone(internal_id);
        self.compact_if_needed();
        Ok(())
    }

    pub fn compact(&mut self) -> bool {
        if self.live_count == self.external_ids.len() {
            return false;
        }
        self.rebuild();
        true
    }

    pub fn knn(
        &self,
        query: &[f32],
        k: usize,
    ) -> Result<(Vec<(String, f32)>, HnswSearchStats), VectorSpaceError> {
        self.knn_with_cancellation(query, k, None)
    }

    fn knn_with_cancellation(
        &self,
        query: &[f32],
        k: usize,
        cancellation: Option<&RequestCancellation>,
    ) -> Result<(Vec<(String, f32)>, HnswSearchStats), VectorSpaceError> {
        if query.len() != self.dimensions {
            return Err(VectorSpaceError::DimensionMismatch {
                expected: self.dimensions,
                got: query.len(),
            });
        }
        if self.is_empty() {
            return Ok((
                Vec::new(),
                HnswSearchStats {
                    visited_nodes: 0,
                    returned_candidates: 0,
                    exact_scored_candidates: 0,
                },
            ));
        }
        let Some(mut entry) = self.entry_point else {
            return Ok((
                Vec::new(),
                HnswSearchStats {
                    visited_nodes: 0,
                    returned_candidates: 0,
                    exact_scored_candidates: 0,
                },
            ));
        };
        if k == 0 {
            return Ok((
                Vec::new(),
                HnswSearchStats {
                    visited_nodes: 0,
                    returned_candidates: 0,
                    exact_scored_candidates: 0,
                },
            ));
        }

        let query = normalize_vector(query);
        for current_level in (1..=self.max_level).rev() {
            if cancellation.is_some_and(RequestCancellation::is_cancelled) {
                return Err(VectorSpaceError::RequestCancelled);
            }
            entry = self.greedy_search(&query, entry, current_level);
        }
        let (candidates, visited_nodes) =
            self.search_layer(&query, entry, self.config.ef_search.max(k), 0, cancellation)?;
        let mut results = candidates
            .into_iter()
            .filter(|candidate| !self.deleted[candidate.internal_id as usize])
            .map(|candidate| {
                (
                    self.external_ids[candidate.internal_id as usize].clone(),
                    candidate.score,
                )
            })
            .collect::<Vec<_>>();
        results.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
        results.truncate(k);
        let returned_candidates = results.len();
        Ok((
            results,
            HnswSearchStats {
                visited_nodes,
                returned_candidates,
                exact_scored_candidates: 0,
            },
        ))
    }

    fn greedy_search(&self, query: &[f32], mut current: u32, level: usize) -> u32 {
        loop {
            let current_score = self.score(query, current);
            let next = self
                .links(current, level)
                .iter()
                .copied()
                .filter_map(|candidate| {
                    let score = self.score(query, candidate);
                    (score > current_score).then_some(ScoredNode {
                        score,
                        internal_id: candidate,
                    })
                })
                .max();
            match next {
                Some(next) => current = next.internal_id,
                None => return current,
            }
        }
    }

    fn search_layer(
        &self,
        query: &[f32],
        entry: u32,
        ef: usize,
        level: usize,
        cancellation: Option<&RequestCancellation>,
    ) -> Result<(Vec<ScoredNode>, usize), VectorSpaceError> {
        HNSW_SEARCH_SCRATCH.with_borrow_mut(|scratch| {
            self.search_layer_with_scratch(query, entry, ef, level, cancellation, scratch)
        })
    }

    fn search_layer_with_scratch(
        &self,
        query: &[f32],
        entry: u32,
        ef: usize,
        level: usize,
        cancellation: Option<&RequestCancellation>,
        scratch: &mut HnswSearchScratch,
    ) -> Result<(Vec<ScoredNode>, usize), VectorSpaceError> {
        let entry = ScoredNode {
            score: self.score(query, entry),
            internal_id: entry,
        };
        scratch.reset(self.external_ids.len(), entry, ef);
        let mut iterations = 0_usize;
        while let Some(current) = scratch.candidates.pop() {
            if iterations & 0xFF == 0 && cancellation.is_some_and(RequestCancellation::is_cancelled)
            {
                return Err(VectorSpaceError::RequestCancelled);
            }
            iterations += 1;
            let worst_score = scratch
                .results
                .peek()
                .map(|item| item.0.score)
                .unwrap_or(f32::NEG_INFINITY);
            if scratch.results.len() >= ef && current.score < worst_score {
                break;
            }
            for neighbor in self.links(current.internal_id, level).iter().copied() {
                if !scratch.mark_visited(neighbor) {
                    continue;
                }
                let candidate = ScoredNode {
                    score: self.score(query, neighbor),
                    internal_id: neighbor,
                };
                if scratch.results.len() < ef || candidate.score >= worst_score {
                    scratch.candidates.push(candidate);
                    scratch.results.push(Reverse(candidate));
                    if scratch.results.len() > ef {
                        scratch.results.pop();
                    }
                }
            }
        }
        Ok((
            scratch.results.iter().map(|item| item.0).collect(),
            scratch.visited_count,
        ))
    }

    fn connect(&mut self, internal_id: u32, neighbor: u32, level: usize) {
        let mut scored = self.neighbors[neighbor as usize][level]
            .iter()
            .copied()
            .chain(std::iter::once(internal_id))
            .map(|candidate| {
                (
                    candidate,
                    dot(self.vector(neighbor), self.vector(candidate)),
                )
            })
            .collect::<Vec<_>>();
        scored.sort_by(|left, right| {
            right.1.total_cmp(&left.1).then_with(|| {
                self.external_ids[left.0 as usize].cmp(&self.external_ids[right.0 as usize])
            })
        });
        scored.dedup_by_key(|candidate| candidate.0);
        self.neighbors[neighbor as usize][level] = scored
            .into_iter()
            .take(self.config.m)
            .map(|candidate| candidate.0)
            .collect();
    }

    fn select_neighbors(&self, candidates: Vec<ScoredNode>, limit: usize) -> Vec<u32> {
        let mut selected = candidates
            .into_iter()
            .filter(|candidate| !self.deleted[candidate.internal_id as usize])
            .collect::<Vec<_>>();
        selected.sort_by(|left, right| {
            right.score.total_cmp(&left.score).then_with(|| {
                self.external_ids[left.internal_id as usize]
                    .cmp(&self.external_ids[right.internal_id as usize])
            })
        });
        selected
            .into_iter()
            .take(limit)
            .map(|candidate| candidate.internal_id)
            .collect()
    }

    fn score(&self, query: &[f32], internal_id: u32) -> f32 {
        dot(query, self.vector(internal_id))
    }

    fn vector(&self, internal_id: u32) -> &[f32] {
        let start = internal_id as usize * self.dimensions;
        &self.vectors[start..start + self.dimensions]
    }

    fn links(&self, internal_id: u32, level: usize) -> &[u32] {
        self.neighbors[internal_id as usize]
            .get(level)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    fn push_node(&mut self, id: String, vector: Vec<f32>, level: usize) {
        let internal_id = self.external_ids.len() as u32;
        self.id_to_internal.insert(id.clone(), internal_id);
        self.external_ids.push(id);
        self.vectors.extend(vector);
        self.levels.push(level);
        self.neighbors.push(vec![Vec::new(); level + 1]);
        self.deleted.push(false);
        self.live_count += 1;
    }

    fn tombstone(&mut self, internal_id: u32) {
        let index = internal_id as usize;
        self.deleted[index] = true;
        self.live_count -= 1;
        self.id_to_internal.remove(&self.external_ids[index]);
        if self.entry_point == Some(internal_id) {
            self.select_entry_point();
        }
    }

    fn select_entry_point(&mut self) {
        self.entry_point = (0..self.external_ids.len())
            .filter(|index| !self.deleted[*index])
            .max_by(|left, right| {
                self.levels[*left]
                    .cmp(&self.levels[*right])
                    .then_with(|| self.external_ids[*right].cmp(&self.external_ids[*left]))
            })
            .map(|index| index as u32);
        self.max_level = self
            .entry_point
            .map(|entry| self.levels[entry as usize])
            .unwrap_or(0);
    }

    fn from_artifact(snapshot: HnswArtifactIndex) -> Result<Self, VectorSpaceError> {
        HnswIndex::new(snapshot.dimensions, snapshot.config)
            .map_err(|_| VectorSpaceError::CorruptArtifact("invalid HNSW configuration"))?;
        let node_count = snapshot.external_ids.len();
        if node_count > u32::MAX as usize {
            return Err(VectorSpaceError::CorruptArtifact(
                "HNSW node count exceeds u32 capacity",
            ));
        }
        if snapshot.vectors.len() != node_count.saturating_mul(snapshot.dimensions)
            || snapshot.levels.len() != node_count
            || snapshot.neighbors.len() != node_count
            || snapshot.deleted.len() != node_count
        {
            return Err(VectorSpaceError::CorruptArtifact(
                "HNSW dense array lengths differ",
            ));
        }

        let deleted_count = snapshot.deleted.iter().filter(|deleted| **deleted).count();
        if snapshot.live_count > node_count || node_count - deleted_count != snapshot.live_count {
            return Err(VectorSpaceError::CorruptArtifact(
                "HNSW live and deleted counts are inconsistent",
            ));
        }

        let mut id_to_internal = HashMap::with_capacity(snapshot.live_count);
        for (index, id) in snapshot.external_ids.iter().enumerate() {
            let vector_start = index * snapshot.dimensions;
            let vector = &snapshot.vectors[vector_start..vector_start + snapshot.dimensions];
            if vector.iter().any(|value| !value.is_finite()) {
                return Err(VectorSpaceError::CorruptArtifact(
                    "HNSW vector contains a non-finite value",
                ));
            }
            let norm_squared = dot(vector, vector);
            if norm_squared != 0.0 && (norm_squared - 1.0).abs() > 1e-4 {
                return Err(VectorSpaceError::CorruptArtifact(
                    "HNSW vector is not normalized",
                ));
            }
            if snapshot.neighbors[index].len() != snapshot.levels[index] + 1 {
                return Err(VectorSpaceError::CorruptArtifact(
                    "HNSW neighbor levels do not match node level",
                ));
            }
            if !snapshot.deleted[index] && id_to_internal.insert(id.clone(), index as u32).is_some()
            {
                return Err(VectorSpaceError::CorruptArtifact(
                    "HNSW contains duplicate active external IDs",
                ));
            }
        }

        for (index, levels) in snapshot.neighbors.iter().enumerate() {
            for (level, links) in levels.iter().enumerate() {
                if links.len() > snapshot.config.m {
                    return Err(VectorSpaceError::CorruptArtifact(
                        "HNSW neighbor list exceeds configured M",
                    ));
                }
                let mut unique_links = links.clone();
                unique_links.sort_unstable();
                if unique_links.windows(2).any(|pair| pair[0] == pair[1]) {
                    return Err(VectorSpaceError::CorruptArtifact(
                        "HNSW neighbor list contains duplicates",
                    ));
                }
                for neighbor in links {
                    let neighbor = *neighbor as usize;
                    if neighbor >= node_count {
                        return Err(VectorSpaceError::CorruptArtifact(
                            "HNSW neighbor ID is out of bounds",
                        ));
                    }
                    if neighbor == index {
                        return Err(VectorSpaceError::CorruptArtifact(
                            "HNSW node links to itself",
                        ));
                    }
                    if snapshot.levels[neighbor] < level {
                        return Err(VectorSpaceError::CorruptArtifact(
                            "HNSW neighbor does not exist at linked level",
                        ));
                    }
                }
            }
        }

        let highest_live_level = snapshot
            .levels
            .iter()
            .enumerate()
            .filter(|(index, _)| !snapshot.deleted[*index])
            .map(|(_, level)| *level)
            .max();
        match (
            snapshot.live_count,
            snapshot.entry_point,
            highest_live_level,
        ) {
            (0, None, None) if snapshot.max_level == 0 => {}
            (0, _, _) => {
                return Err(VectorSpaceError::CorruptArtifact(
                    "empty HNSW index has entry metadata",
                ));
            }
            (_, Some(entry), Some(highest_level))
                if (entry as usize) < node_count
                    && !snapshot.deleted[entry as usize]
                    && snapshot.levels[entry as usize] == highest_level
                    && snapshot.max_level == highest_level => {}
            _ => {
                return Err(VectorSpaceError::CorruptArtifact(
                    "HNSW entry point or max level is invalid",
                ));
            }
        }

        Ok(Self {
            dimensions: snapshot.dimensions,
            config: snapshot.config,
            external_ids: snapshot.external_ids,
            id_to_internal,
            vectors: snapshot.vectors,
            levels: snapshot.levels,
            neighbors: snapshot.neighbors,
            deleted: snapshot.deleted,
            live_count: snapshot.live_count,
            entry_point: snapshot.entry_point,
            max_level: snapshot.max_level,
        })
    }

    fn rebuild_threshold(&self) -> usize {
        (self.external_ids.len() / 4).max(8)
    }

    fn compact_if_needed(&mut self) {
        if self.external_ids.len() - self.live_count >= self.rebuild_threshold() {
            self.rebuild();
        }
    }

    fn rebuild(&mut self) {
        let active_entries = self
            .external_ids
            .iter()
            .enumerate()
            .filter(|(index, _)| !self.deleted[*index])
            .map(|(index, id)| (id.clone(), self.vector(index as u32).to_vec()))
            .collect::<Vec<_>>();
        self.external_ids.clear();
        self.id_to_internal.clear();
        self.vectors.clear();
        self.levels.clear();
        self.neighbors.clear();
        self.deleted.clear();
        self.live_count = 0;
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
    pub tombstones: usize,
    pub estimated_memory_bytes: usize,
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

#[derive(Debug, Serialize, Deserialize)]
struct RegistryArtifact {
    format_version: u32,
    source_generation: u64,
    hnsw_indexes: BTreeMap<String, HnswArtifactIndex>,
    exact_euclidean_indexes: BTreeMap<String, ExactEuclideanArtifactIndex>,
}

#[derive(Debug, Serialize, Deserialize)]
struct HnswArtifactIndex {
    dimensions: usize,
    config: HnswConfig,
    generation: u64,
    external_ids: Vec<String>,
    vectors: Vec<f32>,
    levels: Vec<usize>,
    neighbors: Vec<Vec<Vec<u32>>>,
    deleted: Vec<bool>,
    live_count: usize,
    entry_point: Option<u32>,
    max_level: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct ExactEuclideanArtifactIndex {
    dimensions: usize,
    generation: u64,
    entries: BTreeMap<String, Vec<f32>>,
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

/// A validated registry artifact paired with the committed storage revision
/// from which it was derived.
#[derive(Debug)]
pub struct LoadedRegistryArtifact {
    pub registry: HnswRegistry,
    pub source_generation: u64,
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

    pub fn drop_index(&self, name: &str) -> Result<(), VectorSpaceError> {
        if self.indexes.write().remove(name).is_some() {
            return Ok(());
        }
        if self.exact_euclidean_indexes.write().remove(name).is_some() {
            return Ok(());
        }
        Err(VectorSpaceError::IndexNotFound(name.to_string()))
    }

    pub fn status(&self, name: &str) -> Result<HnswIndexStatus, VectorSpaceError> {
        let indexes = self.indexes.read();
        if let Some(managed) = indexes.get(name) {
            return Ok(HnswIndexStatus {
                dimensions: managed.index.dimensions,
                generation: managed.generation,
                strategy: managed.index.metric(),
                ready: true,
                tombstones: managed.index.external_ids.len() - managed.index.live_count,
                estimated_memory_bytes: managed.index.estimated_memory_bytes(),
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
            tombstones: 0,
            estimated_memory_bytes: managed
                .entries
                .iter()
                .map(|(id, vector)| {
                    std::mem::size_of::<String>()
                        + id.capacity()
                        + vector.capacity() * std::mem::size_of::<f32>()
                })
                .sum(),
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

    /// Rebuild an HNSW index to remove accumulated tombstones.
    ///
    /// Returns `true` only when a rebuild was necessary. Exact Euclidean
    /// indexes remove entries eagerly and therefore never require compaction.
    pub fn compact(&self, name: &str) -> Result<bool, VectorSpaceError> {
        let mut indexes = self.indexes.write();
        if let Some(managed) = indexes.get_mut(name) {
            if managed.index.compact() {
                managed.generation = managed.generation.saturating_add(1);
                return Ok(true);
            }
            return Ok(false);
        }
        if self.exact_euclidean_indexes.read().contains_key(name) {
            return Ok(false);
        }
        Err(VectorSpaceError::IndexNotFound(name.to_string()))
    }

    pub fn knn(
        &self,
        name: &str,
        query: &[f32],
        k: usize,
    ) -> Result<(Vec<(String, f32)>, HnswSearchStats), VectorSpaceError> {
        self.knn_cancellable(name, query, k, None)
    }

    pub fn knn_with_cancellation(
        &self,
        name: &str,
        query: &[f32],
        k: usize,
        cancellation: &RequestCancellation,
    ) -> Result<(Vec<(String, f32)>, HnswSearchStats), VectorSpaceError> {
        self.knn_cancellable(name, query, k, Some(cancellation))
    }

    fn knn_cancellable(
        &self,
        name: &str,
        query: &[f32],
        k: usize,
        cancellation: Option<&RequestCancellation>,
    ) -> Result<(Vec<(String, f32)>, HnswSearchStats), VectorSpaceError> {
        let indexes = self.indexes.read();
        if let Some(managed) = indexes.get(name) {
            return managed.index.knn_with_cancellation(query, k, cancellation);
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
        let mut scores = Vec::with_capacity(managed.entries.len());
        for (position, (id, vector)) in managed.entries.iter().enumerate() {
            if position & 0xFF == 0 && cancellation.is_some_and(RequestCancellation::is_cancelled) {
                return Err(VectorSpaceError::RequestCancelled);
            }
            scores.push((id.clone(), euclidean_score(query, vector)));
        }
        scores.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
        scores.truncate(k);
        let returned_candidates = scores.len();
        Ok((
            scores,
            HnswSearchStats {
                visited_nodes: managed.entries.len(),
                returned_candidates,
                exact_scored_candidates: managed.entries.len(),
            },
        ))
    }

    /// Persist the dense HNSW graph and exact Euclidean index state.
    pub fn save_artifact(&self, path: impl AsRef<Path>) -> Result<(), VectorSpaceError> {
        self.save_artifact_at_generation(path, 0)
    }

    pub fn save_artifact_at_generation(
        &self,
        path: impl AsRef<Path>,
        source_generation: u64,
    ) -> Result<(), VectorSpaceError> {
        let hnsw_indexes = self
            .indexes
            .read()
            .iter()
            .map(|(name, managed)| {
                (
                    name.clone(),
                    HnswArtifactIndex {
                        dimensions: managed.index.dimensions,
                        config: managed.index.config,
                        generation: managed.generation,
                        external_ids: managed.index.external_ids.clone(),
                        vectors: managed.index.vectors.clone(),
                        levels: managed.index.levels.clone(),
                        neighbors: managed.index.neighbors.clone(),
                        deleted: managed.index.deleted.clone(),
                        live_count: managed.index.live_count,
                        entry_point: managed.index.entry_point,
                        max_level: managed.index.max_level,
                    },
                )
            })
            .collect();
        let exact_euclidean_indexes = self
            .exact_euclidean_indexes
            .read()
            .iter()
            .map(|(name, managed)| {
                (
                    name.clone(),
                    ExactEuclideanArtifactIndex {
                        dimensions: managed.dimensions,
                        generation: managed.generation,
                        entries: managed.entries.clone(),
                    },
                )
            })
            .collect();
        let payload = rmp_serde::to_vec_named(&RegistryArtifact {
            format_version: REGISTRY_ARTIFACT_FORMAT_VERSION,
            source_generation,
            hnsw_indexes,
            exact_euclidean_indexes,
        })
        .map_err(|error| VectorSpaceError::ArtifactSerialization(error.to_string()))?;
        let mut bytes = Vec::with_capacity(REGISTRY_ARTIFACT_MAGIC.len() + payload.len() + 8);
        bytes.extend_from_slice(REGISTRY_ARTIFACT_MAGIC);
        bytes.extend_from_slice(&payload);
        bytes.extend_from_slice(&artifact_checksum(&payload).to_le_bytes());

        let path = path.as_ref();
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let temporary = path.with_extension("tmp");
        fs::write(&temporary, bytes)?;
        fs::rename(temporary, path)?;
        Ok(())
    }

    /// Load a registry artifact without accepting malformed graph state.
    pub fn load_artifact(path: impl AsRef<Path>) -> Result<Self, VectorSpaceError> {
        Ok(Self::load_artifact_with_source_generation(path)?.registry)
    }

    pub fn load_artifact_with_source_generation(
        path: impl AsRef<Path>,
    ) -> Result<LoadedRegistryArtifact, VectorSpaceError> {
        let bytes = fs::read(path)?;
        if bytes.len() < REGISTRY_ARTIFACT_MAGIC.len() + 8
            || !bytes.starts_with(REGISTRY_ARTIFACT_MAGIC)
        {
            return Err(VectorSpaceError::CorruptArtifact(
                "invalid magic or truncated header",
            ));
        }
        let checksum_offset = bytes.len() - 8;
        let payload = &bytes[REGISTRY_ARTIFACT_MAGIC.len()..checksum_offset];
        let expected_checksum = u64::from_le_bytes(
            bytes[checksum_offset..]
                .try_into()
                .map_err(|_| VectorSpaceError::CorruptArtifact("invalid checksum"))?,
        );
        if artifact_checksum(payload) != expected_checksum {
            return Err(VectorSpaceError::CorruptArtifact("checksum mismatch"));
        }
        let artifact: RegistryArtifact = rmp_serde::from_slice(payload)
            .map_err(|_| VectorSpaceError::CorruptArtifact("invalid artifact payload"))?;
        if artifact.format_version != REGISTRY_ARTIFACT_FORMAT_VERSION {
            return Err(VectorSpaceError::UnsupportedArtifactFormat {
                expected: REGISTRY_ARTIFACT_FORMAT_VERSION,
                actual: artifact.format_version,
            });
        }

        let registry = Self::new();
        for (name, snapshot) in artifact.hnsw_indexes {
            let generation = snapshot.generation;
            let index = HnswIndex::from_artifact(snapshot)?;
            registry
                .indexes
                .write()
                .insert(name, ManagedHnswIndex { index, generation });
        }
        for (name, snapshot) in artifact.exact_euclidean_indexes {
            registry.create_exact_euclidean_index(&name, snapshot.dimensions)?;
            for (id, vector) in snapshot.entries {
                registry.upsert(&name, id, vector)?;
            }
            registry
                .exact_euclidean_indexes
                .write()
                .get_mut(&name)
                .expect("created exact Euclidean index is present")
                .generation = snapshot.generation;
        }
        Ok(LoadedRegistryArtifact {
            registry,
            source_generation: artifact.source_generation,
        })
    }

    pub fn index_names(&self) -> Vec<String> {
        let mut names = self.indexes.read().keys().cloned().collect::<Vec<_>>();
        names.extend(self.exact_euclidean_indexes.read().keys().cloned());
        names.sort();
        names
    }
}

fn artifact_checksum(payload: &[u8]) -> u64 {
    let mut hasher = fnv::FnvHasher::default();
    hasher.write(payload);
    hasher.finish()
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

fn deterministic_level(id: &str, m: usize) -> usize {
    let mut state = 0xcbf2_9ce4_8422_2325_u64;
    for byte in id.bytes() {
        state ^= u64::from(byte);
        state = state.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let mut level = 0;
    while level < 16 && state.is_multiple_of(m as u64) {
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
        self.entries.insert(id.into(), normalize_vector(&vector));
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
        if k == 0 {
            return Ok(Vec::new());
        }
        let query = normalize_vector(query);
        let mut best = BinaryHeap::with_capacity(k.saturating_add(1));
        for (id, vector) in &self.entries {
            best.push(ExactScoredEntry {
                score: dot(&query, vector),
                id,
            });
            if best.len() > k {
                best.pop();
            }
        }
        let mut scores = best
            .into_iter()
            .map(|entry| (entry.id.to_string(), entry.score))
            .collect::<Vec<_>>();
        scores.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
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
    copperdb_simd::dot_f32(a, b).expect("vector dimensions must match")
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
    fn vector_file_store_normalizes_reopens_and_preserves_deletes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("vectors.bin");
        let mut store = VectorFileStore::open(&path, 2).unwrap();
        assert!(store.is_empty());
        assert_eq!(store.dimensions(), 2);
        assert!(matches!(
            store.upsert("invalid", &[1.0]),
            Err(VectorSpaceError::DimensionMismatch { .. })
        ));

        store.upsert("updated", &[3.0, 4.0]).unwrap();
        let normalized = store.get("updated").unwrap().unwrap();
        assert!((normalized[0] - 0.6).abs() < 1e-6);
        assert!((normalized[1] - 0.8).abs() < 1e-6);

        store.upsert("updated", &[0.0, 2.0]).unwrap();
        store.upsert("retained", &[0.0, 0.0]).unwrap();
        assert!(store.remove("updated").unwrap());
        assert!(!store.remove("missing").unwrap());
        drop(store);

        let reopened = VectorFileStore::open(&path, 2).unwrap();
        assert_eq!(reopened.len(), 1);
        assert_eq!(reopened.get("updated").unwrap(), None);
        assert_eq!(reopened.get("retained").unwrap(), Some(vec![0.0, 0.0]));
    }

    #[test]
    fn vector_file_store_batches_durable_upserts() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("vectors.bin");
        let mut store = VectorFileStore::open(&path, 2).unwrap();
        store
            .upsert_batch([
                ("first".to_string(), vec![3.0, 4.0]),
                ("second".to_string(), vec![0.0, 2.0]),
            ])
            .unwrap();
        drop(store);

        let reopened = VectorFileStore::open(&path, 2).unwrap();
        assert_eq!(reopened.len(), 2);
        assert_eq!(reopened.get("first").unwrap(), Some(vec![0.6, 0.8]));
        assert_eq!(reopened.get("second").unwrap(), Some(vec![0.0, 1.0]));
    }

    #[test]
    fn vector_file_store_exactly_reranks_candidates() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("vectors.bin");
        let mut store = VectorFileStore::open(&path, 2).unwrap();
        store.upsert("zeta", &[1.0, 1.0]).unwrap();
        store.upsert("alpha", &[1.0, 1.0]).unwrap();
        store.upsert("best", &[1.0, 0.0]).unwrap();

        assert_eq!(
            store
                .score_candidates(&[1.0, 0.0], ["zeta", "missing", "alpha", "best"], 3)
                .unwrap()
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>(),
            vec!["best", "alpha", "zeta"]
        );
        assert!(matches!(
            store.score_candidates(&[1.0], ["best"], 1),
            Err(VectorSpaceError::DimensionMismatch { .. })
        ));
    }

    #[test]
    fn vector_file_store_rejects_corrupt_headers() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("vectors.bin");
        fs::write(&path, b"not a vector file").unwrap();
        assert!(matches!(
            VectorFileStore::open(&path, 2),
            Err(VectorSpaceError::CorruptVectorFileStore(_))
        ));
    }

    #[test]
    fn vector_file_store_rejects_oversized_record_ids() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("vectors.bin");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(VECTOR_FILE_STORE_MAGIC);
        bytes.extend_from_slice(&VECTOR_FILE_STORE_FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&2_u32.to_le_bytes());
        bytes.push(VECTOR_FILE_STORE_UPSERT);
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        fs::write(&path, bytes).unwrap();

        assert!(matches!(
            VectorFileStore::open(&path, 2),
            Err(VectorSpaceError::CorruptVectorFileStore(_))
        ));
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
    fn exact_cosine_normalizes_mutations_and_handles_zero_vectors() {
        let mut space = VectorSpace::new("test", 2);
        space.insert("updated", vec![3.0, 0.0]).unwrap();
        space.insert("updated", vec![0.0, 5.0]).unwrap();
        space.insert("zero", vec![0.0, 0.0]).unwrap();

        assert!(space.knn(&[0.0, 7.0], 0).unwrap().is_empty());
        let results = space.knn(&[0.0, 7.0], 2).unwrap();
        assert_eq!(results[0].0, "updated");
        assert!((results[0].1 - 1.0).abs() < 1e-6);
        assert_eq!(results[1], ("zero".to_string(), 0.0));
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
        assert_eq!(
            results.iter().map(|(id, _)| id).collect::<Vec<_>>(),
            oracle.iter().map(|(id, _)| id).collect::<Vec<_>>()
        );
        assert!(results
            .iter()
            .zip(oracle.iter())
            .all(|((_, actual), (_, expected))| (actual - expected).abs() < 1e-6));
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
    fn hnsw_tombstones_are_filtered_and_upserts_append_a_fresh_dense_node() {
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

        let other_internal = index.id_to_internal["other"];
        let old_updated_internal = index.id_to_internal["updated"];
        let node_count = index.external_ids.len();
        index.upsert("updated", vec![1.0, 0.0]).unwrap();
        assert_eq!(index.id_to_internal["other"], other_internal);
        assert!(index.deleted[old_updated_internal as usize]);
        assert_eq!(index.id_to_internal["updated"] as usize, node_count);
        assert_eq!(index.external_ids.len(), node_count + 1);
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
    fn registry_explicitly_compacts_below_threshold_tombstones() {
        let registry = HnswRegistry::new();
        registry
            .create_index("documents.embedding", 2, HnswConfig::default())
            .unwrap();
        registry
            .upsert("documents.embedding", "removed", vec![1.0, 0.0])
            .unwrap();
        registry
            .upsert("documents.embedding", "retained", vec![0.0, 1.0])
            .unwrap();
        registry.remove("documents.embedding", "removed").unwrap();

        let before = registry.status("documents.embedding").unwrap();
        assert_eq!(before.tombstones, 1);
        assert!(registry.compact("documents.embedding").unwrap());

        let after = registry.status("documents.embedding").unwrap();
        assert_eq!(after.tombstones, 0);
        assert_eq!(after.generation, before.generation + 1);
        assert!(!registry.compact("documents.embedding").unwrap());
        assert_eq!(
            registry
                .knn("documents.embedding", &[1.0, 0.0], 2)
                .unwrap()
                .0
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>(),
            vec!["retained"]
        );
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
        assert!(before_query.estimated_memory_bytes > 0);
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
        assert!(
            registry
                .status("documents.embedding")
                .unwrap()
                .estimated_memory_bytes
                > before_query.estimated_memory_bytes
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
        assert!(status.estimated_memory_bytes > 0);
        assert!(registry
            .upsert("documents.embedding", "invalid", vec![1.0])
            .is_err());
    }

    #[test]
    fn registry_artifact_round_trips_and_rejects_corruption() {
        let directory = tempfile::tempdir().unwrap();
        let artifact = directory.path().join("vectors.artifact");
        let registry = HnswRegistry::new();
        registry
            .create_index("documents.cosine", 2, HnswConfig::default())
            .unwrap();
        registry
            .create_exact_euclidean_index("documents.euclidean", 2)
            .unwrap();
        registry
            .upsert("documents.cosine", "one", vec![1.0, 0.0])
            .unwrap();
        registry
            .upsert("documents.cosine", "two", vec![0.0, 1.0])
            .unwrap();
        registry
            .upsert("documents.cosine", "removed", vec![-1.0, 0.0])
            .unwrap();
        registry.remove("documents.cosine", "removed").unwrap();
        registry
            .upsert("documents.euclidean", "two", vec![0.0, 1.0])
            .unwrap();
        let expected_graph = registry
            .indexes
            .read()
            .get("documents.cosine")
            .unwrap()
            .index
            .clone();
        registry.save_artifact_at_generation(&artifact, 42).unwrap();

        let loaded = HnswRegistry::load_artifact_with_source_generation(&artifact).unwrap();
        assert_eq!(loaded.source_generation, 42);
        let restored = loaded.registry;
        assert_eq!(restored.status("documents.cosine").unwrap().generation, 4);
        assert_eq!(
            restored
                .indexes
                .read()
                .get("documents.cosine")
                .unwrap()
                .index,
            expected_graph,
            "loading must install the persisted dense topology without rebuilding it"
        );
        assert_eq!(
            restored.knn("documents.cosine", &[1.0, 0.0], 1).unwrap().0[0].0,
            "one"
        );
        assert_eq!(
            restored.status("documents.euclidean").unwrap().strategy,
            SimilarityMetric::ExactEuclidean
        );

        fs::write(&artifact, b"corrupt").unwrap();
        assert!(matches!(
            HnswRegistry::load_artifact(&artifact),
            Err(VectorSpaceError::CorruptArtifact(_))
        ));
    }

    #[test]
    fn registry_artifact_rejects_malformed_hnsw_topology() {
        let directory = tempfile::tempdir().unwrap();
        let artifact_path = directory.path().join("vectors.artifact");
        let registry = HnswRegistry::new();
        registry
            .create_index("documents.cosine", 2, HnswConfig::default())
            .unwrap();
        registry
            .upsert("documents.cosine", "one", vec![1.0, 0.0])
            .unwrap();
        registry
            .upsert("documents.cosine", "two", vec![0.0, 1.0])
            .unwrap();
        registry.save_artifact(&artifact_path).unwrap();

        let bytes = fs::read(&artifact_path).unwrap();
        let checksum_offset = bytes.len() - 8;
        let payload = &bytes[REGISTRY_ARTIFACT_MAGIC.len()..checksum_offset];
        let mut artifact: RegistryArtifact = rmp_serde::from_slice(payload).unwrap();
        artifact
            .hnsw_indexes
            .get_mut("documents.cosine")
            .unwrap()
            .neighbors[0][0] = vec![u32::MAX];
        let payload = rmp_serde::to_vec_named(&artifact).unwrap();
        let mut malformed = Vec::new();
        malformed.extend_from_slice(REGISTRY_ARTIFACT_MAGIC);
        malformed.extend_from_slice(&payload);
        malformed.extend_from_slice(&artifact_checksum(&payload).to_le_bytes());
        fs::write(&artifact_path, malformed).unwrap();

        assert!(matches!(
            HnswRegistry::load_artifact(&artifact_path),
            Err(VectorSpaceError::CorruptArtifact(
                "HNSW neighbor ID is out of bounds"
            ))
        ));
    }
}
