use super::*;
use copperdb_vectorspace::{
    HnswConfig, HnswIndexStatus, HnswRegistry, SimilarityMetric, VectorFileStore, VectorSpaceError,
};
use std::{path::PathBuf, sync::Mutex};

const VECTOR_REGISTRY_ARTIFACT_FILE: &str = "vectors.hnsw";
const VECTOR_FILE_STORE_DIRECTORY: &str = "vectors";
const HNSW_CANDIDATE_MULTIPLIER: usize = 20;
const HNSW_MIN_CANDIDATES: usize = 200;
const HNSW_MAX_CANDIDATES: usize = 5_000;
const HNSW_LEXICAL_SEED_MAX_TERMS: usize = 256;
const HNSW_LEXICAL_SEED_PER_TERM: usize = 8;

#[derive(Debug, Clone)]
struct VectorIndexBinding {
    name: String,
    label: String,
    property: String,
    entity_type: IndexEntityType,
    dimensions: usize,
}

/// Per-database owner for schema-declared cosine HNSW indexes.
///
/// Construction is an explicit lifecycle operation performed by the engine.
/// Query paths only read the populated registry.
#[derive(Debug)]
pub(crate) struct VectorIndexManager {
    registry: Arc<HnswRegistry>,
    artifact_path: Option<PathBuf>,
    file_store_directory: Option<PathBuf>,
    file_stores: Mutex<BTreeMap<String, VectorFileStore>>,
    file_store_bindings: Mutex<Vec<VectorIndexBinding>>,
}

impl VectorIndexManager {
    pub(crate) fn build(storage: &StorageEngine) -> Result<Self, CopperDbError> {
        let mut registry = Arc::new(HnswRegistry::new());
        let mut bindings = Vec::new();

        for definition in storage.load_index_definitions()? {
            if definition.kind != IndexKind::Vector {
                continue;
            }
            let Some(property) = definition.properties.first() else {
                return Err(CopperDbError::Config(format!(
                    "vector index {} is missing a target property",
                    definition.name
                )));
            };
            let options = storage.load_index_options(&definition.name)?;
            let dimensions = vector_index_dimensions(&definition.name, options.as_ref())?;
            match vector_similarity(options.as_ref()) {
                Some("euclidean") => registry
                    .create_exact_euclidean_index(&definition.name, dimensions)
                    .map_err(vector_error)?,
                Some("cosine") | None => registry
                    .create_index(&definition.name, dimensions, HnswConfig::default())
                    .map_err(vector_error)?,
                Some(similarity) => {
                    return Err(CopperDbError::Config(format!(
                        "vector index {} uses unsupported similarity function {similarity}",
                        definition.name
                    )));
                }
            }
            bindings.push(VectorIndexBinding {
                name: definition.name,
                label: definition.label,
                property: property.clone(),
                entity_type: definition.entity_type,
                dimensions,
            });
        }

        let artifact_path = storage
            .data_dir()
            .map(|path| path.join(VECTOR_REGISTRY_ARTIFACT_FILE));
        let artifact_is_current = artifact_path
            .as_ref()
            .filter(|path| path.exists())
            .and_then(|path| match HnswRegistry::load_artifact_with_source_generation(path) {
                Ok(artifact) => Some(artifact),
                Err(error) => {
                    tracing::warn!(path = %path.display(), %error, "ignoring invalid vector registry artifact");
                    None
                }
            })
            .filter(|artifact| {
                match storage.wal_applied_sequence() {
                    Ok(generation) => artifact.source_generation == generation,
                    Err(error) => {
                        tracing::warn!(%error, "unable to validate vector registry artifact generation");
                        false
                    }
                }
            })
            .filter(|artifact| registry_matches_declared_indexes(&artifact.registry, &registry));

        if let Some(artifact) = artifact_is_current {
            registry = Arc::new(artifact.registry);
        } else {
            for binding in &bindings {
                match binding.entity_type {
                    IndexEntityType::Node => {
                        let seed_ids = lexical_seed_node_ids(storage, &binding.label)?;
                        let mut seeded_vectors = Vec::new();
                        let mut other_vectors = Vec::new();
                        for node in storage.all_node_records()? {
                            let matches_label = binding.label.is_empty()
                                || node.labels.iter().any(|label| label == &binding.label);
                            if matches_label {
                                if let Some(vector) =
                                    node_vector_for_property(&node, &binding.property)
                                {
                                    let entry = (node.id, vector);
                                    if seed_ids.contains(&entry.0) {
                                        seeded_vectors.push(entry);
                                    } else {
                                        other_vectors.push(entry);
                                    }
                                }
                            }
                        }
                        for (id, vector) in seeded_vectors.into_iter().chain(other_vectors) {
                            registry
                                .upsert(&binding.name, id, vector)
                                .map_err(vector_error)?;
                        }
                    }
                    IndexEntityType::Relationship => {
                        let edges = if binding.label.is_empty() {
                            storage.all_edges()?
                        } else {
                            storage.get_edges_by_type(&binding.label)?
                        };
                        for edge in edges {
                            if let Some(vector) = edge_vector_for_property(&edge, &binding.property)
                            {
                                registry
                                    .upsert(&binding.name, &edge.id, vector)
                                    .map_err(vector_error)?;
                            }
                        }
                    }
                }
            }
        }
        let manager = Self {
            registry,
            artifact_path,
            file_store_directory: storage
                .data_dir()
                .map(|path| path.join(VECTOR_FILE_STORE_DIRECTORY)),
            file_stores: Mutex::new(BTreeMap::new()),
            file_store_bindings: Mutex::new(bindings),
        };
        manager.rebuild_file_stores(storage);
        manager.persist_artifact(storage);
        Ok(manager)
    }

    pub(crate) fn status(&self, name: &str) -> Result<HnswIndexStatus, CopperDbError> {
        self.registry.status(name).map_err(vector_error)
    }

    pub(crate) fn initialized_index_count(&self) -> Option<usize> {
        self.file_store_bindings
            .lock()
            .ok()
            .as_deref()
            .map(Vec::len)
    }

    pub(crate) fn query_node_indexes(
        &self,
        cancellation: &copperdb_util::RequestCancellation,
        query: &[f32],
        limit: usize,
        min_score: f32,
        labels: &[String],
        index_names: &[String],
    ) -> Result<Vec<(String, f32, String)>, CopperDbError> {
        let bindings = self
            .file_store_bindings
            .lock()
            .map_err(|_| CopperDbError::Config("vector index bindings lock poisoned".into()))?
            .iter()
            .filter(|binding| {
                binding.entity_type == IndexEntityType::Node
                    && binding.dimensions == query.len()
                    && (labels.is_empty() || labels.contains(&binding.label))
                    && (index_names.is_empty() || index_names.contains(&binding.name))
            })
            .cloned()
            .collect::<Vec<_>>();
        if bindings.is_empty() {
            return Err(CopperDbError::Config(format!(
                "no node vector index configured for {} query dimensions",
                query.len()
            )));
        }

        let mut best_by_id = BTreeMap::<String, (f32, String)>::new();
        for binding in bindings {
            let (matches, _) = self
                .query(cancellation, &binding.name, query, limit)
                .map_err(vector_error)?;
            for (id, score) in matches {
                if score < min_score {
                    continue;
                }
                let replace = best_by_id
                    .get(&id)
                    .is_none_or(|(current_score, current_label)| {
                        score > *current_score
                            || (score == *current_score && binding.label < *current_label)
                    });
                if replace {
                    best_by_id.insert(id, (score, binding.label.clone()));
                }
            }
        }

        let mut matches = best_by_id
            .into_iter()
            .map(|(id, (score, label))| (id, score, label))
            .collect::<Vec<_>>();
        matches.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
        matches.truncate(limit);
        Ok(matches)
    }

    pub(crate) fn compact(
        &self,
        storage: &StorageEngine,
        name: &str,
    ) -> Result<bool, CopperDbError> {
        let compacted = self.registry.compact(name).map_err(vector_error)?;
        if compacted {
            self.persist_artifact(storage);
        }
        Ok(compacted)
    }

    pub(crate) fn registry(&self) -> Arc<HnswRegistry> {
        Arc::clone(&self.registry)
    }

    pub(crate) fn query_callback(self: &Arc<Self>) -> copperdb_eval::VectorIndexQuery {
        let manager = Arc::downgrade(self);
        Arc::new(move |cancellation, name, query, limit| {
            let Some(manager) = manager.upgrade() else {
                return Err(VectorSpaceError::IndexNotFound(name.to_string()));
            };
            manager.query(cancellation, name, query, limit)
        })
    }

    fn query(
        &self,
        cancellation: &copperdb_util::RequestCancellation,
        name: &str,
        query: &[f32],
        limit: usize,
    ) -> Result<(Vec<(String, f32)>, copperdb_vectorspace::HnswSearchStats), VectorSpaceError> {
        let status = self.registry.status(name)?;
        if status.strategy != SimilarityMetric::HnswCosine || limit == 0 {
            return self
                .registry
                .knn_with_cancellation(name, query, limit, cancellation);
        }
        let candidate_limit = limit
            .saturating_mul(HNSW_CANDIDATE_MULTIPLIER)
            .clamp(HNSW_MIN_CANDIDATES, HNSW_MAX_CANDIDATES);
        let (candidates, mut stats) =
            self.registry
                .knn_with_cancellation(name, query, candidate_limit, cancellation)?;
        let stores = self
            .file_stores
            .lock()
            .map_err(|_| VectorSpaceError::IndexNotFound(name.to_string()))?;
        let Some(store) = stores.get(name) else {
            let matches = candidates.into_iter().take(limit).collect::<Vec<_>>();
            stats.returned_candidates = matches.len();
            return Ok((matches, stats));
        };
        let candidate_ids = candidates
            .iter()
            .map(|(id, _)| id.as_str())
            .collect::<Vec<_>>();
        let exact_scored_candidates = candidate_ids.len();
        let matches =
            store.score_candidates_cancellable(query, candidate_ids, limit, cancellation)?;
        stats.returned_candidates = matches.len();
        stats.exact_scored_candidates = exact_scored_candidates;
        Ok((matches, stats))
    }

    pub(crate) fn enable_persistence(self: &Arc<Self>, storage: &Arc<StorageEngine>) {
        let manager = Arc::downgrade(self);
        let weak_storage = Arc::downgrade(storage);
        storage.on_commit_completed(Arc::new(move || {
            let (Some(manager), Some(storage)) = (manager.upgrade(), weak_storage.upgrade()) else {
                return;
            };
            manager.persist_artifact(&storage);
        }));
        let manager = Arc::downgrade(self);
        storage.on_node_created(Arc::new(move |node| {
            if let Some(manager) = manager.upgrade() {
                manager.maintain_node_file_stores(&node);
            }
        }));
        let manager = Arc::downgrade(self);
        storage.on_node_updated(Arc::new(move |node| {
            if let Some(manager) = manager.upgrade() {
                manager.maintain_node_file_stores(&node);
            }
        }));
        let manager = Arc::downgrade(self);
        storage.on_node_deleted(Arc::new(move |id| {
            if let Some(manager) = manager.upgrade() {
                manager.remove_file_store_entry(IndexEntityType::Node, &id);
            }
        }));
        let manager = Arc::downgrade(self);
        storage.on_edge_created(Arc::new(move |edge| {
            if let Some(manager) = manager.upgrade() {
                manager.maintain_edge_file_stores(&edge);
            }
        }));
        let manager = Arc::downgrade(self);
        storage.on_edge_updated(Arc::new(move |edge| {
            if let Some(manager) = manager.upgrade() {
                manager.maintain_edge_file_stores(&edge);
            }
        }));
        let manager = Arc::downgrade(self);
        storage.on_edge_deleted(Arc::new(move |id| {
            if let Some(manager) = manager.upgrade() {
                manager.remove_file_store_entry(IndexEntityType::Relationship, &id);
            }
        }));
    }

    pub(crate) fn artifact_refresh_callback(
        self: &Arc<Self>,
        storage: &Arc<StorageEngine>,
    ) -> Arc<dyn Fn() + Send + Sync> {
        let manager = Arc::downgrade(self);
        let storage = Arc::downgrade(storage);
        Arc::new(move || {
            let (Some(manager), Some(storage)) = (manager.upgrade(), storage.upgrade()) else {
                return;
            };
            manager.refresh_file_store_bindings(&storage);
            manager.persist_artifact(&storage);
        })
    }

    fn refresh_file_store_bindings(&self, storage: &StorageEngine) {
        let bindings = match file_store_bindings(storage, &self.registry) {
            Ok(bindings) => bindings,
            Err(error) => {
                tracing::warn!(%error, "failed to load vector file store bindings");
                return;
            }
        };
        match self.file_store_bindings.lock() {
            Ok(mut current) => *current = bindings,
            Err(_) => {
                tracing::warn!("vector file store bindings lock is poisoned");
                return;
            }
        }
        self.rebuild_file_stores(storage);
    }

    fn rebuild_file_stores(&self, storage: &StorageEngine) {
        let Some(directory) = self.file_store_directory.as_ref() else {
            return;
        };
        let bindings = match self.file_store_bindings.lock() {
            Ok(bindings) => bindings.clone(),
            Err(_) => {
                tracing::warn!("vector file store bindings lock is poisoned");
                return;
            }
        };
        let old_stores = match self.file_stores.lock() {
            Ok(mut stores) => std::mem::take(&mut *stores),
            Err(_) => {
                tracing::warn!("vector file store lock is poisoned");
                return;
            }
        };
        for store in old_stores.into_values() {
            if let Err(error) = std::fs::remove_file(store.path()) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(path = %store.path().display(), %error, "failed to discard stale vector file store");
                }
            }
        }

        let mut stores = BTreeMap::new();
        for binding in bindings {
            let path = vector_file_store_path(directory, &binding.name);
            if let Err(error) = std::fs::remove_file(&path) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(index = %binding.name, path = %path.display(), %error, "failed to reset vector file store before rebuild");
                    continue;
                }
            }
            let mut store = match VectorFileStore::open(&path, binding.dimensions) {
                Ok(store) => store,
                Err(error) => {
                    tracing::warn!(index = %binding.name, path = %path.display(), %error, "failed to create vector file store");
                    continue;
                }
            };
            if let Err(error) = populate_file_store(storage, &binding, &mut store) {
                tracing::warn!(index = %binding.name, %error, "failed to rebuild vector file store");
                let _ = std::fs::remove_file(store.path());
                continue;
            }
            stores.insert(binding.name.clone(), store);
        }
        if let Ok(mut current) = self.file_stores.lock() {
            *current = stores;
        } else {
            tracing::warn!("vector file store lock is poisoned");
        }
    }

    fn maintain_node_file_stores(&self, node: &NodeRecord) {
        self.maintain_file_stores(
            IndexEntityType::Node,
            &node.id,
            &node.labels,
            None,
            |binding| node_vector_for_property(node, &binding.property),
        );
    }

    fn maintain_edge_file_stores(&self, edge: &EdgeRecord) {
        self.maintain_file_stores(
            IndexEntityType::Relationship,
            &edge.id,
            &[],
            Some(&edge.edge_type),
            |binding| edge_vector_for_property(edge, &binding.property),
        );
    }

    fn maintain_file_stores<F>(
        &self,
        entity_type: IndexEntityType,
        id: &str,
        labels: &[String],
        edge_type: Option<&str>,
        vector_for_binding: F,
    ) where
        F: Fn(&VectorIndexBinding) -> Option<Vec<f32>>,
    {
        let bindings = match self.file_store_bindings.lock() {
            Ok(bindings) => bindings.clone(),
            Err(_) => {
                tracing::warn!("vector file store bindings lock is poisoned");
                return;
            }
        };
        let mut stores = match self.file_stores.lock() {
            Ok(stores) => stores,
            Err(_) => {
                tracing::warn!("vector file store lock is poisoned");
                return;
            }
        };
        for binding in bindings
            .into_iter()
            .filter(|binding| binding.entity_type == entity_type)
        {
            let Some(store) = stores.get_mut(&binding.name) else {
                continue;
            };
            let matches_binding = match entity_type {
                IndexEntityType::Node => {
                    binding.label.is_empty() || labels.iter().any(|label| label == &binding.label)
                }
                IndexEntityType::Relationship => {
                    binding.label.is_empty() || edge_type == Some(binding.label.as_str())
                }
            };
            let result = if matches_binding {
                match vector_for_binding(&binding) {
                    Some(vector) => store.upsert(id, &vector).map(|()| true),
                    None => store.remove(id),
                }
            } else {
                store.remove(id)
            };
            if let Err(error) = result {
                tracing::warn!(index = %binding.name, entity = %id, %error, "failed to maintain vector file store");
            }
        }
    }

    fn remove_file_store_entry(&self, entity_type: IndexEntityType, id: &str) {
        let bindings = match self.file_store_bindings.lock() {
            Ok(bindings) => bindings.clone(),
            Err(_) => {
                tracing::warn!("vector file store bindings lock is poisoned");
                return;
            }
        };
        let mut stores = match self.file_stores.lock() {
            Ok(stores) => stores,
            Err(_) => {
                tracing::warn!("vector file store lock is poisoned");
                return;
            }
        };
        for binding in bindings
            .into_iter()
            .filter(|binding| binding.entity_type == entity_type)
        {
            let Some(store) = stores.get_mut(&binding.name) else {
                continue;
            };
            if let Err(error) = store.remove(id) {
                tracing::warn!(index = %binding.name, entity = %id, %error, "failed to remove vector file store entry");
            }
        }
    }

    fn persist_artifact(&self, storage: &StorageEngine) {
        let Some(path) = self.artifact_path.as_ref() else {
            return;
        };
        let generation = match storage.wal_applied_sequence() {
            Ok(generation) => generation,
            Err(error) => {
                tracing::warn!(%error, "unable to read vector artifact source generation");
                return;
            }
        };
        if let Err(error) = self.registry.save_artifact_at_generation(path, generation) {
            tracing::warn!(path = %path.display(), %error, "failed to persist vector registry artifact");
        }
    }
}

fn file_store_bindings(
    storage: &StorageEngine,
    registry: &HnswRegistry,
) -> Result<Vec<VectorIndexBinding>, CopperDbError> {
    storage
        .load_index_definitions()?
        .into_iter()
        .filter(|definition| definition.kind == IndexKind::Vector)
        .filter_map(|definition| {
            let status = registry.status(&definition.name).ok()?;
            if status.strategy != SimilarityMetric::HnswCosine {
                return None;
            }
            let property = definition.properties.first()?.clone();
            Some(Ok(VectorIndexBinding {
                name: definition.name,
                label: definition.label,
                property,
                entity_type: definition.entity_type,
                dimensions: status.dimensions,
            }))
        })
        .collect()
}

fn lexical_seed_node_ids(
    storage: &StorageEngine,
    label: &str,
) -> Result<std::collections::HashSet<String>, CopperDbError> {
    let mut seed_ids = std::collections::HashSet::new();
    for definition in storage.load_index_definitions()? {
        if definition.kind != IndexKind::FullText
            || definition.entity_type != IndexEntityType::Node
            || definition.label != label
        {
            continue;
        }
        seed_ids.extend(storage.lexical_seed_doc_ids(
            &definition.label,
            &definition.properties,
            HNSW_LEXICAL_SEED_MAX_TERMS,
            HNSW_LEXICAL_SEED_PER_TERM,
        )?);
    }
    Ok(seed_ids)
}

fn vector_file_store_path(directory: &std::path::Path, index_name: &str) -> PathBuf {
    let encoded = index_name
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    directory.join(format!("{encoded}.vec"))
}

fn populate_file_store(
    storage: &StorageEngine,
    binding: &VectorIndexBinding,
    store: &mut VectorFileStore,
) -> Result<(), CopperDbError> {
    let entries = match binding.entity_type {
        IndexEntityType::Node => storage
            .all_node_records()?
            .into_iter()
            .filter(|node| {
                binding.label.is_empty() || node.labels.iter().any(|label| label == &binding.label)
            })
            .filter_map(|node| {
                node_vector_for_property(&node, &binding.property).map(|vector| (node.id, vector))
            })
            .collect::<Vec<_>>(),
        IndexEntityType::Relationship => {
            let edges = if binding.label.is_empty() {
                storage.all_edges()?
            } else {
                storage.get_edges_by_type(&binding.label)?
            };
            edges
                .into_iter()
                .filter_map(|edge| {
                    edge_vector_for_property(&edge, &binding.property)
                        .map(|vector| (edge.id, vector))
                })
                .collect::<Vec<_>>()
        }
    };
    store.upsert_batch(entries).map_err(vector_error)
}

fn registry_matches_declared_indexes(candidate: &HnswRegistry, declared: &HnswRegistry) -> bool {
    if candidate.index_names() != declared.index_names() {
        return false;
    }
    declared.index_names().into_iter().all(|name| {
        let Ok(expected) = declared.status(&name) else {
            return false;
        };
        let Ok(actual) = candidate.status(&name) else {
            return false;
        };
        actual.dimensions == expected.dimensions && actual.strategy == expected.strategy
    })
}

fn vector_index_dimensions(
    index_name: &str,
    options: Option<&HashMap<String, serde_json::Value>>,
) -> Result<usize, CopperDbError> {
    options
        .and_then(|options| options.get("indexConfig"))
        .and_then(serde_json::Value::as_object)
        .and_then(|config| config.get("vector.dimensions"))
        .and_then(serde_json::Value::as_u64)
        .and_then(|dimensions| usize::try_from(dimensions).ok())
        .filter(|dimensions| *dimensions > 0)
        .ok_or_else(|| {
            CopperDbError::Config(format!(
                "vector index {index_name} requires a positive vector.dimensions option"
            ))
        })
}

fn vector_similarity(options: Option<&HashMap<String, serde_json::Value>>) -> Option<&str> {
    options
        .and_then(|options| options.get("indexConfig"))
        .and_then(serde_json::Value::as_object)
        .and_then(|config| config.get("vector.similarity_function"))
        .and_then(serde_json::Value::as_str)
}

fn node_vector_for_property(node: &NodeRecord, property: &str) -> Option<Vec<f32>> {
    if let Some(vector) = node
        .named_embeddings
        .get(property)
        .filter(|vector| !vector.is_empty())
    {
        return Some(vector.clone());
    }
    if let Some(vector) = node.properties.get(property).and_then(value_to_vector) {
        return Some(vector);
    }
    node.chunk_embeddings
        .iter()
        .find(|vector| !vector.is_empty())
        .cloned()
}

fn edge_vector_for_property(edge: &EdgeRecord, property: &str) -> Option<Vec<f32>> {
    edge.properties.get(property).and_then(value_to_vector)
}

fn value_to_vector(value: &serde_json::Value) -> Option<Vec<f32>> {
    value.as_array().and_then(|items| {
        items
            .iter()
            .map(|item| item.as_f64().map(|component| component as f32))
            .collect()
    })
}

fn vector_error(error: VectorSpaceError) -> CopperDbError {
    CopperDbError::Config(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use copperdb_storage::{EdgeRecord, IndexDefinition, NodeEmbeddingMetadata};
    use copperdb_vectorspace::HnswRegistry;
    use std::collections::BTreeMap;

    fn vector_index_definition() -> IndexDefinition {
        IndexDefinition {
            name: "document_embedding".to_string(),
            entity_type: IndexEntityType::Node,
            label: "Document".to_string(),
            properties: vec!["embedding".to_string()],
            kind: IndexKind::Vector,
        }
    }

    fn vector_options() -> HashMap<String, serde_json::Value> {
        HashMap::from([(
            "indexConfig".to_string(),
            serde_json::json!({
                "vector.dimensions": 2,
                "vector.similarity_function": "cosine"
            }),
        )])
    }

    fn node(id: &str, embedding: Vec<f32>) -> NodeRecord {
        NodeRecord {
            id: id.to_string(),
            labels: vec!["Document".to_string()],
            properties: BTreeMap::new(),
            named_embeddings: BTreeMap::from([("embedding".to_string(), embedding)]),
            chunk_embeddings: Vec::new(),
            embed_meta: NodeEmbeddingMetadata::default(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
        }
    }

    fn relationship_index_definition() -> IndexDefinition {
        IndexDefinition {
            name: "relationship_embedding".to_string(),
            entity_type: IndexEntityType::Relationship,
            label: "RELATES".to_string(),
            properties: vec!["embedding".to_string()],
            kind: IndexKind::Vector,
        }
    }

    fn edge(id: &str, embedding: Vec<f32>) -> EdgeRecord {
        EdgeRecord {
            id: id.to_string(),
            start_node: "start".to_string(),
            end_node: "end".to_string(),
            edge_type: "RELATES".to_string(),
            properties: BTreeMap::from([("embedding".to_string(), serde_json::json!(embedding))]),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
        }
    }

    #[test]
    fn builds_declared_indexes_from_existing_nodes() {
        let storage = StorageEngine::open_temporary().unwrap();
        storage
            .persist_index_definition(&vector_index_definition())
            .unwrap();
        storage
            .persist_index_options("document_embedding", &vector_options())
            .unwrap();
        storage
            .put_node_record(&node("existing", vec![1.0, 0.0]))
            .unwrap();

        let manager = VectorIndexManager::build(&storage).unwrap();
        assert_eq!(manager.status("document_embedding").unwrap().generation, 1);
        let status = manager.status("document_embedding").unwrap();
        assert!(status.ready);
        assert_eq!(status.generation, 1);
        assert_eq!(
            manager
                .registry
                .knn("document_embedding", &[1.0, 0.0], 1)
                .unwrap()
                .0[0]
                .0,
            "existing"
        );
    }

    #[test]
    fn persistent_cosine_query_expands_and_exactly_reranks_candidates() {
        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().join("vector-db");
        let storage = StorageEngine::open(&data_dir).unwrap();
        storage
            .persist_index_definition(&vector_index_definition())
            .unwrap();
        storage
            .persist_index_options("document_embedding", &vector_options())
            .unwrap();
        for position in 0..8 {
            storage
                .put_node_record(&node(
                    &format!("node-{position}"),
                    vec![8.0 - position as f32, position as f32],
                ))
                .unwrap();
        }

        let manager = VectorIndexManager::build(&storage).unwrap();
        let cancellation = copperdb_util::RequestCancellation::new();
        let (matches, stats) = manager
            .query(&cancellation, "document_embedding", &[1.0, 0.0], 1)
            .unwrap();

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].0, "node-0");
        assert!(stats.exact_scored_candidates > matches.len());
        assert_eq!(stats.returned_candidates, matches.len());
    }

    #[test]
    fn copperdb_startup_builds_persisted_indexes_and_registers_maintenance() {
        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().join("vector-db");
        {
            let storage = StorageEngine::open(&data_dir).unwrap();
            storage
                .persist_index_definition(&vector_index_definition())
                .unwrap();
            storage
                .persist_index_options("document_embedding", &vector_options())
                .unwrap();
            storage
                .put_node_record(&node("existing", vec![1.0, 0.0]))
                .unwrap();
            storage.flush().unwrap();
        }

        let db = CopperDb::open(DatabaseConfig {
            data_dir: data_dir.to_string_lossy().into_owned(),
            ..DatabaseConfig::default()
        })
        .unwrap();
        assert_eq!(
            db.vector_index_status("document_embedding")
                .unwrap()
                .generation,
            1
        );
        assert!(
            db.vector_index_status("document_embedding")
                .unwrap()
                .estimated_memory_bytes
                > 0
        );

        db.storage()
            .put_node_record(&node("new", vec![0.0, 1.0]))
            .unwrap();
        assert_eq!(
            db.vector_index_status("document_embedding")
                .unwrap()
                .generation,
            2
        );
        let result = db
            .execute(
                "CALL db.index.vector.queryNodes('document_embedding', 1, [0.0, 1.0]) YIELD node, score RETURN node, score",
                HashMap::new(),
            )
            .unwrap();
        assert_eq!(
            result.rows[0]
                .get("node")
                .and_then(serde_json::Value::as_object)
                .and_then(|node| node.get("_id"))
                .and_then(serde_json::Value::as_str),
            Some("new")
        );
        drop(db);

        let reopened = CopperDb::open(DatabaseConfig {
            data_dir: data_dir.to_string_lossy().into_owned(),
            ..DatabaseConfig::default()
        })
        .unwrap();
        let result = reopened
            .execute(
                "CALL db.index.vector.queryNodes('document_embedding', 1, [0.0, 1.0]) YIELD node, score RETURN node, score",
                HashMap::new(),
            )
            .unwrap();
        assert_eq!(
            result.rows[0]
                .get("node")
                .and_then(serde_json::Value::as_object)
                .and_then(|node| node.get("_id"))
                .and_then(serde_json::Value::as_str),
            Some("new")
        );
    }

    #[test]
    fn file_store_mirrors_committed_node_lifecycle() {
        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().join("vector-db");
        {
            let storage = StorageEngine::open(&data_dir).unwrap();
            storage
                .persist_index_definition(&vector_index_definition())
                .unwrap();
            storage
                .persist_index_options("document_embedding", &vector_options())
                .unwrap();
            storage
                .put_node_record(&node("existing", vec![3.0, 4.0]))
                .unwrap();
            storage.flush().unwrap();
        }

        let db = CopperDb::open(DatabaseConfig {
            data_dir: data_dir.to_string_lossy().into_owned(),
            ..DatabaseConfig::default()
        })
        .unwrap();
        let path = vector_file_store_path(
            &data_dir.join(VECTOR_FILE_STORE_DIRECTORY),
            "document_embedding",
        );
        let store = VectorFileStore::open(&path, 2).unwrap();
        assert_eq!(store.len(), 1);
        assert_eq!(store.get("existing").unwrap(), Some(vec![0.6, 0.8]));

        db.storage()
            .put_node_record(&node("new", vec![0.0, 2.0]))
            .unwrap();
        let store = VectorFileStore::open(&path, 2).unwrap();
        assert_eq!(store.get("new").unwrap(), Some(vec![0.0, 1.0]));

        let mut no_longer_matching = node("existing", vec![1.0, 0.0]);
        no_longer_matching.labels.clear();
        db.storage().put_node_record(&no_longer_matching).unwrap();
        let store = VectorFileStore::open(&path, 2).unwrap();
        assert_eq!(store.get("existing").unwrap(), None);

        db.storage().delete_node_record("new").unwrap();
        let store = VectorFileStore::open(&path, 2).unwrap();
        assert!(store.is_empty());
    }

    #[test]
    fn file_store_rebuild_discards_stale_derived_records() {
        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().join("vector-db");
        let storage = StorageEngine::open(&data_dir).unwrap();
        storage
            .persist_index_definition(&vector_index_definition())
            .unwrap();
        storage
            .persist_index_options("document_embedding", &vector_options())
            .unwrap();
        storage
            .put_node_record(&node("stale", vec![1.0, 0.0]))
            .unwrap();
        VectorIndexManager::build(&storage).unwrap();

        storage.delete_node_record("stale").unwrap();
        VectorIndexManager::build(&storage).unwrap();

        let path = vector_file_store_path(
            &data_dir.join(VECTOR_FILE_STORE_DIRECTORY),
            "document_embedding",
        );
        assert!(VectorFileStore::open(&path, 2).unwrap().is_empty());
    }

    #[test]
    fn stale_vector_artifact_is_rebuilt_from_committed_storage() {
        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().join("vector-db");
        let storage = StorageEngine::open(&data_dir).unwrap();
        storage
            .persist_index_definition(&vector_index_definition())
            .unwrap();
        storage
            .persist_index_options("document_embedding", &vector_options())
            .unwrap();
        storage
            .put_node_record(&node("existing", vec![1.0, 0.0]))
            .unwrap();

        let manager = VectorIndexManager::build(&storage).unwrap();
        let artifact = data_dir.join(VECTOR_REGISTRY_ARTIFACT_FILE);
        assert!(artifact.exists());
        assert_eq!(manager.status("document_embedding").unwrap().generation, 1);

        storage
            .put_node_record(&node("new", vec![0.0, 1.0]))
            .unwrap();
        let rebuilt = VectorIndexManager::build(&storage).unwrap();
        assert_eq!(rebuilt.status("document_embedding").unwrap().generation, 2);
        assert_eq!(
            rebuilt
                .registry
                .knn("document_embedding", &[0.0, 1.0], 1)
                .unwrap()
                .0[0]
                .0,
            "new"
        );
    }

    #[test]
    fn explicit_compaction_refreshes_the_persisted_registry_artifact() {
        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().join("vector-db");
        let storage = StorageEngine::open(&data_dir).unwrap();
        storage
            .persist_index_definition(&vector_index_definition())
            .unwrap();
        storage
            .persist_index_options("document_embedding", &vector_options())
            .unwrap();
        storage
            .put_node_record(&node("existing", vec![1.0, 0.0]))
            .unwrap();

        let manager = VectorIndexManager::build(&storage).unwrap();
        manager
            .registry()
            .remove("document_embedding", "existing")
            .unwrap();
        assert_eq!(manager.status("document_embedding").unwrap().tombstones, 1);

        assert!(manager.compact(&storage, "document_embedding").unwrap());
        let status = manager.status("document_embedding").unwrap();
        assert_eq!(status.tombstones, 0);

        let artifact = HnswRegistry::load_artifact_with_source_generation(
            data_dir.join(VECTOR_REGISTRY_ARTIFACT_FILE),
        )
        .unwrap();
        assert_eq!(
            artifact
                .registry
                .status("document_embedding")
                .unwrap()
                .generation,
            status.generation
        );
    }

    #[test]
    fn vector_ddl_refreshes_the_persisted_registry_artifact() {
        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().join("vector-db");
        let db = CopperDb::open(DatabaseConfig {
            data_dir: data_dir.to_string_lossy().into_owned(),
            ..DatabaseConfig::default()
        })
        .unwrap();
        let create = "CREATE VECTOR INDEX document_embedding FOR (n:Document) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 2}}";
        db.execute(create, HashMap::new()).unwrap();

        let artifact = data_dir.join(VECTOR_REGISTRY_ARTIFACT_FILE);
        let loaded = HnswRegistry::load_artifact_with_source_generation(&artifact).unwrap();
        assert_eq!(loaded.registry.index_names(), vec!["document_embedding"]);
        let file_store = vector_file_store_path(
            &data_dir.join(VECTOR_FILE_STORE_DIRECTORY),
            "document_embedding",
        );
        assert!(file_store.exists());
        assert_eq!(
            loaded.source_generation,
            db.storage().wal_applied_sequence().unwrap()
        );

        db.execute("DROP INDEX document_embedding", HashMap::new())
            .unwrap();
        let loaded = HnswRegistry::load_artifact_with_source_generation(&artifact).unwrap();
        assert!(loaded.registry.index_names().is_empty());
        assert!(!file_store.exists());
        assert_eq!(
            loaded.source_generation,
            db.storage().wal_applied_sequence().unwrap()
        );
    }

    #[test]
    fn relationship_indexes_build_maintain_and_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().join("vector-db");
        {
            let storage = StorageEngine::open(&data_dir).unwrap();
            storage
                .persist_index_definition(&relationship_index_definition())
                .unwrap();
            storage
                .persist_index_options("relationship_embedding", &vector_options())
                .unwrap();
            storage
                .put_edge_record(&edge("existing", vec![1.0, 0.0]))
                .unwrap();
            storage.flush().unwrap();
        }

        let db = CopperDb::open(DatabaseConfig {
            data_dir: data_dir.to_string_lossy().into_owned(),
            ..DatabaseConfig::default()
        })
        .unwrap();
        assert_eq!(
            db.vector_index_status("relationship_embedding")
                .unwrap()
                .generation,
            1
        );
        let file_store_path = vector_file_store_path(
            &data_dir.join(VECTOR_FILE_STORE_DIRECTORY),
            "relationship_embedding",
        );
        let file_store = VectorFileStore::open(&file_store_path, 2).unwrap();
        assert_eq!(file_store.get("existing").unwrap(), Some(vec![1.0, 0.0]));
        db.storage()
            .put_edge_record(&edge("new", vec![0.0, 1.0]))
            .unwrap();
        let file_store = VectorFileStore::open(&file_store_path, 2).unwrap();
        assert_eq!(file_store.get("new").unwrap(), Some(vec![0.0, 1.0]));
        let result = db
            .execute(
                "CALL db.index.vector.queryRelationships('relationship_embedding', 1, [0.0, 1.0]) YIELD relationship, score RETURN relationship, score",
                HashMap::new(),
            )
            .unwrap();
        assert_eq!(
            result.rows[0]
                .get("relationship")
                .and_then(serde_json::Value::as_object)
                .and_then(|edge| edge.get("_id"))
                .and_then(serde_json::Value::as_str),
            Some("new")
        );
        db.storage()
            .put_edge_record(&edge("existing", vec![0.0, 1.0]))
            .unwrap();
        let file_store = VectorFileStore::open(&file_store_path, 2).unwrap();
        assert_eq!(file_store.get("existing").unwrap(), Some(vec![0.0, 1.0]));
        assert_eq!(
            db.execute(
                "CALL db.index.vector.queryRelationships('relationship_embedding', 1, [0.0, 1.0]) YIELD relationship RETURN relationship",
                HashMap::new(),
            )
            .unwrap()
            .rows[0]
            .get("relationship")
            .and_then(serde_json::Value::as_object)
            .and_then(|edge| edge.get("_id"))
            .and_then(serde_json::Value::as_str),
            Some("existing")
        );
        db.storage().delete_edge_record("new").unwrap();
        assert_eq!(
            VectorFileStore::open(&file_store_path, 2)
                .unwrap()
                .get("new")
                .unwrap(),
            None
        );
        assert_eq!(
            db.execute(
                "CALL db.index.vector.queryRelationships('relationship_embedding', 1, [0.0, 1.0]) YIELD relationship RETURN relationship",
                HashMap::new(),
            )
            .unwrap()
            .rows[0]
            .get("relationship")
            .and_then(serde_json::Value::as_object)
            .and_then(|edge| edge.get("_id"))
            .and_then(serde_json::Value::as_str),
            Some("existing")
        );
        drop(db);

        let reopened = CopperDb::open(DatabaseConfig {
            data_dir: data_dir.to_string_lossy().into_owned(),
            ..DatabaseConfig::default()
        })
        .unwrap();
        assert_eq!(
            reopened
                .execute(
                    "CALL db.index.vector.queryRelationships('relationship_embedding', 1, [0.0, 1.0]) YIELD relationship RETURN relationship",
                    HashMap::new(),
                )
                .unwrap()
                .rows[0]
                .get("relationship")
                .and_then(serde_json::Value::as_object)
                .and_then(|edge| edge.get("_id"))
                .and_then(serde_json::Value::as_str),
            Some("existing")
        );
    }
}
