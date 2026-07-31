use super::*;
use copperdb_vectorspace::{HnswConfig, HnswIndexStatus, HnswRegistry, VectorSpaceError};
use std::path::PathBuf;

const VECTOR_REGISTRY_ARTIFACT_FILE: &str = "vectors.hnsw";

#[derive(Debug, Clone)]
struct VectorIndexBinding {
    name: String,
    label: String,
    property: String,
}

/// Per-database owner for schema-declared cosine HNSW indexes.
///
/// Construction is an explicit lifecycle operation performed by the engine.
/// Query paths only read the populated registry.
#[derive(Debug)]
pub(crate) struct VectorIndexManager {
    registry: Arc<HnswRegistry>,
    artifact_path: Option<PathBuf>,
}

impl VectorIndexManager {
    pub(crate) fn build(storage: &StorageEngine) -> Result<Self, CopperDbError> {
        let mut registry = Arc::new(HnswRegistry::new());
        let mut bindings = Vec::new();

        for definition in storage.load_index_definitions()? {
            if definition.kind != IndexKind::Vector
                || definition.entity_type != IndexEntityType::Node
            {
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
            for node in storage.all_node_records()? {
                for binding in &bindings {
                    let matches_label = binding.label.is_empty()
                        || node.labels.iter().any(|label| label == &binding.label);
                    if !matches_label {
                        continue;
                    }
                    if let Some(vector) = node_vector_for_property(&node, &binding.property) {
                        registry
                            .upsert(&binding.name, &node.id, vector)
                            .map_err(vector_error)?;
                    }
                }
            }
        }
        let manager = Self {
            registry,
            artifact_path,
        };
        manager.persist_artifact(storage);
        Ok(manager)
    }

    pub(crate) fn status(&self, name: &str) -> Result<HnswIndexStatus, CopperDbError> {
        self.registry.status(name).map_err(vector_error)
    }

    pub(crate) fn registry(&self) -> Arc<HnswRegistry> {
        Arc::clone(&self.registry)
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
            manager.persist_artifact(&storage);
        })
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
    use copperdb_storage::{IndexDefinition, NodeEmbeddingMetadata};
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
        assert_eq!(
            loaded.source_generation,
            db.storage().wal_applied_sequence().unwrap()
        );

        db.execute("DROP INDEX document_embedding", HashMap::new())
            .unwrap();
        let loaded = HnswRegistry::load_artifact_with_source_generation(&artifact).unwrap();
        assert!(loaded.registry.index_names().is_empty());
        assert_eq!(
            loaded.source_generation,
            db.storage().wal_applied_sequence().unwrap()
        );
    }
}
