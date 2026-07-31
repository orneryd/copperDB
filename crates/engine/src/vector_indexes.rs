use super::*;
use copperdb_vectorspace::{HnswConfig, HnswIndexStatus, HnswRegistry, VectorSpaceError};

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
}

impl VectorIndexManager {
    pub(crate) fn build(storage: &StorageEngine) -> Result<Self, CopperDbError> {
        let registry = Arc::new(HnswRegistry::new());
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
        let manager = Self { registry };
        Ok(manager)
    }

    pub(crate) fn status(&self, name: &str) -> Result<HnswIndexStatus, CopperDbError> {
        self.registry.status(name).map_err(vector_error)
    }

    pub(crate) fn registry(&self) -> Arc<HnswRegistry> {
        Arc::clone(&self.registry)
    }
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
    }
}
