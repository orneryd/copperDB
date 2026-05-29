use crate::{
    EdgeAdjacencyDirection, EdgeRecord, MvccLifecycleDebtKey, MvccLifecycleStatus, MvccSnapshot,
    MvccSnapshotLease, NamespaceSchema, NodeRecord, StorageEngine, StorageError,
};

pub struct NamespacedStorageEngine<'a> {
    inner: &'a StorageEngine,
    namespace: String,
    prefix: String,
}

impl<'a> NamespacedStorageEngine<'a> {
    pub(crate) fn new(inner: &'a StorageEngine, namespace: impl Into<String>) -> Self {
        let namespace = namespace.into();
        let prefix = format!("{namespace}:");
        Self {
            inner,
            namespace,
            prefix,
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn inner(&self) -> &StorageEngine {
        self.inner
    }

    pub fn put_node_record(&self, node: &NodeRecord) -> Result<(), StorageError> {
        let mut namespaced = node.clone();
        namespaced.id = self.prefix_id(&namespaced.id);
        self.inner.put_node_record(&namespaced)
    }

    pub fn get_node_record(&self, id: &str) -> Result<Option<NodeRecord>, StorageError> {
        self.inner
            .get_node_record(&self.prefix_id(id))
            .map(|node| node.map(|node| self.to_user_node(node)))
    }

    pub fn begin_mvcc_snapshot(&self) -> MvccSnapshot {
        self.inner.begin_mvcc_snapshot()
    }

    pub fn begin_registered_mvcc_snapshot(&self) -> MvccSnapshotLease {
        self.inner.begin_registered_mvcc_snapshot()
    }

    pub fn get_node_record_visible_at(
        &self,
        snapshot: &MvccSnapshot,
        id: &str,
    ) -> Result<Option<NodeRecord>, StorageError> {
        self.inner
            .get_node_record_visible_at(snapshot, &self.prefix_id(id))
            .map(|node| node.map(|node| self.to_user_node(node)))
    }

    pub fn delete_node_record(&self, id: &str) -> Result<(), StorageError> {
        self.inner.delete_node_record(&self.prefix_id(id))
    }

    pub fn put_edge_record(&self, edge: &EdgeRecord) -> Result<(), StorageError> {
        let mut namespaced = edge.clone();
        namespaced.id = self.prefix_id(&namespaced.id);
        namespaced.start_node = self.prefix_id(&namespaced.start_node);
        namespaced.end_node = self.prefix_id(&namespaced.end_node);
        self.inner.put_edge_record(&namespaced)
    }

    pub fn get_edge_record(&self, id: &str) -> Result<Option<EdgeRecord>, StorageError> {
        self.inner
            .get_edge_record(&self.prefix_id(id))
            .map(|edge| edge.map(|edge| self.to_user_edge(edge)))
    }

    pub fn get_edge_record_visible_at(
        &self,
        snapshot: &MvccSnapshot,
        id: &str,
    ) -> Result<Option<EdgeRecord>, StorageError> {
        self.inner
            .get_edge_record_visible_at(snapshot, &self.prefix_id(id))
            .map(|edge| edge.map(|edge| self.to_user_edge(edge)))
    }

    pub fn delete_edge_record(&self, id: &str) -> Result<(), StorageError> {
        self.inner.delete_edge_record(&self.prefix_id(id))
    }

    pub fn get_nodes_by_label(&self, label: &str) -> Result<Vec<NodeRecord>, StorageError> {
        Ok(self
            .inner
            .get_nodes_by_label(label)?
            .into_iter()
            .filter(|node| self.in_namespace(&node.id))
            .map(|node| self.to_user_node(node))
            .collect())
    }

    pub fn get_nodes_by_label_visible_at(
        &self,
        snapshot: &MvccSnapshot,
        label: &str,
    ) -> Result<Vec<NodeRecord>, StorageError> {
        Ok(self
            .inner
            .get_nodes_by_label_visible_at(snapshot, label)?
            .into_iter()
            .filter(|node| self.in_namespace(&node.id))
            .map(|node| self.to_user_node(node))
            .collect())
    }

    pub fn get_edges_by_type(&self, edge_type: &str) -> Result<Vec<EdgeRecord>, StorageError> {
        Ok(self
            .inner
            .get_edges_by_type(edge_type)?
            .into_iter()
            .filter(|edge| self.in_namespace(&edge.id))
            .map(|edge| self.to_user_edge(edge))
            .collect())
    }

    pub fn get_edges_by_type_visible_at(
        &self,
        snapshot: &MvccSnapshot,
        edge_type: &str,
    ) -> Result<Vec<EdgeRecord>, StorageError> {
        Ok(self
            .inner
            .get_edges_by_type_visible_at(snapshot, edge_type)?
            .into_iter()
            .filter(|edge| self.in_namespace(&edge.id))
            .map(|edge| self.to_user_edge(edge))
            .collect())
    }

    pub fn get_edges_from_node(&self, node_id: &str) -> Result<Vec<EdgeRecord>, StorageError> {
        Ok(self
            .inner
            .get_edges_from_node(&self.prefix_id(node_id))?
            .into_iter()
            .filter(|edge| self.in_namespace(&edge.id))
            .map(|edge| self.to_user_edge(edge))
            .collect())
    }

    pub fn get_edges_to_node(&self, node_id: &str) -> Result<Vec<EdgeRecord>, StorageError> {
        Ok(self
            .inner
            .get_edges_to_node(&self.prefix_id(node_id))?
            .into_iter()
            .filter(|edge| self.in_namespace(&edge.id))
            .map(|edge| self.to_user_edge(edge))
            .collect())
    }

    pub fn get_adjacent_edges(
        &self,
        node_id: &str,
        direction: EdgeAdjacencyDirection,
        edge_type: Option<&str>,
    ) -> Result<Vec<EdgeRecord>, StorageError> {
        Ok(self
            .inner
            .get_adjacent_edges(&self.prefix_id(node_id), direction, edge_type)?
            .into_iter()
            .filter(|edge| self.in_namespace(&edge.id))
            .map(|edge| self.to_user_edge(edge))
            .collect())
    }

    pub fn all_nodes(&self) -> Result<Vec<NodeRecord>, StorageError> {
        let mut nodes = Vec::new();
        self.stream_node_records(|node| {
            nodes.push(node);
            Ok(())
        })?;
        Ok(nodes)
    }

    pub fn all_edges(&self) -> Result<Vec<EdgeRecord>, StorageError> {
        let mut edges = Vec::new();
        self.stream_edge_records(|edge| {
            edges.push(edge);
            Ok(())
        })?;
        Ok(edges)
    }

    pub fn stream_node_records<F>(&self, mut visit: F) -> Result<u64, StorageError>
    where
        F: FnMut(NodeRecord) -> Result<(), StorageError>,
    {
        self.inner
            .stream_node_records_by_prefix(&self.prefix, |node| visit(self.to_user_node(node)))
    }

    pub fn stream_edge_records<F>(&self, mut visit: F) -> Result<u64, StorageError>
    where
        F: FnMut(EdgeRecord) -> Result<(), StorageError>,
    {
        let mut streamed = 0;
        self.inner.stream_edge_records(|edge| {
            if !self.in_namespace(&edge.id) {
                return Ok(());
            }
            streamed += 1;
            visit(self.to_user_edge(edge))
        })?;
        Ok(streamed)
    }

    pub fn node_count(&self) -> Result<u64, StorageError> {
        self.inner.node_count_by_prefix(&self.prefix)
    }

    pub fn edge_count(&self) -> Result<u64, StorageError> {
        self.inner.edge_count_by_prefix(&self.prefix)
    }

    pub fn node_count_by_label(&self, label: &str) -> Result<u64, StorageError> {
        self.inner
            .node_count_by_label_in_namespace(&self.namespace, label)
    }

    pub fn schema(&self) -> Result<NamespaceSchema, StorageError> {
        self.inner.schema_for_namespace(&self.namespace)
    }

    pub fn delete_all(&self) -> Result<(u64, u64), StorageError> {
        self.inner.delete_by_prefix(&self.prefix)
    }

    pub fn lifecycle_status(&self) -> MvccLifecycleStatus {
        self.inner.lifecycle_status()
    }

    pub fn trigger_prune_now(&self, retain_last_n_versions: u64) -> usize {
        self.inner.trigger_prune_now(retain_last_n_versions)
    }

    pub fn pause_lifecycle(&self) {
        self.inner.pause_lifecycle();
    }

    pub fn resume_lifecycle(&self) {
        self.inner.resume_lifecycle();
    }

    pub fn set_lifecycle_schedule_ms(&self, interval_ms: u64) {
        self.inner.set_lifecycle_schedule_ms(interval_ms);
    }

    pub fn top_lifecycle_debt_keys(&self, limit: usize) -> Vec<MvccLifecycleDebtKey> {
        self.inner
            .top_lifecycle_debt_keys(limit.saturating_mul(4).max(limit))
            .into_iter()
            .filter_map(|debt| {
                let (kind, id) = debt.logical_key.split_once(':')?;
                if !self.in_namespace(id) {
                    return None;
                }
                Some(MvccLifecycleDebtKey {
                    logical_key: format!("{kind}:{}", self.unprefix_id(id)),
                    retained_versions: debt.retained_versions,
                    prune_debt: debt.prune_debt,
                })
            })
            .take(limit)
            .collect()
    }

    fn prefix_id(&self, id: &str) -> String {
        if self.in_namespace(id) {
            id.to_string()
        } else {
            format!("{}{id}", self.prefix)
        }
    }

    fn unprefix_id(&self, id: &str) -> String {
        id.strip_prefix(&self.prefix).unwrap_or(id).to_string()
    }

    fn in_namespace(&self, id: &str) -> bool {
        id.starts_with(&self.prefix)
    }

    fn to_user_node(&self, mut node: NodeRecord) -> NodeRecord {
        node.id = self.unprefix_id(&node.id);
        node
    }

    fn to_user_edge(&self, mut edge: EdgeRecord) -> EdgeRecord {
        edge.id = self.unprefix_id(&edge.id);
        edge.start_node = self.unprefix_id(&edge.start_node);
        edge.end_node = self.unprefix_id(&edge.end_node);
        edge
    }
}
