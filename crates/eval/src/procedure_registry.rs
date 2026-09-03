use crate::{EvalError, QueryStats, Row};
use copperdb_storage::{
    EdgeAdjacencyDirection, EdgeRecord, NodeRecord, StorageEngine, StorageTransaction,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use thiserror::Error;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ImportFileError {
    #[error("local APOC import file access is disabled")]
    Disabled,
    #[error("remote APOC URL access is disabled")]
    RemoteDisabled,
    #[error("APOC remote URL host is not allowlisted")]
    RemoteHostNotAllowed,
    #[error("APOC remote URL host is required")]
    RemoteHostRequired,
    #[error("APOC remote URLs may not contain userinfo")]
    RemoteUserInfo,
    #[error("APOC remote URLs may not contain fragments")]
    RemoteFragment,
    #[error("APOC remote URL host resolves to a disallowed address")]
    RemoteAddressDisallowed,
    #[error("failed to resolve APOC remote host")]
    RemoteResolveFailed,
    #[error("failed to request APOC remote URL")]
    RemoteRequestFailed,
    #[error("APOC remote URL returned HTTP status {0}")]
    RemoteHttpStatus(u16),
    #[error("file URL may not contain an authority section (i.e. it should be 'file:///')")]
    FileUrlAuthority,
    #[error("file URL may not contain a query component")]
    FileUrlQuery,
    #[error("file URL may not contain a fragment component")]
    FileUrlFragment,
    #[error("unsupported APOC file URL scheme {0:?}")]
    UnsupportedScheme(String),
    #[error("APOC import source is not a regular file")]
    NotRegularFile,
    #[error("APOC import source escapes the configured root")]
    RootEscape,
    #[error("APOC import source exceeds the {limit} byte limit")]
    TooLarge { limit: usize },
    #[error("failed to read APOC import source")]
    ReadFailed,
    #[error(transparent)]
    RequestCancelled(#[from] copperdb_util::RequestCancelled),
}

pub trait ImportFileService: Send + Sync {
    fn read(
        &self,
        request_context: &copperdb_util::RequestContext,
        source: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ImportFileError>;
}

#[derive(Debug, Default)]
pub struct DeniedImportFileService;

impl ImportFileService for DeniedImportFileService {
    fn read(
        &self,
        _request_context: &copperdb_util::RequestContext,
        source: &str,
        _max_bytes: usize,
    ) -> Result<Vec<u8>, ImportFileError> {
        if source.starts_with("http://") || source.starts_with("https://") {
            Err(ImportFileError::RemoteDisabled)
        } else {
            Err(ImportFileError::Disabled)
        }
    }
}

#[derive(Debug, Clone)]
pub struct RootedImportFileService {
    root: PathBuf,
}

impl RootedImportFileService {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, ImportFileError> {
        let root = root
            .as_ref()
            .canonicalize()
            .map_err(|_| ImportFileError::ReadFailed)?;
        if !root.is_dir() {
            return Err(ImportFileError::ReadFailed);
        }
        Ok(Self { root })
    }

    fn resolve(&self, source: &str) -> Result<PathBuf, ImportFileError> {
        if source.starts_with("http://") || source.starts_with("https://") {
            return Err(ImportFileError::RemoteDisabled);
        }
        if source.starts_with("file://") && !source.starts_with("file:///") {
            return Err(ImportFileError::FileUrlAuthority);
        }
        let path = if source.contains("://") {
            let url = Url::parse(source)
                .map_err(|_| ImportFileError::UnsupportedScheme(String::new()))?;
            if url.scheme() != "file" {
                return Err(ImportFileError::UnsupportedScheme(url.scheme().into()));
            }
            if url.host_str().is_some() {
                return Err(ImportFileError::FileUrlAuthority);
            }
            if url.query().is_some() {
                return Err(ImportFileError::FileUrlQuery);
            }
            if url.fragment().is_some() {
                return Err(ImportFileError::FileUrlFragment);
            }
            url.to_file_path()
                .map_err(|_| ImportFileError::ReadFailed)?
        } else {
            PathBuf::from(source)
        };
        let mut relative = PathBuf::new();
        for component in path.components() {
            match component {
                Component::Normal(component) => relative.push(component),
                Component::ParentDir => {
                    relative.pop();
                }
                Component::CurDir | Component::RootDir | Component::Prefix(_) => {}
            }
        }
        let resolved = self
            .root
            .join(relative)
            .canonicalize()
            .map_err(|_| ImportFileError::ReadFailed)?;
        if !resolved.starts_with(&self.root) {
            return Err(ImportFileError::RootEscape);
        }
        Ok(resolved)
    }
}

impl ImportFileService for RootedImportFileService {
    fn read(
        &self,
        request_context: &copperdb_util::RequestContext,
        source: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ImportFileError> {
        request_context.check_active()?;
        let path = self.resolve(source)?;
        let metadata = path.metadata().map_err(|_| ImportFileError::ReadFailed)?;
        if !metadata.is_file() {
            return Err(ImportFileError::NotRegularFile);
        }
        if metadata.len() > max_bytes as u64 {
            return Err(ImportFileError::TooLarge { limit: max_bytes });
        }
        let mut file = File::open(path).map_err(|_| ImportFileError::ReadFailed)?;
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        let mut chunk = [0u8; 16 * 1024];
        loop {
            request_context.check_active()?;
            let read = file
                .read(&mut chunk)
                .map_err(|_| ImportFileError::ReadFailed)?;
            if read == 0 {
                break;
            }
            if bytes.len().saturating_add(read) > max_bytes {
                return Err(ImportFileError::TooLarge { limit: max_bytes });
            }
            bytes.extend_from_slice(&chunk[..read]);
        }
        Ok(bytes)
    }
}

#[derive(Debug, Clone)]
pub struct RemoteImportFileService {
    allowlist: Vec<String>,
    allow_non_public_addresses: bool,
}

impl RemoteImportFileService {
    pub fn new(host_allowlist: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let mut allowlist: Vec<String> = host_allowlist
            .into_iter()
            .map(Into::into)
            .map(|host: String| host.trim().to_ascii_lowercase())
            .filter(|host| !host.is_empty())
            .collect();
        allowlist.sort();
        allowlist.dedup();
        Self {
            allowlist,
            allow_non_public_addresses: false,
        }
    }

    #[cfg(test)]
    fn allowing_non_public_addresses_for_tests(mut self) -> Self {
        self.allow_non_public_addresses = true;
        self
    }

    fn host_allowed(&self, host: &str) -> bool {
        let host = host.to_ascii_lowercase();
        self.allowlist.iter().any(|entry| {
            entry == &host
                || entry.strip_prefix("*.").is_some_and(|suffix| {
                    host.len() > suffix.len()
                        && host.ends_with(suffix)
                        && host.as_bytes()[host.len() - suffix.len() - 1] == b'.'
                })
        })
    }

    fn validate_url(
        &self,
        source: &str,
    ) -> Result<(Url, String, Vec<SocketAddr>), ImportFileError> {
        let url = Url::parse(source).map_err(|_| ImportFileError::RemoteHostRequired)?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(ImportFileError::UnsupportedScheme(url.scheme().into()));
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(ImportFileError::RemoteUserInfo);
        }
        if url.fragment().is_some() {
            return Err(ImportFileError::RemoteFragment);
        }
        let host = url
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or(ImportFileError::RemoteHostRequired)?
            .to_ascii_lowercase();
        if !self.host_allowed(&host) {
            return Err(ImportFileError::RemoteHostNotAllowed);
        }
        let port = url
            .port_or_known_default()
            .ok_or(ImportFileError::RemoteHostRequired)?;
        let addresses: Vec<SocketAddr> = (host.as_str(), port)
            .to_socket_addrs()
            .map_err(|_| ImportFileError::RemoteResolveFailed)?
            .collect();
        if addresses.is_empty() {
            return Err(ImportFileError::RemoteResolveFailed);
        }
        if !self.allow_non_public_addresses
            && addresses
                .iter()
                .any(|address| is_disallowed_remote_address(address.ip()))
        {
            return Err(ImportFileError::RemoteAddressDisallowed);
        }
        Ok((url, host, addresses))
    }
}

impl ImportFileService for RemoteImportFileService {
    fn read(
        &self,
        request_context: &copperdb_util::RequestContext,
        source: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ImportFileError> {
        request_context.check_active()?;
        let (url, host, addresses) = self.validate_url(source)?;
        let timeout = match request_context.deadline() {
            Some(deadline) => deadline
                .duration_since(std::time::SystemTime::now())
                .map_err(|_| {
                    request_context.cancel_due_to_deadline();
                    copperdb_util::RequestCancelled
                })?
                .min(Duration::from_secs(10)),
            None => Duration::from_secs(10),
        };
        let client = reqwest::blocking::Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .resolve_to_addrs(&host, &addresses)
            .build()
            .map_err(|_| ImportFileError::RemoteRequestFailed)?;
        let mut response =
            client
                .get(url)
                .send()
                .map_err(|_| match request_context.check_active() {
                    Ok(()) => ImportFileError::RemoteRequestFailed,
                    Err(cancelled) => cancelled.into(),
                })?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(ImportFileError::RemoteHttpStatus(
                response.status().as_u16(),
            ));
        }
        if response
            .content_length()
            .is_some_and(|length| length > max_bytes as u64)
        {
            return Err(ImportFileError::TooLarge { limit: max_bytes });
        }
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 16 * 1024];
        loop {
            request_context.check_active()?;
            let read =
                response
                    .read(&mut chunk)
                    .map_err(|_| match request_context.check_active() {
                        Ok(()) => ImportFileError::RemoteRequestFailed,
                        Err(cancelled) => cancelled.into(),
                    })?;
            if read == 0 {
                break;
            }
            if bytes.len().saturating_add(read) > max_bytes {
                return Err(ImportFileError::TooLarge { limit: max_bytes });
            }
            bytes.extend_from_slice(&chunk[..read]);
        }
        Ok(bytes)
    }
}

pub struct RestrictedImportFileService {
    local: Arc<dyn ImportFileService>,
    remote: Option<RemoteImportFileService>,
}

impl RestrictedImportFileService {
    pub fn new(local: Arc<dyn ImportFileService>, remote: Option<RemoteImportFileService>) -> Self {
        Self { local, remote }
    }
}

impl ImportFileService for RestrictedImportFileService {
    fn read(
        &self,
        request_context: &copperdb_util::RequestContext,
        source: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ImportFileError> {
        if source.starts_with("http://") || source.starts_with("https://") {
            self.remote
                .as_ref()
                .ok_or(ImportFileError::RemoteDisabled)?
                .read(request_context, source, max_bytes)
        } else {
            self.local.read(request_context, source, max_bytes)
        }
    }
}

fn is_disallowed_remote_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            address.is_private()
                || address.is_loopback()
                || address.is_link_local()
                || address.is_multicast()
                || address.is_unspecified()
                || address.is_broadcast()
        }
        IpAddr::V6(address) => {
            address
                .to_ipv4_mapped()
                .is_some_and(|mapped| is_disallowed_remote_address(IpAddr::V4(mapped)))
                || address.is_unique_local()
                || address.is_loopback()
                || address.is_unicast_link_local()
                || address.is_multicast()
                || address.is_unspecified()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcedureMode {
    Read,
    Write,
    Dbms,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphDirection {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GraphNode {
    pub id: String,
    pub labels: Vec<String>,
    pub properties: BTreeMap<String, Value>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

impl GraphNode {
    pub fn to_value(&self) -> Value {
        Value::Object(
            self.properties
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .chain([
                    ("_id".to_string(), Value::String(self.id.clone())),
                    (
                        "_labels".to_string(),
                        Value::Array(self.labels.iter().cloned().map(Value::String).collect()),
                    ),
                    (
                        "_created_at_unix_ms".to_string(),
                        Value::from(self.created_at_unix_ms),
                    ),
                    (
                        "_updated_at_unix_ms".to_string(),
                        Value::from(self.updated_at_unix_ms),
                    ),
                ])
                .collect(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRelationship {
    pub id: String,
    pub start_node: String,
    pub end_node: String,
    pub relationship_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{code}")]
pub struct GraphReadError {
    pub code: String,
}

pub trait GraphReadService: Send + Sync {
    fn node(&self, id: &str) -> Result<Option<GraphNode>, GraphReadError>;

    fn nodes(&self) -> Result<Vec<GraphNode>, GraphReadError>;

    fn relationships(
        &self,
        node_id: &str,
        direction: GraphDirection,
        relationship_types: &[String],
    ) -> Result<Vec<GraphRelationship>, GraphReadError>;
}

impl GraphReadService for StorageEngine {
    fn node(&self, id: &str) -> Result<Option<GraphNode>, GraphReadError> {
        self.get_node_record(id)
            .map(|node| node.map(graph_node))
            .map_err(|_| graph_read_error())
    }

    fn nodes(&self) -> Result<Vec<GraphNode>, GraphReadError> {
        let mut nodes = self
            .all_node_records()
            .map_err(|_| graph_read_error())?
            .into_iter()
            .map(graph_node)
            .collect::<Vec<_>>();
        nodes.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(nodes)
    }

    fn relationships(
        &self,
        node_id: &str,
        direction: GraphDirection,
        relationship_types: &[String],
    ) -> Result<Vec<GraphRelationship>, GraphReadError> {
        let storage_direction = match direction {
            GraphDirection::Outgoing => EdgeAdjacencyDirection::Outgoing,
            GraphDirection::Incoming => EdgeAdjacencyDirection::Incoming,
            GraphDirection::Both => EdgeAdjacencyDirection::Both,
        };
        let mut relationships = Vec::new();
        if relationship_types.is_empty() {
            relationships.extend(
                self.get_adjacent_edges(node_id, storage_direction, None)
                    .map_err(|_| graph_read_error())?,
            );
        } else {
            for relationship_type in relationship_types {
                relationships.extend(
                    self.get_adjacent_edges(node_id, storage_direction, Some(relationship_type))
                        .map_err(|_| graph_read_error())?,
                );
            }
        }
        relationships.sort_by(|left, right| left.id.cmp(&right.id));
        relationships.dedup_by(|left, right| left.id == right.id);
        Ok(relationships
            .into_iter()
            .map(|relationship| GraphRelationship {
                id: relationship.id,
                start_node: relationship.start_node,
                end_node: relationship.end_node,
                relationship_type: relationship.edge_type,
            })
            .collect())
    }
}

pub(crate) struct TransactionGraphService<'transaction, 'engine> {
    transaction: Mutex<&'transaction mut StorageTransaction<'engine>>,
}

impl<'transaction, 'engine> TransactionGraphService<'transaction, 'engine> {
    pub(crate) fn new(transaction: &'transaction mut StorageTransaction<'engine>) -> Self {
        Self {
            transaction: Mutex::new(transaction),
        }
    }
}

impl GraphReadService for TransactionGraphService<'_, '_> {
    fn node(&self, id: &str) -> Result<Option<GraphNode>, GraphReadError> {
        self.transaction
            .lock()
            .map_err(|_| graph_read_error())?
            .get_node_record(id)
            .map(|node| node.map(graph_node))
            .map_err(|_| graph_read_error())
    }

    fn nodes(&self) -> Result<Vec<GraphNode>, GraphReadError> {
        let mut nodes = self
            .transaction
            .lock()
            .map_err(|_| graph_read_error())?
            .all_node_records()
            .map_err(|_| graph_read_error())?
            .into_iter()
            .map(graph_node)
            .collect::<Vec<_>>();
        nodes.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(nodes)
    }

    fn relationships(
        &self,
        node_id: &str,
        direction: GraphDirection,
        relationship_types: &[String],
    ) -> Result<Vec<GraphRelationship>, GraphReadError> {
        let direction = match direction {
            GraphDirection::Outgoing => EdgeAdjacencyDirection::Outgoing,
            GraphDirection::Incoming => EdgeAdjacencyDirection::Incoming,
            GraphDirection::Both => EdgeAdjacencyDirection::Both,
        };
        let transaction = self.transaction.lock().map_err(|_| graph_read_error())?;
        let mut relationships = transaction
            .get_adjacent_edges(node_id, direction, None)
            .map_err(|_| graph_read_error())?;
        if !relationship_types.is_empty() {
            relationships.retain(|edge| relationship_types.contains(&edge.edge_type));
        }
        Ok(relationships
            .into_iter()
            .map(|relationship| GraphRelationship {
                id: relationship.id,
                start_node: relationship.start_node,
                end_node: relationship.end_node,
                relationship_type: relationship.edge_type,
            })
            .collect())
    }
}

fn graph_node(node: copperdb_storage::NodeRecord) -> GraphNode {
    GraphNode {
        id: node.id,
        labels: node.labels,
        properties: node.properties,
        created_at_unix_ms: node.created_at_unix_ms,
        updated_at_unix_ms: node.updated_at_unix_ms,
    }
}

fn graph_read_error() -> GraphReadError {
    GraphReadError {
        code: "graph_read_failed".into(),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GraphWriteNode {
    pub id: String,
    pub labels: Vec<String>,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GraphWriteRelationship {
    pub id: String,
    pub start_node: String,
    pub end_node: String,
    pub relationship_type: String,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GraphWriteSummary {
    pub nodes_created: usize,
    pub relationships_created: usize,
    pub properties_set: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GraphWriteError {
    #[error("package graph write access is disabled")]
    Disabled,
    #[error("package graph write batch contains a duplicate record")]
    DuplicateRecord,
    #[error("package graph write batch conflicts with an existing record")]
    ExistingRecord,
    #[error("package graph write relationship endpoint does not exist")]
    MissingEndpoint,
    #[error("package graph write batch failed")]
    WriteFailed,
    #[error(transparent)]
    RequestCancelled(#[from] copperdb_util::RequestCancelled),
}

impl From<copperdb_storage::StorageError> for GraphWriteError {
    fn from(_error: copperdb_storage::StorageError) -> Self {
        Self::WriteFailed
    }
}

pub trait GraphWriteService: Send + Sync {
    fn import_batch(
        &self,
        request_context: &copperdb_util::RequestContext,
        nodes: &[GraphWriteNode],
        relationships: &[GraphWriteRelationship],
    ) -> Result<GraphWriteSummary, GraphWriteError>;
}

#[derive(Debug, Default)]
pub struct DeniedGraphWriteService;

impl GraphWriteService for DeniedGraphWriteService {
    fn import_batch(
        &self,
        _request_context: &copperdb_util::RequestContext,
        _nodes: &[GraphWriteNode],
        _relationships: &[GraphWriteRelationship],
    ) -> Result<GraphWriteSummary, GraphWriteError> {
        Err(GraphWriteError::Disabled)
    }
}

impl GraphWriteService for StorageEngine {
    fn import_batch(
        &self,
        request_context: &copperdb_util::RequestContext,
        nodes: &[GraphWriteNode],
        relationships: &[GraphWriteRelationship],
    ) -> Result<GraphWriteSummary, GraphWriteError> {
        validate_graph_write_batch(
            request_context,
            nodes,
            relationships,
            |id| self.get_node_record(id).map(|node| node.is_some()),
            |id| self.get_edge_record(id).map(|edge| edge.is_some()),
        )?;
        let timestamp = unix_time_millis();
        self.batch_write(|batch| {
            for node in nodes {
                request_context.check_active()?;
                batch.put_node_record(&graph_write_node_record(node, timestamp));
            }
            for relationship in relationships {
                request_context.check_active()?;
                batch.put_edge_record(&graph_write_edge_record(relationship, timestamp));
            }
            Ok::<_, GraphWriteError>(())
        })?;
        Ok(graph_write_summary(nodes, relationships))
    }
}

impl GraphWriteService for TransactionGraphService<'_, '_> {
    fn import_batch(
        &self,
        request_context: &copperdb_util::RequestContext,
        nodes: &[GraphWriteNode],
        relationships: &[GraphWriteRelationship],
    ) -> Result<GraphWriteSummary, GraphWriteError> {
        let mut transaction = self
            .transaction
            .lock()
            .map_err(|_| GraphWriteError::WriteFailed)?;
        validate_graph_write_batch(
            request_context,
            nodes,
            relationships,
            |id| transaction.get_node_record(id).map(|node| node.is_some()),
            |id| transaction.get_edge_record(id).map(|edge| edge.is_some()),
        )?;
        let timestamp = unix_time_millis();
        for node in nodes {
            request_context.check_active()?;
            transaction.put_node_record(graph_write_node_record(node, timestamp));
        }
        for relationship in relationships {
            request_context.check_active()?;
            transaction.put_edge_record(graph_write_edge_record(relationship, timestamp));
        }
        Ok(graph_write_summary(nodes, relationships))
    }
}

fn validate_graph_write_batch<NodeExists, EdgeExists>(
    request_context: &copperdb_util::RequestContext,
    nodes: &[GraphWriteNode],
    relationships: &[GraphWriteRelationship],
    mut node_exists: NodeExists,
    mut edge_exists: EdgeExists,
) -> Result<(), GraphWriteError>
where
    NodeExists: FnMut(&str) -> Result<bool, copperdb_storage::StorageError>,
    EdgeExists: FnMut(&str) -> Result<bool, copperdb_storage::StorageError>,
{
    let mut node_ids = HashSet::with_capacity(nodes.len());
    for node in nodes {
        request_context.check_active()?;
        if !node_ids.insert(node.id.as_str()) {
            return Err(GraphWriteError::DuplicateRecord);
        }
        if node_exists(&node.id).map_err(|_| GraphWriteError::WriteFailed)? {
            return Err(GraphWriteError::ExistingRecord);
        }
    }
    let mut relationship_ids = HashSet::with_capacity(relationships.len());
    for relationship in relationships {
        request_context.check_active()?;
        if !relationship_ids.insert(relationship.id.as_str()) {
            return Err(GraphWriteError::DuplicateRecord);
        }
        if edge_exists(&relationship.id).map_err(|_| GraphWriteError::WriteFailed)? {
            return Err(GraphWriteError::ExistingRecord);
        }
        for endpoint in [&relationship.start_node, &relationship.end_node] {
            if !node_ids.contains(endpoint.as_str())
                && !node_exists(endpoint).map_err(|_| GraphWriteError::WriteFailed)?
            {
                return Err(GraphWriteError::MissingEndpoint);
            }
        }
    }
    Ok(())
}

fn graph_write_node_record(node: &GraphWriteNode, timestamp: i64) -> NodeRecord {
    NodeRecord {
        id: node.id.clone(),
        labels: node.labels.clone(),
        properties: node.properties.clone(),
        named_embeddings: BTreeMap::new(),
        chunk_embeddings: Vec::new(),
        embed_meta: Default::default(),
        created_at_unix_ms: timestamp,
        updated_at_unix_ms: timestamp,
    }
}

fn graph_write_edge_record(relationship: &GraphWriteRelationship, timestamp: i64) -> EdgeRecord {
    EdgeRecord {
        id: relationship.id.clone(),
        start_node: relationship.start_node.clone(),
        end_node: relationship.end_node.clone(),
        edge_type: relationship.relationship_type.clone(),
        properties: relationship.properties.clone(),
        created_at_unix_ms: timestamp,
        updated_at_unix_ms: timestamp,
    }
}

fn graph_write_summary(
    nodes: &[GraphWriteNode],
    relationships: &[GraphWriteRelationship],
) -> GraphWriteSummary {
    GraphWriteSummary {
        nodes_created: nodes.len(),
        relationships_created: relationships.len(),
        properties_set: nodes
            .iter()
            .map(|node| node.properties.len())
            .chain(
                relationships
                    .iter()
                    .map(|relationship| relationship.properties.len()),
            )
            .sum(),
    }
}

fn unix_time_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

impl ProcedureMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "READ",
            Self::Write => "WRITE",
            Self::Dbms => "DBMS",
        }
    }
}

pub struct ProcedureCallContext<'a> {
    pub row: &'a Row,
    pub params: &'a HashMap<String, Value>,
    pub capabilities: &'a [String],
    pub caller_roles: &'a [String],
    pub database: Option<&'a str>,
    pub request_context: &'a copperdb_util::RequestContext,
    pub graph_read: &'a dyn GraphReadService,
    pub graph_write: &'a dyn GraphWriteService,
    pub import_files: &'a dyn ImportFileService,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ProcedureOutput {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    pub stats: QueryStats,
}

impl ProcedureOutput {
    pub fn new(columns: Vec<String>, rows: Vec<Row>) -> Self {
        Self {
            columns,
            rows,
            stats: QueryStats::default(),
        }
    }

    pub fn with_stats(mut self, stats: QueryStats) -> Self {
        self.stats = stats;
        self
    }
}

impl From<ProcedureOutput> for crate::EvalResult {
    fn from(output: ProcedureOutput) -> Self {
        Self {
            columns: output.columns,
            rows: output.rows,
            stats: output.stats,
        }
    }
}

#[derive(Debug, Error)]
pub enum ProcedureError {
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    RequestCancelled(#[from] copperdb_util::RequestCancelled),
}

impl From<ProcedureError> for EvalError {
    fn from(error: ProcedureError) -> Self {
        match error {
            ProcedureError::Message(message) => Self::ExecutionError(message),
            ProcedureError::RequestCancelled(cancelled) => Self::RequestCancelled(cancelled),
        }
    }
}

pub type ProcedureHandler = Arc<
    dyn Fn(&ProcedureCallContext<'_>, &[Value]) -> Result<ProcedureOutput, ProcedureError>
        + Send
        + Sync
        + 'static,
>;

pub type ProcedureRegistrar = Arc<
    dyn Fn(&mut ProcedureRegistryBuilder) -> Result<(), ProcedureRegistryError>
        + Send
        + Sync
        + 'static,
>;

#[cfg(test)]
mod import_file_tests {
    use super::*;
    use copperdb_util::RequestContext;
    use std::fs;
    use std::io::Write as _;
    use std::net::TcpListener;
    use tempfile::tempdir;

    fn one_shot_http_server(response: &'static [u8]) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 1024];
            let _ = stream.read(&mut request).unwrap();
            stream.write_all(response).unwrap();
        });
        format!("http://{address}/payload.json")
    }

    #[test]
    fn rooted_service_reads_rebased_file_urls_with_bounds() {
        let root = tempdir().unwrap();
        fs::create_dir(root.path().join("safe")).unwrap();
        fs::write(root.path().join("safe/payload.json"), br#"{"id":1}"#).unwrap();
        let service = RootedImportFileService::new(root.path()).unwrap();
        let request = RequestContext::detached();

        let bytes = service
            .read(&request, "file:///../safe/payload.json", 64)
            .unwrap();

        assert_eq!(bytes, br#"{"id":1}"#);
        assert_eq!(
            service.read(&request, "safe/payload.json", 4).unwrap_err(),
            ImportFileError::TooLarge { limit: 4 }
        );
    }

    #[test]
    fn denied_service_distinguishes_local_and_remote_sources() {
        let service = DeniedImportFileService;
        let request = RequestContext::detached();

        assert_eq!(
            service.read(&request, "payload.json", 64).unwrap_err(),
            ImportFileError::Disabled
        );
        assert_eq!(
            service
                .read(&request, "https://example.com/payload.json", 64)
                .unwrap_err(),
            ImportFileError::RemoteDisabled
        );
    }

    #[test]
    fn rooted_service_rejects_unsafe_file_url_components_and_cancellation() {
        let root = tempdir().unwrap();
        fs::write(root.path().join("payload.json"), br#"{"id":1}"#).unwrap();
        let service = RootedImportFileService::new(root.path()).unwrap();
        let request = RequestContext::detached();

        for (source, expected) in [
            (
                "file://localhost/payload.json",
                ImportFileError::FileUrlAuthority,
            ),
            (
                "file:///payload.json?token=secret",
                ImportFileError::FileUrlQuery,
            ),
            (
                "file:///payload.json#fragment",
                ImportFileError::FileUrlFragment,
            ),
        ] {
            assert_eq!(service.read(&request, source, 64).unwrap_err(), expected);
        }

        request.cancel();
        assert_eq!(
            service.read(&request, "payload.json", 64).unwrap_err(),
            ImportFileError::RequestCancelled(copperdb_util::RequestCancelled)
        );
    }

    #[cfg(unix)]
    #[test]
    fn rooted_service_rejects_symlink_escape() {
        use std::os::unix::fs::symlink;

        let root = tempdir().unwrap();
        let outside = tempdir().unwrap();
        fs::write(outside.path().join("secret.json"), br#"{"secret":true}"#).unwrap();
        symlink(outside.path(), root.path().join("escape")).unwrap();
        let service = RootedImportFileService::new(root.path()).unwrap();

        assert_eq!(
            service
                .read(&RequestContext::detached(), "escape/secret.json", 1_024)
                .unwrap_err(),
            ImportFileError::RootEscape
        );
    }

    #[test]
    fn remote_service_enforces_host_and_url_policy_before_request() {
        let service = RemoteImportFileService::new(["example.com", "*.example.org"]);
        let request = RequestContext::detached();

        assert!(!service.host_allowed("example.org"));
        assert!(service.host_allowed("api.example.org"));
        assert!(service.host_allowed("deep.api.example.org"));
        assert!(!service.host_allowed("example.org.attacker.test"));
        assert_eq!(
            service
                .read(&request, "https://attacker.test/payload.json", 64)
                .unwrap_err(),
            ImportFileError::RemoteHostNotAllowed
        );
        assert_eq!(
            service
                .read(&request, "https://user@example.com/payload.json", 64)
                .unwrap_err(),
            ImportFileError::RemoteUserInfo
        );
        assert_eq!(
            service
                .read(&request, "https://example.com/payload.json#secret", 64)
                .unwrap_err(),
            ImportFileError::RemoteFragment
        );
    }

    #[test]
    fn remote_service_rejects_non_public_resolved_addresses() {
        let service = RemoteImportFileService::new(["localhost", "127.0.0.1"]);
        let request = RequestContext::detached();

        assert_eq!(
            service
                .read(&request, "http://localhost/payload.json", 64)
                .unwrap_err(),
            ImportFileError::RemoteAddressDisallowed
        );
        assert_eq!(
            service
                .read(&request, "http://127.0.0.1/payload.json", 64)
                .unwrap_err(),
            ImportFileError::RemoteAddressDisallowed
        );
        assert!(is_disallowed_remote_address(
            "::ffff:127.0.0.1".parse().unwrap()
        ));
        assert!(is_disallowed_remote_address(
            "::ffff:10.0.0.1".parse().unwrap()
        ));
    }

    #[test]
    fn remote_service_streams_bounded_success_and_rejects_redirects_and_statuses() {
        let success_url = one_shot_http_server(
            b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\n{\"id\":1}",
        );
        let service =
            RemoteImportFileService::new(["127.0.0.1"]).allowing_non_public_addresses_for_tests();
        let request = RequestContext::detached();
        assert_eq!(
            service.read(&request, &success_url, 8).unwrap(),
            br#"{"id":1}"#
        );

        let oversized_url = one_shot_http_server(
            b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\n{\"id\":1}",
        );
        assert_eq!(
            service.read(&request, &oversized_url, 7).unwrap_err(),
            ImportFileError::TooLarge { limit: 7 }
        );

        let redirect_url = one_shot_http_server(
            b"HTTP/1.1 302 Found\r\nLocation: http://attacker.test/\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        );
        assert_eq!(
            service.read(&request, &redirect_url, 64).unwrap_err(),
            ImportFileError::RemoteHttpStatus(302)
        );

        let failure_url = one_shot_http_server(
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        );
        assert_eq!(
            service.read(&request, &failure_url, 64).unwrap_err(),
            ImportFileError::RemoteHttpStatus(503)
        );
    }

    #[test]
    fn restricted_service_keeps_local_and_remote_permissions_independent() {
        let root = tempdir().unwrap();
        fs::write(root.path().join("payload.json"), br#"{"local":true}"#).unwrap();
        let local: Arc<dyn ImportFileService> =
            Arc::new(RootedImportFileService::new(root.path()).unwrap());
        let local_only = RestrictedImportFileService::new(local, None);
        let request = RequestContext::detached();

        assert_eq!(
            local_only.read(&request, "payload.json", 64).unwrap(),
            br#"{"local":true}"#
        );
        assert_eq!(
            local_only
                .read(&request, "https://example.com/payload.json", 64)
                .unwrap_err(),
            ImportFileError::RemoteDisabled
        );
    }

    #[test]
    fn graph_write_service_rejects_invalid_batch_without_partial_writes() {
        let storage = StorageEngine::open_memory().unwrap();
        let nodes = [GraphWriteNode {
            id: "node-1".into(),
            labels: vec!["Person".into()],
            properties: BTreeMap::from([("name".into(), Value::String("Ada".into()))]),
        }];
        let relationships = [GraphWriteRelationship {
            id: "relationship-1".into(),
            start_node: "node-1".into(),
            end_node: "missing".into(),
            relationship_type: "KNOWS".into(),
            properties: BTreeMap::new(),
        }];

        assert_eq!(
            storage
                .import_batch(&RequestContext::detached(), &nodes, &relationships)
                .unwrap_err(),
            GraphWriteError::MissingEndpoint
        );
        assert!(storage.get_node_record("node-1").unwrap().is_none());
        assert!(storage.get_edge_record("relationship-1").unwrap().is_none());
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum BuiltinProcedure {
    DbLabels,
    DbRelationshipTypes,
    DbPropertyKeys,
    DbConstraints,
    DbIndexes,
    DbPing,
    DbInfo,
    DbSchemaNodeProperties,
    DbSchemaRelProperties,
    DbSchemaVisualization,
    NornicDbVersion,
    NornicDbStats,
    NornicDbDecayInfo,
    NornicDbKnowledgePolicyInfo,
    DbmsProcedures,
    DbmsFunctions,
    DbmsComponents,
    DbmsInfo,
    DbmsListConfig,
    DbmsClientConfig,
    DbmsListConnections,
    FulltextListAnalyzers,
    KnowledgePolicyResolve,
    KnowledgePolicyProfiles,
    KnowledgePolicyPolicies,
    FulltextQueryNodes,
    FulltextQueryRelationships,
    VectorQueryNodes,
    VectorQueryRelationships,
    SetNodeVectorProperty,
    SetRelationshipVectorProperty,
}

#[derive(Clone)]
enum ProcedureImplementation {
    Builtin(BuiltinProcedure),
    Extension(ProcedureHandler),
}

#[derive(Clone)]
pub struct ProcedureDescriptor {
    canonical_name: String,
    display_name: String,
    aliases: Vec<String>,
    signature: String,
    description: String,
    mode: ProcedureMode,
    package_id: Option<String>,
    required_capabilities: Vec<String>,
    required_roles: Vec<String>,
    hidden: bool,
    implementation: ProcedureImplementation,
}

impl fmt::Debug for ProcedureDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProcedureDescriptor")
            .field("canonical_name", &self.canonical_name)
            .field("display_name", &self.display_name)
            .field("aliases", &self.aliases)
            .field("signature", &self.signature)
            .field("description", &self.description)
            .field("mode", &self.mode)
            .field("package_id", &self.package_id)
            .field("required_capabilities", &self.required_capabilities)
            .field("required_roles", &self.required_roles)
            .field("hidden", &self.hidden)
            .finish_non_exhaustive()
    }
}

impl ProcedureDescriptor {
    pub fn extension(
        name: impl Into<String>,
        aliases: impl IntoIterator<Item = impl Into<String>>,
        signature: impl Into<String>,
        description: impl Into<String>,
        mode: ProcedureMode,
        handler: ProcedureHandler,
    ) -> Self {
        let display_name = name.into();
        Self {
            canonical_name: normalize_name(&display_name),
            display_name,
            aliases: aliases.into_iter().map(Into::into).collect(),
            signature: signature.into(),
            description: description.into(),
            mode,
            package_id: None,
            required_capabilities: Vec::new(),
            required_roles: Vec::new(),
            hidden: false,
            implementation: ProcedureImplementation::Extension(handler),
        }
    }

    fn builtin(
        name: &str,
        signature: &str,
        description: &str,
        mode: ProcedureMode,
        implementation: BuiltinProcedure,
    ) -> Self {
        Self {
            canonical_name: normalize_name(name),
            display_name: name.to_string(),
            aliases: Vec::new(),
            signature: signature.to_string(),
            description: description.to_string(),
            mode,
            package_id: None,
            required_capabilities: Vec::new(),
            required_roles: Vec::new(),
            hidden: false,
            implementation: ProcedureImplementation::Builtin(implementation),
        }
    }

    pub fn requiring_capabilities(
        mut self,
        capabilities: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.required_capabilities = capabilities.into_iter().map(Into::into).collect();
        self
    }

    pub fn requiring_roles(mut self, roles: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.required_roles = roles.into_iter().map(Into::into).collect();
        self
    }

    pub fn hidden(mut self) -> Self {
        self.hidden = true;
        self
    }

    pub fn attributed_to(mut self, package_id: impl Into<String>) -> Self {
        self.package_id = Some(package_id.into());
        self
    }

    pub fn canonical_name(&self) -> &str {
        &self.canonical_name
    }

    pub fn name(&self) -> &str {
        &self.display_name
    }

    pub fn aliases(&self) -> &[String] {
        &self.aliases
    }

    pub fn signature(&self) -> &str {
        &self.signature
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn localized_description(&self, language: &copperdb_localization::LanguageTag) -> &str {
        copperdb_localization::matching_procedure_description(
            &self.display_name,
            &self.description,
            language,
        )
    }

    pub fn mode(&self) -> ProcedureMode {
        self.mode
    }

    pub fn package_id(&self) -> Option<&str> {
        self.package_id.as_deref()
    }

    pub fn required_capabilities(&self) -> &[String] {
        &self.required_capabilities
    }

    pub fn required_roles(&self) -> &[String] {
        &self.required_roles
    }

    pub fn is_hidden(&self) -> bool {
        self.hidden
    }

    pub(crate) fn builtin_implementation(&self) -> Option<BuiltinProcedure> {
        match self.implementation {
            ProcedureImplementation::Builtin(implementation) => Some(implementation),
            ProcedureImplementation::Extension(_) => None,
        }
    }

    pub(crate) fn extension_handler(&self) -> Option<&ProcedureHandler> {
        match &self.implementation {
            ProcedureImplementation::Builtin(_) => None,
            ProcedureImplementation::Extension(handler) => Some(handler),
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProcedureRegistryError {
    #[error("procedure name or alias collision: {name}")]
    NameCollision { name: String },
}

#[derive(Debug, Default)]
pub struct ProcedureRegistryBuilder {
    descriptors: Vec<ProcedureDescriptor>,
    names: HashMap<String, usize>,
}

impl ProcedureRegistryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_builtins() -> Self {
        let mut builder = Self::new();
        for descriptor in ProcedureRegistry::builtins().descriptors() {
            builder
                .register(descriptor.clone())
                .expect("built-in procedure names must be unique");
        }
        builder
    }

    pub fn register(
        &mut self,
        descriptor: ProcedureDescriptor,
    ) -> Result<&mut Self, ProcedureRegistryError> {
        let index = self.descriptors.len();
        let mut names = Vec::with_capacity(descriptor.aliases.len() + 1);
        names.push(descriptor.canonical_name.clone());
        names.extend(descriptor.aliases.iter().map(|alias| normalize_name(alias)));
        let mut descriptor_names = HashSet::with_capacity(names.len());
        if let Some(name) = names.iter().find(|name| {
            !descriptor_names.insert((*name).clone()) || self.names.contains_key(*name)
        }) {
            return Err(ProcedureRegistryError::NameCollision { name: name.clone() });
        }
        for name in names {
            self.names.insert(name, index);
        }
        self.descriptors.push(descriptor);
        Ok(self)
    }

    pub fn build(self) -> ProcedureRegistry {
        ProcedureRegistry {
            descriptors: self.descriptors.into(),
            names: self.names,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProcedureRegistry {
    descriptors: Arc<[ProcedureDescriptor]>,
    names: HashMap<String, usize>,
}

impl ProcedureRegistry {
    pub fn builtins() -> &'static Self {
        static BUILTINS: OnceLock<ProcedureRegistry> = OnceLock::new();
        BUILTINS.get_or_init(build_builtin_registry)
    }

    pub fn get(&self, name: &str) -> Option<&ProcedureDescriptor> {
        self.names
            .get(&normalize_name(name))
            .map(|index| &self.descriptors[*index])
    }

    pub fn descriptors(&self) -> &[ProcedureDescriptor] {
        &self.descriptors
    }
}

fn normalize_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

fn build_builtin_registry() -> ProcedureRegistry {
    use BuiltinProcedure as B;
    use ProcedureMode::{Dbms, Read, Write};

    let definitions = [
        ("db.constraints", "db.constraints() :: (name :: STRING, type :: STRING, labelsOrTypes :: LIST<STRING>, properties :: LIST<STRING>, propertyType :: STRING)", "Lists all constraints in the database", Read, B::DbConstraints),
        ("db.index.fulltext.listAvailableAnalyzers", "db.index.fulltext.listAvailableAnalyzers() :: (analyzer :: STRING, description :: STRING)", "Lists available fulltext analyzers", Read, B::FulltextListAnalyzers),
        ("db.index.fulltext.queryNodes", "db.index.fulltext.queryNodes(indexName :: STRING, query :: STRING, options = {} :: MAP) :: (node :: NODE, score :: FLOAT)", "Fulltext search on nodes", Read, B::FulltextQueryNodes),
        ("db.index.vector.queryNodes", "db.index.vector.queryNodes(indexName :: STRING, numberOfResults :: INTEGER, query :: LIST<FLOAT>|STRING|$param) :: (node :: NODE, score :: FLOAT)", "Vector search on nodes", Read, B::VectorQueryNodes),
        ("db.index.fulltext.queryRelationships", "db.index.fulltext.queryRelationships(indexName :: STRING, query :: STRING, options = {} :: MAP) :: (relationship :: RELATIONSHIP, score :: FLOAT)", "Fulltext search on relationships", Read, B::FulltextQueryRelationships),
        ("db.index.vector.queryRelationships", "db.index.vector.queryRelationships(indexName :: STRING, numberOfResults :: INTEGER, query :: LIST<FLOAT>|STRING|$param) :: (relationship :: RELATIONSHIP, score :: FLOAT)", "Vector search on relationships", Read, B::VectorQueryRelationships),
        ("db.create.setNodeVectorProperty", "db.create.setNodeVectorProperty(nodeId :: STRING, propertyKey :: STRING, vector :: LIST<FLOAT>)", "Sets vector property on a node", Write, B::SetNodeVectorProperty),
        ("db.create.setRelationshipVectorProperty", "db.create.setRelationshipVectorProperty(relationshipId :: STRING, propertyKey :: STRING, vector :: LIST<FLOAT>)", "Sets vector property on a relationship", Write, B::SetRelationshipVectorProperty),
        ("db.indexes", "db.indexes() :: (name :: STRING, type :: STRING, labelsOrTypes :: LIST<STRING>, properties :: LIST<STRING>, state :: STRING)", "Lists all indexes in the database", Read, B::DbIndexes),
        ("db.info", "db.info() :: (id :: STRING, name :: STRING, creationDate :: STRING, nodeCount :: INTEGER, relationshipCount :: INTEGER)", "Returns database information", Read, B::DbInfo),
        ("db.labels", "db.labels() :: (label :: STRING)", "Lists all labels in the database", Read, B::DbLabels),
        ("db.ping", "db.ping() :: (success :: BOOLEAN)", "Checks database connectivity", Read, B::DbPing),
        ("db.propertyKeys", "db.propertyKeys() :: (propertyKey :: STRING)", "Lists all property keys in the database", Read, B::DbPropertyKeys),
        ("db.relationshipTypes", "db.relationshipTypes() :: (relationshipType :: STRING)", "Lists all relationship types in the database", Read, B::DbRelationshipTypes),
        ("db.schema.nodeProperties", "db.schema.nodeProperties() :: (nodeLabel :: STRING, propertyName :: STRING, propertyType :: STRING)", "Returns node properties by label", Read, B::DbSchemaNodeProperties),
        ("db.schema.relProperties", "db.schema.relProperties() :: (relType :: STRING, propertyName :: STRING, propertyType :: STRING)", "Returns relationship properties by type", Read, B::DbSchemaRelProperties),
        ("db.schema.visualization", "db.schema.visualization() :: (nodes :: LIST<MAP>, relationships :: LIST<MAP>)", "Visualizes schema", Read, B::DbSchemaVisualization),
        ("dbms.clientConfig", "dbms.clientConfig() :: (name :: STRING, value :: ANY)", "Returns client configuration", Dbms, B::DbmsClientConfig),
        ("dbms.components", "dbms.components() :: (name :: STRING, versions :: LIST<STRING>, edition :: STRING)", "Lists DBMS components", Dbms, B::DbmsComponents),
        ("dbms.functions", "dbms.functions() :: (name :: STRING, signature :: STRING, description :: STRING, category :: STRING, package :: STRING)", "Lists functions", Dbms, B::DbmsFunctions),
        ("dbms.procedures", "dbms.procedures() :: (name :: STRING, signature :: STRING, description :: STRING, mode :: STRING, package :: STRING)", "Lists procedures", Dbms, B::DbmsProcedures),
        ("dbms.info", "dbms.info() :: (id :: STRING, name :: STRING, creationDate :: STRING)", "Returns DBMS information", Dbms, B::DbmsInfo),
        ("dbms.listConfig", "dbms.listConfig() :: (name :: STRING, description :: STRING, value :: ANY, dynamic :: BOOLEAN)", "Lists DBMS configuration", Dbms, B::DbmsListConfig),
        ("dbms.listConnections", "dbms.listConnections() :: (connectionId :: STRING, connectTime :: STRING, connector :: STRING, username :: STRING, userAgent :: STRING, clientAddress :: STRING)", "Lists active DBMS connections", Dbms, B::DbmsListConnections),
        ("nornicdb.decay.info", "nornicdb.decay.info() :: (enabled :: BOOLEAN, system :: STRING, configuredVia :: STRING)", "Returns knowledge-layer scoring configuration", Read, B::NornicDbDecayInfo),
        ("nornicdb.knowledgepolicy.info", "nornicdb.knowledgepolicy.info() :: (enabled :: BOOLEAN, system :: STRING, decayProfiles :: INTEGER, decayBindings :: INTEGER, promotionProfiles :: INTEGER, promotionPolicies :: INTEGER, configuredVia :: STRING)", "Returns knowledge-layer profile and policy catalog counts", Read, B::NornicDbKnowledgePolicyInfo),
        ("nornicdb.knowledgepolicy.resolve", "nornicdb.knowledgepolicy.resolve(entityId :: STRING = '', labelsCsv :: STRING = '', edgeType :: STRING = '') :: (entityId :: STRING, targetKind :: STRING, targetLabels :: STRING, targetEdgeType :: STRING, decayBinding :: STRING, promotionPolicy :: STRING, matchedPromotionProfile :: STRING, matchedPromotionPredicate :: STRING, scoreFrom :: STRING, anchorUnixMs :: INTEGER, accessCount :: INTEGER, lastAccessedAtUnixMs :: INTEGER, baseScore :: FLOAT, finalScore :: FLOAT, visibilityThreshold :: FLOAT, suppressed :: BOOLEAN, dryRun :: BOOLEAN, explanation :: STRING)", "Resolves the effective knowledge-layer scoring policy for an entity, label set, or edge type", Read, B::KnowledgePolicyResolve),
        ("nornicdb.knowledgepolicy.policies", "nornicdb.knowledgepolicy.policies() :: (kind :: STRING, Name :: STRING, Scope :: STRING, Multiplier :: FLOAT, ScoreFloor :: FLOAT, ScoreCap :: FLOAT, Enabled :: BOOLEAN, TargetLabels :: LIST<STRING>, TargetEdgeType :: STRING, IsWildcard :: BOOLEAN, IsEdge :: BOOLEAN)", "Returns knowledge-layer promotion profiles and policies", Read, B::KnowledgePolicyPolicies),
        ("nornicdb.knowledgepolicy.profiles", "nornicdb.knowledgepolicy.profiles() :: (kind :: STRING, Name :: STRING, HalfLifeSeconds :: INTEGER, VisibilityThreshold :: FLOAT, ScoreFloor :: FLOAT, Function :: STRING, Scope :: STRING, DecayEnabled :: BOOLEAN, ScoreFrom :: STRING, ScoreFromProperty :: STRING, Enabled :: BOOLEAN, TargetLabels :: LIST<STRING>, TargetEdgeType :: STRING, IsWildcard :: BOOLEAN, IsEdge :: BOOLEAN, ProfileRef :: STRING, NoDecay :: BOOLEAN, Order :: INTEGER)", "Returns knowledge-layer decay bundles and bindings", Read, B::KnowledgePolicyProfiles),
        ("nornicdb.stats", "nornicdb.stats() :: (nodes :: INTEGER, relationships :: INTEGER, labels :: INTEGER, relationshipTypes :: INTEGER)", "Returns NornicDB stats", Read, B::NornicDbStats),
        ("nornicdb.version", "nornicdb.version() :: (version :: STRING, build :: STRING, edition :: STRING)", "Returns NornicDB version", Read, B::NornicDbVersion),
    ];
    let mut builder = ProcedureRegistryBuilder::new();
    for (name, signature, description, mode, implementation) in definitions {
        builder
            .register(ProcedureDescriptor::builtin(
                name,
                signature,
                description,
                mode,
                implementation,
            ))
            .expect("built-in procedure names must be unique");
    }
    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handler() -> ProcedureHandler {
        Arc::new(|_, _| Ok(ProcedureOutput::default()))
    }

    #[test]
    fn lookup_is_case_insensitive() {
        assert_eq!(
            ProcedureRegistry::builtins()
                .get("DB.LABELS")
                .unwrap()
                .name(),
            "db.labels"
        );
    }

    #[test]
    fn registration_rejects_self_and_existing_collisions() {
        let mut builder = ProcedureRegistryBuilder::new();
        let self_collision = builder
            .register(ProcedureDescriptor::extension(
                "example.one",
                ["EXAMPLE.ONE"],
                "",
                "",
                ProcedureMode::Read,
                handler(),
            ))
            .unwrap_err();
        assert_eq!(
            self_collision,
            ProcedureRegistryError::NameCollision {
                name: "example.one".into()
            }
        );
        builder
            .register(ProcedureDescriptor::extension(
                "example.one",
                ["example.alias"],
                "",
                "",
                ProcedureMode::Read,
                handler(),
            ))
            .unwrap();
        let collision = builder
            .register(ProcedureDescriptor::extension(
                "EXAMPLE.ALIAS",
                std::iter::empty::<&str>(),
                "",
                "",
                ProcedureMode::Read,
                handler(),
            ))
            .unwrap_err();
        assert_eq!(
            collision,
            ProcedureRegistryError::NameCollision {
                name: "example.alias".into()
            }
        );
    }

    #[test]
    fn builtin_lookup_loop_is_stable() {
        let registry = ProcedureRegistry::builtins();
        for _ in 0..10_000 {
            assert!(registry.get("DBMS.PROCEDURES").is_some());
            assert!(registry.get("missing.procedure").is_none());
        }
    }

    #[test]
    fn builtin_procedure_metadata_matches_generated_upstream_inventory() {
        let spanish = copperdb_localization::LanguageTag::parse("es-ES")
            .unwrap()
            .unwrap();
        let registry = ProcedureRegistry::builtins();

        for descriptor in registry.descriptors() {
            assert!(
                copperdb_localization::procedure_description(descriptor.name(), &spanish).is_some(),
                "missing generated metadata for {}",
                descriptor.name()
            );
        }
        assert_eq!(
            registry
                .get("db.info")
                .unwrap()
                .localized_description(&spanish),
            "Devuelve información de la base de datos"
        );
    }
}
