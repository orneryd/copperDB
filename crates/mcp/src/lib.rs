//! Model Context Protocol (MCP) server for copperdb.
//!
//! Equivalent to Go's `pkg/mcp` in NornicDB.
//! Exposes copperdb as an MCP tool provider, allowing LLMs (Claude, GPT-4, etc.)
//! to query the graph database directly via tool calling.
//!
//! ## MCP Overview
//! MCP is an open protocol for connecting LLMs to data sources.
//! https://modelcontextprotocol.io/

use async_trait::async_trait;
use copperdb_engine::CopperDb as GraphEngine;
use copperdb_util::RequestContext;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use thiserror::Error;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

pub const MCP_PROTOCOL_VERSION: &str = "2024-11-05";
pub const DEFAULT_MAX_REQUEST_BYTES: usize = 10 * 1024 * 1024;
pub const DEFAULT_MAX_BATCH_ENTRIES: usize = 32;
pub const DEFAULT_MAX_TOOL_OUTPUT_BYTES: usize = 1024 * 1024;

#[derive(Debug, Error)]
pub enum McpError {
    #[error("stdio transport error: {0}")]
    Stdio(#[from] std::io::Error),
    #[error("MCP serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("JSON-RPC error: {0}")]
    JsonRpc(String),
    #[error("tool not found: {0}")]
    ToolNotFound(String),
    #[error("invalid params: {0}")]
    InvalidParams(String),
    #[error("invalid schema for tool {tool}: {message}")]
    InvalidSchema { tool: String, message: String },
    #[error("execution error: {0}")]
    ExecutionError(String),
    #[error("tool {tool} output is {actual_bytes} bytes; limit is {max_bytes} bytes")]
    OutputTooLarge {
        tool: String,
        actual_bytes: usize,
        max_bytes: usize,
    },
}

/// MCP tool definition (exposed to LLMs).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tool {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

/// MCP JSON-RPC 2.0 request.
#[derive(Debug, Clone, Serialize)]
pub struct McpRequest {
    pub jsonrpc: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<Option<serde_json::Value>>,
    pub method: String,
    pub params: Option<serde_json::Value>,
}

impl<'de> Deserialize<'de> for McpRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RequestFields {
            jsonrpc: String,
            method: String,
            params: Option<serde_json::Value>,
        }

        let mut value = serde_json::Value::deserialize(deserializer)?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| serde::de::Error::custom("MCP request must be an object"))?;
        let id = object
            .remove("id")
            .map(|id| if id.is_null() { None } else { Some(id) });
        let fields: RequestFields =
            serde_json::from_value(value).map_err(serde::de::Error::custom)?;
        Ok(Self {
            jsonrpc: fields.jsonrpc,
            id,
            method: fields.method,
            params: fields.params,
        })
    }
}

impl McpRequest {
    pub fn new(
        id: impl Into<serde_json::Value>,
        method: impl Into<String>,
        params: Option<serde_json::Value>,
    ) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id: Some(Some(id.into())),
            method: method.into(),
            params,
        }
    }

    pub fn is_notification(&self) -> bool {
        self.id.is_none()
    }

    pub fn take_database(&mut self) -> Option<String> {
        if self.method != "tools/call" {
            return None;
        }
        let arguments = self
            .params
            .as_mut()?
            .as_object_mut()?
            .get_mut("arguments")?
            .as_object_mut()?;
        let database = arguments
            .remove("database")
            .and_then(|value| value.as_str().map(str::to_owned));
        let alias = arguments
            .remove("db")
            .and_then(|value| value.as_str().map(str::to_owned));
        database
            .filter(|database| !database.is_empty())
            .or(alias)
            .map(|database| database.trim().to_owned())
            .filter(|database| !database.is_empty())
    }

    fn response_id(&self) -> serde_json::Value {
        self.id.clone().flatten().unwrap_or(serde_json::Value::Null)
    }
}

/// MCP JSON-RPC 2.0 response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpResponse {
    pub jsonrpc: String,
    pub id: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<McpResponseError>,
}

impl McpResponse {
    pub fn ok(id: serde_json::Value, result: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn error(id: serde_json::Value, code: i32, message: impl Into<String>) -> Self {
        Self::error_with_data(id, code, message, None)
    }

    pub fn error_with_data(
        id: serde_json::Value,
        code: i32,
        message: impl Into<String>,
        data: Option<serde_json::Value>,
    ) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: None,
            error: Some(McpResponseError {
                code,
                message: message.into(),
                data,
            }),
        }
    }
}

/// JSON-RPC error object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpResponseError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// Tool call parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolCallParams {
    pub name: String,
    pub arguments: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InitializeParams {
    pub protocol_version: Option<String>,
    #[serde(default)]
    pub capabilities: serde_json::Map<String, serde_json::Value>,
    pub client_info: Option<ClientInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientInfo {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolAccess {
    Read,
    Write,
    Admin,
}

#[derive(Clone)]
pub struct ToolExecutionContext {
    request_context: RequestContext,
    engine: Option<Arc<GraphEngine>>,
    roles: Vec<String>,
}

impl ToolExecutionContext {
    pub fn request_context(&self) -> &RequestContext {
        &self.request_context
    }

    pub fn engine(&self) -> Option<&Arc<GraphEngine>> {
        self.engine.as_ref()
    }

    pub fn roles(&self) -> &[String] {
        &self.roles
    }
}

#[async_trait]
pub trait ToolHandler: Send + Sync {
    async fn call(
        &self,
        context: ToolExecutionContext,
        arguments: serde_json::Value,
    ) -> Result<serde_json::Value, String>;
}

struct RegisteredTool {
    descriptor: Tool,
    validator: jsonschema::Validator,
    access: ToolAccess,
    handler: Arc<dyn ToolHandler>,
}

impl ToolAccess {
    pub fn requires_write(self) -> bool {
        matches!(self, Self::Write | Self::Admin)
    }

    pub fn requires_admin(self) -> bool {
        self == Self::Admin
    }
}

/// Registry of available MCP tools.
pub struct ToolRegistry {
    tools: HashMap<String, RegisteredTool>,
    engine: Option<Arc<GraphEngine>>,
    roles: Vec<String>,
    max_tool_output_bytes: usize,
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self {
            tools: HashMap::new(),
            engine: None,
            roles: Vec::new(),
            max_tool_output_bytes: DEFAULT_MAX_TOOL_OUTPUT_BYTES,
        }
    }
}

impl ToolRegistry {
    pub fn new() -> Self {
        let mut registry = Self::default();
        for (tool, access, handler) in copperdb_tools_with_access() {
            registry
                .register_with_handler(tool, access, handler)
                .expect("built-in MCP tool schemas must be valid");
        }
        registry
    }

    /// Create a registry with a graph engine for real Cypher execution.
    pub fn with_engine(engine: Arc<GraphEngine>) -> Self {
        Self::with_engine_and_roles(engine, vec!["admin".into()])
    }

    pub fn with_engine_and_roles(engine: Arc<GraphEngine>, roles: Vec<String>) -> Self {
        let mut registry = Self::new();
        registry.engine = Some(engine);
        registry.roles = roles;
        registry
    }

    #[cfg(test)]
    fn with_max_tool_output_bytes(mut self, max_tool_output_bytes: usize) -> Self {
        self.max_tool_output_bytes = max_tool_output_bytes;
        self
    }

    pub fn register_with_handler(
        &mut self,
        tool: Tool,
        access: ToolAccess,
        handler: Arc<dyn ToolHandler>,
    ) -> Result<(), McpError> {
        let validator = jsonschema::validator_for(&tool.input_schema).map_err(|error| {
            McpError::InvalidSchema {
                tool: tool.name.clone(),
                message: error.to_string(),
            }
        })?;
        self.tools.insert(
            tool.name.clone(),
            RegisteredTool {
                descriptor: tool,
                validator,
                access,
                handler,
            },
        );
        Ok(())
    }

    pub fn required_access(&self, request: &McpRequest) -> Result<Option<ToolAccess>, McpError> {
        if request.method != "tools/call" {
            return Ok(None);
        }
        let params = request
            .params
            .clone()
            .ok_or_else(|| McpError::InvalidParams("missing params".into()))?;
        let call: ToolCallParams = serde_json::from_value(params)
            .map_err(|error| McpError::InvalidParams(error.to_string()))?;
        self.tools
            .get(&call.name)
            .map(|tool| tool.access)
            .map(Some)
            .ok_or(McpError::ToolNotFound(call.name))
    }

    pub fn get(&self, name: &str) -> Option<&Tool> {
        self.tools.get(name).map(|tool| &tool.descriptor)
    }

    pub fn list(&self) -> Vec<&Tool> {
        let mut tools: Vec<&Tool> = self.tools.values().map(|tool| &tool.descriptor).collect();
        tools.sort_by_key(|t| &t.name);
        tools
    }

    pub fn len(&self) -> usize {
        self.tools.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }

    /// Dispatch an MCP request. Returns an MCP response.
    pub async fn dispatch(&self, request: &McpRequest) -> McpResponse {
        let request_context = RequestContext::detached();
        self.dispatch_with_context(&request_context, request).await
    }

    pub async fn dispatch_with_context(
        &self,
        request_context: &RequestContext,
        request: &McpRequest,
    ) -> McpResponse {
        if request.jsonrpc != "2.0" {
            return McpResponse::error_with_data(
                request.response_id(),
                -32600,
                "Invalid Request",
                Some(serde_json::json!({"expected": "2.0"})),
            );
        }
        match request.method.as_str() {
            "tools/list" => {
                let tools: Vec<&Tool> = self.list();
                McpResponse::ok(request.response_id(), serde_json::json!({ "tools": tools }))
            }
            "tools/call" => {
                let params = match &request.params {
                    Some(p) => p,
                    None => {
                        return McpResponse::error(request.response_id(), -32602, "missing params")
                    }
                };
                let call: ToolCallParams = match serde_json::from_value(params.clone()) {
                    Ok(c) => c,
                    Err(e) => {
                        return McpResponse::error(request.response_id(), -32602, e.to_string())
                    }
                };
                let Some(tool) = self.tools.get(&call.name) else {
                    return McpResponse::error(
                        request.response_id(),
                        -32601,
                        format!("tool not found: {}", call.name),
                    );
                };
                let arguments = call
                    .arguments
                    .clone()
                    .unwrap_or_else(|| serde_json::json!({}));
                if let Err(error) = tool.validator.validate(&arguments) {
                    return McpResponse::error_with_data(
                        request.response_id(),
                        -32602,
                        "Invalid params",
                        Some(serde_json::json!({
                            "tool": call.name,
                            "kind": "schema_validation",
                            "detail": error.to_string()
                        })),
                    );
                }
                let handler = Arc::clone(&tool.handler);
                let context = ToolExecutionContext {
                    request_context: request_context.clone(),
                    engine: self.engine.clone(),
                    roles: self.roles.clone(),
                };
                match handler.call(context, arguments).await {
                    Ok(result) => McpResponse::ok(
                        request.response_id(),
                        self.enforce_tool_output_limit(&call.name, result),
                    ),
                    Err(e) => McpResponse::error(request.response_id(), -32000, e),
                }
            }
            "initialize" => {
                let params = match request.params.clone() {
                    Some(params) => match serde_json::from_value::<InitializeParams>(params) {
                        Ok(params) => params,
                        Err(error) => {
                            return McpResponse::error_with_data(
                                request.response_id(),
                                -32602,
                                "Invalid params",
                                Some(serde_json::json!({"detail": error.to_string()})),
                            )
                        }
                    },
                    None => InitializeParams::default(),
                };
                if params
                    .protocol_version
                    .as_deref()
                    .is_some_and(|version| version != MCP_PROTOCOL_VERSION)
                {
                    return McpResponse::error_with_data(
                        request.response_id(),
                        -32602,
                        "Unsupported protocol version",
                        Some(serde_json::json!({"supported": [MCP_PROTOCOL_VERSION]})),
                    );
                }
                McpResponse::ok(
                    request.response_id(),
                    serde_json::json!({
                        "protocolVersion": MCP_PROTOCOL_VERSION,
                        "capabilities": { "tools": {"listChanged": false} },
                        "serverInfo": { "name": "copperdb", "version": env!("CARGO_PKG_VERSION") }
                    }),
                )
            }
            "notifications/initialized" => McpResponse::ok(
                request.response_id(),
                serde_json::Value::Object(serde_json::Map::new()),
            ),
            _ => McpResponse::error(request.response_id(), -32601, "method not found"),
        }
    }

    fn enforce_tool_output_limit(
        &self,
        tool: &str,
        result: serde_json::Value,
    ) -> serde_json::Value {
        let actual_bytes = serde_json::to_vec(&result).map_or(usize::MAX, |bytes| bytes.len());
        if actual_bytes <= self.max_tool_output_bytes {
            return result;
        }
        let error = McpError::OutputTooLarge {
            tool: tool.to_string(),
            actual_bytes,
            max_bytes: self.max_tool_output_bytes,
        };
        serde_json::json!({
            "content": [{"type": "text", "text": error.to_string()}],
            "isError": true,
            "_meta": {
                "kind": "output_too_large",
                "tool": tool,
                "actualBytes": actual_bytes,
                "maxBytes": self.max_tool_output_bytes
            }
        })
    }
}

pub async fn serve_stdio<R, W>(
    registry: &ToolRegistry,
    reader: R,
    writer: W,
) -> Result<(), McpError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    serve_stdio_with(reader, writer, |request| async move {
        registry.dispatch(&request).await
    })
    .await
}

pub async fn serve_stdio_with<R, W, D, F>(
    mut reader: R,
    mut writer: W,
    mut dispatch: D,
) -> Result<(), McpError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
    D: FnMut(McpRequest) -> F,
    F: Future<Output = McpResponse>,
{
    while let Some(message) = read_stdio_message(&mut reader).await? {
        let response = match message {
            StdioMessage::Payload(payload) => match serde_json::from_slice(&payload) {
                Ok(value) => dispatch_stdio_value(&mut dispatch, value).await?,
                Err(error) => Some(serde_json::to_value(McpResponse::error_with_data(
                    serde_json::Value::Null,
                    -32700,
                    "Parse error",
                    Some(serde_json::json!({"detail": error.to_string()})),
                ))?),
            },
            StdioMessage::TooLarge(actual_bytes) => {
                Some(serde_json::to_value(McpResponse::error_with_data(
                    serde_json::Value::Null,
                    -32600,
                    "Invalid Request",
                    Some(serde_json::json!({
                        "kind": "request_too_large",
                        "actualBytes": actual_bytes,
                        "maxBytes": DEFAULT_MAX_REQUEST_BYTES
                    })),
                ))?)
            }
        };
        if let Some(response) = response {
            let mut encoded = serde_json::to_vec(&response)?;
            encoded.push(b'\n');
            writer.write_all(&encoded).await?;
            writer.flush().await?;
        }
    }
    Ok(())
}

enum StdioMessage {
    Payload(Vec<u8>),
    TooLarge(usize),
}

async fn read_stdio_message<R>(reader: &mut R) -> std::io::Result<Option<StdioMessage>>
where
    R: AsyncBufRead + Unpin,
{
    let mut payload = Vec::new();
    let mut actual_bytes = 0usize;
    let mut read_any = false;

    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if !read_any {
                return Ok(None);
            }
            break;
        }
        read_any = true;
        let newline = available.iter().position(|byte| *byte == b'\n');
        let payload_bytes = newline.unwrap_or(available.len());
        actual_bytes = actual_bytes.saturating_add(payload_bytes);
        if payload.len() < DEFAULT_MAX_REQUEST_BYTES {
            let retained = payload_bytes.min(DEFAULT_MAX_REQUEST_BYTES - payload.len());
            payload.extend_from_slice(&available[..retained]);
        }
        let consumed = newline.map_or(available.len(), |position| position + 1);
        reader.consume(consumed);
        if newline.is_some() {
            break;
        }
    }

    if actual_bytes > DEFAULT_MAX_REQUEST_BYTES {
        Ok(Some(StdioMessage::TooLarge(actual_bytes)))
    } else {
        Ok(Some(StdioMessage::Payload(payload)))
    }
}

async fn dispatch_stdio_value<D, F>(
    dispatch: &mut D,
    value: serde_json::Value,
) -> Result<Option<serde_json::Value>, McpError>
where
    D: FnMut(McpRequest) -> F,
    F: Future<Output = McpResponse>,
{
    if let serde_json::Value::Array(entries) = value {
        if entries.is_empty() || entries.len() > DEFAULT_MAX_BATCH_ENTRIES {
            return Ok(Some(serde_json::to_value(McpResponse::error_with_data(
                serde_json::Value::Null,
                -32600,
                "Invalid Request",
                Some(serde_json::json!({
                    "kind": "invalid_batch_size",
                    "maxEntries": DEFAULT_MAX_BATCH_ENTRIES
                })),
            ))?));
        }
        let mut responses = Vec::with_capacity(entries.len());
        for entry in entries {
            if let Some(response) = dispatch_stdio_entry(dispatch, entry).await? {
                responses.push(response);
            }
        }
        return Ok((!responses.is_empty()).then_some(serde_json::Value::Array(responses)));
    }
    dispatch_stdio_entry(dispatch, value).await
}

async fn dispatch_stdio_entry<D, F>(
    dispatch: &mut D,
    value: serde_json::Value,
) -> Result<Option<serde_json::Value>, McpError>
where
    D: FnMut(McpRequest) -> F,
    F: Future<Output = McpResponse>,
{
    let request = match serde_json::from_value::<McpRequest>(value) {
        Ok(request) => request,
        Err(error) => {
            return Ok(Some(serde_json::to_value(McpResponse::error_with_data(
                serde_json::Value::Null,
                -32600,
                "Invalid Request",
                Some(serde_json::json!({"detail": error.to_string()})),
            ))?));
        }
    };
    let is_notification = request.is_notification();
    let response = dispatch(request).await;
    if is_notification {
        Ok(None)
    } else {
        Ok(Some(serde_json::to_value(response)?))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunCypherArgs {
    query: String,
    #[serde(default)]
    params: HashMap<String, serde_json::Value>,
    #[serde(default, rename = "database")]
    _database: Option<String>,
}

struct RunCypherHandler;

#[async_trait]
impl ToolHandler for RunCypherHandler {
    async fn call(
        &self,
        context: ToolExecutionContext,
        arguments: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let arguments: RunCypherArgs =
            serde_json::from_value(arguments).map_err(|error| error.to_string())?;
        let engine = context.engine.ok_or("no graph engine configured")?;
        let result = tokio::task::spawn_blocking(move || {
            engine.execute_as_with_context(
                &context.request_context,
                &arguments.query,
                arguments.params,
                &context.roles,
            )
        })
        .await
        .map_err(|error| error.to_string())?;
        match result {
            Ok(result) => Ok(serde_json::json!({
                "content": [{
                    "type": "text",
                    "text": format!(
                        "Query executed successfully.\nRows: {}\nStats: {:?}",
                        result.rows.len(), result.stats
                    )
                }]
            })),
            Err(error) => Ok(serde_json::json!({
                "content": [{ "type": "text", "text": format!("Error: {error}") }],
                "isError": true
            })),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FindSimilarArgs {
    text: String,
    #[serde(default = "default_search_limit")]
    k: usize,
    label: Option<String>,
    #[serde(default, rename = "database")]
    _database: Option<String>,
}

fn default_search_limit() -> usize {
    10
}

struct FindSimilarHandler;

#[async_trait]
impl ToolHandler for FindSimilarHandler {
    async fn call(
        &self,
        context: ToolExecutionContext,
        arguments: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let arguments: FindSimilarArgs =
            serde_json::from_value(arguments).map_err(|error| error.to_string())?;
        let engine = context.engine.ok_or("no graph engine configured")?;
        let result = tokio::task::spawn_blocking(move || {
            let labels = arguments.label.into_iter().collect::<Vec<_>>();
            engine.search_fulltext_nodes_with_context(
                &context.request_context,
                "",
                &labels,
                &arguments.text,
                arguments.k,
            )
        })
        .await
        .map_err(|error| error.to_string())?;
        match result {
            Ok(results) => Ok(serde_json::json!({
                "content": [{
                    "type": "text",
                    "text": serde_json::to_string_pretty(&results).unwrap_or_else(|_| "[]".into())
                }]
            })),
            Err(error) => Ok(serde_json::json!({
                "content": [{ "type": "text", "text": format!("Search error: {error}") }],
                "isError": true
            })),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoreArgs {
    content: String,
    #[serde(default)]
    labels: Vec<String>,
    #[serde(default, rename = "type")]
    node_type: Option<String>,
    title: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    metadata: serde_json::Map<String, serde_json::Value>,
    #[serde(default, rename = "database")]
    _database: Option<String>,
}

struct StoreHandler;

#[async_trait]
impl ToolHandler for StoreHandler {
    async fn call(
        &self,
        context: ToolExecutionContext,
        arguments: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let arguments: StoreArgs =
            serde_json::from_value(arguments).map_err(|error| error.to_string())?;
        if arguments.content.is_empty() {
            return Err("content is required".into());
        }
        let labels = store_labels(arguments.labels, arguments.node_type)?;
        let title = arguments
            .title
            .filter(|title| !title.is_empty())
            .unwrap_or_else(|| generate_title(&arguments.content, 100));
        let mut properties = serde_json::Map::from_iter([
            ("title".into(), serde_json::Value::String(title.clone())),
            (
                "content".into(),
                serde_json::Value::String(arguments.content),
            ),
            (
                "created_at".into(),
                serde_json::Value::String(
                    OffsetDateTime::now_utc()
                        .format(&Rfc3339)
                        .map_err(|error| error.to_string())?,
                ),
            ),
        ]);
        if !arguments.tags.is_empty() {
            properties.insert("tags".into(), serde_json::json!(arguments.tags));
        }
        properties.extend(arguments.metadata);
        for reserved in ["embedding", "embeddings", "vector"] {
            properties.remove(reserved);
        }

        let engine = context.engine.ok_or("no graph engine configured")?;
        let query = format!(
            "CREATE (n:{}) SET n += $props RETURN elementId(n) AS id",
            labels.join(":")
        );
        let result = tokio::task::spawn_blocking(move || {
            let mut transaction = engine.begin_storage_transaction()?;
            let result = engine.execute_in_storage_transaction_as_with_context(
                &context.request_context,
                &mut transaction,
                &query,
                HashMap::from([("props".into(), serde_json::Value::Object(properties))]),
                &context.roles,
            )?;
            transaction.commit()?;
            Ok::<_, copperdb_engine::CopperDbError>(result)
        })
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| format!("failed to store node: {error}"))?;
        let id = result
            .rows
            .first()
            .and_then(|row| row.get("id"))
            .and_then(serde_json::Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or("store returned invalid node id")?;
        Ok(serde_json::json!({
            "id": id,
            "title": title,
            "embedded": true
        }))
    }
}

fn store_labels(labels: Vec<String>, node_type: Option<String>) -> Result<Vec<String>, String> {
    let labels = if labels.is_empty() {
        vec![node_type
            .filter(|label| !label.is_empty())
            .unwrap_or_else(|| "Memory".into())]
    } else {
        labels
    };
    for label in &labels {
        let mut characters = label.chars();
        if !characters
            .next()
            .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
            || !characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
        {
            return Err(format!(
                "invalid label: {label:?} (must be a valid identifier)"
            ));
        }
    }
    Ok(labels)
}

fn generate_title(content: &str, max_characters: usize) -> String {
    let title = content
        .split_once('\n')
        .map_or(content, |(first_line, _)| first_line)
        .trim();
    if title.chars().count() <= max_characters {
        return title.into();
    }
    if max_characters <= 3 {
        return title.chars().take(max_characters).collect();
    }
    format!(
        "{}...",
        title.chars().take(max_characters - 3).collect::<String>()
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecallArgs {
    #[serde(default)]
    id: String,
    #[serde(default, rename = "type")]
    node_types: Vec<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    since: String,
    #[serde(default = "default_search_limit")]
    limit: usize,
    #[serde(default, rename = "database")]
    _database: Option<String>,
}

struct RecallHandler;

#[async_trait]
impl ToolHandler for RecallHandler {
    async fn call(
        &self,
        context: ToolExecutionContext,
        arguments: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let arguments: RecallArgs =
            serde_json::from_value(arguments).map_err(|error| error.to_string())?;
        let engine = context.engine.ok_or("no graph engine configured")?;
        let requested_id = arguments.id.clone();
        let nodes = tokio::task::spawn_blocking(move || {
            if !arguments.id.is_empty() {
                let node = engine.get_readable_node_with_context(
                    &context.request_context,
                    &arguments.id,
                    &context.roles,
                )?;
                return Ok::<_, copperdb_engine::CopperDbError>(
                    node.and_then(|node| {
                        recall_node_from_parts(
                            node.id,
                            &node.labels,
                            serde_json::Map::from_iter(node.properties),
                        )
                    })
                    .into_iter()
                    .collect(),
                );
            }
            let (query, params) = if arguments.id.is_empty() {
                let mut query = "MATCH (n)".to_string();
                let mut params = HashMap::new();
                if !arguments.since.is_empty() {
                    query.push_str(" WHERE coalesce(n.created_at, '') >= $since");
                    params.insert("since".into(), serde_json::json!(arguments.since));
                }
                query.push_str(
                    " RETURN elementId(n) AS id, labels(n) AS labels, n AS properties LIMIT $limit",
                );
                params.insert("limit".into(), serde_json::json!(arguments.limit));
                (query, params)
            } else {
                unreachable!("direct ID recall returns before filter query construction")
            };
            let result = engine.execute_as_with_context(
                &context.request_context,
                &query,
                params,
                &context.roles,
            )?;
            let mut nodes = Vec::new();
            for mut row in result.rows {
                let labels = row
                    .get("labels")
                    .and_then(serde_json::Value::as_array)
                    .map(|labels| {
                        labels
                            .iter()
                            .filter_map(serde_json::Value::as_str)
                            .map(str::to_owned)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let Some(properties) = row.get("properties").and_then(serde_json::Value::as_object)
                else {
                    continue;
                };
                let Some(properties) =
                    engine.filter_readable_node_properties(&labels, properties, &context.roles)?
                else {
                    continue;
                };
                row.insert("properties".into(), serde_json::Value::Object(properties));
                let Some(node) = recall_node(&row) else {
                    continue;
                };
                if arguments
                    .node_types
                    .first()
                    .is_some_and(|node_type| node["type"] != *node_type)
                {
                    continue;
                }
                if !has_all_tags(&node["properties"], &arguments.tags) {
                    continue;
                }
                nodes.push(node);
                if nodes.len() >= arguments.limit {
                    break;
                }
            }
            Ok::<_, copperdb_engine::CopperDbError>(nodes)
        })
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| format!("failed to recall nodes: {error}"))?;
        if !requested_id.is_empty() && nodes.is_empty() {
            return Err(format!("node not found: {requested_id}"));
        }
        Ok(serde_json::json!({"count": nodes.len(), "nodes": nodes}))
    }
}

fn recall_node(row: &HashMap<String, serde_json::Value>) -> Option<serde_json::Value> {
    let id = row.get("id")?.as_str()?.to_string();
    let labels = row
        .get("labels")?
        .as_array()?
        .iter()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let properties = row.get("properties")?.as_object()?;
    recall_node_from_parts(id, &labels, properties.clone())
}

fn recall_node_from_parts(
    id: String,
    labels: &[String],
    properties: serde_json::Map<String, serde_json::Value>,
) -> Option<serde_json::Value> {
    let node_type = labels.first().map(String::as_str).unwrap_or("Node");
    let title = properties
        .get("title")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let content = properties
        .get("content")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let properties = sanitize_properties_for_llm(&properties);
    let mut node = serde_json::Map::from_iter([
        ("id".into(), serde_json::json!(id)),
        ("type".into(), serde_json::json!(node_type)),
    ]);
    if !title.is_empty() {
        node.insert("title".into(), serde_json::json!(title));
    }
    if !content.is_empty() {
        node.insert("content".into(), serde_json::json!(content));
    }
    if !properties.is_empty() {
        node.insert("properties".into(), serde_json::Value::Object(properties));
    }
    Some(serde_json::Value::Object(node))
}

fn has_all_tags(properties: &serde_json::Value, requested: &[String]) -> bool {
    requested.iter().all(|requested_tag| {
        properties
            .get("tags")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|tags| tags.iter().any(|tag| tag == requested_tag))
    })
}

fn sanitize_properties_for_llm(
    properties: &serde_json::Map<String, serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    properties
        .iter()
        .filter(|(name, value)| {
            !matches!(
                name.as_str(),
                "embedding"
                    | "embedding_model"
                    | "embedding_dimensions"
                    | "has_embedding"
                    | "embedded_at"
            ) && value.as_array().is_none_or(|values| values.len() <= 100)
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

/// Built-in copperdb MCP tools.
pub fn copperdb_tools() -> Vec<Tool> {
    copperdb_tools_with_access()
        .into_iter()
        .map(|(tool, _, _)| tool)
        .collect()
}

fn copperdb_tools_with_access() -> Vec<(Tool, ToolAccess, Arc<dyn ToolHandler>)> {
    vec![
        (
            Tool {
                name: "store".into(),
                description: "Store a piece of knowledge, decision, or information as a labeled node in the graph. Returns its node ID for future reference.".into(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "content": {"type": "string", "minLength": 1, "description": "The main content to store."},
                        "labels": {"type": "array", "items": {"type": "string"}, "description": "Node labels. Takes precedence over type."},
                        "type": {"type": "string", "description": "Single node label used when labels is omitted. Defaults to Memory."},
                        "title": {"type": "string", "description": "Optional title. Generated from content when omitted."},
                        "tags": {"type": "array", "items": {"type": "string"}, "description": "Tags stored on the node."},
                        "metadata": {"type": "object", "description": "Additional node properties.", "additionalProperties": true},
                        "database": {"type": "string", "description": "Database name to use. If omitted, uses the server's configured default database."}
                    },
                    "required": ["content"],
                    "additionalProperties": false
                }),
            },
            ToolAccess::Write,
            Arc::new(StoreHandler),
        ),
        (
            Tool {
                name: "recall".into(),
                description: "Retrieve specific knowledge by ID, or search by criteria (type, tags, date range). Use when you know what you're looking for; use discover for semantic search.".into(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "id": {"type": "string", "description": "Node ID to retrieve directly. If provided, ignores other filters."},
                        "type": {"type": "array", "items": {"type": "string"}, "description": "Filter by node types."},
                        "tags": {"type": "array", "items": {"type": "string"}, "description": "Filter by tags; nodes must have all specified tags."},
                        "since": {"type": "string", "format": "date-time", "description": "Filter by creation time in RFC3339 format."},
                        "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 10, "description": "Maximum number of results."},
                        "database": {"type": "string", "description": "Database name to use. If omitted, uses the server's configured default database."}
                    },
                    "additionalProperties": false
                }),
            },
            ToolAccess::Read,
            Arc::new(RecallHandler),
        ),
        (
            Tool {
                name: "run_cypher".into(),
                description: "Execute a Cypher query against the copperdb graph database".into(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "query": { "type": "string", "description": "The Cypher query to execute" },
                        "params": { "type": "object", "description": "Query parameters" },
                        "database": { "type": "string", "description": "Database name to use. If omitted, uses the server's configured default database." }
                    },
                    "required": ["query"]
                    ,"additionalProperties": false
                }),
            },
            ToolAccess::Admin,
            Arc::new(RunCypherHandler),
        ),
        (
            Tool {
                name: "find_similar".into(),
                description: "Find semantically similar nodes using vector search".into(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "text": { "type": "string", "description": "Text to find similar nodes for" },
                        "k": { "type": "integer", "minimum": 1, "maximum": 100, "description": "Number of results", "default": 10 },
                        "label": { "type": "string", "description": "Filter by node label" },
                        "database": { "type": "string", "description": "Database name to use. If omitted, uses the server's configured default database." }
                    },
                    "required": ["text"],
                    "additionalProperties": false
                }),
            },
            ToolAccess::Read,
            Arc::new(FindSimilarHandler),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, BufReader};

    struct EchoHandler;

    struct MustNotRunHandler;

    #[async_trait]
    impl ToolHandler for EchoHandler {
        async fn call(
            &self,
            context: ToolExecutionContext,
            arguments: serde_json::Value,
        ) -> Result<serde_json::Value, String> {
            tokio::task::yield_now().await;
            Ok(serde_json::json!({
                "arguments": arguments,
                "cancelled": context.request_context().is_cancelled(),
                "hasEngine": context.engine().is_some(),
                "roles": context.roles()
            }))
        }
    }

    #[async_trait]
    impl ToolHandler for MustNotRunHandler {
        async fn call(
            &self,
            _context: ToolExecutionContext,
            _arguments: serde_json::Value,
        ) -> Result<serde_json::Value, String> {
            panic!("schema-invalid arguments reached the tool handler")
        }
    }

    async fn run_stdio(input: impl AsRef<[u8]>) -> Vec<serde_json::Value> {
        let (server_reader, mut client_writer) = tokio::io::duplex(1024 * 1024);
        let (mut client_reader, server_writer) = tokio::io::duplex(1024 * 1024);
        let server = tokio::spawn(async move {
            serve_stdio(
                &ToolRegistry::new(),
                BufReader::new(server_reader),
                server_writer,
            )
            .await
            .unwrap();
        });
        client_writer.write_all(input.as_ref()).await.unwrap();
        client_writer.shutdown().await.unwrap();
        let mut output = String::new();
        client_reader.read_to_string(&mut output).await.unwrap();
        server.await.unwrap();
        output
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn stdio_frames_requests_notifications_batches_and_errors() {
        let input = concat!(
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}\n",
            "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n",
            "[{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"initialize\"},",
            "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}]\n",
            "[{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}]\n",
            "[]\n",
            "42\n",
            "not-json\n"
        );

        let responses = run_stdio(input).await;

        assert_eq!(responses.len(), 5);
        assert_eq!(responses[0]["id"], 1);
        assert_eq!(responses[0]["result"]["tools"].as_array().unwrap().len(), 4);
        assert_eq!(responses[1].as_array().unwrap().len(), 1);
        assert_eq!(responses[1][0]["id"], 2);
        assert_eq!(responses[2]["error"]["code"], -32600);
        assert_eq!(responses[2]["error"]["data"]["kind"], "invalid_batch_size");
        assert_eq!(responses[3]["error"]["code"], -32600);
        assert_eq!(responses[4]["error"]["code"], -32700);
    }

    #[tokio::test]
    async fn stdio_bounds_input_and_recovers_at_the_next_message() {
        let mut input = vec![b' '; DEFAULT_MAX_REQUEST_BYTES + 1];
        input.extend_from_slice(b"\n{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/list\"}\n");

        let responses = run_stdio(input).await;

        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0]["error"]["code"], -32600);
        assert_eq!(responses[0]["error"]["data"]["kind"], "request_too_large");
        assert_eq!(
            responses[0]["error"]["data"]["actualBytes"],
            DEFAULT_MAX_REQUEST_BYTES + 1
        );
        assert_eq!(responses[1]["id"], 3);
    }

    #[test]
    fn test_tool_registry_default_tools() {
        let registry = ToolRegistry::new();
        assert_eq!(registry.len(), 4);
        for name in ["store", "recall", "run_cypher", "find_similar"] {
            let tool = registry.get(name).expect("built-in tool");
            assert_eq!(
                tool.input_schema["properties"]["database"]["type"],
                "string"
            );
            assert!(tool.input_schema["properties"]["database"]["description"]
                .as_str()
                .unwrap()
                .contains("configured default database"));
        }
    }

    #[test]
    fn test_tool_registry_register() {
        let mut registry = ToolRegistry::new();
        registry
            .register_with_handler(
                Tool {
                    name: "custom_tool".into(),
                    description: "A custom tool".into(),
                    input_schema: serde_json::json!({}),
                },
                ToolAccess::Read,
                Arc::new(EchoHandler),
            )
            .unwrap();
        assert_eq!(registry.len(), 5);
    }

    #[tokio::test]
    async fn store_persists_upstream_fields_and_audits_the_transaction() {
        let engine = Arc::new(copperdb_engine::CopperDb::open_temporary().unwrap());
        let registry = ToolRegistry::with_engine_and_roles(engine.clone(), vec!["admin".into()]);
        let request = McpRequest::new(
            22,
            "tools/call",
            Some(serde_json::json!({
                "name": "store",
                "arguments": {
                    "content": "First line\nSecond line should not become title",
                    "labels": ["Note", "Knowledge"],
                    "type": "Ignored",
                    "tags": ["alpha", "beta"],
                    "metadata": {
                        "owner": "alice",
                        "embedding": [1, 2, 3],
                        "embeddings": {"default": [1, 2, 3]},
                        "vector": "drop-me"
                    }
                }
            })),
        );

        assert_eq!(
            registry.required_access(&request).unwrap(),
            Some(ToolAccess::Write)
        );
        let response = registry.dispatch(&request).await;
        assert!(response.error.is_none(), "{:?}", response.error);
        let result = response.result.unwrap();
        assert_eq!(result["title"], "First line");
        assert_eq!(result["embedded"], true);
        let id = result["id"].as_str().expect("stored node id");
        let node = engine
            .storage()
            .get_node_record(id)
            .unwrap()
            .expect("persisted node");
        assert_eq!(node.labels, vec!["Note", "Knowledge"]);
        assert_eq!(node.properties["title"], "First line");
        assert_eq!(
            node.properties["content"],
            "First line\nSecond line should not become title"
        );
        assert_eq!(node.properties["owner"], "alice");
        assert_eq!(
            node.properties["tags"],
            serde_json::json!(["alpha", "beta"])
        );
        assert!(node.properties["created_at"]
            .as_str()
            .is_some_and(|created_at| created_at.contains('T') && created_at.ends_with('Z')));
        for reserved in ["embedding", "embeddings", "vector"] {
            assert!(!node.properties.contains_key(reserved));
        }
        let events = engine.audit_log().events().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type.as_str(), "DATA_CREATE");
        assert_eq!(events[0].action.as_deref(), Some("CREATE"));
        assert!(engine.audit_log().verify_chain().unwrap().valid);

        let before = engine.storage().all_node_records().unwrap().len();
        for (id, invalid_label) in [(23, "123bad"), (24, "Mémoire")] {
            let invalid = registry
                .dispatch(&McpRequest::new(
                    id,
                    "tools/call",
                    Some(serde_json::json!({
                        "name": "store",
                        "arguments": {"content": "bad", "type": invalid_label}
                    })),
                ))
                .await;
            assert_eq!(invalid.error.unwrap().code, -32000);
        }
        assert_eq!(engine.storage().all_node_records().unwrap().len(), before);

        let long_title = "é".repeat(101);
        let default_label = registry
            .dispatch(&McpRequest::new(
                25,
                "tools/call",
                Some(serde_json::json!({
                    "name": "store",
                    "arguments": {"content": long_title}
                })),
            ))
            .await;
        assert!(default_label.error.is_none(), "{:?}", default_label.error);
        let default_result = default_label.result.unwrap();
        let generated_title = default_result["title"].as_str().unwrap();
        assert_eq!(generated_title.chars().count(), 100);
        assert!(generated_title.ends_with("..."));
        let default_node = engine
            .storage()
            .get_node_record(default_result["id"].as_str().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(default_node.labels, vec!["Memory"]);
    }

    #[tokio::test]
    async fn recall_retrieves_by_id_and_applies_upstream_filters() {
        let engine = Arc::new(copperdb_engine::CopperDb::open_temporary().unwrap());
        let registry = ToolRegistry::with_engine_and_roles(engine.clone(), vec!["admin".into()]);
        let access_request = McpRequest::new(
            25,
            "tools/call",
            Some(serde_json::json!({"name": "recall", "arguments": {}})),
        );
        assert_eq!(
            registry.required_access(&access_request).unwrap(),
            Some(ToolAccess::Read)
        );
        let create = |label: &str,
                      title: &str,
                      created_at: &str,
                      tags: serde_json::Value,
                      large: serde_json::Value| {
            engine
                .execute(
                    &format!("CREATE (n:{label}) SET n += $props RETURN elementId(n) AS id"),
                    HashMap::from([(
                        "props".into(),
                        serde_json::json!({
                            "title": title,
                            "content": format!("{title} content"),
                            "created_at": created_at,
                            "tags": tags,
                            "embedding": [1, 2, 3],
                            "embedding_model": "test",
                            "large": large
                        }),
                    )]),
                )
                .unwrap()
                .rows[0]["id"]
                .as_str()
                .unwrap()
                .to_string()
        };
        let memory_id = create(
            "Memory",
            "Recent Memory",
            "2026-03-01T10:00:00Z",
            serde_json::json!(["alpha", "beta"]),
            serde_json::json!(vec![0; 101]),
        );
        create(
            "Task",
            "Old Task",
            "2025-01-01T10:00:00Z",
            serde_json::json!(["beta"]),
            serde_json::json!([1, 2]),
        );

        let by_id = registry
            .dispatch(&McpRequest::new(
                26,
                "tools/call",
                Some(serde_json::json!({
                    "name": "recall",
                    "arguments": {"id": memory_id, "type": ["Task"], "limit": 1}
                })),
            ))
            .await;
        assert!(by_id.error.is_none(), "{:?}", by_id.error);
        let by_id = by_id.result.unwrap();
        assert_eq!(by_id["count"], 1);
        assert_eq!(by_id["nodes"][0]["id"], memory_id);
        assert_eq!(by_id["nodes"][0]["type"], "Memory");
        assert_eq!(by_id["nodes"][0]["title"], "Recent Memory");
        assert_eq!(by_id["nodes"][0]["content"], "Recent Memory content");
        assert!(by_id["nodes"][0]["properties"].get("embedding").is_none());
        assert!(by_id["nodes"][0]["properties"]
            .get("embedding_model")
            .is_none());
        assert!(by_id["nodes"][0]["properties"].get("large").is_none());

        let filtered = registry
            .dispatch(&McpRequest::new(
                27,
                "tools/call",
                Some(serde_json::json!({
                    "name": "recall",
                    "arguments": {
                        "type": ["Memory", "Task"],
                        "tags": ["alpha", "beta"],
                        "since": "2026-01-01T00:00:00Z",
                        "limit": 5
                    }
                })),
            ))
            .await;
        assert!(filtered.error.is_none(), "{:?}", filtered.error);
        let filtered = filtered.result.unwrap();
        assert_eq!(filtered["count"], 1);
        assert_eq!(filtered["nodes"][0]["id"], memory_id);

        let missing = registry
            .dispatch(&McpRequest::new(
                28,
                "tools/call",
                Some(serde_json::json!({
                    "name": "recall",
                    "arguments": {"id": "missing"}
                })),
            ))
            .await;
        assert_eq!(missing.error.unwrap().message, "node not found: missing");
        let events = engine.audit_log().events().unwrap();
        assert_eq!(events.last().unwrap().event_type.as_str(), "DATA_READ");
        assert_eq!(events.last().unwrap().action.as_deref(), Some("MATCH"));
        assert!(engine.audit_log().verify_chain().unwrap().valid);
    }

    #[tokio::test]
    async fn registered_async_handler_shares_descriptor_access_and_execution_entry() {
        let mut registry = ToolRegistry::new();
        registry
            .register_with_handler(
                Tool {
                    name: "echo".into(),
                    description: "Echo typed arguments".into(),
                    input_schema: serde_json::json!({
                        "type": "object",
                        "properties": {"value": {"type": "string"}},
                        "required": ["value"],
                        "additionalProperties": false
                    }),
                },
                ToolAccess::Write,
                Arc::new(EchoHandler),
            )
            .unwrap();
        let request = McpRequest::new(
            20,
            "tools/call",
            Some(serde_json::json!({
                "name": "echo",
                "arguments": {"value": "async"}
            })),
        );

        assert_eq!(
            registry.required_access(&request).unwrap(),
            Some(ToolAccess::Write)
        );
        assert_eq!(
            registry.get("echo").unwrap().description,
            "Echo typed arguments"
        );
        let response = registry.dispatch(&request).await;
        assert!(response.error.is_none(), "{:?}", response.error);
        assert_eq!(
            response.result.unwrap(),
            serde_json::json!({
                "arguments": {"value": "async"},
                "cancelled": false,
                "hasEngine": false,
                "roles": []
            })
        );

        let mut invalid_registry = ToolRegistry::default();
        invalid_registry
            .register_with_handler(
                Tool {
                    name: "strict".into(),
                    description: "Reject invalid arguments".into(),
                    input_schema: serde_json::json!({
                        "type": "object",
                        "required": ["requiredValue"]
                    }),
                },
                ToolAccess::Read,
                Arc::new(MustNotRunHandler),
            )
            .unwrap();
        let invalid_response = invalid_registry
            .dispatch(&McpRequest::new(
                21,
                "tools/call",
                Some(serde_json::json!({"name": "strict", "arguments": {}})),
            ))
            .await;
        let error = invalid_response.error.expect("schema validation error");
        assert_eq!(error.code, -32602);
        assert_eq!(error.data.unwrap()["kind"], "schema_validation");
    }

    #[test]
    fn tool_registry_rejects_invalid_schema_without_registering_tool() {
        let mut registry = ToolRegistry::new();
        let error = registry
            .register_with_handler(
                Tool {
                    name: "invalid_tool".into(),
                    description: "Invalid schema".into(),
                    input_schema: serde_json::json!({"type": "not-a-json-schema-type"}),
                },
                ToolAccess::Read,
                Arc::new(EchoHandler),
            )
            .unwrap_err();

        assert!(matches!(
            error,
            McpError::InvalidSchema { ref tool, .. } if tool == "invalid_tool"
        ));
        assert!(registry.get("invalid_tool").is_none());
    }

    #[tokio::test]
    async fn test_dispatch_initialize() {
        let registry = ToolRegistry::new();
        let req = McpRequest::new(
            serde_json::json!(1),
            "initialize",
            Some(serde_json::json!({
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {"tools": {}},
                "clientInfo": {"name": "test-client", "version": "1.0"}
            })),
        );
        let resp = registry.dispatch(&req).await;
        assert!(resp.error.is_none());
        let result = resp.result.unwrap();
        assert_eq!(result["protocolVersion"], MCP_PROTOCOL_VERSION);
        assert_eq!(result["capabilities"]["tools"]["listChanged"], false);
    }

    #[tokio::test]
    async fn notification_request_omits_id_and_accepts_initialized_method() {
        let request: McpRequest = serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {}
        }))
        .unwrap();

        assert!(request.is_notification());
        assert!(serde_json::to_value(&request).unwrap().get("id").is_none());
        let response = ToolRegistry::new().dispatch(&request).await;
        assert!(response.error.is_none());
        assert_eq!(response.id, serde_json::Value::Null);
        assert_eq!(response.result, Some(serde_json::json!({})));
    }

    #[tokio::test]
    async fn explicit_null_id_is_a_request_not_a_notification() {
        let request: McpRequest = serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": null,
            "method": "tools/list"
        }))
        .unwrap();

        assert!(!request.is_notification());
        assert_eq!(request.id, Some(None));
        assert_eq!(
            serde_json::to_value(&request).unwrap()["id"],
            serde_json::Value::Null
        );
        let response = ToolRegistry::new().dispatch(&request).await;
        assert_eq!(response.id, serde_json::Value::Null);
        assert!(response.result.is_some());
    }

    #[test]
    fn tool_call_database_selection_matches_upstream_precedence_and_cleanup() {
        let mut request = McpRequest::new(
            1,
            "tools/call",
            Some(serde_json::json!({
                "name": "run_cypher",
                "arguments": {
                    "database": " tenant_a ",
                    "db": "tenant_b",
                    "query": "RETURN 1"
                }
            })),
        );

        assert_eq!(request.take_database().as_deref(), Some("tenant_a"));
        assert_eq!(
            request.params.as_ref().unwrap()["arguments"],
            serde_json::json!({"query": "RETURN 1"})
        );

        let mut alias = McpRequest::new(
            2,
            "tools/call",
            Some(serde_json::json!({
                "name": "run_cypher",
                "arguments": {"database": 42, "db": " tenant_b "}
            })),
        );
        assert_eq!(alias.take_database().as_deref(), Some("tenant_b"));

        let mut protocol_request = McpRequest::new(
            3,
            "initialize",
            Some(serde_json::json!({"database": "tenant_a"})),
        );
        assert_eq!(protocol_request.take_database(), None);
        assert_eq!(
            protocol_request.params.unwrap(),
            serde_json::json!({"database": "tenant_a"})
        );
    }

    #[tokio::test]
    async fn initialize_rejects_unsupported_protocol_version() {
        let registry = ToolRegistry::new();
        let request = McpRequest::new(
            serde_json::json!(11),
            "initialize",
            Some(serde_json::json!({"protocolVersion": "2099-01-01"})),
        );

        let error = registry.dispatch(&request).await.error.unwrap();

        assert_eq!(error.code, -32602);
        assert_eq!(error.message, "Unsupported protocol version");
        assert_eq!(
            error.data.unwrap()["supported"],
            serde_json::json!([MCP_PROTOCOL_VERSION])
        );
    }

    #[tokio::test]
    async fn dispatch_rejects_invalid_json_rpc_version() {
        let registry = ToolRegistry::new();
        let mut request = McpRequest::new(serde_json::json!(12), "tools/list", None);
        request.jsonrpc = "1.0".into();

        let error = registry.dispatch(&request).await.error.unwrap();

        assert_eq!(error.code, -32600);
        assert_eq!(error.message, "Invalid Request");
        assert_eq!(error.data.unwrap()["expected"], "2.0");
    }

    #[tokio::test]
    async fn test_dispatch_tools_list() {
        let registry = ToolRegistry::new();
        let req = McpRequest::new(serde_json::json!(2), "tools/list", None);
        let resp = registry.dispatch(&req).await;
        assert!(resp.error.is_none());
        let tools = resp.result.unwrap()["tools"].as_array().unwrap().len();
        assert_eq!(tools, 4);
    }

    #[test]
    fn tool_access_is_classified_before_execution() {
        let registry = ToolRegistry::new();
        let run_cypher = McpRequest::new(
            serde_json::json!(14),
            "tools/call",
            Some(serde_json::json!({
                "name": "run_cypher",
                "arguments": {"query": "RETURN 1"}
            })),
        );
        let find_similar = McpRequest::new(
            serde_json::json!(15),
            "tools/call",
            Some(serde_json::json!({
                "name": "find_similar",
                "arguments": {"text": "graph"}
            })),
        );

        assert_eq!(
            registry.required_access(&run_cypher).unwrap(),
            Some(ToolAccess::Admin)
        );
        assert_eq!(
            registry.required_access(&find_similar).unwrap(),
            Some(ToolAccess::Read)
        );
        assert_eq!(
            registry
                .required_access(&McpRequest::new(serde_json::json!(16), "tools/list", None))
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn test_dispatch_tool_call() {
        let engine = Arc::new(copperdb_engine::CopperDb::open_temporary().unwrap());
        let registry = ToolRegistry::with_engine(engine);
        let req = McpRequest::new(
            serde_json::json!(3),
            "tools/call",
            Some(
                serde_json::json!({ "name": "run_cypher", "arguments": { "query": "RETURN 1 AS n" } }),
            ),
        );
        let resp = registry.dispatch(&req).await;
        assert!(
            resp.error.is_none(),
            "expected success, got: {:?}",
            resp.error
        );
    }

    #[tokio::test]
    async fn run_cypher_forwards_declared_parameters() {
        let engine = Arc::new(copperdb_engine::CopperDb::open_temporary().unwrap());
        let registry = ToolRegistry::with_engine_and_roles(engine, vec!["reader".into()]);
        let request = McpRequest::new(
            serde_json::json!(17),
            "tools/call",
            Some(serde_json::json!({
                "name": "run_cypher",
                "arguments": {
                    "query": "RETURN $value AS value",
                    "params": {"value": "forwarded"}
                }
            })),
        );

        let response = registry.dispatch(&request).await;

        assert!(response.error.is_none(), "{:?}", response.error);
        let result = response.result.unwrap();
        assert!(result.get("isError").is_none(), "{result}");
        assert!(result["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("Rows: 1"));
    }

    #[tokio::test]
    async fn tool_call_rejects_arguments_that_do_not_match_advertised_schema() {
        let registry = ToolRegistry::new();
        let request = McpRequest::new(
            serde_json::json!(13),
            "tools/call",
            Some(serde_json::json!({
                "name": "find_similar",
                "arguments": {"text": "graph", "k": 101, "unexpected": true}
            })),
        );

        let error = registry.dispatch(&request).await.error.unwrap();

        assert_eq!(error.code, -32602);
        assert_eq!(error.message, "Invalid params");
        let data = error.data.unwrap();
        assert_eq!(data["tool"], "find_similar");
        assert_eq!(data["kind"], "schema_validation");
    }

    #[test]
    fn tool_output_within_limit_is_unchanged() {
        let registry = ToolRegistry::new().with_max_tool_output_bytes(512);
        let result = serde_json::json!({
            "content": [{"type": "text", "text": "small result"}]
        });

        assert_eq!(
            registry.enforce_tool_output_limit("find_similar", result.clone()),
            result
        );
    }

    #[test]
    fn oversized_tool_output_becomes_bounded_structured_error() {
        let registry = ToolRegistry::new().with_max_tool_output_bytes(512);
        let oversized = serde_json::json!({
            "content": [{"type": "text", "text": "é".repeat(400)}]
        });
        let actual_bytes = serde_json::to_vec(&oversized).unwrap().len();
        assert!(actual_bytes > 512);

        let result = registry.enforce_tool_output_limit("find_similar", oversized);

        assert_eq!(result["isError"], true);
        assert_eq!(result["_meta"]["kind"], "output_too_large");
        assert_eq!(result["_meta"]["tool"], "find_similar");
        assert_eq!(result["_meta"]["actualBytes"], actual_bytes);
        assert_eq!(result["_meta"]["maxBytes"], 512);
        assert_eq!(
            result["content"][0]["text"],
            format!("tool find_similar output is {actual_bytes} bytes; limit is 512 bytes")
        );
        assert!(serde_json::to_vec(&result).unwrap().len() <= 512);
    }

    #[tokio::test]
    async fn dispatch_with_context_cancels_cypher_before_execution() {
        let engine = Arc::new(copperdb_engine::CopperDb::open_temporary().unwrap());
        let registry = ToolRegistry::with_engine(engine);
        let request_context = RequestContext::detached();
        request_context.cancel();
        let request = McpRequest::new(
            serde_json::json!(6),
            "tools/call",
            Some(
                serde_json::json!({ "name": "run_cypher", "arguments": { "query": "RETURN 1 AS n" } }),
            ),
        );

        let response = registry
            .dispatch_with_context(&request_context, &request)
            .await;

        assert!(response.error.is_none());
        let result = response.result.expect("tool result");
        assert_eq!(result["isError"], true);
        assert_eq!(result["content"][0]["text"], "Error: request cancelled");
    }

    #[tokio::test]
    async fn dispatch_with_context_cancels_fulltext_before_search() {
        let engine = Arc::new(copperdb_engine::CopperDb::open_temporary().unwrap());
        let registry = ToolRegistry::with_engine(engine);
        let request_context = RequestContext::detached();
        request_context.cancel();
        let request = McpRequest::new(
            serde_json::json!(7),
            "tools/call",
            Some(
                serde_json::json!({ "name": "find_similar", "arguments": { "text": "graph", "k": 3 } }),
            ),
        );

        let response = registry
            .dispatch_with_context(&request_context, &request)
            .await;

        assert!(response.error.is_none());
        let result = response.result.expect("tool result");
        assert_eq!(result["isError"], true);
        assert_eq!(
            result["content"][0]["text"],
            "Search error: request cancelled"
        );
    }

    #[tokio::test]
    async fn test_dispatch_unknown_tool() {
        let registry = ToolRegistry::new();
        let req = McpRequest::new(
            serde_json::json!(4),
            "tools/call",
            Some(serde_json::json!({ "name": "nonexistent" })),
        );
        let resp = registry.dispatch(&req).await;
        assert!(resp.error.is_some());
    }

    #[tokio::test]
    async fn test_dispatch_unknown_method() {
        let registry = ToolRegistry::new();
        let req = McpRequest::new(serde_json::json!(5), "unknown/method", None);
        let resp = registry.dispatch(&req).await;
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, -32601);
    }

    #[test]
    fn test_mcp_response_serialization() {
        let resp = McpResponse::ok(serde_json::json!(1), serde_json::json!({"status": "ok"}));
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: McpResponse = serde_json::from_str(&json).unwrap();
        assert!(decoded.result.is_some());
        assert!(decoded.error.is_none());
    }
}
