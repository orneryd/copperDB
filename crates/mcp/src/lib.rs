//! Model Context Protocol (MCP) server for copperdb.
//!
//! Equivalent to Go's `pkg/mcp` in NornicDB.
//! Exposes copperdb as an MCP tool provider, allowing LLMs (Claude, GPT-4, etc.)
//! to query the graph database directly via tool calling.
//!
//! ## MCP Overview
//! MCP is an open protocol for connecting LLMs to data sources.
//! https://modelcontextprotocol.io/

use copperdb_engine::CopperDb as GraphEngine;
use copperdb_util::RequestContext;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

pub const MCP_PROTOCOL_VERSION: &str = "2024-11-05";
pub const DEFAULT_MAX_TOOL_OUTPUT_BYTES: usize = 1024 * 1024;

#[derive(Debug, Error)]
pub enum McpError {
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpRequest {
    pub jsonrpc: String,
    pub id: serde_json::Value,
    pub method: String,
    pub params: Option<serde_json::Value>,
}

impl McpRequest {
    pub fn new(
        id: impl Into<serde_json::Value>,
        method: impl Into<String>,
        params: Option<serde_json::Value>,
    ) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id: id.into(),
            method: method.into(),
            params,
        }
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
    tools: HashMap<String, Tool>,
    validators: HashMap<String, jsonschema::Validator>,
    access: HashMap<String, ToolAccess>,
    engine: Option<Arc<GraphEngine>>,
    roles: Vec<String>,
    max_tool_output_bytes: usize,
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self {
            tools: HashMap::new(),
            validators: HashMap::new(),
            access: HashMap::new(),
            engine: None,
            roles: Vec::new(),
            max_tool_output_bytes: DEFAULT_MAX_TOOL_OUTPUT_BYTES,
        }
    }
}

impl ToolRegistry {
    pub fn new() -> Self {
        let mut registry = Self::default();
        for (tool, access) in copperdb_tools_with_access() {
            registry
                .register_with_access(tool, access)
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

    pub fn register(&mut self, tool: Tool) -> Result<(), McpError> {
        self.register_with_access(tool, ToolAccess::Read)
    }

    pub fn register_with_access(&mut self, tool: Tool, access: ToolAccess) -> Result<(), McpError> {
        let validator = jsonschema::validator_for(&tool.input_schema).map_err(|error| {
            McpError::InvalidSchema {
                tool: tool.name.clone(),
                message: error.to_string(),
            }
        })?;
        self.validators.insert(tool.name.clone(), validator);
        self.access.insert(tool.name.clone(), access);
        self.tools.insert(tool.name.clone(), tool);
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
        self.access
            .get(&call.name)
            .copied()
            .map(Some)
            .ok_or(McpError::ToolNotFound(call.name))
    }

    pub fn get(&self, name: &str) -> Option<&Tool> {
        self.tools.get(name)
    }

    pub fn list(&self) -> Vec<&Tool> {
        let mut tools: Vec<&Tool> = self.tools.values().collect();
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
    pub fn dispatch(&self, request: &McpRequest) -> McpResponse {
        let request_context = RequestContext::detached();
        self.dispatch_with_context(&request_context, request)
    }

    pub fn dispatch_with_context(
        &self,
        request_context: &RequestContext,
        request: &McpRequest,
    ) -> McpResponse {
        if request.jsonrpc != "2.0" {
            return McpResponse::error_with_data(
                request.id.clone(),
                -32600,
                "Invalid Request",
                Some(serde_json::json!({"expected": "2.0"})),
            );
        }
        match request.method.as_str() {
            "tools/list" => {
                let tools: Vec<&Tool> = self.list();
                McpResponse::ok(request.id.clone(), serde_json::json!({ "tools": tools }))
            }
            "tools/call" => {
                let params = match &request.params {
                    Some(p) => p,
                    None => {
                        return McpResponse::error(request.id.clone(), -32602, "missing params")
                    }
                };
                let call: ToolCallParams = match serde_json::from_value(params.clone()) {
                    Ok(c) => c,
                    Err(e) => return McpResponse::error(request.id.clone(), -32602, e.to_string()),
                };
                if self.get(&call.name).is_none() {
                    return McpResponse::error(
                        request.id.clone(),
                        -32601,
                        format!("tool not found: {}", call.name),
                    );
                }
                let arguments = call
                    .arguments
                    .clone()
                    .unwrap_or_else(|| serde_json::json!({}));
                let validator = self
                    .validators
                    .get(&call.name)
                    .expect("registered MCP tool must have a compiled validator");
                if let Err(error) = validator.validate(&arguments) {
                    return McpResponse::error_with_data(
                        request.id.clone(),
                        -32602,
                        "Invalid params",
                        Some(serde_json::json!({
                            "tool": call.name,
                            "kind": "schema_validation",
                            "detail": error.to_string()
                        })),
                    );
                }
                match self.execute_tool(request_context, &call.name, &call.arguments) {
                    Ok(result) => McpResponse::ok(
                        request.id.clone(),
                        self.enforce_tool_output_limit(&call.name, result),
                    ),
                    Err(e) => McpResponse::error(request.id.clone(), -32000, e),
                }
            }
            "initialize" => {
                let params = match request.params.clone() {
                    Some(params) => match serde_json::from_value::<InitializeParams>(params) {
                        Ok(params) => params,
                        Err(error) => {
                            return McpResponse::error_with_data(
                                request.id.clone(),
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
                        request.id.clone(),
                        -32602,
                        "Unsupported protocol version",
                        Some(serde_json::json!({"supported": [MCP_PROTOCOL_VERSION]})),
                    );
                }
                McpResponse::ok(
                    request.id.clone(),
                    serde_json::json!({
                        "protocolVersion": MCP_PROTOCOL_VERSION,
                        "capabilities": { "tools": {"listChanged": false} },
                        "serverInfo": { "name": "copperdb", "version": env!("CARGO_PKG_VERSION") }
                    }),
                )
            }
            _ => McpResponse::error(request.id.clone(), -32601, "method not found"),
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

    /// Execute a named tool with the given arguments.
    fn execute_tool(
        &self,
        request_context: &RequestContext,
        name: &str,
        arguments: &Option<serde_json::Value>,
    ) -> Result<serde_json::Value, String> {
        match name {
            "run_cypher" => {
                let query = arguments
                    .as_ref()
                    .and_then(|args| args.get("query"))
                    .and_then(|v| v.as_str())
                    .ok_or("missing 'query' parameter")?;
                let engine = self.engine.as_ref().ok_or("no graph engine configured")?;
                let params = arguments
                    .as_ref()
                    .and_then(|args| args.get("params"))
                    .and_then(serde_json::Value::as_object)
                    .map(|params| {
                        params
                            .iter()
                            .map(|(key, value)| (key.clone(), value.clone()))
                            .collect()
                    })
                    .unwrap_or_default();
                match engine.execute_as_with_context(request_context, query, params, &self.roles) {
                    Ok(result) => {
                        let text = format!(
                            "Query executed successfully.\nRows: {}\nStats: {:?}",
                            result.rows.len(),
                            result.stats
                        );
                        Ok(serde_json::json!({
                            "content": [{ "type": "text", "text": text }]
                        }))
                    }
                    Err(e) => Ok(serde_json::json!({
                        "content": [{ "type": "text", "text": format!("Error: {e}") }],
                        "isError": true
                    })),
                }
            }
            "find_similar" => {
                let text = arguments
                    .as_ref()
                    .and_then(|args| args.get("text"))
                    .and_then(|v| v.as_str())
                    .ok_or("missing 'text' parameter")?;
                let k = arguments
                    .as_ref()
                    .and_then(|args| args.get("k"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(10) as usize;
                let engine = self.engine.as_ref().ok_or("no graph engine configured")?;
                match engine.search_fulltext_nodes_with_context(request_context, "", &[], text, k) {
                    Ok(results) => {
                        let json = serde_json::to_string_pretty(&results)
                            .unwrap_or_else(|_| "[]".to_string());
                        Ok(serde_json::json!({
                            "content": [{ "type": "text", "text": json }]
                        }))
                    }
                    Err(e) => Ok(serde_json::json!({
                        "content": [{ "type": "text", "text": format!("Search error: {e}") }],
                        "isError": true
                    })),
                }
            }
            _ => Err(format!("tool not found: {name}")),
        }
    }
}

/// Built-in copperdb MCP tools.
pub fn copperdb_tools() -> Vec<Tool> {
    copperdb_tools_with_access()
        .into_iter()
        .map(|(tool, _)| tool)
        .collect()
}

fn copperdb_tools_with_access() -> Vec<(Tool, ToolAccess)> {
    vec![
        (
            Tool {
                name: "run_cypher".into(),
                description: "Execute a Cypher query against the copperdb graph database".into(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "query": { "type": "string", "description": "The Cypher query to execute" },
                        "params": { "type": "object", "description": "Query parameters" }
                    },
                    "required": ["query"]
                    ,"additionalProperties": false
                }),
            },
            ToolAccess::Admin,
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
                        "label": { "type": "string", "description": "Filter by node label" }
                    },
                    "required": ["text"],
                    "additionalProperties": false
                }),
            },
            ToolAccess::Read,
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tool_registry_default_tools() {
        let registry = ToolRegistry::new();
        assert_eq!(registry.len(), 2);
        assert!(registry.get("run_cypher").is_some());
        assert!(registry.get("find_similar").is_some());
    }

    #[test]
    fn test_tool_registry_register() {
        let mut registry = ToolRegistry::new();
        registry
            .register(Tool {
                name: "custom_tool".into(),
                description: "A custom tool".into(),
                input_schema: serde_json::json!({}),
            })
            .unwrap();
        assert_eq!(registry.len(), 3);
    }

    #[test]
    fn tool_registry_rejects_invalid_schema_without_registering_tool() {
        let mut registry = ToolRegistry::new();
        let error = registry
            .register(Tool {
                name: "invalid_tool".into(),
                description: "Invalid schema".into(),
                input_schema: serde_json::json!({"type": "not-a-json-schema-type"}),
            })
            .unwrap_err();

        assert!(matches!(
            error,
            McpError::InvalidSchema { ref tool, .. } if tool == "invalid_tool"
        ));
        assert!(registry.get("invalid_tool").is_none());
    }

    #[test]
    fn test_dispatch_initialize() {
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
        let resp = registry.dispatch(&req);
        assert!(resp.error.is_none());
        let result = resp.result.unwrap();
        assert_eq!(result["protocolVersion"], MCP_PROTOCOL_VERSION);
        assert_eq!(result["capabilities"]["tools"]["listChanged"], false);
    }

    #[test]
    fn initialize_rejects_unsupported_protocol_version() {
        let registry = ToolRegistry::new();
        let request = McpRequest::new(
            serde_json::json!(11),
            "initialize",
            Some(serde_json::json!({"protocolVersion": "2099-01-01"})),
        );

        let error = registry.dispatch(&request).error.unwrap();

        assert_eq!(error.code, -32602);
        assert_eq!(error.message, "Unsupported protocol version");
        assert_eq!(
            error.data.unwrap()["supported"],
            serde_json::json!([MCP_PROTOCOL_VERSION])
        );
    }

    #[test]
    fn dispatch_rejects_invalid_json_rpc_version() {
        let registry = ToolRegistry::new();
        let mut request = McpRequest::new(serde_json::json!(12), "tools/list", None);
        request.jsonrpc = "1.0".into();

        let error = registry.dispatch(&request).error.unwrap();

        assert_eq!(error.code, -32600);
        assert_eq!(error.message, "Invalid Request");
        assert_eq!(error.data.unwrap()["expected"], "2.0");
    }

    #[test]
    fn test_dispatch_tools_list() {
        let registry = ToolRegistry::new();
        let req = McpRequest::new(serde_json::json!(2), "tools/list", None);
        let resp = registry.dispatch(&req);
        assert!(resp.error.is_none());
        let tools = resp.result.unwrap()["tools"].as_array().unwrap().len();
        assert_eq!(tools, 2);
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

    #[test]
    fn test_dispatch_tool_call() {
        let engine = Arc::new(copperdb_engine::CopperDb::open_temporary().unwrap());
        let registry = ToolRegistry::with_engine(engine);
        let req = McpRequest::new(
            serde_json::json!(3),
            "tools/call",
            Some(
                serde_json::json!({ "name": "run_cypher", "arguments": { "query": "RETURN 1 AS n" } }),
            ),
        );
        let resp = registry.dispatch(&req);
        assert!(
            resp.error.is_none(),
            "expected success, got: {:?}",
            resp.error
        );
    }

    #[test]
    fn run_cypher_forwards_declared_parameters() {
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

        let response = registry.dispatch(&request);

        assert!(response.error.is_none(), "{:?}", response.error);
        let result = response.result.unwrap();
        assert!(result.get("isError").is_none(), "{result}");
        assert!(result["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("Rows: 1"));
    }

    #[test]
    fn tool_call_rejects_arguments_that_do_not_match_advertised_schema() {
        let registry = ToolRegistry::new();
        let request = McpRequest::new(
            serde_json::json!(13),
            "tools/call",
            Some(serde_json::json!({
                "name": "find_similar",
                "arguments": {"text": "graph", "k": 101, "unexpected": true}
            })),
        );

        let error = registry.dispatch(&request).error.unwrap();

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

    #[test]
    fn dispatch_with_context_cancels_cypher_before_execution() {
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

        let response = registry.dispatch_with_context(&request_context, &request);

        assert!(response.error.is_none());
        let result = response.result.expect("tool result");
        assert_eq!(result["isError"], true);
        assert_eq!(result["content"][0]["text"], "Error: request cancelled");
    }

    #[test]
    fn dispatch_with_context_cancels_fulltext_before_search() {
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

        let response = registry.dispatch_with_context(&request_context, &request);

        assert!(response.error.is_none());
        let result = response.result.expect("tool result");
        assert_eq!(result["isError"], true);
        assert_eq!(
            result["content"][0]["text"],
            "Search error: request cancelled"
        );
    }

    #[test]
    fn test_dispatch_unknown_tool() {
        let registry = ToolRegistry::new();
        let req = McpRequest::new(
            serde_json::json!(4),
            "tools/call",
            Some(serde_json::json!({ "name": "nonexistent" })),
        );
        let resp = registry.dispatch(&req);
        assert!(resp.error.is_some());
    }

    #[test]
    fn test_dispatch_unknown_method() {
        let registry = ToolRegistry::new();
        let req = McpRequest::new(serde_json::json!(5), "unknown/method", None);
        let resp = registry.dispatch(&req);
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
