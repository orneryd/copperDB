//! Model Context Protocol (MCP) server for copperdb.
//!
//! Equivalent to Go's `pkg/mcp` in NornicDB.
//! Exposes copperdb as an MCP tool provider, allowing LLMs (Claude, GPT-4, etc.)
//! to query the graph database directly via tool calling.
//!
//! ## MCP Overview
//! MCP is an open protocol for connecting LLMs to data sources.
//! https://modelcontextprotocol.io/

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum McpError {
    #[error("JSON-RPC error: {0}")]
    JsonRpc(String),
    #[error("tool not found: {0}")]
    ToolNotFound(String),
    #[error("invalid params: {0}")]
    InvalidParams(String),
    #[error("execution error: {0}")]
    ExecutionError(String),
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
    pub fn new(id: impl Into<serde_json::Value>, method: impl Into<String>, params: Option<serde_json::Value>) -> Self {
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
        Self { jsonrpc: "2.0".into(), id, result: Some(result), error: None }
    }

    pub fn error(id: serde_json::Value, code: i32, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: None,
            error: Some(McpResponseError { code, message: message.into() }),
        }
    }
}

/// JSON-RPC error object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpResponseError {
    pub code: i32,
    pub message: String,
}

/// Tool call parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallParams {
    pub name: String,
    pub arguments: Option<serde_json::Value>,
}

/// Registry of available MCP tools.
pub struct ToolRegistry {
    tools: HashMap<String, Tool>,
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ToolRegistry {
    pub fn new() -> Self {
        let mut registry = Self { tools: HashMap::new() };
        for tool in copperdb_tools() {
            registry.register(tool);
        }
        registry
    }

    pub fn register(&mut self, tool: Tool) {
        self.tools.insert(tool.name.clone(), tool);
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
        match request.method.as_str() {
            "tools/list" => {
                let tools: Vec<&Tool> = self.list();
                McpResponse::ok(
                    request.id.clone(),
                    serde_json::json!({ "tools": tools }),
                )
            }
            "tools/call" => {
                let params = match &request.params {
                    Some(p) => p,
                    None => return McpResponse::error(request.id.clone(), -32602, "missing params"),
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
                McpResponse::ok(
                    request.id.clone(),
                    serde_json::json!({ "content": [{ "type": "text", "text": "stub response" }] }),
                )
            }
            "initialize" => McpResponse::ok(
                request.id.clone(),
                serde_json::json!({
                    "protocolVersion": "2024-11-05",
                    "capabilities": { "tools": {} },
                    "serverInfo": { "name": "copperdb", "version": env!("CARGO_PKG_VERSION") }
                }),
            ),
            _ => McpResponse::error(request.id.clone(), -32601, "method not found"),
        }
    }
}

/// Built-in copperdb MCP tools.
pub fn copperdb_tools() -> Vec<Tool> {
    vec![
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
            }),
        },
        Tool {
            name: "find_similar".into(),
            description: "Find semantically similar nodes using vector search".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "text": { "type": "string", "description": "Text to find similar nodes for" },
                    "k": { "type": "integer", "description": "Number of results", "default": 10 },
                    "label": { "type": "string", "description": "Filter by node label" }
                },
                "required": ["text"]
            }),
        },
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
        registry.register(Tool {
            name: "custom_tool".into(),
            description: "A custom tool".into(),
            input_schema: serde_json::json!({}),
        });
        assert_eq!(registry.len(), 3);
    }

    #[test]
    fn test_dispatch_initialize() {
        let registry = ToolRegistry::new();
        let req = McpRequest::new(serde_json::json!(1), "initialize", None);
        let resp = registry.dispatch(&req);
        assert!(resp.error.is_none());
        assert!(resp.result.is_some());
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
    fn test_dispatch_tool_call() {
        let registry = ToolRegistry::new();
        let req = McpRequest::new(
            serde_json::json!(3),
            "tools/call",
            Some(serde_json::json!({ "name": "run_cypher", "arguments": { "query": "MATCH (n) RETURN n" } })),
        );
        let resp = registry.dispatch(&req);
        assert!(resp.error.is_none());
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
        let resp = McpResponse::ok(
            serde_json::json!(1),
            serde_json::json!({"status": "ok"}),
        );
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: McpResponse = serde_json::from_str(&json).unwrap();
        assert!(decoded.result.is_some());
        assert!(decoded.error.is_none());
    }
}
