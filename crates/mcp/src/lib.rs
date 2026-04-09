//! Model Context Protocol (MCP) server for magnetDB.
//!
//! Equivalent to Go's `pkg/mcp` in NornicDB.
//! Exposes magnetDB as an MCP tool provider, allowing LLMs (Claude, GPT-4, etc.)
//! to query the graph database directly via tool calling.
//!
//! ## MCP Overview
//! MCP is an open protocol for connecting LLMs to data sources.
//! https://modelcontextprotocol.io/
//!
//! ## Rust Implementation Notes
//! There is no official Rust MCP SDK yet (as of 2025). This crate implements
//! the MCP JSON-RPC 2.0 wire protocol manually using `axum` + WebSocket.
//! Track: https://github.com/modelcontextprotocol/specification

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum McpError {
    #[error("JSON-RPC error: {0}")]
    JsonRpc(String),
    #[error("tool not found: {0}")]
    ToolNotFound(String),
    #[error("invalid params: {0}")]
    InvalidParams(String),
}

/// MCP tool definition (exposed to LLMs).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tool {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

/// Built-in magnetDB MCP tools.
pub fn magnetdb_tools() -> Vec<Tool> {
    vec![
        Tool {
            name: "run_cypher".into(),
            description: "Execute a Cypher query against the magnetDB graph database".into(),
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

// TODO: Implement JSON-RPC 2.0 handler and WebSocket transport.
// TODO: Add tool execution wiring to magnetdb-eval + magnetdb-vectorspace.
