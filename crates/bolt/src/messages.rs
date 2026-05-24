//! Bolt protocol message types.
//! Reference: https://7687.org/bolt/bolt-protocol-message-specification-4.html

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BoltMessage {
    Hello {
        extra: std::collections::HashMap<String, serde_json::Value>,
    },
    Logon {
        auth: std::collections::HashMap<String, String>,
    },
    Logoff,
    Run {
        query: String,
        parameters: std::collections::HashMap<String, serde_json::Value>,
        extra: std::collections::HashMap<String, serde_json::Value>,
    },
    Pull {
        n: i64,
        qid: i64,
    },
    Discard {
        n: i64,
        qid: i64,
    },
    Begin {
        extra: std::collections::HashMap<String, serde_json::Value>,
    },
    Commit,
    Rollback,
    Reset,
    Route {
        routing: std::collections::HashMap<String, serde_json::Value>,
        bookmarks: Vec<String>,
        db: Option<String>,
    },
    // Server responses
    Success {
        metadata: std::collections::HashMap<String, serde_json::Value>,
    },
    Failure {
        metadata: std::collections::HashMap<String, serde_json::Value>,
    },
    Ignored,
    Record {
        data: Vec<serde_json::Value>,
    },
}
