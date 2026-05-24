#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricSpec {
    pub name: &'static str,
    pub label_shapes: &'static [&'static [&'static str]],
    pub source: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumSpec {
    pub name: &'static str,
    pub values: &'static [&'static str],
    pub source: &'static str,
}

pub const NORNICDB_MAIN_REF: &str = "refs/heads/main";
pub const NORNICDB_OBSERVABILITY_PATH: &str = "pkg/observability";

pub const ENUM_CATALOG: &[EnumSpec] = &[
    EnumSpec { name: "AllowedAuthResults", values: &["success", "failure", "denied"], source: "catalog_auth.go" },
    EnumSpec { name: "AllowedAuthProtocols", values: &["bolt", "http", "grpc"], source: "catalog_auth.go" },
    EnumSpec { name: "AllowedBoltResults", values: &["success", "error", "timeout"], source: "catalog_bolt.go" },
    EnumSpec { name: "AllowedBoltOps", values: &["hello", "run", "pull", "begin", "commit", "discard", "reset", "goodbye", "route", "ack_failure"], source: "catalog_bolt.go" },
    EnumSpec { name: "AllowedPackstreamReasons", values: &["truncated", "invalid_marker", "wrong_type", "oversize"], source: "catalog_bolt.go" },
    EnumSpec { name: "AllowedBoltTransports", values: &["tcp", "tcp_tls", "ws", "ws_tls"], source: "catalog_bolt.go" },
    EnumSpec { name: "AllowedBoltConnectionRejectReasons", values: &["max_connections", "sniff_timeout", "auth_timeout", "tls_handshake", "ws_handshake", "oversized_message", "requires_tls", "unrecognized_prefix", "ws_disabled"], source: "catalog_bolt.go" },
    EnumSpec { name: "AllowedCacheNames", values: &["query_result", "schema", "label", "node_lookup"], source: "catalog_cache.go" },
    EnumSpec { name: "AllowedEvictionReasons", values: &["lru", "ttl", "capacity", "manual"], source: "catalog_cache.go" },
    EnumSpec { name: "AllowedCypherOpTypes", values: &["read", "write", "schema", "admin", "fabric", "parse_error"], source: "catalog_cypher.go" },
    EnumSpec { name: "AllowedEmbedBackends", values: &["gpu", "cpu", "cuda", "metal", "vulkan"], source: "catalog_embed.go" },
    EnumSpec { name: "AllowedEmbedResults", values: &["success", "failure", "cached"], source: "catalog_embed.go" },
    EnumSpec { name: "AllowedEmbedProviders", values: &["ollama", "openai", "local", "other"], source: "catalog_embed.go" },
    EnumSpec { name: "AllowedStatusClasses", values: &["1xx", "2xx", "3xx", "4xx", "5xx"], source: "catalog_http.go" },
    EnumSpec { name: "AllowedKnowledgePolicyEntityKinds", values: &["node", "edge", "property"], source: "catalog_knowledge_policy.go" },
    EnumSpec { name: "AllowedKnowledgePolicyScoreResults", values: &["visible", "suppressed", "no_decay"], source: "catalog_knowledge_policy.go" },
    EnumSpec { name: "AllowedKnowledgePolicySuppressReasons", values: &["below_threshold", "score_floor", "on_access", "explicit_flag", "rule_cap"], source: "catalog_knowledge_policy.go" },
    EnumSpec { name: "AllowedKnowledgePolicyOnAccessResults", values: &["applied", "skipped_no_policy", "error"], source: "catalog_knowledge_policy.go" },
    EnumSpec { name: "AllowedKnowledgePolicyReconcileTriggers", values: &["schema_change", "startup", "manual"], source: "catalog_knowledge_policy.go" },
    EnumSpec { name: "AllowedMVCCBands", values: &["normal", "warn", "high", "critical"], source: "catalog_mvcc.go" },
    EnumSpec { name: "AllowedReplicationModes", values: &["standalone", "ha_standby", "raft", "multi_region"], source: "catalog_replication.go" },
    EnumSpec { name: "AllowedReplicationRoles", values: &["follower", "candidate", "leader", "standby"], source: "catalog_replication.go" },
    EnumSpec { name: "AllowedSearchModes", values: &["vector", "bm25", "hybrid"], source: "catalog_search.go" },
    EnumSpec { name: "AllowedSearchResults", values: &["success", "no_results", "error"], source: "catalog_search.go" },
    EnumSpec { name: "AllowedSearchStages", values: &["embed", "index", "fuse"], source: "catalog_search.go" },
    EnumSpec { name: "AllowedSearchIndexKinds", values: &["hnsw", "bm25"], source: "catalog_search.go" },
    EnumSpec { name: "AllowedStorageBytesKinds", values: &["nodes", "edges", "index", "wal", "search"], source: "catalog_storage.go" },
    EnumSpec { name: "AllowedStorageOps", values: &["get", "put", "delete", "scan"], source: "catalog_storage.go" },
    EnumSpec { name: "AllowedStorageIndexes", values: &["label", "edge_between", "temporal", "embedding", "user_created"], source: "catalog_storage.go" },
    EnumSpec { name: "AllowedStorageResults", values: &["success", "failure", "aborted"], source: "catalog_storage.go" },
];

const NONE: &[&str] = &[];
const HTTP_BASE: &[&str] = &["method", "path_template", "status_class"];
const HTTP_TENANT: &[&str] = &["method", "path_template", "status_class", "database"];
const CYPHER_BASE: &[&str] = &["op_type"];
const CYPHER_TENANT: &[&str] = &["op_type", "database"];
const TENANT_ONLY: &[&str] = &["database"];
const KP_ENTITY: &[&str] = &["entity_kind"];
const KP_ENTITY_TENANT: &[&str] = &["entity_kind", "database"];
const KP_ENTITY_RESULT: &[&str] = &["entity_kind", "result"];
const KP_ENTITY_RESULT_TENANT: &[&str] = &["entity_kind", "result", "database"];
const KP_ENTITY_REASON: &[&str] = &["entity_kind", "reason"];
const KP_ENTITY_REASON_TENANT: &[&str] = &["entity_kind", "reason", "database"];
const KP_ON_ACCESS: &[&str] = &["result"];
const KP_ON_ACCESS_TENANT: &[&str] = &["result", "database"];
const KP_TRIGGER: &[&str] = &["trigger"];
const KP_TRIGGER_TENANT: &[&str] = &["trigger", "database"];
const MVCC_BASE: &[&str] = &["band"];
const MVCC_TENANT: &[&str] = &["database", "band"];
const SEARCH_REQ_BASE: &[&str] = &["mode", "result"];
const SEARCH_REQ_TENANT: &[&str] = &["database", "mode", "result"];
const SEARCH_DUR_BASE: &[&str] = &["mode", "stage"];
const SEARCH_DUR_TENANT: &[&str] = &["database", "mode", "stage"];
const STORAGE_INDEX_BASE: &[&str] = &["index", "result"];
const STORAGE_INDEX_TENANT: &[&str] = &["database", "index", "result"];

pub const METRIC_CATALOG: &[MetricSpec] = &[
    MetricSpec { name: "nornicdb_auth_attempts_total", label_shapes: &[&["result", "protocol"]], source: "catalog_auth.go" },
    MetricSpec { name: "nornicdb_bolt_connections_active", label_shapes: &[&["transport"]], source: "catalog_bolt.go" },
    MetricSpec { name: "nornicdb_bolt_connections_total", label_shapes: &[&["result", "transport"]], source: "catalog_bolt.go" },
    MetricSpec { name: "nornicdb_bolt_connections_rejected_total", label_shapes: &[&["reason"]], source: "catalog_bolt.go" },
    MetricSpec { name: "nornicdb_bolt_websocket_oversized_total", label_shapes: &[NONE], source: "catalog_bolt.go" },
    MetricSpec { name: "nornicdb_bolt_session_duration_seconds", label_shapes: &[NONE], source: "catalog_bolt.go" },
    MetricSpec { name: "nornicdb_bolt_messages_total", label_shapes: &[&["op", "result"]], source: "catalog_bolt.go" },
    MetricSpec { name: "nornicdb_bolt_message_duration_seconds", label_shapes: &[&["op"]], source: "catalog_bolt.go" },
    MetricSpec { name: "nornicdb_bolt_packstream_decode_errors_total", label_shapes: &[&["reason"]], source: "catalog_bolt.go" },
    MetricSpec { name: "nornicdb_cache_hits_total", label_shapes: &[&["cache"]], source: "catalog_cache.go" },
    MetricSpec { name: "nornicdb_cache_misses_total", label_shapes: &[&["cache"]], source: "catalog_cache.go" },
    MetricSpec { name: "nornicdb_cache_size_bytes", label_shapes: &[&["cache"]], source: "catalog_cache.go" },
    MetricSpec { name: "nornicdb_cache_evictions_total", label_shapes: &[&["cache", "reason"]], source: "catalog_cache.go" },
    MetricSpec { name: "nornicdb_process_uptime_seconds", label_shapes: &[NONE], source: "catalog_cache.go" },
    MetricSpec { name: "nornicdb_build_info", label_shapes: &[NONE], source: "catalog_cache.go" },
    MetricSpec { name: "nornicdb_cypher_queries_total", label_shapes: &[CYPHER_BASE, CYPHER_TENANT], source: "catalog_cypher.go" },
    MetricSpec { name: "nornicdb_cypher_query_duration_seconds", label_shapes: &[CYPHER_BASE, CYPHER_TENANT], source: "catalog_cypher.go" },
    MetricSpec { name: "nornicdb_cypher_planner_duration_seconds", label_shapes: &[&["op_type"]], source: "catalog_cypher.go" },
    MetricSpec { name: "nornicdb_cypher_planner_cache_hits_total", label_shapes: &[NONE], source: "catalog_cypher.go" },
    MetricSpec { name: "nornicdb_cypher_planner_cache_misses_total", label_shapes: &[NONE], source: "catalog_cypher.go" },
    MetricSpec { name: "nornicdb_cypher_planner_cache_size", label_shapes: &[NONE], source: "catalog_cypher.go" },
    MetricSpec { name: "nornicdb_cypher_rows_returned_rows", label_shapes: &[&["op_type"]], source: "catalog_cypher.go" },
    MetricSpec { name: "nornicdb_cypher_active_transactions", label_shapes: &[NONE], source: "catalog_cypher.go" },
    MetricSpec { name: "nornicdb_cypher_transaction_conflicts_total", label_shapes: &[NONE, TENANT_ONLY], source: "catalog_cypher.go" },
    MetricSpec { name: "nornicdb_cypher_slow_queries_total", label_shapes: &[NONE, TENANT_ONLY], source: "catalog_cypher.go" },
    MetricSpec { name: "nornicdb_cypher_slow_query_threshold_seconds", label_shapes: &[NONE], source: "catalog_cypher.go" },
    MetricSpec { name: "nornicdb_embed_queue_depth", label_shapes: &[NONE], source: "catalog_embed.go" },
    MetricSpec { name: "nornicdb_embed_processed_total", label_shapes: &[&["provider", "model", "result", "mode"]], source: "catalog_embed.go" },
    MetricSpec { name: "nornicdb_embed_duration_seconds", label_shapes: &[&["provider", "model", "mode"]], source: "catalog_embed.go" },
    MetricSpec { name: "nornicdb_embed_cache_hits_total", label_shapes: &[NONE], source: "catalog_embed.go" },
    MetricSpec { name: "nornicdb_embed_cache_misses_total", label_shapes: &[NONE], source: "catalog_embed.go" },
    MetricSpec { name: "nornicdb_embed_worker_running", label_shapes: &[NONE], source: "catalog_embed.go" },
    MetricSpec { name: "nornicdb_embed_ffi_panics_total", label_shapes: &[&["mode"]], source: "catalog_embed.go" },
    MetricSpec { name: "nornicdb_http_requests_total", label_shapes: &[HTTP_BASE, HTTP_TENANT], source: "catalog_http.go" },
    MetricSpec { name: "nornicdb_http_request_duration_seconds", label_shapes: &[HTTP_BASE, HTTP_TENANT], source: "catalog_http.go" },
    MetricSpec { name: "nornicdb_http_in_flight_requests", label_shapes: &[NONE], source: "catalog_http.go" },
    MetricSpec { name: "nornicdb_http_request_body_bytes", label_shapes: &[&["method", "path_template"]], source: "catalog_http.go" },
    MetricSpec { name: "nornicdb_http_response_body_bytes", label_shapes: &[&["method", "path_template"]], source: "catalog_http.go" },
    MetricSpec { name: "nornicdb_knowledge_policy_scored_total", label_shapes: &[KP_ENTITY_RESULT, KP_ENTITY_RESULT_TENANT], source: "catalog_knowledge_policy.go" },
    MetricSpec { name: "nornicdb_knowledge_policy_decay_score", label_shapes: &[KP_ENTITY, KP_ENTITY_TENANT], source: "catalog_knowledge_policy.go" },
    MetricSpec { name: "nornicdb_knowledge_policy_suppressions_total", label_shapes: &[KP_ENTITY_REASON, KP_ENTITY_REASON_TENANT], source: "catalog_knowledge_policy.go" },
    MetricSpec { name: "nornicdb_knowledge_policy_access_flush_batch_rows", label_shapes: &[NONE], source: "catalog_knowledge_policy.go" },
    MetricSpec { name: "nornicdb_knowledge_policy_access_flush_duration_seconds", label_shapes: &[NONE], source: "catalog_knowledge_policy.go" },
    MetricSpec { name: "nornicdb_knowledge_policy_access_flush_buffer_fullness", label_shapes: &[NONE], source: "catalog_knowledge_policy.go" },
    MetricSpec { name: "nornicdb_knowledge_policy_on_access_mutations_total", label_shapes: &[KP_ON_ACCESS, KP_ON_ACCESS_TENANT], source: "catalog_knowledge_policy.go" },
    MetricSpec { name: "nornicdb_knowledge_policy_deindex_enqueued_total", label_shapes: &[KP_ENTITY, KP_ENTITY_TENANT], source: "catalog_knowledge_policy.go" },
    MetricSpec { name: "nornicdb_knowledge_policy_read_filter_dropped_total", label_shapes: &[KP_ENTITY, KP_ENTITY_TENANT], source: "catalog_knowledge_policy.go" },
    MetricSpec { name: "nornicdb_knowledge_policy_reconcile_total", label_shapes: &[KP_TRIGGER, KP_TRIGGER_TENANT], source: "catalog_knowledge_policy.go" },
    MetricSpec { name: "nornicdb_mvcc_pressure_band", label_shapes: &[MVCC_BASE, MVCC_TENANT], source: "catalog_mvcc.go" },
    MetricSpec { name: "nornicdb_mvcc_pinned_bytes", label_shapes: &[NONE], source: "catalog_mvcc.go" },
    MetricSpec { name: "nornicdb_mvcc_oldest_reader_age_seconds", label_shapes: &[NONE], source: "catalog_mvcc.go" },
    MetricSpec { name: "nornicdb_mvcc_active_readers", label_shapes: &[NONE], source: "catalog_mvcc.go" },
    MetricSpec { name: "nornicdb_replication_role", label_shapes: &[NONE], source: "catalog_replication.go" },
    MetricSpec { name: "nornicdb_replication_term", label_shapes: &[NONE], source: "catalog_replication.go" },
    MetricSpec { name: "nornicdb_replication_commit_index", label_shapes: &[NONE], source: "catalog_replication.go" },
    MetricSpec { name: "nornicdb_replication_apply_index", label_shapes: &[NONE], source: "catalog_replication.go" },
    MetricSpec { name: "nornicdb_replication_lag_bytes", label_shapes: &[&["peer"]], source: "catalog_replication.go" },
    MetricSpec { name: "nornicdb_replication_lag_entries", label_shapes: &[&["peer"]], source: "catalog_replication.go" },
    MetricSpec { name: "nornicdb_replication_apply_duration_seconds", label_shapes: &[NONE], source: "catalog_replication.go" },
    MetricSpec { name: "nornicdb_replication_rtt_seconds", label_shapes: &[&["peer"]], source: "catalog_replication.go" },
    MetricSpec { name: "nornicdb_replication_leader_changes_total", label_shapes: &[NONE], source: "catalog_replication.go" },
    MetricSpec { name: "nornicdb_replication_last_contact_seconds", label_shapes: &[&["peer"]], source: "catalog_replication.go" },
    MetricSpec { name: "nornicdb_search_requests_total", label_shapes: &[SEARCH_REQ_BASE, SEARCH_REQ_TENANT], source: "catalog_search.go" },
    MetricSpec { name: "nornicdb_search_duration_seconds", label_shapes: &[SEARCH_DUR_BASE, SEARCH_DUR_TENANT], source: "catalog_search.go" },
    MetricSpec { name: "nornicdb_search_candidates_rows", label_shapes: &[NONE], source: "catalog_search.go" },
    MetricSpec { name: "nornicdb_search_index_size_bytes", label_shapes: &[&["kind"]], source: "catalog_search.go" },
    MetricSpec { name: "nornicdb_storage_nodes_total", label_shapes: &[NONE], source: "catalog_storage.go" },
    MetricSpec { name: "nornicdb_storage_edges_total", label_shapes: &[NONE], source: "catalog_storage.go" },
    MetricSpec { name: "nornicdb_storage_id_dict_counter_nodes", label_shapes: &[NONE], source: "catalog_storage.go" },
    MetricSpec { name: "nornicdb_storage_id_dict_counter_edges", label_shapes: &[NONE], source: "catalog_storage.go" },
    MetricSpec { name: "nornicdb_storage_id_dict_freelist_nodes", label_shapes: &[NONE], source: "catalog_storage.go" },
    MetricSpec { name: "nornicdb_storage_id_dict_freelist_edges", label_shapes: &[NONE], source: "catalog_storage.go" },
    MetricSpec { name: "nornicdb_storage_bytes", label_shapes: &[&["kind"]], source: "catalog_storage.go" },
    MetricSpec { name: "nornicdb_storage_op_duration_seconds", label_shapes: &[&["op"]], source: "catalog_storage.go" },
    MetricSpec { name: "nornicdb_storage_compactions_total", label_shapes: &[&["level", "result"]], source: "catalog_storage.go" },
    MetricSpec { name: "nornicdb_storage_compaction_duration_seconds", label_shapes: &[&["level"]], source: "catalog_storage.go" },
    MetricSpec { name: "nornicdb_storage_wal_lag_bytes", label_shapes: &[NONE], source: "catalog_storage.go" },
    MetricSpec { name: "nornicdb_storage_index_rebuild_total", label_shapes: &[STORAGE_INDEX_BASE, STORAGE_INDEX_TENANT], source: "catalog_storage.go" },
];
