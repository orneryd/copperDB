//! Security monitoring and anomaly detection.
//!
//! Equivalent to Go's `pkg/heimdall` in NornicDB.
//! Named after the Norse god who watches over the Bifrost.
//! Monitors query patterns, detects anomalies, and triggers responses.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use thiserror::Error;

use async_trait::async_trait;
use copperdb_plugin::{
    ActionCallContext, ActionDescriptor, ActionError, DatabaseEventHookDescriptor,
    DatabaseEventType, PackageCapability, PackageDefinition, PackageDescriptor, PackageFactory,
    PackageHealth, PackageHostContext, PackageInstance, PackageInstanceError,
    PackageLifecycleContext, PackageStatus,
};
use regex::Regex;
use semver::Version;

pub const PACKAGE_ID: &str = "watcher";
pub const QUERY_ACTION: &str = "heimdall_watcher_query";

const STATUS_UNINITIALIZED: u8 = 0;
const STATUS_READY: u8 = 1;
const STATUS_RUNNING: u8 = 2;
const STATUS_STOPPED: u8 = 3;

pub fn factory() -> WatcherFactory {
    WatcherFactory::default()
}

pub fn package() -> PackageDefinition {
    let descriptor =
        PackageDescriptor::new(PACKAGE_ID, Version::new(1, 0, 0), "copperdb contributors")
            .requesting([PackageCapability::QueryRead, PackageCapability::Events]);
    PackageDefinition::new(descriptor).with_action(ActionDescriptor::new(
        QUERY_ACTION,
        "Execute a read-only Cypher query for repository investigation. Use when explicit graph inspection is needed for code understanding. Params: cypher (required), params (optional), database (optional).",
        json!({
            "type": "object",
            "properties": {
                "cypher": {"type": "string", "description": "Cypher query to execute"},
                "params": {"type": "object", "description": "Optional query parameters"},
                "database": {"type": "string", "description": "Logical database name (optional)"}
            },
            "required": ["cypher"]
        }),
        "coding",
        Arc::new(query_action),
    ))
    .with_event_hook(DatabaseEventHookDescriptor::new(Arc::new(|event| {
        Box::pin(async move {
            if event.event_type == DatabaseEventType::QueryFailed {
                let database = event
                    .metadata
                    .get("database")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                tracing::warn!(
                    database = %database,
                    error = %event.error,
                    "Heimdall observed a failed query"
                );
            }
            Ok(())
        })
    })))
}

#[derive(Debug, Clone, Default)]
pub struct WatcherFactory {
    status: Arc<AtomicU8>,
}

impl PackageFactory for WatcherFactory {
    fn definition(&self) -> PackageDefinition {
        package()
    }

    fn create(
        &self,
        _host: PackageHostContext,
    ) -> Result<Arc<dyn PackageInstance>, PackageInstanceError> {
        Ok(Arc::new(WatcherInstance {
            status: Arc::clone(&self.status),
        }))
    }
}

#[derive(Debug)]
struct WatcherInstance {
    status: Arc<AtomicU8>,
}

#[async_trait]
impl PackageInstance for WatcherInstance {
    async fn initialize(
        &self,
        _context: PackageLifecycleContext,
    ) -> Result<(), PackageInstanceError> {
        self.status.store(STATUS_READY, Ordering::Release);
        Ok(())
    }

    async fn start(&self, _context: PackageLifecycleContext) -> Result<(), PackageInstanceError> {
        self.status.store(STATUS_RUNNING, Ordering::Release);
        Ok(())
    }

    async fn stop(&self, _context: PackageLifecycleContext) -> Result<(), PackageInstanceError> {
        self.status.store(STATUS_STOPPED, Ordering::Release);
        Ok(())
    }

    async fn shutdown(
        &self,
        _context: PackageLifecycleContext,
    ) -> Result<(), PackageInstanceError> {
        self.status.store(STATUS_UNINITIALIZED, Ordering::Release);
        Ok(())
    }

    fn status(&self) -> PackageStatus {
        match self.status.load(Ordering::Acquire) {
            STATUS_READY => PackageStatus::Ready,
            STATUS_RUNNING => PackageStatus::Running,
            STATUS_STOPPED => PackageStatus::Stopped,
            _ => PackageStatus::Uninitialized,
        }
    }

    fn health(&self) -> PackageHealth {
        let status = self.status();
        let mut health = PackageHealth::new(
            status,
            matches!(status, PackageStatus::Ready | PackageStatus::Running),
        );
        health.message = Some(format!("Heimdall coding plugin is {status:?}").to_ascii_lowercase());
        health
    }
}

fn query_action(context: &ActionCallContext<'_>, input: &Value) -> Result<Value, ActionError> {
    let cypher = input
        .get("cypher")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if cypher.is_empty() {
        return Ok(action_failure("cypher parameter required"));
    }
    if cypher.len() > 10_000 {
        return Ok(action_failure("query too long (max 10000 characters)"));
    }
    if contains_write_operation(cypher) {
        return Ok(action_failure(
            "query contains write operations; heimdall_watcher_query only allows read-only Cypher",
        ));
    }
    let database = input
        .get("database")
        .and_then(Value::as_str)
        .filter(|database| !database.is_empty())
        .unwrap_or(context.default_database);
    let params = input
        .get("params")
        .and_then(Value::as_object)
        .map(|params| {
            params
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    match context.query_service.query_read(
        context.request_context,
        database,
        cypher,
        &params,
        context.caller_roles,
    ) {
        Ok(result) => Ok(json!({
            "success": true,
            "message": format!("Query returned {} row(s)", result.rows.len()),
            "data": {"rows": result.rows}
        })),
        Err(error) => Ok(action_failure(format!("Query failed: {}", error.code))),
    }
}

fn action_failure(message: impl Into<String>) -> Value {
    json!({"success": false, "message": message.into()})
}

fn contains_write_operation(cypher: &str) -> bool {
    static READ_ONLY_GUARD: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(
            r"(?i)\b(CREATE|MERGE|SET|DELETE|REMOVE|FOREACH|DETACH\s+DELETE|LOAD\s+CSV|DROP|ALTER|GRANT|DENY|REVOKE)\b",
        )
        .expect("static read-only expression is valid")
    });
    let without_comments = cypher
        .lines()
        .map(|line| line.split_once("//").map_or(line, |(prefix, _)| prefix))
        .collect::<Vec<_>>()
        .join("\n");
    READ_ONLY_GUARD.is_match(&without_comments)
}

#[derive(Debug, Error)]
pub enum HeimdallError {
    #[error("anomaly detected: {0}")]
    AnomalyDetected(String),
    #[error("rate limit exceeded for {0}")]
    RateLimitExceeded(String),
}

/// Anomaly severity levels.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AnomalyLevel {
    Low,
    Medium,
    High,
    Critical,
}

/// A detected security anomaly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Anomaly {
    pub level: AnomalyLevel,
    pub description: String,
    pub username: String,
    pub source_ip: Option<String>,
}

/// Simple rate limiter using a sliding window counter.
pub struct RateLimiter {
    counter: Arc<AtomicU64>,
    max_per_second: u64,
    window_start: std::sync::Mutex<std::time::Instant>,
}

impl RateLimiter {
    pub fn new(max_per_second: u64) -> Self {
        Self {
            counter: Arc::new(AtomicU64::new(0)),
            max_per_second,
            window_start: std::sync::Mutex::new(std::time::Instant::now()),
        }
    }

    /// Try to allow a request. Returns `Ok(())` if within rate limit.
    pub fn check(&self, username: &str) -> Result<(), HeimdallError> {
        let mut start = self.window_start.lock().unwrap();
        if start.elapsed().as_secs() >= 1 {
            self.counter.store(0, Ordering::Relaxed);
            *start = std::time::Instant::now();
        }
        let count = self.counter.fetch_add(1, Ordering::Relaxed);
        if count >= self.max_per_second {
            Err(HeimdallError::RateLimitExceeded(username.to_owned()))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use copperdb_plugin::{
        ActionQueryResult, ActionQueryService, PackageRuntime, PackageSpec, resolve_packages,
    };
    use copperdb_util::RequestContext;
    use std::sync::Mutex;

    type RecordedQuery = (String, String, BTreeMap<String, Value>, Vec<String>);

    #[derive(Debug)]
    struct RecordingQueryService {
        calls: Mutex<Vec<RecordedQuery>>,
        result: Mutex<Result<ActionQueryResult, ActionError>>,
    }

    impl Default for RecordingQueryService {
        fn default() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                result: Mutex::new(Ok(ActionQueryResult::default())),
            }
        }
    }

    impl ActionQueryService for RecordingQueryService {
        fn query_read(
            &self,
            _request_context: &RequestContext,
            database: &str,
            cypher: &str,
            params: &BTreeMap<String, Value>,
            caller_roles: &[String],
        ) -> Result<ActionQueryResult, ActionError> {
            self.calls.lock().unwrap().push((
                database.to_string(),
                cypher.to_string(),
                params.clone(),
                caller_roles.to_vec(),
            ));
            self.result.lock().unwrap().clone()
        }
    }

    #[test]
    fn test_rate_limiter_allows_within_limit() {
        let limiter = RateLimiter::new(10);
        for _ in 0..10 {
            assert!(limiter.check("alice").is_ok());
        }
    }

    #[test]
    fn test_rate_limiter_blocks_over_limit() {
        let limiter = RateLimiter::new(2);
        assert!(limiter.check("alice").is_ok());
        assert!(limiter.check("alice").is_ok());
        assert!(limiter.check("alice").is_err());
    }

    #[test]
    fn test_rate_limiter_error_type() {
        let limiter = RateLimiter::new(1);
        let _ = limiter.check("bob");
        let err = limiter.check("bob").unwrap_err();
        assert!(matches!(err, HeimdallError::RateLimitExceeded(_)));
    }

    #[test]
    fn test_anomaly_levels() {
        let anomaly = Anomaly {
            level: AnomalyLevel::High,
            description: "unusual query pattern".into(),
            username: "alice".into(),
            source_ip: Some("10.0.0.1".into()),
        };
        assert_eq!(anomaly.level, AnomalyLevel::High);
    }

    #[test]
    fn test_anomaly_serialization() {
        let anomaly = Anomaly {
            level: AnomalyLevel::Critical,
            description: "brute force".into(),
            username: "attacker".into(),
            source_ip: None,
        };
        let json = serde_json::to_string(&anomaly).unwrap();
        let decoded: Anomaly = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.level, AnomalyLevel::Critical);
        assert_eq!(decoded.username, "attacker");
    }

    #[test]
    fn watcher_query_matches_upstream_read_only_contract() {
        let resolved = resolve_packages([package()]).unwrap();
        let registry = resolved.action_registry();
        let service = RecordingQueryService {
            result: Mutex::new(Ok(ActionQueryResult {
                rows: vec![BTreeMap::from([("one".into(), json!(1))])],
            })),
            ..RecordingQueryService::default()
        };
        let request = RequestContext::detached();
        let roles = vec!["reader".to_string()];
        let context = ActionCallContext {
            request_context: &request,
            default_database: "copperdb",
            caller_roles: &roles,
            query_service: &service,
        };

        let result = registry
            .execute(
                QUERY_ACTION,
                &context,
                &json!({"cypher": "RETURN $value AS one", "params": {"value": 1}}),
            )
            .unwrap();

        assert_eq!(
            result,
            json!({
                "success": true,
                "message": "Query returned 1 row(s)",
                "data": {"rows": [{"one": 1}]}
            })
        );
        assert_eq!(
            service.calls.lock().unwrap().as_slice(),
            &[(
                "copperdb".into(),
                "RETURN $value AS one".into(),
                BTreeMap::from([("value".into(), json!(1))]),
                vec!["reader".into()]
            )]
        );
        assert_eq!(
            registry.get(QUERY_ACTION).unwrap().package_id(),
            Some(PACKAGE_ID)
        );
    }

    #[test]
    fn watcher_query_rejects_writes_but_ignores_line_comments() {
        let resolved = resolve_packages([package()]).unwrap();
        let registry = resolved.action_registry();
        let service = RecordingQueryService::default();
        let request = RequestContext::detached();
        let context = ActionCallContext {
            request_context: &request,
            default_database: "copperdb",
            caller_roles: &[],
            query_service: &service,
        };

        let blocked = registry
            .execute(
                QUERY_ACTION,
                &context,
                &json!({"cypher": "CREATE (:Thing)"}),
            )
            .unwrap();
        assert_eq!(
            blocked,
            action_failure(
                "query contains write operations; heimdall_watcher_query only allows read-only Cypher"
            )
        );
        let allowed = registry
            .execute(
                QUERY_ACTION,
                &context,
                &json!({"cypher": "// CREATE (:Ignored)\nRETURN 1"}),
            )
            .unwrap();
        assert_eq!(allowed["success"], true);
        assert_eq!(service.calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn watcher_lifecycle_returns_to_uninitialized() {
        let factory = factory();
        let runtime = PackageRuntime::start(
            [Arc::new(factory.clone()) as Arc<dyn PackageFactory>],
            [PackageSpec::new(PACKAGE_ID)
                .granting([PackageCapability::QueryRead, PackageCapability::Events])],
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(
            runtime.health().await[PACKAGE_ID].status,
            PackageStatus::Running
        );

        runtime.shutdown().await.unwrap();

        assert_eq!(factory.status.load(Ordering::Acquire), STATUS_UNINITIALIZED);
    }
}
