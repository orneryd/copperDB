//! NornicDB observability catalog port for copperdb.

mod catalog;

pub use catalog::{
    EnumSpec, MetricSpec, ENUM_CATALOG, METRIC_CATALOG, NORNICDB_MAIN_REF,
    NORNICDB_OBSERVABILITY_PATH,
};

use std::collections::{BTreeMap, HashMap};
use std::panic::{catch_unwind, UnwindSafe};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const REDACTED_PLACEHOLDER: &str = "<REDACTED>";
pub const DEFAULT_REDACT_KEYS: &[&str] = &[
    "password",
    "token",
    "authorization",
    "secret",
    "api_key",
    "credentials",
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ObservabilityConfig {
    pub metrics: MetricsConfig,
    pub tracing: TracingConfig,
    pub pprof: PprofConfig,
}

impl ObservabilityConfig {
    pub fn apply_env_map(&mut self, env: &BTreeMap<String, String>) {
        if let Some(value) = env
            .get("COPPERDB_TELEMETRY_LISTEN")
            .filter(|v| !v.is_empty())
        {
            self.metrics.listen = value.clone();
        }
        if let Some(value) = env.get("COPPERDB_TELEMETRY_PORT").filter(|v| !v.is_empty()) {
            if self.metrics.listen.is_empty() {
                self.metrics.listen = format!(":{value}");
            }
        }
        if let Some(value) = parse_bool_env(env, "COPPERDB_TRACING_ENABLED") {
            self.tracing.enabled = value;
        }
        if let Some(value) = parse_bool_env(env, "COPPERDB_OTLP_INSECURE") {
            self.tracing.insecure = value;
        }
        if let Some(value) = env
            .get("COPPERDB_TRACE_SAMPLE_RATIO")
            .and_then(|value| value.parse::<f64>().ok())
        {
            self.tracing.sample_ratio = value.clamp(0.0, 1.0);
        }
        if let Some(value) = env
            .get("COPPERDB_TRACE_PARENT_MODE")
            .filter(|v| !v.is_empty())
        {
            self.tracing.parent_mode = value.clone();
        }
        if let Some(value) = env
            .get("COPPERDB_TRACE_PARENT_MAX_QPS")
            .and_then(|value| value.parse::<u32>().ok())
        {
            self.tracing.parent_max_qps = value.max(1);
        }
        if let Some(value) = parse_bool_env(env, "COPPERDB_PPROF_ENABLED") {
            self.pprof.enabled = value;
        }
        if let Some(value) = env.get("COPPERDB_PPROF_LISTEN").filter(|v| !v.is_empty()) {
            self.pprof.listen = value.clone();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub listen: String,
    pub tenant_labels_enabled: bool,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listen: ":9090".into(),
            tenant_labels_enabled: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TracingConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub protocol: String,
    pub insecure: bool,
    pub timeout: Duration,
    pub sample_ratio: f64,
    pub parent_mode: String,
    pub parent_max_qps: u32,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: String::new(),
            protocol: "grpc".into(),
            insecure: false,
            timeout: Duration::from_secs(5),
            sample_ratio: 0.01,
            parent_mode: String::new(),
            parent_max_qps: 100,
        }
    }
}

impl TracingConfig {
    pub fn otlp_endpoint_from_env_map(&self, env: &BTreeMap<String, String>) -> (String, bool) {
        if let Some(value) = env
            .get("OTEL_EXPORTER_OTLP_ENDPOINT")
            .filter(|value| !value.trim().is_empty())
        {
            return (value.trim().to_string(), true);
        }
        if let Some(value) = env
            .get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
            .filter(|value| !value.trim().is_empty())
        {
            return (value.trim().to_string(), true);
        }
        (self.endpoint.trim().to_string(), false)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PprofConfig {
    pub enabled: bool,
    pub listen: String,
}

impl Default for PprofConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen: "127.0.0.1:9091".into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MetricValue {
    Counter(f64),
    Gauge(f64),
    Histogram(Vec<f64>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricSample {
    pub labels: Vec<(String, String)>,
    pub value: MetricValue,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TelemetryError {
    #[error("unknown metric: {0}")]
    UnknownMetric(String),
    #[error("duplicate label key: {0}")]
    DuplicateLabel(String),
    #[error("invalid labels for {metric}: expected one of {expected:?}, got {got:?}")]
    InvalidLabels {
        metric: String,
        expected: Vec<Vec<String>>,
        got: Vec<String>,
    },
    #[error("metric type mismatch for {metric}: existing={existing}, requested={requested}")]
    MetricTypeMismatch {
        metric: String,
        existing: &'static str,
        requested: &'static str,
    },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum HealthError {
    #[error("health check failed: {0}")]
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SampleKey {
    metric: String,
    labels: Vec<(String, String)>,
}

#[derive(Debug, Default)]
pub struct Telemetry {
    values: RwLock<HashMap<SampleKey, MetricValue>>,
}

impl Telemetry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn metric_spec(name: &str) -> Option<&'static MetricSpec> {
        METRIC_CATALOG.iter().find(|metric| metric.name == name)
    }

    pub fn record_counter(
        &self,
        name: &str,
        labels: &[(&str, &str)],
    ) -> Result<(), TelemetryError> {
        let key = validate_labels(name, labels)?;
        self.with_value(key, "counter", |current| match current {
            None => Ok(MetricValue::Counter(1.0)),
            Some(MetricValue::Counter(existing)) => Ok(MetricValue::Counter(existing + 1.0)),
            Some(other) => Err(TelemetryError::MetricTypeMismatch {
                metric: name.into(),
                existing: metric_value_kind(&other),
                requested: "counter",
            }),
        })
    }

    pub fn set_gauge(
        &self,
        name: &str,
        labels: &[(&str, &str)],
        value: f64,
    ) -> Result<(), TelemetryError> {
        let key = validate_labels(name, labels)?;
        self.with_value(key, "gauge", |current| match current {
            None | Some(MetricValue::Gauge(_)) => Ok(MetricValue::Gauge(value)),
            Some(other) => Err(TelemetryError::MetricTypeMismatch {
                metric: name.into(),
                existing: metric_value_kind(&other),
                requested: "gauge",
            }),
        })
    }

    pub fn observe_histogram(
        &self,
        name: &str,
        labels: &[(&str, &str)],
        value: f64,
    ) -> Result<(), TelemetryError> {
        let key = validate_labels(name, labels)?;
        self.with_value(key, "histogram", |current| match current {
            None => Ok(MetricValue::Histogram(vec![value])),
            Some(MetricValue::Histogram(mut values)) => {
                values.push(value);
                Ok(MetricValue::Histogram(values))
            }
            Some(other) => Err(TelemetryError::MetricTypeMismatch {
                metric: name.into(),
                existing: metric_value_kind(&other),
                requested: "histogram",
            }),
        })
    }

    pub fn snapshot_metric(&self, name: &str) -> Result<Vec<MetricSample>, TelemetryError> {
        if Self::metric_spec(name).is_none() {
            return Err(TelemetryError::UnknownMetric(name.into()));
        }

        let values = self.values.read().expect("telemetry lock poisoned");
        let mut out = values
            .iter()
            .filter(|(key, _)| key.metric == name)
            .map(|(key, value)| MetricSample {
                labels: key.labels.clone(),
                value: value.clone(),
            })
            .collect::<Vec<_>>();

        out.sort_by(|a, b| a.labels.cmp(&b.labels));
        Ok(out)
    }

    pub fn seed_zero_catalog_metrics(&self) {
        for spec in METRIC_CATALOG {
            let shape = spec.label_shapes.first().copied().unwrap_or(&[]);
            if shape.is_empty() {
                let _ = self.set_gauge(spec.name, &[], 0.0);
            } else {
                let labels = shape.iter().map(|name| (*name, "mock")).collect::<Vec<_>>();
                let _ = self.set_gauge(spec.name, &labels, 0.0);
            }
        }
    }

    fn with_value(
        &self,
        key: SampleKey,
        _kind: &'static str,
        update: impl FnOnce(Option<MetricValue>) -> Result<MetricValue, TelemetryError>,
    ) -> Result<(), TelemetryError> {
        let mut values = self.values.write().expect("telemetry lock poisoned");
        let current = values.remove(&key);
        let next = update(current)?;
        values.insert(key, next);
        Ok(())
    }
}

pub type CheckResult = Result<(), String>;
type CheckFunc = Arc<dyn Fn() -> CheckResult + Send + Sync>;

#[derive(Clone)]
struct RegisteredCheck {
    check: CheckFunc,
    required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckStatus {
    pub ok: bool,
    pub latency_ms: u128,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyResult {
    pub ok: bool,
    pub checks: BTreeMap<String, CheckStatus>,
}

#[derive(Default)]
pub struct Health {
    checks: RwLock<BTreeMap<String, RegisteredCheck>>,
}

impl Health {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &self,
        name: impl Into<String>,
        required: bool,
        check: impl Fn() -> CheckResult + Send + Sync + 'static,
    ) {
        self.checks.write().expect("health lock poisoned").insert(
            name.into(),
            RegisteredCheck {
                check: Arc::new(check),
                required,
            },
        );
    }

    pub fn deregister(&self, name: &str) {
        self.checks
            .write()
            .expect("health lock poisoned")
            .remove(name);
    }

    pub fn ready(&self) -> ReadyResult {
        let snapshot = self.checks.read().expect("health lock poisoned").clone();
        let mut out = ReadyResult {
            ok: true,
            checks: BTreeMap::new(),
        };
        for (name, registered) in snapshot {
            let start = Instant::now();
            let result = (registered.check)();
            let status = CheckStatus {
                ok: result.is_ok(),
                latency_ms: start.elapsed().as_millis(),
                error: result.err(),
            };
            if registered.required && !status.ok {
                out.ok = false;
            }
            out.checks.insert(name, status);
        }
        out
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceInfo {
    pub name: String,
    pub version: String,
    pub component: Option<String>,
    pub node_id: Option<String>,
    pub cluster_mode: Option<String>,
    pub replication_role: Option<String>,
}

impl ServiceInfo {
    pub fn resource_attributes(&self, env: &BTreeMap<String, String>) -> BTreeMap<String, String> {
        let (instance_id, _) = resolve_instance_id(self.node_id.as_deref(), env);
        let mut attrs = BTreeMap::new();
        attrs.insert("service.name".into(), self.name.clone());
        attrs.insert("service.version".into(), self.version.clone());
        attrs.insert("service.instance.id".into(), instance_id);
        if let Some(component) = &self.component {
            attrs.insert("service.component".into(), component.clone());
        }
        if let Some(cluster_mode) = &self.cluster_mode {
            attrs.insert("copperdb.cluster.mode".into(), cluster_mode.clone());
        }
        if let Some(replication_role) = &self.replication_role {
            attrs.insert("copperdb.replication.role".into(), replication_role.clone());
        }
        attrs
    }
}

pub fn resolve_instance_id(
    node_id: Option<&str>,
    env: &BTreeMap<String, String>,
) -> (String, &'static str) {
    if let Some(node_id) = node_id.filter(|value| !value.trim().is_empty()) {
        return (node_id.to_string(), "config");
    }
    if let Some(pod) = env.get("POD_NAME").filter(|value| !value.trim().is_empty()) {
        return (pod.clone(), "POD_NAME");
    }
    if let Some(host) = env.get("HOSTNAME").filter(|value| !value.trim().is_empty()) {
        return (host.clone(), "HOSTNAME");
    }
    ("standalone".into(), "fallback")
}

pub fn redact_fields(
    fields: &BTreeMap<String, String>,
    extra_keys: &[&str],
) -> BTreeMap<String, String> {
    let mut redact_keys = DEFAULT_REDACT_KEYS
        .iter()
        .map(|key| key.to_ascii_lowercase())
        .collect::<std::collections::BTreeSet<_>>();
    redact_keys.extend(extra_keys.iter().map(|key| key.to_ascii_lowercase()));
    fields
        .iter()
        .map(|(key, value)| {
            if redact_keys.contains(&key.to_ascii_lowercase()) {
                (key.clone(), REDACTED_PLACEHOLDER.to_string())
            } else {
                (key.clone(), value.replace(['\r', '\n'], ""))
            }
        })
        .collect()
}

pub fn mandatory_fields(service: &str, version: &str, node_id: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("service".into(), service.into()),
        ("version".into(), version.into()),
        ("node_id".into(), node_id.into()),
    ])
}

pub fn run_recovering<T>(operation: impl FnOnce() -> T + UnwindSafe) -> Result<T, String> {
    catch_unwind(operation).map_err(|panic| {
        panic
            .downcast_ref::<&str>()
            .map(|value| (*value).to_string())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "panic".into())
    })
}

fn parse_bool_env(env: &BTreeMap<String, String>, key: &str) -> Option<bool> {
    match env.get(key)?.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn validate_labels(name: &str, labels: &[(&str, &str)]) -> Result<SampleKey, TelemetryError> {
    let metric = Telemetry::metric_spec(name)
        .ok_or_else(|| TelemetryError::UnknownMetric(name.to_string()))?;

    let mut map = BTreeMap::<String, String>::new();
    for (k, v) in labels {
        if map.insert((*k).to_string(), (*v).to_string()).is_some() {
            return Err(TelemetryError::DuplicateLabel((*k).to_string()));
        }
    }

    let got = map.keys().cloned().collect::<Vec<_>>();
    let valid = metric.label_shapes.iter().any(|shape| {
        let mut expected = shape.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        expected.sort();
        expected == got
    });

    if !valid {
        let expected = metric
            .label_shapes
            .iter()
            .map(|shape| shape.iter().map(|s| s.to_string()).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        return Err(TelemetryError::InvalidLabels {
            metric: name.to_string(),
            expected,
            got,
        });
    }

    Ok(SampleKey {
        metric: name.to_string(),
        labels: map.into_iter().collect(),
    })
}

fn metric_value_kind(value: &MetricValue) -> &'static str {
    match value {
        MetricValue::Counter(_) => "counter",
        MetricValue::Gauge(_) => "gauge",
        MetricValue::Histogram(_) => "histogram",
    }
}

pub fn classify_cypher_op_type(query: &str) -> &'static str {
    let normalized = query.trim().to_ascii_uppercase();
    if normalized.is_empty() {
        "parse_error"
    } else if normalized.starts_with("CREATE INDEX")
        || normalized.starts_with("DROP INDEX")
        || normalized.starts_with("CREATE CONSTRAINT")
        || normalized.starts_with("DROP CONSTRAINT")
    {
        "schema"
    } else if normalized.starts_with("MATCH") || normalized.starts_with("RETURN") {
        "read"
    } else if normalized.starts_with("CREATE")
        || normalized.starts_with("MERGE")
        || normalized.starts_with("DELETE")
        || normalized.starts_with("SET")
    {
        "write"
    } else if normalized.starts_with("SHOW DATABASE")
        || normalized.starts_with("CREATE DATABASE")
        || normalized.starts_with("DROP DATABASE")
    {
        "admin"
    } else if normalized.starts_with("USE ") {
        "fabric"
    } else {
        "read"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_is_copied_from_nornicdb_observability() {
        assert!(METRIC_CATALOG.len() >= 70);
        assert!(METRIC_CATALOG
            .iter()
            .any(|m| m.name == "nornicdb_auth_attempts_total"));
        assert!(METRIC_CATALOG
            .iter()
            .any(|m| m.name == "nornicdb_http_requests_total"));
        assert!(METRIC_CATALOG
            .iter()
            .any(|m| m.name == "nornicdb_replication_last_contact_seconds"));
        assert!(METRIC_CATALOG
            .iter()
            .any(|m| m.name == "nornicdb_knowledge_policy_decay_score"));
        assert!(ENUM_CATALOG
            .iter()
            .any(|e| e.name == "AllowedCypherOpTypes"));
        assert!(ENUM_CATALOG
            .iter()
            .any(|e| e.name == "AllowedStorageIndexes"));
    }

    #[test]
    fn record_unknown_metric_returns_error() {
        let telemetry = Telemetry::new();
        let err = telemetry
            .record_counter("nornicdb_missing_metric_total", &[])
            .unwrap_err();
        assert_eq!(
            err,
            TelemetryError::UnknownMetric("nornicdb_missing_metric_total".into())
        );
    }

    #[test]
    fn duplicate_labels_are_rejected_deterministically() {
        let telemetry = Telemetry::new();
        let err = telemetry
            .record_counter(
                "nornicdb_auth_attempts_total",
                &[
                    ("result", "success"),
                    ("protocol", "http"),
                    ("protocol", "bolt"),
                ],
            )
            .unwrap_err();
        assert_eq!(err, TelemetryError::DuplicateLabel("protocol".into()));
    }

    #[test]
    fn wrong_label_shape_is_rejected_with_expected_shapes() {
        let telemetry = Telemetry::new();
        let err = telemetry
            .record_counter(
                "nornicdb_http_requests_total",
                &[("method", "GET"), ("path_template", "/health")],
            )
            .unwrap_err();

        match err {
            TelemetryError::InvalidLabels {
                metric,
                expected,
                got,
            } => {
                assert_eq!(metric, "nornicdb_http_requests_total");
                assert_eq!(got, vec!["method".to_string(), "path_template".to_string()]);
                assert_eq!(expected.len(), 2);
                assert!(expected.contains(&vec![
                    "method".into(),
                    "path_template".into(),
                    "status_class".into()
                ]));
                assert!(expected.contains(&vec![
                    "method".into(),
                    "path_template".into(),
                    "status_class".into(),
                    "database".into()
                ]));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn metric_type_mismatch_errors_are_covered() {
        let telemetry = Telemetry::new();
        telemetry
            .set_gauge("nornicdb_storage_wal_lag_bytes", &[], 8.0)
            .unwrap();
        let err = telemetry
            .record_counter("nornicdb_storage_wal_lag_bytes", &[])
            .unwrap_err();
        assert_eq!(
            err,
            TelemetryError::MetricTypeMismatch {
                metric: "nornicdb_storage_wal_lag_bytes".into(),
                existing: "gauge",
                requested: "counter",
            }
        );
    }

    #[test]
    fn deterministic_snapshot_asserts_labels_and_values() {
        let telemetry = Telemetry::new();

        telemetry
            .record_counter(
                "nornicdb_http_requests_total",
                &[
                    ("method", "GET"),
                    ("path_template", "/health"),
                    ("status_class", "2xx"),
                ],
            )
            .unwrap();
        telemetry
            .record_counter(
                "nornicdb_http_requests_total",
                &[
                    ("method", "GET"),
                    ("path_template", "/health"),
                    ("status_class", "2xx"),
                ],
            )
            .unwrap();
        telemetry
            .observe_histogram(
                "nornicdb_cypher_query_duration_seconds",
                &[("op_type", "read")],
                0.015,
            )
            .unwrap();

        let snapshot = telemetry
            .snapshot_metric("nornicdb_http_requests_total")
            .unwrap();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(
            snapshot[0],
            MetricSample {
                labels: vec![
                    ("method".into(), "GET".into()),
                    ("path_template".into(), "/health".into()),
                    ("status_class".into(), "2xx".into()),
                ],
                value: MetricValue::Counter(2.0),
            }
        );

        let cypher_snapshot = telemetry
            .snapshot_metric("nornicdb_cypher_query_duration_seconds")
            .unwrap();
        assert_eq!(cypher_snapshot.len(), 1);
        assert_eq!(
            cypher_snapshot[0],
            MetricSample {
                labels: vec![("op_type".into(), "read".into())],
                value: MetricValue::Histogram(vec![0.015]),
            }
        );
    }

    #[test]
    fn classify_cypher_op_type_matches_expected_mapping() {
        assert_eq!(classify_cypher_op_type("MATCH (n) RETURN n"), "read");
        assert_eq!(classify_cypher_op_type("CREATE (n)"), "write");
        assert_eq!(classify_cypher_op_type("CREATE INDEX foo"), "schema");
        assert_eq!(classify_cypher_op_type("SHOW DATABASES"), "admin");
        assert_eq!(
            classify_cypher_op_type("USE db MATCH (n) RETURN n"),
            "fabric"
        );
        assert_eq!(classify_cypher_op_type("   "), "parse_error");
    }

    #[test]
    fn observability_config_defaults_and_env_overlay_match_contract() {
        let mut config = ObservabilityConfig::default();
        assert!(config.metrics.enabled);
        assert_eq!(config.metrics.listen, ":9090");
        assert!(!config.tracing.enabled);
        assert_eq!(config.tracing.timeout, Duration::from_secs(5));
        assert_eq!(config.pprof.listen, "127.0.0.1:9091");

        let env = BTreeMap::from([
            ("COPPERDB_TRACING_ENABLED".into(), "true".into()),
            ("COPPERDB_TRACE_SAMPLE_RATIO".into(), "2.5".into()),
            ("COPPERDB_TRACE_PARENT_MODE".into(), "capped".into()),
            ("COPPERDB_TRACE_PARENT_MAX_QPS".into(), "0".into()),
            ("COPPERDB_PPROF_ENABLED".into(), "true".into()),
            ("COPPERDB_PPROF_LISTEN".into(), "127.0.0.1:9191".into()),
        ]);
        config.apply_env_map(&env);
        assert!(config.tracing.enabled);
        assert_eq!(config.tracing.sample_ratio, 1.0);
        assert_eq!(config.tracing.parent_mode, "capped");
        assert_eq!(config.tracing.parent_max_qps, 1);
        assert!(config.pprof.enabled);
        assert_eq!(config.pprof.listen, "127.0.0.1:9191");
    }

    #[test]
    fn otlp_endpoint_env_precedence_is_preserved() {
        let tracing = TracingConfig {
            endpoint: "http://yaml:4317".into(),
            ..Default::default()
        };
        let env = BTreeMap::from([(
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT".into(),
            "http://env:4317".into(),
        )]);
        assert_eq!(
            tracing.otlp_endpoint_from_env_map(&env),
            ("http://env:4317".into(), true)
        );
        assert_eq!(
            tracing.otlp_endpoint_from_env_map(&BTreeMap::new()),
            ("http://yaml:4317".into(), false)
        );
    }

    #[test]
    fn health_registry_reports_required_failures_only_for_overall_status() {
        let health = Health::new();
        health.register("storage", true, || Ok(()));
        health.register("downstream", false, || Err("offline".into()));
        let ready = health.ready();
        assert!(ready.ok);
        assert!(ready.checks["storage"].ok);
        assert!(!ready.checks["downstream"].ok);

        health.register("replication", true, || Err("no quorum".into()));
        let ready = health.ready();
        assert!(!ready.ok);
        assert_eq!(
            ready.checks["replication"].error.as_deref(),
            Some("no quorum")
        );
    }

    #[test]
    fn resource_identity_resolution_uses_config_pod_hostname_fallback_order() {
        let env = BTreeMap::from([
            ("POD_NAME".into(), "pod-a".into()),
            ("HOSTNAME".into(), "host-a".into()),
        ]);
        assert_eq!(
            resolve_instance_id(Some("node-a"), &env),
            ("node-a".into(), "config")
        );
        assert_eq!(
            resolve_instance_id(None, &env),
            ("pod-a".into(), "POD_NAME")
        );
        let service = ServiceInfo {
            name: "copperdb".into(),
            version: "0.1.0".into(),
            component: Some("server".into()),
            node_id: None,
            cluster_mode: Some("cluster".into()),
            replication_role: Some("primary".into()),
        };
        let attrs = service.resource_attributes(&env);
        assert_eq!(attrs["service.instance.id"], "pod-a");
        assert_eq!(attrs["copperdb.cluster.mode"], "cluster");
    }

    #[test]
    fn redaction_is_case_insensitive_and_strips_crlf() {
        let fields = BTreeMap::from([
            ("Authorization".into(), "Bearer secret".into()),
            ("message".into(), "hello\nworld".into()),
            ("custom".into(), "hide-me".into()),
        ]);
        let redacted = redact_fields(&fields, &["custom"]);
        assert_eq!(redacted["Authorization"], REDACTED_PLACEHOLDER);
        assert_eq!(redacted["custom"], REDACTED_PLACEHOLDER);
        assert_eq!(redacted["message"], "helloworld");
    }

    #[test]
    fn mandatory_fields_and_recovering_boundary_are_available() {
        let fields = mandatory_fields("copperdb", "0.1.0", "node-a");
        assert_eq!(fields["service"], "copperdb");
        assert_eq!(fields["version"], "0.1.0");
        assert_eq!(fields["node_id"], "node-a");

        assert_eq!(run_recovering(|| 42).unwrap(), 42);
        assert_eq!(run_recovering(|| panic!("boom")).unwrap_err(), "boom");
    }
}
