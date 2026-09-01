//! NornicDB observability catalog port for copperdb.

mod catalog;

pub use catalog::{
    EnumSpec, MetricSpec, ENUM_CATALOG, METRIC_CATALOG, NORNICDB_MAIN_REF,
    NORNICDB_OBSERVABILITY_PATH,
};

use std::collections::{BTreeMap, HashMap};
use std::panic::{catch_unwind, UnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::{OnceLock, RwLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use copperdb_util::RequestCancellationReason;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{
    BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracer, SdkTracerProvider,
};
use opentelemetry_sdk::Resource;

pub const REDACTED_PLACEHOLDER: &str = "<REDACTED>";
pub const DEFAULT_REDACT_KEYS: &[&str] = &[
    "password",
    "token",
    "authorization",
    "secret",
    "api_key",
    "credentials",
];

static GLOBAL_TELEMETRY: OnceLock<Arc<Telemetry>> = OnceLock::new();

pub fn install_global_telemetry(telemetry: Arc<Telemetry>) -> Result<(), Arc<Telemetry>> {
    GLOBAL_TELEMETRY.set(telemetry)
}

pub fn global_telemetry() -> Option<&'static Telemetry> {
    GLOBAL_TELEMETRY.get().map(Arc::as_ref)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ObservabilityConfig {
    pub metrics: MetricsConfig,
    pub tracing: TracingConfig,
    pub pprof: PprofConfig,
}

impl ObservabilityConfig {
    pub fn apply_env_map(&mut self, env: &BTreeMap<String, String>) {
        if let Some(value) = parse_bool_env(env, "COPPERDB_METRICS_ENABLED") {
            self.metrics.enabled = value;
        }
        let explicit_listen = env
            .get("COPPERDB_TELEMETRY_LISTEN")
            .filter(|value| !value.is_empty());
        if let Some(value) = explicit_listen {
            self.metrics.listen = value.clone();
        } else if let Some(value) = env
            .get("COPPERDB_TELEMETRY_PORT")
            .filter(|value| !value.is_empty())
        {
            self.metrics.listen = format!("127.0.0.1:{value}");
        }
        if let Some(value) = parse_bool_env(env, "COPPERDB_TRACING_ENABLED") {
            self.tracing.enabled = value;
        }
        if let Some(value) = env
            .get("COPPERDB_OTLP_ENDPOINT")
            .filter(|value| !value.is_empty())
        {
            self.tracing.endpoint = value.clone();
        }
        if let Some(value) = env
            .get("COPPERDB_OTLP_PROTOCOL")
            .filter(|value| !value.is_empty())
        {
            self.tracing.protocol = value.clone();
        }
        if let Some(value) = parse_bool_env(env, "COPPERDB_OTLP_INSECURE") {
            self.tracing.insecure = value;
        }
        if let Some(value) = env
            .get("COPPERDB_OTLP_TIMEOUT_MS")
            .and_then(|value| value.parse::<u64>().ok())
        {
            self.tracing.timeout = Duration::from_millis(value.max(1));
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
            listen: "127.0.0.1:9090".into(),
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
    Histogram {
        count: u64,
        sum: f64,
        buckets: Vec<(u64, u64)>,
    },
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
    #[error("invalid label value for {metric}.{label}: {value}")]
    InvalidLabelValue {
        metric: String,
        label: String,
        value: String,
    },
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

#[derive(Debug)]
pub struct Telemetry {
    enabled: bool,
    values: RwLock<HashMap<SampleKey, MetricValue>>,
}

impl Default for Telemetry {
    fn default() -> Self {
        Self {
            enabled: true,
            values: RwLock::new(HashMap::new()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderMode {
    Noop,
    Test,
    Production,
}

#[derive(Debug, Clone)]
pub struct TelemetryProvider {
    mode: ProviderMode,
    telemetry: Arc<Telemetry>,
    tracer_provider: Option<Arc<SdkTracerProvider>>,
    tracing_error: Option<Arc<str>>,
    shutdown: Arc<AtomicBool>,
}

impl TelemetryProvider {
    pub fn noop() -> Self {
        Self {
            mode: ProviderMode::Noop,
            telemetry: Arc::new(Telemetry {
                enabled: false,
                values: RwLock::new(HashMap::new()),
            }),
            tracer_provider: None,
            tracing_error: None,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn test() -> Self {
        Self {
            mode: ProviderMode::Test,
            telemetry: Arc::new(Telemetry::new()),
            tracer_provider: None,
            tracing_error: None,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn production() -> Self {
        Self {
            mode: ProviderMode::Production,
            telemetry: Arc::new(Telemetry::new()),
            tracer_provider: None,
            tracing_error: None,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn initialize(
        config: &ObservabilityConfig,
        service: &ServiceInfo,
        env: &BTreeMap<String, String>,
    ) -> Self {
        let mut provider = if config.metrics.enabled {
            Self::production()
        } else {
            Self::noop()
        };
        if !config.tracing.enabled {
            return provider;
        }
        match build_tracer_provider(&config.tracing, service, env) {
            Ok(tracer_provider) => {
                global::set_text_map_propagator(TraceContextPropagator::new());
                global::set_tracer_provider(tracer_provider.clone());
                provider.tracer_provider = Some(Arc::new(tracer_provider));
            }
            Err(error) => provider.tracing_error = Some(error.into()),
        }
        provider
    }

    pub fn mode(&self) -> ProviderMode {
        self.mode
    }

    pub fn telemetry(&self) -> Arc<Telemetry> {
        Arc::clone(&self.telemetry)
    }

    pub fn tracer(&self) -> Option<SdkTracer> {
        self.tracer_provider
            .as_ref()
            .map(|provider| provider.tracer("copperdb"))
    }

    pub fn tracing_error(&self) -> Option<&str> {
        self.tracing_error.as_deref()
    }

    pub fn shutdown(&self, timeout: Duration) -> Result<(), String> {
        if self.shutdown.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.tracer_provider
            .as_ref()
            .map(|provider| provider.shutdown_with_timeout(timeout))
            .transpose()
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

fn build_tracer_provider(
    config: &TracingConfig,
    service: &ServiceInfo,
    env: &BTreeMap<String, String>,
) -> Result<SdkTracerProvider, String> {
    let (endpoint, _) = config.otlp_endpoint_from_env_map(env);
    if endpoint.starts_with("http://") && !config.insecure {
        return Err("plaintext OTLP endpoint requires COPPERDB_OTLP_INSECURE=true".into());
    }
    let exporter = build_span_exporter(config, &endpoint)?;
    let batch = BatchSpanProcessor::builder(exporter)
        .with_batch_config(
            BatchConfigBuilder::default()
                .with_max_queue_size(8_192)
                .with_max_export_batch_size(1_024)
                .with_scheduled_delay(Duration::from_secs(2))
                .build(),
        )
        .build();
    let resource = Resource::builder_empty()
        .with_attributes(
            service
                .resource_attributes(env)
                .into_iter()
                .map(|(key, value)| KeyValue::new(key, value)),
        )
        .build();
    Ok(SdkTracerProvider::builder()
        .with_span_processor(batch)
        .with_sampler(parent_sampler(config.sample_ratio))
        .with_resource(resource)
        .build())
}

fn parent_sampler(sample_ratio: f64) -> Sampler {
    Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(sample_ratio)))
}

fn build_span_exporter(config: &TracingConfig, endpoint: &str) -> Result<SpanExporter, String> {
    match config.protocol.trim().to_ascii_lowercase().as_str() {
        "grpc" => {
            let mut builder = SpanExporter::builder()
                .with_tonic()
                .with_timeout(config.timeout);
            if !endpoint.is_empty() {
                builder = builder.with_endpoint(endpoint);
            }
            builder.build().map_err(|error| error.to_string())
        }
        "http" | "http/protobuf" => {
            let mut builder = SpanExporter::builder()
                .with_http()
                .with_timeout(config.timeout);
            if !endpoint.is_empty() {
                builder = builder.with_endpoint(endpoint);
            }
            builder.build().map_err(|error| error.to_string())
        }
        protocol => Err(format!("unsupported OTLP trace protocol: {protocol}")),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationProtocol {
    Http,
    Bolt,
    Grpc,
    Graphql,
    Mcp,
}

impl CancellationProtocol {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Bolt => "bolt",
            Self::Grpc => "grpc",
            Self::Graphql => "graphql",
            Self::Mcp => "mcp",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationStage {
    Ingress,
    Execution,
}

impl CancellationStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ingress => "ingress",
            Self::Execution => "execution",
        }
    }
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
        if !self.enabled {
            return Ok(());
        }
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

    pub fn record_request_cancellation(
        &self,
        protocol: CancellationProtocol,
        stage: CancellationStage,
        reason: RequestCancellationReason,
    ) -> Result<(), TelemetryError> {
        self.record_counter(
            "copperdb_request_cancellations_total",
            &[
                ("protocol", protocol.as_str()),
                ("stage", stage.as_str()),
                ("reason", reason.as_str()),
            ],
        )
    }

    pub fn set_gauge(
        &self,
        name: &str,
        labels: &[(&str, &str)],
        value: f64,
    ) -> Result<(), TelemetryError> {
        if !self.enabled {
            return Ok(());
        }
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
        if !self.enabled {
            return Ok(());
        }
        let key = validate_labels(name, labels)?;
        self.with_value(key, "histogram", |current| match current {
            None => Ok(histogram_with_observation(value)),
            Some(MetricValue::Histogram {
                count,
                sum,
                mut buckets,
            }) => {
                observe_histogram_buckets(&mut buckets, value);
                Ok(MetricValue::Histogram {
                    count: count + 1,
                    sum: sum + value,
                    buckets,
                })
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

    pub fn encode_openmetrics(&self) -> String {
        let values = self.values.read().expect("telemetry lock poisoned");
        let mut samples = values.iter().collect::<Vec<_>>();
        samples.sort_by(|(left, _), (right, _)| {
            left.metric
                .cmp(&right.metric)
                .then_with(|| left.labels.cmp(&right.labels))
        });

        let mut output = String::new();
        let mut current_metric = None;
        for (key, value) in samples {
            if current_metric != Some(key.metric.as_str()) {
                current_metric = Some(&key.metric);
                output.push_str("# TYPE ");
                output.push_str(&key.metric);
                output.push(' ');
                output.push_str(metric_value_kind(value));
                output.push('\n');
            }
            let labels = encode_labels(&key.labels);
            match value {
                MetricValue::Counter(value) | MetricValue::Gauge(value) => {
                    output.push_str(&key.metric);
                    output.push_str(&labels);
                    output.push(' ');
                    output.push_str(&format_metric_number(*value));
                    output.push('\n');
                }
                MetricValue::Histogram {
                    count,
                    sum,
                    buckets,
                } => {
                    for (bound_micros, bucket_count) in buckets {
                        output.push_str(&key.metric);
                        output.push_str("_bucket");
                        output.push_str(&encode_histogram_labels(
                            &key.labels,
                            &format_metric_number(*bound_micros as f64 / 1_000_000.0),
                        ));
                        output.push(' ');
                        output.push_str(&bucket_count.to_string());
                        output.push('\n');
                    }
                    output.push_str(&key.metric);
                    output.push_str("_bucket");
                    output.push_str(&encode_histogram_labels(&key.labels, "+Inf"));
                    output.push(' ');
                    output.push_str(&count.to_string());
                    output.push('\n');
                    output.push_str(&key.metric);
                    output.push_str("_count");
                    output.push_str(&labels);
                    output.push(' ');
                    output.push_str(&count.to_string());
                    output.push('\n');
                    output.push_str(&key.metric);
                    output.push_str("_sum");
                    output.push_str(&labels);
                    output.push(' ');
                    output.push_str(&format_metric_number(*sum));
                    output.push('\n');
                }
            }
        }
        output.push_str("# EOF\n");
        output
    }

    pub fn encode_prometheus(&self) -> String {
        self.encode_openmetrics()
            .strip_suffix("# EOF\n")
            .unwrap_or_default()
            .to_string()
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

fn encode_labels(labels: &[(String, String)]) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let encoded = labels
        .iter()
        .map(|(key, value)| {
            let value = value
                .replace('\\', "\\\\")
                .replace('\n', "\\n")
                .replace('"', "\\\"");
            format!("{key}=\"{value}\"")
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{encoded}}}")
}

fn encode_histogram_labels(labels: &[(String, String)], bound: &str) -> String {
    let mut labels = labels.to_vec();
    labels.push(("le".into(), bound.into()));
    encode_labels(&labels)
}

const HISTOGRAM_BOUNDS_MICROS: &[u64] = &[
    1_000, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_500_000,
    5_000_000, 10_000_000,
];

fn histogram_with_observation(value: f64) -> MetricValue {
    let mut buckets = HISTOGRAM_BOUNDS_MICROS
        .iter()
        .map(|bound| (*bound, 0))
        .collect::<Vec<_>>();
    observe_histogram_buckets(&mut buckets, value);
    MetricValue::Histogram {
        count: 1,
        sum: value,
        buckets,
    }
}

fn observe_histogram_buckets(buckets: &mut [(u64, u64)], value: f64) {
    for (bound_micros, count) in buckets {
        if value <= *bound_micros as f64 / 1_000_000.0 {
            *count += 1;
        }
    }
}

fn format_metric_number(value: f64) -> String {
    if value == f64::INFINITY {
        "+Inf".into()
    } else if value == f64::NEG_INFINITY {
        "-Inf".into()
    } else if value.is_nan() {
        "NaN".into()
    } else {
        value.to_string()
    }
}

pub type CheckResult = Result<(), String>;
type CheckFunc = Arc<dyn Fn() -> CheckResult + Send + Sync>;

#[derive(Clone)]
struct RegisteredCheck {
    check: CheckFunc,
    required: bool,
    timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CheckStatus {
    pub ok: bool,
    pub latency_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
        self.register_with_timeout(name, required, Duration::from_secs(1), check);
    }

    pub fn register_with_timeout(
        &self,
        name: impl Into<String>,
        required: bool,
        timeout: Duration,
        check: impl Fn() -> CheckResult + Send + Sync + 'static,
    ) {
        self.checks.write().expect("health lock poisoned").insert(
            name.into(),
            RegisteredCheck {
                check: Arc::new(check),
                required,
                timeout: timeout.max(Duration::from_millis(1)),
            },
        );
    }

    pub fn deregister(&self, name: &str) {
        self.checks
            .write()
            .expect("health lock poisoned")
            .remove(name);
    }

    pub async fn ready(&self) -> ReadyResult {
        let snapshot = self.checks.read().expect("health lock poisoned").clone();
        let mut checks = tokio::task::JoinSet::new();
        for (name, registered) in snapshot {
            checks.spawn(async move {
                let started_at = Instant::now();
                let result = tokio::time::timeout(
                    registered.timeout,
                    tokio::task::spawn_blocking(move || (registered.check)()),
                )
                .await
                .map_err(|_| "deadline exceeded".to_string())
                .and_then(|result| result.map_err(|error| error.to_string()))
                .and_then(|result| result);
                (
                    name,
                    CheckStatus {
                        ok: result.is_ok(),
                        latency_ms: started_at.elapsed().as_millis(),
                        error: result.err(),
                    },
                    registered.required,
                )
            });
        }
        let mut out = ReadyResult {
            ok: true,
            checks: BTreeMap::new(),
        };
        while let Some(result) = checks.join_next().await {
            let (name, status, required) = result.expect("health check task must not panic");
            if required && !status.ok {
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

    for (label, value) in &map {
        if let Some(allowed) = allowed_label_values(name, label) {
            if !allowed.contains(&value.as_str()) {
                return Err(TelemetryError::InvalidLabelValue {
                    metric: name.into(),
                    label: label.clone(),
                    value: value.clone(),
                });
            }
        }
    }

    Ok(SampleKey {
        metric: name.to_string(),
        labels: map.into_iter().collect(),
    })
}

fn allowed_label_values(metric: &str, label: &str) -> Option<&'static [&'static str]> {
    match (metric, label) {
        ("copperdb_request_cancellations_total", "protocol") => {
            Some(&["http", "bolt", "grpc", "graphql", "mcp"])
        }
        ("copperdb_request_cancellations_total", "stage") => Some(&["ingress", "execution"]),
        ("copperdb_request_cancellations_total", "reason") => Some(&["explicit", "deadline"]),
        ("nornicdb_auth_attempts_total", "result") => Some(&["success", "failure", "denied"]),
        ("nornicdb_auth_attempts_total", "protocol") => Some(&["bolt", "http", "grpc"]),
        (name, "op") if name.starts_with("nornicdb_bolt_") => Some(&[
            "hello",
            "run",
            "pull",
            "begin",
            "commit",
            "discard",
            "reset",
            "goodbye",
            "route",
            "ack_failure",
        ]),
        (name, "result") if name.starts_with("nornicdb_bolt_") => {
            Some(&["success", "error", "timeout"])
        }
        (name, "transport") if name.starts_with("nornicdb_bolt_") => {
            Some(&["tcp", "tcp_tls", "ws", "ws_tls"])
        }
        (name, "status_class") if name.starts_with("nornicdb_http_") => {
            Some(&["1xx", "2xx", "3xx", "4xx", "5xx"])
        }
        (name, "op_type") if name.starts_with("nornicdb_cypher_") => {
            Some(&["read", "write", "schema", "admin", "fabric", "parse_error"])
        }
        (name, "mode") if name.starts_with("nornicdb_search_") => {
            Some(&["vector", "bm25", "hybrid"])
        }
        (name, "result") if name.starts_with("nornicdb_search_") => {
            Some(&["success", "no_results", "error"])
        }
        (name, "stage") if name.starts_with("nornicdb_search_") => {
            Some(&["embed", "index", "fuse"])
        }
        ("nornicdb_storage_op_duration_seconds", "op") => Some(&["get", "put", "delete", "scan"]),
        ("nornicdb_storage_bytes", "kind") => Some(&["nodes", "edges", "index", "wal", "search"]),
        (name, "result") if name.starts_with("nornicdb_storage_") => {
            Some(&["success", "failure", "aborted"])
        }
        _ => None,
    }
}

fn metric_value_kind(value: &MetricValue) -> &'static str {
    match value {
        MetricValue::Counter(_) => "counter",
        MetricValue::Gauge(_) => "gauge",
        MetricValue::Histogram { .. } => "histogram",
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
    use opentelemetry::trace::{
        Span as _, SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
        Tracer as _,
    };
    use opentelemetry::Context;

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
    fn noop_provider_accepts_records_without_retaining_samples() {
        let provider = TelemetryProvider::noop();
        let telemetry = provider.telemetry();

        telemetry
            .record_counter("not_even_a_catalog_metric", &[("secret", "value")])
            .unwrap();

        assert_eq!(provider.mode(), ProviderMode::Noop);
        assert_eq!(telemetry.encode_openmetrics(), "# EOF\n");
    }

    #[test]
    fn invalid_trace_exporter_falls_back_without_disabling_metrics() {
        let config = ObservabilityConfig {
            tracing: TracingConfig {
                enabled: true,
                protocol: "invalid".into(),
                ..Default::default()
            },
            ..Default::default()
        };
        let provider = TelemetryProvider::initialize(
            &config,
            &ServiceInfo {
                name: "test".into(),
                version: "1".into(),
                component: None,
                node_id: Some("node-1".into()),
                cluster_mode: None,
                replication_role: None,
            },
            &BTreeMap::new(),
        );

        assert_eq!(provider.mode(), ProviderMode::Production);
        assert!(provider.tracer().is_none());
        assert_eq!(
            provider.tracing_error(),
            Some("unsupported OTLP trace protocol: invalid")
        );
        provider
            .telemetry()
            .record_counter("nornicdb_bolt_websocket_oversized_total", &[])
            .unwrap();
        assert!(provider
            .telemetry()
            .encode_openmetrics()
            .contains("nornicdb_bolt_websocket_oversized_total 1"));
        provider.shutdown(Duration::from_millis(1)).unwrap();
        provider.shutdown(Duration::from_millis(1)).unwrap();
    }

    #[tokio::test]
    async fn unreachable_collector_does_not_block_startup_or_metrics() {
        let mut config = ObservabilityConfig::default();
        config.tracing.enabled = true;
        config.tracing.endpoint = "http://127.0.0.1:1".into();
        config.tracing.insecure = true;
        config.tracing.timeout = Duration::from_millis(10);
        let started = Instant::now();

        let provider = TelemetryProvider::initialize(
            &config,
            &ServiceInfo {
                name: "copperdb-test".into(),
                version: "1".into(),
                component: None,
                node_id: None,
                cluster_mode: None,
                replication_role: None,
            },
            &BTreeMap::new(),
        );

        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(provider.tracer().is_some());
        provider
            .telemetry()
            .record_counter("nornicdb_bolt_websocket_oversized_total", &[])
            .unwrap();
        assert!(provider
            .telemetry()
            .encode_openmetrics()
            .contains("nornicdb_bolt_websocket_oversized_total 1"));
        let shutdown_started = Instant::now();
        let _ = provider.shutdown(Duration::from_millis(25));
        assert!(shutdown_started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn parent_sampler_drops_roots_and_keeps_sampled_remote_parents() {
        let tracer_provider = SdkTracerProvider::builder()
            .with_sampler(parent_sampler(0.0))
            .build();
        let tracer = tracer_provider.tracer("sampling-test");
        let root = tracer.start("root");
        assert!(!root.span_context().is_sampled());

        let parent_span_context = SpanContext::new(
            TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap(),
            SpanId::from_hex("00f067aa0ba902b7").unwrap(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        );
        let parent = Context::new().with_remote_span_context(parent_span_context);
        let child = tracer.start_with_context("child", &parent);
        assert!(child.span_context().is_sampled());
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
    fn closed_label_axes_reject_unbounded_values() {
        let telemetry = Telemetry::new();
        let error = telemetry
            .record_counter(
                "nornicdb_http_requests_total",
                &[
                    ("method", "GET"),
                    ("path_template", "/health"),
                    ("status_class", "tenant-supplied"),
                ],
            )
            .unwrap_err();

        assert_eq!(
            error,
            TelemetryError::InvalidLabelValue {
                metric: "nornicdb_http_requests_total".into(),
                label: "status_class".into(),
                value: "tenant-supplied".into(),
            }
        );
        assert!(telemetry
            .record_counter(
                "nornicdb_search_requests_total",
                &[("mode", "semantic"), ("result", "success")],
            )
            .is_err());
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
                value: histogram_with_observation(0.015),
            }
        );
    }

    #[test]
    fn openmetrics_encoding_is_deterministic_and_omits_unrecorded_metrics() {
        let telemetry = Telemetry::new();
        telemetry
            .record_counter(
                "nornicdb_http_requests_total",
                &[
                    ("method", "GET"),
                    ("path_template", "/quoted/\"line\nslash\\"),
                    ("status_class", "2xx"),
                ],
            )
            .unwrap();
        telemetry
            .observe_histogram(
                "nornicdb_cypher_query_duration_seconds",
                &[("op_type", "read")],
                0.01,
            )
            .unwrap();
        telemetry
            .observe_histogram(
                "nornicdb_cypher_query_duration_seconds",
                &[("op_type", "read")],
                0.02,
            )
            .unwrap();

        assert_eq!(
            telemetry.encode_openmetrics(),
            concat!(
                "# TYPE nornicdb_cypher_query_duration_seconds histogram\n",
                "nornicdb_cypher_query_duration_seconds_bucket{op_type=\"read\",le=\"0.001\"} 0\n",
                "nornicdb_cypher_query_duration_seconds_bucket{op_type=\"read\",le=\"0.005\"} 0\n",
                "nornicdb_cypher_query_duration_seconds_bucket{op_type=\"read\",le=\"0.01\"} 1\n",
                "nornicdb_cypher_query_duration_seconds_bucket{op_type=\"read\",le=\"0.025\"} 2\n",
                "nornicdb_cypher_query_duration_seconds_bucket{op_type=\"read\",le=\"0.05\"} 2\n",
                "nornicdb_cypher_query_duration_seconds_bucket{op_type=\"read\",le=\"0.1\"} 2\n",
                "nornicdb_cypher_query_duration_seconds_bucket{op_type=\"read\",le=\"0.25\"} 2\n",
                "nornicdb_cypher_query_duration_seconds_bucket{op_type=\"read\",le=\"0.5\"} 2\n",
                "nornicdb_cypher_query_duration_seconds_bucket{op_type=\"read\",le=\"1\"} 2\n",
                "nornicdb_cypher_query_duration_seconds_bucket{op_type=\"read\",le=\"2.5\"} 2\n",
                "nornicdb_cypher_query_duration_seconds_bucket{op_type=\"read\",le=\"5\"} 2\n",
                "nornicdb_cypher_query_duration_seconds_bucket{op_type=\"read\",le=\"10\"} 2\n",
                "nornicdb_cypher_query_duration_seconds_bucket{op_type=\"read\",le=\"+Inf\"} 2\n",
                "nornicdb_cypher_query_duration_seconds_count{op_type=\"read\"} 2\n",
                "nornicdb_cypher_query_duration_seconds_sum{op_type=\"read\"} 0.03\n",
                "# TYPE nornicdb_http_requests_total counter\n",
                "nornicdb_http_requests_total{method=\"GET\",path_template=\"/quoted/\\\"line\\nslash\\\\\",status_class=\"2xx\"} 1\n",
                "# EOF\n",
            )
        );
        assert!(!telemetry
            .encode_openmetrics()
            .contains("nornicdb_storage_wal_lag_bytes"));
    }

    #[test]
    fn records_request_cancellation_with_bounded_labels() {
        let telemetry = Telemetry::new();
        telemetry
            .record_request_cancellation(
                CancellationProtocol::Http,
                CancellationStage::Ingress,
                RequestCancellationReason::Deadline,
            )
            .unwrap();
        telemetry
            .record_request_cancellation(
                CancellationProtocol::Bolt,
                CancellationStage::Execution,
                RequestCancellationReason::Explicit,
            )
            .unwrap();

        assert_eq!(
            telemetry
                .snapshot_metric("copperdb_request_cancellations_total")
                .unwrap(),
            vec![
                MetricSample {
                    labels: vec![
                        ("protocol".into(), "bolt".into()),
                        ("reason".into(), "explicit".into()),
                        ("stage".into(), "execution".into()),
                    ],
                    value: MetricValue::Counter(1.0),
                },
                MetricSample {
                    labels: vec![
                        ("protocol".into(), "http".into()),
                        ("reason".into(), "deadline".into()),
                        ("stage".into(), "ingress".into()),
                    ],
                    value: MetricValue::Counter(1.0),
                },
            ]
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
        assert_eq!(config.metrics.listen, "127.0.0.1:9090");
        assert!(!config.tracing.enabled);
        assert_eq!(config.tracing.timeout, Duration::from_secs(5));
        assert_eq!(config.pprof.listen, "127.0.0.1:9091");

        let env = BTreeMap::from([
            ("COPPERDB_TRACING_ENABLED".into(), "true".into()),
            ("COPPERDB_METRICS_ENABLED".into(), "false".into()),
            ("COPPERDB_OTLP_ENDPOINT".into(), "https://collector".into()),
            ("COPPERDB_OTLP_PROTOCOL".into(), "http".into()),
            ("COPPERDB_OTLP_TIMEOUT_MS".into(), "250".into()),
            ("COPPERDB_TRACE_SAMPLE_RATIO".into(), "2.5".into()),
            ("COPPERDB_TRACE_PARENT_MODE".into(), "capped".into()),
            ("COPPERDB_TRACE_PARENT_MAX_QPS".into(), "0".into()),
            ("COPPERDB_PPROF_ENABLED".into(), "true".into()),
            ("COPPERDB_PPROF_LISTEN".into(), "127.0.0.1:9191".into()),
            ("COPPERDB_TELEMETRY_PORT".into(), "9190".into()),
        ]);
        config.apply_env_map(&env);
        assert!(config.tracing.enabled);
        assert!(!config.metrics.enabled);
        assert_eq!(config.tracing.endpoint, "https://collector");
        assert_eq!(config.tracing.protocol, "http");
        assert_eq!(config.tracing.timeout, Duration::from_millis(250));
        assert_eq!(config.tracing.sample_ratio, 1.0);
        assert_eq!(config.tracing.parent_mode, "capped");
        assert_eq!(config.tracing.parent_max_qps, 1);
        assert!(config.pprof.enabled);
        assert_eq!(config.pprof.listen, "127.0.0.1:9191");
        assert_eq!(config.metrics.listen, "127.0.0.1:9190");
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

    #[tokio::test]
    async fn health_registry_reports_required_failures_only_for_overall_status() {
        let health = Health::new();
        health.register("storage", true, || Ok(()));
        health.register("downstream", false, || Err("offline".into()));
        let ready = health.ready().await;
        assert!(ready.ok);
        assert!(ready.checks["storage"].ok);
        assert!(!ready.checks["downstream"].ok);

        health.register("replication", true, || Err("no quorum".into()));
        let ready = health.ready().await;
        assert!(!ready.ok);
        assert_eq!(
            ready.checks["replication"].error.as_deref(),
            Some("no quorum")
        );
    }

    #[tokio::test]
    async fn health_registry_runs_checks_concurrently_and_bounds_each_timeout() {
        let health = Health::new();
        health.register_with_timeout("slow", true, Duration::from_millis(10), || {
            std::thread::sleep(Duration::from_millis(50));
            Ok(())
        });
        health.register_with_timeout("peer", false, Duration::from_millis(100), || {
            std::thread::sleep(Duration::from_millis(50));
            Ok(())
        });

        let started_at = Instant::now();
        let ready = health.ready().await;
        assert!(started_at.elapsed() < Duration::from_millis(90));
        assert!(!ready.ok);
        assert_eq!(
            ready.checks["slow"].error.as_deref(),
            Some("deadline exceeded")
        );
        assert!(ready.checks["peer"].ok);
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
