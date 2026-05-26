//! NornicDB observability catalog port for copperdb.

mod catalog;

pub use catalog::{
    EnumSpec, MetricSpec, ENUM_CATALOG, METRIC_CATALOG, NORNICDB_MAIN_REF,
    NORNICDB_OBSERVABILITY_PATH,
};

use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;

use thiserror::Error;

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

    pub fn mock_unimplemented_catalog_metrics(&self) {
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
}
