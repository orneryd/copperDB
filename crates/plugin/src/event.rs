use futures::FutureExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub const EVENT_INGRESS_CAPACITY: usize = 1_000;
pub const EVENT_HOOK_CAPACITY: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatabaseEventType {
    #[serde(rename = "node.created")]
    NodeCreated,
    #[serde(rename = "node.updated")]
    NodeUpdated,
    #[serde(rename = "node.deleted")]
    NodeDeleted,
    #[serde(rename = "node.read")]
    NodeRead,
    #[serde(rename = "relationship.created")]
    RelationshipCreated,
    #[serde(rename = "relationship.updated")]
    RelationshipUpdated,
    #[serde(rename = "relationship.deleted")]
    RelationshipDeleted,
    #[serde(rename = "query.executed")]
    QueryExecuted,
    #[serde(rename = "query.failed")]
    QueryFailed,
    #[serde(rename = "index.created")]
    IndexCreated,
    #[serde(rename = "index.dropped")]
    IndexDropped,
    #[serde(rename = "transaction.commit")]
    TransactionCommit,
    #[serde(rename = "transaction.rollback")]
    TransactionRollback,
    #[serde(rename = "database.started")]
    DatabaseStarted,
    #[serde(rename = "database.shutdown")]
    DatabaseShutdown,
    #[serde(rename = "backup.started")]
    BackupStarted,
    #[serde(rename = "backup.completed")]
    BackupCompleted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatabaseEvent {
    #[serde(rename = "type")]
    pub event_type: DatabaseEventType,
    pub timestamp: String,
    pub request_id: String,
    pub node_id: String,
    pub node_labels: Vec<String>,
    pub relationship_id: String,
    pub relationship_type: String,
    pub source_node_id: String,
    pub target_node_id: String,
    pub properties: BTreeMap<String, Value>,
    pub old_properties: BTreeMap<String, Value>,
    pub query: String,
    pub query_params: BTreeMap<String, Value>,
    pub duration: u64,
    pub rows_affected: i64,
    pub error: String,
    pub user_id: String,
    pub source: String,
    pub metadata: BTreeMap<String, Value>,
}

impl DatabaseEvent {
    pub fn new(event_type: DatabaseEventType) -> Self {
        Self {
            event_type,
            timestamp: String::new(),
            request_id: String::new(),
            node_id: String::new(),
            node_labels: Vec::new(),
            relationship_id: String::new(),
            relationship_type: String::new(),
            source_node_id: String::new(),
            target_node_id: String::new(),
            properties: BTreeMap::new(),
            old_properties: BTreeMap::new(),
            query: String::new(),
            query_params: BTreeMap::new(),
            duration: 0,
            rows_affected: 0,
            error: String::new(),
            user_id: String::new(),
            source: String::new(),
            metadata: BTreeMap::new(),
        }
    }
}

pub type DatabaseEventFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>>;
pub type DatabaseEventHandler =
    Arc<dyn Fn(DatabaseEvent) -> DatabaseEventFuture + Send + Sync + 'static>;

#[derive(Clone)]
pub struct DatabaseEventHookDescriptor {
    package_id: Option<String>,
    handler: DatabaseEventHandler,
}

impl fmt::Debug for DatabaseEventHookDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DatabaseEventHookDescriptor")
            .field("package_id", &self.package_id)
            .finish_non_exhaustive()
    }
}

impl DatabaseEventHookDescriptor {
    pub fn new(handler: DatabaseEventHandler) -> Self {
        Self {
            package_id: None,
            handler,
        }
    }

    pub fn attributed_to(mut self, package_id: impl Into<String>) -> Self {
        self.package_id = Some(package_id.into());
        self
    }

    pub fn package_id(&self) -> Option<&str> {
        self.package_id.as_deref()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EventHookMetrics {
    pub enqueued: u64,
    pub delivered: u64,
    pub dropped: u64,
    pub errors: u64,
    pub panics: u64,
    pub timeouts: u64,
}

#[derive(Debug, Default)]
struct EventHookCounters {
    enqueued: AtomicU64,
    delivered: AtomicU64,
    dropped: AtomicU64,
    errors: AtomicU64,
    panics: AtomicU64,
    timeouts: AtomicU64,
}

impl EventHookCounters {
    fn snapshot(&self) -> EventHookMetrics {
        EventHookMetrics {
            enqueued: self.enqueued.load(Ordering::Relaxed),
            delivered: self.delivered.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            panics: self.panics.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
struct HookSender {
    sender: mpsc::Sender<DatabaseEvent>,
    counters: Arc<EventHookCounters>,
}

#[derive(Debug)]
pub struct DatabaseEventRuntime {
    sender: Mutex<Option<mpsc::Sender<DatabaseEvent>>>,
    ingress_dropped: AtomicU64,
    counters: BTreeMap<String, Arc<EventHookCounters>>,
    cancellation: CancellationToken,
    tasks: tokio::sync::Mutex<Vec<JoinHandle<()>>>,
}

impl DatabaseEventRuntime {
    pub fn start(hooks: &[DatabaseEventHookDescriptor], hook_timeout: Duration) -> Self {
        let cancellation = CancellationToken::new();
        let mut hook_senders = Vec::with_capacity(hooks.len());
        let mut counters = BTreeMap::new();
        let mut tasks = Vec::with_capacity(hooks.len() + 1);
        for hook in hooks {
            let package_id = hook.package_id.as_deref().unwrap_or("unknown").to_string();
            let package_counters = Arc::clone(
                counters
                    .entry(package_id)
                    .or_insert_with(|| Arc::new(EventHookCounters::default())),
            );
            let (sender, mut receiver) = mpsc::channel(EVENT_HOOK_CAPACITY);
            hook_senders.push(HookSender {
                sender,
                counters: Arc::clone(&package_counters),
            });
            let handler = Arc::clone(&hook.handler);
            let worker_cancellation = cancellation.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    let event = tokio::select! {
                        _ = worker_cancellation.cancelled() => break,
                        event = receiver.recv() => match event {
                            Some(event) => event,
                            None => break,
                        },
                    };
                    match tokio::time::timeout(
                        hook_timeout,
                        AssertUnwindSafe(handler(event)).catch_unwind(),
                    )
                    .await
                    {
                        Ok(Ok(Ok(()))) => {
                            package_counters.delivered.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(Ok(Err(_))) => {
                            package_counters.errors.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(Err(_)) => {
                            package_counters.panics.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            package_counters.timeouts.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }));
        }
        let (sender, mut receiver) = mpsc::channel::<DatabaseEvent>(EVENT_INGRESS_CAPACITY);
        let dispatcher_cancellation = cancellation.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                let mut event = tokio::select! {
                    _ = dispatcher_cancellation.cancelled() => break,
                    event = receiver.recv() => match event {
                        Some(event) => event,
                        None => break,
                    },
                };
                if event.timestamp.is_empty() {
                    event.timestamp = OffsetDateTime::now_utc()
                        .format(&Rfc3339)
                        .expect("UTC timestamp is RFC3339 representable");
                }
                for hook in &hook_senders {
                    match hook.sender.try_send(event.clone()) {
                        Ok(()) => {
                            hook.counters.enqueued.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            hook.counters.dropped.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {}
                    }
                }
            }
        }));
        Self {
            sender: Mutex::new(Some(sender)),
            ingress_dropped: AtomicU64::new(0),
            counters,
            cancellation,
            tasks: tokio::sync::Mutex::new(tasks),
        }
    }

    pub fn emit(&self, event: DatabaseEvent) -> bool {
        if self.cancellation.is_cancelled() {
            return false;
        }
        let sender = self.sender.lock().unwrap();
        let Some(sender) = sender.as_ref() else {
            return false;
        };
        match sender.try_send(event) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.ingress_dropped.fetch_add(1, Ordering::Relaxed);
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }

    pub fn ingress_dropped(&self) -> u64 {
        self.ingress_dropped.load(Ordering::Relaxed)
    }

    pub fn metrics(&self) -> BTreeMap<String, EventHookMetrics> {
        self.counters
            .iter()
            .map(|(package_id, counters)| (package_id.clone(), counters.snapshot()))
            .collect()
    }

    pub async fn shutdown(&self) {
        self.sender.lock().unwrap().take();
        self.cancellation.cancel();
        for task in self.tasks.lock().await.drain(..) {
            let _ = task.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fills_timestamps_and_preserves_per_hook_fifo_order() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let handler_received = Arc::clone(&received);
        let hook = DatabaseEventHookDescriptor::new(Arc::new(move |event| {
            let handler_received = Arc::clone(&handler_received);
            Box::pin(async move {
                handler_received.lock().unwrap().push(event);
                Ok(())
            })
        }))
        .attributed_to("example.watcher");
        let runtime = DatabaseEventRuntime::start(&[hook], Duration::from_secs(1));
        for sequence in 0..3 {
            let mut event = DatabaseEvent::new(DatabaseEventType::QueryFailed);
            event
                .metadata
                .insert("sequence".into(), Value::from(sequence));
            assert!(runtime.emit(event));
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while received.lock().unwrap().len() < 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        {
            let received = received.lock().unwrap();
            assert!(received.iter().all(|event| !event.timestamp.is_empty()));
            assert_eq!(
                received
                    .iter()
                    .map(|event| event.metadata["sequence"].as_u64().unwrap())
                    .collect::<Vec<_>>(),
                vec![0, 1, 2]
            );
        }
        assert_eq!(runtime.metrics()["example.watcher"].delivered, 3);
        runtime.shutdown().await;
        assert!(!runtime.emit(DatabaseEvent::new(DatabaseEventType::QueryFailed)));
    }

    #[tokio::test]
    async fn isolates_hook_panics_and_continues_delivery() {
        let calls = Arc::new(AtomicU64::new(0));
        let handler_calls = Arc::clone(&calls);
        let hook = DatabaseEventHookDescriptor::new(Arc::new(move |_event| {
            let call = handler_calls.fetch_add(1, Ordering::Relaxed);
            Box::pin(async move {
                assert_ne!(call, 0, "first event panic");
                Ok(())
            })
        }))
        .attributed_to("example.watcher");
        let runtime = DatabaseEventRuntime::start(&[hook], Duration::from_secs(1));
        assert!(runtime.emit(DatabaseEvent::new(DatabaseEventType::QueryFailed)));
        assert!(runtime.emit(DatabaseEvent::new(DatabaseEventType::QueryExecuted)));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let metrics = runtime.metrics()["example.watcher"];
                if metrics.panics == 1 && metrics.delivered == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn times_out_a_hook_and_continues_delivery() {
        let calls = Arc::new(AtomicU64::new(0));
        let handler_calls = Arc::clone(&calls);
        let hook = DatabaseEventHookDescriptor::new(Arc::new(move |_event| {
            let call = handler_calls.fetch_add(1, Ordering::Relaxed);
            Box::pin(async move {
                if call == 0 {
                    std::future::pending().await
                } else {
                    Ok(())
                }
            })
        }))
        .attributed_to("example.watcher");
        let runtime = DatabaseEventRuntime::start(&[hook], Duration::from_millis(1));
        assert!(runtime.emit(DatabaseEvent::new(DatabaseEventType::QueryFailed)));
        assert!(runtime.emit(DatabaseEvent::new(DatabaseEventType::QueryExecuted)));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let metrics = runtime.metrics()["example.watcher"];
                if metrics.timeouts == 1 && metrics.delivered == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        runtime.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn drops_newest_ingress_event_when_saturated() {
        let runtime = DatabaseEventRuntime::start(&[], Duration::from_secs(1));
        for _ in 0..EVENT_INGRESS_CAPACITY {
            assert!(runtime.emit(DatabaseEvent::new(DatabaseEventType::QueryExecuted)));
        }
        assert!(!runtime.emit(DatabaseEvent::new(DatabaseEventType::QueryExecuted)));
        assert_eq!(runtime.ingress_dropped(), 1);
        runtime.shutdown().await;
    }

    #[test]
    fn event_types_use_upstream_dotted_names() {
        assert_eq!(
            serde_json::to_value(DatabaseEventType::QueryFailed).unwrap(),
            Value::String("query.failed".into())
        );
    }
}
