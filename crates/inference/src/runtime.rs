use std::collections::{BTreeMap, VecDeque};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

use copperdb_util::RequestContext;

use crate::{
    HeimdallReview, HeimdallReviewState, InferenceError, MaterializationPolicy, ReviewActor,
    Suggestion, SuggestionRepository,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderReview {
    pub suggestion_id: String,
    pub approved: bool,
    pub reasoning: String,
    pub relationship_type_override: Option<String>,
    pub output_digest: String,
}

pub trait ReviewProvider: Send + Sync + 'static {
    fn review(
        &self,
        request_context: &RequestContext,
        suggestions: &[Suggestion],
    ) -> Result<Vec<ProviderReview>, InferenceError>;
}

#[derive(Default)]
pub struct ProviderRegistryBuilder {
    providers: BTreeMap<String, Arc<dyn ReviewProvider>>,
}

impl ProviderRegistryBuilder {
    pub fn register(
        &mut self,
        id: &str,
        provider: Arc<dyn ReviewProvider>,
    ) -> Result<(), InferenceError> {
        let id = normalize_id(id)?;
        if self.providers.insert(id.clone(), provider).is_some() {
            return Err(InferenceError::DuplicateProvider(id));
        }
        Ok(())
    }

    pub fn build(self) -> ProviderRegistry {
        ProviderRegistry {
            providers: self.providers,
        }
    }
}

pub struct ProviderRegistry {
    providers: BTreeMap<String, Arc<dyn ReviewProvider>>,
}

impl ProviderRegistry {
    fn get(&self, id: &str) -> Result<Arc<dyn ReviewProvider>, InferenceError> {
        let id = normalize_id(id)?;
        self.providers
            .get(&id)
            .cloned()
            .ok_or(InferenceError::UnknownProvider(id))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerConfig {
    pub provider_id: String,
    pub provider_name: String,
    pub provider_version: String,
    pub policy_id: String,
    pub policy_version: String,
    pub queue_capacity: usize,
    pub notification_capacity: usize,
    pub provider_timeout: Duration,
    pub retry_limit: u32,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            provider_id: "heimdall".into(),
            provider_name: "heimdall".into(),
            provider_version: "1".into(),
            policy_id: "auto-links".into(),
            policy_version: "1".into(),
            queue_capacity: 1_000,
            notification_capacity: 256,
            provider_timeout: Duration::from_secs(5),
            retry_limit: 2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationKind {
    Created,
    Reviewed,
    Materialized,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InferenceNotification {
    pub kind: NotificationKind,
    pub suggestion_id: String,
}

pub struct SuggestionScheduler {
    repository: Arc<SuggestionRepository>,
    providers: Arc<ProviderRegistry>,
    config: SchedulerConfig,
    queue: Mutex<VecDeque<String>>,
    attempts: Mutex<BTreeMap<String, u32>>,
    notifications: Mutex<VecDeque<InferenceNotification>>,
    provider_in_flight: Arc<AtomicBool>,
}

impl SuggestionScheduler {
    pub fn new(
        repository: Arc<SuggestionRepository>,
        providers: Arc<ProviderRegistry>,
        config: SchedulerConfig,
    ) -> Self {
        Self {
            repository,
            providers,
            config,
            queue: Mutex::new(VecDeque::new()),
            attempts: Mutex::new(BTreeMap::new()),
            notifications: Mutex::new(VecDeque::new()),
            provider_in_flight: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn enqueue(&self, suggestion_id: impl Into<String>) -> Result<(), InferenceError> {
        let suggestion_id = suggestion_id.into();
        let mut queue = self.queue.lock().expect("inference queue lock");
        if queue.len() >= self.config.queue_capacity {
            return Err(InferenceError::QueueFull);
        }
        if !queue.contains(&suggestion_id) {
            queue.push_back(suggestion_id.clone());
            drop(queue);
            self.notify(NotificationKind::Created, suggestion_id);
        }
        Ok(())
    }

    pub fn recover_pending(&self) -> Result<usize, InferenceError> {
        let pending = self.repository.list_pending(self.config.queue_capacity)?;
        let mut recovered = 0;
        for suggestion in pending {
            self.enqueue(suggestion.id)?;
            recovered += 1;
        }
        Ok(recovered)
    }

    pub fn run_next(
        &self,
        request_context: &RequestContext,
    ) -> Result<Option<Suggestion>, InferenceError> {
        request_context
            .check_active()
            .map_err(|_| InferenceError::RequestCancelled)?;
        let Some(suggestion_id) = self.queue.lock().expect("inference queue lock").pop_front()
        else {
            return Ok(None);
        };
        let suggestion = self
            .repository
            .get(&suggestion_id)?
            .ok_or_else(|| InferenceError::SuggestionNotFound(suggestion_id.clone()))?;
        let provider = self.providers.get(&self.config.provider_id)?;
        if self
            .provider_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            self.queue
                .lock()
                .expect("inference queue lock")
                .push_front(suggestion_id);
            return Err(InferenceError::ProviderFailure(
                "provider worker is still busy".into(),
            ));
        }
        let (context, context_guard) =
            request_context.child(SystemTime::now().checked_add(self.config.provider_timeout));
        let cancellation = context.cancellation().clone();
        let submitted = suggestion.clone();
        let (sender, receiver) = mpsc::sync_channel(1);
        let provider_in_flight = Arc::clone(&self.provider_in_flight);
        thread::spawn(move || {
            let _context_guard = context_guard;
            let result = catch_unwind(AssertUnwindSafe(|| provider.review(&context, &[submitted])))
                .map_err(|_| InferenceError::ProviderFailure("provider panicked".into()))
                .and_then(|result| result);
            let _ = sender.send(result);
            provider_in_flight.store(false, Ordering::Release);
        });
        let received = receiver.recv_timeout(self.config.provider_timeout);
        if received.is_err() {
            cancellation.cancel();
        }
        let result = received
            .map_err(|_| InferenceError::ProviderTimeout)
            .and_then(|result| result)
            .and_then(|reviews| {
                if reviews.len() != 1 || reviews[0].suggestion_id != suggestion_id {
                    return Err(InferenceError::ProviderFailure(
                        "provider output did not exactly match submitted suggestions".into(),
                    ));
                }
                Ok(reviews
                    .into_iter()
                    .next()
                    .expect("one review was validated"))
            });
        match result {
            Ok(review) => {
                self.attempts
                    .lock()
                    .expect("inference attempts lock")
                    .remove(&suggestion_id);
                let reviewed = self.repository.record_heimdall_review(HeimdallReview {
                    id: format!("{}:{}", self.config.provider_id, suggestion.revision),
                    suggestion_id: suggestion_id.clone(),
                    state: if review.approved {
                        HeimdallReviewState::Approved
                    } else {
                        HeimdallReviewState::Rejected
                    },
                    model_provider: self.config.provider_id.clone(),
                    model_id: self.config.provider_name.clone(),
                    model_version: self.config.provider_version.clone(),
                    policy_id: self.config.policy_id.clone(),
                    policy_version: self.config.policy_version.clone(),
                    input_digest: suggestion.latest_provenance.input_digest.clone(),
                    output_digest: review.output_digest,
                    reasoning: review.reasoning,
                    relationship_type_override: review.relationship_type_override,
                    reviewed_at_unix_ms: now_unix_ms(),
                })?;
                self.notify(NotificationKind::Reviewed, suggestion_id);
                Ok(Some(reviewed))
            }
            Err(error) => {
                let mut attempts = self.attempts.lock().expect("inference attempts lock");
                let count = attempts.entry(suggestion_id.clone()).or_default();
                *count += 1;
                if *count <= self.config.retry_limit {
                    drop(attempts);
                    self.enqueue(suggestion_id)?;
                } else {
                    attempts.remove(&suggestion_id);
                    drop(attempts);
                    self.repository.record_heimdall_review(HeimdallReview {
                        id: format!("{}:{}:failed", self.config.provider_id, suggestion.revision),
                        suggestion_id: suggestion_id.clone(),
                        state: HeimdallReviewState::Failed,
                        model_provider: self.config.provider_id.clone(),
                        model_id: self.config.provider_name.clone(),
                        model_version: self.config.provider_version.clone(),
                        policy_id: self.config.policy_id.clone(),
                        policy_version: self.config.policy_version.clone(),
                        input_digest: suggestion.latest_provenance.input_digest.clone(),
                        output_digest: String::new(),
                        reasoning: error.to_string(),
                        relationship_type_override: None,
                        reviewed_at_unix_ms: now_unix_ms(),
                    })?;
                    self.notify(NotificationKind::Failed, suggestion_id);
                }
                Err(error)
            }
        }
    }

    pub fn drain_notifications(&self) -> Vec<InferenceNotification> {
        self.notifications
            .lock()
            .expect("inference notification lock")
            .drain(..)
            .collect()
    }

    pub fn materialize(
        &self,
        suggestion_id: &str,
        policy: MaterializationPolicy,
        actor: &ReviewActor,
    ) -> Result<Suggestion, InferenceError> {
        let suggestion = self.repository.materialize(suggestion_id, policy, actor)?;
        self.notify(NotificationKind::Materialized, suggestion_id.into());
        Ok(suggestion)
    }

    fn notify(&self, kind: NotificationKind, suggestion_id: String) {
        let mut notifications = self
            .notifications
            .lock()
            .expect("inference notification lock");
        if notifications.len() < self.config.notification_capacity {
            notifications.push_back(InferenceNotification {
                kind,
                suggestion_id,
            });
        }
    }
}

fn normalize_id(id: &str) -> Result<String, InferenceError> {
    let id = id.trim().to_ascii_lowercase();
    if id.is_empty() {
        Err(InferenceError::InvalidInput("empty provider id".into()))
    } else {
        Ok(id)
    }
}

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Evidence, Provenance};
    use copperdb_audit::{AuditConfig, AuditLog};
    use copperdb_storage::StorageEngine;
    use std::collections::BTreeMap;

    struct Approver;

    struct SlowProvider;

    struct ExtraOutputProvider;

    struct CancellationProvider {
        started: Arc<std::sync::Barrier>,
    }

    impl ReviewProvider for Approver {
        fn review(
            &self,
            _request_context: &RequestContext,
            suggestions: &[Suggestion],
        ) -> Result<Vec<ProviderReview>, InferenceError> {
            Ok(vec![ProviderReview {
                suggestion_id: suggestions[0].id.clone(),
                approved: true,
                reasoning: "evidence accepted".into(),
                relationship_type_override: None,
                output_digest: "output".into(),
            }])
        }
    }

    impl ReviewProvider for SlowProvider {
        fn review(
            &self,
            _request_context: &RequestContext,
            _suggestions: &[Suggestion],
        ) -> Result<Vec<ProviderReview>, InferenceError> {
            thread::sleep(Duration::from_millis(25));
            Ok(Vec::new())
        }
    }

    impl ReviewProvider for ExtraOutputProvider {
        fn review(
            &self,
            _request_context: &RequestContext,
            suggestions: &[Suggestion],
        ) -> Result<Vec<ProviderReview>, InferenceError> {
            Ok(vec![
                ProviderReview {
                    suggestion_id: suggestions[0].id.clone(),
                    approved: true,
                    reasoning: "valid".into(),
                    relationship_type_override: None,
                    output_digest: "one".into(),
                },
                ProviderReview {
                    suggestion_id: "injected".into(),
                    approved: true,
                    reasoning: "invalid extra".into(),
                    relationship_type_override: None,
                    output_digest: "two".into(),
                },
            ])
        }
    }

    impl ReviewProvider for CancellationProvider {
        fn review(
            &self,
            request_context: &RequestContext,
            _suggestions: &[Suggestion],
        ) -> Result<Vec<ProviderReview>, InferenceError> {
            self.started.wait();
            loop {
                if request_context.check_active().is_err() {
                    return Err(InferenceError::RequestCancelled);
                }
                thread::yield_now();
            }
        }
    }

    fn pending_repository() -> (Arc<SuggestionRepository>, String) {
        let storage = Arc::new(StorageEngine::open_temporary().unwrap());
        let audit = Arc::new(AuditLog::new(Arc::clone(&storage), AuditConfig::default()).unwrap());
        let repository = Arc::new(SuggestionRepository::new(storage, audit));
        let now = now_unix_ms();
        let mut id = String::new();
        for index in 0..3 {
            id = repository
                .record_evidence(Evidence {
                    id: format!("e{index}"),
                    database: "copperdb".into(),
                    source_id: "a".into(),
                    target_id: "b".into(),
                    relationship_type: "RELATES_TO".into(),
                    signal: "similarity".into(),
                    score: 0.8,
                    session_id: format!("s{}", index.min(1)),
                    request_id: None,
                    observed_at_unix_ms: now + index,
                    reason: "similarity".into(),
                    provenance: Provenance {
                        algorithm: "similarity".into(),
                        algorithm_version: "1".into(),
                        input_digest: "input".into(),
                        ..Provenance::default()
                    },
                    metadata: BTreeMap::new(),
                })
                .unwrap()
                .id;
        }
        (repository, id)
    }

    #[test]
    fn registry_is_immutable_normalized_and_rejects_duplicates() {
        let mut builder = ProviderRegistryBuilder::default();
        builder.register(" Heimdall ", Arc::new(Approver)).unwrap();
        assert!(matches!(
            builder.register("heimdall", Arc::new(Approver)),
            Err(InferenceError::DuplicateProvider(_))
        ));
        let registry = builder.build();
        assert!(registry.get("HEIMDALL").is_ok());
        assert!(matches!(
            registry.get("missing"),
            Err(InferenceError::UnknownProvider(_))
        ));
    }

    #[test]
    fn bounded_scheduler_reviews_and_notifies_fifo() {
        let (repository, suggestion_id) = pending_repository();
        let mut builder = ProviderRegistryBuilder::default();
        builder.register("heimdall", Arc::new(Approver)).unwrap();
        let scheduler = SuggestionScheduler::new(
            repository,
            Arc::new(builder.build()),
            SchedulerConfig {
                queue_capacity: 1,
                ..SchedulerConfig::default()
            },
        );
        scheduler.enqueue(suggestion_id.clone()).unwrap();
        assert!(matches!(
            scheduler.enqueue("other"),
            Err(InferenceError::QueueFull)
        ));
        let reviewed = scheduler
            .run_next(&RequestContext::detached())
            .unwrap()
            .unwrap();
        assert_eq!(reviewed.heimdall_review, HeimdallReviewState::Approved);
        assert_eq!(
            scheduler
                .drain_notifications()
                .into_iter()
                .map(|event| event.kind)
                .collect::<Vec<_>>(),
            vec![NotificationKind::Created, NotificationKind::Reviewed]
        );
    }

    #[test]
    fn scheduler_recovers_pending_and_times_out_fail_closed() {
        let (repository, suggestion_id) = pending_repository();
        let mut builder = ProviderRegistryBuilder::default();
        builder
            .register("heimdall", Arc::new(SlowProvider))
            .unwrap();
        let scheduler = SuggestionScheduler::new(
            Arc::clone(&repository),
            Arc::new(builder.build()),
            SchedulerConfig {
                provider_timeout: Duration::from_millis(1),
                retry_limit: 0,
                ..SchedulerConfig::default()
            },
        );
        assert_eq!(scheduler.recover_pending().unwrap(), 1);
        assert!(matches!(
            scheduler.run_next(&RequestContext::detached()),
            Err(InferenceError::ProviderTimeout)
        ));
        let suggestion = repository.get(&suggestion_id).unwrap().unwrap();
        assert_eq!(suggestion.status, crate::SuggestionStatus::PendingReview);
        assert_eq!(suggestion.heimdall_review, HeimdallReviewState::Failed);
        assert_eq!(scheduler.recover_pending().unwrap(), 0);
        assert!(repository.storage().all_edges().unwrap().is_empty());
    }

    #[test]
    fn scheduler_rejects_provider_membership_broadening() {
        let (repository, suggestion_id) = pending_repository();
        let mut builder = ProviderRegistryBuilder::default();
        builder
            .register("heimdall", Arc::new(ExtraOutputProvider))
            .unwrap();
        let scheduler = SuggestionScheduler::new(
            repository,
            Arc::new(builder.build()),
            SchedulerConfig {
                retry_limit: 0,
                ..SchedulerConfig::default()
            },
        );
        scheduler.enqueue(suggestion_id).unwrap();
        assert!(matches!(
            scheduler.run_next(&RequestContext::detached()),
            Err(InferenceError::ProviderFailure(_))
        ));
    }

    #[test]
    fn scheduler_propagates_parent_cancellation_to_provider() {
        let (repository, suggestion_id) = pending_repository();
        let started = Arc::new(std::sync::Barrier::new(2));
        let mut builder = ProviderRegistryBuilder::default();
        builder
            .register(
                "heimdall",
                Arc::new(CancellationProvider {
                    started: Arc::clone(&started),
                }),
            )
            .unwrap();
        let scheduler = Arc::new(SuggestionScheduler::new(
            repository,
            Arc::new(builder.build()),
            SchedulerConfig {
                provider_timeout: Duration::from_secs(1),
                retry_limit: 0,
                ..SchedulerConfig::default()
            },
        ));
        scheduler.enqueue(suggestion_id).unwrap();
        let (context, _guard) = RequestContext::root(None);
        let worker_context = context.clone();
        let worker = {
            let scheduler = Arc::clone(&scheduler);
            thread::spawn(move || scheduler.run_next(&worker_context))
        };
        started.wait();
        context.cancel();

        assert!(matches!(
            worker.join().unwrap(),
            Err(InferenceError::RequestCancelled)
        ));
    }
}
