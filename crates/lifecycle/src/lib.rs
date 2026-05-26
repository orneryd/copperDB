//! Component lifecycle supervision.
//!
//! This ports NornicDB's startup/shutdown contract: start components in forward
//! registration order, stop on the first component error or cancellation, and
//! drain every component in reverse order with a fresh shutdown budget.

use async_trait::async_trait;
use std::{fmt, future::Future, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Error)]
pub enum LifecycleError {
    #[error("component {component} failed during start: {source}")]
    Start {
        component: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("component {component} failed during shutdown: {source}")]
    Shutdown {
        component: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("component task failed to join: {0}")]
    Join(String),
    #[error("supervisor was cancelled")]
    Cancelled,
}

#[derive(Debug, Default, Error)]
#[error("lifecycle completed with errors")]
pub struct LifecycleErrors(pub Vec<LifecycleError>);

impl LifecycleErrors {
    pub fn push(&mut self, error: LifecycleError) {
        self.0.push(error);
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[async_trait]
pub trait Component: Send + Sync + fmt::Debug + 'static {
    fn name(&self) -> &str;
    async fn start(&self, token: CancellationToken) -> Result<(), BoxError>;
    async fn shutdown(&self) -> Result<(), BoxError>;
}

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type DynComponent = Arc<dyn Component>;

#[derive(Debug, Default)]
pub struct Supervisor {
    components: Vec<DynComponent>,
    shutdown_timeout: Duration,
}

impl Supervisor {
    pub fn new() -> Self {
        Self {
            components: Vec::new(),
            shutdown_timeout: SHUTDOWN_TIMEOUT,
        }
    }

    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    pub fn register<C>(&mut self, component: C)
    where
        C: Component,
    {
        self.components.push(Arc::new(component));
    }

    pub fn components(&self) -> &[DynComponent] {
        &self.components
    }

    pub async fn run_until<F>(&self, stop: F) -> Result<(), LifecycleErrors>
    where
        F: Future<Output = ()> + Send,
    {
        let token = CancellationToken::new();
        let mut starts = JoinSet::new();

        for component in &self.components {
            let component = Arc::clone(component);
            let child_token = token.child_token();
            starts.spawn(async move {
                let component_name = component.name().to_string();
                component
                    .start(child_token)
                    .await
                    .map_err(|source| LifecycleError::Start {
                        component: component_name,
                        source,
                    })
            });
        }

        tokio::pin!(stop);
        let mut errors = LifecycleErrors::default();

        tokio::select! {
            _ = &mut stop => {
                token.cancel();
            }
            join = starts.join_next() => {
                token.cancel();
                match join {
                    Some(Ok(Err(error))) => errors.push(error),
                    Some(Err(error)) => errors.push(LifecycleError::Join(error.to_string())),
                    Some(Ok(Ok(()))) | None => {}
                }
            }
        }

        while let Some(join) = starts.join_next().await {
            match join {
                Ok(Ok(())) => {}
                Ok(Err(error)) => errors.push(error),
                Err(error) => errors.push(LifecycleError::Join(error.to_string())),
            }
        }

        if let Err(mut shutdown_errors) =
            drain_reverse(&self.components, self.shutdown_timeout).await
        {
            errors.0.append(&mut shutdown_errors.0);
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

pub async fn drain_reverse(
    components: &[DynComponent],
    shutdown_timeout: Duration,
) -> Result<(), LifecycleErrors> {
    let mut errors = LifecycleErrors::default();

    for component in components.iter().rev() {
        let component_name = component.name().to_string();
        match tokio::time::timeout(shutdown_timeout, component.shutdown()).await {
            Ok(Ok(())) => {}
            Ok(Err(source)) => errors.push(LifecycleError::Shutdown {
                component: component_name,
                source,
            }),
            Err(source) => errors.push(LifecycleError::Shutdown {
                component: component_name,
                source: Box::new(source),
            }),
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use std::sync::{Arc, Mutex};

    #[derive(Debug)]
    struct RecordingComponent {
        name: &'static str,
        events: Arc<Mutex<Vec<String>>>,
        fail_start: bool,
        fail_shutdown: bool,
    }

    impl RecordingComponent {
        fn new(name: &'static str, events: Arc<Mutex<Vec<String>>>) -> Self {
            Self {
                name,
                events,
                fail_start: false,
                fail_shutdown: false,
            }
        }
    }

    #[async_trait]
    impl Component for RecordingComponent {
        fn name(&self) -> &str {
            self.name
        }

        async fn start(&self, token: CancellationToken) -> Result<(), BoxError> {
            self.events
                .lock()
                .unwrap()
                .push(format!("start:{}", self.name));
            if self.fail_start {
                return Err(anyhow!("boom").into());
            }
            token.cancelled().await;
            Ok(())
        }

        async fn shutdown(&self) -> Result<(), BoxError> {
            self.events
                .lock()
                .unwrap()
                .push(format!("shutdown:{}", self.name));
            if self.fail_shutdown {
                return Err(anyhow!("shutdown boom").into());
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn supervisor_drains_in_reverse_order_after_stop() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut supervisor = Supervisor::new();
        supervisor.register(RecordingComponent::new("one", events.clone()));
        supervisor.register(RecordingComponent::new("two", events.clone()));

        supervisor.run_until(async {}).await.unwrap();

        let events = events.lock().unwrap().clone();
        assert!(events.contains(&"start:one".to_string()));
        assert!(events.contains(&"start:two".to_string()));
        assert_eq!(
            &events[events.len() - 2..],
            ["shutdown:two".to_string(), "shutdown:one".to_string()]
        );
    }

    #[tokio::test]
    async fn supervisor_reports_start_and_shutdown_errors() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut supervisor = Supervisor::new();
        supervisor.register(RecordingComponent::new("healthy", events.clone()));
        let mut failing = RecordingComponent::new("failing", events);
        failing.fail_start = true;
        failing.fail_shutdown = true;
        supervisor.register(failing);

        let errors = supervisor
            .run_until(std::future::pending())
            .await
            .unwrap_err();
        assert!(errors
            .0
            .iter()
            .any(|error| matches!(error, LifecycleError::Start { component, .. } if component == "failing")));
        assert!(errors
            .0
            .iter()
            .any(|error| matches!(error, LifecycleError::Shutdown { component, .. } if component == "failing")));
    }
}
