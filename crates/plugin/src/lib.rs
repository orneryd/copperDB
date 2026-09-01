//! Versioned, statically linked package contract for CopperDB extensions.

mod action;
mod event;

pub use action::{
    ActionCallContext, ActionDescriptor, ActionError, ActionHandler, ActionQueryResult,
    ActionQueryService, ActionRegistry, ActionRegistryBuilder, ActionRegistryError,
};
pub use event::{
    DatabaseEvent, DatabaseEventFuture, DatabaseEventHandler, DatabaseEventHookDescriptor,
    DatabaseEventRuntime, DatabaseEventType, EventHookMetrics, EVENT_HOOK_CAPACITY,
    EVENT_INGRESS_CAPACITY,
};

use async_trait::async_trait;
use copperdb_eval::{
    ProcedureDescriptor, ProcedureRegistry, ProcedureRegistryBuilder, ProcedureRegistryError,
};
use copperdb_filter::{
    FunctionDescriptor, FunctionRegistry, FunctionRegistryBuilder, FunctionRegistryError,
};
use futures::FutureExt;
use semver::{Version, VersionReq};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub const HOST_API_VERSION: &str = "1.0.0";
pub const DEFAULT_LIFECYCLE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageCapability {
    QueryRead,
    QueryWrite,
    Schema,
    DbmsAdmin,
    FileImport,
    FileExport,
    Network,
    Metrics,
    Audit,
    Events,
    ModelInvocation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageDependency {
    package_id: String,
    version: VersionReq,
}

impl PackageDependency {
    pub fn new(package_id: impl Into<String>, version: VersionReq) -> Self {
        Self {
            package_id: package_id.into(),
            version,
        }
    }

    pub fn package_id(&self) -> &str {
        &self.package_id
    }

    pub fn version(&self) -> &VersionReq {
        &self.version
    }
}

#[derive(Debug, Clone)]
pub struct PackageDescriptor {
    id: String,
    version: Version,
    provider: String,
    host_api: VersionReq,
    dependencies: Vec<PackageDependency>,
    requested_capabilities: BTreeSet<PackageCapability>,
    configuration_schema: Value,
}

impl PackageDescriptor {
    pub fn new(id: impl Into<String>, version: Version, provider: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            version,
            provider: provider.into(),
            host_api: VersionReq::parse("^1.0").expect("static host API requirement is valid"),
            dependencies: Vec::new(),
            requested_capabilities: BTreeSet::new(),
            configuration_schema: Value::Object(Default::default()),
        }
    }

    pub fn with_host_api(mut self, host_api: VersionReq) -> Self {
        self.host_api = host_api;
        self
    }

    pub fn with_dependency(mut self, dependency: PackageDependency) -> Self {
        self.dependencies.push(dependency);
        self
    }

    pub fn requesting(mut self, capabilities: impl IntoIterator<Item = PackageCapability>) -> Self {
        self.requested_capabilities.extend(capabilities);
        self
    }

    pub fn with_configuration_schema(mut self, schema: Value) -> Self {
        self.configuration_schema = schema;
        self
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn version(&self) -> &Version {
        &self.version
    }

    pub fn provider(&self) -> &str {
        &self.provider
    }

    pub fn host_api(&self) -> &VersionReq {
        &self.host_api
    }

    pub fn dependencies(&self) -> &[PackageDependency] {
        &self.dependencies
    }

    pub fn requested_capabilities(&self) -> &BTreeSet<PackageCapability> {
        &self.requested_capabilities
    }

    pub fn configuration_schema(&self) -> &Value {
        &self.configuration_schema
    }
}

#[derive(Debug, Clone)]
pub struct PackageDefinition {
    descriptor: PackageDescriptor,
    functions: Vec<FunctionDescriptor>,
    procedures: Vec<ProcedureDescriptor>,
    actions: Vec<ActionDescriptor>,
    event_hooks: Vec<DatabaseEventHookDescriptor>,
}

impl PackageDefinition {
    pub fn new(descriptor: PackageDescriptor) -> Self {
        Self {
            descriptor,
            functions: Vec::new(),
            procedures: Vec::new(),
            actions: Vec::new(),
            event_hooks: Vec::new(),
        }
    }

    pub fn with_function(mut self, function: FunctionDescriptor) -> Self {
        self.functions.push(function);
        self
    }

    pub fn with_procedure(mut self, procedure: ProcedureDescriptor) -> Self {
        self.procedures.push(procedure);
        self
    }

    pub fn with_action(mut self, action: ActionDescriptor) -> Self {
        self.actions.push(action);
        self
    }

    pub fn with_event_hook(mut self, hook: DatabaseEventHookDescriptor) -> Self {
        self.event_hooks.push(hook);
        self
    }

    pub fn descriptor(&self) -> &PackageDescriptor {
        &self.descriptor
    }

    pub fn functions(&self) -> &[FunctionDescriptor] {
        &self.functions
    }

    pub fn procedures(&self) -> &[ProcedureDescriptor] {
        &self.procedures
    }

    pub fn actions(&self) -> &[ActionDescriptor] {
        &self.actions
    }

    pub fn event_hooks(&self) -> &[DatabaseEventHookDescriptor] {
        &self.event_hooks
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedPackage {
    pub id: String,
    pub version: Version,
    pub provider: String,
    pub requested_capabilities: BTreeSet<PackageCapability>,
}

#[derive(Debug, Clone)]
pub struct ResolvedPackageSet {
    packages: Arc<[LoadedPackage]>,
    function_registry: Arc<FunctionRegistry>,
    procedure_registry: Arc<ProcedureRegistry>,
    action_registry: Arc<ActionRegistry>,
    event_hooks: Arc<[DatabaseEventHookDescriptor]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageStatus {
    Uninitialized,
    Initializing,
    Ready,
    Running,
    Stopping,
    Stopped,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageHealth {
    pub status: PackageStatus,
    pub healthy: bool,
    pub message: Option<String>,
    pub last_check_unix_ms: u64,
}

impl PackageHealth {
    pub fn new(status: PackageStatus, healthy: bool) -> Self {
        Self {
            status,
            healthy,
            message: None,
            last_check_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{code}")]
pub struct PackageInstanceError {
    pub code: String,
}

impl PackageInstanceError {
    pub fn new(code: impl Into<String>) -> Self {
        Self { code: code.into() }
    }
}

#[derive(Debug, Clone)]
pub struct PackageHostContext {
    configuration: Value,
    granted_capabilities: BTreeSet<PackageCapability>,
}

impl PackageHostContext {
    pub fn new(configuration: Value, granted_capabilities: BTreeSet<PackageCapability>) -> Self {
        Self {
            configuration,
            granted_capabilities,
        }
    }

    pub fn configuration(&self) -> &Value {
        &self.configuration
    }

    pub fn granted_capabilities(&self) -> &BTreeSet<PackageCapability> {
        &self.granted_capabilities
    }
}

#[derive(Debug, Clone)]
pub struct PackageLifecycleContext {
    cancellation: CancellationToken,
}

impl PackageLifecycleContext {
    fn new(cancellation: CancellationToken) -> Self {
        Self { cancellation }
    }

    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
}

#[async_trait]
pub trait PackageInstance: Send + Sync + fmt::Debug + 'static {
    async fn initialize(
        &self,
        _context: PackageLifecycleContext,
    ) -> Result<(), PackageInstanceError> {
        Ok(())
    }

    async fn start(&self, _context: PackageLifecycleContext) -> Result<(), PackageInstanceError> {
        Ok(())
    }

    async fn stop(&self, _context: PackageLifecycleContext) -> Result<(), PackageInstanceError> {
        Ok(())
    }

    async fn shutdown(
        &self,
        _context: PackageLifecycleContext,
    ) -> Result<(), PackageInstanceError> {
        Ok(())
    }

    fn status(&self) -> PackageStatus;

    fn health(&self) -> PackageHealth {
        let status = self.status();
        PackageHealth::new(
            status,
            matches!(status, PackageStatus::Ready | PackageStatus::Running),
        )
    }
}

pub trait PackageFactory: Send + Sync + fmt::Debug + 'static {
    fn definition(&self) -> PackageDefinition;

    fn create(
        &self,
        host: PackageHostContext,
    ) -> Result<Arc<dyn PackageInstance>, PackageInstanceError>;
}

#[derive(Debug, Clone)]
pub struct StaticPackageFactory {
    definition: PackageDefinition,
}

impl StaticPackageFactory {
    pub fn new(definition: PackageDefinition) -> Self {
        Self { definition }
    }
}

impl PackageFactory for StaticPackageFactory {
    fn definition(&self) -> PackageDefinition {
        self.definition.clone()
    }

    fn create(
        &self,
        _host: PackageHostContext,
    ) -> Result<Arc<dyn PackageInstance>, PackageInstanceError> {
        Ok(Arc::new(StaticPackageInstance::default()))
    }
}

#[derive(Debug, Default)]
struct StaticPackageInstance {
    status: AtomicU8,
}

#[async_trait]
impl PackageInstance for StaticPackageInstance {
    async fn initialize(
        &self,
        _context: PackageLifecycleContext,
    ) -> Result<(), PackageInstanceError> {
        self.status
            .store(status_value(PackageStatus::Ready), Ordering::Release);
        Ok(())
    }

    async fn start(&self, _context: PackageLifecycleContext) -> Result<(), PackageInstanceError> {
        self.status
            .store(status_value(PackageStatus::Running), Ordering::Release);
        Ok(())
    }

    async fn stop(&self, _context: PackageLifecycleContext) -> Result<(), PackageInstanceError> {
        self.status
            .store(status_value(PackageStatus::Stopped), Ordering::Release);
        Ok(())
    }

    fn status(&self) -> PackageStatus {
        status_from_value(self.status.load(Ordering::Acquire))
    }
}

fn status_value(status: PackageStatus) -> u8 {
    status as u8
}

fn status_from_value(value: u8) -> PackageStatus {
    match value {
        value if value == status_value(PackageStatus::Initializing) => PackageStatus::Initializing,
        value if value == status_value(PackageStatus::Ready) => PackageStatus::Ready,
        value if value == status_value(PackageStatus::Running) => PackageStatus::Running,
        value if value == status_value(PackageStatus::Stopping) => PackageStatus::Stopping,
        value if value == status_value(PackageStatus::Stopped) => PackageStatus::Stopped,
        value if value == status_value(PackageStatus::Error) => PackageStatus::Error,
        _ => PackageStatus::Uninitialized,
    }
}

#[derive(Debug, Clone)]
pub struct PackageSpec {
    pub id: String,
    pub required: bool,
    pub configuration: Value,
    pub granted_capabilities: BTreeSet<PackageCapability>,
}

impl PackageSpec {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            required: false,
            configuration: Value::Object(Default::default()),
            granted_capabilities: BTreeSet::new(),
        }
    }

    pub fn required(mut self, required: bool) -> Self {
        self.required = required;
        self
    }

    pub fn with_configuration(mut self, configuration: Value) -> Self {
        self.configuration = configuration;
        self
    }

    pub fn granting(mut self, capabilities: impl IntoIterator<Item = PackageCapability>) -> Self {
        self.granted_capabilities.extend(capabilities);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageLifecycleStage {
    Create,
    Initialize,
    Start,
    Dependency,
    Stop,
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageFailure {
    pub package_id: String,
    pub stage: PackageLifecycleStage,
    pub code: String,
}

#[derive(Debug, Error)]
pub enum PackageRuntimeError {
    #[error(transparent)]
    Load(#[from] PackageLoadError),
    #[error("unknown configured package: {package_id}")]
    UnknownPackage { package_id: String },
    #[error("duplicate configured package: {package_id}")]
    DuplicateSpec { package_id: String },
    #[error("package {package_id} was not granted capability {capability:?}")]
    MissingCapability {
        package_id: String,
        capability: PackageCapability,
    },
    #[error("required package {failure:?} failed")]
    RequiredFailure { failure: PackageFailure },
    #[error("package shutdown completed with failures: {failures:?}")]
    Shutdown { failures: Vec<PackageFailure> },
}

#[derive(Debug)]
struct RunningPackage {
    id: String,
    instance: Arc<dyn PackageInstance>,
}

#[derive(Debug)]
pub struct PackageRuntime {
    packages: Arc<ResolvedPackageSet>,
    running: Mutex<Vec<RunningPackage>>,
    failures: Arc<[PackageFailure]>,
    lifecycle_timeout: Duration,
    stopped: AtomicBool,
    events: DatabaseEventRuntime,
}

impl PackageRuntime {
    pub async fn start(
        factories: impl IntoIterator<Item = Arc<dyn PackageFactory>>,
        specs: impl IntoIterator<Item = PackageSpec>,
        lifecycle_timeout: Duration,
    ) -> Result<Self, PackageRuntimeError> {
        let factories = factories.into_iter().collect::<Vec<_>>();
        let specs = specs.into_iter().collect::<Vec<_>>();
        let mut factory_by_id = BTreeMap::new();
        let mut definitions = Vec::with_capacity(factories.len());
        for (index, factory) in factories.iter().enumerate() {
            let definition = factory.definition();
            let id = definition.descriptor().id().to_string();
            if factory_by_id.insert(id.clone(), index).is_some() {
                return Err(PackageRuntimeError::Load(
                    PackageLoadError::DuplicatePackage { package_id: id },
                ));
            }
            definitions.push(definition);
        }

        let mut spec_by_id = BTreeMap::new();
        for spec in specs {
            if !factory_by_id.contains_key(&spec.id) {
                return Err(PackageRuntimeError::UnknownPackage {
                    package_id: spec.id,
                });
            }
            let package_id = spec.id.clone();
            if spec_by_id.insert(package_id.clone(), spec).is_some() {
                return Err(PackageRuntimeError::DuplicateSpec { package_id });
            }
        }

        let enabled_definitions = definitions
            .iter()
            .filter(|definition| spec_by_id.contains_key(definition.descriptor().id()))
            .cloned()
            .collect::<Vec<_>>();
        let validated = resolve_packages(enabled_definitions.clone())?;
        let definition_by_id = enabled_definitions
            .into_iter()
            .map(|definition| (definition.descriptor().id().to_string(), definition))
            .collect::<BTreeMap<_, _>>();

        for package in validated.packages() {
            let definition = definition_by_id
                .get(&package.id)
                .expect("validated package definition is present");
            let spec = spec_by_id
                .get(&package.id)
                .expect("validated package specification is present");
            for capability in definition.descriptor().requested_capabilities() {
                if !spec.granted_capabilities.contains(capability) {
                    return Err(PackageRuntimeError::MissingCapability {
                        package_id: package.id.clone(),
                        capability: *capability,
                    });
                }
            }
        }

        let mut running = Vec::new();
        let mut successful_definitions = Vec::new();
        let mut failed_ids = BTreeSet::new();
        let mut failures = Vec::new();

        for package in validated.packages() {
            let definition = definition_by_id
                .get(&package.id)
                .expect("validated package definition is present");
            let spec = spec_by_id
                .get(&package.id)
                .expect("validated package specification is present");
            if definition
                .descriptor()
                .dependencies()
                .iter()
                .any(|dependency| failed_ids.contains(dependency.package_id()))
            {
                let failure = PackageFailure {
                    package_id: package.id.clone(),
                    stage: PackageLifecycleStage::Dependency,
                    code: "dependency_unavailable".into(),
                };
                failed_ids.insert(package.id.clone());
                if spec.required {
                    drain_running(&running, lifecycle_timeout).await;
                    return Err(PackageRuntimeError::RequiredFailure { failure });
                }
                failures.push(failure);
                continue;
            }

            let factory = &factories[*factory_by_id
                .get(&package.id)
                .expect("validated package factory is present")];
            let host = PackageHostContext::new(
                spec.configuration.clone(),
                spec.granted_capabilities.clone(),
            );
            let instance = match std::panic::catch_unwind(AssertUnwindSafe(|| factory.create(host)))
            {
                Ok(Ok(instance)) => instance,
                Ok(Err(error)) => {
                    let failure =
                        package_failure(&package.id, PackageLifecycleStage::Create, error.code);
                    failed_ids.insert(package.id.clone());
                    if spec.required {
                        drain_running(&running, lifecycle_timeout).await;
                        return Err(PackageRuntimeError::RequiredFailure { failure });
                    }
                    failures.push(failure);
                    continue;
                }
                Err(_) => {
                    let failure = package_failure(
                        &package.id,
                        PackageLifecycleStage::Create,
                        "package_panic",
                    );
                    failed_ids.insert(package.id.clone());
                    if spec.required {
                        drain_running(&running, lifecycle_timeout).await;
                        return Err(PackageRuntimeError::RequiredFailure { failure });
                    }
                    failures.push(failure);
                    continue;
                }
            };

            if let Err(failure) = supervise_call(
                &package.id,
                PackageLifecycleStage::Initialize,
                lifecycle_timeout,
                |context| instance.initialize(context),
            )
            .await
            {
                drain_instance(&package.id, &instance, lifecycle_timeout).await;
                failed_ids.insert(package.id.clone());
                if spec.required {
                    drain_running(&running, lifecycle_timeout).await;
                    return Err(PackageRuntimeError::RequiredFailure { failure });
                }
                failures.push(failure);
                continue;
            }
            if let Err(failure) = supervise_call(
                &package.id,
                PackageLifecycleStage::Start,
                lifecycle_timeout,
                |context| instance.start(context),
            )
            .await
            {
                drain_instance(&package.id, &instance, lifecycle_timeout).await;
                failed_ids.insert(package.id.clone());
                if spec.required {
                    drain_running(&running, lifecycle_timeout).await;
                    return Err(PackageRuntimeError::RequiredFailure { failure });
                }
                failures.push(failure);
                continue;
            }
            successful_definitions.push(definition.clone());
            running.push(RunningPackage {
                id: package.id.clone(),
                instance,
            });
        }

        let packages = Arc::new(resolve_packages(successful_definitions)?);
        let events = DatabaseEventRuntime::start(packages.event_hooks(), lifecycle_timeout);
        Ok(Self {
            packages,
            running: Mutex::new(running),
            failures: failures.into(),
            lifecycle_timeout,
            stopped: AtomicBool::new(false),
            events,
        })
    }

    pub fn packages(&self) -> Arc<ResolvedPackageSet> {
        Arc::clone(&self.packages)
    }

    pub fn failures(&self) -> &[PackageFailure] {
        &self.failures
    }

    pub async fn health(&self) -> BTreeMap<String, PackageHealth> {
        self.running
            .lock()
            .await
            .iter()
            .map(|package| (package.id.clone(), package.instance.health()))
            .collect()
    }

    pub fn emit_event(&self, event: DatabaseEvent) -> bool {
        self.events.emit(event)
    }

    pub fn event_metrics(&self) -> BTreeMap<String, EventHookMetrics> {
        self.events.metrics()
    }

    pub async fn shutdown(&self) -> Result<(), PackageRuntimeError> {
        if self.stopped.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.events.shutdown().await;
        let mut running = self.running.lock().await;
        let failures = drain_running_collect(&running, self.lifecycle_timeout).await;
        running.clear();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(PackageRuntimeError::Shutdown { failures })
        }
    }
}

fn package_failure(
    package_id: &str,
    stage: PackageLifecycleStage,
    code: impl Into<String>,
) -> PackageFailure {
    PackageFailure {
        package_id: package_id.to_string(),
        stage,
        code: code.into(),
    }
}

async fn supervise_call<Call, CallFuture>(
    package_id: &str,
    stage: PackageLifecycleStage,
    timeout: Duration,
    call: Call,
) -> Result<(), PackageFailure>
where
    Call: FnOnce(PackageLifecycleContext) -> CallFuture,
    CallFuture: Future<Output = Result<(), PackageInstanceError>>,
{
    let cancellation = CancellationToken::new();
    let future = call(PackageLifecycleContext::new(cancellation.clone()));
    let result = match tokio::time::timeout(timeout, AssertUnwindSafe(future).catch_unwind()).await
    {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(package_failure(package_id, stage, error.code)),
        Ok(Err(_)) => Err(package_failure(package_id, stage, "package_panic")),
        Err(_) => Err(package_failure(package_id, stage, "package_timeout")),
    };
    cancellation.cancel();
    result
}

async fn drain_instance(
    package_id: &str,
    instance: &Arc<dyn PackageInstance>,
    timeout: Duration,
) -> Vec<PackageFailure> {
    let mut failures = Vec::new();
    for (stage, result) in [
        (
            PackageLifecycleStage::Stop,
            supervise_call(
                package_id,
                PackageLifecycleStage::Stop,
                timeout,
                |context| instance.stop(context),
            )
            .await,
        ),
        (
            PackageLifecycleStage::Shutdown,
            supervise_call(
                package_id,
                PackageLifecycleStage::Shutdown,
                timeout,
                |context| instance.shutdown(context),
            )
            .await,
        ),
    ] {
        if let Err(mut failure) = result {
            failure.stage = stage;
            failures.push(failure);
        }
    }
    failures
}

async fn drain_running(running: &[RunningPackage], timeout: Duration) {
    let _ = drain_running_collect(running, timeout).await;
}

async fn drain_running_collect(
    running: &[RunningPackage],
    timeout: Duration,
) -> Vec<PackageFailure> {
    let mut failures = Vec::new();
    for package in running.iter().rev() {
        failures.extend(drain_instance(&package.id, &package.instance, timeout).await);
    }
    failures
}

impl ResolvedPackageSet {
    pub fn packages(&self) -> &[LoadedPackage] {
        &self.packages
    }

    pub fn function_registry(&self) -> Arc<FunctionRegistry> {
        Arc::clone(&self.function_registry)
    }

    pub fn procedure_registry(&self) -> Arc<ProcedureRegistry> {
        Arc::clone(&self.procedure_registry)
    }

    pub fn action_registry(&self) -> Arc<ActionRegistry> {
        Arc::clone(&self.action_registry)
    }

    pub fn event_hooks(&self) -> &[DatabaseEventHookDescriptor] {
        &self.event_hooks
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PackageLoadError {
    #[error("invalid package ID: {package_id}")]
    InvalidPackageId { package_id: String },
    #[error("package provider must not be empty: {package_id}")]
    EmptyProvider { package_id: String },
    #[error("duplicate package ID: {package_id}")]
    DuplicatePackage { package_id: String },
    #[error("package {package_id} is incompatible with host API {host_api}; requires {required}")]
    IncompatibleHost {
        package_id: String,
        host_api: Version,
        required: VersionReq,
    },
    #[error("package {package_id} requires missing package {dependency_id}")]
    MissingDependency {
        package_id: String,
        dependency_id: String,
    },
    #[error(
        "package {package_id} requires {dependency_id} {required}, but version {actual} is loaded"
    )]
    DependencyVersion {
        package_id: String,
        dependency_id: String,
        required: VersionReq,
        actual: Version,
    },
    #[error("package dependency cycle: {packages:?}")]
    DependencyCycle { packages: Vec<String> },
    #[error("package {package_id} function registration failed: {source}")]
    FunctionRegistration {
        package_id: String,
        source: FunctionRegistryError,
    },
    #[error("package {package_id} procedure registration failed: {source}")]
    ProcedureRegistration {
        package_id: String,
        source: ProcedureRegistryError,
    },
    #[error("package {package_id} action registration failed: {source}")]
    ActionRegistration {
        package_id: String,
        source: ActionRegistryError,
    },
}

pub fn resolve_packages(
    definitions: impl IntoIterator<Item = PackageDefinition>,
) -> Result<ResolvedPackageSet, PackageLoadError> {
    let definitions = definitions.into_iter().collect::<Vec<_>>();
    let host_api = Version::parse(HOST_API_VERSION).expect("static host API version is valid");
    let mut by_id = BTreeMap::new();

    for (index, definition) in definitions.iter().enumerate() {
        let descriptor = definition.descriptor();
        validate_package_id(descriptor.id())?;
        if descriptor.provider().trim().is_empty() {
            return Err(PackageLoadError::EmptyProvider {
                package_id: descriptor.id().to_string(),
            });
        }
        if !descriptor.host_api().matches(&host_api) {
            return Err(PackageLoadError::IncompatibleHost {
                package_id: descriptor.id().to_string(),
                host_api: host_api.clone(),
                required: descriptor.host_api().clone(),
            });
        }
        if by_id.insert(descriptor.id().to_string(), index).is_some() {
            return Err(PackageLoadError::DuplicatePackage {
                package_id: descriptor.id().to_string(),
            });
        }
    }

    let order = dependency_order(&definitions, &by_id)?;
    let mut function_builder = FunctionRegistryBuilder::with_builtins();
    let mut procedure_builder = ProcedureRegistryBuilder::with_builtins();
    let mut action_builder = ActionRegistryBuilder::default();
    let mut event_hooks = Vec::new();
    let mut packages = Vec::with_capacity(order.len());

    for index in order {
        let definition = &definitions[index];
        let descriptor = definition.descriptor();
        for function in definition.functions() {
            function_builder
                .register(function.clone().attributed_to(descriptor.id()))
                .map_err(|source| PackageLoadError::FunctionRegistration {
                    package_id: descriptor.id().to_string(),
                    source,
                })?;
        }
        for procedure in definition.procedures() {
            procedure_builder
                .register(procedure.clone().attributed_to(descriptor.id()))
                .map_err(|source| PackageLoadError::ProcedureRegistration {
                    package_id: descriptor.id().to_string(),
                    source,
                })?;
        }
        for action in definition.actions() {
            action_builder
                .register(action.clone().attributed_to(descriptor.id()))
                .map_err(|source| PackageLoadError::ActionRegistration {
                    package_id: descriptor.id().to_string(),
                    source,
                })?;
        }
        event_hooks.extend(
            definition
                .event_hooks()
                .iter()
                .cloned()
                .map(|hook| hook.attributed_to(descriptor.id())),
        );
        packages.push(LoadedPackage {
            id: descriptor.id().to_string(),
            version: descriptor.version().clone(),
            provider: descriptor.provider().to_string(),
            requested_capabilities: descriptor.requested_capabilities().clone(),
        });
    }

    Ok(ResolvedPackageSet {
        packages: packages.into(),
        function_registry: Arc::new(function_builder.build()),
        procedure_registry: Arc::new(procedure_builder.build()),
        action_registry: Arc::new(action_builder.build()),
        event_hooks: event_hooks.into(),
    })
}

fn validate_package_id(package_id: &str) -> Result<(), PackageLoadError> {
    let valid = !package_id.is_empty()
        && package_id.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || (index > 0 && matches!(byte, b'.' | b'_' | b'-'))
        });
    if valid {
        Ok(())
    } else {
        Err(PackageLoadError::InvalidPackageId {
            package_id: package_id.to_string(),
        })
    }
}

fn dependency_order(
    definitions: &[PackageDefinition],
    by_id: &BTreeMap<String, usize>,
) -> Result<Vec<usize>, PackageLoadError> {
    let mut incoming = definitions
        .iter()
        .map(|definition| (definition.descriptor().id().to_string(), 0usize))
        .collect::<BTreeMap<_, _>>();
    let mut dependents = BTreeMap::<String, Vec<String>>::new();

    for definition in definitions {
        let descriptor = definition.descriptor();
        let mut seen = BTreeSet::new();
        for dependency in descriptor.dependencies() {
            if !seen.insert(dependency.package_id()) {
                continue;
            }
            let Some(dependency_index) = by_id.get(dependency.package_id()) else {
                return Err(PackageLoadError::MissingDependency {
                    package_id: descriptor.id().to_string(),
                    dependency_id: dependency.package_id().to_string(),
                });
            };
            let dependency_descriptor = definitions[*dependency_index].descriptor();
            if !dependency
                .version()
                .matches(dependency_descriptor.version())
            {
                return Err(PackageLoadError::DependencyVersion {
                    package_id: descriptor.id().to_string(),
                    dependency_id: dependency.package_id().to_string(),
                    required: dependency.version().clone(),
                    actual: dependency_descriptor.version().clone(),
                });
            }
            *incoming
                .get_mut(descriptor.id())
                .expect("validated package is present") += 1;
            dependents
                .entry(dependency.package_id().to_string())
                .or_default()
                .push(descriptor.id().to_string());
        }
    }

    let mut ready = incoming
        .iter()
        .filter_map(|(id, count)| (*count == 0).then_some(id.clone()))
        .collect::<BTreeSet<_>>();
    let mut order = Vec::with_capacity(definitions.len());
    while let Some(id) = ready.pop_first() {
        order.push(*by_id.get(&id).expect("ready package is present"));
        if let Some(children) = dependents.get(&id) {
            for child in children {
                let count = incoming
                    .get_mut(child)
                    .expect("dependent package is present");
                *count -= 1;
                if *count == 0 {
                    ready.insert(child.clone());
                }
            }
        }
    }
    if order.len() != definitions.len() {
        return Err(PackageLoadError::DependencyCycle {
            packages: incoming
                .into_iter()
                .filter_map(|(id, count)| (count > 0).then_some(id))
                .collect(),
        });
    }
    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;
    use copperdb_eval::{ProcedureMode, ProcedureOutput};
    use serde_json::json;
    use std::sync::Mutex as StdMutex;

    fn descriptor(id: &str, version: &str) -> PackageDescriptor {
        PackageDescriptor::new(
            id,
            Version::parse(version).unwrap(),
            "copperdb-test-provider",
        )
    }

    fn function(name: &str) -> FunctionDescriptor {
        FunctionDescriptor::extension(
            name,
            std::iter::empty::<&str>(),
            format!("{name}() :: STRING"),
            "Test package function",
            "Testing",
            Arc::new(|_, _| Ok(json!("ok"))),
        )
    }

    fn procedure(name: &str) -> ProcedureDescriptor {
        ProcedureDescriptor::extension(
            name,
            std::iter::empty::<&str>(),
            format!("{name}() :: (value :: STRING)"),
            "Test package procedure",
            ProcedureMode::Read,
            Arc::new(|_, _| Ok(ProcedureOutput::default())),
        )
    }

    #[test]
    fn resolves_dependencies_in_stable_order_and_builds_registries() {
        let consumer =
            PackageDefinition::new(descriptor("example.consumer", "2.0.0").with_dependency(
                PackageDependency::new("example.base", VersionReq::parse("^1.0").unwrap()),
            ))
            .with_procedure(procedure("example.consumer.run"));
        let base = PackageDefinition::new(descriptor("example.base", "1.4.0"))
            .with_function(function("example.base.value"));

        let resolved = resolve_packages([consumer, base]).unwrap();

        assert_eq!(
            resolved
                .packages()
                .iter()
                .map(|package| package.id.as_str())
                .collect::<Vec<_>>(),
            vec!["example.base", "example.consumer"]
        );
        assert!(resolved
            .function_registry()
            .get("example.base.value")
            .is_some());
        assert!(resolved
            .procedure_registry()
            .get("example.consumer.run")
            .is_some());
    }

    #[test]
    fn rejects_duplicate_and_invalid_package_ids() {
        let duplicate = resolve_packages([
            PackageDefinition::new(descriptor("example.same", "1.0.0")),
            PackageDefinition::new(descriptor("example.same", "2.0.0")),
        ])
        .unwrap_err();
        assert_eq!(
            duplicate,
            PackageLoadError::DuplicatePackage {
                package_id: "example.same".into()
            }
        );

        let invalid = resolve_packages([PackageDefinition::new(descriptor(
            "Example/Unsafe",
            "1.0.0",
        ))])
        .unwrap_err();
        assert_eq!(
            invalid,
            PackageLoadError::InvalidPackageId {
                package_id: "Example/Unsafe".into()
            }
        );
    }

    #[test]
    fn rejects_missing_mismatched_and_cyclic_dependencies() {
        let missing = PackageDefinition::new(
            descriptor("example.consumer", "1.0.0")
                .with_dependency(PackageDependency::new("example.missing", VersionReq::STAR)),
        );
        assert!(matches!(
            resolve_packages([missing]),
            Err(PackageLoadError::MissingDependency { .. })
        ));

        let mismatched =
            PackageDefinition::new(descriptor("example.consumer", "1.0.0").with_dependency(
                PackageDependency::new("example.base", VersionReq::parse("^2.0").unwrap()),
            ));
        assert!(matches!(
            resolve_packages([
                PackageDefinition::new(descriptor("example.base", "1.0.0")),
                mismatched,
            ]),
            Err(PackageLoadError::DependencyVersion { .. })
        ));

        let left = PackageDefinition::new(
            descriptor("example.left", "1.0.0")
                .with_dependency(PackageDependency::new("example.right", VersionReq::STAR)),
        );
        let right = PackageDefinition::new(
            descriptor("example.right", "1.0.0")
                .with_dependency(PackageDependency::new("example.left", VersionReq::STAR)),
        );
        assert_eq!(
            resolve_packages([right, left]).unwrap_err(),
            PackageLoadError::DependencyCycle {
                packages: vec!["example.left".into(), "example.right".into()]
            }
        );
    }

    #[test]
    fn rejects_incompatible_host_and_descriptor_collisions_transactionally() {
        let incompatible = PackageDefinition::new(
            descriptor("example.future", "1.0.0")
                .with_host_api(VersionReq::parse(">=2.0").unwrap()),
        );
        assert!(matches!(
            resolve_packages([incompatible]),
            Err(PackageLoadError::IncompatibleHost { .. })
        ));

        let first = PackageDefinition::new(descriptor("example.first", "1.0.0"))
            .with_function(function("example.collision"));
        let second = PackageDefinition::new(descriptor("example.second", "1.0.0"))
            .with_function(function("EXAMPLE.COLLISION"));
        let error = resolve_packages([second, first]).unwrap_err();
        assert!(matches!(
            error,
            PackageLoadError::FunctionRegistration {
                package_id,
                source: FunctionRegistryError::NameCollision { .. },
            } if package_id == "example.second"
        ));
    }

    #[derive(Debug)]
    struct RecordingFactory {
        definition: PackageDefinition,
        instance: Arc<RecordingInstance>,
        create_error: Option<&'static str>,
    }

    impl RecordingFactory {
        fn new(
            definition: PackageDefinition,
            events: Arc<StdMutex<Vec<String>>>,
        ) -> (Arc<Self>, Arc<RecordingInstance>) {
            let id = definition.descriptor().id().to_string();
            let instance = Arc::new(RecordingInstance {
                id,
                events,
                fail_stage: StdMutex::new(None),
                status: AtomicU8::new(status_value(PackageStatus::Uninitialized)),
            });
            (
                Arc::new(Self {
                    definition,
                    instance: Arc::clone(&instance),
                    create_error: None,
                }),
                instance,
            )
        }
    }

    impl PackageFactory for RecordingFactory {
        fn definition(&self) -> PackageDefinition {
            self.definition.clone()
        }

        fn create(
            &self,
            host: PackageHostContext,
        ) -> Result<Arc<dyn PackageInstance>, PackageInstanceError> {
            self.instance.events.lock().unwrap().push(format!(
                "create:{}:{}:{}",
                self.instance.id,
                host.configuration(),
                host.granted_capabilities().len()
            ));
            if let Some(code) = self.create_error {
                return Err(PackageInstanceError::new(code));
            }
            Ok(self.instance.clone())
        }
    }

    #[derive(Debug)]
    struct RecordingInstance {
        id: String,
        events: Arc<StdMutex<Vec<String>>>,
        fail_stage: StdMutex<Option<PackageLifecycleStage>>,
        status: AtomicU8,
    }

    impl RecordingInstance {
        fn record(&self, stage: &str) {
            self.events
                .lock()
                .unwrap()
                .push(format!("{stage}:{}", self.id));
        }

        fn stage_result(&self, stage: PackageLifecycleStage) -> Result<(), PackageInstanceError> {
            if *self.fail_stage.lock().unwrap() == Some(stage) {
                Err(PackageInstanceError::new(format!(
                    "{}_failed",
                    format!("{stage:?}").to_lowercase()
                )))
            } else {
                Ok(())
            }
        }

        fn fail_at(&self, stage: PackageLifecycleStage) {
            *self.fail_stage.lock().unwrap() = Some(stage);
        }
    }

    #[async_trait]
    impl PackageInstance for RecordingInstance {
        async fn initialize(
            &self,
            _context: PackageLifecycleContext,
        ) -> Result<(), PackageInstanceError> {
            self.record("initialize");
            self.stage_result(PackageLifecycleStage::Initialize)?;
            self.status
                .store(status_value(PackageStatus::Ready), Ordering::Release);
            Ok(())
        }

        async fn start(
            &self,
            _context: PackageLifecycleContext,
        ) -> Result<(), PackageInstanceError> {
            self.record("start");
            self.stage_result(PackageLifecycleStage::Start)?;
            self.status
                .store(status_value(PackageStatus::Running), Ordering::Release);
            Ok(())
        }

        async fn stop(
            &self,
            _context: PackageLifecycleContext,
        ) -> Result<(), PackageInstanceError> {
            self.record("stop");
            self.stage_result(PackageLifecycleStage::Stop)?;
            self.status
                .store(status_value(PackageStatus::Stopped), Ordering::Release);
            Ok(())
        }

        async fn shutdown(
            &self,
            _context: PackageLifecycleContext,
        ) -> Result<(), PackageInstanceError> {
            self.record("shutdown");
            self.stage_result(PackageLifecycleStage::Shutdown)
        }

        fn status(&self) -> PackageStatus {
            status_from_value(self.status.load(Ordering::Acquire))
        }
    }

    fn dyn_factory(factory: Arc<RecordingFactory>) -> Arc<dyn PackageFactory> {
        factory
    }

    #[tokio::test]
    async fn lifecycle_starts_in_dependency_order_and_drains_in_reverse() {
        let events = Arc::new(StdMutex::new(Vec::new()));
        let base_definition = PackageDefinition::new(descriptor("example.base", "1.0.0"));
        let consumer_definition = PackageDefinition::new(
            descriptor("example.consumer", "1.0.0")
                .with_dependency(PackageDependency::new("example.base", VersionReq::STAR)),
        );
        let (base, _) = RecordingFactory::new(base_definition, Arc::clone(&events));
        let (consumer, _) = RecordingFactory::new(consumer_definition, Arc::clone(&events));

        let runtime = PackageRuntime::start(
            [dyn_factory(consumer), dyn_factory(base)],
            [
                PackageSpec::new("example.consumer").with_configuration(json!({"mode": "test"})),
                PackageSpec::new("example.base"),
            ],
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        assert_eq!(
            runtime
                .packages()
                .packages()
                .iter()
                .map(|package| package.id.as_str())
                .collect::<Vec<_>>(),
            ["example.base", "example.consumer"]
        );
        assert!(runtime.failures().is_empty());
        assert_eq!(
            runtime
                .health()
                .await
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["example.base", "example.consumer"]
        );
        runtime.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();

        assert_eq!(
            *events.lock().unwrap(),
            [
                "create:example.base:{}:0",
                "initialize:example.base",
                "start:example.base",
                "create:example.consumer:{\"mode\":\"test\"}:0",
                "initialize:example.consumer",
                "start:example.consumer",
                "stop:example.consumer",
                "shutdown:example.consumer",
                "stop:example.base",
                "shutdown:example.base",
            ]
        );
    }

    #[tokio::test]
    async fn optional_failure_excludes_dependents_but_keeps_unrelated_packages() {
        let events = Arc::new(StdMutex::new(Vec::new()));
        let (base, base_instance) = RecordingFactory::new(
            PackageDefinition::new(descriptor("example.base", "1.0.0")),
            Arc::clone(&events),
        );
        base_instance.fail_at(PackageLifecycleStage::Start);
        let (dependent, _) = RecordingFactory::new(
            PackageDefinition::new(
                descriptor("example.dependent", "1.0.0")
                    .with_dependency(PackageDependency::new("example.base", VersionReq::STAR)),
            ),
            Arc::clone(&events),
        );
        let (unrelated, _) = RecordingFactory::new(
            PackageDefinition::new(descriptor("example.unrelated", "1.0.0")),
            Arc::clone(&events),
        );

        let runtime = PackageRuntime::start(
            [
                dyn_factory(base),
                dyn_factory(dependent),
                dyn_factory(unrelated),
            ],
            [
                PackageSpec::new("example.base"),
                PackageSpec::new("example.dependent"),
                PackageSpec::new("example.unrelated"),
            ],
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        assert_eq!(
            runtime
                .packages()
                .packages()
                .iter()
                .map(|package| package.id.as_str())
                .collect::<Vec<_>>(),
            ["example.unrelated"]
        );
        assert_eq!(
            runtime.failures(),
            [
                PackageFailure {
                    package_id: "example.base".into(),
                    stage: PackageLifecycleStage::Start,
                    code: "start_failed".into(),
                },
                PackageFailure {
                    package_id: "example.dependent".into(),
                    stage: PackageLifecycleStage::Dependency,
                    code: "dependency_unavailable".into(),
                },
            ]
        );
        runtime.shutdown().await.unwrap();
        assert!(!events
            .lock()
            .unwrap()
            .iter()
            .any(|event| event.contains("create:example.dependent")));
    }

    #[tokio::test]
    async fn required_start_failure_rolls_back_started_packages() {
        let events = Arc::new(StdMutex::new(Vec::new()));
        let (base, _) = RecordingFactory::new(
            PackageDefinition::new(descriptor("example.base", "1.0.0")),
            Arc::clone(&events),
        );
        let (required, required_instance) = RecordingFactory::new(
            PackageDefinition::new(descriptor("example.required", "1.0.0")),
            Arc::clone(&events),
        );
        required_instance.fail_at(PackageLifecycleStage::Start);

        let error = PackageRuntime::start(
            [dyn_factory(base), dyn_factory(required)],
            [
                PackageSpec::new("example.base"),
                PackageSpec::new("example.required").required(true),
            ],
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            PackageRuntimeError::RequiredFailure {
                failure: PackageFailure {
                    package_id,
                    stage: PackageLifecycleStage::Start,
                    code,
                }
            } if package_id == "example.required" && code == "start_failed"
        ));
        let events = events.lock().unwrap();
        assert!(events.ends_with(&["stop:example.base".into(), "shutdown:example.base".into(),]));
    }

    #[tokio::test]
    async fn missing_capability_fails_before_factory_creation() {
        let events = Arc::new(StdMutex::new(Vec::new()));
        let definition = PackageDefinition::new(
            descriptor("example.network", "1.0.0").requesting([PackageCapability::Network]),
        );
        let (factory, _) = RecordingFactory::new(definition, Arc::clone(&events));

        let error = PackageRuntime::start(
            [dyn_factory(factory)],
            [PackageSpec::new("example.network")],
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            PackageRuntimeError::MissingCapability {
                package_id,
                capability: PackageCapability::Network,
            } if package_id == "example.network"
        ));
        assert!(events.lock().unwrap().is_empty());
    }

    #[derive(Debug)]
    struct BehaviorFactory {
        definition: PackageDefinition,
        behavior: LifecycleBehavior,
    }

    #[derive(Debug, Clone, Copy)]
    enum LifecycleBehavior {
        PanicOnStart,
        BlockOnStart,
    }

    impl PackageFactory for BehaviorFactory {
        fn definition(&self) -> PackageDefinition {
            self.definition.clone()
        }

        fn create(
            &self,
            _host: PackageHostContext,
        ) -> Result<Arc<dyn PackageInstance>, PackageInstanceError> {
            Ok(Arc::new(BehaviorInstance(self.behavior)))
        }
    }

    #[derive(Debug)]
    struct BehaviorInstance(LifecycleBehavior);

    #[async_trait]
    impl PackageInstance for BehaviorInstance {
        async fn start(
            &self,
            _context: PackageLifecycleContext,
        ) -> Result<(), PackageInstanceError> {
            match self.0 {
                LifecycleBehavior::PanicOnStart => panic!("private panic payload"),
                LifecycleBehavior::BlockOnStart => std::future::pending().await,
            }
        }

        fn status(&self) -> PackageStatus {
            PackageStatus::Initializing
        }
    }

    #[tokio::test]
    async fn optional_panic_is_isolated_with_a_stable_public_code() {
        let factory: Arc<dyn PackageFactory> = Arc::new(BehaviorFactory {
            definition: PackageDefinition::new(descriptor("example.panic", "1.0.0")),
            behavior: LifecycleBehavior::PanicOnStart,
        });

        let runtime = PackageRuntime::start(
            [factory],
            [PackageSpec::new("example.panic")],
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        assert!(runtime.packages().packages().is_empty());
        assert_eq!(
            runtime.failures(),
            [PackageFailure {
                package_id: "example.panic".into(),
                stage: PackageLifecycleStage::Start,
                code: "package_panic".into(),
            }]
        );
    }

    #[tokio::test]
    async fn required_hook_timeout_fails_startup_with_a_stable_code() {
        let factory: Arc<dyn PackageFactory> = Arc::new(BehaviorFactory {
            definition: PackageDefinition::new(descriptor("example.blocked", "1.0.0")),
            behavior: LifecycleBehavior::BlockOnStart,
        });

        let error = PackageRuntime::start(
            [factory],
            [PackageSpec::new("example.blocked").required(true)],
            Duration::from_millis(1),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            PackageRuntimeError::RequiredFailure {
                failure: PackageFailure {
                    package_id,
                    stage: PackageLifecycleStage::Start,
                    code,
                }
            } if package_id == "example.blocked" && code == "package_timeout"
        ));
    }
}
