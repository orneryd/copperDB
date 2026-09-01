//! Versioned, statically linked package contract for CopperDB extensions.

use copperdb_eval::{
    ProcedureDescriptor, ProcedureRegistry, ProcedureRegistryBuilder, ProcedureRegistryError,
};
use copperdb_filter::{
    FunctionDescriptor, FunctionRegistry, FunctionRegistryBuilder, FunctionRegistryError,
};
use semver::{Version, VersionReq};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use thiserror::Error;

pub const HOST_API_VERSION: &str = "1.0.0";

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
}

impl PackageDefinition {
    pub fn new(descriptor: PackageDescriptor) -> Self {
        Self {
            descriptor,
            functions: Vec::new(),
            procedures: Vec::new(),
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

    pub fn descriptor(&self) -> &PackageDescriptor {
        &self.descriptor
    }

    pub fn functions(&self) -> &[FunctionDescriptor] {
        &self.functions
    }

    pub fn procedures(&self) -> &[ProcedureDescriptor] {
        &self.procedures
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
}
