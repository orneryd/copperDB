use crate::{EvalError, QueryStats, Row};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, OnceLock};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcedureMode {
    Read,
    Write,
    Dbms,
}

impl ProcedureMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "READ",
            Self::Write => "WRITE",
            Self::Dbms => "DBMS",
        }
    }
}

pub struct ProcedureCallContext<'a> {
    pub row: &'a Row,
    pub params: &'a HashMap<String, Value>,
    pub capabilities: &'a [String],
    pub caller_roles: &'a [String],
    pub database: Option<&'a str>,
    pub request_context: &'a copperdb_util::RequestContext,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ProcedureOutput {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
}

impl ProcedureOutput {
    pub fn new(columns: Vec<String>, rows: Vec<Row>) -> Self {
        Self { columns, rows }
    }
}

impl From<ProcedureOutput> for crate::EvalResult {
    fn from(output: ProcedureOutput) -> Self {
        Self {
            columns: output.columns,
            rows: output.rows,
            stats: QueryStats::default(),
        }
    }
}

#[derive(Debug, Error)]
pub enum ProcedureError {
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    RequestCancelled(#[from] copperdb_util::RequestCancelled),
}

impl From<ProcedureError> for EvalError {
    fn from(error: ProcedureError) -> Self {
        match error {
            ProcedureError::Message(message) => Self::ExecutionError(message),
            ProcedureError::RequestCancelled(cancelled) => Self::RequestCancelled(cancelled),
        }
    }
}

pub type ProcedureHandler = Arc<
    dyn Fn(&ProcedureCallContext<'_>, &[Value]) -> Result<ProcedureOutput, ProcedureError>
        + Send
        + Sync
        + 'static,
>;

pub type ProcedureRegistrar = Arc<
    dyn Fn(&mut ProcedureRegistryBuilder) -> Result<(), ProcedureRegistryError>
        + Send
        + Sync
        + 'static,
>;

#[derive(Debug, Clone, Copy)]
pub(crate) enum BuiltinProcedure {
    DbLabels,
    DbRelationshipTypes,
    DbPropertyKeys,
    DbConstraints,
    DbIndexes,
    DbPing,
    DbInfo,
    DbSchemaNodeProperties,
    DbSchemaRelProperties,
    DbSchemaVisualization,
    NornicDbVersion,
    NornicDbStats,
    NornicDbDecayInfo,
    NornicDbKnowledgePolicyInfo,
    DbmsProcedures,
    DbmsFunctions,
    DbmsComponents,
    DbmsInfo,
    DbmsListConfig,
    DbmsClientConfig,
    DbmsListConnections,
    FulltextListAnalyzers,
    KnowledgePolicyResolve,
    KnowledgePolicyProfiles,
    KnowledgePolicyPolicies,
    FulltextQueryNodes,
    FulltextQueryRelationships,
    VectorQueryNodes,
    VectorQueryRelationships,
    SetNodeVectorProperty,
    SetRelationshipVectorProperty,
}

#[derive(Clone)]
enum ProcedureImplementation {
    Builtin(BuiltinProcedure),
    Extension(ProcedureHandler),
}

#[derive(Clone)]
pub struct ProcedureDescriptor {
    canonical_name: String,
    display_name: String,
    aliases: Vec<String>,
    signature: String,
    description: String,
    mode: ProcedureMode,
    package_id: Option<String>,
    required_capabilities: Vec<String>,
    required_roles: Vec<String>,
    hidden: bool,
    implementation: ProcedureImplementation,
}

impl fmt::Debug for ProcedureDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProcedureDescriptor")
            .field("canonical_name", &self.canonical_name)
            .field("display_name", &self.display_name)
            .field("aliases", &self.aliases)
            .field("signature", &self.signature)
            .field("description", &self.description)
            .field("mode", &self.mode)
            .field("package_id", &self.package_id)
            .field("required_capabilities", &self.required_capabilities)
            .field("required_roles", &self.required_roles)
            .field("hidden", &self.hidden)
            .finish_non_exhaustive()
    }
}

impl ProcedureDescriptor {
    pub fn extension(
        name: impl Into<String>,
        aliases: impl IntoIterator<Item = impl Into<String>>,
        signature: impl Into<String>,
        description: impl Into<String>,
        mode: ProcedureMode,
        handler: ProcedureHandler,
    ) -> Self {
        let display_name = name.into();
        Self {
            canonical_name: normalize_name(&display_name),
            display_name,
            aliases: aliases.into_iter().map(Into::into).collect(),
            signature: signature.into(),
            description: description.into(),
            mode,
            package_id: None,
            required_capabilities: Vec::new(),
            required_roles: Vec::new(),
            hidden: false,
            implementation: ProcedureImplementation::Extension(handler),
        }
    }

    fn builtin(
        name: &str,
        signature: &str,
        description: &str,
        mode: ProcedureMode,
        implementation: BuiltinProcedure,
    ) -> Self {
        Self {
            canonical_name: normalize_name(name),
            display_name: name.to_string(),
            aliases: Vec::new(),
            signature: signature.to_string(),
            description: description.to_string(),
            mode,
            package_id: None,
            required_capabilities: Vec::new(),
            required_roles: Vec::new(),
            hidden: false,
            implementation: ProcedureImplementation::Builtin(implementation),
        }
    }

    pub fn requiring_capabilities(
        mut self,
        capabilities: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.required_capabilities = capabilities.into_iter().map(Into::into).collect();
        self
    }

    pub fn requiring_roles(mut self, roles: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.required_roles = roles.into_iter().map(Into::into).collect();
        self
    }

    pub fn hidden(mut self) -> Self {
        self.hidden = true;
        self
    }

    pub fn attributed_to(mut self, package_id: impl Into<String>) -> Self {
        self.package_id = Some(package_id.into());
        self
    }

    pub fn canonical_name(&self) -> &str {
        &self.canonical_name
    }

    pub fn name(&self) -> &str {
        &self.display_name
    }

    pub fn aliases(&self) -> &[String] {
        &self.aliases
    }

    pub fn signature(&self) -> &str {
        &self.signature
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn mode(&self) -> ProcedureMode {
        self.mode
    }

    pub fn package_id(&self) -> Option<&str> {
        self.package_id.as_deref()
    }

    pub fn required_capabilities(&self) -> &[String] {
        &self.required_capabilities
    }

    pub fn required_roles(&self) -> &[String] {
        &self.required_roles
    }

    pub fn is_hidden(&self) -> bool {
        self.hidden
    }

    pub(crate) fn builtin_implementation(&self) -> Option<BuiltinProcedure> {
        match self.implementation {
            ProcedureImplementation::Builtin(implementation) => Some(implementation),
            ProcedureImplementation::Extension(_) => None,
        }
    }

    pub(crate) fn extension_handler(&self) -> Option<&ProcedureHandler> {
        match &self.implementation {
            ProcedureImplementation::Builtin(_) => None,
            ProcedureImplementation::Extension(handler) => Some(handler),
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProcedureRegistryError {
    #[error("procedure name or alias collision: {name}")]
    NameCollision { name: String },
}

#[derive(Debug, Default)]
pub struct ProcedureRegistryBuilder {
    descriptors: Vec<ProcedureDescriptor>,
    names: HashMap<String, usize>,
}

impl ProcedureRegistryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_builtins() -> Self {
        let mut builder = Self::new();
        for descriptor in ProcedureRegistry::builtins().descriptors() {
            builder
                .register(descriptor.clone())
                .expect("built-in procedure names must be unique");
        }
        builder
    }

    pub fn register(
        &mut self,
        descriptor: ProcedureDescriptor,
    ) -> Result<&mut Self, ProcedureRegistryError> {
        let index = self.descriptors.len();
        let mut names = Vec::with_capacity(descriptor.aliases.len() + 1);
        names.push(descriptor.canonical_name.clone());
        names.extend(descriptor.aliases.iter().map(|alias| normalize_name(alias)));
        let mut descriptor_names = HashSet::with_capacity(names.len());
        if let Some(name) = names.iter().find(|name| {
            !descriptor_names.insert((*name).clone()) || self.names.contains_key(*name)
        }) {
            return Err(ProcedureRegistryError::NameCollision { name: name.clone() });
        }
        for name in names {
            self.names.insert(name, index);
        }
        self.descriptors.push(descriptor);
        Ok(self)
    }

    pub fn build(self) -> ProcedureRegistry {
        ProcedureRegistry {
            descriptors: self.descriptors.into(),
            names: self.names,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProcedureRegistry {
    descriptors: Arc<[ProcedureDescriptor]>,
    names: HashMap<String, usize>,
}

impl ProcedureRegistry {
    pub fn builtins() -> &'static Self {
        static BUILTINS: OnceLock<ProcedureRegistry> = OnceLock::new();
        BUILTINS.get_or_init(build_builtin_registry)
    }

    pub fn get(&self, name: &str) -> Option<&ProcedureDescriptor> {
        self.names
            .get(&normalize_name(name))
            .map(|index| &self.descriptors[*index])
    }

    pub fn descriptors(&self) -> &[ProcedureDescriptor] {
        &self.descriptors
    }
}

fn normalize_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

fn build_builtin_registry() -> ProcedureRegistry {
    use BuiltinProcedure as B;
    use ProcedureMode::{Dbms, Read, Write};

    let definitions = [
        ("db.constraints", "db.constraints() :: (name :: STRING, type :: STRING, labelsOrTypes :: LIST<STRING>, properties :: LIST<STRING>, propertyType :: STRING)", "Lists all constraints in the database", Read, B::DbConstraints),
        ("db.index.fulltext.listAvailableAnalyzers", "db.index.fulltext.listAvailableAnalyzers() :: (analyzer :: STRING, description :: STRING)", "Lists available fulltext analyzers", Read, B::FulltextListAnalyzers),
        ("db.index.fulltext.queryNodes", "db.index.fulltext.queryNodes(indexName :: STRING, query :: STRING, options = {} :: MAP) :: (node :: NODE, score :: FLOAT)", "Fulltext search on nodes", Read, B::FulltextQueryNodes),
        ("db.index.vector.queryNodes", "db.index.vector.queryNodes(indexName :: STRING, numberOfResults :: INTEGER, query :: LIST<FLOAT>|STRING|$param) :: (node :: NODE, score :: FLOAT)", "Vector search on nodes", Read, B::VectorQueryNodes),
        ("db.index.fulltext.queryRelationships", "db.index.fulltext.queryRelationships(indexName :: STRING, query :: STRING, options = {} :: MAP) :: (relationship :: RELATIONSHIP, score :: FLOAT)", "Fulltext search on relationships", Read, B::FulltextQueryRelationships),
        ("db.index.vector.queryRelationships", "db.index.vector.queryRelationships(indexName :: STRING, numberOfResults :: INTEGER, query :: LIST<FLOAT>|STRING|$param) :: (relationship :: RELATIONSHIP, score :: FLOAT)", "Vector search on relationships", Read, B::VectorQueryRelationships),
        ("db.create.setNodeVectorProperty", "db.create.setNodeVectorProperty(nodeId :: STRING, propertyKey :: STRING, vector :: LIST<FLOAT>)", "Sets vector property on a node", Write, B::SetNodeVectorProperty),
        ("db.create.setRelationshipVectorProperty", "db.create.setRelationshipVectorProperty(relationshipId :: STRING, propertyKey :: STRING, vector :: LIST<FLOAT>)", "Sets vector property on a relationship", Write, B::SetRelationshipVectorProperty),
        ("db.indexes", "db.indexes() :: (name :: STRING, type :: STRING, labelsOrTypes :: LIST<STRING>, properties :: LIST<STRING>, state :: STRING)", "Lists all indexes in the database", Read, B::DbIndexes),
        ("db.info", "db.info() :: (id :: STRING, name :: STRING, creationDate :: STRING, nodeCount :: INTEGER, relationshipCount :: INTEGER)", "Returns database information", Read, B::DbInfo),
        ("db.labels", "db.labels() :: (label :: STRING)", "Lists all labels in the database", Read, B::DbLabels),
        ("db.ping", "db.ping() :: (success :: BOOLEAN)", "Checks database connectivity", Read, B::DbPing),
        ("db.propertyKeys", "db.propertyKeys() :: (propertyKey :: STRING)", "Lists all property keys in the database", Read, B::DbPropertyKeys),
        ("db.relationshipTypes", "db.relationshipTypes() :: (relationshipType :: STRING)", "Lists all relationship types in the database", Read, B::DbRelationshipTypes),
        ("db.schema.nodeProperties", "db.schema.nodeProperties() :: (nodeLabel :: STRING, propertyName :: STRING, propertyType :: STRING)", "Returns node properties by label", Read, B::DbSchemaNodeProperties),
        ("db.schema.relProperties", "db.schema.relProperties() :: (relType :: STRING, propertyName :: STRING, propertyType :: STRING)", "Returns relationship properties by type", Read, B::DbSchemaRelProperties),
        ("db.schema.visualization", "db.schema.visualization() :: (nodes :: LIST<MAP>, relationships :: LIST<MAP>)", "Visualizes schema", Read, B::DbSchemaVisualization),
        ("dbms.clientConfig", "dbms.clientConfig() :: (name :: STRING, value :: ANY)", "Returns client configuration", Dbms, B::DbmsClientConfig),
        ("dbms.components", "dbms.components() :: (name :: STRING, versions :: LIST<STRING>, edition :: STRING)", "Lists DBMS components", Dbms, B::DbmsComponents),
        ("dbms.functions", "dbms.functions() :: (name :: STRING, signature :: STRING, description :: STRING, category :: STRING, package :: STRING)", "Lists functions", Dbms, B::DbmsFunctions),
        ("dbms.procedures", "dbms.procedures() :: (name :: STRING, signature :: STRING, description :: STRING, mode :: STRING, package :: STRING)", "Lists procedures", Dbms, B::DbmsProcedures),
        ("dbms.info", "dbms.info() :: (id :: STRING, name :: STRING, creationDate :: STRING)", "Returns DBMS information", Dbms, B::DbmsInfo),
        ("dbms.listConfig", "dbms.listConfig() :: (name :: STRING, description :: STRING, value :: ANY, dynamic :: BOOLEAN)", "Lists DBMS configuration", Dbms, B::DbmsListConfig),
        ("dbms.listConnections", "dbms.listConnections() :: (connectionId :: STRING, connectTime :: STRING, connector :: STRING, username :: STRING, userAgent :: STRING, clientAddress :: STRING)", "Lists active DBMS connections", Dbms, B::DbmsListConnections),
        ("nornicdb.decay.info", "nornicdb.decay.info() :: (enabled :: BOOLEAN, system :: STRING, configuredVia :: STRING)", "Returns knowledge-layer scoring configuration", Read, B::NornicDbDecayInfo),
        ("nornicdb.knowledgepolicy.info", "nornicdb.knowledgepolicy.info() :: (enabled :: BOOLEAN, system :: STRING, decayProfiles :: INTEGER, decayBindings :: INTEGER, promotionProfiles :: INTEGER, promotionPolicies :: INTEGER, configuredVia :: STRING)", "Returns knowledge-layer profile and policy catalog counts", Read, B::NornicDbKnowledgePolicyInfo),
        ("nornicdb.knowledgepolicy.resolve", "nornicdb.knowledgepolicy.resolve(entityId :: STRING = '', labelsCsv :: STRING = '', edgeType :: STRING = '') :: (entityId :: STRING, targetKind :: STRING, targetLabels :: STRING, targetEdgeType :: STRING, decayBinding :: STRING, promotionPolicy :: STRING, matchedPromotionProfile :: STRING, matchedPromotionPredicate :: STRING, scoreFrom :: STRING, anchorUnixMs :: INTEGER, accessCount :: INTEGER, lastAccessedAtUnixMs :: INTEGER, baseScore :: FLOAT, finalScore :: FLOAT, visibilityThreshold :: FLOAT, suppressed :: BOOLEAN, dryRun :: BOOLEAN, explanation :: STRING)", "Resolves the effective knowledge-layer scoring policy for an entity, label set, or edge type", Read, B::KnowledgePolicyResolve),
        ("nornicdb.knowledgepolicy.policies", "nornicdb.knowledgepolicy.policies() :: (kind :: STRING, Name :: STRING, Scope :: STRING, Multiplier :: FLOAT, ScoreFloor :: FLOAT, ScoreCap :: FLOAT, Enabled :: BOOLEAN, TargetLabels :: LIST<STRING>, TargetEdgeType :: STRING, IsWildcard :: BOOLEAN, IsEdge :: BOOLEAN)", "Returns knowledge-layer promotion profiles and policies", Read, B::KnowledgePolicyPolicies),
        ("nornicdb.knowledgepolicy.profiles", "nornicdb.knowledgepolicy.profiles() :: (kind :: STRING, Name :: STRING, HalfLifeSeconds :: INTEGER, VisibilityThreshold :: FLOAT, ScoreFloor :: FLOAT, Function :: STRING, Scope :: STRING, DecayEnabled :: BOOLEAN, ScoreFrom :: STRING, ScoreFromProperty :: STRING, Enabled :: BOOLEAN, TargetLabels :: LIST<STRING>, TargetEdgeType :: STRING, IsWildcard :: BOOLEAN, IsEdge :: BOOLEAN, ProfileRef :: STRING, NoDecay :: BOOLEAN, Order :: INTEGER)", "Returns knowledge-layer decay bundles and bindings", Read, B::KnowledgePolicyProfiles),
        ("nornicdb.stats", "nornicdb.stats() :: (nodes :: INTEGER, relationships :: INTEGER, labels :: INTEGER, relationshipTypes :: INTEGER)", "Returns NornicDB stats", Read, B::NornicDbStats),
        ("nornicdb.version", "nornicdb.version() :: (version :: STRING, build :: STRING, edition :: STRING)", "Returns NornicDB version", Read, B::NornicDbVersion),
    ];
    let mut builder = ProcedureRegistryBuilder::new();
    for (name, signature, description, mode, implementation) in definitions {
        builder
            .register(ProcedureDescriptor::builtin(
                name,
                signature,
                description,
                mode,
                implementation,
            ))
            .expect("built-in procedure names must be unique");
    }
    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handler() -> ProcedureHandler {
        Arc::new(|_, _| Ok(ProcedureOutput::default()))
    }

    #[test]
    fn lookup_is_case_insensitive() {
        assert_eq!(
            ProcedureRegistry::builtins()
                .get("DB.LABELS")
                .unwrap()
                .name(),
            "db.labels"
        );
    }

    #[test]
    fn registration_rejects_self_and_existing_collisions() {
        let mut builder = ProcedureRegistryBuilder::new();
        let self_collision = builder
            .register(ProcedureDescriptor::extension(
                "example.one",
                ["EXAMPLE.ONE"],
                "",
                "",
                ProcedureMode::Read,
                handler(),
            ))
            .unwrap_err();
        assert_eq!(
            self_collision,
            ProcedureRegistryError::NameCollision {
                name: "example.one".into()
            }
        );
        builder
            .register(ProcedureDescriptor::extension(
                "example.one",
                ["example.alias"],
                "",
                "",
                ProcedureMode::Read,
                handler(),
            ))
            .unwrap();
        let collision = builder
            .register(ProcedureDescriptor::extension(
                "EXAMPLE.ALIAS",
                std::iter::empty::<&str>(),
                "",
                "",
                ProcedureMode::Read,
                handler(),
            ))
            .unwrap_err();
        assert_eq!(
            collision,
            ProcedureRegistryError::NameCollision {
                name: "example.alias".into()
            }
        );
    }

    #[test]
    fn builtin_lookup_loop_is_stable() {
        let registry = ProcedureRegistry::builtins();
        for _ in 0..10_000 {
            assert!(registry.get("DBMS.PROCEDURES").is_some());
            assert!(registry.get("missing.procedure").is_none());
        }
    }
}
