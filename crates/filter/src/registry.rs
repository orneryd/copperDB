use super::{FilterError, Row};
use serde_json::Value;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, OnceLock};
use thiserror::Error;

pub struct FunctionCallContext<'a> {
    pub row: &'a Row,
    pub params: &'a HashMap<String, Value>,
    pub capabilities: &'a [String],
    pub caller_roles: &'a [String],
    pub database: Option<&'a str>,
    pub request_context: Option<&'a copperdb_util::RequestContext>,
}

#[derive(Debug, Clone, Default)]
pub struct FunctionExecutionContext {
    pub capabilities: Vec<String>,
    pub caller_roles: Vec<String>,
    pub database: Option<String>,
    pub request_context: Option<copperdb_util::RequestContext>,
}

#[derive(Clone)]
struct InstalledFunctionRegistry {
    registry: Arc<FunctionRegistry>,
    context: FunctionExecutionContext,
}

thread_local! {
    static CURRENT_FUNCTION_REGISTRY: RefCell<Option<InstalledFunctionRegistry>> = const { RefCell::new(None) };
}

pub fn with_function_registry<T>(
    registry: Arc<FunctionRegistry>,
    context: FunctionExecutionContext,
    operation: impl FnOnce() -> T,
) -> T {
    struct RestoreGuard(Option<InstalledFunctionRegistry>);
    impl Drop for RestoreGuard {
        fn drop(&mut self) {
            CURRENT_FUNCTION_REGISTRY.with(|current| {
                current.replace(self.0.take());
            });
        }
    }

    let previous = CURRENT_FUNCTION_REGISTRY
        .with(|current| current.replace(Some(InstalledFunctionRegistry { registry, context })));
    let _guard = RestoreGuard(previous);
    operation()
}

pub(crate) fn with_current_function_registry<T>(
    operation: impl FnOnce(&FunctionRegistry, &FunctionExecutionContext) -> T,
) -> T {
    CURRENT_FUNCTION_REGISTRY.with(|current| {
        let installed = current.borrow();
        if let Some(installed) = installed.as_ref() {
            operation(&installed.registry, &installed.context)
        } else {
            operation(
                FunctionRegistry::builtins(),
                &FunctionExecutionContext::default(),
            )
        }
    })
}

pub type FunctionHandler = Arc<
    dyn Fn(&FunctionCallContext<'_>, &[Value]) -> Result<Value, FilterError>
        + Send
        + Sync
        + 'static,
>;

pub type FunctionRegistrar = Arc<
    dyn Fn(&mut FunctionRegistryBuilder) -> Result<(), FunctionRegistryError>
        + Send
        + Sync
        + 'static,
>;

#[derive(Clone)]
enum FunctionImplementation {
    Builtin,
    Extension(FunctionHandler),
}

#[derive(Clone)]
pub struct FunctionDescriptor {
    canonical_name: String,
    display_name: String,
    aliases: Vec<String>,
    signature: String,
    description: String,
    category: String,
    alias_descriptions: HashMap<String, String>,
    hidden: bool,
    implementation: FunctionImplementation,
}

impl fmt::Debug for FunctionDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FunctionDescriptor")
            .field("canonical_name", &self.canonical_name)
            .field("display_name", &self.display_name)
            .field("aliases", &self.aliases)
            .field("signature", &self.signature)
            .field("description", &self.description)
            .field("category", &self.category)
            .field("hidden", &self.hidden)
            .finish_non_exhaustive()
    }
}

impl FunctionDescriptor {
    pub fn extension(
        name: impl Into<String>,
        aliases: impl IntoIterator<Item = impl Into<String>>,
        signature: impl Into<String>,
        description: impl Into<String>,
        category: impl Into<String>,
        handler: FunctionHandler,
    ) -> Self {
        let display_name = name.into();
        Self {
            canonical_name: normalize_name(&display_name),
            display_name,
            aliases: aliases.into_iter().map(Into::into).collect(),
            signature: signature.into(),
            description: description.into(),
            category: category.into(),
            alias_descriptions: HashMap::new(),
            hidden: false,
            implementation: FunctionImplementation::Extension(handler),
        }
    }

    fn builtin(name: &str, aliases: &[&str]) -> Self {
        let (signature, description, category) = builtin_metadata(name);
        let alias_descriptions = if name == "id" {
            HashMap::from([(
                normalize_name("elementId"),
                "Returns the element id of a node or relationship".to_string(),
            )])
        } else {
            HashMap::new()
        };
        Self {
            canonical_name: normalize_name(name),
            display_name: name.to_string(),
            aliases: aliases.iter().map(|alias| (*alias).to_string()).collect(),
            signature,
            description,
            category,
            alias_descriptions,
            hidden: false,
            implementation: FunctionImplementation::Builtin,
        }
    }

    pub fn hidden(mut self) -> Self {
        self.hidden = true;
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

    pub fn description_for_name(&self, name: &str) -> &str {
        self.alias_descriptions
            .get(&normalize_name(name))
            .map_or(&self.description, String::as_str)
    }

    pub fn category(&self) -> &str {
        &self.category
    }

    pub fn is_hidden(&self) -> bool {
        self.hidden
    }

    pub(crate) fn dispatch_name(&self) -> &str {
        &self.canonical_name
    }

    pub(crate) fn extension_handler(&self) -> Option<&FunctionHandler> {
        match &self.implementation {
            FunctionImplementation::Builtin => None,
            FunctionImplementation::Extension(handler) => Some(handler),
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum FunctionRegistryError {
    #[error("function name or alias collision: {name}")]
    NameCollision { name: String },
}

#[derive(Debug, Default)]
pub struct FunctionRegistryBuilder {
    descriptors: Vec<FunctionDescriptor>,
    names: HashMap<String, usize>,
}

impl FunctionRegistryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_builtins() -> Self {
        let mut builder = Self::new();
        for descriptor in FunctionRegistry::builtins().descriptors() {
            builder
                .register(descriptor.clone())
                .expect("built-in function names must be unique");
        }
        builder
    }

    pub fn register(
        &mut self,
        descriptor: FunctionDescriptor,
    ) -> Result<&mut Self, FunctionRegistryError> {
        let index = self.descriptors.len();
        let mut names = Vec::with_capacity(descriptor.aliases.len() + 1);
        names.push(descriptor.canonical_name.clone());
        names.extend(descriptor.aliases.iter().map(|alias| normalize_name(alias)));
        let mut descriptor_names = HashSet::with_capacity(names.len());
        if let Some(name) = names.iter().find(|name| {
            !descriptor_names.insert((*name).clone()) || self.names.contains_key(*name)
        }) {
            return Err(FunctionRegistryError::NameCollision { name: name.clone() });
        }
        for name in names {
            self.names.insert(name, index);
        }
        self.descriptors.push(descriptor);
        Ok(self)
    }

    pub fn build(self) -> FunctionRegistry {
        FunctionRegistry {
            descriptors: self.descriptors.into(),
            names: self.names,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FunctionRegistry {
    descriptors: Arc<[FunctionDescriptor]>,
    names: HashMap<String, usize>,
}

impl FunctionRegistry {
    pub fn builtins() -> &'static Self {
        static BUILTINS: OnceLock<FunctionRegistry> = OnceLock::new();
        BUILTINS.get_or_init(build_builtin_registry)
    }

    pub fn get(&self, name: &str) -> Option<&FunctionDescriptor> {
        self.names
            .get(name)
            .or_else(|| {
                name.bytes()
                    .any(|byte| byte.is_ascii_uppercase())
                    .then(|| self.names.get(&normalize_name(name)))
                    .flatten()
            })
            .map(|index| &self.descriptors[*index])
    }

    pub fn descriptors(&self) -> &[FunctionDescriptor] {
        &self.descriptors
    }
}

fn normalize_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

fn builtin_metadata(name: &str) -> (String, String, String) {
    let legacy = match name {
        "abs" => Some((
            "abs(input :: NUMBER) :: NUMBER",
            "Returns the absolute value of a number",
            "Numeric",
        )),
        "avg" => Some((
            "avg(input :: NUMBER) :: NUMBER",
            "Returns the average of numeric values",
            "Aggregating",
        )),
        "stdev" => Some((
            "stdev(input :: NUMBER) :: FLOAT",
            "Returns the sample standard deviation of numeric values",
            "Aggregating",
        )),
        "stdevp" => Some((
            "stdevp(input :: NUMBER) :: FLOAT",
            "Returns the population standard deviation of numeric values",
            "Aggregating",
        )),
        "ceil" => Some((
            "ceil(input :: NUMBER) :: NUMBER",
            "Returns the smallest integer greater than or equal to the input",
            "Numeric",
        )),
        "coalesce" => Some((
            "coalesce(input :: ANY...) :: ANY",
            "Returns the first non-null value in the list",
            "Scalar",
        )),
        "collect" => Some((
            "collect(input :: ANY) :: LIST<ANY>",
            "Collects values into a list",
            "Aggregating",
        )),
        "contains" => Some((
            "contains(input :: STRING, substring :: STRING) :: BOOLEAN",
            "Returns whether the string contains the substring",
            "String",
        )),
        "count" => Some((
            "count(input :: ANY) :: INTEGER",
            "Returns the number of values or rows",
            "Aggregating",
        )),
        "date" => Some(("date() :: STRING", "Returns the current date", "Temporal")),
        "datetime" => Some((
            "datetime() :: STRING",
            "Returns the current datetime",
            "Temporal",
        )),
        "duration" => Some((
            "duration() :: STRING",
            "Returns the current duration since epoch",
            "Temporal",
        )),
        "endsWith" => Some((
            "endsWith(input :: STRING, substring :: STRING) :: BOOLEAN",
            "Returns whether the string ends with the substring",
            "String",
        )),
        "exists" => Some((
            "exists(input :: ANY) :: BOOLEAN",
            "Returns whether the value is not null",
            "Scalar",
        )),
        "floor" => Some((
            "floor(input :: NUMBER) :: NUMBER",
            "Returns the largest integer less than or equal to the input",
            "Numeric",
        )),
        "head" => Some((
            "head(input :: LIST<ANY>) :: ANY",
            "Returns the first element of a list",
            "List",
        )),
        "id" => Some((
            "id(input :: NODE|RELATIONSHIP) :: STRING",
            "Returns the internal id of a node or relationship",
            "Scalar",
        )),
        "keys" => Some((
            "keys(input :: NODE|RELATIONSHIP|MAP) :: LIST<STRING>",
            "Returns the property keys of a node, relationship, or map",
            "Scalar",
        )),
        "labels" => Some((
            "labels(input :: NODE) :: LIST<STRING>",
            "Returns the labels of a node",
            "Scalar",
        )),
        "last" => Some((
            "last(input :: LIST<ANY>) :: ANY",
            "Returns the last element of a list",
            "List",
        )),
        "left" => Some((
            "left(input :: STRING, length :: INTEGER) :: STRING",
            "Returns the leftmost characters of a string",
            "String",
        )),
        "length" => Some((
            "length(input :: PATH) :: INTEGER",
            "Returns the length of a path",
            "Scalar",
        )),
        "ltrim" => Some((
            "ltrim(input :: STRING) :: STRING",
            "Returns the string with leading whitespace removed",
            "String",
        )),
        "max" => Some((
            "max(input :: NUMBER) :: NUMBER",
            "Returns the maximum of numeric values",
            "Aggregating",
        )),
        "min" => Some((
            "min(input :: NUMBER) :: NUMBER",
            "Returns the minimum of numeric values",
            "Aggregating",
        )),
        "nodes" => Some((
            "nodes(input :: PATH) :: LIST<NODE>",
            "Returns the nodes in a path",
            "Scalar",
        )),
        "now" => Some((
            "now() :: INTEGER",
            "Returns the current timestamp in milliseconds",
            "Temporal",
        )),
        "properties" => Some((
            "properties(input :: NODE|RELATIONSHIP|MAP) :: MAP",
            "Returns the properties of a node, relationship, or map",
            "Scalar",
        )),
        "range" => Some((
            "range(start :: INTEGER, end :: INTEGER [, step :: INTEGER]) :: LIST<INTEGER>",
            "Creates a list of integers in the given range",
            "List",
        )),
        "relationships" => Some((
            "relationships(input :: PATH) :: LIST<RELATIONSHIP>",
            "Returns the relationships in a path",
            "Scalar",
        )),
        "replace" => Some((
            "replace(input :: STRING, from :: STRING, to :: STRING) :: STRING",
            "Replaces all occurrences of a substring",
            "String",
        )),
        "reverse" => Some((
            "reverse(input :: LIST<ANY>) :: LIST<ANY>",
            "Returns the list in reverse order",
            "List",
        )),
        "right" => Some((
            "right(input :: STRING, length :: INTEGER) :: STRING",
            "Returns the rightmost characters of a string",
            "String",
        )),
        "round" => Some((
            "round(input :: NUMBER) :: NUMBER",
            "Returns the nearest integer to the input",
            "Numeric",
        )),
        "rtrim" => Some((
            "rtrim(input :: STRING) :: STRING",
            "Returns the string with trailing whitespace removed",
            "String",
        )),
        "size" => Some((
            "size(input :: LIST<ANY>|STRING) :: INTEGER",
            "Returns the size of a list or string",
            "List",
        )),
        "split" => Some((
            "split(input :: STRING, delimiter :: STRING) :: LIST<STRING>",
            "Splits a string by the delimiter",
            "String",
        )),
        "startsWith" => Some((
            "startsWith(input :: STRING, substring :: STRING) :: BOOLEAN",
            "Returns whether the string starts with the substring",
            "String",
        )),
        "substring" => Some((
            "substring(input :: STRING, start :: INTEGER [, length :: INTEGER]) :: STRING",
            "Returns a substring of the input",
            "String",
        )),
        "sum" => Some((
            "sum(input :: NUMBER) :: NUMBER",
            "Returns the sum of numeric values",
            "Aggregating",
        )),
        "tail" => Some((
            "tail(input :: LIST<ANY>) :: LIST<ANY>",
            "Returns the list without the first element",
            "List",
        )),
        "toBoolean" => Some((
            "toBoolean(input :: ANY) :: BOOLEAN",
            "Converts a value to boolean",
            "Scalar",
        )),
        "toFloat" => Some((
            "toFloat(input :: ANY) :: FLOAT",
            "Converts a value to float",
            "Scalar",
        )),
        "toInteger" => Some((
            "toInteger(input :: ANY) :: INTEGER",
            "Converts a value to integer",
            "Scalar",
        )),
        "toLower" => Some((
            "toLower(input :: STRING) :: STRING",
            "Returns the string in lowercase",
            "String",
        )),
        "toString" => Some((
            "toString(input :: ANY) :: STRING",
            "Converts a value to string",
            "Scalar",
        )),
        "toUpper" => Some((
            "toUpper(input :: STRING) :: STRING",
            "Returns the string in uppercase",
            "String",
        )),
        "trim" => Some((
            "trim(input :: STRING) :: STRING",
            "Returns the string with leading and trailing whitespace removed",
            "String",
        )),
        "type" => Some((
            "type(input :: RELATIONSHIP) :: STRING",
            "Returns the type of a relationship",
            "Scalar",
        )),
        _ => None,
    };
    if let Some((signature, description, category)) = legacy {
        return (
            signature.to_string(),
            description.to_string(),
            category.to_string(),
        );
    }

    let signature = format!("{name}(input :: ANY...) :: ANY");
    let description = format!("Built-in {name} function");
    let category = match normalize_name(name).as_str() {
        "count" | "avg" | "sum" | "min" | "max" | "stdev" | "stdevp" => "Aggregating",
        "abs" | "ceil" | "floor" | "round" | "sign" | "sqrt" | "rand" | "pi" | "sin" | "cos"
        | "tan" | "asin" | "acos" | "atan" | "atan2" | "degrees" | "radians" | "pow" | "exp"
        | "log" | "log10" | "sinh" | "cosh" | "tanh" | "e" => "Numeric",
        "toupper" | "tolower" | "trim" | "ltrim" | "rtrim" | "split" | "replace" | "substring"
        | "left" | "right" | "startswith" | "endswith" | "contains" | "char_length" => "String",
        "date" | "datetime" | "time" | "localdatetime" | "localtime" | "duration" | "now" => {
            "Temporal"
        }
        "range" | "head" | "tail" | "last" | "reverse" | "slice" | "indexof" | "size" => "List",
        "vector.similarity.cosine" | "vector.similarity.euclidean" => "Vector",
        _ => "Scalar",
    };
    (signature, description, category.to_string())
}

fn build_builtin_registry() -> FunctionRegistry {
    let groups: &[(&str, &[&str])] = &[
        ("count", &[]),
        ("avg", &[]),
        ("sum", &[]),
        ("min", &[]),
        ("max", &[]),
        ("stdev", &[]),
        ("stdevp", &[]),
        ("size", &[]),
        ("collect", &[]),
        ("type", &[]),
        ("labels", &[]),
        ("nodes", &[]),
        ("relationships", &[]),
        ("length", &[]),
        ("id", &["elementId"]),
        ("toString", &["str"]),
        ("toInteger", &["int", "integer"]),
        ("toFloat", &["float"]),
        ("toUpper", &["upper"]),
        ("toLower", &["lower"]),
        ("trim", &[]),
        ("ltrim", &[]),
        ("rtrim", &[]),
        ("split", &[]),
        ("replace", &[]),
        ("substring", &[]),
        ("left", &[]),
        ("right", &[]),
        ("startsWith", &["starts_with"]),
        ("endsWith", &["ends_with"]),
        ("contains", &[]),
        ("now", &["timestamp"]),
        ("date", &[]),
        ("datetime", &[]),
        ("time", &[]),
        ("localdatetime", &[]),
        ("localtime", &[]),
        ("duration", &[]),
        ("date.year", &[]),
        ("date.month", &[]),
        ("date.day", &[]),
        ("date.week", &[]),
        ("date.quarter", &[]),
        ("date.dayOfWeek", &[]),
        ("date.dayOfYear", &[]),
        ("date.truncate", &[]),
        ("datetime.year", &[]),
        ("datetime.month", &[]),
        ("datetime.day", &[]),
        ("datetime.hour", &[]),
        ("datetime.minute", &[]),
        ("datetime.second", &[]),
        ("datetime.truncate", &[]),
        ("time.hour", &[]),
        ("time.minute", &[]),
        ("time.second", &[]),
        ("time.truncate", &[]),
        ("exists", &[]),
        ("keys", &[]),
        ("properties", &[]),
        ("coalesce", &[]),
        ("abs", &[]),
        ("ceil", &[]),
        ("floor", &[]),
        ("round", &[]),
        ("sign", &[]),
        ("sqrt", &[]),
        ("rand", &[]),
        ("pi", &[]),
        ("range", &[]),
        ("head", &[]),
        ("tail", &[]),
        ("last", &[]),
        ("reverse", &[]),
        ("all", &[]),
        ("any", &[]),
        ("none", &[]),
        ("single", &[]),
        ("sin", &[]),
        ("cos", &[]),
        ("tan", &[]),
        ("asin", &[]),
        ("acos", &[]),
        ("atan", &[]),
        ("atan2", &[]),
        ("degrees", &[]),
        ("radians", &[]),
        ("pow", &["power"]),
        ("exp", &[]),
        ("log", &[]),
        ("log10", &[]),
        ("randomUUID", &[]),
        ("toBoolean", &["bool"]),
        ("toList", &[]),
        ("isEmpty", &[]),
        ("sinh", &[]),
        ("cosh", &[]),
        ("tanh", &[]),
        ("e", &[]),
        ("nullIf", &[]),
        ("valueType", &["valueTypeOf"]),
        ("char_length", &["character_length"]),
        ("toIntegerOrNull", &[]),
        ("toFloatOrNull", &[]),
        ("toBooleanOrNull", &[]),
        ("slice", &[]),
        ("indexOf", &[]),
        ("vector.similarity.cosine", &[]),
        ("vector.similarity.euclidean", &[]),
        ("db.create.setNodeVectorProperty", &[]),
        ("db.create.setRelationshipVectorProperty", &[]),
    ];
    let mut builder = FunctionRegistryBuilder::new();
    for (name, aliases) in groups {
        builder
            .register(FunctionDescriptor::builtin(name, aliases))
            .expect("built-in function names must be unique");
    }
    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handler() -> FunctionHandler {
        Arc::new(|_, args| Ok(args.first().cloned().unwrap_or(Value::Null)))
    }

    #[test]
    fn lookup_is_case_insensitive_and_resolves_aliases() {
        let registry = FunctionRegistry::builtins();
        assert_eq!(registry.get("TOUPPER").unwrap().name(), "toUpper");
        assert_eq!(registry.get("INTEGER").unwrap().name(), "toInteger");
    }

    #[test]
    fn registration_rejects_canonical_and_alias_collisions() {
        let mut builder = FunctionRegistryBuilder::new();
        let duplicate_alias = builder
            .register(FunctionDescriptor::extension(
                "example.duplicate",
                ["EXAMPLE.DUPLICATE"],
                "",
                "",
                "Scalar",
                handler(),
            ))
            .unwrap_err();
        assert_eq!(
            duplicate_alias,
            FunctionRegistryError::NameCollision {
                name: "example.duplicate".into()
            }
        );
        builder
            .register(FunctionDescriptor::extension(
                "example.one",
                ["example.alias"],
                "",
                "",
                "Scalar",
                handler(),
            ))
            .unwrap();
        let canonical = builder
            .register(FunctionDescriptor::extension(
                "EXAMPLE.ONE",
                std::iter::empty::<&str>(),
                "",
                "",
                "Scalar",
                handler(),
            ))
            .unwrap_err();
        assert_eq!(
            canonical,
            FunctionRegistryError::NameCollision {
                name: "example.one".into()
            }
        );
        let alias = builder
            .register(FunctionDescriptor::extension(
                "example.two",
                ["Example.Alias"],
                "",
                "",
                "Scalar",
                handler(),
            ))
            .unwrap_err();
        assert_eq!(
            alias,
            FunctionRegistryError::NameCollision {
                name: "example.alias".into()
            }
        );
    }
}
