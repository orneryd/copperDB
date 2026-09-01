use copperdb_util::RequestContext;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ActionQueryResult {
    pub rows: Vec<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{code}")]
pub struct ActionError {
    pub code: String,
}

impl ActionError {
    pub fn new(code: impl Into<String>) -> Self {
        Self { code: code.into() }
    }
}

pub trait ActionQueryService: Send + Sync {
    fn query_read(
        &self,
        request_context: &RequestContext,
        database: &str,
        cypher: &str,
        params: &BTreeMap<String, Value>,
        caller_roles: &[String],
    ) -> Result<ActionQueryResult, ActionError>;
}

pub struct ActionCallContext<'a> {
    pub request_context: &'a RequestContext,
    pub default_database: &'a str,
    pub caller_roles: &'a [String],
    pub query_service: &'a dyn ActionQueryService,
}

pub type ActionHandler = Arc<
    dyn Fn(&ActionCallContext<'_>, &Value) -> Result<Value, ActionError> + Send + Sync + 'static,
>;

#[derive(Clone)]
pub struct ActionDescriptor {
    canonical_name: String,
    display_name: String,
    description: String,
    input_schema: Value,
    category: String,
    package_id: Option<String>,
    allowed_roles: Vec<String>,
    handler: ActionHandler,
}

impl fmt::Debug for ActionDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActionDescriptor")
            .field("canonical_name", &self.canonical_name)
            .field("display_name", &self.display_name)
            .field("description", &self.description)
            .field("input_schema", &self.input_schema)
            .field("category", &self.category)
            .field("package_id", &self.package_id)
            .field("allowed_roles", &self.allowed_roles)
            .finish_non_exhaustive()
    }
}

impl ActionDescriptor {
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: Value,
        category: impl Into<String>,
        handler: ActionHandler,
    ) -> Self {
        let display_name = name.into();
        Self {
            canonical_name: normalize_name(&display_name),
            display_name,
            description: description.into(),
            input_schema,
            category: category.into(),
            package_id: None,
            allowed_roles: Vec::new(),
            handler,
        }
    }

    pub fn allowing_roles(mut self, roles: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.allowed_roles = roles.into_iter().map(Into::into).collect();
        self
    }

    pub fn attributed_to(mut self, package_id: impl Into<String>) -> Self {
        self.package_id = Some(package_id.into());
        self
    }

    pub fn name(&self) -> &str {
        &self.display_name
    }

    pub fn canonical_name(&self) -> &str {
        &self.canonical_name
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn input_schema(&self) -> &Value {
        &self.input_schema
    }

    pub fn category(&self) -> &str {
        &self.category
    }

    pub fn package_id(&self) -> Option<&str> {
        self.package_id.as_deref()
    }

    pub fn allowed_roles(&self) -> &[String] {
        &self.allowed_roles
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ActionRegistryError {
    #[error("action name collision: {name}")]
    NameCollision { name: String },
}

#[derive(Debug, Default)]
pub struct ActionRegistryBuilder {
    descriptors: Vec<ActionDescriptor>,
    names: HashMap<String, usize>,
}

impl ActionRegistryBuilder {
    pub fn register(&mut self, descriptor: ActionDescriptor) -> Result<(), ActionRegistryError> {
        let name = descriptor.canonical_name().to_string();
        if self.names.contains_key(&name) {
            return Err(ActionRegistryError::NameCollision { name });
        }
        self.names.insert(name, self.descriptors.len());
        self.descriptors.push(descriptor);
        Ok(())
    }

    pub fn build(self) -> ActionRegistry {
        ActionRegistry {
            descriptors: self.descriptors.into(),
            names: self.names,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ActionRegistry {
    descriptors: Arc<[ActionDescriptor]>,
    names: HashMap<String, usize>,
}

impl ActionRegistry {
    pub fn empty() -> Self {
        ActionRegistryBuilder::default().build()
    }

    pub fn get(&self, name: &str) -> Option<&ActionDescriptor> {
        self.names
            .get(&normalize_name(name))
            .and_then(|index| self.descriptors.get(*index))
    }

    pub fn descriptors(&self) -> &[ActionDescriptor] {
        &self.descriptors
    }

    pub fn execute(
        &self,
        name: &str,
        context: &ActionCallContext<'_>,
        input: &Value,
    ) -> Result<Value, ActionError> {
        context
            .request_context
            .check_active()
            .map_err(|_| ActionError::new("request_cancelled"))?;
        let descriptor = self
            .get(name)
            .ok_or_else(|| ActionError::new("action_not_found"))?;
        if !descriptor.allowed_roles.is_empty()
            && !context.caller_roles.iter().any(|role| {
                descriptor
                    .allowed_roles
                    .iter()
                    .any(|allowed| allowed == role)
            })
        {
            return Err(ActionError::new("action_forbidden"));
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (descriptor.handler)(context, input)
        }))
        .map_err(|_| ActionError::new("action_panic"))??;
        context
            .request_context
            .check_active()
            .map_err(|_| ActionError::new("request_cancelled"))?;
        Ok(result)
    }
}

fn normalize_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct UnusedQueryService;

    impl ActionQueryService for UnusedQueryService {
        fn query_read(
            &self,
            _request_context: &RequestContext,
            _database: &str,
            _cypher: &str,
            _params: &BTreeMap<String, Value>,
            _caller_roles: &[String],
        ) -> Result<ActionQueryResult, ActionError> {
            panic!("query service must not be called")
        }
    }

    fn context<'a>(
        request_context: &'a RequestContext,
        caller_roles: &'a [String],
        query_service: &'a dyn ActionQueryService,
    ) -> ActionCallContext<'a> {
        ActionCallContext {
            request_context,
            default_database: "copperdb",
            caller_roles,
            query_service,
        }
    }

    #[test]
    fn denies_roles_before_dispatch_and_accepts_canonical_case() {
        let called = Arc::new(AtomicBool::new(false));
        let handler_called = Arc::clone(&called);
        let mut builder = ActionRegistryBuilder::default();
        builder
            .register(
                ActionDescriptor::new(
                    "Example.Read",
                    "Read example data",
                    json!({"type": "object"}),
                    "testing",
                    Arc::new(move |_, _| {
                        handler_called.store(true, Ordering::Relaxed);
                        Ok(json!({"ok": true}))
                    }),
                )
                .allowing_roles(["reader"]),
            )
            .unwrap();
        let registry = builder.build();
        let request = RequestContext::detached();
        let query_service = UnusedQueryService;

        let denied = registry
            .execute(
                "example.read",
                &context(&request, &["viewer".into()], &query_service),
                &json!({}),
            )
            .unwrap_err();
        assert_eq!(denied.code, "action_forbidden");
        assert!(!called.load(Ordering::Relaxed));

        let allowed = registry
            .execute(
                "EXAMPLE.READ",
                &context(&request, &["reader".into()], &query_service),
                &json!({}),
            )
            .unwrap();
        assert_eq!(allowed, json!({"ok": true}));
        assert!(called.load(Ordering::Relaxed));
    }

    #[test]
    fn rejects_cancelled_requests_before_dispatch() {
        let called = Arc::new(AtomicBool::new(false));
        let handler_called = Arc::clone(&called);
        let mut builder = ActionRegistryBuilder::default();
        builder
            .register(ActionDescriptor::new(
                "example.cancelled",
                "Cancellation test",
                json!({"type": "object"}),
                "testing",
                Arc::new(move |_, _| {
                    handler_called.store(true, Ordering::Relaxed);
                    Ok(Value::Null)
                }),
            ))
            .unwrap();
        let registry = builder.build();
        let request = RequestContext::detached();
        request.cancel();
        let query_service = UnusedQueryService;

        let error = registry
            .execute(
                "example.cancelled",
                &context(&request, &[], &query_service),
                &json!({}),
            )
            .unwrap_err();

        assert_eq!(error.code, "request_cancelled");
        assert!(!called.load(Ordering::Relaxed));
    }

    #[test]
    fn isolates_handler_panics_with_stable_error_code() {
        let mut builder = ActionRegistryBuilder::default();
        builder
            .register(ActionDescriptor::new(
                "example.panic",
                "Panic test",
                json!({"type": "object"}),
                "testing",
                Arc::new(|_, _| panic!("package action panic")),
            ))
            .unwrap();
        let registry = builder.build();
        let request = RequestContext::detached();
        let query_service = UnusedQueryService;

        let error = registry
            .execute(
                "example.panic",
                &context(&request, &[], &query_service),
                &json!({}),
            )
            .unwrap_err();

        assert_eq!(error.code, "action_panic");
    }
}
