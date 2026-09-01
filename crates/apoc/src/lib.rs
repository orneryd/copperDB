//! Representative APOC package loaded through `copperdb-plugin`.

use copperdb_eval::{
    GraphDirection, GraphNode, ProcedureCallContext, ProcedureDescriptor, ProcedureError,
    ProcedureMode, ProcedureOutput, Row,
};
use copperdb_filter::{FunctionDescriptor, FunctionHandler};
use copperdb_plugin::{
    PackageCapability, PackageDefinition, PackageDescriptor, StaticPackageFactory,
};
use semver::Version;
use serde_json::{Map, Number, Value};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

pub const PACKAGE_ID: &str = "copperdb.apoc";
const MAX_TRAVERSAL_LEVEL: usize = 64;
const MAX_VISITED_NODES: usize = 100_000;
const MAX_JSON_BYTES: usize = 16 * 1024 * 1024;
const MAX_JSON_ROWS: usize = 100_000;

pub fn factory() -> StaticPackageFactory {
    StaticPackageFactory::new(package())
}

pub fn package() -> PackageDefinition {
    let descriptor =
        PackageDescriptor::new(PACKAGE_ID, Version::new(1, 0, 0), "copperdb contributors")
            .requesting([PackageCapability::QueryRead]);
    PackageDefinition::new(descriptor)
        .with_function(function(
            "apoc.create.uuid",
            "apoc.create.uuid() :: STRING",
            "Generate UUID",
            "create",
            Arc::new(|_, _| Ok(create_uuid())),
        ))
        .with_function(function(
            "apoc.text.join",
            "apoc.text.join(values :: LIST<ANY>, delimiter :: ANY) :: STRING",
            "Join strings",
            "text",
            Arc::new(|_, args| Ok(text_join(args))),
        ))
        .with_function(function(
            "apoc.coll.flatten",
            "apoc.coll.flatten(value :: ANY) :: LIST<ANY>",
            "Flatten nested lists",
            "coll",
            Arc::new(|_, args| Ok(coll_flatten(args))),
        ))
        .with_function(function(
            "apoc.coll.toSet",
            "apoc.coll.toSet(value :: ANY) :: LIST<ANY>",
            "Remove duplicates",
            "coll",
            Arc::new(|_, args| Ok(coll_to_set(args))),
        ))
        .with_function(function(
            "apoc.map.merge",
            "apoc.map.merge(first :: MAP, second :: MAP) :: MAP",
            "Merge maps",
            "map",
            Arc::new(|_, args| Ok(map_merge(args))),
        ))
        .with_function(function(
            "apoc.convert.toJson",
            "apoc.convert.toJson(value :: ANY) :: STRING",
            "Convert to JSON",
            "convert",
            Arc::new(|_, args| Ok(convert_to_json(args))),
        ))
        .with_function(function(
            "apoc.convert.fromJsonMap",
            "apoc.convert.fromJsonMap(json :: STRING) :: MAP",
            "Parse JSON map",
            "convert",
            Arc::new(|_, args| Ok(convert_from_json_map(args))),
        ))
        .with_function(function(
            "apoc.meta.type",
            "apoc.meta.type(value :: ANY) :: STRING",
            "Get type",
            "meta",
            Arc::new(|_, args| Ok(meta_type(args))),
        ))
        .with_procedure(
            ProcedureDescriptor::extension(
                "apoc.path.subgraphNodes",
                std::iter::empty::<&str>(),
                "apoc.path.subgraphNodes(startNode :: NODE, config = {} :: MAP) :: (node :: NODE)",
                "Returns nodes reachable from a start node",
                ProcedureMode::Read,
                Arc::new(subgraph_nodes),
            )
            .requiring_capabilities(["query:read"]),
        )
        .with_procedure(
            ProcedureDescriptor::extension(
                "apoc.load.json",
                std::iter::empty::<&str>(),
                "apoc.load.json(urlOrKeyOrBinary :: STRING, path :: STRING = '', config :: MAP = {}) :: (value :: MAP)",
                "Loads JSON",
                ProcedureMode::Read,
                Arc::new(load_json),
            )
            .requiring_capabilities(["query:read"]),
        )
}

fn load_json(
    context: &ProcedureCallContext<'_>,
    args: &[Value],
) -> Result<ProcedureOutput, ProcedureError> {
    if args.is_empty() {
        return Err(ProcedureError::Message(
            "procedure apoc.load.json requires at least 1 arguments, got 0".into(),
        ));
    }
    if args.len() > 3 {
        return Err(ProcedureError::Message(format!(
            "procedure apoc.load.json accepts at most 3 arguments, got {}",
            args.len()
        )));
    }
    let source = match &args[0] {
        Value::String(source) => source.clone(),
        Value::Null => "null".into(),
        source => source.to_string(),
    };
    if source.is_empty() {
        return Err(ProcedureError::Message(
            "apoc.load.json requires a URL or file path".into(),
        ));
    }
    let bytes = context
        .import_files
        .read(context.request_context, &source, MAX_JSON_BYTES)
        .map_err(|error| ProcedureError::Message(format!("failed to load JSON: {error}")))?;
    context.request_context.check_active()?;
    let value = serde_json::from_slice::<Value>(&bytes)
        .map(json_numbers_to_float)
        .map_err(|error| ProcedureError::Message(format!("failed to load JSON: {error}")))?;
    let values = match value {
        Value::Array(values) => values,
        value => vec![value],
    };
    if values.len() > MAX_JSON_ROWS {
        return Err(ProcedureError::Message(format!(
            "failed to load JSON: result exceeds the {MAX_JSON_ROWS} row limit"
        )));
    }
    let mut rows = Vec::with_capacity(values.len());
    for value in values {
        context.request_context.check_active()?;
        let mut row = Row::new();
        row.insert("value".into(), value);
        rows.push(row);
    }
    Ok(ProcedureOutput::new(vec!["value".into()], rows))
}

fn function(
    name: &str,
    signature: &str,
    description: &str,
    category: &str,
    handler: FunctionHandler,
) -> FunctionDescriptor {
    FunctionDescriptor::extension(
        name,
        std::iter::empty::<&str>(),
        signature,
        description,
        category,
        handler,
    )
}

fn create_uuid() -> Value {
    let mut bytes = [0u8; 16];
    let _ = getrandom::fill(&mut bytes);
    Value::String(format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    ))
}

fn text_join(args: &[Value]) -> Value {
    if args.len() < 2 {
        return Value::Null;
    }
    let Some(values) = args[0].as_array() else {
        return Value::String(String::new());
    };
    let delimiter = args[1].as_str().unwrap_or_default();
    Value::String(
        values
            .iter()
            .map(go_format)
            .collect::<Vec<_>>()
            .join(delimiter),
    )
}

fn go_format(value: &Value) -> String {
    match value {
        Value::Null => "<nil>".into(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(values) => format!(
            "[{}]",
            values.iter().map(go_format).collect::<Vec<_>>().join(" ")
        ),
        Value::Object(values) => format!(
            "map[{}]",
            values
                .iter()
                .map(|(key, value)| format!("{key}:{}", go_format(value)))
                .collect::<Vec<_>>()
                .join(" ")
        ),
    }
}

fn coll_flatten(args: &[Value]) -> Value {
    let Some(value) = args.first() else {
        return Value::Null;
    };
    let mut flattened = Vec::new();
    flatten_value(value, &mut flattened);
    Value::Array(flattened)
}

fn flatten_value(value: &Value, flattened: &mut Vec<Value>) {
    if let Value::Array(values) = value {
        for value in values {
            flatten_value(value, flattened);
        }
    } else {
        flattened.push(value.clone());
    }
}

fn coll_to_set(args: &[Value]) -> Value {
    let Some(Value::Array(values)) = args.first() else {
        return Value::Array(Vec::new());
    };
    let mut seen = HashSet::new();
    Value::Array(
        values
            .iter()
            .filter(|value| seen.insert(type_value_key(value)))
            .cloned()
            .collect(),
    )
}

fn type_value_key(value: &Value) -> String {
    let kind = match value {
        Value::Null => "nil",
        Value::Bool(_) => "bool",
        Value::Number(number) if number.is_i64() || number.is_u64() => "int64",
        Value::Number(_) => "float64",
        Value::String(_) => "string",
        Value::Array(_) => "[]interface {}",
        Value::Object(_) => "map[string]interface {}",
    };
    format!("{kind}:{}", go_format(value))
}

fn map_merge(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::Object(Map::new());
    }
    let (Value::Object(first), Value::Object(second)) = (&args[0], &args[1]) else {
        return Value::Object(Map::new());
    };
    let mut merged = first.clone();
    merged.extend(second.clone());
    Value::Object(merged)
}

fn convert_to_json(args: &[Value]) -> Value {
    let Some(value) = args.first() else {
        return Value::Null;
    };
    match serde_json::to_string(value) {
        Ok(json) => Value::String(
            json.replace('&', "\\u0026")
                .replace('<', "\\u003c")
                .replace('>', "\\u003e"),
        ),
        Err(_) => Value::Null,
    }
}

fn convert_from_json_map(args: &[Value]) -> Value {
    let Some(json) = args.first().and_then(Value::as_str) else {
        return Value::Null;
    };
    match serde_json::from_str::<Value>(json) {
        Ok(Value::Object(values)) => Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, json_numbers_to_float(value)))
                .collect(),
        ),
        _ => Value::Null,
    }
}

fn json_numbers_to_float(value: Value) -> Value {
    match value {
        Value::Number(number) => number
            .as_f64()
            .and_then(Number::from_f64)
            .map_or(Value::Null, Value::Number),
        Value::Array(values) => {
            Value::Array(values.into_iter().map(json_numbers_to_float).collect())
        }
        Value::Object(values) => Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, json_numbers_to_float(value)))
                .collect(),
        ),
        value => value,
    }
}

fn meta_type(args: &[Value]) -> Value {
    let kind = match args.first().unwrap_or(&Value::Null) {
        Value::Null => "NULL",
        Value::Bool(_) => "BOOLEAN",
        Value::Number(number) if number.is_i64() || number.is_u64() => "INTEGER",
        Value::Number(_) => "FLOAT",
        Value::String(_) => "STRING",
        Value::Array(_) => "LIST",
        Value::Object(_) => "MAP",
    };
    Value::String(kind.into())
}

#[derive(Debug)]
struct TraversalConfig {
    min_level: usize,
    max_level: usize,
    direction: GraphDirection,
    relationship_types: Vec<String>,
    include_labels: Vec<String>,
    exclude_labels: Vec<String>,
    termination_labels: Vec<String>,
    limit: Option<usize>,
}

fn subgraph_nodes(
    context: &ProcedureCallContext<'_>,
    args: &[Value],
) -> Result<ProcedureOutput, ProcedureError> {
    if !(1..=2).contains(&args.len()) {
        return Err(ProcedureError::Message(
            "apoc.path.subgraphNodes expects one or two arguments".into(),
        ));
    }
    let Some(start_id) = args[0]
        .as_object()
        .and_then(|node| node.get("_id"))
        .and_then(Value::as_str)
    else {
        return Ok(ProcedureOutput::new(vec!["node".into()], Vec::new()));
    };
    let config = traversal_config(args.get(1));
    let mut queue = VecDeque::from([(start_id.to_string(), 0usize)]);
    let mut visited = HashSet::from([start_id.to_string()]);
    let mut rows = Vec::new();

    while let Some((node_id, level)) = queue.pop_front() {
        context.request_context.check_active()?;
        let Some(node) = context
            .graph_read
            .node(&node_id)
            .map_err(|error| ProcedureError::Message(error.code))?
        else {
            continue;
        };
        if level >= config.min_level && label_included(&node, &config) {
            let mut row = Row::new();
            row.insert("node".into(), node.to_value());
            rows.push(row);
            if config.limit.is_some_and(|limit| rows.len() >= limit) {
                break;
            }
        }
        if level >= config.max_level || has_any_label(&node, &config.termination_labels) {
            continue;
        }
        let relationships = context
            .graph_read
            .relationships(&node_id, config.direction, &config.relationship_types)
            .map_err(|error| ProcedureError::Message(error.code))?;
        for relationship in relationships {
            let next_id = match config.direction {
                GraphDirection::Outgoing if relationship.start_node == node_id => {
                    Some(relationship.end_node)
                }
                GraphDirection::Incoming if relationship.end_node == node_id => {
                    Some(relationship.start_node)
                }
                GraphDirection::Both if relationship.start_node == node_id => {
                    Some(relationship.end_node)
                }
                GraphDirection::Both if relationship.end_node == node_id => {
                    Some(relationship.start_node)
                }
                _ => None,
            };
            if let Some(next_id) = next_id {
                if visited.len() >= MAX_VISITED_NODES {
                    return Err(ProcedureError::Message("traversal_limit_exceeded".into()));
                }
                if visited.insert(next_id.clone()) {
                    queue.push_back((next_id, level + 1));
                }
            }
        }
    }

    Ok(ProcedureOutput::new(vec!["node".into()], rows))
}

fn traversal_config(config: Option<&Value>) -> TraversalConfig {
    let config = config.and_then(Value::as_object);
    let min_level = config
        .and_then(|config| config.get("minLevel"))
        .and_then(nonnegative_usize)
        .unwrap_or(0)
        .min(MAX_TRAVERSAL_LEVEL);
    let max_level = config
        .and_then(|config| config.get("maxLevel"))
        .and_then(positive_usize)
        .unwrap_or(3)
        .min(MAX_TRAVERSAL_LEVEL);
    let relationship_filter = config
        .and_then(|config| config.get("relationshipFilter"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let (direction, relationship_types) = relationship_filter_config(relationship_filter);
    let label_filter = config
        .and_then(|config| config.get("labelFilter"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let (include_labels, exclude_labels, termination_labels) = label_filter_config(label_filter);
    let limit = config
        .and_then(|config| config.get("limit"))
        .and_then(positive_usize);
    TraversalConfig {
        min_level,
        max_level,
        direction,
        relationship_types,
        include_labels,
        exclude_labels,
        termination_labels,
        limit,
    }
}

fn nonnegative_usize(value: &Value) -> Option<usize> {
    value
        .as_u64()
        .or_else(|| value.as_str()?.parse().ok())
        .and_then(|value| value.try_into().ok())
}

fn positive_usize(value: &Value) -> Option<usize> {
    nonnegative_usize(value).filter(|value| *value > 0)
}

fn relationship_filter_config(filter: &str) -> (GraphDirection, Vec<String>) {
    let (direction, filter) = match filter.as_bytes().first() {
        Some(b'>') => (GraphDirection::Outgoing, &filter[1..]),
        Some(b'<') => (GraphDirection::Incoming, &filter[1..]),
        _ => (GraphDirection::Both, filter),
    };
    let relationship_types = filter
        .split('|')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    (direction, relationship_types)
}

fn label_filter_config(filter: &str) -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut include = Vec::new();
    let mut exclude = Vec::new();
    let mut terminate = Vec::new();
    for item in filter.split('|').map(str::trim) {
        let Some((marker, label)) = item.split_at_checked(1) else {
            continue;
        };
        if label.is_empty() {
            continue;
        }
        match marker {
            "+" => include.push(label.to_string()),
            "-" => exclude.push(label.to_string()),
            "/" => terminate.push(label.to_string()),
            _ => {}
        }
    }
    (include, exclude, terminate)
}

fn label_included(node: &GraphNode, config: &TraversalConfig) -> bool {
    !has_any_label(node, &config.exclude_labels)
        && (config.include_labels.is_empty() || has_any_label(node, &config.include_labels))
}

fn has_any_label(node: &GraphNode, labels: &[String]) -> bool {
    labels
        .iter()
        .any(|label| node.labels.iter().any(|actual| actual == label))
}

#[cfg(test)]
mod tests {
    use super::*;
    use copperdb_engine::{CopperDb, DatabaseConfig};
    use copperdb_eval::{DeniedImportFileService, RootedImportFileService};
    use copperdb_plugin::resolve_packages;
    use copperdb_storage::{EdgeRecord, NodeRecord, StorageEngine};
    use serde_json::json;
    use std::collections::{BTreeMap, HashMap};
    use std::fs;
    use tempfile::tempdir;

    fn database() -> CopperDb {
        let packages = resolve_packages([package()]).unwrap();
        CopperDb::from_storage_with_packages(
            Arc::new(StorageEngine::open_memory().unwrap()),
            DatabaseConfig::default(),
            &packages,
        )
        .unwrap()
    }

    fn evaluate(database: &CopperDb, expression: &str) -> Value {
        database
            .execute(&format!("RETURN {expression} AS value"), HashMap::new())
            .unwrap()
            .rows
            .remove(0)
            .remove("value")
            .unwrap()
    }

    fn seed_traversal(database: &CopperDb) {
        for (id, labels) in [
            ("a", vec!["Root"]),
            ("b", vec!["Hidden"]),
            ("c", vec!["Visible"]),
            ("d", vec!["Stop"]),
            ("e", vec!["Beyond"]),
        ] {
            database
                .storage()
                .put_node_record(&NodeRecord {
                    id: id.into(),
                    labels: labels.into_iter().map(str::to_string).collect(),
                    properties: BTreeMap::from([("name".into(), json!(id))]),
                    named_embeddings: BTreeMap::new(),
                    chunk_embeddings: Vec::new(),
                    embed_meta: Default::default(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })
                .unwrap();
        }
        for (id, start_node, end_node, edge_type) in [
            ("01", "a", "b", "KNOWS"),
            ("02", "a", "c", "LIKES"),
            ("03", "b", "d", "KNOWS"),
            ("04", "d", "e", "KNOWS"),
        ] {
            database
                .storage()
                .put_edge_record(&EdgeRecord {
                    id: id.into(),
                    start_node: start_node.into(),
                    end_node: end_node.into(),
                    edge_type: edge_type.into(),
                    properties: BTreeMap::new(),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                })
                .unwrap();
        }
    }

    fn traverse(database: &CopperDb, start_id: &str, config: &str) -> Vec<String> {
        let result = database
            .execute(
                &format!(
                    "CALL apoc.path.subgraphNodes($start, {config}) YIELD node RETURN node._id AS id"
                ),
                HashMap::from([("start".into(), json!({"_id": start_id}))]),
            )
            .unwrap();
        result
            .rows
            .into_iter()
            .map(|row| row["id"].as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn package_resolves_all_representative_functions() {
        let packages = resolve_packages([package()]).unwrap();
        assert_eq!(packages.packages()[0].id, PACKAGE_ID);
        for name in [
            "apoc.create.uuid",
            "apoc.text.join",
            "apoc.coll.flatten",
            "apoc.coll.toSet",
            "apoc.map.merge",
            "apoc.convert.toJson",
            "apoc.convert.fromJsonMap",
            "apoc.meta.type",
        ] {
            let registry = packages.function_registry();
            let descriptor = registry.get(name).unwrap_or_else(|| panic!("{name}"));
            assert_eq!(descriptor.package_id(), Some(PACKAGE_ID));
        }
        let procedures = packages.procedure_registry();
        let descriptor = procedures.get("apoc.path.subgraphNodes").unwrap();
        assert_eq!(descriptor.mode(), ProcedureMode::Read);
        assert_eq!(descriptor.package_id(), Some(PACKAGE_ID));
        assert_eq!(descriptor.required_capabilities(), ["query:read"]);
        let descriptor = procedures.get("apoc.load.json").unwrap();
        assert_eq!(descriptor.mode(), ProcedureMode::Read);
        assert_eq!(descriptor.package_id(), Some(PACKAGE_ID));
        assert_eq!(
            descriptor.signature(),
            "apoc.load.json(urlOrKeyOrBinary :: STRING, path :: STRING = '', config :: MAP = {}) :: (value :: MAP)"
        );
    }

    #[test]
    fn load_json_expands_root_arrays_and_preserves_upstream_number_shape() {
        let root = tempdir().unwrap();
        fs::write(
            root.path().join("payload.json"),
            br#"[{"id":1},{"id":2.5}]"#,
        )
        .unwrap();
        let storage = StorageEngine::open_temporary().unwrap();
        let import_files = RootedImportFileService::new(root.path()).unwrap();
        let request_context = copperdb_util::RequestContext::detached();
        let row = Row::new();
        let params = HashMap::new();
        let capabilities = ["query:read".into()];
        let caller_roles = ["admin".into()];
        let context = ProcedureCallContext {
            row: &row,
            params: &params,
            capabilities: &capabilities,
            caller_roles: &caller_roles,
            database: Some("copperdb"),
            request_context: &request_context,
            graph_read: &storage,
            import_files: &import_files,
        };

        let output = load_json(&context, &[json!("payload.json")]).unwrap();

        assert_eq!(output.columns, ["value"]);
        assert_eq!(output.rows.len(), 2);
        assert_eq!(output.rows[0]["value"]["id"].as_f64(), Some(1.0));
        assert_eq!(output.rows[1]["value"]["id"].as_f64(), Some(2.5));
    }

    #[test]
    fn load_json_is_default_denied_and_rejects_trailing_json() {
        let storage = StorageEngine::open_temporary().unwrap();
        let request_context = copperdb_util::RequestContext::detached();
        let row = Row::new();
        let params = HashMap::new();
        let capabilities = ["query:read".into()];
        let caller_roles = ["admin".into()];
        let denied = DeniedImportFileService;
        let denied_context = ProcedureCallContext {
            row: &row,
            params: &params,
            capabilities: &capabilities,
            caller_roles: &caller_roles,
            database: Some("copperdb"),
            request_context: &request_context,
            graph_read: &storage,
            import_files: &denied,
        };
        assert_eq!(
            load_json(&denied_context, &[json!("payload.json")])
                .unwrap_err()
                .to_string(),
            "failed to load JSON: local APOC import file access is disabled"
        );

        let root = tempdir().unwrap();
        fs::write(root.path().join("records.json"), b"{\"id\":1}\n{\"id\":2}").unwrap();
        let import_files = RootedImportFileService::new(root.path()).unwrap();
        let trailing_context = ProcedureCallContext {
            import_files: &import_files,
            ..denied_context
        };
        assert!(load_json(&trailing_context, &[json!("records.json")])
            .unwrap_err()
            .to_string()
            .starts_with("failed to load JSON:"));
    }

    #[test]
    fn pure_functions_match_nornicdb_query_contracts() {
        let database = database();
        let uuid = evaluate(&database, "apoc.create.uuid()");
        let uuid = uuid.as_str().unwrap();
        assert_eq!(uuid.len(), 36);
        assert_eq!(&uuid[8..9], "-");
        assert_eq!(&uuid[13..14], "-");
        assert_eq!(&uuid[18..19], "-");
        assert_eq!(&uuid[23..24], "-");

        assert_eq!(
            evaluate(&database, "apoc.text.join([1, true, null], '|')"),
            json!("1|true|<nil>")
        );
        assert_eq!(
            evaluate(&database, "apoc.coll.flatten([1, [2, [3]], 4])"),
            json!([1, 2, 3, 4])
        );
        assert_eq!(
            evaluate(&database, "apoc.coll.toSet([1, 2, 1, '1', 2])"),
            json!([1, 2, "1"])
        );
        assert_eq!(
            evaluate(
                &database,
                "apoc.map.merge({a: 1, same: 'first'}, {b: 2, same: 'second'})"
            ),
            json!({"a": 1, "b": 2, "same": "second"})
        );
        assert_eq!(
            evaluate(&database, "apoc.convert.toJson({b: '<x>', a: 1})"),
            json!(r#"{"a":1,"b":"\u003cx\u003e"}"#)
        );
        let parsed = evaluate(
            &database,
            r#"apoc.convert.fromJsonMap('{"count":1,"nested":{"value":2.5}}')"#,
        );
        assert_eq!(parsed["count"].as_f64(), Some(1.0));
        assert_eq!(parsed["nested"]["value"].as_f64(), Some(2.5));
        assert_eq!(evaluate(&database, "apoc.meta.type(null)"), json!("NULL"));
        assert_eq!(evaluate(&database, "apoc.meta.type(42)"), json!("INTEGER"));
        assert_eq!(evaluate(&database, "apoc.meta.type([])"), json!("LIST"));
        let discovery = database
            .execute(
                "CALL dbms.functions() YIELD name, package RETURN name, package",
                HashMap::new(),
            )
            .unwrap();
        let apoc_type = discovery
            .rows
            .iter()
            .find(|row| row.get("name") == Some(&json!("apoc.meta.type")))
            .unwrap();
        assert_eq!(apoc_type.get("package"), Some(&json!(PACKAGE_ID)));
    }

    #[test]
    fn invalid_inputs_use_nornicdb_sentinel_values() {
        let database = database();
        assert_eq!(evaluate(&database, "apoc.text.join(['a'])"), Value::Null);
        assert_eq!(
            evaluate(&database, "apoc.coll.flatten(null)"),
            json!([null])
        );
        assert_eq!(evaluate(&database, "apoc.coll.toSet(null)"), json!([]));
        assert_eq!(evaluate(&database, "apoc.map.merge({a: 1})"), json!({}));
        assert_eq!(
            evaluate(&database, "apoc.convert.fromJsonMap('[1, 2]')"),
            Value::Null
        );
    }

    #[test]
    fn subgraph_nodes_matches_bounded_nornicdb_bfs_filters() {
        let database = database();
        seed_traversal(&database);

        assert_eq!(
            traverse(
                &database,
                "a",
                "{relationshipFilter: '>KNOWS', maxLevel: 2}"
            ),
            ["a", "b", "d"]
        );
        assert_eq!(
            traverse(
                &database,
                "d",
                "{relationshipFilter: '<KNOWS', maxLevel: 2}"
            ),
            ["d", "b", "a"]
        );
        assert_eq!(
            traverse(
                &database,
                "a",
                "{relationshipFilter: '>KNOWS', maxLevel: 4, labelFilter: '-Hidden|/Stop'}"
            ),
            ["a", "d"]
        );
        assert_eq!(
            traverse(
                &database,
                "a",
                "{relationshipFilter: '>KNOWS', maxLevel: 4, limit: 2}"
            ),
            ["a", "b"]
        );
    }

    #[test]
    fn subgraph_nodes_handles_optional_config_and_invalid_start() {
        let database = database();
        seed_traversal(&database);
        let default_result = database
            .execute(
                "CALL apoc.path.subgraphNodes($start) YIELD node RETURN node._id AS id",
                HashMap::from([("start".into(), json!({"_id": "a"}))]),
            )
            .unwrap();
        assert_eq!(default_result.rows.len(), 5);

        let null_result = database
            .execute(
                "CALL apoc.path.subgraphNodes(null, {}) YIELD node RETURN node",
                HashMap::new(),
            )
            .unwrap();
        assert!(null_result.rows.is_empty());

        let arity_error = database
            .execute("CALL apoc.path.subgraphNodes()", HashMap::new())
            .unwrap_err();
        assert!(arity_error
            .to_string()
            .contains("expects one or two arguments"));
    }
}
