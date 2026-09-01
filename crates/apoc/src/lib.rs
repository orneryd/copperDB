//! Representative APOC package loaded through `copperdb-plugin`.

use copperdb_filter::{FunctionDescriptor, FunctionHandler};
use copperdb_plugin::{PackageDefinition, PackageDescriptor};
use semver::Version;
use serde_json::{Map, Number, Value};
use std::collections::HashSet;
use std::sync::Arc;

pub const PACKAGE_ID: &str = "copperdb.apoc";

pub fn package() -> PackageDefinition {
    let descriptor =
        PackageDescriptor::new(PACKAGE_ID, Version::new(1, 0, 0), "copperdb contributors");
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

#[cfg(test)]
mod tests {
    use super::*;
    use copperdb_engine::{CopperDb, DatabaseConfig};
    use copperdb_plugin::resolve_packages;
    use copperdb_storage::StorageEngine;
    use serde_json::json;
    use std::collections::HashMap;

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
}
