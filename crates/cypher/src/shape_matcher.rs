use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShapeKind {
    Unknown,
    CompoundCreateDeleteRel,
    CompoundPropCreateDeleteRel,
    CompoundPropCreateDeleteReturnCountRel,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShapeProbe {
    pub matcher: String,
    pub matched: bool,
    pub reject_reason: String,
    pub normalized_query: String,
    pub captured_fields: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShapeValue {
    String(String),
    Int(i64),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShapeCaptures {
    pub ordered: Vec<(String, ShapeValue)>,
    pub by_name: HashMap<String, ShapeValue>,
}

impl ShapeCaptures {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_string(&mut self, name: &str, value: impl Into<String>) {
        let value = ShapeValue::String(value.into());
        self.ordered.push((name.to_string(), value.clone()));
        self.by_name.insert(name.to_string(), value);
    }

    pub fn add_int(&mut self, name: &str, value: i64) {
        let value = ShapeValue::Int(value);
        self.ordered.push((name.to_string(), value.clone()));
        self.by_name.insert(name.to_string(), value);
    }

    pub fn string(&self, name: &str) -> String {
        match self.by_name.get(name) {
            Some(ShapeValue::String(value)) => value.clone(),
            _ => String::new(),
        }
    }

    pub fn int(&self, name: &str) -> i64 {
        match self.by_name.get(name) {
            Some(ShapeValue::Int(value)) => *value,
            _ => 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShapeMatch {
    pub kind: ShapeKind,
    pub captures: ShapeCaptures,
    pub probe: ShapeProbe,
}

impl ShapeMatch {
    pub fn unknown(matcher: &str, query: &str, reject_reason: &str) -> Self {
        Self {
            kind: ShapeKind::Unknown,
            captures: ShapeCaptures::new(),
            probe: ShapeProbe {
                matcher: matcher.to_string(),
                matched: false,
                reject_reason: reject_reason.to_string(),
                normalized_query: query.trim().to_string(),
                captured_fields: HashMap::new(),
            },
        }
    }
}

pub fn pending_shape_execution_todo(kind: ShapeKind) -> Option<&'static str> {
    match kind {
        ShapeKind::Unknown => None,
        _ => Some(
            "TODO(eval/engine): route parser-only compound query-shape matches into the eventual fast-path executor parity slice.",
        ),
    }
}
