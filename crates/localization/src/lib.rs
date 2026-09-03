//! Protocol-neutral localization, stable diagnostics, and locale negotiation.

rust_i18n::i18n!("locales", fallback = "en-US");

use fluent_langneg::{negotiate_languages, NegotiationStrategy};
use icu_locid::{LanguageIdentifier, Locale};
use icu_plurals::{PluralCategory, PluralRules};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::env;
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

pub const ENV_LANGUAGE: &str = "COPPERDB_LANGUAGE";
pub const AUTO_LANGUAGE: &str = "auto";
pub const SOURCE_LANGUAGE: &str = "en-US";
const MAX_WARNING_KEYS: usize = 1_024;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LanguageTag(String);

impl LanguageTag {
    pub fn parse(value: &str) -> Result<Option<Self>, LocaleError> {
        let mut value = value.trim();
        if value.is_empty()
            || value.eq_ignore_ascii_case(AUTO_LANGUAGE)
            || matches!(
                value.to_ascii_uppercase().as_str(),
                "C" | "POSIX" | "C.UTF-8" | "C.UTF8"
            )
        {
            return Ok(None);
        }
        if let Some(index) = value.find(['.', '@']) {
            value = &value[..index];
        }
        let normalized = value.replace('_', "-");
        let locale = normalized
            .parse::<Locale>()
            .map_err(|_| LocaleError::InvalidLanguage(value.to_string()))?;
        Ok(Some(Self(locale.id.to_string())))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LanguageTag {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LocaleError {
    #[error("invalid language {0:?}")]
    InvalidLanguage(String),
    #[error("{0}: {1}")]
    InvalidConfiguredLanguage(&'static str, Box<LocaleError>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreferenceSource {
    Environment,
    Config,
    Os,
    Fallback,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessPreferences {
    pub preferences: Vec<LanguageTag>,
    pub source: PreferenceSource,
    pub detection_error: Option<String>,
}

pub fn resolve_process_preferences(
    configured: &str,
    detect_os: impl FnOnce() -> Result<Vec<String>, String>,
) -> Result<ProcessPreferences, LocaleError> {
    resolve_preferences_with(configured, |key| env::var(key).ok(), detect_os)
}

pub fn resolve_preferences_with(
    configured: &str,
    lookup_env: impl FnOnce(&str) -> Option<String>,
    detect_os: impl FnOnce() -> Result<Vec<String>, String>,
) -> Result<ProcessPreferences, LocaleError> {
    if let Some(value) = lookup_env(ENV_LANGUAGE) {
        if let Some(tag) = parse_explicit(&value, ENV_LANGUAGE)? {
            return Ok(ProcessPreferences {
                preferences: vec![tag],
                source: PreferenceSource::Environment,
                detection_error: None,
            });
        }
        return detect_or_fallback(detect_os);
    }
    if let Some(tag) = parse_explicit(configured, "localization.language")? {
        return Ok(ProcessPreferences {
            preferences: vec![tag],
            source: PreferenceSource::Config,
            detection_error: None,
        });
    }
    detect_or_fallback(detect_os)
}

fn parse_explicit(value: &str, source: &'static str) -> Result<Option<LanguageTag>, LocaleError> {
    LanguageTag::parse(value)
        .map_err(|error| LocaleError::InvalidConfiguredLanguage(source, Box::new(error)))
}

fn detect_or_fallback(
    detect_os: impl FnOnce() -> Result<Vec<String>, String>,
) -> Result<ProcessPreferences, LocaleError> {
    match detect_os() {
        Ok(values) => {
            let preferences = values
                .iter()
                .filter_map(|value| LanguageTag::parse(value).transpose())
                .collect::<Result<Vec<_>, _>>()?;
            if !preferences.is_empty() {
                return Ok(ProcessPreferences {
                    preferences,
                    source: PreferenceSource::Os,
                    detection_error: None,
                });
            }
            Ok(fallback(Some(
                "operating system language was not detected".into(),
            )))
        }
        Err(error) => Ok(fallback(Some(error))),
    }
}

fn fallback(detection_error: Option<String>) -> ProcessPreferences {
    ProcessPreferences {
        preferences: vec![LanguageTag(SOURCE_LANGUAGE.into())],
        source: PreferenceSource::Fallback,
        detection_error,
    }
}

pub fn parse_accept_language(value: &str) -> Vec<LanguageTag> {
    if value.trim().is_empty() {
        return Vec::new();
    }
    let parsed = value
        .split(',')
        .enumerate()
        .map(|(index, item)| parse_language_range(item, index))
        .collect::<Result<Vec<_>, _>>();
    let Ok(mut preferences) = parsed else {
        return Vec::new();
    };
    preferences.retain(|(_, quality, _)| *quality > 0);
    preferences.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.2.cmp(&right.2)));
    preferences.into_iter().map(|(tag, _, _)| tag).collect()
}

fn parse_language_range(
    item: &str,
    index: usize,
) -> Result<(LanguageTag, u16, usize), LocaleError> {
    let mut fields = item.trim().split(';');
    let raw_tag = fields.next().unwrap_or_default().trim();
    let tag = if raw_tag == "*" {
        LanguageTag("*".into())
    } else {
        LanguageTag::parse(raw_tag)?.ok_or_else(|| LocaleError::InvalidLanguage(raw_tag.into()))?
    };
    let quality = match fields.next() {
        None => 1_000,
        Some(parameter) => parse_quality(parameter.trim())?,
    };
    if fields.next().is_some() {
        return Err(LocaleError::InvalidLanguage(item.into()));
    }
    Ok((tag, quality, index))
}

fn parse_quality(parameter: &str) -> Result<u16, LocaleError> {
    let Some((name, value)) = parameter.split_once('=') else {
        return Err(LocaleError::InvalidLanguage(parameter.into()));
    };
    if !name.trim().eq_ignore_ascii_case("q") {
        return Err(LocaleError::InvalidLanguage(parameter.into()));
    }
    let value = value.trim();
    let valid = value == "0"
        || value == "1"
        || value.strip_prefix("0.").is_some_and(|fraction| {
            fraction.len() <= 3 && fraction.chars().all(|digit| digit.is_ascii_digit())
        })
        || value.strip_prefix("1.").is_some_and(|fraction| {
            fraction.len() <= 3 && fraction.chars().all(|digit| digit == '0')
        });
    if !valid {
        return Err(LocaleError::InvalidLanguage(parameter.into()));
    }
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    let mut thousandths = fraction.parse::<u16>().unwrap_or(0);
    for _ in fraction.len()..3 {
        thousandths *= 10;
    }
    Ok(if whole == "1" { 1_000 } else { thousandths })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocaleMatch {
    pub requested: LanguageTag,
    pub resolved: LanguageTag,
    pub exact: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    String(String),
    Number(i64),
    Bool(bool),
    Decimal(String),
    Bytes(Vec<u8>),
}

impl fmt::Display for Value {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(value) => formatter.write_str(value),
            Self::Number(value) => value.fmt(formatter),
            Self::Bool(value) => value.fmt(formatter),
            Self::Decimal(value) => formatter.write_str(value),
            Self::Bytes(value) => formatter.write_str(&String::from_utf8_lossy(value)),
        }
    }
}

impl Value {
    fn truthy(&self) -> bool {
        match self {
            Self::String(value) | Self::Decimal(value) => !value.is_empty(),
            Self::Number(value) => *value != 0,
            Self::Bool(value) => *value,
            Self::Bytes(value) => !value.is_empty(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub id: &'static str,
    pub fallback: &'static str,
    pub data: BTreeMap<&'static str, Value>,
    pub plural_count: Option<i64>,
}

impl Message {
    pub fn new(id: &'static str, fallback: &'static str) -> Self {
        Self {
            id,
            fallback,
            data: BTreeMap::new(),
            plural_count: None,
        }
    }

    pub fn from_catalog(id: &'static str) -> Option<Self> {
        let index = CATALOG_INVENTORY
            .binary_search_by_key(&(id, SOURCE_LANGUAGE), |entry| (entry.id, entry.language))
            .ok()?;
        Some(Self::new(id, CATALOG_INVENTORY[index].other))
    }

    pub fn with(mut self, key: &'static str, value: impl Into<String>) -> Self {
        self.data.insert(key, Value::String(value.into()));
        self
    }

    pub fn with_bool(mut self, key: &'static str, value: bool) -> Self {
        self.data.insert(key, Value::Bool(value));
        self
    }

    pub fn with_number(mut self, key: &'static str, value: i64) -> Self {
        self.data.insert(key, Value::Number(value));
        self
    }

    pub fn with_decimal(mut self, key: &'static str, value: impl ToString) -> Self {
        self.data.insert(key, Value::Decimal(value.to_string()));
        self
    }

    pub fn with_bytes(mut self, key: &'static str, value: impl Into<Vec<u8>>) -> Self {
        self.data.insert(key, Value::Bytes(value.into()));
        self
    }

    pub fn plural(mut self, count: i64) -> Self {
        self.data.insert("Count", Value::Number(count));
        self.plural_count = Some(count);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedMessage {
    pub text: String,
    pub language: LanguageTag,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RenderError {
    #[error("source catalog entry {0} is missing")]
    MissingSource(String),
    #[error("message {message_id} is missing placeholder {placeholder}")]
    MissingPlaceholder {
        message_id: String,
        placeholder: String,
    },
    #[error("message {0} contains an unsupported template action")]
    InvalidTemplate(String),
}

#[derive(Debug)]
pub struct Manager {
    default: LanguageTag,
    warned: Mutex<HashSet<String>>,
    missing_entries: AtomicU64,
}

impl Manager {
    pub fn new(default_preferences: &[LanguageTag]) -> Self {
        let default = match_preferences(default_preferences).resolved;
        Self {
            default,
            warned: Mutex::new(HashSet::new()),
            missing_entries: AtomicU64::new(0),
        }
    }

    pub fn supported_languages() -> Vec<LanguageTag> {
        available_languages()
    }

    pub fn default_language(&self) -> &LanguageTag {
        &self.default
    }

    pub fn resolve(&self, source: &str, preferences: &[LanguageTag]) -> LocaleMatch {
        let matched = match_preferences(preferences);
        if !matched.exact {
            self.warn_once(
                format!("pack:{source}:{}:{}", matched.requested, matched.resolved),
                "localization.language_pack_missing",
            );
        }
        matched
    }

    pub fn render(
        &self,
        preferences: &[LanguageTag],
        message: &Message,
    ) -> Result<RenderedMessage, RenderError> {
        let effective_preferences = if preferences.is_empty() {
            std::slice::from_ref(&self.default)
        } else {
            preferences
        };
        let matched = self.resolve("render", effective_preferences);
        let (template, language) = match catalog_entry(matched.resolved.as_str(), message) {
            Some(template) => (template, matched.resolved),
            None => {
                self.missing_entries.fetch_add(1, Ordering::Relaxed);
                self.warn_once(
                    format!("message:{}:{}", matched.resolved, message.id),
                    "localization.catalog_entry_missing",
                );
                let template = catalog_entry(SOURCE_LANGUAGE, message)
                    .ok_or_else(|| RenderError::MissingSource(message.id.into()))?;
                (template, LanguageTag(SOURCE_LANGUAGE.into()))
            }
        };
        Ok(RenderedMessage {
            text: interpolate(message.id, &template, &message.data)?,
            language,
        })
    }

    pub fn render_display(
        &self,
        preferences: &[LanguageTag],
        display: &dyn fmt::Display,
    ) -> Option<RenderedMessage> {
        let message = message_for_display(&display.to_string())?;
        self.render(preferences, &message).ok()
    }

    pub fn missing_catalog_entry_count(&self) -> u64 {
        self.missing_entries.load(Ordering::Relaxed)
    }

    pub fn warning_count(&self) -> usize {
        self.warned
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .len()
    }

    fn warn_once(&self, key: String, event_id: &'static str) {
        let mut warned = self
            .warned
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if warned.len() >= MAX_WARNING_KEYS {
            return;
        }
        let inserted = warned.insert(key.clone());
        drop(warned);
        if inserted {
            tracing::warn!(event_id, warning_key = %key, "localization fallback");
        }
    }
}

fn available_languages() -> Vec<LanguageTag> {
    _rust_i18n_available_locales()
        .into_iter()
        .filter_map(|language| LanguageTag::parse(&language).ok().flatten())
        .collect()
}

fn match_preferences(preferences: &[LanguageTag]) -> LocaleMatch {
    let available = available_languages();
    let requested = preferences
        .first()
        .cloned()
        .unwrap_or_else(|| LanguageTag(SOURCE_LANGUAGE.into()));
    let wildcard = preferences
        .iter()
        .position(|preference| preference.as_str() == "*");
    let effective = wildcard.map_or(preferences, |index| &preferences[..index]);
    let requested_ids = effective
        .iter()
        .filter_map(|preference| preference.as_str().parse::<LanguageIdentifier>().ok())
        .collect::<Vec<_>>();
    let available_ids = available
        .iter()
        .filter_map(|language| language.as_str().parse::<LanguageIdentifier>().ok())
        .collect::<Vec<_>>();
    let source_id = SOURCE_LANGUAGE.parse::<LanguageIdentifier>().unwrap();
    let resolved_id = negotiate_languages(
        &requested_ids,
        &available_ids,
        Some(&source_id),
        NegotiationStrategy::Lookup,
    )
    .into_iter()
    .next()
    .unwrap_or(&source_id);
    let resolved = LanguageTag(resolved_id.to_string());
    let exact = requested == resolved;
    LocaleMatch {
        requested,
        resolved,
        exact,
    }
}

fn catalog_entry(language: &str, message: &Message) -> Option<Cow<'static, str>> {
    let plural_form = message
        .plural_count
        .and_then(|count| plural_form(language, count));
    if let Some(form) = plural_form.filter(|form| *form != "other") {
        let plural_key = format!("{}.{form}", message.id);
        if let Some(value) = _rust_i18n_try_translate(language, plural_key) {
            return Some(value);
        }
    }
    if let Some(value) = _rust_i18n_try_translate(language, message.id) {
        return Some(value);
    }
    if let Ok(index) = CATALOG_INVENTORY
        .binary_search_by_key(&(message.id, language), |entry| (entry.id, entry.language))
    {
        let entry = CATALOG_INVENTORY[index];
        return Some(Cow::Borrowed(if plural_form == Some("one") {
            entry.one.unwrap_or(entry.other)
        } else {
            entry.other
        }));
    }
    if language == SOURCE_LANGUAGE && !message.fallback.is_empty() {
        return Some(Cow::Borrowed(message.fallback));
    }
    None
}

fn plural_form(language: &str, count: i64) -> Option<&'static str> {
    let locale = language.parse::<Locale>().ok()?;
    let rules = PluralRules::try_new_cardinal(&locale.into()).ok()?;
    Some(match rules.category_for(count) {
        PluralCategory::Zero => "zero",
        PluralCategory::One => "one",
        PluralCategory::Two => "two",
        PluralCategory::Few => "few",
        PluralCategory::Many => "many",
        PluralCategory::Other => "other",
    })
}

fn interpolate(
    message_id: &str,
    template: &str,
    data: &BTreeMap<&'static str, Value>,
) -> Result<String, RenderError> {
    let (output, _, stop) = render_template_segment(message_id, template, data, 0, &[])?;
    debug_assert!(stop.is_none());
    Ok(output)
}

fn render_template_segment(
    message_id: &str,
    template: &str,
    data: &BTreeMap<&'static str, Value>,
    mut position: usize,
    stops: &[&str],
) -> Result<(String, usize, Option<String>), RenderError> {
    let mut output = String::new();
    while let Some(relative_start) = template[position..].find("{{") {
        let start = position + relative_start;
        output.push_str(&template[position..start]);
        let action_start = start + 2;
        let Some(relative_end) = template[action_start..].find("}}") else {
            return Err(RenderError::InvalidTemplate(message_id.into()));
        };
        let end = action_start + relative_end;
        let action = template[action_start..end].trim();
        position = end + 2;

        if stops.contains(&action) {
            return Ok((output, position, Some(action.to_string())));
        }
        if let Some(field) = action.strip_prefix("if .") {
            let value = template_value(message_id, field.trim(), data)?;
            let (truthy, next, stop) =
                render_template_segment(message_id, template, data, position, &["else", "end"])?;
            position = next;
            let mut falsy = String::new();
            if stop.as_deref() == Some("else") {
                let (rendered, next, stop) =
                    render_template_segment(message_id, template, data, position, &["end"])?;
                if stop.as_deref() != Some("end") {
                    return Err(RenderError::InvalidTemplate(message_id.into()));
                }
                falsy = rendered;
                position = next;
            } else if stop.as_deref() != Some("end") {
                return Err(RenderError::InvalidTemplate(message_id.into()));
            }
            output.push_str(if value.truthy() { &truthy } else { &falsy });
            continue;
        }
        if let Some(printf) = action.strip_prefix("printf ") {
            output.push_str(&render_printf(message_id, printf, data)?);
            continue;
        }
        if let Some(field) = action.strip_prefix('.') {
            output.push_str(&template_value(message_id, field.trim(), data)?.to_string());
            continue;
        }
        return Err(RenderError::InvalidTemplate(message_id.into()));
    }
    output.push_str(&template[position..]);
    Ok((output, template.len(), None))
}

fn template_value<'a>(
    message_id: &str,
    field: &str,
    data: &'a BTreeMap<&'static str, Value>,
) -> Result<&'a Value, RenderError> {
    data.get(field)
        .ok_or_else(|| RenderError::MissingPlaceholder {
            message_id: message_id.into(),
            placeholder: field.into(),
        })
}

fn render_printf(
    message_id: &str,
    action: &str,
    data: &BTreeMap<&'static str, Value>,
) -> Result<String, RenderError> {
    let Some(action) = action.strip_prefix('"') else {
        return Err(RenderError::InvalidTemplate(message_id.into()));
    };
    let Some((format, field)) = action.split_once("\" .") else {
        return Err(RenderError::InvalidTemplate(message_id.into()));
    };
    let value = template_value(message_id, field.trim(), data)?;
    match format {
        "%q" => Ok(format!("{:?}", value.to_string())),
        "%x" => match value {
            Value::Bytes(bytes) => Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect()),
            Value::String(value) => Ok(value
                .as_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect()),
            _ => Err(RenderError::InvalidTemplate(message_id.into())),
        },
        "%-20s" => Ok(format!("{:<20}", value.to_string())),
        "%.6f" => value
            .to_string()
            .parse::<f64>()
            .map(|value| format!("{value:.6}"))
            .map_err(|_| RenderError::InvalidTemplate(message_id.into())),
        _ => Err(RenderError::InvalidTemplate(message_id.into())),
    }
}

pub struct LocalizedError {
    code: &'static str,
    message: Message,
    cause: Option<Box<dyn Error + Send + Sync>>,
}

impl LocalizedError {
    pub fn new(
        code: &'static str,
        message: Message,
        cause: Option<Box<dyn Error + Send + Sync>>,
    ) -> Self {
        Self {
            code,
            message,
            cause,
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &Message {
        &self.message
    }

    pub fn render(
        &self,
        manager: &Manager,
        preferences: &[LanguageTag],
    ) -> Result<RenderedMessage, RenderError> {
        manager.render(preferences, &self.message)
    }
}

impl fmt::Debug for LocalizedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalizedError")
            .field("code", &self.code)
            .field("message", &self.message)
            .field("cause", &self.cause.as_ref().map(|cause| cause.to_string()))
            .finish()
    }
}

impl fmt::Display for LocalizedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.fallback.is_empty() {
            formatter.write_str(self.message.id)
        } else {
            formatter.write_str(self.message.fallback)
        }
    }
}

impl Error for LocalizedError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.cause
            .as_deref()
            .map(|cause| cause as &(dyn Error + 'static))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticEvent {
    pub id: &'static str,
    pub message: Message,
    pub fields: BTreeMap<&'static str, Value>,
}

pub trait StableLocalizedDiagnostic {
    fn diagnostic_id(&self) -> &'static str;
    fn localized_message(&self) -> Message;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CatalogInventoryEntry {
    pub id: &'static str,
    pub language: &'static str,
    pub one: Option<&'static str>,
    pub other: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProcedureMetadataEntry {
    pub name: &'static str,
    pub en: &'static str,
    pub es: &'static str,
}

include!("generated_catalog.rs");

pub fn procedure_description(name: &str, language: &LanguageTag) -> Option<&'static str> {
    let index = PROCEDURE_METADATA
        .binary_search_by_key(&name, |entry| entry.name)
        .ok()?;
    let entry = PROCEDURE_METADATA[index];
    let source = CATALOG_INVENTORY.iter().find(|catalog| {
        catalog.language == SOURCE_LANGUAGE
            && catalog.id.starts_with("cypherproceduremetadata.")
            && catalog.other == entry.en
    })?;
    CATALOG_INVENTORY
        .binary_search_by_key(&(source.id, language.as_str()), |catalog| {
            (catalog.id, catalog.language)
        })
        .ok()
        .map(|localized| CATALOG_INVENTORY[localized].other)
        .or(Some(source.other))
}

pub fn matching_procedure_description<'a>(
    name: &str,
    source_description: &'a str,
    language: &LanguageTag,
) -> &'a str {
    let Ok(index) = PROCEDURE_METADATA.binary_search_by_key(&name, |entry| entry.name) else {
        return source_description;
    };
    let entry = PROCEDURE_METADATA[index];
    if entry.en != source_description {
        return source_description;
    }
    procedure_description(name, language).unwrap_or(source_description)
}

pub fn message_for_display(display: &str) -> Option<Message> {
    let source_entries = CATALOG_INVENTORY
        .iter()
        .filter(|entry| entry.language == SOURCE_LANGUAGE);
    if let Some(entry) = source_entries.clone().find(|entry| entry.other == display) {
        return Some(Message::new(entry.id, entry.other));
    }

    let mut best = None;
    for entry in source_entries {
        if let Some(data) = match_template(entry.other, display) {
            let specificity = literal_template_bytes(entry.other);
            if best
                .as_ref()
                .is_none_or(|(best_specificity, _): &(usize, Message)| {
                    specificity > *best_specificity
                })
            {
                best = Some((
                    specificity,
                    Message {
                        id: entry.id,
                        fallback: entry.other,
                        data,
                        plural_count: None,
                    },
                ));
            }
        }
    }
    best.map(|(_, message)| message)
}

fn literal_template_bytes(template: &str) -> usize {
    let mut literal_bytes = 0;
    let mut remaining = template;
    while let Some(start) = remaining.find("{{") {
        literal_bytes += start;
        let tail = &remaining[start + 2..];
        let Some(end) = tail.find("}}") else {
            return literal_bytes;
        };
        remaining = &tail[end + 2..];
    }
    literal_bytes + remaining.len()
}

fn match_template(template: &'static str, display: &str) -> Option<BTreeMap<&'static str, Value>> {
    if !template.contains("{{") {
        return (template == display).then(BTreeMap::new);
    }
    if template.contains("{{if ") || template.contains("{{end}}") {
        return None;
    }

    let mut data = BTreeMap::new();
    let mut template_rest = template;
    let mut display_rest = display;
    while let Some(start) = template_rest.find("{{") {
        let literal = &template_rest[..start];
        display_rest = display_rest.strip_prefix(literal)?;
        let field_tail = &template_rest[start + 2..];
        let field_end = field_tail.find("}}")?;
        let field = field_tail[..field_end].trim().trim_start_matches('.');
        if field.is_empty() {
            return None;
        }
        template_rest = &field_tail[field_end + 2..];
        let next_literal_end = template_rest.find("{{").unwrap_or(template_rest.len());
        let next_literal = &template_rest[..next_literal_end];
        let value_end = if next_literal.is_empty() {
            display_rest.len()
        } else {
            display_rest.find(next_literal)?
        };
        data.insert(field, Value::String(display_rest[..value_end].to_string()));
        display_rest = &display_rest[value_end..];
    }
    display_rest
        .strip_prefix(template_rest)
        .filter(|remainder| remainder.is_empty())
        .map(|_| data)
}

pub fn validate_catalog_inventory(entries: &[CatalogInventoryEntry]) -> Result<(), String> {
    let mut seen = HashSet::new();
    let mut by_id: BTreeMap<&str, Vec<&CatalogInventoryEntry>> = BTreeMap::new();
    for entry in entries {
        if !seen.insert((entry.id, entry.language)) {
            return Err(format!(
                "duplicate catalog entry {} {}",
                entry.id, entry.language
            ));
        }
        by_id.entry(entry.id).or_default().push(entry);
    }
    let required_languages = entries
        .iter()
        .map(|entry| entry.language)
        .collect::<BTreeSet<_>>();
    for (id, localized) in by_id {
        for language in &required_languages {
            if !localized.iter().any(|entry| entry.language == *language) {
                return Err(format!("catalog entry {id} is missing {language}"));
            }
        }
        let source = localized
            .iter()
            .find(|entry| entry.language == SOURCE_LANGUAGE)
            .unwrap();
        let source_other = placeholders(source.other);
        let source_one = source.one.map(placeholders);
        for entry in &localized {
            if placeholders(entry.other) != source_other {
                return Err(format!(
                    "placeholder mismatch for {id} {} other",
                    entry.language
                ));
            }
            if entry.one.map(placeholders) != source_one {
                return Err(format!(
                    "plural or placeholder mismatch for {id} {} one",
                    entry.language
                ));
            }
            if entry.language == "en-XA" {
                let starts_envelope = entry.other.starts_with("[!! ");
                let ends_envelope = entry.other.trim_end().ends_with(" !!]");
                if starts_envelope != ends_envelope {
                    return Err(format!("invalid pseudo-locale envelope for {id}"));
                }
                if id.starts_with("cypherproceduremetadata.")
                    && (!starts_envelope || entry.other == source.other)
                {
                    return Err(format!("procedure metadata {id} is not pseudo-localized"));
                }
            }
        }
    }
    Ok(())
}

fn placeholders(template: &str) -> Vec<&str> {
    let mut remaining = template;
    let mut found = Vec::new();
    while let Some(start) = remaining.find("{{") {
        let after_start = &remaining[start + 2..];
        let Some(end) = after_start.find("}}") else {
            break;
        };
        found.push(after_start[..end].trim().trim_start_matches('.'));
        remaining = &after_start[end + 2..];
    }
    found.sort_unstable();
    found
}

pub mod messages {
    use super::Message;

    pub fn invalid_request_body() -> Message {
        Message::new("server.invalid_request_body", "invalid request body")
    }

    pub fn not_authenticated() -> Message {
        Message::new("security.not_authenticated", "Not authenticated")
    }

    pub fn mcp_parse_error() -> Message {
        Message::new("mcp.parse_error", "Parse error")
    }

    pub fn bolt_authentication_required() -> Message {
        Message::new("bolt.authentication_required", "Authentication required")
    }

    pub fn bolt_no_transaction_to_commit() -> Message {
        Message::new("bolt.no_transaction_to_commit", "No transaction to commit")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag(value: &str) -> LanguageTag {
        LanguageTag::parse(value).unwrap().unwrap()
    }

    #[test]
    fn normalizes_bcp47_and_posix_forms() {
        assert_eq!(tag("en_US.UTF-8").as_str(), "en-US");
        assert_eq!(tag("zh_hant_tw@calendar=roc").as_str(), "zh-Hant-TW");
        for neutral in ["", "auto", "C", "POSIX", "C.UTF-8"] {
            assert_eq!(LanguageTag::parse(neutral).unwrap(), None, "{neutral}");
        }
        assert!(LanguageTag::parse("not_a_locale_@").is_err());
    }

    #[test]
    fn resolves_environment_config_os_and_fallback_precedence() {
        let env = resolve_preferences_with(
            "fr-FR",
            |_| Some("es_ES.UTF-8".into()),
            || panic!("OS detection must not run"),
        )
        .unwrap();
        assert_eq!(env.source, PreferenceSource::Environment);
        assert_eq!(env.preferences, vec![tag("es-ES")]);

        let config =
            resolve_preferences_with("fr-FR", |_| None, || panic!("OS detection must not run"))
                .unwrap();
        assert_eq!(config.source, PreferenceSource::Config);

        let os = resolve_preferences_with("auto", |_| None, || Ok(vec!["de-DE".into()])).unwrap();
        assert_eq!(os.source, PreferenceSource::Os);
        assert_eq!(os.preferences, vec![tag("de-DE")]);

        let fallback =
            resolve_preferences_with("auto", |_| None, || Err("unavailable".into())).unwrap();
        assert_eq!(fallback.source, PreferenceSource::Fallback);
        assert_eq!(fallback.preferences, vec![tag("en-US")]);
        assert_eq!(fallback.detection_error.as_deref(), Some("unavailable"));
    }

    #[test]
    fn parses_weighted_ordered_request_preferences() {
        assert_eq!(
            parse_accept_language("fr-FR;q=0.2, es-ES, en;q=0.5, de;q=0"),
            vec![tag("es-ES"), tag("en"), tag("fr-FR")]
        );
        assert_eq!(
            parse_accept_language("zh-hant-tw;Q=0.7, en-US;q=0.700"),
            vec![tag("zh-Hant-TW"), tag("en-US")]
        );
        for invalid in [
            "es-ES;q=banana,en-US",
            "es-ES;q=1.1,en-US",
            "es-ES;q=-0.1,en-US",
            "es-ES;q=0.1234,en-US",
            "es-ES;level=1,en-US",
            "es-ES;q=0.5;foo=bar,en-US",
            "es--ES,en-US",
        ] {
            assert!(parse_accept_language(invalid).is_empty(), "{invalid}");
        }
    }

    #[test]
    fn negotiates_installed_locales_with_scripts_and_wildcards() {
        assert_eq!(match_preferences(&[tag("es-MX")]).resolved, tag("es-ES"));
        assert_eq!(
            match_preferences(&[tag("zh-Hant-TW"), tag("es-MX")]).resolved,
            tag("es-ES")
        );
        assert_eq!(
            match_preferences(&parse_accept_language("fr-FR, *;q=0.1")).resolved,
            tag("en-US")
        );
    }

    #[test]
    fn renders_catalog_plural_pseudo_and_source_fallback() {
        let manager = Manager::new(&[tag("en-US")]);
        assert_eq!(
            Manager::supported_languages(),
            [tag("en-US"), tag("en-XA"), tag("es-ES")]
        );
        assert_eq!(
            manager
                .render(&[tag("es-ES")], &messages::invalid_request_body())
                .unwrap(),
            RenderedMessage {
                text: "cuerpo de solicitud no válido".into(),
                language: tag("es-ES")
            }
        );
        assert_eq!(
            manager
                .render(&[tag("en-XA")], &messages::invalid_request_body())
                .unwrap()
                .text,
            "[!! invalid request body !!]"
        );
        let external = Message::new("test.english_only", "English source fallback");
        let rendered = manager.render(&[tag("es-ES")], &external).unwrap();
        assert_eq!(rendered.text, "English source fallback");
        assert_eq!(rendered.language, tag("en-US"));
        manager.render(&[tag("es-ES")], &external).unwrap();
        assert_eq!(manager.missing_catalog_entry_count(), 2);
        assert_eq!(manager.warning_count(), 1);
    }

    #[test]
    fn rust_i18n_catalog_selects_plural_variants() {
        let manager = Manager::new(&[tag("en-US")]);
        let singular = Message::from_catalog("localization.items_processed")
            .unwrap()
            .plural(1);
        let plural = Message::from_catalog("localization.items_processed")
            .unwrap()
            .plural(2);

        assert_eq!(
            manager.render(&[tag("es-ES")], &singular).unwrap().text,
            "1 elemento procesado"
        );
        assert_eq!(
            manager.render(&[tag("es-ES")], &plural).unwrap().text,
            "2 elementos procesados"
        );

        assert_eq!(plural_form("pl-PL", 1), Some("one"));
        assert_eq!(plural_form("pl-PL", 2), Some("few"));
        assert_eq!(plural_form("pl-PL", 5), Some("many"));
        assert_eq!(plural_form("en-US", 2), Some("other"));
    }

    #[test]
    fn renders_imported_go_template_subset() {
        let data = BTreeMap::from([
            ("Cause", Value::String("disk unavailable".into())),
            ("DeletesFailed", Value::Number(3)),
            ("EdgesFailed", Value::Number(2)),
            ("HasFlushErrors", Value::Bool(true)),
            ("HasUnflushed", Value::Bool(true)),
            ("NodesFailed", Value::Number(1)),
            ("PendingEdgeDeletes", Value::Number(7)),
            ("PendingEdges", Value::Number(5)),
            ("PendingNodeDeletes", Value::Number(6)),
            ("PendingNodes", Value::Number(4)),
        ]);
        let nested = "{{if .HasFlushErrors}}flush errors: {{.NodesFailed}} nodes failed, {{.EdgesFailed}} edges failed, {{.DeletesFailed}} deletes failed{{if .HasUnflushed}}; {{end}}{{end}}{{if .HasUnflushed}}unflushed: {{.PendingNodes}} nodes, {{.PendingEdges}} edges, {{.PendingNodeDeletes}} node deletes, {{.PendingEdgeDeletes}} edge deletes (POTENTIAL DATA LOSS){{end}}; engine close: {{.Cause}}";
        assert_eq!(
            interpolate("storage.client.async.close_engine_failed", nested, &data).unwrap(),
            "flush errors: 1 nodes failed, 2 edges failed, 3 deletes failed; unflushed: 4 nodes, 5 edges, 6 node deletes, 7 edge deletes (POTENTIAL DATA LOSS); engine close: disk unavailable"
        );

        assert_eq!(
            interpolate(
                "test.printf",
                "{{printf \"%q\" .Quoted}} {{printf \"%x\" .Prefix}} |{{printf \"%-20s\" .Label}}| {{printf \"%.6f\" .Value}}",
                &BTreeMap::from([
                    ("Label", Value::String("Person".into())),
                    ("Prefix", Value::Bytes(vec![0, 15, 255])),
                    ("Quoted", Value::String("a\"b".into())),
                    ("Value", Value::Decimal("0.125".into())),
                ]),
            )
            .unwrap(),
            "\"a\\\"b\" 000fff |Person              | 0.125000"
        );

        let conditional = "{{if .Parentheses}}invalid syntax: missing parentheses{{else}}invalid procedure syntax{{end}}";
        assert_eq!(
            interpolate(
                "test.conditional",
                conditional,
                &BTreeMap::from([("Parentheses", Value::Bool(false))]),
            )
            .unwrap(),
            "invalid procedure syntax"
        );
    }

    #[test]
    fn explicit_locales_are_isolated_across_threads() {
        let manager = std::sync::Arc::new(Manager::new(&[tag("en-US")]));
        let english = {
            let manager = manager.clone();
            std::thread::spawn(move || {
                for _ in 0..100 {
                    assert_eq!(
                        manager
                            .render(&[tag("en-US")], &messages::invalid_request_body())
                            .unwrap()
                            .text,
                        "invalid request body"
                    );
                }
            })
        };
        let spanish = {
            let manager = manager.clone();
            std::thread::spawn(move || {
                for _ in 0..100 {
                    assert_eq!(
                        manager
                            .render(&[tag("es-ES")], &messages::invalid_request_body())
                            .unwrap()
                            .text,
                        "cuerpo de solicitud no válido"
                    );
                }
            })
        };

        english.join().unwrap();
        spanish.join().unwrap();
    }

    #[test]
    fn render_warns_once_for_missing_pack_and_warning_storage_is_bounded() {
        let manager = Manager::new(&[tag("en-US")]);
        let message = messages::invalid_request_body();

        manager.render(&[tag("fr-FR")], &message).unwrap();
        manager.render(&[tag("fr-FR")], &message).unwrap();
        assert_eq!(manager.warning_count(), 1);

        for index in 0..MAX_WARNING_KEYS * 2 {
            manager.warn_once(format!("test:{index}"), "localization.test_warning");
        }
        assert_eq!(manager.warning_count(), MAX_WARNING_KEYS);
    }

    #[test]
    fn localized_error_preserves_code_source_and_downcast() {
        #[derive(Debug, thiserror::Error)]
        #[error("disk unavailable")]
        struct DiskError;

        let error = LocalizedError::new(
            "storage.unavailable",
            Message::new("storage.unavailable", "storage unavailable"),
            Some(Box::new(DiskError)),
        );
        assert_eq!(error.code(), "storage.unavailable");
        assert_eq!(error.to_string(), "storage unavailable");
        assert!(error
            .source()
            .unwrap()
            .downcast_ref::<DiskError>()
            .is_some());
    }

    #[test]
    fn display_boundary_matches_static_and_structured_domain_errors() {
        let manager = Manager::new(&[]);
        let spanish = tag("es-ES");

        let static_message = message_for_display("not leader").unwrap();
        assert_eq!(static_message.id, "replication.not_leader");
        assert_eq!(
            manager
                .render(std::slice::from_ref(&spanish), &static_message)
                .unwrap()
                .text,
            "el nodo no es líder"
        );

        let structured = message_for_display("failed to get points: peer reset").unwrap();
        assert_eq!(structured.id, "qdrant.get_points_failed");
        assert_eq!(structured.data["Cause"], Value::String("peer reset".into()));
        assert_eq!(
            manager.render(&[spanish], &structured).unwrap().text,
            "no se pudieron obtener los puntos: peer reset"
        );
    }

    #[test]
    fn telemetry_identity_and_fields_do_not_depend_on_locale() {
        let event = DiagnosticEvent {
            id: "server.request.invalid_body",
            message: messages::invalid_request_body(),
            fields: BTreeMap::from([
                ("component", Value::String("server".into())),
                ("status", Value::Number(400)),
            ]),
        };
        let manager = Manager::new(&[tag("en-US")]);
        let english = manager.render(&[tag("en-US")], &event.message).unwrap();
        let spanish = manager.render(&[tag("es-ES")], &event.message).unwrap();
        assert_ne!(english.text, spanish.text);
        assert_eq!(event.id, "server.request.invalid_body");
        assert_eq!(event.fields["status"], Value::Number(400));
    }

    #[test]
    fn catalog_contract_is_complete_and_deterministic() {
        assert_eq!(CATALOG_INVENTORY.len(), (1_807 + 7) * 3);
        assert_eq!(MESSAGE_IDS.len(), 1_807 + 7);
        assert!(MESSAGE_IDS.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(CATALOG_INVENTORY.windows(2).all(|pair| pair[0] < pair[1]));
        validate_catalog_inventory(CATALOG_INVENTORY).unwrap();

        let mut duplicate = CATALOG_INVENTORY.to_vec();
        duplicate.push(CATALOG_INVENTORY[0]);
        assert!(validate_catalog_inventory(&duplicate)
            .unwrap_err()
            .contains("duplicate catalog entry"));

        let missing = CATALOG_INVENTORY
            .iter()
            .copied()
            .filter(|entry| !(entry.id == "mcp.parse_error" && entry.language == "es-ES"))
            .collect::<Vec<_>>();
        assert_eq!(
            validate_catalog_inventory(&missing).unwrap_err(),
            "catalog entry mcp.parse_error is missing es-ES"
        );

        let malformed_pseudo = [
            CatalogInventoryEntry {
                id: "test.pseudo",
                language: "en-US",
                one: None,
                other: "source",
            },
            CatalogInventoryEntry {
                id: "test.pseudo",
                language: "en-XA",
                one: None,
                other: "[!! source",
            },
            CatalogInventoryEntry {
                id: "test.pseudo",
                language: "es-ES",
                one: None,
                other: "origen",
            },
        ];
        assert_eq!(
            validate_catalog_inventory(&malformed_pseudo).unwrap_err(),
            "invalid pseudo-locale envelope for test.pseudo"
        );
    }
}
