use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde::{Deserialize, Serialize};

const UPSTREAM_COMMIT: &str = "21b998cb";
const COPPERDB_CATALOG: [(&str, &str, &str); 21] = [
    (
        "bolt.database_switch_during_transaction",
        "en-US",
        "cannot change database during an active transaction",
    ),
    (
        "bolt.database_switch_during_transaction",
        "en-XA",
        "[!! cannot change database during an active transaction !!]",
    ),
    (
        "bolt.database_switch_during_transaction",
        "es-ES",
        "no se puede cambiar de base de datos durante una transacción activa",
    ),
    (
        "bolt.no_active_cursor",
        "en-US",
        "no Bolt result cursor is active",
    ),
    (
        "bolt.no_active_cursor",
        "en-XA",
        "[!! no Bolt result cursor is active !!]",
    ),
    (
        "bolt.no_active_cursor",
        "es-ES",
        "no hay ningún cursor de resultados Bolt activo",
    ),
    (
        "bolt.transaction_already_active",
        "en-US",
        "transaction already active",
    ),
    (
        "bolt.transaction_already_active",
        "en-XA",
        "[!! transaction already active !!]",
    ),
    (
        "bolt.transaction_already_active",
        "es-ES",
        "ya hay una transacción activa",
    ),
    ("bolt.unknown_cursor", "en-US", "unknown Bolt result cursor"),
    (
        "bolt.unknown_cursor",
        "en-XA",
        "[!! unknown Bolt result cursor !!]",
    ),
    (
        "bolt.unknown_cursor",
        "es-ES",
        "cursor de resultados Bolt desconocido",
    ),
    (
        "storage.log.bfs_adjacency_build",
        "en-US",
        "BFS adjacency-cache construction phase breakdown",
    ),
    (
        "storage.log.bfs_adjacency_build",
        "en-XA",
        "[!! BFS adjacency-cache construction phase breakdown !!]",
    ),
    (
        "storage.log.bfs_adjacency_build",
        "es-ES",
        "desglose de la fase de construcción de la caché de adyacencia BFS",
    ),
    (
        "storage.log.bfs_edge_snapshot",
        "en-US",
        "BFS edge snapshot phase breakdown",
    ),
    (
        "storage.log.bfs_edge_snapshot",
        "en-XA",
        "[!! BFS edge snapshot phase breakdown !!]",
    ),
    (
        "storage.log.bfs_edge_snapshot",
        "es-ES",
        "desglose de la fase de instantánea de aristas BFS",
    ),
    (
        "storage.log.flush_guard_failed",
        "en-US",
        "storage flush failed during FlushGuard drop",
    ),
    (
        "storage.log.flush_guard_failed",
        "en-XA",
        "[!! storage flush failed during FlushGuard drop !!]",
    ),
    (
        "storage.log.flush_guard_failed",
        "es-ES",
        "falló el vaciado del almacenamiento al liberar FlushGuard",
    ),
];

#[derive(Debug, Deserialize)]
struct CatalogEntry {
    id: String,
    #[serde(default)]
    zero: Option<String>,
    #[serde(default)]
    one: Option<String>,
    #[serde(default)]
    two: Option<String>,
    #[serde(default)]
    few: Option<String>,
    #[serde(default)]
    many: Option<String>,
    other: String,
}

#[derive(Debug, Deserialize)]
struct ProcedureEntry {
    name: String,
    en: String,
    es: String,
}

fn main() {
    let check = std::env::args()
        .skip(1)
        .any(|argument| argument == "--check");
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let upstream = std::env::var_os("NORNICDB_UPSTREAM")
        .map(PathBuf::from)
        .unwrap_or_else(|| crate_dir.join("../../../NornicDB"));
    verify_upstream(&upstream);

    let workspace = crate_dir.join("../..");
    let generated = format_generated(generate(&upstream, &workspace, &crate_dir, check));
    let output = crate_dir.join("src/generated_catalog.rs");
    if check {
        let existing = fs::read_to_string(&output).expect("read generated catalog");
        assert_eq!(
            existing, generated,
            "generated localization catalog is stale"
        );
    } else {
        fs::write(output, generated).expect("write generated catalog");
    }
}

fn format_generated(generated: String) -> String {
    let mut child = Command::new("rustfmt")
        .args(["--edition", "2021", "--emit", "stdout"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("start rustfmt for generated catalog");
    child
        .stdin
        .as_mut()
        .expect("open rustfmt stdin")
        .write_all(generated.as_bytes())
        .expect("write generated catalog to rustfmt");
    let output = child
        .wait_with_output()
        .expect("format generated catalog with rustfmt");
    assert!(output.status.success(), "rustfmt generated catalog");
    String::from_utf8(output.stdout).expect("formatted generated catalog is UTF-8")
}

fn verify_upstream(upstream: &Path) {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(upstream)
        .output()
        .expect("run git in NornicDB upstream");
    assert!(output.status.success(), "resolve NornicDB upstream commit");
    let commit = String::from_utf8(output.stdout).expect("git commit is UTF-8");
    assert!(
        commit.trim().starts_with(UPSTREAM_COMMIT),
        "NornicDB upstream must be pinned to {UPSTREAM_COMMIT}, found {}",
        commit.trim()
    );
}

fn generate(upstream: &Path, workspace: &Path, crate_dir: &Path, check: bool) -> String {
    let catalog_dir = upstream.join("pkg/localization/catalog");
    let mut paths = fs::read_dir(&catalog_dir)
        .expect("read upstream catalog directory")
        .map(|entry| entry.expect("read catalog entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "yaml")
        })
        .collect::<Vec<_>>();
    paths.sort();

    let mut catalogs = BTreeMap::<(String, String), CatalogEntry>::new();
    let mut upstream_languages = BTreeSet::new();
    for path in paths {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("catalog file name is UTF-8");
        let language = file_name
            .strip_suffix(".yaml")
            .and_then(|name| name.rsplit('.').next())
            .unwrap_or_else(|| panic!("unexpected upstream catalog name {file_name}"));
        upstream_languages.insert(language.to_string());
        let entries: Vec<CatalogEntry> =
            serde_yaml::from_str(&fs::read_to_string(&path).expect("read upstream catalog file"))
                .expect("parse upstream catalog file");
        for entry in entries {
            let key = (entry.id.clone(), language.to_string());
            assert!(
                catalogs.insert(key.clone(), entry).is_none(),
                "duplicate {key:?}"
            );
        }
    }

    load_local_only_catalogs(
        &crate_dir.join("locales"),
        &upstream_languages,
        &mut catalogs,
    );

    let source_ids = catalogs
        .keys()
        .filter(|(_, language)| language == "en-US")
        .map(|(id, _)| id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        source_ids.len(),
        1_807,
        "unexpected upstream source inventory"
    );
    for (id, language, other) in COPPERDB_CATALOG {
        let key = (id.to_string(), language.to_string());
        assert!(
            catalogs
                .insert(
                    key.clone(),
                    CatalogEntry {
                        id: id.to_string(),
                        zero: None,
                        one: None,
                        two: None,
                        few: None,
                        many: None,
                        other: other.to_string(),
                    },
                )
                .is_none(),
            "duplicate supplemental {key:?}"
        );
    }
    let languages = catalogs
        .keys()
        .map(|(_, language)| language.clone())
        .collect::<BTreeSet<_>>();
    for id in &source_ids {
        let source = &catalogs[&(id.clone(), SOURCE_LANGUAGE.into())];
        for language in &languages {
            let translated = catalogs
                .get(&(id.clone(), language.clone()))
                .unwrap_or_else(|| panic!("{language} missing {id}"));
            let source_forms = source.forms();
            let translated_forms = translated.forms();
            assert!(
                source_forms.keys().eq(translated_forms.keys()),
                "{language} {id} forms"
            );
            for (form, source_template) in source_forms {
                assert_eq!(
                    fields(source_template),
                    fields(translated_forms[form]),
                    "{language} {id} {form}"
                );
            }
        }
    }
    validate_source_inventory(workspace, &catalogs);
    write_locale_files(&crate_dir.join("locales"), &catalogs, &languages, check);

    let procedures: Vec<ProcedureEntry> = serde_yaml::from_str(
        &fs::read_to_string(upstream.join("pkg/localization/procedure_metadata.yaml"))
            .expect("read procedure metadata"),
    )
    .expect("parse procedure metadata");
    assert!(procedures
        .windows(2)
        .all(|pair| pair[0].name < pair[1].name));

    let mut output = String::from(
        "// Code generated from NornicDB 21b998cb by examples/generate_catalog.rs; DO NOT EDIT.\n\n",
    );
    output.push_str("pub const CATALOG_INVENTORY: &[CatalogInventoryEntry] = &[\n");
    for ((id, language), entry) in &catalogs {
        writeln!(
            output,
            "    CatalogInventoryEntry {{ id: {id:?}, language: {language:?}, one: {}, other: {:?} }},",
            rust_option(entry.one.as_deref()),
            entry.other
        )
        .unwrap();
    }
    output.push_str("];\n\npub const PROCEDURE_METADATA: &[ProcedureMetadataEntry] = &[\n");
    for entry in procedures {
        writeln!(
            output,
            "    ProcedureMetadataEntry {{ name: {:?}, en: {:?}, es: {:?} }},",
            entry.name, entry.en, entry.es
        )
        .unwrap();
    }
    output.push_str("];\n\npub const MESSAGE_IDS: &[&str] = &[\n");
    for id in catalogs
        .keys()
        .filter(|(_, language)| language == SOURCE_LANGUAGE)
        .map(|(id, _)| id)
    {
        writeln!(output, "    {id:?},").unwrap();
    }
    output.push_str("];\n");
    output
}

#[derive(Deserialize, Serialize)]
struct LocaleFile {
    #[serde(rename = "_version")]
    version: u8,
    #[serde(flatten)]
    messages: BTreeMap<String, String>,
}

fn write_locale_files(
    locale_dir: &Path,
    catalogs: &BTreeMap<(String, String), CatalogEntry>,
    languages: &BTreeSet<String>,
    check: bool,
) {
    fs::create_dir_all(locale_dir).expect("create localization catalog directory");
    for language in languages {
        let mut messages = BTreeMap::new();
        for ((id, entry_language), entry) in catalogs {
            if entry_language != language {
                continue;
            }
            messages.insert(id.clone(), entry.other.clone());
            for (form, template) in entry.forms() {
                if form != "other" {
                    messages.insert(format!("{id}.{form}"), template.to_string());
                }
            }
        }
        let contents = serde_yaml::to_string(&LocaleFile {
            version: 1,
            messages,
        })
        .expect("serialize rust-i18n locale file");
        let path = locale_dir.join(format!("{language}.yml"));
        if check {
            let existing = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
            assert_eq!(existing, contents, "{} is stale", path.display());
        } else {
            fs::write(&path, contents)
                .unwrap_or_else(|error| panic!("write {}: {error}", path.display()));
        }
    }
}

fn load_local_only_catalogs(
    locale_dir: &Path,
    upstream_languages: &BTreeSet<String>,
    catalogs: &mut BTreeMap<(String, String), CatalogEntry>,
) {
    let mut paths = fs::read_dir(locale_dir)
        .expect("read local catalog directory")
        .map(|entry| entry.expect("read local catalog entry").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "yml"))
        .collect::<Vec<_>>();
    paths.sort();
    for path in paths {
        let language = path
            .file_stem()
            .and_then(|name| name.to_str())
            .expect("local catalog file name is UTF-8");
        if upstream_languages.contains(language) {
            continue;
        }
        let locale: LocaleFile =
            serde_yaml::from_str(&fs::read_to_string(&path).expect("read local-only catalog file"))
                .expect("parse local-only catalog file");
        for (id, other) in locale
            .messages
            .iter()
            .filter(|(id, _)| !is_plural_form_key(id))
        {
            let key = (id.clone(), language.to_string());
            let one = locale.messages.get(&format!("{id}.one")).cloned();
            assert!(
                catalogs
                    .insert(
                        key.clone(),
                        CatalogEntry {
                            id: id.clone(),
                            zero: locale.messages.get(&format!("{id}.zero")).cloned(),
                            one,
                            two: locale.messages.get(&format!("{id}.two")).cloned(),
                            few: locale.messages.get(&format!("{id}.few")).cloned(),
                            many: locale.messages.get(&format!("{id}.many")).cloned(),
                            other: other.clone(),
                        },
                    )
                    .is_none(),
                "duplicate local-only {key:?}"
            );
        }
    }
}

fn is_plural_form_key(id: &str) -> bool {
    ["zero", "one", "two", "few", "many"]
        .iter()
        .any(|form| id.ends_with(&format!(".{form}")))
}

impl CatalogEntry {
    fn forms(&self) -> BTreeMap<&'static str, &str> {
        let mut forms = BTreeMap::from([("other", self.other.as_str())]);
        for (name, value) in [
            ("zero", self.zero.as_deref()),
            ("one", self.one.as_deref()),
            ("two", self.two.as_deref()),
            ("few", self.few.as_deref()),
            ("many", self.many.as_deref()),
        ] {
            if let Some(value) = value {
                forms.insert(name, value);
            }
        }
        forms
    }
}

fn rust_option(value: Option<&str>) -> String {
    value.map_or("None".into(), |value| format!("Some({value:?})"))
}

const SOURCE_LANGUAGE: &str = "en-US";

fn validate_source_inventory(
    workspace: &Path,
    catalogs: &BTreeMap<(String, String), CatalogEntry>,
) {
    let mut files = Vec::new();
    collect_rust_files(&workspace.join("crates"), &mut files);
    files.sort();
    let mut used = BTreeSet::new();
    for path in files {
        if path.ends_with("generated_catalog.rs")
            || path
                .components()
                .any(|component| component.as_os_str() == "examples")
        {
            continue;
        }
        let source = fs::read_to_string(&path).expect("read Rust inventory source");
        let source = source.split("#[cfg(test)]").next().unwrap_or(&source);
        collect_immediate_literal(source, "Message::new(", &mut used);
        collect_immediate_literal(source, "Message::from_catalog(", &mut used);
        collect_immediate_literal(source, "event_id = ", &mut used);
        collect_second_literal(source, "localize_id(", &mut used);
    }
    for id in used {
        assert!(
            catalogs.contains_key(&(id.clone(), SOURCE_LANGUAGE.into())),
            "production localization ID {id} is missing from the source catalog"
        );
    }
}

fn collect_rust_files(directory: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory).expect("read Rust source directory") {
        let path = entry.expect("read Rust source entry").path();
        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

fn collect_immediate_literal(source: &str, marker: &str, used: &mut BTreeSet<String>) {
    for tail in source.split(marker).skip(1) {
        if let Some(id) = quoted_prefix(tail.trim_start()) {
            used.insert(id.to_string());
        }
    }
}

fn collect_second_literal(source: &str, marker: &str, used: &mut BTreeSet<String>) {
    for tail in source.split(marker).skip(1) {
        let Some((_, argument)) = tail.split_once(',') else {
            continue;
        };
        if let Some(id) = quoted_prefix(argument.trim_start()) {
            used.insert(id.to_string());
        }
    }
}

fn quoted_prefix(value: &str) -> Option<&str> {
    let value = value.strip_prefix('"')?;
    let end = value.find('"')?;
    Some(&value[..end])
}

fn fields(template: &str) -> Vec<&str> {
    let mut fields = Vec::new();
    let mut remaining = template;
    while let Some(start) = remaining.find("{{") {
        let tail = &remaining[start + 2..];
        let Some(end) = tail.find("}}") else { break };
        fields.push(tail[..end].trim().trim_start_matches('.'));
        remaining = &tail[end + 2..];
    }
    fields.sort_unstable();
    fields
}
