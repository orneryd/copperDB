# CopperDB Localization

CopperDB uses `rust-i18n` for compile-time embedded, per-language YAML catalogs. The protocol-neutral `Manager` always supplies an explicit locale, so concurrent requests do not share global locale state.

## Catalog Layout

Catalogs live under `locales/<language-tag>.yml` in rust-i18n version 1 format. Stable diagnostic IDs remain dotted keys:

```yaml
_version: 1
server.invalid_request_body: invalid request body
localization.items_processed: '{{.Count}} items processed'
localization.items_processed.one: '{{.Count}} item processed'
```

The base key is the `other` plural form. Append `.zero`, `.one`, `.two`, `.few`, or `.many` for CLDR cardinal forms required by a language. CopperDB selects forms with ICU plural rules. Keep `{{.Field}}` placeholders identical across languages and preserve the same form set as the source entry.

## Updating From NornicDB

The checked generator imports the pinned NornicDB catalogs, validates every locale and placeholder, adds CopperDB-owned messages, emits the YAML files, and refreshes the source inventory:

```text
cargo run -p copperdb-localization --example generate_catalog
cargo run -p copperdb-localization --example generate_catalog -- --check
```

Set `NORNICDB_UPSTREAM` when the upstream checkout is not at `../NornicDB` relative to this workspace.

## Adding A Language

1. Add `locales/<canonical-language-tag>.yml` with `_version: 1`.
2. Translate every base key and matching plural-form keys while preserving placeholders.
3. Add exact-match, language-fallback, plural, and request-boundary tests.
4. Run the generator check and the localization crate tests.

The generator discovers language tags from NornicDB's `active.*.<language-tag>.yaml` files and from complete local-only `locales/<language-tag>.yml` packs. No Rust language inventory needs updating. A local-only pack must contain the full source message inventory, including CopperDB-owned messages; the generator rejects missing entries and placeholder drift.

Imported Go templates support the catalog actions currently used by NornicDB: named fields, `if`/`else`/`end`, and `printf` formats `%q`, `%x`, `%-20s`, and `%.6f`. Adding another action requires an explicit renderer case and exact compatibility tests.

Do not call `rust_i18n::set_locale` in server code. Process defaults and request preferences are resolved by `Manager`; rendering remains explicit and request-scoped.