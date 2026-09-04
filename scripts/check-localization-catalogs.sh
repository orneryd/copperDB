#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
cargo run -p copperdb-localization --example generate_catalog -- --check
cargo test -p copperdb-localization catalog_contract_is_complete_and_deterministic