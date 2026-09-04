#!/usr/bin/env bash

set -euo pipefail

# aws-smithy-runtime 1.14 still enables its legacy Hyper 0.14 connector by
# default. CopperDB uses its modern AWS-LC default HTTPS client. Tantivy 0.26.1
# still pins lru 0.16. Remove these exceptions as their owners publish fixes.
cargo audit \
    --deny warnings \
    --ignore RUSTSEC-2026-0258 \
    --ignore RUSTSEC-2026-0104 \
    --ignore RUSTSEC-2026-0098 \
    --ignore RUSTSEC-2026-0099 \
    --ignore RUSTSEC-2026-0253