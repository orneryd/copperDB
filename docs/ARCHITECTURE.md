# Architecture

copperDB is a Rust rewrite of [NornicDB](https://github.com/orneryd/NornicDB)'s property-graph database engine. The current supported runtime is single-node execution. Distributed/fabric/replication, cross-node transactions, and GPU acceleration are retained as future-state architecture.

Current implementation status and execution priority are tracked only in [COPPERDB_NORNICDB_PARITY_PLAN.md](COPPERDB_NORNICDB_PARITY_PLAN.md). Architecture descriptions here are not completion claims.

## Project Structure

```
copperDB/
├── Cargo.toml          # Workspace manifest
├── crates/
│   ├── audit/          # ← pkg/audit
│   ├── auth/           # ← pkg/auth
│   ├── bolt/           # ← pkg/bolt
│   ├── buildinfo/      # ← pkg/buildinfo
│   ├── cache/          # ← pkg/cache
│   ├── compliance/     # ← pkg/compliance
│   ├── config/         # ← pkg/config
│   ├── copperdb/       # ← pkg/nornicdb (executable)
│   ├── cypher/         # ← pkg/cypher
│   ├── decay/          # Temporal/knowledge-policy decay
│   ├── embed/          # ← pkg/embed
│   ├── embeddingutil/  # ← pkg/embeddingutil
│   ├── encryption/     # ← pkg/encryption
│   ├── engine/         # ← pkg/nornicdb (engine core)
│   ├── envutil/        # ← pkg/envutil
│   ├── errors/         # Shared error types
│   ├── eval/           # ← pkg/eval
│   ├── fabric/         # ← pkg/fabric
│   ├── filter/         # ← pkg/filter
│   ├── gpu/            # ← pkg/gpu
│   ├── graphql/        # ← pkg/graphql
│   ├── heimdall/       # ← pkg/heimdall
│   ├── indexing/       # ← pkg/indexing
│   ├── inference/      # ← pkg/inference
│   ├── kms/            # ← pkg/kms
│   ├── linkpredict/    # ← pkg/linkpredict
│   ├── localllm/       # ← pkg/embed (local GGUF)
│   ├── math/           # ← pkg/math
│   ├── mcp/            # ← pkg/mcp
│   ├── multidb/        # ← pkg/multidb
│   ├── nornicgrpc/     # ← pkg/nornicgrpc
│   ├── pool/           # ← pkg/pool
│   ├── qdrantgrpc/     # ← pkg/qdrantgrpc
│   ├── replication/    # ← pkg/replication
│   ├── retention/      # ← pkg/retention
│   ├── search/         # ← pkg/search
│   ├── security/       # ← pkg/security
│   ├── server/         # ← pkg/server
│   ├── simd/           # ← pkg/simd
│   ├── storage/        # ← pkg/storage
│   ├── temporal/       # ← pkg/temporal
│   ├── textchunk/      # ← pkg/textchunk
│   ├── topology/       # Cluster topology (Rust-native)
│   ├── txsession/      # ← pkg/txsession
│   ├── util/           # ← pkg/util
│   └── vectorspace/    # ← pkg/vectorspace
├── data/               # Default data directory
├── docs/               # Documentation
├── lib/                # Native libraries (llama.cpp, etc.)
├── scripts/            # Build & utility scripts
├── tools/              # Developer tools
└── ui/                 # Web dashboard (React/Vite)
```

## Storage Engine

copperDB uses **fjall** (a Rust LSM-tree key-value store) as its embedded storage backend. Both fjall and BadgerDB (used by NornicDB) are LSM-tree based with similar performance characteristics. Key differences:

- fjall uses lock-free B-trees internally (not pure LSM)
- fjall is pure Rust (no FFI); BadgerDB is pure Go
- For higher write throughput, `rocksdb` can be considered as an alternative backend

### Key Trees

| Tree      | Purpose                          |
|-----------|----------------------------------|
| `nodes`   | Node records (id → properties)   |
| `edges`   | Edge/relationship records         |
| `indexes` | Property, fulltext, and label indexes |
| `meta`    | Counters, schema, and metadata    |

## Search Architecture

### BM25 Full-Text Search

Implements the BM25 V2 algorithm matching NornicDB's scoring:

- **k1 = 1.2**, **b = 0.75** (length normalization)
- IDF: `ln(1 + (N - df + 0.5) / (df + 0.5))`
- 34 English stop words filtered
- Minimum token length: 2 characters
- `lexical_seed_doc_ids()` selects high-IDF documents to seed HNSW construction

### Vector Search

The `vectorspace` crate provides a deterministic in-memory HNSW graph and named-index registry. During `CopperDb` startup, declared node indexes with explicit dimensions are built and maintained from committed node events; `db.index.vector.queryNodes` uses registry candidates and storage hydration. Cosine indexes use HNSW traversal, while Euclidean indexes use an explicit exact strategy and are never reported as HNSW. Relationship indexes, persistence, and broader lifecycle work remain active parity work. The `localllm` and `embed` crates provide local GGUF/llama.cpp components that still need full per-database runtime composition.

### Hybrid Search (RRF)

Reciprocal Rank Fusion merging of BM25 + vector results is defined in the `search` crate. The HTTP search endpoint (`POST /db/{database}/search`) returns BM25-ranked results with node property enrichment.

## Embedding Pipeline

```
Text → textchunk → localllm (llama.cpp GGUF) → typed node embedding fields → vectorspace (HNSW)
```

## Go → Rust Dependency Mapping

| NornicDB Go             | copperDB Rust        |
|-------------------------|----------------------|
| badger/v4 (KV store)    | fjall                |
| antlr4-go (Cypher parser)| handwritten Rust parser |
| neo4j-go-driver (client)| neo4rs               |
| gqlgen (GraphQL)        | async-graphql        |
| vek (SIMD)              | wide                 |
| msgpack/v5              | rmp-serde            |
| x/crypto                | aes-gcm, argon2, sha2, hmac, ring |
| grpc + protobuf         | tonic + prost        |
| golang-jwt              | jsonwebtoken         |
| google/uuid             | uuid                 |
| yaml.v3 + toml          | serde_yaml + toml    |
| cobra (CLI)             | clap                 |
| gorilla/websocket       | tokio-tungstenite    |
| net/http                | axum                 |
| purego + ffi            | libloading           |
| blevesearch             | BM25 (in-house)      |
| x/sync                  | tokio, parking_lot, dashmap |

## GPU Acceleration

copperDB consolidates NornicDB's four GPU backends under `wgpu` (WebGPU-based, cross-platform):
- **macOS**: Metal backend (automatic)
- **Linux/Windows with NVIDIA**: Vulkan backend
- **Direct CUDA**: `cudarc` crate (optional feature)
