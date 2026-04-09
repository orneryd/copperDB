# magnetDB

A Rust implementation of the [NornicDB](https://github.com/orneryd/NornicDB) graph database engine.

magnetDB provides the same capabilities as NornicDB — a property-graph database with
Cypher query support, vector embeddings, temporal data, Raft replication, and GPU
acceleration — implemented idiomatically in Rust.

---

## Project Structure

The repository is organized as a Cargo workspace. Each crate under `crates/` corresponds
directly to a Go package under `pkg/` in NornicDB:

```
magnetDB/
├── Cargo.toml          # Workspace manifest (all shared dependency versions here)
└── crates/
    ├── audit/          # ← pkg/audit
    ├── auth/           # ← pkg/auth
    ├── bolt/           # ← pkg/bolt
    ├── buildinfo/      # ← pkg/buildinfo
    ├── cache/          # ← pkg/cache
    ├── compliance/     # ← pkg/compliance
    ├── config/         # ← pkg/config
    ├── convert/        # ← pkg/convert
    ├── cypher/         # ← pkg/cypher
    ├── decay/          # ← pkg/decay
    ├── embed/          # ← pkg/embed
    ├── embeddingutil/  # ← pkg/embeddingutil
    ├── encryption/     # ← pkg/encryption
    ├── envutil/        # ← pkg/envutil
    ├── eval/           # ← pkg/eval
    ├── fabric/         # ← pkg/fabric
    ├── filter/         # ← pkg/filter
    ├── gpu/            # ← pkg/gpu
    ├── graphql/        # ← pkg/graphql
    ├── heimdall/       # ← pkg/heimdall
    ├── indexing/       # ← pkg/indexing
    ├── inference/      # ← pkg/inference
    ├── kms/            # ← pkg/kms
    ├── linkpredict/    # ← pkg/linkpredict
    ├── localllm/       # ← pkg/embed (local GGUF)
    ├── magnetdb/       # ← pkg/nornicdb  (main engine)
    ├── math/           # ← pkg/math
    ├── mcp/            # ← pkg/mcp
    ├── multidb/        # ← pkg/multidb
    ├── nornicgrpc/     # ← pkg/nornicgrpc
    ├── pool/           # ← pkg/pool
    ├── qdrantgrpc/     # ← pkg/qdrantgrpc
    ├── replication/    # ← pkg/replication
    ├── retention/      # ← pkg/retention
    ├── search/         # ← pkg/search
    ├── security/       # ← pkg/security
    ├── server/         # ← pkg/server
    ├── simd/           # ← pkg/simd
    ├── storage/        # ← pkg/storage
    ├── temporal/       # ← pkg/temporal
    ├── textchunk/      # ← pkg/textchunk
    ├── txsession/      # ← pkg/txsession
    ├── util/           # ← pkg/util
    └── vectorspace/    # ← pkg/vectorspace
```

---

## Building

```bash
cargo build --workspace
```

## Testing

```bash
cargo test --workspace
```

---

## Go → Rust Dependency Mapping

The following table maps every significant NornicDB Go dependency to its Rust equivalent.

| NornicDB Go Dependency | Purpose | Rust Equivalent |
|------------------------|---------|-----------------|
| `github.com/dgraph-io/badger/v4` | Embedded key-value store | `sled = "0.34"` |
| `github.com/antlr4-go/antlr/v4` | Cypher query parser (ANTLR4 runtime) | ⚠️ **No direct equivalent** — see note below |
| `github.com/neo4j/neo4j-go-driver/v5` | Neo4j Bolt client | `neo4rs` (client only) |
| `github.com/qdrant/go-client` | Qdrant vector DB gRPC client | `qdrant-client = "1"` *(add when needed)* |
| `github.com/99designs/gqlgen` | GraphQL schema + codegen | `async-graphql = "7"` |
| `github.com/vektah/gqlparser/v2` | GraphQL parser | included in `async-graphql` |
| `github.com/viterin/vek` | SIMD float32 vector ops | `wide = "0.7"` |
| `github.com/vmihailenco/msgpack/v5` | MessagePack serialization | `rmp-serde = "1"` |
| `golang.org/x/crypto` | AES-GCM, argon2, SHA2, HMAC | `aes-gcm`, `argon2`, `sha2`, `hmac`, `ring` |
| `google.golang.org/grpc` | gRPC transport | `tonic = "0.12"` |
| `google.golang.org/protobuf` | Protobuf encoding | `prost = "0.13"` |
| `github.com/golang-jwt/jwt` | JWT tokens | `jsonwebtoken = "9"` |
| `golang.org/x/oauth2` | OAuth 2.0 | `oauth2 = "4"` |
| `github.com/google/uuid` | UUID generation | `uuid = "1"` |
| `gopkg.in/yaml.v3` | YAML config files | `serde_yaml = "0.9"` |
| `github.com/BurntSushi/toml` | TOML config files | `toml = "0.8"` |
| `github.com/spf13/cobra` | CLI framework | `clap = "4"` |
| `github.com/gorilla/websocket` | WebSocket transport (MCP) | `tokio-tungstenite = "0.24"` |
| `net/http` (Go stdlib) | HTTP server | `axum = "0.7"` |
| `github.com/hashicorp/raft` | Raft consensus (implied) | `openraft = "0.9"` |
| `github.com/ebitengine/purego` + `github.com/jupiterrider/ffi` | CGo-free FFI for native libs | `libloading = "0.8"` |
| `github.com/hybridgroup/yzma` | WASM runtime for GGUF models | `libloading` (native) or `wasmtime = "20"` |
| CUDA (NVIDIA) via CGo wrappers | GPU compute | `wgpu = "0.20"` (cross-platform) or `cudarc` (CUDA) |
| Metal (Apple) via Objective-C bridge | GPU compute (macOS) | `wgpu = "0.20"` (Metal backend) |
| Vulkan via CGo | GPU compute (cross-platform) | `wgpu = "0.20"` (Vulkan backend) |
| OpenCL via CGo | GPU compute (portable) | `opencl3 = "0.9"` *(opt-in feature)* |
| `cloud.google.com/go/kms` | GCP Cloud KMS | ⚠️ **No official Rust SDK** — use REST API via `reqwest` |
| `github.com/aws/aws-sdk-go-v2/service/kms` | AWS KMS | `aws-sdk-kms = "1"` (official) |
| Azure Key Vault SDK | Azure KMS | `azure_security_keyvault_keys` crate |
| `github.com/blevesearch/bleve` | Full-text search | `tantivy = "0.22"` |
| petgraph (N/A in NornicDB — custom) | Graph algorithms | `petgraph = "0.6"` |
| `golang.org/x/sync` | Async primitives | Rust built-ins: `tokio`, `parking_lot`, `dashmap` |
| `github.com/stretchr/testify` | Test assertions | Rust built-in `#[test]` + `assert_eq!` |

---

## Items Requiring Custom Implementation

The following NornicDB components have **no direct Rust library equivalent** and must be
implemented from scratch (or by integrating multiple crates):

### 1. Cypher Query Parser (`crates/cypher`) ⚠️ HIGH PRIORITY

**NornicDB approach**: Uses an ANTLR4-generated parser via `github.com/antlr4-go/antlr/v4`.

**Problem**: There is no production-ready ANTLR4 Rust runtime or pre-built Cypher grammar
library for Rust.

**Recommended approaches** (in order of maturity):

- **Option A — `pest` PEG grammar**: Write a Cypher PEG grammar file (`cypher.pest`) and
  use `pest = "2"` + `pest_derive`. This is the most idiomatic Rust approach.
  Reference grammar: https://s3.amazonaws.com/artifacts.opencypher.org/cypher.ebnf
  
- **Option B — `lalrpop`**: Use `lalrpop = "0.20"` for an LALR(1) parser. More complex
  but handles left-recursive grammars naturally.

- **Option C — `nom`**: Hand-write a combinator parser with `nom = "7"`. Most flexible,
  highest effort.

The `crates/cypher` crate already defines the complete AST; only the parser function
`cypher::parse()` returns `Err(UnsupportedClause)` and must be completed.

### 2. Neo4j Bolt Protocol Server (`crates/bolt`) ⚠️ HIGH PRIORITY

**NornicDB approach**: Full Bolt v1–v5 server implementation.

**Problem**: `neo4rs` is a Bolt *client*. There is no existing Rust Bolt *server* library.

**What must be implemented**:
- PackStream encoder/decoder (stub in `crates/bolt/src/packstream.rs` — basic encoding done)
- Bolt handshake (version negotiation)
- Authentication (HELLO/LOGON)
- Message dispatch loop (RUN, PULL, BEGIN, COMMIT, ROLLBACK)
- Chunked message framing

Reference: https://7687.org/ (Bolt protocol specification)

### 3. GCP Cloud KMS (`crates/kms`)

**NornicDB approach**: Uses `cloud.google.com/go/kms` (official Google Cloud Go SDK).

**Problem**: No official Google Cloud KMS Rust SDK exists.

**Options**:
- Use the `google-cloud-kms` crate (unofficial, community-maintained)
- Call the GCP KMS REST API directly via `reqwest` with `yup-oauth2` for authentication

### 4. Local LLM via GGUF (`crates/localllm`)

**NornicDB approach**: Uses `github.com/hybridgroup/yzma` (WASM runtime) to run GGUF models
in-process without CGo.

**Options**:
- Use `libloading` to call a pre-built `libllama.so` / `libllama.dylib` at runtime (current stub approach)
- Use `llama_cpp_2` crate (wraps llama.cpp with Rust bindings, requires a C++ build)
- Use `candle-core` (HuggingFace Candle) for a pure-Rust inference path (no GGUF yet)

**Build instructions for libllama**:
```bash
git clone https://github.com/ggerganov/llama.cpp
cd llama.cpp && mkdir build && cd build
cmake .. -DBUILD_SHARED_LIBS=ON
cmake --build . --config Release
```

### 5. Model Context Protocol (`crates/mcp`)

**NornicDB approach**: Implements the MCP JSON-RPC 2.0 protocol over WebSocket/stdio.

**Problem**: No official Rust MCP SDK existed at time of writing (April 2025).
Track: https://github.com/modelcontextprotocol/specification

**What must be implemented**:
- JSON-RPC 2.0 request/response dispatcher (over `axum` WebSocket)
- Tool registry and invocation
- MCP `initialize` / `tools/list` / `tools/call` handlers

**Note**: The `mcp-rs` crate is emerging on crates.io — check for updates.

### 6. Kalman Filter (`crates/decay`)

**NornicDB approach**: Custom Kalman filter for decay rate adaptation.

**Problem**: No widely-used Rust Kalman filter crate exists for this use case.

**Current state**: A simplified 1-D Kalman adapter is implemented in
`crates/decay/src/lib.rs` (`KalmanAdapter`). For production, extend to a full
matrix-based Kalman filter using `nalgebra`'s matrix operations.

### 7. gRPC Protobuf Definitions (`crates/nornicgrpc`, `crates/qdrantgrpc`)

**What must be implemented**:
- Write `.proto` service definition files in `crates/nornicgrpc/proto/`
- Add a `build.rs` that calls `tonic_build::compile_protos()` 
- Implement service handler structs

See `crates/nornicgrpc/src/lib.rs` for the build.rs example.

---

## Architecture Notes

### Storage Engine
magnetDB uses `sled` (an embedded Rust key-value store) instead of BadgerDB.
Both are LSM-tree based and offer similar performance characteristics.
Key differences:
- `sled` uses lock-free B-trees internally (not pure LSM)
- `sled` is pure Rust (no CGo); BadgerDB is pure Go
- For higher write throughput, consider `rocksdb` crate (FFI to RocksDB)

### Replication
NornicDB's replication module is built around `hashicorp/raft` (the Go standard).
magnetDB uses `openraft` (TiKV's modern, async-native Raft library).
`openraft` requires implementing three traits: `RaftStateMachine`, `RaftStorage`, and
`RaftNetwork`. These are stubs in `crates/replication/src/lib.rs`.

### GPU Acceleration
NornicDB has four separate GPU backends (CUDA/Metal/Vulkan/OpenCL) each with CGo wrappers.
magnetDB consolidates these under `wgpu` (WebGPU-based, cross-platform compute):
- On macOS: uses Metal backend automatically
- On Linux/Windows with NVIDIA: uses Vulkan backend
- For direct CUDA access: add `cudarc` as an optional feature dependency

### Embedding Pipeline
```
Text → textchunk → embed (OpenAI API or localllm) → vectorspace (HNSW index)
```
Vector search is currently brute-force in `crates/vectorspace`. For production,
integrate the `instant-distance` or `hnsw_rs` crate for approximate nearest neighbor search.

---

## Implementation Status

| Crate | Status | Notes |
|-------|--------|-------|
| `util` | ✅ Scaffolded | Ready |
| `buildinfo` | ✅ Scaffolded | Ready |
| `envutil` | ✅ Scaffolded | Ready |
| `convert` | ✅ Scaffolded | PackStream types ready |
| `math` | ✅ Scaffolded | Full impl |
| `config` | ✅ Scaffolded | Full impl |
| `cache` | ✅ Scaffolded | Full LRU+TTL impl |
| `storage` | ✅ Scaffolded | Full sled impl |
| `auth` | ✅ Scaffolded | JWT+RBAC impl |
| `encryption` | ✅ Scaffolded | AES-256-GCM DEK/KEK impl |
| `kms` | ✅ Scaffolded | Local KMS impl; AWS/Azure/GCP stubs |
| `simd` | ✅ Scaffolded | `wide` f32x8 impl |
| `decay` | ✅ Scaffolded | Exp/Power/Gaussian + Kalman filter |
| `vectorspace` | ✅ Scaffolded | Brute-force KNN; HNSW needed |
| `embed` | ✅ Scaffolded | Mock embedder; OpenAI/local TODO |
| `embeddingutil` | ✅ Scaffolded | Normalization/similarity helpers |
| `textchunk` | ✅ Scaffolded | Char + sentence chunking |
| `temporal` | ✅ Scaffolded | Temporal edges + session |
| `audit` | ✅ Scaffolded | Event logging + sink trait |
| `compliance` | ✅ Scaffolded | Policy enforcement |
| `heimdall` | ✅ Scaffolded | Rate limiting |
| `filter` | ✅ Scaffolded | Predicate filtering |
| `indexing` | ✅ Scaffolded | Index registry |
| `linkpredict` | ✅ Scaffolded | CN/Jaccard/AA/PA algorithms |
| `pool` | ✅ Scaffolded | Generic connection pool |
| `txsession` | ✅ Scaffolded | ACID transaction lifecycle |
| `retention` | ✅ Scaffolded | TTL-based expiry |
| `multidb` | ✅ Scaffolded | Multi-database manager |
| `fabric` | ✅ Scaffolded | Cluster routing |
| `gpu` | ✅ Scaffolded | CPU fallback; wgpu TODO |
| `cypher` | 🔧 AST complete | **Parser not implemented** |
| `eval` | 🔧 Stub | Requires cypher parser |
| `bolt` | 🔧 Partial | PackStream encoding partial; server TODO |
| `replication` | 🔧 Stub | openraft traits not implemented |
| `graphql` | 🔧 Scaffolded | Schema stub; handlers need storage wiring |
| `server` | 🔧 Scaffolded | Routes defined; handlers need storage wiring |
| `mcp` | 🔧 Scaffolded | Tool definitions; JSON-RPC TODO |
| `nornicgrpc` | 🔧 Stub | Proto files needed |
| `qdrantgrpc` | 🔧 Stub | Add `qdrant-client` crate |
| `localllm` | 🔧 Stub | llama.cpp FFI needed |
| `inference` | 🔧 Stub | Pipeline wiring needed |
| `search` | 🔧 Stub | Tantivy integration needed |
| `security` | 🔧 Stub | rustls integration needed |
| `magnetdb` | 🔧 Stub | Requires all above |

---

## Contributing

See individual crate `src/lib.rs` files for inline TODO comments pointing to the
equivalent NornicDB Go source file for reference implementation guidance.
