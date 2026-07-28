# Implementation Status

> Historical status snapshot. The authoritative current audit, package status, and next steps are in [COPPERDB_NORNICDB_PARITY_PLAN.md](COPPERDB_NORNICDB_PARITY_PLAN.md). A checked crate below means a baseline existed when this snapshot was written; it must not be read as full NornicDB parity or production readiness.

| Crate            | Status       | Notes |
|------------------|-------------|-------|
| `util`           | ✅ Complete  | |
| `buildinfo`      | ✅ Complete  | |
| `envutil`        | ✅ Complete  | |
| `convert`        | ✅ Complete  | PackStream types |
| `math`           | ✅ Complete  | |
| `config`         | ✅ Complete  | |
| `cache`          | ✅ Complete  | LRU + TTL |
| `storage`        | ✅ Complete  | fjall LSM-tree, BM25 V2, MVCC |
| `auth`           | ✅ Complete  | JWT + RBAC |
| `encryption`     | ✅ Complete  | AES-256-GCM DEK/KEK |
| `kms`            | ✅ Complete  | Local KMS; AWS/Azure/GCP stubs |
| `simd`           | ✅ Complete  | wide f32x8 |
| `decay`          | ✅ Complete  | Exp/Power/Gaussian + Kalman filter |
| `vectorspace`    | ✅ Complete  | HNSW-preferred cosine scoring |
| `embed`          | ✅ Complete  | Local GGUF via llama.cpp FFI |
| `embeddingutil`  | ✅ Complete  | Normalization/similarity helpers |
| `textchunk`      | ✅ Complete  | Char + sentence chunking |
| `temporal`       | ✅ Complete  | Temporal edges + session |
| `audit`          | ✅ Complete  | Event logging + sink trait |
| `compliance`     | ✅ Complete  | Policy enforcement |
| `heimdall`       | ✅ Complete  | Rate limiting |
| `filter`         | ✅ Complete  | Predicate filtering |
| `indexing`       | ✅ Complete  | Index registry + labelless lookup |
| `linkpredict`    | ✅ Complete  | CN/Jaccard/AA/PA algorithms |
| `pool`           | ✅ Complete  | Generic connection pool |
| `txsession`      | ✅ Complete  | ACID transaction lifecycle |
| `retention`      | ✅ Complete  | TTL-based expiry + legal holds |
| `multidb`        | ✅ Complete  | Multi-database manager |
| `cypher`         | ✅ Complete  | PEG parser + full AST |
| `eval`           | ✅ Complete  | Cypher evaluator, 291 tests |
| `bolt`           | ✅ Complete  | PackStream + Bolt v4.4 server |
| `server`         | ✅ Complete  | HTTP routes + search endpoint |
| `engine`         | ✅ Complete  | CopperDb engine core, 83 tests |
| `copperdb`       | ✅ Complete  | Executable assembly |
| `localllm`       | ✅ Complete  | llama.cpp FFI via libloading |
| `search`         | ✅ Complete  | RRF types + merge logic |
| `security`       | ✅ Complete  | rustls integration |
| `graphql`        | 🔧 Partial   | Schema stub; handlers pending |
| `mcp`            | 🔧 Partial   | Tool definitions; JSON-RPC pending |
| `nornicgrpc`     | 🔧 Stub      | Proto files needed |
| `qdrantgrpc`     | 🔧 Stub      | Add qdrant-client crate |
| `inference`      | 🔧 Stub      | Pipeline wiring needed |
| `fabric`         | 🔮 Future    | Deferred cluster routing |
| `gpu`            | 🔮 Future    | wgpu acceleration |
| `replication`    | 🔮 Future    | Deferred distributed architecture |

## Items Requiring Custom Implementation

### Cypher Query Parser (`crates/cypher`)

Uses a `pest` PEG grammar (`cypher.pest`) — the most idiomatic Rust approach for the openCypher grammar. Full AST defined with 142 parser tests.

Reference: https://s3.amazonaws.com/artifacts.opencypher.org/cypher.ebnf

### Neo4j Bolt Protocol Server (`crates/bolt`)

Full Bolt v1–v5 server implementation including:
- PackStream encoder/decoder
- Bolt handshake (version negotiation)
- Authentication (HELLO/LOGON)
- Message dispatch loop (RUN, PULL, BEGIN, COMMIT, ROLLBACK)
- Chunked message framing

Reference: https://7687.org/

### Local LLM via GGUF (`crates/localllm`)

Runtime loading of llama.cpp via `libloading` (manual FFI, matching NornicDB's CGO approach). Supports GPU-first with CPU fallback. Build:

```bash
make build-llama       # CPU
make build-llama-cuda   # CUDA
```

### Model Context Protocol (`crates/mcp`)

JSON-RPC 2.0 over WebSocket/stdio with tool registry and invocation handlers. Tracks the [MCP specification](https://github.com/modelcontextprotocol/specification).

### Kalman Filter (`crates/decay`)

Simplified 1-D Kalman adapter for decay rate adaptation. Extendable to full matrix-based Kalman via `nalgebra`.
