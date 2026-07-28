<p align="center">
  <img src="https://raw.githubusercontent.com/orneryd/copperDB/refs/heads/main/logo.svg" alt="copperDB Logo" width="200"/>
</p>

<h1 align="center">copperDB</h1>

<p align="center">
  Neo4j-compatible &bull; GPU-accelerated &bull; Memory that evolves<br/>
</p>

<p align="center">
  <a href="https://neo4j.com/"><img src="https://img.shields.io/badge/neo4j-compatible-008CC1?logo=neo4j" alt="Neo4j Compatible"></a>
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/rust-stable-orange?logo=rust" alt="Rust"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="License"></a>
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> &bull;
  <a href="#what-is-copperdb">What It Is</a> &bull;
  <a href="#features">Features</a> &bull;
  <a href="#documentation">Docs</a> &bull;
  <a href="#status">Status</a>
</p>

---

## What Is copperDB?

copperDB is an **experimental graph database** built in Rust. It speaks Neo4j's Bolt protocol, runs Cypher queries, indexes your data with BM25 full-text search, and embeds nodes locally with llama.cpp — all in a single binary.

It's the Rust evolution of [NornicDB](https://github.com/orneryd/NornicDB), rebuilt from the storage engine up for performance and safety.

> **copperDB is experimental.** APIs may change, features are evolving, and production deployments should validate against their workload. The current parity audit identifies security, transaction, search, and lifecycle work that must land before production-readiness claims.

---

## Quick Start

```bash
# Clone and build
git clone https://github.com/orneryd/copperDB.git
cd copperDB
make build

# Start the server
make run
```

Open **http://localhost:7474** — Neo4j Browser connects automatically via Bolt on port 7687.

```bash
# Custom ports
make run HTTP_PORT=4000 BOLT_PORT=8687

# Production build with UI
cd ui && npm install && npm run build
cd ..
cargo run --release --package copperdb
```

> See [docs/BUILDING.md](docs/BUILDING.md) for full configuration options.

---

## Features

### Neo4j Compatibility
- **Bolt protocol v4.4** — connect with Neo4j Browser, Bloom, or any Bolt driver
- **Cypher query language** — `MATCH`, `CREATE`, `MERGE`, `SET`, `DELETE`, `REMOVE`, aggregations, shortest-path, list comprehensions
- **Multi-database support** — `CREATE DATABASE`, `SHOW DATABASES`, `DROP DATABASE`
- **Transactions** — auto-commit and explicit `BEGIN`/`COMMIT`/`ROLLBACK`

### Search
- **BM25 V2 full-text search** with length normalization — ranked text retrieval across node properties
- **Vector embeddings** via local GGUF models (llama.cpp) — no external embedding service needed
- **HTTP search endpoint** — `POST /db/{database}/search` with JSON API

### Storage & Indexing
- **LSM-tree storage** (fjall) — embedded, zero-configuration, crash-safe
- **MVCC versioning** — historical reads and concurrent access
- **Property indexes** — range, temporal, and full-text
- **Knowledge policies** — TTL-based decay, legal holds, erasure requests

### Embeddings & AI
- **Local LLM inference** via llama.cpp GGUF models — GPU-first with CPU fallback
- **Managed embeddings** — typed node embedding fields with metadata tracking
- **Vector search baseline** — cosine similarity scoring; maintained HNSW indexing is active parity work

### Operations
- **Web dashboard** — React UI for database management
- **Authentication** — JWT + RBAC
- **Encryption** — AES-256-GCM at rest
- **Audit logging** — event sink with structured output
- **Rate limiting** — per-tenant throttling

---

## Connect

Connect any Neo4j-compatible tool to `bolt://localhost:7687`:

| Tool            | How                           |
|-----------------|-------------------------------|
| Neo4j Browser   | Open http://localhost:7474    |
| Neo4j Bloom     | Connect to bolt://localhost:7687 |
| Python          | `neo4j` driver                |
| JavaScript      | `neo4j-driver` npm package    |
| Go              | `neo4j-go-driver`             |

Example Cypher queries:

```cypher
-- Create nodes and relationships
CREATE (alice:Person {name: 'Alice', bio: 'Builds reliable graph systems'})
CREATE (bob:Person {name: 'Bob', bio: 'Writes storage engines'})
CREATE (alice)-[:KNOWS {since: 2024}]->(bob)

-- Search with full-text index
CREATE FULLTEXT INDEX personBio FOR (n:Person) ON EACH [n.bio]
CALL db.search.fulltext('Person', ['bio'], 'graph systems')

-- Find shortest path
MATCH p = shortestPath((a:Person {name: 'Alice'})-[:KNOWS*]-(b:Person {name: 'Bob'}))
RETURN p
```

---

## Status

copperDB is under active development. The core engine runs a substantial Cypher subset, serves Bolt clients, and maintains local indexes. Authentication defaults, explicit Bolt transactions, and semantic/vector runtime behavior remain active parity work.

See [docs/COPPERDB_NORNICDB_PARITY_PLAN.md](docs/COPPERDB_NORNICDB_PARITY_PLAN.md) for the audited implementation status and next steps.

**Current focus**: Engine parity with NornicDB — search, embeddings, Cypher coverage, and performance.

---

## Documentation

| Document                                                     | Content                              |
|--------------------------------------------------------------|--------------------------------------|
| [docs/BUILDING.md](docs/BUILDING.md)                         | Build, run, test, and configure      |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)                 | Project structure and design          |
| [docs/COPPERDB_NORNICDB_PARITY_PLAN.md](docs/COPPERDB_NORNICDB_PARITY_PLAN.md) | Authoritative parity audit and implementation plan |
| [docs/COPPERDB_STRATEGIC_ROADMAP.md](docs/COPPERDB_STRATEGIC_ROADMAP.md) | Long-term roadmap          |

---

## License

MIT — see [LICENSE](LICENSE).

---

<p align="center">
  <sub>Built in Rust. Powered by fjall, axum, and llama.cpp.</sub>
</p>
