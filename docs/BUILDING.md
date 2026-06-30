# Building & Running copperDB

## Quick Start

```bash
# Build
make build

# Run (HTTP + Bolt + UI)
make run

# Run with custom ports
make run HTTP_PORT=4000 BOLT_PORT=8687
```

## Production Build

```bash
cargo build --release --package copperdb
```

To include the web UI assets:

```bash
cd ui && npm install && npm run build
cd ..
cargo run --release --package copperdb -- --http-port 7474 --bolt-port 7687
```

Or with explicit environment variables:

```bash
RUST_LOG=info \
COPPERDB_ADDRESS=0.0.0.0 \
COPPERDB_HTTP_PORT=7474 \
COPPERDB_BOLT_PORT=7687 \
COPPERDB_BASE_PATH=/ \
COPPERDB_STATIC_DIR=ui/dist \
cargo run --release --package copperdb -- \
  --address 0.0.0.0 \
  --http-port 7474 \
  --bolt-port 7687 \
  --base-path / \
  --static-dir ui/dist
```

## Testing

```bash
# All core tests
make test

# Specific crate
cargo test --lib -p copperdb-storage
cargo test --lib -p copperdb-eval
cargo test --lib -p copperdb-engine

# All tests including integration
cargo test --workspace
```

## Clean Build

```bash
make clean
# or
cargo clean
```

## Default Ports

| Service | Port  |
|---------|-------|
| HTTP/UI | 7474  |
| Bolt    | 7687  |

## Configuration

Configuration precedence (highest to lowest):

1. CLI flags
2. `COPPERDB_*` environment variables
3. Config file (`--config`, `COPPERDB_CONFIG`, `./copperdb.yaml`, `./copperdb.yml`, `./copperdb.toml`)
4. Built-in defaults

### Supported Listeners

| CLI Flag          | Env Variable             |
|-------------------|--------------------------|
| `--address`       | `COPPERDB_ADDRESS`       |
| `--http-address`  | `COPPERDB_HTTP_ADDRESS`  |
| `--bolt-address`  | `COPPERDB_BOLT_ADDRESS`  |
| `--http-port`     | `COPPERDB_HTTP_PORT`     |
| `--bolt-port`     | `COPPERDB_BOLT_PORT`     |
| `--headless`      | `COPPERDB_HEADLESS`      |
| `--base-path`     | `COPPERDB_BASE_PATH`     |
| `--config`        | `COPPERDB_CONFIG`        |

Neo4j-compatible environment variables are also accepted:

- `NEO4J_dbms_connector_http_listen__address_port`
- `NEO4J_dbms_connector_bolt_listen__address_port`

## Logging

Default filter: `copperdb=info,fjall=warn,info`

Override with `RUST_LOG`:

```bash
RUST_LOG=copperdb=debug make run
RUST_LOG=fjall=info make run   # Show storage recovery details
```
