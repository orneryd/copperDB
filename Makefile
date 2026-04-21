SHELL := /bin/sh

RUST_BIN := $(HOME)/.cargo/bin
export PATH := $(RUST_BIN):$(PATH)

ADDRESS ?= 0.0.0.0
HTTP_PORT ?= 7474
BOLT_PORT ?= 7687
DB_NAME ?= copperdb
RUST_LOG ?= info
BASE_PATH ?= /
UI_DIR := ui
UI_DIST := $(UI_DIR)/dist
VITE_BASE_PATH ?= $(BASE_PATH)

.PHONY: help build build-ui build-binary test run clean fmt check

help:
	@printf '%s\n' 'copperDB developer commands:'
	@printf '%s\n' '  make build  - build the UI and native binary, then print the run command'
	@printf '%s\n' '  make build-ui - build the browser assets into ui/dist'
	@printf '%s\n' '  make build-binary - build the copperdb binary'
	@printf '%s\n' '  make test   - run the workspace test suite'
	@printf '%s\n' '  make run    - start the copperdb HTTP and Bolt servers'
	@printf '%s\n' '  make fmt    - format the workspace'
	@printf '%s\n' '  make check  - cargo check the workspace'
	@printf '%s\n' '  make clean  - remove build artifacts'

build-ui:
	@printf '%s\n' 'Building UI assets...'
	@cd $(UI_DIR) && npm install && VITE_BASE_PATH=$(VITE_BASE_PATH) npm run build
	@printf '%s\n' '✓ UI built successfully'

build-binary:
	@cargo build -p copperdb

build: build-ui build-binary
	@printf '\n'
	@printf '%s\n' '==============================================================='
	@printf '%s\n' ' Build complete!'
	@printf '%s\n' '==============================================================='
	@printf '\n'
	@printf '%s\n' 'UI: $(UI_DIST)'
	@printf '%s\n' 'Binary: target/debug/copperdb'
	@printf '\n'
	@printf '%s\n' 'Environment:'
	@printf '%s\n' '  export PATH="$$HOME/.cargo/bin:$$PATH"'
	@printf '%s\n' '  export RUST_LOG=$(RUST_LOG)'
	@printf '%s\n' '  export COPPERDB_ADDRESS=$(ADDRESS)'
	@printf '%s\n' '  export COPPERDB_HTTP_PORT=$(HTTP_PORT)'
	@printf '%s\n' '  export COPPERDB_BOLT_PORT=$(BOLT_PORT)'
	@printf '%s\n' '  export COPPERDB_BASE_PATH=$(BASE_PATH)'
	@printf '%s\n' '  export COPPERDB_STATIC_DIR=$(UI_DIST)'
	@printf '%s\n' 'Run:'
	@printf '%s\n' '  cargo run -p copperdb -- --address $(ADDRESS) --http-port $(HTTP_PORT) --bolt-port $(BOLT_PORT) --db-name $(DB_NAME) --base-path $(BASE_PATH) --static-dir $(UI_DIST)'
	@printf '\n'
	@printf '%s\n' 'Connect:'
	@printf '%s\n' '  Browser:  http://127.0.0.1:$(HTTP_PORT)$(BASE_PATH)'
	@printf '%s\n' '  Bolt:     bolt://127.0.0.1:$(BOLT_PORT)'
	@printf '%s\n' '  Username: admin'
	@printf '%s\n' '  Password: password'
	@printf '%s\n' 'Verify:'
	@printf '%s\n' '  curl http://127.0.0.1:$(HTTP_PORT)/health'
	@printf '%s\n' '  nc -vz 127.0.0.1 $(BOLT_PORT)'

test:
	@cargo test --workspace

run:
	@RUST_LOG=$(RUST_LOG) COPPERDB_ADDRESS=$(ADDRESS) COPPERDB_HTTP_PORT=$(HTTP_PORT) COPPERDB_BOLT_PORT=$(BOLT_PORT) COPPERDB_BASE_PATH=$(BASE_PATH) COPPERDB_STATIC_DIR=$(UI_DIST) cargo run -p copperdb -- --address $(ADDRESS) --http-port $(HTTP_PORT) --bolt-port $(BOLT_PORT) --db-name $(DB_NAME) --base-path $(BASE_PATH) --static-dir $(UI_DIST)

fmt:
	@cargo fmt --all

check:
	@cargo check --workspace

clean:
	@cargo clean