# 02: Bolt Authentication And Explicit Transactions

Status: in progress. Priority: P0. Owners: `bolt`, `server`, `auth`, `engine`, `txsession`, `storage`, `errors`.

## Objective

Replace permissive Bolt authentication and acknowledgement-only transaction messages with authenticated, database-scoped sessions owning real isolated storage transactions.

## Current Anchors

- `BoltSession::process_message`, `QueryExecutor`, and failure encoding in `crates/bolt/src/server.rs`.
- message decoding in `crates/bolt/src/dispatch.rs` and `messages.rs`.
- `AppStateBoltExecutor` in `crates/server/src/lib.rs` currently supplies administrative roles.
- `Authenticator` in `crates/auth`; `SessionManager`/`TransactionManager` in `txsession`.
- NornicDB references: `pkg/bolt/server.go`, auth adapter, bookmark, integration, relationship-delete, and snapshot-conflict tests.

## Protocol Contract

Bolt 4 `HELLO` and Bolt 5 `LOGON` authenticate against the shared authenticator. A session stores principal, database grants, protocol state, active transaction, result cursors, and last bookmark. `RUN`, `BEGIN`, and `ROUTE` require appropriate authentication. Explicit commit acknowledges only after configured storage durability succeeds.

## Implementation Phases

1. Define a Bolt auth adapter returning a principal and database-scoped roles. Implement `HELLO`/`LOGON`/`LOGOFF`; reject queries in unauthenticated or failed states.
2. Replace the executor API with principal-aware implicit execution and explicit `begin`, `run`, `commit`, `rollback`, and `close` methods. Remove hard-coded `admin` roles.
3. Bind a real engine/storage transaction from item 8 to each explicit Bolt transaction. Track database immutably for its lifetime.
4. Roll back on `RESET`, `LOGOFF`, disconnect, timeout, handler error, or failed-state cleanup. Never report rollback after a durable commit decision.
5. Implement cursor IDs and bounded `PULL`/`DISCARD {n,qid}`, `has_more`, summary counters, notifications, and bookmarks.
6. Centralize Neo4j status/error encoding through `copperdb-errors`, including retryable transaction conflicts.

## Progress

- Complete: Phase 1. Bolt `HELLO` and `LOGON` authenticate through the shared durable authenticator; `LOGOFF` clears the session principal; failed or unauthenticated sessions receive `Neo.ClientError.Security.Unauthorized`.
- Complete: secure `RUN` calls receive the authenticated principal and its actual roles. `AppStateBoltExecutor` no longer grants a hard-coded `admin` role for authenticated Bolt sessions.
- Complete: authenticated Bolt `BEGIN`, `COMMIT`, and `ROLLBACK` own a database-scoped engine transaction handle, clean up on `LOGOFF` and `RESET`, and return the engine-issued commit bookmark. This preserves NornicDB and Neo4j Bolt protocol compatibility.
- Complete: `RUN` carries the active transaction handle through the executor boundary and is pinned to the database selected at `BEGIN`; an attempt to switch databases during a transaction fails without executing the query.
- Complete: Phase 3. `AppStateBoltExecutor` owns an `Arc`-backed `StorageTransaction` for each Bolt transaction, routes transactional `RUN` through the private storage overlay, commits that overlay before issuing the engine bookmark, and discards it on rollback. Server regressions prove read-your-writes, private node and relationship visibility, durable commit publication, and rollback invisibility.
- Complete: Phase 4. `RESET`, `LOGOFF`, TCP and WebSocket teardown, transport and handler errors, idle receive timeouts, and failed transactional `RUN` all roll back an active explicit transaction through one idempotent cleanup path. Durable commit removes the active handle before its logical bookmark commit, so later cleanup cannot report a rollback after the storage commit decision. Regressions cover disconnect, timeout, and failed-query cleanup.

## Tests

- Invalid, expired, and valid credentials across supported protocol versions.
- Database role allow/deny and no privilege escalation through metadata.
- Commit visibility, rollback invisibility, read-your-writes, disconnect cleanup.
- Multiple cursors, partial pull/discard, invalid qid, bounded memory.
- Bookmark chaining and retryable error status.
- Driver E2E tests over TCP and WebSocket.

Run `cargo test -p copperdb-bolt --lib`, `cargo test -p copperdb-txsession`, `cargo test -p copperdb-server bolt`, then driver integration tests.

## Performance And Risks

Cache principal context per connection. Page records rather than cloning complete results. Transaction isolation cannot be simulated in Bolt; item 8 is a hard dependency for completion. Cleanup must be idempotent and bounded.

## Definition Of Done

- No Bolt query runs without the required authenticated principal.
- Explicit transactions own isolated writes and durable commit/rollback outcomes.
- Pagination, counters, bookmarks, cleanup, roles, and errors pass driver E2E tests.