# TIDX Agent Instructions

## Project Overview
High-throughput Tempo blockchain indexer in Rust, inspired by golden-axe.

## Commands

### Build & Check
```bash
cargo check          # Fast type checking
cargo build          # Debug build
cargo build --release # Release build
```

### Test
```bash
# Start test infrastructure (PostgreSQL + Tempo node)
docker compose -f docker/local/docker-compose.yml up -d postgres tempo

# Wait for services to be healthy
docker compose -f docker/local/docker-compose.yml ps

# Run tests
cargo test

# Run specific test
cargo test smoke_test
```

### Generate Load (for benchmarking)
```bash
# Use tempo-bench to generate millions of transactions
docker run --rm --network host ghcr.io/tempoxyz/tempo-bench:latest \
  run-max-tps \
  --duration 60 \
  --tps 5000 \
  --accounts 10000 \
  --target-urls http://localhost:8545 \
  --faucet
```

### Run (Docker)
```bash
# Production deployment with PostgreSQL, Prometheus, Grafana
# Edit config.toml to configure chains
docker compose up -d

# View logs
docker compose logs -f tidx

# Access services:
# - HTTP API: http://localhost:8080
# - Prometheus: http://localhost:9091
# - Grafana: http://localhost:3000 (admin/admin)
```

### Run (Local)
```bash
# Start indexing (reads from config.toml)
cargo run -- up

# Use custom config file
cargo run -- up --config /path/to/config.toml

# Check sync status
cargo run -- status
```

### Run in a worktree (runbook)

Follow these steps when landing in a fresh git worktree and you need the
indexer running. Multiple worktrees can run their own indexer concurrently
against the shared docker-compose infra — `make dev` derives unique HTTP
and Prometheus ports from the worktree directory name.

**Step 1 — confirm shared infra is running.** postgres and clickhouse must
be up (typically started once from the main checkout with `make up`).
Quick check:

```bash
docker ps --format '{{.Names}}' | grep -E 'postgres|clickhouse'
```

If nothing returns, start the infra from the main checkout:
`docker compose -f docker/prod/docker-compose.yml up -d postgres clickhouse`.

**Step 2 — make sure `.env` is available in this worktree.** `.env` is
gitignored, so fresh worktrees don't have one. Symlink the main checkout's
`.env` (one-time per worktree):

```bash
# Adjust the path to wherever your main checkout lives.
ln -s /absolute/path/to/main/tidx/.env .env
```

`.env.example` at the repo root lists the variables that must be populated
(RPC URLs in particular).

**Step 3 — start the indexer.**

```bash
make dev
```

`make dev` prints the per-worktree ports, then:
1. Derives a stable port offset from `basename $(pwd)` (base 18080 / 19090,
   offset 0–899) so concurrent worktrees don't collide.
2. Generates `config.local.toml` (gitignored) from `config.prod.toml` with
   those ports patched in.
3. Sources `.env` if present and runs
   `cargo run -- up --config config.local.toml`.

**Step 4 — verify.** Hit the health endpoint on the HTTP port that
`make dev` printed (e.g. `curl http://localhost:18082/health`). You should
get `{"ok": true, ...}`.

**Note on data isolation.** Worktrees share the postgres databases declared
in `config.prod.toml` (`tidx_mainnet`, `tidx_sepolia`). If you need a
worktree to write into its own database, edit `config.local.toml` (or
change `pg_url` in `config.prod.toml` before running `make dev`).

### HTTP API Endpoints
```bash
# Health check
curl http://localhost:8080/health

# Sync status
curl http://localhost:8080/status

# Execute SQL query
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT num FROM blocks ORDER BY num DESC LIMIT 5"}'

# Query decoded event logs
curl "http://localhost:8080/logs/Transfer(address,address,uint256)?limit=10&after=1h"

# Prometheus metrics (default port 9090)
curl http://localhost:9090/metrics
```

### Benchmarks
```bash
cargo bench
```

## Architecture

- `src/api/` - HTTP API server (axum router, handlers)
- `src/cli/` - CLI commands (up, status, query, sync, compress)
- `src/service/` - Shared business logic (status, query execution)
- `src/sync/` - Sync engine, RPC fetcher, decoder, writer
- `src/db/` - Database pool and schema management
- `src/types.rs` - Core data types (BlockRow, TxRow, LogRow)
- `migrations/` - SQL migrations
- `tests/common/` - Test infrastructure (real Tempo node, TestDb, TestClickHouse)
- `tests/smoke_test.rs` - PostgreSQL integration tests (sync, queries, events)
- `tests/clickhouse_test.rs` - ClickHouse OLAP integration tests

## Tempo Networks

| Network | Chain ID | RPC |
|---------|----------|-----|
| Presto (mainnet) | 4217 | https://rpc.mainnet.tempo.xyz |
| Moderato (testnet) | 42431 | https://rpc.testnet.tempo.xyz |

## Code Style
- Follow existing patterns in the codebase
- Use `anyhow::Result` for error handling
- Use `tracing` for logging
- Prefer `alloy` types for Ethereum primitives
- **Never use mocks** - always prefer real implementations over mocks

## Git Workflow
- **Commit incrementally** - never batch multiple features into one commit
- Commit after each logical change (new feature, optimization, refactor, test)
- Use conventional commit messages: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `perf:`
- Each commit should be independently reviewable and revertable
