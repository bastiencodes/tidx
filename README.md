# ak47

**High-throughput Tempo blockchain indexer in Rust**

[![Build Status](https://github.com/tempoxyz/ak47/actions/workflows/ci.yml/badge.svg)](https://github.com/tempoxyz/ak47/actions)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

## What is ak47?

ak47 is a high-performance blockchain indexer built specifically for [Tempo](https://tempo.xyz), the blockchain for payments at scale. Inspired by [golden-axe](https://github.com/indexsupply/golden-axe), it indexes blocks, transactions, and logs into TimescaleDB for fast querying.

**Key features:**
- **Tempo-native**: Built for Tempo's instant finality—no reorg handling needed
- **High throughput**: PostgreSQL COPY protocol + pipelined sync for maximum write speed
- **Bidirectional sync**: Follow head in realtime, backfill history toward genesis
- **Resume support**: Interrupted syncs resume automatically from where they left off
- **TimescaleDB**: Hypertables with columnar compression for fast analytics
- **SQL queries**: Direct SQL access with JIT event signature decoding

## Installation

```bash
# From source
git clone https://github.com/tempoxyz/ak47
cd ak47
cargo build --release

# Or with Docker
docker pull ghcr.io/tempoxyz/ak47:latest
```

## CLI

### Commands

| Command | Description |
|---------|-------------|
| `ak47 up` | Start continuous sync from chain head (realtime) |
| `ak47 status` | Show sync status, coverage, and gaps |
| `ak47 sync backfill` | Backfill blocks from head toward genesis |
| `ak47 sync status` | Show sync status (same as `ak47 status`) |
| `ak47 query` | Run SQL queries against indexed data |
| `ak47 compress` | Compress TimescaleDB chunks and refresh aggregates |

### Sync Commands

```bash
# Follow chain head continuously
ak47 up --rpc <RPC_URL> --db <DB_URL>

# Backfill to genesis (resumes if interrupted)
ak47 sync --rpc <RPC_URL> --db <DB_URL> backfill --to 0

# Backfill from specific block
ak47 sync --rpc <RPC_URL> --db <DB_URL> backfill --from 10000 --to 0

# Check sync progress
ak47 sync --rpc <RPC_URL> --db <DB_URL> status
```

### Query Commands

```bash
# Raw SQL query
ak47 query --db <DB_URL> "SELECT * FROM blocks ORDER BY num DESC LIMIT 10"

# Query with event signature decoding
ak47 query --db <DB_URL> \
  --signature "Transfer(address indexed from, address indexed to, uint256 value)" \
  "SELECT * FROM \"Transfer\" LIMIT 10"
```

### Configuration

All commands accept these common flags:

| Flag | Env Variable | Description |
|------|--------------|-------------|
| `--rpc <url>` | `AK47_RPC_URL` | Tempo RPC endpoint |
| `--db <url>` | `AK47_DATABASE_URL` | PostgreSQL/TimescaleDB connection URL |

## Tempo Networks

| Network | Chain ID | RPC |
|---------|----------|-----|
| Presto (mainnet) | 4217 | `https://rpc.presto.tempo.xyz` |
| Andantino (testnet) | 42429 | `https://rpc.testnet.tempo.xyz` |
| Moderato | 42431 | `https://rpc.moderato.tempo.xyz` |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        ak47 Indexer                          │
├─────────────────────────────────────────────────────────────┤
│  RPC Client ──► Decoder ──► Writer ──► Sync State           │
│      │                         │                             │
│      ▼                         ▼                             │
│  Tempo Node              TimescaleDB                         │
│  (JSON-RPC)        (blocks, txs, logs tables)               │
└─────────────────────────────────────────────────────────────┘
```

### Database Schema

Tables use TimescaleDB hypertables partitioned by block number:

| Table | Primary Key | Description |
|-------|-------------|-------------|
| **blocks** | `(num)` | Block headers with hash, timestamp, gas info |
| **txs** | `(block_num, idx)` | Transactions with Tempo-specific fields |
| **logs** | `(block_num, log_idx)` | Event logs with selector indexing |
| **sync_state** | `(id)` | Sync progress (synced_num, backfill_num) |

## Performance

ak47 is optimized for high-throughput indexing:

| Optimization | Description |
|-------------|-------------|
| **Binary COPY** | PostgreSQL COPY protocol for bulk inserts (2-3x faster than INSERT) |
| **Pipelined sync** | Fetches next batch while writing current batch (overlapped I/O) |
| **Batch RPC** | Fetches blocks and receipts in parallel batches |
| **Gzip compression** | Compresses RPC responses to reduce network overhead |
| **Unlogged staging** | Uses unlogged staging tables for transient COPY data |

### Benchmarks

```bash
cargo bench --bench sync_bench    # Write throughput
cargo bench --bench query_bench   # Query performance
```

## Development

### Prerequisites

- Rust 1.75+
- Docker & Docker Compose
- PostgreSQL client (optional, for `psql`)

### Make Commands

| Command | Description |
|---------|-------------|
| `make up` | Start TimescaleDB + Tempo node |
| `make down` | Stop all services |
| `make test` | Run tests (auto-starts infrastructure) |
| `make bench` | Run benchmarks |
| `make seed` | Generate test transactions (`DURATION=30 TPS=100`) |
| `make seed-heavy` | Generate ~1M+ txs with full variance |
| `make reset` | Drop and recreate database |
| `make psql` | Open psql shell to test database |

### Running Tests

```bash
# Start test infrastructure
docker compose -f docker-compose.test.yml up -d

# Run all tests
make test

# Run specific test suites
cargo test --test smoke_test -- --test-threads=1
cargo test --test query_test -- --test-threads=1
```

### Test Infrastructure

Tests use real services (no mocks):
- `docker-compose.test.yml` - TimescaleDB + Tempo node
- `tests/common/testdb.rs` - Test database with auto-seeding
- `tests/common/tempo.rs` - Tempo node helpers

## License

MIT License - see [LICENSE](./LICENSE) for details.

## Acknowledgments

- [golden-axe](https://github.com/indexsupply/golden-axe) - Inspiration for the indexing architecture
- [Tempo](https://github.com/tempoxyz/tempo) - The blockchain we're indexing
- [Reth](https://github.com/paradigmxyz/reth) - Rust Ethereum patterns and practices
