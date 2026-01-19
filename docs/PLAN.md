# AK47 Implementation Plan

> A high-throughput Tempo blockchain indexer in Rust, inspired by [golden-axe](https://github.com/indexsupply/golden-axe).

## Design Principles

1. **Tempo-specific**: Built for Tempo's instant finality, no reorg handling needed
2. **Vertical slices**: Every phase produces a working, testable end-to-end pipeline
3. **Reuse tempo-primitives**: No reinventing transaction/block types
4. **One database backend**: TimescaleDB only (hybrid row/column via Hypercore)
5. **Trait-based extensibility**: Swap implementations without generic explosion
6. **Code minimalism**: Fewer lines, same performance

---

## Tempo Networks

| Network | Chain ID | RPC |
|---------|----------|-----|
| Presto (mainnet) | 4217 | `https://rpc.presto.tempo.xyz` |
| Andantino (testnet) | 42429 | `https://rpc.testnet.tempo.xyz` |
| Moderato | 42431 | `https://rpc.moderato.tempo.xyz` |

**Tempo-specific assumptions:**
- Instant finality (no reorgs)
- Transaction type 0x76 with batch calls, 2D nonces, fee sponsorship
- Millisecond-precision timestamps via `TempoHeader`
- SubBlock transactions flattened into main tables

---

## Query Performance SLAs

| Query Type | Target Latency | Conditions |
|------------|----------------|------------|
| **Point lookup** (block/tx by hash) | < 1ms | Indexed, warmed cache |
| **Selector scan** (logs by topic0, last 24h) | < 5ms | Indexed, < 10k rows |
| **Time-range aggregate** (tx count per hour) | < 50ms | Compressed chunks |
| **Full table scan** | < 500ms | Statement timeout |

**Benchmark gates (CI after Phase 3):**
- `write_bench`: > 10k blocks/sec COPY throughput
- `query_bench`: Point lookups p99 < 2ms
- `decode_bench`: > 50k txs/sec decode throughput

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                           AK47 Indexer                               │
├─────────────────────────────────────────────────────────────────────┤
│  ┌──────────┐  ┌──────────────┐  ┌───────────┐  ┌────────────────┐ │
│  │  Config  │  │     Sync     │  │   Query   │  │  Observability │ │
│  │ + Chains │  │ Coordinator  │  │    API    │  │   (Metrics)    │ │
│  └────┬─────┘  └──────┬───────┘  └─────┬─────┘  └────────────────┘ │
│       │               │                │                            │
│  ┌────┴───────────────┴────────────────┴────┐                       │
│  │           Ingestion Pipeline             │                       │
│  │  RpcClient → Decoder → Writer → Cursor   │                       │
│  └──────────────────┬───────────────────────┘                       │
│                     │                                                │
│  ┌──────────────────┴───────────────────────┐                       │
│  │         Query Engine (JIT CTE)           │                       │
│  │   signature → selector → indexed query   │                       │
│  └──────────────────────────────────────────┘                       │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    TimescaleDB (PostgreSQL)                          │
├─────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌──────────────────┐  ┌────────────────────┐ │
│  │  Partitioned    │  │ Selector Indexes │  │    Continuous      │ │
│  │  Tables         │  │ (chain, topic0,  │  │    Aggregates      │ │
│  │  (blocks, txs,  │  │  block_timestamp)│  │    (optional)      │ │
│  │   logs)         │  │                  │  │                    │ │
│  └─────────────────┘  └──────────────────┘  └────────────────────┘ │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  Hypercore: Rowstore (hot) ←→ Columnstore (cold, 90%+ comp) │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Core Traits

Narrow traits with `dyn Trait` for easy mocking:

```rust
/// RPC client interface (reuses tempo-primitives types)
#[trait_variant::make(Send)]
pub trait RpcClient: Sync {
    async fn chain_id(&self) -> Result<u64>;
    async fn latest_block_number(&self) -> Result<u64>;
    async fn get_block(&self, num: u64, full_txs: bool) -> Result<Block>;
    async fn get_blocks_batch(&self, range: RangeInclusive<u64>) -> Result<Vec<Block>>;
    async fn get_logs(&self, from: u64, to: u64) -> Result<Vec<Log>>;
}

/// Database writer (single implementation: Postgres/TimescaleDB)
#[trait_variant::make(Send)]
pub trait DbWriter: Sync {
    async fn ensure_partitions(&self, block_num: u64) -> Result<()>;
    async fn write_blocks(&self, blocks: &[BlockRow]) -> Result<()>;
    async fn write_txs(&self, txs: &[TxRow]) -> Result<()>;
    async fn write_logs(&self, logs: &[LogRow]) -> Result<()>;
}

/// Cursor persistence
#[trait_variant::make(Send)]
pub trait CursorStore: Sync {
    async fn load(&self) -> Result<SyncState>;
    async fn commit(&self, state: &SyncState) -> Result<()>;
}
```

---

## Database Schema

### Static Tables (partitioned by block range)

```sql
-- Blocks (Tempo-specific: no is_canonical needed due to instant finality)
CREATE TABLE blocks (
    num             INT8 NOT NULL,
    hash            BYTEA NOT NULL,
    parent_hash     BYTEA NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    timestamp_ms    INT8 NOT NULL,  -- Tempo millisecond precision
    gas_limit       NUMERIC NOT NULL,
    gas_used        NUMERIC NOT NULL,
    miner           BYTEA NOT NULL,
    extra_data      BYTEA,
    PRIMARY KEY (num)
) PARTITION BY RANGE (num);

-- Transactions (Tempo 0x76 native)
CREATE TABLE txs (
    block_num               INT8 NOT NULL,
    block_timestamp         TIMESTAMPTZ NOT NULL,
    idx                     INT4 NOT NULL,
    hash                    BYTEA NOT NULL,
    type                    INT2 NOT NULL,          -- 0x76 for Tempo native
    "from"                  BYTEA NOT NULL,
    "to"                    BYTEA,                  -- First call target (NULL = CREATE)
    value                   NUMERIC NOT NULL,       -- Sum of all call values
    input                   BYTEA NOT NULL,         -- First call input
    gas_limit               INT8 NOT NULL,
    max_fee_per_gas         NUMERIC NOT NULL,       -- EIP-1559
    max_priority_fee_per_gas NUMERIC NOT NULL,      -- EIP-1559
    gas_used                INT8,                   -- From receipt
    -- Tempo 2D nonce system
    nonce_key               BYTEA NOT NULL,         -- U256, key 0 = protocol nonce
    nonce                   INT8 NOT NULL,
    -- Tempo fee sponsorship
    fee_token               BYTEA,                  -- Alternative fee token (NULL = native)
    fee_payer               BYTEA,                  -- Who paid fees (NULL = sender)
    -- Tempo batch calls
    calls                   JSONB,                  -- [{to, value, input}, ...] for 0x76
    call_count              INT2 NOT NULL DEFAULT 1,
    -- Tempo time windows
    valid_before            INT8,                   -- Unix timestamp (NULL = no constraint)
    valid_after             INT8,                   -- Unix timestamp (NULL = no constraint)
    -- Signature info
    signature_type          INT2,                   -- 0=secp256k1, 1=P256, 2=WebAuthn
    PRIMARY KEY (block_num, idx)
) PARTITION BY RANGE (block_num);

-- Logs
CREATE TABLE logs (
    block_num       INT8 NOT NULL,
    block_timestamp TIMESTAMPTZ NOT NULL,
    log_idx         INT4 NOT NULL,
    tx_idx          INT4 NOT NULL,
    tx_hash         BYTEA NOT NULL,
    address         BYTEA NOT NULL,
    selector        BYTEA,          -- topic0[0..4] for fast lookups
    topics          BYTEA[] NOT NULL,
    data            BYTEA NOT NULL,
    PRIMARY KEY (block_num, log_idx)
) PARTITION BY RANGE (block_num);

-- Sync state (single row per network, but we only index one at a time)
CREATE TABLE sync_state (
    id              INT4 PRIMARY KEY DEFAULT 1,
    chain_id        INT8 NOT NULL,
    head_num        INT8 NOT NULL DEFAULT 0,
    synced_num      INT8 NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (id = 1)  -- Single row constraint
);
```

### Indexes

```sql
-- Fast selector queries (golden-axe pattern)
CREATE INDEX idx_logs_selector ON logs (selector, block_timestamp DESC);
CREATE INDEX idx_logs_address ON logs (address, block_timestamp DESC);

-- Transaction lookups
CREATE INDEX idx_txs_hash ON txs (hash) INCLUDE (block_num, idx);
CREATE INDEX idx_txs_from ON txs ("from", block_timestamp DESC);
CREATE INDEX idx_txs_to ON txs ("to", block_timestamp DESC);
CREATE INDEX idx_txs_selector ON txs (substring(input, 1, 4), block_timestamp DESC)
    WHERE length(input) >= 4;

-- Tempo-specific queries
CREATE INDEX idx_txs_fee_token ON txs (fee_token) WHERE fee_token IS NOT NULL;
CREATE INDEX idx_txs_sponsored ON txs (fee_payer) WHERE fee_payer IS NOT NULL;
CREATE INDEX idx_txs_scheduled ON txs (valid_after, valid_before) 
    WHERE valid_after IS NOT NULL OR valid_before IS NOT NULL;
CREATE INDEX idx_txs_batch ON txs (call_count) WHERE call_count > 1;

-- Block lookups
CREATE INDEX idx_blocks_hash ON blocks (hash);
CREATE INDEX idx_blocks_timestamp ON blocks (timestamp DESC);

-- BRIN for time-ordered scans (100-1000x smaller than B-tree)
CREATE INDEX idx_logs_block_brin ON logs USING brin (block_num) WITH (pages_per_range = 32);
CREATE INDEX idx_txs_block_brin ON txs USING brin (block_num) WITH (pages_per_range = 32);
```

### Dynamic Partitions (2M blocks per range)

```sql
-- Block range partitions (created as sync progresses)
CREATE TABLE blocks_b{label} PARTITION OF blocks
    FOR VALUES FROM ({from}) TO ({to});

CREATE TABLE txs_b{label} PARTITION OF txs
    FOR VALUES FROM ({from}) TO ({to});
ALTER TABLE txs_b{label} SET (toast_tuple_target = 128);

CREATE TABLE logs_b{label} PARTITION OF logs
    FOR VALUES FROM ({from}) TO ({to});
ALTER TABLE logs_b{label} SET (toast_tuple_target = 128);
```

---

## Implementation Phases

### Phase 0: Test Harness + Smoke Sync ✅
**Duration**: 1 day  
**Status**: Complete  
**Deliverable**: Working E2E test infrastructure + single block sync

#### What You Get
- `ak47 up --rpc <url> --db <url>` syncs latest block
- `ak47 status` shows sync state
- Ephemeral test DB via docker-compose (TimescaleDB + Tempo node)
- Real Tempo node for integration tests (no mocks)
- Criterion benchmark scaffolding

#### Implementation Tasks

**0.1: Docker Test Environment**
```yaml
# docker-compose.test.yml
services:
  timescaledb:
    image: timescale/timescaledb:latest-pg16
    environment:
      POSTGRES_USER: ak47
      POSTGRES_PASSWORD: ak47
      POSTGRES_DB: ak47_test
    ports:
      - "5433:5432"
    tmpfs:
      - /var/lib/postgresql/data  # Ephemeral for speed
```

**0.2: Mock RPC Server**
```rust
// tests/common/mock_rpc.rs
pub struct MockRpc {
    blocks: HashMap<u64, serde_json::Value>,
    chain_id: u64,
}

impl MockRpc {
    pub fn from_fixtures(dir: &Path) -> Self { ... }
    
    pub async fn serve(self) -> (String, JoinHandle<()>) {
        let app = Router::new()
            .route("/", post(handle_rpc));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        (url, tokio::spawn(axum::serve(listener, app)))
    }
}

async fn handle_rpc(Json(req): Json<RpcRequest>) -> Json<RpcResponse> {
    match req.method.as_str() {
        "eth_chainId" => json!({ "result": "0xa619" }), // 42429
        "eth_blockNumber" => json!({ "result": "0x100" }),
        "eth_getBlockByNumber" => { /* return fixture */ },
        _ => json!({ "error": { "code": -32601, "message": "Method not found" }}),
    }
}
```

**0.3: Test DB Helper**
```rust
// tests/common/testdb.rs
pub struct TestDb {
    pub pool: deadpool_postgres::Pool,
    pub url: String,
}

impl TestDb {
    pub async fn new() -> Self {
        let url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or("postgres://ak47:ak47@localhost:5433/ak47_test".into());
        let pool = create_pool(&url).await.unwrap();
        run_migrations(&pool).await.unwrap();
        Self { pool, url }
    }
    
    pub async fn truncate_all(&self) {
        self.pool.get().await.unwrap()
            .batch_execute("TRUNCATE blocks, txs, logs, sync_state CASCADE")
            .await.unwrap();
    }
    
    pub async fn block_count(&self) -> i64 {
        self.pool.get().await.unwrap()
            .query_one("SELECT COUNT(*) FROM blocks", &[])
            .await.unwrap().get(0)
    }
}
```

**0.4: Smoke Test**
```rust
// tests/smoke_test.rs
#[tokio::test]
async fn test_sync_single_block() {
    let db = TestDb::new().await;
    db.truncate_all().await;
    
    let (rpc_url, _handle) = MockRpc::from_fixtures("fixtures/blocks").serve().await;
    
    let engine = SyncEngine::new(db.pool.clone(), &rpc_url).await.unwrap();
    engine.sync_block(256).await.unwrap();
    
    assert_eq!(db.block_count().await, 1);
    
    let block = db.pool.get().await.unwrap()
        .query_one("SELECT num, timestamp_ms FROM blocks WHERE num = 256", &[])
        .await.unwrap();
    assert_eq!(block.get::<_, i64>(0), 256);
}
```

**0.5: Benchmark Scaffolding**
```rust
// benches/write_bench.rs
use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};

fn bench_block_write(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let db = rt.block_on(TestDb::new());
    
    let mut group = c.benchmark_group("block_writes");
    for batch_size in [10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::new("copy", batch_size),
            &batch_size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let blocks = generate_blocks(size);
                    write_blocks_copy(&db.pool, &blocks).await.unwrap();
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_block_write);
criterion_main!(benches);
```

#### Verification
```bash
# Start test DB
docker compose -f docker-compose.test.yml up -d

# Run smoke test
cargo test smoke_test

# Manual verification
cargo run -- up https://rpc.testnet.tempo.xyz --db postgres://ak47:ak47@localhost:5432/ak47
cargo run -- status
# Should show: Andantino (42429) | Head: <latest> | Synced: <latest>
```

#### Success Criteria
- [x] `cargo test` passes with ephemeral DB
- [x] Single block inserted with correct hash/timestamp (ms precision)
- [x] Sync state updated atomically
- [x] Benchmark harness runs without errors

---

### Phase 1: Block Backfill + Query ✅
**Duration**: 2-3 days  
**Status**: Complete  
**Deliverable**: Backfill blocks and query them

#### What You Get
- `ak47 sync forward --from N --to M` backfills block range
- `ak47 query "SELECT num, timestamp FROM blocks LIMIT 10"` works
- Batch RPC fetcher for efficient block retrieval
- Dynamic partition creation (2M blocks per partition)

---

### Phase 1.5: TimescaleDB Optimization ✅
**Duration**: 1 day  
**Status**: Complete  
**Deliverable**: Columnar storage for cold data, 90%+ compression

#### What You Get
- Tables converted to TimescaleDB hypertables
- Automatic compression for old chunks (columnar storage via Hypercore)
- Continuous aggregates for common analytics queries
- 90%+ storage reduction on historical data

#### Why TimescaleDB Hypercore?
- **Hot data (recent)**: Row-based storage for fast writes and point lookups
- **Cold data (historical)**: Columnar storage with 90%+ compression
- **Transparent**: Same SQL queries work on both, TimescaleDB handles routing
- **Automatic**: Compression policies run in background

#### Implementation Tasks

**1.5.1: Convert to Hypertables**
```sql
-- migrations/006_timescale.sql

-- Convert blocks to hypertable (2M block chunks)
SELECT create_hypertable('blocks', by_range('num', 2000000), migrate_data => true);

-- Convert txs to hypertable
SELECT create_hypertable('txs', by_range('block_num', 2000000), migrate_data => true);

-- Convert logs to hypertable  
SELECT create_hypertable('logs', by_range('block_num', 2000000), migrate_data => true);
```

**1.5.2: Enable Compression**
```sql
-- Compression settings for blocks
ALTER TABLE blocks SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'num DESC'
);

-- Compression settings for txs (segment by type for better compression)
ALTER TABLE txs SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'type',
    timescaledb.compress_orderby = 'block_num DESC, idx'
);

-- Compression settings for logs (segment by selector for event queries)
ALTER TABLE logs SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'selector',
    timescaledb.compress_orderby = 'block_num DESC, log_idx'
);

-- Auto-compress chunks older than 1M blocks behind head
SELECT add_compression_policy('blocks', INTERVAL '7 days');
SELECT add_compression_policy('txs', INTERVAL '7 days');
SELECT add_compression_policy('logs', INTERVAL '7 days');
```

**1.5.3: Continuous Aggregates**
```sql
-- Hourly transaction counts by type
CREATE MATERIALIZED VIEW txs_hourly
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 hour', block_timestamp) AS bucket,
    type,
    COUNT(*) as tx_count,
    SUM(gas_used) as total_gas
FROM txs
GROUP BY bucket, type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('txs_hourly',
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);

-- Daily block stats
CREATE MATERIALIZED VIEW blocks_daily
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 day', timestamp) AS bucket,
    COUNT(*) as block_count,
    AVG(gas_used) as avg_gas,
    MAX(gas_used) as max_gas
FROM blocks
GROUP BY bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('blocks_daily',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day'
);
```

**1.5.4: CLI Command for Compression**
```rust
// src/cli/compress.rs
pub async fn run_compress(pool: &Pool) -> Result<()> {
    let conn = pool.get().await?;
    
    // Get uncompressed chunks
    let chunks: Vec<String> = conn.query(
        "SELECT chunk_name FROM timescaledb_information.chunks 
         WHERE is_compressed = false 
         ORDER BY range_start",
        &[],
    ).await?.iter().map(|r| r.get(0)).collect();
    
    for chunk in chunks {
        info!(chunk = %chunk, "Compressing chunk");
        conn.execute(&format!("SELECT compress_chunk('{}')", chunk), &[]).await?;
    }
    
    Ok(())
}
```

#### Verification
```bash
# Check hypertable status
psql $DATABASE_URL -c "SELECT * FROM timescaledb_information.hypertables"

# Check compression ratio
psql $DATABASE_URL -c "
SELECT 
    hypertable_name,
    pg_size_pretty(before_compression_total_bytes) as before,
    pg_size_pretty(after_compression_total_bytes) as after,
    round(100 - (after_compression_total_bytes::float / before_compression_total_bytes * 100), 1) as compression_pct
FROM timescaledb_information.compression_settings cs
JOIN hypertable_compression_stats(cs.hypertable_name) ON true
"

# Query continuous aggregate
cargo run -- query "SELECT * FROM txs_hourly ORDER BY bucket DESC LIMIT 10"

# Compare query performance (columnar vs row)
EXPLAIN ANALYZE SELECT type, COUNT(*) FROM txs GROUP BY type;
```

#### Success Criteria
- [ ] Tables converted to hypertables
- [ ] Compression enabled with 90%+ ratio on old chunks
- [ ] Continuous aggregates populated
- [ ] Aggregate queries use materialized views (check EXPLAIN)
- [ ] No performance regression on writes

---

### Phase 2: Transactions + Logs ✅
**Duration**: 3-5 days  
**Status**: Complete  
**Deliverable**: Full block/tx/log indexing with Tempo-specific fields

#### What You Get
- Transactions decoded with all Tempo 0x76 fields (calls, 2D nonces, fee tokens)
- Custom `TempoBlock` and `TempoTransaction` types for RPC decoding
- Logs with pre-extracted selector (topic0[0..4])
- Tempo-specific queries work

#### Implementation Tasks (Completed)
- Created `src/tempo/block.rs` - TempoBlock with millisecond timestamps
- Created `src/tempo/transaction.rs` - TempoTransaction with calls, signatures
- Updated decoder to handle Tempo RPC responses
- Tested with 130k+ transactions indexed

---

### Phase 2 Reference: Binary COPY (Future Optimization)

**2.1: Binary COPY Writer**
```rust
// src/sync/writer.rs
pub async fn write_blocks_copy(pool: &Pool, blocks: &[BlockRow]) -> Result<u64> {
    let conn = pool.get().await?;
    
    let sink = conn.copy_in(
        "COPY blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner, extra_data) 
         FROM STDIN WITH (FORMAT binary)"
    ).await?;
    
    let writer = BinaryCopyInWriter::new(sink, &[
        Type::INT8, Type::BYTEA, Type::BYTEA, Type::TIMESTAMPTZ, Type::INT8,
        Type::NUMERIC, Type::NUMERIC, Type::BYTEA, Type::BYTEA,
    ]);
    pin_mut!(writer);
    
    for block in blocks {
        writer.as_mut().write(&[
            &block.num,
            &block.hash.as_slice(),
            &block.parent_hash.as_slice(),
            &block.timestamp,
            &block.timestamp_ms,
            &Decimal::from(block.gas_limit),
            &Decimal::from(block.gas_used),
            &block.miner.as_slice(),
            &block.extra_data.as_deref(),
        ]).await?;
    }
    
    writer.finish().await
}
```

**1.3: Dynamic Partition Manager**
```rust
// src/db/partitions.rs
const PARTITION_SIZE: u64 = 2_000_000;

impl PartitionManager {
    pub async fn ensure_partition(&self, block_num: u64) -> Result<()> {
        let partition_start = (block_num / PARTITION_SIZE) * PARTITION_SIZE;
        let partition_end = partition_start + PARTITION_SIZE;
        let label = format!("{}m", partition_start / 1_000_000);
        
        let sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS blocks_b{label}
                PARTITION OF blocks FOR VALUES FROM ({partition_start}) TO ({partition_end});
            CREATE TABLE IF NOT EXISTS txs_b{label}
                PARTITION OF txs FOR VALUES FROM ({partition_start}) TO ({partition_end});
            CREATE TABLE IF NOT EXISTS logs_b{label}
                PARTITION OF logs FOR VALUES FROM ({partition_start}) TO ({partition_end});
            "#
        );
        
        self.pool.get().await?.batch_execute(&sql).await?;
        Ok(())
    }
}
```

**1.4: CLI Commands**
```rust
// src/cli/sync.rs
#[derive(Subcommand)]
pub enum SyncCommands {
    Forward {
        #[arg(long)]
        from: u64,
        #[arg(long)]
        to: u64,
        #[arg(long, default_value = "1000")]
        batch_size: u64,
    },
}

pub async fn run_forward(from: u64, to: u64, batch_size: u64) -> Result<()> {
    let pool = get_pool().await?;
    let rpc = get_rpc_client().await?;
    let partitions = PartitionManager::new(pool.clone());
    
    for chunk_start in (from..=to).step_by(batch_size as usize) {
        let chunk_end = (chunk_start + batch_size - 1).min(to);
        
        partitions.ensure_partition(chunk_end).await?;
        
        let blocks = rpc.get_blocks_batch(chunk_start..=chunk_end).await?;
        let rows: Vec<BlockRow> = blocks.into_iter().map(Into::into).collect();
        
        let count = write_blocks_copy(&pool, &rows).await?;
        println!("Synced blocks {}-{} ({} rows)", chunk_start, chunk_end, count);
    }
    
    Ok(())
}
```

**1.5: Query Command**
```rust
// src/cli/query.rs
pub async fn run(sql: String) -> Result<()> {
    // Validate SQL (SELECT only)
    if !sql.trim().to_uppercase().starts_with("SELECT") {
        return Err(anyhow!("Only SELECT queries allowed"));
    }
    
    let pool = get_pool().await?;
    let rows = pool.get().await?.query(&sql, &[]).await?;
    
    // Print as table
    for row in rows {
        println!("{:?}", row);
    }
    
    Ok(())
}
```

#### Verification
```bash
# Backfill 1000 blocks
cargo run -- sync forward --from 0 --to 1000

# Query blocks
cargo run -- query "SELECT num, encode(hash, 'hex') FROM blocks ORDER BY num DESC LIMIT 5"

# Verify partition creation
psql -c "SELECT tablename FROM pg_tables WHERE tablename LIKE 'blocks_b%'"
```

#### Benchmarks
```bash
cargo bench --bench write_bench
# Target: > 5k blocks/sec COPY throughput
```

#### Success Criteria
- [ ] 1000 blocks synced in < 30s
- [ ] Re-running same range is idempotent (ON CONFLICT DO NOTHING)
- [ ] Partitions created automatically for block ranges

---

### Phase 2: Transactions + Logs
**Duration**: 3-5 days  
**Deliverable**: Full block/tx/log indexing with Tempo-specific fields

#### What You Get
- Transactions decoded with all Tempo 0x76 fields
- Logs with pre-extracted selector (topic0[0..4])
- Tempo-specific queries work

#### Implementation Tasks

**2.1: Tempo Transaction Decoder**
```rust
// src/sync/decoder.rs
use tempo_primitives::{TempoTxEnvelope, TempoTransaction, SignatureType};

pub fn decode_transaction(tx: &TempoTxEnvelope, block_num: u64, idx: u32) -> TxRow {
    match tx {
        TempoTxEnvelope::AA(signed) => {
            let inner = signed.tx();
            TxRow {
                block_num,
                idx,
                hash: signed.tx_hash().to_vec(),
                tx_type: 0x76,
                from: signed.recover_signer().unwrap().to_vec(),
                to: inner.calls.first().and_then(|c| c.to.to().map(|a| a.to_vec())),
                value: inner.calls.iter().fold(U256::ZERO, |acc, c| acc + c.value),
                input: inner.calls.first().map(|c| c.input.to_vec()).unwrap_or_default(),
                gas_limit: inner.gas_limit as i64,
                max_fee_per_gas: inner.max_fee_per_gas.into(),
                max_priority_fee_per_gas: inner.max_priority_fee_per_gas.into(),
                nonce_key: inner.nonce_key.to_be_bytes_vec(),
                nonce: inner.nonce as i64,
                fee_token: inner.fee_token.map(|a| a.to_vec()),
                fee_payer: signed.recover_fee_payer().ok().map(|a| a.to_vec()),
                calls: serde_json::to_value(&inner.calls).ok(),
                call_count: inner.calls.len() as i16,
                valid_before: inner.valid_before.map(|t| t as i64),
                valid_after: inner.valid_after.map(|t| t as i64),
                signature_type: Some(match signed.signature().signature_type() {
                    SignatureType::Secp256k1 => 0,
                    SignatureType::P256 => 1,
                    SignatureType::WebAuthn => 2,
                }),
                ..Default::default()
            }
        }
        // Handle legacy tx types
        TempoTxEnvelope::Legacy(signed) => { ... }
        TempoTxEnvelope::Eip1559(signed) => { ... }
        _ => { ... }
    }
}
```

**2.2: Log Decoder with Selector Extraction**
```rust
// src/sync/decoder.rs
pub fn decode_log(log: &Log, block_num: u64, tx_idx: u32, log_idx: u32) -> LogRow {
    let selector = log.topics().first()
        .map(|t| t.as_slice()[0..4].to_vec());
    
    LogRow {
        block_num,
        log_idx: log_idx as i32,
        tx_idx: tx_idx as i32,
        tx_hash: log.transaction_hash.unwrap().to_vec(),
        address: log.address().to_vec(),
        selector,
        topics: log.topics().iter().map(|t| t.to_vec()).collect(),
        data: log.data.data.to_vec(),
        ..Default::default()
    }
}
```

**2.3: Binary COPY for Txs and Logs**
```rust
// src/sync/writer.rs
pub async fn write_txs_copy(pool: &Pool, txs: &[TxRow]) -> Result<u64> {
    let conn = pool.get().await?;
    let sink = conn.copy_in(
        "COPY txs (block_num, block_timestamp, idx, hash, type, \"from\", \"to\", value, input,
                   gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used,
                   nonce_key, nonce, fee_token, fee_payer, calls, call_count,
                   valid_before, valid_after, signature_type)
         FROM STDIN WITH (FORMAT binary)"
    ).await?;
    
    // ... similar to blocks
}

pub async fn write_logs_copy(pool: &Pool, logs: &[LogRow]) -> Result<u64> {
    let conn = pool.get().await?;
    let sink = conn.copy_in(
        "COPY logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topics, data)
         FROM STDIN WITH (FORMAT binary)"
    ).await?;
    
    // ... similar to blocks
}
```

**2.4: Golden Test Fixtures**
```rust
// tests/decode_test.rs
#[test]
fn test_decode_tempo_batch_tx() {
    let fixture = include_bytes!("../fixtures/txs/tempo_batch.json");
    let tx: TempoTxEnvelope = serde_json::from_slice(fixture).unwrap();
    
    let row = decode_transaction(&tx, 1000, 0);
    
    assert_eq!(row.tx_type, 0x76);
    assert_eq!(row.call_count, 3);
    assert!(row.calls.is_some());
    assert!(row.nonce_key.len() == 32);
}

#[test]
fn test_decode_sponsored_tx() {
    let fixture = include_bytes!("../fixtures/txs/tempo_sponsored.json");
    let tx: TempoTxEnvelope = serde_json::from_slice(fixture).unwrap();
    
    let row = decode_transaction(&tx, 1000, 0);
    
    assert!(row.fee_payer.is_some());
    assert_ne!(row.fee_payer, Some(row.from.clone()));
}
```

#### Verification
```bash
# Sync range with transactions
cargo run -- sync forward --from 100 --to 200

# Query Tempo-specific fields
cargo run -- query "SELECT encode(hash, 'hex'), fee_token, call_count FROM txs WHERE call_count > 1 LIMIT 5"

# Query sponsored transactions
cargo run -- query "SELECT encode(hash, 'hex'), encode(fee_payer, 'hex') FROM txs WHERE fee_payer IS NOT NULL LIMIT 5"

# Query logs by selector
cargo run -- query "SELECT block_num, encode(address, 'hex'), encode(selector, 'hex') FROM logs LIMIT 10"

# Query time-windowed transactions
cargo run -- query "SELECT encode(hash, 'hex'), valid_after, valid_before FROM txs WHERE valid_after IS NOT NULL"
```

#### Success Criteria
- [ ] 0x76 transactions decoded with all Tempo fields
- [ ] Logs have `selector` populated (first 4 bytes of topic0)
- [ ] `call_count` matches `jsonb_array_length(calls)`
- [ ] Fee payer correctly extracted from sponsored txs
- [ ] `valid_before`/`valid_after` preserved

---

### Phase 3: Realtime Tail + Consistency ✅
**Duration**: 2-3 days  
**Status**: Complete  
**Deliverable**: Continuous sync with chain consistency guarantees

#### What You Get
- `ak47 up` runs continuously, tailing new blocks
- Parent-hash validation ensures chain consistency
- No reorg handling needed (Tempo has instant finality)
- Graceful shutdown with state persistence
- Gap detection and backfilling

#### Implementation Tasks

**3.1: Sync Engine**
```rust
// src/sync/engine.rs
pub struct SyncEngine {
    pool: Pool,
    rpc: RpcClient,
    partitions: PartitionManager,
    shutdown: broadcast::Receiver<()>,
}

impl SyncEngine {
    pub async fn run(&mut self) -> Result<()> {
        loop {
            tokio::select! {
                _ = self.shutdown.recv() => {
                    tracing::info!("Shutting down sync engine");
                    break;
                }
                result = self.tick() => {
                    if let Err(e) = result {
                        tracing::error!(error = %e, "Sync tick failed");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
        Ok(())
    }
    
    async fn tick(&mut self) -> Result<()> {
        let state = self.load_state().await?;
        let remote_head = self.rpc.latest_block_number().await?;
        
        if state.synced_num >= remote_head {
            // Caught up, poll faster
            tokio::time::sleep(Duration::from_millis(500)).await;
            return Ok(());
        }
        
        // Sync in batches
        let from = state.synced_num + 1;
        let to = (from + 100).min(remote_head);
        
        self.sync_range(from, to).await?;
        self.update_state(to, remote_head).await?;
        
        Ok(())
    }
}
```

**3.2: Parent Hash Validation**
```rust
// src/sync/engine.rs
async fn sync_range(&self, from: u64, to: u64) -> Result<()> {
    self.partitions.ensure_partition(to).await?;
    
    let blocks = self.rpc.get_blocks_batch(from..=to).await?;
    
    // Validate parent hash chain
    if from > 0 {
        let expected_parent = self.get_block_hash(from - 1).await?;
        let actual_parent = blocks.first().unwrap().parent_hash;
        
        if expected_parent != actual_parent {
            return Err(anyhow!(
                "Parent hash mismatch at block {}: expected {}, got {}",
                from, expected_parent, actual_parent
            ));
        }
    }
    
    // Validate internal chain
    for window in blocks.windows(2) {
        if window[1].parent_hash != window[0].hash {
            return Err(anyhow!("Internal chain break at block {}", window[1].number));
        }
    }
    
    // Write blocks, txs, logs
    self.write_batch(&blocks).await?;
    
    Ok(())
}
```

**3.3: Gap Detection**
```rust
// src/sync/engine.rs
async fn detect_gaps(&self) -> Result<Vec<(u64, u64)>> {
    let gaps = self.pool.get().await?.query(
        r#"
        WITH numbered AS (
            SELECT num, LAG(num) OVER (ORDER BY num) as prev_num
            FROM blocks
        )
        SELECT prev_num + 1 as gap_start, num - 1 as gap_end
        FROM numbered
        WHERE num - prev_num > 1
        "#,
        &[],
    ).await?;
    
    Ok(gaps.iter().map(|r| (r.get(0), r.get(1))).collect())
}

async fn fill_gaps(&mut self) -> Result<()> {
    let gaps = self.detect_gaps().await?;
    for (start, end) in gaps {
        tracing::info!(from = start, to = end, "Filling gap");
        self.sync_range(start, end).await?;
    }
    Ok(())
}
```

**3.4: Status Command**
```rust
// src/cli/status.rs
pub async fn run(watch: bool) -> Result<()> {
    loop {
        let pool = get_pool().await?;
        let state = pool.get().await?.query_one(
            "SELECT chain_id, head_num, synced_num, updated_at FROM sync_state WHERE id = 1",
            &[],
        ).await?;
        
        let chain_id: i64 = state.get(0);
        let head: i64 = state.get(1);
        let synced: i64 = state.get(2);
        let updated: DateTime<Utc> = state.get(3);
        
        let chain_name = match chain_id {
            4217 => "Presto",
            42429 => "Andantino",
            42431 => "Moderato",
            _ => "Unknown",
        };
        
        let lag = head - synced;
        let age = Utc::now() - updated;
        
        if watch {
            print!("\x1B[2J\x1B[1;1H"); // Clear screen
        }
        
        println!("AK47 Status");
        println!("═══════════════════════════════════════");
        println!("Network:    {} ({})", chain_name, chain_id);
        println!("Head:       {}", head);
        println!("Synced:     {}", synced);
        println!("Lag:        {} blocks", lag);
        println!("Updated:    {} ({} ago)", updated.format("%H:%M:%S"), format_duration(age));
        
        if !watch {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}
```

#### Verification
```bash
# Start continuous sync
cargo run -- up https://rpc.testnet.tempo.xyz --db postgres://...

# In another terminal, watch status
watch -n 1 'cargo run -- status'

# Verify no gaps
cargo run -- query "
    WITH gaps AS (
        SELECT num, LAG(num) OVER (ORDER BY num) as prev
        FROM blocks
    )
    SELECT COUNT(*) FROM gaps WHERE num - prev > 1
"
# Should return 0

# Verify parent hash chain
cargo run -- query "
    WITH chained AS (
        SELECT num, hash, parent_hash, 
               LAG(hash) OVER (ORDER BY num) as expected_parent
        FROM blocks
    )
    SELECT COUNT(*) FROM chained 
    WHERE num > (SELECT MIN(num) FROM blocks) 
      AND parent_hash != expected_parent
"
# Should return 0
```

#### Success Criteria
- [ ] New blocks indexed within 1s of production
- [ ] No gaps in block sequence
- [ ] Parent hash chain valid
- [ ] Graceful shutdown persists state

---

### Phase 4: Query Engine + JIT Selectors
**Duration**: 3-5 days  
**Deliverable**: Query logs by event signature with automatic decoding

#### What You Get
- `ak47 query-logs "Transfer(address,address,uint256)" --after 1h`
- JIT signature → selector conversion
- ABI decoding SQL functions (golden-axe pattern)
- Prepared statement cache

#### Implementation Tasks

**4.1: Signature Parser**
```rust
// src/query/parser.rs
use sha3::{Keccak256, Digest};

pub struct EventSignature {
    pub name: String,
    pub params: Vec<AbiParam>,
    pub selector: [u8; 4],
}

impl EventSignature {
    pub fn parse(sig: &str) -> Result<Self> {
        // Parse "Transfer(address,address,uint256)"
        let (name, params_str) = sig.split_once('(')
            .ok_or_else(|| anyhow!("Invalid signature"))?;
        let params_str = params_str.trim_end_matches(')');
        
        let params: Vec<AbiParam> = params_str.split(',')
            .filter(|s| !s.is_empty())
            .map(AbiParam::parse)
            .collect::<Result<_>>()?;
        
        // Calculate selector (keccak256 of canonical signature)
        let canonical = format!("{}({})", name, 
            params.iter().map(|p| p.canonical_type()).collect::<Vec<_>>().join(","));
        let hash = Keccak256::digest(canonical.as_bytes());
        let selector: [u8; 4] = hash[0..4].try_into().unwrap();
        
        Ok(Self { name: name.to_string(), params, selector })
    }
}

#[derive(Debug)]
pub struct AbiParam {
    pub name: Option<String>,
    pub ty: AbiType,
    pub indexed: bool,
}
```

**4.2: CTE Query Generator (Golden Axe Pattern)**
```rust
// src/query/abi.rs
impl EventSignature {
    pub fn to_cte_sql(&self) -> String {
        let mut columns = vec!["block_num", "block_timestamp", "log_idx", "tx_hash", "address"];
        let mut selects = vec![];
        
        let mut topic_idx = 1; // topic0 is selector
        let mut data_offset = 0;
        
        for param in &self.params {
            let col_name = param.name.as_deref().unwrap_or(&format!("arg{}", topic_idx));
            
            if param.indexed {
                // Indexed params come from topics
                let decode = param.ty.topic_decode_sql(topic_idx);
                selects.push(format!("{} AS {}", decode, col_name));
                topic_idx += 1;
            } else {
                // Non-indexed params come from data
                let decode = param.ty.data_decode_sql(data_offset);
                selects.push(format!("{} AS {}", decode, col_name));
                data_offset += 32; // Each slot is 32 bytes
            }
            columns.push(col_name);
        }
        
        format!(
            r#"{name} AS (
                SELECT block_num, block_timestamp, log_idx, tx_hash, address,
                       {selects}
                FROM logs
                WHERE selector = '\x{selector}'
            )"#,
            name = self.name,
            selects = selects.join(", "),
            selector = hex::encode(self.selector),
        )
    }
}

impl AbiType {
    fn topic_decode_sql(&self, topic_idx: usize) -> String {
        match self {
            AbiType::Address => format!("abi_address(topics[{}])", topic_idx),
            AbiType::Uint256 => format!("abi_uint(topics[{}])", topic_idx),
            AbiType::Bytes32 => format!("topics[{}]", topic_idx),
            _ => format!("topics[{}]", topic_idx),
        }
    }
    
    fn data_decode_sql(&self, offset: usize) -> String {
        match self {
            AbiType::Address => format!("abi_address(substring(data FROM {} FOR 32))", offset + 1),
            AbiType::Uint256 => format!("abi_uint(substring(data FROM {} FOR 32))", offset + 1),
            AbiType::Bool => format!("abi_bool(substring(data FROM {} FOR 32))", offset + 1),
            _ => format!("substring(data FROM {} FOR 32)", offset + 1),
        }
    }
}
```

**4.3: ABI Helper SQL Functions (Migration)**
```sql
-- migrations/V003__abi_functions.sql

CREATE OR REPLACE FUNCTION abi_uint(input BYTEA) RETURNS NUMERIC AS $$
DECLARE n NUMERIC := 0;
BEGIN
  FOR i IN 1..length(input) LOOP
    n := n * 256 + get_byte(input, i - 1);
  END LOOP;
  RETURN n;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION abi_int(input BYTEA) RETURNS NUMERIC AS $$
DECLARE
  n NUMERIC := 0;
  is_negative BOOLEAN;
BEGIN
  is_negative := get_byte(input, 0) >= 128;
  FOR i IN 1..length(input) LOOP
    n := n * 256 + get_byte(input, i - 1);
  END LOOP;
  IF is_negative THEN
    n := n - power(2::NUMERIC, length(input) * 8);
  END IF;
  RETURN n;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION abi_address(input BYTEA) RETURNS BYTEA AS $$
BEGIN
  RETURN substring(input FROM 13 FOR 20);
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION abi_bool(input BYTEA) RETURNS BOOLEAN AS $$
BEGIN
  RETURN get_byte(input, 31) != 0;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION abi_bytes(input BYTEA) RETURNS BYTEA AS $$
DECLARE length INT;
BEGIN
  length := get_byte(input, 31);
  RETURN substring(input FROM 33 FOR length);
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION abi_string(input BYTEA) RETURNS TEXT AS $$
BEGIN
  RETURN convert_from(abi_bytes(input), 'UTF8');
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;
```

**4.4: Query Logs CLI**
```rust
// src/cli/query.rs
pub async fn query_logs(
    signature: String,
    after: Option<String>,
    limit: Option<i64>,
) -> Result<()> {
    let sig = EventSignature::parse(&signature)?;
    let cte = sig.to_cte_sql();
    
    let after_interval = after.unwrap_or("24 hours".into());
    let limit = limit.unwrap_or(100);
    
    let sql = format!(
        "WITH {cte} SELECT * FROM {name} WHERE block_timestamp > now() - interval '{after_interval}' LIMIT {limit}",
        cte = cte,
        name = sig.name,
        after_interval = after_interval,
        limit = limit,
    );
    
    let pool = get_pool().await?;
    let rows = pool.get().await?.query(&sql, &[]).await?;
    
    // Print results
    for row in rows {
        println!("{:?}", row);
    }
    
    Ok(())
}
```

#### Verification
```bash
# Query Transfer events
cargo run -- query-logs "Transfer(address indexed from, address indexed to, uint256 value)" --after "1 hour" --limit 10

# Verify selector calculation
cargo run -- query "SELECT encode('\\xddf252ad'::bytea, 'hex')"
# Should match keccak256("Transfer(address,address,uint256)")[0:4]

# Verify uses index (check plan)
cargo run -- query "EXPLAIN ANALYZE SELECT * FROM logs WHERE selector = '\\xddf252ad' AND block_timestamp > now() - interval '1 hour'"
# Should show "Index Scan using idx_logs_selector"
```

#### Benchmarks
```bash
cargo bench --bench query_bench
# Target: Selector queries < 5ms for 24h range
```

#### Success Criteria
- [ ] Signature parsing matches keccak256 hash
- [ ] CTE generates correct decode expressions
- [ ] Query plan uses `idx_logs_selector` index
- [ ] p99 latency < 10ms for indexed queries

---

### Phase 5: HTTP API + Rate Limiting
**Duration**: 2-4 days  
**Deliverable**: Production-ready API with safety guardrails

#### What You Get
- `GET /status` - Sync status
- `POST /query` - Raw SQL (SELECT only)
- `GET /logs/:signature` - Event queries
- Rate limiting (Postgres-backed token buckets)
- Statement timeout + row limits

#### Implementation Tasks

**5.1: API Router**
```rust
// src/query/api.rs
pub fn router(pool: Pool) -> Router {
    let state = AppState { pool };
    
    Router::new()
        .route("/status", get(handle_status))
        .route("/query", post(handle_query))
        .route("/logs/:signature", get(handle_logs))
        .route("/health", get(|| async { "OK" }))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
```

**5.2: Query Endpoint with Safety**
```rust
// src/query/api.rs
#[derive(Deserialize)]
pub struct QueryRequest {
    sql: String,
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
    #[serde(default = "default_limit")]
    limit: i64,
}

fn default_timeout() -> u64 { 200 }
fn default_limit() -> i64 { 10000 }

async fn handle_query(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    // Validate: SELECT only
    let normalized = req.sql.trim().to_uppercase();
    if !normalized.starts_with("SELECT") {
        return Err(ApiError::BadRequest("Only SELECT queries allowed".into()));
    }
    
    // Reject dangerous patterns
    let forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER", "CREATE"];
    for word in forbidden {
        if normalized.contains(word) {
            return Err(ApiError::BadRequest(format!("{} not allowed", word)));
        }
    }
    
    // Add LIMIT if not present
    let sql = if !normalized.contains("LIMIT") {
        format!("{} LIMIT {}", req.sql, req.limit)
    } else {
        req.sql.clone()
    };
    
    // Execute with timeout
    let conn = state.pool.get().await?;
    conn.execute(&format!("SET statement_timeout = {}", req.timeout_ms), &[]).await?;
    
    let result = tokio::time::timeout(
        Duration::from_millis(req.timeout_ms + 100), // Buffer for network
        conn.query(&sql, &[])
    ).await;
    
    match result {
        Ok(Ok(rows)) => Ok(Json(QueryResponse::from_rows(rows))),
        Ok(Err(e)) => Err(ApiError::QueryError(e.to_string())),
        Err(_) => Err(ApiError::Timeout),
    }
}
```

**5.3: Rate Limiting (Postgres-backed)**
```sql
-- migrations/V004__rate_limits.sql
CREATE TABLE rate_limits (
    key TEXT PRIMARY KEY,
    tokens INT NOT NULL,
    max_tokens INT NOT NULL DEFAULT 100,
    last_refill TIMESTAMPTZ NOT NULL DEFAULT now(),
    refill_rate INT NOT NULL DEFAULT 10  -- tokens per second
);

CREATE OR REPLACE FUNCTION try_consume_token(p_key TEXT) RETURNS BOOLEAN AS $$
DECLARE
    v_tokens INT;
    v_max INT;
    v_rate INT;
    v_last TIMESTAMPTZ;
    v_now TIMESTAMPTZ := now();
    v_elapsed FLOAT;
    v_new_tokens INT;
BEGIN
    -- Upsert rate limit record
    INSERT INTO rate_limits (key, tokens, last_refill)
    VALUES (p_key, 99, v_now)  -- Start with max-1 tokens
    ON CONFLICT (key) DO NOTHING;
    
    -- Get current state
    SELECT tokens, max_tokens, refill_rate, last_refill
    INTO v_tokens, v_max, v_rate, v_last
    FROM rate_limits WHERE key = p_key FOR UPDATE;
    
    -- Calculate refill
    v_elapsed := EXTRACT(EPOCH FROM (v_now - v_last));
    v_new_tokens := LEAST(v_max, v_tokens + (v_elapsed * v_rate)::INT);
    
    -- Try to consume
    IF v_new_tokens >= 1 THEN
        UPDATE rate_limits SET tokens = v_new_tokens - 1, last_refill = v_now
        WHERE key = p_key;
        RETURN TRUE;
    ELSE
        UPDATE rate_limits SET last_refill = v_now WHERE key = p_key;
        RETURN FALSE;
    END IF;
END;
$$ LANGUAGE plpgsql;
```

```rust
// src/query/ratelimit.rs
pub async fn check_rate_limit(pool: &Pool, key: &str) -> Result<bool, ApiError> {
    let conn = pool.get().await?;
    let row = conn.query_one(
        "SELECT try_consume_token($1)",
        &[&key]
    ).await?;
    Ok(row.get(0))
}

// Usage in handler
async fn handle_query(...) -> Result<...> {
    let key = extract_api_key(&headers).unwrap_or("anonymous");
    if !check_rate_limit(&state.pool, key).await? {
        return Err(ApiError::RateLimited);
    }
    // ... rest of handler
}
```

#### Verification
```bash
# Start API server
cargo run -- up https://rpc.testnet.tempo.xyz --db postgres://... --port 8080

# Test status
curl http://localhost:8080/status

# Test query
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT num, encode(hash, '\''hex'\'') FROM blocks ORDER BY num DESC LIMIT 5"}'

# Test logs endpoint
curl "http://localhost:8080/logs/Transfer(address,address,uint256)?after=1h&limit=10"

# Test rate limit
for i in {1..200}; do curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/status; done | sort | uniq -c
# Should see 429 responses after limit exceeded

# Test timeout
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT pg_sleep(10)", "timeout_ms": 100}'
# Should return 408 or timeout error
```

#### Success Criteria
- [ ] Non-SELECT queries rejected with 400
- [ ] Queries timeout after specified duration
- [ ] Rate limit enforced and persists across restarts
- [ ] CORS headers present

---

### Phase 6: Polish + Observability
**Duration**: 1-2 days  
**Deliverable**: Production-ready deployment

#### What You Get
- Prometheus metrics (`/metrics`)
- Structured JSON logging
- Docker image + docker-compose
- Health checks

#### Implementation Tasks

**6.1: Prometheus Metrics**
```rust
// src/main.rs
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;

fn setup_metrics() {
    PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], 9090))
        .install()
        .expect("Failed to install Prometheus exporter");
}

// Usage throughout code:
// counter!("ak47_blocks_synced").increment(count);
// gauge!("ak47_sync_lag_blocks").set(lag as f64);
// histogram!("ak47_query_duration_ms").record(duration.as_millis() as f64);
```

**6.2: Structured Logging**
```rust
// src/main.rs
fn setup_logging() {
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .with_current_span(true)
        .with_span_list(true)
        .finish();
    
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set subscriber");
}
```

**6.3: Dockerfile**
```dockerfile
# Dockerfile
FROM rust:1.83-slim as builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/ak47 /usr/local/bin/
COPY migrations /migrations
EXPOSE 8080 9090
ENTRYPOINT ["ak47"]
```

**6.4: Docker Compose**
```yaml
# docker-compose.yml
services:
  timescaledb:
    image: timescale/timescaledb:latest-pg16
    environment:
      POSTGRES_USER: ak47
      POSTGRES_PASSWORD: ak47
      POSTGRES_DB: ak47
    volumes:
      - timescale_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ak47"]
      interval: 5s
      timeout: 5s
      retries: 5

  ak47:
    build: .
    depends_on:
      timescaledb:
        condition: service_healthy
    environment:
      DATABASE_URL: postgres://ak47:ak47@timescaledb:5432/ak47
      RUST_LOG: ak47=info
    ports:
      - "8080:8080"
      - "9090:9090"
    command: ["up", "https://rpc.testnet.tempo.xyz"]

volumes:
  timescale_data:
```

**6.5: Health Endpoints**
```rust
// src/query/api.rs
async fn handle_health() -> &'static str {
    "OK"
}

async fn handle_ready(State(state): State<AppState>) -> Result<&'static str, StatusCode> {
    // Check DB connection
    state.pool.get().await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        .query_one("SELECT 1", &[]).await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    
    Ok("READY")
}
```

#### Verification
```bash
# Build and run with Docker
docker compose up -d

# Check health
curl http://localhost:8080/health
curl http://localhost:8080/ready

# Check metrics
curl http://localhost:9090/metrics | grep ak47

# Check logs
docker compose logs -f ak47 | jq .

# Check sync status
curl http://localhost:8080/status
```

#### Success Criteria
- [ ] Metrics exported to Prometheus
- [ ] Logs are valid JSON with trace context
- [ ] Docker image < 100MB
- [ ] Health checks pass when service is ready

---

## Test Harness Architecture

### Directory Structure
```
tests/
├── common/
│   ├── mod.rs          # Test utilities
│   ├── testdb.rs       # Ephemeral DB setup
│   ├── mock_rpc.rs     # Mock JSON-RPC server
│   └── fixtures.rs     # Test data generators
├── smoke_test.rs       # Phase 0: Basic E2E
├── backfill_test.rs    # Phase 1: Block sync
├── txs_logs_test.rs    # Phase 2: Full data
├── consistency_test.rs # Phase 3: Chain integrity
└── query_test.rs       # Phase 4: Query engine
```

### Mock RPC Server
```rust
// tests/common/mock_rpc.rs
pub struct MockRpc {
    blocks: HashMap<u64, Block>,
    logs: Vec<Log>,
}

impl MockRpc {
    pub fn from_fixtures(dir: &Path) -> Self { ... }
    
    pub async fn serve(self, port: u16) -> JoinHandle<()> {
        // Axum router handling eth_chainId, eth_getBlockByNumber, eth_getLogs
    }
}
```

### Test DB Helper
```rust
// tests/common/testdb.rs
pub struct TestDb {
    pub pool: Pool,
    pub url: String,
}

impl TestDb {
    pub async fn new() -> Self {
        // Connect to docker-compose test DB
        // Run migrations
        // Return pool
    }
    
    pub async fn assert_block_count(&self, chain: u64, expected: i64) { ... }
    pub async fn assert_no_gaps(&self, chain: u64) { ... }
}
```

### Fixtures
```
fixtures/
├── blocks/
│   ├── block_0.json
│   ├── block_1.json
│   └── ...
├── txs/
│   ├── tempo_0x76_batch.json    # Batch transaction
│   ├── tempo_0x76_sponsored.json # Fee sponsorship
│   └── legacy_eip1559.json       # Standard EVM tx
└── golden_baselines.json         # Performance baselines
```

---

## Project Structure (Final)

```
ak47/
├── Cargo.toml
├── Dockerfile
├── docker-compose.yml
├── docker-compose.test.yml
│
├── src/
│   ├── main.rs
│   ├── lib.rs
│   │
│   ├── cli/
│   │   ├── mod.rs
│   │   ├── up.rs
│   │   ├── status.rs
│   │   ├── query.rs
│   │   └── sync.rs
│   │
│   ├── config.rs
│   ├── types.rs            # Row structs: BlockRow, TxRow, LogRow
│   │
│   ├── sync/
│   │   ├── mod.rs
│   │   ├── engine.rs       # Main sync loop
│   │   ├── fetcher.rs      # RpcClient impl
│   │   ├── decoder.rs      # tempo-primitives → row structs
│   │   └── writer.rs       # Binary COPY + partitions
│   │
│   ├── db/
│   │   ├── mod.rs
│   │   ├── pool.rs
│   │   └── migrations.rs   # Refinery runner
│   │
│   └── query/
│       ├── mod.rs
│       ├── api.rs          # HTTP endpoints
│       ├── parser.rs       # Signature parsing
│       └── ratelimit.rs
│
├── migrations/
│   ├── V001__tables.sql    # blocks, txs, logs, sync_state
│   ├── V002__indexes.sql
│   ├── V003__abi_functions.sql
│   └── V004__rate_limits.sql
│
├── benches/
│   ├── write_bench.rs
│   ├── query_bench.rs
│   └── decode_bench.rs
│
├── tests/
│   ├── common/
│   │   ├── mod.rs
│   │   ├── testdb.rs
│   │   └── mock_rpc.rs
│   ├── smoke_test.rs
│   ├── backfill_test.rs
│   └── query_test.rs
│
├── fixtures/
│   ├── blocks/
│   └── txs/
│
└── docs/
    └── PLAN.md
```

---

## Dependencies

```toml
[dependencies]
# Async runtime
tokio = { version = "1", features = ["full"] }

# HTTP
axum = "0.8"
reqwest = { version = "0.12", features = ["json", "gzip"] }
tower = "0.5"
tower-http = { version = "0.6", features = ["cors", "trace"] }

# Database
tokio-postgres = { version = "0.7", features = ["with-chrono-0_4"] }
deadpool-postgres = "0.14"
refinery = { version = "0.8", features = ["tokio-postgres"] }

# Tempo primitives (reuse, don't reinvent)
tempo-primitives = { git = "https://github.com/tempoxyz/tempo", features = ["serde"] }
alloy = { version = "1", features = ["full"] }

# Serialization
serde = { version = "1", features = ["derive"] }
serde_json = "1"
toml = "0.8"

# CLI
clap = { version = "4", features = ["derive", "env"] }

# Observability
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
metrics = "0.24"
metrics-exporter-prometheus = "0.16"

# Crypto (for selector hashing)
sha3 = "0.10"

# Error handling
thiserror = "2"
anyhow = "1"

# Rate limiting
governor = "0.7"

# Async traits
trait-variant = "0.1"

[dev-dependencies]
criterion = { version = "0.5", features = ["async_tokio", "html_reports"] }
proptest = "1"
test-case = "3"
rand = "0.8"
```

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Binary COPY correctness | Round-trip decode/encode tests; compare with text COPY |
| Partition creation race | Single sync engine; CREATE IF NOT EXISTS |
| Query timeout bypass | Postgres `statement_timeout` at connection level |
| RPC rate limits | Exponential backoff; configurable concurrency |
| Large batch OOM | Bounded channels; configurable batch size |

---

## Future Considerations (Post-MVP)

- [ ] Multi-chain support (non-Tempo EVM chains)
- [ ] Reth snapshot hydration for fast historical sync
- [ ] TimescaleDB continuous aggregates for analytics
- [ ] WebSocket subscriptions for live queries
- [ ] GraphQL API layer
