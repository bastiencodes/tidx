CREATE TABLE IF NOT EXISTS txs (
    block_num               Int64,
    block_timestamp         DateTime64(3, 'UTC'),
    idx                     Int32,
    hash                    String,
    `type`                  Int16,
    `from`                  String,
    `to`                    Nullable(String),
    value                   String,
    input                   String,
    gas_limit               Int64,
    max_fee_per_gas         String,
    max_priority_fee_per_gas String,
    gas_used                Nullable(Int64),
    nonce                   Int64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (block_num, idx);
