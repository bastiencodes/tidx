CREATE TABLE IF NOT EXISTS logs (
    block_num       INT8 NOT NULL,
    block_timestamp TIMESTAMPTZ NOT NULL,
    log_idx         INT4 NOT NULL,
    tx_idx          INT4 NOT NULL,
    tx_hash         BYTEA NOT NULL,
    address         BYTEA NOT NULL,
    selector        BYTEA,
    topics          BYTEA[] NOT NULL,
    data            BYTEA NOT NULL,
    PRIMARY KEY (block_timestamp, block_num, log_idx)
);

CREATE INDEX IF NOT EXISTS idx_logs_block_num ON logs (block_num);
CREATE INDEX IF NOT EXISTS idx_logs_tx_hash ON logs (tx_hash);
CREATE INDEX IF NOT EXISTS idx_logs_selector ON logs (selector, block_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_address ON logs (address, block_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_address_topic ON logs ((topics[1]), address, block_num DESC);
CREATE INDEX IF NOT EXISTS idx_logs_topic_2 ON logs ((topics[2]));
CREATE INDEX IF NOT EXISTS idx_logs_topic_3 ON logs ((topics[3]));
CREATE INDEX IF NOT EXISTS idx_logs_topic_4 ON logs ((topics[4]));
