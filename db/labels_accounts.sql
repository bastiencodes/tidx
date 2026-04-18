-- Address labels for EOAs and protocol contracts (exchanges, bridges,
-- multisigs, etc.). Populated from eth-labels' accounts.json
-- (https://github.com/dawsbot/eth-labels) via `tidx seed-labels`.
--
-- Chain is implicit from which per-chain Postgres DB this table lives in;
-- the seed CLI filters upstream rows by chainId before inserting.
CREATE TABLE IF NOT EXISTS labels_accounts (
    address  BYTEA PRIMARY KEY,
    label    TEXT NOT NULL,
    name_tag TEXT NOT NULL,
    source   TEXT NOT NULL
);

COMMENT ON TABLE labels_accounts IS
    'Address labels. Populated via `tidx seed-labels`.';
COMMENT ON COLUMN labels_accounts.label IS
    'Project slug, e.g. "uniswap", "binance", "0x-protocol".';
COMMENT ON COLUMN labels_accounts.name_tag IS
    'Human-readable tag, e.g. "Binance: Hot Wallet 14".';
COMMENT ON COLUMN labels_accounts.source IS
    'Provenance of this row, e.g. "eth-labels".';
