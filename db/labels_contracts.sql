-- Contract-level labels with rich metadata: ERC20 tokens, NFT collections,
-- and other named contracts. Populated from eth-labels' tokens.json
-- (https://github.com/dawsbot/eth-labels) via `tidx seed-labels`.
--
-- NB: the upstream file is called tokens.json but its content is not
-- ERC20-only — NFT collections (ERC721/ERC1155) are mixed in with no
-- standard marker. We store them all together.
--
-- Chain is implicit from which per-chain Postgres DB this table lives in.
CREATE TABLE IF NOT EXISTS labels_contracts (
    address   BYTEA PRIMARY KEY,
    label     TEXT NOT NULL,
    name_tag  TEXT NOT NULL,
    name      TEXT,
    symbol    TEXT,
    website   TEXT,
    image_url TEXT,
    source    TEXT NOT NULL
);

COMMENT ON TABLE labels_contracts IS
    'Contract labels with metadata (tokens, NFT collections). Populated via `tidx seed-labels`.';
COMMENT ON COLUMN labels_contracts.label IS
    'Project slug, e.g. "uniswap", "aave".';
COMMENT ON COLUMN labels_contracts.name_tag IS
    'Human-readable tag synthesized at seed time from name/symbol.';
COMMENT ON COLUMN labels_contracts.image_url IS
    'Absolute URL to logo/image, or NULL when unavailable.';
COMMENT ON COLUMN labels_contracts.source IS
    'Provenance of this row, e.g. "eth-labels".';
