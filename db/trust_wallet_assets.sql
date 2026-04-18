-- Trust Wallet assets registry mirror.
--
-- Enriches erc20_tokens with human-curated metadata pulled from the
-- trustwallet/assets GitHub repo, populated by
-- src/sync/trustwallet_metadata.rs.
--
-- Absence of a row for (chain_id, address) means Trust Wallet does not
-- list the token; we intentionally don't maintain a negative cache since
-- the worker uses the GitHub Git Trees API to enumerate upstream
-- membership before touching individual info.json blobs.
--
-- Logo URL is NOT stored: it's deterministic from the chain slug and the
-- EIP-55 checksummed address, and composed at API response time.
CREATE TABLE IF NOT EXISTS trust_wallet_assets (
    chain_id    INT8 NOT NULL,
    address     BYTEA NOT NULL,

    -- SHA of the upstream info.json blob (from the Git Trees API). The
    -- worker skips re-fetching info.json when the stored sha matches the
    -- upstream sha.
    info_sha    TEXT NOT NULL,

    -- Typed fields extracted from info.json.
    name        TEXT,
    symbol      TEXT,
    decimals    INT2,
    asset_type  TEXT,          -- info.json's "type" (ERC20, BEP20, coin, ...)
    website     TEXT,
    description TEXT,
    explorer    TEXT,
    status      TEXT,          -- "active" | "spam" | "abandoned"

    -- Flat string array from info.json's "tags".
    tags        JSONB,

    -- Array of {name, url} from info.json's "links" (github, x, discord, ...).
    links       JSONB,

    fetched_at  TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (chain_id, address)
);

-- Enables future filters like ?exclude_spam or ?status=active.
CREATE INDEX IF NOT EXISTS idx_trust_wallet_assets_status
    ON trust_wallet_assets (status);

COMMENT ON COLUMN trust_wallet_assets.info_sha IS
    'Upstream Git blob SHA of info.json, used for change detection across refresh ticks';
COMMENT ON COLUMN trust_wallet_assets.status IS
    'Trust Wallet-reported status: active | spam | abandoned';
COMMENT ON COLUMN trust_wallet_assets.asset_type IS
    'info.json "type" field (ERC20, BEP20, SPL, coin, ...); renamed to avoid the SQL/Rust reserved word';
