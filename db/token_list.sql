-- Curated token-list registry, modelled loosely after the Uniswap Token
-- Lists spec (https://github.com/Uniswap/token-lists). One row per
-- (source, chain_id, address) — keyed on `source` so multiple registries
-- (Trust Wallet today, Uniswap/1inch/CoinGecko tomorrow) can coexist
-- without stepping on each other for the same token.
--
-- Populated by per-source workers (first up: `src/sync/tw_assets.rs`,
-- which writes `source = 'trust_wallet'`). Read-side the table is LEFT
-- JOINed onto `/erc20/tokens` responses to enrich on-chain metadata with
-- human-curated logos, links, and status.
--
-- Spec-shaped fields map directly; source-specific fields (website,
-- description, explorer, status, links, asset_type) live alongside for
-- now and will migrate into an `extensions JSONB` column in a follow-up
-- when a second source actually needs them.
CREATE TABLE IF NOT EXISTS token_list (
    -- Provenance: 'trust_wallet' | 'uniswap' | '1inch' | ...
    source      TEXT NOT NULL,
    chain_id    INT8 NOT NULL,
    address     BYTEA NOT NULL,

    -- Per-source change-detection token. For the trustwallet source this
    -- is the Git blob SHA of the upstream info.json; other sources can
    -- use list versions, content hashes, etc. The worker skips re-fetching
    -- when the stored value matches the upstream value.
    source_sha  TEXT NOT NULL,

    -- Uniswap Token Lists TokenInfo: spec-required fields (all nullable
    -- here since individual sources sometimes omit them and we still
    -- want the row for completeness).
    name        TEXT,
    symbol      TEXT,
    decimals    INT2,

    -- Uniswap Token Lists TokenInfo: spec-optional fields.
    logo_uri    TEXT,   -- matches the spec's `logoURI`
    tags        JSONB,  -- flat string array per spec

    -- Source-specific fields. Kept typed for now; when we add a second
    -- source that doesn't publish these, they'll move into `extensions`.
    asset_type  TEXT,   -- info.json's "type" (ERC20, BEP20, coin, ...)
    website     TEXT,
    description TEXT,
    explorer    TEXT,
    status      TEXT,   -- "active" | "spam" | "abandoned"
    links       JSONB,  -- [{name, url}, ...]

    fetched_at  TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (source, chain_id, address)
);

-- Enables `?exclude_spam` / `?status=active` filters cheaply.
CREATE INDEX IF NOT EXISTS idx_token_list_status
    ON token_list (status);

-- Cross-source lookups: "what does this address look like across every
-- registry we mirror?" — trivially `SELECT * FROM token_list WHERE
-- chain_id = $1 AND address = $2`.
CREATE INDEX IF NOT EXISTS idx_token_list_chain_address
    ON token_list (chain_id, address);

COMMENT ON COLUMN token_list.source IS
    'Provenance tag: trustwallet | uniswap | 1inch | ... — part of the composite primary key';
COMMENT ON COLUMN token_list.source_sha IS
    'Per-source change-detection token (e.g. Git blob SHA for trustwallet); used to skip redundant re-fetches';
COMMENT ON COLUMN token_list.logo_uri IS
    'Spec-compliant logoURI (Uniswap Token Lists). Populated at insert time by the worker';
COMMENT ON COLUMN token_list.status IS
    'Source-reported status where applicable: active | spam | abandoned';
