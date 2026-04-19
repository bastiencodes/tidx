-- ENS primary-name cache (reverse resolution: address → name).
--
-- Populated lazily by the resolver in `src/ens.rs` on the `?ens=true`
-- enrichment path. Each row is the result of a reverse-then-forward
-- resolution round-trip against the ENS Registry on this chain:
--
--   1. reverse: `<addr>.addr.reverse` node → resolver.name() → TEXT
--   2. forward: namehash(<name>) → resolver.addr() → ADDR'
--   3. verified := (addr == ADDR')
--
-- The `verified` flag is a point-in-time assertion. It reflects chain
-- state as of `resolved_at` and MAY be up to `stale_after_secs`
-- (default 24h) out of date — in particular, if the ENS name is
-- transferred to a new owner inside the TTL window, this table may
-- still show `verified = true` for the previous owner's address.
-- Callers whose UX depends on authoritative identity should not rely
-- on this cache; they should resolve against RPC directly.
--
-- Rows are never deleted; they are refreshed in-place. Addresses with
-- no primary name are stored as `name = NULL` (negative cache) so we
-- don't re-resolve them on every request.
--
-- Chain is implicit from the per-chain Postgres DB this table lives in.
-- ENS is only wired for mainnet (chain_id = 1) in v1; sepolia has its
-- own registry deployment and is out of scope for now.
CREATE TABLE IF NOT EXISTS ens_records (
    address         BYTEA       NOT NULL PRIMARY KEY,   -- 20 bytes
    name            TEXT,                               -- NULL = no primary name set
    verified        BOOLEAN     NOT NULL,               -- forward(name) == address
    resolved_at     TIMESTAMPTZ NOT NULL,               -- when the RPC read ran
    resolved_block  INT8        NOT NULL                -- from Multicall3.getBlockNumber()
);

-- Supports forward-style queries ("which address does `alice.eth` map to,
-- according to our cache?") without a full table scan. Partial index so
-- we don't waste space on the negative-cache rows.
CREATE INDEX IF NOT EXISTS idx_ens_records_name
    ON ens_records (name)
    WHERE name IS NOT NULL;

-- Supports the staleness sweep: "find rows whose resolved_at is older
-- than now() - stale_after_secs so we can refresh them."
CREATE INDEX IF NOT EXISTS idx_ens_records_resolved_at
    ON ens_records (resolved_at);

COMMENT ON TABLE ens_records IS
    'ENS reverse-resolution cache. Populated lazily by the ?ens=true enrichment path. Mainnet only in v1.';
COMMENT ON COLUMN ens_records.name IS
    'ENS primary name for this address, or NULL if none is set (negative cache).';
COMMENT ON COLUMN ens_records.verified IS
    'TRUE iff forward resolution of `name` returns this address. Point-in-time assertion; see resolved_at for staleness.';
COMMENT ON COLUMN ens_records.resolved_at IS
    'Wall-clock time the RPC resolution ran. Up to stale_after_secs (default 24h) out of date.';
COMMENT ON COLUMN ens_records.resolved_block IS
    'Block number Multicall3.getBlockNumber() reported in the same atomic batch as the name/addr reads.';
