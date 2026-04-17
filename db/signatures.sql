-- Canonical keccak hash → text signature, populated from the Sourcify
-- Parquet export (https://docs.sourcify.dev/docs/repository/download-dataset/,
-- table `signatures` in v2). The 32-byte hash is topic0 for events; its
-- first 4 bytes are the function/error selector.
--
-- Type (function/event/error) is not stored here. Context at decode time
-- disambiguates: lookups from tx.input[0:4] are function/error candidates,
-- lookups from log.topic0 are event candidates. Multiple text sigs may
-- share a 4-byte prefix — the decoder tries each against the bytes via
-- alloy-dyn-abi and keeps the one that parses.
CREATE TABLE IF NOT EXISTS signatures (
    signature_hash_32 BYTEA PRIMARY KEY,
    signature_hash_4  BYTEA GENERATED ALWAYS AS (SUBSTRING(signature_hash_32 FROM 1 FOR 4)) STORED,
    signature         TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_signatures_hash_4
    ON signatures (signature_hash_4);

COMMENT ON TABLE signatures IS
    'Sourcify signature cache. Populated via `tidx seed-signatures`.';
COMMENT ON COLUMN signatures.signature_hash_32 IS
    'Full keccak256 of the canonical signature text. Equals log.topic0 for events.';
COMMENT ON COLUMN signatures.signature_hash_4 IS
    'First 4 bytes of signature_hash_32. Equals tx.input[0:4] for functions and the selector for custom errors.';
COMMENT ON COLUMN signatures.signature IS
    'Canonical text signature, e.g. transfer(address,uint256).';
