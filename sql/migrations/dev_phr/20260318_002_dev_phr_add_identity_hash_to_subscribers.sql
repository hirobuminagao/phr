


-- =============================================================
-- Migration: add identity_hash column to subscribers
-- Target DB : dev_phr
-- Purpose   : Add join optimization hash for identity matching
-- Related   : ADR-0012 Identity Canonicalization and Join Hash Policy
-- =============================================================

ALTER TABLE dev_phr.subscribers
ADD COLUMN IF NOT EXISTS identity_hash CHAR(64)
    CHARACTER SET ascii
    COLLATE ascii_bin
    NULL
    COMMENT 'SHA256 hash of (person_id_custom|name_kana_full_match|gender_code) used for fast identity joins';


-- Optional index for fast join
CREATE INDEX IF NOT EXISTS idx_subscribers_identity_hash
ON dev_phr.subscribers (identity_hash);