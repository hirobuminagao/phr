

-- =============================================================
-- Migration: add identity_hash columns to hia_dashboard_status and hia_person_years
-- Target DB : work_other
-- Purpose   : Add join optimization hash for identity matching
-- Related   : ADR-0012 Identity Canonicalization and Join Hash Policy
-- =============================================================

ALTER TABLE work_other.hia_dashboard_status
ADD COLUMN IF NOT EXISTS identity_hash CHAR(64)
    CHARACTER SET ascii
    COLLATE ascii_bin
    NULL
    COMMENT 'SHA256 hash of (person_id_custom|name_kana_full_match|gender_code) used for fast identity joins';

CREATE INDEX IF NOT EXISTS idx_hia_dashboard_identity_hash
ON work_other.hia_dashboard_status (identity_hash);


ALTER TABLE work_other.hia_person_years
ADD COLUMN IF NOT EXISTS identity_hash CHAR(64)
    CHARACTER SET ascii
    COLLATE ascii_bin
    NULL
    COMMENT 'SHA256 hash of (person_id_custom|name_kana_full_match|gender_code) used for fast identity joins';

CREATE INDEX IF NOT EXISTS idx_hia_person_years_identity_hash
ON work_other.hia_person_years (identity_hash);