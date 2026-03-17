
-- =============================================================
-- Migration: add identity_hash columns to hia_dashboard_status and hia_person_years
-- Target DB : work_other
-- Purpose   : Add join optimization hash for identity matching
-- Related   : ADR-0012 Identity Canonicalization and Join Hash Policy
-- =============================================================

-- hia_dashboard_status
SET @sql := IF (
    EXISTS (
        SELECT 1
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = 'work_other'
          AND TABLE_NAME = 'hia_dashboard_status'
          AND COLUMN_NAME = 'identity_hash'
    ),
    'SELECT ''identity_hash already exists (dashboard)''',
    'ALTER TABLE work_other.hia_dashboard_status
        ADD COLUMN identity_hash CHAR(64)
            CHARACTER SET ascii
            COLLATE ascii_bin
            NULL
            COMMENT ''SHA256 hash of (person_id_custom|name_kana_full_match|gender_code) used for fast identity joins''' 
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql := IF (
    EXISTS (
        SELECT 1
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = 'work_other'
          AND TABLE_NAME = 'hia_dashboard_status'
          AND INDEX_NAME = 'idx_hia_dashboard_identity_hash'
    ),
    'SELECT ''index already exists (dashboard)''',
    'CREATE INDEX idx_hia_dashboard_identity_hash
        ON work_other.hia_dashboard_status (identity_hash)'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


-- hia_person_years
SET @sql := IF (
    EXISTS (
        SELECT 1
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = 'work_other'
          AND TABLE_NAME = 'hia_person_years'
          AND COLUMN_NAME = 'identity_hash'
    ),
    'SELECT ''identity_hash already exists (person_years)''',
    'ALTER TABLE work_other.hia_person_years
        ADD COLUMN identity_hash CHAR(64)
            CHARACTER SET ascii
            COLLATE ascii_bin
            NULL
            COMMENT ''SHA256 hash of (person_id_custom|name_kana_full_match|gender_code) used for fast identity joins''' 
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql := IF (
    EXISTS (
        SELECT 1
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = 'work_other'
          AND TABLE_NAME = 'hia_person_years'
          AND INDEX_NAME = 'idx_hia_person_years_identity_hash'
    ),
    'SELECT ''index already exists (person_years)''',
    'CREATE INDEX idx_hia_person_years_identity_hash
        ON work_other.hia_person_years (identity_hash)'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;