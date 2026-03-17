-- =============================================================
-- Migration: add identity_hash column to subscribers
-- Target DB : dev_phr
-- Purpose   : Add join optimization hash for identity matching
-- Related   : ADR-0012 Identity Canonicalization and Join Hash Policy
-- =============================================================

SET @sql := IF (
    EXISTS (
        SELECT 1
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = 'dev_phr'
          AND TABLE_NAME = 'subscribers'
          AND COLUMN_NAME = 'identity_hash'
    ),
    'SELECT ''identity_hash column already exists''',
    'ALTER TABLE dev_phr.subscribers
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
        WHERE TABLE_SCHEMA = 'dev_phr'
          AND TABLE_NAME = 'subscribers'
          AND INDEX_NAME = 'idx_subscribers_identity_hash'
    ),
    'SELECT ''idx_subscribers_identity_hash already exists''',
    'CREATE INDEX idx_subscribers_identity_hash
        ON dev_phr.subscribers (identity_hash)'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;