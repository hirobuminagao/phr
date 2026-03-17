

-- =============================================================
-- Migration: add subscriber_name_kana_full_match to hia_dashboard_status
-- Target DB : work_other
-- Purpose   : Store subscriber-side kana match on dashboard rows so identity_hash can be generated consistently
-- Related   : ADR-0012 Identity Canonicalization and Join Hash Policy
-- =============================================================

SET @sql := IF (
    EXISTS (
        SELECT 1
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = 'work_other'
          AND TABLE_NAME = 'hia_dashboard_status'
          AND COLUMN_NAME = 'subscriber_name_kana_full_match'
    ),
    'SELECT ''subscriber_name_kana_full_match already exists''',
    'ALTER TABLE work_other.hia_dashboard_status
        ADD COLUMN subscriber_name_kana_full_match VARCHAR(190)
            CHARACTER SET utf8mb4
            COLLATE utf8mb4_ja_0900_as_cs
            DEFAULT NULL
            COMMENT ''subscriber-side kana match value copied from dev_phr.subscribers for identity matching and hash generation''' 
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
          AND INDEX_NAME = 'idx_hia_dashboard_subscriber_name_kana_full_match'
    ),
    'SELECT ''idx_hia_dashboard_subscriber_name_kana_full_match already exists''',
    'CREATE INDEX idx_hia_dashboard_subscriber_name_kana_full_match
        ON work_other.hia_dashboard_status (subscriber_name_kana_full_match)'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;