

-- =========================================================
-- Table: hia_delivery_exclusion_rules
-- Purpose:
--   Configurable exclusion rules used when rebuilding
--   delivery ZIP packages for insurers.
--
--   These rules allow operations to exclude records
--   (e.g., non-contracted facilities) without changing code.
-- =========================================================

CREATE TABLE IF NOT EXISTS hia_delivery_exclusion_rules (

    -- surrogate key
    exclusion_rule_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

    -- insurer scope
    insurer_number CHAR(8) COLLATE ascii_bin NOT NULL,

    -- target dataset description
    target_schema VARCHAR(64) COLLATE ascii_bin NOT NULL,
    target_table VARCHAR(128) COLLATE ascii_bin NOT NULL,
    target_column VARCHAR(128) COLLATE ascii_bin NOT NULL,

    -- matching definition
    match_type VARCHAR(20) COLLATE ascii_bin NOT NULL DEFAULT 'EQUAL',
    match_value VARCHAR(255) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,

    -- explanation / operations memo
    exclusion_reason VARCHAR(255) COLLATE utf8mb4_ja_0900_as_cs,
    source_note VARCHAR(255) COLLATE utf8mb4_ja_0900_as_cs,

    -- control flag
    is_enabled TINYINT(1) NOT NULL DEFAULT 1,

    -- timestamps
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- primary key
    PRIMARY KEY (exclusion_rule_id),

    -- operational lookup
    INDEX idx_hia_exclusion_insurer (insurer_number),
    INDEX idx_hia_exclusion_target (target_schema, target_table, target_column),
    INDEX idx_hia_exclusion_enabled (is_enabled)

) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;