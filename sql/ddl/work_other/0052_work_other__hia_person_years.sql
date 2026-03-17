-- =========================================================
-- Table: hia_person_years
-- Purpose:
--   Person + exam_year ledger for HIA_fund_ledger_xml pipeline
--   One record represents one person within one exam year.
-- =========================================================

CREATE TABLE IF NOT EXISTS hia_person_years (

    -- surrogate key
    person_year_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

    -- identity (normalized matching key)
    person_id_custom VARCHAR(128) COLLATE ascii_bin NOT NULL,
    name_kana_norm VARCHAR(120) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,
    gender_code VARCHAR(10) COLLATE ascii_bin NOT NULL,
    exam_year INT NOT NULL,

    -- raw identity fields (kept for traceability)
    insurer_number CHAR(8) COLLATE ascii_bin NOT NULL,
    insurance_symbol VARCHAR(20) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,
    insurance_number VARCHAR(20) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,
    insurance_symbol_match VARCHAR(40) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,
    insurance_number_match VARCHAR(20) COLLATE ascii_bin NOT NULL,
    birthdate DATE NOT NULL,

    -- optional metadata from XML (can be NULL if not present)
    report_category VARCHAR(10) COLLATE ascii_bin,
    health_program_code VARCHAR(50) COLLATE utf8mb4_ja_0900_as_cs,

    -- original values (non-normalized)
    name_kana_raw VARCHAR(120) COLLATE utf8mb4_ja_0900_as_cs,

    -- identity hash (for fast join)
    identity_hash CHAR(64)
        CHARACTER SET ascii
        COLLATE ascii_bin
        DEFAULT NULL
        COMMENT 'SHA256 hash of (person_id_custom|name_kana_full_match|gender_code)',

    -- statistics
    dl_count INT UNSIGNED NOT NULL DEFAULT 0,

    -- first seen info
    first_seen_dl_date DATE,
    first_seen_zip_name VARCHAR(255) COLLATE utf8mb4_ja_0900_as_cs,
    first_seen_xml_filename VARCHAR(255) COLLATE utf8mb4_ja_0900_as_cs,

    -- last seen info
    last_seen_dl_date DATE,
    last_seen_zip_name VARCHAR(255) COLLATE utf8mb4_ja_0900_as_cs,
    last_seen_xml_filename VARCHAR(255) COLLATE utf8mb4_ja_0900_as_cs,

    -- timestamps
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- primary key
    PRIMARY KEY (person_year_id),

    -- unique identity constraint
    UNIQUE KEY uq_hia_person_year_identity (
        person_id_custom,
        name_kana_norm,
        gender_code,
        exam_year
    ),

    -- lookup indexes
    INDEX idx_hia_person_year_exam_year (exam_year),
    INDEX idx_hia_person_year_insurer (insurer_number),
    INDEX idx_hia_person_year_symbol_match (insurance_symbol_match),
    INDEX idx_hia_person_year_number_match (insurance_number_match),
    INDEX idx_hia_person_year_person_id (person_id_custom),
    INDEX idx_hia_person_year_identity_hash (identity_hash)

) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;