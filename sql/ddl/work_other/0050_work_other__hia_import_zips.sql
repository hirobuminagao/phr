

-- =========================================================
-- Table: hia_import_zips
-- Purpose:
--   ZIP-level ledger for HIA monthly downloads
--   One record = one downloaded ZIP package
-- =========================================================

CREATE TABLE IF NOT EXISTS hia_import_zips (

    -- surrogate key
    zip_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

    -- source identity
    insurer_number CHAR(8) COLLATE ascii_bin NOT NULL,

    -- folder / file identity (based on MHLW transmission spec)
    folder_name VARCHAR(255) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,
    zip_name VARCHAR(255) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,

    -- parsed metadata
    dl_date DATE NOT NULL,
    send_seq INT UNSIGNED NOT NULL,

    -- file metadata
    zip_sha256 CHAR(64) COLLATE ascii_bin,

    -- processing statistics
    xml_count_total INT UNSIGNED NOT NULL DEFAULT 0,
    xml_count_success INT UNSIGNED NOT NULL DEFAULT 0,
    xml_count_error INT UNSIGNED NOT NULL DEFAULT 0,

    -- status
    import_status VARCHAR(20) COLLATE ascii_bin NOT NULL DEFAULT 'IMPORTED',

    -- timestamps
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- primary key
    PRIMARY KEY (zip_id),

    -- duplicate protection
    UNIQUE KEY uq_hia_import_zip (zip_name),

    -- lookup indexes
    INDEX idx_hia_import_zips_insurer (insurer_number),
    INDEX idx_hia_import_zips_dl_date (dl_date)

) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;