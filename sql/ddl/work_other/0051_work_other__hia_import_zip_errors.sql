

-- =========================================================
-- Table: hia_import_zip_errors
-- Purpose:
--   Error ledger for ZIP processing in HIA_fund_ledger_xml pipeline
--   One record = one error detected while processing a ZIP/XML
-- =========================================================

CREATE TABLE IF NOT EXISTS hia_import_zip_errors (

    -- surrogate key
    zip_error_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

    -- relation to ZIP ledger
    zip_id BIGINT UNSIGNED NOT NULL,

    -- XML context (nullable if error occurs before XML parsing)
    xml_filename VARCHAR(255) COLLATE utf8mb4_ja_0900_as_cs,

    -- error classification
    error_code VARCHAR(50) COLLATE ascii_bin NOT NULL,
    error_message VARCHAR(500) COLLATE utf8mb4_ja_0900_as_cs,

    -- optional raw context
    error_detail TEXT COLLATE utf8mb4_ja_0900_as_cs,

    -- timestamps
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- primary key
    PRIMARY KEY (zip_error_id),

    -- indexes
    INDEX idx_hia_zip_errors_zip (zip_id),
    INDEX idx_hia_zip_errors_code (error_code),
    INDEX idx_hia_zip_errors_xml (xml_filename)

) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;