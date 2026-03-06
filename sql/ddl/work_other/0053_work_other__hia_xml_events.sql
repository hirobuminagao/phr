

-- =========================================================
-- Table: hia_xml_events
-- Purpose:
--   Physical XML event ledger for HIA_fund_ledger_xml pipeline
--   One record = one XML file processed from HIA download
-- =========================================================

CREATE TABLE IF NOT EXISTS hia_xml_events (

    -- surrogate key
    xml_event_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

    -- relations
    person_year_id BIGINT UNSIGNED NOT NULL,
    zip_id BIGINT UNSIGNED NOT NULL,

    -- file identity
    xml_filename VARCHAR(255) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,
    xml_sha256 CHAR(64) COLLATE ascii_bin,

    -- exam information
    exam_date DATE,

    -- facility
    facility_code VARCHAR(32) COLLATE ascii_bin,
    facility_name VARCHAR(255) COLLATE utf8mb4_ja_0900_as_cs,

    -- download metadata
    dl_date DATE NOT NULL,

    -- timestamps
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- primary key
    PRIMARY KEY (xml_event_id),

    -- indexes
    INDEX idx_hia_xml_events_person_year (person_year_id),
    INDEX idx_hia_xml_events_zip (zip_id),
    INDEX idx_hia_xml_events_exam_date (exam_date),
    INDEX idx_hia_xml_events_facility_code (facility_code),

    -- duplicate protection
    UNIQUE KEY uq_hia_xml_unique (
        zip_id,
        xml_filename
    )

) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;