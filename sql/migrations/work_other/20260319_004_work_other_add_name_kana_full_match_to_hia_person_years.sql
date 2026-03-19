

-- =========================================================
-- Migration: add name_kana_full_match to hia_person_years
-- Version: v1.0.2 identity canonicalization alignment
-- =========================================================

ALTER TABLE work_other.hia_person_years
ADD COLUMN name_kana_full_match VARCHAR(190)
    COLLATE utf8mb4_ja_0900_as_cs
    DEFAULT NULL
    COMMENT 'canonical kana match value aligned with subscribers for identity_hash generation';

-- index (optional but recommended for join performance)
CREATE INDEX idx_hia_person_year_name_kana_full_match
ON work_other.hia_person_years (name_kana_full_match);