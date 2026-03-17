-- =============================================================
-- Table: identity_kanji_normalization
-- Purpose: Dictionary for normalizing kanji variants (CJK compatibility,
--          old characters, and common alternate forms) into canonical
--          characters used for identity matching.
-- Related ADR: ADR-0012 Identity Canonicalization and Join Hash Policy
-- =============================================================

CREATE TABLE IF NOT EXISTS dev_phr.identity_kanji_normalization (

    normalization_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

    -- original variant character found in source systems
    original_char VARCHAR(10)
        CHARACTER SET utf8mb4
        COLLATE utf8mb4_ja_0900_as_cs
        NOT NULL,

    -- canonical character used for match normalization
    normalized_char VARCHAR(10)
        CHARACTER SET utf8mb4
        COLLATE utf8mb4_ja_0900_as_cs
        NOT NULL,

    -- optional description (example: "CJK compatibility ideograph")
    description VARCHAR(255)
        CHARACTER SET utf8mb4
        COLLATE utf8mb4_ja_0900_as_cs,

    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (normalization_id),

    UNIQUE KEY uq_identity_kanji_original (original_char),

    INDEX idx_identity_kanji_normalized (normalized_char)

) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;


-- -------------------------------------------------------------
-- Initial seed values (common compatibility / variant characters)
-- -------------------------------------------------------------

INSERT INTO dev_phr.identity_kanji_normalization
(original_char, normalized_char, description)
VALUES

('羽','羽','CJK compatibility ideograph'),
('神','神','CJK compatibility ideograph'),
('塚','塚','CJK compatibility ideograph'),
('礼','礼','CJK compatibility ideograph'),
('猪','猪','CJK compatibility ideograph'),
('﨑','崎','CJK compatibility ideograph'),
('瀨','瀬','Old/variant form'),
('髙','高','Variant form')

ON DUPLICATE KEY UPDATE
normalized_char = VALUES(normalized_char);
