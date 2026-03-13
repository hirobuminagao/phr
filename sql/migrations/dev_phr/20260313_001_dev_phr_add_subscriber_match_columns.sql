

-- PHR v1.0.1
-- Add identity match columns to dev_phr.subscribers
-- ADR-0008

ALTER TABLE dev_phr.subscribers
    ADD COLUMN name_kana_full_match VARCHAR(190) NULL COMMENT 'normalized full kana name for identity matching',
    ADD COLUMN name_full_match VARCHAR(190) NULL COMMENT 'normalized full kanji name for identity matching',
    ADD COLUMN insurance_symbol_match VARCHAR(64) NULL COMMENT 'normalized insurance symbol for identity matching',
    ADD COLUMN insurance_number_match VARCHAR(64) NULL COMMENT 'normalized insurance number for identity matching';

-- Optional indexes to accelerate identity lookup
CREATE INDEX idx_subscribers_name_kana_full_match
    ON dev_phr.subscribers (name_kana_full_match);

CREATE INDEX idx_subscribers_name_full_match
    ON dev_phr.subscribers (name_full_match);

CREATE INDEX idx_subscribers_symbol_match
    ON dev_phr.subscribers (insurance_symbol_match);

CREATE INDEX idx_subscribers_number_match
    ON dev_phr.subscribers (insurance_number_match);