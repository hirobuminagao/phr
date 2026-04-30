

-- =============================================================
-- Migration: add address/postal match columns to staging_subscribers_fund
-- =============================================================

ALTER TABLE dev_phr.staging_subscribers_fund
  ADD COLUMN postal_code_match varchar(7)
    COMMENT '郵便番号（照合用: 数字のみ7桁0埋め）'
    AFTER postal_code_norm,

  ADD COLUMN address_match varchar(255)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs
    COMMENT '住所照合用（住所1 + 全角スペース + 建物。建物なしは住所1のみ）'
    AFTER building_norm;