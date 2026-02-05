CREATE TABLE `dev_phr`.`norm_rules` (
  `rule_id` bigint NOT NULL AUTO_INCREMENT COMMENT '正規化ルールID',
  `xml_value_type` varchar(16) NOT NULL COMMENT 'XML型（CD/INT/REAL/ST/TS など）',
  `requires_result_code_oid` tinyint NOT NULL DEFAULT 0 COMMENT '1=この型では result_code_oid が必須（基本CDは1）',
  `trim_spaces` tinyint NOT NULL DEFAULT 1 COMMENT '1=前後空白除去',
  `normalize_zenkaku` tinyint NOT NULL DEFAULT 1 COMMENT '1=全角英数→半角（記号も含む）',
  `normalize_plus_tokens` tinyint NOT NULL DEFAULT 0 COMMENT '1=+,＋,プラス等を raw_token_norm に寄せる（主にCD向け）',
  `allow_nullflavor` tinyint NOT NULL DEFAULT 1 COMMENT '1=nullFlavor許容（masterと衝突しない範囲で）',
  `unit_policy` enum('NONE','USE_DISPLAY_UNIT','USE_UCUM_UNIT','CHECK_ONLY')
    NOT NULL DEFAULT 'CHECK_ONLY'
    COMMENT 'NONE=何もしない / USE_* = マスタの単位を採用 / CHECK_ONLY=単位が混ざってたら検知だけ',
  `is_active` tinyint NOT NULL DEFAULT 1 COMMENT '1=有効 / 0=無効',
  `note` text COMMENT '備考（運用メモ）',
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),

  PRIMARY KEY (`rule_id`),
  UNIQUE KEY `uq_xml_value_type` (`xml_value_type`),
  KEY `idx_rules_active` (`is_active`, `xml_value_type`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='健診値 正規化ルール（増えにくい規格側）。型別の前処理・ガード・単位ポリシーを管理。';
