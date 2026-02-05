CREATE TABLE `dev_phr`.`subscribers` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '加入者ID（内部採番）',
  `insurer_number` char(8) NOT NULL COMMENT '保険者番号（0埋め8桁想定）',
  `insurance_symbol` varchar(20) NOT NULL COMMENT '保険証記号',
  `insurance_symbol_digits` int unsigned DEFAULT NULL COMMENT '保険証記号から数字のみ抽出した派生キー（例: 埼-30→30、数字なし→NULL）',
  `insurance_number` varchar(20) NOT NULL COMMENT '保険証番号（先頭ゼロ保持）',
  `insurance_branchnumber` varchar(5) DEFAULT NULL COMMENT '枝番（数値文字列）',
  `birth` date NOT NULL COMMENT 'date型に変更20251210⇐生年月日 YYYYMMDD 文字列',
  `gender_code` tinyint unsigned NOT NULL COMMENT '性別コード 1/2/9',
  `name_kana_full` varchar(190) NOT NULL COMMENT '氏名カナ（フル）',
  `person_id_custom` varchar(64) DEFAULT NULL COMMENT 'ナガオPHRキー（将来NOT NULL予定）',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  `name_kana_middle` varchar(190) DEFAULT NULL,
  `name_kanji_family` varchar(190) DEFAULT NULL,
  `name_kanji_given` varchar(190) DEFAULT NULL,
  `name_kanji_middle` varchar(190) DEFAULT NULL,
  `name_kana_family` varchar(190) DEFAULT NULL,
  `name_kana_given` varchar(190) DEFAULT NULL,
  `insured_attribute_name` varchar(190) DEFAULT NULL,
  `relationship_name` varchar(190) DEFAULT NULL,
  `qualification_acquired_date` date DEFAULT NULL COMMENT 'date型に変更20251210',
  `qualification_lost_date` date DEFAULT NULL COMMENT 'date型に変更20251210',
  `name_kanji_full` varchar(190) DEFAULT NULL,
  `relationship_code_raw` varchar(20) DEFAULT NULL,
  `employer_code` varchar(20) DEFAULT NULL,
  `department_code` varchar(20) DEFAULT NULL,
  `distribution_code` varchar(20) DEFAULT NULL,
  `employee_code` varchar(50) DEFAULT NULL,
  `connect_id` varchar(50) DEFAULT NULL,
  `last_change_run_id` bigint unsigned DEFAULT NULL COMMENT 'etl_runs.run_id',

  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_subscribers_personid_namekana` (`person_id_custom`, `name_kana_full`),
  KEY `idx_subscribers_insurer` (`insurer_number`),
  KEY `idx_subscribers_insurance_full` (`insurer_number`, `insurance_symbol`, `insurance_number`, `insurance_branchnumber`),
  KEY `idx_subscribers_gender` (`gender_code`),
  KEY `idx_subscribers_last_change_run` (`last_change_run_id`),

  CONSTRAINT `chk_subscribers_birth`
    CHECK ((`birth` >= DATE '1900-01-01') AND (`birth` <= DATE '2099-12-31')),
  CONSTRAINT `chk_subscribers_gender_code`
    CHECK ((`gender_code` IN (1, 2, 9))),
  CONSTRAINT `chk_subscribers_insurance_branchnumber`
    CHECK ((`insurance_branchnumber` IS NULL) OR REGEXP_LIKE(`insurance_branchnumber`, '^[0-9]+$')),
  CONSTRAINT `chk_subscribers_insurance_number`
    CHECK (REGEXP_LIKE(`insurance_number`, '^[0-9]+$')),
  CONSTRAINT `chk_subscribers_insurer_number`
    CHECK (REGEXP_LIKE(`insurer_number`, '^[0-9]{8}$'))
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
