CREATE TABLE `dev_phr`.`template_mappings` (
  `fund_id` bigint unsigned NOT NULL COMMENT '健保識別ID',
  `version` int NOT NULL COMMENT 'テンプレートバージョン',
  `col_order` int NOT NULL COMMENT 'CSV列順（定義順）',
  `csv_header` varchar(190) NOT NULL COMMENT 'CSVヘッダ名',
  `target_column` varchar(190) NOT NULL COMMENT '格納先カラム名（staging_subscribers_fund）',
  `rule` varchar(190) NOT NULL COMMENT '単一列変換ルール',
  `required` tinyint(1) NOT NULL DEFAULT 0 COMMENT '必須フラグ（1=必須,0=任意）',
  `notes` text COMMENT '備考',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'レコード作成日時',

  UNIQUE KEY `uq_template_mapping` (`fund_id`, `version`, `col_order`, `target_column`),
  KEY `idx_tmplate_template` (`fund_id`, `version`),

  CONSTRAINT `chk_template_required`
    CHECK ((`required` IN (0, 1)))
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
