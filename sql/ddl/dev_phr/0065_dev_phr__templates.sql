CREATE TABLE `dev_phr`.`templates` (
  `fund_id` bigint unsigned NOT NULL COMMENT '健保識別ID',
  `version` int NOT NULL COMMENT 'テンプレートバージョン',
  `name` varchar(190) DEFAULT NULL COMMENT 'テンプレート名',
  `template_type` varchar(190) NOT NULL DEFAULT 'fund_to_staging' COMMENT 'テンプレート種別',
  `target_table` varchar(190) NOT NULL DEFAULT 'staging_subscribers_fund' COMMENT 'マッピング先テーブル名',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'レコード作成日時',
  `configured_on` datetime(3) DEFAULT NULL COMMENT '設定適用日',
  `version_label` varchar(190) DEFAULT NULL COMMENT '表示用バージョンラベル',
  `created_by` varchar(190) DEFAULT NULL COMMENT '作成者',
  `notes` text COMMENT '備考',

  PRIMARY KEY (`fund_id`, `version`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
