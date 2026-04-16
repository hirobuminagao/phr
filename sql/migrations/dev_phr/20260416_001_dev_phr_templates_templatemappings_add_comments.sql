START TRANSACTION;

-- templates: 列コメント追加
ALTER TABLE `dev_phr`.`templates`
  MODIFY COLUMN `fund_id` bigint unsigned NOT NULL COMMENT '健保識別ID',
  MODIFY COLUMN `version` int NOT NULL COMMENT 'テンプレートバージョン',
  MODIFY COLUMN `name` varchar(190) DEFAULT NULL COMMENT 'テンプレート名',
  MODIFY COLUMN `template_type` varchar(190) NOT NULL DEFAULT 'fund_to_staging' COMMENT 'テンプレート種別',
  MODIFY COLUMN `target_table` varchar(190) NOT NULL DEFAULT 'staging_subscribers_fund' COMMENT 'マッピング先テーブル名',
  MODIFY COLUMN `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'レコード作成日時',
  MODIFY COLUMN `configured_on` datetime(3) DEFAULT NULL COMMENT '設定適用日',
  MODIFY COLUMN `version_label` varchar(190) DEFAULT NULL COMMENT '表示用バージョンラベル',
  MODIFY COLUMN `created_by` varchar(190) DEFAULT NULL COMMENT '作成者',
  MODIFY COLUMN `notes` text COMMENT '備考';

-- template_mappings: 列コメント追加
ALTER TABLE `dev_phr`.`template_mappings`
  MODIFY COLUMN `fund_id` bigint unsigned NOT NULL COMMENT '健保識別ID',
  MODIFY COLUMN `version` int NOT NULL COMMENT 'テンプレートバージョン',
  MODIFY COLUMN `col_order` int NOT NULL COMMENT 'CSV列順（定義順）',
  MODIFY COLUMN `csv_header` varchar(190) NOT NULL COMMENT 'CSVヘッダ名',
  MODIFY COLUMN `target_column` varchar(190) NOT NULL COMMENT '格納先カラム名（staging_subscribers_fund）',
  MODIFY COLUMN `rule` varchar(190) NOT NULL COMMENT '単一列変換ルール',
  MODIFY COLUMN `required` tinyint(1) NOT NULL DEFAULT 0 COMMENT '必須フラグ（1=必須,0=任意）',
  MODIFY COLUMN `notes` text COMMENT '備考',
  MODIFY COLUMN `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'レコード作成日時';

COMMIT;
