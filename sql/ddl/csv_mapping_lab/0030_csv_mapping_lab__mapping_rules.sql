CREATE DATABASE IF NOT EXISTS `csv_mapping_lab`
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_ja_0900_as_cs;

CREATE TABLE IF NOT EXISTS `csv_mapping_lab`.`csv_mapping_rules` (
  `rule_id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT 'CSVマッピングルールID',
  `scope` varchar(16) NOT NULL DEFAULT 'global' COMMENT 'global/facility/event',
  `facility_code` varchar(32) DEFAULT NULL COMMENT 'scope=facility の健診機関コード',
  `event_id` bigint unsigned DEFAULT NULL COMMENT 'scope=event のイベントID',
  `condition_type` varchar(32) NOT NULL DEFAULT 'normalized_header_exact' COMMENT 'header_exact/normalized_header_exact/header_contains/sensitive_category',
  `column_no_min` int DEFAULT NULL COMMENT 'この列番以上にだけ適用。NULLなら制限なし',
  `column_no_max` int DEFAULT NULL COMMENT 'この列番以下にだけ適用。NULLなら制限なし',
  `header_pattern` varchar(255) DEFAULT NULL COMMENT 'ヘッダー条件',
  `normalized_header_pattern` varchar(255) DEFAULT NULL COMMENT '正規化ヘッダー条件',
  `value_type` varchar(32) DEFAULT NULL COMMENT 'NUMERIC/DATE/CODE/TEXT/MIXEDなど。NULLなら型不問',
  `sensitive_category` varchar(32) DEFAULT NULL COMMENT '個人系カテゴリ。NULLなら不問',
  `target_kind` varchar(64) NOT NULL COMMENT 'LEDGER_FIELD/EXAM_ITEM_VALUE/IGNORE/REVIEW',
  `target_namecode` char(17) DEFAULT NULL COMMENT 'target_kind=EXAM_ITEM_VALUE のnamecode',
  `target_ledger_field` varchar(64) DEFAULT NULL COMMENT 'target_kind=LEDGER_FIELD のledger field',
  `mapping_strategy` varchar(64) NOT NULL DEFAULT 'DIRECT' COMMENT 'DIRECT/MULTI_COLUMN_JOIN/DERIVED_CODE/METHOD_SELECTION/IGNORE/NEEDS_CONFIRMATION',
  `confidence` decimal(5,4) NOT NULL DEFAULT 0.9000 COMMENT 'ルール信頼度',
  `reason` text DEFAULT NULL COMMENT 'ルール根拠',
  `active` tinyint(1) NOT NULL DEFAULT 1 COMMENT '有効フラグ',
  `created_by` varchar(128) DEFAULT NULL,
  `updated_by` varchar(128) DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`rule_id`),
  KEY `idx_csv_mapping_rules_scope` (`scope`, `facility_code`, `event_id`, `active`),
  KEY `idx_csv_mapping_rules_column_range` (`column_no_min`, `column_no_max`),
  KEY `idx_csv_mapping_rules_header` (`condition_type`, `normalized_header_pattern`),
  KEY `idx_csv_mapping_rules_target_namecode` (`target_namecode`),
  KEY `idx_csv_mapping_rules_target_ledger_field` (`target_ledger_field`),
  CONSTRAINT `chk_csv_mapping_rules_scope` CHECK (`scope` IN ('global', 'facility', 'event')),
  CONSTRAINT `chk_csv_mapping_rules_target` CHECK (`target_kind` IN ('LEDGER_FIELD', 'EXAM_ITEM_VALUE', 'IGNORE', 'REVIEW'))
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE IF NOT EXISTS `csv_mapping_lab`.`csv_mapping_rule_hits` (
  `rule_hit_id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT 'CSVマッピングルールヒットID',
  `analysis_column_id` bigint unsigned NOT NULL COMMENT 'CSV解析列ID',
  `rule_id` bigint unsigned NOT NULL COMMENT 'CSVマッピングルールID',
  `score` decimal(5,4) NOT NULL COMMENT '適用スコア',
  `reason` text DEFAULT NULL COMMENT 'ヒット理由',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`rule_hit_id`),
  UNIQUE KEY `uq_csv_mapping_rule_hits_column_rule` (`analysis_column_id`, `rule_id`),
  KEY `idx_csv_mapping_rule_hits_column` (`analysis_column_id`, `score`),
  KEY `idx_csv_mapping_rule_hits_rule` (`rule_id`),
  CONSTRAINT `fk_csv_mapping_rule_hits_column`
    FOREIGN KEY (`analysis_column_id`) REFERENCES `csv_mapping_lab`.`analysis_columns` (`analysis_column_id`)
    ON DELETE CASCADE,
  CONSTRAINT `fk_csv_mapping_rule_hits_rule`
    FOREIGN KEY (`rule_id`) REFERENCES `csv_mapping_lab`.`csv_mapping_rules` (`rule_id`)
    ON DELETE CASCADE
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
