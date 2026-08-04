-- HIAダッシュボードCSV新フォーマット対応
-- 追加列:
--   dashboard_name_kana       : CSV「氏名カナ」原文
--   dashboard_name_kana_match : CSV「氏名カナ」照合用

SET @col_exists := (
  SELECT COUNT(*)
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = 'work_other'
    AND TABLE_NAME = 'hia_dashboard_status'
    AND COLUMN_NAME = 'dashboard_name_kana'
);

SET @sql := IF(
  @col_exists > 0,
  'SELECT ''dashboard_name_kana already exists''',
  'ALTER TABLE `work_other`.`hia_dashboard_status`
     ADD COLUMN `dashboard_name_kana` varchar(190) DEFAULT NULL COMMENT ''HIAダッシュボードCSV由来の氏名カナ原文'' AFTER `name_match`,
     ADD COLUMN `dashboard_name_kana_match` varchar(190) DEFAULT NULL COMMENT ''HIAダッシュボードCSV由来の氏名カナ照合用'' AFTER `dashboard_name_kana`'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @col_exists := (
  SELECT COUNT(*)
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = 'work_other'
    AND TABLE_NAME = 'hia_dashboard_status'
    AND COLUMN_NAME = 'is_active'
);

SET @sql := IF(
  @col_exists > 0,
  'SELECT ''is_active already exists''',
  'ALTER TABLE `work_other`.`hia_dashboard_status`
     ADD COLUMN `is_active` tinyint(1) NOT NULL DEFAULT 1 COMMENT ''最新全件取込で有効な行か'' AFTER `row_sha256`,
     ADD COLUMN `inactive_run_id` bigint unsigned DEFAULT NULL COMMENT ''非アクティブ化したrun_id'' AFTER `is_active`,
     ADD COLUMN `inactive_at` datetime(3) DEFAULT NULL COMMENT ''非アクティブ化日時'' AFTER `inactive_run_id`,
     ADD COLUMN `inactive_reason` varchar(190) DEFAULT NULL COMMENT ''非アクティブ化理由'' AFTER `inactive_at`'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @idx_exists := (
  SELECT COUNT(*)
  FROM INFORMATION_SCHEMA.STATISTICS
  WHERE TABLE_SCHEMA = 'work_other'
    AND TABLE_NAME = 'hia_dashboard_status'
    AND INDEX_NAME = 'idx_hia_dashboard_active'
);

SET @sql := IF(
  @idx_exists > 0,
  'SELECT ''idx_hia_dashboard_active already exists''',
  'CREATE INDEX `idx_hia_dashboard_active`
     ON `work_other`.`hia_dashboard_status` (`insurer_number`, `is_active`)'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @idx_exists := (
  SELECT COUNT(*)
  FROM INFORMATION_SCHEMA.STATISTICS
  WHERE TABLE_SCHEMA = 'work_other'
    AND TABLE_NAME = 'hia_dashboard_status'
    AND INDEX_NAME = 'idx_hia_dashboard_name_kana_match'
);

SET @sql := IF(
  @idx_exists > 0,
  'SELECT ''idx_hia_dashboard_name_kana_match already exists''',
  'CREATE INDEX `idx_hia_dashboard_name_kana_match`
     ON `work_other`.`hia_dashboard_status` (`dashboard_name_kana_match`)'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
