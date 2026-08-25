CREATE DATABASE IF NOT EXISTS `csv_mapping_lab`
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_ja_0900_as_cs;

CREATE TABLE `csv_mapping_lab`.`analysis_files` (
  `analysis_file_id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT 'CSV解析ファイルID',
  `source_file_name` varchar(255) NOT NULL COMMENT '元CSVファイル名',
  `source_file_path` text DEFAULT NULL COMMENT '元CSV配置パス',
  `source_file_size_bytes` bigint unsigned DEFAULT NULL COMMENT '元CSVファイルサイズ',
  `source_file_sha256` char(64) DEFAULT NULL COMMENT '元CSVファイル内容のSHA256',
  `source_folder_name` varchar(255) DEFAULT NULL COMMENT '受領フォルダ名などの由来',
  `facility_code` varchar(64) DEFAULT NULL COMMENT '健診機関コード。支払基金等の公開コードまたはファイル名由来',
  `facility_name` varchar(255) DEFAULT NULL COMMENT '健診機関名',
  `payment_fund_code` varchar(64) DEFAULT NULL COMMENT '支払基金公開データ等で確認したコード',
  `payment_fund_name` varchar(255) DEFAULT NULL COMMENT '支払基金公開データ等で確認した名称',
  `encoding` varchar(32) DEFAULT NULL COMMENT '文字コード',
  `delimiter` varchar(8) NOT NULL DEFAULT ',' COMMENT '区切り文字',
  `quote_char` varchar(8) DEFAULT '"' COMMENT 'quote文字',
  `header_row_no` int NOT NULL DEFAULT 1 COMMENT 'ヘッダー行番号',
  `data_start_row_no` int NOT NULL DEFAULT 2 COMMENT 'データ開始行番号',
  `row_count` int NOT NULL DEFAULT 0 COMMENT 'データ行数',
  `column_count` int NOT NULL DEFAULT 0 COMMENT '列数',
  `header_sha256` char(64) DEFAULT NULL COMMENT 'ヘッダー構造のSHA256',
  `header_snapshot_json` json DEFAULT NULL COMMENT 'ヘッダー行と正規化列のスナップショット',
  `sample_row_count` int NOT NULL DEFAULT 0 COMMENT 'サンプル値抽出に使った最大行数',
  `parse_status` varchar(32) NOT NULL DEFAULT 'PENDING' COMMENT 'PENDING/OK/WARNING/ERROR',
  `parse_error_message` text DEFAULT NULL COMMENT 'CSV読込エラーまたは警告',
  `analysis_status` varchar(32) NOT NULL DEFAULT 'NEW' COMMENT 'NEW/ANALYZED/REVIEWING/READY_FOR_SEED/SEED_CREATED/ARCHIVED',
  `memo` text DEFAULT NULL COMMENT 'ファイル全体の解析メモ',
  `created_by` varchar(190) DEFAULT NULL COMMENT '作成者',
  `updated_by` varchar(190) DEFAULT NULL COMMENT '更新者',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`analysis_file_id`),
  KEY `idx_analysis_files_facility_code` (`facility_code`),
  KEY `idx_analysis_files_payment_fund_code` (`payment_fund_code`),
  KEY `idx_analysis_files_source_sha256` (`source_file_sha256`),
  KEY `idx_analysis_files_header_sha256` (`header_sha256`),
  KEY `idx_analysis_files_parse_status` (`parse_status`),
  KEY `idx_analysis_files_status` (`analysis_status`),
  KEY `idx_analysis_files_created_at` (`created_at`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
