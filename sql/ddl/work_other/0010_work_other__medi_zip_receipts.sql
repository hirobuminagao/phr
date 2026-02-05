CREATE TABLE `work_other`.`medi_zip_receipts` (
  `zip_receipt_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'ZIP受領ログID',
  `run_id` bigint NOT NULL COMMENT '実行ID',
  `first_seen_run_id` bigint DEFAULT NULL COMMENT '初回検出run_id（最初に見つけた実行）',
  `first_seen_at` datetime(6) DEFAULT NULL COMMENT '初回検出日時',
  `last_seen_run_id` bigint DEFAULT NULL COMMENT '最終検出run_id（最後に見つけた実行）',
  `last_seen_at` datetime(6) DEFAULT NULL COMMENT '最終検出日時',

  `facility_folder_name` varchar(255) DEFAULT NULL COMMENT '施設フォルダ名（健診機関コード_健診機関名）',
  `facility_code` varchar(64) DEFAULT NULL COMMENT '健診機関コード（フォルダ由来）',
  `facility_name` varchar(255) DEFAULT NULL COMMENT '健診機関名（フォルダ由来）',

  `zip_name` varchar(255) NOT NULL COMMENT 'ZIPファイル名',
  `zip_path` varchar(1024) DEFAULT NULL COMMENT 'ZIPパス（監査用）',
  `zip_sha256` char(64) NOT NULL COMMENT 'ZIPのSHA256（hex）',

  `structure_status` enum('OK','ERROR') NOT NULL COMMENT 'ZIP構造判定（DATA基準）',
  `error_code` varchar(64) DEFAULT NULL COMMENT 'ZIPエラー種別（例: ZIP_PASSWORD / ZIP_LONG_PATH / STRUCT_NO_DATA_DIR / STRUCT_MULTI_DATA_DIR / STRUCT_ZERO_XML / ZIP_EXTRACT / ZIP_UNEXPECTED）',
  `error_message` text COMMENT 'ZIP展開エラー詳細（例外メッセージ等）',
  `structure_message` text COMMENT '構造判定詳細（DATA複数/空など）',
  `data_dir_count` int DEFAULT NULL COMMENT '検出したDATAフォルダ数',
  `data_xml_count` int DEFAULT NULL COMMENT 'DATA配下XML件数',
  `admin_note` text COMMENT '管理者メモ（手動：削除/退避など）',

  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  `updated_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時（行が更新された日時）',

  PRIMARY KEY (`zip_receipt_id`),
  UNIQUE KEY `uq_medi_zip_receipts_zip_sha256` (`zip_sha256`),

  KEY `idx_medi_zip_receipts_run` (`run_id`),
  KEY `idx_medi_zip_receipts_zip_sha256` (`zip_sha256`),
  KEY `idx_medi_zip_receipts_facility_code` (`facility_code`),
  KEY `idx_medi_zip_receipts_status` (`structure_status`),
  KEY `idx_medi_zip_receipts_first_seen_run` (`first_seen_run_id`),
  KEY `idx_medi_zip_receipts_last_seen_run` (`last_seen_run_id`),
  KEY `idx_medi_zip_receipts_error_code` (`error_code`)
)
ENGINE=InnoDB
AUTO_INCREMENT=12458
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='【medi】ZIP受領ログ（構造チェック結果）';
