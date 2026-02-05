CREATE TABLE `work_other`.`medi_xml_receipts` (
  `xml_receipt_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'PK',

  `zip_sha256` char(64) NOT NULL COMMENT '親ZIPのSHA256(hex)',
  `zip_inner_path` varchar(512) NOT NULL COMMENT 'ZIP内の相対パス',
  `xml_sha256` char(64) NOT NULL COMMENT 'XMLのSHA256(hex) 一意',

  `file_size` bigint DEFAULT NULL COMMENT 'XMLサイズ(byte)',
  `file_mtime` datetime(6) DEFAULT NULL COMMENT 'ZIP内ファイル更新日時(取れれば)',

  `status` enum('PENDING','OK','ERROR') NOT NULL DEFAULT 'PENDING' COMMENT '処理状態',
  `error_code` varchar(64) DEFAULT NULL COMMENT 'エラーコード',
  `error_message` text COMMENT 'エラーメッセージ（短文化推奨）',

  `document_id` varchar(190) DEFAULT NULL COMMENT 'CDA Document ID（root+extension等の連結推奨）',
  `doc_type` varchar(64) DEFAULT NULL COMMENT '種別（例: kenshin/tokuhoなど）',

  `insurer_number` varchar(8) DEFAULT NULL COMMENT '保険者番号',
  `person_key` varchar(64) DEFAULT NULL COMMENT 'person_key',
  `patient_name_kana` varchar(190) DEFAULT NULL COMMENT '氏名カナ（正規化済み推奨）',
  `birthdate` date DEFAULT NULL COMMENT '生年月日',
  `exam_date` date DEFAULT NULL COMMENT '健診日/実施日（取れる範囲で）',

  `facility_code` varchar(64) DEFAULT NULL COMMENT '健診機関コード等',
  `facility_name` varchar(255) DEFAULT NULL COMMENT '施設名',

  `extracted_json` json DEFAULT NULL COMMENT '抽出したメタ情報（拡張用）',
  `extracted_at` datetime(6) DEFAULT NULL COMMENT '索引抽出完了日時（CDA_INDEXの成功時）',
  `extracted_run_id` bigint DEFAULT NULL COMMENT '索引抽出したrun_id',

  `first_seen_run_id` bigint DEFAULT NULL COMMENT '初回検出run_id（固定）',
  `first_seen_at` datetime(6) DEFAULT NULL COMMENT '初回検出日時',
  `last_seen_run_id` bigint DEFAULT NULL COMMENT '最終検出run_id（更新）',
  `last_seen_at` datetime(6) DEFAULT NULL COMMENT '最終検出日時',

  `admin_note` text COMMENT '管理者メモ',

  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  `updated_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',

  `items_extract_status` varchar(16) DEFAULT 'PENDING',
  `items_extracted_run_id` bigint DEFAULT NULL COMMENT 'item values抽出run_id',
  `items_extracted_at` datetime(6) DEFAULT NULL COMMENT 'item values抽出日時',

  PRIMARY KEY (`xml_receipt_id`),

  UNIQUE KEY `uq_medi_xml_receipts_zip_path` (`zip_sha256`, `zip_inner_path`),

  KEY `idx_medi_xml_receipts_zip_sha256` (`zip_sha256`),
  KEY `idx_medi_xml_receipts_status` (`status`),
  KEY `idx_medi_xml_receipts_docid` (`document_id`),
  KEY `idx_medi_xml_receipts_person_exam` (`person_key`, `exam_date`),
  KEY `idx_medi_xml_receipts_first_seen_run` (`first_seen_run_id`),
  KEY `idx_medi_xml_receipts_last_seen_run` (`last_seen_run_id`),
  KEY `idx_medi_xml_receipts_extracted_at` (`extracted_at`),
  KEY `idx_medi_xml_receipts_xml_sha256` (`xml_sha256`),
  KEY `idx_medi_xml_receipts_items_extract_status` (`items_extract_status`),
  KEY `idx_medi_xml_receipts_items_extracted_run_id` (`items_extracted_run_id`),
  KEY `idx_items_extract_pick` (`items_extract_status`, `updated_at`)
)
ENGINE=InnoDB
AUTO_INCREMENT=362174
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='【medi】XML受領台帳（ZIP内個票の記帳）';
