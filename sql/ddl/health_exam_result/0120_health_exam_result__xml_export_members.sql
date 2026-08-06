CREATE TABLE `health_exam_result`.`xml_export_members` (
  `xml_export_member_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `xml_export_zip_id` bigint unsigned NOT NULL,
  `etl_run_id` bigint unsigned NOT NULL,
  `event_id` bigint NOT NULL,
  `ledger_type` varchar(16) NOT NULL DEFAULT 'CSV',
  `ledger_id` bigint unsigned NOT NULL,
  `source_file_receipt_id` bigint unsigned DEFAULT NULL,
  `subscriber_id` bigint unsigned DEFAULT NULL,
  `hia_subscriber_id` varchar(190) DEFAULT NULL,
  `person_xml_file_name` varchar(255) NOT NULL,
  `person_xml_sha256` char(64) NOT NULL,
  `report_category_code` varchar(64) NOT NULL,
  `program_type_code` varchar(64) NOT NULL,
  `manual_export_approved` tinyint(1) NOT NULL DEFAULT 0,
  `manual_export_reason` text,
  `manual_export_approved_at` datetime(3) DEFAULT NULL,
  `manual_export_approved_by` varchar(190) DEFAULT NULL,
  `hia_upload_status` varchar(32) NOT NULL DEFAULT 'PENDING' COMMENT '個人XML単位のHIAアップロード結果。PENDING/UPLOADED/UPLOAD_ERROR/EXCLUDED等',
  `hia_upload_error_code` varchar(64) DEFAULT NULL COMMENT 'HIAアップロード時の個人単位エラーコードまたは分類',
  `hia_upload_error_message` text DEFAULT NULL COMMENT 'HIAアップロード時の個人単位エラー内容',
  `hia_upload_note` text DEFAULT NULL COMMENT 'HIAアップロード個人単位メモ',
  `hia_uploaded_at` datetime(3) DEFAULT NULL COMMENT '個人XMLのHIAアップロード完了確認日時',
  `hia_uploaded_by` varchar(190) DEFAULT NULL COMMENT '個人XMLのHIAアップロード完了確認者',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`xml_export_member_id`),
  UNIQUE KEY `uq_xml_export_members_zip_ledger` (`xml_export_zip_id`, `ledger_type`, `ledger_id`),
  KEY `idx_xml_export_members_run` (`etl_run_id`),
  KEY `idx_xml_export_members_event` (`event_id`),
  KEY `idx_xml_export_members_source` (`ledger_type`, `ledger_id`),
  KEY `idx_xml_export_members_receipt` (`source_file_receipt_id`),
  KEY `idx_xml_export_members_subscriber` (`subscriber_id`),
  KEY `idx_xml_export_members_hia_upload_status` (`hia_upload_status`),
  CONSTRAINT `fk_xml_export_members_zip`
    FOREIGN KEY (`xml_export_zip_id`) REFERENCES `health_exam_result`.`xml_export_zips` (`xml_export_zip_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
