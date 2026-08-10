CREATE TABLE `health_exam_result`.`xml_export_zips` (
  `xml_export_zip_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `etl_run_id` bigint unsigned NOT NULL,
  `xml_export_list_id` bigint unsigned DEFAULT NULL COMMENT 'このZIPを作成した出力リスト。CLI直接出力時はNULL',
  `event_id` bigint NOT NULL,
  `exam_facility_id` bigint unsigned NOT NULL,
  `facility_code` varchar(64) NOT NULL,
  `facility_name` varchar(255) NOT NULL,
  `facility_folder_name` varchar(255) NOT NULL,
  `insurer_number` varchar(20) NOT NULL,
  `file_date` date NOT NULL,
  `split_no` tinyint unsigned NOT NULL,
  `implementation_code` varchar(8) NOT NULL DEFAULT '1',
  `root_dir_name` varchar(255) NOT NULL,
  `zip_file_name` varchar(255) NOT NULL,
  `zip_path` varchar(1024) NOT NULL,
  `zip_sha256` char(64) NOT NULL,
  `member_count` int unsigned NOT NULL,
  `xsd_bundle_id` varchar(64) NOT NULL,
  `hia_upload_status` varchar(32) NOT NULL DEFAULT 'PENDING' COMMENT 'HIAアップロード作業状態。PENDING/UPLOADED/UPLOAD_ERROR/PARTIAL/CONFIRMED等',
  `hia_uploaded_at` datetime(3) DEFAULT NULL COMMENT 'HIAアップロード実施日時',
  `hia_uploaded_by` varchar(190) DEFAULT NULL COMMENT 'HIAアップロード実施者',
  `hia_upload_checked_at` datetime(3) DEFAULT NULL COMMENT 'HIAアップロード結果確認日時',
  `hia_upload_checked_by` varchar(190) DEFAULT NULL COMMENT 'HIAアップロード結果確認者',
  `hia_upload_error_summary` text DEFAULT NULL COMMENT 'ZIP単位のHIAアップロードエラー要約',
  `hia_upload_note` text DEFAULT NULL COMMENT 'HIAアップロード作業メモ',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`xml_export_zip_id`),
  UNIQUE KEY `uq_xml_export_zips_run_file` (`etl_run_id`, `zip_file_name`),
  KEY `idx_xml_export_zips_event` (`event_id`),
  KEY `idx_xml_export_zips_facility` (`exam_facility_id`),
  KEY `idx_xml_export_zips_receiver` (`insurer_number`),
  KEY `idx_xml_export_zips_file_date` (`file_date`),
  KEY `idx_xml_export_zips_hia_upload_status` (`hia_upload_status`),
  KEY `idx_xml_export_zips_export_list` (`xml_export_list_id`),
  KEY `idx_xml_export_zips_created` (`created_at`),
  CONSTRAINT `fk_xml_export_zips_run`
    FOREIGN KEY (`etl_run_id`) REFERENCES `health_exam_result`.`etl_runs` (`run_id`),
  CONSTRAINT `fk_xml_export_zips_export_list`
    FOREIGN KEY (`xml_export_list_id`) REFERENCES `health_exam_result`.`ops_xml_export_lists` (`xml_export_list_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
