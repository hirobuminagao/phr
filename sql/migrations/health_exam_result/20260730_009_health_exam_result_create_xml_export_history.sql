ALTER TABLE `health_exam_result`.`csv_row_ledger`
  ADD COLUMN `manual_export_approved_at` datetime(3) DEFAULT NULL AFTER `manual_export_reason`,
  ADD COLUMN `manual_export_approved_by` varchar(190) DEFAULT NULL AFTER `manual_export_approved_at`;

ALTER TABLE `health_exam_result`.`exam_result_ledger_report`
  ADD COLUMN `manual_export_approved_at` datetime(3) DEFAULT NULL AFTER `resume_approved_reason`,
  ADD COLUMN `manual_export_approved_by` varchar(190) DEFAULT NULL AFTER `manual_export_approved_at`;

CREATE TABLE `health_exam_result`.`xml_export_zips` (
  `xml_export_zip_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `etl_run_id` bigint unsigned NOT NULL,
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
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`xml_export_zip_id`),
  UNIQUE KEY `uq_xml_export_zips_run_file` (`etl_run_id`, `zip_file_name`),
  KEY `idx_xml_export_zips_event` (`event_id`),
  KEY `idx_xml_export_zips_facility` (`exam_facility_id`),
  KEY `idx_xml_export_zips_receiver` (`insurer_number`),
  KEY `idx_xml_export_zips_file_date` (`file_date`),
  KEY `idx_xml_export_zips_created` (`created_at`),
  CONSTRAINT `fk_xml_export_zips_run`
    FOREIGN KEY (`etl_run_id`) REFERENCES `health_exam_result`.`etl_runs` (`run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs;

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
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`xml_export_member_id`),
  UNIQUE KEY `uq_xml_export_members_zip_ledger` (`xml_export_zip_id`, `ledger_type`, `ledger_id`),
  KEY `idx_xml_export_members_run` (`etl_run_id`),
  KEY `idx_xml_export_members_event` (`event_id`),
  KEY `idx_xml_export_members_source` (`ledger_type`, `ledger_id`),
  KEY `idx_xml_export_members_receipt` (`source_file_receipt_id`),
  KEY `idx_xml_export_members_subscriber` (`subscriber_id`),
  CONSTRAINT `fk_xml_export_members_zip`
    FOREIGN KEY (`xml_export_zip_id`) REFERENCES `health_exam_result`.`xml_export_zips` (`xml_export_zip_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs;
