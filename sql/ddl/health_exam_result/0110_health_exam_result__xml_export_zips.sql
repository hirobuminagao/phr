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
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
