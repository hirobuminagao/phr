CREATE TABLE `health_exam_result`.`hia_download_zips` (
  `download_zip_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `etl_run_id` bigint unsigned DEFAULT NULL,
  `event_id` bigint DEFAULT NULL,
  `insurer_number` varchar(20) NOT NULL,
  `facility_code` varchar(64) DEFAULT NULL,
  `facility_name` varchar(255) DEFAULT NULL,
  `folder_name` varchar(255) NOT NULL,
  `zip_name` varchar(255) NOT NULL,
  `dl_date` date NOT NULL,
  `send_seq` tinyint unsigned NOT NULL DEFAULT 0,
  `zip_sha256` char(64) DEFAULT NULL,
  `source_zip_path` varchar(1024) DEFAULT NULL,
  `archive_zip_path` varchar(1024) DEFAULT NULL,
  `import_status` varchar(32) NOT NULL DEFAULT 'IMPORTED',
  `import_reason` text DEFAULT NULL,
  `xml_count_total` int unsigned NOT NULL DEFAULT 0,
  `xml_count_success` int unsigned NOT NULL DEFAULT 0,
  `xml_count_error` int unsigned NOT NULL DEFAULT 0,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`download_zip_id`),
  KEY `idx_hia_download_zips_zip_sha256` (`zip_sha256`),
  UNIQUE KEY `uq_hia_download_zips_insurer_zip_name` (`insurer_number`, `zip_name`),
  KEY `idx_hia_download_zips_run` (`etl_run_id`),
  KEY `idx_hia_download_zips_event` (`event_id`),
  KEY `idx_hia_download_zips_insurer_date` (`insurer_number`, `dl_date`),
  KEY `idx_hia_download_zips_facility` (`facility_code`),
  KEY `idx_hia_download_zips_status` (`import_status`),
  CONSTRAINT `fk_hia_download_zips_run`
    FOREIGN KEY (`etl_run_id`) REFERENCES `health_exam_result`.`etl_runs` (`run_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE `health_exam_result`.`hia_download_xmls` (
  `hia_download_xml_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `download_zip_id` bigint unsigned NOT NULL,
  `etl_run_id` bigint unsigned DEFAULT NULL,
  `event_id` bigint DEFAULT NULL,
  `xml_filename` varchar(255) NOT NULL,
  `xml_inner_path` varchar(1024) NOT NULL,
  `xml_sha256` char(64) DEFAULT NULL,
  `exam_date` date DEFAULT NULL,
  `exam_year` int DEFAULT NULL,
  `exam_month` char(6) DEFAULT NULL,
  `facility_code` varchar(64) DEFAULT NULL,
  `facility_name` varchar(255) DEFAULT NULL,
  `report_category_code` varchar(64) DEFAULT NULL,
  `program_type_code` varchar(64) DEFAULT NULL,
  `insurer_number` varchar(20) DEFAULT NULL,
  `insurance_symbol_raw` varchar(190) DEFAULT NULL,
  `insurance_number_raw` varchar(190) DEFAULT NULL,
  `insurance_symbol_match` varchar(190) DEFAULT NULL,
  `insurance_number_match` varchar(190) DEFAULT NULL,
  `birthdate` date DEFAULT NULL,
  `name_kana_raw` varchar(190) DEFAULT NULL,
  `name_kana_norm` varchar(190) DEFAULT NULL,
  `gender_code` varchar(16) DEFAULT NULL,
  `person_id_custom` varchar(190) DEFAULT NULL,
  `identity_hash` char(64) DEFAULT NULL,
  `parse_status` varchar(32) NOT NULL DEFAULT 'PARSED',
  `parse_reason` text DEFAULT NULL,
  `is_active_in_zip` tinyint(1) NOT NULL DEFAULT 1,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`hia_download_xml_id`),
  UNIQUE KEY `uq_hia_download_xmls_zip_inner_path` (`download_zip_id`, `xml_inner_path`(700)),
  KEY `idx_hia_download_xmls_sha256` (`xml_sha256`),
  KEY `idx_hia_download_xmls_run` (`etl_run_id`),
  KEY `idx_hia_download_xmls_event` (`event_id`),
  KEY `idx_hia_download_xmls_exam_date` (`exam_date`),
  KEY `idx_hia_download_xmls_exam_month` (`exam_month`),
  KEY `idx_hia_download_xmls_facility` (`facility_code`),
  KEY `idx_hia_download_xmls_insurer` (`insurer_number`),
  KEY `idx_hia_download_xmls_identity_hash` (`identity_hash`),
  KEY `idx_hia_download_xmls_person_id` (`person_id_custom`),
  KEY `idx_hia_download_xmls_parse_status` (`parse_status`),
  CONSTRAINT `fk_hia_download_xmls_zip`
    FOREIGN KEY (`download_zip_id`) REFERENCES `health_exam_result`.`hia_download_zips` (`download_zip_id`),
  CONSTRAINT `fk_hia_download_xmls_run`
    FOREIGN KEY (`etl_run_id`) REFERENCES `health_exam_result`.`etl_runs` (`run_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
