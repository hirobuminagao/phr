CREATE TABLE `phr_master`.`medical_folder_aliases` (
  `alias_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `event_id` bigint NOT NULL,
  `src_folder_raw` varchar(255) NOT NULL,
  `dst_folder_norm` varchar(255) NOT NULL,
  `exam_facility_id` bigint unsigned DEFAULT NULL,
  `expected_source_mode` varchar(32) NOT NULL DEFAULT 'UNKNOWN' COMMENT '想定受領モード。UNKNOWN/XML_ONLY/CSV_ONLY/XML_CSV_MERGE',
  `csv_format_version_id` bigint unsigned DEFAULT NULL COMMENT 'このaliasで使用するCSVテンプレート',
  `manual_judgement` tinyint(1) NOT NULL DEFAULT 0,
  `note` text,
  `is_active` tinyint(1) NOT NULL DEFAULT 1,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`alias_id`),
  UNIQUE KEY `uq_medical_folder_aliases_event_src` (`event_id`, `src_folder_raw`),
  KEY `idx_medical_folder_aliases_event` (`event_id`),
  KEY `idx_medical_folder_aliases_exam_facility` (`exam_facility_id`),
  KEY `idx_medical_folder_aliases_source_mode` (`expected_source_mode`),
  KEY `idx_medical_folder_aliases_csv_format` (`csv_format_version_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
