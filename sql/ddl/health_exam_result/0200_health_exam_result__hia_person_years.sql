CREATE TABLE `health_exam_result`.`hia_person_years` (
  `person_year_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `event_id` bigint DEFAULT NULL,
  `person_id_custom` varchar(190) NOT NULL,
  `identity_hash` char(64) DEFAULT NULL,
  `name_kana_raw` varchar(190) DEFAULT NULL,
  `name_kana_norm` varchar(190) NOT NULL,
  `gender_code` varchar(16) NOT NULL,
  `exam_year` int NOT NULL,
  `insurer_number` varchar(20) NOT NULL,
  `insurance_symbol_raw` varchar(190) DEFAULT NULL,
  `insurance_number_raw` varchar(190) DEFAULT NULL,
  `insurance_symbol_match` varchar(190) NOT NULL,
  `insurance_number_match` varchar(190) NOT NULL,
  `birthdate` date NOT NULL,
  `report_category_code` varchar(64) DEFAULT NULL,
  `program_type_code` varchar(64) DEFAULT NULL,
  `dl_count` int unsigned NOT NULL DEFAULT 0,
  `first_seen_dl_date` date DEFAULT NULL,
  `first_seen_download_zip_id` bigint unsigned DEFAULT NULL,
  `first_seen_hia_download_xml_id` bigint unsigned DEFAULT NULL,
  `last_seen_dl_date` date DEFAULT NULL,
  `last_seen_download_zip_id` bigint unsigned DEFAULT NULL,
  `last_seen_hia_download_xml_id` bigint unsigned DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`person_year_id`),
  UNIQUE KEY `uq_hia_person_years_identity` (`person_id_custom`, `name_kana_norm`, `gender_code`, `exam_year`),
  KEY `idx_hia_person_years_event` (`event_id`),
  KEY `idx_hia_person_years_exam_year` (`exam_year`),
  KEY `idx_hia_person_years_insurer` (`insurer_number`),
  KEY `idx_hia_person_years_identity_hash` (`identity_hash`),
  KEY `idx_hia_person_years_symbol_number` (`insurance_symbol_match`, `insurance_number_match`),
  KEY `idx_hia_person_years_first_zip` (`first_seen_download_zip_id`),
  KEY `idx_hia_person_years_last_zip` (`last_seen_download_zip_id`),
  CONSTRAINT `fk_hia_person_years_first_zip`
    FOREIGN KEY (`first_seen_download_zip_id`) REFERENCES `health_exam_result`.`hia_download_zips` (`download_zip_id`),
  CONSTRAINT `fk_hia_person_years_first_xml`
    FOREIGN KEY (`first_seen_hia_download_xml_id`) REFERENCES `health_exam_result`.`hia_download_xmls` (`hia_download_xml_id`),
  CONSTRAINT `fk_hia_person_years_last_zip`
    FOREIGN KEY (`last_seen_download_zip_id`) REFERENCES `health_exam_result`.`hia_download_zips` (`download_zip_id`),
  CONSTRAINT `fk_hia_person_years_last_xml`
    FOREIGN KEY (`last_seen_hia_download_xml_id`) REFERENCES `health_exam_result`.`hia_download_xmls` (`hia_download_xml_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE `health_exam_result`.`hia_person_xml_events` (
  `person_xml_event_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `person_year_id` bigint unsigned NOT NULL,
  `hia_download_xml_id` bigint unsigned NOT NULL,
  `download_zip_id` bigint unsigned NOT NULL,
  `event_type` varchar(32) NOT NULL DEFAULT 'LINKED',
  `event_status` varchar(32) NOT NULL DEFAULT 'ACTIVE',
  `is_current` tinyint(1) NOT NULL DEFAULT 1,
  `link_reason` text DEFAULT NULL,
  `superseded_by_event_id` bigint unsigned DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`person_xml_event_id`),
  UNIQUE KEY `uq_hia_person_xml_events_person_xml` (`person_year_id`, `hia_download_xml_id`),
  KEY `idx_hia_person_xml_events_xml` (`hia_download_xml_id`),
  KEY `idx_hia_person_xml_events_zip` (`download_zip_id`),
  KEY `idx_hia_person_xml_events_current` (`person_year_id`, `is_current`),
  KEY `idx_hia_person_xml_events_type_status` (`event_type`, `event_status`),
  KEY `idx_hia_person_xml_events_superseded_by` (`superseded_by_event_id`),
  CONSTRAINT `fk_hia_person_xml_events_person_year`
    FOREIGN KEY (`person_year_id`) REFERENCES `health_exam_result`.`hia_person_years` (`person_year_id`),
  CONSTRAINT `fk_hia_person_xml_events_xml`
    FOREIGN KEY (`hia_download_xml_id`) REFERENCES `health_exam_result`.`hia_download_xmls` (`hia_download_xml_id`),
  CONSTRAINT `fk_hia_person_xml_events_zip`
    FOREIGN KEY (`download_zip_id`) REFERENCES `health_exam_result`.`hia_download_zips` (`download_zip_id`),
  CONSTRAINT `fk_hia_person_xml_events_superseded_by`
    FOREIGN KEY (`superseded_by_event_id`) REFERENCES `health_exam_result`.`hia_person_xml_events` (`person_xml_event_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
