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
CREATE TABLE `health_exam_result`.`fund_delivery_lists` (
  `delivery_list_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `event_id` bigint DEFAULT NULL,
  `insurer_number` varchar(20) NOT NULL,
  `list_name` varchar(255) NOT NULL,
  `list_status` varchar(32) NOT NULL DEFAULT 'DRAFT',
  `output_mode` varchar(32) NOT NULL DEFAULT 'EXAM_MONTH',
  `exam_month` char(6) DEFAULT NULL,
  `delivery_policy` varchar(32) NOT NULL DEFAULT 'NOT_DELIVERED_ONLY',
  `same_exam_date_policy` varchar(32) NOT NULL DEFAULT 'LATEST_DOWNLOAD',
  `include_delivery_status` varchar(255) DEFAULT NULL,
  `search_condition_note` text DEFAULT NULL,
  `submitted_at` datetime(3) DEFAULT NULL,
  `submitted_by` varchar(190) DEFAULT NULL,
  `submission_note` text DEFAULT NULL,
  `created_by` varchar(190) DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`delivery_list_id`),
  KEY `idx_fund_delivery_lists_event` (`event_id`),
  KEY `idx_fund_delivery_lists_insurer` (`insurer_number`),
  KEY `idx_fund_delivery_lists_status` (`list_status`),
  KEY `idx_fund_delivery_lists_output_mode` (`output_mode`, `exam_month`),
  KEY `idx_fund_delivery_lists_submitted` (`submitted_at`),
  KEY `idx_fund_delivery_lists_created` (`created_at`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE `health_exam_result`.`fund_delivery_runs` (
  `delivery_run_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `etl_run_id` bigint unsigned DEFAULT NULL,
  `delivery_list_id` bigint unsigned DEFAULT NULL,
  `event_id` bigint DEFAULT NULL,
  `insurer_number` varchar(20) NOT NULL,
  `output_mode` varchar(32) NOT NULL DEFAULT 'EXAM_MONTH',
  `exam_month` char(6) DEFAULT NULL,
  `delivery_policy` varchar(32) NOT NULL DEFAULT 'NOT_DELIVERED_ONLY',
  `same_exam_date_policy` varchar(32) NOT NULL DEFAULT 'LATEST_DOWNLOAD',
  `include_delivery_status` varchar(255) DEFAULT NULL,
  `source_dl_date` date DEFAULT NULL,
  `source_download_zip_id` bigint unsigned DEFAULT NULL,
  `source_zip_name` varchar(255) DEFAULT NULL,
  `output_zip_name` varchar(255) NOT NULL,
  `output_zip_path` varchar(1024) NOT NULL,
  `output_zip_sha256` char(64) DEFAULT NULL,
  `delivery_status` varchar(32) NOT NULL DEFAULT 'CREATED',
  `delivery_xml_count` int unsigned NOT NULL DEFAULT 0,
  `delivery_person_count` int unsigned NOT NULL DEFAULT 0,
  `excluded_prior_count` int unsigned NOT NULL DEFAULT 0,
  `excluded_rule_count` int unsigned NOT NULL DEFAULT 0,
  `deduped_xml_count` int unsigned NOT NULL DEFAULT 0,
  `created_by` varchar(190) DEFAULT NULL,
  `note` text DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`delivery_run_id`),
  KEY `idx_fund_delivery_runs_output_zip` (`output_zip_name`),
  KEY `idx_fund_delivery_runs_run` (`etl_run_id`),
  KEY `idx_fund_delivery_runs_list` (`delivery_list_id`),
  KEY `idx_fund_delivery_runs_event` (`event_id`),
  KEY `idx_fund_delivery_runs_insurer` (`insurer_number`),
  KEY `idx_fund_delivery_runs_output_mode` (`output_mode`, `exam_month`),
  KEY `idx_fund_delivery_runs_source_zip` (`source_download_zip_id`),
  KEY `idx_fund_delivery_runs_status` (`delivery_status`),
  KEY `idx_fund_delivery_runs_created` (`created_at`),
  CONSTRAINT `fk_fund_delivery_runs_run`
    FOREIGN KEY (`etl_run_id`) REFERENCES `health_exam_result`.`etl_runs` (`run_id`),
  CONSTRAINT `fk_fund_delivery_runs_list`
    FOREIGN KEY (`delivery_list_id`) REFERENCES `health_exam_result`.`fund_delivery_lists` (`delivery_list_id`),
  CONSTRAINT `fk_fund_delivery_runs_source_zip`
    FOREIGN KEY (`source_download_zip_id`) REFERENCES `health_exam_result`.`hia_download_zips` (`download_zip_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE `health_exam_result`.`fund_delivery_xml_candidates` (
  `delivery_candidate_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `event_id` bigint DEFAULT NULL,
  `person_year_id` bigint unsigned NOT NULL,
  `hia_download_xml_id` bigint unsigned NOT NULL,
  `person_xml_event_id` bigint unsigned DEFAULT NULL,
  `exam_date` date DEFAULT NULL,
  `exam_month` char(6) DEFAULT NULL,
  `dl_date` date DEFAULT NULL,
  `send_seq` tinyint unsigned NOT NULL DEFAULT 0,
  `candidate_status` varchar(32) NOT NULL DEFAULT 'SELECTED',
  `selection_policy` varchar(32) NOT NULL DEFAULT 'LATEST_DOWNLOAD',
  `selection_reason` text DEFAULT NULL,
  `not_selected_reason` text DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`delivery_candidate_id`),
  UNIQUE KEY `uq_fund_delivery_xml_candidates_xml` (`hia_download_xml_id`),
  KEY `idx_fund_delivery_xml_candidates_event` (`event_id`),
  KEY `idx_fund_delivery_xml_candidates_person` (`person_year_id`),
  KEY `idx_fund_delivery_xml_candidates_person_month` (`person_year_id`, `exam_month`),
  KEY `idx_fund_delivery_xml_candidates_status` (`candidate_status`),
  KEY `idx_fund_delivery_xml_candidates_exam_month` (`exam_month`),
  CONSTRAINT `fk_fund_delivery_xml_candidates_person_year`
    FOREIGN KEY (`person_year_id`) REFERENCES `health_exam_result`.`hia_person_years` (`person_year_id`),
  CONSTRAINT `fk_fund_delivery_xml_candidates_xml`
    FOREIGN KEY (`hia_download_xml_id`) REFERENCES `health_exam_result`.`hia_download_xmls` (`hia_download_xml_id`),
  CONSTRAINT `fk_fund_delivery_xml_candidates_person_event`
    FOREIGN KEY (`person_xml_event_id`) REFERENCES `health_exam_result`.`hia_person_xml_events` (`person_xml_event_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE `health_exam_result`.`fund_delivery_person_status` (
  `delivery_person_status_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `event_id` bigint DEFAULT NULL,
  `person_year_id` bigint unsigned NOT NULL,
  `current_hia_download_xml_id` bigint unsigned DEFAULT NULL,
  `current_delivery_candidate_id` bigint unsigned DEFAULT NULL,
  `delivery_tracking_status` varchar(32) NOT NULL DEFAULT 'NOT_DELIVERED',
  `tracking_reason` text DEFAULT NULL,
  `last_delivery_run_id` bigint unsigned DEFAULT NULL,
  `last_delivery_member_id` bigint unsigned DEFAULT NULL,
  `last_delivered_at` datetime(3) DEFAULT NULL,
  `last_delivered_by` varchar(190) DEFAULT NULL,
  `redelivery_reason` text DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`delivery_person_status_id`),
  UNIQUE KEY `uq_fund_delivery_person_status_person` (`person_year_id`),
  KEY `idx_fund_delivery_person_status_event` (`event_id`),
  KEY `idx_fund_delivery_person_status_current_xml` (`current_hia_download_xml_id`),
  KEY `idx_fund_delivery_person_status_candidate` (`current_delivery_candidate_id`),
  KEY `idx_fund_delivery_person_status_status` (`delivery_tracking_status`),
  KEY `idx_fund_delivery_person_status_last_run` (`last_delivery_run_id`),
  CONSTRAINT `fk_fund_delivery_person_status_person_year`
    FOREIGN KEY (`person_year_id`) REFERENCES `health_exam_result`.`hia_person_years` (`person_year_id`),
  CONSTRAINT `fk_fund_delivery_person_status_current_xml`
    FOREIGN KEY (`current_hia_download_xml_id`) REFERENCES `health_exam_result`.`hia_download_xmls` (`hia_download_xml_id`),
  CONSTRAINT `fk_fund_delivery_person_status_candidate`
    FOREIGN KEY (`current_delivery_candidate_id`) REFERENCES `health_exam_result`.`fund_delivery_xml_candidates` (`delivery_candidate_id`),
  CONSTRAINT `fk_fund_delivery_person_status_last_run`
    FOREIGN KEY (`last_delivery_run_id`) REFERENCES `health_exam_result`.`fund_delivery_runs` (`delivery_run_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE `health_exam_result`.`fund_delivery_list_members` (
  `delivery_list_member_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `delivery_list_id` bigint unsigned NOT NULL,
  `person_year_id` bigint unsigned NOT NULL,
  `delivery_candidate_id` bigint unsigned DEFAULT NULL,
  `hia_download_xml_id` bigint unsigned DEFAULT NULL,
  `list_member_status` varchar(32) NOT NULL DEFAULT 'INCLUDED',
  `list_member_reason` text DEFAULT NULL,
  `added_by` varchar(190) DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`delivery_list_member_id`),
  UNIQUE KEY `uq_fund_delivery_list_members_list_person` (`delivery_list_id`, `person_year_id`),
  KEY `idx_fund_delivery_list_members_person` (`person_year_id`),
  KEY `idx_fund_delivery_list_members_candidate` (`delivery_candidate_id`),
  KEY `idx_fund_delivery_list_members_xml` (`hia_download_xml_id`),
  KEY `idx_fund_delivery_list_members_status` (`list_member_status`),
  CONSTRAINT `fk_fund_delivery_list_members_list`
    FOREIGN KEY (`delivery_list_id`) REFERENCES `health_exam_result`.`fund_delivery_lists` (`delivery_list_id`),
  CONSTRAINT `fk_fund_delivery_list_members_person_year`
    FOREIGN KEY (`person_year_id`) REFERENCES `health_exam_result`.`hia_person_years` (`person_year_id`),
  CONSTRAINT `fk_fund_delivery_list_members_candidate`
    FOREIGN KEY (`delivery_candidate_id`) REFERENCES `health_exam_result`.`fund_delivery_xml_candidates` (`delivery_candidate_id`),
  CONSTRAINT `fk_fund_delivery_list_members_xml`
    FOREIGN KEY (`hia_download_xml_id`) REFERENCES `health_exam_result`.`hia_download_xmls` (`hia_download_xml_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE `health_exam_result`.`fund_delivery_members` (
  `delivery_member_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `delivery_run_id` bigint unsigned NOT NULL,
  `person_year_id` bigint unsigned NOT NULL,
  `hia_download_xml_id` bigint unsigned NOT NULL,
  `delivery_candidate_id` bigint unsigned DEFAULT NULL,
  `person_xml_event_id` bigint unsigned DEFAULT NULL,
  `xml_filename` varchar(255) NOT NULL,
  `xml_sha256` char(64) DEFAULT NULL,
  `facility_code` varchar(64) DEFAULT NULL,
  `facility_name` varchar(255) DEFAULT NULL,
  `exam_date` date DEFAULT NULL,
  `exam_month` char(6) DEFAULT NULL,
  `report_category_code` varchar(64) DEFAULT NULL,
  `program_type_code` varchar(64) DEFAULT NULL,
  `member_status` varchar(32) NOT NULL DEFAULT 'DELIVERED',
  `member_reason` text DEFAULT NULL,
  `submitted_at` datetime(3) DEFAULT NULL,
  `submitted_by` varchar(190) DEFAULT NULL,
  `submission_note` text DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`delivery_member_id`),
  UNIQUE KEY `uq_fund_delivery_members_run_person` (`delivery_run_id`, `person_year_id`),
  KEY `idx_fund_delivery_members_xml` (`hia_download_xml_id`),
  KEY `idx_fund_delivery_members_candidate` (`delivery_candidate_id`),
  KEY `idx_fund_delivery_members_person_event` (`person_xml_event_id`),
  KEY `idx_fund_delivery_members_facility_month` (`facility_code`, `exam_month`),
  KEY `idx_fund_delivery_members_status` (`member_status`),
  KEY `idx_fund_delivery_members_submitted` (`submitted_at`),
  CONSTRAINT `fk_fund_delivery_members_run`
    FOREIGN KEY (`delivery_run_id`) REFERENCES `health_exam_result`.`fund_delivery_runs` (`delivery_run_id`),
  CONSTRAINT `fk_fund_delivery_members_person_year`
    FOREIGN KEY (`person_year_id`) REFERENCES `health_exam_result`.`hia_person_years` (`person_year_id`),
  CONSTRAINT `fk_fund_delivery_members_xml`
    FOREIGN KEY (`hia_download_xml_id`) REFERENCES `health_exam_result`.`hia_download_xmls` (`hia_download_xml_id`),
  CONSTRAINT `fk_fund_delivery_members_candidate`
    FOREIGN KEY (`delivery_candidate_id`) REFERENCES `health_exam_result`.`fund_delivery_xml_candidates` (`delivery_candidate_id`),
  CONSTRAINT `fk_fund_delivery_members_person_event`
    FOREIGN KEY (`person_xml_event_id`) REFERENCES `health_exam_result`.`hia_person_xml_events` (`person_xml_event_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE `health_exam_result`.`fund_delivery_exclusion_rules` (
  `exclusion_rule_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `event_id` bigint DEFAULT NULL,
  `insurer_number` varchar(20) NOT NULL,
  `facility_code` varchar(64) DEFAULT NULL,
  `facility_name` varchar(255) DEFAULT NULL,
  `rule_type` varchar(32) NOT NULL,
  `rule_value` varchar(255) DEFAULT NULL,
  `reason` text DEFAULT NULL,
  `valid_from` date DEFAULT NULL,
  `valid_to` date DEFAULT NULL,
  `is_active` tinyint(1) NOT NULL DEFAULT 1,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`exclusion_rule_id`),
  KEY `idx_fund_delivery_exclusion_rules_event` (`event_id`),
  KEY `idx_fund_delivery_exclusion_rules_insurer` (`insurer_number`),
  KEY `idx_fund_delivery_exclusion_rules_facility` (`facility_code`),
  KEY `idx_fund_delivery_exclusion_rules_active` (`is_active`, `valid_from`, `valid_to`),
  KEY `idx_fund_delivery_exclusion_rules_type` (`rule_type`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
