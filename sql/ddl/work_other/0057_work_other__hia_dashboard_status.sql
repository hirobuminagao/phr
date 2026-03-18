CREATE TABLE `work_other`.`hia_dashboard_status` (

  `hia_dashboard_person_id` bigint unsigned NOT NULL AUTO_INCREMENT,

  -- identity
  `snapshot_identity_key` varchar(190) NOT NULL,
  `insurer_number` varchar(20) NOT NULL,

  -- raw insurance fields
  `insurance_symbol` varchar(190) DEFAULT NULL,
  `insurance_number` varchar(190) DEFAULT NULL,
  `relationship` varchar(64) DEFAULT NULL,
  `branch_number` varchar(64) DEFAULT NULL,

  -- normalized match fields
  `insurance_symbol_match` varchar(190) DEFAULT NULL,
  `insurance_number_match` varchar(190) DEFAULT NULL,
  `relationship_match` varchar(64) DEFAULT NULL,

  -- name
  `name` varchar(190) DEFAULT NULL,
  `name_match` varchar(190) DEFAULT NULL,

  -- subscriber enrichment (resolved from subscribers at import time)
  `subscriber_person_id_custom` varchar(64) DEFAULT NULL,
  `subscriber_name_kana_full` varchar(190) DEFAULT NULL,
  `subscriber_gender_code` tinyint unsigned DEFAULT NULL,
  `subscriber_birth` date DEFAULT NULL,

  -- status
  `status` varchar(64) DEFAULT NULL,
  `reservation_date` date DEFAULT NULL,
  `exam_date` date DEFAULT NULL,

  -- organization
  `company_name` varchar(190) DEFAULT NULL,
  `department_name` varchar(190) DEFAULT NULL,

  -- medical
  `medical_institution` varchar(190) DEFAULT NULL,
  `course_name` varchar(190) DEFAULT NULL,

  -- contact
  `employee_number` varchar(190) DEFAULT NULL,
  `email` varchar(190) DEFAULT NULL,

  -- reminder
  `reminder_send_count` int DEFAULT NULL,

  -- exclusion
  `exclusion_reason` varchar(190) DEFAULT NULL,

  -- diff / tracking
  `row_sha256` char(64) NOT NULL,

  `first_seen_run_id` bigint unsigned NOT NULL,
  `last_seen_run_id` bigint unsigned NOT NULL,

  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  `identity_hash` char(64)
    CHARACTER SET ascii
    COLLATE ascii_bin
    DEFAULT NULL
    COMMENT 'SHA256 hash of (person_id_custom|name_kana_full_match|gender_code) used for fast identity joins',
  `subscriber_name_kana_full_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs
    DEFAULT NULL
    COMMENT 'subscriber-side kana match value copied from dev_phr.subscribers for identity matching and hash generation',

  PRIMARY KEY (`hia_dashboard_person_id`),

  UNIQUE KEY `uq_hia_dashboard_snapshot_identity` (`snapshot_identity_key`),

  KEY `idx_hia_dashboard_insurer` (`insurer_number`),
  KEY `idx_hia_dashboard_symbol_number` (`insurance_symbol_match`, `insurance_number_match`),
  KEY `idx_hia_dashboard_subscriber_person_id_custom` (`subscriber_person_id_custom`),
  KEY `idx_hia_dashboard_identity_hash` (`identity_hash`),
  KEY `idx_hia_dashboard_subscriber_name_kana_full_match` (`subscriber_name_kana_full_match`),
  KEY `idx_hia_dashboard_last_seen_run` (`last_seen_run_id`),

  CONSTRAINT `fk_hia_dashboard_first_run`
    FOREIGN KEY (`first_seen_run_id`)
    REFERENCES `work_other`.`etl_runs` (`run_id`),

  CONSTRAINT `fk_hia_dashboard_last_run`
    FOREIGN KEY (`last_seen_run_id`)
    REFERENCES `work_other`.`etl_runs` (`run_id`)

)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;