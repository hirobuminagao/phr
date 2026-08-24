CREATE TABLE IF NOT EXISTS `manual_exam_entry_drafts` (
  `manual_exam_entry_draft_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `event_id` bigint NOT NULL,
  `draft_status` varchar(32) COLLATE utf8mb4_ja_0900_as_cs NOT NULL DEFAULT 'DRAFT',
  `entry_purpose` varchar(32) COLLATE utf8mb4_ja_0900_as_cs NOT NULL DEFAULT 'PAPER_ONLY',
  `exam_export_case_id` bigint unsigned DEFAULT NULL,
  `subscriber_id` bigint unsigned DEFAULT NULL,
  `hia_subscriber_id` varchar(64) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `person_id_custom` varchar(128) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `insurer_number` varchar(16) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `insurance_symbol` varchar(64) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `insurance_number` varchar(64) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `insurance_branch_number` varchar(16) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `name_full` varchar(255) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `name_kana` varchar(255) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `birthdate` date DEFAULT NULL,
  `gender_code` varchar(16) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `exam_facility_id` bigint unsigned DEFAULT NULL,
  `facility_code` varchar(32) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `facility_name` varchar(255) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `facility_document_id` varchar(255) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `exam_date` date DEFAULT NULL,
  `note` text COLLATE utf8mb4_ja_0900_as_cs,
  `created_by_app_user_id` bigint unsigned DEFAULT NULL,
  `updated_by_app_user_id` bigint unsigned DEFAULT NULL,
  `applied_by_app_user_id` bigint unsigned DEFAULT NULL,
  `applied_at` datetime(3) DEFAULT NULL,
  `applied_exam_ledger_id` bigint unsigned DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`manual_exam_entry_draft_id`),
  KEY `idx_manual_exam_entry_drafts_event_status` (`event_id`,`draft_status`),
  KEY `idx_manual_exam_entry_drafts_case` (`exam_export_case_id`),
  KEY `idx_manual_exam_entry_drafts_subscriber` (`subscriber_id`),
  KEY `idx_manual_exam_entry_drafts_hia` (`hia_subscriber_id`),
  KEY `idx_manual_exam_entry_drafts_person` (`person_id_custom`),
  KEY `idx_manual_exam_entry_drafts_facility_date` (`facility_code`,`exam_date`),
  KEY `idx_manual_exam_entry_drafts_created_by` (`created_by_app_user_id`),
  KEY `idx_manual_exam_entry_drafts_updated_by` (`updated_by_app_user_id`),
  KEY `idx_manual_exam_entry_drafts_applied_ledger` (`applied_exam_ledger_id`),
  CONSTRAINT `fk_manual_exam_entry_drafts_case`
    FOREIGN KEY (`exam_export_case_id`) REFERENCES `exam_export_cases` (`exam_export_case_id`)
    ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `fk_manual_exam_entry_drafts_applied_ledger`
    FOREIGN KEY (`applied_exam_ledger_id`) REFERENCES `exam_ledgers` (`exam_ledger_id`)
    ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE IF NOT EXISTS `manual_exam_entry_draft_values` (
  `manual_exam_entry_draft_value_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `manual_exam_entry_draft_id` bigint unsigned NOT NULL,
  `namecode` varchar(32) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,
  `namecode_display_name` varchar(255) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `identity_item_code` varchar(32) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `identity_item_name` varchar(255) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `xml_value_type` varchar(16) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `raw_value` text COLLATE utf8mb4_ja_0900_as_cs,
  `normalized_value` text COLLATE utf8mb4_ja_0900_as_cs,
  `code_system` varchar(128) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `code_value` varchar(64) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `code_display` varchar(255) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `display_unit` varchar(64) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `ucum_unit` varchar(64) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `method_code` varchar(32) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `method_name` varchar(255) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `occurrence_no` int NOT NULL DEFAULT '1',
  `include_flag` tinyint(1) NOT NULL DEFAULT '1',
  `input_status` varchar(32) COLLATE utf8mb4_ja_0900_as_cs NOT NULL DEFAULT 'DRAFT',
  `note` text COLLATE utf8mb4_ja_0900_as_cs,
  `updated_by_app_user_id` bigint unsigned DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`manual_exam_entry_draft_value_id`),
  KEY `idx_manual_exam_entry_draft_values_draft` (`manual_exam_entry_draft_id`),
  KEY `idx_manual_exam_entry_draft_values_namecode` (`namecode`),
  KEY `idx_manual_exam_entry_draft_values_identity` (`identity_item_code`),
  KEY `idx_manual_exam_entry_draft_values_status` (`input_status`),
  CONSTRAINT `fk_manual_exam_entry_draft_values_draft`
    FOREIGN KEY (`manual_exam_entry_draft_id`) REFERENCES `manual_exam_entry_drafts` (`manual_exam_entry_draft_id`)
    ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE IF NOT EXISTS `manual_exam_entry_draft_audit_logs` (
  `manual_exam_entry_draft_audit_log_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `manual_exam_entry_draft_id` bigint unsigned NOT NULL,
  `manual_exam_entry_draft_value_id` bigint unsigned DEFAULT NULL,
  `event_id` bigint NOT NULL,
  `action_code` varchar(64) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,
  `field_name` varchar(128) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `old_value` text COLLATE utf8mb4_ja_0900_as_cs,
  `new_value` text COLLATE utf8mb4_ja_0900_as_cs,
  `source` varchar(32) COLLATE utf8mb4_ja_0900_as_cs NOT NULL DEFAULT 'ADMIN_UI',
  `note` text COLLATE utf8mb4_ja_0900_as_cs,
  `changed_by_app_user_id` bigint unsigned DEFAULT NULL,
  `changed_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`manual_exam_entry_draft_audit_log_id`),
  KEY `idx_manual_exam_entry_draft_audit_draft` (`manual_exam_entry_draft_id`),
  KEY `idx_manual_exam_entry_draft_audit_value` (`manual_exam_entry_draft_value_id`),
  KEY `idx_manual_exam_entry_draft_audit_event` (`event_id`,`action_code`),
  KEY `idx_manual_exam_entry_draft_audit_changed_by` (`changed_by_app_user_id`),
  CONSTRAINT `fk_manual_exam_entry_draft_audit_draft`
    FOREIGN KEY (`manual_exam_entry_draft_id`) REFERENCES `manual_exam_entry_drafts` (`manual_exam_entry_draft_id`)
    ON DELETE CASCADE ON UPDATE RESTRICT,
  CONSTRAINT `fk_manual_exam_entry_draft_audit_value`
    FOREIGN KEY (`manual_exam_entry_draft_value_id`) REFERENCES `manual_exam_entry_draft_values` (`manual_exam_entry_draft_value_id`)
    ON DELETE SET NULL ON UPDATE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs;
