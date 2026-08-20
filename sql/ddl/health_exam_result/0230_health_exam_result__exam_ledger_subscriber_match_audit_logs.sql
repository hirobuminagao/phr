CREATE TABLE `health_exam_result`.`exam_ledger_subscriber_match_audit_logs` (
  `exam_ledger_subscriber_match_audit_log_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `event_id` bigint NOT NULL,
  `exam_ledger_id` bigint unsigned NOT NULL,
  `old_subscriber_id` bigint unsigned DEFAULT NULL,
  `new_subscriber_id` bigint unsigned DEFAULT NULL,
  `old_hia_subscriber_id` varchar(190) DEFAULT NULL,
  `new_hia_subscriber_id` varchar(190) DEFAULT NULL,
  `old_person_id_custom` varchar(190) DEFAULT NULL,
  `new_person_id_custom` varchar(190) DEFAULT NULL,
  `old_identity_hash` char(64) DEFAULT NULL,
  `new_identity_hash` char(64) DEFAULT NULL,
  `old_subscriber_match_status` varchar(32) DEFAULT NULL,
  `new_subscriber_match_status` varchar(32) DEFAULT NULL,
  `old_subscriber_match_method` varchar(64) DEFAULT NULL,
  `new_subscriber_match_method` varchar(64) DEFAULT NULL,
  `old_subscriber_match_reason` text,
  `new_subscriber_match_reason` text,
  `applied_subscriber_export_values` tinyint(1) NOT NULL DEFAULT 0,
  `applied_fields_json` json DEFAULT NULL,
  `note` text,
  `changed_by_app_user_id` bigint unsigned DEFAULT NULL,
  `changed_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`exam_ledger_subscriber_match_audit_log_id`),
  KEY `idx_exam_ledger_subscriber_match_audit_ledger` (`exam_ledger_id`),
  KEY `idx_exam_ledger_subscriber_match_audit_event` (`event_id`),
  KEY `idx_exam_ledger_subscriber_match_audit_new_subscriber` (`new_subscriber_id`),
  KEY `idx_exam_ledger_subscriber_match_audit_changed_at` (`changed_at`),
  CONSTRAINT `fk_exam_ledger_subscriber_match_audit_ledger`
    FOREIGN KEY (`exam_ledger_id`)
    REFERENCES `health_exam_result`.`exam_ledgers` (`exam_ledger_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='受領ledgerの加入者突合を手動確定した履歴';
