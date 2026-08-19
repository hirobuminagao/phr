ALTER TABLE `health_exam_result`.`exam_item_values`
  ADD COLUMN `review_status` varchar(32) NOT NULL DEFAULT 'NONE' COMMENT 'NONE/NEEDS_CONFIRMATION/APPROVED_WITH_REASON/EXCLUDED/WAITING_RESUBMISSION/RESOLVED_BY_SOURCE_VALUE' AFTER `value_source_role`,
  ADD COLUMN `reviewed_at` datetime(3) DEFAULT NULL AFTER `review_status`,
  ADD COLUMN `reviewed_by_app_user_id` bigint unsigned DEFAULT NULL AFTER `reviewed_at`,
  ADD KEY `idx_exam_item_values_review_status` (`review_status`),
  ADD KEY `idx_exam_item_values_reviewed_by` (`reviewed_by_app_user_id`),
  ADD KEY `idx_exam_item_values_case_review` (`ledger_type`, `ledger_id`, `value_source_role`, `review_status`);

CREATE TABLE `health_exam_result`.`exam_item_value_audit_logs` (
  `exam_item_value_audit_log_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `exam_item_value_id` bigint unsigned NOT NULL,
  `event_id` bigint DEFAULT NULL,
  `ledger_type` varchar(16) DEFAULT NULL,
  `ledger_id` bigint unsigned DEFAULT NULL,
  `field_name` varchar(64) NOT NULL,
  `old_value` text DEFAULT NULL,
  `new_value` text DEFAULT NULL,
  `source` varchar(64) NOT NULL DEFAULT 'ADMIN_UI',
  `note` text DEFAULT NULL,
  `changed_by_app_user_id` bigint unsigned DEFAULT NULL,
  `change_run_id` bigint unsigned DEFAULT NULL,
  `changed_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`exam_item_value_audit_log_id`),
  KEY `idx_exam_item_value_audit_logs_item` (`exam_item_value_id`),
  KEY `idx_exam_item_value_audit_logs_event` (`event_id`),
  KEY `idx_exam_item_value_audit_logs_ledger` (`ledger_type`, `ledger_id`),
  KEY `idx_exam_item_value_audit_logs_field` (`field_name`),
  KEY `idx_exam_item_value_audit_logs_changed_by` (`changed_by_app_user_id`),
  KEY `idx_exam_item_value_audit_logs_changed_at` (`changed_at`),
  CONSTRAINT `fk_exam_item_value_audit_logs_item`
    FOREIGN KEY (`exam_item_value_id`)
    REFERENCES `health_exam_result`.`exam_item_values` (`id`)
    ON DELETE RESTRICT
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='exam_item_valuesの人手判断・理由ありOK・除外等のfield単位変更履歴。';
