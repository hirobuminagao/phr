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
