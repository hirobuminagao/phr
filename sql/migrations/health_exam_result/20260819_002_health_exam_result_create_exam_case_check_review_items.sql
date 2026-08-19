CREATE TABLE `health_exam_result`.`exam_case_check_review_items` (
  `exam_case_check_review_item_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `event_id` bigint NOT NULL,
  `exam_export_case_id` bigint unsigned NOT NULL,
  `check_scope` varchar(32) NOT NULL COMMENT 'ARTICLE44/SPECIFIC_HEALTH等',
  `check_item_code` varchar(32) NOT NULL COMMENT '440...やspecific健診namecode等の業務チェックID',
  `check_item_name` varchar(255) DEFAULT NULL,
  `raw_value_type` varchar(32) DEFAULT NULL,
  `validation_reason` text DEFAULT NULL,
  `review_status` varchar(32) NOT NULL DEFAULT 'NEEDS_CONFIRMATION' COMMENT 'NEEDS_CONFIRMATION/APPROVED_WITH_REASON/EXCLUDED/WAITING_RESUBMISSION/RESOLVED_BY_SOURCE_VALUE/NONE',
  `reviewed_at` datetime(3) DEFAULT NULL,
  `reviewed_by_app_user_id` bigint unsigned DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`exam_case_check_review_item_id`),
  UNIQUE KEY `uq_case_check_review_item` (`exam_export_case_id`, `check_scope`, `check_item_code`),
  KEY `idx_case_check_review_items_event` (`event_id`),
  KEY `idx_case_check_review_items_case` (`exam_export_case_id`),
  KEY `idx_case_check_review_items_scope` (`check_scope`, `check_item_code`),
  KEY `idx_case_check_review_items_status` (`review_status`),
  KEY `idx_case_check_review_items_reviewed_by` (`reviewed_by_app_user_id`),
  CONSTRAINT `fk_case_check_review_items_case`
    FOREIGN KEY (`exam_export_case_id`)
    REFERENCES `health_exam_result`.`exam_export_cases` (`exam_export_case_id`)
    ON DELETE RESTRICT
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='case単位の法定/特定健診チェック不足・理由ありOK・再提出待ち等の現在状態。';

CREATE TABLE `health_exam_result`.`exam_case_check_review_item_audit_logs` (
  `exam_case_check_review_item_audit_log_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `exam_case_check_review_item_id` bigint unsigned NOT NULL,
  `event_id` bigint DEFAULT NULL,
  `exam_export_case_id` bigint unsigned DEFAULT NULL,
  `check_scope` varchar(32) DEFAULT NULL,
  `check_item_code` varchar(32) DEFAULT NULL,
  `field_name` varchar(64) NOT NULL,
  `old_value` text DEFAULT NULL,
  `new_value` text DEFAULT NULL,
  `source` varchar(64) NOT NULL DEFAULT 'ADMIN_UI',
  `note` text DEFAULT NULL,
  `changed_by_app_user_id` bigint unsigned DEFAULT NULL,
  `change_run_id` bigint unsigned DEFAULT NULL,
  `changed_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`exam_case_check_review_item_audit_log_id`),
  KEY `idx_case_check_review_audit_item` (`exam_case_check_review_item_id`),
  KEY `idx_case_check_review_audit_event` (`event_id`),
  KEY `idx_case_check_review_audit_case` (`exam_export_case_id`),
  KEY `idx_case_check_review_audit_scope` (`check_scope`, `check_item_code`),
  KEY `idx_case_check_review_audit_field` (`field_name`),
  KEY `idx_case_check_review_audit_changed_by` (`changed_by_app_user_id`),
  KEY `idx_case_check_review_audit_changed_at` (`changed_at`),
  CONSTRAINT `fk_case_check_review_audit_item`
    FOREIGN KEY (`exam_case_check_review_item_id`)
    REFERENCES `health_exam_result`.`exam_case_check_review_items` (`exam_case_check_review_item_id`)
    ON DELETE RESTRICT
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='caseチェック確認項目の人手判断・理由ありOK・除外等のfield単位変更履歴。';
