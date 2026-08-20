CREATE TABLE `health_exam_result`.`exam_case_basic_info_corrections` (
  `exam_case_basic_info_correction_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `event_id` bigint NOT NULL,
  `exam_export_case_id` bigint unsigned NOT NULL,
  `field_code` varchar(64) NOT NULL COMMENT 'exam_date/insurer_number/name_kana/insurance_symbol/insurance_number/insurance_branch_number/exam_ticket_number/exam_ticket_expires_on/postal_code/address',
  `field_label` varchar(255) NOT NULL,
  `source_value` text DEFAULT NULL COMMENT '補正前にcaseへ入っていた値',
  `corrected_value` text DEFAULT NULL COMMENT '作業者が入力した値',
  `normalized_value` text DEFAULT NULL COMMENT '共通libで出力用に正規化した値',
  `normalization_status` varchar(32) NOT NULL DEFAULT 'OK',
  `normalization_reason` text DEFAULT NULL,
  `correction_status` varchar(32) NOT NULL DEFAULT 'ACTIVE' COMMENT 'ACTIVE/CLEARED',
  `correction_reason` text DEFAULT NULL,
  `corrected_at` datetime(3) DEFAULT NULL,
  `corrected_by_app_user_id` bigint unsigned DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`exam_case_basic_info_correction_id`),
  UNIQUE KEY `uq_case_basic_info_correction_field` (`exam_export_case_id`, `field_code`),
  KEY `idx_case_basic_info_corrections_event` (`event_id`),
  KEY `idx_case_basic_info_corrections_case` (`exam_export_case_id`),
  KEY `idx_case_basic_info_corrections_field` (`field_code`),
  KEY `idx_case_basic_info_corrections_status` (`correction_status`),
  KEY `idx_case_basic_info_corrections_corrected_by` (`corrected_by_app_user_id`),
  CONSTRAINT `fk_case_basic_info_corrections_case`
    FOREIGN KEY (`exam_export_case_id`)
    REFERENCES `health_exam_result`.`exam_export_cases` (`exam_export_case_id`)
    ON DELETE RESTRICT
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='個人case基本情報の補正現在値。出力case本体へ反映したうえで理由と正規化結果を保持する。';

CREATE TABLE `health_exam_result`.`exam_case_basic_info_correction_audit_logs` (
  `exam_case_basic_info_correction_audit_log_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `exam_case_basic_info_correction_id` bigint unsigned NOT NULL,
  `event_id` bigint DEFAULT NULL,
  `exam_export_case_id` bigint unsigned DEFAULT NULL,
  `field_code` varchar(64) DEFAULT NULL,
  `field_name` varchar(64) NOT NULL,
  `old_value` text DEFAULT NULL,
  `new_value` text DEFAULT NULL,
  `source` varchar(64) NOT NULL DEFAULT 'ADMIN_UI',
  `note` text DEFAULT NULL,
  `changed_by_app_user_id` bigint unsigned DEFAULT NULL,
  `change_run_id` bigint unsigned DEFAULT NULL,
  `changed_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`exam_case_basic_info_correction_audit_log_id`),
  KEY `idx_case_basic_info_correction_audit_correction` (`exam_case_basic_info_correction_id`),
  KEY `idx_case_basic_info_correction_audit_event` (`event_id`),
  KEY `idx_case_basic_info_correction_audit_case` (`exam_export_case_id`),
  KEY `idx_case_basic_info_correction_audit_field` (`field_code`, `field_name`),
  KEY `idx_case_basic_info_correction_audit_changed_by` (`changed_by_app_user_id`),
  KEY `idx_case_basic_info_correction_audit_changed_at` (`changed_at`),
  CONSTRAINT `fk_case_basic_info_correction_audit_correction`
    FOREIGN KEY (`exam_case_basic_info_correction_id`)
    REFERENCES `health_exam_result`.`exam_case_basic_info_corrections` (`exam_case_basic_info_correction_id`)
    ON DELETE RESTRICT
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='個人case基本情報補正のfield単位変更履歴。';
