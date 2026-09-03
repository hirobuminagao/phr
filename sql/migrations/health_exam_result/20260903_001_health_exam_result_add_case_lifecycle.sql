ALTER TABLE `health_exam_result`.`exam_export_cases`
  ADD COLUMN `case_lifecycle_status` varchar(16) NOT NULL DEFAULT 'ACTIVE'
    COMMENT 'case lifecycle: ACTIVE/MERGED' AFTER `case_reason`,
  ADD COLUMN `merged_into_case_id` bigint unsigned DEFAULT NULL
    COMMENT 'MERGED時の統合先case ID' AFTER `case_lifecycle_status`,
  ADD COLUMN `merged_at` datetime(3) DEFAULT NULL
    COMMENT 'case統合日時' AFTER `merged_into_case_id`,
  ADD COLUMN `merged_by_app_user_id` bigint unsigned DEFAULT NULL
    COMMENT 'case統合を実行したapp user ID。保守CLIではNULL' AFTER `merged_at`,
  ADD COLUMN `merge_operation_reason` text DEFAULT NULL
    COMMENT 'case統合理由' AFTER `merged_by_app_user_id`,
  ADD COLUMN `active_case_guard` tinyint
    GENERATED ALWAYS AS (
      CASE WHEN `case_lifecycle_status` = 'ACTIVE' THEN 1 ELSE NULL END
    ) STORED COMMENT '同一受診のACTIVE case一意制約用' AFTER `merge_operation_reason`,
  DROP INDEX `uq_exam_export_cases_natural`,
  ADD UNIQUE KEY `uq_exam_export_cases_active_natural` (
    `event_id`, `subscriber_id`, `exam_date`, `exam_facility_id`, `active_case_guard`
  ),
  ADD KEY `idx_exam_export_cases_lifecycle` (`case_lifecycle_status`),
  ADD KEY `idx_exam_export_cases_merged_into` (`merged_into_case_id`),
  ADD CONSTRAINT `fk_exam_export_cases_merged_into`
    FOREIGN KEY (`merged_into_case_id`)
    REFERENCES `health_exam_result`.`exam_export_cases` (`exam_export_case_id`);
