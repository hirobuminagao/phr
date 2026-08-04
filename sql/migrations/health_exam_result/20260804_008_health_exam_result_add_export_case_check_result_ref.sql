ALTER TABLE `health_exam_result`.`exam_check_results`
  ADD COLUMN `exam_export_case_id` bigint unsigned DEFAULT NULL
    AFTER `exam_ledger_id`,
  ADD KEY `idx_exam_check_results_exam_export_case` (`exam_export_case_id`);
