ALTER TABLE `health_exam_result`.`exam_check_results`
  ADD COLUMN `exam_ledger_id` bigint unsigned DEFAULT NULL AFTER `ledger_type`,
  ADD KEY `idx_exam_check_results_exam_ledger` (`exam_ledger_id`);
