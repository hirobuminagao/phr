ALTER TABLE `health_exam_result`.`exam_result_ledger_report`
  ADD COLUMN `exam_ledger_id` bigint unsigned DEFAULT NULL COMMENT '統合ledger ID' AFTER `ledger_type`,
  ADD UNIQUE KEY `uq_exam_result_ledger_report_exam_ledger` (`exam_ledger_id`);
