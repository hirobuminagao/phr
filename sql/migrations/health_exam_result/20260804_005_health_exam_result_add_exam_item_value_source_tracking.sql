ALTER TABLE `health_exam_result`.`exam_item_values`
  ADD COLUMN `source_ledger_type` varchar(16) DEFAULT NULL COMMENT '清書値の採用元ledger_type' AFTER `validation_reason`,
  ADD COLUMN `source_ledger_id` bigint unsigned DEFAULT NULL COMMENT '清書値の採用元ledger_id' AFTER `source_ledger_type`,
  ADD COLUMN `source_exam_item_value_id` bigint unsigned DEFAULT NULL COMMENT '清書値の採用元exam_item_values.id' AFTER `source_ledger_id`,
  ADD COLUMN `value_source_role` varchar(32) DEFAULT NULL COMMENT 'PRIMARY/SUPPLEMENT等' AFTER `source_exam_item_value_id`,
  ADD KEY `idx_exam_item_values_source_value` (`source_ledger_type`, `source_ledger_id`, `source_exam_item_value_id`);
