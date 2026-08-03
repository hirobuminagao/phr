ALTER TABLE `health_exam_result`.`csv_row_ledger`
  ADD COLUMN `insurer_number_source` varchar(32) DEFAULT NULL AFTER `basic_info_reason`,
  ADD COLUMN `insurer_number_completion_status` varchar(32) DEFAULT NULL AFTER `insurer_number_source`,
  ADD COLUMN `insurer_number_completion_reason` text DEFAULT NULL AFTER `insurer_number_completion_status`,
  ADD COLUMN `insurer_number_export_value` varchar(20) DEFAULT NULL AFTER `insurer_number_completion_reason`,
  ADD KEY `idx_csv_row_ledger_insurer_number_completion_status` (`insurer_number_completion_status`);

ALTER TABLE `health_exam_result`.`xml_ledger`
  ADD COLUMN `insurer_number_source` varchar(32) DEFAULT NULL AFTER `basic_info_reason`,
  ADD COLUMN `insurer_number_completion_status` varchar(32) DEFAULT NULL AFTER `insurer_number_source`,
  ADD COLUMN `insurer_number_completion_reason` text DEFAULT NULL AFTER `insurer_number_completion_status`,
  ADD COLUMN `insurer_number_export_value` varchar(20) DEFAULT NULL AFTER `insurer_number_completion_reason`,
  ADD KEY `idx_xml_ledger_insurer_number_completion_status` (`insurer_number_completion_status`);

ALTER TABLE `health_exam_result`.`exam_ledgers`
  ADD COLUMN `insurer_number_source` varchar(32) DEFAULT NULL AFTER `basic_info_reason`,
  ADD COLUMN `insurer_number_completion_status` varchar(32) DEFAULT NULL AFTER `insurer_number_source`,
  ADD COLUMN `insurer_number_completion_reason` text DEFAULT NULL AFTER `insurer_number_completion_status`,
  ADD COLUMN `insurer_number_export_value` varchar(20) DEFAULT NULL AFTER `insurer_number_completion_reason`,
  ADD KEY `idx_exam_ledgers_insurer_number_completion_status` (`insurer_number_completion_status`);
