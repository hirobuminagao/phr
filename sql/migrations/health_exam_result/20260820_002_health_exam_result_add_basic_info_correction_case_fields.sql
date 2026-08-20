ALTER TABLE `health_exam_result`.`exam_export_cases`
  ADD COLUMN `exam_date_export_value` date DEFAULT NULL AFTER `exam_date`,
  ADD COLUMN `exam_date_export_source` varchar(32) DEFAULT NULL AFTER `exam_date_export_value`,
  ADD COLUMN `exam_date_export_reason` text DEFAULT NULL AFTER `exam_date_export_source`,
  ADD COLUMN `insurance_branch_number_export_value` varchar(64) DEFAULT NULL AFTER `insurance_branch_number_raw`,
  ADD COLUMN `insurance_branch_number_export_source` varchar(32) DEFAULT NULL AFTER `insurance_branch_number_export_value`,
  ADD COLUMN `insurance_branch_number_export_reason` text DEFAULT NULL AFTER `insurance_branch_number_export_source`,
  ADD COLUMN `exam_ticket_number_export_value` varchar(190) DEFAULT NULL AFTER `insurance_branch_number_export_reason`,
  ADD COLUMN `exam_ticket_number_export_source` varchar(32) DEFAULT NULL AFTER `exam_ticket_number_export_value`,
  ADD COLUMN `exam_ticket_number_export_reason` text DEFAULT NULL AFTER `exam_ticket_number_export_source`,
  ADD COLUMN `exam_ticket_expires_on_export_value` date DEFAULT NULL AFTER `exam_ticket_number_export_reason`,
  ADD COLUMN `exam_ticket_expires_on_export_source` varchar(32) DEFAULT NULL AFTER `exam_ticket_expires_on_export_value`,
  ADD COLUMN `exam_ticket_expires_on_export_reason` text DEFAULT NULL AFTER `exam_ticket_expires_on_export_source`;
