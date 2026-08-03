ALTER TABLE `health_exam_result`.`csv_row_ledger`
  ADD COLUMN `basic_info_status` varchar(32) DEFAULT NULL AFTER `address`,
  ADD COLUMN `basic_info_reason` text DEFAULT NULL AFTER `basic_info_status`,
  ADD COLUMN `address_source` varchar(32) DEFAULT NULL AFTER `basic_info_reason`,
  ADD COLUMN `address_completion_status` varchar(32) DEFAULT NULL AFTER `address_source`,
  ADD COLUMN `address_completion_reason` text DEFAULT NULL AFTER `address_completion_status`,
  ADD COLUMN `address_completed_value` varchar(255) DEFAULT NULL AFTER `address_completion_reason`,
  ADD COLUMN `postal_code_completed_value` varchar(16) DEFAULT NULL AFTER `address_completed_value`,
  ADD KEY `idx_csv_row_ledger_basic_info_status` (`basic_info_status`),
  ADD KEY `idx_csv_row_ledger_address_completion_status` (`address_completion_status`);

ALTER TABLE `health_exam_result`.`xml_ledger`
  ADD COLUMN `basic_info_status` varchar(32) DEFAULT NULL AFTER `subscriber_match_reason`,
  ADD COLUMN `basic_info_reason` text DEFAULT NULL AFTER `basic_info_status`,
  ADD COLUMN `address_source` varchar(32) DEFAULT NULL AFTER `basic_info_reason`,
  ADD COLUMN `address_completion_status` varchar(32) DEFAULT NULL AFTER `address_source`,
  ADD COLUMN `address_completion_reason` text DEFAULT NULL AFTER `address_completion_status`,
  ADD COLUMN `address_completed_value` varchar(255) DEFAULT NULL AFTER `address_completion_reason`,
  ADD COLUMN `postal_code_completed_value` varchar(16) DEFAULT NULL AFTER `address_completed_value`,
  ADD KEY `idx_xml_ledger_basic_info_status` (`basic_info_status`),
  ADD KEY `idx_xml_ledger_address_completion_status` (`address_completion_status`);

ALTER TABLE `health_exam_result`.`exam_ledgers`
  ADD COLUMN `basic_info_status` varchar(32) DEFAULT NULL AFTER `address`,
  ADD COLUMN `basic_info_reason` text DEFAULT NULL AFTER `basic_info_status`,
  ADD COLUMN `address_source` varchar(32) DEFAULT NULL AFTER `basic_info_reason`,
  ADD COLUMN `address_completion_status` varchar(32) DEFAULT NULL AFTER `address_source`,
  ADD COLUMN `address_completion_reason` text DEFAULT NULL AFTER `address_completion_status`,
  ADD COLUMN `address_completed_value` varchar(255) DEFAULT NULL AFTER `address_completion_reason`,
  ADD COLUMN `postal_code_completed_value` varchar(16) DEFAULT NULL AFTER `address_completed_value`,
  ADD KEY `idx_exam_ledgers_basic_info_status` (`basic_info_status`),
  ADD KEY `idx_exam_ledgers_address_completion_status` (`address_completion_status`);
