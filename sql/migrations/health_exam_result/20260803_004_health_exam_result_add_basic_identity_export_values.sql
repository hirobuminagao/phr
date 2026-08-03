ALTER TABLE `health_exam_result`.`csv_row_ledger`
  ADD COLUMN `insurance_symbol_export_value` varchar(190) DEFAULT NULL AFTER `insurance_symbol_match`,
  ADD COLUMN `insurance_symbol_export_source` varchar(32) DEFAULT NULL AFTER `insurance_symbol_export_value`,
  ADD COLUMN `insurance_symbol_export_reason` text DEFAULT NULL AFTER `insurance_symbol_export_source`,
  ADD COLUMN `insurance_number_export_value` varchar(190) DEFAULT NULL AFTER `insurance_number_match`,
  ADD COLUMN `insurance_number_export_source` varchar(32) DEFAULT NULL AFTER `insurance_number_export_value`,
  ADD COLUMN `insurance_number_export_reason` text DEFAULT NULL AFTER `insurance_number_export_source`,
  ADD COLUMN `name_kana_export_value` varchar(255) DEFAULT NULL AFTER `name_kana_match`,
  ADD COLUMN `name_kana_export_source` varchar(32) DEFAULT NULL AFTER `name_kana_export_value`,
  ADD COLUMN `name_kana_export_reason` text DEFAULT NULL AFTER `name_kana_export_source`,
  ADD KEY `idx_csv_row_ledger_identity_export_source` (`insurance_symbol_export_source`, `insurance_number_export_source`, `name_kana_export_source`);

ALTER TABLE `health_exam_result`.`xml_ledger`
  ADD COLUMN `insurance_symbol_export_value` varchar(190) DEFAULT NULL AFTER `insurance_symbol_match`,
  ADD COLUMN `insurance_symbol_export_source` varchar(32) DEFAULT NULL AFTER `insurance_symbol_export_value`,
  ADD COLUMN `insurance_symbol_export_reason` text DEFAULT NULL AFTER `insurance_symbol_export_source`,
  ADD COLUMN `insurance_number_export_value` varchar(190) DEFAULT NULL AFTER `insurance_number_match`,
  ADD COLUMN `insurance_number_export_source` varchar(32) DEFAULT NULL AFTER `insurance_number_export_value`,
  ADD COLUMN `insurance_number_export_reason` text DEFAULT NULL AFTER `insurance_number_export_source`,
  ADD COLUMN `name_kana_export_value` varchar(255) DEFAULT NULL AFTER `name_kana_match`,
  ADD COLUMN `name_kana_export_source` varchar(32) DEFAULT NULL AFTER `name_kana_export_value`,
  ADD COLUMN `name_kana_export_reason` text DEFAULT NULL AFTER `name_kana_export_source`,
  ADD KEY `idx_xml_ledger_identity_export_source` (`insurance_symbol_export_source`, `insurance_number_export_source`, `name_kana_export_source`);

ALTER TABLE `health_exam_result`.`exam_ledgers`
  ADD COLUMN `insurance_symbol_export_value` varchar(190) DEFAULT NULL AFTER `insurance_symbol_match`,
  ADD COLUMN `insurance_symbol_export_source` varchar(32) DEFAULT NULL AFTER `insurance_symbol_export_value`,
  ADD COLUMN `insurance_symbol_export_reason` text DEFAULT NULL AFTER `insurance_symbol_export_source`,
  ADD COLUMN `insurance_number_export_value` varchar(190) DEFAULT NULL AFTER `insurance_number_match`,
  ADD COLUMN `insurance_number_export_source` varchar(32) DEFAULT NULL AFTER `insurance_number_export_value`,
  ADD COLUMN `insurance_number_export_reason` text DEFAULT NULL AFTER `insurance_number_export_source`,
  ADD COLUMN `name_kana_export_value` varchar(255) DEFAULT NULL AFTER `name_kana_match`,
  ADD COLUMN `name_kana_export_source` varchar(32) DEFAULT NULL AFTER `name_kana_export_value`,
  ADD COLUMN `name_kana_export_reason` text DEFAULT NULL AFTER `name_kana_export_source`,
  ADD KEY `idx_exam_ledgers_identity_export_source` (`insurance_symbol_export_source`, `insurance_number_export_source`, `name_kana_export_source`);
