ALTER TABLE `health_exam_result`.`exam_check_results`
  ADD COLUMN `ledger_type` varchar(16) NOT NULL DEFAULT 'XML' AFTER `id`,
  MODIFY COLUMN `xml_ledger_id` bigint unsigned DEFAULT NULL,
  ADD COLUMN `csv_row_ledger_id` bigint unsigned DEFAULT NULL AFTER `xml_ledger_id`,
  ADD KEY `idx_exam_check_results_ledger_type_xml` (`ledger_type`, `xml_ledger_id`),
  ADD KEY `idx_exam_check_results_ledger_type_csv` (`ledger_type`, `csv_row_ledger_id`);
