ALTER TABLE `phr_master`.`medical_folder_aliases`
  ADD COLUMN `expected_source_mode` varchar(32) NOT NULL DEFAULT 'UNKNOWN' COMMENT '想定受領モード。UNKNOWN/XML_ONLY/CSV_ONLY/XML_CSV_MERGE' AFTER `exam_facility_id`,
  ADD COLUMN `csv_format_version_id` bigint unsigned DEFAULT NULL COMMENT 'このaliasで使用するCSVテンプレート' AFTER `expected_source_mode`,
  ADD KEY `idx_medical_folder_aliases_source_mode` (`expected_source_mode`),
  ADD KEY `idx_medical_folder_aliases_csv_format` (`csv_format_version_id`);
