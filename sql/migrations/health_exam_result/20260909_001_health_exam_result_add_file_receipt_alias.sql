ALTER TABLE `health_exam_result`.`file_receipts`
  ADD COLUMN `medical_folder_alias_id` bigint unsigned DEFAULT NULL
    COMMENT 'scan時の受領フォルダalias ID'
    AFTER `exam_facility_id`,
  ADD KEY `idx_file_receipts_event_alias` (`event_id`, `medical_folder_alias_id`);
