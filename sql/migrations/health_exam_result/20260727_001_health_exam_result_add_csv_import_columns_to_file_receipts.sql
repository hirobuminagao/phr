ALTER TABLE `health_exam_result`.`file_receipts`
  ADD COLUMN `exam_facility_id` bigint unsigned DEFAULT NULL AFTER `facility_name`,
  ADD COLUMN `actual_header_sha256` char(64) DEFAULT NULL AFTER `exam_facility_id`,
  ADD COLUMN `matched_csv_format_version_id` bigint unsigned DEFAULT NULL AFTER `actual_header_sha256`,
  ADD COLUMN `import_resume_approved` tinyint(1) NOT NULL DEFAULT 0 AFTER `summary_message`,
  ADD COLUMN `import_resume_approved_at` datetime(3) DEFAULT NULL AFTER `import_resume_approved`,
  ADD COLUMN `import_resume_approved_by` varchar(190) DEFAULT NULL AFTER `import_resume_approved_at`,
  ADD COLUMN `import_resume_approved_reason` text AFTER `import_resume_approved_by`,
  ADD COLUMN `import_resume_scope` varchar(64) DEFAULT NULL AFTER `import_resume_approved_reason`,
  ADD KEY `idx_file_receipts_exam_facility` (`exam_facility_id`),
  ADD KEY `idx_file_receipts_actual_header` (`actual_header_sha256`),
  ADD KEY `idx_file_receipts_csv_format_version` (`matched_csv_format_version_id`),
  ADD KEY `idx_file_receipts_import_resume` (`import_resume_approved`);
