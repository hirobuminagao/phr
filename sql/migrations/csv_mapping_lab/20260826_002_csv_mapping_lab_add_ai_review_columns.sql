ALTER TABLE `csv_mapping_lab`.`analysis_columns`
  ADD COLUMN `ai_review_status` varchar(32) NOT NULL DEFAULT 'NOT_REVIEWED' AFTER `decision_note`,
  ADD COLUMN `ai_review_note` text NULL AFTER `ai_review_status`,
  ADD COLUMN `ai_reviewed_by` varchar(128) NULL AFTER `ai_review_note`,
  ADD COLUMN `ai_reviewed_at` datetime(3) NULL AFTER `ai_reviewed_by`,
  ADD KEY `idx_analysis_columns_ai_review_status` (`ai_review_status`);
