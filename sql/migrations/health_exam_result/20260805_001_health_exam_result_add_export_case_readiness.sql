ALTER TABLE `health_exam_result`.`exam_export_cases`
  ADD COLUMN `export_readiness_status` varchar(32) NOT NULL DEFAULT 'PENDING'
    COMMENT '運用向けの総合出力状態'
    AFTER `manual_export_approved_by`,
  ADD COLUMN `export_readiness_reason` text DEFAULT NULL
    COMMENT '運用向けの総合出力状態理由'
    AFTER `export_readiness_status`,
  ADD KEY `idx_exam_export_cases_readiness` (`export_readiness_status`);
