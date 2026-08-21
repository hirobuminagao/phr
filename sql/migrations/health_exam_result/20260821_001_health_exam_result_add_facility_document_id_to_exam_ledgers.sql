ALTER TABLE `health_exam_result`.`exam_ledgers`
  ADD COLUMN `facility_document_id` varchar(190) DEFAULT NULL COMMENT '健診機関側の問い合わせ用ドキュメントID'
    AFTER `document_id`,
  ADD KEY `idx_exam_ledgers_facility_document_id` (`facility_document_id`);
