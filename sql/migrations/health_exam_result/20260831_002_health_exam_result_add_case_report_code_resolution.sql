ALTER TABLE `exam_export_cases`
  ADD COLUMN `report_code_resolution_source` varchar(64) DEFAULT NULL
    COMMENT '報告区分・プログラムコードの確定元'
    AFTER `program_code`,
  ADD COLUMN `report_code_resolution_reason` varchar(512) DEFAULT NULL
    COMMENT '報告区分・プログラムコードの確定理由'
    AFTER `report_code_resolution_source`;
