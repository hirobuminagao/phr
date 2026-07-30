ALTER TABLE `health_exam_result`.`xml_ledger`
  ADD COLUMN `report_category_code` varchar(32) DEFAULT NULL
    COMMENT '元XML ClinicalDocument/codeの報告区分コード'
    AFTER `gender_code`,
  ADD COLUMN `program_type_code` varchar(32) DEFAULT NULL
    COMMENT '元XML serviceEvent/codeの健診実施プログラム種別コード'
    AFTER `report_category_code`;

ALTER TABLE `health_exam_result`.`exam_result_ledger_report`
  ADD COLUMN `report_category_code` varchar(32) DEFAULT NULL
    COMMENT 'XML由来の報告区分コード'
    AFTER `gender_code`,
  ADD COLUMN `program_type_code` varchar(32) DEFAULT NULL
    COMMENT 'XML由来の健診実施プログラム種別コード'
    AFTER `report_category_code`;
