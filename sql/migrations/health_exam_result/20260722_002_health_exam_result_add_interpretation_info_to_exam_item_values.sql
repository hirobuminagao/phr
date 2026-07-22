ALTER TABLE `health_exam_result`.`exam_item_values`
  ADD COLUMN `interpretation_code` varchar(16) DEFAULT NULL COMMENT '検査結果解釈コード' AFTER `code_display`,
  ADD COLUMN `interpretation_code_system` varchar(64) DEFAULT NULL COMMENT '検査結果解釈コード体系OID' AFTER `interpretation_code`,
  ADD COLUMN `interpretation_name` varchar(255) DEFAULT NULL COMMENT '検査結果解釈名称' AFTER `interpretation_code_system`;
