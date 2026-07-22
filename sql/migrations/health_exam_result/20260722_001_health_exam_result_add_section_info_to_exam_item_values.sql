ALTER TABLE `health_exam_result`.`exam_item_values`
  ADD COLUMN `section_code` varchar(16) DEFAULT NULL COMMENT '親CDA sectionコード'
    AFTER `namecode`,
  ADD COLUMN `section_code_system` varchar(64) DEFAULT NULL COMMENT '親CDA sectionコード体系OID'
    AFTER `section_code`,
  ADD COLUMN `section_name` varchar(255) DEFAULT NULL COMMENT '親CDA section名称'
    AFTER `section_code_system`;
