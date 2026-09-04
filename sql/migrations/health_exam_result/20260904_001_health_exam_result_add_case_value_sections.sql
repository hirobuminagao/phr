-- case値で受領時のCDA sectionを保持し、再出力時に元sectionへ戻せるようにする。

ALTER TABLE `health_exam_result`.`exam_export_case_values`
  ADD COLUMN `section_code` varchar(16) DEFAULT NULL
    COMMENT '採用元の親CDA sectionコード' AFTER `occurrence_no`,
  ADD COLUMN `section_code_system` varchar(64) DEFAULT NULL
    COMMENT '採用元の親CDA sectionコード体系OID' AFTER `section_code`,
  ADD COLUMN `section_name` varchar(255) DEFAULT NULL
    COMMENT '採用元の親CDA section表示名' AFTER `section_code_system`;

UPDATE `health_exam_result`.`exam_export_case_values` AS cv
INNER JOIN `health_exam_result`.`exam_item_values` AS source_value
  ON source_value.`id` = cv.`source_exam_item_value_id`
SET
  cv.`section_code` = source_value.`section_code`,
  cv.`section_code_system` = source_value.`section_code_system`,
  cv.`section_name` = source_value.`section_name`
WHERE cv.`section_code` IS NULL;
