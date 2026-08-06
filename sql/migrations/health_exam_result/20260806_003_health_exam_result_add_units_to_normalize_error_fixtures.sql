ALTER TABLE `health_exam_result`.`exam_item_value_normalize_error_fixtures`
  ADD COLUMN `raw_unit` varchar(64) DEFAULT NULL AFTER `raw_value_type`,
  ADD COLUMN `normalized_unit` varchar(64) DEFAULT NULL AFTER `raw_unit`,
  ADD COLUMN `master_display_unit` varchar(190) DEFAULT NULL AFTER `normalized_unit`,
  ADD COLUMN `master_ucum_unit` varchar(190) DEFAULT NULL AFTER `master_display_unit`;
