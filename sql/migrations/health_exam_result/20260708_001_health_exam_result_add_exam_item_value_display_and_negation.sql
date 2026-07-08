ALTER TABLE `health_exam_result`.`exam_item_values`
  ADD COLUMN `namecode_display_name` varchar(255) DEFAULT NULL
    AFTER `code_display`,
  ADD COLUMN `negation_ind` tinyint(1) DEFAULT NULL
    AFTER `namecode_display_name`;
