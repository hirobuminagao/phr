ALTER TABLE `phr_master`.`csv_exam_result_mapping_rules`
  ADD COLUMN `value_exclude_values` text DEFAULT NULL
    COMMENT 'Newline-separated raw values excluded when multiple VALUE sources are joined.'
    AFTER `value_join_separator`;
