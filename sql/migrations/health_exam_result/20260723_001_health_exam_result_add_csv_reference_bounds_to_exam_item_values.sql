ALTER TABLE `health_exam_result`.`exam_item_values`
  ADD COLUMN `source_reference_lower` text COMMENT '原本由来の基準下限。CSV等で健診機関が提出した値を保持する'
    AFTER `raw_unit`,
  ADD COLUMN `source_reference_upper` text COMMENT '原本由来の基準上限。CSV等で健診機関が提出した値を保持する'
    AFTER `source_reference_lower`;
