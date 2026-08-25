ALTER TABLE `csv_mapping_lab`.`csv_mapping_rules`
  ADD COLUMN `column_no_min` int DEFAULT NULL COMMENT 'この列番以上にだけ適用。NULLなら制限なし' AFTER `condition_type`,
  ADD COLUMN `column_no_max` int DEFAULT NULL COMMENT 'この列番以下にだけ適用。NULLなら制限なし' AFTER `column_no_min`,
  ADD KEY `idx_csv_mapping_rules_column_range` (`column_no_min`, `column_no_max`);
