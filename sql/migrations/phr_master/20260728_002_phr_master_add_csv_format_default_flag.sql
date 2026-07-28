ALTER TABLE `phr_master`.`csv_format_versions`
  ADD COLUMN `is_default_for_facility` tinyint(1) NOT NULL DEFAULT 0 AFTER `allow_column_no_rules`,
  ADD KEY `idx_csv_format_versions_facility_default` (`exam_facility_id`, `is_default_for_facility`, `is_active`);

