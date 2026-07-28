-- Allow an explicit mapping rule to emit a fixed value or join multiple source
-- columns. This supports common CD finding-presence + ST finding-text pairs
-- without inferring medical meaning from CSV text.
ALTER TABLE `phr_master`.`csv_exam_result_mapping_rules`
  ADD COLUMN `value_source_type` varchar(32) NOT NULL DEFAULT 'SOURCE'
    AFTER `method_structure_type`,
  ADD COLUMN `fixed_value` text
    AFTER `value_source_type`,
  ADD COLUMN `value_join_separator` varchar(32) DEFAULT NULL
    AFTER `fixed_value`;
