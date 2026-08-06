-- Export anonymized/grouped normalize error fixtures from an execution environment.
--
-- Usage:
-- 1. Run this SELECT in the execution environment.
-- 2. Export the result as UTF-8 CSV.
-- 3. Place it under scripts/dev_tools/import_csv/.
-- 4. Import with scripts/dev_tools/import_exam_item_value_normalize_error_fixtures.py.
--
-- This query intentionally excludes person identifiers, ledger ids, file paths,
-- raw rows, and source row numbers. It keeps only grouped value patterns needed
-- for item_master / norm_variants / unit alias maintenance.

SELECT
  eiv.`namecode`,
  COALESCE(eiv.`namecode_display_name`, '') AS `namecode_display_name`,
  eiv.`raw_value`,
  eiv.`raw_value_type`,
  eiv.`raw_unit`,
  eiv.`normalized_unit`,
  em.`display_unit` AS `master_display_unit`,
  em.`ucum_unit` AS `master_ucum_unit`,
  eiv.`code_system`,
  eiv.`normalize_status`,
  eiv.`normalize_reason`,
  eiv.`validation_status`,
  eiv.`validation_reason`,
  COUNT(*) AS `cnt`
FROM `health_exam_result`.`exam_item_values` AS eiv
LEFT JOIN `dev_phr`.`exam_item_master` AS em
  ON em.`namecode` = eiv.`namecode`
WHERE
  COALESCE(eiv.`normalize_status`, '') <> 'OK'
  OR COALESCE(eiv.`validation_status`, '') NOT IN ('', 'VALID')
GROUP BY
  eiv.`namecode`,
  COALESCE(eiv.`namecode_display_name`, ''),
  eiv.`raw_value`,
  eiv.`raw_value_type`,
  eiv.`raw_unit`,
  eiv.`normalized_unit`,
  em.`display_unit`,
  em.`ucum_unit`,
  eiv.`code_system`,
  eiv.`normalize_status`,
  eiv.`normalize_reason`,
  eiv.`validation_status`,
  eiv.`validation_reason`
ORDER BY
  `cnt` DESC,
  eiv.`normalize_reason`,
  eiv.`validation_reason`,
  eiv.`namecode`,
  eiv.`raw_value`;
