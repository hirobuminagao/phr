-- Heartcross appended basic-information column.
-- The production CSV was manually adjusted by appending address
-- at the tail of the two-row header format. The original POSTALCODE rule is
-- retained for postal_code.

SET @heartcross_csv_format_version_id := (
  SELECT cfv.`csv_format_version_id`
  FROM `phr_master`.`csv_format_versions` cfv
  JOIN `phr_master`.`exam_facilities` ef
    ON ef.`exam_facility_id` = cfv.`exam_facility_id`
  WHERE ef.`exam_facility_code` = '4011028133'
    AND cfv.`mapping_version` = 'HEARTCROSS_2026_05_PATTERN_B_V1'
  LIMIT 1
);

INSERT INTO `phr_master`.`csv_exam_result_mapping_rules` (
  `csv_format_version_id`,
  `rule_key`,
  `target_kind`,
  `target_resolution_type`,
  `selection_mode`,
  `target_field`,
  `method_structure_type`,
  `value_source_type`,
  `is_required`,
  `priority`,
  `is_active`,
  `note`
) VALUES
  (
    @heartcross_csv_format_version_id,
    'heartcross.basic.address_appended',
    'LEDGER_FIELD',
    'LEDGER_FIELD',
    'DIRECT',
    'address',
    'SINGLE_COLUMN',
    'SOURCE',
    0,
    100,
    1,
    'seed:heartcross.basic.address_appended:appended tail column 住所'
  )
ON DUPLICATE KEY UPDATE
  `target_kind` = VALUES(`target_kind`),
  `target_resolution_type` = VALUES(`target_resolution_type`),
  `selection_mode` = VALUES(`selection_mode`),
  `target_field` = VALUES(`target_field`),
  `method_structure_type` = VALUES(`method_structure_type`),
  `value_source_type` = VALUES(`value_source_type`),
  `is_required` = VALUES(`is_required`),
  `priority` = VALUES(`priority`),
  `is_active` = VALUES(`is_active`),
  `note` = VALUES(`note`),
  `updated_at` = CURRENT_TIMESTAMP(3);

DELETE c
FROM `phr_master`.`csv_exam_result_mapping_conditions` c
JOIN `phr_master`.`csv_exam_result_mapping_rules` r
  ON r.`csv_exam_result_mapping_rule_id` = c.`csv_exam_result_mapping_rule_id`
WHERE r.`csv_format_version_id` = @heartcross_csv_format_version_id
  AND r.`rule_key` IN (
    'heartcross.basic.address_appended'
  );

INSERT INTO `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_rule_id`,
  `condition_group_no`,
  `condition_type`,
  `locator_type`,
  `header_name`,
  `header_occurrence`,
  `operator`,
  `source_role`,
  `priority`,
  `is_active`,
  `note`
)
SELECT
  r.`csv_exam_result_mapping_rule_id`,
  1,
  'HEADER_MATCH',
  'HEADER_NAME',
  CASE r.`rule_key`
    WHEN 'heartcross.basic.address_appended' THEN '住所'
  END,
  1,
  'PRESENT',
  'VALUE',
  10,
  1,
  CONCAT('seed:', r.`rule_key`, ': appended tail basic-information column')
FROM `phr_master`.`csv_exam_result_mapping_rules` r
WHERE r.`csv_format_version_id` = @heartcross_csv_format_version_id
  AND r.`rule_key` IN (
    'heartcross.basic.address_appended'
  );

SELECT
  r.`rule_key`,
  r.`target_field`,
  r.`priority`,
  c.`header_name`,
  c.`is_active`
FROM `phr_master`.`csv_exam_result_mapping_rules` r
JOIN `phr_master`.`csv_exam_result_mapping_conditions` c
  ON c.`csv_exam_result_mapping_rule_id` = r.`csv_exam_result_mapping_rule_id`
WHERE r.`csv_format_version_id` = @heartcross_csv_format_version_id
  AND r.`rule_key` IN (
    'heartcross.basic.address_appended'
  )
ORDER BY r.`priority`, r.`rule_key`;
