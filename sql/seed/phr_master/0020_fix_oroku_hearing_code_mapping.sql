-- Convert Oroku's facility-local hearing result codes to MHLW OID 2002 codes.
--
-- Oroku CSV: 1=normal, 2=abnormal
-- MHLW OID 1.2.392.200119.6.2002: 1=abnormal finding, 2=no abnormal finding

USE `phr_master`;

START TRANSACTION;

SELECT cfv.`csv_format_version_id`
  INTO @oroku_csv_format_version_id
FROM `phr_master`.`csv_format_versions` cfv
JOIN `phr_master`.`exam_facilities` ef
  ON ef.`exam_facility_id` = cfv.`exam_facility_id`
WHERE ef.`medical_institution_code` = '4710114044'
  AND cfv.`mapping_version` = 'OROKU_2026_05_JOINED_PATTERN_C_V1'
LIMIT 1;

-- These rules passed Oroku's local 1/2 values directly to the MHLW code set.
UPDATE `phr_master`.`csv_exam_result_mapping_conditions` c
JOIN `phr_master`.`csv_exam_result_mapping_rules` r
  ON r.`csv_exam_result_mapping_rule_id` = c.`csv_exam_result_mapping_rule_id`
SET c.`is_active` = 0,
    c.`note` = 'disabled: replaced by Oroku local-code conversion rules',
    c.`updated_at` = CURRENT_TIMESTAMP(3)
WHERE r.`csv_format_version_id` = @oroku_csv_format_version_id
  AND r.`rule_key` IN (
    'oroku.exam.hearing_right_1000',
    'oroku.exam.hearing_left_1000',
    'oroku.exam.hearing_right_4000',
    'oroku.exam.hearing_left_4000'
  );

UPDATE `phr_master`.`csv_exam_result_mapping_rules`
SET `is_active` = 0,
    `note` = 'disabled: Oroku local 1/2 hearing codes require conversion to MHLW OID 2002',
    `updated_at` = CURRENT_TIMESTAMP(3)
WHERE `csv_format_version_id` = @oroku_csv_format_version_id
  AND `rule_key` IN (
    'oroku.exam.hearing_right_1000',
    'oroku.exam.hearing_left_1000',
    'oroku.exam.hearing_right_4000',
    'oroku.exam.hearing_left_4000'
  );

DROP TEMPORARY TABLE IF EXISTS `tmp_oroku_hearing_mapping_seed`;
CREATE TEMPORARY TABLE `tmp_oroku_hearing_mapping_seed` (
  `rule_key` varchar(190) NOT NULL,
  `target_namecode` char(17) NOT NULL,
  `header_name` varchar(255) NOT NULL,
  `source_value` varchar(16) NOT NULL,
  `fixed_value` varchar(16) NOT NULL,
  `priority` int NOT NULL,
  `note` varchar(255) NOT NULL
) ENGINE=Memory DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs;

INSERT INTO `tmp_oroku_hearing_mapping_seed` (
  `rule_key`, `target_namecode`, `header_name`, `source_value`,
  `fixed_value`, `priority`, `note`
) VALUES
  ('oroku.exam.hearing_right_1000_normal', '9D100163100000011', 'オージオ（右）1000Ｈｚ', '1', '2', 1350, 'Oroku 1=normal -> MHLW 2=no abnormal finding'),
  ('oroku.exam.hearing_right_1000_abnormal', '9D100163100000011', 'オージオ（右）1000Ｈｚ', '2', '1', 1351, 'Oroku 2=abnormal -> MHLW 1=abnormal finding'),
  ('oroku.exam.hearing_left_1000_normal', '9D100163500000011', 'オージオ（左）1000Ｈｚ', '1', '2', 1360, 'Oroku 1=normal -> MHLW 2=no abnormal finding'),
  ('oroku.exam.hearing_left_1000_abnormal', '9D100163500000011', 'オージオ（左）1000Ｈｚ', '2', '1', 1361, 'Oroku 2=abnormal -> MHLW 1=abnormal finding'),
  ('oroku.exam.hearing_right_4000_normal', '9D100163200000011', 'オージオ（右）4000Ｈｚ', '1', '2', 1370, 'Oroku 1=normal -> MHLW 2=no abnormal finding'),
  ('oroku.exam.hearing_right_4000_abnormal', '9D100163200000011', 'オージオ（右）4000Ｈｚ', '2', '1', 1371, 'Oroku 2=abnormal -> MHLW 1=abnormal finding'),
  ('oroku.exam.hearing_left_4000_normal', '9D100163600000011', 'オージオ（左）4000Ｈｚ', '1', '2', 1380, 'Oroku 1=normal -> MHLW 2=no abnormal finding'),
  ('oroku.exam.hearing_left_4000_abnormal', '9D100163600000011', 'オージオ（左）4000Ｈｚ', '2', '1', 1381, 'Oroku 2=abnormal -> MHLW 1=abnormal finding');

INSERT INTO `phr_master`.`csv_exam_result_mapping_rules` (
  `csv_format_version_id`, `rule_key`, `target_kind`, `target_resolution_type`,
  `selection_mode`, `target_namecode`, `method_structure_type`,
  `value_source_type`, `fixed_value`, `raw_value_type`, `is_required`,
  `priority`, `is_active`, `note`
)
SELECT
  @oroku_csv_format_version_id,
  s.`rule_key`,
  'EXAM_ITEM_VALUE',
  'SINGLE_NAMECODE',
  'DIRECT',
  s.`target_namecode`,
  'SINGLE_COLUMN',
  'FIXED',
  s.`fixed_value`,
  'CD',
  0,
  s.`priority`,
  1,
  CONCAT('seed:', s.`rule_key`, ':', s.`note`)
FROM `tmp_oroku_hearing_mapping_seed` s
WHERE @oroku_csv_format_version_id IS NOT NULL
ON DUPLICATE KEY UPDATE
  `target_kind` = VALUES(`target_kind`),
  `target_resolution_type` = VALUES(`target_resolution_type`),
  `selection_mode` = VALUES(`selection_mode`),
  `target_namecode` = VALUES(`target_namecode`),
  `method_structure_type` = VALUES(`method_structure_type`),
  `value_source_type` = VALUES(`value_source_type`),
  `fixed_value` = VALUES(`fixed_value`),
  `raw_value_type` = VALUES(`raw_value_type`),
  `is_required` = VALUES(`is_required`),
  `priority` = VALUES(`priority`),
  `is_active` = VALUES(`is_active`),
  `note` = VALUES(`note`),
  `updated_at` = CURRENT_TIMESTAMP(3);

DELETE c
FROM `phr_master`.`csv_exam_result_mapping_conditions` c
JOIN `phr_master`.`csv_exam_result_mapping_rules` r
  ON r.`csv_exam_result_mapping_rule_id` = c.`csv_exam_result_mapping_rule_id`
JOIN `tmp_oroku_hearing_mapping_seed` s
  ON s.`rule_key` = r.`rule_key`
WHERE r.`csv_format_version_id` = @oroku_csv_format_version_id;

INSERT INTO `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_rule_id`, `condition_group_no`, `condition_type`,
  `locator_type`, `header_name`, `header_occurrence`, `column_no`,
  `operator`, `expected_value`, `expected_value_normalized`, `source_role`,
  `priority`, `is_active`, `note`
)
SELECT
  r.`csv_exam_result_mapping_rule_id`,
  1,
  x.`condition_type`,
  'HEADER_NAME',
  s.`header_name`,
  1,
  NULL,
  x.`operator`,
  CASE WHEN x.`condition_type` = 'CELL_VALUE' THEN s.`source_value` ELSE NULL END,
  CASE WHEN x.`condition_type` = 'CELL_VALUE' THEN s.`source_value` ELSE NULL END,
  x.`source_role`,
  x.`priority`,
  1,
  CONCAT('seed condition:', s.`rule_key`)
FROM `tmp_oroku_hearing_mapping_seed` s
JOIN `phr_master`.`csv_exam_result_mapping_rules` r
  ON r.`csv_format_version_id` = @oroku_csv_format_version_id
 AND r.`rule_key` = s.`rule_key`
CROSS JOIN (
  SELECT 'HEADER_MATCH' AS `condition_type`, 'PRESENT' AS `operator`, 'VALUE' AS `source_role`, 100 AS `priority`
  UNION ALL
  SELECT 'CELL_VALUE', 'EQUALS', 'QUALIFIER', 110
) x;

DROP TEMPORARY TABLE IF EXISTS `tmp_oroku_hearing_mapping_seed`;

-- Questionnaire 22 is the phase-4 specific health guidance history item.
-- Its Japanese yes/no values have the same meaning as MHLW OID 2003, so the
-- shared normalizer converts はい/いいえ to 1/2 without a facility override.
INSERT INTO `phr_master`.`csv_exam_result_mapping_rules` (
  `csv_format_version_id`, `rule_key`, `target_kind`, `target_resolution_type`,
  `selection_mode`, `target_namecode`, `method_structure_type`,
  `value_source_type`, `raw_value_type`, `is_required`, `priority`, `is_active`, `note`
)
SELECT
  @oroku_csv_format_version_id,
  'oroku.exam.guidance_history',
  'EXAM_ITEM_VALUE',
  'SINGLE_NAMECODE',
  'DIRECT',
  '9N808000000000011',
  'SINGLE_COLUMN',
  'SOURCE',
  'CD',
  0,
  1600,
  1,
  'seed:oroku.exam.guidance_history:22生活改善指導利用 -> 特定保健指導の受診歴'
WHERE @oroku_csv_format_version_id IS NOT NULL
ON DUPLICATE KEY UPDATE
  `target_kind` = VALUES(`target_kind`),
  `target_resolution_type` = VALUES(`target_resolution_type`),
  `selection_mode` = VALUES(`selection_mode`),
  `target_namecode` = VALUES(`target_namecode`),
  `method_structure_type` = VALUES(`method_structure_type`),
  `value_source_type` = VALUES(`value_source_type`),
  `raw_value_type` = VALUES(`raw_value_type`),
  `is_required` = VALUES(`is_required`),
  `priority` = VALUES(`priority`),
  `is_active` = VALUES(`is_active`),
  `note` = VALUES(`note`),
  `updated_at` = CURRENT_TIMESTAMP(3);

DELETE c
FROM `phr_master`.`csv_exam_result_mapping_conditions` c
JOIN `phr_master`.`csv_exam_result_mapping_rules` r
  ON r.`csv_exam_result_mapping_rule_id` = c.`csv_exam_result_mapping_rule_id`
WHERE r.`csv_format_version_id` = @oroku_csv_format_version_id
  AND r.`rule_key` = 'oroku.exam.guidance_history';

INSERT INTO `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_rule_id`, `condition_group_no`, `condition_type`,
  `locator_type`, `header_name`, `header_occurrence`, `column_no`,
  `operator`, `source_role`, `priority`, `is_active`, `note`
)
SELECT
  r.`csv_exam_result_mapping_rule_id`,
  1,
  'HEADER_MATCH',
  'HEADER_NAME',
  '22生活改善指導利用',
  1,
  NULL,
  'PRESENT',
  'VALUE',
  100,
  1,
  'seed condition:oroku.exam.guidance_history'
FROM `phr_master`.`csv_exam_result_mapping_rules` r
WHERE r.`csv_format_version_id` = @oroku_csv_format_version_id
  AND r.`rule_key` = 'oroku.exam.guidance_history';

COMMIT;
