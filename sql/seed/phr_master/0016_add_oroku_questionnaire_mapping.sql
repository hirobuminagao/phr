-- Seed: add Oroku Hospital specific-health questionnaire CSV mappings.
-- Source: docs/spec/exam_result_csv_import/25_oroku_hospital_joined_pattern_c_review.md
-- Note: column/header 120 is intentionally excluded for initial XML output.

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

DROP TEMPORARY TABLE IF EXISTS `tmp_oroku_questionnaire_mapping_seed`;
CREATE TEMPORARY TABLE `tmp_oroku_questionnaire_mapping_seed` (
  `rule_key` varchar(190) NOT NULL,
  `target_namecode` char(17) NOT NULL,
  `header_name` varchar(255) NOT NULL,
  `priority` int NOT NULL,
  `note` varchar(255)
) ENGINE=Memory DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs;

INSERT INTO `tmp_oroku_questionnaire_mapping_seed`
  (`rule_key`, `target_namecode`, `header_name`, `priority`, `note`)
VALUES
  ('oroku.exam.questionnaire_medication_bp', '9N701000000000011', '1－1血圧を下げる薬', 1390, 'questionnaire: medication blood pressure'),
  ('oroku.exam.questionnaire_medication_glucose', '9N706000000000011', '1－2インスリン注射または血糖を下げる薬', 1400, 'questionnaire: medication glucose'),
  ('oroku.exam.questionnaire_medication_lipid', '9N711000000000011', '1－3コレステロールを下げる薬', 1410, 'questionnaire: medication lipid'),
  ('oroku.exam.questionnaire_stroke_history', '9N716000000000011', '4脳卒中歴', 1420, 'questionnaire: stroke history'),
  ('oroku.exam.questionnaire_heart_disease_history', '9N721000000000011', '5心臓病歴', 1430, 'questionnaire: heart disease history'),
  ('oroku.exam.questionnaire_kidney_failure', '9N726000000000011', '6腎不全', 1440, 'questionnaire: chronic kidney failure'),
  ('oroku.exam.questionnaire_anemia', '9N731000000000011', '7貧血', 1450, 'questionnaire: anemia'),
  ('oroku.exam.questionnaire_smoking', '9N736000000000011', '8習慣的な喫煙', 1460, 'questionnaire: smoking'),
  ('oroku.exam.questionnaire_weight_gain', '9N741000000000011', '9１０kg増', 1470, 'questionnaire: weight gain since age 20'),
  ('oroku.exam.questionnaire_exercise_habit', '9N746000000000011', '10運動習慣', 1480, 'questionnaire: exercise habit'),
  ('oroku.exam.questionnaire_daily_walking', '9N751000000000011', '11日常的歩行', 1490, 'questionnaire: daily walking'),
  ('oroku.exam.questionnaire_walking_speed', '9N756000000000011', '12歩行速度', 1500, 'questionnaire: walking speed'),
  ('oroku.exam.questionnaire_chewing', '9N872000000000011', '13食事をかむか', 1510, 'questionnaire: chewing condition'),
  ('oroku.exam.questionnaire_eating_speed', '9N766000000000011', '14食べる速さ', 1520, 'questionnaire: eating speed'),
  ('oroku.exam.questionnaire_late_dinner', '9N771000000000011', '15就寝前', 1530, 'questionnaire: late dinner'),
  ('oroku.exam.questionnaire_snacking', '9N782000000000011', '16間食の摂取', 1540, 'questionnaire: snacking'),
  ('oroku.exam.questionnaire_skip_breakfast', '9N781000000000011', '17朝食抜く', 1550, 'questionnaire: skipping breakfast'),
  ('oroku.exam.questionnaire_drinking_frequency', '9N786000000000011', '18飲酒', 1560, 'questionnaire: drinking frequency'),
  ('oroku.exam.questionnaire_drinking_amount', '9N791000000000011', '19飲酒の量', 1570, 'questionnaire: drinking amount'),
  ('oroku.exam.questionnaire_sleep', '9N796000000000011', '20睡眠', 1580, 'questionnaire: sleep/rest'),
  ('oroku.exam.questionnaire_lifestyle_improvement', '9N801000000000011', '21生活改善', 1590, 'questionnaire: lifestyle improvement');

INSERT INTO `phr_master`.`csv_exam_result_mapping_rules` (
  `csv_format_version_id`, `rule_key`, `target_kind`, `target_resolution_type`, `selection_mode`,
  `target_namecode`, `method_structure_type`, `value_source_type`, `raw_value_type`,
  `is_required`, `priority`, `is_active`, `note`
)
SELECT
  @oroku_csv_format_version_id,
  s.`rule_key`,
  'EXAM_ITEM_VALUE',
  'SINGLE_NAMECODE',
  'DIRECT',
  s.`target_namecode`,
  'SINGLE_COLUMN',
  'SOURCE',
  'CD',
  0,
  s.`priority`,
  1,
  CONCAT('seed:', s.`rule_key`, ':', s.`note`)
FROM `tmp_oroku_questionnaire_mapping_seed` s
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
JOIN `tmp_oroku_questionnaire_mapping_seed` s
  ON s.`rule_key` = r.`rule_key`
WHERE r.`csv_format_version_id` = @oroku_csv_format_version_id;

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
  s.`header_name`,
  1,
  NULL,
  'PRESENT',
  'VALUE',
  100,
  1,
  CONCAT('seed condition:', s.`rule_key`)
FROM `tmp_oroku_questionnaire_mapping_seed` s
JOIN `phr_master`.`csv_exam_result_mapping_rules` r
  ON r.`csv_format_version_id` = @oroku_csv_format_version_id
 AND r.`rule_key` = s.`rule_key`;

DROP TEMPORARY TABLE IF EXISTS `tmp_oroku_questionnaire_mapping_seed`;

COMMIT;
