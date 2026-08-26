-- Maruyama Clinic CSV exam result mapping seed.
-- Source sample: /Users/hiro/Downloads/0110119070_円山クリニック_sample6月結果.csv
-- Verified on: 2026-08-25

START TRANSACTION;

SELECT `exam_facility_id`
  INTO @maruyama_exam_facility_id
FROM `phr_master`.`exam_facilities`
WHERE `medical_institution_code` = '0110119070'
LIMIT 1;

INSERT INTO `phr_master`.`csv_format_versions` (
  `exam_facility_id`,
  `mapping_version`,
  `file_type`,
  `format_name`,
  `has_header`,
  `header_mode`,
  `header_structure_type`,
  `header_context_rule`,
  `active_header_row_no`,
  `data_start_row_no`,
  `header_sha256`,
  `header_hash_status`,
  `header_mismatch_policy`,
  `allow_column_no_rules`,
  `is_default_for_facility`,
  `duplicate_row_policy`,
  `missing_basic_info_policy`,
  `character_encoding`,
  `encoding_fallback_policy`,
  `delimiter`,
  `quote_char`,
  `valid_from`,
  `note`,
  `is_active`
) VALUES (
  @maruyama_exam_facility_id,
  'MARUYAMA_2026_06_V1',
  'CSV',
  '円山クリニック 2026-06 sample',
  1,
  'SINGLE',
  'SIMPLE_HEADER',
  'NONE',
  1,
  2,
  'b33af8fbc9906d606206efb158ec44aeb51cd07dc4d9506baaac162481fb05ac',
  'VERIFIED',
  'ALLOW_AFTER_CONFIRM',
  0,
  1,
  'SKIP_CHECKED_OK',
  'IMPORT_AND_CHECK_LATER',
  'CP932',
  'ALLOW_COMMON_ENCODINGS',
  ',',
  '"',
  '2026-06-01',
  'seed: maruyama clinic sample. 1075-column single-row header. Initial mapping covers basic ledger fields and statutory/specific-health high priority items.',
  1
)
ON DUPLICATE KEY UPDATE
  `file_type` = VALUES(`file_type`),
  `format_name` = VALUES(`format_name`),
  `has_header` = VALUES(`has_header`),
  `header_mode` = VALUES(`header_mode`),
  `header_structure_type` = VALUES(`header_structure_type`),
  `header_context_rule` = VALUES(`header_context_rule`),
  `active_header_row_no` = VALUES(`active_header_row_no`),
  `data_start_row_no` = VALUES(`data_start_row_no`),
  `header_sha256` = VALUES(`header_sha256`),
  `header_hash_status` = VALUES(`header_hash_status`),
  `header_mismatch_policy` = VALUES(`header_mismatch_policy`),
  `allow_column_no_rules` = VALUES(`allow_column_no_rules`),
  `is_default_for_facility` = VALUES(`is_default_for_facility`),
  `duplicate_row_policy` = VALUES(`duplicate_row_policy`),
  `missing_basic_info_policy` = VALUES(`missing_basic_info_policy`),
  `character_encoding` = VALUES(`character_encoding`),
  `encoding_fallback_policy` = VALUES(`encoding_fallback_policy`),
  `delimiter` = VALUES(`delimiter`),
  `quote_char` = VALUES(`quote_char`),
  `valid_from` = VALUES(`valid_from`),
  `note` = VALUES(`note`),
  `is_active` = VALUES(`is_active`),
  `updated_at` = CURRENT_TIMESTAMP(3);

SELECT `csv_format_version_id`
  INTO @maruyama_csv_format_version_id
FROM `phr_master`.`csv_format_versions`
WHERE `exam_facility_id` = @maruyama_exam_facility_id
  AND `mapping_version` = 'MARUYAMA_2026_06_V1'
LIMIT 1;

CREATE TEMPORARY TABLE IF NOT EXISTS `tmp_maruyama_csv_exam_mapping_seed` (
  `seed_key` varchar(190) NOT NULL,
  `target_kind` varchar(64) NOT NULL,
  `target_field` varchar(64) DEFAULT NULL,
  `target_namecode` char(17) DEFAULT NULL,
  `header_name` varchar(255) NOT NULL,
  `raw_value_type` varchar(32) DEFAULT NULL,
  `raw_unit` varchar(64) DEFAULT NULL,
  `is_required` tinyint(1) NOT NULL DEFAULT 0,
  `priority` int NOT NULL DEFAULT 1000,
  `note` text,
  PRIMARY KEY (`seed_key`)
);

DELETE FROM `tmp_maruyama_csv_exam_mapping_seed`;

INSERT INTO `tmp_maruyama_csv_exam_mapping_seed` (
  `seed_key`, `target_kind`, `target_field`, `target_namecode`,
  `header_name`, `raw_value_type`, `raw_unit`, `is_required`, `priority`, `note`
) VALUES
  ('maruyama.basic.employee_code_ignore', 'IGNORE', NULL, NULL, '社員番号', NULL, NULL, 0, 10, 'employee code is local admin column; not imported'),
  ('maruyama.basic.insurance_symbol_raw', 'LEDGER_FIELD', 'insurance_symbol_raw', NULL, '保険証記号', NULL, NULL, 0, 20, 'basic: insurance symbol'),
  ('maruyama.basic.insurance_number_raw', 'LEDGER_FIELD', 'insurance_number_raw', NULL, '保険証番号', NULL, NULL, 0, 30, 'basic: insurance number'),
  ('maruyama.basic.exam_date', 'LEDGER_FIELD', 'exam_date', NULL, '受診日（西暦）', NULL, NULL, 1, 40, 'basic: exam date'),
  ('maruyama.basic.name_full_raw', 'LEDGER_FIELD', 'name_full_raw', NULL, '漢字氏名', NULL, NULL, 0, 50, 'basic: raw full name'),
  ('maruyama.basic.name_kana_raw', 'LEDGER_FIELD', 'name_kana_raw', NULL, 'カナ氏名', NULL, NULL, 0, 60, 'basic: raw kana name'),
  ('maruyama.basic.gender_raw', 'LEDGER_FIELD', 'gender_raw', NULL, '性別', NULL, NULL, 0, 70, 'basic: raw gender'),
  ('maruyama.basic.birthdate', 'LEDGER_FIELD', 'birthdate', NULL, '生年月日', NULL, NULL, 1, 80, 'basic: birthdate'),
  ('maruyama.basic.insurer_number', 'LEDGER_FIELD', 'insurer_number', NULL, '保険者番号', NULL, NULL, 1, 90, 'basic: insurer number'),
  ('maruyama.exam.height', 'EXAM_ITEM_VALUE', NULL, '9N001000000000001', '身長', 'NM', 'cm', 1, 1000, 'height'),
  ('maruyama.exam.weight', 'EXAM_ITEM_VALUE', NULL, '9N006000000000001', '体重', 'NM', 'kg', 1, 1010, 'weight'),
  ('maruyama.exam.bmi', 'EXAM_ITEM_VALUE', NULL, '9N011000000000001', 'ＢＭＩ', 'NM', NULL, 1, 1020, 'BMI'),
  ('maruyama.exam.obesity_degree', 'EXAM_ITEM_VALUE', NULL, '9N026000000000002', '肥満度', 'NM', '%', 0, 1030, 'obesity degree'),
  ('maruyama.exam.waist', 'EXAM_ITEM_VALUE', NULL, '9N016160100000001', '腹囲', 'NM', 'cm', 1, 1040, 'waist circumference'),
  ('maruyama.exam.sbp_first', 'EXAM_ITEM_VALUE', NULL, '9A751000000000001', '血圧１高', 'NM', 'mmHg', 0, 1050, 'first systolic blood pressure'),
  ('maruyama.exam.dbp_first', 'EXAM_ITEM_VALUE', NULL, '9A761000000000001', '血圧１低', 'NM', 'mmHg', 0, 1060, 'first diastolic blood pressure'),
  ('maruyama.exam.sbp_second', 'EXAM_ITEM_VALUE', NULL, '9A752000000000001', '血圧２高', 'NM', 'mmHg', 0, 1070, 'second systolic blood pressure'),
  ('maruyama.exam.dbp_second', 'EXAM_ITEM_VALUE', NULL, '9A762000000000001', '血圧２低', 'NM', 'mmHg', 0, 1080, 'second diastolic blood pressure'),
  ('maruyama.exam.tg_fasting', 'EXAM_ITEM_VALUE', NULL, '3F015000002327101', '中性脂肪', 'NM', 'mg/dL', 1, 1090, 'triglyceride; first seed maps to fasting because no fasting/random discriminator column was confirmed'),
  ('maruyama.exam.hdl', 'EXAM_ITEM_VALUE', NULL, '3F070000002327101', 'HDLｺﾚｽﾃﾛｰﾙ', 'NM', 'mg/dL', 1, 1100, 'HDL cholesterol'),
  ('maruyama.exam.ldl', 'EXAM_ITEM_VALUE', NULL, '3F077000002327101', 'LDLCHO', 'NM', 'mg/dL', 1, 1110, 'LDL cholesterol'),
  ('maruyama.exam.glucose_fasting', 'EXAM_ITEM_VALUE', NULL, '3D010000001927201', '血糖', 'NM', 'mg/dL', 1, 1120, 'glucose; first seed maps to fasting because no fasting/random discriminator column was confirmed'),
  ('maruyama.exam.hba1c', 'EXAM_ITEM_VALUE', NULL, '3D046000001920402', 'HbA1c', 'NM', '%', 1, 1130, 'HbA1c'),
  ('maruyama.exam.ast', 'EXAM_ITEM_VALUE', NULL, '3B035000002327201', 'GOT', 'NM', 'U/L', 1, 1140, 'AST/GOT'),
  ('maruyama.exam.alt', 'EXAM_ITEM_VALUE', NULL, '3B045000002327201', 'GPT', 'NM', 'U/L', 1, 1150, 'ALT/GPT'),
  ('maruyama.exam.ggt', 'EXAM_ITEM_VALUE', NULL, '3B090000002327101', 'γ-GTP', 'NM', 'U/L', 1, 1160, 'gamma-GTP'),
  ('maruyama.exam.urine_protein', 'EXAM_ITEM_VALUE', NULL, '1A010000000190111', '尿蛋白', 'CD', NULL, 1, 1170, 'urine protein'),
  ('maruyama.exam.urine_sugar', 'EXAM_ITEM_VALUE', NULL, '1A020000000190111', '糖', 'CD', NULL, 1, 1180, 'urine sugar'),
  ('maruyama.exam.rbc', 'EXAM_ITEM_VALUE', NULL, '2A020000001930101', '赤血球', 'NM', '10*4/uL', 0, 1190, 'red blood cell count'),
  ('maruyama.exam.hemoglobin', 'EXAM_ITEM_VALUE', NULL, '2A030000001930101', '血色素', 'NM', 'g/dL', 0, 1200, 'hemoglobin'),
  ('maruyama.exam.hematocrit', 'EXAM_ITEM_VALUE', NULL, '2A040000001930102', 'ﾍﾏﾄｸﾘｯﾄ', 'NM', '%', 0, 1210, 'hematocrit'),
  ('maruyama.exam.ecg_text', 'EXAM_ITEM_VALUE', NULL, '9A110160800000049', '安静心電図１', 'ST', NULL, 0, 1220, 'ECG finding text 1'),
  ('maruyama.exam.chest_xray_text', 'EXAM_ITEM_VALUE', NULL, '9N206160800000049', '胸部Ｘ線１', 'ST', NULL, 0, 1230, 'chest X-ray finding text 1'),
  ('maruyama.exam.subjective_symptoms_text', 'EXAM_ITEM_VALUE', NULL, '9N061160800000049', '自覚症状（その他）', 'ST', NULL, 0, 1240, 'subjective symptoms free text'),
  ('maruyama.exam.objective_symptoms_text', 'EXAM_ITEM_VALUE', NULL, '9N066160800000049', '他覚症状(１)', 'ST', NULL, 0, 1250, 'objective symptoms text 1'),
  ('maruyama.exam.metabolic', 'EXAM_ITEM_VALUE', NULL, '9N501000000000011', 'メタボリック判定', 'CD', NULL, 1, 1260, 'metabolic syndrome judgement'),
  ('maruyama.exam.guidance_level', 'EXAM_ITEM_VALUE', NULL, '9N506000000000011', '保健指導レベル', 'CD', NULL, 1, 1270, 'specific health guidance level'),
  ('maruyama.exam.egfr', 'EXAM_ITEM_VALUE', NULL, '8A065000002391901', 'eGFR', 'NM', 'mL/min/1.73m2', 0, 1280, 'eGFR'),
  ('maruyama.exam.non_hdl', 'EXAM_ITEM_VALUE', NULL, '3F069000002391901', 'non-HDLｺﾚｽﾃﾛｰﾙ', 'NM', 'mg/dL', 0, 1290, 'non-HDL cholesterol'),
  ('maruyama.exam.chewing', 'EXAM_ITEM_VALUE', NULL, '9N821000000000011', '咀嚼', 'CD', NULL, 0, 1300, 'specific health questionnaire chewing'),
  ('maruyama.exam.snacking', 'EXAM_ITEM_VALUE', NULL, '9N816000000000011', '食べ方３（間食）', 'CD', NULL, 0, 1310, 'specific health questionnaire snacking');

INSERT INTO `phr_master`.`csv_exam_result_mapping_rules` (
  `csv_format_version_id`, `rule_key`, `target_kind`, `target_resolution_type`, `selection_mode`,
  `selection_group_code`, `target_namecode`, `target_identity_item_code`, `target_field`,
  `method_structure_type`, `value_source_type`, `raw_value_type`, `raw_unit`,
  `is_required`, `priority`, `is_active`, `note`
)
SELECT
  @maruyama_csv_format_version_id,
  s.`seed_key`,
  s.`target_kind`,
  CASE WHEN s.`target_kind` = 'EXAM_ITEM_VALUE' THEN 'SINGLE_NAMECODE' ELSE 'LEDGER_FIELD' END,
  'DIRECT',
  NULL,
  s.`target_namecode`,
  NULL,
  s.`target_field`,
  'SINGLE_COLUMN',
  'SOURCE',
  s.`raw_value_type`,
  s.`raw_unit`,
  s.`is_required`,
  s.`priority`,
  1,
  CONCAT('seed:', s.`seed_key`, ':', COALESCE(s.`note`, ''))
FROM `tmp_maruyama_csv_exam_mapping_seed` s
ON DUPLICATE KEY UPDATE
  `target_kind` = VALUES(`target_kind`),
  `target_resolution_type` = VALUES(`target_resolution_type`),
  `selection_mode` = VALUES(`selection_mode`),
  `selection_group_code` = VALUES(`selection_group_code`),
  `target_namecode` = VALUES(`target_namecode`),
  `target_identity_item_code` = VALUES(`target_identity_item_code`),
  `target_field` = VALUES(`target_field`),
  `method_structure_type` = VALUES(`method_structure_type`),
  `value_source_type` = VALUES(`value_source_type`),
  `raw_value_type` = VALUES(`raw_value_type`),
  `raw_unit` = VALUES(`raw_unit`),
  `is_required` = VALUES(`is_required`),
  `priority` = VALUES(`priority`),
  `is_active` = VALUES(`is_active`),
  `note` = VALUES(`note`),
  `updated_at` = CURRENT_TIMESTAMP(3);

DELETE c
FROM `phr_master`.`csv_exam_result_mapping_conditions` c
JOIN `phr_master`.`csv_exam_result_mapping_rules` r
  ON r.`csv_exam_result_mapping_rule_id` = c.`csv_exam_result_mapping_rule_id`
WHERE r.`csv_format_version_id` = @maruyama_csv_format_version_id;

INSERT INTO `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_rule_id`, `condition_group_no`, `condition_type`,
  `locator_type`, `header_context`, `header_name`, `header_occurrence`, `column_no`,
  `operator`, `expected_value`, `expected_value_normalized`, `source_role`,
  `priority`, `is_active`, `note`
)
SELECT
  r.`csv_exam_result_mapping_rule_id`,
  1,
  'SOURCE_COLUMN',
  'HEADER_NAME',
  NULL,
  s.`header_name`,
  1,
  NULL,
  NULL,
  NULL,
  NULL,
  'VALUE',
  100,
  1,
  CONCAT('seed:', s.`seed_key`, ': header=', s.`header_name`)
FROM `tmp_maruyama_csv_exam_mapping_seed` s
JOIN `phr_master`.`csv_exam_result_mapping_rules` r
  ON r.`csv_format_version_id` = @maruyama_csv_format_version_id
 AND r.`rule_key` = s.`seed_key`;

UPDATE `phr_master`.`medical_folder_aliases`
SET `csv_format_version_id` = @maruyama_csv_format_version_id,
    `updated_at` = CURRENT_TIMESTAMP(3)
WHERE `exam_facility_id` = @maruyama_exam_facility_id
  AND `is_active` = 1;

DROP TEMPORARY TABLE IF EXISTS `tmp_maruyama_csv_exam_mapping_seed`;

COMMIT;
