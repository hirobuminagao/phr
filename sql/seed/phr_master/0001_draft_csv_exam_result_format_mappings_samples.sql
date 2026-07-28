-- Seed for 02_02_exam_result_csv_import initial sample format mappings.
--
-- Scope:
-- - Run after exam_facilities seed is loaded from the 支払基金 CSV.
-- - Resolve exam_facility_id by exam_facilities.medical_institution_code.
-- - It covers CSV format versions and initial VALUE-focused mappings for:
--   - Hirooka Clinic sample Pattern A
--   - Heartcross Akasaka sample Pattern B
-- - Facility-derived judgement columns are intentionally not mapped in the initial seed.
--
-- Required tables:
-- - phr_master.csv_format_versions
-- - phr_master.csv_exam_result_mapping_rules
-- - phr_master.csv_exam_result_mapping_conditions

USE `phr_master`;

START TRANSACTION;

SELECT `exam_facility_id`
  INTO @hirooka_exam_facility_id
FROM `phr_master`.`exam_facilities`
WHERE `medical_institution_code` = '1310438796'
LIMIT 1;

SELECT `exam_facility_id`
  INTO @heartcross_exam_facility_id
FROM `phr_master`.`exam_facilities`
WHERE `medical_institution_code` = '4011028133'
LIMIT 1;

-- ============================================================
-- Hirooka Clinic / sample 001
-- ============================================================

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
  `duplicate_row_policy`,
  `missing_basic_info_policy`,
  `character_encoding`,
  `delimiter`,
  `quote_char`,
  `note`,
  `is_active`
) VALUES (
  @hirooka_exam_facility_id,
  'HIROOKA_2026_05_PATTERN_A_V1',
  'CSV',
  'ヒロオカクリニック 2026-05 sample Pattern A',
  1,
  'SINGLE',
  'SIMPLE_HEADER',
  'NONE',
  1,
  2,
  '5d03088d9aec595715455bdc35b66ee8fa8c7d9d023d61e14d51de52ce98dfd0',
  'VERIFIED',
  'ALLOW_AFTER_CONFIRM',
  0,
  'SKIP_CHECKED_OK',
  'IMPORT_AND_CHECK_LATER',
  'CP932',
  ',',
  '"',
  'draft seed: hirooka sample. header_snapshot_json should be generated from sample CSV before actual seed.',
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
  `duplicate_row_policy` = VALUES(`duplicate_row_policy`),
  `missing_basic_info_policy` = VALUES(`missing_basic_info_policy`),
  `character_encoding` = VALUES(`character_encoding`),
  `delimiter` = VALUES(`delimiter`),
  `quote_char` = VALUES(`quote_char`),
  `note` = VALUES(`note`),
  `is_active` = VALUES(`is_active`),
  `updated_at` = CURRENT_TIMESTAMP(3);

SELECT `csv_format_version_id`
  INTO @hirooka_csv_format_version_id
FROM `phr_master`.`csv_format_versions`
WHERE `exam_facility_id` = @hirooka_exam_facility_id
  AND `mapping_version` = 'HIROOKA_2026_05_PATTERN_A_V1'
LIMIT 1;

CREATE TEMPORARY TABLE `tmp_csv_exam_mapping_seed` (
  `seed_key` varchar(128) NOT NULL,
  `format_key` varchar(32) NOT NULL,
  `target_kind` varchar(32) NOT NULL,
  `target_field` varchar(64) DEFAULT NULL,
  `target_namecode` char(17) DEFAULT NULL,
  `header_context` varchar(255) DEFAULT NULL,
  `header_name` varchar(255) NOT NULL,
  `header_occurrence` int NOT NULL DEFAULT 1,
  `source_role` varchar(64) NOT NULL DEFAULT 'VALUE',
  `raw_value_type` varchar(32) DEFAULT NULL,
  `raw_unit` varchar(64) DEFAULT NULL,
  `is_required` tinyint(1) NOT NULL DEFAULT 0,
  `priority` int NOT NULL DEFAULT 100,
  `note` text
);

INSERT INTO `tmp_csv_exam_mapping_seed` (
  `seed_key`, `format_key`, `target_kind`, `target_field`, `target_namecode`,
  `header_context`, `header_name`, `header_occurrence`, `source_role`,
  `raw_value_type`, `raw_unit`, `is_required`, `priority`, `note`
) VALUES
-- Hirooka basic information.
('hirooka.basic.exam_date', 'HIROOKA', 'LEDGER_FIELD', 'exam_date', NULL, NULL, '受診日付', 1, 'VALUE', NULL, NULL, 1, 10, 'basic: exam date'),
('hirooka.basic.facility_code', 'HIROOKA', 'LEDGER_FIELD', 'facility_code', NULL, NULL, '健診機関番号', 1, 'VALUE', NULL, NULL, 0, 20, 'basic: source facility code'),
('hirooka.basic.facility_name', 'HIROOKA', 'LEDGER_FIELD', 'facility_name', NULL, NULL, '健診機関名称', 1, 'VALUE', NULL, NULL, 0, 30, 'basic: source facility name'),
('hirooka.basic.name_full_raw', 'HIROOKA', 'LEDGER_FIELD', 'name_full_raw', NULL, NULL, '氏名', 1, 'VALUE', NULL, NULL, 0, 50, 'basic: raw full name'),
('hirooka.basic.name_kana_raw', 'HIROOKA', 'LEDGER_FIELD', 'name_kana_raw', NULL, NULL, 'カナ氏名', 1, 'VALUE', NULL, NULL, 0, 60, 'basic: raw kana name; sample is blank'),
('hirooka.basic.gender_raw', 'HIROOKA', 'LEDGER_FIELD', 'gender_raw', NULL, NULL, '性別', 1, 'VALUE', NULL, NULL, 0, 70, 'basic: raw gender'),
('hirooka.basic.birthdate', 'HIROOKA', 'LEDGER_FIELD', 'birthdate', NULL, NULL, '生年月日', 1, 'VALUE', NULL, NULL, 1, 80, 'basic: birthdate'),
('hirooka.basic.postal_code', 'HIROOKA', 'LEDGER_FIELD', 'postal_code', NULL, NULL, '郵便番号', 1, 'VALUE', NULL, NULL, 0, 90, 'basic: raw postal code'),
('hirooka.basic.address', 'HIROOKA', 'LEDGER_FIELD', 'address', NULL, NULL, '住所', 1, 'VALUE', NULL, NULL, 0, 100, 'basic: raw address; sample is blank'),
('hirooka.basic.insurer_number', 'HIROOKA', 'LEDGER_FIELD', 'insurer_number', NULL, NULL, '保険者番号', 1, 'VALUE', NULL, NULL, 1, 110, 'basic: insurer number'),
('hirooka.basic.insurance_symbol_raw', 'HIROOKA', 'LEDGER_FIELD', 'insurance_symbol_raw', NULL, NULL, '保険記号', 1, 'VALUE', NULL, NULL, 0, 120, 'basic: insurance symbol'),
('hirooka.basic.insurance_number_raw', 'HIROOKA', 'LEDGER_FIELD', 'insurance_number_raw', NULL, NULL, '保険番号', 1, 'VALUE', NULL, NULL, 0, 130, 'basic: insurance number'),
('hirooka.basic.insurance_branch_number_raw', 'HIROOKA', 'LEDGER_FIELD', 'insurance_branch_number_raw', NULL, NULL, '保険枝番', 1, 'VALUE', NULL, NULL, 0, 140, 'basic: insurance branch number'),
('hirooka.basic.person_id_custom', 'HIROOKA', 'LEDGER_FIELD', 'person_id_custom', NULL, NULL, '社員番号', 1, 'VALUE', NULL, NULL, 0, 150, 'basic: employee code'),

-- Hirooka exam item values. Facility-derived judgement columns are not mapped.
('hirooka.exam.height', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N001000000000001', NULL, '身長', 1, 'VALUE', NULL, NULL, 1, 1000, 'height'),
('hirooka.exam.weight', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N006000000000001', NULL, '体重', 1, 'VALUE', NULL, NULL, 1, 1010, 'weight'),
('hirooka.exam.bmi', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N011000000000001', NULL, 'BMI', 1, 'VALUE', NULL, NULL, 1, 1020, 'BMI'),
('hirooka.exam.waist', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N016160100000001', NULL, '腹囲', 1, 'VALUE', NULL, NULL, 1, 1030, 'waist circumference measured'),
('hirooka.exam.sbp_avg', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9A755000000000001', NULL, '平均 収縮期', 1, 'VALUE', NULL, NULL, 1, 1040, 'average systolic blood pressure'),
('hirooka.exam.dbp_avg', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9A765000000000001', NULL, '平均 拡張期', 1, 'VALUE', NULL, NULL, 1, 1050, 'average diastolic blood pressure'),
('hirooka.exam.urine_protein', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '1A010000000190111', NULL, '尿蛋白', 1, 'VALUE', NULL, NULL, 1, 1060, 'urine protein visual method'),
('hirooka.exam.urine_sugar', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '1A020000000190111', NULL, '尿糖', 1, 'VALUE', NULL, NULL, 1, 1070, 'urine sugar visual method'),
('hirooka.exam.rbc', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '2A020000001930101', NULL, '赤血球数', 1, 'VALUE', NULL, NULL, 0, 1080, 'red blood cell count'),
('hirooka.exam.hemoglobin', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '2A030000001930101', NULL, '血色素量', 1, 'VALUE', NULL, NULL, 0, 1090, 'hemoglobin'),
('hirooka.exam.ast', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '3B035000002327201', NULL, 'AST（GOT）', 1, 'VALUE', NULL, NULL, 1, 1100, 'AST JSCC'),
('hirooka.exam.alt', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '3B045000002327201', NULL, 'ALT（GPT）', 1, 'VALUE', NULL, NULL, 1, 1110, 'ALT JSCC'),
('hirooka.exam.ggt', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '3B090000002327101', NULL, 'γ-GTP', 1, 'VALUE', NULL, NULL, 1, 1120, 'gamma-GTP JSCC'),
('hirooka.exam.tg_fasting', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '3F015000002327101', NULL, '空腹時中性脂肪', 1, 'VALUE', NULL, NULL, 1, 1130, 'fasting TG'),
('hirooka.exam.tg_random', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '3F015129902327101', NULL, '随時中性脂肪', 1, 'VALUE', NULL, NULL, 0, 1140, 'random TG; sample blank'),
('hirooka.exam.hdl', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '3F070000002327101', NULL, 'HDLコレステロール', 1, 'VALUE', NULL, NULL, 1, 1150, 'HDL cholesterol'),
('hirooka.exam.ldl', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '3F077000002327101', NULL, 'LDLコレステロール', 1, 'VALUE', NULL, NULL, 1, 1160, 'LDL cholesterol'),
('hirooka.exam.non_hdl', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '3F069000002391901', NULL, 'non-HDLコレステロール', 1, 'VALUE', NULL, NULL, 1, 1170, 'non-HDL cholesterol'),
('hirooka.exam.glucose_fasting', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '3D010000001927201', NULL, '空腹時血糖', 1, 'VALUE', NULL, NULL, 1, 1180, 'fasting glucose'),
('hirooka.exam.glucose_random', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '3D010129901927201', NULL, '随時血糖', 1, 'VALUE', NULL, NULL, 0, 1190, 'random glucose; sample blank'),
('hirooka.exam.hba1c', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '3D046000001920402', NULL, 'HbA1c（NGSP)', 1, 'VALUE', NULL, NULL, 1, 1200, 'HbA1c HPLC'),
('hirooka.exam.creatinine', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '3C015000002327101', NULL, 'クレアチニン', 1, 'VALUE', NULL, NULL, 0, 1210, 'serum creatinine'),
('hirooka.exam.egfr', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '8A065000002391901', NULL, 'eGFR', 1, 'VALUE', NULL, NULL, 0, 1220, 'eGFR'),
('hirooka.exam.ecg_finding_text', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9A110160800000049', NULL, '安静時心電図所見', 1, 'VALUE', NULL, NULL, 0, 1230, 'ECG finding text'),
('hirooka.exam.chest_xray_finding_text', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N206160800000049', NULL, '胸部Ｘ線所見', 1, 'VALUE', NULL, NULL, 0, 1240, 'chest X-ray finding text'),
('hirooka.exam.medical_history_text', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N056160400000049', NULL, '既往歴', 1, 'VALUE', NULL, NULL, 0, 1250, 'medical history text'),
('hirooka.exam.subjective_symptoms_text', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N061160800000049', NULL, '自覚症状', 1, 'VALUE', NULL, NULL, 0, 1260, 'subjective symptoms text'),
('hirooka.exam.objective_symptoms_text', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N066160800000049', NULL, '他覚症状', 1, 'VALUE', NULL, NULL, 0, 1270, 'objective symptoms text'),
('hirooka.exam.work_history', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N051000000000049', NULL, '業務歴', 1, 'VALUE', NULL, NULL, 0, 1280, 'work history text'),
('hirooka.exam.metabolic', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N501000000000011', NULL, 'メタボリックシンドローム判定', 1, 'VALUE', NULL, NULL, 1, 1290, 'standard CD value, not facility ABC judgement'),
('hirooka.exam.guidance_level', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N506000000000011', NULL, '保健指導区分', 1, 'VALUE', NULL, NULL, 1, 1300, 'standard CD value, not facility ABC judgement');

INSERT INTO `phr_master`.`csv_exam_result_mapping_rules` (
  `csv_format_version_id`, `rule_key`, `target_kind`, `target_resolution_type`, `selection_mode`,
  `selection_group_code`, `target_namecode`, `target_identity_item_code`, `target_field`,
  `method_structure_type`, `raw_value_type`, `raw_unit`,
  `is_required`, `priority`, `is_active`, `note`
)
SELECT
  @hirooka_csv_format_version_id,
  `seed_key`,
  `target_kind`,
  CASE WHEN `target_kind` = 'EXAM_ITEM_VALUE' THEN 'SINGLE_NAMECODE' ELSE 'LEDGER_FIELD' END,
  'DIRECT',
  NULL,
  `target_namecode`,
  NULL,
  `target_field`,
  'SINGLE_COLUMN',
  `raw_value_type`,
  `raw_unit`,
  `is_required`,
  `priority`,
  1,
  CONCAT('draft seed:', `seed_key`, ':', COALESCE(`note`, ''))
FROM `tmp_csv_exam_mapping_seed`
WHERE `format_key` = 'HIROOKA'
ON DUPLICATE KEY UPDATE
  `target_kind` = VALUES(`target_kind`),
  `target_resolution_type` = VALUES(`target_resolution_type`),
  `selection_mode` = VALUES(`selection_mode`),
  `selection_group_code` = VALUES(`selection_group_code`),
  `target_namecode` = VALUES(`target_namecode`),
  `target_identity_item_code` = VALUES(`target_identity_item_code`),
  `target_field` = VALUES(`target_field`),
  `method_structure_type` = VALUES(`method_structure_type`),
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
WHERE r.`csv_format_version_id` = @hirooka_csv_format_version_id;

INSERT INTO `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_rule_id`, `condition_group_no`, `condition_type`,
  `locator_type`, `header_context`, `header_name`, `header_occurrence`, `column_no`,
  `operator`, `expected_value`, `expected_value_normalized`, `source_role`,
  `priority`, `is_active`, `note`
)
SELECT
  r.`csv_exam_result_mapping_rule_id`,
  1,
  'HEADER_MATCH',
  'HEADER_NAME',
  s.`header_context`,
  s.`header_name`,
  s.`header_occurrence`,
  NULL,
  'PRESENT',
  NULL,
  NULL,
  s.`source_role`,
  100,
  1,
  CONCAT('draft condition:', s.`seed_key`)
FROM `tmp_csv_exam_mapping_seed` s
JOIN `phr_master`.`csv_exam_result_mapping_rules` r
  ON r.`csv_format_version_id` = @hirooka_csv_format_version_id
 AND r.`note` LIKE CONCAT('draft seed:', s.`seed_key`, ':%')
WHERE s.`format_key` = 'HIROOKA';

-- ============================================================
-- Heartcross Akasaka / sample 001
-- ============================================================

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
  `duplicate_row_policy`,
  `missing_basic_info_policy`,
  `character_encoding`,
  `delimiter`,
  `quote_char`,
  `note`,
  `is_active`
) VALUES (
  @heartcross_exam_facility_id,
  'HEARTCROSS_2026_05_PATTERN_B_V1',
  'CSV',
  'ハートクロス健診プラザ赤坂駅前 2026-05 sample Pattern B',
  1,
  'WITH_CONTEXT',
  'GROUPED_VALUE_METHOD',
  'UPPER_HEADER',
  2,
  3,
  'c659b1303ad36b93303a7a7bf401b15f29b9acad4335de90c51147dc8de40bd0',
  'VERIFIED',
  'ALLOW_AFTER_CONFIRM',
  0,
  'SKIP_CHECKED_OK',
  'IMPORT_AND_CHECK_LATER',
  'CP932',
  ',',
  '"',
  'draft seed: heartcross sample. active header is row 2. exam_date is not in CSV and remains pending facility/other-data confirmation.',
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
  `duplicate_row_policy` = VALUES(`duplicate_row_policy`),
  `missing_basic_info_policy` = VALUES(`missing_basic_info_policy`),
  `character_encoding` = VALUES(`character_encoding`),
  `delimiter` = VALUES(`delimiter`),
  `quote_char` = VALUES(`quote_char`),
  `note` = VALUES(`note`),
  `is_active` = VALUES(`is_active`),
  `updated_at` = CURRENT_TIMESTAMP(3);

SELECT `csv_format_version_id`
  INTO @heartcross_csv_format_version_id
FROM `phr_master`.`csv_format_versions`
WHERE `exam_facility_id` = @heartcross_exam_facility_id
  AND `mapping_version` = 'HEARTCROSS_2026_05_PATTERN_B_V1'
LIMIT 1;

DELETE FROM `tmp_csv_exam_mapping_seed`;

INSERT INTO `tmp_csv_exam_mapping_seed` (
  `seed_key`, `format_key`, `target_kind`, `target_field`, `target_namecode`,
  `header_context`, `header_name`, `header_occurrence`, `source_role`,
  `raw_value_type`, `raw_unit`, `is_required`, `priority`, `note`
) VALUES
-- Heartcross basic information. exam_date is intentionally absent pending facility/other-data confirmation.
('heartcross.basic.insurer_number', 'HEARTCROSS', 'LEDGER_FIELD', 'insurer_number', NULL, NULL, 'INSURER_NUMBER', 1, 'VALUE', NULL, NULL, 1, 10, 'basic: insurer number'),
('heartcross.basic.insurance_symbol_raw', 'HEARTCROSS', 'LEDGER_FIELD', 'insurance_symbol_raw', NULL, NULL, 'INSURANCE_CARD_SYMBOL', 1, 'VALUE', NULL, NULL, 1, 20, 'basic: insurance symbol'),
('heartcross.basic.insurance_number_raw', 'HEARTCROSS', 'LEDGER_FIELD', 'insurance_number_raw', NULL, NULL, 'INSURANCE_CARD_NUMBER', 1, 'VALUE', NULL, NULL, 1, 30, 'basic: insurance number'),
('heartcross.basic.insurance_branch_number_raw', 'HEARTCROSS', 'LEDGER_FIELD', 'insurance_branch_number_raw', NULL, NULL, 'INSURANCE_CARD_BRANCH_NUMBER', 1, 'VALUE', NULL, NULL, 0, 40, 'basic: insurance branch number; sample blank'),
('heartcross.basic.name_kana_raw', 'HEARTCROSS', 'LEDGER_FIELD', 'name_kana_raw', NULL, NULL, 'NAME_KANA', 1, 'VALUE', NULL, NULL, 1, 50, 'basic: kana name'),
('heartcross.basic.birthdate', 'HEARTCROSS', 'LEDGER_FIELD', 'birthdate', NULL, NULL, 'BIRTHDAY', 1, 'VALUE', NULL, NULL, 1, 60, 'basic: birthdate'),
('heartcross.basic.gender_raw', 'HEARTCROSS', 'LEDGER_FIELD', 'gender_raw', NULL, NULL, 'GENDER_CODE', 1, 'VALUE', NULL, NULL, 1, 70, 'basic: provisional gender for local subscriber matching test'),
('heartcross.basic.postal_code', 'HEARTCROSS', 'LEDGER_FIELD', 'postal_code', NULL, NULL, 'POSTALCODE', 1, 'VALUE', NULL, NULL, 0, 80, 'basic: postal code'),

-- Heartcross exam item values. Row 2 code/namecode is used as header_name.
('heartcross.exam.height', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N001000000000001', NULL, '9N001000000000001', 1, 'VALUE', NULL, NULL, 1, 1000, 'height'),
('heartcross.exam.weight', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N006000000000001', NULL, '9N006000000000001', 1, 'VALUE', NULL, NULL, 1, 1010, 'weight'),
('heartcross.exam.bmi', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N011000000000001', NULL, '9N011000000000001', 1, 'VALUE', NULL, NULL, 1, 1020, 'BMI'),
('heartcross.exam.waist', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N016160100000001', NULL, '9N016160100000001', 1, 'VALUE', NULL, NULL, 1, 1030, 'waist circumference measured'),
('heartcross.exam.sbp_avg', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9A755000000000001', NULL, '9A755000000000001', 1, 'VALUE', NULL, NULL, 1, 1040, 'average systolic blood pressure'),
('heartcross.exam.dbp_avg', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9A765000000000001', NULL, '9A765000000000001', 1, 'VALUE', NULL, NULL, 1, 1050, 'average diastolic blood pressure'),
('heartcross.exam.urine_protein', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '1A010000000190111', NULL, '1A010000000190111', 1, 'VALUE', NULL, NULL, 1, 1060, 'urine protein visual method'),
('heartcross.exam.urine_sugar', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '1A020000000190111', NULL, '1A020000000190111', 1, 'VALUE', NULL, NULL, 1, 1070, 'urine sugar visual method'),
('heartcross.exam.urine_occult_blood', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '1A100000000190111', NULL, '1A100000000190111', 1, 'VALUE', NULL, NULL, 0, 1080, 'urine occult blood visual method'),
('heartcross.exam.rbc', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '2A020000001930101', NULL, '2A020000001930101', 1, 'VALUE', NULL, NULL, 0, 1090, 'red blood cell count'),
('heartcross.exam.hemoglobin', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '2A030000001930101', NULL, '2A030000001930101', 1, 'VALUE', NULL, NULL, 0, 1100, 'hemoglobin'),
('heartcross.exam.tg_fasting', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3F015000002327101', NULL, '3F015000002327101', 1, 'VALUE', NULL, NULL, 1, 1110, 'fasting TG'),
('heartcross.exam.hdl', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3F070000002327101', NULL, '3F070000002327101', 1, 'VALUE', NULL, NULL, 1, 1120, 'HDL cholesterol'),
('heartcross.exam.ldl', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3F077000002327101', NULL, '3F077000002327101', 1, 'VALUE', NULL, NULL, 1, 1130, 'LDL cholesterol'),
('heartcross.exam.ast', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3B035000002327201', NULL, '3B035000002327201', 1, 'VALUE', NULL, NULL, 1, 1140, 'AST JSCC'),
('heartcross.exam.alt', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3B045000002327201', NULL, '3B045000002327201', 1, 'VALUE', NULL, NULL, 1, 1150, 'ALT JSCC'),
('heartcross.exam.ggt', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3B090000002327101', NULL, '3B090000002327101', 1, 'VALUE', NULL, NULL, 1, 1160, 'gamma-GTP JSCC'),
('heartcross.exam.postprandial_time', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N141000000000011', NULL, '9N141000000000011', 1, 'VALUE', NULL, NULL, 1, 1170, 'postprandial time CD; sample values 12.0/10.0/15.0 require decision'),
('heartcross.exam.glucose_fasting', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3D010000001927201', NULL, '3D010000001927201', 1, 'VALUE', NULL, NULL, 1, 1180, 'fasting glucose'),
('heartcross.exam.hba1c', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3D046000001920402', NULL, '3D046000001920402', 1, 'VALUE', NULL, NULL, 1, 1190, 'HbA1c HPLC'),
('heartcross.exam.glucose_random', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3D010129901927201', NULL, '3D010129901927201', 1, 'VALUE', NULL, NULL, 0, 1200, 'random glucose; sample blank'),
('heartcross.exam.creatinine', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3C015000002327101', NULL, '3C015000002327101', 1, 'VALUE', NULL, NULL, 0, 1210, 'serum creatinine'),
('heartcross.exam.uric_acid', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3C020000002327101', NULL, '3C020000002327101', 1, 'VALUE', NULL, NULL, 0, 1220, 'uric acid'),
('heartcross.exam.ecg_finding_text', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9A110160800000049', NULL, '9A110160800000049', 1, 'VALUE', NULL, NULL, 0, 1230, 'ECG finding text'),
('heartcross.exam.chest_xray_finding_text', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N206160800000049', NULL, '9N206160800000049', 1, 'VALUE', NULL, NULL, 0, 1240, 'chest X-ray finding text'),
('heartcross.exam.objective_symptoms_cd', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N066000000000011', NULL, '9N066000000000011', 1, 'VALUE', NULL, NULL, 1, 1250, 'objective symptoms CD'),
('heartcross.exam.objective_symptoms_text', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N066160800000049', NULL, '9N066160800000049', 1, 'VALUE', NULL, NULL, 1, 1260, 'objective symptoms text'),
('heartcross.exam.medical_history_cd', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N056000000000011', NULL, '9N056000000000011', 1, 'VALUE', NULL, NULL, 1, 1270, 'medical history CD'),
('heartcross.exam.medical_history_text', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N056160400000049', NULL, '9N056160400000049', 1, 'VALUE', NULL, NULL, 1, 1280, 'medical history text'),
('heartcross.exam.medication_bp', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N701000000000011', NULL, '9N701000000000011', 1, 'VALUE', NULL, NULL, 1, 1290, 'medication for blood pressure'),
('heartcross.exam.medication_glucose', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N706000000000011', NULL, '9N706000000000011', 1, 'VALUE', NULL, NULL, 1, 1300, 'medication for glucose'),
('heartcross.exam.medication_lipid', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N711000000000011', NULL, '9N711000000000011', 1, 'VALUE', NULL, NULL, 1, 1310, 'medication for lipid'),
('heartcross.exam.smoking', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N736000000000011', NULL, '9N736000000000011', 1, 'VALUE', NULL, NULL, 0, 1320, 'smoking'),
('heartcross.exam.metabolic', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N501000000000011', NULL, '9N501000000000011', 1, 'VALUE', NULL, NULL, 1, 1330, 'standard CD value, not facility ABC judgement'),
('heartcross.exam.guidance_level', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N506000000000011', NULL, '9N506000000000011', 1, 'VALUE', NULL, NULL, 1, 1340, 'standard CD value, not facility ABC judgement'),
('heartcross.exam.egfr', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '8A065000002391901', NULL, '8A065000002391901', 1, 'VALUE', NULL, NULL, 0, 1350, 'eGFR'),
('heartcross.exam.non_hdl', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3F069000002391901', NULL, '3F069000002391901', 1, 'VALUE', NULL, NULL, 1, 1360, 'non-HDL cholesterol');

INSERT INTO `phr_master`.`csv_exam_result_mapping_rules` (
  `csv_format_version_id`, `rule_key`, `target_kind`, `target_resolution_type`, `selection_mode`,
  `selection_group_code`, `target_namecode`, `target_identity_item_code`, `target_field`,
  `method_structure_type`, `raw_value_type`, `raw_unit`,
  `is_required`, `priority`, `is_active`, `note`
)
SELECT
  @heartcross_csv_format_version_id,
  `seed_key`,
  `target_kind`,
  CASE WHEN `target_kind` = 'EXAM_ITEM_VALUE' THEN 'SINGLE_NAMECODE' ELSE 'LEDGER_FIELD' END,
  'DIRECT',
  NULL,
  `target_namecode`,
  NULL,
  `target_field`,
  'SINGLE_COLUMN',
  `raw_value_type`,
  `raw_unit`,
  `is_required`,
  `priority`,
  1,
  CONCAT('draft seed:', `seed_key`, ':', COALESCE(`note`, ''))
FROM `tmp_csv_exam_mapping_seed`
WHERE `format_key` = 'HEARTCROSS'
ON DUPLICATE KEY UPDATE
  `target_kind` = VALUES(`target_kind`),
  `target_resolution_type` = VALUES(`target_resolution_type`),
  `selection_mode` = VALUES(`selection_mode`),
  `selection_group_code` = VALUES(`selection_group_code`),
  `target_namecode` = VALUES(`target_namecode`),
  `target_identity_item_code` = VALUES(`target_identity_item_code`),
  `target_field` = VALUES(`target_field`),
  `method_structure_type` = VALUES(`method_structure_type`),
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
WHERE r.`csv_format_version_id` = @heartcross_csv_format_version_id;

INSERT INTO `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_rule_id`, `condition_group_no`, `condition_type`,
  `locator_type`, `header_context`, `header_name`, `header_occurrence`, `column_no`,
  `operator`, `expected_value`, `expected_value_normalized`, `source_role`,
  `priority`, `is_active`, `note`
)
SELECT
  r.`csv_exam_result_mapping_rule_id`,
  1,
  'HEADER_MATCH',
  'HEADER_NAME',
  s.`header_context`,
  s.`header_name`,
  s.`header_occurrence`,
  NULL,
  'PRESENT',
  NULL,
  NULL,
  s.`source_role`,
  100,
  1,
  CONCAT('draft condition:', s.`seed_key`)
FROM `tmp_csv_exam_mapping_seed` s
JOIN `phr_master`.`csv_exam_result_mapping_rules` r
  ON r.`csv_format_version_id` = @heartcross_csv_format_version_id
 AND r.`note` LIKE CONCAT('draft seed:', s.`seed_key`, ':%')
WHERE s.`format_key` = 'HEARTCROSS';

DROP TEMPORARY TABLE `tmp_csv_exam_mapping_seed`;

COMMIT;
