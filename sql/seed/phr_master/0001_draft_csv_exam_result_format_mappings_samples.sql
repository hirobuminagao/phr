-- Seed for 02_02_exam_result_csv_import initial sample format mappings.
--
-- Scope:
-- - Run after exam_facilities seed is loaded from the 支払基金 CSV.
-- - Resolve exam_facility_id by exam_facilities.medical_institution_code.
-- - It covers CSV format versions and initial VALUE-focused mappings for:
--   - Hirooka Clinic sample Pattern A
--   - Healthcare Clinic Atsugi sample Pattern A
--   - Shibuya Westhills Clinic sample Pattern A
--   - Oroku Hospital joined sample Pattern C
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

SELECT `exam_facility_id`
  INTO @atsugi_exam_facility_id
FROM `phr_master`.`exam_facilities`
WHERE `medical_institution_code` = '1412910586'
LIMIT 1;

SELECT `exam_facility_id`
  INTO @shibuya_westhills_exam_facility_id
FROM `phr_master`.`exam_facilities`
WHERE `medical_institution_code` = '1311333301'
LIMIT 1;

SELECT `exam_facility_id`
  INTO @oroku_exam_facility_id
FROM `phr_master`.`exam_facilities`
WHERE `medical_institution_code` = '4710114044'
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
  `encoding_fallback_policy`,
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
  'ALLOW_COMMON_ENCODINGS',
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
  `encoding_fallback_policy` = VALUES(`encoding_fallback_policy`),
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

-- A failed execution can leave a temporary table in the same client session.
DROP TEMPORARY TABLE IF EXISTS `tmp_csv_exam_mapping_seed`;

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
('hirooka.exam.guidance_level', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N506000000000011', NULL, '保健指導区分', 1, 'VALUE', NULL, NULL, 1, 1300, 'standard CD value, not facility ABC judgement'),
('hirooka.exam.sbp_first', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9A751000000000001', NULL, '1回目 収縮期', 1, 'VALUE', NULL, NULL, 0, 1310, 'first systolic blood pressure'),
('hirooka.exam.dbp_first', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9A761000000000001', NULL, '1回目 拡張期', 1, 'VALUE', NULL, NULL, 0, 1320, 'first diastolic blood pressure'),
('hirooka.exam.sbp_second', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9A752000000000001', NULL, '2回目 収縮期', 1, 'VALUE', NULL, NULL, 0, 1330, 'second systolic blood pressure'),
('hirooka.exam.dbp_second', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9A762000000000001', NULL, '2回目 拡張期', 1, 'VALUE', NULL, NULL, 0, 1340, 'second diastolic blood pressure'),
('hirooka.exam.vision_right_uncorrected', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9E160162100000001', NULL, '裸眼遠点視力（右）', 1, 'VALUE', NULL, NULL, 0, 1350, 'uncorrected visual acuity right'),
('hirooka.exam.vision_left_uncorrected', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9E160162200000001', NULL, '裸眼遠点視力（左）', 1, 'VALUE', NULL, NULL, 0, 1360, 'uncorrected visual acuity left'),
('hirooka.exam.vision_right_corrected', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9E160162500000001', NULL, '矯正遠点視力（右）', 1, 'VALUE', NULL, NULL, 0, 1370, 'corrected visual acuity right'),
('hirooka.exam.vision_left_corrected', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9E160162600000001', NULL, '矯正遠点視力（左）', 1, 'VALUE', NULL, NULL, 0, 1380, 'corrected visual acuity left'),
('hirooka.exam.obesity_degree', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N026000000000002', NULL, '肥満度', 1, 'VALUE', NULL, NULL, 0, 1385, 'obesity degree percent'),
('hirooka.exam.urine_occult_blood', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '1A100000000190111', NULL, '尿潜血', 1, 'VALUE', NULL, NULL, 0, 1390, 'urine occult blood visual method'),
('hirooka.exam.wbc', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '2A010000001930101', NULL, '白血球数', 1, 'VALUE', NULL, NULL, 0, 1400, 'white blood cell count'),
('hirooka.exam.hematocrit', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '2A040000001930102', NULL, 'ヘマトクリット', 1, 'VALUE', NULL, NULL, 0, 1410, 'hematocrit'),
('hirooka.exam.platelet', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '2A050000001930101', NULL, '血小板数', 1, 'VALUE', NULL, NULL, 0, 1420, 'platelet count'),
('hirooka.exam.uric_acid', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '3C020000002327101', NULL, '尿酸', 1, 'VALUE', NULL, NULL, 0, 1430, 'serum uric acid'),
('hirooka.exam.total_protein', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '3A010000002327101', NULL, '総蛋白', 1, 'VALUE', NULL, NULL, 0, 1440, 'total protein'),
('hirooka.exam.total_cholesterol', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '3F050000002327101', NULL, '総コレステロール', 1, 'VALUE', NULL, NULL, 0, 1450, 'total cholesterol'),
('hirooka.exam.heart_rate', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N121000000000001', NULL, '心拍数', 1, 'VALUE', NULL, NULL, 0, 1460, 'heart rate'),
('hirooka.exam.abdominal_ultrasound_finding_text', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9F130160800000049', NULL, '腹部超音波所見', 1, 'VALUE', NULL, NULL, 0, 1470, 'abdominal ultrasound finding text'),
('hirooka.exam.gastric_xray_finding_text', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N256160800000049', NULL, '胃部Ｘ線所見', 1, 'VALUE', NULL, NULL, 0, 1480, 'upper GI X-ray finding text'),
('hirooka.exam.gastric_endoscopy_finding_text', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N266160800000049', NULL, '胃部内視鏡所見', 1, 'VALUE', NULL, NULL, 0, 1490, 'upper GI endoscopy finding text'),
('hirooka.exam.fecal_occult_blood_first', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '1B030000001599811', NULL, '便潜血（1回目）', 1, 'VALUE', NULL, NULL, 0, 1500, 'fecal occult blood first occurrence'),
('hirooka.exam.fecal_occult_blood_second', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '1B030000001599811', NULL, '便潜血（2回目）', 1, 'VALUE', NULL, NULL, 0, 1510, 'fecal occult blood second occurrence'),
('hirooka.exam.medication_bp', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N701000000000011', NULL, '１．服薬歴（血圧）', 1, 'VALUE', NULL, NULL, 0, 1600, 'questionnaire medication blood pressure'),
('hirooka.exam.medication_glucose', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N706000000000011', NULL, '２．服薬歴（血糖）', 1, 'VALUE', NULL, NULL, 0, 1610, 'questionnaire medication glucose'),
('hirooka.exam.medication_lipid', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N711000000000011', NULL, '３．服薬歴（脂質）', 1, 'VALUE', NULL, NULL, 0, 1620, 'questionnaire medication lipid'),
('hirooka.exam.history_stroke', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N716000000000011', NULL, '４．既往歴（脳卒中）', 1, 'VALUE', NULL, NULL, 0, 1630, 'questionnaire stroke history'),
('hirooka.exam.history_heart', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N721000000000011', NULL, '５．既往歴（心疾患）', 1, 'VALUE', NULL, NULL, 0, 1640, 'questionnaire heart disease history'),
('hirooka.exam.history_renal', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N726000000000011', NULL, '６．既往歴（慢性腎不全）', 1, 'VALUE', NULL, NULL, 0, 1650, 'questionnaire renal failure history'),
('hirooka.exam.history_anemia', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N731000000000011', NULL, '７．既往歴（貧血）', 1, 'VALUE', NULL, NULL, 0, 1660, 'questionnaire anemia history'),
('hirooka.exam.smoking', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N736000000000011', NULL, '８．喫煙', 1, 'VALUE', NULL, NULL, 0, 1670, 'questionnaire smoking'),
('hirooka.exam.weight_change', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N741000000000011', NULL, '９．２０歳の時の体重から10㎏以上増加していますか。', 1, 'VALUE', NULL, NULL, 0, 1680, 'questionnaire weight change'),
('hirooka.exam.exercise', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N746000000000011', NULL, '10．運動', 1, 'VALUE', NULL, NULL, 0, 1690, 'questionnaire exercise'),
('hirooka.exam.physical_activity', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N751000000000011', NULL, '11．日常の歩行・身体活動', 1, 'VALUE', NULL, NULL, 0, 1700, 'questionnaire physical activity'),
('hirooka.exam.walking_speed', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N756000000000011', NULL, '12．歩行速度', 1, 'VALUE', NULL, NULL, 0, 1710, 'questionnaire walking speed'),
('hirooka.exam.chewing', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N872000000000011', NULL, '13．食事をかんで食べる時', 1, 'VALUE', NULL, NULL, 0, 1720, 'questionnaire chewing'),
('hirooka.exam.eating_speed', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N766000000000011', NULL, '14．食べる速度', 1, 'VALUE', NULL, NULL, 0, 1730, 'questionnaire eating speed'),
('hirooka.exam.late_dinner', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N771000000000011', NULL, '15．遅い夕食', 1, 'VALUE', NULL, NULL, 0, 1740, 'questionnaire late dinner'),
('hirooka.exam.snacking', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N782000000000011', NULL, '16．朝昼夕の３食以外', 1, 'VALUE', NULL, NULL, 0, 1750, 'questionnaire snacking'),
('hirooka.exam.breakfast', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N781000000000011', NULL, '17．朝食ぬき', 1, 'VALUE', NULL, NULL, 0, 1760, 'questionnaire breakfast skipping'),
('hirooka.exam.drinking_frequency', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N786000000000011', NULL, '18．飲酒の頻度', 1, 'VALUE', NULL, NULL, 0, 1770, 'questionnaire drinking frequency'),
('hirooka.exam.drinking_amount', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N791000000000011', NULL, '19．飲酒量', 1, 'VALUE', NULL, NULL, 0, 1780, 'questionnaire drinking amount'),
('hirooka.exam.sleep', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N796000000000011', NULL, '20．睡眠で休養が十分とれていますか。', 1, 'VALUE', NULL, NULL, 0, 1790, 'questionnaire sleep'),
('hirooka.exam.lifestyle_improvement', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N801000000000011', NULL, '21．生活習慣の改善', 1, 'VALUE', NULL, NULL, 0, 1800, 'questionnaire lifestyle improvement'),
('hirooka.exam.guidance_history', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N808000000000011', NULL, '22．保健指導受診歴', 1, 'VALUE', NULL, NULL, 0, 1810, 'questionnaire specific health guidance history'),
('hirooka.exam.postprandial_time', 'HIROOKA', 'EXAM_ITEM_VALUE', NULL, '9N141000000000011', NULL, '健診前の食事状況', 1, 'VALUE', NULL, NULL, 0, 1820, 'postprandial time category');

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

-- Hirooka finding text is used to derive standard finding-presence CD values.
-- Facility ABC/category judgement columns are not used.
INSERT INTO `phr_master`.`csv_exam_result_mapping_rules` (
  `csv_format_version_id`, `rule_key`, `target_kind`, `target_resolution_type`, `selection_mode`,
  `target_namecode`, `method_structure_type`, `value_source_type`, `fixed_value`,
  `is_required`, `priority`, `is_active`, `note`
) VALUES
  (@hirooka_csv_format_version_id, 'hirooka.exam.ecg_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9A110160700000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 1820, 1, 'draft seed:hirooka.exam.ecg_presence_normal:異常所見なし -> 所見なし CD=2'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.ecg_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9A110160700000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 1830, 1, 'draft seed:hirooka.exam.ecg_presence_abnormal:所見本文あり -> 所見あり CD=1'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.chest_xray_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N206160700000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 1840, 1, 'draft seed:hirooka.exam.chest_xray_presence_normal:異常所見なし -> 所見なし CD=2'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.chest_xray_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N206160700000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 1850, 1, 'draft seed:hirooka.exam.chest_xray_presence_abnormal:所見本文あり -> 所見あり CD=1'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.abdominal_ultrasound_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9F130160700000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 1860, 1, 'draft seed:hirooka.exam.abdominal_ultrasound_presence_normal:異常所見なし -> 所見なし CD=2'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.abdominal_ultrasound_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9F130160700000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 1870, 1, 'draft seed:hirooka.exam.abdominal_ultrasound_presence_abnormal:所見本文あり -> 所見あり CD=1'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.gastric_xray_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N256160700000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 1880, 1, 'draft seed:hirooka.exam.gastric_xray_presence_normal:異常所見なし -> 所見なし CD=2'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.gastric_xray_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N256160700000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 1890, 1, 'draft seed:hirooka.exam.gastric_xray_presence_abnormal:所見本文あり -> 所見あり CD=1'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.gastric_endoscopy_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N266160700000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 1900, 1, 'draft seed:hirooka.exam.gastric_endoscopy_presence_normal:異常所見なし -> 所見なし CD=2'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.gastric_endoscopy_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N266160700000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 1910, 1, 'draft seed:hirooka.exam.gastric_endoscopy_presence_abnormal:所見本文あり -> 所見あり CD=1'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.medical_history_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N056000000000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 1920, 1, 'draft seed:hirooka.exam.medical_history_presence_normal:特になし -> 既往歴なし CD=2'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.medical_history_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N056000000000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 1930, 1, 'draft seed:hirooka.exam.medical_history_presence_abnormal:既往歴本文あり -> CD=1'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.subjective_symptoms_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N061000000000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 1940, 1, 'draft seed:hirooka.exam.subjective_symptoms_presence_normal:特になし -> 自覚症状なし CD=2'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.subjective_symptoms_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N061000000000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 1950, 1, 'draft seed:hirooka.exam.subjective_symptoms_presence_abnormal:自覚症状本文あり -> CD=1'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.objective_symptoms_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N066000000000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 1960, 1, 'draft seed:hirooka.exam.objective_symptoms_presence_normal:特になし -> 他覚症状なし CD=2'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.objective_symptoms_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N066000000000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 1970, 1, 'draft seed:hirooka.exam.objective_symptoms_presence_abnormal:他覚症状本文あり -> CD=1'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.hearing_right_1000_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9D100163100000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 1980, 1, 'draft seed:hirooka.exam.hearing_right_1000_normal:判定A -> 所見なし CD=2'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.hearing_right_1000_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9D100163100000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 1990, 1, 'draft seed:hirooka.exam.hearing_right_1000_abnormal:判定A以外 -> 所見あり CD=1'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.hearing_left_1000_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9D100163500000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 2000, 1, 'draft seed:hirooka.exam.hearing_left_1000_normal:判定A -> 所見なし CD=2'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.hearing_left_1000_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9D100163500000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 2010, 1, 'draft seed:hirooka.exam.hearing_left_1000_abnormal:判定A以外 -> 所見あり CD=1'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.hearing_right_4000_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9D100163200000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 2020, 1, 'draft seed:hirooka.exam.hearing_right_4000_normal:判定A -> 所見なし CD=2'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.hearing_right_4000_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9D100163200000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 2030, 1, 'draft seed:hirooka.exam.hearing_right_4000_abnormal:判定A以外 -> 所見あり CD=1'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.hearing_left_4000_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9D100163600000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 2040, 1, 'draft seed:hirooka.exam.hearing_left_4000_normal:判定A -> 所見なし CD=2'),
  (@hirooka_csv_format_version_id, 'hirooka.exam.hearing_left_4000_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9D100163600000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 2050, 1, 'draft seed:hirooka.exam.hearing_left_4000_abnormal:判定A以外 -> 所見あり CD=1')
ON DUPLICATE KEY UPDATE
  `target_kind` = VALUES(`target_kind`),
  `target_resolution_type` = VALUES(`target_resolution_type`),
  `selection_mode` = VALUES(`selection_mode`),
  `target_namecode` = VALUES(`target_namecode`),
  `method_structure_type` = VALUES(`method_structure_type`),
  `value_source_type` = VALUES(`value_source_type`),
  `fixed_value` = VALUES(`fixed_value`),
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

-- The normal and abnormal branches are mutually exclusive. Abnormal means a
-- non-empty finding text other than the facility's explicit no-finding token.
INSERT INTO `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_rule_id`, `condition_group_no`, `condition_type`,
  `locator_type`, `header_name`, `header_occurrence`, `operator`, `expected_value`,
  `source_role`, `priority`, `is_active`, `note`
)
SELECT r.`csv_exam_result_mapping_rule_id`, 1, 'CELL_VALUE', 'HEADER_NAME', x.`header_name`, 1,
       x.`operator`, x.`expected_value`, 'QUALIFIER', x.`priority`, 1,
       CONCAT('draft derived condition:', r.`rule_key`)
FROM `phr_master`.`csv_exam_result_mapping_rules` r
JOIN (
  SELECT 'hirooka.exam.ecg_presence_normal' AS `rule_key`, '安静時心電図所見' AS `header_name`, 'EQUALS' AS `operator`, '異常所見なし' AS `expected_value`, 100 AS `priority`
  UNION ALL SELECT 'hirooka.exam.ecg_presence_abnormal', '安静時心電図所見', 'NOT_EMPTY', NULL, 100
  UNION ALL SELECT 'hirooka.exam.ecg_presence_abnormal', '安静時心電図所見', 'NOT_EQUALS', '異常所見なし', 110
  UNION ALL SELECT 'hirooka.exam.chest_xray_presence_normal', '胸部Ｘ線所見', 'EQUALS', '異常所見なし', 100
  UNION ALL SELECT 'hirooka.exam.chest_xray_presence_abnormal', '胸部Ｘ線所見', 'NOT_EMPTY', NULL, 100
  UNION ALL SELECT 'hirooka.exam.chest_xray_presence_abnormal', '胸部Ｘ線所見', 'NOT_EQUALS', '異常所見なし', 110
  UNION ALL SELECT 'hirooka.exam.abdominal_ultrasound_presence_normal', '腹部超音波所見', 'EQUALS', '異常所見なし', 100
  UNION ALL SELECT 'hirooka.exam.abdominal_ultrasound_presence_abnormal', '腹部超音波所見', 'NOT_EMPTY', NULL, 100
  UNION ALL SELECT 'hirooka.exam.abdominal_ultrasound_presence_abnormal', '腹部超音波所見', 'NOT_EQUALS', '異常所見なし', 110
  UNION ALL SELECT 'hirooka.exam.gastric_xray_presence_normal', '胃部Ｘ線所見', 'EQUALS', '異常所見なし', 100
  UNION ALL SELECT 'hirooka.exam.gastric_xray_presence_abnormal', '胃部Ｘ線所見', 'NOT_EMPTY', NULL, 100
  UNION ALL SELECT 'hirooka.exam.gastric_xray_presence_abnormal', '胃部Ｘ線所見', 'NOT_EQUALS', '異常所見なし', 110
  UNION ALL SELECT 'hirooka.exam.gastric_endoscopy_presence_normal', '胃部内視鏡所見', 'EQUALS', '異常所見なし', 100
  UNION ALL SELECT 'hirooka.exam.gastric_endoscopy_presence_abnormal', '胃部内視鏡所見', 'NOT_EMPTY', NULL, 100
  UNION ALL SELECT 'hirooka.exam.gastric_endoscopy_presence_abnormal', '胃部内視鏡所見', 'NOT_EQUALS', '異常所見なし', 110
  UNION ALL SELECT 'hirooka.exam.medical_history_presence_normal', '既往歴', 'EQUALS', '特になし、', 100
  UNION ALL SELECT 'hirooka.exam.medical_history_presence_abnormal', '既往歴', 'NOT_EMPTY', NULL, 100
  UNION ALL SELECT 'hirooka.exam.medical_history_presence_abnormal', '既往歴', 'NOT_EQUALS', '特になし、', 110
  UNION ALL SELECT 'hirooka.exam.subjective_symptoms_presence_normal', '自覚症状', 'EQUALS', '特になし', 100
  UNION ALL SELECT 'hirooka.exam.subjective_symptoms_presence_abnormal', '自覚症状', 'NOT_EMPTY', NULL, 100
  UNION ALL SELECT 'hirooka.exam.subjective_symptoms_presence_abnormal', '自覚症状', 'NOT_EQUALS', '特になし', 110
  UNION ALL SELECT 'hirooka.exam.objective_symptoms_presence_normal', '他覚症状', 'EQUALS', '特になし', 100
  UNION ALL SELECT 'hirooka.exam.objective_symptoms_presence_abnormal', '他覚症状', 'NOT_EMPTY', NULL, 100
  UNION ALL SELECT 'hirooka.exam.objective_symptoms_presence_abnormal', '他覚症状', 'NOT_EQUALS', '特になし', 110
  UNION ALL SELECT 'hirooka.exam.hearing_right_1000_normal', '右）1000Hz 判定', 'EQUALS', 'Ａ', 100
  UNION ALL SELECT 'hirooka.exam.hearing_right_1000_abnormal', '右）1000Hz 判定', 'NOT_EMPTY', NULL, 100
  UNION ALL SELECT 'hirooka.exam.hearing_right_1000_abnormal', '右）1000Hz 判定', 'NOT_EQUALS', 'Ａ', 110
  UNION ALL SELECT 'hirooka.exam.hearing_left_1000_normal', '左）1000Hz 判定', 'EQUALS', 'Ａ', 100
  UNION ALL SELECT 'hirooka.exam.hearing_left_1000_abnormal', '左）1000Hz 判定', 'NOT_EMPTY', NULL, 100
  UNION ALL SELECT 'hirooka.exam.hearing_left_1000_abnormal', '左）1000Hz 判定', 'NOT_EQUALS', 'Ａ', 110
  UNION ALL SELECT 'hirooka.exam.hearing_right_4000_normal', '右）4000Hz 判定', 'EQUALS', 'Ａ', 100
  UNION ALL SELECT 'hirooka.exam.hearing_right_4000_abnormal', '右）4000Hz 判定', 'NOT_EMPTY', NULL, 100
  UNION ALL SELECT 'hirooka.exam.hearing_right_4000_abnormal', '右）4000Hz 判定', 'NOT_EQUALS', 'Ａ', 110
  UNION ALL SELECT 'hirooka.exam.hearing_left_4000_normal', '左）4000Hz 判定', 'EQUALS', 'Ａ', 100
  UNION ALL SELECT 'hirooka.exam.hearing_left_4000_abnormal', '左）4000Hz 判定', 'NOT_EMPTY', NULL, 100
  UNION ALL SELECT 'hirooka.exam.hearing_left_4000_abnormal', '左）4000Hz 判定', 'NOT_EQUALS', 'Ａ', 110
) x ON x.`rule_key` = r.`rule_key`
WHERE r.`csv_format_version_id` = @hirooka_csv_format_version_id;

-- A configured finding column with an empty cell means no finding. Keep this
-- separate from a missing header, which remains a template mismatch.
INSERT INTO `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_rule_id`, `condition_group_no`, `condition_type`,
  `locator_type`, `header_name`, `header_occurrence`, `operator`, `expected_value`,
  `source_role`, `priority`, `is_active`, `note`
)
SELECT r.`csv_exam_result_mapping_rule_id`, 2, 'CELL_VALUE', 'HEADER_NAME', x.`header_name`, 1,
       'EMPTY', NULL, 'QUALIFIER', 100, 1,
       CONCAT('draft empty means no finding:', r.`rule_key`)
FROM `phr_master`.`csv_exam_result_mapping_rules` r
JOIN (
  SELECT 'hirooka.exam.medical_history_presence_normal' AS `rule_key`, '既往歴' AS `header_name`
  UNION ALL SELECT 'hirooka.exam.subjective_symptoms_presence_normal', '自覚症状'
  UNION ALL SELECT 'hirooka.exam.objective_symptoms_presence_normal', '他覚症状'
) x ON x.`rule_key` = r.`rule_key`
WHERE r.`csv_format_version_id` = @hirooka_csv_format_version_id;

-- Do not store the no-finding token as ST text; retain ST only for abnormal rows.
INSERT INTO `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_rule_id`, `condition_group_no`, `condition_type`,
  `locator_type`, `header_name`, `header_occurrence`, `operator`, `expected_value`,
  `source_role`, `priority`, `is_active`, `note`
)
SELECT r.`csv_exam_result_mapping_rule_id`, 1, 'CELL_VALUE', 'HEADER_NAME', x.`header_name`, 1,
       x.`operator`, x.`expected_value`, 'QUALIFIER', x.`priority`, 1,
       CONCAT('draft abnormal finding condition:', r.`rule_key`)
FROM `phr_master`.`csv_exam_result_mapping_rules` r
JOIN (
  SELECT 'hirooka.exam.ecg_finding_text' AS `rule_key`, '安静時心電図所見' AS `header_name`, 'NOT_EMPTY' AS `operator`, NULL AS `expected_value`, 110 AS `priority`
  UNION ALL SELECT 'hirooka.exam.ecg_finding_text', '安静時心電図所見', 'NOT_EQUALS', '異常所見なし', 120
  UNION ALL SELECT 'hirooka.exam.chest_xray_finding_text', '胸部Ｘ線所見', 'NOT_EMPTY', NULL, 110
  UNION ALL SELECT 'hirooka.exam.chest_xray_finding_text', '胸部Ｘ線所見', 'NOT_EQUALS', '異常所見なし', 120
  UNION ALL SELECT 'hirooka.exam.abdominal_ultrasound_finding_text', '腹部超音波所見', 'NOT_EMPTY', NULL, 110
  UNION ALL SELECT 'hirooka.exam.abdominal_ultrasound_finding_text', '腹部超音波所見', 'NOT_EQUALS', '異常所見なし', 120
  UNION ALL SELECT 'hirooka.exam.gastric_xray_finding_text', '胃部Ｘ線所見', 'NOT_EMPTY', NULL, 110
  UNION ALL SELECT 'hirooka.exam.gastric_xray_finding_text', '胃部Ｘ線所見', 'NOT_EQUALS', '異常所見なし', 120
  UNION ALL SELECT 'hirooka.exam.gastric_endoscopy_finding_text', '胃部内視鏡所見', 'NOT_EMPTY', NULL, 110
  UNION ALL SELECT 'hirooka.exam.gastric_endoscopy_finding_text', '胃部内視鏡所見', 'NOT_EQUALS', '異常所見なし', 120
  UNION ALL SELECT 'hirooka.exam.medical_history_text', '既往歴', 'NOT_EMPTY', NULL, 110
  UNION ALL SELECT 'hirooka.exam.medical_history_text', '既往歴', 'NOT_EQUALS', '特になし、', 120
  UNION ALL SELECT 'hirooka.exam.subjective_symptoms_text', '自覚症状', 'NOT_EMPTY', NULL, 110
  UNION ALL SELECT 'hirooka.exam.subjective_symptoms_text', '自覚症状', 'NOT_EQUALS', '特になし', 120
  UNION ALL SELECT 'hirooka.exam.objective_symptoms_text', '他覚症状', 'NOT_EMPTY', NULL, 110
  UNION ALL SELECT 'hirooka.exam.objective_symptoms_text', '他覚症状', 'NOT_EQUALS', '特になし', 120
) x ON x.`rule_key` = r.`rule_key`
WHERE r.`csv_format_version_id` = @hirooka_csv_format_version_id;

-- ============================================================
-- Healthcare Clinic Atsugi / sample 001
-- ============================================================
--
-- Atsugi sample uses the same 830-column Pattern A header as the Hirooka
-- sample. Register it as a facility-specific format version and copy the
-- confirmed Pattern A rules/conditions without changing medical semantics.

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
  `encoding_fallback_policy`,
  `delimiter`,
  `quote_char`,
  `note`,
  `is_active`
) VALUES (
  @atsugi_exam_facility_id,
  'ATSUGI_2026_05_PATTERN_A_V1',
  'CSV',
  'ヘルスケアクリニック厚木 2026-05 sample Pattern A',
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
  'ALLOW_COMMON_ENCODINGS',
  ',',
  '"',
  'draft seed: atsugi sample. same 830-column Pattern A header as hirooka sample; mappings are copied from hirooka Pattern A.',
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
  `encoding_fallback_policy` = VALUES(`encoding_fallback_policy`),
  `delimiter` = VALUES(`delimiter`),
  `quote_char` = VALUES(`quote_char`),
  `note` = VALUES(`note`),
  `is_active` = VALUES(`is_active`),
  `updated_at` = CURRENT_TIMESTAMP(3);

SELECT `csv_format_version_id`
  INTO @atsugi_csv_format_version_id
FROM `phr_master`.`csv_format_versions`
WHERE `exam_facility_id` = @atsugi_exam_facility_id
  AND `mapping_version` = 'ATSUGI_2026_05_PATTERN_A_V1'
LIMIT 1;

INSERT INTO `phr_master`.`csv_exam_result_mapping_rules` (
  `csv_format_version_id`, `rule_key`, `target_kind`, `target_resolution_type`, `selection_mode`,
  `selection_group_code`, `target_namecode`, `target_identity_item_code`, `target_field`,
  `method_structure_type`, `value_source_type`, `fixed_value`, `value_join_separator`,
  `raw_value_type`, `raw_unit`, `is_required`, `priority`, `is_active`, `note`
)
SELECT
  @atsugi_csv_format_version_id,
  REPLACE(r.`rule_key`, 'hirooka.', 'atsugi.'),
  r.`target_kind`,
  r.`target_resolution_type`,
  r.`selection_mode`,
  r.`selection_group_code`,
  r.`target_namecode`,
  r.`target_identity_item_code`,
  r.`target_field`,
  r.`method_structure_type`,
  r.`value_source_type`,
  r.`fixed_value`,
  r.`value_join_separator`,
  r.`raw_value_type`,
  r.`raw_unit`,
  r.`is_required`,
  r.`priority`,
  r.`is_active`,
  REPLACE(r.`note`, 'hirooka', 'atsugi')
FROM `phr_master`.`csv_exam_result_mapping_rules` r
WHERE r.`csv_format_version_id` = @hirooka_csv_format_version_id
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
  `fixed_value` = VALUES(`fixed_value`),
  `value_join_separator` = VALUES(`value_join_separator`),
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
WHERE r.`csv_format_version_id` = @atsugi_csv_format_version_id;

INSERT INTO `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_rule_id`, `condition_group_no`, `condition_type`,
  `locator_type`, `header_context`, `header_name`, `header_occurrence`, `column_no`,
  `operator`, `expected_value`, `expected_value_normalized`, `source_role`,
  `priority`, `is_active`, `note`
)
SELECT
  atsugi_rule.`csv_exam_result_mapping_rule_id`,
  c.`condition_group_no`,
  c.`condition_type`,
  c.`locator_type`,
  c.`header_context`,
  c.`header_name`,
  c.`header_occurrence`,
  c.`column_no`,
  c.`operator`,
  c.`expected_value`,
  c.`expected_value_normalized`,
  c.`source_role`,
  c.`priority`,
  c.`is_active`,
  REPLACE(c.`note`, 'hirooka', 'atsugi')
FROM `phr_master`.`csv_exam_result_mapping_conditions` c
JOIN `phr_master`.`csv_exam_result_mapping_rules` hirooka_rule
  ON hirooka_rule.`csv_exam_result_mapping_rule_id` = c.`csv_exam_result_mapping_rule_id`
JOIN `phr_master`.`csv_exam_result_mapping_rules` atsugi_rule
  ON atsugi_rule.`csv_format_version_id` = @atsugi_csv_format_version_id
 AND atsugi_rule.`rule_key` = REPLACE(hirooka_rule.`rule_key`, 'hirooka.', 'atsugi.')
WHERE hirooka_rule.`csv_format_version_id` = @hirooka_csv_format_version_id;

-- ============================================================
-- Shibuya Westhills Clinic / sample 001
-- ============================================================
--
-- Shibuya Westhills sample uses the same Pattern A 830-column single-row
-- Japanese header style as Hirooka/Atsugi. The confirmed Pattern A result-value
-- mappings can be reused as facility-specific seed data.

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
  `encoding_fallback_policy`,
  `delimiter`,
  `quote_char`,
  `note`,
  `is_active`
) VALUES (
  @shibuya_westhills_exam_facility_id,
  'SHIBUYA_WESTHILLS_2026_05_PATTERN_A_V1',
  'CSV',
  '渋谷ウエストヒルズクリニック 2026-05 sample Pattern A',
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
  'ALLOW_COMMON_ENCODINGS',
  ',',
  '"',
  'draft seed: shibuya westhills sample. Pattern A 830-column header.',
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
  `encoding_fallback_policy` = VALUES(`encoding_fallback_policy`),
  `delimiter` = VALUES(`delimiter`),
  `quote_char` = VALUES(`quote_char`),
  `note` = VALUES(`note`),
  `is_active` = VALUES(`is_active`),
  `updated_at` = CURRENT_TIMESTAMP(3);

SELECT `csv_format_version_id`
  INTO @shibuya_westhills_csv_format_version_id
FROM `phr_master`.`csv_format_versions`
WHERE `exam_facility_id` = @shibuya_westhills_exam_facility_id
  AND `mapping_version` = 'SHIBUYA_WESTHILLS_2026_05_PATTERN_A_V1'
LIMIT 1;

INSERT INTO `phr_master`.`csv_exam_result_mapping_rules` (
  `csv_format_version_id`, `rule_key`, `target_kind`, `target_resolution_type`, `selection_mode`,
  `selection_group_code`, `target_namecode`, `target_identity_item_code`, `target_field`,
  `method_structure_type`, `value_source_type`, `fixed_value`, `value_join_separator`,
  `raw_value_type`, `raw_unit`, `is_required`, `priority`, `is_active`, `note`
)
SELECT
  @shibuya_westhills_csv_format_version_id,
  REPLACE(r.`rule_key`, 'hirooka.', 'shibuya_westhills.'),
  r.`target_kind`,
  r.`target_resolution_type`,
  r.`selection_mode`,
  r.`selection_group_code`,
  r.`target_namecode`,
  r.`target_identity_item_code`,
  r.`target_field`,
  r.`method_structure_type`,
  r.`value_source_type`,
  r.`fixed_value`,
  r.`value_join_separator`,
  r.`raw_value_type`,
  r.`raw_unit`,
  r.`is_required`,
  r.`priority`,
  r.`is_active`,
  REPLACE(r.`note`, 'hirooka', 'shibuya_westhills')
FROM `phr_master`.`csv_exam_result_mapping_rules` r
WHERE r.`csv_format_version_id` = @hirooka_csv_format_version_id
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
  `fixed_value` = VALUES(`fixed_value`),
  `value_join_separator` = VALUES(`value_join_separator`),
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
WHERE r.`csv_format_version_id` = @shibuya_westhills_csv_format_version_id;

INSERT INTO `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_rule_id`, `condition_group_no`, `condition_type`,
  `locator_type`, `header_context`, `header_name`, `header_occurrence`, `column_no`,
  `operator`, `expected_value`, `expected_value_normalized`, `source_role`,
  `priority`, `is_active`, `note`
)
SELECT
  shibuya_rule.`csv_exam_result_mapping_rule_id`,
  c.`condition_group_no`,
  c.`condition_type`,
  c.`locator_type`,
  c.`header_context`,
  c.`header_name`,
  c.`header_occurrence`,
  c.`column_no`,
  c.`operator`,
  c.`expected_value`,
  c.`expected_value_normalized`,
  c.`source_role`,
  c.`priority`,
  c.`is_active`,
  REPLACE(c.`note`, 'hirooka', 'shibuya_westhills')
FROM `phr_master`.`csv_exam_result_mapping_conditions` c
JOIN `phr_master`.`csv_exam_result_mapping_rules` hirooka_rule
  ON hirooka_rule.`csv_exam_result_mapping_rule_id` = c.`csv_exam_result_mapping_rule_id`
JOIN `phr_master`.`csv_exam_result_mapping_rules` shibuya_rule
  ON shibuya_rule.`csv_format_version_id` = @shibuya_westhills_csv_format_version_id
 AND shibuya_rule.`rule_key` = REPLACE(hirooka_rule.`rule_key`, 'hirooka.', 'shibuya_westhills.')
WHERE hirooka_rule.`csv_format_version_id` = @hirooka_csv_format_version_id;

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
  `encoding_fallback_policy`,
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
  '6ce5a7d844a2351c6f1ef97743f023e3c135cac2d669048fe032f4acfcc25544',
  'VERIFIED',
  'ALLOW_AFTER_CONFIRM',
  0,
  'SKIP_CHECKED_OK',
  'IMPORT_AND_CHECK_LATER',
  'CP932',
  'ALLOW_COMMON_ENCODINGS',
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
  `encoding_fallback_policy` = VALUES(`encoding_fallback_policy`),
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
-- Heartcross basic information. Gender and exam date are appended test columns;
-- the facility/other-data source remains pending confirmation.
('heartcross.basic.insurer_number', 'HEARTCROSS', 'LEDGER_FIELD', 'insurer_number', NULL, NULL, 'INSURER_NUMBER', 1, 'VALUE', NULL, NULL, 1, 10, 'basic: insurer number'),
('heartcross.basic.insurance_symbol_raw', 'HEARTCROSS', 'LEDGER_FIELD', 'insurance_symbol_raw', NULL, NULL, 'INSURANCE_CARD_SYMBOL', 1, 'VALUE', NULL, NULL, 1, 20, 'basic: insurance symbol'),
('heartcross.basic.insurance_number_raw', 'HEARTCROSS', 'LEDGER_FIELD', 'insurance_number_raw', NULL, NULL, 'INSURANCE_CARD_NUMBER', 1, 'VALUE', NULL, NULL, 1, 30, 'basic: insurance number'),
('heartcross.basic.insurance_branch_number_raw', 'HEARTCROSS', 'LEDGER_FIELD', 'insurance_branch_number_raw', NULL, NULL, 'INSURANCE_CARD_BRANCH_NUMBER', 1, 'VALUE', NULL, NULL, 0, 40, 'basic: insurance branch number; sample blank'),
('heartcross.basic.name_kana_raw', 'HEARTCROSS', 'LEDGER_FIELD', 'name_kana_raw', NULL, NULL, 'NAME_KANA', 1, 'VALUE', NULL, NULL, 1, 50, 'basic: kana name'),
('heartcross.basic.birthdate', 'HEARTCROSS', 'LEDGER_FIELD', 'birthdate', NULL, NULL, 'BIRTHDAY', 1, 'VALUE', NULL, NULL, 1, 60, 'basic: birthdate'),
('heartcross.basic.gender_raw', 'HEARTCROSS', 'LEDGER_FIELD', 'gender_raw', NULL, NULL, 'GENDER', 1, 'VALUE', NULL, NULL, 1, 70, 'basic: appended test gender; 男/女 is normalized by the shared gender library'),
('heartcross.basic.exam_date', 'HEARTCROSS', 'LEDGER_FIELD', 'exam_date', NULL, NULL, 'EXAM_DATE', 1, 'VALUE', NULL, NULL, 1, 80, 'basic: appended provisional exam date in YYYY/MM/DD format'),
('heartcross.basic.postal_code', 'HEARTCROSS', 'LEDGER_FIELD', 'postal_code', NULL, NULL, 'POSTALCODE', 1, 'VALUE', NULL, NULL, 0, 90, 'basic: postal code'),

-- Heartcross exam item values. Row 2 code/namecode is used as header_name.
-- The following CSV columns are intentionally not mapped:
-- - intentionally excluded derived/non-standard values: 9N012000000000001, 9N013000000000001
-- - intentionally excluded legacy phase-3 questionnaire item: 9N806000000000011
-- - facility judgement pending confirmation: 9N256160700000011, 9N266160700000011,
--   9F130160700000011,
--   9N251160700000011, 9N271160700000011, 9N276160700000011,
--   9N281160700000011, 9F140160700000011, 9N291160700000011,
--   9N511000000000049
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
('heartcross.exam.non_hdl', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3F069000002391901', NULL, '3F069000002391901', 1, 'VALUE', NULL, NULL, 1, 1360, 'non-HDL cholesterol'),
('heartcross.exam.9n026000000000002', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N026000000000002', NULL, '9N026000000000002', 1, 'VALUE', NULL, NULL, 0, 2000, '肥満度％'),
('heartcross.exam.9n021000000000001', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N021000000000001', NULL, '9N021000000000001', 1, 'VALUE', NULL, NULL, 0, 2010, '内臓脂肪面積'),
('heartcross.exam.9e160162100000001', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9E160162100000001', NULL, '9E160162100000001', 1, 'VALUE', NULL, NULL, 0, 2020, '裸眼視力(右)'),
('heartcross.exam.9e160162200000001', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9E160162200000001', NULL, '9E160162200000001', 1, 'VALUE', NULL, NULL, 0, 2030, '裸眼視力(左)'),
('heartcross.exam.9e160162500000001', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9E160162500000001', NULL, '9E160162500000001', 1, 'VALUE', NULL, NULL, 0, 2040, '矯正視力(右)'),
('heartcross.exam.9e160162600000001', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9E160162600000001', NULL, '9E160162600000001', 1, 'VALUE', NULL, NULL, 0, 2050, '矯正視力(左)'),
('heartcross.exam.9e105162100000001', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9E105162100000001', NULL, '9E105162100000001', 1, 'VALUE', NULL, NULL, 0, 2060, '眼圧(右)'),
('heartcross.exam.9e105162200000001', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9E105162200000001', NULL, '9E105162200000001', 1, 'VALUE', NULL, NULL, 0, 2070, '眼圧(左)'),
('heartcross.exam.9a751000000000001', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9A751000000000001', NULL, '9A751000000000001', 1, 'VALUE', NULL, NULL, 0, 2080, '最高血圧1回目'),
('heartcross.exam.9a761000000000001', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9A761000000000001', NULL, '9A761000000000001', 1, 'VALUE', NULL, NULL, 0, 2090, '最低血圧1回目'),
('heartcross.exam.9a752000000000001', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9A752000000000001', NULL, '9A752000000000001', 1, 'VALUE', NULL, NULL, 0, 2100, '最高血圧2回目'),
('heartcross.exam.9a762000000000001', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9A762000000000001', NULL, '9A762000000000001', 1, 'VALUE', NULL, NULL, 0, 2110, '最低血圧2回目'),
('heartcross.exam.1a030000000190301', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '1A030000000190301', NULL, '1A030000000190301', 1, 'VALUE', NULL, NULL, 0, 2120, '比重'),
('heartcross.exam.2a040000001930102', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '2A040000001930102', NULL, '2A040000001930102', 1, 'VALUE', NULL, NULL, 0, 2130, 'ﾍﾏﾄｸﾘｯﾄ'),
('heartcross.exam.2a050000001930101', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '2A050000001930101', NULL, '2A050000001930101', 1, 'VALUE', NULL, NULL, 0, 2140, '血小板数'),
('heartcross.exam.2a010000001930101', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '2A010000001930101', NULL, '2A010000001930101', 1, 'VALUE', NULL, NULL, 0, 2150, '白血球数'),
('heartcross.exam.2a060000001930101', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '2A060000001930101', NULL, '2A060000001930101', 1, 'VALUE', NULL, NULL, 0, 2160, 'ＭＣＶ'),
('heartcross.exam.2a070000001930101', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '2A070000001930101', NULL, '2A070000001930101', 1, 'VALUE', NULL, NULL, 0, 2170, 'ＭＣＨ'),
('heartcross.exam.2a080000001930101', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '2A080000001930101', NULL, '2A080000001930101', 1, 'VALUE', NULL, NULL, 0, 2180, 'ＭＣＨＣ'),
('heartcross.exam.2a020161001930149', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '2A020161001930149', NULL, '2A020161001930149', 1, 'VALUE', NULL, NULL, 0, 2190, '貧血実施理由'),
('heartcross.exam.3f050000002327101', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3F050000002327101', NULL, '3F050000002327101', 1, 'VALUE', NULL, NULL, 0, 2200, '総ｺﾚｽﾃﾛｰﾙ'),
('heartcross.exam.3b070000002327101', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3B070000002327101', NULL, '3B070000002327101', 1, 'VALUE', NULL, NULL, 0, 2210, 'ＡＬＰ'),
('heartcross.exam.3j010000002327101', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3J010000002327101', NULL, '3J010000002327101', 1, 'VALUE', NULL, NULL, 0, 2220, '総ﾋﾞﾘﾙﾋﾞﾝ'),
('heartcross.exam.3a010000002327101', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3A010000002327101', NULL, '3A010000002327101', 1, 'VALUE', NULL, NULL, 0, 2230, '総蛋白'),
('heartcross.exam.3a015000002327101', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3A015000002327101', NULL, '3A015000002327101', 1, 'VALUE', NULL, NULL, 0, 2240, 'ｱﾙﾌﾞﾐﾝ'),
('heartcross.exam.9e100160900000049', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9E100160900000049', NULL, '9E100160900000049', 1, 'VALUE', NULL, NULL, 0, 2250, '眼底所見1'),
('heartcross.exam.9e100166200000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9E100166200000011', NULL, '9E100166200000011', 1, 'VALUE', NULL, NULL, 0, 2260, '眼底SCHEIE(S)'),
('heartcross.exam.9e100166100000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9E100166100000011', NULL, '9E100166100000011', 1, 'VALUE', NULL, NULL, 0, 2270, '眼底SCHEIE(H)'),
('heartcross.exam.9e100166000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9E100166000000011', NULL, '9E100166000000011', 1, 'VALUE', NULL, NULL, 0, 2280, '眼底KW'),
('heartcross.exam.9e100166300000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9E100166300000011', NULL, '9E100166300000011', 1, 'VALUE', NULL, NULL, 0, 2290, '眼底SCOTT'),
('heartcross.exam.9e100161000000049', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9E100161000000049', NULL, '9E100161000000049', 1, 'VALUE', NULL, NULL, 0, 2300, '眼底実施理由'),
('heartcross.exam.9a110161000000049', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9A110161000000049', NULL, '9A110161000000049', 1, 'VALUE', NULL, NULL, 0, 2310, '心電図実施理由'),
('heartcross.exam.9n121000000000001', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N121000000000001', NULL, '9N121000000000001', 1, 'VALUE', NULL, NULL, 0, 2320, '心拍数'),
('heartcross.exam.9c310000000000001', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9C310000000000001', NULL, '9C310000000000001', 1, 'VALUE', NULL, NULL, 0, 2330, '努力性肺活量'),
('heartcross.exam.9c380000000000002', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9C380000000000002', NULL, '9C380000000000002', 1, 'VALUE', NULL, NULL, 0, 2340, '％肺活量'),
('heartcross.exam.9c320000000000001', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9C320000000000001', NULL, '9C320000000000001', 1, 'VALUE', NULL, NULL, 0, 2350, '1秒量'),
('heartcross.exam.9c330000000000002', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9C330000000000002', NULL, '9C330000000000002', 1, 'VALUE', NULL, NULL, 0, 2360, '1秒率'),
('heartcross.exam.9n256160800000049', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N256160800000049', NULL, '9N256160800000049', 1, 'VALUE', NULL, NULL, 0, 2370, '上部消化管Ｘ所見1'),
('heartcross.exam.9n266160800000049', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N266160800000049', NULL, '9N266160800000049', 1, 'VALUE', NULL, NULL, 0, 2380, '上部消化管内視鏡診断1'),
('heartcross.exam.9f130160800000049', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9F130160800000049', NULL, '9F130160800000049', 1, 'VALUE', NULL, NULL, 0, 2390, '腹部ｴｺｰ所見1'),
('heartcross.exam.9n061000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N061000000000011', NULL, '9N061000000000011', 1, 'VALUE', NULL, NULL, 0, 2400, 'かぜをひいている'),
('heartcross.exam.9n061160800000049', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N061160800000049', NULL, '9N061160800000049', 1, 'VALUE', NULL, NULL, 0, 2410, '食欲がない'),
('heartcross.exam.9n716000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N716000000000011', NULL, '9N716000000000011', 1, 'VALUE', NULL, NULL, 0, 2420, '脳卒中'),
('heartcross.exam.9n721000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N721000000000011', NULL, '9N721000000000011', 1, 'VALUE', NULL, NULL, 0, 2430, '心臓病'),
('heartcross.exam.9n726000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N726000000000011', NULL, '9N726000000000011', 1, 'VALUE', NULL, NULL, 0, 2440, '慢性の腎不全や人工透析'),
('heartcross.exam.9n731000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N731000000000011', NULL, '9N731000000000011', 1, 'VALUE', NULL, NULL, 0, 2450, '貧血'),
('heartcross.exam.9n741000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N741000000000011', NULL, '9N741000000000011', 1, 'VALUE', NULL, NULL, 0, 2460, '20歳時の体重から10kg以上増加'),
('heartcross.exam.9n746000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N746000000000011', NULL, '9N746000000000011', 1, 'VALUE', NULL, NULL, 0, 2470, '30分以上の運動を週2日以上、1年以上実施'),
('heartcross.exam.9n751000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N751000000000011', NULL, '9N751000000000011', 1, 'VALUE', NULL, NULL, 0, 2480, '歩行又は同等の身体活動を1日1時間以上実施'),
('heartcross.exam.9n756000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N756000000000011', NULL, '9N756000000000011', 1, 'VALUE', NULL, NULL, 0, 2490, 'ほぼ同年齢の同性と比較して歩く速度が速い'),
('heartcross.exam.9n872000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N872000000000011', NULL, '9N872000000000011', 1, 'VALUE', NULL, NULL, 0, 2500, '食事をかんで食べる時の状態はどれにあてはまりますか'),
('heartcross.exam.9n766000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N766000000000011', NULL, '9N766000000000011', 1, 'VALUE', NULL, NULL, 0, 2510, '食べる速度が人と比較して速い'),
('heartcross.exam.9n771000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N771000000000011', NULL, '9N771000000000011', 1, 'VALUE', NULL, NULL, 0, 2520, '就寝前2時間以内の夕食が週3回以上'),
('heartcross.exam.9n782000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N782000000000011', NULL, '9N782000000000011', 1, 'VALUE', NULL, NULL, 0, 2530, '朝昼夕の３食以外に間食や甘い飲み物を摂取していますか'),
('heartcross.exam.9n781000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N781000000000011', NULL, '9N781000000000011', 1, 'VALUE', NULL, NULL, 0, 2540, '朝食を抜くことが週3回以上'),
('heartcross.exam.9n786000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N786000000000011', NULL, '9N786000000000011', 1, 'VALUE', NULL, NULL, 0, 2550, 'お酒を飲む頻度'),
('heartcross.exam.9n791000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N791000000000011', NULL, '9N791000000000011', 1, 'VALUE', NULL, NULL, 0, 2560, '1日当たりの飲酒量'),
('heartcross.exam.9n796000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N796000000000011', NULL, '9N796000000000011', 1, 'VALUE', NULL, NULL, 0, 2570, '睡眠で休養が十分とれる'),
('heartcross.exam.9n801000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N801000000000011', NULL, '9N801000000000011', 1, 'VALUE', NULL, NULL, 0, 2580, '生活習慣を改善'),
('heartcross.exam.9n701167000000049', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N701167000000049', NULL, '9N701167000000049', 1, 'VALUE', NULL, NULL, 0, 2590, '血圧：薬剤名'),
('heartcross.exam.9n706167000000049', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N706167000000049', NULL, '9N706167000000049', 1, 'VALUE', NULL, NULL, 0, 2600, '血糖：薬剤名'),
('heartcross.exam.9n711167000000049', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N711167000000049', NULL, '9N711167000000049', 1, 'VALUE', NULL, NULL, 0, 2610, '脂質：薬剤名'),
('heartcross.exam.9n701167100000049', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N701167100000049', NULL, '9N701167100000049', 1, 'VALUE', NULL, NULL, 0, 2620, '血圧：服薬理由'),
('heartcross.exam.9n706167100000049', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N706167100000049', NULL, '9N706167100000049', 1, 'VALUE', NULL, NULL, 0, 2630, '血糖：服薬理由'),
('heartcross.exam.9n711167100000049', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N711167100000049', NULL, '9N711167100000049', 1, 'VALUE', NULL, NULL, 0, 2640, '脂質：服薬理由'),
('heartcross.exam.9n251160800000049', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N251160800000049', NULL, '9N251160800000049', 1, 'VALUE', NULL, NULL, 0, 2650, '胸部CT所見1'),
('heartcross.exam.9n271160800000049', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N271160800000049', NULL, '9N271160800000049', 1, 'VALUE', NULL, NULL, 0, 2660, '婦人科内診所見1'),
('heartcross.exam.9n276160800000049', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N276160800000049', NULL, '9N276160800000049', 1, 'VALUE', NULL, NULL, 0, 2670, '乳部触診所見1'),
('heartcross.exam.9n281160800000049', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N281160800000049', NULL, '9N281160800000049', 1, 'VALUE', NULL, NULL, 0, 2680, 'ﾏﾝﾓ所見1'),
('heartcross.exam.9f140160800000049', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9F140160800000049', NULL, '9F140160800000049', 1, 'VALUE', NULL, NULL, 0, 2690, '乳部エコー所見1'),
('heartcross.exam.9n291160800000049', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N291160800000049', NULL, '9N291160800000049', 1, 'VALUE', NULL, NULL, 0, 2700, '子宮細胞診(頚部)所見1'),
('heartcross.exam.1b030000001599811', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '1B030000001599811', NULL, '1B030000001599811', 1, 'VALUE', NULL, NULL, 0, 2710, '便潜血判定'),
('heartcross.exam.5d305000002399811', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '5D305000002399811', NULL, '5D305000002399811', 1, 'VALUE', NULL, NULL, 0, 2720, '前立腺特異抗原(PSA)'),
('heartcross.exam.9d100163100000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9D100163100000011', NULL, '9D100163100000011', 1, 'VALUE', NULL, NULL, 0, 2730, '聴力(右)1000'),
('heartcross.exam.9d100163200000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9D100163200000011', NULL, '9D100163200000011', 1, 'VALUE', NULL, NULL, 0, 2740, '聴力(右)4000'),
('heartcross.exam.9d100163500000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9D100163500000011', NULL, '9D100163500000011', 1, 'VALUE', NULL, NULL, 0, 2750, '聴力(左)1000'),
('heartcross.exam.9d100163600000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9D100163600000011', NULL, '9D100163600000011', 1, 'VALUE', NULL, NULL, 0, 2760, '聴力(左)4000'),
('heartcross.exam.9d100164000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9D100164000000011', NULL, '9D100164000000011', 1, 'VALUE', NULL, NULL, 0, 2770, '検査法'),
('heartcross.exam.5h010000001910111', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '5H010000001910111', NULL, '5H010000001910111', 1, 'VALUE', NULL, NULL, 0, 2780, '血液型ABO'),
('heartcross.exam.5h020000001910111', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '5H020000001910111', NULL, '5H020000001910111', 1, 'VALUE', NULL, NULL, 0, 2790, '血液型Rh'),
('heartcross.exam.9a110161600000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9A110161600000011', NULL, '9A110161600000011', 1, 'VALUE', NULL, NULL, 0, 2800, '心電図対象者'),
('heartcross.exam.9e100161600000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9E100161600000011', NULL, '9E100161600000011', 1, 'VALUE', NULL, NULL, 0, 2810, '眼底対象者'),
('heartcross.exam.3c015161002399949', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3C015161002399949', NULL, '3C015161002399949', 1, 'VALUE', NULL, NULL, 0, 2820, '血清ｸﾚｱﾁﾆﾝ実施理由'),
('heartcross.exam.3c015161602399911', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3C015161602399911', NULL, '3C015161602399911', 1, 'VALUE', NULL, NULL, 0, 2830, '血清クレアチニン対象者'),
('heartcross.exam.7a021165208543311', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '7A021165208543311', NULL, '7A021165208543311', 1, 'VALUE', NULL, NULL, 0, 2840, '子宮細胞診(頚部)ベセスダ'),
('heartcross.exam.3a015000000106101', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3A015000000106101', NULL, '3A015000000106101', 1, 'VALUE', NULL, NULL, 0, 2850, '尿中ｱﾙﾌﾞﾐﾝ定量'),
('heartcross.exam.3a015000000106128', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3A015000000106128', NULL, '3A015000000106128', 1, 'VALUE', NULL, NULL, 0, 2860, '尿中アルブミンクレアチニン補正値／アルブミン指数'),
('heartcross.exam.3a015000000406126', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '3A015000000406126', NULL, '3A015000000406126', 1, 'VALUE', NULL, NULL, 0, 2870, '尿中アルブミン一日量'),
('heartcross.exam.9n950000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N950000000000011', NULL, '9N950000000000011', 1, 'VALUE', NULL, NULL, 0, 2880, '情報提供の方法'),
('heartcross.exam.9n807000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N807000000000011', NULL, '9N807000000000011', 1, 'VALUE', NULL, NULL, 0, 2890, '健診当日初回面接実施'),
('heartcross.exam.9n932000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N932000000000011', NULL, '9N932000000000011', 1, 'VALUE', NULL, NULL, 0, 2900, 'あなたの現在の健康状態はいかがですか'),
('heartcross.exam.9n933000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N933000000000011', NULL, '9N933000000000011', 1, 'VALUE', NULL, NULL, 0, 2910, '毎日の生活に満足していますか'),
('heartcross.exam.9n934000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N934000000000011', NULL, '9N934000000000011', 1, 'VALUE', NULL, NULL, 0, 2920, '１日３食きちんと食べていますか'),
('heartcross.exam.9n935000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N935000000000011', NULL, '9N935000000000011', 1, 'VALUE', NULL, NULL, 0, 2930, '半年前に比べて固いもの(*)が食べにくくなりましたか'),
('heartcross.exam.9n936000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N936000000000011', NULL, '9N936000000000011', 1, 'VALUE', NULL, NULL, 0, 2940, 'お茶や汁物等でむせることがありますか'),
('heartcross.exam.9n937000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N937000000000011', NULL, '9N937000000000011', 1, 'VALUE', NULL, NULL, 0, 2950, '６カ月間で２～３kg以上の体重減少がありましたか'),
('heartcross.exam.9n938000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N938000000000011', NULL, '9N938000000000011', 1, 'VALUE', NULL, NULL, 0, 2960, '以前に比べて歩く速度が遅くなってきたと思いますか'),
('heartcross.exam.9n939000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N939000000000011', NULL, '9N939000000000011', 1, 'VALUE', NULL, NULL, 0, 2970, 'この1年間に転んだことがありますか'),
('heartcross.exam.9n940000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N940000000000011', NULL, '9N940000000000011', 1, 'VALUE', NULL, NULL, 0, 2980, 'ウォーキング等の運動を週に1回以上していますか'),
('heartcross.exam.9n941000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N941000000000011', NULL, '9N941000000000011', 1, 'VALUE', NULL, NULL, 0, 2990, '周りの人から「いつも同じことを聞く」などの物忘れがあると言われていますか'),
('heartcross.exam.9n942000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N942000000000011', NULL, '9N942000000000011', 1, 'VALUE', NULL, NULL, 0, 3000, '今日が何月何日かわからない時がありますか'),
('heartcross.exam.9n943000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N943000000000011', NULL, '9N943000000000011', 1, 'VALUE', NULL, NULL, 0, 3010, 'あなたはたばこを吸いますか'),
('heartcross.exam.9n944000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N944000000000011', NULL, '9N944000000000011', 1, 'VALUE', NULL, NULL, 0, 3020, '週に1回以上は外出していますか'),
('heartcross.exam.9n945000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N945000000000011', NULL, '9N945000000000011', 1, 'VALUE', NULL, NULL, 0, 3030, 'ふだんから家族や友人と付き合いがありますか'),
('heartcross.exam.9n946000000000011', 'HEARTCROSS', 'EXAM_ITEM_VALUE', NULL, '9N946000000000011', NULL, '9N946000000000011', 1, 'VALUE', NULL, NULL, 0, 3040, '体調が悪いときに、身近に相談できる人がいますか');

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

-- Heartcross ECG/chest X-ray judgement text is used only to derive the
-- standard finding-presence CD. The facility judgement itself is not stored.
INSERT INTO `phr_master`.`csv_exam_result_mapping_rules` (
  `csv_format_version_id`, `rule_key`, `target_kind`, `target_resolution_type`, `selection_mode`,
  `target_namecode`, `method_structure_type`, `value_source_type`, `fixed_value`,
  `is_required`, `priority`, `is_active`, `note`
) VALUES
  (@heartcross_csv_format_version_id, 'heartcross.exam.ecg_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT',
   '9A110160700000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 1370, 1,
   'draft seed:heartcross.exam.ecg_presence_normal:異常なし -> 所見なし CD=2'),
  (@heartcross_csv_format_version_id, 'heartcross.exam.ecg_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT',
   '9A110160700000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 1380, 1,
   'draft seed:heartcross.exam.ecg_presence_abnormal:異常判定かつ所見あり -> 所見あり CD=1'),
  (@heartcross_csv_format_version_id, 'heartcross.exam.chest_xray_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT',
   '9N206160700000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 1390, 1,
   'draft seed:heartcross.exam.chest_xray_presence_normal:異常なし -> 所見なし CD=2'),
  (@heartcross_csv_format_version_id, 'heartcross.exam.chest_xray_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT',
   '9N206160700000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 1400, 1,
   'draft seed:heartcross.exam.chest_xray_presence_abnormal:異常判定かつ所見あり -> 所見あり CD=1'),
  (@heartcross_csv_format_version_id, 'heartcross.exam.medical_history_presence_empty', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT',
   '9N056000000000011', 'MULTI_COLUMN', 'FIXED', '2', 0, 1410, 1,
   'draft seed:heartcross.exam.medical_history_presence_empty:対応2列が空欄 -> 既往歴なし CD=2'),
  (@heartcross_csv_format_version_id, 'heartcross.exam.subjective_symptoms_presence_empty', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT',
   '9N061000000000011', 'MULTI_COLUMN', 'FIXED', '2', 0, 1420, 1,
   'draft seed:heartcross.exam.subjective_symptoms_presence_empty:対応2列が空欄 -> 自覚症状なし CD=2'),
  (@heartcross_csv_format_version_id, 'heartcross.exam.objective_symptoms_presence_empty', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT',
   '9N066000000000011', 'MULTI_COLUMN', 'FIXED', '2', 0, 1430, 1,
   'draft seed:heartcross.exam.objective_symptoms_presence_empty:対応2列が空欄 -> 他覚症状なし CD=2')
ON DUPLICATE KEY UPDATE
  `target_kind` = VALUES(`target_kind`),
  `target_resolution_type` = VALUES(`target_resolution_type`),
  `selection_mode` = VALUES(`selection_mode`),
  `target_namecode` = VALUES(`target_namecode`),
  `method_structure_type` = VALUES(`method_structure_type`),
  `value_source_type` = VALUES(`value_source_type`),
  `fixed_value` = VALUES(`fixed_value`),
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

-- For these three paired finding fields, both configured cells being empty is
-- an explicit no-finding result. The one abnormal objective-symptom sample is
-- intentionally left on the existing direct rule pending facility response.
INSERT INTO `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_rule_id`, `condition_group_no`, `condition_type`,
  `locator_type`, `header_name`, `header_occurrence`, `operator`, `expected_value`,
  `source_role`, `priority`, `is_active`, `note`
)
SELECT r.`csv_exam_result_mapping_rule_id`, 1, 'CELL_VALUE', 'HEADER_NAME', x.`header_name`, 1,
       'EMPTY', NULL, 'QUALIFIER', x.`priority`, 1,
       CONCAT('draft paired empty condition:', r.`rule_key`)
FROM `phr_master`.`csv_exam_result_mapping_rules` r
JOIN (
  SELECT 'heartcross.exam.medical_history_presence_empty' AS `rule_key`, '9N056000000000011' AS `header_name`, 100 AS `priority`
  UNION ALL SELECT 'heartcross.exam.medical_history_presence_empty', '9N056160400000049', 110
  UNION ALL SELECT 'heartcross.exam.subjective_symptoms_presence_empty', '9N061000000000011', 100
  UNION ALL SELECT 'heartcross.exam.subjective_symptoms_presence_empty', '9N061160800000049', 110
  UNION ALL SELECT 'heartcross.exam.objective_symptoms_presence_empty', '9N066000000000011', 100
  UNION ALL SELECT 'heartcross.exam.objective_symptoms_presence_empty', '9N066160800000049', 110
) x ON x.`rule_key` = r.`rule_key`
WHERE r.`csv_format_version_id` = @heartcross_csv_format_version_id;

-- Branch conditions for standard finding-presence CD values.
INSERT INTO `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_rule_id`, `condition_group_no`, `condition_type`,
  `locator_type`, `header_name`, `header_occurrence`, `operator`, `expected_value`,
  `source_role`, `priority`, `is_active`, `note`
)
SELECT r.`csv_exam_result_mapping_rule_id`, 1, 'CELL_VALUE', 'HEADER_NAME', x.`header_name`, 1,
       x.`operator`, x.`expected_value`, 'QUALIFIER', x.`priority`, 1,
       CONCAT('draft derived condition:', r.`rule_key`)
FROM `phr_master`.`csv_exam_result_mapping_rules` r
JOIN (
  SELECT 'heartcross.exam.ecg_presence_normal' AS `rule_key`, '9A110160700000011' AS `header_name`,
         'EQUALS' AS `operator`, '異常なし' AS `expected_value`, 100 AS `priority`
  UNION ALL SELECT 'heartcross.exam.ecg_presence_abnormal', '9A110160700000011', 'NOT_EMPTY', NULL, 100
  UNION ALL SELECT 'heartcross.exam.ecg_presence_abnormal', '9A110160700000011', 'NOT_EQUALS', '異常なし', 110
  UNION ALL SELECT 'heartcross.exam.chest_xray_presence_normal', '9N206160700000011', 'EQUALS', '異常なし', 100
  UNION ALL SELECT 'heartcross.exam.chest_xray_presence_abnormal', '9N206160700000011', 'NOT_EMPTY', NULL, 100
  UNION ALL SELECT 'heartcross.exam.chest_xray_presence_abnormal', '9N206160700000011', 'NOT_EQUALS', '異常なし', 110
) x ON x.`rule_key` = r.`rule_key`
WHERE r.`csv_format_version_id` = @heartcross_csv_format_version_id;

-- Store finding text only for abnormal judgement rows.
INSERT INTO `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_rule_id`, `condition_group_no`, `condition_type`,
  `locator_type`, `header_name`, `header_occurrence`, `operator`, `expected_value`,
  `source_role`, `priority`, `is_active`, `note`
)
SELECT r.`csv_exam_result_mapping_rule_id`, 1, 'CELL_VALUE', 'HEADER_NAME', x.`header_name`, 1,
       x.`operator`, x.`expected_value`, 'QUALIFIER', x.`priority`, 1,
       CONCAT('draft abnormal finding condition:', r.`rule_key`)
FROM `phr_master`.`csv_exam_result_mapping_rules` r
JOIN (
  SELECT 'heartcross.exam.ecg_finding_text' AS `rule_key`, '9A110160700000011' AS `header_name`,
         'NOT_EMPTY' AS `operator`, NULL AS `expected_value`, 110 AS `priority`
  UNION ALL SELECT 'heartcross.exam.ecg_finding_text', '9A110160700000011', 'NOT_EQUALS', '異常なし', 120
  UNION ALL SELECT 'heartcross.exam.chest_xray_finding_text', '9N206160700000011', 'NOT_EMPTY', NULL, 110
  UNION ALL SELECT 'heartcross.exam.chest_xray_finding_text', '9N206160700000011', 'NOT_EQUALS', '異常なし', 120
) x ON x.`rule_key` = r.`rule_key`
WHERE r.`csv_format_version_id` = @heartcross_csv_format_version_id;

-- ============================================================
-- Oroku Hospital / joined sample 001
-- ============================================================
--
-- Oroku sample is a joined CSV. The second source starts at column 124 and is
-- used as the template base. Insurance symbol/number are expected as appended
-- trailing columns in production; insurer number is carried by file_receipts.

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
  `encoding_fallback_policy`,
  `delimiter`,
  `quote_char`,
  `note`,
  `is_active`
) VALUES (
  @oroku_exam_facility_id,
  'OROKU_2026_05_JOINED_PATTERN_C_V1',
  'CSV',
  '小禄病院 2026-05 joined sample Pattern C',
  1,
  'SINGLE',
  'SIMPLE_HEADER',
  'NONE',
  1,
  2,
  'd2c027cceebffc5ebc22407896cab777db3f021b1be701650413e8f3da4f609c',
  'VERIFIED',
  'ALLOW_AFTER_CONFIRM',
  0,
  'SKIP_CHECKED_OK',
  'IMPORT_AND_CHECK_LATER',
  'utf-8-sig',
  'ALLOW_COMMON_ENCODINGS',
  ',',
  '"',
  'draft seed: oroku joined sample. second source is template base; insurance symbol/number are appended trailing columns; insurer number comes from file_receipts.',
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
  `encoding_fallback_policy` = VALUES(`encoding_fallback_policy`),
  `delimiter` = VALUES(`delimiter`),
  `quote_char` = VALUES(`quote_char`),
  `note` = VALUES(`note`),
  `is_active` = VALUES(`is_active`),
  `updated_at` = CURRENT_TIMESTAMP(3);

SELECT `csv_format_version_id`
  INTO @oroku_csv_format_version_id
FROM `phr_master`.`csv_format_versions`
WHERE `exam_facility_id` = @oroku_exam_facility_id
  AND `mapping_version` = 'OROKU_2026_05_JOINED_PATTERN_C_V1'
LIMIT 1;

-- CSVの「医療機関コード」は施設内コードであり、健診機関を識別できない。
-- facility_codeはscan時にexam_facilitiesからfile_receiptsへ保存した値を使用する。
UPDATE `phr_master`.`csv_exam_result_mapping_conditions` c
JOIN `phr_master`.`csv_exam_result_mapping_rules` r
  ON r.`csv_exam_result_mapping_rule_id` = c.`csv_exam_result_mapping_rule_id`
SET c.`is_active` = 0,
    c.`note` = 'disabled: use exam_facilities snapshot from file_receipts',
    c.`updated_at` = CURRENT_TIMESTAMP(3)
WHERE r.`csv_format_version_id` = @oroku_csv_format_version_id
  AND r.`rule_key` = 'oroku.basic.facility_code';

UPDATE `phr_master`.`csv_exam_result_mapping_rules`
SET `is_active` = 0,
    `note` = 'disabled: CSV medical institution code is facility-local; use exam_facilities snapshot from file_receipts',
    `updated_at` = CURRENT_TIMESTAMP(3)
WHERE `csv_format_version_id` = @oroku_csv_format_version_id
  AND `rule_key` = 'oroku.basic.facility_code';

-- 健診コースコードは施設内コードであり、厚生労働省プログラムコードではない。
-- program_codeはeventの年齢基準日を使う共通判定で補完する。
UPDATE `phr_master`.`csv_exam_result_mapping_conditions` c
JOIN `phr_master`.`csv_exam_result_mapping_rules` r
  ON r.`csv_exam_result_mapping_rule_id` = c.`csv_exam_result_mapping_rule_id`
SET c.`is_active` = 0,
    c.`note` = 'disabled: facility-local course code is not an MHLW program code',
    c.`updated_at` = CURRENT_TIMESTAMP(3)
WHERE r.`csv_format_version_id` = @oroku_csv_format_version_id
  AND r.`rule_key` = 'oroku.basic.program_code';

UPDATE `phr_master`.`csv_exam_result_mapping_rules`
SET `is_active` = 0,
    `note` = 'disabled: facility-local course code is not an MHLW program code; derive from event age rule',
    `updated_at` = CURRENT_TIMESTAMP(3)
WHERE `csv_format_version_id` = @oroku_csv_format_version_id
  AND `rule_key` = 'oroku.basic.program_code';

DELETE FROM `tmp_csv_exam_mapping_seed`;

INSERT INTO `tmp_csv_exam_mapping_seed` (
  `seed_key`, `format_key`, `target_kind`, `target_field`, `target_namecode`,
  `header_context`, `header_name`, `header_occurrence`, `source_role`,
  `raw_value_type`, `raw_unit`, `is_required`, `priority`, `note`
) VALUES
-- Oroku basic information. Insurer number is intentionally not mapped from CSV.
('oroku.basic.exam_date', 'OROKU', 'LEDGER_FIELD', 'exam_date', NULL, NULL, '健診実施日', 1, 'VALUE', NULL, NULL, 1, 10, 'basic: exam date from second source'),
('oroku.basic.name_full_raw', 'OROKU', 'LEDGER_FIELD', 'name_full_raw', NULL, NULL, '氏名（漢字）', 1, 'VALUE', NULL, NULL, 0, 50, 'basic: full name from second source'),
('oroku.basic.name_kana_raw', 'OROKU', 'LEDGER_FIELD', 'name_kana_raw', NULL, NULL, '氏名（カナ）', 1, 'VALUE', NULL, NULL, 1, 60, 'basic: kana name from second source'),
('oroku.basic.gender_raw', 'OROKU', 'LEDGER_FIELD', 'gender_raw', NULL, NULL, '性別', 2, 'VALUE', NULL, NULL, 1, 70, 'basic: gender from second source'),
('oroku.basic.birthdate', 'OROKU', 'LEDGER_FIELD', 'birthdate', NULL, NULL, '生年月日', 2, 'VALUE', NULL, NULL, 1, 80, 'basic: birthdate from second source'),
('oroku.basic.insurance_symbol_raw', 'OROKU', 'LEDGER_FIELD', 'insurance_symbol_raw', NULL, NULL, '保険記号', 1, 'VALUE', NULL, NULL, 1, 120, 'basic: insurance symbol appended for production'),
('oroku.basic.insurance_number_raw', 'OROKU', 'LEDGER_FIELD', 'insurance_number_raw', NULL, NULL, '保険番号', 1, 'VALUE', NULL, NULL, 1, 130, 'basic: insurance number appended for production'),

-- Oroku exam item values. Facility judgement/category columns are not mapped.
('oroku.exam.height', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '9N001000000000001', NULL, '身長', 2, 'VALUE', NULL, NULL, 1, 1000, 'height from second source'),
('oroku.exam.weight', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '9N006000000000001', NULL, '体重', 2, 'VALUE', NULL, NULL, 1, 1010, 'weight from second source'),
('oroku.exam.bmi', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '9N011000000000001', NULL, 'ＢＭＩ', 1, 'VALUE', NULL, NULL, 1, 1020, 'BMI from second source'),
('oroku.exam.waist', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '9N016160100000001', NULL, '腹囲', 1, 'VALUE', NULL, NULL, 1, 1030, 'waist circumference from second source'),
('oroku.exam.sbp_first', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '9A751000000000001', NULL, '血圧座位最高（1回目）', 1, 'VALUE', NULL, NULL, 1, 1040, 'first systolic blood pressure'),
('oroku.exam.dbp_first', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '9A761000000000001', NULL, '血圧座位最低（1回目）', 1, 'VALUE', NULL, NULL, 1, 1050, 'first diastolic blood pressure'),
('oroku.exam.urine_protein', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '1A010000000191111', NULL, '尿蛋白', 2, 'VALUE', NULL, NULL, 1, 1060, 'urine protein machine-read code from second source'),
('oroku.exam.urine_sugar', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '1A020000000191111', NULL, '尿糖', 2, 'VALUE', NULL, NULL, 1, 1070, 'urine sugar machine-read code from second source'),
('oroku.exam.urine_occult_blood', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '1A100000000191111', NULL, '尿潜血', 2, 'VALUE', NULL, NULL, 0, 1080, 'urine occult blood machine-read code from second source'),
('oroku.exam.urine_specific_gravity', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '1A030000000190301', NULL, '尿比重', 1, 'VALUE', NULL, NULL, 0, 1090, 'urine specific gravity from first source'),
('oroku.exam.urine_ph', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '1A990000000300052', NULL, '尿pH', 1, 'VALUE', NULL, NULL, 0, 1100, 'urine pH from first source; JLAC10 1A990-0000-003-000-52'),
('oroku.exam.rbc', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '2A020000001930101', NULL, '赤血球数', 1, 'VALUE', NULL, NULL, 0, 1110, 'red blood cell count'),
('oroku.exam.hemoglobin', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '2A030000001930101', NULL, '血色素量', 1, 'VALUE', NULL, NULL, 0, 1120, 'hemoglobin'),
('oroku.exam.hematocrit', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '2A040000001930102', NULL, 'ヘマトクリット値', 1, 'VALUE', NULL, NULL, 0, 1130, 'hematocrit'),
('oroku.exam.platelet', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '2A050000001930101', NULL, '血小板数', 2, 'VALUE', NULL, NULL, 0, 1140, 'platelet count from second source'),
('oroku.exam.ast', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '3B035000002327201', NULL, 'ＧＯＴ（ＡＳＴ）', 1, 'VALUE', NULL, NULL, 1, 1150, 'AST JSCC'),
('oroku.exam.alt', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '3B045000002327201', NULL, 'ＧＰＴ（ＡＬＴ）', 1, 'VALUE', NULL, NULL, 1, 1160, 'ALT JSCC'),
('oroku.exam.ggt', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '3B090000002327101', NULL, 'γ－ＧＴ（γ－ＧＴＰ）', 1, 'VALUE', NULL, NULL, 1, 1170, 'gamma-GTP JSCC'),
('oroku.exam.tg_fasting', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '3F015000002327101', NULL, '空腹時中性脂肪', 1, 'VALUE', NULL, NULL, 1, 1180, 'fasting TG'),
('oroku.exam.hdl', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '3F070000002327101', NULL, 'ＨＤＬ－コレステロール', 1, 'VALUE', NULL, NULL, 1, 1190, 'HDL cholesterol'),
('oroku.exam.ldl', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '3F077000002327101', NULL, 'ＬＤＬ－コレステロール', 1, 'VALUE', NULL, NULL, 1, 1200, 'LDL cholesterol'),
('oroku.exam.non_hdl', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '3F069000002391901', NULL, 'non HDL-コレステロール', 2, 'VALUE', NULL, NULL, 1, 1210, 'non-HDL cholesterol from second source'),
('oroku.exam.glucose_fasting', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '3D010000001927201', NULL, '空腹時血糖', 2, 'VALUE', NULL, NULL, 1, 1220, 'fasting glucose from second source'),
('oroku.exam.hba1c', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '3D046000001920402', NULL, 'ＨｂＡ１ｃ', 1, 'VALUE', NULL, NULL, 1, 1230, 'HbA1c HPLC'),
('oroku.exam.creatinine', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '3C015000002327101', NULL, 'クレアチニン', 2, 'VALUE', NULL, NULL, 0, 1240, 'serum creatinine from second source'),
('oroku.exam.egfr', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '8A065000002391901', NULL, 'eGFR', 2, 'VALUE', NULL, NULL, 0, 1250, 'eGFR from second source'),
('oroku.exam.uric_acid', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '3C020000002327101', NULL, '尿酸', 2, 'VALUE', NULL, NULL, 0, 1260, 'serum uric acid from second source'),
('oroku.exam.total_cholesterol', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '3F050000002327101', NULL, '総コレステロール', 2, 'VALUE', NULL, NULL, 0, 1270, 'total cholesterol from second source'),
('oroku.exam.total_protein', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '3A010000002327101', NULL, '血清総蛋白', 1, 'VALUE', NULL, NULL, 0, 1280, 'total protein'),
('oroku.exam.ca19_9', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '5D130000002399901', NULL, 'ＣＡ１９－９', 1, 'VALUE', NULL, NULL, 0, 1290, 'CA19-9 tumor marker from second source'),
('oroku.exam.ca125', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '5D100000002399901', NULL, 'ＣＡ１２５', 1, 'VALUE', NULL, NULL, 0, 1300, 'CA125 tumor marker from second source'),
('oroku.exam.vision_right_uncorrected', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '9E160162100000001', NULL, '視力裸眼（右）', 1, 'VALUE', NULL, NULL, 0, 1310, 'uncorrected visual acuity right'),
('oroku.exam.vision_left_uncorrected', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '9E160162200000001', NULL, '視力裸眼（左）', 1, 'VALUE', NULL, NULL, 0, 1320, 'uncorrected visual acuity left'),
('oroku.exam.vision_right_corrected', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '9E160162500000001', NULL, '視力矯正（右）', 1, 'VALUE', NULL, NULL, 0, 1330, 'corrected visual acuity right'),
('oroku.exam.vision_left_corrected', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '9E160162600000001', NULL, '視力矯正（左）', 1, 'VALUE', NULL, NULL, 0, 1340, 'corrected visual acuity left'),
('oroku.exam.hearing_right_1000', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '9D100163100000011', NULL, 'オージオ（右）1000Ｈｚ', 1, 'VALUE', NULL, NULL, 0, 1350, 'hearing right 1000Hz explicit finding code from second source'),
('oroku.exam.hearing_left_1000', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '9D100163500000011', NULL, 'オージオ（左）1000Ｈｚ', 1, 'VALUE', NULL, NULL, 0, 1360, 'hearing left 1000Hz explicit finding code from second source'),
('oroku.exam.hearing_right_4000', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '9D100163200000011', NULL, 'オージオ（右）4000Ｈｚ', 1, 'VALUE', NULL, NULL, 0, 1370, 'hearing right 4000Hz explicit finding code from second source'),
('oroku.exam.hearing_left_4000', 'OROKU', 'EXAM_ITEM_VALUE', NULL, '9D100163600000011', NULL, 'オージオ（左）4000Ｈｚ', 1, 'VALUE', NULL, NULL, 0, 1380, 'hearing left 4000Hz explicit finding code from second source');

INSERT INTO `phr_master`.`csv_exam_result_mapping_rules` (
  `csv_format_version_id`, `rule_key`, `target_kind`, `target_resolution_type`, `selection_mode`,
  `selection_group_code`, `target_namecode`, `target_identity_item_code`, `target_field`,
  `method_structure_type`, `raw_value_type`, `raw_unit`,
  `is_required`, `priority`, `is_active`, `note`
)
SELECT
  @oroku_csv_format_version_id,
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
WHERE `format_key` = 'OROKU'
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

-- Oroku finding text and derived finding-presence CD values.
-- Finding text is a result value. Multi-column findings are joined in one ST
-- value when at least one source column has an abnormal finding.
INSERT INTO `phr_master`.`csv_exam_result_mapping_rules` (
  `csv_format_version_id`, `rule_key`, `target_kind`, `target_resolution_type`, `selection_mode`,
  `target_namecode`, `method_structure_type`, `value_source_type`, `fixed_value`,
  `value_join_separator`, `value_exclude_values`, `is_required`, `priority`, `is_active`, `note`
) VALUES
  (@oroku_csv_format_version_id, 'oroku.exam.fundus_finding_text', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9E100160900000049', 'MULTI_COLUMN', 'SOURCE', NULL, ' / ', '異常なし/', 0, 1800, 1, 'draft seed:oroku.exam.fundus_finding_text:眼底カメラ右左所見を結合'),
  (@oroku_csv_format_version_id, 'oroku.exam.ecg_finding_text', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9A110160800000049', 'SINGLE_COLUMN', 'SOURCE', NULL, NULL, NULL, 0, 1810, 1, 'draft seed:oroku.exam.ecg_finding_text:心電図所見'),
  (@oroku_csv_format_version_id, 'oroku.exam.ecg_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9A110160700000011', 'SINGLE_COLUMN', 'FIXED', '2', NULL, NULL, 0, 1820, 1, 'draft seed:oroku.exam.ecg_presence_normal:異常所見なし -> 所見なし CD=2'),
  (@oroku_csv_format_version_id, 'oroku.exam.ecg_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9A110160700000011', 'SINGLE_COLUMN', 'FIXED', '1', NULL, NULL, 0, 1830, 1, 'draft seed:oroku.exam.ecg_presence_abnormal:所見本文あり -> 所見あり CD=1'),
  (@oroku_csv_format_version_id, 'oroku.exam.chest_xray_finding_text', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N206160800000049', 'SINGLE_COLUMN', 'SOURCE', NULL, NULL, NULL, 0, 1840, 1, 'draft seed:oroku.exam.chest_xray_finding_text:胸部X線所見'),
  (@oroku_csv_format_version_id, 'oroku.exam.chest_xray_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N206160700000011', 'SINGLE_COLUMN', 'FIXED', '2', NULL, NULL, 0, 1850, 1, 'draft seed:oroku.exam.chest_xray_presence_normal:異常所見なし -> 所見なし CD=2'),
  (@oroku_csv_format_version_id, 'oroku.exam.chest_xray_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N206160700000011', 'SINGLE_COLUMN', 'FIXED', '1', NULL, NULL, 0, 1860, 1, 'draft seed:oroku.exam.chest_xray_presence_abnormal:所見本文あり -> 所見あり CD=1'),
  (@oroku_csv_format_version_id, 'oroku.exam.chest_ct_finding_text', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N251160800000049', 'SINGLE_COLUMN', 'SOURCE', NULL, NULL, NULL, 0, 1870, 1, 'draft seed:oroku.exam.chest_ct_finding_text:胸部CT所見'),
  (@oroku_csv_format_version_id, 'oroku.exam.chest_ct_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N251160700000011', 'SINGLE_COLUMN', 'FIXED', '1', NULL, NULL, 0, 1880, 1, 'draft seed:oroku.exam.chest_ct_presence_abnormal:胸部CT所見本文あり -> 所見あり CD=1'),
  (@oroku_csv_format_version_id, 'oroku.exam.gastric_xray_finding_text', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N256160800000049', 'MULTI_COLUMN', 'SOURCE', NULL, ' / ', '異常なし/', 0, 1890, 1, 'draft seed:oroku.exam.gastric_xray_finding_text:胃部X線所見1-3を結合'),
  (@oroku_csv_format_version_id, 'oroku.exam.gastric_xray_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N256160700000011', 'SINGLE_COLUMN', 'FIXED', '2', NULL, NULL, 0, 1900, 1, 'draft seed:oroku.exam.gastric_xray_presence_normal:全所見列が異常なし -> 所見なし CD=2'),
  (@oroku_csv_format_version_id, 'oroku.exam.gastric_xray_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N256160700000011', 'SINGLE_COLUMN', 'FIXED', '1', NULL, NULL, 0, 1910, 1, 'draft seed:oroku.exam.gastric_xray_presence_abnormal:いずれかの所見列に異常本文あり -> 所見あり CD=1'),
  (@oroku_csv_format_version_id, 'oroku.exam.gastric_endoscopy_finding_text', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N266160800000049', 'MULTI_COLUMN', 'SOURCE', NULL, ' / ', '異常なし/', 0, 1920, 1, 'draft seed:oroku.exam.gastric_endoscopy_finding_text:胃内視鏡所見1-3を結合'),
  (@oroku_csv_format_version_id, 'oroku.exam.gastric_endoscopy_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N266160700000011', 'SINGLE_COLUMN', 'FIXED', '2', NULL, NULL, 0, 1930, 1, 'draft seed:oroku.exam.gastric_endoscopy_presence_normal:全所見列が異常なし -> 所見なし CD=2'),
  (@oroku_csv_format_version_id, 'oroku.exam.gastric_endoscopy_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N266160700000011', 'SINGLE_COLUMN', 'FIXED', '1', NULL, NULL, 0, 1940, 1, 'draft seed:oroku.exam.gastric_endoscopy_presence_abnormal:いずれかの所見列に異常本文あり -> 所見あり CD=1'),
  (@oroku_csv_format_version_id, 'oroku.exam.abdominal_ultrasound_finding_text', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9F130160800000049', 'MULTI_COLUMN', 'SOURCE', NULL, ' / ', '異常なし/', 0, 1950, 1, 'draft seed:oroku.exam.abdominal_ultrasound_finding_text:腹部超音波所見1-5を結合'),
  (@oroku_csv_format_version_id, 'oroku.exam.abdominal_ultrasound_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9F130160700000011', 'SINGLE_COLUMN', 'FIXED', '2', NULL, NULL, 0, 1960, 1, 'draft seed:oroku.exam.abdominal_ultrasound_presence_normal:全所見列が異常なし -> 所見なし CD=2'),
  (@oroku_csv_format_version_id, 'oroku.exam.abdominal_ultrasound_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9F130160700000011', 'SINGLE_COLUMN', 'FIXED', '1', NULL, NULL, 0, 1970, 1, 'draft seed:oroku.exam.abdominal_ultrasound_presence_abnormal:いずれかの所見列に異常本文あり -> 所見あり CD=1')
ON DUPLICATE KEY UPDATE
  `target_kind` = VALUES(`target_kind`),
  `target_resolution_type` = VALUES(`target_resolution_type`),
  `selection_mode` = VALUES(`selection_mode`),
  `target_namecode` = VALUES(`target_namecode`),
  `method_structure_type` = VALUES(`method_structure_type`),
  `value_source_type` = VALUES(`value_source_type`),
  `fixed_value` = VALUES(`fixed_value`),
  `value_join_separator` = VALUES(`value_join_separator`),
  `value_exclude_values` = VALUES(`value_exclude_values`),
  `is_required` = VALUES(`is_required`),
  `priority` = VALUES(`priority`),
  `is_active` = VALUES(`is_active`),
  `note` = VALUES(`note`),
  `updated_at` = CURRENT_TIMESTAMP(3);

DELETE c
FROM `phr_master`.`csv_exam_result_mapping_conditions` c
JOIN `phr_master`.`csv_exam_result_mapping_rules` r
  ON r.`csv_exam_result_mapping_rule_id` = c.`csv_exam_result_mapping_rule_id`
WHERE r.`csv_format_version_id` = @oroku_csv_format_version_id;

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
  ON r.`csv_format_version_id` = @oroku_csv_format_version_id
 AND r.`note` LIKE CONCAT('draft seed:', s.`seed_key`, ':%')
WHERE s.`format_key` = 'OROKU';

DROP TEMPORARY TABLE IF EXISTS `tmp_oroku_finding_condition_seed`;

CREATE TEMPORARY TABLE `tmp_oroku_finding_condition_seed` (
  `rule_key` varchar(191) NOT NULL,
  `condition_group_no` int NOT NULL,
  `condition_type` varchar(32) NOT NULL,
  `header_name` varchar(255) NOT NULL,
  `operator` varchar(32) NOT NULL,
  `expected_value` varchar(255) NULL,
  `source_role` varchar(32) NOT NULL,
  `priority` int NOT NULL
) ENGINE=Memory DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs;

INSERT INTO `tmp_oroku_finding_condition_seed` (
  `rule_key`, `condition_group_no`, `condition_type`, `header_name`,
  `operator`, `expected_value`, `source_role`, `priority`
) VALUES
-- Fundus finding text: output one ST when either side has an abnormal finding.
('oroku.exam.fundus_finding_text', 1, 'HEADER_MATCH', '眼底カメラ（右）', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.fundus_finding_text', 1, 'HEADER_MATCH', '眼底カメラ（左）', 'PRESENT', NULL, 'VALUE', 110),
('oroku.exam.fundus_finding_text', 1, 'CELL_VALUE', '眼底カメラ（右）', 'NOT_EMPTY', NULL, 'QUALIFIER', 120),
('oroku.exam.fundus_finding_text', 1, 'CELL_VALUE', '眼底カメラ（右）', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 130),
('oroku.exam.fundus_finding_text', 2, 'HEADER_MATCH', '眼底カメラ（右）', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.fundus_finding_text', 2, 'HEADER_MATCH', '眼底カメラ（左）', 'PRESENT', NULL, 'VALUE', 110),
('oroku.exam.fundus_finding_text', 2, 'CELL_VALUE', '眼底カメラ（左）', 'NOT_EMPTY', NULL, 'QUALIFIER', 120),
('oroku.exam.fundus_finding_text', 2, 'CELL_VALUE', '眼底カメラ（左）', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 130),

-- ECG finding/presence.
('oroku.exam.ecg_finding_text', 1, 'HEADER_MATCH', '心電図　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.ecg_finding_text', 1, 'CELL_VALUE', '心電図　所見1', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
('oroku.exam.ecg_finding_text', 1, 'CELL_VALUE', '心電図　所見1', 'NOT_EQUALS', '異常(所見)なし/', 'QUALIFIER', 120),
('oroku.exam.ecg_presence_normal', 1, 'HEADER_MATCH', '心電図　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.ecg_presence_normal', 1, 'CELL_VALUE', '心電図　所見1', 'EQUALS', '異常(所見)なし/', 'QUALIFIER', 110),
('oroku.exam.ecg_presence_abnormal', 1, 'HEADER_MATCH', '心電図　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.ecg_presence_abnormal', 1, 'CELL_VALUE', '心電図　所見1', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
('oroku.exam.ecg_presence_abnormal', 1, 'CELL_VALUE', '心電図　所見1', 'NOT_EQUALS', '異常(所見)なし/', 'QUALIFIER', 120),

-- Chest X-ray finding/presence.
('oroku.exam.chest_xray_finding_text', 1, 'HEADER_MATCH', '胸部Ｘ線所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.chest_xray_finding_text', 1, 'CELL_VALUE', '胸部Ｘ線所見1', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
('oroku.exam.chest_xray_finding_text', 1, 'CELL_VALUE', '胸部Ｘ線所見1', 'NOT_EQUALS', '異常所見なし/', 'QUALIFIER', 120),
('oroku.exam.chest_xray_presence_normal', 1, 'HEADER_MATCH', '胸部Ｘ線所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.chest_xray_presence_normal', 1, 'CELL_VALUE', '胸部Ｘ線所見1', 'EQUALS', '異常所見なし/', 'QUALIFIER', 110),
('oroku.exam.chest_xray_presence_abnormal', 1, 'HEADER_MATCH', '胸部Ｘ線所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.chest_xray_presence_abnormal', 1, 'CELL_VALUE', '胸部Ｘ線所見1', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
('oroku.exam.chest_xray_presence_abnormal', 1, 'CELL_VALUE', '胸部Ｘ線所見1', 'NOT_EQUALS', '異常所見なし/', 'QUALIFIER', 120),

-- Chest CT has finding text only when a finding column is populated.
('oroku.exam.chest_ct_finding_text', 1, 'HEADER_MATCH', '胸部ＣＴ　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.chest_ct_finding_text', 1, 'CELL_VALUE', '胸部ＣＴ　所見1', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
('oroku.exam.chest_ct_presence_abnormal', 1, 'HEADER_MATCH', '胸部ＣＴ　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.chest_ct_presence_abnormal', 1, 'CELL_VALUE', '胸部ＣＴ　所見1', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),

-- Gastric X-ray finding text/presence. All source finding columns are joined
-- for the ST rule; each group is triggered by one abnormal source column.
('oroku.exam.gastric_xray_finding_text', 1, 'HEADER_MATCH', '胃部Ｘ線　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.gastric_xray_finding_text', 1, 'HEADER_MATCH', '胃部Ｘ線　所見2', 'PRESENT', NULL, 'VALUE', 110),
('oroku.exam.gastric_xray_finding_text', 1, 'HEADER_MATCH', '胃部Ｘ線　所見3', 'PRESENT', NULL, 'VALUE', 120),
('oroku.exam.gastric_xray_finding_text', 1, 'CELL_VALUE', '胃部Ｘ線　所見1', 'NOT_EMPTY', NULL, 'QUALIFIER', 130),
('oroku.exam.gastric_xray_finding_text', 1, 'CELL_VALUE', '胃部Ｘ線　所見1', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 140),
('oroku.exam.gastric_xray_finding_text', 2, 'HEADER_MATCH', '胃部Ｘ線　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.gastric_xray_finding_text', 2, 'HEADER_MATCH', '胃部Ｘ線　所見2', 'PRESENT', NULL, 'VALUE', 110),
('oroku.exam.gastric_xray_finding_text', 2, 'HEADER_MATCH', '胃部Ｘ線　所見3', 'PRESENT', NULL, 'VALUE', 120),
('oroku.exam.gastric_xray_finding_text', 2, 'CELL_VALUE', '胃部Ｘ線　所見2', 'NOT_EMPTY', NULL, 'QUALIFIER', 130),
('oroku.exam.gastric_xray_finding_text', 2, 'CELL_VALUE', '胃部Ｘ線　所見2', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 140),
('oroku.exam.gastric_xray_finding_text', 3, 'HEADER_MATCH', '胃部Ｘ線　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.gastric_xray_finding_text', 3, 'HEADER_MATCH', '胃部Ｘ線　所見2', 'PRESENT', NULL, 'VALUE', 110),
('oroku.exam.gastric_xray_finding_text', 3, 'HEADER_MATCH', '胃部Ｘ線　所見3', 'PRESENT', NULL, 'VALUE', 120),
('oroku.exam.gastric_xray_finding_text', 3, 'CELL_VALUE', '胃部Ｘ線　所見3', 'NOT_EMPTY', NULL, 'QUALIFIER', 130),
('oroku.exam.gastric_xray_finding_text', 3, 'CELL_VALUE', '胃部Ｘ線　所見3', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 140),
('oroku.exam.gastric_xray_presence_normal', 1, 'HEADER_MATCH', '胃部Ｘ線　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.gastric_xray_presence_normal', 1, 'CELL_VALUE', '胃部Ｘ線　所見1', 'EQUALS', '異常なし/', 'QUALIFIER', 110),
('oroku.exam.gastric_xray_presence_normal', 1, 'CELL_VALUE', '胃部Ｘ線　所見2', 'EQUALS', '異常なし/', 'QUALIFIER', 120),
('oroku.exam.gastric_xray_presence_normal', 1, 'CELL_VALUE', '胃部Ｘ線　所見3', 'EQUALS', '異常なし/', 'QUALIFIER', 130),
('oroku.exam.gastric_xray_presence_abnormal', 1, 'HEADER_MATCH', '胃部Ｘ線　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.gastric_xray_presence_abnormal', 1, 'CELL_VALUE', '胃部Ｘ線　所見1', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
('oroku.exam.gastric_xray_presence_abnormal', 1, 'CELL_VALUE', '胃部Ｘ線　所見1', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 120),
('oroku.exam.gastric_xray_presence_abnormal', 2, 'HEADER_MATCH', '胃部Ｘ線　所見2', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.gastric_xray_presence_abnormal', 2, 'CELL_VALUE', '胃部Ｘ線　所見2', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
('oroku.exam.gastric_xray_presence_abnormal', 2, 'CELL_VALUE', '胃部Ｘ線　所見2', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 120),
('oroku.exam.gastric_xray_presence_abnormal', 3, 'HEADER_MATCH', '胃部Ｘ線　所見3', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.gastric_xray_presence_abnormal', 3, 'CELL_VALUE', '胃部Ｘ線　所見3', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
('oroku.exam.gastric_xray_presence_abnormal', 3, 'CELL_VALUE', '胃部Ｘ線　所見3', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 120),

-- Gastric endoscopy finding text/presence.
('oroku.exam.gastric_endoscopy_finding_text', 1, 'HEADER_MATCH', '胃内視鏡　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.gastric_endoscopy_finding_text', 1, 'HEADER_MATCH', '胃内視鏡　所見2', 'PRESENT', NULL, 'VALUE', 110),
('oroku.exam.gastric_endoscopy_finding_text', 1, 'HEADER_MATCH', '胃内視鏡　所見3', 'PRESENT', NULL, 'VALUE', 120),
('oroku.exam.gastric_endoscopy_finding_text', 1, 'CELL_VALUE', '胃内視鏡　所見1', 'NOT_EMPTY', NULL, 'QUALIFIER', 130),
('oroku.exam.gastric_endoscopy_finding_text', 1, 'CELL_VALUE', '胃内視鏡　所見1', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 140),
('oroku.exam.gastric_endoscopy_finding_text', 2, 'HEADER_MATCH', '胃内視鏡　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.gastric_endoscopy_finding_text', 2, 'HEADER_MATCH', '胃内視鏡　所見2', 'PRESENT', NULL, 'VALUE', 110),
('oroku.exam.gastric_endoscopy_finding_text', 2, 'HEADER_MATCH', '胃内視鏡　所見3', 'PRESENT', NULL, 'VALUE', 120),
('oroku.exam.gastric_endoscopy_finding_text', 2, 'CELL_VALUE', '胃内視鏡　所見2', 'NOT_EMPTY', NULL, 'QUALIFIER', 130),
('oroku.exam.gastric_endoscopy_finding_text', 2, 'CELL_VALUE', '胃内視鏡　所見2', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 140),
('oroku.exam.gastric_endoscopy_finding_text', 3, 'HEADER_MATCH', '胃内視鏡　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.gastric_endoscopy_finding_text', 3, 'HEADER_MATCH', '胃内視鏡　所見2', 'PRESENT', NULL, 'VALUE', 110),
('oroku.exam.gastric_endoscopy_finding_text', 3, 'HEADER_MATCH', '胃内視鏡　所見3', 'PRESENT', NULL, 'VALUE', 120),
('oroku.exam.gastric_endoscopy_finding_text', 3, 'CELL_VALUE', '胃内視鏡　所見3', 'NOT_EMPTY', NULL, 'QUALIFIER', 130),
('oroku.exam.gastric_endoscopy_finding_text', 3, 'CELL_VALUE', '胃内視鏡　所見3', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 140),
('oroku.exam.gastric_endoscopy_presence_normal', 1, 'HEADER_MATCH', '胃内視鏡　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.gastric_endoscopy_presence_normal', 1, 'CELL_VALUE', '胃内視鏡　所見1', 'EQUALS', '異常なし/', 'QUALIFIER', 110),
('oroku.exam.gastric_endoscopy_presence_normal', 1, 'CELL_VALUE', '胃内視鏡　所見2', 'EQUALS', '異常なし/', 'QUALIFIER', 120),
('oroku.exam.gastric_endoscopy_presence_normal', 1, 'CELL_VALUE', '胃内視鏡　所見3', 'EQUALS', '異常なし/', 'QUALIFIER', 130),
('oroku.exam.gastric_endoscopy_presence_abnormal', 1, 'HEADER_MATCH', '胃内視鏡　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.gastric_endoscopy_presence_abnormal', 1, 'CELL_VALUE', '胃内視鏡　所見1', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
('oroku.exam.gastric_endoscopy_presence_abnormal', 1, 'CELL_VALUE', '胃内視鏡　所見1', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 120),
('oroku.exam.gastric_endoscopy_presence_abnormal', 2, 'HEADER_MATCH', '胃内視鏡　所見2', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.gastric_endoscopy_presence_abnormal', 2, 'CELL_VALUE', '胃内視鏡　所見2', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
('oroku.exam.gastric_endoscopy_presence_abnormal', 2, 'CELL_VALUE', '胃内視鏡　所見2', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 120),
('oroku.exam.gastric_endoscopy_presence_abnormal', 3, 'HEADER_MATCH', '胃内視鏡　所見3', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.gastric_endoscopy_presence_abnormal', 3, 'CELL_VALUE', '胃内視鏡　所見3', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
('oroku.exam.gastric_endoscopy_presence_abnormal', 3, 'CELL_VALUE', '胃内視鏡　所見3', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 120),

-- Abdominal ultrasound finding text/presence.
('oroku.exam.abdominal_ultrasound_finding_text', 1, 'HEADER_MATCH', '腹部超音波　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.abdominal_ultrasound_finding_text', 1, 'HEADER_MATCH', '腹部超音波　所見2', 'PRESENT', NULL, 'VALUE', 110),
('oroku.exam.abdominal_ultrasound_finding_text', 1, 'HEADER_MATCH', '腹部超音波　所見3', 'PRESENT', NULL, 'VALUE', 120),
('oroku.exam.abdominal_ultrasound_finding_text', 1, 'HEADER_MATCH', '腹部超音波　所見4', 'PRESENT', NULL, 'VALUE', 130),
('oroku.exam.abdominal_ultrasound_finding_text', 1, 'HEADER_MATCH', '腹部超音波　所見5', 'PRESENT', NULL, 'VALUE', 140),
('oroku.exam.abdominal_ultrasound_finding_text', 1, 'CELL_VALUE', '腹部超音波　所見1', 'NOT_EMPTY', NULL, 'QUALIFIER', 150),
('oroku.exam.abdominal_ultrasound_finding_text', 1, 'CELL_VALUE', '腹部超音波　所見1', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 160),
('oroku.exam.abdominal_ultrasound_finding_text', 2, 'HEADER_MATCH', '腹部超音波　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.abdominal_ultrasound_finding_text', 2, 'HEADER_MATCH', '腹部超音波　所見2', 'PRESENT', NULL, 'VALUE', 110),
('oroku.exam.abdominal_ultrasound_finding_text', 2, 'HEADER_MATCH', '腹部超音波　所見3', 'PRESENT', NULL, 'VALUE', 120),
('oroku.exam.abdominal_ultrasound_finding_text', 2, 'HEADER_MATCH', '腹部超音波　所見4', 'PRESENT', NULL, 'VALUE', 130),
('oroku.exam.abdominal_ultrasound_finding_text', 2, 'HEADER_MATCH', '腹部超音波　所見5', 'PRESENT', NULL, 'VALUE', 140),
('oroku.exam.abdominal_ultrasound_finding_text', 2, 'CELL_VALUE', '腹部超音波　所見2', 'NOT_EMPTY', NULL, 'QUALIFIER', 150),
('oroku.exam.abdominal_ultrasound_finding_text', 2, 'CELL_VALUE', '腹部超音波　所見2', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 160),
('oroku.exam.abdominal_ultrasound_finding_text', 3, 'HEADER_MATCH', '腹部超音波　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.abdominal_ultrasound_finding_text', 3, 'HEADER_MATCH', '腹部超音波　所見2', 'PRESENT', NULL, 'VALUE', 110),
('oroku.exam.abdominal_ultrasound_finding_text', 3, 'HEADER_MATCH', '腹部超音波　所見3', 'PRESENT', NULL, 'VALUE', 120),
('oroku.exam.abdominal_ultrasound_finding_text', 3, 'HEADER_MATCH', '腹部超音波　所見4', 'PRESENT', NULL, 'VALUE', 130),
('oroku.exam.abdominal_ultrasound_finding_text', 3, 'HEADER_MATCH', '腹部超音波　所見5', 'PRESENT', NULL, 'VALUE', 140),
('oroku.exam.abdominal_ultrasound_finding_text', 3, 'CELL_VALUE', '腹部超音波　所見3', 'NOT_EMPTY', NULL, 'QUALIFIER', 150),
('oroku.exam.abdominal_ultrasound_finding_text', 3, 'CELL_VALUE', '腹部超音波　所見3', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 160),
('oroku.exam.abdominal_ultrasound_finding_text', 4, 'HEADER_MATCH', '腹部超音波　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.abdominal_ultrasound_finding_text', 4, 'HEADER_MATCH', '腹部超音波　所見2', 'PRESENT', NULL, 'VALUE', 110),
('oroku.exam.abdominal_ultrasound_finding_text', 4, 'HEADER_MATCH', '腹部超音波　所見3', 'PRESENT', NULL, 'VALUE', 120),
('oroku.exam.abdominal_ultrasound_finding_text', 4, 'HEADER_MATCH', '腹部超音波　所見4', 'PRESENT', NULL, 'VALUE', 130),
('oroku.exam.abdominal_ultrasound_finding_text', 4, 'HEADER_MATCH', '腹部超音波　所見5', 'PRESENT', NULL, 'VALUE', 140),
('oroku.exam.abdominal_ultrasound_finding_text', 4, 'CELL_VALUE', '腹部超音波　所見4', 'NOT_EMPTY', NULL, 'QUALIFIER', 150),
('oroku.exam.abdominal_ultrasound_finding_text', 4, 'CELL_VALUE', '腹部超音波　所見4', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 160),
('oroku.exam.abdominal_ultrasound_finding_text', 5, 'HEADER_MATCH', '腹部超音波　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.abdominal_ultrasound_finding_text', 5, 'HEADER_MATCH', '腹部超音波　所見2', 'PRESENT', NULL, 'VALUE', 110),
('oroku.exam.abdominal_ultrasound_finding_text', 5, 'HEADER_MATCH', '腹部超音波　所見3', 'PRESENT', NULL, 'VALUE', 120),
('oroku.exam.abdominal_ultrasound_finding_text', 5, 'HEADER_MATCH', '腹部超音波　所見4', 'PRESENT', NULL, 'VALUE', 130),
('oroku.exam.abdominal_ultrasound_finding_text', 5, 'HEADER_MATCH', '腹部超音波　所見5', 'PRESENT', NULL, 'VALUE', 140),
('oroku.exam.abdominal_ultrasound_finding_text', 5, 'CELL_VALUE', '腹部超音波　所見5', 'NOT_EMPTY', NULL, 'QUALIFIER', 150),
('oroku.exam.abdominal_ultrasound_finding_text', 5, 'CELL_VALUE', '腹部超音波　所見5', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 160),
('oroku.exam.abdominal_ultrasound_presence_normal', 1, 'HEADER_MATCH', '腹部超音波　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.abdominal_ultrasound_presence_normal', 1, 'CELL_VALUE', '腹部超音波　所見1', 'EQUALS', '異常なし/', 'QUALIFIER', 110),
('oroku.exam.abdominal_ultrasound_presence_normal', 1, 'CELL_VALUE', '腹部超音波　所見2', 'EQUALS', '異常なし/', 'QUALIFIER', 120),
('oroku.exam.abdominal_ultrasound_presence_normal', 1, 'CELL_VALUE', '腹部超音波　所見3', 'EQUALS', '異常なし/', 'QUALIFIER', 130),
('oroku.exam.abdominal_ultrasound_presence_normal', 1, 'CELL_VALUE', '腹部超音波　所見4', 'EQUALS', '異常なし/', 'QUALIFIER', 140),
('oroku.exam.abdominal_ultrasound_presence_normal', 1, 'CELL_VALUE', '腹部超音波　所見5', 'EQUALS', '異常なし/', 'QUALIFIER', 150),
('oroku.exam.abdominal_ultrasound_presence_abnormal', 1, 'HEADER_MATCH', '腹部超音波　所見1', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.abdominal_ultrasound_presence_abnormal', 1, 'CELL_VALUE', '腹部超音波　所見1', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
('oroku.exam.abdominal_ultrasound_presence_abnormal', 1, 'CELL_VALUE', '腹部超音波　所見1', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 120),
('oroku.exam.abdominal_ultrasound_presence_abnormal', 2, 'HEADER_MATCH', '腹部超音波　所見2', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.abdominal_ultrasound_presence_abnormal', 2, 'CELL_VALUE', '腹部超音波　所見2', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
('oroku.exam.abdominal_ultrasound_presence_abnormal', 2, 'CELL_VALUE', '腹部超音波　所見2', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 120),
('oroku.exam.abdominal_ultrasound_presence_abnormal', 3, 'HEADER_MATCH', '腹部超音波　所見3', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.abdominal_ultrasound_presence_abnormal', 3, 'CELL_VALUE', '腹部超音波　所見3', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
('oroku.exam.abdominal_ultrasound_presence_abnormal', 3, 'CELL_VALUE', '腹部超音波　所見3', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 120),
('oroku.exam.abdominal_ultrasound_presence_abnormal', 4, 'HEADER_MATCH', '腹部超音波　所見4', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.abdominal_ultrasound_presence_abnormal', 4, 'CELL_VALUE', '腹部超音波　所見4', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
('oroku.exam.abdominal_ultrasound_presence_abnormal', 4, 'CELL_VALUE', '腹部超音波　所見4', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 120),
('oroku.exam.abdominal_ultrasound_presence_abnormal', 5, 'HEADER_MATCH', '腹部超音波　所見5', 'PRESENT', NULL, 'VALUE', 100),
('oroku.exam.abdominal_ultrasound_presence_abnormal', 5, 'CELL_VALUE', '腹部超音波　所見5', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
('oroku.exam.abdominal_ultrasound_presence_abnormal', 5, 'CELL_VALUE', '腹部超音波　所見5', 'NOT_EQUALS', '異常なし/', 'QUALIFIER', 120);

INSERT INTO `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_rule_id`, `condition_group_no`, `condition_type`,
  `locator_type`, `header_context`, `header_name`, `header_occurrence`, `column_no`,
  `operator`, `expected_value`, `expected_value_normalized`, `source_role`,
  `priority`, `is_active`, `note`
)
SELECT
  r.`csv_exam_result_mapping_rule_id`,
  s.`condition_group_no`,
  s.`condition_type`,
  'HEADER_NAME',
  NULL,
  s.`header_name`,
  1,
  NULL,
  s.`operator`,
  s.`expected_value`,
  NULL,
  s.`source_role`,
  s.`priority`,
  1,
  CONCAT('draft finding condition:', s.`rule_key`)
FROM `tmp_oroku_finding_condition_seed` s
JOIN `phr_master`.`csv_exam_result_mapping_rules` r
  ON r.`csv_format_version_id` = @oroku_csv_format_version_id
 AND r.`rule_key` = s.`rule_key`;

DROP TEMPORARY TABLE IF EXISTS `tmp_oroku_finding_condition_seed`;

DROP TEMPORARY TABLE IF EXISTS `tmp_csv_exam_mapping_seed`;

COMMIT;
