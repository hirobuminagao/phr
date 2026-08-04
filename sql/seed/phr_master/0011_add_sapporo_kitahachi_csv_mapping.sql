-- Seed for Sapporo Kitahachi CSV sample.
--
-- Scope:
-- - Facility: 0110711777 / 札幌きたはち健診センター
-- - CSV: single header row, CP932, 286 columns
-- - Even when XML exists for the same facility, this CSV is imported the same
--   way as CSV-only facilities. Later merge/export steps decide whether XML or
--   CSV values are adopted.
-- - Facility ABC judgement columns are not treated as medical findings unless a
--   concrete finding/presence source exists.

USE `phr_master`;

START TRANSACTION;

SELECT `exam_facility_id`
  INTO @sapporo_kitahachi_exam_facility_id
FROM `phr_master`.`exam_facilities`
WHERE `medical_institution_code` = '0110711777'
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
  @sapporo_kitahachi_exam_facility_id,
  'SAPPORO_KITAHACHI_2026_05_PATTERN_A_V1',
  'CSV',
  '札幌きたはち健診センター 2026-05 sample Pattern A',
  1,
  'SINGLE',
  'SIMPLE_HEADER',
  'NONE',
  1,
  2,
  '78145da7f07d96b1fd7eed9d31082e552675f827b29311c7e7b9a1f331eab928',
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
  '2026-05-01',
  'draft seed: Sapporo Kitahachi sample. Import as a normal CSV source even when XML also exists. ECG has judgement only and is not enough to create finding text.',
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
  INTO @sapporo_kitahachi_csv_format_version_id
FROM `phr_master`.`csv_format_versions`
WHERE `exam_facility_id` = @sapporo_kitahachi_exam_facility_id
  AND `mapping_version` = 'SAPPORO_KITAHACHI_2026_05_PATTERN_A_V1'
LIMIT 1;

DROP TEMPORARY TABLE IF EXISTS `tmp_sapporo_kitahachi_mapping_seed`;

CREATE TEMPORARY TABLE `tmp_sapporo_kitahachi_mapping_seed` (
  `seed_key` varchar(128) NOT NULL,
  `target_kind` varchar(32) NOT NULL,
  `target_field` varchar(64) DEFAULT NULL,
  `target_namecode` char(17) DEFAULT NULL,
  `header_name` varchar(255) NOT NULL,
  `raw_value_type` varchar(32) DEFAULT NULL,
  `is_required` tinyint(1) NOT NULL DEFAULT 0,
  `priority` int NOT NULL DEFAULT 1000,
  `note` text
);

INSERT INTO `tmp_sapporo_kitahachi_mapping_seed` (
  `seed_key`, `target_kind`, `target_field`, `target_namecode`, `header_name`,
  `raw_value_type`, `is_required`, `priority`, `note`
) VALUES
-- Basic identity / ledger fields.
('kitahachi.basic.exam_date', 'LEDGER_FIELD', 'exam_date', NULL, '受診日', NULL, 1, 10, 'basic: exam date'),
('kitahachi.basic.facility_code', 'LEDGER_FIELD', 'facility_code', NULL, '健診機関番号', NULL, 0, 20, 'basic: source facility code'),
('kitahachi.basic.facility_name', 'LEDGER_FIELD', 'facility_name', NULL, '健診機関名称', NULL, 0, 30, 'basic: source facility name'),
('kitahachi.basic.name_full_raw', 'LEDGER_FIELD', 'name_full_raw', NULL, '氏名', NULL, 0, 40, 'basic: raw full name'),
('kitahachi.basic.name_kana_raw', 'LEDGER_FIELD', 'name_kana_raw', NULL, 'カナ氏名', NULL, 0, 50, 'basic: raw kana name'),
('kitahachi.basic.gender_raw', 'LEDGER_FIELD', 'gender_raw', NULL, '性別', NULL, 0, 60, 'basic: raw gender'),
('kitahachi.basic.birthdate', 'LEDGER_FIELD', 'birthdate', NULL, '生年月日', NULL, 1, 70, 'basic: birthdate'),
('kitahachi.basic.insurer_number', 'LEDGER_FIELD', 'insurer_number', NULL, '保険者番号', NULL, 1, 80, 'basic: insurer number'),
('kitahachi.basic.insurance_symbol_raw', 'LEDGER_FIELD', 'insurance_symbol_raw', NULL, '保険記号', NULL, 0, 90, 'basic: insurance symbol'),
('kitahachi.basic.insurance_number_raw', 'LEDGER_FIELD', 'insurance_number_raw', NULL, '保険番号', NULL, 0, 100, 'basic: insurance number'),
('kitahachi.basic.person_id_custom', 'LEDGER_FIELD', 'person_id_custom', NULL, '社員番号', NULL, 0, 110, 'basic: employee code'),

-- Common legal-check values.
('kitahachi.exam.height', 'EXAM_ITEM_VALUE', NULL, '9N001000000000001', '身長', NULL, 1, 1000, 'height'),
('kitahachi.exam.weight', 'EXAM_ITEM_VALUE', NULL, '9N006000000000001', '体重', NULL, 1, 1010, 'weight'),
('kitahachi.exam.obesity_index', 'EXAM_ITEM_VALUE', NULL, '9N026000000000002', '肥満度', NULL, 0, 1015, 'obesity index'),
('kitahachi.exam.bmi', 'EXAM_ITEM_VALUE', NULL, '9N011000000000001', 'BMI', NULL, 1, 1020, 'BMI'),
('kitahachi.exam.waist', 'EXAM_ITEM_VALUE', NULL, '9N016160100000001', '腹囲', NULL, 1, 1030, 'waist circumference measured'),
('kitahachi.exam.sbp_first', 'EXAM_ITEM_VALUE', NULL, '9A751000000000001', '最高血圧（１回）', NULL, 0, 1040, 'first systolic blood pressure'),
('kitahachi.exam.dbp_first', 'EXAM_ITEM_VALUE', NULL, '9A761000000000001', '最低血圧（１回）', NULL, 0, 1050, 'first diastolic blood pressure'),
('kitahachi.exam.sbp_second', 'EXAM_ITEM_VALUE', NULL, '9A752000000000001', '最高血圧（２回）', NULL, 0, 1060, 'second systolic blood pressure'),
('kitahachi.exam.dbp_second', 'EXAM_ITEM_VALUE', NULL, '9A762000000000001', '最低血圧（２回）', NULL, 0, 1070, 'second diastolic blood pressure'),
('kitahachi.exam.vision_right_uncorrected', 'EXAM_ITEM_VALUE', NULL, '9E160162100000001', '裸眼（右）', NULL, 0, 1080, 'uncorrected visual acuity right'),
('kitahachi.exam.vision_left_uncorrected', 'EXAM_ITEM_VALUE', NULL, '9E160162200000001', '裸眼（左）', NULL, 0, 1090, 'uncorrected visual acuity left'),
('kitahachi.exam.vision_right_corrected', 'EXAM_ITEM_VALUE', NULL, '9E160162500000001', '矯正（右）', NULL, 0, 1100, 'corrected visual acuity right'),
('kitahachi.exam.vision_left_corrected', 'EXAM_ITEM_VALUE', NULL, '9E160162600000001', '矯正（左）', NULL, 0, 1110, 'corrected visual acuity left'),
('kitahachi.exam.hearing_right_1000', 'EXAM_ITEM_VALUE', NULL, '9D100163100000011', '聴力（右）1000Hz', 'CD', 0, 1120, 'hearing right 1000Hz'),
('kitahachi.exam.hearing_left_1000', 'EXAM_ITEM_VALUE', NULL, '9D100163500000011', '聴力（左）1000Hz', 'CD', 0, 1130, 'hearing left 1000Hz'),
('kitahachi.exam.hearing_right_4000', 'EXAM_ITEM_VALUE', NULL, '9D100163200000011', '聴力（右）4000Hz', 'CD', 0, 1140, 'hearing right 4000Hz'),
('kitahachi.exam.hearing_left_4000', 'EXAM_ITEM_VALUE', NULL, '9D100163600000011', '聴力（左）4000Hz', 'CD', 0, 1150, 'hearing left 4000Hz'),
('kitahachi.exam.urine_sugar', 'EXAM_ITEM_VALUE', NULL, '1A020000000190111', '尿糖', 'CO', 1, 1160, 'urine sugar visual method'),
('kitahachi.exam.urine_protein', 'EXAM_ITEM_VALUE', NULL, '1A010000000190111', '尿蛋白', 'CO', 1, 1170, 'urine protein visual method'),
('kitahachi.exam.urine_occult_blood', 'EXAM_ITEM_VALUE', NULL, '1A100000000190111', '尿潜血', 'CO', 0, 1180, 'urine occult blood visual method'),
('kitahachi.exam.medical_history_text', 'EXAM_ITEM_VALUE', NULL, '9N056160400000049', '既往歴について（治療）', 'ST', 0, 1190, 'medical history finding text; abnormal rows only'),
('kitahachi.exam.subjective_symptoms_text', 'EXAM_ITEM_VALUE', NULL, '9N061160800000049', '自覚症状について', 'ST', 0, 1200, 'subjective symptoms finding text; abnormal rows only'),
('kitahachi.exam.objective_symptoms_text', 'EXAM_ITEM_VALUE', NULL, '9N066160800000049', '他覚症状について', 'ST', 0, 1210, 'objective symptoms finding text; abnormal rows only'),
('kitahachi.exam.chest_xray_finding_text', 'EXAM_ITEM_VALUE', NULL, '9N206160800000049', '胸部X線検査', 'ST', 0, 1220, 'chest X-ray finding text; abnormal rows only'),

-- Blood count / chemistry.
('kitahachi.exam.wbc', 'EXAM_ITEM_VALUE', NULL, '2A010000001930101', '白血球数', NULL, 0, 1400, 'white blood cell count'),
('kitahachi.exam.rbc', 'EXAM_ITEM_VALUE', NULL, '2A020000001930101', '赤血球数', NULL, 0, 1410, 'red blood cell count'),
('kitahachi.exam.hemoglobin', 'EXAM_ITEM_VALUE', NULL, '2A030000001930101', 'ヘモグロビン', NULL, 0, 1420, 'hemoglobin'),
('kitahachi.exam.hematocrit', 'EXAM_ITEM_VALUE', NULL, '2A040000001930102', 'ヘマトクリット', NULL, 0, 1430, 'hematocrit'),
('kitahachi.exam.platelet', 'EXAM_ITEM_VALUE', NULL, '2A050000001930101', '血小板数', NULL, 0, 1440, 'platelet count'),
('kitahachi.exam.total_cholesterol', 'EXAM_ITEM_VALUE', NULL, '3F050000002327101', '総コレステロール', NULL, 0, 1450, 'total cholesterol'),
('kitahachi.exam.hdl', 'EXAM_ITEM_VALUE', NULL, '3F070000002327101', 'HDLコレステロール', NULL, 1, 1460, 'HDL cholesterol'),
('kitahachi.exam.ldl', 'EXAM_ITEM_VALUE', NULL, '3F077000002327101', 'LDLコレステロール', NULL, 1, 1470, 'LDL cholesterol'),
('kitahachi.exam.non_hdl', 'EXAM_ITEM_VALUE', NULL, '3F069000002391901', 'non-HDLコレステロール', NULL, 1, 1480, 'non-HDL cholesterol'),
('kitahachi.exam.tg_fasting', 'EXAM_ITEM_VALUE', NULL, '3F015000002327101', '空腹時中性脂肪', NULL, 1, 1490, 'fasting triglyceride'),
('kitahachi.exam.tg_random', 'EXAM_ITEM_VALUE', NULL, '3F015129902327101', '随時中性脂肪', NULL, 0, 1500, 'random triglyceride'),
('kitahachi.exam.glucose_fasting', 'EXAM_ITEM_VALUE', NULL, '3D010000001927201', '空腹時血糖', NULL, 1, 1510, 'fasting glucose'),
('kitahachi.exam.glucose_random', 'EXAM_ITEM_VALUE', NULL, '3D010129901927201', '随時血糖', NULL, 0, 1520, 'random glucose'),
('kitahachi.exam.hba1c', 'EXAM_ITEM_VALUE', NULL, '3D046000001920402', 'HbA1c（N)', NULL, 1, 1530, 'HbA1c NGSP'),
('kitahachi.exam.ast', 'EXAM_ITEM_VALUE', NULL, '3B035000002327201', 'AST(GOT)', NULL, 1, 1540, 'AST JSCC'),
('kitahachi.exam.alt', 'EXAM_ITEM_VALUE', NULL, '3B045000002327201', 'ALT(GPT)', NULL, 1, 1550, 'ALT JSCC'),
('kitahachi.exam.ggt', 'EXAM_ITEM_VALUE', NULL, '3B090000002327101', 'γーGTP', NULL, 1, 1560, 'gamma-GTP JSCC'),
('kitahachi.exam.total_protein', 'EXAM_ITEM_VALUE', NULL, '3A010000002327101', '総蛋白', NULL, 0, 1570, 'total protein'),
('kitahachi.exam.creatinine', 'EXAM_ITEM_VALUE', NULL, '3C015000002327101', 'クレアチニン', NULL, 0, 1580, 'serum creatinine'),
('kitahachi.exam.egfr', 'EXAM_ITEM_VALUE', NULL, '8A065000002391901', 'eGFR', NULL, 0, 1590, 'eGFR'),
('kitahachi.exam.uric_acid', 'EXAM_ITEM_VALUE', NULL, '3C020000002327101', '尿酸', NULL, 0, 1600, 'serum uric acid'),
('kitahachi.exam.heart_rate', 'EXAM_ITEM_VALUE', NULL, '9N121000000000001', '心拍数（安静心電図用）', NULL, 0, 1610, 'heart rate'),
('kitahachi.exam.ecg_finding_text', 'EXAM_ITEM_VALUE', NULL, '9A110160800000049', '安静時12誘導', 'ST', 0, 1615, 'ECG finding text'),
('kitahachi.exam.fecal_occult_blood_first', 'EXAM_ITEM_VALUE', NULL, '1B030000001599811', '便潜血（1日目）', 'CD', 0, 1620, 'fecal occult blood first occurrence'),
('kitahachi.exam.fecal_occult_blood_second', 'EXAM_ITEM_VALUE', NULL, '1B030000001599811', '便潜血（2日目）', 'CD', 0, 1630, 'fecal occult blood second occurrence'),
('kitahachi.exam.gastric_xray_finding_text', 'EXAM_ITEM_VALUE', NULL, '9N256160800000049', '胃部X線検査', 'ST', 0, 1640, 'upper GI X-ray finding text'),
('kitahachi.exam.gynecology_finding_text', 'EXAM_ITEM_VALUE', NULL, '9N271160800000049', '婦人科診察所見', 'ST', 0, 1650, 'gynecology finding text'),
('kitahachi.exam.cervical_cytology_nichibo', 'EXAM_ITEM_VALUE', NULL, '7A021165008543311', '子宮細胞診日母', 'CO', 0, 1660, 'cervical cytology Nichibo class'),
('kitahachi.exam.cervical_cytology_bethesda', 'EXAM_ITEM_VALUE', NULL, '7A021165208543311', '子宮細胞診ベセスダ', 'CD', 0, 1670, 'cervical cytology Bethesda system 2001'),

-- Specific health check questionnaire.
('kitahachi.exam.medication_bp', 'EXAM_ITEM_VALUE', NULL, '9N701000000000011', '1.服薬歴（血圧）', 'CD', 0, 1700, 'questionnaire medication blood pressure'),
('kitahachi.exam.medication_glucose', 'EXAM_ITEM_VALUE', NULL, '9N706000000000011', '2.服薬歴（血糖）', 'CD', 0, 1710, 'questionnaire medication glucose'),
('kitahachi.exam.medication_lipid', 'EXAM_ITEM_VALUE', NULL, '9N711000000000011', '3.服薬歴（脂質）', 'CD', 0, 1720, 'questionnaire medication lipid'),
('kitahachi.exam.history_stroke', 'EXAM_ITEM_VALUE', NULL, '9N716000000000011', '4.既往歴（脳卒中）', 'CD', 0, 1730, 'questionnaire stroke history'),
('kitahachi.exam.history_heart', 'EXAM_ITEM_VALUE', NULL, '9N721000000000011', '5.既往歴（心疾患）', 'CD', 0, 1740, 'questionnaire heart disease history'),
('kitahachi.exam.history_renal', 'EXAM_ITEM_VALUE', NULL, '9N726000000000011', '6.既往歴（慢性腎不全）', 'CD', 0, 1750, 'questionnaire renal failure history'),
('kitahachi.exam.history_anemia', 'EXAM_ITEM_VALUE', NULL, '9N731000000000011', '7.既往歴（貧血）', 'CD', 0, 1760, 'questionnaire anemia history'),
('kitahachi.exam.smoking', 'EXAM_ITEM_VALUE', NULL, '9N736000000000011', '8.喫煙(R06)', 'CD', 0, 1770, 'questionnaire smoking R06'),
('kitahachi.exam.weight_change', 'EXAM_ITEM_VALUE', NULL, '9N741000000000011', '9.20歳の時の体重から10㎏以上増加している', 'CD', 0, 1780, 'questionnaire weight change'),
('kitahachi.exam.exercise', 'EXAM_ITEM_VALUE', NULL, '9N746000000000011', '10.1回30分以上の軽い汗をかく運動を週2日以上、1年以上実施している', 'CD', 0, 1790, 'questionnaire exercise'),
('kitahachi.exam.physical_activity', 'EXAM_ITEM_VALUE', NULL, '9N751000000000011', '11.日常生活において歩行又は同等の身体活動を1日1時間以上実施している', 'CD', 0, 1800, 'questionnaire physical activity'),
('kitahachi.exam.walking_speed', 'EXAM_ITEM_VALUE', NULL, '9N756000000000011', '12.ほぼ同じ年齢の同性と比較して歩く速度が速い', 'CD', 0, 1810, 'questionnaire walking speed'),
('kitahachi.exam.chewing', 'EXAM_ITEM_VALUE', NULL, '9N872000000000011', '13.食事をかんで食べる時の状態はどれにあてはまりますか', 'CD', 0, 1820, 'questionnaire chewing'),
('kitahachi.exam.eating_speed', 'EXAM_ITEM_VALUE', NULL, '9N766000000000011', '14.人と比較して食べる速度が速い', 'CD', 0, 1830, 'questionnaire eating speed'),
('kitahachi.exam.late_dinner', 'EXAM_ITEM_VALUE', NULL, '9N771000000000011', '15.就寝前の2時間以内に夕食を取ることが週に3回以上ある', 'CD', 0, 1840, 'questionnaire late dinner'),
('kitahachi.exam.snacking', 'EXAM_ITEM_VALUE', NULL, '9N782000000000011', '16.朝昼夕の3食以外に間食をしたり甘い飲み物をとることがある', 'CD', 0, 1850, 'questionnaire snacking'),
('kitahachi.exam.breakfast', 'EXAM_ITEM_VALUE', NULL, '9N781000000000011', '17.朝食を抜くことが週に3回以上ある', 'CD', 0, 1860, 'questionnaire breakfast skipping'),
('kitahachi.exam.drinking_frequency', 'EXAM_ITEM_VALUE', NULL, '9N786000000000011', '18.飲酒の頻度(R06)', 'CD', 0, 1870, 'questionnaire drinking frequency R06'),
('kitahachi.exam.drinking_amount', 'EXAM_ITEM_VALUE', NULL, '9N791000000000011', '19.飲酒量(R06)', 'CO', 0, 1880, 'questionnaire drinking amount R06'),
('kitahachi.exam.sleep', 'EXAM_ITEM_VALUE', NULL, '9N796000000000011', '20.睡眠で休養が十分とれている', 'CD', 0, 1890, 'questionnaire sleep'),
('kitahachi.exam.lifestyle_improvement', 'EXAM_ITEM_VALUE', NULL, '9N801000000000011', '21.運動や食生活等の生活習慣を改善してみようと思いますか', 'CD', 0, 1900, 'questionnaire lifestyle improvement'),
('kitahachi.exam.guidance_history', 'EXAM_ITEM_VALUE', NULL, '9N808000000000011', '22.生活習慣の改善について保健指導を受ける機会があれば、利用しますか', 'CD', 0, 1910, 'questionnaire specific health guidance history'),
('kitahachi.exam.metabolic', 'EXAM_ITEM_VALUE', NULL, '9N501000000000011', 'メタボリック症候群診断', 'CD', 0, 1920, 'metabolic syndrome judgement'),
('kitahachi.exam.guidance_level', 'EXAM_ITEM_VALUE', NULL, '9N506000000000011', '特・階層化結果', 'CD', 0, 1930, 'specific health guidance level'),
('kitahachi.exam.doctor_judgement', 'EXAM_ITEM_VALUE', NULL, '9N511000000000049', '指導コメント', 'ST', 0, 1940, 'doctor judgement text from facility guidance comment');

INSERT INTO `phr_master`.`csv_exam_result_mapping_rules` (
  `csv_format_version_id`, `rule_key`, `target_kind`, `target_resolution_type`, `selection_mode`,
  `selection_group_code`, `target_namecode`, `target_identity_item_code`, `target_field`,
  `method_structure_type`, `raw_value_type`, `raw_unit`,
  `is_required`, `priority`, `is_active`, `note`
)
SELECT
  @sapporo_kitahachi_csv_format_version_id,
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
  NULL,
  `is_required`,
  `priority`,
  1,
  CONCAT('draft seed:', `seed_key`, ':', COALESCE(`note`, ''))
FROM `tmp_sapporo_kitahachi_mapping_seed`
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
WHERE r.`csv_format_version_id` = @sapporo_kitahachi_csv_format_version_id;

INSERT INTO `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_rule_id`, `condition_group_no`, `condition_type`,
  `locator_type`, `header_name`, `header_occurrence`, `column_no`,
  `operator`, `expected_value`, `source_role`, `priority`, `is_active`, `note`
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
  NULL,
  'VALUE',
  100,
  1,
  CONCAT('draft condition:', s.`seed_key`)
FROM `tmp_sapporo_kitahachi_mapping_seed` s
JOIN `phr_master`.`csv_exam_result_mapping_rules` r
  ON r.`csv_format_version_id` = @sapporo_kitahachi_csv_format_version_id
 AND r.`note` LIKE CONCAT('draft seed:', s.`seed_key`, ':%');

-- Do not store normal/no-finding tokens as ST text.
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
  SELECT 'kitahachi.exam.medical_history_text' AS `rule_key`, '既往歴について（治療）' AS `header_name`, 'NOT_EMPTY' AS `operator`, NULL AS `expected_value`, 110 AS `priority`
  UNION ALL SELECT 'kitahachi.exam.medical_history_text', '既往歴について（治療）', 'NOT_EQUALS', '特になし', 120
  UNION ALL SELECT 'kitahachi.exam.subjective_symptoms_text', '自覚症状について', 'NOT_EMPTY', NULL, 110
  UNION ALL SELECT 'kitahachi.exam.subjective_symptoms_text', '自覚症状について', 'NOT_EQUALS', '特になし', 120
  UNION ALL SELECT 'kitahachi.exam.objective_symptoms_text', '他覚症状について', 'NOT_EMPTY', NULL, 110
  UNION ALL SELECT 'kitahachi.exam.objective_symptoms_text', '他覚症状について', 'NOT_EQUALS', '特になし', 120
  UNION ALL SELECT 'kitahachi.exam.objective_symptoms_text', '他覚症状について', 'NOT_EQUALS', '所見なし', 130
  UNION ALL SELECT 'kitahachi.exam.chest_xray_finding_text', '胸部X線検査', 'NOT_EMPTY', NULL, 110
  UNION ALL SELECT 'kitahachi.exam.chest_xray_finding_text', '胸部X線検査', 'NOT_EQUALS', '所見なし', 120
) x ON x.`rule_key` = r.`rule_key`
WHERE r.`csv_format_version_id` = @sapporo_kitahachi_csv_format_version_id;

-- Fundus finding: a blank cell means not performed/no entry. If either right
-- or left has "所見なし" or a concrete finding, create a finding entry.
INSERT INTO `phr_master`.`csv_exam_result_mapping_rules` (
  `csv_format_version_id`, `rule_key`, `target_kind`, `target_resolution_type`, `selection_mode`,
  `target_namecode`, `method_structure_type`, `value_source_type`, `fixed_value`,
  `value_join_separator`, `value_exclude_values`, `raw_value_type`,
  `is_required`, `priority`, `is_active`, `note`
) VALUES (
  @sapporo_kitahachi_csv_format_version_id,
  'kitahachi.exam.fundus_finding_text',
  'EXAM_ITEM_VALUE',
  'SINGLE_NAMECODE',
  'DIRECT',
  '9E100160900000049',
  'MULTI_COLUMN',
  'SOURCE',
  NULL,
  ' / ',
  NULL,
  'ST',
  0,
  1950,
  1,
  'draft seed:kitahachi.exam.fundus_finding_text:右左眼底所見を結合。空欄のみならentryなし、所見なしはentryとして保持。'
)
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
  `raw_value_type` = VALUES(`raw_value_type`),
  `is_required` = VALUES(`is_required`),
  `priority` = VALUES(`priority`),
  `is_active` = VALUES(`is_active`),
  `note` = VALUES(`note`),
  `updated_at` = CURRENT_TIMESTAMP(3);

INSERT INTO `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_rule_id`, `condition_group_no`, `condition_type`,
  `locator_type`, `header_name`, `header_occurrence`, `operator`, `expected_value`,
  `source_role`, `priority`, `is_active`, `note`
)
SELECT r.`csv_exam_result_mapping_rule_id`, x.`condition_group_no`, x.`condition_type`,
       'HEADER_NAME', x.`header_name`, 1, x.`operator`, x.`expected_value`,
       x.`source_role`, x.`priority`, 1, CONCAT('draft fundus condition:', r.`rule_key`)
FROM `phr_master`.`csv_exam_result_mapping_rules` r
JOIN (
  SELECT 1 AS `condition_group_no`, 'HEADER_MATCH' AS `condition_type`, '眼底所見（右）' AS `header_name`, 'PRESENT' AS `operator`, NULL AS `expected_value`, 'VALUE' AS `source_role`, 100 AS `priority`
  UNION ALL SELECT 1, 'HEADER_MATCH', '眼底所見（左）', 'PRESENT', NULL, 'VALUE', 110
  UNION ALL SELECT 1, 'CELL_VALUE', '眼底所見（右）', 'NOT_EMPTY', NULL, 'QUALIFIER', 120
  UNION ALL SELECT 2, 'HEADER_MATCH', '眼底所見（右）', 'PRESENT', NULL, 'VALUE', 100
  UNION ALL SELECT 2, 'HEADER_MATCH', '眼底所見（左）', 'PRESENT', NULL, 'VALUE', 110
  UNION ALL SELECT 2, 'CELL_VALUE', '眼底所見（左）', 'NOT_EMPTY', NULL, 'QUALIFIER', 120
) x ON 1 = 1
WHERE r.`csv_format_version_id` = @sapporo_kitahachi_csv_format_version_id
  AND r.`rule_key` = 'kitahachi.exam.fundus_finding_text';

-- Optional imaging/ultrasound findings: blank cells mean no entry. If a
-- finding column has "所見なし" or concrete text, preserve it as ST evidence.
INSERT INTO `phr_master`.`csv_exam_result_mapping_rules` (
  `csv_format_version_id`, `rule_key`, `target_kind`, `target_resolution_type`, `selection_mode`,
  `target_namecode`, `method_structure_type`, `value_source_type`, `fixed_value`,
  `value_join_separator`, `value_exclude_values`, `raw_value_type`,
  `is_required`, `priority`, `is_active`, `note`
) VALUES
  (
    @sapporo_kitahachi_csv_format_version_id,
    'kitahachi.exam.breast_ultrasound_finding_text',
    'EXAM_ITEM_VALUE',
    'SINGLE_NAMECODE',
    'DIRECT',
    '9F140160800000049',
    'MULTI_COLUMN',
    'SOURCE',
    NULL,
    ' / ',
    NULL,
    'ST',
    0,
    1960,
    1,
    'draft seed:kitahachi.exam.breast_ultrasound_finding_text:右左乳腺超音波所見を結合。空欄のみならentryなし、所見なしはentryとして保持。'
  ),
  (
    @sapporo_kitahachi_csv_format_version_id,
    'kitahachi.exam.mammography_finding_text',
    'EXAM_ITEM_VALUE',
    'SINGLE_NAMECODE',
    'DIRECT',
    '9N281160800000049',
    'MULTI_COLUMN',
    'SOURCE',
    NULL,
    ' / ',
    NULL,
    'ST',
    0,
    1970,
    1,
    'draft seed:kitahachi.exam.mammography_finding_text:右左マンモグラフィ所見を結合。空欄のみならentryなし、所見なしはentryとして保持。'
  ),
  (
    @sapporo_kitahachi_csv_format_version_id,
    'kitahachi.exam.abdominal_ultrasound_finding_text',
    'EXAM_ITEM_VALUE',
    'SINGLE_NAMECODE',
    'DIRECT',
    '9F130160800000049',
    'MULTI_COLUMN',
    'SOURCE',
    NULL,
    ' / ',
    NULL,
    'ST',
    0,
    1980,
    1,
    'draft seed:kitahachi.exam.abdominal_ultrasound_finding_text:腹部超音波の各臓器所見を結合。空欄のみならentryなし、所見なしはentryとして保持。'
  )
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
  `raw_value_type` = VALUES(`raw_value_type`),
  `is_required` = VALUES(`is_required`),
  `priority` = VALUES(`priority`),
  `is_active` = VALUES(`is_active`),
  `note` = VALUES(`note`),
  `updated_at` = CURRENT_TIMESTAMP(3);

INSERT INTO `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_rule_id`, `condition_group_no`, `condition_type`,
  `locator_type`, `header_name`, `header_occurrence`, `operator`, `expected_value`,
  `source_role`, `priority`, `is_active`, `note`
)
SELECT r.`csv_exam_result_mapping_rule_id`, x.`condition_group_no`, x.`condition_type`,
       'HEADER_NAME', x.`header_name`, 1, x.`operator`, x.`expected_value`,
       x.`source_role`, x.`priority`, 1, CONCAT('draft optional finding condition:', r.`rule_key`)
FROM `phr_master`.`csv_exam_result_mapping_rules` r
JOIN (
  SELECT 'kitahachi.exam.breast_ultrasound_finding_text' AS `rule_key`, 1 AS `condition_group_no`, 'HEADER_MATCH' AS `condition_type`, '乳腺超音波（右）' AS `header_name`, 'PRESENT' AS `operator`, NULL AS `expected_value`, 'VALUE' AS `source_role`, 100 AS `priority`
  UNION ALL SELECT 'kitahachi.exam.breast_ultrasound_finding_text', 1, 'HEADER_MATCH', '乳腺超音波（左）', 'PRESENT', NULL, 'VALUE', 110
  UNION ALL SELECT 'kitahachi.exam.breast_ultrasound_finding_text', 1, 'CELL_VALUE', '乳腺超音波（右）', 'NOT_EMPTY', NULL, 'QUALIFIER', 120
  UNION ALL SELECT 'kitahachi.exam.breast_ultrasound_finding_text', 2, 'HEADER_MATCH', '乳腺超音波（右）', 'PRESENT', NULL, 'VALUE', 100
  UNION ALL SELECT 'kitahachi.exam.breast_ultrasound_finding_text', 2, 'HEADER_MATCH', '乳腺超音波（左）', 'PRESENT', NULL, 'VALUE', 110
  UNION ALL SELECT 'kitahachi.exam.breast_ultrasound_finding_text', 2, 'CELL_VALUE', '乳腺超音波（左）', 'NOT_EMPTY', NULL, 'QUALIFIER', 120
  UNION ALL SELECT 'kitahachi.exam.mammography_finding_text', 1, 'HEADER_MATCH', 'マンモグラフィ（右）', 'PRESENT', NULL, 'VALUE', 100
  UNION ALL SELECT 'kitahachi.exam.mammography_finding_text', 1, 'HEADER_MATCH', 'マンモグラフィ（左）', 'PRESENT', NULL, 'VALUE', 110
  UNION ALL SELECT 'kitahachi.exam.mammography_finding_text', 1, 'CELL_VALUE', 'マンモグラフィ（右）', 'NOT_EMPTY', NULL, 'QUALIFIER', 120
  UNION ALL SELECT 'kitahachi.exam.mammography_finding_text', 2, 'HEADER_MATCH', 'マンモグラフィ（右）', 'PRESENT', NULL, 'VALUE', 100
  UNION ALL SELECT 'kitahachi.exam.mammography_finding_text', 2, 'HEADER_MATCH', 'マンモグラフィ（左）', 'PRESENT', NULL, 'VALUE', 110
  UNION ALL SELECT 'kitahachi.exam.mammography_finding_text', 2, 'CELL_VALUE', 'マンモグラフィ（左）', 'NOT_EMPTY', NULL, 'QUALIFIER', 120
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 1, 'HEADER_MATCH', '腹部超音波（胆のう）', 'PRESENT', NULL, 'VALUE', 100
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 1, 'HEADER_MATCH', '腹部超音波（肝臓）', 'PRESENT', NULL, 'VALUE', 110
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 1, 'HEADER_MATCH', '腹部超音波（膵臓）', 'PRESENT', NULL, 'VALUE', 120
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 1, 'HEADER_MATCH', '腹部超音波（腎臓）', 'PRESENT', NULL, 'VALUE', 130
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 1, 'HEADER_MATCH', '腹部超音波（脾臓）', 'PRESENT', NULL, 'VALUE', 140
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 1, 'HEADER_MATCH', '腹部超音波（その他）', 'PRESENT', NULL, 'VALUE', 150
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 1, 'HEADER_MATCH', '腹部超音波（大動脈）所見', 'PRESENT', NULL, 'VALUE', 160
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 1, 'CELL_VALUE', '腹部超音波（胆のう）', 'NOT_EMPTY', NULL, 'QUALIFIER', 170
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 2, 'HEADER_MATCH', '腹部超音波（胆のう）', 'PRESENT', NULL, 'VALUE', 100
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 2, 'HEADER_MATCH', '腹部超音波（肝臓）', 'PRESENT', NULL, 'VALUE', 110
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 2, 'HEADER_MATCH', '腹部超音波（膵臓）', 'PRESENT', NULL, 'VALUE', 120
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 2, 'HEADER_MATCH', '腹部超音波（腎臓）', 'PRESENT', NULL, 'VALUE', 130
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 2, 'HEADER_MATCH', '腹部超音波（脾臓）', 'PRESENT', NULL, 'VALUE', 140
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 2, 'HEADER_MATCH', '腹部超音波（その他）', 'PRESENT', NULL, 'VALUE', 150
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 2, 'HEADER_MATCH', '腹部超音波（大動脈）所見', 'PRESENT', NULL, 'VALUE', 160
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 2, 'CELL_VALUE', '腹部超音波（肝臓）', 'NOT_EMPTY', NULL, 'QUALIFIER', 170
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 3, 'HEADER_MATCH', '腹部超音波（胆のう）', 'PRESENT', NULL, 'VALUE', 100
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 3, 'HEADER_MATCH', '腹部超音波（肝臓）', 'PRESENT', NULL, 'VALUE', 110
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 3, 'HEADER_MATCH', '腹部超音波（膵臓）', 'PRESENT', NULL, 'VALUE', 120
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 3, 'HEADER_MATCH', '腹部超音波（腎臓）', 'PRESENT', NULL, 'VALUE', 130
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 3, 'HEADER_MATCH', '腹部超音波（脾臓）', 'PRESENT', NULL, 'VALUE', 140
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 3, 'HEADER_MATCH', '腹部超音波（その他）', 'PRESENT', NULL, 'VALUE', 150
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 3, 'HEADER_MATCH', '腹部超音波（大動脈）所見', 'PRESENT', NULL, 'VALUE', 160
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 3, 'CELL_VALUE', '腹部超音波（膵臓）', 'NOT_EMPTY', NULL, 'QUALIFIER', 170
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 4, 'HEADER_MATCH', '腹部超音波（胆のう）', 'PRESENT', NULL, 'VALUE', 100
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 4, 'HEADER_MATCH', '腹部超音波（肝臓）', 'PRESENT', NULL, 'VALUE', 110
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 4, 'HEADER_MATCH', '腹部超音波（膵臓）', 'PRESENT', NULL, 'VALUE', 120
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 4, 'HEADER_MATCH', '腹部超音波（腎臓）', 'PRESENT', NULL, 'VALUE', 130
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 4, 'HEADER_MATCH', '腹部超音波（脾臓）', 'PRESENT', NULL, 'VALUE', 140
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 4, 'HEADER_MATCH', '腹部超音波（その他）', 'PRESENT', NULL, 'VALUE', 150
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 4, 'HEADER_MATCH', '腹部超音波（大動脈）所見', 'PRESENT', NULL, 'VALUE', 160
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 4, 'CELL_VALUE', '腹部超音波（腎臓）', 'NOT_EMPTY', NULL, 'QUALIFIER', 170
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 5, 'HEADER_MATCH', '腹部超音波（胆のう）', 'PRESENT', NULL, 'VALUE', 100
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 5, 'HEADER_MATCH', '腹部超音波（肝臓）', 'PRESENT', NULL, 'VALUE', 110
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 5, 'HEADER_MATCH', '腹部超音波（膵臓）', 'PRESENT', NULL, 'VALUE', 120
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 5, 'HEADER_MATCH', '腹部超音波（腎臓）', 'PRESENT', NULL, 'VALUE', 130
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 5, 'HEADER_MATCH', '腹部超音波（脾臓）', 'PRESENT', NULL, 'VALUE', 140
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 5, 'HEADER_MATCH', '腹部超音波（その他）', 'PRESENT', NULL, 'VALUE', 150
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 5, 'HEADER_MATCH', '腹部超音波（大動脈）所見', 'PRESENT', NULL, 'VALUE', 160
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 5, 'CELL_VALUE', '腹部超音波（脾臓）', 'NOT_EMPTY', NULL, 'QUALIFIER', 170
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 6, 'HEADER_MATCH', '腹部超音波（胆のう）', 'PRESENT', NULL, 'VALUE', 100
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 6, 'HEADER_MATCH', '腹部超音波（肝臓）', 'PRESENT', NULL, 'VALUE', 110
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 6, 'HEADER_MATCH', '腹部超音波（膵臓）', 'PRESENT', NULL, 'VALUE', 120
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 6, 'HEADER_MATCH', '腹部超音波（腎臓）', 'PRESENT', NULL, 'VALUE', 130
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 6, 'HEADER_MATCH', '腹部超音波（脾臓）', 'PRESENT', NULL, 'VALUE', 140
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 6, 'HEADER_MATCH', '腹部超音波（その他）', 'PRESENT', NULL, 'VALUE', 150
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 6, 'HEADER_MATCH', '腹部超音波（大動脈）所見', 'PRESENT', NULL, 'VALUE', 160
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 6, 'CELL_VALUE', '腹部超音波（その他）', 'NOT_EMPTY', NULL, 'QUALIFIER', 170
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 7, 'HEADER_MATCH', '腹部超音波（胆のう）', 'PRESENT', NULL, 'VALUE', 100
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 7, 'HEADER_MATCH', '腹部超音波（肝臓）', 'PRESENT', NULL, 'VALUE', 110
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 7, 'HEADER_MATCH', '腹部超音波（膵臓）', 'PRESENT', NULL, 'VALUE', 120
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 7, 'HEADER_MATCH', '腹部超音波（腎臓）', 'PRESENT', NULL, 'VALUE', 130
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 7, 'HEADER_MATCH', '腹部超音波（脾臓）', 'PRESENT', NULL, 'VALUE', 140
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 7, 'HEADER_MATCH', '腹部超音波（その他）', 'PRESENT', NULL, 'VALUE', 150
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 7, 'HEADER_MATCH', '腹部超音波（大動脈）所見', 'PRESENT', NULL, 'VALUE', 160
  UNION ALL SELECT 'kitahachi.exam.abdominal_ultrasound_finding_text', 7, 'CELL_VALUE', '腹部超音波（大動脈）所見', 'NOT_EMPTY', NULL, 'QUALIFIER', 170
) x ON x.`rule_key` = r.`rule_key`
WHERE r.`csv_format_version_id` = @sapporo_kitahachi_csv_format_version_id
  AND r.`rule_key` IN (
    'kitahachi.exam.breast_ultrasound_finding_text',
    'kitahachi.exam.mammography_finding_text',
    'kitahachi.exam.abdominal_ultrasound_finding_text'
  );

INSERT INTO `phr_master`.`csv_exam_result_mapping_rules` (
  `csv_format_version_id`, `rule_key`, `target_kind`, `target_resolution_type`, `selection_mode`,
  `target_namecode`, `method_structure_type`, `value_source_type`, `fixed_value`,
  `is_required`, `priority`, `is_active`, `note`
) VALUES
  (@sapporo_kitahachi_csv_format_version_id, 'kitahachi.exam.medical_history_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N056000000000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 1300, 1, 'draft seed:kitahachi.exam.medical_history_presence_normal:empty/特になし -> CD=2'),
  (@sapporo_kitahachi_csv_format_version_id, 'kitahachi.exam.medical_history_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N056000000000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 1310, 1, 'draft seed:kitahachi.exam.medical_history_presence_abnormal:finding text exists -> CD=1'),
  (@sapporo_kitahachi_csv_format_version_id, 'kitahachi.exam.subjective_symptoms_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N061000000000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 1320, 1, 'draft seed:kitahachi.exam.subjective_symptoms_presence_normal:empty/特になし -> CD=2'),
  (@sapporo_kitahachi_csv_format_version_id, 'kitahachi.exam.subjective_symptoms_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N061000000000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 1330, 1, 'draft seed:kitahachi.exam.subjective_symptoms_presence_abnormal:finding text exists -> CD=1'),
  (@sapporo_kitahachi_csv_format_version_id, 'kitahachi.exam.objective_symptoms_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N066000000000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 1340, 1, 'draft seed:kitahachi.exam.objective_symptoms_presence_normal:empty/特になし -> CD=2'),
  (@sapporo_kitahachi_csv_format_version_id, 'kitahachi.exam.objective_symptoms_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N066000000000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 1350, 1, 'draft seed:kitahachi.exam.objective_symptoms_presence_abnormal:finding text exists -> CD=1'),
  (@sapporo_kitahachi_csv_format_version_id, 'kitahachi.exam.chest_xray_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N206160700000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 1360, 1, 'draft seed:kitahachi.exam.chest_xray_presence_normal:所見なし/empty -> CD=2'),
  (@sapporo_kitahachi_csv_format_version_id, 'kitahachi.exam.chest_xray_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N206160700000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 1370, 1, 'draft seed:kitahachi.exam.chest_xray_presence_abnormal:finding text exists -> CD=1'),
  (@sapporo_kitahachi_csv_format_version_id, 'kitahachi.exam.ecg_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9A110160700000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 1380, 1, 'draft seed:kitahachi.exam.ecg_presence_normal:所見なし/empty -> CD=2'),
  (@sapporo_kitahachi_csv_format_version_id, 'kitahachi.exam.ecg_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9A110160700000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 1390, 1, 'draft seed:kitahachi.exam.ecg_presence_abnormal:finding text exists -> CD=1')
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

INSERT INTO `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_rule_id`, `condition_group_no`, `condition_type`,
  `locator_type`, `header_name`, `header_occurrence`, `operator`, `expected_value`,
  `source_role`, `priority`, `is_active`, `note`
)
SELECT r.`csv_exam_result_mapping_rule_id`, x.`condition_group_no`, 'CELL_VALUE',
       'HEADER_NAME', x.`header_name`, 1, x.`operator`, x.`expected_value`,
       'QUALIFIER', x.`priority`, 1, CONCAT('draft derived condition:', r.`rule_key`)
FROM `phr_master`.`csv_exam_result_mapping_rules` r
JOIN (
  SELECT 'kitahachi.exam.medical_history_presence_normal' AS `rule_key`, 1 AS `condition_group_no`, '既往歴について（治療）' AS `header_name`, 'EQUALS' AS `operator`, '特になし' AS `expected_value`, 100 AS `priority`
  UNION ALL SELECT 'kitahachi.exam.medical_history_presence_normal', 2, '既往歴について（治療）', 'EMPTY', NULL, 100
  UNION ALL SELECT 'kitahachi.exam.medical_history_presence_abnormal', 1, '既往歴について（治療）', 'NOT_EMPTY', NULL, 100
  UNION ALL SELECT 'kitahachi.exam.medical_history_presence_abnormal', 1, '既往歴について（治療）', 'NOT_EQUALS', '特になし', 110
  UNION ALL SELECT 'kitahachi.exam.subjective_symptoms_presence_normal', 1, '自覚症状について', 'EQUALS', '特になし', 100
  UNION ALL SELECT 'kitahachi.exam.subjective_symptoms_presence_normal', 2, '自覚症状について', 'EMPTY', NULL, 100
  UNION ALL SELECT 'kitahachi.exam.subjective_symptoms_presence_abnormal', 1, '自覚症状について', 'NOT_EMPTY', NULL, 100
  UNION ALL SELECT 'kitahachi.exam.subjective_symptoms_presence_abnormal', 1, '自覚症状について', 'NOT_EQUALS', '特になし', 110
  UNION ALL SELECT 'kitahachi.exam.objective_symptoms_presence_normal', 1, '他覚症状について', 'EQUALS', '特になし', 100
  UNION ALL SELECT 'kitahachi.exam.objective_symptoms_presence_normal', 2, '他覚症状について', 'EQUALS', '所見なし', 100
  UNION ALL SELECT 'kitahachi.exam.objective_symptoms_presence_normal', 3, '他覚症状について', 'EMPTY', NULL, 100
  UNION ALL SELECT 'kitahachi.exam.objective_symptoms_presence_abnormal', 1, '他覚症状について', 'NOT_EMPTY', NULL, 100
  UNION ALL SELECT 'kitahachi.exam.objective_symptoms_presence_abnormal', 1, '他覚症状について', 'NOT_EQUALS', '特になし', 110
  UNION ALL SELECT 'kitahachi.exam.objective_symptoms_presence_abnormal', 1, '他覚症状について', 'NOT_EQUALS', '所見なし', 120
  UNION ALL SELECT 'kitahachi.exam.chest_xray_presence_normal', 1, '胸部X線検査', 'EQUALS', '所見なし', 100
  UNION ALL SELECT 'kitahachi.exam.chest_xray_presence_normal', 2, '胸部X線検査', 'EMPTY', NULL, 100
  UNION ALL SELECT 'kitahachi.exam.chest_xray_presence_abnormal', 1, '胸部X線検査', 'NOT_EMPTY', NULL, 100
  UNION ALL SELECT 'kitahachi.exam.chest_xray_presence_abnormal', 1, '胸部X線検査', 'NOT_EQUALS', '所見なし', 110
  UNION ALL SELECT 'kitahachi.exam.ecg_presence_normal', 1, '安静時12誘導', 'EQUALS', '所見なし', 100
  UNION ALL SELECT 'kitahachi.exam.ecg_presence_normal', 2, '安静時12誘導', 'EMPTY', NULL, 100
  UNION ALL SELECT 'kitahachi.exam.ecg_presence_abnormal', 1, '安静時12誘導', 'NOT_EMPTY', NULL, 100
  UNION ALL SELECT 'kitahachi.exam.ecg_presence_abnormal', 1, '安静時12誘導', 'NOT_EQUALS', '所見なし', 110
) x ON x.`rule_key` = r.`rule_key`
WHERE r.`csv_format_version_id` = @sapporo_kitahachi_csv_format_version_id;

DROP TEMPORARY TABLE IF EXISTS `tmp_sapporo_kitahachi_mapping_seed`;

COMMIT;
