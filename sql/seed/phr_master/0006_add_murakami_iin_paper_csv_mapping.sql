-- Seed: Murakami Iin paper-to-CSV mapping.
-- Source sample: docs/spec/exam_result_csv_import/samples/murakami_iin/murakami_iin_paper_sample_001.csv
-- Original local file: /Users/hiro/Downloads/紙からcsvサンプル.csv
-- Original local file sha256 at design time: 83cbddcfdea7e2c81f818b9f545594cbb458ebe410bdfdc631d4c3b11107a210
-- Workspace anonymized sample file sha256: 3c741e230800488acd7e23afcdef7b6f9e4ec42615d4abcfe375c5a8474baa11
-- The CSV has two header rows: row 1 is the active key row; row 2 is Japanese context/label.

USE `phr_master`;

START TRANSACTION;

SELECT `exam_facility_id`
  INTO @murakami_exam_facility_id
FROM `phr_master`.`exam_facilities`
WHERE `medical_institution_code` = '4210225498'
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
  `duplicate_row_policy`,
  `missing_basic_info_policy`,
  `character_encoding`,
  `encoding_fallback_policy`,
  `delimiter`,
  `quote_char`,
  `note`,
  `is_active`
) VALUES (
  @murakami_exam_facility_id,
  'MURAKAMI_IIN_PAPER_2026_05_V1',
  'CSV',
  '村上医院 paper-to-CSV 2026-05 V1',
  1,
  'WITH_CONTEXT',
  'SIMPLE_HEADER',
  'LOWER_HEADER_LABEL',
  1,
  3,
  'd37bc4a347f697b9ad8bf34580f12dfd292b0dab0a803dce0bb3f6621afb3875',
  'VERIFIED',
  'ALLOW_AFTER_CONFIRM',
  0,
  'SKIP_CHECKED_OK',
  'IMPORT_AND_CHECK_LATER',
  'utf-8-sig',
  'ALLOW_COMMON_ENCODINGS',
  ',',
  '"',
  'seed: murakami iin paper-to-CSV sample. active header row is row 1; row 2 is Japanese label/context. Finding text normal tokens are not stored as ST.',
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
  INTO @murakami_csv_format_version_id
FROM `phr_master`.`csv_format_versions`
WHERE `exam_facility_id` = @murakami_exam_facility_id
  AND `mapping_version` = 'MURAKAMI_IIN_PAPER_2026_05_V1'
LIMIT 1;

DROP TEMPORARY TABLE IF EXISTS `tmp_murakami_csv_exam_mapping_seed`;
CREATE TEMPORARY TABLE `tmp_murakami_csv_exam_mapping_seed` (
  `seed_key` varchar(128) NOT NULL,
  `target_kind` varchar(32) NOT NULL,
  `target_field` varchar(64) DEFAULT NULL,
  `target_namecode` char(17) DEFAULT NULL,
  `header_name` varchar(255) NOT NULL,
  `is_required` tinyint(1) NOT NULL DEFAULT 0,
  `priority` int NOT NULL DEFAULT 100,
  `note` text
);

INSERT INTO `tmp_murakami_csv_exam_mapping_seed` (`seed_key`, `target_kind`, `target_field`, `target_namecode`, `header_name`, `is_required`, `priority`, `note`) VALUES
  ('murakami.basic.facility_code', 'LEDGER_FIELD', 'facility_code', NULL, 'HEALTH_EXAMINATION_ORGANIZATION_NO', 0, 20, 'basic: source facility code'),
  ('murakami.basic.facility_name', 'LEDGER_FIELD', 'facility_name', NULL, 'HEALTH_EXAMINATION_ORGANIZATION_NAME', 0, 30, 'basic: source facility name'),
  ('murakami.basic.name_full_raw', 'LEDGER_FIELD', 'name_full_raw', NULL, 'NAME_FULL', 0, 50, 'basic: raw full name; sample blank'),
  ('murakami.basic.name_kana_raw', 'LEDGER_FIELD', 'name_kana_raw', NULL, 'NAME_KANA', 1, 60, 'basic: raw kana name; sample blank'),
  ('murakami.basic.postal_code', 'LEDGER_FIELD', 'postal_code', NULL, 'POSTALCODE', 0, 70, 'basic: postal code'),
  ('murakami.basic.address', 'LEDGER_FIELD', 'address', NULL, 'ADDRESS', 0, 80, 'basic: address; sample blank'),
  ('murakami.basic.exam_date', 'LEDGER_FIELD', 'exam_date', NULL, 'HEALTH_EXAMINATION_DATE', 1, 90, 'basic: exam date'),
  ('murakami.basic.birthdate', 'LEDGER_FIELD', 'birthdate', NULL, 'BIRTHDAY', 1, 100, 'basic: birthdate'),
  ('murakami.basic.gender_raw', 'LEDGER_FIELD', 'gender_raw', NULL, 'GENDER', 1, 110, 'basic: gender'),
  ('murakami.basic.insurer_number', 'LEDGER_FIELD', 'insurer_number', NULL, 'INSURER_NUMBER', 1, 120, 'basic: insurer number'),
  ('murakami.basic.insurance_symbol_raw', 'LEDGER_FIELD', 'insurance_symbol_raw', NULL, 'INSURANCE_CARD_SYMBOL', 0, 130, 'basic: insurance symbol; sample blank'),
  ('murakami.basic.insurance_number_raw', 'LEDGER_FIELD', 'insurance_number_raw', NULL, 'INSURANCE_CARD_NUMBER', 0, 140, 'basic: insurance number; sample blank'),
  ('murakami.exam.5h010000001910111', 'EXAM_ITEM_VALUE', NULL, '5H010000001910111', '5H010000001910111', 0, 1000, 'exam item: 血液型(ABO)'),
  ('murakami.exam.5h020000001910111', 'EXAM_ITEM_VALUE', NULL, '5H020000001910111', '5H020000001910111', 0, 1010, 'exam item: 血液型(Rh)'),
  ('murakami.exam.9n001000000000001', 'EXAM_ITEM_VALUE', NULL, '9N001000000000001', '9N001000000000001', 0, 1020, 'exam item: 身体計測-身長'),
  ('murakami.exam.9n006000000000001', 'EXAM_ITEM_VALUE', NULL, '9N006000000000001', '9N006000000000001', 0, 1030, 'exam item: 身体計測-体重'),
  ('murakami.exam.9n011000000000001', 'EXAM_ITEM_VALUE', NULL, '9N011000000000001', '9N011000000000001', 0, 1040, 'exam item: 身体計測-BMI'),
  ('murakami.exam.9n016160100000001', 'EXAM_ITEM_VALUE', NULL, '9N016160100000001', '9N016160100000001', 0, 1050, 'exam item: 身体計測-腹囲'),
  ('murakami.exam.9e160162100000001', 'EXAM_ITEM_VALUE', NULL, '9E160162100000001', '9E160162100000001', 0, 1060, 'exam item: 視力-視力(右)'),
  ('murakami.exam.9e160162200000001', 'EXAM_ITEM_VALUE', NULL, '9E160162200000001', '9E160162200000001', 0, 1070, 'exam item: 視力-視力(左)'),
  ('murakami.exam.9e160162500000001', 'EXAM_ITEM_VALUE', NULL, '9E160162500000001', '9E160162500000001', 0, 1080, 'exam item: 視力-視力(右:矯正)'),
  ('murakami.exam.9e160162600000001', 'EXAM_ITEM_VALUE', NULL, '9E160162600000001', '9E160162600000001', 0, 1090, 'exam item: 視力-視力(左:矯正)'),
  ('murakami.exam.9e105162100000001', 'EXAM_ITEM_VALUE', NULL, '9E105162100000001', '9E105162100000001', 0, 1100, 'exam item: 眼圧-眼圧'),
  ('murakami.exam.9e105162200000001', 'EXAM_ITEM_VALUE', NULL, '9E105162200000001', '9E105162200000001', 0, 1110, 'exam item: 眼圧-眼圧'),
  ('murakami.exam.9d100163100000011', 'EXAM_ITEM_VALUE', NULL, '9D100163100000011', '9D100163100000011', 0, 1120, 'exam item: 聴力-１０００Hz'),
  ('murakami.exam.9d100163500000011', 'EXAM_ITEM_VALUE', NULL, '9D100163500000011', '9D100163500000011', 0, 1130, 'exam item: 聴力-１０００Hz'),
  ('murakami.exam.9d100163200000011', 'EXAM_ITEM_VALUE', NULL, '9D100163200000011', '9D100163200000011', 0, 1140, 'exam item: 聴力-４０００Hz'),
  ('murakami.exam.9d100163600000011', 'EXAM_ITEM_VALUE', NULL, '9D100163600000011', '9D100163600000011', 0, 1150, 'exam item: 聴力-４０００Hz'),
  ('murakami.exam.9d100160900000049', 'EXAM_ITEM_VALUE', NULL, '9D100160900000049', '9D100160900000049', 0, 1160, 'exam item: 聴力-聴力(その他の所見)'),
  ('murakami.exam.9d100164000000011', 'EXAM_ITEM_VALUE', NULL, '9D100164000000011', '9D100164000000011', 0, 1170, 'exam item: 聴力-聴力（検査方法）'),
  ('murakami.exam.9a751000000000001', 'EXAM_ITEM_VALUE', NULL, '9A751000000000001', '9A751000000000001', 0, 1180, 'exam item: 血圧-収縮期血圧(1回目)'),
  ('murakami.exam.9a752000000000001', 'EXAM_ITEM_VALUE', NULL, '9A752000000000001', '9A752000000000001', 0, 1190, 'exam item: 血圧-収縮期血圧(2回目)'),
  ('murakami.exam.9a755000000000001', 'EXAM_ITEM_VALUE', NULL, '9A755000000000001', '9A755000000000001', 0, 1200, 'exam item: 血圧-収縮期血圧(その他)'),
  ('murakami.exam.9a761000000000001', 'EXAM_ITEM_VALUE', NULL, '9A761000000000001', '9A761000000000001', 0, 1210, 'exam item: 血圧-拡張期血圧(1回目)'),
  ('murakami.exam.9a762000000000001', 'EXAM_ITEM_VALUE', NULL, '9A762000000000001', '9A762000000000001', 0, 1220, 'exam item: 血圧-拡張期血圧(2回目)'),
  ('murakami.exam.9a765000000000001', 'EXAM_ITEM_VALUE', NULL, '9A765000000000001', '9A765000000000001', 0, 1230, 'exam item: 血圧-拡張期血圧(その他)'),
  ('murakami.exam.9n121000000000001', 'EXAM_ITEM_VALUE', NULL, '9N121000000000001', '9N121000000000001', 0, 1240, 'exam item: 血圧-心拍数'),
  ('murakami.exam.1a010000000190111', 'EXAM_ITEM_VALUE', NULL, '1A010000000190111', '1A010000000190111', 0, 1250, 'exam item: 尿一般・腎-蛋白'),
  ('murakami.exam.1a100000000190111', 'EXAM_ITEM_VALUE', NULL, '1A100000000190111', '1A100000000190111', 0, 1260, 'exam item: 尿一般・腎-潜血'),
  ('murakami.exam.3c015000002399901', 'EXAM_ITEM_VALUE', NULL, '3C015000002399901', '3C015000002399901', 0, 1270, 'exam item: 尿一般・腎-クレアチニン'),
  ('murakami.exam.8a065000002391901', 'EXAM_ITEM_VALUE', NULL, '8A065000002391901', '8A065000002391901', 0, 1280, 'exam item: 尿一般・腎-eGFR'),
  ('murakami.exam.3c020000002399901', 'EXAM_ITEM_VALUE', NULL, '3C020000002399901', '3C020000002399901', 0, 1290, 'exam item: 尿酸-尿酸'),
  ('murakami.exam.9c310000000000001', 'EXAM_ITEM_VALUE', NULL, '9C310000000000001', '9C310000000000001', 0, 1300, 'exam item: 肺機能-努力肺活量'),
  ('murakami.exam.9c320000000000001', 'EXAM_ITEM_VALUE', NULL, '9C320000000000001', '9C320000000000001', 0, 1310, 'exam item: 肺機能-1秒量'),
  ('murakami.exam.9c330000000000002', 'EXAM_ITEM_VALUE', NULL, '9C330000000000002', '9C330000000000002', 0, 1320, 'exam item: 肺機能-1秒率'),
  ('murakami.exam.2a010000001930101', 'EXAM_ITEM_VALUE', NULL, '2A010000001930101', '2A010000001930101', 0, 1330, 'exam item: 血液一般-白血球数'),
  ('murakami.exam.2a020000001930101', 'EXAM_ITEM_VALUE', NULL, '2A020000001930101', '2A020000001930101', 0, 1340, 'exam item: 血液一般-赤血球数'),
  ('murakami.exam.2a030000001930101', 'EXAM_ITEM_VALUE', NULL, '2A030000001930101', '2A030000001930101', 0, 1350, 'exam item: 血液一般-ヘモグロビン'),
  ('murakami.exam.2a040000001930102', 'EXAM_ITEM_VALUE', NULL, '2A040000001930102', '2A040000001930102', 0, 1360, 'exam item: 血液一般-ヘマトクリット'),
  ('murakami.exam.2a060000001930101', 'EXAM_ITEM_VALUE', NULL, '2A060000001930101', '2A060000001930101', 0, 1370, 'exam item: 血液一般-MCV'),
  ('murakami.exam.2a070000001930101', 'EXAM_ITEM_VALUE', NULL, '2A070000001930101', '2A070000001930101', 0, 1380, 'exam item: 血液一般-MCH'),
  ('murakami.exam.2a080000001930101', 'EXAM_ITEM_VALUE', NULL, '2A080000001930101', '2A080000001930101', 0, 1390, 'exam item: 血液一般-MCHC'),
  ('murakami.exam.2a050000001930101', 'EXAM_ITEM_VALUE', NULL, '2A050000001930101', '2A050000001930101', 0, 1400, 'exam item: 血液一般-血小板'),
  ('murakami.exam.5c070000002399901', 'EXAM_ITEM_VALUE', NULL, '5C070000002399901', '5C070000002399901', 0, 1410, 'exam item: 炎症-CRP'),
  ('murakami.exam.3b035000002399901', 'EXAM_ITEM_VALUE', NULL, '3B035000002399901', '3B035000002399901', 0, 1420, 'exam item: 肝・胆・膵-AST/GOT'),
  ('murakami.exam.3b045000002399901', 'EXAM_ITEM_VALUE', NULL, '3B045000002399901', '3B045000002399901', 0, 1430, 'exam item: 肝・胆・膵-ALT/GPT'),
  ('murakami.exam.3b090000002399901', 'EXAM_ITEM_VALUE', NULL, '3B090000002399901', '3B090000002399901', 0, 1440, 'exam item: 肝・胆・膵-γ-GTP'),
  ('murakami.exam.3b070000002399901', 'EXAM_ITEM_VALUE', NULL, '3B070000002399901', '3B070000002399901', 0, 1450, 'exam item: 肝・胆・膵-ALP'),
  ('murakami.exam.3b050000002399901', 'EXAM_ITEM_VALUE', NULL, '3B050000002399901', '3B050000002399901', 0, 1460, 'exam item: 肝・胆・膵-LDH'),
  ('murakami.exam.3a010000002399901', 'EXAM_ITEM_VALUE', NULL, '3A010000002399901', '3A010000002399901', 0, 1470, 'exam item: 肝・胆・膵-総蛋白'),
  ('murakami.exam.3f050000002399901', 'EXAM_ITEM_VALUE', NULL, '3F050000002399901', '3F050000002399901', 0, 1480, 'exam item: 脂質-総コレステロール'),
  ('murakami.exam.3f015000002399901', 'EXAM_ITEM_VALUE', NULL, '3F015000002399901', '3F015000002399901', 0, 1490, 'exam item: 脂質-中性脂肪'),
  ('murakami.exam.3f015129902399901', 'EXAM_ITEM_VALUE', NULL, '3F015129902399901', '3F015129902399901', 0, 1500, 'exam item: 脂質-中性脂肪'),
  ('murakami.exam.3f070000002399901', 'EXAM_ITEM_VALUE', NULL, '3F070000002399901', '3F070000002399901', 0, 1510, 'exam item: 脂質-HDLコレステロール'),
  ('murakami.exam.3f077000002399901', 'EXAM_ITEM_VALUE', NULL, '3F077000002399901', '3F077000002399901', 0, 1520, 'exam item: 脂質-LDLコレステロール'),
  ('murakami.exam.3f069000002391901', 'EXAM_ITEM_VALUE', NULL, '3F069000002391901', '3F069000002391901', 0, 1530, 'exam item: 脂質-Nonコレステロール'),
  ('murakami.exam.3d010000001999901', 'EXAM_ITEM_VALUE', NULL, '3D010000001999901', '3D010000001999901', 0, 1540, 'exam item: 糖代謝-血糖'),
  ('murakami.exam.3d010129901999901', 'EXAM_ITEM_VALUE', NULL, '3D010129901999901', '3D010129901999901', 0, 1550, 'exam item: 3D010129901999901'),
  ('murakami.exam.1a020000000190111', 'EXAM_ITEM_VALUE', NULL, '1A020000000190111', '1A020000000190111', 0, 1560, 'exam item: 糖代謝-尿糖'),
  ('murakami.exam.3d046000001999902', 'EXAM_ITEM_VALUE', NULL, '3D046000001999902', '3D046000001999902', 0, 1570, 'exam item: 糖代謝-HbA1c'),
  ('murakami.exam.5d305000002399811', 'EXAM_ITEM_VALUE', NULL, '5D305000002399811', '5D305000002399811', 0, 1580, 'exam item: 腫瘍マーカー-PSA'),
  ('murakami.exam.1b030000001599811', 'EXAM_ITEM_VALUE', NULL, '1B030000001599811', '1B030000001599811', 0, 1590, 'exam item: 便潜血-1回目'),
  ('murakami.exam.9e100166100000011', 'EXAM_ITEM_VALUE', NULL, '9E100166100000011', '9E100166100000011', 0, 1600, 'exam item: 眼底-Scheie'),
  ('murakami.exam.9e100166200000011', 'EXAM_ITEM_VALUE', NULL, '9E100166200000011', '9E100166200000011', 0, 1610, 'exam item: 眼底-Scheie'),
  ('murakami.exam.7a021165008543311', 'EXAM_ITEM_VALUE', NULL, '7A021165008543311', '7A021165008543311', 0, 1620, 'exam item: 子宮-子宮細胞診'),
  ('murakami.exam.9n071000000000049', 'EXAM_ITEM_VALUE', NULL, '9N071000000000049', '9N071000000000049', 0, 1630, 'exam item: 家族歴-[項目なし]'),
  ('murakami.exam.9n501000000000011', 'EXAM_ITEM_VALUE', NULL, '9N501000000000011', '9N501000000000011', 0, 1640, 'exam item: メタボリックシンドローム判定-[項目なし]'),
  ('murakami.exam.9n511000000000049', 'EXAM_ITEM_VALUE', NULL, '9N511000000000049', '9N511000000000049', 0, 1650, 'exam item: 医師の判断-[項目なし]'),
  ('murakami.exam.9n516000000000049', 'EXAM_ITEM_VALUE', NULL, '9N516000000000049', '9N516000000000049', 0, 1660, 'exam item: 9N516000000000049'),
  ('murakami.exam.9n701000000000011', 'EXAM_ITEM_VALUE', NULL, '9N701000000000011', '9N701000000000011', 0, 1670, 'exam item: 1-a．血圧を下げる薬'),
  ('murakami.exam.9n706000000000011', 'EXAM_ITEM_VALUE', NULL, '9N706000000000011', '9N706000000000011', 0, 1680, 'exam item: 2-b．インスリン注射又は血糖を下げる薬'),
  ('murakami.exam.9n711000000000011', 'EXAM_ITEM_VALUE', NULL, '9N711000000000011', '9N711000000000011', 0, 1690, 'exam item: 3-c．コレステロールを下げる薬'),
  ('murakami.exam.9n716000000000011', 'EXAM_ITEM_VALUE', NULL, '9N716000000000011', '9N716000000000011', 0, 1700, 'exam item: 4-医師から脳卒中（脳出血、脳梗塞等）にかかっているといわれたり、治療を受けたことがありますか'),
  ('murakami.exam.9n721000000000011', 'EXAM_ITEM_VALUE', NULL, '9N721000000000011', '9N721000000000011', 0, 1710, 'exam item: 5-医師から心臓病（狭心症、心筋梗塞等）にかかっているといわれたり、治療を受けたことがありますか'),
  ('murakami.exam.9n726000000000011', 'EXAM_ITEM_VALUE', NULL, '9N726000000000011', '9N726000000000011', 0, 1720, 'exam item: 6-医師から、慢性の腎不全にかかっているといわれたり、治療（人工透析）を受けたことがありますか'),
  ('murakami.exam.9n731000000000011', 'EXAM_ITEM_VALUE', NULL, '9N731000000000011', '9N731000000000011', 0, 1730, 'exam item: 7-医師から貧血といわれたことがある'),
  ('murakami.exam.9n736000000000011', 'EXAM_ITEM_VALUE', NULL, '9N736000000000011', '9N736000000000011', 0, 1740, 'exam item: 8-現在、たばこを習慣的に吸っていますか'),
  ('murakami.exam.9n741000000000011', 'EXAM_ITEM_VALUE', NULL, '9N741000000000011', '9N741000000000011', 0, 1750, 'exam item: 9-20歳の時の体重から10kg以上増加している'),
  ('murakami.exam.9n746000000000011', 'EXAM_ITEM_VALUE', NULL, '9N746000000000011', '9N746000000000011', 0, 1760, 'exam item: 10-1回30分以上の軽く汗をかく運動を週2日以上、1年以上実施'),
  ('murakami.exam.9n751000000000011', 'EXAM_ITEM_VALUE', NULL, '9N751000000000011', '9N751000000000011', 0, 1770, 'exam item: 11-日常生活において歩行又は同等の身体活動を1日1時間以上実施'),
  ('murakami.exam.9n756000000000011', 'EXAM_ITEM_VALUE', NULL, '9N756000000000011', '9N756000000000011', 0, 1780, 'exam item: 12-ほぼ同じ年齢の同性と比較して歩く速度が速い'),
  ('murakami.exam.9n872000000000011', 'EXAM_ITEM_VALUE', NULL, '9N872000000000011', '9N872000000000011', 0, 1790, 'exam item: 13-食事をかんで食べる時の状態'),
  ('murakami.exam.9n766000000000011', 'EXAM_ITEM_VALUE', NULL, '9N766000000000011', '9N766000000000011', 0, 1800, 'exam item: 14-人と比較して食べる速度が速い'),
  ('murakami.exam.9n771000000000011', 'EXAM_ITEM_VALUE', NULL, '9N771000000000011', '9N771000000000011', 0, 1810, 'exam item: 15-就寝前の2時間以内に夕食をとることが週に3回以上ある'),
  ('murakami.exam.9n782000000000011', 'EXAM_ITEM_VALUE', NULL, '9N782000000000011', '9N782000000000011', 0, 1820, 'exam item: 16-朝昼夕の3食以外に間食や甘い飲み物を摂取していますか'),
  ('murakami.exam.9n781000000000011', 'EXAM_ITEM_VALUE', NULL, '9N781000000000011', '9N781000000000011', 0, 1830, 'exam item: 17-朝食を抜くことが週に3回以上ある'),
  ('murakami.exam.9n786000000000011', 'EXAM_ITEM_VALUE', NULL, '9N786000000000011', '9N786000000000011', 0, 1840, 'exam item: 18-お酒（日本酒、焼酎、ビール、洋酒など）を飲む頻度はどのくらいですか'),
  ('murakami.exam.9n791000000000011', 'EXAM_ITEM_VALUE', NULL, '9N791000000000011', '9N791000000000011', 0, 1850, 'exam item: 19-飲酒日の1日当たりの飲酒量'),
  ('murakami.exam.9n796000000000011', 'EXAM_ITEM_VALUE', NULL, '9N796000000000011', '9N796000000000011', 0, 1860, 'exam item: 20-睡眠で休養が十分とれている'),
  ('murakami.exam.9n801000000000011', 'EXAM_ITEM_VALUE', NULL, '9N801000000000011', '9N801000000000011', 0, 1870, 'exam item: 21-運動や食生活等の生活習慣を改善してみようと思いますか');

INSERT INTO `phr_master`.`csv_exam_result_mapping_rules` (
  `csv_format_version_id`, `rule_key`, `target_kind`, `target_resolution_type`, `selection_mode`,
  `target_namecode`, `target_field`, `method_structure_type`, `value_source_type`,
  `is_required`, `priority`, `is_active`, `note`
)
SELECT
  @murakami_csv_format_version_id,
  `seed_key`,
  `target_kind`,
  CASE WHEN `target_kind` = 'EXAM_ITEM_VALUE' THEN 'SINGLE_NAMECODE' ELSE 'LEDGER_FIELD' END,
  'DIRECT',
  `target_namecode`,
  `target_field`,
  'SINGLE_COLUMN',
  'SOURCE',
  `is_required`,
  `priority`,
  1,
  CONCAT('seed:', `seed_key`, ':', COALESCE(`note`, ''))
FROM `tmp_murakami_csv_exam_mapping_seed`
ON DUPLICATE KEY UPDATE
  `target_kind` = VALUES(`target_kind`),
  `target_resolution_type` = VALUES(`target_resolution_type`),
  `selection_mode` = VALUES(`selection_mode`),
  `target_namecode` = VALUES(`target_namecode`),
  `target_field` = VALUES(`target_field`),
  `method_structure_type` = VALUES(`method_structure_type`),
  `value_source_type` = VALUES(`value_source_type`),
  `is_required` = VALUES(`is_required`),
  `priority` = VALUES(`priority`),
  `is_active` = VALUES(`is_active`),
  `note` = VALUES(`note`),
  `updated_at` = CURRENT_TIMESTAMP(3);

INSERT INTO `phr_master`.`csv_exam_result_mapping_rules` (
  `csv_format_version_id`, `rule_key`, `target_kind`, `target_resolution_type`, `selection_mode`,
  `target_namecode`, `method_structure_type`, `value_source_type`, `fixed_value`,
  `is_required`, `priority`, `is_active`, `note`
) VALUES
  (@murakami_csv_format_version_id, 'murakami.exam.medical_history_text_from_cd_column', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N056160400000049', 'SINGLE_COLUMN', 'SOURCE', NULL, 0, 1900, 1, 'seed:murakami.exam.medical_history_text_from_cd_column:既往歴欄の文章を具体的な既往歴STとして格納'),
  (@murakami_csv_format_version_id, 'murakami.exam.medical_history_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N056000000000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 1910, 1, 'seed:murakami.exam.medical_history_presence_abnormal:既往歴欄に文章がある場合は既往歴あり CD=1'),
  (@murakami_csv_format_version_id, 'murakami.exam.subjective_symptoms_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N061000000000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 1920, 1, 'seed:murakami.exam.subjective_symptoms_presence_normal:自覚症状 特記事項なし/空欄 -> CD=2'),
  (@murakami_csv_format_version_id, 'murakami.exam.subjective_symptoms_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N061000000000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 1930, 1, 'seed:murakami.exam.subjective_symptoms_presence_abnormal:自覚症状欄に本文がある場合はCD=1'),
  (@murakami_csv_format_version_id, 'murakami.exam.subjective_symptoms_text', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N061160800000049', 'SINGLE_COLUMN', 'SOURCE', NULL, 0, 1940, 1, 'seed:murakami.exam.subjective_symptoms_text:自覚症状STは異常本文のみ格納'),
  (@murakami_csv_format_version_id, 'murakami.exam.objective_symptoms_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N066000000000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 1950, 1, 'seed:murakami.exam.objective_symptoms_presence_normal:他覚症状 特記事項なし/空欄 -> CD=2'),
  (@murakami_csv_format_version_id, 'murakami.exam.objective_symptoms_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N066000000000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 1960, 1, 'seed:murakami.exam.objective_symptoms_presence_abnormal:他覚症状欄に本文がある場合はCD=1'),
  (@murakami_csv_format_version_id, 'murakami.exam.objective_symptoms_text', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N066160800000049', 'SINGLE_COLUMN', 'SOURCE', NULL, 0, 1970, 1, 'seed:murakami.exam.objective_symptoms_text:他覚症状STは異常本文のみ格納'),
  (@murakami_csv_format_version_id, 'murakami.exam.chest_xray_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N206160700000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 1980, 1, 'seed:murakami.exam.chest_xray_presence_normal:胸部X線 異常なし/空欄 -> CD=2'),
  (@murakami_csv_format_version_id, 'murakami.exam.chest_xray_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N206160700000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 1990, 1, 'seed:murakami.exam.chest_xray_presence_abnormal:胸部X線 所見本文あり -> CD=1'),
  (@murakami_csv_format_version_id, 'murakami.exam.chest_xray_text', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N206160800000049', 'SINGLE_COLUMN', 'SOURCE', NULL, 0, 2000, 1, 'seed:murakami.exam.chest_xray_text:胸部X線STは異常本文のみ格納'),
  (@murakami_csv_format_version_id, 'murakami.exam.ecg_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9A110160700000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 2010, 1, 'seed:murakami.exam.ecg_presence_normal:心電図 異常なし/空欄 -> CD=2'),
  (@murakami_csv_format_version_id, 'murakami.exam.ecg_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9A110160700000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 2020, 1, 'seed:murakami.exam.ecg_presence_abnormal:心電図 所見本文あり -> CD=1'),
  (@murakami_csv_format_version_id, 'murakami.exam.ecg_text', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9A110160800000049', 'SINGLE_COLUMN', 'SOURCE', NULL, 0, 2030, 1, 'seed:murakami.exam.ecg_text:心電図STは異常本文のみ格納'),
  (@murakami_csv_format_version_id, 'murakami.exam.gastric_endoscopy_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N266160700000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 2040, 1, 'seed:murakami.exam.gastric_endoscopy_presence_normal:内視鏡 異常なし/空欄 -> CD=2'),
  (@murakami_csv_format_version_id, 'murakami.exam.gastric_endoscopy_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N266160700000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 2050, 1, 'seed:murakami.exam.gastric_endoscopy_presence_abnormal:内視鏡 所見本文あり -> CD=1'),
  (@murakami_csv_format_version_id, 'murakami.exam.gastric_endoscopy_text', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9N266160800000049', 'SINGLE_COLUMN', 'SOURCE', NULL, 0, 2060, 1, 'seed:murakami.exam.gastric_endoscopy_text:内視鏡STは異常本文のみ格納'),
  (@murakami_csv_format_version_id, 'murakami.exam.abdominal_ultrasound_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9F130160700000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 2070, 1, 'seed:murakami.exam.abdominal_ultrasound_presence_normal:腹部超音波 異常なし/空欄 -> CD=2'),
  (@murakami_csv_format_version_id, 'murakami.exam.abdominal_ultrasound_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9F130160700000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 2080, 1, 'seed:murakami.exam.abdominal_ultrasound_presence_abnormal:腹部超音波 所見本文あり -> CD=1'),
  (@murakami_csv_format_version_id, 'murakami.exam.abdominal_ultrasound_text', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9F130160800000049', 'SINGLE_COLUMN', 'SOURCE', NULL, 0, 2090, 1, 'seed:murakami.exam.abdominal_ultrasound_text:腹部超音波STは異常本文のみ格納'),
  (@murakami_csv_format_version_id, 'murakami.exam.breast_ultrasound_presence_normal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9F140160700000011', 'SINGLE_COLUMN', 'FIXED', '2', 0, 2100, 1, 'seed:murakami.exam.breast_ultrasound_presence_normal:乳房超音波 異常なし/空欄 -> CD=2'),
  (@murakami_csv_format_version_id, 'murakami.exam.breast_ultrasound_presence_abnormal', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9F140160700000011', 'SINGLE_COLUMN', 'FIXED', '1', 0, 2110, 1, 'seed:murakami.exam.breast_ultrasound_presence_abnormal:乳房超音波 所見本文あり -> CD=1'),
  (@murakami_csv_format_version_id, 'murakami.exam.breast_ultrasound_text', 'EXAM_ITEM_VALUE', 'SINGLE_NAMECODE', 'DIRECT', '9F140160800000049', 'SINGLE_COLUMN', 'SOURCE', NULL, 0, 2120, 1, 'seed:murakami.exam.breast_ultrasound_text:乳房超音波STは異常本文のみ格納')
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
WHERE r.`csv_format_version_id` = @murakami_csv_format_version_id;

INSERT INTO `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_rule_id`, `condition_group_no`, `condition_type`,
  `locator_type`, `header_name`, `header_occurrence`, `operator`, `source_role`,
  `priority`, `is_active`, `note`
)
SELECT
  r.`csv_exam_result_mapping_rule_id`, 1, 'HEADER_MATCH',
  'HEADER_NAME', s.`header_name`, 1, 'PRESENT', 'VALUE',
  100, 1, CONCAT('seed condition:', s.`seed_key`)
FROM `tmp_murakami_csv_exam_mapping_seed` s
JOIN `phr_master`.`csv_exam_result_mapping_rules` r
  ON r.`csv_format_version_id` = @murakami_csv_format_version_id
 AND r.`rule_key` = s.`seed_key`;

DROP TEMPORARY TABLE IF EXISTS `tmp_murakami_condition_seed`;
CREATE TEMPORARY TABLE `tmp_murakami_condition_seed` (`rule_key` varchar(128), `group_no` int, `condition_type` varchar(32), `header_name` varchar(255), `operator` varchar(32), `expected_value` varchar(255), `source_role` varchar(32), `priority` int);
INSERT INTO `tmp_murakami_condition_seed` VALUES
  ('murakami.exam.medical_history_text_from_cd_column', 1, 'HEADER_MATCH', '9N056000000000011', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.medical_history_text_from_cd_column', 1, 'CELL_VALUE', '9N056000000000011', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.medical_history_text_from_cd_column', 1, 'CELL_VALUE', '9N056000000000011', 'NOT_EQUALS', '異常なし', 'QUALIFIER', 111),
  ('murakami.exam.medical_history_text_from_cd_column', 1, 'CELL_VALUE', '9N056000000000011', 'NOT_EQUALS', '所見なし', 'QUALIFIER', 112),
  ('murakami.exam.medical_history_text_from_cd_column', 1, 'CELL_VALUE', '9N056000000000011', 'NOT_EQUALS', '特記事項なし', 'QUALIFIER', 113),
  ('murakami.exam.medical_history_presence_abnormal', 1, 'HEADER_MATCH', '9N056000000000011', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.medical_history_presence_abnormal', 1, 'CELL_VALUE', '9N056000000000011', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.subjective_symptoms_presence_normal', 1, 'HEADER_MATCH', '9N061000000000011', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.subjective_symptoms_presence_normal', 1, 'CELL_VALUE', '9N061000000000011', 'IN', '異常なし,所見なし,特記事項なし', 'QUALIFIER', 110),
  ('murakami.exam.subjective_symptoms_presence_normal', 2, 'HEADER_MATCH', '9N061000000000011', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.subjective_symptoms_presence_normal', 2, 'CELL_VALUE', '9N061000000000011', 'EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.subjective_symptoms_presence_abnormal', 1, 'HEADER_MATCH', '9N061000000000011', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.subjective_symptoms_presence_abnormal', 1, 'CELL_VALUE', '9N061000000000011', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.subjective_symptoms_presence_abnormal', 1, 'CELL_VALUE', '9N061000000000011', 'NOT_EQUALS', '異常なし', 'QUALIFIER', 111),
  ('murakami.exam.subjective_symptoms_presence_abnormal', 1, 'CELL_VALUE', '9N061000000000011', 'NOT_EQUALS', '所見なし', 'QUALIFIER', 112),
  ('murakami.exam.subjective_symptoms_presence_abnormal', 1, 'CELL_VALUE', '9N061000000000011', 'NOT_EQUALS', '特記事項なし', 'QUALIFIER', 113),
  ('murakami.exam.subjective_symptoms_text', 1, 'HEADER_MATCH', '9N061160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.subjective_symptoms_text', 1, 'CELL_VALUE', '9N061160800000049', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.subjective_symptoms_text', 1, 'CELL_VALUE', '9N061160800000049', 'NOT_EQUALS', '異常なし', 'QUALIFIER', 111),
  ('murakami.exam.subjective_symptoms_text', 1, 'CELL_VALUE', '9N061160800000049', 'NOT_EQUALS', '所見なし', 'QUALIFIER', 112),
  ('murakami.exam.subjective_symptoms_text', 1, 'CELL_VALUE', '9N061160800000049', 'NOT_EQUALS', '特記事項なし', 'QUALIFIER', 113),
  ('murakami.exam.objective_symptoms_presence_normal', 1, 'HEADER_MATCH', '9N066000000000011', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.objective_symptoms_presence_normal', 1, 'CELL_VALUE', '9N066000000000011', 'IN', '異常なし,所見なし,特記事項なし', 'QUALIFIER', 110),
  ('murakami.exam.objective_symptoms_presence_normal', 2, 'HEADER_MATCH', '9N066000000000011', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.objective_symptoms_presence_normal', 2, 'CELL_VALUE', '9N066000000000011', 'EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.objective_symptoms_presence_abnormal', 1, 'HEADER_MATCH', '9N066000000000011', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.objective_symptoms_presence_abnormal', 1, 'CELL_VALUE', '9N066000000000011', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.objective_symptoms_presence_abnormal', 1, 'CELL_VALUE', '9N066000000000011', 'NOT_EQUALS', '異常なし', 'QUALIFIER', 111),
  ('murakami.exam.objective_symptoms_presence_abnormal', 1, 'CELL_VALUE', '9N066000000000011', 'NOT_EQUALS', '所見なし', 'QUALIFIER', 112),
  ('murakami.exam.objective_symptoms_presence_abnormal', 1, 'CELL_VALUE', '9N066000000000011', 'NOT_EQUALS', '特記事項なし', 'QUALIFIER', 113),
  ('murakami.exam.objective_symptoms_text', 1, 'HEADER_MATCH', '9N066160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.objective_symptoms_text', 1, 'CELL_VALUE', '9N066160800000049', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.objective_symptoms_text', 1, 'CELL_VALUE', '9N066160800000049', 'NOT_EQUALS', '異常なし', 'QUALIFIER', 111),
  ('murakami.exam.objective_symptoms_text', 1, 'CELL_VALUE', '9N066160800000049', 'NOT_EQUALS', '所見なし', 'QUALIFIER', 112),
  ('murakami.exam.objective_symptoms_text', 1, 'CELL_VALUE', '9N066160800000049', 'NOT_EQUALS', '特記事項なし', 'QUALIFIER', 113),
  ('murakami.exam.chest_xray_presence_normal', 1, 'HEADER_MATCH', '9N206160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.chest_xray_presence_normal', 1, 'CELL_VALUE', '9N206160800000049', 'IN', '異常なし,所見なし,特記事項なし', 'QUALIFIER', 110),
  ('murakami.exam.chest_xray_presence_normal', 2, 'HEADER_MATCH', '9N206160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.chest_xray_presence_normal', 2, 'CELL_VALUE', '9N206160800000049', 'EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.chest_xray_presence_abnormal', 1, 'HEADER_MATCH', '9N206160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.chest_xray_presence_abnormal', 1, 'CELL_VALUE', '9N206160800000049', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.chest_xray_presence_abnormal', 1, 'CELL_VALUE', '9N206160800000049', 'NOT_EQUALS', '異常なし', 'QUALIFIER', 111),
  ('murakami.exam.chest_xray_presence_abnormal', 1, 'CELL_VALUE', '9N206160800000049', 'NOT_EQUALS', '所見なし', 'QUALIFIER', 112),
  ('murakami.exam.chest_xray_presence_abnormal', 1, 'CELL_VALUE', '9N206160800000049', 'NOT_EQUALS', '特記事項なし', 'QUALIFIER', 113),
  ('murakami.exam.chest_xray_text', 1, 'HEADER_MATCH', '9N206160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.chest_xray_text', 1, 'CELL_VALUE', '9N206160800000049', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.chest_xray_text', 1, 'CELL_VALUE', '9N206160800000049', 'NOT_EQUALS', '異常なし', 'QUALIFIER', 111),
  ('murakami.exam.chest_xray_text', 1, 'CELL_VALUE', '9N206160800000049', 'NOT_EQUALS', '所見なし', 'QUALIFIER', 112),
  ('murakami.exam.chest_xray_text', 1, 'CELL_VALUE', '9N206160800000049', 'NOT_EQUALS', '特記事項なし', 'QUALIFIER', 113),
  ('murakami.exam.ecg_presence_normal', 1, 'HEADER_MATCH', '9A110160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.ecg_presence_normal', 1, 'CELL_VALUE', '9A110160800000049', 'IN', '異常なし,所見なし,特記事項なし', 'QUALIFIER', 110),
  ('murakami.exam.ecg_presence_normal', 2, 'HEADER_MATCH', '9A110160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.ecg_presence_normal', 2, 'CELL_VALUE', '9A110160800000049', 'EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.ecg_presence_abnormal', 1, 'HEADER_MATCH', '9A110160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.ecg_presence_abnormal', 1, 'CELL_VALUE', '9A110160800000049', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.ecg_presence_abnormal', 1, 'CELL_VALUE', '9A110160800000049', 'NOT_EQUALS', '異常なし', 'QUALIFIER', 111),
  ('murakami.exam.ecg_presence_abnormal', 1, 'CELL_VALUE', '9A110160800000049', 'NOT_EQUALS', '所見なし', 'QUALIFIER', 112),
  ('murakami.exam.ecg_presence_abnormal', 1, 'CELL_VALUE', '9A110160800000049', 'NOT_EQUALS', '特記事項なし', 'QUALIFIER', 113),
  ('murakami.exam.ecg_text', 1, 'HEADER_MATCH', '9A110160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.ecg_text', 1, 'CELL_VALUE', '9A110160800000049', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.ecg_text', 1, 'CELL_VALUE', '9A110160800000049', 'NOT_EQUALS', '異常なし', 'QUALIFIER', 111),
  ('murakami.exam.ecg_text', 1, 'CELL_VALUE', '9A110160800000049', 'NOT_EQUALS', '所見なし', 'QUALIFIER', 112),
  ('murakami.exam.ecg_text', 1, 'CELL_VALUE', '9A110160800000049', 'NOT_EQUALS', '特記事項なし', 'QUALIFIER', 113),
  ('murakami.exam.gastric_endoscopy_presence_normal', 1, 'HEADER_MATCH', '9N266160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.gastric_endoscopy_presence_normal', 1, 'CELL_VALUE', '9N266160800000049', 'IN', '異常なし,所見なし,特記事項なし', 'QUALIFIER', 110),
  ('murakami.exam.gastric_endoscopy_presence_normal', 2, 'HEADER_MATCH', '9N266160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.gastric_endoscopy_presence_normal', 2, 'CELL_VALUE', '9N266160800000049', 'EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.gastric_endoscopy_presence_abnormal', 1, 'HEADER_MATCH', '9N266160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.gastric_endoscopy_presence_abnormal', 1, 'CELL_VALUE', '9N266160800000049', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.gastric_endoscopy_presence_abnormal', 1, 'CELL_VALUE', '9N266160800000049', 'NOT_EQUALS', '異常なし', 'QUALIFIER', 111),
  ('murakami.exam.gastric_endoscopy_presence_abnormal', 1, 'CELL_VALUE', '9N266160800000049', 'NOT_EQUALS', '所見なし', 'QUALIFIER', 112),
  ('murakami.exam.gastric_endoscopy_presence_abnormal', 1, 'CELL_VALUE', '9N266160800000049', 'NOT_EQUALS', '特記事項なし', 'QUALIFIER', 113),
  ('murakami.exam.gastric_endoscopy_text', 1, 'HEADER_MATCH', '9N266160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.gastric_endoscopy_text', 1, 'CELL_VALUE', '9N266160800000049', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.gastric_endoscopy_text', 1, 'CELL_VALUE', '9N266160800000049', 'NOT_EQUALS', '異常なし', 'QUALIFIER', 111),
  ('murakami.exam.gastric_endoscopy_text', 1, 'CELL_VALUE', '9N266160800000049', 'NOT_EQUALS', '所見なし', 'QUALIFIER', 112),
  ('murakami.exam.gastric_endoscopy_text', 1, 'CELL_VALUE', '9N266160800000049', 'NOT_EQUALS', '特記事項なし', 'QUALIFIER', 113),
  ('murakami.exam.abdominal_ultrasound_presence_normal', 1, 'HEADER_MATCH', '9F130160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.abdominal_ultrasound_presence_normal', 1, 'CELL_VALUE', '9F130160800000049', 'IN', '異常なし,所見なし,特記事項なし', 'QUALIFIER', 110),
  ('murakami.exam.abdominal_ultrasound_presence_normal', 2, 'HEADER_MATCH', '9F130160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.abdominal_ultrasound_presence_normal', 2, 'CELL_VALUE', '9F130160800000049', 'EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.abdominal_ultrasound_presence_abnormal', 1, 'HEADER_MATCH', '9F130160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.abdominal_ultrasound_presence_abnormal', 1, 'CELL_VALUE', '9F130160800000049', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.abdominal_ultrasound_presence_abnormal', 1, 'CELL_VALUE', '9F130160800000049', 'NOT_EQUALS', '異常なし', 'QUALIFIER', 111),
  ('murakami.exam.abdominal_ultrasound_presence_abnormal', 1, 'CELL_VALUE', '9F130160800000049', 'NOT_EQUALS', '所見なし', 'QUALIFIER', 112),
  ('murakami.exam.abdominal_ultrasound_presence_abnormal', 1, 'CELL_VALUE', '9F130160800000049', 'NOT_EQUALS', '特記事項なし', 'QUALIFIER', 113),
  ('murakami.exam.abdominal_ultrasound_text', 1, 'HEADER_MATCH', '9F130160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.abdominal_ultrasound_text', 1, 'CELL_VALUE', '9F130160800000049', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.abdominal_ultrasound_text', 1, 'CELL_VALUE', '9F130160800000049', 'NOT_EQUALS', '異常なし', 'QUALIFIER', 111),
  ('murakami.exam.abdominal_ultrasound_text', 1, 'CELL_VALUE', '9F130160800000049', 'NOT_EQUALS', '所見なし', 'QUALIFIER', 112),
  ('murakami.exam.abdominal_ultrasound_text', 1, 'CELL_VALUE', '9F130160800000049', 'NOT_EQUALS', '特記事項なし', 'QUALIFIER', 113),
  ('murakami.exam.breast_ultrasound_presence_normal', 1, 'HEADER_MATCH', '9F140160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.breast_ultrasound_presence_normal', 1, 'CELL_VALUE', '9F140160800000049', 'IN', '異常なし,所見なし,特記事項なし', 'QUALIFIER', 110),
  ('murakami.exam.breast_ultrasound_presence_normal', 2, 'HEADER_MATCH', '9F140160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.breast_ultrasound_presence_normal', 2, 'CELL_VALUE', '9F140160800000049', 'EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.breast_ultrasound_presence_abnormal', 1, 'HEADER_MATCH', '9F140160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.breast_ultrasound_presence_abnormal', 1, 'CELL_VALUE', '9F140160800000049', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.breast_ultrasound_presence_abnormal', 1, 'CELL_VALUE', '9F140160800000049', 'NOT_EQUALS', '異常なし', 'QUALIFIER', 111),
  ('murakami.exam.breast_ultrasound_presence_abnormal', 1, 'CELL_VALUE', '9F140160800000049', 'NOT_EQUALS', '所見なし', 'QUALIFIER', 112),
  ('murakami.exam.breast_ultrasound_presence_abnormal', 1, 'CELL_VALUE', '9F140160800000049', 'NOT_EQUALS', '特記事項なし', 'QUALIFIER', 113),
  ('murakami.exam.breast_ultrasound_text', 1, 'HEADER_MATCH', '9F140160800000049', 'PRESENT', NULL, 'VALUE', 100),
  ('murakami.exam.breast_ultrasound_text', 1, 'CELL_VALUE', '9F140160800000049', 'NOT_EMPTY', NULL, 'QUALIFIER', 110),
  ('murakami.exam.breast_ultrasound_text', 1, 'CELL_VALUE', '9F140160800000049', 'NOT_EQUALS', '異常なし', 'QUALIFIER', 111),
  ('murakami.exam.breast_ultrasound_text', 1, 'CELL_VALUE', '9F140160800000049', 'NOT_EQUALS', '所見なし', 'QUALIFIER', 112),
  ('murakami.exam.breast_ultrasound_text', 1, 'CELL_VALUE', '9F140160800000049', 'NOT_EQUALS', '特記事項なし', 'QUALIFIER', 113);

INSERT INTO `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_rule_id`, `condition_group_no`, `condition_type`,
  `locator_type`, `header_name`, `header_occurrence`, `operator`, `expected_value`, `source_role`,
  `priority`, `is_active`, `note`
)
SELECT
  r.`csv_exam_result_mapping_rule_id`, s.`group_no`, s.`condition_type`,
  'HEADER_NAME', s.`header_name`, 1, s.`operator`, s.`expected_value`, s.`source_role`,
  s.`priority`, 1, CONCAT('seed condition:', s.`rule_key`)
FROM `tmp_murakami_condition_seed` s
JOIN `phr_master`.`csv_exam_result_mapping_rules` r
  ON r.`csv_format_version_id` = @murakami_csv_format_version_id
 AND r.`rule_key` = s.`rule_key`;

SELECT
  cfv.`mapping_version`,
  COUNT(DISTINCT r.`csv_exam_result_mapping_rule_id`) AS rule_count,
  COUNT(c.`csv_exam_result_mapping_condition_id`) AS condition_count
FROM `phr_master`.`csv_format_versions` cfv
JOIN `phr_master`.`csv_exam_result_mapping_rules` r
  ON r.`csv_format_version_id` = cfv.`csv_format_version_id`
LEFT JOIN `phr_master`.`csv_exam_result_mapping_conditions` c
  ON c.`csv_exam_result_mapping_rule_id` = r.`csv_exam_result_mapping_rule_id`
WHERE cfv.`csv_format_version_id` = @murakami_csv_format_version_id
GROUP BY cfv.`mapping_version`;

COMMIT;
