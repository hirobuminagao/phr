CREATE DATABASE IF NOT EXISTS `csv_mapping_lab`
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_ja_0900_as_cs;

DELETE FROM `csv_mapping_lab`.`csv_mapping_rules`
WHERE `reason` LIKE 'seed: maruyama 001-120%';

INSERT INTO `csv_mapping_lab`.`csv_mapping_rules` (
  `scope`, `facility_code`, `condition_type`, `column_no_min`, `column_no_max`, `header_pattern`, `normalized_header_pattern`,
  `value_type`, `target_kind`, `target_namecode`, `target_ledger_field`,
  `mapping_strategy`, `confidence`, `reason`, `created_by`, `updated_by`
)
VALUES
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '社員番号', '社員番号', 'EMPTY', 'LEDGER_FIELD', NULL, 'person_id_custom', 'DIRECT', 0.9500, 'seed: maruyama 001-120 basic ledger field', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '保険証記号', '保険証記号', 'NUMERIC', 'LEDGER_FIELD', NULL, 'insurance_symbol_raw', 'DIRECT', 0.9500, 'seed: maruyama 001-120 basic ledger field', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '保険証番号', '保険証番号', 'NUMERIC', 'LEDGER_FIELD', NULL, 'insurance_number_raw', 'DIRECT', 0.9500, 'seed: maruyama 001-120 basic ledger field', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '受診日（西暦）', '受診日(西暦)', 'DATE', 'LEDGER_FIELD', NULL, 'exam_date', 'DIRECT', 0.9500, 'seed: maruyama 001-120 basic ledger field', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '漢字氏名', '漢字氏名', 'CODE', 'LEDGER_FIELD', NULL, 'name_full_raw', 'DIRECT', 0.9500, 'seed: maruyama 001-120 basic ledger field', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, 'カナ氏名', 'カナ氏名', 'CODE', 'LEDGER_FIELD', NULL, 'name_kana_raw', 'DIRECT', 0.9500, 'seed: maruyama 001-120 basic ledger field', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '性別', '性別', 'CODE', 'LEDGER_FIELD', NULL, 'gender_raw', 'DIRECT', 0.9500, 'seed: maruyama 001-120 basic ledger field', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '生年月日', '生年月日', 'DATE', 'LEDGER_FIELD', NULL, 'birthdate', 'DIRECT', 0.9500, 'seed: maruyama 001-120 basic ledger field', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '保険者番号', '保険者番号', 'NUMERIC', 'LEDGER_FIELD', NULL, 'insurer_number', 'DIRECT', 0.9500, 'seed: maruyama 001-120 basic ledger field', 'seed', 'seed'),

  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '受診時年齢', '受診時年齢', 'NUMERIC', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 001-120 derived/display column; not imported initially', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '年度年齢', '年度年齢', 'NUMERIC', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 001-120 derived/display column; not imported initially', 'seed', 'seed'),
  ('facility', '0110119070', 'header_contains', 1, 120, '現在はブランク', '現在はブランク', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.9000, 'seed: maruyama 001-120 explicitly blank column', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '保険者名称', '保険者名称', 'CODE', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 001-120 insurer display column; event insurer is fixed separately', 'seed', 'seed'),
  ('facility', '0110119070', 'header_contains', 1, 120, '負担区分', '負担区分', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 001-120 ticket/payment admin column', 'seed', 'seed'),
  ('facility', '0110119070', 'header_contains', 1, 120, '負担内容', '負担内容', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 001-120 ticket/payment admin column', 'seed', 'seed'),
  ('facility', '0110119070', 'header_contains', 1, 120, '受診券', '受診券', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 001-120 empty ticket admin column', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '交付年月日', '交付年月日', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 001-120 empty ticket admin column', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '有効期限', '有効期限', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 001-120 empty ticket admin column', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '事業主健診有無', '事業主健診有無', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 001-120 empty admin column', 'seed', 'seed'),

  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '身長', '身長', 'NUMERIC', 'EXAM_ITEM_VALUE', '9N001000000000001', NULL, 'DIRECT', 0.9500, 'seed: maruyama 001-120 exam item', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '体重', '体重', 'NUMERIC', 'EXAM_ITEM_VALUE', '9N006000000000001', NULL, 'DIRECT', 0.9500, 'seed: maruyama 001-120 exam item', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '標準体重', '標準体重', 'NUMERIC', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 001-120 derived/display column; not imported initially', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '肥満度', '肥満度', 'NUMERIC', 'EXAM_ITEM_VALUE', '9N026000000000002', NULL, 'DIRECT', 0.9000, 'seed: maruyama 001-120 exam item', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, 'ＢＭＩ', 'BMI', 'NUMERIC', 'EXAM_ITEM_VALUE', '9N011000000000001', NULL, 'DIRECT', 0.9500, 'seed: maruyama 001-120 exam item', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '腹囲', '腹囲', 'NUMERIC', 'EXAM_ITEM_VALUE', '9N016160100000001', NULL, 'DIRECT', 0.9500, 'seed: maruyama 001-120 exam item', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '腹囲測定法', '腹囲測定法', 'CODE', 'REVIEW', NULL, NULL, 'NEEDS_CONFIRMATION', 0.7500, 'seed: maruyama 001-120 method discriminator; confirm before import', 'seed', 'seed'),

  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '血圧１高', '血圧1高', 'NUMERIC', 'EXAM_ITEM_VALUE', '9A751000000000001', NULL, 'DIRECT', 0.9000, 'seed: maruyama 001-120 blood pressure first systolic', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '血圧１低', '血圧1低', 'NUMERIC', 'EXAM_ITEM_VALUE', '9A761000000000001', NULL, 'DIRECT', 0.9000, 'seed: maruyama 001-120 blood pressure first diastolic', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '血圧２高', '血圧2高', 'NUMERIC', 'EXAM_ITEM_VALUE', '9A752000000000001', NULL, 'DIRECT', 0.9000, 'seed: maruyama 001-120 blood pressure second systolic', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '血圧２低', '血圧2低', 'NUMERIC', 'EXAM_ITEM_VALUE', '9A762000000000001', NULL, 'DIRECT', 0.9000, 'seed: maruyama 001-120 blood pressure second diastolic', 'seed', 'seed'),

  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '中性脂肪', '中性脂肪', 'NUMERIC', 'EXAM_ITEM_VALUE', '3F015000002327101', NULL, 'DIRECT', 0.8500, 'seed: maruyama 001-120 triglyceride; fasting/random discriminator not confirmed', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, 'HDLｺﾚｽﾃﾛｰﾙ', 'HDLコレステロール', 'NUMERIC', 'EXAM_ITEM_VALUE', '3F070000002327101', NULL, 'DIRECT', 0.9500, 'seed: maruyama 001-120 exam item', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, 'LDLCHO', 'LDLCHO', 'NUMERIC', 'EXAM_ITEM_VALUE', '3F077000002327101', NULL, 'DIRECT', 0.9500, 'seed: maruyama 001-120 exam item', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '血糖', '血糖', 'NUMERIC', 'EXAM_ITEM_VALUE', '3D010000001927201', NULL, 'DIRECT', 0.8500, 'seed: maruyama 001-120 glucose; fasting/random discriminator not confirmed', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, 'HbA1c', 'HBA1C', 'NUMERIC', 'EXAM_ITEM_VALUE', '3D046000001920402', NULL, 'DIRECT', 0.9500, 'seed: maruyama 001-120 exam item', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, 'GOT', 'GOT', 'NUMERIC', 'EXAM_ITEM_VALUE', '3B035000002327201', NULL, 'DIRECT', 0.9500, 'seed: maruyama 001-120 AST/GOT', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, 'GPT', 'GPT', 'NUMERIC', 'EXAM_ITEM_VALUE', '3B045000002327201', NULL, 'DIRECT', 0.9500, 'seed: maruyama 001-120 ALT/GPT', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, 'γ-GTP', 'Γ-GTP', 'NUMERIC', 'EXAM_ITEM_VALUE', '3B090000002327101', NULL, 'DIRECT', 0.9500, 'seed: maruyama 001-120 gamma-GTP', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '尿蛋白', '尿蛋白', 'CODE', 'EXAM_ITEM_VALUE', '1A010000000190111', NULL, 'DIRECT', 0.9500, 'seed: maruyama 001-120 urine protein', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '糖', '糖', 'CODE', 'EXAM_ITEM_VALUE', '1A020000000190111', NULL, 'DIRECT', 0.9000, 'seed: maruyama 001-120 urine sugar; surrounding header is urine judgement', 'seed', 'seed'),

  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '赤血球', '赤血球', 'NUMERIC', 'EXAM_ITEM_VALUE', '2A020000001930101', NULL, 'DIRECT', 0.9500, 'seed: maruyama 001-120 RBC', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '血色素', '血色素', 'NUMERIC', 'EXAM_ITEM_VALUE', '2A030000001930101', NULL, 'DIRECT', 0.9500, 'seed: maruyama 001-120 hemoglobin', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, 'ﾍﾏﾄｸﾘｯﾄ', 'ヘマトクリット', 'NUMERIC', 'EXAM_ITEM_VALUE', '2A040000001930102', NULL, 'DIRECT', 0.9500, 'seed: maruyama 001-120 hematocrit', 'seed', 'seed'),

  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '安静心電図１', '安静心電図1', 'CODE', 'EXAM_ITEM_VALUE', '9A110160800000049', NULL, 'DIRECT', 0.8500, 'seed: maruyama 001-120 ECG finding text series', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '安静心電図２', '安静心電図2', 'CODE', 'EXAM_ITEM_VALUE', '9A110160800000049', NULL, 'DIRECT', 0.8500, 'seed: maruyama 001-120 ECG finding text series', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '安静心電図３', '安静心電図3', 'CODE', 'EXAM_ITEM_VALUE', '9A110160800000049', NULL, 'DIRECT', 0.8500, 'seed: maruyama 001-120 ECG finding text series', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 1, 120, '安静心電図４', '安静心電図4', 'EMPTY', 'EXAM_ITEM_VALUE', '9A110160800000049', NULL, 'DIRECT', 0.7500, 'seed: maruyama 001-120 ECG finding text series; currently empty but reserved column', 'seed', 'seed');
