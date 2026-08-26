INSERT INTO `csv_mapping_lab`.`csv_mapping_rules` (
  `scope`, `facility_code`, `event_id`, `condition_type`, `column_no_min`, `column_no_max`,
  `header_pattern`, `normalized_header_pattern`, `value_type`, `target_kind`, `target_namecode`,
  `target_ledger_field`, `mapping_strategy`, `confidence`, `reason`, `created_by`, `updated_by`
)
VALUES
  ('facility', '0110119070', NULL, 'normalized_header_exact', 121, 240, '1000Hz右所見有無', '1000HZ右所見有無', 'CODE', 'EXAM_ITEM_VALUE', '9D100163100000011', NULL, 'DIRECT', 0.9500, '円山CSVの聴力所見有無を提出用CD項目へ対応。', 'migration', 'migration'),
  ('facility', '0110119070', NULL, 'normalized_header_exact', 121, 240, '4000Hz右所見有無', '4000HZ右所見有無', 'CODE', 'EXAM_ITEM_VALUE', '9D100163200000011', NULL, 'DIRECT', 0.9500, '円山CSVの聴力所見有無を提出用CD項目へ対応。', 'migration', 'migration'),
  ('facility', '0110119070', NULL, 'normalized_header_exact', 121, 240, '1000Hz左所見有無', '1000HZ左所見有無', 'CODE', 'EXAM_ITEM_VALUE', '9D100163500000011', NULL, 'DIRECT', 0.9500, '円山CSVの聴力所見有無を提出用CD項目へ対応。', 'migration', 'migration'),
  ('facility', '0110119070', NULL, 'normalized_header_exact', 121, 240, '4000Hz左所見有無', '4000HZ左所見有無', 'CODE', 'EXAM_ITEM_VALUE', '9D100163600000011', NULL, 'DIRECT', 0.9500, '円山CSVの聴力所見有無を提出用CD項目へ対応。', 'migration', 'migration');

UPDATE `csv_mapping_lab`.`analysis_columns`
SET
  `candidate_target_kind` = 'EXAM_ITEM_VALUE',
  `candidate_namecode` = CASE `normalized_header_name`
    WHEN '1000HZ右所見有無' THEN '9D100163100000011'
    WHEN '4000HZ右所見有無' THEN '9D100163200000011'
    WHEN '1000HZ左所見有無' THEN '9D100163500000011'
    WHEN '4000HZ左所見有無' THEN '9D100163600000011'
    ELSE `candidate_namecode`
  END,
  `candidate_ledger_field` = NULL,
  `candidate_confidence` = 0.9500,
  `analysis_note` = 'manual decision: 聴力所見有無を提出用CD項目として採用。'
WHERE `analysis_file_id` = 3
  AND `column_no` IN (178, 179, 180, 181)
  AND `normalized_header_name` IN ('1000HZ右所見有無', '4000HZ右所見有無', '1000HZ左所見有無', '4000HZ左所見有無');
