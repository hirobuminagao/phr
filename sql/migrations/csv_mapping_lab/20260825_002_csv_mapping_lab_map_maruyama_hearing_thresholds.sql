UPDATE `csv_mapping_lab`.`csv_mapping_rules`
SET
  `target_kind` = 'EXAM_ITEM_VALUE',
  `target_namecode` = CASE `normalized_header_pattern`
    WHEN '1000HZ右' THEN '9D100163100000001'
    WHEN '4000HZ右' THEN '9D100163200000001'
    WHEN '1000HZ左' THEN '9D100163500000001'
    WHEN '4000HZ左' THEN '9D100163600000001'
    ELSE `target_namecode`
  END,
  `target_ledger_field` = NULL,
  `mapping_strategy` = 'DIRECT',
  `confidence` = 0.9500,
  `reason` = '円山CSVの聴力dB原値をJLAC10構成PQ項目として保持。提出XML出力対象外。',
  `updated_by` = 'migration'
WHERE `facility_code` = '0110119070'
  AND `normalized_header_pattern` IN ('1000HZ右', '4000HZ右', '1000HZ左', '4000HZ左')
  AND (`column_no_min` IS NULL OR `column_no_min` <= 176)
  AND (`column_no_max` IS NULL OR `column_no_max` >= 173);

UPDATE `csv_mapping_lab`.`analysis_columns`
SET
  `candidate_target_kind` = 'EXAM_ITEM_VALUE',
  `candidate_namecode` = CASE `normalized_header_name`
    WHEN '1000HZ右' THEN '9D100163100000001'
    WHEN '4000HZ右' THEN '9D100163200000001'
    WHEN '1000HZ左' THEN '9D100163500000001'
    WHEN '4000HZ左' THEN '9D100163600000001'
    ELSE `candidate_namecode`
  END,
  `candidate_ledger_field` = NULL,
  `candidate_confidence` = 0.9500,
  `analysis_note` = 'manual decision: 聴力dB原値をJLAC10構成PQ項目として保持。提出XML出力対象外。'
WHERE `analysis_file_id` = 3
  AND `column_no` IN (173, 174, 175, 176)
  AND `normalized_header_name` IN ('1000HZ右', '4000HZ右', '1000HZ左', '4000HZ左');
