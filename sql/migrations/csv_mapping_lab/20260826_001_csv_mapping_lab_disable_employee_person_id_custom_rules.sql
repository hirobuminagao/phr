UPDATE `csv_mapping_lab`.`csv_mapping_rules`
SET
  `target_kind` = 'IGNORE',
  `target_namecode` = NULL,
  `target_ledger_field` = NULL,
  `mapping_strategy` = 'IGNORE',
  `confidence` = 0.9500,
  `reason` = 'migration: employee code is local admin column; not imported as person_id_custom',
  `updated_by` = 'migration',
  `updated_at` = CURRENT_TIMESTAMP(3)
WHERE `target_kind` = 'LEDGER_FIELD'
  AND `target_ledger_field` = 'person_id_custom'
  AND (
    `header_pattern` IN ('社員番号', '社員コード', '従業員番号')
    OR `normalized_header_pattern` IN ('社員番号', '社員コード', '従業員番号')
  );
