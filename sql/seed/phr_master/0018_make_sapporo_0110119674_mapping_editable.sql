UPDATE `phr_master`.`csv_exam_result_mapping_rules`
   SET `rule_origin_type` = 'SCREEN',
       `edit_capability` = 'BASIC_SIMPLE'
 WHERE `csv_format_version_id` = 72
   AND `method_structure_type` IN ('SINGLE_COLUMN', 'MULTI_COLUMN_JOIN')
   AND `value_source_type` = 'SOURCE'
   AND `selection_mode` = 'DIRECT'
   AND `target_kind` IN ('LEDGER_FIELD', 'EXAM_ITEM_VALUE')
   AND (`fixed_value` IS NULL OR `fixed_value` = '');
