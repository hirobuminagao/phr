UPDATE `phr_master`.`csv_exam_result_mapping_rules` AS r
JOIN `phr_master`.`csv_exam_result_mapping_conditions` AS c
  ON c.`csv_exam_result_mapping_rule_id` = r.`csv_exam_result_mapping_rule_id`
SET
  r.`target_kind` = 'IGNORE',
  r.`target_field` = NULL,
  r.`target_namecode` = NULL,
  r.`target_identity_item_code` = NULL,
  r.`selection_mode` = 'SINGLE',
  r.`selection_group_code` = NULL,
  r.`is_required` = 0,
  r.`updated_at` = CURRENT_TIMESTAMP(3)
WHERE r.`target_kind` = 'LEDGER_FIELD'
  AND r.`target_field` = 'person_id_custom'
  AND c.`header_name` IN ('社員番号', '社員コード', '従業員番号');
