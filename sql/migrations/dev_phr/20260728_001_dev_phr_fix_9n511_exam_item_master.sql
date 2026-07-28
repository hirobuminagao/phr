-- Fix local/execution `dev_phr.exam_item_master` drift for Annex 2 item 9N511.
--
-- Annex 2:
-- - namecode: 9N511000000000049
-- - item: 医師の診断(判定)
-- - XML value type: ST
-- - result_code_oid: none
-- - section: 01010
-- - legal report item: yes
--
-- The repository export already has this shape; this migration is for
-- environments where 9N511 was incorrectly treated as a coded value.
UPDATE `dev_phr`.`exam_item_master`
SET
  `item_name` = '医師の診断(判定)',
  `xml_value_type` = 'ST',
  `result_code_oid` = NULL,
  `display_unit` = NULL,
  `ucum_unit` = NULL,
  `method_name` = NULL,
  `category_name` = '医師の判断',
  `data_type_label` = '文字列',
  `xml_method_code` = NULL,
  `xpath_template` = '//cda:observation[ cda:code/@code=''9N511000000000049'' and cda:code/@codeSystem=''1.2.392.200119.6.1005'']/cda:value',
  `value_method` = 'text()',
  `nullflavor_allowed` = 0,
  `kubun_no` = 400,
  `kubun_name` = '医師の判断',
  `jun_no` = 1950,
  `identity_item_code` = '9N511',
  `identity_item_name` = '医師の診断（判定）',
  `annex2_exec_requirement` = 'MUST',
  `annex2_legal_report_flag` = 1,
  `cda_section_code_default` = '01010',
  `update_type` = '更新',
  `update_reason` = '付属2確認: 9N511は文字列STであり結果コードOIDを持たない',
  `updated_at` = CURRENT_TIMESTAMP(6)
WHERE `namecode` = '9N511000000000049';
