INSERT INTO `dev_phr`.`exam_item_master` (
  `namecode`, `item_name`, `xml_value_type`, `item_code_oid`, `result_code_oid`,
  `display_unit`, `ucum_unit`, `method_name`, `category_name`, `data_type_label`,
  `xml_method_code`, `xpath_template`, `value_method`, `nullflavor_allowed`,
  `importance`, `importance_group`, `notes`, `update_type`, `update_reason`,
  `source_last_updated`, `kubun_no`, `kubun_name`, `jun_no`,
  `identity_item_code`, `identity_item_name`, `annex2_exec_requirement`,
  `annex2_legal_report_flag`, `cda_section_code_default`,
  `annex2_series_group_identifier`, `annex2_series_group_relation_code`
)
VALUES
  (
    '9D100163100000001', '聴力(右:1000Hz dB)', 'PQ', '1.2.392.200119.6.1005', NULL,
    'dB', 'dB', NULL, 'がん検診・生体検査等', '数字',
    NULL, NULL, NULL, 0,
    NULL, NULL, 'CSV原値保持用。JLAC10構成コード。提出XML出力対象外。', '追加', '聴力dB原値保持用',
    '2026-08-25', 200, 'がん検診・生体検査等', 1581,
    '9D100', '聴力', NULL,
    0, '01990',
    NULL, NULL
  ),
  (
    '9D100163200000001', '聴力(右:4000Hz dB)', 'PQ', '1.2.392.200119.6.1005', NULL,
    'dB', 'dB', NULL, 'がん検診・生体検査等', '数字',
    NULL, NULL, NULL, 0,
    NULL, NULL, 'CSV原値保持用。JLAC10構成コード。提出XML出力対象外。', '追加', '聴力dB原値保持用',
    '2026-08-25', 200, 'がん検診・生体検査等', 1582,
    '9D100', '聴力', NULL,
    0, '01990',
    NULL, NULL
  ),
  (
    '9D100163500000001', '聴力(左:1000Hz dB)', 'PQ', '1.2.392.200119.6.1005', NULL,
    'dB', 'dB', NULL, 'がん検診・生体検査等', '数字',
    NULL, NULL, NULL, 0,
    NULL, NULL, 'CSV原値保持用。JLAC10構成コード。提出XML出力対象外。', '追加', '聴力dB原値保持用',
    '2026-08-25', 200, 'がん検診・生体検査等', 1585,
    '9D100', '聴力', NULL,
    0, '01990',
    NULL, NULL
  ),
  (
    '9D100163600000001', '聴力(左:4000Hz dB)', 'PQ', '1.2.392.200119.6.1005', NULL,
    'dB', 'dB', NULL, 'がん検診・生体検査等', '数字',
    NULL, NULL, NULL, 0,
    NULL, NULL, 'CSV原値保持用。JLAC10構成コード。提出XML出力対象外。', '追加', '聴力dB原値保持用',
    '2026-08-25', 200, 'がん検診・生体検査等', 1586,
    '9D100', '聴力', NULL,
    0, '01990',
    NULL, NULL
  )
ON DUPLICATE KEY UPDATE
  `item_name` = VALUES(`item_name`),
  `xml_value_type` = VALUES(`xml_value_type`),
  `display_unit` = VALUES(`display_unit`),
  `ucum_unit` = VALUES(`ucum_unit`),
  `category_name` = VALUES(`category_name`),
  `data_type_label` = VALUES(`data_type_label`),
  `notes` = VALUES(`notes`),
  `update_type` = VALUES(`update_type`),
  `update_reason` = VALUES(`update_reason`),
  `source_last_updated` = VALUES(`source_last_updated`),
  `kubun_no` = VALUES(`kubun_no`),
  `kubun_name` = VALUES(`kubun_name`),
  `jun_no` = VALUES(`jun_no`),
  `identity_item_code` = VALUES(`identity_item_code`),
  `identity_item_name` = VALUES(`identity_item_name`),
  `annex2_legal_report_flag` = VALUES(`annex2_legal_report_flag`),
  `cda_section_code_default` = VALUES(`cda_section_code_default`);
