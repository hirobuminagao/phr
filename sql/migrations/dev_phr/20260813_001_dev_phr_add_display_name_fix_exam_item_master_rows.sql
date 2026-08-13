-- Add exam_item_master rows required to fill empty displayName in received/exported XML.
-- These rows are promoted from the m4 normalize-error candidate list and are accepted
-- as optional/non-legal-report exam result entries.
--
-- Not included here:
-- - 9N806000000000011: legacy phase-3 questionnaire item, intentionally excluded for phase 4.
-- - Z9F120000Z9725049, Z9N22000000000011, Z9N22160800000049:
--   facility-specific / confirmation-required items. Keep them visible as review targets.

INSERT INTO `dev_phr`.`exam_item_master` (
  `namecode`,
  `item_name`,
  `xml_value_type`,
  `item_code_oid`,
  `result_code_oid`,
  `display_unit`,
  `ucum_unit`,
  `method_name`,
  `category_name`,
  `data_type_label`,
  `xml_method_code`,
  `nullflavor_allowed`,
  `notes`,
  `update_type`,
  `update_reason`,
  `source_last_updated`,
  `kubun_no`,
  `kubun_name`,
  `jun_no`,
  `identity_item_code`,
  `identity_item_name`,
  `annex2_exec_requirement`,
  `annex2_legal_report_flag`,
  `cda_section_code_default`
) VALUES
('Z9N06000000000001', '標準体重', 'PQ', '1.2.392.200119.6.1005', NULL, 'kg', 'kg', NULL, '身体計測', '数字', NULL, NULL, 'Facility-derived standard weight accepted as optional/non-legal-report item from XML ZIP displayName check.', '追加', 'XML ZIPチェックでdisplayName補完対象として検出された任意身体計測値を受けるため追加', '2026-08-13', 200, '任意追加項目', 1550, 'Z9N060', '標準体重', NULL, 0, '01990'),
('9Z5210000Z9625001', '骨塩定量(DIP法)', 'PQ', '1.2.392.200119.6.1005', NULL, NULL, NULL, 'DIP法', '骨密度検査', '数字', '9Z52120009', NULL, 'JLAC10 optional bone density item added from XML ZIP displayName check.', '追加', 'XML ZIPチェックでdisplayName補完対象として検出された任意骨密度検査値を受けるため追加', '2026-08-13', 200, '任意追加項目', 1551, '9Z521', '骨塩定量', NULL, 0, '01990'),
('9Z5210000Z9625002', '骨塩定量(DIP法)対YAM%', 'PQ', '1.2.392.200119.6.1005', NULL, '%', '%', 'DIP法', '骨密度検査', '数字', '9Z52120009', NULL, 'JLAC10 optional bone density item added from XML ZIP displayName check.', '追加', 'XML ZIPチェックでdisplayName補完対象として検出された任意骨密度検査値を受けるため追加', '2026-08-13', 200, '任意追加項目', 1552, '9Z521', '骨塩定量', NULL, 0, '01990'),
('9Z5210000Z9625049', '骨塩定量(DIP法)判定', 'ST', '1.2.392.200119.6.1005', NULL, NULL, NULL, 'DIP法', '骨密度検査', '文字列', '9Z52120009', NULL, 'JLAC10 optional bone density finding added from XML ZIP displayName check.', '追加', 'XML ZIPチェックでdisplayName補完対象として検出された任意骨密度検査判定を受けるため追加', '2026-08-13', 200, '任意追加項目', 1553, '9Z521', '骨塩定量', NULL, 0, '01990'),
('7A021160808543311', '子宮頸部細胞診(所見有無)', 'CD', '1.2.392.200119.6.1005', '1.2.392.200119.6.2002', NULL, NULL, '方法問わず', 'がん検診・生体検査等', 'コード', NULL, NULL, 'Optional cervical cytology finding flag added from XML ZIP displayName check.', '追加', 'XML ZIPチェックでdisplayName補完対象として検出された任意婦人科検査値を受けるため追加', '2026-08-13', 200, '任意追加項目', 1554, '7A021', '子宮頸部細胞診', NULL, 0, '01990'),
('5D120000002399801', 'CA15-3', 'PQ', '1.2.392.200119.6.1005', NULL, 'U/ml', 'U/mL', 'その他', 'がん検診・生体検査等', '数字', '5D12020009', NULL, 'JLAC10 optional tumor marker item added from XML ZIP displayName check.', '追加', 'XML ZIPチェックでdisplayName補完対象として検出された任意腫瘍マーカー値を受けるため追加', '2026-08-13', 200, '任意追加項目', 1555, '5D120', 'CA15-3', NULL, 0, '01990')
ON DUPLICATE KEY UPDATE
  `item_name` = VALUES(`item_name`),
  `xml_value_type` = VALUES(`xml_value_type`),
  `item_code_oid` = VALUES(`item_code_oid`),
  `result_code_oid` = VALUES(`result_code_oid`),
  `display_unit` = VALUES(`display_unit`),
  `ucum_unit` = VALUES(`ucum_unit`),
  `method_name` = VALUES(`method_name`),
  `category_name` = VALUES(`category_name`),
  `data_type_label` = VALUES(`data_type_label`),
  `xml_method_code` = VALUES(`xml_method_code`),
  `nullflavor_allowed` = VALUES(`nullflavor_allowed`),
  `notes` = VALUES(`notes`),
  `update_type` = VALUES(`update_type`),
  `update_reason` = VALUES(`update_reason`),
  `source_last_updated` = VALUES(`source_last_updated`),
  `kubun_no` = VALUES(`kubun_no`),
  `kubun_name` = VALUES(`kubun_name`),
  `jun_no` = VALUES(`jun_no`),
  `identity_item_code` = VALUES(`identity_item_code`),
  `identity_item_name` = VALUES(`identity_item_name`),
  `annex2_exec_requirement` = VALUES(`annex2_exec_requirement`),
  `annex2_legal_report_flag` = VALUES(`annex2_legal_report_flag`),
  `cda_section_code_default` = VALUES(`cda_section_code_default`),
  `updated_at` = CURRENT_TIMESTAMP(6);
