-- Add the JLAC10 CLEIA quantitative variant of PSA (prostate-specific antigen).
--
-- JLAC10 namecode breakdown used here:
-- - analyte: 5D305 = PSA (prostate-specific antigen)
-- - method/result variant: 3052 = CLEIA
-- - result identification: 01 = quantitative value
--
-- References checked on 2026-08-31:
-- - MHLW XML health-examination item information identifies 5D305 as PSA.
--   https://www.mhlw.go.jp/bunya/shakaihosho/iryouseido01/dl/info02i_07.pdf
-- - Published JLAC10 facility comparison material lists
--   5D305000002305201 as PSA with ng/mL and CLEIA.
--   https://happylibus.com/doc/100423/

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
) VALUES (
  '5D305000002305201',
  'PSA(前立腺特異抗原)',
  'PQ',
  '1.2.392.200119.6.1005',
  NULL,
  'ng/ml',
  'ng/mL',
  'CLEIA',
  'がん検診・生体検査等',
  '数字',
  '5D30520005',
  NULL,
  'JLAC10 PSA quantitative CLEIA variant (5D305000002305201).',
  '追加',
  'CSV/XML健診結果取込の任意PSA定量値(CLEIA)を受けるため追加',
  '2026-08-31',
  200,
  '任意追加項目',
  1544,
  '5D305',
  'ＰＳＡ（前立腺特異抗原）',
  NULL,
  0,
  '01990'
)
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
