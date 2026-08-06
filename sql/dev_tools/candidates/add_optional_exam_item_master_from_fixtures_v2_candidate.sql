-- Candidate: add additional optional exam_item_master rows found from anonymized normalize error fixtures.
--
-- Scope:
-- - Add official/JLAC-like optional items that can safely be represented as exam result entries.
-- - Do not add facility/custom ZG/Z* guidance codes or standard weight here; those remain excluded/non-target.
--
-- This is an m4 review candidate, not an execution-environment migration.
-- Promote selected rows to a formal dev_phr migration/seed only after review.

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
('Z9N06000000000001', '標準体重', 'PQ', '1.2.392.200119.6.1005', NULL, 'kg', 'kg', NULL, '身体計測', '数字', NULL, NULL, 'Facility-derived standard weight accepted as optional/non-legal-report item from normalize error fixture.', '追加', 'CSV/XML健診結果取込の任意身体計測値を受けるため追加', '2026-08-06', 200, '任意追加項目', 1515, 'Z9N060', '標準体重', NULL, 0, '01990'),
('9Z5070000Z9225001', '骨塩定量(DXA/DEX法)', 'PQ', '1.2.392.200119.6.1005', NULL, NULL, NULL, 'DXA/DEX法', '骨密度検査', '数字', '9Z50720009', NULL, 'JLAC10 optional bone density item added from normalize error fixture.', '追加', 'CSV/XML健診結果取込の任意骨密度検査値を受けるため追加', '2026-08-06', 200, '任意追加項目', 1516, '9Z507', '骨塩定量', NULL, 0, '01990'),
('9Z5210000Z9625001', '骨塩定量(DIP法)', 'PQ', '1.2.392.200119.6.1005', NULL, NULL, NULL, 'DIP法', '骨密度検査', '数字', '9Z52120009', NULL, 'JLAC10 optional bone density item added from normalize error fixture.', '追加', 'CSV/XML健診結果取込の任意骨密度検査値を受けるため追加', '2026-08-06', 200, '任意追加項目', 1517, '9Z521', '骨塩定量', NULL, 0, '01990'),
('9Z5210000Z9625002', '骨塩定量(DIP法)対YAM%', 'PQ', '1.2.392.200119.6.1005', NULL, '%', '%', 'DIP法', '骨密度検査', '数字', '9Z52120009', NULL, 'JLAC10 optional bone density item added from normalize error fixture.', '追加', 'CSV/XML健診結果取込の任意骨密度検査値を受けるため追加', '2026-08-06', 200, '任意追加項目', 1518, '9Z521', '骨塩定量', NULL, 0, '01990'),
('9Z5210000Z9625049', '骨塩定量(DIP法)判定', 'ST', '1.2.392.200119.6.1005', NULL, NULL, NULL, 'DIP法', '骨密度検査', '文字列', '9Z52120009', NULL, 'JLAC10 optional bone density finding added from normalize error fixture.', '追加', 'CSV/XML健診結果取込の任意骨密度検査判定を受けるため追加', '2026-08-06', 200, '任意追加項目', 1519, '9Z521', '骨塩定量', NULL, 0, '01990'),

('7A021160808543311', '子宮頸部細胞診(所見有無)', 'CD', '1.2.392.200119.6.1005', '1.2.392.200119.6.2002', NULL, NULL, '方法問わず', 'がん検診・生体検査等', 'コード', NULL, NULL, 'Optional cervical cytology finding flag added from normalize error fixture.', '追加', 'CSV/XML健診結果取込の任意婦人科検査値を受けるため追加', '2026-08-06', 200, '任意追加項目', 1520, '7A021', '子宮頸部細胞診', NULL, 0, '01990'),
('7A021160808543349', '子宮頸部細胞診(所見)', 'ST', '1.2.392.200119.6.1005', NULL, NULL, NULL, '方法問わず', 'がん検診・生体検査等', '文字列', NULL, NULL, 'Optional cervical cytology finding text added from normalize error fixture.', '追加', 'CSV/XML健診結果取込の任意婦人科検査値を受けるため追加', '2026-08-06', 200, '任意追加項目', 1521, '7A021', '子宮頸部細胞診', NULL, 0, '01990'),
('9F160160700000011', '婦人科超音波検査(所見有無)', 'CD', '1.2.392.200119.6.1005', '1.2.392.200119.6.2002', NULL, NULL, '超音波検査', 'がん検診・生体検査等', 'コード', NULL, NULL, 'Optional gynecological ultrasound finding flag added from normalize error fixture.', '追加', 'CSV/XML健診結果取込の任意婦人科検査値を受けるため追加', '2026-08-06', 200, '任意追加項目', 1522, '9F160', '婦人科超音波検査', NULL, 0, '01990'),
('9F160160800000049', '婦人科超音波検査(所見)', 'ST', '1.2.392.200119.6.1005', NULL, NULL, NULL, '超音波検査', 'がん検診・生体検査等', '文字列', NULL, NULL, 'Optional gynecological ultrasound finding text added from normalize error fixture.', '追加', 'CSV/XML健診結果取込の任意婦人科検査値を受けるため追加', '2026-08-06', 200, '任意追加項目', 1523, '9F160', '婦人科超音波検査', NULL, 0, '01990'),

('5D015000002302311', 'AFP判定', 'CD', '1.2.392.200119.6.1005', '1.2.392.200119.6.2100', NULL, NULL, 'EIA', 'がん検診・生体検査等', 'コード', '5D01520003', NULL, 'Optional tumor marker qualitative judgement added from normalize error fixture.', '追加', 'CSV/XML健診結果取込の任意腫瘍マーカー判定を受けるため追加', '2026-08-06', 200, '任意追加項目', 1524, '5D015', 'AFP', NULL, 0, '01990'),
('5D100000002305101', 'CA125', 'PQ', '1.2.392.200119.6.1005', NULL, 'U/ml', 'U/mL', 'CLIA', 'がん検診・生体検査等', '数字', '5D10020005', NULL, 'JLAC10 optional tumor marker item added from normalize error fixture.', '追加', 'CSV/XML健診結果取込の任意腫瘍マーカー値を受けるため追加', '2026-08-06', 200, '任意追加項目', 1525, '5D100', 'CA125', NULL, 0, '01990'),
('5D130000002305101', 'CA19-9', 'PQ', '1.2.392.200119.6.1005', NULL, 'U/ml', 'U/mL', 'CLIA', 'がん検診・生体検査等', '数字', '5D13020005', NULL, 'JLAC10 optional tumor marker item added from normalize error fixture.', '追加', 'CSV/XML健診結果取込の任意腫瘍マーカー値を受けるため追加', '2026-08-06', 200, '任意追加項目', 1526, '5D130', 'CA19-9', NULL, 0, '01990'),
('5D120000002399801', 'CA15-3', 'PQ', '1.2.392.200119.6.1005', NULL, 'U/ml', 'U/mL', 'その他', 'がん検診・生体検査等', '数字', '5D12020009', NULL, 'JLAC10 optional tumor marker item added from normalize error fixture.', '追加', 'CSV/XML健診結果取込の任意腫瘍マーカー値を受けるため追加', '2026-08-06', 200, '任意追加項目', 1527, '5D120', 'CA15-3', NULL, 0, '01990'),
('5E065000002302311', '血清ヘリコバクターピロリ抗体IgG判定', 'CD', '1.2.392.200119.6.1005', '1.2.392.200119.6.2100', NULL, NULL, 'EIA', '血液検査', 'コード', '5E06520003', NULL, 'Optional Helicobacter pylori antibody judgement added from normalize error fixture.', '追加', 'CSV/XML健診結果取込の任意ピロリ検査判定を受けるため追加', '2026-08-06', 200, '任意追加項目', 1528, '5E065', 'ヘリコバクターピロリ抗体', NULL, 0, '01990'),

('3B347000002392051', 'ABCD分類判定', 'ST', '1.2.392.200119.6.1005', NULL, NULL, NULL, 'その他', '胃がんリスク検査', '文字列', '3B34720009', NULL, 'Optional gastric cancer risk item added from normalize error fixture.', '追加', 'CSV/XML健診結果取込の任意胃がんリスク検査値を受けるため追加', '2026-08-06', 200, '任意追加項目', 1529, '3B347', 'ABCD分類', NULL, 0, '01990'),
('3B347000002392052', 'ヘリコバクターピロリ抗体', 'PQ', '1.2.392.200119.6.1005', NULL, NULL, NULL, 'その他', '胃がんリスク検査', '数字', '3B34720009', NULL, 'Optional gastric cancer risk item added from normalize error fixture.', '追加', 'CSV/XML健診結果取込の任意胃がんリスク検査値を受けるため追加', '2026-08-06', 200, '任意追加項目', 1530, '3B347', 'ヘリコバクターピロリ抗体', NULL, 0, '01990'),
('3B347000002392053', 'ペプシノゲン1', 'PQ', '1.2.392.200119.6.1005', NULL, 'ng/ml', 'ng/mL', 'その他', '胃がんリスク検査', '数字', '3B34720009', NULL, 'Optional gastric cancer risk item added from normalize error fixture.', '追加', 'CSV/XML健診結果取込の任意胃がんリスク検査値を受けるため追加', '2026-08-06', 200, '任意追加項目', 1531, '3B347', 'ペプシノゲン1', NULL, 0, '01990'),
('3B347000002392054', 'ペプシノゲン2', 'PQ', '1.2.392.200119.6.1005', NULL, 'ng/ml', 'ng/mL', 'その他', '胃がんリスク検査', '数字', '3B34720009', NULL, 'Optional gastric cancer risk item added from normalize error fixture.', '追加', 'CSV/XML健診結果取込の任意胃がんリスク検査値を受けるため追加', '2026-08-06', 200, '任意追加項目', 1532, '3B347', 'ペプシノゲン2', NULL, 0, '01990'),
('3B347000002392055', 'ペプシノゲン1/2比', 'PQ', '1.2.392.200119.6.1005', NULL, NULL, NULL, 'その他', '胃がんリスク検査', '数字', '3B34720009', NULL, 'Optional gastric cancer risk item added from normalize error fixture.', '追加', 'CSV/XML健診結果取込の任意胃がんリスク検査値を受けるため追加', '2026-08-06', 200, '任意追加項目', 1533, '3B347', 'ペプシノゲン1/2比', NULL, 0, '01990'),
('3B347000002392056', 'ペプシノゲン判定', 'CD', '1.2.392.200119.6.1005', '1.2.392.200119.6.2102', NULL, NULL, 'その他', '胃がんリスク検査', 'コード', '3B34720009', NULL, 'Optional gastric cancer risk judgement added from normalize error fixture.', '追加', 'CSV/XML健診結果取込の任意胃がんリスク検査判定を受けるため追加', '2026-08-06', 200, '任意追加項目', 1534, '3B347', 'ペプシノゲン判定', NULL, 0, '01990')
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
