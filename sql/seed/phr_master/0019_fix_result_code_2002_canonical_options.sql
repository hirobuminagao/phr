-- Restore canonical options for MHLW result code OID 1.2.392.200119.6.2002.
--
-- Some execution environments have only canonical 3/4 for this OID, which makes
-- hearing CD item choices show only "要再検査" and "検査不適" on the admin UI.

INSERT INTO `phr_master`.`norm_variants` (
  `result_code_oid`,
  `raw_token_norm`,
  `raw_value_utf8`,
  `normalized_code`,
  `code_system`,
  `display_name`,
  `is_canonical`,
  `priority`,
  `is_active`,
  `note`
) VALUES
  ('1.2.392.200119.6.2002', '1', '1', '1', '1.2.392.200119.6.2002', '異常所見あり', 1, 1, 1, 'Restore canonical MHLW 2002 option: 1=異常所見あり.'),
  ('1.2.392.200119.6.2002', '2', '2', '2', '1.2.392.200119.6.2002', '異常所見なし', 1, 2, 1, 'Restore canonical MHLW 2002 option: 2=異常所見なし.')
ON DUPLICATE KEY UPDATE
  `raw_token_norm` = VALUES(`raw_token_norm`),
  `normalized_code` = VALUES(`normalized_code`),
  `code_system` = VALUES(`code_system`),
  `display_name` = VALUES(`display_name`),
  `is_canonical` = VALUES(`is_canonical`),
  `priority` = VALUES(`priority`),
  `is_active` = VALUES(`is_active`),
  `note` = VALUES(`note`),
  `updated_at` = CURRENT_TIMESTAMP(6);
