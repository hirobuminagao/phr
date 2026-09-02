-- Accept common Japanese hearing-result labels for MHLW result code OID 2002.
--
-- MHLW OID 1.2.392.200119.6.2002:
--   1 = abnormal finding
--   2 = no abnormal finding

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
  ('1.2.392.200119.6.2002', '正常', '正常', '2', '1.2.392.200119.6.2002', '異常所見なし', 0, 20, 1, 'Hearing text variant: 正常 -> 2 (異常所見なし).'),
  ('1.2.392.200119.6.2002', '低下', '低下', '1', '1.2.392.200119.6.2002', '異常所見あり', 0, 20, 1, 'Hearing text variant: 低下 -> 1 (異常所見あり).')
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
