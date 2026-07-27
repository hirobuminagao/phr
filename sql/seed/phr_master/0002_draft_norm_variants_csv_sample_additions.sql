-- Draft additions for CSV sample normalize verification.
--
-- Scope:
-- - Safe aliases observed in Hirooka / Heartcross sample CSVs.
-- - Facility judgement values are intentionally excluded.
-- - Free-text findings accidentally mapped to CD should remain errors.

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
  ('1.2.392.200119.6.2102', '(-)', '(-)', '1', '1.2.392.200119.6.2102', '－', 0, 90, 1, 'CSV sample alias: urine qualitative minus with parentheses'),
  ('1.2.392.200119.6.2102', '(±)', '(±)', '2', '1.2.392.200119.6.2102', '±', 0, 90, 1, 'CSV sample alias: urine qualitative plus-minus with parentheses'),
  ('1.2.392.200119.6.2102', '(+)', '(+)', '3', '1.2.392.200119.6.2102', '1＋', 0, 90, 1, 'CSV sample alias: urine qualitative one-plus with parentheses'),
  ('1.2.392.200119.6.3001', '情報提供', '情報提供', '3', '1.2.392.200119.6.3001', 'なし（情報提供）', 0, 90, 1, 'CSV sample alias: health guidance information only'),
  ('1.2.392.200119.6.3001', '動機付け支援', '動機付け支援', '2', '1.2.392.200119.6.3001', '動機づけ支援', 0, 90, 1, 'CSV sample alias: spelling variant'),
  ('1.2.392.200119.6.2001', '異常所見なし', '異常所見なし', '2', '1.2.392.200119.6.2001', '特記事項なし', 0, 90, 1, 'CSV sample alias: objective finding none'),
  ('1.2.392.200119.6.2202', '10.0', '10.0', '2', '1.2.392.200119.6.2202', '10時間以上', 0, 90, 1, 'CSV sample alias: postprandial hours numeric'),
  ('1.2.392.200119.6.2202', '12.0', '12.0', '2', '1.2.392.200119.6.2202', '10時間以上', 0, 90, 1, 'CSV sample alias: postprandial hours numeric'),
  ('1.2.392.200119.6.2202', '15.0', '15.0', '2', '1.2.392.200119.6.2202', '10時間以上', 0, 90, 1, 'CSV sample alias: postprandial hours numeric')
ON DUPLICATE KEY UPDATE
  `raw_token_norm` = VALUES(`raw_token_norm`),
  `normalized_code` = VALUES(`normalized_code`),
  `code_system` = VALUES(`code_system`),
  `display_name` = VALUES(`display_name`),
  `priority` = VALUES(`priority`),
  `is_active` = VALUES(`is_active`),
  `note` = VALUES(`note`),
  `updated_at` = CURRENT_TIMESTAMP(6);
