-- Backfill result code variants that are required by runtime imports.
-- This file is safe for execution environments. It does not load normalize
-- error fixture rows.

UPDATE `phr_master`.`norm_variants`
SET
  `is_active` = 0,
  `note` = CONCAT(COALESCE(`note`, ''), ' Deactivated: provisional fixture-derived 2121 alias is not part of execution seed.'),
  `updated_at` = CURRENT_TIMESTAMP(6)
WHERE `result_code_oid` = '1.2.392.200119.6.2121'
  AND `note` LIKE 'Observed in exam_item_values_error_20260805 fixture.%'
  AND `note` NOT LIKE '%Deactivated:%';

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
  -- Fill code_system for commonly used aliases where older rows had NULL.
  ('1.2.392.200119.6.2100', '+', '+', '1', '1.2.392.200119.6.2100', '陽性', 0, 90, 1, 'Backfill code_system for positive/negative result aliases.'),
  ('1.2.392.200119.6.2100', '(+)', '(+)', '1', '1.2.392.200119.6.2100', '陽性', 0, 90, 1, 'Backfill code_system for positive/negative result aliases.'),
  ('1.2.392.200119.6.2100', '（＋）', '（＋）', '1', '1.2.392.200119.6.2100', '陽性', 0, 90, 1, 'Backfill code_system for positive/negative result aliases.'),
  ('1.2.392.200119.6.2100', '-', '-', '2', '1.2.392.200119.6.2100', '陰性', 0, 90, 1, 'Backfill code_system for positive/negative result aliases.'),
  ('1.2.392.200119.6.2100', '(-)', '(-)', '2', '1.2.392.200119.6.2100', '陰性', 0, 90, 1, 'Backfill code_system for positive/negative result aliases.'),
  ('1.2.392.200119.6.2100', '（－）', '（－）', '2', '1.2.392.200119.6.2100', '陰性', 0, 90, 1, 'Backfill code_system for positive/negative result aliases.'),
  ('1.2.392.200119.6.2100', '1', '1', '1', '1.2.392.200119.6.2100', '陽性', 0, 90, 1, 'Backfill code_system for positive/negative result aliases.'),
  ('1.2.392.200119.6.2100', '2', '2', '2', '1.2.392.200119.6.2100', '陰性', 0, 90, 1, 'Backfill code_system for positive/negative result aliases.'),
  ('1.2.392.200119.6.2100', '陽性', '陽性', '1', '1.2.392.200119.6.2100', '陽性', 0, 90, 1, 'Backfill code_system for positive/negative result aliases.'),
  ('1.2.392.200119.6.2100', '陰性', '陰性', '2', '1.2.392.200119.6.2100', '陰性', 0, 90, 1, 'Backfill code_system for positive/negative result aliases.'),

  ('1.2.392.200119.6.2102', '1＋', '1＋', '3', '1.2.392.200119.6.2102', '1＋', 0, 90, 1, 'Backfill code_system for qualitative urine aliases.'),
  ('1.2.392.200119.6.2102', '2＋', '2＋', '4', '1.2.392.200119.6.2102', '2＋', 0, 90, 1, 'Backfill code_system for qualitative urine aliases.'),
  ('1.2.392.200119.6.2102', '3＋', '3＋', '5', '1.2.392.200119.6.2102', '3＋', 0, 90, 1, 'Backfill code_system for qualitative urine aliases.'),
  ('1.2.392.200119.6.2102', '±', '±', '2', '1.2.392.200119.6.2102', '±', 0, 90, 1, 'Backfill code_system for qualitative urine aliases.'),
  ('1.2.392.200119.6.2102', '＋', '＋', '3', '1.2.392.200119.6.2102', '1＋', 0, 90, 1, 'Backfill code_system for qualitative urine aliases.'),
  ('1.2.392.200119.6.2102', '＋＋', '＋＋', '4', '1.2.392.200119.6.2102', '2＋', 0, 90, 1, 'Backfill code_system for qualitative urine aliases.'),
  ('1.2.392.200119.6.2102', '＋＋＋', '＋＋＋', '5', '1.2.392.200119.6.2102', '3＋', 0, 90, 1, 'Backfill code_system for qualitative urine aliases.'),
  ('1.2.392.200119.6.2102', '－', '－', '1', '1.2.392.200119.6.2102', '－', 0, 90, 1, 'Backfill code_system for qualitative urine aliases.')
ON DUPLICATE KEY UPDATE
  `raw_token_norm` = VALUES(`raw_token_norm`),
  `normalized_code` = VALUES(`normalized_code`),
  `code_system` = VALUES(`code_system`),
  `display_name` = VALUES(`display_name`),
  `priority` = VALUES(`priority`),
  `is_active` = VALUES(`is_active`),
  `note` = VALUES(`note`),
  `updated_at` = CURRENT_TIMESTAMP(6);
