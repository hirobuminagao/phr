-- Add common dash aliases for urine qualitative CO values.
--
-- Observed in paper-to-CSV samples:
-- - Murakami Iin uses plain half-width "-" for urine protein, urine sugar,
--   and urine occult blood negative values.
--
-- Result code OID 1.2.392.200119.6.2102 is shared by urine qualitative
-- result values. Standard code 1 means negative / minus.

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
  ('1.2.392.200119.6.2102', '-', '-', '1', '1.2.392.200119.6.2102', '－', 0, 90, 1, 'CSV paper alias: urine qualitative plain half-width dash'),
  ('1.2.392.200119.6.2102', '−', '−', '1', '1.2.392.200119.6.2102', '－', 0, 90, 1, 'CSV paper alias: urine qualitative minus sign'),
  ('1.2.392.200119.6.2102', 'ー', 'ー', '1', '1.2.392.200119.6.2102', '－', 0, 90, 1, 'CSV paper alias: urine qualitative prolonged sound mark used as dash')
ON DUPLICATE KEY UPDATE
  `raw_token_norm` = VALUES(`raw_token_norm`),
  `normalized_code` = VALUES(`normalized_code`),
  `code_system` = VALUES(`code_system`),
  `display_name` = VALUES(`display_name`),
  `priority` = VALUES(`priority`),
  `is_active` = VALUES(`is_active`),
  `note` = VALUES(`note`),
  `updated_at` = CURRENT_TIMESTAMP(6);
