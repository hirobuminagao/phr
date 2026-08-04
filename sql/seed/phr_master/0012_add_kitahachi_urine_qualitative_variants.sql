-- Add urine qualitative aliases observed in Sapporo Kitahachi CSV.
--
-- The facility CSV uses parenthesized plus grades. The observed XML for the
-- same examinees showed:
-- - CSV （２＋） -> code 4 / display ＋＋
-- - CSV （４＋） -> code 5 / display ＋＋＋
--
-- CSV （３＋） is intentionally not added until an XML/reference example is
-- confirmed.

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
  (
    '1.2.392.200119.6.2102',
    '（２＋）',
    '（２＋）',
    '4',
    '1.2.392.200119.6.2102',
    '＋＋',
    0,
    90,
    1,
    'Sapporo Kitahachi observed XML alias: urine sugar CSV （２＋） -> code 4/display ＋＋'
  ),
  (
    '1.2.392.200119.6.2102',
    '（４＋）',
    '（４＋）',
    '5',
    '1.2.392.200119.6.2102',
    '＋＋＋',
    0,
    90,
    1,
    'Sapporo Kitahachi observed XML alias: urine sugar CSV （４＋） -> code 5/display ＋＋＋'
  )
ON DUPLICATE KEY UPDATE
  `raw_token_norm` = VALUES(`raw_token_norm`),
  `normalized_code` = VALUES(`normalized_code`),
  `code_system` = VALUES(`code_system`),
  `display_name` = VALUES(`display_name`),
  `priority` = VALUES(`priority`),
  `is_active` = VALUES(`is_active`),
  `note` = VALUES(`note`),
  `updated_at` = CURRENT_TIMESTAMP(6);
