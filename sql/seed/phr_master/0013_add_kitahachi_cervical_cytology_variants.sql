-- Add cervical cytology aliases observed in Sapporo Kitahachi CSV.
--
-- The facility CSV uses full-width "Class" notation. Keep the alias narrow to
-- observed values so unconfirmed classes still surface as normalization errors.

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
    '1.2.392.200119.6.2120',
    'ＣｌａｓｓⅠ',
    'ＣｌａｓｓⅠ',
    '1',
    '1.2.392.200119.6.2120',
    'classI',
    0,
    90,
    1,
    'Sapporo Kitahachi observed CSV alias: full-width Class I'
  ),
  (
    '1.2.392.200119.6.2120',
    'ＣｌａｓｓⅡ',
    'ＣｌａｓｓⅡ',
    '2',
    '1.2.392.200119.6.2120',
    'classII',
    0,
    90,
    1,
    'Sapporo Kitahachi observed CSV alias: full-width Class II'
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
