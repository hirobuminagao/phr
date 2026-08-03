-- Add questionnaire aliases found during runtime CSV import validation.
--
-- These are raw values observed in medical-result CSV imports where the
-- destination item is CD/CO and the Annex 1 result-code OID is known.

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
    '1.2.392.200119.6.24060',
    '以前は吸っていたが、最近 1 ヶ月間は吸っていない',
    '以前は吸っていたが、最近 1 ヶ月間は吸っていない',
    '2',
    '1.2.392.200119.6.24060',
    '以前は吸っていたが、最近1ヶ月間は吸っていない',
    0,
    90,
    1,
    'runtime alias: smoking former smoker with spaces'
  ),
  (
    '1.2.392.200119.6.24040',
    '飲酒（週5～6日）',
    '飲酒（週5～6日）',
    '2',
    '1.2.392.200119.6.24040',
    '週5～6日',
    0,
    90,
    1,
    'runtime alias: drinking frequency with item label'
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
