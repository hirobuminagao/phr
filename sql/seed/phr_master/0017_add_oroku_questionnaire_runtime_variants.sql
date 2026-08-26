-- Seed: add Oroku Hospital questionnaire runtime normalize variants.
-- Source: m4 CSV re-import validation for OROKU_2026_05_JOINED_PATTERN_C_V1.
-- Purpose: keep questionnaire values imported by 0016 from remaining NORMALIZE_VARIANT_NOT_FOUND.

USE `phr_master`;

START TRANSACTION;

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
  ('1.2.392.200119.6.24040', '月に１～３日', '月に１～３日', '5', '1.2.392.200119.6.24040', '月に1～3日', 0, 90, 1, 'Oroku questionnaire alias: drinking frequency full-width digit'),
  ('1.2.392.200119.6.24040', '週１～２日', '週１～２日', '4', '1.2.392.200119.6.24040', '週1～2日', 0, 90, 1, 'Oroku questionnaire alias: drinking frequency full-width digit'),
  ('1.2.392.200119.6.24040', '週３～４日', '週３～４日', '3', '1.2.392.200119.6.24040', '週3～4日', 0, 90, 1, 'Oroku questionnaire alias: drinking frequency full-width digit'),
  ('1.2.392.200119.6.24040', '週５～６日', '週５～６日', '2', '1.2.392.200119.6.24040', '週5～6日', 0, 90, 1, 'Oroku questionnaire alias: drinking frequency full-width digit'),
  ('1.2.392.200119.6.24050', '１～２合未満', '１～２合未満', '2', '1.2.392.200119.6.24050', '1～2合未満', 0, 90, 1, 'Oroku questionnaire alias: drinking amount full-width digit'),
  ('1.2.392.200119.6.24050', '２～３合未満', '２～３合未満', '3', '1.2.392.200119.6.24050', '2～3合未満', 0, 90, 1, 'Oroku questionnaire alias: drinking amount full-width digit'),
  ('1.2.392.200119.6.24050', '３～５合未満', '３～５合未満', '4', '1.2.392.200119.6.24050', '3～5合未満', 0, 90, 1, 'Oroku questionnaire alias: drinking amount full-width digit'),
  ('1.2.392.200119.6.2007', '改善するつもり（６か月以内）', '改善するつもり（６か月以内）', '2', '1.2.392.200119.6.2007', '意志あり（6ヵ月以内）', 0, 90, 1, 'Oroku questionnaire alias: lifestyle improvement stage 2'),
  ('1.2.392.200119.6.2007', '改善するつもり（近いうち）', '改善するつもり（近いうち）', '3', '1.2.392.200119.6.2007', '意志あり（近いうち）', 0, 90, 1, 'Oroku questionnaire alias: lifestyle improvement stage 3'),
  ('1.2.392.200119.6.2007', '既に改善に取り組んでいる（６ヶ月未満）', '既に改善に取り組んでいる（６ヶ月未満）', '4', '1.2.392.200119.6.2007', '取組済み（6ヵ月未満）', 0, 90, 1, 'Oroku questionnaire alias: lifestyle improvement stage 4'),
  ('1.2.392.200119.6.2007', '既に改善に取り組んでいる（６ヶ月以上）', '既に改善に取り組んでいる（６ヶ月以上）', '5', '1.2.392.200119.6.2007', '取組済み（6ヵ月以上）', 0, 90, 1, 'Oroku questionnaire alias: lifestyle improvement stage 5'),
  ('1.2.392.200119.6.18030', 'かみにくいことがある。', 'かみにくいことがある。', '2', '1.2.392.200119.6.18030', 'かみにくい', 0, 90, 1, 'Oroku questionnaire alias: chewing with punctuation')
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

COMMIT;
