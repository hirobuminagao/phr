-- Add event 2 aliases confirmed from the production root folder listing on 2026-07-30.
-- The source listing contains folder names only and is not stored in the repository.
-- Existing historical aliases are intentionally retained because one facility may have
-- more than one accepted source folder name over time.

INSERT INTO `phr_master`.`medical_folder_aliases` (
  `event_id`,
  `src_folder_raw`,
  `dst_folder_norm`,
  `exam_facility_id`,
  `note`,
  `manual_judgement`,
  `is_active`,
  `created_at`,
  `updated_at`
) VALUES
  (2, '0220700066_慈恵クリニック', '0220700066_慈恵クリニック', (SELECT ef.`exam_facility_id` FROM `phr_master`.`exam_facilities` ef WHERE ef.`medical_institution_code` = '0220700066' LIMIT 1), '2026-07-30 実機フォルダ一覧で確認', 0, 1, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3)),
  (2, '0915210181_国際医療福祉大学那須医療センター', '0915210181_国際医療福祉大学那須医療センター', (SELECT ef.`exam_facility_id` FROM `phr_master`.`exam_facilities` ef WHERE ef.`medical_institution_code` = '0915210181' LIMIT 1), '2026-07-30 実機フォルダ一覧で確認', 0, 1, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3)),
  (2, '1110101675_医療法人博仁会共済病院', '1110101675_医療法人博仁会共済病院', (SELECT ef.`exam_facility_id` FROM `phr_master`.`exam_facilities` ef WHERE ef.`medical_institution_code` = '1110101675' LIMIT 1), '2026-07-30 実機フォルダ一覧で確認', 0, 1, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3)),
  (2, '1111303551_伊奈病院', '1111303551_伊奈病院', (SELECT ef.`exam_facility_id` FROM `phr_master`.`exam_facilities` ef WHERE ef.`medical_institution_code` = '1111303551' LIMIT 1), '2026-07-30 実機フォルダ一覧で確認', 0, 1, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3)),
  (2, '1116508881_しおや消化器内科クリニック', '1116508881_しおや消化器内科クリニック', (SELECT ef.`exam_facility_id` FROM `phr_master`.`exam_facilities` ef WHERE ef.`medical_institution_code` = '1116508881' LIMIT 1), '2026-07-30 実機フォルダ一覧で確認', 0, 1, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3)),
  (2, '1311330802_恵比寿健診センター', '1311330802_恵比寿健診センター', (SELECT ef.`exam_facility_id` FROM `phr_master`.`exam_facilities` ef WHERE ef.`medical_institution_code` = '1311330802' LIMIT 1), '2026-07-30 実機フォルダ一覧で確認', 0, 1, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3)),
  (2, '1316020085_ＩＭＳＭｅーＬｉｆｅクリニック渋谷', '1316020085_ＩＭＳＭｅーＬｉｆｅクリニック渋谷', (SELECT ef.`exam_facility_id` FROM `phr_master`.`exam_facilities` ef WHERE ef.`medical_institution_code` = '1316020085' LIMIT 1), '2026-07-30 実機フォルダ一覧で確認', 0, 1, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3)),
  (2, '2010217285_松本市医師会', '2010217285_松本市医師会', (SELECT ef.`exam_facility_id` FROM `phr_master`.`exam_facilities` ef WHERE ef.`medical_institution_code` = '2010217285' LIMIT 1), '2026-07-30 実機フォルダ一覧で確認', 0, 1, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3)),
  (2, '2719109346_KKCウエルネス新大阪健診クリニック', '2719109346_KKCウエルネス新大阪健診クリニック', (SELECT ef.`exam_facility_id` FROM `phr_master`.`exam_facilities` ef WHERE ef.`medical_institution_code` = '2720700059' LIMIT 1), '2026-07-30 実機フォルダ一覧で確認。確認済み採用コード2720700059へ紐付け', 0, 1, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3)),
  (2, '3010111502_星野クリニック', '3010111502_星野クリニック', (SELECT ef.`exam_facility_id` FROM `phr_master`.`exam_facilities` ef WHERE ef.`medical_institution_code` = '3010111502' LIMIT 1), '2026-07-30 実機フォルダ一覧で確認', 0, 1, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3)),
  (2, '3010112062_和歌山市医師会成人病センター', '3010112062_和歌山市医師会成人病センター', (SELECT ef.`exam_facility_id` FROM `phr_master`.`exam_facilities` ef WHERE ef.`medical_institution_code` = '3010112062' LIMIT 1), '2026-07-30 実機フォルダ一覧で確認', 0, 1, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3)),
  (2, '4010215970_福岡労働衛生研究所　健診スクエア博多', '4010215970_福岡労働衛生研究所　健診スクエア博多', (SELECT ef.`exam_facility_id` FROM `phr_master`.`exam_facilities` ef WHERE ef.`medical_institution_code` = '4010215970' LIMIT 1), '2026-07-30 実機フォルダ一覧で確認。旧仮フォルダとは別の正式採番済みフォルダ', 0, 1, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3)),
  (2, '4011229285_福岡労働衛生研究所　労衛研健診センター', '4011229285_福岡労働衛生研究所　労衛研健診センター', (SELECT ef.`exam_facility_id` FROM `phr_master`.`exam_facilities` ef WHERE ef.`medical_institution_code` = '4011229285' LIMIT 1), '2026-07-30 実機フォルダ一覧で確認', 0, 1, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3))
ON DUPLICATE KEY UPDATE
  `dst_folder_norm` = VALUES(`dst_folder_norm`),
  `exam_facility_id` = VALUES(`exam_facility_id`),
  `note` = VALUES(`note`),
  `manual_judgement` = VALUES(`manual_judgement`),
  `is_active` = VALUES(`is_active`),
  `updated_at` = CURRENT_TIMESTAMP(3);

SELECT
  COUNT(*) AS `event2_alias_count`,
  SUM(CASE WHEN `exam_facility_id` IS NULL THEN 1 ELSE 0 END) AS `unresolved_alias_count`
FROM `phr_master`.`medical_folder_aliases`
WHERE `event_id` = 2;
