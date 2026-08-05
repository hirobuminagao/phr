-- Add event 2 aliases observed during the production scan on 2026-08-05.
-- The exam facilities already exist in phr_master.exam_facilities from the
-- Social Insurance Medical Fee Payment Fund open CSV.

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
  (
    2,
    '1315827167_六本木ヒルズ桜十字クリニック',
    '1315827167_六本木ヒルズ桜十字クリニック',
    (SELECT ef.`exam_facility_id` FROM `phr_master`.`exam_facilities` ef WHERE ef.`medical_institution_code` = '1315827167' LIMIT 1),
    '2026-08-05 実機スキャンエラーから追加。支払基金CSV由来の健診機関へ紐付け',
    0,
    1,
    CURRENT_TIMESTAMP(3),
    CURRENT_TIMESTAMP(3)
  ),
  (
    2,
    '4011026459_ガーデンシティ健診プラザ',
    '4011026459_ガーデンシティ健診プラザ',
    (SELECT ef.`exam_facility_id` FROM `phr_master`.`exam_facilities` ef WHERE ef.`medical_institution_code` = '4011026459' LIMIT 1),
    '2026-08-05 実機スキャンエラーから追加。支払基金CSV由来の健診機関へ紐付け',
    0,
    1,
    CURRENT_TIMESTAMP(3),
    CURRENT_TIMESTAMP(3)
  )
ON DUPLICATE KEY UPDATE
  `dst_folder_norm` = VALUES(`dst_folder_norm`),
  `exam_facility_id` = VALUES(`exam_facility_id`),
  `note` = VALUES(`note`),
  `manual_judgement` = VALUES(`manual_judgement`),
  `is_active` = VALUES(`is_active`),
  `updated_at` = CURRENT_TIMESTAMP(3);

SELECT
  `src_folder_raw`,
  `exam_facility_id`,
  `note`
FROM `phr_master`.`medical_folder_aliases`
WHERE `event_id` = 2
  AND `src_folder_raw` IN (
    '1315827167_六本木ヒルズ桜十字クリニック',
    '4011026459_ガーデンシティ健診プラザ'
  )
ORDER BY `src_folder_raw`;
