-- Keep both historical receive-folder aliases while exporting them to one canonical folder.
UPDATE `phr_master`.`medical_folder_aliases`
SET
  `dst_folder_norm` = '2719109346_KKCウエルネス新大阪健診クリニック',
  `updated_at` = CURRENT_TIMESTAMP(3)
WHERE `event_id` = 2
  AND `src_folder_raw` IN (
    '2719109346_KKCウエルネス新大阪健診クリニック',
    '2719109346_KKCウエルネ新大阪健診クリニック'
  );
