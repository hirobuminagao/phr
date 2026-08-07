-- Align exam_item_master XML units with the MHLW Annex 2 source CSV
-- (scripts/work_folder/mat/kenshin_item_master.csv).
--
-- HIA XML export uses ucum_unit as the XML value/@unit source.
-- Do not substitute UCUM-looking aliases here; this column must match Annex 2
-- XML用単位 for the official XML format.

UPDATE `dev_phr`.`exam_item_master`
SET `ucum_unit` = NULL, `updated_at` = CURRENT_TIMESTAMP(6)
WHERE `namecode` IN (
  '3A016000002327102',
  '1A030000000190301',
  '1A030000000199901',
  '9E160162100000001',
  '9E160162500000001',
  '9E160162200000001',
  '9E160162600000001'
);

UPDATE `dev_phr`.`exam_item_master`
SET `ucum_unit` = 'fl', `updated_at` = CURRENT_TIMESTAMP(6)
WHERE `namecode` = '2A060000001930101';

UPDATE `dev_phr`.`exam_item_master`
SET `ucum_unit` = '{times}', `updated_at` = CURRENT_TIMESTAMP(6)
WHERE `namecode` = '9N091000000000001';

UPDATE `dev_phr`.`exam_item_master`
SET `ucum_unit` = 'mg/g*CR', `updated_at` = CURRENT_TIMESTAMP(6)
WHERE `namecode` IN (
  '3A015000000106128',
  '3A015000000199928'
);

UPDATE `dev_phr`.`exam_item_master`
SET `ucum_unit` = 'mg/day', `updated_at` = CURRENT_TIMESTAMP(6)
WHERE `namecode` IN (
  '3A015000000406126',
  '3A015000000499926'
);

UPDATE `dev_phr`.`exam_item_master`
SET `ucum_unit` = '{h`b}/min', `updated_at` = CURRENT_TIMESTAMP(6)
WHERE `namecode` = '9N121000000000001';

UPDATE `dev_phr`.`exam_item_master`
SET `ucum_unit` = '/mm3', `updated_at` = CURRENT_TIMESTAMP(6)
WHERE `namecode` = '2A010000001930101';
