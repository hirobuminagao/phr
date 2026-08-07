UPDATE `dev_phr`.`exam_item_master`
SET
  `annex2_series_group_identifier` = '1B040Z12015Z0111',
  `annex2_series_group_relation_code` = 'COMP',
  `cda_section_code_default` = '01990',
  `updated_at` = CURRENT_TIMESTAMP(6)
WHERE `namecode` IN (
  '1B040Z121015Z0111',
  '1B040Z122015Z0111'
);
