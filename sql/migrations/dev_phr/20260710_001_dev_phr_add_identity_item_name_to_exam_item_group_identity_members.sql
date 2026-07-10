ALTER TABLE `dev_phr`.`exam_item_group_identity_members`
  ADD COLUMN `identity_item_name` varchar(190) DEFAULT NULL
    COMMENT '同一性項目名称'
    AFTER `identity_item_code`;

UPDATE `dev_phr`.`exam_item_group_identity_members` im
LEFT JOIN (
  SELECT
    identity_item_code,
    COALESCE(MAX(NULLIF(identity_item_name, '')), identity_item_code) AS identity_item_name
  FROM `dev_phr`.`exam_item_master`
  WHERE identity_item_code IS NOT NULL
  GROUP BY identity_item_code
) em
  ON em.identity_item_code = im.identity_item_code
SET im.identity_item_name = COALESCE(em.identity_item_name, im.identity_item_code)
WHERE im.identity_item_name IS NULL;
