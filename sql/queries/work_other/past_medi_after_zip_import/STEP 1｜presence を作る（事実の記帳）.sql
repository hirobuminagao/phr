/* 01_build_presence.sql */
INSERT INTO work_other.medi_lsio_identity_presence
  (xml_sha256, group_code, identity_item_code, present_flag)

-- ルールあり（ANY_NONEMPTY）
SELECT
  v.xml_sha256,
  g.group_code,
  g.identity_item_code,
  1 AS present_flag
FROM dev_phr.exam_item_group_identity_members g
JOIN work_other.medi_xml_item_values v
  ON FIND_IN_SET(v.namecode, g.required_presence_namecodes) > 0
WHERE g.group_code = 'LSIO_Legal_Item'
  AND g.required_flag = 1
  AND g.presence_value_mode = 'ANY_NONEMPTY'
GROUP BY v.xml_sha256, g.group_code, g.identity_item_code

UNION ALL

-- ルールなし（namecode一致）
SELECT
  v.xml_sha256,
  g.group_code,
  g.identity_item_code,
  1 AS present_flag
FROM dev_phr.exam_item_group_identity_members g
JOIN dev_phr.exam_item_master m
  ON m.identity_item_code = g.identity_item_code
JOIN work_other.medi_xml_item_values v
  ON v.namecode = m.namecode
WHERE g.group_code = 'LSIO_Legal_Item'
  AND g.required_flag = 1
  AND g.required_presence_namecodes IS NULL
GROUP BY v.xml_sha256, g.group_code, g.identity_item_code

ON DUPLICATE KEY UPDATE
  present_flag = VALUES(present_flag);
