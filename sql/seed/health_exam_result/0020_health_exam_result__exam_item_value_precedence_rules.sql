-- Initial source-precedence exception rules for combined exam ledgers.
--
-- Default merge behavior remains XML-first. These rules are exceptions only.

DELETE FROM `health_exam_result`.`exam_item_value_precedence_rules`
WHERE `event_id` IS NULL
  AND `exam_facility_id` IS NULL
  AND `namecode` = '9N511000000000049'
  AND `occurrence_no` IS NULL
  AND `action` = 'CSV_IF_XML_MATCHES_PATTERN'
  AND `xml_value_condition_type` = 'REGEXP'
  AND `xml_value_condition_pattern` = '^メタボリックシンドローム判定にて(非該当|予備群該当|基準該当)です。$';

INSERT INTO `health_exam_result`.`exam_item_value_precedence_rules` (
  `event_id`,
  `exam_facility_id`,
  `namecode`,
  `occurrence_no`,
  `action`,
  `xml_value_condition_type`,
  `xml_value_condition_pattern`,
  `csv_value_condition_type`,
  `csv_value_condition_pattern`,
  `join_separator`,
  `priority`,
  `is_active`,
  `note`
) VALUES
  (
    NULL,
    NULL,
    '9N511000000000049',
    NULL,
    'CSV_IF_XML_MATCHES_PATTERN',
    'REGEXP',
    '^メタボリックシンドローム判定にて(非該当|予備群該当|基準該当)です。$',
    'NOT_EMPTY',
    NULL,
    '\n',
    100,
    1,
    'If XML doctor judgement is only a verbalized metabolic syndrome judgement, use CSV 9N511 when available.'
  )
;
