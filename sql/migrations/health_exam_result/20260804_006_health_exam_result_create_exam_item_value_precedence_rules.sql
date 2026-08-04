CREATE TABLE IF NOT EXISTS `health_exam_result`.`exam_item_value_precedence_rules` (
  `precedence_rule_id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '清書値採用例外ルールID',
  `event_id` bigint DEFAULT NULL COMMENT 'NULLならevent共通',
  `exam_facility_id` bigint unsigned DEFAULT NULL COMMENT 'NULLなら施設共通',
  `namecode` char(17) NOT NULL,
  `occurrence_no` int DEFAULT NULL COMMENT 'NULLなら同namecode全occurrence',
  `action` varchar(32) NOT NULL COMMENT 'XML_FIRST/CSV_FIRST/CSV_IF_XML_MATCHES_PATTERN/JOIN_XML_CSV/MANUAL_REVIEW',
  `xml_value_condition_type` varchar(32) DEFAULT NULL COMMENT 'ALWAYS/EQUALS/CONTAINS/REGEXP',
  `xml_value_condition_pattern` text,
  `csv_value_condition_type` varchar(32) DEFAULT NULL COMMENT 'ALWAYS/EQUALS/CONTAINS/REGEXP/NOT_EMPTY',
  `csv_value_condition_pattern` text,
  `join_separator` varchar(64) NOT NULL DEFAULT '\n',
  `priority` int NOT NULL DEFAULT 100,
  `is_active` tinyint(1) NOT NULL DEFAULT 1,
  `note` text,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`precedence_rule_id`),
  KEY `idx_exam_item_value_precedence_rules_lookup` (`is_active`, `event_id`, `exam_facility_id`, `namecode`, `occurrence_no`, `priority`),
  KEY `idx_exam_item_value_precedence_rules_action` (`action`)
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='XML/CSV結合時のnamecode単位採用例外ルール';
