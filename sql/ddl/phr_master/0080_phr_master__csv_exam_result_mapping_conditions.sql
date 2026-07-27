CREATE TABLE `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_condition_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `csv_exam_result_mapping_rule_id` bigint unsigned NOT NULL,
  `condition_group_no` int NOT NULL DEFAULT 1,
  `condition_type` varchar(64) NOT NULL,
  `locator_type` varchar(64) DEFAULT NULL,
  `header_context` varchar(255) DEFAULT NULL,
  `header_name` varchar(255) DEFAULT NULL,
  `header_occurrence` int DEFAULT NULL,
  `column_no` int DEFAULT NULL,
  `operator` varchar(32) DEFAULT NULL,
  `expected_value` varchar(255) DEFAULT NULL,
  `expected_value_normalized` varchar(255) DEFAULT NULL,
  `source_role` varchar(64) DEFAULT NULL,
  `priority` int NOT NULL DEFAULT 1000,
  `note` text,
  `is_active` tinyint(1) NOT NULL DEFAULT 1,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`csv_exam_result_mapping_condition_id`),
  KEY `idx_csv_exam_result_mapping_conditions_rule` (`csv_exam_result_mapping_rule_id`, `is_active`, `condition_group_no`, `priority`),
  KEY `idx_csv_exam_result_mapping_conditions_type` (`condition_type`),
  KEY `idx_csv_exam_result_mapping_conditions_locator` (`locator_type`, `header_context`, `header_name`, `header_occurrence`, `column_no`),
  KEY `idx_csv_exam_result_mapping_conditions_source_role` (`source_role`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
