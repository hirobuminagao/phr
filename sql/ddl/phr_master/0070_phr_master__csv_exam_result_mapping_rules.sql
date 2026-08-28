CREATE TABLE `phr_master`.`csv_exam_result_mapping_rules` (
  `csv_exam_result_mapping_rule_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `csv_format_version_id` bigint unsigned NOT NULL,
  `rule_key` varchar(190) NOT NULL,
  `target_kind` varchar(64) NOT NULL,
  `target_resolution_type` varchar(64) NOT NULL DEFAULT 'SINGLE_NAMECODE',
  `selection_mode` varchar(64) NOT NULL DEFAULT 'DIRECT',
  `selection_group_code` varchar(64) DEFAULT NULL,
  `target_namecode` char(17) DEFAULT NULL,
  `target_identity_item_code` varchar(32) DEFAULT NULL,
  `target_field` varchar(64) DEFAULT NULL,
  `method_structure_type` varchar(64) DEFAULT NULL,
  `value_source_type` varchar(32) NOT NULL DEFAULT 'SOURCE',
  `fixed_value` text,
  `value_join_separator` varchar(32) DEFAULT NULL,
  `value_exclude_values` text,
  `raw_value_type` varchar(32) DEFAULT NULL,
  `raw_unit` varchar(64) DEFAULT NULL,
  `rule_origin_type` varchar(32) NOT NULL DEFAULT 'SEED',
  `edit_capability` varchar(32) NOT NULL DEFAULT 'VIEW_ONLY',
  `is_required` tinyint(1) NOT NULL DEFAULT 0,
  `priority` int NOT NULL DEFAULT 1000,
  `note` text,
  `is_active` tinyint(1) NOT NULL DEFAULT 1,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`csv_exam_result_mapping_rule_id`),
  UNIQUE KEY `uq_csv_exam_result_mapping_rules_key` (`csv_format_version_id`, `rule_key`),
  KEY `idx_csv_exam_result_mapping_rules_format` (`csv_format_version_id`, `is_active`, `priority`),
  KEY `idx_csv_exam_result_mapping_rules_target_kind` (`target_kind`),
  KEY `idx_csv_exam_result_mapping_rules_namecode` (`target_namecode`),
  KEY `idx_csv_exam_result_mapping_rules_identity` (`target_identity_item_code`),
  KEY `idx_csv_exam_result_mapping_rules_selection` (`selection_group_code`, `selection_mode`, `priority`),
  KEY `idx_csv_exam_result_mapping_rules_edit` (`rule_origin_type`, `edit_capability`, `is_active`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
