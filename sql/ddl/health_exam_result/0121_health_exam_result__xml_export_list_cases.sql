CREATE TABLE `health_exam_result`.`xml_export_list_cases` (
  `xml_export_list_case_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `xml_export_list_id` bigint unsigned NOT NULL,
  `exam_export_case_id` bigint unsigned NOT NULL,
  `list_case_status` varchar(32) NOT NULL DEFAULT 'SELECTED' COMMENT 'SELECTED/READY/REMOVED/EXPORTED/EXPORT_ERROR',
  `export_readiness_status_snapshot` varchar(32) DEFAULT NULL,
  `export_readiness_reason_snapshot` text DEFAULT NULL,
  `added_by` varchar(190) DEFAULT NULL,
  `added_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `removed_by` varchar(190) DEFAULT NULL,
  `removed_at` datetime(3) DEFAULT NULL,
  `remove_reason` text DEFAULT NULL,
  `exported_xml_export_member_id` bigint unsigned DEFAULT NULL,
  `exported_at` datetime(3) DEFAULT NULL,
  `export_error_reason` text DEFAULT NULL,
  `list_case_note` text DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`xml_export_list_case_id`),
  UNIQUE KEY `uq_xml_export_list_cases_list_case` (`xml_export_list_id`, `exam_export_case_id`),
  KEY `idx_xml_export_list_cases_case` (`exam_export_case_id`),
  KEY `idx_xml_export_list_cases_status` (`list_case_status`),
  KEY `idx_xml_export_list_cases_export_member` (`exported_xml_export_member_id`),
  CONSTRAINT `fk_xml_export_list_cases_list`
    FOREIGN KEY (`xml_export_list_id`) REFERENCES `health_exam_result`.`xml_export_lists` (`xml_export_list_id`),
  CONSTRAINT `fk_xml_export_list_cases_case`
    FOREIGN KEY (`exam_export_case_id`) REFERENCES `health_exam_result`.`exam_export_cases` (`exam_export_case_id`),
  CONSTRAINT `fk_xml_export_list_cases_export_member`
    FOREIGN KEY (`exported_xml_export_member_id`) REFERENCES `health_exam_result`.`xml_export_members` (`xml_export_member_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
