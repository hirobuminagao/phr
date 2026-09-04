CREATE TABLE `health_exam_result`.`ops_external_feedback_item_details` (
  `external_feedback_item_detail_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `external_feedback_item_id` bigint unsigned NOT NULL,
  `detail_type` varchar(32) NOT NULL COMMENT 'BASIC_INFO/EXAM_ITEM/OTHER',
  `basic_field_code` varchar(64) DEFAULT NULL,
  `namecode` varchar(64) DEFAULT NULL,
  `section_code` varchar(16) DEFAULT NULL,
  `check_item_code` varchar(64) DEFAULT NULL,
  `issue_level` varchar(16) NOT NULL DEFAULT 'ERROR',
  `handling_status` varchar(32) NOT NULL DEFAULT 'OPEN',
  `external_error_code` varchar(128) DEFAULT NULL,
  `external_message` text DEFAULT NULL,
  `reported_value` text DEFAULT NULL,
  `expected_value` text DEFAULT NULL,
  `corrected_value` text DEFAULT NULL,
  `resolution_note` text DEFAULT NULL,
  `created_by` varchar(190) DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_by` varchar(190) DEFAULT NULL,
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`external_feedback_item_detail_id`),
  KEY `idx_ops_external_feedback_item_details_item` (`external_feedback_item_id`),
  KEY `idx_ops_external_feedback_item_details_type_status` (`detail_type`, `handling_status`),
  KEY `idx_ops_external_feedback_item_details_namecode` (`namecode`, `section_code`),
  CONSTRAINT `fk_ops_external_feedback_item_details_item`
    FOREIGN KEY (`external_feedback_item_id`)
    REFERENCES `health_exam_result`.`ops_external_feedback_items` (`external_feedback_item_id`)
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;

INSERT INTO `health_exam_result`.`ops_external_feedback_item_details` (
  external_feedback_item_id, detail_type, namecode, check_item_code,
  issue_level, handling_status, external_error_code, external_message,
  reported_value, resolution_note, created_by, created_at, updated_by, updated_at
)
SELECT
  external_feedback_item_id,
  CASE
    WHEN issue_category IN ('SUBSCRIBER', 'BASIC_INFO') THEN 'BASIC_INFO'
    WHEN issue_category = 'EXAM_ITEM' THEN 'EXAM_ITEM'
    ELSE 'OTHER'
  END,
  namecode, check_item_code, issue_level, handling_status,
  external_error_code, external_message, reported_value, resolution_note,
  created_by, created_at, updated_by, updated_at
FROM `health_exam_result`.`ops_external_feedback_items`;
