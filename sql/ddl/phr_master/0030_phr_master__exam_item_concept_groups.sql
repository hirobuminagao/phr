CREATE TABLE `phr_master`.`exam_item_concept_groups` (
  `concept_group_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `concept_group_code` varchar(64) NOT NULL,
  `concept_group_name` varchar(255) NOT NULL,
  `concept_group_kind` varchar(64) NOT NULL,
  `parent_concept_group_id` bigint unsigned DEFAULT NULL,
  `concept_group_category` varchar(64) DEFAULT NULL,
  `description` text,
  `sort_no` int NOT NULL DEFAULT 1000,
  `is_active` tinyint(1) NOT NULL DEFAULT 1,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`concept_group_id`),
  UNIQUE KEY `uq_exam_item_concept_groups_code` (`concept_group_code`),
  KEY `idx_exam_item_concept_groups_kind` (`concept_group_kind`),
  KEY `idx_exam_item_concept_groups_parent` (`parent_concept_group_id`),
  KEY `idx_exam_item_concept_groups_category` (`concept_group_category`),
  KEY `idx_exam_item_concept_groups_active` (`is_active`, `sort_no`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
