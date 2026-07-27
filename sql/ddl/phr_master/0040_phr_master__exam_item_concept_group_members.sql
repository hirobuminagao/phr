CREATE TABLE `phr_master`.`exam_item_concept_group_members` (
  `concept_group_member_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `concept_group_id` bigint unsigned NOT NULL,
  `identity_item_code` varchar(32) DEFAULT NULL,
  `namecode` char(17) DEFAULT NULL,
  `member_role` varchar(64) DEFAULT NULL,
  `display_name` varchar(255) DEFAULT NULL,
  `sort_no` int NOT NULL DEFAULT 1000,
  `is_required_candidate` tinyint(1) NOT NULL DEFAULT 0,
  `note` text,
  `is_active` tinyint(1) NOT NULL DEFAULT 1,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`concept_group_member_id`),
  UNIQUE KEY `uq_exam_item_concept_group_members_identity`
    (`concept_group_id`, `identity_item_code`, `namecode`),
  KEY `idx_exam_item_concept_group_members_group` (`concept_group_id`, `is_active`, `sort_no`),
  KEY `idx_exam_item_concept_group_members_identity` (`identity_item_code`),
  KEY `idx_exam_item_concept_group_members_namecode` (`namecode`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
