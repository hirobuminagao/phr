CREATE TABLE `health_exam_result`.`medical_folder_aliases` (
  `alias_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `event_id` bigint NOT NULL,
  `src_folder_raw` varchar(255) NOT NULL,
  `dst_folder_norm` varchar(255) NOT NULL,
  `manual_judgement` tinyint(1) NOT NULL DEFAULT 0,
  `note` text,
  `is_active` tinyint(1) NOT NULL DEFAULT 1,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`alias_id`),
  UNIQUE KEY `uq_medical_folder_aliases_event_src` (`event_id`, `src_folder_raw`),
  KEY `idx_medical_folder_aliases_event` (`event_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
