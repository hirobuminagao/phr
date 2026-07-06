CREATE TABLE `health_exam_result`.`etl_runs` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `run_type` varchar(64) NOT NULL,
  `event_id` bigint DEFAULT NULL,
  `started_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `finished_at` datetime(3) DEFAULT NULL,
  `status` varchar(32) NOT NULL,
  `summary_message` text,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`id`),
  KEY `idx_etl_runs_run_type_started` (`run_type`, `started_at`),
  KEY `idx_etl_runs_event_started` (`event_id`, `started_at`),
  KEY `idx_etl_runs_status_started` (`status`, `started_at`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
