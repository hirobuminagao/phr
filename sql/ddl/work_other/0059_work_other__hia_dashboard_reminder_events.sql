CREATE TABLE `work_other`.`hia_dashboard_reminder_events` (

  `event_id` bigint unsigned NOT NULL AUTO_INCREMENT,

  `hia_dashboard_person_id` bigint unsigned NOT NULL,
  `run_id` bigint unsigned NOT NULL,

  `sent_at` datetime(3) NOT NULL,

  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`event_id`),

  UNIQUE KEY `uq_hia_dashboard_reminder_person_sent` (`hia_dashboard_person_id`, `sent_at`),

  KEY `idx_hia_dashboard_reminder_run` (`run_id`),

  CONSTRAINT `fk_hia_dashboard_reminder_person`
    FOREIGN KEY (`hia_dashboard_person_id`)
    REFERENCES `work_other`.`hia_dashboard_status` (`hia_dashboard_person_id`)
    ON DELETE CASCADE,

  CONSTRAINT `fk_hia_dashboard_reminder_run`
    FOREIGN KEY (`run_id`)
    REFERENCES `work_other`.`etl_runs` (`run_id`)

)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
