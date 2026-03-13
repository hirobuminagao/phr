

CREATE TABLE `work_other`.`hia_dashboard_status_history` (

  `history_id` bigint unsigned NOT NULL AUTO_INCREMENT,

  `hia_dashboard_person_id` bigint unsigned NOT NULL,

  `run_id` bigint unsigned NOT NULL,

  `column_name` varchar(190) NOT NULL,
  `old_value` text,
  `new_value` text,

  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`history_id`),

  KEY `idx_hia_dashboard_history_person` (`hia_dashboard_person_id`),
  KEY `idx_hia_dashboard_history_run` (`run_id`),

  CONSTRAINT `fk_hia_dashboard_history_person`
    FOREIGN KEY (`hia_dashboard_person_id`)
    REFERENCES `work_other`.`hia_dashboard_status` (`hia_dashboard_person_id`)
    ON DELETE CASCADE,

  CONSTRAINT `fk_hia_dashboard_history_run`
    FOREIGN KEY (`run_id`)
    REFERENCES `work_other`.`etl_runs` (`run_id`)

)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;