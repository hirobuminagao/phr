CREATE TABLE `dev_phr`.`etl_runs` (
  `run_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `phase` enum('import','apply') NOT NULL,
  `source` varchar(190) NOT NULL,
  `db_schema` varchar(64) DEFAULT NULL,
  `status` enum('running','success','partial','failed') NOT NULL DEFAULT 'running',
  `started_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `finished_at` datetime(3) DEFAULT NULL,
  `db_path` varchar(190) DEFAULT NULL,
  `input_base` varchar(190) DEFAULT NULL,
  `input_file` varchar(190) DEFAULT NULL,
  `insurer_number` varchar(20) DEFAULT NULL,
  `dry_run` tinyint(1) DEFAULT NULL,
  `limit_rows` int DEFAULT NULL,
  `files` int NOT NULL DEFAULT 0,
  `rows_seen` int NOT NULL DEFAULT 0,
  `rows_inserted` int NOT NULL DEFAULT 0,
  `rows_updated` int NOT NULL DEFAULT 0,
  `rows_unchanged` int NOT NULL DEFAULT 0,
  `rows_skipped` int NOT NULL DEFAULT 0,
  `errors` int NOT NULL DEFAULT 0,
  `notes` text,
  `admin_note` text,

  PRIMARY KEY (`run_id`),
  KEY `idx_etl_runs_insurer_started` (`insurer_number`, `started_at`),
  KEY `idx_etl_runs_phase_started` (`phase`, `started_at`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
