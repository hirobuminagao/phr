CREATE TABLE `health_exam_result`.`etl_errors` (
  `error_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `run_id` bigint unsigned DEFAULT NULL,
  `phase` varchar(64) NOT NULL,
  `source` varchar(190) NOT NULL,
  `insurer_number` varchar(20) DEFAULT NULL,
  `src_file` varchar(190) DEFAULT NULL,
  `src_row_no` int DEFAULT NULL,
  `src_line_no` int DEFAULT NULL,
  `staging_rowid` bigint DEFAULT NULL,
  `person_id_custom` varchar(190) DEFAULT NULL,
  `field` varchar(190) DEFAULT NULL,
  `field_value` text,
  `error_code` varchar(190) DEFAULT NULL,
  `message` text,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`error_id`),
  KEY `idx_etl_errors_run_created` (`run_id`, `created_at`),
  KEY `idx_etl_errors_src` (`src_file`, `src_line_no`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
