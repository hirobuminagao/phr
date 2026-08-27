CREATE TABLE `phr_master`.`csv_format_header_columns` (
  `csv_format_header_column_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `csv_format_version_id` bigint unsigned NOT NULL,
  `column_no` int NOT NULL,
  `header_context` varchar(255) DEFAULT NULL,
  `header_name` varchar(255) DEFAULT NULL,
  `normalized_header_name` varchar(255) DEFAULT NULL,
  `header_occurrence` int NOT NULL DEFAULT 1,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`csv_format_header_column_id`),
  UNIQUE KEY `uq_csv_format_header_columns_format_col` (`csv_format_version_id`, `column_no`),
  KEY `idx_csv_format_header_columns_header` (`csv_format_version_id`, `normalized_header_name`, `header_occurrence`),
  KEY `idx_csv_format_header_columns_context` (`csv_format_version_id`, `header_context`, `header_name`, `header_occurrence`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
