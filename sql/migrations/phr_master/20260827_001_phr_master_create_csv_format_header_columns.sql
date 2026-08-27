CREATE TABLE IF NOT EXISTS `phr_master`.`csv_format_header_columns` (
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

INSERT INTO `phr_master`.`csv_format_header_columns` (
  `csv_format_version_id`,
  `column_no`,
  `header_context`,
  `header_name`,
  `normalized_header_name`,
  `header_occurrence`
)
SELECT
  cfv.`csv_format_version_id`,
  CAST(jt.`column_no` AS UNSIGNED),
  NULLIF(jt.`header_context`, ''),
  NULLIF(COALESCE(jt.`header_name`, jt.`name`), ''),
  UPPER(REPLACE(REPLACE(REPLACE(COALESCE(jt.`header_name`, jt.`name`, ''), ' ', ''), '　', ''), '\t', '')),
  COALESCE(CAST(jt.`header_occurrence` AS UNSIGNED), 1)
FROM `phr_master`.`csv_format_versions` AS cfv
JOIN JSON_TABLE(
  cfv.`header_snapshot_json`,
  '$.normalized_columns[*]'
  COLUMNS (
    `column_no` int PATH '$.column_no',
    `header_context` varchar(255) PATH '$.context' NULL ON EMPTY,
    `header_name` varchar(255) PATH '$.header_name' NULL ON EMPTY,
    `name` varchar(255) PATH '$.name' NULL ON EMPTY,
    `header_occurrence` int PATH '$.occurrence' NULL ON EMPTY
  )
) AS jt
WHERE cfv.`header_snapshot_json` IS NOT NULL
  AND jt.`column_no` IS NOT NULL
ON DUPLICATE KEY UPDATE
  `header_context` = VALUES(`header_context`),
  `header_name` = VALUES(`header_name`),
  `normalized_header_name` = VALUES(`normalized_header_name`),
  `header_occurrence` = VALUES(`header_occurrence`);
