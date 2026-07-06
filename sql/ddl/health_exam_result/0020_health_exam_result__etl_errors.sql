CREATE TABLE `health_exam_result`.`etl_errors` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `run_id` bigint unsigned NOT NULL,
  `file_receipt_id` bigint unsigned DEFAULT NULL,
  `xml_ledger_id` bigint unsigned DEFAULT NULL,
  `item_value_id` bigint unsigned DEFAULT NULL,
  `error_type` varchar(64) NOT NULL,
  `error_code` varchar(190) DEFAULT NULL,
  `error_message` text,
  `status` varchar(32) NOT NULL,
  `resolved_by_xml_ledger_id` bigint unsigned DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `resolved_at` datetime(3) DEFAULT NULL,

  PRIMARY KEY (`id`),
  KEY `idx_etl_errors_run` (`run_id`),
  KEY `idx_etl_errors_file_receipt` (`file_receipt_id`),
  KEY `idx_etl_errors_xml_ledger` (`xml_ledger_id`),
  KEY `idx_etl_errors_item_value` (`item_value_id`),
  KEY `idx_etl_errors_error_code` (`error_code`),
  KEY `idx_etl_errors_status_created` (`status`, `created_at`),
  KEY `idx_etl_errors_resolved_by_xml_ledger` (`resolved_by_xml_ledger_id`),

  CONSTRAINT `fk_health_exam_result_etl_errors_run`
    FOREIGN KEY (`run_id`)
    REFERENCES `health_exam_result`.`etl_runs` (`id`)
    ON DELETE CASCADE
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
