CREATE TABLE `dev_phr`.`subscriber_audit` (
  `audit_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `subscriber_id` bigint unsigned NOT NULL,
  `field` varchar(190) COLLATE utf8mb4_ja_0900_as_cs NOT NULL COMMENT '変更されたフィールド名',
  `old_value` text,
  `new_value` text,
  `changed_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '変更日時',
  `source` varchar(50) DEFAULT NULL,
  `note` text,
  `change_run_id` bigint unsigned DEFAULT NULL COMMENT 'etl_runs.run_id',
  PRIMARY KEY (`audit_id`),
  KEY `idx_audit_subscriber` (`subscriber_id`),
  KEY `idx_audit_run` (`change_run_id`),
  CONSTRAINT `fk_audit_run` FOREIGN KEY (`change_run_id`) REFERENCES `dev_phr`.`etl_runs` (`run_id`) ON DELETE SET NULL ON UPDATE RESTRICT,
  CONSTRAINT `fk_audit_subscriber` FOREIGN KEY (`subscriber_id`) REFERENCES `dev_phr`.`subscribers` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;