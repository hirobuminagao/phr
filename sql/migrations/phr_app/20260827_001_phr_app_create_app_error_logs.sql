CREATE DATABASE IF NOT EXISTS `phr_app`
DEFAULT CHARACTER SET utf8mb4
COLLATE utf8mb4_ja_0900_as_cs;

USE `phr_app`;

CREATE TABLE IF NOT EXISTS `app_error_logs` (
  `app_error_log_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `log_id` VARCHAR(40) NOT NULL,
  `status_code` SMALLINT UNSIGNED NOT NULL DEFAULT 500,
  `method` VARCHAR(16) NOT NULL,
  `path` VARCHAR(512) NOT NULL,
  `query_string` TEXT NULL,
  `app_user_id` BIGINT UNSIGNED NULL,
  `employee_no` VARCHAR(64) NULL,
  `client_ip` VARCHAR(64) NULL,
  `user_agent` VARCHAR(512) NULL,
  `exception_type` VARCHAR(255) NOT NULL,
  `exception_message` TEXT NULL,
  `traceback_text` MEDIUMTEXT NULL,
  `admin_note` TEXT NULL,
  `resolved_at` DATETIME NULL,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`app_error_log_id`),
  UNIQUE KEY `uq_app_error_logs_log_id` (`log_id`),
  KEY `idx_app_error_logs_created_at` (`created_at`),
  KEY `idx_app_error_logs_path_created` (`path`, `created_at`),
  KEY `idx_app_error_logs_resolved` (`resolved_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs;
