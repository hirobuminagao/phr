CREATE TABLE IF NOT EXISTS `app_users` (
  `app_user_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `employee_no` varchar(32) COLLATE utf8mb4_ja_0900_as_cs NOT NULL COMMENT '社員番号。ログインIDとして使用する',
  `display_name` varchar(190) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,
  `display_name_kana` varchar(190) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `department_name` varchar(190) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `email` varchar(255) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `password_hash` varchar(255) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,
  `password_hash_algorithm` varchar(32) COLLATE utf8mb4_ja_0900_as_cs NOT NULL DEFAULT 'pbkdf2_sha256',
  `password_changed_at` datetime(3) DEFAULT NULL,
  `must_change_password` tinyint(1) NOT NULL DEFAULT '1',
  `failed_login_count` int unsigned NOT NULL DEFAULT '0',
  `locked_until` datetime(3) DEFAULT NULL,
  `last_login_at` datetime(3) DEFAULT NULL,
  `last_login_ip` varchar(45) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `approval_status` varchar(32) COLLATE utf8mb4_ja_0900_as_cs NOT NULL DEFAULT 'APPROVED' COMMENT 'APPROVED/PENDING/REJECTED。APPROVEDのみログイン可',
  `approval_requested_at` datetime(3) DEFAULT NULL,
  `approved_at` datetime(3) DEFAULT NULL,
  `approved_by_app_user_id` bigint unsigned DEFAULT NULL,
  `approval_note` text COLLATE utf8mb4_ja_0900_as_cs,
  `is_active` tinyint(1) NOT NULL DEFAULT '1',
  `note` text COLLATE utf8mb4_ja_0900_as_cs,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`app_user_id`),
  UNIQUE KEY `uq_app_users_employee_no` (`employee_no`),
  KEY `idx_app_users_approval` (`approval_status`,`is_active`),
  KEY `idx_app_users_approved_by` (`approved_by_app_user_id`),
  KEY `idx_app_users_active` (`is_active`),
  KEY `idx_app_users_last_login` (`last_login_at`),
  CONSTRAINT `fk_app_users_approved_by` FOREIGN KEY (`approved_by_app_user_id`) REFERENCES `app_users` (`app_user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE IF NOT EXISTS `app_roles` (
  `app_role_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `role_code` varchar(64) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,
  `role_name` varchar(190) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,
  `description` text COLLATE utf8mb4_ja_0900_as_cs,
  `is_system_role` tinyint(1) NOT NULL DEFAULT '0',
  `is_active` tinyint(1) NOT NULL DEFAULT '1',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`app_role_id`),
  UNIQUE KEY `uq_app_roles_role_code` (`role_code`),
  KEY `idx_app_roles_active` (`is_active`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE IF NOT EXISTS `app_permissions` (
  `app_permission_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `permission_code` varchar(128) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,
  `permission_name` varchar(190) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,
  `permission_group` varchar(64) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,
  `description` text COLLATE utf8mb4_ja_0900_as_cs,
  `is_active` tinyint(1) NOT NULL DEFAULT '1',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`app_permission_id`),
  UNIQUE KEY `uq_app_permissions_permission_code` (`permission_code`),
  KEY `idx_app_permissions_group` (`permission_group`,`is_active`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE IF NOT EXISTS `app_role_permissions` (
  `app_role_permission_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `app_role_id` bigint unsigned NOT NULL,
  `app_permission_id` bigint unsigned NOT NULL,
  `is_allowed` tinyint(1) NOT NULL DEFAULT '1',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`app_role_permission_id`),
  UNIQUE KEY `uq_app_role_permissions_role_permission` (`app_role_id`,`app_permission_id`),
  KEY `idx_app_role_permissions_permission` (`app_permission_id`),
  CONSTRAINT `fk_app_role_permissions_role` FOREIGN KEY (`app_role_id`) REFERENCES `app_roles` (`app_role_id`),
  CONSTRAINT `fk_app_role_permissions_permission` FOREIGN KEY (`app_permission_id`) REFERENCES `app_permissions` (`app_permission_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE IF NOT EXISTS `app_user_roles` (
  `app_user_role_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `app_user_id` bigint unsigned NOT NULL,
  `app_role_id` bigint unsigned NOT NULL,
  `valid_from` date DEFAULT NULL,
  `valid_to` date DEFAULT NULL,
  `is_active` tinyint(1) NOT NULL DEFAULT '1',
  `assigned_by_app_user_id` bigint unsigned DEFAULT NULL,
  `assigned_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `note` text COLLATE utf8mb4_ja_0900_as_cs,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`app_user_role_id`),
  UNIQUE KEY `uq_app_user_roles_user_role_from` (`app_user_id`,`app_role_id`,`valid_from`),
  KEY `idx_app_user_roles_role` (`app_role_id`,`is_active`),
  KEY `idx_app_user_roles_assigned_by` (`assigned_by_app_user_id`),
  CONSTRAINT `fk_app_user_roles_user` FOREIGN KEY (`app_user_id`) REFERENCES `app_users` (`app_user_id`),
  CONSTRAINT `fk_app_user_roles_role` FOREIGN KEY (`app_role_id`) REFERENCES `app_roles` (`app_role_id`),
  CONSTRAINT `fk_app_user_roles_assigned_by` FOREIGN KEY (`assigned_by_app_user_id`) REFERENCES `app_users` (`app_user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE IF NOT EXISTS `app_user_permissions` (
  `app_user_permission_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `app_user_id` bigint unsigned NOT NULL,
  `app_permission_id` bigint unsigned NOT NULL,
  `is_allowed` tinyint(1) NOT NULL DEFAULT '1' COMMENT '個人単位の明示許可/拒否',
  `assigned_by_app_user_id` bigint unsigned DEFAULT NULL,
  `note` text COLLATE utf8mb4_ja_0900_as_cs,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`app_user_permission_id`),
  UNIQUE KEY `uq_app_user_permissions_user_permission` (`app_user_id`,`app_permission_id`),
  KEY `idx_app_user_permissions_permission` (`app_permission_id`,`is_allowed`),
  KEY `idx_app_user_permissions_assigned_by` (`assigned_by_app_user_id`),
  CONSTRAINT `fk_app_user_permissions_user` FOREIGN KEY (`app_user_id`) REFERENCES `app_users` (`app_user_id`),
  CONSTRAINT `fk_app_user_permissions_permission` FOREIGN KEY (`app_permission_id`) REFERENCES `app_permissions` (`app_permission_id`),
  CONSTRAINT `fk_app_user_permissions_assigned_by` FOREIGN KEY (`assigned_by_app_user_id`) REFERENCES `app_users` (`app_user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE IF NOT EXISTS `app_user_allowed_ips` (
  `app_user_allowed_ip_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `app_user_id` bigint unsigned NOT NULL,
  `allowed_ip` varchar(45) COLLATE utf8mb4_ja_0900_as_cs NOT NULL COMMENT '許可する端末IP。範囲指定は初期対象外',
  `label` varchar(190) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `is_active` tinyint(1) NOT NULL DEFAULT '1',
  `note` text COLLATE utf8mb4_ja_0900_as_cs,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`app_user_allowed_ip_id`),
  UNIQUE KEY `uq_app_user_allowed_ips_user_ip` (`app_user_id`,`allowed_ip`),
  KEY `idx_app_user_allowed_ips_ip` (`allowed_ip`,`is_active`),
  CONSTRAINT `fk_app_user_allowed_ips_user` FOREIGN KEY (`app_user_id`) REFERENCES `app_users` (`app_user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE IF NOT EXISTS `app_sessions` (
  `app_session_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `session_token_sha256` char(64) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,
  `app_user_id` bigint unsigned NOT NULL,
  `client_ip` varchar(45) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `user_agent` varchar(512) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `issued_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `expires_at` datetime(3) NOT NULL,
  `last_seen_at` datetime(3) DEFAULT NULL,
  `revoked_at` datetime(3) DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`app_session_id`),
  UNIQUE KEY `uq_app_sessions_token` (`session_token_sha256`),
  KEY `idx_app_sessions_user_active` (`app_user_id`,`revoked_at`,`expires_at`),
  CONSTRAINT `fk_app_sessions_user` FOREIGN KEY (`app_user_id`) REFERENCES `app_users` (`app_user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE IF NOT EXISTS `app_login_attempts` (
  `app_login_attempt_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `employee_no` varchar(32) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,
  `app_user_id` bigint unsigned DEFAULT NULL,
  `success` tinyint(1) NOT NULL DEFAULT '0',
  `failure_reason` varchar(64) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `client_ip` varchar(45) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `user_agent` varchar(512) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`app_login_attempt_id`),
  KEY `idx_app_login_attempts_employee_no` (`employee_no`,`created_at`),
  KEY `idx_app_login_attempts_user` (`app_user_id`,`created_at`),
  CONSTRAINT `fk_app_login_attempts_user` FOREIGN KEY (`app_user_id`) REFERENCES `app_users` (`app_user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE IF NOT EXISTS `app_audit_logs` (
  `app_audit_log_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `app_user_id` bigint unsigned DEFAULT NULL,
  `employee_no` varchar(32) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `action_code` varchar(128) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,
  `target_schema` varchar(64) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `target_table` varchar(128) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `target_id` varchar(128) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `before_json` json DEFAULT NULL,
  `after_json` json DEFAULT NULL,
  `client_ip` varchar(45) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `user_agent` varchar(512) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`app_audit_log_id`),
  KEY `idx_app_audit_logs_user` (`app_user_id`,`created_at`),
  KEY `idx_app_audit_logs_action` (`action_code`,`created_at`),
  KEY `idx_app_audit_logs_target` (`target_schema`,`target_table`,`target_id`),
  CONSTRAINT `fk_app_audit_logs_user` FOREIGN KEY (`app_user_id`) REFERENCES `app_users` (`app_user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs;
