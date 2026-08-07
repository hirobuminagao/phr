CREATE TABLE IF NOT EXISTS `app_settings` (
  `setting_key` varchar(128) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,
  `setting_value` varchar(1024) COLLATE utf8mb4_ja_0900_as_cs NOT NULL,
  `value_type` varchar(32) COLLATE utf8mb4_ja_0900_as_cs NOT NULL DEFAULT 'string',
  `setting_group` varchar(64) COLLATE utf8mb4_ja_0900_as_cs NOT NULL DEFAULT 'general',
  `description` text COLLATE utf8mb4_ja_0900_as_cs,
  `updated_by_app_user_id` bigint unsigned DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`setting_key`),
  KEY `idx_app_settings_group` (`setting_group`),
  KEY `idx_app_settings_updated_by` (`updated_by_app_user_id`),
  CONSTRAINT `fk_app_settings_updated_by`
    FOREIGN KEY (`updated_by_app_user_id`) REFERENCES `app_users` (`app_user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs;

INSERT INTO `app_settings` (
  `setting_key`, `setting_value`, `value_type`, `setting_group`, `description`
)
VALUES
  ('session_lifetime_minutes', '720', 'int', 'security', 'ログインセッションの最大有効時間。初期値は12時間'),
  ('session_idle_timeout_minutes', '60', 'int', 'security', '無操作状態で自動ログアウトするまでの分数。0は無効'),
  ('personal_info_audit_enabled', '1', 'bool', 'audit', '個人情報を含む画面閲覧・ダウンロードを監査ログへ記録する')
ON DUPLICATE KEY UPDATE
  `setting_value` = VALUES(`setting_value`),
  `value_type` = VALUES(`value_type`),
  `setting_group` = VALUES(`setting_group`),
  `description` = VALUES(`description`);
