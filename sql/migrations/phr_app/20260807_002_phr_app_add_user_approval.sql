ALTER TABLE `phr_app`.`app_users`
  ADD COLUMN `approval_status` varchar(32) COLLATE utf8mb4_ja_0900_as_cs NOT NULL DEFAULT 'APPROVED'
    COMMENT 'APPROVED/PENDING/REJECTED。APPROVEDのみログイン可'
    AFTER `last_login_ip`,
  ADD COLUMN `approval_requested_at` datetime(3) DEFAULT NULL
    AFTER `approval_status`,
  ADD COLUMN `approved_at` datetime(3) DEFAULT NULL
    AFTER `approval_requested_at`,
  ADD COLUMN `approved_by_app_user_id` bigint unsigned DEFAULT NULL
    AFTER `approved_at`,
  ADD COLUMN `approval_note` text COLLATE utf8mb4_ja_0900_as_cs
    AFTER `approved_by_app_user_id`,
  ADD KEY `idx_app_users_approval` (`approval_status`, `is_active`),
  ADD KEY `idx_app_users_approved_by` (`approved_by_app_user_id`);

ALTER TABLE `phr_app`.`app_users`
  ADD CONSTRAINT `fk_app_users_approved_by`
    FOREIGN KEY (`approved_by_app_user_id`)
    REFERENCES `phr_app`.`app_users` (`app_user_id`);

UPDATE `phr_app`.`app_users`
   SET `approval_status` = 'APPROVED',
       `approved_at` = COALESCE(`approved_at`, CURRENT_TIMESTAMP(3))
 WHERE `is_active` = 1
   AND `approval_status` = 'APPROVED';

INSERT INTO `phr_app`.`app_roles` (`role_code`, `role_name`, `description`, `is_system_role`, `is_active`)
VALUES
  ('PENDING', '未承認', '初回登録直後の未承認ユーザー用ロール。ログインは承認後に許可する', 1, 1)
ON DUPLICATE KEY UPDATE
  `role_name` = VALUES(`role_name`),
  `description` = VALUES(`description`),
  `is_system_role` = VALUES(`is_system_role`),
  `is_active` = VALUES(`is_active`);
