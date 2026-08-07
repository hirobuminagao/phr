USE `phr_app`;

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

INSERT INTO `app_permissions` (`permission_code`, `permission_name`, `permission_group`, `description`, `is_active`)
VALUES
  ('hia_upload.perform', 'HIAアップロード作業', 'hia', '出力済みZIPをHIAへアップロードする作業を担当する', 1)
ON DUPLICATE KEY UPDATE
  `permission_name` = VALUES(`permission_name`),
  `permission_group` = VALUES(`permission_group`),
  `description` = VALUES(`description`),
  `is_active` = VALUES(`is_active`);

INSERT INTO `app_role_permissions` (`app_role_id`, `app_permission_id`, `is_allowed`)
SELECT r.`app_role_id`, p.`app_permission_id`, 1
FROM `app_roles` r
JOIN `app_permissions` p
WHERE r.`role_code` = 'ADMIN'
ON DUPLICATE KEY UPDATE `is_allowed` = VALUES(`is_allowed`);

UPDATE `app_role_permissions` rp
JOIN `app_roles` r
  ON r.`app_role_id` = rp.`app_role_id`
JOIN `app_permissions` p
  ON p.`app_permission_id` = rp.`app_permission_id`
SET rp.`is_allowed` = 0
WHERE r.`role_code` IN ('EDITOR', 'VIEWER')
  AND p.`permission_code` IN (
    'export_lists.edit',
    'xml_export.official',
    'hia_upload.perform',
    'hia_upload_status.edit'
  );
