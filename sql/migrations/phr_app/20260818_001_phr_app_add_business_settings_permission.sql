USE `phr_app`;

INSERT INTO `app_permissions` (`permission_code`, `permission_name`, `permission_group`, `description`, `is_active`)
VALUES
  (
    'business_settings.view',
    '業務管理参照',
    'business',
    'イベント、健診機関、受領フォルダなど業務側の管理カテゴリを参照する',
    1
  ),
  (
    'business_settings.manage',
    '業務管理設定',
    'business',
    'イベント、健診機関、受領フォルダなど業務側の管理カテゴリを操作する',
    1
  )
ON DUPLICATE KEY UPDATE
  `permission_name` = VALUES(`permission_name`),
  `permission_group` = VALUES(`permission_group`),
  `description` = VALUES(`description`),
  `is_active` = VALUES(`is_active`);

INSERT INTO `app_role_permissions` (`app_role_id`, `app_permission_id`, `is_allowed`)
SELECT r.`app_role_id`, p.`app_permission_id`, 1
FROM `app_roles` r
JOIN `app_permissions` p
WHERE r.`role_code` IN ('ADMIN', 'EDITOR')
  AND p.`permission_code` IN ('business_settings.view', 'business_settings.manage')
ON DUPLICATE KEY UPDATE `is_allowed` = VALUES(`is_allowed`);

INSERT INTO `app_role_permissions` (`app_role_id`, `app_permission_id`, `is_allowed`)
SELECT r.`app_role_id`, p.`app_permission_id`, 0
FROM `app_roles` r
JOIN `app_permissions` p
WHERE r.`role_code` = 'VIEWER'
  AND p.`permission_code` IN ('business_settings.view', 'business_settings.manage')
ON DUPLICATE KEY UPDATE `is_allowed` = VALUES(`is_allowed`);
