USE `phr_app`;

INSERT INTO `app_permissions` (`permission_code`, `permission_name`, `permission_group`, `description`, `is_active`)
VALUES
  ('exam_export_cases.csv_download', 'case CSV出力', 'xml_export', '個人case一覧の絞り込み結果を加入者情報付きCSVとして出力する', 1)
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
  AND p.`permission_code` = 'exam_export_cases.csv_download'
ON DUPLICATE KEY UPDATE `is_allowed` = VALUES(`is_allowed`);

UPDATE `app_role_permissions` rp
JOIN `app_roles` r
  ON r.`app_role_id` = rp.`app_role_id`
JOIN `app_permissions` p
  ON p.`app_permission_id` = rp.`app_permission_id`
SET rp.`is_allowed` = 0
WHERE r.`role_code` IN ('EDITOR', 'VIEWER')
  AND p.`permission_code` = 'exam_export_cases.csv_download';
